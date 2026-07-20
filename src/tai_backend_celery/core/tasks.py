"""The Celery task surface: ``tool_execution`` and ``callback_task``.

Both tasks are :class:`AsyncTask` subclasses — each task object owns one
long-lived private event loop, so pooled per-loop clients opened by tool code
are reused across task invocations instead of being rebuilt per call (the loop
is closed, with its client pools, by ``close_loop`` at worker-child shutdown).

``tool_execution`` retries automatically on transient transport errors
(connection/timeout) with jittered exponential backoff, never on ``ValueError``
(a bad argument does not become correct by retrying). It runs the target tool
through the host handle inside :func:`prevent_celery_stream_capture`, which
temporarily restores the real OS stdio so subprocesses spawned by tools (e.g.
stdio MCP servers) inherit valid file descriptors instead of Celery's
``LoggingProxy``.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Coroutine, Generator
from contextlib import contextmanager
from typing import Any

from celery import Task
from celery.exceptions import TimeoutError as CeleryTimeoutError
from tai_contract.app import tai_app

from tai_backend_celery.core.app import celery_app
from tai_backend_celery.core.callback import CallbackSchema, callback_execution
from tai_backend_celery.core.settings import celery_settings

# Task options every dispatching extension exposes; ``callback_kwargs`` chains a
# follow-up tool (see ``callback_task``) via a Celery ``link`` signature.
CELERY_TASK_OPTS: dict[str, Any] = {
    "queue": str | None,
    "countdown": int | None,
    "priority": int | None,
    "retry": bool | None,
    "routing_key": str | None,
    "expires": str | float | None,
    "eta": str | None,
    "callback_kwargs": CallbackSchema | None,
}

CELERY_SCHEDULE_OPTS: dict[str, Any] = {
    "backend_schedule_name": str,
    "backend_schedule": int | float | str | dict[str, Any],
}


@contextmanager
def prevent_celery_stream_capture() -> Generator[None]:
    """Temporarily restore ``sys.stdout``/``sys.stderr`` to the real OS file
    descriptors, bypassing Celery's ``LoggingProxy``, so subprocesses spawned by
    the tool (e.g. stdio MCP servers) inherit valid file descriptors."""
    captured_stdout = sys.stdout
    captured_stderr = sys.stderr
    try:
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        yield
    finally:
        # Restore the Celery proxies so logging continues strictly after.
        sys.stdout = captured_stdout
        sys.stderr = captured_stderr


class AsyncTask(Task):
    """A task whose instance owns one private event loop for its async work."""

    _loop: asyncio.AbstractEventLoop | None = None

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The task's private loop, (re)created when absent or closed."""
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        return self._loop

    def run_async(self, coroutine: Coroutine[Any, Any, Any]) -> Any:
        return self.loop.run_until_complete(coroutine)

    def close_loop(self) -> None:
        """Close the task loop, shutting down its per-loop pooled clients first.

        Client pools are keyed per event loop, so they must be closed ON this
        loop before it goes away; run at worker-child shutdown.
        """
        loop = self._loop
        self._loop = None
        if loop is None or loop.is_closed():
            return
        try:
            loop.run_until_complete(tai_app.clients.shutdown_clients())
        finally:
            loop.close()


async def run_tool(**kwargs: Any) -> Any:
    """Pop the dispatch-injected tool name and run that tool with the rest."""
    tool_name = kwargs.pop(celery_settings().tool_name_arg)
    return await tai_app.tools.run_tool(tool_name, kwargs)


@celery_app.task(
    name="celery.callback_task",
    base=AsyncTask,
    bind=True,
    dont_autoretry_for=(ValueError,),
    autoretry_for=(ConnectionError, TimeoutError, CeleryTimeoutError),
)
def callback_task(self: AsyncTask, result: Any, callback: CallbackSchema) -> Any:
    """Run a :class:`CallbackSchema` over the parent task's result.

    Linked to ``tool_execution`` via ``apply_async(link=...)``, so ``result`` is
    the parent's return value, handed to the jq condition/expression inputs
    as-is (an object result is queried as an object).
    """
    return self.run_async(callback_execution(result, callback))


@celery_app.task(
    name="celery.tool_execution",
    base=AsyncTask,
    bind=True,
    autoretry_for=(ConnectionError, TimeoutError, CeleryTimeoutError),
    dont_autoretry_for=(ValueError,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
)
def tool_execution(self: AsyncTask, *args: Any, **kwargs: Any) -> Any:
    """Execute one app tool on the worker (the body of every dispatched task)."""
    with prevent_celery_stream_capture():
        return self.run_async(run_tool(**kwargs))
