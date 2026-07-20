"""The Celery application and the pool-child fork-safety hooks.

``celery_app`` is built at import time from :class:`CelerySettings` (RedBeat is
the beat scheduler; pool restarts and task events are enabled).

Fork safety: ``worker_process_init`` runs in each freshly forked pool child and
evicts the monitoring vendor client through the contract's fork-safe
``writer.shutdown()`` (the parent's client owns threads/sockets that are dead in
the child; the first use in the child rebuilds it cleanly). The eviction is
logged, never silent, and telemetry stays enabled. ``worker_process_shutdown``
flushes buffered monitoring spans and closes each task loop's pooled clients
before the child exits.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

from celery import Celery, signals
from celery_pydantic import pydantic_celery
from tai42_contract.app import tai42_app

from tai42_backend_celery.core.settings import celery_settings

logger = logging.getLogger(__name__)


# --- fork-safety signal hooks ------------------------------------------------


@signals.worker_process_init.connect
def on_worker_process_init(sender: Any = None, **kwargs: Any) -> None:
    """Runs in each freshly forked pool child before it takes work.

    The parent built the monitoring vendor client pre-fork; its exporter
    threads/sockets are dead in this child. ``writer.shutdown()`` is the
    contract's fork-safe evict: it drops the vendor client wholesale so the
    child's first span rebuilds it clean. Telemetry stays enabled — the evict is
    logged, and on macOS a warning names the platform hazard (the system
    resolver is not fork-safe once the parent initialized platform frameworks,
    so a child's first telemetry export may crash the child; prefer the ``solo``
    or ``threads`` pool there).
    """
    tai42_app.monitoring.active.writer.shutdown()
    logger.info("worker child %s: evicted the pre-fork monitoring client (fork-safe shutdown)", sender)
    if sys.platform == "darwin":
        logger.warning(
            "macOS prefork child: the system DNS resolver is not fork-safe, so telemetry export from this "
            "child may crash it; use the 'solo' or 'threads' worker pool on macOS if that happens"
        )


@signals.worker_process_shutdown.connect
def on_worker_process_shutdown(sender: Any = None, **kwargs: Any) -> None:
    """Runs in each pool child as it exits: flush buffered monitoring spans
    (they would otherwise be lost waiting for the SDK's periodic flush), then
    close each task loop's pooled clients."""
    try:
        tai42_app.monitoring.active.writer.flush()
    except Exception as e:
        logger.warning("Error flushing monitoring on worker child shutdown: %s", e)

    from tai42_backend_celery.core.tasks import callback_task, tool_execution

    for task in (tool_execution, callback_task):
        try:
            task.close_loop()
        except Exception as e:
            logger.warning("Error closing task loop clients on worker child shutdown: %s", e)


# --- app factory ---------------------------------------------------------------


def create_celery_app() -> Celery:
    settings = celery_settings()
    app = Celery(
        "TaiMCPCelery",
        broker=settings.broker_url,
        backend=settings.result_backend,
    )

    pydantic_celery(app)

    app.conf.update(
        beat_scheduler="redbeat.RedBeatScheduler",
        redbeat_redis_url=settings.redbeat_redis_url,
        redbeat_key_prefix=settings.redbeat_key_prefix,
        beat_max_loop_interval=settings.beat_max_loop_interval,
        timezone="UTC",
        worker_pool_restarts=True,
        # One child by default (see CelerySettings.worker_concurrency for the
        # soft-pool-restart limitation this bounds, and how a reload surfaces it).
        worker_concurrency=settings.worker_concurrency,
        worker_send_task_events=True,  # worker emits task-* events
        task_send_sent_event=True,  # beat/client emits 'task-sent'
    )

    return app


celery_app = create_celery_app()
