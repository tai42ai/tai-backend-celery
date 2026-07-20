"""Prefork-pool turnover after a worker-bus op applies in this process.

A Celery prefork worker forks children that inherit the parent process's tool
registry at fork time. When a worker-bus op mutates this process's registry, the
already-forked children keep serving the OLD registry until they happen to be
recycled — so the op has not truly applied on this worker until the pool has
re-forked. The ``on_fleet_op_applied`` handler here re-forks the LOCAL pool and
CONFIRMS the turnover: it polls ``stats`` until every pre-restart child pid is
gone and the pool is back to full size, and RAISES otherwise. A raise turns this
worker's bus reply into ``failed`` — the correct outcome, because a bare
``pool_restart`` returns once the restart is ARMED, before the pool has finished
re-forking: an ``applied`` reply that skips confirmation can be observed by a fast
follow-up read while the pre-restart children still serve the stale registry.

This turnover runs inside the op apply, before this worker's terminal reply is
sent, so its budget is derived from the bus apply window (``TAI_BUS_APPLY_TIMEOUT``)
and sits a margin under it — the confirm or raise then reaches the publisher before
its report cut, so a stalled turnover reports a truthful ``failed`` rather than the
publisher guessing ``timed_out``.

Registration is worker-scoped: :func:`register` is called from
:meth:`CeleryBackend.launch` (the plugin imports in every process, including ASGI
workers with no pool, so this must never register at import time) and the handler
guards on an actual prefork pool — a ``solo``/``threads``/``gevent`` pool has no
forked children to recycle, so the handler is a no-op there. It also fires only
for MUTATING ops: the query op ``list_failed_mcps`` carries no registry change,
so it never re-forks the pool.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from celery import signals
from tai_contract.app import tai_app

from tai_backend_celery.core.app import celery_app
from tai_backend_celery.core.settings import celery_settings

logger = logging.getLogger(__name__)

# The bus ops that MUTATE this process's tool registry, so the forked children
# must be recycled to re-inherit it. Mirrors the skeleton's dispatchable ops
# minus the query op ``list_failed_mcps`` — a read must never re-fork the pool.
_MUTATING_OPS: frozenset[str] = frozenset(
    {
        "reload_config",
        "reload_mcp",
        "deregister_mcp",
        "reload_tool",
        "remove_tool",
        "reload_failed_mcps",
    }
)

# Per-``stats``/``pool_restart`` control-call timeout and the pause between
# turnover polls. The whole-turnover budget is derived per call (see
# :func:`_turnover_budget`) so it tracks the bus apply window.
_POOL_CONTROL_TIMEOUT = 2.0
_POOL_RESTART_TIMEOUT = 10.0
_POOL_RECYCLE_POLL_INTERVAL = 0.5

# The turnover runs INSIDE the op apply, before this worker's terminal
# (``applied``/``failed``) reply is sent, and the publisher waits only its bus
# apply timeout for that reply. So the whole-turnover budget is derived from the
# same deployment knob the bus reads — ``TAI_BUS_APPLY_TIMEOUT`` (read straight
# from the env, a shared deployment-config convention) — and sits a margin under
# it. The margin leaves room for the received-ack + terminal-reply round-trip so
# this worker's confirm-or-raise reaches the publisher BEFORE its report cut,
# making a stalled turnover record a truthful ``failed`` instead of a
# ``timed_out`` guess. Floored so a small apply window still leaves the pool a
# usable window to re-fork.
_BUS_APPLY_TIMEOUT_ENV = "TAI_BUS_APPLY_TIMEOUT"
_BUS_APPLY_TIMEOUT_DEFAULT = 30.0
_TURNOVER_BUDGET_MARGIN = 3.0
_TURNOVER_BUDGET_FLOOR = 5.0

# This worker's own node name, captured at worker setup, and a flag set once the
# worker is consuming control commands. The handler addresses ``pool_restart`` /
# ``stats`` to this name; before the worker is ready it stays a no-op (children
# forked during bootstrap inherit the freshly built registry, so none is stale).
_local_nodename: str | None = None
_worker_ready = threading.Event()


def register() -> None:
    """Wire the prefork-pool turnover into this worker process.

    Worker-scoped — called from ``launch`` before the worker boots, never at
    import: connects the setup/ready signals that identify the local pool and
    registers the post-apply handler on the app's lifecycle seam.
    """
    signals.celeryd_after_setup.connect(_record_local_nodename)
    signals.worker_ready.connect(_mark_worker_ready)
    tai_app.lifecycle.on_fleet_op_applied(_on_fleet_op_applied)


def _record_local_nodename(sender: Any = None, instance: Any = None, **kwargs: Any) -> None:
    """Capture this worker's node name; ``celeryd_after_setup`` sends it as the
    ``sender`` and carries the worker ``instance`` (its ``hostname``)."""
    global _local_nodename
    name = getattr(instance, "hostname", None) or sender
    if name:
        _local_nodename = str(name)


def _mark_worker_ready(sender: Any = None, **kwargs: Any) -> None:
    """Mark the worker ready to answer control commands (``pool_restart`` /
    ``stats``), so the turnover only runs once the local pidbox is consuming."""
    _worker_ready.set()


async def _on_fleet_op_applied(op_name: str) -> None:
    """Re-fork and confirm the local prefork pool after a mutating bus op.

    A query op carries no registry change and never re-forks the pool. The
    turnover's control I/O is blocking, so it runs off the serving loop; a raise
    from it propagates, failing the op's terminal reply — the correct signal when
    a child could still be serving the stale registry.
    """
    if op_name not in _MUTATING_OPS:
        return
    await asyncio.to_thread(_turnover_local_pool, op_name)


def _turnover_local_pool(op_name: str) -> None:
    """Restart the local prefork pool and confirm the whole pool re-forked.

    No-op for a non-prefork pool (no forked children to recycle) and before the
    worker has even reached setup (``_local_nodename`` still ``None``): the pool has
    not forked yet, so the children it forks next inherit THIS (post-op) registry and
    none is stale.

    A worker that HAS reached setup (``_local_nodename`` captured) but is not yet
    consuming control commands (``worker_ready`` unset) is the dangerous window: its
    bootstrap child may already have forked from the PRE-op registry, yet its pidbox
    cannot answer ``pool_restart``/``stats`` to recycle it. Skipping there would reply
    ``applied`` while that bootstrap child still serves the stale registry — the exact
    turnover contract this module upholds. So the turnover WAITS (bounded by the
    turnover budget) for the worker to start consuming, then recycles; a worker that
    never starts consuming raises loudly (the op reports ``failed``)."""
    if _local_nodename is None:
        logger.debug("prefork turnover skipped for %s: worker has not reached setup (no forked pool yet)", op_name)
        return
    budget = _turnover_budget()
    deadline = time.monotonic() + budget
    if not _worker_ready.is_set() and not _worker_ready.wait(timeout=budget):
        raise RuntimeError(
            f"[{_local_nodename}] worker did not start consuming control commands within {budget}s; "
            "cannot recycle a possibly-stale prefork pool after a mutating bus op"
        )
    hostname = _local_nodename
    before = _prefork_pool_state(hostname)
    if before is None:
        return
    _refresh_manifest_env()
    _restart_pool(hostname)
    _confirm_turnover(hostname, before, max(0.0, deadline - time.monotonic()))


def _turnover_budget() -> float:
    """The whole-turnover budget, derived to sit UNDER the bus apply window.

    Reads ``TAI_BUS_APPLY_TIMEOUT`` — the same knob the bus honors — and returns
    it less a margin, floored, so the confirm-or-raise lands before the
    publisher's report cut. Raising the operator's bus apply timeout raises this
    budget under it; a malformed value raises.
    """
    raw = os.environ.get(_BUS_APPLY_TIMEOUT_ENV)
    apply_timeout = _BUS_APPLY_TIMEOUT_DEFAULT if raw is None else float(raw)
    return max(_TURNOVER_BUDGET_FLOOR, apply_timeout - _TURNOVER_BUDGET_MARGIN)


@contextmanager
def _control_connection() -> Iterator[Any]:
    """A fresh, short-lived broker connection dedicated to one turnover control
    call (``stats`` / ``pool_restart``), never the app's shared connection pool.

    The pooled connection is inherited by the prefork children at their fork, and
    ``pool_restart`` TERMINATES those children — a dying child tears the shared
    broker connection down at the protocol level, so the parent's next control
    write over it hits a broken pipe. A connection opened here (not shared with
    any child being recycled) and closed right after sidesteps that.
    """
    with celery_app.connection_for_write() as conn:
        yield conn


def _pool_pids(pool: dict[str, Any]) -> set[int]:
    """The child PIDs a prefork worker reports in its ``stats`` pool section."""
    return {p for p in pool.get("processes", []) if isinstance(p, int)}


def _prefork_pool_state(hostname: str) -> tuple[set[int], int] | None:
    """This worker's prefork pool state ``(child_pids, max_concurrency)`` from a
    single ``stats`` call, or ``None`` when the pool is not prefork (solo/threads/
    gevent) — those have no forked children to recycle. A worker that does not
    answer its own ``stats``, or answers without a pool section, is raised: its
    turnover cannot be verified, and a prefork worker skipped here would keep
    serving stale tools from its forked children.
    """
    with _control_connection() as conn:
        stats = (
            celery_app.control.inspect(
                destination=[hostname], timeout=_POOL_CONTROL_TIMEOUT, limit=1, connection=conn
            ).stats()
            or {}
        )
    cfg = stats.get(hostname)
    if not isinstance(cfg, dict):
        raise RuntimeError(f"worker {hostname} did not answer stats; cannot verify its pool for turnover")
    pool = cfg.get("pool")
    if not isinstance(pool, dict):
        raise RuntimeError(f"worker {hostname} stats carried no pool section; cannot verify turnover")
    if "prefork" not in str(pool.get("implementation", "")).lower():
        return None
    pids = _pool_pids(pool)
    max_conc = pool.get("max-concurrency")
    return pids, max_conc if isinstance(max_conc, int) and max_conc > 0 else len(pids)


def _refresh_manifest_env() -> None:
    """Publish the live manifest JSON into the env so the re-forked children
    inherit the current registry."""
    os.environ[celery_settings().manifest_key] = json.dumps(tai_app.admin.live_manifest, separators=(",", ":"))


def _restart_pool(hostname: str) -> None:
    """Arm this worker's pool restart. The reply only means the restart is armed;
    the actual turnover is confirmed by :func:`_confirm_turnover`."""
    with _control_connection() as conn:
        replies = celery_app.control.broadcast(
            "pool_restart",
            arguments={"reload": False},
            reply=True,
            destination=[hostname],
            timeout=_POOL_RESTART_TIMEOUT,
            # One addressed worker, one reply — return on it instead of waiting out
            # the full timeout.
            limit=1,
            connection=conn,
        )
    reply = next(iter(replies or []), {}).get(hostname, {})
    if not (isinstance(reply, dict) and "ok" in reply):
        raise RuntimeError(f"pool restart request failed for {hostname}: {reply!r}")


def _confirm_turnover(hostname: str, before: tuple[set[int], int], timeout: float) -> None:
    """Poll ``stats`` until the pool has fully re-forked — every pre-restart child
    pid gone AND the pool back to full size — or the deadline elapses, in which
    case the worker is raised loudly (its reload may still be serving stale
    tools).

    ``pool_restart`` returns once the restart is ARMED, before the pool has
    finished re-forking, so its immediate reply does not prove the pool is serving
    the new registry — a fast follow-up read could still reach a pre-restart child.
    This confirmation holds the bus reply until every pre-restart child pid is gone
    and the pool is back to full size, making an incomplete re-fork fail loudly
    instead of reporting success behind stale children.
    """
    old_pids, max_conc = before
    surviving = old_pids
    size = 0
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        with _control_connection() as conn:
            stats = (
                celery_app.control.inspect(
                    destination=[hostname], timeout=min(_POOL_CONTROL_TIMEOUT, remaining), limit=1, connection=conn
                ).stats()
                or {}
            )
        cfg = stats.get(hostname)
        if isinstance(cfg, dict) and isinstance(cfg.get("pool"), dict):
            new_pids = _pool_pids(cfg["pool"])
            surviving = old_pids & new_pids
            size = len(new_pids)
            if not surviving and size >= max_conc:
                return
        nap = min(_POOL_RECYCLE_POLL_INTERVAL, max(0.0, deadline - time.monotonic()))
        if nap == 0.0:
            break
        time.sleep(nap)
    if surviving:
        detail = f"old children still present: {sorted(surviving)}; they may still serve stale tools"
    else:
        detail = f"pool came back short: {size}/{max_conc} children"
    raise RuntimeError(f"[{hostname}] prefork pool did not fully recycle within {timeout}s ({detail})")
