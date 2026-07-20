"""Celery backend settings — the ``CELERY_`` env group.

``tool_name_arg`` / ``task_timeout`` / ``manifest_key`` mirror the host's base
backend settings surface (same names, same defaults) so both sides agree without
either importing the other. ``manifest_key`` names the env var the launcher and
the prefork-pool turnover write the live manifest JSON into, so preforked pool
children inherit it.

``redbeat_schedule_key`` intentionally yields ``redbeat::schedule`` (prefix
``redbeat:`` + literal ``:schedule``) — the exact zset key RedBeat maintains.
"""

from __future__ import annotations

from pydantic_settings import SettingsConfigDict
from tai42_kit.settings import TaiBaseSettings, settings_cache


class CelerySettings(TaiBaseSettings):
    model_config = SettingsConfigDict(env_prefix="CELERY_")

    broker_url: str = "amqp://localhost:5672//"
    result_backend: str = "redis://localhost:6379/0"
    redbeat_redis_url: str = "redis://localhost:6379/0"
    redbeat_key_prefix: str = "redbeat:"
    beat_max_loop_interval: int = 60

    # Prefork pool size for ``tai backend worker`` (env ``CELERY_WORKER_CONCURRENCY``),
    # capped at ONE child so a bus op can re-fork the WHOLE pool.
    #
    # Celery's ``pool_restart`` is a SOFT restart: it arms a per-child restart sentinel
    # and RETURNS as soon as the restart is armed — before the pool has finished
    # re-forking. So its immediate return does not prove the pool serves the new
    # registry: a bus op that replied ``applied`` on that return could be observed
    # while pre-restart children still serve the OLD tool registry, and a child that is
    # BUSY (mid-task) cannot recycle until its task completes, so a larger pool under
    # load can leave children un-recycled past the confirmation budget.
    #
    # The prefork-pool turnover does not paper over that: after every mutating bus op
    # the ``on_fleet_op_applied`` successor (``core.prefork``) confirms the re-fork
    # against ``stats`` and RAISES when the pool has not fully recycled, so raising this
    # value makes ops fail LOUDLY rather than silently leave children serving stale
    # tools. Serving a larger pool AND live-reloading it needs a pool that does not
    # inherit its registry through ``fork`` (``worker_pool="threads"``/``"gevent"``), or
    # a full worker-process respawn on reload — a backend design choice, not something
    # raising this value enables.
    worker_concurrency: int = 1

    # Shared backend-settings surface (host-agreed names and defaults).
    tool_name_arg: str = "backend_tool_name"
    task_timeout: int = 300
    manifest_key: str = "MANIFEST_KEY"

    @property
    def redbeat_schedule_key(self) -> str:
        """The RedBeat schedule zset key (``redbeat::schedule`` by default)."""
        return f"{self.redbeat_key_prefix}:schedule"

    def redbeat_task_key(self, name: str) -> str:
        """The RedBeat entry hash key for ``name`` (idempotent on prefixed input)."""
        if name.startswith(self.redbeat_key_prefix):
            return name
        return f"{self.redbeat_key_prefix}{name}"


@settings_cache
def celery_settings() -> CelerySettings:
    return CelerySettings()
