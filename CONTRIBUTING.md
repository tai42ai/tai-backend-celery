# Contributing to tai-backend-celery

`tai-backend-celery` is a Celery **execution backend** for the TAI ecosystem: it
implements `tai_contract.backend.Backend`, launching the worker runtime (`worker`
/ `beat` / `flower`), and ships the `sync_task` / `async_task` / `schedule_task`
tool extensions plus the `backend_*` tool surface (including the scheduling
marker tools the host's schedules API and backup round trip depend on). The hard
rule (the plugin rule): **it depends on `tai-contract` + `tai-kit` only and never
imports the skeleton.** Importing `tai_backend_celery` registers everything
through the global `tai_app` handle as a side-effect, and a manifest's
`backend_module` names the package. Fleet config propagation is not a backend
concern — live-reload ops reach each worker through the skeleton's own worker bus;
this backend's one addition on top of that is re-forking its prefork pool after a
mutating op so the children re-inherit the updated tool registry.

## Ground rules

- **No skeleton import — ever.** The package is contract-facing; the ban is
  enforced by ruff (`flake8-tidy-imports`), so a stray import fails lint:
  ```bash
  grep -rn "tai_skeleton" src/   # must be empty
  ```
- **No control plane in the backend.** Fleet ops arrive over the app's worker
  bus; the backend only confirms its own pool turnover on top.
- **Turnover is confirmed, never assumed.** After a mutating op the backend polls
  `stats` until every pre-restart child PID is gone and the pool is back to full
  size, and fails the op loudly (naming the worker) otherwise.
- **Loud errors.** No swallowed exceptions or silent fallbacks. A failed task
  re-raises; per-row schedule import errors are surfaced as
  `{"index", "name", "error"}`; `backend_list_failed_tasks` raises
  `NotImplementedError` (Celery keeps no queryable failed-task index).
- **Typed package** (`py.typed`). Pyright runs clean.

## Layout

- The `CeleryBackend` (the `Backend` impl) and its registration, plus the Celery
  application with its pool-child fork-safety hooks.
- The `backend_*` tool surface and the `sync_task` / `async_task` /
  `schedule_task` BACKEND-kind extensions.
- The RedBeat schedule store and the `backend_export_schedules` /
  `backend_import_schedules` backup round trip.
- The `CELERY_` settings.

## Dev

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

For local cross-repo work, `make dev` editable-installs the sibling `tai-*`
checkouts this package builds on into the venv. While `[tool.uv.sources]` pins
those siblings to local paths, `uv sync` already installs them editable and
`make dev` changes nothing; once the lock resolves them from the registry,
`uv sync` / `uv run` installs the published builds instead, so re-run
`make dev` afterward to restore the editable links.

Before any commit, run a secret scan over `src/` and `tests/` (e.g.
`detect-secrets scan`).

## License

By contributing you agree your contributions are licensed under Apache-2.0.
