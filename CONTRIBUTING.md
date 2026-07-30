# Contributing to tai42-backend-celery

`tai42-backend-celery` is a Celery **execution backend** for the TAI ecosystem: it
implements `tai42_contract.backend.Backend`, launching the worker runtime (`worker`
/ `beat` / `flower`), and ships the `sync_task` / `async_task` / `schedule_task`
tool extensions plus the `backend_*` tool surface (including the scheduling
marker tools the host's schedules API and backup round trip depend on). The hard
rule (the plugin rule): **it depends on `tai42-contract` + `tai42-kit` only and never
imports the skeleton.** Importing `tai42_backend_celery` registers everything
through the global `tai42_app` handle as a side-effect, and a manifest's
`backend_module` names the package. Fleet config propagation is not a backend
concern — live-reload ops reach each worker through the skeleton's own worker bus;
this backend's one addition on top of that is re-forking its prefork pool after a
mutating op so the children re-inherit the updated tool registry.

## Ground rules

- **No skeleton import — ever.** The package is contract-facing; the ban is
  enforced by ruff (`flake8-tidy-imports`), so a stray import fails lint:
  ```bash
  grep -rn "tai42_skeleton" src/   # must be empty
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

## Naming

PyPI is a flat namespace with no owner in the path, so distributions carry the
`tai42-` prefix. GitHub repositories keep their `tai-` names, because the
`tai42ai` organisation already namespaces them. Import packages follow the
distribution.

| Surface | Form |
| --- | --- |
| Distribution — PyPI, `pip install`, dependency pins | `tai42-<name>` |
| Import package | `tai42_<name>` |
| GitHub repository | `tai-<name>` |

So a dependency is declared as `tai42-<name>` while its repository is named
`tai-<name>`, and both spellings are correct in their own context.

Some surfaces are deliberately neither, and must not be renamed: the `tai` CLI
command (`tai42` is an alias), the Prometheus metric namespace (`tai_tool_*`),
`TAI_*` environment variables, and the `tai-plugin.yml` descriptor filename.

## Dev

```bash
uv venv --python 3.13
uv pip install --no-sources --group dev --editable .
uv run --no-sync pytest --cov --cov-report=term-missing
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync pyright
```

`make dev` installs the sibling `tai-contract` and `tai-kit` repos as editable installs for local cross-repo development.

Before any commit, run a secret scan over `src/` and `tests/` (e.g.
`detect-secrets scan`).

## Dependency resolution

`uv.lock` pins the `tai42-*` siblings to their released index versions while `[tool.uv.sources]` points them at local `../tai-*` checkouts. The two disagree deliberately: CI sets `UV_NO_SOURCES=1` and asserts the lock with `uv sync --locked`, so it resolves the artifacts a user installs. A bare `uv lock` beside sibling checkouts re-couples the lock to editable path entries, which then fails that `--locked` check — run `uv lock --no-sources` instead. See [How dependencies resolve](https://tai42.ai/contributing#how-dependencies-resolve).

## License

By contributing you agree your contributions are licensed under Apache-2.0.
