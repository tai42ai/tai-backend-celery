"""Celery execution backend for the TAI ecosystem.

Importing this package registers everything through the global ``tai42_app``
handle as a side-effect (there is no entry-point): the :class:`CeleryBackend`
(``tai42_app.backends.register_backend``), the Celery application with its
pool-child fork-safety hooks, the Celery task surface
(``celery.tool_execution`` / ``celery.callback_task``), the
``backend_*`` tool surface, and the ``sync_task`` / ``schedule_task`` /
``async_task`` BACKEND-kind tool extensions. The host names this package in
its manifest's ``backend_module`` field and imports it at startup. This
package never imports the host skeleton — it talks to the host only through
the ``tai42_app`` handle from ``tai42_contract.app``.
"""

import tai42_backend_celery.core.tasks  # registers the Celery task surface
import tai42_backend_celery.extensions.extensions  # registers the tool extensions
import tai42_backend_celery.tools.tools  # noqa: F401  (registers the backend_* tools)
from tai42_backend_celery.core.backend import CeleryBackend

__all__ = ["CeleryBackend"]
