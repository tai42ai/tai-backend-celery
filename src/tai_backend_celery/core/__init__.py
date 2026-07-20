"""Core Celery backend package.

The top-level ``tai_backend_celery`` package imports these modules, so their
import side effects fire exactly once per app start:
:mod:`~tai_backend_celery.core.backend` registers :class:`CeleryBackend`
through ``tai_app.backends.register_backend``, :mod:`~tai_backend_celery.core.app`
builds the Celery application and installs the pool-child fork-safety signal
hooks, and :mod:`~tai_backend_celery.core.tasks` registers the Celery task surface.
"""
