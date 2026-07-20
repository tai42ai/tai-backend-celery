"""Backend-neutral schedule record used by the export/import backup round trip."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator


class ScheduleRecord(BaseModel):
    """One schedule in a backend-neutral, JSON-serializable form.

    ``name`` identifies the schedule; ``args`` and ``kwargs`` are the positional
    and keyword arguments handed to the scheduled call; ``schedule`` holds the
    canonical interval-or-crontab dict that
    :func:`tai_kit.utils.runtime.schedule_util.normalize_schedule` produces;
    ``enabled`` marks whether the schedule is active.

    ``model_dump`` yields a plain JSON-ready dict and ``model_validate`` reads
    one back, so a record survives a round trip through a backup document
    unchanged.
    """

    name: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    schedule: dict[str, Any]
    enabled: bool

    @model_validator(mode="after")
    def _check_schedule_kind(self) -> ScheduleRecord:
        kind = self.schedule.get("__type__")
        if kind not in {"interval", "crontab"}:
            raise ValueError(f"schedule '__type__' must be 'interval' or 'crontab', got {kind!r}")
        if kind == "interval":
            every = self.schedule.get("every")
            # bool is an int subclass; a boolean 'every' is not a valid period.
            if not isinstance(every, int | float) or isinstance(every, bool):
                raise ValueError(f"interval schedule requires numeric 'every', got {every!r}")
            relative = self.schedule.get("relative")
            if relative is not None and not isinstance(relative, bool):
                raise ValueError(f"interval 'relative' must be a bool, got {relative!r}")
        else:  # crontab
            missing = [
                field
                for field in ("minute", "hour", "day_of_month", "month_of_year", "day_of_week")
                if field not in self.schedule
            ]
            if missing:
                raise ValueError(f"crontab schedule missing required field(s): {missing}")
        return self
