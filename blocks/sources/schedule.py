from __future__ import annotations

from typing import Any, Type, Union
from datetime import datetime

from schedule import Job

from blocks.types import Event, Source, EventOrEvents


class Scheduler(Source):
    """
    Class represents source of events which are emitted by schedule.

    Therefore, other processors in your program may use these events as time-driven triggers.

    Uses "schedule" library (https://schedule.readthedocs.io/en/stable/)
    under the hood and exposes only subset of it's API.

    Example::

      >>> from blocks import Event
      >>> from blocks.sources.schedule import Scheduler

      >>> class MyEvent(Event):
      ...     pass

      >>> blocks = (Scheduler(MyEvent).every().minute.at(':30'), ...)
    """

    def __init__(self, scheduled_event: Type[Event], *, skip_when_busy: bool = True) -> None:
        self._event = scheduled_event
        self._job = Job(1)
        # FixMe (tribunsky.kir): def fun2(x: tp) -> None: ...  # Error: "tp" is not valid as a type
        self.patch_annotations({scheduled_event})

        self._skip_when_busy = skip_when_busy

    def __call__(self, _: Any = None) -> EventOrEvents:
        if self._skip_when_busy:
            if self._job.should_run:
                return self.__emit_single_event()
        else:
            if not self._job.last_run:
                return self.__emit_single_event()
            if self._job.last_run is None or self._job.period is None:
                raise RuntimeError("Couldn't determine {0} settings, please set-up {0}".format(self.__class__.__name__))
            if datetime.now() - self._job.last_run > self._job.period:
                missed_runs = (datetime.now() - self._job.last_run) // self._job.period
                self._job.last_run = datetime.now()
                return [self._event() for _ in range(missed_runs)]
        return []

    def every(self, interval: int = 1) -> Scheduler:
        self._job.interval = interval
        return self

    @property
    def second(self) -> Scheduler:
        return self.__hide_schedule_api('seconds')

    @property
    def minute(self) -> Scheduler:
        return self.__hide_schedule_api('minutes')

    @property
    def hour(self) -> Scheduler:
        return self.__hide_schedule_api('hours')

    @property
    def seconds(self) -> Scheduler:
        return self.second

    @property
    def minutes(self) -> Scheduler:
        return self.minute

    @property
    def hours(self) -> Scheduler:
        return self.hour

    def at(self, time_str: str) -> Scheduler:
        self._job.at(time_str)
        self._job._schedule_next_run()
        return self

    def __hide_schedule_api(self, unit: str) -> Scheduler:
        self._job.unit = unit
        self._job._schedule_next_run()
        return self

    def __emit_single_event(self) -> EventOrEvents:
        self._job.last_run = datetime.now()
        self._job._schedule_next_run()
        return [self._event()]
