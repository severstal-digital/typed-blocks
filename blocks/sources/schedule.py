from __future__ import annotations

from typing import Any, List, Type, Union
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

    def __init__(self, scheduled_event: Type[Event]) -> None:
        self._event = scheduled_event
        self._job = Job(1)
        self.__call__.__annotations__['return'] = List[scheduled_event]  # type: ignore

    def __call__(self, _: Any = None) -> EventOrEvents:
        if self._job.should_run:
            self._job.last_run = datetime.now()
            self._job._schedule_next_run()
            return [self._event()]
        return []

    def every(self, interval: Union[int, float] = 1) -> Scheduler:
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
