from typing import List, Union, Type

from sqlalchemy import Engine
from sqlmodel import Session, SQLModel

from blocks import Processor


class PsqlWriter(Processor):
    def __init__(
        self,
        type_events: List[Type[SQLModel]],
        engine: Engine
    ) -> None:
        self._engine = engine
        self.__call__.__annotations__.update(
            {
                'event': Union[tuple(type_events)],
            },
        )

    @property
    def _conn(self) -> Session:
        return Session(self._engine)

    def __call__(self, event: SQLModel) -> None:
        # todo: provide for the option of storing requests in a stack
        with self._conn as conn:
            conn.add(event)
            conn.commit()
            conn.refresh(event)

