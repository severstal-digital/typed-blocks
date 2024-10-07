from typing import List

from sqlalchemy import Engine
from sqlmodel import Session

from blocks import Source, Event
from blocks.psql.types import Query
from blocks.types import EventOrEvents


class PsqlReader(Source):
    def __init__(
        self,
        queries: List[Query],
        engine: Engine
    ) -> None:
        self._engine = engine
        self._queries = queries
        self.patch_annotations({query.codec for query in queries})

    @property
    def _conn(self) -> Session:
        return Session(self._engine)

    def __call__(self) -> EventOrEvents:
        with self._conn as conn:
            events = self._run_queries(conn)
        return events

    def _run_queries(self, conn: Session) -> List[Event]:
        data = []
        for query in self._queries:
            result = conn.exec(query.text).all()
            data.extend(result)
        return data
