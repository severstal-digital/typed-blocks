from __future__ import annotations

from typing import List, Type, Tuple
from dataclasses import dataclass

from blocks import Event


# ToDo (tribunsky.kir): make Table generic which is parametrized via event for rows.
@dataclass
class Table(Event):
    """
    Base class for database table description.
    Performs runtime type checks on every received row
    from query and drops invalid rows and then groups
    rows into single table. Database source will return
    all rows grouped in list as a single event in this case.
    Example::

      >>> from typing import Optional
      >>> from blocks.db import Row, Table
      >>>
      >>> class SomeTableRow(Row):
      ...     field1: Optional[int]
      ...     field2: str
      >>>
      >>> class SomeTable(Table):
      ...     rows: List[SomeTableRow]
      >>>
      >>> query = Query('SELECT * FROM some_table', SomeTable)
    """
    rows: List[Event]

    @property
    def columns(self) -> List[str]:
        # ToDo (tribunsky.kir): stupid, but will work for simple cases
        tp = self.type()
        return list(tp.__annotations__)

    @property
    def values(self) -> List[Tuple]:
        if self.rows:
            values = []
            fields = self.columns
            for row in self.rows:
                tpl = tuple(getattr(row, col) for col in fields)
                values.append(tpl)
            return values
        return []

    @classmethod
    def type(cls) -> Type[Event]:
        # pydantic:
        # row_codec = query.codec.__annotations__['rows'].__args__[0]
        # dataclass:
        # return cls.rows.__args__[0]
        [tp] = cls.__annotations__['rows'].__args__
        return tp
