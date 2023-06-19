from __future__ import annotations

from typing import Dict, List, Type, Tuple, Union
from dataclasses import asdict, dataclass

from blocks import Event


@dataclass
class Row(Event):
    """
    Base class for database row description.
    Performs runtime type checks on table row received
    from query and drops invalid rows. Database source will returns
    every row as a single event in this case.
    Example::

      >>> from typing import Optional
      >>>
      >>> from blocks.db import Row
      >>>
      >>> class TableRow(Row):
      ...     field1: Optional[int]
      ...     field2: str
      >>>
      >>> query = Query('SELECT * FROM some_table', TableRow)
    """
    ...

    @property
    def columns(self) -> List[str]:
        return list(asdict(self).keys())

    @property
    def values(self) -> Tuple:
        return tuple(asdict(self).values())

    @property
    def as_dict(self) -> Dict:
        return asdict(self)


@dataclass
class Table(Event):
    """
    Base class for database table description.
    Performs runtime type checks on every received row
    from query and drops invalid rows and then groups
    rows into single table. Database source will returns
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
    rows: List[Row]

    @property
    def columns(self) -> List[str]:
        # ToDo (tribunsky.kir): stupid, but will work for simple cases
        if self.rows:
            return self.rows[0].columns
        return []

    @property
    def values(self) -> List[Tuple]:
        return [row.values for row in self.rows]

    @classmethod
    def type(cls) -> Type[Row]:
        # pydantic:
        # row_codec = query.codec.__annotations__['rows'].__args__[0]
        # dataclass:
        # return cls.rows.__args__[0]
        [tp] = cls.__annotations__['rows'].__args__
        return tp


@dataclass
class Query(Event):
    """
    Database query class that maps query result to given rows or table classes
    in case of selection and fills query with data from given row
    in case of insertion or updating.

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
      >>> query1 = Query('SELECT * FROM some_table', SomeTable)
      >>> query2 = Query('SELECT * FROM some_table', SomeTableRow)
    """
    text: str
    codec: Type[Union[Row, Table]]
    partition_key: Union[str, None] = None
