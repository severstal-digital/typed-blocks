from __future__ import annotations

from typing import List, Type, Union, NamedTuple
from dataclasses import dataclass

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
