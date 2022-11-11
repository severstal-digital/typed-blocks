from typing import List, Type, NamedTuple
from dataclasses import dataclass

import pytest

from blocks.compat import HAS_PYDANTIC

from blocks.db.next.types import Table


class RowNT(NamedTuple):
    id: int


class TableNT(Table):
    rows: List[RowNT]


@dataclass
class RowDC:
    id: int


class TableDC(Table):
    rows: List[RowDC]


types_mapping = {
    RowNT: TableNT,
    RowDC: TableDC,
}


if HAS_PYDANTIC:
    from pydantic import BaseModel

    class RowP(BaseModel):
        id: int


    class TableP(Table):
        rows: List[RowP]

    types_mapping.update({RowP: TableP})


@pytest.mark.parametrize('row,table', [(row, table) for row, table in types_mapping.items()])
def test_table(row: Type[object], table: Type[Table]):
    t = table(rows=[])
    assert t.columns == ['id']
    assert t.values == []

    t.rows.append(row(id=1))
    assert t.columns == ['id']
    assert t.values == [(1,)]
