import datetime
from textwrap import dedent
from dataclasses import dataclass

import pytest

from blocks.db import Row
from blocks.db.next.sql import Query, Dialects


@dataclass
class TableRow(Row):
    id: int
    name: str
    dt: datetime.datetime


@pytest.fixture
def row() -> TableRow:
    return TableRow(id=1, name='test', dt=datetime.datetime.now())


# ToDo (tribunsky.kir): looks like there is a nice to be a query's method/representation
def check(query: Query, answer: str) -> None:
    q = str(query)
    delim = ' '
    normalized = dedent(delim.join([el.strip() for el in q.replace('\n', delim).split(delim) if el.strip()])).casefold()
    assert normalized == answer.casefold()


def test_select_all() -> None:
    query = Query(dialect=Dialects.sqlite).SELECT('*').FROM('table')
    answer = 'SELECT * FROM table'
    check(query, answer)


def test_select_model() -> None:
    query = Query(dialect=Dialects.sqlite).SELECT(TableRow).FROM('table')
    answer = 'SELECT id, name, dt from table'
    check(query, answer)


def test_select_model_where() -> None:
    query = Query(dialect=Dialects.sqlite).SELECT(TableRow).FROM('table').WHERE('name is not null', 'dt is null')
    answer = 'SELECT id, name, dt from table where name is not null and dt is null'
    check(query, answer)


def test_insert(row) -> None:
    query = Query(dialect=Dialects.sqlite).INSERT(TableRow).INTO('table')
    answer = 'INSERT INTO table (id, name, dt) VALUES (?, ?, ?)'
    check(query, answer)

    tpl = query.parametrize(row)
    assert tpl == (row.id, row.name, row.dt)


def test_update_where_self_reference(row) -> None:
    query = Query(dialect=Dialects.sqlite).UPDATE('table').SET(TableRow).WHERE('id')
    answer = 'UPDATE table SET id = ?, name = ?, dt = ? WHERE id = ?'
    check(query, answer)

    tpl = query.parametrize(row)
    assert tpl == (row.id, row.name, row.dt, row.id)


def test_update_where_self_reference_multiple(row):
    query = Query(dialect=Dialects.sqlite).UPDATE('table').SET(TableRow).WHERE('id', 'name is not null')
    answer = 'UPDATE table SET id = ?, name = ?, dt = ? WHERE id = ? AND name is not null'
    check(query, answer)

    tpl = query.parametrize(row)
    assert tpl == (row.id, row.name, row.dt, row.id)


def test_update_where_clause(row):
    query = Query(dialect=Dialects.sqlite).UPDATE('table').SET(TableRow).WHERE('id > 10')
    answer = 'UPDATE table SET id = ?, name = ?, dt = ? WHERE id > 10'
    check(query, answer)

    tpl = query.parametrize(row)
    assert tpl == (row.id, row.name, row.dt)
