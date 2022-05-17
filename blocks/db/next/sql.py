# Hardly reusing & dirty hacking this https://death.andgravity.com/query-builder-how

from enum import Enum
from types import MappingProxyType
from typing import Any, Optional, Type, Union

# ToDo (tribunsky.kir): allow different base classes for relational DB
from dataclasses import fields

import functools
import textwrap

from blocks.db.types import Row, Table
from blocks.logger import logger


class Dialects(str, Enum):
    postgres: str = 'postgres'
    sqlite: str = 'sqlite'


_PLACEHOLDERS = MappingProxyType({
    Dialects.postgres: '%s',
    Dialects.sqlite: '?',
})

# Ordering matters =/
KEYWORDS = (
    'INSERT',
    'INTO',
    'UPDATE',
    'SELECT',
    'FROM',
    'SET',
    'WHERE',
    'ORDER BY',
    'LIMIT',
)

DEFAULT_SEPARATOR = ','


class Query(object):

    def __init__(self, codec: Optional[Type[Union[Row, Table]]] = None, dialect: str = Dialects.postgres) -> None:
        message = [
            'You are using EXPERIMENTAL API for queries',
            'which is currently unstable and absolutely buggy & unsafe!',
            'Use it on your own risk!'
        ]
        logger.warning(' '.join(message))
        self.data = {k: [] for k in KEYWORDS}
        self._whereable = set()
        self.codec: Type[Union[Row, Table]] = codec
        self._fields = []
        self._placeholder = _PLACEHOLDERS[dialect]

    @property
    def text(self) -> str:
        return str(self)

    def parametrize(self, event: Union[Row, Table]) -> tuple:
        return tuple([getattr(event, f) for f in self._fields])

    def add(self, keyword: str, *args) -> 'Query':
        target = self.data[keyword]

        # ToDo (tribunsky.kir): Separate different verbs or enter some simple grammar
        # ToDo (tribunsky.kir): Do not put placeholders as strings while adding parts of query,
        #                       use some stub and
        for arg in args:
            if keyword == 'INTO' and self.data.get('INSERT'):
                insert = self.data.get('INSERT')
                if insert:
                    statement = ['INTO'] + [arg] + [', '.join(insert)]
                    self.data['INSERT'] = [' '.join(statement)]
                else:
                    target.append(_clean_up(arg))
            elif isinstance(arg, str):
                if keyword == 'WHERE' and arg in self._whereable:
                    fld = _clean_up(arg)
                    target.append(fld + (' = ' + self._placeholder))
                    self._fields.append(fld)
                else:
                    target.append(_clean_up(arg))
            else:
                self.codec = arg
                self._whereable = set(field.name for field in fields(arg))
                if keyword == 'INSERT':
                    attrs = [attr.name for attr in fields(arg)]
                    flds = []
                    vals = []
                    for ix, attr in enumerate(attrs, 1):
                        self._fields.append(attr)
                        if ix == 1:
                            flds.append('(' + attr)
                            vals.append(('(' + self._placeholder))
                        elif ix == len(attrs):
                            flds.append(attr + ')')
                            vals.append((self._placeholder + ')'))
                        else:
                            flds.append(attr)
                            vals.append((self._placeholder))

                    into_table = self.data.get('INTO')
                    if into_table:
                        self.data['INSERT'] = ['INTO'] + into_table + flds
                    else:
                        self.data['INSERT'] = flds
                    self.data['VALUES'] = [', '.join(vals)]
                else:
                    if keyword == 'SET':
                        to_append = []
                        for field in fields(arg):
                            to_append.append(field.name + (' = ' + self._placeholder))
                            self._fields.append(field.name)
                        target += to_append
                    else:
                        target += [field.name for field in fields(arg)]

        return self

    def __getattr__(self, name: str) -> Any:
        if not name.isupper():
            return getattr(super(), name)
        return functools.partial(self.add, name.replace('_', ' '))

    def __str__(self) -> str:
        return ''.join(self._lines())

    def _lines(self):
        for keyword, things in self.data.items():
            if not things:
                continue

            yield '{0}\n'.format(keyword)
            yield from self._lines_keyword(keyword, things)

    def _lines_keyword(self, keyword: str, things):
        for i, thing in enumerate(things, 1):
            last = i == len(things)

            yield self._indent(thing)

            if not last:
                if keyword == 'WHERE':
                    yield ' AND '
                else:
                    yield DEFAULT_SEPARATOR

            yield '\n'

    _indent = functools.partial(textwrap.indent, prefix='    ')


def _clean_up(thing: str) -> str:
    return textwrap.dedent(thing.rstrip()).strip()
