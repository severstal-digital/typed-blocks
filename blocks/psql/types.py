from dataclasses import dataclass
from typing import Type, Union

from sqlmodel import SQLModel, select


@dataclass
class Query:
    text: select
    codec: Type[SQLModel]
    partition_key: Union[str, None] = None
