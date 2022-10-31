import pytest

try:
    from pydantic import BaseModel
except ImportError:
    pytest.skip("skipping pydantic-only tests", allow_module_level=True)

from blocks.redis.serdes import serialize, deserialize


class ManyFieldsModel(BaseModel):
    flag: bool = True
    string: str = 'string'
    pi: float = 3.15
    digit: int = 13
    other_flag: bool = False


def test_ser_des_pydantic() -> None:
    value = ManyFieldsModel()
    serialized = serialize(value.dict())
    deserialized = deserialize(serialized)
    assert value == ManyFieldsModel(**deserialized)
