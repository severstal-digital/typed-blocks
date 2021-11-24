from typing import Any, Dict, Callable, Optional
from datetime import datetime

# ToDo (tribunsky.kir): Implement something more flexible than {str: str}
Serializer = Callable[[Dict[str, Any]], Dict[bytes, bytes]]
Deserializer = Callable[[Dict[bytes, bytes]], Dict[str, Optional[bytes]]]


def deserialize(message: Dict[bytes, bytes]) -> Dict[str, Optional[bytes]]:
    dct = {}
    for key, value in message.items():
        to_insert = None if value == b'null' else value
        dct[key.decode()] = to_insert
    return dct


def serialize(dct: Dict[str, Any]) -> Dict[bytes, bytes]:
    message = {}
    for k, v in dct.items():
        if v is None:
            value = b'null'
        elif isinstance(v, bool):
            value = str(v).lower().encode()
        elif isinstance(v, bytes):
            value = v
        elif isinstance(v, datetime):
            value = str(int(v.timestamp() * 1000)).encode()
        else:
            value = str(v).encode()
        message[k.encode()] = value
    return message
