import contextlib
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Iterator

from blocks import App, processor, source
from blocks.ioc import Container, Depends
from blocks.types.ioc import Dependency


def read_only_provider() -> None:
    raise NotImplementedError


def get_read_only_mode() -> Dependency:
    @contextlib.contextmanager
    def manager() -> Iterator[bool]:
        yield True  # This setting determines whether printer will start

    return manager


c = Container(overrides={
    read_only_provider: get_read_only_mode()
})


@c.inject
def read_only(func: Callable[[Any], Any], is_read_only: bool = Depends(read_only_provider)) -> Callable[[Any], Any]:
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        if is_read_only is False:
            return func(*args, **kwargs)
        else:
            print(f"App running in read-only mode {type(func).__name__} will not be fulfilled")

    return wrapper


@dataclass
class E:
    x: int


@source
def generator() -> E:
    return E(1)


@processor
@read_only
def printer(e: E) -> None:
    print('1', e)


blocks = (generator(), printer())
App(blocks).run(once=True)
