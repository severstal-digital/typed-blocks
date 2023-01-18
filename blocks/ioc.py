import contextlib
from functools import wraps
from inspect import getfullargspec
from typing import Any, Callable, Dict, get_type_hints, Tuple, Type, Iterator, Optional

from blocks.types.ioc import Dependency


class Depends:
    def __init__(self, dependency: Any = None):
        self.dependency = dependency


def static_dependency(value: Any) -> Callable[[], Any]:
    @contextlib.contextmanager
    def manager() -> Iterator[Any]:
        yield value

    return manager


class Container:
    """
      >>> import contextlib
      >>> from dataclasses import dataclass
      >>> from typing import Iterator

      >>> from blocks.types.ioc import Dependency
      >>> from blocks.ioc import Container, Depends, static_dependency

      >>> def some_provider() -> None:
      >>>    raise NotImplementedError

      >>> def get_some() -> Dependency:
      >>>    @contextlib.contextmanager
      >>>    def manager() -> Iterator[int]:
      >>>        yield 42
      >>>    return manager

      >>> @dataclass
      >>> class Foo:
      >>>     value: str

      >>> c = Container(overrides={
      >>>         some_provider: get_some(),
      >>>         Foo: static_dependency(Foo(value="bar"))
      >>>     }
      >>> )

      >>> @c.inject
      >>> def show_info(some: int = Depends(some_provider), foo: Foo = Depends()) -> None:
      >>>     print(some)
      >>>     print(foo)

      >>> show_info()
    """
    __slots__ = ("_overrides",)

    def __init__(self, overrides: Dict[Callable, Dependency]):
        self._overrides = overrides

    def _normalize_dependecy(self, type_: Optional[Type], depends: Depends) -> Any:
        if depends.dependency is None:
            return type_
        return depends.dependency

    def _get_dependencies(self, func: Callable) -> Dict[str, Callable]:
        hints = get_type_hints(func)
        argspec = getfullargspec(func)
        defaults: Tuple[Depends, ...] = argspec.defaults or tuple()
        kwonlydefaults = argspec.kwonlydefaults or {}
        positional = {
            arg: self._normalize_dependecy(hints.get(arg), default)
            for arg, default in zip(argspec.args[::-1], defaults[::-1])
            if isinstance(default, Depends)
        }
        keyword = {
            arg: self._normalize_dependecy(hints.get(arg), default)
            for arg, default in kwonlydefaults.items()
            if isinstance(default, Depends)
        }
        return {
            **positional,
            **keyword,
        }

    def inject(self, handler: Callable) -> Callable:
        required_dependencies = self._get_dependencies(handler)

        @wraps(handler)
        def wrapper(*args: Tuple, **kwargs: Dict) -> Any:
            stack = contextlib.ExitStack()
            dependencies = {}
            for name, dependency in required_dependencies.items():
                dependency_context = self._overrides[dependency]()
                dependencies[name] = stack.enter_context(dependency_context)
            try:
                kwargs.update(dependencies)
                return handler(*args, **kwargs)
            finally:
                stack.close()

        return wrapper
