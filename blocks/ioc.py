import contextlib
from functools import wraps
from inspect import getfullargspec
from typing import Any, Callable, Dict, get_type_hints, Type, Iterator

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

    __slots__ = ("_overrides",)

    def __init__(self, overrides: Dict[Callable, Dependency]):
        self._overrides = overrides

    def _normalize_dependecy(self, type_: Type, depends: Depends):
        if depends.dependency is None:
            return type_
        return depends.dependency

    def _get_dependencies(self, func: Callable) -> Dict[str, Callable]:
        hints = get_type_hints(func)
        argspec = getfullargspec(func)
        defaults = argspec.defaults or []
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
        def wrapper(*args, **kwargs):
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
