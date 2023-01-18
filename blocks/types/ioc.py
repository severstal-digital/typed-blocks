from typing import Callable
from contextlib import AbstractContextManager


Dependency = Callable[[], AbstractContextManager]
