import os
from typing import Generator

import pytest


@pytest.fixture(autouse=True)
def run_around_tests() -> Generator[None, None, None]:
    file_name = 'GRAPH.png'
    if os.path.exists(file_name):
        raise
    try:
        yield
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)
