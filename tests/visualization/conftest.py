import os
from typing import Generator

import pytest


@pytest.fixture(autouse=True)
def run_around_tests() -> Generator[None, None, None]:
    files = ['GRAPH.png', 'DAG.svg']
    for file_name in files:
        if os.path.exists(file_name):
            raise
    try:
        yield
    finally:
        for file_name in files:
            if os.path.exists(file_name):
                os.remove(file_name)
