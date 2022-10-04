from enum import Enum


class RenderingKernelType(str, Enum):
    # NetworkX + matplotlib
    matplotlib: str = 'matplotlib'
    graphviz: str = 'graphviz'
