from dataclasses import dataclass

from blocks import App, source, processor
from blocks.types.process_metrics import AggregatedMetric


@dataclass
class E:
    x: int


@dataclass
class E2:
    y: int


@source
def generator() -> E:
    return E(1)


@processor
def printer_e(e: E) -> E2:
    print('1', e)
    return E2(e.x)


@processor
def printer_e2(e: E2) -> None:
    print('2', e)


@processor
def printer_metric(e: AggregatedMetric) -> None:
    print(e)


if __name__ == '__main__':
    blocks = (generator(), printer_e(), printer_e2(), printer_metric())
    App(
        blocks,
        # By default, the interval for aggregation of metrics is 60 seconds, but you can set it any way you want
        metric_time_interval=30
    ).run()
