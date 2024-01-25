from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class Metric:
    name: str
    value: float
    time: datetime

    def __iter__(self):
        yield from asdict(self).values()
