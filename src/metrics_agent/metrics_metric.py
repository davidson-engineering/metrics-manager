from dataclasses import dataclass, asdict, field
from datetime import datetime


@dataclass
class Metric:
    name: str
    value: float
    time: datetime

    def __iter__(self):
        yield from asdict(self).values()
