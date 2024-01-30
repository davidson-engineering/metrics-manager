from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any
from collections.abc import Sequence


@dataclass
class Metric(Sequence):
    name: str
    value: float
    time: datetime

    def __iter__(self):
        yield from asdict(self).values()

    def __repr__(self):
        return f"{self.name}: {self.value} @ {self.time}"

    def __getitem__(self, index):
        if index == 0:
            return self.name
        elif index == 1:
            return self.value
        elif index == 2:
            return self.time
        else:
            raise IndexError("Metric index out of range")

    def __len__(self):
        return len(asdict(self))


def main():
    metric = Metric(name="test", value=0.5, time=datetime.now())
    print(metric)
    print(metric[0])
    print(metric[1])
    print(metric[2])
    print(len(metric))


if __name__ == "__main__":
    main()
