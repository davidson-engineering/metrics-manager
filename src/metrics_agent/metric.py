#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------

from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Any
from collections.abc import Sequence


@dataclass
class Metric(Sequence):
    name: str
    value: float
    time: datetime = field(default_factory=datetime.utcnow)

    def __iter__(self) -> Any:
        yield from asdict(self).values()

    def __repr__(self) -> str:
        return f"{self.name}: {self.value} @ {self.time}"

    def __getitem__(self, index) -> Any:
        if index == 0:
            return self.name
        elif index == 1:
            return self.value
        elif index == 2:
            return self.time
        else:
            raise IndexError("Metric index out of range")

    def __len__(self) -> int:
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
