from dataclasses import dataclass
from datetime import datetime

@dataclass
class Metric:
    metric_name: str
    value: float
    timestamp: datetime