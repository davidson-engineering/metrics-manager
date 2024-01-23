from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict, field

from datetime import datetime

import pandas as pd
from src.metrics_agent.metrics_metric import Metric


@dataclass
class MetricStats:
    name: str
    value: dict = field(
        default_factory=dict(
            mean=None,
            max=None,
            min=None,
            count=None,
            std=None,
            sum=None,
        )
    )
    time: datetime = None

    def __iter__(self):
        yield from asdict(self).values()


class MetricsAggregator(ABC):
    def aggregate(self, metrics):
        aggregated_results = self.aggregate_method(metrics)

        return aggregated_results

    @abstractmethod
    def aggregate_method(self, values):
        ...


class MetricsAggregatorStats(MetricsAggregator):
    def aggregate_method(self, metrics):
        df = pd.DataFrame(metrics).set_index("name")
        df_mean = df.groupby("name").mean()
        df_time = df_mean.drop(columns=["value"])
        df_notime = df.drop(columns=["time"]).groupby("name")

        mean = df_mean.drop(columns=["time"]).rename(columns={"value": "mean"})
        max = df_notime.max().rename(columns={"value": "max"})
        min = df_notime.min().rename(columns={"value": "min"})
        count = df_notime.count().rename(columns={"value": "count"})
        std = df_notime.std().rename(columns={"value": "std"})
        sum = df_notime.sum().rename(columns={"value": "sum"})

        metrics_stats_dict = pd.concat(
            [mean, max, min, count, std, sum],
            axis=1,
        ).to_dict(orient="index")

        metrics_stats = [
            MetricStats(name=k, value=v, time=df_time.loc[k, "time"])
            for k, v in metrics_stats_dict.items()
        ]

        return metrics_stats
