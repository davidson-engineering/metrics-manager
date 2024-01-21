from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd
from metrics_metric import Metric

class MetricsAggregator(ABC):
    def aggregate(self, metrics):


        return aggregated_results

    @abstractmethod
    def aggregate_method(self, values):
        pass



class AverageMetricsAggregator(MetricsAggregator):
    def aggregate_method(self, values):
        return sum(values) / len(values)

class MedianMetricsAggregator(MetricsAggregator):
    def aggregate_method(self, values):
        return sorted(values)[len(values) // 2]

class MaxMetricsAggregator(MetricsAggregator):
    def aggregate_method(self, values):
        return max(values)

class MinMetricsAggregator(MetricsAggregator):
    def aggregate_method(self, values):
        return min(values)
