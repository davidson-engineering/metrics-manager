from abc import ABC, abstractmethod
from collections import defaultdict 

from fast_influxdb_client import FastInfluxDBClient, InfluxMetric


class MetricsTransmitter(ABC):
    @abstractmethod
    def transmit(self, metric):
        ...
    
    @abstractmethod
    def convert(self, metric):
        ...


class InfluxDBMetricsTransmitter(MetricsTransmitter):

    def __init__(self, config):
        self.client = FastInfluxDBClient.from_config_file(config)

    def convert(self, metric):
        metric = defaultdict(None, metric)
        return InfluxMetric(
            measurement=metric.name,
            time=metric.timestamp,
            tags=metric.tags,
            fields=metric.values,
        )
    
    def transmit(self, metrics):
        for metric in metrics:
            metric = self.convert(metric)
            self.client.write_metric(metric)
