from abc import ABC, abstractmethod

from fast_influxdb_client import FastInfluxDBClient, InfluxMetric


class DatabaseClient(ABC):
    @abstractmethod
    def send(self, metric):
        ...

    @abstractmethod
    def convert(self, metric):
        ...


class InfluxDBClient(DatabaseClient):
    def __init__(self, config, local_tz="UTC"):
        self.client = FastInfluxDBClient.from_config_file(config)
        self.client.default_bucket = "testing"
        self.local_tz = local_tz

    def convert(self, metric):
        return InfluxMetric(
            measurement=metric.name,
            time=metric.time.tz_localize(self.local_tz),
            fields=metric.value,
        )

    def send(self, metrics):
        for metric in metrics:
            metric = self.convert(metric)
            self.client.write_metric(metric)
