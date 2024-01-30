from abc import ABC, abstractmethod

from fast_influxdb_client import FastInfluxDBClient, InfluxMetric


class DatabaseClient(ABC):
    @abstractmethod
    def send(self, metric):
        ...

    @abstractmethod
    def convert(self, metric):
        ...


class InfluxDatabaseClient(DatabaseClient):
    def __init__(self, config, local_tz="UTC", write_precision="S"):
        self._client = FastInfluxDBClient.from_config_file(
            config, write_precision=write_precision
        )
        self._client.default_bucket = "testing"
        self.local_tz = local_tz

    def convert(self, metric):
        try:
            time = metric.time.tz_localize(self.local_tz)
        except TypeError:
            time = metric.time
        return InfluxMetric(
            measurement=metric.name,
            time=time,
            fields=metric.value,
        )

    def send(self, metrics):
        for metric in metrics:
            metric = self.convert(metric)
            self._client.write_metric(metric)
