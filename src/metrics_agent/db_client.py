#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------

from abc import ABC, abstractmethod

from fast_influxdb_client import FastInfluxDBClient, InfluxMetric
from metrics_agent.metric import Metric


class DatabaseClient(ABC):
    @abstractmethod
    def send(self, metric): ...

    @abstractmethod
    def convert(self, metric): ...


class InfluxDatabaseClient(DatabaseClient):
    def __init__(self, config, local_tz="UTC", write_precision="S"):
        self._client = FastInfluxDBClient.from_config_file(
            config, write_precision=write_precision
        )
        self._client.default_bucket = "testing"
        self.local_tz = local_tz

    def convert(self, metric: dict):
        influx_metric = InfluxMetric(**metric)
        try:
            influx_metric.time = influx_metric.time.tz_localize(self.local_tz)
        except (TypeError, AttributeError):
            pass
        influx_metric.time = int(influx_metric.time)
        return influx_metric

    def send(self, metrics):
        for metric in metrics:
            influx_metric = self.convert(metric)
            self._client.write_metric(influx_metric)
