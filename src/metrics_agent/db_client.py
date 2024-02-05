#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------

from abc import ABC, abstractmethod
from typing import Union
from dataclasses import asdict, is_dataclass, dataclass
from fast_influxdb_client import FastInfluxDBClient, InfluxMetric
from metrics_agent.metric import Metric


def check_attributes(metric: dict):
    try:
        assert "measurement" in metric
        assert "fields" in metric
        assert "time" in metric
    except AssertionError as e:
        raise ValueError("metric must contain measurement, fields and time") from e
    return True
        
class DatabaseClient(ABC):
    @abstractmethod
    def send(self, metrics): ...

    @abstractmethod
    def convert(self, metrics): ...


class InfluxDatabaseClient(DatabaseClient):
    def __init__(self, config, local_tz="UTC", write_precision="S"):
        self._client = FastInfluxDBClient.from_config_file(
            config, write_precision=write_precision
        )
        self._client.default_bucket = "testing"
        self.local_tz = local_tz

    def convert(self, metric: Union[tuple, dict, dataclass]) -> dict:
        if is_dataclass(metric):
            metric = asdict(metric)
            assert check_attributes(metric)
        elif isinstance(metric, Metric):
            metric = dict(measurement=metric.name, fields=dict(value=metric.value), time=metric.time)
        elif isinstance(metric, dict):
            assert check_attributes(metric)
        elif isinstance(metric, tuple):
            try:
                assert len(metric) == 3
                metric = dict(measurement=metric[0], fields=dict(value=metric[1]), time=metric[2])
            except AssertionError as e:
                raise ValueError("metric must contain measurement, fields and time") from e
        else:
            raise ValueError("metric must be either a tuple, dict, or a dataclass")
        try:
            metric["time"] = metric["time"].tz_localize(self.local_tz)
        except (TypeError, AttributeError):
            pass
        metric["time"] = int(metric["time"]) #TODO test this with datetime
        return metric

    def send(self, metrics):
        for metric in metrics:
            metric = self.convert(metric)
        self._client.write_metric(metrics)
