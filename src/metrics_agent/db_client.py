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
from fast_influxdb_client import FastInfluxDBClient
import logging
import json

logger = logging.getLogger(__name__)


def check_attributes(metric: dict, keys=("measurement", "fields", "time")) -> bool:
    try:
        assert all(key in metric for key in keys)
    except AssertionError as e:
        raise AttributeError(f"Metric must contain {keys}") from e
    return True


class DatabaseClient(ABC):
    @abstractmethod
    def send(self, metrics): ...

    @abstractmethod
    def convert(self, metrics): ...


class InfluxDatabaseClient(DatabaseClient):
    def __init__(self, config, write_precision="S", default_bucket="testing"):
        self._client = FastInfluxDBClient.from_config_file(
            config, write_precision=write_precision
        )
        self._client.default_bucket = default_bucket

    def convert(self, metric: Union[tuple, dict, dataclass]) -> dict:

        ensure_keys = ("measurement", "fields", "time", "tags")

        if is_dataclass(metric):
            metric = asdict(metric)

        elif isinstance(metric, tuple):
            try:
                assert len(metric) >= 3
                if isinstance(metric[2], dict):
                    fields = metric[2]
                else:
                    fields = dict(value=metric[2])
                if len(metric) == 4:
                    tags = metric[3]
                else:
                    tags = {}
                metric = dict(
                    measurement=metric[0], fields=fields, time=metric[2], tags=tags
                )
            except AssertionError as e:
                raise ValueError(
                    f"metric must be a dict containing {ensure_keys} or be a tuple of length 3 in format {ensure_keys}"
                ) from e
        elif isinstance(metric, dict):
            pass
        elif isinstance(metric, str):
            # Assume metric is JSON. Convert to dict
            metric = json.loads(metric)
        else:
            raise ValueError("metric must be either a tuple, dict, or a dataclass")
        if "tags" not in metric:
            metric["tags"] = {}
        assert check_attributes(metric, ensure_keys)

        return metric

    def send(self, metrics: list[Union[tuple, dict, dataclass]]):
        try:
            metrics = [self.convert(metric) for metric in metrics]
        except Exception as e:
            logger.error(f"Error converting metrics: {e}. Continuing...")
            return
        self._client.write_metric(metrics)
