import pytest
from datetime import datetime


def test_metric():
    from metrics_agent.metric import Metric

    metric = Metric(name="test_metric", value=0.5)
    assert metric.name == "test_metric"
    assert metric.value == 0.5
    assert isinstance(metric.time, datetime)

    metric = Metric(name="test_metric", value=0.5, time="2021-01-01 00:00:00")
    assert metric.name == "test_metric"
    assert metric.value == 0.5
    assert metric.time == "2021-01-01 00:00:00"
    assert not hasattr(metric, "tags")
    assert not hasattr(metric, "priority")
    assert len(metric) == 3
