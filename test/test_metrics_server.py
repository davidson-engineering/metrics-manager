import pytest
from metrics_agent import MetricsClient
import time
from datetime import datetime, timedelta, timezone
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from metrics_agent import MetricsAgent


def test_db_client(metrics_agent_server):
    assert metrics_agent_server.client._client.ping() is True


def test_metrics_client(metrics_agent_server, caplog, random_dataset_1):
    caplog.set_level(logging.INFO)
    agent: MetricsAgent = metrics_agent_server
    agent.start_aggregator_thread()

    client = MetricsClient(host="localhost", port=9000).start()

    time_start = (datetime.now(timezone.utc) - timedelta(seconds=10)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    random_metrics = [
        ("cpu_usage", rand_number["value"], time.time())
        for rand_number in random_dataset_1
    ]
    random_metrics2 = [
        ("memory_usage", 1 - rand_number["value"], time.time())
        for rand_number in random_dataset_1
    ]
    # Example: Add metrics to the buffer
    [client.add_metric(*metric) for metric in random_metrics]
    [client.add_metric(*metric) for metric in random_metrics2]

    client.run_until_buffer_empty()
    agent.run_until_buffer_empty()

    time.sleep(2)

    time_stop = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Query the data back from the database as a pandas dataframe
    query = (
        f'from(bucket: "testing") |> range(start: {time_start}, stop: {time_stop})'
        '|> filter(fn: (r) => r._measurement == "cpu_usage")'
        '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
    )

    df = agent.client._client.query_api().query_data_frame(query)
    assert df["count"].max() == pytest.approx(100, abs=100)

    # Query the data back from the database as a pandas dataframe
    query = (
        f'from(bucket: "testing") |> range(start: {time_start}, stop: {time_stop})'
        '|> filter(fn: (r) => r._measurement == "memory_usage")'
        '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
    )

    df = agent.client._client.query_api().query_data_frame(query)
    assert df["count"].max() == pytest.approx(100, abs=100)
