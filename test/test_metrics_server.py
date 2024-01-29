import pytest
from network_sync import MetricsClient
import time
from datetime import datetime, timedelta, timezone
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agent import MetricsAgent


def test_db_client(metrics_agent_server):
    agent = metrics_agent_server()
    assert agent.client._client.ping() is True


def test_metrics_client(metrics_agent_server, caplog, random_dataset_1):
    caplog.set_level(logging.DEBUG)
    agent: MetricsAgent = metrics_agent_server(interval=1)
    agent.start_aggregator_thread()

    client = MetricsClient(host="localhost", port=9000)
    client.start()

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

    time.sleep(10)
    agent.run_until_buffer_empty()

    time_stop = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Query the data back from the database as a pandas dataframe
    query = (
        f'from(bucket: "testing") |> range(start: {time_start}, stop: {time_stop})'
        '|> filter(fn: (r) => r._measurement == "cpu_usage")'
        '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
    )

    df = agent.client._client.query_api().query_data_frame(query)
    print(df.head())

    # Query the data back from the database as a pandas dataframe
    query = (
        f'from(bucket: "testing") |> range(start: {time_start}, stop: {time_stop})'
        '|> filter(fn: (r) => r._measurement == "memory_usage")'
        '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
    )

    df = agent.client._client.query_api().query_data_frame(query)
    print(df.head())
