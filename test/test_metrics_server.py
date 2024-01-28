import pytest
from metrics_client import MetricsClient
import time
from datetime import datetime, timedelta, timezone

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from metrics_agent import MetricsAgent


def test_db_client(metrics_agent_server):
    agent = metrics_agent_server()
    assert agent.client._client.ping() is True


def test_metrics_client(metrics_agent_server):
    agent: MetricsAgent = metrics_agent_server()
    agent.start_aggregator_thread()

    server_config = {
        "host": "localhost",
        "port": 9000,
    }

    client = MetricsClient(server_config)
    client.start()

    # Example: Add metrics to the buffer
    client.add_metric("cpu_usage", 80.0, time.time())
    client.add_metric("memory_usage", 60.0, time.time())

    agent.run_until_buffer_empty()

    time_start = (datetime.now(timezone.utc) - timedelta(seconds=10)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
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
