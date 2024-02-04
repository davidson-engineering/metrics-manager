import pytest
import time
from datetime import datetime, timedelta, timezone
import logging

from network_simple.client import SimpleClientTCP
from metrics_agent import MetricsAgent
from metrics_agent.metric import Metric

logger = logging.getLogger(__name__)


class MetricsClientTCP(SimpleClientTCP):
    def add_metric(self, name, value, time):
        if not isinstance(time, (int, float)):
            time = time.timestamp()
        self.buffer.append(Metric(name, value, time))


def test_metrics_client(metrics_agent_server: MetricsAgent, caplog, random_dataset_1):
    caplog.set_level(logging.DEBUG)
    with metrics_agent_server as agent:
        assert agent.client._client.ping() is True
        agent.start_aggregator_thread()

        client = MetricsClientTCP(host="localhost", port=9000).start()

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

        time.sleep(10)

        time_stop = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Query the data back from the database as a pandas dataframe
        query = (
            f'from(bucket: "testing") |> range(start: {time_start}, stop: {time_stop})'
            '|> filter(fn: (r) => r._measurement == "cpu_usage")'
            '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
        )

        df = agent.client._client.query_api().query_data_frame(query)
        assert df["count"].max() == pytest.approx(100, abs=100)
        logger.debug("first test passed")

        # Query the data back from the database as a pandas dataframe
        query = (
            f'from(bucket: "testing") |> range(start: {time_start}, stop: {time_stop})'
            '|> filter(fn: (r) => r._measurement == "memory_usage")'
            '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
        )

        df = agent.client._client.query_api().query_data_frame(query)
        assert df["count"].max() == pytest.approx(100, abs=100)
