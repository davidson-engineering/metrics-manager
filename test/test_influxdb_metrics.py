import pytest
import logging
from datetime import datetime, timedelta, timezone
import time

from metrics_agent import Metric

logger = logging.getLogger(__name__)

def test_db_client(metrics_agent):
    agent = metrics_agent()
    assert agent.db_client._client.ping() is True

def test_metrics_agent_simple(metrics_agent, random_dataset_1):
    agent = metrics_agent()
    assert agent.update_interval == 1

    time_start = (datetime.now(timezone.utc) - timedelta(seconds=10)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    random_metrics = [
        ("cpu_usage", {"cpu0":rand_number["value"]}, time.time())
        for rand_number in random_dataset_1
    ]
    random_metrics2 = [
        ("memory_usage", {"mem0":1 - rand_number["value"]}, time.time())
        for rand_number in random_dataset_1
    ]
    # Example: Add metrics to the buffer
    [agent.add_metric(*metric) for metric in random_metrics]
    [agent.add_metric(*metric) for metric in random_metrics2]

    # agent.run_until_buffer_empty()
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



def run_aggregator_test(agent, dataset):
    from metrics_agent.aggregator import MetricsAggregatorStats
    import time

    random_dataset_1_stats = []

    for metric_chunk in dataset:
        metrics = [Metric(**metric, time=datetime.now()) for metric in metric_chunk]
        # Calculate some stats on the dataset in a controlled environment
        random_dataset_1_stats.append(MetricsAggregatorStats().aggregate(metrics)[0])

    interval = agent.update_interval
    agent.start_aggregator_thread()

    time_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for metric_chunk in dataset:
        for metric in metric_chunk:
            agent.add_metric(**metric)
        time.sleep(interval * 1.1)

    # ensure the buffer is empty before continuing
    agent.run_until_buffer_empty()

    time_stop = (datetime.now(timezone.utc) + timedelta(seconds=10)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    # Query the data back from the database as a pandas dataframe
    query = (
        f'from(bucket: "testing") |> range(start: {time_start}, stop: {time_stop})'
        '|> filter(fn: (r) => r._measurement == "random_dataset")'
        '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
    )

    df = agent.client._client.query_api().query_data_frame(query)

    # compare the stats from the database to the stats calculated in the controlled environment
    for _, index in df.iterrows():
        metric = random_dataset_1_stats.pop(0)
        assert metric.value["mean"] == pytest.approx(index["mean"])
        assert metric.value["max"] == pytest.approx(index["max"])
        assert metric.value["min"] == pytest.approx(index["min"])
        assert metric.value["count"] == pytest.approx(index["count"])
        assert metric.value["std"] == pytest.approx(index["std"])
        assert metric.value["sum"] == pytest.approx(index["sum"])


def test_metrics_aggregator_stats_small(metrics_agent, random_dataset_1_chunked):
    update_interval = 1
    agent = metrics_agent(update_interval=update_interval)
    run_aggregator_test(agent=agent, dataset=random_dataset_1_chunked)


def test_metrics_aggregator_stats_medium(metrics_agent, random_dataset_2_chunked):
    update_interval = 1
    agent = metrics_agent(update_interval=update_interval)
    run_aggregator_test(agent=agent, dataset=random_dataset_2_chunked)


def test_metrics_aggregator_stats_large(metrics_agent, random_dataset_3_chunked):
    update_interval = 1
    agent = metrics_agent(update_interval=update_interval)
    run_aggregator_test(agent=agent, dataset=random_dataset_3_chunked)
