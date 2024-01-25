import random
from datetime import datetime, timedelta, timezone

import pytest
from itertools import islice

from metrics_agent.metric import Metric


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


@pytest.fixture
def metrics_agent():
    from metrics_agent.agent import MetricsAgent
    from metrics_agent.db_client import InfluxDatabaseClient
    from metrics_agent.aggregator import MetricsAggregatorStats

    def metrics_agent_func(interval=1):
        client = InfluxDatabaseClient(
            config="test/influxdb_testing_config.toml", local_tz="America/Vancouver"
        )
        metrics_agent = MetricsAgent(
            interval=interval, client=client, aggregator=MetricsAggregatorStats()
        )
        return metrics_agent

    return metrics_agent_func


def parse_datetime_input(time):
    if isinstance(time, str):
        time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    elif isinstance(time, datetime):
        time = time
    else:
        raise ValueError("start_time must be either str or datetime")
    return time


def generate_random_dataset(size, seed=0, chunk_size=None):
    random.seed(seed)
    random_dataset = [
        dict(name="random_dataset", value=random.random()) for _ in range(size)
    ]
    if chunk_size:
        return tuple(chunk(random_dataset, chunk_size))
    else:
        return random_dataset


def generate_random_dataset_timed(
    size, seed=0, start_time="2021-01-01 00:00:00", period=1, chunk_size=None
):
    random.seed(seed)

    time = parse_datetime_input(start_time)

    random_dataset = [
        Metric(
            name="random_dataset",
            value=random.random(),
            time=time + timedelta(seconds=period * i),
        )
        for i in range(size)
    ]
    if chunk_size:
        return tuple(chunk(random_dataset, chunk_size))
    else:
        return random_dataset


@pytest.fixture
def random_dataset_1():
    return generate_random_dataset(size=100, seed=1)


@pytest.fixture
def random_dataset_1_chunked():
    return generate_random_dataset(seed=1, size=100, chunk_size=10)


@pytest.fixture
def random_dataset_2_chunked():
    return generate_random_dataset(seed=2, size=1000, chunk_size=100)


@pytest.fixture
def random_dataset_3_chunked():
    return generate_random_dataset(seed=3, size=10000, chunk_size=1000)


@pytest.fixture
def random_dataset_1_timed():
    return generate_random_dataset_timed(size=1000, seed=1)


def test_random_dataset_1(random_dataset_1):
    assert len(random_dataset_1) == 100
    assert random_dataset_1[0]["name"] == "random_dataset"
    assert random_dataset_1[0]["value"] < 1
    assert random_dataset_1[0]["value"] > 0


def test_random_dataset_1_chunked(random_dataset_1_chunked):
    assert len(random_dataset_1_chunked) == 10
    assert len(random_dataset_1_chunked[0]) == 10
    assert random_dataset_1_chunked[0][0]["name"] == "random_dataset"
    assert random_dataset_1_chunked[0][0]["value"] < 1
    assert random_dataset_1_chunked[0][0]["value"] > 0


def test_random_dataset(random_dataset_1_timed):
    assert len(random_dataset_1_timed) == 1000
    assert random_dataset_1_timed[0].name == "random_dataset"
    assert random_dataset_1_timed[0].value < 1
    assert random_dataset_1_timed[0].value > 0
    assert random_dataset_1_timed[0].time == datetime(2021, 1, 1, 0, 0, 0)


def test_random_dataset_2_chunked(random_dataset_2_chunked):
    assert len(random_dataset_2_chunked) == 10
    assert len(random_dataset_2_chunked[0]) == 100
    assert random_dataset_2_chunked[0][0]["name"] == "random_dataset"
    assert random_dataset_2_chunked[0][0]["value"] < 1
    assert random_dataset_2_chunked[0][0]["value"] > 0


def test_random_dataset_3_chunked(random_dataset_3_chunked):
    assert len(random_dataset_3_chunked) == 10
    assert len(random_dataset_3_chunked[0]) == 1000
    assert random_dataset_3_chunked[0][0]["name"] == "random_dataset"
    assert random_dataset_3_chunked[0][0]["value"] < 1
    assert random_dataset_3_chunked[0][0]["value"] > 0


def run_aggregator_test(agent, dataset):
    from metrics_agent.aggregator import MetricsAggregatorStats
    import time

    random_dataset_1_stats = []

    for metric_chunk in dataset:
        metrics = [Metric(**metric, time=datetime.now()) for metric in metric_chunk]
        # Calculate some stats on the dataset in a controlled environment
        random_dataset_1_stats.append(MetricsAggregatorStats().aggregate(metrics)[0])

    interval = agent.interval
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
    interval = 1
    agent = metrics_agent(interval=interval)
    run_aggregator_test(agent=agent, dataset=random_dataset_1_chunked)


# def test_metrics_aggregator_stats_medium(metrics_agent, random_dataset_2_chunked):
#     interval = 1
#     agent = metrics_agent(interval=interval)
#     run_aggregator_test(agent=agent, dataset=random_dataset_2_chunked)


# def test_metrics_aggregator_stats_large(metrics_agent, random_dataset_3_chunked):
#     interval = 1
#     agent = metrics_agent(interval=interval)
#     run_aggregator_test(agent=agent, dataset=random_dataset_3_chunked)
