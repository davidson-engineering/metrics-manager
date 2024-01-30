import pytest
import random
from datetime import datetime, timedelta
from itertools import islice

from metrics_agent.metric import Metric


INFLUXDB_TESTING_CONFIG_FILEPATH = "test/influxdb_testing_config.toml"
LOCAL_TZ = "America/Vancouver"


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


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


@pytest.fixture
def metrics_agent():
    from metrics_agent import MetricsAgent
    from metrics_agent.db_client import InfluxDatabaseClient
    from metrics_agent.aggregator import MetricsAggregatorStats

    def metrics_agent_func(interval=1):
        client = InfluxDatabaseClient(
            config=INFLUXDB_TESTING_CONFIG_FILEPATH, local_tz=LOCAL_TZ
        )
        metrics_agent = MetricsAgent(
            interval=interval,
            client=client,
            aggregator=MetricsAggregatorStats(),
            autostart=False,
        )
        return metrics_agent

    return metrics_agent_func


@pytest.fixture
def metrics_agent_server():
    from metrics_agent.agent import MetricsAgent
    from metrics_agent.db_client import InfluxDatabaseClient
    from metrics_agent.aggregator import MetricsAggregatorStats

    def metrics_agent_func(interval=1):
        client = InfluxDatabaseClient(
            config=INFLUXDB_TESTING_CONFIG_FILEPATH, local_tz=LOCAL_TZ
        )
        metrics_agent = MetricsAgent(
            interval=interval,
            client=client,
            aggregator=MetricsAggregatorStats(),
            autostart=False,
            server=True,
            host="localhost",
            port=9000,
        )
        return metrics_agent

    return metrics_agent_func
