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
    from metrics_agent.processors import MetricsAggregatorStats

    def metrics_agent_func(update_interval=1):
        db_client = InfluxDatabaseClient(
            config=INFLUXDB_TESTING_CONFIG_FILEPATH, local_tz=LOCAL_TZ
        )
        metrics_agent = MetricsAgent(
            update_interval=update_interval,
            db_client=db_client,
            autostart=True,
        )
        return metrics_agent

    return metrics_agent_func


@pytest.fixture
def metrics_agent_server():
    from metrics_agent import MetricsAgent
    from metrics_agent.db_client import InfluxDatabaseClient
    from metrics_agent.processors import MetricsAggregatorStats

    db_client = InfluxDatabaseClient(
        config=INFLUXDB_TESTING_CONFIG_FILEPATH, local_tz=LOCAL_TZ
    )
    metrics_agent = MetricsAgent(
        update_interval=1,
        db_client=db_client,
        autostart=True,
        server_tcp=True,
    )
    return metrics_agent


import socket
import threading


def UDP_echo_server(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
        server_socket.bind((host, port))
        print(f"Echo server is listening on {host}:{port}")

        while True:
            data, address = server_socket.recvfrom(1024)
            print(f"Received data from {address}: {data.decode()}")

            data = f"ECHO: {data.decode()}".encode()

            server_socket.sendto(data, address)
            print(f"Echoed back to {address}: {data.decode()}")


@pytest.fixture
def echo_server_in_thread():
    def echo_server_in_thread_func(host, port):
        server_thread = threading.Thread(target=UDP_echo_server, args=(host, port))
        server_thread.daemon = True  # Daemonize the thread so it terminates when the main thread terminates
        server_thread.start()
        return server_thread

    return echo_server_in_thread_func
