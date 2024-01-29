import pytest

from datetime import datetime, timedelta, timezone


from agent import Metric


@pytest.fixture
def metrics_agent():
    from agent.agent import MetricsAgent
    from agent.db_client import InfluxDatabaseClient
    from agent.aggregator import MetricsAggregatorStats

    def metrics_agent_func(interval=1):
        client = InfluxDatabaseClient(
            config="test/influxdb_testing_config.toml", local_tz="America/Vancouver"
        )
        metrics_agent = MetricsAgent(
            interval=interval,
            client=client,
            aggregator=MetricsAggregatorStats(),
            autostart=False,
        )
        return metrics_agent

    return metrics_agent_func


def test_db_client(metrics_agent):
    agent = metrics_agent()
    assert agent.client._client.ping() is True


def run_aggregator_test(agent, dataset):
    from agent.aggregator import MetricsAggregatorStats
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


def test_metrics_aggregator_stats_medium(metrics_agent, random_dataset_2_chunked):
    interval = 1
    agent = metrics_agent(interval=interval)
    run_aggregator_test(agent=agent, dataset=random_dataset_2_chunked)


def test_metrics_aggregator_stats_large(metrics_agent, random_dataset_3_chunked):
    interval = 1
    agent = metrics_agent(interval=interval)
    run_aggregator_test(agent=agent, dataset=random_dataset_3_chunked)
