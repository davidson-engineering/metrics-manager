from fast_influxdb_client import FastInfluxDBClient, InfluxMetric


@pytest.fixture
def influxdb_client():
    return FastInfluxDBClient.from_config_file('config_test.toml')