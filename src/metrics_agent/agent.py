#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# ---------------------------------------------------------------------------
"""An agent for collecting, aggregating and sending metrics to a database"""
# ---------------------------------------------------------------------------

import time
import threading
import logging

from metrics_agent.aggregator import MetricsAggregatorStats
from metrics_agent.buffer.buffer import MetricsBuffer
from metrics_agent.network_sync import AgentServerTCP
from metrics_agent.db_client import DatabaseClient

logger = logging.getLogger(__name__)

BUFFER_LENGTH_AGENT = 8192
BUFFER_LENGTH_SERVER = 8192


class DataFormatException(Exception):
    pass


class MetricsAgent:
    """

    An agent for collecting, aggregating and sending metrics to a database

    :param interval: The interval at which the agent will aggregate and send metrics to the database
    :param server: Whether to start a server to receive metrics from other agents
    :param client: The client to send metrics to
    :param aggregator: The aggregator to use to aggregate metrics
    :param autostart: Whether to start the aggregator thread automatically
    :param port: The port to start the server on
    :param host: The host to start the server on

    """

    def __init__(
        self,
        interval=10,
        server=None,
        client=None,
        aggregator=None,
        autostart=True,
        port=9000,
        host="localhost",
        buffer_length=None,
    ):
        self._metrics_buffer = MetricsBuffer(
            maxlen=buffer_length or BUFFER_LENGTH_AGENT
        )
        self._last_sent_time = time.time()
        self._lock = threading.Lock()  # To ensure thread safety

        if server is True:
            self.server = AgentServerTCP(
                output_buffer=self._metrics_buffer,
                host=host,
                port=port,
                buffer_length=buffer_length or BUFFER_LENGTH_SERVER,
            )
            self.server.daemon = True

        else:
            self.server = server

        self.interval = interval
        self.client: DatabaseClient = client
        self.aggregator = aggregator or MetricsAggregatorStats()

        if autostart:
            self.start_aggregator_thread()
            logger.info("Started aggregator thread")

    def add_metric(self, name, value, timestamp=None):
        self._metrics_buffer.add((name, value, timestamp))
        logger.debug(f"Added metric to buffer: {name}={value}")

    def aggregate_and_send(self):
        if time.time() - self._last_sent_time >= self.interval:
            if self._metrics_buffer.not_empty():
                # dump buffer to list of metrics
                metrics = self._metrics_buffer.dump()
                self._last_sent_time = time.time()
                aggregated_metrics = self.aggregator.aggregate(metrics)
                self.client.send(aggregated_metrics)

    def start_aggregator_thread(self):
        self.aggregator_thread = threading.Thread(
            target=self.run_aggregator, daemon=True
        ).start()
        logger.debug("Started aggregator thread")

    def run_aggregator(self):
        while True:
            self.aggregate_and_send()
            time.sleep(self.interval)  # Adjust sleep time as needed

    def stop_aggregator_thread(self):
        self.aggregator_thread.join()
        logger.debug("Stopped aggregator thread")

    def clear_metrics_buffer(self):
        with self._lock:
            self._metrics_buffer.clear()

    def get_metrics_buffer_size(self):
        return self._metrics_buffer.get_size()

    def run_until_buffer_empty(self):
        while self._metrics_buffer.not_empty():
            time.sleep(self.interval)
        logger.debug("Buffer is empty")

    def start(self):
        self.start_aggregator_thread()
        return self

    def __del__(self):
        try:
            # This method is called when the object is about to be destroyed
            self.stop_aggregator_thread()
            logger.debug("Stopped aggregator thread")
        except AttributeError:
            pass
        try:
            self.server.stop()
            logger.debug("Stopped server thread")
        except AttributeError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.__del__()


def main():
    from metrics_agent.db_client import InfluxDatabaseClient

    client = InfluxDatabaseClient("config/influx.toml", local_tz="America/Vancouver")

    # Example usage
    metrics_agent = MetricsAgent(
        interval=1, client=client, aggregator=MetricsAggregatorStats(), server=True
    )

    n = 10_000
    # Simulating metric collection
    for _ in range(n):
        metrics_agent.add_metric(name="queries", value=True)
    while True:
        # Wait for the agent to finish sending all metrics to the database before ending the program
        metrics_agent.run_until_buffer_empty()
        time.sleep(1)


if __name__ == "__main__":
    main()
