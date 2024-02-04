#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------
"""An agent for collecting, aggregating and sending metrics to a database"""
# ---------------------------------------------------------------------------

import time
import threading
import logging

from metrics_agent.aggregator import MetricsAggregatorStats
from buffered.buffer import Buffer
from network_simple.server import SimpleServerTCP
from metrics_agent.db_client import DatabaseClient

logger = logging.getLogger(__name__)

BUFFER_LENGTH_AGENT = 8192
BUFFER_LENGTH_SERVER = 8192


class DataFormatException(Exception):
    pass


class MetricsAgent:
    """

    An agent for collecting, processing and sending metrics to a database

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
        self._input_buffer = Buffer(maxlen=buffer_length or BUFFER_LENGTH_AGENT)
        self._send_buffer = Buffer(maxlen=buffer_length or BUFFER_LENGTH_AGENT)
        self._last_sent_time = time.time()
        self._lock = threading.Lock()  # To ensure thread safety

        if server is True:
            self.server = SimpleServerTCP(
                output_buffer=self._input_buffer,
                host=host,
                port=port,
                buffer_length=buffer_length or BUFFER_LENGTH_SERVER,
            )
        else:
            self.server = server

        self.interval = interval
        self.client: DatabaseClient = client
        self.aggregator = aggregator or MetricsAggregatorStats()

        if autostart:
            self.start()

    def add_metric(self, name, value, timestamp=None):
        self._input_buffer.add((name, value, timestamp))
        logger.debug(f"Added metric to buffer: {name}={value}")

    def process_buffer(self):
        if time.time() - self._last_sent_time >= self.interval:
            if self._input_buffer.not_empty():
                # dump buffer to list of metrics
                metrics_raw = self._input_buffer.dump()
                metrics = [self.aggregator.convert(metric) for metric in metrics]
                self._last_sent_time = time.time()
                aggregated_metrics = self.aggregator.aggregate(metrics)
                self._send_buffer.add(aggregated_metrics)

    def passthrough(self):
        while self._input_buffer.not_empty():
            self._send_buffer.add(next(self._input_buffer))

    def send_to_database(self):
        processed_metrics = self._send_buffer.dump()
        self.client.send(processed_metrics)

    def start_processing_thread(self):
        self.processing_thread = threading.Thread(
            target=self.run_processing, daemon=True
        ).start()
        logger.debug("Started processing thread")

    def start_sending_thread(self):
        self.sending_thread = threading.Thread(
            target=self.run_sending, daemon=True
        ).start()
        logger.debug("Started send thread")

    def run_processing(self):
        while True:
            # self.process_buffer()
            self.passthrough()
            time.sleep(self.interval)  # Adjust sleep time as needed

    def run_sending(self):
        while True:
            self.send_to_database()
            time.sleep(self.interval)

    def stop_processing_thread(self):
        self.processing_thread.join()
        logger.debug("Stopped processing thread")

    def stop_sending_thread(self):
        self.sending_thread.join()
        logger.debug("Stopped sending thread")

    def clear_input_buffer(self):
        with self._lock:
            self._input_buffer.clear()

    def get_input_buffer_size(self):
        return self._input_buffer.get_size()

    def run_until_buffer_empty(self):
        while self._input_buffer.not_empty():
            time.sleep(self.interval)
        logger.debug("Buffer is empty")

    def start(self):
        self.start_processing_thread()
        self.start_sending_thread()
        return self

    def __del__(self):
        try:
            # This method is called when the object is about to be destroyed
            self.stop_processing_thread()
        except AttributeError:
            pass
        try:
            self.stop_sending_thread()
        except AttributeError:
            pass
        try:
            self.server.stop()
        except AttributeError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.__del__()


def main():
    from metrics_agent.db_client import InfluxDatabaseClient

    logging.basicConfig(level=logging.DEBUG)

    client = InfluxDatabaseClient("config/influx.toml", local_tz="America/Vancouver")

    # Example usage
    metrics_agent = MetricsAgent(
        interval=1, client=client, aggregator=MetricsAggregatorStats(), server=True
    )

    # n = 10_000
    # # Simulating metric collection
    # for _ in range(n):
    #     metrics_agent.add_metric(name="queries", value=True)
    while True:
        # Wait for the agent to finish sending all metrics to the database before ending the program
        metrics_agent.run_until_buffer_empty()
        time.sleep(1)


if __name__ == "__main__":
    main()
