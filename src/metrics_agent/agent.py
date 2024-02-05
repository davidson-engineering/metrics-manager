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
from typing import Union
from dataclasses import dataclass

from metrics_agent.aggregator import MetricsAggregatorStats
from buffered.buffer import Buffer
from network_simple.server import SimpleServerTCP, SimpleServerUDP
from metrics_agent.db_client import DatabaseClient

from metrics_agent.exceptions import DataFormatException


BUFFER_LENGTH_AGENT = 8192
BUFFER_LENGTH_SERVER = 8192
BATCH_SIZE_POST_PROCESSING = 1000
BATCH_SIZE_SENDING = 1000

DEFAULT_HOSTNAME = "localhost"
DEFAULT_SERVER_PORT_TCP = 9000
DEFAULT_SERVER_PORT_UDP = 9001


@dataclass
class HostAddress:
    host: str
    port: int


DEFAULT_ADDRESS_TCP = HostAddress(DEFAULT_HOSTNAME, DEFAULT_SERVER_PORT_TCP)
DEFAULT_ADDRESS_UDP = HostAddress(DEFAULT_HOSTNAME, DEFAULT_SERVER_PORT_UDP)

logger = logging.getLogger(__name__)


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
        db_client: DatabaseClient,
        post_processors: Union[list, tuple] = None,
        autostart: bool = True,
        update_interval: float = 10,
        server_tcp: bool = False,
        address_tcp: HostAddress = DEFAULT_ADDRESS_TCP,
        server_udp: bool = False,
        address_udp: HostAddress = DEFAULT_ADDRESS_UDP,
    ):
        # Set up the buffers
        self._input_buffer = Buffer(maxlen=BUFFER_LENGTH_AGENT)
        self._send_buffer = Buffer(maxlen=BUFFER_LENGTH_AGENT)

        # Initialize the last sent time
        self._last_sent_time: float = time.time()
        # self._lock = threading.Lock()  # To ensure thread safety
        self.update_interval = update_interval
        self.db_client = db_client
        self.post_processors = post_processors

        # Set up the server(s)
        if server_tcp:
            self.server_tcp = SimpleServerTCP(
                output_buffer=self._input_buffer,
                host=address_tcp.host,
                port=address_tcp.port,
                buffer_length=BUFFER_LENGTH_SERVER,
            ).start()

        if server_udp:
            self.server_udp = SimpleServerUDP(
                output_buffer=self._input_buffer,
                host=address_udp.host,
                port=address_udp.port,
                buffer_length=BUFFER_LENGTH_SERVER,
            ).start()

        if autostart:
            self.start()

    def add_metric(self, measurement: str, fields: dict, timestamp:int=None):
        self._input_buffer.add((measurement, fields, timestamp))
        logger.debug(f"Added metric to buffer: {measurement}={fields}")

    def process_buffer(self):
        while self._input_buffer.not_empty():
            # dump buffer to list of metrics
            metrics_raw = self._input_buffer.dump()
            metrics = [self.aggregator.convert(metric) for metric in metrics]
            self._last_sent_time = time.time()
            aggregated_metrics = self.aggregator.aggregate(metrics)
            self._send_buffer.add(aggregated_metrics)

    def passthrough(self):
        # If no post processors are defined, pass through the input buffer to the send buffer
        while self._input_buffer.not_empty():
            self._send_buffer.add(next(self._input_buffer))

    def send_to_database(self):
        # Send the metrics in the send buffer to the database
        processed_metrics = self._send_buffer.dump(maximum=BATCH_SIZE_SENDING)
        self.db_client.send(processed_metrics)

    # Thread management methods
    # *************************************************************************
    def start_post_processing_thread(self):
        self.post_processing_thread = threading.Thread(
            target=self.run_post_processing, daemon=True
        )
        self.post_processing_thread.start()
        logger.debug("Started processing thread")

    def start_sending_thread(self):
        self.sending_thread = threading.Thread(
            target=self.run_sending, daemon=True
        )
        self.sending_thread.start()
        logger.debug("Started send thread")

    def run_post_processing(self):
        while True:
            if self.post_processors:
                for post_processor in self.post_processors:
                    post_processor(self._input_buffer)
            else:
                self.passthrough()

            time.sleep(self.update_interval)  # Adjust sleep time as needed

    def run_sending(self):
        while True:
            self.send_to_database()
            time.sleep(self.update_interval)

    def stop_post_processing_thread(self):
        self.post_processing_thread.join()
        logger.debug("Stopped processing thread")

    def stop_sending_thread(self):
        self.sending_thread.join()
        logger.debug("Stopped sending thread")

    def start(self):
        self.start_post_processing_thread()
        self.start_sending_thread()
        return self

    # Buffer management methods
    # *************************************************************************

    def clear_input_buffer(self):
        with self._lock:
            self._input_buffer.clear()

    def get_input_buffer_size(self):
        return self._input_buffer.get_size()

    def run_until_buffer_empty(self):
        while self._input_buffer.not_empty():
            time.sleep(self.update_interval)
        logger.debug("Buffer is empty")

    def __repr__(self):
        return f"{self.__class__.__name__}({self.db_client})"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.__del__()

    def __del__(self):  # sourcery skip: use-contextlib-suppress
        try:
            # This method is called when the object is about to be destroyed
            self.stop_post_processing_thread()
        except AttributeError:
            pass
        try:
            self.stop_sending_thread()
        except AttributeError:
            pass
        try:
            self.server_tcp.stop()
            self.server_udp.stop()
        except AttributeError:
            pass
        logger.info(f"Metrics agent {self.__repr__()} destroyed")


def main():
    from metrics_agent.db_client import InfluxDatabaseClient

    logging.basicConfig(level=logging.DEBUG)

    db_client = InfluxDatabaseClient("config/influx.toml", local_tz="America/Vancouver")

    # Example usage
    metrics_agent = MetricsAgent(
        update_interval=1, db_client=db_client
    )

    n = 100
    # Simulating metric collection
    for _ in range(n):
        metrics_agent.add_metric("queries", {"count": 100}, time.time())
    while True:
        # Wait for the agent to finish sending all metrics to the database before ending the program
        metrics_agent.run_until_buffer_empty()
        time.sleep(1)


if __name__ == "__main__":
    main()
