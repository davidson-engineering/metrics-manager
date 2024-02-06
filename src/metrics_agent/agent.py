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


from buffered.buffer import Buffer
from network_simple.server import SimpleServerTCP, SimpleServerUDP
from metrics_agent.db_client import DatabaseClient
from application_metrics import SessionMetrics, ApplicationMetrics
from metrics_agent.exceptions import DataFormatException


BUFFER_LENGTH_AGENT = 16_384
BUFFER_LENGTH_SERVER = 16_384
BATCH_SIZE_POST_PROCESSING = 1000
BATCH_SIZE_SENDING = 5000

DEFAULT_HOSTNAME = "localhost"
DEFAULT_SERVER_PORT_TCP = 9000
DEFAULT_SERVER_PORT_UDP = 9001

SESSION_STATS_UPDATE_INTERVAL = 60

DEFAULT_ADDRESS_TCP = (DEFAULT_HOSTNAME, DEFAULT_SERVER_PORT_TCP)
DEFAULT_ADDRESS_UDP = (DEFAULT_HOSTNAME, DEFAULT_SERVER_PORT_UDP)


@dataclass
class MetricsAgentStatistics(ApplicationMetrics):
    metrics_received: int = 0
    metrics_sent: int = 0
    metrics_failed: int = 0
    metrics_buffered: int = 0
    metrics_dropped: int = 0
    metrics_processed: int = 0


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
        upload_stats_enabled: bool = False,
        server_enabled_tcp: bool = False,
        server_address_tcp: tuple = DEFAULT_ADDRESS_TCP,
        server_enabled_udp: bool = False,
        server_address_udp: tuple = DEFAULT_ADDRESS_UDP,
    ):
        self.session_stats = SessionMetrics(
            total_stats=MetricsAgentStatistics(), period_stats=MetricsAgentStatistics()
        )
        self.upload_stats_enabled = upload_stats_enabled
        # Set up the buffers
        self._input_buffer = Buffer(maxlen=BUFFER_LENGTH_AGENT)
        self._send_buffer = Buffer(maxlen=BUFFER_LENGTH_AGENT)

        # Initialize the last sent time
        self._last_sent_time: float = time.time()
        self.update_interval = update_interval
        self.db_client = db_client
        self.post_processors = post_processors

        # Set up the server(s)
        if server_enabled_tcp:
            self.server_tcp = SimpleServerTCP(
                output_buffer=self._input_buffer,
                server_address=server_address_tcp,
                buffer_length=BUFFER_LENGTH_SERVER,
            )
        else:
            self.server_tcp = None

        if server_enabled_udp:
            self.server_udp = SimpleServerUDP(
                output_buffer=self._input_buffer,
                server_address=server_address_udp,
                buffer_length=BUFFER_LENGTH_SERVER,
            )
        else:
            self.server_udp = None

        if autostart:
            self.start()

    def add_metric(self, measurement: str, fields: dict, timestamp: int = None):
        self._input_buffer.add((measurement, fields, timestamp))
        logger.debug(f"Added metric to buffer: {measurement}={fields}")
        self.session_stats.increment("metrics_received")

    def post_process(self):
        while self._input_buffer.not_empty():
            # dump buffer to list of metrics
            metrics = self._input_buffer.dump(BATCH_SIZE_POST_PROCESSING)
            for post_processor in self.post_processors:
                metrics = [post_processor.process(metric) for metric in metrics]
            self._last_sent_time = time.time()
            self._send_buffer.add(metrics)
            self.session_stats.increment("metrics_processed", len(metrics))

    def passthrough(self):
        # If no post processors are defined, pass through the input buffer to the send buffer
        while self._input_buffer.not_empty():
            self._send_buffer.add(next(self._input_buffer))
            self.session_stats.increment("metrics_processed")

    def send_to_database(self):
        # Send the metrics in the send buffer to the database
        processed_metrics = self._send_buffer.dump(maximum=BATCH_SIZE_SENDING)
        if processed_metrics:
            self.db_client.send(processed_metrics)
            logger.info(f"Sent {len(processed_metrics)} metrics to database")
            self.session_stats.increment("metrics_sent", len(processed_metrics))

    # Thread management methods
    # *************************************************************************
    def start_post_processing_thread(self):
        self.post_processing_thread = threading.Thread(
            target=self.run_post_processing, daemon=True
        )
        self.post_processing_thread.start()
        logger.debug("Started processing thread")

    def start_sending_thread(self):
        self.sending_thread = threading.Thread(target=self.run_sending, daemon=True)
        self.sending_thread.start()
        logger.debug("Started send thread")

    def start_session_stats_thread(self):
        self.session_stats_thread = threading.Thread(
            target=self.update_session_stats, daemon=True
        )
        self.session_stats_thread.start()
        logger.debug("Started session stats thread")

    def update_session_stats(self):
        while True:
            self.db_client.send([self.session_stats.build_metrics()])
            if self.server_tcp:
                self.db_client.send([self.server_tcp.session_stats.build_metrics()])
            if self.server_udp:
                self.db_client.send([self.server_udp.session_stats.build_metrics()])
            time.sleep(SESSION_STATS_UPDATE_INTERVAL)

    def run_post_processing(self):
        while True:
            if self.post_processors:
                self.post_process()
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
        if self.upload_stats_enabled:
            self.start_session_stats_thread()
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
    metrics_agent = MetricsAgent(update_interval=1, db_client=db_client)

    n = 10000
    # Simulating metric collection
    for _ in range(n):
        metrics_agent.add_metric("queries", {"count": 10}, time.time())
    while True:
        # Wait for the agent to finish sending all metrics to the database before ending the program
        metrics_agent.run_until_buffer_empty()
        time.sleep(1)


if __name__ == "__main__":
    main()
