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
import yaml
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from buffered.buffer import Buffer
from metrics_agent.db_client import DatabaseClient
from application_metrics import SessionMetrics, ApplicationMetrics
from metrics_agent.exceptions import ConfigFileDoesNotExist

logger = logging.getLogger(__name__)

BUFFER_LENGTH = 16_384
STATS_UPLOAD_ENABLED = True
STATS_UPLOAD_INTERVAL_SECONDS = 60
BATCH_SIZE_SENDING = 5000
BATCH_SIZE_PROCESSING = 1000
UPDATE_INTERVAL_SECONDS = 10


def load_toml_file(filepath):
    with open(filepath, mode="rb") as fp:
        return tomllib.load(fp)


def read_yaml_file(filepath):
    with open(filepath, "r") as file:
        return yaml.safe_load(file)


def csv_to_metrics(csv_filepath):
    import pandas as pd
    import numpy as np

    df = pd.read_csv(csv_filepath)
    # Convert 'Time' column to integer
    df["time"] = df["time"].astype(int)

    # Convert 'nan' strings to actual NaN values
    df.replace("nan", np.nan, inplace=True)

    # Convert DataFrame to a list of dictionaries
    metrics = []
    for _, row in df.iterrows():
        metric = {"time": row["time"], "fields": row.drop("time").to_dict()}
        metrics.append(metric)
    # Convert DataFrame to a list of dictionaries
    return metrics


@dataclass
class MetricsAgentStatistics(ApplicationMetrics):
    name: str = "agent-statistics"
    class_: str = "metrics_agent"
    instance_id: int = 0
    hostname: str = "gfyvrdatadash"
    metrics_received: int = 0
    metrics_sent: int = 0
    metrics_failed: int = 0
    metrics_buffered: int = 0
    metrics_dropped: int = 0
    metrics_processed: int = 0


class MetricsAgent:
    """

    An agent for collecting, processing and sending metrics to a database

    :param interval: The interval at which the agent will aggregate and send metrics to the database
    :param server: Whether to start a server to receive metrics from other agents
    :param client: The client to send metrics to
    :param aggregator: The aggregator to use to aggregate metrics
    :param autostart: Whether to start the aggregator thread automatically

    """

    def __init__(
        self,
        db_client: DatabaseClient,
        processors: Union[list, tuple] = None,
        autostart: bool = True,
        update_interval: float = None,
        config: Union[dict, str] = None,
    ):

        # Setup Agent
        # *************************************************************************
        # Parse configuration from file
        if isinstance(config, str):
            if not Path(config).exists():
                raise ConfigFileDoesNotExist
            config = load_toml_file(config)

        # If no configuation specified, then set as blank dict so default values will be used
        config = config or {}

        self.update_interval = update_interval or config.get(
            "update_interval", UPDATE_INTERVAL_SECONDS
        )

        config_stats = config.get("statistics", {})
        self.stats_upload_enabled = config_stats.get("enabled", STATS_UPLOAD_ENABLED)
        self.stats_update_interval = config_stats.get(
            "update_interval", STATS_UPLOAD_INTERVAL_SECONDS
        )

        buffer_length = config.get("buffer_length", BUFFER_LENGTH)

        config_db_client = config.get("db_client", {})
        self.batch_size_sending = config_db_client.get("batch_size", BATCH_SIZE_SENDING)

        config_processing = config.get("processing", {})
        self.batch_size_processing = config_processing.get(
            "batch_size", BATCH_SIZE_PROCESSING
        )

        # Set up the agent buffers
        self._input_buffer = Buffer(maxlen=buffer_length)
        self._send_buffer = Buffer(maxlen=buffer_length)

        # Initialize the last sent time
        self._last_sent_time: float = time.time()
        self.db_client = db_client
        self.processors = processors

        # Setup agent statistics for monitoring
        # *************************************************************************
        self.session_stats = SessionMetrics(
            total_stats=MetricsAgentStatistics(),
            period_stats=MetricsAgentStatistics(),
        )

        if autostart:
            self.start()

    def add_metric_to_queue(
        self, measurement: str, fields: dict, time: int = None, **kwargs
    ):
        metric = dict(measurement=measurement, fields=fields, time=time, **kwargs)
        self._input_buffer.add(metric)
        logger.debug(f"Added metric to buffer: {measurement}={fields}")
        self.session_stats.increment("metrics_received")

    def process(self):
        while self._input_buffer.not_empty():
            # dump buffer to list of metrics
            metrics = self._input_buffer.dump(self.batch_size_processing)
            for processor in self.processors:
                logger.debug(f"Processing metrics using {processor}")
                metrics = processor.process(metrics)
            self._last_sent_time = time.time()
            self._send_buffer.add(metrics)
            self.session_stats.increment("metrics_processed", len(metrics))

    def passthrough(self):
        # If no post processors are defined, pass through the input buffer to the send buffer
        while self._input_buffer.not_empty():
            self._send_buffer.add(next(self._input_buffer))
            self.session_stats.increment("metrics_processed")

    def send_to_database(self, metrics_to_send):
        # Send the metrics in the send buffer to the database
        if metrics_to_send:
            self.db_client.send(metrics_to_send)
            logger.info(f"Sent {len(metrics_to_send)} metrics to database")
            self.session_stats.increment("metrics_sent", len(metrics_to_send))

    # Thread management methods
    # *************************************************************************
    def start_processing_thread(self):
        self.processing_thread = threading.Thread(
            target=self.run_processing, daemon=True
        )
        self.processing_thread.start()
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
            time.sleep(self.stats_update_interval)

    def run_processing(self):
        while True:
            if self.processors:
                self.process()
            else:
                self.passthrough()

            time.sleep(self.update_interval)  # Adjust sleep time as needed

    def run_sending(self):
        while True:
            # Dump metrics from buffer, and send to the database client
            metrics_to_send = self._send_buffer.dump(maximum=self.batch_size_sending)
            self.send_to_database(metrics_to_send)
            time.sleep(self.update_interval)

    def stop_processing_thread(self):
        self.processing_thread.join()
        logger.debug("Stopped processing thread")

    def stop_sending_thread(self):
        self.sending_thread.join()
        logger.debug("Stopped sending thread")

    def start(self):
        self.start_processing_thread()
        self.start_sending_thread()
        if self.stats_upload_enabled:
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
        logger.info(f"Metrics agent {self} destroyed")


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
