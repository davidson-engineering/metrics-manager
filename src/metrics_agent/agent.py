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
from pathlib import Path

from buffered.buffer import Buffer

from metrics_agent.exceptions import ConfigFileDoesNotExist

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fast_database_clients.fast_database_client import DatabaseClientBase

logger = logging.getLogger(__name__)

BUFFER_LENGTH = 65_536
STATS_UPLOAD_ENABLED = True
STATS_UPLOAD_INTERVAL_SECONDS = 60
BATCH_SIZE_SENDING = 5_000
BATCH_SIZE_PROCESSING = 1000
UPDATE_INTERVAL_SECONDS = 10


def load_config(filepath: Union[str, Path]) -> dict:
    if isinstance(filepath, str):
        filepath = Path(filepath)

    if not Path(filepath).exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    # if extension is .json
    if filepath.suffix == ".json":
        import json

        with open(filepath, "r") as file:
            return json.load(file)

    # if extension is .yaml
    if filepath.suffix == ".yaml":
        import yaml

        with open(filepath, "r") as file:
            return yaml.safe_load(file)
    # if extension is .toml
    if filepath.suffix == ".toml":
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib

        with open(filepath, "rb") as file:
            return tomllib.load(file)

    # else load as binary
    with open(filepath, "rb") as file:
        return file.read()


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
        database_client,
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
            config = load_config(config)

        # If no configuation specified, then set as blank dict so default values will be used
        config = config or {}

        self.update_interval = update_interval or config.get(
            "update_interval", UPDATE_INTERVAL_SECONDS
        )

        buffer_length = config.get("buffer_length", BUFFER_LENGTH)

        config_database_client = config.get("db_client", {})
        self.batch_size_sending = config_database_client.get(
            "batch_size", BATCH_SIZE_SENDING
        )

        config_processing = config.get("processing", {})
        self.batch_size_processing = config_processing.get(
            "batch_size", BATCH_SIZE_PROCESSING
        )

        # Set up the agent buffers
        self._input_buffer = Buffer(maxlen=buffer_length)
        self._send_buffer = Buffer(maxlen=buffer_length)

        # Initialize the last sent time
        self._last_sent_time: float = time.time()
        self.database_client = database_client
        self.processors = processors

        if autostart:
            self.start()

    def add_metric_to_queue(
        self, measurement: str, fields: dict, time: int = None, **kwargs
    ):
        metric = dict(measurement=measurement, fields=fields, time=time, **kwargs)
        self._input_buffer.put(metric)
        logger.debug(f"Added metric to buffer: {measurement}={fields}")
        # self.session_stats.increment("metrics_received")

    def process(self):
        while self._input_buffer.not_empty():
            # dump buffer to list of metrics
            metrics = self._input_buffer.dump(self.batch_size_processing)
            for processor in self.processors:
                logger.debug(f"Processing metrics using {processor}")
                metrics = processor.process(metrics)
            number_metrics_processed = len(metrics)
            self._last_sent_time = time.time()
            self._send_buffer.put(metrics)
            # self.session_stats.increment("metrics_processed", number_metrics_processed)

    def passthrough(self):
        # If no post processors are defined, pass through the input buffer to the send buffer
        while self._input_buffer.not_empty():
            self._send_buffer.put(next(self._input_buffer))
            # self.session_stats.increment("metrics_processed")

    from prometheus_client import start_http_server, Summary

    def send_to_database(self, metrics_to_send):
        # Send the metrics in the send buffer to the database
        if metrics_to_send:
            number_metrics_sent = len(metrics_to_send)
            self.database_client.write(metrics_to_send)
            logger.info(f"Sent {number_metrics_sent} metrics to database")
            # self.session_stats.increment("metrics_sent", number_metrics_sent)

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

    # def start_session_stats_thread(self):
    #     self.session_stats_thread = threading.Thread(
    #         target=self.update_session_stats, daemon=True
    #     )
    #     self.session_stats_thread.start()
    #     logger.debug("Started session stats thread")

    # def update_session_stats(self):
    #     while True:
    #         self._input_buffer.put(self.session_stats.build_metrics())
    #         time.sleep(self.stats_update_interval)

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
            metrics_to_send = self._send_buffer.dump(max=self.batch_size_sending)
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
        # if self.stats_upload_enabled:
        # self.start_session_stats_thread()
        return self

    # Buffer management methods
    # *************************************************************************

    def clear_input_buffer(self):
        self._input_buffer.clear()

    def get_input_buffer_size(self):
        return self._input_buffer.size()

    def run_until_buffer_empty(self):
        while self._input_buffer.not_empty():
            time.sleep(self.update_interval)
        logger.debug("Buffer is empty")

    def __repr__(self):
        return f"{self.__class__.__name__}({self.database_client})"

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
    from fast_database_clients.fast_influxdb_client import FastInfluxDBClient
    from node_client import NodeSwarmClient
    from network_simple import SimpleServerTCP
    from metrics_agent.processors import (
        JSONReader,
        Formatter,
        TimeLocalizer,
        ExpandFields,
        TimePrecision,
    )

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)

    config = load_config("config/application.toml")

    # Create a client for the agent to write data to a database
    database_client = FastInfluxDBClient.from_config_file(
        config_file="config/influx_test.toml"
    )

    # create the agent and assign it the client and desired processors
    agent = MetricsAgent(
        database_client=database_client,
        processors=[
            JSONReader(),
            TimeLocalizer(),
            TimePrecision(),
            ExpandFields(),
            Formatter(),
        ],
        config=config["agent"],
    )

    # Start TCP Server

    server_address = (
        config["server"]["host"],
        config["server"]["port"],
    )

    server_tcp = SimpleServerTCP(
        output_buffer=agent._input_buffer,
        server_address=server_address,
    )

    # Start up the server to expose the metrics.
    start_http_server(8000)
    # Generate some requests.
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
