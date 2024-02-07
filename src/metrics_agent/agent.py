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
import json

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from buffered.buffer import Buffer
from network_simple.server import SimpleServerTCP, SimpleServerUDP
from metrics_agent.db_client import DatabaseClient
from application_metrics import SessionMetrics, ApplicationMetrics
from metrics_agent.exceptions import DataFormatException

from metrics_agent.node_client import NodeSwarmClient

logger = logging.getLogger(__name__)


def load_toml(filepath):
    with open(filepath, mode="rb") as fp:
        return tomllib.load(fp)


def read_yaml_file(filepath):
    with open(filepath, "r") as file:
        return yaml.safe_load(file)


config = load_toml("config/application.toml")

TAGS_EXTRA_FILEPATH = "config/tags_extra.yaml"
METRIC_FORMAT_FILEPATH = "config/metric_format.yaml"


def expand_metric_dict(original_dict):

    metrics_expanded = []

    for field, value in original_dict["fields"].items():
        new_dict = {
            "measurement": original_dict["measurement"],
            "fields": {field: value},
            "tags": original_dict.get("tags", {}),
            "time": original_dict.get("time", {}),
        }
        metrics_expanded.append(new_dict)

    return metrics_expanded


def expand_metrics(metrics):
    expanded_metrics = []
    for metric in metrics:
        if isinstance(metric, str):
            # Assume metric is JSON. Convert to dict
            metric = json.loads(metric)
        expanded_metric = expand_metric_dict(metric)
        expanded_metrics.extend(expanded_metric)
    return expanded_metrics


# def create_mapping_dict(source_dict):

#     # Initialize an empty mapping dictionary
#     mapping_dict = {}

#     # Iterate over the keys in the YAML file
#     for key, value in source_dict.items():
#         source_id = value.get("source_id", key)
#         mapping_dict[source_id] = key

#     return mapping_dict


@dataclass
class MetricsAgentStatistics(ApplicationMetrics):
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
        post_processors: Union[list, tuple] = None,
        autostart: bool = True,
        update_interval: float = None,
    ):

        # Setup Agent
        # *************************************************************************
        # Parse configuration from file

        self.stats_upload_enabled = config["statistics"]["enabled"]
        self.stats_update_interval = config["statistics"]["update_interval"]

        buffer_length_agent = config["agent"]["buffer_length"]
        buffer_length_server = config["server"]["buffer_length"]

        self.batch_size_sending = config["database_client"]["batch_size"]
        self.batch_size_post_processing = config["post_proccessing"]["batch_size"]

        server_address_tcp = (
            config["server"]["tcp"]["ip_address"],
            config["server"]["tcp"]["port"],
        )
        server_address_udp = (
            config["server"]["udp"]["ip_address"],
            config["server"]["udp"]["port"],
        )

        # Set up the agent buffers
        self._input_buffer = Buffer(maxlen=buffer_length_agent)
        self._send_buffer = Buffer(maxlen=buffer_length_agent)

        # Initialize the last sent time
        self._last_sent_time: float = time.time()
        self.update_interval = update_interval or config["agent"]["update_interval"]
        self.db_client = db_client
        self.post_processors = post_processors
        self.measurement_format = read_yaml_file(METRIC_FORMAT_FILEPATH)
        self.tags_extra = read_yaml_file(TAGS_EXTRA_FILEPATH)

        # Set up the server(s)
        # *************************************************************************
        if config["server"]["tcp"]["enabled"]:
            self.server_tcp = SimpleServerTCP(
                output_buffer=self._input_buffer,
                server_address=server_address_tcp,
                buffer_length=buffer_length_server,
            )
        else:
            self.server_tcp = None

        if config["server"]["udp"]["enabled"]:
            self.server_udp = SimpleServerUDP(
                output_buffer=self._input_buffer,
                server_address=server_address_udp,
                buffer_length=buffer_length_server,
            )
        else:
            self.server_udp = None

        # Set up an Agent to retrieve data from the Arduino nodes
        # *************************************************************************
        if config["node_agent"]["enabled"]:
            self.node_client = NodeSwarmClient(
                buffer=self._input_buffer,
                update_interval=config["node_agent"]["update_interval"],
            )

        # Setup agent statistics for monitoring
        # *************************************************************************
        self.session_stats = SessionMetrics(
            total_stats=MetricsAgentStatistics(),
            period_stats=MetricsAgentStatistics(),
        )

        if autostart:
            self.start()

    def add_metric(self, measurement: str, fields: dict, timestamp: int = None):
        self._input_buffer.add((measurement, fields, timestamp))
        logger.debug(f"Added metric to buffer: {measurement}={fields}")
        self.session_stats.increment("metrics_received")

    def post_process(self):
        while self._input_buffer.not_empty():
            # dump buffer to list of metrics
            metrics = self._input_buffer.dump(self.batch_size_post_processing)
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

    def send_to_database(self, metrics_to_send):
        # Send the metrics in the send buffer to the database
        if metrics_to_send:
            self.db_client.send(metrics_to_send)
            logger.info(f"Sent {len(metrics_to_send)} metrics to database")
            self.session_stats.increment("metrics_sent", len(metrics_to_send))

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
            time.sleep(self.stats_update_interval)

    def run_post_processing(self):
        while True:
            if self.post_processors:
                self.post_process()
            else:
                self.passthrough()

            time.sleep(self.update_interval)  # Adjust sleep time as needed

    def format_metrics(self, metrics_to_send):
        tags_extra = read_yaml_file(TAGS_EXTRA_FILEPATH)
        metric_formats = read_yaml_file(METRIC_FORMAT_FILEPATH)

        metrics_to_send = expand_metrics(metrics_to_send)

        for metric in metrics_to_send:
            for k, v in metric["fields"].items():
                try:
                    metric_format = metric_formats[k]
                except KeyError:
                    # No format specified for key, continue
                    continue
                if metric_format["type"] == "float":
                    metric["fields"][k] = float(metric["fields"][k])
                elif metric_format["type"] == "str":
                    metric["fields"][k] = str(metric["fields"][k])
                else:
                    raise ValueError("metric_format not specified")
                try:
                    metric["fields"] = {
                        metric_format["db_fieldname"]: metric["fields"][k]
                    }
                except KeyError:
                    # No database fieldname specified, use existing field name
                    continue
            metric["tags"] = metric["tags"] | metric_format["tags"] | tags_extra

        return metrics_to_send

    def run_sending(self):
        while True:
            metrics_to_send = self._send_buffer.dump(maximum=self.batch_size_sending)
            metrics_to_send = self.format_metrics(metrics_to_send)
            self.send_to_database(metrics_to_send)
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
