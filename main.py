#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# ---------------------------------------------------------------------------
"""Some demonstrative code for using the metrics agent"""
# ---------------------------------------------------------------------------
from logging.config import dictConfig
import asyncio

from buffered import Buffer
from data_node_network.node_client import Node, NodeClientTCP
from data_node_network.configuration import node_config
from metrics_processor import MetricsProcessor
from fast_database_clients.fast_influxdb_client import FastInfluxDBClient
from metrics_processor import (
    JSONReader,
    Formatter,
    TimeLocalizer,
    ExpandFields,
    TimePrecision,
)
from metrics_processor import load_config
from network_simple import SimpleServerTCP


def setup_logging(filepath="config/logger.yaml"):
    import yaml
    from pathlib import Path

    if Path(filepath).exists():
        with open(filepath, "r") as stream:
            config = yaml.load(stream, Loader=yaml.FullLoader)
    else:
        raise FileNotFoundError
    with Path("logs/") as p:
        p.mkdir(exist_ok=True)
    logger = dictConfig(config)
    return logger


def main():

    config = load_config("config/application.toml")

    processing_buffer = Buffer(maxlen=config["processor"]["input_buffer_length"])
    database_buffer = Buffer(maxlen=config["processor"]["output_buffer_length"])

    # Create a TCP Server
    server_address = (
        config["server"]["host"],
        config["server"]["port"],
    )
    manager_server_tcp = SimpleServerTCP(
        output_buffer=processing_buffer,
        server_address=server_address,
    )

    # Create a client to gather data from the data nodes
    node_client: NodeClientTCP = NodeClientTCP(
        nodes=node_config, buffer=processing_buffer
    )

    # Create a metrics processor for the data pipeline
    metrics_processor = MetricsProcessor(
        input_buffer=processing_buffer,
        output_buffer=database_buffer,
        pipelines=[
            JSONReader(),
            TimeLocalizer(),
            TimePrecision(),
            ExpandFields(),
            Formatter(),
        ],
        config=config["processor"],
    )

    # Create a client to write metrics to an InfluxDB database
    database_client = FastInfluxDBClient.from_config_file(
        input_buffer=database_buffer, config_file="config/influx_test.toml"
    )

    # Start the node client last, as it will start the event loop and block
    node_client.start(interval=config["node_network"]["node_client"])


if __name__ == "__main__":

    setup_logging()
    main()
