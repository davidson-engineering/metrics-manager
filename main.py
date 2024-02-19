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

from data_node_network.node_client import Node, NodeClientTCP
from data_node_network.configuration import node_config
from metrics_agent import MetricsAgent
from fast_database_clients.fast_influxdb_client import FastInfluxDBClient
from metrics_agent import (
    JSONReader,
    Formatter,
    TimeLocalizer,
    ExpandFields,
    TimePrecision,
)


def setup_logging(filepath="config/logger.yaml"):
    import yaml
    from pathlib import Path

    if Path(filepath).exists():
        with open(filepath, "r") as stream:
            config = yaml.load(stream, Loader=yaml.FullLoader)
    else:
        raise FileNotFoundError
    logger = dictConfig(config)
    return logger

def main():

    from metrics_agent import load_config
    from network_simple import SimpleServerTCP

    config = load_config("config/application.toml")

    # Create a client for the agent to write data to a database
    database_client = FastInfluxDBClient.from_config_file(
        config_file="config/influx_test.toml"
    )
    logger = setup_logging(database_client)

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

    # Create a client
    node_list = [Node(node) for node in node_config.values()]
    node_client: NodeClientTCP = NodeClientTCP(node_list, buffer=agent._input_buffer)
    
    node_client.start(interval=config["node_network"]["node_client"])



if __name__ == "__main__":
    
    setup_logging()
    main()
