#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# ---------------------------------------------------------------------------
"""Some demonstrative code for using the metrics agent"""
# ---------------------------------------------------------------------------
import logging
import os
import asyncio

from custom_logging import setup_logger, ColoredLogFormatter
from metrics_agent import MetricsAgent
from metrics_agent.db_client import InfluxDatabaseClient


def setup_logging(client):
    import sys

    # Setup logging
    script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    debug_file_handler = logging.FileHandler(filename=f"logs/{script_name}.debug.log")
    debug_file_handler.setLevel(logging.DEBUG)

    info_file_handler = logging.FileHandler(filename=f"logs/{script_name}.info.log")
    info_file_handler.setLevel(logging.INFO)

    # set the log format for the handlers
    console_handler.setFormatter(
        ColoredLogFormatter(
            fmt="%(asctime)s,%(msecs)d - %(name)s - %(levelname)-8s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    debug_file_handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s,%(msecs)d - %(name)s - %(levelname)-8s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    #info_file_handler.setFormatter(
    #     logging.Formatter(
    #         fmt="%(asctime)s,%(msecs)d - %(name)s - %(levelname)-8s - %(message)s",
    #         datefmt="%Y-%m-%d %H:%M:%S",
    #     )
    # )

    # Setup and assign logging handler to influxdb
    #influx_logging_handler = client.get_logging_handler()
    #influx_logging_handler.setLevel(logging.INFO)

    # create the logger
    logger = setup_logger(
        handlers=[
            console_handler,
            debug_file_handler,
            #influx_logging_handler,
            info_file_handler,
        ]
    )

    return logger


def main():

    # Create a client for the agent to write data to a database
    db_client = InfluxDatabaseClient(
        "config/influx.toml", local_tz="America/Vancouver", default_bucket="prototype-zero"
    )
    logger = setup_logging(db_client._client)

    # create the agent and assign it the client and desired aggregator, as well as the desired interval for updating the database
    agent = MetricsAgent(
        db_client=db_client,
    )

    asyncio.run(agent.node_client.request_data_periodically())


if __name__ == "__main__":
    main()
