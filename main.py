#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# ---------------------------------------------------------------------------
"""Some demostrative code for using the metrics agent"""
# ---------------------------------------------------------------------------


def main():
    import time
    import random

    from metrics_agent import MetricsAgent, MetricsAggregatorStats
    from metrics_agent.db_client import InfluxDatabaseClient

    # Create a client for the agent to write data to a database
    client = InfluxDatabaseClient("config/config.toml", local_tz="America/Vancouver")

    # create the agent and assign it the client and desired aggregator, as well as the desired interval for updating the database
    metrics_agent = MetricsAgent(
        interval=2, client=client, aggregator=MetricsAggregatorStats()
    )

    # Start the aggregator thread. The agent will automatically start aggregating and sending data to the database at the specified interval
    metrics_agent.start_aggregator_thread()

    # Simulating metric collection
    n = 1000
    val_max = 1000
    for _ in range(n):
        metric_value = random.randint(1, val_max)
        metrics_agent.add_metric(name="random data", value=metric_value)

    # Wait for the agent to finish sending all metrics to the database before ending the program
    while metrics_agent.metrics_buffer.not_empty():
        time.sleep(metrics_agent.interval)


if __name__ == "__main__":
    main()
