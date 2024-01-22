#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_module_description"""
# ---------------------------------------------------------------------------

import time
import threading
import random

from aggregator import MetricsAggregatorStats
from metrics_buffer import MetricsBuffer


class MetricsAgent:
    def __init__(self, interval=10, client=None, aggregator=None):
        self.metrics_buffer = MetricsBuffer()
        self.interval = interval
        self.last_sent_time = time.time()
        self.lock = threading.Lock()  # To ensure thread safety
        self.client = client
        self.aggregator = aggregator or MetricsAggregatorStats()

    def add_metric(self, name, value):
        with self.lock:
            self.metrics_buffer.add_metric(name, value)

    def aggregate_and_send(self):
        with self.lock:
            if time.time() - self.last_sent_time >= self.interval:
                if self.metrics_buffer.not_empty():
                    # dump buffer to list of metrics
                    metrics = self.metrics_buffer.dump_buffer()
                    self.last_sent_time = time.time()
                    aggregated_metrics = self.aggregator.aggregate(metrics)
                    self.client.send(aggregated_metrics)

    def start_aggregator_thread(self):
        aggregator_thread = threading.Thread(target=self.run_aggregator, daemon=True)
        aggregator_thread.start()

    def run_aggregator(self):
        while True:
            self.aggregate_and_send()
            time.sleep(1)  # Adjust sleep time as needed


def main():
    from database_client import InfluxDBClient

    client = InfluxDBClient("config/config.toml", local_tz="America/Vancouver")

    # Example usage
    metrics_agent = MetricsAgent(
        interval=2, client=client, aggregator=MetricsAggregatorStats()
    )

    # Start the aggregator thread
    metrics_agent.start_aggregator_thread()

    while True:
        n = 1000
        val_max = 1000
        # Simulating metric collection
        for i in range(n):
            metric_value = random.randint(1, val_max)
            metrics_agent.add_metric(name="random data", value=metric_value)

        for i in range(n):
            metric_value = random.randint(1, val_max)
            metrics_agent.add_metric(name="random data2", value=metric_value)

        # Simulating metric collection
        for i in range(n):
            metric_value = random.randint(1, val_max)
            metrics_agent.add_metric(name="random data3", value=metric_value)

        for i in range(n):
            metric_value = random.randint(1, val_max)
            metrics_agent.add_metric(name="random data4", value=metric_value)

    # metrics_agent.aggregate_and_send()


if __name__ == "__main__":
    main()
