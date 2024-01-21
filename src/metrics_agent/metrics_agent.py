#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_module_description"""
# ---------------------------------------------------------------------------

from dataclasses import dataclass
import time
import threading
from datetime import datetime, timezone

import aggregator as ma
import transmitter as mt
import metrics_buffer as mb



        
class MetricsAgent:
    def __init__(self, interval=10, transmitter=None, aggregator=None):
        self.metrics_buffer = mb.MetricsBuffer()
        self.interval = interval
        self.last_sent_time = time.time()
        self.lock = threading.Lock()  # To ensure thread safety
        self.transmitter = transmitter
        self.aggregator = aggregator or ma.AverageMetricsAggregator()

    def add_metric(self, name, value):
        with self.lock:
            self.metrics_buffer.add_metric(name, value)

    def aggregate_and_send(self):
        with self.lock:
            if time.time() - self.last_sent_time >= self.interval:
                if self.metrics_buffer.isnotempty():
                    # Perform aggregation (e.g., averaging for simplicity)
                    aggregated_metrics = self.aggregator(self.metrics_buffer.get_sorted_metrics())

                    # Save to database (replace this with your database operation)
                    self.transmitter.transmit(aggregated_metrics)

                    # Reset metrics list and update last sent time
                    self.metrics_buffer.clear_buffer()

                    self.last_sent_time = time.time()
    

    def start_aggregator_thread(self):
        aggregator_thread = threading.Thread(target=self.run_aggregator, daemon=True)
        aggregator_thread.start()

    def run_aggregator(self):
        while True:
            self.aggregate_and_send()
            time.sleep(1)  # Adjust sleep time as needed


def main():

    from transmitter import InfluxDBMetricsTransmitter
    client = InfluxDBMetricsTransmitter("config/config.toml")

    # Example usage
    metrics_agent = MetricsAgent(interval=3, transmitter=client)

    # Start the aggregator thread
    metrics_agent.start_aggregator_thread()

    for i in range(2):
    # Simulating metric collection
        for i in range(10):
            metric_value = i * 2
            metrics_agent.add_metric(name="random data", value=metric_value)

        for i in range(10):
            metric_value = i * 2
            metrics_agent.add_metric(name="random data2", value=metric_value)

    # Ensure any remaining metrics are sent
    metrics_agent.aggregate_and_send()

    time.sleep(3)  # Wait for the aggregator thread to finish



    metrics_agent.aggregate_and_send()
    
    time.sleep(3)  # Wait for the aggregator thread to finish



if __name__ == "__main__":
    main()