# metrics-agent
### An agent to aggregate metrics before sending to a database

Reduces the volume of metrics stored in the databse when high metric traffic is required, such as event logging.


```python
import time
import random

from metrics_agent import MetricsAgent, MetricsAggregatorStats
from src.metrics_agent.database_client import InfluxDBClient

# Create a client for the agent to write data to a database
client = InfluxDBClient("config/config.toml", local_tz="America/Vancouver")

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
```
