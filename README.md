# Metrics Manager

`metrics-manager` is a Python-based tool for managing and monitoring various metrics in real-time. This tool is ideal for collecting, processing, storing, and visualizing experimental data from multiple sources in a flexible and scalable manner. Built with Docker orchestration and designed for asynchronous operation, it enables high-performance data handling across projects.

## Features

- **Real-Time Data Collection**: Continuously gathers metrics from various sources.
- **Asynchronous Processing**: Processes data asynchronously to ensure low-latency, high-throughput operation.
- **Flexible Storage**: Easily integrates with multiple storage backends to meet different data persistence needs.
- **Visualization Capabilities**: Includes built-in options to visualize metrics for easy monitoring and analysis.
- **Docker Orchestration**: Uses Docker for easy deployment and scalability.

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/davidson-engineering/metrics-manager.git
   cd metrics-manager
   ```

2. **Install Dependencies**:
   Ensure you have Python 3.7+ and Docker installed, then install Python dependencies:
   ```bash
   pip install .
   ```

3. **Set Up Docker Containers**:
   Build and start the Docker containers:
   ```bash
   docker-compose up --build
   ```

## Usage

### Basic Setup

1. **Initialize Metrics Manager**:
   ```python
   from metrics_manager import MetricsManager

   manager = MetricsManager()
   ```

2. **Configure Data Sources**:
   Add data sources to the manager, defining metric names, sources, and update intervals as needed:
   ```python
   manager.add_metric("CPU_Usage", source="system", interval=1)
   manager.add_metric("Memory_Usage", source="system", interval=1)
   ```

3. **Start Collecting Metrics**:
   Start the manager to begin real-time data collection and processing.
   ```python
   manager.start()
   ```

### Data Storage

`metrics-manager` supports flexible storage configurations. By default, it uses an in-memory database, but it can be configured to use external databases like PostgreSQL or MongoDB for persistent storage. To specify a storage backend, update the configuration file in `config/storage.yaml`.

### Visualization

Metrics Manager includes visualization capabilities to graphically represent metrics over time. Use built-in functions to render graphs directly in your application or output the data to a dashboard:

```python
from metrics_manager.visualize import plot_metrics

plot_metrics("CPU_Usage", timeframe="last_hour")
```

### Docker Deployment

To deploy `metrics-manager` in a Docker environment, use `docker-compose.yml` provided in the repository. This configuration handles dependencies and ensures each component of the manager is properly orchestrated for seamless data collection, storage, and visualization.

## Customization

You can customize Metrics Manager by adding:
- **New Metrics**: Define custom metrics with unique sources and intervals.
- **Custom Storage Backends**: Update `storage.yaml` to connect to your preferred storage solution.
- **Visualization Options**: Configure visualization settings in `config/visualize.yaml` for tailored graphs and outputs.

## License

This project is licensed under the MIT License.
