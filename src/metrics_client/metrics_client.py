import socket
from metrics_agent.buffer import Buffer
from datetime import datetime, timezone
from typing import Union
import socket
import time
import threading
import logging

try:
    import tomllib
except ImportError:
    import tomli as tomllib

logger = logging.getLogger(__name__)


def load_config(config_path):
    with open(config_path, "r") as f:
        config = tomllib.load(f)
    return config


class MetricsClient:
    def __init__(self, config: dict, autostart=False, update_interval=1):
        self.config = config
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.buffer = Buffer()
        self._lock = threading.Lock()  # To ensure thread safety
        self.run_client_thread = threading.Thread(target=self.run_client, daemon=True)
        self.update_interval = update_interval
        if autostart:
            self.start()

    def add_metric(self, name: str, value: float, timestamp: Union[float, datetime]):
        with self._lock:
            if isinstance(timestamp, datetime):
                timestamp = timestamp.timestamp()
            data = (name, value, timestamp)
            self.buffer.add(data)
            logger.debug(f"Added data to client buffer: {name}={value}")

    def send(self):
        with self._lock:
            data = self.buffer.dump_buffer()
            for el in data:
                datastring = ",".join(map(str, el))
                logger.debug(f"Sending data: {datastring}")
                self._socket.sendto(
                    datastring.encode(),
                    (self.config["host"], self.config["port"]),
                )

    def run_client(self):
        while True:
            self.send()
            time.sleep(self.update_interval)

    def start(self):
        self.run_client_thread.start()
        logger.debug("Started client thread")

    def __del__(self):
        # This method is called when the object is about to be destroyed
        self._socket.close()
        logger.debug("Closed socket")


def main():
    client_config = {
        "ip": "localhost",
        "port": 9000,
    }

    client = MetricsClient(client_config)

    # Example: Add metrics to the buffer
    client.add_metric("cpu_usage", 80.0, time.time())
    client.add_metric("memory_usage", 60.0, time.time())

    # Example: Send the buffer to the server
    client.send()


if __name__ == "__main__":
    main()
