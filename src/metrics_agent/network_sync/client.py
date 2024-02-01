import socket
from metrics_agent.buffer.buffer import Buffer
from datetime import datetime, timezone
from typing import Union
import socket
import time
import threading
import logging
from abc import ABC, abstractmethod
import bufsock

from metrics_agent.buffer import PacketBuffer
from metrics_agent.buffer import PicklerPackager

logger = logging.getLogger(__name__)


MAXIMUM_PACKET_SIZE = 4096


class AgentClient(ABC):
    def __init__(self, host="localhost", port=0, autostart=False, update_interval=1):
        self.host = host
        self.port = port
        self._buffer = PacketBuffer(
            packager=PicklerPackager(), max_packet_size=MAXIMUM_PACKET_SIZE
        )
        self.update_interval = update_interval
        self.run_client_thread = threading.Thread(target=self.run_client, daemon=True)
        if autostart:
            self.start()

    @abstractmethod
    def send(self):
        ...

    def run_client(self):
        while True:
            self.send()
            time.sleep(self.update_interval)

    def start(self):
        self.run_client_thread.start()
        logger.debug("Started client thread")
        return self

    def stop(self):
        self.run_client_thread.join()
        logger.debug("Stopped client thread")

    def run_until_buffer_empty(self):
        while self._buffer.not_empty():
            self.send()
            logger.info("Waiting for buffer to empty")
            time.sleep(self.update_interval)
        else:
            logger.info("Buffer empty")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()

    def __repr__(self):
        return f"{self.__class__.__name__}({self.host}, {self.port})"


class AgentClientTCP(AgentClient):
    def send(self):
        while self._buffer.not_empty():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.host, self.port))
                packet = self._buffer.next_packet()
                logger.debug(f"Sending packet: {packet}")
                s.send(packet)


class AgentClientUDP(AgentClient):
    def send(self):
        while self._buffer.not_empty():
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                packet = self._buffer.next_packet()
                logger.debug(f"Sending packet: {packet}")
                s.sendto(packet, (self.host, self.port))


class MetricsMixin:
    def add_metric(self, name: str, value: float, timestamp: Union[float, datetime]):
        if isinstance(timestamp, datetime):
            timestamp = timestamp.timestamp()
        data = (name, value, timestamp)
        self._buffer.add(data)
        logger.debug(f"Added data to client buffer: {name}={value}")


class MetricsClientTCP(AgentClientTCP, MetricsMixin):
    ...


class MetricsClientUDP(AgentClientUDP, MetricsMixin):
    ...


def main():
    import logging

    logging.basicConfig(level=logging.DEBUG)
    client_config = {
        "host": "localhost",
        "port": 9000,
    }

    client = MetricsClientTCP(**client_config)

    # Example: Add metrics to the buffer
    client.add_metric("cpu_usage", 80.0, time.time())
    client.add_metric("memory_usage", 60.0, time.time())

    # Example: Send the buffer to the server
    client.send()

    time.sleep(10)


if __name__ == "__main__":
    main()
