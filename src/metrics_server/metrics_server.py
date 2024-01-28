import socketserver
import logging
from collections import deque

from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

MAXIMUM_PACKET_SIZE = 1024


class MetricTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(MAXIMUM_PACKET_SIZE).strip()
        logger.info(f"{self.client_address[0]} wrote:")
        logger.info(data)
        self.server.buffer.append(data)  # Append the data to the deque


class MetricsServer(socketserver.TCPServer):
    def __init__(
        self,
        server_address: Tuple[str, int],
        RequestHandlerClass: socketserver.BaseRequestHandler,
        data_buffer_size: Optional[int] = 1000,
    ):
        super().__init__(server_address, RequestHandlerClass)
        self.buffer: deque = deque(maxlen=data_buffer_size)

    def run(self):
        logger.info(
            f"Starting metrics server on {self.server_address[0]} at port {self.server_address[1]}"
        )
        self.serve_forever()
