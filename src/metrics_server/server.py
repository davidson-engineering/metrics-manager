import socketserver
import logging
from metrics_agent.buffer import Buffer
from typing import List, Tuple

logger = logging.getLogger(__name__)

MAXIMUM_PACKET_SIZE = 1500

buffer = Buffer(maxlen=1024)

class MetricTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        global buffer
        try:
            data = self.request.recv(MAXIMUM_PACKET_SIZE).decode('utf-8').strip()
            # ignore any blank data
            if not data:
                return
            logger.info(f"Received data from {self.client_address[0]}: {data}")
            buffer.add(data)  # Append the data to the deque
        except Exception as e:
            logger.error(e)


class MetricsServer(socketserver.TCPServer):
    def __init__(
        self,
        server_address: Tuple[str, int],
        RequestHandlerClass: socketserver.BaseRequestHandler,
    ):
        super().__init__(server_address, RequestHandlerClass)

    def fetch_buffer(self) -> List[bytes]:
        global buffer
        return buffer.dump_buffer()

    def start_server(self):
        logger.info(
            f"Starting metrics server on {self.server_address[0]} at port {self.server_address[1]}"
        )
        self.serve_forever()

    def stop_server(self):
        logger.info(
            f"Stopping metrics server on {self.server_address[0]} at port {self.server_address[1]}"
        )
        self.shutdown()