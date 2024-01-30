import socketserver
import logging
from agent.buffer import Buffer
from typing import List, Tuple

from network_sync.byte_stream import ByteStream

logger = logging.getLogger(__name__)

MAXIMUM_PACKET_SIZE = 4096

buffer = Buffer(maxlen=1024)
data_decoder = ByteStream()


def is_empty(obj):
    if isinstance(obj, str):
        return not bool(obj.strip())  # Consider empty strings as true
    elif isinstance(obj, list):
        return all(
            is_empty(element) for element in obj
        )  # Recursively check elements in lists
    else:
        return not bool(obj)  # Other non-list, non-string objects are considered true


class MetricTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        global buffer
        try:
            data = self.request.recv(MAXIMUM_PACKET_SIZE)
            length = len(data)
            data = data_decoder.decode(data)
            # ignore any blank data
            if is_empty(data):
                return
            logger.info(
                f"Received {length} bytes of data from {self.client_address[0]}"
            )
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

    def peek_buffer(self) -> List[bytes]:
        global buffer
        return buffer.get_buffer_copy()

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
