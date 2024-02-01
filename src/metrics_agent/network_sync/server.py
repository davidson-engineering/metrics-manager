import socketserver
import logging
from socketserver import BaseRequestHandler
from typing import List, Tuple
import bufsock
import threading
import time

from metrics_agent.buffer import Buffer, PacketBuffer

logger = logging.getLogger(__name__)

MAXIMUM_PACKET_SIZE = 4096

input_buffer = PacketBuffer(maxlen=4096)


def is_empty(obj):
    if isinstance(obj, str):
        return not bool(obj.strip())  # Consider empty strings as true
    elif isinstance(obj, list):
        return all(
            is_empty(element) for element in obj
        )  # Recursively check elements in lists
    else:
        return not bool(obj)  # Other non-list, non-string objects are considered true


class AgentTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        global input_buffer
        try:
            data = self.request.recv(MAXIMUM_PACKET_SIZE)
            input_buffer.add(data)
            length = len(data)
            # ignore any blank data
            if is_empty(data):
                return
            logger.info(
                f"Received {length} bytes of data from {self.client_address[0]}"
            )
        except Exception as e:
            logger.error(e)


class AgentServer:
    def __init__(
        self,
        output_buffer: Buffer,
        autostart=True,
        host: str = "localhost",
        port: int = 0,
    ):
        self._output_buffer = output_buffer
        self.host = host
        self.port = port

        handler_thread: threading.Thread = threading.Thread(
            target=self.start_server, daemon=True
        )

        datafeed_thread: threading.Thread = threading.Thread(
            target=self.datafeed_to_output_buffer, daemon=True
        )

        if autostart:
            handler_thread.start()
            datafeed_thread.start()

        handler_thread.join()
        datafeed_thread.join()

        self.handler_thread = handler_thread
        self.datafeed_thread = datafeed_thread

    def fetch_buffer(self) -> List[bytes]:
        return input_buffer.unpack_packets()

    def peek_buffer(self) -> List[bytes]:
        return input_buffer.get_copy()

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

    def datafeed_to_output_buffer(self):
        while True:
            if data := self.fetch_buffer():
                self._output_buffer.add(data)
            else:
                logger.debug("No data to fetch")
            time.sleep(1)

    def __del__(self):
        self.stop_server()
        self.server_thread.join()
        logger.info("Server thread stopped")
        super().__del__()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop_server()
        self.server_thread.join()
        logger.info("Server thread stopped")
        super().__exit__(exc_type, exc_value, traceback)

    def __enter__(self):
        return self

    def __repr__(self):
        return f"{self.__class__.__name__}({self.host}, {self.port})"

    def __str__(self):
        return f"{self.__class__.__name__}({self.host}, {self.port})"


class AgentServerTCP(socketserver.TCPServer, AgentServer):
    def __init__(
        self,
        output_buffer=None,
        host: str = "localhost",
        port: int = 0,
        RequestHandlerClass: BaseRequestHandler = AgentTCPHandler,
        autostart=True,
        **kwargs,
    ) -> None:
        socketserver.TCPServer.__init__(
            self, (host, port), RequestHandlerClass, **kwargs
        )
        AgentServer.__init__(
            self,
            output_buffer=output_buffer,
            autostart=autostart,
            host=host,
            port=port,
            **kwargs,
        )


def main():
    buffer = Buffer()
    server = AgentServerTCP(
        output_buffer=buffer,
        host="localhost",
        port=9000,
        RequestHandlerClass=AgentTCPHandler,
        autostart=True,
    )

    while True:
        print(buffer.peek())
        time.sleep(1)


if __name__ == "__main__":
    main()
