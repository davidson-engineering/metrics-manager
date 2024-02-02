import socketserver
import logging
from socketserver import BaseRequestHandler
from typing import List, Tuple
import bufsock
import threading
import time

from metrics_agent.buffer import Buffer, PackagedBuffer, JSONPackager

logger = logging.getLogger(__name__)

MAXIMUM_PACKET_SIZE = 4096
BUFFER_LENGTH = 8192


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
            self.server._input_buffer.add(data)
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
        buffer_length=BUFFER_LENGTH,
        autostart=False,
        host: str = "localhost",
        port: int = 0,
        update_interval=0.5,
        packager=None,
    ):
        self.host = host
        self.port = port
        self.update_interval = update_interval

        self._output_buffer = output_buffer
        self._input_buffer = PackagedBuffer(
            maxlen=buffer_length, packager=packager or JSONPackager()
        )

        handler_thread: threading.Thread = threading.Thread(
            target=self.handle_connections, daemon=True
        )

        decoding_thread: threading.Thread = threading.Thread(
            target=self.decode_input_buffer, daemon=True
        )

        if autostart:
            handler_thread.start()
            decoding_thread.start()

            # handler_thread.join()
            # datafeed_thread.join()

        self.handler_thread = handler_thread
        self.decoding_thread = decoding_thread

    def fetch_buffer(self) -> List[bytes]:
        return input_buffer.unpack_packets()

    def peek_buffer(self) -> List[bytes]:
        return input_buffer.get_copy()

    def handle_connections(self):
        logger.info(
            f"Starting metrics server on {self.server_address[0]} at port {self.server_address[1]}"
        )
        self.serve_forever(poll_interval=self.update_interval)

    def decode_input_buffer(self):
        while True:
            while self._input_buffer.not_empty():
                decoded_data = self._input_buffer.unpack_next()
                self._output_buffer.add(decoded_data)
            time.sleep(self.update_interval)

    def start(self):
        self.handler_thread.start()
        self.decoding_thread.start()

    def stop(self):
        logger.info(
            f"Stopping metrics server on {self.server_address[0]} at port {self.server_address[1]}"
        )
        self.shutdown()
        self.handle_connections.join()
        self.decode_input_buffer.join()

    def __del__(self):
        self.stop()
        logger.info("Server thread stopped")
        super().__del__()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
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
        buffer_length=BUFFER_LENGTH,
        **kwargs,
    ) -> None:
        socketserver.TCPServer.__init__(
            self, (host, port), RequestHandlerClass, **kwargs
        )
        AgentServer.__init__(
            self,
            output_buffer=output_buffer,
            buffer_length=buffer_length,
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
    print(server)

    while True:
        print(buffer.get_size())
        time.sleep(1)


if __name__ == "__main__":
    main()
