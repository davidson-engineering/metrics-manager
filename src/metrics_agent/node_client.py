import asyncio
from dataclasses import dataclass, field
from typing import Union
from collections import deque
from yaml import safe_load

from metrics_agent.async_client import AsyncClient

NODE_FILEPATH = "config/nodes.yaml"
NODE_COMMAND_FILEPATH = "config/node_commands.yaml"


@dataclass
class Node:
    ip_address: str
    port: int
    name: str
    bucket: str
    type: str
    extra_tags: dict = field(default_factory=dict)
    priority: int = 0

    def __post_init__(self):
        self.address = (self.ip_address, self.port)


@dataclass
class NodeCommands:
    request_data: str
    hello_world: str
    get_time: str
    start_logging: str
    stop_logging: str
    view_files: str
    view_status: str
    download_file: str


def parse_yaml(filepath=None):
    with open(filepath, "r") as f:
        return safe_load(f)


def find_node_by_ip(nodes_dict, ip_address):
    for node_id, node_data in nodes_dict.items():
        if node_data.get("ip_address") == ip_address:
            return node_data
    return None


def fetch_nodes(filepath):
    node_list = parse_yaml(filepath)
    return [Node(**node) for _, node in node_list.items()]


def fetch_commands(filepath):
    node_commands = parse_yaml(filepath)
    return {
        node_type: NodeCommands(**commands)
        for node_type, commands in node_commands.items()
    }


class NodeClient(AsyncClient):

    _node_list_filepath = NODE_FILEPATH
    _node_command_filepath = NODE_COMMAND_FILEPATH

    def __init__(
        self,
        buffer: Union[list, deque],
        server_address: tuple = None,
        node: Union[dict, Node] = None,
        update_interval=10,
    ):
        super().__init__(
            server_address=server_address,
            buffer=buffer,
        )
        self.node_list = fetch_nodes(self._node_list_filepath)
        self.commands = fetch_commands(self._node_command_filepath)
        self.update_interval = update_interval

        if isinstance(node, dict):
            self.node = Node(**node)
        elif node is None:
            # Retrieve node from config
            node_list = parse_yaml(self._node_list_filepath)
            self.node = Node(**find_node_by_ip(node_list, server_address[0]))

    async def send_command(self, command):
        node = self.node
        message = getattr(self.commands[node.type], command)
        await self.request(message, node.address)

    async def request_data(self):
        await self.send_command(command="request_data")

    async def hello_world(self):
        await self.send_command(command="hello_world")

    async def request_data_periodically(self, update_interval=None):
        update_interval = update_interval or self.update_interval
        while True:
            await self.request_data()
            await asyncio.sleep(update_interval)

    async def hello_world(self):
        await self.send_command(command="hello_world")


class NodeSwarmClient(AsyncClient):

    _node_list_filepath = NODE_FILEPATH
    _node_command_filepath = NODE_COMMAND_FILEPATH

    def __init__(
        self,
        buffer: Union[list, deque],
        update_interval=10,
    ):
        super().__init__(buffer=buffer)
        self.update_interval = update_interval
        self.nodes = fetch_nodes(self._node_list_filepath)
        self.commands = fetch_commands(self._node_command_filepath)

    async def send_command(self, command):
        for node in self.nodes:
            message = getattr(self.commands[node.type], command)
            await self.request(message, node.address)

    async def request_data(self):
        await self.send_command(command="request_data")

    async def request_data_periodically(self, update_interval=None):
        update_interval = update_interval or self.update_interval
        while True:
            await self.request_data()
            await asyncio.sleep(update_interval)

    async def hello_world(self):
        await self.send_command(command="hello_world")


def main():
    buffer = []
    server_address = ("10.10.32.90", 8888)
    node_client = NodeClient(server_address=server_address, buffer=buffer)
    asyncio.run(node_client.request_data())
    print(f"{buffer=}")


if __name__ == "__main__":
    main()
