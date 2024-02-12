import pytest
from metrics_agent import NodeClient, AsyncClient, NodeSwarmClient
import asyncio
import logging

logging.basicConfig(level=logging.DEBUG)


def swarm_command(command):
    buffer = []
    node_client = NodeSwarmClient(buffer=buffer)
    asyncio.run(node_client.send_command(command))
    for data, node in zip(buffer, node_client.nodes):
        logging.info(
            f"{node.name}:{node.address} | node_client.send_command({command}): {data}"
        )


def test_echo_server_in_thread(echo_server_in_thread):
    buffer = []

    # Echo server for testing
    server_address = ("localhost", 9001)
    echo_server = echo_server_in_thread(*server_address)
    client = AsyncClient(server_address=server_address, buffer=buffer)
    asyncio.run(client.request("Hello"))
    assert buffer[0] == "ECHO: Hello"


def test_node_request():
    buffer = []
    server_address = ("10.10.32.90", 8888)
    node_client = NodeClient(server_address=server_address, buffer=buffer)
    asyncio.run(node_client.request_data())
    logging.info(
        f'{node_client.node.name}:{node_client.node.address} | node_client.send_command("get_time"): {buffer[0]}'
    )


def test_node_swarm_request():
    buffer = []
    node_client = NodeSwarmClient(buffer=buffer)
    asyncio.run(node_client.request_data())
    for data, node in zip(buffer, node_client.nodes):
        logging.info(
            f'{node.name}:{node.address} | node_client.send_command("get_time"): {data}'
        )


def test_swarm_get_time():
    swarm_command("get_time")


def test_swarm_get_files():
    swarm_command("view_files")


def test_swarm_get_status():
    swarm_command("view_status")
