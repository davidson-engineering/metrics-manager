from collections import deque
from datetime import datetime, timezone
import logging
from copy import deepcopy


from metrics_agent.buffer.packager import Packager, SeparatorPackager
from metrics_agent.metric import Metric

logger = logging.getLogger(__name__)


class Buffer:
    def __init__(self, data=None, maxlen=4096):
        data = data or []
        self.buffer = deque(data, maxlen=maxlen)

    def add(self, data):
        try:
            if isinstance(data[0], list):
                self.buffer.extend(data)
        except TypeError:
            pass
        self.buffer.append(data)

    def reinsert(self, data):
        if isinstance(data[0], list):
            self.buffer.extendleft(data)
        else:
            self.buffer.appendleft(data)

    def clear(self):
        self.buffer.clear()

    def get_copy(self):
        return deepcopy(self)

    def get_size(self):
        return len(self.buffer)

    def not_empty(self):
        return len(self.buffer) > 0

    def dump(self):
        buffer = list(self.buffer)
        self.clear()
        return buffer

    def peek(self, idx=None):
        if idx:
            return self.buffer[idx]
        return list(self.buffer)

    def __next__(self):
        return self.buffer.popleft()

    def __iter__(self):
        return self

    def __len__(self):
        return len(self.buffer)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.buffer})"

    def __str__(self):
        return f"{self.__class__.__name__}({self.buffer})"

    def __getitem__(self, index):
        return self.buffer[index]

    def __setitem__(self, index, value):
        self.buffer[index] = value


class MetricsBuffer(Buffer):
    def add_metric(self, name, value, timestamp=None):
        timestamp = timestamp or datetime.now(timezone.utc)
        metric = Metric(name, value, timestamp)
        self.add(metric)


class PacketBuffer(Buffer):
    def __init__(
        self,
        data=None,
        packager: Packager = None,
        maxlen=4096,
        max_packet_size=4096,
        terminator=b"\0",
    ):
        """
        Packager is a class that packs and unpacks data into a string
        max_packet_size is the maximum size of a packet
        terminator is the terminator for the packet

        Example:
        >>> data = [("cpu", 0.5, 1622555555.0), ("memory", 0.6, 1622555556.0), ("cpu", 0.7, 1622555557.0)]
        >>> packager = Packager(sep_major="|", sep_minor=":", terminator="\0")
        >>> packet_buffer = PacketBuffer(data, packager=packager)
        >>> packet = packet_buffer.next_packet(10)
        >>> packet
        b'cpu:0.5:1622555555.0|memory:0.6:1622555556.0|'
        >>> packet_buffer.decode(packet)
        [['cpu', '0.5', '1622555555.0'], ['memory', '0.6', '1622555556.0'], ['cpu', '0.7', '1622555557.0']]
        """

        data = data or []
        super().__init__(data, maxlen=maxlen)
        self.packager = packager
        self.max_packet_size = max_packet_size
        self.terminator = terminator
        self.len_terminator = len(terminator)

    def next_package(self):
        next_data = next(self)
        # If a packager is specified, pack the next data before returning it
        if self.packager:
            return self.packager.pack(next_data, terminate=False)
        # If no packager is specified, return the next data
        return next_data

    def pack(self, data, terminate=True):
        if self.packager:
            return self.packager.pack(data, terminate)
        return data

    def unpack(self, data):
        if self.packager:
            return self.packager.unpack(data)
        return data

    def next_packet(self, max_packet_size=None):
        # If max_packet_size is not specified, use the default value
        max_packet_size = max_packet_size or self.max_packet_size
        packet = b""
        # Get the next package
        data = self.next_package()
        # Check if the next package is too large to fit in the specified packet size
        if len(data) > max_packet_size:
            raise ValueError(
                f"Maximum packet size of {max_packet_size} too small. Data is {len(data)} bytes long."
            )
        # Check if adding the next package to the packet will exceed the maximum packet size
        while len(packet) + len(data) + self.len_terminator <= max_packet_size:
            # Add the next package to the packet
            packet += data
            try:
                # Try get the next package. If there are no more packages, break the loop
                data = self.next_package()
            except IndexError:
                break
        else:
            data = self.unpack(data)
            self.reinsert(data)
        # Add the terminator to the packet before returning it
        return packet + self.terminator

    def dump_as_packets(self, max_packet_size=None):
        packets = []
        while self.not_empty():
            packet = self.next_packet(max_packet_size)
            packets.append(packet)
        return packets

    def unpack_packets(self):
        data = []
        for packet in self.dump():
            packet = self.unpack(packet)
            data.extend(packet)
        return data


def test1():
    data = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
    packager = SeparatorPackager(sep_major="|", sep_minor=";")
    datastream = PacketBuffer(data, packager=packager)
    print(datastream)
    packet = datastream.next_packet(1024)
    print()
    print(f"{packet} of length{len(packet)}")
    print(f"unpacked data: {packager.unpack(packet)}")

    datastream = PacketBuffer(data, packager=packager)
    packet = datastream.next_packet(10)
    print(f"{packet} of length{len(packet)}")


def main():
    def modify_buffer(buffer: Buffer) -> None:
        buffer.add(1)
        buffer.add(2)
        buffer.add(3)
        buffer.add(4)
        buffer.add(5)

    buffer = Buffer()
    print(buffer)
    modify_buffer(buffer)
    print(buffer)


if __name__ == "__main__":
    test1()
    main()
