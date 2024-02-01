from metrics_agent.buffer import PacketBuffer
from metrics_agent.buffer.packager import SeparatorPackager, PicklerPackager

packager = SeparatorPackager(
    sep_major="|", sep_minor=":", terminator="\0", encoded=False
)
packager_encoded = SeparatorPackager(
    sep_major="|", sep_minor=":", terminator="\0", encoded=True
)

pickler_packager = PicklerPackager(terminator=b"\0")


def test_packager_packing():
    data = [
        ("cpu", 0.5, 1622555555.0),
        ("memory", 0.6, 1622555556.0),
        ("cpu", 0.7, 1622555557.0),
    ]
    packed_data = packager.pack(data)
    assert (
        packed_data
        == "cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|\0"
    )
    packed_data = packager_encoded.pack(data)
    assert (
        packed_data
        == b"cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|\0"
    )


def test_packager_unpacking():
    data = "cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|\0"
    unpacked_data = packager.unpack(data)
    assert unpacked_data == [
        ["cpu", "0.5", "1622555555.0"],
        ["memory", "0.6", "1622555556.0"],
        ["cpu", "0.7", "1622555557.0"],
    ]


def test_packager_unpacking_long():
    data = "cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|"
    packed_data = packager.unpack(data)
    assert packed_data == [
        ["cpu", "0.5", "1622555555.0"],
        ["memory", "0.6", "1622555556.0"],
        ["cpu", "0.7", "1622555557.0"],
        ["cpu", "0.5", "1622555555.0"],
        ["memory", "0.6", "1622555556.0"],
        ["cpu", "0.7", "1622555557.0"],
        ["cpu", "0.5", "1622555555.0"],
        ["memory", "0.6", "1622555556.0"],
        ["cpu", "0.7", "1622555557.0"],
        ["cpu", "0.5", "1622555555.0"],
        ["memory", "0.6", "1622555556.0"],
        ["cpu", "0.7", "1622555557.0"],
        ["cpu", "0.5", "1622555555.0"],
        ["memory", "0.6", "1622555556.0"],
        ["cpu", "0.7", "1622555557.0"],
        ["cpu", "0.5", "1622555555.0"],
        ["memory", "0.6", "1622555556.0"],
        ["cpu", "0.7", "1622555557.0"],
    ]


def test_packetbuffer():
    data = [
        ("cpu", 0.5, 1622555555.0),
        ("memory", 0.6, 1622555556.0),
        ("cpu", 0.7, 1622555557.0),
    ]
    buffer = PacketBuffer(data, packager=packager_encoded)
    packets = buffer.get_copy().dump_as_packets(max_packet_size=49)
    assert packets == [
        b"cpu:0.5:1622555555.0|memory:0.6:1622555556.0|\0",
        b"cpu:0.7:1622555557.0|\0",
    ]
    packets = buffer.get_copy().dump_as_packets(max_packet_size=45)
    assert packets == [
        b"cpu:0.5:1622555555.0|\0",
        b"memory:0.6:1622555556.0|\0",
        b"cpu:0.7:1622555557.0|\0",
    ]


def test_packetbuffer_chunked_long():
    data = [
        ("cpu", 0.5, 1622555555.0),
        ("memory", 0.6, 1622555556.0),
        ("cpu", 0.7, 1622555557.0),
        ("cpu", 0.5, 1622555555.0),
        ("memory", 0.6, 1622555556.0),
        ("cpu", 0.7, 1622555557.0),
        ("cpu", 0.5, 1622555555.0),
        ("memory", 0.6, 1622555556.0),
        ("cpu", 0.7, 1622555557.0),
        ("cpu", 0.5, 1622555555.0),
        ("memory", 0.6, 1622555556.0),
        ("cpu", 0.7, 1622555557.0),
        ("cpu", 0.5, 1622555555.0),
        ("memory", 0.6, 1622555556.0),
        ("cpu", 0.7, 1622555557.0),
        ("cpu", 0.5, 1622555555.0),
        ("memory", 0.6, 1622555556.0),
        ("cpu", 0.7, 1622555557.0),
    ]
    buffer = PacketBuffer(data, packager=packager_encoded)
    packets = buffer.dump_as_packets(max_packet_size=50)
    assert packets == [
        b"cpu:0.5:1622555555.0|memory:0.6:1622555556.0|\0",
        b"cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|\0",
        b"memory:0.6:1622555556.0|cpu:0.7:1622555557.0|\0",
        b"cpu:0.5:1622555555.0|memory:0.6:1622555556.0|\0",
        b"cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|\0",
        b"memory:0.6:1622555556.0|cpu:0.7:1622555557.0|\0",
        b"cpu:0.5:1622555555.0|memory:0.6:1622555556.0|\0",
        b"cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|\0",
        b"memory:0.6:1622555556.0|cpu:0.7:1622555557.0|\0",
    ]


def test_pickler_tuple():
    data = [
        ("cpu", 0.5, 1622555555.0),
        ("memory", 0.6, 1622555556.0),
    ]
    packed_data = pickler_packager.pack(data)

    assert (
        packed_data
        == b"\x80\x05\x95<\x00\x00\x00\x00\x00\x00\x00]\x94(\x8c\x03cpu\x94G?\xe0\x00\x00\x00\x00\x00\x00GA\xd8-\x8e\xe8\xc0\x00\x00\x87\x94\x8c\x06memory\x94G?\xe3333333GA\xd8-\x8e\xe9\x00\x00\x00\x87\x94e.\x00"
    )


def test_pickler_dict():
    data = {"cpu": 0.5, "memory": 0.6, "time": 1622555555.0}
    packed_data = pickler_packager.pack(data)

    assert (
        packed_data
        == b"\x80\x05\x956\x00\x00\x00\x00\x00\x00\x00}\x94(\x8c\x03cpu\x94G?\xe0\x00\x00\x00\x00\x00\x00\x8c\x06memory\x94G?\xe3333333\x8c\x04time\x94GA\xd8-\x8e\xe8\xc0\x00\x00u.\x00"
    )
