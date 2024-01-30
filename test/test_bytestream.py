from network_sync import ByteStream


def test_bytestream_encode():
    data = [
        ("cpu", 0.5, 1622555555.0),
        ("memory", 0.6, 1622555556.0),
        ("cpu", 0.7, 1622555557.0),
    ]
    encoder = ByteStream(first_separator="|", second_separator=":")
    encoded_data = encoder.encode(data)[0]
    assert (
        encoded_data
        == b"cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|"
    )


def test_bytestream_decode():
    data = b"cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|"
    decoder = ByteStream(first_separator="|", second_separator=":")
    decoded_data = decoder.decode(data)
    assert decoded_data == [
        ["cpu", "0.5", "1622555555.0"],
        ["memory", "0.6", "1622555556.0"],
        ["cpu", "0.7", "1622555557.0"],
    ]


def test_bytestream_decode_long():
    data = b"cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|"
    decoder = ByteStream(first_separator="|", second_separator=":")
    decoded_data = decoder.decode(data)
    assert decoded_data == [
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


def test_bytestream_chunked():
    data = [
        ("cpu", 0.5, 1622555555.0),
        ("memory", 0.6, 1622555556.0),
        ("cpu", 0.7, 1622555557.0),
    ]
    decoder = ByteStream(first_separator="|", second_separator=":")
    chunks = decoder.chunked_encode(data, 45)
    assert chunks == [
        b"cpu:0.5:1622555555.0|memory:0.6:1622555556.0|",
        b"cpu:0.7:1622555557.0|",
    ]
    chunks = decoder.chunked_encode(data, 44)
    assert chunks == [
        b"cpu:0.5:1622555555.0|",
        b"memory:0.6:1622555556.0|",
        b"cpu:0.7:1622555557.0|",
    ]


def test_bytestream_chunked_long():
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
    decoder = ByteStream(first_separator="|", second_separator=":")
    chunks = decoder.chunked_encode(data, 50)
    assert chunks == [
        b"cpu:0.5:1622555555.0|memory:0.6:1622555556.0|",
        b"cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|",
        b"memory:0.6:1622555556.0|cpu:0.7:1622555557.0|",
        b"cpu:0.5:1622555555.0|memory:0.6:1622555556.0|",
        b"cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|",
        b"memory:0.6:1622555556.0|cpu:0.7:1622555557.0|",
        b"cpu:0.5:1622555555.0|memory:0.6:1622555556.0|",
        b"cpu:0.7:1622555557.0|cpu:0.5:1622555555.0|",
        b"memory:0.6:1622555556.0|cpu:0.7:1622555557.0|",
    ]
