from typing import Iterable

from itertools import islice


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


class ByteStream:
    """
    ByteStream is a class that packages and encodes data into bytes.
    The data is packaged into a string with a first separator and a second separator.
    The first separator separates the data items, and the second separator separates the data values.
    The data is then encoded into bytes and chunked into smaller pieces if the length exceeds the maximum length.

    Example:
    >>> data = [("cpu", 0.5, 1622555555.0), ("memory", 0.6, 1622555556.0), ("cpu", 0.7, 1622555557.0)]
    >>> encoder = ByteStream(first_separator="|", second_separator=":")
    >>> encoded_data = encoder.encode(data)[0]
    >>> encoded_data
    b'cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|'
    >>> decoder = ByteStream(first_separator="|", second_separator=":")
    >>> decoded_data = decoder.decode(encoded_data)
    >>> decoded_data
    [['cpu', '0.5', '1622555555.0'], ['memory', '0.6', '1622555556.0'], ['cpu', '0.7', '1622555557.0']]
    """

    def __init__(self, first_separator="|", second_separator=";"):
        self.first_separator = first_separator
        self.second_separator = second_separator

    def package(self, data):
        packaged_data = ""
        for item in data:
            packaged_data += self.second_separator.join(map(str, item))
            packaged_data += self.first_separator
        return packaged_data

    def encode(self, data, max_length=1024):
        return self.chunked_encode(data, max_length)

    def decode(self, data: bytes):
        decoded_data = []
        items = (
            data.removesuffix(self.first_separator.encode("utf-8"))
            .decode("utf-8")
            .split(self.first_separator)
        )
        for item in items:  # Exclude the last empty item after the last separator
            values = item.split(self.second_separator)
            decoded_data.append(values)
        return decoded_data

    def chunked_encode(self, data, max_chunk_size):
        data = self.package(data)
        indices = [
            index for index, char in enumerate(data) if char == self.first_separator
        ]
        break_point = 0
        break_points = []
        for i, index in enumerate(indices):
            if index >= max_chunk_size + break_point:
                break_point = indices[i - 1]
                break_points.append(break_point + 1)
        data = [data[i:j] for i, j in zip([0] + break_points, break_points + [None])]

        return [chunk.encode("utf-8") for chunk in data]


def main():
    data = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
    data_encoder = ByteStream()
    encoded = data_encoder.encode(data)
    print(encoded)
    decoded = data_encoder.decode(encoded)
    print(decoded)


if __name__ == "__main__":
    main()
