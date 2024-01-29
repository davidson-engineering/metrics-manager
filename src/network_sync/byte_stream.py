from typing import Iterable


class ByteStream:
    def __init__(self, first_separator="/>", second_separator=";"):
        self.first_separator = first_separator
        self.second_separator = second_separator

    def separate(self, data: Iterable):
        sep2 = [self.second_separator.join(map(str, el)) for el in data]
        return self.first_separator.join(map(str, sep2))

    def encode(self, data: str):
        assert isinstance(data, Iterable)
        return self.separate(data).encode()

    def decode(self, data: bytes):
        assert isinstance(data, bytes)
        sep1 = data.decode().split(self.first_separator)
        return [el.split(self.second_separator) for el in sep1]


def main():
    data = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
    data_encoder = ByteStream()
    encoded = data_encoder.encode(data)
    print(encoded)
    decoded = data_encoder.decode(encoded)
    print(decoded)


if __name__ == "__main__":
    main()
