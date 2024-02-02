from abc import ABC, abstractmethod
import pickle
import json


class Packager(ABC):
    def __init__(self, terminator="\0"):
        self.terminator = terminator

    @abstractmethod
    def pack(self, data, terminate=True):
        ...

    @abstractmethod
    def unpack(self, data):
        ...


class SeparatorPackager(Packager):
    def __init__(self, sep_major="|", sep_minor=";", terminator="\0", encoded=True):
        """
        Packager is a class that packs and unpacks data into a string
        The data is packed into a string with a major separator and a minor separator.
        The major separator separates the data items, and the minor separator separates the data values.
        The data is then encoded into bytes and chunked into smaller pieces if the length exceeds the maximum length.

        Example:
        >>> data = [("cpu", 0.5, 1622555555.0), ("memory", 0.6, 1622555556.0), ("cpu", 0.7, 1622555557.0)]
        >>> packager = Packager(sep_major="|", sep_minor=";", terminator="\0")
        >>> encoded_data = packager.pack(data)
        >>> encoded_data
        b'cpu:0.5:1622555555.0|memory:0.6:1622555556.0|cpu:0.7:1622555557.0|\0'
        >>> decoded_data = packager.unpack(encoded_data)
        >>> decoded_data
        [['cpu', '0.5', '1622555555.0'], ['memory', '0.6', '1622555556.0'], ['cpu', '0.7', '1622555557.0']]
        """
        super().__init__(terminator)
        # Set the major and minor separators
        self.sep_major = sep_major
        self.sep_minor = sep_minor
        self.encoded = encoded
        self.coding = "utf-8"

    def pack(self, data, terminate=True):
        # Pack some data into a separated string
        packed_data = ""
        if not isinstance(data[0], (list, tuple)):
            data = [data]
        for item in data:
            packed_data += self.sep_minor.join(map(str, item))
            packed_data += self.sep_major
        if terminate:
            packed_data += self.terminator
        if self.encoded:
            packed_data = packed_data.encode(self.coding)
        return packed_data

    def unpack(self, data):
        # Unpack some separated data into a list of lists
        unpacked = []
        if self.encoded:
            data = data.decode(self.coding)
        # Remove the terminator from the end of the data if present
        data = data.removesuffix(self.terminator).removesuffix(self.sep_major)
        items = data.split(self.sep_major)
        for item in items:  # Exclude the last empty item after the last separator
            values = item.split(self.sep_minor)
            unpacked.append(values)
        return unpacked


class PicklerPackager(Packager):
    def pack(self, data, terminate=True):
        return pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL) + (
            self.terminator if terminate else b""
        )

    def unpack(self, data):
        return pickle.loads(data)


class JSONPackager(Packager):
    def pack(self, data, terminate=True):
        json_packed = json.dumps(data) + (self.terminator if terminate else "")
        return json_packed.encode("utf-8")

    def unpack(self, data):
        if data := data.decode("utf-8").removesuffix(self.terminator):
            return json.loads(data)
