class CustomException(Exception):
    pass


class DataFormatException(CustomException):
    pass


class ConfigFileDoesNotExist(CustomException):
    pass
