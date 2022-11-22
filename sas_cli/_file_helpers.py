import os


def is_valid_file(filepath: str) -> bool:
    return os.path.exists(filepath) and filepath.endswith(".sas")


class InvalidFileException(Exception):
    pass
