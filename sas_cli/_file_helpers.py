import os
from argparse import ArgumentTypeError


def valid_sas_file(filepath: str) -> str:
    if not (os.path.exists(filepath) and filepath.endswith(".sas")):
        raise InvalidFileException(
            f"'{filepath}' does not exist or is not a valid .sas file"
        )
    return filepath


class InvalidFileException(ArgumentTypeError):
    pass
