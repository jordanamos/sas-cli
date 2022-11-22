import argparse
import os
from typing import Sequence

from saspy import SASsession


def run_program(args: argparse.Namespace) -> int:
    with open(args.filepath) as f:
        contents_text = f.read()

    sas = SASsession()
    sas.submit(contents_text)
    sas.endsas()

    return 0


def is_valid_file(filepath: str) -> bool:
    return os.path.exists(filepath) and filepath.endswith(".sas")


class InvalidFileException(Exception):
    pass


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a SAS program",
    )
    parser.add_argument(
        "filepath",
        metavar="FILE",
        help="the full path to the lame SAS (.sas) program you wish to run",
    )
    args = parser.parse_args(argv)

    ret = 0

    if is_valid_file(args.filepath):
        ret |= run_program(args)
    else:
        raise InvalidFileException(
            f"'{args.filepath}' does not exist or is not a valid .sas file"
        )

    return ret


if __name__ == "__main__":
    raise SystemExit(main())
