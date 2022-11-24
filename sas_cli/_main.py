import argparse
import os
import pprint
from typing import Sequence

from saspy import SASsession


def valid_sas_file(filepath: str) -> str:
    try:
        open(filepath)
    except OSError as e:
        message = f"can't open '{filepath}': {e}"
        raise argparse.ArgumentTypeError(message)

    if not filepath.endswith(".sas"):
        raise argparse.ArgumentTypeError(f"'{filepath}' is not a valid .sas file")

    return filepath


def run_program(args: argparse.Namespace) -> int:
    with open(args.file_path) as f:
        contents_text = f.read()

    with SASsession() as sas:
        result = sas.submit(contents_text)

    if args.show_log:
        print(result["LOG"])

    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a SAS program",
    )
    parser.add_argument(
        "file_path",
        metavar="FILE",
        help="the full path to the lame SAS (.sas) program you wish to run",
        type=valid_sas_file,
    )
    parser.add_argument("--show-log", dest="show_log", action="store_true")

    ret = 0
    args = parser.parse_args(argv)
    ret = run_program(args)
    return ret


if __name__ == "__main__":
    raise SystemExit(main())
