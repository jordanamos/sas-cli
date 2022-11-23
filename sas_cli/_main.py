import argparse
import os
from typing import Sequence

from saspy import SASsession


def valid_sas_file(filepath: str) -> str:
    if not (os.path.exists(filepath) and filepath.endswith(".sas")):
        raise argparse.ArgumentTypeError(
            f"'{filepath}' does not exist or is not a valid .sas file"
        )
    return filepath


def run_program(args: argparse.Namespace) -> int:

    with open(args.filepath) as f:
        contents_text = f.read()

    with SASsession(cfgfile="sascfg_personal.py") as sas:
        result = sas.submit(contents_text)

    print(result["LOG"])
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a SAS program",
    )
    parser.add_argument(
        "filepath",
        metavar="FILE",
        help="the full path to the lame SAS (.sas) program you wish to run",
        type=valid_sas_file,
    )

    args = parser.parse_args(argv)
    ret = run_program(args)
    return ret


if __name__ == "__main__":
    raise SystemExit(main())
