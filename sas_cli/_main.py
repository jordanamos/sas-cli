import argparse
import configparser
import os
from pathlib import Path
from typing import Sequence

from saspy import SASsession

SAS_CONFIG_PERSONAL = os.path.join(os.getcwd(), "_sas_cfg_personal.py")


def valid_sas_file(filepath: str) -> str:
    try:
        open(filepath)
    except (FileNotFoundError, OSError) as e:
        message = f"Can't open '{filepath}': {e}"
        raise argparse.ArgumentTypeError(message)

    if not filepath.endswith(".sas"):
        raise argparse.ArgumentTypeError(
            f"The file '{filepath}' is not a valid .sas file"
        )
    return filepath


def run_program(args: argparse.Namespace) -> int:
    with open(args.program_path) as f:
        program_code = f.read()

    with SASsession(cfgfile=SAS_CONFIG_PERSONAL) as sas:
        print(f"Running program: {args.program_path}\n")
        result = sas.submit(program_code)

    # print(result)
    if args.show_log:
        print(result["LOG"])
    print(result["LST"])
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a SAS program",
    )

    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument(
        "program_path",
        metavar="FILE",
        help="the path to the SAS (.sas) program you wish to run",
        type=valid_sas_file,
        # nargs="*",
    )
    run_parser.add_argument("-l", "--show-log", dest="show_log", action="store_true")

    ret = 0
    args = parser.parse_args(argv)

    if args.command == "run":
        ret = run_program(args)

    return ret


if __name__ == "__main__":
    raise SystemExit(main())
