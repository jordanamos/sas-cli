import argparse
from typing import Sequence

from saspy import SASsession

from sas_cli._file_helpers import valid_sas_file


def run_program(args: argparse.Namespace) -> int:

    with open(args.filepath) as f:
        contents_text = f.read()

    sas = SASsession()
    sas.submit(contents_text)
    sas.endsas()
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
