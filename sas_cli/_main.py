import argparse
import importlib.metadata as importlib_metadata
import sys
import time
from typing import Sequence

from saspy import SASsession
from saspy import logger as sapy_logger
from saspy.sasexceptions import (
    SASConfigNotFoundError,
    SASConfigNotValidError,
    SASIOConnectionError,
    SASIONotSupportedError,
)

MAX_OUTPUT_OBS = 10000


def valid_sas_file(filepath: str) -> str:
    try:
        with open(filepath):
            pass
    except (OSError) as e:
        message = f"Can't open '{filepath}': {e}"
        raise argparse.ArgumentTypeError(message)

    if not filepath.endswith(".sas"):
        raise argparse.ArgumentTypeError(
            f"The file '{filepath}' is not a valid .sas file"
        )
    return filepath


def get_sas_session() -> SASsession:
    try:
        return SASsession()
    except SASIOConnectionError:
        raise
    except (
        SASConfigNotValidError,
        SASConfigNotFoundError,
        SASIONotSupportedError,
        AttributeError,
    ) as e:
        message = (
            f"\nSaspy configuration error. Configuration file "
            f"not found or is not valid: {e}"
        )
        print(message, file=sys.stderr)
        raise SASConfigNotValidError(message)


def run_sas_program(args: argparse.Namespace) -> int:
    """
    Runs a SAS program file
    """
    try:
        with open(args.program_path) as f:
            program_code = f.read()

        with get_sas_session() as sas:
            start_time = time.localtime()
            sapy_logger.info(
                f"Started running program: {args.program_path} at "
                f"{time.strftime('%H:%M:%S', start_time)}",
            )
            result = sas.submit(program_code)
            end_time = time.localtime()
            sapy_logger.info(
                f"Finished running program: {args.program_path} at "
                f"{time.strftime('%H:%M:%S', end_time)}\n",
            )
            sas_output = result["LST"]
            sas_log = result["LOG"]
            sys_err = sas.SYSERR()
            sys_err_text = sas.SYSERRORTEXT()

        if sys_err_text or sys_err > 6:
            message = f"{sys_err}: {sys_err_text}"

            show_log = input(
                "Do you wish to view the log before exiting? [y]es / [n]o:"
            )
            if show_log.lower() in ["y", "yes", "si"]:
                print(sas_log)
            raise RuntimeError(message)

        if args.show_log:
            print(sas_log)

        if sas_output:
            print(f"\nOutput:\n{sas_output}")
    except RuntimeError as e:
        print(
            f"\nAn error occured while running '{args.program_path}': {e}",
            file=sys.stderr,
        )
        return 1
    except (
        SASIOConnectionError,
        SASConfigNotValidError,
    ):
        return 1
    return 0


def get_sas_lib(args: argparse.Namespace) -> int:
    """
    List the members or datasets within a SAS library
    """
    try:
        with get_sas_session() as sas:
            list_of_tables = sas.list_tables(
                args.libref,
                results="pandas",
            )
            if list_of_tables is not None:
                print(list_of_tables)
    except (
        SASIOConnectionError,
        SASConfigNotValidError,
    ):
        return 1
    return 0


def get_sas_data(args: argparse.Namespace) -> int:
    """
    Get sample data from a SAS dataset
    or info about the dataset if the --info-only(-i) flag is set
    (PROC DATASETS)
    """
    try:
        with get_sas_session() as sas:
            data = sas.sasdata(
                table=args.dataset,
                libref=args.libref,
                dsopts={
                    "where": args.where,
                    "obs": args.obs,
                    "keep": args.keep,
                    "drop": args.drop,
                },
                results="PANDAS",
            )
            try:
                # this is used to parse the dsopts and get an exception we can handle
                # rather than a crappy SAS log that would otherwise be displayed
                # with a direct to_df() call
                column_info = data.columnInfo()
                if args.info_only:
                    print(column_info)
                else:
                    print(data.to_df())
            except ValueError as e:
                print(e, file=sys.stderr)
                return 1
    except (
        SASIOConnectionError,
        SASConfigNotValidError,
    ):
        return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="A command line interface to SAS",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f'%(prog)s {importlib_metadata.version("sas_cli")}',
    )

    subparsers = parser.add_subparsers(
        dest="command",
    )

    # run parser
    run_parser = subparsers.add_parser(
        "run",
        description="Run a SAS program file",
    )
    run_parser.add_argument(
        "program_path",
        metavar="FILE",
        help="specify the path to the SAS (.sas) program you wish to run",
        type=valid_sas_file,
    )
    run_parser.add_argument(
        "--show-log",
        dest="show_log",
        action="store_true",
        help="displays the SAS log once the program has finished executing",
    )

    # data parser
    data_parser = subparsers.add_parser(
        "data",
        description="Describe or print sample data from a SAS dataset",
    )
    data_parser.add_argument(
        "dataset",
        metavar="DATASET",
        help="specify the SAS dataset/table name",
    )
    data_parser.add_argument(
        "-lib",
        "--libref",
        metavar="",
        help="specify the SAS internal libref (default is %(default)s)",
        default="WORK",
    )
    data_parser.add_argument(
        "--obs",
        metavar="",
        type=int,
        help=f"specify the number of output observations \
            between 0 and {MAX_OUTPUT_OBS:,} (default is %(default)s). ",
        default=10,
    )
    data_parser.add_argument(
        "--keep",
        metavar="",
        help="specify a string containing the columns to \
            keep in the output eg. 'column_1 column_2'",
        default="",
    )
    data_parser.add_argument(
        "--drop",
        metavar="",
        help="specify a string containing the columns to \
            drop in the output eg. 'column_1 column_2'",
        default="",
    )
    data_parser.add_argument(
        "--where",
        metavar="",
        help="specify a string containing where clause conditions \
            eg. \"financial_year='2021-22'\"",
        default="",
    )
    data_parser.add_argument(
        "-i",
        "--info-only",
        help="displays information about a SAS dataset rather than data",
        action="store_true",
    )

    # lib parser
    lib_parser = subparsers.add_parser(
        "lib", description="List the members of a SAS library"
    )
    lib_parser.add_argument(
        "libref",
        metavar="LIBREF",
        help="specify the SAS internal libref",
    )

    ret = 0
    args = parser.parse_args(argv)

    if args.command == "run":
        ret = run_sas_program(args)
    elif args.command == "data":
        if args.obs > MAX_OUTPUT_OBS:
            print(
                f"Option obs '{args.obs:,}' is too large and has been "
                f"set to {MAX_OUTPUT_OBS:,}."
            )
            args.obs = MAX_OUTPUT_OBS
        ret = get_sas_data(args)
    elif args.command == "lib":
        ret = get_sas_lib(args)

    return ret


if __name__ == "__main__":
    raise SystemExit(main())
