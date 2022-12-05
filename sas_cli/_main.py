import argparse
import sys
from typing import Sequence

from saspy import SASsession
from saspy.sasexceptions import (
    SASConfigNotFoundError,
    SASConfigNotValidError,
    SASIOConnectionError,
    SASIONotSupportedError,
)


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


def run_sas_program(args: argparse.Namespace) -> int:
    """
    Runs a SAS program file
    """
    try:
        with open(args.program_path) as f:
            program_code = f.read()

        with SASsession() as sas:
            print(f"Running program: {args.program_path}\n")
            result = sas.submit(
                program_code
            )  # returns a dict with 2 keys: 'LOG' and 'LST'
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

        print(f"\nOutput:\n{sas_output}")
    # runtime error
    except RuntimeError as e:
        print(
            f"\nAn error occured while running '{args.program_path}': {e}",
            file=sys.stderr,
        )
        return 1
    # connection error
    except SASIOConnectionError as e:
        print(
            f"\nUnable to connect to SAS: {e}",
            file=sys.stderr,
        )
        return 1
    # cant open file error
    except OSError as e:
        print(
            f"\nCan't open '{args.program_path}': {e}",
            file=sys.stderr,
        )
        return 1
    # saspy config errors
    except (
        SASConfigNotValidError,
        SASConfigNotFoundError,
        SASIONotSupportedError,
        AttributeError,
    ) as e:
        print(
            f"\nSaspy configuration error: {e}",
            file=sys.stderr,
        )
        return 1

    return 0


def get_sas_lib(args: argparse.Namespace) -> int:
    """
    List the members or datasets within a SAS library
    """
    with SASsession() as sas:
        list_of_tables = sas.list_tables(
            args.libref,
            results="pandas",
        )
        if list_of_tables is not None:
            print(list_of_tables)
    return 0


def get_sas_data(args: argparse.Namespace) -> int:
    """
    Get sample data from a SAS dataset
    or if the -i flag is set, lists the variables of the SAS dataset
    (PROC DATASETS)
    """
    with SASsession() as sas:
        try:
            if args.info:
                print(
                    sas.sasdata(
                        table=args.dataset,
                        libref=args.libref,
                    ).columnInfo(),
                )
            else:
                options = {
                    "where": """""",
                    "obs": args.obs,
                    "keep": args.keep,
                }
                df = sas.sd2df(
                    table=args.dataset,
                    libref=args.libref,
                    dsopts=options,
                )
                print(df)
        except (FileNotFoundError, ValueError):
            return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="A command line interface to SAS",
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
        # nargs="*",
    )
    run_parser.add_argument(
        "-log",
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
        "-l",
        "--libref",
        metavar="",
        help="specify the SAS internal libref (default is %(default)s)",
        default="WORK",
    )
    data_parser.add_argument(
        "-o",
        "--obs",
        metavar="",
        type=int,
        help="specify the number of output observations (default is %(default)s)",
        default=10,
    )
    data_parser.add_argument(
        "-k",
        "--keep",
        metavar="",
        help="specify the columns to keep in the output."
        "Multiple columns can be specified in a quoted space separated string eg. 'column_1 column_2'",
        default="",
    )
    data_parser.add_argument(
        "-i",
        "--info",
        help="displays info about a SAS dataset rather than data",
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
        ret = get_sas_data(args)
    elif args.command == "lib":
        ret = get_sas_lib(args)

    return ret


if __name__ == "__main__":
    raise SystemExit(main())
