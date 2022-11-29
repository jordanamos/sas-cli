import argparse
import configparser
import os
import sys
from pathlib import Path
from typing import Sequence

import pandas as pd
from saspy import SASsession

CONFIG_FILE = "config.ini"


def load_config(args: argparse.Namespace) -> dict:

    Path(CONFIG_FILE).touch()

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    if args.delete_working_dir:
        config.set("", "working_directory", "")
        print("Current working directory was unset")
    if args.set_working_dir:
        config.set("", "working_directory", args.set_working_dir)
        print(
            f"Current working directory set as '{args.set_working_dir}'. File paths parsed to this program will be relative from this directory"
        )

    with open(CONFIG_FILE, "w") as config_file:
        config.write(config_file)

    return dict(config.items("DEFAULT"))


def valid_sas_file(filepath: str) -> str:
    try:
        open(filepath)
        # TODO confirm both exceptions are thrown by open
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

    with SASsession() as sas:
        print(f"Running program: {args.program_path}\n")
        result = sas.submit(program_code)
        sys_err = sas.SYSERR()
        sys_err_text = sas.SYSERRORTEXT()

    if sys_err_text:
        message = (
            f"An error has occured during program execution: {sys_err}: {sys_err_text}"
        )
        print(message, file=sys.stderr)
        show_log = input("Do you wish to view the log before exiting? [y]es / [n]o:")
        if show_log.lower() in ["y", "yes", "si"]:
            print(result["LOG"])
        return 1

    # print(result)
    if args.show_log:
        print(result["LOG"])

    return 0


def list_lib(args: argparse.Namespace) -> int:
    with SASsession() as sas:
        list_of_tables = sas.list_tables(args.libref, results="pandas")
        if list_of_tables is not None:
            print(list_of_tables)
    return 0


def list_datasets(args: argparse.Namespace) -> int:
    with SASsession() as sas:
        try:
            if args.info:
                print(sas.sasdata(table=args.dataset, libref=args.libref).columnInfo())
            else:
                options = {
                    "where": """""",
                    "obs": args.obs,
                    "keep": args.keep,
                }
                df = sas.sd2df(table=args.dataset, libref=args.libref, dsopts=options)
                print(df)
        except (FileNotFoundError, ValueError) as e:
            return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="A command line interface to SAS",
    )

    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", description="Run a SAS program file")
    run_parser.add_argument(
        "program_path",
        metavar="FILE",
        help="specify the path to the SAS (.sas) program you wish to run",
        type=valid_sas_file,
        # nargs="*",
    )
    run_parser.add_argument("-log", "--show-log", dest="show_log", action="store_true")

    dataset_parser = subparsers.add_parser(
        "data", description="Describe or print sample data from a SAS dataset"
    )
    dataset_parser.add_argument(
        "dataset",
        metavar="DATASET",
        help="specify the SAS dataset/table name",
    )
    dataset_parser.add_argument(
        "-l",
        "--libref",
        metavar="",
        help="specify the SAS internal libref (default is %(default)s)",
        default="WORK",
    )
    dataset_parser.add_argument(
        "-o",
        "--obs",
        metavar="",
        type=int,
        help="specify the amount of output observations (default is %(default)s)",
        default=10,
    )
    dataset_parser.add_argument(
        "-k",
        "--keep",
        metavar="",
        help="specify the columns to keep in the output in a quoted space separated string eg. 'column_1 column_2'",
        default="",
    )
    dataset_parser.add_argument(
        "-i",
        "--info",
        help="display info about a SAS dataset, or if no dataset provided a SAS library",
        action="store_true",
    )

    lib_parser = subparsers.add_parser(
        "lib", description="List the datasets in a SAS library"
    )
    lib_parser.add_argument(
        "libref",
        metavar="LIBREF",
        help="specify the SAS internal libref",
    )

    ret = 0
    args = parser.parse_args(argv)

    # config = load_config(args)
    if args.command == "run":
        ret = run_program(args)
    elif args.command == "data":
        ret = list_datasets(args)
    elif args.command == "lib":
        ret = list_lib(args)

    return ret


if __name__ == "__main__":
    raise SystemExit(main())
