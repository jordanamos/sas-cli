import argparse
import concurrent.futures
import configparser
import importlib.metadata as importlib_metadata
import pathlib
import sys
import time
from collections.abc import Generator
from collections.abc import Sequence
from typing import TextIO

from saspy import logger as saspy_logger
from saspy import SASsession
from saspy.sasexceptions import SASConfigNotFoundError
from saspy.sasexceptions import SASConfigNotValidError
from saspy.sasexceptions import SASIOConnectionError
from saspy.sasexceptions import SASIONotSupportedError

MAX_OUTPUT_OBS = 10000
SAS_CLI_REPLACEMENT_IDENTIFIER = "{{%sas%}}"
CONFIG_FILE = "config.ini"


def valid_sas_file(filepath: str) -> str:
    try:
        with open(filepath):
            pass
    except OSError as e:
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


def setup_logging(args: argparse.Namespace) -> tuple[str, str]:
    """
    Uses the config settings set in config.ini:
        - sas_logging_directory
        - logging_mount_point

    if both are not set, creates a 'logs' directory in the parent folder of the program
    Returns a tuple (2):
        index 0 = the path to the SAS log to be used by SAS program
        index 1 = the local mount point to that same directory
    """
    path = pathlib.Path(args.program_path)
    log_file_name = f"{path.stem}_{time.strftime('%H%M%S', time.localtime())}.log"

    if args.sas_logging_directory and args.logging_mount_point:
        # SAS windows server
        logging_file = str(
            pathlib.PureWindowsPath(args.sas_logging_directory) / log_file_name
        )
        logging_mount_point = str(
            pathlib.Path(args.logging_mount_point) / log_file_name
        )
        return (logging_file, logging_mount_point)
    else:
        # potentially used for local installs of SAS - Untested
        logging_dir = path.parent.absolute() / "logs"
        logging_dir.mkdir(exist_ok=True)
        logging_file = str(logging_dir / log_file_name)
        return (logging_file, logging_file)


def run_sas_program(args: argparse.Namespace) -> int:
    """
    Runs a SAS program file
    """
    try:
        with open(args.program_path) as f:
            program_code = f.read()

        with get_sas_session() as sas:

            show_live_log = False
            if args.show_log:
                # Theres no way to get the live SAS log without directing SAS to
                # print to a file and using a thread to concurrently read the file
                sas_log_file_path, log_file_mount_path = setup_logging(args)
                try:
                    if not args.sas_logging_directory:
                        raise FileNotFoundError(
                            "option 'sas_logging_directory' not set"
                        )
                    sas.submit(
                        f"""%let dir_exists =
                        %sysfunc(fileexist({args.sas_logging_directory}));
                    """
                    )
                    # check if SAS can reach the directory
                    if sas.symget("dir_exists"):
                        # check if python can reach the same directory and if so
                        # create the file.
                        with open(log_file_mount_path, "w"):
                            show_live_log = True
                            logging_code_suffix = (
                                f'PROC PRINTTO LOG="{sas_log_file_path}"; RUN;\n'
                            )
                            program_code = logging_code_suffix + program_code
                    else:
                        message = "SAS unable to reach logging "
                        f"directory: {args.sas_logging_directory}"
                        raise FileNotFoundError(message)
                except (OSError, FileNotFoundError) as e:
                    print(f"Check logging configuration in {args.config}: {e}")
                    print("Log will print after execution.")

            start_time = time.localtime()
            saspy_logger.info(
                f"Started running program: {args.program_path} at "
                f"{time.strftime('%H:%M:%S', start_time)}",
            )

            if show_live_log:
                with concurrent.futures.ThreadPoolExecutor() as ex:

                    def get_new_lines(file: TextIO) -> Generator[str, None, None]:
                        # go to end of file
                        file.seek(0, 2)
                        while code_runner.running():
                            line = file.readline()
                            if not line:
                                time.sleep(0.1)
                                continue
                            yield line

                    with open(log_file_mount_path) as log_file:
                        code_runner = ex.submit(
                            sas.submit,
                            code=program_code,
                            printto=True,
                        )
                        loglines = get_new_lines(log_file)
                        for line in loglines:
                            print(line, end="")

                    result = code_runner.result()
            else:
                result = sas.submit(program_code, printto=True)
                if args.show_log:
                    print(result["LOG"])

            end_time = time.localtime()
            saspy_logger.info(
                f"Finished running program: {args.program_path} at "
                f"{time.strftime('%H:%M:%S', end_time)}\n",
            )

            sas_output = result["LST"]
            sys_err = sas.SYSERR()
            sys_err_text = sas.SYSERRORTEXT()

        if sys_err_text or sys_err > 6:
            message = f"{sys_err}: {sys_err_text}"
            if not args.show_log:
                show_log = input(
                    "Do you wish to view the log before exiting? [y]es / [n]o:"
                )
                if show_log.lower() in ["y", "yes", "si"]:
                    print(result["LOG"])
            raise RuntimeError(message)
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

    config_parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )
    config_parser.add_argument(
        "-c",
        "--config",
        help="specify a config file. default is %(default)s",
        metavar="FILE",
        default=CONFIG_FILE,
    )

    args, argv = config_parser.parse_known_args()
    defaults = {
        "sas_logging_directory": "",
        "logging_mount_point": "",
    }
    if args.config:
        config = configparser.ConfigParser()
        config.read(args.config)
        defaults.update(dict(config.items("DEFAULTS")))

    parser = argparse.ArgumentParser(
        description="A command line interface to SAS", parents=[config_parser]
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f'sas-cli {importlib_metadata.version("sas_cli")}',
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
            between 0 and {MAX_OUTPUT_OBS:,} (default is %(default)s).",
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

    parser.set_defaults(**defaults)

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
