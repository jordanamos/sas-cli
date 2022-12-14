import argparse
import concurrent.futures
import configparser
import importlib.metadata as importlib_metadata
import os
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


def prepare_log_files(args: argparse.Namespace) -> tuple[str, str]:
    """
    Uses the config settings set in specified config
    file (default config.ini):
        - sas_server_logging_dir
        - local_logging_dir

    Requires both to be set, otherwise creates a 'logs' directory in the parent
    directory of which the program is being executed from

    Returns a tuple (2):
        index 0 = the path to the SAS log to be used by SAS program
        index 1 = the local mount point to that same directory
    """
    path = pathlib.Path(args.program_path)
    log_file_name = f"{time.strftime('%H%M%S', time.localtime())}_{path.stem}.log"

    if args.sas_server_logging_dir and args.local_logging_dir:
        # predefined logging directories set
        # SAS windows server
        log_file_sas = str(
            pathlib.PureWindowsPath(args.sas_server_logging_dir) / log_file_name
        )
        log_file_local = str(pathlib.Path(args.local_logging_dir) / log_file_name)
    else:
        # no predefined logging directory set in config
        # potentially used for local installs of SAS - Untested
        logging_dir = path.parent.absolute() / "logs"
        log_file_sas = log_file_local = str(logging_dir / log_file_name)

    return (log_file_sas, log_file_local)


def delete_file_if_exists(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)


def setup_live_log(args: argparse.Namespace, sas: SASsession) -> tuple[str, str] | None:
    """
    Creates the log file at the given directory and checks to see if SAS can see it
    and if so, returns a tuple containing the SAS path and the local path to the log
    file else deletes the created file and returns None
    """
    log_file_sas, log_file_local = prepare_log_files(args)
    # create the local file if it doesnt exist
    pathlib.Path(log_file_local).parent.mkdir(exist_ok=True, parents=True)
    pathlib.Path(log_file_local).touch()
    saspy_logger.info(f"Log file is '{log_file_local}'")
    # this SAS function returns 1 if the dir exists or 0
    sas.submit(f"%LET dir_exists = %SYSFUNC(FILEEXIST({log_file_sas}));")
    print(sas.symget(""))
    # Check if SAS can see the newly created log file
    if sas.symget("dir_exists", int()) == 1:
        return (log_file_sas, log_file_local)
    else:
        message = (
            f"SAS unable to log to file {log_file_sas} or the local file "
            f"{log_file_local} does not exist. Check config in {args.config}\n"
            "Printing log after execution."
        )
        print(message)
        delete_file_if_exists(log_file_local)
        return None


def run_sas_program(args: argparse.Namespace) -> int:
    """
    Runs a SAS program file
    """
    try:
        with open(args.program_path) as f:
            program_code = f.read()

        with get_sas_session() as sas:
            log_file_paths = None

            if args.show_log:
                log_file_paths = setup_live_log(args, sas)

            start_time = time.localtime()
            saspy_logger.info(
                f"Started running program: {args.program_path} at "
                f"{time.strftime('%H:%M:%S', start_time)}",
            )

            if log_file_paths:
                log_file_sas, log_file_local = log_file_paths
                logging_code_suffix = f'PROC PRINTTO LOG="{log_file_sas}"; RUN;\n'
                # prepend code to direct SAS to log to file
                # subsequent PROC PRINTTO calls will override this and break the log
                program_code = logging_code_suffix + program_code
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

                    with open(log_file_local) as log_file:
                        code_runner = ex.submit(
                            sas.submit,
                            code=program_code,
                            printto=True,
                        )
                        loglines = get_new_lines(log_file)
                        for line in loglines:
                            print(line, end="")

                    result = code_runner.result()
                    # delete the log file
                    delete_file_if_exists(log_file_local)
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
        add_help=False,
    )
    config_parser.add_argument(
        "-c",
        "--config",
        help="specify a config file. default is %(default)s",
        metavar="FILE",
        default=CONFIG_FILE,
    )

    config_args, remaining_args = config_parser.parse_known_args()
    logging_defaults = {
        "sas_server_logging_dir": "",
        "local_logging_dir": "",
    }
    if config_args.config:
        config = configparser.ConfigParser()
        config.read(config_args.config)
        logging_defaults.update(dict(config.items("LOGGING")))

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

    parser.set_defaults(**logging_defaults)
    args = parser.parse_args(argv)
    ret = 0

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
