import argparse
import concurrent.futures
import configparser
import importlib.metadata as importlib_metadata
import os
import pathlib
import re
import sys
import time
from collections.abc import Generator
from collections.abc import Sequence
from typing import TextIO

import tabulate
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


def integer_in_range(obs: str) -> int:
    if int(obs) < 1 or int(obs) > MAX_OUTPUT_OBS:
        raise argparse.ArgumentTypeError(
            f"The specified number of output observations '{obs}' must be "
            "between 1 and {MAX_OUTPUT_OBS:,}"
        )
    return int(obs)


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
            "\nSaspy configuration error. Configuration file "
            f"not found or is not valid: {e}"
        )
        print(message, file=sys.stderr)
        raise SASConfigNotValidError(message)


def print_output_info(scaproc_file: pathlib.Path) -> None:

    time_to_wait = 5
    time_counter = 0

    while not scaproc_file.exists() and time_to_wait > time_counter:
        time.sleep(1)
        time_counter += 1

    def get_jobsplit_lines(scaproc_file: TextIO) -> Generator[str, None, None]:
        for line in scaproc_file:
            if "JOBSPLIT" in line and "OUTPUT" in line:
                line = re.sub(r"\/\* JOBSPLIT: ", "", line).replace(" */", "")
                yield line

    # https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/proc/p0k5uaxpaz2uzin1qvbqmmafnqtl.htm
    if os.path.exists(scaproc_file):
        with open(scaproc_file) as f:
            loglines = get_jobsplit_lines(f)
            keys = ["DATASET", "FILE"]
            ret: dict[str, set[str]] = {key: set() for key in keys}
            for line in loglines:
                segments = line.split()
                key = segments.pop(0)
                value = segments.pop()
                ret[key].add(value)
        print(f"\nOutput\n\n {tabulate.tabulate(ret, headers=list(ret.keys()))}\n")
    else:
        print(
            f"Unable to get output information (waited {time_to_wait} secs). "
            f"Check {scaproc_file} exists.",
            file=sys.stderr,
        )


def run_sas_program(args: argparse.Namespace) -> int:
    """
    Runs a SAS program file
    """
    try:
        with open(args.program_path) as f:
            program_code = f.read()

        with get_sas_session() as sas:

            sas_write_to_files = False

            log_file_sas = None
            log_file_local = None
            output_file_sas = None
            output_file_local = None
            # attemp to setup live logging and scaproc to handle outputs
            if args.sas_server_logging_dir and args.local_logging_dir:
                args.sas_server_logging_dir = pathlib.PureWindowsPath(
                    args.sas_server_logging_dir
                )
                args.local_logging_dir = pathlib.Path(args.local_logging_dir)

                base_file_name = (
                    f"{time.strftime('%H%M%S', time.localtime())}_"
                    f"{pathlib.Path(args.program_path).stem}"
                )

                log_file_sas = args.sas_server_logging_dir / (base_file_name + ".log")
                log_file_local = args.local_logging_dir / (base_file_name + ".log")
                output_file_sas = args.sas_server_logging_dir / (
                    base_file_name + ".txt"
                )
                output_file_local = args.local_logging_dir / (base_file_name + ".txt")

                # create the local file if it doesnt exist
                log_file_local.parent.mkdir(exist_ok=True, parents=True)
                log_file_local.touch()
                # Check if SAS can see the newly created log file
                # this SAS function returns 1 if the file exists or 0
                sas.submit(f"%LET dir_exists = %SYSFUNC(FILEEXIST({log_file_sas}));")
                if sas.symget("dir_exists", int()) == 1:
                    # setup scaproc for output and loging file
                    program_code = (
                        f'PROC SCAPROC;RECORD "{output_file_sas}"; RUN;\n'
                        + f'PROC PRINTTO LOG="{log_file_sas}"; RUN;\n'
                        + program_code
                    )
                    sas_write_to_files = True
                else:
                    print(
                        f"SAS unable to log to {log_file_sas} or {log_file_local}"
                        f" does not exist. Check config in {args.config}\n"
                    )
                    # delete the file if we created it above
                    log_file_local.unlink(missing_ok=True)

            saspy_logger.info(
                f"Started running {args.program_path} at "
                f"{time.strftime('%H:%M:%S', time.localtime())}",
            )

            if args.show_log and sas_write_to_files:
                # read the log file as it is being written to by SAS.
                # a live log is better than getting the log AFTER the program
                # has executed particularly for longer running programs
                with concurrent.futures.ThreadPoolExecutor() as ex:

                    def read_new_lines(file: TextIO) -> Generator[str, None, None]:
                        while code_runner.running():
                            line = file.readline()
                            if not line:
                                continue
                            yield line

                    with open(str(log_file_local)) as log_file:
                        log_file.seek(0, 2)
                        code_runner = ex.submit(
                            sas.submit,
                            code=program_code,
                            printto=True,
                        )
                        loglines = read_new_lines(log_file)
                        for line in loglines:
                            print(line, end="")

                    result = code_runner.result()
            else:
                result = sas.submit(program_code, printto=True)
                if args.show_log:
                    print(result["LOG"])
                sys_err_text = sas.SYSERRORTEXT()
                sys_err = sas.SYSERR()
                if sys_err_text or sys_err:
                    message = f"{sys_err}: {sys_err_text}"
                    raise RuntimeError(message)

            saspy_logger.info(
                f"Finished at {time.strftime('%H:%M:%S', time.localtime())}",
            )

        if sas_write_to_files:
            with open(str(log_file_local)) as log:
                errors: Generator[list[str], None, None] = (
                    [f"{log_file_local}:{num}", line]
                    for num, line in enumerate(log, 1)
                    if line.startswith("ERROR")
                )
                headers = ["Log Line", "Error Text"]
                print(f"\n{tabulate.tabulate(errors, headers=headers)}")
            if output_file_local is not None:
                print_output_info(output_file_local)
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


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
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

    config_args, _ = config_parser.parse_known_args(argv)

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
        type=integer_in_range,
        help=f"specify the number of output observations \
            between 1 and {MAX_OUTPUT_OBS:,} (default is %(default)s).",
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
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    ret = 0
    if args.command == "run":
        ret = run_sas_program(args)
    elif args.command == "data":
        ret = get_sas_data(args)
    elif args.command == "lib":
        ret = get_sas_lib(args)

    return ret


if __name__ == "__main__":
    raise SystemExit(main())
