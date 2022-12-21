import argparse
import concurrent.futures
import configparser
import importlib.metadata as importlib_metadata
import re
import sys
import time
from collections.abc import Generator
from collections.abc import Sequence
from pathlib import Path
from pathlib import PureWindowsPath
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


def get_outputs(scaproc_file: Path) -> dict[str, set[str]] | None:
    def get_jobsplit_lines(scaproc_file: TextIO) -> Generator[str, None, None]:
        for line in scaproc_file:
            if "JOBSPLIT" in line and "OUTPUT" in line:
                line = re.sub(r"\/\* JOBSPLIT: ", "", line).replace(" */", "")
                yield line

    # https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/proc/p0k5uaxpaz2uzin1qvbqmmafnqtl.htm
    if scaproc_file.exists():
        with open(scaproc_file) as f:
            loglines = get_jobsplit_lines(f)
            keys = ["DATASET", "FILE"]
            ret: dict[str, set[str]] = {key: set() for key in keys}
            for line in loglines:
                segments = line.split()
                key = segments.pop(0)
                value = segments.pop()
                ret[key].add(value)
            return ret
    else:
        print(
            f"Unable to get output information check {scaproc_file} exists.",
            file=sys.stderr,
        )
        return None


def run_sas_program_simple(sas: SASsession, args: argparse.Namespace) -> int:
    try:
        with open(args.program_path) as f:
            program_code = f.read()

        saspy_logger.info(
            f"Started running {args.program_path} at "
            f"{time.strftime('%H:%M:%S', time.localtime())}",
        )
        result = sas.submit(program_code, printto=True)
        if args.show_log:
            print(result["LOG"], end="")
        sys_err_text = sas.SYSERRORTEXT()
        sys_err = sas.SYSERR()
        if sys_err_text or sys_err:
            message = f"{sys_err}: {sys_err_text}"
            raise RuntimeError(message)
    except RuntimeError as e:
        print(
            f"\nAn error occured while running '{args.program_path}': {e}",
            file=sys.stderr,
        )
        return 1
    return 0


def run_sas_program(sas: SASsession, args: argparse.Namespace) -> int:
    """
    Runs a SAS program file
    """
    try:
        if not (args.sas_server_logging_dir and args.local_logging_dir):
            return run_sas_program_simple(sas, args)

        # attempt to setup live logging and scaproc to handle outputs
        args.sas_server_logging_dir = PureWindowsPath(args.sas_server_logging_dir)
        args.local_logging_dir = Path(args.local_logging_dir)

        base_file_name = (
            f"{time.strftime('%H%M%S', time.localtime())}_"
            f"{Path(args.program_path).stem}"
        )

        log_file_sas = args.sas_server_logging_dir / (base_file_name + ".log")
        log_file_local = args.local_logging_dir / (base_file_name + ".log")
        output_file_sas = args.sas_server_logging_dir / (
            base_file_name + "_scaproc.txt"
        )
        output_file_local = args.local_logging_dir / (base_file_name + "_scaproc.txt")
        log_file_local.parent.mkdir(exist_ok=True, parents=True)
        log_file_local.touch()
        # Check if SAS can see the newly created log file
        # this SAS function returns 1 if the file exists or 0
        sas.submit(f"%LET dir_exists = %SYSFUNC(FILEEXIST({log_file_sas}));")
        if sas.symget("dir_exists", int()) == 1:

            with open(args.program_path) as f:
                program_code = f.read()

            program_code = (
                f'PROC PRINTTO LOG="{log_file_sas}"; RUN;\n'
                + f'PROC SCAPROC; RECORD "{output_file_sas}"; RUN;\n'
                + program_code
                + "\nPROC SCAPROC; WRITE; RUN;"
            )
            if args.show_log:
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
            else:
                sas.submit(program_code, printto=True)

            with open(str(log_file_local)) as log:
                errors: Generator[list[str], None, None] = (
                    [f"{log_file_local}:{num}", error_line]
                    for num, error_line in enumerate(log, 1)
                    if error_line.startswith("ERROR")
                )
                headers = ["Log Line", "Error Text"]
                print(f"Errors:\n\n{tabulate.tabulate(errors, headers=headers)}")
                outputs = get_outputs(output_file_local)
                if outputs:
                    table = tabulate.tabulate(outputs, headers=list(outputs.keys()))
                    print(f"\nOutputs:\n\n {table}\n")
        else:
            print(
                f"SAS unable to log to '{log_file_sas}' or '{log_file_local}'"
                f" does not exist. Check config in {args.config}\n"
            )
            # delete the file if we created it above
            log_file_local.unlink(missing_ok=True)
            return run_sas_program_simple(sas, args)
    except (
        SASIOConnectionError,
        SASConfigNotValidError,
    ):
        return 1
    return 0


def get_sas_lib(sas: SASsession, args: argparse.Namespace) -> int:
    """
    List the members or datasets within a SAS library
    """
    list_of_tables = sas.list_tables(
        args.libref,
        results="pandas",
    )
    if list_of_tables is not None:
        print(list_of_tables)
    return 0


def get_sas_data(sas: SASsession, args: argparse.Namespace) -> int:
    """
    Get sample data from a SAS dataset
    or info about the dataset if the --info-only(-i) flag is set
    (PROC DATASETS)
    """
    try:
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
    with get_sas_session() as sas:
        if args.command == "run":
            ret = run_sas_program(sas, args)
        elif args.command == "data":
            ret = get_sas_data(sas, args)
        elif args.command == "lib":
            ret = get_sas_lib(sas, args)

    return ret


if __name__ == "__main__":
    raise SystemExit(main())
