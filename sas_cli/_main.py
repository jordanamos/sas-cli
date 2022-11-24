import argparse
import configparser
import os
from typing import Sequence

from saspy import SASsession

CONFIG_FILE = "config.ini"


def set_working_directory(directory: str):
    # TODO write tests
    with open(CONFIG_FILE, "w") as config_file:
        config = configparser.ConfigParser()
        config["working directory"] = {"path": directory}
        config.write(config_file)


def delete_working_dir():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    if config.remove_option(section="working directory", option="path"):
        with open(CONFIG_FILE, "w") as config_file:
            config.write(config_file)


def valid_sas_file(filepath: str) -> str:
    try:
        open(filepath)
    except OSError as e:
        message = f"can't open '{filepath}': {e}"
        raise argparse.ArgumentTypeError(message)

    if not filepath.endswith(".sas"):
        raise argparse.ArgumentTypeError(
            f"the file '{filepath}' is not a valid .sas file"
        )
    return filepath


def existing_directory(directory):
    # TODO write tests
    if not os.path.isdir(directory):
        raise argparse.ArgumentTypeError(
            f" the directory '{directory}' is not a valid directory"
        )
    return directory


def run_program(args: argparse.Namespace) -> int:
    with open(args.program_path) as f:
        program_code = f.read()

    with SASsession() as sas:
        result = sas.submit(program_code)

    if args.show_log:
        print(result["LOG"])

    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a SAS program",
    )
    parser.add_argument(
        "-w",
        "--set-working-dir",
        metavar="DIR",
        help="the full path to the directory you wish to work out of",
        type=existing_directory,
    )
    parser.add_argument(
        "-d", "--unset-working-dir", action="store_true", dest="delete_working_dir"
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument(
        "program_path",
        metavar="FILE",
        help="the full path to the SAS (.sas) program you wish to run",
        type=valid_sas_file,
    )
    run_parser.add_argument("--show-log", dest="show_log", action="store_true")

    ret = 0
    args = parser.parse_args(argv)
    print(args)

    if args.command is None:
        if args.delete_working_dir:
            delete_working_dir()
        if args.set_working_dir:
            set_working_directory(args.set_working_dir)
    else:
        if args.command == "run":
            ret = run_program(args)

    return ret


if __name__ == "__main__":
    raise SystemExit(main())
