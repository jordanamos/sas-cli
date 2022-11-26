import argparse
import configparser
import os
from pathlib import Path
from typing import Sequence

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


def valid_sas_file(filepath: str) -> bool:
    try:
        open(filepath)
    except OSError as e:
        message = f"Can't open '{filepath}': {e}"
        raise argparse.ArgumentTypeError(message)

    if not filepath.endswith(".sas"):
        raise argparse.ArgumentTypeError(
            f"The file '{filepath}' is not a valid .sas file"
        )
    return True


def existing_directory(directory: str):
    # TODO write tests
    if not os.path.isdir(directory):
        raise argparse.ArgumentTypeError(
            f"The directory '{directory}' is not a valid directory"
        )
    return directory


def run_program(args: argparse.Namespace, config: dict) -> int:

    program_path = os.path.join(config.get("working_directory", ""), args.program_path)
    if valid_sas_file(program_path):
        print(f"\nRunning program: {program_path}\n")
        with open(program_path) as f:
            program_code = f.read()

        with SASsession() as sas:
            result = sas.submit(program_code)

        # print(result)
        if args.show_log:
            print(result["LOG"])

        return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a SAS program",
    )
    parser.add_argument(
        "-s",
        "--set-working-dir",
        metavar="DIR",
        help="""the full path to the directory you wish to work out of.
           Convenience option to update config.ini. File paths parsed to this program will
           be relative from this directory.""",
        type=existing_directory,
    )
    parser.add_argument(
        "-u",
        "--unset-working-dir",
        action="store_true",
        dest="delete_working_dir",
        help="unsets the current working directory",
    )
    parser.add_argument(
        "-w",
        "--get-working-dir",
        action="store_true",
        help="prints the current working directory",
    )

    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument(
        "program_path",
        metavar="FILE",
        help="the path to the SAS (.sas) program you wish to run relative to the working directory",
    )
    run_parser.add_argument("--show-log", dest="show_log", action="store_true")

    ret = 0
    args = parser.parse_args(argv)

    config = load_config(args)
    working_dir = config.get("working_directory")

    if args.get_working_dir:
        if working_dir is None or working_dir == "":
            print("No current working directory set")
        else:
            print(f"Current working directory is '{working_dir}'")

    if args.command == "run":
        ret = run_program(args, config)

    return ret


if __name__ == "__main__":
    raise SystemExit(main())
