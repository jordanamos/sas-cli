import argparse
import tempfile
from unittest import mock

import pytest
import saspy

from sas_cli._main import main, run_sas_program, valid_sas_file


# fixtures
@pytest.fixture
def temp_sas_file(scope="session"):
    with tempfile.NamedTemporaryFile(
        mode="r", suffix=".sas", dir="./testing"
    ) as temp_file:
        yield temp_file


@pytest.fixture
def temp_file(scope="session"):
    with tempfile.NamedTemporaryFile(mode="r", dir="./testing") as temp_file:
        yield temp_file


# tests
def test_main_trivial():
    assert main(()) == 0


def test_valid_sas_file(temp_sas_file):
    assert valid_sas_file(temp_sas_file.name) == temp_sas_file.name


def test_valid_sas_file_error(temp_file):
    with pytest.raises(argparse.ArgumentTypeError):
        valid_sas_file(temp_file.name)


@pytest.mark.parametrize(
    "show_log",
    (
        pytest.param(True, id="show log"),
        pytest.param(False, id="hide log"),
    ),
)
def test_run_program(temp_sas_file, show_log):
    args = argparse.Namespace()
    args.command = "run"
    args.program_path = temp_sas_file.name
    args.show_log = show_log
    program_code = mock.mock_open(read_data="%PUT hello world;")

    with mock.patch("builtins.open", program_code):

        def __init__(self):
            self._io = None

        def tst_submit(self, code):
            return {"LOG": "", "LST": ""}

        def SYSERR(self):
            return 0

        def SYSERRORTEXT(self):
            return ""

        with mock.patch.multiple(
            saspy.SASsession,
            __init__=__init__,
            submit=tst_submit,
            SYSERR=SYSERR,
            SYSERRORTEXT=SYSERRORTEXT,
        ):
            assert run_sas_program(args) == 0


def test_run_program_error(temp_file):
    args = argparse.Namespace()
    args.command = "run"
    args.program_path = "sda"
    with pytest.raises(OSError):
        run_sas_program(args)
