import argparse
import tempfile
from io import StringIO
from unittest import mock

import pytest

import sas_cli._main
from sas_cli._main import main, run_sas_program, valid_sas_file


# fixtures
@pytest.fixture()
def temp_sas_file():
    with tempfile.NamedTemporaryFile(mode="r", suffix=".sas") as temp_file:
        yield temp_file


@pytest.fixture()
def temp_file():
    with tempfile.NamedTemporaryFile(mode="r") as temp_file:
        yield temp_file


@pytest.fixture()
def mock_sas_session(monkeypatch, request):
    def mock_init(self):
        self._io = None

    monkeypatch.setattr(sas_cli._main.SASsession, "__init__", mock_init)
    monkeypatch.setattr(
        sas_cli._main.SASsession,
        "submit",
        lambda self, code: {"LOG": "", "LST": ""},
    )
    monkeypatch.setattr(
        sas_cli._main.SASsession,
        "SYSERR",
        lambda s: 0,
    )
    monkeypatch.setattr(
        sas_cli._main.SASsession,
        "SYSERRORTEXT",
        lambda s: "",
    )


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
def test_run_program(
    monkeypatch,
    mock_sas_session,
    temp_sas_file,
    show_log,
):
    args = argparse.Namespace()
    args.command = "run"
    args.program_path = temp_sas_file.name
    args.show_log = show_log
    program_code = mock.mock_open(read_data="%PUT hello world;")

    with mock.patch("builtins.open", program_code):
        assert run_sas_program(args) == 0


def test_run_program_open_file_error():
    args = argparse.Namespace()
    args.command = "run"
    args.program_path = "non/existent/file/path.sas"
    assert run_sas_program(args) == 1


@pytest.mark.parametrize(
    ("sys_err", "sys_err_text"),
    (
        (
            1012,
            "File WORK.DOESNOTEXIST.DATA does not exist.",
        ),
    ),
)
def test_run_program_sas_error(
    monkeypatch,
    capsys,
    mock_sas_session,
    temp_sas_file,
    sys_err,
    sys_err_text,
):
    args = argparse.Namespace()
    args.command = "run"
    args.program_path = temp_sas_file.name
    args.show_log = False
    program_code = mock.mock_open(read_data="%PUT basic sas code without semi-colon")

    with mock.patch("builtins.open", program_code):
        monkeypatch.setattr(
            sas_cli._main.SASsession,
            "SYSERR",
            lambda s: sys_err,
        )
        monkeypatch.setattr(
            sas_cli._main.SASsession,
            "SYSERRORTEXT",
            lambda s: sys_err_text,
        )
        monkeypatch.setattr("sys.stdin", StringIO("yes"))
        assert run_sas_program(args) > 0
        out, err = capsys.readouterr()
        assert (
            err
            == f"\nAn error occured while running '{args.program_path}': {sys_err}: {sys_err_text}\n"
        )
