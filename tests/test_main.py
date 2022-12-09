import argparse
from io import StringIO
from unittest import mock

import pytest

from sas_cli import _main


@pytest.fixture()
def temp_sas_file(tmp_path):
    f = tmp_path / "f.sas"
    with open(f, "w") as temp_sas_file:
        temp_sas_file.write("%PUT hello world;")
        yield temp_sas_file


@pytest.fixture()
def temp_file(tmp_path):
    f = tmp_path / "f.txt"
    with open(f, "w") as temp_file:
        yield temp_file


# tests
def test_main_trivial():
    assert _main.main(()) == 0


def test_valid_sas_file(temp_sas_file):
    print(temp_sas_file.name)
    assert _main.valid_sas_file(temp_sas_file.name) == temp_sas_file.name


def test_valid_sas_file_error(temp_file):
    with pytest.raises(argparse.ArgumentTypeError):
        _main.valid_sas_file(temp_file.name)


@mock.patch("sas_cli._main.SASsession.__init__", return_value=None)
def test_get_sas_session(MockSASsession):
    assert isinstance(_main.get_sas_session(), _main.SASsession)


@pytest.mark.parametrize(
    "show_log",
    (
        pytest.param(True, id="show log"),
        pytest.param(False, id="hide log"),
    ),
)
@mock.patch("sas_cli._main.SASsession")
def test_run_program(
    MockSASsession,
    temp_sas_file,
    show_log,
):
    args = argparse.Namespace()
    args.command = "run"
    args.program_path = temp_sas_file.name
    args.show_log = show_log
    program_code = mock.mock_open(read_data="%PUT hello world;")
    MockSASsession.return_value.__enter__.return_value.submit = mock.Mock(
        return_value={
            "LOG": "",
            "LST": "",
        },
    )
    MockSASsession.return_value.__enter__.return_value.SYSERR = mock.Mock(
        return_value=0,
    )
    MockSASsession.return_value.__enter__.return_value.SYSERRORTEXT = mock.Mock(
        return_value="",
    )
    with mock.patch("builtins.open", program_code):
        assert _main.run_sas_program(args) == 0


@pytest.mark.parametrize(
    ("sys_err", "sys_err_text"),
    (
        (
            1012,
            "File WORK.DOESNOTEXIST.DATA does not exist.",
        ),
    ),
)
@mock.patch("sas_cli._main.SASsession")
def test_run_program_sas_error(
    MockSASsession,
    monkeypatch,
    temp_sas_file,
    capsys,
    sys_err,
    sys_err_text,
):
    args = argparse.Namespace()
    args.command = "run"
    args.program_path = temp_sas_file.name
    args.show_log = False
    MockSASsession.return_value.__enter__.return_value.submit = mock.Mock(
        return_value={
            "LOG": "",
            "LST": "",
        },
    )
    MockSASsession.return_value.__enter__.return_value.SYSERR = mock.Mock(
        return_value=sys_err,
    )
    MockSASsession.return_value.__enter__.return_value.SYSERRORTEXT = mock.Mock(
        return_value=sys_err_text,
    )
    program_code = mock.mock_open(read_data="")
    with mock.patch("builtins.open", program_code):
        monkeypatch.setattr("sys.stdin", StringIO("yes"))
        assert _main.run_sas_program(args) > 0
        out, err = capsys.readouterr()
        assert (
            err
            == f"\nAn error occured while running '{args.program_path}': \
                {sys_err}: {sys_err_text}\n"
        )
