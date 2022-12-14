import argparse
import os
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


def test_delete_file_if_exists(temp_file):
    _main.delete_file_if_exists(temp_file.name)
    assert not os.path.exists(temp_file.name)


@mock.patch("sas_cli._main.argparse.Namespace")
def test_prepare_log_files(args, temp_sas_file):
    args.program_path = temp_sas_file.name
    assert isinstance(_main.prepare_log_files(args), tuple)


@mock.patch("sas_cli._main.prepare_log_files")
@mock.patch("sas_cli._main.SASsession")
def test_setup_live_log(
    MockSASsession,
    prepare_log_files,
    temp_file,
):
    MockSASsession.symget.return_value = 1
    prepare_log_files.return_value = (temp_file.name, temp_file.name)
    with mock.patch("sas_cli._main.pathlib.Path"):
        assert (
            _main.setup_live_log(mock.Mock(), MockSASsession)
            == prepare_log_files.return_value
        )


@mock.patch("sas_cli._main.prepare_log_files")
@mock.patch("sas_cli._main.SASsession")
def test_setup_live_log_not_exists(
    MockSASsession,
    prepare_log_files,
    temp_file,
):
    MockSASsession.symget.return_value = 0
    prepare_log_files.return_value = (temp_file.name, temp_file.name)
    with mock.patch("sas_cli._main.pathlib.Path"):
        assert _main.setup_live_log(mock.Mock(), MockSASsession) is None
        assert not os.path.exists(temp_file.name)


@mock.patch("sas_cli._main.SASsession.__init__", return_value=None)
def test_get_sas_session(MockSASsession):
    assert isinstance(_main.get_sas_session(), _main.SASsession)


@mock.patch("sas_cli._main.SASsession")
def test_run_program_no_log(
    MockSASsession,
    temp_sas_file,
):
    args = mock.Mock()
    args.program_path = temp_sas_file.name
    args.show_log = False
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
    args = mock.Mock()
    args.program_path = temp_sas_file.name
    args.show_log = False
    args.command = "run"

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
            err == f"\nAn error occured while running '{args.program_path}': "
            f"{sys_err}: {sys_err_text}\n"
        )
