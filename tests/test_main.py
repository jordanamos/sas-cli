import argparse
import os
from io import StringIO
from unittest import mock

import pandas as pd
import pytest

from sas_cli import _main


@pytest.fixture(params=[{"syserr": 0, "syserrtext": ""}])
def mock_sas_session(request):

    with mock.patch("sas_cli._main.SASsession") as sas:
        sas.return_value.__enter__.return_value.submit = mock.Mock(
            return_value={
                "LOG": "",
                "LST": "",
            },
        )
        sas.return_value.__enter__.return_value.SYSERR = mock.Mock(
            return_value=request.param.get("syserr", 0),
        )
        sas.return_value.__enter__.return_value.SYSERRORTEXT = mock.Mock(
            return_value=request.param.get("syserrtext", ""),
        )
        yield sas


# tests
def test_main_trivial():
    assert _main.main(()) == 0


def test_parse_config_args(tmp_path):
    f = tmp_path / "config.ini"
    f.write_text(
        f"""[LOGGING]
        sas_server_logging_dir = {tmp_path / "sas_log"}
        local_logging_dir = {tmp_path / "local_log"}
    """
    )
    args = _main.parse_args(["--config", str(f)])
    assert args.sas_server_logging_dir == str(tmp_path / "sas_log")
    assert args.local_logging_dir == str(tmp_path / "local_log")


def test_valid_sas_file(tmp_path):
    f = tmp_path / "f.sas"
    f.touch()
    assert _main.valid_sas_file(str(f)) == str(f)


def test_valid_sas_file_error(tmp_path):
    f = tmp_path / "f.txt"
    f.touch()
    with pytest.raises(argparse.ArgumentTypeError):
        _main.valid_sas_file(str(f))


def test_delete_file_if_exists(tmp_path):
    f = tmp_path / "f.txt"
    f.touch()
    _main.delete_file_if_exists(str(f))
    assert not os.path.exists(str(f))


def test_prepare_log_files(tmp_path, monkeypatch):
    monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
    f = tmp_path / "f.sas"
    args = mock.Mock()
    args.program_path = f.name
    args.sas_server_logging_dir = tmp_path
    args.local_logging_dir = tmp_path
    monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
    assert _main.prepare_log_files(args) == (
        str(_main.pathlib.PureWindowsPath(tmp_path / "1234_f.log")),
        str(_main.pathlib.Path(tmp_path / "1234_f.log")),
    )


@mock.patch("sas_cli._main.argparse.Namespace")
def test_prepare_log_files_creates_logs_dir(args, tmp_path, monkeypatch):
    monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
    f = tmp_path / "f.sas"
    args = mock.Mock()
    args.program_path = f
    args.sas_server_logging_dir = ""
    args.local_logging_dir = ""
    monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
    assert _main.prepare_log_files(args) == (
        str(tmp_path / "logs" / "1234_f.log"),
        str(tmp_path / "logs" / "1234_f.log"),
    )


@mock.patch("sas_cli._main.SASsession")
def test_setup_live_log(
    MockSASsession,
    monkeypatch,
    tmp_path,
):
    f = tmp_path / "f.sas"
    args = mock.Mock()
    args.config = "config.ini"
    args.program_path = f
    args.sas_server_logging_dir = tmp_path
    args.local_logging_dir = tmp_path
    MockSASsession.symget.return_value = 1
    monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
    assert _main.setup_live_log(args, MockSASsession) == _main.prepare_log_files(args)
    assert os.path.exists(tmp_path / "1234_f.log")


@mock.patch("sas_cli._main.SASsession")
def test_setup_live_log_not_exists(
    MockSASsession,
    monkeypatch,
    tmp_path,
):
    f = tmp_path / "f.sas"
    args = mock.Mock()
    args.config = "config.ini"
    args.program_path = f
    args.sas_server_logging_dir = ""
    args.local_logging_dir = ""
    MockSASsession.symget.return_value = 0
    monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
    assert _main.setup_live_log(args, MockSASsession) is None
    assert not os.path.exists(tmp_path / "1234_f.log")


@mock.patch("sas_cli._main.SASsession.__init__", return_value=None)
def test_get_sas_session(MockSASsession):
    assert isinstance(_main.get_sas_session(), _main.SASsession)


def test_run_program_trivial(
    mock_sas_session,
    tmp_path,
):
    f = tmp_path / "f.sas"
    f.write_text("%PUT hello world")
    args = mock.Mock()
    args.program_path = f
    args.show_log = False
    assert _main.run_sas_program(args) == 0


@pytest.mark.parametrize(
    "mock_sas_session",
    [
        dict(
            syserr=1012,
            syserrtext="File WORK.DOESNOTEXIST.DATA does not exist.",
        ),
    ],
    indirect=True,
)
def test_run_program_sas_error(
    mock_sas_session,
    capsys,
    monkeypatch,
    tmp_path,
):
    f = tmp_path / "f.sas"
    f.write_text("%PUT hello world;")
    args = mock.Mock()
    args.program_path = f
    args.show_log = False
    monkeypatch.setattr("sys.stdin", StringIO("no"))
    assert _main.run_sas_program(args) > 0
    out, err = capsys.readouterr()
    syserr = mock_sas_session.return_value.__enter__.return_value.SYSERR.return_value
    syserrortext = (
        mock_sas_session.return_value.__enter__.return_value.SYSERRORTEXT.return_value
    )
    assert (
        err == f"\nAn error occured while running '{args.program_path}': "
        f"{syserr}: {syserrortext}\n"
    )


@mock.patch("sas_cli._main.setup_live_log")
def test_run_program_no_live_log(setup_live_log, mock_sas_session, tmp_path):
    f = tmp_path / "f.sas"
    f.write_text("%PUT hello world")
    args = mock.Mock()
    args.program_path = f
    args.show_log = True
    setup_live_log.return_value = None
    assert _main.run_sas_program(args) == 0


def test_get_sas_data(mock_sas_session):
    args = mock.Mock()
    args.dataset = "test_dataset"
    args.info_only = False
    args.command = "data"
    args.libref = "c_tst"
    args.obs = 10
    mock_sas_session.return_value.__enter__.return_value.sasdata = mock.Mock()
    mock_sas_session.return_value.__enter__.return_value.sasdata.return_value.to_df = (
        mock.Mock(return_value=pd.DataFrame())
    )
    assert _main.get_sas_data(args) == 0


def test_get_sas_data_error(mock_sas_session, capsys):
    args = mock.Mock()
    args.dataset = "test_dataset"
    args.info_only = False
    args.command = "data"
    args.libref = "c_tst"
    args.obs = 10
    mock_sas_session.return_value.__enter__.return_value.sasdata = mock.Mock(
        side_effect=ValueError
    )
    with pytest.raises(ValueError) as e:
        assert _main.get_sas_data(args) == 1
        out, err = capsys.readouterr()
        assert err == e
