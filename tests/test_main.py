import argparse
import os
from io import StringIO
from unittest import mock

import pandas as pd
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


@pytest.fixture(params=[{"syserr": 0, "syserrtext": ""}])
def mock_sas_session(request):

    with mock.patch("sas_cli._main.SASsession") as MockSASsession:
        MockSASsession.return_value.__enter__.return_value.submit = mock.Mock(
            return_value={
                "LOG": "",
                "LST": "",
            },
        )
        MockSASsession.return_value.__enter__.return_value.SYSERR = mock.Mock(
            return_value=request.param.get("syserr", 0),
        )
        MockSASsession.return_value.__enter__.return_value.SYSERRORTEXT = mock.Mock(
            return_value=request.param.get("syserrtext", ""),
        )
        yield MockSASsession


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


def test_prepare_log_files(tmp_path, monkeypatch):
    monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
    f = tmp_path / "f.sas"
    args = mock.Mock()
    args.program_path = f.name
    args.sas_server_logging_dir = tmp_path
    args.local_logging_dir = tmp_path
    monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")

    import pathlib

    assert _main.prepare_log_files(args) == (
        str(pathlib.PureWindowsPath(tmp_path / "1234_f.log")),
        str(pathlib.Path(tmp_path / "1234_f.log")),
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


# def test_get_sas_session_exceptions(tmp_path, capsys, monkeypatch):
#     cfg_file = tmp_path / "f.py"
#     cfg_file.write_text("SAS_config_names=['hi']")
#     with mock.patch("saspy.sasbase.SASconfig._find_config") as _find_config:
#         _find_config(cfgfile=cfg_file.name)
#     with mock.patch("sas_cli._main.SASsession.__init__()") as sas:
#         sas = mock.MagicMock(cfgfile=cfg_file.name)
#     mock_sas_session.side_effect = _main.SASConfigNotValidError("")
#     with pytest.raises(_main.SASConfigNotValidError) as e:
#         _main.get_sas_session()
#         out, err = capsys.readouterr()
#         assert err == ("\nSaspy configuration error. Configuration file "
#             f"not found or is not valid: {e}")


def test_run_program_no_log(
    mock_sas_session,
    temp_sas_file,
):
    args = mock.Mock()
    args.program_path = temp_sas_file.name
    args.show_log = False
    program_code = mock.mock_open(read_data="%PUT hello world;")
    with mock.patch("builtins.open", program_code):
        assert _main.run_sas_program(args) == 0


@mock.patch("sas_cli._main.setup_live_log")
def test_run_program_no_live_log(
    setup_live_log,
    mock_sas_session,
    temp_sas_file,
):
    args = mock.Mock()
    args.program_path = temp_sas_file.name
    args.show_log = True
    program_code = mock.mock_open(read_data="%PUT hello world;")
    setup_live_log.return_value = None
    with mock.patch("builtins.open", program_code):
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
    monkeypatch,
    temp_sas_file,
):
    args = mock.Mock()
    args.program_path = temp_sas_file.name
    args.show_log = False
    args.command = "run"
    program_code = mock.mock_open(read_data="")
    with mock.patch("builtins.open", program_code):
        monkeypatch.setattr("sys.stdin", StringIO("no"))
        assert _main.run_sas_program(args) > 0


def test_get_sas_data(mock_sas_session, capsys):

    args = mock.Mock()
    args.dataset = "test_dataset"
    args.info_only = False
    args.command = "data"
    args.libref = "c_ja"
    args.obs = 10
    args.keep = ""
    args.drop = ""
    mock_sas_session.return_value.__enter__.return_value.sasdata = mock.Mock()
    mock_sas_session.return_value.__enter__.return_value.sasdata.return_value.to_df = (
        mock.Mock(return_value=pd.DataFrame())
    )

    assert _main.get_sas_data(args) == 0
    out, err = capsys.readouterr()
    # assert out == str(pd.DataFrame())
