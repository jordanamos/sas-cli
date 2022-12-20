import argparse
from unittest import mock

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
        sas.SYSERR = mock.Mock(
            return_value=request.param.get("syserr", 0),
        )
        sas.SYSERRORTEXT = mock.Mock(
            return_value=request.param.get("syserrtext", ""),
        )
        yield sas


# tests
@pytest.mark.usefixtures("mock_sas_session")
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


@pytest.mark.parametrize(
    "obs",
    (
        0,
        _main.MAX_OUTPUT_OBS + 1,
    ),
)
def test_integer_in_range_error(obs, capsys):
    with pytest.raises(argparse.ArgumentTypeError):
        _main.integer_in_range(str(obs))
        out, err = capsys.readouterr()
        assert (
            err == f"The specified number of output observations '{obs}' must be "
            "between 1 and {_main.MAX_OUTPUT_OBS:,}"
        )


@pytest.mark.parametrize(
    "obs",
    (
        1,
        _main.MAX_OUTPUT_OBS - 1,
        _main.MAX_OUTPUT_OBS,
    ),
)
def test_integer_in_range(obs):
    assert _main.integer_in_range(str(obs)) == int(obs)


@pytest.mark.usefixtures("mock_sas_session")
def test_get_sas_data_is_called():
    with mock.patch("sas_cli._main.get_sas_data", return_value=0) as m:
        _main.main(["data", "test_table"])
        m.assert_called_once()


@pytest.mark.usefixtures("mock_sas_session")
def test_run_sas_program_is_called(tmp_path):
    f = tmp_path / "f.sas"
    f.write_text("%PUT hello world;")
    with mock.patch("sas_cli._main.run_sas_program", return_value=0) as m:
        _main.main(["run", str(f)])
        m.assert_called_once()


@pytest.mark.usefixtures("mock_sas_session")
def test_get_sas_lib_is_called():
    with mock.patch("sas_cli._main.get_sas_lib", return_value=0) as m:
        _main.main(["lib", "c_tst"])
        m.assert_called_once()


def test_valid_sas_file(tmp_path):
    f = tmp_path / "f.sas"
    f.touch()
    assert _main.valid_sas_file(str(f)) == str(f)


def test_valid_sas_file_invalid_ext(tmp_path, capsys):
    f = tmp_path / "f.txt"
    f.touch()
    with pytest.raises(argparse.ArgumentTypeError):
        _main.valid_sas_file(str(f))
        out, err = capsys.readouterr()
        assert err == f"The file '{f}' is not a valid .sas file"


def test_valid_sas_file_os_error(tmp_path, capsys):
    f = tmp_path / "f.sas"
    with pytest.raises(argparse.ArgumentTypeError) as e:
        _main.valid_sas_file(str(f))
        out, err = capsys.readouterr()
        assert err == f"Can't open '{f}': {e}"


# def test_prepare_log_files(tmp_path, monkeypatch):
#     monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
#     f = tmp_path / "f.sas"
#     args = mock.Mock()
#     args.program_path = f.name
#     args.sas_server_logging_dir = tmp_path
#     args.local_logging_dir = tmp_path
#     monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
#     assert _main.prepare_log_files(args) == (
#         str(_main.pathlib.PureWindowsPath(tmp_path / "1234_f.log")),
#         str(_main.pathlib.Path(tmp_path / "1234_f.log")),
#     )


# @mock.patch("sas_cli._main.argparse.Namespace")
# def test_prepare_log_files_creates_logs_dir(args, tmp_path, monkeypatch):
#     monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
#     f = tmp_path / "f.sas"
#     args = mock.Mock()
#     args.program_path = f
#     args.sas_server_logging_dir = ""
#     args.local_logging_dir = ""
#     monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
#     assert _main.prepare_log_files(args) == (
#         str(tmp_path / "logs" / "1234_f.log"),
#         str(tmp_path / "logs" / "1234_f.log"),
#     )


# @mock.patch("sas_cli._main.SASsession")
# def test_setup_live_log(
#     MockSASsession,
#     monkeypatch,
#     tmp_path,
# ):
#     f = tmp_path / "f.sas"
#     args = mock.Mock()
#     args.config = "config.ini"
#     args.program_path = f
#     args.sas_server_logging_dir = tmp_path
#     args.local_logging_dir = tmp_path
#     MockSASsession.symget.return_value = 1
#     monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
#     assert _main.setup_live_log(args, MockSASsession) == _main.prepare_log_files(args)
#     assert os.path.exists(tmp_path / "1234_f.log")


# @mock.patch("sas_cli._main.SASsession")
# def test_setup_live_log_not_exists(
#     MockSASsession,
#     monkeypatch,
#     tmp_path,
# ):
#     f = tmp_path / "f.sas"
#     args = mock.Mock()
#     args.config = "config.ini"
#     args.program_path = f
#     args.sas_server_logging_dir = ""
#     args.local_logging_dir = ""
#     MockSASsession.symget.return_value = 0
#     monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
#     assert _main.setup_live_log(args, MockSASsession) is None
#     assert not os.path.exists(tmp_path / "1234_f.log")


@mock.patch("sas_cli._main.SASsession.__init__", return_value=None)
def test_get_sas_session(MockSASsession):
    assert isinstance(_main.get_sas_session(), _main.SASsession)


@pytest.mark.parametrize(
    "exception_type",
    (
        _main.SASConfigNotValidError,
        _main.SASConfigNotFoundError,
        _main.SASIONotSupportedError,
        AttributeError,
    ),
)
@mock.patch("sas_cli._main.SASsession.__init__", return_value=None)
def test_get_sas_session_config_error(MockSASsession, exception_type, tmp_path, capsys):
    f = tmp_path / " config.ini"
    MockSASsession.side_effect = exception_type(f)
    with pytest.raises(_main.SASConfigNotValidError) as e:
        _main.get_sas_session()
        out, err = capsys.readouterr()
        assert err == "\nSaspy configuration error. Configuration file "
        f"not found or is not valid: {e}"


@mock.patch("sas_cli._main.SASsession.__init__", return_value=None)
def test_get_sas_session_ioconnection_error(MockSASsession):
    MockSASsession.side_effect = _main.SASIOConnectionError("msg")
    with pytest.raises(_main.SASIOConnectionError):
        _main.get_sas_session()


def test_run_program_trivial(
    mock_sas_session,
    tmp_path,
):
    f = tmp_path / "f.sas"
    f.write_text("%PUT hello world")
    args = mock.Mock()
    args.program_path = f
    args.show_log = False
    args.sas_server_logging_dir = None
    args.local_logging_dir = None
    assert _main.run_sas_program(mock_sas_session, args) == 0


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
def test_run_program_runtime_error(
    mock_sas_session,
    capsys,
    tmp_path,
):
    f = tmp_path / "f.sas"
    f.write_text("%PUT hello world;")
    args = mock.Mock()
    args.program_path = f
    args.show_log = False
    args.sas_server_logging_dir = None
    args.local_logging_dir = None
    assert _main.run_sas_program(mock_sas_session, args) == 1
    out, err = capsys.readouterr()
    assert (
        err == f"\nAn error occured while running '{args.program_path}': "
        f"{mock_sas_session.SYSERR.return_value}: "
        f"{mock_sas_session.SYSERRORTEXT.return_value}\n"
    )


# @mock.patch("sas_cli._main.setup_live_log")
# def test_run_program_no_live_log(setup_live_log, mock_sas_session, tmp_path):
#     f = tmp_path / "f.sas"
#     f.write_text("%PUT hello world")
#     args = mock.Mock()
#     args.program_path = f
#     args.show_log = True
#     setup_live_log.return_value = None
#     assert _main.run_sas_program(args) == 0


def test_get_sas_data():
    args = mock.Mock()
    args.dataset = "test_dataset"
    args.info_only = False
    args.command = "data"
    args.libref = "c_tst"
    args.obs = 10
    mock_sas = mock.MagicMock()
    assert _main.get_sas_data(mock_sas, args) == 0


def test_get_sas_data_error(mock_sas_session, capsys):
    args = mock.Mock()
    args.dataset = "test_dataset"
    args.info_only = False
    args.command = "data"
    args.libref = "c_tst"
    args.obs = 10
    mock_sas_session.sasdata = mock.Mock(side_effect=ValueError)
    with pytest.raises(ValueError) as e:
        assert _main.get_sas_data(mock_sas_session, args) == 1
        out, err = capsys.readouterr()
        assert err == e


def test_get_sas_lib(mock_sas_session):
    args = mock.Mock()
    assert _main.get_sas_lib(mock_sas_session, args) == 0
