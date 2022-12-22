import argparse
import os
from unittest import mock

import pytest

from sas_cli import _main


@pytest.fixture(params=[{"syserr": 0, "syserrtext": ""}])
def mock_sas_session(request):

    with mock.patch("sas_cli._main.SASsession") as sas:

        sas.SYSERR = mock.Mock(
            return_value=request.param.get("syserr", 0),
        )
        sas.SYSERRORTEXT = mock.Mock(
            return_value=request.param.get("syserrtext", ""),
        )
        yield sas


# tests
def test_main_trivial():
    with mock.patch("sas_cli._main.SASsession"):
        assert _main.main(()) == 0


def test_main_trivial_noop():
    with pytest.raises(SystemExit):
        _main.main(("--help",))


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
    with mock.patch("sas_cli._main.get_sas_data", return_value=0) as get_sas_data:
        _main.main(["data", "test_table"])
        get_sas_data.assert_called_once()


@pytest.mark.usefixtures("mock_sas_session")
def test_run_sas_program_is_called(tmp_path):
    f = tmp_path / "f.sas"
    f.write_text("%PUT hello world;")
    with mock.patch("sas_cli._main.run_sas_program", return_value=0) as run_sas_program:
        _main.main(["run", str(f)])
        run_sas_program.assert_called_once()


@pytest.mark.usefixtures("mock_sas_session")
def test_get_sas_lib_is_called():
    with mock.patch("sas_cli._main.get_sas_lib", return_value=0) as get_sas_lib:
        _main.main(["lib", "c_tst"])
        get_sas_lib.assert_called_once()


def test_valid_sas_file(tmp_path):
    f = tmp_path / "f.sas"
    f.touch()
    assert _main.valid_sas_file(str(f)) == str(f)


def test_valid_sas_file_invalid_ext(tmp_path, capsys):
    f = tmp_path / "f.txt"
    f.touch()
    with pytest.raises(argparse.ArgumentTypeError):
        _main.valid_sas_file(str(f))


def test_valid_sas_file_os_error(tmp_path, capsys):
    f = tmp_path / "f.sas"
    with pytest.raises(argparse.ArgumentTypeError):
        _main.valid_sas_file(str(f))


@pytest.mark.parametrize(("info_only", "ret"), ((True, 1), (False, 0)))
def test_get_sas_data(info_only, ret, capsys):
    args = mock.Mock()
    args.info_only = info_only

    with mock.patch("sas_cli._main.SASsession") as sas:
        sasdata = sas.return_value.__enter__.return_value.sasdata
        sasdata.return_value.to_df = mock.Mock(return_value=ret)
        sasdata.return_value.columnInfo = mock.Mock(return_value=ret)

        assert _main.get_sas_data(args) == 0
        sasdata.assert_called_once()
        sasdata.return_value.columnInfo.assert_called_once()
        if not info_only:
            sasdata.return_value.to_df.assert_called_once()
        out, err = capsys.readouterr()
        assert out == (str(ret) + "\n")


def test_get_sas_data_error():
    args = mock.Mock()
    with mock.patch("sas_cli._main.SASsession") as sas:
        sasdata = sas.return_value.__enter__.return_value.sasdata
        sasdata.return_value.columnInfo = mock.Mock(side_effect=ValueError)
        assert _main.get_sas_data(args) == 1
        sasdata.return_value.columnInfo.assert_called_once()


def test_get_sas_lib():
    args = mock.Mock()
    with mock.patch("sas_cli._main.SASsession"):
        assert _main.get_sas_lib(args) == 0


def test_get_outputs(tmp_path):
    f = tmp_path / "f.txt"
    f.write_text(
        r"""
        /* JOBSPLIT: LIBNAME C_TST SPDE '\\tmp1234\SASData\unit\' */
        /* JOBSPLIT: DATASET OUTPUT SEQ WORK._TMP.DATA */
        /* JOBSPLIT: DATASET OUTPUT SEQ WORK._TMP.DATA */
        /* JOBSPLIT: DATASET OUTPUT SEQ WORK._TMP2.DATA */
        /* JOBSPLIT: LIBNAME WORK V9 'G:\SAS_Work\_TD3592_Jfgfdg1343_\Prc2' */
        /* JOBSPLIT: DATASET INPUT MULTI C_TST.TEST_DATABASE.INDEX */
        /* JOBSPLIT: LIBNAME C_TST SPDE '\\tmp1234\SASData\unit\' */
        /* JOBSPLIT: FILE OUTPUT \\tmp1234\124051_test_program.log.txt */
    """
    )
    expected = {
        "DATASET": {"WORK._TMP2.DATA", "WORK._TMP.DATA"},
        "FILE": {"\\\\tmp1234\\124051_test_program.log.txt"},
    }
    assert _main.get_outputs(f) == expected


def test_get_outputs_error(tmp_path, capsys):
    f = tmp_path / "f.txt"
    assert _main.get_outputs(f) is None
    out, err = capsys.readouterr()
    assert err is not None


def test_run_program_simple(tmp_path, capsys):
    f = tmp_path / "f.sas"
    f.write_text("%PUT hello world;")
    args = mock.Mock()
    args.program_path = f
    args.show_log = True
    with mock.patch("sas_cli._main.SASsession") as sas:
        log_msg = "log line 1"
        sas.SYSERR = mock.Mock(return_value=0)
        sas.SYSERRORTEXT = mock.Mock(return_value="")
        sas.submit = mock.Mock(return_value={"LOG": log_msg, "LST": ""})
        assert _main.run_sas_program_simple(sas, args) == 0
        out, err = capsys.readouterr()
        assert out == log_msg


@pytest.mark.parametrize(
    "mock_sas_session",
    [
        dict(
            syserr=1012,
            syserrtext="SAS error message",
        ),
        dict(
            syserr=0,
            syserrtext="SAS error message",
        ),
        dict(
            syserr=1012,
            syserrtext="",
        ),
    ],
    indirect=True,
)
def test_run_program_simple_runtime_error(
    mock_sas_session,
    capsys,
    tmp_path,
):
    f = tmp_path / "f.sas"
    f.write_text("%PUT hello world;")
    args = mock.Mock()
    args.program_path = f
    args.show_log = False
    assert _main.run_sas_program_simple(mock_sas_session, args) == 1
    out, err = capsys.readouterr()
    assert (
        err == f"\nAn error occured while running '{args.program_path}': "
        f"{mock_sas_session.SYSERR.return_value}: "
        f"{mock_sas_session.SYSERRORTEXT.return_value}\n"
    )


@pytest.mark.parametrize(
    ("sas_dir", "local_dir"),
    (
        (None, None),
        ("tmp_path", "tmp_path"),
        (None, "tmp_path"),
        ("tmp_path", None),
    ),
)
@mock.patch("sas_cli._main.SASsession")
def test_run_program_diverts_to_run_simple(
    sas,
    sas_dir,
    local_dir,
    request,
    capsys,
    tmp_path,
    monkeypatch,
):
    p = tmp_path / "prog.sas"
    p.write_text("%PUT hello world;")

    args = mock.Mock()
    args.program_path = p
    args.show_log = False
    args.sas_server_logging_dir = request.getfixturevalue(sas_dir) if sas_dir else None
    args.local_logging_dir = request.getfixturevalue(local_dir) if local_dir else None

    sas_enter = sas.return_value.__enter__.return_value
    sas_enter.symget.return_value = mock.Mock(return_value=0)

    with mock.patch("sas_cli._main.run_sas_program_simple") as run_simple:
        monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
        run_simple.return_value = 0
        assert _main.run_sas_program(args) == 0
        run_simple.assert_called_once()
        out, err = capsys.readouterr()
        assert not os.path.exists(tmp_path / "1234_prog.log")


iter = 0


@mock.patch("sas_cli._main.concurrent.futures.ThreadPoolExecutor.__enter__")
def test_run_program_live_log(
    threadpoolexecutor,
    tmp_path,
    monkeypatch,
):

    p = tmp_path / "prog.sas"
    p.write_text("%PUT hello world;")

    args = mock.Mock()
    args.program_path = p
    args.show_log = True
    args.sas_server_logging_dir = tmp_path
    args.local_logging_dir = tmp_path

    log = tmp_path / "1234_prog.log"

    def submit(*args, **kwargs):
        log.write_text("log line 1\nlog line 2\nlog line 3\n")
        scaproc = tmp_path / "1234_prog_scaproc.txt"
        scaproc.write_text("/* JOBSPLIT: DATASET OUTPUT SEQ WORK._TMP.DATA */")
        return mock.DEFAULT

    global iter

    def run_for_number_of_lines():
        global iter
        iter += 1
        return iter <= (len(open(log).readlines()) + 1)

    with mock.patch("sas_cli._main.SASsession") as sas:
        sas_enter = sas.return_value.__enter__.return_value
        sas_enter.symget = mock.Mock(return_value=1)
        sas_enter.submit = mock.Mock(return_value={"LOG": "", "LST": ""})

        monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
        threadpoolexecutor.return_value.submit = mock.Mock(side_effect=submit)
        threadpoolexecutor.return_value.submit.return_value.running = mock.Mock(
            side_effect=run_for_number_of_lines
        )
        assert _main.run_sas_program(args) == 0
        assert os.path.exists(log)
    iter = 0


def test_run_program_no_live_log(
    tmp_path,
    monkeypatch,
):

    p = tmp_path / "prog.sas"
    p.write_text("%PUT hello world;")

    args = mock.Mock()
    args.program_path = p
    args.show_log = False
    args.sas_server_logging_dir = tmp_path
    args.local_logging_dir = tmp_path

    log = tmp_path / "1234_prog.log"

    with mock.patch("sas_cli._main.SASsession") as sas:
        sas_enter = sas.return_value.__enter__.return_value
        sas_enter.symget = mock.Mock(return_value=1)
        sas_enter.submit = mock.Mock(return_value={"LOG": "", "LST": ""})
        monkeypatch.setattr(_main.time, "strftime", lambda self, _: "1234")
        assert _main.run_sas_program(args) == 0
        assert os.path.exists(log)
