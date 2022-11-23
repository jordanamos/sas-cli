from argparse import ArgumentTypeError

import pytest

from sas_cli._main import main, valid_sas_file


def test_main_error_with_empty_string():
    with pytest.raises(SystemExit):
        main(([""]))


def test_main_error_with_non_existent_file():
    with pytest.raises(SystemExit):
        main((["./path/to/nonexistent/sas/file.sas"]))


def test_valid_sas_file():
    test_program_path = "testing/test_program.sas"
    assert valid_sas_file(test_program_path) == test_program_path


def test_valid_sas_file_error():
    with pytest.raises(ArgumentTypeError):
        valid_sas_file("./path/to/nonexistent/sas/file.sas")
