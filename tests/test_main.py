from argparse import ArgumentTypeError

import pytest

from sas_cli._main import main, valid_sas_file


def test_main_error_with_empty_string():
    with pytest.raises(SystemExit):
        main(([""]))


def test_main_error_with_non_existent_file(non_existent_file_path):
    with pytest.raises(SystemExit):
        main(([non_existent_file_path]))


def test_valid_sas_file(temp_sas_file):
    assert valid_sas_file(temp_sas_file.name) == temp_sas_file.name


def test_valid_sas_file_error(non_existent_file_path):
    with pytest.raises(ArgumentTypeError):
        valid_sas_file(non_existent_file_path)
