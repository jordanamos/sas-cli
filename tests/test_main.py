from argparse import ArgumentTypeError

import pytest

from sas_cli._main import main, valid_sas_file


@pytest.mark.parametrize("input_main", ("", "./path/to/nonexistent/sas/file.sas"))
def test_main_error(input_main):
    with pytest.raises(SystemExit):
        main(([input_main]))


def test_valid_sas_file(temp_sas_file):
    assert valid_sas_file(temp_sas_file.name) == temp_sas_file.name


def test_valid_sas_file_error(non_existent_file_path):
    with pytest.raises(ArgumentTypeError):
        valid_sas_file(non_existent_file_path)
