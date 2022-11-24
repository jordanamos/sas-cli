import tempfile
from argparse import ArgumentTypeError

import pytest

from sas_cli._main import main, valid_sas_file


@pytest.fixture
def temp_sas_file():
    with tempfile.NamedTemporaryFile(mode="r", suffix=".sas", dir=".") as temp_file:
        yield temp_file


@pytest.fixture
def temp_file():
    with tempfile.NamedTemporaryFile(mode="r", dir=".") as temp_file:
        yield temp_file


@pytest.mark.parametrize("test_input", ("", "./path/to/nonexistent/sas/file.sas"))
def test_main_error(test_input):
    with pytest.raises(SystemExit):
        main(([test_input]))


def test_valid_sas_file(temp_sas_file):
    assert valid_sas_file(temp_sas_file.name) == temp_sas_file.name


def test_valid_sas_file_error(temp_file):
    with pytest.raises(ArgumentTypeError):
        valid_sas_file(temp_file.name)
