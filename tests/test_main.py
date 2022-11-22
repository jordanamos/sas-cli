import pytest

from sas_cli._main import main


def test_main_error_with_empty_string(capsys):
    with pytest.raises(Exception):
        main(([""]))
