import tempfile

import pytest


@pytest.fixture
def non_existent_file_path():
    return "./path/to/nonexistent/sas/file.sas"


@pytest.fixture
def temp_sas_file():
    with tempfile.NamedTemporaryFile(
        mode="r", suffix=".sas", dir="./testing"
    ) as temp_file:
        yield temp_file
