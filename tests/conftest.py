import tempfile

import pytest


@pytest.fixture
def temp_sas_file():
    with tempfile.NamedTemporaryFile(mode="r", suffix=".sas", dir=".") as temp_file:
        yield temp_file
