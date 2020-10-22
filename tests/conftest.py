import pytest
import pathlib


@pytest.fixture()
def migrations_dir():
    """Migrations dir"""
    return pathlib.Path(__file__).with_name('migrations')
