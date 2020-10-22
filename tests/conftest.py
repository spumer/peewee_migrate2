import pathlib

import playhouse.db_url
import pytest
import peewee as pw


@pytest.fixture()
def migrations_dir():
    """Migrations dir"""
    return pathlib.Path(__file__).with_name('migrations')


@pytest.fixture()
def database():
    return playhouse.db_url.connect('sqlite:///:memory:')


@pytest.fixture()
def router(migrations_dir, database):
    from peewee_migrate.cli import get_router
    router = get_router(migrations_dir, database)

    assert router.database is database
    assert isinstance(router.database, pw.Database)

    return router
