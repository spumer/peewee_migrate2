import logging
import os
import pathlib

import playhouse.db_url
import pytest
import peewee as pw


class PeeweeLogUnwrapper(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        if 'extra' not in kwargs:
            kwargs['extra'] = {}

        if isinstance(msg, tuple):
            msg, params = msg
            kwargs['extra']['params'] = params

        return msg, kwargs


pw.logger = PeeweeLogUnwrapper(pw.logger, {})


@pytest.fixture()
def assert_log_has_records(caplog):
    caplog.set_level(logging.DEBUG, logger='peewee')

    def _inner(*messages, level=logging.DEBUG):
        records = [r for r in caplog.records if r.levelno >= level]
        recorded_messages = [r.message for r in records]
        for msg in messages:
            assert msg in recorded_messages

    return _inner


@pytest.fixture()
def migrations_dir():
    """Migrations dir"""
    return pathlib.Path(__file__).with_name('migrations')


@pytest.fixture(params=['sqlite', 'postgresql'])
def database(request):
    if request.param == 'sqlite':
        db = playhouse.db_url.connect('sqlite:///:memory:')
    else:
        dsn = os.getenv('POSTGRES_DSN')
        if not dsn:
            raise pytest.skip('Postgres not found')
        db = playhouse.db_url.connect(dsn)

    with db.atomic():
        yield db
        db.rollback()


@pytest.fixture()
def router(migrations_dir, database):
    from peewee_migrate.cli import get_router
    router = get_router(migrations_dir, database)

    assert router.database is database
    assert isinstance(router.database, pw.Database)

    return router
