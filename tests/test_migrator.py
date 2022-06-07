import logging
import typing
from unittest.mock import ANY

import peewee as pw
from typing import NamedTuple
from typing import Type

from unittest import mock

import pytest

from peewee_migrate import Migrator


@pytest.fixture()
def _mock_connection():
    """Monkey patch psycopg2 connect"""
    import psycopg2
    from .mocks import postgres

    with mock.patch.object(psycopg2, 'connect', postgres.MockConnection):
        yield


def model_has_index(model: Type[pw.Model], *, name, fields=ANY):
    meta: pw.Metadata = model._meta

    repr_indexes = [{
        'name': idx._name,
        'fields': list(idx._expressions),
    } for idx in meta.fields_to_index()]

    if fields is not ANY:
        # repr_indexes['fields'] is tuple
        fields = list(fields)

    return {
        'name': name,
        'fields': fields,
    } in repr_indexes


@pytest.fixture()
def sqlite_migrator():
    from playhouse.db_url import connect

    database = connect('sqlite:///:memory:')
    return Migrator(database)


@pytest.fixture()
def migrator(sqlite_migrator):
    migrator = sqlite_migrator

    @migrator.create_table
    class Customer(pw.Model):
        name = pw.CharField()

    @migrator.create_table
    class Order(pw.Model):
        number = pw.CharField()
        uid = pw.CharField(unique=True)
        customer_id = pw.ForeignKeyField(Customer, column_name='customer_id')

    migrator.run()

    yield migrator

    assert not migrator.ops, "migrator.run() or migrator.clean() call missing"


@pytest.mark.parametrize('alias', [False, True])
def test_add_columns(migrator, alias):
    """add_columns reflect changes in model meta"""
    Order = migrator.orm['order']
    if not alias:
        migrator.add_columns(Order, finished=pw.BooleanField(default=False))
    else:
        migrator.add_fields(Order, finished=pw.BooleanField(default=False))
    assert 'finished' in Order._meta.fields
    migrator.run()


@pytest.mark.parametrize('alias', [False, True])
def test_drop_columns(migrator, alias):
    """
    drop_columns change both: model meta and instance
    and ignore unknown fields
    """

    Order = migrator.orm['order']
    customer_id_object_id_name = Order.customer_id.object_id_name

    assert 'uid' in Order._meta.fields  # common field
    assert hasattr(Order, customer_id_object_id_name)  # ForeignKey

    migrator.drop_columns(Order, 'uid', 'customer_id')

    assert 'uid' not in Order._meta.fields
    assert not hasattr(Order, 'customer_id')
    assert not hasattr(Order, customer_id_object_id_name)

    migrator.run()


def test_add_nullable_foreign_key(migrator):
    Order = migrator.orm['order']
    Customer = migrator.orm['customer']

    migrator.add_columns(Order, nullable_customer=pw.ForeignKeyField(Customer, null=True))
    assert 'nullable_customer' in Order._meta.fields
    assert Order.nullable_customer.name == 'nullable_customer'
    assert hasattr(Order, 'nullable_customer_id')  # check object_id_name

    migrator.run()


@pytest.mark.parametrize('alias', [False, True])
def test_rename_column(migrator, alias):
    Order = migrator.orm['order']

    if not alias:
        migrator.rename_column(Order, 'number', 'identifier')
    else:
        migrator.rename_field(Order, 'number', 'identifier')
    assert 'identifier' in Order._meta.fields

    migrator.run()

def test_rename_columne(migrator):
    Order = migrator.orm['order']

    migrator.rename_table("order", "new_name")
    migrator.run()
    assert Order._meta.table_name == "new_name"
    migrator.rename_table("new_name", "order")
    migrator.run()


def test_drop_not_null(migrator):
    Order = migrator.orm['order']

    assert not Order._meta.fields['number'].null
    migrator.drop_not_null(Order, 'number')
    assert Order._meta.fields['number'].null
    assert Order._meta.columns['number'].null

    migrator.run()


def test_add_default(migrator):
    Order = migrator.orm['order']

    migrator.add_default(Order, 'number', '11')
    assert Order._meta.fields['number'].default == '11'

    migrator.run()


@pytest.mark.parametrize('alias', [False, True])
def test_change_columns_change_field_type(migrator, alias):
    Order = migrator.orm['order']

    assert Order.number.field_type != 'INT'
    if not alias:
        migrator.change_columns(Order, number=pw.IntegerField(default=0))
    else:
        migrator.change_fields(Order, number=pw.IntegerField(default=0))
    assert Order.number.field_type == 'INT'
    assert Order._meta.fields['number'].default == 0

    migrator.run()


def test_raw_sql_migration(migrator):
    Customer = migrator.orm['customer']

    Customer.create(name='test')
    migrator.sql("UPDATE customer SET name = 'not-test';")
    migrator.run()

    order = Customer.get()
    assert order.name == 'not-test'



"""
    @migrator.create_table
    class Test(pw.Model):
        indexed_not_unique = pw.CharField(index=True)
"""


def test_add_and_remove_index(migrator, assert_log_has_records):
    @migrator.create_table
    class Test(pw.Model):
        one = pw.CharField(column_name='not_one')
        two = pw.IntegerField()

        three = pw.CharField()
        four = pw.CharField()

        class Meta:
            indexes = [
                # common index
                (('three', 'four'), False),
                # index with custom column name
                (('one', 'three'), False),
            ]

    migrator.run()  # FIXME: LazyOperation: add_index reflect meta and create_table create indexes from Meta, then add_index fails

    # by default indexes created safely with 'IF NOT EXISTS'
    assert_log_has_records(
        'CREATE INDEX IF NOT EXISTS "test_three_four" ON "test" ("three", "four")',
        'CREATE INDEX IF NOT EXISTS "test_not_one_three" ON "test" ("not_one", "three")',
    )

    assert model_has_index(Test, name='test_three_four', fields=[Test.three, Test.four])
    assert model_has_index(Test, name='test_not_one_three', fields=[Test.one, Test.three])

    # manually add common and composite indexes ...
    migrator.add_index(Test, 'one')
    migrator.add_index(Test, 'one', 'two')
    migrator.run()

    assert_log_has_records(
        'CREATE INDEX "test_not_one" ON "test" ("not_one")',
        'CREATE INDEX "test_not_one_two" ON "test" ("not_one", "two")',
    )

    assert model_has_index(Test, name='test_not_one', fields=[Test.one])
    assert model_has_index(Test, name='test_not_one_two', fields=[Test.one, Test.two])

    # ... drop manually added common and composite indexes
    migrator.drop_index(Test, 'one')
    migrator.drop_index(Test, 'one', 'two')
    migrator.run()

    assert_log_has_records(
        'DROP INDEX "test_not_one"',
        'DROP INDEX "test_not_one_two"',
    )

    assert not model_has_index(Test, name='test_not_one', fields=[Test.one])
    assert not model_has_index(Test, name='test_not_one_two', fields=[Test.one, Test.two])

    # ensure pre-defined indexes can be dropped too
    migrator.drop_index(Test, 'three', 'four')
    migrator.drop_index(Test, 'one', 'three')
    migrator.run()
    assert_log_has_records(
        'DROP INDEX "test_three_four"',
        'DROP INDEX "test_not_one_three"',
    )

    assert not model_has_index(Test, name='test_three_four', fields=[Test.three, Test.four])
    assert not model_has_index(Test, name='test_not_one_three', fields=[Test.one, Test.three])


def test_add_index_unique_single(migrator):
    Order = migrator.orm['order']

    migrator.add_index(Order, 'number', unique=True)
    migrator.run()

    assert not Order.number.index
    assert Order.number.unique  # FIXME: why we make field instance unique?
    assert Order._meta.indexes == [(('number',), True)]


def test_add_index_unique_multiple(migrator):
    Order = migrator.orm['order']

    migrator.add_fields(Order, new_number=pw.CharField(null=True))
    migrator.add_index(Order, 'number', 'new_number', unique=True)
    migrator.run()

    assert not Order.number.index
    assert not Order.new_number.index
    assert not Order.new_number.unique
    assert not Order.new_number.unique
    assert Order._meta.indexes == [(('number', 'new_number'), True)]


def test_change_columns_drop_field_defined_unique_index(migrator):
    """Unique index can be defined only on field level when metadata was empty

    Check we can correctly drop this index when change column
    """
    Order = migrator.orm['order']

    # edge-case: Metadata unfilled in original model
    assert not Order._meta.indexes
    assert Order.uid.unique
    migrator.change_columns(Order, uid=pw.IntegerField(default=0))
    migrator.run()
    assert not Order._meta.indexes


def test_migrator_postgres(_mock_connection):
    """
    Ensure change_fields generates queries and
    does not cause exception
    """
    import peewee as pw
    from playhouse.db_url import connect
    from peewee_migrate import Migrator

    database = connect('postgres:///fake')

    migrator = Migrator(database)
    @migrator.create_table
    class User(pw.Model):
        name = pw.CharField()
        created_at = pw.DateField()

    assert User == migrator.orm['user']

    # Date -> DateTime
    migrator.change_fields('user', created_at=pw.DateTimeField())
    migrator.run()
    assert 'ALTER TABLE "user" ALTER COLUMN "created_at" TYPE TIMESTAMP' in database.cursor().queries
    
    # Char -> Text
    migrator.change_fields('user', name=pw.TextField())
    migrator.run()
    assert 'ALTER TABLE "user" ALTER COLUMN "name" TYPE TEXT' in database.cursor().queries


def test_migrator_schema(_mock_connection):
    import peewee as pw
    from playhouse.db_url import connect
    from peewee_migrate import Migrator

    database = connect('postgres:///fake')
    schema_name = 'test_schema'
    migrator = Migrator(database, schema=schema_name)

    def has_schema_select_query():
        return database.cursor().queries[0] == 'SET search_path TO {}'.format(schema_name)

    @migrator.create_table
    class User(pw.Model):
        name = pw.CharField()
        created_at = pw.DateField()

    migrator.run()
    assert has_schema_select_query()

    migrator.change_fields('user', created_at=pw.DateTimeField())
    migrator.run()
    assert has_schema_select_query()

