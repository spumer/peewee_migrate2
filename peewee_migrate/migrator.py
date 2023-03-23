import collections
import typing
from typing import List, Type, cast

import peewee as pw
from functools import wraps
from playhouse.migrate import (
    MySQLMigrator as MqM,
    PostgresqlMigrator as PgM,
    SchemaMigrator as ScM,
    SqliteMigrator as SqM,
    Operation, SQL, PostgresqlDatabase, operation, SqliteDatabase, MySQLDatabase,
    make_index_name
)

from peewee_migrate import LOGGER


class SchemaMigrator(ScM):

    """Implement migrations."""

    @classmethod
    def from_database(cls, database):
        """Initialize migrator by db."""
        if isinstance(database, PostgresqlDatabase):
            return PostgresqlMigrator(database)
        if isinstance(database, SqliteDatabase):
            return SqliteMigrator(database)
        if isinstance(database, MySQLDatabase):
            return MySQLMigrator(database)
        return super(SchemaMigrator, cls).from_database(database)

    @operation
    def select_schema(self, schema):
        """Select database schema"""
        raise NotImplementedError()

    def drop_table(self, model, cascade=True):
        return lambda: model.drop_table(cascade=cascade)

    @operation
    def change_column(self, table, column_name, field):
        """Change column."""
        operations = [self.alter_change_column(table, column_name, field)]
        if not field.null:
            operations.extend([self.add_not_null(table, column_name)])
        return operations

    def alter_change_column(self, table, column, field):
        """Support change columns."""
        ctx = self.make_context()
        field_null, field.null = field.null, True
        ctx = self._alter_table(ctx, table).literal(' ALTER COLUMN ').sql(field.ddl(ctx))
        field.null = field_null
        return ctx

    @operation
    def sql(self, sql, *params):
        """Execute raw SQL."""
        return SQL(sql, *params)

    def alter_add_column(self, table, column_name, field, **kwargs):
        """Fix fieldname for ForeignKeys."""
        name = field.name
        op = super(SchemaMigrator, self).alter_add_column(table, column_name, field, **kwargs)
        if isinstance(field, pw.ForeignKeyField):
            field.name = name
        return op


class MySQLMigrator(SchemaMigrator, MqM):

    def alter_change_column(self, table, column, field):
        """Support change columns."""
        ctx = self.make_context()
        field_null, field.null = field.null, True
        ctx = self._alter_table(ctx, table).literal(' MODIFY COLUMN ').sql(field.ddl(ctx))
        field.null = field_null
        return ctx


class PostgresqlMigrator(SchemaMigrator, PgM):

    """Support the migrations in postgresql."""

    @operation
    def select_schema(self, schema):
        """Select database schema"""
        return self.set_search_path(schema)

    def alter_change_column(self, table, column_name, field):
        """Support change columns."""
        context = super(PostgresqlMigrator, self).alter_change_column(table, column_name, field)
        context._sql.insert(-1, 'TYPE')
        context._sql.insert(-1, ' ')
        return context


class SqliteMigrator(SchemaMigrator, SqM):

    """Support the migrations in sqlite."""

    def drop_table(self, model, cascade=True):
        """SQLite doesnt support cascade syntax by default."""
        return lambda: model.drop_table(cascade=False)

    def alter_change_column(self, table, column, field):
        """Support change columns."""
        return self._update_column(table, column, lambda a, b: b)


def get_model(method):
    """Convert string to model class."""

    @wraps(method)
    def wrapper(migrator, model, *args, **kwargs):
        if isinstance(model, str):
            model = migrator.orm[model]

        model._meta.schema = migrator.schema
        return method(migrator, model, *args, **kwargs)
    return wrapper



class MigrateOperation:
    def state_forwards(self, migrator: 'Migrator'):
        """
        Take the state from the previous migration, and mutate it
        so that it matches what this migration would perform.
        """

        raise NotImplementedError

    def database_forwards(self, schema_migrator: 'SchemaMigrator', from_state, to_state):
        """
        Perform the mutation on the database schema in the normal
        (forwards) direction.
        """
        raise NotImplementedError


class PlayhouseOperation(MigrateOperation):
    def __init__(self, op: Operation):
        self.op = op

    def database_forwards(self, migrator, from_state, to_state):
        assert self.op.migrator == migrator
        self.op.run()


class AddIndex(MigrateOperation):
    def __init__(self, model_index: pw.ModelIndex):
        self.index = model_index

    @staticmethod
    def _resolve_columns(meta: pw.Metadata, field_or_columns: List[str]):
        return [
            meta.combined[v].column_name
            for v in field_or_columns
        ]

    @classmethod
    def from_model(cls, model: Type[pw.Model], fields: List[str], options: dict):
        meta: pw.Metadata = model._meta
        columns = cls._resolve_columns(meta, fields)
        # migrator.add_index call make_index_name
        # ensure our meta will have same name too
        index = model.index(*columns, name=make_index_name(meta.table_name, columns), **options)
        return cls(index)

    def database_forwards(self, schema_migrator: 'SchemaMigrator', from_state, to_state):
        schema_migrator.add_index(model._meta.table_name, columns_, unique=unique)


class CreateTable(MigrateOperation):
    def __init__(self, model: pw.Model):
        self.model = model

    def state_forwards(self, migrator: 'Migrator'):
        migrator.orm[self.model._meta.table_name] = self.model
        self.model._meta.database = migrator.database  # without it we can't run `model.create_table`

    def database_forwards(self, schema_editor, from_state, to_state):
        self.model.create_table()


class Migration:
    def __init__(self, schema_migrator, schema=None):
        self.ops: typing.List[MigrateOperation] = []
        self.schema_migrator = schema_migrator

        if schema:
            self.append(schema_migrator.select_schema(schema))

    def append(self, op):
        if isinstance(op, MigrateOperation):
            self.ops.append(op)
        elif isinstance(op, Operation):
            self.ops.append(PlayhouseOperation(op))
        else:
            raise NotImplementedError(type(op))

    def apply(self):
        for op in self.ops:
            op.state_forwards()
            op.database_forwards(self.schema_migrator, None, None)


class Migrator(object):

    """Provide migrations."""

    def __init__(self, database, schema=None):
        """Initialize the migrator."""
        if isinstance(database, pw.Proxy):
            database = database.obj

        migrator = SchemaMigrator.from_database(database)

        self.database = database
        self.schema = schema
        self.orm = dict()
        self.migration = Migration(migrator, schema)
        self.migrator = migrator

    @property
    def ops(self):
        return self.migration  # backward compatibility

    def run(self):
        """Run operations."""
        self.ops.apply()
        self.clean()

    def python(self, func, *args, **kwargs):
        """Run python code."""
        self.ops.append(lambda: func(*args, **kwargs))

    def sql(self, sql, *params):
        """Execure raw SQL."""
        self.ops.append(self.migrator.sql(sql, *params))

    def clean(self):
        """Clean the operations."""
        raise NotImplementedError  # FIXME: what actually we want to do here?

    def create_table(self, model):
        """Create model and table in database.

        >> migrator.create_table(model)
        """
        self.migration.append(CreateTable(model))
        return model

    create_model = create_table

    @get_model
    def drop_table(self, model, cascade=True):
        """Drop model and table from database.

        >> migrator.drop_table(model, cascade=True)
        """
        del self.orm[model._meta.table_name]
        self.ops.append(self.migrator.drop_table(model, cascade))

    remove_model = drop_table

    @get_model
    def add_columns(self, model, **fields):
        """Create new fields."""
        for name, field in fields.items():
            model._meta.add_field(name, field)
            self.ops.append(self.migrator.add_column(
                model._meta.table_name, field.column_name, field))
            if field.unique:
                self.ops.append(self.migrator.add_index(
                    model._meta.table_name, (field.column_name,), unique=True))
        return model

    add_fields = add_columns

    @get_model
    def change_columns(self, model, **fields):
        """Change fields."""
        for name, field in fields.items():
            old_field = model._meta.fields.get(name, field)
            old_column_name = old_field and old_field.column_name

            model._meta.add_field(name, field)

            if isinstance(old_field, pw.ForeignKeyField):
                self.ops.append(self.migrator.drop_foreign_key_constraint(
                    model._meta.table_name, old_column_name))

            if old_column_name != field.column_name:
                self.ops.append(
                    self.migrator.rename_column(
                        model._meta.table_name, old_column_name, field.column_name))

            if isinstance(field, pw.ForeignKeyField):
                on_delete = field.on_delete if field.on_delete else 'RESTRICT'
                on_update = field.on_update if field.on_update else 'RESTRICT'
                self.ops.append(self.migrator.add_foreign_key_constraint(
                    model._meta.table_name, field.column_name,
                    field.rel_model._meta.table_name, field.rel_field.name,
                    on_delete, on_update))
                continue

            self.ops.append(self.migrator.change_column(
                model._meta.table_name, field.column_name, field))

            if field.unique == old_field.unique:
                continue

            if field.unique:
                assert not field.index, "You can't set unique and index together"  # FIXME: Why?
                self.add_index(model, field.name, unique=field.unique)
            else:
                self.drop_index(model, field.name)

        return model

    change_fields = change_columns

    @get_model
    def drop_columns(self, model, *names, **kwargs):
        """Remove fields from model."""
        fields = [field for field in model._meta.fields.values() if field.name in names]
        cascade = kwargs.pop('cascade', True)
        for field in fields:
            self.__del_field__(model, field)
            if field.unique:
                index_name = make_index_name(model._meta.table_name, [field.column_name])
                self.ops.append(self.migrator.drop_index(model._meta.table_name, index_name))
            self.ops.append(
                self.migrator.drop_column(
                    model._meta.table_name, field.column_name, cascade=cascade))
        return model

    remove_fields = drop_columns

    def __del_field__(self, model, field):
        """Delete field from model."""
        model._meta.remove_field(field.name)
        delattr(model, field.name)
        if isinstance(field, pw.ForeignKeyField):
            obj_id_name = field.column_name
            if field.column_name == field.name:
                obj_id_name += '_id'
            delattr(model, obj_id_name)
            delattr(field.rel_model, field.backref)

    @get_model
    def rename_column(self, model, old_name, new_name):
        """Rename field in model."""
        field = model._meta.fields[old_name]
        if isinstance(field, pw.ForeignKeyField):
            old_name = field.column_name
        self.__del_field__(model, field)
        field.name = field.column_name = new_name
        model._meta.add_field(new_name, field)
        if isinstance(field, pw.ForeignKeyField):
            field.column_name = new_name = field.column_name + '_id'
        self.ops.append(self.migrator.rename_column(model._meta.table_name, old_name, new_name))
        return model

    rename_field = rename_column

    @get_model
    def rename_table(self, model, new_name):
        """Rename table in database."""
        old_name = model._meta.table_name
        del self.orm[model._meta.table_name]
        model._meta.table_name = new_name
        self.orm[model._meta.table_name] = model
        self.ops.append(self.migrator.rename_table(old_name, new_name))
        return model

    @get_model
    def add_index(self, model, *fields: str, **kwargs):
        """Create indexes."""
        op = AddIndex.from_model(
            model, cast(List, fields),
            options=kwargs,
        )

        self.ops.append(op)
        return model

    @get_model
    def drop_index(self, model, *fields):
        """Drop indexes."""
        def _to_columns(field_or_columns):
            return [
                model._meta.combined[v].column_name
                for v in field_or_columns
            ]

        columns_ = _to_columns(fields)
        index_to_drop = model.index(*columns_, name=make_index_name(model._meta.table_name, columns_))

        self.ops.append(self.migrator.drop_index(model._meta.table_name, index_to_drop._name))

        meta_index_pos = None
        for position, index in enumerate(model._meta.indexes):
            if not isinstance(index, pw.ModelIndex):
                assert isinstance(index, (list, tuple))
                index_parts, unique = index
                index = model.index(*_to_columns(index_parts), unique=unique)

            if index._name == index_to_drop._name:
                meta_index_pos = position
                break

        if meta_index_pos is None:
            raise NotImplementedError('Index not found in Meta')
        else:
            model._meta.indexes = list(model._meta.indexes)
            model._meta.indexes.pop(meta_index_pos)

        return model

    @get_model
    def add_not_null(self, model, *names):
        """Add not null."""
        for name in names:
            field = model._meta.fields[name]
            field.null = False
            self.ops.append(self.migrator.add_not_null(model._meta.table_name, field.column_name))
        return model

    @get_model
    def drop_not_null(self, model, *names):
        """Drop not null."""
        for name in names:
            field = model._meta.fields[name]
            field.null = True
            self.ops.append(self.migrator.drop_not_null(model._meta.table_name, field.column_name))
        return model

    @get_model
    def add_default(self, model, name, default):
        """Add default."""
        field = model._meta.fields[name]
        model._meta.defaults[field] = field.default = default
        self.ops.append(self.migrator.apply_default(model._meta.table_name, name, field))
        return model

#  pylama:ignore=W0223,W0212,R
