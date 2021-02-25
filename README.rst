Peewee Migrate 2
################

.. _description:

Peewee Migrate 2 -- A simple migration engine for Peewee


.. _badges:

.. image:: https://travis-ci.org/spumer/peewee_migrate2.svg
    :target: http://travis-ci.org/spumer/peewee_migrate2
    :alt: Build Status


.. image:: https://coveralls.io/repos/github/spumer/peewee_migrate2/badge.svg
    :target: https://coveralls.io/github/spumer/peewee_migrate2
    :alt: Coverals

.. image:: http://img.shields.io/pypi/v/peewee_migrate2.svg?style=flat-square
    :target: https://pypi.python.org/pypi/peewee_migrate2
    :alt: Version

.. _contents:

.. contents::

.. _requirements:


Why Fork?
=========

It's a fork of original https://github.com/klen/peewee_migrate. Thank ``klen`` for that!

But ``klen`` don't support project for a long time.

To fix critical issues project was forked and development continued.


Requirements
=============

- python >= 3.6
- peewee >= 3.3.1

Dependency Note
---------------

For ``Peewee<3.0`` please use ``Peewee-Migrate==0.14.0``.

.. _installation:

Installation
=============

To reduce code changes Python package name don't changed. Only name on PyPI.

If you have installed previous version please remove it before using pip: ::

    pip uninstall peewee_migrate

**Peewee Migrate** should be installed using pip: ::

    pip install peewee_migrate2

.. _usage:

Usage
=====

Do you want Flask_ integration? Look at Flask-PW_.

From shell
----------

Getting help: ::

    $ pw_migrate --help

    Usage: pw_migrate [OPTIONS] COMMAND [ARGS]...

    Options:
        --help  Show this message and exit.

    Commands:
        create   Create migration.
        migrate  Run migrations.
        rollback Rollback migration.

Create migration: ::

    $ pw_migrate create --help

    Usage: pw_migrate create [OPTIONS] NAME

        Create migration.

    Options:
        --auto                  FLAG  Scan sources and create db migrations automatically. Supports autodiscovery.
        --auto-source           TEXT  Set to python module path for changes autoscan (e.g. 'package.models'). Current directory will be recursively scanned by default.
        --database              TEXT  Database connection
        --directory             TEXT  Directory where migrations are stored
        --schema                TEXT  Database schema
        -v, --verbose
        --help                        Show this message and exit.

Run migrations: ::

    $ pw_migrate migrate --help

    Usage: pw_migrate migrate [OPTIONS]

        Run migrations.

    Options:
        --name                  TEXT  Select migration
        --database              TEXT  Database connection
        --directory             TEXT  Directory where migrations are stored
        --schema                TEXT  Database schema
        -v, --verbose
        --help                        Show this message and exit.

Auto create migration: ::

    $ pw_migrate makemigrations --help

    Usage: pw_migrate makemigrations [OPTIONS]

      Create a migration automatically

      Similar to `create` command, but `auto` is True by default, and `name` not
      required

    Options:
        --name TEXT         Migration file name. By default will be
                          'auto_YYYYmmdd_HHMM'
        --auto              Scan sources and create db migrations automatically.
                          Supports autodiscovery.
        --auto-source TEXT  Set to python module path for changes autoscan (e.g.
                          'package.models'). Current directory will be recursively
                          scanned by default.
        --database TEXT     Database connection
        --directory TEXT    Directory where migrations are stored
        --schema                TEXT  Database schema
        -v, --verbose
        --help              Show this message and exit.

From python
-----------
::

    from peewee_migrate import Router
    from peewee import SqliteDatabase

    router = Router(SqliteDatabase('test.db'))

    # Create migration
    router.create('migration_name')

    # Run migration/migrations
    router.run('migration_name')

    # Run all unapplied migrations
    router.run()

Migration files
---------------

By default, migration files are looked up in ``os.getcwd()/migrations`` directory, but custom directory can be given.

Migration files are sorted and applied in ascending order per their filename.

Each migration file must specify ``migrate()`` function and may specify ``rollback()`` function::

    def migrate(migrator, database, fake=False, **kwargs):
        pass

    def rollback(migrator, database, fake=False, **kwargs):
        pass

.. _bugtracker:

Bug tracker
===========

If you have any suggestions, bug reports or
annoyances please report them to the issue tracker
at https://github.com/spumer/peewee_migrate2/issues

.. _contributing:

Contributing
============

Development of starter happens at github: https://github.com/spumer/peewee_migrate2


Contributors
=============

See `AUTHORS.rst`


.. _license:

License
=======

Licensed under a `BSD license`_.

.. _links:

.. _BSD license: http://www.linfo.org/bsdlicense.html
.. _klen: https://klen.github.io/
.. _Flask: http://flask.pocoo.org/
.. _Flask-PW: https://github.com/klen/flask-pw
