# postgresql/pg8000.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors <see AUTHORS
# file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
.. dialect:: postgresql+pg8000
    :name: pg8000
    :dbapi: pg8000
    :connectstring: \
postgresql+pg8000://user:password@host:port/dbname[?key=value&key=value...]
    :url: https://pythonhosted.org/pg8000/

Unicode
-------

When communicating with the server, pg8000 **always uses the server-side
character set**.  SQLAlchemy has no ability to modify what character set
pg8000 chooses to use, and additionally SQLAlchemy does no unicode conversion
of any kind with the pg8000 backend. The origin of the client encoding setting
is ultimately the CLIENT_ENCODING setting in postgresql.conf.

It is not necessary, though is also harmless, to pass the "encoding" parameter
to :func:`.create_engine` when using pg8000.


.. _pg8000_isolation_level:

pg8000 Transaction Isolation Level
-------------------------------------

The pg8000 dialect offers the same isolation level settings as that
of the :ref:`psycopg2 <psycopg2_isolation_level>` dialect:

* ``READ COMMITTED``
* ``READ UNCOMMITTED``
* ``REPEATABLE READ``
* ``SERIALIZABLE``
* ``AUTOCOMMIT``

.. versionadded:: 0.9.5 support for AUTOCOMMIT isolation level when using
   pg8000.

.. seealso::

    :ref:`postgresql_isolation_level`

    :ref:`psycopg2_isolation_level`


"""
from ... import util, exc
import decimal
from ... import processors
from ... import types as sqltypes
from .base import (
    PGDialect, PGCompiler, PGIdentifierPreparer, PGExecutionContext,
    _DECIMAL_TYPES, _FLOAT_TYPES, _INT_TYPES)


class _PGNumeric(sqltypes.Numeric):
    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            if coltype in _FLOAT_TYPES:
                return processors.to_decimal_processor_factory(
                    decimal.Decimal, self._effective_decimal_return_scale)
            elif coltype in _DECIMAL_TYPES or coltype in _INT_TYPES:
                # pg8000 returns Decimal natively for 1700
                return None
            else:
                raise exc.InvalidRequestError(
                    "Unknown PG numeric type: %d" % coltype)
        else:
            if coltype in _FLOAT_TYPES:
                # pg8000 returns float natively for 701
                return None
            elif coltype in _DECIMAL_TYPES or coltype in _INT_TYPES:
                return processors.to_float
            else:
                raise exc.InvalidRequestError(
                    "Unknown PG numeric type: %d" % coltype)


class _PGNumericNoBind(_PGNumeric):
    def bind_processor(self, dialect):
        return None


class PGExecutionContext_pg8000(PGExecutionContext):
    pass


class PGCompiler_pg8000(PGCompiler):
    def visit_mod_binary(self, binary, operator, **kw):
        return self.process(binary.left, **kw) + " %% " + \
            self.process(binary.right, **kw)

    def post_process_text(self, text):
        if '%%' in text:
            util.warn("The SQLAlchemy postgresql dialect "
                      "now automatically escapes '%' in text() "
                      "expressions to '%%'.")
        return text.replace('%', '%%')


class PGIdentifierPreparer_pg8000(PGIdentifierPreparer):
    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value.replace('%', '%%')


class PGDialect_pg8000(PGDialect):
    driver = 'pg8000'

    supports_unicode_statements = True

    supports_unicode_binds = True

    default_paramstyle = 'format'
    supports_sane_multi_rowcount = True
    execution_ctx_cls = PGExecutionContext_pg8000
    statement_compiler = PGCompiler_pg8000
    preparer = PGIdentifierPreparer_pg8000
    description_encoding = 'use_encoding'

    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.Numeric: _PGNumericNoBind,
            sqltypes.Float: _PGNumeric
        }
    )

    def initialize(self, connection):
        if self.dbapi and hasattr(self.dbapi, '__version__'):
            self._dbapi_version = tuple([
                int(x) for x in
                self.dbapi.__version__.split(".")])
        else:
            self._dbapi_version = (99, 99, 99)
        self.supports_sane_multi_rowcount = self._dbapi_version >= (1, 9, 14)
        super(PGDialect_pg8000, self).initialize(connection)

    @classmethod
    def dbapi(cls):
        return __import__('pg8000')

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if 'port' in opts:
            opts['port'] = int(opts['port'])
        opts.update(url.query)
        return ([], opts)

    def is_disconnect(self, e, connection, cursor):
        return "connection is closed" in str(e)

    def set_isolation_level(self, connection, level):
        level = level.replace('_', ' ')

        # adjust for ConnectionFairy possibly being present
        if hasattr(connection, 'connection'):
            connection = connection.connection

        if level == 'AUTOCOMMIT':
            connection.autocommit = True
        elif level in self._isolation_lookup:
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute(
                "SET SESSION CHARACTERISTICS AS TRANSACTION "
                "ISOLATION LEVEL %s" % level)
            cursor.execute("COMMIT")
            cursor.close()
        else:
            raise exc.ArgumentError(
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s or AUTOCOMMIT" %
                (level, self.name, ", ".join(self._isolation_lookup))
            )

    def do_begin_twophase(self, connection, xid):
        print("begin twophase", xid)
        connection.connection.tpc_begin((0, xid, ''))

    def do_prepare_twophase(self, connection, xid):
        print("prepare twophase", xid)
        connection.connection.tpc_prepare()

    def do_rollback_twophase(
            self, connection, xid, is_prepared=True, recover=False):
        connection.connection.tpc_rollback((0, xid, ''))

    def do_commit_twophase(
            self, connection, xid, is_prepared=True, recover=False):
        connection.connection.tpc_commit((0, xid, ''))

    def do_recover_twophase(self, connection):
        return [row[1] for row in connection.connection.tpc_recover()]

dialect = PGDialect_pg8000
