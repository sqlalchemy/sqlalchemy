# postgresql/pg8000.py
# Copyright (C) 2005-2019 the SQLAlchemy authors and contributors <see AUTHORS
# file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
r"""
.. dialect:: postgresql+pg8000
    :name: pg8000
    :dbapi: pg8000
    :connectstring: postgresql+pg8000://user:password@host:port/dbname[?key=value&key=value...]
    :url: https://pythonhosted.org/pg8000/


.. _pg8000_unicode:

Unicode
-------

pg8000 will encode / decode string values between it and the server using the
PostgreSQL ``client_encoding`` parameter; by default this is the value in
the ``postgresql.conf`` file, which often defaults to ``SQL_ASCII``.
Typically, this can be changed to ``utf-8``, as a more useful default::

    #client_encoding = sql_ascii # actually, defaults to database
                                 # encoding
    client_encoding = utf8

The ``client_encoding`` can be overridden for a session by executing the SQL:

SET CLIENT_ENCODING TO 'utf8';

SQLAlchemy will execute this SQL on all new connections based on the value
passed to :func:`.create_engine` using the ``client_encoding`` parameter::

    engine = create_engine(
        "postgresql+pg8000://user:pass@host/dbname", client_encoding='utf8')


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


"""  # noqa
import decimal
import re

from .base import _DECIMAL_TYPES
from .base import _FLOAT_TYPES
from .base import _INT_TYPES
from .base import PGCompiler
from .base import PGDialect
from .base import PGExecutionContext
from .base import PGIdentifierPreparer
from .base import UUID
from .json import JSON
from ... import exc
from ... import processors
from ... import types as sqltypes
from ... import util
from ...sql.elements import quoted_name


try:
    from uuid import UUID as _python_UUID  # noqa
except ImportError:
    _python_UUID = None


class _PGNumeric(sqltypes.Numeric):
    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            if coltype in _FLOAT_TYPES:
                return processors.to_decimal_processor_factory(
                    decimal.Decimal, self._effective_decimal_return_scale
                )
            elif coltype in _DECIMAL_TYPES or coltype in _INT_TYPES:
                # pg8000 returns Decimal natively for 1700
                return None
            else:
                raise exc.InvalidRequestError(
                    "Unknown PG numeric type: %d" % coltype
                )
        else:
            if coltype in _FLOAT_TYPES:
                # pg8000 returns float natively for 701
                return None
            elif coltype in _DECIMAL_TYPES or coltype in _INT_TYPES:
                return processors.to_float
            else:
                raise exc.InvalidRequestError(
                    "Unknown PG numeric type: %d" % coltype
                )


class _PGNumericNoBind(_PGNumeric):
    def bind_processor(self, dialect):
        return None


class _PGJSON(JSON):
    def result_processor(self, dialect, coltype):
        if dialect._dbapi_version > (1, 10, 1):
            return None  # Has native JSON
        else:
            return super(_PGJSON, self).result_processor(dialect, coltype)


class _PGUUID(UUID):
    def bind_processor(self, dialect):
        if not self.as_uuid:

            def process(value):
                if value is not None:
                    value = _python_UUID(value)
                return value

            return process

    def result_processor(self, dialect, coltype):
        if not self.as_uuid:

            def process(value):
                if value is not None:
                    value = str(value)
                return value

            return process


class PGExecutionContext_pg8000(PGExecutionContext):
    pass


class PGCompiler_pg8000(PGCompiler):
    def visit_mod_binary(self, binary, operator, **kw):
        return (
            self.process(binary.left, **kw)
            + " %% "
            + self.process(binary.right, **kw)
        )

    def post_process_text(self, text):
        if "%%" in text:
            util.warn(
                "The SQLAlchemy postgresql dialect "
                "now automatically escapes '%' in text() "
                "expressions to '%%'."
            )
        return text.replace("%", "%%")


class PGIdentifierPreparer_pg8000(PGIdentifierPreparer):
    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value.replace("%", "%%")


class PGDialect_pg8000(PGDialect):
    driver = "pg8000"

    supports_unicode_statements = True

    supports_unicode_binds = True

    default_paramstyle = "format"
    supports_sane_multi_rowcount = True
    execution_ctx_cls = PGExecutionContext_pg8000
    statement_compiler = PGCompiler_pg8000
    preparer = PGIdentifierPreparer_pg8000
    description_encoding = "use_encoding"

    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.Numeric: _PGNumericNoBind,
            sqltypes.Float: _PGNumeric,
            JSON: _PGJSON,
            sqltypes.JSON: _PGJSON,
            UUID: _PGUUID,
        },
    )

    def __init__(self, client_encoding=None, **kwargs):
        PGDialect.__init__(self, **kwargs)
        self.client_encoding = client_encoding

    def initialize(self, connection):
        self.supports_sane_multi_rowcount = self._dbapi_version >= (1, 9, 14)
        super(PGDialect_pg8000, self).initialize(connection)

    @util.memoized_property
    def _dbapi_version(self):
        if self.dbapi and hasattr(self.dbapi, "__version__"):
            return tuple(
                [
                    int(x)
                    for x in re.findall(
                        r"(\d+)(?:[-\.]?|$)", self.dbapi.__version__
                    )
                ]
            )
        else:
            return (99, 99, 99)

    @classmethod
    def dbapi(cls):
        return __import__("pg8000")

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username="user")
        if "port" in opts:
            opts["port"] = int(opts["port"])
        opts.update(url.query)
        return ([], opts)

    def is_disconnect(self, e, connection, cursor):
        return "connection is closed" in str(e)

    def set_isolation_level(self, connection, level):
        level = level.replace("_", " ")

        # adjust for ConnectionFairy possibly being present
        if hasattr(connection, "connection"):
            connection = connection.connection

        if level == "AUTOCOMMIT":
            connection.autocommit = True
        elif level in self._isolation_lookup:
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute(
                "SET SESSION CHARACTERISTICS AS TRANSACTION "
                "ISOLATION LEVEL %s" % level
            )
            cursor.execute("COMMIT")
            cursor.close()
        else:
            raise exc.ArgumentError(
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s or AUTOCOMMIT"
                % (level, self.name, ", ".join(self._isolation_lookup))
            )

    def set_client_encoding(self, connection, client_encoding):
        # adjust for ConnectionFairy possibly being present
        if hasattr(connection, "connection"):
            connection = connection.connection

        cursor = connection.cursor()
        cursor.execute("SET CLIENT_ENCODING TO '" + client_encoding + "'")
        cursor.execute("COMMIT")
        cursor.close()

    def do_begin_twophase(self, connection, xid):
        connection.connection.tpc_begin((0, xid, ""))

    def do_prepare_twophase(self, connection, xid):
        connection.connection.tpc_prepare()

    def do_rollback_twophase(
        self, connection, xid, is_prepared=True, recover=False
    ):
        connection.connection.tpc_rollback((0, xid, ""))

    def do_commit_twophase(
        self, connection, xid, is_prepared=True, recover=False
    ):
        connection.connection.tpc_commit((0, xid, ""))

    def do_recover_twophase(self, connection):
        return [row[1] for row in connection.connection.tpc_recover()]

    def on_connect(self):
        fns = []

        def on_connect(conn):
            conn.py_types[quoted_name] = conn.py_types[util.text_type]

        fns.append(on_connect)

        if self.client_encoding is not None:

            def on_connect(conn):
                self.set_client_encoding(conn, self.client_encoding)

            fns.append(on_connect)

        if self.isolation_level is not None:

            def on_connect(conn):
                self.set_isolation_level(conn, self.isolation_level)

            fns.append(on_connect)

        if len(fns) > 0:

            def on_connect(conn):
                for fn in fns:
                    fn(conn)

            return on_connect
        else:
            return None


dialect = PGDialect_pg8000
