# postgresql/asyncpg.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors <see AUTHORS
# file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
r"""
.. dialect:: postgresql+asyncpg
    :name: asyncpg
    :dbapi: asyncpg
    :connectstring: postgresql+asyncpg://user:password@host:port/dbname[?key=value&key=value...]
    :url: https://magicstack.github.io/asyncpg/

The asyncpg dialect is SQLAlchemy's first Python asyncio dialect.

Using a special asyncio mediation layer, the asyncpg dialect is usable
as the backend for the :ref:`SQLAlchemy asyncio <asyncio_toplevel>`
extension package.

This dialect should normally be used only with the
:func:`_asyncio.create_async_engine` engine creation function::

    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine("postgresql+asyncpg://user:pass@hostname/dbname")

The dialect can also be run as a "synchronous" dialect within the
:func:`_sa.create_engine` function, which will pass "await" calls into
an ad-hoc event loop.  This mode of operation is of **limited use**
and is for special testing scenarios only.  The mode can be enabled by
adding the SQLAlchemy-specific flag ``async_fallback`` to the URL
in conjunction with :func:`_sa.craete_engine`::

    # for testing purposes only; do not use in production!
    engine = create_engine("postgresql+asyncpg://user:pass@hostname/dbname?async_fallback=true")


.. versionadded:: 1.4

.. note::

    By default asyncpg does not decode the ``json`` and ``jsonb`` types and
    returns them as strings. SQLAlchemy sets default type decoder for ``json``
    and ``jsonb`` types using the python builtin ``json.loads`` function.
    The json implementation used can be changed by setting the attribute
    ``json_deserializer`` when creating the engine with
    :func:`create_engine` or :func:`create_async_engine`.

"""  # noqa

import collections
import decimal
import itertools
import json as _py_json
import re

from . import json
from .base import _DECIMAL_TYPES
from .base import _FLOAT_TYPES
from .base import _INT_TYPES
from .base import ENUM
from .base import INTERVAL
from .base import OID
from .base import PGCompiler
from .base import PGDialect
from .base import PGExecutionContext
from .base import PGIdentifierPreparer
from .base import REGCLASS
from .base import UUID
from ... import exc
from ... import pool
from ... import processors
from ... import util
from ...sql import sqltypes
from ...util.concurrency import await_fallback
from ...util.concurrency import await_only


try:
    from uuid import UUID as _python_UUID  # noqa
except ImportError:
    _python_UUID = None


class AsyncpgTime(sqltypes.Time):
    def get_dbapi_type(self, dbapi):
        return dbapi.TIME


class AsyncpgDate(sqltypes.Date):
    def get_dbapi_type(self, dbapi):
        return dbapi.DATE


class AsyncpgDateTime(sqltypes.DateTime):
    def get_dbapi_type(self, dbapi):
        if self.timezone:
            return dbapi.TIMESTAMP_W_TZ
        else:
            return dbapi.TIMESTAMP


class AsyncpgBoolean(sqltypes.Boolean):
    def get_dbapi_type(self, dbapi):
        return dbapi.BOOLEAN


class AsyncPgInterval(INTERVAL):
    def get_dbapi_type(self, dbapi):
        return dbapi.INTERVAL

    @classmethod
    def adapt_emulated_to_native(cls, interval, **kw):

        return AsyncPgInterval(precision=interval.second_precision)


class AsyncPgEnum(ENUM):
    def get_dbapi_type(self, dbapi):
        return dbapi.ENUM


class AsyncpgInteger(sqltypes.Integer):
    def get_dbapi_type(self, dbapi):
        return dbapi.INTEGER


class AsyncpgBigInteger(sqltypes.BigInteger):
    def get_dbapi_type(self, dbapi):
        return dbapi.BIGINTEGER


class AsyncpgJSON(json.JSON):
    def get_dbapi_type(self, dbapi):
        return dbapi.JSON

    def result_processor(self, dialect, coltype):
        return None


class AsyncpgJSONB(json.JSONB):
    def get_dbapi_type(self, dbapi):
        return dbapi.JSONB

    def result_processor(self, dialect, coltype):
        return None


class AsyncpgJSONIndexType(sqltypes.JSON.JSONIndexType):
    def get_dbapi_type(self, dbapi):
        raise NotImplementedError("should not be here")


class AsyncpgJSONIntIndexType(sqltypes.JSON.JSONIntIndexType):
    def get_dbapi_type(self, dbapi):
        return dbapi.INTEGER


class AsyncpgJSONStrIndexType(sqltypes.JSON.JSONStrIndexType):
    def get_dbapi_type(self, dbapi):
        return dbapi.STRING


class AsyncpgJSONPathType(json.JSONPathType):
    def bind_processor(self, dialect):
        def process(value):
            assert isinstance(value, util.collections_abc.Sequence)
            tokens = [util.text_type(elem) for elem in value]
            return tokens

        return process


class AsyncpgUUID(UUID):
    def get_dbapi_type(self, dbapi):
        return dbapi.UUID

    def bind_processor(self, dialect):
        if not self.as_uuid and dialect.use_native_uuid:

            def process(value):
                if value is not None:
                    value = _python_UUID(value)
                return value

            return process

    def result_processor(self, dialect, coltype):
        if not self.as_uuid and dialect.use_native_uuid:

            def process(value):
                if value is not None:
                    value = str(value)
                return value

            return process


class AsyncpgNumeric(sqltypes.Numeric):
    def bind_processor(self, dialect):
        return None

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


class AsyncpgREGCLASS(REGCLASS):
    def get_dbapi_type(self, dbapi):
        return dbapi.STRING


class AsyncpgOID(OID):
    def get_dbapi_type(self, dbapi):
        return dbapi.INTEGER


class PGExecutionContext_asyncpg(PGExecutionContext):
    def pre_exec(self):
        if self.isddl:
            self._dbapi_connection.reset_schema_state()

        if not self.compiled:
            return

        # we have to exclude ENUM because "enum" not really a "type"
        # we can cast to, it has to be the name of the type itself.
        # for now we just omit it from casting
        self.exclude_set_input_sizes = {AsyncAdapt_asyncpg_dbapi.ENUM}

    def create_server_side_cursor(self):
        return self._dbapi_connection.cursor(server_side=True)


class PGCompiler_asyncpg(PGCompiler):
    pass


class PGIdentifierPreparer_asyncpg(PGIdentifierPreparer):
    pass


class AsyncAdapt_asyncpg_cursor:
    __slots__ = (
        "_adapt_connection",
        "_connection",
        "_rows",
        "description",
        "arraysize",
        "rowcount",
        "_inputsizes",
        "_cursor",
    )

    server_side = False

    def __init__(self, adapt_connection):
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self._rows = []
        self._cursor = None
        self.description = None
        self.arraysize = 1
        self.rowcount = -1
        self._inputsizes = None

    def close(self):
        self._rows[:] = []

    def _handle_exception(self, error):
        self._adapt_connection._handle_exception(error)

    def _parameters(self):
        if not self._inputsizes:
            return ("$%d" % idx for idx in itertools.count(1))
        else:

            return (
                "$%d::%s" % (idx, typ) if typ else "$%d" % idx
                for idx, typ in enumerate(
                    (_pg_types.get(typ) for typ in self._inputsizes), 1
                )
            )

    async def _prepare_and_execute(self, operation, parameters):
        # TODO: I guess cache these in an LRU cache, or see if we can
        # use some asyncpg concept

        # TODO: would be nice to support the dollar numeric thing
        # directly, this is much easier for now

        if not self._adapt_connection._started:
            await self._adapt_connection._start_transaction()

        params = self._parameters()
        operation = re.sub(r"\?", lambda m: next(params), operation)
        try:
            prepared_stmt = await self._connection.prepare(operation)

            attributes = prepared_stmt.get_attributes()
            if attributes:
                self.description = [
                    (attr.name, attr.type.oid, None, None, None, None, None)
                    for attr in prepared_stmt.get_attributes()
                ]
            else:
                self.description = None

            if self.server_side:
                self._cursor = await prepared_stmt.cursor(*parameters)
                self.rowcount = -1
            else:
                self._rows = await prepared_stmt.fetch(*parameters)
                status = prepared_stmt.get_statusmsg()

                reg = re.match(r"(?:UPDATE|DELETE|INSERT \d+) (\d+)", status)
                if reg:
                    self.rowcount = int(reg.group(1))
                else:
                    self.rowcount = -1

        except Exception as error:
            self._handle_exception(error)

    def execute(self, operation, parameters=()):
        try:
            self._adapt_connection.await_(
                self._prepare_and_execute(operation, parameters)
            )
        except Exception as error:
            self._handle_exception(error)

    def executemany(self, operation, seq_of_parameters):
        if not self._adapt_connection._started:
            self._adapt_connection.await_(
                self._adapt_connection._start_transaction()
            )

        params = self._parameters()
        operation = re.sub(r"\?", lambda m: next(params), operation)
        try:
            return self._adapt_connection.await_(
                self._connection.executemany(operation, seq_of_parameters)
            )
        except Exception as error:
            self._handle_exception(error)

    def setinputsizes(self, *inputsizes):
        self._inputsizes = inputsizes

    def __iter__(self):
        while self._rows:
            yield self._rows.pop(0)

    def fetchone(self):
        if self._rows:
            return self._rows.pop(0)
        else:
            return None

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize

        retval = self._rows[0:size]
        self._rows[:] = self._rows[size:]
        return retval

    def fetchall(self):
        retval = self._rows[:]
        self._rows[:] = []
        return retval


class AsyncAdapt_asyncpg_ss_cursor(AsyncAdapt_asyncpg_cursor):

    server_side = True
    __slots__ = ("_rowbuffer",)

    def __init__(self, adapt_connection):
        super(AsyncAdapt_asyncpg_ss_cursor, self).__init__(adapt_connection)
        self._rowbuffer = None

    def close(self):
        self._cursor = None
        self._rowbuffer = None

    def _buffer_rows(self):
        new_rows = self._adapt_connection.await_(self._cursor.fetch(50))
        self._rowbuffer = collections.deque(new_rows)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._rowbuffer:
            self._buffer_rows()

        while True:
            while self._rowbuffer:
                yield self._rowbuffer.popleft()

            self._buffer_rows()
            if not self._rowbuffer:
                break

    def fetchone(self):
        if not self._rowbuffer:
            self._buffer_rows()
            if not self._rowbuffer:
                return None
        return self._rowbuffer.popleft()

    def fetchmany(self, size=None):
        if size is None:
            return self.fetchall()

        if not self._rowbuffer:
            self._buffer_rows()

        buf = list(self._rowbuffer)
        lb = len(buf)
        if size > lb:
            buf.extend(
                self._adapt_connection.await_(self._cursor.fetch(size - lb))
            )

        result = buf[0:size]
        self._rowbuffer = collections.deque(buf[size:])
        return result

    def fetchall(self):
        ret = list(self._rowbuffer) + list(
            self._adapt_connection.await_(self._all())
        )
        self._rowbuffer.clear()
        return ret

    async def _all(self):
        rows = []

        # TODO: looks like we have to hand-roll some kind of batching here.
        # hardcoding for the moment but this should be improved.
        while True:
            batch = await self._cursor.fetch(1000)
            if batch:
                rows.extend(batch)
                continue
            else:
                break
        return rows

    def executemany(self, operation, seq_of_parameters):
        raise NotImplementedError(
            "server side cursor doesn't support executemany yet"
        )


class AsyncAdapt_asyncpg_connection:
    __slots__ = (
        "dbapi",
        "_connection",
        "isolation_level",
        "readonly",
        "deferrable",
        "_transaction",
        "_started",
    )

    await_ = staticmethod(await_only)

    def __init__(self, dbapi, connection):
        self.dbapi = dbapi
        self._connection = connection
        self.isolation_level = "read_committed"
        self.readonly = False
        self.deferrable = False
        self._transaction = None
        self._started = False

    def _handle_exception(self, error):
        if not isinstance(error, AsyncAdapt_asyncpg_dbapi.Error):
            exception_mapping = self.dbapi._asyncpg_error_translate

            for super_ in type(error).__mro__:
                if super_ in exception_mapping:
                    translated_error = exception_mapping[super_](
                        "%s: %s" % (type(error), error)
                    )
                    raise translated_error from error
            else:
                raise error
        else:
            raise error

    def set_isolation_level(self, level):
        if self._started:
            self.rollback()
        self.isolation_level = level

    async def _start_transaction(self):
        if self.isolation_level == "autocommit":
            return

        try:
            self._transaction = self._connection.transaction(
                isolation=self.isolation_level,
                readonly=self.readonly,
                deferrable=self.deferrable,
            )
            await self._transaction.start()
        except Exception as error:
            self._handle_exception(error)
        else:
            self._started = True

    def cursor(self, server_side=False):
        if server_side:
            return AsyncAdapt_asyncpg_ss_cursor(self)
        else:
            return AsyncAdapt_asyncpg_cursor(self)

    def reset_schema_state(self):
        self.await_(self._connection.reload_schema_state())

    def rollback(self):
        if self._started:
            self.await_(self._transaction.rollback())

            self._transaction = None
            self._started = False

    def commit(self):
        if self._started:
            self.await_(self._transaction.commit())
            self._transaction = None
            self._started = False

    def close(self):
        self.rollback()

        self.await_(self._connection.close())


class AsyncAdaptFallback_asyncpg_connection(AsyncAdapt_asyncpg_connection):
    await_ = staticmethod(await_fallback)


class AsyncAdapt_asyncpg_dbapi:
    def __init__(self, asyncpg):
        self.asyncpg = asyncpg
        self.paramstyle = "qmark"

    def connect(self, *arg, **kw):
        async_fallback = kw.pop("async_fallback", False)

        if async_fallback:
            return AsyncAdaptFallback_asyncpg_connection(
                self,
                await_fallback(self.asyncpg.connect(*arg, **kw)),
            )
        else:
            return AsyncAdapt_asyncpg_connection(
                self,
                await_only(self.asyncpg.connect(*arg, **kw)),
            )

    class Error(Exception):
        pass

    class Warning(Exception):  # noqa
        pass

    class InterfaceError(Error):
        pass

    class DatabaseError(Error):
        pass

    class InternalError(DatabaseError):
        pass

    class OperationalError(DatabaseError):
        pass

    class ProgrammingError(DatabaseError):
        pass

    class IntegrityError(DatabaseError):
        pass

    class DataError(DatabaseError):
        pass

    class NotSupportedError(DatabaseError):
        pass

    @util.memoized_property
    def _asyncpg_error_translate(self):
        import asyncpg

        return {
            asyncpg.exceptions.IntegrityConstraintViolationError: self.IntegrityError,  # noqa
            asyncpg.exceptions.PostgresError: self.Error,
            asyncpg.exceptions.SyntaxOrAccessError: self.ProgrammingError,
            asyncpg.exceptions.InterfaceError: self.InterfaceError,
        }

    def Binary(self, value):
        return value

    STRING = util.symbol("STRING")
    TIMESTAMP = util.symbol("TIMESTAMP")
    TIMESTAMP_W_TZ = util.symbol("TIMESTAMP_W_TZ")
    TIME = util.symbol("TIME")
    DATE = util.symbol("DATE")
    INTERVAL = util.symbol("INTERVAL")
    NUMBER = util.symbol("NUMBER")
    FLOAT = util.symbol("FLOAT")
    BOOLEAN = util.symbol("BOOLEAN")
    INTEGER = util.symbol("INTEGER")
    BIGINTEGER = util.symbol("BIGINTEGER")
    BYTES = util.symbol("BYTES")
    DECIMAL = util.symbol("DECIMAL")
    JSON = util.symbol("JSON")
    JSONB = util.symbol("JSONB")
    ENUM = util.symbol("ENUM")
    UUID = util.symbol("UUID")
    BYTEA = util.symbol("BYTEA")

    DATETIME = TIMESTAMP
    BINARY = BYTEA


_pg_types = {
    AsyncAdapt_asyncpg_dbapi.STRING: "varchar",
    AsyncAdapt_asyncpg_dbapi.TIMESTAMP: "timestamp",
    AsyncAdapt_asyncpg_dbapi.TIMESTAMP_W_TZ: "timestamp with time zone",
    AsyncAdapt_asyncpg_dbapi.DATE: "date",
    AsyncAdapt_asyncpg_dbapi.TIME: "time",
    AsyncAdapt_asyncpg_dbapi.INTERVAL: "interval",
    AsyncAdapt_asyncpg_dbapi.NUMBER: "numeric",
    AsyncAdapt_asyncpg_dbapi.FLOAT: "float",
    AsyncAdapt_asyncpg_dbapi.BOOLEAN: "bool",
    AsyncAdapt_asyncpg_dbapi.INTEGER: "integer",
    AsyncAdapt_asyncpg_dbapi.BIGINTEGER: "bigint",
    AsyncAdapt_asyncpg_dbapi.BYTES: "bytes",
    AsyncAdapt_asyncpg_dbapi.DECIMAL: "decimal",
    AsyncAdapt_asyncpg_dbapi.JSON: "json",
    AsyncAdapt_asyncpg_dbapi.JSONB: "jsonb",
    AsyncAdapt_asyncpg_dbapi.ENUM: "enum",
    AsyncAdapt_asyncpg_dbapi.UUID: "uuid",
    AsyncAdapt_asyncpg_dbapi.BYTEA: "bytea",
}


class PGDialect_asyncpg(PGDialect):
    driver = "asyncpg"

    supports_unicode_statements = True
    supports_server_side_cursors = True

    supports_unicode_binds = True

    default_paramstyle = "qmark"
    supports_sane_multi_rowcount = False
    execution_ctx_cls = PGExecutionContext_asyncpg
    statement_compiler = PGCompiler_asyncpg
    preparer = PGIdentifierPreparer_asyncpg

    use_setinputsizes = True

    use_native_uuid = True

    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.Time: AsyncpgTime,
            sqltypes.Date: AsyncpgDate,
            sqltypes.DateTime: AsyncpgDateTime,
            sqltypes.Interval: AsyncPgInterval,
            INTERVAL: AsyncPgInterval,
            UUID: AsyncpgUUID,
            sqltypes.Boolean: AsyncpgBoolean,
            sqltypes.Integer: AsyncpgInteger,
            sqltypes.BigInteger: AsyncpgBigInteger,
            sqltypes.Numeric: AsyncpgNumeric,
            sqltypes.JSON: AsyncpgJSON,
            json.JSONB: AsyncpgJSONB,
            sqltypes.JSON.JSONPathType: AsyncpgJSONPathType,
            sqltypes.JSON.JSONIndexType: AsyncpgJSONIndexType,
            sqltypes.JSON.JSONIntIndexType: AsyncpgJSONIntIndexType,
            sqltypes.JSON.JSONStrIndexType: AsyncpgJSONStrIndexType,
            sqltypes.Enum: AsyncPgEnum,
            OID: AsyncpgOID,
            REGCLASS: AsyncpgREGCLASS,
        },
    )

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
        return AsyncAdapt_asyncpg_dbapi(__import__("asyncpg"))

    @util.memoized_property
    def _isolation_lookup(self):
        return {
            "AUTOCOMMIT": "autocommit",
            "READ COMMITTED": "read_committed",
            "REPEATABLE READ": "repeatable_read",
            "SERIALIZABLE": "serializable",
        }

    def set_isolation_level(self, connection, level):
        try:
            level = self._isolation_lookup[level.replace("_", " ")]
        except KeyError as err:
            util.raise_(
                exc.ArgumentError(
                    "Invalid value '%s' for isolation_level. "
                    "Valid isolation levels for %s are %s"
                    % (level, self.name, ", ".join(self._isolation_lookup))
                ),
                replace_context=err,
            )

        connection.set_isolation_level(level)

    def set_readonly(self, connection, value):
        connection.readonly = value

    def get_readonly(self, connection):
        return connection.readonly

    def set_deferrable(self, connection, value):
        connection.deferrable = value

    def get_deferrable(self, connection):
        return connection.deferrable

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username="user")
        if "port" in opts:
            opts["port"] = int(opts["port"])
        opts.update(url.query)
        return ([], opts)

    @classmethod
    def get_pool_class(self, url):
        return pool.AsyncAdaptedQueuePool

    def is_disconnect(self, e, connection, cursor):
        if connection:
            return connection._connection.is_closed()
        else:
            return isinstance(
                e, self.dbapi.InterfaceError
            ) and "connection is closed" in str(e)

    def do_set_input_sizes(self, cursor, list_of_tuples, context):
        if self.positional:
            cursor.setinputsizes(
                *[dbtype for key, dbtype, sqltype in list_of_tuples]
            )
        else:
            cursor.setinputsizes(
                **{
                    key: dbtype
                    for key, dbtype, sqltype in list_of_tuples
                    if dbtype
                }
            )

    def on_connect(self):
        super_connect = super(PGDialect_asyncpg, self).on_connect()

        def _jsonb_encoder(str_value):
            # \x01 is the prefix for jsonb used by PostgreSQL.
            # asyncpg requires it when format='binary'
            return b"\x01" + str_value.encode()

        deserializer = self._json_deserializer or _py_json.loads

        def _json_decoder(bin_value):
            return deserializer(bin_value.decode())

        def _jsonb_decoder(bin_value):
            # the byte is the \x01 prefix for jsonb used by PostgreSQL.
            # asyncpg returns it when format='binary'
            return deserializer(bin_value[1:].decode())

        async def _setup_type_codecs(conn):
            """set up type decoders at the asyncpg level.

            these are set_type_codec() calls to normalize
            There was a tentative decoder for the "char" datatype here
            to have it return strings however this type is actually a binary
            type that other drivers are likely mis-interpreting.

            See https://github.com/MagicStack/asyncpg/issues/623 for reference
            on why it's set up this way.
            """
            await conn._connection.set_type_codec(
                "json",
                encoder=str.encode,
                decoder=_json_decoder,
                schema="pg_catalog",
                format="binary",
            )
            await conn._connection.set_type_codec(
                "jsonb",
                encoder=_jsonb_encoder,
                decoder=_jsonb_decoder,
                schema="pg_catalog",
                format="binary",
            )

        def connect(conn):
            conn.await_(_setup_type_codecs(conn))
            if super_connect is not None:
                super_connect(conn)

        return connect


dialect = PGDialect_asyncpg
