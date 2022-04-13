import decimal

from .array import ARRAY as PGARRAY
from .base import _DECIMAL_TYPES
from .base import _FLOAT_TYPES
from .base import _INT_TYPES
from .base import PGDialect
from .base import PGExecutionContext
from .base import UUID
from .hstore import HSTORE
from ... import exc
from ... import types as sqltypes
from ... import util
from ...engine import processors

_server_side_id = util.counter()


class _PsycopgNumeric(sqltypes.Numeric):
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            if coltype in _FLOAT_TYPES:
                return processors.to_decimal_processor_factory(
                    decimal.Decimal, self._effective_decimal_return_scale
                )
            elif coltype in _DECIMAL_TYPES or coltype in _INT_TYPES:
                # psycopg returns Decimal natively for 1700
                return None
            else:
                raise exc.InvalidRequestError(
                    "Unknown PG numeric type: %d" % coltype
                )
        else:
            if coltype in _FLOAT_TYPES:
                # psycopg returns float natively for 701
                return None
            elif coltype in _DECIMAL_TYPES or coltype in _INT_TYPES:
                return processors.to_float
            else:
                raise exc.InvalidRequestError(
                    "Unknown PG numeric type: %d" % coltype
                )


class _PsycopgHStore(HSTORE):
    def bind_processor(self, dialect):
        if dialect._has_native_hstore:
            return None
        else:
            return super(_PsycopgHStore, self).bind_processor(dialect)

    def result_processor(self, dialect, coltype):
        if dialect._has_native_hstore:
            return None
        else:
            return super(_PsycopgHStore, self).result_processor(
                dialect, coltype
            )


class _PsycopgUUID(UUID):
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        if not self.as_uuid and dialect.use_native_uuid:

            def process(value):
                if value is not None:
                    value = str(value)
                return value

            return process


class _PsycopgARRAY(PGARRAY):
    render_bind_cast = True


class _PGExecutionContext_common_psycopg(PGExecutionContext):
    def create_server_side_cursor(self):
        # use server-side cursors:
        # psycopg
        # https://www.psycopg.org/psycopg3/docs/advanced/cursors.html#server-side-cursors
        # psycopg2
        # https://www.psycopg.org/docs/usage.html#server-side-cursors
        ident = "c_%s_%s" % (hex(id(self))[2:], hex(_server_side_id())[2:])
        return self._dbapi_connection.cursor(ident)


class _PGDialect_common_psycopg(PGDialect):
    supports_statement_cache = True
    supports_server_side_cursors = True

    default_paramstyle = "pyformat"

    _has_native_hstore = True

    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.Numeric: _PsycopgNumeric,
            HSTORE: _PsycopgHStore,
            UUID: _PsycopgUUID,
            sqltypes.ARRAY: _PsycopgARRAY,
        },
    )

    def __init__(
        self,
        client_encoding=None,
        use_native_hstore=True,
        use_native_uuid=True,
        **kwargs,
    ):
        PGDialect.__init__(self, **kwargs)
        if not use_native_hstore:
            self._has_native_hstore = False
        self.use_native_hstore = use_native_hstore
        self.use_native_uuid = use_native_uuid
        self.client_encoding = client_encoding

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username="user", database="dbname")

        is_multihost = False
        if "host" in url.query:
            is_multihost = isinstance(url.query["host"], (list, tuple))

        if opts:
            if "port" in opts:
                opts["port"] = int(opts["port"])
            opts.update(url.query)
            if is_multihost:
                opts["host"] = ",".join(url.query["host"])
            # send individual dbname, user, password, host, port
            # parameters to psycopg2.connect()
            return ([], opts)
        elif url.query:
            # any other connection arguments, pass directly
            opts.update(url.query)
            if is_multihost:
                opts["host"] = ",".join(url.query["host"])
            return ([], opts)
        else:
            # no connection arguments whatsoever; psycopg2.connect()
            # requires that "dsn" be present as a blank string.
            return ([""], opts)

    def get_isolation_level_values(self, dbapi_connection):
        return (
            "AUTOCOMMIT",
            "READ COMMITTED",
            "READ UNCOMMITTED",
            "REPEATABLE READ",
            "SERIALIZABLE",
        )

    def set_deferrable(self, connection, value):
        connection.deferrable = value

    def get_deferrable(self, connection):
        return connection.deferrable

    def _do_autocommit(self, connection, value):
        connection.autocommit = value

    def do_ping(self, dbapi_connection):
        cursor = None
        try:
            self._do_autocommit(dbapi_connection, True)
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute(self._dialect_specific_select_one)
            finally:
                cursor.close()
                if not dbapi_connection.closed:
                    self._do_autocommit(dbapi_connection, False)
        except self.dbapi.Error as err:
            if self.is_disconnect(err, dbapi_connection, cursor):
                return False
            else:
                raise
        else:
            return True
