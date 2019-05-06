# engine/default.py
# Copyright (C) 2005-2019 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Default implementations of per-dialect sqlalchemy.engine classes.

These are semi-private implementation classes which are only of importance
to database dialect authors; dialects will usually use the classes here
as the base class for their own corresponding classes.

"""

import codecs
import random
import re
import weakref

from . import interfaces
from . import reflection
from . import result
from .. import event
from .. import exc
from .. import pool
from .. import processors
from .. import types as sqltypes
from .. import util
from ..sql import compiler
from ..sql import expression
from ..sql import schema


AUTOCOMMIT_REGEXP = re.compile(
    r"\s*(?:UPDATE|INSERT|CREATE|DELETE|DROP|ALTER)", re.I | re.UNICODE
)

# When we're handed literal SQL, ensure it's a SELECT query
SERVER_SIDE_CURSOR_RE = re.compile(r"\s*SELECT", re.I | re.UNICODE)


class DefaultDialect(interfaces.Dialect):
    """Default implementation of Dialect"""

    statement_compiler = compiler.SQLCompiler
    ddl_compiler = compiler.DDLCompiler
    type_compiler = compiler.GenericTypeCompiler
    preparer = compiler.IdentifierPreparer
    supports_alter = True
    supports_comments = False
    inline_comments = False

    # the first value we'd get for an autoincrement
    # column.
    default_sequence_base = 1

    # most DBAPIs happy with this for execute().
    # not cx_oracle.
    execute_sequence_format = tuple

    supports_views = True
    supports_sequences = False
    sequences_optional = False
    preexecute_autoincrement_sequences = False
    postfetch_lastrowid = True
    implicit_returning = False

    supports_right_nested_joins = True
    cte_follows_insert = False

    supports_native_enum = False
    supports_native_boolean = False
    non_native_boolean_check_constraint = True

    supports_simple_order_by_label = True

    engine_config_types = util.immutabledict(
        [
            ("convert_unicode", util.bool_or_str("force")),
            ("pool_timeout", util.asint),
            ("echo", util.bool_or_str("debug")),
            ("echo_pool", util.bool_or_str("debug")),
            ("pool_recycle", util.asint),
            ("pool_size", util.asint),
            ("max_overflow", util.asint),
            ("pool_threadlocal", util.asbool),
        ]
    )

    # if the NUMERIC type
    # returns decimal.Decimal.
    # *not* the FLOAT type however.
    supports_native_decimal = False

    if util.py3k:
        supports_unicode_statements = True
        supports_unicode_binds = True
        returns_unicode_strings = True
        description_encoding = None
    else:
        supports_unicode_statements = False
        supports_unicode_binds = False
        returns_unicode_strings = False
        description_encoding = "use_encoding"

    name = "default"

    # length at which to truncate
    # any identifier.
    max_identifier_length = 9999

    # length at which to truncate
    # the name of an index.
    # Usually None to indicate
    # 'use max_identifier_length'.
    # thanks to MySQL, sigh
    max_index_name_length = None

    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    colspecs = {}
    default_paramstyle = "named"
    supports_default_values = False
    supports_empty_insert = True
    supports_multivalues_insert = False

    supports_server_side_cursors = False

    server_version_info = None

    construct_arguments = None
    """Optional set of argument specifiers for various SQLAlchemy
    constructs, typically schema items.

    To implement, establish as a series of tuples, as in::

        construct_arguments = [
            (schema.Index, {
                "using": False,
                "where": None,
                "ops": None
            })
        ]

    If the above construct is established on the PostgreSQL dialect,
    the :class:`.Index` construct will now accept the keyword arguments
    ``postgresql_using``, ``postgresql_where``, nad ``postgresql_ops``.
    Any other argument specified to the constructor of :class:`.Index`
    which is prefixed with ``postgresql_`` will raise :class:`.ArgumentError`.

    A dialect which does not include a ``construct_arguments`` member will
    not participate in the argument validation system.  For such a dialect,
    any argument name is accepted by all participating constructs, within
    the namespace of arguments prefixed with that dialect name.  The rationale
    here is so that third-party dialects that haven't yet implemented this
    feature continue to function in the old way.

    .. versionadded:: 0.9.2

    .. seealso::

        :class:`.DialectKWArgs` - implementing base class which consumes
        :attr:`.DefaultDialect.construct_arguments`


    """

    # indicates symbol names are
    # UPPERCASEd if they are case insensitive
    # within the database.
    # if this is True, the methods normalize_name()
    # and denormalize_name() must be provided.
    requires_name_normalize = False

    reflection_options = ()

    dbapi_exception_translation_map = util.immutabledict()
    """mapping used in the extremely unusual case that a DBAPI's
    published exceptions don't actually have the __name__ that they
    are linked towards.

    .. versionadded:: 1.0.5

    """

    @util.deprecated_params(
        convert_unicode=(
            "1.3",
            "The :paramref:`.create_engine.convert_unicode` parameter "
            "and corresponding dialect-level parameters are deprecated, "
            "and will be removed in a future release.  Modern DBAPIs support "
            "Python Unicode natively and this parameter is unnecessary.",
        )
    )
    def __init__(
        self,
        convert_unicode=False,
        encoding="utf-8",
        paramstyle=None,
        dbapi=None,
        implicit_returning=None,
        supports_right_nested_joins=None,
        case_sensitive=True,
        supports_native_boolean=None,
        empty_in_strategy="static",
        label_length=None,
        **kwargs
    ):

        if not getattr(self, "ported_sqla_06", True):
            util.warn(
                "The %s dialect is not yet ported to the 0.6 format"
                % self.name
            )

        self.convert_unicode = convert_unicode
        self.encoding = encoding
        self.positional = False
        self._ischema = None
        self.dbapi = dbapi
        if paramstyle is not None:
            self.paramstyle = paramstyle
        elif self.dbapi is not None:
            self.paramstyle = self.dbapi.paramstyle
        else:
            self.paramstyle = self.default_paramstyle
        if implicit_returning is not None:
            self.implicit_returning = implicit_returning
        self.positional = self.paramstyle in ("qmark", "format", "numeric")
        self.identifier_preparer = self.preparer(self)
        self.type_compiler = self.type_compiler(self)
        if supports_right_nested_joins is not None:
            self.supports_right_nested_joins = supports_right_nested_joins
        if supports_native_boolean is not None:
            self.supports_native_boolean = supports_native_boolean
        self.case_sensitive = case_sensitive

        self.empty_in_strategy = empty_in_strategy
        if empty_in_strategy == "static":
            self._use_static_in = True
        elif empty_in_strategy in ("dynamic", "dynamic_warn"):
            self._use_static_in = False
            self._warn_on_empty_in = empty_in_strategy == "dynamic_warn"
        else:
            raise exc.ArgumentError(
                "empty_in_strategy may be 'static', "
                "'dynamic', or 'dynamic_warn'"
            )

        if label_length and label_length > self.max_identifier_length:
            raise exc.ArgumentError(
                "Label length of %d is greater than this dialect's"
                " maximum identifier length of %d"
                % (label_length, self.max_identifier_length)
            )
        self.label_length = label_length

        if self.description_encoding == "use_encoding":
            self._description_decoder = (
                processors.to_unicode_processor_factory
            )(encoding)
        elif self.description_encoding is not None:
            self._description_decoder = (
                processors.to_unicode_processor_factory
            )(self.description_encoding)
        self._encoder = codecs.getencoder(self.encoding)
        self._decoder = processors.to_unicode_processor_factory(self.encoding)

    @util.memoized_property
    def _type_memos(self):
        return weakref.WeakKeyDictionary()

    @property
    def dialect_description(self):
        return self.name + "+" + self.driver

    @property
    def supports_sane_rowcount_returning(self):
        return self.supports_sane_rowcount

    @classmethod
    def get_pool_class(cls, url):
        return getattr(cls, "poolclass", pool.QueuePool)

    def initialize(self, connection):
        try:
            self.server_version_info = self._get_server_version_info(
                connection
            )
        except NotImplementedError:
            self.server_version_info = None
        try:
            self.default_schema_name = self._get_default_schema_name(
                connection
            )
        except NotImplementedError:
            self.default_schema_name = None

        try:
            self.default_isolation_level = self.get_isolation_level(
                connection.connection
            )
        except NotImplementedError:
            self.default_isolation_level = None

        self.returns_unicode_strings = self._check_unicode_returns(connection)

        if (
            self.description_encoding is not None
            and self._check_unicode_description(connection)
        ):
            self._description_decoder = self.description_encoding = None

    def on_connect(self):
        """return a callable which sets up a newly created DBAPI connection.

        This is used to set dialect-wide per-connection options such as
        isolation modes, unicode modes, etc.

        If a callable is returned, it will be assembled into a pool listener
        that receives the direct DBAPI connection, with all wrappers removed.

        If None is returned, no listener will be generated.

        """
        return None

    def _check_unicode_returns(self, connection, additional_tests=None):
        if util.py2k and not self.supports_unicode_statements:
            cast_to = util.binary_type
        else:
            cast_to = util.text_type

        if self.positional:
            parameters = self.execute_sequence_format()
        else:
            parameters = {}

        def check_unicode(test):
            statement = cast_to(
                expression.select([test]).compile(dialect=self)
            )
            try:
                cursor = connection.connection.cursor()
                connection._cursor_execute(cursor, statement, parameters)
                row = cursor.fetchone()
                cursor.close()
            except exc.DBAPIError as de:
                # note that _cursor_execute() will have closed the cursor
                # if an exception is thrown.
                util.warn(
                    "Exception attempting to "
                    "detect unicode returns: %r" % de
                )
                return False
            else:
                return isinstance(row[0], util.text_type)

        tests = [
            # detect plain VARCHAR
            expression.cast(
                expression.literal_column("'test plain returns'"),
                sqltypes.VARCHAR(60),
            ),
            # detect if there's an NVARCHAR type with different behavior
            # available
            expression.cast(
                expression.literal_column("'test unicode returns'"),
                sqltypes.Unicode(60),
            ),
        ]

        if additional_tests:
            tests += additional_tests

        results = {check_unicode(test) for test in tests}

        if results.issuperset([True, False]):
            return "conditional"
        else:
            return results == {True}

    def _check_unicode_description(self, connection):
        # all DBAPIs on Py2K return cursor.description as encoded,
        # until pypy2.1beta2 with sqlite, so let's just check it -
        # it's likely others will start doing this too in Py2k.

        if util.py2k and not self.supports_unicode_statements:
            cast_to = util.binary_type
        else:
            cast_to = util.text_type

        cursor = connection.connection.cursor()
        try:
            cursor.execute(
                cast_to(
                    expression.select(
                        [expression.literal_column("'x'").label("some_label")]
                    ).compile(dialect=self)
                )
            )
            return isinstance(cursor.description[0][0], util.text_type)
        finally:
            cursor.close()

    def type_descriptor(self, typeobj):
        """Provide a database-specific :class:`.TypeEngine` object, given
        the generic object which comes from the types module.

        This method looks for a dictionary called
        ``colspecs`` as a class or instance-level variable,
        and passes on to :func:`.types.adapt_type`.

        """
        return sqltypes.adapt_type(typeobj, self.colspecs)

    def reflecttable(
        self,
        connection,
        table,
        include_columns,
        exclude_columns,
        resolve_fks,
        **opts
    ):
        insp = reflection.Inspector.from_engine(connection)
        return insp.reflecttable(
            table, include_columns, exclude_columns, resolve_fks, **opts
        )

    def get_pk_constraint(self, conn, table_name, schema=None, **kw):
        """Compatibility method, adapts the result of get_primary_keys()
        for those dialects which don't implement get_pk_constraint().

        """
        return {
            "constrained_columns": self.get_primary_keys(
                conn, table_name, schema=schema, **kw
            )
        }

    def validate_identifier(self, ident):
        if len(ident) > self.max_identifier_length:
            raise exc.IdentifierError(
                "Identifier '%s' exceeds maximum length of %d characters"
                % (ident, self.max_identifier_length)
            )

    def connect(self, *cargs, **cparams):
        return self.dbapi.connect(*cargs, **cparams)

    def create_connect_args(self, url):
        opts = url.translate_connect_args()
        opts.update(url.query)
        return [[], opts]

    def set_engine_execution_options(self, engine, opts):
        if "isolation_level" in opts:
            isolation_level = opts["isolation_level"]

            @event.listens_for(engine, "engine_connect")
            def set_isolation(connection, branch):
                if not branch:
                    self._set_connection_isolation(connection, isolation_level)

        if "schema_translate_map" in opts:
            getter = schema._schema_getter(opts["schema_translate_map"])
            engine.schema_for_object = getter

            @event.listens_for(engine, "engine_connect")
            def set_schema_translate_map(connection, branch):
                connection.schema_for_object = getter

    def set_connection_execution_options(self, connection, opts):
        if "isolation_level" in opts:
            self._set_connection_isolation(connection, opts["isolation_level"])

        if "schema_translate_map" in opts:
            getter = schema._schema_getter(opts["schema_translate_map"])
            connection.schema_for_object = getter

    def _set_connection_isolation(self, connection, level):
        if connection.in_transaction():
            util.warn(
                "Connection is already established with a Transaction; "
                "setting isolation_level may implicitly rollback or commit "
                "the existing transaction, or have no effect until "
                "next transaction"
            )
        self.set_isolation_level(connection.connection, level)
        connection.connection._connection_record.finalize_callback.append(
            self.reset_isolation_level
        )

    def do_begin(self, dbapi_connection):
        pass

    def do_rollback(self, dbapi_connection):
        dbapi_connection.rollback()

    def do_commit(self, dbapi_connection):
        dbapi_connection.commit()

    def do_close(self, dbapi_connection):
        dbapi_connection.close()

    @util.memoized_property
    def _dialect_specific_select_one(self):
        return str(expression.select([1]).compile(dialect=self))

    def do_ping(self, dbapi_connection):
        cursor = None
        try:
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute(self._dialect_specific_select_one)
            finally:
                cursor.close()
        except self.dbapi.Error as err:
            if self.is_disconnect(err, dbapi_connection, cursor):
                return False
            else:
                raise
        else:
            return True

    def create_xid(self):
        """Create a random two-phase transaction ID.

        This id will be passed to do_begin_twophase(), do_rollback_twophase(),
        do_commit_twophase().  Its format is unspecified.
        """

        return "_sa_%032x" % random.randint(0, 2 ** 128)

    def do_savepoint(self, connection, name):
        connection.execute(expression.SavepointClause(name))

    def do_rollback_to_savepoint(self, connection, name):
        connection.execute(expression.RollbackToSavepointClause(name))

    def do_release_savepoint(self, connection, name):
        connection.execute(expression.ReleaseSavepointClause(name))

    def do_executemany(self, cursor, statement, parameters, context=None):
        cursor.executemany(statement, parameters)

    def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(statement, parameters)

    def do_execute_no_params(self, cursor, statement, context=None):
        cursor.execute(statement)

    def is_disconnect(self, e, connection, cursor):
        return False

    def reset_isolation_level(self, dbapi_conn):
        # default_isolation_level is read from the first connection
        # after the initial set of 'isolation_level', if any, so is
        # the configured default of this dialect.
        self.set_isolation_level(dbapi_conn, self.default_isolation_level)


class StrCompileDialect(DefaultDialect):

    statement_compiler = compiler.StrSQLCompiler
    ddl_compiler = compiler.DDLCompiler
    type_compiler = compiler.StrSQLTypeCompiler
    preparer = compiler.IdentifierPreparer

    supports_sequences = True
    sequences_optional = True
    preexecute_autoincrement_sequences = False
    implicit_returning = False

    supports_native_boolean = True

    supports_simple_order_by_label = True


class DefaultExecutionContext(interfaces.ExecutionContext):
    isinsert = False
    isupdate = False
    isdelete = False
    is_crud = False
    is_text = False
    isddl = False
    executemany = False
    compiled = None
    statement = None
    result_column_struct = None
    returned_defaults = None
    _is_implicit_returning = False
    _is_explicit_returning = False

    # a hook for SQLite's translation of
    # result column names
    _translate_colname = None

    _expanded_parameters = util.immutabledict()

    @classmethod
    def _init_ddl(cls, dialect, connection, dbapi_connection, compiled_ddl):
        """Initialize execution context for a DDLElement construct."""

        self = cls.__new__(cls)
        self.root_connection = connection
        self._dbapi_connection = dbapi_connection
        self.dialect = connection.dialect

        self.compiled = compiled = compiled_ddl
        self.isddl = True

        self.execution_options = compiled.execution_options
        if connection._execution_options:
            self.execution_options = dict(self.execution_options)
            self.execution_options.update(connection._execution_options)

        if not dialect.supports_unicode_statements:
            self.unicode_statement = util.text_type(compiled)
            self.statement = dialect._encoder(self.unicode_statement)[0]
        else:
            self.statement = self.unicode_statement = util.text_type(compiled)

        self.cursor = self.create_cursor()
        self.compiled_parameters = []

        if dialect.positional:
            self.parameters = [dialect.execute_sequence_format()]
        else:
            self.parameters = [{}]

        return self

    @classmethod
    def _init_compiled(
        cls, dialect, connection, dbapi_connection, compiled, parameters
    ):
        """Initialize execution context for a Compiled construct."""

        self = cls.__new__(cls)
        self.root_connection = connection
        self._dbapi_connection = dbapi_connection
        self.dialect = connection.dialect

        self.compiled = compiled

        # this should be caught in the engine before
        # we get here
        assert compiled.can_execute

        self.execution_options = compiled.execution_options.union(
            connection._execution_options
        )

        self.result_column_struct = (
            compiled._result_columns,
            compiled._ordered_columns,
            compiled._textual_ordered_columns,
        )

        self.unicode_statement = util.text_type(compiled)
        if not dialect.supports_unicode_statements:
            self.statement = self.unicode_statement.encode(
                self.dialect.encoding
            )
        else:
            self.statement = self.unicode_statement

        self.isinsert = compiled.isinsert
        self.isupdate = compiled.isupdate
        self.isdelete = compiled.isdelete
        self.is_text = compiled.isplaintext

        if not parameters:
            self.compiled_parameters = [compiled.construct_params()]
        else:
            self.compiled_parameters = [
                compiled.construct_params(m, _group_number=grp)
                for grp, m in enumerate(parameters)
            ]

            self.executemany = len(parameters) > 1

        self.cursor = self.create_cursor()

        if self.isinsert or self.isupdate or self.isdelete:
            self.is_crud = True
            self._is_explicit_returning = bool(compiled.statement._returning)
            self._is_implicit_returning = bool(
                compiled.returning and not compiled.statement._returning
            )

        if self.compiled.insert_prefetch or self.compiled.update_prefetch:
            if self.executemany:
                self._process_executemany_defaults()
            else:
                self._process_executesingle_defaults()

        processors = compiled._bind_processors

        if compiled.contains_expanding_parameters:
            positiontup = self._expand_in_parameters(compiled, processors)
        elif compiled.positional:
            positiontup = self.compiled.positiontup

        # Convert the dictionary of bind parameter values
        # into a dict or list to be sent to the DBAPI's
        # execute() or executemany() method.
        parameters = []
        if compiled.positional:
            for compiled_params in self.compiled_parameters:
                param = []
                for key in positiontup:
                    if key in processors:
                        param.append(processors[key](compiled_params[key]))
                    else:
                        param.append(compiled_params[key])
                parameters.append(dialect.execute_sequence_format(param))
        else:
            encode = not dialect.supports_unicode_statements
            for compiled_params in self.compiled_parameters:

                if encode:
                    param = dict(
                        (
                            dialect._encoder(key)[0],
                            processors[key](compiled_params[key])
                            if key in processors
                            else compiled_params[key],
                        )
                        for key in compiled_params
                    )
                else:
                    param = dict(
                        (
                            key,
                            processors[key](compiled_params[key])
                            if key in processors
                            else compiled_params[key],
                        )
                        for key in compiled_params
                    )

                parameters.append(param)

        self.parameters = dialect.execute_sequence_format(parameters)

        return self

    def _expand_in_parameters(self, compiled, processors):
        """handle special 'expanding' parameters, IN tuples that are rendered
        on a per-parameter basis for an otherwise fixed SQL statement string.

        """
        if self.executemany:
            raise exc.InvalidRequestError(
                "'expanding' parameters can't be used with " "executemany()"
            )

        if self.compiled.positional and self.compiled._numeric_binds:
            # I'm not familiar with any DBAPI that uses 'numeric'
            raise NotImplementedError(
                "'expanding' bind parameters not supported with "
                "'numeric' paramstyle at this time."
            )

        self._expanded_parameters = {}

        compiled_params = self.compiled_parameters[0]
        if compiled.positional:
            positiontup = []
        else:
            positiontup = None

        replacement_expressions = {}
        to_update_sets = {}

        for name in (
            self.compiled.positiontup
            if compiled.positional
            else self.compiled.binds
        ):
            parameter = self.compiled.binds[name]
            if parameter.expanding:

                if name in replacement_expressions:
                    to_update = to_update_sets[name]
                else:
                    # we are removing the parameter from compiled_params
                    # because it is a list value, which is not expected by
                    # TypeEngine objects that would otherwise be asked to
                    # process it. the single name is being replaced with
                    # individual numbered parameters for each value in the
                    # param.
                    values = compiled_params.pop(name)

                    if not values:
                        to_update = to_update_sets[name] = []
                        replacement_expressions[
                            name
                        ] = self.compiled.visit_empty_set_expr(
                            parameter._expanding_in_types
                            if parameter._expanding_in_types
                            else [parameter.type]
                        )

                    elif isinstance(values[0], (tuple, list)):
                        to_update = to_update_sets[name] = [
                            ("%s_%s_%s" % (name, i, j), value)
                            for i, tuple_element in enumerate(values, 1)
                            for j, value in enumerate(tuple_element, 1)
                        ]
                        replacement_expressions[name] = ", ".join(
                            "(%s)"
                            % ", ".join(
                                self.compiled.bindtemplate
                                % {
                                    "name": to_update[
                                        i * len(tuple_element) + j
                                    ][0]
                                }
                                for j, value in enumerate(tuple_element)
                            )
                            for i, tuple_element in enumerate(values)
                        )
                    else:
                        to_update = to_update_sets[name] = [
                            ("%s_%s" % (name, i), value)
                            for i, value in enumerate(values, 1)
                        ]
                        replacement_expressions[name] = ", ".join(
                            self.compiled.bindtemplate % {"name": key}
                            for key, value in to_update
                        )

                compiled_params.update(to_update)
                processors.update(
                    (key, processors[name])
                    for key, value in to_update
                    if name in processors
                )
                if compiled.positional:
                    positiontup.extend(name for name, value in to_update)
                self._expanded_parameters[name] = [
                    expand_key for expand_key, value in to_update
                ]
            elif compiled.positional:
                positiontup.append(name)

        def process_expanding(m):
            return replacement_expressions[m.group(1)]

        self.statement = re.sub(
            r"\[EXPANDING_(\S+)\]", process_expanding, self.statement
        )
        return positiontup

    @classmethod
    def _init_statement(
        cls, dialect, connection, dbapi_connection, statement, parameters
    ):
        """Initialize execution context for a string SQL statement."""

        self = cls.__new__(cls)
        self.root_connection = connection
        self._dbapi_connection = dbapi_connection
        self.dialect = connection.dialect
        self.is_text = True

        # plain text statement
        self.execution_options = connection._execution_options

        if not parameters:
            if self.dialect.positional:
                self.parameters = [dialect.execute_sequence_format()]
            else:
                self.parameters = [{}]
        elif isinstance(parameters[0], dialect.execute_sequence_format):
            self.parameters = parameters
        elif isinstance(parameters[0], dict):
            if dialect.supports_unicode_statements:
                self.parameters = parameters
            else:
                self.parameters = [
                    {dialect._encoder(k)[0]: d[k] for k in d}
                    for d in parameters
                ] or [{}]
        else:
            self.parameters = [
                dialect.execute_sequence_format(p) for p in parameters
            ]

        self.executemany = len(parameters) > 1

        if not dialect.supports_unicode_statements and isinstance(
            statement, util.text_type
        ):
            self.unicode_statement = statement
            self.statement = dialect._encoder(statement)[0]
        else:
            self.statement = self.unicode_statement = statement

        self.cursor = self.create_cursor()
        return self

    @classmethod
    def _init_default(cls, dialect, connection, dbapi_connection):
        """Initialize execution context for a ColumnDefault construct."""

        self = cls.__new__(cls)
        self.root_connection = connection
        self._dbapi_connection = dbapi_connection
        self.dialect = connection.dialect
        self.execution_options = connection._execution_options
        self.cursor = self.create_cursor()
        return self

    @util.memoized_property
    def engine(self):
        return self.root_connection.engine

    @util.memoized_property
    def postfetch_cols(self):
        return self.compiled.postfetch

    @util.memoized_property
    def prefetch_cols(self):
        if self.isinsert:
            return self.compiled.insert_prefetch
        elif self.isupdate:
            return self.compiled.update_prefetch
        else:
            return ()

    @util.memoized_property
    def returning_cols(self):
        self.compiled.returning

    @util.memoized_property
    def no_parameters(self):
        return self.execution_options.get("no_parameters", False)

    @util.memoized_property
    def should_autocommit(self):
        autocommit = self.execution_options.get(
            "autocommit",
            not self.compiled
            and self.statement
            and expression.PARSE_AUTOCOMMIT
            or False,
        )

        if autocommit is expression.PARSE_AUTOCOMMIT:
            return self.should_autocommit_text(self.unicode_statement)
        else:
            return autocommit

    def _execute_scalar(self, stmt, type_):
        """Execute a string statement on the current cursor, returning a
        scalar result.

        Used to fire off sequences, default phrases, and "select lastrowid"
        types of statements individually or in the context of a parent INSERT
        or UPDATE statement.

        """

        conn = self.root_connection
        if (
            isinstance(stmt, util.text_type)
            and not self.dialect.supports_unicode_statements
        ):
            stmt = self.dialect._encoder(stmt)[0]

        if self.dialect.positional:
            default_params = self.dialect.execute_sequence_format()
        else:
            default_params = {}

        conn._cursor_execute(self.cursor, stmt, default_params, context=self)
        r = self.cursor.fetchone()[0]
        if type_ is not None:
            # apply type post processors to the result
            proc = type_._cached_result_processor(
                self.dialect, self.cursor.description[0][1]
            )
            if proc:
                return proc(r)
        return r

    @property
    def connection(self):
        return self.root_connection._branch()

    def should_autocommit_text(self, statement):
        return AUTOCOMMIT_REGEXP.match(statement)

    def _use_server_side_cursor(self):
        if not self.dialect.supports_server_side_cursors:
            return False

        if self.dialect.server_side_cursors:
            use_server_side = self.execution_options.get(
                "stream_results", True
            ) and (
                (
                    self.compiled
                    and isinstance(
                        self.compiled.statement, expression.Selectable
                    )
                    or (
                        (
                            not self.compiled
                            or isinstance(
                                self.compiled.statement, expression.TextClause
                            )
                        )
                        and self.statement
                        and SERVER_SIDE_CURSOR_RE.match(self.statement)
                    )
                )
            )
        else:
            use_server_side = self.execution_options.get(
                "stream_results", False
            )

        return use_server_side

    def create_cursor(self):
        if self._use_server_side_cursor():
            self._is_server_side = True
            return self.create_server_side_cursor()
        else:
            self._is_server_side = False
            return self._dbapi_connection.cursor()

    def create_server_side_cursor(self):
        raise NotImplementedError()

    def pre_exec(self):
        pass

    def post_exec(self):
        pass

    def get_result_processor(self, type_, colname, coltype):
        """Return a 'result processor' for a given type as present in
        cursor.description.

        This has a default implementation that dialects can override
        for context-sensitive result type handling.

        """
        return type_._cached_result_processor(self.dialect, coltype)

    def get_lastrowid(self):
        """return self.cursor.lastrowid, or equivalent, after an INSERT.

        This may involve calling special cursor functions,
        issuing a new SELECT on the cursor (or a new one),
        or returning a stored value that was
        calculated within post_exec().

        This function will only be called for dialects
        which support "implicit" primary key generation,
        keep preexecute_autoincrement_sequences set to False,
        and when no explicit id value was bound to the
        statement.

        The function is called once, directly after
        post_exec() and before the transaction is committed
        or ResultProxy is generated.   If the post_exec()
        method assigns a value to `self._lastrowid`, the
        value is used in place of calling get_lastrowid().

        Note that this method is *not* equivalent to the
        ``lastrowid`` method on ``ResultProxy``, which is a
        direct proxy to the DBAPI ``lastrowid`` accessor
        in all cases.

        """
        return self.cursor.lastrowid

    def handle_dbapi_exception(self, e):
        pass

    def get_result_proxy(self):
        if self._is_server_side:
            return result.BufferedRowResultProxy(self)
        else:
            return result.ResultProxy(self)

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def supports_sane_rowcount(self):
        return self.dialect.supports_sane_rowcount

    def supports_sane_multi_rowcount(self):
        return self.dialect.supports_sane_multi_rowcount

    def _setup_crud_result_proxy(self):
        if self.isinsert and not self.executemany:
            if (
                not self._is_implicit_returning
                and not self.compiled.inline
                and self.dialect.postfetch_lastrowid
            ):

                self._setup_ins_pk_from_lastrowid()

            elif not self._is_implicit_returning:
                self._setup_ins_pk_from_empty()

        result = self.get_result_proxy()

        if self.isinsert:
            if self._is_implicit_returning:
                row = result.fetchone()
                self.returned_defaults = row
                self._setup_ins_pk_from_implicit_returning(row)
                result._soft_close()
                result._metadata = None
            elif not self._is_explicit_returning:
                result._soft_close()
                result._metadata = None
        elif self.isupdate and self._is_implicit_returning:
            row = result.fetchone()
            self.returned_defaults = row
            result._soft_close()
            result._metadata = None

        elif result._metadata is None:
            # no results, get rowcount
            # (which requires open cursor on some drivers
            # such as kintersbasdb, mxodbc)
            result.rowcount
            result._soft_close()
        return result

    def _setup_ins_pk_from_lastrowid(self):
        key_getter = self.compiled._key_getters_for_crud_column[2]
        table = self.compiled.statement.table
        compiled_params = self.compiled_parameters[0]

        lastrowid = self.get_lastrowid()
        if lastrowid is not None:
            autoinc_col = table._autoincrement_column
            if autoinc_col is not None:
                # apply type post processors to the lastrowid
                proc = autoinc_col.type._cached_result_processor(
                    self.dialect, None
                )
                if proc is not None:
                    lastrowid = proc(lastrowid)
            self.inserted_primary_key = [
                lastrowid
                if c is autoinc_col
                else compiled_params.get(key_getter(c), None)
                for c in table.primary_key
            ]
        else:
            # don't have a usable lastrowid, so
            # do the same as _setup_ins_pk_from_empty
            self.inserted_primary_key = [
                compiled_params.get(key_getter(c), None)
                for c in table.primary_key
            ]

    def _setup_ins_pk_from_empty(self):
        key_getter = self.compiled._key_getters_for_crud_column[2]
        table = self.compiled.statement.table
        compiled_params = self.compiled_parameters[0]
        self.inserted_primary_key = [
            compiled_params.get(key_getter(c), None) for c in table.primary_key
        ]

    def _setup_ins_pk_from_implicit_returning(self, row):
        if row is None:
            self.inserted_primary_key = None
            return

        key_getter = self.compiled._key_getters_for_crud_column[2]
        table = self.compiled.statement.table
        compiled_params = self.compiled_parameters[0]
        self.inserted_primary_key = [
            row[col] if value is None else value
            for col, value in [
                (col, compiled_params.get(key_getter(col), None))
                for col in table.primary_key
            ]
        ]

    def lastrow_has_defaults(self):
        return (self.isinsert or self.isupdate) and bool(
            self.compiled.postfetch
        )

    def set_input_sizes(
        self, translate=None, include_types=None, exclude_types=None
    ):
        """Given a cursor and ClauseParameters, call the appropriate
        style of ``setinputsizes()`` on the cursor, using DB-API types
        from the bind parameter's ``TypeEngine`` objects.

        This method only called by those dialects which require it,
        currently cx_oracle.

        """

        if not hasattr(self.compiled, "bind_names"):
            return

        inputsizes = {}
        for bindparam in self.compiled.bind_names:

            dialect_impl = bindparam.type._unwrapped_dialect_impl(self.dialect)
            dialect_impl_cls = type(dialect_impl)
            dbtype = dialect_impl.get_dbapi_type(self.dialect.dbapi)

            if (
                dbtype is not None
                and (
                    not exclude_types
                    or dbtype not in exclude_types
                    and dialect_impl_cls not in exclude_types
                )
                and (
                    not include_types
                    or dbtype in include_types
                    or dialect_impl_cls in include_types
                )
            ):
                inputsizes[bindparam] = dbtype
            else:
                inputsizes[bindparam] = None

        if self.dialect._has_events:
            self.dialect.dispatch.do_setinputsizes(
                inputsizes, self.cursor, self.statement, self.parameters, self
            )

        if self.dialect.positional:
            positional_inputsizes = []
            for key in self.compiled.positiontup:
                bindparam = self.compiled.binds[key]
                dbtype = inputsizes.get(bindparam, None)
                if dbtype is not None:
                    if key in self._expanded_parameters:
                        positional_inputsizes.extend(
                            [dbtype] * len(self._expanded_parameters[key])
                        )
                    else:
                        positional_inputsizes.append(dbtype)
            try:
                self.cursor.setinputsizes(*positional_inputsizes)
            except BaseException as e:
                self.root_connection._handle_dbapi_exception(
                    e, None, None, None, self
                )
        else:
            keyword_inputsizes = {}
            for bindparam, key in self.compiled.bind_names.items():
                dbtype = inputsizes.get(bindparam, None)
                if dbtype is not None:
                    if translate:
                        # TODO: this part won't work w/ the
                        # expanded_parameters feature, e.g. for cx_oracle
                        # quoted bound names
                        key = translate.get(key, key)
                    if not self.dialect.supports_unicode_binds:
                        key = self.dialect._encoder(key)[0]
                    if key in self._expanded_parameters:
                        keyword_inputsizes.update(
                            (expand_key, dbtype)
                            for expand_key in self._expanded_parameters[key]
                        )
                    else:
                        keyword_inputsizes[key] = dbtype
            try:
                self.cursor.setinputsizes(**keyword_inputsizes)
            except BaseException as e:
                self.root_connection._handle_dbapi_exception(
                    e, None, None, None, self
                )

    def _exec_default(self, column, default, type_):
        if default.is_sequence:
            return self.fire_sequence(default, type_)
        elif default.is_callable:
            self.current_column = column
            return default.arg(self)
        elif default.is_clause_element:
            # TODO: expensive branching here should be
            # pulled into _exec_scalar()
            conn = self.connection
            if not default._arg_is_typed:
                default_arg = expression.type_coerce(default.arg, type_)
            else:
                default_arg = default.arg
            c = expression.select([default_arg]).compile(bind=conn)
            return conn._execute_compiled(c, (), {}).scalar()
        else:
            return default.arg

    current_parameters = None
    """A dictionary of parameters applied to the current row.

    This attribute is only available in the context of a user-defined default
    generation function, e.g. as described at :ref:`context_default_functions`.
    It consists of a dictionary which includes entries for each column/value
    pair that is to be part of the INSERT or UPDATE statement. The keys of the
    dictionary will be the key value of each :class:`.Column`, which is usually
    synonymous with the name.

    Note that the :attr:`.DefaultExecutionContext.current_parameters` attribute
    does not accommodate for the "multi-values" feature of the
    :meth:`.Insert.values` method.  The
    :meth:`.DefaultExecutionContext.get_current_parameters` method should be
    preferred.

    .. seealso::

        :meth:`.DefaultExecutionContext.get_current_parameters`

        :ref:`context_default_functions`

    """

    def get_current_parameters(self, isolate_multiinsert_groups=True):
        """Return a dictionary of parameters applied to the current row.

        This method can only be used in the context of a user-defined default
        generation function, e.g. as described at
        :ref:`context_default_functions`. When invoked, a dictionary is
        returned which includes entries for each column/value pair that is part
        of the INSERT or UPDATE statement. The keys of the dictionary will be
        the key value of each :class:`.Column`, which is usually synonymous
        with the name.

        :param isolate_multiinsert_groups=True: indicates that multi-valued
         INSERT constructs created using :meth:`.Insert.values` should be
         handled by returning only the subset of parameters that are local
         to the current column default invocation.   When ``False``, the
         raw parameters of the statement are returned including the
         naming convention used in the case of multi-valued INSERT.

        .. versionadded:: 1.2  added
           :meth:`.DefaultExecutionContext.get_current_parameters`
           which provides more functionality over the existing
           :attr:`.DefaultExecutionContext.current_parameters`
           attribute.

        .. seealso::

            :attr:`.DefaultExecutionContext.current_parameters`

            :ref:`context_default_functions`

        """
        try:
            parameters = self.current_parameters
            column = self.current_column
        except AttributeError:
            raise exc.InvalidRequestError(
                "get_current_parameters() can only be invoked in the "
                "context of a Python side column default function"
            )
        if (
            isolate_multiinsert_groups
            and self.isinsert
            and self.compiled.statement._has_multi_parameters
        ):
            if column._is_multiparam_column:
                index = column.index + 1
                d = {column.original.key: parameters[column.key]}
            else:
                d = {column.key: parameters[column.key]}
                index = 0
            keys = self.compiled.statement.parameters[0].keys()
            d.update(
                (key, parameters["%s_m%d" % (key, index)]) for key in keys
            )
            return d
        else:
            return parameters

    def get_insert_default(self, column):
        if column.default is None:
            return None
        else:
            return self._exec_default(column, column.default, column.type)

    def get_update_default(self, column):
        if column.onupdate is None:
            return None
        else:
            return self._exec_default(column, column.onupdate, column.type)

    def _process_executemany_defaults(self):
        key_getter = self.compiled._key_getters_for_crud_column[2]

        scalar_defaults = {}

        insert_prefetch = self.compiled.insert_prefetch
        update_prefetch = self.compiled.update_prefetch

        # pre-determine scalar Python-side defaults
        # to avoid many calls of get_insert_default()/
        # get_update_default()
        for c in insert_prefetch:
            if c.default and c.default.is_scalar:
                scalar_defaults[c] = c.default.arg
        for c in update_prefetch:
            if c.onupdate and c.onupdate.is_scalar:
                scalar_defaults[c] = c.onupdate.arg

        for param in self.compiled_parameters:
            self.current_parameters = param
            for c in insert_prefetch:
                if c in scalar_defaults:
                    val = scalar_defaults[c]
                else:
                    val = self.get_insert_default(c)
                if val is not None:
                    param[key_getter(c)] = val
            for c in update_prefetch:
                if c in scalar_defaults:
                    val = scalar_defaults[c]
                else:
                    val = self.get_update_default(c)
                if val is not None:
                    param[key_getter(c)] = val

        del self.current_parameters

    def _process_executesingle_defaults(self):
        key_getter = self.compiled._key_getters_for_crud_column[2]
        self.current_parameters = (
            compiled_parameters
        ) = self.compiled_parameters[0]

        for c in self.compiled.insert_prefetch:
            if c.default and not c.default.is_sequence and c.default.is_scalar:
                val = c.default.arg
            else:
                val = self.get_insert_default(c)

            if val is not None:
                compiled_parameters[key_getter(c)] = val

        for c in self.compiled.update_prefetch:
            val = self.get_update_default(c)

            if val is not None:
                compiled_parameters[key_getter(c)] = val
        del self.current_parameters


DefaultDialect.execution_ctx_cls = DefaultExecutionContext
