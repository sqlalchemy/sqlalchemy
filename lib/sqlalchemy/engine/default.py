# engine/default.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Default implementations of per-dialect sqlalchemy.engine classes.

These are semi-private implementation classes which are only of importance
to database dialect authors; dialects will usually use the classes here
as the base class for their own corresponding classes.

"""

import re, random
from sqlalchemy.engine import base, reflection
from sqlalchemy.sql import compiler, expression
from sqlalchemy import exc, types as sqltypes, util

AUTOCOMMIT_REGEXP = re.compile(
            r'\s*(?:UPDATE|INSERT|CREATE|DELETE|DROP|ALTER)',
            re.I | re.UNICODE)


class DefaultDialect(base.Dialect):
    """Default implementation of Dialect"""

    statement_compiler = compiler.SQLCompiler
    ddl_compiler = compiler.DDLCompiler
    type_compiler = compiler.GenericTypeCompiler
    preparer = compiler.IdentifierPreparer
    supports_alter = True

    # most DBAPIs happy with this for execute().
    # not cx_oracle.
    execute_sequence_format = tuple

    supports_sequences = False
    sequences_optional = False
    preexecute_autoincrement_sequences = False
    postfetch_lastrowid = True
    implicit_returning = False

    supports_native_enum = False
    supports_native_boolean = False

    # if the NUMERIC type
    # returns decimal.Decimal.
    # *not* the FLOAT type however.
    supports_native_decimal = False

    # Py3K
    #supports_unicode_statements = True
    #supports_unicode_binds = True
    # Py2K
    supports_unicode_statements = False
    supports_unicode_binds = False
    returns_unicode_strings = False
    # end Py2K

    name = 'default'

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
    dbapi_type_map = {}
    colspecs = {}
    default_paramstyle = 'named'
    supports_default_values = False
    supports_empty_insert = True

    server_version_info = None

    # indicates symbol names are 
    # UPPERCASEd if they are case insensitive
    # within the database.
    # if this is True, the methods normalize_name()
    # and denormalize_name() must be provided.
    requires_name_normalize = False

    reflection_options = ()

    def __init__(self, convert_unicode=False, assert_unicode=False,
                 encoding='utf-8', paramstyle=None, dbapi=None,
                 implicit_returning=None,
                 label_length=None, **kwargs):

        if not getattr(self, 'ported_sqla_06', True):
            util.warn(
                "The %s dialect is not yet ported to SQLAlchemy 0.6" %
                self.name)

        self.convert_unicode = convert_unicode
        if assert_unicode:
            util.warn_deprecated(
                "assert_unicode is deprecated. "
                "SQLAlchemy emits a warning in all cases where it "
                "would otherwise like to encode a Python unicode object "
                "into a specific encoding but a plain bytestring is "
                "received. "
                "This does *not* apply to DBAPIs that coerce Unicode "
                "natively.")

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
        self.positional = self.paramstyle in ('qmark', 'format', 'numeric')
        self.identifier_preparer = self.preparer(self)
        self.type_compiler = self.type_compiler(self)

        if label_length and label_length > self.max_identifier_length:
            raise exc.ArgumentError(
                    "Label length of %d is greater than this dialect's"
                    " maximum identifier length of %d" %
                    (label_length, self.max_identifier_length))
        self.label_length = label_length

        if not hasattr(self, 'description_encoding'):
            self.description_encoding = getattr(
                                            self, 
                                            'description_encoding', 
                                            encoding)

    @property
    def dialect_description(self):
        return self.name + "+" + self.driver

    def initialize(self, connection):
        try:
            self.server_version_info = \
                            self._get_server_version_info(connection)
        except NotImplementedError:
            self.server_version_info = None
        try:
            self.default_schema_name = \
                            self._get_default_schema_name(connection)
        except NotImplementedError:
            self.default_schema_name = None

        self.returns_unicode_strings = self._check_unicode_returns(connection)

        self.do_rollback(connection.connection)
 
    def on_connect(self):
        """return a callable which sets up a newly created DBAPI connection.

        This is used to set dialect-wide per-connection options such as
        isolation modes, unicode modes, etc.

        If a callable is returned, it will be assembled into a pool listener
        that receives the direct DBAPI connection, with all wrappers removed.

        If None is returned, no listener will be generated.

        """
        return None

    def _check_unicode_returns(self, connection):
        # Py2K
        if self.supports_unicode_statements:
            cast_to = unicode
        else:
            cast_to = str
        # end Py2K
        # Py3K
        #cast_to = str
        def check_unicode(type_):
            cursor = connection.connection.cursor()
            try:
                cursor.execute(
                    cast_to(
                        expression.select( 
                        [expression.cast(
                            expression.literal_column(
                                    "'test unicode returns'"), type_)
                        ]).compile(dialect=self)
                    )
                )
                row = cursor.fetchone()

                return isinstance(row[0], unicode)
            finally:
                cursor.close()

        # detect plain VARCHAR
        unicode_for_varchar = check_unicode(sqltypes.VARCHAR(60))

        # detect if there's an NVARCHAR type with different behavior available
        unicode_for_unicode = check_unicode(sqltypes.Unicode(60))

        if unicode_for_unicode and not unicode_for_varchar:
            return "conditional"
        else:
            return unicode_for_varchar

    def type_descriptor(self, typeobj):
        """Provide a database-specific ``TypeEngine`` object, given
        the generic object which comes from the types module.

        This method looks for a dictionary called
        ``colspecs`` as a class or instance-level variable,
        and passes on to ``types.adapt_type()``.

        """
        return sqltypes.adapt_type(typeobj, self.colspecs)

    def reflecttable(self, connection, table, include_columns):
        insp = reflection.Inspector.from_engine(connection)
        return insp.reflecttable(table, include_columns)

    def get_pk_constraint(self, conn, table_name, schema=None, **kw):
        """Compatiblity method, adapts the result of get_primary_keys()
        for those dialects which don't implement get_pk_constraint().

        """
        return {
            'constrained_columns':
                        self.get_primary_keys(conn, table_name, 
                                                schema=schema, **kw)
        }

    def validate_identifier(self, ident):
        if len(ident) > self.max_identifier_length:
            raise exc.IdentifierError(
                "Identifier '%s' exceeds maximum length of %d characters" % 
                (ident, self.max_identifier_length)
            )

    def connect(self, *cargs, **cparams):
        return self.dbapi.connect(*cargs, **cparams)

    def create_connect_args(self, url):
        opts = url.translate_connect_args()
        opts.update(url.query)
        return [[], opts]

    def do_begin(self, connection):
        """Implementations might want to put logic here for turning
        autocommit on/off, etc.
        """

        pass

    def do_rollback(self, connection):
        """Implementations might want to put logic here for turning
        autocommit on/off, etc.
        """

        connection.rollback()

    def do_commit(self, connection):
        """Implementations might want to put logic here for turning
        autocommit on/off, etc.
        """

        connection.commit()

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

    def is_disconnect(self, e):
        return False


class DefaultExecutionContext(base.ExecutionContext):
    execution_options = util.frozendict()
    isinsert = False
    isupdate = False
    isdelete = False
    isddl = False
    executemany = False
    result_map = None
    compiled = None
    statement = None

    def __init__(self, 
                    dialect, 
                    connection, 
                    compiled_sql=None, 
                    compiled_ddl=None, 
                    statement=None, 
                    parameters=None):

        self.dialect = dialect
        self._connection = self.root_connection = connection
        self.engine = connection.engine

        if compiled_ddl is not None:
            self.compiled = compiled = compiled_ddl
            self.isddl = True

            if compiled.statement._execution_options:
                self.execution_options = compiled.statement._execution_options
            if connection._execution_options:
                self.execution_options = self.execution_options.union(
                                                    connection._execution_options
                                                    )

            if not dialect.supports_unicode_statements:
                self.unicode_statement = unicode(compiled)
                self.statement = self.unicode_statement.encode(self.dialect.encoding)
            else:
                self.statement = self.unicode_statement = unicode(compiled)

            self.cursor = self.create_cursor()
            self.compiled_parameters = []
            self.parameters = [self._default_params]

        elif compiled_sql is not None:
            self.compiled = compiled = compiled_sql

            if not compiled.can_execute:
                raise exc.ArgumentError("Not an executable clause: %s" % compiled)

            if compiled.statement._execution_options:
                self.execution_options = compiled.statement._execution_options
            if connection._execution_options:
                self.execution_options = self.execution_options.union(
                                                        connection._execution_options
                                                        )

            # compiled clauseelement.  process bind params, process table defaults,
            # track collections used by ResultProxy to target and process results

            self.processors = dict(
                (key, value) for key, value in
                ( (compiled.bind_names[bindparam],
                   bindparam.bind_processor(self.dialect))
                  for bindparam in compiled.bind_names )
                if value is not None)

            self.result_map = compiled.result_map

            if not dialect.supports_unicode_statements:
                self.unicode_statement = unicode(compiled)
                self.statement = self.unicode_statement.encode(self.dialect.encoding)
            else:
                self.statement = self.unicode_statement = unicode(compiled)

            self.isinsert = compiled.isinsert
            self.isupdate = compiled.isupdate
            self.isdelete = compiled.isdelete

            if not parameters:
                self.compiled_parameters = [compiled.construct_params()]
            else:
                self.compiled_parameters = [compiled.construct_params(m, _group_number=grp) for
                                            grp,m in enumerate(parameters)]

                self.executemany = len(parameters) > 1

            self.cursor = self.create_cursor()
            if self.isinsert or self.isupdate:
                self.__process_defaults()
            self.parameters = self.__convert_compiled_params(self.compiled_parameters)

        elif statement is not None:
            # plain text statement
            if connection._execution_options:
                self.execution_options = self.execution_options.union(connection._execution_options)
            self.parameters = self.__encode_param_keys(parameters)
            self.executemany = len(parameters) > 1

            if isinstance(statement, unicode) and not dialect.supports_unicode_statements:
                self.unicode_statement = statement
                self.statement = statement.encode(self.dialect.encoding)
            else:
                self.statement = self.unicode_statement = statement

            self.cursor = self.create_cursor()
        else:
            # no statement. used for standalone ColumnDefault execution.
            if connection._execution_options:
                self.execution_options = self.execution_options.union(connection._execution_options)
            self.cursor = self.create_cursor()

    @util.memoized_property
    def is_crud(self):
        return self.isinsert or self.isupdate or self.isdelete

    @util.memoized_property
    def should_autocommit(self):
        autocommit = self.execution_options.get('autocommit', 
                                                not self.compiled and 
                                                self.statement and
                                                expression.PARSE_AUTOCOMMIT 
                                                or False)

        if autocommit is expression.PARSE_AUTOCOMMIT:
            return self.should_autocommit_text(self.unicode_statement)
        else:
            return autocommit

    @util.memoized_property
    def _is_explicit_returning(self):
        return self.compiled and \
            getattr(self.compiled.statement, '_returning', False)

    @util.memoized_property
    def _is_implicit_returning(self):
        return self.compiled and \
            bool(self.compiled.returning) and \
            not self.compiled.statement._returning

    @util.memoized_property
    def _default_params(self):
        if self.dialect.positional:
            return self.dialect.execute_sequence_format()
        else:
            return {}

    def _execute_scalar(self, stmt):
        """Execute a string statement on the current cursor, returning a
        scalar result.

        Used to fire off sequences, default phrases, and "select lastrowid"
        types of statements individually or in the context of a parent INSERT
        or UPDATE statement.

        """

        conn = self._connection
        if isinstance(stmt, unicode) and not self.dialect.supports_unicode_statements:
            stmt = stmt.encode(self.dialect.encoding)
        conn._cursor_execute(self.cursor, stmt, self._default_params)
        return self.cursor.fetchone()[0]

    @property
    def connection(self):
        return self._connection._branch()

    def __encode_param_keys(self, params):
        """Apply string encoding to the keys of dictionary-based bind parameters.

        This is only used executing textual, non-compiled SQL expressions.

        """

        if not params:
            return [self._default_params]
        elif isinstance(params[0], self.dialect.execute_sequence_format):
            return params
        elif isinstance(params[0], dict):
            if self.dialect.supports_unicode_statements:
                return params
            else:
                def proc(d):
                    return dict((k.encode(self.dialect.encoding), d[k]) for k in d)
                return [proc(d) for d in params] or [{}]
        else:
            return [self.dialect.execute_sequence_format(p) for p in params]


    def __convert_compiled_params(self, compiled_parameters):
        """Convert the dictionary of bind parameter values into a dict or list
        to be sent to the DBAPI's execute() or executemany() method.
        """

        processors = self.processors
        parameters = []
        if self.dialect.positional:
            for compiled_params in compiled_parameters:
                param = []
                for key in self.compiled.positiontup:
                    if key in processors:
                        param.append(processors[key](compiled_params[key]))
                    else:
                        param.append(compiled_params[key])
                parameters.append(self.dialect.execute_sequence_format(param))
        else:
            encode = not self.dialect.supports_unicode_statements
            for compiled_params in compiled_parameters:
                param = {}
                if encode:
                    encoding = self.dialect.encoding
                    for key in compiled_params:
                        if key in processors:
                            param[key.encode(encoding)] = processors[key](compiled_params[key])
                        else:
                            param[key.encode(encoding)] = compiled_params[key]
                else:
                    for key in compiled_params:
                        if key in processors:
                            param[key] = processors[key](compiled_params[key])
                        else:
                            param[key] = compiled_params[key]
                parameters.append(param)
        return self.dialect.execute_sequence_format(parameters)

    def should_autocommit_text(self, statement):
        return AUTOCOMMIT_REGEXP.match(statement)

    def create_cursor(self):
        return self._connection.connection.cursor()

    def pre_exec(self):
        pass

    def post_exec(self):
        pass

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
        return base.ResultProxy(self)

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def supports_sane_rowcount(self):
        return self.dialect.supports_sane_rowcount

    def supports_sane_multi_rowcount(self):
        return self.dialect.supports_sane_multi_rowcount

    def post_insert(self):
        if self.dialect.postfetch_lastrowid and \
            (not len(self._inserted_primary_key) or \
                        None in self._inserted_primary_key):

            table = self.compiled.statement.table
            lastrowid = self.get_lastrowid()
            self._inserted_primary_key = [c is table._autoincrement_column and lastrowid or v
                for c, v in zip(table.primary_key, self._inserted_primary_key)
            ]

    def _fetch_implicit_returning(self, resultproxy):
        table = self.compiled.statement.table
        row = resultproxy.fetchone()

        ipk = []
        for c, v in zip(table.primary_key, self._inserted_primary_key):
            if v is not None:
                ipk.append(v)
            else:
                ipk.append(row[c])

        self._inserted_primary_key = ipk

    def last_inserted_params(self):
        return self._last_inserted_params

    def last_updated_params(self):
        return self._last_updated_params

    def lastrow_has_defaults(self):
        return hasattr(self, 'postfetch_cols') and len(self.postfetch_cols)

    def set_input_sizes(self, translate=None, exclude_types=None):
        """Given a cursor and ClauseParameters, call the appropriate
        style of ``setinputsizes()`` on the cursor, using DB-API types
        from the bind parameter's ``TypeEngine`` objects.
        """

        if not hasattr(self.compiled, 'bind_names'):
            return

        types = dict(
                (self.compiled.bind_names[bindparam], bindparam.type)
                 for bindparam in self.compiled.bind_names)

        if self.dialect.positional:
            inputsizes = []
            for key in self.compiled.positiontup:
                typeengine = types[key]
                dbtype = typeengine.dialect_impl(self.dialect).get_dbapi_type(self.dialect.dbapi)
                if dbtype is not None and (not exclude_types or dbtype not in exclude_types):
                    inputsizes.append(dbtype)
            try:
                self.cursor.setinputsizes(*inputsizes)
            except Exception, e:
                self._connection._handle_dbapi_exception(e, None, None, None, self)
                raise
        else:
            inputsizes = {}
            for key in self.compiled.bind_names.values():
                typeengine = types[key]
                dbtype = typeengine.dialect_impl(self.dialect).get_dbapi_type(self.dialect.dbapi)
                if dbtype is not None and (not exclude_types or dbtype not in exclude_types):
                    if translate:
                        key = translate.get(key, key)
                    inputsizes[key.encode(self.dialect.encoding)] = dbtype
            try:
                self.cursor.setinputsizes(**inputsizes)
            except Exception, e:
                self._connection._handle_dbapi_exception(e, None, None, None, self)
                raise

    def _exec_default(self, default):
        if default.is_sequence:
            return self.fire_sequence(default)
        elif default.is_callable:
            return default.arg(self)
        elif default.is_clause_element:
            # TODO: expensive branching here should be 
            # pulled into _exec_scalar()
            conn = self.connection
            c = expression.select([default.arg]).compile(bind=conn)
            return conn._execute_compiled(c, (), {}).scalar()
        else:
            return default.arg

    def get_insert_default(self, column):
        if column.default is None:
            return None
        else:
            return self._exec_default(column.default)

    def get_update_default(self, column):
        if column.onupdate is None:
            return None
        else:
            return self._exec_default(column.onupdate)

    def __process_defaults(self):
        """Generate default values for compiled insert/update statements,
        and generate inserted_primary_key collection.
        """

        if self.executemany:
            if len(self.compiled.prefetch):
                scalar_defaults = {}

                # pre-determine scalar Python-side defaults
                # to avoid many calls of get_insert_default()/get_update_default()
                for c in self.compiled.prefetch:
                    if self.isinsert and c.default and c.default.is_scalar:
                        scalar_defaults[c] = c.default.arg
                    elif self.isupdate and c.onupdate and c.onupdate.is_scalar:
                        scalar_defaults[c] = c.onupdate.arg

                for param in self.compiled_parameters:
                    self.current_parameters = param
                    for c in self.compiled.prefetch:
                        if c in scalar_defaults:
                            val = scalar_defaults[c]
                        elif self.isinsert:
                            val = self.get_insert_default(c)
                        else:
                            val = self.get_update_default(c)
                        if val is not None:
                            param[c.key] = val
                del self.current_parameters

        else:
            self.current_parameters = compiled_parameters = self.compiled_parameters[0]

            for c in self.compiled.prefetch:
                if self.isinsert:
                    val = self.get_insert_default(c)
                else:
                    val = self.get_update_default(c)

                if val is not None:
                    compiled_parameters[c.key] = val
            del self.current_parameters

            if self.isinsert:
                self._inserted_primary_key = [compiled_parameters.get(c.key, None) 
                                            for c in self.compiled.statement.table.primary_key]
                self._last_inserted_params = compiled_parameters
            else:
                self._last_updated_params = compiled_parameters

            self.postfetch_cols = self.compiled.postfetch
            self.prefetch_cols = self.compiled.prefetch

DefaultDialect.execution_ctx_cls = DefaultExecutionContext
