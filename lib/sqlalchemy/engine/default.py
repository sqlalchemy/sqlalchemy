# engine/default.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


from sqlalchemy import schema, exceptions, util, sql, types
import StringIO, sys, re
from sqlalchemy.engine import base

"""Provide default implementations of the engine interfaces"""

class PoolConnectionProvider(base.ConnectionProvider):
    def __init__(self, pool):
        self._pool = pool

    def get_connection(self):
        return self._pool.connect()

    def dispose(self):
        self._pool.dispose()
        if hasattr(self, '_dbproxy'):
            self._dbproxy.dispose()

class DefaultDialect(base.Dialect):
    """Default implementation of Dialect"""

    def __init__(self, convert_unicode=False, encoding='utf-8', default_paramstyle='named', **kwargs):
        self.convert_unicode = convert_unicode
        self.supports_autoclose_results = True
        self.encoding = encoding
        self.positional = False
        self._ischema = None
        self._figure_paramstyle(default=default_paramstyle)

    def create_execution_context(self):
        return DefaultExecutionContext(self)

    def type_descriptor(self, typeobj):
        """Provide a database-specific ``TypeEngine`` object, given
        the generic object which comes from the types module.

        Subclasses will usually use the ``adapt_type()`` method in the
        types module to make this job easy."""

        if type(typeobj) is type:
            typeobj = typeobj()
        return typeobj

    def oid_column_name(self, column):
        return None

    def supports_sane_rowcount(self):
        return True

    def do_begin(self, connection):
        """Implementations might want to put logic here for turning
        autocommit on/off, etc.
        """

        pass

    def do_rollback(self, connection):
        """Implementations might want to put logic here for turning
        autocommit on/off, etc.
        """

        #print "ENGINE ROLLBACK ON ", connection.connection
        connection.rollback()

    def do_commit(self, connection):
        """Implementations might want to put logic here for turning
        autocommit on/off, etc.
        """

        #print "ENGINE COMMIT ON ", connection.connection
        connection.commit()

    def do_executemany(self, cursor, statement, parameters, **kwargs):
        cursor.executemany(statement, parameters)

    def do_execute(self, cursor, statement, parameters, **kwargs):
        cursor.execute(statement, parameters)

    def defaultrunner(self, engine, proxy):
        return base.DefaultRunner(engine, proxy)

    def create_cursor(self, connection):
        return connection.cursor()

    def create_result_proxy_args(self, connection, cursor):
        return dict(should_prefetch=False)

    def _set_paramstyle(self, style):
        self._paramstyle = style
        self._figure_paramstyle(style)

    paramstyle = property(lambda s:s._paramstyle, _set_paramstyle)

    def convert_compiled_params(self, parameters):
        executemany = parameters is not None and isinstance(parameters, list)
        # the bind params are a CompiledParams object.  but all the DBAPI's hate
        # that object (or similar).  so convert it to a clean
        # dictionary/list/tuple of dictionary/tuple of list
        if parameters is not None:
           if self.positional:
                if executemany:
                    parameters = [p.get_raw_list() for p in parameters]
                else:
                    parameters = parameters.get_raw_list()
           else:
                if executemany:
                    parameters = [p.get_raw_dict() for p in parameters]
                else:
                    parameters = parameters.get_raw_dict()
        return parameters

    def _figure_paramstyle(self, paramstyle=None, default='named'):
        db = self.dbapi()
        if paramstyle is not None:
            self._paramstyle = paramstyle
        elif db is not None:
            self._paramstyle = db.paramstyle
        else:
            self._paramstyle = default

        if self._paramstyle == 'named':
            self.positional=False
        elif self._paramstyle == 'pyformat':
            self.positional=False
        elif self._paramstyle == 'qmark' or self._paramstyle == 'format' or self._paramstyle == 'numeric':
            # for positional, use pyformat internally, ANSICompiler will convert
            # to appropriate character upon compilation
            self.positional = True
        else:
            raise DBAPIError("Unsupported paramstyle '%s'" % self._paramstyle)

    def _get_ischema(self):
        # We use a property for ischema so that the accessor
        # creation only happens as needed, since otherwise we
        # have a circularity problem with the generic
        # ansisql.engine()
        if self._ischema is None:
            import sqlalchemy.databases.information_schema as ischema
            self._ischema = ischema.ISchema(self)
        return self._ischema
    ischema = property(_get_ischema, doc="""returns an ISchema object for this engine, which allows access to information_schema tables (if supported)""")

class DefaultExecutionContext(base.ExecutionContext):
    def __init__(self, dialect):
        self.dialect = dialect

    def pre_exec(self, engine, proxy, compiled, parameters):
        self._process_defaults(engine, proxy, compiled, parameters)

    def post_exec(self, engine, proxy, compiled, parameters):
        pass

    def get_rowcount(self, cursor):
        if hasattr(self, '_rowcount'):
            return self._rowcount
        else:
            return cursor.rowcount

    def supports_sane_rowcount(self):
        return self.dialect.supports_sane_rowcount()

    def last_inserted_ids(self):
        return self._last_inserted_ids

    def last_inserted_params(self):
        return self._last_inserted_params

    def last_updated_params(self):
        return self._last_updated_params

    def lastrow_has_defaults(self):
        return self._lastrow_has_defaults

    def set_input_sizes(self, cursor, parameters):
        """Given a cursor and ClauseParameters, call the appropriate
        style of ``setinputsizes()`` on the cursor, using DBAPI types
        from the bind parameter's ``TypeEngine`` objects.
        """

        if isinstance(parameters, list):
            plist = parameters
        else:
            plist = [parameters]
        if self.dialect.positional:
            inputsizes = []
            for params in plist[0:1]:
                for key in params.positional:
                    typeengine = params.binds[key].type
                    dbtype = typeengine.dialect_impl(self.dialect).get_dbapi_type(self.dialect.module)
                    if dbtype is not None:
                        inputsizes.append(dbtype)
            cursor.setinputsizes(*inputsizes)
        else:
            inputsizes = {}
            for params in plist[0:1]:
                for key in params.keys():
                    typeengine = params.binds[key].type
                    dbtype = typeengine.dialect_impl(self.dialect).get_dbapi_type(self.dialect.module)
                    if dbtype is not None:
                        inputsizes[key] = dbtype
            cursor.setinputsizes(**inputsizes)

    def _process_defaults(self, engine, proxy, compiled, parameters):
        """``INSERT`` and ``UPDATE`` statements, when compiled, may
        have additional columns added to their ``VALUES`` and ``SET``
        lists corresponding to column defaults/onupdates that are
        present on the ``Table`` object (i.e. ``ColumnDefault``,
        ``Sequence``, ``PassiveDefault``).  This method pre-execs
        those ``DefaultGenerator`` objects that require pre-execution
        and sets their values within the parameter list, and flags the
        thread-local state about ``PassiveDefault`` objects that may
        require post-fetching the row after it is inserted/updated.

        This method relies upon logic within the ``ANSISQLCompiler``
        in its `visit_insert` and `visit_update` methods that add the
        appropriate column clauses to the statement when its being
        compiled, so that these parameters can be bound to the
        statement.
        """

        if compiled is None: return

        if getattr(compiled, "isinsert", False):
            if isinstance(parameters, list):
                plist = parameters
            else:
                plist = [parameters]
            drunner = self.dialect.defaultrunner(engine, proxy)
            self._lastrow_has_defaults = False
            for param in plist:
                last_inserted_ids = []
                need_lastrowid=False
                # check the "default" status of each column in the table
                for c in compiled.statement.table.c:
                    # check if it will be populated by a SQL clause - we'll need that
                    # after execution.
                    if c in compiled.inline_params:
                        self._lastrow_has_defaults = True
                        if c.primary_key:
                            need_lastrowid = True
                    # check if its not present at all.  see if theres a default
                    # and fire it off, and add to bind parameters.  if
                    # its a pk, add the value to our last_inserted_ids list,
                    # or, if its a SQL-side default, dont do any of that, but we'll need
                    # the SQL-generated value after execution.
                    elif not param.has_key(c.key) or param[c.key] is None:
                        if isinstance(c.default, schema.PassiveDefault):
                            self._lastrow_has_defaults = True
                        newid = drunner.get_column_default(c)
                        if newid is not None:
                            param[c.key] = newid
                            if c.primary_key:
                                last_inserted_ids.append(param[c.key])
                        elif c.primary_key:
                            need_lastrowid = True
                    # its an explicitly passed pk value - add it to
                    # our last_inserted_ids list.
                    elif c.primary_key:
                        last_inserted_ids.append(param[c.key])
                if need_lastrowid:
                    self._last_inserted_ids = None
                else:
                    self._last_inserted_ids = last_inserted_ids
                self._last_inserted_params = param
        elif getattr(compiled, 'isupdate', False):
            if isinstance(parameters, list):
                plist = parameters
            else:
                plist = [parameters]
            drunner = self.dialect.defaultrunner(engine, proxy)
            self._lastrow_has_defaults = False
            for param in plist:
                # check the "onupdate" status of each column in the table
                for c in compiled.statement.table.c:
                    # it will be populated by a SQL clause - we'll need that
                    # after execution.
                    if c in compiled.inline_params:
                        pass
                    # its not in the bind parameters, and theres an "onupdate" defined for the column;
                    # execute it and add to bind params
                    elif c.onupdate is not None and (not param.has_key(c.key) or param[c.key] is None):
                        value = drunner.get_column_onupdate(c)
                        if value is not None:
                            param[c.key] = value
                self._last_updated_params = param
