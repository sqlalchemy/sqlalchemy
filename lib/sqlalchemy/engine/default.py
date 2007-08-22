# engine/default.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Default implementations of per-dialect sqlalchemy.engine classes."""


from sqlalchemy import schema, exceptions, util
import re, random
from sqlalchemy.engine import base
from sqlalchemy.sql import compiler, expression


AUTOCOMMIT_REGEXP = re.compile(r'\s*(?:UPDATE|INSERT|CREATE|DELETE|DROP|ALTER)',
                               re.I | re.UNICODE)
SELECT_REGEXP = re.compile(r'\s*SELECT', re.I | re.UNICODE)


class DefaultDialect(base.Dialect):
    """Default implementation of Dialect"""

    schemagenerator = compiler.SchemaGenerator
    schemadropper = compiler.SchemaDropper
    statement_compiler = compiler.DefaultCompiler
    preparer = compiler.IdentifierPreparer
    defaultrunner = base.DefaultRunner
    supports_alter = True
    supports_unicode_statements = False
    max_identifier_length = 9999
    supports_sane_rowcount = True

    def __init__(self, convert_unicode=False, encoding='utf-8', default_paramstyle='named', paramstyle=None, dbapi=None, **kwargs):
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
            self.paramstyle = default_paramstyle
        self.positional = self.paramstyle in ('qmark', 'format', 'numeric')
        self.identifier_preparer = self.preparer(self)
    
    def dbapi_type_map(self):
        # most DB-APIs have problems with this (such as, psycocpg2 types 
        # are unhashable).  So far Oracle can return it.
        
        return {}
    
    def create_execution_context(self, connection, **kwargs):
        return DefaultExecutionContext(self, connection, **kwargs)

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
        do_commit_twophase().  Its format is unspecified."""
        
        return "_sa_%032x" % random.randint(0,2**128)
        
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
        
    def _get_ischema(self):
        if self._ischema is None:
            import sqlalchemy.databases.information_schema as ischema
            self._ischema = ischema.ISchema(self)
        return self._ischema
    ischema = property(_get_ischema, doc="""returns an ISchema object for this engine, which allows access to information_schema tables (if supported)""")


class DefaultExecutionContext(base.ExecutionContext):
    def __init__(self, dialect, connection, compiled=None, statement=None, parameters=None):
        self.dialect = dialect
        self._connection = connection
        self.compiled = compiled
        self._postfetch_cols = util.Set()
        self.engine = connection.engine
        
        if compiled is not None:
            self.typemap = compiled.typemap
            self.column_labels = compiled.column_labels
            self.statement = unicode(compiled)
            self.isinsert = compiled.isinsert
            self.isupdate = compiled.isupdate
            if parameters is None:
                self.compiled_parameters = compiled.construct_params()
                self.executemany = False
            elif not isinstance(parameters, (list, tuple)):
                self.compiled_parameters = compiled.construct_params(parameters)
                self.executemany = False
            else:
                self.compiled_parameters = [compiled.construct_params(m) for m in parameters]
                if len(self.compiled_parameters) == 1:
                    self.compiled_parameters = self.compiled_parameters[0]
                    self.executemany = False
                else:
                    self.executemany = True

        elif statement is not None:
            self.typemap = self.column_labels = None
            self.parameters = self.__encode_param_keys(parameters)
            self.statement = statement
            self.isinsert = self.isupdate = False
        else:
            self.statement = None
            self.isinsert = self.isupdate = False
            
        if self.statement is not None and not dialect.supports_unicode_statements:
            self.statement = self.statement.encode(self.dialect.encoding)
            
        self.cursor = self.create_cursor()
    
    connection = property(lambda s:s._connection._branch())
    
    root_connection = property(lambda s:s._connection)
    
    def __encode_param_keys(self, params):
        """apply string encoding to the keys of dictionary-based bind parameters"""
        if self.dialect.positional or self.dialect.supports_unicode_statements:
            return params
        else:
            def proc(d):
                # sigh, sometimes we get positional arguments with a dialect
                # that doesnt specify positional (because of execute_text())
                if not isinstance(d, dict):
                    return d
                return dict([(k.encode(self.dialect.encoding), d[k]) for k in d])
            if isinstance(params, list):
                return [proc(d) for d in params]
            else:
                return proc(params)

    def __convert_compiled_params(self, parameters):
        encode = not self.dialect.supports_unicode_statements
        # the bind params are a CompiledParams object.  but all the
        # DB-API's hate that object (or similar).  so convert it to a
        # clean dictionary/list/tuple of dictionary/tuple of list
        if parameters is not None:
            if self.executemany:
                processors = parameters[0].get_processors()
            else:
                processors = parameters.get_processors()

            if self.dialect.positional:
                if self.executemany:
                    parameters = [p.get_raw_list(processors) for p in parameters]
                else:
                    parameters = parameters.get_raw_list(processors)
            else:
                if self.executemany:
                    parameters = [p.get_raw_dict(processors, encode_keys=encode) for p in parameters]
                else:
                    parameters = parameters.get_raw_dict(processors, encode_keys=encode)
        return parameters
                
    def is_select(self):
        """return TRUE if the statement is expected to have result rows."""
        
        return SELECT_REGEXP.match(self.statement)

    def create_cursor(self):
        return self._connection.connection.cursor()

    def pre_execution(self):
        self.pre_exec()
    
    def post_execution(self):
        self.post_exec()
    
    def result(self):
        return self.get_result_proxy()

    def should_autocommit(self):
        return AUTOCOMMIT_REGEXP.match(self.statement)
            
    def pre_exec(self):
        self._process_defaults()
        self.parameters = self.__convert_compiled_params(self.compiled_parameters)

    def post_exec(self):
        pass

    def get_result_proxy(self):
        return base.ResultProxy(self)

    def get_rowcount(self):
        if hasattr(self, '_rowcount'):
            return self._rowcount
        else:
            return self.cursor.rowcount

    def supports_sane_rowcount(self):
        return self.dialect.supports_sane_rowcount

    def last_inserted_ids(self):
        return self._last_inserted_ids

    def last_inserted_params(self):
        return self._last_inserted_params

    def last_updated_params(self):
        return self._last_updated_params

    def lastrow_has_defaults(self):
        return len(self._postfetch_cols)

    def postfetch_cols(self):
        return self._postfetch_cols
        
    def set_input_sizes(self):
        """Given a cursor and ClauseParameters, call the appropriate
        style of ``setinputsizes()`` on the cursor, using DB-API types
        from the bind parameter's ``TypeEngine`` objects.
        """

        if isinstance(self.compiled_parameters, list):
            plist = self.compiled_parameters
        else:
            plist = [self.compiled_parameters]
        if self.dialect.positional:
            inputsizes = []
            for params in plist[0:1]:
                for key in params.positional:
                    typeengine = params.get_type(key)
                    dbtype = typeengine.dialect_impl(self.dialect).get_dbapi_type(self.dialect.dbapi)
                    if dbtype is not None:
                        inputsizes.append(dbtype)
            self.cursor.setinputsizes(*inputsizes)
        else:
            inputsizes = {}
            for params in plist[0:1]:
                for key in params.keys():
                    typeengine = params.get_type(key)
                    dbtype = typeengine.dialect_impl(self.dialect).get_dbapi_type(self.dialect.dbapi)
                    if dbtype is not None:
                        inputsizes[key] = dbtype
            self.cursor.setinputsizes(**inputsizes)

    def _process_defaults(self):
        """generate default values for compiled insert/update statements,
        and generate last_inserted_ids() collection."""

        if self.isinsert:
            drunner = self.dialect.defaultrunner(self)
            if self.executemany:
                # executemany doesn't populate last_inserted_ids()
                firstparam = self.compiled_parameters[0]
                processors = firstparam.get_processors()
                for c in self.compiled.statement.table.c:
                    if c.default is not None:
                        params = self.compiled_parameters
                        for param in params:
                            if not c.key in param or param.get_original(c.key) is None:
                                self.compiled_parameters = param
                                newid = drunner.get_column_default(c)
                                if newid is not None:
                                    param.set_value(c.key, newid)
                        self.compiled_parameters = params
            else:
                param = self.compiled_parameters
                processors = param.get_processors()
                last_inserted_ids = []
                for c in self.compiled.statement.table.c:
                    if c in self.compiled.inline_params:
                        self._postfetch_cols.add(c)
                        if c.primary_key:
                            last_inserted_ids.append(None)
                    elif not c.key in param or param.get_original(c.key) is None:
                        if isinstance(c.default, schema.PassiveDefault):
                            self._postfetch_cols.add(c)
                        newid = drunner.get_column_default(c)
                        if newid is not None:
                            param.set_value(c.key, newid)
                            if c.primary_key:
                                last_inserted_ids.append(param.get_processed(c.key, processors))
                        elif c.primary_key:
                            last_inserted_ids.append(None)
                    elif c.primary_key:
                        last_inserted_ids.append(param.get_processed(c.key, processors))
                self._last_inserted_ids = last_inserted_ids
                self._last_inserted_params = param


        elif self.isupdate:
            drunner = self.dialect.defaultrunner(self)
            if self.executemany:
                for c in self.compiled.statement.table.c:
                    if c.onupdate is not None:
                        params = self.compiled_parameters
                        for param in params:
                            if not c.key in param or param.get_original(c.key) is None:
                                self.compiled_parameters = param
                                value = drunner.get_column_onupdate(c)
                                if value is not None:
                                    param.set_value(c.key, value)
                        self.compiled_parameters = params
            else:
                param = self.compiled_parameters
                for c in self.compiled.statement.table.c:
                    if c in self.compiled.inline_params:
                        self._postfetch_cols.add(c)
                    elif c.onupdate is not None and (not c.key in param or param.get_original(c.key) is None):
                        value = drunner.get_column_onupdate(c)
                        if value is not None:
                            param.set_value(c.key, value)
                self._last_updated_params = param
