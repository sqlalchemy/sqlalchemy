# engine/default.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Default implementations of per-dialect sqlalchemy.engine classes."""


import re, random
from sqlalchemy import util
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
    supports_sane_multi_rowcount = True
    preexecute_sequences = False

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
            if not parameters:
                self.compiled_parameters = [compiled.construct_params()]
                self.executemany = False
            else:
                self.compiled_parameters = [compiled.construct_params(m) for m in parameters]
                self.executemany = len(parameters) > 1

        elif statement is not None:
            self.typemap = self.column_labels = None
            self.parameters = self.__encode_param_keys(parameters)
            self.executemany = len(parameters) > 1
            self.statement = statement
            self.isinsert = self.isupdate = False
        else:
            self.statement = None
            self.isinsert = self.isupdate = self.executemany = False
            
        if self.statement is not None and not dialect.supports_unicode_statements:
            self.statement = self.statement.encode(self.dialect.encoding)
            
        self.cursor = self.create_cursor()
    
    connection = property(lambda s:s._connection._branch())
    
    root_connection = property(lambda s:s._connection)
    
    def __encode_param_keys(self, params):
        """apply string encoding to the keys of dictionary-based bind parameters.
        
        This is only used executing textual, non-compiled SQL expressions."""
        
        if self.dialect.positional or self.dialect.supports_unicode_statements:
            if params:
                return params
            elif self.dialect.positional:
                return [()]
            else:
                return [{}]
        else:
            def proc(d):
                # sigh, sometimes we get positional arguments with a dialect
                # that doesnt specify positional (because of execute_text())
                if not isinstance(d, dict):
                    return d
                return dict([(k.encode(self.dialect.encoding), d[k]) for k in d])
            return [proc(d) for d in params] or [{}]

    def __convert_compiled_params(self, parameters):
        processors = parameters[0].get_processors()
        if self.dialect.positional:
            parameters = [p.get_raw_list(processors) for p in parameters]
        else:
            encode = not self.dialect.supports_unicode_statements
            parameters = [p.get_raw_dict(processors, encode_keys=encode) for p in parameters]
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

    def supports_sane_multi_rowcount(self):
        return self.dialect.supports_sane_multi_rowcount

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

        plist = self.compiled_parameters
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
                        inputsizes[key.encode(self.dialect.encoding)] = dbtype
            self.cursor.setinputsizes(**inputsizes)

    def _process_defaults(self):
        """generate default values for compiled insert/update statements,
        and generate last_inserted_ids() collection."""

        if self.isinsert or self.isupdate:
            if self.executemany:
                if len(self.compiled.prefetch):
                    drunner = self.dialect.defaultrunner(self)
                    params = self.compiled_parameters
                    for param in params:
                        self.compiled_parameters = param
                        for c in self.compiled.prefetch:
                            if self.isinsert:
                                val = drunner.get_column_default(c)
                            else:
                                val = drunner.get_column_onupdate(c)
                            if val is not None:
                                param.set_value(c.key, val)
                    self.compiled_parameters = params
                    
            else:
                compiled_parameters = self.compiled_parameters[0]
                drunner = self.dialect.defaultrunner(self)
                if self.isinsert:
                    self._last_inserted_ids = []
                for c in self.compiled.prefetch:
                    if self.isinsert:
                        val = drunner.get_column_default(c)
                    else:
                        val = drunner.get_column_onupdate(c)
                    if val is not None:
                        compiled_parameters.set_value(c.key, val)

                if self.isinsert:
                    processors = compiled_parameters.get_processors()
                    for c in self.compiled.statement.table.primary_key:
                        if c.key in compiled_parameters:
                            self._last_inserted_ids.append(compiled_parameters.get_processed(c.key, processors))
                        else:
                            self._last_inserted_ids.append(None)
                            
                self._postfetch_cols = self.compiled.postfetch
                if self.isinsert:
                    self._last_inserted_params = compiled_parameters
                else:
                    self._last_updated_params = compiled_parameters
