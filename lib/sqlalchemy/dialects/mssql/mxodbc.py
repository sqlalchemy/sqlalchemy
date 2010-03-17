import re
import sys

from sqlalchemy import types as sqltypes
from sqlalchemy.connectors.mxodbc import MxODBCConnector
from sqlalchemy.dialects.mssql.pyodbc import MSExecutionContext_pyodbc
from sqlalchemy.dialects.mssql.base import MSExecutionContext, MSDialect, MSSQLCompiler

# TODO: does Pyodbc on windows have the same limitations ?
# if so this compiler can be moved to a common "odbc.py" module
# here
# *or* - should we implement this for MS-SQL across the board 
# since its technically MS-SQL's behavior ?
# perhaps yes, with a dialect flag "strict_binds" to turn it off
class MSSQLCompiler_mxodbc(MSSQLCompiler):
    binds_in_columns_clause = False
    
    def visit_in_op(self, binary, **kw):
        kw['literal_binds'] = True
        return "%s IN %s" % (
                                self.process(binary.left, **kw), 
                                self.process(binary.right, **kw)
            )

    def visit_notin_op(self, binary, **kw):
        kw['literal_binds'] = True
        return "%s NOT IN %s" % (
                                self.process(binary.left, **kw), 
                                self.process(binary.right, **kw)
            )
        
    def visit_function(self, func, **kw):
        kw['literal_binds'] = True
        return super(MSSQLCompiler_mxodbc, self).visit_function(func, **kw)
    
    def render_literal_value(self, value):
        # TODO! use mxODBC's literal quoting services here
        if isinstance(value, basestring):
            value = value.replace("'", "''")
            return "'%s'" % value
        else:
            return repr(value)
        
        
class MSExecutionContext_mxodbc(MSExecutionContext_pyodbc):
    """
    The pyodbc execution context is useful for enabling
    SELECT SCOPE_IDENTITY in cases where OUTPUT clause
    does not work (tables with insert triggers).
    """
    #todo - investigate whether the pyodbc execution context
    #       is really only being used in cases where OUTPUT
    #       won't work.

class MSDialect_mxodbc(MxODBCConnector, MSDialect):

    execution_ctx_cls = MSExecutionContext_mxodbc
    
    # TODO: may want to use this only if FreeTDS is not in use,
    # since FreeTDS doesn't seem to use native binds.
    statement_compiler = MSSQLCompiler_mxodbc
    
    def __init__(self, description_encoding='latin-1', **params):
        super(MSDialect_mxodbc, self).__init__(**params)
        self.description_encoding = description_encoding

dialect = MSDialect_mxodbc

