"""
MSSQL dialect tweaked to work with mxODBC, mainly by making use
of the MSSQLStrictCompiler.

This was tested with mxODBC 3.1.2 and the SQL Server Native
Client connected to MSSQL 2005 and 2008 Express Editions.
"""

import re
import sys

from sqlalchemy import types as sqltypes
from sqlalchemy import util
from sqlalchemy.connectors.mxodbc import MxODBCConnector
from sqlalchemy.dialects.mssql.pyodbc import MSExecutionContext_pyodbc
from sqlalchemy.dialects.mssql.base import (MSExecutionContext, MSDialect, 
                                            MSSQLCompiler, MSSQLStrictCompiler,
                                            _MSDateTime, _MSDate, TIME)



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
    
    # TODO: may want to use this only if FreeTDS is not in use,
    # since FreeTDS doesn't seem to use native binds.
    statement_compiler = MSSQLStrictCompiler
    execution_ctx_cls = MSExecutionContext_mxodbc
    colspecs = {
        #sqltypes.Numeric : _MSNumeric,
        sqltypes.DateTime : _MSDateTime,
        sqltypes.Date : _MSDate,
        sqltypes.Time : TIME,
    }


    def __init__(self, description_encoding='latin-1', **params):
        super(MSDialect_mxodbc, self).__init__(**params)
        self.description_encoding = description_encoding

dialect = MSDialect_mxodbc

