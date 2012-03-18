# mssql/mxodbc.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Support for MS-SQL via mxODBC.

mxODBC is available at:

    http://www.egenix.com/

This was tested with mxODBC 3.1.2 and the SQL Server Native
Client connected to MSSQL 2005 and 2008 Express Editions.

Connecting
~~~~~~~~~~

Connection is via DSN::

    mssql+mxodbc://<username>:<password>@<dsnname>

Execution Modes
~~~~~~~~~~~~~~~

mxODBC features two styles of statement execution, using the
``cursor.execute()`` and ``cursor.executedirect()`` methods (the second being
an extension to the DBAPI specification). The former makes use of a particular
API call specific to the SQL Server Native Client ODBC driver known
SQLDescribeParam, while the latter does not.

mxODBC apparently only makes repeated use of a single prepared statement
when SQLDescribeParam is used. The advantage to prepared statement reuse is
one of performance. The disadvantage is that SQLDescribeParam has a limited
set of scenarios in which bind parameters are understood, including that they
cannot be placed within the argument lists of function calls, anywhere outside
the FROM, or even within subqueries within the FROM clause - making the usage
of bind parameters within SELECT statements impossible for all but the most
simplistic statements.

For this reason, the mxODBC dialect uses the "native" mode by default only for
INSERT, UPDATE, and DELETE statements, and uses the escaped string mode for
all other statements. 

This behavior can be controlled via
:meth:`~sqlalchemy.sql.expression.Executable.execution_options` using the
``native_odbc_execute`` flag with a value of ``True`` or ``False``, where a
value of ``True`` will unconditionally use native bind parameters and a value
of ``False`` will unconditionally use string-escaped parameters.

"""


from sqlalchemy import types as sqltypes
from sqlalchemy.connectors.mxodbc import MxODBCConnector
from sqlalchemy.dialects.mssql.pyodbc import MSExecutionContext_pyodbc
from sqlalchemy.dialects.mssql.base import (MSDialect, 
                                            MSSQLStrictCompiler,
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

