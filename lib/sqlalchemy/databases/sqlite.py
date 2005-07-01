import sys, StringIO, string, types

import sqlalchemy.sql as sql
import sqlalchemy.engine as engine
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
from sqlalchemy.ansisql import *

from pysqlite2 import dbapi2 as sqlite
        
colspecs = {        
    schema.INT : "INTEGER",
    schema.CHAR : "CHAR(%(length)s)",
    schema.VARCHAR : "VARCHAR(%(length)s)",
    schema.TEXT : "TEXT",
    schema.FLOAT : "NUMERIC(%(precision)s, %(length)s)",
    schema.DECIMAL : "NUMERIC(%(precision)s, %(length)s)",
    schema.TIMESTAMP : "TIMESTAMP",
    schema.DATETIME : "TIMESTAMP",
    schema.CLOB : "TEXT",
    schema.BLOB : "BLOB",
    schema.BOOLEAN : "BOOLEAN",
}

def engine(filename, **params):
    return SQLiteSQLEngine(filename, **params)
    
class SQLiteSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, filename, **params):
        self.filename = filename
        ansisql.ANSISQLEngine.__init__(self, **params)
    
    def connect_args(self):
        return ([self.filename], {})
        
    def dbapi(self):
        return sqlite
        
    def columnimpl(self, column):
        return SQLiteColumnImpl(column)

class SQLiteColumnImpl(sql.ColumnSelectable):
    def _get_specification(self):
        coltype = self.column.type
        if type(coltype) == types.ClassType:
            key = coltype
        else:
            key = coltype.__class__

        return self.name + " " + colspecs[key] % {'precision': getattr(coltype, 'precision', None), 'length' : getattr(coltype, 'length', None)}

    
    
