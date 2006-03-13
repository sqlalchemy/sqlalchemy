# firebird.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


import sys, StringIO, string

import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
from sqlalchemy import *
import sqlalchemy.types as sqltypes

try:
    import kinterbasdb
except:
    kinterbasdb = None
        
class FBNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}
class FBInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"
class FBSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"
class FBDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "DATE"
class FBText(sqltypes.TEXT):
    def get_col_spec(self):
        return "BLOB SUB_TYPE 2"
class FBString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}
class FBChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}
class FBBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BLOB SUB_TYPE 1"
class FBBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "SMALLINT"
        
colspecs = {
    sqltypes.Integer : FBInteger,
    sqltypes.Smallinteger : FBSmallInteger,
    sqltypes.Numeric : FBNumeric,
    sqltypes.Float : FBNumeric,
    sqltypes.DateTime : FBDateTime,
    sqltypes.Date : FBDateTime,
    sqltypes.String : FBString,
    sqltypes.Binary : FBBinary,
    sqltypes.Boolean : FBBoolean,
    sqltypes.TEXT : FBText,
    sqltypes.CHAR: FBChar,
}

def engine(*args, **params):
    return FBSQLEngine(*args, **params)

def descriptor():
    return {'name':'firebird',
    'description':'Firebird',
    'arguments':[
        ('host', 'Host Server Name', None),
        ('database', 'Database Name', None),
        ('user', 'Username', None),
        ('password', 'Password', None)
    ]}
    
class FBSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, opts, use_ansi = True, module = None, **params):
        self._use_ansi = use_ansi
        self.opts = opts or {}
        if module is None:
            self.module = kinterbasdb
        else:
            self.module = module
        ansisql.ANSISQLEngine.__init__(self, **params)

    def do_commit(self, connection):
        connection.commit(True)

    def do_rollback(self, connection):
        connection.rollback(True)

    def dbapi(self):
        return self.module

    def connect_args(self):
        return [[], self.opts]
        
    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def compiler(self, statement, bindparams, **kwargs):
        return FBCompiler(statement, bindparams, engine=self, use_ansi=self._use_ansi, **kwargs)

    def schemagenerator(self, **params):
        return FBSchemaGenerator(self, **params)
    
    def schemadropper(self, **params):
        return FBSchemaDropper(self, **params)
 
    def defaultrunner(self, proxy):
        return FBDefaultRunner(self, proxy)
        
    def reflecttable(self, table):
        #TODO: map these better
        column_func = {
            14 : lambda r: String(r['FLEN']), # TEXT
            7  : lambda r: Integer(), # SHORT
            8  : lambda r: Integer(), # LONG
            9  : lambda r: Float(), # QUAD
            10 : lambda r: Float(), # FLOAT
            27 : lambda r: Double(), # DOUBLE
            35 : lambda r: DateTime(), # TIMESTAMP
            37 : lambda r: String(r['FLEN']), # VARYING
            261: lambda r: TEXT(), # BLOB
            40 : lambda r: Char(r['FLEN']), # CSTRING
            12 : lambda r: Date(), # DATE
            13 : lambda r: Time(), # TIME
            16 : lambda r: Numeric(precision=r['FPREC'], length=r['FSCALE'] * -1)  #INT64
            }
        tblqry = """\
        SELECT DISTINCT R.RDB$FIELD_NAME AS FNAME,
                  R.RDB$NULL_FLAG AS NULL_FLAG,
                  R.RDB$FIELD_POSITION,
                  F.RDB$FIELD_TYPE AS FTYPE,
                  F.RDB$FIELD_SUB_TYPE AS STYPE,
                  F.RDB$FIELD_LENGTH AS FLEN,
                  F.RDB$FIELD_PRECISION AS FPREC,
                  F.RDB$FIELD_SCALE AS FSCALE
        FROM RDB$RELATION_FIELDS R 
             JOIN RDB$FIELDS F ON R.RDB$FIELD_SOURCE=F.RDB$FIELD_NAME
        WHERE F.RDB$SYSTEM_FLAG=0 and R.RDB$RELATION_NAME=?
        ORDER BY R.RDB$FIELD_POSITION;"""
        keyqry = """
        SELECT RC.RDB$CONSTRAINT_TYPE KEYTYPE,
               RC.RDB$CONSTRAINT_NAME CNAME,
               RC.RDB$INDEX_NAME INAME,
               SE.RDB$FIELD_NAME SENAME,
        FROM RDB$RELATION_CONSTRAINTS RC
            LEFT JOIN RDB$INDEX_SEGMENTS SE
              ON RC.RDB$INDEX_NAME=SE.RDB$INDEX_NAME
        WHERE RC.RDB$RELATION_NAME=? AND SE.RDB$FIELD_NAME=?
        """
    
        #import pdb;pdb.set_trace()
        # get all of the fields for this table
        c = self.execute(tblqry, [table.name.upper()])
        while True:
            row = c.fetchone()
            if not row: break
            args = [row['FNAME']]
            kw = {}
            # get the data types and lengths
            args.append(column_func[row['FTYPE']](row))

            # is it a foreign key (and what is it linked to)

            # is it a primary key?

            table.append_item(Column(*args, **kw))
            # does the field have indexes

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def pre_exec(self, proxy, compiled, parameters, **kwargs):
        pass
        
    def _executemany(self, c, statement, parameters):
        rowcount = 0
        for param in parameters:
            c.execute(statement, param)
            rowcount += c.rowcount
        self.context.rowcount = rowcount

class FBCompiler(ansisql.ANSICompiler):
    """firebird compiler modifies the lexical structure of Select statements to work under 
    non-ANSI configured Firebird databases, if the use_ansi flag is False."""
    
    def __init__(self, engine, statement, parameters, use_ansi = True, **kwargs):
        self._outertable = None
        self._use_ansi = use_ansi
        ansisql.ANSICompiler.__init__(self, engine, statement, parameters, **kwargs)
      
    def visit_column(self, column):
        if self._use_ansi:
            return ansisql.ANSICompiler.visit_column(self, column)
            
        if column.table is self._outertable:
            self.strings[column] = "%s.%s(+)" % (column.table.name, column.name)
        else:
            self.strings[column] = "%s.%s" % (column.table.name, column.name)
       
    def visit_function(self, func):
        if len(func.clauses):
            super(FBCompiler, self).visit_function(func)
        else:
            self.strings[func] = func.name
 
    def visit_insert(self, insert):
        """inserts are required to have the primary keys be explicitly present.
         mapper will by default not put them in the insert statement to comply
         with autoincrement fields that require they not be present.  so, 
         put them all in for all primary key columns."""
        for c in insert.table.primary_key:
            if not self.parameters.has_key(c.key):
                self.parameters[c.key] = None
        return ansisql.ANSICompiler.visit_insert(self, insert)
           
    def visit_select_precolumns(self, select):
        """ called when building a SELECT statment, position is just before column list 
        Firebird puts the limit and offset right after the select...thanks for adding the
        visit_select_precolumns!!!"""
        if select.offset:
            result +=" FIRST " + select.offset
        if select.limit:
            result += " SKIP " + select.limit
        if select.distinct:
            result += " DISTINCT "
        return result

    def limit_clause(self, select):
        """Already taken care of in the visit_select_precolumns method."""
        return ""

class FBSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, override_pk=False, **kwargs):
        colspec = column.name 
        colspec += " " + column.type.get_col_spec()
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        if column.primary_key and not override_pk:
            colspec += " PRIMARY KEY"
        if column.foreign_key:
            colspec += " REFERENCES %s(%s)" % (column.foreign_key.column.table.name, column.foreign_key.column.name) 
        return colspec

    def visit_sequence(self, sequence):
        self.append("CREATE GENERATOR %s" % sequence.name)
        self.execute()

class FBSchemaDropper(ansisql.ANSISchemaDropper):
    def visit_sequence(self, sequence):
        self.append("DROP GENERATOR %s" % sequence.name)
        self.execute()

class FBDefaultRunner(ansisql.ANSIDefaultRunner):
    def exec_default_sql(self, default):
        c = sql.select([default.arg], from_obj=["rdb$database"], engine=self.engine).compile()
        return self.proxy(str(c), c.get_params()).fetchone()[0]
    
    def visit_sequence(self, seq):
        return self.proxy("SELECT gen_id(" + seq.name + ", 1) FROM rdb$database").fetchone()[0]


