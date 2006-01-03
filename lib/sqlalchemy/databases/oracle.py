# oracle.py
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
    import cx_Oracle
except:
    cx_Oracle = None
        
class OracleNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}
class OracleInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"
class OracleDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "DATE"
class OracleText(sqltypes.TEXT):
    def get_col_spec(self):
        return "CLOB"
class OracleString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}
class OracleChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}
class OracleBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BLOB"
class OracleBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BOOLEAN"
        
colspecs = {
    sqltypes.Integer : OracleInteger,
    sqltypes.Numeric : OracleNumeric,
    sqltypes.DateTime : OracleDateTime,
    sqltypes.String : OracleString,
    sqltypes.Binary : OracleBinary,
    sqltypes.Boolean : OracleBoolean,
    sqltypes.TEXT : OracleText,
    sqltypes.CHAR: OracleChar,
}

def engine(*args, **params):
    return OracleSQLEngine(*args, **params)

def descriptor():
    return {'name':'oracle',
    'description':'Oracle',
    'arguments':[
        ('dsn', 'Data Source Name', None),
        ('user', 'Username', None),
        ('password', 'Password', None)
    ]}
    
class OracleSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, opts, use_ansi = True, module = None, **params):
        self._use_ansi = use_ansi
        self.opts = opts or {}
        if module is None:
            self.module = cx_Oracle
        else:
            self.module = module
        ansisql.ANSISQLEngine.__init__(self, **params)

    def dbapi(self):
        return self.module

    def connect_args(self):
        return [[], self.opts]
        
    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def oid_column_name(self):
        return "rowid"

    def compiler(self, statement, bindparams, **kwargs):
        return OracleCompiler(self, statement, bindparams, use_ansi=self._use_ansi, **kwargs)

    def schemagenerator(self, proxy, **params):
        return OracleSchemaGenerator(proxy, **params)
    def schemadropper(self, proxy, **params):
        return OracleSchemaDropper(proxy, **params)
    def defaultrunner(self, proxy):
        return OracleDefaultRunner(self, proxy)
        
    def reflecttable(self, table):
        raise "not implemented"

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def pre_exec(self, proxy, compiled, parameters, **kwargs):
        # this is just an assertion that all the primary key columns in an insert statement
        # have a value set up, or have a default generator ready to go
        if getattr(compiled, "isinsert", False):
            if isinstance(parameters, list):
                plist = parameters
            else:
                plist = [parameters]
            for param in plist:
                for primary_key in compiled.statement.table.primary_key:
                    if not param.has_key(primary_key.key) or param[primary_key.key] is None:
                        if primary_key.default is None:
                            raise "Column '%s.%s': Oracle primary key columns require a default value or a schema.Sequence to create ids" % (primary_key.table.name, primary_key.name)

    def _executemany(self, c, statement, parameters):
        rowcount = 0
        for param in parameters:
            c.execute(statement, param)
            rowcount += c.rowcount
        self.context.rowcount = rowcount

class OracleCompiler(ansisql.ANSICompiler):
    """oracle compiler modifies the lexical structure of Select statements to work under 
    non-ANSI configured Oracle databases, if the use_ansi flag is False."""
    
    def __init__(self, engine, statement, parameters, use_ansi = True, **kwargs):
        self._outertable = None
        self._use_ansi = use_ansi
        ansisql.ANSICompiler.__init__(self, engine, statement, parameters, **kwargs)
        
    def visit_join(self, join):
        if self._use_ansi:
            return ansisql.ANSICompiler.visit_join(self, join)
            
        self.froms[join] = self.get_from_text(join.left) + ", " + self.get_from_text(join.right)
        self.wheres[join] = join.onclause

        if join.isouter:
            # if outer join, push on the right side table as the current "outertable"
            outertable = self._outertable
            self._outertable = join.right

            # now re-visit the onclause, which will be used as a where clause
            # (the first visit occured via the Join object itself right before it called visit_join())
            join.onclause.accept_visitor(self)

            self._outertable = outertable
       
    def visit_alias(self, alias):
	"""oracle doesnt like 'FROM table AS alias'.  is the AS standard SQL??"""
        self.froms[alias] = self.get_from_text(alias.selectable) + " " + alias.name
        self.strings[alias] = self.get_str(alias.selectable)
 
    def visit_column(self, column):
        if self._use_ansi:
            return ansisql.ANSICompiler.visit_column(self, column)
            
        if column.table is self._outertable:
            self.strings[column] = "%s.%s(+)" % (column.table.name, column.name)
        else:
            self.strings[column] = "%s.%s" % (column.table.name, column.name)
        
    def visit_insert(self, insert):
        """inserts are required to have the primary keys be explicitly present.
         mapper will by default not put them in the insert statement to comply
         with autoincrement fields that require they not be present.  so, 
         put them all in for all primary key columns."""
        for c in insert.table.primary_key:
            if not self.parameters.has_key(c.key):
                self.parameters[c.key] = None
        return ansisql.ANSICompiler.visit_insert(self, insert)

    def visit_select(self, select):
        """looks for LIMIT and OFFSET in a select statement, and if so tries to wrap it in a 
        subquery with rownum criterion."""
        if getattr(select, '_oracle_visit', False):
            ansisql.ANSICompiler.visit_select(self, select)
            return
        if select.limit is not None or select.offset is not None:
            select._oracle_visit = True
            limitselect = select.select()
            if select.limit is not None:
                limitselect.append_whereclause("rownum<%d" % select.limit)
            if select.offset is not None:
                limitselect.append_whereclause("rownum>%d" % select.offset)
            limitselect.accept_visitor(self)
            self.strings[select] = self.strings[limitselect]
            self.froms[select] = self.froms[limitselect]
        else:
            ansisql.ANSICompiler.visit_select(self, select)
            
    def limit_clause(self, select):
        return ""

class OracleSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, override_pk=False, **kwargs):
        colspec = column.name
        colspec += " " + column.type.get_col_spec()

        if not column.nullable:
            colspec += " NOT NULL"
        if column.primary_key and not override_pk:
            colspec += " PRIMARY KEY"
        if column.foreign_key:
            colspec += " REFERENCES %s(%s)" % (column.column.foreign_key.column.table.name, column.column.foreign_key.column.name) 
        return colspec

    def visit_sequence(self, sequence):
        self.append("CREATE SEQUENCE %s" % sequence.name)
        self.execute()

class OracleSchemaDropper(ansisql.ANSISchemaDropper):
    def visit_sequence(self, sequence):
        self.append("DROP SEQUENCE %s" % sequence.name)
        self.execute()

class OracleDefaultRunner(ansisql.ANSIDefaultRunner):
    def exec_default_sql(self, default):
        c = sql.select([default.arg], from_obj=["DUAL"], engine=self.engine).compile()
        return self.proxy(str(c), c.get_params()).fetchone()[0]
    
    def visit_sequence(self, seq):
        return self.proxy("SELECT " + seq.name + ".nextval FROM DUAL").fetchone()[0]
