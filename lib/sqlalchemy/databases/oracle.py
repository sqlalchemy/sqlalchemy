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
class OracleSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"
class OracleDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "DATE"
# Note:
# Oracle DATE == DATETIME
# Oracle does not allow milliseconds in DATE
# Oracle does not support TIME columns

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
    sqltypes.Smallinteger : OracleSmallInteger,
    sqltypes.Numeric : OracleNumeric,
    sqltypes.Float : OracleNumeric,
    sqltypes.DateTime : OracleDateTime,
    sqltypes.Date : OracleDateTime,
    sqltypes.String : OracleString,
    sqltypes.Binary : OracleBinary,
    sqltypes.Boolean : OracleBoolean,
    sqltypes.TEXT : OracleText,
    sqltypes.CHAR: OracleChar,
}


ischema_names = {
    'VARCHAR2' : OracleString,
    'DATE' : OracleDateTime,
    'DATETIME' : OracleDateTime,
    'NUMBER' : OracleNumeric,
    'BLOB' : OracleBinary,
    'CLOB' : OracleText
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
        self.opts = self._translate_connect_args((None, 'dsn', 'user', 'password'), opts)
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

    def schemagenerator(self, **params):
        return OracleSchemaGenerator(self, **params)
    def schemadropper(self, **params):
        return OracleSchemaDropper(self, **params)
    def defaultrunner(self, proxy):
        return OracleDefaultRunner(self, proxy)
        
    def reflecttable(self, table):
        c = self.execute ("select COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT from USER_TAB_COLUMNS where TABLE_NAME = :table_name", {'table_name':table.name.upper()})
        
        while True:
            row = c.fetchone()
            if row is None:
                break

            #print "ROW:" , row
            (name, coltype, length, precision, scale, nullable, default) = (row[0], row[1], row[2], row[3], row[4], row[5]=='Y', row[6])

            # INTEGER if the scale is 0 and precision is null
            # NUMBER if the scale and precision are both null
            # NUMBER(9,2) if the precision is 9 and the scale is 2
            # NUMBER(3) if the precision is 3 and scale is 0
            #length is ignored except for CHAR and VARCHAR2
            if coltype=='NUMBER' :
                if precision is None and scale is None:
                    coltype = OracleNumeric
                elif precision is None and scale == 0  :
                    coltype = OracleInteger
                else :
                    coltype = OracleNumeric(precision, scale)
            elif coltype=='CHAR' or coltype=='VARCHAR2':
                coltype = ischema_names.get(coltype, OracleString)(length)
            else:
                coltype = ischema_names.get(coltype)
               
            colargs = []
            if default is not None:
                colargs.append(PassiveDefault(sql.text(default)))
            
            name = name.lower()
            
            table.append_item (schema.Column(name, coltype, nullable=nullable, *colargs))

   
        c = self.execute("""select UCC.CONSTRAINT_NAME, UCC.COLUMN_NAME, UC.CONSTRAINT_TYPE, UC.SEARCH_CONDITION, UC2.TABLE_NAME as REFERENCES_TABLE
from USER_CONS_COLUMNS UCC, USER_CONSTRAINTS UC, USER_CONSTRAINTS UC2
where UCC.CONSTRAINT_NAME = UC.CONSTRAINT_NAME
and UC.R_CONSTRAINT_NAME = UC2.CONSTRAINT_NAME(+)
and UCC.TABLE_NAME = :table_name
order by UCC.CONSTRAINT_NAME""",{'table_name' : table.name.upper()})
        while True:
            row = c.fetchone()
            if row is None:
                break
            #print "ROW:" , row                
            (cons_name, column_name, type, search, referred_table) = row
            if type=='P' : 
                table.c[column_name.lower()]._set_primary_key()
            elif type=='R':
                remotetable = Table(referred_table.lower(), table.engine, autoload = True)
                table.c[column_name.lower()].append_item(schema.ForeignKey(remotetable.primary_key[0]))

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

class OracleCompiler(ansisql.ANSICompiler):
    """oracle compiler modifies the lexical structure of Select statements to work under 
    non-ANSI configured Oracle databases, if the use_ansi flag is False."""
    
    def __init__(self, engine, statement, parameters, use_ansi = True, **kwargs):
        self._outertable = None
        self._use_ansi = use_ansi
        ansisql.ANSICompiler.__init__(self, statement, parameters, engine=engine, **kwargs)
        
    def default_from(self):
        """called when a SELECT statement has no froms, and no FROM clause is to be appended.  
        gives Oracle a chance to tack on a "FROM DUAL" to the string output. """
        return " FROM DUAL"

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
       
    def visit_function(self, func):
        if len(func.clauses):
            super(OracleCompiler, self).visit_function(func)
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

    def visit_select(self, select):
        """looks for LIMIT and OFFSET in a select statement, and if so tries to wrap it in a 
        subquery with row_number() criterion."""
        # TODO: put a real copy-container on Select and copy, or somehow make this
        # not modify the Select statement
        if getattr(select, '_oracle_visit', False):
            # cancel out the compiled order_by on the select
            if hasattr(select, "order_by_clause"):
                self.strings[select.order_by_clause] = ""
            ansisql.ANSICompiler.visit_select(self, select)
            return
        if select.limit is not None or select.offset is not None:
            select._oracle_visit = True
            if hasattr(select, "order_by_clause"):
                orderby = self.strings[select.order_by_clause]
            else:
                # TODO: try to get "oid_column" to be used here
                orderby = "%s.rowid ASC" % select.froms[0].id
            select.append_column(sql.ColumnClause("ROW_NUMBER() OVER (ORDER BY %s)" % orderby).label("ora_rn"))
            limitselect = sql.select([c for c in select.c if c.key!='ora_rn'])
            if select.offset is not None:
                limitselect.append_whereclause("ora_rn>%d" % select.offset)
                if select.limit is not None:
                    limitselect.append_whereclause("ora_rn<=%d" % (select.limit + select.offset))
            else:
                limitselect.append_whereclause("ora_rn<=%d" % select.limit)
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
