# postgres.py
# Copyright (C) 2005 Michael Bayer mike_mp@zzzcomputing.com
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

import sys, StringIO, string, types, re

import sqlalchemy.sql as sql
import sqlalchemy.engine as engine
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
import sqlalchemy.types as sqltypes
from sqlalchemy.ansisql import *
try:
    import psycopg2 as psycopg
except:
    try:
        import psycopg
    except:
        psycopg = None
        
class PGNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}
class PGInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"
class PGDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP"
class PGText(sqltypes.TEXT):
    def get_col_spec(self):
        return "TEXT"
class PGString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}
class PGChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}
class PGBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BYTEA"
class PGBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BOOLEAN"
        
colspecs = {
    sqltypes.Integer : PGInteger,
    sqltypes.Numeric : PGNumeric,
    sqltypes.DateTime : PGDateTime,
    sqltypes.String : PGString,
    sqltypes.Binary : PGBinary,
    sqltypes.Boolean : PGBoolean,
    sqltypes.TEXT : PGText,
    sqltypes.CHAR: PGChar,
}

ischema_names = {
    'integer' : PGInteger,
    'character varying' : PGString,
    'character' : PGChar,
    'text' : PGText,
    'numeric' : PGNumeric,
    'timestamp without time zone' : PGDateTime,
    'bytea' : PGBinary,
}

generic_engine = ansisql.engine()
gen_columns = schema.Table("columns", generic_engine,
    Column("table_schema", String),
    Column("table_name", String),
    Column("column_name", String),
    Column("is_nullable", Integer),
    Column("data_type", String),
    Column("ordinal_position", Integer),
    Column("character_maximum_length", Integer),
    Column("numeric_precision", Integer),
    Column("numeric_precision_radix", Integer),
    schema="information_schema")
    
gen_constraints = schema.Table("table_constraints", generic_engine,
    Column("table_schema", String),
    Column("table_name", String),
    Column("constraint_name", String),
    Column("constraint_type", String),
    schema="information_schema")

gen_column_constraints = schema.Table("constraint_column_usage", generic_engine,
    Column("table_schema", String),
    Column("table_name", String),
    Column("column_name", String),
    Column("constraint_name", String),
    schema="information_schema")

def engine(opts, **params):
    return PGSQLEngine(opts, **params)

class PGSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, opts, module = None, **params):
        if module is None:
            if psycopg is None:
                raise "Couldnt locate psycopg1 or psycopg2: specify postgres module argument"
            self.module = psycopg
        else:
            self.module = module
        self.opts = opts or {}
        ansisql.ANSISQLEngine.__init__(self, **params)

    def connect_args(self):
        return [[], self.opts]

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def compiler(self, statement, bindparams, **kwargs):
        return PGCompiler(self, statement, bindparams, **kwargs)

    def schemagenerator(self, proxy, **params):
        return PGSchemaGenerator(proxy, **params)

    def schemadropper(self, proxy, **params):
        return PGSchemaDropper(proxy, **params)
        
    def reflecttable(self, table):
        raise "not implemented"
        
    def last_inserted_ids(self):
        # if we used sequences or already had all values for the last inserted row,
        # return that list
        if self.context.last_inserted_ids is not None:
            return self.context.last_inserted_ids
        
        # else we have to use lastrowid and select the most recently inserted row    
        table = self.context.last_inserted_table
        if self.context.lastrowid is not None and table is not None and len(table.primary_keys):
            row = sql.select(table.primary_keys, table.rowid_column == self.context.lastrowid).execute().fetchone()
            return [v for v in row]
        else:
            return None
            
    def pre_exec(self, connection, cursor, statement, parameters, echo = None, compiled = None, **kwargs):
        if compiled is None: return
        if getattr(compiled, "isinsert", False):
            if isinstance(parameters, list):
                plist = parameters
            else:
                plist = [parameters]
            # inserts are usually one at a time.  but if we got a list of parameters,
            # it will calculate last_inserted_ids for just the last row in the list. 
            # TODO: why not make last_inserted_ids a 2D array since we have to explicitly sequence
            # it or post-select anyway   
            for param in plist:
                last_inserted_ids = []
                need_lastrowid=False
                for primary_key in compiled.statement.table.primary_keys:
                    if not param.has_key(primary_key.key) or param[primary_key.key] is None:
                        if primary_key.sequence is not None and not primary_key.sequence.optional:
                            if echo is True or self.echo:
                                self.log("select nextval('%s')" % primary_key.sequence.name)
                            cursor.execute("select nextval('%s')" % primary_key.sequence.name)
                            newid = cursor.fetchone()[0]
                            param[primary_key.key] = newid
                            last_inserted_ids.append(param[primary_key.key])
                        else:
                            need_lastrowid = True
                    else:
                        last_inserted_ids.append(param[primary_key.key])
                if need_lastrowid:
                    self.context.last_inserted_ids = None
                else:
                    self.context.last_inserted_ids = last_inserted_ids

    def _executemany(self, c, statement, parameters):
        """we need accurate rowcounts for updates, inserts and deletes.  psycopg2 is not nice enough
        to produce this correctly for an executemany, so we do our own executemany here."""
        rowcount = 0
        for param in parameters:
            c.execute(statement, param)
            rowcount += c.rowcount
        self.context.rowcount = rowcount

    def post_exec(self, connection, cursor, statement, parameters, echo = None, compiled = None, **kwargs):
        if compiled is None: return
        if getattr(compiled, "isinsert", False):
            table = compiled.statement.table
            self.context.last_inserted_table = table
            self.context.lastrowid = cursor.lastrowid
            
    def dbapi(self):
        return self.module

    def reflecttable(self, table):
        columns = gen_columns.toengine(table.engine)
        constraints = gen_constraints.toengine(table.engine)
        column_constraints = gen_column_constraints.toengine(table.engine)
        
        s = select([columns, constraints.c.constraint_type], 
            columns.c.table_name==table.name, 
            order_by=[columns.c.ordinal_position])
            
        s.append_from(sql.outerjoin(columns, column_constraints, 
                              sql.and_(
                                      columns.c.table_name==column_constraints.c.table_name,
                                      columns.c.table_schema==column_constraints.c.table_schema,
                                      columns.c.column_name==column_constraints.c.column_name,
                                  )).outerjoin(constraints, 
                                  sql.and_(
                                      column_constraints.c.table_schema==constraints.c.table_schema,
                                      column_constraints.c.constraint_name==constraints.c.constraint_name,
                                      constraints.c.constraint_type=='PRIMARY KEY'
                                  )))

        if table.schema is not None:
            s.append_whereclause(columns.c.table_schema==table.schema)
        else:
            current_schema = text("select current_schema()", table.engine).scalar()
            s.append_whereclause(columns.c.table_schema==current_schema)

        c = s.execute()
        while True:
            row = c.fetchone()
            if row is None:
                break
            #print "row! " + repr(row)
            (name, type, nullable, primary_key, charlen, numericprec, numericradix) = (
                row[columns.c.column_name], 
                row[columns.c.data_type], 
                not row[columns.c.is_nullable], 
                row[constraints.c.constraint_type] is not None,
                row[columns.c.character_maximum_length],
                row[columns.c.numeric_precision],
                row[columns.c.numeric_precision_radix],
                )

            #match = re.match(r'(\w+)(\(.*?\))?', type)
            #coltype = match.group(1)
            #args = match.group(2)

            #print "coltype: " + repr(coltype) + " args: " + repr(args)
            coltype = ischema_names[type]
            table.append_item(schema.Column(name, coltype, primary_key = primary_key, nullable = nullable))
        return
        c = self.execute("PRAGMA foreign_key_list(" + table.name + ")", {})
        while True:
            row = c.fetchone()
            if row is None:
                break
            (tablename, localcol, remotecol) = (row[2], row[3], row[4])
            #print "row! " + repr(row)
            remotetable = Table(tablename, self, autoload = True)
            table.c[localcol].foreign_key = schema.ForeignKey(remotetable.c[remotecol])

class PGCompiler(ansisql.ANSICompiler):
    def bindparam_string(self, name):
        return "%(" + name + ")s"

    def visit_insert(self, insert):
        """inserts are required to have the primary keys be explicitly present.
         mapper will by default not put them in the insert statement to comply
         with autoincrement fields that require they not be present.  so, 
         put them all in for columns where sequence usage is defined."""
        for c in insert.table.primary_keys:
            if c.sequence is not None and not c.sequence.optional:
                self.bindparams[c.key] = None
        return ansisql.ANSICompiler.visit_insert(self, insert)
        
class PGSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column):
        colspec = column.name
        if column.primary_key and isinstance(column.type, types.Integer) and (column.sequence is None or column.sequence.optional):
            colspec += " SERIAL"
        else:
            colspec += " " + column.type.get_col_spec()

        if not column.nullable:
            colspec += " NOT NULL"
        if column.primary_key:
            colspec += " PRIMARY KEY"
        if column.foreign_key:
            colspec += " REFERENCES %s(%s)" % (column.column.foreign_key.column.table.name, column.column.foreign_key.column.name) 
        return colspec

    def visit_sequence(self, sequence):
        if not sequence.optional:
            self.append("CREATE SEQUENCE %s" % sequence.name)
            self.execute()
            
class PGSchemaDropper(ansisql.ANSISchemaDropper):
    def visit_sequence(self, sequence):
        if not sequence.optional:
            self.append("DROP SEQUENCE %s" % sequence.name)
            self.execute()
