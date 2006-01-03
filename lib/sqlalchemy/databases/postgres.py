# postgres.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import sys, StringIO, string, types, re

import sqlalchemy.sql as sql
import sqlalchemy.engine as engine
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
import sqlalchemy.types as sqltypes
from sqlalchemy import *
import sqlalchemy.databases.information_schema as ischema

try:
    import psycopg2 as psycopg
    #import psycopg2.psycopg1 as psycopg
except:
    try:
        import psycopg
    except:
        psycopg = None


class PGNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}
class PGFloat(sqltypes.Float):
    def get_col_spec(self):
        return "FLOAT(%(precision)s)" % {'precision': self.precision}
class PGInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"
class PG2DateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP"
class PG1DateTime(sqltypes.DateTime):
    def convert_bind_param(self, value, engine):
        # TODO: perform appropriate postgres1 conversion between Python DateTime/MXDateTime
        # this one doesnt seem to work with the "emulation" mode
        return psycopg.TimestampFromMx(value)
    def convert_result_value(self, value, engine):
        # TODO: perform appropriate postgres1 conversion between Python DateTime/MXDateTime
        return value
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
        
pg2_colspecs = {
    sqltypes.Integer : PGInteger,
    sqltypes.Numeric : PGNumeric,
    sqltypes.Float : PGFloat,
    sqltypes.DateTime : PG2DateTime,
    sqltypes.String : PGString,
    sqltypes.Binary : PGBinary,
    sqltypes.Boolean : PGBoolean,
    sqltypes.TEXT : PGText,
    sqltypes.CHAR: PGChar,
}
pg1_colspecs = pg2_colspecs.copy()
pg1_colspecs[sqltypes.DateTime] = PG1DateTime

pg2_ischema_names = {
    'integer' : PGInteger,
    'bigint' : PGInteger,
    'character varying' : PGString,
    'character' : PGChar,
    'text' : PGText,
    'numeric' : PGNumeric,
    'float' : PGFloat,
    'real' : PGFloat,
    'double precision' : PGFloat,
    'timestamp with time zone' : PG2DateTime,
    'timestamp without time zone' : PG2DateTime,
    'bytea' : PGBinary,
    'boolean' : PGBoolean,
}
pg1_ischema_names = pg2_ischema_names.copy()
pg1_ischema_names['timestamp with time zone'] = \
    pg1_ischema_names['timestamp without time zone'] = PG1DateTime

def engine(opts, **params):
    return PGSQLEngine(opts, **params)

def descriptor():
    return {'name':'postgres',
    'description':'PostGres',
    'arguments':[
        ('user',"Database Username",None),
        ('password',"Database Password",None),
        ('database',"Database Name",None),
        ('host',"Hostname", None),
    ]}

class PGSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, opts, module=None, use_oids=False, **params):
        self.use_oids = use_oids
        if module is None:
            if psycopg is None:
                raise "Couldnt locate psycopg1 or psycopg2: specify postgres module argument"
            self.module = psycopg
        else:
            self.module = module
        # figure psycopg version 1 or 2    
        if self.module.__name__.endswith('psycopg2'):
            self.version = 2
        else:
            self.version = 1
        self.opts = opts or {}
        ansisql.ANSISQLEngine.__init__(self, **params)

    def connect_args(self):
        return [[], self.opts]

    def type_descriptor(self, typeobj):
        if self.version == 2:
            return sqltypes.adapt_type(typeobj, pg2_colspecs)
        else:
            return sqltypes.adapt_type(typeobj, pg1_colspecs)

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def compiler(self, statement, bindparams, **kwargs):
        return PGCompiler(self, statement, bindparams, **kwargs)

    def schemagenerator(self, proxy, **params):
        return PGSchemaGenerator(proxy, **params)

    def schemadropper(self, proxy, **params):
        return PGSchemaDropper(proxy, **params)

    def defaultrunner(self, proxy):
        return PGDefaultRunner(self, proxy)
        
    def get_default_schema_name(self):
        if not hasattr(self, '_default_schema_name'):
            self._default_schema_name = text("select current_schema()", self).scalar()
        return self._default_schema_name
        
    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def oid_column_name(self):
        if self.use_oids:
            return "oid"
        else:
            return None

    def pre_exec(self, proxy, statement, parameters, **kwargs):
        return

    def post_exec(self, proxy, compiled, parameters, **kwargs):
        if getattr(compiled, "isinsert", False) and self.context.last_inserted_ids is None:
            if not self.use_oids:
                raise "cant use cursor.lastrowid without OIDs enabled"
            else:
                table = compiled.statement.table
                cursor = proxy()
                if cursor.lastrowid is not None and table is not None and len(table.primary_key):
                    s = sql.select(table.primary_key, table.oid_column == cursor.lastrowid)
                    c = s.compile()
                    cursor = proxy(str(c), c.get_params())
                    row = cursor.fetchone()
                self.context.last_inserted_ids = [v for v in row]

    def _executemany(self, c, statement, parameters):
        """we need accurate rowcounts for updates, inserts and deletes.  psycopg2 is not nice enough
        to produce this correctly for an executemany, so we do our own executemany here."""
        rowcount = 0
        for param in parameters:
            c.execute(statement, param)
            rowcount += c.rowcount
        self.context.rowcount = rowcount

    def dbapi(self):
        return self.module

    def reflecttable(self, table):
        if self.version == 2:
            ischema_names = pg2_ischema_names
        else:
            ischema_names = pg1_ischema_names

        ischema.reflecttable(self, table, ischema_names)

class PGCompiler(ansisql.ANSICompiler):

    def visit_insert_column(self, column):
        # Postgres advises against OID usage and turns it off in 8.1,
        # effectively making cursor.lastrowid
        # useless, effectively making reliance upon SERIAL useless.  
        # so all column primary key inserts must be explicitly present
        if column.primary_key:
            self.parameters[column.key] = None

    def limit_clause(self, select):
        text = ""
        if select.limit is not None:
            text +=  " \n LIMIT " + str(select.limit)
        if select.offset is not None:
            if select.limit is None:
                text += " \n LIMIT ALL"
            text += " OFFSET " + str(select.offset)
        return text
        
class PGSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, override_pk=False, **kwargs):
        colspec = column.name
        if column.primary_key and isinstance(column.type, types.Integer) and (column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional)):
            colspec += " SERIAL"
        else:
            colspec += " " + column.type.get_col_spec()

        if not column.nullable:
            colspec += " NOT NULL"
        if column.primary_key and not override_pk:
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

class PGDefaultRunner(ansisql.ANSIDefaultRunner):
    def get_column_default(self, column):
        if column.primary_key and isinstance(column.type, types.Integer) and (column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional)):
            c = self.proxy("select nextval('%s_%s_seq')" % (column.table.name, column.name))
            return c.fetchone()[0]
        else:
            return ansisql.ANSIDefaultRunner.get_column_default(self, column)
    
    def visit_sequence(self, seq):
        if not seq.optional:
            c = self.proxy("select nextval('%s')" % seq.name)
            return c.fetchone()[0]
        else:
            return None
