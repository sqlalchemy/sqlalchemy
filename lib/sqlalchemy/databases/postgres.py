# postgres.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import datetime, sys, StringIO, string, types, re

import sqlalchemy.util as util
import sqlalchemy.sql as sql
import sqlalchemy.engine as engine
import sqlalchemy.engine.default as default
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
import sqlalchemy.types as sqltypes
import sqlalchemy.exceptions as exceptions
import information_schema as ischema

try:
    import mx.DateTime.DateTime as mxDateTime
except:
    mxDateTime = None

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
class PGSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"
class PG2DateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP"
class PG1DateTime(sqltypes.DateTime):
    def convert_bind_param(self, value, dialect):
        if value is not None:
            if isinstance(value, datetime.datetime):
                seconds = float(str(value.second) + "."
                                + str(value.microsecond))
                mx_datetime = mxDateTime(value.year, value.month, value.day,
                                         value.hour, value.minute,
                                         seconds)
                return psycopg.TimestampFromMx(mx_datetime)
            return psycopg.TimestampFromMx(value)
        else:
            return None
    def convert_result_value(self, value, dialect):
        if value is None:
            return None
        second_parts = str(value.second).split(".")
        seconds = int(second_parts[0])
        microseconds = int(second_parts[1])
        return datetime.datetime(value.year, value.month, value.day,
                                 value.hour, value.minute, seconds,
                                 microseconds)
    def get_col_spec(self):
        return "TIMESTAMP"
class PG2Date(sqltypes.Date):
    def get_col_spec(self):
        return "DATE"
class PG1Date(sqltypes.Date):
    def convert_bind_param(self, value, dialect):
        # TODO: perform appropriate postgres1 conversion between Python DateTime/MXDateTime
        # this one doesnt seem to work with the "emulation" mode
        if value is not None:
            return psycopg.DateFromMx(value)
        else:
            return None
    def convert_result_value(self, value, dialect):
        # TODO: perform appropriate postgres1 conversion between Python DateTime/MXDateTime
        return value
    def get_col_spec(self):
        return "DATE"
class PG2Time(sqltypes.Time):
    def get_col_spec(self):
        return "TIME"
class PG1Time(sqltypes.Time):
    def convert_bind_param(self, value, dialect):
        # TODO: perform appropriate postgres1 conversion between Python DateTime/MXDateTime
        # this one doesnt seem to work with the "emulation" mode
        if value is not None:
            return psycopg.TimeFromMx(value)
        else:
            return None
    def convert_result_value(self, value, dialect):
        # TODO: perform appropriate postgres1 conversion between Python DateTime/MXDateTime
        return value
    def get_col_spec(self):
        return "TIME"

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
    sqltypes.Smallinteger : PGSmallInteger,
    sqltypes.Numeric : PGNumeric,
    sqltypes.Float : PGFloat,
    sqltypes.DateTime : PG2DateTime,
    sqltypes.Date : PG2Date,
    sqltypes.Time : PG2Time,
    sqltypes.String : PGString,
    sqltypes.Binary : PGBinary,
    sqltypes.Boolean : PGBoolean,
    sqltypes.TEXT : PGText,
    sqltypes.CHAR: PGChar,
}
pg1_colspecs = pg2_colspecs.copy()
pg1_colspecs.update({
    sqltypes.DateTime :  PG1DateTime,
    sqltypes.Date : PG1Date,
    sqltypes.Time : PG1Time
    })

pg2_ischema_names = {
    'integer' : PGInteger,
    'bigint' : PGInteger,
    'smallint' : PGSmallInteger,
    'character varying' : PGString,
    'character' : PGChar,
    'text' : PGText,
    'numeric' : PGNumeric,
    'float' : PGFloat,
    'real' : PGFloat,
    'double precision' : PGFloat,
    'timestamp with time zone' : PG2DateTime,
    'timestamp without time zone' : PG2DateTime,
    'date' : PG2Date,
    'time': PG2Time,
    'bytea' : PGBinary,
    'boolean' : PGBoolean,
}
pg1_ischema_names = pg2_ischema_names.copy()
pg1_ischema_names.update({
    'timestamp with time zone' : PG1DateTime,
    'timestamp without time zone' : PG1DateTime,
    'date' : PG1Date,
    'time' : PG1Time
    })

def engine(opts, **params):
    return PGSQLEngine(opts, **params)

def descriptor():
    return {'name':'postgres',
    'description':'PostGres',
    'arguments':[
        ('username',"Database Username",None),
        ('password',"Database Password",None),
        ('database',"Database Name",None),
        ('host',"Hostname", None),
    ]}

class PGExecutionContext(default.DefaultExecutionContext):

    def post_exec(self, engine, proxy, compiled, parameters, **kwargs):
        if getattr(compiled, "isinsert", False) and self.last_inserted_ids is None:
            if not engine.dialect.use_oids:
                pass
                # will raise invalid error when they go to get them
            else:
                table = compiled.statement.table
                cursor = proxy()
                if cursor.lastrowid is not None and table is not None and len(table.primary_key):
                    s = sql.select(table.primary_key, table.oid_column == cursor.lastrowid)
                    c = s.compile(engine=engine)
                    cursor = proxy(str(c), c.get_params())
                    row = cursor.fetchone()
                self._last_inserted_ids = [v for v in row]
    
class PGDialect(ansisql.ANSIDialect):
    def __init__(self, module=None, use_oids=False, **params):
        self.use_oids = use_oids
        if module is None:
            #if psycopg is None:
            #    raise exceptions.ArgumentError("Couldnt locate psycopg1 or psycopg2: specify postgres module argument")
            self.module = psycopg
        else:
            self.module = module
        # figure psycopg version 1 or 2    
        try:
            if self.module.__version__.startswith('2'):
                self.version = 2
            else:
                self.version = 1
        except:
            self.version = 1
        ansisql.ANSIDialect.__init__(self, **params)
        # produce consistent paramstyle even if psycopg2 module not present
        if self.module is None:
            self.paramstyle = 'pyformat'
        
    def create_connect_args(self, url):
        opts = url.translate_connect_args(['host', 'database', 'user', 'password', 'port'])
        if opts.has_key('port'):
            if self.version == 2:
                opts['port'] = int(opts['port'])
            else:
                opts['port'] = str(opts['port'])
        opts.update(url.query)
        return ([], opts)

    def create_execution_context(self):
        return PGExecutionContext(self)

    def type_descriptor(self, typeobj):
        if self.version == 2:
            return sqltypes.adapt_type(typeobj, pg2_colspecs)
        else:
            return sqltypes.adapt_type(typeobj, pg1_colspecs)

    def compiler(self, statement, bindparams, **kwargs):
        return PGCompiler(self, statement, bindparams, **kwargs)
    def schemagenerator(self, *args, **kwargs):
        return PGSchemaGenerator(*args, **kwargs)
    def schemadropper(self, *args, **kwargs):
        return PGSchemaDropper(*args, **kwargs)
    def defaultrunner(self, engine, proxy):
        return PGDefaultRunner(engine, proxy)
    def preparer(self):
        return PGIdentifierPreparer()
        
    def get_default_schema_name(self, connection):
        if not hasattr(self, '_default_schema_name'):
            self._default_schema_name = connection.scalar("select current_schema()", None)
        return self._default_schema_name
        
    def last_inserted_ids(self):
        if self.context.last_inserted_ids is None:
            raise exceptions.InvalidRequestError("no INSERT executed, or cant use cursor.lastrowid without Postgres OIDs enabled")
        else:
            return self.context.last_inserted_ids

    def oid_column_name(self):
        if self.use_oids:
            return "oid"
        else:
            return None

    def do_executemany(self, c, statement, parameters, context=None):
        """we need accurate rowcounts for updates, inserts and deletes.  psycopg2 is not nice enough
        to produce this correctly for an executemany, so we do our own executemany here."""
        rowcount = 0
        for param in parameters:
            c.execute(statement, param)
            rowcount += c.rowcount
        if context is not None:
            context._rowcount = rowcount

    def dbapi(self):
        return self.module

    def has_table(self, connection, table_name):
        cursor = connection.execute("""select relname from pg_class where lower(relname) = %(name)s""", {'name':table_name.lower()})
        return bool( not not cursor.rowcount )

    def reflecttable(self, connection, table):
        if self.version == 2:
            ischema_names = pg2_ischema_names
        else:
            ischema_names = pg1_ischema_names

        ischema.reflecttable(connection, table, ischema_names)

class PGCompiler(ansisql.ANSICompiler):
        
    def visit_insert_column(self, column, parameters):
        # Postgres advises against OID usage and turns it off in 8.1,
        # effectively making cursor.lastrowid
        # useless, effectively making reliance upon SERIAL useless.  
        # so all column primary key inserts must be explicitly present
        if column.primary_key:
            parameters[column.key] = None

    def limit_clause(self, select):
        text = ""
        if select.limit is not None:
            text +=  " \n LIMIT " + str(select.limit)
        if select.offset is not None:
            if select.limit is None:
                text += " \n LIMIT ALL"
            text += " OFFSET " + str(select.offset)
        return text

    def visit_select_precolumns(self, select):
        if select.distinct:
            if type(select.distinct) == bool:
                return "DISTINCT "
            if type(select.distinct) == list:
                dist_set = "DISTINCT ON ("
                for col in select.distinct:
                    dist_set += self.strings[col] + ", "
                    dist_set = dist_set[:-2] + ") "
                return dist_set
            return "DISTINCT ON (" + str(select.distinct) + ") "
        else:
            return ""

    def binary_operator_string(self, binary):
        if isinstance(binary.type, sqltypes.String) and binary.operator == '+':
            return '||'
        else:
            return ansisql.ANSICompiler.binary_operator_string(self, binary)        
        
class PGSchemaGenerator(ansisql.ANSISchemaGenerator):
        
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        if column.primary_key and not column.foreign_key and isinstance(column.type, sqltypes.Integer) and (column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional)):
            colspec += " SERIAL"
        else:
            colspec += " " + column.type.engine_impl(self.engine).get_col_spec()
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
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
    def get_column_default(self, column, isinsert=True):
        if column.primary_key:
            # passive defaults on primary keys have to be overridden
            if isinstance(column.default, schema.PassiveDefault):
                c = self.proxy("select %s" % column.default.arg)
                return c.fetchone()[0]
            elif isinstance(column.type, sqltypes.Integer) and (column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional)):
                sch = column.table.schema
                if sch is not None:
                    exc = "select nextval('%s.%s_%s_seq')" % (sch, column.table.name, column.name)
                else:
                    exc = "select nextval('%s_%s_seq')" % (column.table.name, column.name)
                c = self.proxy(exc)
                return c.fetchone()[0]
            else:
                return ansisql.ANSIDefaultRunner.get_column_default(self, column)
        else:
            return ansisql.ANSIDefaultRunner.get_column_default(self, column)
        
    def visit_sequence(self, seq):
        if not seq.optional:
            c = self.proxy("select nextval('%s')" % seq.name)
            return c.fetchone()[0]
        else:
            return None

class PGIdentifierPreparer(ansisql.ANSIIdentifierPreparer):
    def _fold_identifier_case(self, value):
        return value.lower()

dialect = PGDialect
