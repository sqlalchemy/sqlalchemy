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
from sqlalchemy.databases import information_schema as ischema
from sqlalchemy import * 
import re

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
        if not self.precision:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}
class PGFloat(sqltypes.Float):
    def get_col_spec(self):
        if not self.precision:
            return "FLOAT"
        else:
            return "FLOAT(%(precision)s)" % {'precision': self.precision}
class PGInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"
class PGSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"
class PGBigInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "BIGINT"
class PG2DateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP " + (self.timezone and "WITH" or "WITHOUT") + " TIME ZONE"
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
        return "TIMESTAMP " + (self.timezone and "WITH" or "WITHOUT") + " TIME ZONE"
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
        return "TIME " + (self.timezone and "WITH" or "WITHOUT") + " TIME ZONE"
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
        return "TIME " + (self.timezone and "WITH" or "WITHOUT") + " TIME ZONE"

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
    'bigint' : PGBigInteger,
    'smallint' : PGSmallInteger,
    'character varying' : PGString,
    'character' : PGChar,
    'text' : PGText,
    'numeric' : PGNumeric,
    'float' : PGFloat,
    'real' : PGFloat,
    'double precision' : PGFloat,
    'timestamp' : PG2DateTime,
    'timestamp with time zone' : PG2DateTime,
    'timestamp without time zone' : PG2DateTime,
    'time with time zone' : PG2Time,
    'time without time zone' : PG2Time,
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
    def __init__(self, module=None, use_oids=False, use_information_schema=False, **params):
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
        self.use_information_schema = use_information_schema
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
        return PGIdentifierPreparer(self)
        
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
        # TODO: why are we case folding here ?
        cursor = connection.execute("""select relname from pg_class where lower(relname) = %(name)s""", {'name':table_name.lower()})
        return bool( not not cursor.rowcount )

    def has_sequence(self, connection, sequence_name):
        cursor = connection.execute('''SELECT relname FROM pg_class WHERE relkind = 'S' AND relnamespace IN ( SELECT oid FROM pg_namespace WHERE nspname NOT LIKE 'pg_%%' AND nspname != 'information_schema' AND relname = %(seqname)s);''', {'seqname': sequence_name})
        return bool(not not cursor.rowcount)
        
    def reflecttable(self, connection, table):
        if self.version == 2:
            ischema_names = pg2_ischema_names
        else:
            ischema_names = pg1_ischema_names

        if self.use_information_schema:
            ischema.reflecttable(connection, table, ischema_names)
        else:
            preparer = self.identifier_preparer
            if table.schema is not None:
                current_schema = table.schema
            else:
                current_schema = connection.default_schema_name()
    
            ## information schema in pg suffers from too many permissions' restrictions
            ## let us find out at the pg way what is needed...
    
            SQL_COLS = """
                SELECT a.attname,
                  pg_catalog.format_type(a.atttypid, a.atttypmod),
                  (SELECT substring(d.adsrc for 128) FROM pg_catalog.pg_attrdef d
                   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
                  AS DEFAULT,
                  a.attnotnull, a.attnum
                FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = (
                    SELECT c.oid
                    FROM pg_catalog.pg_class c
                         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE (n.nspname = :schema OR pg_catalog.pg_table_is_visible(c.oid))
                          AND c.relname = :table_name AND (c.relkind = 'r' OR c.relkind = 'v')
                ) AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
            """
    
            s = text(SQL_COLS )
            c = connection.execute(s, table_name=table.name, schema=current_schema)
            found_table = False
            while True:
                row = c.fetchone()
                if row is None:
                    break
                found_table = True
                name = row['attname']
                ## strip (30) from character varying(30)
                attype = re.search('([^\(]+)', row['format_type']).group(1)
    
                nullable = row['attnotnull'] == False
                try:
                    charlen = re.search('\(([\d,]+)\)',row['format_type']).group(1)
                except:
                    charlen = False
    
                numericprec = False
                numericscale = False
                default = row['default']
                if attype == 'numeric':
                    if charlen is False:
                        numericprec, numericscale = (None, None)
                    else:
                        numericprec, numericscale = charlen.split(',')
                    charlen = False
                if attype == 'double precision':
                    numericprec, numericscale = (53, None)
                    charlen = False
                if attype == 'integer':
                    numericprec, numericscale = (32, 0)
                    charlen = False

                args = []
                for a in (charlen, numericprec, numericscale):
                    if a is None:
                        args.append(None)
                    elif a is not False:
                        args.append(int(a))

                kwargs = {}
                if attype == 'timestamp with time zone':
                    kwargs['timezone'] = True
                elif attype == 'timestamp without time zone':
                    kwargs['timezone'] = False
    
                coltype = ischema_names[attype]
                coltype = coltype(*args, **kwargs)
                colargs= []
                if default is not None:
                    colargs.append(PassiveDefault(sql.text(default)))
                table.append_column(schema.Column(name, coltype, nullable=nullable, *colargs))
    
    
            if not found_table:
                raise exceptions.NoSuchTableError(table.name)
    
            # Primary keys
            PK_SQL = """
              SELECT attname FROM pg_attribute 
              WHERE attrelid = (
                 SELECT indexrelid FROM  pg_index i, pg_class c, pg_namespace n
                 WHERE n.nspname = :schema AND c.relname = :table_name 
                 AND c.oid = i.indrelid AND n.oid = c.relnamespace
                 AND i.indisprimary = 't' ) ;
            """ 
            t = text(PK_SQL)
            c = connection.execute(t, table_name=table.name, schema=current_schema)
            while True:
                row = c.fetchone()
                if row is None:
                    break
                pk = row[0]
                table.primary_key.add(table.c[pk])
    
            # Foreign keys
            FK_SQL = """
              SELECT conname, pg_catalog.pg_get_constraintdef(oid, true) as condef 
              FROM  pg_catalog.pg_constraint r 
              WHERE r.conrelid = (
                  SELECT c.oid FROM pg_catalog.pg_class c 
                               LEFT JOIN pg_catalog.pg_namespace n
                               ON n.oid = c.relnamespace 
                  WHERE c.relname = :table_name 
                    AND pg_catalog.pg_table_is_visible(c.oid)) 
                    AND r.contype = 'f' ORDER BY 1
    
            """
            
            t = text(FK_SQL)
            c = connection.execute(t, table_name=table.name)
            while True:
                row = c.fetchone()
                if row is None:
                    break

                foreign_key_pattern = 'FOREIGN KEY \((.*?)\) REFERENCES (?:(.*?)\.)?(.*?)\((.*?)\)'
                m = re.search(foreign_key_pattern, row['condef'])
                (constrained_columns, referred_schema, referred_table, referred_columns) = m.groups() 
                
                constrained_columns = [preparer._unquote_identifier(x) for x in re.split(r'\s*,\s*', constrained_columns)]
                if referred_schema:
                    referred_schema = preparer._unquote_identifier(referred_schema)
                referred_table = preparer._unquote_identifier(referred_table)
                referred_columns = [preparer._unquote_identifier(x) for x in re.split(r'\s*,\s', referred_columns)]
                
                refspec = []
                if referred_schema is not None:
                    schema.Table(referred_table, table.metadata, autoload=True, schema=referred_schema, 
                                autoload_with=connection)
                    for column in referred_columns:
                        refspec.append(".".join([referred_schema, referred_table, column]))
                else:
                    schema.Table(referred_table, table.metadata, autoload=True, autoload_with=connection)
                    for column in referred_columns:
                        refspec.append(".".join([referred_table, column]))
                
                table.append_constraint(ForeignKeyConstraint(constrained_columns, refspec, row['conname']))

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
        if column.primary_key and len(column.foreign_keys)==0 and column.autoincrement and isinstance(column.type, sqltypes.Integer) and not isinstance(column.type, sqltypes.SmallInteger) and (column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional)):
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
        if not sequence.optional and (not self.dialect.has_sequence(self.connection, sequence.name)):
            self.append("CREATE SEQUENCE %s" % self.preparer.format_sequence(sequence))
            self.execute()
            
class PGSchemaDropper(ansisql.ANSISchemaDropper):
    def visit_sequence(self, sequence):
        if not sequence.optional and (self.dialect.has_sequence(self.connection, sequence.name)):
            self.append("DROP SEQUENCE %s" % sequence.name)
            self.execute()

class PGDefaultRunner(ansisql.ANSIDefaultRunner):
    def get_column_default(self, column, isinsert=True):
        if column.primary_key:
            # passive defaults on primary keys have to be overridden
            if isinstance(column.default, schema.PassiveDefault):
                c = self.proxy("select %s" % column.default.arg)
                return c.fetchone()[0]
            elif (isinstance(column.type, sqltypes.Integer) and column.autoincrement) and (column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional)):
                sch = column.table.schema
                # TODO: this has to build into the Sequence object so we can get the quoting 
                # logic from it
                if sch is not None:
                    exc = "select nextval('\"%s\".\"%s_%s_seq\"')" % (sch, column.table.name, column.name)
                else:
                    exc = "select nextval('\"%s_%s_seq\"')" % (column.table.name, column.name)
                c = self.proxy(exc)
                return c.fetchone()[0]
            else:
                return ansisql.ANSIDefaultRunner.get_column_default(self, column)
        else:
            return ansisql.ANSIDefaultRunner.get_column_default(self, column)
        
    def visit_sequence(self, seq):
        if not seq.optional:
            c = self.proxy("select nextval('%s')" % seq.name) #TODO: self.dialect.preparer.format_sequence(seq))
            return c.fetchone()[0]
        else:
            return None

class PGIdentifierPreparer(ansisql.ANSIIdentifierPreparer):
    def _fold_identifier_case(self, value):
        return value.lower()
    def _unquote_identifier(self, value):
        if value[0] == self.initial_quote:
            value = value[1:-1].replace('""','"')
        return value
    
dialect = PGDialect
