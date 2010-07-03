# informix.py
# Copyright (C) 2005,2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# coding: gbk
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Support for the Informix database.

This dialect is *not* tested on SQLAlchemy 0.6.


"""


import datetime

from sqlalchemy import sql, schema, exc, pool, util
from sqlalchemy.sql import compiler
from sqlalchemy.engine import default, reflection
from sqlalchemy import types as sqltypes


class InfoDateTime(sqltypes.DateTime):
    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                if value.microsecond:
                    value = value.replace(microsecond=0)
            return value
        return process

class InfoTime(sqltypes.Time):
    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                if value.microsecond:
                    value = value.replace(microsecond=0)
            return value
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, datetime.datetime):
                return value.time()
            else:
                return value
        return process


colspecs = {
    sqltypes.DateTime : InfoDateTime,
    sqltypes.Time: InfoTime,
}


ischema_names = {
    0   : sqltypes.CHAR,       # CHAR
    1   : sqltypes.SMALLINT, # SMALLINT
    2   : sqltypes.INTEGER,      # INT
    3   : sqltypes.FLOAT,      # Float
    3   : sqltypes.Float,      # SmallFloat
    5   : sqltypes.DECIMAL,      # DECIMAL
    6   : sqltypes.Integer,      # Serial
    7   : sqltypes.DATE,         # DATE
    8   : sqltypes.Numeric,      # MONEY
    10  : sqltypes.DATETIME,     # DATETIME
    11  : sqltypes.LargeBinary,       # BYTE
    12  : sqltypes.TEXT,         # TEXT
    13  : sqltypes.VARCHAR,       # VARCHAR
    15  : sqltypes.NCHAR,       # NCHAR
    16  : sqltypes.NVARCHAR,       # NVARCHAR
    17  : sqltypes.Integer,      # INT8
    18  : sqltypes.Integer,      # Serial8
    43  : sqltypes.String,       # LVARCHAR
    -1  : sqltypes.BLOB,       # BLOB
    -1  : sqltypes.CLOB,         # CLOB
}


class InfoTypeCompiler(compiler.GenericTypeCompiler):
    def visit_DATETIME(self, type_):
        return "DATETIME YEAR TO SECOND"

    def visit_TIME(self, type_):
        return "DATETIME HOUR TO SECOND"

    def visit_large_binary(self, type_):
        return "BYTE"

    def visit_boolean(self, type_):
        return "SMALLINT"

class InfoSQLCompiler(compiler.SQLCompiler):

    def default_from(self):
        return " from systables where tabname = 'systables' "

    def get_select_precolumns(self, select):
        s = select._distinct and "DISTINCT " or ""
        # only has limit
        if select._limit:
            s += " FIRST %s " % select._limit
        else:
            s += ""
        return s

    def visit_select(self, select):
        # the column in order by clause must in select too

        def __label(c):
            try:
                return c._label.lower()
            except:
                return ''

        # TODO: dont modify the original select, generate a new one
        a = [__label(c) for c in select._raw_columns]
        for c in select._order_by_clause.clauses:
            if __label(c) not in a:
                select.append_column(c)

        return compiler.SQLCompiler.visit_select(self, select)

    def limit_clause(self, select):
        if select._offset is not None and select._offset > 0:
            raise NotImplementedError("Informix does not support OFFSET")
        return ""

    def visit_function(self, func):
        if func.name.lower() == 'current_date':
            return "today"
        elif func.name.lower() == 'current_time':
            return "CURRENT HOUR TO SECOND"
        elif func.name.lower() in ('current_timestamp', 'now'):
            return "CURRENT YEAR TO SECOND"
        else:
            return compiler.SQLCompiler.visit_function(self, func)


class InfoDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, first_pk=False):
        colspec = self.preparer.format_column(column)
        if column.primary_key and \
                    len(column.foreign_keys)==0 and \
                    column.autoincrement and \
           isinstance(column.type, sqltypes.Integer) and first_pk:
            colspec += " SERIAL"
        else:
            colspec += " " + self.dialect.type_compiler.process(column.type)
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"

        return colspec


class InfoIdentifierPreparer(compiler.IdentifierPreparer):
    def __init__(self, dialect):
        super(InfoIdentifierPreparer, self).\
                        __init__(dialect, initial_quote="'")

    def format_constraint(self, constraint):
        # informix doesnt support names for constraints
        return ''

    def _requires_quotes(self, value):
        return False

class InformixDialect(default.DefaultDialect):
    name = 'informix'

    max_identifier_length = 128 # adjusts at runtime based on server version
    
    type_compiler = InfoTypeCompiler
    statement_compiler = InfoSQLCompiler
    ddl_compiler = InfoDDLCompiler
    preparer = InfoIdentifierPreparer
    colspecs = colspecs
    ischema_names = ischema_names

    def initialize(self, connection):
        super(InformixDialect, self).initialize(connection)
        
        # http://www.querix.com/support/knowledge-base/error_number_message/error_200
        if self.server_version_info < (9, 2):
            self.max_identifier_length = 18
        else:
            self.max_identifier_length = 128
        
    def do_begin(self, connect):
        cu = connect.cursor()
        cu.execute('SET LOCK MODE TO WAIT')
        #cu.execute('SET ISOLATION TO REPEATABLE READ')

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        s = "select tabname from systables"
        return [row[0] for row in connection.execute(s)]

    def has_table(self, connection, table_name, schema=None):
        cursor = connection.execute(
                """select tabname from systables where tabname=?""",
                table_name.lower())
        return cursor.first() is not None

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        c = connection.execute(
            """select colname, coltype, collength, t3.default, t1.colno from
                syscolumns as t1 , systables as t2 , OUTER sysdefaults as t3
                where t1.tabid = t2.tabid and t2.tabname=? 
                  and t3.tabid = t2.tabid and t3.colno = t1.colno
                order by t1.colno""", table.name.lower())
        columns = []
        for name, colattr, collength, default, colno in rows:
            name = name.lower()
            if include_columns and name not in include_columns:
                continue

            # in 7.31, coltype = 0x000
            #                       ^^-- column type
            #                      ^-- 1 not null, 0 null
            nullable, coltype = divmod(colattr, 256)
            if coltype not in (0, 13) and default:
                default = default.split()[-1]

            if coltype == 0 or coltype == 13: # char, varchar
                coltype = ischema_names[coltype](collength)
                if default:
                    default = "'%s'" % default
            elif coltype == 5: # decimal
                precision, scale = (collength & 0xFF00) >> 8, collength & 0xFF
                if scale == 255:
                    scale = 0
                coltype = sqltypes.Numeric(precision, scale)
            else:
                try:
                    coltype = ischema_names[coltype]
                except KeyError:
                    util.warn("Did not recognize type '%s' of column '%s'" %
                              (coltype, name))
                    coltype = sqltypes.NULLTYPE
            
            # TODO: nullability ??
            nullable = True
            
            column_info = dict(name=name, type=coltype, nullable=nullable,
                               default=default)
            columns.append(column_info)
        return columns

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # FK
        c = connection.execute(
        """select t1.constrname as cons_name , t1.constrtype as cons_type ,
                 t4.colname as local_column , t7.tabname as remote_table ,
                 t6.colname as remote_column
            from sysconstraints as t1 , systables as t2 ,
                 sysindexes as t3 , syscolumns as t4 ,
                 sysreferences as t5 , syscolumns as t6 , systables as t7 ,
                 sysconstraints as t8 , sysindexes as t9
           where t1.tabid = t2.tabid and t2.tabname=? and t1.constrtype = 'R'
             and t3.tabid = t2.tabid and t3.idxname = t1.idxname
             and t4.tabid = t2.tabid and t4.colno = t3.part1
             and t5.constrid = t1.constrid and t8.constrid = t5.primary
             and t6.tabid = t5.ptabid and t6.colno = t9.part1 and t9.idxname =
             t8.idxname
             and t7.tabid = t5.ptabid""", table.name.lower())


        def fkey_rec():
            return {
                 'name' : None,
                 'constrained_columns' : [],
                 'referred_schema' : None,
                 'referred_table' : None,
                 'referred_columns' : []
             }

        fkeys = util.defaultdict(fkey_rec)

        for cons_name, cons_type, local_column, \
                    remote_table, remote_column in rows:

            rec = fkeys[cons_name]
            rec['name'] = cons_name
            local_cols, remote_cols = \
                        rec['constrained_columns'], rec['referred_columns']

            if not rec['referred_table']:
                rec['referred_table'] = remote_table

            local_cols.append(local_column)
            remote_cols.append(remote_column)

        return fkeys.values()

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        c = connection.execute(
            """select t4.colname as local_column
            from sysconstraints as t1 , systables as t2 ,
                 sysindexes as t3 , syscolumns as t4
           where t1.tabid = t2.tabid and t2.tabname=? and t1.constrtype = 'P'
             and t3.tabid = t2.tabid and t3.idxname = t1.idxname
             and t4.tabid = t2.tabid and t4.colno = t3.part1""",
             table.name.lower())
        return [r[0] for r in c.fetchall()]

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        # TODO
        return []
