# informix.py
# Copyright (C) 2005,2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# coding: gbk
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Support for the Informix database.

This dialect is *not* ported to SQLAlchemy 0.6.

This dialect is *not* tested on SQLAlchemy 0.6.


"""


import datetime

from sqlalchemy import sql, schema, exc, pool, util
from sqlalchemy.sql import compiler
from sqlalchemy.engine import default
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


class InfoBoolean(sqltypes.Boolean):
    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            return value and True or False
        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is True:
                return 1
            elif value is False:
                return 0
            elif value is None:
                return None
            else:
                return value and True or False
        return process

colspecs = {
    sqltypes.DateTime : InfoDateTime,
    sqltypes.Time: InfoTime,
    sqltypes.Boolean : InfoBoolean,
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
    11  : sqltypes.Binary,       # BYTE
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

    def visit_binary(self, type_):
        return "BYTE"

    def visit_boolean(self, type_):
        return "SMALLINT"

class InfoSQLCompiler(compiler.SQLCompiler):

    def __init__(self, *args, **kwargs):
        self.limit = 0
        self.offset = 0

        compiler.SQLCompiler.__init__(self, *args, **kwargs)

    def default_from(self):
        return " from systables where tabname = 'systables' "

    def get_select_precolumns(self, select):
        s = select._distinct and "DISTINCT " or ""
        # only has limit
        if select._limit:
            off = select._offset or 0
            s += " FIRST %s " % (select._limit + off)
        else:
            s += ""
        return s

    def visit_select(self, select):
        if select._offset:
            self.offset = select._offset
            self.limit  = select._limit or 0
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

    def visit_clauselist(self, list, **kwargs):
        return ', '.join([s for s in [self.process(c) for c in list.clauses] if s is not None])

class InfoDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, first_pk=False):
        colspec = self.preparer.format_column(column)
        if column.primary_key and len(column.foreign_keys)==0 and column.autoincrement and \
           isinstance(column.type, sqltypes.Integer) and not getattr(self, 'has_serial', False) and first_pk:
            colspec += " SERIAL"
            self.has_serial = True
        else:
            colspec += " " + self.dialect.type_compiler.process(column.type)
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"

        return colspec

    def post_create_table(self, table):
        if hasattr(self, 'has_serial'):
            del self.has_serial
        return ''

class InfoIdentifierPreparer(compiler.IdentifierPreparer):
    def __init__(self, dialect):
        super(InfoIdentifierPreparer, self).__init__(dialect, initial_quote="'")

    def format_constraint(self, constraint):
        # informix doesnt support names for constraints
        return ''

    def _requires_quotes(self, value):
        return False

class InformixDialect(default.DefaultDialect):
    name = 'informix'
    # for informix 7.31
    max_identifier_length = 18
    type_compiler = InfoTypeCompiler
    poolclass = pool.SingletonThreadPool
    statement_compiler = InfoSQLCompiler
    ddl_compiler = InfoDDLCompiler
    preparer = InfoIdentifierPreparer
    colspecs = colspecs
    ischema_names = ischema_names

    ported_sqla_06 = False

    def do_begin(self, connect):
        cu = connect.cursor()
        cu.execute('SET LOCK MODE TO WAIT')
        #cu.execute('SET ISOLATION TO REPEATABLE READ')

    def table_names(self, connection, schema):
        s = "select tabname from systables"
        return [row[0] for row in connection.execute(s)]

    def has_table(self, connection, table_name, schema=None):
        cursor = connection.execute("""select tabname from systables where tabname=?""", table_name.lower())
        return cursor.first() is not None

    def reflecttable(self, connection, table, include_columns):
        c = connection.execute ("select distinct OWNER from systables where tabname=?", table.name.lower())
        rows = c.fetchall()
        if not rows :
            raise exc.NoSuchTableError(table.name)
        else:
            if table.owner is not None:
                if table.owner.lower() in [r[0] for r in rows]:
                    owner = table.owner.lower()
                else:
                    raise AssertionError("Specified owner %s does not own table %s"%(table.owner, table.name))
            else:
                if len(rows)==1:
                    owner = rows[0][0]
                else:
                    raise AssertionError("There are multiple tables with name %s in the schema, you must specifie owner"%table.name)

        c = connection.execute ("""select colname , coltype , collength , t3.default , t1.colno from syscolumns as t1 , systables as t2 , OUTER sysdefaults as t3
                                    where t1.tabid = t2.tabid and t2.tabname=? and t2.owner=?
                                      and t3.tabid = t2.tabid and t3.colno = t1.colno
                                    order by t1.colno""", table.name.lower(), owner)
        rows = c.fetchall()

        if not rows:
            raise exc.NoSuchTableError(table.name)

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
                coltype = ischema_names.get(coltype, InfoString)(collength)
                if default:
                    default = "'%s'" % default
            elif coltype == 5: # decimal
                precision, scale = (collength & 0xFF00) >> 8, collength & 0xFF
                if scale == 255:
                    scale = 0
                coltype = InfoNumeric(precision, scale)
            else:
                try:
                    coltype = ischema_names[coltype]
                except KeyError:
                    util.warn("Did not recognize type '%s' of column '%s'" %
                              (coltype, name))
                    coltype = sqltypes.NULLTYPE

            colargs = []
            if default is not None:
                colargs.append(schema.DefaultClause(sql.text(default)))

            table.append_column(schema.Column(name, coltype, nullable = (nullable == 0), *colargs))

        # FK
        c = connection.execute("""select t1.constrname as cons_name , t1.constrtype as cons_type ,
                                         t4.colname as local_column , t7.tabname as remote_table ,
                                         t6.colname as remote_column
                                    from sysconstraints as t1 , systables as t2 ,
                                         sysindexes as t3 , syscolumns as t4 ,
                                         sysreferences as t5 , syscolumns as t6 , systables as t7 ,
                                         sysconstraints as t8 , sysindexes as t9
                                   where t1.tabid = t2.tabid and t2.tabname=? and t2.owner=? and t1.constrtype = 'R'
                                     and t3.tabid = t2.tabid and t3.idxname = t1.idxname
                                     and t4.tabid = t2.tabid and t4.colno = t3.part1
                                     and t5.constrid = t1.constrid and t8.constrid = t5.primary
                                     and t6.tabid = t5.ptabid and t6.colno = t9.part1 and t9.idxname = t8.idxname
                                     and t7.tabid = t5.ptabid""", table.name.lower(), owner)
        rows = c.fetchall()
        fks = {}
        for cons_name, cons_type, local_column, remote_table, remote_column in rows:
            try:
                fk = fks[cons_name]
            except KeyError:
                fk = ([], [])
                fks[cons_name] = fk
            refspec = ".".join([remote_table, remote_column])
            schema.Table(remote_table, table.metadata, autoload=True, autoload_with=connection)
            if local_column not in fk[0]:
                fk[0].append(local_column)
            if refspec not in fk[1]:
                fk[1].append(refspec)

        for name, value in fks.iteritems():
            table.append_constraint(schema.ForeignKeyConstraint(value[0], value[1], None, link_to_name=True))

        # PK
        c = connection.execute("""select t1.constrname as cons_name , t1.constrtype as cons_type ,
                                         t4.colname as local_column
                                    from sysconstraints as t1 , systables as t2 ,
                                         sysindexes as t3 , syscolumns as t4
                                   where t1.tabid = t2.tabid and t2.tabname=? and t2.owner=? and t1.constrtype = 'P'
                                     and t3.tabid = t2.tabid and t3.idxname = t1.idxname
                                     and t4.tabid = t2.tabid and t4.colno = t3.part1""", table.name.lower(), owner)
        rows = c.fetchall()
        for cons_name, cons_type, local_column in rows:
            table.primary_key.add(table.c[local_column])

