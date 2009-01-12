# informix.py
# Copyright (C) 2005,2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# coding: gbk
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import datetime

from sqlalchemy import sql, schema, exc, pool, util
from sqlalchemy.sql import compiler
from sqlalchemy.engine import default
from sqlalchemy import types as sqltypes


# for offset

class informix_cursor(object):
    def __init__( self , con ):
        self.__cursor = con.cursor()
        self.rowcount = 0

    def offset( self , n ):
        if n > 0:
            self.fetchmany( n )
            self.rowcount = self.__cursor.rowcount - n
            if self.rowcount < 0:
                self.rowcount = 0
        else:
            self.rowcount = self.__cursor.rowcount

    def execute( self , sql , params ):
        if params is None or len( params ) == 0:
            params = []

        return self.__cursor.execute( sql , params )

    def __getattr__( self , name ):
        if name not in ( 'offset' , '__cursor' , 'rowcount' , '__del__' , 'execute' ):
            return getattr( self.__cursor , name )

class InfoNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        if not self.precision:
            return 'NUMERIC'
        else:
            return "NUMERIC(%(precision)s, %(scale)s)" % {'precision': self.precision, 'scale' : self.scale}

class InfoInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"

class InfoSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"

class InfoDate(sqltypes.Date):
    def get_col_spec( self ):
        return "DATE"

class InfoDateTime(sqltypes.DateTime ):
    def get_col_spec(self):
        return "DATETIME YEAR TO SECOND"

    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                if value.microsecond:
                    value = value.replace( microsecond = 0 )
            return value
        return process

class InfoTime(sqltypes.Time ):
    def get_col_spec(self):
        return "DATETIME HOUR TO SECOND"

    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                if value.microsecond:
                    value = value.replace( microsecond = 0 )
            return value
        return process

    def result_processor(self, dialect):
        def process(value):
            if isinstance( value , datetime.datetime ):
                return value.time()
            else:
                return value
        return process

class InfoText(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(255)"

class InfoString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}

    def bind_processor(self, dialect):
        def process(value):
            if value == '':
                return None
            else:
                return value
        return process

class InfoChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}

class InfoBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BYTE"

class InfoBoolean(sqltypes.Boolean):
    default_type = 'NUM'
    def get_col_spec(self):
        return "SMALLINT"

    def result_processor(self, dialect):
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
    sqltypes.Integer : InfoInteger,
    sqltypes.Smallinteger : InfoSmallInteger,
    sqltypes.Numeric : InfoNumeric,
    sqltypes.Float : InfoNumeric,
    sqltypes.DateTime : InfoDateTime,
    sqltypes.Date : InfoDate,
    sqltypes.Time: InfoTime,
    sqltypes.String : InfoString,
    sqltypes.Binary : InfoBinary,
    sqltypes.Boolean : InfoBoolean,
    sqltypes.Text : InfoText,
    sqltypes.CHAR: InfoChar,
}


ischema_names = {
    0   : InfoString,       # CHAR
    1   : InfoSmallInteger, # SMALLINT
    2   : InfoInteger,      # INT
    3   : InfoNumeric,      # Float
    3   : InfoNumeric,      # SmallFloat
    5   : InfoNumeric,      # DECIMAL
    6   : InfoInteger,      # Serial
    7   : InfoDate,         # DATE
    8   : InfoNumeric,      # MONEY
    10  : InfoDateTime,     # DATETIME
    11  : InfoBinary,       # BYTE
    12  : InfoText,         # TEXT
    13  : InfoString,       # VARCHAR
    15  : InfoString,       # NCHAR
    16  : InfoString,       # NVARCHAR
    17  : InfoInteger,      # INT8
    18  : InfoInteger,      # Serial8
    43  : InfoString,       # LVARCHAR
    -1  : InfoBinary,       # BLOB
    -1  : InfoText,         # CLOB
}


class InfoExecutionContext(default.DefaultExecutionContext):
    # cursor.sqlerrd
    # 0 - estimated number of rows returned
    # 1 - serial value after insert or ISAM error code
    # 2 - number of rows processed
    # 3 - estimated cost
    # 4 - offset of the error into the SQL statement
    # 5 - rowid after insert
    def post_exec(self):
        if getattr(self.compiled, "isinsert", False) and self.last_inserted_ids() is None:
            self._last_inserted_ids = [self.cursor.sqlerrd[1]]
        elif hasattr( self.compiled , 'offset' ):
            self.cursor.offset( self.compiled.offset )
        super(InfoExecutionContext, self).post_exec()

    def create_cursor( self ):
        return informix_cursor( self.connection.connection )

class InfoDialect(default.DefaultDialect):
    name = 'informix'
    default_paramstyle = 'qmark'
    # for informix 7.31
    max_identifier_length = 18

    def __init__(self, use_ansi=True, **kwargs):
        self.use_ansi = use_ansi
        default.DefaultDialect.__init__(self, **kwargs)

    def dbapi(cls):
        import informixdb
        return informixdb
    dbapi = classmethod(dbapi)

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.OperationalError):
            return 'closed the connection' in str(e) or 'connection not open' in str(e)
        else:
            return False

    def do_begin(self , connect ):
        cu = connect.cursor()
        cu.execute( 'SET LOCK MODE TO WAIT' )
        #cu.execute( 'SET ISOLATION TO REPEATABLE READ' )

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def create_connect_args(self, url):
        if url.host:
            dsn = '%s@%s' % ( url.database , url.host )
        else:
            dsn = url.database

        if url.username:
            opt = { 'user':url.username , 'password': url.password }
        else:
            opt = {}

        return ([dsn], opt)

    def table_names(self, connection, schema):
        s = "select tabname from systables"
        return [row[0] for row in connection.execute(s)]

    def has_table(self, connection, table_name, schema=None):
        cursor = connection.execute("""select tabname from systables where tabname=?""", table_name.lower() )
        return bool( cursor.fetchone() is not None )

    def reflecttable(self, connection, table, include_columns):
        c = connection.execute ("select distinct OWNER from systables where tabname=?", table.name.lower() )
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
                                    order by t1.colno""", table.name.lower(), owner )
        rows = c.fetchall()

        if not rows:
            raise exc.NoSuchTableError(table.name)

        for name , colattr , collength , default , colno in rows:
            name = name.lower()
            if include_columns and name not in include_columns:
                continue

            # in 7.31, coltype = 0x000
            #                       ^^-- column type
            #                      ^-- 1 not null , 0 null
            nullable , coltype = divmod( colattr , 256 )
            if coltype not in ( 0 , 13 ) and default:
                default = default.split()[-1]

            if coltype == 0 or coltype == 13: # char , varchar
                coltype = ischema_names.get(coltype, InfoString)(collength)
                if default:
                    default = "'%s'" % default
            elif coltype == 5: # decimal
                precision , scale = ( collength & 0xFF00 ) >> 8 , collength & 0xFF
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
                                     and t7.tabid = t5.ptabid""", table.name.lower(), owner )
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
            table.append_constraint(schema.ForeignKeyConstraint(value[0], value[1] , None, link_to_name=True ))

        # PK
        c = connection.execute("""select t1.constrname as cons_name , t1.constrtype as cons_type ,
                                         t4.colname as local_column
                                    from sysconstraints as t1 , systables as t2 ,
                                         sysindexes as t3 , syscolumns as t4
                                   where t1.tabid = t2.tabid and t2.tabname=? and t2.owner=? and t1.constrtype = 'P'
                                     and t3.tabid = t2.tabid and t3.idxname = t1.idxname
                                     and t4.tabid = t2.tabid and t4.colno = t3.part1""", table.name.lower(), owner )
        rows = c.fetchall()
        for cons_name, cons_type, local_column in rows:
            table.primary_key.add( table.c[local_column] )

class InfoCompiler(compiler.DefaultCompiler):
    """Info compiler modifies the lexical structure of Select statements to work under
    non-ANSI configured Oracle databases, if the use_ansi flag is False."""

    def __init__(self, *args, **kwargs):
        self.limit = 0
        self.offset = 0

        compiler.DefaultCompiler.__init__( self , *args, **kwargs )

    def default_from(self):
        return " from systables where tabname = 'systables' "

    def get_select_precolumns( self , select ):
        s = select._distinct and "DISTINCT " or ""
        # only has limit
        if select._limit:
            off = select._offset or 0
            s += " FIRST %s " % ( select._limit + off )
        else:
            s += ""
        return s

    def visit_select(self, select):
        if select._offset:
            self.offset = select._offset
            self.limit  = select._limit or 0
        # the column in order by clause must in select too

        def __label( c ):
            try:
                return c._label.lower()
            except:
                return ''

        # TODO: dont modify the original select, generate a new one
        a = [ __label(c) for c in select._raw_columns ]
        for c in select._order_by_clause.clauses:
            if ( __label(c) not in a ):
                select.append_column( c )

        return compiler.DefaultCompiler.visit_select(self, select)

    def limit_clause(self, select):
        return ""

    def visit_function( self , func ):
        if func.name.lower() == 'current_date':
            return "today"
        elif func.name.lower() == 'current_time':
            return "CURRENT HOUR TO SECOND"
        elif func.name.lower() in ( 'current_timestamp' , 'now' ):
            return "CURRENT YEAR TO SECOND"
        else:
            return compiler.DefaultCompiler.visit_function( self , func )

    def visit_clauselist(self, list, **kwargs):
        return ', '.join([s for s in [self.process(c) for c in list.clauses] if s is not None])

class InfoSchemaGenerator(compiler.SchemaGenerator):
    def get_column_specification(self, column, first_pk=False):
        colspec = self.preparer.format_column(column)
        if column.primary_key and len(column.foreign_keys)==0 and column.autoincrement and \
           isinstance(column.type, sqltypes.Integer) and not getattr( self , 'has_serial' , False ) and first_pk:
            colspec += " SERIAL"
            self.has_serial = True
        else:
            colspec += " " + column.type.dialect_impl(self.dialect).get_col_spec()
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"

        return colspec

    def post_create_table(self, table):
        if hasattr( self , 'has_serial' ):
            del self.has_serial
        return ''

    def visit_primary_key_constraint(self, constraint):
        # for informix 7.31 not support constraint name
        name = constraint.name
        constraint.name = None
        super(InfoSchemaGenerator, self).visit_primary_key_constraint(constraint)
        constraint.name = name

    def visit_unique_constraint(self, constraint):
        # for informix 7.31 not support constraint name
        name = constraint.name
        constraint.name = None
        super(InfoSchemaGenerator, self).visit_unique_constraint(constraint)
        constraint.name = name

    def visit_foreign_key_constraint( self , constraint ):
        if constraint.name is not None:
            constraint.use_alter = True
        else:
            super( InfoSchemaGenerator , self ).visit_foreign_key_constraint( constraint )

    def define_foreign_key(self, constraint):
        # for informix 7.31 not support constraint name
        if constraint.use_alter:
            name = constraint.name
            constraint.name = None
            self.append( "CONSTRAINT " )
            super(InfoSchemaGenerator, self).define_foreign_key(constraint)
            constraint.name = name
            if name is not None:
                self.append( " CONSTRAINT " + name )
        else:
            super(InfoSchemaGenerator, self).define_foreign_key(constraint)

    def visit_index(self, index):
        if len( index.columns ) == 1 and index.columns[0].foreign_key:
            return
        super(InfoSchemaGenerator, self).visit_index(index)

class InfoIdentifierPreparer(compiler.IdentifierPreparer):
    def __init__(self, dialect):
        super(InfoIdentifierPreparer, self).__init__(dialect, initial_quote="'")

    def _requires_quotes(self, value):
        return False

class InfoSchemaDropper(compiler.SchemaDropper):
    def drop_foreignkey(self, constraint):
        if constraint.name is not None:
            super( InfoSchemaDropper , self ).drop_foreignkey( constraint )

dialect = InfoDialect
poolclass = pool.SingletonThreadPool
dialect.statement_compiler = InfoCompiler
dialect.schemagenerator = InfoSchemaGenerator
dialect.schemadropper = InfoSchemaDropper
dialect.preparer = InfoIdentifierPreparer
dialect.execution_ctx_cls = InfoExecutionContext