# sqlite.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


import datetime, re, time

from sqlalchemy import schema, exceptions, pool, PassiveDefault
from sqlalchemy.engine import default
import sqlalchemy.types as sqltypes
import sqlalchemy.util as util
from sqlalchemy.sql import compiler, functions as sql_functions


SELECT_REGEXP = re.compile(r'\s*(?:SELECT|PRAGMA)', re.I | re.UNICODE)

class SLNumeric(sqltypes.Numeric):
    def bind_processor(self, dialect):
        type_ = self.asdecimal and str or float
        def process(value):
            if value is not None:
                return type_(value)
            else:
                return value
        return process

    def get_col_spec(self):
        if self.precision is None:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}

class SLFloat(sqltypes.Float):
    def bind_processor(self, dialect):
        type_ = self.asdecimal and str or float
        def process(value):
            if value is not None:
                return type_(value)
            else:
                return value
        return process

    def get_col_spec(self):
        return "FLOAT"
    
class SLInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"

class SLSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"

class DateTimeMixin(object):
    __format__ = "%Y-%m-%d %H:%M:%S"
    __legacy_microseconds__ = True
    
    def bind_processor(self, dialect):
        def process(value):
            if isinstance(value, basestring):
                # pass string values thru
                return value
            elif value is not None:
                if self.__microsecond__ and getattr(value, 'microsecond', None) is not None:
                    if self.__legacy_microseconds__:
                        return value.strftime(self.__format__ + '.' + str(value.microsecond))
                    else:
                        return value.strftime(self.__format__ + ('.%06d' % value.microsecond))
                else:
                    return value.strftime(self.__format__)
            else:
                return None
        return process

    def _cvt(self, value, dialect):
        if value is None:
            return None
        try:
            (value, microsecond) = value.split('.')
            if self.__legacy_microseconds__:
                microsecond = int(microsecond)
            else:
                microsecond = int((microsecond + '000000')[0:6])
        except ValueError:
            microsecond = 0
        return time.strptime(value, self.__format__)[0:6] + (microsecond,)

class SLDateTime(DateTimeMixin,sqltypes.DateTime):
    __format__ = "%Y-%m-%d %H:%M:%S"
    __microsecond__ = True

    def get_col_spec(self):
        return "TIMESTAMP"

    def result_processor(self, dialect):
        def process(value):
            tup = self._cvt(value, dialect)
            return tup and datetime.datetime(*tup)
        return process

class SLDate(DateTimeMixin, sqltypes.Date):
    __format__ = "%Y-%m-%d"
    __microsecond__ = False

    def get_col_spec(self):
        return "DATE"

    def result_processor(self, dialect):
        def process(value):
            tup = self._cvt(value, dialect)
            return tup and datetime.date(*tup[0:3])
        return process

class SLTime(DateTimeMixin, sqltypes.Time):
    __format__ = "%H:%M:%S"
    __microsecond__ = True

    def get_col_spec(self):
        return "TIME"

    def result_processor(self, dialect):
        def process(value):
            tup = self._cvt(value, dialect)
            return tup and datetime.time(*tup[3:7])
        return process

class SLText(sqltypes.Text):
    def get_col_spec(self):
        return "TEXT"

class SLString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}

class SLChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}

class SLBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BLOB"

class SLBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BOOLEAN"

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return value and 1 or 0
        return process

    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return value and True or False
        return process

colspecs = {
    sqltypes.Binary: SLBinary,
    sqltypes.Boolean: SLBoolean,
    sqltypes.CHAR: SLChar,
    sqltypes.Date: SLDate,
    sqltypes.DateTime: SLDateTime,
    sqltypes.Float: SLFloat,
    sqltypes.Integer: SLInteger,
    sqltypes.NCHAR: SLChar,
    sqltypes.Numeric: SLNumeric,
    sqltypes.Smallinteger: SLSmallInteger,
    sqltypes.String: SLString,
    sqltypes.Text: SLText,
    sqltypes.Time: SLTime,
}

ischema_names = {
    'BLOB': SLBinary,
    'BOOL': SLBoolean,
    'BOOLEAN': SLBoolean,
    'CHAR': SLChar,
    'DATE': SLDate,
    'DATETIME': SLDateTime,
    'DECIMAL': SLNumeric,
    'FLOAT': SLNumeric,
    'INT': SLInteger,
    'INTEGER': SLInteger,
    'NUMERIC': SLNumeric,
    'REAL': SLNumeric,
    'SMALLINT': SLSmallInteger,
    'TEXT': SLText,
    'TIME': SLTime,
    'TIMESTAMP': SLDateTime,
    'VARCHAR': SLString,
}

def descriptor():
    return {'name':'sqlite',
    'description':'SQLite',
    'arguments':[
        ('database', "Database Filename",None)
    ]}

class SQLiteExecutionContext(default.DefaultExecutionContext):
    def post_exec(self):
        if self.compiled.isinsert and not self.executemany:
            if not len(self._last_inserted_ids) or self._last_inserted_ids[0] is None:
                self._last_inserted_ids = [self.cursor.lastrowid] + self._last_inserted_ids[1:]

    def returns_rows_text(self, statement):
        return SELECT_REGEXP.match(statement)

class SQLiteDialect(default.DefaultDialect):
    supports_alter = False
    supports_unicode_statements = True
    default_paramstyle = 'qmark'

    def __init__(self, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)
        def vers(num):
            return tuple([int(x) for x in num.split('.')])
        if self.dbapi is not None:
            sqlite_ver = self.dbapi.version_info
            if sqlite_ver < (2,1,'3'):
                util.warn(
                    ("The installed version of pysqlite2 (%s) is out-dated "
                     "and will cause errors in some cases.  Version 2.1.3 "
                     "or greater is recommended.") %
                    '.'.join([str(subver) for subver in sqlite_ver]))
        self.supports_cast = (self.dbapi is None or vers(self.dbapi.sqlite_version) >= vers("3.2.3"))

    def dbapi(cls):
        try:
            from pysqlite2 import dbapi2 as sqlite
        except ImportError, e:
            try:
                from sqlite3 import dbapi2 as sqlite #try the 2.5+ stdlib name.
            except ImportError:
                raise e
        return sqlite
    dbapi = classmethod(dbapi)

    def server_version_info(self, connection):
        return self.dbapi.sqlite_version_info

    def create_connect_args(self, url):
        if url.username or url.password or url.host or url.port:
            raise exceptions.ArgumentError(
                "Invalid SQLite URL: %s\n"
                "Valid SQLite URL forms are:\n"
                " sqlite:///:memory: (or, sqlite://)\n"
                " sqlite:///relative/path/to/file.db\n"
                " sqlite:////absolute/path/to/file.db" % (url,))
        filename = url.database or ':memory:'

        opts = url.query.copy()
        util.coerce_kw_type(opts, 'timeout', float)
        util.coerce_kw_type(opts, 'isolation_level', str)
        util.coerce_kw_type(opts, 'detect_types', int)
        util.coerce_kw_type(opts, 'check_same_thread', bool)
        util.coerce_kw_type(opts, 'cached_statements', int)

        return ([filename], opts)

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def create_execution_context(self, connection, **kwargs):
        return SQLiteExecutionContext(self, connection, **kwargs)

    def oid_column_name(self, column):
        return "oid"

    def is_disconnect(self, e):
        return isinstance(e, self.dbapi.ProgrammingError) and "Cannot operate on a closed database." in str(e)

    def table_names(self, connection, schema):
        if schema is not None:
            qschema = self.identifier_preparer.quote_identifier(schema)
            master = '%s.sqlite_master' % qschema
            s = ("SELECT name FROM %s "
                 "WHERE type='table' ORDER BY name") % (master,)
            rs = connection.execute(s)
        else:
            try:
                s = ("SELECT name FROM "
                     " (SELECT * FROM sqlite_master UNION ALL "
                     "  SELECT * FROM sqlite_temp_master) "
                     "WHERE type='table' ORDER BY name")
                rs = connection.execute(s)
            except exceptions.DBAPIError:
                raise
                s = ("SELECT name FROM sqlite_master "
                     "WHERE type='table' ORDER BY name")
                rs = connection.execute(s)

        return [row[0] for row in rs]

    def has_table(self, connection, table_name, schema=None):
        quote = self.identifier_preparer.quote_identifier
        if schema is not None:
            pragma = "PRAGMA %s." % quote(schema)
        else:
            pragma = "PRAGMA "
        qtable = quote(table_name)
        cursor = connection.execute("%stable_info(%s)" % (pragma, qtable))
        row = cursor.fetchone()

        # consume remaining rows, to work around
        # http://www.sqlite.org/cvstrac/tktview?tn=1884
        while cursor.fetchone() is not None:
            pass

        return (row is not None)

    def reflecttable(self, connection, table, include_columns):
        preparer = self.identifier_preparer
        if table.schema is None:
            pragma = "PRAGMA "
        else:
            pragma = "PRAGMA %s." % preparer.quote_identifier(table.schema)
        qtable = preparer.format_table(table, False)

        c = connection.execute("%stable_info(%s)" % (pragma, qtable))
        found_table = False
        while True:
            row = c.fetchone()
            if row is None:
                break

            found_table = True
            (name, type_, nullable, has_default, primary_key) = (row[1], row[2].upper(), not row[3], row[4] is not None, row[5])
            name = re.sub(r'^\"|\"$', '', name)
            if include_columns and name not in include_columns:
                continue
            match = re.match(r'(\w+)(\(.*?\))?', type_)
            if match:
                coltype = match.group(1)
                args = match.group(2)
            else:
                coltype = "VARCHAR"
                args = ''

            try:
                coltype = ischema_names[coltype]
            except KeyError:
                util.warn("Did not recognize type '%s' of column '%s'" %
                          (coltype, name))
                coltype = sqltypes.NullType

            if args is not None:
                args = re.findall(r'(\d+)', args)
                coltype = coltype(*[int(a) for a in args])

            colargs= []
            if has_default:
                colargs.append(PassiveDefault('?'))
            table.append_column(schema.Column(name, coltype, primary_key = primary_key, nullable = nullable, *colargs))

        if not found_table:
            raise exceptions.NoSuchTableError(table.name)

        c = connection.execute("%sforeign_key_list(%s)" % (pragma, qtable))
        fks = {}
        while True:
            row = c.fetchone()
            if row is None:
                break
            (constraint_name, tablename, localcol, remotecol) = (row[0], row[2], row[3], row[4])
            tablename = re.sub(r'^\"|\"$', '', tablename)
            localcol = re.sub(r'^\"|\"$', '', localcol)
            remotecol = re.sub(r'^\"|\"$', '', remotecol)
            try:
                fk = fks[constraint_name]
            except KeyError:
                fk = ([],[])
                fks[constraint_name] = fk

            # look up the table based on the given table's engine, not 'self',
            # since it could be a ProxyEngine
            remotetable = schema.Table(tablename, table.metadata, autoload=True, autoload_with=connection)
            constrained_column = table.c[localcol].name
            refspec = ".".join([tablename, remotecol])
            if constrained_column not in fk[0]:
                fk[0].append(constrained_column)
            if refspec not in fk[1]:
                fk[1].append(refspec)
        for name, value in fks.iteritems():
            table.append_constraint(schema.ForeignKeyConstraint(value[0], value[1]))
        # check for UNIQUE indexes
        c = connection.execute("%sindex_list(%s)" % (pragma, qtable))
        unique_indexes = []
        while True:
            row = c.fetchone()
            if row is None:
                break
            if (row[2] == 1):
                unique_indexes.append(row[1])
        # loop thru unique indexes for one that includes the primary key
        for idx in unique_indexes:
            c = connection.execute("%sindex_info(%s)" % (pragma, idx))
            cols = []
            while True:
                row = c.fetchone()
                if row is None:
                    break
                cols.append(row[2])


class SQLiteCompiler(compiler.DefaultCompiler):
    functions = compiler.DefaultCompiler.functions.copy()
    functions.update (
        {
            sql_functions.now: 'CURRENT_TIMESTAMP'
        }
    )

    def visit_cast(self, cast, **kwargs):
        if self.dialect.supports_cast:
            return super(SQLiteCompiler, self).visit_cast(cast)
        else:
            return self.process(cast.clause)

    def limit_clause(self, select):
        text = ""
        if select._limit is not None:
            text +=  " \n LIMIT " + str(select._limit)
        if select._offset is not None:
            if select._limit is None:
                text += " \n LIMIT -1"
            text += " OFFSET " + str(select._offset)
        else:
            text += " OFFSET 0"
        return text

    def for_update_clause(self, select):
        # sqlite has no "FOR UPDATE" AFAICT
        return ''

    def visit_insert(self, insert_stmt):
        self.isinsert = True
        colparams = self._get_colparams(insert_stmt)
        preparer = self.preparer

        if not colparams:
            return "INSERT INTO %s DEFAULT VALUES" % (
                (preparer.format_table(insert_stmt.table),))
        else:
            return ("INSERT INTO %s (%s) VALUES (%s)" %
                    (preparer.format_table(insert_stmt.table),
                     ', '.join([preparer.format_column(c[0])
                                for c in colparams]),
                     ', '.join([c[1] for c in colparams])))


class SQLiteSchemaGenerator(compiler.SchemaGenerator):

    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column) + " " + column.type.dialect_impl(self.dialect, _for_ddl=column).get_col_spec()
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    # this doesnt seem to be needed, although i suspect older versions of sqlite might still
    # not directly support composite primary keys
    #def visit_primary_key_constraint(self, constraint):
    #    if len(constraint) > 1:
    #        self.append(", \n")
    #        # put all PRIMARY KEYS in a UNIQUE index
    #        self.append("\tUNIQUE (%s)" % string.join([c.name for c in constraint],', '))
    #    else:
    #        super(SQLiteSchemaGenerator, self).visit_primary_key_constraint(constraint)

class SQLiteSchemaDropper(compiler.SchemaDropper):
    pass

class SQLiteIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = util.Set([
        'add', 'after', 'all', 'alter', 'analyze', 'and', 'as', 'asc',
        'attach', 'autoincrement', 'before', 'begin', 'between', 'by',
        'cascade', 'case', 'cast', 'check', 'collate', 'column', 'commit',
        'conflict', 'constraint', 'create', 'cross', 'current_date',
        'current_time', 'current_timestamp', 'database', 'default',
        'deferrable', 'deferred', 'delete', 'desc', 'detach', 'distinct',
        'drop', 'each', 'else', 'end', 'escape', 'except', 'exclusive',
        'explain', 'false', 'fail', 'for', 'foreign', 'from', 'full', 'glob',
        'group', 'having', 'if', 'ignore', 'immediate', 'in', 'index',
        'initially', 'inner', 'insert', 'instead', 'intersect', 'into', 'is',
        'isnull', 'join', 'key', 'left', 'like', 'limit', 'match', 'natural',
        'not', 'notnull', 'null', 'of', 'offset', 'on', 'or', 'order', 'outer',
        'plan', 'pragma', 'primary', 'query', 'raise', 'references',
        'reindex', 'rename', 'replace', 'restrict', 'right', 'rollback',
        'row', 'select', 'set', 'table', 'temp', 'temporary', 'then', 'to',
        'transaction', 'trigger', 'true', 'union', 'unique', 'update', 'using',
        'vacuum', 'values', 'view', 'virtual', 'when', 'where',
        ])

    def __init__(self, dialect):
        super(SQLiteIdentifierPreparer, self).__init__(dialect)

dialect = SQLiteDialect
dialect.poolclass = pool.SingletonThreadPool
dialect.statement_compiler = SQLiteCompiler
dialect.schemagenerator = SQLiteSchemaGenerator
dialect.schemadropper = SQLiteSchemaDropper
dialect.preparer = SQLiteIdentifierPreparer
