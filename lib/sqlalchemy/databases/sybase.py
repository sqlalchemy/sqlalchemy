# sybase.py
# Copyright (C) 2007 Fisch Asset Management AG http://www.fam.ch
# Coding: Alexander Houben alexander.houben@thor-solutions.ch
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Sybase database backend.

Known issues / TODO:

 * Uses the mx.ODBC driver from egenix (version 2.1.0)
 * The current version of sqlalchemy.databases.sybase only supports
   mx.ODBC.Windows (other platforms such as mx.ODBC.unixODBC still need
   some development)
 * Support for pyodbc has been built in but is not yet complete (needs
   further development)
 * Results of running tests/alltests.py:
     Ran 934 tests in 287.032s
     FAILED (failures=3, errors=1)
 * Tested on 'Adaptive Server Anywhere 9' (version 9.0.1.1751)
"""

import datetime, operator

from sqlalchemy import util, sql, schema, exc
from sqlalchemy.sql import compiler, expression
from sqlalchemy.engine import default, base
from sqlalchemy import types as sqltypes
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import String, Integer, SMALLINT, CHAR, ForeignKey


__all__ = [
    'SybaseTypeError'
    'SybaseNumeric', 'SybaseFloat', 'SybaseInteger', 'SybaseBigInteger',
    'SybaseTinyInteger', 'SybaseSmallInteger',
    'SybaseDateTime_mxodbc', 'SybaseDateTime_pyodbc',
    'SybaseDate_mxodbc', 'SybaseDate_pyodbc',
    'SybaseTime_mxodbc', 'SybaseTime_pyodbc',
    'SybaseText', 'SybaseString', 'SybaseChar', 'SybaseBinary',
    'SybaseBoolean', 'SybaseTimeStamp', 'SybaseMoney', 'SybaseSmallMoney',
    'SybaseUniqueIdentifier',
    ]


RESERVED_WORDS = set([
    "add", "all", "alter", "and",
    "any", "as", "asc", "backup",
    "begin", "between", "bigint", "binary",
    "bit", "bottom", "break", "by",
    "call", "capability", "cascade", "case",
    "cast", "char", "char_convert", "character",
    "check", "checkpoint", "close", "comment",
    "commit", "connect", "constraint", "contains",
    "continue", "convert", "create", "cross",
    "cube", "current", "current_timestamp", "current_user",
    "cursor", "date", "dbspace", "deallocate",
    "dec", "decimal", "declare", "default",
    "delete", "deleting", "desc", "distinct",
    "do", "double", "drop", "dynamic",
    "else", "elseif", "encrypted", "end",
    "endif", "escape", "except", "exception",
    "exec", "execute", "existing", "exists",
    "externlogin", "fetch", "first", "float",
    "for", "force", "foreign", "forward",
    "from", "full", "goto", "grant",
    "group", "having", "holdlock", "identified",
    "if", "in", "index", "index_lparen",
    "inner", "inout", "insensitive", "insert",
    "inserting", "install", "instead", "int",
    "integer", "integrated", "intersect", "into",
    "iq", "is", "isolation", "join",
    "key", "lateral", "left", "like",
    "lock", "login", "long", "match",
    "membership", "message", "mode", "modify",
    "natural", "new", "no", "noholdlock",
    "not", "notify", "null", "numeric",
    "of", "off", "on", "open",
    "option", "options", "or", "order",
    "others", "out", "outer", "over",
    "passthrough", "precision", "prepare", "primary",
    "print", "privileges", "proc", "procedure",
    "publication", "raiserror", "readtext", "real",
    "reference", "references", "release", "remote",
    "remove", "rename", "reorganize", "resource",
    "restore", "restrict", "return", "revoke",
    "right", "rollback", "rollup", "save",
    "savepoint", "scroll", "select", "sensitive",
    "session", "set", "setuser", "share",
    "smallint", "some", "sqlcode", "sqlstate",
    "start", "stop", "subtrans", "subtransaction",
    "synchronize", "syntax_error", "table", "temporary",
    "then", "time", "timestamp", "tinyint",
    "to", "top", "tran", "trigger",
    "truncate", "tsequal", "unbounded", "union",
    "unique", "unknown", "unsigned", "update",
    "updating", "user", "using", "validate",
    "values", "varbinary", "varchar", "variable",
    "varying", "view", "wait", "waitfor",
    "when", "where", "while", "window",
    "with", "with_cube", "with_lparen", "with_rollup",
    "within", "work", "writetext",
    ])

ischema = MetaData()

tables = Table("SYSTABLE", ischema,
    Column("table_id", Integer, primary_key=True),
    Column("file_id", SMALLINT),
    Column("table_name", CHAR(128)),
    Column("table_type", CHAR(10)),
    Column("creator", Integer),
    #schema="information_schema"
    )

domains = Table("SYSDOMAIN", ischema,
    Column("domain_id", Integer, primary_key=True),
    Column("domain_name", CHAR(128)),
    Column("type_id", SMALLINT),
    Column("precision", SMALLINT, quote=True),
    #schema="information_schema"
    )

columns = Table("SYSCOLUMN", ischema,
    Column("column_id", Integer, primary_key=True),
    Column("table_id", Integer, ForeignKey(tables.c.table_id)),
    Column("pkey", CHAR(1)),
    Column("column_name", CHAR(128)),
    Column("nulls", CHAR(1)),
    Column("width", SMALLINT),
    Column("domain_id", SMALLINT, ForeignKey(domains.c.domain_id)),
    # FIXME: should be mx.BIGINT
    Column("max_identity", Integer),
    # FIXME: should be mx.ODBC.Windows.LONGVARCHAR
    Column("default", String),
    Column("scale", Integer),
    #schema="information_schema"
    )

foreignkeys = Table("SYSFOREIGNKEY", ischema,
    Column("foreign_table_id", Integer, ForeignKey(tables.c.table_id), primary_key=True),
    Column("foreign_key_id", SMALLINT, primary_key=True),
    Column("primary_table_id", Integer, ForeignKey(tables.c.table_id)),
    #schema="information_schema"
    )
fkcols = Table("SYSFKCOL", ischema,
    Column("foreign_table_id", Integer, ForeignKey(columns.c.table_id), primary_key=True),
    Column("foreign_key_id", SMALLINT, ForeignKey(foreignkeys.c.foreign_key_id), primary_key=True),
    Column("foreign_column_id", Integer, ForeignKey(columns.c.column_id), primary_key=True),
    Column("primary_column_id", Integer),
    #schema="information_schema"
    )

class SybaseTypeError(sqltypes.TypeEngine):
    def result_processor(self, dialect):
        return None

    def bind_processor(self, dialect):
        def process(value):
            raise exc.InvalidRequestError("Data type not supported", [value])
        return process

    def get_col_spec(self):
        raise exc.CompileError("Data type not supported")

class SybaseNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        if self.scale is None:
            if self.precision is None:
                return "NUMERIC"
            else:
                return "NUMERIC(%(precision)s)" % {'precision' : self.precision}
        else:
            return "NUMERIC(%(precision)s, %(scale)s)" % {'precision': self.precision, 'scale' : self.scale}

class SybaseFloat(sqltypes.FLOAT, SybaseNumeric):
    def __init__(self, precision = 10, asdecimal = False, scale = 2, **kwargs):
        super(sqltypes.FLOAT, self).__init__(precision, asdecimal, **kwargs)
        self.scale = scale

    def get_col_spec(self):
        # if asdecimal is True, handle same way as SybaseNumeric
        if self.asdecimal:
            return SybaseNumeric.get_col_spec(self)
        if self.precision is None:
            return "FLOAT"
        else:
            return "FLOAT(%(precision)s)" % {'precision': self.precision}

    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return float(value)
        if self.asdecimal:
            return SybaseNumeric.result_processor(self, dialect)
        return process

class SybaseInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"

class SybaseBigInteger(SybaseInteger):
    def get_col_spec(self):
        return "BIGINT"

class SybaseTinyInteger(SybaseInteger):
    def get_col_spec(self):
        return "TINYINT"

class SybaseSmallInteger(SybaseInteger):
    def get_col_spec(self):
        return "SMALLINT"

class SybaseDateTime_mxodbc(sqltypes.DateTime):
    def __init__(self, *a, **kw):
        super(SybaseDateTime_mxodbc, self).__init__(False)

    def get_col_spec(self):
        return "DATETIME"

class SybaseDateTime_pyodbc(sqltypes.DateTime):
    def __init__(self, *a, **kw):
        super(SybaseDateTime_pyodbc, self).__init__(False)

    def get_col_spec(self):
        return "DATETIME"

    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            # Convert the datetime.datetime back to datetime.time
            return value
        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return value
        return process

class SybaseDate_mxodbc(sqltypes.Date):
    def __init__(self, *a, **kw):
        super(SybaseDate_mxodbc, self).__init__(False)

    def get_col_spec(self):
        return "DATE"

class SybaseDate_pyodbc(sqltypes.Date):
    def __init__(self, *a, **kw):
        super(SybaseDate_pyodbc, self).__init__(False)

    def get_col_spec(self):
        return "DATE"

class SybaseTime_mxodbc(sqltypes.Time):
    def __init__(self, *a, **kw):
        super(SybaseTime_mxodbc, self).__init__(False)

    def get_col_spec(self):
        return "DATETIME"

    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            # Convert the datetime.datetime back to datetime.time
            return datetime.time(value.hour, value.minute, value.second, value.microsecond)
        return process

class SybaseTime_pyodbc(sqltypes.Time):
    def __init__(self, *a, **kw):
        super(SybaseTime_pyodbc, self).__init__(False)

    def get_col_spec(self):
        return "DATETIME"

    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            # Convert the datetime.datetime back to datetime.time
            return datetime.time(value.hour, value.minute, value.second, value.microsecond)
        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return datetime.datetime(1970, 1, 1, value.hour, value.minute, value.second, value.microsecond)
        return process

class SybaseText(sqltypes.Text):
    def get_col_spec(self):
        return "TEXT"

class SybaseString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}

class SybaseChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}

class SybaseBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "IMAGE"

class SybaseBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BIT"

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

class SybaseTimeStamp(sqltypes.TIMESTAMP):
    def get_col_spec(self):
        return "TIMESTAMP"

class SybaseMoney(sqltypes.TypeEngine):
    def get_col_spec(self):
        return "MONEY"

class SybaseSmallMoney(SybaseMoney):
    def get_col_spec(self):
        return "SMALLMONEY"

class SybaseUniqueIdentifier(sqltypes.TypeEngine):
    def get_col_spec(self):
        return "UNIQUEIDENTIFIER"

class SybaseSQLExecutionContext(default.DefaultExecutionContext):
    pass

class SybaseSQLExecutionContext_mxodbc(SybaseSQLExecutionContext):

    def __init__(self, dialect, connection, compiled=None, statement=None, parameters=None):
        super(SybaseSQLExecutionContext_mxodbc, self).__init__(dialect, connection, compiled, statement, parameters)

    def pre_exec(self):
        super(SybaseSQLExecutionContext_mxodbc, self).pre_exec()

    def post_exec(self):
        if self.compiled.isinsert:
            table = self.compiled.statement.table
            # get the inserted values of the primary key

            # get any sequence IDs first (using @@identity)
            self.cursor.execute("SELECT @@identity AS lastrowid")
            row = self.cursor.fetchone()
            lastrowid = int(row[0])
            if lastrowid > 0:
                # an IDENTITY was inserted, fetch it
                # FIXME: always insert in front ? This only works if the IDENTITY is the first column, no ?!
                if not hasattr(self, '_last_inserted_ids') or self._last_inserted_ids is None:
                    self._last_inserted_ids = [lastrowid]
                else:
                    self._last_inserted_ids = [lastrowid] + self._last_inserted_ids[1:]
        super(SybaseSQLExecutionContext_mxodbc, self).post_exec()

class SybaseSQLExecutionContext_pyodbc(SybaseSQLExecutionContext):
    def __init__(self, dialect, connection, compiled=None, statement=None, parameters=None):
        super(SybaseSQLExecutionContext_pyodbc, self).__init__(dialect, connection, compiled, statement, parameters)

    def pre_exec(self):
        super(SybaseSQLExecutionContext_pyodbc, self).pre_exec()

    def post_exec(self):
        if self.compiled.isinsert:
            table = self.compiled.statement.table
            # get the inserted values of the primary key

            # get any sequence IDs first (using @@identity)
            self.cursor.execute("SELECT @@identity AS lastrowid")
            row = self.cursor.fetchone()
            lastrowid = int(row[0])
            if lastrowid > 0:
                # an IDENTITY was inserted, fetch it
                # FIXME: always insert in front ? This only works if the IDENTITY is the first column, no ?!
                if not hasattr(self, '_last_inserted_ids') or self._last_inserted_ids is None:
                    self._last_inserted_ids = [lastrowid]
                else:
                    self._last_inserted_ids = [lastrowid] + self._last_inserted_ids[1:]
        super(SybaseSQLExecutionContext_pyodbc, self).post_exec()

class SybaseSQLDialect(default.DefaultDialect):
    colspecs = {
        # FIXME: unicode support
        #sqltypes.Unicode : SybaseUnicode,
        sqltypes.Integer : SybaseInteger,
        sqltypes.SmallInteger : SybaseSmallInteger,
        sqltypes.Numeric : SybaseNumeric,
        sqltypes.Float : SybaseFloat,
        sqltypes.String : SybaseString,
        sqltypes.Binary : SybaseBinary,
        sqltypes.Boolean : SybaseBoolean,
        sqltypes.Text : SybaseText,
        sqltypes.CHAR : SybaseChar,
        sqltypes.TIMESTAMP : SybaseTimeStamp,
        sqltypes.FLOAT : SybaseFloat,
    }

    ischema_names = {
        'integer' : SybaseInteger,
        'unsigned int' : SybaseInteger,
        'unsigned smallint' : SybaseInteger,
        'unsigned bigint' : SybaseInteger,
        'bigint': SybaseBigInteger,
        'smallint' : SybaseSmallInteger,
        'tinyint' : SybaseTinyInteger,
        'varchar' : SybaseString,
        'long varchar' : SybaseText,
        'char' : SybaseChar,
        'decimal' : SybaseNumeric,
        'numeric' : SybaseNumeric,
        'float' : SybaseFloat,
        'double' : SybaseFloat,
        'binary' : SybaseBinary,
        'long binary' : SybaseBinary,
        'varbinary' : SybaseBinary,
        'bit': SybaseBoolean,
        'image' : SybaseBinary,
        'timestamp': SybaseTimeStamp,
        'money': SybaseMoney,
        'smallmoney': SybaseSmallMoney,
        'uniqueidentifier': SybaseUniqueIdentifier,

        'java.lang.Object' : SybaseTypeError,
        'java serialization' : SybaseTypeError,
    }

    name = 'sybase'
    # Sybase backend peculiarities
    supports_unicode_statements = False
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    execution_ctx_cls = SybaseSQLExecutionContext
    
    def __new__(cls, dbapi=None, *args, **kwargs):
        if cls != SybaseSQLDialect:
            return super(SybaseSQLDialect, cls).__new__(cls, *args, **kwargs)
        if dbapi:
            print dbapi.__name__
            dialect = dialect_mapping.get(dbapi.__name__)
            return dialect(*args, **kwargs)
        else:
            return object.__new__(cls, *args, **kwargs)

    def __init__(self, **params):
        super(SybaseSQLDialect, self).__init__(**params)
        self.text_as_varchar = False
        # FIXME: what is the default schema for sybase connections (DBA?) ?
        self.set_default_schema_name("dba")

    def dbapi(cls, module_name=None):
        if module_name:
            try:
                dialect_cls = dialect_mapping[module_name]
                return dialect_cls.import_dbapi()
            except KeyError:
                raise exc.InvalidRequestError("Unsupported SybaseSQL module '%s' requested (must be " + " or ".join([x for x in dialect_mapping.keys()]) + ")" % module_name)
        else:
            for dialect_cls in dialect_mapping.values():
                try:
                    return dialect_cls.import_dbapi()
                except ImportError, e:
                    pass
            else:
                raise ImportError('No DBAPI module detected for SybaseSQL - please install mxodbc')
    dbapi = classmethod(dbapi)

    def type_descriptor(self, typeobj):
        newobj = sqltypes.adapt_type(typeobj, self.colspecs)
        return newobj

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def get_default_schema_name(self, connection):
        return self.schema_name

    def set_default_schema_name(self, schema_name):
        self.schema_name = schema_name

    def do_execute(self, cursor, statement, params, **kwargs):
        params = tuple(params)
        super(SybaseSQLDialect, self).do_execute(cursor, statement, params, **kwargs)

    # FIXME: remove ?
    def _execute(self, c, statement, parameters):
        try:
            if parameters == {}:
                parameters = ()
            c.execute(statement, parameters)
            self.context.rowcount = c.rowcount
            c.DBPROP_COMMITPRESERVE = "Y"
        except Exception, e:
            raise exc.DBAPIError.instance(statement, parameters, e)

    def table_names(self, connection, schema):
        """Ignore the schema and the charset for now."""
        s = sql.select([tables.c.table_name],
                       sql.not_(tables.c.table_name.like("SYS%")) and
                       tables.c.creator >= 100
                       )
        rp = connection.execute(s)
        return [row[0] for row in rp.fetchall()]

    def has_table(self, connection, tablename, schema=None):
        # FIXME: ignore schemas for sybase
        s = sql.select([tables.c.table_name], tables.c.table_name == tablename)

        c = connection.execute(s)
        row = c.fetchone()
        print "has_table: " + tablename + ": " + str(bool(row is not None))
        return row is not None

    def reflecttable(self, connection, table, include_columns):
        # Get base columns
        if table.schema is not None:
            current_schema = table.schema
        else:
            current_schema = self.get_default_schema_name(connection)

        s = sql.select([columns, domains], tables.c.table_name==table.name, from_obj=[columns.join(tables).join(domains)], order_by=[columns.c.column_id])

        c = connection.execute(s)
        found_table = False
        # makes sure we append the columns in the correct order
        while True:
            row = c.fetchone()
            if row is None:
                break
            found_table = True
            (name, type, nullable, charlen, numericprec, numericscale, default, primary_key, max_identity, table_id, column_id) = (
                row[columns.c.column_name],
                row[domains.c.domain_name],
                row[columns.c.nulls] == 'Y',
                row[columns.c.width],
                row[domains.c.precision],
                row[columns.c.scale],
                row[columns.c.default],
                row[columns.c.pkey] == 'Y',
                row[columns.c.max_identity],
                row[tables.c.table_id],
                row[columns.c.column_id],
            )
            if include_columns and name not in include_columns:
                continue

            # FIXME: else problems with SybaseBinary(size)
            if numericscale == 0:
                numericscale = None

            args = []
            for a in (charlen, numericprec, numericscale):
                if a is not None:
                    args.append(a)
            coltype = self.ischema_names.get(type, None)
            if coltype == SybaseString and charlen == -1:
                coltype = SybaseText()
            else:
                if coltype is None:
                    util.warn("Did not recognize type '%s' of column '%s'" %
                              (type, name))
                    coltype = sqltypes.NULLTYPE
                coltype = coltype(*args)
            colargs = []
            if default is not None:
                colargs.append(schema.DefaultClause(sql.text(default)))

            # any sequences ?
            col = schema.Column(name, coltype, nullable=nullable, primary_key=primary_key, *colargs)
            if int(max_identity) > 0:
                col.sequence = schema.Sequence(name + '_identity')
                col.sequence.start = int(max_identity)
                col.sequence.increment = 1

            # append the column
            table.append_column(col)

        # any foreign key constraint for this table ?
        # note: no multi-column foreign keys are considered
        s = "select st1.table_name, sc1.column_name, st2.table_name, sc2.column_name from systable as st1 join sysfkcol on st1.table_id=sysfkcol.foreign_table_id join sysforeignkey join systable as st2 on sysforeignkey.primary_table_id = st2.table_id join syscolumn as sc1 on sysfkcol.foreign_column_id=sc1.column_id and sc1.table_id=st1.table_id join syscolumn as sc2 on sysfkcol.primary_column_id=sc2.column_id and sc2.table_id=st2.table_id where st1.table_name='%(table_name)s';" % { 'table_name' : table.name }
        c = connection.execute(s)
        foreignKeys = {}
        while True:
            row = c.fetchone()
            if row is None:
                break
            (foreign_table, foreign_column, primary_table, primary_column) = (
                row[0], row[1], row[2], row[3],
            )
            if not primary_table in foreignKeys.keys():
                foreignKeys[primary_table] = [['%s' % (foreign_column)], ['%s.%s'%(primary_table, primary_column)]]
            else:
                foreignKeys[primary_table][0].append('%s'%(foreign_column))
                foreignKeys[primary_table][1].append('%s.%s'%(primary_table, primary_column))
        for primary_table in foreignKeys.keys():
            #table.append_constraint(schema.ForeignKeyConstraint(['%s.%s'%(foreign_table, foreign_column)], ['%s.%s'%(primary_table,primary_column)]))
            table.append_constraint(schema.ForeignKeyConstraint(foreignKeys[primary_table][0], foreignKeys[primary_table][1], link_to_name=True))

        if not found_table:
            raise exc.NoSuchTableError(table.name)


class SybaseSQLDialect_mxodbc(SybaseSQLDialect):
    execution_ctx_cls = SybaseSQLExecutionContext_mxodbc
    
    def __init__(self, **params):
        super(SybaseSQLDialect_mxodbc, self).__init__(**params)

        self.dbapi_type_map = {'getdate' : SybaseDate_mxodbc()}

    def import_dbapi(cls):
        #import mx.ODBC.Windows as module
        import mxODBC as module
        return module
    import_dbapi = classmethod(import_dbapi)

    colspecs = SybaseSQLDialect.colspecs.copy()
    colspecs[sqltypes.Time] = SybaseTime_mxodbc
    colspecs[sqltypes.Date] = SybaseDate_mxodbc
    colspecs[sqltypes.DateTime] = SybaseDateTime_mxodbc

    ischema_names = SybaseSQLDialect.ischema_names.copy()
    ischema_names['time'] = SybaseTime_mxodbc
    ischema_names['date'] = SybaseDate_mxodbc
    ischema_names['datetime'] = SybaseDateTime_mxodbc
    ischema_names['smalldatetime'] = SybaseDateTime_mxodbc

    def is_disconnect(self, e):
        # FIXME: optimize
        #return isinstance(e, self.dbapi.Error) and '[08S01]' in str(e)
        #return True
        return False

    def do_execute(self, cursor, statement, parameters, context=None, **kwargs):
        super(SybaseSQLDialect_mxodbc, self).do_execute(cursor, statement, parameters, context=context, **kwargs)

    def create_connect_args(self, url):
        '''Return a tuple of *args,**kwargs'''
        # FIXME: handle mx.odbc.Windows proprietary args
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        argsDict = {}
        argsDict['user'] = opts['user']
        argsDict['password'] = opts['password']
        connArgs = [[opts['dsn']], argsDict]
        return connArgs


class SybaseSQLDialect_pyodbc(SybaseSQLDialect):
    execution_ctx_cls = SybaseSQLExecutionContext_pyodbc
    
    def __init__(self, **params):
        super(SybaseSQLDialect_pyodbc, self).__init__(**params)
        self.dbapi_type_map = {'getdate' : SybaseDate_pyodbc()}

    def import_dbapi(cls):
        import mypyodbc as module
        return module
    import_dbapi = classmethod(import_dbapi)

    colspecs = SybaseSQLDialect.colspecs.copy()
    colspecs[sqltypes.Time] = SybaseTime_pyodbc
    colspecs[sqltypes.Date] = SybaseDate_pyodbc
    colspecs[sqltypes.DateTime] = SybaseDateTime_pyodbc

    ischema_names = SybaseSQLDialect.ischema_names.copy()
    ischema_names['time'] = SybaseTime_pyodbc
    ischema_names['date'] = SybaseDate_pyodbc
    ischema_names['datetime'] = SybaseDateTime_pyodbc
    ischema_names['smalldatetime'] = SybaseDateTime_pyodbc

    def is_disconnect(self, e):
        # FIXME: optimize
        #return isinstance(e, self.dbapi.Error) and '[08S01]' in str(e)
        #return True
        return False

    def do_execute(self, cursor, statement, parameters, context=None, **kwargs):
        super(SybaseSQLDialect_pyodbc, self).do_execute(cursor, statement, parameters, context=context, **kwargs)

    def create_connect_args(self, url):
        '''Return a tuple of *args,**kwargs'''
        # FIXME: handle pyodbc proprietary args
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)

        self.autocommit = False
        if 'autocommit' in opts:
            self.autocommit = bool(int(opts.pop('autocommit')))

        argsDict = {}
        argsDict['UID'] = opts['user']
        argsDict['PWD'] = opts['password']
        argsDict['DSN'] = opts['dsn']
        connArgs = [[';'.join(["%s=%s"%(key, argsDict[key]) for key in argsDict])], {'autocommit' : self.autocommit}]
        return connArgs


dialect_mapping = {
    'sqlalchemy.databases.mxODBC' : SybaseSQLDialect_mxodbc,
#    'pyodbc' : SybaseSQLDialect_pyodbc,
    }


class SybaseSQLCompiler(compiler.DefaultCompiler):
    operators = compiler.DefaultCompiler.operators.copy()
    operators.update({
        sql_operators.mod: lambda x, y: "MOD(%s, %s)" % (x, y),
    })

    extract_map = compiler.DefaultCompiler.extract_map.copy()
    extract_map.update ({
        'doy': 'dayofyear',
        'dow': 'weekday',
        'milliseconds': 'millisecond'
    })


    def bindparam_string(self, name):
        res = super(SybaseSQLCompiler, self).bindparam_string(name)
        if name.lower().startswith('literal'):
            res = 'STRING(%s)' % res
        return res

    def get_select_precolumns(self, select):
        s = select._distinct and "DISTINCT " or ""
        if select._limit:
            #if select._limit == 1:
                #s += "FIRST "
            #else:
                #s += "TOP %s " % (select._limit,)
            s += "TOP %s " % (select._limit,)
        if select._offset:
            if not select._limit:
                # FIXME: sybase doesn't allow an offset without a limit
                # so use a huge value for TOP here
                s += "TOP 1000000 "
            s += "START AT %s " % (select._offset+1,)
        return s

    def limit_clause(self, select):
        # Limit in sybase is after the select keyword
        return ""

    def visit_binary(self, binary):
        """Move bind parameters to the right-hand side of an operator, where possible."""
        if isinstance(binary.left, expression._BindParamClause) and binary.operator == operator.eq:
            return self.process(expression._BinaryExpression(binary.right, binary.left, binary.operator))
        else:
            return super(SybaseSQLCompiler, self).visit_binary(binary)

    def label_select_column(self, select, column, asfrom):
        if isinstance(column, expression.Function):
            return column.label(None)
        else:
            return super(SybaseSQLCompiler, self).label_select_column(select, column, asfrom)

    function_rewrites =  {'current_date': 'getdate',
                         }
    def visit_function(self, func):
        func.name = self.function_rewrites.get(func.name, func.name)
        res = super(SybaseSQLCompiler, self).visit_function(func)
        if func.name.lower() == 'getdate':
            # apply CAST operator
            # FIXME: what about _pyodbc ?
            cast = expression._Cast(func, SybaseDate_mxodbc)
            # infinite recursion
            # res = self.visit_cast(cast)
            res = "CAST(%s AS %s)" % (res, self.process(cast.typeclause))
        return res

    def visit_extract(self, extract):
        field = self.extract_map.get(extract.field, extract.field)
        return 'DATEPART("%s", %s)' % (field, self.process(extract.expr))

    def for_update_clause(self, select):
        # "FOR UPDATE" is only allowed on "DECLARE CURSOR" which SQLAlchemy doesn't use
        return ''

    def order_by_clause(self, select):
        order_by = self.process(select._order_by_clause)

        # SybaseSQL only allows ORDER BY in subqueries if there is a LIMIT
        if order_by and (not self.is_subquery() or select._limit):
            return " ORDER BY " + order_by
        else:
            return ""


class SybaseSQLSchemaGenerator(compiler.SchemaGenerator):
    def get_column_specification(self, column, **kwargs):

        colspec = self.preparer.format_column(column)

        if (not getattr(column.table, 'has_sequence', False)) and column.primary_key and \
                column.autoincrement and isinstance(column.type, sqltypes.Integer):
            if column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional):
                column.sequence = schema.Sequence(column.name + '_seq')

        if hasattr(column, 'sequence'):
            column.table.has_sequence = column
            #colspec += " numeric(30,0) IDENTITY"
            colspec += " Integer IDENTITY"
        else:
            colspec += " " + column.type.dialect_impl(self.dialect).get_col_spec()

        if not column.nullable:
            colspec += " NOT NULL"

        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        return colspec


class SybaseSQLSchemaDropper(compiler.SchemaDropper):
    def visit_index(self, index):
        self.append("\nDROP INDEX %s.%s" % (
            self.preparer.quote_identifier(index.table.name),
            self.preparer.quote(self._validate_identifier(index.name, False), index.quote)
            ))
        self.execute()


class SybaseSQLDefaultRunner(base.DefaultRunner):
    pass


class SybaseSQLIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS

    def __init__(self, dialect):
        super(SybaseSQLIdentifierPreparer, self).__init__(dialect)

    def _escape_identifier(self, value):
        #TODO: determin SybaseSQL's escapeing rules
        return value

    def _fold_identifier_case(self, value):
        #TODO: determin SybaseSQL's case folding rules
        return value


dialect = SybaseSQLDialect
dialect.statement_compiler = SybaseSQLCompiler
dialect.schemagenerator = SybaseSQLSchemaGenerator
dialect.schemadropper = SybaseSQLSchemaDropper
dialect.preparer = SybaseSQLIdentifierPreparer
dialect.defaultrunner = SybaseSQLDefaultRunner
