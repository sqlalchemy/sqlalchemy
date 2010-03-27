# sybase/base.py
# Copyright (C) 2010 Michael Bayer mike_mp@zzzcomputing.com
# get_select_precolumns(), limit_clause() implementation
# copyright (C) 2007 Fisch Asset Management 
# AG http://www.fam.ch, with coding by Alexander Houben 
# alexander.houben@thor-solutions.ch
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for Sybase Adaptive Server Enterprise (ASE).

Note that this dialect is no longer specific to Sybase iAnywhere.
ASE is the primary support platform.

"""

import operator
from sqlalchemy.sql import compiler, expression, text, bindparam
from sqlalchemy.engine import default, base, reflection
from sqlalchemy import types as sqltypes
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import schema as sa_schema
from sqlalchemy import util, sql, exc

from sqlalchemy.types import CHAR, VARCHAR, TIME, NCHAR, NVARCHAR,\
                            TEXT,DATE,DATETIME, FLOAT, NUMERIC,\
                            BIGINT,INT, INTEGER, SMALLINT, BINARY,\
                            VARBINARY, DECIMAL, TIMESTAMP, Unicode,\
                            UnicodeText

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

        
class _SybaseUnitypeMixin(object):
    """these types appear to return a buffer object."""
    
    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                return str(value) #.decode("ucs-2")
            else:
                return None
        return process
    
class UNICHAR(_SybaseUnitypeMixin, sqltypes.Unicode):
    __visit_name__ = 'UNICHAR'

class UNIVARCHAR(_SybaseUnitypeMixin, sqltypes.Unicode):
    __visit_name__ = 'UNIVARCHAR'

class UNITEXT(_SybaseUnitypeMixin, sqltypes.UnicodeText):
    __visit_name__ = 'UNITEXT'

class TINYINT(sqltypes.Integer):
    __visit_name__ = 'TINYINT'

class BIT(sqltypes.TypeEngine):
    __visit_name__ = 'BIT'
    
class MONEY(sqltypes.TypeEngine):
    __visit_name__ = "MONEY"

class SMALLMONEY(sqltypes.TypeEngine):
    __visit_name__ = "SMALLMONEY"

class UNIQUEIDENTIFIER(sqltypes.TypeEngine):
    __visit_name__ = "UNIQUEIDENTIFIER"
  
class IMAGE(sqltypes.LargeBinary):
    __visit_name__ = 'IMAGE'
 

class SybaseTypeCompiler(compiler.GenericTypeCompiler):
    def visit_large_binary(self, type_):
        return self.visit_IMAGE(type_)
    
    def visit_boolean(self, type_):
        return self.visit_BIT(type_)

    def visit_unicode(self, type_):
        return self.visit_NVARCHAR(type_)

    def visit_UNICHAR(self, type_):
        return "UNICHAR(%d)" % type_.length

    def visit_UNIVARCHAR(self, type_):
        return "UNIVARCHAR(%d)" % type_.length

    def visit_UNITEXT(self, type_):
        return "UNITEXT"

    def visit_TINYINT(self, type_):
        return "TINYINT"
        
    def visit_IMAGE(self, type_):
        return "IMAGE"

    def visit_BIT(self, type_):
        return "BIT"

    def visit_MONEY(self, type_):
        return "MONEY"
    
    def visit_SMALLMONEY(self, type_):
        return "SMALLMONEY"
        
    def visit_UNIQUEIDENTIFIER(self, type_):
        return "UNIQUEIDENTIFIER"
        
ischema_names = {
    'integer' : INTEGER,
    'unsigned int' : INTEGER, # TODO: unsigned flags
    'unsigned smallint' : SMALLINT, # TODO: unsigned flags
    'unsigned bigint' : BIGINT, # TODO: unsigned flags
    'bigint': BIGINT,
    'smallint' : SMALLINT,
    'tinyint' : TINYINT,
    'varchar' : VARCHAR,
    'long varchar' : TEXT, # TODO
    'char' : CHAR,
    'decimal' : DECIMAL,
    'numeric' : NUMERIC,
    'float' : FLOAT,
    'double' : NUMERIC, # TODO
    'binary' : BINARY,
    'varbinary' : VARBINARY,
    'bit': BIT,
    'image' : IMAGE,
    'timestamp': TIMESTAMP,
    'money': MONEY,
    'smallmoney': MONEY,
    'uniqueidentifier': UNIQUEIDENTIFIER,

}


class SybaseExecutionContext(default.DefaultExecutionContext):
    _enable_identity_insert = False
    
    def set_ddl_autocommit(self, connection, value):
        """Must be implemented by subclasses to accommodate DDL executions.
        
        "connection" is the raw unwrapped DBAPI connection.   "value"
        is True or False.  when True, the connection should be configured
        such that a DDL can take place subsequently.  when False,
        a DDL has taken place and the connection should be resumed
        into non-autocommit mode.
        
        """
        raise NotImplementedError()
        
    def pre_exec(self):
        if self.isinsert:
            tbl = self.compiled.statement.table
            seq_column = tbl._autoincrement_column
            insert_has_sequence = seq_column is not None
            
            if insert_has_sequence:
                self._enable_identity_insert = seq_column.key in self.compiled_parameters[0]
            else:
                self._enable_identity_insert = False
            
            if self._enable_identity_insert:
                self.cursor.execute("SET IDENTITY_INSERT %s ON" % 
                    self.dialect.identifier_preparer.format_table(tbl))

        if self.isddl:
            # TODO: to enhance this, we can detect "ddl in tran" on the
            # database settings.  this error message should be improved to 
            # include a note about that.
            if not self.should_autocommit:
                raise exc.InvalidRequestError("The Sybase dialect only supports "
                                            "DDL in 'autocommit' mode at this time.")

            self.root_connection.engine.logger.info("AUTOCOMMIT (Assuming no Sybase 'ddl in tran')")

            self.set_ddl_autocommit(self.root_connection.connection.connection, True)
            

    def post_exec(self):
       if self.isddl:
            self.set_ddl_autocommit(self.root_connection, False)
        
       if self._enable_identity_insert:
            self.cursor.execute(
                        "SET IDENTITY_INSERT %s OFF" %  
                                self.dialect.identifier_preparer.
                                    format_table(self.compiled.statement.table)
                        )

    def get_lastrowid(self):
        cursor = self.create_cursor()
        cursor.execute("SELECT @@identity AS lastrowid")
        lastrowid = cursor.fetchone()[0]
        cursor.close()
        return lastrowid

class SybaseSQLCompiler(compiler.SQLCompiler):
    ansi_bind_rules = True

    extract_map = util.update_copy(
        compiler.SQLCompiler.extract_map,
        {
        'doy': 'dayofyear',
        'dow': 'weekday',
        'milliseconds': 'millisecond'
    })

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

    def get_from_hint_text(self, table, text):
        return text

    def limit_clause(self, select):
        # Limit in sybase is after the select keyword
        return ""

    def visit_extract(self, extract, **kw):
        field = self.extract_map.get(extract.field, extract.field)
        return 'DATEPART("%s", %s)' % (field, self.process(extract.expr, **kw))

    def for_update_clause(self, select):
        # "FOR UPDATE" is only allowed on "DECLARE CURSOR" which SQLAlchemy doesn't use
        return ''

    def order_by_clause(self, select, **kw):
        kw['literal_binds'] = True
        order_by = self.process(select._order_by_clause, **kw)

        # SybaseSQL only allows ORDER BY in subqueries if there is a LIMIT
        if order_by and (not self.is_subquery() or select._limit):
            return " ORDER BY " + order_by
        else:
            return ""


class SybaseDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column) + " " + \
                                   self.dialect.type_compiler.process(column.type)

        if column.table is None:
            raise exc.InvalidRequestError("The Sybase dialect requires Table-bound "\
                                                   "columns in order to generate DDL")
        seq_col = column.table._autoincrement_column

        # install a IDENTITY Sequence if we have an implicit IDENTITY column
        if seq_col is column:
            sequence = isinstance(column.default, sa_schema.Sequence) and column.default
            if sequence:
                start, increment = sequence.start or 1, sequence.increment or 1
            else:
                start, increment = 1, 1
            if (start, increment) == (1, 1):
                colspec += " IDENTITY"
            else:
                # TODO: need correct syntax for this
                colspec += " IDENTITY(%s,%s)" % (start, increment)
        else:
            if column.nullable is not None:
                if not column.nullable or column.primary_key:
                    colspec += " NOT NULL"
                else:
                    colspec += " NULL"

            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        return colspec

    def visit_drop_index(self, drop):
        index = drop.element
        return "\nDROP INDEX %s.%s" % (
            self.preparer.quote_identifier(index.table.name),
            self.preparer.quote(self._validate_identifier(index.name, False), index.quote)
            )

class SybaseIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS

class SybaseDialect(default.DefaultDialect):
    name = 'sybase'
    supports_unicode_statements = False
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False

    supports_native_boolean = False
    supports_unicode_binds = False
    postfetch_lastrowid = True

    colspecs = {}
    ischema_names = ischema_names

    type_compiler = SybaseTypeCompiler
    statement_compiler = SybaseSQLCompiler
    ddl_compiler = SybaseDDLCompiler
    preparer = SybaseIdentifierPreparer

    def _get_default_schema_name(self, connection):
        return connection.scalar(
                     text("SELECT user_name() as user_name", typemap={'user_name':Unicode})
             )

    def initialize(self, connection):
        super(SybaseDialect, self).initialize(connection)
        if self.server_version_info is not None and\
            self.server_version_info < (15, ):
            self.max_identifier_length = 30
        else:
            self.max_identifier_length = 255
        
    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        if schema is None:
            schema = self.default_schema_name

        result = connection.execute(
                    text("select sysobjects.name from sysobjects, sysusers "
                         "where sysobjects.uid=sysusers.uid and "
                         "sysusers.name=:schemaname and "
                         "sysobjects.type='U'",
                         bindparams=[
                                  bindparam('schemaname', schema)
                                  ])
         )
        return [r[0] for r in result]

    def has_table(self, connection, tablename, schema=None):
        if schema is None:
            schema = self.default_schema_name

        result = connection.execute(
                    text("select sysobjects.name from sysobjects, sysusers "
                         "where sysobjects.uid=sysusers.uid and "
                         "sysobjects.name=:tablename and "
                         "sysusers.name=:schemaname and "
                         "sysobjects.type='U'",
                         bindparams=[
                                  bindparam('tablename', tablename),
                                  bindparam('schemaname', schema)
                                  ])
                 )
        return result.scalar() is not None

    def reflecttable(self, connection, table, include_columns):
        raise NotImplementedError()

