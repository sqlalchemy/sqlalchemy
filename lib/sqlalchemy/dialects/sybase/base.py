# sybase.py
# Copyright (C) 2007 Fisch Asset Management AG http://www.fam.ch
# Coding: Alexander Houben alexander.houben@thor-solutions.ch
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for the Sybase iAnywhere database.  

This is not (yet) a full backend for Sybase ASE.

This dialect is *not* ported to SQLAlchemy 0.6.

This dialect is *not* tested on SQLAlchemy 0.6.


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
from sqlalchemy.dialects.sybase.schema import *

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


class SybaseImage(sqltypes.LargeBinary):
    __visit_name__ = 'IMAGE'

class SybaseBit(sqltypes.TypeEngine):
    __visit_name__ = 'BIT'
    
class SybaseMoney(sqltypes.TypeEngine):
    __visit_name__ = "MONEY"

class SybaseSmallMoney(SybaseMoney):
    __visit_name__ = "SMALLMONEY"

class SybaseUniqueIdentifier(sqltypes.TypeEngine):
    __visit_name__ = "UNIQUEIDENTIFIER"
    
class SybaseBoolean(sqltypes.Boolean):
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

class SybaseTypeCompiler(compiler.GenericTypeCompiler):
    def visit_large_binary(self, type_):
        return self.visit_IMAGE(type_)
    
    def visit_boolean(self, type_):
        return self.visit_BIT(type_)
        
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
        
colspecs = {
    sqltypes.LargeBinary : SybaseImage,
    sqltypes.Boolean : SybaseBoolean,
}

ischema_names = {
    'integer' : sqltypes.INTEGER,
    'unsigned int' : sqltypes.Integer,
    'unsigned smallint' : sqltypes.SmallInteger,
    'unsigned bigint' : sqltypes.BigInteger,
    'bigint': sqltypes.BIGINT,
    'smallint' : sqltypes.SMALLINT,
    'tinyint' : sqltypes.SmallInteger,
    'varchar' : sqltypes.VARCHAR,
    'long varchar' : sqltypes.Text,
    'char' : sqltypes.CHAR,
    'decimal' : sqltypes.DECIMAL,
    'numeric' : sqltypes.NUMERIC,
    'float' : sqltypes.FLOAT,
    'double' : sqltypes.Numeric,
    'binary' : sqltypes.LargeBinary,
    'long binary' : sqltypes.LargeBinary,
    'varbinary' : sqltypes.LargeBinary,
    'bit': SybaseBit,
    'image' : SybaseImage,
    'timestamp': sqltypes.TIMESTAMP,
    'money': SybaseMoney,
    'smallmoney': SybaseSmallMoney,
    'uniqueidentifier': SybaseUniqueIdentifier,

}


class SybaseExecutionContext(default.DefaultExecutionContext):

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


class SybaseSQLCompiler(compiler.SQLCompiler):

    extract_map = compiler.SQLCompiler.extract_map.copy()
    extract_map.update ({
        'doy': 'dayofyear',
        'dow': 'weekday',
        'milliseconds': 'millisecond'
    })

    def visit_mod(self, binary, **kw):
        return "MOD(%s, %s)" % (self.process(binary.left), self.process(binary.right))

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


class SybaseDDLCompiler(compiler.DDLCompiler):
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
            colspec += " " + self.dialect.type_compiler.process(column.type)

        if not column.nullable:
            colspec += " NOT NULL"

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
    colspecs = colspecs
    ischema_names = ischema_names

    type_compiler = SybaseTypeCompiler
    statement_compiler = SybaseSQLCompiler
    ddl_compiler = SybaseDDLCompiler
    preparer = SybaseIdentifierPreparer

    ported_sqla_06 = False

    schema_name = "dba"

    def __init__(self, **params):
        super(SybaseDialect, self).__init__(**params)
        self.text_as_varchar = False

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def _get_default_schema_name(self, connection):
        # TODO
        return self.schema_name

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
        return connection.execute(s).first() is not None

    def reflecttable(self, connection, table, include_columns):
        # Get base columns
        if table.schema is not None:
            current_schema = table.schema
        else:
            current_schema = self.default_schema_name

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
        for primary_table in foreignKeys.iterkeys():
            #table.append_constraint(schema.ForeignKeyConstraint(['%s.%s'%(foreign_table, foreign_column)], ['%s.%s'%(primary_table,primary_column)]))
            table.append_constraint(schema.ForeignKeyConstraint(foreignKeys[primary_table][0], foreignKeys[primary_table][1], link_to_name=True))

        if not found_table:
            raise exc.NoSuchTableError(table.name)

