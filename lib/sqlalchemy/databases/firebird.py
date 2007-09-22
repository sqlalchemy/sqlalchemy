# firebird.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


import warnings

from sqlalchemy import util, sql, schema, exceptions
from sqlalchemy.sql import compiler
from sqlalchemy.engine import default, base
from sqlalchemy import types as sqltypes


_initialized_kb = False


class FBNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        if self.precision is None:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(length)s)" % { 'precision': self.precision,
                                                            'length' : self.length }

class FBInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"


class FBSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"


class FBDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP"


class FBDate(sqltypes.DateTime):
    def get_col_spec(self):
        return "DATE"


class FBText(sqltypes.TEXT):
    def get_col_spec(self):
        return "BLOB SUB_TYPE 2"


class FBString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}


class FBChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}


class FBBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BLOB SUB_TYPE 1"


class FBBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "SMALLINT"


colspecs = {
    sqltypes.Integer : FBInteger,
    sqltypes.Smallinteger : FBSmallInteger,
    sqltypes.Numeric : FBNumeric,
    sqltypes.Float : FBNumeric,
    sqltypes.DateTime : FBDateTime,
    sqltypes.Date : FBDate,
    sqltypes.String : FBString,
    sqltypes.Binary : FBBinary,
    sqltypes.Boolean : FBBoolean,
    sqltypes.TEXT : FBText,
    sqltypes.CHAR: FBChar,
}


def descriptor():
    return {'name':'firebird',
    'description':'Firebird',
    'arguments':[
        ('host', 'Host Server Name', None),
        ('database', 'Database Name', None),
        ('user', 'Username', None),
        ('password', 'Password', None)
    ]}


class FBExecutionContext(default.DefaultExecutionContext):
    def supports_sane_rowcount(self):
        return True


class FBDialect(default.DefaultDialect):
    supports_sane_rowcount = False
    max_identifier_length = 31
    preexecute_sequences = True

    def __init__(self, type_conv=200, concurrency_level=1, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)

        self.type_conv = type_conv
        self.concurrency_level= concurrency_level

    def dbapi(cls):
        import kinterbasdb
        return kinterbasdb
    dbapi = classmethod(dbapi)
    
    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if opts.get('port'):
            opts['host'] = "%s/%s" % (opts['host'], opts['port'])
            del opts['port']
        opts.update(url.query)

        type_conv = opts.pop('type_conv', self.type_conv)
        concurrency_level = opts.pop('concurrency_level', self.concurrency_level)
        global _initialized_kb
        if not _initialized_kb and self.dbapi is not None:
            _initialized_kb = True
            self.dbapi.init(type_conv=type_conv, concurrency_level=concurrency_level)
        return ([], opts)

    def create_execution_context(self, *args, **kwargs):
        return FBExecutionContext(self, *args, **kwargs)

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def table_names(self, connection, schema):
        s = "SELECT R.RDB$RELATION_NAME FROM RDB$RELATIONS R"
        return [row[0] for row in connection.execute(s)]

    def has_table(self, connection, table_name, schema=None):
        tblqry = """
        SELECT count(*)
        FROM RDB$RELATIONS R
        WHERE R.RDB$RELATION_NAME=?"""

        c = connection.execute(tblqry, [table_name.upper()])
        row = c.fetchone()
        if row[0] > 0:
            return True
        else:
            return False

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.OperationalError):
            return 'Unable to complete network request to host' in str(e)
        else:
            return False

    def reflecttable(self, connection, table, include_columns):
        #TODO: map these better
        column_func = {
            14 : lambda r: sqltypes.String(r['FLEN']), # TEXT
            7  : lambda r: sqltypes.Integer(), # SHORT
            8  : lambda r: sqltypes.Integer(), # LONG
            9  : lambda r: sqltypes.Float(), # QUAD
            10 : lambda r: sqltypes.Float(), # FLOAT
            27 : lambda r: sqltypes.Float(), # DOUBLE
            35 : lambda r: sqltypes.DateTime(), # TIMESTAMP
            37 : lambda r: sqltypes.String(r['FLEN']), # VARYING
            261: lambda r: sqltypes.TEXT(), # BLOB
            40 : lambda r: sqltypes.Char(r['FLEN']), # CSTRING
            12 : lambda r: sqltypes.Date(), # DATE
            13 : lambda r: sqltypes.Time(), # TIME
            16 : lambda r: sqltypes.Numeric(precision=r['FPREC'], length=r['FSCALE'] * -1)  #INT64
            }
        tblqry = """
        SELECT DISTINCT R.RDB$FIELD_NAME AS FNAME,
                  R.RDB$NULL_FLAG AS NULL_FLAG,
                  R.RDB$FIELD_POSITION,
                  F.RDB$FIELD_TYPE AS FTYPE,
                  F.RDB$FIELD_SUB_TYPE AS STYPE,
                  F.RDB$FIELD_LENGTH AS FLEN,
                  F.RDB$FIELD_PRECISION AS FPREC,
                  F.RDB$FIELD_SCALE AS FSCALE
        FROM RDB$RELATION_FIELDS R
             JOIN RDB$FIELDS F ON R.RDB$FIELD_SOURCE=F.RDB$FIELD_NAME
        WHERE F.RDB$SYSTEM_FLAG=0 and R.RDB$RELATION_NAME=?
        ORDER BY R.RDB$FIELD_POSITION"""
        keyqry = """
        SELECT SE.RDB$FIELD_NAME SENAME
        FROM RDB$RELATION_CONSTRAINTS RC
             JOIN RDB$INDEX_SEGMENTS SE
               ON RC.RDB$INDEX_NAME=SE.RDB$INDEX_NAME
        WHERE RC.RDB$CONSTRAINT_TYPE=? AND RC.RDB$RELATION_NAME=?"""
        fkqry = """
        SELECT RC.RDB$CONSTRAINT_NAME CNAME,
               CSE.RDB$FIELD_NAME FNAME,
               IX2.RDB$RELATION_NAME RNAME,
               SE.RDB$FIELD_NAME SENAME
        FROM RDB$RELATION_CONSTRAINTS RC
             JOIN RDB$INDICES IX1
               ON IX1.RDB$INDEX_NAME=RC.RDB$INDEX_NAME
             JOIN RDB$INDICES IX2
               ON IX2.RDB$INDEX_NAME=IX1.RDB$FOREIGN_KEY
             JOIN RDB$INDEX_SEGMENTS CSE
               ON CSE.RDB$INDEX_NAME=IX1.RDB$INDEX_NAME
             JOIN RDB$INDEX_SEGMENTS SE
               ON SE.RDB$INDEX_NAME=IX2.RDB$INDEX_NAME AND SE.RDB$FIELD_POSITION=CSE.RDB$FIELD_POSITION
        WHERE RC.RDB$CONSTRAINT_TYPE=? AND RC.RDB$RELATION_NAME=?
        ORDER BY SE.RDB$INDEX_NAME, SE.RDB$FIELD_POSITION"""

        # get primary key fields
        c = connection.execute(keyqry, ["PRIMARY KEY", table.name.upper()])
        pkfields =[r['SENAME'] for r in c.fetchall()]

        # get all of the fields for this table

        def lower_if_possible(name):
            # Remove trailing spaces: FB uses a CHAR() type,
            # that is padded with spaces
            name = name.rstrip()
            # If its composed only by upper case chars, use
            # the lowered version, otherwise keep the original
            # (even if stripped...)
            lname = name.lower()
            if lname.upper() == name and not ' ' in name:
                return lname
            return name

        c = connection.execute(tblqry, [table.name.upper()])
        row = c.fetchone()
        if not row:
            raise exceptions.NoSuchTableError(table.name)

        while row:
            name = row['FNAME']
            python_name = lower_if_possible(name)
            if include_columns and python_name not in include_columns:
                continue
            args = [python_name]

            kw = {}
            # get the data types and lengths
            coltype = column_func.get(row['FTYPE'], None)
            if coltype is None:
                warnings.warn(RuntimeWarning("Did not recognize type '%s' of column '%s'" % (str(row['FTYPE']), name)))
                coltype = sqltypes.NULLTYPE
            else:
                coltype = coltype(row)
            args.append(coltype)

            # is it a primary key?
            kw['primary_key'] = name in pkfields

            table.append_column(schema.Column(*args, **kw))
            row = c.fetchone()

        # get the foreign keys
        c = connection.execute(fkqry, ["FOREIGN KEY", table.name.upper()])
        fks = {}
        while True:
            row = c.fetchone()
            if not row: break

            cname = lower_if_possible(row['CNAME'])
            try:
                fk = fks[cname]
            except KeyError:
                fks[cname] = fk = ([], [])
            rname = lower_if_possible(row['RNAME'])
            schema.Table(rname, table.metadata, autoload=True, autoload_with=connection)
            fname = lower_if_possible(row['FNAME'])
            refspec = rname + '.' + lower_if_possible(row['SENAME'])
            fk[0].append(fname)
            fk[1].append(refspec)

        for name,value in fks.iteritems():
            table.append_constraint(schema.ForeignKeyConstraint(value[0], value[1], name=name))

    def do_execute(self, cursor, statement, parameters, **kwargs):
        cursor.execute(statement, parameters or [])

    def do_rollback(self, connection):
        connection.rollback(True)

    def do_commit(self, connection):
        connection.commit(True)


class FBCompiler(compiler.DefaultCompiler):
    """Firebird specific idiosincrasies"""

    def visit_alias(self, alias, asfrom=False, **kwargs):
        # Override to not use the AS keyword which FB 1.5 does not like
        if asfrom:
            return self.process(alias.original, asfrom=True, **kwargs) + " " + self.preparer.format_alias(alias, self._anonymize(alias.name))
        else:
            return self.process(alias.original, **kwargs)

    def visit_function(self, func):
        if func.clauses:
            return super(FBCompiler, self).visit_function(func)
        else:
            return func.name

    def default_from(self):
        return " FROM rdb$database"

    def visit_sequence(self, seq):
        return "gen_id(" + seq.name + ", 1)"
        
    def get_select_precolumns(self, select):
        """Called when building a ``SELECT`` statement, position is just
        before column list Firebird puts the limit and offset right
        after the ``SELECT``...
        """

        result = ""
        if select._limit:
            result += " FIRST %d "  % select._limit
        if select._offset:
            result +=" SKIP %d "  %  select._offset
        if select._distinct:
            result += " DISTINCT "
        return result

    def limit_clause(self, select):
        """Already taken care of in the `get_select_precolumns` method."""
        return ""


class FBSchemaGenerator(compiler.SchemaGenerator):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        colspec += " " + column.type.dialect_impl(self.dialect).get_col_spec()

        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable or column.primary_key:
            colspec += " NOT NULL"

        return colspec

    def visit_sequence(self, sequence):
        self.append("CREATE GENERATOR %s" % sequence.name)
        self.execute()


class FBSchemaDropper(compiler.SchemaDropper):
    def visit_sequence(self, sequence):
        self.append("DROP GENERATOR %s" % sequence.name)
        self.execute()


class FBDefaultRunner(base.DefaultRunner):
    def visit_sequence(self, seq):
        return self.execute_string("SELECT gen_id(" + seq.name + ", 1) FROM rdb$database")


RESERVED_WORDS = util.Set(
    ["action", "active", "add", "admin", "after", "all", "alter", "and", "any",
     "as", "asc", "ascending", "at", "auto", "autoddl", "avg", "based", "basename",
     "base_name", "before", "begin", "between", "bigint", "blob", "blobedit", "buffer",
     "by", "cache", "cascade", "case", "cast", "char", "character", "character_length",
     "char_length", "check", "check_point_len", "check_point_length", "close", "collate",
     "collation", "column", "commit", "committed", "compiletime", "computed", "conditional",
     "connect", "constraint", "containing", "continue", "count", "create", "cstring",
     "current", "current_connection", "current_date", "current_role", "current_time",
     "current_timestamp", "current_transaction", "current_user", "cursor", "database",
     "date", "day", "db_key", "debug", "dec", "decimal", "declare", "default", "delete",
     "desc", "descending", "describe", "descriptor", "disconnect", "display", "distinct",
     "do", "domain", "double", "drop", "echo", "edit", "else", "end", "entry_point",
     "escape", "event", "exception", "execute", "exists", "exit", "extern", "external",
     "extract", "fetch", "file", "filter", "float", "for", "foreign", "found", "free_it",
     "from", "full", "function", "gdscode", "generator", "gen_id", "global", "goto",
     "grant", "group", "group_commit_", "group_commit_wait", "having", "help", "hour",
     "if", "immediate", "in", "inactive", "index", "indicator", "init", "inner", "input",
     "input_type", "insert", "int", "integer", "into", "is", "isolation", "isql", "join",
     "key", "lc_messages", "lc_type", "left", "length", "lev", "level", "like", "logfile",
     "log_buffer_size", "log_buf_size", "long", "manual", "max", "maximum", "maximum_segment",
     "max_segment", "merge", "message", "min", "minimum", "minute", "module_name", "month",
     "names", "national", "natural", "nchar", "no", "noauto", "not", "null", "numeric",
     "num_log_buffers", "num_log_bufs", "octet_length", "of", "on", "only", "open", "option",
     "or", "order", "outer", "output", "output_type", "overflow", "page", "pagelength",
     "pages", "page_size", "parameter", "password", "plan", "position", "post_event",
     "precision", "prepare", "primary", "privileges", "procedure", "protected", "public",
     "quit", "raw_partitions", "rdb$db_key", "read", "real", "record_version", "recreate",
     "references", "release", "release", "reserv", "reserving", "restrict", "retain",
     "return", "returning_values", "returns", "revoke", "right", "role", "rollback",
     "row_count", "runtime", "savepoint", "schema", "second", "segment", "select",
     "set", "shadow", "shared", "shell", "show", "singular", "size", "smallint",
     "snapshot", "some", "sort", "sqlcode", "sqlerror", "sqlwarning", "stability",
     "starting", "starts", "statement", "static", "statistics", "sub_type", "sum",
     "suspend", "table", "terminator", "then", "time", "timestamp", "to", "transaction",
     "translate", "translation", "trigger", "trim", "type", "uncommitted", "union",
     "unique", "update", "upper", "user", "using", "value", "values", "varchar",
     "variable", "varying", "version", "view", "wait", "wait_time", "weekday", "when",
     "whenever", "where", "while", "with", "work", "write", "year", "yearday" ])


class FBIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS
    
    def __init__(self, dialect):
        super(FBIdentifierPreparer,self).__init__(dialect, omit_schema=True)


dialect = FBDialect
dialect.statement_compiler = FBCompiler
dialect.schemagenerator = FBSchemaGenerator
dialect.schemadropper = FBSchemaDropper
dialect.defaultrunner = FBDefaultRunner
dialect.preparer = FBIdentifierPreparer

