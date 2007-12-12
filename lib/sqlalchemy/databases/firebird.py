# firebird.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


import datetime
import warnings

from sqlalchemy import exceptions, pool, schema, types as sqltypes, util
from sqlalchemy.engine import base, default
from sqlalchemy.sql import compiler, text


_initialized_kb = False


class FBNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        if self.precision is None:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(length)s)" % { 'precision': self.precision,
                                                            'length' : self.length }

    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect):
        if self.asdecimal:
            return None
        else:
            def process(value):
                if isinstance(value, util.decimal_type):
                    return float(value)
                else:
                    return value
            return process


class FBFloat(sqltypes.Float):
    def get_col_spec(self):
        if not self.precision:
            return "FLOAT"
        else:
            return "FLOAT(%(precision)s)" % {'precision': self.precision}


class FBInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"


class FBSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"


class FBDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP"

    def bind_processor(self, dialect):
        def process(value):
            if value is None or isinstance(value, datetime.datetime):
                return value
            else:
                return datetime.datetime(year=value.year,
                                         month=value.month,
                                         day=value.day)
        return process


class FBDate(sqltypes.DateTime):
    def get_col_spec(self):
        return "DATE"


class FBTime(sqltypes.Time):
    def get_col_spec(self):
        return "TIME"


class FBText(sqltypes.TEXT):
    def get_col_spec(self):
        return "BLOB SUB_TYPE 1"


class FBString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}


class FBChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}


class FBBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BLOB SUB_TYPE 0"


class FBBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "SMALLINT"


colspecs = {
    sqltypes.Integer : FBInteger,
    sqltypes.Smallinteger : FBSmallInteger,
    sqltypes.Numeric : FBNumeric,
    sqltypes.Float : FBFloat,
    sqltypes.DateTime : FBDateTime,
    sqltypes.Date : FBDate,
    sqltypes.Time : FBTime,
    sqltypes.String : FBString,
    sqltypes.Binary : FBBinary,
    sqltypes.Boolean : FBBoolean,
    sqltypes.TEXT : FBText,
    sqltypes.CHAR: FBChar,
}


ischema_names = {
      'SHORT': lambda r: FBSmallInteger(),
       'LONG': lambda r: FBInteger(),
       'QUAD': lambda r: FBFloat(),
      'FLOAT': lambda r: FBFloat(),
       'DATE': lambda r: FBDate(),
       'TIME': lambda r: FBTime(),
       'TEXT': lambda r: FBString(r['flen']),
      'INT64': lambda r: FBNumeric(precision=r['fprec'], length=r['fscale'] * -1), # This generically handles NUMERIC()
     'DOUBLE': lambda r: FBFloat(),
  'TIMESTAMP': lambda r: FBDateTime(),
    'VARYING': lambda r: FBString(r['flen']),
    'CSTRING': lambda r: FBChar(r['flen']),
       'BLOB': lambda r: r['stype']==1 and FBText() or FBBinary
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
    pass


class FBDialect(default.DefaultDialect):
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    max_identifier_length = 31
    preexecute_pk_sequences = True
    supports_pk_autoincrement = False

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

    def _normalize_name(self, name):
        # Remove trailing spaces: FB uses a CHAR() type,
        # that is padded with spaces
        name = name and name.rstrip()
        if name is None:
            return None
        elif name.upper() == name and not self.identifier_preparer._requires_quotes(name.lower()):
            return name.lower()
        else:
            return name

    def _denormalize_name(self, name):
        if name is None:
            return None
        elif name.lower() == name and not self.identifier_preparer._requires_quotes(name.lower()):
            return name.upper()
        else:
            return name

    def table_names(self, connection, schema):
        s = """
        SELECT r.rdb$relation_name
        FROM rdb$relations r
        WHERE r.rdb$system_flag=0
        """
        return [self._normalize_name(row[0]) for row in connection.execute(s)]

    def has_table(self, connection, table_name, schema=None):
        tblqry = """
        SELECT 1 FROM rdb$database
        WHERE EXISTS (SELECT rdb$relation_name
                      FROM rdb$relations
                      WHERE rdb$relation_name=?)
        """
        c = connection.execute(tblqry, [self._denormalize_name(table_name)])
        row = c.fetchone()
        if row is not None:
            return True
        else:
            return False

    def has_sequence(self, connection, sequence_name):
        genqry = """
        SELECT 1 FROM rdb$database
        WHERE EXISTS (SELECT rdb$generator_name
                      FROM rdb$generators
                      WHERE rdb$generator_name=?)
        """
        c = connection.execute(genqry, [self._denormalize_name(sequence_name)])
        row = c.fetchone()
        if row is not None:
            return True
        else:
            return False

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.OperationalError):
            return 'Unable to complete network request to host' in str(e)
        else:
            return False

    def reflecttable(self, connection, table, include_columns):
        # Query to extract the details of all the fields of the given table
        tblqry = """
        SELECT DISTINCT r.rdb$field_name AS fname,
                        r.rdb$null_flag AS null_flag,
                        t.rdb$type_name AS ftype,
                        f.rdb$field_sub_type AS stype,
                        f.rdb$field_length AS flen,
                        f.rdb$field_precision AS fprec,
                        f.rdb$field_scale AS fscale,
                        COALESCE(r.rdb$default_source, f.rdb$default_source) AS fdefault
        FROM rdb$relation_fields r
             JOIN rdb$fields f ON r.rdb$field_source=f.rdb$field_name
             JOIN rdb$types t ON t.rdb$type=f.rdb$field_type AND t.rdb$field_name='RDB$FIELD_TYPE'
        WHERE f.rdb$system_flag=0 AND r.rdb$relation_name=?
        ORDER BY r.rdb$field_position
        """
        # Query to extract the PK/FK constrained fields of the given table
        keyqry = """
        SELECT se.rdb$field_name AS fname
        FROM rdb$relation_constraints rc
             JOIN rdb$index_segments se ON rc.rdb$index_name=se.rdb$index_name
        WHERE rc.rdb$constraint_type=? AND rc.rdb$relation_name=?
        """
        # Query to extract the details of each UK/FK of the given table
        fkqry = """
        SELECT rc.rdb$constraint_name AS cname,
               cse.rdb$field_name AS fname,
               ix2.rdb$relation_name AS targetrname,
               se.rdb$field_name AS targetfname
        FROM rdb$relation_constraints rc
             JOIN rdb$indices ix1 ON ix1.rdb$index_name=rc.rdb$index_name
             JOIN rdb$indices ix2 ON ix2.rdb$index_name=ix1.rdb$foreign_key
             JOIN rdb$index_segments cse ON cse.rdb$index_name=ix1.rdb$index_name
             JOIN rdb$index_segments se ON se.rdb$index_name=ix2.rdb$index_name AND se.rdb$field_position=cse.rdb$field_position
        WHERE rc.rdb$constraint_type=? AND rc.rdb$relation_name=?
        ORDER BY se.rdb$index_name, se.rdb$field_position
        """

        tablename = self._denormalize_name(table.name)

        # get primary key fields
        c = connection.execute(keyqry, ["PRIMARY KEY", tablename])
        pkfields =[self._normalize_name(r['fname']) for r in c.fetchall()]

        # get all of the fields for this table
        c = connection.execute(tblqry, [tablename])

        found_table = False
        while True:
            row = c.fetchone()
            if row is None:
                break
            found_table = True

            name = self._normalize_name(row['fname'])
            if include_columns and name not in include_columns:
                continue
            args = [name]

            kw = {}
            # get the data types and lengths
            coltype = ischema_names.get(row['ftype'].rstrip())
            if coltype is None:
                warnings.warn(RuntimeWarning("Did not recognize type '%s' of column '%s'" % (str(row['ftype']), name)))
                coltype = sqltypes.NULLTYPE
            else:
                coltype = coltype(row)
            args.append(coltype)

            # is it a primary key?
            kw['primary_key'] = name in pkfields

            # is it nullable?
            kw['nullable'] = not bool(row['null_flag'])

            # does it have a default value?
            if row['fdefault'] is not None:
                # the value comes down as "DEFAULT 'value'"
                defvalue = row['fdefault'][8:]
                args.append(schema.PassiveDefault(text(defvalue)))

            table.append_column(schema.Column(*args, **kw))

        if not found_table:
            raise exceptions.NoSuchTableError(table.name)

        # get the foreign keys
        c = connection.execute(fkqry, ["FOREIGN KEY", tablename])
        fks = {}
        while True:
            row = c.fetchone()
            if not row: break

            cname = self._normalize_name(row['cname'])
            try:
                fk = fks[cname]
            except KeyError:
                fks[cname] = fk = ([], [])
            rname = self._normalize_name(row['targetrname'])
            schema.Table(rname, table.metadata, autoload=True, autoload_with=connection)
            fname = self._normalize_name(row['fname'])
            refspec = rname + '.' + self._normalize_name(row['targetfname'])
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

    def function_argspec(self, func):
        if func.clauses:
            return self.process(func.clause_expr)
        else:
            return ""

    def default_from(self):
        return " FROM rdb$database"

    def visit_sequence(self, seq):
        return "gen_id(%s, 1)" % self.preparer.format_sequence(seq)

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
        self.append("CREATE GENERATOR %s" % self.preparer.format_sequence(sequence))
        self.execute()


class FBSchemaDropper(compiler.SchemaDropper):
    def visit_sequence(self, sequence):
        self.append("DROP GENERATOR %s" % self.preparer.format_sequence(sequence))
        self.execute()


class FBDefaultRunner(base.DefaultRunner):
    def visit_sequence(self, seq):
        return self.execute_string("SELECT gen_id(%s, 1) FROM rdb$database" % \
            self.dialect.identifier_preparer.format_sequence(seq))


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
dialect.poolclass = pool.SingletonThreadPool
dialect.statement_compiler = FBCompiler
dialect.schemagenerator = FBSchemaGenerator
dialect.schemadropper = FBSchemaDropper
dialect.defaultrunner = FBDefaultRunner
dialect.preparer = FBIdentifierPreparer
