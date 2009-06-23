# firebird.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Firebird backend
================

This module implements the Firebird backend, thru the kinterbasdb_
DBAPI module.

Firebird dialects
-----------------

Firebird offers two distinct dialects_ (not to be confused with the
SA ``Dialect`` thing):

dialect 1
  This is the old syntax and behaviour, inherited from Interbase pre-6.0.

dialect 3
  This is the newer and supported syntax, introduced in Interbase 6.0.

From the user point of view, the biggest change is in date/time
handling: under dialect 1, there's a single kind of field, ``DATE``
with a synonim ``DATETIME``, that holds a `timestamp` value, that is a
date with hour, minute, second. Under dialect 3 there are three kinds,
a ``DATE`` that holds a date, a ``TIME`` that holds a *time of the
day* value and a ``TIMESTAMP``, equivalent to the old ``DATE``.

The problem is that the dialect of a Firebird database is a property
of the database itself [#]_ (that is, any single database has been
created with one dialect or the other: there is no way to change the
after creation). SQLAlchemy has a single instance of the class that
controls all the connections to a particular kind of database, so it
cannot easily differentiate between the two modes, and in particular
it **cannot** simultaneously talk with two distinct Firebird databases
with different dialects.

By default this module is biased toward dialect 3, but you can easily
tweak it to handle dialect 1 if needed::

  from sqlalchemy import types as sqltypes
  from sqlalchemy.databases.firebird import FBDate, colspecs, ischema_names

  # Adjust the mapping of the timestamp kind
  ischema_names['TIMESTAMP'] = FBDate
  colspecs[sqltypes.DateTime] = FBDate,

Other aspects may be version-specific. You can use the ``server_version_info()`` method
on the ``FBDialect`` class to do whatever is needed::

  from sqlalchemy.databases.firebird import FBCompiler

  if engine.dialect.server_version_info(connection) < (2,0):
      # Change the name of the function ``length`` to use the UDF version
      # instead of ``char_length``
      FBCompiler.LENGTH_FUNCTION_NAME = 'strlen'

Pooling connections
-------------------

The default strategy used by SQLAlchemy to pool the database connections
in particular cases may raise an ``OperationalError`` with a message
`"object XYZ is in use"`. This happens on Firebird when there are two
connections to the database, one is using, or has used, a particular table
and the other tries to drop or alter the same table. To garantee DDL
operations success Firebird recommend doing them as the single connected user.

In case your SA application effectively needs to do DDL operations while other
connections are active, the following setting may alleviate the problem::

  from sqlalchemy import pool
  from sqlalchemy.databases.firebird import dialect

  # Force SA to use a single connection per thread
  dialect.poolclass = pool.SingletonThreadPool

RETURNING support
-----------------

Firebird 2.0 supports returning a result set from inserts, and 2.1 extends
that to deletes and updates.

To use this pass the column/expression list to the ``firebird_returning``
parameter when creating the queries::

  raises = tbl.update(empl.c.sales > 100, values=dict(salary=empl.c.salary * 1.1),
                      firebird_returning=[empl.c.id, empl.c.salary]).execute().fetchall()


.. [#] Well, that is not the whole story, as the client may still ask
       a different (lower) dialect...

.. _dialects: http://mc-computing.com/Databases/Firebird/SQL_Dialect.html
.. _kinterbasdb: http://sourceforge.net/projects/kinterbasdb
"""


import datetime, decimal, re

from sqlalchemy import exc, schema, types as sqltypes, sql, util
from sqlalchemy.engine import base, default


_initialized_kb = False


class FBNumeric(sqltypes.Numeric):
    """Handle ``NUMERIC(precision,scale)`` datatype."""

    def get_col_spec(self):
        if self.precision is None:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(scale)s)" % { 'precision': self.precision,
                                                            'scale' : self.scale }

    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect):
        if self.asdecimal:
            return None
        else:
            def process(value):
                if isinstance(value, decimal.Decimal):
                    return float(value)
                else:
                    return value
            return process


class FBFloat(sqltypes.Float):
    """Handle ``FLOAT(precision)`` datatype."""

    def get_col_spec(self):
        if not self.precision:
            return "FLOAT"
        else:
            return "FLOAT(%(precision)s)" % {'precision': self.precision}


class FBInteger(sqltypes.Integer):
    """Handle ``INTEGER`` datatype."""

    def get_col_spec(self):
        return "INTEGER"


class FBSmallInteger(sqltypes.Smallinteger):
    """Handle ``SMALLINT`` datatype."""

    def get_col_spec(self):
        return "SMALLINT"


class FBDateTime(sqltypes.DateTime):
    """Handle ``TIMESTAMP`` datatype."""

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
    """Handle ``DATE`` datatype."""

    def get_col_spec(self):
        return "DATE"


class FBTime(sqltypes.Time):
    """Handle ``TIME`` datatype."""

    def get_col_spec(self):
        return "TIME"


class FBText(sqltypes.Text):
    """Handle ``BLOB SUB_TYPE 1`` datatype (aka *textual* blob)."""

    def get_col_spec(self):
        return "BLOB SUB_TYPE 1"


class FBString(sqltypes.String):
    """Handle ``VARCHAR(length)`` datatype."""

    def get_col_spec(self):
        if self.length:
            return "VARCHAR(%(length)s)" % {'length' : self.length}
        else:
            return "BLOB SUB_TYPE 1"


class FBChar(sqltypes.CHAR):
    """Handle ``CHAR(length)`` datatype."""

    def get_col_spec(self):
        if self.length:
            return "CHAR(%(length)s)" % {'length' : self.length}
        else:
            return "BLOB SUB_TYPE 1"


class FBBinary(sqltypes.Binary):
    """Handle ``BLOB SUB_TYPE 0`` datatype (aka *binary* blob)."""

    def get_col_spec(self):
        return "BLOB SUB_TYPE 0"


class FBBoolean(sqltypes.Boolean):
    """Handle boolean values as a ``SMALLINT`` datatype."""

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
    sqltypes.Text : FBText,
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
      'INT64': lambda r: FBNumeric(precision=r['fprec'], scale=r['fscale'] * -1), # This generically handles NUMERIC()
     'DOUBLE': lambda r: FBFloat(),
  'TIMESTAMP': lambda r: FBDateTime(),
    'VARYING': lambda r: FBString(r['flen']),
    'CSTRING': lambda r: FBChar(r['flen']),
       'BLOB': lambda r: r['stype']==1 and FBText() or FBBinary()
      }

RETURNING_KW_NAME = 'firebird_returning'

class FBExecutionContext(default.DefaultExecutionContext):
    pass


class FBDialect(default.DefaultDialect):
    """Firebird dialect"""
    name = 'firebird'
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    max_identifier_length = 31
    preexecute_pk_sequences = True
    supports_pk_autoincrement = False

    def __init__(self, type_conv=200, concurrency_level=1, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)

        self.type_conv = type_conv
        self.concurrency_level = concurrency_level

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

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def server_version_info(self, connection):
        """Get the version of the Firebird server used by a connection.

        Returns a tuple of (`major`, `minor`, `build`), three integers
        representing the version of the attached server.
        """

        # This is the simpler approach (the other uses the services api),
        # that for backward compatibility reasons returns a string like
        #   LI-V6.3.3.12981 Firebird 2.0
        # where the first version is a fake one resembling the old
        # Interbase signature. This is more than enough for our purposes,
        # as this is mainly (only?) used by the testsuite.

        from re import match

        fbconn = connection.connection.connection
        version = fbconn.server_version
        m = match('\w+-V(\d+)\.(\d+)\.(\d+)\.(\d+) \w+ (\d+)\.(\d+)', version)
        if not m:
            raise AssertionError("Could not determine version from string '%s'" % version)
        return tuple([int(x) for x in m.group(5, 6, 4)])

    def _normalize_name(self, name):
        """Convert the name to lowercase if it is possible"""

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
        """Revert a *normalized* name to its uppercase equivalent"""

        if name is None:
            return None
        elif name.lower() == name and not self.identifier_preparer._requires_quotes(name.lower()):
            return name.upper()
        else:
            return name

    def table_names(self, connection, schema):
        """Return a list of *normalized* table names omitting system relations."""

        s = """
        SELECT r.rdb$relation_name
        FROM rdb$relations r
        WHERE r.rdb$system_flag=0
        """
        return [self._normalize_name(row[0]) for row in connection.execute(s)]

    def has_table(self, connection, table_name, schema=None):
        """Return ``True`` if the given table exists, ignoring the `schema`."""

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
        """Return ``True`` if the given sequence (generator) exists."""

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
        elif isinstance(e, self.dbapi.ProgrammingError):
            msg = str(e)
            return ('Invalid connection state' in msg or
                    'Invalid cursor state' in msg)
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
        # Heuristic-query to determine the generator associated to a PK field
        genqry = """
        SELECT trigdep.rdb$depended_on_name AS fgenerator
        FROM rdb$dependencies tabdep
             JOIN rdb$dependencies trigdep ON (tabdep.rdb$dependent_name=trigdep.rdb$dependent_name
                                               AND trigdep.rdb$depended_on_type=14
                                               AND trigdep.rdb$dependent_type=2)
             JOIN rdb$triggers trig ON (trig.rdb$trigger_name=tabdep.rdb$dependent_name)
        WHERE tabdep.rdb$depended_on_name=?
          AND tabdep.rdb$depended_on_type=0
          AND trig.rdb$trigger_type=1
          AND tabdep.rdb$field_name=?
          AND (SELECT count(*)
               FROM rdb$dependencies trigdep2
               WHERE trigdep2.rdb$dependent_name = trigdep.rdb$dependent_name) = 2
        """

        tablename = self._denormalize_name(table.name)

        # get primary key fields
        c = connection.execute(keyqry, ["PRIMARY KEY", tablename])
        pkfields = [self._normalize_name(r['fname']) for r in c.fetchall()]

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
            # get the data type
            coltype = ischema_names.get(row['ftype'].rstrip())
            if coltype is None:
                util.warn("Did not recognize type '%s' of column '%s'" %
                          (str(row['ftype']), name))
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
                assert row['fdefault'].upper().startswith('DEFAULT '), row
                defvalue = row['fdefault'][8:]
                args.append(schema.DefaultClause(sql.text(defvalue)))

            col = schema.Column(*args, **kw)
            if kw['primary_key']:
                # if the PK is a single field, try to see if its linked to
                # a sequence thru a trigger
                if len(pkfields)==1:
                    genc = connection.execute(genqry, [tablename, row['fname']])
                    genr = genc.fetchone()
                    if genr is not None:
                        col.sequence = schema.Sequence(self._normalize_name(genr['fgenerator']))

            table.append_column(col)

        if not found_table:
            raise exc.NoSuchTableError(table.name)

        # get the foreign keys
        c = connection.execute(fkqry, ["FOREIGN KEY", tablename])
        fks = {}
        while True:
            row = c.fetchone()
            if not row:
                break

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

        for name, value in fks.iteritems():
            table.append_constraint(schema.ForeignKeyConstraint(value[0], value[1], name=name, link_to_name=True))

    def do_execute(self, cursor, statement, parameters, **kwargs):
        # kinterbase does not accept a None, but wants an empty list
        # when there are no arguments.
        cursor.execute(statement, parameters or [])

    def do_rollback(self, connection):
        # Use the retaining feature, that keeps the transaction going
        connection.rollback(True)

    def do_commit(self, connection):
        # Use the retaining feature, that keeps the transaction going
        connection.commit(True)


def _substring(s, start, length=None):
    "Helper function to handle Firebird 2 SUBSTRING builtin"

    if length is None:
        return "SUBSTRING(%s FROM %s)" % (s, start)
    else:
        return "SUBSTRING(%s FROM %s FOR %s)" % (s, start, length)


class FBCompiler(sql.compiler.DefaultCompiler):
    """Firebird specific idiosincrasies"""

    # Firebird lacks a builtin modulo operator, but there is
    # an equivalent function in the ib_udf library.
    operators = sql.compiler.DefaultCompiler.operators.copy()
    operators.update({
        sql.operators.mod : lambda x, y:"mod(%s, %s)" % (x, y)
        })

    def visit_alias(self, alias, asfrom=False, **kwargs):
        # Override to not use the AS keyword which FB 1.5 does not like
        if asfrom:
            return self.process(alias.original, asfrom=True, **kwargs) + " " + self.preparer.format_alias(alias, self._anonymize(alias.name))
        else:
            return self.process(alias.original, **kwargs)

    functions = sql.compiler.DefaultCompiler.functions.copy()
    functions['substring'] = _substring

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
            result += "FIRST %d "  % select._limit
        if select._offset:
            result +="SKIP %d "  %  select._offset
        if select._distinct:
            result += "DISTINCT "
        return result

    def limit_clause(self, select):
        """Already taken care of in the `get_select_precolumns` method."""

        return ""

    LENGTH_FUNCTION_NAME = 'char_length'
    def function_string(self, func):
        """Substitute the ``length`` function.

        On newer FB there is a ``char_length`` function, while older
        ones need the ``strlen`` UDF.
        """

        if func.name == 'length':
            return self.LENGTH_FUNCTION_NAME + '%(expr)s'
        return super(FBCompiler, self).function_string(func)

    def _append_returning(self, text, stmt):
        returning_cols = stmt.kwargs[RETURNING_KW_NAME]
        def flatten_columnlist(collist):
            for c in collist:
                if isinstance(c, sql.expression.Selectable):
                    for co in c.columns:
                        yield co
                else:
                    yield c
        columns = [self.process(c, within_columns_clause=True)
                   for c in flatten_columnlist(returning_cols)]
        text += ' RETURNING ' + ', '.join(columns)
        return text

    def visit_update(self, update_stmt):
        text = super(FBCompiler, self).visit_update(update_stmt)
        if RETURNING_KW_NAME in update_stmt.kwargs:
            return self._append_returning(text, update_stmt)
        else:
            return text

    def visit_insert(self, insert_stmt):
        text = super(FBCompiler, self).visit_insert(insert_stmt)
        if RETURNING_KW_NAME in insert_stmt.kwargs:
            return self._append_returning(text, insert_stmt)
        else:
            return text

    def visit_delete(self, delete_stmt):
        text = super(FBCompiler, self).visit_delete(delete_stmt)
        if RETURNING_KW_NAME in delete_stmt.kwargs:
            return self._append_returning(text, delete_stmt)
        else:
            return text


class FBSchemaGenerator(sql.compiler.SchemaGenerator):
    """Firebird syntactic idiosincrasies"""

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
        """Generate a ``CREATE GENERATOR`` statement for the sequence."""

        if not self.checkfirst or not self.dialect.has_sequence(self.connection, sequence.name):
            self.append("CREATE GENERATOR %s" % self.preparer.format_sequence(sequence))
            self.execute()


class FBSchemaDropper(sql.compiler.SchemaDropper):
    """Firebird syntactic idiosincrasies"""

    def visit_sequence(self, sequence):
        """Generate a ``DROP GENERATOR`` statement for the sequence."""

        if not self.checkfirst or self.dialect.has_sequence(self.connection, sequence.name):
            self.append("DROP GENERATOR %s" % self.preparer.format_sequence(sequence))
            self.execute()


class FBDefaultRunner(base.DefaultRunner):
    """Firebird specific idiosincrasies"""

    def visit_sequence(self, seq):
        """Get the next value from the sequence using ``gen_id()``."""

        return self.execute_string("SELECT gen_id(%s, 1) FROM rdb$database" % \
            self.dialect.identifier_preparer.format_sequence(seq))


RESERVED_WORDS = set(
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


class FBIdentifierPreparer(sql.compiler.IdentifierPreparer):
    """Install Firebird specific reserved words."""

    reserved_words = RESERVED_WORDS

    def __init__(self, dialect):
        super(FBIdentifierPreparer, self).__init__(dialect, omit_schema=True)


dialect = FBDialect
dialect.statement_compiler = FBCompiler
dialect.schemagenerator = FBSchemaGenerator
dialect.schemadropper = FBSchemaDropper
dialect.defaultrunner = FBDefaultRunner
dialect.preparer = FBIdentifierPreparer
dialect.execution_ctx_cls = FBExecutionContext
