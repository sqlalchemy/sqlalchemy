# postgres.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for the PostgreSQL database.

PostgreSQL supports partial indexes. To create them pass a posgres_where
option to the Index constructor::

  Index('my_index', my_table.c.id, postgres_where=tbl.c.value > 10)

PostgreSQL 8.2+ supports returning a result set from inserts and updates.
To use this pass the column/expression list to the postgres_returning
parameter when creating the queries::

  raises = tbl.update(empl.c.sales > 100, values=dict(salary=empl.c.salary * 1.1),
    postgres_returning=[empl.c.id, empl.c.salary]).execute().fetchall()
"""

import random, re, string

from sqlalchemy import sql, schema, exceptions, util
from sqlalchemy.engine import base, default
from sqlalchemy.sql import compiler, expression
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import types as sqltypes


class PGInet(sqltypes.TypeEngine):
    def get_col_spec(self):
        return "INET"

class PGCidr(sqltypes.TypeEngine):
    def get_col_spec(self):
        return "CIDR"

class PGMacAddr(sqltypes.TypeEngine):
    def get_col_spec(self):
        return "MACADDR"

class PGNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        if not self.precision:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}

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

class PGFloat(sqltypes.Float):
    def get_col_spec(self):
        if not self.precision:
            return "FLOAT"
        else:
            return "FLOAT(%(precision)s)" % {'precision': self.precision}


class PGInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"

class PGSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"

class PGBigInteger(PGInteger):
    def get_col_spec(self):
        return "BIGINT"

class PGDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "TIMESTAMP " + (self.timezone and "WITH" or "WITHOUT") + " TIME ZONE"

class PGDate(sqltypes.Date):
    def get_col_spec(self):
        return "DATE"

class PGTime(sqltypes.Time):
    def get_col_spec(self):
        return "TIME " + (self.timezone and "WITH" or "WITHOUT") + " TIME ZONE"

class PGInterval(sqltypes.TypeEngine):
    def get_col_spec(self):
        return "INTERVAL"

class PGText(sqltypes.Text):
    def get_col_spec(self):
        return "TEXT"

class PGString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}

class PGChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}

class PGBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "BYTEA"

class PGBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BOOLEAN"

class PGArray(sqltypes.MutableType, sqltypes.Concatenable, sqltypes.TypeEngine):
    def __init__(self, item_type, mutable=True):
        if isinstance(item_type, type):
            item_type = item_type()
        self.item_type = item_type
        self.mutable = mutable

    def copy_value(self, value):
        if value is None:
            return None
        elif self.mutable:
            return list(value)
        else:
            return value

    def compare_values(self, x, y):
        return x == y

    def is_mutable(self):
        return self.mutable

    def dialect_impl(self, dialect, **kwargs):
        impl = self.__class__.__new__(self.__class__)
        impl.__dict__.update(self.__dict__)
        impl.item_type = self.item_type.dialect_impl(dialect)
        return impl

    def bind_processor(self, dialect):
        item_proc = self.item_type.bind_processor(dialect)
        def process(value):
            if value is None:
                return value
            def convert_item(item):
                if isinstance(item, (list,tuple)):
                    return [convert_item(child) for child in item]
                else:
                    if item_proc:
                        return item_proc(item)
                    else:
                        return item
            return [convert_item(item) for item in value]
        return process

    def result_processor(self, dialect):
        item_proc = self.item_type.result_processor(dialect)
        def process(value):
            if value is None:
                return value
            def convert_item(item):
                if isinstance(item, list):
                    return [convert_item(child) for child in item]
                else:
                    if item_proc:
                        return item_proc(item)
                    else:
                        return item
            return [convert_item(item) for item in value]
        return process
    def get_col_spec(self):
        return self.item_type.get_col_spec() + '[]'

colspecs = {
    sqltypes.Integer : PGInteger,
    sqltypes.Smallinteger : PGSmallInteger,
    sqltypes.Numeric : PGNumeric,
    sqltypes.Float : PGFloat,
    sqltypes.DateTime : PGDateTime,
    sqltypes.Date : PGDate,
    sqltypes.Time : PGTime,
    sqltypes.String : PGString,
    sqltypes.Binary : PGBinary,
    sqltypes.Boolean : PGBoolean,
    sqltypes.Text : PGText,
    sqltypes.CHAR: PGChar,
}

ischema_names = {
    'integer' : PGInteger,
    'bigint' : PGBigInteger,
    'smallint' : PGSmallInteger,
    'character varying' : PGString,
    'character' : PGChar,
    'text' : PGText,
    'numeric' : PGNumeric,
    'float' : PGFloat,
    'real' : PGFloat,
    'inet': PGInet,
    'cidr': PGCidr,
    'macaddr': PGMacAddr,
    'double precision' : PGFloat,
    'timestamp' : PGDateTime,
    'timestamp with time zone' : PGDateTime,
    'timestamp without time zone' : PGDateTime,
    'time with time zone' : PGTime,
    'time without time zone' : PGTime,
    'date' : PGDate,
    'time': PGTime,
    'bytea' : PGBinary,
    'boolean' : PGBoolean,
    'interval':PGInterval,
}

def descriptor():
    return {'name':'postgres',
    'description':'PostGres',
    'arguments':[
        ('username',"Database Username",None),
        ('password',"Database Password",None),
        ('database',"Database Name",None),
        ('host',"Hostname", None),
    ]}

SERVER_SIDE_CURSOR_RE = re.compile(
    r'\s*SELECT',
    re.I | re.UNICODE)

SELECT_RE = re.compile(
    r'\s*(?:SELECT|FETCH|(UPDATE|INSERT))',
    re.I | re.UNICODE)

RETURNING_RE = re.compile(
    'RETURNING',
    re.I | re.UNICODE)

# This finds if the RETURNING is not inside a quoted/commented values. Handles string literals,
# quoted identifiers, dollar quotes, SQL comments and C style multiline comments. This does not
# handle correctly nested C style quotes, lets hope no one does the following:
# UPDATE tbl SET x=y /* foo /* bar */ RETURNING */
RETURNING_QUOTED_RE = re.compile(
    """\s*(?:UPDATE|INSERT)\s
        (?: # handle quoted and commented tokens separately
            [^'"$/-] # non quote/comment character
            | -(?!-) # a dash that does not begin a comment
            | /(?!\*) # a slash that does not begin a comment
            | "(?:[^"]|"")*" # quoted literal
            | '(?:[^']|'')*' # quoted string
            | \$(?P<dquote>[^$]*)\$.*?\$(?P=dquote)\$ # dollar quotes
            | --[^\\n]*(?=\\n) # SQL comment, leave out line ending as that counts as whitespace
                            # for the returning token
            | /\*([^*]|\*(?!/))*\*/ # C style comment, doesn't handle nesting
        )*
        \sRETURNING\s""", re.I | re.UNICODE | re.VERBOSE)

class PGExecutionContext(default.DefaultExecutionContext):
    def returns_rows_text(self, statement):
        m = SELECT_RE.match(statement)
        return m and (not m.group(1) or (RETURNING_RE.search(statement)
           and RETURNING_QUOTED_RE.match(statement)))

    def returns_rows_compiled(self, compiled):
        return isinstance(compiled.statement, expression.Selectable) or \
            (
                (compiled.isupdate or compiled.isinsert) and "postgres_returning" in compiled.statement.kwargs
            )

    def create_cursor(self):
        self.__is_server_side = \
            self.dialect.server_side_cursors and \
            ((self.compiled and isinstance(self.compiled.statement, expression.Selectable)) \
            or \
            (
                (not self.compiled or isinstance(self.compiled.statement, expression._TextClause)) 
                and self.statement and SERVER_SIDE_CURSOR_RE.match(self.statement))
            )

        if self.__is_server_side:
            # use server-side cursors:
            # http://lists.initd.org/pipermail/psycopg/2007-January/005251.html
            ident = "c_%s_%s" % (hex(id(self))[2:], hex(random.randint(0, 65535))[2:])
            return self._connection.connection.cursor(ident)
        else:
            return self._connection.connection.cursor()
    
    def get_result_proxy(self):
        if self.__is_server_side:
            return base.BufferedRowResultProxy(self)
        else:
            return base.ResultProxy(self)

    def post_exec(self):
        if self.compiled.isinsert and self.last_inserted_ids is None:
            if not self.dialect.use_oids:
                pass
                # will raise invalid error when they go to get them
            else:
                table = self.compiled.statement.table
                if self.cursor.lastrowid is not None and table is not None and len(table.primary_key):
                    s = sql.select(table.primary_key, table.oid_column == self.cursor.lastrowid)
                    row = self.connection.execute(s).fetchone()
                self._last_inserted_ids = [v for v in row]
        super(PGExecutionContext, self).post_exec()

class PGDialect(default.DefaultDialect):
    supports_alter = True
    supports_unicode_statements = False
    max_identifier_length = 63
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    preexecute_pk_sequences = True
    supports_pk_autoincrement = False
    default_paramstyle = 'pyformat'

    def __init__(self, use_oids=False, server_side_cursors=False, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)
        self.use_oids = use_oids
        self.server_side_cursors = server_side_cursors

    def dbapi(cls):
        import psycopg2 as psycopg
        return psycopg
    dbapi = classmethod(dbapi)

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if 'port' in opts:
            opts['port'] = int(opts['port'])
        opts.update(url.query)
        return ([], opts)

    def create_execution_context(self, *args, **kwargs):
        return PGExecutionContext(self, *args, **kwargs)

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def do_begin_twophase(self, connection, xid):
        self.do_begin(connection.connection)

    def do_prepare_twophase(self, connection, xid):
        connection.execute(sql.text("PREPARE TRANSACTION :tid", bindparams=[sql.bindparam('tid', xid)]))

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        if is_prepared:
            if recover:
                #FIXME: ugly hack to get out of transaction context when commiting recoverable transactions
                # Must find out a way how to make the dbapi not open a transaction.
                connection.execute(sql.text("ROLLBACK"))
            connection.execute(sql.text("ROLLBACK PREPARED :tid", bindparams=[sql.bindparam('tid', xid)]))
            connection.execute(sql.text("BEGIN"))
            self.do_rollback(connection.connection)
        else:
            self.do_rollback(connection.connection)

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        if is_prepared:
            if recover:
                connection.execute(sql.text("ROLLBACK"))
            connection.execute(sql.text("COMMIT PREPARED :tid", bindparams=[sql.bindparam('tid', xid)]))
            connection.execute(sql.text("BEGIN"))
            self.do_rollback(connection.connection)
        else:
            self.do_commit(connection.connection)

    def do_recover_twophase(self, connection):
        resultset = connection.execute(sql.text("SELECT gid FROM pg_prepared_xacts"))
        return [row[0] for row in resultset]

    def get_default_schema_name(self, connection):
        return connection.scalar("select current_schema()", None)
    get_default_schema_name = base.connection_memoize(
        ('dialect', 'default_schema_name'))(get_default_schema_name)

    def last_inserted_ids(self):
        if self.context.last_inserted_ids is None:
            raise exceptions.InvalidRequestError("no INSERT executed, or can't use cursor.lastrowid without Postgres OIDs enabled")
        else:
            return self.context.last_inserted_ids

    def oid_column_name(self, column):
        if self.use_oids:
            return "oid"
        else:
            return None

    def has_table(self, connection, table_name, schema=None):
        # seems like case gets folded in pg_class...
        if schema is None:
            cursor = connection.execute("""select relname from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname=current_schema() and lower(relname)=%(name)s""", {'name':table_name.lower().encode(self.encoding)});
        else:
            cursor = connection.execute("""select relname from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname=%(schema)s and lower(relname)=%(name)s""", {'name':table_name.lower().encode(self.encoding), 'schema':schema});
        return bool( not not cursor.rowcount )

    def has_sequence(self, connection, sequence_name):
        cursor = connection.execute('''SELECT relname FROM pg_class WHERE relkind = 'S' AND relnamespace IN ( SELECT oid FROM pg_namespace WHERE nspname NOT LIKE 'pg_%%' AND nspname != 'information_schema' AND relname = %(seqname)s);''', {'seqname': sequence_name.encode(self.encoding)})
        return bool(not not cursor.rowcount)

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.OperationalError):
            return 'closed the connection' in str(e) or 'connection not open' in str(e)
        elif isinstance(e, self.dbapi.InterfaceError):
            return 'connection already closed' in str(e) or 'cursor already closed' in str(e)
        elif isinstance(e, self.dbapi.ProgrammingError):
            # yes, it really says "losed", not "closed"
            return "losed the connection unexpectedly" in str(e)
        else:
            return False

    def table_names(self, connection, schema):
        s = """
        SELECT relname
        FROM pg_class c
        WHERE relkind = 'r'
          AND '%(schema)s' = (select nspname from pg_namespace n where n.oid = c.relnamespace)
        """ % locals()
        return [row[0].decode(self.encoding) for row in connection.execute(s)]

    def server_version_info(self, connection):
        v = connection.execute("select version()").scalar()
        m = re.match('PostgreSQL (\d+)\.(\d+)\.(\d+)', v)
        if not m:
            raise exceptions.AssertionError("Could not determine version from string '%s'" % v)
        return tuple([int(x) for x in m.group(1, 2, 3)])

    def reflecttable(self, connection, table, include_columns):
        preparer = self.identifier_preparer
        if table.schema is not None:
            schema_where_clause = "n.nspname = :schema"
            schemaname = table.schema
            if isinstance(schemaname, str):
                schemaname = schemaname.decode(self.encoding)
        else:
            schema_where_clause = "pg_catalog.pg_table_is_visible(c.oid)"
            schemaname = None

        SQL_COLS = """
            SELECT a.attname,
              pg_catalog.format_type(a.atttypid, a.atttypmod),
              (SELECT substring(d.adsrc for 128) FROM pg_catalog.pg_attrdef d
               WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
              AS DEFAULT,
              a.attnotnull, a.attnum, a.attrelid as table_oid
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = (
                SELECT c.oid
                FROM pg_catalog.pg_class c
                     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                     WHERE (%s)
                     AND c.relname = :table_name AND c.relkind in ('r','v')
            ) AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
        """ % schema_where_clause

        s = sql.text(SQL_COLS, bindparams=[sql.bindparam('table_name', type_=sqltypes.Unicode), sql.bindparam('schema', type_=sqltypes.Unicode)], typemap={'attname':sqltypes.Unicode, 'default':sqltypes.Unicode})
        tablename = table.name
        if isinstance(tablename, str):
            tablename = tablename.decode(self.encoding)
        c = connection.execute(s, table_name=tablename, schema=schemaname)
        rows = c.fetchall()

        if not rows:
            raise exceptions.NoSuchTableError(table.name)

        domains = self._load_domains(connection)

        for name, format_type, default, notnull, attnum, table_oid in rows:
            if include_columns and name not in include_columns:
                continue

            ## strip (30) from character varying(30)
            attype = re.search('([^\([]+)', format_type).group(1)
            nullable = not notnull
            is_array = format_type.endswith('[]')

            try:
                charlen = re.search('\(([\d,]+)\)', format_type).group(1)
            except:
                charlen = False

            numericprec = False
            numericscale = False
            if attype == 'numeric':
                if charlen is False:
                    numericprec, numericscale = (None, None)
                else:
                    numericprec, numericscale = charlen.split(',')
                charlen = False
            if attype == 'double precision':
                numericprec, numericscale = (53, False)
                charlen = False
            if attype == 'integer':
                numericprec, numericscale = (32, 0)
                charlen = False

            args = []
            for a in (charlen, numericprec, numericscale):
                if a is None:
                    args.append(None)
                elif a is not False:
                    args.append(int(a))

            kwargs = {}
            if attype == 'timestamp with time zone':
                kwargs['timezone'] = True
            elif attype == 'timestamp without time zone':
                kwargs['timezone'] = False

            if attype in ischema_names:
                coltype = ischema_names[attype]
            else:
                if attype in domains:
                    domain = domains[attype]
                    if domain['attype'] in ischema_names:
                        # A table can't override whether the domain is nullable.
                        nullable = domain['nullable']

                        if domain['default'] and not default:
                            # It can, however, override the default value, but can't set it to null.
                            default = domain['default']
                        coltype = ischema_names[domain['attype']]
                else:
                    coltype=None

            if coltype:
                coltype = coltype(*args, **kwargs)
                if is_array:
                    coltype = PGArray(coltype)
            else:
                util.warn("Did not recognize type '%s' of column '%s'" %
                          (attype, name))
                coltype = sqltypes.NULLTYPE

            colargs= []
            if default is not None:
                match = re.search(r"""(nextval\(')([^']+)('.*$)""", default)
                if match is not None:
                    # the default is related to a Sequence
                    sch = table.schema
                    if '.' not in match.group(2) and sch is not None:
                        # unconditionally quote the schema name.  this could
                        # later be enhanced to obey quoting rules / "quote schema"
                        default = match.group(1) + ('"%s"' % sch) + '.' + match.group(2) + match.group(3)
                colargs.append(schema.PassiveDefault(sql.text(default)))
            table.append_column(schema.Column(name, coltype, nullable=nullable, *colargs))


        # Primary keys
        PK_SQL = """
          SELECT attname FROM pg_attribute
          WHERE attrelid = (
             SELECT indexrelid FROM pg_index i
             WHERE i.indrelid = :table
             AND i.indisprimary = 't')
          ORDER BY attnum
        """
        t = sql.text(PK_SQL, typemap={'attname':sqltypes.Unicode})
        c = connection.execute(t, table=table_oid)
        for row in c.fetchall():
            pk = row[0]
            col = table.c[pk]
            table.primary_key.add(col)
            if col.default is None:
                col.autoincrement=False

        # Foreign keys
        FK_SQL = """
          SELECT conname, pg_catalog.pg_get_constraintdef(oid, true) as condef
          FROM  pg_catalog.pg_constraint r
          WHERE r.conrelid = :table AND r.contype = 'f'
          ORDER BY 1
        """

        t = sql.text(FK_SQL, typemap={'conname':sqltypes.Unicode, 'condef':sqltypes.Unicode})
        c = connection.execute(t, table=table_oid)
        for conname, condef in c.fetchall():
            m = re.search('FOREIGN KEY \((.*?)\) REFERENCES (?:(.*?)\.)?(.*?)\((.*?)\)', condef).groups()
            (constrained_columns, referred_schema, referred_table, referred_columns) = m
            constrained_columns = [preparer._unquote_identifier(x) for x in re.split(r'\s*,\s*', constrained_columns)]
            if referred_schema:
                referred_schema = preparer._unquote_identifier(referred_schema)
            elif table.schema is not None and table.schema == self.get_default_schema_name(connection):
                # no schema (i.e. its the default schema), and the table we're
                # reflecting has the default schema explicit, then use that.
                # i.e. try to use the user's conventions
                referred_schema = table.schema
            referred_table = preparer._unquote_identifier(referred_table)
            referred_columns = [preparer._unquote_identifier(x) for x in re.split(r'\s*,\s', referred_columns)]

            refspec = []
            if referred_schema is not None:
                schema.Table(referred_table, table.metadata, autoload=True, schema=referred_schema,
                            autoload_with=connection)
                for column in referred_columns:
                    refspec.append(".".join([referred_schema, referred_table, column]))
            else:
                schema.Table(referred_table, table.metadata, autoload=True, autoload_with=connection)
                for column in referred_columns:
                    refspec.append(".".join([referred_table, column]))

            table.append_constraint(schema.ForeignKeyConstraint(constrained_columns, refspec, conname))

    def _load_domains(self, connection):
        ## Load data types for domains:
        SQL_DOMAINS = """
            SELECT t.typname as "name",
                   pg_catalog.format_type(t.typbasetype, t.typtypmod) as "attype",
                   not t.typnotnull as "nullable",
                   t.typdefault as "default",
                   pg_catalog.pg_type_is_visible(t.oid) as "visible",
                   n.nspname as "schema"
            FROM pg_catalog.pg_type t
                 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                 LEFT JOIN pg_catalog.pg_constraint r ON t.oid = r.contypid
            WHERE t.typtype = 'd'
        """

        s = sql.text(SQL_DOMAINS, typemap={'attname':sqltypes.Unicode})
        c = connection.execute(s)

        domains = {}
        for domain in c.fetchall():
            ## strip (30) from character varying(30)
            attype = re.search('([^\(]+)', domain['attype']).group(1)
            if domain['visible']:
                # 'visible' just means whether or not the domain is in a
                # schema that's on the search path -- or not overriden by
                # a schema with higher presedence. If it's not visible,
                # it will be prefixed with the schema-name when it's used.
                name = domain['name']
            else:
                name = "%s.%s" % (domain['schema'], domain['name'])

            domains[name] = {'attype':attype, 'nullable': domain['nullable'], 'default': domain['default']}

        return domains



class PGCompiler(compiler.DefaultCompiler):
    operators = compiler.DefaultCompiler.operators.copy()
    operators.update(
        {
            sql_operators.mod : '%%',
            sql_operators.ilike_op: lambda x, y, escape=None: '%s ILIKE %s' % (x, y) + (escape and ' ESCAPE \'%s\'' % escape or ''),
            sql_operators.notilike_op: lambda x, y, escape=None: '%s NOT ILIKE %s' % (x, y) + (escape and ' ESCAPE \'%s\'' % escape or ''),
        }
    )

    functions = compiler.DefaultCompiler.functions.copy()
    functions.update (
        {
            'TIMESTAMP':lambda x:'TIMESTAMP %s' % x,
        }
    )

    def visit_sequence(self, seq):
        if seq.optional:
            return None
        else:
            return "nextval('%s')" % self.preparer.format_sequence(seq)

    def limit_clause(self, select):
        text = ""
        if select._limit is not None:
            text +=  " \n LIMIT " + str(select._limit)
        if select._offset is not None:
            if select._limit is None:
                text += " \n LIMIT ALL"
            text += " OFFSET " + str(select._offset)
        return text

    def get_select_precolumns(self, select):
        if select._distinct:
            if isinstance(select._distinct, bool):
                return "DISTINCT "
            elif isinstance(select._distinct, (list, tuple)):
                return "DISTINCT ON (" + ', '.join(
                    [(isinstance(col, basestring) and col or self.process(col)) for col in select._distinct]
                )+ ") "
            else:
                return "DISTINCT ON (" + unicode(select._distinct) + ") "
        else:
            return ""

    def for_update_clause(self, select):
        if select.for_update == 'nowait':
            return " FOR UPDATE NOWAIT"
        else:
            return super(PGCompiler, self).for_update_clause(select)

    def _append_returning(self, text, stmt):
        returning_cols = stmt.kwargs['postgres_returning']
        def flatten_columnlist(collist):
            for c in collist:
                if isinstance(c, expression.Selectable):
                    for co in c.columns:
                        yield co
                else:
                    yield c
        columns = [self.process(c) for c in flatten_columnlist(returning_cols)]
        text += ' RETURNING ' + string.join(columns, ', ')
        return text

    def visit_update(self, update_stmt):
        text = super(PGCompiler, self).visit_update(update_stmt)
        if 'postgres_returning' in update_stmt.kwargs:
            return self._append_returning(text, update_stmt)
        else:
            return text

    def visit_insert(self, insert_stmt):
        text = super(PGCompiler, self).visit_insert(insert_stmt)
        if 'postgres_returning' in insert_stmt.kwargs:
            return self._append_returning(text, insert_stmt)
        else:
            return text

class PGSchemaGenerator(compiler.SchemaGenerator):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        if column.primary_key and len(column.foreign_keys)==0 and column.autoincrement and isinstance(column.type, sqltypes.Integer) and not isinstance(column.type, sqltypes.SmallInteger) and (column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional)):
            if isinstance(column.type, PGBigInteger):
                colspec += " BIGSERIAL"
            else:
                colspec += " SERIAL"
        else:
            colspec += " " + column.type.dialect_impl(self.dialect, _for_ddl=column).get_col_spec()
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    def visit_sequence(self, sequence):
        if not sequence.optional and (not self.checkfirst or not self.dialect.has_sequence(self.connection, sequence.name)):
            self.append("CREATE SEQUENCE %s" % self.preparer.format_sequence(sequence))
            self.execute()

    def visit_index(self, index):
        preparer = self.preparer
        self.append("CREATE ")
        if index.unique:
            self.append("UNIQUE ")
        self.append("INDEX %s ON %s (%s)" \
                    % (preparer.quote(index, self._validate_identifier(index.name, True)),
                       preparer.format_table(index.table),
                       string.join([preparer.format_column(c) for c in index.columns], ', ')))
        whereclause = index.kwargs.get('postgres_where', None)
        if whereclause is not None:
            compiler = self._compile(whereclause, None)
            # this might belong to the compiler class
            inlined_clause = str(compiler) % dict(
                [(key,bind.value) for key,bind in compiler.binds.iteritems()])
            self.append(" WHERE " + inlined_clause)
        self.execute()

class PGSchemaDropper(compiler.SchemaDropper):
    def visit_sequence(self, sequence):
        if not sequence.optional and (not self.checkfirst or self.dialect.has_sequence(self.connection, sequence.name)):
            self.append("DROP SEQUENCE %s" % self.preparer.format_sequence(sequence))
            self.execute()

class PGDefaultRunner(base.DefaultRunner):
    def __init__(self, context):
        base.DefaultRunner.__init__(self, context)
        # craete cursor which won't conflict with a server-side cursor
        self.cursor = context._connection.connection.cursor()
    
    def get_column_default(self, column, isinsert=True):
        if column.primary_key:
            # pre-execute passive defaults on primary keys
            if isinstance(column.default, schema.PassiveDefault):
                return self.execute_string("select %s" % column.default.arg)
            elif (isinstance(column.type, sqltypes.Integer) and column.autoincrement) and (column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional)):
                sch = column.table.schema
                # TODO: this has to build into the Sequence object so we can get the quoting
                # logic from it
                if sch is not None:
                    exc = "select nextval('\"%s\".\"%s_%s_seq\"')" % (sch, column.table.name, column.name)
                else:
                    exc = "select nextval('\"%s_%s_seq\"')" % (column.table.name, column.name)
                return self.execute_string(exc.encode(self.dialect.encoding))

        return super(PGDefaultRunner, self).get_column_default(column)

    def visit_sequence(self, seq):
        if not seq.optional:
            return self.execute_string(("select nextval('%s')" % self.dialect.identifier_preparer.format_sequence(seq)))
        else:
            return None

class PGIdentifierPreparer(compiler.IdentifierPreparer):
    def _unquote_identifier(self, value):
        if value[0] == self.initial_quote:
            value = value[1:-1].replace('""','"')
        return value

dialect = PGDialect
dialect.statement_compiler = PGCompiler
dialect.schemagenerator = PGSchemaGenerator
dialect.schemadropper = PGSchemaDropper
dialect.preparer = PGIdentifierPreparer
dialect.defaultrunner = PGDefaultRunner
