# postgres.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for the PostgreSQL database.  

For information on connecting using specific drivers, see the documentation section
regarding that driver.

Sequences/SERIAL
----------------

Postgres supports sequences, and SQLAlchemy uses these as the default means of creating
new primary key values for integer-based primary key columns.   When creating tables, 
SQLAlchemy will issue the ``SERIAL`` datatype for integer-based primary key columns, 
which generates a sequence corresponding to the column and associated with it based on
a naming convention.

To specify a specific named sequence to be used for primary key generation, use the
:func:`~sqlalchemy.schema.Sequence` construct::

    Table('sometable', metadata, 
            Column('id', Integer, Sequence('some_id_seq'), primary_key=True)
        )

Currently, when SQLAlchemy issues a single insert statement, to fulfill the contract of
having the "last insert identifier" available, the sequence is executed independently
beforehand and the new value is retrieved, to be used in the subsequent insert.  Note
that when an :func:`~sqlalchemy.sql.expression.insert()` construct is executed using 
"executemany" semantics, the sequence is not pre-executed and normal PG SERIAL behavior
is used.

Postgres 8.3 supports an ``INSERT...RETURNING`` syntax which SQLAlchemy supports 
as well.  A future release of SQLA will use this feature by default in lieu of 
sequence pre-execution in order to retrieve new primary key values, when available.

INSERT/UPDATE...RETURNING
-------------------------

The dialect supports PG 8.3's ``INSERT..RETURNING`` and ``UPDATE..RETURNING`` syntaxes, 
but must be explicitly enabled on a per-statement basis::

    # INSERT..RETURNING
    result = table.insert(postgres_returning=[table.c.col1, table.c.col2]).\\
        values(name='foo')
    print result.fetchall()
    
    # UPDATE..RETURNING
    result = table.update(postgres_returning=[table.c.col1, table.c.col2]).\\
        where(table.c.name=='foo').values(name='bar')
    print result.fetchall()

Indexes
-------

PostgreSQL supports partial indexes. To create them pass a postgres_where
option to the Index constructor::

  Index('my_index', my_table.c.id, postgres_where=tbl.c.value > 10)



"""

import re, string

from sqlalchemy import sql, schema, exc, util
from sqlalchemy.engine import base, default
from sqlalchemy.sql import compiler, expression
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import types as sqltypes


class PGInet(sqltypes.TypeEngine):
    __visit_name__ = "INET"

class PGCidr(sqltypes.TypeEngine):
    __visit_name__ = "CIDR"

class PGMacAddr(sqltypes.TypeEngine):
    __visit_name__ = "MACADDR"

class PGInterval(sqltypes.TypeEngine):
    __visit_name__ = 'INTERVAL'

class PGArray(sqltypes.MutableType, sqltypes.Concatenable, sqltypes.TypeEngine):
    __visit_name__ = 'ARRAY'
    
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
                if isinstance(item, (list, tuple)):
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


colspecs = {
    sqltypes.Interval:PGInterval
}

ischema_names = {
    'integer' : sqltypes.INTEGER,
    'bigint' : sqltypes.BigInteger,
    'smallint' : sqltypes.SMALLINT,
    'character varying' : sqltypes.VARCHAR,
    'character' : sqltypes.CHAR,
    '"char"' : sqltypes.String,
    'name' : sqltypes.String,
    'text' : sqltypes.TEXT,
    'numeric' : sqltypes.NUMERIC,
    'float' : sqltypes.FLOAT,
    'real' : sqltypes.Float,
    'inet': PGInet,
    'cidr': PGCidr,
    'macaddr': PGMacAddr,
    'double precision' : sqltypes.Float,
    'timestamp' : sqltypes.TIMESTAMP,
    'timestamp with time zone' : sqltypes.TIMESTAMP,
    'timestamp without time zone' : sqltypes.TIMESTAMP,
    'time with time zone' : sqltypes.TIME,
    'time without time zone' : sqltypes.TIME,
    'date' : sqltypes.DATE,
    'time': sqltypes.TIME,
    'bytea' : sqltypes.Binary,
    'boolean' : sqltypes.BOOLEAN,
    'interval':PGInterval,
}



class PGCompiler(compiler.SQLCompiler):
    
    operators = util.update_copy(
        compiler.SQLCompiler.operators,
        {
            sql_operators.mod : '%%',
        
            sql_operators.ilike_op: lambda x, y, escape=None: '%s ILIKE %s' % (x, y) + (escape and ' ESCAPE \'%s\'' % escape or ''),
            sql_operators.notilike_op: lambda x, y, escape=None: '%s NOT ILIKE %s' % (x, y) + (escape and ' ESCAPE \'%s\'' % escape or ''),
            sql_operators.match_op: lambda x, y: '%s @@ to_tsquery(%s)' % (x, y),
        }
    )

    functions = util.update_copy(
        compiler.SQLCompiler.functions,
        {
            'TIMESTAMP':lambda x:'TIMESTAMP %s' % x,
        }
    )

    def post_process_text(self, text):
        if '%%' in text:
            util.warn("The SQLAlchemy postgres dialect now automatically escapes '%' in text() expressions to '%%'.")
        return text.replace('%', '%%')

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
        columns = [self.process(c, within_columns_clause=True) for c in flatten_columnlist(returning_cols)]
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

class PGDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        if column.primary_key and \
            len(column.foreign_keys)==0 and \
            column.autoincrement and \
            isinstance(column.type, sqltypes.Integer) and \
            not isinstance(column.type, sqltypes.SmallInteger) and \
            (column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional)):
            if isinstance(column.type, sqltypes.BigInteger):
                colspec += " BIGSERIAL"
            else:
                colspec += " SERIAL"
        else:
            colspec += " " + self.dialect.type_compiler.process(column.type)
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    def visit_create_sequence(self, create):
        return "CREATE SEQUENCE %s" % self.preparer.format_sequence(create.element)
            
    def visit_drop_sequence(self, drop):
        return "DROP SEQUENCE %s" % self.preparer.format_sequence(drop.element)
        
    def visit_create_index(self, create):
        preparer = self.preparer
        index = create.element
        text = "CREATE "
        if index.unique:
            text += "UNIQUE "
        text += "INDEX %s ON %s (%s)" \
                    % (preparer.quote(self._validate_identifier(index.name, True), index.quote),
                       preparer.format_table(index.table),
                       string.join([preparer.format_column(c) for c in index.columns], ', '))
                       
        whereclause = index.kwargs.get('postgres_where', None)
        if whereclause is not None:
            compiler = self._compile(whereclause, None)
            # this might belong to the compiler class
            inlined_clause = str(compiler) % dict(
                [(key,bind.value) for key,bind in compiler.binds.iteritems()])
            text += " WHERE " + inlined_clause
        return text

class PGDefaultRunner(base.DefaultRunner):
    def __init__(self, context):
        base.DefaultRunner.__init__(self, context)
        # craete cursor which won't conflict with a server-side cursor
        self.cursor = context._connection.connection.cursor()
    
    def get_column_default(self, column, isinsert=True):
        if column.primary_key:
            # pre-execute passive defaults on primary keys
            if (isinstance(column.server_default, schema.DefaultClause) and
                column.server_default.arg is not None):
                return self.execute_string("select %s" % column.server_default.arg)
            elif (isinstance(column.type, sqltypes.Integer) and column.autoincrement) and (column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional)):
                sch = column.table.schema
                # TODO: this has to build into the Sequence object so we can get the quoting
                # logic from it
                if sch is not None:
                    exc = "select nextval('\"%s\".\"%s_%s_seq\"')" % (sch, column.table.name, column.name)
                else:
                    exc = "select nextval('\"%s_%s_seq\"')" % (column.table.name, column.name)

                if self.dialect.supports_unicode_statements:
                    return self.execute_string(exc)
                else:
                    return self.execute_string(exc.encode(self.dialect.encoding))

        return super(PGDefaultRunner, self).get_column_default(column)

    def visit_sequence(self, seq):
        if not seq.optional:
            return self.execute_string(("select nextval('%s')" % self.dialect.identifier_preparer.format_sequence(seq)))
        else:
            return None

class PGTypeCompiler(compiler.GenericTypeCompiler):
    def visit_INET(self, type_):
        return "INET"

    def visit_CIDR(self, type_):
        return "CIDR"

    def visit_MACADDR(self, type_):
        return "MACADDR"

    def visit_FLOAT(self, type_):
        if not type_.precision:
            return "FLOAT"
        else:
            return "FLOAT(%(precision)s)" % {'precision': type_.precision}

    def visit_BIGINT(self, type_):
        return "BIGINT"

    def visit_datetime(self, type_):
        return self.visit_TIMESTAMP(type_)
        
    def visit_TIMESTAMP(self, type_):
        return "TIMESTAMP " + (type_.timezone and "WITH" or "WITHOUT") + " TIME ZONE"

    def visit_TIME(self, type_):
        return "TIME " + (type_.timezone and "WITH" or "WITHOUT") + " TIME ZONE"

    def visit_INTERVAL(self, type_):
        return "INTERVAL"

    def visit_binary(self, type_):
        return self.visit_BYTEA(type_)
        
    def visit_BYTEA(self, type_):
        return "BYTEA"

    def visit_ARRAY(self, type_):
        return self.process(type_.item_type) + '[]'

class PGIdentifierPreparer(compiler.IdentifierPreparer):
    def _unquote_identifier(self, value):
        if value[0] == self.initial_quote:
            value = value[1:-1].replace('""','"')
        return value

class PGDialect(default.DefaultDialect):
    name = 'postgres'
    supports_alter = True
    max_identifier_length = 63
    supports_sane_rowcount = True
    supports_sequences = True
    sequences_optional = True
    preexecute_pk_sequences = True
    supports_pk_autoincrement = False
    supports_default_values = True
    supports_empty_insert = False
    default_paramstyle = 'pyformat'
    ischema_names = ischema_names
    colspecs = colspecs
    
    statement_compiler = PGCompiler
    ddl_compiler = PGDDLCompiler
    type_compiler = PGTypeCompiler
    preparer = PGIdentifierPreparer
    defaultrunner = PGDefaultRunner


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

    @base.connection_memoize(('dialect', 'default_schema_name'))
    def get_default_schema_name(self, connection):
        return connection.scalar("select current_schema()", None)

    def has_table(self, connection, table_name, schema=None):
        # seems like case gets folded in pg_class...
        if schema is None:
            cursor = connection.execute(
                sql.text("""select relname from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname=current_schema() and lower(relname)=:name""",
                    bindparams=[sql.bindparam('name', unicode(table_name.lower()), type_=sqltypes.Unicode)]
                )
            )
        else:
            cursor = connection.execute(
                sql.text("""select relname from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname=:schema and lower(relname)=:name""",
                    bindparams=[sql.bindparam('name', unicode(table_name.lower()), type_=sqltypes.Unicode),
                        sql.bindparam('schema', unicode(schema), type_=sqltypes.Unicode)] 
                )
            )
        return bool( cursor.rowcount )

    def has_sequence(self, connection, sequence_name):
        cursor = connection.execute(
                    sql.text("SELECT relname FROM pg_class WHERE relkind = 'S' AND "
                        "relnamespace IN ( SELECT oid FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' "
                        "AND nspname != 'information_schema' AND relname = :seqname)", 
                        bindparams=[sql.bindparam('seqname', unicode(sequence_name), type_=sqltypes.Unicode)]
                    ))
        return bool(cursor.rowcount)

    def table_names(self, connection, schema):
        result = connection.execute(
            sql.text(u"""SELECT relname
                FROM pg_class c
                WHERE relkind = 'r'
                AND '%s' = (select nspname from pg_namespace n where n.oid = c.relnamespace)""" % schema,
                typemap = {'relname':sqltypes.Unicode}
            )
        )
        return [row[0] for row in result]

    def server_version_info(self, connection):
        v = connection.execute("select version()").scalar()
        m = re.match('PostgreSQL (\d+)\.(\d+)\.(\d+)', v)
        if not m:
            raise AssertionError("Could not determine version from string '%s'" % v)
        return tuple([int(x) for x in m.group(1, 2, 3)])

    def reflecttable(self, connection, table, include_columns):
        preparer = self.identifier_preparer
        if table.schema is not None:
            schema_where_clause = "n.nspname = :schema"
            schemaname = table.schema
            
            # Py2K
            if isinstance(schemaname, str):
                schemaname = schemaname.decode(self.encoding)
            # end Py2K
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
        # Py2K
        if isinstance(tablename, str):
            tablename = tablename.decode(self.encoding)
        # end Py2K
        c = connection.execute(s, table_name=tablename, schema=schemaname)
        rows = c.fetchall()

        if not rows:
            raise exc.NoSuchTableError(table.name)

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

            if attype in self.ischema_names:
                coltype = self.ischema_names[attype]
            else:
                if attype in domains:
                    domain = domains[attype]
                    if domain['attype'] in self.ischema_names:
                        # A table can't override whether the domain is nullable.
                        nullable = domain['nullable']

                        if domain['default'] and not default:
                            # It can, however, override the default value, but can't set it to null.
                            default = domain['default']
                        coltype = self.ischema_names[domain['attype']]
                else:
                    coltype = None

            if coltype:
                coltype = coltype(*args, **kwargs)
                if is_array:
                    coltype = PGArray(coltype)
            else:
                util.warn("Did not recognize type '%s' of column '%s'" %
                          (attype, name))
                coltype = sqltypes.NULLTYPE

            colargs = []
            if default is not None:
                match = re.search(r"""(nextval\(')([^']+)('.*$)""", default)
                if match is not None:
                    # the default is related to a Sequence
                    sch = table.schema
                    if '.' not in match.group(2) and sch is not None:
                        # unconditionally quote the schema name.  this could
                        # later be enhanced to obey quoting rules / "quote schema"
                        default = match.group(1) + ('"%s"' % sch) + '.' + match.group(2) + match.group(3)
                colargs.append(schema.DefaultClause(sql.text(default)))
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
            if pk in table.c:
                col = table.c[pk]
                table.primary_key.add(col)
                if col.default is None:
                    col.autoincrement = False

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

            table.append_constraint(schema.ForeignKeyConstraint(constrained_columns, refspec, conname, link_to_name=True))

        # Indexes 
        IDX_SQL = """
          SELECT c.relname, i.indisunique, i.indexprs, i.indpred,
            a.attname
          FROM pg_index i, pg_class c, pg_attribute a
          WHERE i.indrelid = :table AND i.indexrelid = c.oid
            AND a.attrelid = i.indexrelid AND i.indisprimary = 'f'
          ORDER BY c.relname, a.attnum
        """
        t = sql.text(IDX_SQL, typemap={'attname':sqltypes.Unicode})
        c = connection.execute(t, table=table_oid)
        indexes = {}
        sv_idx_name = None
        for row in c.fetchall():
            idx_name, unique, expr, prd, col = row

            if expr and not idx_name == sv_idx_name:
                util.warn(
                  "Skipped unsupported reflection of expression-based index %s"
                  % idx_name)
                sv_idx_name = idx_name
                continue
            if prd and not idx_name == sv_idx_name:
                util.warn(
                   "Predicate of partial index %s ignored during reflection"
                   % idx_name)
                sv_idx_name = idx_name

            if not indexes.has_key(idx_name):
                indexes[idx_name] = [unique, []]
            indexes[idx_name][1].append(col)

        for name, (unique, columns) in indexes.items():
            schema.Index(name, *[table.columns[c] for c in columns], 
                         **dict(unique=unique))
 


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

