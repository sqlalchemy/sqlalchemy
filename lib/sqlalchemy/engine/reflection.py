"""Provides an abstraction for obtaining database schema information.

Usage Notes:

Here are some general conventions when accessing the low level inspector
methods such as get_table_names, get_columns, etc.

1. Inspector methods return lists of dicts in most cases for the following
   reasons:

   * They're both standard types that can be serialized.
   * Using a dict instead of a tuple allows easy expansion of attributes.
   * Using a list for the outer structure maintains order and is easy to work
     with (e.g. list comprehension [d['name'] for d in cols]).

2. Records that contain a name, such as the column name in a column record
   use the key 'name'. So for most return values, each record will have a
   'name' attribute..
"""

import sqlalchemy
from sqlalchemy import exc, sql
from sqlalchemy import util
from sqlalchemy.types import TypeEngine
from sqlalchemy import schema as sa_schema


@util.decorator
def cache(fn, self, con, *args, **kw):
    info_cache = kw.pop('info_cache', None)
    if info_cache is None:
        return fn(self, con, *args, **kw)
    key = (fn.__name__, args, str(kw))
    ret = info_cache.get(key)
    if ret is None:
        ret = fn(self, con, *args, **kw)
        info_cache[key] = ret
    return ret


class Inspector(object):
    """Performs database schema inspection.

    The Inspector acts as a proxy to the dialects' reflection methods and
    provides higher level functions for accessing database schema information.
    """

    def __init__(self, conn):
        """Initialize the instance.

        :param conn: a :class:`~sqlalchemy.engine.base.Connectable`
        """

        self.conn = conn
        # set the engine
        if hasattr(conn, 'engine'):
            self.engine = conn.engine
        else:
            self.engine = conn
        self.dialect = self.engine.dialect
        self.info_cache = {}

    @classmethod
    def from_engine(cls, engine):
        if hasattr(engine.dialect, 'inspector'):
            return engine.dialect.inspector(engine)
        return Inspector(engine)

    @property
    def default_schema_name(self):
        return self.dialect.get_default_schema_name(self.conn)

    def get_schema_names(self):
        """Return all schema names.
        """

        if hasattr(self.dialect, 'get_schema_names'):
            return self.dialect.get_schema_names(self.conn,
                                                    info_cache=self.info_cache)
        return []

    def get_table_names(self, schema=None, order_by=None):
        """Return all table names in `schema`.

        :param schema: Optional, retrieve names from a non-default schema.
        :param order_by: Optional, may be the string "foreign_key" to sort
                         the result on foreign key dependencies.

        This should probably not return view names or maybe it should return
        them with an indicator t or v.
        """

        if hasattr(self.dialect, 'get_table_names'):
            tnames = self.dialect.get_table_names(self.conn,
            schema,
                                                    info_cache=self.info_cache)
        else:
            tnames = self.engine.table_names(schema)
        if order_by == 'foreign_key':
            ordered_tnames = tnames[:]
            # Order based on foreign key dependencies.
            for tname in tnames:
                table_pos = tnames.index(tname)
                fkeys = self.get_foreign_keys(tname, schema)
                for fkey in fkeys:
                    rtable = fkey['referred_table']
                    if rtable in ordered_tnames:
                        ref_pos = ordered_tnames.index(rtable)
                        # Make sure it's lower in the list than anything it
                        # references.
                        if table_pos > ref_pos:
                            ordered_tnames.pop(table_pos) # rtable moves up 1
                            # insert just below rtable
                            ordered_tnames.index(ref_pos, tname)
            tnames = ordered_tnames
        return tnames

    def get_table_options(self, table_name, schema=None, **kw):
        if hasattr(self.dialect, 'get_table_options'):
            return self.dialect.get_table_options(self.conn, table_name, schema,
                                                  info_cache=self.info_cache,
                                                  **kw)
        return {}

    def get_view_names(self, schema=None):
        """Return all view names in `schema`.

        :param schema: Optional, retrieve names from a non-default schema.
        """

        return self.dialect.get_view_names(self.conn, schema,
                                                  info_cache=self.info_cache)

    def get_view_definition(self, view_name, schema=None):
        """Return definition for `view_name`.

        :param schema: Optional, retrieve names from a non-default schema.
        """

        return self.dialect.get_view_definition(
            self.conn, view_name, schema, info_cache=self.info_cache)

    def get_columns(self, table_name, schema=None, **kw):
        """Return information about columns in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        column information as a list of dicts with these keys:

        name
          the column's name

        type
          :class:`~sqlalchemy.types.TypeEngine`

        nullable
          boolean

        default
          the column's default value

        attrs
          dict containing optional column attributes
        """

        col_defs = self.dialect.get_columns(self.conn, table_name, schema,
                                            info_cache=self.info_cache,
                                            **kw)
        for col_def in col_defs:
            # make this easy and only return instances for coltype
            coltype = col_def['type']
            if not isinstance(coltype, TypeEngine):
                col_def['type'] = coltype()
        return col_defs

    def get_primary_keys(self, table_name, schema=None, **kw):
        """Return information about primary keys in `table_name`.

        Given a string `table_name`, and an optional string `schema`, return
        primary key information as a list of column names.
        """

        pkeys = self.dialect.get_primary_keys(self.conn, table_name, schema,
                                              info_cache=self.info_cache,
                                              **kw)

        return pkeys

    def get_foreign_keys(self, table_name, schema=None, **kw):
        """Return information about foreign_keys in `table_name`.

        Given a string `table_name`, and an optional string `schema`, return
        foreign key information as a list of dicts with these keys:

        constrained_columns
          a list of column names that make up the foreign key

        referred_schema
          the name of the referred schema

        referred_table
          the name of the referred table

        referred_columns
          a list of column names in the referred table that correspond to
          constrained_columns
        """

        fk_defs = self.dialect.get_foreign_keys(self.conn, table_name, schema,
                                                info_cache=self.info_cache,
                                                **kw)
        return fk_defs

    def get_indexes(self, table_name, schema=None):
        """Return information about indexes in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        index information as a list of dicts with these keys:

        name
          the index's name

        column_names
          list of column names in order

        unique
          boolean
        """

        indexes = self.dialect.get_indexes(self.conn, table_name,
                                                  schema,
                                            info_cache=self.info_cache)
        return indexes

    def reflecttable(self, table, include_columns):

        dialect = self.conn.dialect

        # for some work arounds
        from sqlalchemy.dialects.mysql.mysqldb import MySQLDialect

        # MySQL dialect does this.  Applicable with other dialects?
        if hasattr(dialect, '_connection_charset') \
                                        and hasattr(dialect, '_adjust_casing'):
            charset = dialect._connection_charset
            dialect._adjust_casing(table)

        # table attributes we might need.
        oracle_resolve_synonyms = table.kwargs.get('oracle_resolve_synonyms',
                                                   False)

        # some properties that need to be figured out
        fk_use_existing = True

        schema = table.schema
        table_name = table.name

        # apply table options
        tbl_opts = self.get_table_options(table_name, schema, **table.kwargs)
        if tbl_opts:
            table.kwargs.update(tbl_opts)

        # table.kwargs will need to be passed to each reflection method.  Make
        # sure keywords are strings.
        tblkw = table.kwargs.copy()
        for (k, v) in tblkw.items():
            del tblkw[k]
            tblkw[str(k)] = v

        ### Py2K
        # fixme
        # This is breaking mssql, which can't bind unicode.
        ##if isinstance(schema, str):
        ##    schema = schema.decode(dialect.encoding)
        ##if isinstance(table_name, str):
        ##    table_name = table_name.decode(dialect.encoding)
        # end Py2K

        # columns
        found_table = False
        for col_d in self.get_columns(table_name, schema, **tblkw):
            found_table = True
            name = col_d['name']
            coltype = col_d['type']
            nullable = col_d['nullable']
            default = col_d['default']
            attrs = col_d['attrs']
            # construct additional colargs with attrs
            # currently, it's not used here.
            colargs = []
            col_kw = {}
            if 'autoincrement' in col_d:
                col_kw['autoincrement'] = col_d['autoincrement']
            if include_columns and name not in include_columns:
                continue
            if default is not None:
                # fixme
                # mysql does not use sql.text
                if isinstance(dialect, MySQLDialect):
                    colargs.append(sa_schema.DefaultClause(default))
                else:
                    colargs.append(sa_schema.DefaultClause(sql.text(default)))
            col = sa_schema.Column(name, coltype,nullable=nullable, *colargs, **col_kw)
            if 'sequence' in col_d:
                seq = col_d['sequence']
                col.sequence = sa_schema.Sequence(seq['name'], 1, 1)
                if 'start' in seq:
                    col.sequence.start = seq['start']
                if 'increment' in seq:
                    col.sequence.increment = seq['increment']
            table.append_column(col)

        if not found_table:
            raise exc.NoSuchTableError(table.name)

        # Primary keys
        for pk in self.get_primary_keys(table_name, schema, **tblkw):
            if pk in table.c:
                col = table.c[pk]
                table.primary_key.add(col)
                # fixme
                if not isinstance(dialect, MySQLDialect):
                    if col.default is None:
                        col.autoincrement = False
        # Foreign keys
        fkeys = self.get_foreign_keys(table_name, schema, **tblkw)
        for fkey_d in fkeys:
            conname = fkey_d['name']
            constrained_columns = fkey_d['constrained_columns']
            referred_schema = fkey_d['referred_schema']
            referred_table = fkey_d['referred_table']
            referred_columns = fkey_d['referred_columns']
            refspec = []
            if referred_schema is not None:
                sa_schema.Table(referred_table, table.metadata,
                                autoload=True, schema=referred_schema,
                                autoload_with=self.conn,
                                oracle_resolve_synonyms=oracle_resolve_synonyms,
                                useexisting=fk_use_existing
                                )
                for column in referred_columns:
                    refspec.append(".".join(
                        [referred_schema, referred_table, column]))
            else:
                sa_schema.Table(referred_table, table.metadata, autoload=True,
                                autoload_with=self.conn,
                                oracle_resolve_synonyms=oracle_resolve_synonyms,
                                useexisting=fk_use_existing
                                )
                for column in referred_columns:
                    refspec.append(".".join([referred_table, column]))
            table.append_constraint(
                sa_schema.ForeignKeyConstraint(constrained_columns, refspec,
                                               conname, link_to_name=True))
        # Indexes 
        indexes = self.get_indexes(table_name, schema)
        for index_d in indexes:
            name = index_d['name']
            columns = index_d['column_names']
            unique = index_d['unique']
            flavor = index_d.get('type', 'unknown type')
            if include_columns and \
                            not set(columns).issubset(include_columns):
                self.logger.info(
                    "Omitting %s KEY for (%s), key covers ommitted columns." %
                    (flavor, ', '.join(columns)))
                continue
            sa_schema.Index(name, *[table.columns[c] for c in columns], 
                         **dict(unique=unique))
