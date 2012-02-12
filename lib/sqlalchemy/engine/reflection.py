# engine/reflection.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

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
from sqlalchemy.util import topological
from sqlalchemy.types import TypeEngine
from sqlalchemy import schema as sa_schema


@util.decorator
def cache(fn, self, con, *args, **kw):
    info_cache = kw.get('info_cache', None)
    if info_cache is None:
        return fn(self, con, *args, **kw)
    key = (
            fn.__name__, 
            tuple(a for a in args if isinstance(a, basestring)), 
            tuple((k, v) for k, v in kw.iteritems() if isinstance(v, (basestring, int, float)))
        )
    ret = info_cache.get(key)
    if ret is None:
        ret = fn(self, con, *args, **kw)
        info_cache[key] = ret
    return ret


class Inspector(object):
    """Performs database schema inspection.

    The Inspector acts as a proxy to the reflection methods of the
    :class:`~sqlalchemy.engine.base.Dialect`, providing a
    consistent interface as well as caching support for previously
    fetched metadata.

    The preferred method to construct an :class:`.Inspector` is via the
    :meth:`Inspector.from_engine` method.   I.e.::

        engine = create_engine('...')
        insp = Inspector.from_engine(engine)

    Where above, the :class:`~sqlalchemy.engine.base.Dialect` may opt
    to return an :class:`.Inspector` subclass that provides additional
    methods specific to the dialect's target database.

    """

    def __init__(self, bind):
        """Initialize a new :class:`.Inspector`.

        :param bind: a :class:`~sqlalchemy.engine.base.Connectable`, 
          which is typically an instance of 
          :class:`~sqlalchemy.engine.base.Engine` or 
          :class:`~sqlalchemy.engine.base.Connection`.

        For a dialect-specific instance of :class:`.Inspector`, see
        :meth:`Inspector.from_engine`

        """
        # this might not be a connection, it could be an engine.
        self.bind = bind

        # set the engine
        if hasattr(bind, 'engine'):
            self.engine = bind.engine
        else:
            self.engine = bind

        if self.engine is bind:
            # if engine, ensure initialized
            bind.connect().close()

        self.dialect = self.engine.dialect
        self.info_cache = {}

    @classmethod
    def from_engine(cls, bind):
        """Construct a new dialect-specific Inspector object from the given engine or connection.

        :param bind: a :class:`~sqlalchemy.engine.base.Connectable`, 
          which is typically an instance of 
          :class:`~sqlalchemy.engine.base.Engine` or 
          :class:`~sqlalchemy.engine.base.Connection`.

        This method differs from direct a direct constructor call of :class:`.Inspector`
        in that the :class:`~sqlalchemy.engine.base.Dialect` is given a chance to provide
        a dialect-specific :class:`.Inspector` instance, which may provide additional
        methods.

        See the example at :class:`.Inspector`.

        """
        if hasattr(bind.dialect, 'inspector'):
            return bind.dialect.inspector(bind)
        return Inspector(bind)

    @property
    def default_schema_name(self):
        """Return the default schema name presented by the dialect
        for the current engine's database user.

        E.g. this is typically ``public`` for Postgresql and ``dbo``
        for SQL Server.

        """
        return self.dialect.default_schema_name

    def get_schema_names(self):
        """Return all schema names.
        """

        if hasattr(self.dialect, 'get_schema_names'):
            return self.dialect.get_schema_names(self.bind,
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
            tnames = self.dialect.get_table_names(self.bind,
            schema, info_cache=self.info_cache)
        else:
            tnames = self.engine.table_names(schema)
        if order_by == 'foreign_key':
            import random
            random.shuffle(tnames)

            tuples = []
            for tname in tnames:
                for fkey in self.get_foreign_keys(tname, schema):
                    if tname != fkey['referred_table']:
                        tuples.append((tname, fkey['referred_table']))
            tnames = list(topological.sort(tuples, tnames))
        return tnames

    def get_table_options(self, table_name, schema=None, **kw):
        """Return a dictionary of options specified when the table of the given name was created.

        This currently includes some options that apply to MySQL tables.

        """
        if hasattr(self.dialect, 'get_table_options'):
            return self.dialect.get_table_options(self.bind, table_name, schema,
                                                  info_cache=self.info_cache,
                                                  **kw)
        return {}

    def get_view_names(self, schema=None):
        """Return all view names in `schema`.

        :param schema: Optional, retrieve names from a non-default schema.
        """

        return self.dialect.get_view_names(self.bind, schema,
                                                  info_cache=self.info_cache)

    def get_view_definition(self, view_name, schema=None):
        """Return definition for `view_name`.

        :param schema: Optional, retrieve names from a non-default schema.
        """

        return self.dialect.get_view_definition(
            self.bind, view_name, schema, info_cache=self.info_cache)

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

        col_defs = self.dialect.get_columns(self.bind, table_name, schema,
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

        pkeys = self.dialect.get_primary_keys(self.bind, table_name, schema,
                                              info_cache=self.info_cache,
                                              **kw)

        return pkeys

    def get_pk_constraint(self, table_name, schema=None, **kw):
        """Return information about primary key constraint on `table_name`.

        Given a string `table_name`, and an optional string `schema`, return
        primary key information as a dictionary with these keys:

        constrained_columns
          a list of column names that make up the primary key

        name
          optional name of the primary key constraint.

        """
        pkeys = self.dialect.get_pk_constraint(self.bind, table_name, schema,
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

        name
          optional name of the foreign key constraint.

        \**kw
          other options passed to the dialect's get_foreign_keys() method.

        """

        fk_defs = self.dialect.get_foreign_keys(self.bind, table_name, schema,
                                                info_cache=self.info_cache,
                                                **kw)
        return fk_defs

    def get_indexes(self, table_name, schema=None, **kw):
        """Return information about indexes in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        index information as a list of dicts with these keys:

        name
          the index's name

        column_names
          list of column names in order

        unique
          boolean

        \**kw
          other options passed to the dialect's get_indexes() method.
        """

        indexes = self.dialect.get_indexes(self.bind, table_name,
                                                  schema,
                                            info_cache=self.info_cache, **kw)
        return indexes

    def reflecttable(self, table, include_columns, exclude_columns=()):
        """Given a Table object, load its internal constructs based on introspection.

        This is the underlying method used by most dialects to produce 
        table reflection.  Direct usage is like::

            from sqlalchemy import create_engine, MetaData, Table
            from sqlalchemy.engine import reflection

            engine = create_engine('...')
            meta = MetaData()
            user_table = Table('user', meta)
            insp = Inspector.from_engine(engine)
            insp.reflecttable(user_table, None)

        :param table: a :class:`~sqlalchemy.schema.Table` instance.
        :param include_columns: a list of string column names to include
          in the reflection process.  If ``None``, all columns are reflected.

        """
        dialect = self.bind.dialect

        # table attributes we might need.
        reflection_options = dict(
            (k, table.kwargs.get(k)) for k in dialect.reflection_options if k in table.kwargs)

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

        # Py2K
        if isinstance(schema, str):
            schema = schema.decode(dialect.encoding)
        if isinstance(table_name, str):
            table_name = table_name.decode(dialect.encoding)
        # end Py2K

        # columns
        found_table = False
        for col_d in self.get_columns(table_name, schema, **tblkw):
            found_table = True
            table.dispatch.column_reflect(table, col_d)

            name = col_d['name']
            if include_columns and name not in include_columns:
                continue
            if exclude_columns and name in exclude_columns:
                continue

            coltype = col_d['type']
            col_kw = {
                'nullable':col_d['nullable'],
            }
            for k in ('autoincrement', 'quote', 'info', 'key'):
                if k in col_d:
                    col_kw[k] = col_d[k]

            colargs = []
            if col_d.get('default') is not None:
                # the "default" value is assumed to be a literal SQL expression,
                # so is wrapped in text() so that no quoting occurs on re-issuance.
                colargs.append(
                    sa_schema.DefaultClause(
                        sql.text(col_d['default']), _reflected=True
                    )
                )

            if 'sequence' in col_d:
                # TODO: mssql, maxdb and sybase are using this.
                seq = col_d['sequence']
                sequence = sa_schema.Sequence(seq['name'], 1, 1)
                if 'start' in seq:
                    sequence.start = seq['start']
                if 'increment' in seq:
                    sequence.increment = seq['increment']
                colargs.append(sequence)

            col = sa_schema.Column(name, coltype, *colargs, **col_kw)
            table.append_column(col)

        if not found_table:
            raise exc.NoSuchTableError(table.name)

        # Primary keys
        pk_cons = self.get_pk_constraint(table_name, schema, **tblkw)
        if pk_cons:
            pk_cols = [table.c[pk] 
                        for pk in pk_cons['constrained_columns'] 
                        if pk in table.c and pk not in exclude_columns
                    ] + [pk for pk in table.primary_key if pk.key in exclude_columns]
            primary_key_constraint = sa_schema.PrimaryKeyConstraint(name=pk_cons.get('name'), 
                *pk_cols
            )

            table.append_constraint(primary_key_constraint)

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
                                autoload_with=self.bind,
                                **reflection_options
                                )
                for column in referred_columns:
                    refspec.append(".".join(
                        [referred_schema, referred_table, column]))
            else:
                sa_schema.Table(referred_table, table.metadata, autoload=True,
                                autoload_with=self.bind,
                                **reflection_options
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
                util.warn(
                    "Omitting %s KEY for (%s), key covers omitted columns." %
                    (flavor, ', '.join(columns)))
                continue
            sa_schema.Index(name, *[table.columns[c] for c in columns], 
                         **dict(unique=unique))
