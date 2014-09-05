# engine/reflection.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors
# <see AUTHORS file>
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

from .. import exc, sql
from ..sql import schema as sa_schema
from .. import util
from ..sql.type_api import TypeEngine
from ..util import deprecated
from ..util import topological
from .. import inspection
from .base import Connectable


@util.decorator
def cache(fn, self, con, *args, **kw):
    info_cache = kw.get('info_cache', None)
    if info_cache is None:
        return fn(self, con, *args, **kw)
    key = (
        fn.__name__,
        tuple(a for a in args if isinstance(a, util.string_types)),
        tuple((k, v) for k, v in kw.items() if
              isinstance(v,
                         util.string_types + util.int_types + (float, )
                         )
              )
    )
    ret = info_cache.get(key)
    if ret is None:
        ret = fn(self, con, *args, **kw)
        info_cache[key] = ret
    return ret


class Inspector(object):
    """Performs database schema inspection.

    The Inspector acts as a proxy to the reflection methods of the
    :class:`~sqlalchemy.engine.interfaces.Dialect`, providing a
    consistent interface as well as caching support for previously
    fetched metadata.

    A :class:`.Inspector` object is usually created via the
    :func:`.inspect` function::

        from sqlalchemy import inspect, create_engine
        engine = create_engine('...')
        insp = inspect(engine)

    The inspection method above is equivalent to using the
    :meth:`.Inspector.from_engine` method, i.e.::

        engine = create_engine('...')
        insp = Inspector.from_engine(engine)

    Where above, the :class:`~sqlalchemy.engine.interfaces.Dialect` may opt
    to return an :class:`.Inspector` subclass that provides additional
    methods specific to the dialect's target database.

    """

    def __init__(self, bind):
        """Initialize a new :class:`.Inspector`.

        :param bind: a :class:`~sqlalchemy.engine.Connectable`,
          which is typically an instance of
          :class:`~sqlalchemy.engine.Engine` or
          :class:`~sqlalchemy.engine.Connection`.

        For a dialect-specific instance of :class:`.Inspector`, see
        :meth:`.Inspector.from_engine`

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
        """Construct a new dialect-specific Inspector object from the given
        engine or connection.

        :param bind: a :class:`~sqlalchemy.engine.Connectable`,
          which is typically an instance of
          :class:`~sqlalchemy.engine.Engine` or
          :class:`~sqlalchemy.engine.Connection`.

        This method differs from direct a direct constructor call of
        :class:`.Inspector` in that the
        :class:`~sqlalchemy.engine.interfaces.Dialect` is given a chance to
        provide a dialect-specific :class:`.Inspector` instance, which may
        provide additional methods.

        See the example at :class:`.Inspector`.

        """
        if hasattr(bind.dialect, 'inspector'):
            return bind.dialect.inspector(bind)
        return Inspector(bind)

    @inspection._inspects(Connectable)
    def _insp(bind):
        return Inspector.from_engine(bind)

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
        """Return all table names in referred to within a particular schema.

        The names are expected to be real tables only, not views.
        Views are instead returned using the :meth:`.Inspector.get_view_names`
        method.


        :param schema: Schema name. If ``schema`` is left at ``None``, the
         database's default schema is
         used, else the named schema is searched.  If the database does not
         support named schemas, behavior is undefined if ``schema`` is not
         passed as ``None``.  For special quoting, use :class:`.quoted_name`.

        :param order_by: Optional, may be the string "foreign_key" to sort
         the result on foreign key dependencies.

         .. versionchanged:: 0.8 the "foreign_key" sorting sorts tables
            in order of dependee to dependent; that is, in creation
            order, rather than in drop order.  This is to maintain
            consistency with similar features such as
            :attr:`.MetaData.sorted_tables` and :func:`.util.sort_tables`.

        .. seealso::

            :attr:`.MetaData.sorted_tables`

        """

        if hasattr(self.dialect, 'get_table_names'):
            tnames = self.dialect.get_table_names(
                self.bind, schema, info_cache=self.info_cache)
        else:
            tnames = self.engine.table_names(schema)
        if order_by == 'foreign_key':
            tuples = []
            for tname in tnames:
                for fkey in self.get_foreign_keys(tname, schema):
                    if tname != fkey['referred_table']:
                        tuples.append((fkey['referred_table'], tname))
            tnames = list(topological.sort(tuples, tnames))
        return tnames

    def get_table_options(self, table_name, schema=None, **kw):
        """Return a dictionary of options specified when the table of the
        given name was created.

        This currently includes some options that apply to MySQL tables.

        :param table_name: string name of the table.  For special quoting,
         use :class:`.quoted_name`.

        :param schema: string schema name; if omitted, uses the default schema
         of the database connection.  For special quoting,
         use :class:`.quoted_name`.

        """
        if hasattr(self.dialect, 'get_table_options'):
            return self.dialect.get_table_options(
                self.bind, table_name, schema,
                info_cache=self.info_cache, **kw)
        return {}

    def get_view_names(self, schema=None):
        """Return all view names in `schema`.

        :param schema: Optional, retrieve names from a non-default schema.
         For special quoting, use :class:`.quoted_name`.

        """

        return self.dialect.get_view_names(self.bind, schema,
                                           info_cache=self.info_cache)

    def get_view_definition(self, view_name, schema=None):
        """Return definition for `view_name`.

        :param schema: Optional, retrieve names from a non-default schema.
         For special quoting, use :class:`.quoted_name`.

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

        :param table_name: string name of the table.  For special quoting,
         use :class:`.quoted_name`.

        :param schema: string schema name; if omitted, uses the default schema
         of the database connection.  For special quoting,
         use :class:`.quoted_name`.

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

    @deprecated('0.7', 'Call to deprecated method get_primary_keys.'
                '  Use get_pk_constraint instead.')
    def get_primary_keys(self, table_name, schema=None, **kw):
        """Return information about primary keys in `table_name`.

        Given a string `table_name`, and an optional string `schema`, return
        primary key information as a list of column names.
        """

        return self.dialect.get_pk_constraint(self.bind, table_name, schema,
                                              info_cache=self.info_cache,
                                              **kw)['constrained_columns']

    def get_pk_constraint(self, table_name, schema=None, **kw):
        """Return information about primary key constraint on `table_name`.

        Given a string `table_name`, and an optional string `schema`, return
        primary key information as a dictionary with these keys:

        constrained_columns
          a list of column names that make up the primary key

        name
          optional name of the primary key constraint.

        :param table_name: string name of the table.  For special quoting,
         use :class:`.quoted_name`.

        :param schema: string schema name; if omitted, uses the default schema
         of the database connection.  For special quoting,
         use :class:`.quoted_name`.

        """
        return self.dialect.get_pk_constraint(self.bind, table_name, schema,
                                              info_cache=self.info_cache,
                                              **kw)

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

        :param table_name: string name of the table.  For special quoting,
         use :class:`.quoted_name`.

        :param schema: string schema name; if omitted, uses the default schema
         of the database connection.  For special quoting,
         use :class:`.quoted_name`.

        """

        return self.dialect.get_foreign_keys(self.bind, table_name, schema,
                                             info_cache=self.info_cache,
                                             **kw)

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

        :param table_name: string name of the table.  For special quoting,
         use :class:`.quoted_name`.

        :param schema: string schema name; if omitted, uses the default schema
         of the database connection.  For special quoting,
         use :class:`.quoted_name`.

        """

        return self.dialect.get_indexes(self.bind, table_name,
                                        schema,
                                        info_cache=self.info_cache, **kw)

    def get_unique_constraints(self, table_name, schema=None, **kw):
        """Return information about unique constraints in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        unique constraint information as a list of dicts with these keys:

        name
          the unique constraint's name

        column_names
          list of column names in order

        :param table_name: string name of the table.  For special quoting,
         use :class:`.quoted_name`.

        :param schema: string schema name; if omitted, uses the default schema
         of the database connection.  For special quoting,
         use :class:`.quoted_name`.

        .. versionadded:: 0.8.4

        """

        return self.dialect.get_unique_constraints(
            self.bind, table_name, schema, info_cache=self.info_cache, **kw)

    def reflecttable(self, table, include_columns, exclude_columns=()):
        """Given a Table object, load its internal constructs based on
        introspection.

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

        schema = table.schema
        table_name = table.name

        # get table-level arguments that are specifically
        # intended for reflection, e.g. oracle_resolve_synonyms.
        # these are unconditionally passed to related Table
        # objects
        reflection_options = dict(
            (k, table.dialect_kwargs.get(k))
            for k in dialect.reflection_options
            if k in table.dialect_kwargs
        )

        # reflect table options, like mysql_engine
        tbl_opts = self.get_table_options(
            table_name, schema, **table.dialect_kwargs)
        if tbl_opts:
            # add additional kwargs to the Table if the dialect
            # returned them
            table._validate_dialect_kwargs(tbl_opts)

        if util.py2k:
            if isinstance(schema, str):
                schema = schema.decode(dialect.encoding)
            if isinstance(table_name, str):
                table_name = table_name.decode(dialect.encoding)

        found_table = False
        cols_by_orig_name = {}

        for col_d in self.get_columns(
                table_name, schema, **table.dialect_kwargs):
            found_table = True
            orig_name = col_d['name']

            table.dispatch.column_reflect(self, table, col_d)

            name = col_d['name']
            if include_columns and name not in include_columns:
                continue
            if exclude_columns and name in exclude_columns:
                continue

            coltype = col_d['type']

            col_kw = dict(
                (k, col_d[k])
                for k in ['nullable', 'autoincrement', 'quote', 'info', 'key']
                if k in col_d
            )

            colargs = []
            if col_d.get('default') is not None:
                # the "default" value is assumed to be a literal SQL
                # expression, so is wrapped in text() so that no quoting
                # occurs on re-issuance.
                colargs.append(
                    sa_schema.DefaultClause(
                        sql.text(col_d['default']), _reflected=True
                    )
                )

            if 'sequence' in col_d:
                # TODO: mssql and sybase are using this.
                seq = col_d['sequence']
                sequence = sa_schema.Sequence(seq['name'], 1, 1)
                if 'start' in seq:
                    sequence.start = seq['start']
                if 'increment' in seq:
                    sequence.increment = seq['increment']
                colargs.append(sequence)

            cols_by_orig_name[orig_name] = col = \
                sa_schema.Column(name, coltype, *colargs, **col_kw)

            if col.key in table.primary_key:
                col.primary_key = True
            table.append_column(col)

        if not found_table:
            raise exc.NoSuchTableError(table.name)

        pk_cons = self.get_pk_constraint(
            table_name, schema, **table.dialect_kwargs)
        if pk_cons:
            pk_cols = [
                cols_by_orig_name[pk]
                for pk in pk_cons['constrained_columns']
                if pk in cols_by_orig_name and pk not in exclude_columns
            ]

            # update pk constraint name
            table.primary_key.name = pk_cons.get('name')

            # tell the PKConstraint to re-initialize
            # its column collection
            table.primary_key._reload(pk_cols)

        fkeys = self.get_foreign_keys(
            table_name, schema, **table.dialect_kwargs)
        for fkey_d in fkeys:
            conname = fkey_d['name']
            # look for columns by orig name in cols_by_orig_name,
            # but support columns that are in-Python only as fallback
            constrained_columns = [
                cols_by_orig_name[c].key
                if c in cols_by_orig_name else c
                for c in fkey_d['constrained_columns']
            ]
            if exclude_columns and set(constrained_columns).intersection(
                    exclude_columns):
                continue
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
            if 'options' in fkey_d:
                options = fkey_d['options']
            else:
                options = {}
            table.append_constraint(
                sa_schema.ForeignKeyConstraint(constrained_columns, refspec,
                                               conname, link_to_name=True,
                                               **options))
        # Indexes
        indexes = self.get_indexes(table_name, schema)
        for index_d in indexes:
            name = index_d['name']
            columns = index_d['column_names']
            unique = index_d['unique']
            flavor = index_d.get('type', 'index')
            if include_columns and \
                    not set(columns).issubset(include_columns):
                util.warn(
                    "Omitting %s key for (%s), key covers omitted columns." %
                    (flavor or "index", ', '.join(columns)))
                continue
            # look for columns by orig name in cols_by_orig_name,
            # but support columns that are in-Python only as fallback
            idx_cols = []
            for c in columns:
                try:
                    idx_col = cols_by_orig_name[c] \
                        if c in cols_by_orig_name else table.c[c]
                except KeyError:
                    util.warn(
                        "%s key '%s' was not located in "
                        "columns for table '%s'" % (
                            flavor or "index", c, table_name
                        ))
                else:
                    idx_cols.append(idx_col)

            sa_schema.Index(name, *idx_cols, **dict(unique=unique))
