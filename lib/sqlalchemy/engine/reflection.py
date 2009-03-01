"""Provides an abstraction for obtaining database schema information.

Development Notes:

I'm still trying to decide upon conventions for both the Inspector interface as well as the dialect interface the Inspector is to consume.  Below are some of the current conventions.

  1. Inspector methods should return lists of dicts in most cases for the 
     following reasons:
    * They're both simple standard types.
    * Using a dict instead of a tuple allows easy expansion of attributes.
    * Using a list for the outer structure maintains order and is easy to work 
       with (e.g. list comprehension [d['name'] for d in cols]).
    * Being consistent is just good.
  2. Records that contain a name, such as the column name in a column record
     should use the key 'name' in the dict.  This allows the user to expect a
     'name' key and to know what it will reference.


"""
import sqlalchemy
from sqlalchemy import util
from sqlalchemy.types import TypeEngine


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

# keeping this around until all dialects are fixed
@util.decorator
def caches(fn, self, con, *args, **kw):
    # what are we caching?
    fn_name = fn.__name__
    if not fn_name.startswith('get_'):
        # don't recognize this.
        return fn(self, con, *args, **kw)
    else:
        attr_to_cache = fn_name[4:]
    # The first arguments will always be self and con.
    # Assuming *args and *kw will be acceptable to info_cache method.
    if 'info_cache' in kw:
        kw_cp = kw.copy()
        info_cache = kw_cp.pop('info_cache')
        methodname = "%s_%s" % ('get', attr_to_cache)
        # fixme.
        for bad_kw in ('dblink', 'resolve_synonyms'):
            if bad_kw in kw_cp:
                del kw_cp[bad_kw]
        information = getattr(info_cache, methodname)(*args, **kw_cp)
        if information:
            return information
    information = fn(self, con, *args, **kw)
    if 'info_cache' in locals():
        methodname = "%s_%s" % ('set', attr_to_cache)
        getattr(info_cache, methodname)(information, *args, **kw_cp)
    return information 

class DefaultInfoCache(object):
    """Default implementation of InfoCache

    InfoCache provides a means for dialects to cache information obtained for
    reflection and a convenient interface for setting and retrieving cached
    data.

    """
    
    def __init__(self):
        self._cache = dict(schemas={})
        self.tables_are_complete = False
        self.schemas_are_complete = False
        self.views_are_complete = False

    def clear(self):
        """Clear the cache."""
        self._cache = dict(schemas={})

    # schemas

    def get_schemas(self):
        """Return the schemas dict."""
        return self._cache.get('schemas')


    def get_schema(self, schemaname, create=False):
        """Return cached schema and optionally create it if it does not exist.

        """
        schema = self._cache['schemas'].get(schemaname)
        if schema is not None:
            return schema
        elif create:
            return self.add_schema(schemaname)
        return None

    def add_schema(self, schemaname):
        self._cache['schemas'][schemaname] = dict(tables={}, views={})
        return self.get_schema(schemaname)

    def get_schema_names(self, check_complete=True):
        """Return cached schema names.

        By default, only return them if they're complete.

        """
        if check_complete and self.schemas_are_complete:
            return self.get_schemas().keys()
        elif not check_complete:
            return self.get_schemas().keys()
        else:
            return None

    def set_schema_names(self, schemanames):
        for schemaname in schemanames:
            self.add_schema(schemaname)
        self.schemas_are_complete = True

    # tables

    def get_table(self, tablename, schemaname=None, create=False,
                                                        table_type='table'):
        """Return cached table and optionally create it if it does not exist.


        """
        cache = self._cache
        schema = self.get_schema(schemaname, create=create)
        if schema is None:
            return None
        if table_type == 'view':
            table = schema['views'].get(tablename)
        else:
            table = schema['tables'].get(tablename)
        if table is not None:
            return table
        elif create:
            return self.add_table(tablename, schemaname, table_type=table_type)
        return None

    def get_table_names(self, schemaname=None, check_complete=True,
                                                        table_type='table'):
        """Return cached table names.

        By default, only return them if they're complete.

        """
        if table_type == 'view':
            complete = self.views_are_complete
        else:
            complete = self.tables_are_complete
        if check_complete and complete:
            return self.get_tables(schemaname, table_type=table_type).keys()
        elif not check_complete:
            return self.get_tables(schemaname, table_type=table_type).keys()
        else:
            return None

    def add_table(self, tablename, schemaname=None, table_type='table'):
        schema = self.get_schema(schemaname, create=True)
        if table_type == 'table':
            schema['tables'][tablename] = dict(columns={})
        else:
            schema['views'][tablename] = dict(columns={})
        return self.get_table(tablename, schemaname, table_type=table_type)

    def set_table_names(self, tablenames, schemaname=None, table_type='table'):
        for tablename in tablenames:
            self.add_table(tablename, schemaname, table_type)
        if table_type == 'view':
            self.views_are_complete = True
        else:
            self.tables_are_complete = True
            
    # views

    def get_view(self, viewname, schemaname=None, create=False):
        return self.get_table(viewname, schemaname, create, 'view')

    def get_view_names(self, schemaname=None, check_complete=True):
        return self.get_table_names(schemaname, check_complete, 'view')

    def add_view(self, viewname, schemaname=None):
        return self.add_table(viewname, schemaname, 'view')

    def set_view_names(self, viewnames, schemaname=None):
        return self.set_table_names(viewnames, schemaname, 'view')

    def get_view_definition(self, viewname, schemaname=None):
        view_cache = self.get_view(viewname, schemaname)
        if view_cache and 'definition' in view_cache:
            return view_cache['definition']

    def set_view_definition(self, definition, viewname, schemaname=None):
        view_cache = self.get_view(viewname, schemaname, create=True)
        view_cache['definition'] = definition

    # table data

    def _get_table_data(self, key, tablename, schemaname=None):
        table_cache = self.get_table(tablename, schemaname)
        if table_cache is not None and key in table_cache.keys():
            return table_cache[key]

    def _set_table_data(self, key, data, tablename, schemaname=None):
        """Cache data for schemaname.tablename using key.

        It will create a schema and table entry in the cache if needed.

        """
        table_cache = self.get_table(tablename, schemaname, create=True)
        table_cache[key] = data

    # columns

    def get_columns(self, tablename, schemaname=None):
        """Return columns list or None."""
        
        return self._get_table_data('columns', tablename, schemaname)

    def set_columns(self, columns, tablename, schemaname=None):
        """Add list of columns to table cache."""

        return self._set_table_data('columns', columns, tablename, schemaname)

    # primary keys

    def get_primary_keys(self, tablename, schemaname=None):
        """Return primary key list or None."""
        
        return self._get_table_data('primary_keys', tablename, schemaname)

    def set_primary_keys(self, pkeys, tablename, schemaname=None):
        """Add list of primary keys to table cache."""

        return self._set_table_data('primary_keys', pkeys, tablename, schemaname)

    # foreign keys

    def get_foreign_keys(self, tablename, schemaname=None):
        """Return foreign key list or None."""
        
        return self._get_table_data('foreign_keys', tablename, schemaname)

    def set_foreign_keys(self, fkeys, tablename, schemaname=None):
        """Add list of foreign keys to table cache."""

        return self._set_table_data('foreign_keys', fkeys, tablename, schemaname)

    # indexes

    def get_indexes(self, tablename, schemaname=None):
        """Return indexes list or None."""
        
        return self._get_table_data('indexes', tablename, schemaname)

    def set_indexes(self, indexes, tablename, schemaname=None):
        """Add list of indexes to table cache."""

        return self._set_table_data('indexes', indexes, tablename, schemaname)

class Inspector(object):
    """performs database introspection

    """
    
    def __init__(self, conn):
        """

        conn
          [sqlalchemy.engine.base.#Connectable]

        """
        self.conn = conn
        # set the engine
        if hasattr(conn, 'engine'):
            self.engine = conn.engine
        else:
            self.engine = conn
        # fixme. This is just until all dialects are converted
        if hasattr(self, 'info_cache'):
            self.info_cache = self.engine.dialect.info_cache()
        else:
            self.info_cache = {}

    def default_schema_name(self):
        return self.engine.dialect.get_default_schema_name(self.conn)
    default_schema_name = property(default_schema_name)

    def get_schema_names(self):
        """Return all schema names.

        """
        if hasattr(self.engine.dialect, 'get_schema_names'):
            return self.engine.dialect.get_schema_names(self.conn,
                                                    info_cache=self.info_cache)
        return []

    def get_table_names(self, schemaname=None, order_by=None):
        """Return all table names in `schemaname`.
        schemaname:
          Optional, retrieve names from a non-default schema.

        This should probably not return view names or maybe it should return
        them with an indicator t or v.

        """
        if hasattr(self.engine.dialect, 'get_table_names'):
            tnames = self.engine.dialect.get_table_names(self.conn, schemaname,
                                                    info_cache=self.info_cache)
        else:
            tnames = self.engine.table_names(schemaname)
        if order_by == 'foreign_key':
            ordered_tnames = tnames[:]
            # Order based on foreign key dependencies.
            for tname in tnames:
                table_pos = tnames.index(tname)
                fkeys = self.get_foreign_keys(tname, schemaname)
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

    def get_view_names(self, schemaname=None):
        """Return all view names in `schemaname`.
        schemaname:
          Optional, retrieve names from a non-default schema.

        """
        return self.engine.dialect.get_view_names(self.conn, schemaname,
                                                  info_cache=self.info_cache)

    def get_view_definition(self, view_name, schemaname=None):
        """Return definition for `view_name`.
        schemaname:
          Optional, retrieve names from a non-default schema.

        """
        return self.engine.dialect.get_view_definition(
            self.conn, view_name, schemaname, info_cache=self.info_cache)

    def get_columns(self, tablename, schemaname=None):
        """Return information about columns in `tablename`.

        Given a string `tablename` and an optional string `schemaname`, return
        column information as a list of dicts with these keys:

        name
          the column's name

        type
          [sqlalchemy.types#TypeEngine]

        nullable
          boolean

        default
          the column's default value

        attrs
          dict containing optional column attributes

        """

        col_defs = self.engine.dialect.get_columns(self.conn, tablename,
                                                   schemaname,
                                                   info_cache=self.info_cache)
        for col_def in col_defs:
            # make this easy and only return instances for coltype
            coltype = col_def['type']
            if not isinstance(coltype, TypeEngine):
                col_def['type'] = coltype()
        return col_defs

    def get_primary_keys(self, tablename, schemaname=None):
        """Return information about primary keys in `tablename`.

        Given a string `tablename`, and an optional string `schemaname`, return 
        primary key information as a list of column names.

        """

        pkeys = self.engine.dialect.get_primary_keys(self.conn, tablename,
                                                     schemaname,
                                            info_cache=self.info_cache)

        return pkeys

    def get_foreign_keys(self, tablename, schemaname=None):
        """Return information about foreign_keys in `tablename`.

        Given a string `tablename`, and an optional string `schemaname`, return 
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

        fk_defs = self.engine.dialect.get_foreign_keys(self.conn, tablename,
                                                       schemaname,
                                                info_cache=self.info_cache)
        for fk_def in fk_defs:
            referred_schema = fk_def['referred_schema']
            # always set the referred_schema.
            if referred_schema is None and schemaname is None:
                referred_schema = self.engine.dialect.get_default_schema_name(
                                                                    self.conn)
                fk_def['referred_schema'] = referred_schema
        return fk_defs

    def get_indexes(self, tablename, schemaname=None):
        """Return information about indexes in `tablename`.

        Given a string `tablename` and an optional string `schemaname`, return
        index information as a list of dicts with these keys:

        name
          the index's name

        column_names
          list of column names in order

        unique
          boolean

        """

        indexes = self.engine.dialect.get_indexes(self.conn, tablename,
                                                  schemaname,
                                            info_cache=self.info_cache)
        return indexes
