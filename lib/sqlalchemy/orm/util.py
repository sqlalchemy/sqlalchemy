# mapper/util.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import sql, util, exceptions
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import visitors
from sqlalchemy.orm.interfaces import MapperExtension, EXT_CONTINUE

all_cascades = util.Set(["delete", "delete-orphan", "all", "merge",
                         "expunge", "save-update", "refresh-expire", "none"])

class CascadeOptions(object):
    """Keeps track of the options sent to relation().cascade"""

    def __init__(self, arg=""):
        values = util.Set([c.strip() for c in arg.split(',')])
        self.delete_orphan = "delete-orphan" in values
        self.delete = "delete" in values or "all" in values
        self.save_update = "save-update" in values or "all" in values
        self.merge = "merge" in values or "all" in values
        self.expunge = "expunge" in values or "all" in values
        self.refresh_expire = "refresh-expire" in values or "all" in values
        for x in values:
            if x not in all_cascades:
                raise exceptions.ArgumentError("Invalid cascade option '%s'" % x)

    def __contains__(self, item):
        return getattr(self, item.replace("-", "_"), False)

    def __repr__(self):
        return "CascadeOptions(arg=%s)" % repr(",".join(
            [x for x in ['delete', 'save_update', 'merge', 'expunge',
                         'delete_orphan', 'refresh-expire']
             if getattr(self, x, False) is True]))

def polymorphic_union(table_map, typecolname, aliasname='p_union'):
    """Create a ``UNION`` statement used by a polymorphic mapper.

    See the `SQLAlchemy` advanced mapping docs for an example of how
    this is used.
    """

    colnames = util.Set()
    colnamemaps = {}
    types = {}
    for key in table_map.keys():
        table = table_map[key]

        # mysql doesnt like selecting from a select; make it an alias of the select
        if isinstance(table, sql.Select):
            table = table.alias()
            table_map[key] = table

        m = {}
        for c in table.c:
            colnames.add(c.name)
            m[c.name] = c
            types[c.name] = c.type
        colnamemaps[table] = m

    def col(name, table):
        try:
            return colnamemaps[table][name]
        except KeyError:
            return sql.cast(sql.null(), types[name]).label(name)

    result = []
    for type, table in table_map.iteritems():
        if typecolname is not None:
            result.append(sql.select([col(name, table) for name in colnames] +
                                     [sql.literal_column("'%s'" % type).label(typecolname)],
                                     from_obj=[table]))
        else:
            result.append(sql.select([col(name, table) for name in colnames], from_obj=[table]))
    return sql.union_all(*result).alias(aliasname)

class TranslatingDict(dict):
    """A dictionary that stores ``ColumnElement`` objects as keys.

    Incoming ``ColumnElement`` keys are translated against those of an
    underling ``FromClause`` for all operations.  This way the columns
    from any ``Selectable`` that is derived from or underlying this
    ``TranslatingDict`` 's selectable can be used as keys.
    """

    def __init__(self, selectable):
        super(TranslatingDict, self).__init__()
        self.selectable = selectable

    def __translate_col(self, col):
        ourcol = self.selectable.corresponding_column(col, keys_ok=False, raiseerr=False)
        if ourcol is None:
            return col
        else:
            return ourcol

    def __getitem__(self, col):
        return super(TranslatingDict, self).__getitem__(self.__translate_col(col))

    def has_key(self, col):
        return col in self

    def __setitem__(self, col, value):
        return super(TranslatingDict, self).__setitem__(self.__translate_col(col), value)

    def __contains__(self, col):
        return super(TranslatingDict, self).__contains__(self.__translate_col(col))

    def setdefault(self, col, value):
        return super(TranslatingDict, self).setdefault(self.__translate_col(col), value)

class ExtensionCarrier(MapperExtension):
    def __init__(self, _elements=None):
        self.__elements = _elements or []

    def copy(self):
        return ExtensionCarrier(list(self.__elements))
        
    def __iter__(self):
        return iter(self.__elements)

    def insert(self, extension):
        """Insert a MapperExtension at the beginning of this ExtensionCarrier's list."""

        self.__elements.insert(0, extension)

    def append(self, extension):
        """Append a MapperExtension at the end of this ExtensionCarrier's list."""

        self.__elements.append(extension)

    def _create_do(funcname):
        def _do(self, *args, **kwargs):
            for elem in self.__elements:
                ret = getattr(elem, funcname)(*args, **kwargs)
                if ret is not EXT_CONTINUE:
                    return ret
            else:
                return EXT_CONTINUE
        return _do

    instrument_class = _create_do('instrument_class')
    init_instance = _create_do('init_instance')
    init_failed = _create_do('init_failed')
    dispose_class = _create_do('dispose_class')
    get_session = _create_do('get_session')
    load = _create_do('load')
    get = _create_do('get')
    get_by = _create_do('get_by')
    select_by = _create_do('select_by')
    select = _create_do('select')
    translate_row = _create_do('translate_row')
    create_instance = _create_do('create_instance')
    append_result = _create_do('append_result')
    populate_instance = _create_do('populate_instance')
    before_insert = _create_do('before_insert')
    before_update = _create_do('before_update')
    after_update = _create_do('after_update')
    after_insert = _create_do('after_insert')
    before_delete = _create_do('before_delete')
    after_delete = _create_do('after_delete')

class BinaryVisitor(visitors.ClauseVisitor):
    def __init__(self, func):
        self.func = func

    def visit_binary(self, binary):
        self.func(binary)

class AliasedClauses(object):
    """Creates aliases of a mapped tables for usage in ORM queries.
    """

    def __init__(self, mapped_table, alias=None):
        if alias:
            self.alias = alias
        else:
            self.alias = mapped_table.alias()
        self.mapped_table = mapped_table
        self.extra_cols = {}
        self.row_decorator = self._create_row_adapter()
        
    def aliased_column(self, column):
        """return the aliased version of the given column, creating a new label for it if not already
        present in this AliasedClauses."""

        conv = self.alias.corresponding_column(column, raiseerr=False)
        if conv:
            return conv

        if column in self.extra_cols:
            return self.extra_cols[column]

        aliased_column = column
        # for column-level subqueries, swap out its selectable with our
        # eager version as appropriate, and manually build the 
        # "correlation" list of the subquery.  
        class ModifySubquery(visitors.ClauseVisitor):
            def visit_select(s, select):
                select._should_correlate = False
                select.append_correlation(self.alias)
        aliased_column = sql_util.ClauseAdapter(self.alias).chain(ModifySubquery()).traverse(aliased_column, clone=True)
        aliased_column = aliased_column.label(None)
        self.row_decorator.map[column] = aliased_column
        # TODO: this is a little hacky
        for attr in ('name', '_label'):
            if hasattr(column, attr):
                self.row_decorator.map[getattr(column, attr)] = aliased_column
        self.extra_cols[column] = aliased_column
        return aliased_column

    def adapt_clause(self, clause):
        return sql_util.ClauseAdapter(self.alias).traverse(clause, clone=True)
    
    def _create_row_adapter(self):
        """Return a callable which, 
        when passed a RowProxy, will return a new dict-like object
        that translates Column objects to that of this object's Alias before calling upon the row.

        This allows a regular Table to be used to target columns in a row that was in reality generated from an alias
        of that table, in such a way that the row can be passed to logic which knows nothing about the aliased form
        of the table.
        """
        class AliasedRowAdapter(object):
            def __init__(self, row):
                self.row = row
            def __contains__(self, key):
                return key in map or key in self.row
            def has_key(self, key):
                return key in self
            def __getitem__(self, key):
                if key in map:
                    key = map[key]
                return self.row[key]
            def keys(self):
                return map.keys()
        map = {}        
        for c in self.alias.c:
            parent = self.mapped_table.corresponding_column(c)
            map[parent] = c
            map[parent._label] = c
            map[parent.name] = c
        for c in self.extra_cols:
            map[c] = self.extra_cols[c]
            # TODO: this is a little hacky
            for attr in ('name', '_label'):
                if hasattr(c, attr):
                    map[getattr(c, attr)] = self.extra_cols[c]
                
        AliasedRowAdapter.map = map
        return AliasedRowAdapter

    
class PropertyAliasedClauses(AliasedClauses):
    """extends AliasedClauses to add support for primary/secondary joins on a relation()."""
    
    def __init__(self, prop, primaryjoin, secondaryjoin, parentclauses=None):
        super(PropertyAliasedClauses, self).__init__(prop.select_table)
            
        self.parentclauses = parentclauses
        if parentclauses is not None:
            self.path = parentclauses.path + (prop.parent, prop.key)
        else:
            self.path = (prop.parent, prop.key)

        self.prop = prop
        
        if prop.secondary:
            self.secondary = prop.secondary.alias()
            if parentclauses is not None:
                aliasizer = sql_util.ClauseAdapter(self.alias).\
                        chain(sql_util.ClauseAdapter(self.secondary)).\
                        chain(sql_util.ClauseAdapter(parentclauses.alias))
            else:
                aliasizer = sql_util.ClauseAdapter(self.alias).\
                    chain(sql_util.ClauseAdapter(self.secondary))
            self.secondaryjoin = aliasizer.traverse(secondaryjoin, clone=True)
            self.primaryjoin = aliasizer.traverse(primaryjoin, clone=True)
        else:
            if parentclauses is not None: 
                aliasizer = sql_util.ClauseAdapter(self.alias, exclude=prop.local_side)
                aliasizer.chain(sql_util.ClauseAdapter(parentclauses.alias, exclude=prop.remote_side))
            else:
                aliasizer = sql_util.ClauseAdapter(self.alias, exclude=prop.local_side)
            self.primaryjoin = aliasizer.traverse(primaryjoin, clone=True)
            self.secondary = None
            self.secondaryjoin = None
        
        if prop.order_by:
            self.order_by = sql_util.ClauseAdapter(self.alias).copy_and_process(util.to_list(prop.order_by))
        else:
            self.order_by = None
    
    mapper = property(lambda self:self.prop.mapper)
    table = property(lambda self:self.prop.select_table)
    
    def __str__(self):
        return "->".join([str(s) for s in self.path])


def instance_str(instance):
    """Return a string describing an instance."""

    return instance.__class__.__name__ + "@" + hex(id(instance))

def attribute_str(instance, attribute):
    return instance_str(instance) + "." + attribute
