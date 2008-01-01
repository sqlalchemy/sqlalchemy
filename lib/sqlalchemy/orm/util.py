# mapper/util.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import sql, util, exceptions
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import visitors
from sqlalchemy.orm.interfaces import MapperExtension, EXT_CONTINUE, build_path

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


class ExtensionCarrier(object):
    """stores a collection of MapperExtension objects.
    
    allows an extension methods to be called on contained MapperExtensions
    in the order they were added to this object.  Also includes a 'methods' dictionary
    accessor which allows for a quick check if a particular method
    is overridden on any contained MapperExtensions.
    """
    
    def __init__(self, _elements=None):
        self.methods = {}
        if _elements is not None:
            self.__elements = [self.__inspect(e) for e in _elements]
        else:
            self.__elements = []
        
    def copy(self):
        return ExtensionCarrier(list(self.__elements))
        
    def __iter__(self):
        return iter(self.__elements)

    def insert(self, extension):
        """Insert a MapperExtension at the beginning of this ExtensionCarrier's list."""

        self.__elements.insert(0, self.__inspect(extension))

    def append(self, extension):
        """Append a MapperExtension at the end of this ExtensionCarrier's list."""

        self.__elements.append(self.__inspect(extension))

    def __inspect(self, extension):
        for meth in MapperExtension.__dict__.keys():
            if meth not in self.methods and hasattr(extension, meth) and getattr(extension, meth) is not getattr(MapperExtension, meth):
                self.methods[meth] = self.__create_do(meth)
        return extension
           
    def __create_do(self, funcname):
        def _do(*args, **kwargs):
            for elem in self.__elements:
                ret = getattr(elem, funcname)(*args, **kwargs)
                if ret is not EXT_CONTINUE:
                    return ret
            else:
                return EXT_CONTINUE

        try:
            _do.__name__ = funcname
        except:
            # cant set __name__ in py 2.3 
            pass
        return _do
    
    def _pass(self, *args, **kwargs):
        return EXT_CONTINUE
        
    def __getattr__(self, key):
        return self.methods.get(key, self._pass)

class AliasedClauses(object):
    """Creates aliases of a mapped tables for usage in ORM queries.
    """

    def __init__(self, mapped_table, alias=None):
        if alias:
            self.alias = alias
        else:
            self.alias = mapped_table.alias()
        self.mapped_table = mapped_table
        self.row_decorator = self._create_row_adapter()
        
    def aliased_column(self, column):
        """return the aliased version of the given column, creating a new label for it if not already
        present in this AliasedClauses."""

        conv = self.alias.corresponding_column(column)
        if conv:
            return conv

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
        return aliased_column

    def adapt_clause(self, clause):
        return sql_util.ClauseAdapter(self.alias).traverse(clause, clone=True)
    
    def adapt_list(self, clauses):
        return sql_util.ClauseAdapter(self.alias).copy_and_process(clauses)
        
    def _create_row_adapter(self):
        """Return a callable which, 
        when passed a RowProxy, will return a new dict-like object
        that translates Column objects to that of this object's Alias before calling upon the row.

        This allows a regular Table to be used to target columns in a row that was in reality generated from an alias
        of that table, in such a way that the row can be passed to logic which knows nothing about the aliased form
        of the table.
        """
        return create_row_adapter(self.alias, self.mapped_table)

def create_row_adapter(from_, to, equivalent_columns=None):
    """create a row adapter between two selectables.
    
    The returned adapter is a class that can be instantiated repeatedly for any number
    of rows; this is an inexpensive process.  However, the creation of the row
    adapter class itself *is* fairly expensive so caching should be used to prevent
    repeated calls to this function.
    """
    
    map = {}
    for c in to.c:
        corr = from_.corresponding_column(c)
        if corr:
            map[c] = corr
        elif equivalent_columns:
            if c in equivalent_columns:
                for c2 in equivalent_columns[c]:
                    corr = from_.corresponding_column(c2)
                    if corr:
                        map[c] = corr
                        break

    class AliasedRow(object):
        def __init__(self, row):
            self.row = row
        def __contains__(self, key):
            if key in map:
                return map[key] in self.row
            else:
                return key in self.row
        def has_key(self, key):
            return key in self
        def __getitem__(self, key):
            if key in map:
                key = map[key]
            return self.row[key]
        def keys(self):
            return map.keys()
    AliasedRow.map = map
    return AliasedRow

class PropertyAliasedClauses(AliasedClauses):
    """extends AliasedClauses to add support for primary/secondary joins on a relation()."""
    
    def __init__(self, prop, primaryjoin, secondaryjoin, parentclauses=None):
        super(PropertyAliasedClauses, self).__init__(prop.select_table)
            
        self.parentclauses = parentclauses
        if parentclauses is not None:
            self.path = build_path(prop.parent, prop.key, parentclauses.path)
        else:
            self.path = build_path(prop.parent, prop.key)

        self.prop = prop
        
        if prop.secondary:
            self.secondary = prop.secondary.alias()
            if parentclauses is not None:
                primary_aliasizer = sql_util.ClauseAdapter(self.secondary).chain(sql_util.ClauseAdapter(parentclauses.alias))
                secondary_aliasizer = sql_util.ClauseAdapter(self.alias).chain(sql_util.ClauseAdapter(self.secondary))

            else:
                primary_aliasizer = sql_util.ClauseAdapter(self.secondary)
                secondary_aliasizer = sql_util.ClauseAdapter(self.alias).chain(sql_util.ClauseAdapter(self.secondary))
                
            self.secondaryjoin = secondary_aliasizer.traverse(secondaryjoin, clone=True)
            self.primaryjoin = primary_aliasizer.traverse(primaryjoin, clone=True)
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

def state_str(state):
    """Return a string describing an instance."""
    if state is None:
        return "None"
    else:
        return state.class_.__name__ + "@" + hex(id(state.obj()))

def attribute_str(instance, attribute):
    return instance_str(instance) + "." + attribute

def identity_equal(a, b):
    if a is b:
        return True
    id_a = getattr(a, '_instance_key', None)
    id_b = getattr(b, '_instance_key', None)
    if id_a is None or id_b is None:
        return False
    return id_a == id_b

