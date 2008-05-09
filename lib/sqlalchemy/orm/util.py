# mapper/util.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import sql, util, exceptions
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql.util import row_adapter as create_row_adapter
from sqlalchemy.sql import visitors
from sqlalchemy.orm.interfaces import MapperExtension, EXT_CONTINUE, PropComparator

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
    """Creates aliases of a mapped tables for usage in ORM queries, and provides expression adaptation."""

    def __init__(self, alias, equivalents=None, chain_to=None, should_adapt=True):
        self.alias = alias
        self.equivalents = equivalents
        self.row_decorator = self._create_row_adapter()
        self.should_adapt = should_adapt
        if should_adapt:
            self.adapter = sql_util.ClauseAdapter(self.alias, equivalents=equivalents)
        else:
            self.adapter = visitors.NullVisitor()

        if chain_to:
            self.adapter.chain(chain_to.adapter)
            
    def aliased_column(self, column):
        if not self.should_adapt:
            return column
            
        conv = self.alias.corresponding_column(column)
        if conv:
            return conv
        
        # process column-level subqueries    
        aliased_column = sql_util.ClauseAdapter(self.alias, equivalents=self.equivalents).traverse(column, clone=True)

        # anonymize labels which might have specific names
        if isinstance(aliased_column, expression._Label):
            aliased_column = aliased_column.label(None)

        # add to row decorator explicitly
        self.row_decorator({}).map[column] = aliased_column
        return aliased_column

    def adapt_clause(self, clause):
        return self.adapter.traverse(clause, clone=True)
    
    def adapt_list(self, clauses):
        return self.adapter.copy_and_process(clauses)
        
    def _create_row_adapter(self):
        return create_row_adapter(self.alias, equivalent_columns=self.equivalents)


class PropertyAliasedClauses(AliasedClauses):
    """extends AliasedClauses to add support for primary/secondary joins on a relation()."""
    
    def __init__(self, prop, primaryjoin, secondaryjoin, parentclauses=None, alias=None, should_adapt=True):
        self.prop = prop
        self.mapper = self.prop.mapper
        self.table = self.prop.table
        self.parentclauses = parentclauses

        if not alias:
            from_obj = self.mapper._with_polymorphic_selectable()
            alias = from_obj.alias()

        super(PropertyAliasedClauses, self).__init__(alias, equivalents=self.mapper._equivalent_columns, chain_to=parentclauses, should_adapt=should_adapt)
        
        if prop.secondary:
            self.secondary = prop.secondary.alias()
            primary_aliasizer = sql_util.ClauseAdapter(self.secondary)
            secondary_aliasizer = sql_util.ClauseAdapter(self.alias, equivalents=self.equivalents).chain(sql_util.ClauseAdapter(self.secondary))

            if parentclauses is not None:
                primary_aliasizer.chain(sql_util.ClauseAdapter(parentclauses.alias, equivalents=parentclauses.equivalents))

            self.secondaryjoin = secondary_aliasizer.traverse(secondaryjoin, clone=True)
            self.primaryjoin = primary_aliasizer.traverse(primaryjoin, clone=True)
        else:
            primary_aliasizer = sql_util.ClauseAdapter(self.alias, exclude=prop.local_side, equivalents=self.equivalents)
            if parentclauses is not None: 
                primary_aliasizer.chain(sql_util.ClauseAdapter(parentclauses.alias, exclude=prop.remote_side, equivalents=parentclauses.equivalents))
            
            self.primaryjoin = primary_aliasizer.traverse(primaryjoin, clone=True)
            self.secondary = None
            self.secondaryjoin = None
        
        if prop.order_by:
            if prop.secondary:
                # usually this is not used but occasionally someone has a sort key in their secondary
                # table, even tho SA does not support writing this column directly
                self.order_by = secondary_aliasizer.copy_and_process(util.to_list(prop.order_by))
            else:
                self.order_by = primary_aliasizer.copy_and_process(util.to_list(prop.order_by))
                
        else:
            self.order_by = None

class AliasedClass(object):
    def __new__(cls, target):
        from sqlalchemy.orm import attributes
        mapper = _class_to_mapper(target)
        alias = mapper.mapped_table.alias()
        retcls = type(target.__name__ + "Alias", (cls,), {'alias':alias})
        retcls._class_state = mapper._class_state
        for prop in mapper.iterate_properties:
            existing = mapper._class_state.attrs[prop.key]
            setattr(retcls, prop.key, attributes.InstrumentedAttribute(existing.impl, comparator=AliasedComparator(alias, existing.comparator)))

        return retcls

    def __init__(self, alias):
        self.alias = alias

class AliasedComparator(PropComparator):
    def __init__(self, alias, comparator):
        self.alias = alias
        self.comparator = comparator
        self.adapter = sql_util.ClauseAdapter(alias) 

    def clause_element(self):
        return self.adapter.traverse(self.comparator.clause_element(), clone=True)

    def operate(self, op, *other, **kwargs):
        return self.adapter.traverse(self.comparator.operate(op, *other, **kwargs), clone=True)

    def reverse_operate(self, op, other, **kwargs):
        return self.adapter.traverse(self.comparator.reverse_operate(op, *other, **kwargs), clone=True)

from sqlalchemy.sql import expression
_selectable = expression._selectable
def _orm_selectable(selectable):
    if _is_mapped_class(selectable):
        if _is_aliased_class(selectable):
            return selectable.alias
        else:
            return _class_to_mapper(selectable)._with_polymorphic_selectable()
    else:
        return _selectable(selectable)
expression._selectable = _orm_selectable

class _ORMJoin(expression.Join):
    """future functionality."""

    __visit_name__ = expression.Join.__visit_name__
    
    def __init__(self, left, right, onclause=None, isouter=False):
        if _is_mapped_class(left) or _is_mapped_class(right):
            if hasattr(left, '_orm_mappers'):
                left_mapper = left._orm_mappers[1]
                adapt_from = left.right
            else:
                left_mapper = _class_to_mapper(left)
                if _is_aliased_class(left):
                    adapt_from = left.alias
                else:
                    adapt_from = None

            right_mapper = _class_to_mapper(right)
            self._orm_mappers = (left_mapper, right_mapper)
            
            if isinstance(onclause, basestring):
                prop = left_mapper.get_property(onclause)

                if _is_aliased_class(right):
                    adapt_to = right.alias
                else:
                    adapt_to = None

                pj, sj, source, dest, target_adapter = prop._create_joins(source_selectable=adapt_from, dest_selectable=adapt_to, source_polymorphic=True, dest_polymorphic=True)

                if sj:
                    left = sql.join(left, prop.secondary, onclause=pj)
                    onclause = sj
                else:
                    onclause = pj
        expression.Join.__init__(self, left, right, onclause, isouter)

    def join(self, right, onclause=None, isouter=False):
        return _ORMJoin(self, right, onclause, isouter)

    def outerjoin(self, right, onclause=None):
        return _ORMJoin(self, right, onclause, True)

def _join(left, right, onclause=None):
    """future functionality."""
    
    return _ORMJoin(left, right, onclause, False)

def _outerjoin(left, right, onclause=None):
    """future functionality."""

    return _ORMJoin(left, right, onclause, True)
    
def has_identity(object):
    return hasattr(object, '_instance_key')

def _state_has_identity(state):
    return '_instance_key' in state.dict

def _is_mapped_class(cls):
    return hasattr(cls, '_class_state')

def _is_aliased_class(obj):
    return isinstance(obj, type) and issubclass(obj, AliasedClass)
    
def has_mapper(object):
    """Return True if the given object has had a mapper association
    set up, either through loading, or via insertion in a session.
    """

    return hasattr(object, '_entity_name')

def _state_mapper(state, entity_name=None):
    return state.class_._class_state.mappers[state.dict.get('_entity_name', entity_name)]

def object_mapper(object, entity_name=None, raiseerror=True):
    """Given an object, return the primary Mapper associated with the object instance.

        object
            The object instance.

        entity_name
            Entity name of the mapper to retrieve, if the given instance is
            transient.  Otherwise uses the entity name already associated
            with the instance.

        raiseerror
            Defaults to True: raise an ``InvalidRequestError`` if no mapper can
            be located.  If False, return None.

    """

    try:
        mapper = object.__class__._class_state.mappers[getattr(object, '_entity_name', entity_name)]
    except (KeyError, AttributeError):
        if raiseerror:
            raise exceptions.InvalidRequestError("Class '%s' entity name '%s' has no mapper associated with it" % (object.__class__.__name__, getattr(object, '_entity_name', entity_name)))
        else:
            return None
    return mapper

def class_mapper(class_, entity_name=None, compile=True, raiseerror=True):
    """Given a class and optional entity_name, return the primary Mapper associated with the key.

    If no mapper can be located, raises ``InvalidRequestError``.
    """

    try:
        mapper = class_._class_state.mappers[entity_name]
    except (KeyError, AttributeError):
        if raiseerror:
            raise exceptions.InvalidRequestError("Class '%s' entity name '%s' has no mapper associated with it" % (class_.__name__, entity_name))
        else:
            return None
    if compile:
        return mapper.compile()
    else:
        return mapper

def _class_to_mapper(class_or_mapper, entity_name=None, compile=True):
    if isinstance(class_or_mapper, type):
        return class_mapper(class_or_mapper, entity_name=entity_name, compile=compile)
    else:
        if compile:
            return class_or_mapper.compile()
        else:
            return class_or_mapper

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

