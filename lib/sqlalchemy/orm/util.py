# mapper/util.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import new

import sqlalchemy.exceptions as sa_exc
from sqlalchemy import sql, util
from sqlalchemy.sql import expression, util as sql_util, operators
from sqlalchemy.orm.interfaces import MapperExtension, EXT_CONTINUE, PropComparator, MapperProperty, AttributeExtension
from sqlalchemy.orm import attributes, exc


all_cascades = frozenset(("delete", "delete-orphan", "all", "merge",
                          "expunge", "save-update", "refresh-expire",
                          "none"))

_INSTRUMENTOR = ('mapper', 'instrumentor')

class CascadeOptions(object):
    """Keeps track of the options sent to relation().cascade"""

    def __init__(self, arg=""):
        if not arg:
            values = set()
        else:
            values = set(c.strip() for c in arg.split(','))
        self.delete_orphan = "delete-orphan" in values
        self.delete = "delete" in values or "all" in values
        self.save_update = "save-update" in values or "all" in values
        self.merge = "merge" in values or "all" in values
        self.expunge = "expunge" in values or "all" in values
        self.refresh_expire = "refresh-expire" in values or "all" in values
        for x in values:
            if x not in all_cascades:
                raise sa_exc.ArgumentError("Invalid cascade option '%s'" % x)

    def __contains__(self, item):
        return getattr(self, item.replace("-", "_"), False)

    def __repr__(self):
        return "CascadeOptions(%s)" % repr(",".join(
            [x for x in ['delete', 'save_update', 'merge', 'expunge',
                         'delete_orphan', 'refresh-expire']
             if getattr(self, x, False) is True]))


class Validator(AttributeExtension):
    """Runs a validation method on an attribute value to be set or appended."""
    
    def __init__(self, key, validator):
        """Construct a new Validator.
        
            key - name of the attribute to be validated;
            will be passed as the second argument to 
            the validation method (the first is the object instance itself).
            
            validator - an function or instance method which accepts
            three arguments; an instance (usually just 'self' for a method),
            the key name of the attribute, and the value.  The function should
            return the same value given, unless it wishes to modify it.
            
        """
        self.key = key
        self.validator = validator
    
    def append(self, state, value, initiator):
        return self.validator(state.obj(), self.key, value)

    def set(self, state, value, oldvalue, initiator):
        return self.validator(state.obj(), self.key, value)
    
def polymorphic_union(table_map, typecolname, aliasname='p_union'):
    """Create a ``UNION`` statement used by a polymorphic mapper.

    See the `SQLAlchemy` advanced mapping docs for an example of how
    this is used.
    """

    colnames = set()
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
            result.append(sql.select([col(name, table) for name in colnames],
                                     from_obj=[table]))
    return sql.union_all(*result).alias(aliasname)

def identity_key(*args, **kwargs):
    """Get an identity key.

    Valid call signatures:

    * ``identity_key(class, ident)``

      class
          mapped class (must be a positional argument)

      ident
          primary key, if the key is composite this is a tuple


    * ``identity_key(instance=instance)``

      instance
          object instance (must be given as a keyword arg)

    * ``identity_key(class, row=row)``

      class
          mapped class (must be a positional argument)

      row
          result proxy row (must be given as a keyword arg)

    """
    if args:
        if len(args) == 1:
            class_ = args[0]
            try:
                row = kwargs.pop("row")
            except KeyError:
                ident = kwargs.pop("ident")
        elif len(args) == 2:
            class_, ident = args
        elif len(args) == 3:
            class_, ident = args
        else:
            raise sa_exc.ArgumentError("expected up to three "
                "positional arguments, got %s" % len(args))
        if kwargs:
            raise sa_exc.ArgumentError("unknown keyword arguments: %s"
                % ", ".join(kwargs.keys()))
        mapper = class_mapper(class_)
        if "ident" in locals():
            return mapper.identity_key_from_primary_key(ident)
        return mapper.identity_key_from_row(row)
    instance = kwargs.pop("instance")
    if kwargs:
        raise sa_exc.ArgumentError("unknown keyword arguments: %s"
            % ", ".join(kwargs.keys()))
    mapper = object_mapper(instance)
    return mapper.identity_key_from_instance(instance)
    
class ExtensionCarrier(dict):
    """Fronts an ordered collection of MapperExtension objects.

    Bundles multiple MapperExtensions into a unified callable unit,
    encapsulating ordering, looping and EXT_CONTINUE logic.  The
    ExtensionCarrier implements the MapperExtension interface, e.g.::

      carrier.after_insert(...args...)

    The dictionary interface provides containment for implemented
    method names mapped to a callable which executes that method
    for participating extensions.

    """

    interface = set(method for method in dir(MapperExtension)
                    if not method.startswith('_'))

    def __init__(self, extensions=None):
        self._extensions = []
        for ext in extensions or ():
            self.append(ext)

    def copy(self):
        return ExtensionCarrier(self._extensions)

    def push(self, extension):
        """Insert a MapperExtension at the beginning of the collection."""
        self._register(extension)
        self._extensions.insert(0, extension)

    def append(self, extension):
        """Append a MapperExtension at the end of the collection."""
        self._register(extension)
        self._extensions.append(extension)

    def __iter__(self):
        """Iterate over MapperExtensions in the collection."""
        return iter(self._extensions)

    def _register(self, extension):
        """Register callable fronts for overridden interface methods."""
        
        for method in self.interface.difference(self):
            impl = getattr(extension, method, None)
            if impl and impl is not getattr(MapperExtension, method):
                self[method] = self._create_do(method)

    def _create_do(self, method):
        """Return a closure that loops over impls of the named method."""

        def _do(*args, **kwargs):
            for ext in self._extensions:
                ret = getattr(ext, method)(*args, **kwargs)
                if ret is not EXT_CONTINUE:
                    return ret
            else:
                return EXT_CONTINUE
        _do.__name__ = method
        return _do

    @staticmethod
    def _pass(*args, **kwargs):
        return EXT_CONTINUE

    def __getattr__(self, key):
        """Delegate MapperExtension methods to bundled fronts."""
        
        if key not in self.interface:
            raise AttributeError(key)
        return self.get(key, self._pass)

class ORMAdapter(sql_util.ColumnAdapter):
    def __init__(self, entity, equivalents=None, chain_to=None):
        mapper, selectable, is_aliased_class = _entity_info(entity)
        if is_aliased_class:
            self.aliased_class = entity
        else:
            self.aliased_class = None
        sql_util.ColumnAdapter.__init__(self, selectable, equivalents, chain_to)

class AliasedClass(object):
    def __init__(self, cls, alias=None, name=None):
        self.__mapper = _class_to_mapper(cls)
        self.__target = self.__mapper.class_
        alias = alias or self.__mapper._with_polymorphic_selectable.alias()
        self.__adapter = sql_util.ClauseAdapter(alias, equivalents=self.__mapper._equivalent_columns)
        self.__alias = alias
        self._sa_label_name = name
        self.__name__ = 'AliasedClass_' + str(self.__target)

    def __adapt_prop(self, prop):
        existing = getattr(self.__target, prop.key)
        comparator = AliasedComparator(self, self.__adapter, existing.comparator)
        queryattr = attributes.QueryableAttribute(
            existing.impl, parententity=self, comparator=comparator)
        setattr(self, prop.key, queryattr)
        return queryattr

    def __getattr__(self, key):
        prop = self.__mapper._get_property(key, raiseerr=False)
        if prop:
            return self.__adapt_prop(prop)

        for base in self.__target.__mro__:
            try:
                attr = object.__getattribute__(base, key)
            except AttributeError:
                continue
            else:
                break
        else:
            raise AttributeError(key)

        if hasattr(attr, 'func_code'):
            is_method = getattr(self.__target, key, None)
            if is_method and is_method.im_self is not None:
                return new.instancemethod(attr.im_func, self, self)
            else:
                return None
        elif hasattr(attr, '__get__'):
            return attr.__get__(None, self)
        else:
            return attr

    def __repr__(self):
        return '<AliasedClass at 0x%x; %s>' % (
            id(self), self.__target.__name__)

class AliasedComparator(PropComparator):
    def __init__(self, aliasedclass, adapter, comparator):
        self.aliasedclass = aliasedclass
        self.comparator = comparator
        self.adapter = adapter
        self.__clause_element = self.adapter.traverse(self.comparator.__clause_element__())._annotate({'parententity': aliasedclass})

    def __clause_element__(self):
        return self.__clause_element

    def operate(self, op, *other, **kwargs):
        return self.adapter.traverse(self.comparator.operate(op, *other, **kwargs))

    def reverse_operate(self, op, other, **kwargs):
        return self.adapter.traverse(self.comparator.reverse_operate(op, *other, **kwargs))

def _orm_annotate(element, exclude=None):
    """Deep copy the given ClauseElement, annotating each element with the "_orm_adapt" flag.
    
    Elements within the exclude collection will be cloned but not annotated.
    
    """
    def clone(elem):
        if exclude and elem in exclude:
            elem = elem._clone()
        elif '_orm_adapt' not in elem._annotations:
            elem = elem._annotate({'_orm_adapt':True})
        elem._copy_internals(clone=clone)
        return elem
    
    if element is not None:
        element = clone(element)
    return element


class _ORMJoin(expression.Join):
    """Extend Join to support ORM constructs as input."""
    
    __visit_name__ = expression.Join.__visit_name__

    def __init__(self, left, right, onclause=None, isouter=False):
        if hasattr(left, '_orm_mappers'):
            left_mapper = left._orm_mappers[1]
            adapt_from = left.right

        else:
            left_mapper, left, left_is_aliased = _entity_info(left)
            if left_is_aliased or not left_mapper:
                adapt_from = left
            else:
                adapt_from = None

        right_mapper, right, right_is_aliased = _entity_info(right)
        if right_is_aliased:
            adapt_to = right
        else:
            adapt_to = None

        if left_mapper or right_mapper:
            self._orm_mappers = (left_mapper, right_mapper)

            if isinstance(onclause, basestring):
                prop = left_mapper.get_property(onclause)
            elif isinstance(onclause, attributes.QueryableAttribute):
                adapt_from = onclause.__clause_element__()
                prop = onclause.property
            elif isinstance(onclause, MapperProperty):
                prop = onclause
            else:
                prop = None

            if prop:
                pj, sj, source, dest, secondary, target_adapter = prop._create_joins(source_selectable=adapt_from, dest_selectable=adapt_to, source_polymorphic=True, dest_polymorphic=True)

                if sj:
                    left = sql.join(left, secondary, pj, isouter)
                    onclause = sj
                else:
                    onclause = pj
                self._target_adapter = target_adapter

        expression.Join.__init__(self, left, right, onclause, isouter)

    def join(self, right, onclause=None, isouter=False):
        return _ORMJoin(self, right, onclause, isouter)

    def outerjoin(self, right, onclause=None):
        return _ORMJoin(self, right, onclause, True)

def join(left, right, onclause=None, isouter=False):
    """Produce an inner join between left and right clauses.
    
    In addition to the interface provided by 
    sqlalchemy.sql.join(), left and right may be mapped 
    classes or AliasedClass instances. The onclause may be a 
    string name of a relation(), or a class-bound descriptor 
    representing a relation.
    
    """
    return _ORMJoin(left, right, onclause, isouter)

def outerjoin(left, right, onclause=None):
    """Produce a left outer join between left and right clauses.
    
    In addition to the interface provided by 
    sqlalchemy.sql.outerjoin(), left and right may be mapped 
    classes or AliasedClass instances. The onclause may be a 
    string name of a relation(), or a class-bound descriptor 
    representing a relation.
    
    """
    return _ORMJoin(left, right, onclause, True)

def with_parent(instance, prop):
    """Return criterion which selects instances with a given parent.

    instance
      a parent instance, which should be persistent or detached.

     property
       a class-attached descriptor, MapperProperty or string property name
       attached to the parent instance.

     \**kwargs
       all extra keyword arguments are propagated to the constructor of
       Query.

    """
    if isinstance(prop, basestring):
        mapper = object_mapper(instance)
        prop = mapper.get_property(prop, resolve_synonyms=True)
    elif isinstance(prop, attributes.QueryableAttribute):
        prop = prop.property

    return prop.compare(operators.eq, instance, value_is_parent=True)


def _entity_info(entity, compile=True):
    """Return mapping information given a class, mapper, or AliasedClass.
    
    Returns 3-tuple of: mapper, mapped selectable, boolean indicating if this
    is an aliased() construct.
    
    If the given entity is not a mapper, mapped class, or aliased construct,
    returns None, the entity, False.  This is typically used to allow
    unmapped selectables through.
    
    """
    if isinstance(entity, AliasedClass):
        return entity._AliasedClass__mapper, entity._AliasedClass__alias, True
    elif _is_mapped_class(entity):
        if isinstance(entity, type):
            mapper = class_mapper(entity, compile)
        else:
            if compile:
                mapper = entity.compile()
            else:
                mapper = entity
        return mapper, mapper._with_polymorphic_selectable, False
    else:
        return None, entity, False

def _entity_descriptor(entity, key):
    """Return attribute/property information given an entity and string name.
    
    Returns a 2-tuple representing InstrumentedAttribute/MapperProperty.
    
    """
    if isinstance(entity, AliasedClass):
        desc = getattr(entity, key)
        return desc, desc.property
    elif isinstance(entity, type):
        desc = attributes.manager_of_class(entity)[key]
        return desc, desc.property
    else:
        desc = entity.class_manager[key]
        return desc, desc.property

def _orm_columns(entity):
    mapper, selectable, is_aliased_class = _entity_info(entity)
    if isinstance(selectable, expression.Selectable):
        return [c for c in selectable.c]
    else:
        return [selectable]

def _orm_selectable(entity):
    mapper, selectable, is_aliased_class = _entity_info(entity)
    return selectable

def _is_aliased_class(entity):
    return isinstance(entity, AliasedClass)

def _state_mapper(state):
    return state.manager.mapper

def object_mapper(instance):
    """Given an object, return the primary Mapper associated with the object instance.
    
    Raises UnmappedInstanceError if no mapping is configured.
    
    """
    try:
        state = attributes.instance_state(instance)
        if not state.manager.mapper:
            raise exc.UnmappedInstanceError(instance)
        return state.manager.mapper
    except exc.NO_STATE:
        raise exc.UnmappedInstanceError(instance)

def class_mapper(class_, compile=True):
    """Given a class (or an object), return the primary Mapper associated with the key.

    Raises UnmappedClassError if no mapping is configured.
    
    """
    if not isinstance(class_, type):
        class_ = type(class_)
    try:
        class_manager = attributes.manager_of_class(class_)
        mapper = class_manager.mapper
        
        # HACK until [ticket:1142] is complete
        if mapper is None:
            raise AttributeError
            
    except exc.NO_STATE:
        raise exc.UnmappedClassError(class_)

    if compile:
        mapper = mapper.compile()
    return mapper

def _class_to_mapper(class_or_mapper, compile=True):
    if _is_aliased_class(class_or_mapper):
        return class_or_mapper._AliasedClass__mapper
    elif isinstance(class_or_mapper, type):
        return class_mapper(class_or_mapper, compile=compile)
    elif hasattr(class_or_mapper, 'compile'):
        if compile:
            return class_or_mapper.compile()
        else:
            return class_or_mapper
    else:
        raise exc.UnmappedClassError(class_or_mapper)

def has_identity(object):
    state = attributes.instance_state(object)
    return _state_has_identity(state)

def _state_has_identity(state):
    return bool(state.key)

def _is_mapped_class(cls):
    from sqlalchemy.orm import mapperlib as mapper
    if isinstance(cls, (AliasedClass, mapper.Mapper)):
        return True

    manager = attributes.manager_of_class(cls)
    return manager and _INSTRUMENTOR in manager.info

def instance_str(instance):
    """Return a string describing an instance."""

    return state_str(attributes.instance_state(instance))

def state_str(state):
    """Return a string describing an instance via its InstanceState."""
    
    if state is None:
        return "None"
    else:
        return '<%s at 0x%x>' % (state.class_.__name__, id(state.obj()))

def attribute_str(instance, attribute):
    return instance_str(instance) + "." + attribute

def state_attribute_str(state, attribute):
    return state_str(state) + "." + attribute

def identity_equal(a, b):
    if a is b:
        return True
    if a is None or b is None:
        return False
    try:
        state_a = attributes.instance_state(a)
        state_b = attributes.instance_state(b)
    except exc.NO_STATE:
        return False
    if state_a.key is None or state_b.key is None:
        return False
    return state_a.key == state_b.key


# TODO: Avoid circular import.
attributes.identity_equal = identity_equal
attributes._is_aliased_class = _is_aliased_class
attributes._entity_info = _entity_info
