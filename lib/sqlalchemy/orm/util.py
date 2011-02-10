# orm/util.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import sqlalchemy.exceptions as sa_exc
from sqlalchemy import sql, util
from sqlalchemy.sql import expression, util as sql_util, operators
from sqlalchemy.orm.interfaces import MapperExtension, EXT_CONTINUE,\
                                PropComparator, MapperProperty,\
                                AttributeExtension
from sqlalchemy.orm import attributes, exc

mapperlib = util.importlater("sqlalchemy.orm", "mapperlib")

all_cascades = frozenset(("delete", "delete-orphan", "all", "merge",
                          "expunge", "save-update", "refresh-expire",
                          "none"))

_INSTRUMENTOR = ('mapper', 'instrumentor')

class CascadeOptions(object):
    """Keeps track of the options sent to relationship().cascade"""

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

        if self.delete_orphan and not self.delete:
            util.warn("The 'delete-orphan' cascade option requires "
                        "'delete'.  This will raise an error in 0.6.")

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
    """Runs a validation method on an attribute value to be set or appended.

    The Validator class is used by the :func:`~sqlalchemy.orm.validates`
    decorator, and direct access is usually not needed.

    """

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

    See  :ref:`concrete_inheritance` for an example of how
    this is used.
    """

    colnames = set()
    colnamemaps = {}
    types = {}
    for key in table_map.keys():
        table = table_map[key]

        # mysql doesnt like selecting from a select; 
        # make it an alias of the select
        if isinstance(table, sql.Select):
            table = table.alias()
            table_map[key] = table

        m = {}
        for c in table.c:
            colnames.add(c.key)
            m[c.key] = c
            types[c.key] = c.type
        colnamemaps[table] = m

    def col(name, table):
        try:
            return colnamemaps[table][name]
        except KeyError:
            return sql.cast(sql.null(), types[name]).label(name)

    result = []
    for type, table in table_map.iteritems():
        if typecolname is not None:
            result.append(
                    sql.select([col(name, table) for name in colnames] +
                    [sql.literal_column(sql_util._quote_ddl_expr(type)).
                            label(typecolname)],
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
    """Extends ColumnAdapter to accept ORM entities.

    The selectable is extracted from the given entity,
    and the AliasedClass if any is referenced.

    """
    def __init__(self, entity, equivalents=None, 
                            chain_to=None, adapt_required=False):
        self.mapper, selectable, is_aliased_class = _entity_info(entity)
        if is_aliased_class:
            self.aliased_class = entity
        else:
            self.aliased_class = None
        sql_util.ColumnAdapter.__init__(self, selectable, 
                                        equivalents, chain_to,
                                        adapt_required=adapt_required)

    def replace(self, elem):
        entity = elem._annotations.get('parentmapper', None)
        if not entity or entity.isa(self.mapper):
            return sql_util.ColumnAdapter.replace(self, elem)
        else:
            return None

class AliasedClass(object):
    """Represents an "aliased" form of a mapped class for usage with Query.

    The ORM equivalent of a :func:`sqlalchemy.sql.expression.alias`
    construct, this object mimics the mapped class using a
    __getattr__ scheme and maintains a reference to a
    real :class:`~sqlalchemy.sql.expression.Alias` object.

    Usage is via the :class:`~sqlalchemy.orm.aliased()` synonym::

        # find all pairs of users with the same name
        user_alias = aliased(User)
        session.query(User, user_alias).\\
                        join((user_alias, User.id > user_alias.id)).\\
                        filter(User.name==user_alias.name)

    """
    def __init__(self, cls, alias=None, name=None):
        self.__mapper = _class_to_mapper(cls)
        self.__target = self.__mapper.class_
        if alias is None:
            alias = self.__mapper._with_polymorphic_selectable.alias()
        self.__adapter = sql_util.ClauseAdapter(alias,
                                equivalents=self.__mapper._equivalent_columns)
        self.__alias = alias
        # used to assign a name to the RowTuple object
        # returned by Query.
        self._sa_label_name = name
        self.__name__ = 'AliasedClass_' + str(self.__target)

    def __getstate__(self):
        return {
            'mapper':self.__mapper, 
            'alias':self.__alias, 
            'name':self._sa_label_name
        }

    def __setstate__(self, state):
        self.__mapper = state['mapper']
        self.__target = self.__mapper.class_
        alias = state['alias']
        self.__adapter = sql_util.ClauseAdapter(alias,
                                equivalents=self.__mapper._equivalent_columns)
        self.__alias = alias
        name = state['name']
        self._sa_label_name = name
        self.__name__ = 'AliasedClass_' + str(self.__target)

    def __adapt_element(self, elem):
        return self.__adapter.traverse(elem).\
                    _annotate({
                        'parententity': self, 
                        'parentmapper':self.__mapper}
                    )

    def __adapt_prop(self, prop):
        existing = getattr(self.__target, prop.key)
        comparator = existing.comparator.adapted(self.__adapt_element)

        queryattr = attributes.QueryableAttribute(prop.key,
            impl=existing.impl, parententity=self, comparator=comparator)
        setattr(self, prop.key, queryattr)
        return queryattr

    def __getattr__(self, key):
        if self.__mapper.has_property(key):
            return self.__adapt_prop(
                        self.__mapper.get_property(
                            key, _compile_mappers=False
                        )
                    )

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
                return util.types.MethodType(attr.im_func, self, self)
            else:
                return None
        elif hasattr(attr, '__get__'):
            return attr.__get__(None, self)
        else:
            return attr

    def __repr__(self):
        return '<AliasedClass at 0x%x; %s>' % (
            id(self), self.__target.__name__)

def _orm_annotate(element, exclude=None):
    """Deep copy the given ClauseElement, annotating each element with the
    "_orm_adapt" flag.

    Elements within the exclude collection will be cloned but not annotated.

    """
    return sql_util._deep_annotate(element, {'_orm_adapt':True}, exclude)

_orm_deannotate = sql_util._deep_deannotate

class _ORMJoin(expression.Join):
    """Extend Join to support ORM constructs as input."""

    __visit_name__ = expression.Join.__visit_name__

    def __init__(self, left, right, onclause=None, 
                            isouter=False, join_to_left=True):
        adapt_from = None

        if hasattr(left, '_orm_mappers'):
            left_mapper = left._orm_mappers[1]
            if join_to_left:
                adapt_from = left.right
        else:
            left_mapper, left, left_is_aliased = _entity_info(left)
            if join_to_left and (left_is_aliased or not left_mapper):
                adapt_from = left

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
                if adapt_from is None:
                    adapt_from = onclause.__clause_element__()
                prop = onclause.property
            elif isinstance(onclause, MapperProperty):
                prop = onclause
            else:
                prop = None

            if prop:
                pj, sj, source, dest, \
                secondary, target_adapter = prop._create_joins(
                                source_selectable=adapt_from,
                                dest_selectable=adapt_to,
                                source_polymorphic=True,
                                dest_polymorphic=True,
                                of_type=right_mapper)

                if sj is not None:
                    left = sql.join(left, secondary, pj, isouter)
                    onclause = sj
                else:
                    onclause = pj
                self._target_adapter = target_adapter

        expression.Join.__init__(self, left, right, onclause, isouter)

    def join(self, right, onclause=None, isouter=False, join_to_left=True):
        return _ORMJoin(self, right, onclause, isouter, join_to_left)

    def outerjoin(self, right, onclause=None, join_to_left=True):
        return _ORMJoin(self, right, onclause, True, join_to_left)

def join(left, right, onclause=None, isouter=False, join_to_left=True):
    """Produce an inner join between left and right clauses.

    In addition to the interface provided by
    :func:`~sqlalchemy.sql.expression.join()`, left and right may be mapped
    classes or AliasedClass instances. The onclause may be a
    string name of a relationship(), or a class-bound descriptor
    representing a relationship.

    join_to_left indicates to attempt aliasing the ON clause,
    in whatever form it is passed, to the selectable
    passed as the left side.  If False, the onclause
    is used as is.

    """
    return _ORMJoin(left, right, onclause, isouter, join_to_left)

def outerjoin(left, right, onclause=None, join_to_left=True):
    """Produce a left outer join between left and right clauses.

    In addition to the interface provided by
    :func:`~sqlalchemy.sql.expression.outerjoin()`, left and right may be
    mapped classes or AliasedClass instances. The onclause may be a string
    name of a relationship(), or a class-bound descriptor representing a
    relationship.

    """
    return _ORMJoin(left, right, onclause, True, join_to_left)

def with_parent(instance, prop):
    """Create filtering criterion that relates this query's primary entity
    to the given related instance, using established :func:`.relationship()`
    configuration.

    The SQL rendered is the same as that rendered when a lazy loader
    would fire off from the given parent on that attribute, meaning
    that the appropriate state is taken from the parent object in 
    Python without the need to render joins to the parent table
    in the rendered statement.

    As of 0.6.4, this method accepts parent instances in all 
    persistence states, including transient, persistent, and detached.
    Only the requisite primary key/foreign key attributes need to
    be populated.  Previous versions didn't work with transient
    instances.

    :param instance:
      An instance which has some :func:`.relationship`.

    :param property:
      String property name, or class-bound attribute, which indicates
      what relationship from the instance should be used to reconcile the 
      parent/child relationship. 

    """
    if isinstance(prop, basestring):
        mapper = object_mapper(instance)
        prop = mapper.get_property(prop, resolve_synonyms=True)
    elif isinstance(prop, attributes.QueryableAttribute):
        prop = prop.property

    return prop.compare(operators.eq, 
                        instance, 
                        value_is_parent=True)


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

    if isinstance(entity, mapperlib.Mapper):
        mapper = entity

    elif isinstance(entity, type):
        class_manager = attributes.manager_of_class(entity)

        if class_manager is None:
            return None, entity, False

        mapper = class_manager.mapper
    else:
        return None, entity, False

    if compile:
        mapper = mapper.compile()
    return mapper, mapper._with_polymorphic_selectable, False

def _entity_descriptor(entity, key):
    """Return a class attribute given an entity and string name.

    May return :class:`.InstrumentedAttribute` or user-defined
    attribute.

    """
    if not isinstance(entity, (AliasedClass, type)):
        entity = entity.class_

    try:
        return getattr(entity, key)
    except AttributeError:
        raise sa_exc.InvalidRequestError(
                    "Entity '%s' has no property '%s'" % 
                    (entity, key)
                )

def _orm_columns(entity):
    mapper, selectable, is_aliased_class = _entity_info(entity)
    if isinstance(selectable, expression.Selectable):
        return [c for c in selectable.c]
    else:
        return [selectable]

def _orm_selectable(entity):
    mapper, selectable, is_aliased_class = _entity_info(entity)
    return selectable

def _attr_as_key(attr):
    if hasattr(attr, 'key'):
        return attr.key
    else:
        return expression._column_as_key(attr)

def _is_aliased_class(entity):
    return isinstance(entity, AliasedClass)

def _state_mapper(state):
    return state.manager.mapper

def object_mapper(instance):
    """Given an object, return the primary Mapper associated with the object
    instance.

    Raises UnmappedInstanceError if no mapping is configured.

    """
    try:
        state = attributes.instance_state(instance)
        return state.manager.mapper
    except exc.UnmappedClassError:
        raise exc.UnmappedInstanceError(instance)
    except exc.NO_STATE:
        raise exc.UnmappedInstanceError(instance)

def class_mapper(class_, compile=True):
    """Given a class, return the primary Mapper associated with the key.

    Raises UnmappedClassError if no mapping is configured.

    """

    try:
        class_manager = attributes.manager_of_class(class_)
        mapper = class_manager.mapper

    except exc.NO_STATE:
        raise exc.UnmappedClassError(class_)

    if compile:
        mapper = mapper.compile()
    return mapper

def _class_to_mapper(class_or_mapper, compile=True):
    if _is_aliased_class(class_or_mapper):
        return class_or_mapper._AliasedClass__mapper

    elif isinstance(class_or_mapper, type):
        try:
            class_manager = attributes.manager_of_class(class_or_mapper)
            mapper = class_manager.mapper
        except exc.NO_STATE:
            raise exc.UnmappedClassError(class_or_mapper)
    elif isinstance(class_or_mapper, mapperlib.Mapper):
        mapper = class_or_mapper
    else:
        raise exc.UnmappedClassError(class_or_mapper)

    if compile:
        return mapper.compile()
    else:
        return mapper

def has_identity(object):
    state = attributes.instance_state(object)
    return state.has_identity

def _is_mapped_class(cls):
    if isinstance(cls, (AliasedClass, mapperlib.Mapper)):
        return True
    if isinstance(cls, expression.ClauseElement):
        return False
    if isinstance(cls, type):
        manager = attributes.manager_of_class(cls)
        return manager and _INSTRUMENTOR in manager.info
    return False

def instance_str(instance):
    """Return a string describing an instance."""

    return state_str(attributes.instance_state(instance))

def state_str(state):
    """Return a string describing an instance via its InstanceState."""

    if state is None:
        return "None"
    else:
        return '<%s at 0x%x>' % (state.class_.__name__, id(state.obj()))

def state_class_str(state):
    """Return a string describing an instance's class via its InstanceState."""

    if state is None:
        return "None"
    else:
        return '<%s>' % (state.class_.__name__, )

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

