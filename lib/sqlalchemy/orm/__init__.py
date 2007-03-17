# mapper/__init__.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
The mapper package provides object-relational functionality, building upon the schema and sql
packages and tying operations to class properties and constructors.
"""

from sqlalchemy import exceptions
from sqlalchemy import util as sautil
from sqlalchemy.orm.mapper import *
from sqlalchemy.orm import mapper as mapperlib
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.util import polymorphic_union
from sqlalchemy.orm import properties, strategies, interfaces
from sqlalchemy.orm.session import Session as create_session
from sqlalchemy.orm.session import object_session, attribute_manager

__all__ = ['relation', 'backref', 'eagerload', 'lazyload', 'noload', 'deferred', 'defer', 'undefer', 'extension',
        'mapper', 'clear_mappers', 'compile_mappers', 'clear_mapper', 'class_mapper', 'object_mapper', 'MapperExtension', 'Query',
        'cascade_mappers', 'polymorphic_union', 'create_session', 'synonym', 'contains_alias', 'contains_eager', 'EXT_PASS', 'object_session'
        ]

def relation(*args, **kwargs):
    """Provide a relationship of a primary Mapper to a secondary Mapper.

    This corresponds to a parent-child or associative table relationship.
    """

    if len(args) > 1 and isinstance(args[0], type):
        raise exceptions.ArgumentError("relation(class, table, **kwargs) is deprecated.  Please use relation(class, **kwargs) or relation(mapper, **kwargs).")
    return _relation_loader(*args, **kwargs)

def _relation_loader(mapper, secondary=None, primaryjoin=None, secondaryjoin=None, lazy=True, **kwargs):
    return properties.PropertyLoader(mapper, secondary, primaryjoin, secondaryjoin, lazy=lazy, **kwargs)

def backref(name, **kwargs):
    """Create a BackRef object with explicit arguments, which are the same arguments one
    can send to ``relation()``.

    Used with the `backref` keyword argument to ``relation()`` in
    place of a string argument.
    """

    return properties.BackRef(name, **kwargs)

def deferred(*columns, **kwargs):
    """Return a ``DeferredColumnProperty``, which indicates this
    object attributes should only be loaded from its corresponding
    table column when first accessed.

    Used with the `properties` dictionary sent to ``mapper()``.
    """

    return properties.ColumnProperty(deferred=True, *columns, **kwargs)

def mapper(class_, table=None, *args, **params):
    """Return a new ``Mapper`` object.

    See the ``Mapper`` class for a description of arguments.
    """

    return Mapper(class_, table, *args, **params)

def synonym(name, proxy=False):
    """Set up `name` as a synonym to another ``MapperProperty``.

    Used with the `properties` dictionary sent to ``mapper()``.
    """

    return interfaces.SynonymProperty(name, proxy=proxy)

def compile_mappers():
    """Compile all mappers that have been defined.

    This is equivalent to calling ``compile()`` on any individual mapper.
    """

    if not len(mapper_registry):
        return
    mapper_registry.values()[0].compile()

def clear_mappers():
    """Remove all mappers that have been created thus far.

    When new mappers are created, they will be assigned to their
    classes as their primary mapper.
    """

    for mapper in mapper_registry.values():
        attribute_manager.reset_class_managed(mapper.class_)
        if hasattr(mapper.class_, 'c'):
            del mapper.class_.c
    mapper_registry.clear()
    sautil.ArgSingleton.instances.clear()

def clear_mapper(m):
    """Remove the given mapper from the storage of mappers.

    When a new mapper is created for the previous mapper's class, it
    will be used as that classes' new primary mapper.
    """

    del mapper_registry[m.class_key]
    attribute_manager.reset_class_managed(m.class_)
    if hasattr(m.class_, 'c'):
        del m.class_.c
    m.class_key.dispose()

def extension(ext):
    """Return a ``MapperOption`` that will insert the given
    ``MapperExtension`` to the beginning of the list of extensions
    that will be called in the context of the ``Query``.

    Used with ``query.options()``.
    """

    return ExtensionOption(ext)

def eagerload(name):
    """Return a ``MapperOption`` that will convert the property of the
    given name into an eager load.

    Used with ``query.options()``.
    """

    return strategies.EagerLazyOption(name, lazy=False)

def lazyload(name):
    """Return a ``MapperOption`` that will convert the property of the
    given name into a lazy load.

    Used with ``query.options()``.
    """

    return strategies.EagerLazyOption(name, lazy=True)

def noload(name):
    """Return a ``MapperOption`` that will convert the property of the
    given name into a non-load.

    Used with ``query.options()``.
    """

    return strategies.EagerLazyOption(name, lazy=None)

def contains_alias(alias):
    """Return a ``MapperOption`` that will indicate to the query that
    the main table has been aliased.

    `alias` is the string name or ``Alias`` object representing the
    alias.
    """

    class AliasedRow(MapperExtension):
        def __init__(self, alias):
            self.alias = alias
            if isinstance(self.alias, basestring):
                self.selectable = None
            else:
                self.selectable = alias
        def get_selectable(self, mapper):
            if self.selectable is None:
                self.selectable = mapper.mapped_table.alias(self.alias)
            return self.selectable
        def translate_row(self, mapper, context, row):
            newrow = sautil.DictDecorator(row)
            selectable = self.get_selectable(mapper)
            for c in mapper.mapped_table.c:
                c2 = selectable.corresponding_column(c, keys_ok=True, raiseerr=False)
                if c2 and row.has_key(c2):
                    newrow[c] = row[c2]
            return newrow

    return ExtensionOption(AliasedRow(alias))

def contains_eager(key, alias=None, decorator=None):
    """Return a ``MapperOption`` that will indicate to the query that
    the given attribute will be eagerly loaded.

    Used when feeding SQL result sets directly into
    ``query.instances()``.  Also bundles an ``EagerLazyOption`` to
    turn on eager loading in case it isnt already.

    `alias` is the string name of an alias, **or** an ``sql.Alias``
    object, which represents the aliased columns in the query.  This
    argument is optional.

    `decorator` is mutually exclusive of `alias` and is a
    row-processing function which will be applied to the incoming row
    before sending to the eager load handler.  use this for more
    sophisticated row adjustments beyond a straight alias.
    """

    return (strategies.EagerLazyOption(key, lazy=False), strategies.RowDecorateOption(key, alias=alias, decorator=decorator))

def defer(name):
    """Return a ``MapperOption`` that will convert the column property
    of the given name into a deferred load.

    Used with ``query.options()``"""
    return strategies.DeferredOption(name, defer=True)

def undefer(name):
    """Return a ``MapperOption`` that will convert the column property
    of the given name into a non-deferred (regular column) load.

    Used with ``query.options()``.
    """

    return strategies.DeferredOption(name, defer=False)


def cascade_mappers(*classes_or_mappers):
    """Attempt to create a series of ``relations()`` between mappers
    automatically, via introspecting the foreign key relationships of
    the underlying tables.

    Given a list of classes and/or mappers, identify the foreign key
    relationships between the given mappers or corresponding class
    mappers, and create ``relation()`` objects representing those
    relationships, including a backreference. Attempt to find the
    *secondary* table in a many-to-many relationship as well.

    The names of the relations will be a lowercase version of the
    related class.  In the case of one-to-many or many-to-many, the
    name will be *pluralized*, which currently is based on the English
    language (i.e. an 's' or 'es' added to it).

    NOTE: this method usually works poorly, and its usage is generally
    not advised.
    """

    table_to_mapper = {}
    for item in classes_or_mappers:
        if isinstance(item, Mapper):
            m = item
        else:
            klass = item
            m = class_mapper(klass)
        table_to_mapper[m.mapped_table] = m

    def pluralize(name):
        # oh crap, do we need locale stuff now
        if name[-1] == 's':
            return name + "es"
        else:
            return name + "s"

    for table,mapper in table_to_mapper.iteritems():
        for fk in table.foreign_keys:
            if fk.column.table is table:
                continue
            secondary = None
            try:
                m2 = table_to_mapper[fk.column.table]
            except KeyError:
                if len(fk.column.table.primary_key):
                    continue
                for sfk in fk.column.table.foreign_keys:
                    if sfk.column.table is table:
                        continue
                    m2 = table_to_mapper.get(sfk.column.table)
                    secondary = fk.column.table
            if m2 is None:
                continue
            if secondary:
                propname = pluralize(m2.class_.__name__.lower())
                propname2 = pluralize(mapper.class_.__name__.lower())
            else:
                propname = m2.class_.__name__.lower()
                propname2 = pluralize(mapper.class_.__name__.lower())
            mapper.add_property(propname, relation(m2, secondary=secondary, backref=propname2))
