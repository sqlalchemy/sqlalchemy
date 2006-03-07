# mapper/__init__.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
the mapper package provides object-relational functionality, building upon the schema and sql
packages and tying operations to class properties and constructors.
"""
import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.engine as engine
import sqlalchemy.util as util
import objectstore
from exceptions import *
import types as types
from mapper import *
from properties import *
import mapper as mapperlib

__all__ = ['relation', 'backref', 'eagerload', 'lazyload', 'noload', 'deferred', 'defer', 'undefer',
        'mapper', 'clear_mappers', 'objectstore', 'sql', 'extension', 'class_mapper', 'object_mapper', 'MapperExtension',
        'assign_mapper', 'cascade_mappers'
        ]

def relation(*args, **kwargs):
    """provides a relationship of a primary Mapper to a secondary Mapper, which corresponds
    to a parent-child or associative table relationship."""
    if len(args) > 1 and isinstance(args[0], type):
        raise ArgumentError("relation(class, table, **kwargs) is deprecated.  Please use relation(class, **kwargs) or relation(mapper, **kwargs).")
    return _relation_loader(*args, **kwargs)

def _relation_loader(mapper, secondary=None, primaryjoin=None, secondaryjoin=None, lazy=True, **kwargs):
    if lazy:
        return LazyLoader(mapper, secondary, primaryjoin, secondaryjoin, **kwargs)
    elif lazy is None:
        return PropertyLoader(mapper, secondary, primaryjoin, secondaryjoin, **kwargs)
    else:
        return EagerLoader(mapper, secondary, primaryjoin, secondaryjoin, **kwargs)

def backref(name, **kwargs):
    return BackRef(name, **kwargs)
    
def deferred(*columns, **kwargs):
    """returns a DeferredColumnProperty, which indicates this object attributes should only be loaded 
    from its corresponding table column when first accessed."""
    return DeferredColumnProperty(*columns, **kwargs)
    
def mapper(class_, table=None, *args, **params):
    """returns a new or already cached Mapper object."""
    if table is None:
        return class_mapper(class_)

    return Mapper(class_, table, *args, **params)

def clear_mappers():
    """removes all mappers that have been created thus far.  when new mappers are 
    created, they will be assigned to their classes as their primary mapper."""
    mapper_registry.clear()

def clear_mapper(m):
    """removes the given mapper from the storage of mappers.  when a new mapper is 
    created for the previous mapper's class, it will be used as that classes' 
    new primary mapper."""
    del mapper_registry[m.hash_key]

def extension(ext):
    """returns a MapperOption that will add the given MapperExtension to the 
    mapper returned by mapper.options()."""
    return ExtensionOption(ext)
def eagerload(name, **kwargs):
    """returns a MapperOption that will convert the property of the given name
    into an eager load.  Used with mapper.options()"""
    return EagerLazyOption(name, toeager=True, **kwargs)

def lazyload(name, **kwargs):
    """returns a MapperOption that will convert the property of the given name
    into a lazy load.  Used with mapper.options()"""
    return EagerLazyOption(name, toeager=False, **kwargs)

def noload(name, **kwargs):
    """returns a MapperOption that will convert the property of the given name
    into a non-load.  Used with mapper.options()"""
    return EagerLazyOption(name, toeager=None, **kwargs)

def defer(name, **kwargs):
    """returns a MapperOption that will convert the column property of the given 
    name into a deferred load.  Used with mapper.options()"""
    return DeferredOption(name, defer=True)
def undefer(name, **kwargs):
    """returns a MapperOption that will convert the column property of the given
    name into a non-deferred (regular column) load.  Used with mapper.options."""
    return DeferredOption(name, defer=False)
    


def assign_mapper(class_, *args, **params):
    params.setdefault("is_primary", True)
    if not isinstance(getattr(class_, '__init__'), types.MethodType):
        def __init__(self, **kwargs):
             for key, value in kwargs.items():
                 setattr(self, key, value)
        class_.__init__ = __init__
    m = mapper(class_, *args, **params)
    class_.mapper = m
    class_.get = m.get
    class_.select = m.select
    class_.select_by = m.select_by
    class_.selectone = m.selectone
    class_.get_by = m.get_by
    def commit(self):
        objectstore.commit(self)
    def delete(self):
        objectstore.delete(self)
    class_.commit = commit
    class_.delete = delete
    
def cascade_mappers(*classes_or_mappers):
    """given a list of classes and/or mappers, identifies the foreign key relationships
    between the given mappers or corresponding class mappers, and creates relation()
    objects representing those relationships, including a backreference.  Attempts to find
    the "secondary" table in a many-to-many relationship as well.  The names of the relations
    will be a lowercase version of the related class.  In the case of one-to-many or many-to-many,
    the name will be "pluralized", which currently is based on the English language (i.e. an 's' or 
    'es' added to it)."""
    table_to_mapper = {}
    for item in classes_or_mappers:
        if isinstance(item, Mapper):
            m = item
        else:
            klass = item
            m = class_mapper(klass)
        table_to_mapper[m.table] = m
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
            
            