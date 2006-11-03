# mapper/__init__.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
the mapper package provides object-relational functionality, building upon the schema and sql
packages and tying operations to class properties and constructors.
"""
from sqlalchemy import exceptions
from sqlalchemy.orm.mapper import *
from sqlalchemy.orm import mapper as mapperlib
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.util import polymorphic_union
from sqlalchemy.orm import properties, strategies
from sqlalchemy.orm.session import Session as create_session

__all__ = ['relation', 'backref', 'eagerload', 'lazyload', 'noload', 'deferred', 'defer', 'undefer', 'extension', 
        'mapper', 'clear_mappers', 'clear_mapper', 'class_mapper', 'object_mapper', 'MapperExtension', 'Query', 
        'cascade_mappers', 'polymorphic_union', 'create_session', 'synonym', 'contains_eager', 'EXT_PASS'
        ]

def relation(*args, **kwargs):
    """provide a relationship of a primary Mapper to a secondary Mapper.
    
    This corresponds to a parent-child or associative table relationship."""
    if len(args) > 1 and isinstance(args[0], type):
        raise exceptions.ArgumentError("relation(class, table, **kwargs) is deprecated.  Please use relation(class, **kwargs) or relation(mapper, **kwargs).")
    return _relation_loader(*args, **kwargs)

def _relation_loader(mapper, secondary=None, primaryjoin=None, secondaryjoin=None, lazy=True, **kwargs):
    return properties.PropertyLoader(mapper, secondary, primaryjoin, secondaryjoin, lazy=lazy, **kwargs)

def backref(name, **kwargs):
    """create a BackRef object with explicit arguments, which are the same arguments one
    can send to relation().  
    
    used with the "backref" keyword argument to relation() in place
    of a string argument. """
    return properties.BackRef(name, **kwargs)
    
def deferred(*columns, **kwargs):
    """return a DeferredColumnProperty, which indicates this object attributes should only be loaded 
    from its corresponding table column when first accessed.  
    
    used with the 'properties' dictionary sent to mapper()."""
    return properties.ColumnProperty(deferred=True, *columns, **kwargs)
    
def mapper(class_, table=None, *args, **params):
    """return a new Mapper object.
    
    See the Mapper class for a description of arguments."""
    return Mapper(class_, table, *args, **params)

def synonym(name, proxy=False):
    """set up 'name' as a synonym to another MapperProperty.  
    
    Used with the 'properties' dictionary sent to mapper()."""
    return properties.SynonymProperty(name, proxy=proxy)
    
def clear_mappers():
    """remove all mappers that have been created thus far.  
    
    when new mappers are created, they will be assigned to their classes as their primary mapper."""
    mapper_registry.clear()
    
def clear_mapper(m):
    """remove the given mapper from the storage of mappers.  
    
    when a new mapper is created for the previous mapper's class, it will be used as that classes' 
    new primary mapper."""
    del mapper_registry[m.class_key]

def extension(ext):
    """return a MapperOption that will insert the given MapperExtension to the 
    beginning of the list of extensions that will be called in the context of the Query.
    
    used with query.options()."""
    return ExtensionOption(ext)
    
def eagerload(name):
    """return a MapperOption that will convert the property of the given name
    into an eager load.  
    
    used with query.options()."""
    return strategies.EagerLazyOption(name, lazy=False)

def lazyload(name):
    """return a MapperOption that will convert the property of the given name
    into a lazy load.
    
    used with query.options()."""
    return strategies.EagerLazyOption(name, lazy=True)

def noload(name):
    """return a MapperOption that will convert the property of the given name
    into a non-load.  
    
    used with query.options()."""
    return strategies.EagerLazyOption(name, lazy=None)

def contains_eager(key, decorator=None):
    """return a MapperOption that will indicate to the query that the given 
    attribute will be eagerly loaded without any row decoration, or using
    a custom row decorator.  
    
    used when feeding SQL result sets directly into
    query.instances()."""
    return strategies.RowDecorateOption(key, decorator=decorator)
    
def defer(name):
    """return a MapperOption that will convert the column property of the given 
    name into a deferred load.  
    
    used with query.options()"""
    return strategies.DeferredOption(name, defer=True)
def undefer(name):
    """return a MapperOption that will convert the column property of the given
    name into a non-deferred (regular column) load.  
    
    used with query.options()."""
    return strategies.DeferredOption(name, defer=False)
    

def cascade_mappers(*classes_or_mappers):
    """attempt to create a series of relations() between mappers automatically, via
    introspecting the foreign key relationships of the underlying tables.
    
    given a list of classes and/or mappers, identifies the foreign key relationships
    between the given mappers or corresponding class mappers, and creates relation()
    objects representing those relationships, including a backreference.  Attempts to find
    the "secondary" table in a many-to-many relationship as well.  The names of the relations
    will be a lowercase version of the related class.  In the case of one-to-many or many-to-many,
    the name will be "pluralized", which currently is based on the English language (i.e. an 's' or 
    'es' added to it).
    
    NOTE: this method usually works poorly, and its usage is generally not advised.
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
            
            