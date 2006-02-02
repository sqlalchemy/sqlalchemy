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
import types as types
from mapper import *
from properties import *
import mapper as mapperlib

__all__ = ['relation', 'eagerload', 'lazyload', 'noload', 'deferred',  'column', 
        'defer', 'undefer',
        'mapper', 'clear_mappers', 'objectstore', 'sql', 'extension', 'class_mapper', 'object_mapper', 'MapperExtension',
        'assign_mapper'
        ]

def relation(*args, **kwargs):
    """provides a relationship of a primary Mapper to a secondary Mapper, which corresponds
    to a parent-child or associative table relationship."""
    if len(args) > 1 and isinstance(args[0], type):
        raise ValueError("relation(class, table, **kwargs) is deprecated.  Please use relation(class, **kwargs) or relation(mapper, **kwargs).")
    return _relation_loader(*args, **kwargs)

def _relation_loader(mapper, secondary=None, primaryjoin=None, secondaryjoin=None, lazy=True, **kwargs):
    if lazy:
        return LazyLoader(mapper, secondary, primaryjoin, secondaryjoin, **kwargs)
    elif lazy is None:
        return PropertyLoader(mapper, secondary, primaryjoin, secondaryjoin, **kwargs)
    else:
        return EagerLoader(mapper, secondary, primaryjoin, secondaryjoin, **kwargs)

def column(*columns, **kwargs):
    return ColumnProperty(*columns, **kwargs)
    
def deferred(*columns, **kwargs):
    return DeferredColumnProperty(*columns, **kwargs)
    
def mapper(class_, table = None, engine = None, autoload = False, *args, **params):
    """returns a new or already cached Mapper object."""
    if table is None:
        return class_mapper(class_)

    if isinstance(table, str):
        table = schema.Table(table, engine, autoload = autoload, mustexist = not autoload)
            
    hashkey = mapper_hash_key(class_, table, *args, **params)
    #print "HASHKEY: " + hashkey
    try:
        return mapper_registry[hashkey]
    except KeyError:
        m = Mapper(hashkey, class_, table, *args, **params)
        mapper_registry.setdefault(hashkey, m)
        m._init_properties()
        return mapper_registry[hashkey]

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
    
def object_mapper(object):
    """given an object, returns the primary Mapper associated with the object
    or the object's class."""
    return class_mapper(object.__class__)

def class_mapper(class_):
    """given a class, returns the primary Mapper associated with the class."""
    try:
        return mapper_registry[class_._mapper]
    except KeyError:
        pass
    except AttributeError:
        pass
        raise "Class '%s' has no mapper associated with it" % class_.__name__

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