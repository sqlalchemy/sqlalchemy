# objectstore.py
# Copyright (C) 2005 Michael Bayer mike_mp@zzzcomputing.com
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.


"""maintains all currently loaded objects in memory,
using the "identity map" pattern.  Also provides a "unit of work" object which tracks changes
to objects so that they may be properly persisted within a transactional scope."""

import thread
import sqlalchemy.util as util
import weakref

def get_id_key(ident, class_, table, selectable):
    """returns an identity-map key for use in storing/retrieving an item from the identity map, given
    a tuple of the object's primary key values.
    
    ident - a tuple of primary key values corresponding to the object to be stored.  these values
    should be in the same order as the primary keys of the table
    class_ - a reference to the object's class
    table - a Table object where the object's primary fields are stored.
    selectable - a Selectable object which represents all the object's column-based fields.  this Selectable
    may be synonymous with the table argument or can be a larger construct containing that table.
    return value: a tuple object which is used as an identity key.
    """
    return (class_, table, tuple(ident))
def get_instance_key(object, class_, table, selectable):
    """returns an identity-map key for use in storing/retrieving an item from the identity map, given
    the object instance itself.
    
    object - the object to be stored.  it is assumed that the object's primary key attributes are
    populated.
    class_ - a reference to the object's class
    table - a Table object where the object's primary fields are stored.
    selectable - a Selectable object which represents all the object's column-based fields.  this Selectable
    may be synonymous with the table argument or can be a larger construct containing that table.
    return value: a tuple object which is used as an identity key.
    """
    return (class_, table, tuple([getattr(object, column.key, None) for column in selectable.primary_keys]))
def get_row_key(row, class_, table, selectable):
    """returns an identity-map key for use in storing/retrieving an item from the identity map, given
    a result set row.
    
    row - a sqlalchemy.dbengine.RowProxy instance or other map corresponding result-set column
    names to their values within a row.
    class_ - a reference to the object's class
    table - a Table object where the object's primary fields are stored.
    selectable - a Selectable object which represents all the object's column-based fields.  this Selectable
    may be synonymous with the table argument or can be a larger construct containing that table.
    return value: a tuple object which is used as an identity key.
    """
    return (class_, table, tuple([row[column.label] for column in selectable.primary_keys]))

identity_map = {}

def get(key):
    val = identity_map[key]
    if isinstance(val, dict):
        return val[thread.get_ident()]
    else:
        return val
    
def put(key, obj, scope='thread'):

    if isinstance(obj, dict):
        raise "cant put a dict in the object store"
        
    if scope == 'thread':
        try:
            d = identity_map[key]
        except KeyError:
            d = identity_map.setdefault(key, {})
        d[thread.get_ident()] = obj
    else:
        identity_map[key] = obj

def clear(scope='thread'):
    if scope == 'thread':
        for k in identity_map.keys():
            if isinstance(identity_map[k], dict):
                identity_map[k].clear()
    else:
        for k in identity_map.keys():
            if not isinstance(identity_map[k], dict):
                del identity_map[k]
            
def has_key(key):
    if identity_map.has_key(key):
        d = identity_map[key]
        if isinstance(d, dict):
            return d.has_key(thread.get_ident())
        else:
            return True
    else:
        return False
    
class UnitOfWork:
    def __init__(self):
        self.dirty = util.HashSet()
        self.clean = util.HashSet()
        self.deleted = util.HashSet()
        self.attribute_history = weakref.WeakKeyDictionary()
        
    def attribute_set(self, obj, key, value):
        self.register_attribute(obj, key).setattr(value)    
    def attribute_deleted(self, obj, key, value):
        self.register_attribute(obj, key).delattr(value)    
    
    def register_attribute(self, obj, key):
        try:
            attributes = self.attribute_history[obj]
        except KeyError:
            attributes = self.attribute_history.setdefault(obj, {})
        try:
            return attributes[key]
        except KeyError:
            return attributes.setdefault(key, util.PropHistory(obj.__dict__.get(key, None)))
        
    def register_clean(self, obj):
        self.clean.append(obj)
        try:
            del self.dirty[obj]
        except KeyError:
            pass

    def register_new(self, obj):
        pass
        
    def register_dirty(self, obj):
        self.dirty.append(obj)
        try:
            del self.clean[obj]
        except KeyError:
            pass

    def is_dirty(self, obj):
        return self.dirty.contains(obj)
        
    def register_deleted(self, obj):
        pass   
        
    def commit(self):
        for item in self.dirty:
            self.clean.append(item)
        self.dirty.clear()
        
uow = util.ScopedRegistry(lambda: UnitOfWork(), "thread")        