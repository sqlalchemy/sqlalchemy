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

import thread

def get_id_key(ident, class_, table, selectable):
    return (class_, table, tuple(ident))
def get_instance_key(object, class_, table, selectable):
    return (class_, table, tuple([getattr(object, column.key, None) for column in selectable.primary_keys]))
def get_row_key(row, class_, table, selectable):
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
        pass
        
    def register_clean(self, obj):
        pass

    def register_new(self, obj):
        pass
        
    def register_dirty(self, obj):
        pass
        
    def register_deleted(self, obj):
        pass   
        
        
        