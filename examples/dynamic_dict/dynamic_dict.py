"""Illustrates how to place a dictionary-like facade on top of a dynamic_loader, so
that dictionary operations (assuming simple string keys) can operate upon a large 
collection without loading the full collection at once.

This is something that may eventually be added as a feature to dynamic_loader() itself.

Similar approaches could be taken towards sets and dictionaries with non-string keys 
although the hash policy of the members would need to be distilled into a filter() criterion.

"""

class MyProxyDict(object):
    def __init__(self, parent, collection_name, childclass, keyname):
        self.parent = parent
        self.collection_name = collection_name
        self.childclass = childclass
        self.keyname = keyname
        
    def collection(self):
        return getattr(self.parent, self.collection_name)
    collection = property(collection)
    
    def keys(self):
        descriptor = getattr(self.childclass, self.keyname)
        return [x[0] for x in self.collection.values(descriptor)]
        
    def __getitem__(self, key):
        x = self.collection.filter_by(**{self.keyname:key}).first()
        if x:
            return x
        else:
            raise KeyError(key)

    def __setitem__(self, key, value):
        try:
            existing = self[key]
            self.collection.remove(existing)
        except KeyError:
            pass
        self.collection.append(value)

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, dynamic_loader

Base = declarative_base(engine=create_engine('sqlite://'))

class MyParent(Base):
    __tablename__ = 'parent'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    _collection = dynamic_loader("MyChild", cascade="all, delete-orphan")
    
    def child_map(self):
        return MyProxyDict(self, '_collection', MyChild, 'key')
    child_map = property(child_map)
    
class MyChild(Base):
    __tablename__ = 'child'
    id = Column(Integer, primary_key=True)
    key = Column(String(50))
    parent_id = Column(Integer, ForeignKey('parent.id'))

    
Base.metadata.create_all()

sess = sessionmaker()()

p1 = MyParent(name='p1')
sess.add(p1)

p1.child_map['k1'] = k1 = MyChild(key='k1')
p1.child_map['k2'] = k2 = MyChild(key='k2')

assert p1.child_map.keys() == ['k1', 'k2']

assert p1.child_map['k1'] is k1

p1.child_map['k2'] = k2b = MyChild(key='k2')
assert p1.child_map['k2'] is k2b

assert sess.query(MyChild).all() == [k1, k2b]

