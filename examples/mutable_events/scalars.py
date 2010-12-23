from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import event
import weakref

class TrackMutationsMixin(object):
    """Mixin that defines transparent propagation of change
    events to a parent object.
    
    """
    _key = None
    _parent = None
    
    def _set_parent(self, parent, key):
        self._parent = weakref.ref(parent)
        self._key = key
        
    def _remove_parent(self):
        del self._parent
        
    def on_change(self, key=None):
        """Subclasses should call this method whenever change events occur."""
        
        if key is None:
            key = self._key
        if self._parent:
            p = self._parent()
            if p:
                flag_modified(p, self._key)
    
    @classmethod
    def listen(cls, attribute):
        """Establish this type as a mutation listener for the given class and 
        attribute name.
        
        """
        key = attribute.key
        parent_cls = attribute.class_
        
        def on_load(state):
            val = state.dict.get(key, None)
            if val is not None:
                val = cls(val)
                state.dict[key] = val
                val._set_parent(state.obj(), key)

        def on_set(target, value, oldvalue, initiator):
            if not isinstance(value, cls):
                value = cls(value)
            value._set_parent(target.obj(), key)
            if isinstance(oldvalue, cls):
                oldvalue._remove_parent()
            return value
        
        event.listen(parent_cls, 'on_load', on_load, raw=True)
        event.listen(parent_cls, 'on_refresh', on_load, raw=True)
        event.listen(attribute, 'on_set', on_set, raw=True, retval=True)

if __name__ == '__main__':
    from sqlalchemy import Column, Integer, VARCHAR, create_engine
    from sqlalchemy.orm import Session
    from sqlalchemy.types import TypeDecorator
    from sqlalchemy.ext.declarative import declarative_base
    import simplejson

    class JSONEncodedDict(TypeDecorator):
        """Represents an immutable structure as a json-encoded string.
    
        Usage::
    
            JSONEncodedDict(255)
        
        """

        impl = VARCHAR

        def process_bind_param(self, value, dialect):
            if value is not None:
                value = simplejson.dumps(value, use_decimal=True)

            return value

        def process_result_value(self, value, dialect):
            if value is not None:
                value = simplejson.loads(value, use_decimal=True)
            return value

    class MutationDict(TrackMutationsMixin, dict):
        def __init__(self, other):
            self.update(other)
        
        def __setitem__(self, key, value):
            dict.__setitem__(self, key, value)
            self.on_change()
    
        def __delitem__(self, key):
            dict.__delitem__(self, key)
            self.on_change()

    Base = declarative_base()
    class Foo(Base):
        __tablename__ = 'foo'
        id = Column(Integer, primary_key=True)
        data = Column(JSONEncodedDict)

    MutationDict.listen(Foo.data)

    e = create_engine('sqlite://', echo=True)

    Base.metadata.create_all(e)

    sess = Session(e)
    f1 = Foo(data={'a':'b'})
    sess.add(f1)
    sess.commit()

    f1.data['a'] = 'c'
    sess.commit()

    assert f1.data == {'a':'c'}

        