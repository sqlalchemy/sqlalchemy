from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import event
from sqlalchemy.orm import mapper
from sqlalchemy.util import memoized_property
import weakref

class TrackMutationsMixin(object):
    """Mixin that defines transparent propagation of change
    events to a parent object.
    
    """
    @memoized_property
    def _parents(self):
        """Dictionary of parent object->attribute name on the parent."""
        
        return weakref.WeakKeyDictionary()
        
    def on_change(self):
        """Subclasses should call this method whenever change events occur."""
        
        for parent, key in self._parents.items():
            flag_modified(parent, key)
    
    @classmethod
    def associate_with_attribute(cls, attribute):
        """Establish this type as a mutation listener for the given 
        mapped descriptor.
        
        """
        key = attribute.key
        parent_cls = attribute.class_
        
        def on_load(state):
            """Listen for objects loaded or refreshed.   
            
            Wrap the target data member's value with 
            ``TrackMutationsMixin``.
            
            """
            val = state.dict.get(key, None)
            if val is not None:
                val = cls(val)
                state.dict[key] = val
                val._parents[state.obj()] = key

        def on_set(target, value, oldvalue, initiator):
            """Listen for set/replace events on the target
            data member.
            
            Establish a weak reference to the parent object
            on the incoming value, remove it for the one 
            outgoing.
            
            """
            
            if not isinstance(value, cls):
                value = cls(value)
            value._parents[target.obj()] = key
            if isinstance(oldvalue, cls):
                oldvalue._parents.pop(state.obj(), None)
            return value
        
        event.listen(parent_cls, 'on_load', on_load, raw=True)
        event.listen(parent_cls, 'on_refresh', on_load, raw=True)
        event.listen(attribute, 'on_set', on_set, raw=True, retval=True)
    
    @classmethod
    def associate_with_type(cls, type_):
        """Associate this wrapper with all future mapped columns 
        of the given type.
        
        This is a convenience method that calls ``associate_with_attribute`` automatically.
        
        """
        
        def listen_for_type(mapper, class_):
            for prop in mapper.iterate_properties:
                if hasattr(prop, 'columns') and isinstance(prop.columns[0].type, type_):
                    cls.listen(getattr(class_, prop.key))
                    
        event.listen(mapper, 'on_mapper_configured', listen_for_type)
        
        
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
            
    MutationDict.associate_with_type(JSONEncodedDict)
    
    Base = declarative_base()
    class Foo(Base):
        __tablename__ = 'foo'
        id = Column(Integer, primary_key=True)
        data = Column(JSONEncodedDict)
    
    e = create_engine('sqlite://', echo=True)

    Base.metadata.create_all(e)

    sess = Session(e)
    f1 = Foo(data={'a':'b'})
    sess.add(f1)
    sess.commit()

    f1.data['a'] = 'c'
    sess.commit()

    assert f1.data == {'a':'c'}

        