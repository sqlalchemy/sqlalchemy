# this example is probably moving to be an extension.

from sqlalchemy import event
from sqlalchemy.orm import mapper, composite, object_mapper

from sqlalchemy.util import memoized_property
import weakref

class _CompositeMutationsMixinMeta(type):
    def __init__(cls, classname, bases, dict_):
        cls._setup_listeners()
        return type.__init__(cls, classname, bases, dict_)

class CompositeMutationsMixin(object):
    """Mixin that defines transparent propagation of change
    events to a parent object.

    This class might be moved to be a SQLA extension
    due to its complexity and potential for widespread use.
    
    """
    __metaclass__ = _CompositeMutationsMixinMeta

    @memoized_property
    def _parents(self):
        """Dictionary of parent object->attribute name on the parent."""
        
        return weakref.WeakKeyDictionary()

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        self.on_change()
    
    def on_change(self):
        """Subclasses should call this method whenever change events occur."""
        
        for parent, key in self._parents.items():
            
            prop = object_mapper(parent).get_property(key)
            for value, attr_name in zip(self.__composite_values__(), prop._attribute_keys):
                setattr(parent, attr_name, value)
    
    @classmethod
    def _listen_on_attribute(cls, attribute):
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
                val._parents[state.obj()] = key

        def on_set(target, value, oldvalue, initiator):
            """Listen for set/replace events on the target
            data member.
            
            Establish a weak reference to the parent object
            on the incoming value, remove it for the one 
            outgoing.
            
            """
            
            value._parents[target.obj()] = key
            if isinstance(oldvalue, cls):
                oldvalue._parents.pop(state.obj(), None)
            return value
        
        event.listen(parent_cls, 'on_load', on_load, raw=True)
        event.listen(parent_cls, 'on_refresh', on_load, raw=True)
        event.listen(attribute, 'on_set', on_set, raw=True, retval=True)
    
    @classmethod
    def _setup_listeners(cls):
        """Associate this wrapper with all future mapped compoistes
        of the given type.
        
        This is a convenience method that calls ``associate_with_attribute`` automatically.
        
        """
        
        def listen_for_type(mapper, class_):
            for prop in mapper.iterate_properties:
                if hasattr(prop, 'composite_class') and issubclass(prop.composite_class, cls):
                    cls._listen_on_attribute(getattr(class_, prop.key))
                    
        event.listen(mapper, 'on_mapper_configured', listen_for_type)

        
if __name__ == '__main__':
    from sqlalchemy import Column, Integer, create_engine
    from sqlalchemy.orm import Session
    from sqlalchemy.ext.declarative import declarative_base

    class Point(CompositeMutationsMixin):
        def __init__(self, x, y):
            self.x = x
            self.y = y
        
        def __composite_values__(self):
            return self.x, self.y
            
        def __eq__(self, other):
            return isinstance(other, Point) and \
                other.x == self.x and \
                other.y == self.y
    
    Base = declarative_base()
    class Foo(Base):
        __tablename__ = 'foo'
        id = Column(Integer, primary_key=True)
        data = composite(Point, Column('x', Integer), Column('y', Integer))
    
    e = create_engine('sqlite://', echo=True)

    Base.metadata.create_all(e)

    sess = Session(e)
    d = Point(3, 4)
    f1 = Foo(data=d)
    sess.add(f1)
    sess.commit()

    f1.data.y = 5
    sess.commit()

    assert f1.data == Point(3, 5)

        