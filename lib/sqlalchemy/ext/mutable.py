"""Provide support for tracking of in-place changes to scalar values,
which are propagated to owning parent objects.

The ``mutable`` extension is a replacement for the :class:`.types.MutableType`
class as well as the ``mutable=True`` flag available on types which subclass
it.


"""
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import event, types
from sqlalchemy.orm import mapper, object_mapper
from sqlalchemy.util import memoized_property
import weakref

class Mutable(object):
    """Mixin that defines transparent propagation of change
    events to a parent object.
    
    """
    
    @memoized_property
    def _parents(self):
        """Dictionary of parent object->attribute name on the parent."""
        
        return weakref.WeakKeyDictionary()
        
    def change(self):
        """Subclasses should call this method whenever change events occur."""
        
        for parent, key in self._parents.items():
            flag_modified(parent, key)
    
    @classmethod
    def coerce(cls, key, value):
        """Given a value, coerce it into this type.
        
        By default raises ValueError.
        """
        if value is None:
            return None
        raise ValueError("Attribute '%s' accepts objects of type %s" % (key, cls))
        
        
    @classmethod
    def associate_with_attribute(cls, attribute):
        """Establish this type as a mutation listener for the given 
        mapped descriptor.
        
        """
        key = attribute.key
        parent_cls = attribute.class_
        
        def load(state, *args):
            """Listen for objects loaded or refreshed.   
            
            Wrap the target data member's value with 
            ``Mutable``.
            
            """
            val = state.dict.get(key, None)
            if val is not None:
                val = cls.coerce(key, val)
                state.dict[key] = val
                val._parents[state.obj()] = key

        def set(target, value, oldvalue, initiator):
            """Listen for set/replace events on the target
            data member.
            
            Establish a weak reference to the parent object
            on the incoming value, remove it for the one 
            outgoing.
            
            """
            
            if not isinstance(value, cls):
                value = cls.coerce(key, value) 
            value._parents[target.obj()] = key
            if isinstance(oldvalue, cls):
                oldvalue._parents.pop(state.obj(), None)
            return value
        
        event.listen(parent_cls, 'load', load, raw=True)
        event.listen(parent_cls, 'refresh', load, raw=True)
        event.listen(attribute, 'set', set, raw=True, retval=True)

        # TODO: need a deserialize hook here

    @classmethod
    def associate_with(cls, sqltype):
        """Associate this wrapper with all future mapped columns 
        of the given type.
        
        This is a convenience method that calls ``associate_with_attribute`` automatically.

        .. warning:: The listeners established by this method are *global*
           to all mappers, and are *not* garbage collected.   Only use 
           :meth:`.associate_with` for types that are permanent to an application,
           not with ad-hoc types else this will cause unbounded growth
           in memory usage.
        
        """

        def listen_for_type(mapper, class_):
            for prop in mapper.iterate_properties:
                if hasattr(prop, 'columns'):
                    if isinstance(prop.columns[0].type, sqltype):
                        cls.associate_with_attribute(getattr(class_, prop.key))
                        break
                    
        event.listen(mapper, 'mapper_configured', listen_for_type)
    
    @classmethod
    def as_mutable(cls, sqltype):
        """Associate a SQL type with this mutable Python type.
    
        This establishes listeners that will detect ORM mappings against
        the given type, adding mutation event trackers to those mappings.
    
        The type is returned, unconditionally as an instance, so that 
        :meth:`.as_mutable` can be used inline::
    
            Table('mytable', metadata,
                Column('id', Integer, primary_key=True),
                Column('data', MyMutableType.as_mutable(PickleType))
            )
        
        Note that the returned type is always an instance, even if a class
        is given, and that only columns which are declared specifically with that
        type instance receive additional instrumentation.
    
        To associate a particular mutable type with all occurences of a 
        particular type, use the :meth:`.Mutable.associate_with` classmethod
        of the particular :meth:`.Mutable` subclass to establish a global
        assoiation.
    
        .. warning:: The listeners established by this method are *global*
           to all mappers, and are *not* garbage collected.   Only use 
           :meth:`.as_mutable` for types that are permanent to an application,
           not with ad-hoc types else this will cause unbounded growth
           in memory usage.
    
        """
        sqltype = types.to_instance(sqltype)

        def listen_for_type(mapper, class_):
            for prop in mapper.iterate_properties:
                if hasattr(prop, 'columns'):
                    if prop.columns[0].type is sqltype:
                        cls.associate_with_attribute(getattr(class_, prop.key))
                        break
                
        event.listen(mapper, 'mapper_configured', listen_for_type)
        
        return sqltype


class _MutableCompositeMeta(type):
    def __init__(cls, classname, bases, dict_):
        cls._setup_listeners()
        return type.__init__(cls, classname, bases, dict_)

class MutableComposite(object):
    """Mixin that defines transparent propagation of change
    events on a SQLAlchemy "composite" object to its
    owning parent or parents.
    
    Composite classes, in addition to meeting the usage contract
    defined in :ref:`mapper_composite`, also define some system
    of relaying change events to the given :meth:`.change` 
    method, which will notify all parents of the change.  Below
    the special Python method ``__setattr__`` is used to intercept
    all changes::
    
        class Point(MutableComposite):
            def __init__(self, x, y):
                self.x = x
                self.y = y

            def __setattr__(self, key, value):
                object.__setattr__(self, key, value)
                self.change()
        
            def __composite_values__(self):
                return self.x, self.y
            
            def __eq__(self, other):
                return isinstance(other, Point) and \
                    other.x == self.x and \
                    other.y == self.y

    :class:`.MutableComposite` defines a metaclass which augments
    the creation of :class:`.MutableComposite` subclasses with an event
    that will listen for any :func:`~.orm.composite` mappings against the 
    new type, establishing listeners that will track parent associations.

    .. warning:: The listeners established by the :class:`.MutableComposite`
       class are *global* to all mappers, and are *not* garbage collected.   Only use 
       :class:`.MutableComposite` for types that are permanent to an application,
       not with ad-hoc types else this will cause unbounded growth
       in memory usage.
    
    """
    __metaclass__ = _MutableCompositeMeta

    @memoized_property
    def _parents(self):
        """Dictionary of parent object->attribute name on the parent."""
        
        return weakref.WeakKeyDictionary()

    def change(self):
        """Subclasses should call this method whenever change events occur."""
        
        for parent, key in self._parents.items():
            
            prop = object_mapper(parent).get_property(key)
            for value, attr_name in zip(
                                    self.__composite_values__(), 
                                    prop._attribute_keys):
                setattr(parent, attr_name, value)
    
    @classmethod
    def _listen_on_attribute(cls, attribute):
        """Establish this type as a mutation listener for the given 
        mapped descriptor.
        
        """
        key = attribute.key
        parent_cls = attribute.class_
        
        def load(state, *args):
            """Listen for objects loaded or refreshed.   
            
            Wrap the target data member's value with 
            ``Mutable``.
            
            """
            
            val = state.dict.get(key, None)
            if val is not None:
                val._parents[state.obj()] = key

        def set(target, value, oldvalue, initiator):
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
        
        event.listen(parent_cls, 'load', load, raw=True)
        event.listen(parent_cls, 'refresh', load, raw=True)
        event.listen(attribute, 'set', set, raw=True, retval=True)

        # TODO: need a deserialize hook here
    
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
                    
        event.listen(mapper, 'mapper_configured', listen_for_type)

