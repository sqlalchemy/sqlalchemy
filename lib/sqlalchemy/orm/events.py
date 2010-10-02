"""ORM event interfaces.

"""
from sqlalchemy import event, util, exc

class InstrumentationEvents(event.Events):
    """Events related to class instrumentation events.
    
    The listeners here support being established against
    any new style class, that is any object that is a subclass
    of 'type'.  Events will then be fired off for events
    against that class as well as all subclasses.  
    'type' itself is also accepted as a target
    in which case the events fire for all classes.
    
    """
    
    @classmethod
    def accept_with(cls, target):
        from sqlalchemy.orm.instrumentation import instrumentation_registry
        
        if isinstance(target, type):
            return instrumentation_registry
        else:
            return None

    @classmethod
    def listen(cls, fn, identifier, target):
        
        @util.decorator
        def adapt_to_target(fn, cls, *arg):
            if issubclass(cls, target):
                fn(cls, *arg)
        event.Events.listen(fn, identifier, target)

    @classmethod
    def remove(cls, fn, identifier, target):
        raise NotImplementedError("Removal of instrumentation events not yet implemented")

    def on_class_instrument(self, cls):
        """Called after the given class is instrumented.
        
        To get at the :class:`.ClassManager`, use
        :func:`.manager_of_class`.
        
        """

    def on_class_uninstrument(self, cls):
        """Called before the given class is uninstrumented.
        
        To get at the :class:`.ClassManager`, use
        :func:`.manager_of_class`.
        
        """
        
        
    def on_attribute_instrument(self, cls, key, inst):
        """Called when an attribute is instrumented."""

class InstanceEvents(event.Events):
    
    @classmethod
    def accept_with(cls, target):
        from sqlalchemy.orm.instrumentation import ClassManager, manager_of_class
        
        if isinstance(target, ClassManager):
            return target
        elif isinstance(target, type):
            manager = manager_of_class(target)
            if manager:
                return manager
        return None
    
    @classmethod
    def listen(cls, fn, identifier, target, raw=False):
        if not raw:
            fn = _to_instance(fn)
        event.Events.listen(fn, identifier, target)

    @classmethod
    def remove(cls, fn, identifier, target):
        raise NotImplementedError("Removal of instance events not yet implemented")
        
    def on_init(self, target, args, kwargs):
        """"""
        
    def on_init_failure(self, target, args, kwargs):
        """"""
    
    def on_load(self, target):
        """"""
    
    def on_resurrect(self, target):
        """"""

        
class MapperEvents(event.Events):
    """"""
    @classmethod
    def remove(cls, fn, identifier, target):
        raise NotImplementedError("Removal of mapper events not yet implemented")
    
class SessionEvents(event.Events):
    """"""
    @classmethod
    def remove(cls, fn, identifier, target):
        raise NotImplementedError("Removal of session events not yet implemented")

class AttributeEvents(event.Events):
    """Define events for object attributes.

    e.g.::
    
        from sqlalchemy import event
        event.listen(my_append_listener, 'on_append', MyClass.collection)
        event.listen(my_set_listener, 'on_set', 
                                MyClass.somescalar, retval=True)
        
    Several modifiers are available to the listen() function.
    
    :param active_history=False: When True, indicates that the
      "on_set" event would like to receive the "old" value 
      being replaced unconditionally, even if this requires
      firing off database loads.
    :param propagate=False: When True, the listener function will
      be established not just for the class attribute given, but
      for attributes of the same name on all current subclasses 
      of that class, as well as all future subclasses of that 
      class, using an additional listener that listens for 
      instrumentation events.
    :param raw=False: When True, the "target" argument to the
      event will be the :class:`.InstanceState` management
      object, rather than the mapped instance itself.
    :param retval=False:` when True, the user-defined event 
      listening must return the "value" argument from the 
      function.  This gives the listening function the opportunity
      to change the value that is ultimately used for a "set"
      or "append" event.   
    
    """
    
    @classmethod
    def listen(cls, fn, identifier, target, active_history=False, 
                                        raw=False, retval=False,
                                        propagate=False):
        if active_history:
            target.dispatch.active_history = True
        
        # TODO: for removal, need to package the identity
        # of the wrapper with the original function.
        
        if raw is False or retval is False:
            @util.decorator
            def wrap(fn, target, value, *arg):
                if not raw:
                    target = target.obj()
                if not retval:
                    fn(target, value, *arg)
                    return value
                else:
                    return fn(target, value, *arg)
            fn = wrap(fn)
            
        event.Events.listen(fn, identifier, target)
        
        if propagate:
            # TODO: for removal, need to implement
            # packaging this info for operation in reverse.

            class_ = target.class_
            for cls in class_.__subclasses__():
                impl = getattr(cls, target.key)
                if impl is not target:
                    event.Events.listen(fn, identifier, impl)
            
            def configure_listener(class_, key, inst):
                event.Events.listen(fn, identifier, inst)
            event.listen(configure_listener, 'on_attribute_instrument', class_)
        
    @classmethod
    def remove(cls, fn, identifier, target):
        raise NotImplementedError("Removal of attribute events not yet implemented")
        
    @classmethod
    def unwrap(cls, identifier, event):
        return event['value']
        
    def on_append(self, target, value, initiator):
        """Receive a collection append event.

        :param target: the object instance receiving the event.
          If the listener is registered with ``raw=True``, this will
          be the :class:`.InstanceState` object.
        :param value: the value being appended.  If this listener
          is registered with ``retval=True``, the listener
          function must return this value, or a new value which 
          replaces it.
        :param initiator: the attribute implementation object 
          which initiated this event.

        """

    def on_remove(self, target, value, initiator):
        """Receive a collection remove event.

        :param target: the object instance receiving the event.
          If the listener is registered with ``raw=True``, this will
          be the :class:`.InstanceState` object.
        :param value: the value being removed.
        :param initiator: the attribute implementation object 
          which initiated this event.

        """

    def on_set(self, target, value, oldvalue, initiator):
        """Receive a scalar set event.

        :param target: the object instance receiving the event.
          If the listener is registered with ``raw=True``, this will
          be the :class:`.InstanceState` object.
        :param value: the value being set.  If this listener
          is registered with ``retval=True``, the listener
          function must return this value, or a new value which 
          replaces it.
        :param oldvalue: the previous value being replaced.  This
          may also be the symbol ``NEVER_SET`` or ``NO_VALUE``.
          If the listener is registered with ``active_history=True``,
          the previous value of the attribute will be loaded from
          the database if the existing value is currently unloaded 
          or expired.
        :param initiator: the attribute implementation object 
          which initiated this event.

        """

@util.decorator
def _to_instance(fn, state, *arg, **kw):
    """Marshall the :class:`.InstanceState` argument to an instance."""
    
    return fn(state.obj(), *arg, **kw)
    
