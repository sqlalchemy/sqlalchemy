"""ORM event interfaces.

"""
from sqlalchemy import event, exc
import inspect

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
    def listen(cls, fn, identifier, target, propagate=False):
        event.Events.listen(fn, identifier, target, propagate=propagate)

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
    """Define events specific to object lifecycle.
    
    Instance-level don't automatically propagate their associations
    to subclasses.
    
    """
    @classmethod
    def accept_with(cls, target):
        from sqlalchemy.orm.instrumentation import ClassManager, manager_of_class
        from sqlalchemy.orm import Mapper, mapper
        
        if isinstance(target, ClassManager):
            return target
        elif isinstance(target, Mapper):
            return target.class_manager
        elif target is mapper:
            return ClassManager
        elif isinstance(target, type):
            if issubclass(target, Mapper):
                return ClassManager
            else:
                manager = manager_of_class(target)
                if manager:
                    return manager
        return None
    
    @classmethod
    def listen(cls, fn, identifier, target, raw=False, propagate=False):
        if not raw:
            orig_fn = fn
            def wrap(state, *arg, **kw):
                return orig_fn(state.obj(), *arg, **kw)
            fn = wrap

        event.Events.listen(fn, identifier, target, propagate=propagate)
        if propagate:
            for mgr in target.subclass_managers(True):
                event.Events.listen(fn, identifier, mgr, True)
            
    @classmethod
    def remove(cls, fn, identifier, target):
        raise NotImplementedError("Removal of instance events not yet implemented")
        
    def on_init(self, target, args, kwargs):
        """Receive an instance when it's constructor is called.
        
        This method is only called during a userland construction of 
        an object.  It is not called when an object is loaded from the
        database.

        """
        
    def on_init_failure(self, target, args, kwargs):
        """Receive an instance when it's constructor has been called, 
        and raised an exception.
        
        This method is only called during a userland construction of 
        an object.  It is not called when an object is loaded from the
        database.

        """
    
    def on_load(self, target):
        """Receive an object instance after it has been created via
        ``__new__``, and after initial attribute population has
        occurred.

        This typically occurs when the instance is created based on
        incoming result rows, and is only called once for that
        instance's lifetime.

        Note that during a result-row load, this method is called upon
        the first row received for this instance.  Note that some 
        attributes and collections may or may not be loaded or even 
        initialized, depending on what's present in the result rows.

        """
    
    def on_resurrect(self, target):
        """"""

        
class MapperEvents(event.Events):
    """Define events specific to mappings.
    
    e.g.::
    
        from sqlalchemy import event
        from sqlalchemy.orm import mapper

        # attach to a class
        event.listen(my_before_insert_listener, 'on_before_insert', SomeMappedClass)

        # attach to all mappers
        event.listen(some_listener, 'on_before_insert', mapper)
    
    Mapper event listeners are propagated to subclass (inheriting)
    mappers unconditionally.
    
    Several modifiers are available to the listen() function.
    
    :param propagate=False: When True, the event listener should 
       be applied to all inheriting mappers as well.
    :param raw=False: When True, the "target" argument to the
       event, if applicable will be the :class:`.InstanceState` management
       object, rather than the mapped instance itself.
    :param retval=False: when True, the user-defined event function
       must have a return value, the purpose of which is either to
       control subsequent event propagation, or to otherwise alter 
       the operation in progress by the mapper.   Possible values
       here are:
      
       * ``sqlalchemy.orm.interfaces.EXT_CONTINUE`` - continue event
         processing normally.
       * ``sqlalchemy.orm.interfaces.EXT_STOP`` - cancel all subsequent
         event handlers in the chain.
       * other values - the return value specified by specific listeners,
         such as "translate_row" or "create_instance".
     
    """

    @classmethod
    def accept_with(cls, target):
        from sqlalchemy.orm import mapper, class_mapper, Mapper
        if target is mapper:
            return Mapper
        elif isinstance(target, type):
            if issubclass(target, Mapper):
                return target
            else:
                return class_mapper(target)
        else:
            return target
        
    @classmethod
    def listen(cls, fn, identifier, target, 
                            raw=False, retval=False, propagate=False):
        from sqlalchemy.orm.interfaces import EXT_CONTINUE

        if not raw or not retval:
            if not raw:
                meth = getattr(cls, identifier)
                try:
                    target_index = inspect.getargspec(meth)[0].index('target') - 1
                except ValueError:
                    target_index = None
            
            wrapped_fn = fn
            def wrap(*arg, **kw):
                if not raw and target_index is not None:
                    arg = list(arg)
                    arg[target_index] = arg[target_index].obj()
                if not retval:
                    wrapped_fn(*arg, **kw)
                    return EXT_CONTINUE
                else:
                    return wrapped_fn(*arg, **kw)
            fn = wrap
        
        if propagate:
            for mapper in target.self_and_descendants:
                event.Events.listen(fn, identifier, mapper, propagate=True)
        else:
            event.Events.listen(fn, identifier, target)
        
    def on_instrument_class(self, mapper, class_):
        """Receive a class when the mapper is first constructed, and has
        applied instrumentation to the mapped class.
        
        This listener can generally only be applied to the :class:`.Mapper`
        class overall.
        
        """

    def on_translate_row(self, mapper, context, row):
        """Perform pre-processing on the given result row and return a
        new row instance.

        This listener is typically registered with ``retval=True``.
        It is called when the mapper first receives a row, before
        the object identity or the instance itself has been derived
        from that row.   The given row may or may not be a 
        ``RowProxy`` object - it will always be a dictionary-like
        object which contains mapped columns as keys.  The 
        returned object should also be a dictionary-like object
        which recognizes mapped columns as keys.
        
        If the ultimate return value is EXT_CONTINUE, the row
        is not translated.
        
        """

    def on_create_instance(self, mapper, context, row, class_):
        """Receive a row when a new object instance is about to be
        created from that row.

        The method can choose to create the instance itself, or it can return
        EXT_CONTINUE to indicate normal object creation should take place.
        This listener is typically registered with ``retval=True``.

        mapper
          The mapper doing the operation

        context
          The QueryContext generated from the Query.

        row
          The result row from the database

        class\_
          The class we are mapping.

        return value
          A new object instance, or EXT_CONTINUE

        """

    def on_append_result(self, mapper, context, row, target, 
                        result, **flags):
        """Receive an object instance before that instance is appended
        to a result list.

        If this method is registered with ``retval=True``, 
        the append operation can be replaced.  If any value other than
        EXT_CONTINUE is returned, result appending will not proceed for 
        this instance, giving this extension an opportunity to do the 
        appending itself, if desired.

        mapper
          The mapper doing the operation.

        selectcontext
          The QueryContext generated from the Query.

        row
          The result row from the database.

        target
          The object instance to be appended to the result, or
          the InstanceState if registered with ``raw=True``.

        result
          List to which results are being appended.

        \**flags
          extra information about the row, same as criterion in
          ``create_row_processor()`` method of
          :class:`~sqlalchemy.orm.interfaces.MapperProperty`
        """


    def on_populate_instance(self, mapper, context, row, 
                            target, **flags):
        """Receive an instance before that instance has
        its attributes populated.

        This usually corresponds to a newly loaded instance but may
        also correspond to an already-loaded instance which has
        unloaded attributes to be populated.  The method may be called
        many times for a single instance, as multiple result rows are
        used to populate eagerly loaded collections.

        If this listener is registered with ``retval=True`` and 
        returns EXT_CONTINUE, instance population will
        proceed normally.  If any other value or None is returned,
        instance population will not proceed, giving this extension an
        opportunity to populate the instance itself, if desired.

        As of 0.5, most usages of this hook are obsolete.  For a
        generic "object has been newly created from a row" hook, use
        ``reconstruct_instance()``, or the ``@orm.reconstructor``
        decorator.

        """

    def on_before_insert(self, mapper, connection, target):
        """Receive an object instance before that instance is inserted
        into its table.

        This is a good place to set up primary key values and such
        that aren't handled otherwise.

        Column-based attributes can be modified within this method
        which will result in the new value being inserted.  However
        *no* changes to the overall flush plan can be made, and 
        manipulation of the ``Session`` will not have the desired effect.
        To manipulate the ``Session`` within an extension, use 
        ``SessionExtension``.

        """

    def on_after_insert(self, mapper, connection, target):
        """Receive an object instance after that instance is inserted.
        
        """

    def on_before_update(self, mapper, connection, target):
        """Receive an object instance before that instance is updated.

        Note that this method is called for all instances that are marked as
        "dirty", even those which have no net changes to their column-based
        attributes. An object is marked as dirty when any of its column-based
        attributes have a "set attribute" operation called or when any of its
        collections are modified. If, at update time, no column-based
        attributes have any net changes, no UPDATE statement will be issued.
        This means that an instance being sent to before_update is *not* a
        guarantee that an UPDATE statement will be issued (although you can
        affect the outcome here).
        
        To detect if the column-based attributes on the object have net
        changes, and will therefore generate an UPDATE statement, use
        ``object_session(instance).is_modified(instance,
        include_collections=False)``.

        Column-based attributes can be modified within this method
        which will result in the new value being updated.  However
        *no* changes to the overall flush plan can be made, and 
        manipulation of the ``Session`` will not have the desired effect.
        To manipulate the ``Session`` within an extension, use 
        ``SessionExtension``.

        """

    def on_after_update(self, mapper, connection, target):
        """Receive an object instance after that instance is updated.
        
        """

    def on_before_delete(self, mapper, connection, target):
        """Receive an object instance before that instance is deleted.

        Note that *no* changes to the overall flush plan can be made
        here; and manipulation of the ``Session`` will not have the
        desired effect. To manipulate the ``Session`` within an
        extension, use ``SessionExtension``.

        """

    def on_after_delete(self, mapper, connection, target):
        """Receive an object instance after that instance is deleted.

        """

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
    :param retval=False: when True, the user-defined event 
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
        
        if not raw or not retval:
            orig_fn = fn
            def wrap(target, value, *arg):
                if not raw:
                    target = target.obj()
                if not retval:
                    orig_fn(target, value, *arg)
                    return value
                else:
                    return orig_fn(target, value, *arg)
            fn = wrap
            
        event.Events.listen(fn, identifier, target, propagate)
        
        if propagate:
            from sqlalchemy.orm.instrumentation import manager_of_class
            
            manager = manager_of_class(target.class_)
            
            for mgr in manager.subclass_managers(True):
                event.Events.listen(fn, identifier, mgr[target.key], True)
        
    @classmethod
    def remove(cls, fn, identifier, target):
        raise NotImplementedError("Removal of attribute events not yet implemented")
        
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

