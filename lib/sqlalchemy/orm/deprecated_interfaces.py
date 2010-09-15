from sqlalchemy import event
from interfaces import EXT_CONTINUE


class MapperExtension(object):
    """Base implementation for customizing ``Mapper`` behavior.
    
    New extension classes subclass ``MapperExtension`` and are specified
    using the ``extension`` mapper() argument, which is a single
    ``MapperExtension`` or a list of such.   A single mapper
    can maintain a chain of ``MapperExtension`` objects.  When a
    particular mapping event occurs, the corresponding method 
    on each ``MapperExtension`` is invoked serially, and each method
    has the ability to halt the chain from proceeding further.
    
    Each ``MapperExtension`` method returns the symbol
    EXT_CONTINUE by default.   This symbol generally means "move
    to the next ``MapperExtension`` for processing".  For methods
    that return objects like translated rows or new object
    instances, EXT_CONTINUE means the result of the method
    should be ignored.   In some cases it's required for a 
    default mapper activity to be performed, such as adding a 
    new instance to a result list.
    
    The symbol EXT_STOP has significance within a chain
    of ``MapperExtension`` objects that the chain will be stopped
    when this symbol is returned.  Like EXT_CONTINUE, it also
    has additional significance in some cases that a default
    mapper activity will not be performed.
    
    """

    def instrument_class(self, mapper, class_):
        """Receive a class when the mapper is first constructed, and has
        applied instrumentation to the mapped class.
        
        The return value is only significant within the ``MapperExtension`` 
        chain; the parent mapper's behavior isn't modified by this method.
        
        """
        return EXT_CONTINUE

    def init_instance(self, mapper, class_, oldinit, instance, args, kwargs):
        """Receive an instance when it's constructor is called.
        
        This method is only called during a userland construction of 
        an object.  It is not called when an object is loaded from the
        database.
        
        The return value is only significant within the ``MapperExtension`` 
        chain; the parent mapper's behavior isn't modified by this method.

        """
        return EXT_CONTINUE

    def init_failed(self, mapper, class_, oldinit, instance, args, kwargs):
        """Receive an instance when it's constructor has been called, 
        and raised an exception.
        
        This method is only called during a userland construction of 
        an object.  It is not called when an object is loaded from the
        database.
        
        The return value is only significant within the ``MapperExtension`` 
        chain; the parent mapper's behavior isn't modified by this method.

        """
        return EXT_CONTINUE

    def translate_row(self, mapper, context, row):
        """Perform pre-processing on the given result row and return a
        new row instance.

        This is called when the mapper first receives a row, before
        the object identity or the instance itself has been derived
        from that row.   The given row may or may not be a 
        ``RowProxy`` object - it will always be a dictionary-like
        object which contains mapped columns as keys.  The 
        returned object should also be a dictionary-like object
        which recognizes mapped columns as keys.
        
        If the ultimate return value is EXT_CONTINUE, the row
        is not translated.
        
        """
        return EXT_CONTINUE

    def create_instance(self, mapper, selectcontext, row, class_):
        """Receive a row when a new object instance is about to be
        created from that row.

        The method can choose to create the instance itself, or it can return
        EXT_CONTINUE to indicate normal object creation should take place.

        mapper
          The mapper doing the operation

        selectcontext
          The QueryContext generated from the Query.

        row
          The result row from the database

        class\_
          The class we are mapping.

        return value
          A new object instance, or EXT_CONTINUE

        """
        return EXT_CONTINUE

    def append_result(self, mapper, selectcontext, row, instance, 
                        result, **flags):
        """Receive an object instance before that instance is appended
        to a result list.

        If this method returns EXT_CONTINUE, result appending will proceed
        normally.  if this method returns any other value or None,
        result appending will not proceed for this instance, giving
        this extension an opportunity to do the appending itself, if
        desired.

        mapper
          The mapper doing the operation.

        selectcontext
          The QueryContext generated from the Query.

        row
          The result row from the database.

        instance
          The object instance to be appended to the result.

        result
          List to which results are being appended.

        \**flags
          extra information about the row, same as criterion in
          ``create_row_processor()`` method of
          :class:`~sqlalchemy.orm.interfaces.MapperProperty`
        """

        return EXT_CONTINUE

    def populate_instance(self, mapper, selectcontext, row, 
                            instance, **flags):
        """Receive an instance before that instance has
        its attributes populated.

        This usually corresponds to a newly loaded instance but may
        also correspond to an already-loaded instance which has
        unloaded attributes to be populated.  The method may be called
        many times for a single instance, as multiple result rows are
        used to populate eagerly loaded collections.

        If this method returns EXT_CONTINUE, instance population will
        proceed normally.  If any other value or None is returned,
        instance population will not proceed, giving this extension an
        opportunity to populate the instance itself, if desired.

        As of 0.5, most usages of this hook are obsolete.  For a
        generic "object has been newly created from a row" hook, use
        ``reconstruct_instance()``, or the ``@orm.reconstructor``
        decorator.

        """
        return EXT_CONTINUE

    def reconstruct_instance(self, mapper, instance):
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

        The return value is only significant within the ``MapperExtension`` 
        chain; the parent mapper's behavior isn't modified by this method.

        """
        return EXT_CONTINUE

    def before_insert(self, mapper, connection, instance):
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

        The return value is only significant within the ``MapperExtension`` 
        chain; the parent mapper's behavior isn't modified by this method.

        """

        return EXT_CONTINUE

    def after_insert(self, mapper, connection, instance):
        """Receive an object instance after that instance is inserted.

        The return value is only significant within the ``MapperExtension`` 
        chain; the parent mapper's behavior isn't modified by this method.
        
        """

        return EXT_CONTINUE

    def before_update(self, mapper, connection, instance):
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

        The return value is only significant within the ``MapperExtension`` 
        chain; the parent mapper's behavior isn't modified by this method.

        """

        return EXT_CONTINUE

    def after_update(self, mapper, connection, instance):
        """Receive an object instance after that instance is updated.

        The return value is only significant within the ``MapperExtension`` 
        chain; the parent mapper's behavior isn't modified by this method.
        
        """

        return EXT_CONTINUE

    def before_delete(self, mapper, connection, instance):
        """Receive an object instance before that instance is deleted.

        Note that *no* changes to the overall flush plan can be made
        here; and manipulation of the ``Session`` will not have the
        desired effect. To manipulate the ``Session`` within an
        extension, use ``SessionExtension``.

        The return value is only significant within the ``MapperExtension`` 
        chain; the parent mapper's behavior isn't modified by this method.

        """

        return EXT_CONTINUE

    def after_delete(self, mapper, connection, instance):
        """Receive an object instance after that instance is deleted.

        The return value is only significant within the ``MapperExtension``
        chain; the parent mapper's behavior isn't modified by this method.

        """

        return EXT_CONTINUE

class SessionExtension(object):

    """An extension hook object for Sessions.  Subclasses may be
    installed into a Session (or sessionmaker) using the ``extension``
    keyword argument. """

    def before_commit(self, session):
        """Execute right before commit is called.
        
        Note that this may not be per-flush if a longer running
        transaction is ongoing."""

    def after_commit(self, session):
        """Execute after a commit has occured.
        
        Note that this may not be per-flush if a longer running
        transaction is ongoing."""

    def after_rollback(self, session):
        """Execute after a rollback has occured.
        
        Note that this may not be per-flush if a longer running
        transaction is ongoing."""

    def before_flush( self, session, flush_context, instances):
        """Execute before flush process has started.
        
        `instances` is an optional list of objects which were passed to
        the ``flush()`` method. """

    def after_flush(self, session, flush_context):
        """Execute after flush has completed, but before commit has been
        called.
        
        Note that the session's state is still in pre-flush, i.e. 'new',
        'dirty', and 'deleted' lists still show pre-flush state as well
        as the history settings on instance attributes."""

    def after_flush_postexec(self, session, flush_context):
        """Execute after flush has completed, and after the post-exec
        state occurs.
        
        This will be when the 'new', 'dirty', and 'deleted' lists are in
        their final state.  An actual commit() may or may not have
        occured, depending on whether or not the flush started its own
        transaction or participated in a larger transaction. """

    def after_begin( self, session, transaction, connection):
        """Execute after a transaction is begun on a connection
        
        `transaction` is the SessionTransaction. This method is called
        after an engine level transaction is begun on a connection. """

    def after_attach(self, session, instance):
        """Execute after an instance is attached to a session.
        
        This is called after an add, delete or merge. """

    def after_bulk_update( self, session, query, query_context, result):
        """Execute after a bulk update operation to the session.
        
        This is called after a session.query(...).update()
        
        `query` is the query object that this update operation was
        called on. `query_context` was the query context object.
        `result` is the result object returned from the bulk operation.
        """

    def after_bulk_delete( self, session, query, query_context, result):
        """Execute after a bulk delete operation to the session.
        
        This is called after a session.query(...).delete()
        
        `query` is the query object that this delete operation was
        called on. `query_context` was the query context object.
        `result` is the result object returned from the bulk operation.
        """


class AttributeExtension(object):
    """An event handler for individual attribute change events.
    
    .. note:: :class:`AttributeExtension` is deprecated.   Please
       refer to :func:`event.listen` as well as 
       :attr:`AttributeImpl.events`.
    
    AttributeExtension is assembled within the descriptors associated
    with a mapped class.
    
    """

    active_history = True
    """indicates that the set() method would like to receive the 'old' value,
    even if it means firing lazy callables.
    """

    @classmethod
    def _adapt_listener(cls, self, listener):
        event.listen(listener.append, 'on_append', self,
                            active_history=listener.active_history)
        event.listen(listener.remove, 'on_remove', self,
                            active_history=listener.active_history)
        event.listen(listener.set, 'on_set', self,
                            active_history=listener.active_history)
        
    
    def append(self, state, value, initiator):
        """Receive a collection append event.

        The returned value will be used as the actual value to be
        appended.

        """
        return value

    def remove(self, state, value, initiator):
        """Receive a remove event.

        No return value is defined.

        """
        pass

    def set(self, state, value, oldvalue, initiator):
        """Receive a set event.

        The returned value will be used as the actual value to be
        set.

        """
        return value


