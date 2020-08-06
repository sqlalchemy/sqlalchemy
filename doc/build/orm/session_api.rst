.. currentmodule:: sqlalchemy.orm

Session API
===========

Session and sessionmaker()
--------------------------

.. autoclass:: sessionmaker
    :members:
    :inherited-members:

.. autoclass:: ORMExecuteState
    :members:

    .. attribute::  session

        The :class:`_orm.Session` in use.

    .. attribute:: statement

        The SQL statement being invoked.  For an ORM selection as would
        be retrieved from :class:`_orm.Query`, this is an instance of
        :class:`_future.select` that was generated from the ORM query.

    .. attribute:: parameters

        Dictionary of parameters that was passed to :meth:`_orm.Session.execute`.

    .. attribute:: bind_arguments

        The dictionary passed as the
        :paramref:`_orm.Session.execute.bind_arguments` dictionary.  This
        dictionary may be used by extensions to :class:`_orm.Session` to pass
        arguments that will assist in determining amongst a set of database
        connections which one should be used to invoke this statement.

    .. attribute:: local_execution_options

        Dictionary view of the execution options passed to the
        :meth:`.Session.execute` method.  This does not include options
        that may be associated with the statement being invoked.

        .. seealso::

            :attr:`_orm.ORMExecuteState.execution_options`

    .. attribute::  execution_options
        The complete dictionary of current execution options.

        This is a merge of the statement level options with the
        locally passed execution options.

.. autoclass:: Session
   :members:
   :inherited-members:

.. autoclass:: SessionTransaction
   :members:

Session Utilities
-----------------

.. autofunction:: close_all_sessions

.. autofunction:: make_transient

.. autofunction:: make_transient_to_detached

.. autofunction:: object_session

.. autofunction:: sqlalchemy.orm.util.was_deleted

Attribute and State Management Utilities
----------------------------------------

These functions are provided by the SQLAlchemy attribute
instrumentation API to provide a detailed interface for dealing
with instances, attribute values, and history.  Some of them
are useful when constructing event listener functions, such as
those described in :doc:`/orm/events`.

.. currentmodule:: sqlalchemy.orm.util

.. autofunction:: object_state

.. currentmodule:: sqlalchemy.orm.attributes

.. autofunction:: del_attribute

.. autofunction:: get_attribute

.. autofunction:: get_history

.. autofunction:: init_collection

.. autofunction:: flag_modified

.. autofunction:: flag_dirty

.. function:: instance_state

    Return the :class:`.InstanceState` for a given
    mapped object.

    This function is the internal version
    of :func:`.object_state`.   The
    :func:`.object_state` and/or the
    :func:`_sa.inspect` function is preferred here
    as they each emit an informative exception
    if the given object is not mapped.

.. autofunction:: sqlalchemy.orm.instrumentation.is_instrumented

.. autofunction:: set_attribute

.. autofunction:: set_committed_value

.. autoclass:: History
    :members:

