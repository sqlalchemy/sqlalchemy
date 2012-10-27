.. _glossary:

========
Glossary
========

.. note::

	The Glossary is a brand new addition to the documentation.  While
	sparse at the moment we hope to fill it up with plenty of new
	terms soon!

.. glossary::


    descriptor
        In Python, a descriptor is an object attribute with “binding behavior”, one whose attribute access has been overridden by methods in the `descriptor protocol <http://docs.python.org/howto/descriptor.html>`_.
        Those methods are __get__(), __set__(), and __delete__(). If any of those methods are defined
        for an object, it is said to be a descriptor.

        In SQLAlchemy, descriptors are used heavily in order to provide attribute behavior
        on mapped classes.   When a class is mapped as such::

            class MyClass(Base):
                __tablename__ = 'foo'

                id = Column(Integer, primary_key=True)
                data = Column(String)

        The ``MyClass`` class will be :term:`mapped` when its definition
        is complete, at which point the ``id`` and ``data`` attributes,
        starting out as :class:`.Column` objects, will be replaced
        by the :term:`instrumentation` system with instances
        of :class:`.InstrumentedAttribute`, which are descriptors that
        provide the above mentioned ``__get__()``, ``__set__()`` and
        ``__delete__()`` methods.   The :class:`.InstrumentedAttribute`
        will generate a SQL expression when used at the class level::

            >>> print MyClass.data == 5
            data = :data_1

        and at the instance level, keeps track of changes to values,
        and also :term:`lazy loads` unloaded attributes
        from the database::

            >>> m1 = MyClass()
            >>> m1.id = 5
            >>> m1.data = "some data"

            >>> from sqlalchemy import inspect
            >>> inspect(m1).attrs.data.history.added
            "some data"

    discriminator
        A result-set column which is used during :term:`polymorphic` loading
        to determine what kind of mapped class should be applied to a particular
        incoming result row.   In SQLAlchemy, the classes are always part
        of a hierarchy mapping using inheritance mapping.

        .. seealso::

            :ref:`inheritance_toplevel`

    instrumentation
    instrumented
        Instrumentation refers to the process of augmenting the functionality
        and attribute set of a particular class.   Ideally, the
        behavior of the class should remain close to a regular
        class, except that additional behviors and features are
        made available.  The SQLAlchemy :term:`mapping` process,
        among other things, adds database-enabled :term:`descriptors`
        to a mapped
        class which each represent a particular database column
        or relationship to a related class.

    lazy load
        In object relational mapping, a "lazy load" refers to an
        attribute that does not contain its database-side value
        for some period of time, typically when the object is
        first loaded.  Instead, the attribute receives a
        *memoization* that causes it to go out to the database
        and load its data when it's first used.   Using this pattern,
        the complexity and time spent within object fetches can
        sometimes be reduced, in that
        attributes for related tables don't need to be addressed
        immediately.

        .. seealso::

            `Lazy Load (on Martin Fowler) <http://martinfowler.com/eaaCatalog/lazyLoad.html>`_

            :term:`N plus one problem`

            :doc:`orm/loading`

    mapping
    mapped
        We say a class is "mapped" when it has been passed through the
        :func:`.orm.mapper` function.   This process associates the
        class with a database table or other :term:`selectable`
        construct, so that instances of it can be persisted
        using a :class:`.Session` as well as loaded using a
        :class:`.Query`.

    N plus one problem
        The N plus one problem is a common side effect of the
        :term:`lazy load` pattern, whereby an application wishes
        to iterate through a related attribute or collection on
        each member of a result set of objects, where that
        attribute or collection is set to be loaded via the lazy
        load pattern.   The net result is that a SELECT statement
        is emitted to load the initial result set of parent objects;
        then, as the application iterates through each member,
        an additional SELECT statement is emitted for each member
        in order to load the related attribute or collection for
        that member.  The end result is that for a result set of
        N parent objects, there will be N + 1 SELECT statements emitted.

        The N plus one problem is alleviated using :term:`eager loading`.

        .. seealso::

            :doc:`orm/loading`

    polymorphic
    polymorphically
        Refers to a function that handles several types at once.  In SQLAlchemy,
        the term is usually applied to the concept of an ORM mapped class
        whereby a query operation will return different subclasses
        based on information in the result set, typically by checking the
        value of a particular column in the result known as the :term:`discriminator`.

        Polymorphic loading in SQLAlchemy implies that a one or a
        combination of three different schemes are used to map a hierarchy
        of classes; "joined", "single", and "concrete".   The section
        :ref:`inheritance_toplevel` describes inheritance mapping fully.


    release
    releases
    released
        This term refers to when an operation terminates some state which
        corresponds to a service of some kind.  Specifically within
        SQLAlchemy, it usually refers to a reference to a database connection,
        and typically a transaction associated with that connection.
        When we say "the operation releases transactional resources",
        it means basically that we have a :class:`.Connection` object
        and we are calling the :meth:`.Connection.close` method, which has
        the effect of the underlying DBAPI connection being returned
        to the connection pool.   The connection pool, when it receives
        a connection for return, unconditionally calls the ``rollback()``
        method of the DBAPI connection, so that any locks or data snapshots within
        that connection are removed.    Then, the connection is either
        stored locally in memory, still connected but not in a transaction,
        for subsequent reuse by another operation, or it is closed
        immediately, depending on the configuration and current
        state of the connection pool.

        .. seealso::

        	:ref:`pooling_toplevel`


    unit of work
        This pattern is where the system transparently keeps
        track of changes to objects and periodically flushes all those
        pending changes out to the database. SQLAlchemy's Session
        implements this pattern fully in a manner similar to that of
        Hibernate.

        .. seealso::

            `Unit of Work by Martin Fowler <http://martinfowler.com/eaaCatalog/unitOfWork.html>`_

            :doc:`orm/session`
