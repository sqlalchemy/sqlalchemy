.. _glossary:

========
Glossary
========

.. note::

	The Glossary is a brand new addition to the documentation.  While
	sparse at the moment we hope to fill it up with plenty of new
	terms soon!

.. glossary::

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

