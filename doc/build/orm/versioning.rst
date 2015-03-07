.. _mapper_version_counter:

Configuring a Version Counter
=============================

The :class:`.Mapper` supports management of a :term:`version id column`, which
is a single table column that increments or otherwise updates its value
each time an ``UPDATE`` to the mapped table occurs.  This value is checked each
time the ORM emits an ``UPDATE`` or ``DELETE`` against the row to ensure that
the value held in memory matches the database value.

.. warning::

    Because the versioning feature relies upon comparison of the **in memory**
    record of an object, the feature only applies to the :meth:`.Session.flush`
    process, where the ORM flushes individual in-memory rows to the database.
    It does **not** take effect when performing
    a multirow UPDATE or DELETE using :meth:`.Query.update` or :meth:`.Query.delete`
    methods, as these methods only emit an UPDATE or DELETE statement but otherwise
    do not have direct access to the contents of those rows being affected.

The purpose of this feature is to detect when two concurrent transactions
are modifying the same row at roughly the same time, or alternatively to provide
a guard against the usage of a "stale" row in a system that might be re-using
data from a previous transaction without refreshing (e.g. if one sets ``expire_on_commit=False``
with a :class:`.Session`, it is possible to re-use the data from a previous
transaction).

.. topic:: Concurrent transaction updates

    When detecting concurrent updates within transactions, it is typically the
    case that the database's transaction isolation level is below the level of
    :term:`repeatable read`; otherwise, the transaction will not be exposed
    to a new row value created by a concurrent update which conflicts with
    the locally updated value.  In this case, the SQLAlchemy versioning
    feature will typically not be useful for in-transaction conflict detection,
    though it still can be used for cross-transaction staleness detection.

    The database that enforces repeatable reads will typically either have locked the
    target row against a concurrent update, or is employing some form
    of multi version concurrency control such that it will emit an error
    when the transaction is committed.  SQLAlchemy's version_id_col is an alternative
    which allows version tracking to occur for specific tables within a transaction
    that otherwise might not have this isolation level set.

    .. seealso::

        `Repeatable Read Isolation Level <http://www.postgresql.org/docs/9.1/static/transaction-iso.html#XACT-REPEATABLE-READ>`_ - Postgresql's implementation of repeatable read, including a description of the error condition.

Simple Version Counting
-----------------------

The most straightforward way to track versions is to add an integer column
to the mapped table, then establish it as the ``version_id_col`` within the
mapper options::

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        version_id = Column(Integer, nullable=False)
        name = Column(String(50), nullable=False)

        __mapper_args__ = {
            "version_id_col": version_id
        }

Above, the ``User`` mapping tracks integer versions using the column
``version_id``.   When an object of type ``User`` is first flushed, the
``version_id`` column will be given a value of "1".   Then, an UPDATE
of the table later on will always be emitted in a manner similar to the
following::

    UPDATE user SET version_id=:version_id, name=:name
    WHERE user.id = :user_id AND user.version_id = :user_version_id
    {"name": "new name", "version_id": 2, "user_id": 1, "user_version_id": 1}

The above UPDATE statement is updating the row that not only matches
``user.id = 1``, it also is requiring that ``user.version_id = 1``, where "1"
is the last version identifier we've been known to use on this object.
If a transaction elsewhere has modified the row independently, this version id
will no longer match, and the UPDATE statement will report that no rows matched;
this is the condition that SQLAlchemy tests, that exactly one row matched our
UPDATE (or DELETE) statement.  If zero rows match, that indicates our version
of the data is stale, and a :exc:`.StaleDataError` is raised.

.. _custom_version_counter:

Custom Version Counters / Types
-------------------------------

Other kinds of values or counters can be used for versioning.  Common types include
dates and GUIDs.   When using an alternate type or counter scheme, SQLAlchemy
provides a hook for this scheme using the ``version_id_generator`` argument,
which accepts a version generation callable.  This callable is passed the value of the current
known version, and is expected to return the subsequent version.

For example, if we wanted to track the versioning of our ``User`` class
using a randomly generated GUID, we could do this (note that some backends
support a native GUID type, but we illustrate here using a simple string)::

    import uuid

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        version_uuid = Column(String(32))
        name = Column(String(50), nullable=False)

        __mapper_args__ = {
            'version_id_col':version_uuid,
            'version_id_generator':lambda version: uuid.uuid4().hex
        }

The persistence engine will call upon ``uuid.uuid4()`` each time a
``User`` object is subject to an INSERT or an UPDATE.  In this case, our
version generation function can disregard the incoming value of ``version``,
as the ``uuid4()`` function
generates identifiers without any prerequisite value.  If we were using
a sequential versioning scheme such as numeric or a special character system,
we could make use of the given ``version`` in order to help determine the
subsequent value.

.. seealso::

    :ref:`custom_guid_type`

.. _server_side_version_counter:

Server Side Version Counters
----------------------------

The ``version_id_generator`` can also be configured to rely upon a value
that is generated by the database.  In this case, the database would need
some means of generating new identifiers when a row is subject to an INSERT
as well as with an UPDATE.   For the UPDATE case, typically an update trigger
is needed, unless the database in question supports some other native
version identifier.  The Postgresql database in particular supports a system
column called `xmin <http://www.postgresql.org/docs/9.1/static/ddl-system-columns.html>`_
which provides UPDATE versioning.  We can make use
of the Postgresql ``xmin`` column to version our ``User``
class as follows::

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = Column(String(50), nullable=False)
        xmin = Column("xmin", Integer, system=True)

        __mapper_args__ = {
            'version_id_col': xmin,
            'version_id_generator': False
        }

With the above mapping, the ORM will rely upon the ``xmin`` column for
automatically providing the new value of the version id counter.

.. topic:: creating tables that refer to system columns

    In the above scenario, as ``xmin`` is a system column provided by Postgresql,
    we use the ``system=True`` argument to mark it as a system-provided
    column, omitted from the ``CREATE TABLE`` statement.


The ORM typically does not actively fetch the values of database-generated
values when it emits an INSERT or UPDATE, instead leaving these columns as
"expired" and to be fetched when they are next accessed, unless the ``eager_defaults``
:func:`.mapper` flag is set.  However, when a
server side version column is used, the ORM needs to actively fetch the newly
generated value.  This is so that the version counter is set up *before*
any concurrent transaction may update it again.   This fetching is also
best done simultaneously within the INSERT or UPDATE statement using :term:`RETURNING`,
otherwise if emitting a SELECT statement afterwards, there is still a potential
race condition where the version counter may change before it can be fetched.

When the target database supports RETURNING, an INSERT statement for our ``User`` class will look
like this::

    INSERT INTO "user" (name) VALUES (%(name)s) RETURNING "user".id, "user".xmin
    {'name': 'ed'}

Where above, the ORM can acquire any newly generated primary key values along
with server-generated version identifiers in one statement.   When the backend
does not support RETURNING, an additional SELECT must be emitted for **every**
INSERT and UPDATE, which is much less efficient, and also introduces the possibility of
missed version counters::

    INSERT INTO "user" (name) VALUES (%(name)s)
    {'name': 'ed'}

    SELECT "user".version_id AS user_version_id FROM "user" where
    "user".id = :param_1
    {"param_1": 1}

It is *strongly recommended* that server side version counters only be used
when absolutely necessary and only on backends that support :term:`RETURNING`,
e.g. Postgresql, Oracle, SQL Server (though SQL Server has
`major caveats <http://blogs.msdn.com/b/sqlprogrammability/archive/2008/07/11/update-with-output-clause-triggers-and-sqlmoreresults.aspx>`_ when triggers are used), Firebird.

.. versionadded:: 0.9.0

    Support for server side version identifier tracking.

Programmatic or Conditional Version Counters
---------------------------------------------

When ``version_id_generator`` is set to False, we can also programmatically
(and conditionally) set the version identifier on our object in the same way
we assign any other mapped attribute.  Such as if we used our UUID example, but
set ``version_id_generator`` to ``False``, we can set the version identifier
at our choosing::

    import uuid

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        version_uuid = Column(String(32))
        name = Column(String(50), nullable=False)

        __mapper_args__ = {
            'version_id_col':version_uuid,
            'version_id_generator': False
        }

    u1 = User(name='u1', version_uuid=uuid.uuid4())

    session.add(u1)

    session.commit()

    u1.name = 'u2'
    u1.version_uuid = uuid.uuid4()

    session.commit()

We can update our ``User`` object without incrementing the version counter
as well; the value of the counter will remain unchanged, and the UPDATE
statement will still check against the previous value.  This may be useful
for schemes where only certain classes of UPDATE are sensitive to concurrency
issues::

    # will leave version_uuid unchanged
    u1.name = 'u3'
    session.commit()

.. versionadded:: 0.9.0

    Support for programmatic and conditional version identifier tracking.

