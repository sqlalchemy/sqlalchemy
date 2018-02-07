=============================
What's New in SQLAlchemy 1.3?
=============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 1.2
    and SQLAlchemy version 1.3.

Introduction
============

This guide introduces what's new in SQLAlchemy version 1.3
and also documents changes which affect users migrating
their applications from the 1.2 series of SQLAlchemy to 1.3.

Please carefully review the sections on behavioral changes for
potentially backwards-incompatible changes in behavior.

New Features and Improvements - ORM
===================================

.. _change_4257:

info dictionary added to InstanceState
--------------------------------------

Added the ``.info`` dictionary to the :class:`.InstanceState` class, the object
that comes from calling :func:`.inspect` on a mapped object.  This allows custom
recipes to add additional information about an object that will be carried
along with that object's full lifecycle in memory::

    from sqlalchemy import inspect

    u1 = User(id=7, name='ed')

    inspect(u1).info['user_info'] = '7|ed'


:ticket:`4257`

.. _change_4196:

Horizontal Sharding extension supports bulk update and delete methods
---------------------------------------------------------------------

The :class:`.ShardedQuery` extension object supports the :meth:`.Query.update`
and :meth:`.Query.delete` bulk update/delete methods.    The ``query_chooser``
callable is consulted when they are called in order to run the update/delete
across multiple shards based on given criteria.


:ticket:`4196`

Key Behavioral Changes - ORM
=============================

.. _change_4308:

Association proxy has new cascade_scalar_deletes flag
-----------------------------------------------------

Given a mapping as::

    class A(Base):
        __tablename__ = 'test_a'
        id = Column(Integer, primary_key=True)
        ab = relationship(
            'AB', backref='a', uselist=False)
        b = association_proxy(
            'ab', 'b', creator=lambda b: AB(b=b),
            cascade_scalar_deletes=True)


    class B(Base):
        __tablename__ = 'test_b'
        id = Column(Integer, primary_key=True)
        ab = relationship('AB', backref='b', cascade='all, delete-orphan')


    class AB(Base):
        __tablename__ = 'test_ab'
        a_id = Column(Integer, ForeignKey(A.id), primary_key=True)
        b_id = Column(Integer, ForeignKey(B.id), primary_key=True)

An assigment to ``A.b`` will generate an ``AB`` object::

    a.b = B()

The ``A.b`` association is scalar, and includes a new flag
:paramref:`.AssociationProxy.cascade_scalar_deletes`.  When set, setting ``A.b``
to ``None`` will remove ``A.ab`` as well.   The default behavior remains
that it leaves ``a.ab`` in place::

    a.b = None
    assert a.ab is None

While it at first seemed intuitive that this logic should just look at the
"cascade" attribute of the existing relationship, it's not clear from that
alone if the proxied object should be removed, hence the behavior is
made available as an explicit option.

Additionally, ``del`` now works for scalars in a similar manner as setting
to ``None``::

    del a.b
    assert a.ab is None

:ticket:`4308`

.. _change_3423:

AssociationProxy stores class-specific state in a separate container
--------------------------------------------------------------------

The :class:`.AssociationProxy` object makes lots of decisions based on the
parent mapped class it is associated with.   While the
:class:`.AssociationProxy` historically began as a relatively simple "getter",
it became apparent early on that it also needed to make decisions about what
kind of attribute it is referring towards, e.g. scalar or collection, mapped
object or simple value, and similar.  To achieve this, it needs to inspect the
mapped attribute or other descriptor or attribute that it refers towards, as
referenced from its parent class.   However in Python descriptor mechanics, a
descriptor only learns about its "parent" class when it is accessed in the
context of that class, such as calling ``MyClass.some_descriptor``, which calls
the ``__get__()`` method which passes in the class.    The
:class:`.AssociationProxy` object would therefore store state that is specific
to that class, but only once this method were called; trying to inspect this
state ahead of time without first accessing the :class:`.AssociationProxy`
as a descriptor would raise an error.  Additionally, it would  assume that
the first class to be seen by ``__get__()`` would be  the only parent class it
needed to know about.  This is despite the fact that if a particular class
has inheriting subclasses, the association proxy is really working
on behalf of more than one parent class even though it was not explicitly
re-used.  While even with this shortcoming, the association proxy would
still get pretty far with its current behavior, it still leaves shortcomings
in some cases as well as the complex problem of determining the best "owner"
class.

These problems are now solved in that :class:`.AssociationProxy` no longer
modifies its own internal state when ``__get__()`` is called; instead, a new
object is generated per-class known as :class:`.AssociationProxyInstance` which
handles all the state specific to a particular mapped parent class (when the
parent class is not mapped, no :class:`.AssociationProxyInstance` is generated).
The concept of a single "owning class" for the association proxy, which was
nonetheless improved in 1.1, has essentially been replaced with an approach
where the AP now can treat any number of "owning" classes equally.

To accommodate for applications that want to inspect this state for an
:class:`.AssociationProxy` without necessarily calling ``__get__()``, a new
method :meth:`.AssociationProxy.for_class` is added that provides direct access
to a class-specific :class:`.AssociationProxyInstance`, demonstrated as::

    class User(Base):
        # ...

        keywords = association_proxy('kws', 'keyword')


    proxy_state = inspect(User).all_orm_descriptors["keywords"].for_class(User)

Once we have the :class:`.AssociationProxyInstance` object, in the above
example stored in the ``proxy_state`` variable, we can look at attributes
specific to the ``User.keywords`` proxy, such as ``target_class``::


    >>> proxy_state.target_class
    Keyword


:ticket:`3423`

.. _change_4246:

FOR UPDATE clause is rendered within the joined eager load subquery as well as outside
--------------------------------------------------------------------------------------

This change applies specifically to the use of the :func:`.joinedload` loading
strategy in conjunction with a row limited query, e.g. using :meth:`.Query.first`
or :meth:`.Query.limit`, as well as with use of the :class:`.Query.with_for_update` method.

Given a query as::

    session.query(A).options(joinedload(A.b)).limit(5)

The :class:`.Query` object renders a SELECT of the following form when joined
eager loading is combined with LIMIT::

    SELECT subq.a_id, subq.a_data, b_alias.id, b_alias.data FROM (
        SELECT a.id AS a_id, a.data AS a_data FROM a LIMIT 5
    ) AS subq LEFT OUTER JOIN b ON subq.a_id=b.a_id

This is so that the limit of rows takes place for the primary entity without
affecting the joined eager load of related items.   When the above query is
combined with "SELECT..FOR UPDATE", the behavior has been this::

    SELECT subq.a_id, subq.a_data, b_alias.id, b_alias.data FROM (
        SELECT a.id AS a_id, a.data AS a_data FROM a LIMIT 5
    ) AS subq LEFT OUTER JOIN b ON subq.a_id=b.a_id FOR UPDATE

However, MySQL due to https://bugs.mysql.com/bug.php?id=90693 does not lock
the rows inside the subquery, unlike that of Postgresql and other databases.
So the above query now renders as::

    SELECT subq.a_id, subq.a_data, b_alias.id, b_alias.data FROM (
        SELECT a.id AS a_id, a.data AS a_data FROM a LIMIT 5 FOR UPDATE
    ) AS subq LEFT OUTER JOIN b ON subq.a_id=b.a_id FOR UPDATE

On the Oracle dialect, the inner "FOR UPDATE" is not rendered as Oracle does
not support this syntax and the dialect skips any "FOR UPDATE" that is against
a subquery; it isn't necessary in any case since Oracle, like Postgresql,
correctly locks all elements of the returned row.

When using the :paramref:`.Query.with_for_update.of` modifier, typically on
Postgresql, the outer "FOR UPDATE" is omitted, and the OF is now rendered
on the inside; previously, the OF target would not be converted to accommodate
for the subquery correctly.  So
given::

    session.query(A).options(joinedload(A.b)).with_for_update(of=A).limit(5)

The query would now render as::

    SELECT subq.a_id, subq.a_data, b_alias.id, b_alias.data FROM (
        SELECT a.id AS a_id, a.data AS a_data FROM a LIMIT 5 FOR UPDATE OF a
    ) AS subq LEFT OUTER JOIN b ON subq.a_id=b.a_id

The above form should be helpful on Postgresql additionally since Postgresql
will not allow the FOR UPDATE clause to be rendered after the LEFT OUTER JOIN
target.

Overall, FOR UPDATE remains highly specific to the target database in use
and can't easily be generalized for more complex queries.

:ticket:`4246`

.. _change_3844:

passive_deletes='all' will leave FK unchanged for object removed from collection
--------------------------------------------------------------------------------

The :paramref:`.relationship.passive_deletes` option accepts the value
``"all"`` to indicate that no foreign key attributes should be modified when
the object is flushed, even if the relationship's collection / reference has
been removed.   Previously, this did not take place for one-to-many, or
one-to-one relationships, in the following situation::

    class User(Base):
        __tablename__ = 'users'

        id = Column(Integer, primary_key=True)
        addresses = relationship(
            "Address",
            passive_deletes="all")

    class Address(Base):
        __tablename__ = 'addresses'
        id = Column(Integer, primary_key=True)
        email = Column(String)

        user_id = Column(Integer, ForeignKey('users.id'))
        user = relationship("User")

    u1 = session.query(User).first()
    address = u1.addresses[0]
    u1.addresses.remove(address)
    session.commit()

    # would fail and be set to None
    assert address.user_id == u1.id

The fix now includes that ``address.user_id`` is left unchanged as per
``passive_deletes="all"``. This kind of thing is useful for building custom
"version table" schemes and such where rows are archived instead of deleted.

:ticket:`3844`

.. _change_4268:

Association Proxy now Strong References the Parent Object
=========================================================

The long-standing behavior of the association proxy collection maintaining
only a weak reference to the parent object is reverted; the proxy will now
maintain a strong reference to the parent for as long as the proxy
collection itself is also in memory, eliminating the "stale association
proxy" error. This change is being made on an experimental basis to see if
any use cases arise where it causes side effects.

As an example, given a mapping with association proxy::

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)
        bs = relationship("B")
        b_data = association_proxy('bs', 'data')


    class B(Base):
        __tablename__ = 'b'
        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey("a.id"))
        data = Column(String)


    a1 = A(bs=[B(data='b1'), B(data='b2')])

    b_data = a1.b_data

Previously, if ``a1`` were deleted out of scope::

    del a1

Trying to iterate the ``b_data`` collection after ``a1`` is deleted from scope
would raise the error ``"stale association proxy, parent object has gone out of
scope"``.  This is because the association proxy needs to access the actual
``a1.bs`` collection in order to produce a view, and prior to this change it
maintained only a weak reference to ``a1``.   In particular, users would
frequently encounter this error when performing an inline operation
such as::

    collection = session.query(A).filter_by(id=1).first().b_data

Above, because the ``A`` object would be garbage collected before the
``b_data`` collection were actually used.

The change is that the ``b_data`` collection is now maintaining a strong
reference to the ``a1`` object, so that it remains present::

    assert b_data == ['b1', 'b2']

This change introduces the side effect that if an application is passing around
the collection as above, **the parent object won't be garbage collected** until
the collection is also discarded.   As always, if ``a1`` is persistent inside a
particular :class:`.Session`, it will remain part of that session's  state
until it is garbage collected.

Note that this change may be revised if it leads to problems.


:ticket:`4268`

New Features and Improvements - Core
====================================

.. _change_3831:

Binary comparison interpretation for SQL functions
--------------------------------------------------

This enhancement is implemented at the Core level, however is applicable
primarily to the ORM.

A SQL function that compares two elements can now be used as a "comparison"
object, suitable for usage in an ORM :func:`.relationship`, by first
creating the function as usual using the :data:`.func` factory, then
when the function is complete calling upon the :meth:`.FunctionElement.as_comparison`
modifier to produce a :class:`.BinaryExpression` that has a "left" and a "right"
side::

    class Venue(Base):
        __tablename__ = 'venue'
        id = Column(Integer, primary_key=True)
        name = Column(String)

        descendants = relationship(
            "Venue",
            primaryjoin=func.instr(
                remote(foreign(name)), name + "/"
            ).as_comparison(1, 2) == 1,
            viewonly=True,
            order_by=name
        )

Above, the :paramref:`.relationship.primaryjoin` of the "descendants" relationship
will produce a "left" and a "right" expression based on the first and second
arguments passed to ``instr()``.   This allows features like the ORM
lazyload to produce SQL like::

    SELECT venue.id AS venue_id, venue.name AS venue_name
    FROM venue
    WHERE instr(venue.name, (? || ?)) = ? ORDER BY venue.name
    ('parent1', '/', 1)

and a joinedload, such as::

    v1 = s.query(Venue).filter_by(name="parent1").options(
        joinedload(Venue.descendants)).one()

to work as::

    SELECT venue.id AS venue_id, venue.name AS venue_name,
      venue_1.id AS venue_1_id, venue_1.name AS venue_1_name
    FROM venue LEFT OUTER JOIN venue AS venue_1
      ON instr(venue_1.name, (venue.name || ?)) = ?
    WHERE venue.name = ? ORDER BY venue_1.name
    ('/', 1, 'parent1')

This feature is expected to help with situations such as making use of
geometric functions in relationship join conditions, or any case where
the ON clause of the SQL join is expressed in terms of a SQL function.

:ticket:`3831`

.. _change_4271:

Expanding IN feature now supports empty lists
---------------------------------------------

The "expanding IN" feature introduced in version 1.2 at :ref:`change_3953` now
supports empty lists passed to the :meth:`.ColumnOperators.in_` operator.   The implementation
for an empty list will produce an "empty set" expression that is specific to a target
backend, such as "SELECT CAST(NULL AS INTEGER) WHERE 1!=1" for Postgresql,
"SELECT 1 FROM (SELECT 1) as _empty_set WHERE 1!=1" for MySQL::

    >>> from sqlalchemy import create_engine
    >>> from sqlalchemy import select, literal_column, bindparam
    >>> e = create_engine("postgresql://scott:tiger@localhost/test", echo=True)
    >>> with e.connect() as conn:
    ...      conn.execute(
    ...          select([literal_column('1')]).
    ...          where(literal_column('1').in_(bindparam('q', expanding=True))),
    ...          q=[]
    ...      )
    ...
    SELECT 1 WHERE 1 IN (SELECT CAST(NULL AS INTEGER) WHERE 1!=1)

:ticket:`4271`

.. _change_3981:

TypeEngine methods bind_expression, column_expression work with Variant, type-specific types
--------------------------------------------------------------------------------------------

The :meth:`.TypeEngine.bind_expression` and :meth:`.TypeEngine.column_expression` methods
now work when they are present on the "impl" of a particular datatype, allowing these methods
to be used by dialects as well as for :class:`.TypeDecorator` and :class:`.Variant` use cases.

The following example illustrates a :class:`.TypeDecorator` that applies SQL-time conversion
functions to a :class:`.LargeBinary`.   In order for this type to work in the
context of a :class:`.Variant`, the compiler needs to drill into the "impl" of the
variant expression in order to locate these methods::

    from sqlalchemy import TypeDecorator, LargeBinary, func

    class CompressedLargeBinary(TypeDecorator):
        impl = LargeBinary

        def bind_expression(self, bindvalue):
            return func.compress(bindvalue, type_=self)

        def column_expression(self, col):
            return func.uncompress(col, type_=self)

    MyLargeBinary = LargeBinary().with_variant(CompressedLargeBinary(), "sqlite")

The above expression will render a function within SQL when used on SQlite only::

    from sqlalchemy import select, column
    from sqlalchemy.dialects import sqlite
    print(select([column('x', CompressedLargeBinary)]).compile(dialect=sqlite.dialect()))

will render::

    SELECT uncompress(x) AS x

The change also includes that dialects can implement
:meth:`.TypeEngine.bind_expression` and :meth:`.TypeEngine.column_expression`
on dialect-level implementation types where they will now be used; in
particular this will be used for MySQL's new "binary prefix" requirement as
well as for casting decimal bind values for MySQL.

:ticket:`3981`

.. _change_pr467:

New last-in-first-out strategy for QueuePool
---------------------------------------------

The connection pool usually used by :func:`.create_engine` is known
as :class:`.QueuePool`.  This pool uses an object equivalent to Python's
built-in ``Queue`` class in order to store database connections waiting
to be used.   The ``Queue`` features first-in-first-out behavior, which is
intended to provide a round-robin use of the database connections that are
persistently in the pool.   However, a potential downside of this is that
when the utilization of the pool is low, the re-use of each connection in series
means that a server-side timeout strategy that attempts to reduce unused
connections is prevented from shutting down these connections.   To suit
this use case, a new flag :paramref:`.create_engine.pool_use_lifo` is added
which reverses the ``.get()`` method of the ``Queue`` to pull the connection
from the beginning of the queue instead of the end, essentially turning the
"queue" into a "stack" (adding a whole new pool called ``StackPool`` was
considered, however this was too much verbosity).

.. seealso::

    :ref:`pool_use_lifo`





Key Behavioral Changes - Core
=============================

Dialect Improvements and Changes - PostgreSQL
=============================================

.. _change_4237:

Added basic reflection support for Postgresql paritioned tables
---------------------------------------------------------------

SQLAlchemy can render the "PARTITION BY" sequnce within a Postgresql
CREATE TABLE statement using the flag ``postgresql_partition_by``, added in
version 1.2.6.    However, the ``'p'`` type was not part of the reflection
queries used until now.

Given a schema such as::

    dv = Table(
        'data_values', metadata,
        Column('modulus', Integer, nullable=False),
        Column('data', String(30)),
        postgresql_partition_by='range(modulus)')

    sa.event.listen(
        dv,
        "after_create",
        sa.DDL(
            "CREATE TABLE data_values_4_10 PARTITION OF data_values "
            "FOR VALUES FROM (4) TO (10)")
    )

The two table names ``'data_values'`` and ``'data_values_4_10'`` will come
back from :meth:`.Inspector.get_table_names` and additionally the columns
will come back from ``Inspector.get_columns('data_values')`` as well
as ``Inspector.get_columns('data_values_4_10')``.   This also extends to the
use of ``Table(..., autoload=True)`` with these tables.


:ticket:`4237`


Dialect Improvements and Changes - MySQL
=============================================

.. _change_mysql_ping:

Protocol-level ping now used for pre-ping
------------------------------------------

The MySQL dialects including mysqlclient, python-mysql, PyMySQL and
mysql-connector-python now use the ``connection.ping()`` method for the
pool pre-ping feature, described at :ref:`pool_disconnects_pessimistic`.
This is a much more lightweight ping than the previous method of emitting
"SELECT 1" on the connection.

.. _change_mysql_ondupordering:

Control of parameter ordering within ON DUPLICATE KEY UPDATE
------------------------------------------------------------

The order of UPDATE parameters in the ``ON DUPLICATE KEY UPDATE`` clause
can now be explcitly ordered by passing a list of 2-tuples::

    from sqlalchemy.dialects.mysql import insert

    insert_stmt = insert(my_table).values(
        id='some_existing_id',
        data='inserted value')

    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
        [
            ("data", "some data"),
            ("updated_at", func.current_timestamp()),
        ],
    )

.. seealso::

    :ref:`mysql_insert_on_duplicate_key_update`

Dialect Improvements and Changes - SQLite
=============================================

.. _change_3850:

Support for SQLite JSON Added
-----------------------------

A new datatype :class:`.sqlite.JSON` is added which implements SQLite's json
member access functions on behalf of the :class:`.types.JSON`
base datatype.  The SQLite ``JSON_EXTRACT`` and ``JSON_QUOTE`` functions
are used by the implementation to provide basic JSON support.

Note that the name of the datatype itself as rendered in the database is
the name "JSON".   This will create a SQLite datatype with "numeric" affinity,
which normally should not be an issue except in the case of a JSON value that
consists of single integer value.  Nevertheless, following an example
in SQLite's own documentation at https://www.sqlite.org/json1.html the name
JSON is being used for its familiarity.


:ticket:`3850`


Dialect Improvements and Changes - Oracle
=============================================

.. _change_4242:

National char datatypes de-emphasized for generic unicode, re-enabled with option
---------------------------------------------------------------------------------

The :class:`.Unicode` and :class:`.UnicodeText` datatypes by default now
correspond to the ``VARCHAR2`` and ``CLOB`` datatypes on Oracle, rather than
``NVARCHAR2`` and ``NCLOB`` (otherwise known as "national" character set
types).  This will be seen in behaviors such  as that of how they render in
``CREATE TABLE`` statements, as well as that no type object will be passed to
``setinputsizes()`` when bound parameters using :class:`.Unicode` or
:class:`.UnicodeText` are used; cx_Oracle handles the string value natively.
This change is based on advice from cx_Oracle's maintainer that the "national"
datatypes in Oracle are largely obsolete and are not performant.   They also
interfere in some situations such as when applied to the format specifier for
functions like ``trunc()``.

The one case where ``NVARCHAR2`` and related types may be needed is for a
database that is not using a Unicode-compliant character set.  In this case,
the flag ``use_nchar_for_unicode`` can be passed to :func:`.create_engine` to
re-enable the old behavior.

As always, using the :class:`.oracle.NVARCHAR2` and :class:`.oracle.NCLOB`
datatypes explicitly will continue to make use of ``NVARCHAR2`` and ``NCLOB``,
including within DDL as well as when handling bound parameters with cx_Oracle's
``setinputsizes()``.

On the read side, automatic Unicode conversion under Python 2 has been added to
CHAR/VARCHAR/CLOB result rows, to match the behavior of cx_Oracle under Python
3.  In order to mitigate the performance hit that the cx_Oracle dialect  had
previously with this behavior under Python 2, SQLAlchemy's very performant
(when C extensions are built) native Unicode handlers are used under Python 2.
The automatic unicode coercion can be disabled by setting the
``coerce_to_unicode`` flag to False. This flag now defaults to True and applies
to all string data returned in a result set that isn't explicitly under
:class:`.Unicode` or Oracle's NVARCHAR2/NCHAR/NCLOB datatypes.

:ticket:`4242`

Dialect Improvements and Changes - SQL Server
=============================================

.. _change_4158:

Support for pyodbc fast_executemany
-----------------------------------

Pyodbc's recently added "fast_executemany" mode, available when using the
Microsoft ODBC driver, is now an option for the pyodbc / mssql dialect.
Pass it via :func:`.create_engine`::

    engine = create_engine(
        "mssql+pyodbc://scott:tiger@mssql2017:1433/test?driver=ODBC+Driver+13+for+SQL+Server",
        fast_executemany=True)

.. seealso::

    :ref:`mssql_pyodbc_fastexecutemany`


:ticket:`4158`
