==============================
What's New in SQLAlchemy 0.9?
==============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 0.8,
    undergoing maintenance releases as of May, 2013,
    and SQLAlchemy version 0.9, which had its first production
    release on December 30, 2013.

    Document last updated: June 10, 2015

Introduction
============

This guide introduces what's new in SQLAlchemy version 0.9,
and also documents changes which affect users migrating
their applications from the 0.8 series of SQLAlchemy to 0.9.

Please carefully review
:ref:`behavioral_changes_orm_09` and :ref:`behavioral_changes_core_09` for
potentially backwards-incompatible changes.

Platform Support
================

Targeting Python 2.6 and Up Now, Python 3 without 2to3
-------------------------------------------------------

The first achievement of the 0.9 release is to remove the dependency
on the 2to3 tool for Python 3 compatibility.  To make this
more straightforward, the lowest Python release targeted now
is 2.6, which features a wide degree of cross-compatibility with
Python 3.   All SQLAlchemy modules and unit tests are now interpreted
equally well with any Python interpreter from 2.6 forward, including
the 3.1 and 3.2 interpreters.

:ticket:`2671`

C Extensions Supported on Python 3
-----------------------------------

The C extensions have been ported to support Python 3 and now build
in both Python 2 and Python 3 environments.

:ticket:`2161`

.. _behavioral_changes_orm_09:

Behavioral Changes - ORM
========================

.. _migration_2824:

Composite attributes are now returned as their object form when queried on a per-attribute basis
------------------------------------------------------------------------------------------------

Using a :class:`.Query` in conjunction with a composite attribute now returns the object
type maintained by that composite, rather than being broken out into individual
columns.   Using the mapping setup at :ref:`mapper_composite`::

    >>> session.query(Vertex.start, Vertex.end).\
    ...     filter(Vertex.start == Point(3, 4)).all()
    [(Point(x=3, y=4), Point(x=5, y=6))]

This change is backwards-incompatible with code that expects the individual attribute
to be expanded into individual columns.  To get that behavior, use the ``.clauses``
accessor::


    >>> session.query(Vertex.start.clauses, Vertex.end.clauses).\
    ...     filter(Vertex.start == Point(3, 4)).all()
    [(3, 4, 5, 6)]

.. seealso::

    :ref:`change_2824`

:ticket:`2824`


.. _migration_2736:

:meth:`.Query.select_from` no longer applies the clause to corresponding entities
---------------------------------------------------------------------------------

The :meth:`.Query.select_from` method has been popularized in recent versions
as a means of controlling the first thing that a :class:`.Query` object
"selects from", typically for the purposes of controlling how a JOIN will
render.

Consider the following example against the usual ``User`` mapping::

    select_stmt = select([User]).where(User.id == 7).alias()

    q = session.query(User).\
               join(select_stmt, User.id == select_stmt.c.id).\
               filter(User.name == 'ed')

The above statement predictably renders SQL like the following::

    SELECT "user".id AS user_id, "user".name AS user_name
    FROM "user" JOIN (SELECT "user".id AS id, "user".name AS name
    FROM "user"
    WHERE "user".id = :id_1) AS anon_1 ON "user".id = anon_1.id
    WHERE "user".name = :name_1

If we wanted to reverse the order of the left and right elements of the
JOIN, the documentation would lead us to believe we could use
:meth:`.Query.select_from` to do so::

    q = session.query(User).\
            select_from(select_stmt).\
            join(User, User.id == select_stmt.c.id).\
            filter(User.name == 'ed')

However, in version 0.8 and earlier, the above use of :meth:`.Query.select_from`
would apply the ``select_stmt`` to **replace** the ``User`` entity, as it
selects from the ``user`` table which is compatible with ``User``::

    -- SQLAlchemy 0.8 and earlier...
    SELECT anon_1.id AS anon_1_id, anon_1.name AS anon_1_name
    FROM (SELECT "user".id AS id, "user".name AS name
    FROM "user"
    WHERE "user".id = :id_1) AS anon_1 JOIN "user" ON anon_1.id = anon_1.id
    WHERE anon_1.name = :name_1

The above statement is a mess, the ON clause refers ``anon_1.id = anon_1.id``,
our WHERE clause has been replaced with ``anon_1`` as well.

This behavior is quite intentional, but has a different use case from that
which has become popular for :meth:`.Query.select_from`.  The above behavior
is now available by a new method known as :meth:`.Query.select_entity_from`.
This is a lesser used behavior that in modern SQLAlchemy is roughly equivalent
to selecting from a customized :func:`.aliased` construct::

    select_stmt = select([User]).where(User.id == 7)
    user_from_stmt = aliased(User, select_stmt.alias())

    q = session.query(user_from_stmt).filter(user_from_stmt.name == 'ed')

So with SQLAlchemy 0.9, our query that selects from ``select_stmt`` produces
the SQL we expect::

    -- SQLAlchemy 0.9
    SELECT "user".id AS user_id, "user".name AS user_name
    FROM (SELECT "user".id AS id, "user".name AS name
    FROM "user"
    WHERE "user".id = :id_1) AS anon_1 JOIN "user" ON "user".id = id
    WHERE "user".name = :name_1

The :meth:`.Query.select_entity_from` method will be available in SQLAlchemy
**0.8.2**, so applications which rely on the old behavior can transition
to this method first, ensure all tests continue to function, then upgrade
to 0.9 without issue.

:ticket:`2736`


.. _migration_2833:

``viewonly=True`` on ``relationship()`` prevents history from taking effect
---------------------------------------------------------------------------

The ``viewonly`` flag on :func:`.relationship` is applied to prevent changes
to the target attribute from having any effect within the flush process.
This is achieved by eliminating the attribute from being considered during
the flush.  However, up until now, changes to the attribute would still
register the parent object as "dirty" and trigger a potential flush.  The change
is that the ``viewonly`` flag now prevents history from being set for the
target attribute as well.  Attribute events like backrefs and user-defined events
still continue to function normally.

The change is illustrated as follows::

    from sqlalchemy import Column, Integer, ForeignKey, create_engine
    from sqlalchemy.orm import backref, relationship, Session
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import inspect

    Base = declarative_base()

    class A(Base):
        __tablename__ = 'a'
        id = Column(Integer, primary_key=True)

    class B(Base):
        __tablename__ = 'b'

        id = Column(Integer, primary_key=True)
        a_id = Column(Integer, ForeignKey('a.id'))
        a = relationship("A", backref=backref("bs", viewonly=True))

    e = create_engine("sqlite://")
    Base.metadata.create_all(e)

    a = A()
    b = B()

    sess = Session(e)
    sess.add_all([a, b])
    sess.commit()

    b.a = a

    assert b in sess.dirty

    # before 0.9.0
    # assert a in sess.dirty
    # assert inspect(a).attrs.bs.history.has_changes()

    # after 0.9.0
    assert a not in sess.dirty
    assert not inspect(a).attrs.bs.history.has_changes()

:ticket:`2833`

.. _migration_2751:

Association Proxy SQL Expression Improvements and Fixes
-------------------------------------------------------

The ``==`` and ``!=`` operators as implemented by an association proxy
that refers to a scalar value on a scalar relationship now produces
a more complete SQL expression, intended to take into account
the "association" row being present or not when the comparison is against
``None``.

Consider this mapping::

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)

        b_id = Column(Integer, ForeignKey('b.id'), primary_key=True)
        b = relationship("B")
        b_value = association_proxy("b", "value")

    class B(Base):
        __tablename__ = 'b'
        id = Column(Integer, primary_key=True)
        value = Column(String)

Up through 0.8, a query like the following::

    s.query(A).filter(A.b_value == None).all()

would produce::

    SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a
    WHERE EXISTS (SELECT 1
    FROM b
    WHERE b.id = a.b_id AND b.value IS NULL)

In 0.9, it now produces::

    SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a
    WHERE (EXISTS (SELECT 1
    FROM b
    WHERE b.id = a.b_id AND b.value IS NULL)) OR a.b_id IS NULL

The difference being, it not only checks ``b.value``, it also checks
if ``a`` refers to no ``b`` row at all.  This will return different
results versus prior versions, for a system that uses this type of
comparison where some parent rows have no association row.

More critically, a correct expression is emitted for ``A.b_value != None``.
In 0.8, this would return ``True`` for ``A`` rows that had no ``b``::

    SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a
    WHERE NOT (EXISTS (SELECT 1
    FROM b
    WHERE b.id = a.b_id AND b.value IS NULL))

Now in 0.9, the check has been reworked so that it ensures
the A.b_id row is present, in addition to ``B.value`` being
non-NULL::

    SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a
    WHERE EXISTS (SELECT 1
    FROM b
    WHERE b.id = a.b_id AND b.value IS NOT NULL)

In addition, the ``has()`` operator is enhanced such that you can
call it against a scalar column value with no criterion only,
and it will produce criteria that checks for the association row
being present or not::

    s.query(A).filter(A.b_value.has()).all()

output::

    SELECT a.id AS a_id, a.b_id AS a_b_id
    FROM a
    WHERE EXISTS (SELECT 1
    FROM b
    WHERE b.id = a.b_id)

This is equivalent to ``A.b.has()``, but allows one to query
against ``b_value`` directly.

:ticket:`2751`

.. _migration_2810:

Association Proxy Missing Scalar returns None
---------------------------------------------

An association proxy from a scalar attribute to a scalar will now return
``None`` if the proxied object isn't present.  This is consistent with the
fact that missing many-to-ones return None in SQLAlchemy, so should the
proxied value.  E.g.::

    from sqlalchemy import *
    from sqlalchemy.orm import *
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.ext.associationproxy import association_proxy

    Base = declarative_base()

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)
        b = relationship("B", uselist=False)

        bname = association_proxy("b", "name")

    class B(Base):
        __tablename__ = 'b'

        id = Column(Integer, primary_key=True)
        a_id = Column(Integer, ForeignKey('a.id'))
        name = Column(String)

    a1 = A()

    # this is how m2o's always have worked
    assert a1.b is None

    # but prior to 0.9, this would raise AttributeError,
    # now returns None just like the proxied value.
    assert a1.bname is None

:ticket:`2810`


.. _change_2787:

attributes.get_history() will query from the DB by default if value not present
-------------------------------------------------------------------------------

A bugfix regarding :func:`.attributes.get_history` allows a column-based attribute
to query out to the database for an unloaded value, assuming the ``passive``
flag is left at its default of ``PASSIVE_OFF``.  Previously, this flag would
not be honored.  Additionally, a new method :meth:`.AttributeState.load_history`
is added to complement the :attr:`.AttributeState.history` attribute, which
will emit loader callables for an unloaded attribute.

This is a small change demonstrated as follows::

    from sqlalchemy import Column, Integer, String, create_engine, inspect
    from sqlalchemy.orm import Session, attributes
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class A(Base):
        __tablename__ = 'a'
        id = Column(Integer, primary_key=True)
        data = Column(String)

    e = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(e)

    sess = Session(e)

    a1 = A(data='a1')
    sess.add(a1)
    sess.commit()  # a1 is now expired

    # history doesn't emit loader callables
    assert inspect(a1).attrs.data.history == (None, None, None)

    # in 0.8, this would fail to load the unloaded state.
    assert attributes.get_history(a1, 'data') == ((), ['a1',], ())

    # load_history() is now equivalent to get_history() with
    # passive=PASSIVE_OFF ^ INIT_OK
    assert inspect(a1).attrs.data.load_history() == ((), ['a1',], ())

:ticket:`2787`

.. _behavioral_changes_core_09:

Behavioral Changes - Core
=========================

Type objects no longer accept ignored keyword arguments
-------------------------------------------------------

Up through the 0.8 series, most type objects accepted arbitrary keyword
arguments which were silently ignored::

    from sqlalchemy import Date, Integer

    # storage_format argument here has no effect on any backend;
    # it needs to be on the SQLite-specific type
    d = Date(storage_format="%(day)02d.%(month)02d.%(year)04d")

    # display_width argument here has no effect on any backend;
    # it needs to be on the MySQL-specific type
    i = Integer(display_width=5)

This was a very old bug for which a deprecation warning was added to the
0.8 series, but because nobody ever runs Python with the "-W" flag, it
was mostly never seen::


    $ python -W always::DeprecationWarning ~/dev/sqlalchemy/test.py
    /Users/classic/dev/sqlalchemy/test.py:5: SADeprecationWarning: Passing arguments to
    type object constructor <class 'sqlalchemy.types.Date'> is deprecated
      d = Date(storage_format="%(day)02d.%(month)02d.%(year)04d")
    /Users/classic/dev/sqlalchemy/test.py:9: SADeprecationWarning: Passing arguments to
    type object constructor <class 'sqlalchemy.types.Integer'> is deprecated
      i = Integer(display_width=5)

As of the 0.9 series the "catch all" constructor is removed from
:class:`.TypeEngine`, and these meaningless arguments are no longer accepted.

The correct way to make use of dialect-specific arguments such as
``storage_format`` and ``display_width`` is to use the appropriate
dialect-specific types::

    from sqlalchemy.dialects.sqlite import DATE
    from sqlalchemy.dialects.mysql import INTEGER

    d = DATE(storage_format="%(day)02d.%(month)02d.%(year)04d")

    i = INTEGER(display_width=5)

What about the case where we want the dialect-agnostic type also?  We
use the :meth:`.TypeEngine.with_variant` method::

    from sqlalchemy import Date, Integer
    from sqlalchemy.dialects.sqlite import DATE
    from sqlalchemy.dialects.mysql import INTEGER

    d = Date().with_variant(
            DATE(storage_format="%(day)02d.%(month)02d.%(year)04d"),
            "sqlite"
        )

    i = Integer().with_variant(
            INTEGER(display_width=5),
            "mysql"
        )

:meth:`.TypeEngine.with_variant` isn't new, it was added in SQLAlchemy
0.7.2.  So code that is running on the 0.8 series can be corrected to use
this approach and tested before upgrading to 0.9.

``None`` can no longer be used as a "partial AND" constructor
--------------------------------------------------------------

``None`` can no longer be used as the "backstop" to form an AND condition piecemeal.
This pattern was not a documented pattern even though some SQLAlchemy internals
made use of it::

    condition = None

    for cond in conditions:
        condition = condition & cond

    if condition is not None:
        stmt = stmt.where(condition)

The above sequence, when ``conditions`` is non-empty, will on 0.9 produce
``SELECT .. WHERE <condition> AND NULL``.  The ``None`` is no longer implicitly
ignored, and is instead consistent with when ``None`` is interpreted in other
contexts besides that of a conjunction.

The correct code for both 0.8 and 0.9 should read::

    from sqlalchemy.sql import and_

    if conditions:
        stmt = stmt.where(and_(*conditions))

Another variant that works on all backends on 0.9, but on 0.8 only works on
backends that support boolean constants::

    from sqlalchemy.sql import true

    condition = true()

    for cond in conditions:
        condition = cond & condition

    stmt = stmt.where(condition)

On 0.8, this will produce a SELECT statement that always has ``AND true``
in the WHERE clause, which is not accepted by backends that don't support
boolean constants (MySQL, MSSQL).  On 0.9, the ``true`` constant will be dropped
within an ``and_()`` conjunction.

.. seealso::

    :ref:`migration_2804`

.. _migration_2873:

The "password" portion of a ``create_engine()`` no longer considers the ``+`` sign as an encoded space
------------------------------------------------------------------------------------------------------

For whatever reason, the Python function ``unquote_plus()`` was applied to the
"password" field of a URL, which is an incorrect application of the
encoding rules described in `RFC 1738 <http://www.ietf.org/rfc/rfc1738.txt>`_
in that it escaped spaces as plus signs.  The stringiciation of a URL
now only encodes ":", "@", or "/" and nothing else, and is now applied to both the
``username`` and ``password`` fields (previously it only applied to the
password).   On parsing, encoded characters are converted, but plus signs and
spaces are passed through as is::

    # password: "pass word + other:words"
    dbtype://user:pass word + other%3Awords@host/dbname

    # password: "apples/oranges"
    dbtype://username:apples%2Foranges@hostspec/database

    # password: "apples@oranges@@"
    dbtype://username:apples%40oranges%40%40@hostspec/database

    # password: '', username is "username@"
    dbtype://username%40:@hostspec/database


:ticket:`2873`

.. _migration_2879:

The precedence rules for COLLATE have been changed
--------------------------------------------------

Previously, an expression like the following::

    print((column('x') == 'somevalue').collate("en_EN"))

would produce an expression like this::

    -- 0.8 behavior
    (x = :x_1) COLLATE en_EN

The above is misunderstood by MSSQL and is generally not the syntax suggested
for any database.  The expression will now produce the syntax illustrated
by that of most database documentation::

    -- 0.9 behavior
    x = :x_1 COLLATE en_EN

The potentially backwards incompatible change arises if the :meth:`.collate`
operator is being applied to the right-hand column, as follows::

    print(column('x') == literal('somevalue').collate("en_EN"))

In 0.8, this produces::

    x = :param_1 COLLATE en_EN

However in 0.9, will now produce the more accurate, but probably not what you
want, form of::

    x = (:param_1 COLLATE en_EN)

The :meth:`.ColumnOperators.collate` operator now works more appropriately within an
``ORDER BY`` expression as well, as a specific precedence has been given to the
``ASC`` and ``DESC`` operators which will again ensure no parentheses are
generated::

    >>> # 0.8
    >>> print(column('x').collate('en_EN').desc())
    (x COLLATE en_EN) DESC

    >>> # 0.9
    >>> print(column('x').collate('en_EN').desc())
    x COLLATE en_EN DESC

:ticket:`2879`



.. _migration_2878:

PostgreSQL CREATE TYPE <x> AS ENUM now applies quoting to values
----------------------------------------------------------------

The :class:`.postgresql.ENUM` type will now apply escaping to single quote
signs within the enumerated values::

    >>> from sqlalchemy.dialects import postgresql
    >>> type = postgresql.ENUM('one', 'two', "three's", name="myenum")
    >>> from sqlalchemy.dialects.postgresql import base
    >>> print(base.CreateEnumType(type).compile(dialect=postgresql.dialect()))
    CREATE TYPE myenum AS ENUM ('one','two','three''s')

Existing workarounds which already escape single quote signs will need to be
modified, else they will now double-escape.

:ticket:`2878`

New Features
============

.. _feature_2268:

Event Removal API
-----------------

Events established using :func:`.event.listen` or :func:`.event.listens_for`
can now be removed using the new :func:`.event.remove` function.   The ``target``,
``identifier`` and ``fn`` arguments sent to :func:`.event.remove` need to match
exactly those which were sent for listening, and the event will be removed
from all locations in which it had been established::

    @event.listens_for(MyClass, "before_insert", propagate=True)
    def my_before_insert(mapper, connection, target):
        """listen for before_insert"""
        # ...

    event.remove(MyClass, "before_insert", my_before_insert)

In the example above, the ``propagate=True`` flag is set.  This
means ``my_before_insert()`` is established as a listener for ``MyClass``
as well as all subclasses of ``MyClass``.
The system tracks everywhere that the ``my_before_insert()``
listener function had been placed as a result of this call and removes it as
a result of calling :func:`.event.remove`.

The removal system uses a registry to associate arguments passed to
:func:`.event.listen` with collections of event listeners, which are in many
cases wrapped versions of the original user-supplied function.   This registry
makes heavy use of weak references in order to allow all the contained contents,
such as listener targets, to be garbage collected when they go out of scope.

:ticket:`2268`

.. _feature_1418:

New Query Options API; ``load_only()`` option
---------------------------------------------

The system of loader options such as :func:`.orm.joinedload`,
:func:`.orm.subqueryload`, :func:`.orm.lazyload`, :func:`.orm.defer`, etc.
all build upon a new system known as :class:`.Load`.  :class:`.Load` provides
a "method chained" (a.k.a. :term:`generative`) approach to loader options, so that
instead of joining together long paths using dots or multiple attribute names,
an explicit loader style is given for each path.

While the new way is slightly more verbose, it is simpler to understand
in that there is no ambiguity in what options are being applied to which paths;
it simplifies the method signatures of the options and provides greater flexibility
particularly for column-based options.  The old systems are to remain functional
indefinitely as well and all styles can be mixed.

**Old Way**

To set a certain style of loading along every link in a multi-element path, the ``_all()``
option has to be used::

    query(User).options(joinedload_all("orders.items.keywords"))

**New Way**

Loader options are now chainable, so the same ``joinedload(x)`` method is applied
equally to each link, without the need to keep straight between
:func:`.joinedload` and :func:`.joinedload_all`::

    query(User).options(joinedload("orders").joinedload("items").joinedload("keywords"))

**Old Way**

Setting an option on path that is based on a subclass requires that all
links in the path be spelled out as class bound attributes, since the
:meth:`.PropComparator.of_type` method needs to be called::

    session.query(Company).\
        options(
            subqueryload_all(
                Company.employees.of_type(Engineer),
                Engineer.machines
            )
        )

**New Way**

Only those elements in the path that actually need :meth:`.PropComparator.of_type`
need to be set as a class-bound attribute, string-based names can be resumed
afterwards::

    session.query(Company).\
        options(
            subqueryload(Company.employees.of_type(Engineer)).
            subqueryload("machines")
            )
        )

**Old Way**

Setting the loader option on the last link in a long path uses a syntax
that looks a lot like it should be setting the option for all links in the
path, causing confusion::

    query(User).options(subqueryload("orders.items.keywords"))

**New Way**

A path can now be spelled out using :func:`.defaultload` for entries in the
path where the existing loader style should be unchanged.  More verbose
but the intent is clearer::

    query(User).options(defaultload("orders").defaultload("items").subqueryload("keywords"))


The dotted style can still be taken advantage of, particularly in the case
of skipping over several path elements::

    query(User).options(defaultload("orders.items").subqueryload("keywords"))

**Old Way**

The :func:`.defer` option on a path needed to be spelled out with the full
path for each column::

    query(User).options(defer("orders.description"), defer("orders.isopen"))

**New Way**

A single :class:`.Load` object that arrives at the target path can have
:meth:`.Load.defer` called upon it repeatedly::

    query(User).options(defaultload("orders").defer("description").defer("isopen"))

The Load Class
^^^^^^^^^^^^^^^

The :class:`.Load` class can be used directly to provide a "bound" target,
especially when multiple parent entities are present::

    from sqlalchemy.orm import Load

    query(User, Address).options(Load(Address).joinedload("entries"))

Load Only
^^^^^^^^^

A new option :func:`.load_only` achieves a "defer everything but" style of load,
loading only the given columns and deferring the rest::

    from sqlalchemy.orm import load_only

    query(User).options(load_only("name", "fullname"))

    # specify explicit parent entity
    query(User, Address).options(Load(User).load_only("name", "fullname"))

    # specify path
    query(User).options(joinedload(User.addresses).load_only("email_address"))

Class-specific Wildcards
^^^^^^^^^^^^^^^^^^^^^^^^^

Using :class:`.Load`, a wildcard may be used to set the loading for all
relationships (or perhaps columns) on a given entity, without affecting any
others::

    # lazyload all User relationships
    query(User).options(Load(User).lazyload("*"))

    # undefer all User columns
    query(User).options(Load(User).undefer("*"))

    # lazyload all Address relationships
    query(User).options(defaultload(User.addresses).lazyload("*"))

    # undefer all Address columns
    query(User).options(defaultload(User.addresses).undefer("*"))


:ticket:`1418`


.. _feature_2877:

New ``text()`` Capabilities
---------------------------

The :func:`.text` construct gains new methods:

* :meth:`.TextClause.bindparams` allows bound parameter types and values
  to be set flexibly::

      # setup values
      stmt = text("SELECT id, name FROM user "
            "WHERE name=:name AND timestamp=:timestamp").\
            bindparams(name="ed", timestamp=datetime(2012, 11, 10, 15, 12, 35))

      # setup types and/or values
      stmt = text("SELECT id, name FROM user "
            "WHERE name=:name AND timestamp=:timestamp").\
            bindparams(
                bindparam("name", value="ed"),
                bindparam("timestamp", type_=DateTime()
            ).bindparam(timestamp=datetime(2012, 11, 10, 15, 12, 35))

* :meth:`.TextClause.columns` supersedes the ``typemap`` option
  of :func:`.text`, returning a new construct :class:`.TextAsFrom`::

      # turn a text() into an alias(), with a .c. collection:
      stmt = text("SELECT id, name FROM user").columns(id=Integer, name=String)
      stmt = stmt.alias()

      stmt = select([addresses]).select_from(
                    addresses.join(stmt), addresses.c.user_id == stmt.c.id)


      # or into a cte():
      stmt = text("SELECT id, name FROM user").columns(id=Integer, name=String)
      stmt = stmt.cte("x")

      stmt = select([addresses]).select_from(
                    addresses.join(stmt), addresses.c.user_id == stmt.c.id)

:ticket:`2877`

.. _feature_722:

INSERT from SELECT
------------------

After literally years of pointless procrastination this relatively minor
syntactical feature has been added, and is also backported to 0.8.3,
so technically isn't "new" in 0.9.   A :func:`.select` construct or other
compatible construct can be passed to the new method :meth:`.Insert.from_select`
where it will be used to render an ``INSERT .. SELECT`` construct::

    >>> from sqlalchemy.sql import table, column
    >>> t1 = table('t1', column('a'), column('b'))
    >>> t2 = table('t2', column('x'), column('y'))
    >>> print(t1.insert().from_select(['a', 'b'], t2.select().where(t2.c.y == 5)))
    INSERT INTO t1 (a, b) SELECT t2.x, t2.y
    FROM t2
    WHERE t2.y = :y_1

The construct is smart enough to also accommodate ORM objects such as classes
and :class:`.Query` objects::

    s = Session()
    q = s.query(User.id, User.name).filter_by(name='ed')
    ins = insert(Address).from_select((Address.id, Address.email_address), q)

rendering::

    INSERT INTO addresses (id, email_address)
    SELECT users.id AS users_id, users.name AS users_name
    FROM users WHERE users.name = :name_1

:ticket:`722`

.. _feature_github_42:

New FOR UPDATE support on ``select()``, ``Query()``
---------------------------------------------------

An attempt is made to simplify the specification of the ``FOR UPDATE``
clause on ``SELECT`` statements made within Core and ORM, and support is added
for the ``FOR UPDATE OF`` SQL supported by PostgreSQL and Oracle.

Using the core :meth:`.GenerativeSelect.with_for_update`, options like ``FOR SHARE`` and
``NOWAIT`` can be specified individually, rather than linking to arbitrary
string codes::

    stmt = select([table]).with_for_update(read=True, nowait=True, of=table)

On Posgtresql the above statement might render like::

    SELECT table.a, table.b FROM table FOR SHARE OF table NOWAIT

The :class:`.Query` object gains a similar method :meth:`.Query.with_for_update`
which behaves in the same way.  This method supersedes the existing
:meth:`.Query.with_lockmode` method, which translated ``FOR UPDATE`` clauses
using a different system.   At the moment, the "lockmode" string argument is still
accepted by the :meth:`.Session.refresh` method.


.. _feature_2867:

Floating Point String-Conversion Precision Configurable for Native Floating Point Types
---------------------------------------------------------------------------------------

The conversion which SQLAlchemy does whenever a DBAPI returns a Python
floating point type which is to be converted into a Python ``Decimal()``
necessarily involves an intermediary step which converts the floating point
value to a string.  The scale used for this string conversion was previously
hardcoded to 10, and is now configurable.  The setting is available on
both the :class:`.Numeric` as well as the :class:`.Float`
type, as well as all SQL- and dialect-specific descendant types, using the
parameter ``decimal_return_scale``.    If the type supports a ``.scale`` parameter,
as is the case with :class:`.Numeric` and some float types such as
:class:`.mysql.DOUBLE`, the value of ``.scale`` is used as the default
for ``.decimal_return_scale`` if it is not otherwise specified.   If both
``.scale`` and ``.decimal_return_scale`` are absent, then the default of
10 takes place.  E.g.::

    from sqlalchemy.dialects.mysql import DOUBLE
    import decimal

    data = Table('data', metadata,
        Column('double_value',
                    mysql.DOUBLE(decimal_return_scale=12, asdecimal=True))
    )

    conn.execute(
        data.insert(),
        double_value=45.768392065789,
    )
    result = conn.scalar(select([data.c.double_value]))

    # previously, this would typically be Decimal("45.7683920658"),
    # e.g. trimmed to 10 decimal places

    # now we get 12, as requested, as MySQL can support this
    # much precision for DOUBLE
    assert result == decimal.Decimal("45.768392065789")


:ticket:`2867`


.. _change_2824:

Column Bundles for ORM queries
------------------------------

The :class:`.Bundle` allows for querying of sets of columns, which are then
grouped into one name under the tuple returned by the query.  The initial
purposes of :class:`.Bundle` are 1. to allow "composite" ORM columns to be
returned as a single value in a column-based result set, rather than expanding
them out into individual columns and 2. to allow the creation of custom result-set
constructs within the ORM, using ad-hoc columns and return types, without involving
the more heavyweight mechanics of mapped classes.

.. seealso::

    :ref:`migration_2824`

    :ref:`bundles`

:ticket:`2824`


Server Side Version Counting
-----------------------------

The versioning feature of the ORM (now also documented at :ref:`mapper_version_counter`)
can now make use of server-side version counting schemes, such as those produced
by triggers or database system columns, as well as conditional programmatic schemes outside
of the version_id_counter function itself.  By providing the value ``False``
to the ``version_id_generator`` parameter, the ORM will use the already-set version
identifier, or alternatively fetch the version identifier
from each row at the same time the INSERT or UPDATE is emitted.   When using a
server-generated version identifier, it is strongly
recommended that this feature be used only on a backend with strong RETURNING
support (PostgreSQL, SQL Server; Oracle also supports RETURNING but the cx_oracle
driver has only limited support), else the additional SELECT statements will
add significant performance
overhead.   The example provided at :ref:`server_side_version_counter` illustrates
the usage of the PostgreSQL ``xmin`` system column in order to integrate it with
the ORM's versioning feature.

.. seealso::

    :ref:`server_side_version_counter`

:ticket:`2793`

.. _feature_1535:

``include_backrefs=False`` option for ``@validates``
----------------------------------------------------

The :func:`.validates` function now accepts an option ``include_backrefs=True``,
which will bypass firing the validator for the case where the event initiated
from a backref::

    from sqlalchemy import Column, Integer, ForeignKey
    from sqlalchemy.orm import relationship, validates
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)
        bs = relationship("B", backref="a")

        @validates("bs")
        def validate_bs(self, key, item):
            print("A.bs validator")
            return item

    class B(Base):
        __tablename__ = 'b'

        id = Column(Integer, primary_key=True)
        a_id = Column(Integer, ForeignKey('a.id'))

        @validates("a", include_backrefs=False)
        def validate_a(self, key, item):
            print("B.a validator")
            return item

    a1 = A()
    a1.bs.append(B())  # prints only "A.bs validator"


:ticket:`1535`


PostgreSQL JSON Type
--------------------

The PostgreSQL dialect now features a :class:`.postgresql.JSON` type to
complement the :class:`.postgresql.HSTORE` type.

.. seealso::

    :class:`.postgresql.JSON`

:ticket:`2581`

.. _feature_automap:

Automap Extension
-----------------

A new extension is added in **0.9.1** known as :mod:`sqlalchemy.ext.automap`.  This is an
**experimental** extension which expands upon the functionality of Declarative
as well as the :class:`.DeferredReflection` class.  Essentially, the extension
provides a base class :class:`.AutomapBase` which automatically generates
mapped classes and relationships between them based on given table metadata.

The :class:`.MetaData` in use normally might be produced via reflection, but
there is no requirement that reflection is used.   The most basic usage
illustrates how :mod:`sqlalchemy.ext.automap` is able to deliver mapped
classes, including relationships, based on a reflected schema::

    from sqlalchemy.ext.automap import automap_base
    from sqlalchemy.orm import Session
    from sqlalchemy import create_engine

    Base = automap_base()

    # engine, suppose it has two tables 'user' and 'address' set up
    engine = create_engine("sqlite:///mydatabase.db")

    # reflect the tables
    Base.prepare(engine, reflect=True)

    # mapped classes are now created with names matching that of the table
    # name.
    User = Base.classes.user
    Address = Base.classes.address

    session = Session(engine)

    # rudimentary relationships are produced
    session.add(Address(email_address="foo@bar.com", user=User(name="foo")))
    session.commit()

    # collection-based relationships are by default named "<classname>_collection"
    print(u1.address_collection)

Beyond that, the :class:`.AutomapBase` class is a declarative base, and supports
all the features that declarative does.  The "automapping" feature can be used
with an existing, explicitly declared schema to generate relationships and
missing classes only.  Naming schemes and relationship-production routines
can be dropped in using callable functions.

It is hoped that the :class:`.AutomapBase` system provides a quick
and modernized solution to the problem that the very famous
`SQLSoup <https://sqlsoup.readthedocs.io/en/latest/>`_
also tries to solve, that of generating a quick and rudimentary object
model from an existing database on the fly.  By addressing the issue strictly
at the mapper configuration level, and integrating fully with existing
Declarative class techniques, :class:`.AutomapBase` seeks to provide
a well-integrated approach to the issue of expediently auto-generating ad-hoc
mappings.

.. seealso::

    :ref:`automap_toplevel`

Behavioral Improvements
=======================

Improvements that should produce no compatibility issues except in exceedingly
rare and unusual hypothetical cases, but are good to be aware of in case there are
unexpected issues.

.. _feature_joins_09:

Many JOIN and LEFT OUTER JOIN expressions will no longer be wrapped in (SELECT * FROM ..) AS ANON_1
---------------------------------------------------------------------------------------------------

For many years, the SQLAlchemy ORM has been held back from being able to nest
a JOIN inside the right side of an existing JOIN (typically a LEFT OUTER JOIN,
as INNER JOINs could always be flattened)::

    SELECT a.*, b.*, c.* FROM a LEFT OUTER JOIN (b JOIN c ON b.id = c.id) ON a.id

This was due to the fact that SQLite up until version **3.7.16** cannot parse a statement of the above format::

    SQLite version 3.7.15.2 2013-01-09 11:53:05
    Enter ".help" for instructions
    Enter SQL statements terminated with a ";"
    sqlite> create table a(id integer);
    sqlite> create table b(id integer);
    sqlite> create table c(id integer);
    sqlite> select a.id, b.id, c.id from a left outer join (b join c on b.id=c.id) on b.id=a.id;
    Error: no such column: b.id

Right-outer-joins are of course another way to work around right-side
parenthesization; this would be significantly complicated and visually unpleasant
to implement, but fortunately SQLite doesn't support RIGHT OUTER JOIN either :)::

    sqlite> select a.id, b.id, c.id from b join c on b.id=c.id
       ...> right outer join a on b.id=a.id;
    Error: RIGHT and FULL OUTER JOINs are not currently supported

Back in 2005, it wasn't clear if other databases had trouble with this form,
but today it seems clear every database tested except SQLite now supports it
(Oracle 8, a very old database, doesn't support the JOIN keyword at all,
but SQLAlchemy has always had a simple rewriting scheme in place for Oracle's syntax).
To make matters worse, SQLAlchemy's usual workaround of applying a
SELECT often degrades performance on platforms like PostgreSQL and MySQL::

    SELECT a.*, anon_1.* FROM a LEFT OUTER JOIN (
                    SELECT b.id AS b_id, c.id AS c_id
                    FROM b JOIN c ON b.id = c.id
                ) AS anon_1 ON a.id=anon_1.b_id

A JOIN like the above form is commonplace when working with joined-table inheritance structures;
any time :meth:`.Query.join` is used to join from some parent to a joined-table subclass, or
when :func:`.joinedload` is used similarly, SQLAlchemy's ORM would always make sure a nested
JOIN was never rendered, lest the query wouldn't be able to run on SQLite.  Even though
the Core has always supported a JOIN of the more compact form, the ORM had to avoid it.

An additional issue would arise when producing joins across many-to-many relationships
where special criteria is present in the ON clause. Consider an eager load join like the following::

    session.query(Order).outerjoin(Order.items)

Assuming a many-to-many from ``Order`` to ``Item`` which actually refers to a subclass
like ``Subitem``, the SQL for the above would look like::

    SELECT order.id, order.name
    FROM order LEFT OUTER JOIN order_item ON order.id = order_item.order_id
    LEFT OUTER JOIN item ON order_item.item_id = item.id AND item.type = 'subitem'

What's wrong with the above query?  Basically, that it will load many ``order`` /
``order_item`` rows where the criteria of ``item.type == 'subitem'`` is not true.

As of SQLAlchemy 0.9, an entirely new approach has been taken.  The ORM no longer
worries about nesting JOINs in the right side of an enclosing JOIN, and it now will
render these as often as possible while still returning the correct results.  When
the SQL statement is passed to be compiled, the **dialect compiler** will **rewrite the join**
to suit the target backend, if that backend is known to not support a right-nested
JOIN (which currently is only SQLite - if other backends have this issue please
let us know!).

So a regular ``query(Parent).join(Subclass)`` will now usually produce a simpler
expression::

    SELECT parent.id AS parent_id
    FROM parent JOIN (
            base_table JOIN subclass_table
            ON base_table.id = subclass_table.id) ON parent.id = base_table.parent_id

Joined eager loads like ``query(Parent).options(joinedload(Parent.subclasses))``
will alias the individual tables instead of wrapping in an ``ANON_1``::

    SELECT parent.*, base_table_1.*, subclass_table_1.* FROM parent
        LEFT OUTER JOIN (
            base_table AS base_table_1 JOIN subclass_table AS subclass_table_1
            ON base_table_1.id = subclass_table_1.id)
            ON parent.id = base_table_1.parent_id

Many-to-many joins and eagerloads will right nest the "secondary" and "right" tables::

    SELECT order.id, order.name
    FROM order LEFT OUTER JOIN
    (order_item JOIN item ON order_item.item_id = item.id AND item.type = 'subitem')
    ON order_item.order_id = order.id

All of these joins, when rendered with a :class:`.Select` statement that specifically
specifies ``use_labels=True``, which is true for all the queries the ORM emits,
are candidates for "join rewriting", which is the process of rewriting all those right-nested
joins into nested SELECT statements, while maintaining the identical labeling used by
the :class:`.Select`.  So SQLite, the one database that won't support this very
common SQL syntax even in 2013, shoulders the extra complexity itself,
with the above queries rewritten as::

    -- sqlite only!
    SELECT parent.id AS parent_id
        FROM parent JOIN (
            SELECT base_table.id AS base_table_id,
                    base_table.parent_id AS base_table_parent_id,
                    subclass_table.id AS subclass_table_id
            FROM base_table JOIN subclass_table ON base_table.id = subclass_table.id
        ) AS anon_1 ON parent.id = anon_1.base_table_parent_id

    -- sqlite only!
    SELECT parent.id AS parent_id, anon_1.subclass_table_1_id AS subclass_table_1_id,
            anon_1.base_table_1_id AS base_table_1_id,
            anon_1.base_table_1_parent_id AS base_table_1_parent_id
    FROM parent LEFT OUTER JOIN (
        SELECT base_table_1.id AS base_table_1_id,
            base_table_1.parent_id AS base_table_1_parent_id,
            subclass_table_1.id AS subclass_table_1_id
        FROM base_table AS base_table_1
        JOIN subclass_table AS subclass_table_1 ON base_table_1.id = subclass_table_1.id
    ) AS anon_1 ON parent.id = anon_1.base_table_1_parent_id

    -- sqlite only!
    SELECT "order".id AS order_id
    FROM "order" LEFT OUTER JOIN (
            SELECT order_item_1.order_id AS order_item_1_order_id,
                order_item_1.item_id AS order_item_1_item_id,
                item.id AS item_id, item.type AS item_type
    FROM order_item AS order_item_1
        JOIN item ON item.id = order_item_1.item_id AND item.type IN (?)
    ) AS anon_1 ON "order".id = anon_1.order_item_1_order_id

.. note::

    As of SQLAlchemy 1.1, the workarounds present in this feature for SQLite
    will automatically disable themselves when SQLite version **3.7.16**
    or greater is detected, as SQLite has repaired support for right-nested joins.

The :meth:`.Join.alias`, :func:`.aliased` and :func:`.with_polymorphic` functions now
support a new argument, ``flat=True``, which is used to construct aliases of joined-table
entities without embedding into a SELECT.   This flag is not on by default, to help with
backwards compatibility - but now a "polymorhpic" selectable can be joined as a target
without any subqueries generated::

    employee_alias = with_polymorphic(Person, [Engineer, Manager], flat=True)

    session.query(Company).join(
                        Company.employees.of_type(employee_alias)
                    ).filter(
                        or_(
                            Engineer.primary_language == 'python',
                            Manager.manager_name == 'dilbert'
                        )
                    )

Generates (everywhere except SQLite)::

    SELECT companies.company_id AS companies_company_id, companies.name AS companies_name
    FROM companies JOIN (
        people AS people_1
        LEFT OUTER JOIN engineers AS engineers_1 ON people_1.person_id = engineers_1.person_id
        LEFT OUTER JOIN managers AS managers_1 ON people_1.person_id = managers_1.person_id
    ) ON companies.company_id = people_1.company_id
    WHERE engineers.primary_language = %(primary_language_1)s
        OR managers.manager_name = %(manager_name_1)s

:ticket:`2369` :ticket:`2587`

.. _feature_2976:

Right-nested inner joins available in joined eager loads
---------------------------------------------------------

As of version 0.9.4, the above mentioned right-nested joining can be enabled
in the case of a joined eager load where an "outer" join is linked to an "inner"
on the right side.

Normally, a joined eager load chain like the following::

    query(User).options(joinedload("orders", innerjoin=False).joinedload("items", innerjoin=True))

Would not produce an inner join; because of the LEFT OUTER JOIN from user->order,
joined eager loading could not use an INNER join from order->items without changing
the user rows that are returned, and would instead ignore the "chained" ``innerjoin=True``
directive.  How 0.9.0 should have delivered this would be that instead of::

    FROM users LEFT OUTER JOIN orders ON <onclause> LEFT OUTER JOIN items ON <onclause>

the new "right-nested joins are OK" logic would kick in, and we'd get::

    FROM users LEFT OUTER JOIN (orders JOIN items ON <onclause>) ON <onclause>

Since we missed the boat on that, to avoid further regressions we've added the above
functionality by specifying the string ``"nested"`` to :paramref:`.joinedload.innerjoin`::

    query(User).options(joinedload("orders", innerjoin=False).joinedload("items", innerjoin="nested"))

This feature is new in 0.9.4.

:ticket:`2976`



ORM can efficiently fetch just-generated INSERT/UPDATE defaults using RETURNING
-------------------------------------------------------------------------------

The :class:`.Mapper` has long supported an undocumented flag known as
``eager_defaults=True``.  The effect of this flag is that when an INSERT or UPDATE
proceeds, and the row is known to have server-generated default values,
a SELECT would immediately follow it in order to "eagerly" load those new values.
Normally, the server-generated columns are marked as "expired" on the object,
so that no overhead is incurred unless the application actually accesses these
columns soon after the flush.   The ``eager_defaults`` flag was therefore not
of much use as it could only decrease performance, and was present only to support
exotic event schemes where users needed default values to be available
immediately within the flush process.

In 0.9, as a result of the version id enhancements, ``eager_defaults`` can now
emit a RETURNING clause for these values, so on a backend with strong RETURNING
support in particular PostgreSQL, the ORM can fetch newly generated default
and SQL expression values inline with the INSERT or UPDATE.  ``eager_defaults``,
when enabled, makes use of RETURNING automatically when the target backend
and :class:`.Table` supports "implicit returning".

.. _change_2836:

Subquery Eager Loading will apply DISTINCT to the innermost SELECT for some queries
------------------------------------------------------------------------------------

In an effort to reduce the number of duplicate rows that can be generated
by subquery eager loading when a many-to-one relationship is involved, a
DISTINCT keyword will be applied to the innermost SELECT when the join is
targeting columns that do not comprise the primary key, as in when loading
along a many to one.

That is, when subquery loading on a many-to-one from A->B::

    SELECT b.id AS b_id, b.name AS b_name, anon_1.b_id AS a_b_id
    FROM (SELECT DISTINCT a_b_id FROM a) AS anon_1
    JOIN b ON b.id = anon_1.a_b_id

Since ``a.b_id`` is a non-distinct foreign key, DISTINCT is applied so that
redundant ``a.b_id`` are eliminated.  The behavior can be turned on or off
unconditionally for a particular :func:`.relationship` using the flag
``distinct_target_key``, setting the value to ``True`` for unconditionally
on, ``False`` for unconditionally off, and ``None`` for the feature to take
effect when the target SELECT is against columns that do not comprise a full
primary key.  In 0.9, ``None`` is the default.

The option is also backported to 0.8 where the ``distinct_target_key``
option defaults to ``False``.

While the feature here is designed to help performance by eliminating
duplicate rows, the ``DISTINCT`` keyword in SQL itself can have a negative
performance impact.  If columns in the SELECT are not indexed, ``DISTINCT``
will likely perform an ``ORDER BY`` on the rowset which can be expensive.
By keeping the feature limited just to foreign keys which are hopefully
indexed in any case, it's expected that the new defaults are reasonable.

The feature also does not eliminate every possible dupe-row scenario; if
a many-to-one is present elsewhere in the chain of joins, dupe rows may still
be present.

:ticket:`2836`

.. _migration_2789:

Backref handlers can now propagate more than one level deep
-----------------------------------------------------------

The mechanism by which attribute events pass along their "initiator", that is
the object associated with the start of the event, has been changed; instead
of a :class:`.AttributeImpl` being passed, a new object :class:`.attributes.Event`
is passed instead; this object refers to the :class:`.AttributeImpl` as well as
to an "operation token", representing if the operation is an append, remove,
or replace operation.

The attribute event system no longer looks at this "initiator" object in order to halt a
recursive series of attribute events.  Instead, the system of preventing endless
recursion due to mutually-dependent backref handlers has been moved
to the ORM backref event handlers specifically, which now take over the role
of ensuring that a chain of mutually-dependent events (such as append to collection
A.bs, set many-to-one attribute B.a in response) doesn't go into an endless recursion
stream.  The rationale here is that the backref system, given more detail and control
over event propagation, can finally allow operations more than one level deep
to occur; the typical scenario is when a collection append results in a many-to-one
replacement operation, which in turn should cause the item to be removed from a
previous collection::

    class Parent(Base):
        __tablename__ = 'parent'

        id = Column(Integer, primary_key=True)
        children = relationship("Child", backref="parent")

    class Child(Base):
        __tablename__ = 'child'

        id = Column(Integer, primary_key=True)
        parent_id = Column(ForeignKey('parent.id'))

    p1 = Parent()
    p2 = Parent()
    c1 = Child()

    p1.children.append(c1)

    assert c1.parent is p1  # backref event establishes c1.parent as p1

    p2.children.append(c1)

    assert c1.parent is p2  # backref event establishes c1.parent as p2
    assert c1 not in p1.children  # second backref event removes c1 from p1.children

Above, prior to this change, the ``c1`` object would still have been present
in ``p1.children``, even though it is also present in ``p2.children`` at the
same time; the backref handlers would have stopped at replacing ``c1.parent`` with
``p2`` instead of ``p1``.   In 0.9, using the more detailed :class:`.Event`
object as well as letting the backref handlers make more detailed decisions about
these objects, the propagation can continue onto removing ``c1`` from ``p1.children``
while maintaining a check against the propagation from going into an endless
recursive loop.

End-user code which a. makes use of the :meth:`.AttributeEvents.set`,
:meth:`.AttributeEvents.append`, or :meth:`.AttributeEvents.remove` events,
and b. initiates further attribute modification operations as a result of these
events may need to be modified to prevent recursive loops, as the attribute system
no longer stops a chain of events from propagating endlessly in the absence of the backref
event handlers.   Additionally, code which depends upon the value of the ``initiator``
will need to be adjusted to the new API, and furthermore must be ready for the
value of ``initiator`` to change from its original value within a string of
backref-initiated events, as the backref handlers may now swap in a
new ``initiator`` value for some operations.

:ticket:`2789`

.. _change_2838:

The typing system now handles the task of rendering "literal bind" values
-------------------------------------------------------------------------

A new method is added to :class:`.TypeEngine` :meth:`.TypeEngine.literal_processor`
as well as :meth:`.TypeDecorator.process_literal_param` for :class:`.TypeDecorator`
which take on the task of rendering so-called "inline literal parameters" - parameters
that normally render as "bound" values, but are instead being rendered inline
into the SQL statement due to the compiler configuration.  This feature is used
when generating DDL for constructs such as :class:`.CheckConstraint`, as well
as by Alembic when using constructs such as ``op.inline_literal()``.   Previously,
a simple "isinstance" check checked for a few basic types, and the "bind processor"
was used unconditionally, leading to such issues as strings being encoded into utf-8
prematurely.

Custom types written with :class:`.TypeDecorator` should continue to work in
"inline literal" scenarios, as the :meth:`.TypeDecorator.process_literal_param`
falls back to :meth:`.TypeDecorator.process_bind_param` by default, as these methods
usually handle a data manipulation, not as much how the data is presented to the
database.  :meth:`.TypeDecorator.process_literal_param` can be specified to
specifically produce a string representing how a value should be rendered
into an inline DDL statement.

:ticket:`2838`


.. _change_2812:

Schema identifiers now carry along their own quoting information
---------------------------------------------------------------------

This change simplifies the Core's usage of so-called "quote" flags, such
as the ``quote`` flag passed to :class:`.Table` and :class:`.Column`.  The flag
is now internalized within the string name itself, which is now represented
as an instance of  :class:`.quoted_name`, a string subclass.   The
:class:`.IdentifierPreparer` now relies solely on the quoting preferences
reported by the :class:`.quoted_name` object rather than checking for any
explicit ``quote`` flags in most cases.   The issue resolved here includes
that various case-sensitive methods such as :meth:`.Engine.has_table` as well
as similar methods within dialects now function with explicitly quoted names,
without the need to complicate or introduce backwards-incompatible changes
to those APIs (many of which are 3rd party) with the details of quoting flags -
in particular, a wider range of identifiers now function correctly with the
so-called "uppercase" backends like Oracle, Firebird, and DB2 (backends that
store and report upon table and column names using all uppercase for case
insensitive names).

The :class:`.quoted_name` object is used internally as needed; however if
other keywords require fixed quoting preferences, the class is available
publicly.

:ticket:`2812`

.. _migration_2804:

Improved rendering of Boolean constants, NULL constants, conjunctions
----------------------------------------------------------------------

New capabilities have been added to the :func:`.true` and :func:`.false`
constants, in particular in conjunction with :func:`.and_` and :func:`.or_`
functions as well as the behavior of the WHERE/HAVING clauses in conjunction
with these types, boolean types overall, and the :func:`.null` constant.

Starting with a table such as this::

    from sqlalchemy import Table, Boolean, Integer, Column, MetaData

    t1 = Table('t', MetaData(), Column('x', Boolean()), Column('y', Integer))

A select construct will now render the boolean column as a binary expression
on backends that don't feature ``true``/``false`` constant beahvior::

    >>> from sqlalchemy import select, and_, false, true
    >>> from sqlalchemy.dialects import mysql, postgresql

    >>> print(select([t1]).where(t1.c.x).compile(dialect=mysql.dialect()))
    SELECT t.x, t.y  FROM t WHERE t.x = 1

The :func:`.and_` and :func:`.or_` constructs will now exhibit quasi
"short circuit" behavior, that is truncating a rendered expression, when a
:func:`.true` or :func:`.false` constant is present::

    >>> print(select([t1]).where(and_(t1.c.y > 5, false())).compile(
    ...     dialect=postgresql.dialect()))
    SELECT t.x, t.y FROM t WHERE false

:func:`.true` can be used as the base to build up an expression::

    >>> expr = true()
    >>> expr = expr & (t1.c.y > 5)
    >>> print(select([t1]).where(expr))
    SELECT t.x, t.y FROM t WHERE t.y > :y_1

The boolean constants :func:`.true` and :func:`.false` themselves render as
``0 = 1`` and ``1 = 1`` for a backend with no boolean constants::

    >>> print(select([t1]).where(and_(t1.c.y > 5, false())).compile(
    ...     dialect=mysql.dialect()))
    SELECT t.x, t.y FROM t WHERE 0 = 1

Interpretation of ``None``, while not particularly valid SQL, is at least
now consistent::

    >>> print(select([t1.c.x]).where(None))
    SELECT t.x FROM t WHERE NULL

    >>> print(select([t1.c.x]).where(None).where(None))
    SELECT t.x FROM t WHERE NULL AND NULL

    >>> print(select([t1.c.x]).where(and_(None, None)))
    SELECT t.x FROM t WHERE NULL AND NULL

:ticket:`2804`

.. _migration_1068:

Label constructs can now render as their name alone in an ORDER BY
------------------------------------------------------------------

For the case where a :class:`.Label` is used in both the columns clause
as well as the ORDER BY clause of a SELECT, the label will render as
just its name in the ORDER BY clause, assuming the underlying dialect
reports support of this feature.

E.g. an example like::

    from sqlalchemy.sql import table, column, select, func

    t = table('t', column('c1'), column('c2'))
    expr = (func.foo(t.c.c1) + t.c.c2).label("expr")

    stmt = select([expr]).order_by(expr)

    print(stmt)

Prior to 0.9 would render as::

    SELECT foo(t.c1) + t.c2 AS expr
    FROM t ORDER BY foo(t.c1) + t.c2

And now renders as::

    SELECT foo(t.c1) + t.c2 AS expr
    FROM t ORDER BY expr

The ORDER BY only renders the label if the label isn't further
embedded into an expression within the ORDER BY, other than a simple
``ASC`` or ``DESC``.

The above format works on all databases tested, but might have
compatibility issues with older database versions (MySQL 4?  Oracle 8?
etc.).   Based on user reports we can add rules that will disable the
feature based on database version detection.

:ticket:`1068`

.. _migration_2848:

``RowProxy`` now has tuple-sorting behavior
-------------------------------------------

The :class:`.RowProxy` object acts much like a tuple, but up until now
would not sort as a tuple if a list of them were sorted using ``sorted()``.
The ``__eq__()`` method now compares both sides as a tuple and also
an ``__lt__()`` method has been added::

    users.insert().execute(
            dict(user_id=1, user_name='foo'),
            dict(user_id=2, user_name='bar'),
            dict(user_id=3, user_name='def'),
        )

    rows = users.select().order_by(users.c.user_name).execute().fetchall()

    eq_(rows, [(2, 'bar'), (3, 'def'), (1, 'foo')])

    eq_(sorted(rows), [(1, 'foo'), (2, 'bar'), (3, 'def')])

:ticket:`2848`

.. _migration_2850:

A bindparam() construct with no type gets upgraded via copy when a type is available
------------------------------------------------------------------------------------

The logic which "upgrades" a :func:`.bindparam` construct to take on the
type of the enclosing expression has been improved in two ways.  First, the
:func:`.bindparam` object is **copied** before the new type is assigned, so that
the given :func:`.bindparam` is not mutated in place.  Secondly, this same
operation occurs when an :class:`.Insert` or :class:`.Update` construct is compiled,
regarding the "values" that were set in the statement via the :meth:`.ValuesBase.values`
method.

If given an untyped :func:`.bindparam`::

    bp = bindparam("some_col")

If we use this parameter as follows::

    expr = mytable.c.col == bp

The type for ``bp`` remains as ``NullType``, however if ``mytable.c.col``
is of type ``String``, then ``expr.right``, that is the right side of the
binary expression, will take on the ``String`` type.   Previously, ``bp`` itself
would have been changed in place to have ``String`` as its type.

Similarly, this operation occurs in an :class:`.Insert` or :class:`.Update`::

    stmt = mytable.update().values(col=bp)

Above, ``bp`` remains unchanged, but the ``String`` type will be used when
the statement is executed, which we can see by examining the ``binds`` dictionary::

    >>> compiled = stmt.compile()
    >>> compiled.binds['some_col'].type
    String

The feature allows custom types to take their expected effect within INSERT/UPDATE
statements without needing to explicitly specify those types within every
:func:`.bindparam` expression.

The potentially backwards-compatible changes involve two unlikely
scenarios.  Since the bound parameter is
**cloned**, users should not be relying upon making in-place changes to a
:func:`.bindparam` construct once created.   Additionally, code which uses
:func:`.bindparam` within an :class:`.Insert` or :class:`.Update` statement
which is relying on the fact that the :func:`.bindparam` is not typed according
to the column being assigned towards will no longer function in that way.

:ticket:`2850`


.. _migration_1765:

Columns can reliably get their type from a column referred to via ForeignKey
----------------------------------------------------------------------------

There's a long standing behavior which says that a :class:`.Column` can be
declared without a type, as long as that :class:`.Column` is referred to
by a :class:`.ForeignKeyConstraint`, and the type from the referenced column
will be copied into this one.   The problem has been that this feature never
worked very well and wasn't maintained.   The core issue was that the
:class:`.ForeignKey` object doesn't know what target :class:`.Column` it
refers to until it is asked, typically the first time the foreign key is used
to construct a :class:`.Join`.   So until that time, the parent :class:`.Column`
would not have a type, or more specifically, it would have a default type
of :class:`.NullType`.

While it's taken a long time, the work to reorganize the initialization of
:class:`.ForeignKey` objects has been completed such that this feature can
finally work acceptably.  At the core of the change is that the :attr:`.ForeignKey.column`
attribute no longer lazily initializes the location of the target :class:`.Column`;
the issue with this system was that the owning :class:`.Column` would be stuck
with :class:`.NullType` as its type until the :class:`.ForeignKey` happened to
be used.

In the new version, the :class:`.ForeignKey` coordinates with the eventual
:class:`.Column` it will refer to using internal attachment events, so that the
moment the referencing :class:`.Column` is associated with the
:class:`.MetaData`, all :class:`.ForeignKey` objects that
refer to it will be sent a message that they need to initialize their parent
column.   This system is more complicated but works more solidly; as a bonus,
there are now tests in place for a wide variety of :class:`.Column` /
:class:`.ForeignKey` configuration scenarios and error messages have been
improved to be very specific to no less than seven different error conditions.

Scenarios which now work correctly include:

1. The type on a :class:`.Column` is immediately present as soon as the
   target :class:`.Column` becomes associated with the same :class:`.MetaData`;
   this works no matter which side is configured first::

    >>> from sqlalchemy import Table, MetaData, Column, Integer, ForeignKey
    >>> metadata = MetaData()
    >>> t2 = Table('t2', metadata, Column('t1id', ForeignKey('t1.id')))
    >>> t2.c.t1id.type
    NullType()
    >>> t1 = Table('t1', metadata, Column('id', Integer, primary_key=True))
    >>> t2.c.t1id.type
    Integer()

2. The system now works with :class:`.ForeignKeyConstraint` as well::

    >>> from sqlalchemy import Table, MetaData, Column, Integer, ForeignKeyConstraint
    >>> metadata = MetaData()
    >>> t2 = Table('t2', metadata,
    ...     Column('t1a'), Column('t1b'),
    ...     ForeignKeyConstraint(['t1a', 't1b'], ['t1.a', 't1.b']))
    >>> t2.c.t1a.type
    NullType()
    >>> t2.c.t1b.type
    NullType()
    >>> t1 = Table('t1', metadata,
    ...     Column('a', Integer, primary_key=True),
    ...     Column('b', Integer, primary_key=True))
    >>> t2.c.t1a.type
    Integer()
    >>> t2.c.t1b.type
    Integer()

3. It even works for "multiple hops" - that is, a :class:`.ForeignKey` that refers to a
   :class:`.Column` that refers to another :class:`.Column`::

    >>> from sqlalchemy import Table, MetaData, Column, Integer, ForeignKey
    >>> metadata = MetaData()
    >>> t2 = Table('t2', metadata, Column('t1id', ForeignKey('t1.id')))
    >>> t3 = Table('t3', metadata, Column('t2t1id', ForeignKey('t2.t1id')))
    >>> t2.c.t1id.type
    NullType()
    >>> t3.c.t2t1id.type
    NullType()
    >>> t1 = Table('t1', metadata, Column('id', Integer, primary_key=True))
    >>> t2.c.t1id.type
    Integer()
    >>> t3.c.t2t1id.type
    Integer()

:ticket:`1765`


Dialect Changes
===============

Firebird ``fdb`` is now the default Firebird dialect.
-----------------------------------------------------

The ``fdb`` dialect is now used if an engine is created without a dialect
specifier, i.e. ``firebird://``.  ``fdb`` is a ``kinterbasdb`` compatible
DBAPI which per the Firebird project is now their official Python driver.

:ticket:`2504`

Firebird ``fdb`` and ``kinterbasdb`` set ``retaining=False`` by default
-----------------------------------------------------------------------

Both the ``fdb`` and ``kinterbasdb`` DBAPIs support a flag ``retaining=True``
which can be passed to the ``commit()`` and ``rollback()`` methods of its
connection.  The documented rationale for this flag is so that the DBAPI
can re-use internal transaction state for subsequent transactions, for the
purposes of improving performance.   However, newer documentation refers
to analyses of Firebird's "garbage collection" which expresses that this flag
can have a negative effect on the database's ability to process cleanup
tasks, and has been reported as *lowering* performance as a result.

It's not clear how this flag is actually usable given this information,
and as it appears to be only a performance enhancing feature, it now defaults
to ``False``.  The value can be controlled by passing the flag ``retaining=True``
to the :func:`.create_engine` call.  This is a new flag which is added as of
0.8.2, so applications on 0.8.2 can begin setting this to ``True`` or ``False``
as desired.

.. seealso::

    :mod:`sqlalchemy.dialects.firebird.fdb`

    :mod:`sqlalchemy.dialects.firebird.kinterbasdb`

    http://pythonhosted.org/fdb/usage-guide.html#retaining-transactions - information
    on the "retaining" flag.

:ticket:`2763`





