Sessions / Queries
==================

.. contents::
    :local:
    :class: faq
    :backlinks: none


I'm re-loading data with my Session but it isn't seeing changes that I committed elsewhere
------------------------------------------------------------------------------------------

The main issue regarding this behavior is that the session acts as though
the transaction is in the *serializable* isolation state, even if it's not
(and it usually is not).   In practical terms, this means that the session
does not alter any data that it's already read within the scope of a transaction.

If the term "isolation level" is unfamiliar, then you first need to read this link:

`Isolation Level <https://en.wikipedia.org/wiki/Isolation_%28database_systems%29>`_

In short, serializable isolation level generally means
that once you SELECT a series of rows in a transaction, you will get
*the identical data* back each time you re-emit that SELECT.   If you are in
the next-lower isolation level, "repeatable read", you'll
see newly added rows (and no longer see deleted rows), but for rows that
you've *already* loaded, you won't see any change.   Only if you are in a
lower isolation level, e.g. "read committed", does it become possible to
see a row of data change its value.

For information on controlling the isolation level when using the
SQLAlchemy ORM, see :ref:`session_transaction_isolation`.

To simplify things dramatically, the :class:`.Session` itself works in
terms of a completely isolated transaction, and doesn't overwrite any mapped attributes
it's already read unless you tell it to.  The use case of trying to re-read
data you've already loaded in an ongoing transaction is an *uncommon* use
case that in many cases has no effect, so this is considered to be the
exception, not the norm; to work within this exception, several methods
are provided to allow specific data to be reloaded within the context
of an ongoing transaction.

To understand what we mean by "the transaction" when we talk about the
:class:`.Session`, your :class:`.Session` is intended to only work within
a transaction.  An overview of this is at :ref:`unitofwork_transaction`.

Once we've figured out what our isolation level is, and we think that
our isolation level is set at a low enough level so that if we re-SELECT a row,
we should see new data in our :class:`.Session`, how do we see it?

Three ways, from most common to least:

1. We simply end our transaction and start a new one on next access
   with our :class:`.Session` by calling :meth:`.Session.commit` (note
   that if the :class:`.Session` is in the lesser-used "autocommit"
   mode, there would be a call to :meth:`.Session.begin` as well). The
   vast majority of applications and use cases do not have any issues
   with not being able to "see" data in other transactions because
   they stick to this pattern, which is at the core of the best practice of
   **short lived transactions**.
   See :ref:`session_faq_whentocreate` for some thoughts on this.

2. We tell our :class:`.Session` to re-read rows that it has already read,
   either when we next query for them using :meth:`.Session.expire_all`
   or :meth:`.Session.expire`, or immediately on an object using
   :class:`.Session.refresh`.  See :ref:`session_expire` for detail on this.

3. We can run whole queries while setting them to definitely overwrite
   already-loaded objects as they read rows by using
   :meth:`.Query.populate_existing`.

But remember, **the ORM cannot see changes in rows if our isolation
level is repeatable read or higher, unless we start a new transaction**.


"This Session's transaction has been rolled back due to a previous exception during flush." (or similar)
---------------------------------------------------------------------------------------------------------

This is an error that occurs when a :meth:`.Session.flush` raises an exception, rolls back
the transaction, but further commands upon the `Session` are called without an
explicit call to :meth:`.Session.rollback` or :meth:`.Session.close`.

It usually corresponds to an application that catches an exception
upon :meth:`.Session.flush` or :meth:`.Session.commit` and
does not properly handle the exception.    For example::

    from sqlalchemy import create_engine, Column, Integer
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base(create_engine('sqlite://'))

    class Foo(Base):
        __tablename__ = 'foo'
        id = Column(Integer, primary_key=True)

    Base.metadata.create_all()

    session = sessionmaker()()

    # constraint violation
    session.add_all([Foo(id=1), Foo(id=1)])

    try:
        session.commit()
    except:
        # ignore error
        pass

    # continue using session without rolling back
    session.commit()


The usage of the :class:`.Session` should fit within a structure similar to this::

    try:
        <use session>
        session.commit()
    except:
       session.rollback()
       raise
    finally:
       session.close()  # optional, depends on use case

Many things can cause a failure within the try/except besides flushes. You
should always have some kind of "framing" of your session operations so that
connection and transaction resources have a definitive boundary, otherwise
your application doesn't really have its usage of resources under control.
This is not to say that you need to put try/except blocks all throughout your
application - on the contrary, this would be a terrible idea.  You should
architect your application such that there is one (or few) point(s) of
"framing" around session operations.

For a detailed discussion on how to organize usage of the :class:`.Session`,
please see :ref:`session_faq_whentocreate`.

But why does flush() insist on issuing a ROLLBACK?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It would be great if :meth:`.Session.flush` could partially complete and then not roll
back, however this is beyond its current capabilities since its internal
bookkeeping would have to be modified such that it can be halted at any time
and be exactly consistent with what's been flushed to the database. While this
is theoretically possible, the usefulness of the enhancement is greatly
decreased by the fact that many database operations require a ROLLBACK in any
case. Postgres in particular has operations which, once failed, the
transaction is not allowed to continue::

    test=> create table foo(id integer primary key);
    NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "foo_pkey" for table "foo"
    CREATE TABLE
    test=> begin;
    BEGIN
    test=> insert into foo values(1);
    INSERT 0 1
    test=> commit;
    COMMIT
    test=> begin;
    BEGIN
    test=> insert into foo values(1);
    ERROR:  duplicate key value violates unique constraint "foo_pkey"
    test=> insert into foo values(2);
    ERROR:  current transaction is aborted, commands ignored until end of transaction block

What SQLAlchemy offers that solves both issues is support of SAVEPOINT, via
:meth:`.Session.begin_nested`. Using :meth:`.Session.begin_nested`, you can frame an operation that may
potentially fail within a transaction, and then "roll back" to the point
before its failure while maintaining the enclosing transaction.

But why isn't the one automatic call to ROLLBACK enough?  Why must I ROLLBACK again?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is again a matter of the :class:`.Session` providing a consistent interface and
refusing to guess about what context its being used. For example, the
:class:`.Session` supports "framing" above within multiple levels. Such as, suppose
you had a decorator ``@with_session()``, which did this::

    def with_session(fn):
       def go(*args, **kw):
           session.begin(subtransactions=True)
           try:
               ret = fn(*args, **kw)
               session.commit()
               return ret
           except:
               session.rollback()
               raise
       return go

The above decorator begins a transaction if one does not exist already, and
then commits it, if it were the creator. The "subtransactions" flag means that
if :meth:`.Session.begin` were already called by an enclosing function, nothing happens
except a counter is incremented - this counter is decremented when :meth:`.Session.commit`
is called and only when it goes back to zero does the actual COMMIT happen. It
allows this usage pattern::

    @with_session
    def one():
       # do stuff
       two()


    @with_session
    def two():
       # etc.

    one()

    two()

``one()`` can call ``two()``, or ``two()`` can be called by itself, and the
``@with_session`` decorator ensures the appropriate "framing" - the transaction
boundaries stay on the outermost call level. As you can see, if ``two()`` calls
``flush()`` which throws an exception and then issues a ``rollback()``, there will
*always* be a second ``rollback()`` performed by the decorator, and possibly a
third corresponding to two levels of decorator. If the ``flush()`` pushed the
``rollback()`` all the way out to the top of the stack, and then we said that
all remaining ``rollback()`` calls are moot, there is some silent behavior going
on there. A poorly written enclosing method might suppress the exception, and
then call ``commit()`` assuming nothing is wrong, and then you have a silent
failure condition. The main reason people get this error in fact is because
they didn't write clean "framing" code and they would have had other problems
down the road.

If you think the above use case is a little exotic, the same kind of thing
comes into play if you want to SAVEPOINT- you might call ``begin_nested()``
several times, and the ``commit()``/``rollback()`` calls each resolve the most
recent ``begin_nested()``. The meaning of ``rollback()`` or ``commit()`` is
dependent upon which enclosing block it is called, and you might have any
sequence of ``rollback()``/``commit()`` in any order, and its the level of nesting
that determines their behavior.

In both of the above cases, if ``flush()`` broke the nesting of transaction
blocks, the behavior is, depending on scenario, anywhere from "magic" to
silent failure to blatant interruption of code flow.

``flush()`` makes its own "subtransaction", so that a transaction is started up
regardless of the external transactional state, and when complete it calls
``commit()``, or ``rollback()`` upon failure - but that ``rollback()`` corresponds
to its own subtransaction - it doesn't want to guess how you'd like to handle
the external "framing" of the transaction, which could be nested many levels
with any combination of subtransactions and real SAVEPOINTs. The job of
starting/ending the "frame" is kept consistently with the code external to the
``flush()``, and we made a decision that this was the most consistent approach.



How do I make a Query that always adds a certain filter to every query?
------------------------------------------------------------------------------------------------

See the recipe at `PreFilteredQuery <http://www.sqlalchemy.org/trac/wiki/UsageRecipes/PreFilteredQuery>`_.

I've created a mapping against an Outer Join, and while the query returns rows, no objects are returned.  Why not?
------------------------------------------------------------------------------------------------------------------

Rows returned by an outer join may contain NULL for part of the primary key,
as the primary key is the composite of both tables.  The :class:`.Query` object ignores incoming rows
that don't have an acceptable primary key.   Based on the setting of the ``allow_partial_pks``
flag on :func:`.mapper`, a primary key is accepted if the value has at least one non-NULL
value, or alternatively if the value has no NULL values.  See ``allow_partial_pks``
at :func:`.mapper`.


I'm using ``joinedload()`` or ``lazy=False`` to create a JOIN/OUTER JOIN and SQLAlchemy is not constructing the correct query when I try to add a WHERE, ORDER BY, LIMIT, etc. (which relies upon the (OUTER) JOIN)
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

The joins generated by joined eager loading are only used to fully load related
collections, and are designed to have no impact on the primary results of the query.
Since they are anonymously aliased, they cannot be referenced directly.

For detail on this beahvior, see :ref:`zen_of_eager_loading`.

Query has no ``__len__()``, why not?
------------------------------------

The Python ``__len__()`` magic method applied to an object allows the ``len()``
builtin to be used to determine the length of the collection. It's intuitive
that a SQL query object would link ``__len__()`` to the :meth:`.Query.count`
method, which emits a `SELECT COUNT`. The reason this is not possible is
because evaluating the query as a list would incur two SQL calls instead of
one::

    class Iterates(object):
        def __len__(self):
            print("LEN!")
            return 5

        def __iter__(self):
            print("ITER!")
            return iter([1, 2, 3, 4, 5])

    list(Iterates())

output::

    ITER!
    LEN!

How Do I use Textual SQL with ORM Queries?
------------------------------------------

See:

* :ref:`orm_tutorial_literal_sql` - Ad-hoc textual blocks with :class:`.Query`

* :ref:`session_sql_expressions` - Using :class:`.Session` with textual SQL directly.

I'm calling ``Session.delete(myobject)`` and it isn't removed from the parent collection!
------------------------------------------------------------------------------------------

See :ref:`session_deleting_from_collections` for a description of this behavior.

why isn't my ``__init__()`` called when I load objects?
-------------------------------------------------------

See :ref:`mapping_constructors` for a description of this behavior.

how do I use ON DELETE CASCADE with SA's ORM?
---------------------------------------------

SQLAlchemy will always issue UPDATE or DELETE statements for dependent
rows which are currently loaded in the :class:`.Session`.  For rows which
are not loaded, it will by default issue SELECT statements to load
those rows and update/delete those as well; in other words it assumes
there is no ON DELETE CASCADE configured.
To configure SQLAlchemy to cooperate with ON DELETE CASCADE, see
:ref:`passive_deletes`.

I set the "foo_id" attribute on my instance to "7", but the "foo" attribute is still ``None`` - shouldn't it have loaded Foo with id #7?
----------------------------------------------------------------------------------------------------------------------------------------------------

The ORM is not constructed in such a way as to support
immediate population of relationships driven from foreign
key attribute changes - instead, it is designed to work the
other way around - foreign key attributes are handled by the
ORM behind the scenes, the end user sets up object
relationships naturally. Therefore, the recommended way to
set ``o.foo`` is to do just that - set it!::

    foo = Session.query(Foo).get(7)
    o.foo = foo
    Session.commit()

Manipulation of foreign key attributes is of course entirely legal.  However,
setting a foreign-key attribute to a new value currently does not trigger
an "expire" event of the :func:`.relationship` in which it's involved.  This means
that for the following sequence::

    o = Session.query(SomeClass).first()
    assert o.foo is None  # accessing an un-set attribute sets it to None
    o.foo_id = 7

``o.foo`` is initialized to ``None`` when we first accessed it.  Setting
``o.foo_id = 7`` will have the value of "7" as pending, but no flush
has occurred - so ``o.foo`` is still ``None``::

    # attribute is already set to None, has not been
    # reconciled with o.foo_id = 7 yet
    assert o.foo is None

For ``o.foo`` to load based on the foreign key mutation is usually achieved
naturally after the commit, which both flushes the new foreign key value
and expires all state::

    Session.commit()  # expires all attributes

    foo_7 = Session.query(Foo).get(7)

    assert o.foo is foo_7  # o.foo lazyloads on access

A more minimal operation is to expire the attribute individually - this can
be performed for any :term:`persistent` object using :meth:`.Session.expire`::

    o = Session.query(SomeClass).first()
    o.foo_id = 7
    Session.expire(o, ['foo'])  # object must be persistent for this

    foo_7 = Session.query(Foo).get(7)

    assert o.foo is foo_7  # o.foo lazyloads on access

Note that if the object is not persistent but present in the :class:`.Session`,
it's known as :term:`pending`.   This means the row for the object has not been
INSERTed into the database yet.  For such an object, setting ``foo_id`` does not
have meaning until the row is inserted; otherwise there is no row yet::

    new_obj = SomeClass()
    new_obj.foo_id = 7

    Session.add(new_obj)

    # accessing an un-set attribute sets it to None
    assert new_obj.foo is None

    Session.flush()  # emits INSERT

    # expire this because we already set .foo to None
    Session.expire(o, ['foo'])

    assert new_obj.foo is foo_7  # now it loads


.. topic:: Attribute loading for non-persistent objects

    One variant on the "pending" behavior above is if we use the flag
    ``load_on_pending`` on :func:`.relationship`.   When this flag is set, the
    lazy loader will emit for ``new_obj.foo`` before the INSERT proceeds; another
    variant of this is to use the :meth:`.Session.enable_relationship_loading`
    method, which can "attach" an object to a :class:`.Session` in such a way that
    many-to-one relationships load as according to foreign key attributes
    regardless of the object being in any particular state.
    Both techniques are **not recommended for general use**; they were added to suit
    specific programming scenarios encountered by users which involve the repurposing
    of the ORM's usual object states.

The recipe `ExpireRelationshipOnFKChange <http://www.sqlalchemy.org/trac/wiki/UsageRecipes/ExpireRelationshipOnFKChange>`_ features an example using SQLAlchemy events
in order to coordinate the setting of foreign key attributes with many-to-one
relationships.

.. _faq_walk_objects:

How do I walk all objects that are related to a given object?
-------------------------------------------------------------

An object that has other objects related to it will correspond to the
:func:`.relationship` constructs set up between mappers.  This code fragment will
iterate all the objects, correcting for cycles as well::

    from sqlalchemy import inspect


    def walk(obj):
        deque = [obj]

        seen = set()

        while deque:
            obj = deque.pop(0)
            if obj in seen:
                continue
            else:
                seen.add(obj)
                yield obj
            insp = inspect(obj)
            for relationship in insp.mapper.relationships:
                related = getattr(obj, relationship.key)
                if relationship.uselist:
                    deque.extend(related)
                elif related is not None:
                    deque.append(related)

The function can be demonstrated as follows::

    Base = declarative_base()


    class A(Base):
        __tablename__ = 'a'
        id = Column(Integer, primary_key=True)
        bs = relationship("B", backref="a")


    class B(Base):
        __tablename__ = 'b'
        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey('a.id'))
        c_id = Column(ForeignKey('c.id'))
        c = relationship("C", backref="bs")


    class C(Base):
        __tablename__ = 'c'
        id = Column(Integer, primary_key=True)


    a1 = A(bs=[B(), B(c=C())])


    for obj in walk(a1):
        print(obj)

Output::

    <__main__.A object at 0x10303b190>
    <__main__.B object at 0x103025210>
    <__main__.B object at 0x10303b0d0>
    <__main__.C object at 0x103025490>



Is there a way to automagically have only unique keywords (or other kinds of objects) without doing a query for the keyword and getting a reference to the row containing that keyword?
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

When people read the many-to-many example in the docs, they get hit with the
fact that if you create the same ``Keyword`` twice, it gets put in the DB twice.
Which is somewhat inconvenient.

This `UniqueObject <http://www.sqlalchemy.org/trac/wiki/UsageRecipes/UniqueObject>`_ recipe was created to address this issue.


