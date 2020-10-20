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

General
=======

.. _change_4393_general:

Deprecation warnings are emitted for all deprecated elements; new deprecations added
------------------------------------------------------------------------------------

Release 1.3 ensures that all behaviors and APIs that are deprecated, including
all those that have been long listed as "legacy" for years, are emitting
``DeprecationWarning`` warnings. This includes when making use of parameters
such as :paramref:`.Session.weak_identity_map` and classes such as
:class:`.MapperExtension`.     While all deprecations have been noted in the
documentation, often they did not use a proper restructured text directive, or
include in what version they were deprecated.  Whether or not a particular API
feature actually emitted a deprecation warning was not consistent.  The general
attitude was that most or all of these deprecated features were treated as
long-term legacy features with no plans to remove them.

The change includes that all documented deprecations now use a proper
restructured text directive in the documentation with a version number, the
verbiage that the feature or use case will be removed in a future release is
made explicit (e.g., no more legacy forever use cases), and that use of any
such feature or use case will definitely emit a ``DeprecationWarning``, which
in Python 3 as well as when using modern testing tools like Pytest are now made
more explicit in the standard error stream.  The goal is that these long
deprecated features, going back as far as version 0.7 or 0.6, should start
being removed entirely, rather than keeping them around as "legacy" features.
Additionally, some major new deprecations are being added as of version 1.3.
As SQLAlchemy has 14 years of real world use by thousands of developers, it's
possible to point to a single stream of use cases that blend together well, and
to trim away features and patterns that work against this single way of
working.

The larger context is that SQLAlchemy seeks to adjust to the coming Python
3-only world, as well as a type-annotated world, and towards this goal there
are **tentative** plans for a major rework of  SQLAlchemy which would hopefully
greatly reduce the cognitive load of the API as well as perform a major pass
over the great many differences in implementation and use between Core and ORM.
As these two systems evolved dramatically after SQLAlchemy's first release, in
particular the ORM still retains lots of "bolted on" behaviors that keep the
wall of separation between Core and  ORM too high.  By focusing the API
ahead of time on a single pattern for each supported use case, the eventual
job of migrating to a significantly altered API becomes simpler.

For the most major deprecations being added in 1.3, see the linked sections
below.


.. seealso::

    :ref:`change_4393_threadlocal`

    :ref:`change_4393_convertunicode`

    :ref:`change_4423`

:ticket:`4393`

New Features and Improvements - ORM
===================================

.. _change_4423:

Relationship to AliasedClass replaces the need for non primary mappers
-----------------------------------------------------------------------

The "non primary mapper" is a :func:`.mapper` created in the
:ref:`classical_mapping` style, which acts as an additional mapper against an
already mapped class against a different kind of selectable.  The non primary
mapper has its roots in the 0.1, 0.2 series of SQLAlchemy where it was
anticipated that the :func:`.mapper` object was to be the primary query
construction interface, before the :class:`_query.Query` object existed.

With the advent of :class:`_query.Query` and later the :class:`.AliasedClass`
construct, most use cases for the non primary mapper went away.  This was a
good thing since SQLAlchemy also moved away from "classical" mappings altogether
around the 0.5 series in favor of the declarative system.

One use case remained around for non primary mappers when it was realized that
some very hard-to-define :func:`_orm.relationship` configurations could be made
possible when a non-primary mapper with an alternative selectable was made as
the mapping target, rather than trying to construct a
:paramref:`_orm.relationship.primaryjoin` that encompassed all the complexity of a
particular inter-object relationship.

As this use case became more popular, its limitations became apparent,
including that the non primary mapper is difficult to configure against a
selectable that adds new columns, that the mapper does not inherit the
relationships of the original mapping, that relationships which are configured
explicitly on the non primary mapper do  not function well with loader options,
and that the non primary mapper also doesn't provide a fully functional
namespace of column-based attributes which can be used in queries (which again,
in the old 0.1 - 0.4 days, one would use :class:`_schema.Table` objects directly with
the ORM).

The missing piece was to allow the :func:`_orm.relationship` to refer directly
to the :class:`.AliasedClass`.  The :class:`.AliasedClass` already does
everything we want the non primary mapper to do; it allows an existing mapped
class to be loaded from an alternative selectable, it inherits all the
attributes and relationships of the existing mapper, it works
extremely well with loader options, and it provides a class-like
object that can be mixed into queries just like the class itself.
With this change, the recipes that
were formerly for non primary mappers at :ref:`relationship_configure_joins`
are changed to aliased class.

At :ref:`relationship_aliased_class`, the original non primary mapper looked
like::

    j = join(B, D, D.b_id == B.id).join(C, C.id == D.c_id)

    B_viacd = mapper(
        B, j, non_primary=True, primary_key=[j.c.b_id],
        properties={
            "id": j.c.b_id,  # so that 'id' looks the same as before
            "c_id": j.c.c_id,   # needed for disambiguation
            "d_c_id": j.c.d_c_id,  # needed for disambiguation
            "b_id": [j.c.b_id, j.c.d_b_id],
            "d_id": j.c.d_id,
        }
    )

    A.b = relationship(B_viacd, primaryjoin=A.b_id == B_viacd.c.b_id)

The properties were necessary in order to re-map the additional columns
so that they did not conflict with the existing columns mapped to ``B``, as
well as it was necessary to define a new primary key.

With the new approach, all of this verbosity goes away, and the additional
columns are referred towards directly when making the relationship::

    j = join(B, D, D.b_id == B.id).join(C, C.id == D.c_id)

    B_viacd = aliased(B, j, flat=True)

    A.b = relationship(B_viacd, primaryjoin=A.b_id == j.c.b_id)

The non primary mapper is now deprecated with the eventual goal to be that
classical mappings as a feature go away entirely.  The Declarative API would
become the single means of mapping which hopefully will allow internal
improvements and simplifications, as well as a clearer documentation story.


:ticket:`4423`


.. _change_4340:

selectin loading no longer uses JOIN for simple one-to-many
------------------------------------------------------------

The "selectin" loading feature added in 1.2 introduced an extremely
performant new way to eagerly load collections, in many cases much faster
than that of "subquery" eager loading, as it does not rely upon restating
the original SELECT query and instead uses a simple IN clause.  However,
the "selectin" load still relied upon rendering a JOIN between the
parent and related tables, since it needs the parent primary key values
in the row in order to match rows up.     In 1.3, a new optimization
is added which will omit this JOIN in the most common case of a simple
one-to-many load, where the related row already contains the primary key
of the parent row expressed in its foreign key columns.   This again provides
for a dramatic performance improvement as the ORM now can load large numbers
of collections all in one query without using JOIN or subqueries at all.

Given a mapping::

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)
        bs = relationship("B", lazy="selectin")


    class B(Base):
        __tablename__ = 'b'
        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey("a.id"))

In the 1.2 version of "selectin" loading, a load of A to B looks like:

.. sourcecode:: sql

    SELECT a.id AS a_id FROM a
    SELECT a_1.id AS a_1_id, b.id AS b_id, b.a_id AS b_a_id
    FROM a AS a_1 JOIN b ON a_1.id = b.a_id
    WHERE a_1.id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ORDER BY a_1.id
    (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

With the new behavior, the load looks like:

.. sourcecode:: sql


    SELECT a.id AS a_id FROM a
    SELECT b.a_id AS b_a_id, b.id AS b_id FROM b
    WHERE b.a_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ORDER BY b.a_id
    (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

The behavior is being released as automatic, using a similar heuristic that
lazy loading uses in order to determine if related entities can be fetched
directly from the identity map.   However, as with most querying features,
the feature's implementation became more complex as a result of advanced
scenarios regarding polymorphic loading.   If problems are encountered,
users should report a bug, however the change also includes a flag
:paramref:`_orm.relationship.omit_join` which can be set to ``False`` on the
:func:`_orm.relationship` to disable the optimization.


:ticket:`4340`

.. _change_4359:

Improvement to the behavior of many-to-one query expressions
------------------------------------------------------------

When building a query that compares a many-to-one relationship to an
object value, such as::

    u1 = session.query(User).get(5)

    query = session.query(Address).filter(Address.user == u1)

The above expression ``Address.user == u1``, which ultimately compiles to a SQL
expression normally based on the primary key columns of the ``User`` object
like ``"address.user_id = 5"``, uses a deferred callable in order to retrieve
the value ``5`` within the bound expression until  as late as possible.  This
is to suit both the use case where the ``Address.user == u1`` expression may be
against a ``User`` object that isn't flushed yet which relies upon a server-
generated primary key value, as well as that the expression always returns the
correct result even if the primary key value of ``u1`` has been changed since
the expression was created.

However, a side effect of this behavior is that if ``u1`` ends up being expired
by the time the expression is evaluated, it results in an additional SELECT
statement, and in the case that ``u1`` was also detached from the
:class:`.Session`, it would raise an error::

    u1 = session.query(User).get(5)

    query = session.query(Address).filter(Address.user == u1)

    session.expire(u1)
    session.expunge(u1)

    query.all()  # <-- would raise DetachedInstanceError

The expiration / expunging of the object can occur implicitly when the
:class:`.Session` is committed and the ``u1`` instance falls out of scope,
as the ``Address.user == u1`` expression does not strongly reference the
object itself, only its :class:`.InstanceState`.

The fix is to allow the ``Address.user == u1`` expression to evaluate the value
``5`` based on attempting to retrieve or load the value normally at expression
compilation time as it does now, but if the object is detached and has
been expired, it is retrieved from a new mechanism upon the
:class:`.InstanceState` which will memoize the last known value for a
particular attribute on that state when that attribute is expired.  This
mechanism is only enabled for a specific attribute / :class:`.InstanceState`
when needed by the expression feature to conserve performance / memory
overhead.

Originally, simpler approaches such as evaluating the expression immediately
with various arrangements for trying to load the value later if not present
were attempted, however the difficult edge case is that of the value  of a
column attribute (typically a natural primary key) that is being changed.   In
order to ensure that an expression like ``Address.user == u1`` always returns
the correct answer for the current state of ``u1``, it will return the current
database-persisted value for a persistent object, unexpiring via SELECT query
if necessary, and for a detached object it will return the most recent known
value, regardless of when the object was expired using a new feature within the
:class:`.InstanceState` that tracks the last known value of a column attribute
whenever the attribute is to be expired.

Modern attribute API features are used to indicate specific error messages when
the value cannot be evaluated, the two cases of which are when the column
attributes have never been set, and when the object was already expired
when the first evaluation was made and is now detached. In all cases,
:class:`.DetachedInstanceError` is no longer raised.


:ticket:`4359`

.. _change_4353:

Many-to-one replacement won't raise for "raiseload" or detached for "old" object
--------------------------------------------------------------------------------

Given the case where a lazy load would proceed on a many-to-one relationship
in order to load the "old" value, if the relationship does not specify
the :paramref:`_orm.relationship.active_history` flag, an assertion will not
be raised for a detached object::

    a1 = session.query(Address).filter_by(id=5).one()

    session.expunge(a1)

    a1.user = some_user

Above, when the ``.user`` attribute is replaced on the detached ``a1`` object,
a :class:`.DetachedInstanceError` would be raised as the attribute is attempting
to retrieve the previous value of ``.user`` from the identity map.  The change
is that the operation now proceeds without the old value being loaded.

The same change is also made to the ``lazy="raise"`` loader strategy::

    class Address(Base):
        # ...

        user = relationship("User", ..., lazy="raise")

Previously, the association of ``a1.user`` would invoke the "raiseload"
exception as a result of the attribute attempting to retrieve the previous
value.   This assertion is now skipped in the case of loading the "old" value.


:ticket:`4353`


.. _change_4354:

"del" implemented for ORM attributes
------------------------------------

The Python ``del`` operation was not really usable for mapped attributes, either
scalar columns or object references.   Support has been added for this to work correctly,
where the ``del`` operation is roughly equivalent to setting the attribute to the
``None`` value::


    some_object = session.query(SomeObject).get(5)

    del some_object.some_attribute   # from a SQL perspective, works like "= None"

:ticket:`4354`


.. _change_4257:

info dictionary added to InstanceState
--------------------------------------

Added the ``.info`` dictionary to the :class:`.InstanceState` class, the object
that comes from calling :func:`_sa.inspect` on a mapped object.  This allows custom
recipes to add additional information about an object that will be carried
along with that object's full lifecycle in memory::

    from sqlalchemy import inspect

    u1 = User(id=7, name='ed')

    inspect(u1).info['user_info'] = '7|ed'


:ticket:`4257`

.. _change_4196:

Horizontal Sharding extension supports bulk update and delete methods
---------------------------------------------------------------------

The :class:`.ShardedQuery` extension object supports the :meth:`_query.Query.update`
and :meth:`_query.Query.delete` bulk update/delete methods.    The ``query_chooser``
callable is consulted when they are called in order to run the update/delete
across multiple shards based on given criteria.

:ticket:`4196`

Association Proxy Improvements
-------------------------------

While not for any particular reason, the Association Proxy extension
had many improvements this cycle.

.. _change_4308:

Association proxy has new cascade_scalar_deletes flag
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

An assignment to ``A.b`` will generate an ``AB`` object::

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

AssociationProxy stores class-specific state on a per-class basis
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

.. _change_4351:

AssociationProxy now provides standard column operators for a column-oriented target
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Given an :class:`.AssociationProxy` where the target is a database column,
and is **not** an object reference or another association proxy::

    class User(Base):
        # ...

        elements = relationship("Element")

        # column-based association proxy
        values = association_proxy("elements", "value")

    class Element(Base):
        # ...

        value = Column(String)

The ``User.values`` association proxy refers to the ``Element.value`` column.
Standard column operations are now available, such as ``like``::

    >>> print(s.query(User).filter(User.values.like('%foo%')))
    SELECT "user".id AS user_id
    FROM "user"
    WHERE EXISTS (SELECT 1
    FROM element
    WHERE "user".id = element.user_id AND element.value LIKE :value_1)

``equals``::

    >>> print(s.query(User).filter(User.values == 'foo'))
    SELECT "user".id AS user_id
    FROM "user"
    WHERE EXISTS (SELECT 1
    FROM element
    WHERE "user".id = element.user_id AND element.value = :value_1)

When comparing to ``None``, the ``IS NULL`` expression is augmented with
a test that the related row does not exist at all; this is the same
behavior as before::

    >>> print(s.query(User).filter(User.values == None))
    SELECT "user".id AS user_id
    FROM "user"
    WHERE (EXISTS (SELECT 1
    FROM element
    WHERE "user".id = element.user_id AND element.value IS NULL)) OR NOT (EXISTS (SELECT 1
    FROM element
    WHERE "user".id = element.user_id))

Note that the :meth:`.ColumnOperators.contains` operator is in fact a string
comparison operator; **this is a change in behavior** in that previously,
the association proxy used ``.contains`` as a list containment operator only.
With a column-oriented comparison, it now behaves like a "like"::

    >>> print(s.query(User).filter(User.values.contains('foo')))
    SELECT "user".id AS user_id
    FROM "user"
    WHERE EXISTS (SELECT 1
    FROM element
    WHERE "user".id = element.user_id AND (element.value LIKE '%' || :value_1 || '%'))

In order to test the ``User.values`` collection for simple membership of the value
``"foo"``, the equals operator (e.g. ``User.values == 'foo'``) should be used;
this works in previous versions as well.

When using an object-based association proxy with a collection, the behavior is
as before, that of testing for collection membership, e.g. given a mapping::

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        user_elements = relationship("UserElement")

        # object-based association proxy
        elements = association_proxy("user_elements", "element")


    class UserElement(Base):
        __tablename__ = 'user_element'

        id = Column(Integer, primary_key=True)
        user_id = Column(ForeignKey("user.id"))
        element_id = Column(ForeignKey("element.id"))
        element = relationship("Element")


    class Element(Base):
        __tablename__ = 'element'

        id = Column(Integer, primary_key=True)
        value = Column(String)

The ``.contains()`` method produces the same expression as before, testing
the list of ``User.elements`` for the presence of an ``Element`` object::

    >>> print(s.query(User).filter(User.elements.contains(Element(id=1))))
    SELECT "user".id AS user_id
    FROM "user"
    WHERE EXISTS (SELECT 1
    FROM user_element
    WHERE "user".id = user_element.user_id AND :param_1 = user_element.element_id)

Overall, the change is enabled based on the architectural change that is
part of :ref:`change_3423`; as the proxy now spins off additional state when
an expression is generated, there is both an object-target and a column-target
version of the :class:`.AssociationProxyInstance` class.

:ticket:`4351`

Association Proxy now Strong References the Parent Object
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

.. _change_2642:

Implemented bulk replace for sets, dicts with AssociationProxy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Assignment of a set or dictionary to an association proxy collection should
now work correctly, whereas before it would re-create association
proxy members for existing keys, leading to the issue of potential flush
failures due to the delete+insert of the same object it now should only create
new association objects where appropriate::

    class A(Base):
        __tablename__ = "test_a"

        id = Column(Integer, primary_key=True)
        b_rel = relationship(
            "B", collection_class=set, cascade="all, delete-orphan",
        )
        b = association_proxy("b_rel", "value", creator=lambda x: B(value=x))


    class B(Base):
        __tablename__ = "test_b"
        __table_args__ = (UniqueConstraint("a_id", "value"),)

        id = Column(Integer, primary_key=True)
        a_id = Column(Integer, ForeignKey("test_a.id"), nullable=False)
        value = Column(String)

    # ...

    s = Session(e)
    a = A(b={"x", "y", "z"})
    s.add(a)
    s.commit()

    # re-assign where one B should be deleted, one B added, two
    # B's maintained
    a.b = {"x", "z", "q"}

    # only 'q' was added, so only one new B object.  previously
    # all three would have been re-created leading to flush conflicts
    # against the deleted ones.
    assert len(s.new) == 1


:ticket:`2642`

.. _change_1103:

Many-to-one backref checks for collection duplicates during remove operation
----------------------------------------------------------------------------

When an ORM-mapped collection that existed as a Python sequence, typically a
Python ``list`` as is the default for :func:`_orm.relationship`, contained
duplicates, and the object were removed from one of its positions but not the
other(s),  a many-to-one backref would set its attribute to ``None`` even
though the one-to-many side still represented the object as present.  Even
though one-to-many collections cannot have duplicates in the relational model,
an ORM-mapped :func:`_orm.relationship` that uses a sequence collection can have
duplicates inside of it in memory, with the restriction that this duplicate
state can neither be persisted nor retrieved from the database.   In particular,
having a duplicate temporarily present in the list is intrinsic to a Python
"swap" operation.  Given a standard one-to-many/many-to-one setup::

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)
        bs = relationship("B", backref="a")


    class B(Base):
        __tablename__ = 'b'
        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey("a.id"))

If we have an ``A`` object with two ``B`` members, and perform a swap::

    a1 = A(bs=[B(), B()])

    a1.bs[0], a1.bs[1] = a1.bs[1], a1.bs[0]

During the above operation, interception of the standard Python ``__setitem__``
``__delitem__`` methods delivers an interim state where the second ``B()``
object is present twice in the collection.  When the ``B()`` object is removed
from one of the positions, the ``B.a`` backref would set the reference to
``None``, causing the link between the ``A`` and ``B`` object to be removed
during the flush.   The same issue can be demonstrated using plain duplicates::

    >>> a1 = A()
    >>> b1 = B()
    >>> a1.bs.append(b1)
    >>> a1.bs.append(b1)  # append the same b1 object twice
    >>> del a1.bs[1]
    >>> a1.bs  # collection is unaffected so far...
    [<__main__.B object at 0x7f047af5fb70>]
    >>> b1.a   # however b1.a is None
    >>>
    >>> session.add(a1)
    >>> session.commit()  # so upon flush + expire....
    >>> a1.bs  # the value is gone
    []

The fix ensures that when the backref fires off, which is before the collection
is mutated, the collection is checked for exactly one or zero instances of
the target item before unsetting the many-to-one side, using a linear search
which at the moment makes use of ``list.search`` and ``list.__contains__``.

Originally it was thought that an event-based reference counting scheme would
need to be used within the collection internals so that all duplicate instances
could be tracked throughout the lifecycle of the collection, which would have
added a performance/memory/complexity impact to all collection operations,
including the very frequent operations of loading and appending.  The approach
that is taken instead limits the  additional expense  to the less common
operations of collection removal and bulk replacement, and the observed
overhead of the linear scan is negligible; linear scans of relationship-bound
collections are already used within the unit of work as well as when a
collection is bulk replaced.


:ticket:`1103`

Key Behavioral Changes - ORM
=============================

.. _change_4365:

Query.join() handles ambiguity in deciding the "left" side more explicitly
---------------------------------------------------------------------------

Historically, given a query like the following::

    u_alias = aliased(User)
    session.query(User, u_alias).join(Address)

given the standard tutorial mappings, the query would produce a FROM clause
as:

.. sourcecode:: sql

    SELECT ...
    FROM users AS users_1, users JOIN addresses ON users.id = addresses.user_id

That is, the JOIN would implicitly be against the first entity that matches.
The new behavior is that an exception requests that this ambiguity be
resolved::

    sqlalchemy.exc.InvalidRequestError: Can't determine which FROM clause to
    join from, there are multiple FROMS which can join to this entity.
    Try adding an explicit ON clause to help resolve the ambiguity.

The solution is to provide an ON clause, either as an expression::

    # join to User
    session.query(User, u_alias).join(Address, Address.user_id == User.id)

    # join to u_alias
    session.query(User, u_alias).join(Address, Address.user_id == u_alias.id)

Or to use the relationship attribute, if available::

    # join to User
    session.query(User, u_alias).join(Address, User.addresses)

    # join to u_alias
    session.query(User, u_alias).join(Address, u_alias.addresses)

The change includes that a join can now correctly link to a FROM clause that
is not the first element in the list if the join is otherwise non-ambiguous::

    session.query(func.current_timestamp(), User).join(Address)

Prior to this enhancement, the above query would raise::

    sqlalchemy.exc.InvalidRequestError: Don't know how to join from
    CURRENT_TIMESTAMP; please use select_from() to establish the
    left entity/selectable of this join

Now the query works fine:

.. sourcecode:: sql

    SELECT CURRENT_TIMESTAMP AS current_timestamp_1, users.id AS users_id,
    users.name AS users_name, users.fullname AS users_fullname,
    users.password AS users_password
    FROM users JOIN addresses ON users.id = addresses.user_id

Overall the change is directly towards Python's "explicit is better than
implicit" philosophy.

:ticket:`4365`




.. _change_4246:

FOR UPDATE clause is rendered within the joined eager load subquery as well as outside
--------------------------------------------------------------------------------------

This change applies specifically to the use of the :func:`_orm.joinedload` loading
strategy in conjunction with a row limited query, e.g. using :meth:`_query.Query.first`
or :meth:`_query.Query.limit`, as well as with use of the :meth:`_query.Query.with_for_update` method.

Given a query as::

    session.query(A).options(joinedload(A.b)).limit(5)

The :class:`_query.Query` object renders a SELECT of the following form when joined
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
the rows inside the subquery, unlike that of PostgreSQL and other databases.
So the above query now renders as::

    SELECT subq.a_id, subq.a_data, b_alias.id, b_alias.data FROM (
        SELECT a.id AS a_id, a.data AS a_data FROM a LIMIT 5 FOR UPDATE
    ) AS subq LEFT OUTER JOIN b ON subq.a_id=b.a_id FOR UPDATE

On the Oracle dialect, the inner "FOR UPDATE" is not rendered as Oracle does
not support this syntax and the dialect skips any "FOR UPDATE" that is against
a subquery; it isn't necessary in any case since Oracle, like PostgreSQL,
correctly locks all elements of the returned row.

When using the :paramref:`.Query.with_for_update.of` modifier, typically on
PostgreSQL, the outer "FOR UPDATE" is omitted, and the OF is now rendered
on the inside; previously, the OF target would not be converted to accommodate
for the subquery correctly.  So
given::

    session.query(A).options(joinedload(A.b)).with_for_update(of=A).limit(5)

The query would now render as::

    SELECT subq.a_id, subq.a_data, b_alias.id, b_alias.data FROM (
        SELECT a.id AS a_id, a.data AS a_data FROM a LIMIT 5 FOR UPDATE OF a
    ) AS subq LEFT OUTER JOIN b ON subq.a_id=b.a_id

The above form should be helpful on PostgreSQL additionally since PostgreSQL
will not allow the FOR UPDATE clause to be rendered after the LEFT OUTER JOIN
target.

Overall, FOR UPDATE remains highly specific to the target database in use
and can't easily be generalized for more complex queries.

:ticket:`4246`

.. _change_3844:

passive_deletes='all' will leave FK unchanged for object removed from collection
--------------------------------------------------------------------------------

The :paramref:`_orm.relationship.passive_deletes` option accepts the value
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


New Features and Improvements - Core
====================================

.. _change_3989:

New multi-column naming convention tokens, long name truncation
----------------------------------------------------------------

To suit the case where a :class:`_schema.MetaData` naming convention needs to
disambiguate between multiple-column constraints and wishes to use all the
columns within the generated constraint name, a new series of
naming convention tokens are added, including
``column_0N_name``, ``column_0_N_name``, ``column_0N_key``, ``column_0_N_key``,
``referred_column_0N_name``, ``referred_column_0_N_name``, etc., which render
the column name (or key or label) for all columns in the constraint,
joined together either with no separator or with an underscore
separator.  Below we define a convention that will name :class:`.UniqueConstraint`
constraints with a name that joins together the names of all columns::

    metadata = MetaData(naming_convention={
        "uq": "uq_%(table_name)s_%(column_0_N_name)s"
    })

    table = Table(
        'info', metadata,
        Column('a', Integer),
        Column('b', Integer),
        Column('c', Integer),
        UniqueConstraint('a', 'b', 'c')
    )

The CREATE TABLE for the above table will render as::

    CREATE TABLE info (
        a INTEGER,
        b INTEGER,
        c INTEGER,
        CONSTRAINT uq_info_a_b_c UNIQUE (a, b, c)
    )

In addition, long-name truncation logic is now applied to the names generated
by naming conventions, in particular to accommodate for multi-column labels
that can produce very long names.  This logic, which is the same as that used
for truncating long label names in a SELECT statement, replaces excess
characters that go over the identifier-length limit for the target database
with a deterministically generated 4-character hash.  For example, on
PostgreSQL where identifiers cannot be longer than 63 characters, a long
constraint name would normally be generated from the table definition below::

    long_names = Table(
        'long_names', metadata,
        Column('information_channel_code', Integer, key='a'),
        Column('billing_convention_name', Integer, key='b'),
        Column('product_identifier', Integer, key='c'),
        UniqueConstraint('a', 'b', 'c')
    )

The truncation logic will ensure a too-long name isn't generated for the
UNIQUE constraint::

    CREATE TABLE long_names (
        information_channel_code INTEGER,
        billing_convention_name INTEGER,
        product_identifier INTEGER,
        CONSTRAINT uq_long_names_information_channel_code_billing_conventi_a79e
        UNIQUE (information_channel_code, billing_convention_name, product_identifier)
    )

The above suffix ``a79e`` is based on the md5 hash of the long name and will
generate the same value every time to produce consistent names for a given
schema.

Note that the truncation logic also raises :class:`.IdentifierError` when a
constraint name is explicitly too large for a given dialect.  This has been
the behavior for an :class:`.Index` object for a long time, but is now applied
to other kinds of constraints as well::

    from sqlalchemy import Column
    from sqlalchemy import Integer
    from sqlalchemy import MetaData
    from sqlalchemy import Table
    from sqlalchemy import UniqueConstraint
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import AddConstraint

    m = MetaData()
    t = Table("t", m, Column("x", Integer))
    uq = UniqueConstraint(
        t.c.x,
        name="this_is_too_long_of_a_name_for_any_database_backend_even_postgresql",
    )

    print(AddConstraint(uq).compile(dialect=postgresql.dialect()))

will output::

    sqlalchemy.exc.IdentifierError: Identifier
    'this_is_too_long_of_a_name_for_any_database_backend_even_postgresql'
    exceeds maximum length of 63 characters

The exception raise prevents the production of non-deterministic constraint
names truncated by the database backend which are then not compatible with
database migrations later on.

To apply SQLAlchemy-side truncation rules to the above identifier, use the
:func:`.conv` construct::

    uq = UniqueConstraint(
        t.c.x,
        name=conv("this_is_too_long_of_a_name_for_any_database_backend_even_postgresql"),
    )

This will again output deterministically truncated SQL as in::

    ALTER TABLE t ADD CONSTRAINT this_is_too_long_of_a_name_for_any_database_backend_eve_ac05 UNIQUE (x)

There is not at the moment an option to have the names pass through to allow
database-side truncation.  This has already been the case for :class:`.Index`
names for some time and issues have not been raised.

The change also repairs two other issues.  One is that the  ``column_0_key``
token wasn't available even though this token was documented, the other was
that the ``referred_column_0_name`` token would  inadvertently render the
``.key`` and not the ``.name`` of the column if these two values were
different.

.. seealso::

    :ref:`constraint_naming_conventions`

    :paramref:`_schema.MetaData.naming_convention`

:ticket:`3989`

.. _change_3831:

Binary comparison interpretation for SQL functions
--------------------------------------------------

This enhancement is implemented at the Core level, however is applicable
primarily to the ORM.

A SQL function that compares two elements can now be used as a "comparison"
object, suitable for usage in an ORM :func:`_orm.relationship`, by first
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

Above, the :paramref:`_orm.relationship.primaryjoin` of the "descendants" relationship
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
backend, such as "SELECT CAST(NULL AS INTEGER) WHERE 1!=1" for PostgreSQL,
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

The feature also works for tuple-oriented IN statements, where the "empty IN"
expression will be expanded to support the elements given inside the tuple,
such as on PostgreSQL::

    >>> from sqlalchemy import create_engine
    >>> from sqlalchemy import select, literal_column, tuple_, bindparam
    >>> e = create_engine("postgresql://scott:tiger@localhost/test", echo=True)
    >>> with e.connect() as conn:
    ...      conn.execute(
    ...          select([literal_column('1')]).
    ...          where(tuple_(50, "somestring").in_(bindparam('q', expanding=True))),
    ...          q=[]
    ...      )
    ...
    SELECT 1 WHERE (%(param_1)s, %(param_2)s)
    IN (SELECT CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR) WHERE 1!=1)


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

The above expression will render a function within SQL when used on SQLite only::

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

The connection pool usually used by :func:`_sa.create_engine` is known
as :class:`.QueuePool`.  This pool uses an object equivalent to Python's
built-in ``Queue`` class in order to store database connections waiting
to be used.   The ``Queue`` features first-in-first-out behavior, which is
intended to provide a round-robin use of the database connections that are
persistently in the pool.   However, a potential downside of this is that
when the utilization of the pool is low, the re-use of each connection in series
means that a server-side timeout strategy that attempts to reduce unused
connections is prevented from shutting down these connections.   To suit
this use case, a new flag :paramref:`_sa.create_engine.pool_use_lifo` is added
which reverses the ``.get()`` method of the ``Queue`` to pull the connection
from the beginning of the queue instead of the end, essentially turning the
"queue" into a "stack" (adding a whole new pool called ``StackPool`` was
considered, however this was too much verbosity).

.. seealso::

    :ref:`pool_use_lifo`




Key Changes - Core
==================

.. _change_4481:

Coercion of string SQL fragments to text() fully removed
---------------------------------------------------------

The warnings that were first added in version 1.0, described at
:ref:`migration_2992`, have now been converted into exceptions.    Continued
concerns have been raised regarding the automatic coercion of string fragments
passed to methods like :meth:`_query.Query.filter` and :meth:`_expression.Select.order_by` being
converted to :func:`_expression.text` constructs, even though this has emitted a warning.
In the case of :meth:`_expression.Select.order_by`, :meth:`_query.Query.order_by`,
:meth:`_expression.Select.group_by`, and :meth:`_query.Query.group_by`, a string label or column
name is still resolved into the corresponding expression construct, however if
the resolution fails, a :class:`.CompileError` is raised, thus preventing raw
SQL text from being rendered directly.

:ticket:`4481`

.. _change_4393_threadlocal:

"threadlocal" engine strategy deprecated
-----------------------------------------

The "threadlocal engine strategy" was added around SQLAlchemy 0.2, as a
solution to the problem that the standard way of operating in SQLAlchemy 0.1,
which can be summed up as "threadlocal everything",  was found to be lacking.
In retrospect, it seems fairly absurd that by SQLAlchemy's first releases which
were in every regard "alpha", that there was concern that too many users had
already settled on the existing API to simply change it.

The original usage model for SQLAlchemy looked like this::

    engine.begin()

    table.insert().execute(<params>)
    result = table.select().execute()

    table.update().execute(<params>)

    engine.commit()

After a few months of real world use, it was clear that trying to pretend a
"connection" or a "transaction" was a hidden implementation detail was a bad
idea, particularly the moment someone needed to deal with more than one
database connection at a time.   So the usage paradigm we see today was
introduced, minus the context managers since they didn't yet exist in Python::

    conn = engine.connect()
    try:
        trans = conn.begin()

        conn.execute(table.insert(), <params>)
        result = conn.execute(table.select())

        conn.execute(table.update(), <params>)

        trans.commit()
    except:
        trans.rollback()
        raise
    finally:
        conn.close()

The above paradigm was what people needed, but since it was still kind of
verbose (because no context managers), the old way of working was kept around
as well and it became the threadlocal engine strategy.

Today, working with Core is much more succinct, and even more succinct than
the original pattern, thanks to context managers::

    with engine.begin() as conn:
        conn.execute(table.insert(), <params>)
        result = conn.execute(table.select())

        conn.execute(table.update(), <params>)

At this point, any remaining code that is still relying upon the "threadlocal"
style will be encouraged via this deprecation to modernize - the feature should
be removed totally by the next major series of SQLAlchemy, e.g. 1.4.  The
connection pool parameter :paramref:`_pool.Pool.use_threadlocal` is also deprecated
as it does not actually have any effect in most cases, as is the
:meth:`_engine.Engine.contextual_connect` method, which is normally synonymous with
the :meth:`_engine.Engine.connect` method except in the case where the threadlocal
engine is in use.


:ticket:`4393`


.. _change_4393_convertunicode:

convert_unicode parameters deprecated
--------------------------------------

The parameters :paramref:`.String.convert_unicode` and
:paramref:`_sa.create_engine.convert_unicode` are deprecated.    The purpose of
these parameters was to instruct SQLAlchemy to ensure that incoming Python
Unicode objects under Python 2 were encoded to bytestrings before passing to
the database, and to expect bytestrings from the database to be converted back
to Python Unicode objects.   In the pre-Python 3 era, this was an enormous
ordeal to get right, as virtually all Python DBAPIs had no Unicode support
enabled by default, and most had major issues with the Unicode extensions that
they did provide.    Eventually, SQLAlchemy added C extensions, one of the
primary purposes of these extensions was to speed up the Unicode decode process
within result sets.

Once Python 3 was introduced, DBAPIs began to start supporting Unicode more
fully, and more importantly, by default.  However, the conditions under which a
particular DBAPI would or would not return Unicode data from a result, as well
as accept Python Unicode values as parameters, remained extremely complicated.
This was the beginning of the obsolescence of the "convert_unicode" flags,
because they were no longer sufficient as a means of ensuring that
encode/decode was occurring only where needed and not where it wasn't needed.
Instead, "convert_unicode" started to be automatically detected by dialects.
Part of this can be seen in the "SELECT 'test plain returns'" and "SELECT
'test_unicode_returns'" SQL emitted by an engine the first time it connects;
the dialect is testing that the current DBAPI with its current settings and
backend database connection is returning Unicode by default or not.

The end result is that end-user use of the "convert_unicode" flags should no
longer be needed in any circumstances, and if they are, the SQLAlchemy project
needs to know what those cases are and why.   Currently, hundreds of Unicode
round trip tests pass across all major databases without the use of this flag
so there is a fairly high level of confidence that they are no longer needed
except in arguable non use cases such as accessing mis-encoded data from a
legacy database, which would be better suited using custom types.


:ticket:`4393`


Dialect Improvements and Changes - PostgreSQL
=============================================

.. _change_4237:

Added basic reflection support for PostgreSQL partitioned tables
----------------------------------------------------------------

SQLAlchemy can render the "PARTITION BY" sequence within a PostgreSQL
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
back from :meth:`_reflection.Inspector.get_table_names` and additionally the columns
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
can now be explicitly ordered by passing a list of 2-tuples::

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

A new datatype :class:`_sqlite.JSON` is added which implements SQLite's json
member access functions on behalf of the :class:`_types.JSON`
base datatype.  The SQLite ``JSON_EXTRACT`` and ``JSON_QUOTE`` functions
are used by the implementation to provide basic JSON support.

Note that the name of the datatype itself as rendered in the database is
the name "JSON".   This will create a SQLite datatype with "numeric" affinity,
which normally should not be an issue except in the case of a JSON value that
consists of single integer value.  Nevertheless, following an example
in SQLite's own documentation at https://www.sqlite.org/json1.html the name
JSON is being used for its familiarity.


:ticket:`3850`

.. _change_4360:

Support for SQLite ON CONFLICT in constraints added
----------------------------------------------------

SQLite supports a non-standard ON CONFLICT clause that may be specified
for standalone constraints as well as some column-inline constraints such as
NOT NULL. Support has been added for these clauses via the ``sqlite_on_conflict``
keyword added to objects like :class:`.UniqueConstraint`  as well
as several :class:`_schema.Column` -specific variants::

    some_table = Table(
        'some_table', metadata,
        Column('id', Integer, primary_key=True, sqlite_on_conflict_primary_key='FAIL'),
        Column('data', Integer),
        UniqueConstraint('id', 'data', sqlite_on_conflict='IGNORE')
    )

The above table would render in a CREATE TABLE statement as::

    CREATE TABLE some_table (
        id INTEGER NOT NULL,
        data INTEGER,
        PRIMARY KEY (id) ON CONFLICT FAIL,
        UNIQUE (id, data) ON CONFLICT IGNORE
    )

.. seealso::

    :ref:`sqlite_on_conflict_ddl`

:ticket:`4360`

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
the flag ``use_nchar_for_unicode`` can be passed to :func:`_sa.create_engine` to
re-enable the old behavior.

As always, using the :class:`_oracle.NVARCHAR2` and :class:`_oracle.NCLOB`
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

.. _change_4369:

cx_Oracle connect arguments modernized, deprecated parameters removed
---------------------------------------------------------------------

A series of modernizations to the parameters accepted by the cx_oracle
dialect as well as the URL string:

* The deprecated parameters ``auto_setinputsizes``, ``allow_twophase``,
  ``exclude_setinputsizes`` are removed.

* The value of the ``threaded`` parameter, which has always been defaulted
  to True for the SQLAlchemy dialect, is no longer generated by default.
  The SQLAlchemy :class:`_engine.Connection` object is not considered to be thread-safe
  itself so there's no need for this flag to be passed.

* It's deprecated to pass ``threaded`` to :func:`_sa.create_engine` itself.
  To set the value of ``threaded`` to ``True``, pass it to either the
  :paramref:`_sa.create_engine.connect_args` dictionary or use the query
  string e.g. ``oracle+cx_oracle://...?threaded=true``.

* All parameters passed on the URL query string that are not otherwise
  specially consumed are now passed to the cx_Oracle.connect() function.
  A selection of these are also coerced either into cx_Oracle constants
  or booleans including ``mode``, ``purity``, ``events``, and ``threaded``.

* As was the case earlier, all cx_Oracle ``.connect()`` arguments are accepted
  via the :paramref:`_sa.create_engine.connect_args` dictionary, the documentation
  was inaccurate regarding this.

:ticket:`4369`

Dialect Improvements and Changes - SQL Server
=============================================

.. _change_4158:

Support for pyodbc fast_executemany
-----------------------------------

Pyodbc's recently added "fast_executemany" mode, available when using the
Microsoft ODBC driver, is now an option for the pyodbc / mssql dialect.
Pass it via :func:`_sa.create_engine`::

    engine = create_engine(
        "mssql+pyodbc://scott:tiger@mssql2017:1433/test?driver=ODBC+Driver+13+for+SQL+Server",
        fast_executemany=True)

.. seealso::

    :ref:`mssql_pyodbc_fastexecutemany`


:ticket:`4158`

.. _change_4362:

New parameters to affect IDENTITY start and increment, use of Sequence deprecated
---------------------------------------------------------------------------------

SQL Server as of SQL Server 2012 now supports sequences with real
``CREATE SEQUENCE`` syntax.  In :ticket:`4235`, SQLAlchemy will add support for
these using :class:`.Sequence` in the same way as for any other dialect.
However, the current situation is that :class:`.Sequence` has been repurposed
on SQL Server specifically in order to affect the "start" and "increment"
parameters for the ``IDENTITY`` specification on a primary key column.  In order
to make the transition towards normal sequences being available as well,
using :class:`.Sequence` will emit a deprecation warning throughout the
1.3 series.  In order to affect "start" and "increment", use the
new ``mssql_identity_start`` and ``mssql_identity_increment`` parameters
on :class:`_schema.Column`::

    test = Table(
        'test', metadata,
        Column(
            'id', Integer, primary_key=True, mssql_identity_start=100,
             mssql_identity_increment=10
        ),
        Column('name', String(20))
    )

In order to emit ``IDENTITY`` on a non-primary key column, which is a little-used
but valid SQL Server use case, use the :paramref:`_schema.Column.autoincrement` flag,
setting it to ``True`` on the target column, ``False`` on any integer
primary key column::


    test = Table(
        'test', metadata,
        Column('id', Integer, primary_key=True, autoincrement=False),
        Column('number', Integer, autoincrement=True)
    )

.. seealso::

    :ref:`mssql_identity`

:ticket:`4362`

:ticket:`4235`

.. _change_4500:

Changed StatementError formatting (newlines and %s)
=================================================================================

Two changes are introduced to the string representation for ``StatementError``.
The "detail" and "SQL" portions of the string representation are now
separated by newlines, and newlines that are present in the original SQL
statement are maintained.   The goal is to improve readability while still
keeping the original error message on one line for logging purposes.

This means that an error message that previously looked like this::

    sqlalchemy.exc.StatementError: (sqlalchemy.exc.InvalidRequestError) A value is required for bind parameter 'id' [SQL: 'select * from reviews\nwhere id = ?'] (Background on this error at: http://sqlalche.me/e/cd3x)

Will now look like this::

    sqlalchemy.exc.StatementError: (sqlalchemy.exc.InvalidRequestError) A value is required for bind parameter 'id'
    [SQL: select * from reviews
    where id = ?]
    (Background on this error at: http://sqlalche.me/e/cd3x)

The primary impact of this change is that consumers can no longer assume that
a complete exception message is on a single line, however the original
"error" portion that is generated from the DBAPI driver or SQLAlchemy internals
will still be on the first line.

:ticket:`4500`

