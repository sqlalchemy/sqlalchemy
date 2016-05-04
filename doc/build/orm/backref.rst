.. _relationships_backref:

Linking Relationships with Backref
----------------------------------

The :paramref:`~.relationship.backref` keyword argument was first introduced in :ref:`ormtutorial_toplevel`, and has been
mentioned throughout many of the examples here.   What does it actually do ?   Let's start
with the canonical ``User`` and ``Address`` scenario::

    from sqlalchemy import Integer, ForeignKey, String, Column
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String)

        addresses = relationship("Address", backref="user")

    class Address(Base):
        __tablename__ = 'address'
        id = Column(Integer, primary_key=True)
        email = Column(String)
        user_id = Column(Integer, ForeignKey('user.id'))

The above configuration establishes a collection of ``Address`` objects on ``User`` called
``User.addresses``.   It also establishes a ``.user`` attribute on ``Address`` which will
refer to the parent ``User`` object.

In fact, the :paramref:`~.relationship.backref` keyword is only a common shortcut for placing a second
:func:`.relationship` onto the ``Address`` mapping, including the establishment
of an event listener on both sides which will mirror attribute operations
in both directions.   The above configuration is equivalent to::

    from sqlalchemy import Integer, ForeignKey, String, Column
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String)

        addresses = relationship("Address", back_populates="user")

    class Address(Base):
        __tablename__ = 'address'
        id = Column(Integer, primary_key=True)
        email = Column(String)
        user_id = Column(Integer, ForeignKey('user.id'))

        user = relationship("User", back_populates="addresses")

Above, we add a ``.user`` relationship to ``Address`` explicitly.  On
both relationships, the :paramref:`~.relationship.back_populates` directive tells each relationship
about the other one, indicating that they should establish "bidirectional"
behavior between each other.   The primary effect of this configuration
is that the relationship adds event handlers to both attributes
which have the behavior of "when an append or set event occurs here, set ourselves
onto the incoming attribute using this particular attribute name".
The behavior is illustrated as follows.   Start with a ``User`` and an ``Address``
instance.  The ``.addresses`` collection is empty, and the ``.user`` attribute
is ``None``::

    >>> u1 = User()
    >>> a1 = Address()
    >>> u1.addresses
    []
    >>> print(a1.user)
    None

However, once the ``Address`` is appended to the ``u1.addresses`` collection,
both the collection and the scalar attribute have been populated::

    >>> u1.addresses.append(a1)
    >>> u1.addresses
    [<__main__.Address object at 0x12a6ed0>]
    >>> a1.user
    <__main__.User object at 0x12a6590>

This behavior of course works in reverse for removal operations as well, as well
as for equivalent operations on both sides.   Such as
when ``.user`` is set again to ``None``, the ``Address`` object is removed
from the reverse collection::

    >>> a1.user = None
    >>> u1.addresses
    []

The manipulation of the ``.addresses`` collection and the ``.user`` attribute
occurs entirely in Python without any interaction with the SQL database.
Without this behavior, the proper state would be apparent on both sides once the
data has been flushed to the database, and later reloaded after a commit or
expiration operation occurs.  The :paramref:`~.relationship.backref`/:paramref:`~.relationship.back_populates` behavior has the advantage
that common bidirectional operations can reflect the correct state without requiring
a database round trip.

Remember, when the :paramref:`~.relationship.backref` keyword is used on a single relationship, it's
exactly the same as if the above two relationships were created individually
using :paramref:`~.relationship.back_populates` on each.

Backref Arguments
~~~~~~~~~~~~~~~~~~

We've established that the :paramref:`~.relationship.backref` keyword is merely a shortcut for building
two individual :func:`.relationship` constructs that refer to each other.  Part of
the behavior of this shortcut is that certain configurational arguments applied to
the :func:`.relationship`
will also be applied to the other direction - namely those arguments that describe
the relationship at a schema level, and are unlikely to be different in the reverse
direction.  The usual case
here is a many-to-many :func:`.relationship` that has a :paramref:`~.relationship.secondary` argument,
or a one-to-many or many-to-one which has a :paramref:`~.relationship.primaryjoin` argument (the
:paramref:`~.relationship.primaryjoin` argument is discussed in :ref:`relationship_primaryjoin`).  Such
as if we limited the list of ``Address`` objects to those which start with "tony"::

    from sqlalchemy import Integer, ForeignKey, String, Column
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String)

        addresses = relationship("Address",
                        primaryjoin="and_(User.id==Address.user_id, "
                            "Address.email.startswith('tony'))",
                        backref="user")

    class Address(Base):
        __tablename__ = 'address'
        id = Column(Integer, primary_key=True)
        email = Column(String)
        user_id = Column(Integer, ForeignKey('user.id'))

We can observe, by inspecting the resulting property, that both sides
of the relationship have this join condition applied::

    >>> print(User.addresses.property.primaryjoin)
    "user".id = address.user_id AND address.email LIKE :email_1 || '%%'
    >>>
    >>> print(Address.user.property.primaryjoin)
    "user".id = address.user_id AND address.email LIKE :email_1 || '%%'
    >>>

This reuse of arguments should pretty much do the "right thing" - it
uses only arguments that are applicable, and in the case of a many-to-
many relationship, will reverse the usage of
:paramref:`~.relationship.primaryjoin` and
:paramref:`~.relationship.secondaryjoin` to correspond to the other
direction (see the example in :ref:`self_referential_many_to_many` for
this).

It's very often the case however that we'd like to specify arguments
that are specific to just the side where we happened to place the
"backref". This includes :func:`.relationship` arguments like
:paramref:`~.relationship.lazy`,
:paramref:`~.relationship.remote_side`,
:paramref:`~.relationship.cascade` and
:paramref:`~.relationship.cascade_backrefs`.   For this case we use
the :func:`.backref` function in place of a string::

    # <other imports>
    from sqlalchemy.orm import backref

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String)

        addresses = relationship("Address",
                        backref=backref("user", lazy="joined"))

Where above, we placed a ``lazy="joined"`` directive only on the ``Address.user``
side, indicating that when a query against ``Address`` is made, a join to the ``User``
entity should be made automatically which will populate the ``.user`` attribute of each
returned ``Address``.   The :func:`.backref` function formatted the arguments we gave
it into a form that is interpreted by the receiving :func:`.relationship` as additional
arguments to be applied to the new relationship it creates.

One Way Backrefs
~~~~~~~~~~~~~~~~~

An unusual case is that of the "one way backref".   This is where the
"back-populating" behavior of the backref is only desirable in one
direction. An example of this is a collection which contains a
filtering :paramref:`~.relationship.primaryjoin` condition.   We'd
like to append items to this collection as needed, and have them
populate the "parent" object on the incoming object. However, we'd
also like to have items that are not part of the collection, but still
have the same "parent" association - these items should never be in
the collection.

Taking our previous example, where we established a
:paramref:`~.relationship.primaryjoin` that limited the collection
only to ``Address`` objects whose email address started with the word
``tony``, the usual backref behavior is that all items populate in
both directions.   We wouldn't want this behavior for a case like the
following::

    >>> u1 = User()
    >>> a1 = Address(email='mary')
    >>> a1.user = u1
    >>> u1.addresses
    [<__main__.Address object at 0x1411910>]

Above, the ``Address`` object that doesn't match the criterion of "starts with 'tony'"
is present in the ``addresses`` collection of ``u1``.   After these objects are flushed,
the transaction committed and their attributes expired for a re-load, the ``addresses``
collection will hit the database on next access and no longer have this ``Address`` object
present, due to the filtering condition.   But we can do away with this unwanted side
of the "backref" behavior on the Python side by using two separate :func:`.relationship` constructs,
placing :paramref:`~.relationship.back_populates` only on one side::

    from sqlalchemy import Integer, ForeignKey, String, Column
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String)
        addresses = relationship("Address",
                        primaryjoin="and_(User.id==Address.user_id, "
                            "Address.email.startswith('tony'))",
                        back_populates="user")

    class Address(Base):
        __tablename__ = 'address'
        id = Column(Integer, primary_key=True)
        email = Column(String)
        user_id = Column(Integer, ForeignKey('user.id'))
        user = relationship("User")

With the above scenario, appending an ``Address`` object to the ``.addresses``
collection of a ``User`` will always establish the ``.user`` attribute on that
``Address``::

    >>> u1 = User()
    >>> a1 = Address(email='tony')
    >>> u1.addresses.append(a1)
    >>> a1.user
    <__main__.User object at 0x1411850>

However, applying a ``User`` to the ``.user`` attribute of an ``Address``,
will not append the ``Address`` object to the collection::

    >>> a2 = Address(email='mary')
    >>> a2.user = u1
    >>> a2 in u1.addresses
    False

Of course, we've disabled some of the usefulness of
:paramref:`~.relationship.backref` here, in that when we do append an
``Address`` that corresponds to the criteria of
``email.startswith('tony')``, it won't show up in the
``User.addresses`` collection until the session is flushed, and the
attributes reloaded after a commit or expire operation.   While we
could consider an attribute event that checks this criterion in
Python, this starts to cross the line of duplicating too much SQL
behavior in Python.  The backref behavior itself is only a slight
transgression of this philosophy - SQLAlchemy tries to keep these to a
minimum overall.
