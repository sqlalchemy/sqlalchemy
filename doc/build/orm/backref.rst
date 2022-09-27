.. _relationships_backref:

Using the legacy 'backref' relationship parameter
--------------------------------------------------

.. note:: The :paramref:`_orm.relationship.backref` keyword should be considered
   legacy, and use of :paramref:`_orm.relationship.back_populates` with explicit
   :func:`_orm.relationship` constructs should be preferred.  Using
   individual :func:`_orm.relationship` constructs provides advantages
   including that both ORM mapped classes will include their attributes
   up front as the class is constructed, rather than as a deferred step,
   and configuration is more straightforward as all arguments are explicit.
   New :pep:`484` features in SQLAlchemy 2.0 also take advantage of
   attributes being explicitly present in source code rather than
   using dynamic attribute generation.

.. seealso::

    For general information about bidirectional relationships, see the
    following sections:

    :ref:`tutorial_orm_related_objects` - in the :ref:`unified_tutorial`,
    presents an overview of bi-directional relationship configuration
    and behaviors using :paramref:`_orm.relationship.back_populates`

    :ref:`back_populates_cascade` - notes on bi-directional :func:`_orm.relationship`
    behavior regarding :class:`_orm.Session` cascade behaviors.

    :paramref:`_orm.relationship.back_populates`


The :paramref:`_orm.relationship.backref` keyword argument on the
:func:`_orm.relationship` construct allows the
automatic generation of a new :func:`_orm.relationship` that will be automatically
be added to the ORM mapping for the related class.  It will then be
placed into a :paramref:`_orm.relationship.back_populates` configuration
against the current :func:`_orm.relationship` being configured, with both
:func:`_orm.relationship` constructs referring to each other.

Starting with the following example::

    from sqlalchemy import Column, ForeignKey, Integer, String
    from sqlalchemy.orm import DeclarativeBase, relationship


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __tablename__ = "user"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String)

        addresses = relationship("Address", backref="user")


    class Address(Base):
        __tablename__ = "address"
        id = mapped_column(Integer, primary_key=True)
        email = mapped_column(String)
        user_id = mapped_column(Integer, ForeignKey("user.id"))

The above configuration establishes a collection of ``Address`` objects on ``User`` called
``User.addresses``.   It also establishes a ``.user`` attribute on ``Address`` which will
refer to the parent ``User`` object.   Using :paramref:`_orm.relationship.back_populates`
it's equivalent to the following::

    from sqlalchemy import Column, ForeignKey, Integer, String
    from sqlalchemy.orm import DeclarativeBase, relationship


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __tablename__ = "user"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String)

        addresses = relationship("Address", back_populates="user")


    class Address(Base):
        __tablename__ = "address"
        id = mapped_column(Integer, primary_key=True)
        email = mapped_column(String)
        user_id = mapped_column(Integer, ForeignKey("user.id"))

        user = relationship("User", back_populates="addresses")

The behavior of the ``User.addresses`` and ``Address.user`` relationships
is that they now behave in a **bi-directional** way, indicating that
changes on one side of the relationship impact the other.   An example
and discussion of this behavior is in the :ref:`unified_tutorial`
at :ref:`tutorial_orm_related_objects`.


Backref Default Arguments
~~~~~~~~~~~~~~~~~~~~~~~~~

Since :paramref:`_orm.relationship.backref` generates a whole new
:func:`_orm.relationship`, the generation process by default
will attempt to include corresponding arguments in the new
:func:`_orm.relationship` that correspond to the original arguments.
As an example, below is a :func:`_orm.relationship` that includes a
:ref:`custom join condition <relationship_configure_joins>`
which also includes the :paramref:`_orm.relationship.backref` keyword::

    from sqlalchemy import Column, ForeignKey, Integer, String
    from sqlalchemy.orm import DeclarativeBase, relationship


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __tablename__ = "user"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String)

        addresses = relationship(
            "Address",
            primaryjoin=(
                "and_(User.id==Address.user_id, Address.email.startswith('tony'))"
            ),
            backref="user",
        )


    class Address(Base):
        __tablename__ = "address"
        id = mapped_column(Integer, primary_key=True)
        email = mapped_column(String)
        user_id = mapped_column(Integer, ForeignKey("user.id"))

When the "backref" is generated, the :paramref:`_orm.relationship.primaryjoin`
condition is copied to the new :func:`_orm.relationship` as well::

    >>> print(User.addresses.property.primaryjoin)
    "user".id = address.user_id AND address.email LIKE :email_1 || '%%'
    >>>
    >>> print(Address.user.property.primaryjoin)
    "user".id = address.user_id AND address.email LIKE :email_1 || '%%'
    >>>

Other arguments that are transferrable include the
:paramref:`_orm.relationship.secondary` parameter that refers to a
many-to-many association table, as well as the "join" arguments
:paramref:`_orm.relationship.primaryjoin` and
:paramref:`_orm.relationship.secondaryjoin`; "backref" is smart enough to know
that these two arguments should also be "reversed" when generating
the opposite side.

Specifying Backref Arguments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Lots of other arguments for a "backref" are not implicit, and
include arguments like
:paramref:`_orm.relationship.lazy`,
:paramref:`_orm.relationship.remote_side`,
:paramref:`_orm.relationship.cascade` and
:paramref:`_orm.relationship.cascade_backrefs`.   For this case we use
the :func:`.backref` function in place of a string; this will store
a specific set of arguments that will be transferred to the new
:func:`_orm.relationship` when generated::

    # <other imports>
    from sqlalchemy.orm import backref


    class User(Base):
        __tablename__ = "user"
        id = mapped_column(Integer, primary_key=True)
        name = mapped_column(String)

        addresses = relationship(
            "Address",
            backref=backref("user", lazy="joined"),
        )

Where above, we placed a ``lazy="joined"`` directive only on the ``Address.user``
side, indicating that when a query against ``Address`` is made, a join to the ``User``
entity should be made automatically which will populate the ``.user`` attribute of each
returned ``Address``.   The :func:`.backref` function formatted the arguments we gave
it into a form that is interpreted by the receiving :func:`_orm.relationship` as additional
arguments to be applied to the new relationship it creates.

