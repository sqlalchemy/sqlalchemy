.. _declarative_configuring_relationships:

=========================
Configuring Relationships
=========================

.. seealso::

    This section describes specifics about how the Declarative system
    interacts with SQLAlchemy ORM relationship constructs.  For general
    information about setting up relationships between mappings,
    see :ref:`ormtutorial_toplevel` and :ref:`relationship_patterns`.

Relationships to other classes are done in the usual way, with the added
feature that the class specified to :func:`~sqlalchemy.orm.relationship`
may be a string name.  The "class registry" associated with ``Base``
is used at mapper compilation time to resolve the name into the actual
class object, which is expected to have been defined once the mapper
configuration is used::

    class User(Base):
        __tablename__ = 'users'

        id = Column(Integer, primary_key=True)
        name = Column(String(50))
        addresses = relationship("Address", backref="user")

    class Address(Base):
        __tablename__ = 'addresses'

        id = Column(Integer, primary_key=True)
        email = Column(String(50))
        user_id = Column(Integer, ForeignKey('users.id'))

Column constructs, since they are just that, are immediately usable,
as below where we define a primary join condition on the ``Address``
class using them::

    class Address(Base):
        __tablename__ = 'addresses'

        id = Column(Integer, primary_key=True)
        email = Column(String(50))
        user_id = Column(Integer, ForeignKey('users.id'))
        user = relationship(User, primaryjoin=user_id == User.id)

.. _declarative_relationship_eval:

Evaluation of relationship arguments
=====================================

In addition to the main argument for :func:`~sqlalchemy.orm.relationship`,
other arguments which depend upon the columns present on an as-yet
undefined class may also be specified as strings.   For most of these
arguments except that of the main argument, these strings are
**evaluated as Python expressions using Python's built-in eval() function.**

The full namespace available within this evaluation includes all classes mapped
for this declarative base, as well as the contents of the ``sqlalchemy``
package, including expression functions like
:func:`~sqlalchemy.sql.expression.desc` and
:attr:`~sqlalchemy.sql.expression.func`::

    class User(Base):
        # ....
        addresses = relationship("Address",
                             order_by="desc(Address.email)",
                             primaryjoin="Address.user_id==User.id")

.. warning::

    The strings accepted by the following parameters:

        :paramref:`_orm.relationship.order_by`

        :paramref:`_orm.relationship.primaryjoin`

        :paramref:`_orm.relationship.secondaryjoin`

        :paramref:`_orm.relationship.secondary`

        :paramref:`_orm.relationship.remote_side`

        :paramref:`_orm.relationship.foreign_keys`

        :paramref:`_orm.relationship._user_defined_foreign_keys`

    Are **evaluated as Python code expressions using eval().  DO NOT PASS
    UNTRUSTED INPUT TO THESE ARGUMENTS.**

    In addition, prior to version 1.3.16 of SQLAlchemy, the main
    "argument" to :func:`_orm.relationship` is also evaluated as Python
    code.  **DO NOT PASS UNTRUSTED INPUT TO THIS ARGUMENT.**

.. versionchanged:: 1.3.16

    The string evaluation of the main "argument" no longer accepts an open
    ended Python expression, instead only accepting a string class name
    or dotted package-qualified name.

For the case where more than one module contains a class of the same name,
string class names can also be specified as module-qualified paths
within any of these string expressions::

    class User(Base):
        # ....
        addresses = relationship("myapp.model.address.Address",
                             order_by="desc(myapp.model.address.Address.email)",
                             primaryjoin="myapp.model.address.Address.user_id=="
                                            "myapp.model.user.User.id")

The qualified path can be any partial path that removes ambiguity between
the names.  For example, to disambiguate between
``myapp.model.address.Address`` and ``myapp.model.lookup.Address``,
we can specify ``address.Address`` or ``lookup.Address``::

    class User(Base):
        # ....
        addresses = relationship("address.Address",
                             order_by="desc(address.Address.email)",
                             primaryjoin="address.Address.user_id=="
                                            "User.id")

Two alternatives also exist to using string-based attributes.  A lambda
can also be used, which will be evaluated after all mappers have been
configured::

    class User(Base):
        # ...
        addresses = relationship(lambda: Address,
                             order_by=lambda: desc(Address.email),
                             primaryjoin=lambda: Address.user_id==User.id)

Or, the relationship can be added to the class explicitly after the classes
are available::

    User.addresses = relationship(Address,
                              primaryjoin=Address.user_id==User.id)



.. _declarative_many_to_many:

Configuring Many-to-Many Relationships
======================================

Many-to-many relationships are also declared in the same way
with declarative as with traditional mappings. The
``secondary`` argument to
:func:`_orm.relationship` is as usual passed a
:class:`_schema.Table` object, which is typically declared in the
traditional way.  The :class:`_schema.Table` usually shares
the :class:`_schema.MetaData` object used by the declarative base::

    keyword_author = Table(
        'keyword_author', Base.metadata,
        Column('author_id', Integer, ForeignKey('authors.id')),
        Column('keyword_id', Integer, ForeignKey('keywords.id'))
        )

    class Author(Base):
        __tablename__ = 'authors'
        id = Column(Integer, primary_key=True)
        keywords = relationship("Keyword", secondary=keyword_author)

Like other :func:`~sqlalchemy.orm.relationship` arguments, a string is accepted
as well, passing the string name of the table as defined in the
``Base.metadata.tables`` collection::

    class Author(Base):
        __tablename__ = 'authors'
        id = Column(Integer, primary_key=True)
        keywords = relationship("Keyword", secondary="keyword_author")

As with traditional mapping, its generally not a good idea to use
a :class:`_schema.Table` as the "secondary" argument which is also mapped to
a class, unless the :func:`_orm.relationship` is declared with ``viewonly=True``.
Otherwise, the unit-of-work system may attempt duplicate INSERT and
DELETE statements against the underlying table.

