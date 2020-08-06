.. _mapping_attributes_toplevel:

.. currentmodule:: sqlalchemy.orm

Changing Attribute Behavior
===========================

.. _simple_validators:

Simple Validators
-----------------

A quick way to add a "validation" routine to an attribute is to use the
:func:`~sqlalchemy.orm.validates` decorator. An attribute validator can raise
an exception, halting the process of mutating the attribute's value, or can
change the given value into something different. Validators, like all
attribute extensions, are only called by normal userland code; they are not
issued when the ORM is populating the object::

    from sqlalchemy.orm import validates

    class EmailAddress(Base):
        __tablename__ = 'address'

        id = Column(Integer, primary_key=True)
        email = Column(String)

        @validates('email')
        def validate_email(self, key, address):
            assert '@' in address
            return address

.. versionchanged:: 1.0.0 - validators are no longer triggered within
   the flush process when the newly fetched values for primary key
   columns as well as some python- or server-side defaults are fetched.
   Prior to 1.0, validators may be triggered in those cases as well.


Validators also receive collection append events, when items are added to a
collection::

    from sqlalchemy.orm import validates

    class User(Base):
        # ...

        addresses = relationship("Address")

        @validates('addresses')
        def validate_address(self, key, address):
            assert '@' in address.email
            return address


The validation function by default does not get emitted for collection
remove events, as the typical expectation is that a value being discarded
doesn't require validation.  However, :func:`.validates` supports reception
of these events by specifying ``include_removes=True`` to the decorator.  When
this flag is set, the validation function must receive an additional boolean
argument which if ``True`` indicates that the operation is a removal::

    from sqlalchemy.orm import validates

    class User(Base):
        # ...

        addresses = relationship("Address")

        @validates('addresses', include_removes=True)
        def validate_address(self, key, address, is_remove):
            if is_remove:
                raise ValueError(
                        "not allowed to remove items from the collection")
            else:
                assert '@' in address.email
                return address

The case where mutually dependent validators are linked via a backref
can also be tailored, using the ``include_backrefs=False`` option; this option,
when set to ``False``, prevents a validation function from emitting if the
event occurs as a result of a backref::

    from sqlalchemy.orm import validates

    class User(Base):
        # ...

        addresses = relationship("Address", backref='user')

        @validates('addresses', include_backrefs=False)
        def validate_address(self, key, address):
            assert '@' in address.email
            return address

Above, if we were to assign to ``Address.user`` as in ``some_address.user = some_user``,
the ``validate_address()`` function would *not* be emitted, even though an append
occurs to ``some_user.addresses`` - the event is caused by a backref.

Note that the :func:`~.validates` decorator is a convenience function built on
top of attribute events.   An application that requires more control over
configuration of attribute change behavior can make use of this system,
described at :class:`~.AttributeEvents`.

.. autofunction:: validates

Using Custom Datatypes at the Core Level
-----------------------------------------

A non-ORM means of affecting the value of a column in a way that suits
converting data between how it is represented in Python, vs. how it is
represented in the database, can be achieved by using a custom datatype that is
applied to the mapped :class:`_schema.Table` metadata.     This is more common in the
case of some style of encoding / decoding that occurs both as data goes to the
database and as it is returned; read more about this in the Core documentation
at :ref:`types_typedecorator`.


.. _mapper_hybrids:

Using Descriptors and Hybrids
-----------------------------

A more comprehensive way to produce modified behavior for an attribute is to
use :term:`descriptors`.  These are commonly used in Python using the ``property()``
function. The standard SQLAlchemy technique for descriptors is to create a
plain descriptor, and to have it read/write from a mapped attribute with a
different name. Below we illustrate this using Python 2.6-style properties::

    class EmailAddress(Base):
        __tablename__ = 'email_address'

        id = Column(Integer, primary_key=True)

        # name the attribute with an underscore,
        # different from the column name
        _email = Column("email", String)

        # then create an ".email" attribute
        # to get/set "._email"
        @property
        def email(self):
            return self._email

        @email.setter
        def email(self, email):
            self._email = email

The approach above will work, but there's more we can add. While our
``EmailAddress`` object will shuttle the value through the ``email``
descriptor and into the ``_email`` mapped attribute, the class level
``EmailAddress.email`` attribute does not have the usual expression semantics
usable with :class:`_query.Query`. To provide these, we instead use the
:mod:`~sqlalchemy.ext.hybrid` extension as follows::

    from sqlalchemy.ext.hybrid import hybrid_property

    class EmailAddress(Base):
        __tablename__ = 'email_address'

        id = Column(Integer, primary_key=True)

        _email = Column("email", String)

        @hybrid_property
        def email(self):
            return self._email

        @email.setter
        def email(self, email):
            self._email = email

The ``.email`` attribute, in addition to providing getter/setter behavior when we have an
instance of ``EmailAddress``, also provides a SQL expression when used at the class level,
that is, from the ``EmailAddress`` class directly:

.. sourcecode:: python+sql

    from sqlalchemy.orm import Session
    session = Session()

    {sql}address = session.query(EmailAddress).\
                     filter(EmailAddress.email == 'address@example.com').\
                     one()
    SELECT address.email AS address_email, address.id AS address_id
    FROM address
    WHERE address.email = ?
    ('address@example.com',)
    {stop}

    address.email = 'otheraddress@example.com'
    {sql}session.commit()
    UPDATE address SET email=? WHERE address.id = ?
    ('otheraddress@example.com', 1)
    COMMIT
    {stop}

The :class:`~.hybrid_property` also allows us to change the behavior of the
attribute, including defining separate behaviors when the attribute is
accessed at the instance level versus at the class/expression level, using the
:meth:`.hybrid_property.expression` modifier. Such as, if we wanted to add a
host name automatically, we might define two sets of string manipulation
logic::

    class EmailAddress(Base):
        __tablename__ = 'email_address'

        id = Column(Integer, primary_key=True)

        _email = Column("email", String)

        @hybrid_property
        def email(self):
            """Return the value of _email up until the last twelve
            characters."""

            return self._email[:-12]

        @email.setter
        def email(self, email):
            """Set the value of _email, tacking on the twelve character
            value @example.com."""

            self._email = email + "@example.com"

        @email.expression
        def email(cls):
            """Produce a SQL expression that represents the value
            of the _email column, minus the last twelve characters."""

            return func.substr(cls._email, 0, func.length(cls._email) - 12)

Above, accessing the ``email`` property of an instance of ``EmailAddress``
will return the value of the ``_email`` attribute, removing or adding the
hostname ``@example.com`` from the value. When we query against the ``email``
attribute, a SQL function is rendered which produces the same effect:

.. sourcecode:: python+sql

    {sql}address = session.query(EmailAddress).filter(EmailAddress.email == 'address').one()
    SELECT address.email AS address_email, address.id AS address_id
    FROM address
    WHERE substr(address.email, ?, length(address.email) - ?) = ?
    (0, 12, 'address')
    {stop}

Read more about Hybrids at :ref:`hybrids_toplevel`.

.. _synonyms:

Synonyms
--------

Synonyms are a mapper-level construct that allow any attribute on a class
to "mirror" another attribute that is mapped.

In the most basic sense, the synonym is an easy way to make a certain
attribute available by an additional name::

    class MyClass(Base):
        __tablename__ = 'my_table'

        id = Column(Integer, primary_key=True)
        job_status = Column(String(50))

        status = synonym("job_status")

The above class ``MyClass`` has two attributes, ``.job_status`` and
``.status`` that will behave as one attribute, both at the expression
level::

    >>> print(MyClass.job_status == 'some_status')
    my_table.job_status = :job_status_1

    >>> print(MyClass.status == 'some_status')
    my_table.job_status = :job_status_1

and at the instance level::

    >>> m1 = MyClass(status='x')
    >>> m1.status, m1.job_status
    ('x', 'x')

    >>> m1.job_status = 'y'
    >>> m1.status, m1.job_status
    ('y', 'y')

The :func:`.synonym` can be used for any kind of mapped attribute that
subclasses :class:`.MapperProperty`, including mapped columns and relationships,
as well as synonyms themselves.

Beyond a simple mirror, :func:`.synonym` can also be made to reference
a user-defined :term:`descriptor`.  We can supply our
``status`` synonym with a ``@property``::

    class MyClass(Base):
        __tablename__ = 'my_table'

        id = Column(Integer, primary_key=True)
        status = Column(String(50))

        @property
        def job_status(self):
            return "Status: " + self.status

        job_status = synonym("status", descriptor=job_status)

When using Declarative, the above pattern can be expressed more succinctly
using the :func:`.synonym_for` decorator::

    from sqlalchemy.ext.declarative import synonym_for

    class MyClass(Base):
        __tablename__ = 'my_table'

        id = Column(Integer, primary_key=True)
        status = Column(String(50))

        @synonym_for("status")
        @property
        def job_status(self):
            return "Status: " + self.status

While the :func:`.synonym` is useful for simple mirroring, the use case
of augmenting attribute behavior with descriptors is better handled in modern
usage using the :ref:`hybrid attribute <mapper_hybrids>` feature, which
is more oriented towards Python descriptors.   Technically, a :func:`.synonym`
can do everything that a :class:`.hybrid_property` can do, as it also supports
injection of custom SQL capabilities, but the hybrid is more straightforward
to use in more complex situations.

.. autofunction:: synonym

.. _custom_comparators:

Operator Customization
----------------------

The "operators" used by the SQLAlchemy ORM and Core expression language
are fully customizable.  For example, the comparison expression
``User.name == 'ed'`` makes usage of an operator built into Python
itself called ``operator.eq`` - the actual SQL construct which SQLAlchemy
associates with such an operator can be modified.  New
operations can be associated with column expressions as well.   The operators
which take place for column expressions are most directly redefined at the
type level -  see the
section :ref:`types_operators` for a description.

ORM level functions like :func:`.column_property`, :func:`_orm.relationship`,
and :func:`.composite` also provide for operator redefinition at the ORM
level, by passing a :class:`.PropComparator` subclass to the ``comparator_factory``
argument of each function.  Customization of operators at this level is a
rare use case.  See the documentation at :class:`.PropComparator`
for an overview.

