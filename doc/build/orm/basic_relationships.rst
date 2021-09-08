.. _relationship_patterns:

Basic Relationship Patterns
---------------------------

A quick walkthrough of the basic relational patterns.

The imports used for each of the following sections is as follows::

    from sqlalchemy import Table, Column, Integer, ForeignKey
    from sqlalchemy.orm import relationship
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()


.. _relationship_patterns_o2m:

One To Many
~~~~~~~~~~~

A one to many relationship places a foreign key on the child table referencing
the parent.  :func:`_orm.relationship` is then specified on the parent, as referencing
a collection of items represented by the child::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        children = relationship("Child")

    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('parent.id'))

To establish a bidirectional relationship in one-to-many, where the "reverse"
side is a many to one, specify an additional :func:`_orm.relationship` and connect
the two using the :paramref:`_orm.relationship.back_populates` parameter::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        children = relationship("Child", back_populates="parent")

    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('parent.id'))
        parent = relationship("Parent", back_populates="children")

``Child`` will get a ``parent`` attribute with many-to-one semantics.

Alternatively, the :paramref:`_orm.relationship.backref` option may be used
on a single :func:`_orm.relationship` instead of using
:paramref:`_orm.relationship.back_populates`::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        children = relationship("Child", backref="parent")

Configuring Delete Behavior for One to Many
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is often the case that all ``Child`` objects should be deleted
when their owning ``Parent`` is deleted.  To configure this behavior,
the ``delete`` cascade option described at :ref:`cascade_delete` is used.
An additional option is that a ``Child`` object can itself be deleted when
it is deassociated from its parent.  This behavior is described at
:ref:`cascade_delete_orphan`.

.. seealso::

    :ref:`cascade_delete`

    :ref:`passive_deletes`

    :ref:`cascade_delete_orphan`


.. _relationship_patterns_m2o:

Many To One
~~~~~~~~~~~

Many to one places a foreign key in the parent table referencing the child.
:func:`_orm.relationship` is declared on the parent, where a new scalar-holding
attribute will be created::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        child_id = Column(Integer, ForeignKey('child.id'))
        child = relationship("Child")

    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)

Bidirectional behavior is achieved by adding a second :func:`_orm.relationship`
and applying the :paramref:`_orm.relationship.back_populates` parameter
in both directions::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        child_id = Column(Integer, ForeignKey('child.id'))
        child = relationship("Child", back_populates="parents")

    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
        parents = relationship("Parent", back_populates="child")

Alternatively, the :paramref:`_orm.relationship.backref` parameter
may be applied to a single :func:`_orm.relationship`, such as ``Parent.child``::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        child_id = Column(Integer, ForeignKey('child.id'))
        child = relationship("Child", backref="parents")

.. _relationships_one_to_one:

One To One
~~~~~~~~~~

One To One is essentially a bidirectional relationship with a scalar
attribute on both sides.  Within the ORM, "one-to-one" is considered as a
convention where the ORM expects that only one related row will exist
for any parent row.

The "one-to-one" convention is achieved by applying a value of
``False`` to the :paramref:`_orm.relationship.uselist` parameter of the
:func:`_orm.relationship` construct, or in some cases the :func:`_orm.backref`
construct, applying it on the "one-to-many" or "collection" side of a
relationship.

In the example below we present a bidirectional relationship that includes
both :ref:`one-to-many <relationship_patterns_o2m>` (``Parent.children``) and
a :ref:`many-to-one <relationship_patterns_m2o>` (``Child.parent``)
relationships::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)

        # one-to-many collection
        children = relationship("Child", back_populates="parent")

    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('parent.id'))

        # many-to-one scalar
        parent = relationship("Parent", back_populates="children")

Above, ``Parent.children`` is the "one-to-many" side referring to a collection,
and ``Child.parent`` is the "many-to-one" side referring to a single object.
To convert this to "one-to-one", the "one-to-many" or "collection" side
is converted into a scalar relationship using the ``uselist=False`` flag,
renaming ``Parent.children`` to ``Parent.child`` for clarity::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)

        # previously one-to-many Parent.children is now
        # one-to-one Parent.child
        child = relationship("Child", back_populates="parent", uselist=False)

    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('parent.id'))

        # many-to-one side remains, see tip below
        parent = relationship("Parent", back_populates="child")

Above, when we load a ``Parent`` object, the ``Parent.child`` attribute
will refer to a single ``Child`` object rather than a collection.  If we
replace the value of ``Parent.child`` with a new ``Child`` object, the ORM's
unit of work process will replace the previous ``Child`` row with the new one,
setting the previous ``child.parent_id`` column to NULL by default unless there
are specific :ref:`cascade <unitofwork_cascades>` behaviors set up.

.. tip::

  As mentioned previously, the ORM considers the "one-to-one" pattern as a
  convention, where it makes the assumption that when it loads the
  ``Parent.child`` attribute on a ``Parent`` object, it will get only one
  row back.  If more than one row is returned, the ORM will emit a warning.

  However, the ``Child.parent`` side of the above relationship remains as a
  "many-to-one" relationship and is unchanged, and there is no intrinsic system
  within the ORM itself that prevents more than one ``Child`` object to be
  created against the same ``Parent`` during persistence.  Instead, techniques
  such as :ref:`unique constraints <schema_unique_constraint>` may be used in
  the actual database schema to enforce this arrangement, where a unique
  constraint on the ``Child.parent_id`` column would ensure that only
  one ``Child`` row may refer to a particular ``Parent`` row at a time.


In the case where the :paramref:`_orm.relationship.backref`
parameter is used to define the "one-to-many" side, this can be converted
to the "one-to-one" convention using the :func:`_orm.backref`
function which allows the relationship generated by the
:paramref:`_orm.relationship.backref` parameter to receive custom parameters,
in this case the ``uselist`` parameter::

    from sqlalchemy.orm import backref

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)

    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('parent.id'))
        parent = relationship("Parent", backref=backref("child", uselist=False))



.. _relationships_many_to_many:

Many To Many
~~~~~~~~~~~~

Many to Many adds an association table between two classes. The association
table is indicated by the :paramref:`_orm.relationship.secondary` argument to
:func:`_orm.relationship`.  Usually, the :class:`_schema.Table` uses the
:class:`_schema.MetaData` object associated with the declarative base
class, so that the :class:`_schema.ForeignKey` directives can locate the
remote tables with which to link::

    association_table = Table('association', Base.metadata,
        Column('left_id', ForeignKey('left.id')),
        Column('right_id', ForeignKey('right.id'))
    )

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship("Child",
                        secondary=association_table)

    class Child(Base):
        __tablename__ = 'right'
        id = Column(Integer, primary_key=True)

.. tip::

    The "association table" above has foreign key constraints established that
    refer to the two entity tables on either side of the relationship.  The data
    type of each of ``association.left_id`` and ``association.right_id`` is
    normally inferred from that of the referenced table and may be omitted.
    It is also **recommended**, though not in any way required by SQLAlchemy,
    that the columns which refer to the two entity tables are established within
    either a **unique constraint** or more commonly as the **primary key constraint**;
    this ensures that duplicate rows won't be persisted within the table regardless
    of issues on the application side::

        association_table = Table('association', Base.metadata,
            Column('left_id', ForeignKey('left.id'), primary_key=True),
            Column('right_id', ForeignKey('right.id'), primary_key=True)
        )

For a bidirectional relationship, both sides of the relationship contain a
collection.  Specify using :paramref:`_orm.relationship.back_populates`, and
for each :func:`_orm.relationship` specify the common association table::

    association_table = Table('association', Base.metadata,
        Column('left_id', ForeignKey('left.id'), primary_key=True),
        Column('right_id', ForeignKey('right.id'), primary_key=True)
    )

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship(
            "Child",
            secondary=association_table,
            back_populates="parents")

    class Child(Base):
        __tablename__ = 'right'
        id = Column(Integer, primary_key=True)
        parents = relationship(
            "Parent",
            secondary=association_table,
            back_populates="children")

When using the :paramref:`_orm.relationship.backref` parameter instead of
:paramref:`_orm.relationship.back_populates`, the backref will automatically
use the same :paramref:`_orm.relationship.secondary` argument for the
reverse relationship::

    association_table = Table('association', Base.metadata,
        Column('left_id', ForeignKey('left.id'), primary_key=True),
        Column('right_id', ForeignKey('right.id'), primary_key=True)
    )

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship("Child",
                        secondary=association_table,
                        backref="parents")

    class Child(Base):
        __tablename__ = 'right'
        id = Column(Integer, primary_key=True)

The :paramref:`_orm.relationship.secondary` argument of
:func:`_orm.relationship` also accepts a callable that returns the ultimate
argument, which is evaluated only when mappers are first used.   Using this, we
can define the ``association_table`` at a later point, as long as it's
available to the callable after all module initialization is complete::

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship("Child",
                        secondary=lambda: association_table,
                        backref="parents")

With the declarative extension in use, the traditional "string name of the table"
is accepted as well, matching the name of the table as stored in ``Base.metadata.tables``::

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship("Child",
                        secondary="association",
                        backref="parents")

.. warning:: When passed as a Python-evaluable string, the
    :paramref:`_orm.relationship.secondary` argument is interpreted using Python's
    ``eval()`` function. **DO NOT PASS UNTRUSTED INPUT TO THIS STRING**. See
    :ref:`declarative_relationship_eval` for details on declarative
    evaluation of :func:`_orm.relationship` arguments.


.. _relationships_many_to_many_deletion:

Deleting Rows from the Many to Many Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A behavior which is unique to the :paramref:`_orm.relationship.secondary`
argument to :func:`_orm.relationship` is that the :class:`_schema.Table` which
is specified here is automatically subject to INSERT and DELETE statements, as
objects are added or removed from the collection. There is **no need to delete
from this table manually**.   The act of removing a record from the collection
will have the effect of the row being deleted on flush::

    # row will be deleted from the "secondary" table
    # automatically
    myparent.children.remove(somechild)

A question which often arises is how the row in the "secondary" table can be deleted
when the child object is handed directly to :meth:`.Session.delete`::

    session.delete(somechild)

There are several possibilities here:

* If there is a :func:`_orm.relationship` from ``Parent`` to ``Child``, but there is
  **not** a reverse-relationship that links a particular ``Child`` to each ``Parent``,
  SQLAlchemy will not have any awareness that when deleting this particular
  ``Child`` object, it needs to maintain the "secondary" table that links it to
  the ``Parent``.  No delete of the "secondary" table will occur.
* If there is a relationship that links a particular ``Child`` to each ``Parent``,
  suppose it's called ``Child.parents``, SQLAlchemy by default will load in
  the ``Child.parents`` collection to locate all ``Parent`` objects, and remove
  each row from the "secondary" table which establishes this link.  Note that
  this relationship does not need to be bidirectional; SQLAlchemy is strictly
  looking at every :func:`_orm.relationship` associated with the ``Child`` object
  being deleted.
* A higher performing option here is to use ON DELETE CASCADE directives
  with the foreign keys used by the database.   Assuming the database supports
  this feature, the database itself can be made to automatically delete rows in the
  "secondary" table as referencing rows in "child" are deleted.   SQLAlchemy
  can be instructed to forego actively loading in the ``Child.parents``
  collection in this case using the :paramref:`_orm.relationship.passive_deletes`
  directive on :func:`_orm.relationship`; see :ref:`passive_deletes` for more details
  on this.

Note again, these behaviors are *only* relevant to the
:paramref:`_orm.relationship.secondary` option used with
:func:`_orm.relationship`.   If dealing with association tables that are mapped
explicitly and are *not* present in the :paramref:`_orm.relationship.secondary`
option of a relevant :func:`_orm.relationship`, cascade rules can be used
instead to automatically delete entities in reaction to a related entity being
deleted - see :ref:`unitofwork_cascades` for information on this feature.

.. seealso::

    :ref:`cascade_delete_many_to_many`

    :ref:`passive_deletes_many_to_many`


.. _association_pattern:

Association Object
~~~~~~~~~~~~~~~~~~

The association object pattern is a variant on many-to-many: it's used
when your association table contains additional columns beyond those
which are foreign keys to the left and right tables. Instead of using
the :paramref:`_orm.relationship.secondary` argument, you map a new class
directly to the association table. The left side of the relationship
references the association object via one-to-many, and the association
class references the right side via many-to-one.  Below we illustrate
an association table mapped to the ``Association`` class which
includes a column called ``extra_data``, which is a string value that
is stored along with each association between ``Parent`` and
``Child``::

    class Association(Base):
        __tablename__ = 'association'
        left_id = Column(ForeignKey('left.id'), primary_key=True)
        right_id = Column(ForeignKey('right.id'), primary_key=True)
        extra_data = Column(String(50))
        child = relationship("Child")

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship("Association")

    class Child(Base):
        __tablename__ = 'right'
        id = Column(Integer, primary_key=True)

As always, the bidirectional version makes use of :paramref:`_orm.relationship.back_populates`
or :paramref:`_orm.relationship.backref`::

    class Association(Base):
        __tablename__ = 'association'
        left_id = Column(ForeignKey('left.id'), primary_key=True)
        right_id = Column(ForeignKey('right.id'), primary_key=True)
        extra_data = Column(String(50))
        child = relationship("Child", back_populates="parents")
        parent = relationship("Parent", back_populates="children")

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship("Association", back_populates="parent")

    class Child(Base):
        __tablename__ = 'right'
        id = Column(Integer, primary_key=True)
        parents = relationship("Association", back_populates="child")

Working with the association pattern in its direct form requires that child
objects are associated with an association instance before being appended to
the parent; similarly, access from parent to child goes through the
association object::

    # create parent, append a child via association
    p = Parent()
    a = Association(extra_data="some data")
    a.child = Child()
    p.children.append(a)

    # iterate through child objects via association, including association
    # attributes
    for assoc in p.children:
        print(assoc.extra_data)
        print(assoc.child)

To enhance the association object pattern such that direct
access to the ``Association`` object is optional, SQLAlchemy
provides the :ref:`associationproxy_toplevel` extension. This
extension allows the configuration of attributes which will
access two "hops" with a single access, one "hop" to the
associated object, and a second to a target attribute.

.. warning::

  The association object pattern **does not coordinate changes with a
  separate relationship that maps the association table as "secondary"**.

  Below, changes made to ``Parent.children`` will not be coordinated
  with changes made to ``Parent.child_associations`` or
  ``Child.parent_associations`` in Python; while all of these relationships will continue
  to function normally by themselves, changes on one will not show up in another
  until the :class:`.Session` is expired, which normally occurs automatically
  after :meth:`.Session.commit`::

        class Association(Base):
            __tablename__ = 'association'

            left_id = Column(ForeignKey('left.id'), primary_key=True)
            right_id = Column(ForeignKey('right.id'), primary_key=True)
            extra_data = Column(String(50))

            child = relationship("Child", backref="parent_associations")
            parent = relationship("Parent", backref="child_associations")

        class Parent(Base):
            __tablename__ = 'left'
            id = Column(Integer, primary_key=True)

            children = relationship("Child", secondary="association")

        class Child(Base):
            __tablename__ = 'right'
            id = Column(Integer, primary_key=True)

  Additionally, just as changes to one relationship aren't reflected in the
  others automatically, writing the same data to both relationships will cause
  conflicting INSERT or DELETE statements as well, such as below where we
  establish the same relationship between a ``Parent`` and ``Child`` object
  twice::

        p1 = Parent()
        c1 = Child()
        p1.children.append(c1)

        # redundant, will cause a duplicate INSERT on Association
        p1.child_associations.append(Association(child=c1))

  It's fine to use a mapping like the above if you know what
  you're doing, though it may be a good idea to apply the ``viewonly=True`` parameter
  to the "secondary" relationship to avoid the issue of redundant changes
  being logged.  However, to get a foolproof pattern that allows a simple
  two-object ``Parent->Child`` relationship while still using the association
  object pattern, use the association proxy extension
  as documented at :ref:`associationproxy_toplevel`.

.. _orm_declarative_relationship_eval:

Late-Evaluation of Relationship Arguments
-----------------------------------------

Many of the examples in the preceding sections illustrate mappings
where the various :func:`_orm.relationship` constructs refer to their target
classes using a string name, rather than the class itself::

    class Parent(Base):
        # ...

        children = relationship("Child", back_populates="parent")

    class Child(Base):
        # ...

        parent = relationship("Parent", back_populates="children")

These string names are resolved into classes in the mapper resolution stage,
which is an internal process that occurs typically after all mappings have
been defined and is normally triggered by the first usage of the mappings
themselves.     The :class:`_orm.registry` object is the container in which
these names are stored and resolved to the mapped classes they refer towards.

In addition to the main class argument for :func:`_orm.relationship`,
other arguments which depend upon the columns present on an as-yet
undefined class may also be specified either as Python functions, or more
commonly as strings.   For most of these
arguments except that of the main argument, string inputs are
**evaluated as Python expressions using Python's built-in eval() function**,
as they are intended to receive complete SQL expressions.

.. warning:: As the Python ``eval()`` function is used to interpret the
   late-evaluated string arguments passed to :func:`_orm.relationship` mapper
   configuration construct, these arguments should **not** be repurposed
   such that they would receive untrusted user input; ``eval()`` is
   **not secure** against untrusted user input.

The full namespace available within this evaluation includes all classes mapped
for this declarative base, as well as the contents of the ``sqlalchemy``
package, including expression functions like :func:`_sql.desc` and
:attr:`_functions.func`::

    class Parent(Base):
        # ...

        children = relationship(
            "Child",
            order_by="desc(Child.email_address)",
            primaryjoin="Parent.id == Child.parent_id"
        )

For the case where more than one module contains a class of the same name,
string class names can also be specified as module-qualified paths
within any of these string expressions::

    class Parent(Base):
        # ...

        children = relationship(
            "myapp.mymodel.Child",
            order_by="desc(myapp.mymodel.Child.email_address)",
            primaryjoin="myapp.mymodel.Parent.id == myapp.mymodel.Child.parent_id"
        )

The qualified path can be any partial path that removes ambiguity between
the names.  For example, to disambiguate between
``myapp.model1.Child`` and ``myapp.model2.Child``,
we can specify ``model1.Child`` or ``model2.Child``::

    class Parent(Base):
        # ...

        children = relationship(
            "model1.Child",
            order_by="desc(mymodel1.Child.email_address)",
            primaryjoin="Parent.id == model1.Child.parent_id"
        )

The :func:`_orm.relationship` construct also accepts Python functions or
lambdas as input for these arguments.   This has the advantage of providing
more compile-time safety and better support for IDEs and :pep:`484` scenarios.

A Python functional approach might look like the following::

    from sqlalchemy import desc

    def _resolve_child_model():
         from myapplication import Child
         return Child

    class Parent(Base):
        # ...

        children = relationship(
            _resolve_child_model(),
            order_by=lambda: desc(_resolve_child_model().email_address),
            primaryjoin=lambda: Parent.id == _resolve_child_model().parent_id
        )

The full list of parameters which accept Python functions/lambdas or strings
that will be passed to ``eval()`` are:

* :paramref:`_orm.relationship.order_by`

* :paramref:`_orm.relationship.primaryjoin`

* :paramref:`_orm.relationship.secondaryjoin`

* :paramref:`_orm.relationship.secondary`

* :paramref:`_orm.relationship.remote_side`

* :paramref:`_orm.relationship.foreign_keys`

* :paramref:`_orm.relationship._user_defined_foreign_keys`

.. versionchanged:: 1.3.16

    Prior to SQLAlchemy 1.3.16, the main :paramref:`_orm.relationship.argument`
    to :func:`_orm.relationship` was also evaluated through ``eval()``   As of
    1.3.16 the string name is resolved from the class resolver directly without
    supporting custom Python expressions.

.. warning::

    As stated previously, the above parameters to :func:`_orm.relationship`
    are **evaluated as Python code expressions using eval().  DO NOT PASS
    UNTRUSTED INPUT TO THESE ARGUMENTS.**

It should also be noted that in a similar way as described at
:ref:`orm_declarative_table_adding_columns`, any :class:`_orm.MapperProperty`
construct can be added to a declarative base mapping at any time.  If
we wanted to implement this :func:`_orm.relationship` after the ``Address``
class were available, we could also apply it afterwards::

    # first, module A, where Child has not been created yet,
    # we create a Parent class which knows nothing about Child

    class Parent(Base):
        # ...


    #... later, in Module B, which is imported after module A:

    class Child(Base):
        # ...

    from module_a import Parent

    # assign the User.addresses relationship as a class variable.  The
    # declarative base class will intercept this and map the relationship.
    Parent.children = relationship(
        Child,
        primaryjoin=Child.parent_id==Parent.id
    )

.. note:: assignment of mapped properties to a declaratively mapped class will only
    function correctly if the "declarative base" class is used, which also
    provides for a metaclass-driven ``__setattr__()`` method which will
    intercept these operations. It will **not** work if the declarative
    decorator provided by :meth:`_orm.registry.mapped` is used, nor will it
    work for an imperatively mapped class mapped by
    :meth:`_orm.registry.map_imperatively`.


.. _orm_declarative_relationship_secondary_eval:

Late-Evaluation for a many-to-many relationship
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Many-to-many relationships include a reference to an additional, typically non-mapped
:class:`_schema.Table` object that is typically present in the :class:`_schema.MetaData`
collection referred towards by the :class:`_orm.registry`.   The late-evaluation
system also includes support for having this attribute be specified as a
string argument which will be resolved from this :class:`_schema.MetaData`
collection.  Below we specify an association table ``keyword_author``,
sharing the :class:`_schema.MetaData` collection associated with our
declarative base and its :class:`_orm.registry`.  We can then refer to this
:class:`_schema.Table` by name in the :paramref:`_orm.relationship.secondary`
parameter::

    keyword_author = Table(
        'keyword_author', Base.metadata,
        Column('author_id', Integer, ForeignKey('authors.id')),
        Column('keyword_id', Integer, ForeignKey('keywords.id'))
        )

    class Author(Base):
        __tablename__ = 'authors'
        id = Column(Integer, primary_key=True)
        keywords = relationship("Keyword", secondary="keyword_author")

For additional detail on many-to-many relationships see the section
:ref:`relationships_many_to_many`.
