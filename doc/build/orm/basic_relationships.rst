.. _relationship_patterns:

Basic Relationship Patterns
---------------------------

A quick walkthrough of the basic relational patterns, which in this section are illustrated
using :ref:`Declarative <orm_explicit_declarative_base>` style mappings
based on the use of the :class:`_orm.Mapped` annotation type.

The setup for each of the following sections is as follows::

    from __future__ import annotations
    from typing import List

    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass

Declarative vs. Imperative Forms
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As SQLAlchemy has evolved, different ORM configurational styles have emerged.
For examples in this section and others that use annotated
:ref:`Declarative <orm_explicit_declarative_base>` mappings with
:class:`_orm.Mapped`, the corresponding non-annotated form should use the
desired class, or string class name, as the first argument passed to
:func:`_orm.relationship`.  The example below illustrates the form used in
this document, which is a fully Declarative example using :pep:`484` annotations,
where the :func:`_orm.relationship` construct is also deriving the target
class and collection type from the :class:`_orm.Mapped` annotation,
which is the most modern form of SQLAlchemy Declarative mapping::

    class Parent(Base):
        __tablename__ = "parent_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[List["Child"]] = relationship(back_populates="parent")


    class Child(Base):
        __tablename__ = "child_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        parent_id: Mapped[int] = mapped_column(ForeignKey("parent_table.id"))
        parent: Mapped["Parent"] = relationship(back_populates="children")

In contrast, using a Declarative mapping **without** annotations is
the more "classic" form of mapping, where :func:`_orm.relationship`
requires all parameters passed to it directly, as in the example below::

    class Parent(Base):
        __tablename__ = "parent_table"

        id = mapped_column(Integer, primary_key=True)
        children = relationship("Child", back_populates="parent")


    class Child(Base):
        __tablename__ = "child_table"

        id = mapped_column(Integer, primary_key=True)
        parent_id = mapped_column(ForeignKey("parent_table.id"))
        parent = relationship("Parent", back_populates="children")

Finally, using :ref:`Imperative Mapping <orm_imperative_mapping>`, which
is SQLAlchemy's original mapping form before Declarative was made (which
nonetheless remains preferred by a vocal minority of users), the above
configuration looks like::

    registry.map_imperatively(
        Parent,
        parent_table,
        properties={"children": relationship("Child", back_populates="parent")},
    )

    registry.map_imperatively(
        Child,
        child_table,
        properties={"parent": relationship("Parent", back_populates="children")},
    )

Additionally, the default collection style for non-annotated mappings is
``list``.  To use a ``set`` or other collection without annotations, indicate
it using the :paramref:`_orm.relationship.collection_class` parameter::

    class Parent(Base):
        __tablename__ = "parent_table"

        id = mapped_column(Integer, primary_key=True)
        children = relationship("Child", collection_class=set, ...)

Detail on collection configuration for :func:`_orm.relationship` is at
:ref:`custom_collections`.

Additional differences between annotated and non-annotated / imperative
styles will be noted as needed.

.. _relationship_patterns_o2m:

One To Many
~~~~~~~~~~~

A one to many relationship places a foreign key on the child table referencing
the parent.  :func:`_orm.relationship` is then specified on the parent, as referencing
a collection of items represented by the child::

    class Parent(Base):
        __tablename__ = "parent_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[List["Child"]] = relationship()


    class Child(Base):
        __tablename__ = "child_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        parent_id: Mapped[int] = mapped_column(ForeignKey("parent_table.id"))

To establish a bidirectional relationship in one-to-many, where the "reverse"
side is a many to one, specify an additional :func:`_orm.relationship` and connect
the two using the :paramref:`_orm.relationship.back_populates` parameter,
using the attribute name of each :func:`_orm.relationship`
as the value for :paramref:`_orm.relationship.back_populates` on the other::


    class Parent(Base):
        __tablename__ = "parent_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[List["Child"]] = relationship(back_populates="parent")


    class Child(Base):
        __tablename__ = "child_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        parent_id: Mapped[int] = mapped_column(ForeignKey("parent_table.id"))
        parent: Mapped["Parent"] = relationship(back_populates="children")

``Child`` will get a ``parent`` attribute with many-to-one semantics.

.. _relationship_patterns_o2m_collection:

Using Sets, Lists, or other Collection Types for One To Many
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Using annotated Declarative mappings, the type of collection used for the
:func:`_orm.relationship` is derived from the collection type passed to the
:class:`_orm.Mapped` container type.  The example from the previous section
may be written to use a ``set`` rather than a ``list`` for the
``Parent.children`` collection using ``Mapped[Set["Child"]]``::

    class Parent(Base):
        __tablename__ = "parent_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[Set["Child"]] = relationship(back_populates="parent")

When using non-annotated forms including imperative mappings, the Python
class to use as a collection may be passed using the
:paramref:`_orm.relationship.collection_class` parameter.

.. seealso::

    :ref:`custom_collections` - contains further detail on collection
    configuration including some techniques to map :func:`_orm.relationship`
    to dictionaries.


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
        __tablename__ = "parent_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        child_id: Mapped[int] = mapped_column(ForeignKey("child_table.id"))
        child: Mapped["Child"] = relationship()


    class Child(Base):
        __tablename__ = "child_table"

        id: Mapped[int] = mapped_column(primary_key=True)

The above example shows a many-to-one relationship that assumes non-nullable
behavior; the next section, :ref:`relationship_patterns_nullable_m2o`,
illustrates a nullable version.

Bidirectional behavior is achieved by adding a second :func:`_orm.relationship`
and applying the :paramref:`_orm.relationship.back_populates` parameter
in both directions, using the attribute name of each :func:`_orm.relationship`
as the value for :paramref:`_orm.relationship.back_populates` on the other::

    class Parent(Base):
        __tablename__ = "parent_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        child_id: Mapped[int] = mapped_column(ForeignKey("child_table.id"))
        child: Mapped["Child"] = relationship(back_populates="parents")


    class Child(Base):
        __tablename__ = "child_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        parents: Mapped[List["Parent"]] = relationship(back_populates="child")

.. _relationship_patterns_nullable_m2o:

Nullable Many-to-One
^^^^^^^^^^^^^^^^^^^^

In the preceding example, the ``Parent.child`` relationship is not typed as
allowing ``None``; this follows from the ``Parent.child_id`` column itself
not being nullable, as it is typed with ``Mapped[int]``.    If we wanted
``Parent.child`` to be a **nullable** many-to-one, we can set both
``Parent.child_id`` and ``Parent.child`` to be ``Optional[]``, in which
case the configuration would look like::

    from typing import Optional


    class Parent(Base):
        __tablename__ = "parent_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        child_id: Mapped[Optional[int]] = mapped_column(ForeignKey("child_table.id"))
        child: Mapped[Optional["Child"]] = relationship(back_populates="parents")


    class Child(Base):
        __tablename__ = "child_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        parents: Mapped[List["Parent"]] = relationship(back_populates="child")

Above, the column for ``Parent.child_id`` will be created in DDL to allow
``NULL`` values. When using :func:`_orm.mapped_column` with explicit typing
declarations, the specification of ``child_id: Mapped[Optional[int]]`` is
equivalent to setting :paramref:`_schema.Column.nullable` to ``True`` on the
:class:`_schema.Column`, whereas ``child_id: Mapped[int]`` is equivalent to
setting it to ``False``. See :ref:`orm_declarative_mapped_column_nullability`
for background on this behavior.

.. tip::

  If using Python 3.10 or greater, :pep:`604` syntax is more convenient
  to indicate optional types using ``| None``, which when combined with
  :pep:`563` postponed annotation evaluation so that string-quoted types aren't
  required, would look like::

      from __future__ import annotations


      class Parent(Base):
          __tablename__ = "parent_table"

          id: Mapped[int] = mapped_column(primary_key=True)
          child_id: Mapped[int | None] = mapped_column(ForeignKey("child_table.id"))
          child: Mapped[Child | None] = relationship(back_populates="parents")


      class Child(Base):
          __tablename__ = "child_table"

          id: Mapped[int] = mapped_column(primary_key=True)
          parents: Mapped[List[Parent]] = relationship(back_populates="child")

.. _relationships_one_to_one:

One To One
~~~~~~~~~~

One To One is essentially a :ref:`relationship_patterns_o2m`
relationship from a foreign key perspective, but indicates that there will
only be one row at any time that refers to a particular parent row.

When using annotated mappings with :class:`_orm.Mapped`, the "one-to-one"
convention is achieved by applying a non-collection type to the
:class:`_orm.Mapped` annotation on both sides of the relationship, which will
imply to the ORM that a collection should not be used on either side, as in the
example below::

    class Parent(Base):
        __tablename__ = "parent_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        child: Mapped["Child"] = relationship(back_populates="parent")


    class Child(Base):
        __tablename__ = "child_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        parent_id: Mapped[int] = mapped_column(ForeignKey("parent_table.id"))
        parent: Mapped["Parent"] = relationship(back_populates="child")

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
  "many-to-one" relationship.  By itself, it will not detect assignment
  of more than one ``Child``, unless the :paramref:`_orm.relationship.single_parent`
  parameter is set, which may be useful::

    class Child(Base):
        __tablename__ = "child_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        parent_id: Mapped[int] = mapped_column(ForeignKey("parent_table.id"))
        parent: Mapped["Parent"] = relationship(back_populates="child", single_parent=True)

  Outside of setting this parameter, the "one-to-many" side (which here is
  one-to-one by convention) will also not reliably detect if more than one
  ``Child`` is associated with a single ``Parent``, such as in the case where
  the multiple ``Child`` objects are pending and not database-persistent.

  Whether or not :paramref:`_orm.relationship.single_parent` is used, it is
  recommended that the database schema include a :ref:`unique constraint
  <schema_unique_constraint>` to indicate that the ``Child.parent_id`` column
  should be unique, to ensure at the database level that only one ``Child`` row may refer
  to a particular ``Parent`` row at a time (see :ref:`orm_declarative_table_configuration`
  for background on the ``__table_args__`` tuple syntax)::

    from sqlalchemy import UniqueConstraint


    class Child(Base):
        __tablename__ = "child_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        parent_id: Mapped[int] = mapped_column(ForeignKey("parent_table.id"))
        parent: Mapped["Parent"] = relationship(back_populates="child")

        __table_args__ = (UniqueConstraint("parent_id"),)

.. versionadded:: 2.0  The :func:`_orm.relationship` construct can derive
   the effective value of the :paramref:`_orm.relationship.uselist`
   parameter from a given :class:`_orm.Mapped` annotation.

Setting uselist=False for non-annotated configurations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using :func:`_orm.relationship` without the benefit of :class:`_orm.Mapped`
annotations, the one-to-one pattern can be enabled using the
:paramref:`_orm.relationship.uselist` parameter set to ``False`` on what
would normally be the "many" side, illustrated in a non-annotated
Declarative configuration below::


    class Parent(Base):
        __tablename__ = "parent_table"

        id = mapped_column(Integer, primary_key=True)
        child = relationship("Child", uselist=False, back_populates="parent")


    class Child(Base):
        __tablename__ = "child_table"

        id = mapped_column(Integer, primary_key=True)
        parent_id = mapped_column(ForeignKey("parent_table.id"))
        parent = relationship("Parent", back_populates="child")

.. _relationships_many_to_many:

Many To Many
~~~~~~~~~~~~

Many to Many adds an association table between two classes. The association
table is nearly always given as a Core :class:`_schema.Table` object or
other Core selectable such as a :class:`_sql.Join` object, and is
indicated by the :paramref:`_orm.relationship.secondary` argument to
:func:`_orm.relationship`. Usually, the :class:`_schema.Table` uses the
:class:`_schema.MetaData` object associated with the declarative base class, so
that the :class:`_schema.ForeignKey` directives can locate the remote tables
with which to link::

    from __future__ import annotations

    from sqlalchemy import Column
    from sqlalchemy import Table
    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    # note for a Core table, we use the sqlalchemy.Column construct,
    # not sqlalchemy.orm.mapped_column
    association_table = Table(
        "association_table",
        Base.metadata,
        Column("left_id", ForeignKey("left_table.id")),
        Column("right_id", ForeignKey("right_table.id")),
    )


    class Parent(Base):
        __tablename__ = "left_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[List[Child]] = relationship(secondary=association_table)


    class Child(Base):
        __tablename__ = "right_table"

        id: Mapped[int] = mapped_column(primary_key=True)

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

        association_table = Table(
            "association_table",
            Base.metadata,
            Column("left_id", ForeignKey("left_table.id"), primary_key=True),
            Column("right_id", ForeignKey("right_table.id"), primary_key=True),
        )

Setting Bi-Directional Many-to-many
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For a bidirectional relationship, both sides of the relationship contain a
collection.  Specify using :paramref:`_orm.relationship.back_populates`, and
for each :func:`_orm.relationship` specify the common association table::

    from __future__ import annotations

    from sqlalchemy import Column
    from sqlalchemy import Table
    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    association_table = Table(
        "association_table",
        Base.metadata,
        Column("left_id", ForeignKey("left_table.id"), primary_key=True),
        Column("right_id", ForeignKey("right_table.id"), primary_key=True),
    )


    class Parent(Base):
        __tablename__ = "left_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[List[Child]] = relationship(
            secondary=association_table, back_populates="parents"
        )


    class Child(Base):
        __tablename__ = "right_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        parents: Mapped[List[Parent]] = relationship(
            secondary=association_table, back_populates="children"
        )

Using a late-evaluated form for the "secondary" argument
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :paramref:`_orm.relationship.secondary` parameter of
:func:`_orm.relationship` also accepts two different "late evaluated" forms,
including string table name as well as lambda callable.   See the section
:ref:`orm_declarative_relationship_secondary_eval` for background and
examples.


Using Sets, Lists, or other Collection Types for Many To Many
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Configuration of collections for a Many to Many relationship is identical
to that of :ref:`relationship_patterns_o2m`, as described at
:ref:`relationship_patterns_o2m_collection`.    For an annotated mapping
using :class:`_orm.Mapped`, the collection can be indicated by the
type of collection used within the :class:`_orm.Mapped` generic class,
such as ``set``::

    class Parent(Base):
        __tablename__ = "left_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[Set["Child"]] = relationship(secondary=association_table)

When using non-annotated forms including imperative mappings, as is
the case with one-to-many, the Python
class to use as a collection may be passed using the
:paramref:`_orm.relationship.collection_class` parameter.

.. seealso::

    :ref:`custom_collections` - contains further detail on collection
    configuration including some techniques to map :func:`_orm.relationship`
    to dictionaries.

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

The association object pattern is a variant on many-to-many: it's used when an
association table contains additional columns beyond those which are foreign
keys to the parent and child (or left and right) tables, columns which are most
ideally mapped to their own ORM mapped class. This mapped class is mapped
against the :class:`.Table` that would otherwise be noted as
:paramref:`_orm.relationship.secondary` when using the many-to-many pattern.

In the association object pattern, the :paramref:`_orm.relationship.secondary`
parameter is not used; instead, a class is mapped directly to the association
table. Two individual :func:`_orm.relationship` constructs then link first the
parent side to the mapped association class via one to many, and then the
mapped association class to the child side via many-to-one, to form a
uni-directional association object relationship from parent, to association, to
child. For a bi-directional relationship, four :func:`_orm.relationship`
constructs are used to link the mapped association class to both parent and
child in both directions.

The example below illustrates a new class ``Association`` which maps
to the :class:`.Table` named ``association``; this table now includes
an additional column called ``extra_data``, which is a string value that
is stored along with each association between ``Parent`` and
``Child``.   By mapping the table to an explicit class, rudimental access
from ``Parent`` to ``Child`` makes explicit use of ``Association``::

    from typing import Optional

    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    class Association(Base):
        __tablename__ = "association_table"
        left_id: Mapped[int] = mapped_column(ForeignKey("left_table.id"), primary_key=True)
        right_id: Mapped[int] = mapped_column(
            ForeignKey("right_table.id"), primary_key=True
        )
        extra_data: Mapped[Optional[str]]
        child: Mapped["Child"] = relationship()


    class Parent(Base):
        __tablename__ = "left_table"
        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[List["Association"]] = relationship()


    class Child(Base):
        __tablename__ = "right_table"
        id: Mapped[int] = mapped_column(primary_key=True)

To illustrate the bi-directional version, we add two more :func:`_orm.relationship`
constructs, linked to the existing ones using :paramref:`_orm.relationship.back_populates`::

    from typing import Optional

    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    class Association(Base):
        __tablename__ = "association_table"
        left_id: Mapped[int] = mapped_column(ForeignKey("left_table.id"), primary_key=True)
        right_id: Mapped[int] = mapped_column(
            ForeignKey("right_table.id"), primary_key=True
        )
        extra_data: Mapped[Optional[str]]
        child: Mapped["Child"] = relationship(back_populates="parents")
        parent: Mapped["Parent"] = relationship(back_populates="children")


    class Parent(Base):
        __tablename__ = "left_table"
        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[List["Association"]] = relationship(back_populates="parent")


    class Child(Base):
        __tablename__ = "right_table"
        id: Mapped[int] = mapped_column(primary_key=True)
        parents: Mapped[List["Association"]] = relationship(back_populates="child")

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

.. seealso::

    :ref:`associationproxy_toplevel` - allows direct "many to many" style
    access between parent and child for a three-class association object mapping.

.. warning::

  Avoid mixing the association object pattern with the :ref:`many-to-many <relationships_many_to_many>`
  pattern directly, as this produces conditions where data may be read
  and written in an inconsistent fashion without special steps;
  the :ref:`association proxy <associationproxy_toplevel>` is typically
  used to provide more succinct access.  For more detailed background
  on the caveats introduced by this combination, see the next section
  :ref:`association_pattern_w_m2m`.

.. _association_pattern_w_m2m:

Combining Association Object with Many-to-Many Access Patterns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As mentioned in the previous section, the association object pattern does not
automatically integrate with usage of the many-to-many pattern against the same
tables/columns at the same time.  From this it follows that read operations
may return conflicting data and write operations may also attempt to flush
conflicting changes, causing either integrity errors or unexpected
inserts or deletes.

To illustrate, the example below configures a bidirectional many-to-many relationship
between ``Parent`` and ``Child`` via ``Parent.children`` and ``Child.parents``.
At the same time, an association object relationship is also configured,
between ``Parent.child_associations -> Association.child``
and ``Child.parent_associations -> Association.parent``::

    from typing import Optional

    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    class Association(Base):
        __tablename__ = "association_table"

        left_id: Mapped[int] = mapped_column(ForeignKey("left_table.id"), primary_key=True)
        right_id: Mapped[int] = mapped_column(
            ForeignKey("right_table.id"), primary_key=True
        )
        extra_data: Mapped[Optional[str]]

        # association between Assocation -> Child
        child: Mapped["Child"] = relationship(back_populates="parent_associations")

        # association between Assocation -> Parent
        parent: Mapped["Parent"] = relationship(back_populates="child_associations")


    class Parent(Base):
        __tablename__ = "left_table"

        id: Mapped[int] = mapped_column(primary_key=True)

        # many-to-many relationship to Child, bypassing the `Association` class
        children: Mapped[List["Child"]] = relationship(
            secondary="association_table", back_populates="parents"
        )

        # association between Parent -> Association -> Child
        child_associations: Mapped[List["Association"]] = relationship(
            back_populates="parent"
        )


    class Child(Base):
        __tablename__ = "right_table"

        id: Mapped[int] = mapped_column(primary_key=True)

        # many-to-many relationship to Parent, bypassing the `Association` class
        parents: Mapped[List["Parent"]] = relationship(
            secondary="association_table", back_populates="children"
        )

        # association between Child -> Association -> Parent
        parent_associations: Mapped[List["Association"]] = relationship(
            back_populates="child"
        )

When using this ORM model to make changes, changes made to
``Parent.children`` will not be coordinated with changes made to
``Parent.child_associations`` or ``Child.parent_associations`` in Python;
while all of these relationships will continue to function normally by
themselves, changes on one will not show up in another until the
:class:`.Session` is expired, which normally occurs automatically after
:meth:`.Session.commit`.

Additionally, if conflicting changes are made,
such as adding a new ``Association`` object while also appending the same
related ``Child`` to ``Parent.children``, this will raise integrity
errors when the unit of work flush process proceeds, as in the
example below::

      p1 = Parent()
      c1 = Child()
      p1.children.append(c1)

      # redundant, will cause a duplicate INSERT on Association
      p1.child_associations.append(Association(child=c1))

Appending ``Child`` to ``Parent.children`` directly also implies the
creation of rows in the ``association`` table without indicating any
value for the ``association.extra_data`` column, which will receive
``NULL`` for its value.

It's fine to use a mapping like the above if you know what you're doing; there
may be good reason to use many-to-many relationships in the case where use
of the "association object" pattern is infrequent, which is that it's easier to
load relationships along a single many-to-many relationship, which can also
optimize slightly better how the "secondary" table is used in SQL statements,
compared to how two separate relationships to an explicit association class is
used.   It's at least a good idea to apply the
:paramref:`_orm.relationship.viewonly` parameter
to the "secondary" relationship to avoid the issue of conflicting
changes occurring, as well as preventing ``NULL`` being written to the
additional association columns, as below::

    class Parent(Base):
        __tablename__ = "left_table"

        id: Mapped[int] = mapped_column(primary_key=True)

        # many-to-many relationship to Child, bypassing the `Association` class
        children: Mapped[List["Child"]] = relationship(
            secondary="association_table", back_populates="parents", viewonly=True
        )

        # association between Parent -> Association -> Child
        child_associations: Mapped[List["Association"]] = relationship(
            back_populates="parent"
        )


    class Child(Base):
        __tablename__ = "right_table"

        id: Mapped[int] = mapped_column(primary_key=True)

        # many-to-many relationship to Parent, bypassing the `Association` class
        parents: Mapped[List["Parent"]] = relationship(
            secondary="association_table", back_populates="children", viewonly=True
        )

        # association between Child -> Association -> Parent
        parent_associations: Mapped[List["Association"]] = relationship(
            back_populates="child"
        )

The above mapping will not write any changes to ``Parent.children`` or
``Child.parents`` to the database, preventing conflicting writes.  However, reads
of ``Parent.children`` or ``Child.parents`` will not necessarily match the data
that's read from ``Parent.child_associations`` or ``Child.parent_associations``,
if changes are being made to these collections within the same transaction
or :class:`.Session` as where the viewonly collections are being read.  If
use of the association object relationships is infrequent and is carefully
organized against code that accesses the many-to-many collections to avoid
stale reads (in extreme cases, making direct use of :meth:`_orm.Session.expire`
to cause collections to be refreshed within the current transaction), the pattern may be feasible.

A popular alternative to the above pattern is one where the direct many-to-many
``Parent.children`` and ``Child.parents`` relationships are replaced with
an extension that will transparently proxy through the ``Association``
class, while keeping everything consistent from the ORM's point of
view.  This extension is known as the :ref:`Association Proxy <associationproxy_toplevel>`.

.. seealso::

    :ref:`associationproxy_toplevel` - allows direct "many to many" style
    access between parent and child for a three-class association object mapping.

.. _orm_declarative_relationship_eval:

Late-Evaluation of Relationship Arguments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Most of the examples in the preceding sections illustrate mappings
where the various :func:`_orm.relationship` constructs refer to their target
classes using a string name, rather than the class itself, such as when
using :class:`_orm.Mapped`, a forward reference is generated that exists
at runtime only as a string::

    class Parent(Base):
        # ...

        children: Mapped[List["Child"]] = relationship(back_populates="parent")


    class Child(Base):
        # ...

        parent: Mapped["Parent"] = relationship(back_populates="children")

Similarly, when using non-annotated forms such as non-annotated Declarative
or Imperative mappings, a string name is also supported directly by
the :func:`_orm.relationship` construct::

    registry.map_imperatively(
        Parent,
        parent_table,
        properties={"children": relationship("Child", back_populates="parent")},
    )

    registry.map_imperatively(
        Child,
        child_table,
        properties={"parent": relationship("Parent", back_populates="children")},
    )

These string names are resolved into classes in the mapper resolution stage,
which is an internal process that occurs typically after all mappings have been
defined and is normally triggered by the first usage of the mappings
themselves.  The :class:`_orm.registry` object is the container where these
names are stored and resolved to the mapped classes to which they refer.

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

        children: Mapped[List["Child"]] = relationship(
            order_by="desc(Child.email_address)",
            primaryjoin="Parent.id == Child.parent_id",
        )

For the case where more than one module contains a class of the same name,
string class names can also be specified as module-qualified paths
within any of these string expressions::

    class Parent(Base):
        # ...

        children: Mapped[List["myapp.mymodel.Child"]] = relationship(
            order_by="desc(myapp.mymodel.Child.email_address)",
            primaryjoin="myapp.mymodel.Parent.id == myapp.mymodel.Child.parent_id",
        )

In an example like the above, the string passed to :class:`_orm.Mapped`
can be disambiguated from a specific class argument by passing the class
location string directly to the first positional parameter (:paramref:`_orm.relationship.argument`) as well.
Below illustrates a typing-only import for ``Child``, combined with a
runtime specifier for the target class that will search for the correct
name within the :class:`_orm.registry`::

    import typing

    if typing.TYPE_CHECKING:
        from myapp.mymodel import Child


    class Parent(Base):
        # ...

        children: Mapped[List["Child"]] = relationship(
            "myapp.mymodel.Child",
            order_by="desc(myapp.mymodel.Child.email_address)",
            primaryjoin="myapp.mymodel.Parent.id == myapp.mymodel.Child.parent_id",
        )

The qualified path can be any partial path that removes ambiguity between
the names.  For example, to disambiguate between
``myapp.model1.Child`` and ``myapp.model2.Child``,
we can specify ``model1.Child`` or ``model2.Child``::

    class Parent(Base):
        # ...

        children: Mapped[List["Child"]] = relationship(
            "model1.Child",
            order_by="desc(mymodel1.Child.email_address)",
            primaryjoin="Parent.id == model1.Child.parent_id",
        )

The :func:`_orm.relationship` construct also accepts Python functions or
lambdas as input for these arguments.  A Python functional approach might look
like the following::

    import typing

    from sqlalchemy import desc

    if typing.TYPE_CHECKING:
        from myapplication import Child


    def _resolve_child_model():
        from myapplication import Child

        return Child


    class Parent(Base):
        # ...

        children: Mapped[List["Child"]] = relationship(
            _resolve_child_model,
            order_by=lambda: desc(_resolve_child_model().email_address),
            primaryjoin=lambda: Parent.id == _resolve_child_model().parent_id,
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

.. warning::

    As stated previously, the above parameters to :func:`_orm.relationship`
    are **evaluated as Python code expressions using eval().  DO NOT PASS
    UNTRUSTED INPUT TO THESE ARGUMENTS.**

.. _orm_declarative_table_adding_relationship:

Adding Relationships to Mapped Classes After Declaration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It should also be noted that in a similar way as described at
:ref:`orm_declarative_table_adding_columns`, any :class:`_orm.MapperProperty`
construct can be added to a declarative base mapping at any time
(noting that annotated forms are not supported in this context).  If
we wanted to implement this :func:`_orm.relationship` after the ``Address``
class were available, we could also apply it afterwards::

    # first, module A, where Child has not been created yet,
    # we create a Parent class which knows nothing about Child


    class Parent(Base): ...


    # ... later, in Module B, which is imported after module A:


    class Child(Base): ...


    from module_a import Parent

    # assign the User.addresses relationship as a class variable.  The
    # declarative base class will intercept this and map the relationship.
    Parent.children = relationship(Child, primaryjoin=Child.parent_id == Parent.id)

As is the case for ORM mapped columns, there's no capability for
the :class:`_orm.Mapped` annotation type to take part in this operation;
therefore, the related class must be specified directly within the
:func:`_orm.relationship` construct, either as the class itself, the string
name of the class, or a callable function that returns a reference to
the target class.

.. note:: As is the case for ORM mapped columns, assignment of mapped
    properties to an already mapped class will only
    function correctly if the "declarative base" class is used, meaning
    the user-defined subclass of :class:`_orm.DeclarativeBase` or the
    dynamically generated class returned by :func:`_orm.declarative_base`
    or :meth:`_orm.registry.generate_base`.   This "base" class includes
    a Python metaclass which implements a special ``__setattr__()`` method
    that intercepts these operations.

    Runtime assignment of class-mapped attributes to a mapped class will **not** work
    if the class is mapped using decorators like :meth:`_orm.registry.mapped`
    or imperative functions like :meth:`_orm.registry.map_imperatively`.


.. _orm_declarative_relationship_secondary_eval:

Using a late-evaluated form for the "secondary" argument of many-to-many
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Many-to-many relationships make use of the
:paramref:`_orm.relationship.secondary` parameter, which ordinarily
indicates a reference to a typically non-mapped :class:`_schema.Table`
object or other Core selectable object.  Late evaluation
using a lambda callable is typical.

For the example given at :ref:`relationships_many_to_many`, if we assumed
that the ``association_table`` :class:`.Table` object would be defined at a point later on in the
module than the mapped class itself, we may write the :func:`_orm.relationship`
using a lambda as::

    class Parent(Base):
        __tablename__ = "left_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[List["Child"]] = relationship(
            "Child", secondary=lambda: association_table
        )

As a shortcut for table names that are also **valid Python identifiers**, the
:paramref:`_orm.relationship.secondary` parameter may also be passed as a
string, where resolution works by evaluation of the string as a Python
expression, with simple identifier names linked to same-named
:class:`_schema.Table` objects that are present in the same
:class:`_schema.MetaData` collection referenced by the current
:class:`_orm.registry`.

In the example below, the expression
``"association_table"`` is evaluated as a variable
named "association_table" that is resolved against the table names within
the :class:`.MetaData` collection::

    class Parent(Base):
        __tablename__ = "left_table"

        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[List["Child"]] = relationship(secondary="association_table")

.. note:: When passed as a string, the name passed to
    :paramref:`_orm.relationship.secondary` **must be a valid Python identifier**
    starting with a letter and containing only alphanumeric characters or
    underscores.   Other characters such as dashes etc. will be interpreted
    as Python operators which will not resolve to the name given.  Please consider
    using lambda expressions rather than strings for improved clarity.

.. warning:: When passed as a string,
    :paramref:`_orm.relationship.secondary` argument is interpreted using Python's
    ``eval()`` function, even though it's typically the name of a table.
    **DO NOT PASS UNTRUSTED INPUT TO THIS STRING**.



