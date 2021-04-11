.. _unitofwork_cascades:

Cascades
========

Mappers support the concept of configurable :term:`cascade` behavior on
:func:`~sqlalchemy.orm.relationship` constructs.  This refers
to how operations performed on a "parent" object relative to a
particular :class:`.Session` should be propagated to items
referred to by that relationship (e.g. "child" objects), and is
affected by the :paramref:`_orm.relationship.cascade` option.

The default behavior of cascade is limited to cascades of the
so-called :ref:`cascade_save_update` and :ref:`cascade_merge` settings.
The typical "alternative" setting for cascade is to add
the :ref:`cascade_delete` and :ref:`cascade_delete_orphan` options;
these settings are appropriate for related objects which only exist as
long as they are attached to their parent, and are otherwise deleted.

Cascade behavior is configured using the
:paramref:`_orm.relationship.cascade` option on
:func:`~sqlalchemy.orm.relationship`::

    class Order(Base):
        __tablename__ = 'order'

        items = relationship("Item", cascade="all, delete-orphan")
        customer = relationship("User", cascade="save-update")

To set cascades on a backref, the same flag can be used with the
:func:`~.sqlalchemy.orm.backref` function, which ultimately feeds
its arguments back into :func:`~sqlalchemy.orm.relationship`::

    class Item(Base):
        __tablename__ = 'item'

        order = relationship("Order",
                        backref=backref("items", cascade="all, delete-orphan")
                    )

.. sidebar:: The Origins of Cascade

    SQLAlchemy's notion of cascading behavior on relationships,
    as well as the options to configure them, are primarily derived
    from the similar feature in the Hibernate ORM; Hibernate refers
    to "cascade" in a few places such as in
    `Example: Parent/Child <https://docs.jboss.org/hibernate/orm/3.3/reference/en-US/html/example-parentchild.html>`_.
    If cascades are confusing, we'll refer to their conclusion,
    stating "The sections we have just covered can be a bit confusing.
    However, in practice, it all works out nicely."

The default value of :paramref:`_orm.relationship.cascade` is ``save-update, merge``.
The typical alternative setting for this parameter is either
``all`` or more commonly ``all, delete-orphan``.  The ``all`` symbol
is a synonym for ``save-update, merge, refresh-expire, expunge, delete``,
and using it in conjunction with ``delete-orphan`` indicates that the child
object should follow along with its parent in all cases, and be deleted once
it is no longer associated with that parent.

.. warning:: The ``all`` cascade option implies the
   :ref:`cascade_refresh_expire`
   cascade setting which may not be desirable when using the
   :ref:`asyncio_toplevel` extension, as it will expire related objects
   more aggressively than is typically appropriate in an explicit IO context.
   See the notes at :ref:`asyncio_orm_avoid_lazyloads` for further background.

The list of available values which can be specified for
the :paramref:`_orm.relationship.cascade` parameter are described in the following subsections.

.. _cascade_save_update:

save-update
-----------

``save-update`` cascade indicates that when an object is placed into a
:class:`.Session` via :meth:`.Session.add`, all the objects associated
with it via this :func:`_orm.relationship` should also be added to that
same :class:`.Session`.  Suppose we have an object ``user1`` with two
related objects ``address1``, ``address2``::

    >>> user1 = User()
    >>> address1, address2 = Address(), Address()
    >>> user1.addresses = [address1, address2]

If we add ``user1`` to a :class:`.Session`, it will also add
``address1``, ``address2`` implicitly::

    >>> sess = Session()
    >>> sess.add(user1)
    >>> address1 in sess
    True

``save-update`` cascade also affects attribute operations for objects
that are already present in a :class:`.Session`.  If we add a third
object, ``address3`` to the ``user1.addresses`` collection, it
becomes part of the state of that :class:`.Session`::

    >>> address3 = Address()
    >>> user1.addresses.append(address3)
    >>> address3 in sess
    >>> True

A ``save-update`` cascade can exhibit surprising behavior when removing an item from
a collection or de-associating an object from a scalar attribute. In some cases, the
orphaned objects may still be pulled into the ex-parent's :class:`.Session`; this is
so that the flush process may handle that related object appropriately.
This case usually only arises if an object is removed from one :class:`.Session`
and added to another::

    >>> user1 = sess1.query(User).filter_by(id=1).first()
    >>> address1 = user1.addresses[0]
    >>> sess1.close()   # user1, address1 no longer associated with sess1
    >>> user1.addresses.remove(address1)  # address1 no longer associated with user1
    >>> sess2 = Session()
    >>> sess2.add(user1)   # ... but it still gets added to the new session,
    >>> address1 in sess2  # because it's still "pending" for flush
    True

The ``save-update`` cascade is on by default, and is typically taken
for granted; it simplifies code by allowing a single call to
:meth:`.Session.add` to register an entire structure of objects within
that :class:`.Session` at once.   While it can be disabled, there
is usually not a need to do so.

One case where ``save-update`` cascade does sometimes get in the way is in that
it takes place in both directions for bi-directional relationships, e.g.
backrefs, meaning that the association of a child object with a particular parent
can have the effect of the parent object being implicitly associated with that
child object's :class:`.Session`; this pattern, as well as how to modify its
behavior using the :paramref:`_orm.relationship.cascade_backrefs` flag,
is discussed in the section :ref:`backref_cascade`.

.. _cascade_delete:

delete
------

The ``delete`` cascade indicates that when a "parent" object
is marked for deletion, its related "child" objects should also be marked
for deletion.   If for example we have a relationship ``User.addresses``
with ``delete`` cascade configured::

    class User(Base):
        # ...

        addresses = relationship("Address", cascade="all, delete")

If using the above mapping, we have a ``User`` object and two
related ``Address`` objects::

    >>> user1 = sess.query(User).filter_by(id=1).first()
    >>> address1, address2 = user1.addresses

If we mark ``user1`` for deletion, after the flush operation proceeds,
``address1`` and ``address2`` will also be deleted:

.. sourcecode:: python+sql

    >>> sess.delete(user1)
    >>> sess.commit()
    {opensql}DELETE FROM address WHERE address.id = ?
    ((1,), (2,))
    DELETE FROM user WHERE user.id = ?
    (1,)
    COMMIT

Alternatively, if our ``User.addresses`` relationship does *not* have
``delete`` cascade, SQLAlchemy's default behavior is to instead de-associate
``address1`` and ``address2`` from ``user1`` by setting their foreign key
reference to ``NULL``.  Using a mapping as follows::

    class User(Base):
        # ...

        addresses = relationship("Address")

Upon deletion of a parent ``User`` object, the rows in ``address`` are not
deleted, but are instead de-associated:

.. sourcecode:: python+sql

    >>> sess.delete(user1)
    >>> sess.commit()
    {opensql}UPDATE address SET user_id=? WHERE address.id = ?
    (None, 1)
    UPDATE address SET user_id=? WHERE address.id = ?
    (None, 2)
    DELETE FROM user WHERE user.id = ?
    (1,)
    COMMIT

:ref:`cascade_delete` cascade on one-to-many relationships is often combined
with :ref:`cascade_delete_orphan` cascade, which will emit a DELETE for the
related row if the "child" object is deassociated from the parent.  The
combination of ``delete`` and ``delete-orphan`` cascade covers both
situations where SQLAlchemy has to decide between setting a foreign key
column to NULL versus deleting the row entirely.

The feature by default works completely independently of database-configured
``FOREIGN KEY`` constraints that may themselves configure ``CASCADE`` behavior.
In order to integrate more efficiently with this configuration, additional
directives described at :ref:`passive_deletes` should be used.

.. seealso::

    :ref:`passive_deletes`

    :ref:`cascade_delete_many_to_many`

    :ref:`cascade_delete_orphan`

.. _cascade_delete_many_to_many:

Using delete cascade with many-to-many relationships
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``cascade="all, delete"`` option works equally well with a many-to-many
relationship, one that uses :paramref:`_orm.relationship.secondary` to
indicate an association table.   When a parent object is deleted, and therefore
de-associated with its related objects, the unit of work process will normally
delete rows from the association table, but leave the related objects intact.
When combined with ``cascade="all, delete"``, additional ``DELETE`` statements
will take place for the child rows themselves.

The following example adapts that of :ref:`relationships_many_to_many` to
illustrate the ``cascade="all, delete"`` setting on **one** side of the
association::

    association_table = Table('association', Base.metadata,
        Column('left_id', Integer, ForeignKey('left.id')),
        Column('right_id', Integer, ForeignKey('right.id'))
    )

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship(
            "Child",
            secondary=association_table,
            back_populates="parents",
            cascade="all, delete"
        )

    class Child(Base):
        __tablename__ = 'right'
        id = Column(Integer, primary_key=True)
        parents = relationship(
            "Parent",
            secondary=association_table,
            back_populates="children",
        )

Above, when a ``Parent`` object is marked for deletion
using :meth:`_orm.Session.delete`, the flush process will as usual delete
the associated rows from the ``association`` table, however per cascade
rules it will also delete all related ``Child`` rows.


.. warning::

    If the above ``cascade="all, delete"`` setting were configured on **both**
    relationships, then the cascade action would continue cascading through all
    ``Parent`` and ``Child`` objects, loading each ``children`` and ``parents``
    collection encountered and deleting everything that's connected.   It is
    typically not desireable for "delete" cascade to be configured
    bidirectionally.

.. seealso::

  :ref:`relationships_many_to_many_deletion`

  :ref:`passive_deletes_many_to_many`

.. _passive_deletes:

Using foreign key ON DELETE cascade with ORM relationships
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The behavior of SQLAlchemy's "delete" cascade overlaps with the
``ON DELETE`` feature of a database ``FOREIGN KEY`` constraint.
SQLAlchemy allows configuration of these schema-level :term:`DDL` behaviors
using the :class:`_schema.ForeignKey` and :class:`_schema.ForeignKeyConstraint`
constructs; usage of these objects in conjunction with :class:`_schema.Table`
metadata is described at :ref:`on_update_on_delete`.

In order to use ``ON DELETE`` foreign key cascades in conjunction with
:func:`_orm.relationship`, it's important to note first and foremost that the
:paramref:`_orm.relationship.cascade` setting must still be configured to
match the desired "delete" or "set null" behavior (using ``delete`` cascade
or leaving it omitted), so that whether the ORM or the database
level constraints will handle the task of actually modifying the data in the
database, the ORM will still be able to appropriately track the state of
locally present objects that may be affected.

There is then an additional option on :func:`_orm.relationship` which
indicates the degree to which the ORM should try to run DELETE/UPDATE
operations on related rows itself, vs. how much it should rely upon expecting
the database-side FOREIGN KEY constraint cascade to handle the task; this is
the :paramref:`_orm.relationship.passive_deletes` parameter and it accepts
options ``False`` (the default), ``True`` and ``"all"``.

The most typical example is that where child rows are to be deleted when
parent rows are deleted, and that ``ON DELETE CASCADE`` is configured
on the relevant ``FOREIGN KEY`` constraint as well::


    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        children = relationship(
            "Child", back_populates="parent",
            cascade="all, delete",
            passive_deletes=True
        )

    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('parent.id', ondelete="CASCADE"))
        parent = relationship("Parent", back_populates="children")

The behavior of the above configuration when a parent row is deleted
is as follows:

1. The application calls ``session.delete(my_parent)``, where ``my_parent``
   is an instance of ``Parent``.

2. When the :class:`_orm.Session` next flushes changes to the database,
   all of the **currently loaded** items within the ``my_parent.children``
   collection are deleted by the ORM, meaning a ``DELETE`` statement is
   emitted for each record.

3. If the ``my_parent.children`` collection is **unloaded**, then no ``DELETE``
   statements are emitted.   If the :paramref:`_orm.relationship.passive_deletes`
   flag were **not** set on this :func:`_orm.relationship`, then a ``SELECT``
   statement for unloaded ``Child`` objects would have been emitted.

4. A ``DELETE`` statement is then emitted for the ``my_parent`` row itself.

5. The database-level ``ON DELETE CASCADE`` setting ensures that all rows in
   ``child`` which refer to the affected row in ``parent`` are also deleted.

6. The ``Parent`` instance referred to by ``my_parent``, as well as all
   instances of ``Child`` that were related to this object and were
   **loaded** (i.e. step 2 above took place), are de-associated from the
   :class:`._orm.Session`.

.. note::

    To use "ON DELETE CASCADE", the underlying database engine must
    support ``FOREIGN KEY`` constraints and they must be enforcing:

    * When using MySQL, an appropriate storage engine must be
      selected.  See :ref:`mysql_storage_engines` for details.

    * When using SQLite, foreign key support must be enabled explicitly.
      See :ref:`sqlite_foreign_keys` for details.

.. topic:: Notes on Passive Deletes

    It is important to note the differences between the ORM and the relational
    database's notion of "cascade" as well as how they integrate:

    * A database level ``ON DELETE`` cascade is configured effectively
      on the **many-to-one** side of the relationship; that is, we configure
      it relative to the ``FOREIGN KEY`` constraint that is the "many" side
      of a relationship.  At the ORM level, **this direction is reversed**.
      SQLAlchemy handles the deletion of "child" objects relative to a
      "parent" from the "parent" side, which means that ``delete`` and
      ``delete-orphan`` cascade are configured on the **one-to-many**
      side.

    * Database level foreign keys with no ``ON DELETE`` setting are often used
      to **prevent** a parent row from being removed, as it would necessarily
      leave an unhandled related row present.  If this behavior is desired in a
      one-to-many relationship, SQLAlchemy's default behavior of setting a
      foreign key to ``NULL`` can be caught in one of two ways:

        * The easiest and most common is just to set the foreign-key-holding
          column to ``NOT NULL`` at the database schema level.  An attempt by
          SQLAlchemy to set the column to NULL will fail with a simple NOT NULL
          constraint exception.

        * The other, more special case way is to set the
          :paramref:`_orm.relationship.passive_deletes` flag to the string
          ``"all"``.  This has the effect of entirely disabling
          SQLAlchemy's behavior of setting the foreign key column to NULL,
          and a DELETE will be emitted for the parent row without any
          affect on the child row, even if the child row is present in
          memory. This may be desirable in the case when database-level
          foreign key triggers, either special ``ON DELETE`` settings or
          otherwise, need to be activated in all cases when a parent row is
          deleted.

    * Database level ``ON DELETE`` cascade is generally much more efficient
      than relying upon the "cascade" delete feature of SQLAlchemy.  The
      database can chain a series of cascade operations across many
      relationships at once; e.g. if row A is deleted, all the related rows in
      table B can be deleted, and all the C rows related to each of those B
      rows, and on and on, all within the scope of a single DELETE statement.
      SQLAlchemy on the other hand, in order to support the cascading delete
      operation fully, has to individually load each related collection in
      order to target all rows that then may have further related collections.
      That is, SQLAlchemy isn't sophisticated enough to emit a DELETE for all
      those related rows at once within this context.

    * SQLAlchemy doesn't **need** to be this sophisticated, as we instead
      provide smooth integration with the database's own ``ON DELETE``
      functionality, by using the :paramref:`_orm.relationship.passive_deletes`
      option in conjunction with properly configured foreign key constraints.
      Under this behavior, SQLAlchemy only emits DELETE for those rows that are
      already locally present in the :class:`.Session`; for any collections
      that are unloaded, it leaves them to the database to handle, rather than
      emitting a SELECT for them.  The section :ref:`passive_deletes` provides
      an example of this use.

    * While database-level ``ON DELETE`` functionality works only on the "many"
      side of a relationship, SQLAlchemy's "delete" cascade has **limited**
      ability to operate in the *reverse* direction as well, meaning it can be
      configured on the "many" side to delete an object on the "one" side when
      the reference on the "many" side is deleted.  However this can easily
      result in constraint violations if there are other objects referring to
      this "one" side from the "many", so it typically is only useful when a
      relationship is in fact a "one to one".  The
      :paramref:`_orm.relationship.single_parent` flag should be used to
      establish an in-Python assertion for this case.

.. _passive_deletes_many_to_many:

Using foreign key ON DELETE with many-to-many relationships
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As described at :ref:`cascade_delete_many_to_many`, "delete" cascade works
for many-to-many relationships as well.  To make use of ``ON DELETE CASCADE``
foreign keys in conjunction with many to many, ``FOREIGN KEY`` directives
are configured on the association table.   These directives can handle
the task of automatically deleting from the association table, but cannot
accommodate the automatic deletion of the related objects themselves.

In this case, the :paramref:`_orm.relationship.passive_deletes` directive can
save us some additional ``SELECT`` statements during a delete operation but
there are still some collections that the ORM will continue to load, in order
to locate affected child objects and handle them correctly.

.. note::

  Hypothetical optimizations to this could include a single ``DELETE``
  statement against all parent-associated rows of the association table at
  once, then use ``RETURNING`` to locate affected related child rows, however
  this is not currently part of the ORM unit of work implementation.

In this configuration, we configure ``ON DELETE CASCADE`` on both foreign key
constraints of the association table.  We configure ``cascade="all, delete"``
on the parent->child side of the relationship, and we can then configure
``passive_deletes=True`` on the **other** side of the bidirectional
relationship as illustrated below::

    association_table = Table('association', Base.metadata,
        Column('left_id', Integer, ForeignKey('left.id', ondelete="CASCADE")),
        Column('right_id', Integer, ForeignKey('right.id', ondelete="CASCADE"))
    )

    class Parent(Base):
        __tablename__ = 'left'
        id = Column(Integer, primary_key=True)
        children = relationship(
            "Child",
            secondary=association_table,
            back_populates="parents",
            cascade="all, delete",
        )

    class Child(Base):
        __tablename__ = 'right'
        id = Column(Integer, primary_key=True)
        parents = relationship(
            "Parent",
            secondary=association_table,
            back_populates="children",
            passive_deletes=True
        )

Using the above configuration, the deletion of a ``Parent`` object proceeds
as follows:

1. A ``Parent`` object is marked for deletion using
   :meth:`_orm.Session.delete`.

2. When the flush occurs, if the ``Parent.children`` collection is not loaded,
   the ORM will first emit a SELECT statement in order to load the ``Child``
   objects that correspond to ``Parent.children``.

3. It will then then emit ``DELETE`` statements for the rows in ``association``
   which correspond to that parent row.

4. for each ``Child`` object affected by this immediate deletion, because
   ``passive_deletes=True`` is configured, the unit of work will not need to
   try to emit SELECT statements for each ``Child.parents`` collection as it
   is assumed the corresponding rows in ``association`` will be deleted.

5. ``DELETE`` statements are then emitted for each ``Child`` object that was
   loaded from ``Parent.children``.


.. _cascade_delete_orphan:

delete-orphan
-------------

``delete-orphan`` cascade adds behavior to the ``delete`` cascade,
such that a child object will be marked for deletion when it is
de-associated from the parent, not just when the parent is marked
for deletion.   This is a common feature when dealing with a related
object that is "owned" by its parent, with a NOT NULL foreign key,
so that removal of the item from the parent collection results
in its deletion.

``delete-orphan`` cascade implies that each child object can only
have one parent at a time, and in the **vast majority of cases is configured
only on a one-to-many relationship.**   For the much less common
case of setting it on a many-to-one or
many-to-many relationship, the "many" side can be forced to allow only
a single object at a time by configuring the :paramref:`_orm.relationship.single_parent` argument,
which establishes Python-side validation that ensures the object
is associated with only one parent at a time, however this greatly limits
the functionality of the "many" relationship and is usually not what's
desired.

.. seealso::

  :ref:`error_bbf0` - background on a common error scenario involving delete-orphan
  cascade.

.. _cascade_merge:

merge
-----

``merge`` cascade indicates that the :meth:`.Session.merge`
operation should be propagated from a parent that's the subject
of the :meth:`.Session.merge` call down to referred objects.
This cascade is also on by default.

.. _cascade_refresh_expire:

refresh-expire
--------------

``refresh-expire`` is an uncommon option, indicating that the
:meth:`.Session.expire` operation should be propagated from a parent
down to referred objects.   When using :meth:`.Session.refresh`,
the referred objects are expired only, but not actually refreshed.

.. _cascade_expunge:

expunge
-------

``expunge`` cascade indicates that when the parent object is removed
from the :class:`.Session` using :meth:`.Session.expunge`, the
operation should be propagated down to referred objects.

.. _backref_cascade:

Controlling Cascade on Backrefs
-------------------------------

.. note:: This section applies to a behavior that is removed in SQLAlchemy 2.0.
   By setting the :paramref:`_orm.Session.future` flag on a given
   :class:`_orm.Session`, the 2.0 behavior will be achieved which is
   essentially that the :paramref:`_orm.relationship.cascade_backrefs` flag is
   ignored.   See the section :ref:`change_5150` for notes.

In :term:`1.x style` ORM usage, the :ref:`cascade_save_update` cascade by
default takes place on attribute change events emitted from backrefs.  This is
probably a confusing statement more easily described through demonstration; it
means that, given a mapping such as this::

    mapper_registry.map_imperatively(Order, order_table, properties={
        'items' : relationship(Item, backref='order')
    })

If an ``Order`` is already in the session, and is assigned to the ``order``
attribute of an ``Item``, the backref appends the ``Item`` to the ``items``
collection of that ``Order``, resulting in the ``save-update`` cascade taking
place::

    >>> o1 = Order()
    >>> session.add(o1)
    >>> o1 in session
    True

    >>> i1 = Item()
    >>> i1.order = o1
    >>> i1 in o1.items
    True
    >>> i1 in session
    True

This behavior can be disabled using the :paramref:`_orm.relationship.cascade_backrefs` flag::

    mapper_registry.map_imperatively(Order, order_table, properties={
        'items' : relationship(Item, backref='order', cascade_backrefs=False)
    })

So above, the assignment of ``i1.order = o1`` will append ``i1`` to the ``items``
collection of ``o1``, but will not add ``i1`` to the session.   You can, of
course, :meth:`~.Session.add` ``i1`` to the session at a later point.   This
option may be helpful for situations where an object needs to be kept out of a
session until it's construction is completed, but still needs to be given
associations to objects which are already persistent in the target session.



.. _session_deleting_from_collections:

Notes on Delete - Deleting Objects Referenced from Collections and Scalar Relationships
----------------------------------------------------------------------------------------

The ORM in general never modifies the contents of a collection or scalar
relationship during the flush process.  This means, if your class has a
:func:`_orm.relationship` that refers to a collection of objects, or a reference
to a single object such as many-to-one, the contents of this attribute will
not be modified when the flush process occurs.  Instead, it is expected
that the :class:`.Session` would eventually be expired, either through the expire-on-commit behavior of
:meth:`.Session.commit` or through explicit use of :meth:`.Session.expire`.
At that point, any referenced object or collection associated with that
:class:`.Session` will be cleared and will re-load itself upon next access.

A common confusion that arises regarding this behavior involves the use of the
:meth:`~.Session.delete` method.   When :meth:`.Session.delete` is invoked upon
an object and the :class:`.Session` is flushed, the row is deleted from the
database.  Rows that refer to the target row via  foreign key, assuming they
are tracked using a :func:`_orm.relationship` between the two mapped object types,
will also see their foreign key attributes UPDATED to null, or if delete
cascade is set up, the related rows will be deleted as well. However, even
though rows related to the deleted object might be themselves modified as well,
**no changes occur to relationship-bound collections or object references on
the objects** involved in the operation within the scope of the flush
itself.   This means if the object was a
member of a related collection, it will still be present on the Python side
until that collection is expired.  Similarly, if the object were
referenced via many-to-one or one-to-one from another object, that reference
will remain present on that object until the object is expired as well.

Below, we illustrate that after an ``Address`` object is marked
for deletion, it's still present in the collection associated with the
parent ``User``, even after a flush::

    >>> address = user.addresses[1]
    >>> session.delete(address)
    >>> session.flush()
    >>> address in user.addresses
    True

When the above session is committed, all attributes are expired.  The next
access of ``user.addresses`` will re-load the collection, revealing the
desired state::

    >>> session.commit()
    >>> address in user.addresses
    False

There is a recipe for intercepting :meth:`.Session.delete` and invoking this
expiration automatically; see `ExpireRelationshipOnFKChange <http://www.sqlalchemy.org/trac/wiki/UsageRecipes/ExpireRelationshipOnFKChange>`_ for this.  However, the usual practice of
deleting items within collections is to forego the usage of
:meth:`~.Session.delete` directly, and instead use cascade behavior to
automatically invoke the deletion as a result of removing the object from the
parent collection.  The ``delete-orphan`` cascade accomplishes this, as
illustrated in the example below::

    class User(Base):
        __tablename__ = 'user'

        # ...

        addresses = relationship(
            "Address", cascade="all, delete-orphan")

    # ...

    del user.addresses[1]
    session.flush()

Where above, upon removing the ``Address`` object from the ``User.addresses``
collection, the ``delete-orphan`` cascade has the effect of marking the ``Address``
object for deletion in the same way as passing it to :meth:`~.Session.delete`.

The ``delete-orphan`` cascade can also be applied to a many-to-one
or one-to-one relationship, so that when an object is de-associated from its
parent, it is also automatically marked for deletion.   Using ``delete-orphan``
cascade on a many-to-one or one-to-one requires an additional flag
:paramref:`_orm.relationship.single_parent` which invokes an assertion
that this related object is not to shared with any other parent simultaneously::

    class User(Base):
        # ...

        preference = relationship(
            "Preference", cascade="all, delete-orphan",
            single_parent=True)


Above, if a hypothetical ``Preference`` object is removed from a ``User``,
it will be deleted on flush::

    some_user.preference = None
    session.flush()  # will delete the Preference object

.. seealso::

    :ref:`unitofwork_cascades` for detail on cascades.

