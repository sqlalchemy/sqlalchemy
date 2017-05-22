Special Relationship Persistence Patterns
=========================================

.. _post_update:

Rows that point to themselves / Mutually Dependent Rows
-------------------------------------------------------

This is a very specific case where relationship() must perform an INSERT and a
second UPDATE in order to properly populate a row (and vice versa an UPDATE
and DELETE in order to delete without violating foreign key constraints). The
two use cases are:

* A table contains a foreign key to itself, and a single row will
  have a foreign key value pointing to its own primary key.
* Two tables each contain a foreign key referencing the other
  table, with a row in each table referencing the other.

For example::

              user
    ---------------------------------
    user_id    name   related_user_id
       1       'ed'          1

Or::

                 widget                                                  entry
    -------------------------------------------             ---------------------------------
    widget_id     name        favorite_entry_id             entry_id      name      widget_id
       1       'somewidget'          5                         5       'someentry'     1

In the first case, a row points to itself. Technically, a database that uses
sequences such as PostgreSQL or Oracle can INSERT the row at once using a
previously generated value, but databases which rely upon autoincrement-style
primary key identifiers cannot. The :func:`~sqlalchemy.orm.relationship`
always assumes a "parent/child" model of row population during flush, so
unless you are populating the primary key/foreign key columns directly,
:func:`~sqlalchemy.orm.relationship` needs to use two statements.

In the second case, the "widget" row must be inserted before any referring
"entry" rows, but then the "favorite_entry_id" column of that "widget" row
cannot be set until the "entry" rows have been generated. In this case, it's
typically impossible to insert the "widget" and "entry" rows using just two
INSERT statements; an UPDATE must be performed in order to keep foreign key
constraints fulfilled. The exception is if the foreign keys are configured as
"deferred until commit" (a feature some databases support) and if the
identifiers were populated manually (again essentially bypassing
:func:`~sqlalchemy.orm.relationship`).

To enable the usage of a supplementary UPDATE statement,
we use the :paramref:`~.relationship.post_update` option
of :func:`.relationship`.  This specifies that the linkage between the
two rows should be created using an UPDATE statement after both rows
have been INSERTED; it also causes the rows to be de-associated with
each other via UPDATE before a DELETE is emitted.  The flag should
be placed on just *one* of the relationships, preferably the
many-to-one side.  Below we illustrate
a complete example, including two :class:`.ForeignKey` constructs::

    from sqlalchemy import Integer, ForeignKey, Column
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class Entry(Base):
        __tablename__ = 'entry'
        entry_id = Column(Integer, primary_key=True)
        widget_id = Column(Integer, ForeignKey('widget.widget_id'))
        name = Column(String(50))

    class Widget(Base):
        __tablename__ = 'widget'

        widget_id = Column(Integer, primary_key=True)
        favorite_entry_id = Column(Integer,
                                ForeignKey('entry.entry_id',
                                name="fk_favorite_entry"))
        name = Column(String(50))

        entries = relationship(Entry, primaryjoin=
                                        widget_id==Entry.widget_id)
        favorite_entry = relationship(Entry,
                                    primaryjoin=
                                        favorite_entry_id==Entry.entry_id,
                                    post_update=True)

When a structure against the above configuration is flushed, the "widget" row will be
INSERTed minus the "favorite_entry_id" value, then all the "entry" rows will
be INSERTed referencing the parent "widget" row, and then an UPDATE statement
will populate the "favorite_entry_id" column of the "widget" table (it's one
row at a time for the time being):

.. sourcecode:: pycon+sql

    >>> w1 = Widget(name='somewidget')
    >>> e1 = Entry(name='someentry')
    >>> w1.favorite_entry = e1
    >>> w1.entries = [e1]
    >>> session.add_all([w1, e1])
    {sql}>>> session.commit()
    BEGIN (implicit)
    INSERT INTO widget (favorite_entry_id, name) VALUES (?, ?)
    (None, 'somewidget')
    INSERT INTO entry (widget_id, name) VALUES (?, ?)
    (1, 'someentry')
    UPDATE widget SET favorite_entry_id=? WHERE widget.widget_id = ?
    (1, 1)
    COMMIT

An additional configuration we can specify is to supply a more
comprehensive foreign key constraint on ``Widget``, such that
it's guaranteed that ``favorite_entry_id`` refers to an ``Entry``
that also refers to this ``Widget``.  We can use a composite foreign key,
as illustrated below::

    from sqlalchemy import Integer, ForeignKey, String, \
            Column, UniqueConstraint, ForeignKeyConstraint
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import relationship

    Base = declarative_base()

    class Entry(Base):
        __tablename__ = 'entry'
        entry_id = Column(Integer, primary_key=True)
        widget_id = Column(Integer, ForeignKey('widget.widget_id'))
        name = Column(String(50))
        __table_args__ = (
            UniqueConstraint("entry_id", "widget_id"),
        )

    class Widget(Base):
        __tablename__ = 'widget'

        widget_id = Column(Integer, autoincrement='ignore_fk', primary_key=True)
        favorite_entry_id = Column(Integer)

        name = Column(String(50))

        __table_args__ = (
            ForeignKeyConstraint(
                ["widget_id", "favorite_entry_id"],
                ["entry.widget_id", "entry.entry_id"],
                name="fk_favorite_entry"
            ),
        )

        entries = relationship(Entry, primaryjoin=
                                        widget_id==Entry.widget_id,
                                        foreign_keys=Entry.widget_id)
        favorite_entry = relationship(Entry,
                                    primaryjoin=
                                        favorite_entry_id==Entry.entry_id,
                                    foreign_keys=favorite_entry_id,
                                    post_update=True)

The above mapping features a composite :class:`.ForeignKeyConstraint`
bridging the ``widget_id`` and ``favorite_entry_id`` columns.  To ensure
that ``Widget.widget_id`` remains an "autoincrementing" column we specify
:paramref:`~.Column.autoincrement` to the value ``"ignore_fk"``
on :class:`.Column`, and additionally on each
:func:`.relationship` we must limit those columns considered as part of
the foreign key for the purposes of joining and cross-population.

.. _passive_updates:

Mutable Primary Keys / Update Cascades
--------------------------------------

When the primary key of an entity changes, related items
which reference the primary key must also be updated as
well. For databases which enforce referential integrity,
the best strategy is to use the database's ON UPDATE CASCADE
functionality in order to propagate primary key changes
to referenced foreign keys - the values cannot be out
of sync for any moment unless the constraints are marked as "deferrable",
that is, not enforced until the transaction completes.

It is **highly recommended** that an application which seeks to employ
natural primary keys with mutable values to use the ``ON UPDATE CASCADE``
capabilities of the database.   An example mapping which
illustrates this is::

    class User(Base):
        __tablename__ = 'user'
        __table_args__ = {'mysql_engine': 'InnoDB'}

        username = Column(String(50), primary_key=True)
        fullname = Column(String(100))

        addresses = relationship("Address")


    class Address(Base):
        __tablename__ = 'address'
        __table_args__ = {'mysql_engine': 'InnoDB'}

        email = Column(String(50), primary_key=True)
        username = Column(String(50),
                    ForeignKey('user.username', onupdate="cascade")
                )

Above, we illustrate ``onupdate="cascade"`` on the :class:`.ForeignKey`
object, and we also illustrate the ``mysql_engine='InnoDB'`` setting
which, on a MySQL backend, ensures that the ``InnoDB`` engine supporting
referential integrity is used.  When using SQLite, referential integrity
should be enabled, using the configuration described at
:ref:`sqlite_foreign_keys`.

.. seealso::

    :ref:`passive_deletes` - supporting ON DELETE CASCADE with relationships

    :paramref:`.orm.mapper.passive_updates` - similar feature on :func:`.mapper`


Simulating limited ON UPDATE CASCADE without foreign key support
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In those cases when a database that does not support referential integrity
is used, and natural primary keys with mutable values are in play,
SQLAlchemy offers a feature in order to allow propagation of primary key
values to already-referenced foreign keys to a **limited** extent,
by emitting an UPDATE statement against foreign key columns that immediately
reference a primary key column whose value has changed.
The primary platforms without referential integrity features are
MySQL when the ``MyISAM`` storage engine is used, and SQLite when the
``PRAGMA foreign_keys=ON`` pragma is not used.  The Oracle database also
has no support for ``ON UPDATE CASCADE``, but because it still enforces
referential integrity, needs constraints to be marked as deferrable
so that SQLAlchemy can emit UPDATE statements.

The feature is enabled by setting the
:paramref:`~.relationship.passive_updates` flag to ``False``,
most preferably on a one-to-many or
many-to-many :func:`.relationship`.  When "updates" are no longer
"passive" this indicates that SQLAlchemy will
issue UPDATE statements individually for
objects referenced in the collection referred to by the parent object
with a changing primary key value.  This also implies that collections
will be fully loaded into memory if not already locally present.

Our previous mapping using ``passive_updates=False`` looks like::

    class User(Base):
        __tablename__ = 'user'

        username = Column(String(50), primary_key=True)
        fullname = Column(String(100))

        # passive_updates=False *only* needed if the database
        # does not implement ON UPDATE CASCADE
        addresses = relationship("Address", passive_updates=False)

    class Address(Base):
        __tablename__ = 'address'

        email = Column(String(50), primary_key=True)
        username = Column(String(50), ForeignKey('user.username'))

Key limitations of ``passive_updates=False`` include:

* it performs much more poorly than direct database ON UPDATE CASCADE,
  because it needs to fully pre-load affected collections using SELECT
  and also must emit  UPDATE statements against those values, which it
  will attempt to run  in "batches" but still runs on a per-row basis
  at the DBAPI level.

* the feature cannot "cascade" more than one level.  That is,
  if mapping X has a foreign key which refers to the primary key
  of mapping Y, but then mapping Y's primary key is itself a foreign key
  to mapping Z, ``passive_updates=False`` cannot cascade a change in
  primary key value from ``Z`` to ``X``.

* Configuring ``passive_updates=False`` only on the many-to-one
  side of a relationship will not have a full effect, as the
  unit of work searches only through the current identity
  map for objects that may be referencing the one with a
  mutating primary key, not throughout the database.

As virtually all databases other than Oracle now support ``ON UPDATE CASCADE``,
it is highly recommended that traditional ``ON UPDATE CASCADE`` support be used
in the case that natural and mutable primary key values are in use.

