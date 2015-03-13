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
---------------------------------------

When the primary key of an entity changes, related items
which reference the primary key must also be updated as
well. For databases which enforce referential integrity,
it's required to use the database's ON UPDATE CASCADE
functionality in order to propagate primary key changes
to referenced foreign keys - the values cannot be out
of sync for any moment.

For databases that don't support this, such as SQLite and
MySQL without their referential integrity options turned
on, the :paramref:`~.relationship.passive_updates` flag can
be set to ``False``, most preferably on a one-to-many or
many-to-many :func:`.relationship`, which instructs
SQLAlchemy to issue UPDATE statements individually for
objects referenced in the collection, loading them into
memory if not already locally present. The
:paramref:`~.relationship.passive_updates` flag can also be ``False`` in
conjunction with ON UPDATE CASCADE functionality,
although in that case the unit of work will be issuing
extra SELECT and UPDATE statements unnecessarily.

A typical mutable primary key setup might look like::

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
        username = Column(String(50),
                    ForeignKey('user.username', onupdate="cascade")
                )

:paramref:`~.relationship.passive_updates` is set to ``True`` by default,
indicating that ON UPDATE CASCADE is expected to be in
place in the usual case for foreign keys that expect
to have a mutating parent key.

A :paramref:`~.relationship.passive_updates` setting of False may be configured on any
direction of relationship, i.e. one-to-many, many-to-one,
and many-to-many, although it is much more effective when
placed just on the one-to-many or many-to-many side.
Configuring the :paramref:`~.relationship.passive_updates`
to False only on the
many-to-one side will have only a partial effect, as the
unit of work searches only through the current identity
map for objects that may be referencing the one with a
mutating primary key, not throughout the database.
