=============================
What's New in SQLAlchemy 1.4?
=============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 1.3
    and SQLAlchemy version 1.4.


Behavioral Changes - ORM
========================

.. _change_4662:

The "New instance conflicts with existing identity" error is now a warning
---------------------------------------------------------------------------

SQLAlchemy has always had logic to detect when an object in the :class:`.Session`
to be inserted has the same primary key as an object that is already present::

    class Product(Base):
        __tablename__ = 'product'

        id = Column(Integer, primary_key=True)

    session = Session(engine)

    # add Product with primary key 1
    session.add(Product(id=1))
    session.flush()

    # add another Product with same primary key
    session.add(Product(id=1))
    s.commit()  # <-- will raise FlushError

The change is that the :class:`.FlushError` is altered to be only a warning::

    sqlalchemy/orm/persistence.py:408: SAWarning: New instance <Product at 0x7f1ff65e0ba8> with identity key (<class '__main__.Product'>, (1,), None) conflicts with persistent instance <Product at 0x7f1ff60a4550>


Subsequent to that, the condition will attempt to insert the row into the
database which will emit :class:`.IntegrityError`, which is the same error that
would be raised if the primary key identity was not already present in the
:class:`.Session`::

    sqlalchemy.exc.IntegrityError: (sqlite3.IntegrityError) UNIQUE constraint failed: product.id

The rationale is to allow code that is using :class:`.IntegrityError` to catch
duplicates to function regardless of the existing state of the
:class:`.Session`, as is often done using savepoints::


    # add another Product with same primary key
    try:
        with session.begin_nested():
            session.add(Product(id=1))
    except exc.IntegrityError:
        print("row already exists")

The above logic was not fully feasible earlier, as in the case that the
``Product`` object with the existing identity were already in the
:class:`.Session`, the code would also have to catch :class:`.FlushError`,
which additionally is not filtered for the specific condition of integrity
issues.   With the change, the above block behaves consistently with the
exception of the warning also being emitted.

Since the logic in question deals with the primary key, all databases emit an
integrity error in the case of primary key conflicts on INSERT.    The case
where an error would not be raised, that would have earlier, is the extremely
unusual scenario of a mapping that defines a primary key on the mapped
selectable that is more restrictive than what is actually configured in the
database schema, such as when mapping to joins of tables or when defining
additional columns as part of a composite primary key that is not actually
constrained in the database schema. However, these situations also work  more
consistently in that the INSERT would theoretically proceed whether or not the
existing identity were still in the database.  The warning can also be
configured to raise an exception using the Python warnings filter.


:ticket:`4662`
