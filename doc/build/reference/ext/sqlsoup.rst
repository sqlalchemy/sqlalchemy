SqlSoup
=======

:author: Jonathan Ellis

SqlSoup creates mapped classes on the fly from tables, which are automatically reflected from the database based on name.  It is essentially a nicer version of the "row data gateway" pattern.

.. sourcecode:: python+sql

    >>> from sqlalchemy.ext.sqlsoup import SqlSoup
    >>> soup = SqlSoup('sqlite:///')

    >>> db.users.select(order_by=[db.users.c.name])
    [MappedUsers(name='Bhargan Basepair',email='basepair@example.edu',password='basepair',classname=None,admin=1),
     MappedUsers(name='Joe Student',email='student@example.edu',password='student',classname=None,admin=0)]

Full SqlSoup documentation is on the `SQLAlchemy Wiki <http://www.sqlalchemy.org/trac/wiki/SqlSoup>`_.

.. automodule:: sqlalchemy.ext.sqlsoup
   :members:
   :undoc-members:
