orderinglist
============

.. module: sqlalchemy.ext.orderinglist

:author: Jason Kirtland

``orderinglist`` is a helper for mutable ordered relations.  It will intercept
list operations performed on a relation collection and automatically
synchronize changes in list position with an attribute on the related objects.
(See :ref:`advdatamapping_entitycollections` for more information on the general pattern.)

Example: Two tables that store slides in a presentation.  Each slide
has a number of bullet points, displayed in order by the 'position'
column on the bullets table.  These bullets can be inserted and re-ordered
by your end users, and you need to update the 'position' column of all
affected rows when changes are made.

.. sourcecode:: python+sql

    slides_table = Table('Slides', metadata,
                         Column('id', Integer, primary_key=True),
                         Column('name', String))

    bullets_table = Table('Bullets', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('slide_id', Integer, ForeignKey('Slides.id')),
                          Column('position', Integer),
                          Column('text', String))

     class Slide(object):
         pass
     class Bullet(object):
         pass

     mapper(Slide, slides_table, properties={
           'bullets': relation(Bullet, order_by=[bullets_table.c.position])
     })
     mapper(Bullet, bullets_table)

The standard relation mapping will produce a list-like attribute on each Slide
containing all related Bullets, but coping with changes in ordering is totally
your responsibility.  If you insert a Bullet into that list, there is no
magic- it won't have a position attribute unless you assign it it one, and
you'll need to manually renumber all the subsequent Bullets in the list to
accommodate the insert.

An ``orderinglist`` can automate this and manage the 'position' attribute on all
related bullets for you.

.. sourcecode:: python+sql
        
    mapper(Slide, slides_table, properties={
           'bullets': relation(Bullet,
                               collection_class=ordering_list('position'),
                               order_by=[bullets_table.c.position])
    })
    mapper(Bullet, bullets_table)

    s = Slide()
    s.bullets.append(Bullet())
    s.bullets.append(Bullet())
    s.bullets[1].position
    >>> 1
    s.bullets.insert(1, Bullet())
    s.bullets[2].position
    >>> 2

Use the ``ordering_list`` function to set up the ``collection_class`` on relations
(as in the mapper example above).  This implementation depends on the list
starting in the proper order, so be SURE to put an order_by on your relation.

``ordering_list`` takes the name of the related object's ordering attribute as
an argument.  By default, the zero-based integer index of the object's
position in the ``ordering_list`` is synchronized with the ordering attribute:
index 0 will get position 0, index 1 position 1, etc.  To start numbering at 1
or some other integer, provide ``count_from=1``.

Ordering values are not limited to incrementing integers.  Almost any scheme
can implemented by supplying a custom ``ordering_func`` that maps a Python list
index to any value you require.  See the [module
documentation](rel:docstrings_sqlalchemy.ext.orderinglist) for more
information, and also check out the unit tests for examples of stepped
numbering, alphabetical and Fibonacci numbering.

.. automodule:: sqlalchemy.ext.orderinglist
   :members:
   :undoc-members:
