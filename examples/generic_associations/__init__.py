"""
Illustrates various methods of associating multiple types of 
parents with a particular child object.

The examples all use the declarative extension along with 
declarative mixins.   Each one presents the identical use
case at the end - two classes, ``Customer`` and ``Supplier``, both
subclassing the ``HasAddresses`` mixin, which ensures that the
parent class is provided with an ``addresses`` collection
which contains ``Address`` objects.

The configurations include:

* ``table_per_related.py`` - illustrates a distinct table per related collection.
* ``table_per_association.py`` - illustrates a shared collection table, using a
  table per association.
* ``discriminator_on_association.py`` - shared collection table and shared
  association table, including a discriminator column.

The ``discriminator_on_association.py`` script in particular is a modernized
version of the "polymorphic associations" example present in older versions of
SQLAlchemy, originally from the blog post at http://techspot.zzzeek.org/2007/05/29/polymorphic-associations-with-sqlalchemy/.

"""