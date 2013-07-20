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
* ``generic_fk.py`` - imitates the approach taken by popular frameworks such
  as Django and Ruby on Rails to create a so-called "generic foreign key".

The ``discriminator_on_association.py`` and ``generic_fk.py`` scripts
are modernized versions of recipes presented in the 2007 blog post
`Polymorphic Associations with SQLAlchemy <http://techspot.zzzeek.org/2007/05/29/polymorphic-associations-with-sqlalchemy/>`_.
.

"""