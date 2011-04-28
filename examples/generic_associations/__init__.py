"""
Illustrates various methods of associating multiple types of 
parents with a particular child object.

The examples all use the declarative extension along with 
declarative mixins.   Each one presents the identical use
case at the end - two clases, ``Customer`` and ``Supplier``, both
subclassing the ``HasAddresses`` mixin, which ensures that the
parent class is provided with an ``addresses`` collection
which contains ``Address`` objects.

The ``discriminator_on_association.py`` script in particular is a modernized
version of the "polymorphic associations" example present in older versions of
SQLAlchemy.


"""