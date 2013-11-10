"""
Illustrates various methods of associating multiple types of
parents with a particular child object.

The examples all use the declarative extension along with
declarative mixins.   Each one presents the identical use
case at the end - two classes, ``Customer`` and ``Supplier``, both
subclassing the ``HasAddresses`` mixin, which ensures that the
parent class is provided with an ``addresses`` collection
which contains ``Address`` objects.

The :viewsource:`.discriminator_on_association` and :viewsource:`.generic_fk` scripts
are modernized versions of recipes presented in the 2007 blog post
`Polymorphic Associations with SQLAlchemy <http://techspot.zzzeek.org/2007/05/29/polymorphic-associations-with-sqlalchemy/>`_.

.. autosource::

"""