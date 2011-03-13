"""

Examples illustrating the usage of the "association object" pattern,
where an intermediary class mediates the relationship between two
classes that are associated in a many-to-many pattern.

This directory includes the following examples:

* basic_association.py - illustrate a many-to-many relationship between an 
  "Order" and a collection of "Item" objects, associating a purchase price
  with each via an association object called "OrderItem"
* proxied_association.py - same example as basic_association, adding in
  usage of :mod:`sqlalchemy.ext.associationproxy` to make explicit references
  to "OrderItem" optional.
* dict_of_sets_with_default.py - an advanced association proxy example which
  illustrates nesting of association proxies to produce multi-level Python
  collections, in this case a dictionary with string keys and sets of integers
  as values, which conceal the underlying mapped classes.

"""