"""
Illustrates polymorphic associations, a method of associating a particular child object with many different types of parent object.

This example is based off the original blog post at `<http://techspot.zzzeek.org/?p=13>`_ and illustrates three techniques:

* ``poly_assoc.py`` - imitates the non-foreign-key schema used by Ruby on Rails' Active Record.
* ``poly_assoc_fk.py`` - Adds a polymorphic association table so that referential integrity can be maintained.
* ``poly_assoc_generic.py`` - further automates the approach of ``poly_assoc_fk.py`` to also generate the association table definitions automatically.

"""