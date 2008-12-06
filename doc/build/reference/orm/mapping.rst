Class Mapping
=============

.. module:: sqlalchemy.orm

Defining Mappings
-----------------

Python classes are mapped to the database using the :func:`mapper` function.

.. autofunction:: mapper

Mapper Properties
-----------------

A basic mapping of a class will simply make the columns of the
database table or selectable available as attributes on the class.
**Mapper properties** allow you to customize and add additional
properties to your classes, for example making the results one-to-many
join available as a Python list of :func:`related <relation>` objects.

Mapper properties are most commonly included in the :func:`mapper`
call::

  mapper(Parent, properties={
     'children': relation(Children)
  }

.. autofunction:: backref

.. autofunction:: column_property

.. autofunction:: comparable_property

.. autofunction:: composite

.. autofunction:: deferred

.. autofunction:: dynamic_loader

.. autofunction:: relation

.. autofunction:: synonym

Decorators
----------

.. autofunction:: reconstructor

.. autofunction:: validates

Utilities
---------

.. autofunction:: object_mapper

.. autofunction:: class_mapper

.. autofunction:: compile_mappers

.. autofunction:: clear_mappers

Internals
---------

.. autoclass:: sqlalchemy.orm.mapper.Mapper
   :members:
