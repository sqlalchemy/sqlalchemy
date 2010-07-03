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
join available as a Python list of :func:`related <relationship>` objects.

Mapper properties are most commonly included in the :func:`mapper`
call::

  mapper(Parent, properties={
     'children': relationship(Children)
  }

.. autofunction:: backref

.. autofunction:: column_property

.. autofunction:: comparable_property

.. autofunction:: composite

.. autofunction:: deferred

.. autofunction:: dynamic_loader

.. autofunction:: relation

.. autofunction:: relationship

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

Attribute Utilities
-------------------
.. autofunction:: sqlalchemy.orm.attributes.del_attribute

.. autofunction:: sqlalchemy.orm.attributes.get_attribute

.. autofunction:: sqlalchemy.orm.attributes.get_history

.. autofunction:: sqlalchemy.orm.attributes.init_collection

.. function:: sqlalchemy.orm.attributes.instance_state

    Return the :class:`InstanceState` for a given object.

.. autofunction:: sqlalchemy.orm.attributes.is_instrumented

.. function:: sqlalchemy.orm.attributes.manager_of_class

    Return the :class:`ClassManager` for a given class.

.. autofunction:: sqlalchemy.orm.attributes.set_attribute

.. autofunction:: sqlalchemy.orm.attributes.set_committed_value

Internals
---------

.. autoclass:: sqlalchemy.orm.mapper.Mapper
   :members:

.. autoclass:: sqlalchemy.orm.interfaces.MapperProperty
  :members:
