.. _core_inspection_toplevel:
.. _inspection_toplevel:

Runtime Inspection API
======================

.. automodule:: sqlalchemy.inspection

.. autofunction:: sqlalchemy.inspect


Available Inspection Targets
----------------------------

Below is a listing of many of the most common inspection targets.

* :class:`.Connectable` (i.e. :class:`_engine.Engine`,
  :class:`_engine.Connection`) - returns an :class:`_reflection.Inspector` object.
* :class:`_expression.ClauseElement` - all SQL expression components, including
  :class:`_schema.Table`, :class:`_schema.Column`, serve as their own inspection objects,
  meaning any of these objects passed to :func:`_sa.inspect` return themselves.
* ``object`` - an object given will be checked by the ORM for a mapping -
  if so, an :class:`.InstanceState` is returned representing the mapped
  state of the object.  The :class:`.InstanceState` also provides access
  to per attribute state via the :class:`.AttributeState` interface as well
  as the per-flush "history" of any attribute via the :class:`.History`
  object.
* ``type`` (i.e. a class) - a class given will be checked by the ORM for a
  mapping - if so, a :class:`_orm.Mapper` for that class is returned.
* mapped attribute - passing a mapped attribute to :func:`_sa.inspect`, such
  as ``inspect(MyClass.some_attribute)``, returns a :class:`.QueryableAttribute`
  object, which is the :term:`descriptor` associated with a mapped class.
  This descriptor refers to a :class:`.MapperProperty`, which is usually
  an instance of :class:`.ColumnProperty`
  or :class:`.RelationshipProperty`, via its :attr:`.QueryableAttribute.property`
  attribute.
* :class:`.AliasedClass` - returns an :class:`.AliasedInsp` object.

