.. _orm_internal_toplevel:

ORM Internals
=============

Key ORM constructs, not otherwise covered in other
sections, are listed here.

.. currentmodule:: sqlalchemy.orm

.. autoclass:: AttributeState
    :members:

.. autoclass:: CascadeOptions
    :members:

.. autoclass:: ClassManager
    :members:
    :inherited-members:

.. autoclass:: ColumnProperty
    :members:

    .. attribute:: Comparator.expressions

         The full sequence of columns referenced by this
         attribute, adjusted for any aliasing in progress.

         .. versionadded:: 1.3.17

         .. seealso::

            :ref:`maptojoin` - usage example

.. autoclass:: CompositeProperty
    :members:


.. autoclass:: AttributeEvent
    :members:

.. autoclass:: IdentityMap
    :members:

.. autoclass:: InspectionAttr
    :members:

.. autoclass:: InspectionAttrInfo
    :members:

.. autoclass:: InstanceState
    :members:


.. autoclass:: InstrumentedAttribute
    :members: __get__, __set__, __delete__
    :undoc-members:

.. autodata:: MANYTOONE

.. autodata:: MANYTOMANY

.. autoclass:: Mapped

.. autoclass:: MapperProperty
    :members:

    .. py:attribute:: info

        Info dictionary associated with the object, allowing user-defined
        data to be associated with this :class:`.InspectionAttr`.

        The dictionary is generated when first accessed.  Alternatively,
        it can be specified as a constructor argument to the
        :func:`.column_property`, :func:`_orm.relationship`, or :func:`.composite`
        functions.

        .. versionchanged:: 1.0.0 :attr:`.InspectionAttr.info` moved
           from :class:`.MapperProperty` so that it can apply to a wider
           variety of ORM and extension constructs.

        .. seealso::

            :attr:`.QueryableAttribute.info`

            :attr:`.SchemaItem.info`

.. autodata:: NOT_EXTENSION

.. autofunction:: merge_result

.. autofunction:: merge_frozen_result


.. autodata:: ONETOMANY

.. autoclass:: PropComparator
    :members:
    :inherited-members:

.. autoclass:: RelationshipProperty
    :members:
    :inherited-members:

.. autoclass:: SynonymProperty
    :members:
    :inherited-members:

.. autoclass:: QueryContext
    :members:


.. autoclass:: QueryableAttribute
    :members:
    :inherited-members:

.. autoclass:: UOWTransaction
    :members:

