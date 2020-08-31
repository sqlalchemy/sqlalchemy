.. _orm_internal_toplevel:

ORM Internals
=============

Key ORM constructs, not otherwise covered in other
sections, are listed here.

.. currentmodule:: sqlalchemy.orm

.. autoclass:: sqlalchemy.orm.state.AttributeState
    :members:

.. autoclass:: sqlalchemy.orm.util.CascadeOptions
    :members:

.. autoclass:: sqlalchemy.orm.instrumentation.ClassManager
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.ColumnProperty
    :members:

    .. attribute:: Comparator.expressions

         The full sequence of columns referenced by this
         attribute, adjusted for any aliasing in progress.

         .. versionadded:: 1.3.17

         .. seealso::

            :ref:`maptojoin` - usage example

.. autoclass:: sqlalchemy.orm.CompositeProperty
    :members:


.. autoclass:: sqlalchemy.orm.attributes.Event
    :members:

.. autoclass:: sqlalchemy.orm.identity.IdentityMap
    :members:

.. autoclass:: sqlalchemy.orm.base.InspectionAttr
    :members:

.. autoclass:: sqlalchemy.orm.base.InspectionAttrInfo
    :members:

.. autoclass:: sqlalchemy.orm.state.InstanceState
    :members:


.. autoclass:: sqlalchemy.orm.attributes.InstrumentedAttribute
    :members: __get__, __set__, __delete__
    :undoc-members:

.. autodata:: sqlalchemy.orm.MANYTOONE

.. autodata:: sqlalchemy.orm.MANYTOMANY

.. autoclass:: sqlalchemy.orm.MapperProperty
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

.. autodata:: sqlalchemy.orm.interfaces.NOT_EXTENSION

.. autofunction:: sqlalchemy.orm.loading.merge_result

.. autofunction:: sqlalchemy.orm.loading.merge_frozen_result


.. autodata:: sqlalchemy.orm.ONETOMANY

.. autoclass:: sqlalchemy.orm.PropComparator
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.RelationshipProperty
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.SynonymProperty
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.query.QueryContext
    :members:


.. autoclass:: sqlalchemy.orm.attributes.QueryableAttribute
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.session.UOWTransaction
    :members:

