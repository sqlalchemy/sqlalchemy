.. _orm_internal_toplevel:

ORM Internals
=============

Key ORM constructs, not otherwise covered in other
sections, are listed here.

.. currentmodule: sqlalchemy.orm

.. autoclass:: sqlalchemy.orm.state.AttributeState
    :members:

.. autoclass:: sqlalchemy.orm.util.CascadeOptions
    :members:

.. autoclass:: sqlalchemy.orm.instrumentation.ClassManager
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.properties.ColumnProperty
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.properties.ComparableProperty
    :members:

.. autoclass:: sqlalchemy.orm.descriptor_props.CompositeProperty
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

.. autodata:: sqlalchemy.orm.interfaces.MANYTOONE

.. autodata:: sqlalchemy.orm.interfaces.MANYTOMANY

.. autoclass:: sqlalchemy.orm.interfaces.MapperProperty
    :members:

    .. py:attribute:: info

        Info dictionary associated with the object, allowing user-defined
        data to be associated with this :class:`.InspectionAttr`.

        The dictionary is generated when first accessed.  Alternatively,
        it can be specified as a constructor argument to the
        :func:`.column_property`, :func:`.relationship`, or :func:`.composite`
        functions.

        .. versionadded:: 0.8  Added support for .info to all
           :class:`.MapperProperty` subclasses.

        .. versionchanged:: 1.0.0 :attr:`.InspectionAttr.info` moved
           from :class:`.MapperProperty` so that it can apply to a wider
           variety of ORM and extension constructs.

        .. seealso::

            :attr:`.QueryableAttribute.info`

            :attr:`.SchemaItem.info`

.. autodata:: sqlalchemy.orm.interfaces.NOT_EXTENSION


.. autodata:: sqlalchemy.orm.interfaces.ONETOMANY

.. autoclass:: sqlalchemy.orm.interfaces.PropComparator
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.properties.RelationshipProperty
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.descriptor_props.SynonymProperty
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.query.QueryContext
    :members:


.. autoclass:: sqlalchemy.orm.attributes.QueryableAttribute
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.session.UOWTransaction
    :members:

