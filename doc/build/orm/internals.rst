.. _orm_internal_toplevel:

ORM Internals
=============

Key ORM constructs, not otherwise covered in other
sections, are listed here.

.. currentmodule: sqlalchemy.orm

.. autoclass:: sqlalchemy.orm.state.AttributeState
    :members:

.. autoclass:: sqlalchemy.orm.instrumentation.ClassManager
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.properties.ColumnProperty
    :members:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.descriptor_props.CompositeProperty
    :members:


.. autoclass:: sqlalchemy.orm.attributes.Event
    :members:


.. autoclass:: sqlalchemy.orm.interfaces._InspectionAttr
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

