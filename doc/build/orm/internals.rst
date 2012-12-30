.. _orm_internal_toplevel:

ORM Internals
=============

Key ORM constructs, not otherwise covered in other
sections, are listed here.

.. currentmodule: sqlalchemy.orm

.. autoclass:: sqlalchemy.orm.state.AttributeState
    :members:
    :show-inheritance:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.instrumentation.ClassManager
    :members:
    :show-inheritance:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.properties.ColumnProperty
    :members:
    :show-inheritance:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.descriptor_props.CompositeProperty
    :members:
    :show-inheritance:

.. autoclass:: sqlalchemy.orm.interfaces._InspectionAttr
    :members:
    :show-inheritance:

.. autoclass:: sqlalchemy.orm.state.InstanceState
    :members:
    :show-inheritance:

.. autoclass:: sqlalchemy.orm.attributes.InstrumentedAttribute
    :members: __get__, __set__, __delete__
    :show-inheritance:
    :undoc-members:

.. autoclass:: sqlalchemy.orm.interfaces.MapperProperty
    :members:
    :show-inheritance:

.. autodata:: sqlalchemy.orm.interfaces.NOT_EXTENSION

.. autoclass:: sqlalchemy.orm.interfaces.PropComparator
    :members:
    :show-inheritance:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.properties.RelationshipProperty
    :members:
    :show-inheritance:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.descriptor_props.SynonymProperty
    :members:
    :show-inheritance:
    :inherited-members:

.. autoclass:: sqlalchemy.orm.query.QueryContext
    :members:
    :show-inheritance:

.. autoclass:: sqlalchemy.orm.attributes.QueryableAttribute
    :members:
    :show-inheritance:
    :inherited-members:
