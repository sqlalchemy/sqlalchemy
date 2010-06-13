.. _inspector_api_toplevel:

Schema Introspection
====================

.. module:: sqlalchemy.engine.reflection

SQLAlchemy provides rich schema introspection capabilities.    The most common methods for this include the "autoload" argument of :class:`~sqlalchemy.schema.Table`::

    from sqlalchemy import create_engine, MetaData, Table
    engine = create_engine('...')
    meta = MetaData()
    user_table = Table('user', meta, autoload=True, autoload_with=engine)
    
As well as the :meth:`~sqlalchemy.schema.MetaData.reflect` method of :class:`~sqlalchemy.schema.MetaData`::

    from sqlalchemy import create_engine, MetaData, Table
    engine = create_engine('...')
    meta = MetaData()
    meta.reflect(engine)
    user_table = meta.tables['user']

Further examples of reflection using :class:`~sqlalchemy.schema.Table` and :class:`~sqlalchemy.schema.MetaData` can be found at :ref:`metadata_reflection`.

There is also a low-level inspection interface available for more specific operations, known as the :class:`Inspector`::

    from sqlalchemy import create_engine
    from sqlalchemy.engine import reflection
    engine = create_engine('...')
    insp = reflection.Inspector.from_engine(engine)
    print insp.get_table_names()

.. autoclass:: Inspector
    :members:
    :undoc-members:
    :show-inheritance:
