Connections
===========

Creating Engines
----------------

.. autofunction:: sqlalchemy.create_engine

.. autofunction:: sqlalchemy.engine_from_config

.. autoclass:: sqlalchemy.engine.url.URL
    :members:
    
Connectables
------------

.. autoclass:: sqlalchemy.engine.base.Engine
   :members:

.. autoclass:: sqlalchemy.engine.base.Connection
   :members:

.. autoclass:: sqlalchemy.engine.base.Connectable
   :members:

Result Objects
--------------

.. autoclass:: sqlalchemy.engine.base.ResultProxy
    :members:
    
.. autoclass:: sqlalchemy.engine.base.RowProxy
    :members:

Transactions
------------

.. autoclass:: sqlalchemy.engine.base.Transaction
    :members:
    :undoc-members:
    
Internals
---------

.. autofunction:: sqlalchemy.engine.base.connection_memoize

.. autoclass:: sqlalchemy.engine.base.Dialect
    :members:
    
.. autoclass:: sqlalchemy.engine.default.DefaultDialect
    :members:
    :show-inheritance:

.. autoclass:: sqlalchemy.engine.default.DefaultExecutionContext
    :members:
    :show-inheritance:

.. autoclass:: sqlalchemy.engine.base.DefaultRunner
    :members:
    :show-inheritance:
    
.. autoclass:: sqlalchemy.engine.base.ExecutionContext
    :members:

.. autoclass:: sqlalchemy.engine.base.SchemaIterator
    :members:
    :show-inheritance:
    