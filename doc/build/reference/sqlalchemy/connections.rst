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

.. currentmodule:: sqlalchemy.engine.base

.. autoclass:: Engine
   :members:

.. autoclass:: Connection
   :members:

.. autoclass:: Connectable
   :members:
   :undoc-members:

Result Objects
--------------

.. autoclass:: sqlalchemy.engine.base.ResultProxy
    :members:
    
.. autoclass:: sqlalchemy.engine.base.RowProxy
    :members:

Transactions
------------

.. autoclass:: Transaction
    :members:
    :undoc-members:
    
Internals
---------

.. autofunction:: connection_memoize

.. autoclass:: Dialect
    :members:
    
.. autoclass:: sqlalchemy.engine.default.DefaultDialect
    :members:
    :show-inheritance:

.. autoclass:: sqlalchemy.engine.default.DefaultExecutionContext
    :members:
    :show-inheritance:

.. autoclass:: ExecutionContext
    :members:

