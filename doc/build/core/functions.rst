.. _functions_toplevel:
.. _generic_functions:

=========================
SQL and Generic Functions
=========================

.. currentmodule:: sqlalchemy.sql.functions

SQL functions are invoked by using the :data:`_sql.func` namespace.
See the tutorial at :ref:`tutorial_functions` for background on how to
use the :data:`_sql.func` object to render SQL functions in statements.

.. seealso::

    :ref:`tutorial_functions` - in the :ref:`unified_tutorial`

Function API
------------

The base API for SQL functions, which provides for the :data:`_sql.func`
namespace as well as classes that may be used for extensibility.

.. autoclass:: AnsiFunction
   :exclude-members: inherit_cache, __new__

.. autoclass:: Function

.. autoclass:: FunctionElement
   :members:
   :exclude-members: inherit_cache, __new__

.. autoclass:: GenericFunction
   :exclude-members: inherit_cache, __new__

.. autofunction:: register_function


Selected "Known" Functions
--------------------------

These are :class:`.GenericFunction` implementations for a selected set of
common SQL functions that set up the expected return type for each function
automatically.  The are invoked in the same way as any other member of the
:data:`_sql.func` namespace::

    select(func.count("*")).select_from(some_table)

Note that any name not known to :data:`_sql.func` generates the function name
as is - there is no restriction on what SQL functions can be called, known or
unknown to SQLAlchemy, built-in or user defined. The section here only
describes those functions where SQLAlchemy already knows what argument and
return types are in use.

.. autoclass:: array_agg
    :no-members:

.. autoclass:: char_length
    :no-members:

.. autoclass:: coalesce
    :no-members:

.. autoclass:: concat
    :no-members:

.. autoclass:: count
    :no-members:

.. autoclass:: cube
    :no-members:

.. autoclass:: cume_dist
    :no-members:

.. autoclass:: current_date
    :no-members:

.. autoclass:: current_time
    :no-members:

.. autoclass:: current_timestamp
    :no-members:

.. autoclass:: current_user
    :no-members:

.. autoclass:: dense_rank
    :no-members:

.. autoclass:: grouping_sets
    :no-members:

.. autoclass:: localtime
    :no-members:

.. autoclass:: localtimestamp
    :no-members:

.. autoclass:: max
    :no-members:

.. autoclass:: min
    :no-members:

.. autoclass:: mode
    :no-members:

.. autoclass:: next_value
    :no-members:

.. autoclass:: now
    :no-members:

.. autoclass:: percent_rank
    :no-members:

.. autoclass:: percentile_cont
    :no-members:

.. autoclass:: percentile_disc
    :no-members:

.. autoclass:: random
    :no-members:

.. autoclass:: rank
    :no-members:

.. autoclass:: rollup
    :no-members:

.. autoclass:: session_user
    :no-members:

.. autoclass:: sum
    :no-members:

.. autoclass:: sysdate
    :no-members:

.. autoclass:: user
    :no-members:
