Insert, Updates, Deletes
========================

INSERT, UPDATE and DELETE statements build on a hierarchy starting
with :class:`.UpdateBase`.   The :class:`.Insert` and :class:`.Update`
constructs build on the intermediary :class:`.ValuesBase`.

.. module:: sqlalchemy.sql.expression

.. autofunction:: delete

.. autofunction:: insert

.. autofunction:: update


.. autoclass:: Delete
   :members:
   :inherited-members:

.. autoclass:: Insert
   :members:
   :inherited-members:

.. autoclass:: Update
  :members:
  :inherited-members:

.. autoclass:: sqlalchemy.sql.expression.UpdateBase
  :members:
  :inherited-members:

.. autoclass:: sqlalchemy.sql.expression.ValuesBase
   :members:



