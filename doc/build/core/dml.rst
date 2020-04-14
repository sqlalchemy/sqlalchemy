Insert, Updates, Deletes
========================

INSERT, UPDATE and DELETE statements build on a hierarchy starting
with :class:`.UpdateBase`.   The :class:`_expression.Insert` and :class:`_expression.Update`
constructs build on the intermediary :class:`.ValuesBase`.

.. currentmodule:: sqlalchemy.sql.expression

.. autofunction:: delete

.. autofunction:: insert

.. autofunction:: update


.. autoclass:: Delete
   :members:

   .. automethod:: Delete.returning

.. autoclass:: Insert
   :members:

   .. automethod:: Insert.values

   .. automethod:: Insert.returning

.. autoclass:: Update
   :members:

   .. automethod:: Update.returning

   .. automethod:: Update.values

.. autoclass:: sqlalchemy.sql.expression.UpdateBase
   :members:

.. autoclass:: sqlalchemy.sql.expression.ValuesBase
   :members:



