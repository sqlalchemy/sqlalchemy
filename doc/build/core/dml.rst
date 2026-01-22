Insert, Updates, Deletes
========================

INSERT, UPDATE and DELETE statements build on a hierarchy starting
with :class:`.UpdateBase`.   The :class:`_expression.Insert` and :class:`_expression.Update`
constructs build on the intermediary :class:`.ValuesBase`.

.. currentmodule:: sqlalchemy.sql.expression

.. _dml_foundational_consructors:

DML Foundational Constructors
--------------------------------------

Top level "INSERT", "UPDATE", "DELETE" constructors.

.. autofunction:: delete

.. autofunction:: insert

.. autofunction:: update


DML Class Documentation Constructors
--------------------------------------

Class documentation for the constructors listed at
:ref:`dml_foundational_consructors`.

.. autoclass:: Delete
   :members:

   .. automethod:: Delete.where

   .. automethod:: Delete.with_dialect_options

   .. automethod:: Delete.returning

   .. automethod:: Delete.ext

   .. automethod:: Delete.apply_syntax_extension_point

.. autoclass:: Insert
   :members:

   .. automethod:: Insert.with_dialect_options

   .. automethod:: Insert.values

   .. automethod:: Insert.returning

   .. automethod:: Insert.ext

   .. automethod:: Insert.apply_syntax_extension_point

.. autoclass:: Update
   :members:

   .. automethod:: Update.returning

   .. automethod:: Update.where

   .. automethod:: Update.with_dialect_options

   .. automethod:: Update.values

   .. automethod:: Update.ext

   .. automethod:: Update.apply_syntax_extension_point

.. autoclass:: sqlalchemy.sql.expression.UpdateBase
   :members:

.. autoclass:: sqlalchemy.sql.expression.ValuesBase
   :members:



