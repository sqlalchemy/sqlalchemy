.. _declarative_toplevel:

.. currentmodule:: sqlalchemy.ext.declarative

======================
Declarative Extensions
======================

Extensions specific to the :ref:`Declarative <orm_declarative_mapping>`
mapping API.

.. versionchanged:: 1.4  The vast majority of the Declarative extension is now
   integrated into the SQLAlchemy ORM and is importable from the
   ``sqlalchemy.orm`` namespace.  See the documentation at
   :ref:`orm_declarative_mapping` for new documentation.
   For an overview of the change, see :ref:`change_5508`.

.. autoclass:: AbstractConcreteBase

.. autoclass:: ConcreteBase

.. autoclass:: DeferredReflection
   :members:

.. these pages have all been integrated into the main ORM documentation
   however are still here as placeholder docs with links to where they've moved

.. toctree::
   :hidden:

   api
   basic_use
   inheritance
   mixins
   relationships
   table_config