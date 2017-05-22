.. automodule:: sqlalchemy.ext.declarative

===============
Declarative API
===============

API Reference
=============

.. autofunction:: declarative_base

.. autofunction:: as_declarative

.. autoclass:: declared_attr
    :members:

.. autofunction:: sqlalchemy.ext.declarative.api._declarative_constructor

.. autofunction:: has_inherited_table

.. autofunction:: synonym_for

.. autofunction:: comparable_using

.. autofunction:: instrument_declarative

.. autoclass:: AbstractConcreteBase

.. autoclass:: ConcreteBase

.. autoclass:: DeferredReflection
   :members:


Special Directives
------------------

``__declare_last__()``
~~~~~~~~~~~~~~~~~~~~~~

The ``__declare_last__()`` hook allows definition of
a class level function that is automatically called by the
:meth:`.MapperEvents.after_configured` event, which occurs after mappings are
assumed to be completed and the 'configure' step has finished::

    class MyClass(Base):
        @classmethod
        def __declare_last__(cls):
            ""
            # do something with mappings

.. versionadded:: 0.7.3

``__declare_first__()``
~~~~~~~~~~~~~~~~~~~~~~~

Like ``__declare_last__()``, but is called at the beginning of mapper
configuration via the :meth:`.MapperEvents.before_configured` event::

    class MyClass(Base):
        @classmethod
        def __declare_first__(cls):
            ""
            # do something before mappings are configured

.. versionadded:: 0.9.3

.. _declarative_abstract:

``__abstract__``
~~~~~~~~~~~~~~~~

``__abstract__`` causes declarative to skip the production
of a table or mapper for the class entirely.  A class can be added within a
hierarchy in the same way as mixin (see :ref:`declarative_mixins`), allowing
subclasses to extend just from the special class::

    class SomeAbstractBase(Base):
        __abstract__ = True

        def some_helpful_method(self):
            ""

        @declared_attr
        def __mapper_args__(cls):
            return {"helpful mapper arguments":True}

    class MyMappedClass(SomeAbstractBase):
        ""

One possible use of ``__abstract__`` is to use a distinct
:class:`.MetaData` for different bases::

    Base = declarative_base()

    class DefaultBase(Base):
        __abstract__ = True
        metadata = MetaData()

    class OtherBase(Base):
        __abstract__ = True
        metadata = MetaData()

Above, classes which inherit from ``DefaultBase`` will use one
:class:`.MetaData` as the registry of tables, and those which inherit from
``OtherBase`` will use a different one. The tables themselves can then be
created perhaps within distinct databases::

    DefaultBase.metadata.create_all(some_engine)
    OtherBase.metadata_create_all(some_other_engine)

.. versionadded:: 0.7.3


