.. _metadata_temporal:

.. currentmodule:: sqlalchemy.schema

=========================================================
Tempral Constructions (System and Application versioning)
=========================================================

This section will discuss temporal features added in SQL:2011, which include
DDL constructs :term:`PERIOD FOR`, :term:`PERIOD FOR SYSTEM_TIME`,
:term:`WITH SYSTEM VERSIONING`, and `:term:`WITHOUT OVERLAPS`. In SQLAlchemy
these constructs can be represented using :class:`_schema.Period`,
:class:`ApplicationTimePeriod` and :class:`.SystemTimePeriod`.

Temporal Construction Context
-----------------------------


Working with Application-Time Periods
-------------------------------------

Using Application-Time Periods in Primary Keys
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Working with System-Time Periods
--------------------------------

Backend-Specific Application Versioning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Currently, the only 


Temporal API
------------
.. autoclass:: Period
    :members:
    :inherited-members:

.. autoclass:: ApplicationTimePeriod
    :members:
    :inherited-members:

.. autoclass:: SystemTimePeriod
    :members:
    :inherited-members:
