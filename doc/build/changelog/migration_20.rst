=============================
What's New in SQLAlchemy 2.0
=============================

.. admonition:: About this Document

    This document describes SQLAlchemy 2.0, which represents a major reworking
    of API and usage paradigms from the 1.x series.   Moving from a 1.x
    SQLAlchemy application to 2.0 requires migration to new 2.0-style paradigms,
    most of which are available in the 1.x series for future compatibility.


Deprecations
============

Engine / pool threadlocal flags, engine strategies removed
----------------------------------------------------------

The system of "engine strategies" has been removed.   Extensibility for
the :func:`.create_engine` process has been available since the 1.x
series using the :class:`.CreateEnginePlugin` extension.   The
"threadlocal" strategy has been removed and the "mock" strategy is replaced
with a direct function :func:`.create_mock_engine`.

Additionally, the ``use_threadlocal`` option of connection pools is also
removed.

This removes:

* ``pool_use_threadlocal``
* ``strategy="threadlocal"`` in :func:`.create_engine`
* ``strategy="mock"`` in :func:`.create_engine` is replaced by
  :func:`.create_mock_engine`
* :meth:`.Engine.contextual_connect` - use :meth:`.Engine.connect`
* :meth:`.Pool.unique_connection` - use :meth:`.Pool.connect`


