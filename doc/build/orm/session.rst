.. _session_toplevel:

=================
Using the Session
=================

.. module:: sqlalchemy.orm.session

The :func:`.orm.mapper` function and :mod:`~sqlalchemy.ext.declarative` extensions
are the primary configurational interface for the ORM. Once mappings are
configured, the primary usage interface for persistence operations is the
:class:`.Session`.

.. toctree::
    :maxdepth: 2

    session_basics
    session_state_management
    cascades
    session_transaction
    persistence_techniques
    contextual
    session_events
    session_api


