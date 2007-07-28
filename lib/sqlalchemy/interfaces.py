# interfaces.py
# Copyright (C) 2007 Jason Kirtland jek@discorporate.us
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


class PoolListener(object):
    """Hooks into the lifecycle of connections in a ``Pool``.

    """

    def connect(dbapi_con, con_record):
        """Called once for each new DBAPI connection or pool's ``creator()``.

        dbapi_con:
          A newly connected raw DBAPI connection (not a SQLAlchemy
          ``Connection`` wrapper).

        con_record:
          The ``_ConnectionRecord`` that currently owns the connection
        """

    def checkout(dbapi_con, con_record):
        """Called when a connection is retrieved from the pool.

        dbapi_con:
          A raw DBAPI connection

        con_record:
          The ``_ConnectionRecord`` that currently owns the connection

        If you raise an ``exceptions.DisconnectionError``, the current
        connection will be disposed and a fresh connection retrieved.
        Processing of all checkout listeners will abort and restart
        using the new connection.
        """

    def checkin(dbapi_con, con_record):
        """Called when a connection returns to the pool.

        Note that the connection may be closed, and may be None if the
        connection has been invalidated.  ``checkin`` will not be called
        for detached connections.  (They do not return to the pool.)

        dbapi_con:
          A raw DBAPI connection

        con_record:
          The _ConnectionRecord that currently owns the connection
        """
