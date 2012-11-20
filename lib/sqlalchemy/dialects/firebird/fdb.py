# firebird/fdb.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
.. dialect:: firebird+fdb
    :name: fdb
    :dbapi: pyodbc
    :connectstring: firebird+fdb://user:password@host:port/path/to/db[?key=value&key=value...]
    :url: http://pypi.python.org/pypi/fdb/

    fdb is a kinterbasdb compatible DBAPI for Firebird.

    .. versionadded:: 0.8 - Support for the fdb Firebird driver.

Status
------

The fdb dialect is new and not yet tested (can't get fdb to build).


"""

from .kinterbasdb import FBDialect_kinterbasdb
from ... import util


class FBDialect_fdb(FBDialect_kinterbasdb):

    @classmethod
    def dbapi(cls):
        return  __import__('fdb')

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if opts.get('port'):
            opts['host'] = "%s/%s" % (opts['host'], opts['port'])
            del opts['port']
        opts.update(url.query)

        util.coerce_kw_type(opts, 'type_conv', int)

        return ([], opts)

    def _get_server_version_info(self, connection):
        """Get the version of the Firebird server used by a connection.

        Returns a tuple of (`major`, `minor`, `build`), three integers
        representing the version of the attached server.
        """

        # This is the simpler approach (the other uses the services api),
        # that for backward compatibility reasons returns a string like
        #   LI-V6.3.3.12981 Firebird 2.0
        # where the first version is a fake one resembling the old
        # Interbase signature.

        isc_info_firebird_version = 103
        fbconn = connection.connection

        version = fbconn.db_info(isc_info_firebird_version)

        return self._parse_version_info(version)

dialect = FBDialect_fdb
