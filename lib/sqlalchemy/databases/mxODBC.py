# mxODBC.py
# Copyright (C) 2007 Fisch Asset Management AG http://www.fam.ch
# Coding: Alexander Houben alexander.houben@thor-solutions.ch
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
A wrapper for a mx.ODBC.Windows DB-API connection.

Makes sure the mx module is configured to return datetime objects instead
of mx.DateTime.DateTime objects.
"""

from mx.ODBC.Windows import *


class Cursor:
    def __init__(self, cursor):
        self.cursor = cursor

    def __getattr__(self, attr):
        res = getattr(self.cursor, attr)
        return res

    def execute(self, *args, **kwargs):
        res = self.cursor.execute(*args, **kwargs)
        return res


class Connection:
    def myErrorHandler(self, connection, cursor, errorclass, errorvalue):
        err0, err1, err2, err3 = errorvalue
        #print ", ".join(["Err%d: %s"%(x, errorvalue[x]) for x in range(4)])
        if int(err1) == 109:
            # Ignore "Null value eliminated in aggregate function", this is not an error
            return
        raise errorclass, errorvalue

    def __init__(self, conn):
        self.conn = conn
        # install a mx ODBC error handler
        self.conn.errorhandler = self.myErrorHandler

    def __getattr__(self, attr):
        res = getattr(self.conn, attr)
        return res

    def cursor(self, *args, **kwargs):
        res = Cursor(self.conn.cursor(*args, **kwargs))
        return res


# override 'connect' call
def connect(*args, **kwargs):
    import mx.ODBC.Windows
    conn = mx.ODBC.Windows.Connect(*args, **kwargs)
    conn.datetimeformat = mx.ODBC.Windows.PYDATETIME_DATETIMEFORMAT
    return Connection(conn)
Connect = connect
