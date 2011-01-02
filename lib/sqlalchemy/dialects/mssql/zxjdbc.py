# mssql/zxjdbc.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for the Microsoft SQL Server database via the zxjdbc JDBC
connector.

JDBC Driver
-----------

Requires the jTDS driver, available from: http://jtds.sourceforge.net/

Connecting
----------

URLs are of the standard form of
``mssql+zxjdbc://user:pass@host:port/dbname[?key=value&key=value...]``.

Additional arguments which may be specified either as query string
arguments on the URL, or as keyword arguments to
:func:`~sqlalchemy.create_engine()` will be passed as Connection
properties to the underlying JDBC driver.

"""
from sqlalchemy.connectors.zxJDBC import ZxJDBCConnector
from sqlalchemy.dialects.mssql.base import MSDialect, MSExecutionContext
from sqlalchemy.engine import base

class MSExecutionContext_zxjdbc(MSExecutionContext):

    _embedded_scope_identity = False

    def pre_exec(self):
        super(MSExecutionContext_zxjdbc, self).pre_exec()
        # scope_identity after the fact returns null in jTDS so we must
        # embed it
        if self._select_lastrowid and self.dialect.use_scope_identity:
            self._embedded_scope_identity = True
            self.statement += "; SELECT scope_identity()"

    def post_exec(self):
        if self._embedded_scope_identity:
            while True:
                try:
                    row = self.cursor.fetchall()[0]
                    break
                except self.dialect.dbapi.Error, e:
                    self.cursor.nextset()
            self._lastrowid = int(row[0])

        if (self.isinsert or self.isupdate or self.isdelete) and \
            self.compiled.returning:
            self._result_proxy = base.FullyBufferedResultProxy(self)

        if self._enable_identity_insert:
            table = self.dialect.identifier_preparer.format_table(
                                        self.compiled.statement.table)
            self.cursor.execute("SET IDENTITY_INSERT %s OFF" % table)


class MSDialect_zxjdbc(ZxJDBCConnector, MSDialect):
    jdbc_db_name = 'jtds:sqlserver'
    jdbc_driver_name = 'net.sourceforge.jtds.jdbc.Driver'

    execution_ctx_cls = MSExecutionContext_zxjdbc

    def _get_server_version_info(self, connection):
        return tuple(
                    int(x) 
                    for x in connection.connection.dbversion.split('.')
                )

dialect = MSDialect_zxjdbc
