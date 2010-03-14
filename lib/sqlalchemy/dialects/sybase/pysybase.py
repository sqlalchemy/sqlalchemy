# pysybase.py
# Copyright (C) 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Support for Sybase via the python-sybase driver.

http://python-sybase.sourceforge.net/

Connect strings are of the form::

    sybase+pysybase://<username>:<password>@<dsn>/[database name]

"""

from sqlalchemy.dialects.sybase.base import SybaseDialect, \
                                        SybaseExecutionContext, SybaseSQLCompiler


class SybaseExecutionContext_pysybase(SybaseExecutionContext):
    def pre_exec(self):
        for param in self.parameters:
            for key in list(param):
                param["@" + key] = param[key]
                del param[key]

        if self.isddl:
            # TODO: to enhance this, we can detect "ddl in tran" on the
            # database settings.  this error message should be improved to 
            # include a note about that.
            if not self.should_autocommit:
                raise exc.InvalidRequestError("The Sybase dialect only supports "
                                            "DDL in 'autocommit' mode at this time.")
            # call commit() on the Sybase connection directly,
            # to avoid any side effects of calling a Connection 
            # transactional method inside of pre_exec()
            self.root_connection.engine.logger.info("COMMIT (Assuming no Sybase 'ddl in tran')")
            self.root_connection.connection.commit()

class SybaseSQLCompiler_pysybase(SybaseSQLCompiler):
    def bindparam_string(self, name):
        return "@" + name
   
class SybaseDialect_pysybase(SybaseDialect):
    driver = 'pysybase'
    execution_ctx_cls = SybaseExecutionContext_pysybase
    statement_compiler = SybaseSQLCompiler_pysybase

    @classmethod
    def dbapi(cls):
        import Sybase
        return Sybase

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user', password='passwd')

        return ([opts.pop('host')], opts)

    def _get_server_version_info(self, connection):
       return connection.scalar("select @@version_number")

    def is_disconnect(self, e):
        if isinstance(e, (self.dbapi.OperationalError, self.dbapi.ProgrammingError)):
            msg = str(e)
            return ('Unable to complete network request to host' in msg or
                    'Invalid connection state' in msg or
                    'Invalid cursor state' in msg)
        else:
            return False

dialect = SybaseDialect_pysybase
