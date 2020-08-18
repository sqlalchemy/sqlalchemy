# mysql/mariadb.py
# Copyright (C) 2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

.. dialect:: mysql+mariadb
    :name: MariaDB Connector/Python
    :dbapi: mariadb
    :connectstring: mysql+mariadb://<user>:<password>@<host>[:<port>]/<dbname>
    :url: https://pypi.org/project/mariadb/

Driver Status
-------------

MariaDB Connector/Python enables python programs to access MariaDB and MySQL
databases, using an API which is compliant with the Python DB API 2.0 (PEP-249).
It is written in C and uses MariaDB Connector/C client library for client server
communication. 

Status: stable

.. mariadb: https://github.com/mariadb-corporation/mariadb-connector-python

"""
import os
import re

from .base import MySQLCompiler
from .base import MySQLDialect
from .base import MySQLExecutionContext
from .base import MySQLIdentifierPreparer
from .base import TEXT
from ... import sql
from ... import util
from distutils.version import StrictVersion

import mariadb.constants.CLIENT as CLIENT

mariadb_cpy_minimum_version="1.0.1"

class MySQLExecutionContext_mariadbconnector(MySQLExecutionContext):
    pass

class MySQLCompiler_mariadbconnector(MySQLCompiler):
    pass

class MySQLIdentifierPreparer_mariadbconnector(MySQLIdentifierPreparer):
    pass

# MariaDB binary protocol doesn't support XA yet, so we need
# to rewrite the statement. See https://jira.mariadb.org/browse/MDEV-16708

def check_unsupported_xa(statement, parameter):
    if parameter is None or parameter.__len__() != 1:
        return None
    sql = re.sub(re.compile("/\\*.*?\\*/",re.DOTALL ) ,"" ,statement)
    words= sql.split(None, 1)
    if words[0].lower() == "xa":
        if sql.find(" ?"):
            replace= "'%s'" % parameter[0]
            return sql.replace("?", replace)

class MySQLDialect_mariadbconnector(MySQLDialect):
    is_mariadb = True
    driver = "mariadbconnector"
    supports_unicode_statements = True
    encoding = "utf8mb4"
    convert_unicode = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_native_decimal = True
    default_paramstyle = "qmark"
    execution_ctx_cls = MySQLExecutionContext_mariadbconnector
    statement_compiler = MySQLCompiler_mariadbconnector
    preparer = MySQLIdentifierPreparer_mariadbconnector

    def __init__(self, server_side_cursors=False, **kwargs):
        super(MySQLDialect_mariadbconnector, self).__init__(**kwargs)
        self.server_side_cursors = True
        self.paramstyle= "qmark"
        if self.dbapi is not None:
            assert StrictVersion(self.dbapi.__version__) >= StrictVersion(mariadb_cpy_minimum_version),\
                "The installed version (%s) of MariaDB Connector/Pyton is not supported."\
                "Please install MariaDB Connector/Python %s or newer" % (self.dbapi.__version__, mariadb_cpy_minimum_version)

    @classmethod
    def dbapi(cls):
        return __import__("mariadb")

    def on_connect(self):
        super_ = super(MySQLDialect_mariadbconnector, self).on_connect()

        def on_connect(conn):
            if super_ is not None:
                super_(conn)

        return on_connect

    def is_disconnect(self, e, connection, cursor):
        if super(MySQLDialect_mariadbconnector, self).is_disconnect(
            e, connection, cursor
        ):
            return True
        elif isinstance(e, self.dbapi.Error):
            str_e = str(e).lower()
            return (
                "not connected" in str_e or "isn't valid" in str_e
            )
        else:
            return False

    def do_execute(self, cursor, statement, parameters, context=None):
        xa= check_unsupported_xa(statement, parameters)
        if xa:
            cursor.execute(xa, buffered=True)
        else:
            cursor.execute(statement, parameters, buffered=True)

    def do_executemany(self, cursor, statement, parameters, context=None):
        cursor.executemany(statement, parameters)

    def create_connect_args(self, url):
        opts = url.translate_connect_args()

        int_params = ["connect_timeout", "read_timeout", "write_timeout", "client_flag",
                     "port", "pool_size"]
        bool_params = ["local_infile", "ssl_verify_cert", "ssl", "pool_reset_connection"]

        for key in int_params:
          util.coerce_kw_type(opts, key, int)
        for key in bool_params:
          util.coerce_kw_type(opts, key, bool)

        # FOUND_ROWS must be set in CLIENT_FLAGS to enable
        # supports_sane_rowcount.
        client_flag = opts.get("client_flag", 0)
        if self.dbapi is not None:
            try:
                CLIENT_FLAGS = __import__(
                    self.dbapi.__name__ + ".constants.CLIENT"
                ).constants.CLIENT
                client_flag |= CLIENT.FOUND_ROWS
            except (AttributeError, ImportError):
                self.supports_sane_rowcount = False
            opts["client_flag"] = client_flag
        return [[], opts]

    def _extract_error_code(self, exception):
        try:
          rc= exception.errno
        except:
          rc= -1
        return rc

    def _detect_charset(self, connection):
        return "utf8mb4"

    _isolation_lookup = set(
        [
            "SERIALIZABLE",
            "READ UNCOMMITTED",
            "READ COMMITTED",
            "REPEATABLE READ",
            "AUTOCOMMIT",
        ]
    )

    def _set_isolation_level(self, connection, level):
        if level == "AUTOCOMMIT":
            connection.autocommit= True
        else:
            connection.autocommit= False
            super(MySQLDialect_mariadbconnector, self)._set_isolation_level(
                connection, level
            )


dialect = MySQLDialect_mariadbconnector
