# sqlite/pysqlcipher.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
.. dialect:: sqlite+pysqlcipher
    :name: pysqlcipher
    :dbapi: pysqlcipher
    :connectstring: sqlite+pysqlcipher://:passphrase/file_path[?kdf_iter=<iter>]
    :url: https://pypi.python.org/pypi/pysqlcipher

    ``pysqlcipher`` is a fork of the standard ``pysqlite`` driver to make
    use of the `SQLCipher <https://www.zetetic.net/sqlcipher>`_ backend.

    ``pysqlcipher3`` is a fork of ``pysqlcipher`` for Python 3. This dialect
    will attempt to import it if ``pysqlcipher`` is non-present.

    .. versionadded:: 1.1.4 - added fallback import for pysqlcipher3

    .. versionadded:: 0.9.9 - added pysqlcipher dialect

Driver
------

The driver here is the
`pysqlcipher <https://pypi.python.org/pypi/pysqlcipher>`_
driver, which makes use of the SQLCipher engine.  This system essentially
introduces new PRAGMA commands to SQLite which allows the setting of a
passphrase and other encryption parameters, allowing the database
file to be encrypted.

`pysqlcipher3` is a fork of `pysqlcipher` with support for Python 3,
the driver is the same.

Connect Strings
---------------

The format of the connect string is in every way the same as that
of the :mod:`~sqlalchemy.dialects.sqlite.pysqlite` driver, except that the
"password" field is now accepted, which should contain a passphrase::

    e = create_engine('sqlite+pysqlcipher://:testing@/foo.db')

For an absolute file path, two leading slashes should be used for the
database name::

    e = create_engine('sqlite+pysqlcipher://:testing@//path/to/foo.db')

Additional encryption-related pragmas must be executed manually,
using the ``first_connect`` pool event. A selection of the pragmas supported
by SQLCipher is documented at
https://www.zetetic.net/sqlcipher/sqlcipher-api/.

.. warning:: Previously the documentation wrongly stated that these
   pragma could be passed in the url string. This has never worked
   for the 1.3 series of sqlalchemy. The 1.4 series adds proper
   support for them when passed in the url string.


Pooling Behavior
----------------

The driver makes a change to the default pool behavior of pysqlite
as described in :ref:`pysqlite_threading_pooling`.   The pysqlcipher driver
has been observed to be significantly slower on connection than the
pysqlite driver, most likely due to the encryption overhead, so the
dialect here defaults to using the :class:`.SingletonThreadPool`
implementation,
instead of the :class:`.NullPool` pool used by pysqlite.  As always, the pool
implementation is entirely configurable using the
:paramref:`_sa.create_engine.poolclass` parameter; the :class:`.StaticPool`
may
be more feasible for single-threaded use, or :class:`.NullPool` may be used
to prevent unencrypted connections from being held open for long periods of
time, at the expense of slower startup time for new connections.


"""  # noqa

from __future__ import absolute_import

from .pysqlite import SQLiteDialect_pysqlite
from ... import pool
from ...engine import url as _url


class SQLiteDialect_pysqlcipher(SQLiteDialect_pysqlite):
    driver = "pysqlcipher"

    @classmethod
    def dbapi(cls):
        try:
            from pysqlcipher import dbapi2 as sqlcipher
        except ImportError as e:
            try:
                from pysqlcipher3 import dbapi2 as sqlcipher
            except ImportError:
                raise e
        return sqlcipher

    @classmethod
    def get_pool_class(cls, url):
        return pool.SingletonThreadPool

    def connect(self, *cargs, **cparams):
        passphrase = cparams.pop("passphrase", "")

        conn = super(SQLiteDialect_pysqlcipher, self).connect(
            *cargs, **cparams
        )
        conn.execute('pragma key="%s"' % passphrase)

        return conn

    def create_connect_args(self, url):
        super_url = _url.URL(
            url.drivername,
            username=url.username,
            host=url.host,
            database=url.database,
            query=url.query,
        )
        c_args, opts = super(
            SQLiteDialect_pysqlcipher, self
        ).create_connect_args(super_url)
        opts["passphrase"] = url.password
        return c_args, opts


dialect = SQLiteDialect_pysqlcipher
