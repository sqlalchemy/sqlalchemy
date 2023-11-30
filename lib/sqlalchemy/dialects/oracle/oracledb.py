# dialects/oracle/oracledb.py
# Copyright (C) 2005-2023 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors

r"""
.. dialect:: oracle+oracledb
    :name: python-oracledb
    :dbapi: oracledb
    :connectstring: oracle+oracledb://user:pass@hostname:port[/dbname][?service_name=<service>[&key=value&key=value...]]
    :url: https://oracle.github.io/python-oracledb/

python-oracledb is released by Oracle to supersede the cx_Oracle driver.
It is fully compatible with cx_Oracle and features both a "thin" client
mode that requires no dependencies, as well as a "thick" mode that uses
the Oracle Client Interface in the same way as cx_Oracle.

.. seealso::

    :ref:`cx_oracle` - all of cx_Oracle's notes apply to the oracledb driver
    as well.

Thick mode support
------------------

By default the ``python-oracledb`` is started in thin mode, that does not
require oracle client libraries to be installed in the system. The
``python-oracledb`` driver also support a "thick" mode, that behaves
similarly to ``cx_oracle`` and requires that Oracle Client Interface (OCI)
is installed.

To enable this mode, the user may call ``oracledb.init_oracle_client``
manually, or by passing the parameter ``thick_mode=True`` to
:func:`_sa.create_engine`. To pass custom arguments to ``init_oracle_client``,
like the ``lib_dir`` path, a dict may be passed to this parameter, as in::

    engine = sa.create_engine("oracle+oracledb://...", thick_mode={
        "lib_dir": "/path/to/oracle/client/lib", "driver_name": "my-app"
    })

.. seealso::

    https://python-oracledb.readthedocs.io/en/latest/api_manual/module.html#oracledb.init_oracle_client


.. versionadded:: 2.0.0 added support for oracledb driver.

"""  # noqa
import re

from .cx_oracle import OracleDialect_cx_oracle as _OracleDialect_cx_oracle
from ... import exc


class OracleDialect_oracledb(_OracleDialect_cx_oracle):
    supports_statement_cache = True
    driver = "oracledb"

    def __init__(
        self,
        auto_convert_lobs=True,
        coerce_to_decimal=True,
        arraysize=50,
        encoding_errors=None,
        thick_mode=None,
        **kwargs,
    ):
        super().__init__(
            auto_convert_lobs,
            coerce_to_decimal,
            arraysize,
            encoding_errors,
            **kwargs,
        )

        if self.dbapi is not None and (
            thick_mode or isinstance(thick_mode, dict)
        ):
            kw = thick_mode if isinstance(thick_mode, dict) else {}
            self.dbapi.init_oracle_client(**kw)

    @classmethod
    def import_dbapi(cls):
        import oracledb

        return oracledb

    @classmethod
    def is_thin_mode(cls, connection):
        return connection.connection.dbapi_connection.thin

    def _load_version(self, dbapi_module):
        version = (0, 0, 0)
        if dbapi_module is not None:
            m = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?", dbapi_module.version)
            if m:
                version = tuple(
                    int(x) for x in m.group(1, 2, 3) if x is not None
                )
        self.oracledb_ver = version
        if self.oracledb_ver < (1,) and self.oracledb_ver > (0, 0, 0):
            raise exc.InvalidRequestError(
                "oracledb version 1 and above are supported"
            )


dialect = OracleDialect_oracledb
