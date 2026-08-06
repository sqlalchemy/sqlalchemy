.. change::
    :tags: feature, engine

    Added :attr:`.Dialect.dbapi_version`, a standardized accessor for the
    version of the DBAPI module in use by a dialect, in contrast to
    :attr:`.Dialect.server_version_info` which refers to the database server.
    The implementation on :class:`.DefaultDialect` makes use of a new
    per-dialect method :meth:`.Dialect.retrieve_dbapi_version` in order to
    retrieve the version from the DBAPI module and return it as a
    :class:`.VersionInfo` object, which is a tuple subclass with additional
    properties; third-party dialects should also implement the
    :meth:`.Dialect.retrieve_dbapi_version` method.

.. change::
    :tags: bug, postgresql

    Fixed issue in the asyncpg dialect where the version of the ``asyncpg``
    DBAPI would always be reported as ``(99, 99, 99)``, as the version was
    looked up on the dialect's DBAPI wrapper module rather than on the
    ``asyncpg`` module itself.

.. change::
    :tags: bug, mysql

    Fixed issue where the version of the DBAPI reported by the mysqldb and
    pymysql dialects was incorrect.  Current mysqlclient releases publish
    ``MySQLdb.version_info`` and no version string at all, so no version was
    reported; pymysql publishes ``__version__`` and ``version_info`` as
    mysqlclient compatibility values, so the version reported for pymysql
    was that of the mysqlclient release it emulates, e.g. ``(2, 2, 8)``
    rather than ``(1, 2, 0)``.

.. change::
    :tags: bug, testing

    The version specifications used by testing exclusions such as
    ``testing.fails_if("+asyncmy<0.2.13")`` now support a driver name, in
    which case the comparison is against the version of the DBAPI rather
    than that of the database server.  Previously this form raised
    ``AssertionError: DBAPI version specs not supported yet``.
