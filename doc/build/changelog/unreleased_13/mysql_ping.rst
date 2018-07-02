.. change::
    :tags: feature, mysql

    The "pre-ping" feature of the connection pool now uses
    the ``ping()`` method of the DBAPI connection in the case of
    mysqlclient, PyMySQL and mysql-connector-python.  Pull request
    courtesy Maxim Bublis.

    .. seealso::

        :ref:`change_mysql_ping`
