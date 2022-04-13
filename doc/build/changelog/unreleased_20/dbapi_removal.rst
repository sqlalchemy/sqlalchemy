.. change::
    :tags: mssql, removed
    :tickets: 7258

    Removed support for the mxodbc driver due to lack of testing support. ODBC
    users may use the pyodbc dialect which is fully supported.

.. change::
    :tags: mysql, removed
    :tickets: 7258

    Removed support for the OurSQL driver for MySQL and MariaDB, as this
    driver does not seem to be maintained.

.. change::
    :tags: postgresql, removed
    :tickets: 7258

    Removed support for multiple deprecated drivers::

        - pypostgresql for PostgreSQL. This is available as an
          external driver at https://github.com/PyGreSQL
        - pygresql for PostgreSQL.

    Please switch to one of the supported drivers or to the external
    version of the same driver.
