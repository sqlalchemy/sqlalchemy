

==============
1.1 Changelog
==============

.. changelog_imports::

    .. include:: changelog_10.rst
        :start-line: 5

    .. include:: changelog_09.rst
        :start-line: 5

    .. include:: changelog_08.rst
        :start-line: 5

    .. include:: changelog_07.rst
        :start-line: 5

.. changelog::
    :version: 1.1.0b1

    .. change::
        :tags: bug, sql
        :tickets: 2528

        The behavior of the :func:`.union` construct and related constructs
        such as :meth:`.Query.union` now handle the case where the embedded
        SELECT statements need to be parenthesized due to the fact that they
        include LIMIT, OFFSET and/or ORDER BY.   These queries **do not work
        on SQLite**, and will fail on that backend as they did before, but
        should now work on all other backends.

        .. seealso::

            :ref:`change_2528`

    .. change::
        :tags: bug, mssql
        :tickets: 3504

        Fixed issue where the SQL Server dialect would reflect a string-
        or other variable-length column type with unbounded length
        by assigning the token ``"max"`` to the
        length attribute of the string.   While using the ``"max"`` token
        explicitly is supported by the SQL Server dialect, it isn't part
        of the normal contract of the base string types, and instead the
        length should just be left as None.   The dialect now assigns the
        length to None on reflection of the type so that the type behaves
        normally in other contexts.

        .. seealso::

            :ref:`change_3504`