

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
        :tags: bug, postgresql
        :tickets: 3499

        The "hashable" flag on special datatypes such as :class:`.postgresql.ARRAY`,
        :class:`.postgresql.JSON` and :class:`.postgresql.HSTORE` is now
        set to False, which allows these types to be fetchable in ORM
        queries that include entities within the row.

        .. seealso::

            :ref:`change_3499`

            :ref:`change_3499_postgresql`

    .. change::
        :tags: bug, postgresql
        :tickets: 3487

        The Postgresql :class:`.postgresql.ARRAY` type now supports multidimensional
        indexed access, e.g. expressions such as ``somecol[5][6]`` without
        any need for explicit casts or type coercions, provided
        that the :paramref:`.postgresql.ARRAY.dimensions` parameter is set to the
        desired number of dimensions.

        .. seealso::

            :ref:`change_3503`

    .. change::
        :tags: bug, postgresql
        :tickets: 3503

        The return type for the :class:`.postgresql.JSON` and :class:`.postgresql.JSONB`
        when using indexed access has been fixed to work like Postgresql itself,
        and returns an expression that itself is of type :class:`.postgresql.JSON`
        or :class:`.postgresql.JSONB`.  Previously, the accessor would return
        :class:`.NullType` which disallowed subsequent JSON-like operators to be
        used.

        .. seealso::

            :ref:`change_3503`

    .. change::
        :tags: bug, postgresql
        :tickets: 3503

        The :class:`.postgresql.JSON`, :class:`.postgresql.JSONB` and
        :class:`.postgresql.HSTORE` datatypes now allow full control over the
        return type from an indexed textual access operation, either ``column[someindex].astext``
        for a JSON type or ``column[someindex]`` for an HSTORE type,
        via the :paramref:`.postgresql.JSON.astext_type` and
        :paramref:`.postgresql.HSTORE.text_type` parameters.

        .. seealso::

            :ref:`change_3503`


    .. change::
        :tags: bug, postgresql
        :tickets: 3503

        The :attr:`.postgresql.JSON.Comparator.astext` modifier no longer
        calls upon :meth:`.ColumnElement.cast` implicitly, as PG's JSON/JSONB
        types allow cross-casting between each other as well.  Code that
        makes use of :meth:`.ColumnElement.cast` on JSON indexed access,
        e.g. ``col[someindex].cast(Integer)``, will need to be changed
        to call :attr:`.postgresql.JSON.Comparator.astext` explicitly.

        .. seealso::

            :ref:`change_3503_cast`


    .. change::
        :tags: bug, orm, postgresql
        :tickets: 3514

        Additional fixes have been made regarding the value of ``None``
        in conjunction with the Postgresql :class:`.JSON` type.  When
        the :paramref:`.JSON.none_as_null` flag is left at its default
        value of ``False``, the ORM will now correctly insert the Json
        "'null'" string into the column whenever the value on the ORM
        object is set to the value ``None`` or when the value ``None``
        is used with :meth:`.Session.bulk_insert_mappings`,
        **including** if the column has a default or server default on it.

        .. seealso::

            :ref:`change_3514`

    .. change::
        :tags: feature, postgresql
        :tickets: 3514

        Added a new constant :attr:`.postgresql.JSON.NULL`, indicating
        that the JSON NULL value should be used for a value
        regardless of other settings.

        .. seealso::

            :ref:`change_3514_jsonnull`

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