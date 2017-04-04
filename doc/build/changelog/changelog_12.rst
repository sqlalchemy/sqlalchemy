==============
1.2 Changelog
==============

.. changelog_imports::

    .. include:: changelog_11.rst
        :start-line: 5

    .. include:: changelog_10.rst
        :start-line: 5

.. changelog::
    :version: 1.2.0b1

    .. change:: 3957
        :tags: bug, sql
        :tickets: 3957

        Fixed bug where a column-level :class:`.CheckConstraint` would fail
        to compile the SQL expression using the underlying dialect compiler
        as well as apply proper flags to generate literal values as
        inline, in the case that the sqltext is a Core expression and
        not just a plain string.   This was long-ago fixed for table-level
        check constraints in 0.9 as part of :ticket:`2742`, which more commonly
        feature Core SQL expressions as opposed to plain string expressions.

    .. change:: 2626
        :tags: bug, mssql
        :tickets: 2626

        The SQL Server dialect now allows for a database and/or owner name
        with a dot inside of it, using brackets explicitly in the string around
        the owner and optionally the database name as well.  In addition,
        sending the :class:`.quoted_name` construct for the schema name will
        not split on the dot and will deliver the full string as the "owner".
        :class:`.quoted_name` is also now available from the ``sqlalchemy.sql``
        import space.

        .. seealso::

            :ref:`change_2626`

    .. change:: 3923
        :tags: bug, sql
        :tickets: 3923

        Fixed bug where a SQL-oriented Python-side column default could fail to
        be executed properly upon INSERT in the "pre-execute" codepath, if the
        SQL itself were an untyped expression, such as plain text.  The "pre-
        execute" codepath is fairly uncommon however can apply to non-integer
        primary key columns with SQL defaults when RETURNING is not used.

    .. change:: 3785
        :tags: bug, sql
        :tickets: 3785

        The expression used for COLLATE as rendered by the column-level
        :func:`.expression.collate` and :meth:`.ColumnOperators.collate` is now
        quoted as an identifier when the name is case sensitive, e.g. has
        uppercase characters.  Note that this does not impact type-level
        collation, which is already quoted.

        .. seealso::

            :ref:`change_3785`

    .. change:: 3229
        :tags: feature, orm, ext
        :tickets: 3229

        The :meth:`.Query.update` method can now accommodate both
        hybrid attributes as well as composite attributes as a source
        of the key to be placed in the SET clause.   For hybrids, an
        additional decorator :meth:`.hybrid_property.update_expression`
        is supplied for which the user supplies a tuple-returning function.

        .. seealso::

            :ref:`change_3229`

    .. change:: 3753
        :tags: bug, orm
        :tickets: 3753

        The :func:`.attributes.flag_modified` function now raises
        :class:`.InvalidRequestError` if the named attribute key is not
        present within the object, as this is assumed to be present
        in the flush process.  To mark an object "dirty" for a flush
        without referring to any specific attribute, the
        :func:`.attributes.flag_dirty` function may be used.

        .. seealso::

            :ref:`change_3753`

    .. change:: 3911_3912
        :tags: bug, ext
        :tickets: 3911, 3912

        The :class:`sqlalchemy.ext.hybrid.hybrid_property` class now supports
        calling mutators like ``@setter``, ``@expression`` etc. multiple times
        across subclasses, and now provides a ``@getter`` mutator, so that
        a particular hybrid can be repurposed across subclasses or other
        classes.  This now matches the behavior of ``@property`` in standard
        Python.

        .. seealso::

            :ref:`change_3911_3912`



    .. change:: 1546
        :tags: feature, sql, postgresql, mysql, oracle
        :tickets: 1546

        Added support for SQL comments on :class:`.Table` and :class:`.Column`
        objects, via the new :paramref:`.Table.comment` and
        :paramref:`.Column.comment` arguments.   The comments are included
        as part of DDL on table creation, either inline or via an appropriate
        ALTER statement, and are also reflected back within table reflection,
        as well as via the :class:`.Inspector`.   Supported backends currently
        include MySQL, Postgresql, and Oracle.  Many thanks to Frazer McLean
        for a large amount of effort on this.

        .. seealso::

            :ref:`change_1546`

    .. change:: 3919
        :tags: feature, engine
        :tickets: 3919

        Added native "pessimistic disconnection" handling to the :class:`.Pool`
        object.  The new parameter :paramref:`.Pool.pre_ping`, available from
        the engine as :paramref:`.create_engine.pool_pre_ping`, applies an
        efficient form of the "pre-ping" recipe featured in the pooling
        documentation, which upon each connection check out, emits a simple
        statement, typically "SELECT 1", to test the connection for liveness.
        If the existing connection is no longer able to respond to commands,
        the connection is transparently recycled, and all other connections
        made prior to the current timestamp are invalidated.

        .. seealso::

            :ref:`pool_disconnects_pessimistic`

            :ref:`change_3919`

    .. change:: 3939
        :tags: bug, sql
        :tickets: 3939

        Fixed bug where the use of an :class:`.Alias` object in a column
        context would raise an argument error when it tried to group itself
        into a parenthesized expression.   Using :class:`.Alias` in this way
        is not yet a fully supported API, however it applies to some end-user
        recipes and may have a more prominent role in support of some
        future Postgresql features.

    .. change:: 3366
        :tags: bug, orm
        :tickets: 3366

        The "evaluate" strategy used by :meth:`.Query.update` and
        :meth:`.Query.delete` can now accommodate a simple
        object comparison from a many-to-one relationship to an instance,
        when the attribute names of the primary key / foreign key columns
        don't match the actual names of the columns.  Previously this would
        do a simple name-based match and fail with an AttributeError.

    .. change:: 3896_a
        :tags: feature, orm
        :tickets: 3896

        Added new attribute event :meth:`.AttributeEvents.bulk_replace`.
        This event is triggered when a collection is assigned to a
        relationship, before the incoming collection is compared with the
        existing one.  This early event allows for conversion of incoming
        non-ORM objects as well.  The event is integrated with the
        ``@validates`` decorator.

        .. seealso::

            :ref:`change_3896_event`

    .. change:: 3896_b
        :tags: bug, orm
        :tickets: 3896

        The ``@validates`` decorator now allows the decorated method to receive
        objects from a "bulk collection set" operation that have not yet
        been compared to the existing collection.  This allows incoming values
        to be converted to compatible ORM objects as is already allowed
        from an "append" event.   Note that this means that the
        ``@validates`` method is called for **all** values during a collection
        assignment, rather than just the ones that are new.

        .. seealso::

            :ref:`change_3896_validates`

    .. change:: 3938
        :tags: bug, engine
        :tickets: 3938

        Fixed bug where in the unusual case of passing a
        :class:`.Compiled` object directly to :meth:`.Connection.execute`,
        the dialect with which the :class:`.Compiled` object were generated
        was not consulted for the paramstyle of the string statement, instead
        assuming it would match the dialect-level paramstyle, causing
        mismatches to occur.

    .. change:: 3918
        :tags: bug, ext
        :tickets: 3918

        Fixed a bug in the ``sqlalchemy.ext.serializer`` extension whereby
        an "annotated" SQL element (as produced by the ORM for many types
        of SQL expressions) could not be reliably serialized.  Also bumped
        the default pickle level for the serializer to "HIGHEST_PROTOCOL".

    .. change:: 3891
        :tags: bug, orm
        :tickets: 3891

        Fixed bug in single-table inheritance where the select_from()
        argument would not be taken into account when limiting rows
        to a subclass.  Previously, only expressions in the
        columns requested would be taken into account.

        .. seealso::

            :ref:`change_3891`

    .. change:: 3913
        :tags: bug, orm
        :tickets: 3913

        When assigning a collection to an attribute mapped by a relationship,
        the previous collection is no longer mutated.  Previously, the old
        collection would be emptied out in conjunction with the "item remove"
        events that fire off; the events now fire off without affecting
        the old collection.

        .. seealso::

            :ref:`change_3913`

    .. change:: 3932
        :tags: bug, oracle
        :tickets: 3932

        The cx_Oracle dialect now supports "sane multi rowcount", that is,
        when a series of parameter sets are executed via DBAPI
        ``cursor.executemany()``, we can make use of ``cursor.rowcount`` to
        verify the number of rows matched.  This has an impact within the
        ORM when detecting concurrent modification scenarios, in that
        some simple conditions can now be detected even when the ORM
        is batching statements, as well as when the more strict versioning
        feature is used, the ORM can still use statement batching.  The
        flag is enabled for cx_Oracle assuming at least version 5.0, which
        is now commonplace.

    .. change:: 3907
        :tags: feature, sql
        :tickets: 3907

        The longstanding behavior of the :meth:`.ColumnOperators.in_` and
        :meth:`.ColumnOperators.notin_` operators emitting a warning when
        the right-hand condition is an empty sequence has been revised;
        a simple "static" expression of "1 != 1" or "1 = 1" is now rendered
        by default, rather than pulling in the original left-hand
        expression.  This causes the result for a NULL column comparison
        against an empty set to change from NULL to true/false.  The
        behavior is configurable, and the old behavior can be enabled
        using the :paramref:`.create_engine.empty_in_strategy` parameter
        to :func:`.create_engine`.

        .. seealso::

            :ref:`change_3907`

    .. change:: 3276
        :tags: bug, oracle
        :tickets: 3276

        Oracle reflection now "normalizes" the name given to a foreign key
        constraint, that is, returns it as all lower case for a case
        insensitive name.  This was already the behavior for indexes
        and primary key constraints as well as all table and column names.
        This will allow Alembic autogenerate scripts to compare and render
        foreign key constraint names correctly when initially specified
        as case insensitive.

        .. seealso::

            :ref:`change_3276`

    .. change:: 2694
        :tags: feature, sql
        :tickets: 2694

        Added a new option ``autoescape`` to the "startswith" and
        "endswith" classes of comparators; this supplies an escape character
        also applies it to all occurrences of the wildcard characters "%"
        and "_" automatically.  Pull request courtesy Diana Clarke.

        .. seealso::

            :ref:`change_2694`

    .. change:: 3934
        :tags: bug, orm
        :tickets: 3934

        The state of the :class:`.Session` is now present when the
        :meth:`.SessionEvents.after_rollback` event is emitted, that is,  the
        attribute state of objects prior to their being expired.   This is now
        consistent with the  behavior of the
        :meth:`.SessionEvents.after_commit` event which  also emits before the
        attribute state of objects is expired.

        .. seealso::

            :ref:`change_3934`
