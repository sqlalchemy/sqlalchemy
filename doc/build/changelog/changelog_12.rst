=============
1.2 Changelog
=============

.. changelog_imports::

    .. include:: changelog_11.rst
        :start-line: 5

    .. include:: changelog_10.rst
        :start-line: 5

.. changelog::
    :version: 1.2.0b1
    :released: July 10, 2017

    .. change:: scoped_autocommit
        :tags: feature, orm

        Added ``.autocommit`` attribute to :class:`.scoped_session`, proxying
        the ``.autocommit`` attribute of the underling :class:`.Session`
        currently assigned to the thread.  Pull request courtesy
        Ben Fagin.

    .. change:: 4009
        :tags: feature, mysql
        :tickets: 4009

        Added support for MySQL's ON DUPLICATE KEY UPDATE
        MySQL-specific :class:`.mysql.dml.Insert` object.
        Pull request courtesy Michael Doronin.

        .. seealso::

            :ref:`change_4009`

    .. change:: 4018
        :tags: bug, sql
        :tickets: 4018

        The rules for type coercion between :class:`.Numeric`, :class:`.Integer`,
        and date-related types now include additional logic that will attempt
        to preserve the settings of the incoming type on the "resolved" type.
        Currently the target for this is the ``asdecimal`` flag, so that
        a math operation between :class:`.Numeric` or :class:`.Float` and
        :class:`.Integer` will preserve the "asdecimal" flag as well as
        if the type should be the :class:`.Float` subclass.

        .. seealso::

            :ref:`change_floats_12`

    .. change:: 4020
        :tags: bug, sql, mysql
        :tickets: 4020

        The result processor for the :class:`.Float` type now unconditionally
        runs values through the ``float()`` processor if the dialect
        specifies that it also supports "native decimal" mode.  While most
        backends will deliver Python ``float`` objects for a floating point
        datatype, the MySQL backends in some cases lack the typing information
        in order to provide this and return ``Decimal`` unless the float
        conversion is done.

        .. seealso::

            :ref:`change_floats_12`

    .. change:: 4017
        :tags: bug, sql
        :tickets: 4017

        Added some extra strictness to the handling of Python "float" values
        passed to SQL statements.  A "float" value will be associated with the
        :class:`.Float` datatype and not the Decimal-coercing :class:`.Numeric`
        datatype as was the case before, eliminating a confusing warning
        emitted on SQLite as well as unecessary coercion to Decimal.

        .. seealso::

            :ref:`change_floats_12`

    .. change:: 3058
        :tags: feature, orm
        :tickets: 3058

        Added a new feature :func:`.orm.with_expression` that allows an ad-hoc
        SQL expression to be added to a specific entity in a query at result
        time.  This is an alternative to the SQL expression being delivered as
        a separate element in the result tuple.

        .. seealso::

            :ref:`change_3058`

    .. change:: 3496
        :tags: bug, orm
        :tickets: 3496

        An UPDATE emitted as a result of the
        :paramref:`.relationship.post_update` feature will now integrate with
        the versioning feature to both bump the version id of the row as well
        as assert that the existing version number was matched.

        .. seealso::

            :ref:`change_3496`

    .. change:: 3769
        :tags: bug, ext
        :tickets: 3769

        The :meth:`.AssociationProxy.any`, :meth:`.AssociationProxy.has`
        and :meth:`.AssociationProxy.contains` comparison methods now support
        linkage to an attribute that is itself also an
        :class:`.AssociationProxy`, recursively.

        .. seealso::

            :ref:`change_3769`

    .. change:: 3853
        :tags: bug, ext
        :tickets: 3853

        Implemented in-place mutation operators ``__ior__``, ``__iand__``,
        ``__ixor__`` and ``__isub__`` for :class:`.mutable.MutableSet`
        and ``__iadd__`` for :class:`.mutable.MutableList` so that change
        events are fired off when these mutator methods are used to alter the
        collection.

        .. seealso::

            :ref:`change_3853`

    .. change:: 3847
        :tags: bug, declarative
        :tickets: 3847

        A warning is emitted if the :attr:`.declared_attr.cascading` modifier
        is used with a declarative attribute that is itself declared on
        a class that is to be mapped, as opposed to a declarative mixin
        class or ``__abstract__`` class.  The :attr:`.declared_attr.cascading`
        modifier currently only applies to mixin/abstract classes.

    .. change:: 4003
        :tags: feature, oracle
        :tickets: 4003

        The Oracle dialect now inspects unique and check constraints when using
        :meth:`.Inspector.get_unique_constraints`,
        :meth:`.Inspector.get_check_constraints`.
        As Oracle does not have unique constraints that are separate from a unique
        :class:`.Index`, a :class:`.Table` that's reflected will still continue
        to not have :class:`.UniqueConstraint` objects associated with it.
        Pull requests courtesy Eloy Felix.

        .. seealso::

            :ref:`change_4003`

    .. change:: 3948
        :tags: feature, orm
        :tickets: 3948

        Added a new style of mapper-level inheritance loading
        "polymorphic selectin".  This style of loading
        emits queries for each subclass in an inheritance
        hierarchy subsequent to the load of the base
        object type, using IN to specify the desired
        primary key values.

        .. seealso::

            :ref:`change_3948`

    .. change:: 3472
        :tags: bug, orm
        :tickets: 3471, 3472

        Repaired several use cases involving the
        :paramref:`.relationship.post_update` feature when used in conjunction
        with a column that has an "onupdate" value.   When the UPDATE emits,
        the corresponding object attribute is now expired or refreshed so that
        the newly generated "onupdate" value can populate on the object;
        previously the stale value would remain.  Additionally, if the target
        attribute is set in Python for the INSERT of the object, the value is
        now re-sent during the UPDATE so that the "onupdate" does not overwrite
        it (note this works just as well for server-generated onupdates).
        Finally, the :meth:`.SessionEvents.refresh_flush` event is now emitted
        for these attributes when refreshed within the flush.

        .. seealso::

            :ref:`change_3471`

    .. change:: 3996
        :tags: bug, orm
        :tickets: 3996

        Fixed bug where programmatic version_id counter in conjunction with
        joined table inheritance would fail if the version_id counter
        were not actually incremented and no other values on the base table
        were modified, as the UPDATE would have an empty SET clause.  Since
        programmatic version_id where version counter is not incremented
        is a documented use case, this specific condition is now detected
        and the UPDATE now sets the version_id value to itself, so that
        concurrency checks still take place.

    .. change:: 3848
        :tags: bug, orm, declarative
        :tickets: 3848

        Fixed bug where using :class:`.declared_attr` on an
        :class:`.AbstractConcreteBase` where a particular return value were some
        non-mapped symbol, including ``None``, would cause the attribute
        to hard-evaluate just once and store the value to the object
        dictionary, not allowing it to invoke for subclasses.   This behavior
        is normal when :class:`.declared_attr` is on a mapped class, and
        does not occur on a mixin or abstract class.  Since
        :class:`.AbstractConcreteBase` is both "abstract" and actually
        "mapped", a special exception case is made here so that the
        "abstract" behavior takes precedence for :class:`.declared_attr`.

    .. change:: 3673
        :tags: bug, orm
        :tickets: 3673

        The versioning feature does not support NULL for the version counter.
        An exception is now raised if the version id is programmatic and
        was set to NULL for an UPDATE.  Pull request courtesy Diana Clarke.

    .. change:: 3999
        :tags: bug, sql
        :tickets: 3999

        The operator precedence for all comparison operators such as LIKE, IS,
        IN, MATCH, equals, greater than, less than, etc. has all been merged
        into one level, so that expressions which make use of these against
        each other will produce parentheses between them.   This suits the
        stated operator precedence of databases like Oracle, MySQL and others
        which place all of these operators as equal precedence, as well as
        Postgresql as of 9.5 which has also flattened its operator precendence.

        .. seealso::

            :ref:`change_3999`


    .. change:: 3796
        :tags: bug, orm
        :tickets: 3796

        Removed a very old keyword argument from :class:`.scoped_session`
        called ``scope``.  This keyword was never documented and was an
        early attempt at allowing for variable scopes.

        .. seealso::

            :ref:`change_3796`

    .. change:: 3871
        :tags: bug, mysql
        :tickets: 3871

        Added support for views that are unreflectable due to stale
        table definitions, when calling :meth:`.MetaData.reflect`; a warning
        is emitted for the table that cannot respond to ``DESCRIBE``,
        but the operation succeeds.

    .. change:: baked_opts
        :tags: feature, ext

        Added new flag :paramref:`.Session.enable_baked_queries` to the
        :class:`.Session` to allow baked queries to be disabled
        session-wide, reducing memory use.   Also added new :class:`.Bakery`
        wrapper so that the bakery returned by :paramref:`.BakedQuery.bakery`
        can be inspected.

    .. change:: 3988
        :tags: bug, orm
        :tickets: 3988

        Fixed bug where combining a "with_polymorphic" load in conjunction
        with subclass-linked relationships that specify joinedload with
        innerjoin=True, would fail to demote those "innerjoins" to
        "outerjoins" to suit the other polymorphic classes that don't
        support that relationship.   This applies to both a single and a
        joined inheritance polymorphic load.

    .. change:: 3991
        :tags: bug, orm
        :tickets: 3991

        Added new argument :paramref:`.with_for_update` to the
        :meth:`.Session.refresh` method.  When the :meth:`.Query.with_lockmode`
        method were deprecated in favor of :meth:`.Query.with_for_update`,
        the :meth:`.Session.refresh` method was never updated to reflect
        the new option.

        .. seealso::

            :ref:`change_3991`

    .. change:: 3984
        :tags: bug, orm
        :tickets: 3984

        Fixed bug where a :func:`.column_property` that is also marked as
        "deferred" would be marked as "expired" during a flush, causing it
        to be loaded along with the unexpiry of regular attributes even
        though this attribute was never accessed.

    .. change:: 3873
        :tags: bug, sql
        :tickets: 3873

        Repaired issue where the type of an expression that used
        :meth:`.ColumnOperators.is_` or similar would not be a "boolean" type,
        instead the type would be "nulltype", as well as when using custom
        comparison operators against an untyped expression.   This typing can
        impact how the expression behaves in larger contexts as well as
        in result-row-handling.

    .. change:: 3941
        :tags: bug, ext
        :tickets: 3941

        Improved the association proxy list collection so that premature
        autoflush against a newly created association object can be prevented
        in the case where ``list.append()`` is being used, and a lazy load
        would be invoked when the association proxy accesses the endpoint
        collection.  The endpoint collection is now accessed first before
        the creator is invoked to produce the association object.

    .. change:: 3969
        :tags: bug, sql
        :tickets: 3969

        Fixed the negation of a :class:`.Label` construct so that the
        inner element is negated correctly, when the :func:`.not_` modifier
        is applied to the labeled expression.

    .. change:: 3944
        :tags: feature, orm
        :tickets: 3944

        Added a new kind of eager loading called "selectin" loading.  This
        style of loading is very similar to "subquery" eager loading,
        except that it uses an IN expression given a list of primary key
        values from the loaded parent objects, rather than re-stating the
        original query.   This produces a more efficient query that is
        "baked" (e.g. the SQL string is cached) and also works in the
        context of :meth:`.Query.yield_per`.

        .. seealso::

            :ref:`change_3944`

    .. change::
        :tags: bug, orm
        :tickets: 3967

        Fixed bug in subquery eager loading where the "join_depth" parameter
        for self-referential relationships would not be correctly honored,
        loading all available levels deep rather than correctly counting
        the specified number of levels for eager loading.

    .. change::
        :tags: bug, orm

        Added warnings to the LRU "compiled cache" used by the :class:`.Mapper`
        (and ultimately will be for other ORM-based LRU caches) such that
        when the cache starts hitting its size limits, the application will
        emit a warning that this is a performance-degrading situation that
        may require attention.   The LRU caches can reach their size limits
        primarily if an application is making use of an unbounded number
        of :class:`.Engine` objects, which is an antipattern.  Otherwise,
        this may suggest an issue that should be brought to the SQLAlchemy
        developer's attention.

    .. change:: 3964
        :tags: bug, postgresql
        :tickets: 3964

        Fixed bug where the base :class:`.sqltypes.ARRAY` datatype would not
        invoke the bind/result processors of :class:`.postgresql.ARRAY`.

    .. change:: 3963
        :tags: bug, orm
        :tickets: 3963

        Fixed bug to improve upon the specificity of loader options that
        take effect subsequent to the lazy load of a related entity, so
        that the loader options will match to an aliased or non-aliased
        entity more specifically if those options include entity information.

    .. change:: 3954
        :tags: feature, orm
        :tickets: 3954

        The ``lazy="select"`` loader strategy now makes used of the
        :class:`.BakedQuery` query caching system in all cases.  This
        removes most overhead of generating a :class:`.Query` object and
        running it into a :func:`.select` and then string SQL statement from
        the process of lazy-loading related collections and objects.  The
        "baked" lazy loader has also been improved such that it can now
        cache in most cases where query load options are used.

        .. seealso::

            :ref:`change_3954`

    .. change:: 3740
        :tags: bug, sql
        :tickets: 3740

        The system by which percent signs in SQL statements are "doubled"
        for escaping purposes has been refined.   The "doubling" of percent
        signs mostly associated with the :obj:`.literal_column` construct
        as well as operators like :meth:`.ColumnOperators.contains` now
        occurs based on the stated paramstyle of the DBAPI in use; for
        percent-sensitive paramstyles as are common with the Postgresql
        and MySQL drivers the doubling will occur, for others like that
        of SQLite it will not.   This allows more database-agnostic use
        of the :obj:`.literal_column` construct to be possible.

        .. seealso::

            :ref:`change_3740`

    .. change:: 3959
        :tags: bug, postgresql
        :tickets: 3959

        Added support for all possible "fields" identifiers when reflecting the
        Postgresql ``INTERVAL`` datatype, e.g. "YEAR", "MONTH", "DAY TO
        MINUTE", etc..   In addition, the :class:`.postgresql.INTERVAL`
        datatype itself now includes a new parameter
        :paramref:`.postgresql.INTERVAL.fields` where these qualifiers can be
        specified; the qualifier is also reflected back into the resulting
        datatype upon reflection / inspection.

        .. seealso::

            :ref:`change_3959`

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

    .. change:: 3953
        :tags: feature, sql
        :tickets: 3953

        Added a new kind of :func:`.bindparam` called "expanding".  This is
        for use in ``IN`` expressions where the list of elements is rendered
        into individual bound parameters at statement execution time, rather
        than at statement compilation time.  This allows both a single bound
        parameter name to be linked to an IN expression of multiple elements,
        as well as allows query caching to be used with IN expressions.  The
        new feature allows the related features of "select in" loading and
        "polymorphic in" loading to make use of the baked query extension
        to reduce call overhead.   This feature should be considered to be
        **experimental** for 1.2.

        .. seealso::

            :ref:`change_3953`

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

    .. change:: 3303
        :tags: feature, orm
        :tickets: 3303

        Added new event handler :meth:`.AttributeEvents.modified` which is
        triggered when the func:`.attributes.flag_modified` function is
        invoked, which is common when using the :mod:`sqlalchemy.ext.mutable`
        extension module.

        .. seealso::

            :ref:`change_3303`

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

    .. change:: 3607
        :tags: bug, orm
        :tickets: 3607

        Fixed bug where :meth:`.Query.with_parent` would not work if the
        :class:`.Query` were against an :func:`.aliased` construct rather than
        a regular mapped class.  Also adds a new parameter
        :paramref:`.util.with_parent.from_entity` to the standalone
        :func:`.util.with_parent` function as well as
        :meth:`.Query.with_parent`.
