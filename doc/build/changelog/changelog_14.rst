=============
1.4 Changelog
=============

This document details individual issue-level changes made throughout
1.4 releases.  For a narrative overview of what's new in 1.4, see
:ref:`migration_14_toplevel`.


.. changelog_imports::

    .. include:: changelog_13.rst
        :start-line: 5


.. changelog::
    :version: 1.4.55
    :include_notes_from: unreleased_14

.. changelog::
    :version: 1.4.54
    :released: September 5, 2024

    .. change::
        :tags: bug, regression, orm
        :tickets: 11728
        :versions: 2.0.33

        Fixed regression from 1.3 where the column key used for a hybrid property
        might be populated with that of the underlying column that it returns, for
        a property that returns an ORM mapped column directly, rather than the key
        used by the hybrid property itself.

    .. change::
        :tags: change, general
        :tickets: 11818
        :versions: 2.0.33 1.4.54

        The pin for ``setuptools<69.3`` in ``pyproject.toml`` has been removed.
        This pin was to prevent a sudden change in setuptools to use :pep:`625`
        from taking place, which would change the file name of SQLAlchemy's source
        distribution on pypi to be an all lower case name, which is likely to cause
        problems with various build environments that expected the previous naming
        style.  However, the presence of this pin is holding back environments that
        otherwise want to use a newer setuptools, so we've decided to move forward
        with this change, with the assumption that build environments will have
        largely accommodated the setuptools change by now.

        This change was first released in version 2.0.33 however is being
        backported to 1.4.54 to support ongoing releases.


    .. change::
        :tags: bug, postgresql
        :tickets: 11819
        :versions: 2.0.33, 1.4.54

        Fixed critical issue in the asyncpg driver where a rollback or commit that
        fails specifically for the ``MissingGreenlet`` condition or any other error
        that is not raised by asyncpg itself would discard the asyncpg transaction
        in any case, even though the transaction were still idle, leaving to a
        server side condition with an idle transaction that then goes back into the
        connection pool.   The flags for "transaction closed" are now not reset for
        errors that are raised outside of asyncpg itself.  When asyncpg itself
        raises an error for ``.commit()`` or ``.rollback()``, asyncpg does then
        discard of this transaction.

    .. change::
        :tags: change, general

        The setuptools "test" command is removed from the 1.4 series as modern
        versions of setuptools actively refuse to accommodate this extension being
        present.   This change was already part of the 2.0 series.   To run the
        test suite use the ``tox`` command.

.. changelog::
    :version: 1.4.53
    :released: July 29, 2024

    .. change::
        :tags: bug, general
        :tickets: 11417
        :versions: 2.0.31

        Set up full Python 3.13 support to the extent currently possible, repairing
        issues within internal language helpers as well as the serializer extension
        module.

        For version 1.4, this also modernizes the "extras" names in setup.cfg
        to use dashes and not underscores for two-word names.  Underscore names
        are still present to accommodate potential compatibility issues.

    .. change::
        :tags: bug, sql
        :tickets: 11471
        :versions: 2.0.31

        Fixed caching issue where using the :meth:`.TextualSelect.add_cte` method
        of the :class:`.TextualSelect` construct would not set a correct cache key
        which distinguished between different CTE expressions.

    .. change::
        :tags: bug, engine
        :tickets: 11499

        Adjustments to the C extensions, which are specific to the SQLAlchemy 1.x
        series, to work under Python 3.13.  Pull request courtesy Ben Beasley.

    .. change::
        :tags: bug, mssql
        :tickets: 11514
        :versions: 2.0.32

        Fixed issue where SQL Server drivers don't support bound parameters when
        rendering the "frame specification" for a window function, e.g. "ROWS
        BETWEEN", etc.


    .. change::
        :tags: bug, sql
        :tickets: 11544
        :versions: 2.0

        Fixed caching issue where the
        :paramref:`_sql.Select.with_for_update.key_share` element of
        :meth:`_sql.Select.with_for_update` was not considered as part of the cache
        key, leading to incorrect caching if different variations of this parameter
        were used with an otherwise identical statement.

    .. change::
        :tags: bug, orm, regression
        :tickets: 11562
        :versions: 2.0.32

        Fixed regression going back to 1.4 where accessing a collection using the
        "dynamic" strategy on a transient object and attempting to query would
        raise an internal error rather than the expected :class:`.NoResultFound`
        that occurred in 1.3.

    .. change::
        :tags: bug, reflection, sqlite
        :tickets: 11582
        :versions: 2.0.32

        Fixed reflection of computed column in SQLite to properly account
        for complex expressions.

    .. change::
        :tags: usecase, engine
        :versions: 2.0.31

        Modified the internal representation used for adapting asyncio calls to
        greenlets to allow for duck-typed compatibility with third party libraries
        that implement SQLAlchemy's "greenlet-to-asyncio" pattern directly.
        Running code within a greenlet that features the attribute
        ``__sqlalchemy_greenlet_provider__ = True`` will allow calls to
        :func:`sqlalchemy.util.await_only` directly.


    .. change::
        :tags: bug, mypy
        :versions: 2.0.32

        The deprecated mypy plugin is no longer fully functional with the latest
        series of mypy 1.11.0, as changes in the mypy interpreter are no longer
        compatible with the approach used by the plugin.  If code is dependent on
        the mypy plugin with sqlalchemy2-stubs, it's recommended to pin mypy to be
        below the 1.11.0 series.    Seek upgrading to the 2.0 series of SQLAlchemy
        and migrating to the modern type annotations.

        .. seealso::

            mypy_toplevel -- section was removed

.. changelog::
    :version: 1.4.52
    :released: March 4, 2024

    .. change::
        :tags: bug, orm
        :tickets: 10365, 11412

        Fixed bug where ORM :func:`_orm.with_loader_criteria` would not apply
        itself to a :meth:`_sql.Select.join` where the ON clause were given as a
        plain SQL comparison, rather than as a relationship target or similar.

        This is a backport of the same issue fixed in version 2.0 for 2.0.22.

        **update** - this was found to also fix an issue where
        single-inheritance criteria would not be correctly applied to a
        subclass entity that only appeared in the ``select_from()`` list,
        see :ticket:`11412`

.. changelog::
    :version: 1.4.51
    :released: January 2, 2024

    .. change::
        :tags: bug, mysql
        :tickets: 10650
        :versions: 2.0.24

        Fixed regression introduced by the fix in ticket :ticket:`10492` when using
        pool pre-ping with PyMySQL version older than 1.0.

    .. change::
        :tags: bug, orm
        :tickets: 10782
        :versions: 2.0.24, 1.4.51

        Improved a fix first implemented for :ticket:`3208` released in version
        0.9.8, where the registry of classes used internally by declarative could
        be subject to a race condition in the case where individual mapped classes
        are being garbage collected at the same time while new mapped classes are
        being constructed, as can happen in some test suite configurations or
        dynamic class creation environments.   In addition to the weakref check
        already added, the list of items being iterated is also copied first to
        avoid "list changed while iterating" errors.  Pull request courtesy Yilei
        Yang.


    .. change::
        :tags: bug, asyncio
        :tickets: 10813
        :versions: 1.4.51, 2.0.25

        Fixed critical issue in asyncio version of the connection pool where
        calling :meth:`_asyncio.AsyncEngine.dispose` would produce a new connection
        pool that did not fully re-establish the use of asyncio-compatible mutexes,
        leading to the use of a plain ``threading.Lock()`` which would then cause
        deadlocks in an asyncio context when using concurrency features like
        ``asyncio.gather()``.

.. changelog::
    :version: 1.4.50
    :released: October 29, 2023

    .. change::
        :tags: bug, sql
        :tickets: 10142
        :versions: 2.0.23

        Fixed issue where using the same bound parameter more than once with
        ``literal_execute=True`` in some combinations with other literal rendering
        parameters would cause the wrong values to render due to an iteration
        issue.

    .. change::
        :tags: mysql, usecase
        :versions: 2.0.20

        Updated aiomysql dialect since the dialect appears to be maintained again.
        Re-added to the ci testing using version 0.2.0.

    .. change::
        :tags: bug, orm
        :tickets: 10223
        :versions: 2.0.20

        Fixed fundamental issue which prevented some forms of ORM "annotations"
        from taking place for subqueries which made use of :meth:`_sql.Select.join`
        against a relationship target.  These annotations are used whenever a
        subquery is used in special situations such as within
        :meth:`_orm.PropComparator.and_` and other ORM-specific scenarios.

    .. change::
        :tags: bug, sql
        :tickets: 10213
        :versions: 2.0.20

        Fixed issue where unpickling of a :class:`_schema.Column` or other
        :class:`_sql.ColumnElement` would fail to restore the correct "comparator"
        object, which is used to generate SQL expressions specific to the type
        object.

    .. change::
        :tags: bug, mysql
        :tickets: 10492
        :versions: 2.0.23

        Repaired a new incompatibility in the MySQL "pre-ping" routine where the
        ``False`` argument passed to ``connection.ping()``, which is intended to
        disable an unwanted "automatic reconnect" feature,  is being deprecated in
        MySQL drivers and backends, and is producing warnings for some versions of
        MySQL's native client drivers.  It's removed for mysqlclient, whereas for
        PyMySQL and drivers based on PyMySQL, the parameter will be deprecated and
        removed at some point, so API introspection is used to future proof against
        these various stages of removal.

    .. change::
        :tags: schema, bug
        :tickets: 10207
        :versions: 2.0.21

        Modified the rendering of the Oracle only :paramref:`.Identity.order`
        parameter that's part of both :class:`.Sequence` and :class:`.Identity` to
        only take place for the Oracle backend, and not other backends such as that
        of PostgreSQL.  A future release will rename the
        :paramref:`.Identity.order`, :paramref:`.Sequence.order`  and
        :paramref:`.Identity.on_null` parameters to Oracle-specific names,
        deprecating the old names, these parameters only apply to Oracle.

    .. change::
        :tags: bug, mssql, reflection
        :tickets: 10504
        :versions: 2.0.23

        Fixed issue where identity column reflection would fail
        for a bigint column with a large identity start value
        (more than 18 digits).

.. changelog::
    :version: 1.4.49
    :released: July 5, 2023

    .. change::
        :tags: bug, sql
        :tickets: 10042
        :versions: 2.0.18

        Fixed issue where the :meth:`_sql.ColumnOperators.regexp_match`
        when using "flags" would not produce a "stable" cache key, that
        is, the cache key would keep changing each time causing cache pollution.
        The same issue existed for :meth:`_sql.ColumnOperators.regexp_replace`
        with both the flags and the actual replacement expression.
        The flags are now represented as fixed modifier strings rendered as
        safestrings rather than bound parameters, and the replacement
        expression is established within the primary portion of the "binary"
        element so that it generates an appropriate cache key.

        Note that as part of this change, the
        :paramref:`_sql.ColumnOperators.regexp_match.flags` and
        :paramref:`_sql.ColumnOperators.regexp_replace.flags` have been modified to
        render as literal strings only, whereas previously they were rendered as
        full SQL expressions, typically bound parameters.   These parameters should
        always be passed as plain Python strings and not as SQL expression
        constructs; it's not expected that SQL expression constructs were used in
        practice for this parameter, so this is a backwards-incompatible change.

        The change also modifies the internal structure of the expression
        generated, for :meth:`_sql.ColumnOperators.regexp_replace` with or without
        flags, and for :meth:`_sql.ColumnOperators.regexp_match` with flags. Third
        party dialects which may have implemented regexp implementations of their
        own (no such dialects could be located in a search, so impact is expected
        to be low) would need to adjust the traversal of the structure to
        accommodate.


    .. change::
        :tags: bug, sql
        :versions: 2.0.18

        Fixed issue in mostly-internal :class:`.CacheKey` construct where the
        ``__ne__()`` operator were not properly implemented, leading to nonsensical
        results when comparing :class:`.CacheKey` instances to each other.




    .. change::
        :tags: bug, extensions
        :versions: 2.0.17

        Fixed issue in mypy plugin for use with mypy 1.4.

    .. change::
        :tags: platform, usecase

        Compatibility improvements to work fully with Python 3.12

.. changelog::
    :version: 1.4.48
    :released: April 30, 2023

    .. change::
        :tags: bug, orm
        :tickets: 9728
        :versions: 2.0.12

        Fixed critical caching issue where the combination of
        :func:`_orm.aliased()` and :func:`_hybrid.hybrid_property` expression
        compositions would cause a cache key mismatch, leading to cache keys that
        held onto the actual :func:`_orm.aliased` object while also not matching
        that of equivalent constructs, filling up the cache.

    .. change::
        :tags: bug, orm
        :tickets: 9634
        :versions: 2.0.10

        Fixed bug where various ORM-specific getters such as
        :attr:`.ORMExecuteState.is_column_load`,
        :attr:`.ORMExecuteState.is_relationship_load`,
        :attr:`.ORMExecuteState.loader_strategy_path` etc. would throw an
        ``AttributeError`` if the SQL statement itself were a "compound select"
        such as a UNION.

    .. change::
        :tags: bug, orm
        :tickets: 9590
        :versions: 2.0.9

        Fixed endless loop which could occur when using "relationship to aliased
        class" feature and also indicating a recursive eager loader such as
        ``lazy="selectinload"`` in the loader, in combination with another eager
        loader on the opposite side. The check for cycles has been fixed to include
        aliased class relationships.

.. changelog::
    :version: 1.4.47
    :released: March 18, 2023

    .. change::
        :tags: bug, sql
        :tickets: 9075
        :versions: 2.0.0rc3

        Fixed bug / regression where using :func:`.bindparam()` with the same name
        as a column in the :meth:`.Update.values` method of :class:`.Update`, as
        well as the :meth:`_dml.Insert.values` method of :class:`_dml.Insert` in 2.0 only,
        would in some cases silently fail to honor the SQL expression in which the
        parameter were presented, replacing the expression with a new parameter of
        the same name and discarding any other elements of the SQL expression, such
        as SQL functions, etc. The specific case would be statements that were
        constructed against ORM entities rather than plain :class:`.Table`
        instances, but would occur if the statement were invoked with a
        :class:`.Session` or a :class:`.Connection`.

        :class:`.Update` part of the issue was present in both 2.0 and 1.4 and is
        backported to 1.4.

    .. change::
        :tags: bug, oracle
        :tickets: 5047

        Added :class:`_oracle.ROWID` to reflected types as this type may be used in
        a "CREATE TABLE" statement.

    .. change::
        :tags: bug, sql
        :tickets: 7664

        Fixed stringify for a the :class:`.CreateSchema` and :class:`.DropSchema`
        DDL constructs, which would fail with an ``AttributeError`` when
        stringified without a dialect.


    .. change::
        :tags: usecase, mysql
        :tickets: 9047
        :versions: 2.0.0

        Added support to MySQL index reflection to correctly reflect the
        ``mysql_length`` dictionary, which previously was being ignored.

    .. change::
        :tags: bug, postgresql
        :tickets: 9048
        :versions: 2.0.0

        Added support to the asyncpg dialect to return the ``cursor.rowcount``
        value for SELECT statements when available. While this is not a typical use
        for ``cursor.rowcount``, the other PostgreSQL dialects generally provide
        this value. Pull request courtesy Michael Gorven.

    .. change::
        :tags: bug, mssql
        :tickets: 9133

        Fixed bug where a schema name given with brackets, but no dots inside the
        name, for parameters such as :paramref:`_schema.Table.schema` would not be
        interpreted within the context of the SQL Server dialect's documented
        behavior of interpreting explicit brackets as token delimiters, first added
        in 1.2 for #2626, when referring to the schema name in reflection
        operations. The original assumption for #2626's behavior was that the
        special interpretation of brackets was only significant if dots were
        present, however in practice, the brackets are not included as part of the
        identifier name for all SQL rendering operations since these are not valid
        characters within regular or delimited identifiers.  Pull request courtesy
        Shan.


    .. change::
        :tags: bug, mypy
        :versions: 2.0.0rc3

        Adjustments made to the mypy plugin to accommodate for some potential
        changes being made for issue #236 sqlalchemy2-stubs when using SQLAlchemy
        1.4. These changes are being kept in sync within SQLAlchemy 2.0.
        The changes are also backwards compatible with older versions of
        sqlalchemy2-stubs.


    .. change::
        :tags: bug, mypy
        :tickets: 9102
        :versions: 2.0.0rc3

        Fixed crash in mypy plugin which could occur on both 1.4 and 2.0 versions
        if a decorator for the :func:`_orm.registry.mapped` decorator were used
        that was referenced in an expression with more than two components (e.g.
        ``@Backend.mapper_registry.mapped``). This scenario is now ignored; when
        using the plugin, the decorator expression needs to be two components (i.e.
        ``@reg.mapped``).

    .. change::
        :tags: bug, sql
        :tickets: 9506

        Fixed critical SQL caching issue where use of the
        :meth:`_sql.Operators.op` custom operator function would not produce an appropriate
        cache key, leading to reduce the effectiveness of the SQL cache.


.. changelog::
    :version: 1.4.46
    :released: January 3, 2023

    .. change::
        :tags: bug, engine
        :tickets: 8974
        :versions: 2.0.0rc1

        Fixed a long-standing race condition in the connection pool which could
        occur under eventlet/gevent monkeypatching schemes in conjunction with the
        use of eventlet/gevent ``Timeout`` conditions, where a connection pool
        checkout that's interrupted due to the timeout would fail to clean up the
        failed state, causing the underlying connection record and sometimes the
        database connection itself to "leak", leaving the pool in an invalid state
        with unreachable entries. This issue was first identified and fixed in
        SQLAlchemy 1.2 for :ticket:`4225`, however the failure modes detected in
        that fix failed to accommodate for ``BaseException``, rather than
        ``Exception``, which prevented eventlet/gevent ``Timeout`` from being
        caught. In addition, a block within initial pool connect has also been
        identified and hardened with a ``BaseException`` -> "clean failed connect"
        block to accommodate for the same condition in this location.
        Big thanks to Github user @niklaus for their tenacious efforts in
        identifying and describing this intricate issue.

    .. change::
        :tags: bug, postgresql
        :tickets: 9023
        :versions: 2.0.0rc1

        Fixed bug where the PostgreSQL
        :paramref:`_postgresql.Insert.on_conflict_do_update.constraint` parameter
        would accept an :class:`.Index` object, however would not expand this index
        out into its individual index expressions, instead rendering its name in an
        ON CONFLICT ON CONSTRAINT clause, which is not accepted by PostgreSQL; the
        "constraint name" form only accepts unique or exclude constraint names. The
        parameter continues to accept the index but now expands it out into its
        component expressions for the render.

    .. change::
        :tags: bug, general
        :tickets: 8995
        :versions: 2.0.0rc1

        Fixed regression where the base compat module was calling upon
        ``platform.architecture()`` in order to detect some system properties,
        which results in an over-broad system call against the system-level
        ``file`` call that is unavailable under some circumstances, including
        within some secure environment configurations.

    .. change::
        :tags: usecase, postgresql
        :tickets: 8393
        :versions: 2.0.0b5

        Added the PostgreSQL type ``MACADDR8``.
        Pull request courtesy of Asim Farooq.

    .. change::
        :tags: bug, sqlite
        :tickets: 8969
        :versions: 2.0.0b5

        Fixed regression caused by new support for reflection of partial indexes on
        SQLite added in 1.4.45 for :ticket:`8804`, where the ``index_list`` pragma
        command in very old versions of SQLite (possibly prior to 3.8.9) does not
        return the current expected number of columns, leading to exceptions raised
        when reflecting tables and indexes.

    .. change::
        :tags: bug, tests
        :versions: 2.0.0rc1

        Fixed issue in tox.ini file where changes in the tox 4.0 series to the
        format of "passenv" caused tox to not function correctly, in particular
        raising an error as of tox 4.0.6.

    .. change::
        :tags: bug, tests
        :tickets: 9002
        :versions: 2.0.0rc1

        Added new exclusion rule for third party dialects called
        ``unusual_column_name_characters``, which can be "closed" for third party
        dialects that don't support column names with unusual characters such as
        dots, slashes, or percent signs in them, even if the name is properly
        quoted.


    .. change::
        :tags: bug, sql
        :tickets: 9009
        :versions: 2.0.0b5

        Added parameter
        :paramref:`.FunctionElement.column_valued.joins_implicitly`, which is
        useful in preventing the "cartesian product" warning when making use of
        table-valued or column-valued functions. This parameter was already
        introduced for :meth:`.FunctionElement.table_valued` in :ticket:`7845`,
        however it failed to be added for :meth:`.FunctionElement.column_valued`
        as well.

    .. change::
        :tags: change, general
        :tickets: 8983

        A new deprecation "uber warning" is now emitted at runtime the
        first time any SQLAlchemy 2.0 deprecation warning would normally be
        emitted, but the ``SQLALCHEMY_WARN_20`` environment variable is not set.
        The warning emits only once at most, before setting a boolean to prevent
        it from emitting a second time.

        This deprecation warning intends to notify users who may not have set an
        appropriate constraint in their requirements files to block against a
        surprise SQLAlchemy 2.0 upgrade and also alert that the SQLAlchemy 2.0
        upgrade process is available, as the first full 2.0 release is expected
        very soon. The deprecation warning can be silenced by setting the
        environment variable ``SQLALCHEMY_SILENCE_UBER_WARNING`` to ``"1"``.

        .. seealso::

            :ref:`migration_20_toplevel`

    .. change::
        :tags: bug, orm
        :tickets: 9033
        :versions: 2.0.0rc1

        Fixed issue in the internal SQL traversal for DML statements like
        :class:`_dml.Update` and :class:`_dml.Delete` which would cause among other
        potential issues, a specific issue using lambda statements with the ORM
        update/delete feature.

    .. change::
        :tags: bug, sql
        :tickets: 8989
        :versions: 2.0.0b5

        Fixed bug where SQL compilation would fail (assertion fail in 2.0, NoneType
        error in 1.4) when using an expression whose type included
        :meth:`_types.TypeEngine.bind_expression`, in the context of an "expanding"
        (i.e. "IN") parameter in conjunction with the ``literal_binds`` compiler
        parameter.

    .. change::
        :tags: bug, sql
        :tickets: 9029
        :versions: 2.0.0rc1

        Fixed issue in lambda SQL feature where the calculated type of a literal
        value would not take into account the type coercion rules of the "compared
        to type", leading to a lack of typing information for SQL expressions, such
        as comparisons to :class:`_types.JSON` elements and similar.

.. changelog::
    :version: 1.4.45
    :released: December 10, 2022

    .. change::
        :tags: bug, orm
        :tickets: 8862
        :versions: 2.0.0rc1

        Fixed bug where :meth:`_orm.Session.merge` would fail to preserve the
        current loaded contents of relationship attributes that were indicated with
        the :paramref:`_orm.relationship.viewonly` parameter, thus defeating
        strategies that use :meth:`_orm.Session.merge` to pull fully loaded objects
        from caches and other similar techniques. In a related change, fixed issue
        where an object that contains a loaded relationship that was nonetheless
        configured as ``lazy='raise'`` on the mapping would fail when passed to
        :meth:`_orm.Session.merge`; checks for "raise" are now suspended within
        the merge process assuming the :paramref:`_orm.Session.merge.load`
        parameter remains at its default of ``True``.

        Overall, this is a behavioral adjustment to a change introduced in the 1.4
        series as of :ticket:`4994`, which took "merge" out of the set of cascades
        applied by default to "viewonly" relationships. As "viewonly" relationships
        aren't persisted under any circumstances, allowing their contents to
        transfer during "merge" does not impact the persistence behavior of the
        target object. This allows :meth:`_orm.Session.merge` to correctly suit one
        of its use cases, that of adding objects to a :class:`.Session` that were
        loaded elsewhere, often for the purposes of restoring from a cache.


    .. change::
        :tags: bug, orm
        :tickets: 8881
        :versions: 2.0.0rc1

        Fixed issues in :func:`_orm.with_expression` where expressions that were
        composed of columns that were referenced from the enclosing SELECT would
        not render correct SQL in some contexts, in the case where the expression
        had a label name that matched the attribute which used
        :func:`_orm.query_expression`, even when :func:`_orm.query_expression` had
        no default expression. For the moment, if the :func:`_orm.query_expression`
        does have a default expression, that label name is still used for that
        default, and an additional label with the same name will continue to be
        ignored. Overall, this case is pretty thorny so further adjustments might
        be warranted.

    .. change::
        :tags: bug, sqlite
        :tickets: 8866

        Backported a fix for SQLite reflection of unique constraints in attached
        schemas, released in 2.0 as a small part of :ticket:`4379`. Previously,
        unique constraints in attached schemas would be ignored by SQLite
        reflection. Pull request courtesy Michael Gorven.

    .. change::
        :tags: bug, asyncio
        :tickets: 8952
        :versions: 2.0.0rc1

        Removed non-functional ``merge()`` method from
        :class:`_asyncio.AsyncResult`.  This method has never worked and was
        included with :class:`_asyncio.AsyncResult` in error.

    .. change::
        :tags: bug, oracle
        :tickets: 8708
        :versions: 2.0.0b4

        Continued fixes for Oracle fix :ticket:`8708` released in 1.4.43 where
        bound parameter names that start with underscores, which are disallowed by
        Oracle, were still not being properly escaped in all circumstances.


    .. change::
        :tags: bug, postgresql
        :tickets: 8748
        :versions: 2.0.0rc1

        Made an adjustment to how the PostgreSQL dialect considers column types
        when it reflects columns from a table, to accommodate for alternative
        backends which may return NULL from the PG ``format_type()`` function.

    .. change::
        :tags: usecase, sqlite
        :tickets: 8903
        :versions: 2.0.0rc1

        Added support for the SQLite backend to reflect the "DEFERRABLE" and
        "INITIALLY" keywords which may be present on a foreign key construct. Pull
        request courtesy Michael Gorven.

    .. change::
        :tags: usecase, sql
        :tickets: 8800
        :versions: 2.0.0rc1

        An informative re-raise is now thrown in the case where any "literal
        bindparam" render operation fails, indicating the value itself and
        the datatype in use, to assist in debugging when literal params
        are being rendered in a statement.

    .. change::
        :tags: usecase, sqlite
        :tickets: 8804
        :versions: 2.0.0rc1

        Added support for reflection of expression-oriented WHERE criteria included
        in indexes on the SQLite dialect, in a manner similar to that of the
        PostgreSQL dialect. Pull request courtesy Tobias Pfeiffer.

    .. change::
        :tags: bug, sql
        :tickets: 8827
        :versions: 2.0.0rc1

        Fixed a series of issues regarding the position and sometimes the identity
        of rendered bound parameters, such as those used for SQLite, asyncpg,
        MySQL, Oracle and others. Some compiled forms would not maintain the order
        of parameters correctly, such as the PostgreSQL ``regexp_replace()``
        function, the "nesting" feature of the :class:`.CTE` construct first
        introduced in :ticket:`4123`, and selectable tables formed by using the
        :meth:`.FunctionElement.column_valued` method with Oracle.


    .. change::
        :tags: bug, oracle
        :tickets: 8945
        :versions: 2.0.0rc1

        Fixed issue in Oracle compiler where the syntax for
        :meth:`.FunctionElement.column_valued` was incorrect, rendering the name
        ``COLUMN_VALUE`` without qualifying the source table correctly.

    .. change::
        :tags: bug, engine
        :tickets: 8963
        :versions: 2.0.0rc1

        Fixed issue where :meth:`_engine.Result.freeze` method would not work for
        textual SQL using either :func:`_sql.text` or
        :meth:`_engine.Connection.exec_driver_sql`.


.. changelog::
    :version: 1.4.44
    :released: November 12, 2022

    .. change::
        :tags: bug, sql
        :tickets: 8790
        :versions: 2.0.0b4

        Fixed critical memory issue identified in cache key generation, where for
        very large and complex ORM statements that make use of lots of ORM aliases
        with subqueries, cache key generation could produce excessively large keys
        that were orders of magnitude bigger than the statement itself. Much thanks
        to Rollo Konig Brock for their very patient, long term help in finally
        identifying this issue.

    .. change::
        :tags: bug, postgresql, mssql
        :tickets: 8770
        :versions: 2.0.0b4

        For the PostgreSQL and SQL Server dialects only, adjusted the compiler so
        that when rendering column expressions in the RETURNING clause, the "non
        anon" label that's used in SELECT statements is suggested for SQL
        expression elements that generate a label; the primary example is a SQL
        function that may be emitting as part of the column's type, where the label
        name should match the column's name by default. This restores a not-well
        defined behavior that had changed in version 1.4.21 due to :ticket:`6718`,
        :ticket:`6710`. The Oracle dialect has a different RETURNING implementation
        and was not affected by this issue. Version 2.0 features an across the
        board change for its widely expanded support of RETURNING on other
        backends.


    .. change::
        :tags: bug, oracle

        Fixed issue in the Oracle dialect where an INSERT statement that used
        ``insert(some_table).values(...).returning(some_table)`` against a full
        :class:`.Table` object at once would fail to execute, raising an exception.

    .. change::
        :tags: bug, tests
        :tickets: 8793
        :versions: 2.0.0b4

        Fixed issue where the ``--disable-asyncio`` parameter to the test suite
        would fail to not actually run greenlet tests and would also not prevent
        the suite from using a "wrapping" greenlet for the whole suite. This
        parameter now ensures that no greenlet or asyncio use will occur within the
        entire run when set.

    .. change::
        :tags: bug, tests

        Adjusted the test suite which tests the Mypy plugin to accommodate for
        changes in Mypy 0.990 regarding how it handles message output, which affect
        how sys.path is interpreted when determining if notes and errors should be
        printed for particular files. The change broke the test suite as the files
        within the test directory itself no longer produced messaging when run
        under the mypy API.

.. changelog::
    :version: 1.4.43
    :released: November 4, 2022

    .. change::
        :tags: bug, orm
        :tickets: 8738
        :versions: 2.0.0b3

        Fixed issue in joined eager loading where an assertion fail would occur
        with a particular combination of outer/inner joined eager loads, when
        eager loading across three mappers where the middle mapper was
        an inherited subclass mapper.


    .. change::
        :tags: bug, oracle
        :tickets: 8708
        :versions: 2.0.0b3

        Fixed issue where bound parameter names, including those automatically
        derived from similarly-named database columns, which contained characters
        that normally require quoting with Oracle would not be escaped when using
        "expanding parameters" with the Oracle dialect, causing execution errors.
        The usual "quoting" for bound parameters used by the Oracle dialect is not
        used with the "expanding parameters" architecture, so escaping for a large
        range of characters is used instead, now using a list of characters/escapes
        that are specific to Oracle.



    .. change::
        :tags: bug, orm
        :tickets: 8721
        :versions: 2.0.0b3

        Fixed bug involving :class:`.Select` constructs, where combinations of
        :meth:`.Select.select_from` with :meth:`.Select.join`, as well as when
        using :meth:`.Select.join_from`, would cause the
        :func:`_orm.with_loader_criteria` feature as well as the IN criteria needed
        for single-table inheritance queries to not render, in cases where the
        columns clause of the query did not explicitly include the left-hand side
        entity of the JOIN. The correct entity is now transferred to the
        :class:`.Join` object that's generated internally, so that the criteria
        against the left side entity is correctly added.


    .. change::
        :tags: bug, mssql
        :tickets: 8714
        :versions: 2.0.0b3

        Fixed issue with :meth:`.Inspector.has_table`, which when used against a
        temporary table with the SQL Server dialect would fail on some Azure
        variants, due to an unnecessary information schema query that is not
        supported on those server versions. Pull request courtesy Mike Barry.

    .. change::
        :tags: bug, orm
        :tickets: 8711
        :versions: 2.0.0b3

        An informative exception is now raised when the
        :func:`_orm.with_loader_criteria` option is used as a loader option added
        to a specific "loader path", such as when using it within
        :meth:`.Load.options`. This use is not supported as
        :func:`_orm.with_loader_criteria` is only intended to be used as a top
        level loader option. Previously, an internal error would be generated.

    .. change::
        :tags: bug, oracle
        :tickets: 8744
        :versions: 2.0.0b3

        Fixed issue where the ``nls_session_parameters`` view queried on first
        connect in order to get the default decimal point character may not be
        available depending on Oracle connection modes, and would therefore raise
        an error.  The approach to detecting decimal char has been simplified to
        test a decimal value directly, instead of reading system views, which
        works on any backend / driver.


    .. change::
        :tags: bug, orm
        :tickets: 8753
        :versions: 2.0.0b3

        Improved "dictionary mode" for :meth:`_orm.Session.get` so that synonym
        names which refer to primary key attribute names may be indicated in the
        named dictionary.

    .. change::
        :tags: bug, engine, regression
        :tickets: 8717
        :versions: 2.0.0b3

        Fixed issue where the :meth:`.PoolEvents.reset` event hook would not be be
        called in all cases when a :class:`_engine.Connection` were closed and was
        in the process of returning its DBAPI connection to the connection pool.

        The scenario was when the :class:`_engine.Connection` had already emitted
        ``.rollback()`` on its DBAPI connection within the process of returning
        the connection to the pool, where it would then instruct the connection
        pool to forego doing its own "reset" to save on the additional method
        call.  However, this prevented custom pool reset schemes from being
        used within this hook, as such hooks by definition are doing more than
        just calling ``.rollback()``, and need to be invoked under all
        circumstances.  This was a regression that appeared in version 1.4.

        For version 1.4, the :meth:`.PoolEvents.checkin` remains viable as an
        alternate event hook to use for custom "reset" implementations. Version 2.0
        will feature an improved version of :meth:`.PoolEvents.reset` which is
        called for additional scenarios such as termination of asyncio connections,
        and is also passed contextual information about the reset, to allow for
        "custom connection reset" schemes which can respond to different reset
        scenarios in different ways.

    .. change::
        :tags: bug, orm
        :tickets: 8704
        :versions: 2.0.0b3

        Fixed issue where "selectin_polymorphic" loading for inheritance mappers
        would not function correctly if the :paramref:`_orm.Mapper.polymorphic_on`
        parameter referred to a SQL expression that was not directly mapped on the
        class.

    .. change::
        :tags: bug, orm
        :tickets: 8710
        :versions: 2.0.0b3

        Fixed issue where the underlying DBAPI cursor would not be closed when
        using the :class:`_orm.Query` object as an iterator, if a user-defined exception
        case were raised within the iteration process, thereby causing the iterator
        to be closed by the Python interpreter.  When using
        :meth:`_orm.Query.yield_per` to create server-side cursors, this would lead
        to the usual MySQL-related issues with server side cursors out of sync,
        and without direct access to the :class:`.Result` object, end-user code
        could not access the cursor in order to close it.

        To resolve, a catch for ``GeneratorExit`` is applied within the iterator
        method, which will close the result object in those cases when the
        iterator were interrupted, and by definition will be closed by the
        Python interpreter.

        As part of this change as implemented for the 1.4 series, ensured that
        ``.close()`` methods are available on all :class:`.Result` implementations
        including :class:`.ScalarResult`, :class:`.MappingResult`.  The 2.0
        version of this change also includes new context manager patterns for use
        with :class:`.Result` classes.

    .. change::
        :tags: bug, engine
        :tickets: 8710

        Ensured all :class:`.Result` objects include a :meth:`.Result.close` method
        as well as a :attr:`.Result.closed` attribute, including on
        :class:`.ScalarResult` and :class:`.MappingResult`.

    .. change::
        :tags: bug, mssql, reflection
        :tickets: 8700
        :versions: 2.0.0b3

        Fixed issue with :meth:`.Inspector.has_table`, which when used against a
        view with the SQL Server dialect would erroneously return ``False``, due to
        a regression in the 1.4 series which removed support for this on SQL
        Server. The issue is not present in the 2.0 series which uses a different
        reflection architecture. Test support is added to ensure ``has_table()``
        remains working per spec re: views.

    .. change::
        :tags: bug, sql
        :tickets: 8724
        :versions: 2.0.0b3

        Fixed issue which prevented the :func:`_sql.literal_column` construct from
        working properly within the context of a :class:`.Select` construct as well
        as other potential places where "anonymized labels" might be generated, if
        the literal expression contained characters which could interfere with
        format strings, such as open parenthesis, due to an implementation detail
        of the "anonymous label" structure.


.. changelog::
    :version: 1.4.42
    :released: October 16, 2022

    .. change::
        :tags: bug, asyncio
        :tickets: 8516

        Improved implementation of ``asyncio.shield()`` used in context managers as
        added in :ticket:`8145`, such that the "close" operation is enclosed within
        an ``asyncio.Task`` which is then strongly referenced as the operation
        proceeds. This is per Python documentation indicating that the task is
        otherwise not strongly referenced.

    .. change::
        :tags: bug, orm
        :tickets: 8614

        The :paramref:`_orm.Session.execute.bind_arguments` dictionary is no longer
        mutated when passed to :meth:`_orm.Session.execute` and similar; instead,
        it's copied to an internal dictionary for state changes. Among other
        things, this fixes and issue where the "clause" passed to the
        :meth:`_orm.Session.get_bind` method would be incorrectly referring to the
        :class:`_sql.Select` construct used for the "fetch" synchronization
        strategy, when the actual query being emitted was a :class:`_dml.Delete` or
        :class:`_dml.Update`. This would interfere with recipes for "routing
        sessions".

    .. change::
        :tags: bug, orm
        :tickets: 7094

        A warning is emitted in ORM configurations when an explicit
        :func:`_orm.remote` annotation is applied to columns that are local to the
        immediate mapped class, when the referenced class does not include any of
        the same table columns. Ideally this would raise an error at some point as
        it's not correct from a mapping point of view.

    .. change::
        :tags: bug, orm
        :tickets: 7545

        A warning is emitted when attempting to configure a mapped class within an
        inheritance hierarchy where the mapper is not given any polymorphic
        identity, however there is a polymorphic discriminator column assigned.
        Such classes should be abstract if they never intend to load directly.


    .. change::
        :tags: bug, mssql, regression
        :tickets: 8525

        Fixed yet another regression in SQL Server isolation level fetch (see
        :ticket:`8231`, :ticket:`8475`), this time with "Microsoft Dynamics CRM
        Database via Azure Active Directory", which apparently lacks the
        ``system_views`` view entirely. Error catching has been extended that under
        no circumstances will this method ever fail, provided database connectivity
        is present.

    .. change::
        :tags: orm, bug, regression
        :tickets: 8569

        Fixed regression for 1.4 in :func:`_orm.contains_eager` where the "wrap in
        subquery" logic of :func:`_orm.joinedload` would be inadvertently triggered
        for use of the :func:`_orm.contains_eager` function with similar statements
        (e.g. those that use ``distinct()``, ``limit()`` or ``offset()``), which
        would then lead to secondary issues with queries that used some
        combinations of SQL label names and aliasing. This "wrapping" is not
        appropriate for :func:`_orm.contains_eager` which has always had the
        contract that the user-defined SQL statement is unmodified with the
        exception of adding the appropriate columns to be fetched.

    .. change::
        :tags: bug, orm, regression
        :tickets: 8507

        Fixed regression where using ORM update() with synchronize_session='fetch'
        would fail due to the use of evaluators that are now used to determine the
        in-Python value for expressions in the SET clause when refreshing
        objects; if the evaluators make use of math operators against non-numeric
        values such as PostgreSQL JSONB, the non-evaluable condition would fail to
        be detected correctly. The evaluator now limits the use of math mutation
        operators to numeric types only, with the exception of "+" that continues
        to work for strings as well. SQLAlchemy 2.0 may alter this further by
        fetching the SET values completely rather than using evaluation.

    .. change::
        :tags: usecase, postgresql
        :tickets: 8574

        :class:`_postgresql.aggregate_order_by` now supports cache generation.

    .. change::
        :tags: bug, mysql
        :tickets: 8588

        Adjusted the regular expression used to match "CREATE VIEW" when
        testing for views to work more flexibly, no longer requiring the
        special keyword "ALGORITHM" in the middle, which was intended to be
        optional but was not working correctly.  The change allows view reflection
        to work more completely on MySQL-compatible variants such as StarRocks.
        Pull request courtesy John Bodley.

    .. change::
        :tags: bug, engine
        :tickets: 8536

        Fixed issue where mixing "*" with additional explicitly-named column
        expressions within the columns clause of a :func:`_sql.select` construct
        would cause result-column targeting to sometimes consider the label name or
        other non-repeated names to be an ambiguous target.

.. changelog::
    :version: 1.4.41
    :released: September 6, 2022

    .. change::
        :tags: bug, sql
        :tickets: 8441

        Fixed issue where use of the :func:`_sql.table` construct, passing a string
        for the :paramref:`_sql.table.schema` parameter, would fail to take the
        "schema" string into account when producing a cache key, thus leading to
        caching collisions if multiple, same-named :func:`_sql.table` constructs
        with different schemas were used.


    .. change::
        :tags: bug, events, orm
        :tickets: 8467

        Fixed event listening issue where event listeners added to a superclass
        would be lost if a subclass were created which then had its own listeners
        associated. The practical example is that of the :class:`.sessionmaker`
        class created after events have been associated with the
        :class:`_orm.Session` class.

    .. change::
        :tags: orm, bug
        :tickets: 8401

        Hardened the cache key strategy for the :func:`_orm.aliased` and
        :func:`_orm.with_polymorphic` constructs. While no issue involving actual
        statements being cached can easily be demonstrated (if at all), these two
        constructs were not including enough of what makes them unique in their
        cache keys for caching on the aliased construct alone to be accurate.

    .. change::
        :tags: bug, orm, regression
        :tickets: 8456

        Fixed regression appearing in the 1.4 series where a joined-inheritance
        query placed as a subquery within an enclosing query for that same entity
        would fail to render the JOIN correctly for the inner query. The issue
        manifested in two different ways prior and subsequent to version 1.4.18
        (related issue :ticket:`6595`), in one case rendering JOIN twice, in the
        other losing the JOIN entirely. To resolve, the conditions under which
        "polymorphic loading" are applied have been scaled back to not be invoked
        for simple joined inheritance queries.

    .. change::
        :tags: bug, orm
        :tickets: 8446

        Fixed issue in :mod:`sqlalchemy.ext.mutable` extension where collection
        links to the parent object would be lost if the object were merged with
        :meth:`.Session.merge` while also passing :paramref:`.Session.merge.load`
        as False.

    .. change::
        :tags: bug, orm
        :tickets: 8399

        Fixed issue involving :func:`_orm.with_loader_criteria` where a closure
        variable used as bound parameter value within the lambda would not carry
        forward correctly into additional relationship loaders such as
        :func:`_orm.selectinload` and :func:`_orm.lazyload` after the statement
        were cached, using the stale originally-cached value instead.


    .. change::
        :tags: bug, mssql, regression
        :tickets: 8475

        Fixed regression caused by the fix for :ticket:`8231` released in 1.4.40
        where connection would fail if the user did not have permission to query
        the ``dm_exec_sessions`` or ``dm_pdw_nodes_exec_sessions`` system views
        when trying to determine the current transaction isolation level.

    .. change::
        :tags: bug, asyncio
        :tickets: 8419

        Integrated support for asyncpg's ``terminate()`` method call for cases
        where the connection pool is recycling a possibly timed-out connection,
        where a connection is being garbage collected that wasn't gracefully
        closed, as well as when the connection has been invalidated. This allows
        asyncpg to abandon the connection without waiting for a response that may
        incur long timeouts.

.. changelog::
    :version: 1.4.40
    :released: August 8, 2022

    .. change::
        :tags: bug, orm
        :tickets: 8357

        Fixed issue where referencing a CTE multiple times in conjunction with a
        polymorphic SELECT could result in multiple "clones" of the same CTE being
        constructed, which would then trigger these two CTEs as duplicates. To
        resolve, the two CTEs are deep-compared when this occurs to ensure that
        they are equivalent, then are treated as equivalent.


    .. change::
        :tags: bug, orm, declarative
        :tickets: 8190

        Fixed issue where a hierarchy of classes set up as an abstract or mixin
        declarative classes could not declare standalone columns on a superclass
        that would then be copied correctly to a :class:`_orm.declared_attr`
        callable that wanted to make use of them on a descendant class.

    .. change::
        :tags: bug, types
        :tickets: 7249

        Fixed issue where :class:`.TypeDecorator` would not correctly proxy the
        ``__getitem__()`` operator when decorating the :class:`_types.ARRAY`
        datatype, without explicit workarounds.

    .. change::
        :tags: bug, asyncio
        :tickets: 8145

        Added ``asyncio.shield()`` to the connection and session release process
        specifically within the ``__aexit__()`` context manager exit, when using
        :class:`.AsyncConnection` or :class:`.AsyncSession` as a context manager
        that releases the object when the context manager is complete. This appears
        to help with task cancellation when using alternate concurrency libraries
        such as ``anyio``, ``uvloop`` that otherwise don't provide an async context
        for the connection pool to release the connection properly during task
        cancellation.



    .. change::
        :tags: bug, postgresql
        :tickets: 4392

        Fixed issue in psycopg2 dialect where the "multiple hosts" feature
        implemented for :ticket:`4392`, where multiple ``host:port`` pairs could be
        passed in the query string as
        ``?host=host1:port1&host=host2:port2&host=host3:port3`` was not implemented
        correctly, as it did not propagate the "port" parameter appropriately.
        Connections that didn't use a different "port" likely worked without issue,
        and connections that had "port" for some of the entries may have
        incorrectly passed on that hostname. The format is now corrected to pass
        hosts/ports appropriately.

        As part of this change, maintained support for another multihost style that
        worked unintentionally, which is comma-separated
        ``?host=h1,h2,h3&port=p1,p2,p3``. This format is more consistent with
        libpq's query-string format, whereas the previous format is inspired by a
        different aspect of libpq's URI format but is not quite the same thing.

        If the two styles are mixed together, an error is raised as this is
        ambiguous.

    .. change::
        :tags: bug, sql
        :tickets: 8253

        Adjusted the SQL compilation for string containment functions
        ``.contains()``, ``.startswith()``, ``.endswith()`` to force the use of the
        string concatenation operator, rather than relying upon the overload of the
        addition operator, so that non-standard use of these operators with for
        example bytestrings still produces string concatenation operators.


    .. change::
        :tags: bug, orm
        :tickets: 8235

        A :func:`_sql.select` construct that is passed a sole '*' argument for
        ``SELECT *``, either via string, :func:`_sql.text`, or
        :func:`_sql.literal_column`, will be interpreted as a Core-level SQL
        statement rather than as an ORM level statement. This is so that the ``*``,
        when expanded to match any number of columns, will result in all columns
        returned in the result. the ORM- level interpretation of
        :func:`_sql.select` needs to know the names and types of all ORM columns up
        front which can't be achieved when ``'*'`` is used.

        If ``'*`` is used amongst other expressions simultaneously with an ORM
        statement, an error is raised as this can't be interpreted correctly by the
        ORM.

    .. change::
        :tags: bug, mssql
        :tickets: 8210

        Fixed issues that prevented the new usage patterns for using DML with ORM
        objects presented at :ref:`orm_dml_returning_objects` from working
        correctly with the SQL Server pyodbc dialect.


    .. change::
        :tags: bug, mssql
        :tickets: 8231

        Fixed issue where the SQL Server dialect's query for the current isolation
        level would fail on Azure Synapse Analytics, due to the way in which this
        database handles transaction rollbacks after an error has occurred. The
        initial query has been modified to no longer rely upon catching an error
        when attempting to detect the appropriate system view. Additionally, to
        better support this database's very specific "rollback" behavior,
        implemented new parameter ``ignore_no_transaction_on_rollback`` indicating
        that a rollback should ignore Azure Synapse error 'No corresponding
        transaction found. (111214)', which is raised if no transaction is present
        in conflict with the Python DBAPI.

        Initial patch and valuable debugging assistance courtesy of @ww2406.

        .. seealso::

            :ref:`azure_synapse_ignore_no_transaction_on_rollback`

    .. change::
        :tags: bug, mypy
        :tickets: 8196

        Fixed a crash of the mypy plugin when using a lambda as a Column
        default. Pull request courtesy of tchapi.


    .. change::
        :tags: usecase, engine

        Implemented new :paramref:`_engine.Connection.execution_options.yield_per`
        execution option for :class:`_engine.Connection` in Core, to mirror that of
        the same :ref:`yield_per <orm_queryguide_yield_per>` option available in
        the ORM. The option sets both the
        :paramref:`_engine.Connection.execution_options.stream_results` option at
        the same time as invoking :meth:`_engine.Result.yield_per`, to provide the
        most common streaming result configuration which also mirrors that of the
        ORM use case in its usage pattern.

        .. seealso::

            :ref:`engine_stream_results` - revised documentation


    .. change::
        :tags: bug, engine

        Fixed bug in :class:`_engine.Result` where the usage of a buffered result
        strategy would not be used if the dialect in use did not support an
        explicit "server side cursor" setting, when using
        :paramref:`_engine.Connection.execution_options.stream_results`. This is in
        error as DBAPIs such as that of SQLite and Oracle already use a
        non-buffered result fetching scheme, which still benefits from usage of
        partial result fetching.   The "buffered" strategy is now used in all
        cases where :paramref:`_engine.Connection.execution_options.stream_results`
        is set.


    .. change::
        :tags: bug, engine
        :tickets: 8199

        Added :meth:`.FilterResult.yield_per` so that result implementations
        such as :class:`.MappingResult`, :class:`.ScalarResult` and
        :class:`.AsyncResult` have access to this method.

.. changelog::
    :version: 1.4.39
    :released: June 24, 2022

    .. change::
        :tags: bug, orm, regression
        :tickets: 8133

        Fixed regression caused by :ticket:`8133` where the pickle format for
        mutable attributes was changed, without a fallback to recognize the old
        format, causing in-place upgrades of SQLAlchemy to no longer be able to
        read pickled data from previous versions. A check plus a fallback for the
        old format is now in place.

.. changelog::
    :version: 1.4.38
    :released: June 23, 2022

    .. change::
        :tags: bug, orm, regression
        :tickets: 8162

        Fixed regression caused by :ticket:`8064` where a particular check for
        column correspondence was made too liberal, resulting in incorrect
        rendering for some ORM subqueries such as those using
        :meth:`.PropComparator.has` or :meth:`.PropComparator.any` in conjunction
        with joined-inheritance queries that also use legacy aliasing features.

    .. change::
        :tags: bug, engine
        :tickets: 8115

        Repaired a deprecation warning class decorator that was preventing key
        objects such as :class:`_engine.Connection` from having a proper
        ``__weakref__`` attribute, causing operations like Python standard library
        ``inspect.getmembers()`` to fail.


    .. change::
        :tags: bug, sql
        :tickets: 8098

        Fixed multiple observed race conditions related to :func:`.lambda_stmt`,
        including an initial "dogpile" issue when a new Python code object is
        initially analyzed among multiple simultaneous threads which created both a
        performance issue as well as some internal corruption of state.
        Additionally repaired observed race condition which could occur when
        "cloning" an expression construct that is also in the process of being
        compiled or otherwise accessed in a different thread due to memoized
        attributes altering the ``__dict__`` while iterated, for Python versions
        prior to 3.10; in particular the lambda SQL construct is sensitive to this
        as it holds onto a single statement object persistently. The iteration has
        been refined to use ``dict.copy()`` with or without an additional iteration
        instead.

    .. change::
        :tags: bug, sql
        :tickets: 8084

        Enhanced the mechanism of :class:`.Cast` and other "wrapping"
        column constructs to more fully preserve a wrapped :class:`.Label`
        construct, including that the label name will be preserved in the
        ``.c`` collection of a :class:`.Subquery`.  The label was already
        able to render in the SQL correctly on the outside of the construct
        which it was wrapped inside.

    .. change::
        :tags: bug, orm, sql
        :tickets: 8091

        Fixed an issue where :meth:`_sql.GenerativeSelect.fetch` would not
        be applied when executing a statement using the ORM.

    .. change::
        :tags: bug, orm
        :tickets: 8109

        Fixed issue where a :func:`_orm.with_loader_criteria` option could not be
        pickled, as is necessary when it is carried along for propagation to lazy
        loaders in conjunction with a caching scheme. Currently, the only form that
        is supported as picklable is to pass the "where criteria" as a fixed
        module-level callable function that produces a SQL expression. An ad-hoc
        "lambda" can't be pickled, and a SQL expression object is usually not fully
        picklable directly.


    .. change::
        :tags: bug, schema
        :tickets: 8100, 8101

        Fixed bugs involving the :paramref:`.Table.include_columns` and the
        :paramref:`.Table.resolve_fks` parameters on :class:`.Table`; these
        little-used parameters were apparently not working for columns that refer
        to foreign key constraints.

        In the first case, not-included columns that refer to foreign keys would
        still attempt to create a :class:`.ForeignKey` object, producing errors
        when attempting to resolve the columns for the foreign key constraint
        within reflection; foreign key constraints that refer to skipped columns
        are now omitted from the table reflection process in the same way as
        occurs for :class:`.Index` and :class:`.UniqueConstraint` objects with the
        same conditions. No warning is produced however, as we likely want to
        remove the include_columns warnings for all constraints in 2.0.

        In the latter case, the production of table aliases or subqueries would
        fail on an FK related table not found despite the presence of
        ``resolve_fks=False``; the logic has been repaired so that if a related
        table is not found, the :class:`.ForeignKey` object is still proxied to the
        aliased table or subquery (these :class:`.ForeignKey` objects are normally
        used in the production of join conditions), but it is sent with a flag that
        it's not resolvable. The aliased table / subquery will then work normally,
        with the exception that it cannot be used to generate a join condition
        automatically, as the foreign key information is missing. This was already
        the behavior for such foreign key constraints produced using non-reflection
        methods, such as joining :class:`.Table` objects from different
        :class:`.MetaData` collections.

    .. change::
        :tags: bug, sql
        :tickets: 8113

        Adjusted the fix made for :ticket:`8056` which adjusted the escaping of
        bound parameter names with special characters such that the escaped names
        were translated after the SQL compilation step, which broke a published
        recipe on the FAQ illustrating how to merge parameter names into the string
        output of a compiled SQL string. The change restores the escaped names that
        come from ``compiled.params`` and adds a conditional parameter to
        :meth:`.SQLCompiler.construct_params` named ``escape_names`` that defaults
        to ``True``, restoring the old behavior by default.

    .. change::
        :tags: bug, schema, mssql
        :tickets: 8111

        Fixed issue where :class:`.Table` objects that made use of IDENTITY columns
        with a :class:`.Numeric` datatype would produce errors when attempting to
        reconcile the "autoincrement" column, preventing construction of the
        :class:`.Column` from using the :paramref:`.Column.autoincrement` parameter
        as well as emitting errors when attempting to invoke an :class:`_dml.Insert`
        construct.


    .. change::
        :tags: bug, extensions
        :tickets: 8133

        Fixed bug in :class:`.Mutable` where pickling and unpickling of an ORM
        mapped instance would not correctly restore state for mappings that
        contained multiple :class:`.Mutable`-enabled attributes.

.. changelog::
    :version: 1.4.37
    :released: May 31, 2022

    .. change::
        :tags: bug, mssql
        :tickets: 8062

        Fix issue where a password with a leading "{" would result in login failure.

    .. change::
        :tags: bug, sql, postgresql, sqlite
        :tickets: 8014

        Fixed bug where the PostgreSQL
        :meth:`_postgresql.Insert.on_conflict_do_update` method and the SQLite
        :meth:`_sqlite.Insert.on_conflict_do_update` method would both fail to
        correctly accommodate a column with a separate ".key" when specifying the
        column using its key name in the dictionary passed to
        :paramref:`_postgresql.Insert.on_conflict_do_update.set_`, as well as if
        the :attr:`_postgresql.Insert.excluded` collection were used as the
        dictionary directly.

    .. change::
        :tags: bug, sql
        :tickets: 8073

        An informative error is raised for the use case where
        :meth:`_dml.Insert.from_select` is being passed a "compound select" object such
        as a UNION, yet the INSERT statement needs to append additional columns to
        support Python-side or explicit SQL defaults from the table metadata. In
        this case a subquery of the compound object should be passed.

    .. change::
        :tags: bug, orm
        :tickets: 8064

        Fixed issue where using a :func:`_orm.column_property` construct containing
        a subquery against an already-mapped column attribute would not correctly
        apply ORM-compilation behaviors to the subquery, including that the "IN"
        expression added for a single-table inherits expression would fail to be
        included.

    .. change::
        :tags: bug, orm
        :tickets: 8001

        Fixed issue where ORM results would apply incorrect key names to the
        returned :class:`.Row` objects in the case where the set of columns to be
        selected were changed, such as when using
        :meth:`.Select.with_only_columns`.

    .. change::
        :tags: bug, mysql
        :tickets: 7966

        Further adjustments to the MySQL PyODBC dialect to allow for complete
        connectivity, which was previously still not working despite fixes in
        :ticket:`7871`.

    .. change::
        :tags: bug, sql
        :tickets: 7979

        Fixed an issue where using :func:`.bindparam` with no explicit data or type
        given could be coerced into the incorrect type when used in expressions
        such as when using :meth:`_types.ARRAY.Comparator.any` and
        :meth:`_types.ARRAY.Comparator.all`.


    .. change::
        :tags: bug, oracle
        :tickets: 8053

        Fixed SQL compiler issue where the "bind processing" function for a bound
        parameter would not be correctly applied to a bound value if the bound
        parameter's name were "escaped". Concretely, this applies, among other
        cases, to Oracle when a :class:`.Column` has a name that itself requires
        quoting, such that the quoting-required name is then used for the bound
        parameters generated within DML statements, and the datatype in use
        requires bind processing, such as the :class:`.Enum` datatype.

    .. change::
        :tags: bug, mssql, reflection
        :tickets: 8035

        Explicitly specify the collation when reflecting table columns using
        MSSQL to prevent "collation conflict" errors.

    .. change::
        :tags: bug, orm, oracle, postgresql
        :tickets: 8056

        Fixed bug, likely a regression from 1.3, where usage of column names that
        require bound parameter escaping, more concretely when using Oracle with
        column names that require quoting such as those that start with an
        underscore, or in less common cases with some PostgreSQL drivers when using
        column names that contain percent signs, would cause the ORM versioning
        feature to not work correctly if the versioning column itself had such a
        name, as the ORM assumes certain bound parameter naming conventions that
        were being interfered with via the quotes. This issue is related to
        :ticket:`8053` and essentially revises the approach towards fixing this,
        revising the original issue :ticket:`5653` that created the initial
        implementation for generalized bound-parameter name quoting.

    .. change::
        :tags: bug, mysql
        :tickets: 8036

        Added disconnect code for MySQL error 4031, introduced in MySQL >= 8.0.24,
        indicating connection idle timeout exceeded. In particular this repairs an
        issue where pre-ping could not reconnect on a timed-out connection. Pull
        request courtesy valievkarim.

    .. change::
        :tags: bug, sql
        :tickets: 8018

        An informative error is raised if two individual :class:`.BindParameter`
        objects share the same name, yet one is used within an "expanding" context
        (typically an IN expression) and the other is not; mixing the same name in
        these two different styles of usage is not supported and typically the
        ``expanding=True`` parameter should be set on the parameters that are to
        receive list values outside of IN expressions (where ``expanding`` is set
        by default).

    .. change::
        :tags: bug, engine, tests
        :tickets: 8019

        Fixed issue where support for logging "stacklevel" implemented in
        :ticket:`7612` required adjustment to work with recently released Python
        3.11.0b1, also repairs the unit tests which tested this feature.

    .. change::
        :tags: usecase, oracle
        :tickets: 8066

        Added two new error codes for Oracle disconnect handling to support early
        testing of the new "python-oracledb" driver released by Oracle.

.. changelog::
    :version: 1.4.36
    :released: April 26, 2022

    .. change::
        :tags: bug, mysql, regression
        :tickets: 7871

        Fixed a regression in the untested MySQL PyODBC dialect caused by the fix
        for :ticket:`7518` in version 1.4.32 where an argument was being propagated
        incorrectly upon first connect, leading to a ``TypeError``.

    .. change::
        :tags: bug, orm, regression
        :tickets: 7936

        Fixed regression where the change made for :ticket:`7861`, released in
        version 1.4.33, that brought the :class:`_sql.Insert` construct to be partially
        recognized as an ORM-enabled statement did not properly transfer the
        correct mapper / mapped table state to the :class:`.Session`, causing the
        :meth:`.Session.get_bind` method to fail for a :class:`.Session` that was
        bound to engines and/or connections using the :paramref:`.Session.binds`
        parameter.

    .. change::
        :tags: bug, engine
        :tickets: 7875

        Fixed a memory leak in the C extensions which could occur when calling upon
        named members of :class:`.Row` when the member does not exist under Python
        3; in particular this could occur during NumPy transformations when it
        attempts to call members such as ``.__array__``, but the issue was
        surrounding any ``AttributeError`` thrown by the :class:`.Row` object. This
        issue does not apply to version 2.0 which has already transitioned to
        Cython. Thanks much to Sebastian Berg for identifying the problem.


    .. change::
        :tags: bug, postgresql
        :tickets: 6515

        Fixed bug in :class:`_sqltypes.ARRAY` datatype in combination with :class:`.Enum` on
        PostgreSQL where using the ``.any()`` or ``.all()`` methods to render SQL
        ANY() or ALL(), given members of the Python enumeration as arguments, would
        produce a type adaptation failure on all drivers.

    .. change::
        :tags: bug, postgresql
        :tickets: 7943

        Implemented :attr:`_postgresql.UUID.python_type` attribute for the
        PostgreSQL :class:`_postgresql.UUID` type object. The attribute will return
        either ``str`` or ``uuid.UUID`` based on the
        :paramref:`_postgresql.UUID.as_uuid` parameter setting. Previously, this
        attribute was unimplemented. Pull request courtesy Alex Grönholm.

    .. change::
        :tags: bug, tests
        :tickets: 7919

        For third party dialects, repaired a missing requirement for the
        ``SimpleUpdateDeleteTest`` suite test which was not checking for a working
        "rowcount" function on the target dialect.


    .. change::
        :tags: bug, postgresql
        :tickets: 7930

        Fixed an issue in the psycopg2 dialect when using the
        :paramref:`_sa.create_engine.pool_pre_ping` parameter which would cause
        user-configured ``AUTOCOMMIT`` isolation level to be inadvertently reset by
        the "ping" handler.

    .. change::
        :tags: bug, asyncio
        :tickets: 7937

        Repaired handling of ``contextvar.ContextVar`` objects inside of async
        adapted event handlers. Previously, values applied to a ``ContextVar``
        would not be propagated in the specific case of calling upon awaitables
        inside of non-awaitable code.


    .. change::
        :tags: bug, engine
        :tickets: 7953

        Added a warning regarding a bug which exists in the :meth:`_result.Result.columns`
        method when passing 0 for the index in conjunction with a :class:`_result.Result`
        that will return a single ORM entity, which indicates that the current
        behavior of :meth:`_result.Result.columns` is broken in this case as the
        :class:`_result.Result` object will yield scalar values and not :class:`.Row`
        objects. The issue will be fixed in 2.0, which would be a
        backwards-incompatible change for code that relies on the current broken
        behavior. Code which wants to receive a collection of scalar values should
        use the :meth:`_result.Result.scalars` method, which will return a new
        :class:`.ScalarResult` object that yields non-row scalar objects.


    .. change::
        :tags: bug, schema
        :tickets: 7958

        Fixed bug where :class:`.ForeignKeyConstraint` naming conventions using the
        ``referred_column_0`` naming convention key would not work if the foreign
        key constraint were set up as a :class:`.ForeignKey` object rather than an
        explicit :class:`.ForeignKeyConstraint` object. As this change makes use of
        a backport of some fixes from version 2.0, an additional little-known
        feature that has likely been broken for many years is also fixed which is
        that a :class:`.ForeignKey` object may refer to a referred table by name of
        the table alone without using a column name, if the name of the referent
        column is the same as that of the referred column.

        The ``referred_column_0`` naming convention key was previously not tested
        with the :class:`.ForeignKey` object, only :class:`.ForeignKeyConstraint`,
        and this bug reveals that the feature has never worked correctly unless
        :class:`.ForeignKeyConstraint` is used for all FK constraints. This bug
        traces back to the original introduction of the feature introduced for
        :ticket:`3989`.

    .. change::
        :tags: bug, orm, declarative
        :tickets: 7900

        Modified the :class:`.DeclarativeMeta` metaclass to pass ``cls.__dict__``
        into the declarative scanning process to look for attributes, rather than
        the separate dictionary passed to the type's ``__init__()`` method. This
        allows user-defined base classes that add attributes within an
        ``__init_subclass__()`` to work as expected, as ``__init_subclass__()`` can
        only affect the ``cls.__dict__`` itself and not the other dictionary. This
        is technically a regression from 1.3 where ``__dict__`` was being used.




.. changelog::
    :version: 1.4.35
    :released: April 6, 2022

    .. change::
        :tags: bug, sql
        :tickets: 7890

        Fixed bug in newly implemented
        :paramref:`.FunctionElement.table_valued.joins_implicitly` feature where
        the parameter would not automatically propagate from the original
        :class:`.TableValuedAlias` object to the secondary object produced when
        calling upon :meth:`.TableValuedAlias.render_derived` or
        :meth:`.TableValuedAlias.alias`.

        Additionally repaired these issues in :class:`.TableValuedAlias`:

        * repaired a potential memory issue which could occur when
          repeatedly calling :meth:`.TableValuedAlias.render_derived` against
          successive copies of the same object (for .alias(), we currently
          have to still continue chaining from the previous element.  not sure
          if this can be improved but this is standard behavior for .alias()
          elsewhere)
        * repaired issue where the individual element types would be lost when
          calling upon :meth:`.TableValuedAlias.render_derived` or
          :meth:`.TableValuedAlias.alias`.

    .. change::
        :tags: bug, sql, regression
        :tickets: 7903

        Fixed regression caused by :ticket:`7823` which impacted the caching
        system, such that bound parameters that had been "cloned" within ORM
        operations, such as polymorphic loading, would in some cases not acquire
        their correct execution-time value leading to incorrect bind values being
        rendered.

.. changelog::
    :version: 1.4.34
    :released: March 31, 2022

    .. change::
        :tags: bug, orm, regression
        :tickets: 7878

        Fixed regression caused by :ticket:`7861` where invoking an
        :class:`_sql.Insert` construct which contained ORM entities directly via
        :meth:`_orm.Session.execute` would fail.

    .. change::
        :tags: bug, postgresql
        :tickets: 7880

        Scaled back a fix made for :ticket:`6581` where "executemany values" mode
        for psycopg2 were disabled for all "ON CONFLICT" styles of INSERT, to
        not apply to the "ON CONFLICT DO NOTHING" clause, which does not include
        any parameters and is safe for "executemany values" mode.  "ON CONFLICT
        DO UPDATE" is still blocked from "executemany values" as there may
        be additional parameters in the DO UPDATE clause that cannot be batched
        (which is the original issue fixed by :ticket:`6581`).

.. changelog::
    :version: 1.4.33
    :released: March 31, 2022

    .. change::
        :tags: bug, engine
        :tickets: 7853

        Further clarified connection-level logging to indicate the BEGIN, ROLLBACK
        and COMMIT log messages do not actually indicate a real transaction when
        the AUTOCOMMIT isolation level is in use; messaging has been extended to
        include the BEGIN message itself, and the messaging has also been fixed to
        accommodate when the :class:`_engine.Engine` level
        :paramref:`_sa.create_engine.isolation_level` parameter was used directly.

    .. change::
        :tags: bug, mssql, regression
        :tickets: 7812

        Fixed regression caused by :ticket:`7160` where FK reflection in
        conjunction with a low compatibility level setting (compatibility level 80:
        SQL Server 2000) causes an "Ambiguous column name" error. Patch courtesy
        @Lin-Your.

    .. change::
        :tags: usecase, schema
        :tickets: 7860

        Added support so that the :paramref:`.Table.to_metadata.referred_schema_fn`
        callable passed to :meth:`.Table.to_metadata` may return the value
        :attr:`.BLANK_SCHEMA` to indicate that the referenced foreign key should be
        reset to None. The :attr:`.RETAIN_SCHEMA` symbol may also be returned from
        this function to indicate "no change", which will behave the same as
        ``None`` currently does which also indicates no change.


    .. change::
        :tags: bug, sqlite, reflection
        :tickets: 5463

        Fixed bug where the name of CHECK constraints under SQLite would not be
        reflected if the name were created using quotes, as is the case when the
        name uses mixed case or special characters.


    .. change::
        :tags: bug, orm, regression
        :tickets: 7868

        Fixed regression in "dynamic" loader strategy where the
        :meth:`_orm.Query.filter_by` method would not be given an appropriate
        entity to filter from, in the case where a "secondary" table were present
        in the relationship being queried and the mapping were against something
        complex such as a "with polymorphic".

    .. change::
        :tags: bug, orm
        :tickets: 7801

        Fixed bug where :func:`_orm.composite` attributes would not work in
        conjunction with the :func:`_orm.selectin_polymorphic` loader strategy for
        joined table inheritance.


    .. change::
        :tags: bug, orm, performance
        :tickets: 7823

        Improvements in memory usage by the ORM, removing a significant set of
        intermediary expression objects that are typically stored when a copy of an
        expression object is created. These clones have been greatly reduced,
        reducing the number of total expression objects stored in memory by
        ORM mappings by about 30%.

    .. change::
        :tags: usecase, orm
        :tickets: 7805

        Added :paramref:`_orm.with_polymorphic.adapt_on_names` to the
        :func:`_orm.with_polymorphic` function, which allows a polymorphic load
        (typically with concrete mapping) to be stated against an alternative
        selectable that will adapt to the original mapped selectable on column
        names alone.

    .. change::
        :tags: usecase, sql
        :tickets: 7845

        Added new parameter
        :paramref:`.FunctionElement.table_valued.joins_implicitly`, for the
        :meth:`.FunctionElement.table_valued` construct. This parameter
        indicates that the table-valued function provided will automatically
        perform an implicit join with the referenced table. This effectively
        disables the 'from linting' feature, such as the 'cartesian product'
        warning, from triggering due to the presence of this parameter.  May be
        used for functions such as ``func.json_each()``.

    .. change::
        :tags: usecase, engine
        :tickets: 7877, 7815

        Added new parameter :paramref:`_engine.Engine.dispose.close`, defaulting to True.
        When False, the engine disposal does not touch the connections in the old
        pool at all, simply dropping the pool and replacing it. This use case is so
        that when the original pool is transferred from a parent process, the
        parent process may continue to use those connections.

        .. seealso::

            :ref:`pooling_multiprocessing` - revised documentation

    .. change::
        :tags: bug, orm
        :tickets: 7799

        Fixed issue where the :func:`_orm.selectin_polymorphic` loader option would
        not work with joined inheritance mappers that don't have a fixed
        "polymorphic_on" column.   Additionally added test support for a wider
        variety of usage patterns with this construct.

    .. change::
        :tags: usecase, orm
        :tickets: 7861

        Added new attributes :attr:`.UpdateBase.returning_column_descriptions` and
        :attr:`.UpdateBase.entity_description` to allow for inspection of ORM
        attributes and entities that are installed as part of an :class:`_sql.Insert`,
        :class:`.Update`, or :class:`.Delete` construct. The
        :attr:`.Select.column_descriptions` accessor is also now implemented for
        Core-only selectables.

    .. change::
        :tags: bug, sql
        :tickets: 7876

        The :paramref:`.bindparam.literal_execute` parameter now takes part
        of the cache generation of a :func:`.bindparam`, since it changes
        the sql string generated by the compiler.
        Previously the correct bind values were used, but the ``literal_execute``
        would be ignored on subsequent executions of the same query.

    .. change::
        :tags: bug, orm
        :tickets: 7862

        Fixed bug in :func:`_orm.with_loader_criteria` function where loader
        criteria would not be applied to a joined eager load that were invoked
        within the scope of a refresh operation for the parent object.

    .. change::
        :tags: bug, orm
        :tickets: 7842

        Fixed issue where the :class:`_orm.Mapper` would reduce a user-defined
        :paramref:`_orm.Mapper.primary_key` argument too aggressively, in the case
        of mapping to a ``UNION`` where for some of the SELECT entries, two columns
        are essentially equivalent, but in another, they are not, such as in a
        recursive CTE. The logic here has been changed to accept a given
        user-defined PK as given, where columns will be related to the mapped
        selectable but no longer "reduced" as this heuristic can't accommodate for
        all situations.

    .. change::
        :tags: bug, ext
        :tickets: 7827

        Improved the error message that's raised for the case where the
        :func:`.association_proxy` construct attempts to access a target attribute
        at the class level, and this access fails. The particular use case here is
        when proxying to a hybrid attribute that does not include a working
        class-level implementation.


    .. change::
        :tags: bug, sql, regression
        :tickets: 7798

        Fixed regression caused by :ticket:`7760` where the new capabilities of
        :class:`.TextualSelect` were not fully implemented within the compiler
        properly, leading to issues with composed INSERT constructs such as "INSERT
        FROM SELECT" and "INSERT...ON CONFLICT" when combined with CTE and textual
        statements.

.. changelog::
    :version: 1.4.32
    :released: March 6, 2022

    .. change::
        :tags: bug, sql
        :tickets: 7721

        Fixed type-related error messages that would fail for values that were
        tuples, due to string formatting syntax, including compile of unsupported
        literal values and invalid boolean values.

    .. change::
        :tags: bug, sql, mysql
        :tickets: 7720, 7789, 7598

        Fixed issues in MySQL :class:`_mysql.SET` datatype as well as the generic
        :class:`.Enum` datatype where the ``__repr__()`` method would not render
        all optional parameters in the string output, impacting the use of these
        types in Alembic autogenerate. Pull request for MySQL courtesy Yuki
        Nishimine.


    .. change::
        :tags: bug, sqlite
        :tickets: 7736

        Fixed issue where SQLite unique constraint reflection would fail to detect
        a column-inline UNIQUE constraint where the column name had an underscore
        in its name.

    .. change::
        :tags: usecase, sqlite
        :tickets: 7736

        Added support for reflecting SQLite inline unique constraints where
        the column names are formatted with SQLite "escape quotes" ``[]``
        or `````, which are discarded by the database when producing the
        column name.

    .. change::
        :tags: bug, oracle
        :tickets: 7676

        Fixed issue in Oracle dialect where using a column name that requires
        quoting when written as a bound parameter, such as ``"_id"``, would not
        correctly track a Python generated default value due to the bound-parameter
        rewriting missing this value, causing an Oracle error to be raised.

    .. change::
        :tags: bug, tests
        :tickets: 7599

        Improvements to the test suite's integration with pytest such that the
        "warnings" plugin, if manually enabled, will not interfere with the test
        suite, such that third parties can enable the warnings plugin or make use
        of the ``-W`` parameter and SQLAlchemy's test suite will continue to pass.
        Additionally, modernized the detection of the "pytest-xdist" plugin so that
        plugins can be globally disabled using PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
        without breaking the test suite if xdist were still installed. Warning
        filters that promote deprecation warnings to errors are now localized to
        SQLAlchemy-specific warnings, or within SQLAlchemy-specific sources for
        general Python deprecation warnings, so that non-SQLAlchemy deprecation
        warnings emitted from pytest plugins should also not impact the test suite.


    .. change::
        :tags: bug, sql

        The :class:`_sqltypes.Enum` datatype now emits a warning if the
        :paramref:`_sqltypes.Enum.length` argument is specified without also
        specifying :paramref:`_sqltypes.Enum.native_enum` as False, as the
        parameter is otherwise silently ignored in this case, despite the fact that
        the :class:`_sqltypes.Enum` datatype will still render VARCHAR DDL on
        backends that don't have a native ENUM datatype such as SQLite. This
        behavior may change in a future release so that "length" is honored for all
        non-native "enum" types regardless of the "native_enum" setting.


    .. change::
        :tags: bug, mysql, regression
        :tickets: 7518

        Fixed regression caused by :ticket:`7518` where changing the syntax "SHOW
        VARIABLES" to "SELECT @@" broke compatibility with MySQL versions older
        than 5.6, including early 5.0 releases. While these are very old MySQL
        versions, a change in compatibility was not planned, so version-specific
        logic has been restored to fall back to "SHOW VARIABLES" for MySQL server
        versions < 5.6.

    .. change::
        :tags: bug, asyncio

        Fixed issues where a descriptive error message was not raised for some
        classes of event listening with an async engine, which should instead be a
        sync engine instance.

    .. change::
        :tags: bug, mariadb, regression
        :tickets: 7738

        Fixed regression in mariadbconnector dialect as of mariadb connector 1.0.10
        where the DBAPI no longer pre-buffers cursor.lastrowid, leading to errors
        when inserting objects with the ORM as well as causing non-availability of
        the :attr:`_result.CursorResult.inserted_primary_key` attribute. The
        dialect now fetches this value proactively for situations where it applies.

    .. change::
        :tags: usecase, postgresql
        :tickets: 7600

        Added compiler support for the PostgreSQL ``NOT VALID`` phrase when rendering
        DDL for the :class:`.CheckConstraint`, :class:`.ForeignKeyConstraint`
        and :class:`.ForeignKey` schema constructs.  Pull request courtesy
        Gilbert Gilb's.

        .. seealso::

            :ref:`postgresql_constraint_options`

    .. change::
        :tags: bug, orm, regression
        :tickets: 7594

        Fixed regression where the ORM exception that is to be raised when an
        INSERT silently fails to actually insert a row (such as from a trigger)
        would not be reached, due to a runtime exception raised ahead of time due
        to the missing primary key value, thus raising an uninformative exception
        rather than the correct one. For 1.4 and above, a new
        :class:`_ormexc.FlushError` is added for this case that's raised earlier
        than the previous "null identity" exception was for 1.3, as a situation
        where the number of rows actually INSERTed does not match what was expected
        is a more critical situation in 1.4 as it prevents batching of multiple
        objects from working correctly. This is separate from the case where a
        newly fetched primary key is fetched as NULL, which continues to raise the
        existing "null identity" exception.

    .. change::
        :tags: bug, tests
        :tickets: 7045

        Made corrections to the default pytest configuration regarding how test
        discovery is configured, to fix issue where the test suite would not
        configure warnings correctly and also attempt to load example suites as
        tests, in the specific case where the SQLAlchemy checkout were located in
        an absolute path that had a super-directory named "test".

    .. change::
        :tags: bug, orm
        :tickets: 7697

        Fixed issue where using a fully qualified path for the classname in
        :func:`_orm.relationship` that nonetheless contained an incorrect name for
        path tokens that were not the first token, would fail to raise an
        informative error and would instead fail randomly at a later step.

    .. change::
        :tags: bug, oracle, regression
        :tickets: 7748

        Added support to parse "DPI" error codes from cx_Oracle exception objects
        such as ``DPI-1080`` and ``DPI-1010``, both of which now indicate a
        disconnect scenario as of cx_Oracle 8.3.

    .. change::
        :tags: bug, sql
        :tickets: 7760

        Fixed issue where the :meth:`.HasCTE.add_cte` method as called upon a
        :class:`.TextualSelect` instance was not being accommodated by the SQL
        compiler. The fix additionally adds more "SELECT"-like compiler behavior to
        :class:`.TextualSelect` including that DML CTEs such as UPDATE and INSERT
        may be accommodated.

    .. change::
        :tags: bug, engine
        :tickets: 7612

        Adjusted the logging for key SQLAlchemy components including
        :class:`_engine.Engine`, :class:`_engine.Connection` to establish an
        appropriate stack level parameter, so that the Python logging tokens
        ``funcName`` and ``lineno`` when used in custom logging formatters will
        report the correct information, which can be useful when filtering log
        output; supported on Python 3.8 and above. Pull request courtesy Markus
        Gerstel.

    .. change::
        :tags: bug, asyncio
        :tickets: 7667

        Fixed issue where the :meth:`_asyncio.AsyncSession.execute` method failed
        to raise an informative exception if the
        :paramref:`_engine.Connection.execution_options.stream_results` execution
        option were used, which is incompatible with a sync-style
        :class:`_result.Result` object when using an asyncio calling style, as the
        operation to fetch more rows would need to be awaited. An exception is now
        raised in this scenario in the same way one was already raised when the
        :paramref:`_engine.Connection.execution_options.stream_results` option
        would be used with the :meth:`_asyncio.AsyncConnection.execute` method.

        Additionally, for improved stability with state-sensitive database drivers
        such as asyncmy, the cursor is now closed when this error condition is
        raised; previously with the asyncmy dialect, the connection would go into
        an invalid state with unconsumed server side results remaining.


.. changelog::
    :version: 1.4.31
    :released: January 20, 2022

    .. change::
        :tags: bug, postgresql, regression
        :tickets: 7590

        Fixed regression where the change in :ticket:`7148` to repair ENUM handling
        in PostgreSQL broke the use case of an empty ARRAY of ENUM, preventing rows
        that contained an empty array from being handled correctly when fetching
        results.

    .. change::
        :tags: bug, orm
        :tickets: 7591

        Fixed issue in :meth:`_orm.Session.bulk_save_objects` where the sorting
        that takes place when the ``preserve_order`` parameter is set to False
        would sort partially on ``Mapper`` objects, which is rejected in Python
        3.11.


    .. change::
        :tags: bug, mysql, regression
        :tickets: 7593

        Fixed regression in asyncmy dialect caused by :ticket:`7567` where removal
        of the PyMySQL dependency broke binary columns, due to the asyncmy dialect
        not being properly included within CI tests.

    .. change::
        :tags: mssql
        :tickets: 7243

        Added support for ``FILESTREAM`` when using ``VARBINARY(max)``
        in MSSQL.

        .. seealso::

            :paramref:`_mssql.VARBINARY.filestream`

.. changelog::
    :version: 1.4.30
    :released: January 19, 2022

    .. change::
        :tags: usecase, asyncio
        :tickets: 7580

        Added new method :meth:`.AdaptedConnection.run_async` to the DBAPI
        connection interface used by asyncio drivers, which allows methods to be
        called against the underlying "driver" connection directly within a
        sync-style function where the ``await`` keyword can't be used, such as
        within SQLAlchemy event handler functions. The method is analogous to the
        :meth:`_asyncio.AsyncConnection.run_sync` method which translates
        async-style calls to sync-style. The method is useful for things like
        connection-pool on-connect handlers that need to invoke awaitable methods
        on the driver connection when it's first created.

        .. seealso::

            :ref:`asyncio_events_run_async`


    .. change::
        :tags: bug, orm
        :tickets: 7507

        Fixed issue in joined-inheritance load of additional attributes
        functionality in deep multi-level inheritance where an intermediary table
        that contained no columns would not be included in the tables joined,
        instead linking those tables to their primary key identifiers. While this
        works fine, it nonetheless in 1.4 began producing the cartesian product
        compiler warning. The logic has been changed so that these intermediary
        tables are included regardless. While this does include additional tables
        in the query that are not technically necessary, this only occurs for the
        highly unusual case of deep 3+ level inheritance with intermediary tables
        that have no non primary key columns, potential performance impact is
        therefore expected to be negligible.

    .. change::
        :tags: bug, orm
        :tickets: 7579

        Fixed issue where calling upon :meth:`_orm.registry.map_imperatively` more
        than once for the same class would produce an unexpected error, rather than
        an informative error that the target class is already mapped. This behavior
        differed from that of the :func:`_orm.mapper` function which does report an
        informative message already.

    .. change::
        :tags: bug, sql, postgresql
        :tickets: 7537

        Added additional rule to the system that determines ``TypeEngine``
        implementations from Python literals to apply a second level of adjustment
        to the type, so that a Python datetime with or without tzinfo can set the
        ``timezone=True`` parameter on the returned :class:`.DateTime` object, as
        well as :class:`.Time`. This helps with some round-trip scenarios on
        type-sensitive PostgreSQL dialects such as asyncpg, psycopg3 (2.0 only).

    .. change::
        :tags: bug, postgresql, asyncpg
        :tickets: 7537

        Improved support for asyncpg handling of TIME WITH TIMEZONE, which
        was not fully implemented.

    .. change::
        :tags: usecase, postgresql
        :tickets: 7561

        Added string rendering to the :class:`.postgresql.UUID` datatype, so that
        stringifying a statement with "literal_binds" that uses this type will
        render an appropriate string value for the PostgreSQL backend. Pull request
        courtesy José Duarte.

    .. change::
        :tags: bug, orm, asyncio
        :tickets: 7524

        Added missing method :meth:`_asyncio.AsyncSession.invalidate` to the
        :class:`_asyncio.AsyncSession` class.


    .. change::
        :tags: bug, orm, regression
        :tickets: 7557

        Fixed regression which appeared in 1.4.23 which could cause loader options
        to be mis-handled in some cases, in particular when using joined table
        inheritance in combination with the ``polymorphic_load="selectin"`` option
        as well as relationship lazy loading, leading to a ``TypeError``.


    .. change::
        :tags: bug, mypy
        :tickets: 7321

        Fixed Mypy crash when running id daemon mode caused by a
        missing attribute on an internal mypy ``Var`` instance.

    .. change::
        :tags: change, mysql
        :tickets: 7518

        Replace ``SHOW VARIABLES LIKE`` statement with equivalent
        ``SELECT @@variable`` in MySQL and MariaDB dialect initialization.
        This should avoid mutex contention caused by ``SHOW VARIABLES``,
        improving initialization performance.

    .. change::
        :tags: bug, orm, regression
        :tickets: 7576

        Fixed ORM regression where calling the :func:`_orm.aliased` function
        against an existing :func:`_orm.aliased` construct would fail to produce
        correct SQL if the existing construct were against a fixed table. The fix
        allows that the original :func:`_orm.aliased` construct is disregarded if
        it were only against a table that's now being replaced. It also allows for
        correct behavior when constructing a :func:`_orm.aliased` without a
        selectable argument against a :func:`_orm.aliased` that's against a
        subquery, to create an alias of that subquery (i.e. to change its name).

        The nesting behavior of :func:`_orm.aliased` remains in place for the case
        where the outer :func:`_orm.aliased` object is against a subquery which in
        turn refers to the inner :func:`_orm.aliased` object. This is a relatively
        new 1.4 feature that helps to suit use cases that were previously served by
        the deprecated ``Query.from_self()`` method.

    .. change::
        :tags: bug, orm
        :tickets: 7514

        Fixed issue where :meth:`_sql.Select.correlate_except` method, when passed
        either the ``None`` value or no arguments, would not correlate any elements
        when used in an ORM context (that is, passing ORM entities as FROM
        clauses), rather than causing all FROM elements to be considered as
        "correlated" in the same way which occurs when using Core-only constructs.

    .. change::
        :tags: bug, orm, regression
        :tickets: 7505

        Fixed regression from 1.3 where the "subqueryload" loader strategy would
        fail with a stack trace if used against a query that made use of
        :meth:`_orm.Query.from_statement` or :meth:`_sql.Select.from_statement`. As
        subqueryload requires modifying the original statement, it's not compatible
        with the "from_statement" use case, especially for statements made against
        the :func:`_sql.text` construct. The behavior now is equivalent to that of
        1.3 and previously, which is that the loader strategy silently degrades to
        not be used for such statements, typically falling back to using the
        lazyload strategy.


    .. change::
        :tags: bug, reflection, postgresql, mssql
        :tickets: 7382

        Fixed reflection of covering indexes to report ``include_columns`` as part
        of the ``dialect_options`` entry in the reflected index dictionary, thereby
        enabling round trips from reflection->create to be complete. Included
        columns continue to also be present under the ``include_columns`` key for
        backwards compatibility.

    .. change::
        :tags: bug, mysql
        :tickets: 7567

        Removed unnecessary dependency on PyMySQL from the asyncmy dialect. Pull
        request courtesy long2ice.


    .. change::
        :tags: bug, postgresql
        :tickets: 7418

        Fixed handling of array of enum values which require escape characters.

    .. change::
        :tags: bug, sql
        :tickets: 7032

        Added an informative error message when a method object is passed to a SQL
        construct. Previously, when such a callable were passed, as is a common
        typographical error when dealing with method-chained SQL constructs, they
        were interpreted as "lambda SQL" targets to be invoked at compilation time,
        which would lead to silent failures. As this feature was not intended to be
        used with methods, method objects are now rejected.

.. changelog::
    :version: 1.4.29
    :released: December 22, 2021

    .. change::
        :tags: usecase, asyncio
        :tickets: 7301

        Added :func:`_asyncio.async_engine_config` function to create
        an async engine from a configuration dict.  This otherwise
        behaves the same as :func:`_sa.engine_from_config`.

    .. change::
        :tags: bug, orm
        :tickets: 7489

        Fixed issue in new "loader criteria" method
        :meth:`_orm.PropComparator.and_` where usage with a loader strategy like
        :func:`_orm.selectinload` against a column that was a member of the ``.c.``
        collection of a subquery object, where the subquery would be dynamically
        added to the FROM clause of the statement, would be subject to stale
        parameter values within the subquery in the SQL statement cache, as the
        process used by the loader strategy to replace the parameters at execution
        time would fail to accommodate the subquery when received in this form.


    .. change::
        :tags: bug, orm
        :tickets: 7491

        Fixed recursion overflow which could occur within ORM statement compilation
        when using either the :func:`_orm.with_loader_criteria` feature or the the
        :meth:`_orm.PropComparator.and_` method within a loader strategy in
        conjunction with a subquery which referred to the same entity being altered
        by the criteria option, or loaded by the loader strategy.  A check for
        coming across the same loader criteria option in a recursive fashion has
        been added to accommodate for this scenario.


    .. change::
        :tags: bug, orm, mypy
        :tickets: 7462, 7368

        Fixed issue where the ``__class_getitem__()`` method of the generated
        declarative base class by :func:`_orm.as_declarative` would lead to
        inaccessible class attributes such as ``__table__``, for cases where a
        ``Generic[T]`` style typing declaration were used in the class hierarchy.
        This is in continuation from the basic addition of ``__class_getitem__()``
        in :ticket:`7368`. Pull request courtesy Kai Mueller.

    .. change::
        :tags: bug, mypy
        :tickets: 7496

        Fixed mypy regression where the release of mypy 0.930 added additional
        internal checks to the format of "named types", requiring that they be
        fully qualified and locatable. This broke the mypy plugin for SQLAlchemy,
        raising an assertion error, as there was use of symbols such as
        ``__builtins__`` and other un-locatable or unqualified names that
        previously had not raised any assertions.


    .. change::
        :tags: bug, engine
        :tickets: 7432

        Corrected the error message for the ``AttributeError`` that's raised when
        attempting to write to an attribute on the :class:`_result.Row` class,
        which is immutable. The previous message claimed the column didn't exist
        which is misleading.

    .. change::
        :tags: bug, mariadb
        :tickets: 7457

        Corrected the error classes inspected for the "is_disconnect" check for the
        ``mariadbconnector`` dialect, which was failing for disconnects that
        occurred due to common MySQL/MariaDB error codes such as 2006; the DBAPI
        appears to currently use the ``mariadb.InterfaceError`` exception class for
        disconnect errors such as error code 2006, which has been added to the list
        of classes checked.


    .. change::
        :tags: bug, orm, regression
        :tickets: 7447

        Fixed caching-related issue where the use of a loader option of the form
        ``lazyload(aliased(A).bs).joinedload(B.cs)`` would fail to result in the
        joinedload being invoked for runs subsequent to the query being cached, due
        to a mismatch for the options / object path applied to the objects loaded
        for a query with a lead entity that used ``aliased()``.


    .. change::
        :tags: bug, tests, regression
        :tickets: 7450

        Fixed a regression in the test suite where the test called
        ``CompareAndCopyTest::test_all_present`` would fail on some platforms due
        to additional testing artifacts being detected. Pull request courtesy Nils
        Philippsen.


    .. change::
        :tags: usecase, orm
        :tickets: 7410

        Added :paramref:`_orm.Session.get.execution_options` parameter which was
        previously missing from the :meth:`_orm.Session.get` method.

    .. change::
        :tags: bug, engine, regression
        :tickets: 7446

        Fixed regression in the :func:`_engine.make_url` function used to parse URL
        strings where the query string parsing would go into a recursion overflow
        if a Python 2 ``u''`` string were used.

.. changelog::
    :version: 1.4.28
    :released: December 9, 2021

    .. change::
        :tags: bug, mypy
        :tickets: 7321

        Fixed Mypy crash which would occur when using Mypy plugin against code
        which made use of :class:`_orm.declared_attr` methods for non-mapped names
        like ``__mapper_args__``, ``__table_args__``, or other dunder names, as the
        plugin would try to interpret these as mapped attributes which would then
        be later mis-handled. As part of this change, the decorated function is
        still converted by the plugin into a generic assignment statement (e.g.
        ``__mapper_args__: Any``) so that the argument signature can continue to be
        annotated in the same way one would for any other ``@classmethod`` without
        Mypy complaining about the wrong argument type for a method that isn't
        explicitly ``@classmethod``.



    .. change::
        :tags: bug, orm, ext
        :tickets: 7425

        Fixed issue where the internal cloning used by the
        :meth:`_orm.PropComparator.any` method on a :func:`_orm.relationship` in
        the case where the related class also makes use of ORM polymorphic loading,
        would fail if a hybrid property on the related, polymorphic class were used
        within the criteria for the ``any()`` operation.

    .. change::
        :tags: bug, platform
        :tickets: 7311

        Python 3.10 has deprecated "distutils" in favor of explicit use of
        "setuptools" in :pep:`632`; SQLAlchemy's setup.py has replaced imports
        accordingly. However, since setuptools itself only recently added the
        replacement symbols mentioned in pep-632 as of November of 2021 in version
        59.0.1, ``setup.py`` still has fallback imports to distutils, as SQLAlchemy
        1.4 does not have a hard setuptools versioning requirement at this time.
        SQLAlchemy 2.0 is expected to use a full :pep:`517` installation layout
        which will indicate appropriate setuptools versioning up front.

    .. change::
        :tags: bug, sql, regression
        :tickets: 7319

        Extended the :attr:`.TypeDecorator.cache_ok` attribute and corresponding
        warning message if this flag is not defined, a behavior first established
        for :class:`.TypeDecorator` as part of :ticket:`6436`, to also take place
        for :class:`.UserDefinedType`, by generalizing the flag and associated
        caching logic to a new common base for these two types,
        :class:`.ExternalType` to create :attr:`.UserDefinedType.cache_ok`.

        The change means any current :class:`.UserDefinedType` will now cause SQL
        statement caching to no longer take place for statements which make use of
        the datatype, along with a warning being emitted, unless the class defines
        the :attr:`.UserDefinedType.cache_ok` flag as True. If the datatype cannot
        form a deterministic, hashable cache key derived from its arguments,
        the attribute may be set to False which will continue to keep caching disabled but will suppress the
        warning. In particular, custom datatypes currently used in packages such as
        SQLAlchemy-utils will need to implement this flag. The issue was observed
        as a result of a SQLAlchemy-utils datatype that is not currently cacheable.

        .. seealso::

            :attr:`.ExternalType.cache_ok`

    .. change::
        :tags: deprecated, orm
        :tickets: 4390

        Deprecated an undocumented loader option syntax ``".*"``, which appears to
        be no different than passing a single asterisk, and will emit a deprecation
        warning if used. This syntax may have been intended for something but there
        is currently no need for it.


    .. change::
        :tags: bug, orm, mypy
        :tickets: 7368

        Fixed issue where the :func:`_orm.as_declarative` decorator and similar
        functions used to generate the declarative base class would not copy the
        ``__class_getitem__()`` method from a given superclass, which prevented the
        use of pep-484 generics in conjunction with the ``Base`` class. Pull
        request courtesy Kai Mueller.

    .. change::
        :tags: usecase, engine
        :tickets: 7400

        Added support for ``copy()`` and ``deepcopy()`` to the :class:`_url.URL`
        class. Pull request courtesy Tom Ritchford.

    .. change::
        :tags: bug, orm, regression
        :tickets: 7318

        Fixed ORM regression where the new behavior of "eager loaders run on
        unexpire" added in :ticket:`1763` would lead to loader option errors being
        raised inappropriately for the case where a single :class:`_orm.Query` or
        :class:`_sql.Select` were used to load multiple kinds of entities, along
        with loader options that apply to just one of those kinds of entity like a
        :func:`_orm.joinedload`, and later the objects would be refreshed from
        expiration, where the loader options would attempt to be applied to the
        mismatched object type and then raise an exception. The check for this
        mismatch now bypasses raising an error for this case.

    .. change::
        :tags: bug, sql
        :tickets: 7394

        Custom SQL elements, third party dialects, custom or third party datatypes
        will all generate consistent warnings when they do not clearly opt in or
        out of SQL statement caching, which is achieved by setting the appropriate
        attributes on each type of class. The warning links to documentation
        sections which indicate the appropriate approach for each type of object in
        order for caching to be enabled.

    .. change::
        :tags: bug, sql
        :tickets: 7394

        Fixed missing caching directives for a few lesser used classes in SQL Core
        which would cause ``[no key]`` to be logged for elements which made use of
        these.

    .. change::
        :tags: bug, postgresql
        :tickets: 7394

        Fixed missing caching directives for :class:`_postgresql.hstore` and
        :class:`_postgresql.array` constructs which would cause ``[no key]``
        to be logged for these elements.

    .. change::
        :tags: bug, orm
        :tickets: 7394

        User defined ORM options, such as those illustrated in the dogpile.caching
        example which subclass :class:`_orm.UserDefinedOption`, by definition are
        handled on every statement execution and do not need to be considered as
        part of the cache key for the statement. Caching of the base
        :class:`.ExecutableOption` class has been modified so that it is no longer
        a :class:`.HasCacheKey` subclass directly, so that the presence of user
        defined option objects will not have the unwanted side effect of disabling
        statement caching. Only ORM specific loader and criteria options, which are
        all internal to SQLAlchemy, now participate within the caching system.

    .. change::
        :tags: bug, orm
        :tickets: 7394

        Fixed issue where mappings that made use of :func:`_orm.synonym` and
        potentially other kinds of "proxy" attributes would not in all cases
        successfully generate a cache key for their SQL statements, leading to
        degraded performance for those statements.

    .. change::
        :tags: sql, usecase
        :tickets: 7259

        "Compound select" methods like :meth:`_sql.Select.union`,
        :meth:`_sql.Select.intersect_all` etc. now accept ``*other`` as an argument
        rather than ``other`` to allow for multiple additional SELECTs to be
        compounded with the parent statement at once. In particular, the change as
        applied to :meth:`_sql.CTE.union` and :meth:`_sql.CTE.union_all` now allow
        for a so-called "non-linear CTE" to be created with the :class:`_sql.CTE`
        construct, whereas previously there was no way to have more than two CTE
        sub-elements in a UNION together while still correctly calling upon the CTE
        in recursive fashion. Pull request courtesy Eric Masseran.

    .. change::
        :tags: bug, tests

        Implemented support for the test suite to run correctly under Pytest 7.
        Previously, only Pytest 6.x was supported for Python 3, however the version
        was not pinned on the upper bound in tox.ini. Pytest is not pinned in
        tox.ini to be lower than version 8 so that SQLAlchemy versions released
        with the current codebase will be able to be tested under tox without
        changes to the environment.   Much thanks to the Pytest developers for
        their help with this issue.


    .. change::
        :tags: orm, bug
        :tickets: 7389

        Fixed issue where a list mapped with :func:`_orm.relationship` would go
        into an endless loop if in-place added to itself, i.e. the ``+=`` operator
        were used, as well as if ``.extend()`` were given the same list.


    .. change::
        :tags: usecase, sql
        :tickets: 7386

        Support multiple clause elements in the :meth:`_sql.Exists.where` method,
        unifying the api with the one presented by a normal :func:`_sql.select`
        construct.

    .. change::
        :tags: bug, orm
        :tickets: 7388

        Fixed issue where if an exception occurred when the :class:`_orm.Session`
        were to close the connection within the :meth:`_orm.Session.commit` method,
        when using a context manager for :meth:`_orm.Session.begin` , it would
        attempt a rollback which would not be possible as the :class:`_orm.Session`
        was in between where the transaction is committed and the connection is
        then to be returned to the pool, raising the exception "this
        sessiontransaction is in the committed state". This exception can occur
        mostly in an asyncio context where CancelledError can be raised.


.. changelog::
    :version: 1.4.27
    :released: November 11, 2021

    .. change::
        :tags: bug, engine
        :tickets: 7291

        Fixed issue in future :class:`_engine.Connection` object where the
        :meth:`_engine.Connection.execute` method would not accept a non-dict
        mapping object, such as SQLAlchemy's own :class:`.RowMapping` or other
        ``abc.collections.Mapping`` object as a parameter dictionary.

    .. change::
        :tags: bug, mysql, mariadb
        :tickets: 7167

        Reorganized the list of reserved words into two separate lists, one for
        MySQL and one for MariaDB, so that these diverging sets of words can be
        managed more accurately; adjusted the MySQL/MariaDB dialect to switch among
        these lists based on either explicitly configured or
        server-version-detected "MySQL" or "MariaDB" backend. Added all current
        reserved words through MySQL 8 and current MariaDB versions including
        recently added keywords like "lead" . Pull request courtesy Kevin Kirsche.

    .. change::
        :tags: bug, orm
        :tickets: 7224

        Fixed bug in "relationship to aliased class" feature introduced at
        :ref:`relationship_aliased_class` where it was not possible to create a
        loader strategy option targeting an attribute on the target using the
        :func:`_orm.aliased` construct directly in a second loader option, such as
        ``selectinload(A.aliased_bs).joinedload(aliased_b.cs)``, without explicitly
        qualifying using :meth:`_orm.PropComparator.of_type` on the preceding
        element of the path. Additionally, targeting the non-aliased class directly
        would be accepted (inappropriately), but would silently fail, such as
        ``selectinload(A.aliased_bs).joinedload(B.cs)``; this now raises an error
        referring to the typing mismatch.


    .. change::
        :tags: bug, schema
        :tickets: 7295

        Fixed issue in :class:`.Table` where the
        :paramref:`.Table.implicit_returning` parameter would not be
        accommodated correctly when passed along with
        :paramref:`.Table.extend_existing` to augment an existing
        :class:`.Table`.

    .. change::
        :tags: bug, postgresql, asyncpg
        :tickets: 7283

        Changed the asyncpg dialect to bind the :class:`.Float` type to the "float"
        PostgreSQL type instead of "numeric" so that the value ``float(inf)`` can
        be accommodated. Added test suite support for persistence of the "inf"
        value.


    .. change::
        :tags: bug, engine, regression
        :tickets: 7274
        :versions: 2.0.0b1

        Fixed regression where the :meth:`_engine.CursorResult.fetchmany` method
        would fail to autoclose a server-side cursor (i.e. when ``stream_results``
        or ``yield_per`` is in use, either Core or ORM oriented results) when the
        results were fully exhausted.

    .. change::
        :tags: bug, orm
        :tickets: 7274
        :versions: 2.0.0b1

        All :class:`_result.Result` objects will now consistently raise
        :class:`_exc.ResourceClosedError` if they are used after a hard close,
        which includes the "hard close" that occurs after calling "single row or
        value" methods like :meth:`_result.Result.first` and
        :meth:`_result.Result.scalar`. This was already the behavior of the most
        common class of result objects returned for Core statement executions, i.e.
        those based on :class:`_engine.CursorResult`, so this behavior is not new.
        However, the change has been extended to properly accommodate for the ORM
        "filtering" result objects returned when using 2.0 style ORM queries,
        which would previously behave in "soft closed" style of returning empty
        results, or wouldn't actually "soft close" at all and would continue
        yielding from the underlying cursor.

        As part of this change, also added :meth:`_result.Result.close` to the base
        :class:`_result.Result` class and implemented it for the filtered result
        implementations that are used by the ORM, so that it is possible to call
        the :meth:`_engine.CursorResult.close` method on the underlying
        :class:`_engine.CursorResult` when the ``yield_per`` execution option
        is in use to close a server side cursor before remaining ORM results have
        been fetched. This was again already available for Core result sets but the
        change makes it available for 2.0 style ORM results as well.


    .. change::
        :tags: bug, mysql
        :tickets: 7281
        :versions: 2.0.0b1

        Fixed issue in MySQL :meth:`_mysql.Insert.on_duplicate_key_update` which
        would render the wrong column name when an expression were used in a VALUES
        expression. Pull request courtesy Cristian Sabaila.

    .. change::
        :tags: bug, sql, regression
        :tickets: 7292

        Fixed regression where the row objects returned for ORM queries, which are
        now the normal :class:`_sql.Row` objects, would not be interpreted by the
        :meth:`_sql.ColumnOperators.in_` operator as tuple values to be broken out
        into individual bound parameters, and would instead pass them as single
        values to the driver leading to failures. The change to the "expanding IN"
        system now accommodates for the expression already being of type
        :class:`.TupleType` and treats values accordingly if so. In the uncommon
        case of using "tuple-in" with an untyped statement such as a textual
        statement with no typing information, a tuple value is detected for values
        that implement ``collections.abc.Sequence``, but that are not ``str`` or
        ``bytes``, as always when testing for ``Sequence``.

    .. change::
        :tags: usecase, sql

        Added :class:`.TupleType` to the top level ``sqlalchemy`` import namespace.

    .. change::
        :tags: bug, sql
        :tickets: 7269

        Fixed issue where using the feature of using a string label for ordering or
        grouping described at :ref:`tutorial_order_by_label` would fail to function
        correctly if used on a :class:`.CTE` construct, when the CTE were embedded
        inside of an enclosing :class:`_sql.Select` statement that itself was set
        up as a scalar subquery.



    .. change::
        :tags: bug, orm, regression
        :tickets: 7239

        Fixed 1.4 regression where :meth:`_orm.Query.filter_by` would not function
        correctly on a :class:`_orm.Query` that was produced from
        :meth:`_orm.Query.union`, :meth:`_orm.Query.from_self` or similar.

    .. change::
        :tags: bug, orm
        :tickets: 7304

        Fixed issue where deferred polymorphic loading of attributes from a
        joined-table inheritance subclass would fail to populate the attribute
        correctly if the :func:`_orm.load_only` option were used to originally
        exclude that attribute, in the case where the load_only were descending
        from a relationship loader option.  The fix allows that other valid options
        such as ``defer(..., raiseload=True)`` etc. still function as expected.

    .. change::
        :tags: postgresql, usecase, asyncpg
        :tickets: 7284
        :versions: 2.0.0b1

        Added overridable methods ``PGDialect_asyncpg.setup_asyncpg_json_codec``
        and ``PGDialect_asyncpg.setup_asyncpg_jsonb_codec`` codec, which handle the
        required task of registering JSON/JSONB codecs for these datatypes when
        using asyncpg. The change is that methods are broken out as individual,
        overridable methods to support third party dialects that need to alter or
        disable how these particular codecs are set up.



    .. change::
        :tags: bug, engine
        :tickets: 7272
        :versions: 2.0.0b1

        Fixed issue in future :class:`_engine.Engine` where calling upon
        :meth:`_engine.Engine.begin` and entering the context manager would not
        close the connection if the actual BEGIN operation failed for some reason,
        such as an event handler raising an exception; this use case failed to be
        tested for the future version of the engine. Note that the "future" context
        managers which handle ``begin()`` blocks in Core and ORM don't actually run
        the "BEGIN" operation until the context managers are actually entered. This
        is different from the legacy version which runs the "BEGIN" operation up
        front.

    .. change::
        :tags: mssql, bug
        :tickets: 7300

        Adjusted the compiler's generation of "post compile" symbols including
        those used for "expanding IN" as well as for the "schema translate map" to
        not be based directly on plain bracketed strings with underscores, as this
        conflicts directly with SQL Server's quoting format of also using brackets,
        which produces false matches when the compiler replaces "post compile" and
        "schema translate" symbols. The issue created easy to reproduce examples
        both with the :meth:`.Inspector.get_schema_names` method when used in
        conjunction with the
        :paramref:`_engine.Connection.execution_options.schema_translate_map`
        feature, as well in the unlikely case that a symbol overlapping with the
        internal name "POSTCOMPILE" would be used with a feature like "expanding
        in".


    .. change::
        :tags: postgresql, pg8000
        :tickets: 7167

        Improve array handling when using PostgreSQL with the
        pg8000 dialect.

    .. change::
        :tags: bug, orm, regression
        :tickets: 7244

        Fixed 1.4 regression where :meth:`_orm.Query.filter_by` would not function
        correctly when :meth:`_orm.Query.join` were joined to an entity which made
        use of :meth:`_orm.PropComparator.of_type` to specify an aliased version of
        the target entity. The issue also applies to future style ORM queries
        constructed with :func:`_sql.select`.


    .. change::
        :tags: bug, sql, regression
        :tickets: 7287

        Fixed regression where the :func:`_sql.text` construct would no longer be
        accepted as a target case in the "whens" list within a :func:`_sql.case`
        construct. The regression appears related to an attempt to guard against
        some forms of literal values that were considered to be ambiguous when
        passed here; however, there's no reason the target cases shouldn't be
        interpreted as open-ended SQL expressions just like anywhere else, and a
        literal string or tuple will be converted to a bound parameter as would be
        the case elsewhere.

.. changelog::
    :version: 1.4.26
    :released: October 19, 2021

    .. change::
        :tags: orm
        :tickets: 6284

        Passing a :class:`.Query` object to :meth:`_orm.Session.execute` is not
        the intended use of this object, and will now raise a deprecation warning.

    .. change::
        :tags: bug, postgresql
        :tickets: 5387

        Added a "disconnect" condition for the "SSL SYSCALL error: Bad address"
        error message as reported by psycopg2. Pull request courtesy Zeke Brechtel.

    .. change::
        :tags: bug, orm

        Improved the exception message generated when configuring a mapping with
        joined table inheritance where the two tables either have no foreign key
        relationships set up, or where they have multiple foreign key relationships
        set up. The message is now ORM specific and includes context that the
        :paramref:`_orm.Mapper.inherit_condition` parameter may be needed
        particularly for the ambiguous foreign keys case.


    .. change::
        :tags: bug, sql
        :tickets: 6520

        Fixed issue where SQL queries using the
        :meth:`_functions.FunctionElement.within_group` construct could not be
        pickled, typically when using the ``sqlalchemy.ext.serializer`` extension
        but also for general generic pickling.

    .. change::
        :tags: bug, orm
        :tickets: 7189

        Fixed issue with :func:`_orm.with_loader_criteria` feature where ON
        criteria would not be added to a JOIN for a query of the form
        ``select(A).join(B)``, stating a target while making use of an implicit
        ON clause.

    .. change::
        :tags: bug, orm
        :tickets: 7205

        Fixed bug where the ORM "plugin", necessary for features such as
        :func:`_orm.with_loader_criteria` to work correctly, would not be applied
        to a :func:`_sql.select` which queried from an ORM column expression if it
        made use of the :meth:`_sql.ColumnElement.label` modifier.



    .. change::
        :tags: bug, mypy
        :tickets: 6435

        Fixed issue in mypy plugin to improve upon some issues detecting ``Enum()``
        SQL types containing custom Python enumeration classes. Pull request
        courtesy Hiroshi Ogawa.

    .. change::
        :tags: bug, mysql
        :tickets: 7144

        Fixed issue in MySQL :func:`_mysql.match` construct where passing a clause
        expression such as :func:`_sql.bindparam` or other SQL expression for the
        "against" parameter would fail. Pull request courtesy Anton Kovalevich.


    .. change::
        :tags: bug, mssql
        :tickets: 7160

        Fixed issue with :meth:`.Inspector.get_foreign_keys` where foreign
        keys were omitted if they were established against a unique
        index instead of a unique constraint.


    .. change::
        :tags: usecase, mssql

        Added reflection support for SQL Server foreign key options, including
        "ON UPDATE" and "ON DELETE" values of "CASCADE" and "SET NULL".

    .. change::
        :tags: bug, sql
        :tickets: 4123

        Repaired issue in new :paramref:`_sql.HasCTE.cte.nesting` parameter
        introduced with :ticket:`4123` where a recursive :class:`_sql.CTE` using
        :paramref:`_sql.HasCTE.cte.recursive` in typical conjunction with UNION
        would not compile correctly.  Additionally makes some adjustments so that
        the :class:`_sql.CTE` construct creates a correct cache key.
        Pull request courtesy Eric Masseran.

    .. change::
        :tags: bug, engine
        :tickets: 7130

        Fixed issue where the deprecation warning for the :class:`.URL` constructor
        which indicates that the :meth:`.URL.create` method should be used would
        not emit if a full positional argument list of seven arguments were passed;
        additionally, validation of URL arguments will now occur if the constructor
        is called in this way, which was being skipped previously.

    .. change::
        :tags: bug, orm
        :tickets: 7103

        Add missing methods added in :ticket:`6991` to
        :class:`_scoping.scoped_session` and :func:`_asyncio.async_scoped_session`.

    .. change::
        :tags: bug, examples
        :tickets: 7169

        Repaired the examples in examples/versioned_rows to use SQLAlchemy 1.4 APIs
        correctly; these examples had been missed when API changes like removing
        "passive" from :meth:`_orm.Session.is_modified` were made as well as the
        :meth:`_ormevents.SessionEvents.do_orm_execute()` event hook were added.

    .. change::
        :tags: bug, orm
        :tickets: 6974, 6972

        An extra layer of warning messages has been added to the functionality
        of :meth:`_orm.Query.join` and the ORM version of
        :meth:`_sql.Select.join`, where a few places where "automatic aliasing"
        continues to occur will now be called out as a pattern to avoid, mostly
        specific to the area of joined table inheritance where classes that share
        common base tables are being joined together without using explicit aliases.
        One case emits a legacy warning for a pattern that's not recommended,
        the other case is fully deprecated.

        The automatic aliasing within ORM join() which occurs for overlapping
        mapped tables does not work consistently with all APIs such as
        :func:`_orm.contains_eager()`, and rather than continue to try to make
        these use cases work everywhere, replacing with a more user-explicit
        pattern is clearer, less prone to bugs and simplifies SQLAlchemy's
        internals further.

        The warnings include links to the errors.rst page where each pattern is
        demonstrated along with the recommended pattern to fix.

        .. seealso::

            :ref:`error_xaj1`

            :ref:`error_xaj2`

    .. change::
        :tags: bug, sql
        :tickets: 7061

        Account for the :paramref:`_sql.table.schema` parameter passed to
        the :func:`_sql.table` construct, such that it is taken into account
        when accessing the :attr:`_sql.TableClause.fullname` attribute.

    .. change::
        :tags: bug, sql
        :tickets: 7140

        Fixed an inconsistency in the :meth:`_sql.ColumnOperators.any_` /
        :meth:`_sql.ColumnOperators.all_` functions / methods where the special
        behavior these functions have of "flipping" the expression such that the
        "ANY" / "ALL" expression is always on the right side would not function if
        the comparison were against the None value, that is, "column.any_() ==
        None" should produce the same SQL expression as "null() == column.any_()".
        Added more docs to clarify this as well, plus mentions that any_() / all_()
        generally supersede the ARRAY version "any()" / "all()".

    .. change::
        :tags: engine, bug, postgresql
        :tickets: 3247

        The :meth:`_reflection.Inspector.reflect_table` method now supports
        reflecting tables that do not have user defined columns. This allows
        :meth:`_schema.MetaData.reflect` to properly complete reflection on
        databases that contain such tables. Currently, only PostgreSQL is known to
        support such a construct among the common database backends.

    .. change::
        :tags: sql, bug, regression
        :tickets: 7177

        Fixed issue where "expanding IN" would fail to function correctly with
        datatypes that use the :meth:`_types.TypeEngine.bind_expression` method,
        where the method would need to be applied to each element of the
        IN expression rather than the overall IN expression itself.

    .. change::
        :tags: postgresql, bug, regression
        :tickets: 7177

        Fixed issue where IN expressions against a series of array elements, as can
        be done with PostgreSQL, would fail to function correctly due to multiple
        issues within the "expanding IN" feature of SQLAlchemy Core that was
        standardized in version 1.4.  The psycopg2 dialect now makes use of the
        :meth:`_types.TypeEngine.bind_expression` method with :class:`_types.ARRAY`
        to portably apply the correct casts to elements.  The asyncpg dialect was
        not affected by this issue as it applies bind-level casts at the driver
        level rather than at the compiler level.


    .. change::
        :tags: bug, mysql
        :tickets: 7204

        Fixed installation issue where the ``sqlalchemy.dialects.mysql`` module
        would not be importable if "greenlet" were not installed.

    .. change::
        :tags: bug, mssql
        :tickets: 7168

        Fixed issue with :meth:`.Inspector.has_table` where it would return False
        if a local temp table with the same name from a different session happened
        to be returned first when querying tempdb.  This is a continuation of
        :ticket:`6910` which accounted for the temp table existing only in the
        alternate session and not the current one.

    .. change::
        :tags: bug, orm
        :tickets: 7128

        Fixed bug where iterating a :class:`_result.Result` from a :class:`_orm.Session`
        after that :class:`_orm.Session` were closed would partially attach objects
        to that session in an essentially invalid state. It now raises an exception
        with a link to new documentation if an **un-buffered** result is iterated
        from a :class:`_orm.Session` that was closed or otherwise had the
        :meth:`_orm.Session.expunge_all` method called after that :class:`_result.Result`
        was generated. The ``prebuffer_rows`` execution option, as is used
        automatically by the asyncio extension for client-side result sets, may be
        used to produce a :class:`_result.Result` where the ORM objects are prebuffered,
        and in this case iterating the result will produce a series of detached
        objects.

        .. seealso::

            :ref:`error_lkrp`

    .. change::
        :tags: bug, mssql, regression
        :tickets: 7129

        Fixed bug in SQL Server :class:`_mssql.DATETIMEOFFSET` datatype where the
        ODBC implementation would not generate the correct DDL, for cases where the
        type were converted using the ``dialect.type_descriptor()`` method, the
        usage of which is illustrated in some documented examples for
        :class:`.TypeDecorator`, though not necessary for most datatypes.
        Regression was introduced by :ticket:`6366`. As part of this change, the
        full list of SQL Server date types have been amended to return a "dialect
        impl" that generates the same DDL name as the supertype.

    .. change::
        :tags: bug, sql
        :tickets: 7153

        Adjusted the "column disambiguation" logic that's new in 1.4, where the
        same expression repeated gets an "extra anonymous" label, so that the logic
        more aggressively deduplicates those labels when the repeated element
        is the same Python expression object each time, as occurs in cases like
        when using "singleton" values like :func:`_sql.null`.  This is based on
        the observation that at least some databases (e.g. MySQL, but not SQLite)
        will raise an error if the same label is repeated inside of a subquery.

    .. change::
        :tags: bug, orm
        :tickets: 7154

        Related to :ticket:`7153`, fixed an issue where result column lookups would
        fail for "adapted" SELECT statements that selected for "constant" value
        expressions most typically the NULL expression, as would occur in such
        places as joined eager loading in conjunction with limit/offset. This was
        overall a regression due to issue :ticket:`6259` which removed all
        "adaption" for constants like NULL, "true", and "false" when rewriting
        expressions in a SQL statement, but this broke the case where the same
        adaption logic were used to resolve the constant to a labeled expression
        for the purposes of result set targeting.

    .. change::
        :tags: bug, orm, regression
        :tickets: 7134

        Fixed regression where ORM loaded objects could not be pickled in cases
        where loader options making use of ``"*"`` were used in certain
        combinations, such as combining the :func:`_orm.joinedload` loader strategy
        with ``raiseload('*')`` of sub-elements.


    .. change::
        :tags: bug, engine
        :tickets: 7077

        Implemented proper ``__reduce__()`` methods for all SQLAlchemy exception
        objects to ensure they all support clean round trips when pickling, as
        exception objects are often serialized for the purposes of various
        debugging tools.

    .. change::
        :tags: bug, orm, regression
        :tickets: 7209

        Fixed regression where the use of a :class:`_hybrid.hybrid_property`
        attribute or a mapped :func:`_orm.composite` attribute as a key passed to
        the :meth:`_dml.Update.values` method for an ORM-enabled
        :class:`_dml.Update` statement, as well as when using it via the legacy
        :meth:`_orm.Query.update` method, would be processed for incoming
        ORM/hybrid/composite values within the compilation stage of the UPDATE
        statement, which meant that in those cases where caching occurred,
        subsequent invocations of the same statement would no longer receive the
        correct values. This would include not only hybrids that use the
        :meth:`_hybrid.hybrid_property.update_expression` method, but any use of a
        plain hybrid attribute as well. For composites, the issue instead caused a
        non-repeatable cache key to be generated, which would break caching and
        could fill up the statement cache with repeated statements.

        The :class:`_dml.Update` construct now handles the processing of key/value
        pairs passed to :meth:`_dml.Update.values` and
        :meth:`_dml.Update.ordered_values` up front when the construct is first
        generated, before the cache key has been generated so that the key/value
        pairs are processed each time, and so that the cache key is generated
        against the individual column/value pairs that will ultimately be
        used in the statement.


.. changelog::
    :version: 1.4.25
    :released: September 22, 2021

    .. change::
        :tags: bug, platform, regression
        :tickets: 7024

        Fixed regression due to :ticket:`7024` where the reorganization of the
        "platform machine" names used by the ``greenlet`` dependency mis-spelled
        "aarch64" and additionally omitted uppercase "AMD64" as is needed for
        Windows machines. Pull request courtesy James Dow.

.. changelog::
    :version: 1.4.24
    :released: September 22, 2021

    .. change::
        :tags: bug, asyncio
        :tickets: 6943

        Fixed a bug in :meth:`_asyncio.AsyncSession.execute` and
        :meth:`_asyncio.AsyncSession.stream` that required ``execution_options``
        to be an instance of ``immutabledict`` when defined. It now
        correctly accepts any mapping.

    .. change::
        :tags: engine, asyncio, usecase
        :tickets: 6832

        Improve the interface used by adapted drivers, like the asyncio ones,
        to access the actual connection object returned by the driver.

        The :class:`._ConnectionFairy` object has two new attributes:

        * :attr:`._ConnectionFairy.dbapi_connection` always represents a DBAPI
          compatible object. For pep-249 drivers, this is the DBAPI connection as
          it always has been, previously accessed under the ``.connection``
          attribute. For asyncio drivers that SQLAlchemy adapts into a pep-249
          interface, the returned object will normally be a SQLAlchemy adaption
          object called :class:`_engine.AdaptedConnection`.
        * :attr:`._ConnectionFairy.driver_connection` always represents the actual
          connection object maintained by the third party pep-249 DBAPI or async
          driver in use. For standard pep-249 DBAPIs, this will always be the same
          object as that of the ``dbapi_connection``. For an asyncio driver, it
          will be the underlying asyncio-only connection object.

        The ``.connection`` attribute remains available and is now a legacy alias
        of ``.dbapi_connection``.

        .. seealso::

            :ref:`faq_dbapi_connection`


    .. change::
        :tags: bug, sql
        :tickets: 7052

        Implemented missing methods in :class:`_functions.FunctionElement` which,
        while unused, would lead pylint to report them as unimplemented abstract
        methods.

    .. change::
        :tags: bug, mssql, reflection
        :tickets: 6910

        Fixed an issue where :meth:`_reflection.has_table` returned
        ``True`` for local temporary tables that actually belonged to a
        different SQL Server session (connection). An extra check is now
        performed to ensure that the temp table detected is in fact owned
        by the current session.

    .. change::
        :tags: bug, engine, regression
        :tickets: 6913

        Fixed issue where the ability of the
        :meth:`_events.ConnectionEvents.before_execute` method to alter the SQL
        statement object passed, returning the new object to be invoked, was
        inadvertently removed. This behavior has been restored.


    .. change::
        :tags: bug, engine
        :tickets: 6958

        Ensure that ``str()`` is called on the an
        :paramref:`_url.URL.create.password` argument, allowing usage of objects
        that implement the ``__str__()`` method as password attributes. Also
        clarified that one such object is not appropriate to dynamically change the
        password for each database connection; the approaches at
        :ref:`engines_dynamic_tokens` should be used instead.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6979

        Fixed ORM issue where column expressions passed to ``query()`` or
        ORM-enabled ``select()`` would be deduplicated on the identity of the
        object, such as a phrase like ``select(A.id, null(), null())`` would
        produce only one "NULL" expression, which previously was not the case in
        1.3. However, the change also allows for ORM expressions to render as given
        as well, such as ``select(A.data, A.data)`` will produce a result row with
        two columns.

    .. change::
        :tags: bug, engine
        :tickets: 6983

        Fixed issue in :class:`_engine.URL` where validation of "drivername" would
        not appropriately respond to the ``None`` value where a string were
        expected.

    .. change::
        :tags: bug, mypy
        :tickets: 6950

        Fixed issue where mypy plugin would crash when interpreting a
        ``query_expression()`` construct.

    .. change::
        :tags: usecase, sql
        :tickets: 4123

        Added new parameter :paramref:`_sql.HasCTE.cte.nesting` to the
        :class:`_sql.CTE` constructor and :meth:`_sql.HasCTE.cte` method, which
        flags the CTE as one which should remain nested within an enclosing CTE,
        rather than being moved to the top level of the outermost SELECT. While in
        the vast majority of cases there is no difference in SQL functionality,
        users have identified various edge-cases where true nesting of CTE
        constructs is desirable. Much thanks to Eric Masseran for lots of work on
        this intricate feature.

    .. change::
        :tags: usecase, engine, orm
        :tickets: 6990

        Added new methods :meth:`_orm.Session.scalars`,
        :meth:`_engine.Connection.scalars`, :meth:`_asyncio.AsyncSession.scalars`
        and :meth:`_asyncio.AsyncSession.stream_scalars`, which provide a short cut
        to the use case of receiving a row-oriented :class:`_result.Result` object
        and converting it to a :class:`_result.ScalarResult` object via the
        :meth:`_engine.Result.scalars` method, to return a list of values rather
        than a list of rows. The new methods are analogous to the long existing
        :meth:`_orm.Session.scalar` and :meth:`_engine.Connection.scalar` methods
        used to return a single value from the first row only. Pull request
        courtesy Miguel Grinberg.

    .. change::
        :tags: usecase, orm
        :tickets: 6955

        Added loader options to :meth:`_orm.Session.merge` and
        :meth:`_asyncio.AsyncSession.merge` via a new
        :paramref:`_orm.Session.merge.options` parameter, which will apply the
        given loader options to the ``get()`` used internally by merge, allowing
        eager loading of relationships etc. to be applied when the merge process
        loads a new object. Pull request courtesy Daniel Stone.

    .. change::
        :tags: feature, asyncio, mysql
        :tickets: 6993

        Added initial support for the ``asyncmy`` asyncio database driver for MySQL
        and MariaDB. This driver is very new, however appears to be the only
        current alternative to the ``aiomysql`` driver which currently appears to
        be unmaintained and is not working with current Python versions. Much
        thanks to long2ice for the pull request for this dialect.

        .. seealso::

            :ref:`asyncmy`

    .. change::
        :tags: bug, asyncio

        Added missing ``**kw`` arguments to the
        :meth:`_asyncio.AsyncSession.connection` method.

    .. change::
        :tags: bug, sql
        :tickets: 7055

        Fixed a two issues where combinations of ``select()`` and ``join()`` when
        adapted to form a copy of the element would not completely copy the state
        of all column objects associated with subqueries. A key problem this caused
        is that usage of the :meth:`_sql.ClauseElement.params` method (which should
        probably be moved into a legacy category as it is inefficient and error
        prone) would leave copies of the old :class:`_sql.BindParameter` objects
        around, leading to issues in correctly setting the parameters at execution
        time.



    .. change::
        :tags: bug, orm, regression
        :tickets: 6924

        Fixed issue in recently repaired ``Query.with_entities()`` method where the
        flag that determines automatic uniquing for legacy ORM ``Query`` objects
        only would be set to ``True`` inappropriately in cases where the
        ``with_entities()`` call would be setting the ``Query`` to return
        column-only rows, which are not uniqued.

    .. change::
        :tags: bug, postgresql
        :tickets: 6912

        Qualify ``version()`` call to avoid shadowing issues if a different
        search path is configured by the user.

    .. change::
        :tags: bug, engine, postgresql
        :tickets: 6963

        Fixed issue where an engine that had
        :paramref:`_sa.create_engine.implicit_returning` set to False would fail to
        function when PostgreSQL's "fast insertmany" feature were used in
        conjunction with a ``Sequence``, as well as if any kind of "executemany"
        with "return_defaults()" were used in conjunction with a ``Sequence``. Note
        that PostgreSQL "fast insertmany" uses "RETURNING" by definition, when the
        SQL statement is passed to the driver; overall, the
        :paramref:`_sa.create_engine.implicit_returning` flag is legacy and has no
        real use in modern SQLAlchemy, and will be deprecated in a separate change.

    .. change::
        :tags: bug, mypy
        :tickets: 6937

        Fixed issue in mypy plugin where columns on a mixin would not be correctly
        interpreted if the mapped class relied upon a ``__tablename__`` routine
        that came from a superclass.

    .. change::
        :tags: bug, postgresql
        :tickets: 6106

        The :class:`_postgresql.ENUM` datatype is PostgreSQL-native and therefore
        should not be used with the ``native_enum=False`` flag. This flag is now
        ignored if passed to the :class:`_postgresql.ENUM` datatype and a warning
        is emitted; previously the flag would cause the type object to fail to
        function correctly.


    .. change::
        :tags: bug, sql
        :tickets: 7036

        Fixed issue related to new :meth:`_sql.HasCTE.add_cte` feature where
        pairing two "INSERT..FROM SELECT" statements simultaneously would lose
        track of the two independent SELECT statements, leading to the wrong SQL.

    .. change::
        :tags: asyncio, bug
        :tickets: 6746

        Deprecate usage of :class:`_orm.scoped_session` with asyncio drivers. When
        using Asyncio the :class:`_asyncio.async_scoped_session` should be used
        instead.

    .. change::
        :tags: bug, platform
        :tickets: 7024

        Further adjusted the "greenlet" package specifier in setup.cfg to use a
        long chain of "or" expressions, so that the comparison of
        ``platform_machine`` to a specific identifier matches only the complete
        string.

    .. change::
        :tags: bug, sqlite

        Fixed bug where the error message for SQLite invalid isolation level on the
        pysqlite driver would fail to indicate that "AUTOCOMMIT" is one of the
        valid isolation levels.

    .. change::
        :tags: bug, sql
        :tickets: 7060

        Fixed issue where using ORM column expressions as keys in the list of
        dictionaries passed to :meth:`_sql.Insert.values` for "multi-valued insert"
        would not be processed correctly into the correct column expressions.

    .. change::
        :tags: asyncio, usecase
        :tickets: 6746

        The :class:`_asyncio.AsyncSession` now supports overriding which
        :class:`_orm.Session` it uses as the proxied instance. A custom ``Session``
        class can be passed using the :paramref:`.AsyncSession.sync_session_class`
        parameter or by subclassing the ``AsyncSession`` and specifying a custom
        :attr:`.AsyncSession.sync_session_class`.

    .. change::
        :tags: bug, oracle, performance
        :tickets: 4486

        Added a CAST(VARCHAR2(128)) to the "table name", "owner", and other
        DDL-name parameters as used in reflection queries against Oracle system
        views such as ALL_TABLES, ALL_TAB_CONSTRAINTS, etc to better enable
        indexing to take place against these columns, as they previously would be
        implicitly handled as NVARCHAR2 due to Python's use of Unicode for strings;
        these columns are documented in all Oracle versions as being VARCHAR2 with
        lengths varying from 30 to 128 characters depending on server version.
        Additionally, test support has been enabled for Unicode-named DDL
        structures against Oracle databases.

.. changelog::
    :version: 1.4.23
    :released: August 18, 2021

    .. change::
        :tags: bug, sql
        :tickets: 6752

        Fix issue in :class:`_sql.CTE` where new :meth:`_sql.HasCTE.add_cte` method
        added in version 1.4.21 / :ticket:`6752` failed to function correctly for
        "compound select" structures such as :func:`_sql.union`,
        :func:`_sql.union_all`, :func:`_sql.except`, etc. Pull request courtesy
        Eric Masseran.

    .. change::
        :tags: orm, usecase
        :tickets: 6808

        Added new attribute :attr:`_sql.Select.columns_clause_froms` that will
        retrieve the FROM list implied by the columns clause of the
        :class:`_sql.Select` statement. This differs from the old
        :attr:`_sql.Select.froms` collection in that it does not perform any ORM
        compilation steps, which necessarily deannotate the FROM elements and do
        things like compute joinedloads etc., which makes it not an appropriate
        candidate for the :meth:`_sql.Select.select_from` method. Additionally adds
        a new parameter
        :paramref:`_sql.Select.with_only_columns.maintain_column_froms` that
        transfers this collection to :meth:`_sql.Select.select_from` before
        replacing the columns collection.

        In addition, the :attr:`_sql.Select.froms` is renamed to
        :meth:`_sql.Select.get_final_froms`, to stress that this collection is not
        a simple accessor and is instead calculated given the full state of the
        object, which can be an expensive call when used in an ORM context.

        Additionally fixes a regression involving the
        :func:`_orm.with_only_columns` function to support applying criteria to
        column elements that were replaced with either
        :meth:`_sql.Select.with_only_columns` or :meth:`_orm.Query.with_entities` ,
        which had broken as part of :ticket:`6503` released in 1.4.19.

    .. change::
        :tags: bug, orm, sql
        :tickets: 6824

        Fixed issue where a bound parameter object that was "cloned" would cause a
        name conflict in the compiler, if more than one clone of this parameter
        were used at the same time in a single statement. This could occur in
        particular with things like ORM single table inheritance queries that
        indicated the same "discriminator" value multiple times in one query.


    .. change::
        :tags: bug, mssql, sql
        :tickets: 6863

        Fixed issue where the ``literal_binds`` compiler flag, as used externally
        to render bound parameters inline, would fail to work when used with a
        certain class of parameters known as "literal_execute", which covers things
        like LIMIT and OFFSET values for dialects where the drivers don't allow a
        bound parameter, such as SQL Server's "TOP" clause.  The issue locally
        seemed to affect only the MSSQL dialect.

    .. change::
        :tags: bug, orm
        :tickets: 6869

        Fixed issue in loader strategies where the use of the
        :meth:`_orm.Load.options` method, particularly when nesting multiple calls,
        would generate an overly long and more importantly non-deterministic cache
        key, leading to very large cache keys which were also not allowing
        efficient cache usage, both in terms of total memory used as well as number
        of entries used in the cache itself.

    .. change::
        :tags: bug, sql
        :tickets: 6858

        Fixed an issue in the ``CacheKey.to_offline_string()`` method used by the
        dogpile.caching example where attempting to create a proper cache key from
        the special "lambda" query generated by the lazy loader would fail to
        include the parameter values, leading to an incorrect cache key.


    .. change::
        :tags: bug, orm
        :tickets: 6887

        Revised the means by which the
        :attr:`_orm.ORMExecuteState.user_defined_options` accessor receives
        :class:`_orm.UserDefinedOption` and related option objects from the
        context, with particular emphasis on the "selectinload" on the loader
        strategy where this previously was not working; other strategies did not
        have this problem. The objects that are associated with the current query
        being executed, and not that of a query being cached, are now propagated
        unconditionally. This essentially separates them out from the "loader
        strategy" options which are explicitly associated with the compiled state
        of a query and need to be used in relation to the cached query.

        The effect of this fix is that a user-defined option, such as those used
        by the dogpile.caching example as well as for other recipes such as
        defining a "shard id" for the horizontal sharing extension, will be
        correctly propagated to eager and lazy loaders regardless of whether
        a cached query was ultimately invoked.


    .. change::
        :tags: bug, sql
        :tickets: 6886

        Adjusted the "from linter" warning feature to accommodate for a chain of
        joins more than one level deep where the ON clauses don't explicitly match
        up the targets, such as an expression such as "ON TRUE". This mode of use
        is intended to cancel the cartesian product warning simply by the fact that
        there's a JOIN from "a to b", which was not working for the case where the
        chain of joins had more than one element.

    .. change::
        :tags: bug, postgresql
        :tickets: 6886

        Added the "is_comparison" flag to the PostgreSQL "overlaps",
        "contained_by", "contains" operators, so that they work in relevant ORM
        contexts as well as in conjunction with the "from linter" feature.

    .. change::
        :tags: bug, orm
        :tickets: 6812

        Fixed issue where the unit of work would internally use a 2.0-deprecated
        SQL expression form, emitting a deprecation warning when SQLALCHEMY_WARN_20
        were enabled.


    .. change::
        :tags: bug, orm
        :tickets: 6881

        Fixed issue in :func:`_orm.selectinload` where use of the new
        :meth:`_orm.PropComparator.and_` feature within options that were nested
        more than one level deep would fail to update bound parameter values that
        were in the nested criteria, as a side effect of SQL statement caching.


    .. change::
        :tags: bug, general
        :tickets: 6136

        The setup requirements have been modified such ``greenlet`` is a default
        requirement only for those platforms that are well known for ``greenlet``
        to be installable and for which there is already a pre-built binary on
        pypi; the current list is ``x86_64 aarch64 ppc64le amd64 win32``. For other
        platforms, greenlet will not install by default, which should enable
        installation and test suite running of SQLAlchemy 1.4 on platforms that
        don't support ``greenlet``, excluding any asyncio features. In order to
        install with the ``greenlet`` dependency included on a machine architecture
        outside of the above list, the ``[asyncio]`` extra may be included by
        running ``pip install sqlalchemy[asyncio]`` which will then attempt to
        install ``greenlet``.

        Additionally, the test suite has been repaired so that tests can complete
        fully when greenlet is not installed, with appropriate skips for
        asyncio-related tests.

    .. change::
        :tags: enum, schema
        :tickets: 6146

        Unify behaviour :class:`_schema.Enum` in native and non-native
        implementations regarding the accepted values for an enum with
        aliased elements.
        When :paramref:`_schema.Enum.omit_aliases` is ``False`` all values,
        alias included, are accepted as valid values.
        When :paramref:`_schema.Enum.omit_aliases` is ``True`` only non aliased values
        are accepted as valid values.

    .. change::
        :tags: bug, ext
        :tickets: 6816

        Fixed issue where the horizontal sharding extension would not correctly
        accommodate for a plain textual SQL statement passed to
        :meth:`_orm.Session.execute`.

    .. change::
        :tags: bug, orm
        :tickets: 6889, 6079

        Adjusted ORM loader internals to no longer use the "lambda caching" system
        that was added in 1.4, as well as repaired one location that was still
        using the previous "baked query" system for a query. The lambda caching
        system remains an effective way to reduce the overhead of building up
        queries that have relatively fixed usage patterns. In the case of loader
        strategies, the queries used are responsible for moving through lots of
        arbitrary options and criteria, which is both generated and sometimes
        consumed by end-user code, that make the lambda cache concept not any more
        efficient than not using it, at the cost of more complexity. In particular
        the problems noted by :ticket:`6881` and :ticket:`6887` are made are made
        considerably less complicated by removing this feature internally.



    .. change::
        :tags: bug, orm
        :tickets: 6889

        Fixed an issue where the :class:`_orm.Bundle` construct would not create
        proper cache keys, leading to inefficient use of the query cache.  This
        had some impact on the "selectinload" strategy and was identified as
        part of :ticket:`6889`.

    .. change::
        :tags: usecase, mypy
        :tickets: 6804, 6759

        Added support for SQLAlchemy classes to be defined in user code using
        "generic class" syntax as defined by ``sqlalchemy2-stubs``, e.g.
        ``Column[String]``, without the need for qualifying these constructs within
        a ``TYPE_CHECKING`` block by implementing the Python special method
        ``__class_getitem__()``, which allows this syntax to pass without error at
        runtime.

    .. change::
        :tags: bug, sql

        Fixed issue in lambda caching system where an element of a query that
        produces no cache key, like a custom option or clause element, would still
        populate the expression in the "lambda cache" inappropriately.

.. changelog::
    :version: 1.4.22
    :released: July 21, 2021

    .. change::
        :tags: bug, sql
        :tickets: 6786

        Fixed issue where use of the :paramref:`_sql.case.whens` parameter passing
        a dictionary positionally and not as a keyword argument would emit a 2.0
        deprecation warning, referring to the deprecation of passing a list
        positionally. The dictionary format of "whens", passed positionally, is
        still supported and was accidentally marked as deprecated.


    .. change::
        :tags: bug, orm
        :tickets: 6775

        Fixed issue in new :meth:`_schema.Table.table_valued` method where the
        resulting :class:`_sql.TableValuedColumn` construct would not respond
        correctly to alias adaptation as is used throughout the ORM, such as for
        eager loading, polymorphic loading, etc.


    .. change::
        :tags: bug, orm
        :tickets: 6769

        Fixed issue where usage of the :meth:`_result.Result.unique` method with an
        ORM result that included column expressions with unhashable types, such as
        ``JSON`` or ``ARRAY`` using non-tuples would silently fall back to using
        the ``id()`` function, rather than raising an error. This now raises an
        error when the :meth:`_result.Result.unique` method is used in a 2.0 style
        ORM query. Additionally, hashability is assumed to be True for result
        values of unknown type, such as often happens when using SQL functions of
        unknown return type; if values are truly not hashable then the ``hash()``
        itself will raise.

        For legacy ORM queries, since the legacy :class:`_orm.Query` object
        uniquifies in all cases, the old rules remain in place, which is to use
        ``id()`` for result values of unknown type as this legacy uniquing is
        mostly for the purpose of uniquing ORM entities and not column values.

    .. change::
        :tags: orm, bug
        :tickets: 6771

        Fixed an issue where clearing of mappers during things like test suite
        teardowns could cause a "dictionary changed size" warning during garbage
        collection, due to iteration of a weak-referencing dictionary. A ``list()``
        has been applied to prevent concurrent GC from affecting this operation.

    .. change::
        :tags: bug, sql
        :tickets: 6770

        Fixed issue where type-specific bound parameter handlers would not be
        called upon in the case of using the :meth:`_sql.Insert.values` method with
        the Python ``None`` value; in particular, this would be noticed when using
        the :class:`_types.JSON` datatype as well as related PostgreSQL specific
        types such as :class:`_postgresql.JSONB` which would fail to encode the
        Python ``None`` value into JSON null, however the issue was generalized to
        any bound parameter handler in conjunction with this specific method of
        :class:`_sql.Insert`.


    .. change::
        :tags: bug, engine
        :tickets: 6740

        Added some guards against ``KeyError`` in the event system to accommodate
        the case that the interpreter is shutting down at the same time
        :meth:`_engine.Engine.dispose` is being called, which would cause stack
        trace warnings.


    .. change::
        :tags: bug, orm, regression
        :tickets: 6793

        Fixed critical caching issue where the ORM's persistence feature using
        INSERT..RETURNING would cache an incorrect query when mixing the "bulk
        save" and standard "flush" forms of INSERT.

.. changelog::
    :version: 1.4.21
    :released: July 14, 2021

    .. change::
        :tags: usecase, orm
        :tickets: 6708

        Modified the approach used for history tracking of scalar object
        relationships that are not many-to-one, i.e. one-to-one relationships that
        would otherwise be one-to-many. When replacing a one-to-one value, the
        "old" value that would be replaced is no longer loaded immediately, and is
        instead handled during the flush process. This eliminates an historically
        troublesome lazy load that otherwise often occurs when assigning to a
        one-to-one attribute, and is particularly troublesome when using
        "lazy='raise'" as well as asyncio use cases.

        This change does cause a behavioral change within the
        :meth:`_orm.AttributeEvents.set` event, which is nonetheless currently
        documented, which is that the event applied to such a one-to-one attribute
        will no longer receive the "old" parameter if it is unloaded and the
        :paramref:`_orm.relationship.active_history` flag is not set. As is
        documented in :meth:`_orm.AttributeEvents.set`, if the event handler needs
        to receive the "old" value when the event fires off, the active_history
        flag must be established either with the event listener or with the
        relationship. This is already the behavior with other kinds of attributes
        such as many-to-one and column value references.

        The change additionally will defer updating a backref on the "old" value
        in the less common case that the "old" value is locally present in the
        session, but isn't loaded on the relationship in question, until the
        next flush occurs.  If this causes an issue, again the normal
        :paramref:`_orm.relationship.active_history` flag can be set to ``True``
        on the relationship.

    .. change::
        :tags: usecase, sql
        :tickets: 6752

        Added new method :meth:`_sql.HasCTE.add_cte` to each of the
        :func:`_sql.select`, :func:`_sql.insert`, :func:`_sql.update` and
        :func:`_sql.delete` constructs. This method will add the given
        :class:`_sql.CTE` as an "independent" CTE of the statement, meaning it
        renders in the WITH clause above the statement unconditionally even if it
        is not otherwise referenced in the primary statement. This is a popular use
        case on the PostgreSQL database where a CTE is used for a DML statement
        that runs against database rows independently of the primary statement.

    .. change::
        :tags: bug, postgresql
        :tickets: 6755

        Fixed issue in :meth:`_postgresql.Insert.on_conflict_do_nothing` and
        :meth:`_postgresql.Insert.on_conflict_do_update` where the name of a unique
        constraint passed as the ``constraint`` parameter would not be properly
        truncated for length if it were based on a naming convention that generated
        a too-long name for the PostgreSQL max identifier length of 63 characters,
        in the same way which occurs within a CREATE TABLE statement.

    .. change::
        :tags: bug, sql
        :tickets: 6710

        Fixed issue in CTE constructs where a recursive CTE that referred to a
        SELECT that has duplicate column names, which are typically deduplicated
        using labeling logic in 1.4, would fail to refer to the deduplicated label
        name correctly within the WITH clause.

    .. change::
        :tags: bug, regression, mssql
        :tickets: 6697

        Fixed regression where the special dotted-schema name handling for the SQL
        Server dialect would not function correctly if the dotted schema name were
        used within the ``schema_translate_map`` feature.

    .. change::
        :tags: orm, regression
        :tickets: 6718

        Fixed ORM regression where ad-hoc label names generated for hybrid
        properties and potentially other similar types of ORM-enabled expressions
        would usually be propagated outwards through subqueries, allowing the name
        to be retained in the final keys of the result set even when selecting from
        subqueries. Additional state is now tracked in this case that isn't lost
        when a hybrid is selected out of a Core select / subquery.


    .. change::
        :tags: bug, postgresql
        :tickets: 6739

        Fixed issue where the PostgreSQL ``ENUM`` datatype as embedded in the
        ``ARRAY`` datatype would fail to emit correctly in create/drop when the
        ``schema_translate_map`` feature were also in use. Additionally repairs a
        related issue where the same ``schema_translate_map`` feature would not
        work for the ``ENUM`` datatype in combination with a ``CAST``, that's also
        intrinsic to how the ``ARRAY(ENUM)`` combination works on the PostgreSQL
        dialect.


    .. change::
        :tags: bug, sql, regression
        :tickets: 6735

        Fixed regression where the :func:`_sql.tablesample` construct would fail to
        be executable when constructed given a floating-point sampling value not
        embedded within a SQL function.

    .. change::
        :tags: bug, postgresql
        :tickets: 6696

        Fixed issue in :meth:`_postgresql.Insert.on_conflict_do_nothing` and
        :meth:`_postgresql.Insert.on_conflict_do_update` where the name of a unique
        constraint passed as the ``constraint`` parameter would not be properly
        quoted if it contained characters which required quoting.


    .. change::
        :tags: bug, regression, orm
        :tickets: 6698

        Fixed regression caused in 1.4.19 due to :ticket:`6503` and related
        involving :meth:`_orm.Query.with_entities` where the new structure used
        would be inappropriately transferred to an enclosing :class:`_orm.Query`
        when making use of set operations such as :meth:`_orm.Query.union`, causing
        the JOIN instructions within to be applied to the outside query as well.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6762

        Fixed regression which appeared in version 1.4.3 due to :ticket:`6060`
        where rules that limit ORM adaptation of derived selectables interfered
        with other ORM-adaptation based cases, in this case when applying
        adaptations for a :func:`_orm.with_polymorphic` against a mapping which
        uses a :func:`_orm.column_property` which in turn makes use of a scalar
        select that includes a :func:`_orm.aliased` object of the mapped table.

.. changelog::
    :version: 1.4.20
    :released: June 28, 2021

    .. change::
        :tags: bug, regression, orm
        :tickets: 6680

        Fixed regression in ORM regarding an internal reconstitution step for the
        :func:`_orm.with_polymorphic` construct, when the user-facing object is
        garbage collected as the query is processed. The reconstitution was not
        ensuring the sub-entities for the "polymorphic" case were handled, leading
        to an ``AttributeError``.

    .. change::
        :tags: usecase, sql
        :tickets: 6646

        Add a impl parameter to :class:`_types.PickleType` constructor, allowing
        any arbitrary type to be used in place of the default implementation of
        :class:`_types.LargeBinary`. Pull request courtesy jason3gb.

    .. change::
        :tags: bug, engine
        :tickets: 5348

        Fixed an issue in the C extension for the :class:`_result.Row` class which
        could lead to a memory leak in the unlikely case of a :class:`_result.Row`
        object which referred to an ORM object that then was mutated to refer back
        to the ``Row`` itself, creating a cycle. The Python C APIs for tracking GC
        cycles has been added to the native :class:`_result.Row` implementation to
        accommodate for this case.


    .. change::
        :tags: bug, engine
        :tickets: 6665

        Fixed old issue where a :func:`_sql.select()` made against the token "*",
        which then yielded exactly one column, would fail to correctly organize the
        ``cursor.description`` column name into the keys of the result object.



    .. change::
        :tags: usecase, mysql
        :tickets: 6659

        Made a small adjustment in the table reflection feature of the MySQL
        dialect to accommodate for alternate MySQL-oriented databases such as TiDB
        which include their own "comment" directives at the end of a constraint
        directive within "CREATE TABLE" where the format doesn't have the
        additional space character after the comment, in this case the TiDB
        "clustered index" feature. Pull request courtesy Daniël van Eeden.

    .. change::
        :tags: bug, schema
        :tickets: 6685

        Fixed issue where passing ``None`` for the value of
        :paramref:`_schema.Table.prefixes` would not store an empty list, but
        rather the constant ``None``, which may be unexpected by third party
        dialects. The issue is revealed by a usage in recent versions of Alembic
        that are passing ``None`` for this value. Pull request courtesy Kai
        Mueller.

    .. change::
        :tags: bug, regression, ext
        :tickets: 6679

        Fixed regression in :mod:`sqlalchemy.ext.automap` extension such that the
        use case of creating an explicit mapped class to a table that is also the
        :paramref:`_orm.relationship.secondary` element of a
        :func:`_orm.relationship` that automap will be generating would emit
        the "overlaps" warnings introduced in 1.4 and discussed at
        :ref:`error_qzyx`. While generating this case from automap is still
        subject to the same caveats mentioned in the 'overlaps' warning,
        since automap is primarily intended for more ad-hoc
        use cases, the condition triggering the warning is disabled when a
        many-to-many relationship with this specific pattern is
        generated.


    .. change::
        :tags: bug, regression, orm
        :tickets: 6678

        Adjusted :meth:`_orm.Query.union` and similar set operations to be
        correctly compatible with the new capabilities just added in
        :ticket:`6661`, with SQLAlchemy 1.4.19, such that the SELECT statements
        rendered as elements of the UNION or other set operation will include
        directly mapped columns that are mapped as deferred; this both fixes a
        regression involving unions with multiple levels of nesting that would
        produce a column mismatch, and also allows the :func:`_orm.undefer` option
        to be used at the top level of such a :class:`_orm.Query` without having to
        apply the option to each of the elements within the UNION.

    .. change::
        :tags: bug, sql, orm
        :tickets: 6668

        Fixed the class hierarchy for the :class:`_schema.Sequence` and the more
        general :class:`_schema.DefaultGenerator` base, as these are "executable"
        as statements they need to include :class:`_sql.Executable` in their
        hierarchy, not just :class:`_roles.StatementRole` as was applied
        arbitrarily to :class:`_schema.Sequence` previously. The fix allows
        :class:`_schema.Sequence` to work in all ``.execute()`` methods including
        with :meth:`_orm.Session.execute` which was not working in the case that a
        :meth:`_orm.SessionEvents.do_orm_execute` handler was also established.


    .. change::
        :tags: bug, orm
        :tickets: 6538

        Adjusted the check in the mapper for a callable object that is used as a
        ``@validates`` validator function or a ``@reconstructor`` reconstruction
        function, to check for "callable" more liberally such as to accommodate
        objects based on fundamental attributes like ``__func__`` and
        ``__call__``, rather than testing for ``MethodType`` / ``FunctionType``,
        allowing things like cython functions to work properly. Pull request
        courtesy Miłosz Stypiński.

.. changelog::
    :version: 1.4.19
    :released: June 22, 2021

    .. change::
        :tags: bug, mssql
        :tickets: 6658

        Fixed bug where the "schema_translate_map" feature would fail to function
        correctly in conjunction with an INSERT into a table that has an IDENTITY
        column, where the value of the IDENTITY column were specified in the values
        of the INSERT thus triggering SQLAlchemy's feature of setting IDENTITY
        INSERT to "on"; it's in this directive where the schema translate map would
        fail to be honored.


    .. change::
        :tags: bug, sql
        :tickets: 6663

        Fixed issue in CTE constructs mostly relevant to ORM use cases where a
        recursive CTE against "anonymous" labels such as those seen in ORM
        ``column_property()`` mappings would render in the
        ``WITH RECURSIVE xyz(...)`` section as their raw internal label and not a
        cleanly anonymized name.

    .. change::
        :tags: mssql, change
        :tickets: 6503, 6253

        Made improvements to the server version regexp used by the pymssql dialect
        to prevent a regexp overflow in case of an invalid version string.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6503, 6253

        Fixed further regressions in the same area as that of :ticket:`6052` where
        loader options as well as invocations of methods like
        :meth:`_orm.Query.join` would fail if the left side of the statement for
        which the option/join depends upon were replaced by using the
        :meth:`_orm.Query.with_entities` method, or when using 2.0 style queries
        when using the :meth:`_sql.Select.with_only_columns` method. A new set of
        state has been added to the objects which tracks the "left" entities that
        the options / join were made against which is memoized when the lead
        entities are changed.

    .. change::
        :tags: bug, asyncio, postgresql
        :tickets: 6652

        Fixed bug in asyncio implementation where the greenlet adaptation system
        failed to propagate ``BaseException`` subclasses, most notably including
        ``asyncio.CancelledError``, to the exception handling logic used by the
        engine to invalidate and clean up the connection, thus preventing
        connections from being correctly disposed when a task was cancelled.



    .. change::
        :tags: usecase, asyncio
        :tickets: 6583

        Implemented :class:`_asyncio.async_scoped_session` to address some
        asyncio-related incompatibilities between :class:`_orm.scoped_session` and
        :class:`_asyncio.AsyncSession`, in which some methods (notably the
        :meth:`_asyncio.async_scoped_session.remove` method) should be used with
        the ``await`` keyword.

        .. seealso::

            :ref:`asyncio_scoped_session`

    .. change::
        :tags: usecase, mysql
        :tickets: 6132

        Added new construct :class:`_mysql.match`, which provides for the full
        range of MySQL's MATCH operator including multiple column support and
        modifiers. Pull request courtesy Anton Kovalevich.

        .. seealso::

            :class:`_mysql.match`

    .. change::
        :tags: bug, postgresql, oracle
        :tickets: 6649

        Fixed issue where the ``INTERVAL`` datatype on PostgreSQL and Oracle would
        produce an ``AttributeError`` when used in the context of a comparison
        operation against a ``timedelta()`` object. Pull request courtesy
        MajorDallas.

    .. change::
        :tags: bug, mypy
        :tickets: 6476

        Fixed issue in mypy plugin where class info for a custom declarative base
        would not be handled correctly on a cached mypy pass, leading to an
        AssertionError being raised.

    .. change::
        :tags: bug, orm
        :tickets: 6661

        Refined the behavior of ORM subquery rendering with regards to deferred
        columns and column properties to be more compatible with that of 1.3 while
        also providing for 1.4's newer features. As a subquery in 1.4 does not make
        use of loader options, including :func:`_orm.undefer`, a subquery that is
        against an ORM entity with deferred attributes will now render those
        deferred attributes that refer directly to mapped table columns, as these
        are needed in the outer SELECT if that outer SELECT makes use of these
        columns; however a deferred attribute that refers to a composed SQL
        expression as we normally do with :func:`_orm.column_property` will not be
        part of the subquery, as these can be selected explicitly if needed in the
        subquery. If the entity is being SELECTed from this subquery, the column
        expression can still render on "the outside" in terms of the derived
        subquery columns. This produces essentially the same behavior as when
        working with 1.3. However in this case the fix has to also make sure that
        the ``.selected_columns`` collection of an ORM-enabled :func:`_sql.select`
        also follows these rules, which in particular allows recursive CTEs to
        render correctly in this scenario, which were previously failing to render
        correctly due to this issue.

    .. change::
        :tags: bug, postgresql
        :tickets: 6621

        Fixed issue where the pool "pre ping" feature would implicitly start a
        transaction, which would then interfere with custom transactional flags
        such as PostgreSQL's "read only" mode when used with the psycopg2 driver.


.. changelog::
    :version: 1.4.18
    :released: June 10, 2021

    .. change::
        :tags: bug, orm
        :tickets: 6072, 6487

        Clarified the current purpose of the
        :paramref:`_orm.relationship.bake_queries` flag, which in 1.4 is to enable
        or disable "lambda caching" of statements within the "lazyload" and
        "selectinload" loader strategies; this is separate from the more
        foundational SQL query cache that is used for most statements.
        Additionally, the lazy loader no longer uses its own cache for many-to-one
        SQL queries, which was an implementation quirk that doesn't exist for any
        other loader scenario. Finally, the "lru cache" warning that the lazyloader
        and selectinloader strategies could emit when handling a wide array of
        class/relationship combinations has been removed; based on analysis of some
        end-user cases, this warning doesn't suggest any significant issue. While
        setting ``bake_queries=False`` for such a relationship will remove this
        cache from being used, there's no particular performance gain in this case
        as using no caching vs. using a cache that needs to refresh often likely
        still wins out on the caching being used side.


    .. change::
        :tags: bug, asyncio
        :tickets: 6575

        Fixed an issue that presented itself when using the :class:`_pool.NullPool`
        or the :class:`_pool.StaticPool` with an async engine. This mostly affected
        the aiosqlite dialect.

    .. change::
        :tags: bug, sqlite, regression
        :tickets: 6586

        The fix for pysqlcipher released in version 1.4.3 :ticket:`5848` was
        unfortunately non-working, in that the new ``on_connect_url`` hook was
        erroneously not receiving a ``URL`` object under normal usage of
        :func:`_sa.create_engine` and instead received a string that was unhandled;
        the test suite failed to fully set up the actual conditions under which
        this hook is called. This has been fixed.

    .. change::
        :tags: bug, postgresql, regression
        :tickets: 6581

        Fixed regression where using the PostgreSQL "INSERT..ON CONFLICT" structure
        would fail to work with the psycopg2 driver if it were used in an
        "executemany" context along with bound parameters in the "SET" clause, due
        to the implicit use of the psycopg2 fast execution helpers which are not
        appropriate for this style of INSERT statement; as these helpers are the
        default in 1.4 this is effectively a regression.  Additional checks to
        exclude this kind of statement from that particular extension have been
        added.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6285

        Adjusted the means by which classes such as :class:`_orm.scoped_session`
        and :class:`_asyncio.AsyncSession` are generated from the base
        :class:`_orm.Session` class, such that custom :class:`_orm.Session`
        subclasses such as that used by Flask-SQLAlchemy don't need to implement
        positional arguments when they call into the superclass method, and can
        continue using the same argument styles as in previous releases.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6595

        Fixed issue where query production for joinedload against a complex left
        hand side involving joined-table inheritance could fail to produce a
        correct query, due to a clause adaption issue.

    .. change::
        :tags: bug, orm, regression, performance
        :tickets: 6596

        Fixed regression involving how the ORM would resolve a given mapped column
        to a result row, where under cases such as joined eager loading, a slightly
        more expensive "fallback" could take place to set up this resolution due to
        some logic that was removed since 1.3. The issue could also cause
        deprecation warnings involving column resolution to be emitted when using a
        1.4 style query with joined eager loading.

    .. change::
        :tags: bug, orm
        :tickets: 6591

        Fixed issue in experimental "select ORM objects from INSERT/UPDATE" use
        case where an error was raised if the statement were against a
        single-table-inheritance subclass.

    .. change::
        :tags: bug, asyncio
        :tickets: 6592

        Added ``asyncio.exceptions.TimeoutError``,
        ``asyncio.exceptions.CancelledError`` as so-called "exit exceptions", a
        class of exceptions that include things like ``GreenletExit`` and
        ``KeyboardInterrupt``, which are considered to be events that warrant
        considering a DBAPI connection to be in an unusable state where it should
        be recycled.

    .. change::
        :tags: bug, orm
        :tickets: 6400

        The warning that's emitted for :func:`_orm.relationship` when multiple
        relationships would overlap with each other as far as foreign key
        attributes written towards, now includes the specific "overlaps" argument
        to use for each warning in order to silence the warning without changing
        the mapping.

    .. change::
        :tags: usecase, asyncio
        :tickets: 6319

        Implemented a new registry architecture that allows the ``Async`` version
        of an object, like ``AsyncSession``, ``AsyncConnection``, etc., to be
        locatable given the proxied "sync" object, i.e. ``Session``,
        ``Connection``. Previously, to the degree such lookup functions were used,
        an ``Async`` object would be re-created each time, which was less than
        ideal as the identity and state of the "async" object would not be
        preserved across calls.

        From there, new helper functions :func:`_asyncio.async_object_session`,
        :func:`_asyncio.async_session` as well as a new :class:`_orm.InstanceState`
        attribute :attr:`_orm.InstanceState.async_session` have been added, which
        are used to retrieve the original :class:`_asyncio.AsyncSession` associated
        with an ORM mapped object, a :class:`_orm.Session` associated with an
        :class:`_asyncio.AsyncSession`, and an :class:`_asyncio.AsyncSession`
        associated with an :class:`_orm.InstanceState`, respectively.

        This patch also implements new methods
        :meth:`_asyncio.AsyncSession.in_nested_transaction`,
        :meth:`_asyncio.AsyncSession.get_transaction`,
        :meth:`_asyncio.AsyncSession.get_nested_transaction`.

.. changelog::
    :version: 1.4.17
    :released: May 29, 2021

    .. change::
        :tags: bug, orm, regression
        :tickets: 6558

        Fixed regression caused by just-released performance fix mentioned in #6550
        where a query.join() to a relationship could produce an AttributeError if
        the query were made against non-ORM structures only, a fairly unusual
        calling pattern.

.. changelog::
    :version: 1.4.16
    :released: May 28, 2021

    .. change::
        :tags: bug, engine
        :tickets: 6482

        Fixed issue where an ``@`` sign in the database portion of a URL would not
        be interpreted correctly if the URL also had a username:password section.


    .. change::
        :tags: bug, ext
        :tickets: 6529

        Fixed a deprecation warning that was emitted when using
        :func:`_automap.automap_base` without passing an existing
        ``Base``.


    .. change::
        :tags: bug, pep484
        :tickets: 6461

        Remove pep484 types from the code.
        Current effort is around the stub package, and having typing in
        two places makes thing worse, since the types in the SQLAlchemy
        source were usually outdated compared to the version in the stubs.

    .. change::
        :tags: usecase, mssql
        :tickets: 6464

        Implemented support for a :class:`_sql.CTE` construct to be used directly
        as the target of a :func:`_sql.delete` construct, i.e. "WITH ... AS cte
        DELETE FROM cte". This appears to be a useful feature of SQL Server.

    .. change::
        :tags: bug, general
        :tickets: 6540, 6543

        Resolved various deprecation warnings which were appearing as of Python
        version 3.10.0b1.

    .. change::
        :tags: bug, orm
        :tickets: 6471

        Fixed issue when using :paramref:`_orm.relationship.cascade_backrefs`
        parameter set to ``False``, which per :ref:`change_5150` is set to become
        the standard behavior in SQLAlchemy 2.0, where adding the item to a
        collection that uniquifies, such as ``set`` or ``dict`` would fail to fire
        a cascade event if the object were already associated in that collection
        via the backref. This fix represents a fundamental change in the collection
        mechanics by introducing a new event state which can fire off for a
        collection mutation even if there is no net change on the collection; the
        action is now suited using a new event hook
        :meth:`_orm.AttributeEvents.append_wo_mutation`.



    .. change::
        :tags: bug, orm, regression
        :tickets: 6550

        Fixed regression involving clause adaption of labeled ORM compound
        elements, such as single-table inheritance discriminator expressions with
        conditionals or CASE expressions, which could cause aliased expressions
        such as those used in ORM join / joinedload operations to not be adapted
        correctly, such as referring to the wrong table in the ON clause in a join.

        This change also improves a performance bump that was located within the
        process of invoking :meth:`_sql.Select.join` given an ORM attribute
        as a target.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6495

        Fixed regression where the full combination of joined inheritance, global
        with_polymorphic, self-referential relationship and joined loading would
        fail to be able to produce a query with the scope of lazy loads and object
        refresh operations that also attempted to render the joined loader.

    .. change::
        :tags: bug, engine
        :tickets: 6329

        Fixed a long-standing issue with :class:`.URL` where query parameters
        following the question mark would not be parsed correctly if the URL did
        not contain a database portion with a backslash.

    .. change::
        :tags: bug, sql, regression
        :tickets: 6549

        Fixed regression in dynamic loader strategy and :func:`_orm.relationship`
        overall where the :paramref:`_orm.relationship.order_by` parameter were
        stored as a mutable list, which could then be mutated when combined with
        additional "order_by" methods used against the dynamic query object,
        causing the ORDER BY criteria to continue to grow repetitively.

    .. change::
        :tags: bug, orm
        :tickets: 6484

        Enhanced the bind resolution rules for :meth:`_orm.Session.execute` so that
        when a non-ORM statement such as an :func:`_sql.insert` construct
        nonetheless is built against ORM objects, to the greatest degree possible
        the ORM entity will be used to resolve the bind, such as for a
        :class:`_orm.Session` that has a bind map set up on a common superclass
        without specific mappers or tables named in the map.

    .. change::
        :tags: bug, regression, ext
        :tickets: 6390

        Fixed regression in the ``sqlalchemy.ext.instrumentation`` extension that
        prevented instrumentation disposal from working completely. This fix
        includes both a 1.4 regression fix as well as a fix for a related issue
        that existed in 1.3 also.   As part of this change, the
        :class:`sqlalchemy.ext.instrumentation.InstrumentationManager` class now
        has a new method ``unregister()``, which replaces the previous method
        ``dispose()``, which was not called as of version 1.4.


.. changelog::
    :version: 1.4.15
    :released: May 11, 2021

    .. change::
        :tags: bug, documentation, mysql
        :tickets: 5397

        Added support for the ``ssl_check_hostname=`` parameter in mysql connection
        URIs and updated the mysql dialect documentation regarding secure
        connections. Original pull request courtesy of Jerry Zhao.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6449

        Fixed additional regression caused by "eager loaders run on unexpire"
        feature :ticket:`1763` where the feature would run for a
        ``contains_eager()`` eagerload option in the case that the
        ``contains_eager()`` were chained to an additional eager loader option,
        which would then produce an incorrect query as the original query-bound
        join criteria were no longer present.

    .. change::
        :tags: feature, general
        :tickets: 6241

        A new approach has been applied to the warnings system in SQLAlchemy to
        accurately predict the appropriate stack level for each warning
        dynamically. This allows evaluating the source of SQLAlchemy-generated
        warnings and deprecation warnings to be more straightforward as the warning
        will indicate the source line within end-user code, rather than from an
        arbitrary level within SQLAlchemy's own source code.

    .. change::
        :tags: bug, orm
        :tickets: 6459

        Fixed issue in subquery loader strategy which prevented caching from
        working correctly. This would have been seen in the logs as a "generated"
        message instead of "cached" for all subqueryload SQL emitted, which by
        saturating the cache with new keys would degrade overall performance; it
        also would produce "LRU size alert" warnings.


    .. change::
        :tags: bug, sql
        :tickets: 6460

        Adjusted the logic added as part of :ticket:`6397` in 1.4.12 so that
        internal mutation of the :class:`.BindParameter` object occurs within the
        clause construction phase as it did before, rather than in the compilation
        phase. In the latter case, the mutation still produced side effects against
        the incoming construct and additionally could potentially interfere with
        other internal mutation routines.

.. changelog::
    :version: 1.4.14
    :released: May 6, 2021

    .. change::
        :tags: bug, regression, orm
        :tickets: 6426

        Fixed regression involving ``lazy='dynamic'`` loader in conjunction with a
        detached object. The previous behavior was that the dynamic loader upon
        calling methods like ``.all()`` returns empty lists for detached objects
        without error, this has been restored; however a warning is now emitted as
        this is not the correct result. Other dynamic loader scenarios correctly
        raise ``DetachedInstanceError``.

    .. change::
        :tags: bug, regression, sql
        :tickets: 6428

        Fixed regression caused by the "empty in" change just made in
        :ticket:`6397` 1.4.12 where the expression needs to be parenthesized for
        the "not in" use case, otherwise the condition will interfere with the
        other filtering criteria.


    .. change::
        :tags: bug, sql, regression
        :tickets: 6436

        The :class:`.TypeDecorator` class will now emit a warning when used in SQL
        compilation with caching unless the ``.cache_ok`` flag is set to ``True``
        or ``False``. A new class-level attribute :attr:`.TypeDecorator.cache_ok`
        may be set which will be used as an indication that all the parameters
        passed to the object are safe to be used as a cache key if set to ``True``,
        ``False`` means they are not.

    .. change::
        :tags: engine, bug, regression
        :tickets: 6427

        Established a deprecation path for calling upon the
        :meth:`_cursor.CursorResult.keys` method for a statement that returns no
        rows to provide support for legacy patterns used by the "records" package
        as well as any other non-migrated applications. Previously, this would
        raise :class:`.ResourceClosedException` unconditionally in the same way as
        it does when attempting to fetch rows. While this is the correct behavior
        going forward, the ``LegacyCursorResult`` object will now in
        this case return an empty list for ``.keys()`` as it did in 1.3, while also
        emitting a 2.0 deprecation warning. The :class:`_cursor.CursorResult`, used
        when using a 2.0-style "future" engine, will continue to raise as it does
        now.

    .. change::
        :tags: usecase, engine, orm
        :tickets: 6288

        Applied consistent behavior to the use case of
        calling ``.commit()`` or ``.rollback()`` inside of an existing
        ``.begin()`` context manager, with the addition of potentially
        emitting SQL within the block subsequent to the commit or rollback.
        This change continues upon the change first added in
        :ticket:`6155` where the use case of calling "rollback" inside of
        a ``.begin()`` contextmanager block was proposed:

        * calling ``.commit()`` or ``.rollback()`` will now be allowed
          without error or warning within all scopes, including
          that of legacy and future :class:`_engine.Engine`, ORM
          :class:`_orm.Session`, asyncio :class:`.AsyncEngine`.  Previously,
          the :class:`_orm.Session` disallowed this.

        * The remaining scope of the context manager is then closed;
          when the block ends, a check is emitted to see if the transaction
          was already ended, and if so the block returns without action.

        * It will now raise **an error** if subsequent SQL of any kind
          is emitted within the block, **after** ``.commit()`` or
          ``.rollback()`` is called.   The block should be closed as
          the state of the executable object would otherwise be undefined
          in this state.

.. changelog::
    :version: 1.4.13
    :released: May 3, 2021

    .. change::
        :tags: bug, regression, orm
        :tickets: 6410

        Fixed regression in ``selectinload`` loader strategy that would cause it to
        cache its internal state incorrectly when handling relationships that join
        across more than one column, such as when using a composite foreign key.
        The invalid caching would then cause other unrelated loader operations to
        fail.


    .. change::
        :tags: bug, orm, regression
        :tickets: 6414

        Fixed regression where :meth:`_orm.Query.filter_by` would not work if the
        lead entity were a SQL function or other expression derived from the
        primary entity in question, rather than a simple entity or column of that
        entity. Additionally, improved the behavior of
        :meth:`_sql.Select.filter_by` overall to work with column expressions even
        in a non-ORM context.

    .. change::
        :tags: bug, engine, regression
        :tickets: 6408

        Restored a legacy transactional behavior that was inadvertently removed
        from the :class:`_engine.Connection` as it was never tested as a known use
        case in previous versions, where calling upon the
        :meth:`_engine.Connection.begin_nested` method, when no transaction is
        present, does not create a SAVEPOINT at all and instead starts an outer
        transaction, returning a :class:`.RootTransaction` object instead of a
        :class:`.NestedTransaction` object.  This :class:`.RootTransaction` then
        will emit a real COMMIT on the database connection when committed.
        Previously, the 2.0 style behavior was present in all cases that would
        autobegin a transaction but not commit it, which is a behavioral change.

        When using a :term:`2.0 style` connection object, the behavior is unchanged
        from previous 1.4 versions; calling :meth:`_engine.Connection.begin_nested`
        will "autobegin" the outer transaction if not already present, and then as
        instructed emit a SAVEPOINT, returning the :class:`.NestedTransaction`
        object. The outer transaction is committed by calling upon
        :meth:`_engine.Connection.commit`, as is "commit-as-you-go" style usage.

        In non-"future" mode, while the old behavior is restored, it also
        emits a 2.0 deprecation warning as this is a legacy behavior.


    .. change::
        :tags: bug, asyncio, regression
        :tickets: 6409

        Fixed a regression introduced by :ticket:`6337` that would create an
        ``asyncio.Lock`` which could be attached to the wrong loop when
        instantiating the async engine before any asyncio loop was started, leading
        to an asyncio error message when attempting to use the engine under certain
        circumstances.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6419

        Fixed regression where using :func:`_orm.selectinload` and
        :func:`_orm.subqueryload` to load a two-level-deep path would lead to an
        attribute error.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6420

        Fixed regression where using the :func:`_orm.noload` loader strategy in
        conjunction with a "dynamic" relationship would lead to an attribute error
        as the noload strategy would attempt to apply itself to the dynamic loader.

    .. change::
        :tags: usecase, postgresql
        :tickets: 6198

        Add support for server side cursors in the pg8000 dialect for PostgreSQL.
        This allows use of the
        :paramref:`.Connection.execution_options.stream_results` option.

.. changelog::
    :version: 1.4.12
    :released: April 29, 2021

    .. change::
        :tags: bug, orm, regression, caching
        :tickets: 6391

        Fixed critical regression where bound parameter tracking as used in the SQL
        caching system could fail to track all parameters for the case where the
        same SQL expression containing a parameter were used in an ORM-related
        query using a feature such as class inheritance, which was then embedded in
        an enclosing expression which would make use of that same expression
        multiple times, such as a UNION. The ORM would individually copy the
        individual SELECT statements as part of compilation with class inheritance,
        which then embedded in the enclosing statement would fail to accommodate
        for all parameters. The logic that tracks this condition has been adjusted
        to work for multiple copies of a parameter.

    .. change::
        :tags: bug, sql
        :tickets: 6258, 6397

        Revised the "EMPTY IN" expression to no longer rely upon using a subquery,
        as this was causing some compatibility and performance problems. The new
        approach for selected databases takes advantage of using a NULL-returning
        IN expression combined with the usual "1 != 1" or "1 = 1" expression
        appended by AND or OR. The expression is now the default for all backends
        other than SQLite, which still had some compatibility issues regarding
        tuple "IN" for older SQLite versions.

        Third party dialects can still override how the "empty set" expression
        renders by implementing a new compiler method
        ``def visit_empty_set_op_expr(self, type_, expand_op)``, which takes
        precedence over the existing
        ``def visit_empty_set_expr(self, element_types)`` which remains in place.


    .. change::
        :tags: bug, orm
        :tickets: 6350

        Fixed two distinct issues mostly affecting
        :class:`_hybrid.hybrid_property`, which would come into play under common
        mis-configuration scenarios that were silently ignored in 1.3, and now
        failed in 1.4, where the "expression" implementation would return a non
        :class:`_sql.ClauseElement` such as a boolean value. For both issues, 1.3's
        behavior was to silently ignore the mis-configuration and ultimately
        attempt to interpret the value as a SQL expression, which would lead to an
        incorrect query.

        * Fixed issue regarding interaction of the attribute system with
          hybrid_property, where if the ``__clause_element__()`` method of the
          attribute returned a non-:class:`_sql.ClauseElement` object, an internal
          ``AttributeError`` would lead the attribute to return the ``expression``
          function on the hybrid_property itself, as the attribute error was
          against the name ``.expression`` which would invoke the ``__getattr__()``
          method as a fallback. This now raises explicitly. In 1.3 the
          non-:class:`_sql.ClauseElement` was returned directly.

        * Fixed issue in SQL argument coercions system where passing the wrong
          kind of object to methods that expect column expressions would fail if
          the object were altogether not a SQLAlchemy object, such as a Python
          function, in cases where the object were not just coerced into a bound
          value. Again 1.3 did not have a comprehensive argument coercion system
          so this case would also pass silently.


    .. change::
        :tags: bug, orm
        :tickets: 6378

        Fixed issue where using a :class:`_sql.Select` as a subquery in an ORM
        context would modify the :class:`_sql.Select` in place to disable
        eagerloads on that object, which would then cause that same
        :class:`_sql.Select` to not eagerload if it were then re-used in a
        top-level execution context.


    .. change::
        :tags: bug, regression, sql
        :tickets: 6343

        Fixed regression where usage of the :func:`_sql.text` construct inside the
        columns clause of a :class:`_sql.Select` construct, which is better handled
        by using a :func:`_sql.literal_column` construct, would nonetheless prevent
        constructs like :func:`_sql.union` from working correctly. Other use cases,
        such as constructing subqueries, continue to work the same as in prior
        versions where the :func:`_sql.text` construct is silently omitted from the
        collection of exported columns.   Also repairs similar use within the
        ORM.


    .. change::
        :tags: bug, regression, sql
        :tickets: 6261

        Fixed regression involving legacy methods such as
        :meth:`_sql.Select.append_column` where internal assertions would fail.

    .. change::
        :tags: usecase, sqlite
        :tickets: 6379

        Default to using ``SingletonThreadPool`` for in-memory SQLite databases
        created using URI filenames. Previously the default pool used was the
        ``NullPool`` that precented sharing the same database between multiple
        engines.

    .. change::
        :tags: bug, regression, sql
        :tickets: 6300

        Fixed regression caused by :ticket:`5395` where tuning back the check for
        sequences in :func:`_sql.select` now caused failures when doing 2.0-style
        querying with a mapped class that also happens to have an ``__iter__()``
        method. Tuned the check some more to accommodate this as well as some other
        interesting ``__iter__()`` scenarios.


    .. change::
        :tags: bug, mssql, schema
        :tickets: 6345

        Add :meth:`_types.TypeEngine.as_generic` support for
        :class:`sqlalchemy.dialects.mysql.BIT` columns, mapping
        them to :class:`_sql.sqltypes.Boolean`.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6360, 6359

        Fixed issue where the new :ref:`autobegin <session_autobegin>` behavior
        failed to "autobegin" in the case where an existing persistent object has
        an attribute change, which would then impact the behavior of
        :meth:`_orm.Session.rollback` in that no snapshot was created to be rolled
        back. The "attribute modify" mechanics have been updated to ensure
        "autobegin", which does not perform any database work, does occur when
        persistent attributes change in the same manner as when
        :meth:`_orm.Session.add` is called. This is a regression as in 1.3, the
        rollback() method always had a transaction to roll back and would expire
        every time.

    .. change::
        :tags: bug, mssql, regression
        :tickets: 6366

        Fixed regression caused by :ticket:`6306` which added support for
        ``DateTime(timezone=True)``, where the previous behavior of the pyodbc
        driver of implicitly dropping the tzinfo from a timezone-aware date when
        INSERTing into a timezone-naive DATETIME column were lost, leading to a SQL
        Server error when inserting timezone-aware datetime objects into
        timezone-native database columns.

    .. change::
        :tags: orm, bug, regression
        :tickets: 6386

        Fixed regression in ORM where using hybrid property to indicate an
        expression from a different entity would confuse the column-labeling logic
        in the ORM and attempt to derive the name of the hybrid from that other
        class, leading to an attribute error. The owning class of the hybrid
        attribute is now tracked along with the name.

    .. change::
        :tags: orm, bug, regression
        :tickets: 6401

        Fixed regression in hybrid_property where a hybrid against a SQL function
        would generate an ``AttributeError`` when attempting to generate an entry
        for the ``.c`` collection of a subquery in some cases; among other things
        this would impact its use in cases like that of ``Query.count()``.


    .. change::
        :tags: bug, postgresql
        :tickets: 6373

        Fixed very old issue where the :class:`_types.Enum` datatype would not
        inherit the :paramref:`_schema.MetaData.schema` parameter of a
        :class:`_schema.MetaData` object when that object were passed to the
        :class:`_types.Enum` using :paramref:`_types.Enum.metadata`.

    .. change::
        :tags: bug, orm, dataclasses
        :tickets: 6346

        Adjusted the declarative scan for dataclasses so that the inheritance
        behavior of :func:`_orm.declared_attr` established on a mixin, when using
        the new form of having it inside of a ``dataclasses.field()`` construct and
        not actually a descriptor attribute on the class, correctly accommodates
        the case when the target class to be mapped is a subclass of an existing
        mapped class which has already mapped that :func:`_orm.declared_attr`, and
        therefore should not be re-applied to this class.


    .. change::
        :tags: bug, schema, mysql, mariadb, oracle, postgresql
        :tickets: 6338

        Ensure that the MySQL and MariaDB dialect ignore the
        :class:`_sql.Identity` construct while rendering the ``AUTO_INCREMENT``
        keyword in a create table.

        The Oracle and PostgreSQL compiler was updated to not render
        :class:`_sql.Identity` if the database version does not support it
        (Oracle < 12 and PostgreSQL < 10). Previously it was rendered regardless
        of the database version.

    .. change::
        :tags: bug, orm
        :tickets: 6353

        Fixed an issue with the (deprecated in 1.4)
        :meth:`_schema.ForeignKeyConstraint.copy` method that caused an error when
        invoked with the ``schema`` argument.

    .. change::
        :tags: bug, engine
        :tickets: 6361

        Fixed issue where usage of an explicit :class:`.Sequence` would produce
        inconsistent "inline" behavior for an :class:`_sql.Insert` construct that
        includes multiple values phrases; the first seq would be inline but
        subsequent ones would be "pre-execute", leading to inconsistent sequence
        ordering. The sequence expressions are now fully inline.

.. changelog::
    :version: 1.4.11
    :released: April 21, 2021

    .. change::
        :tags: bug, engine, regression
        :tickets: 6337

        Fixed critical regression caused by the change in :ticket:`5497` where the
        connection pool "init" phase no longer occurred within mutexed isolation,
        allowing other threads to proceed with the dialect uninitialized, which
        could then impact the compilation of SQL statements.


    .. change::
        :tags: bug, orm, regression, declarative
        :tickets: 6331

        Fixed regression where recent changes to support Python dataclasses had the
        inadvertent effect that an ORM mapped class could not successfully override
        the ``__new__()`` method.

.. changelog::
    :version: 1.4.10
    :released: April 20, 2021

    .. change::
        :tags: bug, declarative, regression
        :tickets: 6291

        Fixed :func:`_declarative.instrument_declarative` that called
        a non existing registry method.

    .. change::
        :tags: bug, orm
        :tickets: 6320

        Fixed bug in new :func:`_orm.with_loader_criteria` feature where using a
        mixin class with :func:`_orm.declared_attr` on an attribute that were
        accessed inside the custom lambda would emit a warning regarding using an
        unmapped declared attr, when the lambda callable were first initialized.
        This warning is now prevented using special instrumentation for this
        lambda initialization step.


    .. change::
        :tags: usecase, mssql
        :tickets: 6306

        The :paramref:`_types.DateTime.timezone` parameter when set to ``True``
        will now make use of the ``DATETIMEOFFSET`` column type with SQL Server
        when used to emit DDL, rather than ``DATETIME`` where the flag was silently
        ignored.

    .. change::
        :tags: orm, bug, regression
        :tickets: 6326

        Fixed additional regression caused by the "eagerloaders on refresh" feature
        added in :ticket:`1763` where the refresh operation historically would set
        ``populate_existing``, which given the new feature now overwrites pending
        changes on eagerly loaded objects when autoflush is false. The
        populate_existing flag has been turned off for this case and a more
        specific method used to ensure the correct attributes refreshed.

    .. change::
        :tags: bug, orm, result
        :tickets: 6299

        Fixed an issue when using 2.0 style execution that prevented using
        :meth:`_result.Result.scalar_one` or
        :meth:`_result.Result.scalar_one_or_none` after calling
        :meth:`_result.Result.unique`, for the case where the ORM is returning a
        single-element row in any case.

    .. change::
        :tags: bug, sql
        :tickets: 6327

        Fixed issue in SQL compiler where the bound parameters set up for a
        :class:`.Values` construct wouldn't be positionally tracked correctly if
        inside of a :class:`_sql.CTE`, affecting database drivers that support
        VALUES + ctes and use positional parameters such as SQL Server in
        particular as well as asyncpg.   The fix also repairs support for
        compiler flags such as ``literal_binds``.

    .. change::
        :tags: bug, schema
        :tickets: 6287

        Fixed issue where :func:`_functions.next_value` was not deriving its type
        from the corresponding :class:`_schema.Sequence`, instead hardcoded to
        :class:`_types.Integer`. The specific numeric type is now used.

    .. change::
        :tags: bug, mypy
        :tickets: 6255

        Fixed issue where mypy plugin would not correctly interpret an explicit
        :class:`_orm.Mapped` annotation in conjunction with a
        :func:`_orm.relationship` that refers to a class by string name; the
        correct annotation would be downgraded to a less specific one leading to
        typing errors.

    .. change::
        :tags: bug, sql
        :tickets: 6256

        Repaired and solidified issues regarding custom functions and other
        arbitrary expression constructs which within SQLAlchemy's column labeling
        mechanics would seek to use ``str(obj)`` to get a string representation to
        use as an anonymous column name in the ``.c`` collection of a subquery.
        This is a very legacy behavior that performs poorly and leads to lots of
        issues, so has been revised to no longer perform any compilation by
        establishing specific methods on :class:`.FunctionElement` to handle this
        case, as SQL functions are the only use case that it came into play. An
        effect of this behavior is that an unlabeled column expression with no
        derivable name will be given an arbitrary label starting with the prefix
        ``"_no_label"`` in the ``.c`` collection of a subquery; these were
        previously being represented either as the generic stringification of that
        expression, or as an internal symbol.

    .. change::
        :tags: usecase, orm
        :ticketS: 6301

        Altered some of the behavior repaired in :ticket:`6232` where the
        ``immediateload`` loader strategy no longer goes into recursive loops; the
        modification is that an eager load (joinedload, selectinload, or
        subqueryload) from A->bs->B which then states ``immediateload`` for a
        simple manytoone B->a->A that's in the identity map will populate the B->A,
        so that this attribute is back-populated when the collection of A/A.bs are
        loaded. This allows the objects to be functional when detached.


.. changelog::
    :version: 1.4.9
    :released: April 17, 2021

    .. change::
        :tags: bug, sql, regression
        :tickets: 6290

        Fixed regression where an empty in statement on a tuple would result
        in an error when compiled with the option ``literal_binds=True``.

    .. change::
        :tags: bug, regression, orm, performance, sql
        :tickets: 6304

        Fixed a critical performance issue where the traversal of a
        :func:`_sql.select` construct would traverse a repetitive product of the
        represented FROM clauses as they were each referenced by columns in
        the columns clause; for a series of nested subqueries with lots of columns
        this could cause a large delay and significant memory growth. This
        traversal is used by a wide variety of SQL and ORM functions, including by
        the ORM :class:`_orm.Session` when it's configured to have
        "table-per-bind", which while this is not a common use case, it seems to be
        what Flask-SQLAlchemy is hardcoded as using, so the issue impacts
        Flask-SQLAlchemy users. The traversal has been repaired to uniqify on FROM
        clauses which was effectively what would happen implicitly with the pre-1.4
        architecture.

    .. change::
        :tags: bug, postgresql, sql, regression
        :tickets: 6303

        Fixed an argument error in the default and PostgreSQL compilers that
        would interfere with an UPDATE..FROM or DELETE..FROM..USING statement
        that was then SELECTed from as a CTE.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6272

        Fixed regression where an attribute that is mapped to a
        :func:`_orm.synonym` could not be used in column loader options such as
        :func:`_orm.load_only`.

    .. change::
        :tags: usecase, orm
        :tickets: 6267

        Established support for :func:`_orm.synoynm` in conjunction with
        hybrid property, assocaitionproxy is set up completely, including that
        synonyms can be established linking to these constructs which work
        fully.   This is a behavior that was semi-explicitly disallowed previously,
        however since it did not fail in every scenario, explicit support
        for assoc proxy and hybrids has been added.


.. changelog::
    :version: 1.4.8
    :released: April 15, 2021

    .. change::
        :tags: change, mypy

        Updated Mypy plugin to only use the public plugin interface of the
        semantic analyzer.

    .. change::
        :tags: bug, mssql, regression
        :tickets: 6265

        Fixed an additional regression in the same area as that of :ticket:`6173`,
        :ticket:`6184`, where using a value of 0 for OFFSET in conjunction with
        LIMIT with SQL Server would create a statement using "TOP", as was the
        behavior in 1.3, however due to caching would then fail to respond
        accordingly to other values of OFFSET. If the "0" wasn't first, then it
        would be fine. For the fix, the "TOP" syntax is now only emitted if the
        OFFSET value is omitted entirely, that is, :meth:`_sql.Select.offset` is
        not used. Note that this change now requires that if the "with_ties" or
        "percent" modifiers are used, the statement can't specify an OFFSET of
        zero, it now needs to be omitted entirely.

    .. change::
        :tags: bug, engine

        The :meth:`_engine.Dialect.has_table` method now raises an informative
        exception if a non-Connection is passed to it, as this incorrect behavior
        seems to be common.  This method is not intended for external use outside
        of a dialect.  Please use the :meth:`.Inspector.has_table` method
        or for cross-compatibility with older SQLAlchemy versions, the
        :meth:`_engine.Engine.has_table` method.


    .. change::
        :tags: bug, regression, sql
        :tickets: 6249

        Fixed regression where the :class:`_sql.BindParameter` object would not
        properly render for an IN expression (i.e. using the "post compile" feature
        in 1.4) if the object were copied from either an internal cloning
        operation, or from a pickle operation, and the parameter name contained
        spaces or other special characters.

    .. change::
        :tags: bug, mypy
        :tickets: 6205

        Revised the fix for ``OrderingList`` from version 1.4.7 which was testing
        against the incorrect API.

    .. change::
        :tags: bug, asyncio
        :tickets: 6220

        Fix typo that prevented setting the ``bind`` attribute of an
        :class:`_asyncio.AsyncSession` to the correct value.

    .. change::
        :tags: feature, sql
        :tickets: 3314

        The tuple returned by :attr:`.CursorResult.inserted_primary_key` is now a
        :class:`_result.Row` object with a named tuple interface on top of the
        existing tuple interface.




    .. change::
        :tags: bug, regression, sql, sqlite
        :tickets: 6254

        Fixed regression where the introduction of the INSERT syntax "INSERT...
        VALUES (DEFAULT)" was not supported on some backends that do however
        support "INSERT..DEFAULT VALUES", including SQLite. The two syntaxes are
        now each individually supported or non-supported for each dialect, for
        example MySQL supports "VALUES (DEFAULT)" but not "DEFAULT VALUES".
        Support for Oracle has also been enabled.

    .. change::
        :tags: bug, regression, orm
        :tickets: 6259

        Fixed a cache leak involving the :func:`_orm.with_expression` loader
        option, where the given SQL expression would not be correctly considered as
        part of the cache key.

        Additionally, fixed regression involving the corresponding
        :func:`_orm.query_expression` feature. While the bug technically exists in
        1.3 as well, it was not exposed until 1.4. The "default expr" value of
        ``null()`` would be rendered when not needed, and additionally was also not
        adapted correctly when the ORM rewrites statements such as when using
        joined eager loading. The fix ensures "singleton" expressions like ``NULL``
        and ``true`` aren't "adapted" to refer to columns in ORM statements, and
        additionally ensures that a :func:`_orm.query_expression` with no default
        expression doesn't render in the statement if a
        :func:`_orm.with_expression` isn't used.

    .. change::
        :tags: bug, orm
        :tickets: 6252

        Fixed issue in the new feature of :meth:`_orm.Session.refresh` introduced
        by :ticket:`1763` where eagerly loaded relationships are also refreshed,
        where the ``lazy="raise"`` and ``lazy="raise_on_sql"`` loader strategies
        would interfere with the :func:`_orm.immediateload` loader strategy, thus
        breaking the feature for relationships that were loaded with
        :func:`_orm.selectinload`, :func:`_orm.subqueryload` as well.

.. changelog::
    :version: 1.4.7
    :released: April 9, 2021

    .. change::
        :tags: bug, sql, regression
        :tickets: 6222

        Enhanced the "expanding" feature used for :meth:`_sql.ColumnOperators.in_`
        operations to infer the type of expression from the right hand list of
        elements, if the left hand side does not have any explicit type set up.
        This allows the expression to support stringification among other things.
        In 1.3, "expanding" was not automatically used for
        :meth:`_sql.ColumnOperators.in_` expressions, so in that sense this change
        fixes a behavioral regression.


    .. change::
        :tags: bug, mypy

        Fixed issue in Mypy plugin where the plugin wasn’t inferring the correct
        type for columns of subclasses that don’t directly descend from
        ``TypeEngine``, in particular that of  ``TypeDecorator`` and
        ``UserDefinedType``.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6221

        Fixed regression where the :func:`_orm.subqueryload` loader strategy would
        fail to correctly accommodate sub-options, such as a :func:`_orm.defer`
        option on a column, if the "path" of the subqueryload were more than one
        level deep.


    .. change::
        :tags: bug, sql

        Fixed the "stringify" compiler to support a basic stringification
        of a "multirow" INSERT statement, i.e. one with multiple tuples
        following the VALUES keyword.


    .. change::
        :tags: bug, orm, regression
        :tickets: 6211

        Fixed regression where the :func:`_orm.merge_frozen_result` function relied
        upon by the dogpile.caching example was not included in tests and began
        failing due to incorrect internal arguments.

    .. change::
        :tags: bug, engine, regression
        :tickets: 6218

        Fixed up the behavior of the :class:`_result.Row` object when dictionary
        access is used upon it, meaning converting to a dict via ``dict(row)`` or
        accessing members using strings or other objects i.e. ``row["some_key"]``
        works as it would with a dictionary, rather than raising ``TypeError`` as
        would be the case with a tuple, whether or not the C extensions are in
        place. This was originally supposed to emit a 2.0 deprecation warning for
        the "non-future" case using ``LegacyRow``, and was to raise
        ``TypeError`` for the "future" :class:`_result.Row` class. However, the C
        version of :class:`_result.Row` was failing to raise this ``TypeError``,
        and to complicate matters, the :meth:`_orm.Session.execute` method now
        returns :class:`_result.Row` in all cases to maintain consistency with the
        ORM result case, so users who didn't have C extensions installed would
        see different behavior in this one case for existing pre-1.4 style
        code.

        Therefore, in order to soften the overall upgrade scheme as most users have
        not been exposed to the more strict behavior of :class:`_result.Row` up
        through 1.4.6, ``LegacyRow`` and :class:`_result.Row` both
        provide for string-key access as well as support for ``dict(row)``, in all
        cases emitting the 2.0 deprecation warning when ``SQLALCHEMY_WARN_20`` is
        enabled. The :class:`_result.Row` object still uses tuple-like behavior for
        ``__contains__``, which is probably the only noticeable behavioral change
        compared to ``LegacyRow``, other than the removal of
        dictionary-style methods ``values()`` and ``items()``.

    .. change::
        :tags: bug, regression, orm
        :tickets: 6233

        Fixed critical regression where the :class:`_orm.Session` could fail to
        "autobegin" a new transaction when a flush occurred without an existing
        transaction in place, implicitly placing the :class:`_orm.Session` into
        legacy autocommit mode which commit the transaction. The
        :class:`_orm.Session` now has a check that will prevent this condition from
        occurring, in addition to repairing the flush issue.

        Additionally, scaled back part of the change made as part of :ticket:`5226`
        which can run autoflush during an unexpire operation, to not actually
        do this in the case of a :class:`_orm.Session` using legacy
        :paramref:`_orm.Session.autocommit` mode, as this incurs a commit within
        a refresh operation.

    .. change::
        :tags: change, tests

        Added a new flag to :class:`.DefaultDialect` called ``supports_schemas``;
        third party dialects may set this flag to ``False`` to disable SQLAlchemy's
        schema-level tests when running the test suite for a third party dialect.

    .. change::
        :tags: bug, regression, schema
        :tickets: 6216

        Fixed regression where usage of a token in the
        :paramref:`_engine.Connection.execution_options.schema_translate_map`
        dictionary which contained special characters such as braces would fail to
        be substituted properly. Use of square bracket characters ``[]`` is now
        explicitly disallowed as these are used as a delimiter character in the
        current implementation.

    .. change::
        :tags: bug, regression, orm
        :tickets: 6215

        Fixed regression where the ORM compilation scheme would assume the function
        name of a hybrid property would be the same as the attribute name in such a
        way that an ``AttributeError`` would be raised, when it would attempt to
        determine the correct name for each element in a result tuple. A similar
        issue exists in 1.3 but only impacts the names of tuple rows. The fix here
        adds a check that the hybrid's function name is actually present in the
        ``__dict__`` of the class or its superclasses before assigning this name;
        otherwise, the hybrid is considered to be "unnamed" and ORM result tuples
        will use the naming scheme of the underlying expression.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6232

        Fixed critical regression caused by the new feature added as part of
        :ticket:`1763`, eager loaders are invoked on unexpire operations. The new
        feature makes use of the "immediateload" eager loader strategy as a
        substitute for a collection loading strategy, which unlike the other
        "post-load" strategies was not accommodating for recursive invocations
        between mutually-dependent relationships, leading to recursion overflow
        errors.


.. changelog::
    :version: 1.4.6
    :released: April 6, 2021

    .. change::
        :tags: bug, sql, regression, oracle, mssql
        :tickets: 6202

        Fixed further regressions in the same area as that of :ticket:`6173` released in
        1.4.5, where a "postcompile" parameter, again most typically those used for
        LIMIT/OFFSET rendering in Oracle and SQL Server, would fail to be processed
        correctly if the same parameter rendered in multiple places in the
        statement.



    .. change::
        :tags: bug, orm, regression
        :tickets: 6203

        Fixed regression where a deprecated form of :meth:`_orm.Query.join` were
        used, passing a series of entities to join from without any ON clause in a
        single :meth:`_orm.Query.join` call, would fail to function correctly.

    .. change::
        :tags: bug, mypy
        :tickets: 6147

        Applied a series of refactorings and fixes to accommodate for Mypy
        "incremental" mode across multiple files, which previously was not taken
        into account. In this mode the Mypy plugin has to accommodate Python
        datatypes expressed in other files coming in with less information than
        they have on a direct run.

        Additionally, a new decorator :func:`_orm.declarative_mixin` is added,
        which is necessary for the Mypy plugin to be able to definifitely identify
        a Declarative mixin class that is otherwise not used inside a particular
        Python file.

        .. seealso::

            mypy_declarative_mixins -- section was removed


    .. change::
        :tags: bug, mypy
        :tickets: 6205

        Fixed issue where the Mypy plugin would fail to interpret the
        "collection_class" of a relationship if it were a callable and not a class.
        Also improved type matching and error reporting for collection-oriented
        relationships.


    .. change::
        :tags: bug, sql
        :tickets: 6204

        Executing a :class:`_sql.Subquery` using :meth:`_engine.Connection.execute`
        is deprecated and will emit a deprecation warning; this use case was an
        oversight that should have been removed from 1.4. The operation will now
        execute the underlying :class:`_sql.Select` object directly for backwards
        compatibility. Similarly, the :class:`_sql.CTE` class is also not
        appropriate for execution. In 1.3, attempting to execute a CTE would result
        in an invalid "blank" SQL statement being executed; since this use case was
        not working it now raises :class:`_exc.ObjectNotExecutableError`.
        Previously, 1.4 was attempting to execute the CTE as a statement however it
        was working only erratically.

    .. change::
        :tags: bug, regression, orm
        :tickets: 6206

        Fixed critical regression where the :meth:`_orm.Query.yield_per` method in
        the ORM would set up the internal :class:`_engine.Result` to yield chunks
        at a time, however made use of the new :meth:`_engine.Result.unique` method
        which uniques across the entire result. This would lead to lost rows since
        the ORM is using ``id(obj)`` as the uniquing function, which leads to
        repeated identifiers for new objects as already-seen objects are garbage
        collected. 1.3's behavior here was to "unique" across each chunk, which
        does not actually produce "uniqued" results when results are yielded in
        chunks. As the :meth:`_orm.Query.yield_per` method is already explicitly
        disallowed when joined eager loading is in place, which is the primary
        rationale for the "uniquing" feature, the "uniquing" feature is now turned
        off entirely when :meth:`_orm.Query.yield_per` is used.

        This regression only applies to the legacy :class:`_orm.Query` object; when
        using :term:`2.0 style` execution, "uniquing" is not automatically applied.
        To prevent the issue from arising from explicit use of
        :meth:`_engine.Result.unique`, an error is now raised if rows are fetched
        from a "uniqued" ORM-level :class:`_engine.Result` if any
        :ref:`yield per <orm_queryguide_yield_per>` API is also in use, as the
        purpose of ``yield_per`` is to allow for arbitrarily large numbers of rows,
        which cannot be uniqued in memory without growing the number of entries to
        fit the complete result size.


    .. change::
        :tags: usecase, asyncio, postgresql
        :tickets: 6199

        Added accessors ``.sqlstate`` and synonym ``.pgcode`` to the ``.orig``
        attribute of the SQLAlchemy exception class raised by the asyncpg DBAPI
        adapter, that is, the intermediary exception object that wraps on top of
        that raised by the asyncpg library itself, but below the level of the
        SQLAlchemy dialect.

.. changelog::
    :version: 1.4.5
    :released: April 2, 2021

    .. change::
        :tags: bug, sql, postgresql
        :tickets: 6183

        Fixed bug in new :meth:`_functions.FunctionElement.render_derived` feature
        where column names rendered out explicitly in the alias SQL would not have
        proper quoting applied for case sensitive names and other non-alphanumeric
        names.

    .. change::
        :tags: bug, regression, orm
        :tickets: 6172

        Fixed regression where the :func:`_orm.joinedload` loader strategy would
        not successfully joinedload to a mapper that is mapper against a
        :class:`.CTE` construct.

    .. change::
        :tags: bug, regression, sql
        :tickets: 6181

        Fixed regression where use of the :meth:`.Operators.in_` method with a
        :class:`_sql.Select` object against a non-table-bound column would produce
        an ``AttributeError``, or more generally using a :class:`_sql.ScalarSelect`
        that has no datatype in a binary expression would produce invalid state.


    .. change::
        :tags: bug, mypy
        :tickets: sqlalchemy/sqlalchemy2-stubs/#14

        Fixed issue in mypy plugin where newly added support for
        :func:`_orm.as_declarative` needed to more fully add the
        ``DeclarativeMeta`` class to the mypy interpreter's state so that it does
        not result in a name not found error; additionally improves how global
        names are setup for the plugin including the ``Mapped`` name.


    .. change::
        :tags: bug, mysql, regression
        :tickets: 6163

        Fixed regression in the MySQL dialect where the reflection query used to
        detect if a table exists would fail on very old MySQL 5.0 and 5.1 versions.

    .. change::
        :tags: bug, sql
        :tickets: 6184

        Added a new flag to the :class:`_engine.Dialect` class called
        :attr:`_engine.Dialect.supports_statement_cache`. This flag now needs to be present
        directly on a dialect class in order for SQLAlchemy's
        :ref:`query cache <sql_caching>` to take effect for that dialect. The
        rationale is based on discovered issues such as :ticket:`6173` revealing
        that dialects which hardcode literal values from the compiled statement,
        often the numerical parameters used for LIMIT / OFFSET, will not be
        compatible with caching until these dialects are revised to use the
        parameters present in the statement only. For third party dialects where
        this flag is not applied, the SQL logging will show the message "dialect
        does not support caching", indicating the dialect should seek to apply this
        flag once they have verified that no per-statement literal values are being
        rendered within the compilation phase.

        .. seealso::

          :ref:`engine_thirdparty_caching`

    .. change::
        :tags: bug, postgresql
        :tickets: 6099

        Fixed typo in the fix for :ticket:`6099` released in 1.4.4 that completely
        prevented this change from working correctly, i.e. the error message did not match
        what was actually emitted by pg8000.

    .. change::
        :tags: bug, orm, regression
        :tickets: 6171

        Scaled back the warning message added in :ticket:`5171` to not warn for
        overlapping columns in an inheritance scenario where a particular
        relationship is local to a subclass and therefore does not represent an
        overlap.

    .. change::
        :tags: bug, regression, oracle
        :tickets: 6173

        Fixed critical regression where the Oracle compiler would not maintain the
        correct parameter values in the LIMIT/OFFSET for a select due to a caching
        issue.


    .. change::
        :tags: bug, postgresql
        :tickets: 6170

        Fixed issue where the PostgreSQL :class:`.PGInspector`, when generated
        against an :class:`_engine.Engine`, would fail for ``.get_enums()``,
        ``.get_view_names()``, ``.get_foreign_table_names()`` and
        ``.get_table_oid()`` when used against a "future" style engine and not the
        connection directly.

    .. change::
        :tags: bug, schema
        :tickets: 6146

        Introduce a new parameter :paramref:`_types.Enum.omit_aliases` in
        :class:`_types.Enum` type allow filtering aliases when using a pep435 Enum.
        Previous versions of SQLAlchemy kept aliases in all cases, creating
        database enum type with additional states, meaning that they were treated
        as different values in the db. For backward compatibility this flag
        defaults to ``False`` in the 1.4 series, but will be switched to ``True``
        in a future version. A deprecation warning is raise if this flag is not
        specified and the passed enum contains aliases.

    .. change::
        :tags: bug, mssql
        :tickets: 6163

        Fixed a regression in MSSQL 2012+ that prevented the order by clause
        to be rendered when ``offset=0`` is used in a subquery.

    .. change::
        :tags: bug, asyncio
        :tickets: 6166


        Fixed issue where the asyncio extension could not be loaded
        if running Python 3.6 with the backport library of
        ``contextvars`` installed.

.. changelog::
    :version: 1.4.4
    :released: March 30, 2021

    .. change::
        :tags: bug, misc

        Adjusted the usage of the ``importlib_metadata`` library for loading
        setuptools entrypoints in order to accommodate for some deprecation
        changes.


    .. change::
        :tags: bug, postgresql
        :tickets: 6099

        Modified the ``is_disconnect()`` handler for the pg8000 dialect, which now
        accommodates for a new ``InterfaceError`` emitted by pg8000 1.19.0. Pull
        request courtesy Hamdi Burak Usul.


    .. change::
        :tags: bug, orm
        :tickets: 6139

        Fixed critical issue in the new :meth:`_orm.PropComparator.and_` feature
        where loader strategies that emit secondary SELECT statements such as
        :func:`_orm.selectinload` and :func:`_orm.lazyload` would fail to
        accommodate for bound parameters in the user-defined criteria in terms of
        the current statement being executed, as opposed to the cached statement,
        causing stale bound values to be used.

        This also adds a warning for the case where an object that uses
        :func:`_orm.lazyload` in conjunction with :meth:`_orm.PropComparator.and_`
        is attempted to be serialized; the loader criteria cannot reliably
        be serialized and deserialized and eager loading should be used for this
        case.


    .. change::
        :tags: bug, engine
        :tickets: 6138

        Repair wrong arguments to exception handling method
        in CursorResult.

    .. change::
        :tags: bug, regression, orm
        :tickets: 6144

        Fixed missing method :meth:`_orm.Session.get` from the
        :class:`_orm.ScopedSession` interface.


    .. change::
        :tags: usecase, engine
        :tickets: 6155

        Modified the context manager used by :class:`_engine.Transaction` so that
        an "already detached" warning is not emitted by the ending of the context
        manager itself, if the transaction were already manually rolled back inside
        the block. This applies to regular transactions, savepoint transactions,
        and legacy "marker" transactions. A warning is still emitted if the
        ``.rollback()`` method is called explicitly more than once.

.. changelog::
    :version: 1.4.3
    :released: March 25, 2021

    .. change::
        :tags: bug, orm
        :tickets: 6069

        Fixed a bug where python 2.7.5 (default on CentOS 7) wasn't able to import
        sqlalchemy, because on this version of Python ``exec "statement"`` and
        ``exec("statement")`` do not behave the same way.  The compatibility
        ``exec_()`` function was used instead.

    .. change::
        :tags: sqlite, feature, asyncio
        :tickets: 5920

        Added support for the aiosqlite database driver for use with the
        SQLAlchemy asyncio extension.

        .. seealso::

          :ref:`aiosqlite`

    .. change::
        :tags: bug, regression, orm, declarative
        :tickets: 6128

        Fixed regression where the ``.metadata`` attribute on a per class level
        would not be honored, breaking the use case of per-class-hierarchy
        :class:`.schema.MetaData` for abstract declarative classes and mixins.


        .. seealso::

          :ref:`declarative_metadata`

    .. change::
        :tags: bug, mypy

        Added support for the Mypy extension to correctly interpret a declarative
        base class that's generated using the :func:`_orm.as_declarative` function
        as well as the :meth:`_orm.registry.as_declarative_base` method.

    .. change::
        :tags: bug, mypy
        :tickets: 6109

        Fixed bug in Mypy plugin where the Python type detection
        for the :class:`_types.Boolean` column type would produce
        an exception; additionally implemented support for :class:`_types.Enum`,
        including detection of a string-based enum vs. use of Python ``enum.Enum``.

    .. change::
        :tags: bug, reflection, postgresql
        :tickets: 6129

        Fixed reflection of identity columns in tables with mixed case names
        in PostgreSQL.

    .. change::
        :tags: bug, sqlite, regression
        :tickets: 5848

        Repaired the ``pysqlcipher`` dialect to connect correctly which had
        regressed in 1.4, and added test + CI support to maintain the driver
        in working condition.  The dialect now imports the ``sqlcipher3`` module
        for Python 3 by default before falling back to ``pysqlcipher3`` which
        is documented as now being unmaintained.

        .. seealso::

          :ref:`pysqlcipher`


    .. change::
        :tags: bug, orm
        :tickets: 6060

        Fixed bug where ORM queries using a correlated subquery in conjunction with
        :func:`_orm.column_property` would fail to correlate correctly to an
        enclosing subquery or to a CTE when :meth:`_sql.Select.correlate_except`
        were used in the property to control correlation, in cases where the
        subquery contained the same selectables as ones within the correlated
        subquery that were intended to not be correlated.

    .. change::
        :tags: bug, orm
        :tickets: 6131

        Fixed bug where combinations of the new "relationship with criteria"
        feature could fail in conjunction with features that make use of the new
        "lambda SQL" feature, including loader strategies such as selectinload and
        lazyload, for more complicated scenarios such as polymorphic loading.

    .. change::
        :tags: bug, orm
        :tickets: 6124

        Repaired support so that the :meth:`_sql.ClauseElement.params` method can
        work correctly with a :class:`_sql.Select` object that includes joins
        across ORM relationship structures, which is a new feature in 1.4.


    .. change::
        :tags: bug, engine, regression
        :tickets: 6119

        Restored the :class:`_engine.ResultProxy` name back to the
        ``sqlalchemy.engine`` namespace. This name refers to the
        ``LegacyCursorResult`` object.

    .. change::
        :tags: bug, orm
        :tickets: 6115

        Fixed issue where a "removed in 2.0" warning were generated internally by
        the relationship loader mechanics.


.. changelog::
    :version: 1.4.2
    :released: March 19, 2021

    .. change::
        :tags: bug, orm, dataclasses
        :tickets: 6093

        Fixed issue in new ORM dataclasses functionality where dataclass fields on
        an abstract base or mixin that contained column or other mapping constructs
        would not be mapped if they also included a "default" key within the
        dataclasses.field() object.


    .. change::
        :tags: bug, regression, orm
        :tickets: 6088

        Fixed regression where the :attr:`_orm.Query.selectable` accessor, which is
        a synonym for :meth:`_orm.Query.__clause_element__`, got removed, it's now
        restored.

    .. change::
        :tags: bug, engine, regression

        Restored top level import for ``sqlalchemy.engine.reflection``. This
        ensures that the base :class:`_reflection.Inspector` class is properly
        registered so that :func:`_sa.inspect` works for third party dialects that
        don't otherwise import this package.


    .. change::
        :tags: bug, regression, orm
        :tickets: 6086

        Fixed regression where use of an unnamed SQL expression such as a SQL
        function would raise a column targeting error if the query itself were
        using joinedload for an entity and was also being wrapped in a subquery by
        the joinedload eager loading process.


    .. change::
        :tags: bug, orm, regression
        :tickets: 6092

        Fixed regression where the :meth:`_orm.Query.filter_by` method would fail
        to locate the correct source entity if the :meth:`_orm.Query.join` method
        had been used targeting an entity without any kind of ON clause.


    .. change::
        :tags: postgresql, usecase
        :tickets: 6982

        Rename the column name used by a reflection query that used
        a reserved word in some postgresql compatible databases.

    .. change::
        :tags: usecase, orm, dataclasses
        :tickets: 6100

        Added support for the :class:`_orm.declared_attr` object to work in the
        context of dataclass fields.

        .. seealso::

            :ref:`orm_declarative_dataclasses_mixin`

    .. change::
        :tags: bug, sql, regression
        :tickets: 6101

        Fixed issue where using a ``func`` that includes dotted packagenames would
        fail to be cacheable by the SQL caching system due to a Python list of
        names that needed to be a tuple.


    .. change::
        :tags: bug, regression, orm
        :tickets: 6095

        Fixed regression where the SQL compilation of a :class:`.Function` would
        not work correctly if the object had been "annotated", which is an internal
        memoization process used mostly by the ORM. In particular it could affect
        ORM lazy loads which make greater use of this feature in 1.4.

    .. change::
        :tags: bug, sql, regression
        :tickets: 6097

        Fixed regression in the :func:`_sql.case` construct, where the "dictionary"
        form of argument specification failed to work correctly if it were passed
        positionally, rather than as a "whens" keyword argument.

    .. change::
        :tags: bug, orm
        :tickets: 6090

        Fixed regression where the :class:`.ConcreteBase` would fail to map at all
        when a mapped column name overlapped with the discriminator column name,
        producing an assertion error. The use case here did not function correctly
        in 1.3 as the polymorphic union would produce a query that ignored the
        discriminator column entirely, while emitting duplicate column warnings. As
        1.4's architecture cannot easily reproduce this essentially broken behavior
        of 1.3 at the ``select()`` level right now, the use case now raises an
        informative error message instructing the user to use the
        ``.ConcreteBase._concrete_discriminator_name`` attribute to resolve the
        conflict. To assist with this configuration,
        ``.ConcreteBase._concrete_discriminator_name`` may be placed on the base
        class only where it will be automatically used by subclasses; previously
        this was not the case.


    .. change::
        :tags: bug, mypy
        :tickets: sqlalchemy/sqlalchemy2-stubs/2

        Fixed issue in MyPy extension which crashed on detecting the type of a
        :class:`.Column` if the type were given with a module prefix like
        ``sa.Integer()``.


.. changelog::
    :version: 1.4.1
    :released: March 17, 2021

    .. change::
        :tags: bug, orm, regression
        :tickets: 6066

        Fixed regression where producing a Core expression construct such as
        :func:`_sql.select` using ORM entities would eagerly configure the mappers,
        in an effort to maintain compatibility with the :class:`_orm.Query` object
        which necessarily does this to support many backref-related legacy cases.
        However, core :func:`_sql.select` constructs are also used in mapper
        configurations and such, and to that degree this eager configuration is
        more of an inconvenience, so eager configure has been disabled for the
        :func:`_sql.select` and other Core constructs in the absence of ORM loading
        types of functions such as :class:`_orm.Load`.

        The change maintains the behavior of :class:`_orm.Query` so that backwards
        compatibility is maintained. However, when using a :func:`_sql.select` in
        conjunction with ORM entities, a "backref" that isn't explicitly placed on
        one of the classes until mapper configure time won't be available unless
        :func:`_orm.configure_mappers` or the newer :func:`_orm.registry.configure`
        has been called elsewhere. Prefer using
        :paramref:`_orm.relationship.back_populates` for more explicit relationship
        configuration which does not have the eager configure requirement.


    .. change::
        :tags: bug, mssql, regression
        :tickets: 6058

        Fixed regression where a new setinputsizes() API that's available for
        pyodbc was enabled, which is apparently incompatible with pyodbc's
        fast_executemany() mode in the absence of more accurate typing information,
        which as of yet is not fully implemented or tested. The pyodbc dialect and
        connector has been modified so that setinputsizes() is not used at all
        unless the parameter ``use_setinputsizes`` is passed to the dialect, e.g.
        via :func:`_sa.create_engine`, at which point its behavior can be
        customized using the :meth:`.DialectEvents.do_setinputsizes` hook.

        .. seealso::

          :ref:`mssql_pyodbc_setinputsizes`

    .. change::
        :tags: bug, orm, regression
        :tickets: 6055

        Fixed a critical regression in the relationship lazy loader where the SQL
        criteria used to fetch a related many-to-one object could go stale in
        relation to other memoized structures within the loader if the mapper had
        configuration changes, such as can occur when mappers are late configured
        or configured on demand, producing a comparison to None and returning no
        object. Huge thanks to Alan Hamlett for their help tracking this down late
        into the night.



    .. change::
        :tags: bug, regression
        :tickets: 6068

        Added back ``items`` and ``values`` to ``ColumnCollection`` class.
        The regression was introduced while adding support for duplicate
        columns in from clauses and selectable in ticket #4753.


    .. change::
        :tags: bug, engine, regression
        :tickets: 6074

        The Python ``namedtuple()`` has the behavior such that the names ``count``
        and ``index`` will be served as tuple values if the named tuple includes
        those names; if they are absent, then their behavior as methods of
        ``collections.abc.Sequence`` is maintained. Therefore the
        :class:`_result.Row` and ``LegacyRow`` classes have been fixed
        so that they work in this same way, maintaining the expected behavior for
        database rows that have columns named "index" or "count".

    .. change::
        :tags: bug, orm, regression
        :tickets: 6076

        Fixed regression where the :meth:`_orm.Query.exists` method would fail to
        create an expression if the entity list of the :class:`_orm.Query` were
        an arbitrary SQL column expression.


    .. change::
        :tags: bug, orm, regression
        :tickets: 6052

        Fixed regression where calling upon :meth:`_orm.Query.count` in conjunction
        with a loader option such as :func:`_orm.joinedload` would fail to ignore
        the loader option. This is a behavior that has always been very specific to
        the :meth:`_orm.Query.count` method; an error is normally raised if a given
        :class:`_orm.Query` has options that don't apply to what it is returning.

    .. change::
        :tags: bug, orm, declarative, regression
        :tickets: 6054

        Fixed bug where user-mapped classes that contained an attribute named
        "registry" would cause conflicts with the new registry-based mapping system
        when using :class:`.DeclarativeMeta`. While the attribute remains
        something that can be set explicitly on a declarative base to be
        consumed by the metaclass, once located it is placed under a private
        class variable so it does not conflict with future subclasses that use
        the same name for other purposes.



    .. change::
        :tags: bug, orm, regression
        :tickets: 6067

        Fixed regression in :meth:`_orm.Session.identity_key`, including that the
        method and related methods were not covered by any unit test as well as
        that the method contained a typo preventing it from functioning correctly.


.. changelog::
    :version: 1.4.0
    :released: March 15, 2021

    .. change::
        :tags: bug, mssql
        :tickets: 5919

        Fix a reflection error for MSSQL 2005 introduced by the reflection of
        filtered indexes.

    .. change::
        :tags: feature, mypy
        :tickets: 4609

        Rudimentary and experimental support for Mypy has been added in the form of
        a new plugin, which itself depends on new typing stubs for SQLAlchemy. The
        plugin allows declarative mappings in their standard form to both be
        compatible with Mypy as well as to provide typing support for mapped
        classes and instances.

        .. seealso::

            mypy_toplevel -- section was removed

    .. change::
        :tags: bug, sql
        :tickets: 6016

        Fixed bug where the "percent escaping" feature that occurs with dialects
        that use the "format" or "pyformat" bound parameter styles was not enabled
        for the :meth:`_sql.Operators.op` and :class:`_sql.custom_op` constructs,
        for custom operators that use percent signs. The percent sign will now be
        automatically doubled based on the paramstyle as necessary.



    .. change::
        :tags: bug, regression, sql
        :tickets: 5979

        Fixed regression where the "unsupported compilation error" for unknown
        datatypes would fail to raise correctly.

    .. change::
        :tags: ext, usecase
        :tickets: 5942

        Add new parameter
        :paramref:`_automap.AutomapBase.prepare.reflection_options`
        to allow passing of :meth:`_schema.MetaData.reflect` options like ``only``
        or dialect-specific reflection options like ``oracle_resolve_synonyms``.

    .. change::
        :tags: change, sql

        Altered the compilation for the :class:`.CTE` construct so that a string is
        returned representing the inner SELECT statement if the :class:`.CTE` is
        stringified directly, outside of the context of an enclosing SELECT; This
        is the same behavior of :meth:`_sql.FromClause.alias` and
        :meth:`_sql.Select.subquery`. Previously, a blank string would be
        returned as the CTE is normally placed above a SELECT after that SELECT has
        been generated, which is generally misleading when debugging.


    .. change::
        :tags: bug, orm
        :tickets: 5981

        Fixed regression where the :paramref:`_orm.relationship.query_class`
        parameter stopped being functional for "dynamic" relationships.  The
        ``AppenderQuery`` remains dependent on the legacy :class:`_orm.Query`
        class; users are encouraged to migrate from the use of "dynamic"
        relationships to using :func:`_orm.with_parent` instead.


    .. change::
        :tags: bug, orm, regression
        :tickets: 6003

        Fixed regression where :meth:`_orm.Query.join` would produce no effect if
        the query itself as well as the join target were against a
        :class:`_schema.Table` object, rather than a mapped class. This was part of
        a more systemic issue where the legacy ORM query compiler would not be
        correctly used from a :class:`_orm.Query` if the statement produced had not
        ORM entities present within it.


    .. change::
        :tags: bug, regression, sql
        :tickets: 6008

        Fixed regression where usage of the standalone :func:`_sql.distinct()` used
        in the form of being directly SELECTed would fail to be locatable in the
        result set by column identity, which is how the ORM locates columns. While
        standalone :func:`_sql.distinct()` is not oriented towards being directly
        SELECTed (use :meth:`_sql.select.distinct` for a regular
        ``SELECT DISTINCT..``) , it was usable to a limited extent in this way
        previously (but wouldn't work in subqueries, for example). The column
        targeting for unary expressions such as "DISTINCT <col>" has been improved
        so that this case works again, and an additional improvement has been made
        so that usage of this form in a subquery at least generates valid SQL which
        was not the case previously.

        The change additionally enhances the ability to target elements in
        ``row._mapping`` based on SQL expression objects in ORM-enabled
        SELECT statements, including whether the statement was invoked by
        ``connection.execute()`` or ``session.execute()``.

    .. change::
        :tags: bug, orm, asyncio
        :tickets: 5998

        The API for :meth:`_asyncio.AsyncSession.delete` is now an awaitable;
        this method cascades along relationships which must be loaded in a
        similar manner as the :meth:`_asyncio.AsyncSession.merge` method.


    .. change::
        :tags: usecase, postgresql, mysql, asyncio
        :tickets: 5967

        Added an ``asyncio.Lock()`` within SQLAlchemy's emulated DBAPI cursor,
        local to the connection, for the asyncpg and aiomysql dialects for the
        scope of the ``cursor.execute()`` and ``cursor.executemany()`` methods. The
        rationale is to prevent failures and corruption for the case where the
        connection is used in multiple awaitables at once.

        While this use case can also occur with threaded code and non-asyncio
        dialects, we anticipate this kind of use will be more common under asyncio,
        as the asyncio API is encouraging of such use. It's definitely better to
        use a distinct connection per concurrent awaitable however as concurrency
        will not be achieved otherwise.

        For the asyncpg dialect, this is so that the space between
        the call to ``prepare()`` and ``fetch()`` is prevented from allowing
        concurrent executions on the connection from causing interface error
        exceptions, as well as preventing race conditions when starting a new
        transaction. Other PostgreSQL DBAPIs are threadsafe at the connection level
        so this intends to provide a similar behavior, outside the realm of server
        side cursors.

        For the aiomysql dialect, the mutex will provide safety such that
        the statement execution and the result set fetch, which are two distinct
        steps at the connection level, won't get corrupted by concurrent
        executions on the same connection.


    .. change::
        :tags: bug, engine
        :tickets: 6002

        Improved engine logging to note ROLLBACK and COMMIT which is logged while
        the DBAPI driver is in AUTOCOMMIT mode. These ROLLBACK/COMMIT are library
        level and do not have any effect when AUTOCOMMIT is in effect, however it's
        still worthwhile to log as these indicate where SQLAlchemy sees the
        "transaction" demarcation.

    .. change::
        :tags: bug, regression, engine
        :tickets: 6004

        Fixed a regression where the "reset agent" of the connection pool wasn't
        really being utilized by the :class:`_engine.Connection` when it were
        closed, and also leading to a double-rollback scenario that was somewhat
        wasteful.   The newer architecture of the engine has been updated so that
        the connection pool "reset-on-return" logic will be skipped when the
        :class:`_engine.Connection` explicitly closes out the transaction before
        returning the pool to the connection.

    .. change::
        :tags: bug, schema
        :tickets: 5953

        Deprecated all schema-level ``.copy()`` methods and renamed to
        ``_copy()``.  These are not standard Python "copy()" methods as they
        typically rely upon being instantiated within particular contexts
        which are passed to the method as optional keyword arguments.   The
        :meth:`_schema.Table.tometadata` method is the public API that provides
        copying for :class:`_schema.Table` objects.

    .. change::
        :tags: bug, ext
        :tickets: 6020

        The ``sqlalchemy.ext.mutable`` extension now tracks the "parents"
        collection using the :class:`.InstanceState` associated with objects,
        rather than the object itself. The latter approach required that the object
        be hashable so that it can be inside of a ``WeakKeyDictionary``, which goes
        against the behavioral contract of the ORM overall which is that ORM mapped
        objects do not need to provide any particular kind of ``__hash__()`` method
        and that unhashable objects are supported.

    .. change::
        :tags: bug, orm
        :tickets: 5984

        The unit of work process now turns off all "lazy='raise'" behavior
        altogether when a flush is proceeding.  While there are areas where the UOW
        is sometimes loading things that aren't ultimately needed, the lazy="raise"
        strategy is not helpful here as the user often does not have much control
        or visibility into the flush process.


.. changelog::
    :version: 1.4.0b3
    :released: March 15, 2021
    :released: February 15, 2021

    .. change::
        :tags: bug, orm
        :tickets: 5933

        Fixed issue in new 1.4/2.0 style ORM queries where a statement-level label
        style would not be preserved in the keys used by result rows; this has been
        applied to all combinations of Core/ORM columns / session vs. connection
        etc. so that the linkage from statement to result row is the same in all
        cases.   As part of this change, the labeling of column expressions
        in rows has been improved to retain the original name of the ORM
        attribute even if used in a subquery.




    .. change::
        :tags: bug, sql
        :tickets: 5924

        Fixed bug where the "cartesian product" assertion was not correctly
        accommodating for joins between tables that relied upon the use of LATERAL
        to connect from a subquery to another subquery in the enclosing context.

    .. change::
        :tags: bug, sql
        :tickets: 5934

        Fixed 1.4 regression where the :meth:`_functions.Function.in_` method was
        not covered by tests and failed to function properly in all cases.

    .. change::
        :tags: bug, engine, postgresql
        :tickets: 5941

        Continued with the improvement made as part of :ticket:`5653` to further
        support bound parameter names, including those generated against column
        names, for names that include colons, parenthesis, and question marks, as
        well as improved test support, so that bound parameter names even if they
        are auto-derived from column names should have no problem including for
        parenthesis in psycopg2's "pyformat" style.

        As part of this change, the format used by the asyncpg DBAPI adapter (which
        is local to SQLAlchemy's asyncpg dialect) has been changed from using
        "qmark" paramstyle to "format", as there is a standard and internally
        supported SQL string escaping style for names that use percent signs with
        "format" style (i.e. to double percent signs), as opposed to names that use
        question marks with "qmark" style (where an escaping system is not defined
        by pep-249 or Python).

        .. seealso::

          :ref:`change_5941`

    .. change::
        :tags: sql, usecase, postgresql, sqlite
        :tickets: 5939

        Enhance ``set_`` keyword of :class:`.OnConflictDoUpdate` to accept a
        :class:`.ColumnCollection`, such as the ``.c.`` collection from a
        :class:`Selectable`, or the ``.excluded`` contextual object.

    .. change::
        :tags: feature, orm

        The ORM used in :term:`2.0 style` can now return ORM objects from the rows
        returned by an UPDATE..RETURNING or INSERT..RETURNING statement, by
        supplying the construct to :meth:`_sql.Select.from_statement` in an ORM
        context.

        .. seealso::

          :ref:`orm_dml_returning_objects`



    .. change::
        :tags: bug, sql
        :tickets: 5935

        Fixed regression where use of an arbitrary iterable with the
        :func:`_sql.select` function was not working, outside of plain lists. The
        forwards/backwards compatibility logic here now checks for a wider range of
        incoming "iterable" types including that a ``.c`` collection from a
        selectable can be passed directly. Pull request compliments of Oliver Rice.

.. changelog::
    :version: 1.4.0b2
    :released: March 15, 2021
    :released: February 3, 2021

    .. change::
        :tags: usecase, sql
        :tickets: 5695

        Multiple calls to "returning", e.g. :meth:`_sql.Insert.returning`,
        may now be chained to add new columns to the RETURNING clause.


    .. change::
      :tags: bug, asyncio
      :tickets: 5615

      Adjusted the greenlet integration, which provides support for Python asyncio
      in SQLAlchemy, to accommodate for the handling of Python ``contextvars``
      (introduced in Python 3.7) for ``greenlet`` versions greater than 0.4.17.
      Greenlet version 0.4.17 added automatic handling of contextvars in a
      backwards-incompatible way; we've coordinated with the greenlet authors to
      add a preferred API for this in versions subsequent to 0.4.17 which is now
      supported by SQLAlchemy's greenlet integration.  For greenlet versions prior
      to 0.4.17 no behavioral change is needed, version 0.4.17 itself is blocked
      from the dependencies.

    .. change::
        :tags: bug, engine, sqlite
        :tickets: 5845

        Fixed bug in the 2.0 "future" version of :class:`_engine.Engine` where emitting
        SQL during the :meth:`.EngineEvents.begin` event hook would cause a
        re-entrant (recursive) condition due to autobegin, affecting among other
        things the recipe documented for SQLite to allow for savepoints and
        serializable isolation support.


    .. change::
        :tags: bug, orm, regression
        :tickets: 5845

        Fixed issue in new :class:`_orm.Session` similar to that of the
        :class:`_engine.Connection` where the new "autobegin" logic could be
        tripped into a re-entrant (recursive) state if SQL were executed within the
        :meth:`.SessionEvents.after_transaction_create` event hook.

    .. change::
        :tags: sql
        :tickets: 4757

        Replace :meth:`_orm.Query.with_labels` and
        :meth:`_sql.GenerativeSelect.apply_labels` with explicit getters and
        setters :meth:`_sql.GenerativeSelect.get_label_style` and
        :meth:`_sql.GenerativeSelect.set_label_style` to accommodate the three
        supported label styles:
        :attr:`_sql.SelectLabelStyle.LABEL_STYLE_DISAMBIGUATE_ONLY`,
        :attr:`_sql.SelectLabelStyle.LABEL_STYLE_TABLENAME_PLUS_COL`, and
        :attr:`_sql.SelectLabelStyle.LABEL_STYLE_NONE`.

        In addition, for Core and "future style" ORM queries,
        ``LABEL_STYLE_DISAMBIGUATE_ONLY`` is now the default label style. This
        style differs from the existing "no labels" style in that labeling is
        applied in the case of column name conflicts; with ``LABEL_STYLE_NONE``, a
        duplicate column name is not accessible via name in any case.

        For cases where labeling is significant, namely that the ``.c`` collection
        of a subquery is able to refer to all columns unambiguously, the behavior
        of ``LABEL_STYLE_DISAMBIGUATE_ONLY`` is now sufficient for all
        SQLAlchemy features across Core and ORM which involve this behavior.
        Result set rows since SQLAlchemy 1.0 are usually aligned with column
        constructs positionally.

        For legacy ORM queries using :class:`_query.Query`, the table-plus-column
        names labeling style applied by ``LABEL_STYLE_TABLENAME_PLUS_COL``
        continues to be used so that existing test suites and logging facilities
        see no change in behavior by default.

    .. change::
        :tags: bug, orm, unitofwork
        :tickets: 5735

        Improved the unit of work topological sorting system such that the
        toplogical sort is now deterministic based on the sorting of the input set,
        which itself is now sorted at the level of mappers, so that the same inputs
        of affected mappers should produce the same output every time, among
        mappers / tables that don't have any dependency on each other. This further
        reduces the chance of deadlocks as can be observed in a flush that UPDATEs
        among multiple, unrelated tables such that row locks are generated.


    .. change::
        :tags: changed, orm
        :tickets: 5897

        Mapper "configuration", which occurs within the
        :func:`_orm.configure_mappers` function, is now organized to be on a
        per-registry basis. This allows for example the mappers within a certain
        declarative base to be configured, but not those of another base that is
        also present in memory. The goal is to provide a means of reducing
        application startup time by only running the "configure" process for sets
        of mappers that are needed. This also adds the
        :meth:`_orm.registry.configure` method that will run configure for the
        mappers local in a particular registry only.

    .. change::
        :tags: bug, orm
        :tickets: 5702

        Fixed regression where the :paramref:`.Bundle.single_entity` flag would
        take effect for a :class:`.Bundle` even though it were not set.
        Additionally, this flag is legacy as it only makes sense for the
        :class:`_orm.Query` object and not 2.0 style execution.  a deprecation
        warning is emitted when used with new-style execution.

    .. change::
        :tags: bug, sql
        :tickets: 5858

        Fixed issue in new :meth:`_sql.Select.join` method where chaining from the
        current JOIN wasn't looking at the right state, causing an expression like
        "FROM a JOIN b <onclause>, b JOIN c <onclause>" rather than
        "FROM a JOIN b <onclause> JOIN c <onclause>".

    .. change::
        :tags: usecase, sql

        Added :meth:`_sql.Select.outerjoin_from` method to complement
        :meth:`_sql.Select.join_from`.

    .. change::
        :tags: usecase, sql
        :tickets: 5888

        Adjusted the "literal_binds" feature of :class:`_sql.Compiler` to render
        NULL for a bound parameter that has ``None`` as the value, either
        explicitly passed or omitted. The previous error message "bind parameter
        without a renderable value" is removed, and a missing or ``None`` value
        will now render NULL in all cases. Previously, rendering of NULL was
        starting to happen for DML statements due to internal refactorings, but was
        not explicitly part of test coverage, which it now is.

        While no error is raised, when the context is within that of a column
        comparison, and the operator is not "IS"/"IS NOT", a warning is emitted
        that this is not generally useful from a SQL perspective.


    .. change::
        :tags: bug, orm
        :tickets: 5750

        Fixed regression where creating an :class:`_orm.aliased` construct against
        a plain selectable and including a name would raise an assertionerror.


    .. change::
        :tags: bug, mssql, mysql, datatypes
        :tickets: 5788
        :versions: 1.4.0b2

        Decimal accuracy and behavior has been improved when extracting floating
        point and/or decimal values from JSON strings using the
        :meth:`_sql.sqltypes.JSON.Comparator.as_float` method, when the numeric
        value inside of the JSON string has many significant digits; previously,
        MySQL backends would truncate values with many significant digits and SQL
        Server backends would raise an exception due to a DECIMAL cast with
        insufficient significant digits.   Both backends now use a FLOAT-compatible
        approach that does not hardcode significant digits for floating point
        values. For precision numerics, a new method
        :meth:`_sql.sqltypes.JSON.Comparator.as_numeric` has been added which
        accepts arguments for precision and scale, and will return values as Python
        ``Decimal`` objects with no floating point conversion assuming the DBAPI
        supports it (all but pysqlite).

    .. change::
        :tags: feature, orm, declarative
        :tickets: 5745

        Added an alternate resolution scheme to Declarative that will extract the
        SQLAlchemy column or mapped property from the "metadata" dictionary of a
        dataclasses.Field object.  This allows full declarative mappings to be
        combined with dataclass fields.

        .. seealso::

            :ref:`orm_declarative_dataclasses_declarative_table`

    .. change::
        :tags: bug, sql
        :tickets: 5754

        Deprecation warnings are emitted under "SQLALCHEMY_WARN_20" mode when
        passing a plain string to :meth:`_orm.Session.execute`.


    .. change::
        :tags: bug, sql, orm
        :tickets: 5760, 5763, 5765, 5768, 5770

        A wide variety of fixes to the "lambda SQL" feature introduced at
        :ref:`engine_lambda_caching` have been implemented based on user feedback,
        with an emphasis on its use within the :func:`_orm.with_loader_criteria`
        feature where it is most prominently used [ticket:5760]:

        * Fixed the issue where boolean True/False values, which were referred
          to in the closure variables of the lambda, would cause failures.
          [ticket:5763]

        * Repaired a non-working detection for Python functions embedded in the
          lambda that produce bound values; this case is likely not supportable
          so raises an informative error, where the function should be invoked
          outside the lambda itself.  New documentation has been added to
          further detail this behavior. [ticket:5770]

        * The lambda system by default now rejects the use of non-SQL elements
          within the closure variables of the lambda entirely, where the error
          suggests the two options of either explicitly ignoring closure variables
          that are not SQL parameters, or specifying a specific set of values to be
          considered as part of the cache key based on hash value.   This critically
          prevents the lambda system from assuming that arbitrary objects within
          the lambda's closure are appropriate for caching while also refusing to
          ignore them by default, preventing the case where their state might
          not be constant and have an impact on the SQL construct produced.
          The error message is comprehensive and new documentation has been
          added to further detail this behavior. [ticket:5765]

        * Fixed support for the edge case where an ``in_()`` expression
          against a list of SQL elements, such as :func:`_sql.literal` objects,
          would fail to be accommodated correctly. [ticket:5768]


    .. change::
        :tags: bug, orm
        :tickets: 5760, 5766, 5762, 5761, 5764

        Related to the fixes for the lambda criteria system within Core, within the
        ORM implemented a variety of fixes for the
        :func:`_orm.with_loader_criteria` feature as well as the
        :meth:`_orm.SessionEvents.do_orm_execute` event handler that is often
        used in conjunction [ticket:5760]:


        * fixed issue where :func:`_orm.with_loader_criteria` function would fail
          if the given entity or base included non-mapped mixins in its descending
          class hierarchy [ticket:5766]

        * The :func:`_orm.with_loader_criteria` feature is now unconditionally
          disabled for the case of ORM "refresh" operations, including loads
          of deferred or expired column attributes as well as for explicit
          operations like :meth:`_orm.Session.refresh`.  These loads are necessarily
          based on primary key identity where additional WHERE criteria is
          never appropriate.  [ticket:5762]

        * Added new attribute :attr:`_orm.ORMExecuteState.is_column_load` to indicate
          that a :meth:`_orm.SessionEvents.do_orm_execute` handler that a particular
          operation is a primary-key-directed column attribute load, where additional
          criteria should not be added.  The :func:`_orm.with_loader_criteria`
          function as above ignores these in any case now.  [ticket:5761]

        * Fixed issue where the :attr:`_orm.ORMExecuteState.is_relationship_load`
          attribute would not be set correctly for many lazy loads as well as all
          selectinloads.  The flag is essential in order to test if options should
          be added to statements or if they would already have been propagated via
          relationship loads.  [ticket:5764]


    .. change::
        :tags: usecase, orm

        Added :attr:`_orm.ORMExecuteState.bind_mapper` and
        :attr:`_orm.ORMExecuteState.all_mappers` accessors to
        :class:`_orm.ORMExecuteState` event object, so that handlers can respond to
        the target mapper and/or mapped class or classes involved in an ORM
        statement execution.

    .. change::
        :tags: bug, engine, postgresql, oracle

        Adjusted the "setinputsizes" logic relied upon by the cx_Oracle, asyncpg
        and pg8000 dialects to support a :class:`.TypeDecorator` that includes
        an override the :meth:`.TypeDecorator.get_dbapi_type()` method.


    .. change::
        :tags: postgresql, performance

        Enhanced the performance of the asyncpg dialect by caching the asyncpg
        PreparedStatement objects on a per-connection basis. For a test case that
        makes use of the same statement on a set of pooled connections this appears
        to grant a 10-20% speed improvement.  The cache size is adjustable and may
        also be disabled.

        .. seealso::

            :ref:`asyncpg_prepared_statement_cache`

    .. change::
        :tags: feature, mysql
        :tickets: 5747

        Added support for the aiomysql driver when using the asyncio SQLAlchemy
        extension.

        .. seealso::

          :ref:`aiomysql`

    .. change::
        :tags: bug, reflection
        :tickets: 5684

        Fixed bug where the now-deprecated ``autoload`` parameter was being called
        internally within the reflection routines when a related table were
        reflected.


    .. change::
        :tags: platform, performance
        :tickets: 5681

        Adjusted some elements related to internal class production at import time
        which added significant latency to the time spent to import the library vs.
        that of 1.3.   The time is now about 20-30% slower than 1.3 instead of
        200%.


    .. change::
        :tags: changed, schema
        :tickets: 5775

        Altered the behavior of the :class:`_schema.Identity` construct such that
        when applied to a :class:`_schema.Column`, it will automatically imply that
        the value of :paramref:`_sql.Column.nullable` should default to ``False``,
        in a similar manner as when the :paramref:`_sql.Column.primary_key`
        parameter is set to ``True``.   This matches the default behavior of all
        supporting databases where ``IDENTITY`` implies ``NOT NULL``.  The
        PostgreSQL backend is the only one that supports adding ``NULL`` to an
        ``IDENTITY`` column, which is here supported by passing a ``True`` value
        for the :paramref:`_sql.Column.nullable` parameter at the same time.


    .. change::
        :tags: bug, postgresql
        :tickets: 5698

        Fixed a small regression where the query for "show
        standard_conforming_strings" upon initialization would be emitted even if
        the server version info were detected as less than version 8.2, previously
        it would only occur for server version 8.2 or greater. The query fails on
        Amazon Redshift which reports a PG server version older than this value.


    .. change::
        :tags: bug, sql, postgresql, mysql, sqlite
        :tickets: 5169

        An informative error message is now raised for a selected set of DML
        methods (currently all part of :class:`_dml.Insert` constructs) if they are
        called a second time, which would implicitly cancel out the previous
        setting.  The methods altered include:
        :class:`_sqlite.Insert.on_conflict_do_update`,
        :class:`_sqlite.Insert.on_conflict_do_nothing` (SQLite),
        :class:`_postgresql.Insert.on_conflict_do_update`,
        :class:`_postgresql.Insert.on_conflict_do_nothing` (PostgreSQL),
        :class:`_mysql.Insert.on_duplicate_key_update` (MySQL)

    .. change::
        :tags: pool, tests, usecase
        :tickets: 5582

        Improve documentation and add test for sub-second pool timeouts.
        Pull request courtesy Jordan Pittier.

    .. change::
        :tags: bug, general

        Fixed a SQLite source file that had non-ascii characters inside of its
        docstring without a source encoding, introduced within the "INSERT..ON
        CONFLICT" feature, which would cause failures under Python 2.

    .. change::
        :tags: sqlite, usecase
        :tickets: 4010

        Implemented INSERT... ON CONFLICT clause for SQLite. Pull request courtesy
        Ramon Williams.

        .. seealso::

            :ref:`sqlite_on_conflict_insert`

    .. change::
        :tags: bug, asyncio
        :tickets: 5811

        Implemented "connection-binding" for :class:`.AsyncSession`, the ability to
        pass an :class:`.AsyncConnection` to create an :class:`.AsyncSession`.
        Previously, this use case was not implemented and would use the associated
        engine when the connection were passed.  This fixes the issue where the
        "join a session to an external transaction" use case would not work
        correctly for the :class:`.AsyncSession`.  Additionally, added methods
        :meth:`.AsyncConnection.in_transaction`,
        :meth:`.AsyncConnection.in_nested_transaction`,
        :meth:`.AsyncConnection.get_transaction`,
        :meth:`.AsyncConnection.get_nested_transaction` and
        :attr:`.AsyncConnection.info` attribute.

    .. change::
        :tags: usecase, asyncio

        The :class:`.AsyncEngine`, :class:`.AsyncConnection` and
        :class:`.AsyncTransaction` objects may be compared using Python ``==`` or
        ``!=``, which will compare the two given objects based on the "sync" object
        they are proxying towards. This is useful as there are cases particularly
        for :class:`.AsyncTransaction` where multiple instances of
        :class:`.AsyncTransaction` can be proxying towards the same sync
        :class:`_engine.Transaction`, and are actually equivalent.   The
        :meth:`.AsyncConnection.get_transaction` method will currently return a new
        proxying :class:`.AsyncTransaction` each time as the
        :class:`.AsyncTransaction` is not otherwise statefully associated with its
        originating :class:`.AsyncConnection`.

    .. change::
        :tags: bug, oracle
        :tickets: 5884

        Oracle two-phase transactions at a rudimentary level are now no longer
        deprecated. After receiving support from cx_Oracle devs we can provide for
        basic xid + begin/prepare support with some limitations, which will work
        more fully in an upcoming release of cx_Oracle. Two phase "recovery" is not
        currently supported.

    .. change::
        :tags: asyncio

        The SQLAlchemy async mode now detects and raises an informative
        error when an non asyncio compatible :term:`DBAPI` is used.
        Using a standard ``DBAPI`` with async SQLAlchemy will cause
        it to block like any sync call, interrupting the executing asyncio
        loop.

    .. change::
        :tags: usecase, orm, asyncio
        :tickets: 5796, 5797, 5802

        Added :meth:`_asyncio.AsyncSession.scalar`,
        :meth:`_asyncio.AsyncSession.get` as well as support for
        :meth:`_orm.sessionmaker.begin` to work as an async context manager with
        :class:`_asyncio.AsyncSession`.  Also added
        :meth:`_asyncio.AsyncSession.in_transaction` accessor.

    .. change::
        :tags: bug, sql
        :tickets: 5785

        Fixed issue in new :class:`_sql.Values` construct where passing tuples of
        objects would fall back to per-value type detection rather than making use
        of the :class:`_schema.Column` objects passed directly to
        :class:`_sql.Values` that tells SQLAlchemy what the expected type is. This
        would lead to issues for objects such as enumerations and numpy strings
        that are not actually necessary since the expected type is given.

    .. change::
        :tags: bug, engine

        Added the "future" keyword to the list of words that are known by the
        :func:`_sa.engine_from_config` function, so that the values "true" and
        "false" may be configured as "boolean" values when using a key such
        as ``sqlalchemy.future = true`` or ``sqlalchemy.future = false``.


    .. change::
        :tags: usecase, schema
        :tickets: 5712

        The :meth:`_events.DDLEvents.column_reflect` event may now be applied to a
        :class:`_schema.MetaData` object where it will take effect for the
        :class:`_schema.Table` objects local to that collection.

        .. seealso::

            :meth:`_events.DDLEvents.column_reflect`

            :ref:`mapper_automated_reflection_schemes` - in the ORM mapping documentation

            :ref:`automap_intercepting_columns` - in the :ref:`automap_toplevel` documentation




    .. change::
        :tags: feature, engine

        Dialect-specific constructs such as
        :meth:`_postgresql.Insert.on_conflict_do_update` can now stringify in-place
        without the need to specify an explicit dialect object.  The constructs,
        when called upon for ``str()``, ``print()``, etc. now have internal
        direction to call upon their appropriate dialect rather than the
        "default"dialect which doesn't know how to stringify these.   The approach
        is also adapted to generic schema-level create/drop such as
        :class:`_schema.AddConstraint`, which will adapt its stringify dialect to
        one indicated by the element within it, such as the
        :class:`_postgresql.ExcludeConstraint` object.


    .. change::
        :tags: feature, engine
        :tickets: 5911

        Added new execution option
        :paramref:`_engine.Connection.execution_options.logging_token`. This option
        will add an additional per-message token to log messages generated by the
        :class:`_engine.Connection` as it executes statements. This token is not
        part of the logger name itself (that part can be affected using the
        existing :paramref:`_sa.create_engine.logging_name` parameter), so is
        appropriate for ad-hoc connection use without the side effect of creating
        many new loggers. The option can be set at the level of
        :class:`_engine.Connection` or :class:`_engine.Engine`.

        .. seealso::

          :ref:`dbengine_logging_tokens`

    .. change::
        :tags: bug, pool
        :tickets: 5708

        Fixed regression where a connection pool event specified with a keyword,
        most notably ``insert=True``, would be lost when the event were set up.
        This would prevent startup events that need to fire before dialect-level
        events from working correctly.


    .. change::
        :tags: usecase, pool
        :tickets: 5708, 5497

        The internal mechanics of the engine connection routine has been altered
        such that it's now guaranteed that a user-defined event handler for the
        :meth:`_pool.PoolEvents.connect` handler, when established using
        ``insert=True``, will allow an event handler to run that is definitely
        invoked **before** any dialect-specific initialization starts up, most
        notably when it does things like detect default schema name.
        Previously, this would occur in most cases but not unconditionally.
        A new example is added to the schema documentation illustrating how to
        establish the "default schema name" within an on-connect event.

    .. change::
        :tags: usecase, postgresql

        Added a read/write ``.autocommit`` attribute to the DBAPI-adaptation layer
        for the asyncpg dialect.   This so that when working with DBAPI-specific
        schemes that need to use "autocommit" directly with the DBAPI connection,
        the same ``.autocommit`` attribute which works with both psycopg2 as well
        as pg8000 is available.

    .. change::
        :tags: bug, oracle
        :tickets: 5716

        The Oracle dialect now uses
        ``select sys_context( 'userenv', 'current_schema' ) from dual`` to get
        the default schema name, rather than ``SELECT USER FROM DUAL``, to
        accommodate for changes to the session-local schema name under Oracle.

    .. change::
        :tags: schema, feature
        :tickets: 5659

        Added :meth:`_types.TypeEngine.as_generic` to map dialect-specific types,
        such as :class:`sqlalchemy.dialects.mysql.INTEGER`, with the "best match"
        generic SQLAlchemy type, in this case :class:`_types.Integer`.  Pull
        request courtesy Andrew Hannigan.

        .. seealso::

          :ref:`metadata_reflection_dbagnostic_types` - example usage

    .. change::
        :tags: bug, sql
        :tickets: 5717

        Fixed issue where a :class:`.RemovedIn20Warning` would erroneously emit
        when the ``.bind`` attribute were accessed internally on objects,
        particularly when stringifying a SQL construct.

    .. change::
        :tags: bug, orm
        :tickets: 5781

        Fixed 1.4 regression where the use of :meth:`_orm.Query.having` in
        conjunction with queries with internally adapted SQL elements (common in
        inheritance scenarios) would fail due to an incorrect function call. Pull
        request courtesy esoh.


    .. change::
        :tags: bug, pool, pypy
        :tickets: 5842

        Fixed issue where connection pool would not return connections to the pool
        or otherwise be finalized upon garbage collection under pypy if the checked
        out connection fell out of scope without being closed.   This is a long
        standing issue due to pypy's difference in GC behavior that does not call
        weakref finalizers if they are relative to another object that is also
        being garbage collected.  A strong reference to the related record is now
        maintained so that the weakref has a strong-referenced "base" to trigger
        off of.

    .. change::
        :tags: bug, sqlite
        :tickets: 5699

        Use python ``re.search()`` instead of ``re.match()`` as the operation
        used by the :meth:`Column.regexp_match` method when using sqlite.
        This matches the behavior of regular expressions on other databases
        as well as that of well-known SQLite plugins.

    .. change::
        :tags: changed, postgresql

        Fixed issue where the psycopg2 dialect would silently pass the
        ``use_native_unicode=False`` flag without actually having any effect under
        Python 3, as the psycopg2 DBAPI uses Unicode unconditionally under Python
        3.  This usage now raises an :class:`_exc.ArgumentError` when used under
        Python 3. Added test support for Python 2.

    .. change::
        :tags: bug, postgresql
        :tickets: 5722
        :versions: 1.4.0b2

        Established support for :class:`_schema.Column` objects as well as ORM
        instrumented attributes as keys in the ``set_`` dictionary passed to the
        :meth:`_postgresql.Insert.on_conflict_do_update` and
        :meth:`_sqlite.Insert.on_conflict_do_update` methods, which match to the
        :class:`_schema.Column` objects in the ``.c`` collection of the target
        :class:`_schema.Table`. Previously,  only string column names were
        expected; a column expression would be assumed to be an out-of-table
        expression that would render fully along with a warning.

    .. change::
        :tags: feature, sql
        :tickets: 3566

        Implemented support for "table valued functions" along with additional
        syntaxes supported by PostgreSQL, one of the most commonly requested
        features. Table valued functions are SQL functions that return lists of
        values or rows, and are prevalent in PostgreSQL in the area of JSON
        functions, where the "table value" is commonly referred to as the
        "record" datatype. Table valued functions are also supported by Oracle and
        SQL Server.

        Features added include:

        * the :meth:`_functions.FunctionElement.table_valued` modifier that creates a table-like
          selectable object from a SQL function
        * A :class:`_sql.TableValuedAlias` construct that renders a SQL function
          as a named table
        * Support for PostgreSQL's special "derived column" syntax that includes
          column names and sometimes datatypes, such as for the
          ``json_to_recordset`` function, using the
          :meth:`_sql.TableValuedAlias.render_derived` method.
        * Support for PostgreSQL's "WITH ORDINALITY" construct using the
          :paramref:`_functions.FunctionElement.table_valued.with_ordinality` parameter
        * Support for selection FROM a SQL function as column-valued scalar, a
          syntax supported by PostgreSQL and Oracle, via the
          :meth:`_functions.FunctionElement.column_valued` method
        * A way to SELECT a single column from a table-valued expression without
          using a FROM clause via the :meth:`_functions.FunctionElement.scalar_table_valued`
          method.

        .. seealso::

          :ref:`tutorial_functions_table_valued` - in the :ref:`unified_tutorial`

    .. change::
        :tags: bug, asyncio
        :tickets: 5827

        Fixed bug in asyncio connection pool where ``asyncio.TimeoutError`` would
        be raised rather than :class:`.exc.TimeoutError`.  Also repaired the
        :paramref:`_sa.create_engine.pool_timeout` parameter set to zero when using
        the async engine, which previously would ignore the timeout and block
        rather than timing out immediately as is the behavior with regular
        :class:`.QueuePool`.

    .. change::
        :tags: bug, postgresql, asyncio
        :tickets: 5824

        Fixed bug in asyncpg dialect where a failure during a "commit" or less
        likely a "rollback" should cancel the entire transaction; it's no longer
        possible to emit rollback. Previously the connection would continue to
        await a rollback that could not succeed as asyncpg would reject it.

    .. change::
        :tags: bug, orm

        Fixed an issue where the API to create a custom executable SQL construct
        using the ``sqlalchemy.ext.compiles`` extension according to the
        documentation that's been up for many years would no longer function if
        only ``Executable, ClauseElement`` were used as the base classes,
        additional classes were needed if wanting to use
        :meth:`_orm.Session.execute`. This has been resolved so that those extra
        classes aren't needed.

    .. change::
        :tags: bug, regression, orm
        :tickets: 5867

        Fixed ORM unit of work regression where an errant "assert primary_key"
        statement interferes with primary key generation sequences that don't
        actually consider the columns in the table to use a real primary key
        constraint, instead using :paramref:`_orm.Mapper.primary_key` to establish
        certain columns as "primary".

    .. change::
        :tags: bug, sql
        :tickets: 5722
        :versions: 1.4.0b2

        Properly render ``cycle=False`` and ``order=False`` as ``NO CYCLE`` and
        ``NO ORDER`` in :class:`_sql.Sequence` and :class:`_sql.Identity`
        objects.

    .. change::
        :tags: schema, usecase
        :tickets: 2843

        Added parameters :paramref:`_ddl.CreateTable.if_not_exists`,
        :paramref:`_ddl.CreateIndex.if_not_exists`,
        :paramref:`_ddl.DropTable.if_exists` and
        :paramref:`_ddl.DropIndex.if_exists` to the :class:`_ddl.CreateTable`,
        :class:`_ddl.DropTable`, :class:`_ddl.CreateIndex` and
        :class:`_ddl.DropIndex` constructs which result in "IF NOT EXISTS" / "IF
        EXISTS" DDL being added to the CREATE/DROP. These phrases are not accepted
        by all databases and the operation will fail on a database that does not
        support it as there is no similarly compatible fallback within the scope of
        a single DDL statement.  Pull request courtesy Ramon Williams.

    .. change::
        :tags: bug, pool, asyncio
        :tickets: 5823

        When using an asyncio engine, the connection pool will now detach and
        discard a pooled connection that is was not explicitly closed/returned to
        the pool when its tracking object is garbage collected, emitting a warning
        that the connection was not properly closed.   As this operation occurs
        during Python gc finalizers, it's not safe to run any IO operations upon
        the connection including transaction rollback or connection close as this
        will often be outside of the event loop.

        The ``AsyncAdaptedQueue`` used by default on async dpapis
        should instantiate a queue only when it's first used
        to avoid binding it to a possibly wrong event loop.

.. changelog::
    :version: 1.4.0b1
    :released: March 15, 2021
    :released: November 2, 2020

    .. change::
        :tags: feature, orm
        :tickets: 5159

        The ORM can now generate queries previously only available when using
        :class:`_orm.Query` using the :func:`_sql.select` construct directly.
        A new system by which ORM "plugins" may establish themselves within a
        Core :class:`_sql.Select` allow the majority of query building logic
        previously inside of :class:`_orm.Query` to now take place within
        a compilation-level extension for :class:`_sql.Select`.  Similar changes
        have been made for the :class:`_sql.Update` and :class:`_sql.Delete`
        constructs as well.  The constructs when invoked using :meth:`_orm.Session.execute`
        now do ORM-related work within the method. For :class:`_sql.Select`,
        the :class:`_engine.Result` object returned now contains ORM-level
        entities and results.

        .. seealso::

            :ref:`change_5159`

    .. change::
        :tags: feature,sql
        :tickets: 4737

        Added "from linting" as a built-in feature to the SQL compiler.  This
        allows the compiler to maintain graph of all the FROM clauses in a
        particular SELECT statement, linked by criteria in either the WHERE
        or in JOIN clauses that link these FROM clauses together.  If any two
        FROM clauses have no path between them, a warning is emitted that the
        query may be producing a cartesian product.   As the Core expression
        language as well as the ORM are built on an "implicit FROMs" model where
        a particular FROM clause is automatically added if any part of the query
        refers to it, it is easy for this to happen inadvertently and it is
        hoped that the new feature helps with this issue.

        .. seealso::

            :ref:`change_4737`

    .. change::
        :tags: deprecated, orm
        :tickets: 5606

        The "slice index" feature used by :class:`_orm.Query` as well as by the
        dynamic relationship loader will no longer accept negative indexes in
        SQLAlchemy 2.0.  These operations do not work efficiently and load the
        entire collection in, which is both surprising and undesirable.   These
        will warn in 1.4 unless the :paramref:`_orm.Session.future` flag is set in
        which case they will raise IndexError.


    .. change::
        :tags: sql, change
        :tickets: 4617

        The "clause coercion" system, which is SQLAlchemy Core's system of receiving
        arguments and resolving them into :class:`_expression.ClauseElement` structures in order
        to build up SQL expression objects, has been rewritten from a series of
        ad-hoc functions to a fully consistent class-based system.   This change
        is internal and should have no impact on end users other than more specific
        error messages when the wrong kind of argument is passed to an expression
        object, however the change is part of a larger set of changes involving
        the role and behavior of :func:`_expression.select` objects.


    .. change::
        :tags: bug, mysql

        The MySQL and MariaDB dialects now query from the information_schema.tables
        system view in order to determine if a particular table exists or not.
        Previously, the "DESCRIBE" command was used with an exception catch to
        detect non-existent,  which would have the undesirable effect of emitting a
        ROLLBACK on the connection. There appeared to be legacy encoding issues
        which prevented the use of "SHOW TABLES", for this, but as MySQL support is
        now at 5.0.2  or above due to :ticket:`4189`, the information_schema tables
        are now available in all cases.


    .. change::
        :tags: bug, orm
        :tickets: 5122

        A query that is against a mapped inheritance subclass which also uses
        :meth:`_query.Query.select_entity_from` or a similar technique in order  to
        provide an existing subquery to SELECT from, will now raise an error if the
        given subquery returns entities that do not correspond to the given
        subclass, that is, they are sibling or superclasses in the same hierarchy.
        Previously, these would be returned without error.  Additionally, if the
        inheritance mapping is a single-inheritance mapping, the given subquery
        must apply the appropriate filtering against the polymorphic discriminator
        column in order to avoid this error; previously, the :class:`_query.Query` would
        add this criteria to the outside query however this interferes with some
        kinds of query that return other kinds of entities as well.

        .. seealso::

            :ref:`change_5122`

    .. change::
        :tags: bug, engine
        :tickets: 5004

        Revised the :paramref:`.Connection.execution_options.schema_translate_map`
        feature such that the processing of the SQL statement to receive a specific
        schema name occurs within the execution phase of the statement, rather than
        at the compile phase.   This is to support the statement being efficiently
        cached.   Previously, the current schema being rendered into the statement
        for a particular run would be considered as part of the cache key itself,
        meaning that for a run against hundreds of schemas, there would be hundreds
        of cache keys, rendering the cache much less performant.  The new behavior
        is that the rendering is done in a similar  manner as the "post compile"
        rendering added in 1.4 as part of :ticket:`4645`, :ticket:`4808`.

    .. change::
        :tags: usecase, sql
        :tickets: 527

        The :meth:`.Index.create` and :meth:`.Index.drop` methods now have a
        parameter :paramref:`.Index.create.checkfirst`, in the same way as that of
        :class:`_schema.Table` and :class:`.Sequence`, which when enabled will cause the
        operation to detect if the index exists (or not) before performing a create
        or drop operation.


    .. change::
        :tags: sql, postgresql
        :tickets: 5498

        Allow specifying the data type when creating a :class:`.Sequence` in
        PostgreSQL by using the parameter :paramref:`.Sequence.data_type`.

    .. change::
        :tags: change, mssql
        :tickets: 5084

        SQL Server OFFSET and FETCH keywords are now used for limit/offset, rather
        than using a window function, for SQL Server versions 11 and higher. TOP is
        still used for a query that features only LIMIT.   Pull request courtesy
        Elkin.

    .. change::
        :tags: deprecated, engine
        :tickets: 5526

        The :class:`_engine.URL` object is now an immutable named tuple. To modify
        a URL object, use the :meth:`_engine.URL.set` method to produce a new URL
        object.

        .. seealso::

            :ref:`change_5526` - notes on migration


    .. change::
        :tags: change, postgresql

        When using the psycopg2 dialect for PostgreSQL, psycopg2 minimum version is
        set at 2.7. The psycopg2 dialect relies upon many features of psycopg2
        released in the past few years, so to simplify the dialect, version 2.7,
        released in March, 2017 is now the minimum version required.


    .. change::
        :tags: usecase, sql

        The :func:`.true` and :func:`.false` operators may now be applied as the
        "onclause" of a :func:`_expression.join` on a backend that does not support
        "native boolean" expressions, e.g. Oracle or SQL Server, and the expression
        will render as "1=1" for true and "1=0" false.  This is the behavior that
        was introduced many years ago in :ticket:`2804` for and/or expressions.

    .. change::
        :tags: feature, engine
        :tickets: 5087, 4395, 4959

        Implemented an all-new :class:`_result.Result` object that replaces the previous
        ``ResultProxy`` object.   As implemented in Core, the subclass
        :class:`_result.CursorResult` features a compatible calling interface with the
        previous ``ResultProxy``, and additionally adds a great amount of new
        functionality that can be applied to Core result sets as well as ORM result
        sets, which are now integrated into the same model.   :class:`_result.Result`
        includes features such as column selection and rearrangement, improved
        fetchmany patterns, uniquing, as well as a variety of implementations that
        can be used to create database results from in-memory structures as well.


        .. seealso::

            :ref:`change_result_14_core`


    .. change::
        :tags: renamed, engine
        :tickets: 5244

        The :meth:`_reflection.Inspector.reflecttable` was renamed to
        :meth:`_reflection.Inspector.reflect_table`.

    .. change::
        :tags: change, orm
        :tickets: 4662

        The condition where a pending object being flushed with an identity that
        already exists in the identity map has been adjusted to emit a warning,
        rather than throw a :class:`.FlushError`. The rationale is so that the
        flush will proceed and raise a :class:`.IntegrityError` instead, in the
        same way as if the existing object were not present in the identity map
        already.   This helps with schemes that are using the
        :class:`.IntegrityError` as a means of catching whether or not a row
        already exists in the table.

        .. seealso::

            :ref:`change_4662`


    .. change::
        :tags: bug, sql
        :tickets: 5001

        Fixed issue where when constructing constraints from ORM-bound columns,
        primarily :class:`_schema.ForeignKey` objects but also :class:`.UniqueConstraint`,
        :class:`.CheckConstraint` and others, the ORM-level
        :class:`.InstrumentedAttribute` is discarded entirely, and all ORM-level
        annotations from the columns are removed; this is so that the constraints
        are still fully pickleable without the ORM-level entities being pulled in.
        These annotations are not necessary to be present at the schema/metadata
        level.

    .. change::
        :tags: bug, mysql
        :tickets: 5568

        The "skip_locked" keyword used with ``with_for_update()`` will render "SKIP
        LOCKED" on all MySQL backends, meaning it will fail for MySQL less than
        version 8 and on current MariaDB backends.  This is because those backends
        do not support "SKIP LOCKED" or any equivalent, so this error should not be
        silently ignored.   This is upgraded from a warning in the 1.3 series.


    .. change::
        :tags: performance, postgresql
        :tickets: 5401

        The psycopg2 dialect now defaults to using the very performant
        ``execute_values()`` psycopg2 extension for compiled INSERT statements,
        and also implements RETURNING support when this extension is used.  This
        allows INSERT statements that even include an autoincremented SERIAL
        or IDENTITY value to run very fast while still being able to return the
        newly generated primary key values.   The ORM will then integrate this
        new feature in a separate change.

        .. seealso::

            :ref:`change_5401` - full list of changes regarding the
            ``executemany_mode`` parameter.


    .. change::
        :tags: feature, orm
        :tickets: 4472

        Added the ability to add arbitrary criteria to the ON clause generated
        by a relationship attribute in a query, which applies to methods such
        as :meth:`_query.Query.join` as well as loader options like
        :func:`_orm.joinedload`.   Additionally, a "global" version of the option
        allows limiting criteria to be applied to particular entities in
        a query globally.

        .. seealso::

            :ref:`loader_option_criteria`

            :ref:`do_orm_execute_global_criteria`

            :func:`_orm.with_loader_criteria`

    .. change::
        :tags: renamed, sql

        :class:`_schema.Table` parameter ``mustexist`` has been renamed
        to :paramref:`_schema.Table.must_exist` and will now warn when used.

    .. change::
        :tags: removed, sql
        :tickets: 4632

        The "threadlocal" execution strategy, deprecated in 1.3, has been
        removed for 1.4, as well as the concept of "engine strategies" and the
        ``Engine.contextual_connect`` method.  The "strategy='mock'" keyword
        argument is still accepted for now with a deprecation warning; use
        :func:`.create_mock_engine` instead for this use case.

        .. seealso::

            :ref:`change_4393_threadlocal` - from the 1.3 migration notes which
            discusses the rationale for deprecation.

    .. change::
        :tags: mssql, postgresql, reflection, schema, usecase
        :tickets: 4458

        Improved support for covering indexes (with INCLUDE columns). Added the
        ability for postgresql to render CREATE INDEX statements with an INCLUDE
        clause from Core. Index reflection also report INCLUDE columns separately
        for both mssql and postgresql (11+).

    .. change::
        :tags: change, platform
        :tickets: 5400

        The ``importlib_metadata`` library is used to scan for setuptools
        entrypoints rather than pkg_resources.   as importlib_metadata is a small
        library that is included as of Python 3.8, the compatibility library is
        installed as a dependency for Python versions older than 3.8.


    .. change::
        :tags: feature, sql, mssql, oracle
        :tickets: 4808

        Added new "post compile parameters" feature.  This feature allows a
        :func:`.bindparam` construct to have its value rendered into the SQL string
        before being passed to the DBAPI driver, but after the compilation step,
        using the "literal render" feature of the compiler.  The immediate
        rationale for this feature is to support LIMIT/OFFSET schemes that don't
        work or perform well as bound parameters handled by the database driver,
        while still allowing for SQLAlchemy SQL constructs to be cacheable in their
        compiled form.     The immediate targets for the new feature are the "TOP
        N" clause used by SQL Server (and Sybase) which does not support a bound
        parameter, as well as the "ROWNUM" and optional "FIRST_ROWS()" schemes used
        by the Oracle dialect, the former of which has been known to perform better
        without bound parameters and the latter of which does not support a bound
        parameter.   The feature builds upon the mechanisms first developed to
        support "expanding" parameters for IN expressions.   As part of this
        feature, the Oracle ``use_binds_for_limits`` feature is turned on
        unconditionally and this flag is now deprecated.

        .. seealso::

            :ref:`change_4808`

    .. change::
        :tags: feature, sql
        :tickets: 1390

        Add support for regular expression on supported backends.
        Two operations have been defined:

        * :meth:`_sql.ColumnOperators.regexp_match` implementing a regular
          expression match like function.
        * :meth:`_sql.ColumnOperators.regexp_replace` implementing a regular
          expression string replace function.

        Supported backends include SQLite, PostgreSQL, MySQL / MariaDB, and Oracle.

        .. seealso::

            :ref:`change_1390`

    .. change::
        :tags: bug, orm
        :tickets: 4696

        The internal attribute symbols NO_VALUE and NEVER_SET have been unified, as
        there was no meaningful difference between these two symbols, other than a
        few codepaths where they were differentiated in subtle and undocumented
        ways, these have been fixed.


    .. change::
        :tags: oracle, bug

        Correctly render :class:`_schema.Sequence` and :class:`_schema.Identity`
        column options ``nominvalue`` and ``nomaxvalue`` as ``NOMAXVALUE` and
        ``NOMINVALUE`` on oracle database.

    .. change::
        :tags: bug, schema
        :tickets: 4262

        Cleaned up the internal ``str()`` for datatypes so that all types produce a
        string representation without any dialect present, including that it works
        for third-party dialect types without that dialect being present.  The
        string representation defaults to being the UPPERCASE name of that type
        with nothing else.


    .. change::
        :tags: deprecated, sql
        :tickets: 5010

        The :meth:`_sql.Join.alias` method is deprecated and will be removed in
        SQLAlchemy 2.0.   An explicit select + subquery, or aliasing of the inner
        tables, should be used instead.


    .. change::
        :tags: bug, orm
        :tickets: 4194

        Fixed bug where a versioning column specified on a mapper against a
        :func:`_expression.select` construct where the version_id_col itself were against the
        underlying table would incur additional loads when accessed, even if the
        value were locally persisted by the flush.  The actual fix is a result of
        the changes in :ticket:`4617`,  by fact that a :func:`_expression.select` object no
        longer has a ``.c`` attribute and therefore does not confuse the mapper
        into thinking there's an unknown column value present.

    .. change::
        :tags: bug, orm
        :tickets: 3858

        An ``UnmappedInstanceError`` is now raised for :class:`.InstrumentedAttribute`
        if an instance is an unmapped object. Prior to this an ``AttributeError``
        was raised. Pull request courtesy Ramon Williams.

    .. change::
        :tags: removed, platform
        :tickets: 5634

        Dropped support for python 3.4 and 3.5 that has reached EOL. SQLAlchemy 1.4
        series requires python 2.7 or 3.6+.

        .. seealso::

            :ref:`change_5634`

    .. change::
        :tags: performance, sql
        :tickets: 4639

        An all-encompassing reorganization and refactoring of Core and ORM
        internals now allows all Core and ORM statements within the areas of
        DQL (e.g. SELECTs) and DML (e.g. INSERT, UPDATE, DELETE) to allow their
        SQL compilation as well as the construction of result-fetching metadata
        to be fully cached in most cases.   This effectively provides a transparent
        and generalized version of what the "Baked Query" extension has offered
        for the ORM in past versions.  The new feature can calculate the
        cache key for any given SQL construction based on the string that
        it would ultimately produce for a given dialect, allowing functions that
        compose the equivalent select(), Query(), insert(), update() or delete()
        object each time to have that statement cached after it's generated
        the first time.

        The feature is enabled transparently but includes some new programming
        paradigms that may be employed to make the caching even more efficient.

        .. seealso::

            :ref:`change_4639`

            :ref:`sql_caching`

    .. change::
        :tags: orm, removed
        :tickets: 4638

        All long-deprecated "extension" classes have been removed, including
        MapperExtension, SessionExtension, PoolListener, ConnectionProxy,
        AttributeExtension.  These classes have been deprecated since version 0.7
        long superseded by the event listener system.


    .. change::
        :tags: feature, mssql, sql
        :tickets: 4384

        Added support for the :class:`_types.JSON` datatype on the SQL Server
        dialect using the :class:`_mssql.JSON` implementation, which implements SQL
        Server's JSON functionality against the ``NVARCHAR(max)`` datatype as per
        SQL Server documentation. Implementation courtesy Gord Thompson.

    .. change::
        :tags: change, sql
        :tickets: 4868

        Added a core :class:`Values` object that enables a VALUES construct
        to be used in the FROM clause of an SQL statement for databases that
        support it (mainly PostgreSQL and SQL Server).

    .. change::
        :tags: usecase, mysql
        :tickets: 5496

        Added a new dialect token "mariadb" that may be used in place of "mysql" in
        the :func:`_sa.create_engine` URL.  This will deliver a MariaDB dialect
        subclass of the MySQLDialect in use that forces the "is_mariadb" flag to
        True.  The dialect will raise an error if a server version string that does
        not indicate MariaDB in use is received.   This is useful for
        MariaDB-specific testing scenarios as well as to support applications that
        are hardcoding to MariaDB-only concepts.  As MariaDB and MySQL featuresets
        and usage patterns continue to diverge, this pattern may become more
        prominent.


    .. change::
        :tags: bug, postgresql

        The pg8000 dialect has been revised and modernized for the most recent
        version of the pg8000 driver for PostgreSQL. Pull request courtesy Tony
        Locke. Note that this necessarily pins pg8000 at 1.16.6 or greater,
        which no longer has Python 2 support. Python 2 users who require pg8000
        should ensure their requirements are pinned at ``SQLAlchemy<1.4``.

    .. change::
        :tags: bug, orm
        :tickets: 5074

        The :class:`.Session` object no longer initiates a
        :class:`.SessionTransaction` object immediately upon construction or after
        the previous transaction is closed; instead, "autobegin" logic now
        initiates the new :class:`.SessionTransaction` on demand when it is next
        needed.  Rationale includes to remove reference cycles from a
        :class:`.Session` that has been closed out, as well as to remove the
        overhead incurred by the creation of :class:`.SessionTransaction` objects
        that are often discarded immediately. This change affects the behavior of
        the :meth:`.SessionEvents.after_transaction_create` hook in that the event
        will be emitted when the :class:`.Session` first requires a
        :class:`.SessionTransaction` be present, rather than whenever the
        :class:`.Session` were created or the previous :class:`.SessionTransaction`
        were closed.   Interactions with the :class:`_engine.Engine` and the database
        itself remain unaffected.

        .. seealso::

            :ref:`change_5074`


    .. change::
        :tags: oracle, change

        The LIMIT / OFFSET scheme used in Oracle now makes use of named subqueries
        rather than unnamed subqueries when it transparently rewrites a SELECT
        statement to one that uses a subquery that includes ROWNUM.  The change is
        part of a larger change where unnamed subqueries are no longer directly
        supported by Core, as well as to modernize the internal use of the select()
        construct within the Oracle dialect.


    .. change::
        :tags: feature, engine, orm
        :tickets: 3414

        SQLAlchemy now includes support for Python asyncio within both Core and
        ORM, using the included :ref:`asyncio extension <asyncio_toplevel>`. The
        extension makes use of the `greenlet
        <https://greenlet.readthedocs.io/en/latest/>`_ library in order to adapt
        SQLAlchemy's sync-oriented internals such that an asyncio interface that
        ultimately interacts with an asyncio database adapter is now feasible.  The
        single driver supported at the moment is the
        :ref:`dialect-postgresql-asyncpg` driver for PostgreSQL.

        .. seealso::

            :ref:`change_3414`


    .. change::
        :tags: removed, sql

        Removed the ``sqlalchemy.sql.visitors.iterate_depthfirst`` and
        ``sqlalchemy.sql.visitors.traverse_depthfirst`` functions.  These functions
        were unused by any part of SQLAlchemy.  The
        :func:`_sa.sql.visitors.iterate` and :func:`_sa.sql.visitors.traverse`
        functions are commonly used for these functions.  Also removed unused
        options from the remaining functions including "column_collections",
        "schema_visitor".


    .. change::
        :tags: orm, performance

        The bulk update and delete methods :meth:`.Query.update` and
        :meth:`.Query.delete`, as well as their 2.0-style counterparts, now make
        use of RETURNING when the "fetch" strategy is used in order to fetch the
        list of affected primary key identites, rather than emitting a separate
        SELECT, when the backend in use supports RETURNING.  Additionally, the
        "fetch" strategy will in ordinary cases not expire the attributes that have
        been updated, and will instead apply the updated values directly in the
        same way that the "evaluate" strategy does, to avoid having to refresh the
        object.   The "evaluate" strategy will also fall back to expiring
        attributes that were updated to a SQL expression that was unevaluable in
        Python.

        .. seealso::

            :ref:`change_orm_update_returning_14`

    .. change::
        :tags: bug, orm
        :tickets: 4829

        Added new entity-targeting capabilities to the ORM query context
        help with the case where the :class:`.Session` is using a bind dictionary
        against mapped classes, rather than a single bind, and the :class:`_query.Query`
        is against a Core statement that was ultimately generated from a method
        such as :meth:`_query.Query.subquery`.  First implemented using a deep
        search, the current approach leverages the unified :func:`_sql.select`
        construct to keep track of the first mapper that is part of
        the construct.


    .. change::
        :tags: mssql

        The mssql dialect will assume that at least MSSQL 2005 is used.
        There is no hard exception raised if a previous version is detected,
        but operations may fail for older versions.

    .. change::
        :tags: bug, inheritance, orm
        :tickets: 4212

        An :class:`.ArgumentError` is now raised if both the ``selectable`` and
        ``flat`` parameters are set to True in :func:`.orm.with_polymorphic`. The
        selectable name is already aliased and applying flat=True overrides the
        selectable name with an anonymous name that would've previously caused the
        code to break. Pull request courtesy Ramon Williams.

    .. change::
        :tags: mysql, usecase
        :tickets: 4976

        Added support for use of the :class:`.Sequence` construct with MariaDB 10.3
        and greater, as this is now supported by this database.  The construct
        integrates with the :class:`_schema.Table` object in the same way that it does for
        other databases like PostgreSQL and Oracle; if is present on the integer
        primary key "autoincrement" column, it is used to generate defaults.   For
        backwards compatibility, to support a :class:`_schema.Table` that has a
        :class:`.Sequence` on it to support sequence only databases like Oracle,
        while still not having the sequence fire off for MariaDB, the optional=True
        flag should be set, which indicates the sequence should only be used to
        generate the primary key if the target database offers no other option.

        .. seealso::

            :ref:`change_4976`


    .. change::
        :tags: deprecated, engine
        :tickets: 4634

        The :paramref:`_schema.MetaData.bind` argument as well as the overall
        concept of "bound metadata" is deprecated in SQLAlchemy 1.4 and will be
        removed in SQLAlchemy 2.0.  The parameter as well as related functions now
        emit a :class:`_exc.RemovedIn20Warning` when :ref:`deprecation_20_mode` is
        in use.

        .. seealso::

            :ref:`migration_20_implicit_execution`



    .. change::
        :tags: change, extensions
        :tickets: 5142

        Added new parameter :paramref:`_automap.AutomapBase.prepare.autoload_with`
        which supersedes :paramref:`_automap.AutomapBase.prepare.reflect`
        and :paramref:`_automap.AutomapBase.prepare.engine`.



    .. change::
        :tags: usecase, mssql, postgresql
        :tickets: 4966

        Added support for inspection / reflection of partial indexes / filtered
        indexes, i.e. those which use the ``mssql_where`` or ``postgresql_where``
        parameters, with :class:`_schema.Index`.   The entry is both part of the
        dictionary returned by :meth:`.Inspector.get_indexes` as well as part of a
        reflected :class:`_schema.Index` construct that was reflected.  Pull
        request courtesy Ramon Williams.

    .. change::
        :tags: mssql, feature
        :tickets: 4235, 4633

        Added support for "CREATE SEQUENCE" and full :class:`.Sequence` support for
        Microsoft SQL Server.  This removes the deprecated feature of using
        :class:`.Sequence` objects to manipulate IDENTITY characteristics which
        should now be performed using ``mssql_identity_start`` and
        ``mssql_identity_increment`` as documented at :ref:`mssql_identity`. The
        change includes a new parameter :paramref:`.Sequence.data_type` to
        accommodate SQL Server's choice of datatype, which for that backend
        includes INTEGER, BIGINT, and DECIMAL(n, 0).   The default starting value
        for SQL Server's version of :class:`.Sequence` has been set at 1; this
        default is now emitted within the CREATE SEQUENCE DDL for all backends.

        .. seealso::

            :ref:`change_4235`

    .. change::
        :tags: bug, orm
        :tickets: 4718

        Fixed issue in polymorphic loading internals which would fall back to a
        more expensive, soon-to-be-deprecated form of result column lookup within
        certain unexpiration scenarios in conjunction with the use of
        "with_polymorphic".

    .. change::
        :tags: mssql, reflection
        :tickets: 5527

        As part of the support for reflecting :class:`_schema.Identity` objects,
        the method :meth:`_reflection.Inspector.get_columns` no longer returns
        ``mssql_identity_start`` and ``mssql_identity_increment`` as part of the
        ``dialect_options``. Use the information in the ``identity`` key instead.

    .. change::
        :tags: schema, sql
        :tickets: 5362, 5324, 5360

        Added the :class:`_schema.Identity` construct that can be used to
        configure identity columns rendered with GENERATED { ALWAYS |
        BY DEFAULT } AS IDENTITY. Currently the supported backends are
        PostgreSQL >= 10, Oracle >= 12 and MSSQL (with different syntax
        and a subset of functionalities).

    .. change::
        :tags: change, orm, sql

        A selection of Core and ORM query objects now perform much more of their
        Python computational tasks within the compile step, rather than at
        construction time.  This is to support an upcoming caching model that will
        provide for caching of the compiled statement structure based on a cache
        key that is derived from the statement construct, which itself is expected
        to be newly constructed in Python code each time it is used.    This means
        that the internal state of these objects may not be the same as it used to
        be, as well as that some but not all error raise scenarios for various
        kinds of argument validation will occur within the compilation / execution
        phase, rather than at statement construction time.   See the migration
        notes linked below for complete details.

        .. seealso::

            :ref:`change_deferred_construction`


    .. change::
        :tags: usecase, mssql, reflection
        :tickets: 5506

        Added support for reflection of temporary tables with the SQL Server dialect.
        Table names that are prefixed by a pound sign "#" are now introspected from
        the MSSQL "tempdb" system catalog.

    .. change::
        :tags: firebird, deprecated
        :tickets: 5189

        The Firebird dialect is deprecated, as there is now a 3rd party
        dialect that supports this database.

    .. change::
        :tags: misc, deprecated
        :tickets: 5189

        The Sybase dialect is deprecated.


    .. change::
        :tags: mssql, deprecated
        :tickets: 5189

        The adodbapi and mxODBC dialects are deprecated.


    .. change::
        :tags: mysql, deprecated
        :tickets: 5189

        The OurSQL dialect is deprecated.

    .. change::
        :tags: postgresql, deprecated
        :tickets: 5189

        The pygresql and py-postgresql dialects are deprecated.

    .. change::
       :tags: bug, sql
       :tickets: 4649, 4569

       Registered function names based on :class:`.GenericFunction` are now
       retrieved in a case-insensitive fashion in all cases, removing the
       deprecation logic from 1.3 which temporarily allowed multiple
       :class:`.GenericFunction` objects to exist with differing cases.   A
       :class:`.GenericFunction` that replaces another on the same name whether or
       not it's case sensitive emits a warning before replacing the object.

    .. change::
        :tags: orm, performance, postgresql
        :tickets: 5263

        Implemented support for the psycopg2 ``execute_values()`` extension
        within the ORM flush process via the enhancements to Core made
        in :ticket:`5401`, so that this extension is used
        both as a strategy to batch INSERT statements together as well as
        that RETURNING may now be used among multiple parameter sets to
        retrieve primary key values back in batch.   This allows nearly
        all INSERT statements emitted by the ORM on behalf of PostgreSQL
        to be submitted in batch and also via the ``execute_values()``
        extension which benches at five times faster than plain
        executemany() for this particular backend.

        .. seealso::

            :ref:`change_5263`

    .. change::
        :tags: change, general
        :tickets: 4789

        "python setup.py test" is no longer a test runner, as this is deprecated by
        Pypa.   Please use "tox" with no arguments for a basic test run.


    .. change::
        :tags: usecase, oracle
        :tickets: 4857

        The max_identifier_length for the Oracle dialect is now 128 characters by
        default, unless compatibility version less than 12.2 upon first connect, in
        which case the legacy length of 30 characters is used.  This is a
        continuation of the issue as committed to the 1.3 series which adds max
        identifier length detection upon first connect as well as warns for the
        change in Oracle server.

        .. seealso::

            :ref:`oracle_max_identifier_lengths` - in the Oracle dialect documentation


    .. change::
        :tags: bug, oracle
        :tickets: 4971

        The :class:`_oracle.INTERVAL` class of the Oracle dialect is now correctly
        a subclass of the abstract version of :class:`.Interval` as well as the
        correct "emulated" base class, which allows for correct behavior under both
        native and non-native modes; previously it was only based on
        :class:`.TypeEngine`.


    .. change::
        :tags: bug, orm
        :tickets: 4994

        An error is raised if any persistence-related "cascade" settings are made
        on a :func:`_orm.relationship` that also sets up viewonly=True.   The "cascade"
        settings now default to non-persistence related settings only when viewonly
        is also set.  This is the continuation from :ticket:`4993` where this
        setting was changed to emit a warning in 1.3.

        .. seealso::

            :ref:`change_4994`



    .. change::
        :tags: bug, sql
        :tickets: 5054

        Creating an :func:`.and_` or :func:`.or_` construct with no arguments or
        empty ``*args`` will now emit a deprecation warning, as the SQL produced is
        a no-op (i.e. it renders as a blank string). This behavior is considered to
        be non-intuitive, so for empty or possibly empty :func:`.and_` or
        :func:`.or_` constructs, an appropriate default boolean should be included,
        such as ``and_(True, *args)`` or ``or_(False, *args)``.   As has been the
        case for many major versions of SQLAlchemy, these particular boolean
        values will not render if the ``*args`` portion is non-empty.

    .. change::
        :tags: removed, sql

        Removed the concept of a bound engine from the :class:`.Compiler` object,
        and removed the ``.execute()`` and ``.scalar()`` methods from
        :class:`.Compiler`. These were essentially forgotten methods from over a
        decade ago and had no practical use, and it's not appropriate for the
        :class:`.Compiler` object itself to be maintaining a reference to an
        :class:`_engine.Engine`.

    .. change::
       :tags: performance, engine
       :tickets: 4524

       The pool "pre-ping" feature has been refined to not invoke for a DBAPI
       connection that was just opened in the same checkout operation.  pre ping
       only applies to a DBAPI connection that's been checked into the pool
       and is being checked out again.

    .. change::
        :tags: deprecated, engine

        The ``server_side_cursors`` engine-wide parameter is deprecated and will be
        removed in a future release.  For unbuffered cursors, the
        :paramref:`_engine.Connection.execution_options.stream_results` execution
        option should be used on a per-execution basis.

    .. change::
        :tags: bug, orm
        :tickets: 4699

        Improved declarative inheritance scanning to not get tripped up when the
        same base class appears multiple times in the base inheritance list.


    .. change::
        :tags: orm, change
        :tickets: 4395

        The automatic uniquing of rows on the client side is turned off for the new
        :term:`2.0 style` of ORM querying.  This improves both clarity and
        performance.  However, uniquing of rows on the client side is generally
        necessary when using joined eager loading for collections, as there
        will be duplicates of the primary entity for each element in the
        collection because a join was used.  This uniquing must now be manually
        enabled and can be achieved using the new
        :meth:`_engine.Result.unique` modifier.   To avoid silent failure, the ORM
        explicitly requires the method be called when the result of an ORM
        query in 2.0 style makes use of joined load collections.    The newer
        :func:`_orm.selectinload` strategy is likely preferable for eager loading
        of collections in any case.

        .. seealso::

            :ref:`joinedload_not_uniqued`

    .. change::
        :tags: bug, orm
        :tickets: 4195

        Fixed bug in ORM versioning feature where assignment of an explicit
        version_id for a counter configured against a mapped selectable where
        version_id_col is against the underlying table would fail if the previous
        value were expired; this was due to the fact that the  mapped attribute
        would not be configured with active_history=True.


    .. change::
        :tags: mssql, bug, schema
        :tickets: 5597

        Fixed an issue where :meth:`_reflection.has_table` always returned
        ``False`` for temporary tables.

    .. change::
        :tags: mssql, engine
        :tickets: 4809

        Deprecated the ``legacy_schema_aliasing`` parameter to
        :meth:`_sa.create_engine`.   This is a long-outdated parameter that has
        defaulted to False since version 1.1.

    .. change::
        :tags: usecase, orm
        :tickets: 1653

        The evaluator that takes place within the ORM bulk update and delete for
        synchronize_session="evaluate" now supports the IN and NOT IN operators.
        Tuple IN is also supported.


    .. change::
        :tags: change, sql
        :tickets: 5284

        The :func:`_expression.select` construct is moving towards a new calling
        form that is ``select(col1, col2, col3, ..)``, with all other keyword
        arguments removed, as these are all suited using generative methods.    The
        single list of column or table arguments passed to ``select()`` is still
        accepted, however is no longer necessary if expressions are passed in a
        simple positional style.   Other keyword arguments are disallowed when this
        form is used.


        .. seealso::

            :ref:`change_5284`

    .. change::
        :tags: change, sqlite
        :tickets: 4895

        Dropped support for right-nested join rewriting to support old SQLite
        versions prior to 3.7.16, released in 2013.   It is expected that
        all modern Python versions among those now supported should all include
        much newer versions of SQLite.

        .. seealso::

            :ref:`change_4895`


    .. change::
        :tags: deprecated, engine
        :tickets: 5131

        The :meth:`_engine.Connection.connect` method is deprecated as is the concept of
        "connection branching", which copies a :class:`_engine.Connection` into a new one
        that has a no-op ".close()" method.  This pattern is oriented around the
        "connectionless execution" concept which is also being removed in 2.0.

    .. change::
       :tags: bug, general
       :tickets: 4656, 4689

       Refactored the internal conventions used to cross-import modules that have
       mutual dependencies between them, such that the inspected arguments of
       functions and methods are no longer modified.  This allows tools like
       pylint, Pycharm, other code linters, as well as hypothetical pep-484
       implementations added in the future to function correctly as they no longer
       see missing arguments to function calls.   The new approach is also
       simpler and more performant.

       .. seealso::

            :ref:`change_4656`

    .. change::
        :tags: sql, usecase
        :tickets: 5191

        Change the method ``__str`` of :class:`ColumnCollection` to avoid
        confusing it with a python list of string.

    .. change::
        :tags: sql, reflection
        :tickets: 4741

        The "NO ACTION" keyword for foreign key "ON UPDATE" is now considered to be
        the default cascade for a foreign key on all supporting backends (SQlite,
        MySQL, PostgreSQL) and when detected is not included in the reflection
        dictionary; this is already the behavior for PostgreSQL and MySQL for all
        previous SQLAlchemy versions in any case.   The "RESTRICT" keyword is
        positively stored when detected; PostgreSQL does report on this keyword,
        and MySQL as of version 8.0 does as well.  On earlier MySQL versions, it is
        not reported by the database.

    .. change::
        :tags: sql, reflection
        :tickets: 5527, 5324

        Added support for reflecting "identity" columns, which are now returned
        as part of the structure returned by :meth:`_reflection.Inspector.get_columns`.
        When reflecting full :class:`_schema.Table` objects, identity columns will
        be represented using the :class:`_schema.Identity` construct.
        Currently the supported backends are
        PostgreSQL >= 10, Oracle >= 12 and MSSQL (with different syntax
        and a subset of functionalities).

    .. change::
        :tags: feature, sql
        :tickets: 4753

        The :func:`_expression.select` construct and related constructs now allow for
        duplication of column labels and columns themselves in the columns clause,
        mirroring exactly how column expressions were passed in.   This allows
        the tuples returned by an executed result to match what was SELECTed
        for in the first place, which is how the ORM :class:`_query.Query` works, so
        this establishes better cross-compatibility between the two constructs.
        Additionally, it allows column-positioning-sensitive structures such as
        UNIONs (i.e. :class:`_selectable.CompoundSelect`) to be more intuitively constructed
        in those cases where a particular column might appear in more than one
        place.   To support this change, the :class:`_expression.ColumnCollection` has been
        revised to support duplicate columns as well as to allow integer index
        access.

        .. seealso::

            :ref:`change_4753`


    .. change::
        :tags: renamed, sql
        :tickets: 4617

        The :meth:`_expression.SelectBase.as_scalar` and :meth:`_query.Query.as_scalar` methods have
        been renamed to :meth:`_expression.SelectBase.scalar_subquery` and
        :meth:`_query.Query.scalar_subquery`, respectively.  The old names continue to
        exist within 1.4 series with a deprecation warning.  In addition, the
        implicit coercion of :class:`_expression.SelectBase`, :class:`_expression.Alias`, and other
        SELECT oriented objects into scalar subqueries when evaluated in a column
        context is also deprecated, and emits a warning that the
        :meth:`_expression.SelectBase.scalar_subquery` method should be called explicitly.
        This warning will in a later major release become an error, however the
        message will always be clear when :meth:`_expression.SelectBase.scalar_subquery` needs
        to be invoked.   The latter part of the change is for clarity and to reduce
        the implicit decisionmaking by the query coercion system.   The
        :meth:`.Subquery.as_scalar` method, which was previously
        ``Alias.as_scalar``, is also deprecated; ``.scalar_subquery()`` should be
        invoked directly from ` :func:`_expression.select` object or :class:`_query.Query` object.

        This change is part of the larger change to convert :func:`_expression.select` objects
        to no longer be directly part of the "from clause" class hierarchy, which
        also includes an overhaul of the clause coercion system.


    .. change::
        :tags: bug, mssql
        :tickets: 4980

        Fixed the base class of the :class:`_mssql.DATETIMEOFFSET` datatype to
        be based on the :class:`.DateTime` class hierarchy, as this is a
        datetime-holding datatype.


    .. change::
        :tags: bug, engine
        :tickets: 4712

        The :class:`_engine.Connection` object will now not clear a rolled-back
        transaction  until the outermost transaction is explicitly rolled back.
        This is essentially the same behavior that the ORM :class:`.Session` has
        had for a long time, where an explicit call to ``.rollback()`` on all
        enclosing transactions is required for the transaction to logically clear,
        even though the DBAPI-level transaction has already been rolled back.
        The new behavior helps with situations such as the "ORM rollback test suite"
        pattern where the test suite rolls the transaction back within the ORM
        scope, but the test harness which seeks to control the scope of the
        transaction externally does not expect a new transaction to start
        implicitly.

        .. seealso::

            :ref:`change_4712`


    .. change::
        :tags: deprecated, orm
        :tickets: 4719

        Calling the :meth:`_query.Query.instances` method without passing a
        :class:`.QueryContext` is deprecated.   The original use case for this was
        that a :class:`_query.Query` could yield ORM objects when given only the entities
        to be selected as well as a DBAPI cursor object.  However, for this to work
        correctly there is essential metadata that is passed from a SQLAlchemy
        :class:`_engine.ResultProxy` that is derived from the mapped column expressions,
        which comes originally from the :class:`.QueryContext`.   To retrieve ORM
        results from arbitrary SELECT statements, the :meth:`_query.Query.from_statement`
        method should be used.


    .. change::
        :tags: deprecated, sql

        The :class:`_schema.Table` class now raises a deprecation warning
        when columns with the same name are defined. To replace a column a new
        parameter :paramref:`_schema.Table.append_column.replace_existing` was
        added to the :meth:`_schema.Table.append_column` method.

        The :meth:`_expression.ColumnCollection.contains_column` will now
        raises an error when called with a string, suggesting the caller
        to use ``in`` instead.

    .. change::
        :tags: deprecated, engine
        :tickets: 4878

        The :paramref:`.case_sensitive` flag on :func:`_sa.create_engine` is
        deprecated; this flag was part of the transition of the result row object
        to allow case sensitive column matching as the default, while providing
        backwards compatibility for the former matching method.   All string access
        for a row should be assumed to be case sensitive just like any other Python
        mapping.


    .. change::
        :tags: bug, sql
        :tickets: 5127

        Improved the :func:`_sql.tuple_` construct such that it behaves predictably
        when used in a columns-clause context.  The SQL tuple is not supported as a
        "SELECT" columns clause element on most backends; on those that do
        (PostgreSQL, not surprisingly), the Python DBAPI does not have a "nested
        type" concept so there are still challenges in fetching rows for such an
        object. Use of :func:`_sql.tuple_` in a :func:`_sql.select` or
        :class:`_orm.Query` will now raise a :class:`_exc.CompileError` at the
        point at which the :func:`_sql.tuple_` object is seen as presenting itself
        for fetching rows (i.e., if the tuple is in the columns clause of a
        subquery, no error is raised).  For ORM use,the :class:`_orm.Bundle` object
        is an explicit directive that a series of columns should be returned as a
        sub-tuple per row and is suggested by the error message. Additionally ,the
        tuple will now render with parenthesis in all contexts. Previously, the
        parenthesization would not render in a columns context leading to
        non-defined behavior.

    .. change::
        :tags: usecase, sql
        :tickets: 5576

        Add support to ``FETCH {FIRST | NEXT} [ count ]
        {ROW | ROWS} {ONLY | WITH TIES}`` in the select for the supported
        backends, currently PostgreSQL, Oracle and MSSQL.

    .. change::
        :tags: feature, engine, alchemy2
        :tickets: 4644

        Implemented the :paramref:`_sa.create_engine.future` parameter which
        enables forwards compatibility with SQLAlchemy 2. is used for forwards
        compatibility with SQLAlchemy 2.   This engine features
        always-transactional behavior with autobegin.

        .. seealso::

            :ref:`migration_20_toplevel`

    .. change::
        :tags: usecase, sql
        :tickets: 4449

        Additional logic has been added such that certain SQL expressions which
        typically wrap a single database column will use the name of that column as
        their "anonymous label" name within a SELECT statement, potentially making
        key-based lookups in result tuples more intuitive.   The primary example of
        this is that of a CAST expression, e.g. ``CAST(table.colname AS INTEGER)``,
        which will export its default name as "colname", rather than the usual
        "anon_1" label, that is, ``CAST(table.colname AS INTEGER) AS colname``.
        If the inner expression doesn't have a name, then the previous "anonymous
        label" logic is used.  When using SELECT statements that make use of
        :meth:`_expression.Select.apply_labels`, such as those emitted by the ORM, the
        labeling logic will produce ``<tablename>_<inner column name>`` in the same
        was as if the column were named alone.   The logic applies right now to the
        :func:`.cast` and :func:`.type_coerce` constructs as well as some
        single-element boolean expressions.

        .. seealso::

            :ref:`change_4449`

    .. change::
        :tags: feature, orm
        :tickets: 5508

        The ORM Declarative system is now unified into the ORM itself, with new
        import spaces under ``sqlalchemy.orm`` and new kinds of mappings.  Support
        for decorator-based mappings without using a base class, support for
        classical style-mapper() calls that have access to the declarative class
        registry for relationships, and full integration of Declarative with 3rd
        party class attribute systems like ``dataclasses`` and ``attrs`` is now
        supported.

        .. seealso::

            :ref:`change_5508`

            :ref:`change_5027`

    .. change::
        :tags: removed, platform
        :tickets: 5094

        Removed all dialect code related to support for Jython and zxJDBC. Jython
        has not been supported by SQLAlchemy for many years and it is not expected
        that the current zxJDBC code is at all functional; for the moment it just
        takes up space and adds confusion by showing up in documentation. At the
        moment, it appears that Jython has achieved Python 2.7 support in its
        releases but not Python 3.   If Jython were to be supported again, the form
        it should take is against the Python 3 version of Jython, and the various
        zxJDBC stubs for various backends should be implemented as a third party
        dialect.


    .. change::
        :tags: feature, sql
        :tickets: 5221

        Enhanced the disambiguating labels feature of the
        :func:`_expression.select` construct such that when a select statement
        is used in a subquery, repeated column names from different tables are now
        automatically labeled with a unique label name, without the need to use the
        full "apply_labels()" feature that combines tablename plus column name.
        The disambiguated labels are available as plain string keys in the .c
        collection of the subquery, and most importantly the feature allows an ORM
        :func:`_orm.aliased` construct against the combination of an entity and an
        arbitrary subquery to work correctly, targeting the correct columns despite
        same-named columns in the source tables, without the need for an "apply
        labels" warning.


        .. seealso::

            :ref:`migration_20_query_from_self` - Illustrates the new
            disambiguation feature as part of a strategy to migrate away from the
            :meth:`_query.Query.from_self` method.

    .. change::
        :tags: usecase, postgresql
        :tickets: 5549

        Added support for PostgreSQL "readonly" and "deferrable" flags for all of
        psycopg2, asyncpg and pg8000 dialects.   This takes advantage of a newly
        generalized version of the "isolation level" API to support other kinds of
        session attributes set via execution options that are reliably reset
        when connections are returned to the connection pool.

        .. seealso::

            :ref:`postgresql_readonly_deferrable`

    .. change::
        :tags: mysql, feature
        :tickets: 5459

        Added support for MariaDB Connector/Python to the mysql dialect. Original
        pull request courtesy Georg Richter.

    .. change::
        :tags: usecase, orm
        :tickets: 5171

        Enhanced logic that tracks if relationships will be conflicting with each
        other when they write to the same column to include simple cases of two
        relationships that should have a "backref" between them.   This means that
        if two relationships are not viewonly, are not linked with back_populates
        and are not otherwise in an inheriting sibling/overriding arrangement, and
        will populate the same foreign key column, a warning is emitted at mapper
        configuration time warning that a conflict may arise.  A new parameter
        :paramref:`_orm.relationship.overlaps` is added to suit those very rare cases
        where such an overlapping persistence arrangement may be unavoidable.


    .. change::
        :tags: deprecated, orm
        :tickets: 4705, 5202

        Using strings to represent relationship names in ORM operations such as
        :meth:`_orm.Query.join`, as well as strings for all ORM attribute names
        in loader options like :func:`_orm.selectinload`
        is deprecated and will be removed in SQLAlchemy 2.0.  The class-bound
        attribute should be passed instead.  This provides much better specificity
        to the given method, allows for modifiers such as ``of_type()``, and
        reduces internal complexity.

        Additionally, the ``aliased`` and ``from_joinpoint`` parameters to
        :meth:`_orm.Query.join` are also deprecated.   The :func:`_orm.aliased`
        construct now provides for a great deal of flexibility and capability
        and should be used directly.

        .. seealso::

            :ref:`migration_20_orm_query_join_strings`

            :ref:`migration_20_query_join_options`

    .. change::
        :tags: change, platform
        :tickets: 5404

        Installation has been modernized to use setup.cfg for most package
        metadata.

    .. change::
        :tags: bug, sql, postgresql
        :tickets: 5653

        Improved support for column names that contain percent signs in the string,
        including repaired issues involving anonymous labels that also embedded a
        column name with a percent sign in it, as well as re-established support
        for bound parameter names with percent signs embedded on the psycopg2
        dialect, using a late-escaping process similar to that used by the
        cx_Oracle dialect.


    .. change::
        :tags: orm, deprecated
        :tickets: 5134

        Deprecated logic in :meth:`_query.Query.distinct` that automatically adds
        columns in the ORDER BY clause to the columns clause; this will be removed
        in 2.0.

        .. seealso::

            :ref:`migration_20_query_distinct`

    .. change::
       :tags: orm, removed
       :tickets: 4642

       Remove the deprecated loader options ``joinedload_all``, ``subqueryload_all``,
       ``lazyload_all``, ``selectinload_all``. The normal version with method chaining
       should be used in their place.

    .. change::
        :tags: bug, sql
        :tickets: 4887

        Custom functions that are created as subclasses of
        :class:`.FunctionElement` will now generate an "anonymous label" based on
        the "name" of the function just like any other :class:`.Function` object,
        e.g. ``"SELECT myfunc() AS myfunc_1"``. While SELECT statements no longer
        require labels in order for the result proxy object to function, the ORM
        still targets columns in rows by using objects as mapping keys, which works
        more reliably when the column expressions have distinct names.  In any
        case, the behavior is  now made consistent between functions generated by
        :attr:`.func` and those generated as custom :class:`.FunctionElement`
        objects.


    .. change::
        :tags: usecase, extensions
        :tickets: 4887

        Custom compiler constructs created using the :mod:`sqlalchemy.ext.compiled`
        extension will automatically add contextual information to the compiler
        when a custom construct is interpreted as an element in the columns
        clause of a SELECT statement, such that the custom element will be
        targetable as a key in result row mappings, which is the kind of targeting
        that the ORM uses in order to match column elements into result tuples.

    .. change::
        :tags: engine, bug
        :tickets: 5497

        Adjusted the dialect initialization process such that the
        :meth:`_engine.Dialect.on_connect` is not called a second time
        on the first connection.   The hook is called first, then the
        :meth:`_engine.Dialect.initialize` is called if that connection is the
        first for that dialect, then no more events are called.   This eliminates
        the two calls to the "on_connect" function which can produce very
        difficult debugging situations.

    .. change::
        :tags: feature, engine, pyodbc
        :tickets: 5649

        Reworked the "setinputsizes()" set of dialect hooks to be correctly
        extensible for any arbitrary DBAPI, by allowing dialects individual hooks
        that may invoke cursor.setinputsizes() in the appropriate style for that
        DBAPI.   In particular this is intended to support pyodbc's style of usage
        which is fundamentally different from that of cx_Oracle.  Added support
        for pyodbc.


    .. change::
        :tags: deprecated, engine
        :tickets: 4846

        "Implicit autocommit", which is the COMMIT that occurs when a DML or DDL
        statement is emitted on a connection, is deprecated and won't be part of
        SQLAlchemy 2.0.   A 2.0-style warning is emitted when autocommit takes
        effect, so that the calling code may be adjusted to use an explicit
        transaction.

        As part of this change, DDL methods such as
        :meth:`_schema.MetaData.create_all` when used against an
        :class:`_engine.Engine` will run the operation in a BEGIN block if one is
        not started already.

        .. seealso::

            :ref:`deprecation_20_mode`


    .. change::
        :tags: deprecated, orm
        :tickets: 5573

        Passing keyword arguments to methods such as :meth:`_orm.Session.execute`
        to be passed into the :meth:`_orm.Session.get_bind` method is deprecated;
        the new :paramref:`_orm.Session.execute.bind_arguments` dictionary should
        be passed instead.


    .. change::
       :tags: renamed, schema
       :tickets: 5413

       Renamed the :meth:`_schema.Table.tometadata` method to
       :meth:`_schema.Table.to_metadata`.  The previous name remains with a
       deprecation warning.

    .. change::
       :tags: bug, sql
       :tickets: 4336

       Reworked the :meth:`_expression.ClauseElement.compare` methods in terms of a new
       visitor-based approach, and additionally added test coverage ensuring that
       all :class:`_expression.ClauseElement` subclasses can be accurately compared
       against each other in terms of structure.   Structural comparison
       capability is used to a small degree within the ORM currently, however
       it also may form the basis for new caching features.

    .. change::
        :tags: feature, orm
        :tickets: 1763

        Eager loaders, such as joined loading, SELECT IN loading, etc., when
        configured on a mapper or via query options will now be invoked during
        the refresh on an expired object; in the case of selectinload and
        subqueryload, since the additional load is for a single object only,
        the "immediateload" scheme is used in these cases which resembles the
        single-parent query emitted by lazy loading.

        .. seealso::

            :ref:`change_1763`

    .. change::
        :tags: usecase, orm
        :tickets: 5018, 3903

        The ORM bulk update and delete operations, historically available via the
        :meth:`_orm.Query.update` and :meth:`_orm.Query.delete` methods as well as
        via the :class:`_dml.Update` and :class:`_dml.Delete` constructs for
        :term:`2.0 style` execution, will now automatically accommodate for the
        additional WHERE criteria needed for a single-table inheritance
        discriminator in order to limit the statement to rows referring to the
        specific subtype requested.   The new :func:`_orm.with_loader_criteria`
        construct is also supported for with bulk update/delete operations.

    .. change::
       :tags: engine, removed
       :tickets: 4643

       Remove deprecated method ``get_primary_keys`` in the :class:`.Dialect` and
       :class:`_reflection.Inspector` classes. Please refer to the
       :meth:`.Dialect.get_pk_constraint` and :meth:`_reflection.Inspector.get_primary_keys`
       methods.

       Remove deprecated event ``dbapi_error`` and the method
       ``ConnectionEvents.dbapi_error``. Please refer to the
       :meth:`_events.ConnectionEvents.handle_error` event.
       This change also removes the attributes ``ExecutionContext.is_disconnect``
       and ``ExecutionContext.exception``.

    .. change::
       :tags: removed, postgresql
       :tickets: 4643

       Remove support for deprecated engine URLs of the form ``postgres://``;
       this has emitted a warning for many years and projects should be
       using ``postgresql://``.

    .. change::
       :tags: removed, mysql
       :tickets: 4643

       Remove deprecated dialect ``mysql+gaerdbms`` that has been deprecated
       since version 1.0. Use the MySQLdb dialect directly.

       Remove deprecated parameter ``quoting`` from :class:`.mysql.ENUM`
       and :class:`.mysql.SET` in the ``mysql`` dialect. The values passed to the
       enum or the set are quoted by SQLAlchemy when needed automatically.

    .. change::
       :tags: removed, orm
       :tickets: 4643

       Remove deprecated function ``comparable_property``. Please refer to the
       :mod:`~sqlalchemy.ext.hybrid` extension. This also removes the function
       ``comparable_using`` in the declarative extension.

       Remove deprecated function ``compile_mappers``.  Please use
       :func:`.configure_mappers`

       Remove deprecated method ``collection.linker``. Please refer to the
       :meth:`.AttributeEvents.init_collection` and
       :meth:`.AttributeEvents.dispose_collection` event handlers.

       Remove deprecated method ``Session.prune`` and parameter
       ``Session.weak_identity_map``. See the recipe at
       :ref:`session_referencing_behavior` for an event-based approach to
       maintaining strong identity references.
       This change also removes the class ``StrongInstanceDict``.

       Remove deprecated parameter ``mapper.order_by``. Use :meth:`_query.Query.order_by`
       to determine the ordering of a result set.

       Remove deprecated parameter ``Session._enable_transaction_accounting``.

       Remove deprecated parameter ``Session.is_modified.passive``.

    .. change::
       :tags: removed, schema
       :tickets: 4643

       Remove deprecated class ``Binary``. Please use :class:`.LargeBinary`.

    .. change::
       :tags: removed, sql
       :tickets: 4643

       Remove deprecated methods ``Compiled.compile``, ``ClauseElement.__and__`` and
       ``ClauseElement.__or__`` and attribute ``Over.func``.

       Remove deprecated ``FromClause.count`` method. Please use the
       :class:`_functions.count` function available from the
       :attr:`.func` namespace.

    .. change::
       :tags: removed,  sql
       :tickets: 4643

       Remove deprecated parameters ``text.bindparams`` and ``text.typemap``.
       Please refer to the :meth:`_expression.TextClause.bindparams` and
       :meth:`_expression.TextClause.columns` methods.

       Remove deprecated parameter ``Table.useexisting``. Please use
       :paramref:`_schema.Table.extend_existing`.

    .. change::
        :tags: bug, orm
        :tickets: 4836

        An exception is now raised if the ORM loads a row for a polymorphic
        instance that has a primary key but the discriminator column is NULL, as
        discriminator columns should not be null.



    .. change::
        :tags: bug, sql
        :tickets: 4002

        Deprecate usage of ``DISTINCT ON`` in dialect other than PostgreSQL.
        Deprecate old usage of string distinct in MySQL dialect

    .. change::
        :tags: orm, usecase
        :tickets: 5237

        Update :paramref:`_orm.relationship.sync_backref` flag in a relationship
        to make it implicitly ``False`` in ``viewonly=True`` relationships,
        preventing synchronization events.


        .. seealso::

            :ref:`change_5237_14`

    .. change::
        :tags: deprecated, engine
        :tickets: 4877

        Deprecated the behavior by which a :class:`_schema.Column` can be used as the key
        in a result set row lookup, when that :class:`_schema.Column` is not part of the
        SQL selectable that is being selected; that is, it is only matched on name.
        A deprecation warning is now emitted for this case.   Various ORM use
        cases, such as those involving :func:`_expression.text` constructs, have been improved
        so that this fallback logic is avoided in most cases.


    .. change::
        :tags: change, schema
        :tickets: 5367

        The :paramref:`.Enum.create_constraint` and
        :paramref:`.Boolean.create_constraint` parameters now default to False,
        indicating when a so-called "non-native" version of these two datatypes is
        created, a CHECK constraint will not be generated by default.   These CHECK
        constraints present schema-management maintenance complexities that should
        be opted in to, rather than being turned on by default.

        .. seealso::

            :ref:`change_5367`

    .. change::
        :tags: feature, sql
        :tickets: 4645

        The "expanding IN" feature, which generates IN expressions at query
        execution time which are based on the particular parameters associated with
        the statement execution, is now used for all IN expressions made against
        lists of literal values.   This allows IN expressions to be fully cacheable
        independently of the list of values being passed, and also includes support
        for empty lists. For any scenario where the IN expression contains
        non-literal SQL expressions, the old behavior of pre-rendering for each
        position in the IN is maintained. The change also completes support for
        expanding IN with tuples, where previously type-specific bind processors
        weren't taking effect.

        .. seealso::

            :ref:`change_4645`

    .. change::
        :tags: bug, mysql
        :tickets: 4189

        MySQL dialect's server_version_info tuple is now all numeric.  String
        tokens like "MariaDB" are no longer present so that numeric comparison
        works in all cases.  The .is_mariadb flag on the dialect should be
        consulted for whether or not mariadb was detected.   Additionally removed
        structures meant to support extremely old MySQL versions 3.x and 4.x;
        the minimum MySQL version supported is now version 5.0.2.


    .. change::
        :tags: engine, feature
        :tickets: 2056

        Added new reflection method :meth:`.Inspector.get_sequence_names` which
        returns all the sequences defined and :meth:`.Inspector.has_sequence` to
        check if a particular sequence exits.
        Support for this method has been added to the backend that support
        :class:`.Sequence`: PostgreSQL, Oracle and MariaDB >= 10.3.

    .. change::
        :tags: usecase, postgresql
        :tickets: 4914

        The maximum buffer size for the :class:`.BufferedRowResultProxy`, which
        is used by dialects such as PostgreSQL when ``stream_results=True``, can
        now be set to a number greater than 1000 and the buffer will grow to
        that size.  Previously, the buffer would not go beyond 1000 even if the
        value were set larger.   The growth of the buffer is also now based
        on a simple multiplying factor currently set to 5.  Pull request courtesy
        Soumaya Mauthoor.


    .. change::
        :tags: bug, orm
        :tickets: 4519

        Accessing a collection-oriented attribute on a newly created object no
        longer mutates ``__dict__``, but still returns an empty collection as has
        always been the case.   This allows collection-oriented attributes to work
        consistently in comparison to scalar attributes which return ``None``, but
        also don't mutate ``__dict__``.  In order to accommodate for the collection
        being mutated, the same empty collection is returned each time once
        initially created, and when it is mutated (e.g. an item appended, added,
        etc.) it is then moved into ``__dict__``.  This removes the last of
        mutating side-effects on read-only attribute access within the ORM.

        .. seealso::

            :ref:`change_4519`

    .. change::
        :tags: change, sql
        :tickets: 4617

        As part of the SQLAlchemy 2.0 migration project, a conceptual change has
        been made to the role of the :class:`_expression.SelectBase` class hierarchy,
        which is the root of all "SELECT" statement constructs, in that they no
        longer serve directly as FROM clauses, that is, they no longer subclass
        :class:`_expression.FromClause`.  For end users, the change mostly means that any
        placement of a :func:`_expression.select` construct in the FROM clause of another
        :func:`_expression.select` requires first that it be wrapped in a subquery first,
        which historically is through the use of the :meth:`_expression.SelectBase.alias`
        method, and is now also available through the use of
        :meth:`_expression.SelectBase.subquery`.    This was usually a requirement in any
        case since several databases don't accept unnamed SELECT subqueries
        in their FROM clause in any case.

        .. seealso::

            :ref:`change_4617`

    .. change::
        :tags: change, sql
        :tickets: 4617

        Added a new Core class :class:`.Subquery`, which takes the place of
        :class:`_expression.Alias` when creating named subqueries against a :class:`_expression.SelectBase`
        object.   :class:`.Subquery` acts in the same way as :class:`_expression.Alias`
        and is produced from the :meth:`_expression.SelectBase.subquery` method; for
        ease of use and backwards compatibility, the :meth:`_expression.SelectBase.alias`
        method is synonymous with this new method.

        .. seealso::

            :ref:`change_4617`

    .. change::
        :tags: change, orm
        :tickets: 4617

        The ORM will now warn when asked to coerce a :func:`_expression.select` construct into
        a subquery implicitly.  This occurs within places such as the
        :meth:`_query.Query.select_entity_from` and  :meth:`_query.Query.select_from` methods
        as well as within the :func:`.with_polymorphic` function.  When a
        :class:`_expression.SelectBase` (which is what's produced by :func:`_expression.select`) or
        :class:`_query.Query` object is passed directly to these functions and others,
        the ORM is typically coercing them to be a subquery by calling the
        :meth:`_expression.SelectBase.alias` method automatically (which is now superseded by
        the :meth:`_expression.SelectBase.subquery` method).   See the migration notes linked
        below for further details.

        .. seealso::

            :ref:`change_4617`

    .. change::
        :tags: bug, sql
        :tickets: 4617

        The ORDER BY clause of a :class:`_selectable.CompoundSelect`, e.g. UNION, EXCEPT, etc.
        will not render the table name associated with a given column when applying
        :meth:`_selectable.CompoundSelect.order_by` in terms of a :class:`_schema.Table` - bound
        column.   Most databases require that the names in the ORDER BY clause be
        expressed as label names only which are matched to names in the first
        SELECT statement.    The change is related to :ticket:`4617` in that a
        previous workaround was to refer to the ``.c`` attribute of the
        :class:`_selectable.CompoundSelect` in order to get at a column that has no table
        name.  As the subquery is now named, this change allows both the workaround
        to continue to work, as well as allows table-bound columns as well as the
        :attr:`_selectable.CompoundSelect.selected_columns` collections to be usable in the
        :meth:`_selectable.CompoundSelect.order_by` method.

    .. change::
        :tags: bug, orm
        :tickets: 5226

        The refresh of an expired object will now trigger an autoflush if the list
        of expired attributes include one or more attributes that were explicitly
        expired or refreshed using the :meth:`.Session.expire` or
        :meth:`.Session.refresh` methods.   This is an attempt to find a middle
        ground between the normal unexpiry of attributes that can happen in many
        cases where autoflush is not desirable, vs. the case where attributes are
        being explicitly expired or refreshed and it is possible that these
        attributes depend upon other pending state within the session that needs to
        be flushed.   The two methods now also gain a new flag
        :paramref:`.Session.expire.autoflush` and
        :paramref:`.Session.refresh.autoflush`, defaulting to True; when set to
        False, this will disable the autoflush that occurs on unexpire for these
        attributes.

    .. change::
        :tags: feature, sql
        :tickets: 5380

        Along with the new transparent statement caching feature introduced as part
        of :ticket:`4369`, a new feature intended to decrease the Python overhead
        of creating statements is added, allowing lambdas to be used when
        indicating arguments being passed to a statement object such as select(),
        Query(), update(), etc., as well as allowing the construction of full
        statements within lambdas in a similar manner as that of the "baked query"
        system.   The rationale of using lambdas is adapted from that of the "baked
        query" approach which uses lambdas to encapsulate any amount of Python code
        into a callable that only needs to be called when the statement is first
        constructed into a string.  The new feature however is more sophisticated
        in that Python literal values that would be passed as parameters are
        automatically extracted, so that there is no longer a need to use
        bindparam() objects with such queries.   Use of the feature is optional and
        can be used to as small or as great a degree as is desired, while still
        allowing statements to be fully cacheable.

        .. seealso::

            :ref:`engine_lambda_caching`


    .. change::
        :tags: feature, orm
        :tickets: 5027

        Added support for direct mapping of Python classes that are defined using
        the Python ``dataclasses`` decorator.    Pull request courtesy Václav
        Klusák.  The new feature integrates into new support at the Declarative
        level for systems such as ``dataclasses`` and ``attrs``.

        .. seealso::

            :ref:`change_5027`

            :ref:`change_5508`


    .. change::
        :tags: change, engine
        :tickets: 4710

        The ``RowProxy`` class is no longer a "proxy" object, and is instead
        directly populated with the post-processed contents of the DBAPI row tuple
        upon construction.   Now named :class:`.Row`, the mechanics of how the
        Python-level value processors have been simplified, particularly as it impacts the
        format of the C code, so that a DBAPI row is processed into a result tuple
        up front.   The object returned by the :class:`_engine.ResultProxy` is now the
        ``LegacyRow`` subclass, which maintains mapping/tuple hybrid behavior,
        however the base :class:`.Row` class now behaves more fully like a named
        tuple.

        .. seealso::

            :ref:`change_4710_core`


    .. change::
        :tags: change, orm
        :tickets: 4710

        The "KeyedTuple" class returned by :class:`_query.Query` is now replaced with the
        Core :class:`.Row` class, which behaves in the same way as KeyedTuple.
        In SQLAlchemy 2.0, both Core and ORM will return result rows using the same
        :class:`.Row` object.   In the interim, Core uses a backwards-compatibility
        class ``LegacyRow`` that maintains the former mapping/tuple hybrid
        behavior used by "RowProxy".

        .. seealso::

            :ref:`change_4710_orm`

    .. change::
        :tags: feature, orm
        :tickets: 4826

        Added "raiseload" feature for ORM mapped columns via :paramref:`.orm.defer.raiseload`
        parameter on :func:`.defer` and :func:`.deferred`.   This provides
        similar behavior for column-expression mapped attributes as the
        :func:`.raiseload` option does for relationship mapped attributes.  The
        change also includes some behavioral changes to deferred columns regarding
        expiration; see the migration notes for details.

        .. seealso::

            :ref:`change_4826`


    .. change::
        :tags: bug, orm
        :tickets: 5150

        The behavior of the :paramref:`_orm.relationship.cascade_backrefs` flag
        will be reversed in 2.0 and set to ``False`` unconditionally, such that
        backrefs don't cascade save-update operations from a forwards-assignment to
        a backwards assignment.   A 2.0 deprecation warning is emitted when the
        parameter is left at its default of ``True`` at the point at which such a
        cascade operation actually takes place.   The new behavior can be
        established as always by setting the flag to ``False`` on a specific
        :func:`_orm.relationship`, or more generally can be set up across the board
        by setting the :paramref:`_orm.Session.future` flag to True.

        .. seealso::

            :ref:`change_5150`

    .. change::
        :tags: deprecated, engine
        :tickets: 4755

        Deprecated remaining engine-level introspection and utility methods
        including :meth:`_engine.Engine.run_callable`, :meth:`_engine.Engine.transaction`,
        :meth:`_engine.Engine.table_names`, :meth:`_engine.Engine.has_table`.   The utility
        methods are superseded by modern context-manager patterns, and the table
        introspection tasks are suited by the :class:`_reflection.Inspector` object.

    .. change::
        :tags: removed, engine
        :tickets: 4755

        The internal dialect method ``Dialect.reflecttable`` has been removed.  A
        review of third party dialects has not found any making use of this method,
        as it was already documented as one that should not be used by external
        dialects.  Additionally, the private ``Engine._run_visitor`` method
        is also removed.


    .. change::
        :tags: removed, engine
        :tickets: 4755

        The long-deprecated ``Inspector.get_table_names.order_by`` parameter has
        been removed.

    .. change::
        :tags: feature, engine
        :tickets: 4755

        The :paramref:`_schema.Table.autoload_with` parameter now accepts an :class:`_reflection.Inspector` object
        directly, as well as any :class:`_engine.Engine` or :class:`_engine.Connection` as was the case before.


    .. change::
        :tags: change, performance, engine, py3k
        :tickets: 5315

        Disabled the "unicode returns" check that runs on dialect startup when
        running under Python 3, which for many years has occurred in order to test
        the current DBAPI's behavior for whether or not it returns Python Unicode
        or Py2K strings for the VARCHAR and NVARCHAR datatypes.  The check still
        occurs by default under Python 2, however the mechanism to test the
        behavior will be removed in SQLAlchemy 2.0 when Python 2 support is also
        removed.

        This logic was very effective when it was needed, however now that Python 3
        is standard, all DBAPIs are expected to return Python 3 strings for
        character datatypes.  In the unlikely case that a third party DBAPI does
        not support this, the conversion logic within :class:`.String` is still
        available and the third party dialect may specify this in its upfront
        dialect flags by setting the dialect level flag ``returns_unicode_strings``
        to one of :attr:`.String.RETURNS_CONDITIONAL` or
        :attr:`.String.RETURNS_BYTES`, both of which will enable Unicode conversion
        even under Python 3.

    .. change::
        :tags: renamed, sql
        :tickets: 5435, 5429

        Several operators are renamed to achieve more consistent naming across
        SQLAlchemy.

        The operator changes are:

        * ``isfalse`` is now ``is_false``
        * ``isnot_distinct_from`` is now ``is_not_distinct_from``
        * ``istrue`` is now ``is_true``
        * ``notbetween`` is now ``not_between``
        * ``notcontains`` is now ``not_contains``
        * ``notendswith`` is now ``not_endswith``
        * ``notilike`` is now ``not_ilike``
        * ``notlike`` is now ``not_like``
        * ``notmatch`` is now ``not_match``
        * ``notstartswith`` is now ``not_startswith``
        * ``nullsfirst`` is now ``nulls_first``
        * ``nullslast`` is now ``nulls_last``
        * ``isnot`` is now ``is_not``
        * ``notin_`` is now ``not_in``

        Because these are core operators, the internal migration strategy for this
        change is to support legacy terms for an extended period of time -- if not
        indefinitely -- but update all documentation, tutorials, and internal usage
        to the new terms.  The new terms are used to define the functions, and
        the legacy terms have been deprecated into aliases of the new terms.



    .. change::
        :tags: orm, deprecated
        :tickets: 5192

        The :func:`.eagerload` and :func:`.relation` were old aliases and are
        now deprecated. Use :func:`_orm.joinedload` and :func:`_orm.relationship`
        respectively.


    .. change::
        :tags: bug, sql
        :tickets: 4621

        The :class:`_expression.Join` construct no longer considers the "onclause" as a source
        of additional FROM objects to be omitted from the FROM list of an enclosing
        :class:`_expression.Select` object as standalone FROM objects. This applies to an ON
        clause that includes a reference to another  FROM object outside the JOIN;
        while this is usually not correct from a SQL perspective, it's also
        incorrect for it to be omitted, and the behavioral change makes the
        :class:`_expression.Select` / :class:`_expression.Join` behave a bit more intuitively.

