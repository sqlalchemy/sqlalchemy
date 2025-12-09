=============
2.0 Changelog
=============

.. changelog_imports::

    .. include:: changelog_14.rst
        :start-line: 5


.. changelog::
    :version: 2.0.45
    :released: December 9, 2025

    .. change::
        :tags: bug, typing
        :tickets: 12730

        Fixed typing issue where :meth:`.Select.with_for_update` would not support
        lists of ORM entities or other FROM clauses in the
        :paramref:`.Select.with_for_update.of` parameter. Pull request courtesy
        Shamil.

    .. change::
        :tags: bug, orm
        :tickets: 12858

        Fixed issue where calling :meth:`.Mapper.add_property` within mapper event
        hooks such as :meth:`.MapperEvents.instrument_class`,
        :meth:`.MapperEvents.after_mapper_constructed`, or
        :meth:`.MapperEvents.before_mapper_configured` would raise an
        ``AttributeError`` because the mapper's internal property collections were
        not yet initialized. The :meth:`.Mapper.add_property` method now handles
        early-stage property additions correctly, allowing properties including
        column properties, deferred columns, and relationships to be added during
        mapper initialization events.  Pull request courtesy G Allajmi.

    .. change::
        :tags: bug, postgresql
        :tickets: 12867

        Fixed issue where PostgreSQL dialect options such as ``postgresql_include``
        on :class:`.PrimaryKeyConstraint` and :class:`.UniqueConstraint` were
        rendered in the wrong position when combined with constraint deferrability
        options like ``deferrable=True``. Pull request courtesy G Allajmi.

    .. change::
        :tags: bug, sql
        :tickets: 12915

        Some improvements to the :meth:`_sql.ClauseElement.params` method to
        replace bound parameters in a query were made, however the ultimate issue
        in :ticket:`12915` involving ORM :func:`_orm.aliased` cannot be fixed fully
        until 2.1, where the method is being rewritten to work without relying on
        Core cloned traversal.

    .. change::
        :tags: bug, sqlite, reflection
        :tickets: 12924

        A series of improvements have been made for reflection of CHECK constraints
        on SQLite. The reflection logic now correctly handles table names
        containing the strings "CHECK" or "CONSTRAINT", properly supports all four
        SQLite identifier quoting styles (double quotes, single quotes, brackets,
        and backticks) for constraint names, and accurately parses CHECK constraint
        expressions containing parentheses within string literals using balanced
        parenthesis matching with string context tracking.    Big thanks to
        GruzdevAV for new test cases and implementation ideas.

    .. change::
        :tags: bug, orm
        :tickets: 12952

        Fixed issue in Python 3.14 where dataclass transformation would fail when
        a mapped class using :class:`.MappedAsDataclass` included a
        :func:`.relationship` referencing a class that was not available at
        runtime (e.g., within a ``TYPE_CHECKING`` block). This occurred when using
        Python 3.14's :pep:`649` deferred annotations feature, which is the
        default behavior without a ``from __future__ import annotations``
        directive.

    .. change::
        :tags: bug, sqlite
        :tickets: 12954

        Fixed issue where SQLite dialect would fail to reflect constraint names
        that contained uppercase letters or other characters requiring quoting. The
        regular expressions used to parse primary key, foreign key, and unique
        constraint names from the ``CREATE TABLE`` statement have been updated to
        properly handle both quoted and unquoted constraint names.

    .. change::
        :tags: bug, typing

        Fixed typing issue where :class:`.coalesce` would not return the correct
        return type when a nullable form of that argument were passed, even though
        this function is meant to select the non-null entry among possibly null
        arguments.  Pull request courtesy Yannick PÉROUX.


    .. change::
        :tags: usecase, mysql
        :tickets: 12964

        Added support for MySQL 8.0.1 + ``FOR SHARE`` to be emitted for the
        :meth:`.Select.with_for_update` method, which offers compatibility with
        ``NOWAIT`` and ``SKIP LOCKED``.  The new syntax is used only for MySQL when
        version 8.0.1 or higher is detected. Pull request courtesy JetDrag.

    .. change::
        :tags: bug, sql
        :tickets: 12987

        Fixed issue where using the :meth:`.ColumnOperators.in_` operator with a
        nested :class:`.CompoundSelect` statement (e.g. an ``INTERSECT`` of
        ``UNION`` queries) would raise a :class:`NotImplementedError` when the
        nested compound select was the first argument to the outer compound select.
        The ``_scalar_type()`` internal method now properly handles nested compound
        selects.

    .. change::
        :tags: bug, postgresql
        :tickets: 13015

        Fixed the structure of the SQL string used for the
        :ref:`engine_insertmanyvalues` feature when an explicit sequence with
        ``nextval()`` is used. The SQL function invocation for the sequence has
        been moved from being rendered inline within each tuple inside of VALUES to
        being rendered once in the SELECT that reads from VALUES. This change
        ensures the function is invoked in the correct order as rows are processed,
        rather than assuming PostgreSQL will execute inline function calls within
        VALUES in a particular order. While current PostgreSQL versions appear to
        handle the previous approach correctly, the database does not guarantee
        this behavior for future versions.

    .. change::
        :tags: usecase, postgresql
        :tickets: 6511

        Added support for reflection of collation in types for PostgreSQL.
        The ``collation`` will be set only if different from the default
        one for the type.
        Pull request courtesy Denis Laxalde.

    .. change::
        :tags: bug, examples

        Fixed the "short_selects" performance example where the cache was being
        used in all the examples, making it impossible to compare performance with
        and without the cache.   Less important comparisons like "lambdas" and
        "baked queries" have been removed.


    .. change::
        :tags: change, tests

        A noxfile.py has been added to allow testing with nox.  This is a direct
        port of 2.1's move to nox, however leaves the tox.ini file in place and
        retains all test documentation in terms of tox.   Version 2.1 will move to
        nox fully, including deprecation warnings for tox and new testing
        documentation.

.. changelog::
    :version: 2.0.44
    :released: October 10, 2025

    .. change::
        :tags: bug, sql
        :tickets: 12271

        Improved the implementation of :meth:`.UpdateBase.returning` to use more
        robust logic in setting up the ``.c`` collection of a derived statement
        such as a CTE.  This fixes issues related to RETURNING clauses that feature
        expressions based on returned columns with or without qualifying labels.

    .. change::
        :tags: usecase, asyncio
        :tickets: 12273

        Generalize the terminate logic employed by the asyncpg dialect to reuse
        it in the aiomysql and asyncmy dialect implementation.

    .. change::
        :tags: bug, mssql
        :tickets: 12798

        Improved the base implementation of the asyncio cursor such that it
        includes the option for the underlying driver's cursor to be actively
        closed in those cases where it requires ``await`` in order to complete the
        close sequence, rather than relying on garbage collection to "close" it,
        when a plain :class:`.Result` is returned that does not use ``await`` for
        any of its methods.  The previous approach of relying on gc was fine for
        MySQL and SQLite dialects but has caused problems with the aioodbc
        implementation on top of SQL Server.   The new option is enabled
        for those dialects which have an "awaitable" ``cursor.close()``, which
        includes the aioodbc, aiomysql, and asyncmy dialects (aiosqlite is also
        modified for 2.1 only).

    .. change::
        :tags: bug, ext
        :tickets: 12802

        Fixed issue caused by an unwanted functional change while typing
        the :class:`.MutableList` class.
        This change also reverts all other functional changes done in
        the same change.

    .. change::
        :tags: bug, typing
        :tickets: 12813

        Fixed typing bug where the :meth:`.Session.execute` method advertised that
        it would return a :class:`.CursorResult` if given an insert/update/delete
        statement.  This is not the general case as several flavors of ORM
        insert/update do not actually yield a :class:`.CursorResult` which cannot
        be differentiated at the typing overload level, so the method now yields
        :class:`.Result` in all cases.  For those cases where
        :class:`.CursorResult` is known to be returned and the ``.rowcount``
        attribute is required, please use ``typing.cast()``.

    .. change::
        :tags: usecase, orm
        :tickets: 12829

        The way ORM Annotated Declarative interprets Python :pep:`695` type aliases
        in ``Mapped[]`` annotations has been refined to expand the lookup scheme. A
        :pep:`695` type can now be resolved based on either its direct presence in
        :paramref:`_orm.registry.type_annotation_map` or its immediate resolved
        value, as long as a recursive lookup across multiple :pep:`695` types is
        not required for it to resolve. This change reverses part of the
        restrictions introduced in 2.0.37 as part of :ticket:`11955`, which
        deprecated (and disallowed in 2.1) the ability to resolve any :pep:`695`
        type that was not explicitly present in
        :paramref:`_orm.registry.type_annotation_map`. Recursive lookups of
        :pep:`695` types remains deprecated in 2.0 and disallowed in version 2.1,
        as do implicit lookups of ``NewType`` types without an entry in
        :paramref:`_orm.registry.type_annotation_map`.

        Additionally, new support has been added for generic :pep:`695` aliases that
        refer to :pep:`593` ``Annotated`` constructs containing
        :func:`_orm.mapped_column` configurations. See the sections below for
        examples.

        .. seealso::

            :ref:`orm_declarative_type_map_pep695_types`

            :ref:`orm_declarative_mapped_column_generic_pep593`

    .. change::
        :tags: bug, postgresql
        :tickets: 12847

        Fixed issue where selecting an enum array column containing NULL values
        would fail to parse properly in the PostgreSQL dialect. The
        :func:`._split_enum_values` function now correctly handles NULL entries by
        converting them to Python ``None`` values.

    .. change::
        :tags: bug, typing
        :tickets: 12855

        Added new decorator :func:`_orm.mapped_as_dataclass`, which is a function
        based form of :meth:`_orm.registry.mapped_as_dataclass`; the method form
        :meth:`_orm.registry.mapped_as_dataclass` does not seem to be correctly
        recognized within the scope of :pep:`681` in recent mypy versions.

    .. change::
        :tags: bug, sqlite
        :tickets: 12864

        Fixed issue where SQLite table reflection would fail for tables using
        ``WITHOUT ROWID`` and/or ``STRICT`` table options when the table contained
        generated columns. The regular expression used to parse ``CREATE TABLE``
        statements for generated column detection has been updated to properly
        handle these SQLite table options that appear after the column definitions.
        Pull request courtesy Tip ten Brink.

    .. change::
        :tags: bug, postgresql
        :tickets: 12874

        Fixed issue where the :func:`_sql.any_` and :func:`_sql.all_` aggregation
        operators would not correctly coerce the datatype of the compared value, in
        those cases where the compared value were not a simple int/str etc., such
        as a Python ``Enum`` or other custom value.   This would lead to execution
        time errors for these values.  This issue is essentially the same as
        :ticket:`6515` which was for the now-legacy :meth:`.ARRAY.any` and
        :meth:`.ARRAY.all` methods.

    .. change::
        :tags: bug, engine
        :tickets: 12881

        Implemented initial support for free-threaded Python by adding new tests
        and reworking the test harness to include Python 3.13t and Python 3.14t in
        test runs. Two concurrency issues have been identified and fixed: the first
        involves initialization of the ``.c`` collection on a ``FromClause``, a
        continuation of :ticket:`12302`, where an optional mutex under
        free-threading is added; the second involves synchronization of the pool
        "first_connect" event, which first received thread synchronization in
        :ticket:`2964`, however under free-threading the creation of the mutex
        itself runs under the same free-threading mutex. Support for free-threaded
        wheels on Pypi is implemented as well within the 2.1 series only.  Initial
        pull request and test suite courtesy Lysandros Nikolaou.

    .. change::
        :tags: bug, schema
        :tickets: 12884

        Fixed issue where :meth:`_schema.MetaData.reflect` did not forward
        dialect-specific keyword arguments to the :class:`_engine.Inspector`
        methods, causing options like ``oracle_resolve_synonyms`` to be ignored
        during reflection. The method now ensures that all extra kwargs passed to
        :meth:`_schema.MetaData.reflect` are forwarded to
        :meth:`_engine.Inspector.get_table_names` and related reflection methods.
        Pull request courtesy Lukáš Kožušník.

    .. change::
        :tags: bug, mssql
        :tickets: 12894

        Fixed issue where the index reflection for SQL Server would
        not correctly return the order of the column inside an index
        when the order of the columns in the index did not match the
        order of the columns in the table.
        Pull request courtesy of Allen Chen.

    .. change::
        :tags: bug, orm
        :tickets: 12905

        Fixed a caching issue where :func:`_orm.with_loader_criteria` would
        incorrectly reuse cached bound parameter values when used with
        :class:`_sql.CompoundSelect` constructs such as :func:`_sql.union`. The
        issue was caused by the cache key for compound selects not including the
        execution options that are part of the :class:`_sql.Executable` base class,
        which :func:`_orm.with_loader_criteria` uses to apply its criteria
        dynamically. The fix ensures that compound selects and other executable
        constructs properly include execution options in their cache key traversal.

    .. change::
        :tags: bug, mssql, reflection
        :tickets: 12907

        Fixed issue in the MSSQL dialect's foreign key reflection query where
        duplicate rows could be returned when a foreign key column and its
        referenced primary key column have the same name, and both the referencing
        and referenced tables have indexes with the same name. This resulted in an
        "ForeignKeyConstraint with duplicate source column references are not
        supported" error when attempting to reflect such tables. The query has been
        corrected to exclude indexes on the child table when looking for unique
        indexes referenced by foreign keys.

    .. change::
        :tags: bug, platform

        Unblocked automatic greenlet installation for Python 3.14 now that
        there are greenlet wheels on pypi for python 3.14.

.. changelog::
    :version: 2.0.43
    :released: August 11, 2025

    .. change::
        :tags: usecase, oracle
        :tickets: 12711

        Extended :class:`_oracle.VECTOR` to support sparse vectors. This update
        introduces :class:`_oracle.VectorStorageType` to specify sparse or dense
        storage and added :class:`_oracle.SparseVector`. Pull request courtesy
        Suraj Shaw.

    .. change::
        :tags: bug, orm
        :tickets: 12748

        Fixed issue where using the ``post_update`` feature would apply incorrect
        "pre-fetched" values to the ORM objects after a multi-row UPDATE process
        completed.  These "pre-fetched" values would come from any column that had
        an :paramref:`.Column.onupdate` callable or a version id generator used by
        :paramref:`.orm.Mapper.version_id_generator`; for a version id generator
        that delivered random identifiers like timestamps or UUIDs, this incorrect
        data would lead to a DELETE statement against those same rows to fail in
        the next step.


    .. change::
        :tags: bug, postgresql
        :tickets: 12778

        Fixed regression in PostgreSQL dialect where JSONB subscription syntax
        would generate incorrect SQL for JSONB-returning functions, causing syntax
        errors. The dialect now properly wraps function calls and expressions in
        parentheses when using the ``[]`` subscription syntax, generating
        ``(function_call)[index]`` instead of ``function_call[index]`` to comply
        with PostgreSQL syntax requirements.

    .. change::
        :tags: usecase, engine
        :tickets: 12784

        Added new parameter :paramref:`.create_engine.skip_autocommit_rollback`
        which provides for a per-dialect feature of preventing the DBAPI
        ``.rollback()`` from being called under any circumstances, if the
        connection is detected as being in "autocommit" mode.   This improves upon
        a critical performance issue identified in MySQL dialects where the network
        overhead of the ``.rollback()`` call remains prohibitive even if autocommit
        mode is set.

        .. seealso::

            :ref:`dbapi_autocommit_skip_rollback`

    .. change::
        :tags: bug, orm
        :tickets: 12787

        Fixed issue where :paramref:`_orm.mapped_column.use_existing_column`
        parameter in :func:`_orm.mapped_column` would not work when the
        :func:`_orm.mapped_column` is used inside of an ``Annotated`` type alias in
        polymorphic inheritance scenarios. The parameter is now properly recognized
        and processed during declarative mapping configuration.

    .. change::
        :tags: bug, orm
        :tickets: 12790

        Improved the implementation of the :func:`_orm.selectin_polymorphic`
        inheritance loader strategy to properly render the IN expressions using
        chunks of 500 records each, in the same manner as that of the
        :func:`_orm.selectinload` relationship loader strategy.  Previously, the IN
        expression would be arbitrarily large, leading to failures on databases
        that have limits on the size of IN expressions including Oracle Database.

.. changelog::
    :version: 2.0.42
    :released: July 29, 2025

    .. change::
        :tags: usecase, orm
        :tickets: 10674

        Added ``dataclass_metadata`` argument to all ORM attribute constructors
        that accept dataclasses parameters, e.g. :paramref:`.mapped_column.dataclass_metadata`,
        :paramref:`.relationship.dataclass_metadata`, etc.
        It's passed to the underlying dataclass ``metadata`` attribute
        of the dataclass field. Pull request courtesy Sigmund Lahn.

    .. change::
        :tags: usecase, postgresql
        :tickets: 10927

        Added support for PostgreSQL 14+ :class:`.JSONB` subscripting syntax.
        When connected to PostgreSQL 14 or later, JSONB columns now
        automatically use the native subscript notation ``jsonb_col['key']``
        instead of the arrow operator ``jsonb_col -> 'key'`` for both read and
        write operations. This provides better compatibility with PostgreSQL's
        native JSONB subscripting feature while maintaining backward
        compatibility with older PostgreSQL versions. JSON columns continue to
        use the traditional arrow syntax regardless of PostgreSQL version.

        .. warning::

          **For applications that have indexes against JSONB subscript
          expressions**

          This change caused an unintended side effect for indexes that were
          created against expressions that use subscript notation, e.g.
          ``Index("ix_entity_json_ab_text", data["a"]["b"].astext)``. If these
          indexes were generated with the older syntax e.g. ``((entity.data ->
          'a') ->> 'b')``, they will not be used by the PostgreSQL query
          planner when a query is made using SQLAlchemy 2.0.42 or higher on
          PostgreSQL versions 14 or higher. This occurs because the new text
          will resemble ``(entity.data['a'] ->> 'b')`` which will fail to
          produce the exact textual syntax match required by the PostgreSQL
          query planner.  Therefore, for users upgrading to SQLAlchemy 2.0.42
          or higher, existing indexes that were created against :class:`.JSONB`
          expressions that use subscripting would need to be dropped and
          re-created in order for them to work with the new query syntax, e.g.
          an expression like ``((entity.data -> 'a') ->> 'b')`` would become
          ``(entity.data['a'] ->> 'b')``.

          .. seealso::

                :ticket:`12868` - discussion of this issue

    .. change::
        :tags: bug, orm
        :tickets: 12593

        Implemented the :func:`_orm.defer`, :func:`_orm.undefer` and
        :func:`_orm.load_only` loader options to work for composite attributes, a
        use case that had never been supported previously.

    .. change::
        :tags: bug, postgresql, reflection
        :tickets: 12600

        Fixed regression caused by :ticket:`10665` where the newly modified
        constraint reflection query would fail on older versions of PostgreSQL
        such as version 9.6.  Pull request courtesy Denis Laxalde.

    .. change::
        :tags: bug, mysql
        :tickets: 12648

        Fixed yet another regression caused by by the DEFAULT rendering changes in
        2.0.40 :ticket:`12425`, similar to :ticket:`12488`, this time where using a
        CURRENT_TIMESTAMP function with a fractional seconds portion inside a
        textual default value would also fail to be recognized as a
        non-parenthesized server default.



    .. change::
        :tags: bug, mssql
        :tickets: 12654

        Reworked SQL Server column reflection to be based on the ``sys.columns``
        table rather than ``information_schema.columns`` view.  By correctly using
        the SQL Server ``object_id()`` function as a lead and joining to related
        tables on object_id rather than names, this repairs a variety of issues in
        SQL Server reflection, including:

        * Issue where reflected column comments would not correctly line up
          with the columns themselves in the case that the table had been ALTERed
        * Correctly targets tables with awkward names such as names with brackets,
          when reflecting not just the basic table / columns but also extended
          information including IDENTITY, computed columns, comments which
          did not work previously
        * Correctly targets IDENTITY, computed status from temporary tables
          which did not work previously

    .. change::
        :tags: bug, sql
        :tickets: 12681

        Fixed issue where :func:`.select` of a free-standing scalar expression that
        has a unary operator applied, such as negation, would not apply result
        processors to the selected column even though the correct type remains in
        place for the unary expression.


    .. change::
        :tags: bug, sql
        :tickets: 12692

        Hardening of the compiler's actions for UPDATE statements that access
        multiple tables to report more specifically when tables or aliases are
        referenced in the SET clause; on cases where the backend does not support
        secondary tables in the SET clause, an explicit error is raised, and on the
        MySQL or similar backends that support such a SET clause, more specific
        checking for not-properly-included tables is performed.  Overall the change
        is preventing these erroneous forms of UPDATE statements from being
        compiled, whereas previously it was relied on the database to raise an
        error, which was not always guaranteed to happen, or to be non-ambiguous,
        due to cases where the parent table included the same column name as the
        secondary table column being updated.


    .. change::
        :tags: bug, orm
        :tickets: 12692

        Fixed bug where the ORM would pull in the wrong column into an UPDATE when
        a key name inside of the :meth:`.ValuesBase.values` method could be located
        from an ORM entity mentioned in the statement, but where that ORM entity
        was not the actual table that the statement was inserting or updating.  An
        extra check for this edge case is added to avoid this problem.

    .. change::
        :tags: bug, postgresql
        :tickets: 12728

        Re-raise catched ``CancelledError`` in the terminate method of the
        asyncpg dialect to avoid possible hangs of the code execution.


    .. change::
        :tags: usecase, sql
        :tickets: 12734

        The :func:`_sql.values` construct gains a new method :meth:`_sql.Values.cte`,
        which allows creation of a named, explicit-columns :class:`.CTE` against an
        unnamed ``VALUES`` expression, producing a syntax that allows column-oriented
        selection from a ``VALUES`` construct on modern versions of PostgreSQL, SQLite,
        and MariaDB.

    .. change::
        :tags: bug, reflection, postgresql
        :tickets: 12744

        Fixes bug that would mistakenly interpret a domain or enum type
        with name starting in ``interval`` as an ``INTERVAL`` type while
        reflecting a table.

    .. change::
        :tags: usecase, postgresql
        :tickets: 8664

        Added ``postgresql_ops`` key to the ``dialect_options`` entry in reflected
        dictionary. This maps names of columns used in the index to respective
        operator class, if distinct from the default one for column's data type.
        Pull request courtesy Denis Laxalde.

        .. seealso::

            :ref:`postgresql_operator_classes`

    .. change::
        :tags: engine

        Improved validation of execution parameters passed to the
        :meth:`_engine.Connection.execute` and similar methods to
        provided a better error when tuples are passed in.
        Previously the execution would fail with a difficult to
        understand error message.

.. changelog::
    :version: 2.0.41
    :released: May 14, 2025

    .. change::
        :tags: usecase, postgresql
        :tickets: 10665

        Added support for ``postgresql_include`` keyword argument to
        :class:`_schema.UniqueConstraint` and :class:`_schema.PrimaryKeyConstraint`.
        Pull request courtesy Denis Laxalde.

        .. seealso::

            :ref:`postgresql_constraint_options`

    .. change::
        :tags: usecase, oracle
        :tickets: 12317, 12341

        Added new datatype :class:`_oracle.VECTOR` and accompanying DDL and DQL
        support to fully support this type for Oracle Database. This change
        includes the base :class:`_oracle.VECTOR` type that adds new type-specific
        methods ``l2_distance``, ``cosine_distance``, ``inner_product`` as well as
        new parameters ``oracle_vector`` for the :class:`.Index` construct,
        allowing vector indexes to be configured, and ``oracle_fetch_approximate``
        for the :meth:`.Select.fetch` clause.  Pull request courtesy Suraj Shaw.

        .. seealso::

            :ref:`oracle_vector_datatype`


    .. change::
        :tags: bug, platform
        :tickets: 12405

        Adjusted the test suite as well as the ORM's method of scanning classes for
        annotations to work under current beta releases of Python 3.14 (currently
        3.14.0b1) as part of an ongoing effort to support the production release of
        this Python release.  Further changes to Python's means of working with
        annotations is expected in subsequent beta releases for which SQLAlchemy's
        test suite will need further adjustments.



    .. change::
        :tags: bug, mysql
        :tickets: 12488

        Fixed regression caused by the DEFAULT rendering changes in version 2.0.40
        via :ticket:`12425` where using lowercase ``on update`` in a MySQL server
        default would incorrectly apply parenthesis, leading to errors when MySQL
        interpreted the rendered DDL.  Pull request courtesy Alexander Ruehe.

    .. change::
        :tags: bug, sqlite
        :tickets: 12566

        Fixed and added test support for some SQLite SQL functions hardcoded into
        the compiler, most notably the ``localtimestamp`` function which rendered
        with incorrect internal quoting.

    .. change::
        :tags: bug, engine
        :tickets: 12579

        The error message that is emitted when a URL cannot be parsed no longer
        includes the URL itself within the error message.


    .. change::
        :tags: bug, typing
        :tickets: 12588

        Removed ``__getattr__()`` rule from ``sqlalchemy/__init__.py`` that
        appeared to be trying to correct for a previous typographical error in the
        imports. This rule interferes with type checking and is removed.


    .. change::
        :tags: bug, installation

        Removed the "license classifier" from setup.cfg for SQLAlchemy 2.0, which
        eliminates loud deprecation warnings when building the package.  SQLAlchemy
        2.1 will use a full :pep:`639` configuration in pyproject.toml while
        SQLAlchemy 2.0 remains using ``setup.cfg`` for setup.



.. changelog::
    :version: 2.0.40
    :released: March 27, 2025

    .. change::
        :tags: usecase, postgresql
        :tickets: 11595

        Added support for specifying a list of columns for ``SET NULL`` and ``SET
        DEFAULT`` actions of ``ON DELETE`` clause of foreign key definition on
        PostgreSQL.  Pull request courtesy Denis Laxalde.

        .. seealso::

            :ref:`postgresql_constraint_options`

    .. change::
        :tags: bug, orm
        :tickets: 12329

        Fixed regression which occurred as of 2.0.37 where the checked
        :class:`.ArgumentError` that's raised when an inappropriate type or object
        is used inside of a :class:`.Mapped` annotation would raise ``TypeError``
        with "boolean value of this clause is not defined" if the object resolved
        into a SQL expression in a boolean context, for programs where future
        annotations mode was not enabled.  This case is now handled explicitly and
        a new error message has also been tailored for this case.  In addition, as
        there are at least half a dozen distinct error scenarios for intepretation
        of the :class:`.Mapped` construct, these scenarios have all been unified
        under a new subclass of :class:`.ArgumentError` called
        :class:`.MappedAnnotationError`, to provide some continuity between these
        different scenarios, even though specific messaging remains distinct.

    .. change::
        :tags: bug, mysql
        :tickets: 12332

        Support has been re-added for the MySQL-Connector/Python DBAPI using the
        ``mysql+mysqlconnector://`` URL scheme.   The DBAPI now works against
        modern MySQL versions as well as MariaDB versions (in the latter case it's
        required to pass charset/collation explicitly).   Note however that
        server side cursor support is disabled due to unresolved issues with this
        driver.

    .. change::
        :tags: bug, sql
        :tickets: 12363

        Fixed issue in :class:`.CTE` constructs involving multiple DDL
        :class:`_sql.Insert` statements with multiple VALUES parameter sets where the
        bound parameter names generated for these parameter sets would conflict,
        generating a compile time error.


    .. change::
        :tags: bug, sqlite
        :tickets: 12425

        Expanded the rules for when to apply parenthesis to a server default in DDL
        to suit the general case of a default string that contains non-word
        characters such as spaces or operators and is not a string literal.

    .. change::
        :tags: bug, mysql
        :tickets: 12425

        Fixed issue in MySQL server default reflection where a default that has
        spaces would not be correctly reflected.  Additionally, expanded the rules
        for when to apply parenthesis to a server default in DDL to suit the
        general case of a default string that contains non-word characters such as
        spaces or operators and is not a string literal.


    .. change::
        :tags: usecase, postgresql
        :tickets: 12432

        When building a PostgreSQL ``ARRAY`` literal using
        :class:`_postgresql.array` with an empty ``clauses`` argument, the
        :paramref:`_postgresql.array.type_` parameter is now significant in that it
        will be used to render the resulting ``ARRAY[]`` SQL expression with a
        cast, such as ``ARRAY[]::INTEGER``. Pull request courtesy Denis Laxalde.

    .. change::
        :tags: sql, usecase
        :tickets: 12450

        Implemented support for the GROUPS frame specification in window functions
        by adding :paramref:`_sql.over.groups` option to :func:`_sql.over`
        and :meth:`.FunctionElement.over`. Pull request courtesy Kaan Dikmen.

    .. change::
        :tags: bug, sql
        :tickets: 12451

        Fixed regression caused by :ticket:`7471` leading to a SQL compilation
        issue where name disambiguation for two same-named FROM clauses with table
        aliasing in use at the same time would produce invalid SQL in the FROM
        clause with two "AS" clauses for the aliased table, due to double aliasing.

    .. change::
        :tags: bug, asyncio
        :tickets: 12471

        Fixed issue where :meth:`.AsyncSession.get_transaction` and
        :meth:`.AsyncSession.get_nested_transaction` would fail with
        ``NotImplementedError`` if the "proxy transaction" used by
        :class:`.AsyncSession` were garbage collected and needed regeneration.

    .. change::
        :tags: bug, orm
        :tickets: 12473

        Fixed regression in ORM Annotated Declarative class interpretation caused
        by ``typing_extension==4.13.0`` that introduced a different implementation
        for ``TypeAliasType`` while SQLAlchemy assumed that it would be equivalent
        to the ``typing`` version, leading to pep-695 type annotations not
        resolving to SQL types as expected.

.. changelog::
    :version: 2.0.39
    :released: March 11, 2025

    .. change::
        :tags: bug, postgresql
        :tickets: 11751

        Add SQL typing to reflection query used to retrieve a the structure
        of IDENTITY columns, adding explicit JSON typing to the query to suit
        unusual PostgreSQL driver configurations that don't support JSON natively.

    .. change::
        :tags: bug, postgresql

        Fixed issue affecting PostgreSQL 17.3 and greater where reflection of
        domains with "NOT NULL" as part of their definition would include an
        invalid constraint entry in the data returned by
        :meth:`_postgresql.PGInspector.get_domains` corresponding to an additional
        "NOT NULL" constraint that isn't a CHECK constraint; the existing
        ``"nullable"`` entry in the dictionary already indicates if the domain
        includes a "not null" constraint.   Note that such domains also cannot be
        reflected on PostgreSQL 17.0 through 17.2 due to a bug on the PostgreSQL
        side; if encountering errors in reflection of domains which include NOT
        NULL, upgrade to PostgreSQL server 17.3 or greater.

    .. change::
        :tags: typing, usecase
        :tickets: 11922

        Support generic types for compound selects (:func:`_sql.union`,
        :func:`_sql.union_all`, :meth:`_sql.Select.union`,
        :meth:`_sql.Select.union_all`, etc) returning the type of the first select.
        Pull request courtesy of Mingyu Park.

    .. change::
        :tags: bug, postgresql
        :tickets: 12060

        Fixed issue in PostgreSQL network types :class:`_postgresql.INET`,
        :class:`_postgresql.CIDR`, :class:`_postgresql.MACADDR`,
        :class:`_postgresql.MACADDR8` where sending string values to compare to
        these types would render an explicit CAST to VARCHAR, causing some SQL /
        driver combinations to fail.  Pull request courtesy Denis Laxalde.

    .. change::
        :tags: bug, orm
        :tickets: 12326

        Fixed bug where using DML returning such as :meth:`.Insert.returning` with
        an ORM model that has :func:`_orm.column_property` constructs that contain
        subqueries would fail with an internal error.

    .. change::
        :tags: bug, orm
        :tickets: 12328

        Fixed bug in ORM enabled UPDATE (and theoretically DELETE) where using a
        multi-table DML statement would not allow ORM mapped columns from mappers
        other than the primary UPDATE mapper to be named in the RETURNING clause;
        they would be omitted instead and cause a column not found exception.

    .. change::
        :tags: bug, asyncio
        :tickets: 12338

        Fixed bug where :meth:`_asyncio.AsyncResult.scalar`,
        :meth:`_asyncio.AsyncResult.scalar_one_or_none`, and
        :meth:`_asyncio.AsyncResult.scalar_one` would raise an ``AttributeError``
        due to a missing internal attribute.  Pull request courtesy Allen Ho.

    .. change::
        :tags: bug, orm
        :tickets: 12357

        Fixed issue where the "is ORM" flag of a :func:`.select` or other ORM
        statement would not be propagated to the ORM :class:`.Session` based on a
        multi-part operator expression alone, e.g. such as ``Cls.attr + Cls.attr +
        Cls.attr`` or similar, leading to ORM behaviors not taking place for such
        statements.

    .. change::
        :tags: bug, orm
        :tickets: 12364

        Fixed issue where using :func:`_orm.aliased` around a :class:`.CTE`
        construct could cause inappropriate "duplicate CTE" errors in cases where
        that aliased construct appeared multiple times in a single statement.

    .. change::
        :tags: bug, sqlite
        :tickets: 12368

        Fixed issue that omitted the comma between multiple SQLite table extension
        clauses, currently ``WITH ROWID`` and ``STRICT``, when both options
        :paramref:`.Table.sqlite_with_rowid` and  :paramref:`.Table.sqlite_strict`
        were configured at their non-default settings at the same time.  Pull
        request courtesy david-fed.

    .. change::
        :tags: bug, sql
        :tickets: 12382

        Added new parameters :paramref:`.AddConstraint.isolate_from_table` and
        :paramref:`.DropConstraint.isolate_from_table`, defaulting to True, which
        both document and allow to be controllable the long-standing behavior of
        these two constructs blocking the given constraint from being included
        inline within the "CREATE TABLE" sequence, under the assumption that
        separate add/drop directives were to be used.

    .. change::
        :tags: bug, postgresql
        :tickets: 12417

        Fixed compiler issue in the PostgreSQL dialect where incorrect keywords
        would be passed when using "FOR UPDATE OF" inside of a subquery.

.. changelog::
    :version: 2.0.38
    :released: February 6, 2025

    .. change::
        :tags: postgresql, usecase, asyncio
        :tickets: 12077

        Added an additional ``asyncio.shield()`` call within the connection
        terminate process of the asyncpg driver, to mitigate an issue where
        terminate would be prevented from completing under the anyio concurrency
        library.

    .. change::
        :tags: bug, dml, mariadb, mysql
        :tickets: 12117

        Fixed a bug where the MySQL statement compiler would not properly compile
        statements where :meth:`_mysql.Insert.on_duplicate_key_update` was passed
        values that included ORM-mapped attributes (e.g.
        :class:`InstrumentedAttribute` objects) as keys. Pull request courtesy of
        mingyu.

    .. change::
        :tags: bug, postgresql
        :tickets: 12159

        Adjusted the asyncpg connection wrapper so that the
        ``connection.transaction()`` call sent to asyncpg sends ``None`` for
        ``isolation_level`` if not otherwise set in the SQLAlchemy dialect/wrapper,
        thereby allowing asyncpg to make use of the server level setting for
        ``isolation_level`` in the absense of a client-level setting. Previously,
        this behavior of asyncpg was blocked by a hardcoded ``read_committed``.

    .. change::
        :tags: bug, sqlite, aiosqlite, asyncio, pool
        :tickets: 12285

        Changed default connection pool used by the ``aiosqlite`` dialect
        from :class:`.NullPool` to :class:`.AsyncAdaptedQueuePool`; this change
        should have been made when 2.0 was first released as the ``pysqlite``
        dialect was similarly changed to use :class:`.QueuePool` as detailed
        in :ref:`change_7490`.


    .. change::
        :tags: bug, engine
        :tickets: 12289

        Fixed event-related issue where invoking :meth:`.Engine.execution_options`
        on a :class:`.Engine` multiple times while making use of event-registering
        parameters such as ``isolation_level`` would lead to internal errors
        involving event registration.

    .. change::
        :tags: bug, sql
        :tickets: 12302

        Reorganized the internals by which the ``.c`` collection on a
        :class:`.FromClause` gets generated so that it is resilient against the
        collection being accessed in concurrent fashion.   An example is creating a
        :class:`.Alias` or :class:`.Subquery` and accessing it as a module level
        variable.  This impacts the Oracle dialect which uses such module-level
        global alias objects but is of general use as well.

    .. change::
        :tags: bug, sql
        :tickets: 12314

        Fixed SQL composition bug which impacted caching where using a ``None``
        value inside of an ``in_()`` expression would bypass the usual "expanded
        bind parameter" logic used by the IN construct, which allows proper caching
        to take place.


.. changelog::
    :version: 2.0.37
    :released: January 9, 2025

    .. change::
        :tags: usecase, mariadb
        :tickets: 10720

        Added sql types ``INET4`` and ``INET6`` in the MariaDB dialect.  Pull
        request courtesy Adam Žurek.

    .. change::
        :tags: bug, orm
        :tickets: 11370

        Fixed issue regarding ``Union`` types that would be present in the
        :paramref:`_orm.registry.type_annotation_map` of a :class:`_orm.registry`
        or declarative base class, where a :class:`.Mapped` element that included
        one of the subtypes present in that ``Union`` would be matched to that
        entry, potentially ignoring other entries that matched exactly.   The
        correct behavior now takes place such that an entry should only match in
        :paramref:`_orm.registry.type_annotation_map` exactly, as a ``Union`` type
        is a self-contained type. For example, an attribute with ``Mapped[float]``
        would previously match to a :paramref:`_orm.registry.type_annotation_map`
        entry ``Union[float, Decimal]``; this will no longer match and will now
        only match to an entry that states ``float``. Pull request courtesy Frazer
        McLean.

    .. change::
        :tags: bug, postgresql
        :tickets: 11724

        Fixes issue in :meth:`.Dialect.get_multi_indexes` in the PostgreSQL
        dialect, where an error would be thrown when attempting to use alembic with
        a vector index from the pgvecto.rs extension.

    .. change::
        :tags:  usecase, mysql, mariadb
        :tickets: 11764

        Added support for the ``LIMIT`` clause with ``DELETE`` for the MySQL and
        MariaDB dialects, to complement the already present option for
        ``UPDATE``. The :meth:`.Delete.with_dialect_options` method of the
        :func:`.delete` construct accepts parameters for ``mysql_limit`` and
        ``mariadb_limit``, allowing users to specify a limit on the number of rows
        deleted. Pull request courtesy of Pablo Nicolás Estevez.


    .. change::
        :tags:  bug, mysql, mariadb

        Added logic to ensure that the ``mysql_limit`` and ``mariadb_limit``
        parameters of :meth:`.Update.with_dialect_options` and
        :meth:`.Delete.with_dialect_options` when compiled to string will only
        compile if the parameter is passed as an integer; a ``ValueError`` is
        raised otherwise.

    .. change::
        :tags: bug, orm
        :tickets: 11944

        Fixed bug in how type unions were handled within
        :paramref:`_orm.registry.type_annotation_map` as well as
        :class:`._orm.Mapped` that made the lookup behavior of ``a | b`` different
        from that of ``Union[a, b]``.

    .. change::
        :tags: bug, orm
        :tickets: 11955

        .. note:: this change has been revised in version 2.0.44.  Simple matches
           of ``TypeAliasType`` without a type map entry are no longer deprecated.

        Consistently handle ``TypeAliasType`` (defined in PEP 695) obtained with
        the ``type X = int`` syntax introduced in python 3.12. Now in all cases one
        such alias must be explicitly added to the type map for it to be usable
        inside :class:`.Mapped`. This change also revises the approach added in
        :ticket:`11305`, now requiring the ``TypeAliasType`` to be added to the
        type map. Documentation on how unions and type alias types are handled by
        SQLAlchemy has been added in the
        :ref:`orm_declarative_mapped_column_type_map` section of the documentation.

    .. change::
        :tags: feature, oracle
        :tickets: 12016

        Added new table option ``oracle_tablespace`` to specify the ``TABLESPACE``
        option when creating a table in Oracle. This allows users to define the
        tablespace in which the table should be created. Pull request courtesy of
        Miguel Grillo.

    .. change::
        :tags: orm, bug
        :tickets: 12019

        Fixed regression caused by an internal code change in response to recent
        Mypy releases that caused the very unusual case of a list of ORM-mapped
        attribute expressions passed to :meth:`.ColumnOperators.in_` to no longer
        be accepted.

    .. change::
        :tags: oracle, usecase
        :tickets: 12032

        Use the connection attribute ``max_identifier_length`` available
        in oracledb since version 2.5 when determining the identifier length
        in the Oracle dialect.

    .. change::
        :tags: bug, sql
        :tickets: 12084

        Fixed issue in "lambda SQL" feature where the tracking of bound parameters
        could be corrupted if the same lambda were evaluated across multiple
        compile phases, including when using the same lambda across multiple engine
        instances or with statement caching disabled.


    .. change::
        :tags: usecase, postgresql
        :tickets: 12093

        The :class:`_postgresql.Range` type now supports
        :meth:`_postgresql.Range.__contains__`. Pull request courtesy of Frazer
        McLean.

    .. change::
        :tags: bug, oracle
        :tickets: 12100

        Fixed compilation of ``TABLE`` function when used in a ``FROM`` clause in
        Oracle Database dialect.

    .. change::
        :tags: bug, oracle
        :tickets: 12150

        Fixed issue in oracledb / cx_oracle dialects where output type handlers for
        ``CLOB`` were being routed to ``NVARCHAR`` rather than ``VARCHAR``, causing
        a double conversion to take place.


    .. change::
        :tags: bug, postgresql
        :tickets: 12170

        Fixed issue where creating a table with a primary column of
        :class:`_sql.SmallInteger` and using the asyncpg driver would result in
        the type being compiled to ``SERIAL`` rather than ``SMALLSERIAL``.

    .. change::
        :tags: bug, orm
        :tickets: 12207

        Fixed issues in type handling within the
        :paramref:`_orm.registry.type_annotation_map` feature which prevented the
        use of unions, using either pep-604 or ``Union`` syntaxes under future
        annotations mode, which contained multiple generic types as elements from
        being correctly resolvable.

    .. change::
        :tags: bug, orm
        :tickets: 12216

        Fixed issue in event system which prevented an event listener from being
        attached and detached from multiple class-like objects, namely the
        :class:`.sessionmaker` or :class:`.scoped_session` targets that assign to
        :class:`.Session` subclasses.


    .. change::
        :tags: bug, postgresql
        :tickets: 12220

        Adjusted the asyncpg dialect so that an empty SQL string, which is valid
        for PostgreSQL server, may be successfully processed at the dialect level,
        such as when using :meth:`.Connection.exec_driver_sql`. Pull request
        courtesy Andrew Jackson.


    .. change::
        :tags: usecase, sqlite
        :tickets: 7398

        Added SQLite table option to enable ``STRICT`` tables. Pull request
        courtesy of Guilherme Crocetti.

.. changelog::
    :version: 2.0.36
    :released: October 15, 2024

    .. change::
        :tags: bug, schema
        :tickets: 11317

        Fixed bug where SQL functions passed to
        :paramref:`_schema.Column.server_default` would not be rendered with the
        particular form of parenthesization now required by newer versions of MySQL
        and MariaDB. Pull request courtesy of huuya.

    .. change::
        :tags: bug, orm
        :tickets: 11912

        Fixed bug in ORM bulk update/delete where using RETURNING with bulk
        update/delete in combination with ``populate_existing`` would fail to
        accommodate the ``populate_existing`` option.

    .. change::
        :tags: bug, orm
        :tickets: 11917

        Continuing from :ticket:`11912`, columns marked with
        :paramref:`.mapped_column.onupdate`,
        :paramref:`.mapped_column.server_onupdate`, or :class:`.Computed` are now
        refreshed in ORM instances when running an ORM enabled UPDATE with WHERE
        criteria, even if the statement does not use RETURNING or
        ``populate_existing``.

    .. change::
        :tags: usecase, orm
        :tickets: 11923

        Added new parameter :paramref:`_orm.mapped_column.hash` to ORM constructs
        such as :meth:`_orm.mapped_column`, :meth:`_orm.relationship`, etc.,
        which is interpreted for ORM Native Dataclasses in the same way as other
        dataclass-specific field parameters.

    .. change::
        :tags: bug, postgresql, reflection
        :tickets: 11961

        Fixed bug in reflection of table comments where unrelated text would be
        returned if an entry in the ``pg_description`` table happened to share the
        same oid (objoid) as the table being reflected.

    .. change::
        :tags: bug, orm
        :tickets: 11965

        Fixed regression caused by fixes to joined eager loading in :ticket:`11449`
        released in 2.0.31, where a particular joinedload case could not be
        asserted correctly.   We now have an example of that case so the assertion
        has been repaired to allow for it.


    .. change::
        :tags: orm, bug
        :tickets: 11973

        Improved the error message emitted when trying to map as dataclass a class
        while also manually providing the ``__table__`` attribute.
        This usage is currently not supported.

    .. change::
        :tags: mysql, performance
        :tickets: 11975

        Improved a query used for the MySQL 8 backend when reflecting foreign keys
        to be better optimized.   Previously, for a database that had millions of
        columns across all tables, the query could be prohibitively slow; the query
        has been reworked to take better advantage of existing indexes.

    .. change::
        :tags: usecase, sql
        :tickets: 11978

        Datatypes that are binary based such as :class:`.VARBINARY` will resolve to
        :class:`.LargeBinary` when the :meth:`.TypeEngine.as_generic()` method is
        called.

    .. change::
        :tags: postgresql, bug
        :tickets: 11994

        The :class:`.postgresql.JSON` and :class:`.postgresql.JSONB` datatypes will
        now render a "bind cast" in all cases for all PostgreSQL backends,
        including psycopg2, whereas previously it was only enabled for some
        backends.   This allows greater accuracy in allowing the database server to
        recognize when a string value is to be interpreted as JSON.

    .. change::
        :tags: bug, orm
        :tickets: 11995

        Refined the check which the ORM lazy loader uses to detect "this would be
        loading by primary key and the primary key is NULL, skip loading" to take
        into account the current setting for the
        :paramref:`.orm.Mapper.allow_partial_pks` parameter. If this parameter is
        ``False``, then a composite PK value that has partial NULL elements should
        also be skipped.   This can apply to some composite overlapping foreign key
        configurations.


    .. change::
        :tags: bug, orm
        :tickets: 11997

        Fixed bug in ORM "update with WHERE clause" feature where an explicit
        ``.returning()`` would interfere with the "fetch" synchronize strategy due
        to an assumption that the ORM mapped class featured the primary key columns
        in a specific position within the RETURNING.  This has been fixed to use
        appropriate ORM column targeting.

    .. change::
        :tags: bug, sql, regression
        :tickets: 12002

        Fixed regression from 1.4 where some datatypes such as those derived from
        :class:`.TypeDecorator` could not be pickled when they were part of a
        larger SQL expression composition due to internal supporting structures
        themselves not being pickleable.

.. changelog::
    :version: 2.0.35
    :released: September 16, 2024

    .. change::
        :tags: bug, orm, typing
        :tickets: 11820

        Fixed issue where it was not possible to use ``typing.Literal`` with
        ``Mapped[]`` on Python 3.8 and 3.9.  Pull request courtesy Frazer McLean.

    .. change::
        :tags: bug, sqlite, regression
        :tickets: 11840

        The changes made for SQLite CHECK constraint reflection in versions 2.0.33
        and 2.0.34 , :ticket:`11832` and :ticket:`11677`, have now been fully
        reverted, as users continued to identify existing use cases that stopped
        working after this change.   For the moment, because SQLite does not
        provide any consistent way of delivering information about CHECK
        constraints, SQLAlchemy is limited in what CHECK constraint syntaxes can be
        reflected, including that a CHECK constraint must be stated all on a
        single, independent line (or inline on a column definition)  without
        newlines, tabs in the constraint definition or unusual characters in the
        constraint name.  Overall, reflection for SQLite is tailored towards being
        able to reflect CREATE TABLE statements that were originally created by
        SQLAlchemy DDL constructs.  Long term work on a DDL parser that does not
        rely upon regular expressions may eventually improve upon this situation.
        A wide range of additional cross-dialect CHECK constraint reflection tests
        have been added as it was also a bug that these changes did not trip any
        existing tests.

    .. change::
        :tags: orm, bug
        :tickets: 11849

        Fixed issue in ORM evaluator where two datatypes being evaluated with the
        SQL concatenator operator would not be checked for
        :class:`.UnevaluatableError` based on their datatype; this missed the case
        of :class:`_postgresql.JSONB` values being used in a concatenate operation
        which is supported by PostgreSQL as well as how SQLAlchemy renders the SQL
        for this operation, but does not work at the Python level. By implementing
        :class:`.UnevaluatableError` for this combination, ORM update statements
        will now fall back to "expire" when a concatenated JSON value used in a SET
        clause is to be synchronized to a Python object.

    .. change::
        :tags: bug, orm
        :tickets: 11853

        An warning is emitted if :func:`_orm.joinedload` or
        :func:`_orm.subqueryload` are used as a top level option against a
        statement that is not a SELECT statement, such as with an
        ``insert().returning()``.   There are no JOINs in INSERT statements nor is
        there a "subquery" that can be repurposed for subquery eager loading, and
        for UPDATE/DELETE joinedload does not support these either, so it is never
        appropriate for this use to pass silently.

    .. change::
        :tags: bug, orm
        :tickets: 11855

        Fixed issue where using loader options such as :func:`_orm.selectinload`
        with additional criteria in combination with ORM DML such as
        :func:`_sql.insert` with RETURNING would not correctly set up internal
        contexts required for caching to work correctly, leading to incorrect
        results.

    .. change::
        :tags: bug, mysql
        :tickets: 11870

        Fixed issue in mariadbconnector dialect where query string arguments that
        weren't checked integer or boolean arguments would be ignored, such as
        string arguments like ``unix_socket``, etc.  As part of this change, the
        argument parsing for particular elements such as ``client_flags``,
        ``compress``, ``local_infile`` has been made more consistent across all
        MySQL / MariaDB dialect which accept each argument. Pull request courtesy
        Tobias Alex-Petersen.


.. changelog::
    :version: 2.0.34
    :released: September 4, 2024

    .. change::
        :tags: bug, orm
        :tickets: 11831

        Fixed regression caused by issue :ticket:`11814` which broke support for
        certain flavors of :pep:`593` ``Annotated`` in the type_annotation_map when
        builtin types such as ``list``, ``dict`` were used without an element type.
        While this is an incomplete style of typing, these types nonetheless
        previously would be located in the type_annotation_map correctly.

    .. change::
        :tags: bug, sqlite
        :tickets: 11832

        Fixed regression in SQLite reflection caused by :ticket:`11677` which
        interfered with reflection for CHECK constraints that were followed
        by other kinds of constraints within the same table definition.   Pull
        request courtesy Harutaka Kawamura.


.. changelog::
    :version: 2.0.33
    :released: September 3, 2024

    .. change::
        :tags: bug, sqlite
        :tickets: 11677

        Improvements to the regex used by the SQLite dialect to reflect the name
        and contents of a CHECK constraint.  Constraints with newline, tab, or
        space characters in either or both the constraint text and constraint name
        are now properly reflected.   Pull request courtesy Jeff Horemans.



    .. change::
        :tags: bug, engine
        :tickets: 11687

        Fixed issue in internal reflection cache where particular reflection
        scenarios regarding same-named quoted_name() constructs would not be
        correctly cached.  Pull request courtesy Felix Lüdin.

    .. change::
        :tags: bug, sql, regression
        :tickets: 11703

        Fixed regression in :meth:`_sql.Select.with_statement_hint` and others
        where the generative behavior of the method stopped producing a copy of the
        object.

    .. change::
        :tags: bug, mysql
        :tickets: 11731

        Fixed issue in MySQL dialect where using INSERT..FROM SELECT in combination
        with ON DUPLICATE KEY UPDATE would erroneously render on MySQL 8 and above
        the "AS new" clause, leading to syntax failures.  This clause is required
        on MySQL 8 to follow the VALUES clause if use of the "new" alias is
        present, however is not permitted to follow a FROM SELECT clause.


    .. change::
        :tags: bug, sqlite
        :tickets: 11746

        Improvements to the regex used by the SQLite dialect to reflect the name
        and contents of a UNIQUE constraint that is defined inline within a column
        definition inside of a SQLite CREATE TABLE statement, accommodating for tab
        characters present within the column / constraint line. Pull request
        courtesy John A Stevenson.




    .. change::
        :tags: bug, typing
        :tickets: 11782

        Fixed typing issue with :meth:`_sql.Select.with_only_columns`.

    .. change::
        :tags: bug, orm
        :tickets: 11788

        Correctly cleanup the internal top-level module registry when no
        inner modules or classes are registered into it.

    .. change::
        :tags: bug, schema
        :tickets: 11802

        Fixed bug where the ``metadata`` element of an ``Enum`` datatype would not
        be transferred to the new :class:`.MetaData` object when the type had been
        copied via a :meth:`.Table.to_metadata` operation, leading to inconsistent
        behaviors within create/drop sequences.

    .. change::
        :tags: bug, orm
        :tickets: 11814

        Improvements to the ORM annotated declarative type map lookup dealing with
        composed types such as ``dict[str, Any]`` linking to JSON (or others) with
        or without "future annotations" mode.



    .. change::
        :tags: change, general
        :tickets: 11818

        The pin for ``setuptools<69.3`` in ``pyproject.toml`` has been removed.
        This pin was to prevent a sudden change in setuptools to use :pep:`625`
        from taking place, which would change the file name of SQLAlchemy's source
        distribution on pypi to be an all lower case name, which is likely to cause
        problems with various build environments that expected the previous naming
        style.  However, the presence of this pin is holding back environments that
        otherwise want to use a newer setuptools, so we've decided to move forward
        with this change, with the assumption that build environments will have
        largely accommodated the setuptools change by now.



    .. change::
        :tags: bug, postgresql
        :tickets: 11821

        Revising the asyncpg ``terminate()`` fix first made in :ticket:`10717`
        which improved the resiliency of this call under all circumstances, adding
        ``asyncio.CancelledError`` to the list of exceptions that are intercepted
        as failing for a graceful ``.close()`` which will then proceed to call
        ``.terminate()``.

    .. change::
        :tags: bug, mssql
        :tickets: 11822

        Added error "The server failed to resume the transaction" to the list of
        error strings for the pymssql driver in determining a disconnect scenario,
        as observed by one user using pymssql under otherwise unknown conditions as
        leaving an unusable connection in the connection pool which fails to ping
        cleanly.

    .. change::
        :tags: bug, tests

        Added missing ``array_type`` property to the testing suite
        ``SuiteRequirements`` class.

.. changelog::
    :version: 2.0.32
    :released: August 5, 2024

    .. change::
        :tags: bug, examples
        :tickets: 10267

        Fixed issue in history_meta example where the "version" column in the
        versioned table needs to default to the most recent version number in the
        history table on INSERT, to suit the use case of a table where rows are
        deleted, and can then be replaced by new rows that re-use the same primary
        key identity.  This fix adds an additonal SELECT query per INSERT in the
        main table, which may be inefficient; for cases where primary keys are not
        re-used, the default function may be omitted.  Patch courtesy  Philipp H.
        v. Loewenfeld.

    .. change::
        :tags: bug, oracle
        :tickets: 11557

        Fixed table reflection on Oracle 10.2 and older where compression options
        are not supported.

    .. change::
        :tags: oracle, usecase
        :tickets: 10820

        Added API support for server-side cursors for the oracledb async dialect,
        allowing use of the :meth:`_asyncio.AsyncConnection.stream` and similar
        stream methods.

    .. change::
        :tags: bug, orm
        :tickets: 10834

        Fixed issue where using the :meth:`_orm.Query.enable_eagerloads` and
        :meth:`_orm.Query.yield_per` methods at the same time, in order to disable
        eager loading that's configured on the mapper directly, would be silently
        ignored, leading to errors or unexpected eager population of attributes.

    .. change::
        :tags: orm
        :tickets: 11163

        Added a warning noting when an
        :meth:`_engine.ConnectionEvents.engine_connect` event may be leaving
        a transaction open, which can alter the behavior of a
        :class:`_orm.Session` using such an engine as bind.
        On SQLAlchemy 2.1 :paramref:`_orm.Session.join_transaction_mode` will
        instead be ignored in all cases when the session bind is
        an :class:`_engine.Engine`.

    .. change::
        :tags: bug, general, regression
        :tickets: 11435

        Restored legacy class names removed from
        ``sqlalalchemy.orm.collections.*``, including
        :class:`_orm.MappedCollection`, :func:`_orm.mapped_collection`,
        :func:`_orm.column_mapped_collection`,
        :func:`_orm.attribute_mapped_collection`. Pull request courtesy Takashi
        Kajinami.

    .. change::
        :tags: bug, sql
        :tickets: 11471

        Follow up of :ticket:`11471` to fix caching issue where using the
        :meth:`.CompoundSelectState.add_cte` method of the
        :class:`.CompoundSelectState` construct would not set a correct cache key
        which distinguished between different CTE expressions. Also added tests
        that would detect issues similar to the one fixed in :ticket:`11544`.

    .. change::
        :tags: bug, mysql
        :tickets: 11479

        Fixed issue in MySQL dialect where ENUM values that contained percent signs
        were not properly escaped for the driver.


    .. change::
        :tags: usecase, oracle
        :tickets: 11480

        Implemented two-phase transactions for the oracledb dialect. Historically,
        this feature never worked with the cx_Oracle dialect, however recent
        improvements to the oracledb successor now allow this to be possible.  The
        two phase transaction API is available at the Core level via the
        :meth:`_engine.Connection.begin_twophase` method.

    .. change::
        :tags: bug, postgresql
        :tickets: 11522

        It is now considered a pool-invalidating disconnect event when psycopg2
        throws an "SSL SYSCALL error: Success" error message, which can occur when
        the SSL connection to Postgres is terminated abnormally.

    .. change::
        :tags: bug, schema
        :tickets: 11530

        Fixed additional issues in the event system triggered by unpickling of a
        :class:`.Enum` datatype, continuing from :ticket:`11365` and
        :ticket:`11360`,  where dynamically generated elements of the event
        structure would not be present when unpickling in a new process.

    .. change::
        :tags: bug, engine
        :tickets: 11532

        Fixed issue in "insertmanyvalues" feature where a particular call to
        ``cursor.fetchall()`` were not wrapped in SQLAlchemy's exception wrapper,
        which apparently can raise a database exception during fetch when using
        pyodbc.

    .. change::
        :tags: usecase, orm
        :tickets: 11575

        The :paramref:`_orm.aliased.name` parameter to :func:`_orm.aliased` may now
        be combined with the :paramref:`_orm.aliased.flat` parameter, producing
        per-table names based on a name-prefixed naming convention.  Pull request
        courtesy Eric Atkin.

    .. change::
        :tags: bug, postgresql
        :tickets: 11576

        Fixed issue where the :func:`_sql.collate` construct, which explicitly sets
        a collation for a given expression, would maintain collation settings for
        the underlying type object from the expression, causing SQL expressions to
        have both collations stated at once when used in further expressions for
        specific dialects that render explicit type casts, such as that of asyncpg.
        The :func:`_sql.collate` construct now assigns its own type to explicitly
        include the new collation, assuming it's a string type.

    .. change::
        :tags: bug, sql
        :tickets: 11592

        Fixed bug where the :meth:`.Operators.nulls_first()` and
        :meth:`.Operators.nulls_last()` modifiers would not be treated the same way
        as :meth:`.Operators.desc()` and :meth:`.Operators.asc()` when determining
        if an ORDER BY should be against a label name already in the statement. All
        four modifiers are now treated the same within ORDER BY.

    .. change::
        :tags: bug, orm, regression
        :tickets: 11625

        Fixed regression appearing in 2.0.21 caused by :ticket:`10279` where using
        a :func:`_sql.delete` or :func:`_sql.update` against an ORM class that is
        the base of an inheritance hierarchy, while also specifying that subclasses
        should be loaded polymorphically, would leak the polymorphic joins into the
        UPDATE or DELETE statement as well creating incorrect SQL.

    .. change::
        :tags: bug, orm, regression
        :tickets: 11661

        Fixed regression from version 1.4 in
        :meth:`_orm.Session.bulk_insert_mappings` where using the
        :paramref:`_orm.Session.bulk_insert_mappings.return_defaults` parameter
        would not populate the passed in dictionaries with newly generated primary
        key values.


    .. change::
        :tags: bug, oracle, sqlite
        :tickets: 11663

        Implemented bitwise operators for Oracle which was previously
        non-functional due to a non-standard syntax used by this database.
        Oracle's support for bitwise "or" and "xor" starts with server version 21.
        Additionally repaired the implementation of "xor" for SQLite.

        As part of this change, the dialect compliance test suite has been enhanced
        to include support for server-side bitwise tests; third party dialect
        authors should refer to new "supports_bitwise" methods in the
        requirements.py file to enable these tests.




    .. change::
        :tags: bug, typing

        Fixed internal typing issues to establish compatibility with mypy 1.11.0.
        Note that this does not include issues which have arisen with the
        deprecated mypy plugin used by SQLAlchemy 1.4-style code; see the addiional
        change note for this plugin indicating revised compatibility.

.. changelog::
    :version: 2.0.31
    :released: June 18, 2024

    .. change::
        :tags: usecase, reflection, mysql
        :tickets: 11285

        Added missing foreign key reflection option ``SET DEFAULT``
        in the MySQL and MariaDB dialects.
        Pull request courtesy of Quentin Roche.

    .. change::
        :tags: usecase, orm
        :tickets: 11361

        Added missing parameter :paramref:`_orm.with_polymorphic.name` that
        allows specifying the name of returned :class:`_orm.AliasedClass`.

    .. change::
        :tags: bug, orm
        :tickets: 11365

        Fixed issue where a :class:`.MetaData` collection would not be
        serializable, if an :class:`.Enum` or :class:`.Boolean` datatype were
        present which had been adapted. This specific scenario in turn could occur
        when using the :class:`.Enum` or :class:`.Boolean` within ORM Annotated
        Declarative form where type objects frequently get copied.

    .. change::
        :tags: schema, usecase
        :tickets: 11374

        Added :paramref:`_schema.Column.insert_default` as an alias of
        :paramref:`_schema.Column.default` for compatibility with
        :func:`_orm.mapped_column`.

    .. change::
        :tags: bug, general
        :tickets: 11417

        Set up full Python 3.13 support to the extent currently possible, repairing
        issues within internal language helpers as well as the serializer extension
        module.

    .. change::
        :tags: bug, sql
        :tickets: 11422

        Fixed issue when serializing an :func:`_sql.over` clause with
        unbounded range or rows.

    .. change::
        :tags: bug, sql
        :tickets: 11423

        Added missing methods :meth:`_sql.FunctionFilter.within_group`
        and :meth:`_sql.WithinGroup.filter`

    .. change::
        :tags: bug, sql
        :tickets: 11426

        Fixed bug in :meth:`_sql.FunctionFilter.filter` that would mutate
        the existing function in-place. It now behaves like the rest of the
        SQLAlchemy API, returning a new instance instead of mutating the
        original one.

    .. change::
        :tags: bug, orm
        :tickets: 11446

        Fixed issue where the :func:`_orm.selectinload` and
        :func:`_orm.subqueryload` loader options would fail to take effect when
        made against an inherited subclass that itself included a subclass-specific
        :paramref:`_orm.Mapper.with_polymorphic` setting.

    .. change::
        :tags: bug, orm
        :tickets: 11449

        Fixed very old issue involving the :paramref:`_orm.joinedload.innerjoin`
        parameter where making use of this parameter mixed into a query that also
        included joined eager loads along a self-referential or other cyclical
        relationship, along with complicating factors like inner joins added for
        secondary tables and such, would have the chance of splicing a particular
        inner join to the wrong part of the query.  Additional state has been added
        to the internal method that does this splice to make a better decision as
        to where splicing should proceed.

    .. change::
        :tags: bug, orm, regression
        :tickets: 11509

        Fixed bug in ORM Declarative where the ``__table__`` directive could not be
        declared as a class function with :func:`_orm.declared_attr` on a
        superclass, including an ``__abstract__`` class as well as coming from the
        declarative base itself.  This was a regression since 1.4 where this was
        working, and there were apparently no tests for this particular use case.

.. changelog::
    :version: 2.0.30
    :released: May 5, 2024

    .. change::
        :tags: bug, typing, regression
        :tickets: 11200

        Fixed typing regression caused by :ticket:`11055` in version 2.0.29 that
        added ``ParamSpec`` to the asyncio ``run_sync()`` methods, where using
        :meth:`_asyncio.AsyncConnection.run_sync` with
        :meth:`_schema.MetaData.reflect` would fail on mypy due to a mypy issue.
        Pull request courtesy of Francisco R. Del Roio.

    .. change::
        :tags: bug, engine
        :tickets: 11210

        Fixed issue in the
        :paramref:`_engine.Connection.execution_options.logging_token` option,
        where changing the value of ``logging_token`` on a connection that has
        already logged messages would not be updated to reflect the new logging
        token.  This in particular prevented the use of
        :meth:`_orm.Session.connection` to change the option on the connection,
        since the BEGIN logging message would already have been emitted.

    .. change::
        :tags: bug, orm
        :tickets: 11220

        Added new attribute :attr:`_orm.ORMExecuteState.is_from_statement` to
        detect statements created using :meth:`_sql.Select.from_statement`, and
        enhanced ``FromStatement`` to set :attr:`_orm.ORMExecuteState.is_select`,
        :attr:`_orm.ORMExecuteState.is_insert`,
        :attr:`_orm.ORMExecuteState.is_update`, and
        :attr:`_orm.ORMExecuteState.is_delete` according to the element that is
        sent to the :meth:`_sql.Select.from_statement` method itself.

    .. change::
        :tags: bug, test
        :tickets: 11268

        Ensure the ``PYTHONPATH`` variable is properly initialized when
        using ``subprocess.run`` in the tests.

    .. change::
        :tags: bug, orm
        :tickets: 11291

        Fixed issue in  :func:`_orm.selectin_polymorphic` loader option where
        attributes defined with :func:`_orm.composite` on a superclass would cause
        an internal exception on load.


    .. change::
        :tags: bug, orm, regression
        :tickets: 11292

        Fixed regression from 1.4 where using :func:`_orm.defaultload` in
        conjunction with a non-propagating loader like :func:`_orm.contains_eager`
        would nonetheless propagate the :func:`_orm.contains_eager` to a lazy load
        operation, causing incorrect queries as this option is only intended to
        come from an original load.



    .. change::
        :tags: bug, orm
        :tickets: 11305

        Fixed issue in ORM Annotated Declarative where typing issue where literals
        defined using :pep:`695` type aliases would not work with inference of
        :class:`.Enum` datatypes. Pull request courtesy of Alc-Alc.

    .. change::
        :tags: bug, engine
        :tickets: 11306

        Fixed issue in cursor handling which affected handling of duplicate
        :class:`_sql.Column` or similar objcts in the columns clause of
        :func:`_sql.select`, both in combination with arbitary :func:`_sql.text()`
        clauses in the SELECT list, as well as when attempting to retrieve
        :meth:`_engine.Result.mappings` for the object, which would lead to an
        internal error.



    .. change::
        :tags: bug, orm
        :tickets: 11327

        Fixed issue in :func:`_orm.selectin_polymorphic` loader option where the
        SELECT emitted would only accommodate for the child-most class among the
        result rows that were returned, leading intermediary-class attributes to be
        unloaded if there were no concrete instances of that intermediary-class
        present in the result.   This issue only presented itself for multi-level
        inheritance hierarchies.

    .. change::
        :tags: bug, orm
        :tickets: 11332

        Fixed issue in :meth:`_orm.Session.bulk_save_objects` where the form of the
        identity key produced when using ``return_defaults=True`` would be
        incorrect. This could lead to an errors during pickling as well as identity
        map mismatches.

    .. change::
        :tags: bug, installation
        :tickets: 11334

        Fixed an internal class that was testing for unexpected attributes to work
        correctly under upcoming Python 3.13.   Pull request courtesy Edgar
        Ramírez-Mondragón.

    .. change::
        :tags: bug, orm
        :tickets: 11347

        Fixed issue where attribute key names in :class:`_orm.Bundle` would not be
        correct when using ORM enabled :class:`_sql.select` vs.
        :class:`_orm.Query`, when the statement contained duplicate column names.

    .. change::
        :tags: bug, typing

        Fixed issue in typing for :class:`_orm.Bundle` where creating a nested
        :class:`_orm.Bundle` structure were not allowed.

.. changelog::
    :version: 2.0.29
    :released: March 23, 2024

    .. change::
        :tags: bug, orm
        :tickets: 10611

        Fixed Declarative issue where typing a relationship using
        :class:`_orm.Relationship` rather than :class:`_orm.Mapped` would
        inadvertently pull in the "dynamic" relationship loader strategy for that
        attribute.

    .. change::
        :tags: postgresql, usecase
        :tickets: 10693

        The PostgreSQL dialect now returns :class:`_postgresql.DOMAIN` instances
        when reflecting a column that has a domain as type. Previously, the domain
        data type was returned instead. As part of this change, the domain
        reflection was improved to also return the collation of the text types.
        Pull request courtesy of Thomas Stephenson.

    .. change::
        :tags: bug, typing
        :tickets: 11055

        Fixed typing issue allowing asyncio ``run_sync()`` methods to correctly
        type the parameters according to the callable that was passed, making use
        of :pep:`612` ``ParamSpec`` variables.  Pull request courtesy Francisco R.
        Del Roio.

    .. change::
        :tags: bug, orm
        :tickets: 11091

        Fixed issue in ORM annotated declarative where using
        :func:`_orm.mapped_column()` with an :paramref:`_orm.mapped_column.index`
        or :paramref:`_orm.mapped_column.unique` setting of False would be
        overridden by an incoming ``Annotated`` element that featured that
        parameter set to ``True``, even though the immediate
        :func:`_orm.mapped_column()` element is more specific and should take
        precedence.  The logic to reconcile the booleans has been enhanced to
        accommodate a local value of ``False`` as still taking precedence over an
        incoming ``True`` value from the annotated element.

    .. change::
        :tags: usecase, orm
        :tickets: 11130

        Added support for the :pep:`695` ``TypeAliasType`` construct as well as the
        python 3.12 native ``type`` keyword to work with ORM Annotated Declarative
        form when using these constructs to link to a :pep:`593` ``Annotated``
        container, allowing the resolution of the ``Annotated`` to proceed when
        these constructs are used in a :class:`_orm.Mapped` typing container.

    .. change::
        :tags: bug, engine
        :tickets: 11157

        Fixed issue in :ref:`engine_insertmanyvalues` feature where using a primary
        key column with an "inline execute" default generator such as an explicit
        :class:`.Sequence` with an explcit schema name, while at the same time
        using the
        :paramref:`_engine.Connection.execution_options.schema_translate_map`
        feature would fail to render the sequence or the parameters properly,
        leading to errors.

    .. change::
        :tags: bug, engine
        :tickets: 11160

        Made a change to the adjustment made in version 2.0.10 for :ticket:`9618`,
        which added the behavior of reconciling RETURNING rows from a bulk INSERT
        to the parameters that were passed to it.  This behavior included a
        comparison of already-DB-converted bound parameter values against returned
        row values that was not always "symmetrical" for SQL column types such as
        UUIDs, depending on specifics of how different DBAPIs receive such values
        versus how they return them, necessitating the need for additional
        "sentinel value resolver" methods on these column types.  Unfortunately
        this broke third party column types such as UUID/GUID types in libraries
        like SQLModel which did not implement this special method, raising an error
        "Can't match sentinel values in result set to parameter sets".  Rather than
        attempt to further explain and document this implementation detail of the
        "insertmanyvalues" feature including a public version of the new
        method, the approach is intead revised to no longer need this extra
        conversion step, and the logic that does the comparison now works on the
        pre-converted bound parameter value compared to the post-result-processed
        value, which should always be of a matching datatype.  In the unusual case
        that a custom SQL column type that also happens to be used in a "sentinel"
        column for bulk INSERT is not receiving and returning the same value type,
        the "Can't match" error will be raised, however the mitigation is
        straightforward in that the same Python datatype should be passed as that
        returned.

    .. change::
        :tags: bug, orm, regression
        :tickets: 11173

        Fixed regression from version 2.0.28 caused by the fix for :ticket:`11085`
        where the newer method of adjusting post-cache bound parameter values would
        interefere with the implementation for the :func:`_orm.subqueryload` loader
        option, which has some more legacy patterns in use internally, when
        the additional loader criteria feature were used with this loader option.

    .. change::
        :tags: bug, sql, regression
        :tickets: 11176

        Fixed regression from the 1.4 series where the refactor of the
        :meth:`_types.TypeEngine.with_variant` method introduced at
        :ref:`change_6980` failed to accommodate for the ``.copy()`` method, which
        will lose the variant mappings that are set up. This becomes an issue for
        the very specific case of a "schema" type, which includes types such as
        :class:`.Enum` and :class:`_types.ARRAY`, when they are then used in the context
        of an ORM Declarative mapping with mixins where copying of types comes into
        play.  The variant mapping is now copied as well.

    .. change::
        :tags: bug, tests
        :tickets: 11187

        Backported to SQLAlchemy 2.0 an improvement to the test suite with regards
        to how asyncio related tests are run, now using the newer Python 3.11
        ``asyncio.Runner`` or a backported equivalent, rather than relying on the
        previous implementation based on ``asyncio.get_running_loop()``.  This
        should hopefully prevent issues with large suite runs on CPU loaded
        hardware where the event loop seems to become corrupted, leading to
        cascading failures.


.. changelog::
    :version: 2.0.28
    :released: March 4, 2024

    .. change::
        :tags: engine, usecase
        :tickets: 10974

        Added new core execution option
        :paramref:`_engine.Connection.execution_options.preserve_rowcount`. When
        set, the ``cursor.rowcount`` attribute from the DBAPI cursor will be
        unconditionally memoized at statement execution time, so that whatever
        value the DBAPI offers for any kind of statement will be available using
        the :attr:`_engine.CursorResult.rowcount` attribute from the
        :class:`_engine.CursorResult`.  This allows the rowcount to be accessed for
        statements such as INSERT and SELECT, to the degree supported by the DBAPI
        in use. The :ref:`engine_insertmanyvalues` also supports this option and
        will ensure :attr:`_engine.CursorResult.rowcount` is correctly set for a
        bulk INSERT of rows when set.

    .. change::
        :tags: bug, orm, regression
        :tickets: 11010

        Fixed regression caused by :ticket:`9779` where using the "secondary" table
        in a relationship ``and_()`` expression would fail to be aliased to match
        how the "secondary" table normally renders within a
        :meth:`_sql.Select.join` expression, leading to an invalid query.

    .. change::
        :tags: bug, orm, performance, regression
        :tickets: 11085

        Adjusted the fix made in :ticket:`10570`, released in 2.0.23, where new
        logic was added to reconcile possibly changing bound parameter values
        across cache key generations used within the :func:`_orm.with_expression`
        construct.  The new logic changes the approach by which the new bound
        parameter values are associated with the statement, avoiding the need to
        deep-copy the statement which can result in a significant performance
        penalty for very deep / complex SQL constructs.  The new approach no longer
        requires this deep-copy step.

    .. change::
        :tags: bug, asyncio
        :tickets: 8771

        An error is raised if a :class:`.QueuePool` or other non-asyncio pool class
        is passed to :func:`_asyncio.create_async_engine`.  This engine only
        accepts asyncio-compatible pool classes including
        :class:`.AsyncAdaptedQueuePool`. Other pool classes such as
        :class:`.NullPool` are compatible with both synchronous and asynchronous
        engines as they do not perform any locking.

        .. seealso::

            :ref:`pool_api`


    .. change::
        :tags: change, tests

        pytest support in the tox.ini file has been updated to support pytest 8.1.

.. changelog::
    :version: 2.0.27
    :released: February 13, 2024

    .. change::
        :tags: bug, postgresql, regression
        :tickets: 11005

        Fixed regression caused by just-released fix for :ticket:`10863` where an
        invalid exception class were added to the "except" block, which does not
        get exercised unless such a catch actually happens.   A mock-style test has
        been added to ensure this catch is exercised in unit tests.


.. changelog::
    :version: 2.0.26
    :released: February 11, 2024

    .. change::
        :tags: usecase, postgresql, reflection
        :tickets: 10777

        Added support for reflection of PostgreSQL CHECK constraints marked with
        "NO INHERIT", setting the key ``no_inherit=True`` in the reflected data.
        Pull request courtesy Ellis Valentiner.

    .. change::
        :tags: bug, sql
        :tickets: 10843

        Fixed issues in :func:`_sql.case` where the logic for determining the
        type of the expression could result in :class:`.NullType` if the last
        element in the "whens" had no type, or in other cases where the type
        could resolve to ``None``.  The logic has been updated to scan all
        given expressions so that the first non-null type is used, as well as
        to always ensure a type is present.  Pull request courtesy David Evans.

    .. change::
        :tags: bug, mysql
        :tickets: 10850

        Fixed issue where NULL/NOT NULL would not be properly reflected from a
        MySQL column that also specified the VIRTUAL or STORED directives.  Pull
        request courtesy Georg Wicke-Arndt.

    .. change::
        :tags: bug, regression, postgresql
        :tickets: 10863

        Fixed regression in the asyncpg dialect caused by :ticket:`10717` in
        release 2.0.24 where the change that now attempts to gracefully close the
        asyncpg connection before terminating would not fall back to
        ``terminate()`` for other potential connection-related exceptions other
        than a timeout error, not taking into account cases where the graceful
        ``.close()`` attempt fails for other reasons such as connection errors.


    .. change::
        :tags: oracle, bug, performance
        :tickets: 10877

        Changed the default arraysize of the Oracle dialects so that the value set
        by the driver is used, that is 100 at the time of writing for both
        cx_oracle and oracledb. Previously the value was set to 50 by default. The
        setting of 50 could cause significant performance regressions compared to
        when using cx_oracle/oracledb alone to fetch many hundreds of rows over
        slower networks.

    .. change::
        :tags: bug, mysql
        :tickets: 10893

        Fixed issue in asyncio dialects asyncmy and aiomysql, where their
        ``.close()`` method is apparently not a graceful close.  replace with
        non-standard ``.ensure_closed()`` method that's awaitable and move
        ``.close()`` to the so-called "terminate" case.

    .. change::
        :tags: bug, orm
        :tickets: 10896

        Replaced the "loader depth is excessively deep" warning with a shorter
        message added to the caching badge within SQL logging, for those statements
        where the ORM disabled the cache due to a too-deep chain of loader options.
        The condition which this warning highlights is difficult to resolve and is
        generally just a limitation in the ORM's application of SQL caching. A
        future feature may include the ability to tune the threshold where caching
        is disabled, but for now the warning will no longer be a nuisance.

    .. change::
        :tags: bug, orm
        :tickets: 10899

        Fixed issue where it was not possible to use a type (such as an enum)
        within a :class:`_orm.Mapped` container type if that type were declared
        locally within the class body.  The scope of locals used for the eval now
        includes that of the class body itself.  In addition, the expression within
        :class:`_orm.Mapped` may also refer to the class name itself, if used as a
        string or with future annotations mode.

    .. change::
        :tags: usecase, postgresql
        :tickets: 10904

        Support the ``USING <method>`` option for PostgreSQL ``CREATE TABLE`` to
        specify the access method to use to store the contents for the new table.
        Pull request courtesy Edgar Ramírez-Mondragón.

        .. seealso::

            :ref:`postgresql_table_options`

    .. change::
        :tags: bug, examples
        :tickets: 10920

        Fixed regression in history_meta example where the use of
        :meth:`_schema.MetaData.to_metadata` to make a copy of the history table
        would also copy indexes (which is a good thing), but causing naming
        conflicts indexes regardless of naming scheme used for those indexes. A
        "_history" suffix is now added to these indexes in the same way as is
        achieved for the table name.


    .. change::
        :tags: bug, orm
        :tickets: 10967

        Fixed issue where using :meth:`_orm.Session.delete` along with the
        :paramref:`_orm.Mapper.version_id_col` feature would fail to use the
        correct version identifier in the case that an additional UPDATE were
        emitted against the target object as a result of the use of
        :paramref:`_orm.relationship.post_update` on the object.  The issue is
        similar to :ticket:`10800` just fixed in version 2.0.25 for the case of
        updates alone.

    .. change::
        :tags: bug, orm
        :tickets: 10990

        Fixed issue where an assertion within the implementation for
        :func:`_orm.with_expression` would raise if a SQL expression that was not
        cacheable were used; this was a 2.0 regression since 1.4.

    .. change::
        :tags: postgresql, usecase
        :tickets: 9736

        Correctly type PostgreSQL RANGE and MULTIRANGE types as ``Range[T]``
        and ``Sequence[Range[T]]``.
        Introduced utility sequence :class:`_postgresql.MultiRange` to allow better
        interoperability of MULTIRANGE types.

    .. change::
        :tags: postgresql, usecase

        Differentiate between INT4 and INT8 ranges and multi-ranges types when
        inferring the database type from a :class:`_postgresql.Range` or
        :class:`_postgresql.MultiRange` instance, preferring INT4 if the values
        fit into it.

    .. change::
        :tags: bug, typing

        Fixed the type signature for the :meth:`.PoolEvents.checkin` event to
        indicate that the given :class:`.DBAPIConnection` argument may be ``None``
        in the case where the connection has been invalidated.

    .. change::
        :tags: bug, examples

        Fixed the performance example scripts in examples/performance to mostly
        work with the Oracle database, by adding the :class:`.Identity` construct
        to all the tables and allowing primary generation to occur on this backend.
        A few of the "raw DBAPI" cases still are not compatible with Oracle.


    .. change::
        :tags: bug, mssql

        Fixed an issue regarding the use of the :class:`.Uuid` datatype with the
        :paramref:`.Uuid.as_uuid` parameter set to False, when using the pymssql
        dialect. ORM-optimized INSERT statements (e.g. the "insertmanyvalues"
        feature) would not correctly align primary key UUID values for bulk INSERT
        statements, resulting in errors.  Similar issues were fixed for the
        PostgreSQL drivers as well.


    .. change::
        :tags: bug, postgresql

        Fixed an issue regarding the use of the :class:`.Uuid` datatype with the
        :paramref:`.Uuid.as_uuid` parameter set to False, when using PostgreSQL
        dialects. ORM-optimized INSERT statements (e.g. the "insertmanyvalues"
        feature) would not correctly align primary key UUID values for bulk INSERT
        statements, resulting in errors.  Similar issues were fixed for the
        pymssql driver as well.

.. changelog::
    :version: 2.0.25
    :released: January 2, 2024

    .. change::
        :tags: oracle, asyncio
        :tickets: 10679

        Added support for :ref:`oracledb` in asyncio mode, using the newly released
        version of the ``oracledb`` DBAPI that includes asyncio support. For the
        2.0 series, this is a preview release, where the current implementation
        does not yet have include support for
        :meth:`_asyncio.AsyncConnection.stream`. Improved support is planned for
        the 2.1 release of SQLAlchemy.

    .. change::
        :tags: bug, orm
        :tickets: 10800

        Fixed issue where when making use of the
        :paramref:`_orm.relationship.post_update` feature at the same time as using
        a mapper version_id_col could lead to a situation where the second UPDATE
        statement emitted by the post-update feature would fail to make use of the
        correct version identifier, assuming an UPDATE was already emitted in that
        flush which had already bumped the version counter.

    .. change::
        :tags: bug, typing
        :tickets: 10801, 10818

        Fixed regressions caused by typing added to the ``sqlalchemy.sql.functions``
        module in version 2.0.24, as part of :ticket:`6810`:

        * Further enhancements to pep-484 typing to allow SQL functions from
          :attr:`_sql.func` derived elements to work more effectively with ORM-mapped
          attributes (:ticket:`10801`)

        * Fixed the argument types passed to functions so that literal expressions
          like strings and ints are again interpreted correctly (:ticket:`10818`)


    .. change::
        :tags: usecase, orm
        :tickets: 10807

        Added preliminary support for Python 3.12 pep-695 type alias structures,
        when resolving custom type maps for ORM Annotated Declarative mappings.


    .. change::
        :tags: bug, orm
        :tickets: 10815

        Fixed issue where ORM Annotated Declarative would mis-interpret the left
        hand side of a relationship without any collection specified as
        uselist=True if the left type were given as a class and not a string,
        without using future-style annotations.

    .. change::
        :tags: bug, sql
        :tickets: 10817

        Improved compilation of :func:`_sql.any_` / :func:`_sql.all_` in the
        context of a negation of boolean comparison, will now render ``NOT (expr)``
        rather than reversing the equality operator to not equals, allowing
        finer-grained control of negations for these non-typical operators.

.. changelog::
    :version: 2.0.24
    :released: December 28, 2023

    .. change::
        :tags: bug, orm
        :tickets: 10597

        Fixed issue where use of :func:`_orm.foreign` annotation on a
        non-initialized :func:`_orm.mapped_column` construct would produce an
        expression without a type, which was then not updated at initialization
        time of the actual column, leading to issues such as relationships not
        determining ``use_get`` appropriately.


    .. change::
        :tags: bug, schema
        :tickets: 10654

        Fixed issue where error reporting for unexpected schema item when creating
        objects like :class:`_schema.Table` would incorrectly handle an argument
        that was itself passed as a tuple, leading to a formatting error.  The
        error message has been modernized to use f-strings.

    .. change::
        :tags: bug, engine
        :tickets: 10662

        Fixed URL-encoding of the username and password components of
        :class:`.engine.URL` objects when converting them to string using the
        :meth:`_engine.URL.render_as_string` method, by using Python standard
        library ``urllib.parse.quote`` while allowing for plus signs and spaces to
        remain unchanged as supported by SQLAlchemy's non-standard URL parsing,
        rather than the legacy home-grown routine from many years ago. Pull request
        courtesy of Xavier NUNN.

    .. change::
        :tags: bug, orm
        :tickets: 10668

        Improved the error message produced when the unit of work process sets the
        value of a primary key column to NULL due to a related object with a
        dependency rule on that column being deleted, to include not just the
        destination object and column name but also the source column from which
        the NULL value is originating.  Pull request courtesy Jan Vollmer.

    .. change::
        :tags: bug, postgresql
        :tickets: 10717

        Adjusted the asyncpg dialect such that when the ``terminate()`` method is
        used to discard an invalidated connection, the dialect will first attempt
        to gracefully close the connection using ``.close()`` with a timeout, if
        the operation is proceeding within an async event loop context only. This
        allows the asyncpg driver to attend to finalizing a ``TimeoutError``
        including being able to close a long-running query server side, which
        otherwise can keep running after the program has exited.

    .. change::
        :tags: bug, orm
        :tickets: 10732

        Modified the ``__init_subclass__()`` method used by
        :class:`_orm.MappedAsDataclass`, :class:`_orm.DeclarativeBase` and
        :class:`_orm.DeclarativeBaseNoMeta` to accept arbitrary ``**kw`` and to
        propagate them to the ``super()`` call, allowing greater flexibility in
        arranging custom superclasses and mixins which make use of
        ``__init_subclass__()`` keyword arguments.  Pull request courtesy Michael
        Oliver.


    .. change::
        :tags: bug, tests
        :tickets: 10747

        Improvements to the test suite to further harden its ability to run
        when Python ``greenlet`` is not installed.   There is now a tox
        target that includes the token "nogreenlet" that will run the suite
        with greenlet not installed (note that it still temporarily installs
        greenlet as part of the tox config, however).

    .. change::
        :tags: bug, sql
        :tickets: 10753

        Fixed issue in stringify for SQL elements, where a specific dialect is not
        passed,  where a dialect-specific element such as the PostgreSQL "on
        conflict do update" construct is encountered and then fails to provide for
        a stringify dialect with the appropriate state to render the construct,
        leading to internal errors.

    .. change::
        :tags: bug, sql

        Fixed issue where stringifying or compiling a :class:`.CTE` that was
        against a DML construct such as an :func:`_sql.insert` construct would fail
        to stringify, due to a mis-detection that the statement overall is an
        INSERT, leading to internal errors.

    .. change::
        :tags: bug, orm
        :tickets: 10776

        Ensured the use case of :class:`.Bundle` objects used in the
        ``returning()`` portion of ORM-enabled INSERT, UPDATE and DELETE statements
        is tested and works fully.   This was never explicitly implemented or
        tested previously and did not work correctly in the 1.4 series; in the 2.0
        series, ORM UPDATE/DELETE with WHERE criteria was missing an implementation
        method preventing :class:`.Bundle` objects from working.

    .. change::
        :tags: bug, orm
        :tickets: 10784

        Fixed 2.0 regression in :class:`.MutableList` where a routine that detects
        sequences would not correctly filter out string or bytes instances, making
        it impossible to assign a string value to a specific index (while
        non-sequence values would work fine).

    .. change::
        :tags: change, asyncio

        The ``async_fallback`` dialect argument is now deprecated, and will be
        removed in SQLAlchemy 2.1.   This flag has not been used for SQLAlchemy's
        test suite for some time.   asyncio dialects can still run in a synchronous
        style by running code within a greenlet using :func:`_util.greenlet_spawn`.

    .. change::
       :tags: bug, typing
       :tickets: 6810

       Completed pep-484 typing for the ``sqlalchemy.sql.functions`` module.
       :func:`_sql.select` constructs made against ``func`` elements should now
       have filled-in return types.

.. changelog::
    :version: 2.0.23
    :released: November 2, 2023

    .. change::
        :tags: bug, oracle
        :tickets: 10509

        Fixed issue in :class:`.Interval` datatype where the Oracle implementation
        was not being used for DDL generation, leading to the ``day_precision`` and
        ``second_precision`` parameters to be ignored, despite being supported by
        this dialect.  Pull request courtesy Indivar.

    .. change::
        :tags: bug, orm
        :tickets: 10516

        Fixed issue where the ``__allow_unmapped__`` directive failed to allow for
        legacy :class:`.Column` / :func:`.deferred` mappings that nonetheless had
        annotations such as ``Any`` or a specific type without ``Mapped[]`` as
        their type, without errors related to locating the attribute name.

    .. change::
        :tags: bug, mariadb
        :tickets: 10056

        Adjusted the MySQL / MariaDB dialects to default a generated column to NULL
        when using MariaDB, if :paramref:`_schema.Column.nullable` was not
        specified with an explicit ``True`` or ``False`` value, as MariaDB does not
        support the "NOT NULL" phrase with a generated column.  Pull request
        courtesy Indivar.


    .. change::
        :tags: bug, mariadb, regression
        :tickets: 10505

        Established a workaround for what seems to be an intrinsic issue across
        MySQL/MariaDB drivers where a RETURNING result for DELETE DML which returns
        no rows using SQLAlchemy's "empty IN" criteria fails to provide a
        cursor.description, which then yields result that returns no rows,
        leading to regressions for the ORM that in the 2.0 series uses RETURNING
        for bulk DELETE statements for the "synchronize session" feature. To
        resolve, when the specific case of "no description when RETURNING was
        given" is detected, an "empty result" with a correct cursor description is
        generated and used in place of the non-working cursor.

    .. change::
        :tags: bug, orm
        :tickets: 10570

        Fixed caching bug where using the :func:`_orm.with_expression` construct in
        conjunction with loader options :func:`_orm.selectinload`,
        :func:`_orm.lazyload` would fail to substitute bound parameter values
        correctly on subsequent caching runs.

    .. change::
        :tags: usecase, mssql
        :tickets: 6521

        Added support for the ``aioodbc`` driver implemented for SQL Server,
        which builds on top of the pyodbc and general aio* dialect architecture.

        .. seealso::

            :ref:`mssql_aioodbc` - in the SQL Server dialect documentation.



    .. change::
        :tags: bug, sql
        :tickets: 10535

        Added compiler-level None/NULL handling for the "literal processors" of all
        datatypes that include literal processing, that is, where a value is
        rendered inline within a SQL statement rather than as a bound parameter,
        for all those types that do not feature explicit "null value" handling.
        Previously this behavior was undefined and inconsistent.

    .. change::
        :tags: usecase, orm
        :tickets: 10575

        Implemented the :paramref:`_orm.Session.bulk_insert_mappings.render_nulls`
        parameter for new style bulk ORM inserts, allowing ``render_nulls=True`` as
        an execution option.   This allows for bulk ORM inserts with a mixture of
        ``None`` values in the parameter dictionaries to use a single batch of rows
        for a given set of dicationary keys, rather than breaking up into batches
        that omit the NULL columns from each INSERT.

        .. seealso::

            :ref:`orm_queryguide_insert_null_params`

    .. change::
        :tags: bug, postgresql
        :tickets: 10479

        Fixed 2.0 regression caused by :ticket:`7744` where chains of expressions
        involving PostgreSQL JSON operators combined with other operators such as
        string concatenation would lose correct parenthesization, due to an
        implementation detail specific to the PostgreSQL dialect.

    .. change::
        :tags: bug, postgresql
        :tickets: 10532

        Fixed SQL handling for "insertmanyvalues" when using the
        :class:`.postgresql.BIT` datatype with the asyncpg backend.  The
        :class:`.postgresql.BIT` on asyncpg apparently requires the use of an
        asyncpg-specific ``BitString`` type which is currently exposed when using
        this DBAPI, making it incompatible with other PostgreSQL DBAPIs that all
        work with plain bitstrings here.  A future fix in version 2.1 will
        normalize this datatype across all PG backends.   Pull request courtesy
        Sören Oldag.


    .. change::
        :tags: usecase, sql
        :tickets: 9737

        Implemented "literal value processing" for the :class:`.Interval` datatype
        for both the PostgreSQL and Oracle dialects, allowing literal rendering of
        interval values.  Pull request courtesy Indivar Mishra.

    .. change::
        :tags: bug, oracle
        :tickets: 10470

        Fixed issue where the cx_Oracle dialect claimed to support a lower
        cx_Oracle version (7.x) than was actually supported in practice within the
        2.0 series of SQLAlchemy. The dialect imports symbols that are only in
        cx_Oracle 8 or higher, so runtime dialect checks as well as setup.cfg
        requirements have been updated to reflect this compatibility.

    .. change::
        :tags: sql

        Removed unused placeholder method :meth:`.TypeEngine.compare_against_backend`
        This method was used by very old versions of Alembic.
        See https://github.com/sqlalchemy/alembic/issues/1293 for details.

    .. change::
        :tags: bug, orm
        :tickets: 10472

        Fixed bug in ORM annotated declarative where using a ``ClassVar`` that
        nonetheless referred in some way to an ORM mapped class name would fail to
        be interpreted as a ``ClassVar`` that's not mapped.

    .. change::
        :tags: bug, asyncio
        :tickets: 10421

        Fixed bug with method :meth:`_asyncio.AsyncSession.close_all`
        that was not working correctly.
        Also added function :func:`_asyncio.close_all_sessions` that's
        the equivalent of :func:`_orm.close_all_sessions`.
        Pull request courtesy of Bryan不可思议.

.. changelog::
    :version: 2.0.22
    :released: October 12, 2023

    .. change::
        :tags: bug, orm
        :tickets: 10369, 10046

        Fixed a wide range of :func:`_orm.mapped_column` parameters that were not
        being transferred when using the :func:`_orm.mapped_column` object inside
        of a pep-593 ``Annotated`` object, including
        :paramref:`_orm.mapped_column.sort_order`,
        :paramref:`_orm.mapped_column.deferred`,
        :paramref:`_orm.mapped_column.autoincrement`,
        :paramref:`_orm.mapped_column.system`, :paramref:`_orm.mapped_column.info`
        etc.

        Additionally, it remains not supported to have dataclass arguments, such as
        :paramref:`_orm.mapped_column.kw_only`,
        :paramref:`_orm.mapped_column.default_factory` etc. indicated within the
        :func:`_orm.mapped_column` received by ``Annotated``, as this is not
        supported with pep-681 Dataclass Transforms.  A warning is now emitted when
        these parameters are used within ``Annotated`` in this way (and they
        continue to be ignored).

    .. change::
        :tags: bug, orm
        :tickets: 10459

        Fixed issue where calling :meth:`_engine.Result.unique` with a new-style
        :func:`.select` query in the ORM, where one or more columns yields values
        that are of "unknown hashability", typically when using JSON functions like
        ``func.json_build_object()`` without providing a type, would fail
        internally when the returned values were not actually hashable. The
        behavior is repaired to test the objects as they are received for
        hashability in this case, raising an informative error message if not. Note
        that for values of "known unhashability", such as when the
        :class:`_types.JSON` or :class:`_types.ARRAY` types are used directly, an
        informative error message was already raised.

        The "hashabiltiy testing" fix here is applied to legacy :class:`.Query` as
        well, however in the legacy case, :meth:`_engine.Result.unique` is used for
        nearly all queries, so no new warning is emitted here; the legacy behavior
        of falling back to using ``id()`` in this case is maintained, with the
        improvement that an unknown type that turns out to be hashable will now be
        uniqufied, whereas previously it would not.

    .. change::
        :tags: bug, orm
        :tickets: 10453

        Fixed regression in recently revised "insertmanyvalues" feature (likely
        issue :ticket:`9618`) where the ORM would inadvertently attempt to
        interpret a non-RETURNING result as one with RETURNING, in the case where
        the ``implicit_returning=False`` parameter were applied to the mapped
        :class:`.Table`, indicating that "insertmanyvalues" cannot be used if the
        primary key values are not provided.

    .. change::
        :tags: bug, engine

        Fixed issue within some dialects where the dialect could incorrectly return
        an empty result set for an INSERT statement that does not actually return
        rows at all, due to artfacts from pre- or post-fetching the primary key of
        the row or rows still being present.  Affected dialects included asyncpg,
        all mssql dialects.

    .. change::
        :tags: bug, typing
        :tickets: 10451

        Fixed typing issue where the argument list passed to :class:`.Values` was
        too-restrictively tied to ``List`` rather than ``Sequence``.  Pull request
        courtesy Iuri de Silvio.

    .. change::
        :tags: bug, orm
        :tickets: 10365, 11412

        Fixed bug where ORM :func:`_orm.with_loader_criteria` would not apply
        itself to a :meth:`_sql.Select.join` where the ON clause were given as a
        plain SQL comparison, rather than as a relationship target or similar.

        **update** - this was found to also fix an issue where
        single-inheritance criteria would not be correctly applied to a
        subclass entity that only appeared in the ``select_from()`` list,
        see :ticket:`11412`

    .. change::
        :tags: bug, sql
        :tickets: 10408

        Fixed issue where referring to a FROM entry in the SET clause of an UPDATE
        statement would not include it in the FROM clause of the UPDATE statement,
        if that entry were nowhere else in the statement; this occurs currently for
        CTEs that were added using :meth:`.Update.add_cte` to provide the desired
        CTE at the top of the statement.

    .. change::
        :tags: bug, mariadb
        :tickets: 10396

        Modified the mariadb-connector driver to pre-load the ``cursor.rowcount``
        value for all queries, to suit tools such as Pandas that hardcode to
        calling :attr:`.Result.rowcount` in this way. SQLAlchemy normally pre-loads
        ``cursor.rowcount`` only for UPDATE/DELETE statements and otherwise passes
        through to the DBAPI where it can return -1 if no value is available.
        However, mariadb-connector does not support invoking ``cursor.rowcount``
        after the cursor itself is closed, raising an error instead.  Generic test
        support has been added to ensure all backends support the allowing
        :attr:`.Result.rowcount` to succceed (that is, returning an integer
        value with -1 for "not available") after the result is closed.



    .. change::
        :tags: bug, mariadb

        Additional fixes for the mariadb-connector dialect to support UUID data
        values in the result in INSERT..RETURNING statements.

    .. change::
        :tags: bug, mssql
        :tickets: 10458

        Fixed bug where the rule that prevents ORDER BY from emitting within
        subqueries on SQL Server was not being disabled in the case where the
        :meth:`.select.fetch` method were used to limit rows in conjunction with
        WITH TIES or PERCENT, preventing valid subqueries with TOP / ORDER BY from
        being used.



    .. change::
        :tags: bug, sql
        :tickets: 10443

        Fixed 2.0 regression where the :class:`.DDL` construct would no longer
        ``__repr__()`` due to the removed ``on`` attribute not being accommodated.
        Pull request courtesy Iuri de Silvio.

    .. change::
        :tags: orm, usecase
        :tickets: 10202

        Added method :meth:`_orm.Session.get_one` that behaves like
        :meth:`_orm.Session.get` but raises an exception instead of returning
        ``None`` if no instance was found with the provided primary key.
        Pull request courtesy of Carlos Sousa.


    .. change::
        :tags: asyncio, bug

        Fixed the :paramref:`_asyncio.AsyncSession.get.execution_options` parameter
        which was not being propagated to the underlying :class:`_orm.Session` and
        was instead being ignored.

    .. change::
        :tags: bug, orm
        :tickets: 10412

        Fixed issue where :class:`.Mapped` symbols like :class:`.WriteOnlyMapped`
        and :class:`.DynamicMapped` could not be correctly resolved when referenced
        as an element of a sub-module in the given annotation, assuming
        string-based or "future annotations" style annotations.

    .. change::
        :tags: bug, engine
        :tickets: 10414

        Fixed issue where under some garbage collection / exception scenarios the
        connection pool's cleanup routine would raise an error due to an unexpected
        set of state, which can be reproduced under specific conditions.

    .. change::
        :tags: bug, typing

        Updates to the codebase to support Mypy 1.6.0.

    .. change::
        :tags: usecase, orm
        :tickets: 7787

        Added an option to permanently close sessions.
        Set to ``False`` the new parameter :paramref:`_orm.Session.close_resets_only`
        will prevent a :class:`_orm.Session` from performing any other
        operation after :meth:`_orm.Session.close` has been called.

        Added new method :meth:`_orm.Session.reset` that will reset a :class:`_orm.Session`
        to its initial state. This is an alias of :meth:`_orm.Session.close`,
        unless :paramref:`_orm.Session.close_resets_only` is set to ``False``.

    .. change::
        :tags: orm, bug
        :tickets: 10385

        Fixed issue with ``__allow_unmapped__`` declarative option
        where types that were declared using collection types such as
        ``list[SomeClass]`` vs. the typing construct ``List[SomeClass]``
        would fail to be recognized correctly.  Pull request courtesy
        Pascal Corpet.

.. changelog::
    :version: 2.0.21
    :released: September 18, 2023

    .. change::
        :tags: bug, sql
        :tickets: 9610

        Adjusted the operator precedence for the string concatenation operator to
        be equal to that of string matching operators, such as
        :meth:`.ColumnElement.like`, :meth:`.ColumnElement.regexp_match`,
        :meth:`.ColumnElement.match`, etc., as well as plain ``==`` which has the
        same precedence as string comparison operators, so that parenthesis will be
        applied to a string concatenation expression that follows a string match
        operator. This provides for backends such as PostgreSQL where the "regexp
        match" operator is apparently of higher precedence than the string
        concatenation operator.

    .. change::
        :tags: bug, sql
        :tickets: 10342

        Qualified the use of ``hashlib.md5()`` within the DDL compiler, which is
        used to generate deterministic four-character suffixes for long index and
        constraint names in DDL statements, to include the Python 3.9+
        ``usedforsecurity=False`` parameter so that Python interpreters built for
        restricted environments such as FIPS do not consider this call to be
        related to security concerns.

    .. change::
        :tags: bug, postgresql
        :tickets: 10226

        Fixed regression which appeared in 2.0 due to :ticket:`8491` where the
        revised "ping" used for PostgreSQL dialects when the
        :paramref:`_sa.create_engine.pool_pre_ping` parameter is in use would
        interfere with the use of asyncpg with PGBouncer "transaction" mode, as the
        multiple PostgreSQL commands emitted by asnycpg could be broken out among
        multiple connections leading to errors, due to the lack of any transaction
        around this newly revised "ping".   The ping is now invoked within a
        transaction, in the same way that is implicit with all other backends that
        are based on the pep-249 DBAPI; this guarantees that the series of PG
        commands sent by asyncpg for this command are invoked on the same backend
        connection without it jumping to a different connection mid-command.  The
        transaction is not used if the asyncpg dialect is used in "AUTOCOMMIT"
        mode, which remains incompatible with pgbouncer transaction mode.


    .. change::
        :tags: bug, orm
        :tickets: 10279

        Adjusted the ORM's interpretation of the "target" entity used within
        :class:`.Update` and :class:`.Delete` to not interfere with the target
        "from" object passed to the statement, such as when passing an ORM-mapped
        :class:`_orm.aliased` construct that should be maintained within a phrase
        like "UPDATE FROM".  Cases like ORM session synchonize using "SELECT"
        statements such as with MySQL/ MariaDB will still have issues with
        UPDATE/DELETE of this form so it's best to disable synchonize_session when
        using DML statements of this type.

    .. change::
        :tags: bug, orm
        :tickets: 10348

        Added new capability to the :func:`_orm.selectin_polymorphic` loader option
        which allows other loader options to be bundled as siblings, referring to
        one of its subclasses, within the sub-options of parent loader option.
        Previously, this pattern was only supported if the
        :func:`_orm.selectin_polymorphic` were at the top level of the options for
        the query.   See new documentation section for example.

        As part of this change, improved the behavior of the
        :meth:`_orm.Load.selectin_polymorphic` method / loader strategy so that the
        subclass load does not load most already-loaded columns from the parent
        table, when the option is used against a class that is already being
        relationship-loaded.  Previously, the logic to load only the subclass
        columns worked only for a top level class load.

        .. seealso::

            :ref:`polymorphic_selectin_as_loader_option_target_plus_opts`

    .. change::
        :tags: bug, typing
        :tickets: 10264, 9284

        Fixed regression introduced in 2.0.20 via :ticket:`9600` fix which
        attempted to add more formal typing to
        :paramref:`_schema.MetaData.naming_convention`. This change prevented basic
        naming convention dictionaries from passing typing and has been adjusted so
        that a plain dictionary of strings for keys as well as dictionaries that
        use constraint types as keys or a mix of both, are again accepted.

        As part of this change, lesser used forms of the naming convention
        dictionary are also typed, including that it currently allows for
        ``Constraint`` type objects as keys as well.

    .. change::
        :tags: usecase, typing
        :tickets: 10288

        Made the contained type for :class:`.Mapped` covariant; this is to allow
        greater flexibility for end-user typing scenarios, such as the use of
        protocols to represent particular mapped class structures that are passed
        to other functions. As part of this change, the contained type was also
        made covariant for dependent and related types such as
        :class:`_orm.base.SQLORMOperations`, :class:`_orm.WriteOnlyMapped`, and
        :class:`_sql.SQLColumnExpression`. Pull request courtesy Roméo Després.


    .. change::
        :tags: bug, engine
        :tickets: 10275

        Fixed a series of reflection issues affecting the PostgreSQL,
        MySQL/MariaDB, and SQLite dialects when reflecting foreign key constraints
        where the target column contained parenthesis in one or both of the table
        name or column name.


    .. change::
        :tags: bug, sql
        :tickets: 10280

        The :class:`.Values` construct will now automatically create a proxy (i.e.
        a copy) of a :class:`_sql.column` if the column were already associated
        with an existing FROM clause.  This allows that an expression like
        ``values_obj.c.colname`` will produce the correct FROM clause even in the
        case that ``colname`` was passed as a :class:`_sql.column` that was already
        used with a previous :class:`.Values` or other table construct.
        Originally this was considered to be a candidate for an error condition,
        however it's likely this pattern is already in widespread use so it's
        now added to support.

    .. change::
        :tags: bug, setup
        :tickets: 10321

        Fixed very old issue where the full extent of SQLAlchemy modules, including
        ``sqlalchemy.testing.fixtures``, could not be imported outside of a pytest
        run. This suits inspection utilities such as ``pkgutil`` that attempt to
        import all installed modules in all packages.

    .. change::
        :tags: usecase, sql
        :tickets: 10269

        Adjusted the :class:`_types.Enum` datatype to accept an argument of
        ``None`` for the :paramref:`_types.Enum.length` parameter, resulting in a
        VARCHAR or other textual type with no length in the resulting DDL. This
        allows for new elements of any length to be added to the type after it
        exists in the schema.  Pull request courtesy Eugene Toder.


    .. change::
        :tags: bug, typing
        :tickets: 9878

        Fixed the type annotation for ``__class_getitem__()`` as applied to the
        ``Visitable`` class at the base of expression constructs to accept ``Any``
        for a key, rather than ``str``, which helps with some IDEs such as PyCharm
        when attempting to write typing annotations for SQL constructs which
        include generic selectors.  Pull request courtesy Jordan Macdonald.


    .. change::
        :tags: bug, typing
        :tickets: 10353

        Repaired the core "SQL element" class ``SQLCoreOperations`` to support the
        ``__hash__()`` method from a typing perspective, as objects like
        :class:`.Column` and ORM :class:`.InstrumentedAttribute` are hashable and
        are used as dictionary keys in the public API for the :class:`_dml.Update`
        and :class:`_dml.Insert` constructs.  Previously, type checkers were not
        aware the root SQL element was hashable.

    .. change::
        :tags: bug, typing
        :tickets: 10337

        Fixed typing issue with :meth:`_sql.Existing.select_from` that
        prevented its use with ORM classes.

    .. change::
        :tags: usecase, sql
        :tickets: 9873

        Added new generic SQL function :class:`_functions.aggregate_strings`, which
        accepts a SQL expression and a decimeter, concatenating strings on multiple
        rows into a single aggregate value. The function is compiled on a
        per-backend basis, into functions such as ``group_concat(),``
        ``string_agg()``, or ``LISTAGG()``.
        Pull request courtesy Joshua Morris.

    .. change::
        :tags: typing, bug
        :tickets: 10131

        Update type annotations for ORM loading options, restricting them to accept
        only `"*"` instead of any string for string arguments.  Pull request
        courtesy Janek Nouvertné.

.. changelog::
    :version: 2.0.20
    :released: August 15, 2023

    .. change::
        :tags: bug, orm
        :tickets: 10169

        Fixed issue where the ORM's generation of a SELECT from a joined
        inheritance model with same-named columns in superclass and subclass would
        somehow not send the correct list of column names to the :class:`.CTE`
        construct, when the RECURSIVE column list were generated.


    .. change::
        :tags: bug, typing
        :tickets: 9185

        Typing improvements:

        * :class:`.CursorResult` is returned for some forms of
          :meth:`_orm.Session.execute` where DML without RETURNING is used
        * fixed type for :paramref:`_orm.Query.with_for_update.of` parameter within
          :meth:`_orm.Query.with_for_update`
        * improvements to ``_DMLColumnArgument`` type used by some DML methods to
          pass column expressions
        * Add overload to :func:`_sql.literal` so that it is inferred that the
          return type is ``BindParameter[NullType]`` where
          :paramref:`_sql.literal.type_` param is None
        * Add overloads to :meth:`_sql.ColumnElement.op` so that the inferred
          type when :paramref:`_sql.ColumnElement.op.return_type` is not provided
          is ``Callable[[Any], BinaryExpression[Any]]``
        * Add missing overload to :meth:`_sql.ColumnElement.__add__`

        Pull request courtesy Mehdi Gmira.


    .. change::
        :tags: usecase, orm
        :tickets: 10192

        Implemented the "RETURNING '*'" use case for ORM enabled DML statements.
        This will render in as many cases as possible and return the unfiltered
        result set, however is not supported for multi-parameter "ORM bulk INSERT"
        statements that have specific column rendering requirements.


    .. change::
        :tags: bug, typing
        :tickets: 10182

        Fixed issue in :class:`_orm.Session` and :class:`_asyncio.AsyncSession`
        methods such as :meth:`_orm.Session.connection` where the
        :paramref:`_orm.Session.connection.execution_options` parameter were
        hardcoded to an internal type that is not user-facing.

    .. change::
        :tags: orm, bug
        :tickets: 10231

        Fixed fairly major issue where execution options passed to
        :meth:`_orm.Session.execute`, as well as execution options local to the ORM
        executed statement itself, would not be propagated along to eager loaders
        such as that of :func:`_orm.selectinload`, :func:`_orm.immediateload`, and
        :meth:`_orm.subqueryload`, making it impossible to do things such as
        disabling the cache for a single statement or using
        ``schema_translate_map`` for a single statement, as well as the use of
        user-custom execution options.   A change has been made where **all**
        user-facing execution options present for :meth:`_orm.Session.execute` will
        be propagated along to additional loaders.

        As part of this change, the warning for "excessively deep" eager loaders
        leading to caching being disabled can be silenced on a per-statement
        basis by sending ``execution_options={"compiled_cache": None}`` to
        :meth:`_orm.Session.execute`, which will disable caching for the full
        series of statements within that scope.

    .. change::
        :tags: usecase, asyncio
        :tickets: 9698

        Added new methods :meth:`_asyncio.AsyncConnection.aclose` as a synonym for
        :meth:`_asyncio.AsyncConnection.close` and
        :meth:`_asyncio.AsyncSession.aclose` as a synonym for
        :meth:`_asyncio.AsyncSession.close` to the
        :class:`_asyncio.AsyncConnection` and :class:`_asyncio.AsyncSession`
        objects, to provide compatibility with Python standard library
        ``@contextlib.aclosing`` construct. Pull request courtesy Grigoriev Semyon.

    .. change::
        :tags: bug, orm
        :tickets: 10124

        Fixed issue where internal cloning used by the ORM for expressions like
        :meth:`_orm.relationship.Comparator.any` to produce correlated EXISTS
        constructs would interfere with the "cartesian product warning" feature of
        the SQL compiler, leading the SQL compiler to warn when all elements of the
        statement were correctly joined.

    .. change::
        :tags: orm, bug
        :tickets: 10139

        Fixed issue where the ``lazy="immediateload"`` loader strategy would place
        an internal loading token into the ORM mapped attribute under circumstances
        where the load should not occur, such as in a recursive self-referential
        load.   As part of this change, the ``lazy="immediateload"`` strategy now
        honors the :paramref:`_orm.relationship.join_depth` parameter for
        self-referential eager loads in the same way as that of other eager
        loaders, where leaving it unset or set at zero will lead to a
        self-referential immediateload not occurring, setting it to a value of one
        or greater will immediateload up until that given depth.


    .. change::
        :tags: bug, orm
        :tickets: 10175

        Fixed issue where dictionary-based collections such as
        :func:`_orm.attribute_keyed_dict` did not fully pickle/unpickle correctly,
        leading to issues when attempting to mutate such a collection after
        unpickling.


    .. change::
        :tags: bug, orm
        :tickets: 10125

        Fixed issue where chaining :func:`_orm.load_only` or other wildcard use of
        :func:`_orm.defer` from another eager loader using a :func:`_orm.aliased`
        against a joined inheritance subclass would fail to take effect for columns
        local to the superclass.


    .. change::
        :tags: bug, orm
        :tickets: 10167

        Fixed issue where an ORM-enabled :func:`_sql.select` construct would not
        render any CTEs added only via the :meth:`_sql.Select.add_cte` method that
        were not otherwise referenced in the statement.

    .. change::
        :tags: bug, examples

        The dogpile_caching examples have been updated for 2.0 style queries.
        Within the "caching query" logic itself there is one conditional added to
        differentiate between ``Query`` and ``select()`` when performing an
        invalidation operation.

    .. change::
        :tags: typing, usecase
        :tickets: 10173

        Added new typing only utility functions :func:`.Nullable` and
        :func:`.NotNullable` to type a column or ORM class as, respectively,
        nullable or not nullable.
        These function are no-op at runtime, returning the input unchanged.

    .. change::
        :tags: bug, engine
        :tickets: 10147

        Fixed critical issue where setting
        :paramref:`_sa.create_engine.isolation_level` to ``AUTOCOMMIT`` (as opposed
        to using the :meth:`_engine.Engine.execution_options` method) would fail to
        restore "autocommit" to a pooled connection if an alternate isolation level
        were temporarily selected using
        :paramref:`_engine.Connection.execution_options.isolation_level`.

.. changelog::
    :version: 2.0.19
    :released: July 15, 2023

    .. change::
        :tags: bug, orm
        :tickets: 10089

        Fixed issue where setting a relationship collection directly, where an
        object in the new collection were already present, would not trigger a
        cascade event for that object, leading to it not being added to the
        :class:`_orm.Session` if it were not already present.  This is similar in
        nature to :ticket:`6471` and is a more apparent issue due to the removal of
        ``cascade_backrefs`` in the 2.0 series.  The
        :meth:`_orm.AttributeEvents.append_wo_mutation` event added as part of
        :ticket:`6471` is now also emitted for existing members of a collection
        that are present in a bulk set of that same collection.

    .. change::
        :tags: bug, engine
        :tickets: 10093

        Renamed :attr:`_result.Row.t` and :meth:`_result.Row.tuple` to
        :attr:`_result.Row._t` and :meth:`_result.Row._tuple`; this is to suit the
        policy that all methods and pre-defined attributes on :class:`.Row` should
        be in the style of Python standard library ``namedtuple`` where all fixed
        names have a leading underscore, to avoid name conflicts with existing
        column names.   The previous method/attribute is now deprecated and will
        emit a deprecation warning.

    .. change::
        :tags: bug, postgresql
        :tickets: 10069

        Fixed regression caused by improvements to PostgreSQL URL parsing in
        :ticket:`10004` where "host" query string arguments that had colons in
        them, to support various third party proxy servers and/or dialects, would
        not parse correctly as these were evaluted as ``host:port`` combinations.
        Parsing has been updated to consider a colon as indicating a ``host:port``
        value only if the hostname contains only alphanumeric characters with dots
        or dashes only (e.g. no slashes), followed by exactly one colon followed by
        an all-integer token of zero or more integers.  In all other cases, the
        full string is taken as a host.

    .. change::
        :tags: bug, engine
        :tickets: 10079

        Added detection for non-string, non-:class:`_engine.URL` objects to the
        :func:`_engine.make_url` function, allowing ``ArgumentError`` to be thrown
        immediately, rather than causing failures later on.  Special logic ensures
        that mock forms of :class:`_engine.URL` are allowed through.  Pull request
        courtesy Grigoriev Semyon.

    .. change::
        :tags: bug, orm
        :tickets: 10090

        Fixed issue where objects that were associated with an unloaded collection
        via backref, but were not merged into the :class:`_orm.Session` due to the
        removal of ``cascade_backrefs`` in the 2.0 series, would not emit a warning
        that these objects were not being included in a flush, even though they
        were pending members of the collection; in other such cases, a warning is
        emitted when a collection being flushed contains non-attached objects which
        will be essentially discarded.  The addition of the warning for
        backref-pending collection members establishes greater consistency with
        collections that may be present or non-present and possibly flushed or not
        flushed at different times based on different relationship loading
        strategies.

    .. change::
        :tags: bug, postgresql
        :tickets: 10096

        Fixed issue where comparisons to the :class:`_postgresql.CITEXT` datatype
        would cast the right side to ``VARCHAR``, leading to the right side not
        being interpreted as a ``CITEXT`` datatype, for the asyncpg, psycopg3 and
        pg80000 dialects.   This led to the :class:`_postgresql.CITEXT` type being
        essentially unusable for practical use; this is now fixed and the test
        suite has been corrected to properly assert that expressions are rendered
        correctly.

    .. change::
        :tags: bug, orm, regression
        :tickets: 10098

        Fixed additional regression caused by :ticket:`9805` where more aggressive
        propagation of the "ORM" flag on statements could lead to an internal
        attribute error when embedding an ORM :class:`.Query` construct that
        nonetheless contained no ORM entities within a Core SQL statement, in this
        case ORM-enabled UPDATE and DELETE statements.


.. changelog::
    :version: 2.0.18
    :released: July 5, 2023

    .. change::
        :tags: usecase, typing
        :tickets: 10054

        Improved typing when using standalone operator functions from
        ``sqlalchemy.sql.operators`` such as ``sqlalchemy.sql.operators.eq``.

    .. change::
        :tags: usecase, mariadb, reflection
        :tickets: 10028

        Allowed reflecting :class:`_types.UUID` columns from MariaDB. This allows
        Alembic to properly detect the type of such columns in existing MariaDB
        databases.

    .. change::
        :tags: bug, postgresql
        :tickets: 9945

        Added new parameter ``native_inet_types=False`` to all PostgreSQL
        dialects, which indicates converters used by the DBAPI to
        convert rows from PostgreSQL :class:`.INET` and :class:`.CIDR` columns
        into Python ``ipaddress`` datatypes should be disabled, returning strings
        instead.  This allows code written to work with strings for these datatypes
        to be migrated to asyncpg, psycopg, or pg8000 without code changes
        other than adding this parameter to the :func:`_sa.create_engine`
        or :func:`_asyncio.create_async_engine` function call.

        .. seealso::

            :ref:`postgresql_network_datatypes`

    .. change::
        :tags: usecase, extensions
        :tickets: 10013

        Added new option to :func:`.association_proxy`
        :paramref:`.association_proxy.create_on_none_assignment`; when an
        association proxy which refers to a scalar relationship is assigned the
        value ``None``, and the referenced object is not present, a new object is
        created via the creator.  This was apparently an undefined behavior in the
        1.2 series that was silently removed.

    .. change::
        :tags: bug, typing
        :tickets: 10061

        Fixed some of the typing within the :func:`_orm.aliased` construct to
        correctly accept a :class:`.Table` object that's been aliased with
        :meth:`.Table.alias`, as well as general support for :class:`.FromClause`
        objects to be passed as the "selectable" argument, since this is all
        supported.

    .. change::
        :tags: bug, engine
        :tickets: 10025

        Adjusted the :paramref:`_sa.create_engine.schema_translate_map` feature
        such that **all** schema names in the statement are now tokenized,
        regardless of whether or not a specific name is in the immediate schema
        translate map given, and to fallback to substituting the original name when
        the key is not in the actual schema translate map at execution time.  These
        two changes allow for repeated use of a compiled object with schema
        schema_translate_maps that include or dont include various keys on each
        run, allowing cached SQL constructs to continue to function at runtime when
        schema translate maps with different sets of keys are used each time. In
        addition, added detection of schema_translate_map dictionaries which gain
        or lose a ``None`` key across calls for the same statement, which affects
        compilation of the statement and is not compatible with caching; an
        exception is raised for these scenarios.

    .. change::
        :tags: bug, mssql, sql
        :tickets: 9932

        Fixed issue where performing :class:`.Cast` to a string type with an
        explicit collation would render the COLLATE clause inside the CAST
        function, which resulted in a syntax error.

    .. change::
        :tags: usecase, mssql
        :tickets: 7340

        Added support for creation and reflection of COLUMNSTORE
        indexes in MSSQL dialect. Can be specified on indexes
        specifying ``mssql_columnstore=True``.

    .. change::
        :tags: usecase, postgresql
        :tickets: 10004

        Added multi-host support for the asyncpg dialect.  General improvements and
        error checking added to the PostgreSQL URL routines for the "multihost" use
        case added as well.  Pull request courtesy Ilia Dmitriev.

        .. seealso::

            :ref:`asyncpg_multihost`

.. changelog::
    :version: 2.0.17
    :released: June 23, 2023

    .. change::
        :tags: usecase, postgresql
        :tickets: 9965

        The pg8000 dialect now supports RANGE and MULTIRANGE datatypes, using the
        existing RANGE API described at :ref:`postgresql_ranges`.  Range and
        multirange types are supported in the pg8000 driver from version 1.29.8.
        Pull request courtesy Tony Locke.

    .. change::
        :tags: bug, orm, regression
        :tickets: 9870

        Fixed regression in the 2.0 series where a query that used
        :func:`.undefer_group` with :func:`_orm.selectinload` or
        :func:`_orm.subqueryload` would raise an ``AttributeError``. Pull request
        courtesy of Matthew Martin.

    .. change::
        :tags: bug, orm
        :tickets: 9957

        Fixed issue in ORM Annotated Declarative which prevented a
        :class:`_orm.declared_attr` from being used on a mixin which did not return
        a :class:`.Mapped` datatype, and instead returned a supplemental ORM
        datatype such as :class:`.AssociationProxy`.  The Declarative runtime would
        erroneously try to interpret this annotation as needing to be
        :class:`.Mapped` and raise an error.


    .. change::
        :tags: bug, orm, typing
        :tickets: 9957

        Fixed typing issue where using the :class:`.AssociationProxy` return type
        from a :class:`_orm.declared_attr` function was disallowed.

    .. change::
        :tags: bug, orm, regression
        :tickets: 9936

        Fixed regression introduced in 2.0.16 by :ticket:`9879` where passing a
        callable to the :paramref:`_orm.mapped_column.default` parameter of
        :class:`_orm.mapped_column` while also setting ``init=False`` would
        interpret this value as a Dataclass default value which would be assigned
        directly to new instances of the object directly, bypassing the default
        generator taking place as the :paramref:`_schema.Column.default`
        value generator on the underlying :class:`_schema.Column`.  This condition
        is now detected so that the previous behavior is maintained, however a
        deprecation warning for this ambiguous use is emitted; to populate the
        default generator for a :class:`_schema.Column`, the
        :paramref:`_orm.mapped_column.insert_default` parameter should be used,
        which disambiguates from the :paramref:`_orm.mapped_column.default`
        parameter whose name is fixed as per pep-681.


    .. change::
        :tags: bug, orm
        :tickets: 9973

        Additional hardening and documentation for the ORM :class:`_orm.Session`
        "state change" system, which detects concurrent use of
        :class:`_orm.Session` and :class:`_asyncio.AsyncSession` objects; an
        additional check is added within the process to acquire connections from
        the underlying engine, which is a critical section with regards to internal
        connection management.

    .. change::
        :tags: bug, orm
        :tickets: 10006

        Fixed issue in ORM loader strategy logic which further allows for long
        chains of :func:`_orm.contains_eager` loader options across complex
        inheriting polymorphic / aliased / of_type() relationship chains to take
        proper effect in queries.

    .. change::
        :tags: bug, orm, declarative
        :tickets: 3532

        A warning is emitted when an ORM :func:`_orm.relationship` and other
        :class:`.MapperProperty` objects are assigned to two different class
        attributes at once; only one of the attributes will be mapped.  A warning
        for this condition was already in place for :class:`_schema.Column` and
        :class:`_orm.mapped_column` objects.


    .. change::
        :tags: bug, orm
        :tickets: 9963

        Fixed issue in support for the :class:`.Enum` datatype in the
        :paramref:`_orm.registry.type_annotation_map` first added as part of
        :ticket:`8859` where using a custom :class:`.Enum` with fixed configuration
        in the map would fail to transfer the :paramref:`.Enum.name` parameter,
        which among other issues would prevent PostgreSQL enums from working if the
        enum values were passed as individual values.  Logic has been updated so
        that "name" is transferred over, but also that the default :class:`.Enum`
        which is against the plain Python `enum.Enum` class or other "empty" enum
        won't set a hardcoded name of ``"enum"`` either.

    .. change::
        :tags: bug, typing
        :tickets: 9985

        Fixed typing issue which prevented :class:`_orm.WriteOnlyMapped` and
        :class:`_orm.DynamicMapped` attributes from being used fully within ORM
        queries.

.. changelog::
    :version: 2.0.16
    :released: June 10, 2023

    .. change::
        :tags: usecase, postgresql, reflection
        :tickets: 9838

        Cast ``NAME`` columns to ``TEXT`` when using ``ARRAY_AGG`` in PostgreSQL
        reflection. This seems to improve compatibility with some PostgreSQL
        derivatives that may not support aggregations on the ``NAME`` type.

    .. change::
        :tags: bug, orm
        :tickets: 9862

        Fixed issue where :class:`.DeclarativeBaseNoMeta` declarative base class
        would not function with non-mapped mixins or abstract classes, raising an
        ``AttributeError`` instead.

    .. change::
        :tags: usecase, orm
        :tickets: 9828

        Improved :meth:`.DeferredReflection.prepare` to accept arbitrary ``**kw``
        arguments that are passed to :meth:`_schema.MetaData.reflect`, allowing use
        cases such as reflection of views as well as dialect-specific arguments to
        be passed. Additionally, modernized the
        :paramref:`.DeferredReflection.prepare.bind` argument so that either an
        :class:`.Engine` or :class:`.Connection` are accepted as the "bind"
        argument.

    .. change::
        :tags: usecase, asyncio
        :tickets: 8215

        Added new :paramref:`_asyncio.create_async_engine.async_creator` parameter
        to :func:`.create_async_engine`, which accomplishes the same purpose as the
        :paramref:`.create_engine.creator` parameter of :func:`.create_engine`.
        This is a no-argument callable that provides a new asyncio connection,
        using the asyncio database driver directly. The
        :func:`.create_async_engine` function will wrap the driver-level connection
        in the appropriate structures. Pull request courtesy of Jack Wotherspoon.

    .. change::
        :tags: bug, orm, regression
        :tickets: 9820

        Fixed regression in the 2.0 series where the default value of
        :paramref:`_orm.validates.include_backrefs` got changed to ``False`` for
        the :func:`_orm.validates` function. This default is now restored to
        ``True``.

    .. change::
        :tags: bug, orm
        :tickets: 9917

        Fixed bug in new feature which allows a WHERE clause to be used in
        conjunction with :ref:`orm_queryguide_bulk_update`, added in version 2.0.11
        as part of :ticket:`9583`, where sending dictionaries that did not include
        the primary key values for each row would run through the bulk process and
        include "pk=NULL" for the rows, silently failing.   An exception is now
        raised if primary key values for bulk UPDATE are not supplied.

    .. change::
        :tags: bug, postgresql
        :tickets: 9836

        Use proper precedence on PostgreSQL specific operators, such as ``@>``.
        Previously the precedence was wrong, leading to wrong parenthesis when
        rendering against and ``ANY`` or ``ALL`` construct.

    .. change::
        :tags: bug, orm, dataclasses
        :tickets: 9879

        Fixed an issue where generating dataclasses fields that specified a
        ``default`` value and set ``init=False`` would not work.
        The dataclasses behavior in this case is to set the default
        value on the class, that's not compatible with the descriptors used
        by SQLAlchemy. To support this case the default is transformed to
        a ``default_factory`` when generating the dataclass.

    .. change::
        :tags: bug, orm
        :tickets: 9841

        A deprecation warning is emitted whenever a property is added to a
        :class:`_orm.Mapper` where an ORM mapped property were already configured,
        or an attribute is already present on the class. Previously, there was a
        non-deprecation warning for this case that did not emit consistently. The
        logic for this warning has been improved so that it detects end-user
        replacement of attribute while not having false positives for internal
        Declarative and other cases where replacement of descriptors with new ones
        is expected.

    .. change::
        :tags: bug, postgresql
        :tickets: 9907

        Fixed issue where the :paramref:`.ColumnOperators.like.escape` and similar
        parameters did not allow an empty string as an argument that would be
        passed through as the "escape" character; this is a supported syntax by
        PostgreSQL.  Pull requset courtesy Martin Caslavsky.

    .. change::
        :tags: bug, orm
        :tickets: 9869

        Improved the argument chacking on the
        :paramref:`_orm.registry.map_imperatively.local_table` parameter of the
        :meth:`_orm.registry.map_imperatively` method, ensuring only a
        :class:`.Table` or other :class:`.FromClause` is passed, and not an
        existing mapped class, which would lead to undefined behavior as the object
        were further interpreted for a new mapping.

    .. change::
        :tags: usecase, postgresql
        :tickets: 9041

        Unified the custom PostgreSQL operator definitions, since they are
        shared among multiple different data types.

    .. change::
        :tags: platform, usecase

        Compatibility improvements allowing the complete test suite to pass
        on Python 3.12.0b1.

    .. change::
        :tags: bug, orm
        :tickets: 9913

        The :attr:`_orm.InstanceState.unloaded_expirable` attribute is a synonym
        for :attr:`_orm.InstanceState.unloaded`, and is now deprecated; this
        attribute was always implementation-specific and should not have been
        public.

    .. change::
        :tags: usecase, postgresql
        :tickets: 8240

        Added support for PostgreSQL 10 ``NULLS NOT DISTINCT`` feature of
        unique indexes and unique constraint using the dialect option
        ``postgresql_nulls_not_distinct``.
        Updated the reflection logic to also correctly take this option
        into account.
        Pull request courtesy of Pavel Siarchenia.

.. changelog::
    :version: 2.0.15
    :released: May 19, 2023

    .. change::
        :tags: bug, orm
        :tickets: 9805

        As more projects are using new-style "2.0" ORM querying, it's becoming
        apparent that the conditional nature of "autoflush", being based on whether
        or not the given statement refers to ORM entities, is becoming more of a
        key behavior. Up until now, the "ORM" flag for a statement has been loosely
        based around whether or not the statement returns rows that correspond to
        ORM entities or columns; the original purpose of the "ORM" flag was to
        enable ORM-entity fetching rules which apply post-processing to Core result
        sets as well as ORM loader strategies to the statement.  For statements
        that don't build on rows that contain ORM entities, the "ORM" flag was
        considered to be mostly unnecessary.

        It still may be the case that "autoflush" would be better taking effect for
        *all* usage of :meth:`_orm.Session.execute` and related methods, even for
        purely Core SQL constructs. However, this still could impact legacy cases
        where this is not expected and may be more of a 2.1 thing. For now however,
        the rules for the "ORM-flag" have been opened up so that a statement that
        includes ORM entities or attributes anywhere within, including in the WHERE
        / ORDER BY / GROUP BY clause alone, within scalar subqueries, etc. will
        enable this flag.  This will cause "autoflush" to occur for such statements
        and also be visible via the :attr:`_orm.ORMExecuteState.is_orm_statement`
        event-level attribute.



    .. change::
        :tags: bug, postgresql, regression
        :tickets: 9808

        Repaired the base :class:`.Uuid` datatype for the PostgreSQL dialect to
        make full use of the PG-specific ``UUID`` dialect-specific datatype when
        "native_uuid" is selected, so that PG driver behaviors are included. This
        issue became apparent due to the insertmanyvalues improvement made as part
        of :ticket:`9618`, where in a similar manner as that of :ticket:`9739`, the
        asyncpg driver is very sensitive to datatype casts being present or not,
        and the PostgreSQL driver-specific native ``UUID`` datatype must be invoked
        when this generic type is used so that these casts take place.


.. changelog::
    :version: 2.0.14
    :released: May 18, 2023

    .. change::
        :tags: bug, sql
        :tickets: 9772

        Fixed issue in :func:`_sql.values` construct where an internal compilation
        error would occur if the construct were used inside of a scalar subquery.

    .. change::
        :tags: usecase, sql
        :tickets: 9752


        Generalized the MSSQL :func:`_sql.try_cast` function into the
        ``sqlalchemy.`` import namespace so that it may be implemented by third
        party dialects as well. Within SQLAlchemy, the :func:`_sql.try_cast`
        function remains a SQL Server-only construct that will raise
        :class:`.CompileError` if used with backends that don't support it.

        :func:`_sql.try_cast` implements a CAST where un-castable conversions are
        returned as NULL, instead of raising an error. Theoretically, the construct
        could be implemented by third party dialects for Google BigQuery, DuckDB,
        and Snowflake, and possibly others.

        Pull request courtesy Nick Crews.

    .. change::
        :tags: bug, tests, pypy
        :tickets: 9789

        Fixed test that relied on the ``sys.getsizeof()`` function to not run on
        pypy, where this function appears to have different behavior than it does
        on cpython.

    .. change::
        :tags: bug, orm
        :tickets: 9777

        Modified the ``JoinedLoader`` implementation to use a simpler approach in
        one particular area where it previously used a cached structure that would
        be shared among threads. The rationale is to avoid a potential race
        condition which is suspected of being the cause of a particular crash
        that's been reported multiple times. The cached structure in question is
        still ultimately "cached" via the compiled SQL cache, so a performance
        degradation is not anticipated.

    .. change::
        :tags: bug, orm, regression
        :tickets: 9767

        Fixed regression where use of :func:`_dml.update` or :func:`_dml.delete`
        within a :class:`_sql.CTE` construct, then used in a :func:`_sql.select`,
        would raise a :class:`.CompileError` as a result of ORM related rules for
        performing ORM-level update/delete statements.

    .. change::
        :tags: bug, orm
        :tickets: 9766

        Fixed issue in new ORM Annotated Declarative where using a
        :class:`_schema.ForeignKey` (or other column-level constraint) inside of
        :func:`_orm.mapped_column` which is then copied out to models via pep-593
        ``Annotated`` would apply duplicates of each constraint to the
        :class:`_schema.Column` as produced in the target :class:`_schema.Table`,
        leading to incorrect CREATE TABLE DDL as well as migration directives under
        Alembic.

    .. change::
        :tags: bug, orm
        :tickets: 9779

        Fixed issue where using additional relationship criteria with the
        :func:`_orm.joinedload` loader option, where the additional criteria itself
        contained correlated subqueries that referred to the joined entities and
        therefore also required "adaption" to aliased entities, would be excluded
        from this adaption, producing the wrong ON clause for the joinedload.

    .. change::
        :tags: bug, postgresql
        :tickets: 9773

        Fixed apparently very old issue where the
        :paramref:`_postgresql.ENUM.create_type` parameter, when set to its
        non-default of ``False``, would not be propagated when the
        :class:`_schema.Column` which it's a part of were copied, as is common when
        using ORM Declarative mixins.

.. changelog::
    :version: 2.0.13
    :released: May 10, 2023

    .. change::
        :tags: usecase, asyncio
        :tickets: 9731

        Added a new helper mixin :class:`_asyncio.AsyncAttrs` that seeks to improve
        the use of lazy-loader and other expired or deferred ORM attributes with
        asyncio, providing a simple attribute accessor that provides an ``await``
        interface to any ORM attribute, whether or not it needs to emit SQL.

        .. seealso::

            :class:`_asyncio.AsyncAttrs`

    .. change::
        :tags: bug, orm
        :tickets: 9717

        Fixed issue where ORM Annotated Declarative would not resolve forward
        references correctly in all cases; in particular, when using
        ``from __future__ import annotations`` in combination with Pydantic
        dataclasses.

    .. change::
        :tags: typing, sql
        :tickets: 9656

        Added type :data:`_sql.ColumnExpressionArgument` as a public-facing type
        that indicates column-oriented arguments which are passed to SQLAlchemy
        constructs, such as :meth:`_sql.Select.where`, :func:`_sql.and_` and
        others. This may be used to add typing to end-user functions which call
        these methods.

    .. change::
        :tags: bug, orm
        :tickets: 9746

        Fixed issue in new :ref:`orm_queryguide_upsert_returning` feature where the
        ``populate_existing`` execution option was not being propagated to the
        loading option, preventing existing attributes from being refreshed
        in-place.

    .. change::
        :tags: bug, sql

        Fixed the base class for dialect-specific float/double types; Oracle
        :class:`_oracle.BINARY_DOUBLE` now subclasses :class:`_sqltypes.Double`,
        and internal types for :class:`_sqltypes.Float` for asyncpg and pg8000 now
        correctly subclass :class:`_sqltypes.Float`.

    .. change::
        :tags: bug, ext
        :tickets: 9676

        Fixed issue in :class:`_mutable.Mutable` where event registration for ORM
        mapped attributes would be called repeatedly for mapped inheritance
        subclasses, leading to duplicate events being invoked in inheritance
        hierarchies.

    .. change::
        :tags: bug, orm
        :tickets: 9715

        Fixed loader strategy pathing issues where eager loaders such as
        :func:`_orm.joinedload` / :func:`_orm.selectinload` would fail to traverse
        fully for many-levels deep following a load that had a
        :func:`_orm.with_polymorphic` or similar construct as an interim member.

    .. change::
        :tags: usecase, sql
        :tickets: 9721

        Implemented the "cartesian product warning" for UPDATE and DELETE
        statements, those which include multiple tables that are not correlated
        together in some way.

    .. change::
        :tags: bug, sql

        Fixed issue where :func:`_dml.update` construct that included multiple
        tables and no VALUES clause would raise with an internal error. Current
        behavior for :class:`_dml.Update` with no values is to generate a SQL
        UPDATE statement with an empty "set" clause, so this has been made
        consistent for this specific sub-case.

    .. change::
        :tags: oracle, reflection
        :tickets: 9597

        Added reflection support in the Oracle dialect to expression based indexes
        and the ordering direction of index expressions.

    .. change::
        :tags: performance, schema
        :tickets: 9597

        Improved how table columns are added, avoiding unnecessary allocations,
        significantly speeding up the creation of many table, like when reflecting
        entire schemas.

    .. change::
        :tags: bug, typing
        :tickets: 9762

        Fixed typing for the :paramref:`_orm.Session.get.with_for_update` parameter
        of :meth:`_orm.Session.get` and :meth:`_orm.Session.refresh` (as well as
        corresponding methods on :class:`_asyncio.AsyncSession`) to accept boolean
        ``True`` and all other argument forms accepted by the parameter at runtime.

    .. change::
        :tags: bug, postgresql, regression
        :tickets: 9739

        Fixed another regression due to the "insertmanyvalues" change in 2.0.10 as
        part of :ticket:`9618`, in a similar way as regression :ticket:`9701`, where
        :class:`.LargeBinary` datatypes also need additional casts on when using the
        asyncpg driver specifically in order to work with the new bulk INSERT
        format.

    .. change::
        :tags: bug, orm
        :tickets: 9630

        Fixed issue in :func:`_orm.mapped_column` construct where the correct
        warning for "column X named directly multiple times" would not be emitted
        when ORM mapped attributes referred to the same :class:`_schema.Column`, if
        the :func:`_orm.mapped_column` construct were involved, raising an internal
        assertion instead.

    .. change::
        :tags: bug, asyncio

        Fixed issue in semi-private ``await_only()`` and ``await_fallback()``
        concurrency functions where the given awaitable would remain un-awaited if
        the function threw a ``GreenletError``, which could cause "was not awaited"
        warnings later on if the program continued. In this case, the given
        awaitable is now cancelled before the exception is thrown.

.. changelog::
    :version: 2.0.12
    :released: April 30, 2023

    .. change::
        :tags: bug, mysql, mariadb
        :tickets: 9722

        Fixed issues regarding reflection of comments for :class:`_schema.Table`
        and :class:`_schema.Column` objects, where the comments contained control
        characters such as newlines. Additional testing support for these
        characters as well as extended Unicode characters in table and column
        comments (the latter of which aren't supported by MySQL/MariaDB) added to
        testing overall.

.. changelog::
    :version: 2.0.11
    :released: April 26, 2023

    .. change::
        :tags: bug, engine, regression
        :tickets: 9682

        Fixed regression which prevented the :attr:`_engine.URL.normalized_query`
        attribute of :class:`_engine.URL` from functioning.

    .. change::
        :tags: bug, postgresql, regression
        :tickets: 9701

        Fixed critical regression caused by :ticket:`9618`, which modified the
        architecture of the :term:`insertmanyvalues` feature for 2.0.10, which
        caused floating point values to lose all decimal places when being inserted
        using the insertmanyvalues feature with either the psycopg2 or psycopg
        drivers.


    .. change::
        :tags: bug, mssql

        Implemented the :class:`_sqltypes.Double` type for SQL Server, where it
        will render ``DOUBLE PRECISION`` at DDL time.  This is implemented using
        a new MSSQL datatype :class:`_mssql.DOUBLE_PRECISION` which also may
        be used directly.


    .. change::
        :tags: bug, oracle

        Fixed issue in Oracle dialects where ``Decimal`` returning types such as
        :class:`_sqltypes.Numeric` would return floating point values, rather than
        ``Decimal`` objects, when these columns were used in the
        :meth:`_dml.Insert.returning` clause to return INSERTed values.

    .. change::
        :tags: bug, orm
        :tickets: 9583, 9595

        Fixed 2.0 regression where use of :func:`_sql.bindparam()` inside of
        :meth:`_dml.Insert.values` would fail to be interpreted correctly when
        executing the :class:`_dml.Insert` statement using the ORM
        :class:`_orm.Session`, due to the new
        :ref:`ORM-enabled insert feature <orm_queryguide_bulk_insert>` not
        implementing this use case.

    .. change::
        :tags: usecase, orm
        :tickets: 9583, 9595

        The :ref:`ORM bulk INSERT and UPDATE <orm_expression_update_delete>`
        features now add these capabilities:

        * The requirement that extra parameters aren't passed when using ORM
          INSERT using the "orm" dml_strategy setting is lifted.
        * The requirement that additional WHERE criteria is not passed when using
          ORM UPDATE using the "bulk" dml_strategy setting is lifted.  Note that
          in this case, the check for expected row count is turned off.

    .. change::
        :tags: usecase, sql
        :tickets: 8285

        Added support for slice access with :class:`.ColumnCollection`, e.g.
        ``table.c[0:5]``, ``subquery.c[:-1]`` etc. Slice access returns a sub
        :class:`.ColumnCollection` in the same way as passing a tuple of keys. This
        is a natural continuation of the key-tuple access added for :ticket:`8285`,
        where it appears to be an oversight that the slice access use case was
        omitted.

    .. change::
        :tags: bug, typing
        :tickets: 9644

        Improved typing of :class:`_engine.RowMapping` to indicate that it
        support also :class:`_schema.Column` as index objects, not only
        string names. Pull request courtesy Andy Freeland.

    .. change::
        :tags: engine, performance
        :tickets: 9678, 9680

        A series of performance enhancements to :class:`_engine.Row`:

        * ``__getattr__`` performance of the row's "named tuple" interface has
          been improved; within this change, the :class:`_engine.Row`
          implementation has been streamlined, removing constructs and logic
          that were specific to the 1.4 and prior series of SQLAlchemy.
          As part of this change, the serialization format of :class:`_engine.Row`
          has been modified slightly, however rows which were pickled with previous
          SQLAlchemy 2.0 releases will be recognized within the new format.
          Pull request courtesy J. Nick Koston.

        * Improved row processing performance for "binary" datatypes by making the
          "bytes" handler conditional on a per driver basis.  As a result, the
          "bytes" result handler has been removed for nearly all drivers other than
          psycopg2, all of which in modern forms support returning Python "bytes"
          directly.  Pull request courtesy J. Nick Koston.

        * Additional refactorings inside of :class:`_engine.Row` to improve
          performance by Federico Caselli.




.. changelog::
    :version: 2.0.10
    :released: April 21, 2023

    .. change::
        :tags: bug, typing
        :tickets: 9650

        Added typing information for recently added operators
        :meth:`.ColumnOperators.icontains`, :meth:`.ColumnOperators.istartswith`,
        :meth:`.ColumnOperators.iendswith`, and bitwise operators
        :meth:`.ColumnOperators.bitwise_and`, :meth:`.ColumnOperators.bitwise_or`,
        :meth:`.ColumnOperators.bitwise_xor`, :meth:`.ColumnOperators.bitwise_not`,
        :meth:`.ColumnOperators.bitwise_lshift`
        :meth:`.ColumnOperators.bitwise_rshift`. Pull request courtesy Martijn
        Pieters.


    .. change::
        :tags: bug, oracle

        Fixed issue where the :class:`_sqltypes.Uuid` datatype could not be used in
        an INSERT..RETURNING clause with the Oracle dialect.

    .. change::
        :tags: usecase, engine
        :tickets: 9613

        Added :func:`_sa.create_pool_from_url` and
        :func:`_asyncio.create_async_pool_from_url` to create
        a :class:`_pool.Pool` instance from an input url passed as string
        or :class:`_sa.URL`.

    .. change::
        :tags: bug, engine
        :tickets: 9618, 9603

        Repaired a major shortcoming which was identified in the
        :ref:`engine_insertmanyvalues` performance optimization feature first
        introduced in the 2.0 series. This was a continuation of the change in
        2.0.9 which disabled the SQL Server version of the feature due to a
        reliance in the ORM on apparent row ordering that is not guaranteed to take
        place. The fix applies new logic to all "insertmanyvalues" operations,
        which takes effect when a new parameter
        :paramref:`_dml.Insert.returning.sort_by_parameter_order` on the
        :meth:`_dml.Insert.returning` or :meth:`_dml.UpdateBase.return_defaults`
        methods, that through a combination of alternate SQL forms, direct
        correspondence of client side parameters, and in some cases downgrading to
        running row-at-a-time, will apply sorting to each batch of returned rows
        using correspondence to primary key or other unique values in each row
        which can be correlated to the input data.

        Performance impact is expected to be minimal as nearly all common primary
        key scenarios are suitable for parameter-ordered batching to be
        achieved for all backends other than SQLite, while "row-at-a-time"
        mode operates with a bare minimum of Python overhead compared to the very
        heavyweight approaches used in the 1.x series. For SQLite, there is no
        difference in performance when "row-at-a-time" mode is used.

        It's anticipated that with an efficient "row-at-a-time" INSERT with
        RETURNING batching capability, the "insertmanyvalues" feature can be later
        be more easily generalized to third party backends that include RETURNING
        support but not necessarily easy ways to guarantee a correspondence
        with parameter order.

        .. seealso::

            :ref:`engine_insertmanyvalues_returning_order`


    .. change::
        :tags: bug, mssql
        :tickets: 9618, 9603

        Restored the :term:`insertmanyvalues` feature for Microsoft SQL Server.
        This feature was disabled in version 2.0.9 due to an apparent reliance
        on the ordering of RETURNING that is not guaranteed.   The architecture of
        the "insertmanyvalues" feature has been reworked to accommodate for
        specific organizations of INSERT statements and result row handling that
        can guarantee the correspondence of returned rows to input records.

        .. seealso::

          :ref:`engine_insertmanyvalues_returning_order`


    .. change::
        :tags: usecase, postgresql
        :tickets: 9608

        Added ``prepared_statement_name_func`` connection argument option in the
        asyncpg dialect. This option allows passing a callable used to customize
        the name of the prepared statement that will be created by the driver
        when executing queries.  Pull request courtesy Pavel Sirotkin.

        .. seealso::

            :ref:`asyncpg_prepared_statement_name`

    .. change::
        :tags: typing, bug

        Updates to the codebase to pass typing with Mypy 1.2.0.

    .. change::
        :tags: bug, typing
        :tickets: 9669

        Fixed typing issue where :meth:`_orm.PropComparator.and_` expressions would
        not be correctly typed inside of loader options such as
        :func:`_orm.selectinload`.

    .. change::
        :tags: bug, orm
        :tickets: 9625

        Fixed issue where the :meth:`_orm.declared_attr.directive` modifier was not
        correctly honored for subclasses when applied to the ``__mapper_args__``
        special method name, as opposed to direct use of
        :class:`_orm.declared_attr`. The two constructs should have identical
        runtime behaviors.

    .. change::
        :tags: bug, postgresql
        :tickets: 9611

        Restored the :paramref:`_postgresql.ENUM.name` parameter as optional in the
        signature for :class:`_postgresql.ENUM`, as this is chosen automatically
        from a given pep-435 ``Enum`` type.


    .. change::
        :tags: bug, postgresql
        :tickets: 9621

        Fixed issue where the comparison for :class:`_postgresql.ENUM` against a
        plain string would cast that right-hand side type as VARCHAR, which due to
        more explicit casting added to dialects such as asyncpg would produce a
        PostgreSQL type mismatch error.


    .. change::
        :tags: bug, orm
        :tickets: 9635

        Made an improvement to the :func:`_orm.with_loader_criteria` loader option
        to allow it to be indicated in the :meth:`.Executable.options` method of a
        top-level statement that is not itself an ORM statement. Examples include
        :func:`_sql.select` that's embedded in compound statements such as
        :func:`_sql.union`, within an :meth:`_dml.Insert.from_select` construct, as
        well as within CTE expressions that are not ORM related at the top level.

    .. change::
        :tags: bug, orm
        :tickets: 9685

        Fixed bug in ORM bulk insert feature where additional unnecessary columns
        would be rendered in the INSERT statement if RETURNING of individual columns
        were requested.

    .. change::
        :tags: bug, postgresql
        :tickets: 9615

        Fixed issue that prevented reflection of expression based indexes
        with long expressions in PostgreSQL. The expression where erroneously
        truncated to the identifier length (that's 63 bytes by default).

    .. change::
          :tags: usecase, postgresql
          :tickets: 9509

          Add missing :meth:`_postgresql.Range.intersection` method.
          Pull request courtesy Yurii Karabas.

    .. change::
        :tags: bug, orm
        :tickets: 9628

        Fixed bug in ORM Declarative Dataclasses where the
        :func:`_orm.query_expression` and :func:`_orm.column_property`
        constructs, which are documented as read-only constructs in the context of
        a Declarative mapping, could not be used with a
        :class:`_orm.MappedAsDataclass` class without adding ``init=False``, which
        in the case of :func:`_orm.query_expression` was not possible as no
        ``init`` parameter was included. These constructs have been modified from a
        dataclass perspective to be assumed to be "read only", setting
        ``init=False`` by default and no longer including them in the pep-681
        constructor. The dataclass parameters for :func:`_orm.column_property`
        ``init``, ``default``, ``default_factory``, ``kw_only`` are now deprecated;
        these fields don't apply to :func:`_orm.column_property` as used in a
        Declarative dataclasses configuration where the construct would be
        read-only. Also added read-specific parameter
        :paramref:`_orm.query_expression.compare` to
        :func:`_orm.query_expression`; :paramref:`_orm.query_expression.repr`
        was already present.



    .. change::
        :tags: bug, orm

        Added missing :paramref:`_orm.mapped_column.active_history` parameter
        to :func:`_orm.mapped_column` construct.

.. changelog::
    :version: 2.0.9
    :released: April 5, 2023

    .. change::
        :tags: bug, mssql
        :tickets: 9603

        The SQLAlchemy "insertmanyvalues" feature which allows fast INSERT of
        many rows while also supporting RETURNING is temporarily disabled for
        SQL Server. As the unit of work currently relies upon this feature such
        that it matches existing ORM objects to returned primary key
        identities, this particular use pattern does not work with SQL Server
        in all cases as the order of rows returned by "OUTPUT inserted" may not
        always match the order in which the tuples were sent, leading to
        the ORM making the wrong decisions about these objects in subsequent
        operations.

        The feature will be re-enabled in an upcoming release and will again
        take effect for multi-row INSERT statements, however the unit-of-work's
        use of the feature will be disabled, possibly for all dialects, unless
        ORM-mapped tables also include a "sentinel" column so that the
        returned rows can be referenced back to the original data passed in.


    .. change::
        :tags: bug, mariadb
        :tickets: 9588

        Added ``row_number`` as reserved word in MariaDb.

    .. change::
        :tags: bug, mssql
        :tickets: 9586

        Changed the bulk INSERT strategy used for SQL Server "executemany" with
        pyodbc when ``fast_executemany`` is set to ``True`` by using
        ``fast_executemany`` / ``cursor.executemany()`` for bulk INSERT that does
        not include RETURNING, restoring the same behavior as was used in
        SQLAlchemy 1.4 when this parameter is set.

        New performance details from end users have shown that ``fast_executemany``
        is still much faster for very large datasets as it uses ODBC commands that
        can receive all rows in a single round trip, allowing for much larger
        datasizes than the batches that can be sent by "insertmanyvalues"
        as was implemented for SQL Server.

        While this change was made such that "insertmanyvalues" continued to be
        used for INSERT that includes RETURNING, as well as if ``fast_executemany``
        were not set, due to :ticket:`9603`, the "insertmanyvalues" strategy has
        been disabled for SQL Server across the board in any case.

.. changelog::
    :version: 2.0.8
    :released: March 31, 2023

    .. change::
        :tags: bug, orm
        :tickets: 9553

        Fixed issue in ORM Annotated Declarative where using a recursive type (e.g.
        using a nested Dict type) would result in a recursion overflow in the ORM's
        annotation resolution logic, even if this datatype were not necessary to
        map the column.

    .. change::
        :tags: bug, examples

        Fixed issue in "versioned history" example where using a declarative base
        that is derived from :class:`_orm.DeclarativeBase` would fail to be mapped.
        Additionally, repaired the given test suite so that the documented
        instructions for running the example using Python unittest now work again.

    .. change::
        :tags: bug, orm
        :tickets: 9550

        Fixed issue where the :func:`_orm.mapped_column` construct would raise an
        internal error if used on a Declarative mixin and included the
        :paramref:`_orm.mapped_column.deferred` parameter.

    .. change::
        :tags: bug, mysql
        :tickets: 9544

        Fixed issue where string datatypes such as :class:`_sqltypes.CHAR`,
        :class:`_sqltypes.VARCHAR`, :class:`_sqltypes.TEXT`, as well as binary
        :class:`_sqltypes.BLOB`, could not be produced with an explicit length of
        zero, which has special meaning for MySQL. Pull request courtesy J. Nick
        Koston.

    .. change::
        :tags: bug, orm
        :tickets: 9537

        Expanded the warning emitted when a plain :func:`_sql.column` object is
        present in a Declarative mapping to include any arbitrary SQL expression
        that is not declared within an appropriate property type such as
        :func:`_orm.column_property`, :func:`_orm.deferred`, etc. These attributes
        are otherwise not mapped at all and remain unchanged within the class
        dictionary. As it seems likely that such an expression is usually not
        what's intended, this case now warns for all such otherwise ignored
        expressions, rather than just the :func:`_sql.column` case.

    .. change::
        :tags: bug, orm
        :tickets: 9519

        Fixed regression where accessing the expression value of a hybrid property
        on a class that was either unmapped or not-yet-mapped (such as calling upon
        it within a :func:`_orm.declared_attr` method) would raise an internal
        error, as an internal fetch for the parent class' mapper would fail and an
        instruction for this failure to be ignored were inadvertently removed in
        2.0.

    .. change::
        :tags: bug, orm
        :tickets: 9350

        Fields that are declared on Declarative Mixins and then combined with
        classes that make use of :class:`_orm.MappedAsDataclass`, where those mixin
        fields are not themselves part of a dataclass, now emit a deprecation
        warning as these fields will be ignored in a future release, as Python
        dataclasses behavior is to ignore these fields. Type checkers will not see
        these fields under pep-681.

        .. seealso::

            :ref:`error_dcmx` - background on rationale

            :ref:`orm_declarative_dc_mixins`

    .. change::
        :tags: bug, postgresql
        :tickets: 9511

        Fixed critical regression in PostgreSQL dialects such as asyncpg which rely
        upon explicit casts in SQL in order for datatypes to be passed to the
        driver correctly, where a :class:`.String` datatype would be cast along
        with the exact column length being compared, leading to implicit truncation
        when comparing a ``VARCHAR`` of a smaller length to a string of greater
        length regardless of operator in use (e.g. LIKE, MATCH, etc.). The
        PostgreSQL dialect now omits the length from ``VARCHAR`` when rendering
        these casts.

    .. change::
        :tags: bug, util
        :tickets: 9487

        Implemented missing methods ``copy`` and ``pop`` in
        OrderedSet class.

    .. change::
        :tags: bug, typing
        :tickets: 9536

        Fixed typing for :func:`_orm.deferred` and :func:`_orm.query_expression`
        to work correctly with 2.0 style mappings.

    .. change::
        :tags: bug, orm
        :tickets: 9526

        Fixed issue where the :meth:`_sql.BindParameter.render_literal_execute`
        method would fail when called on a parameter that also had ORM annotations
        associated with it. In practice, this would be observed as a failure of SQL
        compilation when using some combinations of a dialect that uses "FETCH
        FIRST" such as Oracle along with a :class:`_sql.Select` construct that uses
        :meth:`_sql.Select.limit`, within some ORM contexts, including if the
        statement were embedded within a relationship primaryjoin expression.


    .. change::
        :tags: usecase, orm
        :tickets: 9563

        Exceptions such as ``TypeError`` and ``ValueError`` raised by Python
        dataclasses when making use of the :class:`_orm.MappedAsDataclass` mixin
        class or :meth:`_orm.registry.mapped_as_dataclass` decorator are now
        wrapped within an :class:`.InvalidRequestError` wrapper along with
        informative context about the error message, referring to the Python
        dataclasses documentation as the authoritative source of background
        information on the cause of the exception.

        .. seealso::

            :ref:`error_dcte`


    .. change::
        :tags: bug, orm
        :tickets: 9549

        Towards maintaining consistency with unit-of-work changes made for
        :ticket:`5984` and :ticket:`8862`, both of which disable "lazy='raise'"
        handling within :class:`_orm.Session` processes that aren't triggered by
        attribute access, the :meth:`_orm.Session.delete` method will now also
        disable "lazy='raise'" handling when it traverses relationship paths in
        order to process the "delete" and "delete-orphan" cascade rules.
        Previously, there was no easy way to generically call
        :meth:`_orm.Session.delete` on an object that had "lazy='raise'" set up
        such that only the necessary relationships would be loaded. As
        "lazy='raise'" is primarily intended to catch SQL loading that emits on
        attribute access, :meth:`_orm.Session.delete` is now made to behave like
        other :class:`_orm.Session` methods including :meth:`_orm.Session.merge` as
        well as :meth:`_orm.Session.flush` along with autoflush.

    .. change::
        :tags: bug, orm
        :tickets: 9564

        Fixed issue where an annotation-only :class:`_orm.Mapped` directive could
        not be used in a Declarative mixin class, without that attribute attempting
        to take effect for single- or joined-inheritance subclasses of mapped
        classes that had already mapped that attribute on a superclass, producing
        conflicting column errors and/or warnings.


    .. change::
        :tags: bug, orm, typing
        :tickets: 9514

        Properly type :paramref:`_dml.Insert.from_select.names` to accept
        a list of string or columns or mapped attributes.

.. changelog::
    :version: 2.0.7
    :released: March 18, 2023

    .. change::
        :tags: usecase, postgresql
        :tickets: 9416

        Added new PostgreSQL type :class:`_postgresql.CITEXT`. Pull request
        courtesy Julian David Rath.

    .. change::
        :tags: bug, typing
        :tickets: 9502

        Fixed typing issue where :func:`_orm.composite` would not allow an
        arbitrary callable as the source of the composite class.

    .. change::
          :tags: usecase, postgresql
          :tickets: 9442

          Modifications to the base PostgreSQL dialect to allow for better integration with the
          sqlalchemy-redshift third party dialect for SQLAlchemy 2.0. Pull request courtesy
          matthewgdv.

.. changelog::
    :version: 2.0.6
    :released: March 13, 2023

    .. change::
        :tags: bug, sql, regression
        :tickets: 9461

        Fixed regression where the fix for :ticket:`8098`, which was released in
        the 1.4 series and provided a layer of concurrency-safe checks for the
        lambda SQL API, included additional fixes in the patch that failed to be
        applied to the main branch. These additional fixes have been applied.

    .. change::
        :tags: bug, typing
        :tickets: 9451

        Fixed typing issue where :meth:`.ColumnElement.cast` did not allow a
        :class:`.TypeEngine` argument independent of the type of the
        :class:`.ColumnElement` itself, which is the purpose of
        :meth:`.ColumnElement.cast`.

    .. change::
        :tags: bug, orm
        :tickets: 9460

        Fixed bug where the "active history" feature was not fully
        implemented for composite attributes, making it impossible to receive
        events that included the "old" value.   This seems to have been the case
        with older SQLAlchemy versions as well, where "active_history" would
        be propagated to the underlying column-based attributes, but an event
        handler listening to the composite attribute itself would not be given
        the "old" value being replaced, even if the composite() were set up
        with active_history=True.

        Additionally, fixed a regression that's local to 2.0 which disallowed
        active_history on composite from being assigned to the impl with
        ``attr.impl.active_history=True``.


    .. change::
        :tags: bug, oracle
        :tickets: 9459

        Fixed reflection bug where Oracle "name normalize" would not work correctly
        for reflection of symbols that are in the "PUBLIC" schema, such as
        synonyms, meaning the PUBLIC name could not be indicated as lower case on
        the Python side for the :paramref:`_schema.Table.schema` argument. Using
        uppercase "PUBLIC" would work, but would then lead to awkward SQL queries
        including a quoted ``"PUBLIC"`` name as well as indexing the table under
        uppercase "PUBLIC", which was inconsistent.

    .. change::
        :tags: bug, typing

        Fixed issues to allow typing tests to pass under Mypy 1.1.1.

    .. change::
        :tags: bug, sql
        :tickets: 9440

        Fixed regression where the :func:`_sql.select` construct would not be able
        to render if it were given no columns and then used in the context of an
        EXISTS, raising an internal exception instead. While an empty "SELECT" is
        not typically valid SQL, in the context of EXISTS databases such as
        PostgreSQL allow it, and in any case the condition now no longer raises
        an internal exception.


    .. change::
        :tags: bug, orm
        :tickets: 9418

        Fixed regression involving pickling of Python rows between the cython and
        pure Python implementations of :class:`.Row`, which occurred as part of
        refactoring code for version 2.0 with typing. A particular constant were
        turned into a string based ``Enum`` for the pure Python version of
        :class:`.Row` whereas the cython version continued to use an integer
        constant, leading to deserialization failures.

.. changelog::
    :version: 2.0.5.post1
    :released: March 5, 2023

    .. change::
        :tags: bug, orm
        :tickets: 9418

        Added constructor arguments to the built-in mapping collection types
        including :class:`.KeyFuncDict`, :func:`_orm.attribute_keyed_dict`,
        :func:`_orm.column_keyed_dict` so that these dictionary types may be
        constructed in place given the data up front; this provides further
        compatibility with tools such as Python dataclasses ``.asdict()`` which
        relies upon invoking these classes directly as ordinary dictionary classes.

    .. change::
        :tags: bug, orm, regression
        :tickets: 9424

        Fixed multiple regressions due to :ticket:`8372`, involving
        :func:`_orm.attribute_mapped_collection` (now called
        :func:`_orm.attribute_keyed_dict`).

        First, the collection was no longer usable with "key" attributes that were
        not themselves ordinary mapped attributes; attributes linked to descriptors
        and/or association proxy attributes have been fixed.

        Second, if an event or other operation needed access to the "key" in order
        to populate the dictionary from an mapped attribute that was not
        loaded, this also would raise an error inappropriately, rather than
        trying to load the attribute as was the behavior in 1.4.  This is also
        fixed.

        For both cases, the behavior of :ticket:`8372` has been expanded.
        :ticket:`8372` introduced an error that raises when the derived key that
        would be used as a mapped dictionary key is effectively unassigned. In this
        change, a warning only is emitted if the effective value of the ".key"
        attribute is ``None``, where it cannot be unambiguously determined if this
        ``None`` was intentional or not. ``None`` will be not supported as mapped
        collection dictionary keys going forward (as it typically refers to NULL
        which means "unknown"). Setting
        :paramref:`_orm.attribute_keyed_dict.ignore_unpopulated_attribute` will now
        cause such ``None`` keys to be ignored as well.

    .. change::
        :tags: engine, performance
        :tickets: 9343

        A small optimization to the Cython implementation of :class:`.Result`
        using a cdef for a particular int value to avoid Python overhead. Pull
        request courtesy Matus Valo.


    .. change::
        :tags: bug, mssql
        :tickets: 9414

        Fixed issue in the new :class:`.Uuid` datatype which prevented it from
        working with the pymssql driver. As pymssql seems to be maintained again,
        restored testing support for pymssql.

    .. change::
        :tags: bug, mssql

        Tweaked the pymssql dialect to take better advantage of
        RETURNING for INSERT statements in order to retrieve last inserted primary
        key values, in the same way as occurs for the mssql+pyodbc dialect right
        now.

    .. change::
        :tags: bug, orm

        Identified that the ``sqlite`` and ``mssql+pyodbc`` dialects are now
        compatible with the SQLAlchemy ORM's "versioned rows" feature, since
        SQLAlchemy now computes rowcount for a RETURNING statement in this specific
        case by counting the rows returned, rather than relying upon
        ``cursor.rowcount``.  In particular, the ORM versioned rows use case
        (documented at :ref:`mapper_version_counter`) should now be fully
        supported with the SQL Server pyodbc dialect.


    .. change::
        :tags: bug, postgresql
        :tickets: 9349

        Fixed issue in PostgreSQL :class:`_postgresql.ExcludeConstraint` where
        literal values were being compiled as bound parameters and not direct
        inline values as is required for DDL.

    .. change::
        :tags: bug, typing

        Fixed bug where the :meth:`_engine.Connection.scalars` method was not typed
        as allowing a multiple-parameters list, which is now supported using
        insertmanyvalues operations.

    .. change::
        :tags: bug, typing
        :tickets: 9376

        Improved typing for the mapping passed to :meth:`.Insert.values` and
        :meth:`.Update.values` to be more open-ended about collection type, by
        indicating read-only ``Mapping`` instead of writeable ``Dict`` which would
        error out on too limited of a key type.

    .. change::
        :tags: schema

        Validate that when provided the :paramref:`_schema.MetaData.schema`
        argument of :class:`_schema.MetaData` is a string.

    .. change::
        :tags: typing, usecase
        :tickets: 9338

        Exported the type returned by
        :meth:`_orm.scoped_session.query_property` using a new public type
        :class:`.orm.QueryPropertyDescriptor`.

    .. change::
        :tags: bug, mysql, postgresql
        :tickets: 5648

        The support for pool ping listeners to receive exception events via the
        :meth:`.DialectEvents.handle_error` event added in 2.0.0b1 for
        :ticket:`5648` failed to take into account dialect-specific ping routines
        such as that of MySQL and PostgreSQL. The dialect feature has been reworked
        so that all dialects participate within event handling.   Additionally,
        a new boolean element :attr:`.ExceptionContext.is_pre_ping` is added
        which identifies if this operation is occurring within the pre-ping
        operation.

        For this release, third party dialects which implement a custom
        :meth:`_engine.Dialect.do_ping` method can opt in to the newly improved
        behavior by having their method no longer catch exceptions or check
        exceptions for "is_disconnect", instead just propagating all exceptions
        outwards. Checking the exception for "is_disconnect" is now done by an
        enclosing method on the default dialect, which ensures that the event hook
        is invoked for all exception scenarios before testing the exception as a
        "disconnect" exception. If an existing ``do_ping()`` method continues to
        catch exceptions and check "is_disconnect", it will continue to work as it
        did previously, but ``handle_error`` hooks will not have access to the
        exception if it isn't propagated outwards.

    .. change::
        :tags: bug, ext
        :tickets: 9367

        Fixed issue in automap where calling :meth:`_automap.AutomapBase.prepare`
        from a specific mapped class, rather than from the
        :class:`_automap.AutomapBase` directly, would not use the correct base
        class when automap detected new tables, instead using the given class,
        leading to mappers trying to configure inheritance. While one should
        normally call :meth:`_automap.AutomapBase.prepare` from the base in any
        case, it shouldn't misbehave that badly when called from a subclass.


    .. change::
        :tags: bug, sqlite, regression
        :tickets: 9379

        Fixed regression for SQLite connections where use of the ``deterministic``
        parameter when establishing database functions would fail for older SQLite
        versions, those prior to version 3.8.3. The version checking logic has been
        improved to accommodate for this case.

    .. change::
        :tags: bug, typing
        :tickets: 9391

        Added missing init overload to the :class:`_types.Numeric` type object so
        that pep-484 type checkers may properly resolve the complete type, deriving
        from the :paramref:`_types.Numeric.asdecimal` parameter whether ``Decimal``
        or ``float`` objects will be represented.

    .. change::
        :tags: bug, typing
        :tickets: 9398

        Fixed typing bug where :meth:`_sql.Select.from_statement` would not accept
        :func:`_sql.text` or :class:`.TextualSelect` objects as a valid type.
        Additionally repaired the :class:`.TextClause.columns` method to have a
        return type, which was missing.

    .. change::
        :tags: bug, orm declarative
        :tickets: 9332

        Fixed issue where new :paramref:`_orm.mapped_column.use_existing_column`
        feature would not work if the two same-named columns were mapped under
        attribute names that were differently-named from an explicit name given to
        the column itself. The attribute names can now be differently named when
        using this parameter.

    .. change::
        :tags: bug, orm
        :tickets: 9373

        Added support for the :paramref:`_orm.Mapper.polymorphic_load` parameter to
        be applied to each mapper in an inheritance hierarchy more than one level
        deep, allowing columns to load for all classes in the hierarchy that
        indicate ``"selectin"`` using a single statement, rather than ignoring
        elements on those intermediary classes that nonetheless indicate they also
        would participate in ``"selectin"`` loading and were not part of the
        base-most SELECT statement.

    .. change::
        :tags: bug, orm
        :tickets: 8853, 9335

        Continued the fix for :ticket:`8853`, allowing the :class:`_orm.Mapped`
        name to be fully qualified regardless of whether or not
        ``from __annotations__ import future`` were present. This issue first fixed
        in 2.0.0b3 confirmed that this case worked via the test suite, however the
        test suite apparently was not testing the behavior for the name
        :class:`_orm.Mapped` not being locally present at all; string resolution
        has been updated to ensure the :class:`_orm.Mapped` symbol is locatable as
        applies to how the ORM uses these functions.

    .. change::
        :tags: bug, typing
        :tickets: 9340

        Fixed typing issue where :func:`_orm.with_polymorphic` would not
        record the class type correctly.

    .. change::
        :tags: bug, ext, regression
        :tickets: 9380

        Fixed regression caused by typing added to ``sqlalchemy.ext.mutable`` for
        :ticket:`8667`, where the semantics of the ``.pop()`` method changed such
        that the method was non-working. Pull request courtesy Nils Philippsen.

    .. change::
        :tags: bug, sql, regression
        :tickets: 9390

        Restore the :func:`.nullslast` and :func:`.nullsfirst` legacy functions
        into the ``sqlalchemy`` import namespace. Previously, the newer
        :func:`.nulls_last` and :func:`.nulls_first` functions were available, but
        the legacy ones were inadvertently removed.

    .. change::
        :tags: bug, postgresql
        :tickets: 9401

        Fixed issue where the PostgreSQL :class:`_postgresql.ExcludeConstraint`
        construct would not be copyable within operations such as
        :meth:`_schema.Table.to_metadata` as well as within some Alembic scenarios,
        if the constraint contained textual expression elements.

    .. change::
        :tags: bug, engine
        :tickets: 9423

        Fixed bug where :class:`_engine.Row` objects could not be reliably unpickled
        across processes due to an accidental reliance on an unstable hash value.

.. changelog::
    :version: 2.0.4
    :released: February 17, 2023

    .. change::
        :tags: bug, orm, regression
        :tickets: 9273

        Fixed regression introduced in version 2.0.2 due to :ticket:`9217` where
        using DML RETURNING statements, as well as
        :meth:`_sql.Select.from_statement` constructs as was "fixed" in
        :ticket:`9217`, in conjunction with ORM mapped classes that used
        expressions such as with :func:`_orm.column_property`, would lead to an
        internal error within Core where it would attempt to match the expression
        by name. The fix repairs the Core issue, and also adjusts the fix in
        :ticket:`9217` to not take effect for the DML RETURNING use case, where it
        adds unnecessary overhead.

    .. change::
        :tags: usecase, typing
        :tickets: 9321

        Improved the typing support for the :ref:`hybrids_toplevel`
        extension, updated all documentation to use ORM Annotated Declarative
        mappings, and added a new modifier called :attr:`.hybrid_property.inplace`.
        This modifier provides a way to alter the state of a :class:`.hybrid_property`
        **in place**, which is essentially what very early versions of hybrids
        did, before SQLAlchemy version 1.2.0 :ticket:`3912` changed this to
        remove in-place mutation.  This in-place mutation is now restored on an
        **opt-in** basis to allow a single hybrid to have multiple methods
        set up, without the need to name all the methods the same and without the
        need to carefully "chain" differently-named methods in order to maintain
        the composition.  Typing tools such as Mypy and Pyright do not allow
        same-named methods on a class, so with this change a succinct method
        of setting up hybrids with typing support is restored.

        .. seealso::

            :ref:`hybrid_pep484_naming`

    .. change::
        :tags: bug, orm

        Marked the internal ``EvaluatorCompiler`` module as private to the ORM, and
        renamed it to ``_EvaluatorCompiler``. For users that may have been relying
        upon this, the name ``EvaluatorCompiler`` is still present, however this
        use is not supported and will be removed in a future release.

    .. change::
        :tags: orm, usecase
        :tickets: 9297

        To accommodate a change in column ordering used by ORM Declarative in
        SQLAlchemy 2.0, a new parameter :paramref:`_orm.mapped_column.sort_order`
        has been added that can be used to control the order of the columns defined
        in the table by the ORM, for common use cases such as mixins with primary
        key columns that should appear first in tables. The change notes at
        :ref:`change_9297` illustrate the default change in ordering behavior
        (which is part of all SQLAlchemy 2.0 releases) as well as use of the
        :paramref:`_orm.mapped_column.sort_order` to control column ordering when
        using mixins and multiple classes (new in 2.0.4).

        .. seealso::

            :ref:`change_9297`

    .. change::
        :tags: sql
        :tickets: 9277

        Added public property :attr:`_schema.Table.autoincrement_column` that
        returns the column identified as autoincrementing in the column.

    .. change::
        :tags: oracle, bug
        :tickets: 9295

        Adjusted the behavior of the ``thick_mode`` parameter for the
        :ref:`oracledb` dialect to correctly accept ``False`` as a value.
        Previously, only ``None`` would indicate that thick mode should be
        disabled.

    .. change::
        :tags: usecase, orm
        :tickets: 9298

        The :meth:`_orm.Session.refresh` method will now immediately load a
        relationship-bound attribute that is explicitly named within the
        :paramref:`_orm.Session.refresh.attribute_names` collection even if it is
        currently linked to the "select" loader, which normally is a "lazy" loader
        that does not fire off during a refresh. The "lazy loader" strategy will
        now detect that the operation is specifically a user-initiated
        :meth:`_orm.Session.refresh` operation which named this attribute
        explicitly, and will then call upon the "immediateload" strategy to
        actually emit SQL to load the attribute. This should be helpful in
        particular for some asyncio situations where the loading of an unloaded
        lazy-loaded attribute must be forced, without using the actual lazy-loading
        attribute pattern not supported in asyncio.


    .. change::
        :tags: bug, sql
        :tickets: 9313

        Fixed issue where element types of a tuple value would be hardcoded to take
        on the types from a compared-to tuple, when the comparison were using the
        :meth:`.ColumnOperators.in_` operator. This was inconsistent with the usual
        way that types are determined for a binary expression, which is that the
        actual element type on the right side is considered first before applying
        the left-hand-side type.

    .. change::
        :tags: usecase, orm declarative
        :tickets: 9266

        Added new parameter ``dataclasses_callable`` to both the
        :class:`_orm.MappedAsDataclass` class as well as the
        :meth:`_orm.registry.mapped_as_dataclass` method which allows an
        alternative callable to Python ``dataclasses.dataclass`` to be used in
        order to produce dataclasses. The use case here is to drop in Pydantic's
        dataclass function instead. Adjustments have been made to the mixin support
        added for :ticket:`9179` in version 2.0.1 so that the ``__annotations__``
        collection of the mixin is rewritten to not include the
        :class:`_orm.Mapped` container, in the same way as occurs with mapped
        classes, so that the Pydantic dataclasses constructor is not exposed to
        unknown types.

        .. seealso::

            :ref:`dataclasses_pydantic`


.. changelog::
    :version: 2.0.3
    :released: February 9, 2023

    .. change::
        :tags: typing, bug
        :tickets: 9254

        Remove ``typing.Self`` workaround, now using :pep:`673` for most methods
        that return ``Self``. As a consequence of this change ``mypy>=1.0.0`` is
        now required to type check SQLAlchemy code.
        Pull request courtesy Yurii Karabas.

    .. change::
        :tags: bug, sql, regression
        :tickets: 9271

        Fixed critical regression in SQL expression formulation in the 2.0 series
        due to :ticket:`7744` which improved support for SQL expressions that
        contained many elements against the same operator repeatedly; parenthesis
        grouping would be lost with expression elements beyond the first two
        elements.


.. changelog::
    :version: 2.0.2
    :released: February 6, 2023

    .. change::
        :tags: bug, orm declarative
        :tickets: 9249

        Fixed regression caused by the fix for :ticket:`9171`, which itself was
        fixing a regression, involving the mechanics of ``__init__()`` on classes
        that extend from :class:`_orm.DeclarativeBase`. The change made it such
        that ``__init__()`` was applied to the user-defined base if there were no
        ``__init__()`` method directly on the class. This has been adjusted so that
        ``__init__()`` is applied only if no other class in the hierarchy of the
        user-defined base has an ``__init__()`` method. This again allows
        user-defined base classes based on :class:`_orm.DeclarativeBase` to include
        mixins that themselves include a custom ``__init__()`` method.

    .. change::
        :tags: bug, mysql, regression
        :tickets: 9251

        Fixed regression caused by issue :ticket:`9058` which adjusted the MySQL
        dialect's ``has_table()`` to again use "DESCRIBE", where the specific error
        code raised by MySQL version 8 when using a non-existent schema name was
        unexpected and failed to be interpreted as a boolean result.



    .. change::
        :tags: bug, sqlite
        :tickets: 9251

        Fixed the SQLite dialect's ``has_table()`` function to correctly report
        False for queries that include a non-None schema name for a schema that
        doesn't exist; previously, a database error was raised.


    .. change::
        :tags: bug, orm declarative
        :tickets: 9226

        Fixed issue in ORM Declarative Dataclass mappings related to newly added
        support for mixins added in 2.0.1 via :ticket:`9179`, where a combination
        of using mixins plus ORM inheritance would mis-classify fields in some
        cases leading to field-level dataclass arguments such as ``init=False`` being
        lost.

    .. change::
        :tags: bug, orm, ression
        :tickets: 9232

        Fixed obscure ORM inheritance issue caused by :ticket:`8705` where some
        scenarios of inheriting mappers that indicated groups of columns from the
        local table and the inheriting table together under a
        :func:`_orm.column_property` would nonetheless warn that properties of the
        same name were being combined implicitly.

    .. change::
        :tags: orm, bug, regression
        :tickets: 9228

        Fixed regression where using the :paramref:`_orm.Mapper.version_id_col`
        feature with a regular Python-side incrementing column would fail to work
        for SQLite and other databases that don't support "rowcount" with
        "RETURNING", as "RETURNING" would be assumed for such columns even though
        that's not what actually takes place.

    .. change::
        :tags: bug, orm declarative
        :tickets: 9240

        Repaired ORM Declarative mappings to allow for the
        :paramref:`_orm.Mapper.primary_key` parameter to be specified within
        ``__mapper_args__`` when using :func:`_orm.mapped_column`. Despite this
        usage being directly in the 2.0 documentation, the :class:`_orm.Mapper` was
        not accepting the :func:`_orm.mapped_column` construct in this context. Ths
        feature was already working for the :paramref:`_orm.Mapper.version_id_col`
        and :paramref:`_orm.Mapper.polymorphic_on` parameters.

        As part of this change, the ``__mapper_args__`` attribute may be specified
        without using :func:`_orm.declared_attr` on a non-mapped mixin class,
        including a ``"primary_key"`` entry that refers to :class:`_schema.Column`
        or :func:`_orm.mapped_column` objects locally present on the mixin;
        Declarative will also translate these columns into the correct ones for a
        particular mapped class. This again was working already for the
        :paramref:`_orm.Mapper.version_id_col` and
        :paramref:`_orm.Mapper.polymorphic_on` parameters.  Additionally,
        elements within ``"primary_key"`` may be indicated as string names of
        existing mapped properties.

    .. change::
        :tags: usecase, sql
        :tickets: 8780

        Added a full suite of new SQL bitwise operators, for performing
        database-side bitwise expressions on appropriate data values such as
        integers, bit-strings, and similar. Pull request courtesy Yegor Statkevich.

        .. seealso::

            :ref:`operators_bitwise`


    .. change::
        :tags: bug, orm declarative
        :tickets: 9211

        An explicit error is raised if a mapping attempts to mix the use of
        :class:`_orm.MappedAsDataclass` with
        :meth:`_orm.registry.mapped_as_dataclass` within the same class hierarchy,
        as this produces issues with the dataclass function being applied at the
        wrong time to the mapped class, leading to errors during the mapping
        process.

    .. change::
        :tags: bug, orm, regression
        :tickets: 9217

        Fixed regression when using :meth:`_sql.Select.from_statement` in an ORM
        context, where matching of columns to SQL labels based on name alone was
        disabled for ORM-statements that weren't fully textual. This would prevent
        arbitrary SQL expressions with column-name labels from matching up to the
        entity to be loaded, which previously would work within the 1.4
        and previous series, so the previous behavior has been restored.

    .. change::
        :tags: bug, asyncio
        :tickets: 9237

        Repaired a regression caused by the fix for :ticket:`8419` which caused
        asyncpg connections to be reset (i.e. transaction ``rollback()`` called)
        and returned to the pool normally in the case that the connection were not
        explicitly returned to the connection pool and was instead being
        intercepted by Python garbage collection, which would fail if the garbage
        collection operation were being called outside of the asyncio event loop,
        leading to a large amount of stack trace activity dumped into logging
        and standard output.

        The correct behavior is restored, which is that all asyncio connections
        that are garbage collected due to not being explicitly returned to the
        connection pool are detached from the pool and discarded, along with a
        warning, rather than being returned the pool, as they cannot be reliably
        reset. In the case of asyncpg connections, the asyncpg-specific
        ``terminate()`` method will be used to end the connection more gracefully
        within this process as opposed to just dropping it.

        This change includes a small behavioral change that is hoped to be useful
        for debugging asyncio applications, where the warning that's emitted in the
        case of asyncio connections being unexpectedly garbage collected has been
        made slightly more aggressive by moving it outside of a ``try/except``
        block and into a ``finally:`` block, where it will emit unconditionally
        regardless of whether the detach/termination operation succeeded or not. It
        will also have the effect that applications or test suites which promote
        Python warnings to exceptions will see this as a full exception raise,
        whereas previously it was not possible for this warning to actually
        propagate as an exception. Applications and test suites which need to
        tolerate this warning in the interim should adjust the Python warnings
        filter to allow these warnings to not raise.

        The behavior for traditional sync connections remains unchanged, that
        garbage collected connections continue to be returned to the pool normally
        without emitting a warning. This will likely be changed in a future major
        release to at least emit a similar warning as is emitted for asyncio
        drivers, as it is a usage error for pooled connections to be intercepted by
        garbage collection without being properly returned to the pool.

    .. change::
        :tags: usecase, orm
        :tickets: 9220

        Added new event hook :meth:`_orm.MapperEvents.after_mapper_constructed`,
        which supplies an event hook to take place right as the
        :class:`_orm.Mapper` object has been fully constructed, but before the
        :meth:`_orm.registry.configure` call has been called. This allows code that
        can create additional mappings and table structures based on the initial
        configuration of a :class:`_orm.Mapper`, which also integrates within
        Declarative configuration. Previously, when using Declarative, where the
        :class:`_orm.Mapper` object is created within the class creation process,
        there was no documented means of running code at this point.  The change
        is to immediately benefit custom mapping schemes such as that
        of the :ref:`examples_versioned_history` example, which generate additional
        mappers and tables in response to the creation of mapped classes.


    .. change::
        :tags: usecase, orm
        :tickets: 9220

        The infrequently used :attr:`_orm.Mapper.iterate_properties` attribute and
        :meth:`_orm.Mapper.get_property` method, which are primarily used
        internally, no longer implicitly invoke the :meth:`_orm.registry.configure`
        process. Public access to these methods is extremely rare and the only
        benefit to having :meth:`_orm.registry.configure` would have been allowing
        "backref" properties be present in these collections. In order to support
        the new :meth:`_orm.MapperEvents.after_mapper_constructed` event, iteration
        and access to the internal :class:`_orm.MapperProperty` objects is now
        possible without triggering an implicit configure of the mapper itself.

        The more-public facing route to iteration of all mapper attributes, the
        :attr:`_orm.Mapper.attrs` collection and similar, will still implicitly
        invoke the :meth:`_orm.registry.configure` step thus making backref
        attributes available.

        In all cases, the :meth:`_orm.registry.configure` is always available to
        be called directly.

    .. change::
        :tags: bug, examples
        :tickets: 9220

        Reworked the :ref:`examples_versioned_history` to work with
        version 2.0, while at the same time improving the overall working of
        this example to use newer APIs, including a newly added hook
        :meth:`_orm.MapperEvents.after_mapper_constructed`.



    .. change::
        :tags: bug, mysql
        :tickets: 8626

        Added support for MySQL 8's new ``AS <name> ON DUPLICATE KEY`` syntax when
        using :meth:`_mysql.Insert.on_duplicate_key_update`, which is required for
        newer versions of MySQL 8 as the previous syntax using ``VALUES()`` now
        emits a deprecation warning with those versions. Server version detection
        is employed to determine if traditional MariaDB / MySQL < 8 ``VALUES()``
        syntax should be used, vs. the newer MySQL 8 required syntax. Pull request
        courtesy Caspar Wylie.

.. changelog::
    :version: 2.0.1
    :released: February 1, 2023

    .. change::
        :tags: bug, typing
        :tickets: 9174

        Opened up typing on :paramref:`.Select.with_for_update.of` to also accept
        table and mapped class arguments, as seems to be available for the MySQL
        dialect.

    .. change::
        :tags: bug, orm, regression
        :tickets: 9164

        Fixed regression where ORM models that used joined table inheritance with a
        composite foreign key would encounter an internal error in the mapper
        internals.



    .. change::
        :tags: bug, sql
        :tickets: 7664

        Corrected the fix for :ticket:`7664`, released in version 2.0.0, to also
        include :class:`.DropSchema` which was inadvertently missed in this fix,
        allowing stringification without a dialect. The fixes for both constructs
        is backported to the 1.4 series as of 1.4.47.


    .. change::
        :tags: bug, orm declarative
        :tickets: 9175

        Added support for :pep:`484` ``NewType`` to be used in the
        :paramref:`_orm.registry.type_annotation_map` as well as within
        :class:`.Mapped` constructs. These types will behave in the same way as
        custom subclasses of types right now; they must appear explicitly within
        the :paramref:`_orm.registry.type_annotation_map` to be mapped.

    .. change::
        :tags: bug, typing
        :tickets: 9183

        Fixed typing for limit/offset methods including :meth:`.Select.limit`,
        :meth:`.Select.offset`, :meth:`_orm.Query.limit`, :meth:`_orm.Query.offset`
        to allow ``None``, which is the documented API to "cancel" the current
        limit/offset.



    .. change::
        :tags: bug, orm declarative
        :tickets: 9179

        When using the :class:`.MappedAsDataclass` superclass, all classes within
        the hierarchy that are subclasses of this class will now be run through the
        ``@dataclasses.dataclass`` function whether or not they are actually
        mapped, so that non-ORM fields declared on non-mapped classes within the
        hierarchy will be used when mapped subclasses are turned into dataclasses.
        This behavior applies both to intermediary classes mapped with
        ``__abstract__ = True`` as well as to the user-defined declarative base
        itself, assuming :class:`.MappedAsDataclass` is present as a superclass for
        these classes.

        This allows non-mapped attributes such as ``InitVar`` declarations on
        superclasses to be used, without the need to run the
        ``@dataclasses.dataclass`` decorator explicitly on each non-mapped class.
        The new behavior is considered as correct as this is what the :pep:`681`
        implementation expects when using a superclass to indicate dataclass
        behavior.

    .. change::
        :tags: bug, typing
        :tickets: 9170

        Fixed typing issue where :func:`_orm.mapped_column` objects typed as
        :class:`_orm.Mapped` wouldn't be accepted in schema constraints such as
        :class:`_schema.ForeignKey`, :class:`_schema.UniqueConstraint` or
        :class:`_schema.Index`.

    .. change::
        :tags: bug, orm declarative
        :tickets: 9187

        Added support for :pep:`586` ``Literal[]`` to be used in the
        :paramref:`_orm.registry.type_annotation_map` as well as within
        :class:`.Mapped` constructs. To use custom types such as these, they must
        appear explicitly within the :paramref:`_orm.registry.type_annotation_map`
        to be mapped.  Pull request courtesy Frederik Aalund.

        As part of this change, the support for :class:`.sqltypes.Enum` in the
        :paramref:`_orm.registry.type_annotation_map` has been expanded to include
        support for ``Literal[]`` types consisting of string values to be used,
        in addition to ``enum.Enum`` datatypes.    If a ``Literal[]`` datatype
        is used within ``Mapped[]`` that is not linked in
        :paramref:`_orm.registry.type_annotation_map` to a specific datatype,
        a :class:`.sqltypes.Enum` will be used by default.

        .. seealso::

            :ref:`orm_declarative_mapped_column_enums`


    .. change::
        :tags: bug, orm declarative
        :tickets: 9200

        Fixed issue involving the use of :class:`.sqltypes.Enum` within the
        :paramref:`_orm.registry.type_annotation_map` where the
        :paramref:`_sqltypes.Enum.native_enum` parameter would not be correctly
        copied to the mapped column datatype, if it were overridden
        as stated in the documentation to set this parameter to False.



    .. change::
        :tags: bug, orm declarative, regression
        :tickets: 9171

        Fixed regression in :class:`.DeclarativeBase` class where the registry's
        default constructor would not be applied to the base itself, which is
        different from how the previous :func:`_orm.declarative_base` construct
        works. This would prevent a mapped class with its own ``__init__()`` method
        from calling ``super().__init__()`` in order to access the registry's
        default constructor and automatically populate attributes, instead hitting
        ``object.__init__()`` which would raise a ``TypeError`` on any arguments.




    .. change::
        :tags: bug, sql, regression
        :tickets: 9173

        Fixed regression related to the implementation for the new
        "insertmanyvalues" feature where an internal ``TypeError`` would occur in
        arrangements where a :func:`_sql.insert` would be referenced inside
        of another :func:`_sql.insert` via a CTE; made additional repairs for this
        use case for positional dialects such as asyncpg when using
        "insertmanyvalues".



    .. change::
        :tags: bug, typing
        :tickets: 9156

        Fixed typing for :meth:`_expression.ColumnElement.cast` to accept
        both ``Type[TypeEngine[T]]`` and ``TypeEngine[T]``; previously
        only ``TypeEngine[T]`` was accepted.  Pull request courtesy Yurii Karabas.

    .. change::
        :tags: bug, orm declarative
        :tickets: 9177

        Improved the ruleset used to interpret :pep:`593` ``Annotated`` types when
        used with Annotated Declarative mapping, the inner type will be checked for
        "Optional" in all cases which will be added to the criteria by which the
        column is set as "nullable" or not; if the type within the ``Annotated``
        container is optional (or unioned with ``None``), the column will be
        considered nullable if there are no explicit
        :paramref:`_orm.mapped_column.nullable` parameters overriding it.

    .. change::
        :tags: bug, orm
        :tickets: 9182

        Improved the error reporting when linking strategy options from a base
        class to another attribute that's off a subclass, where ``of_type()``
        should be used. Previously, when :meth:`.Load.options` is used, the message
        would lack informative detail that ``of_type()`` should be used, which was
        not the case when linking the options directly. The informative detail now
        emits even if :meth:`.Load.options` is used.



.. changelog::
    :version: 2.0.0
    :released: January 26, 2023

    .. change::
        :tags: bug, sql
        :tickets: 7664

        Fixed stringify for a the :class:`.CreateSchema` DDL construct, which
        would fail with an ``AttributeError`` when stringified without a
        dialect. Update: Note this fix failed to accommodate for
        :class:`.DropSchema`; a followup fix in version 2.0.1 repairs this
        case. The fix for both elements is backported to 1.4.47.

    .. change::
        :tags: usecase, orm extensions
        :tickets: 5145

        Added new feature to :class:`.AutomapBase` for autoload of classes across
        multiple schemas which may have overlapping names, by providing a
        :paramref:`.AutomapBase.prepare.modulename_for_table` parameter which
        allows customization of the ``__module__`` attribute of newly generated
        classes, as well as a new collection :attr:`.AutomapBase.by_module`, which
        stores a dot-separated namespace of module names linked to classes based on
        the ``__module__`` attribute.

        Additionally, the :meth:`.AutomapBase.prepare` method may now be invoked
        any number of times, with or without reflection enabled; only newly
        added tables that were not previously mapped will be processed on each
        call.   Previously, the :meth:`.MetaData.reflect` method would need to be
        called explicitly each time.

        .. seealso::

            :ref:`automap_by_module` - illustrates use of both techniques at once.

    .. change::
        :tags: orm, bug
        :tickets: 7305

        Improved the notification of warnings that are emitted within the configure
        mappers or flush process, which are often invoked as part of a different
        operation, to add additional context to the message that indicates one of
        these operations as the source of the warning within operations that may
        not be obviously related.

    .. change::
        :tags: bug, typing
        :tickets: 9129

        Added typing for the built-in generic functions that are available from the
        :data:`_sql.func` namespace, which accept a particular set of arguments and
        return a particular type, such as for :class:`_sql.count`,
        :class:`_sql.current_timestamp`, etc.

    .. change::
        :tags: bug, typing
        :tickets: 9120

        Corrected the type passed for "lambda statements" so that a plain lambda is
        accepted by mypy, pyright, others without any errors about argument types.
        Additionally implemented typing for more of the public API for lambda
        statements and ensured :class:`.StatementLambdaElement` is part of the
        :class:`.Executable` hierarchy so it's typed as accepted by
        :meth:`_engine.Connection.execute`.

    .. change::
        :tags: typing, bug
        :tickets: 9122

        The :meth:`_sql.ColumnOperators.in_` and
        :meth:`_sql.ColumnOperators.not_in` methods are typed to include
        ``Iterable[Any]`` rather than ``Sequence[Any]`` for more flexibility in
        argument type.


    .. change::
        :tags: typing, bug
        :tickets: 9123

        The :func:`_sql.or_` and :func:`_sql.and_` from a typing perspective
        require the first argument to be present, however these functions still
        accept zero arguments which will emit a deprecation warning at runtime.
        Typing is also added to support sending the fixed literal ``False`` for
        :func:`_sql.or_` and ``True`` for :func:`_sql.and_` as the first argument
        only, however the documentation now indicates sending the
        :func:`_sql.false` and :func:`_sql.true` constructs in these cases as a
        more explicit approach.


    .. change::
        :tags: typing, bug
        :tickets: 9125

        Fixed typing issue where iterating over a :class:`_orm.Query` object
        was not correctly typed.

    .. change::
        :tags: typing, bug
        :tickets: 9136

        Fixed typing issue where the object type when using :class:`_engine.Result`
        as a context manager were not preserved, indicating :class:`_engine.Result`
        in all cases rather than the specific :class:`_engine.Result` sub-type.
        Pull request courtesy Martin Baláž.

    .. change::
        :tags: typing, bug
        :tickets: 9150

        Fixed issue where using the :paramref:`_orm.relationship.remote_side`
        and similar parameters, passing an annotated declarative object typed as
        :class:`_orm.Mapped`, would not be accepted by the type checker.

    .. change::
        :tags: typing, bug
        :tickets: 9148

        Added typing to legacy operators such as ``isnot()``, ``notin_()``, etc.
        which previously were referencing the newer operators but were not
        themselves typed.

    .. change::
        :tags: feature, orm extensions
        :tickets: 7226

        Added new option to horizontal sharding API
        :class:`_horizontal.set_shard_id` which sets the effective shard identifier
        to query against, for both the primary query as well as for all secondary
        loaders including relationship eager loaders as well as relationship and
        column lazy loaders.

    .. change::
        :tags: bug, mssql, regression
        :tickets: 9142

        The newly added comment reflection and rendering capability of the MSSQL
        dialect, added in :ticket:`7844`, will now be disabled by default if it
        cannot be determined that an unsupported backend such as Azure Synapse may
        be in use; this backend does not support table and column comments and does
        not support the SQL Server routines in use to generate them as well as to
        reflect them. A new parameter ``supports_comments`` is added to the dialect
        which defaults to ``None``, indicating that comment support should be
        auto-detected. When set to ``True`` or ``False``, the comment support is
        either enabled or disabled unconditionally.

        .. seealso::

            :ref:`mssql_comment_support`


.. changelog::
    :version: 2.0.0rc3
    :released: January 26, 2023
    :released: January 18, 2023

    .. change::
        :tags: bug, typing
        :tickets: 9096

        Fixes to the annotations within the ``sqlalchemy.ext.hybrid`` extension for
        more effective typing of user-defined methods. The typing now uses
        :pep:`612` features, now supported by recent versions of Mypy, to maintain
        argument signatures for :class:`.hybrid_method`. Return values for hybrid
        methods are accepted as SQL expressions in contexts such as
        :meth:`_sql.Select.where` while still supporting SQL methods.

    .. change::
        :tags: bug, orm
        :tickets: 9099

        Fixed issue where using a pep-593 ``Annotated`` type in the
        :paramref:`_orm.registry.type_annotation_map` which itself contained a
        generic plain container or ``collections.abc`` type (e.g. ``list``,
        ``dict``, ``collections.abc.Sequence``, etc. ) as the target type would
        produce an internal error when the ORM were trying to interpret the
        ``Annotated`` instance.



    .. change::
        :tags: bug, orm
        :tickets: 9100

        Added an error message when a :func:`_orm.relationship` is mapped against
        an abstract container type, such as ``Mapped[Sequence[B]]``, without
        providing the :paramref:`_orm.relationship.container_class` parameter which
        is necessary when the type is abstract. Previously the abstract
        container would attempt to be instantiated at a later step and fail.



    .. change::
        :tags: orm, feature
        :tickets: 9060

        Added a new parameter to :class:`_orm.Mapper` called
        :paramref:`_orm.Mapper.polymorphic_abstract`. The purpose of this directive
        is so that the ORM will not consider the class to be instantiated or loaded
        directly, only subclasses. The actual effect is that the
        :class:`_orm.Mapper` will prevent direct instantiation of instances
        of the class and will expect that the class does not have a distinct
        polymorphic identity configured.

        In practice, the class that is mapped with
        :paramref:`_orm.Mapper.polymorphic_abstract` can be used as the target of a
        :func:`_orm.relationship` as well as be used in queries; subclasses must of
        course include polymorphic identities in their mappings.

        The new parameter is automatically applied to classes that subclass
        the :class:`.AbstractConcreteBase` class, as this class is not intended
        to be instantiated.

        .. seealso::

            :ref:`orm_inheritance_abstract_poly`


    .. change::
        :tags: bug, postgresql
        :tickets: 9106

        Fixed regression where psycopg3 changed an API call as of version 3.1.8 to
        expect a specific object type that was previously not enforced, breaking
        connectivity for the psycopg3 dialect.

    .. change::
        :tags: oracle, usecase
        :tickets: 9086

        Added support for the Oracle SQL type ``TIMESTAMP WITH LOCAL TIME ZONE``,
        using a newly added Oracle-specific :class:`_oracle.TIMESTAMP` datatype.

.. changelog::
    :version: 2.0.0rc2
    :released: January 26, 2023
    :released: January 9, 2023

    .. change::
        :tags: bug, typing
        :tickets: 9067

        The Data Class Transforms argument ``field_descriptors`` was renamed
        to ``field_specifiers`` in the accepted version of PEP 681.

    .. change::
        :tags: bug, oracle
        :tickets: 9059

        Supported use case for foreign key constraints where the local column is
        marked as "invisible". The errors normally generated when a
        :class:`.ForeignKeyConstraint` is created that check for the target column
        are disabled when reflecting, and the constraint is skipped with a warning
        in the same way which already occurs for an :class:`.Index` with a similar
        issue.

    .. change::
        :tags: bug, orm
        :tickets: 9071

        Fixed issue where an overly restrictive ORM mapping rule were added in 2.0
        which prevented mappings against :class:`.TableClause` objects, such as
        those used in the view recipe on the wiki.

    .. change::
        :tags: bug, mysql
        :tickets: 9058

        Restored the behavior of :meth:`.Inspector.has_table` to report on
        temporary tables for MySQL / MariaDB. This is currently the behavior for
        all other included dialects, but was removed for MySQL in 1.4 due to no
        longer using the DESCRIBE command; there was no documented support for temp
        tables being reported by the :meth:`.Inspector.has_table` method in this
        version or on any previous version, so the previous behavior was undefined.

        As SQLAlchemy 2.0 has added formal support for temp table status via
        :meth:`.Inspector.has_table`, the MySQL /MariaDB dialect has been reverted
        to use the "DESCRIBE" statement as it did in the SQLAlchemy 1.3 series and
        previously, and test support is added to include MySQL / MariaDB for
        this behavior.   The previous issues with ROLLBACK being emitted which
        1.4 sought to improve upon don't apply in SQLAlchemy 2.0 due to
        simplifications in how :class:`.Connection` handles transactions.

        DESCRIBE is necessary as MariaDB in particular has no consistently
        available public information schema of any kind in order to report on temp
        tables other than DESCRIBE/SHOW COLUMNS, which rely on throwing an error
        in order to report no results.

    .. change::
        :tags: json, postgresql
        :tickets: 7147

        Implemented missing ``JSONB`` operations:

        * ``@@`` using :meth:`_postgresql.JSONB.Comparator.path_match`
        * ``@?`` using :meth:`_postgresql.JSONB.Comparator.path_exists`
        * ``#-`` using :meth:`_postgresql.JSONB.Comparator.delete_path`

        Pull request courtesy of Guilherme Martins Crocetti.

.. changelog::
    :version: 2.0.0rc1
    :released: January 26, 2023
    :released: December 28, 2022

    .. change::
        :tags: bug, typing
        :tickets: 6810, 9025

        pep-484 typing has been completed for the
        ``sqlalchemy.ext.horizontal_shard`` extension as well as the
        ``sqlalchemy.orm.events`` module. Thanks to Gleb Kisenkov for their
        efforts.


    .. change::
        :tags: postgresql, bug
        :tickets: 8977
        :versions: 2.0.0rc1

        Added support for explicit use of PG full text functions with asyncpg and
        psycopg (SQLAlchemy 2.0 only), with regards to the ``REGCONFIG`` type cast
        for the first argument, which previously would be incorrectly cast to a
        VARCHAR, causing failures on these dialects that rely upon explicit type
        casts. This includes support for :class:`_postgresql.to_tsvector`,
        :class:`_postgresql.to_tsquery`, :class:`_postgresql.plainto_tsquery`,
        :class:`_postgresql.phraseto_tsquery`,
        :class:`_postgresql.websearch_to_tsquery`,
        :class:`_postgresql.ts_headline`, each of which will determine based on
        number of arguments passed if the first string argument should be
        interpreted as a PostgreSQL "REGCONFIG" value; if so, the argument is typed
        using a newly added type object :class:`_postgresql.REGCONFIG` which is
        then explicitly cast in the SQL expression.


    .. change::
        :tags: bug, orm
        :tickets: 4629

        A warning is emitted if a backref name used in :func:`_orm.relationship`
        names an attribute on the target class which already has a method or
        attribute assigned to that name, as the backref declaration will replace
        that attribute.

    .. change::
        :tags: bug, postgresql
        :tickets: 9020

        Fixed regression where newly revised PostgreSQL range types such as
        :class:`_postgresql.INT4RANGE` could not be set up as the impl of a
        :class:`.TypeDecorator` custom type, instead raising a ``TypeError``.

    .. change::
        :tags: usecase, orm
        :tickets: 7837

        Adjustments to the :class:`_orm.Session` in terms of extensibility,
        as well as updates to the :class:`.ShardedSession` extension:

        * :meth:`_orm.Session.get` now accepts
          :paramref:`_orm.Session.get.bind_arguments`, which in particular may be
          useful when using the horizontal sharding extension.

        * :meth:`_orm.Session.get_bind` accepts arbitrary kw arguments, which
          assists in developing code that uses a :class:`_orm.Session` class which
          overrides this method with additional arguments.

        * Added a new ORM execution option ``identity_token`` which may be used
          to directly affect the "identity token" that will be associated with
          newly loaded ORM objects.  This token is how sharding approaches
          (namely the :class:`.ShardedSession`, but can be used in other cases
          as well) separate object identities across different "shards".

          .. seealso::

              :ref:`queryguide_identity_token`

        * The :meth:`_orm.SessionEvents.do_orm_execute` event hook may now be used
          to affect all ORM-related options, including ``autoflush``,
          ``populate_existing``, and ``yield_per``; these options are re-consumed
          subsequent to event hooks being invoked before they are acted upon.
          Previously, options like ``autoflush`` would have been already evaluated
          at this point. The new ``identity_token`` option is also supported in
          this mode and is now used by the horizontal sharding extension.


        * The :class:`.ShardedSession` class replaces the
          :paramref:`.ShardedSession.id_chooser` hook with a new hook
          :paramref:`.ShardedSession.identity_chooser`, which no longer relies upon
          the legacy :class:`_orm.Query` object.
          :paramref:`.ShardedSession.id_chooser` is still accepted in place of
          :paramref:`.ShardedSession.identity_chooser` with a deprecation warning.

    .. change::
        :tags: usecase, orm
        :tickets: 9015

        The behavior of "joining an external transaction into a Session" has been
        revised and improved, allowing explicit control over how the
        :class:`_orm.Session` will accommodate an incoming
        :class:`_engine.Connection` that already has a transaction and possibly a
        savepoint already established. The new parameter
        :paramref:`_orm.Session.join_transaction_mode` includes a series of option
        values which can accommodate the existing transaction in several ways, most
        importantly allowing a :class:`_orm.Session` to operate in a fully
        transactional style using savepoints exclusively, while leaving the
        externally initiated transaction non-committed and active under all
        circumstances, allowing test suites to rollback all changes that take place
        within tests.

        Additionally, revised the :meth:`_orm.Session.close` method to fully close
        out savepoints that may still be present, which also allows the
        "external transaction" recipe to proceed without warnings if the
        :class:`_orm.Session` did not explicitly end its own SAVEPOINT
        transactions.

        .. seealso::

            :ref:`change_9015`


    .. change::
        :tags: bug, sql
        :tickets: 8988

        Added test support to ensure that all compiler ``visit_xyz()`` methods
        across all :class:`.Compiler` implementations in SQLAlchemy accept a
        ``**kw`` parameter, so that all compilers accept additional keyword
        arguments under all circumstances.

    .. change::
        :tags: bug, postgresql
        :tickets: 8984

        The :meth:`_postgresql.Range.__eq___` will now return ``NotImplemented``
        when comparing with an instance of a different class, instead of raising
        an :exc:`AttributeError` exception.

    .. change::
        :tags: bug, sql
        :tickets: 6114

        The :meth:`.SQLCompiler.construct_params` method, as well as the
        :attr:`.SQLCompiler.params` accessor, will now return the
        exact parameters that correspond to a compiled statement that used
        the ``render_postcompile`` parameter to compile.   Previously,
        the method returned a parameter structure that by itself didn't correspond
        to either the original parameters or the expanded ones.

        Passing a new dictionary of parameters to
        :meth:`.SQLCompiler.construct_params` for a :class:`.SQLCompiler` that was
        constructed with ``render_postcompile`` is now disallowed; instead, to make
        a new SQL string and parameter set for an alternate set of parameters, a
        new method :meth:`.SQLCompiler.construct_expanded_state` is added which
        will produce a new expanded form for the given parameter set, using the
        :class:`.ExpandedState` container which includes a new SQL statement
        and new parameter dictionary, as well as a positional parameter tuple.


    .. change::
        :tags: bug, orm
        :tickets: 8703, 8997, 8996

        A series of changes and improvements regarding
        :meth:`_orm.Session.refresh`. The overall change is that primary key
        attributes for an object are now included in a refresh operation
        unconditionally when relationship-bound attributes are to be refreshed,
        even if not expired and even if not specified in the refresh.

        * Improved :meth:`_orm.Session.refresh` so that if autoflush is enabled
          (as is the default for :class:`_orm.Session`), the autoflush takes place
          at an earlier part of the refresh process so that pending primary key
          changes are applied without errors being raised.  Previously, this
          autoflush took place too late in the process and the SELECT statement
          would not use the correct key to locate the row and an
          :class:`.InvalidRequestError` would be raised.

        * When the above condition is present, that is, unflushed primary key
          changes are present on the object, but autoflush is not enabled,
          the refresh() method now explicitly disallows the operation to proceed,
          and an informative :class:`.InvalidRequestError` is raised asking that
          the pending primary key changes be flushed first.  Previously,
          this use case was simply broken and :class:`.InvalidRequestError`
          would be raised anyway. This restriction is so that it's safe for the
          primary key attributes to be refreshed, as is necessary for the case of
          being able to refresh the object with relationship-bound secondary
          eagerloaders also being emitted. This rule applies in all cases to keep
          API behavior consistent regardless of whether or not the PK cols are
          actually needed in the refresh, as it is unusual to be refreshing
          some attributes on an object while keeping other attributes "pending"
          in any case.

        * The :meth:`_orm.Session.refresh` method has been enhanced such that
          attributes which are :func:`_orm.relationship`-bound and linked to an
          eager loader, either at mapping time or via last-used loader options,
          will be refreshed in all cases even when a list of attributes is passed
          that does not include any columns on the parent row. This builds upon the
          feature first implemented for non-column attributes as part of
          :ticket:`1763` fixed in 1.4 allowing eagerly-loaded relationship-bound
          attributes to participate in the :meth:`_orm.Session.refresh` operation.
          If the refresh operation does not indicate any columns on the parent row
          to be refreshed, the primary key columns will nonetheless be included
          in the refresh operation, which allows the load to proceed into the
          secondary relationship loaders indicated as it does normally.
          Previously an :class:`.InvalidRequestError` error would be raised
          for this condition (:ticket:`8703`)

        * Fixed issue where an unnecessary additional SELECT would be emitted in
          the case where :meth:`_orm.Session.refresh` were called with a
          combination of expired attributes, as well as an eager loader such as
          :func:`_orm.selectinload` that emits a "secondary" query, if the primary
          key attributes were also in an expired state.  As the primary key
          attributes are now included in the refresh automatically, there is no
          additional load for these attributes when a relationship loader
          goes to select for them (:ticket:`8997`)

        * Fixed regression caused by :ticket:`8126` released in 2.0.0b1 where the
          :meth:`_orm.Session.refresh` method would fail with an
          ``AttributeError``, if passed both an expired column name as well as the
          name of a relationship-bound attribute that was linked to a "secondary"
          eagerloader such as the :func:`_orm.selectinload` eager loader
          (:ticket:`8996`)

    .. change::
        :tags: bug, sql
        :tickets: 8994

        To accommodate for third party dialects with different character escaping
        needs regarding bound parameters, the system by which SQLAlchemy "escapes"
        (i.e., replaces with another character in its place) special characters in
        bound parameter names has been made extensible for third party dialects,
        using the :attr:`.SQLCompiler.bindname_escape_chars` dictionary which can
        be overridden at the class declaration level on any :class:`.SQLCompiler`
        subclass. As part of this change, also added the dot ``"."`` as a default
        "escaped" character.


    .. change::
        :tags: orm, feature
        :tickets: 8889

        Added a new default value for the :paramref:`.Mapper.eager_defaults`
        parameter "auto", which will automatically fetch table default values
        during a unit of work flush, if the dialect supports RETURNING for the
        INSERT being run, as well as
        :ref:`insertmanyvalues <engine_insertmanyvalues>` available. Eager fetches
        for server-side UPDATE defaults, which are very uncommon, continue to only
        take place if :paramref:`.Mapper.eager_defaults` is set to ``True``, as
        there is no batch-RETURNING form for UPDATE statements.


    .. change::
        :tags: usecase, orm
        :tickets: 8973

        Removed the requirement that the ``__allow_unmapped__`` attribute be used
        on Declarative Dataclass Mapped class when non-``Mapped[]`` annotations are
        detected; previously, an error message that was intended to support legacy
        ORM typed mappings would be raised, which additionally did not mention
        correct patterns to use with Dataclasses specifically. This error message
        is now no longer raised if :meth:`_orm.registry.mapped_as_dataclass` or
        :class:`_orm.MappedAsDataclass` is used.

        .. seealso::

            :ref:`orm_declarative_native_dataclasses_non_mapped_fields`


    .. change::
        :tags: bug, orm
        :tickets: 8168

        Improved a fix first made in version 1.4 for :ticket:`8456` which scaled
        back the usage of internal "polymorphic adapters", that are used to render
        ORM queries when the :paramref:`_orm.Mapper.with_polymorphic` parameter is
        used. These adapters, which are very complex and error prone, are now used
        only in those cases where an explicit user-supplied subquery is used for
        :paramref:`_orm.Mapper.with_polymorphic`, which includes only the use case
        of concrete inheritance mappings that use the
        :func:`_orm.polymorphic_union` helper, as well as the legacy use case of
        using an aliased subquery for joined inheritance mappings, which is not
        needed in modern use.

        For the most common case of joined inheritance mappings that use the
        built-in polymorphic loading scheme, which includes those which make use of
        the :paramref:`_orm.Mapper.polymorphic_load` parameter set to ``inline``,
        polymorphic adapters are now no longer used. This has both a positive
        performance impact on the construction of queries as well as a
        substantial simplification of the internal query rendering process.

        The specific issue targeted was to allow a :func:`_orm.column_property`
        to refer to joined-inheritance classes within a scalar subquery, which now
        works as intuitively as is feasible.



.. changelog::
    :version: 2.0.0b4
    :released: January 26, 2023
    :released: December 5, 2022

    .. change::
        :tags: usecase, orm
        :tickets: 8859

        Added support custom user-defined types which extend the Python
        ``enum.Enum`` base class to be resolved automatically
        to SQLAlchemy :class:`.Enum` SQL types, when using the Annotated
        Declarative Table feature.  The feature is made possible through new
        lookup features added to the ORM type map feature, and includes support
        for changing the arguments of the :class:`.Enum` that's generated by
        default as well as setting up specific ``enum.Enum`` types within
        the map with specific arguments.

        .. seealso::

            :ref:`orm_declarative_mapped_column_enums`

    .. change::
        :tags: bug, typing
        :tickets: 8783

        Adjusted internal use of the Python ``enum.IntFlag`` class which changed
        its behavioral contract in Python 3.11. This was not causing runtime
        failures however caused typing runs to fail under Python 3.11.

    .. change::
        :tags: usecase, typing
        :tickets: 8847

        Added a new type :class:`.SQLColumnExpression` which may be indicated in
        user code to represent any SQL column oriented expression, including both
        those based on :class:`.ColumnElement` as well as on ORM
        :class:`.QueryableAttribute`. This type is a real class, not an alias, so
        can also be used as the foundation for other objects.  An additional
        ORM-specific subclass :class:`.SQLORMExpression` is also included.


    .. change::
        :tags: bug, typing
        :tickets: 8667, 6810

        The ``sqlalchemy.ext.mutable`` extension and ``sqlalchemy.ext.automap``
        extensions are now fully pep-484 typed. Huge thanks to Gleb Kisenkov for
        their efforts on this.



    .. change::
        :tags: bug, sql
        :tickets: 8849

        The approach to the ``numeric`` pep-249 paramstyle has been rewritten, and
        is now fully supported, including by features such as "expanding IN" and
        "insertmanyvalues". Parameter names may also be repeated in the source SQL
        construct which will be correctly represented within the numeric format
        using a single parameter. Introduced an additional numeric paramstyle
        called ``numeric_dollar``, which is specifically what's used by the asyncpg
        dialect; the paramstyle is equivalent to ``numeric`` except numeric
        indicators are indicated by a dollar-sign rather than a colon. The asyncpg
        dialect now uses ``numeric_dollar`` paramstyle directly, rather than
        compiling to ``format`` style first.

        The ``numeric`` and ``numeric_dollar`` paramstyles assume that the target
        backend is capable of receiving the numeric parameters in any order,
        and will match the given parameter values to the statement based on
        matching their position (1-based) to the numeric indicator.  This is the
        normal behavior of "numeric" paramstyles, although it was observed that
        the SQLite DBAPI implements a not-used "numeric" style that does not honor
        parameter ordering.

    .. change::
        :tags: usecase, postgresql
        :tickets: 8765

        Complementing :ticket:`8690`, new comparison methods such as
        :meth:`_postgresql.Range.adjacent_to`,
        :meth:`_postgresql.Range.difference`, :meth:`_postgresql.Range.union`,
        etc., were added to the PG-specific range objects, bringing them in par
        with the standard operators implemented by the underlying
        :attr:`_postgresql.AbstractRange.comparator_factory`.

        In addition, the ``__bool__()`` method of the class has been corrected to
        be consistent with the common Python containers behavior as well as how
        other popular PostgreSQL drivers do: it now tells whether the range
        instance is *not* empty, rather than the other way around.

        Pull request courtesy Lele Gaifax.

    .. change::
        :tags: bug, sql
        :tickets: 8770

        Adjusted the rendering of ``RETURNING``, in particular when using
        :class:`_sql.Insert`, such that it now renders columns using the same logic
        as that of the :class:`.Select` construct to generate labels, which will
        include disambiguating labels, as well as that a SQL function surrounding a
        named column will be labeled using the column name itself. This establishes
        better cross-compatibility when selecting rows from either :class:`.Select`
        constructs or from DML statements that use :meth:`.UpdateBase.returning`. A
        narrower scale change was also made for the 1.4 series that adjusted the
        function label issue only.

    .. change::
        :tags: change, postgresql, asyncpg
        :tickets: 8926

        Changed the paramstyle used by asyncpg from ``format`` to
        ``numeric_dollar``. This has two main benefits since it does not require
        additional processing of the statement and allows for duplicate parameters
        to be present in the statements.

    .. change::
        :tags: bug, orm
        :tickets: 8888

        Fixed issue where use of an unknown datatype within a :class:`.Mapped`
        annotation for a column-based attribute would silently fail to map the
        attribute, rather than reporting an exception; an informative exception
        message is now raised.

    .. change::
        :tags: bug, orm
        :tickets: 8777

        Fixed a suite of issues involving :class:`.Mapped` use with dictionary
        types, such as ``Mapped[Dict[str, str] | None]``, would not be correctly
        interpreted in Declarative ORM mappings. Support to correctly
        "de-optionalize" this type including for lookup in ``type_annotation_map``
        has been fixed.

    .. change::
        :tags: feature, orm
        :tickets: 8822

        Added a new parameter :paramref:`_orm.mapped_column.use_existing_column` to
        accommodate the use case of a single-table inheritance mapping that uses
        the pattern of more than one subclass indicating the same column to take
        place on the superclass. This pattern was previously possible by using
        :func:`_orm.declared_attr` in conjunction with locating the existing column
        in the ``.__table__`` of the superclass, however is now updated to work
        with :func:`_orm.mapped_column` as well as with pep-484 typing, in a
        simple and succinct way.

        .. seealso::

           :ref:`orm_inheritance_column_conflicts`




    .. change::
        :tags: bug, mssql
        :tickets: 8917

        Fixed regression caused by the combination of :ticket:`8177`, re-enable
        setinputsizes for SQL server unless fast_executemany + DBAPI executemany is
        used for a statement, along with :ticket:`6047`, implement
        "insertmanyvalues", which bypasses DBAPI executemany in place of a custom
        DBAPI execute for INSERT statements. setinputsizes would incorrectly not be
        used for a multiple parameter-set INSERT statement that used
        "insertmanyvalues" if fast_executemany were turned on, as the check would
        incorrectly assume this is a DBAPI executemany call.  The "regression"
        would then be that the "insertmanyvalues" statement format is apparently
        slightly more sensitive to multiple rows that don't use the same types
        for each row, so in such a case setinputsizes is especially needed.

        The fix repairs the fast_executemany check so that it only disables
        setinputsizes if true DBAPI executemany is to be used.

    .. change::
        :tags: bug, orm, performance
        :tickets: 8796

        Additional performance enhancements within ORM-enabled SQL statements,
        specifically targeting callcounts within the construction of ORM
        statements, using combinations of :func:`_orm.aliased` with
        :func:`_sql.union` and similar "compound" constructs, in addition to direct
        performance improvements to the ``corresponding_column()`` internal method
        that is used heavily by the ORM by constructs like :func:`_orm.aliased` and
        similar.


    .. change::
        :tags: bug, postgresql
        :tickets: 8884

        Added additional type-detection for the new PostgreSQL
        :class:`_postgresql.Range` type, where previous cases that allowed the
        psycopg2-native range objects to be received directly by the DBAPI without
        SQLAlchemy intercepting them stopped working, as we now have our own value
        object. The :class:`_postgresql.Range` object has been enhanced such that
        SQLAlchemy Core detects it in otherwise ambiguous situations (such as
        comparison to dates) and applies appropriate bind handlers. Pull request
        courtesy Lele Gaifax.

    .. change::
        :tags: bug, orm
        :tickets: 8880

        Fixed bug in :ref:`orm_declarative_native_dataclasses` feature where using
        plain dataclass fields with the ``__allow_unmapped__`` directive in a
        mapping would not create a dataclass with the correct class-level state for
        those fields, copying the raw ``Field`` object to the class inappropriately
        after dataclasses itself had replaced the ``Field`` object with the
        class-level default value.

    .. change::
        :tags: usecase, orm extensions
        :tickets: 8878

        Added support for the :func:`.association_proxy` extension function to
        take part within Python ``dataclasses`` configuration, when using
        the native dataclasses feature described at
        :ref:`orm_declarative_native_dataclasses`.  Included are attribute-level
        arguments including :paramref:`.association_proxy.init` and
        :paramref:`.association_proxy.default_factory`.

        Documentation for association proxy has also been updated to use
        "Annotated Declarative Table" forms within examples, including type
        annotations used for :class:`.AssocationProxy` itself.


    .. change::
        :tags: bug, typing

        Corrected typing support for the :paramref:`_orm.relationship.secondary`
        argument which may also accept a callable (lambda) that returns a
        :class:`.FromClause`.

    .. change::
        :tags: bug, orm, regression
        :tickets: 8812

        Fixed regression where flushing a mapped class that's mapped against a
        subquery, such as a direct mapping or some forms of concrete table
        inheritance, would fail if the :paramref:`_orm.Mapper.eager_defaults`
        parameter were used.

    .. change::
        :tags: bug, schema
        :tickets: 8925

        Stricter rules are in place for appending of :class:`.Column` objects to
        :class:`.Table` objects, both moving some previous deprecation warnings to
        exceptions, and preventing some previous scenarios that would cause
        duplicate columns to appear in tables, when
        :paramref:`.Table.extend_existing` were set to ``True``, for both
        programmatic :class:`.Table` construction as well as during reflection
        operations.

        See :ref:`change_8925` for a rundown of these changes.

        .. seealso::

            :ref:`change_8925`

    .. change::
        :tags: usecase, orm
        :tickets: 8905

        Added :paramref:`_orm.mapped_column.compare` parameter to relevant ORM
        attribute constructs including :func:`_orm.mapped_column`,
        :func:`_orm.relationship` etc. to provide for the Python dataclasses
        ``compare`` parameter on ``field()``, when using the
        :ref:`orm_declarative_native_dataclasses` feature. Pull request courtesy
        Simon Schiele.

    .. change::
        :tags: sql, usecase
        :tickets: 6289

        Added :class:`_expression.ScalarValues` that can be used as a column
        element allowing using :class:`_expression.Values` inside ``IN`` clauses
        or in conjunction with ``ANY`` or ``ALL`` collection aggregates.
        This new class is generated using the method
        :meth:`_expression.Values.scalar_values`.
        The :class:`_expression.Values` instance is now coerced to a
        :class:`_expression.ScalarValues` when used in a ``IN`` or ``NOT IN``
        operation.

    .. change::
        :tags: bug, orm
        :tickets: 8853

        Fixed regression in 2.0.0b3 caused by :ticket:`8759` where indicating the
        :class:`.Mapped` name using a qualified name such as
        ``sqlalchemy.orm.Mapped`` would fail to be recognized by Declarative as
        indicating the :class:`.Mapped` construct.

    .. change::
        :tags: bug, typing
        :tickets: 8842

        Improved the typing for :class:`.sessionmaker` and
        :class:`.async_sessionmaker`, so that the default type of their return value
        will be :class:`.Session` or :class:`.AsyncSession`, without the need to
        type this explicitly. Previously, Mypy would not automaticaly infer these
        return types from its generic base.

        As part of this change, arguments for :class:`.Session`,
        :class:`.AsyncSession`, :class:`.sessionmaker` and
        :class:`.async_sessionmaker` beyond the initial "bind" argument have been
        made keyword-only, which includes parameters that have always been
        documented as keyword arguments, such as :paramref:`.Session.autoflush`,
        :paramref:`.Session.class_`, etc.

        Pull request courtesy Sam Bull.


    .. change::
        :tags: bug, typing
        :tickets: 8776

        Fixed issue where passing a callbale function returning an iterable
        of column elements to :paramref:`_orm.relationship.order_by` was
        flagged as an error in type checkers.

.. changelog::
    :version: 2.0.0b3
    :released: January 26, 2023
    :released: November 4, 2022

    .. change::
        :tags: bug, orm, declarative
        :tickets: 8759

        Added support in ORM declarative annotations for class names specified for
        :func:`_orm.relationship`, as well as the name of the :class:`_orm.Mapped`
        symbol itself, to be different names than their direct class name, to
        support scenarios such as where :class:`_orm.Mapped` is imported as
        ``from sqlalchemy.orm import Mapped as M``, or where related class names
        are imported with an alternate name in a similar fashion. Additionally, a
        target class name given as the lead argument for :func:`_orm.relationship`
        will always supersede the name given in the left hand annotation, so that
        otherwise un-importable names that also don't match the class name can
        still be used in annotations.

    .. change::
        :tags: bug, orm, declarative
        :tickets: 8692

        Improved support for legacy 1.4 mappings that use annotations which don't
        include ``Mapped[]``, by ensuring the ``__allow_unmapped__`` attribute can
        be used to allow such legacy annotations to pass through Annotated
        Declarative without raising an error and without being interpreted in an
        ORM runtime context. Additionally improved the error message generated when
        this condition is detected, and added more documentation for how this
        situation should be handled. Unfortunately the 1.4 WARN_SQLALCHEMY_20
        migration warning cannot detect this particular configurational issue at
        runtime with its current architecture.

    .. change::
        :tags: usecase, postgresql
        :tickets: 8690

        Refined the new approach to range objects described at :ref:`change_7156`
        to accommodate driver-specific range and multirange objects, to better
        accommodate both legacy code as well as when passing results from raw SQL
        result sets back into new range or multirange expressions.

    .. change::
        :tags: usecase, engine
        :tickets: 8717

        Added new parameter :paramref:`.PoolEvents.reset.reset_state` parameter to
        the :meth:`.PoolEvents.reset` event, with deprecation logic in place that
        will continue to accept event hooks using the previous set of arguments.
        This indicates various state information about how the reset is taking
        place and is used to allow custom reset schemes to take place with full
        context given.

        Within this change a fix that's also backported to 1.4 is included which
        re-enables the :meth:`.PoolEvents.reset` event to continue to take place
        under all circumstances, including when :class:`.Connection` has already
        "reset" the connection.

        The two changes together allow custom reset schemes to be implemented using
        the :meth:`.PoolEvents.reset` event, instead of the
        :meth:`.PoolEvents.checkin` event (which continues to function as it always
        has).

    .. change::
        :tags: bug, orm, declarative
        :tickets: 8705

        Changed a fundamental configuration behavior of :class:`.Mapper`, where
        :class:`_schema.Column` objects that are explicitly present in the
        :paramref:`_orm.Mapper.properties` dictionary, either directly or enclosed
        within a mapper property object, will now be mapped within the order of how
        they appear within the mapped :class:`.Table` (or other selectable) itself
        (assuming they are in fact part of that table's list of columns), thereby
        maintaining the same order of columns in the mapped selectable as is
        instrumented on the mapped class, as well as what renders in an ORM SELECT
        statement for that mapper. Previously (where "previously" means since
        version 0.0.1), :class:`.Column` objects in the
        :paramref:`_orm.Mapper.properties` dictionary would always be mapped first,
        ahead of when the other columns in the mapped :class:`.Table` would be
        mapped, causing a discrepancy in the order in which the mapper would
        assign attributes to the mapped class as well as the order in which they
        would render in statements.

        The change most prominently takes place in the way that Declarative
        assigns declared columns to the :class:`.Mapper`, specifically how
        :class:`.Column` (or :func:`_orm.mapped_column`) objects are handled
        when they have a DDL name that is explicitly different from the mapped
        attribute name, as well as when constructs such as :func:`_orm.deferred`
        etc. are used.   The new behavior will see the column ordering within
        the mapped :class:`.Table` being the same order in which the attributes
        are mapped onto the class, assigned within the :class:`.Mapper` itself,
        and rendered in ORM statements such as SELECT statements, independent
        of how the :class:`_schema.Column` was configured against the
        :class:`.Mapper`.

    .. change::
        :tags: feature, engine
        :tickets: 8710

        To better support the use case of iterating :class:`.Result` and
        :class:`.AsyncResult` objects where user-defined exceptions may interrupt
        the iteration, both objects as well as variants such as
        :class:`.ScalarResult`, :class:`.MappingResult`,
        :class:`.AsyncScalarResult`, :class:`.AsyncMappingResult` now support
        context manager usage, where the result will be closed at the end of
        the context manager block.

        In addition, ensured that all the above
        mentioned :class:`.Result` objects include a :meth:`.Result.close` method
        as well as :attr:`.Result.closed` accessors, including
        :class:`.ScalarResult` and :class:`.MappingResult` which previously did
        not have a ``.close()`` method.

        .. seealso::

            :ref:`change_8710`


    .. change::
        :tags: bug, typing

        Corrected various typing issues within the engine and async engine
        packages.

    .. change::
        :tags: bug, orm, declarative
        :tickets: 8718

        Fixed issue in new dataclass mapping feature where a column declared on the
        decalrative base / abstract base / mixin would leak into the constructor
        for an inheriting subclass under some circumstances.

    .. change::
        :tags: bug, orm declarative
        :tickets: 8742

        Fixed issues within the declarative typing resolver (i.e. which resolves
        ``ForwardRef`` objects) where types that were declared for columns in one
        particular source file would raise ``NameError`` when the ultimate mapped
        class were in another source file.  The types are now resolved in terms
        of the module for each class in which the types are used.

    .. change::
        :tags: feature, postgresql
        :tickets: 8706

        Added new methods :meth:`_postgresql.Range.contains` and
        :meth:`_postgresql.Range.contained_by` to the new :class:`.Range` data
        object, which mirror the behavior of the PostgreSQL ``@>`` and ``<@``
        operators, as well as the
        :meth:`_postgresql.AbstractRange.comparator_factory.contains` and
        :meth:`_postgresql.AbstractRange.comparator_factory.contained_by` SQL
        operator methods. Pull request courtesy Lele Gaifax.

.. changelog::
    :version: 2.0.0b2
    :released: January 26, 2023
    :released: October 20, 2022

    .. change::
        :tags: bug, orm
        :tickets: 8656

        Removed the warning that emits when using ORM-enabled update/delete
        regarding evaluation of columns by name, first added in :ticket:`4073`;
        this warning actually covers up a scenario that otherwise could populate
        the wrong Python value for an ORM mapped attribute depending on what the
        actual column is, so this deprecated case is removed. In 2.0, ORM enabled
        update/delete uses "auto" for "synchronize_session", which should do the
        right thing automatically for any given UPDATE expression.

    .. change::
        :tags: bug, mssql
        :tickets: 8661

        Fixed regression caused by SQL Server pyodbc change :ticket:`8177` where we
        now use ``setinputsizes()`` by default; for VARCHAR, this fails if the
        character size is greater than 4000 (or 2000, depending on data) characters
        as the incoming datatype is NVARCHAR, which has a limit of 4000 characters,
        despite the fact that VARCHAR can handle unlimited characters. Additional
        pyodbc-specific typing information is now passed to ``setinputsizes()``
        when the datatype's size is > 2000 characters. The change is also applied
        to the :class:`_types.JSON` type which was also impacted by this issue for large
        JSON serializations.

    .. change::
        :tags: bug, typing
        :tickets: 8645

        Fixed typing issue where pylance strict mode would report "instance
        variable overrides class variable" when using a method to define
        ``__tablename__``, ``__mapper_args__`` or ``__table_args__``.

    .. change::
        :tags: mssql, bug
        :tickets: 7211

        The :class:`.Sequence` construct restores itself to the DDL behavior it
        had prior to the 1.4 series, where creating a :class:`.Sequence` with
        no additional arguments will emit a simple ``CREATE SEQUENCE`` instruction
        **without** any additional parameters for "start value".   For most backends,
        this is how things worked previously in any case; **however**, for
        MS SQL Server, the default value on this database is
        ``-2**63``; to prevent this generally impractical default
        from taking effect on SQL Server, the :paramref:`.Sequence.start` parameter
        should be provided.   As usage of :class:`.Sequence` is unusual
        for SQL Server which for many years has standardized on ``IDENTITY``,
        it is hoped that this change has minimal impact.

        .. seealso::

            :ref:`change_7211`

    .. change::
        :tags: bug, declarative, orm
        :tickets: 8665

        Improved the :class:`.DeclarativeBase` class so that when combined with
        other mixins like :class:`.MappedAsDataclass`, the order of the classes may
        be in either order.


    .. change::
        :tags: usecase, declarative, orm
        :tickets: 8665

        Added support for mapped classes that are also ``Generic`` subclasses,
        to be specified as a ``GenericAlias`` object (e.g. ``MyClass[str]``)
        within statements and calls to :func:`_sa.inspect`.



    .. change::
        :tags: bug, orm, declarative
        :tickets: 8668

        Fixed bug in new ORM typed declarative mappings where the ability
        to use ``Optional[MyClass]`` or similar forms such as ``MyClass | None``
        in the type annotation for a many-to-one relationship was not implemented,
        leading to errors.   Documentation has also been added for this use
        case to the relationship configuration documentation.

    .. change::
        :tags: bug, typing
        :tickets: 8644

        Fixed typing issue where pylance strict mode would report "partially
        unknown" datatype for the :func:`_orm.mapped_column` construct.

    .. change::
        :tags: bug, regression, sql
        :tickets: 8639

        Fixed bug in new "insertmanyvalues" feature where INSERT that included a
        subquery with :func:`_sql.bindparam` inside of it would fail to render
        correctly in "insertmanyvalues" format. This affected psycopg2 most
        directly as "insertmanyvalues" is used unconditionally with this driver.


    .. change::
        :tags: bug, orm, declarative
        :tickets: 8688

        Fixed issue with new dataclass mapping feature where arguments passed to
        the dataclasses API could sometimes be mis-ordered when dealing with mixins
        that override :func:`_orm.mapped_column` declarations, leading to
        initializer problems.

.. changelog::
    :version: 2.0.0b1
    :released: January 26, 2023
    :released: October 13, 2022

    .. change::
        :tags: bug, sql
        :tickets: 7888

        The FROM clauses that are established on a :func:`_sql.select` construct
        when using the :meth:`_sql.Select.select_from` method will now render first
        in the FROM clause of the rendered SELECT, which serves to maintain the
        ordering of clauses as was passed to the :meth:`_sql.Select.select_from`
        method itself without being affected by the presence of those clauses also
        being mentioned in other parts of the query. If other elements of the
        :class:`_sql.Select` also generate FROM clauses, such as the columns clause
        or WHERE clause, these will render after the clauses delivered by
        :meth:`_sql.Select.select_from` assuming they were not explictly passed to
        :meth:`_sql.Select.select_from` also. This improvement is useful in those
        cases where a particular database generates a desirable query plan based on
        a particular ordering of FROM clauses and allows full control over the
        ordering of FROM clauses.

    .. change::
        :tags: usecase, sql
        :tickets: 7998

        Altered the compilation mechanics of the :class:`_dml.Insert` construct
        such that the "autoincrement primary key" column value will be fetched via
        ``cursor.lastrowid`` or RETURNING even if present in the parameter set or
        within the :meth:`_dml.Insert.values` method as a plain bound value, for
        single-row INSERT statements on specific backends that are known to
        generate autoincrementing values even when explicit NULL is passed. This
        restores a behavior that was in the 1.3 series for both the use case of
        separate parameter set as well as :meth:`_dml.Insert.values`. In 1.4, the
        parameter set behavior unintentionally changed to no longer do this, but
        the :meth:`_dml.Insert.values` method would still fetch autoincrement
        values up until 1.4.21 where :ticket:`6770` changed the behavior yet again
        again unintentionally as this use case was never covered.

        The behavior is now defined as "working" to suit the case where databases
        such as SQLite, MySQL and MariaDB will ignore an explicit NULL primary key
        value and nonetheless invoke an autoincrement generator.

    .. change::
        :tags: change, postgresql

        SQLAlchemy now requires PostgreSQL version 9 or greater.
        Older versions may still work in some limited use cases.

    .. change::
        :tags: bug, orm

        Fixed issue where the :meth:`_orm.registry.map_declaratively` method
        would return an internal "mapper config" object and not the
        :class:`.Mapper` object as stated in the API documentation.

    .. change::
        :tags: sybase, removed
        :tickets: 7258

        Removed the "sybase" internal dialect that was deprecated in previous
        SQLAlchemy versions.  Third party dialect support is available.

        .. seealso::

            :ref:`external_toplevel`

    .. change::
        :tags: bug, orm
        :tickets: 7463

        Fixed performance regression which appeared at least in version 1.3 if not
        earlier (sometime after 1.0) where the loading of deferred columns, those
        explicitly mapped with :func:`_orm.defer` as opposed to non-deferred
        columns that were expired, from a joined inheritance subclass would not use
        the "optimized" query which only queried the immediate table that contains
        the unloaded columns, instead running a full ORM query which would emit a
        JOIN for all base tables, which is not necessary when only loading columns
        from the subclass.


    .. change::
        :tags: bug, sql
        :tickets: 7791

        The :paramref:`.Enum.length` parameter, which sets the length of the
        ``VARCHAR`` column for non-native enumeration types, is now used
        unconditionally when emitting DDL for the ``VARCHAR`` datatype, including
        when the :paramref:`.Enum.native_enum` parameter is set to ``True`` for
        target backends that continue to use ``VARCHAR``. Previously the parameter
        would be erroneously ignored in this case. The warning previously emitted
        for this case is now removed.

    .. change::
        :tags: bug, orm
        :tickets: 6986

        The internals for the :class:`_orm.Load` object and related loader strategy
        patterns have been mostly rewritten, to take advantage of the fact that
        only attribute-bound paths, not strings, are now supported. The rewrite
        hopes to make it more straightforward to address new use cases and subtle
        issues within the loader strategy system going forward.

    .. change::
        :tags: usecase, orm

        Added :paramref:`_orm.load_only.raiseload` parameter to the
        :func:`_orm.load_only` loader option, so that the unloaded attributes may
        have "raise" behavior rather than lazy loading. Previously there wasn't
        really a way to do this with the :func:`_orm.load_only` option directly.

    .. change::
        :tags: change, engine
        :tickets: 7122

        Some small API changes regarding engines and dialects:

        * The :meth:`.Dialect.set_isolation_level`, :meth:`.Dialect.get_isolation_level`,
          :meth:
          dialect methods will always be passed the raw DBAPI connection

        * The :class:`.Connection` and :class:`.Engine` classes no longer share a base
          ``Connectable`` superclass, which has been removed.

        * Added a new interface class :class:`.PoolProxiedConnection` - this is the
          public facing interface for the familiar :class:`._ConnectionFairy`
          class which is nonetheless a private class.

    .. change::
        :tags: feature, sql
        :tickets: 3482

          Added long-requested case-insensitive string operators
          :meth:`_sql.ColumnOperators.icontains`,
          :meth:`_sql.ColumnOperators.istartswith`,
          :meth:`_sql.ColumnOperators.iendswith`, which produce case-insensitive
          LIKE compositions (using ILIKE on PostgreSQL, and the LOWER() function on
          all other backends) to complement the existing LIKE composition operators
          :meth:`_sql.ColumnOperators.contains`,
          :meth:`_sql.ColumnOperators.startswith`, etc. Huge thanks to Matias
          Martinez Rebori for their meticulous and complete efforts in implementing
          these new methods.

    .. change::
        :tags: usecase, postgresql
        :tickets: 8138

        Added literal type rendering for the :class:`_sqltypes.ARRAY` and
        :class:`_postgresql.ARRAY` datatypes. The generic stringify will render
        using brackets, e.g. ``[1, 2, 3]`` and the PostgreSQL specific will use the
        ARRAY literal e.g. ``ARRAY[1, 2, 3]``.   Multiple dimensions and quoting
        are also taken into account.

    .. change::
        :tags: bug, orm
        :tickets: 8166

        Made an improvement to the "deferred" / "load_only" set of strategy options
        where if a certain object is loaded from two different logical paths within
        one query, attributes that have been configured by at least one of the
        options to be populated will be populated in all cases, even if other load
        paths for that same object did not set this option. previously, it was
        based on randomness as to which "path" addressed the object first.

    .. change::
        :tags: feature, orm, sql
        :tickets: 6047

        Added new feature to all included dialects that support RETURNING
        called "insertmanyvalues".  This is a generalization of the
        "fast executemany" feature first introduced for the psycopg2 driver
        in 1.4 at :ref:`change_5263`, which allows the ORM to batch INSERT
        statements into a much more efficient SQL structure while still being
        able to fetch newly generated primary key and SQL default values
        using RETURNING.

        The feature now applies to the many dialects that support RETURNING along
        with multiple VALUES constructs for INSERT, including all PostgreSQL
        drivers, SQLite, MariaDB, MS SQL Server. Separately, the Oracle dialect
        also gains the same capability using native cx_Oracle or OracleDB features.

    .. change::
        :tags: bug, engine
        :tickets: 8523

        The :class:`_pool.QueuePool` now ignores ``max_overflow`` when
        ``pool_size=0``, properly making the pool unlimited in all cases.

    .. change::
        :tags: bug, sql
        :tickets: 7909

        The in-place type detection for Python integers, as occurs with an
        expression such as ``literal(25)``, will now apply value-based adaption as
        well to accommodate Python large integers, where the datatype determined
        will be :class:`.BigInteger` rather than :class:`.Integer`. This
        accommodates for dialects such as that of asyncpg which both sends implicit
        typing information to the driver as well as is sensitive to numeric scale.

    .. change::
        :tags: postgresql, mssql, change
        :tickets: 7225

        The parameter :paramref:`_types.UUID.as_uuid` of :class:`_types.UUID`,
        previously specific to the PostgreSQL dialect but now generalized for Core
        (along with a new backend-agnostic :class:`_types.Uuid` datatype) now
        defaults to ``True``, indicating that Python ``UUID`` objects are accepted
        by this datatype by default. Additionally, the SQL Server
        :class:`_mssql.UNIQUEIDENTIFIER` datatype has been converted to be a
        UUID-receiving type; for legacy code that makes use of
        :class:`_mssql.UNIQUEIDENTIFIER` using string values, set the
        :paramref:`_mssql.UNIQUEIDENTIFIER.as_uuid` parameter to ``False``.

    .. change::
        :tags: bug, orm
        :tickets: 8344

        Fixed issue in ORM enabled UPDATE when the statement is created against a
        joined-inheritance subclass, updating only local table columns, where the
        "fetch" synchronization strategy would not render the correct RETURNING
        clause for databases that use RETURNING for fetch synchronization.
        Also adjusts the strategy used for RETURNING in UPDATE FROM and
        DELETE FROM statements.

    .. change::
        :tags: usecase, mariadb
        :tickets: 8344

        Added a new execution option ``is_delete_using=True``, which is consumed
        by the ORM when using an ORM-enabled DELETE statement in conjunction with
        the "fetch" synchronization strategy; this option indicates that the
        DELETE statement is expected to use multiple tables, which on MariaDB
        is the DELETE..USING syntax.   The option then indicates that
        RETURNING (newly implemented in SQLAlchemy 2.0 for MariaDB
        for  :ticket:`7011`) should not be used for databases that are known
        to not support "DELETE..USING..RETURNING" syntax, even though they
        support "DELETE..USING", which is MariaDB's current capability.

        The rationale for this option is that the current workings of ORM-enabled
        DELETE doesn't know up front if a DELETE statement is against multiple
        tables or not until compilation occurs, which is cached in any case, yet it
        needs to be known so that a SELECT for the to-be-deleted row can be emitted
        up front. Instead of applying an across-the-board performance penalty for
        all DELETE statements by proactively checking them all for this
        relatively unusual SQL pattern, the ``is_delete_using=True`` execution
        option is requested via a new exception message that is raised
        within the compilation step.  This exception message is specifically
        (and only) raised when:   the statement is an ORM-enabled DELETE where
        the "fetch" synchronization strategy has been requested; the
        backend is MariaDB or other backend with this specific limitation;
        the statement has been detected within the initial compilation
        that it would otherwise emit "DELETE..USING..RETURNING".   By applying
        the execution option, the ORM knows to run a SELECT upfront instead.
        A similar option is implemented for ORM-enabled UPDATE but there is not
        currently a backend where it is needed.



    .. change::
        :tags: bug, orm, asyncio
        :tickets: 7703

        Removed the unused ``**kw`` arguments from
        :class:`_asyncio.AsyncSession.begin` and
        :class:`_asyncio.AsyncSession.begin_nested`. These kw aren't used and
        appear to have been added to the API in error.

    .. change::
        :tags: feature, sql
        :tickets: 8285

        Added new syntax to the :attr:`.FromClause.c` collection on all
        :class:`.FromClause` objects allowing tuples of keys to be passed to
        ``__getitem__()``, along with support for the :func:`_sql.select` construct
        to handle the resulting tuple-like collection directly, allowing the syntax
        ``select(table.c['a', 'b', 'c'])`` to be possible. The sub-collection
        returned is itself a :class:`.ColumnCollection` which is also directly
        consumable by :func:`_sql.select` and similar now.

        .. seealso::

            :ref:`tutorial_selecting_columns`

    .. change::
        :tags: general, changed
        :tickets: 7257

        Migrated the codebase to remove all pre-2.0 behaviors and architectures
        that were previously noted as deprecated for removal in 2.0, including,
        but not limited to:

        * removal of all Python 2 code, minimum version is now Python 3.7

        * :class:`_engine.Engine` and :class:`_engine.Connection` now use the
          new 2.0 style of working, which includes "autobegin", library level
          autocommit removed, subtransactions and "branched" connections
          removed

        * Result objects use 2.0-style behaviors; :class:`_result.Row` is fully
          a named tuple without "mapping" behavior, use :class:`_result.RowMapping`
          for "mapping" behavior

        * All Unicode encoding/decoding architecture has been removed from
          SQLAlchemy.  All modern DBAPI implementations support Unicode
          transparently thanks to Python 3, so the ``convert_unicode`` feature
          as well as related mechanisms to look for bytestrings in
          DBAPI ``cursor.description`` etc. have been removed.

        * The ``.bind`` attribute and parameter from :class:`.MetaData`,
          :class:`.Table`, and from all DDL/DML/DQL elements that previously could
          refer to a "bound engine"

        * The standalone ``sqlalchemy.orm.mapper()`` function is removed; all
          classical mapping should be done through the
          :meth:`_orm.registry.map_imperatively` method of :class:`_orm.registry`.

        * The :meth:`_orm.Query.join` method no longer accepts strings for
          relationship names; the long-documented approach of using
          ``Class.attrname`` for join targets is now standard.

        * :meth:`_orm.Query.join` no longer accepts the "aliased" and
          "from_joinpoint" arguments

        * :meth:`_orm.Query.join` no longer accepts chains of multiple join
          targets in one method call.

        * ``Query.from_self()``, ``Query.select_entity_from()`` and
          ``Query.with_polymorphic()`` are removed.

        * The :paramref:`_orm.relationship.cascade_backrefs` parameter must now
          remain at its new default of ``False``; the ``save-update`` cascade
          no longer cascades along a backref.

        * the :paramref:`_orm.Session.future` parameter must always be set to
          ``True``.  2.0-style transactional patterns for :class:`_orm.Session`
          are now always in effect.

        * Loader options no longer accept strings for attribute names.  The
          long-documented approach of using ``Class.attrname`` for loader option
          targets is now standard.

        * Legacy forms of :func:`_sql.select` removed, including
          ``select([cols])``, the "whereclause" and keyword parameters of
          ``some_table.select()``.

        * Legacy "in-place mutator" methods on :class:`_sql.Select` such as
          ``append_whereclause()``, ``append_order_by()`` etc are removed.

        * Removed the very old "dbapi_proxy" module, which in very early
          SQLAlchemy releases was used to provide a transparent connection pool
          over a raw DBAPI connection.

    .. change::
        :tags: feature, orm
        :tickets: 8375

        Added new parameter :paramref:`_orm.AttributeEvents.include_key`, which
        will include the dictionary or list key for operations such as
        ``__setitem__()`` (e.g. ``obj[key] = value``) and ``__delitem__()`` (e.g.
        ``del obj[key]``), using a new keyword parameter "key" or "keys", depending
        on event, e.g. :paramref:`_orm.AttributeEvents.append.key`,
        :paramref:`_orm.AttributeEvents.bulk_replace.keys`. This allows event
        handlers to take into account the key that was passed to the operation and
        is of particular importance for dictionary operations working with
        :class:`_orm.MappedCollection`.


    .. change::
        :tags: postgresql, usecase
        :tickets: 7156, 8540

        Adds support for PostgreSQL multirange types, introduced in PostgreSQL 14.
        Support for PostgreSQL ranges and multiranges has now been generalized to
        the psycopg3, psycopg2 and asyncpg backends, with room for further dialect
        support, using a backend-agnostic :class:`_postgresql.Range` data object
        that's constructor-compatible with the previously used psycopg2 object. See
        the new documentation for usage patterns.

        In addition, range type handling has been enhanced so that it automatically
        renders type casts, so that in-place round trips for statements that don't
        provide the database with any context don't require the :func:`_sql.cast`
        construct to be explicit for the database to know the desired type
        (discussed at :ticket:`8540`).

        Thanks very much to @zeeeeeb for the pull request implementing and testing
        the new datatypes and psycopg support.

        .. seealso::

            :ref:`change_7156`

            :ref:`postgresql_ranges`

    .. change::
        :tags: usecase, oracle
        :tickets: 8221

        Oracle will now use FETCH FIRST N ROWS / OFFSET syntax for limit/offset
        support by default for Oracle 12c and above. This syntax was already
        available when :meth:`_sql.Select.fetch` were used directly, it's now
        implied for :meth:`_sql.Select.limit` and :meth:`_sql.Select.offset` as
        well.


    .. change::
        :tags: feature, orm
        :tickets: 3162

        Added new parameter :paramref:`_sql.Operators.op.python_impl`, available
        from :meth:`_sql.Operators.op` and also when using the
        :class:`_sql.Operators.custom_op` constructor directly, which allows an
        in-Python evaluation function to be provided along with the custom SQL
        operator. This evaluation function becomes the implementation used when the
        operator object is used given plain Python objects as operands on both
        sides, and in particular is compatible with the
        ``synchronize_session='evaluate'`` option used with
        :ref:`orm_expression_update_delete`.

    .. change::
        :tags: schema, postgresql
        :tickets: 5677

        Added support for comments on :class:`.Constraint` objects, including
        DDL and reflection; the field is added to the base :class:`.Constraint`
        class and corresponding constructors, however PostgreSQL is the only
        included backend to support the feature right now.
        See parameters such as :paramref:`.ForeignKeyConstraint.comment`,
        :paramref:`.UniqueConstraint.comment` or
        :paramref:`.CheckConstraint.comment`.

    .. change::
        :tags: sqlite, usecase
        :tickets: 8234

        Added new parameter to SQLite for reflection methods called
        ``sqlite_include_internal=True``; when omitted, local tables that start
        with the prefix ``sqlite_``, which per SQLite documentation are noted as
        "internal schema" tables such as the ``sqlite_sequence`` table generated to
        support "AUTOINCREMENT" columns, will not be included in reflection methods
        that return lists of local objects. This prevents issues for example when
        using Alembic autogenerate, which previously would consider these
        SQLite-generated tables as being remove from the model.

        .. seealso::

            :ref:`sqlite_include_internal`

    .. change::
        :tags: feature, postgresql
        :tickets: 7316

        Added a new PostgreSQL :class:`_postgresql.DOMAIN` datatype, which follows
        the same CREATE TYPE / DROP TYPE behaviors as that of PostgreSQL
        :class:`_postgresql.ENUM`. Much thanks to David Baumgold for the efforts on
        this.

        .. seealso::

            :class:`_postgresql.DOMAIN`

    .. change::
        :tags: change, postgresql

        The :paramref:`_postgresql.ENUM.name` parameter for the PostgreSQL-specific
        :class:`_postgresql.ENUM` datatype is now a required keyword argument. The
        "name" is necessary in any case in order for the :class:`_postgresql.ENUM`
        to be usable as an error would be raised at SQL/DDL render time if "name"
        were not present.

    .. change::
        :tags: oracle, feature
        :tickets: 8054

        Add support for the new oracle driver ``oracledb``.

        .. seealso::

            :ref:`ticket_8054`

            :ref:`oracledb`

    .. change::
        :tags: bug, engine
        :tickets: 8567

        For improved security, the :class:`_url.URL` object will now use password
        obfuscation by default when ``str(url)`` is called. To stringify a URL with
        cleartext password, the :meth:`_url.URL.render_as_string` may be used,
        passing the :paramref:`_url.URL.render_as_string.hide_password` parameter
        as ``False``. Thanks to our contributors for this pull request.

        .. seealso::

            :ref:`change_8567`

    .. change::
        :tags: change, orm

        To better accommodate explicit typing, the names of some ORM constructs
        that are typically constructed internally, but nonetheless are sometimes
        visible in messaging as well as typing, have been changed to more succinct
        names which also match the name of their constructing function (with
        different casing), in all cases maintaining aliases to the old names for
        the forseeable future:

        * :class:`_orm.RelationshipProperty` becomes an alias for the primary name
          :class:`_orm.Relationship`, which is constructed as always from the
          :func:`_orm.relationship` function
        * :class:`_orm.SynonymProperty` becomes an alias for the primary name
          :class:`_orm.Synonym`, constructed as always from the
          :func:`_orm.synonym` function
        * :class:`_orm.CompositeProperty` becomes an alias for the primary name
          :class:`_orm.Composite`, constructed as always from the
          :func:`_orm.composite` function

    .. change::
        :tags: orm, change
        :tickets: 8608

        For consistency with the prominent ORM concept :class:`_orm.Mapped`, the
        names of the dictionary-oriented collections,
        :func:`_orm.attribute_mapped_collection`,
        :func:`_orm.column_mapped_collection`, and :class:`_orm.MappedCollection`,
        are changed to :func:`_orm.attribute_keyed_dict`,
        :func:`_orm.column_keyed_dict` and :class:`_orm.KeyFuncDict`, using the
        phrase "dict" to minimize any confusion against the term "mapped". The old
        names will remain indefinitely with no schedule for removal.

    .. change::
        :tags: bug, sql
        :tickets: 7354

        Added ``if_exists`` and ``if_not_exists`` parameters for all "Create" /
        "Drop" constructs including :class:`.CreateSequence`,
        :class:`.DropSequence`, :class:`.CreateIndex`, :class:`.DropIndex`, etc.
        allowing generic "IF EXISTS" / "IF NOT EXISTS" phrases to be rendered
        within DDL. Pull request courtesy Jesse Bakker.


    .. change::
        :tags: engine, usecase
        :tickets: 6342

        Generalized the :paramref:`_sa.create_engine.isolation_level` parameter to
        the base dialect so that it is no longer dependent on individual dialects
        to be present. This parameter sets up the "isolation level" setting to
        occur for all new database connections as soon as they are created by the
        connection pool, where the value then stays set without being reset on
        every checkin.

        The :paramref:`_sa.create_engine.isolation_level` parameter is essentially
        equivalent in functionality to using the
        :paramref:`_engine.Engine.execution_options.isolation_level` parameter via
        :meth:`_engine.Engine.execution_options` for an engine-wide setting. The
        difference is in that the former setting assigns the isolation level just
        once when a connection is created, the latter sets and resets the given
        level on each connection checkout.

    .. change::
        :tags: bug, orm
        :tickets: 8372

        Changed the attribute access method used by
        :func:`_orm.attribute_mapped_collection` and
        :func:`_orm.column_mapped_collection` (now called
        :func:`_orm.attribute_keyed_dict` and :func:`_orm.column_keyed_dict`) ,
        used when populating the dictionary, to assert that the data value on
        the object to be used as the dictionary key is actually present, and is
        not instead using "None" due to the attribute never being actually
        assigned. This is used to prevent a mis-population of None for a key
        when assigning via a backref where the "key" attribute on the object is
        not yet assigned.

        As the failure mode here is a transitory condition that is not typically
        persisted to the database, and is easy to produce via the constructor of
        the class based on the order in which parameters are assigned, it is very
        possible that many applications include this behavior already which is
        silently passed over. To accommodate for applications where this error is
        now raised, a new parameter
        :paramref:`_orm.attribute_keyed_dict.ignore_unpopulated_attribute`
        is also added to both :func:`_orm.attribute_keyed_dict` and
        :func:`_orm.column_keyed_dict` that instead causes the erroneous
        backref assignment to be skipped.

    .. change::
        :tags: usecase, postgresql
        :tickets: 8491

        The "ping" query emitted when configuring
        :paramref:`_sa.create_engine.pool_pre_ping` for psycopg, asyncpg and
        pg8000, but not for psycopg2, has been changed to be an empty query (``;``)
        instead of ``SELECT 1``; additionally, for the asyncpg driver, the
        unnecessary use of a prepared statement for this query has been fixed.
        Rationale is to eliminate the need for PostgreSQL to produce a query plan
        when the ping is emitted. The operation is not currently supported by the
        ``psycopg2`` driver which continues to use ``SELECT 1``.

    .. change::
        :tags: bug, oracle
        :tickets: 7494

        Adjustments made to the BLOB / CLOB / NCLOB datatypes in the cx_Oracle and
        oracledb dialects, to improve performance based on recommendations from
        Oracle developers.

    .. change::
        :tags: feature, orm
        :tickets: 7433

        The :class:`_orm.Session` (and by extension :class:`.AsyncSession`) now has
        new state-tracking functionality that will proactively trap any unexpected
        state changes which occur as a particular transactional method proceeds.
        This is to allow situations where the :class:`_orm.Session` is being used
        in a thread-unsafe manner, where event hooks or similar may be calling
        unexpected methods within operations, as well as potentially under other
        concurrency situations such as asyncio or gevent to raise an informative
        message when the illegal access first occurs, rather than passing silently
        leading to secondary failures due to the :class:`_orm.Session` being in an
        invalid state.

        .. seealso::

            :ref:`change_7433`

    .. change::
        :tags: postgresql, dialect
        :tickets: 6842

        Added support for ``psycopg`` dialect supporting both sync and async
        execution. This dialect is available under the ``postgresql+psycopg`` name
        for both the :func:`_sa.create_engine` and
        :func:`_asyncio.create_async_engine` engine-creation functions.

        .. seealso::

            :ref:`ticket_6842`

            :ref:`postgresql_psycopg`



    .. change::
        :tags: usecase, sqlite
        :tickets: 6195

        Added RETURNING support for the SQLite dialect.  SQLite supports RETURNING
        since version 3.35.


    .. change::
        :tags: usecase, mariadb
        :tickets: 7011

        Added INSERT..RETURNING and DELETE..RETURNING support for the MariaDB
        dialect.  UPDATE..RETURNING is not yet supported by MariaDB.  MariaDB
        supports INSERT..RETURNING as of 10.5.0 and DELETE..RETURNING as of
        10.0.5.



    .. change::
        :tags: feature, orm

        The :func:`_orm.composite` mapping construct now supports automatic
        resolution of values when used with a Python ``dataclass``; the
        ``__composite_values__()`` method no longer needs to be implemented as this
        method is derived from inspection of the dataclass.

        Additionally, classes mapped by :class:`_orm.composite` now support
        ordering comparison operations, e.g. ``<``, ``>=``, etc.

        See the new documentation at :ref:`mapper_composite` for examples.

    .. change::
        :tags: engine, bug
        :tickets: 7161

        The :meth:`_engine.Inspector.has_table` method will now consistently check
        for views of the given name as well as tables. Previously this behavior was
        dialect dependent, with PostgreSQL, MySQL/MariaDB and SQLite supporting it,
        and Oracle and SQL Server not supporting it. Third party dialects should
        also seek to ensure their :meth:`_engine.Inspector.has_table` method
        searches for views as well as tables for the given name.

    .. change::
        :tags: feature, engine
        :tickets: 5648

        The :meth:`.DialectEvents.handle_error` event is now moved to the
        :class:`.DialectEvents` suite from the :class:`.EngineEvents` suite, and
        now participates in the connection pool "pre ping" event for those dialects
        that make use of disconnect codes in order to detect if the database is
        live. This allows end-user code to alter the state of "pre ping". Note that
        this does not include dialects which contain a native "ping" method such as
        that of psycopg2 or most MySQL dialects.

    .. change::
        :tags: feature, sql
        :tickets: 7212

        Added new backend-agnostic :class:`_types.Uuid` datatype generalized from
        the PostgreSQL dialects to now be a core type, as well as migrated
        :class:`_types.UUID` from the PostgreSQL dialect. The SQL Server
        :class:`_mssql.UNIQUEIDENTIFIER` datatype also becomes a UUID-handling
        datatype. Thanks to Trevor Gross for the help on this.

    .. change::
        :tags: feature, orm
        :tickets: 8126

        Added very experimental feature to the :func:`_orm.selectinload` and
        :func:`_orm.immediateload` loader options called
        :paramref:`_orm.selectinload.recursion_depth` /
        :paramref:`_orm.immediateload.recursion_depth` , which allows a single
        loader option to automatically recurse into self-referential relationships.
        Is set to an integer indicating depth, and may also be set to -1 to
        indicate to continue loading until no more levels deep are found.
        Major internal changes to :func:`_orm.selectinload` and
        :func:`_orm.immediateload` allow this feature to work while continuing
        to make correct use of the compilation cache, as well as not using
        arbitrary recursion, so any level of depth is supported (though would
        emit that many queries).  This may be useful for
        self-referential structures that must be loaded fully eagerly, such as when
        using asyncio.

        A warning is also emitted when loader options are connected together with
        arbitrary lengths (that is, without using the new ``recursion_depth``
        option) when excessive recursion depth is detected in related object
        loading. This operation continues to use huge amounts of memory and
        performs extremely poorly; the cache is disabled when this condition is
        detected to protect the cache from being flooded with arbitrary statements.

    .. change::
        :tags: bug, orm
        :tickets: 8403

        Added new parameter :paramref:`.AbstractConcreteBase.strict_attrs` to the
        :class:`.AbstractConcreteBase` declarative mixin class. The effect of this
        parameter is that the scope of attributes on subclasses is correctly
        limited to the subclass in which each attribute is declared, rather than
        the previous behavior where all attributes of the entire hierarchy are
        applied to the base "abstract" class. This produces a cleaner, more correct
        mapping where subclasses no longer have non-useful attributes on them which
        are only relevant to sibling classes. The default for this parameter is
        False, which leaves the previous behavior unchanged; this is to support
        existing code that makes explicit use of these attributes in queries.
        To migrate to the newer approach, apply explicit attributes to the abstract
        base class as needed.

    .. change::
        :tags: usecase, mysql, mariadb
        :tickets: 8503

        The ``ROLLUP`` function will now correctly render ``WITH ROLLUP`` on
        MySql and MariaDB, allowing the use of group by rollup with these
        backend.

    .. change::
        :tags: feature, orm
        :tickets: 6928

        Added new parameter :paramref:`_orm.Session.autobegin`, which when set to
        ``False`` will prevent the :class:`_orm.Session` from beginning a
        transaction implicitly. The :meth:`_orm.Session.begin` method must be
        called explicitly first in order to proceed with operations, otherwise an
        error is raised whenever any operation would otherwise have begun
        automatically. This option can be used to create a "safe"
        :class:`_orm.Session` that won't implicitly start new transactions.

        As part of this change, also added a new status variable
        :class:`_orm.SessionTransaction.origin` which may be useful for event
        handling code to be aware of the origin of a particular
        :class:`_orm.SessionTransaction`.



    .. change::
        :tags: feature, platform
        :tickets: 7256

        The SQLAlchemy C extensions have been replaced with all new implementations
        written in Cython.  Like the C extensions before, pre-built wheel files
        for a wide range of platforms are available on pypi so that building
        is not an issue for common platforms.  For custom builds, ``python setup.py build_ext``
        works as before, needing only the additional Cython install.  ``pyproject.toml``
        is also part of the source now which will establish the proper build dependencies
        when using pip.


        .. seealso::

            :ref:`change_7256`

    .. change::
        :tags: change, platform
        :tickets: 7311

        SQLAlchemy's source build and installation now includes a ``pyproject.toml`` file
        for full :pep:`517` support.

        .. seealso::

            :ref:`change_7311`

    .. change::
        :tags: feature, schema
        :tickets: 7631

        Expanded on the "conditional DDL" system implemented by the
        :class:`_schema.ExecutableDDLElement` class (renamed from
        :class:`_schema.DDLElement`) to be directly available on
        :class:`_schema.SchemaItem` constructs such as :class:`_schema.Index`,
        :class:`_schema.ForeignKeyConstraint`, etc. such that the conditional logic
        for generating these elements is included within the default DDL emitting
        process. This system can also be accommodated by a future release of
        Alembic to support conditional DDL elements within all schema-management
        systems.


        .. seealso::

            :ref:`ticket_7631`

    .. change::
        :tags: change, oracle
        :tickets:`4379`

        Materialized views on oracle are now reflected as views.
        On previous versions of SQLAlchemy the views were returned among
        the table names, not among the view names. As a side effect of
        this change they are not reflected by default by
        :meth:`_sql.MetaData.reflect`, unless ``views=True`` is set.
        To get a list of materialized views, use the new
        inspection method :meth:`.Inspector.get_materialized_view_names`.

    .. change::
        :tags: bug, sqlite
        :tickets: 7299

        Removed the warning that emits from the :class:`_types.Numeric` type about
        DBAPIs not supporting Decimal values natively. This warning was oriented
        towards SQLite, which does not have any real way without additional
        extensions or workarounds of handling precision numeric values more than 15
        significant digits as it only uses floating point math to represent
        numbers. As this is a known and documented limitation in SQLite itself, and
        not a quirk of the pysqlite driver, there's no need for SQLAlchemy to warn
        for this. The change does not otherwise modify how precision numerics are
        handled. Values can continue to be handled as ``Decimal()`` or ``float()``
        as configured with the :class:`_types.Numeric`, :class:`_types.Float` , and
        related datatypes, just without the ability to maintain precision beyond 15
        significant digits when using SQLite, unless alternate representations such
        as strings are used.

    .. change::
        :tags: mssql, bug
        :tickets: 8177

        The ``use_setinputsizes`` parameter for the ``mssql+pyodbc`` dialect now
        defaults to ``True``; this is so that non-unicode string comparisons are
        bound by pyodbc to pyodbc.SQL_VARCHAR rather than pyodbc.SQL_WVARCHAR,
        allowing indexes against VARCHAR columns to take effect. In order for the
        ``fast_executemany=True`` parameter to continue functioning, the
        ``use_setinputsizes`` mode now skips the ``cursor.setinputsizes()`` call
        specifically when ``fast_executemany`` is True and the specific method in
        use is ``cursor.executemany()``, which doesn't support setinputsizes. The
        change also adds appropriate pyodbc DBAPI typing to values that are typed
        as :class:`_types.Unicode` or :class:`_types.UnicodeText`, as well as
        altered the base :class:`_types.JSON` datatype to consider JSON string
        values as :class:`_types.Unicode` rather than :class:`_types.String`.

    .. change::
        :tags: bug, sqlite, performance
        :tickets: 7490

        The SQLite dialect now defaults to :class:`_pool.QueuePool` when a file
        based database is used. This is set along with setting the
        ``check_same_thread`` parameter to ``False``. It has been observed that the
        previous approach of defaulting to :class:`_pool.NullPool`, which does not
        hold onto database connections after they are released, did in fact have a
        measurable negative performance impact. As always, the pool class is
        customizable via the :paramref:`_sa.create_engine.poolclass` parameter.

        .. seealso::

            :ref:`change_7490`


    .. change::
        :tags: usecase, schema
        :tickets: 8141

        Added parameter :paramref:`_ddl.DropConstraint.if_exists` to the
        :class:`_ddl.DropConstraint` construct which result in "IF EXISTS" DDL
        being added to the DROP statement.
        This phrase is not accepted by all databases and the operation will fail
        on a database that does not support it as there is no similarly compatible
        fallback within the scope of a single DDL statement.
        Pull request courtesy Mike Fiedler.

    .. change::
        :tags: change, postgresql

        In support of new PostgreSQL features including the psycopg3 dialect as
        well as extended "fast insertmany" support, the system by which typing
        information for bound parameters is passed to the PostgreSQL database has
        been redesigned to use inline casts emitted by the SQL compiler, and is now
        applied to all PostgreSQL dialects. This is in contrast to the previous
        approach which would rely upon the DBAPI in use to render these casts
        itself, which in cases such as that of pg8000 and the adapted asyncpg
        driver, would use the pep-249 ``setinputsizes()`` method, or with the
        psycopg2 driver would rely on the driver itself in most cases, with some
        special exceptions made for ARRAY.

        The new approach now has all PostgreSQL dialects rendering these casts as
        needed using PostgreSQL double-colon style within the compiler, and the use
        of ``setinputsizes()`` is removed for PostgreSQL dialects, as this was not
        generally part of these DBAPIs in any case (pg8000 being the only
        exception, which added the method at the request of SQLAlchemy developers).

        Advantages to this approach include per-statement performance, as no second
        pass over the compiled statement is required at execution time, better
        support for all DBAPIs, as there is now one consistent system of applying
        typing information, and improved transparency, as the SQL logging output,
        as well as the string output of a compiled statement, will show these casts
        present in the statement directly, whereas previously these casts were not
        visible in logging output as they would occur after the statement were
        logged.



    .. change::
        :tags: engine, removed

        Removed the previously deprecated ``case_sensitive`` parameter from
        :func:`_sa.create_engine`, which would impact only the lookup of string
        column names in Core-only result set rows; it had no effect on the behavior
        of the ORM. The effective behavior of what ``case_sensitive`` refers
        towards remains at its default value of ``True``, meaning that string names
        looked up in ``row._mapping`` will match case-sensitively, just like any
        other Python mapping.

        Note that the ``case_sensitive`` parameter was not in any way related to
        the general subject of case sensitivity control, quoting, and "name
        normalization" (i.e. converting for databases that consider all uppercase
        words to be case insensitive) for DDL identifier names, which remains a
        normal core feature of SQLAlchemy.



    .. change::
        :tags: bug, sql
        :tickets: 7744

        Improved the construction of SQL binary expressions to allow for very long
        expressions against the same associative operator without special steps
        needed in order to avoid high memory use and excess recursion depth. A
        particular binary operation ``A op B`` can now be joined against another
        element ``op C`` and the resulting structure will be "flattened" so that
        the representation as well as SQL compilation does not require recursion.

        One effect of this change is that string concatenation expressions which
        use SQL functions come out as "flat", e.g. MySQL will now render
        ``concat('x', 'y', 'z', ...)``` rather than nesting together two-element
        functions like ``concat(concat('x', 'y'), 'z')``.  Third-party dialects
        which override the string concatenation operator will need to implement
        a new method ``def visit_concat_op_expression_clauselist()`` to
        accompany the existing ``def visit_concat_op_binary()`` method.

    .. change::
        :tags: feature, sql
        :tickets: 5465

        Added :class:`.Double`, :class:`.DOUBLE`,
        :class:`_sqltypes.DOUBLE_PRECISION`
        datatypes to the base ``sqlalchemy.`` module namespace, for explicit use of
        double/double precision as well as generic "double" datatypes. Use
        :class:`.Double` for generic support that will resolve to DOUBLE/DOUBLE
        PRECISION/FLOAT as needed for different backends.


    .. change::
        :tags: feature, oracle
        :tickets: 5465

        Implemented DDL and reflection support for ``FLOAT`` datatypes which
        include an explicit "binary_precision" value. Using the Oracle-specific
        :class:`_oracle.FLOAT` datatype, the new parameter
        :paramref:`_oracle.FLOAT.binary_precision` may be specified which will
        render Oracle's precision for floating point types directly. This value is
        interpreted during reflection. Upon reflecting back a ``FLOAT`` datatype,
        the datatype returned is one of :class:`_types.DOUBLE_PRECISION` for a
        ``FLOAT`` for a precision of 126 (this is also Oracle's default precision
        for ``FLOAT``), :class:`_types.REAL` for a precision of 63, and
        :class:`_oracle.FLOAT` for a custom precision, as per Oracle documentation.

        As part of this change, the generic :paramref:`_sqltypes.Float.precision`
        value is explicitly rejected when generating DDL for Oracle, as this
        precision cannot be accurately converted to "binary precision"; instead, an
        error message encourages the use of
        :meth:`_sqltypes.TypeEngine.with_variant` so that Oracle's specific form of
        precision may be chosen exactly. This is a backwards-incompatible change in
        behavior, as the previous "precision" value was silently ignored for
        Oracle.

        .. seealso::

            :ref:`change_5465_oracle`

    .. change::
        :tags: postgresql, psycopg2
        :tickets: 7238

        Update psycopg2 dialect to use the DBAPI interface to execute
        two phase transactions. Previously SQL commands were execute
        to handle this kind of transactions.

    .. change::
        :tags: deprecations, engine
        :tickets: 6962

        The :paramref:`_sa.create_engine.implicit_returning` parameter is
        deprecated on the :func:`_sa.create_engine` function only; the parameter
        remains available on the :class:`_schema.Table` object. This parameter was
        originally intended to enable the "implicit returning" feature of
        SQLAlchemy when it was first developed and was not enabled by default.
        Under modern use, there's no reason this parameter should be disabled, and
        it has been observed to cause confusion as it degrades performance and
        makes it more difficult for the ORM to retrieve recently inserted server
        defaults. The parameter remains available on :class:`_schema.Table` to
        specifically suit database-level edge cases which make RETURNING
        infeasible, the sole example currently being SQL Server's limitation that
        INSERT RETURNING may not be used on a table that has INSERT triggers on it.


    .. change::
        :tags: bug, oracle
        :tickets: 6962

        Related to the deprecation for
        :paramref:`_sa.create_engine.implicit_returning`, the "implicit_returning"
        feature is now enabled for the Oracle dialect in all cases; previously, the
        feature would be turned off when an Oracle 8/8i version were detected,
        however online documentation indicates both versions support the same
        RETURNING syntax as modern versions.

    .. change::
        :tags: bug, schema
        :tickets: 8102

        The warnings that are emitted regarding reflection of indexes or unique
        constraints, when the :paramref:`.Table.include_columns` parameter is used
        to exclude columns that are then found to be part of those constraints,
        have been removed. When the :paramref:`.Table.include_columns` parameter is
        used it should be expected that the resulting :class:`.Table` construct
        will not include constraints that rely upon omitted columns. This change
        was made in response to :ticket:`8100` which repaired
        :paramref:`.Table.include_columns` in conjunction with foreign key
        constraints that rely upon omitted columns, where the use case became
        clear that omitting such constraints should be expected.

    .. change::
        :tags: bug, postgresql
        :tickets: 7086

        The :meth:`.Operators.match` operator now uses ``plainto_tsquery()`` for
        PostgreSQL full text search, rather than ``to_tsquery()``. The rationale
        for this change is to provide better cross-compatibility with match on
        other database backends.    Full support for all PostgreSQL full text
        functions remains available through the use of :data:`.func` in
        conjunction with :meth:`.Operators.bool_op` (an improved version of
        :meth:`.Operators.op` for boolean operators).

        .. seealso::

            :ref:`change_7086`

    .. change::
        :tags: usecase, sql
        :tickets: 5052

        Added modified ISO-8601 rendering (i.e. ISO-8601 with the T converted to a
        space) when using ``literal_binds`` with the SQL compilers provided by the
        PostgreSQL, MySQL, MariaDB, MSSQL, Oracle dialects. For Oracle, the ISO
        format is wrapped inside of an appropriate TO_DATE() function call.
        Previously this rendering was not implemented for dialect-specific
        compilation.

        .. seealso::

            :ref:`change_5052`

    .. change::
        :tags: removed, engine
        :tickets: 7258

        Removed legacy and deprecated package ``sqlalchemy.databases``.
        Please use ``sqlalchemy.dialects`` instead.

    .. change::
        :tags: usecase, schema
        :tickets: 8394

        Implemented the DDL event hooks :meth:`.DDLEvents.before_create`,
        :meth:`.DDLEvents.after_create`, :meth:`.DDLEvents.before_drop`,
        :meth:`.DDLEvents.after_drop` for all :class:`.SchemaItem` objects that
        include a distinct CREATE or DROP step, when that step is invoked as a
        distinct SQL statement, including for :class:`.ForeignKeyConstraint`,
        :class:`.Sequence`, :class:`.Index`, and PostgreSQL's
        :class:`_postgresql.ENUM`.

    .. change::
        :tags: engine, feature

        The :meth:`.ConnectionEvents.set_connection_execution_options`
        and :meth:`.ConnectionEvents.set_engine_execution_options`
        event hooks now allow the given options dictionary to be modified
        in-place, where the new contents will be received as the ultimate
        execution options to be acted upon. Previously, in-place modifications to
        the dictionary were not supported.

    .. change::
        :tags: bug, sql
        :tickets: 4926

        Implemented full support for "truediv" and "floordiv" using the
        "/" and "//" operators.  A "truediv" operation between two expressions
        using :class:`_types.Integer` now considers the result to be
        :class:`_types.Numeric`, and the dialect-level compilation will cast
        the right operand to a numeric type on a dialect-specific basis to ensure
        truediv is achieved.  For floordiv, conversion is also added for those
        databases that don't already do floordiv by default (MySQL, Oracle) and
        the ``FLOOR()`` function is rendered in this case, as well as for
        cases where the right operand is not an integer (needed for PostgreSQL,
        others).

        The change resolves issues both with inconsistent behavior of the
        division operator on different backends and also fixes an issue where
        integer division on Oracle would fail to be able to fetch a result due
        to inappropriate outputtypehandlers.

        .. seealso::

            :ref:`change_4926`

    .. change::
        :tags: postgresql, schema
        :tickets: 8216

        Introduced the type :class:`_postgresql.JSONPATH` that can be used
        in cast expressions. This is required by some PostgreSQL dialects
        when using functions such as ``jsonb_path_exists`` or
        ``jsonb_path_match`` that accept a ``jsonpath`` as input.

        .. seealso::

            :ref:`postgresql_json_types` - PostgreSQL JSON types.

    .. change::
        :tags: schema, mysql, mariadb
        :tickets: 4038

        Add support for Partitioning and Sample pages on MySQL and MariaDB
        reflected options.
        The options are stored in the table dialect options dictionary, so
        the following keyword need to be prefixed with ``mysql_`` or ``mariadb_``
        depending on the backend.
        Supported options are:

        * ``stats_sample_pages``
        * ``partition_by``
        * ``partitions``
        * ``subpartition_by``

        These options are also reflected when loading a table from database,
        and will populate the table :attr:`_schema.Table.dialect_options`.
        Pull request courtesy of Ramon Will.

    .. change::
        :tags: usecase, mssql
        :tickets: 8288

        Implemented reflection of the "clustered index" flag ``mssql_clustered``
        for the SQL Server dialect. Pull request courtesy John Lennox.

    .. change::
        :tags: reflection, postgresql
        :tickets: 7442

        The PostgreSQL dialect now supports reflection of expression based indexes.
        The reflection is supported both when using
        :meth:`_engine.Inspector.get_indexes` and when reflecting a
        :class:`_schema.Table` using :paramref:`_schema.Table.autoload_with`.
        Thanks to immerrr and Aidan Kane for the help on this ticket.

    .. change::
        :tags: firebird, removed
        :tickets: 7258

        Removed the "firebird" internal dialect that was deprecated in previous
        SQLAlchemy versions.  Third party dialect support is available.

        .. seealso::

            :ref:`external_toplevel`

    .. change::
        :tags: bug, orm
        :tickets: 7495

        The behavior of :func:`_orm.defer` regarding primary key and "polymorphic
        discriminator" columns is revised such that these columns are no longer
        deferrable, either explicitly or when using a wildcard such as
        ``defer('*')``. Previously, a wildcard deferral would not load
        PK/polymorphic columns which led to errors in all cases, as the ORM relies
        upon these columns to produce object identities. The behavior of explicit
        deferral of primary key columns is unchanged as these deferrals already
        were implicitly ignored.

    .. change::
        :tags: bug, sql
        :tickets: 7471

        Added an additional lookup step to the compiler which will track all FROM
        clauses which are tables, that may have the same name shared in multiple
        schemas where one of the schemas is the implicit "default" schema; in this
        case, the table name when referring to that name without a schema
        qualification will be rendered with an anonymous alias name at the compiler
        level in order to disambiguate the two (or more) names. The approach of
        schema-qualifying the normally unqualified name with the server-detected
        "default schema name" value was also considered, however this approach
        doesn't apply to Oracle nor is it accepted by SQL Server, nor would it work
        with multiple entries in the PostgreSQL search path. The name collision
        issue resolved here has been identified as affecting at least Oracle,
        PostgreSQL, SQL Server, MySQL and MariaDB.


    .. change::
        :tags: improvement, typing
        :tickets: 6980

        The :meth:`_sqltypes.TypeEngine.with_variant` method now returns a copy of
        the original :class:`_sqltypes.TypeEngine` object, rather than wrapping it
        inside the ``Variant`` class, which is effectively removed (the import
        symbol remains for backwards compatibility with code that may be testing
        for this symbol). While the previous approach maintained in-Python
        behaviors, maintaining the original type allows for clearer type checking
        and debugging.

        :meth:`_sqltypes.TypeEngine.with_variant` also accepts multiple dialect
        names per call as well, in particular this is helpful for related
        backend names such as ``"mysql", "mariadb"``.

        .. seealso::

            :ref:`change_6980`




    .. change::
        :tags: usecase, sqlite, performance
        :tickets: 7029

        SQLite datetime, date, and time datatypes now use Python standard lib
        ``fromisoformat()`` methods in order to parse incoming datetime, date, and
        time string values. This improves performance vs. the previous regular
        expression-based approach, and also automatically accommodates for datetime
        and time formats that contain either a six-digit "microseconds" format or a
        three-digit "milliseconds" format.

    .. change::
        :tags: usecase, mssql
        :tickets: 7844

        Added support table and column comments on MSSQL when
        creating a table. Added support for reflecting table comments.
        Thanks to Daniel Hall for the help in this pull request.

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

        Removed support for multiple deprecated drivers:

            - pypostgresql for PostgreSQL. This is available as an
              external driver at https://github.com/PyGreSQL
            - pygresql for PostgreSQL.

        Please switch to one of the supported drivers or to the external
        version of the same driver.

    .. change::
        :tags: bug, engine
        :tickets: 7953

        Fixed issue in :meth:`.Result.columns` method where calling upon
        :meth:`.Result.columns` with a single index could in some cases,
        particularly ORM result object cases, cause the :class:`.Result` to yield
        scalar objects rather than :class:`.Row` objects, as though the
        :meth:`.Result.scalars` method had been called. In SQLAlchemy 1.4, this
        scenario emits a warning that the behavior will change in SQLAlchemy 2.0.

    .. change::
        :tags: usecase, sql
        :tickets: 7759

        Added new parameter :paramref:`.HasCTE.add_cte.nest_here` to
        :meth:`.HasCTE.add_cte` which will "nest" a given :class:`.CTE` at the
        level of the parent statement. This parameter is equivalent to using the
        :paramref:`.HasCTE.cte.nesting` parameter, but may be more intuitive in
        some scenarios as it allows the nesting attribute to be set simultaneously
        along with the explicit level of the CTE.

        The :meth:`.HasCTE.add_cte` method also accepts multiple CTE objects.

    .. change::
        :tags: bug, orm
        :tickets: 7438

        Fixed bug in the behavior of the :paramref:`_orm.Mapper.eager_defaults`
        parameter such that client-side SQL default or onupdate expressions in the
        table definition alone will trigger a fetch operation using RETURNING or
        SELECT when the ORM emits an INSERT or UPDATE for the row. Previously, only
        server side defaults established as part of table DDL and/or server-side
        onupdate expressions would trigger this fetch, even though client-side SQL
        expressions would be included when the fetch was rendered.

    .. change::
        :tags: performance, schema
        :tickets: 4379

        Rearchitected the schema reflection API to allow participating dialects to
        make use of high performing batch queries to reflect the schemas of many
        tables at once using fewer queries by an order of magnitude. The
        new performance features are targeted first at the PostgreSQL and Oracle
        backends, and may be applied to any dialect that makes use of SELECT
        queries against system catalog tables to reflect tables. The change also
        includes new API features and behavioral improvements to the
        :class:`.Inspector` object, including consistent, cached behavior of
        methods like :meth:`.Inspector.has_table`,
        :meth:`.Inspector.get_table_names` and new methods
        :meth:`.Inspector.has_schema` and :meth:`.Inspector.has_index`.

        .. seealso::

            :ref:`change_4379` - full background


    .. change::
        :tags: bug, engine

        Passing a :class:`.DefaultGenerator` object such as a :class:`.Sequence` to
        the :meth:`.Connection.execute` method is deprecated, as this method is
        typed as returning a :class:`.CursorResult` object, and not a plain scalar
        value. The :meth:`.Connection.scalar` method should be used instead, which
        has been reworked with new internal codepaths to suit invoking a SELECT for
        default generation objects without going through the
        :meth:`.Connection.execute` method.

    .. change::
        :tags: usecase, sqlite
        :tickets: 7185

        The SQLite dialect now supports UPDATE..FROM syntax, for UPDATE statements
        that may refer to additional tables within the WHERE criteria of the
        statement without the need to use subqueries. This syntax is invoked
        automatically when using the :class:`_dml.Update` construct when more than
        one table or other entity or selectable is used.

    .. change::
        :tags: general, changed

        The :meth:`_orm.Query.instances` method is deprecated.  The behavioral
        contract of this method, which is that it can iterate objects through
        arbitrary result sets, is long obsolete and no longer tested.
        Arbitrary statements can return objects by using constructs such
        as :meth`.Select.from_statement` or :func:`_orm.aliased`.

    .. change::
        :tags: feature, orm

        Declarative mixins which use :class:`_schema.Column` objects that contain
        :class:`_schema.ForeignKey` references no longer need to use
        :func:`_orm.declared_attr` to achieve this mapping; the
        :class:`_schema.ForeignKey` object is copied along with the
        :class:`_schema.Column` itself when the column is applied to the declared
        mapping.

    .. change::
        :tags: oracle, feature
        :tickets: 6245

        Full "RETURNING" support is implemented for the cx_Oracle dialect, covering
        two individual types of functionality:

        * multi-row RETURNING is implemented, meaning multiple RETURNING rows are
          now received for DML statements that produce more than one row for
          RETURNING.
        * "executemany RETURNING" is also implemented - this allows RETURNING to
          yield row-per statement when ``cursor.executemany()`` is used.
          The implementation of this part of the feature delivers dramatic
          performance improvements to ORM inserts, in the same way as was
          added for psycopg2 in the SQLAlchemy 1.4 change :ref:`change_5263`.


    .. change::
        :tags: oracle

        cx_Oracle 7 is now the minimum version for cx_Oracle.

    .. change::
        :tags: bug, sql
        :tickets: 7551

        Python string values for which a SQL type is determined from the type of
        the value, mainly when using :func:`_sql.literal`, will now apply the
        :class:`_types.String` type, rather than the :class:`_types.Unicode`
        datatype, for Python string values that test as "ascii only" using Python
        ``str.isascii()``. If the string is not ``isascii()``, the
        :class:`_types.Unicode` datatype will be bound instead, which was used in
        all string detection previously. This behavior **only applies to in-place
        detection of datatypes when using ``literal()`` or other contexts that have
        no existing datatype**, which is not usually the case under normal
        :class:`_schema.Column` comparison operations, where the type of the
        :class:`_schema.Column` being compared always takes precedence.

        Use of the :class:`_types.Unicode` datatype can determine literal string
        formatting on backends such as SQL Server, where a literal value (i.e.
        using ``literal_binds``) will be rendered as ``N'<value>'`` instead of
        ``'value'``. For normal bound value handling, the :class:`_types.Unicode`
        datatype also may have implications for passing values to the DBAPI, again
        in the case of SQL Server, the pyodbc driver supports the use of
        :ref:`setinputsizes mode <mssql_pyodbc_setinputsizes>` which will handle
        :class:`_types.String` versus :class:`_types.Unicode` differently.


    .. change::
        :tags: bug, sql
        :tickets: 7083

        The :class:`_functions.array_agg` will now set the array dimensions to 1.
        Improved :class:`_types.ARRAY` processing to accept ``None`` values as
        value of a multi-array.
