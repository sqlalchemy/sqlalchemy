=============
2.1 Changelog
=============

.. changelog_imports::

    .. include:: changelog_20.rst
        :start-line: 5


.. changelog::
    :version: 2.1.0b2
    :include_notes_from: unreleased_21

.. changelog::
    :version: 2.1.0b1
    :released: January 21, 2026

    .. change::
        :tags: feature, orm
        :tickets: 10050

        The :paramref:`_orm.relationship.back_populates` argument to
        :func:`_orm.relationship` may now be passed as a Python callable, which
        resolves to either the direct linked ORM attribute, or a string value as
        before.  ORM attributes are also accepted directly by
        :paramref:`_orm.relationship.back_populates`.   This change allows type
        checkers and IDEs to confirm the argument for
        :paramref:`_orm.relationship.back_populates` is valid. Thanks to Priyanshu
        Parikh for the help on suggesting and helping to implement this feature.

        .. seealso::

            :ref:`change_10050`


    .. change::
        :tags: change, platform
        :tickets: 10197

        The ``greenlet`` dependency used for asyncio support no longer installs
        by default.  This dependency does not publish wheel files for every architecture
        and is not needed for applications that aren't using asyncio features.
        Use the ``sqlalchemy[asyncio]`` install target to include this dependency.

        .. seealso::

            :ref:`change_10197`



    .. change::
        :tags: change, sql
        :tickets: 10236

        The ``.c`` and ``.columns`` attributes on the :class:`.Select` and
        :class:`.TextualSelect` constructs, which are not instances of
        :class:`.FromClause`, have been removed completely, in addition to the
        ``.select()`` method as well as other codepaths which would implicitly
        generate a subquery from a :class:`.Select` without the need to explicitly
        call the :meth:`.Select.subquery` method.

        In the case of ``.c`` and ``.columns``, these attributes were never useful
        in practice and have caused a great deal of confusion, hence were
        deprecated back in version 1.4, and have emitted warnings since that
        version.   Accessing the columns that are specific to a :class:`.Select`
        construct is done via the :attr:`.Select.selected_columns` attribute, which
        was added in version 1.4 to suit the use case that users often expected
        ``.c`` to accomplish.  In the larger sense, implicit production of
        subqueries works against SQLAlchemy's modern practice of making SQL
        structure as explicit as possible.

        Note that this is **not related** to the usual :attr:`.FromClause.c` and
        :attr:`.FromClause.columns` attributes, common to objects such as
        :class:`.Table` and :class:`.Subquery`,  which are unaffected by this
        change.

        .. seealso::

            :ref:`change_4617` - original notes from SQLAlchemy 1.4


    .. change::
        :tags: schema
        :tickets: 10247

        Deprecate Oracle only parameters :paramref:`_schema.Sequence.order`,
        :paramref:`_schema.Identity.order` and :paramref:`_schema.Identity.on_null`.
        They should be configured using the dialect kwargs ``oracle_order`` and
        ``oracle_on_null``.

    .. change::
        :tags: change, asyncio
        :tickets: 10296

        Added an initialize step to the import of
        ``sqlalchemy.ext.asyncio`` so that ``greenlet`` will
        be imported only when the asyncio extension is first imported.
        Alternatively, the ``greenlet`` library is still imported lazily on
        first use to support use case that don't make direct use of the
        SQLAlchemy asyncio extension.

    .. change::
        :tags: bug, sql
        :tickets: 10300

        The :class:`.Double` type is now used when a Python float value is detected
        as a literal value to be sent as a bound parameter, rather than the
        :class:`.Float` type.  :class:`.Double` has the same implementation as
        :class:`.Float`, but when rendered in a CAST, produces ``DOUBLE`` or
        ``DOUBLE PRECISION`` rather than ``FLOAT``.   The former better matches
        Python's ``float`` datatype which uses 8-byte double-precision storage.
        Third party dialects which don't support the :class:`.Double` type directly
        may need adjustment so that they render an appropriate keyword (e.g.
        ``FLOAT``) when the :class:`.Double` datatype is encountered.

        .. seealso::

            :ref:`change_10300`

    .. change::
        :tags: usecase, mariadb
        :tickets: 10339

        Modified the MariaDB dialect so that when using the :class:`_sqltypes.Uuid`
        datatype with  MariaDB >= 10.7, leaving the
        :paramref:`_sqltypes.Uuid.native_uuid` parameter at its default of True,
        the native ``UUID`` datatype will be rendered in DDL and used for database
        communication, rather than ``CHAR(32)`` (the non-native UUID type) as was
        the case previously.   This is a behavioral change since 2.0, where the
        generic :class:`_sqltypes.Uuid` datatype delivered ``CHAR(32)`` for all
        MySQL and MariaDB variants.   Support for all major DBAPIs is implemented
        including support for less common "insertmanyvalues" scenarios where UUID
        values are generated in different ways for primary keys.   Thanks much to
        Volodymyr Kochetkov for delivering the PR.


    .. change::
        :tags: change, asyncio
        :tickets: 10415

        Adapted all asyncio dialects, including aiosqlite, aiomysql, asyncmy,
        psycopg, asyncpg to use the generic asyncio connection adapter first added
        in :ticket:`6521` for the aioodbc DBAPI, allowing these dialects to take
        advantage of a common framework.

    .. change::
        :tags: change, orm
        :tickets: 10497

        A sweep through class and function names in the ORM renames many classes
        and functions that have no intent of public visibility to be underscored.
        This is to reduce ambiguity as to which APIs are intended to be targeted by
        third party applications and extensions.  Third parties are encouraged to
        propose new public APIs in Discussions to the extent they are needed to
        replace those that have been clarified as private.

    .. change::
        :tags: change, orm
        :tickets: 10500

        The ``first_init`` ORM event has been removed.  This event was
        non-functional throughout the 1.4 and 2.0 series and could not be invoked
        without raising an internal error, so it is not expected that there is any
        real-world use of this event hook.

    .. change::
        :tags: feature, postgresql
        :tickets: 10556

        Adds a new ``str`` subclass :class:`_postgresql.BitString` representing
        PostgreSQL bitstrings in python, that includes
        functionality for converting to and from ``int`` and ``bytes``, in
        addition to implementing utility methods and operators for dealing with bits.

        This new class is returned automatically by the :class:`postgresql.BIT` type.

        .. seealso::

            :ref:`change_10556`

    .. change::
        :tags: bug, orm
        :tickets: 10564

        The :paramref:`_orm.relationship.secondary` parameter no longer uses Python
        ``eval()`` to evaluate the given string.   This parameter when passed a
        string should resolve to a table name that's present in the local
        :class:`.MetaData` collection only, and never needs to be any kind of
        Python expression otherwise.  To use a real deferred callable based on a
        name that may not be locally present yet, use a lambda instead.

    .. change::
        :tags: usecase, postgresql
        :tickets: 10604

        Added new parameter :paramref:`.Enum.create_type` to the Core
        :class:`.Enum` class. This parameter is automatically passed to the
        corresponding :class:`_postgresql.ENUM` native type during DDL operations,
        allowing control over whether the PostgreSQL ENUM type is implicitly
        created or dropped within DDL operations that are otherwise targeting
        tables only. This provides control over the
        :paramref:`_postgresql.ENUM.create_type` behavior without requiring
        explicit creation of a :class:`_postgresql.ENUM` object.

    .. change::
        :tags: typing, feature
        :tickets: 10635

        The :class:`.Row` object now no longer makes use of an intermediary
        ``Tuple`` in order to represent its individual element types; instead,
        the individual element types are present directly, via new :pep:`646`
        integration, now available in more recent versions of Mypy.  Mypy
        1.7 or greater is now required for statements, results and rows
        to be correctly typed.   Pull request courtesy Yurii Karabas.

        .. seealso::

            :ref:`change_10635`

    .. change::
        :tags: typing
        :tickets: 10646

        The default implementation of :attr:`_types.TypeEngine.python_type` now
        returns ``object`` instead of ``NotImplementedError``, since that's the
        base for all types in Python3.
        The ``python_type`` of :class:`_types.JSON` no longer returns ``dict``,
        but instead fallbacks to the generic implementation.

    .. change::
        :tags: change, orm
        :tickets: 10721

        Removed legacy signatures dating back to 0.9 release from the
        :meth:`_orm.SessionEvents.after_bulk_update` and
        :meth:`_orm.SessionEvents.after_bulk_delete`.

    .. change::
        :tags: bug, sql
        :tickets: 10788

        Fixed issue in name normalization (e.g. "uppercase" backends like Oracle)
        where using a :class:`.TextualSelect` would not properly maintain as
        uppercase column names that were quoted as uppercase, even though
        the :class:`.TextualSelect` includes a :class:`.Column` that explicitly
        holds this uppercase name.

    .. change::
        :tags: usecase, engine
        :tickets: 10789

        Added new execution option
        :paramref:`_engine.Connection.execution_options.driver_column_names`. This
        option disables the "name normalize" step that takes place against the
        DBAPI ``cursor.description`` for uppercase-default backends like Oracle,
        and will cause the keys of a result set (e.g. named tuple names, dictionary
        keys in :attr:`.Row._mapping`, etc.) to be exactly what was delivered in
        cursor.description.   This is mostly useful for plain textual statements
        using :func:`_sql.text` or :meth:`_engine.Connection.exec_driver_sql`.

    .. change::
        :tags: bug, engine
        :tickets: 10802

        Fixed issue in "insertmanyvalues" feature where an INSERT..RETURNING
        that also made use of a sentinel column to track results would fail to
        filter out the additional column when :meth:`.Result.unique` were used
        to uniquify the result set.

    .. change::
        :tags: usecase, orm
        :tickets: 10816

        The :paramref:`_orm.Session.flush.objects` parameter is now
        deprecated.

    .. change::
        :tags: change, postgresql
        :tickets: 10821

        The :meth:`_types.ARRAY.Comparator.any` and
        :meth:`_types.ARRAY.Comparator.all` methods for the :class:`_types.ARRAY`
        type are now deprecated for removal; these two methods along with
        :func:`_postgresql.Any` and :func:`_postgresql.All` have been legacy for
        some time as they are superseded by the :func:`_sql.any_` and
        :func:`_sql.all_` functions, which feature more intuitive use.


    .. change::
        :tags: postgresql, feature
        :tickets: 10909

        Support for storage parameters in ``CREATE TABLE`` using the ``WITH``
        clause has been added. The ``postgresql_with`` dialect option of
        :class:`_schema.Table` accepts a mapping of key/value options.

        .. seealso::

            :ref:`postgresql_table_options_with` - in the PostgreSQL dialect
            documentation

    .. change::
        :tags: postgresql, usecase
        :tickets: 10909

        The PostgreSQL dialect now support reflection of table options, including
        the storage parameters, table access method and table spaces. These options
        are automatically reflected when autoloading a table, and are also
        available via the :meth:`_engine.Inspector.get_table_options` and
        :meth:`_engine.Inspector.get_multi_table_optionsmethod` methods.

    .. change::
        :tags: orm
        :tickets: 11045

        The :func:`_orm.noload` relationship loader option and related
        ``lazy='noload'`` setting is deprecated and will be removed in a future
        release.   This option was originally intended for custom loader patterns
        that are no longer applicable in modern SQLAlchemy.

    .. change::
        :tags: bug, sqlite
        :tickets: 11074

        Improved the behavior of JSON accessors :meth:`.JSON.Comparator.as_string`,
        :meth:`.JSON.Comparator.as_boolean`, :meth:`.JSON.Comparator.as_float`,
        :meth:`.JSON.Comparator.as_integer` to use CAST in a similar way that
        the PostgreSQL, MySQL and SQL Server dialects do to help enforce the
        expected Python type is returned.



    .. change::
        :tags: bug, mssql
        :tickets: 11074

        The :meth:`.JSON.Comparator.as_boolean` method when used on a JSON value on
        SQL Server will now force a cast to occur for values that are not simple
        `true`/`false` JSON literals, forcing SQL Server to attempt to interpret
        the given value as a 1/0 BIT, or raise an error if not possible. Previously
        the expression would return NULL.



    .. change::
        :tags: orm
        :tickets: 11163

        Ignore :paramref:`_orm.Session.join_transaction_mode` in all cases when
        the bind provided to the :class:`_orm.Session` is an
        :class:`_engine.Engine`.
        Previously if an event that executed before the session logic,
        like :meth:`_engine.ConnectionEvents.engine_connect`,
        left the connection with an active transaction, the
        :paramref:`_orm.Session.join_transaction_mode` behavior took
        place, leading to a surprising behavior.

    .. change::
        :tags: bug, orm
        :tickets: 11226

        Fixed issue where joined eager loading would fail to use the "nested" form
        of the query when GROUP BY or DISTINCT were present if the eager joins
        being added were many-to-ones, leading to additional columns in the columns
        clause which would then cause errors.  The check for "nested" is tuned to
        be enabled for these queries even for many-to-one joined eager loaders, and
        the "only do nested if it's one to many" aspect is now localized to when
        the query only has LIMIT or OFFSET added.

    .. change::
        :tags: bug, engine
        :tickets: 11234

        Adjusted URL parsing and stringification to apply url quoting to the
        "database" portion of the URL.  This allows a URL where the "database"
        portion includes special characters such as question marks to be
        accommodated.

        .. seealso::

            :ref:`change_11234`

    .. change::
        :tags: bug, mssql
        :tickets: 11250

        Fix mssql+pyodbc issue where valid plus signs in an already-unquoted
        ``odbc_connect=`` (raw DBAPI) connection string are replaced with spaces.

        The pyodbc connector would unconditionally pass the odbc_connect value
        to unquote_plus(), even if it was not required. So, if the (unquoted)
        odbc_connect value contained ``PWD=pass+word`` that would get changed to
        ``PWD=pass word``, and the login would fail. One workaround was to quote
        just the plus sign — ``PWD=pass%2Bword`` — which would then get unquoted
        to ``PWD=pass+word``.

    .. change::
        :tags: bug, orm
        :tickets: 11349

        Revised the set "binary" operators for the association proxy ``set()``
        interface to correctly raise ``TypeError`` for invalid use of the ``|``,
        ``&``, ``^``, and ``-`` operators, as well as the in-place mutation
        versions of these methods, to match the behavior of standard Python
        ``set()`` as well as SQLAlchemy ORM's "instrumented" set implementation.



    .. change::
        :tags: bug, sql
        :tickets: 11515

        Enhanced the caching structure of the :paramref:`_expression.over.rows`
        and :paramref:`_expression.over.range` so that different numerical
        values for the rows /
        range fields are cached on the same cache key, to the extent that the
        underlying SQL does not actually change (i.e. "unbounded", "current row",
        negative/positive status will still change the cache key).  This prevents
        the use of many different numerical range/rows value for a query that is
        otherwise identical from filling up the SQL cache.

        Note that the semi-private compiler method ``_format_frame_clause()``
        is removed by this fix, replaced with a new method
        ``visit_frame_clause()``.  Third party dialects which may have referred
        to this method will need to change the name and revise the approach to
        rendering the correct SQL for that dialect.


    .. change::
        :tags: feature, oracle
        :tickets: 11633

        Added support for native BOOLEAN support in Oracle Database 23c and above.
        The Oracle dialect now renders ``BOOLEAN`` automatically when
        :class:`.Boolean` is used in DDL, and also now supports direct use of the
        :class:`.BOOLEAN` datatype, when 23c and above is in use.  For Oracle
        versions prior to 23c, boolean values continue to be emulated using
        SMALLINT as before.   Special case handling is also present to ensure a
        SMALLINT that's interpreted with the :class:`.Boolean` datatype on Oracle
        Database 23c and above continues to return bool values. Pull request
        courtesy Yeongbae Jeon.

        .. seealso::

            :ref:`oracle_boolean_support`

    .. change::
        :tags: orm, usecase
        :tickets: 11776

        Added the utility method :meth:`_orm.Session.merge_all` and
        :meth:`_orm.Session.delete_all` that operate on a collection
        of instances.

    .. change::
        :tags: bug, schema
        :tickets: 11811

        The :class:`.Float` and :class:`.Numeric` types are no longer automatically
        considered as auto-incrementing columns when the
        :paramref:`_schema.Column.autoincrement` parameter is left at its default
        of ``"auto"`` on a :class:`_schema.Column` that is part of the primary key.
        When the parameter is set to ``True``, a :class:`.Numeric` type will be
        accepted as an auto-incrementing datatype for primary key columns, but only
        if its scale is explicitly given as zero; otherwise, an error is raised.
        This is a change from 2.0 where all numeric types including floats were
        automatically considered as "autoincrement" for primary key columns.

    .. change::
        :tags: bug, asyncio
        :tickets: 11956

        Refactored all asyncio dialects so that exceptions which occur on failed
        connection attempts are appropriately wrapped with SQLAlchemy exception
        objects, allowing for consistent error handling.

    .. change::
        :tags: bug, orm
        :tickets: 12168

        A significant behavioral change has been made to the behavior of the
        :paramref:`_orm.mapped_column.default` and
        :paramref:`_orm.relationship.default` parameters, as well as the
        :paramref:`_orm.relationship.default_factory` parameter with
        collection-based relationships, when used with SQLAlchemy's
        :ref:`orm_declarative_native_dataclasses` feature introduced in 2.0, where
        the given value (assumed to be an immutable scalar value for
        :paramref:`_orm.mapped_column.default` and a simple collection class for
        :paramref:`_orm.relationship.default_factory`) is no longer passed to the
        ``@dataclass`` API as a real default, instead a token that leaves the value
        un-set in the object's ``__dict__`` is used, in conjunction with a
        descriptor-level default.  This prevents an un-set default value from
        overriding a default that was actually set elsewhere, such as in
        relationship / foreign key assignment patterns as well as in
        :meth:`_orm.Session.merge` scenarios.   See the full writeup in the
        :ref:`migration_21_toplevel` document which includes guidance on how to
        re-enable the 2.0 version of the behavior if needed.

        .. seealso::

            :ref:`change_12168`

    .. change::
        :tags: feature, sql
        :tickets: 12195

        Added the ability to create custom SQL constructs that can define new
        clauses within SELECT, INSERT, UPDATE, and DELETE statements without
        needing to modify the construction or compilation code of of
        :class:`.Select`, :class:`_sql.Insert`, :class:`.Update`, or :class:`.Delete`
        directly.  Support for testing these constructs, including caching support,
        is present along with an example test suite.  The use case for these
        constructs is expected to be third party dialects for analytical SQL
        (so-called NewSQL) or other novel styles of database that introduce new
        clauses to these statements.   A new example suite is included which
        illustrates the ``QUALIFY`` SQL construct used by several NewSQL databases
        which includes a cacheable implementation as well as a test suite.

        .. seealso::

            :ref:`examples_syntax_extensions`


    .. change::
        :tags: sql
        :tickets: 12218

        Removed the automatic coercion of executable objects, such as
        :class:`_orm.Query`, when passed into :meth:`_orm.Session.execute`.
        This usage raised a deprecation warning since the 1.4 series.

    .. change::
        :tags: reflection, mysql, mariadb
        :tickets: 12240

        Updated the reflection logic for indexes in the MariaDB and MySQL
        dialect to avoid setting the undocumented ``type`` key in the
        :class:`_engine.ReflectedIndex` dicts returned by
        :class:`_engine.Inspector.get_indexes` method.

    .. change::
        :tags: typing, orm
        :tickets: 12293

        Removed the deprecated mypy plugin.
        The plugin was non-functional with newer version of mypy and it's no
        longer needed with modern SQLAlchemy declarative style.

    .. change::
        :tags: feature, postgresql
        :tickets: 12342

        Added syntax extension :func:`_postgresql.distinct_on` to build ``DISTINCT
        ON`` clauses. The old api, that passed columns to
        :meth:`_sql.Select.distinct`, is now deprecated.

    .. change::
        :tags: typing, orm
        :tickets: 12346

        Deprecated the ``declarative_mixin`` decorator since it was used only
        by the now removed mypy plugin.

    .. change::
        :tags: bug, orm
        :tickets: 12395

        The behavior of :func:`_orm.with_polymorphic` when used with a single
        inheritance mapping has been changed such that its behavior should match as
        closely as possible to that of an equivalent joined inheritance mapping.
        Specifically this means that the base class specified in the
        :func:`_orm.with_polymorphic` construct will be the basemost class that is
        loaded, as well as all descendant classes of that basemost class.
        The change includes that the descendant classes named will no longer be
        exclusively indicated in "WHERE polymorphic_col IN" criteria; instead, the
        whole hierarchy starting with the given basemost class will be loaded.  If
        the query indicates that rows should only be instances of a specific
        subclass within the polymorphic hierarchy, an error is raised if an
        incompatible superclass is loaded in the result since it cannot be made to
        match the requested class; this behavior is the same as what joined
        inheritance has done for many years. The change also allows a single result
        set to include column-level results from multiple sibling classes at once
        which was not previously possible with single table inheritance.

    .. change::
        :tags: orm, changed
        :tickets: 12437

        The "non primary" mapper feature, long deprecated in SQLAlchemy since
        version 1.3, has been removed.   The sole use case for "non primary"
        mappers was that of using :func:`_orm.relationship` to link to a mapped
        class against an alternative selectable; this use case is now suited by the
        :ref:`relationship_aliased_class` feature.



    .. change::
        :tags: misc, changed
        :tickets: 12441

        Removed multiple api that were deprecated in the 1.3 series and earlier.
        The list of removed features includes:

        * The ``force`` parameter of ``IdentifierPreparer.quote`` and
          ``IdentifierPreparer.quote_schema``;
        * The ``threaded`` parameter of the cx-Oracle dialect;
        * The ``_json_serializer`` and ``_json_deserializer`` parameters of the
          SQLite dialect;
        * The ``collection.converter`` decorator;
        * The ``Mapper.mapped_table`` property;
        * The ``Session.close_all`` method;
        * Support for multiple arguments in :func:`_orm.defer` and
          :func:`_orm.undefer`.

    .. change::
        :tags: core, feature, sql
        :tickets: 12479

          The Core operator system now includes the ``matmul`` operator, i.e. the
          ``@`` operator in Python as an optional operator.
          In addition to the ``__matmul__`` and ``__rmatmul__`` operator support
          this change also adds the missing ``__rrshift__`` and ``__rlshift__``.
          Pull request courtesy Aramís Segovia.

    .. change::
        :tags: feature, sql
        :tickets: 12496

        Added new Core feature :func:`_sql.from_dml_column` that may be used in
        expressions inside of :meth:`.UpdateBase.values` for INSERT or UPDATE; this
        construct will copy whatever SQL expression is used for the given target
        column in the statement to be used with additional columns. The construct
        is mostly intended to be a helper with ORM :class:`.hybrid_property` within
        DML hooks.

    .. change::
        :tags: feature, orm
        :tickets: 12496

        Added new hybrid method :meth:`.hybrid_property.bulk_dml` which
        works in a similar way as :meth:`.hybrid_property.update_expression` for
        bulk ORM operations.  A user-defined class method can now populate a bulk
        insert mapping dictionary using the desired hybrid mechanics.   New
        documentation is added showing how both of these methods can be used
        including in combination with the new :func:`_sql.from_dml_column`
        construct.

        .. seealso::

            :ref:`change_12496`

    .. change::
        :tags: feature, sql
        :tickets: 12548

        Added support for Python 3.14+ template strings (t-strings) via the new
        :func:`_sql.tstring` construct. This feature makes use of Python 3.14
        template strings as defined in :pep:`750`, allowing for ergonomic SQL
        statement construction by automatically interpolating Python values and
        SQLAlchemy expressions within template strings.

        .. seealso::

            :ref:`change_12548` - in :ref:`migration_21_toplevel`

    .. change::
        :tags: feature, orm
        :tickets: 12570

        Added new parameter :paramref:`_orm.composite.return_none_on` to
        :func:`_orm.composite`, which allows control over if and when this
        composite attribute should resolve to ``None`` when queried or retrieved
        from the object directly.  By default, a composite object is always present
        on the attribute, including for a pending object which is a behavioral
        change since 2.0.   When :paramref:`_orm.composite.return_none_on` is
        specified, a callable is passed that returns True or False to indicate if
        the given arguments indicate the composite should be returned as None. This
        parameter may also be set automatically when ORM Annotated Declarative is
        used; if the annotation is given as ``Mapped[SomeClass|None]``, a
        :paramref:`_orm.composite.return_none_on` rule is applied that will return
        ``None`` if all contained columns are themselves ``None``.

        .. seealso::

            :ref:`change_12570`

    .. change::
        :tags: bug, sql
        :tickets: 12596

        Updated the :func:`_sql.over` clause to allow non integer values in
        :paramref:`_sql.over.range_` clause. Previously, only integer values
        were allowed and any other values would lead to a failure.
        To specify a non-integer value, use the new :class:`_sql.FrameClause`
        construct along with the new :class:`_sql.FrameClauseType` enum to specify
        the frame boundaries. For example::

            from sqlalchemy import FrameClause, FrameClauseType

            select(
                func.sum(table.c.value).over(
                    range_=FrameClause(
                        3.14,
                        2.71,
                        FrameClauseType.PRECEDING,
                        FrameClauseType.FOLLOWING,
                    )
                )
            )

        .. seealso::

            :ref:`change_12596` - in the :ref:`migration guide
            <migration_21_toplevel>`

    .. change::
        :tags: usecase, orm
        :tickets: 12631

        Added support for using :func:`_orm.with_expression` to populate a
        :func:`_orm.query_expression` attribute that is also configured as the
        ``polymorphic_on`` discriminator column. The ORM now detects when a query
        expression column is serving as the polymorphic discriminator and updates
        it to use the column provided via :func:`_orm.with_expression`, enabling
        polymorphic loading to work correctly in this scenario. This allows for
        patterns such as where the discriminator value is computed from a related
        table.

    .. change::
        :tags: feature, orm
        :tickets: 12659

        Added support for per-session execution options that are merged into all
        queries executed within that session. The :class:`_orm.Session`,
        :class:`_orm.sessionmaker`, :class:`_orm.scoped_session`,
        :class:`_asyncio.AsyncSession`, and
        :class:`_asyncio.async_sessionmaker` constructors now accept an
        :paramref:`_orm.Session.execution_options` parameter that will be applied
        to all explicit query executions (e.g. using :meth:`_orm.Session.execute`,
        :meth:`_orm.Session.get`, :meth:`_orm.Session.scalars`) for that session
        instance.

    .. change::
        :tags: change, postgresql
        :tickets: 10594, 12690

        Named types such as :class:`_postgresql.ENUM` and
        :class:`_postgresql.DOMAIN` (as well as the dialect-agnostic
        :class:`_types.Enum` version) are now more strongly associated with the
        :class:`_schema.MetaData` at the top of the table hierarchy and are
        de-associated with any particular :class:`_schema.Table` they may be a part
        of. This better represents how PostgreSQL named types exist independently
        of any particular table, and that they may be used across many tables
        simultaneously.  The change impacts the behavior of the "default schema"
        for a named type, as well as the CREATE/DROP behavior in relationship to
        the :class:`.MetaData` and :class:`.Table` construct.  The change also
        includes a new :class:`.CheckFirst` enumeration which allows fine grained
        control over "check" queries during DDL operations, as well as that the
        :paramref:`_types.SchemaType.inherit_schema` parameter is deprecated and
        will emit a deprecation warning when used.  See the migration notes for
        full details.

        .. seealso::

            :ref:`change_10594_postgresql` - Complete details on PostgreSQL named type changes

    .. change::
        :tags: bug, sql
        :tickets: 12736

        Added a new concept of "operator classes" to the SQL operators supported by
        SQLAlchemy, represented within the enum :class:`.OperatorClass`.  The
        purpose of this structure is to provide an extra layer of validation when a
        particular kind of SQL operation is used with a particular datatype, to
        catch early the use of an operator that does not have any relevance to the
        datatype in use; a simple example is an integer or numeric column used with
        a "string match" operator.

        .. seealso::

            :ref:`change_12736`



    .. change::
        :tags: bug, postgresql
        :tickets: 12761

        A :class:`.CompileError` is raised if attempting to create a PostgreSQL
        :class:`_postgresql.ENUM` or :class:`_postgresql.DOMAIN` datatype using a
        name that matches a known pg_catalog datatype name, and a default schema is
        not specified.   These types must be explicit within a schema in order to
        be differentiated from the built-in pg_catalog type.  The "public" or
        otherwise default schema is not chosen by default here since the type can
        only be reflected back using the explicit schema name as well (it is
        otherwise not visible due to the pg_catalog name).  Pull request courtesy
        Kapil Dagur.



    .. change::
        :tags: bug, orm
        :tickets: 12769

        Improved the behavior of standalone "operators" like :func:`_sql.desc`,
        :func:`_sql.asc`, :func:`_sql.all_`, etc. so that they consult the given
        expression object for an overriding method for that operator, even if the
        object is not itself a ``ClauseElement``, such as if it's an ORM attribute.
        This allows custom comparators for things like :func:`_orm.composite` to
        provide custom implementations of methods like ``desc()``, ``asc()``, etc.


    .. change::
        :tags: usecase, orm
        :tickets: 12769

        Added default implementations of :meth:`.ColumnOperators.desc`,
        :meth:`.ColumnOperators.asc`, :meth:`.ColumnOperators.nulls_first`,
        :meth:`.ColumnOperators.nulls_last` to :func:`_orm.composite` attributes,
        by default applying the modifier to all contained columns.  Can be
        overridden using a custom comparator.

    .. change::
        :tags: usecase, orm
        :tickets: 12838

        The :func:`_orm.aliased` object now emits warnings when an attribute is
        accessed on an aliased class that cannot be located in the target
        selectable, for those cases where the :func:`_orm.aliased` is against a
        different FROM clause than the regular mapped table (such as a subquery).
        This helps users identify cases where column names don't match between the
        aliased class and the underlying selectable. When
        :paramref:`_orm.aliased.adapt_on_names` is ``True``, the warning suggests
        checking the column name; when ``False``, it suggests using the
        ``adapt_on_names`` parameter for name-based matching.

    .. change::
        :tags: bug, orm
        :tickets: 12843

        ORM entities can now be involved within the SQL expressions used within
        :paramref:`_orm.relationship.primaryjoin` and
        :paramref:`_orm.relationship.secondaryjoin` parameters without the ORM
        entity information being implicitly sanitized, allowing ORM-specific
        features such as single-inheritance criteria in subqueries to continue
        working even when used in this context.   This is made possible by overall
        ORM simplifications that occurred as of the 2.0 series.  The changes here
        also provide a performance boost (up to 20%) for certain query compilation
        scenarios.

    .. change::
        :tags: usecase, sql
        :tickets: 12853

        Added new generalized aggregate function ordering to functions via the
        :func:`_functions.FunctionElement.aggregate_order_by` method, which
        receives an expression and generates the appropriate embedded "ORDER BY" or
        "WITHIN GROUP (ORDER BY)" phrase depending on backend database.  This new
        function supersedes the use of the PostgreSQL
        :func:`_postgresql.aggregate_order_by` function, which remains present for
        backward compatibility.   To complement the new parameter, the
        :paramref:`_functions.aggregate_strings.order_by` which adds ORDER BY
        capability to the :class:`_functions.aggregate_strings` dialect-agnostic
        function which works for all included backends. Thanks much to Reuven
        Starodubski with help on this patch.



    .. change::
        :tags: usecase, orm
        :tickets: 12854

        Improvements to the use case of using :ref:`Declarative Dataclass Mapping
        <orm_declarative_native_dataclasses>` with intermediary classes that are
        unmapped.   As was the existing behavior, classes can subclass
        :class:`_orm.MappedAsDataclass` alone without a declarative base to act as
        mixins, or along with a declarative base as well as ``__abstract__ = True``
        to define an abstract base.  However, the improved behavior scans ORM
        attributes like :func:`_orm.mapped_column` in this case to create correct
        ``dataclasses.field()`` constructs based on their arguments, allowing for
        more natural ordering of fields without dataclass errors being thrown.
        Additionally, added a new :func:`_orm.unmapped_dataclass` decorator
        function, which may be used to create unmapped mixins in a mapped hierarchy
        that is using the :func:`_orm.mapped_dataclass` decorator to create mapped
        dataclasses.

        .. seealso::

            :ref:`orm_declarative_dc_mixins`

    .. change::
        :tags: feature, postgresql
        :tickets: 12866

        Support for ``VIRTUAL`` computed columns on PostgreSQL 18 and later has
        been added. The default behavior when :paramref:`.Computed.persisted` is
        not specified has been changed to align with PostgreSQL 18's default of
        ``VIRTUAL``. When :paramref:`.Computed.persisted` is not specified, no
        keyword is rendered on PostgreSQL 18 and later; on older versions a
        warning is emitted and ``STORED`` is used as the default. To explicitly
        request ``STORED`` behavior on all PostgreSQL versions, specify
        ``persisted=True``.

    .. change::
        :tags: feature, platform
        :tickets: 12881

        Free-threaded Python versions are now supported in wheels released on Pypi.
        This integrates with overall free-threaded support added as part of
        :ticket:`12881` for the 2.0 and 2.1 series, which includes new test suites
        as well as a few improvements to race conditions observed under
        freethreading.


    .. change::
        :tags: bug, orm
        :tickets: 12921

        The :meth:`_events.SessionEvents.do_orm_execute` event now allows direct
        mutation or replacement of the :attr:`.ORMExecuteState.parameters`
        dictionary or list, which will take effect when the the statement is
        executed.  Previously, changes to this collection were not accommodated by
        the event hook.  Pull request courtesy Shamil.


    .. change::
        :tags: bug, sql
        :tickets: 12931

        Fixed an issue in :meth:`_sql.Select.join_from` where the join condition
        between the left and right tables specified in the method call could be
        incorrectly determined based on an intermediate table already present in
        the FROM clause, rather than matching the foreign keys between the
        immediate left and right arguments. The join condition is now determined by
        matching primary keys between the two tables explicitly passed to
        :meth:`_sql.Select.join_from`, ensuring consistent and predictable join
        behavior regardless of the order of join operations or other tables present
        in the query.  The fix is applied to both the Core and ORM implementations
        of :meth:`_sql.Select.join_from`.

    .. change::
        :tags: usecase, sql
        :tickets: 12932

        Changed the query style for ORM queries emitted by :meth:`.Session.get` as
        well as many-to-one lazy load queries to use the default labeling style,
        :attr:`_sql.SelectLabelStyle.LABEL_STYLE_DISAMBIGUATE_ONLY`, which normally
        does not apply labels to columns in a SELECT statement. Previously, the
        older style :attr:`_sql.SelectLabelStyle.LABEL_STYLE_TABLENAME_PLUS_COL`
        that labels columns as `<tablename>_<columname>` was used for
        :meth:`.Session.get` to maintain compatibility with :class:`_orm.Query`.
        The change allows the string representation of ORM queries to be less
        verbose in all cases outside of legacy :class:`_orm.Query` use. Pull
        request courtesy Inada Naoki.

    .. change::
        :tags: usecase, postgresql
        :tickets: 12948

        Added support for PostgreSQL 14+ HSTORE subscripting syntax. When connected
        to PostgreSQL 14 or later, HSTORE columns now automatically use the native
        subscript notation ``hstore_col['key']`` instead of the arrow operator
        ``hstore_col -> 'key'`` for both read and write operations. This provides
        better compatibility with PostgreSQL's native HSTORE subscripting feature
        while maintaining backward compatibility with older PostgreSQL versions.

        .. warning:: Indexes in existing PostgreSQL databases which were indexed
           on an HSTORE subscript expression would need to be updated in order to
           match the new SQL syntax.

        .. seealso::

            :ref:`change_12948` - in the :ref:`migration guide
            <migration_21_toplevel>`



    .. change::
        :tags: orm, usecase
        :tickets: 12960

        Added :class:`_orm.DictBundle` as a subclass of :class:`_orm.Bundle`
        that returns ``dict`` objects.

    .. change::
        :tags: bug, sql
        :tickets: 12990

        Fixed issue where anonymous label generation for :class:`.CTE` constructs
        could produce name collisions when Python's garbage collector reused memory
        addresses during complex query compilation. The anonymous name generation
        for :class:`.CTE` and other aliased constructs like :class:`.Alias`,
        :class:`.Subquery` and others now use :func:`os.urandom` to generate unique
        identifiers instead of relying on object ``id()``, ensuring uniqueness even
        in cases of aggressive garbage collection and memory reuse.

    .. change::
        :tags: schema, usecase
        :tickets: 13006

        The the parameter :paramref:`_schema.DropConstraint.isolate_from_table`
        was deprecated since it has no effect on the drop table behavior.
        Its default values was also changed to ``False``.

    .. change::
        :tags: usecase, oracle
        :tickets: 13010

        The default DBAPI driver for the Oracle Database dialect has been changed
        to ``oracledb`` instead of ``cx_oracle``. The ``cx_oracle`` driver remains
        fully supported and can be explicitly specified in the connection URL
        using ``oracle+cx_oracle://``.

        The ``oracledb`` driver is a modernized version of ``cx_oracle`` with
        better performance characteristics and ongoing active development from
        Oracle.

        .. seealso::

            :ref:`change_13010_oracle`


    .. change::
        :tags: usecase, postgresql
        :tickets: 13010

        The default DBAPI driver for the PostgreSQL dialect has been changed to
        ``psycopg`` (psycopg version 3) instead of ``psycopg2``. The ``psycopg2``
        driver remains fully supported and can be explicitly specified in the
        connection URL using ``postgresql+psycopg2://``.

        The ``psycopg`` (version 3) driver includes improvements over ``psycopg2``
        including better performance when using C extensions and native support
        for async operations.

        .. seealso::

            :ref:`change_13010_postgresql`

    .. change::
        :tags: feature, postgresql, sql
        :tickets: 13014

        Added support for monotonic server-side functions such as PostgreSQL 18's
        ``uuidv7()`` to work with the :ref:`engine_insertmanyvalues` feature.
        By passing ``monotonic=True`` to any :class:`.Function`, the function can
        be used as a sentinel for tracking row order in batched INSERT operations
        with RETURNING, allowing the ORM and Core to efficiently batch INSERT
        statements while maintaining deterministic row ordering.

        .. seealso::

            :ref:`change_13014_postgresql`

            :ref:`engine_insertmanyvalues_monotonic_functions`

            :ref:`postgresql_monotonic_functions`

    .. change::
        :tags: bug, engine
        :tickets: 13018

        Fixed issue in the :meth:`.ConnectionEvents.after_cursor_execute` method
        where the SQL statement and parameter list for an "insertmanyvalues"
        operation sent to the event would not be the actual SQL / parameters just
        emitted on the cursor, instead being the non-batched form of the statement
        that's used as a template to generate the batched statements.

    .. change::
        :tags: bug, orm
        :tickets: 13021

        A change in the mechanics of how Python dataclasses are applied to classes
        that use :class:`.MappedAsDataclass` or
        :meth:`.registry.mapped_as_dataclass` to apply ``__annotations__`` that are
        as identical as is possible to the original ``__annotations__`` given,
        while also adding attributes that SQLAlchemy considers to be part of
        dataclass ``__annotations__``, then restoring the previous annotations in
        exactly the same format as they were, using patterns that work with
        :pep:`649` as closely as possible.

    .. change::
        :tags: bug, orm
        :tickets: 13060

        Removed the ``ORDER BY`` clause from queries generated by
        :func:`_orm.selectin_polymorphic` and the
        :paramref:`_orm.Mapper.polymorphic_load` parameter set to ``"selectin"``.
        The ``ORDER BY`` clause appears to have been an unnecessary implementation
        artifact.

    .. change::
        :tags: bug, orm
        :tickets: 13070

        A significant change to the ORM mechanics involved with both
        :func:`.orm.with_loader_criteria` as well as single table inheritance, to
        more aggressively locate WHERE criteria which should be augmented by either
        the custom criteria or single-table inheritance criteria; SELECT statements
        that do not include the entity within the columns clause or as an explicit
        FROM, but still reference the entity within the WHERE clause, are now
        covered, in particular this will allow subqueries using ``EXISTS (SELECT
        1)`` such as those rendered by :meth:`.RelationshipProperty.Comparator.any`
        and :meth:`.RelationshipProperty.Comparator.has`.

    .. change::
        :tags: bug, mariadb
        :tickets: 13076

        Fixes to the MySQL/MariaDB dialect so that mariadb-specific features such
        as the :class:`.mariadb.INET4` and :class:`.mariadb.INET6` datatype may be
        used with an :class:`.Engine` that uses a ``mysql://`` URL, if the backend
        database is actually a mariadb database.   Previously, support for MariaDB
        features when ``mysql://`` URLs were used instead of ``mariadb://`` URLs
        was ad-hoc; with this issue resolution, the full set of schema / compiler /
        type features are now available regardless of how the URL was presented.

    .. change::
        :tags: feature, schema
        :tickets: 181

        Added support for the SQL ``CREATE VIEW`` statement via the new
        :class:`.CreateView` DDL class. The new class allows creating database
        views from SELECT statements, with support for options such as
        ``TEMPORARY``, ``IF NOT EXISTS``, and ``MATERIALIZED`` where supported by
        the target database. Views defined with :class:`.CreateView` integrate with
        :class:`.MetaData` for automated DDL generation and provide a
        :class:`.Table` object for querying.

        .. seealso::

            :ref:`change_4950`



    .. change::
        :tags: feature, schema
        :tickets: 4950

        Added support for the SQL ``CREATE TABLE ... AS SELECT`` construct via the
        new :class:`_schema.CreateTableAs` DDL construct and the
        :meth:`_sql.Select.into` method. The new construct allows creating a
        table directly from the results of a SELECT statement, with support for
        options such as ``TEMPORARY`` and ``IF NOT EXISTS`` where supported by the
        target database. Tables defined with :class:`_schema.CreateTableAs`
        integrate with :class:`.MetaData` for automated DDL generation and provide
        a :class:`.Table` object for querying. Pull request courtesy Greg Jarzab.

        .. seealso::

            :ref:`change_4950`



    .. change::
        :tags: change, sql
        :tickets: 5252

        the :class:`.Numeric` and :class:`.Float` SQL types have been separated out
        so that :class:`.Float` no longer inherits from :class:`.Numeric`; instead,
        they both extend from a common mixin :class:`.NumericCommon`.  This
        corrects for some architectural shortcomings where numeric and float types
        are typically separate, and establishes more consistency with
        :class:`.Integer` also being a distinct type.   The change should not have
        any end-user implications except for code that may be using
        ``isinstance()`` to test for the :class:`.Numeric` datatype; third party
        dialects which rely upon specific implementation types for numeric and/or
        float may also require adjustment to maintain compatibility.

    .. change::
        :tags: change, sql
        :tickets: 7066, 12915

        Added new implementation for the :meth:`.Select.params` method and that of
        similar statements, via a new statement-only
        :meth:`.ExecutableStatement.params` method which works more efficiently and
        correctly than the previous implementations available from
        :class:`.ClauseElement`, by associating the given parameter dictionary with
        the statement overall rather than cloning the statement and rewriting its
        bound parameters.  The :meth:`_sql.ClauseElement.params` and
        :meth:`_sql.ClauseElement.unique_params` methods, when called on an object
        that does not implement :class:`.ExecutableStatement`, will continue to
        work the old way of cloning the object, and will emit a deprecation
        warning.    This issue both resolves the architectural / performance
        concerns of :ticket:`7066` and also provides correct ORM compatibility for
        functions like :func:`_orm.aliased`, reported by :ticket:`12915`.

        .. seealso::

            :ref:`change_7066`

    .. change::
        :tags: usecase, sql
        :tickets: 7910

        Added method :meth:`.TableClause.insert_column` to complement
        :meth:`.TableClause.append_column`, which inserts the given column at a
        specific index.   This can be helpful for prepending primary key columns to
        tables, etc.


    .. change::
        :tags: feature, asyncio
        :tickets: 8047

        The "emulated" exception hierarchies for the asyncio
        drivers such as asyncpg, aiomysql, aioodbc, etc. have been standardized
        on a common base :class:`.EmulatedDBAPIException`, which is now what's
        available from the :attr:`.StatementException.orig` attribute on a
        SQLAlchemy :class:`.DBAPIError` object.   Within :class:`.EmulatedDBAPIException`
        and the subclasses in its hierarchy, the original driver-level exception is
        also now available via the :attr:`.EmulatedDBAPIException.orig` attribute,
        and is also available from :class:`.DBAPIError` directly using the
        :attr:`.DBAPIError.driver_exception` attribute.



    .. change::
        :tags: feature, postgresql
        :tickets: 8047

        Added additional emulated error classes for the subclasses of
        ``asyncpg.exception.IntegrityError`` including ``RestrictViolationError``,
        ``NotNullViolationError``, ``ForeignKeyViolationError``,
        ``UniqueViolationError`` ``CheckViolationError``,
        ``ExclusionViolationError``.  These exceptions are not directly thrown by
        SQLAlchemy's asyncio emulation, however are available from the
        newly added :attr:`.DBAPIError.driver_exception` attribute when a
        :class:`.IntegrityError` is caught.

    .. change::
        :tags: usecase, sql
        :tickets: 8579

        Added support for the pow operator (``**``), with a default SQL
        implementation of the ``POW()`` function.   On Oracle Database, PostgreSQL
        and MSSQL it renders as ``POWER()``.   As part of this change, the operator
        routes through a new first class ``func`` member :class:`_functions.pow`,
        which renders on Oracle Database, PostgreSQL and MSSQL as ``POWER()``.

    .. change::
        :tags: usecase, sql, orm
        :tickets: 8601

        The :meth:`_sql.Select.filter_by`, :meth:`.Update.filter_by` and
        :meth:`.Delete.filter_by` methods now search across all entities
        present in the statement, rather than limiting their search to only the
        last joined entity or the first FROM entity. This allows these methods
        to locate attributes unambiguously across multiple joined tables,
        resolving issues where changing the order of operations such as
        :meth:`_sql.Select.with_only_columns` would cause the method to fail.

        If an attribute name exists in more than one FROM clause entity, an
        :class:`_exc.AmbiguousColumnError` is now raised, indicating that
        :meth:`_sql.Select.filter` (or :meth:`_sql.Select.where`) should be used
        instead with explicit table-qualified column references.

        .. seealso::

            :ref:`change_8601` - Migration notes

    .. change::
        :tags: change, engine
        :tickets: 9647

        An empty sequence passed to any ``execute()`` method now
        raised a deprecation warning, since such an executemany
        is invalid.
        Pull request courtesy of Carlos Sousa.

    .. change::
        :tags: feature, orm
        :tickets: 9809

        Session autoflush behavior has been simplified to unconditionally flush the
        session each time an execution takes place, regardless of whether an ORM
        statement or Core statement is being executed. This change eliminates the
        previous conditional logic that only flushed when ORM-related statements
        were detected, which had become difficult to define clearly with the unified
        v2 syntax that allows both Core and ORM execution patterns. The change
        provides more consistent and predictable session behavior across all types
        of SQL execution.

        .. seealso::

            :ref:`change_9809`

    .. change::
        :tags: feature, orm
        :tickets: 9832

        Added :class:`_orm.RegistryEvents` event class that allows event listeners
        to be established on a :class:`_orm.registry` object. The new class
        provides three events: :meth:`_orm.RegistryEvents.resolve_type_annotation`
        which allows customization of type annotation resolution that can
        supplement or replace the use of the
        :paramref:`.registry.type_annotation_map` dictionary, including that it can
        be helpful with custom resolution for complex types such as those of
        :pep:`695`, as well as :meth:`_orm.RegistryEvents.before_configured` and
        :meth:`_orm.RegistryEvents.after_configured`, which are registry-local
        forms of the mapper-wide version of these hooks.

        .. seealso::

            :ref:`change_9832`

    .. change::
        :tags: change, asyncio

        Removed the compatibility ``async_fallback`` mode for async dialects,
        since it's no longer used by SQLAlchemy tests.
        Also removed the internal function ``await_fallback()`` and renamed
        the internal function ``await_only()`` to ``await_()``.
        No change is expected to user code.

    .. change::
        :tags: feature, mysql

        Added new construct :func:`_mysql.limit` which can be applied to any
        :func:`_sql.update` or :func:`_sql.delete` to provide the LIMIT keyword to
        UPDATE and DELETE.  This new construct supersedes the use of the
        "mysql_limit" dialect keyword argument.


    .. change::
        :tags: change, tests

        The top-level test runner has been changed to use ``nox``, adding a
        ``noxfile.py`` as well as some included modules.   The ``tox.ini`` file
        remains in place so that ``tox`` runs will continue to function in the near
        term, however it will be eventually removed and improvements and
        maintenance going forward will be only towards ``noxfile.py``.



    .. change::
        :tags: change, platform

        Updated the setup manifest definition to use PEP 621-compliant
        pyproject.toml. Also updated the extra install dependency to comply with
        PEP-685. Thanks for the help of Matt Oberle and KOLANICH on this change.

    .. change::
        :tags: change, platform
        :tickets: 10357, 12029, 12819

        Python 3.10 or above is now required; support for Python 3.9, 3.8 and 3.7
        is dropped as these versions are EOL.

    .. change::
        :tags: engine, change

        The private method ``Connection._execute_compiled`` is removed.  This method may
        have been used for some special purposes however the :class:`.SQLCompiler`
        object has lots of special state that should be set up for an execute call,
        which we don't support.
