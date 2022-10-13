=============
2.0 Changelog
=============

.. changelog_imports::

    .. include:: changelog_14.rst
        :start-line: 5


.. changelog::
    :version: 2.0.0b1
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
        :tags: bug, pool
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
        :func:`_orm.column_mapped_collection`, used when populating the dictionary,
        to assert that the data value on the object to be used as the dictionary
        key is actually present, and is not instead using "None" due to the
        attribute never being actually assigned. This is used to prevent a
        mis-population of None for a key when assigning via a backref where the
        "key" attribute on the object is not yet assigned.

        As the failure mode here is a transitory condition that is not typically
        persisted to the database, and is easy to produce via the constructor of
        the class based on the order in which parameters are assigned, it is very
        possible that many applications include this behavior already which is
        silently passed over. To accommodate for applications where this error is
        now raised, a new parameter
        :paramref:`_orm.attribute_mapped_collection.ignore_unpopulated_attribute`
        is also added to both :func:`_orm.attribute_mapped_collection` and
        :func:`_orm.column_mapped_collection` that instead causes the erroneous
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
        :tags: feature, types
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
        :tags: feature, types
        :tickets: 5465

        Added :class:`.Double`, :class:`.DOUBLE`, :class:`.DOUBLE_PRECISION`
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
        :tags: usecase, datatypes
        :tickets: 5052

        Added modified ISO-8601 rendering (i.e. ISO-8601 with the T converted to a
        space) when using ``literal_binds`` with the SQL compilers provided by the
        PostgreSQL, MySQL, MariaDB, MSSQL, Oracle dialects. For Oracle, the ISO
        format is wrapped inside of an appropriate TO_DATE() function call.
        Previously this rendering was not implemented for dialect-specific
        compilation.

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
        :tags: bug, types
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
