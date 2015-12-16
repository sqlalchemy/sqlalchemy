

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
        :tags: bug, orm
        :tickets: 3601

        The :meth:`.Session.merge` method now tracks pending objects by
        primary key before emitting an INSERT, and merges distinct objects with
        duplicate primary keys together as they are encountered, which is
        essentially semi-deterministic at best.   This behavior
        matches what happens already with persistent objects.

        .. seealso::

            :ref:`change_3601`

    .. change::
        :tags: bug, postgresql
        :tickets: 3587

        Added support for reflecting the source of materialized views
        to the Postgresql version of the :meth:`.Inspector.get_view_definition`
        method.

    .. change::
        :tags: bug, orm
        :tickets: 3582

        Fixed bug where the "single table inheritance" criteria would be
        added onto the end of a query in some inappropriate situations, such
        as when querying from an exists() of a single-inheritance subclass.

        .. seealso::

            :ref:`change_3582`

    .. change::
        :tags: enhancement, schema
        :pullreq: github:204

        The default generation functions passed to :class:`.Column` objects
        are now run through "update_wrapper", or an equivalent function
        if a callable non-function is passed, so that introspection tools
        preserve the name and docstring of the wrapped function.  Pull
        request courtesy hsum.

    .. change::
        :tags: change, sql, mysql
        :tickets: 3216

        The system by which a :class:`.Column` considers itself to be an
        "auto increment" column has been changed, such that autoincrement
        is no longer implicitly enabled for a :class:`.Table` that has a
        composite primary key.  In order to accommodate being able to enable
        autoincrement for a composite PK member column while at the same time
        maintaining SQLAlchemy's long standing behavior of enabling
        implicit autoincrement for a single integer primary key, a third
        state has been added to the :paramref:`.Column.autoincrement` parameter
        ``"auto"``, which is now the default.

        .. seealso::

            :ref:`change_3216`

            :ref:`change_mysql_3216`

    .. change::
        :tags: change, mysql
        :tickets: 3216

        The MySQL dialect no longer generates an extra "KEY" directive when
        generating CREATE TABLE DDL for a table using InnoDB with a
        composite primary key with AUTO_INCREMENT on a column that isn't the
        first column;  to overcome InnoDB's limitation here, the PRIMARY KEY
        constraint is now generated with the AUTO_INCREMENT column placed
        first in the list of columns.

        .. seealso::

            :ref:`change_mysql_3216`

            :ref:`change_3216`

    .. change::
        :tags: change, sqlite
        :pullreq: github:198

        Added support to the SQLite dialect for the
        :meth:`.Inspector.get_schema_names` method to work with SQLite;
        pull request courtesy Brian Van Klaveren.  Also repaired support
        for creation of indexes with schemas as well as reflection of
        foreign key constraints in schema-bound tables.

        .. seealso::

            :ref:`change_sqlite_schemas`

    .. change::
        :tags: change, mssql
        :tickets: 3434

        The ``legacy_schema_aliasing`` flag, introduced in version 1.0.5
        as part of :ticket:`3424` to allow disabling of the MSSQL dialect's
        attempts to create aliases for schema-qualified tables, now defaults
        to False; the old behavior is now disabled unless explicitly turned on.

        .. seealso::

            :ref:`change_3434`

    .. change::
        :tags: bug, orm
        :tickets: 3250

        Added a new type-level modifier :meth:`.TypeEngine.evaluates_none`
        which indicates to the ORM that a positive set of None should be
        persisted as the value NULL, instead of omitting the column from
        the INSERT statement.  This feature is used both as part of the
        implementation for :ticket:`3514` as well as a standalone feature
        available on any type.

        .. seealso::

            :ref:`change_3250`

    .. change::
        :tags: bug, postgresql
        :tickets: 2729

        The use of a :class:`.postgresql.ARRAY` object that refers
        to a :class:`.types.Enum` or :class:`.postgresql.ENUM` subtype
        will now emit the expected "CREATE TYPE" and "DROP TYPE" DDL when
        the type is used within a "CREATE TABLE" or "DROP TABLE".

        .. seealso::

            :ref:`change_2729`

    .. change::
        :tags: bug, sql
        :tickets: 3531

        The :func:`.type_coerce` construct is now a fully fledged Core
        expression element which is late-evaluated at compile time.  Previously,
        the function was only a conversion function which would handle different
        expression inputs by returning either a :class:`.Label` of a column-oriented
        expression or a copy of a given :class:`.BindParameter` object,
        which in particular prevented the operation from being logically
        maintained when an ORM-level expression transformation would convert
        a column to a bound parameter (e.g. for lazy loading).

        .. seealso::

            :ref:`change_3531`

    .. change::
        :tags: bug, orm
        :tickets: 3526

        Internal calls to "bookkeeping" functions within
        :meth:`.Session.bulk_save_objects` and related bulk methods have
        been scaled back to the extent that this functionality is not
        currently used, e.g. checks for column default values to be
        fetched after an INSERT or UPDATE statement.

    .. change::
        :tags: feature, orm
        :tickets: 2677

        The :class:`.SessionEvents` suite now includes events to allow
        unambiguous tracking of all object lifecycle state transitions
        in terms of the :class:`.Session` itself, e.g. pending,
        transient,  persistent, detached.   The state of the object
        within each event is also defined.

        .. seealso::

            :ref:`change_2677`

    .. change::
        :tags: feature, orm
        :tickets: 2677

        Added a new session lifecycle state :term:`deleted`.  This new state
        represents an object that has been deleted from the :term:`persistent`
        state and will move to the :term:`detached` state once the transaction
        is committed.  This resolves the long-standing issue that objects
        which were deleted existed in a gray area between persistent and
        detached.   The :attr:`.InstanceState.persistent` accessor will
        **no longer** report on a deleted object as persistent; the
        :attr:`.InstanceState.deleted` accessor will instead be True for
        these objects, until they become detached.

        .. seealso::

            :ref:`change_2677`

    .. change::
        :tags: change, orm
        :tickets: 2677

        The :paramref:`.Session.weak_identity_map` parameter is deprecated.
        See the new recipe at :ref:`session_referencing_behavior` for
        an event-based approach to maintaining strong identity map behavior.

        .. seealso::

            :ref:`change_2677`

    .. change::
        :tags: bug, sql
        :tickets: 2919

        The :class:`.TypeDecorator` type extender will now work in conjunction
        with a :class:`.SchemaType` implementation, typically :class:`.Enum`
        or :class:`.Boolean` with regards to ensuring that the per-table
        events are propagated from the implementation type to the outer type.
        These events are used
        to ensure that the constraints or Postgresql types (e.g. ENUM)
        are correctly created (and possibly dropped) along with the parent
        table.

        .. seealso::

            :ref:`change_2919`

    .. change::
        :tags: feature, sql
        :tickets: 1370

        Added support for "set-aggregate" functions of the form
        ``<function> WITHIN GROUP (ORDER BY <criteria>)``, using the
        method :meth:`.FunctionElement.within_group`.  A series of common
        set-aggregate functions with return types derived from the set have
        been added. This includes functions like :class:`.percentile_cont`,
        :class:`.dense_rank` and others.

        .. seealso::

            :ref:`change_3132`

    .. change::
        :tags: feature, sql, postgresql
        :tickets: 3132

        Added support for the SQL-standard function :class:`.array_agg`,
        which automatically returns an :class:`.Array` of the correct type
        and supports index / slice operations, as well as
        :func:`.postgresql.array_agg`, which returns a :class:`.postgresql.ARRAY`
        with additional comparison features.   As arrays are only
        supported on Postgresql at the moment, only actually works on
        Postgresql.  Also added a new construct
        :class:`.postgresql.aggregate_order_by` in support of PG's
        "ORDER BY" extension.

        .. seealso::

            :ref:`change_3132`

    .. change::
        :tags: feature, sql
        :tickets: 3516

        Added a new type to core :class:`.types.Array`.  This is the
        base of the PostgreSQL :class:`.ARRAY` type, and is now part of Core
        to begin supporting various SQL-standard array-supporting features
        including some functions and eventual support for native arrays
        on other databases that have an "array" concept, such as DB2 or Oracle.
        Additionally, new operators :func:`.expression.any_` and
        :func:`.expression.all_` have been added.  These support not just
        array constructs on Postgresql, but also subqueries that are usable
        on MySQL (but sadly not on Postgresql).

        .. seealso::

            :ref:`change_3516`

    .. change::
        :tags: feature, orm
        :tickets: 3321

        Added new checks for the common error case of passing mapped classes
        or mapped instances into contexts where they are interpreted as
        SQL bound parameters; a new exception is raised for this.

        .. seealso::

            :ref:`change_3321`

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

            :ref:`change_3250`

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
