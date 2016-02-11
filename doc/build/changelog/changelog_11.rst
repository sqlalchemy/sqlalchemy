

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
        :tags: feature, sql
        :tickets: 2551

        CTE functionality has been expanded to support all DML, allowing
        INSERT, UPDATE, and DELETE statements to both specify their own
        WITH clause, as well as for these statements themselves to be
        CTE expressions when they include a RETURNING clause.

        .. seealso::

            :ref:`change_2551`

    .. change::
        :tags: bug, orm
        :tickets: 3641

        A refinement to the logic which adds columns to the resulting SQL when
        :meth:`.Query.distinct` is combined with :meth:`.Query.order_by` such
        that columns which are already present will not be added
        a second time, even if they are labeled with a different name.
        Regardless of this change, the extra columns added to the SQL have
        never been returned in the final result, so this change only impacts
        the string form of the statement as well as its behavior when used in
        a Core execution context.   Additionally, columns are no longer added
        when the DISTINCT ON format is used, provided the query is not
        wrapped inside a subquery due to joined eager loading.

        .. seealso::

            :ref:`change_3641`

    .. change::
        :tags: feature, sql
        :tickets: 3292, 3095

        Added support for PEP-435-style enumerated classes, namely
        Python 3's ``enum.Enum`` class but also including compatible
        enumeration libraries, to the :class:`.types.Enum` datatype.
        The :class:`.types.Enum` datatype now also performs in-Python validation
        of incoming values, and adds an option to forego creating the
        CHECK constraint :paramref:`.Enum.create_constraint`.
        Pull request courtesy Alex Grönholm.

        .. seealso::

            :ref:`change_3292`

            :ref:`change_3095`

    .. change::
        :tags: change, postgresql

        The ``sqlalchemy.dialects.postgres`` module, long deprecated, is
        removed; this has emitted a warning for many years and projects
        should be calling upon ``sqlalchemy.dialects.postgresql``.
        Engine URLs of the form ``postgres://`` will still continue to function,
        however.

    .. change::
        :tags: bug, sqlite
        :tickets: 3634

        The workaround for right-nested joins on SQLite, where they are rewritten
        as subqueries in order to work around SQLite's lack of support for this
        syntax, is lifted when SQLite version 3.7.16 or greater is detected.

        .. seealso::

            :ref:`change_3634`

    .. change::
        :tags: bug, sqlite
        :tickets: 3633

        The workaround for SQLite's unexpected delivery of column names as
        ``tablename.columnname`` for some kinds of queries is now disabled
        when SQLite version 3.10.0 or greater is detected.

        .. seealso::

            :ref:`change_3633`

    .. change::
        :tags: feature, orm
        :tickets: 2349

        Added new parameter :paramref:`.orm.mapper.passive_deletes` to
        available mapper options.   This allows a DELETE to proceed
        for a joined-table inheritance mapping against the base table only,
        while allowing for ON DELETE CASCADE to handle deleting the row
        from the subclass tables.

        .. seealso::

            :ref:`change_2349`


    .. change::
        :tags: bug, sybase
        :tickets: 2278

        The unsupported Sybase dialect now raises ``NotImplementedError``
        when attempting to compile a query that includes "offset"; Sybase
        has no straightforward "offset" feature.

    .. change::
        :tags: feature, orm
        :tickets: 3631

        Calling str() on a core SQL construct has been made more "friendly",
        when the construct contains non-standard SQL elements such as
        RETURNING, array index operations, or dialect-specific or custom
        datatypes.  A string is now returned in these cases rendering an
        approximation of the construct (typically the Postgresql-style
        version of it) rather than raising an error.

        .. seealso::

            :ref:`change_3631`

    .. change::
        :tags: bug, orm
        :tickets: 3630

        Fixed issue where two same-named relationships that refer to
        a base class and a concrete-inherited subclass would raise an error
        if those relationships were set up using "backref", while setting up the
        identical configuration using relationship() instead with the conflicting
        names would succeed, as is allowed in the case of a concrete mapping.

        .. seealso::

            :ref:`change_3630`

    .. change::
        :tags: feature, orm
        :tickets: 3081

        The ``str()`` call for :class:`.Query` will now take into account
        the :class:`.Engine` to which the :class:`.Session` is bound, when
        generating the string form of the SQL, so that the actual SQL
        that would be emitted to the database is shown, if possible.  Previously,
        only the engine associated with the :class:`.MetaData` to which the
        mappings are associated would be used, if present.  If
        no bind can be located either on the :class:`.Session` or on
        the :class:`.MetaData` to which the mappings are associated, then
        the "default" dialect is used to render the SQL, as was the case
        previously.

        .. seealso::

            :ref:`change_3081`

    .. change::
        :tags: feature, sql
        :tickets: 3501

        A deep improvement to the recently added :meth:`.TextClause.columns`
        method, and its interaction with result-row processing, now allows
        the columns passed to the method to be positionally matched with the
        result columns in the statement, rather than matching on name alone.
        The advantage to this includes that when linking a textual SQL statement
        to an ORM or Core table model, no system of labeling or de-duping of
        common column names needs to occur, which also means there's no need
        to worry about how label names match to ORM columns and so-forth.  In
        addition, the :class:`.ResultProxy` has been further enhanced to
        map column and string keys to a row with greater precision in some
        cases.

        .. seealso::

            :ref:`change_3501` - feature overview

            :ref:`behavior_change_3501` - backwards compatibility remarks

    .. change::
        :tags: feature, engine
        :tickets: 2685

        Multi-tenancy schema translation for :class:`.Table` objects is added.
        This supports the use case of an application that uses the same set of
        :class:`.Table` objects in many schemas, such as schema-per-user.
        A new execution option
        :paramref:`.Connection.execution_options.schema_translate_map` is
        added.

        .. seealso::

            :ref:`change_2685`

    .. change::
        :tags: feature, engine
        :tickets: 3536

        Added a new entrypoint system to the engine to allow "plugins" to
        be stated in the query string for a URL.   Custom plugins can
        be written which will be given the chance up front to alter and/or
        consume the engine's URL and keyword arguments, and then at engine
        create time will be given the engine itself to allow additional
        modifications or event registration.  Plugins are written as a
        subclass of :class:`.CreateEnginePlugin`; see that class for
        details.

    .. change::
        :tags: feature, mysql
        :tickets: 3547

        Added :class:`.mysql.JSON` for MySQL 5.7.  The JSON type provides
        persistence of JSON values in MySQL as well as basic operator support
        of "getitem" and "getpath", making use of the ``JSON_EXTRACT``
        function in order to refer to individual paths in a JSON structure.

        .. seealso::

            :ref:`change_3547`

    .. change::
        :tags: feature, sql
        :tickets: 3619

        Added a new type to core :class:`.types.JSON`.  This is the
        base of the PostgreSQL :class:`.postgresql.JSON` type as well as that
        of the new :class:`.mysql.JSON` type, so that a PG/MySQL-agnostic
        JSON column may be used.  The type features basic index and path
        searching support.

        .. seealso::

            :ref:`change_3619`

    .. change::
        :tags: bug, sql
        :tickets: 3616

        Fixed an assertion that would raise somewhat inappropriately
        if a :class:`.Index` were associated with a :class:`.Column` that
        is associated with a lower-case-t :class:`.TableClause`; the
        association should be ignored for the purposes of associating
        the index with a :class:`.Table`.

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
        which automatically returns an :class:`.postgresql.ARRAY` of the correct type
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

        Added a new type to core :class:`.types.ARRAY`.  This is the
        base of the PostgreSQL :class:`.postgresql.ARRAY` type, and is now part of Core
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
        in conjunction with the Postgresql :class:`.postgresql.JSON` type.  When
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
