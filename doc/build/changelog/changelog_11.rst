

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
    :version: 1.1.0b2
    :released: July 1, 2016

    .. change::
        :tags: bug, ext, postgresql
        :tickets: 3732

        Made a slight behavioral change in the ``sqlalchemy.ext.compiler``
        extension, whereby the existing compilation schemes for an established
        construct would be removed if that construct was itself didn't already
        have its own dedicated ``__visit_name__``.  This was a
        rare occurrence in 1.0, however in 1.1 :class:`.postgresql.ARRAY`
        subclasses :class:`.sqltypes.ARRAY` and has this behavior.
        As a result, setting up a compilation handler for another dialect
        such as SQLite would render the main :class:`.postgresql.ARRAY`
        object no longer compilable.

    .. change::
        :tags: bug, sql
        :tickets: 3730

        The processing performed by the :class:`.Boolean` datatype for backends
        that only feature integer types has been made consistent between the
        pure Python and C-extension versions, in that the C-extension version
        will accept any integer value from the database as a boolean, not just
        zero and one; additionally, non-boolean integer values being sent to
        the database are coerced to exactly zero or one, instead of being
        passed as the original integer value.

        .. seealso::

            :ref:`change_3730`

    .. change::
        :tags: bug, sql
        :tickets: 3725

        Rolled back the validation rules a bit in :class:`.Enum` to allow
        unknown string values to pass through, unless the flag
        ``validate_string=True`` is passed to the Enum; any other kind of object is
        still of course rejected.  While the immediate use
        is to allow comparisons to enums with LIKE, the fact that this
        use exists indicates there may be more unknown-string-comparsion use
        cases than we expected, which hints that perhaps there are some
        unknown string-INSERT cases too.

    .. change::
        :tags: bug, mysql
        :tickets: 3726

        Dialed back the "order the primary key columns per auto-increment"
        described in :ref:`change_mysql_3216` a bit, so that if the
        :class:`.PrimaryKeyConstraint` is explicitly defined, the order
        of columns is maintained exactly, allowing control of this behavior
        when necessary.

.. changelog::
    :version: 1.1.0b1
    :released: June 16, 2016

    .. change::
        :tags: feature, sql
        :tickets: 3718

        Added TABLESAMPLE support via the new :meth:`.FromClause.tablesample`
        method and standalone function.  Pull request courtesy Ilja Everilä.

        .. seealso::

            :ref:`change_3718`

    .. change::
        :tags: feature, orm, ext

        A new ORM extension :ref:`indexable_toplevel` is added, which allows
        construction of Python attributes which refer to specific elements
        of "indexed" structures such as arrays and JSON fields.  Pull request
        courtesy Jeong YunWon.

        .. seealso::

            :ref:`feature_indexable`

    .. change::
        :tags: bug, sql
        :tickets: 3724

        :meth:`.FromClause.count` is deprecated.  This function makes use of
        an arbitrary column in the table and is not reliable; for Core use,
        ``func.count()`` should be preferred.

    .. change::
        :tags: feature, postgresql
        :tickets: 3529

        Added support for Postgresql's INSERT..ON CONFLICT using a new
        Postgresql-specific :class:`.postgresql.dml.Insert` object.
        Pull request and extensive efforts here by Robin Thomas.

        .. seealso::

            :ref:`change_3529`

    .. change::
        :tags: feature, postgresql
        :pullreq: bitbucket:84

        The DDL for DROP INDEX will emit "CONCURRENTLY" if the
        ``postgresql_concurrently`` flag is set upon the
        :class:`.Index` and if the database in use is detected as
        Postgresql version 9.2 or greater.   For CREATE INDEX, database
        version detection is also added which will omit the clause if
        PG version is less than 8.2.  Pull request courtesy Iuri de Silvio.

    .. change::
        :tags: bug, orm
        :tickets: 3708

        Fixed an issue where a many-to-one change of an object from one
        parent to another could work inconsistently when combined with
        an un-flushed modication of the foreign key attribute.  The attribute
        move now considers the database-committed value of the foreign key
        in order to locate the "previous" parent of the object being
        moved.   This allows events to fire off correctly including
        backref events.  Previously, these events would not always fire.
        Applications which may have relied on the previously broken
        behavior may be affected.

        .. seealso::

            :ref:`change_3708`

    .. change::
        :tags: feature, sql
        :tickets: 3049

        Added support for ranges in window functions, using the
        :paramref:`.expression.over.range_` and
        :paramref:`.expression.over.rows` parameters.

        .. seealso::

            :ref:`change_3049`

    .. change::
        :tags: feature, orm

        Added new flag :paramref:`.Session.bulk_insert_mappings.render_nulls`
        which allows an ORM bulk INSERT to occur with NULL values rendered;
        this bypasses server side defaults, however allows all statements
        to be formed with the same set of columns, allowing them to be
        batched.  Pull request courtesy Tobias Sauerwein.

    .. change::
        :tags: feature, postgresql
        :tickets: 3588

        Added new parameter :paramref:`.PGInspector.get_view_names.include`,
        allowing specification for what kinds of views should be returned.
        Currently "plain" and "materialized" views are included.  Pull
        request courtesy Sebastian Bank.

    .. change::
        :tags: feature, mssql

        The ``mssql_clustered`` flag available on :class:`.UniqueConstraint`,
        :class:`.PrimaryKeyConstraint`, :class:`.Index` now defaults to
        ``None``, and can be set to False which will render the NONCLUSTERED
        keyword in particular for a primary key, allowing a different index to
        be used as "clustered". Pull request courtesy Saulius Žemaitaitis.

    .. change::
        :tags: feature, orm
        :tickets: 1311

        Added new event :meth:`.AttributeEvents.init_scalar`, as well
        as a new example suite illustrating its use.  This event can be used
        to provide a Core-generated default value to a Python-side attribute
        before the object is persisted.

        .. seealso::

            :ref:`change_1311`

    .. change::
        :tags: feature, postgresql
        :tickets: 3720

        Added ``postgresql_tablespace`` as an argument to :class:`.Index`
        to allow specification of TABLESPACE for an index in Postgresql.
        Complements the same-named parameter on :class:`.Table`.  Pull
        request courtesy Benjamin Bertrand.

    .. change::
        :tags: orm, feature
        :pullreq: github:237

        Added :paramref:`.AutomapBase.prepare.schema` to the
        :meth:`.AutomapBase.prepare` method, to indicate which schema
        tables should be reflected from if not the default schema.
        Pull request courtesy Josh Marlow.

    .. change::
        :tags: feature, sqlite
        :pullreq: github:244

        The SQLite dialect now reflects ON UPDATE and ON DELETE phrases
        within foreign key constraints.  Pull request courtesy
        Michal Petrucha.

    .. change::
        :tags: bug, mssql
        :pullreq: bitbucket:58

        Adjustments to the mxODBC dialect to make use of the ``BinaryNull``
        symbol when appropriate in conjunction with the ``VARBINARY``
        data type.  Pull request courtesy Sheila Allen.

    .. change::
        :tags: feature, sql
        :pullreq: bitbucket:80

        Implemented reflection of CHECK constraints for SQLite and Postgresql.
        This is available via the new inspector method
        :meth:`.Inspector.get_check_constraints` as well as when reflecting
        :class:`.Table` objects in the form of :class:`.CheckConstraint`
        objects present in the constraints collection.  Pull request courtesy
        Alex Grönholm.

    .. change::
        :tags: feature, postgresql
        :pullreq: github:297

        Added new parameter
        :paramref:`.GenerativeSelect.with_for_update.key_share`, which
        will render the ``FOR NO KEY UPDATE`` version of ``FOR UPDATE``
        and ``FOR KEY SHARE`` instead of ``FOR SHARE``
        on the Postgresql backend.  Pull request courtesy Sergey Skopin.

    .. change::
        :tags: feature, postgresql, oracle
        :pullreq: bitbucket:86

        Added new parameter
        :paramref:`.GenerativeSelect.with_for_update.skip_locked`, which
        will render the ``SKIP LOCKED`` phrase for a ``FOR UPDATE`` or
        ``FOR SHARE`` lock on the Postgresql and Oracle backends.  Pull
        request courtesy Jack Zhou.

    .. change::
        :tags: change, orm
        :tickets: 3394

        The :paramref:`.Mapper.order_by` parameter is deprecated.
        This is an old parameter no longer relevant to how SQLAlchemy
        works, once the Query object was introduced.  By deprecating it
        we establish that we aren't supporting non-working use cases
        and that we encourage applications to move off of the use of this
        parameter.

        .. seealso::

            :ref:`change_3394`

    .. change::
        :tags: feature, postgresql

        Added a new dialect for the PyGreSQL Postgresql dialect.  Thanks
        to Christoph Zwerschke and Kaolin Imago Fire for their efforts.

    .. change::
        :tags: bug, ext
        :tickets: 3653

        The docstring specified on a hybrid property or method is now honored
        at the class level, allowing it to work with tools like Sphinx
        autodoc.  The mechanics here necessarily involve some wrapping of
        expressions to occur for hybrid properties, which may cause them
        to appear differently using introspection.

        .. seealso::

            :ref:`change_3653`

    .. change::
        :tags: feature, sql

        New :meth:`.ColumnOperators.is_distinct_from` and
        :meth:`.ColumnOperators.isnot_distinct_from` operators; pull request
        courtesy Sebastian Bank.

        .. seealso::

            :ref:`change_is_distinct_from`

    .. change::
        :tags: bug, orm
        :tickets: 3488

        Fixed bug where deferred columns would inadvertently be set up
        for database load on the next object-wide unexpire, when the object
        were merged into the session with ``session.merge(obj, load=False)``.

    .. change::
        :tags: feature, sql
        :pullreq: github:275

        Added a hook in :meth:`.DDLCompiler.visit_create_table` called
        :meth:`.DDLCompiler.create_table_suffix`, allowing custom dialects
        to add keywords after the "CREATE TABLE" clause.  Pull request
        courtesy Mark Sandan.

    .. change::
        :tags: feature, sql
        :pullreq: github:231

        Negative integer indexes are now accommodated by rows
        returned from a :class:`.ResultProxy`.  Pull request courtesy
        Emanuele Gaifas.

        .. seealso::

            :ref:`change_gh_231`

    .. change::
        :tags: feature, sqlite
        :tickets: 3629

        The SQLite dialect now reflects the names of primary key constraints.
        Pull request courtesy Diana Clarke.

        .. seealso::

            :ref:`change_3629`

    .. change::
        :tags: feature, sql
        :tickets: 2857

        Added :meth:`.Select.lateral` and related constructs to allow
        for the SQL standard LATERAL keyword, currently only supported
        by Postgresql.

        .. seealso::

            :ref:`change_2857`

    .. change::
        :tags: feature, sql
        :tickets: 1957
        :pullreq: github:209

        Added support for rendering "FULL OUTER JOIN" to both Core and ORM.
        Pull request courtesy Stefan Urbanek.

        .. seealso::

            :ref:`change_1957`

    .. change::
        :tags: feature, engine

        Added connection pool events :meth:`ConnectionEvents.close`,
        :meth:`.ConnectionEvents.detach`,
        :meth:`.ConnectionEvents.close_detached`.

    .. change::
        :tags: bug, orm, mysql
        :tickets: 3680

        Further continuing on the common MySQL exception case of
        a savepoint being cancelled first covered in :ticket:`2696`,
        the failure mode in which the :class:`.Session` is placed when a
        SAVEPOINT vanishes before rollback has been improved to allow the
        :class:`.Session` to still function outside of that savepoint.
        It is assumed that the savepoint operation failed and was cancelled.

        .. seealso::

            :ref:`change_3680`

    .. change::
        :tags: feature, mssql
        :tickets: 3534

        Added basic isolation level support to the SQL Server dialects
        via :paramref:`.create_engine.isolation_level` and
        :paramref:`.Connection.execution_options.isolation_level`
        parameters.

        .. seealso::

            :ref:`change_3534`

    .. change::
        :tags: feature, mysql
        :tickets: 3332

        Added support for "autocommit" on MySQL drivers, via the
        AUTOCOMMIT isolation level setting.  Pull request courtesy
        Roman Podoliaka.

        .. seealso::

            :ref:`change_3332`

    .. change::
        :tags: bug, orm
        :tickets: 3677

        Fixed bug where a newly inserted instance that is rolled back
        would still potentially cause persistence conflicts on the next
        transaction, because the instance would not be checked that it
        was expired.   This fix will resolve a large class of cases that
        erronously cause the "New instance with identity X conflicts with
        persistent instance Y" error.

        .. seealso::

            :ref:`change_3677`

    .. change::
        :tags: bug, orm
        :tickets: 3662

        An improvement to the workings of :meth:`.Query.correlate` such
        that when a "polymorphic" entity is used which represents a straight
        join of several tables, the statement will ensure that all the
        tables within the join are part of what's correlating.

        .. seealso::

            :ref:`change_3662`

    .. change::
        :tags: bug, orm
        :tickets: 3431

        Fixed bug which would cause an eagerly loaded many-to-one attribute
        to not be loaded, if the joined eager load were from a row where the
        same entity were present multiple times, some calling for the attribute
        to be eagerly loaded and others not.  The logic here is revised to
        take in the attribute even though a different loader path has
        handled the parent entity already.

        .. seealso::

            :ref:`change_3431`

    .. change::
        :tags: feature, engine
        :tickets: 2837

        All string formatting of bound parameter sets and result rows for
        logging, exception, and  ``repr()`` purposes now truncate very large
        scalar values within each collection, including an
        "N characters truncated"
        notation, similar to how the display for large multiple-parameter sets
        are themselves truncated.


        .. seealso::

            :ref:`change_2837`

    .. change::
        :tags: feature, ext
        :tickets: 3297

        Added :class:`.MutableSet` and :class:`.MutableList` helper classes
        to the :ref:`mutable_toplevel` extension.  Pull request courtesy
        Jeong YunWon.

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
        :tags: feature, orm
        :tickets: 3512
        :pullreq: github:193

        Added new relationship loading strategy :func:`.orm.raiseload` (also
        accessible via ``lazy='raise'``).  This strategy behaves almost like
        :func:`.orm.noload` but instead of returning ``None`` it raises an
        InvalidRequestError.  Pull request courtesy Adrian Moennich.

        .. seealso::

            :ref:`change_3512`

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
