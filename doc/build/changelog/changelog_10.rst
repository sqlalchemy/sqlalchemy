==============
1.0 Changelog
==============

.. changelog_imports::

    .. include:: changelog_09.rst
        :start-line: 5

    .. include:: changelog_08.rst
        :start-line: 5

    .. include:: changelog_07.rst
        :start-line: 5

.. changelog::
	:version: 1.0.0

    Version 1.0.0 is the first release of the 1.0 series.   Many changes
    described here are also present in the 0.9 and sometimes the 0.8
    series as well.  For changes that are specific to 1.0 with an emphasis
    on compatibility concerns, see :doc:`/changelog/migration_10`.

    .. change::
        :tags: bug, declarative
        :tickets: 2670

        A relationship set up with :class:`.declared_attr` on
        a :class:`.AbstractConcreteBase` base class will now be configured
        on the abstract base mapping automatically, in addition to being
        set up on descendant concrete classes as usual.

        .. seealso::

            :ref:`feature_3150`

    .. change::
        :tags: feature, declarative
        :tickets: 3150

        The :class:`.declared_attr` construct has newly improved
        behaviors and features in conjunction with declarative.  The
        decorated function will now have access to the final column
        copies present on the local mixin when invoked, and will also
        be invoked exactly once for each mapped class, the returned result
        being memoized.   A new modifier :attr:`.declared_attr.cascading`
        is added as well.

        .. seealso::

            :ref:`feature_3150`

    .. change::
        :tags: feature, ext
        :tickets: 3210

        The :mod:`sqlalchemy.ext.automap` extension will now set
        ``cascade="all, delete-orphan"`` automatically on a one-to-many
        relationship/backref where the foreign key is detected as containing
        one or more non-nullable columns.  This argument is present in the
        keywords passed to :func:`.automap.generate_relationship` in this
        case and can still be overridden.  Additionally, if the
        :class:`.ForeignKeyConstraint` specifies ``ondelete="CASCADE"``
        for a non-nullable or ``ondelete="SET NULL"`` for a nullable set
        of columns, the argument ``passive_deletes=True`` is also added to the
        relationship.  Note that not all backends support reflection of
        ondelete, but backends that do include Postgresql and MySQL.

    .. change::
        :tags: feature, sql
        :tickets: 3206

        Added new method :meth:`.Select.with_statement_hint` and ORM
        method :meth:`.Query.with_statement_hint` to support statement-level
        hints that are not specific to a table.

    .. change::
        :tags: bug, sqlite
        :tickets: 3203
        :pullreq: bitbucket:31

        SQLite now supports reflection of unique constraints from
        temp tables; previously, this would fail with a TypeError.
        Pull request courtesy Johannes Erdfelt.

        .. seealso::

            :ref:`change_3204` - changes regarding SQLite temporary
            table and view reflection.

    .. change::
        :tags: bug, sqlite
        :tickets: 3204

        Added :meth:`.Inspector.get_temp_table_names` and
        :meth:`.Inspector.get_temp_view_names`; currently, only the
        SQLite and Oracle dialects support these methods.  The return of
        temporary table and view names has been **removed** from SQLite and
        Oracle's version of :meth:`.Inspector.get_table_names` and
        :meth:`.Inspector.get_view_names`; other database backends cannot
        support this information (such as MySQL), and the scope of operation
        is different in that the tables can be local to a session and
        typically aren't supported in remote schemas.

        .. seealso::

            :ref:`change_3204`

    .. change::
        :tags: feature, postgresql
        :tickets: 2891
        :pullreq: github:128

        Support has been added for reflection of materialized views
        and foreign tables, as well as support for materialized views
        within :meth:`.Inspector.get_view_names`, and a new method
        :meth:`.PGInspector.get_foreign_table_names` available on the
        Postgresql version of :class:`.Inspector`.  Pull request courtesy
        Rodrigo Menezes.

        .. seealso::

            :ref:`feature_2891`


    .. change::
        :tags: feature, orm

        Added new event handlers :meth:`.AttributeEvents.init_collection`
        and :meth:`.AttributeEvents.dispose_collection`, which track when
        a collection is first associated with an instance and when it is
        replaced.  These handlers supersede the :meth:`.collection.linker`
        annotation. The old hook remains supported through an event adapter.

    .. change::
        :tags: bug, orm
        :tickets: 3148, 3188

        A major rework to the behavior of expression labels, most
        specifically when used with ColumnProperty constructs with
        custom SQL expressions and in conjunction with the "order by
        labels" logic first introduced in 0.9.  Fixes include that an
        ``order_by(Entity.some_col_prop)`` will now make use of "order by
        label" rules even if Entity has been subject to aliasing,
        either via inheritance rendering or via the use of the
        ``aliased()`` construct; rendering of the same column property
        multiple times with aliasing (e.g. ``query(Entity.some_prop,
        entity_alias.some_prop)``) will label each occurrence of the
        entity with a distinct label, and additionally "order by
        label" rules will work for both (e.g.
        ``order_by(Entity.some_prop, entity_alias.some_prop)``).
        Additional issues that could prevent the "order by label"
        logic from working in 0.9, most notably that the state of a
        Label could change such that "order by label" would stop
        working depending on how things were called, has been fixed.

        .. seealso::

            :ref:`bug_3188`


    .. change::
        :tags: bug, mysql
        :tickets: 3186

        MySQL boolean symbols "true", "false" work again.  0.9's change
        in :ticket:`2682` disallowed the MySQL dialect from making use of the
        "true" and "false" symbols in the context of "IS" / "IS NOT", but
        MySQL supports this syntax even though it has no boolean type.
        MySQL remains "non native boolean", but the :func:`.true`
        and :func:`.false` symbols again produce the
        keywords "true" and "false", so that an expression like
        ``column.is_(true())`` again works on MySQL.

        .. seealso::

            :ref:`bug_3186`

    .. change::
        :tags: changed, mssql
        :tickets: 3182

        The hostname-based connection format for SQL Server when using
        pyodbc will no longer specify a default "driver name", and a warning
        is emitted if this is missing.  The optimal driver name for SQL Server
        changes frequently and is per-platform, so hostname based connections
        need to specify this.  DSN-based connections are preferred.

        .. seealso::

            :ref:`change_3182`

    .. change::
        :tags: changed, sql

        The :func:`~.expression.column` and :func:`~.expression.table`
        constructs are now importable from the "from sqlalchemy" namespace,
        just like every other Core construct.

    .. change::
        :tags: changed, sql
        :tickets: 2992

        The implicit conversion of strings to :func:`.text` constructs
        when passed to most builder methods of :func:`.select` as
        well as :class:`.Query` now emits a warning with just the
        plain string sent.   The textual conversion still proceeds normally,
        however.  The only method that accepts a string without a warning
        are the "label reference" methods like order_by(), group_by();
        these functions will now at compile time attempt to resolve a single
        string argument to a column or label expression present in the
        selectable; if none is located, the expression still renders, but
        you get the warning again. The rationale here is that the implicit
        conversion from string to text is more unexpected than not these days,
        and it is better that the user send more direction to the Core / ORM
        when passing a raw string as to what direction should be taken.
        Core/ORM tutorials have been updated to go more in depth as to how text
        is handled.

        .. seealso::

            :ref:`migration_2992`


    .. change::
        :tags: feature, engine
        :tickets: 3178

        A new style of warning can be emitted which will "filter" up to
        N occurrences of a parameterized string.   This allows parameterized
        warnings that can refer to their arguments to be delivered a fixed
        number of times until allowing Python warning filters to squelch them,
        and prevents memory from growing unbounded within Python's
        warning registries.

        .. seealso::

            :ref:`feature_3178`

    .. change::
        :tags: feature, orm

        The :class:`.Query` will raise an exception when :meth:`.Query.yield_per`
        is used with mappings or options where either
        subquery eager loading, or joined eager loading with collections,
        would take place.  These loading strategies are
        not currently compatible with yield_per, so by raising this error,
        the method is safer to use.  Eager loads can be disabled with
        the ``lazyload('*')`` option or :meth:`.Query.enable_eagerloads`.

        .. seealso::

            :ref:`migration_yield_per_eager_loading`

    .. change::
        :tags: bug, orm
        :tickets: 3177

        Changed the approach by which the "single inheritance criterion"
        is applied, when using :meth:`.Query.from_self`, or its common
        user :meth:`.Query.count`.  The criteria to limit rows to those
        with a certain type is now indicated on the inside subquery,
        not the outside one, so that even if the "type" column is not
        available in the columns clause, we can filter on it on the "inner"
        query.

        .. seealso::

            :ref:`migration_3177`

    .. change::
        :tags: changed, orm

        The ``proc()`` callable passed to the ``create_row_processor()``
        method of custom :class:`.Bundle` classes now accepts only a single
        "row" argument.

        .. seealso::

            :ref:`bundle_api_change`

    .. change::
        :tags: changed, orm

        Deprecated event hooks removed:  ``populate_instance``,
        ``create_instance``, ``translate_row``, ``append_result``

        .. seealso::

            :ref:`migration_deprecated_orm_events`

    .. change::
        :tags: bug, orm
        :tickets: 3145

        Made a small adjustment to the mechanics of lazy loading,
        such that it has less chance of interfering with a joinload() in the
        very rare circumstance that an object points to itself; in this
        scenario, the object refers to itself while loading its attributes
        which can cause a mixup between loaders.   The use case of
        "object points to itself" is not fully supported, but the fix also
        removes some overhead so for now is part of testing.

    .. change::
        :tags: feature, orm
        :tickets: 3176

        A new implementation for :class:`.KeyedTuple` used by the
        :class:`.Query` object offers dramatic speed improvements when
        fetching large numbers of column-oriented rows.

        .. seealso::

            :ref:`feature_3176`

    .. change::
        :tags: feature, orm
        :tickets: 3008

        The behavior of :paramref:`.joinedload.innerjoin` as well as
        :paramref:`.relationship.innerjoin` is now to use "nested"
        inner joins, that is, right-nested, as the default behavior when an
        inner join joined eager load is chained to an outer join eager load.

        .. seealso::

            :ref:`migration_3008`

    .. change::
        :tags: bug, orm
        :tickets: 3171

        The "resurrect" ORM event has been removed.  This event hook had
        no purpose since the old "mutable attribute" system was removed
        in 0.8.

    .. change::
        :tags: bug, sql
        :tickets: 3169

        Using :meth:`.Insert.from_select`  now implies ``inline=True``
        on :func:`.insert`.  This helps to fix a bug where an
        INSERT...FROM SELECT construct would inadvertently be compiled
        as "implicit returning" on supporting backends, which would
        cause breakage in the case of an INSERT that inserts zero rows
        (as implicit returning expects a row), as well as arbitrary
        return data in the case of an INSERT that inserts multiple
        rows (e.g. only the first row of many).
        A similar change is also applied to an INSERT..VALUES
        with multiple parameter sets; implicit RETURNING will no longer emit
        for this statement either.  As both of these constructs deal
        with varible numbers of rows, the
        :attr:`.ResultProxy.inserted_primary_key` accessor does not
        apply.   Previously, there was a documentation note that one
        may prefer ``inline=True`` with INSERT..FROM SELECT as some databases
        don't support returning and therefore can't do "implicit" returning,
        but there's no reason an INSERT...FROM SELECT needs implicit returning
        in any case.   Regular explicit :meth:`.Insert.returning` should
        be used to return variable numbers of result rows if inserted
        data is needed.

    .. change::
        :tags: bug, orm
        :tickets: 3167

        Fixed bug where attribute "set" events or columns with
        ``@validates`` would have events triggered within the flush process,
        when those columns were the targets of a "fetch and populate"
        operation, such as an autoincremented primary key, a Python side
        default, or a server-side default "eagerly" fetched via RETURNING.

    .. change::
        :tags: feature, oracle

        Added support for the Oracle table option ON COMMIT.

    .. change::
        :tags: feature, postgresql
        :tickets: 2051

        Added support for PG table options TABLESPACE, ON COMMIT,
        WITH(OUT) OIDS, and INHERITS, when rendering DDL via
        the :class:`.Table` construct.   Pull request courtesy
        malikdiarra.

        .. seealso::

            :ref:`postgresql_table_options`

    .. change::
        :tags: bug, orm, py3k

        The :class:`.IdentityMap` exposed from :class:`.Session.identity`
        now returns lists for ``items()`` and ``values()`` in Py3K.
        Early porting to Py3K here had these returning iterators, when
        they technically should be "iterable views"..for now, lists are OK.

    .. change::
        :tags: orm, feature

        UPDATE statements can now be batched within an ORM flush
        into more performant executemany() call, similarly to how INSERT
        statements can be batched; this will be invoked within flush
        to the degree that subsequent UPDATE statements for the
        same mapping and table involve the identical columns within the
        VALUES clause, that no SET-level SQL expressions
        are embedded, and that the versioning requirements for the mapping
        are compatible with the backend dialect's ability to return
        a correct rowcount for an executemany operation.

    .. change::
        :tags: engine, bug
        :tickets: 3163

        Removing (or adding) an event listener at the same time that the event
        is being run itself, either from inside the listener or from a
        concurrent thread, now raises a RuntimeError, as the collection used is
        now an instance of ``colletions.deque()`` and does not support changes
        while being iterated.  Previously, a plain Python list was used where
        removal from inside the event itself would produce silent failures.

    .. change::
        :tags: orm, feature
        :tickets: 2963

        The ``info`` parameter has been added to the constructor for
        :class:`.SynonymProperty` and :class:`.ComparableProperty`.

    .. change::
        :tags: sql, feature
        :tickets: 2963

        The ``info`` parameter has been added as a constructor argument
        to all schema constructs including :class:`.MetaData`,
        :class:`.Index`, :class:`.ForeignKey`, :class:`.ForeignKeyConstraint`,
        :class:`.UniqueConstraint`, :class:`.PrimaryKeyConstraint`,
        :class:`.CheckConstraint`.

    .. change::
        :tags: orm, feature
        :tickets: 2971

        The :meth:`.InspectionAttr.info` collection is now moved down to
        :class:`.InspectionAttr`, where in addition to being available
        on all :class:`.MapperProperty` objects, it is also now available
        on hybrid properties, association proxies, when accessed via
        :attr:`.Mapper.all_orm_descriptors`.

    .. change::
        :tags: sql, feature
        :tickets: 3027
        :pullrequest: bitbucket:29

        The :paramref:`.Table.autoload_with` flag now implies that
        :paramref:`.Table.autoload` should be ``True``.  Pull request
        courtesy Malik Diarra.

    .. change::
        :tags: postgresql, feature
        :pullreq: github:126

        Added new method :meth:`.PGInspector.get_enums`, when using the
        inspector for Postgresql will provide a list of ENUM types.
        Pull request courtesy Ilya Pekelny.

    .. change::
        :tags: mysql, bug

        The MySQL dialect will now disable :meth:`.ConnectionEvents.handle_error`
        events from firing for those statements which it uses internally
        to detect if a table exists or not.   This is achieved using an
        execution option ``skip_user_error_events`` that disables the handle
        error event for the scope of that execution.   In this way, user code
        that rewrites exceptions doesn't need to worry about the MySQL
        dialect or other dialects that occasionally need to catch
        SQLAlchemy specific exceptions.

    .. change::
        :tags: mysql, bug
        :tickets: 2515

        Changed the default value of "raise_on_warnings" to False for
        MySQLconnector.  This was set at True for some reason.  The "buffered"
        flag unfortunately must stay at True as MySQLconnector does not allow
        a cursor to be closed unless all results are fully fetched.

    .. change::
        :tags: bug, orm
        :tickets: 3117

        The "evaulator" for query.update()/delete() won't work with multi-table
        updates, and needs to be set to `synchronize_session=False` or
        `synchronize_session='fetch'`; this now raises an exception, with a
        message to change the synchronize setting.
        This is upgraded from a warning emitted as of 0.9.7.

    .. change::
        :tags: removed

        The Drizzle dialect has been removed from the Core; it is now
        available as `sqlalchemy-drizzle <https://bitbucket.org/zzzeek/sqlalchemy-drizzle>`_,
        an independent, third party dialect.  The dialect is still based
        almost entirely off of the MySQL dialect present in SQLAlchemy.

        .. seealso::

            :ref:`change_2984`

    .. change::
        :tags: enhancement, orm
        :tickets: 3061

        Adjustment to attribute mechanics concerning when a value is
        implicitly initialized to None via first access; this action,
        which has always resulted in a population of the attribute,
        no longer does so; the None value is returned but the underlying
        attribute receives no set event.  This is consistent with how collections
        work and allows attribute mechanics to behave more consistently;
        in particular, getting an attribute with no value does not squash
        the event that should proceed if the value is actually set to None.

        .. seealso::

        	:ref:`migration_3061`

	.. change::
		:tags: feature, sql
		:tickets: 3034

		The :meth:`.Select.limit` and :meth:`.Select.offset` methods
		now accept any SQL expression, in addition to integer values, as
		arguments.  Typically this is used to allow a bound parameter to be
		passed, which can be substituted with a value later thus allowing
		Python-side caching of the SQL query.   The implementation
		here is fully backwards compatible with existing third party dialects,
		however those dialects which implement special LIMIT/OFFSET systems
		will need modification in order to take advantage of the new
		capabilities.  Limit and offset also support "literal_binds" mode,
        where bound parameters are rendered inline as strings based on
        a compile-time option.
        Work on this feature is courtesy of Dobes Vandermeer.


		.. seealso::

			:ref:`feature_3034`.
