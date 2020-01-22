=============
1.3 Changelog
=============

.. changelog_imports::

    .. include:: changelog_12.rst
        :start-line: 5

    .. include:: changelog_11.rst
        :start-line: 5

.. changelog::
    :version: 1.3.13
    :released: January 22, 2020

    .. change::
        :tags: bug, postgresql
        :tickets: 5039

        Fixed issue where the PostgreSQL dialect would fail to parse a reflected
        CHECK constraint that was a boolean-valued function (as opposed to a
        boolean-valued expression).

    .. change::
        :tags: bug, ext
        :tickets: 5086

        Fixed bug in sqlalchemy.ext.serializer where a unique
        :class:`.BindParameter` object could conflict with itself if it were
        present in the mapping itself, as well as the filter condition of the
        query, as one side would be used against the non-deserialized version and
        the other side would use the deserialized version.  Logic is added to
        :class:`.BindParameter` similar to its "clone" method which will uniquify
        the parameter name upon deserialize so that it doesn't conflict with its
        original.


    .. change::
        :tags: usecase, sql
        :tickets: 5079

        A function created using :class:`.GenericFunction` can now specify that the
        name of the function should be rendered with or without quotes by assigning
        the :class:`.quoted_name` construct to the .name element of the object.
        Prior to 1.3.4, quoting was never applied to function names, and some
        quoting was introduced in :ticket:`4467` but no means to force quoting for
        a mixed case name was available.  Additionally, the :class:`.quoted_name`
        construct when used as the name will properly register its lowercase name
        in the function registry so that the name continues to be available via the
        ``func.`` registry.

        .. seealso::

            :class:`.GenericFunction`


    .. change::
        :tags: bug, engine
        :tickets: 5048

        Fixed issue where the collection of value processors on a
        :class:`.Compiled` object would be mutated when "expanding IN" parameters
        were used with a datatype that has bind value processors; in particular,
        this would mean that when using statement caching and/or baked queries, the
        same compiled._bind_processors collection would be mutated concurrently.
        Since these processors are the same function for a given bind parameter
        namespace every time, there was no actual negative effect of this issue,
        however, the execution of a :class:`.Compiled` object should never be
        causing any changes in its state, especially given that they are intended
        to be thread-safe and reusable once fully constructed.


    .. change::
        :tags: tests, postgresql
        :tickets: 5057

        Improved detection of two phase transactions requirement for the PostgreSQL
        database by testing that max_prepared_transactions is set to a value
        greater than 0.  Pull request courtesy Federico Caselli.


    .. change::
        :tags: bug, orm, engine
        :tickets: 5056, 5050, 5071

        Added test support and repaired a wide variety of unnecessary reference
        cycles created for short-lived objects, mostly in the area of ORM queries.
        Thanks much to Carson Ip for the help on this.


    .. change::
        :tags: orm, bug
        :tickets: 5107

        Fixed regression in loader options introduced in 1.3.0b3 via :ticket:`4468`
        where the ability to create a loader option using
        :meth:`.PropComparator.of_type` targeting an aliased entity that is an
        inheriting subclass of the entity which the preceding relationship refers
        to would fail to produce a matching path.   See also :ticket:`5082` fixed
        in this same release which involves a similar kind of issue.

    .. change::
        :tags: bug, tests
        :tickets: 4946

        Fixed a few test failures which would occur on Windows due to SQLite file
        locking issues, as well as some timing issues in connection pool related
        tests; pull request courtesy Federico Caselli.


    .. change::
        :tags: orm, bug
        :tickets: 5082

        Fixed regression in joined eager loading introduced in 1.3.0b3 via
        :ticket:`4468` where the ability to create a joined option across a
        :func:`.with_polymorphic` into a polymorphic subclass using
        :meth:`.RelationshipProperty.of_type` and then further along regular mapped
        relationships would fail as the polymorphic subclass would not add itself
        to the load path in a way that could be located by the loader strategy.  A
        tweak has been made to resolve this scenario.


    .. change::
        :tags: performance, orm

        Identified a performance issue in the system by which a join is constructed
        based on a mapped relationship.   The clause adaption system would be used
        for the majority of join expressions including in the common case where no
        adaptation is needed.   The conditions under which this adaptation occur
        have been refined so that average non-aliased joins along a simple
        relationship without a "secondary" table use about 70% less function calls.


    .. change::
        :tags: usecase, postgresql
        :tickets: 5040

        Added support for prefixes to the :class:`.CTE` construct, to allow
        support for Postgresql 12 "MATERIALIZED" and "NOT MATERIALIZED" phrases.
        Pull request courtesy Marat Sharafutdinov.

        .. seealso::

            :meth:`.HasCTE.cte`

    .. change::
        :tags: bug, mssql
        :tickets: 5045

        Fixed issue where a timezone-aware ``datetime`` value being converted to
        string for use as a parameter value of a :class:`.mssql.DATETIMEOFFSET`
        column was omitting the fractional seconds.

    .. change::
        :tags: bug, orm
        :tickets: 5068

        Repaired a warning in the ORM flush process that was not covered by  test
        coverage when deleting objects that use the "version_id" feature. This
        warning is generally unreachable unless using a dialect that sets the
        "supports_sane_rowcount" flag to False, which  is not typically the case
        however is possible for some MySQL configurations as well as older Firebird
        drivers, and likely some third party dialects.

    .. change::
        :tags: bug, orm
        :tickets: 5065

        Fixed bug where usage of joined eager loading would not properly wrap the
        query inside of a subquery when :meth:`.Query.group_by` were used against
        the query.   When any kind of result-limiting approach is used, such as
        DISTINCT, LIMIT, OFFSET, joined eager loading embeds the row-limited query
        inside of a subquery so that the collection results are not impacted.   For
        some reason, the presence of GROUP BY was never included in this criterion,
        even though it has a similar effect as using DISTINCT.   Additionally, the
        bug would prevent using GROUP BY at all for a joined eager load query for
        most database platforms which forbid non-aggregated, non-grouped columns
        from being in the query, as the additional columns for the joined eager
        load would not be accepted by the database.



.. changelog::
    :version: 1.3.12
    :released: December 16, 2019

    .. change::
        :tags: bug, sql
        :tickets: 5028

        Fixed bug where "distinct" keyword passed to :func:`.select` would not
        treat a string value as a "label reference" in the same way that the
        :meth:`.select.distinct` does; it would instead raise unconditionally. This
        keyword argument and the others passed to :func:`.select` will ultimately
        be deprecated for SQLAlchemy 2.0.


    .. change::
        :tags: bug, orm
        :tickets: 4997

        Fixed issue involving ``lazy="raise"`` strategy where an ORM delete of an
        object would raise for a simple "use-get" style many-to-one relationship
        that had lazy="raise" configured.  This is inconsistent vs. the change
        introduced in 1.3 as part of :ticket:`4353`, where it was established that
        a history operation that does not expect emit SQL should bypass the
        ``lazy="raise"`` check, and instead effectively treat it as
        ``lazy="raise_on_sql"`` for this case.  The fix adjusts the lazy loader
        strategy to not raise for the case where the lazy load was instructed that
        it should not emit SQL if the object were not present.

    .. change::
        :tags: bug, sql

        Changed the text of the exception for "Can't resolve label reference" to
        include other kinds of label coercions, namely that "DISTINCT" is also in
        this category under the PostgreSQL dialect.


    .. change::
        :tags: bug, orm
        :tickets: 5000

        Fixed regression introduced in 1.3.0 related to the association proxy
        refactor in :ticket:`4351` that prevented :func:`.composite` attributes
        from working in terms of an association proxy that references them.

    .. change::
        :tags: bug, mssql
        :tickets: 4983

        Repaired support for the :class:`.mssql.DATETIMEOFFSET` datatype on PyODBC,
        by adding PyODBC-level result handlers as it does not include native
        support for this datatype.  This includes usage of the Python 3 "timezone"
        tzinfo subclass in order to set up a timezone, which on Python 2 makes
        use of a minimal backport of "timezone" in sqlalchemy.util.


    .. change::
        :tags: bug, orm
        :tickets: 4993

        Setting persistence-related flags on :func:`.relationship` while also
        setting viewonly=True will now emit a regular warning, as these flags do
        not make sense for a viewonly=True relationship.   In particular, the
        "cascade" settings have their own warning that is generated based on the
        individual values, such as "delete, delete-orphan", that should not apply
        to a viewonly relationship.   Note however that in the case of "cascade",
        these settings are still erroneously taking effect even though the
        relationship is set up as "viewonly".   In 1.4, all persistence-related
        cascade settings will be disallowed on a viewonly=True relationship in
        order to resolve this issue.

    .. change::
        :tags: bug, sqlite
        :tickets: 5014

        Fixed issue to workaround SQLite's behavior of assigning "numeric" affinity
        to JSON datatypes, first described at :ref:`change_3850`, which returns
        scalar numeric JSON values as a number and not as a string that can be JSON
        deserialized.  The SQLite-specific JSON deserializer now gracefully
        degrades for this case as an exception and bypasses deserialization for
        single numeric values, as from a JSON perspective they are already
        deserialized.



    .. change::
        :tags: bug, orm, py3k
        :tickets: 4990

        Fixed issue where when assigning a collection to itself as a slice, the
        mutation operation would fail as it would first erase the assigned
        collection inadvertently.   As an assignment that does not change  the
        contents should not generate events, the operation is now a no-op. Note
        that the fix only applies to Python 3; in Python 2, the ``__setitem__``
        hook isn't called in this case; ``__setslice__`` is used instead which
        recreates the list item-by-item in all cases.

    .. change::
        :tags: bug, orm
        :tickets: 5034

        Fixed issue where by if the "begin" of a transaction failed at the Core
        engine/connection level, such as due to network error or database is locked
        for some transactional recipes, within the context of the :class:`.Session`
        procuring that connection from the conneciton pool and then immediately
        returning it, the ORM :class:`.Session` would not close the connection
        despite this connection not being stored within the state of that
        :class:`.Session`.  This would lead to the connection being cleaned out by
        the connection pool weakref handler within garbage collection which is an
        unpreferred codepath that in some special configurations can emit errors in
        standard error.

.. changelog::
    :version: 1.3.11
    :released: November 11, 2019

    .. change::
        :tags: bug, mssql
        :tickets: 4973

        Fixed issue in MSSQL dialect where an expression-based OFFSET value in a
        SELECT would be rejected, even though the dialect can render this
        expression inside of a ROW NUMBER-oriented LIMIT/OFFSET construct.


    .. change::
        :tags: orm, usecase
        :tickets: 4934

        Added accessor :meth:`.Query.is_single_entity` to :class:`.Query`, which
        will indicate if the results returned by this :class:`.Query` will be a
        list of ORM entities, or a tuple of entities or column expressions.
        SQLAlchemy hopes to improve upon the behavior of single entity / tuples in
        future releases such that the behavior would be explicit up front, however
        this attribute should be helpful with the current behavior.  Pull request
        courtesy Patrick Hayes.

    .. change::
        :tags: bug, mysql
        :tickets: 4945

        Added "Connection was killed" message interpreted from the base
        pymysql.Error class in order to detect closed connection, based on reports
        that this message is arriving via a pymysql.InternalError() object which
        indicates pymysql is not handling it correctly.

    .. change::
        :tags: bug, orm
        :tickets: 4954

        The :paramref:`.relationship.omit_join` flag was not intended to be
        manually set to True, and will now emit a warning when this occurs.  The
        omit_join optimization is detected automatically, and the ``omit_join``
        flag was only intended to disable the optimization in the hypothetical case
        that the optimization may have interfered with correct results, which has
        not been observed with the modern version of this feature.   Setting the
        flag to True when it is not automatically detected may cause the selectin
        load feature to not work correctly when a non-default primary join
        condition is in use.


    .. change::
        :tags: bug, orm
        :tickets: 4915

        A warning is emitted if a primary key value is passed to :meth:`.Query.get`
        that consists of None for all primary key column positions.   Previously,
        passing a single None outside of a tuple would raise a ``TypeError`` and
        passing a composite None (tuple of None values) would silently pass
        through.   The fix now coerces the single None into a tuple where it is
        handled consistently with the other None conditions.  Thanks to Lev
        Izraelit for the help with this.


    .. change::
        :tags: bug, orm
        :tickets: 4947

        The :class:`.BakedQuery` will not cache a query that was modified by a
        :meth:`.QueryEvents.before_compile` event, so that compilation hooks that
        may be applying ad-hoc modifications to queries will take effect on each
        run.  In particular this is helpful for events that modify queries used in
        lazy loading as well as eager loading such as "select in" loading.  In
        order to re-enable caching for a query modified by this event, a new
        flag ``bake_ok`` is added; see :ref:`baked_with_before_compile` for
        details.

        A longer term plan to provide a new form of SQL caching should solve this
        kind of issue more comprehensively.

    .. change::
        :tags: bug, tests
        :tickets: 4920

        Fixed test failures which would occur with newer SQLite as of version 3.30
        or greater, due to their addition of nulls ordering syntax as well as new
        restrictions on aggregate functions.  Pull request courtesy Nils Philippsen.



    .. change::
        :tags: bug, installation, windows
        :tickets: 4967

        Added a workaround for a setuptools-related failure that has been observed
        as occurring on Windows installations, where setuptools is not correctly
        reporting a build error when the MSVC build dependencies are not installed
        and therefore not allowing graceful degradation into non C extensions
        builds.

    .. change::
        :tags: bug, sql, py3k
        :tickets: 4931

        Changed the ``repr()`` of the :class:`.quoted_name` construct to use
        regular string repr() under Python 3, rather than running it through
        "backslashreplace" escaping, which can be misleading.

    .. change::
        :tags: bug, oracle, firebird
        :tickets: 4931

        Modified the approach of "name normalization" for the Oracle and Firebird
        dialects, which converts from the UPPERCASE-as-case-insensitive convention
        of these dialects into lowercase-as-case-insensitive for SQLAlchemy, to not
        automatically apply the :class:`.quoted_name` construct to a name that
        matches itself under upper or lower case conversion, as is the case for
        many non-european characters.   All names used within metadata structures
        are converted to :class:`.quoted_name` objects in any case; the change
        here would only affect the output of some inspection functions.

    .. change::
        :tags: bug, schema
        :tickets: 4911

        Fixed bug where a table that would have a column label overlap with a plain
        column name, such as "foo.id AS foo_id" vs. "foo.foo_id", would prematurely
        generate the ``._label`` attribute for a column before this overlap could
        be detected due to the use of the ``index=True`` or ``unique=True`` flag on
        the column in conjunction with the default naming convention of
        ``"column_0_label"``.  This would then lead to failures when ``._label``
        were used later to generate a bound parameter name, in particular those
        used by the ORM when generating the WHERE clause for an UPDATE statement.
        The issue has been fixed by using an alternate ``._label`` accessor for DDL
        generation that does not affect the state of the :class:`.Column`.   The
        accessor also bypasses the key-deduplication step as it is not necessary
        for DDL, the naming is now consistently ``"<tablename>_<columnname>"``
        without any subsequent numeric symbols when used in DDL.



    .. change::
        :tags: bug, engine
        :tickets: 4902

        Fixed bug where parameter repr as used in logging and error reporting needs
        additional context in order to distinguish between a list of parameters for
        a single statement and a list of parameter lists, as the "list of lists"
        structure could also indicate a single parameter list where the first
        parameter itself is a list, such as for an array parameter.   The
        engine/connection now passes in an additional boolean indicating how the
        parameters should be considered.  The only SQLAlchemy backend that expects
        arrays as parameters is that of  psycopg2 which uses pyformat parameters,
        so this issue has not been too apparent, however as other drivers that use
        positional gain more features it is important that this be supported. It
        also eliminates the need for the parameter repr function to guess based on
        the parameter structure passed.

    .. change::
        :tags: usecase, schema
        :tickets: 4894

        Added DDL support for "computed columns"; these are DDL column
        specifications for columns that have a server-computed value, either upon
        SELECT (known as "virtual") or at the point of which they are INSERTed or
        UPDATEd (known as "stored").  Support is established for Postgresql, MySQL,
        Oracle SQL Server and Firebird. Thanks to Federico Caselli for lots of work
        on this one.

        .. seealso::

            :ref:`computed_ddl`


    .. change::
        :tags: bug, engine, postgresql
        :tickets: 4955

        Fixed bug in :class:`.Inspector` where the cache key generation did not
        take into account arguments passed in the form of tuples, such as the tuple
        of view name styles to return for the PostgreSQL dialect. This would lead
        the inspector to cache too generally for a more specific set of criteria.
        The logic has been adjusted to include every keyword element in the cache,
        as every argument is expected to be appropriate for a cache else the
        caching decorator should be bypassed by the dialect.


    .. change::
        :tags: bug, mssql
        :tickets: 4923

        Fixed an issue in the :meth:`.Engine.table_names` method where it would
        feed the dialect's default schema name back into the dialect level table
        function, which in the case of SQL Server would interpret it as a
        dot-tokenized schema name as viewed by the mssql dialect, which would
        cause the method to fail in the case where the database username actually
        had a dot inside of it.  In 1.3, this method is still used by the
        :meth:`.MetaData.reflect` function so is a prominent codepath. In 1.4,
        which is the current master development branch, this issue doesn't exist,
        both because :meth:`.MetaData.reflect` isn't using this method nor does the
        method pass the default schema name explicitly.  The fix nonetheless
        guards against the default server name value returned by the dialect from
        being interpreted as dot-tokenized name under any circumstances by
        wrapping it in quoted_name().

    .. change::
        :tags: bug, orm
        :tickets: 4974

        Fixed ORM bug where a "secondary" table that referred to a selectable which
        in some way would refer to the local primary table would apply aliasing to
        both sides of the join condition when a relationship-related join, either
        via :meth:`.Query.join` or by :func:`.joinedload`, were generated.  The
        "local" side is now excluded.

    .. change::
        :tags: usecase, sql
        :tickets: 4276

        Added new accessors to expressions of type :class:`.JSON` to allow for
        specific datatype access and comparison, covering strings, integers,
        numeric, boolean elements.   This revises the documented approach of
        CASTing to string when comparing values, instead adding specific
        functionality into the PostgreSQL, SQlite, MySQL dialects to reliably
        deliver these basic types in all cases.

        .. seealso::

            :class:`.JSON`

            :meth:`.JSON.Comparator.as_string`

            :meth:`.JSON.Comparator.as_boolean`

            :meth:`.JSON.Comparator.as_float`

            :meth:`.JSON.Comparator.as_integer`

    .. change::
        :tags: usecase, oracle
        :tickets: 4799

        Added dialect-level flag ``encoding_errors`` to the cx_Oracle dialect,
        which can be specified as part of :func:`.create_engine`.   This is passed
        to SQLAlchemy's unicode decoding converter under Python 2, and to
        cx_Oracle's ``cursor.var()`` object as the ``encodingErrors`` parameter
        under Python 3, for the very unusual case that broken encodings are present
        in the target database which cannot be fetched unless error handling is
        relaxed.  The value is ultimately one of the Python "encoding errors"
        parameters passed to ``decode()``.

    .. change::
        :tags: usecase, sql
        :tickets: 4933

        The :func:`.text` construct now supports "unique" bound parameters, which
        will dynamically uniquify themselves on compilation thus allowing multiple
        :func:`.text` constructs with the same bound parameter names to be combined
        together.


    .. change::
        :tags: bug, oracle
        :tickets: 4913

        The :class:`.sqltypes.NCHAR` datatype will now bind to the
        ``cx_Oracle.FIXED_NCHAR`` DBAPI data bindings when used in a bound
        parameter, which supplies proper comparison behavior against a
        variable-length string.  Previously, the :class:`.sqltypes.NCHAR` datatype
        would bind to ``cx_oracle.NCHAR`` which is not fixed length; the
        :class:`.sqltypes.CHAR` datatype already binds to ``cx_Oracle.FIXED_CHAR``
        so it is now consistent that :class:`.sqltypes.NCHAR` binds to
        ``cx_Oracle.FIXED_NCHAR``.



    .. change::
        :tags: bug, firebird
        :tickets: 4903

        Added additional "disconnect" message "Error writing data to the
        connection" to Firebird disconnection detection.  Pull request courtesy
        lukens.

.. changelog::
    :version: 1.3.10
    :released: October 9, 2019

    .. change::
        :tags: bug, mssql
        :tickets: 4857

        Fixed bug in SQL Server dialect with new "max_identifier_length" feature
        where the mssql dialect already featured this flag, and the implementation
        did not accommodate for the new initialization hook correctly.


    .. change::
        :tags: bug, oracle
        :tickets: 4898, 4857

        Fixed regression in Oracle dialect that was inadvertently using max
        identifier length of 128 characters on Oracle server 12.2 and greater even
        though the stated contract for the remainder of the 1.3 series is  that
        this value stays at 30 until version SQLAlchemy 1.4.  Also repaired issues
        with the retrieval of the "compatibility" version, and removed the warning
        emitted when the "v$parameter" view was not accessible as this was  causing
        user confusion.

.. changelog::
    :version: 1.3.9
    :released: October 4, 2019

    .. change::
        :tags: usecase, engine
        :tickets: 4857

        Added new :func:`.create_engine` parameter
        :paramref:`.create_engine.max_identifier_length`. This overrides the
        dialect-coded "max identifier length" in order to accommodate for databases
        that have recently changed this length and the SQLAlchemy dialect has
        not yet been adjusted to detect for that version.  This parameter interacts
        with the existing :paramref:`.create_engine.label_length` parameter in that
        it establishes the maximum (and default) value for anonymously generated
        labels.   Additionally, post-connection detection of max identifier lengths
        has been added to the dialect system.  This feature is first being used
        by the Oracle dialect.

        .. seealso::

            :ref:`oracle_max_identifier_lengths` - in the Oracle dialect documentation

    .. change::
        :tags: usecase, oracle
        :tickets: 4857

        The Oracle dialect now emits a warning if Oracle version 12.2 or greater is
        used, and the :paramref:`.create_engine.max_identifier_length` parameter is
        not set.   The version in this specific case defaults to that of the
        "compatibility" version set in the Oracle server configuration, not the
        actual server version.   In version 1.4, the default max_identifier_length
        for 12.2 or greater will move to 128 characters.  In order to maintain
        forwards compatibility, applications should set
        :paramref:`.create_engine.max_identifier_length` to 30 in order to maintain
        the same length behavior, or to 128 in order to test the upcoming behavior.
        This length determines among other things how generated constraint names
        are truncated for statements like ``CREATE CONSTRAINT`` and ``DROP
        CONSTRAINT``, which means a the new length may produce a name-mismatch
        against a name that was generated with the old length, impacting database
        migrations.

        .. seealso::

            :ref:`oracle_max_identifier_lengths` - in the Oracle dialect documentation

    .. change::
        :tags: usecase, sqlite
        :tickets: 4863

        Added support for sqlite "URI" connections, which allow for sqlite-specific
        flags to be passed in the query string such as "read only" for Python
        sqlite3 drivers that support this.

        .. seealso::

            :ref:`pysqlite_uri_connections`

    .. change::
        :tags: bug, tests
        :tickets: 4285

        Fixed unit test regression released in 1.3.8 that would cause failure for
        Oracle, SQL Server and other non-native ENUM platforms due to new
        enumeration tests added as part of :ticket:`4285` enum sortability in the
        unit of work; the enumerations created constraints that were duplicated on
        name.

    .. change::
        :tags: bug, oracle
        :tickets: 4886

        Restored adding cx_Oracle.DATETIME to the setinputsizes() call when a
        SQLAlchemy :class:`.Date`, :class:`.DateTime` or :class:`.Time` datatype is
        used, as some complex queries require this to be present.  This was removed
        in the 1.2 series for arbitrary reasons.

    .. change::
        :tags: bug, mssql
        :tickets: 4883

        Added identifier quoting to the schema name applied to the "use" statement
        which is invoked when a SQL Server multipart schema name is used within  a
        :class:`.Table` that is being reflected, as well as for :class:`.Inspector`
        methods such as :meth:`.Inspector.get_table_names`; this accommodates for
        special characters or spaces in the database name.  Additionally, the "use"
        statement is not emitted if the current database matches the target owner
        database name being passed.

    .. change::
        :tags: bug, orm
        :tickets: 4872

        Fixed regression in selectinload loader strategy caused by :ticket:`4775`
        (released in version 1.3.6) where a many-to-one attribute of None would no
        longer be populated by the loader.  While this was usually not noticeable
        due to the lazyloader populating None upon get, it would lead to a detached
        instance error if the object were detached.

    .. change::
        :tags: bug, orm
        :tickets: 4873

        Passing a plain string expression to :meth:`.Session.query` is deprecated,
        as all string coercions were removed in :ticket:`4481` and this one should
        have been included.   The :func:`.literal_column` function may be used to
        produce a textual column expression.

    .. change::
        :tags: usecase, sql
        :tickets: 4847

        Added an explicit error message for the case when objects passed to
        :class:`.Table` are not :class:`.SchemaItem` objects, rather than resolving
        to an attribute error.


    .. change::
        :tags: bug, orm
        :tickets: 4890

        A warning is emitted for a condition in which the :class:`.Session` may
        implicitly swap an object out of the identity map for another one with the
        same primary key, detaching the old one, which can be an observed result of
        load operations which occur within the :meth:`.SessionEvents.after_flush`
        hook.  The warning is intended to notify the user that some special
        condition has caused this to happen and that the previous object may not be
        in the expected state.

    .. change::
        :tags: bug, sql
        :tickets: 4837

        Characters that interfere with "pyformat" or "named" formats in bound
        parameters, namely ``%, (, )`` and the space character, as well as a few
        other typically undesirable characters, are stripped early for a
        :func:`.bindparam` that is using an anonymized name, which is typically
        generated automatically from a named column which itself includes these
        characters in its name and does not use a ``.key``, so that they do not
        interfere either with the SQLAlchemy compiler's use of string formatting or
        with the driver-level parsing of the parameter, both of which could be
        demonstrated before the fix.  The change only applies to anonymized
        parameter names that are generated and consumed internally, not end-user
        defined names, so the change should have no impact on any existing code.
        Applies in particular to the psycopg2 driver which does not otherwise quote
        special parameter names, but also strips leading underscores to suit Oracle
        (but not yet leading numbers, as some anon parameters are currently
        entirely numeric/underscore based); Oracle in any case continues to quote
        parameter names that include special characters.

.. changelog::
    :version: 1.3.8
    :released: August 27, 2019

    .. change::
        :tags: bug, orm
        :tickets: 4823

        Fixed bug where :class:`.Load` objects were not pickleable due to
        mapper/relationship state in the internal context dictionary.  These
        objects are now converted to picklable using similar techniques as that of
        other elements within the loader option system that have long been
        serializable.

    .. change::
        :tags: bug, postgresql
        :tickets: 4623

        Revised the approach for the just added support for the psycopg2
        "execute_values()" feature added in 1.3.7 for :ticket:`4623`.  The approach
        relied upon a regular expression that would fail to match for a more
        complex INSERT statement such as one which had subqueries involved.   The
        new approach matches exactly the string that was rendered as the VALUES
        clause.

    .. change::
        :tags: usecase, orm
        :tickets: 4285

        Added support for the use of an :class:`.Enum` datatype using Python
        pep-435 enumeration objects as values for use as a primary key column
        mapped by the ORM.  As these values are not inherently sortable, as
        required by the ORM for primary keys, a new
        :attr:`.TypeEngine.sort_key_function` attribute is added to the typing
        system which allows any SQL type to  implement a sorting for Python objects
        of its type which is consulted by the unit of work.   The :class:`.Enum`
        type then defines this using the  database value of a given enumeration.
        The sorting scheme can be  also be redefined by passing a callable to the
        :paramref:`.Enum.sort_key_function` parameter.  Pull request courtesy
        Nicolas Caniart.

    .. change::
        :tags: bug, engine
        :tickets: 4807

        Fixed an issue whereby if the dialect "initialize" process which occurs on
        first connect would encounter an unexpected exception, the initialize
        process would fail to complete and then no longer attempt on subsequent
        connection attempts, leaving the dialect in an un-initialized, or partially
        initialized state, within the scope of parameters that need to be
        established based on inspection of a live connection.   The "invoke once"
        logic in the event system has been reworked to accommodate for this
        occurrence using new, private API features that establish an "exec once"
        hook that will continue to allow the initializer to fire off on subsequent
        connections, until it completes without raising an exception. This does not
        impact the behavior of the existing ``once=True`` flag within the event
        system.

    .. change::
        :tags: bug, sqlite, reflection
        :tickets: 4810

        Fixed bug where a FOREIGN KEY that was set up to refer to the parent table
        by table name only without the column names would not correctly be
        reflected as far as setting up the "referred columns", since SQLite's
        PRAGMA does not report on these columns if they weren't given explicitly.
        For some reason this was harcoded to assume the name of the local column,
        which might work for some cases but is not correct. The new approach
        reflects the primary key of the referred table and uses the constraint
        columns list as the referred columns list, if the remote column(s) aren't
        present in the reflected pragma directly.


    .. change::
        :tags: bug, postgresql
        :tickets: 4822

        Fixed bug where Postgresql operators such as
        :meth:`.postgresql.ARRAY.Comparator.contains` and
        :meth:`.postgresql.ARRAY.Comparator.contained_by` would fail to function
        correctly for non-integer values when used against a
        :class:`.postgresql.array` object, due to an erroneous assert statement.

    .. change::
        :tags: feature, engine
        :tickets: 4815

        Added new parameter :paramref:`.create_engine.hide_parameters` which when
        set to True will cause SQL parameters to no longer be logged, nor rendered
        in the string representation of a :class:`.StatementError` object.


    .. change::
        :tags: usecase, postgresql
        :tickets: 4824

        Added support for reflection of CHECK constraints that include the special
        PostgreSQL qualifier "NOT VALID", which can be present for CHECK
        constraints that were added to an exsiting table with the directive that
        they not be applied to existing data in the table. The PostgreSQL
        dictionary for CHECK constraints as returned by
        :meth:`.Inspector.get_check_constraints` may include an additional entry
        ``dialect_options`` which within will contain an entry ``"not_valid":
        True`` if this symbol is detected.   Pull request courtesy Bill Finn.

.. changelog::
    :version: 1.3.7
    :released: August 14, 2019

    .. change::
        :tags: bug, sql
        :tickets: 4778

        Fixed issue where :class:`.Index` object which contained a mixture of
        functional expressions which were not resolvable to a particular column,
        in combination with string-based column names, would fail to initialize
        its internal state correctly leading to failures during DDL compilation.

    .. change::
        :tags: bug, sqlite
        :tickets: 4798

        The dialects that support json are supposed to take arguments
        ``json_serializer`` and ``json_deserializer`` at the create_engine() level,
        however the SQLite dialect calls them ``_json_serilizer`` and
        ``_json_deserilalizer``.  The names have been corrected, the old names are
        accepted with a change warning, and these parameters are now documented as
        :paramref:`.create_engine.json_serializer` and
        :paramref:`.create_engine.json_deserializer`.


    .. change::
        :tags: bug, mysql
        :tickets: 4804

        The MySQL dialects will emit "SET NAMES" at the start of a connection when
        charset is given to the MySQL driver, to appease an apparent behavior
        observed in MySQL 8.0 that raises a collation error when a UNION includes
        string columns unioned against columns of the form CAST(NULL AS CHAR(..)),
        which is what SQLAlchemy's polymorphic_union function does.   The issue
        seems to have affected PyMySQL for at least a year, however has recently
        appeared as of mysqlclient 1.4.4 based on changes in how this DBAPI creates
        a connection.  As the presence of this directive impacts three separate
        MySQL charset settings which each have intricate effects based on their
        presense,  SQLAlchemy will now emit the directive on new connections to
        ensure correct behavior.

    .. change::
        :tags: usecase, postgresql
        :tickets: 4623

        Added new dialect flag for the psycopg2 dialect, ``executemany_mode`` which
        supersedes the previous experimental ``use_batch_mode`` flag.
        ``executemany_mode`` supports both the "execute batch" and "execute values"
        functions provided by psycopg2, the latter which is used for compiled
        :func:`.insert` constructs.   Pull request courtesy Yuval Dinari.

        .. seealso::

            :ref:`psycopg2_executemany_mode`




    .. change::
        :tags: bug, sql
        :tickets: 4787

        Fixed bug where :meth:`.TypeEngine.column_expression` method would not be
        applied to subsequent SELECT statements inside of a UNION or other
        :class:`.CompoundSelect`, even though the SELECT statements are rendered at
        the topmost level of the statement.   New logic now differentiates between
        rendering the column expression, which is needed for all SELECTs in the
        list, vs. gathering the returned data type for the result row, which is
        needed only for the first SELECT.

    .. change::
        :tags: bug, sqlite
        :tickets: 4793

        Fixed bug where usage of "PRAGMA table_info" in SQLite dialect meant that
        reflection features to detect for table existence, list of table columns,
        and list of foreign keys, would default to any table in any attached
        database, when no schema name was given and the table did not exist in the
        base schema.  The fix explicitly runs PRAGMA for the 'main' schema and then
        the 'temp' schema if the 'main' returned no rows, to maintain the behavior
        of tables + temp tables in the "no schema" namespace, attached tables only
        in the "schema" namespace.


    .. change::
        :tags: bug, sql
        :tickets: 4780

        Fixed issue where internal cloning of SELECT constructs could lead to a key
        error if the copy of the SELECT changed its state such that its list of
        columns changed.  This was observed to be occurring in some ORM scenarios
        which may be unique to 1.3 and above, so is partially a regression fix.



    .. change::
        :tags: bug, orm
        :tickets: 4777

        Fixed regression caused by new selectinload for many-to-one logic where
        a primaryjoin condition not based on real foreign keys would cause
        KeyError if a related object did not exist for a given key value on the
        parent object.

    .. change::
        :tags: usecase, mysql
        :tickets: 4783

        Added reserved words ARRAY and MEMBER to the MySQL reserved words list, as
        MySQL 8.0 has now made these reserved.


    .. change::
        :tags: bug, events
        :tickets: 4794

        Fixed issue in event system where using the ``once=True`` flag with
        dynamically generated listener functions would cause event registration of
        future events to fail if those listener functions were garbage collected
        after they were used, due to an assumption that a listened function is
        strongly referenced.  The "once" wrapped is now modified to strongly
        reference the inner function persistently, and documentation is updated
        that using "once" does not imply automatic de-registration of listener
        functions.

    .. change::
        :tags: bug, mysql
        :tickets: 4751

        Added another fix for an upstream MySQL 8 issue where a case sensitive
        table name is reported incorrectly in foreign key constraint reflection,
        this is an extension of the fix first added for :ticket:`4344` which
        affects a case sensitive column name.  The new issue occurs through MySQL
        8.0.17, so the general logic of the 88718 fix remains in place.

        .. seealso::

            https://bugs.mysql.com/bug.php?id=96365 - upstream bug


    .. change::
        :tags: usecase, mssql
        :tickets: 4782

        Added new :func:`.mssql.try_cast` construct for SQL Server which emits
        "TRY_CAST" syntax.  Pull request courtesy Leonel Atencio.

    .. change::
        :tags: bug, orm
        :tickets: 4803

        Fixed bug where using :meth:`.Query.first` or a slice expression in
        conjunction with a query that has an expression based "offset" applied
        would raise TypeError, due to an "or" conditional against "offset" that did
        not expect it to be a SQL expression as opposed to an integer or None.


.. changelog::
    :version: 1.3.6
    :released: July 21, 2019

    .. change::
        :tags: bug, engine
        :tickets: 4754

        Fixed bug where using reflection function such as :meth:`.MetaData.reflect`
        with an :class:`.Engine` object that had execution options applied to it
        would fail, as the resulting :class:`.OptionEngine` proxy object failed to
        include a ``.engine`` attribute used within the reflection routines.

    .. change::
        :tags: bug, mysql
        :tickets: 4743

        Fixed bug where the special logic to render "NULL" for the
        :class:`.TIMESTAMP` datatype when ``nullable=True`` would not work if the
        column's datatype were a :class:`.TypeDecorator` or a :class:`.Variant`.
        The logic now ensures that it unwraps down to the original
        :class:`.TIMESTAMP` so that this special case NULL keyword is correctly
        rendered when requested.

    .. change::
        :tags: performance, orm
        :tickets: 4775

        The optimization applied to selectin loading in :ticket:`4340` where a JOIN
        is not needed to eagerly load related items is now applied to many-to-one
        relationships as well, so that only the related table is queried for a
        simple join condition.   In this case, the related items are queried
        based on the value of a foreign key column on the parent; if these columns
        are deferred or otherwise not loaded on any of the parent objects in
        the collection, the loader falls back to the JOIN method.


    .. change::
        :tags: bug, orm
        :tickets: 4773

        Fixed regression caused by :ticket:`4365` where a join from an entity to
        itself without using aliases no longer raises an informative error message,
        instead failing on an assertion.  The informative error condition has been
        restored.


    .. change::
        :tags: orm, feature
        :tickets: 4736

        Added new loader option method :meth:`.Load.options` which allows loader
        options to be constructed hierarchically, so that many sub-options can be
        applied to a particular path without needing to call :func:`.defaultload`
        many times.  Thanks to Alessio Bogon for the idea.


    .. change::
        :tags: usecase, postgresql
        :tickets: 4771

        Added support for reflection of indexes on PostgreSQL partitioned tables,
        which was added to PostgreSQL as of version 11.

    .. change::
       :tags: bug, mysql
       :tickets: 4624

       Enhanced MySQL/MariaDB version string parsing to accommodate for exotic
       MariaDB version strings where the "MariaDB" word is embedded among other
       alphanumeric characters such as "MariaDBV1".   This detection is critical in
       order to correctly accommodate for API features that have split between MySQL
       and MariaDB such as the "transaction_isolation" system variable.


    .. change::
        :tags: bug, mssql
        :tickets: 4745

        Ensured that the queries used to reflect indexes and view definitions will
        explicitly CAST string parameters into NVARCHAR, as many SQL Server drivers
        frequently treat string values, particularly those with non-ascii
        characters or larger string values, as TEXT which often don't compare
        correctly against VARCHAR characters in SQL Server's information schema
        tables for some reason.    These CAST operations already take place for
        reflection queries against SQL Server ``information_schema.`` tables but
        were missing from three additional queries that are against ``sys.``
        tables.

    .. change::
        :tags: bug, orm
        :tickets: 4713

        Fixed an issue where the :meth:`.orm._ORMJoin.join` method, which is a
        not-internally-used ORM-level method that exposes what is normally an
        internal process of :meth:`.Query.join`, did not propagate the ``full`` and
        ``outerjoin`` keyword arguments correctly.  Pull request courtesy Denis
        Kataev.

    .. change::
        :tags: bug, sql
        :tickets: 4758

        Adjusted the initialization for :class:`.Enum` to minimize how often it
        invokes the ``.__members__`` attribute of a given PEP-435 enumeration
        object, to suit the case where this attribute is expensive to invoke, as is
        the case for some popular third party enumeration libraries.


    .. change::
        :tags: bug, orm
        :tickets: 4772

        Fixed bug where a many-to-one relationship that specified ``uselist=True``
        would fail to update correctly during a primary key change where a related
        column needs to change.


    .. change::
        :tags: bug, orm
        :tickets: 4772

        Fixed bug where the detection for many-to-one or one-to-one use with a
        "dynamic" relationship, which is an invalid configuration, would fail to
        raise if the relationship were configured with ``uselist=True``.  The
        current fix is that it warns, instead of raises, as this would otherwise be
        backwards incompatible, however in a future release it will be a raise.


    .. change::
        :tags: bug, orm
        :tickets: 4767

        Fixed bug where a synonym created against a mapped attribute that does not
        exist yet, as is the case when it refers to backref before mappers are
        configured, would raise recursion errors when trying to test for attributes
        on it which ultimately don't exist (as occurs when the classes are run
        through Sphinx autodoc), as the unconfigured state of the synonym would put
        it into an attribute not found loop.


    .. change::
        :tags: usecase, postgresql
        :tickets: 4756

        Added support for multidimensional Postgresql array literals via nesting
        the :class:`.postgresql.array` object within another one.  The
        multidimensional array type is detected automatically.

        .. seealso::

            :class:`.postgresql.array`

    .. change::
        :tags: bug, sql, postgresql
        :tickets: 4760

        Fixed issue where the :class:`.array_agg` construct in combination with
        :meth:`.FunctionElement.filter` would not produce the correct operator
        precedence in combination with the array index operator.


    .. change::
        :tags: bug, sql
        :tickets: 4747

        Fixed an unlikely issue where the "corresponding column" routine for unions
        and other :class:`.CompoundSelect` objects could return the wrong column in
        some overlapping column situtations, thus potentially impacting some ORM
        operations when set operations are in use, if the underlying
        :func:`.select` constructs were used previously in other similar kinds of
        routines, due to a cached value not being cleared.

    .. change::
        :tags: usecase, sqlite
        :tickets: 4766

        Added support for composite (tuple) IN operators with SQLite, by rendering
        the VALUES keyword for this backend.  As other backends such as DB2 are
        known to use the same syntax, the syntax is enabled in the base compiler
        using a dialect-level flag ``tuple_in_values``.   The change also includes
        support for "empty IN tuple" expressions for SQLite when using "in_()"
        between a tuple value and an empty set.


.. changelog::
    :version: 1.3.5
    :released: June 17, 2019

    .. change::
        :tags: bug, mysql
        :tickets: 4715

        Fixed bug where MySQL ON DUPLICATE KEY UPDATE would not accommodate setting
        a column to the value NULL.  Pull request courtesy Lukáš Banič.

    .. change::
        :tags: bug, orm
        :tickets: 4723

        Fixed a series of related bugs regarding joined table inheritance more than
        two levels deep, in conjunction with modification to primary key values,
        where those primary key columns are also linked together in a foreign key
        relationship as is typical for joined table inheritance.  The intermediary
        table in a  three-level inheritance hierarchy will now get its UPDATE if
        only the primary key value has changed and passive_updates=False (e.g.
        foreign key constraints not being enforced), whereas before it would be
        skipped; similarly, with passive_updates=True (e.g. ON UPDATE  CASCADE in
        effect), the third-level table will not receive an UPDATE statement as was
        the case earlier which would fail since CASCADE already modified it.   In a
        related issue, a relationship linked to a three-level inheritance hierarchy
        on the primary key of an intermediary table of a joined-inheritance
        hierarchy will also correctly have its foreign key column updated when the
        parent object's primary key is modified, even if that parent object is a
        subclass of the linked parent class, whereas before these classes would
        not be counted.

    .. change::
        :tags: bug, orm
        :tickets: 4729

        Fixed bug where the :attr:`.Mapper.all_orm_descriptors` accessor would
        return an entry for the :class:`.Mapper` itself under the declarative
        ``__mapper___`` key, when this is not a descriptor.  The ``.is_attribute``
        flag that's present on all :class:`.InspectionAttr` objects is now
        consulted, which has also been modified to be ``True`` for an association
        proxy, as it was erroneously set to False for this object.

    .. change::
        :tags: bug, orm
        :tickets: 4704

        Fixed regression in :meth:`.Query.join` where the ``aliased=True`` flag
        would not properly apply clause adaptation to filter criteria, if a
        previous join were made to the same entity.  This is because the adapters
        were placed in the wrong order.   The order has been reversed so that the
        adapter for the most recent ``aliased=True`` call takes precedence as was
        the case in 1.2 and earlier.  This broke the "elementtree" examples among
        other things.

    .. change::
        :tags: bug, orm, py3k
        :tickets: 4674

        Replaced the Python compatbility routines for ``getfullargspec()`` with a
        fully vendored version from Python 3.3.  Originally, Python was emitting
        deprecation warnings for this function in Python 3.8 alphas.  While this
        change was reverted, it was observed that Python 3 implementations for
        ``getfullargspec()`` are an order of magnitude slower as of the 3.4 series
        where it was rewritten against ``Signature``.  While Python plans to
        improve upon this situation, SQLAlchemy projects for now are using a simple
        replacement to avoid any future issues.

    .. change::
        :tags: bug, orm
        :tickets: 4694

        Reworked the attribute mechanics used by :class:`.AliasedClass` to no
        longer rely upon calling ``__getattribute__`` on the MRO of the wrapped
        class, and to instead resolve the attribute normally on the wrapped class
        using getattr(), and then unwrap/adapt that.  This allows a greater range
        of attribute styles on the mapped class including special ``__getattr__()``
        schemes; but it also makes the code simpler and more resilient in general.

    .. change::
        :tags: usecase, postgresql
        :tickets: 4717

        Added support for column sorting flags when reflecting indexes for
        PostgreSQL, including ASC, DESC, NULLSFIRST, NULLSLAST.  Also adds this
        facility to the reflection system in general which can be applied to other
        dialects in future releases.  Pull request courtesy Eli Collins.

    .. change::
        :tags: bug, postgresql
        :tickets: 4701

        Fixed bug where PostgreSQL dialect could not correctly reflect an ENUM
        datatype that has no members, returning a list with ``None`` for the
        ``get_enums()`` call and raising a TypeError when reflecting a column which
        has such a datatype.   The inspection now returns an empty list.

    .. change::
        :tags: bug, sql
        :tickets: 4730

        Fixed a series of quoting issues which all stemmed from the concept of the
        :func:`.literal_column` construct, which when being "proxied" through a
        subquery to be referred towards by a label that matches its text, the label
        would not have quoting rules applied to it, even if the string in the
        :class:`.Label` were set up as a :class:`.quoted_name` construct.  Not
        applying quoting to the text of the :class:`.Label` is a bug because this
        text is strictly a SQL identifier name and not a SQL expression, and the
        string should not have quotes embedded into it already unlike the
        :func:`.literal_column` which it may be applied towards.   The existing
        behavior of a non-labeled :func:`.literal_column` being propagated as is on
        the outside of a subquery is maintained in order to help with manual
        quoting schemes, although it's not clear if valid SQL can be generated for
        such a construct in any case.

.. changelog::
    :version: 1.3.4
    :released: May 27, 2019

    .. change::
        :tags: feature, mssql
        :tickets: 4657

        Added support for SQL Server filtered indexes, via the ``mssql_where``
        parameter which works similarly to that of the ``postgresql_where`` index
        function in the PostgreSQL dialect.

        .. seealso::

            :ref:`mssql_index_where`

    .. change::
       :tags: bug, misc
       :tickets: 4625

       Removed errant "sqla_nose.py" symbol from MANIFEST.in which created an
       undesirable warning message.

    .. change::
        :tags: bug, sql
        :tickets: 4653

        Fixed that the :class:`.GenericFunction` class was inadvertently
        registering itself as one of the named functions.  Pull request courtesy
        Adrien Berchet.

    .. change::
       :tags: bug, engine, postgresql
       :tickets: 4663

       Moved the "rollback" which occurs during dialect initialization so that it
       occurs after additional dialect-specific initialize steps, in particular
       those of the psycopg2 dialect which would inadvertently leave transactional
       state on the first new connection, which could interfere with some
       psycopg2-specific APIs which require that no transaction is started.  Pull
       request courtesy Matthew Wilkes.


    .. change::
        :tags: bug, orm
        :tickets: 4695

        Fixed issue where the :paramref:`.AttributeEvents.active_history` flag
        would not be set for an event listener that propgated to a subclass via the
        :paramref:`.AttributeEvents.propagate` flag.   This bug has been present
        for the full span of the :class:`.AttributeEvents` system.


    .. change::
        :tags: bug, orm
        :tickets: 4690

        Fixed regression where new association proxy system was still not proxying
        hybrid attributes when they made use of the ``@hybrid_property.expression``
        decorator to return an alternate SQL expression, or when the hybrid
        returned an arbitrary :class:`.PropComparator`, at the expression level.
        This involved further generalization of the heuristics used to detect the
        type of object being proxied at the level of :class:`.QueryableAttribute`,
        to better detect if the descriptor ultimately serves mapped classes or
        column expressions.

    .. change::
        :tags: bug, orm
        :tickets: 4686

        Applied the mapper "configure mutex" against the declarative class mapping
        process, to guard against the race which can occur if mappers are used
        while dynamic module import schemes are still in the process of configuring
        mappers for related classes.  This does not guard against all possible race
        conditions, such as if the concurrent import has not yet encountered the
        dependent classes as of yet, however it guards against as much as possible
        within the SQLAlchemy declarative process.

    .. change::
        :tags: bug, mssql
        :tickets: 4680

        Added error code 20047 to "is_disconnect" for pymssql.  Pull request
        courtesy Jon Schuff.


    .. change::
       :tags: bug, postgresql, orm
       :tickets: 4661

       Fixed an issue where the "number of rows matched" warning would emit even if
       the dialect reported "supports_sane_multi_rowcount=False", as is the case
       for psycogp2 with ``use_batch_mode=True`` and others.


    .. change::
        :tags: bug, sql
        :tickets: 4618

        Fixed issue where double negation of a boolean column wouldn't reset
        the "NOT" operator.

    .. change::
        :tags: mysql, bug
        :tickets: 4650

        Added support for DROP CHECK constraint which is required by MySQL 8.0.16
        to drop a CHECK constraint; MariaDB supports plain DROP CONSTRAINT.  The
        logic distinguishes between the two syntaxes by checking the server version
        string for MariaDB presence.    Alembic migrations has already worked
        around this issue by implementing its own DROP for MySQL / MariaDB CHECK
        constraints, however this change implements it straight in Core so that its
        available for general use.   Pull request courtesy Hannes Hansen.

    .. change::
       :tags: bug, orm
       :tickets: 4647

       A warning is now emitted for the case where a transient object is being
       merged into the session with :meth:`.Session.merge` when that object is
       already transient in the :class:`.Session`.   This warns for the case where
       the object would normally be double-inserted.


    .. change::
        :tags: bug, orm
        :tickets: 4676

        Fixed regression in new relationship m2o comparison logic first introduced
        at :ref:`change_4359` when comparing to an attribute that is persisted as
        NULL and is in an un-fetched state in the mapped instance.  Since the
        attribute has no explicit default, it needs to default to NULL when
        accessed in a persistent setting.


    .. change::
        :tags: bug, sql
        :tickets: 4569

        The :class:`.GenericFunction` namespace is being migrated so that function
        names are looked up in a case-insensitive manner, as SQL  functions do not
        collide on case sensitive differences nor is this something which would
        occur with user-defined functions or stored procedures.   Lookups for
        functions declared with :class:`.GenericFunction` now use a case
        insensitive scheme,  however a deprecation case is supported which allows
        two or more :class:`.GenericFunction` objects with the same name of
        different cases to exist, which will cause case sensitive lookups to occur
        for that particular name, while emitting a warning at function registration
        time.  Thanks to Adrien Berchet for a lot of work on this complicated
        feature.


.. changelog::
    :version: 1.3.3
    :released: April 15, 2019

    .. change::
        :tags: bug, postgresql
        :tickets: 4601

        Fixed regression from release 1.3.2 caused by :ticket:`4562` where a URL
        that contained only a query string and no hostname, such as for the
        purposes of specifying a service file with connection information, would no
        longer be propagated to psycopg2 properly.   The change in :ticket:`4562`
        has been adjusted to further suit psycopg2's exact requirements, which is
        that if there are any connection parameters whatsoever, the "dsn" parameter
        is no longer required, so in this case the query string parameters are
        passed alone.

    .. change::
       :tags: bug, pool
       :tickets: 4585

       Fixed behavioral regression as a result of deprecating the "use_threadlocal"
       flag for :class:`.Pool`, where the :class:`.SingletonThreadPool` no longer
       makes use of this option which causes the "rollback on return" logic to take
       place when the same :class:`.Engine` is used multiple times in the context
       of a transaction to connect or implicitly execute, thereby cancelling the
       transaction.   While this is not the recommended way to work with engines
       and connections, it is nonetheless a confusing behavioral change as when
       using :class:`.SingletonThreadPool`, the transaction should stay open
       regardless of what else is done with the same engine in the same thread.
       The ``use_threadlocal`` flag remains deprecated however the
       :class:`.SingletonThreadPool` now implements its own version of the same
       logic.


    .. change::
       :tags: bug, orm
       :tickets: 4584

       Fixed 1.3 regression in new "ambiguous FROMs" query logic introduced in
       :ref:`change_4365` where a :class:`.Query` that explicitly places an entity
       in the FROM clause with :meth:`.Query.select_from` and also joins to it
       using :meth:`.Query.join` would later cause an "ambiguous FROM" error if
       that entity were used in additional joins, as the entity appears twice in
       the "from" list of the :class:`.Query`.  The fix resolves this ambiguity by
       folding the standalone entity into the join that it's already a part of in
       the same way that ultimately happens when the SELECT statement is rendered.

    .. change::
        :tags: bug, ext
        :tickets: 4603

        Fixed bug where using ``copy.copy()`` or ``copy.deepcopy()`` on
        :class:`.MutableList` would cause the items within the list to be
        duplicated, due to an inconsistency in how Python pickle and copy both make
        use of ``__getstate__()`` and ``__setstate__()`` regarding lists.  In order
        to resolve, a ``__reduce_ex__`` method had to be added to
        :class:`.MutableList`.  In order to maintain backwards compatibility with
        existing pickles based on ``__getstate__()``, the ``__setstate__()`` method
        remains as well; the test suite asserts that pickles made against the old
        version of the class can still be deserialized by the pickle module.

    .. change::
       :tags: bug, orm
       :tickets: 4606

       Adjusted the :meth:`.Query.filter_by` method to not call :func:`.and()`
       internally against multiple criteria, instead passing it off to
       :meth:`.Query.filter` as a series of criteria, instead of a single criteria.
       This allows :meth:`.Query.filter_by` to defer to :meth:`.Query.filter`'s
       treatment of variable numbers of clauses, including the case where the list
       is empty.  In this case, the :class:`.Query` object will not have a
       ``.whereclause``, which allows subsequent "no whereclause" methods like
       :meth:`.Query.select_from` to behave consistently.

    .. change::
       :tags: bug, mssql
       :tickets: 4587

       Fixed issue in SQL Server dialect where if a bound parameter were present in
       an ORDER BY expression that would ultimately not be rendered in the SQL
       Server version of the statement, the parameters would still be part of the
       execution parameters, leading to DBAPI-level errors.  Pull request courtesy
       Matt Lewellyn.

.. changelog::
    :version: 1.3.2
    :released: April 2, 2019

    .. change::
       :tags: bug, documentation, sql
       :tickets: 4580

       Thanks to :ref:`change_3981`, we no longer need to rely on recipes that
       subclass dialect-specific types directly, :class:`.TypeDecorator` can now
       handle all cases.   Additionally, the above change made it slightly less
       likely that a direct subclass of a base SQLAlchemy type would work as
       expected, which could be misleading.  Documentation has been updated to use
       :class:`.TypeDecorator` for these examples including the PostgreSQL
       "ArrayOfEnum" example datatype and direct support for the "subclass a type
       directly" has been removed.

    .. change::
       :tags: bug, postgresql
       :tickets: 4550

       Modified the :paramref:`.Select.with_for_update.of` parameter so that if a
       join or other composed selectable is passed, the individual :class:`.Table`
       objects will be filtered from it, allowing one to pass a join() object to
       the parameter, as occurs normally when using joined table inheritance with
       the ORM.  Pull request courtesy Raymond Lu.


    .. change::
        :tags: feature, postgresql
        :tickets: 4562

        Added support for parameter-less connection URLs for the psycopg2 dialect,
        meaning, the URL can be passed to :func:`.create_engine` as
        ``"postgresql+psycopg2://"`` with no additional arguments to indicate an
        empty DSN passed to libpq, which indicates to connect to "localhost" with
        no username, password, or database given. Pull request courtesy Julian
        Mehnle.

    .. change::
       :tags: bug, orm, ext
       :tickets: 4574, 4573

       Restored instance-level support for plain Python descriptors, e.g.
       ``@property`` objects, in conjunction with association proxies, in that if
       the proxied object is not within ORM scope at all, it gets classified as
       "ambiguous" but is proxed directly.  For class level access, a basic class
       level``__get__()`` now returns the
       :class:`.AmbiguousAssociationProxyInstance` directly, rather than raising
       its exception, which is the closest approximation to the previous behavior
       that returned the :class:`.AssociationProxy` itself that's possible.  Also
       improved the stringification of these objects to be more descriptive of
       current state.

    .. change::
       :tags: bug, orm
       :tickets: 4537

       Fixed bug where use of :func:`.with_polymorphic` or other aliased construct
       would not properly adapt when the aliased target were used as the
       :meth:`.Select.correlate_except` target of a subquery used inside of a
       :func:`.column_property`. This required a fix to the clause adaption
       mechanics to properly handle a selectable that shows up in the "correlate
       except" list, in a similar manner as which occurs for selectables that show
       up in the "correlate" list.  This is ultimately a fairly fundamental bug
       that has lasted for a long time but it is hard to come across it.


    .. change::
       :tags: bug, orm
       :tickets: 4566

       Fixed regression where a new error message that was supposed to raise when
       attempting to link a relationship option to an AliasedClass without using
       :meth:`.PropComparator.of_type` would instead raise an ``AttributeError``.
       Note that in 1.3, it is no longer valid to create an option path from a
       plain mapper relationship to an :class:`.AliasedClass` without using
       :meth:`.PropComparator.of_type`.

.. changelog::
    :version: 1.3.1
    :released: March 9, 2019

    .. change::
       :tags: bug, mssql
       :tickets: 4525

       Fixed regression in SQL Server reflection due to :ticket:`4393` where the
       removal of open-ended ``**kw`` from the :class:`.Float` datatype caused
       reflection of this type to fail due to a "scale" argument being passed.

    .. change::
       :tags: bug, orm, ext
       :tickets: 4522

       Fixed regression where an association proxy linked to a synonym would no
       longer work, both at instance level and at class level.

.. changelog::
    :version: 1.3.0
    :released: March 4, 2019

    .. change::
       :tags: feature, schema
       :tickets: 4517

       Added new parameters :paramref:`.Table.resolve_fks` and
       :paramref:`.MetaData.reflect.resolve_fks` which when set to False will
       disable the automatic reflection of related tables encountered in
       :class:`.ForeignKey` objects, which can both reduce SQL overhead for omitted
       tables as well as avoid tables that can't be reflected for database-specific
       reasons.  Two :class:`.Table` objects present in the same :class:`.MetaData`
       collection can still refer to each other even if the reflection of the two
       tables occurred separately.


    .. change::
       :tags: feature, orm
       :tickets: 4316

       The :meth:`.Query.get` method can now accept a dictionary of attribute keys
       and values as a means of indicating the primary key value to load; is
       particularly useful for composite primary keys.  Pull request courtesy
       Sanjana S.

    .. change::
       :tags: feature, orm
       :tickets: 3133

       A SQL expression can now be assigned to a primary key attribute for an ORM
       flush in the same manner as ordinary attributes as described in
       :ref:`flush_embedded_sql_expressions` where the expression will be evaulated
       and then returned to the ORM using RETURNING, or in the case of pysqlite,
       works using the cursor.lastrowid attribute.Requires either a database that
       supports RETURNING (e.g. Postgresql, Oracle, SQL Server) or pysqlite.

    .. change::
       :tags: bug, sql
       :tickets: 4509

       The :class:`.Alias` class and related subclasses :class:`.CTE`,
       :class:`.Lateral` and :class:`.TableSample` have been reworked so that it is
       not possible for a user to construct the objects directly.  These constructs
       require that the standalone construction function or selectable-bound method
       be used to instantiate new objects.


    .. change::
       :tags: feature, engine
       :tickets: 4500

       Revised the formatting for :class:`.StatementError` when stringified. Each
       error detail is broken up over multiple newlines instead of spaced out on a
       single line.  Additionally, the SQL representation now stringifies the SQL
       statement rather than using ``repr()``, so that newlines are rendered as is.
       Pull request courtesy Nate Clark.

       .. seealso::

            :ref:`change_4500`

.. changelog::
    :version: 1.3.0b3
    :released: March 4, 2019
    :released: February 8, 2019

    .. change::
       :tags: bug, ext
       :tickets: 2642

       Implemented a more comprehensive assignment operation (e.g. "bulk replace")
       when using association proxy with sets or dictionaries.  Fixes the problem
       of redundant proxy objects being created to replace the old ones, which
       leads to excessive events and SQL and in the case of unique constraints
       will cause the flush to fail.

       .. seealso::

          :ref:`change_2642`

    .. change::
        :tags: bug, postgresql
        :tickets: 4473

        Fixed issue where using an uppercase name for an index type (e.g. GIST,
        BTREE, etc. ) or an EXCLUDE constraint would treat it as an identifier to
        be quoted, rather than rendering it as is. The new behavior converts these
        types to lowercase and ensures they contain only valid SQL characters.

    .. change::
       :tags: bug, orm
       :tickets: 4469

       Improved the behavior of :func:`.orm.with_polymorphic` in conjunction with
       loader options, in particular wildcard operations as well as
       :func:`.orm.load_only`.  The polymorphic object will be more accurately
       targeted so that column-level options on the entity will correctly take
       effect.The issue is a continuation of the same kinds of things fixed in
       :ticket:`4468`.


    .. change::
       :tags: bug, sql
       :tickets: 4481

       Fully removed the behavior of strings passed directly as components of a
       :func:`.select` or :class:`.Query` object being coerced to :func:`.text`
       constructs automatically; the warning that has been emitted is now an
       ArgumentError or in the case of order_by() / group_by() a CompileError.
       This has emitted a warning since version 1.0 however its presence continues
       to create concerns for the potential of mis-use of this behavior.

       Note that public CVEs have been posted for order_by() / group_by() which
       are resolved by this commit:  CVE-2019-7164  CVE-2019-7548


       .. seealso::

        :ref:`change_4481`

    .. change::
       :tags: bug, sql
       :tickets: 4467

       Quoting is applied to :class:`.Function` names, those which are usually but
       not necessarily generated from the :attr:`.sql.func` construct,  at compile
       time if they contain illegal characters, such as spaces or punctuation. The
       names are as before treated as case insensitive however, meaning if the
       names contain uppercase or mixed case characters, that alone does not
       trigger quoting. The case insensitivity is currently maintained for
       backwards compatibility.


    .. change::
       :tags: bug, sql
       :tickets: 4481

       Added "SQL phrase validation" to key DDL phrases that are accepted as plain
       strings, including :paramref:`.ForeignKeyConstraint.on_delete`,
       :paramref:`.ForeignKeyConstraint.on_update`,
       :paramref:`.ExcludeConstraint.using`,
       :paramref:`.ForeignKeyConstraint.initially`, for areas where a series of SQL
       keywords only are expected.Any non-space characters that suggest the phrase
       would need to be quoted will raise a :class:`.CompileError`.   This change
       is related to the series of changes committed as part of :ticket:`4481`.

    .. change::
       :tags: bug, orm, declarative
       :tickets: 4470

       Added some helper exceptions that invoke when a mapping based on
       :class:`.AbstractConcreteBase`, :class:`.DeferredReflection`, or
       :class:`.AutoMap` is used before the mapping is ready to be used, which
       contain descriptive information on the class, rather than falling through
       into other failure modes that are less informative.


    .. change::
       :tags: change, tests
       :tickets: 4460

       The test system has removed support for Nose, which is unmaintained for
       several years and is producing warnings under Python 3. The test suite is
       currently standardized on Pytest.  Pull request courtesy Parth Shandilya.

.. changelog::
    :version: 1.3.0b2
    :released: March 4, 2019
    :released: January 25, 2019

    .. change::
       :tags: bug, ext
       :tickets: 4401

       Fixed a regression in 1.3.0b1 caused by :ticket:`3423` where association
       proxy objects that access an attribute that's only present on a polymorphic
       subclass would raise an ``AttributeError`` even though the actual instance
       being accessed was an instance of that subclass.

    .. change::
        :tags: bug, orm
        :tickets: 1103

        Fixed long-standing issue where duplicate collection members would cause a
        backref to delete the association between the member and its parent object
        when one of the duplicates were removed, as occurs as a side effect of
        swapping two objects in one statement.

        .. seealso::

            :ref:`change_1103`

    .. change::
       :tags: bug, mssql
       :tickets: 4442

       The ``literal_processor`` for the :class:`.Unicode` and
       :class:`.UnicodeText` datatypes now render an ``N`` character in front of
       the literal string expression as required by SQL Server for Unicode string
       values rendered in SQL expressions.

    .. change::
       :tags: feature, orm
       :tickets: 4423

       Implemented a new feature whereby the :class:`.AliasedClass` construct can
       now be used as the target of a :func:`.relationship`.  This allows the
       concept of "non primary mappers" to no longer be necessary, as the
       :class:`.AliasedClass` is much easier to configure and automatically inherits
       all the relationships of the mapped class, as well as preserves the
       ability for loader options to work normally.

       .. seealso::

            :ref:`change_4423`

    .. change::
       :tags: bug, orm
       :tickets: 4373

       Extended the fix first made as part of :ticket:`3287`, where a loader option
       made against a subclass using a wildcard would extend itself to include
       application of the wildcard to attributes on the super classes as well, to a
       "bound" loader option as well, e.g. in an expression like
       ``Load(SomeSubClass).load_only('foo')``.  Columns that are part of the
       parent class of ``SomeSubClass`` will also be excluded in the same way as if
       the unbound option ``load_only('foo')`` were used.

    .. change::
       :tags: bug, orm
       :tickets: 4433

       Improved error messages emitted by the ORM in the area of loader option
       traversal.  This includes early detection of mis-matched loader strategies
       along with a clearer explanation why these strategies don't match.


    .. change::
       :tags: change, orm
       :tickets: 4412

       Added a new function :func:`.close_all_sessions` which takes
       over the task of the :meth:`.Session.close_all` method, which
       is now deprecated as this is confusing as a classmethod.
       Pull request courtesy Augustin Trancart.

    .. change::
       :tags: feature, orm
       :tickets: 4397

       Added new :meth:`.MapperEvents.before_mapper_configured` event.   This
       event complements the other "configure" stage mapper events with a per
       mapper event that receives each :class:`.Mapper` right before its
       configure step, and additionally may be used to prevent or delay the
       configuration of specific :class:`.Mapper` objects using a new
       return value :attr:`.orm.interfaces.EXT_SKIP`.  See the
       documentation link for an example.

       .. seealso::

          :meth:`.MapperEvents.before_mapper_configured`



    .. change::
       :tags: bug, orm

       The "remove" event for collections is now called before the item is removed
       in the case of the ``collection.remove()`` method, as is consistent with the
       behavior for most other forms of collection item removal (such as
       ``__delitem__``, replacement under ``__setitem__``).  For ``pop()`` methods,
       the remove event still fires after the operation.

    .. change::
        :tags: bug, orm declarative
        :tickets: 4372

       Added a ``__clause_element__()`` method to :class:`.ColumnProperty` which
       can allow the usage of a not-fully-declared column or deferred attribute in
       a declarative mapped class slightly more friendly when it's used in a
       constraint or other column-oriented scenario within the class declaration,
       though this still can't work in open-ended expressions; prefer to call the
       :attr:`.ColumnProperty.expression` attribute if receiving ``TypeError``.

    .. change::
       :tags: bug, orm, engine
       :tickets: 4464

       Added accessors for execution options to Core and ORM, via
       :meth:`.Query.get_execution_options`,
       :meth:`.Connection.get_execution_options`,
       :meth:`.Engine.get_execution_options`, and
       :meth:`.Executable.get_execution_options`.  PR courtesy Daniel Lister.

    .. change::
       :tags: bug, orm
       :tickets: 4446

       Fixed issue in association proxy due to :ticket:`3423` which caused the use
       of custom :class:`.PropComparator` objects with hybrid attributes, such as
       the one demonstrated in  the ``dictlike-polymorphic`` example to not
       function within an association proxy.  The strictness that was added in
       :ticket:`3423` has been relaxed, and additional logic to accommodate for
       an association proxy that links to a custom hybrid have been added.

    .. change::
       :tags: change, general
       :tickets: 4393

       A large change throughout the library has ensured that all objects,
       parameters, and behaviors which have been noted as deprecated or legacy now
       emit ``DeprecationWarning`` warnings when invoked.As the Python 3
       interpreter now defaults to displaying deprecation warnings, as well as that
       modern test suites based on tools like tox and pytest tend to display
       deprecation warnings, this change should make it easier to note what API
       features are obsolete. A major rationale for this change is so that long-
       deprecated features that nonetheless still see continue to see real world
       use can  finally be removed in the near future; the biggest example of this
       are the :class:`.SessionExtension` and :class:`.MapperExtension` classes as
       well as a handful of other pre-event extension hooks, which have been
       deprecated since version 0.7 but still remain in the library.  Another is
       that several major longstanding behaviors are to be deprecated as well,
       including the threadlocal engine strategy, the convert_unicode flag, and non
       primary mappers.

       .. seealso::

          :ref:`change_4393_general`


    .. change::
       :tags: change, engine
       :tickets: 4393

       The "threadlocal" engine strategy which has been a legacy feature of
       SQLAlchemy since around version 0.2 is now deprecated, along with the
       :paramref:`.Pool.threadlocal` parameter of :class:`.Pool` which has no
       effect in most modern use cases.

       .. seealso::

          :ref:`change_4393_threadlocal`

    .. change::
       :tags: change, sql
       :tickets: 4393

       The :paramref:`.create_engine.convert_unicode` and
       :paramref:`.String.convert_unicode` parameters have been deprecated.  These
       parameters were built back when most Python DBAPIs had little to no support
       for Python Unicode objects, and SQLAlchemy needed to take on the very
       complex task of marshalling data and SQL strings between Unicode and
       bytestrings throughout the system in a performant way.  Thanks to Python 3,
       DBAPIs were compelled to adapt to Unicode-aware APIs and today all DBAPIs
       supported by SQLAlchemy support Unicode natively, including on Python 2,
       allowing this long-lived and very complicated feature to finally be (mostly)
       removed.  There are still of course a few Python 2 edge cases where
       SQLAlchemy has to deal with Unicode however these are handled automatically;
       in modern use, there should be no need for end-user interaction with these
       flags.

       .. seealso::

          :ref:`change_4393_convertunicode`

    .. change::
       :tags: bug, orm
       :tickets: 3777

       Implemented the ``.get_history()`` method, which also implies availability
       of :attr:`.AttributeState.history`, for :func:`.synonym` attributes.
       Previously, trying to access attribute history via a synonym would raise an
       ``AttributeError``.

    .. change::
       :tags: feature, engine
       :tickets: 3689

       Added public accessor :meth:`.QueuePool.timeout` that returns the configured
       timeout for a :class:`.QueuePool` object.  Pull request courtesy Irina Delamare.

    .. change::
       :tags: feature, sql
       :tickets: 4386

       Amended the :class:`.AnsiFunction` class, the base of common SQL
       functions like ``CURRENT_TIMESTAMP``, to accept positional arguments
       like a regular ad-hoc function.  This to suit the case that many of
       these functions on specific backends accept arguments such as
       "fractional seconds" precision and such.  If the function is created
       with arguments, it renders the parenthesis and the arguments.  If
       no arguments are present, the compiler generates the non-parenthesized form.

.. changelog::
    :version: 1.3.0b1
    :released: March 4, 2019
    :released: November 16, 2018

    .. change::
       :tags: bug, ext
       :tickets: 3423

       Reworked :class:`.AssociationProxy` to store state that's specific to a
       parent class in a separate object, so that a single
       :class:`.AssociationProxy` can serve for multiple parent classes, as is
       intrinsic to inheritance, without any ambiguity in the state returned by it.
       A new method :meth:`.AssociationProxy.for_class` is added to allow
       inspection of class-specific state.

       .. seealso::

            :ref:`change_3423`


    .. change::
       :tags: bug, oracle
       :tickets: 4369

       Updated the parameters that can be sent to the cx_Oracle DBAPI to both allow
       for all current parameters as well as for future parameters not added yet.
       In addition, removed unused parameters that were deprecated in version 1.2,
       and additionally we are now defaulting "threaded" to False.

       .. seealso::

          :ref:`change_4369`

    .. change::
        :tags: bug, oracle
        :tickets: 4242

        The Oracle dialect will no longer use the NCHAR/NCLOB datatypes
        represent generic unicode strings or clob fields in conjunction with
        :class:`.Unicode` and :class:`.UnicodeText` unless the flag
        ``use_nchar_for_unicode=True`` is passed to :func:`.create_engine` -
        this includes CREATE TABLE behavior as well as ``setinputsizes()`` for
        bound parameters.   On the read side, automatic Unicode conversion under
        Python 2 has been added to CHAR/VARCHAR/CLOB result rows, to match the
        behavior of cx_Oracle under Python 3.  In order to mitigate the performance
        hit under Python 2, SQLAlchemy's very performant (when C extensions
        are built) native Unicode handlers are used under Python 2.

        .. seealso::

            :ref:`change_4242`

    .. change::
        :tags: bug, orm
        :tickets: 3844

        Fixed issue regarding passive_deletes="all", where the foreign key
        attribute of an object is maintained with its value even after the object
        is removed from its parent collection.  Previously, the unit of work would
        set this to NULL even though passive_deletes indicated it should not be
        modified.

        .. seealso::

            :ref:`change_3844`

    .. change::
        :tags: bug, ext
        :tickets: 4268

        The long-standing behavior of the association proxy collection maintaining
        only a weak reference to the parent object is reverted; the proxy will now
        maintain a strong reference to the parent for as long as the proxy
        collection itself is also in memory, eliminating the "stale association
        proxy" error. This change is being made on an experimental basis to see if
        any use cases arise where it causes side effects.

        .. seealso::

            :ref:`change_4268`


    .. change::
        :tags: bug, sql
        :tickets: 4302

        Added "like" based operators as "comparison" operators, including
        :meth:`.ColumnOperators.startswith` :meth:`.ColumnOperators.endswith`
        :meth:`.ColumnOperators.ilike` :meth:`.ColumnOperators.notilike` among many
        others, so that all of these operators can be the basis for an ORM
        "primaryjoin" condition.


    .. change::
        :tags: feature, sqlite
        :tickets: 3850

        Added support for SQLite's json functionality via the new
        SQLite implementation for :class:`.types.JSON`, :class:`.sqlite.JSON`.
        The name used for the type is ``JSON``, following an example found at
        SQLite's own documentation. Pull request courtesy Ilja Everilä.

        .. seealso::

            :ref:`change_3850`

    .. change::
       :tags: feature, engine

       Added new "lifo" mode to :class:`.QueuePool`, typically enabled by setting
       the flag :paramref:`.create_engine.pool_use_lifo` to True.   "lifo" mode
       means the same connection just checked in will be the first to be checked
       out again, allowing excess connections to be cleaned up from the server
       side during periods of the pool being only partially utilized.  Pull request
       courtesy Taem Park.

       .. seealso::

          :ref:`change_pr467`

    .. change::
       :tags: bug, orm
       :tickets: 4359

       Improved the behavior of a relationship-bound many-to-one object expression
       such that the retrieval of column values on the related object are now
       resilient against the object being detached from its parent
       :class:`.Session`, even if the attribute has been expired.  New features
       within the :class:`.InstanceState` are used to memoize the last known value
       of a particular column attribute before its expired, so that the expression
       can still evaluate when the object is detached and expired at the same
       time.  Error conditions are also improved using modern attribute state
       features to produce more specific messages as needed.

       .. seealso::

            :ref:`change_4359`

    .. change::
        :tags: feature, mysql
        :tickets: 4219

        Support added for the "WITH PARSER" syntax of CREATE FULLTEXT INDEX
        in MySQL, using the ``mysql_with_parser`` keyword argument.  Reflection
        is also supported, which accommodates MySQL's special comment format
        for reporting on this option as well.  Additionally, the "FULLTEXT" and
        "SPATIAL" index prefixes are now reflected back into the ``mysql_prefix``
        index option.



    .. change::
        :tags: bug, orm, mysql, postgresql
        :tickets: 4246

        The ORM now doubles the "FOR UPDATE" clause within the subquery that
        renders in conjunction with joined eager loading in some cases, as it has
        been observed that MySQL does not lock the rows from a subquery.   This
        means the query renders with two FOR UPDATE clauses; note that on some
        backends such as Oracle, FOR UPDATE clauses on subqueries are silently
        ignored since they are unnecessary.  Additionally, in the case of the "OF"
        clause used primarily with PostgreSQL, the FOR UPDATE is rendered only on
        the inner subquery when this is used so that the selectable can be targeted
        to the table within the SELECT statement.

        .. seealso::

            :ref:`change_4246`

    .. change::
        :tags: feature, mssql
        :tickets: 4158

        Added ``fast_executemany=True`` parameter to the SQL Server pyodbc dialect,
        which enables use of pyodbc's new performance feature of the same name
        when using Microsoft ODBC drivers.

        .. seealso::

            :ref:`change_4158`

    .. change::
        :tags: bug, ext
        :tickets: 4308

        Fixed multiple issues regarding de-association of scalar objects with the
        association proxy.  ``del`` now works, and additionally a new flag
        :paramref:`.AssociationProxy.cascade_scalar_deletes` is added, which when
        set to True indicates that setting a scalar attribute to ``None`` or
        deleting via ``del`` will also set the source association to ``None``.

        .. seealso::

            :ref:`change_4308`


    .. change::
        :tags: feature, ext
        :tickets: 4318

        Added new feature :meth:`.BakedQuery.to_query`, which allows for a
        clean way of using one :class:`.BakedQuery` as a subquery inside of another
        :class:`.BakedQuery` without needing to refer explicitly to a
        :class:`.Session`.


    .. change::
       :tags: feature, sqlite
       :tickets: 4360

       Implemented the SQLite ``ON CONFLICT`` clause as understood at the DDL
       level, e.g. for primary key, unique, and CHECK constraints as well as
       specified on a :class:`.Column` to satisfy inline primary key and NOT NULL.
       Pull request courtesy Denis Kataev.

       .. seealso::

          :ref:`change_4360`

    .. change::
       :tags: feature, postgresql
       :tickets: 4237

       Added rudimental support for reflection of PostgreSQL
       partitioned tables, e.g. that relkind='p' is added to reflection
       queries that return table information.

       .. seealso::

            :ref:`change_4237`

    .. change::
       :tags: feature, ext
       :tickets: 4351

       The :class:`.AssociationProxy` now has standard column comparison operations
       such as :meth:`.ColumnOperators.like` and
       :meth:`.ColumnOperators.startswith` available when the target attribute is a
       plain column - the EXISTS expression that joins to the target table is
       rendered as usual, but the column expression is then use within the WHERE
       criteria of the EXISTS.  Note that this alters the behavior of the
       ``.contains()`` method on the association proxy to make use of
       :meth:`.ColumnOperators.contains` when used on a column-based attribute.

       .. seealso::

          :ref:`change_4351`


    .. change::
        :tags: feature, orm

        Added new flag :paramref:`.Session.bulk_save_objects.preserve_order` to the
        :meth:`.Session.bulk_save_objects` method, which defaults to True. When set
        to False, the given mappings will be grouped into inserts and updates per
        each object type, to allow for greater opportunities to batch common
        operations together.  Pull request courtesy Alessandro Cucci.

    .. change::
        :tags: bug, orm
        :tickets: 4365

        Refactored :meth:`.Query.join` to further clarify the individual components
        of structuring the join. This refactor adds the ability for
        :meth:`.Query.join` to determine the most appropriate "left" side of the
        join when there is more than one element in the FROM list or the query is
        against multiple entities.  If more than one FROM/entity matches, an error
        is raised that asks for an ON clause to be specified to resolve the
        ambiguity.  In particular this targets the regression we saw in
        :ticket:`4363` but is also of general use.   The codepaths within
        :meth:`.Query.join` are now easier to follow and the error cases are
        decided more specifically at an earlier point in the operation.

        .. seealso::

            :ref:`change_4365`

    .. change::
        :tags: bug, sql
        :tickets: 3981

        Fixed issue with :meth:`.TypeEngine.bind_expression` and
        :meth:`.TypeEngine.column_expression` methods where these methods would not
        work if the target type were part of a :class:`.Variant`, or other target
        type of a :class:`.TypeDecorator`.  Additionally, the SQL compiler now
        calls upon the dialect-level implementation when it renders these methods
        so that dialects can now provide for SQL-level processing for built-in
        types.

        .. seealso::

            :ref:`change_3981`


    .. change::
        :tags: bug, orm
        :tickets: 4304

        Fixed long-standing issue in :class:`.Query` where a scalar subquery such
        as produced by :meth:`.Query.exists`, :meth:`.Query.as_scalar` and other
        derivations from :attr:`.Query.statement` would not correctly be adapted
        when used in a new :class:`.Query` that required entity adaptation, such as
        when the query were turned into a union, or a from_self(), etc. The change
        removes the "no adaptation" annotation from the :func:`.select` object
        produced by the :attr:`.Query.statement` accessor.

    .. change::
        :tags: bug, orm, declarative
        :tickets: 4133

        Fixed bug where declarative would not update the state of the
        :class:`.Mapper` as far as what attributes were present, when additional
        attributes were added or removed after the mapper attribute collections had
        already been called and memoized.  Additionally, a ``NotImplementedError``
        is now raised if a fully mapped attribute (e.g. column, relationship, etc.)
        is deleted from a class that is currently mapped, since the mapper will not
        function correctly if the attribute has been removed.

    .. change::
       :tags: bug, mssql
       :tickets: 4362

       Deprecated the use of :class:`.Sequence` with SQL Server in order to affect
       the "start" and "increment" of the IDENTITY value, in favor of new
       parameters ``mssql_identity_start`` and ``mssql_identity_increment`` which
       set these parameters directly.  :class:`.Sequence` will be used to generate
       real ``CREATE SEQUENCE`` DDL with SQL Server in a future release.

       .. seealso::

            :ref:`change_4362`


    .. change::
        :tags: feature, mysql

        Added support for the parameters in an ON DUPLICATE KEY UPDATE statement on
        MySQL to be ordered, since parameter order in a MySQL UPDATE clause is
        significant, in a similar manner as that described at
        :ref:`updates_order_parameters`.  Pull request courtesy Maxim Bublis.

        .. seealso::

            :ref:`change_mysql_ondupordering`

    .. change::
       :tags: feature, sql
       :tickets: 4144

       Added :class:`.Sequence` to the "string SQL" system that will render a
       meaningful string expression (``"<next sequence value: my_sequence>"``)
       when stringifying without a dialect a statement that includes a "sequence
       nextvalue" expression, rather than raising a compilation error.



    .. change::
        :tags: bug, orm
        :tickets: 4232

        An informative exception is re-raised when a primary key value is not
        sortable in Python during an ORM flush under Python 3, such as an ``Enum``
        that has no ``__lt__()`` method; normally Python 3 raises a ``TypeError``
        in this case.   The flush process sorts persistent objects by primary key
        in Python so the values must be sortable.


    .. change::
       :tags: orm, bug
       :tickets: 3604

       Removed the collection converter used by the :class:`.MappedCollection`
       class. This converter was used only to assert that the incoming dictionary
       keys matched that of their corresponding objects, and only during a bulk set
       operation.  The converter can interfere with a custom validator or
       :meth:`.AttributeEvents.bulk_replace` listener that wants to convert
       incoming values further.  The ``TypeError`` which would be raised by this
       converter when an incoming key didn't match the value is removed; incoming
       values during a bulk assignment will be keyed to their value-generated key,
       and not the key that's explicitly present in the dictionary.

       Overall, @converter is superseded by the
       :meth:`.AttributeEvents.bulk_replace` event handler added as part of
       :ticket:`3896`.

    .. change::
       :tags: feature, sql
       :tickets: 3989

       Added new naming convention tokens ``column_0N_name``, ``column_0_N_name``,
       etc., which will render the names / keys / labels for all columns referenced
       by a particular constraint in a sequence.  In order to accommodate for the
       length of such a naming convention, the SQL compiler's auto-truncation
       feature now applies itself to constraint names as well, which creates a
       shortened, deterministically generated name for the constraint that will
       apply to a target backend without going over the character limit of that
       backend.

       The change also repairs two other issues.  One is that the  ``column_0_key``
       token wasn't available even though this token was documented, the other was
       that the ``referred_column_0_name`` token would  inadvertently render the
       ``.key`` and not the ``.name`` of the column if these two values were
       different.

       .. seealso::

          :ref:`change_3989`


    .. change::
        :tags: feature, ext
        :tickets: 4196

        Added support for bulk :meth:`.Query.update` and :meth:`.Query.delete`
        to the :class:`.ShardedQuery` class within the horizontal sharding
        extension.  This also adds an additional expansion hook to the
        bulk update/delete methods :meth:`.Query._execute_crud`.

        .. seealso::

            :ref:`change_4196`

    .. change::
        :tags: feature, sql
        :tickets: 4271

        Added new logic to the "expanding IN" bound parameter feature whereby if
        the given list is empty, a special "empty set" expression that is specific
        to different backends is generated, thus allowing IN expressions to be
        fully dynamic including empty IN expressions.

        .. seealso::

            :ref:`change_4271`



    .. change::
        :tags: feature, mysql

        The "pre-ping" feature of the connection pool now uses
        the ``ping()`` method of the DBAPI connection in the case of
        mysqlclient, PyMySQL and mysql-connector-python.  Pull request
        courtesy Maxim Bublis.

        .. seealso::

            :ref:`change_mysql_ping`

    .. change::
        :tags: feature, orm
        :tickets: 4340

        The "selectin" loader strategy now omits the JOIN in the case of a simple
        one-to-many load, where it instead relies loads only from the related
        table, relying upon the foreign key columns of the related table in order
        to match up to primary keys in the parent table.   This optimization can be
        disabled by setting the :paramref:`.relationship.omit_join` flag to False.
        Many thanks to Jayson Reis for the efforts on this.

        .. seealso::

            :ref:`change_4340`

    .. change::
       :tags: bug, orm
       :tickets: 4353

       Added new behavior to the lazy load that takes place when the "old" value of
       a many-to-one is retrieved, such that exceptions which would be raised due
       to either ``lazy="raise"`` or a detached session error are skipped.

       .. seealso::

        :ref:`change_4353`

    .. change::
        :tags: feature, sql

        The Python builtin ``dir()`` is now supported for a SQLAlchemy "properties"
        object, such as that of a Core columns collection (e.g. ``.c``),
        ``mapper.attrs``, etc.  Allows iPython autocompletion to work as well.
        Pull request courtesy Uwe Korn.

    .. change::
       :tags: feature, orm
       :tickets: 4257

       Added ``.info`` dictionary to the :class:`.InstanceState` class, the object
       that comes from calling :func:`.inspect` on a mapped object.

       .. seealso::

            :ref:`change_4257`

    .. change::
        :tags: feature, sql
        :tickets: 3831

        Added new feature :meth:`.FunctionElement.as_comparison` which allows a SQL
        function to act as a binary comparison operation that can work within the
        ORM.

        .. seealso::

            :ref:`change_3831`

    .. change::
       :tags: bug, orm
       :tickets: 4354

       A long-standing oversight in the ORM, the ``__delete__`` method for a many-
       to-one relationship was non-functional, e.g. for an operation such as ``del
       a.b``.  This is now implemented and is equivalent to setting the attribute
       to ``None``.

       .. seealso::

            :ref:`change_4354`
