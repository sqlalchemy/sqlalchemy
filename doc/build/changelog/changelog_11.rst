=============
1.1 Changelog
=============

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
    :version: 1.1.18
    :released: March 6, 2018

    .. change::
        :tags: bug, mysql
        :tickets: 4205
        :versions: 1.2.5

        MySQL dialects now query the server version using ``SELECT @@version``
        explicitly to the server to ensure we are getting the correct version
        information back.   Proxy servers like MaxScale interfere with the value
        that is passed to the DBAPI's connection.server_version value so this
        is no longer reliable.

    .. change::
        :tags: bug, postgresql, py3k
        :tickets: 4208
        :versions: 1.2.5

        Fixed bug in PostgreSQL COLLATE / ARRAY adjustment first introduced
        in :ticket:`4006` where new behaviors in Python 3.7 regular expressions
        caused the fix to fail.

.. changelog::
    :version: 1.1.17
    :released: February 22, 2018

    .. change::
        :tags: bug, ext
        :tickets: 4185

        Repaired regression caused in 1.2.3 and 1.1.16 regarding association proxy
        objects, revising the approach to :ticket:`4185` when calculating the
        "owning class" of an association proxy to default to choosing the current
        class if the proxy object is not directly associated with a mapped class,
        such as a mixin.

.. changelog::
    :version: 1.1.16
    :released: February 16, 2018

    .. change::
        :tags: bug, postgresql
        :versions: 1.2.3

        Added "SSL SYSCALL error: Operation timed out" to the list
        of messages that trigger a "disconnect" scenario for the
        psycopg2 driver.  Pull request courtesy André Cruz.

    .. change::
        :tags: bug, orm
        :tickets: 4187
        :versions: 1.2.3

        Fixed issue in post_update feature where an UPDATE is emitted
        when the parent object has been deleted but the dependent object
        is not.   This issue has existed for a long time however
        since 1.2 now asserts rows matched for post_update, this
        was raising an error.

    .. change::
        :tags: bug, mysql
        :tickets: 4136
        :versions: 1.2.0b4

        Fixed bug where the MySQL "concat" and "match" operators failed to
        propagate kwargs to the left and right expressions, causing compiler
        options such as "literal_binds" to fail.

    .. change::
        :tags: bug, sql
        :versions: 1.2.0b4

        Added :func:`.nullsfirst` and :func:`.nullslast` as top level imports
        in the ``sqlalchemy.`` and ``sqlalchemy.sql.`` namespace.  Pull request
        courtesy Lele Gaifax.

    .. change::
        :tags: bug, orm
        :tickets: 4185
        :versions: 1.2.3

        Fixed regression caused by fix for issue :ticket:`4116` affecting versions
        1.2.2 as well as 1.1.15, which had the effect of mis-calculation of the
        "owning class" of an :class:`.AssociationProxy` as the ``NoneType`` class
        in some declarative mixin/inheritance situations as well as if the
        association proxy were accessed off of an un-mapped class.  The "figure out
        the owner" logic has been replaced by an in-depth routine that searches
        through the complete mapper hierarchy assigned to the class or subclass to
        determine the correct (we hope) match; will not assign the owner if no
        match is found.  An exception is now raised if the proxy is used
        against an un-mapped instance.


    .. change::
        :tags: bug, sql
        :tickets: 4162
        :versions: 1.2.1

        Fixed bug in :meth:`_expression.Insert.values` where using the "multi-values"
        format in combination with :class:`_schema.Column` objects as keys rather
        than strings would fail.   Pull request courtesy Aubrey Stark-Toller.

    .. change::
        :tags: bug, postgresql
        :versions: 1.2.3

        Added "TRUNCATE" to the list of keywords accepted by the
        PostgreSQL dialect as an "autocommit"-triggering keyword.
        Pull request courtesy Jacob Hayes.

    .. change::
        :tags: bug, pool
        :tickets: 4184
        :versions: 1.2.3

        Fixed a fairly serious connection pool bug where a connection that is
        acquired after being refreshed as a result of a user-defined
        :class:`_exc.DisconnectionError` or due to the 1.2-released "pre_ping" feature
        would not be correctly reset if the connection were returned to the pool by
        weakref cleanup (e.g. the front-facing object is garbage collected); the
        weakref would still refer to the previously invalidated DBAPI connection
        which would have the reset operation erroneously called upon it instead.
        This would lead to stack traces in the logs and a connection being checked
        into the pool without being reset, which can cause locking issues.


    .. change::
        :tags: bug, orm
        :tickets: 4151
        :versions: 1.2.1

        Fixed bug where an object that is expunged during a rollback of
        a nested or subtransaction which also had its primary key mutated
        would not be correctly removed from the session, causing subsequent
        issues in using the session.

.. changelog::
    :version: 1.1.15
    :released: November 3, 2017

    .. change::
        :tags: bug, sqlite
        :tickets: 4099
        :versions: 1.2.0b3

        Fixed bug where SQLite CHECK constraint reflection would fail
        if the referenced table were in a remote schema, e.g. on SQLite a
        remote database referred to by ATTACH.

    .. change::
        :tags: bug, mysql
        :tickets: 4097
        :versions: 1.2.0b3

        Warning emitted when MariaDB 10.2.8 or earlier in the 10.2
        series is detected as there are major issues with CHECK
        constraints within these versions that were resolved as of
        10.2.9.

        Note that this changelog message was NOT released with
        SQLAlchemy 1.2.0b3 and was added retroactively.

    .. change::
        :tags: bug, mssql
        :tickets: 4095
        :versions: 1.2.0b3

        Added a full range of "connection closed" exception codes to the
        PyODBC dialect for SQL Server, including '08S01', '01002', '08003',
        '08007', '08S02', '08001', 'HYT00', 'HY010'.  Previously, only '08S01'
        was covered.

    .. change::
        :tags: bug, sql
        :tickets: 4126
        :versions: 1.2.0

        Fixed bug where ``__repr__`` of :class:`.ColumnDefault` would fail
        if the argument were a tuple.  Pull request courtesy Nicolas Caniart.

    .. change::
        :tags: bug, orm, declarative
        :tickets: 4124
        :versions: 1.2.0

        Fixed bug where a descriptor that is elsewhere a mapped column
        or relationship within a hierarchy based on :class:`.AbstractConcreteBase`
        would be referred towards during a refresh operation, causing an error
        as the attribute is not mapped as a mapper property.
        A similar issue can arise for other attributes like the "type" column
        added by :class:`.AbstractConcreteBase` if the class fails to include
        "concrete=True" in its mapper, however the check here should also
        prevent that scenario from causing a problem.

    .. change:: 4006
        :tags: bug, postgresql
        :tickets: 4006
        :versions: 1.2.0b3

        Made further fixes to the :class:`_types.ARRAY` class in conjunction with
        COLLATE, as the fix made in :ticket:`4006` failed to accommodate
        for a multidimensional array.

    .. change::
        :tags: bug, orm, ext
        :tickets: 4116
        :versions: 1.2.0

        Fixed bug where the association proxy would inadvertently link itself
        to an :class:`.AliasedClass` object if it were called first with
        the :class:`.AliasedClass` as a parent, causing errors upon subsequent
        usage.

    .. change::
        :tags: bug, mysql
        :tickets: 4120
        :versions: 1.2.0

        MySQL 5.7.20 now warns for use of the @tx_isolation variable; a version
        check is now performed and uses @transaction_isolation instead
        to prevent this warning.

    .. change::
        :tags: bug, postgresql
        :tickets: 4107
        :versions: 1.2.0b3

        Fixed bug in :obj:`_functions.array_agg` function where passing an argument
        that is already of type :class:`_types.ARRAY`, such as a PostgreSQL
        :obj:`_postgresql.array` construct, would produce a ``ValueError``, due
        to the function attempting to nest the arrays.

    .. change::
        :tags: bug, orm
        :tickets: 4078
        :versions: 1.2.0b3

        Fixed bug where ORM relationship would warn against conflicting sync
        targets (e.g. two relationships would both write to the same column) for
        sibling classes in an inheritance hierarchy, where the two relationships
        would never actually conflict during writes.

    .. change::
        :tags: bug, postgresql
        :tickets: 4074
        :versions: 1.2.0b3

        Fixed bug in PostgreSQL :meth:`.postgresql.dml.Insert.on_conflict_do_update`
        which would prevent the insert statement from being used as a CTE,
        e.g. via :meth:`_expression.Insert.cte`, within another statement.

    .. change::
        :tags: bug, orm
        :tickets: 4103
        :versions: 1.2.0b3

        Fixed bug where correlated select used against single-table inheritance
        entity would fail to render correctly in the outer query, due to adjustment
        for single inheritance discriminator criteria inappropriately re-applying
        the criteria to the outer query.

    .. change::
        :tags: bug, mysql
        :tickets: 4096
        :versions: 1.2.0b3

        Fixed issue where CURRENT_TIMESTAMP would not reflect correctly
        in the MariaDB 10.2 series due to a syntax change, where the function
        is now represented as ``current_timestamp()``.

    .. change::
        :tags: bug, mysql
        :tickets: 4098
        :versions: 1.2.0b3

        MariaDB 10.2 now supports CHECK constraints (warning: use version 10.2.9
        or greater due to upstream issues noted in :ticket:`4097`).  Reflection
        now takes these CHECK constraints into account when they are present in
        the ``SHOW CREATE TABLE`` output.

    .. change::
        :tags: bug, sql
        :tickets: 4093
        :versions: 1.2.0b3

        Fixed bug where the recently added :meth:`.ColumnOperators.any_`
        and :meth:`.ColumnOperators.all_` methods didn't work when called
        as methods, as opposed to using the standalone functions
        :func:`_expression.any_` and :func:`_expression.all_`.  Also
        added documentation examples for these relatively unintuitive
        SQL operators.

.. changelog::
    :version: 1.1.14
    :released: September 5, 2017

    .. change::
        :tags: bug, orm
        :tickets: 4069
        :versions: 1.2.0b3

        Fixed bug in :meth:`.Session.merge` following along similar lines as that
        of :ticket:`4030`, where an internal check for a target object in
        the identity map could lead to an error if it were to be garbage collected
        immediately before the merge routine actually retrieves the object.

    .. change::
        :tags: bug, orm
        :tickets: 4048
        :versions: 1.2.0b3

        Fixed bug where an :func:`.undefer_group` option would not be recognized
        if it extended from a relationship that was loading using joined eager
        loading.  Additionally, as the bug led to excess work being performed,
        Python function call counts are also improved by 20% within the initial
        calculation of result set columns, complementing the joined eager load
        improvements of :ticket:`3915`.

    .. change::
        :tags: bug, orm
        :tickets: 4068

        Fixed race condition in ORM identity map which would cause objects
        to be inappropriately removed during a load operation, causing
        duplicate object identities to occur, particularly under joined eager
        loading which involves deduplication of objects.  The issue is specific
        to garbage collection of weak references and is observed only under the
        PyPy interpreter.

    .. change::
        :tags: bug, orm
        :tickets: 4056
        :versions: 1.2.0b3

        Fixed bug in :meth:`.Session.merge` where objects in a collection that had
        the primary key attribute set to ``None`` for a key that is  typically
        autoincrementing would be considered to be a database-persisted key for
        part of the internal deduplication process, causing only one object to
        actually be inserted in the database.

    .. change::
        :tags: bug, sql
        :tickets: 4053

        Altered the range specification for window functions to allow
        for two of the same PRECEDING or FOLLOWING keywords in a range
        by allowing for the left side of the range to be positive
        and for the right to be negative, e.g. (1, 3) is
        "1 FOLLOWING AND 3 FOLLOWING".

    .. change::
        :tags: bug, orm
        :tickets: 4067
        :versions: 1.2.0b3

        An :class:`.InvalidRequestError` is raised when a :func:`.synonym`
        is used against an attribute that is not against a :class:`.MapperProperty`,
        such as an association proxy.  Previously, a recursion overflow would
        occur trying to locate non-existent attributes.

.. changelog::
    :version: 1.1.13
    :released: August 3, 2017

.. changelog::
    :version: 1.1.12
    :released: July 24, 2017

    .. change:: cache_order_sequence
        :tags: feature, oracle, postgresql
        :versions: 1.2.0b1

        Added new keywords :paramref:`.Sequence.cache` and
        :paramref:`.Sequence.order` to :class:`.Sequence`, to allow rendering
        of the CACHE parameter understood by Oracle and PostgreSQL, and the
        ORDER parameter understood by Oracle.  Pull request
        courtesy David Moore.


    .. change:: 4033
        :tags: bug, orm
        :tickets: 4033
        :versions: 1.2.0b2

        Fixed regression from 1.1.11 where adding additional non-entity
        columns to a query that includes an entity with subqueryload
        relationships would fail, due to an inspection added in 1.1.11 as a
        result of :ticket:`4011`.


    .. change:: 4031
        :tags: bug, orm
        :versions: 1.2.0b2
        :tickets: 4031

        Fixed bug involving JSON NULL evaluation logic added in 1.1 as part
        of :ticket:`3514` where the logic would not accommodate ORM
        mapped attributes named differently from the :class:`_schema.Column`
        that was mapped.

    .. change:: 4030
        :tags: bug, orm
        :versions: 1.2.0b2
        :tickets: 4030

        Added ``KeyError`` checks to all methods within
        :class:`.WeakInstanceDict` where a check for ``key in dict`` is
        followed by indexed access to that key, to guard against a race against
        garbage collection that under load can remove the key from the dict
        after the code assumes its present, leading to very infrequent
        ``KeyError`` raises.

.. changelog::
    :version: 1.1.11
    :released: Monday, June 19, 2017

    .. change:: 4012
        :tags: bug, sql
        :tickets: 4012
        :versions: 1.2.0b1

        Fixed AttributeError which would occur in :class:`.WithinGroup`
        construct during an iteration of the structure.

    .. change:: 4011
        :tags: bug, orm
        :tickets: 4011
        :versions: 1.2.0b1

        Fixed issue with subquery eagerloading which continues on from
        the series of issues fixed in :ticket:`2699`, :ticket:`3106`,
        :ticket:`3893` involving that the "subquery" contains the correct
        FROM clause when beginning from a joined inheritance subclass
        and then subquery eager loading onto a relationship from
        the base class, while the query also includes criteria against
        the subclass. The fix in the previous tickets did not accommodate
        for additional subqueryload operations loading more deeply from
        the first level, so the fix has been further generalized.

    .. change:: 4005
        :tags: bug, postgresql
        :tickets: 4005
        :versions: 1.2.0b1

        Continuing with the fix that correctly handles PostgreSQL
        version string "10devel" released in 1.1.8, an additional regexp
        bump to handle version strings of the form "10beta1".   While
        PostgreSQL now offers better ways to get this information, we
        are sticking w/ the regexp at least through 1.1.x for the least
        amount of risk to compatibility w/ older or alternate PostgreSQL
        databases.

    .. change:: 4006
        :tags: bug, postgresql
        :tickets: 4006
        :versions: 1.2.0b1

        Fixed bug where using :class:`_types.ARRAY` with a string type that
        features a collation would fail to produce the correct syntax
        within CREATE TABLE.

    .. change:: 4007
        :tags: bug, mysql
        :tickets: 4007
        :versions: 1.2.0b1

        MySQL 5.7 has introduced permission limiting for the "SHOW VARIABLES"
        command; the MySQL dialect will now handle when SHOW returns no
        row, in particular for the initial fetch of SQL_MODE, and will
        emit a warning that user permissions should be modified to allow the
        row to be present.

    .. change:: 3994
        :tags: bug, mssql
        :tickets: 3994
        :versions: 1.2.0b1

        Fixed bug where SQL Server transaction isolation must be fetched
        from a different view when using Azure data warehouse, the query
        is now attempted against both views and then a NotImplemented
        is raised unconditionally if failure continues to provide the
        best resiliency against future arbitrary API changes in new
        SQL Server versions.

    .. change:: 3997
        :tags: bug, oracle
        :tickets: 3997
        :versions: 1.2.0b1

        Support for two-phase transactions has been removed entirely for
        cx_Oracle when version 6.0b1 or later of the DBAPI is in use.  The two-
        phase feature historically has never been usable under cx_Oracle 5.x in
        any case, and cx_Oracle 6.x has removed the connection-level "twophase"
        flag upon which this feature relied.

    .. change:: 3973
        :tags: bug, mssql
        :tickets: 3973
        :versions: 1.2.0b1

        Added a placeholder type :class:`_mssql.XML` to the SQL Server
        dialect, so that a reflected table which includes this type can
        be re-rendered as a CREATE TABLE.  The type has no special round-trip
        behavior nor does it currently support additional qualifying
        arguments.

.. changelog::
    :version: 1.1.10
    :released: Friday, May 19, 2017

    .. change:: 3986
        :tags: bug, orm
        :versions: 1.2.0b1
        :tickets: 3986

        Fixed bug where a cascade such as "delete-orphan" (but others as well)
        would fail to locate an object linked to a relationship that itself
        is local to a subclass in an inheritance relationship, thus causing
        the operation to not take place.

    .. change:: 3975
        :tags: bug, oracle
        :versions: 1.2.0b1
        :tickets: 3975

        Fixed bug in cx_Oracle dialect where version string parsing would
        fail for cx_Oracle version 6.0b1 due to the "b" character.  Version
        string parsing is now via a regexp rather than a simple split.

    .. change:: 3949
        :tags: bug, schema
        :versions: 1.2.0b1
        :tickets: 3949

        An :class:`.ArgumentError` is now raised if a
        :class:`_schema.ForeignKeyConstraint` object is created with a mismatched
        number of "local" and "remote" columns, which otherwise causes the
        internal state of the constraint to be incorrect.   Note that this
        also impacts the condition where a dialect's reflection process
        produces a mismatched set of columns for a foreign key constraint.

    .. change:: 3980
        :tags: bug, ext
        :versions: 1.2.0b1
        :tickets: 3980

        Protected against testing "None" as a class in the case where
        declarative classes are being garbage collected and new
        automap prepare() operations are taking place concurrently, very
        infrequently hitting a weakref that has not been fully acted upon
        after gc.

    .. change::
        :tags: bug, postgresql
        :versions: 1.2.0b1

        Added "autocommit" support for GRANT, REVOKE keywords.  Pull request
        courtesy Jacob Hayes.

    .. change:: 3966
        :tags: bug, mysql
        :versions: 1.2.0b1
        :tickets: 3966

        Removed an ancient and unnecessary intercept of the UTC_TIMESTAMP
        MySQL function, which was getting in the way of using it with a
        parameter.

    .. change:: 3961
        :tags: bug, mysql
        :versions: 1.2.0b1
        :tickets: 3961

        Fixed bug in MySQL dialect regarding rendering of table options in
        conjunction with PARTITION options when rendering CREATE TABLE.
        The PARTITION related options need to follow the table options,
        whereas previously this ordering was not enforced.


.. changelog::
    :version: 1.1.9
    :released: April 4, 2017

    .. change:: 3956
        :tags: bug, ext
        :tickets: 3956

        Fixed regression released in 1.1.8 due to :ticket:`3950` where the
        deeper search for information about column types in the case of a
        "schema type" or a :class:`.TypeDecorator` would produce an attribute
        error if the mapping also contained a :obj:`.column_property`.

    .. change:: 3952
        :tags: bug, sql
        :versions: 1.2.0b1
        :tickets: 3952

        Fixed regression released in 1.1.5 due to :ticket:`3859` where
        adjustments to the "right-hand-side" evaluation of an expression
        based on :class:`.Variant` to honor the underlying type's
        "right-hand-side" rules caused the :class:`.Variant` type
        to be inappropriately lost, in those cases when we *do* want the
        left-hand side type to be transferred directly to the right hand side
        so that bind-level rules can be applied to the expression's argument.

    .. change:: 3955
        :tags: bug, sql, postgresql
        :versions: 1.2.0b1
        :tickets: 3955

        Changed the mechanics of :class:`_engine.ResultProxy` to unconditionally
        delay the "autoclose" step until the :class:`_engine.Connection` is done
        with the object; in the case where PostgreSQL ON CONFLICT with
        RETURNING returns no rows, autoclose was occurring in this previously
        non-existent use case, causing the usual autocommit behavior that
        occurs unconditionally upon INSERT/UPDATE/DELETE to fail.

.. changelog::
    :version: 1.1.8
    :released: March 31, 2017

    .. change:: 3950
        :tags: bug, ext
        :versions: 1.2.0b1
        :tickets: 3950

        Fixed bug in :mod:`sqlalchemy.ext.mutable` where the
        :meth:`.Mutable.as_mutable` method would not track a type that had
        been copied using :meth:`.TypeEngine.copy`.  This became more of
        a regression in 1.1 compared to 1.0 because the :class:`.TypeDecorator`
        class is now a subclass of :class:`.SchemaEventTarget`, which among
        other things indicates to the parent :class:`_schema.Column` that the type
        should be copied when the :class:`_schema.Column` is.  These copies are
        common when using declarative with mixins or abstract classes.

    .. change::
        :tags: bug, ext
        :versions: 1.2.0b1

        Added support for bound parameters, e.g. those normally set up
        via :meth:`_query.Query.params`, to the :meth:`.baked.Result.count`
        method.  Previously, support for parameters were omitted. Pull request
        courtesy Pat Deegan.

    .. change::
        :tags: bug, postgresql
        :versions: 1.2.0b1

        Added support for parsing the PostgreSQL version string for
        a development version like "PostgreSQL 10devel".  Pull request
        courtesy Sean McCully.

.. changelog::
    :version: 1.1.7
    :released: March 27, 2017

    .. change::
        :tags: feature, orm
        :tickets: 3933
        :versions: 1.2.0b1

        An :func:`.aliased()` construct can now be passed to the
        :meth:`_query.Query.select_entity_from` method.   Entities will be pulled
        from the selectable represented by the :func:`.aliased` construct.
        This allows special options for :func:`.aliased` such as
        :paramref:`.aliased.adapt_on_names` to be used in conjunction with
        :meth:`_query.Query.select_entity_from`.

    .. change::
        :tags: bug, engine
        :tickets: 3946
        :versions: 1.2.0b1

        Added an exception handler that will warn for the "cause" exception on
        Py2K when the "autorollback" feature of :class:`_engine.Connection` itself
        raises an exception. In Py3K, the two exceptions are naturally reported
        by the interpreter as one occurring during the handling of the other.
        This is continuing with the series of changes for rollback failure
        handling that were last visited as part of :ticket:`2696` in 1.0.12.

    .. change::
        :tags: bug, orm
        :tickets: 3947
        :versions: 1.2.0b1

        Fixed a race condition which could occur under threaded environments
        as a result of the caching added via :ticket:`3915`.   An internal
        collection of ``Column`` objects could be regenerated on an alias
        object inappropriately, confusing a joined eager loader when it
        attempts to render SQL and collect results and resulting in an
        attribute error.   The collection is now generated up front before
        the alias object is cached and shared among threads.

    .. change::
        :tags: bug, sql, postgresql
        :tickets: 2892

        Added support for the :class:`.Variant` and the :class:`.SchemaType`
        objects to be compatible with each other.  That is, a variant
        can be created against a type like :class:`.Enum`, and the instructions
        to create constraints and/or database-specific type objects will
        propagate correctly as per the variant's dialect mapping.

    .. change::
        :tags: bug, sql
        :tickets: 3931

        Fixed bug in compiler where the string identifier of a savepoint would
        be cached in the identifier quoting dictionary; as these identifiers
        are arbitrary, a small memory leak could occur if a single
        :class:`_engine.Connection` had an unbounded number of savepoints used,
        as well as if the savepoint clause constructs were used directly
        with an unbounded umber of savepoint names.   The memory leak does
        **not** impact the vast majority of cases as normally the
        :class:`_engine.Connection`, which renders savepoint names with a simple
        counter starting at "1", is used on a per-transaction or
        per-fixed-number-of-transactions basis before being discarded.

    .. change::
        :tags: bug, sql
        :tickets: 3924

        Fixed bug in new "schema translate" feature where the translated schema
        name would be invoked in terms of an alias name when rendered along
        with a column expression; occurred only when the source translate
        name was "None".   The "schema translate" feature now only takes
        effect for :class:`.SchemaItem` and :class:`.SchemaType` subclasses,
        that is, objects that correspond to a DDL-creatable structure in
        a database.

.. changelog::
    :version: 1.1.6
    :released: February 28, 2017

    .. change::
        :tags: bug, mysql

        Added new MySQL 8.0 reserved words to the MySQL dialect for proper
        quoting.  Pull request courtesy Hanno Schlichting.

    .. change:: 3915
        :tags: bug, orm
        :tickets: 3915

        Addressed some long unattended performance concerns within the joined
        eager loader query construction system that have accumulated since
        earlier versions as a result of increased abstraction. The use of ad-
        hoc :class:`.AliasedClass` objects per query, which produces lots of
        column lookup overhead each time, has been replaced with a cached
        approach that makes use of a small pool of :class:`.AliasedClass`
        objects that are reused between invocations of joined eager loading.
        Some mechanics involving eager join path construction have also been
        optimized.   Callcounts for an end-to-end query construction + single
        row fetch test with a worst-case joined loader scenario have been
        reduced by about 60% vs. 1.1.5 and 42% vs. that of 0.8.6.

    .. change:: 3804
        :tags: bug, postgresql
        :tickets: 3804

        Added regular expressions for the "IMPORT FOREIGN SCHEMA",
        "REFRESH MATERIALIZED VIEW" PostgreSQL statements so that they
        autocommit when invoked via a connection or engine without
        an explicit transaction.  Pull requests courtesy Frazer McLean
        and Paweł Stiasny.

    .. change:: 3909
        :tags: bug, orm
        :tickets: 3909

        Fixed a major inefficiency in the "eager_defaults" feature whereby
        an unnecessary SELECT would be emitted for column values where the
        ORM had explicitly inserted NULL, corresponding to attributes that
        were unset on the object but did not have any server default
        specified, as well as expired attributes on update that nevertheless
        had no server onupdate set up.   As these columns are not part of the
        RETURNING that eager_defaults tries to use, they should not be
        post-SELECTed either.

    .. change:: 3908
        :tags: bug, orm
        :tickets: 3908

        Fixed two closely related bugs involving the mapper eager_defaults
        flag in conjunction with single-table inheritance; one where the
        eager defaults logic would inadvertently try to access a column
        that's part of the mapper's "exclude_properties" list (used by
        Declarative with single table inheritance) during the eager defaults
        fetch, and the other where the full load of the row in order to
        fetch the defaults would fail to use the correct inheriting mapper.


    .. change:: 3905
        :tags: bug, sql
        :tickets: 3905

        Fixed bug whereby the :meth:`.DDLEvents.column_reflect` event would not
        allow a non-textual expression to be passed as the value of the
        "default" for the new column, such as a :class:`.FetchedValue`
        object to indicate a generic triggered default or a
        :func:`_expression.text` construct.  Clarified the documentation
        in this regard as well.

    .. change:: 3901
        :tags: bug, ext
        :tickets: 3901

        Fixed bug in new :mod:`sqlalchemy.ext.indexable` extension
        where setting of a property that itself refers to another property
        would fail.

    .. change:: 3900
        :tags: bug, postgresql
        :tickets: 3900

        Fixed bug in PostgreSQL :class:`.ExcludeConstraint` where the
        "whereclause" and "using" parameters would not be copied during an
        operation like :meth:`_schema.Table.tometadata`.

    .. change:: 3898
        :tags: bug, mssql
        :tickets: 3898

        Added a version check to the "get_isolation_level" feature, which is
        invoked upon first connect, so that it skips for SQL Server version
        2000, as the necessary system view is not available prior to SQL Server
        2005.

    .. change:: 3897
        :tags: feature, ext
        :tickets: 3896

        Added :meth:`.baked.Result.scalar` and :meth:`.baked.Result.count`
        to the "baked" query system.

    .. change:: 3895
        :tags: bug, orm, declarative
        :tickets: 3895

        Fixed bug where the "automatic exclude" feature of declarative that
        ensures a column local to a single table inheritance subclass does
        not appear as an attribute on other derivations of the base would
        not take effect for multiple levels of subclassing from the base.

    .. change:: 3893
        :tags: bug, orm
        :tickets: 3893

        Fixed bug first introduced in 0.9.7 as a result of :ticket:`3106`
        which would cause an incorrect query in some forms of multi-level
        subqueryload against aliased entities, with an unnecessary extra
        FROM entity in the innermost subquery.

.. changelog::
    :version: 1.1.5
    :released: January 17, 2017

    .. change:: mysql_index_prefix
        :tags: feature, mysql

        Added a new parameter ``mysql_prefix`` supported by the :class:`.Index`
        construct, allows specification of MySQL-specific prefixes such as
        "FULLTEXT". Pull request courtesy Joseph Schorr.

    .. change:: 3854
        :tags: bug, orm
        :tickets: 3854

        Fixed bug in subquery loading where an object encountered as an
        "existing" row, e.g. already loaded from a different path in the
        same query, would not invoke subquery loaders for unloaded attributes
        that specified this loading.  This issue is in the same area
        as that of :ticket:`3431`, :ticket:`3811` which involved
        similar issues with joined loading.

    .. change:: 3888
        :tags: bug, postgresql
        :tickets: 3888

        Fixed bug in new "ON CONFLICT DO UPDATE" feature where the "set"
        values for the UPDATE clause would not be subject to type-level
        processing, as normally takes effect to handle both user-defined
        type level conversions as well as dialect-required conversions, such
        as those required for JSON datatypes.   Additionally, clarified that
        the keys in the ``set_`` dictionary should match the "key" of the
        column, if distinct from the column name.  A warning is emitted
        for remaining column names that don't match column keys; for
        compatibility reasons, these are emitted as they were previously.

    .. change:: 3872
        :tags: bug, examples
        :tickets: 3872

        Fixed two issues with the versioned_history example, one is that
        the history table now gets autoincrement=False to avoid 1.1's new
        errors regarding composite primary keys with autoincrement; the other
        is that the sqlite_autoincrement flag is now used to ensure on SQLite,
        unique identifiers are used for the lifespan of a table even if
        some rows are deleted.  Pull request courtesy Carlos García Montoro.

    .. change:: 3882
        :tags: bug, sql
        :tickets: 3882

        Fixed bug originally introduced in 0.9 via :ticket:`1068` where
        order_by(<some Label()>) would order by the label name based on name
        alone, that is, even if the labeled expression were not at all the same
        expression otherwise present, implicitly or explicitly, in the
        selectable.  The logic that orders by label now ensures that the
        labeled expression is related to the one that resolves to that name
        before ordering by the label name; additionally, the name has to
        resolve to an actual label explicit in the expression elsewhere, not
        just a column name.  This logic is carefully kept separate from the
        order by(textual name) feature that has a slightly different purpose.

    .. change:: try_finally_for_noautoflush
        :tags: bug, orm

        The :attr:`.Session.no_autoflush` context manager now ensures that
        the autoflush flag is reset within a "finally" block, so that if
        an exception is raised within the block, the state still resets
        appropriately.  Pull request courtesy Emin Arakelian.

    .. change:: 3878
        :tags: bug, sql
        :tickets: 3878

        Fixed 1.1 regression where "import *" would not work for
        sqlalchemy.sql.expression, due to mis-spelled ``any_`` and ``all_``
        functions.

    .. change:: 3880
        :tags: bg, sql
        :tickets: 3880

        Fixed bug where literal_binds compiler flag was not honored by the
        :class:`_expression.Insert` construct for the "multiple values" feature; the
        subsequent values are now rendered as literals.

    .. change:: 3877
        :tags: bug, oracle, postgresql
        :tickets: 3877

        Fixed bug where an INSERT from SELECT where the source table contains
        an autoincrementing Sequence would fail to compile correctly.

    .. change:: 3876
        :tags: bug, mssql
        :tickets: 3876

        Fixed bug where SQL Server dialects would attempt to select the
        last row identity for an INSERT from SELECT, failing in the case when
        the SELECT has no rows.  For such a statement,
        the inline flag is set to True indicating no last primary key
        should be fetched.

    .. change:: 3875
        :tags: bug, oracle
        :tickets: 3875

        Fixed bug where the "COMPRESSION" keyword was used in the ALL_TABLES
        query on Oracle 9.2; even though Oracle docs state table compression
        was introduced in 9i, the actual column is not present until
        10.1.

    .. change:: 3874
        :tags: bug, orm
        :tickets: 3874

        Fixed bug where the single-table inheritance query criteria would not
        be inserted into the query in the case that the :class:`.Bundle`
        construct were used as the selection criteria.

    .. change:: repr_for_url_reflect
        :tags: bug, sql

        The engine URL embedded in the exception for "could not reflect"
        in :meth:`_schema.MetaData.reflect` now conceals the password; also
        the ``__repr__`` for :class:`.TLEngine` now acts like that of
        :class:`_engine.Engine`, concealing the URL password.  Pull request courtesy
        Valery Yundin.

    .. change:: 3867
        :tags: bug, mysql
        :tickets: 3867

        The MySQL dialect now will not warn when a reflected column has a
        "COMMENT" keyword on it, but note however the comment is not yet
        reflected; this is on the roadmap for a future release.  Pull request
        courtesy Lele Long.

    .. change:: pg_timestamp_zero_prec
        :tags: bug, postgresql

        The :class:`_postgresql.TIME` and :class:`_postgresql.TIMESTAMP`
        datatypes now support a setting of zero for "precision"; previously
        a zero would be ignored.  Pull request courtesy Ionuț Ciocîrlan.

    .. change:: 3861
        :tags: bug, engine
        :tickets: 3861

        The "extend_existing" option of :class:`_schema.Table` reflection would
        cause indexes and constraints to be doubled up in the case that the parameter
        were used with :meth:`_schema.MetaData.reflect` (as the automap extension does)
        due to tables being reflected both within the foreign key path as well
        as directly.  A new de-duplicating set is passed through within the
        :meth:`_schema.MetaData.reflect` sequence to prevent double reflection in this
        way.

    .. change:: 3859
        :tags: bug, sql
        :tickets: 3859

        Fixed issue in :class:`.Variant` where the "right hand coercion" logic,
        inherited from :class:`.TypeDecorator`, would
        coerce the right-hand side into the :class:`.Variant` itself, rather than
        what the default type for the :class:`.Variant` would do.   In the
        case of :class:`.Variant`, we want the type to act mostly like the base
        type so the default logic of :class:`.TypeDecorator` is now overridden
        to fall back to the underlying wrapped type's logic.   Is mostly relevant
        for JSON at the moment.

    .. change:: 3856
        :tags: bug, orm
        :tickets: 3856

        Fixed bug related to :ticket:`3177`, where a UNION or other set operation
        emitted by a :class:`_query.Query` would apply "single-inheritance" criteria
        to the outside of the union (also referencing the wrong selectable),
        even though this criteria is now expected to
        be already present on the inside subqueries.  The single-inheritance
        criteria is now omitted once union() or another set operation is
        called against :class:`_query.Query` in the same way as :meth:`_query.Query.from_self`.

    .. change:: 3548
        :tags: bug, firebird
        :tickets: 3548

        Ported the fix for Oracle quoted-lowercase names to Firebird, so that
        a table name that is quoted as lower case can be reflected properly
        including when the table name comes from the get_table_names()
        inspection function.

.. changelog::
    :version: 1.1.4
    :released: November 15, 2016

    .. change::  3842
        :tags: bug, sql
        :tickets: 3842

        Fixed bug where newly added warning for primary key on insert w/o
        autoincrement setting (see :ref:`change_3216`) would fail to emit
        correctly when invoked upon a lower-case :func:`.table` construct.

    .. change::  3852
        :tags: bug, orm
        :tickets: 3852

        Fixed regression in collections due to :ticket:`3457` whereby
        deserialize during pickle or deepcopy would fail to establish all
        attributes of an ORM collection, causing further mutation operations to
        fail.

    .. change::  default_schema
        :tags: bug, engine

        Removed long-broken "default_schema_name()" method from
        :class:`_engine.Connection`.  This method was left over from a very old
        version and was non-working (e.g. would raise).  Pull request
        courtesy Benjamin Dopplinger.

    .. change:: pragma
        :tags: bug, sqlite

        Added quotes to the PRAGMA directives in the pysqlcipher dialect
        to support additional cipher arguments appropriately.  Pull request
        courtesy Kevin Jurczyk.

    .. change:: 3846
        :tags: bug, postgresql
        :tickets: 3846, 3807

        Fixed regression caused by the fix in :ticket:`3807` (version 1.1.0)
        where we ensured that the tablename was qualified in the WHERE clause
        of the DO UPDATE portion of PostgreSQL's ON CONFLICT, however you
        *cannot* put the table name in the  WHERE clause in the actual ON
        CONFLICT itself.   This was an incorrect assumption, so that portion
        of the change in :ticket:`3807` is rolled back.

    .. change:: 3845
        :tags: bug, orm
        :tickets: 3845

        Fixed long-standing bug where the "noload" relationship loading
        strategy would cause backrefs and/or back_populates options to be
        ignored.

    .. change:: sscursor_mysql
        :tags: feature, mysql

        Added support for server side cursors to the mysqlclient and
        pymysql dialects.   This feature is available via the
        :paramref:`.Connection.execution_options.stream_results` flag as well
        as the ``server_side_cursors=True`` dialect argument in the
        same way that it has been for psycopg2 on PostgreSQL.  Pull request
        courtesy Roman Podoliaka.

    .. change::
        :tags: bug, mysql
        :tickets: 3841

        MySQL's native ENUM type supports any non-valid value being sent, and
        in response will return a blank string.  A hardcoded rule to check for
        "is returning the blank string" has been added to the  MySQL
        implementation for ENUM so that this blank string is returned to the
        application rather than being rejected as a non-valid value.  Note that
        if your MySQL enum is linking values to objects, you still get the
        blank string back.

    .. change::
        :tags: bug, sqlite, py3k

        Added an optional import for the pysqlcipher3 DBAPI when using the
        pysqlcipher dialect.  This package will attempt to be imported
        if the Python-2 only pysqlcipher DBAPI is non-present.
        Pull request courtesy Kevin Jurczyk.

.. changelog::
    :version: 1.1.3
    :released: October 27, 2016

    .. change::
        :tags: bug, orm
        :tickets: 3839

        Fixed regression caused by :ticket:`2677` whereby calling
        :meth:`.Session.delete` on an object that was already flushed as
        deleted in that session would fail to set up the object in the
        identity map (or reject the object), causing flush errors as the
        object were in a state not accommodated by the unit of work.
        The pre-1.1 behavior in this case has been restored, which is that
        the object is put back into the identity map so that the DELETE
        statement will be attempted again, which emits a warning that the number
        of expected rows was not matched (unless the row were restored outside
        of the session).

    .. change::
        :tags: bug, postgresql
        :tickets: 3835

        PostgreSQL table reflection will ensure that the
        :paramref:`_schema.Column.autoincrement` flag is set to False when reflecting
        a primary key column that is not of an :class:`.Integer` datatype,
        even if the default is related to an integer-generating sequence.
        This can happen if a column is created as SERIAL and the datatype
        is changed.  The autoincrement flag can only be True if the datatype
        is of integer affinity in the 1.1 series.

    .. change::
        :tags: bug, orm
        :tickets: 3836

        Fixed regression where some :class:`_query.Query` methods like
        :meth:`_query.Query.update` and others would fail if the :class:`_query.Query`
        were against a series of mapped columns, rather than the mapped
        entity as a whole.

    .. change::
        :tags: bug, sql
        :tickets: 3833

        Fixed bug involving new value translation and validation feature
        in :class:`.Enum` whereby using the enum object in a string
        concatenation would maintain the :class:`.Enum` type as the type
        of the expression overall, producing missing lookups.  A string
        concatenation against an :class:`.Enum`-typed column now uses
        :class:`.String` as the datatype of the expression itself.

    .. change::
        :tags: bug, sql
        :tickets: 3832

        Fixed regression which occurred as a side effect of :ticket:`2919`,
        which in the less typical case of a user-defined
        :class:`.TypeDecorator` that was also itself an instance of
        :class:`.SchemaType` (rather than the implementation being such)
        would cause the column attachment events to be skipped for the
        type itself.


.. changelog::
    :version: 1.1.2
    :released: October 17, 2016

    .. change::
        :tags: bug, sql
        :tickets: 3823

        Fixed a regression caused by a newly added function that performs the
        "wrap callable" function of sql :class:`.DefaultGenerator` objects,
        an attribute error raised for ``__module__`` when the default callable
        was a ``functools.partial`` or other object that doesn't have a
        ``__module__`` attribute.

    .. change::
        :tags: bug, orm
        :tickets: 3824

        Fixed bug involving the rule to disable a joined collection eager
        loader on the other side of a many-to-one lazy loader, first added
        in :ticket:`1495`, where the rule would fail if the parent object
        had some other lazyloader-bound query options associated with it.

    .. change::
        :tags: bug, orm
        :tickets: 3822

        Fixed self-referential entity, deferred column loading issue in a
        similar style as that of :ticket:`3431`, :ticket:`3811` where an entity
        is present in multiple positions within the row due to self-referential
        eager loading; when the deferred loader only applies to one of the
        paths, the "present" column loader will now override the deferred non-
        load for that entity regardless of row ordering.

    .. change::
        :tags: bug, sql, postgresql
        :tickets: 3827

        Fixed regression in :class:`.Enum` type where event handlers were not
        transferred in the case of the type object being copied, due to a
        conflicting copy() method added as part of :ticket:`3250`.  This copy
        occurs normally in situations when the column is copied, such as
        in tometadata() or when using declarative mixins with columns.  The
        event handler not being present would impact the constraint being
        created for a non-native enumerated type, but more critically the
        ENUM object on the PostgreSQL backend.


    .. change::
        :tags: bug, postgresql, sql
        :tickets: 3828

        Changed the naming convention used when generating bound parameters
        for a multi-VALUES insert statement, so that the numbered parameter
        names don't conflict with the anonymized parameters of a WHERE clause,
        as is now common in a PostgreSQL ON CONFLICT construct.

.. changelog::
    :version: 1.1.1
    :released: October 7, 2016

    .. change::
        :tags: bug, mssql
        :tickets: 3820

        The "SELECT SERVERPROPERTY"
        query added in :ticket:`3810` and :ticket:`3814` is failing on unknown
        combinations of Pyodbc and SQL Server.  While failure of this function
        was anticipated, the exception catch was not broad enough so it now
        catches all forms of pyodbc.Error.

    .. change::
        :tags: bug, core
        :tickets: 3216

        Changed the CompileError raised when various primary key missing
        situations are detected to a warning.  The statement is again
        passed to the database where it will fail and the DBAPI error (usually
        IntegrityError) raises as usual.

        .. seealso::

            :ref:`change_3216`

.. changelog::
    :version: 1.1.0
    :released: October 5, 2016

    .. change::
        :tags: bug, sql
        :tickets: 3805

        Execution options can now be propagated from within a
        statement at compile time to the outermost statement, so that
        if an embedded element wants to set "autocommit" to be True for example,
        it can propagate this to the enclosing statement.  Currently, this
        feature is enabled for a DML-oriented CTE embedded inside of a SELECT
        statement, e.g. INSERT/UPDATE/DELETE inside of SELECT.

    .. change::
        :tags: bug, orm
        :tickets: 3802

        ORM attributes can now be assigned any object that is has a
        ``__clause_element__()`` attribute, which will result in inline
        SQL the way any :class:`_expression.ClauseElement` class does.  This covers other
        mapped attributes not otherwise transformed by further expression
        constructs.

    .. change::
        :tags: feature, orm
        :tickets: 3812

        Enhanced the new "raise" lazy loader strategy to also include a
        "raise_on_sql" variant, available both via :paramref:`.orm.relationship.lazy`
        as well as :func:`_orm.raiseload`.   This variant only raises if the
        lazy load would actually emit SQL, vs. raising if the lazy loader
        mechanism is invoked at all.

    .. change::
        :tags: bug, postgresql
        :tickets: 3813

        An adjustment to ON CONFLICT such that the "inserted_primary_key"
        logic is able to accommodate the case where there's no INSERT or
        UPDATE and there's no net change.  The value comes out as None
        in this case, rather than failing on an exception.

    .. change::
        :tags: bug, orm
        :tickets: 3811

        Made an adjustment to the bug fix first introduced in [ticket:3431]
        that involves an object appearing in multiple contexts in a single
        result set, such that an eager loader that would set the related
        object value to be None will still fire off, thus satisfying the
        load of that attribute.  Previously, the adjustment only honored
        a non-None value arriving for an eagerly loaded attribute in a
        secondary row.

    .. change::
        :tags: bug, orm
        :tickets: 3808

        Fixed bug in new :meth:`.SessionEvents.persistent_to_deleted` event
        where the target object could be garbage collected before the event
        is fired off.

    .. change::
        :tags: bug, sql
        :tickets: 3809

        A string sent as a column default via the
        :paramref:`_schema.Column.server_default` parameter is now escaped for quotes.

        .. seealso::

            :ref:`change_3809`

    .. change::
        :tags: bug, postgresql
        :tickets: 3807

        Fixed issue in new PG "on conflict" construct where columns including
        those of the "excluded" namespace would not be table-qualified
        in the WHERE clauses in the statement.

     .. change::
        :tags: bug, sql, postgresql
        :tickets: 3806

        Added compiler-level flags used by PostgreSQL to place additional
        parenthesis than would normally be generated by precedence rules
        around operations involving JSON, HSTORE indexing operators as well as
        within their operands since it has been observed that PostgreSQL's
        precedence rules for at least the HSTORE indexing operator is not
        consistent between 9.4 and 9.5.

    .. change::
        :tags: bug, sql, mysql
        :tickets: 3803

        The ``BaseException`` exception class is now intercepted by the
        exception-handling routines of :class:`_engine.Connection`, and includes
        handling by the :meth:`_events.ConnectionEvents.handle_error`
        event.  The :class:`_engine.Connection` is now **invalidated** by default in
        the case of a system level exception that is not a subclass of
        ``Exception``, including ``KeyboardInterrupt`` and the greenlet
        ``GreenletExit`` class, to prevent further operations from occurring
        upon a database connection that is in an unknown and possibly
        corrupted state.  The MySQL drivers are most targeted by this change
        however the change is across all DBAPIs.

        .. seealso::

            :ref:`change_3803`

    .. change::
        :tags: bug, sql
        :tickets: 3799

        The "eq" and "ne" operators are no longer part of the list of
        "associative" operators, while they remain considered to be
        "commutative".  This allows an expression like ``(x == y) == z``
        to be maintained at the SQL level with parenthesis.  Pull request
        courtesy John Passaro.

    .. change::
        :tags: bug, orm
        :tickets: 3767

        The primaryjoin of a :func:`_orm.relationship` construct can now include
        a :func:`.bindparam` object that includes a callable function to
        generate values.  Previously, the lazy loader strategy would
        be incompatible with this use, and additionally would fail to correctly
        detect if the "use_get" criteria should be used if the primary key
        were involved with the bound parameter.

    .. change::
        :tags: bug, orm
        :tickets: 3801

        An UPDATE emitted from the ORM flush process can now accommodate a
        SQL expression element for a column within the primary key of an
        object, if the target database supports RETURNING in order to provide
        the new value, or if the PK value is set "to itself" for the purposes
        of bumping some other trigger / onupdate on the column.

    .. change::
        :tags: bug, orm
        :tickets: 3788

        Fixed bug where the "simple many-to-one" condition that allows  lazy
        loading to use get() from identity map would fail to be  invoked if the
        primaryjoin of the relationship had multiple clauses separated by AND
        which were not in the same order as that of the primary key columns
        being compared in each clause. This ordering
        difference occurs for a composite foreign key where the table-bound
        columns on the referencing side were not in the same order in the .c
        collection as the primary key columns on the referenced side....which
        in turn occurs a lot if one is using declarative mixins and/or
        declared_attr to set up columns.

    .. change::
        :tags: bug, sql
        :tickets: 3789

        Stringify of expression with unnamed :class:`_schema.Column` objects, as
        occurs in lots of situations including ORM error reporting,
        will now render the name in string context as "<name unknown>"
        rather than raising a compile error.

    .. change::
        :tags: bug, sql
        :tickets: 3786

        Raise a more descriptive exception / message when ClauseElement
        or non-SQLAlchemy objects that are not "executable" are erroneously
        passed to ``.execute()``; a new exception ObjectNotExecutableError
        is raised consistently in all cases.

    .. change::
        :tags: bug, orm
        :tickets: 3776

        An exception is raised when two ``@validates`` decorators on a mapping
        make use of the same name.  Only one validator of a certain name
        at a time is supported, there's no mechanism to chain these together,
        as the order of the validators at the level of function decorator
        can't be made deterministic.

        .. seealso::

            :ref:`change_3776`

    .. change::
        :tags: bug, orm

        Mapper errors raised during :func:`.configure_mappers` now explicitly
        include the name of the originating mapper in the exception message
        to help in those situations where the wrapped exception does not
        itself include the source mapper.  Pull request courtesy
        John Perkins.

    .. change::
        :tags: bug, mysql
        :tickets: 3766

        Fixed bug where the "literal_binds" flag would not be propagated
        to a CAST expression under MySQL.

    .. change::
        :tags: bug, sql, postgresql, mysql
        :tickets: 3765

        Fixed regression in JSON datatypes where the "literal processor" for
        a JSON index value would not be invoked.  The native String and Integer
        datatypes are now called upon from within the JSONIndexType
        and JSONPathType.  This is applied to the generic, PostgreSQL, and
        MySQL JSON types and also has a dependency on :ticket:`3766`.

    .. change::
        :tags: change, orm

        Passing False to :meth:`_query.Query.order_by` in order to cancel
        all order by's is deprecated; there is no longer any difference
        between calling this method with False or with None.

    .. change::
        :tags: feature, orm

        The :meth:`_query.Query.group_by` method now resets the group by collection
        if an argument of ``None`` is passed, in the same way that
        :meth:`_query.Query.order_by` has worked for a long time.  Pull request
        courtesy Iuri Diniz.

    .. change::
        :tags: bug, sql
        :tickets: 3763

        Fixed bug where :class:`.Index` would fail to extract columns from
        compound SQL expressions if those SQL expressions were wrapped inside
        of an ORM-style ``__clause_element__()`` construct.  This bug
        exists in 1.0.x as well, however in 1.1 is more noticeable as
        hybrid_property @expression now returns a wrapped element.

    .. change::
        :tags: change, orm, declarative

        Constructing a declarative base class that inherits from another class
        will also inherit its docstring. This means
        :func:`~.ext.declarative.as_declarative` acts more like a normal class
        decorator.

.. changelog::
    :version: 1.1.0b3
    :released: July 26, 2016

    .. change::
        :tags: change, orm
        :tickets: 3749

        Removed a warning that dates back to 0.4 which emits when a same-named
        relationship is placed on two mappers that inherits via joined or
        single table inheritance.   The warning does not apply to the
        current unit of work implementation.

        .. seealso::

            :ref:`change_3749`


    .. change::
        :tags: bug, sql
        :tickets: 3745

        Fixed bug in new CTE feature for update/insert/delete stated
        as a CTE inside of an enclosing statement (typically SELECT) whereby
        oninsert and onupdate values weren't called upon for the embedded
        statement.

    .. change::
        :tags: bug, sql
        :tickets: 3744

        Fixed bug in new CTE feature for update/insert/delete whereby
        an anonymous (e.g. no name passed) :class:`_expression.CTE` construct around
        the statement would fail.

    .. change::
        :tags: bug, ext

        sqlalchemy.ext.indexable will intercept IndexError as well
        as KeyError when raising as AttributeError.

    .. change::
        :tags: feature, ext

        Added a "default" parameter to the new sqlalchemy.ext.indexable
        extension.

.. changelog::
    :version: 1.1.0b2
    :released: July 1, 2016

    .. change::
        :tags: bug, ext, postgresql
        :tickets: 3732

        Made a slight behavioral change in the ``sqlalchemy.ext.compiler``
        extension, whereby the existing compilation schemes for an established
        construct would be removed if that construct itself didn't already
        have its own dedicated ``__visit_name__``.  This was a
        rare occurrence in 1.0, however in 1.1 :class:`_postgresql.ARRAY`
        subclasses :class:`_types.ARRAY` and has this behavior.
        As a result, setting up a compilation handler for another dialect
        such as SQLite would render the main :class:`_postgresql.ARRAY`
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
        use exists indicates there may be more unknown-string-comparison use
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

        Added TABLESAMPLE support via the new :meth:`_expression.FromClause.tablesample`
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

        :meth:`_expression.FromClause.count` is deprecated.  This function makes use of
        an arbitrary column in the table and is not reliable; for Core use,
        ``func.count()`` should be preferred.

    .. change::
        :tags: feature, postgresql
        :tickets: 3529

        Added support for PostgreSQL's INSERT..ON CONFLICT using a new
        PostgreSQL-specific :class:`.postgresql.dml.Insert` object.
        Pull request and extensive efforts here by Robin Thomas.

        .. seealso::

            :ref:`change_3529`

    .. change::
        :tags: feature, postgresql

        The DDL for DROP INDEX will emit "CONCURRENTLY" if the
        ``postgresql_concurrently`` flag is set upon the
        :class:`.Index` and if the database in use is detected as
        PostgreSQL version 9.2 or greater.   For CREATE INDEX, database
        version detection is also added which will omit the clause if
        PG version is less than 8.2.  Pull request courtesy Iuri de Silvio.

    .. change::
        :tags: bug, orm
        :tickets: 3708

        Fixed an issue where a many-to-one change of an object from one
        parent to another could work inconsistently when combined with
        an un-flushed modification of the foreign key attribute.  The attribute
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
        to allow specification of TABLESPACE for an index in PostgreSQL.
        Complements the same-named parameter on :class:`_schema.Table`.  Pull
        request courtesy Benjamin Bertrand.

    .. change::
        :tags: orm, feature

        Added :paramref:`.AutomapBase.prepare.schema` to the
        :meth:`.AutomapBase.prepare` method, to indicate which schema
        tables should be reflected from if not the default schema.
        Pull request courtesy Josh Marlow.

    .. change::
        :tags: feature, sqlite

        The SQLite dialect now reflects ON UPDATE and ON DELETE phrases
        within foreign key constraints.  Pull request courtesy
        Michal Petrucha.

    .. change::
        :tags: bug, mssql

        Adjustments to the mxODBC dialect to make use of the ``BinaryNull``
        symbol when appropriate in conjunction with the ``VARBINARY``
        data type.  Pull request courtesy Sheila Allen.

    .. change::
        :tags: feature, sql

        Implemented reflection of CHECK constraints for SQLite and PostgreSQL.
        This is available via the new inspector method
        :meth:`_reflection.Inspector.get_check_constraints` as well as when reflecting
        :class:`_schema.Table` objects in the form of :class:`.CheckConstraint`
        objects present in the constraints collection.  Pull request courtesy
        Alex Grönholm.

    .. change::
        :tags: feature, postgresql

        Added new parameter
        :paramref:`.GenerativeSelect.with_for_update.key_share`, which
        will render the ``FOR NO KEY UPDATE`` version of ``FOR UPDATE``
        and ``FOR KEY SHARE`` instead of ``FOR SHARE``
        on the PostgreSQL backend.  Pull request courtesy Sergey Skopin.

    .. change::
        :tags: feature, postgresql, oracle

        Added new parameter
        :paramref:`.GenerativeSelect.with_for_update.skip_locked`, which
        will render the ``SKIP LOCKED`` phrase for a ``FOR UPDATE`` or
        ``FOR SHARE`` lock on the PostgreSQL and Oracle backends.  Pull
        request courtesy Jack Zhou.

    .. change::
        :tags: change, orm
        :tickets: 3394

        The :paramref:`_orm.Mapper.order_by` parameter is deprecated.
        This is an old parameter no longer relevant to how SQLAlchemy
        works, once the Query object was introduced.  By deprecating it
        we establish that we aren't supporting non-working use cases
        and that we encourage applications to move off of the use of this
        parameter.

        .. seealso::

            :ref:`change_3394`

    .. change::
        :tags: feature, postgresql

        Added a new dialect for the PyGreSQL PostgreSQL dialect.  Thanks
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

        Added a hook in :meth:`.DDLCompiler.visit_create_table` called
        :meth:`.DDLCompiler.create_table_suffix`, allowing custom dialects
        to add keywords after the "CREATE TABLE" clause.  Pull request
        courtesy Mark Sandan.

    .. change::
        :tags: feature, sql

        Negative integer indexes are now accommodated by rows
        returned from a :class:`_engine.ResultProxy`.  Pull request courtesy
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

        Added :meth:`_expression.Select.lateral` and related constructs to allow
        for the SQL standard LATERAL keyword, currently only supported
        by PostgreSQL.

        .. seealso::

            :ref:`change_2857`

    .. change::
        :tags: feature, sql
        :tickets: 1957

        Added support for rendering "FULL OUTER JOIN" to both Core and ORM.
        Pull request courtesy Stefan Urbanek.

        .. seealso::

            :ref:`change_1957`

    .. change::
        :tags: feature, engine

        Added connection pool events :meth:`ConnectionEvents.close`,
        :meth:`_events.ConnectionEvents.detach`,
        :meth:`_events.ConnectionEvents.close_detached`.

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
        via :paramref:`_sa.create_engine.isolation_level` and
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
        erroneously cause the "New instance with identity X conflicts with
        persistent instance Y" error.

        .. seealso::

            :ref:`change_3677`

    .. change::
        :tags: bug, orm
        :tickets: 3662

        An improvement to the workings of :meth:`_query.Query.correlate` such
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
        :meth:`_query.Query.distinct` is combined with :meth:`_query.Query.order_by` such
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
        enumeration libraries, to the :class:`_types.Enum` datatype.
        The :class:`_types.Enum` datatype now also performs in-Python validation
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
        approximation of the construct (typically the PostgreSQL-style
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

        The ``str()`` call for :class:`_query.Query` will now take into account
        the :class:`_engine.Engine` to which the :class:`.Session` is bound, when
        generating the string form of the SQL, so that the actual SQL
        that would be emitted to the database is shown, if possible.  Previously,
        only the engine associated with the :class:`_schema.MetaData` to which the
        mappings are associated would be used, if present.  If
        no bind can be located either on the :class:`.Session` or on
        the :class:`_schema.MetaData` to which the mappings are associated, then
        the "default" dialect is used to render the SQL, as was the case
        previously.

        .. seealso::

            :ref:`change_3081`

    .. change::
        :tags: feature, sql
        :tickets: 3501

        A deep improvement to the recently added :meth:`_expression.TextClause.columns`
        method, and its interaction with result-row processing, now allows
        the columns passed to the method to be positionally matched with the
        result columns in the statement, rather than matching on name alone.
        The advantage to this includes that when linking a textual SQL statement
        to an ORM or Core table model, no system of labeling or de-duping of
        common column names needs to occur, which also means there's no need
        to worry about how label names match to ORM columns and so-forth.  In
        addition, the :class:`_engine.ResultProxy` has been further enhanced to
        map column and string keys to a row with greater precision in some
        cases.

        .. seealso::

            :ref:`change_3501` - feature overview

            :ref:`behavior_change_3501` - backwards compatibility remarks

    .. change::
        :tags: feature, engine
        :tickets: 2685

        Multi-tenancy schema translation for :class:`_schema.Table` objects is added.
        This supports the use case of an application that uses the same set of
        :class:`_schema.Table` objects in many schemas, such as schema-per-user.
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

        Added a new type to core :class:`_types.JSON`.  This is the
        base of the PostgreSQL :class:`_postgresql.JSON` type as well as that
        of the new :class:`.mysql.JSON` type, so that a PG/MySQL-agnostic
        JSON column may be used.  The type features basic index and path
        searching support.

        .. seealso::

            :ref:`change_3619`

    .. change::
        :tags: bug, sql
        :tickets: 3616

        Fixed an assertion that would raise somewhat inappropriately
        if a :class:`.Index` were associated with a :class:`_schema.Column` that
        is associated with a lower-case-t :class:`_expression.TableClause`; the
        association should be ignored for the purposes of associating
        the index with a :class:`_schema.Table`.

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
        to the PostgreSQL version of the :meth:`_reflection.Inspector.get_view_definition`
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

        The default generation functions passed to :class:`_schema.Column` objects
        are now run through "update_wrapper", or an equivalent function
        if a callable non-function is passed, so that introspection tools
        preserve the name and docstring of the wrapped function.  Pull
        request courtesy hsum.

    .. change::
        :tags: change, sql, mysql
        :tickets: 3216

        The system by which a :class:`_schema.Column` considers itself to be an
        "auto increment" column has been changed, such that autoincrement
        is no longer implicitly enabled for a :class:`_schema.Table` that has a
        composite primary key.  In order to accommodate being able to enable
        autoincrement for a composite PK member column while at the same time
        maintaining SQLAlchemy's long standing behavior of enabling
        implicit autoincrement for a single integer primary key, a third
        state has been added to the :paramref:`_schema.Column.autoincrement` parameter
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

        Added support to the SQLite dialect for the
        :meth:`_reflection.Inspector.get_schema_names` method to work with SQLite;
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

        The use of a :class:`_postgresql.ARRAY` object that refers
        to a :class:`_types.Enum` or :class:`_postgresql.ENUM` subtype
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
        to ensure that the constraints or PostgreSQL types (e.g. ENUM)
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

        Added support for the SQL-standard function :class:`_functions.array_agg`,
        which automatically returns an :class:`_postgresql.ARRAY` of the correct type
        and supports index / slice operations, as well as
        :func:`_postgresql.array_agg`, which returns a :class:`_postgresql.ARRAY`
        with additional comparison features.   As arrays are only
        supported on PostgreSQL at the moment, only actually works on
        PostgreSQL.  Also added a new construct
        :class:`_postgresql.aggregate_order_by` in support of PG's
        "ORDER BY" extension.

        .. seealso::

            :ref:`change_3132`

    .. change::
        :tags: feature, sql
        :tickets: 3516

        Added a new type to core :class:`_types.ARRAY`.  This is the
        base of the PostgreSQL :class:`_postgresql.ARRAY` type, and is now part of Core
        to begin supporting various SQL-standard array-supporting features
        including some functions and eventual support for native arrays
        on other databases that have an "array" concept, such as DB2 or Oracle.
        Additionally, new operators :func:`_expression.any_` and
        :func:`_expression.all_` have been added.  These support not just
        array constructs on PostgreSQL, but also subqueries that are usable
        on MySQL (but sadly not on PostgreSQL).

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

        The "hashable" flag on special datatypes such as :class:`_postgresql.ARRAY`,
        :class:`_postgresql.JSON` and :class:`_postgresql.HSTORE` is now
        set to False, which allows these types to be fetchable in ORM
        queries that include entities within the row.

        .. seealso::

            :ref:`change_3499`

            :ref:`change_3499_postgresql`

    .. change::
        :tags: bug, postgresql
        :tickets: 3487

        The PostgreSQL :class:`_postgresql.ARRAY` type now supports multidimensional
        indexed access, e.g. expressions such as ``somecol[5][6]`` without
        any need for explicit casts or type coercions, provided
        that the :paramref:`.postgresql.ARRAY.dimensions` parameter is set to the
        desired number of dimensions.

        .. seealso::

            :ref:`change_3503`

    .. change::
        :tags: bug, postgresql
        :tickets: 3503

        The return type for the :class:`_postgresql.JSON` and :class:`_postgresql.JSONB`
        when using indexed access has been fixed to work like PostgreSQL itself,
        and returns an expression that itself is of type :class:`_postgresql.JSON`
        or :class:`_postgresql.JSONB`.  Previously, the accessor would return
        :class:`.NullType` which disallowed subsequent JSON-like operators to be
        used.

        .. seealso::

            :ref:`change_3503`

    .. change::
        :tags: bug, postgresql
        :tickets: 3503

        The :class:`_postgresql.JSON`, :class:`_postgresql.JSONB` and
        :class:`_postgresql.HSTORE` datatypes now allow full control over the
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
        calls upon :meth:`_expression.ColumnElement.cast` implicitly, as PG's JSON/JSONB
        types allow cross-casting between each other as well.  Code that
        makes use of :meth:`_expression.ColumnElement.cast` on JSON indexed access,
        e.g. ``col[someindex].cast(Integer)``, will need to be changed
        to call :attr:`.postgresql.JSON.Comparator.astext` explicitly.

        .. seealso::

            :ref:`change_3503_cast`


    .. change::
        :tags: bug, orm, postgresql
        :tickets: 3514

        Additional fixes have been made regarding the value of ``None``
        in conjunction with the PostgreSQL :class:`_postgresql.JSON` type.  When
        the :paramref:`_types.JSON.none_as_null` flag is left at its default
        value of ``False``, the ORM will now correctly insert the JSON
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

        The behavior of the :func:`_expression.union` construct and related constructs
        such as :meth:`_query.Query.union` now handle the case where the embedded
        SELECT statements need to be parenthesized due to the fact that they
        include LIMIT, OFFSET and/or ORDER BY.   These queries **do not work
        on SQLite**, and will fail on that backend as they did before, but
        should now work on all other backends.

        .. seealso::

            :ref:`change_2528`

    .. change::
        :tags: feature, orm
        :tickets: 3512

        Added new relationship loading strategy :func:`_orm.raiseload` (also
        accessible via ``lazy='raise'``).  This strategy behaves almost like
        :func:`_orm.noload` but instead of returning ``None`` it raises an
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
