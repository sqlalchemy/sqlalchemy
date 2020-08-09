=============
1.2 Changelog
=============

.. changelog_imports::

    .. include:: changelog_11.rst
        :start-line: 5


    .. include:: changelog_10.rst
        :start-line: 5


.. changelog::
    :version: 1.2.19
    :released: April 15, 2019

    .. change::
       :tags: bug, orm
       :tickets: 4507

       Fixed a regression in 1.2 due to the introduction of baked queries for
       relationship lazy loaders, where a race condition is created during the
       generation of the "lazy clause" which occurs within a memoized attribute. If
       two threads initialize the memoized attribute concurrently, the baked query
       could be generated with bind parameter keys that are then replaced with new
       keys by the next run, leading to a lazy load query that specifies the
       related criteria as ``None``. The fix establishes that the parameter names
       are fixed before the new clause and parameter objects are generated, so that
       the names are the same every time.

    .. change::
       :tags: bug, oracle
       :tickets: 4506

       Added support for reflection of the :class:`_types.NCHAR` datatype to the Oracle
       dialect, and added :class:`_types.NCHAR` to the list of types exported by the
       Oracle dialect.


    .. change::
       :tags: bug, examples
       :tickets: 4528

       Fixed bug in large_resultsets example case where a re-named "id" variable
       due to code reformatting caused the test to fail.  Pull request courtesy
       Matt Schuchhardt.

    .. change::
       :tags: bug, mssql
       :tickets: 4536
       :versions: 1.3.1

       A commit() is emitted after an isolation level change to SNAPSHOT, as both
       pyodbc and pymssql open an implicit transaction which blocks subsequent SQL
       from being emitted in the current transaction.

    .. change::
       :tags: bug, engine
       :tickets: 4406

       Comparing two objects of :class:`.URL` using ``__eq__()`` did not take port
       number into consideration, two objects differing only by port number were
       considered equal. Port comparison is now added in ``__eq__()`` method of
       :class:`.URL`, objects differing by port number are now not equal.
       Additionally, ``__ne__()`` was not implemented for :class:`.URL` which
       caused unexpected result when ``!=`` was used in Python2, since there are no
       implied relationships among the comparison operators in Python2.

.. changelog::
    :version: 1.2.18
    :released: February 15, 2019

    .. change::
       :tags: bug, orm
       :tickets: 4468

       Fixed a regression in 1.2 where a wildcard/load_only loader option would
       not work correctly against a loader path where of_type() were used to limit
       to a particular subclass.  The fix only works for of_type() of a simple
       subclass so far, not a with_polymorphic entity which will be addressed in a
       separate issue; it is unlikely this latter case was working previously.


    .. change::
       :tags: bug, orm
       :tickets: 4489

       Fixed fairly simple but critical issue where the
       :meth:`.SessionEvents.pending_to_persistent` event would be invoked for
       objects not just when they move from pending to persistent, but when they
       were also already persistent and just being updated, thus causing the event
       to be invoked for all objects on every update.

    .. change::
       :tags: bug, sql
       :tickets: 4485

       Fixed issue where the :class:`_types.JSON` type had a read-only
       :attr:`_types.JSON.should_evaluate_none` attribute, which would cause failures
       when making use of the :meth:`.TypeEngine.evaluates_none` method in
       conjunction with this type.  Pull request courtesy Sanjana S.

    .. change::
       :tags: bug, mssql
       :tickets: 4499

       Fixed bug where the SQL Server "IDENTITY_INSERT" logic that allows an INSERT
       to proceed with an explicit value on an IDENTITY column was not detecting
       the case where :meth:`_expression.Insert.values` were used with a dictionary that
       contained a :class:`_schema.Column` as key and a SQL expression as a value.

    .. change::
       :tags: bug, sqlite
       :tickets: 4474

       Fixed bug in SQLite DDL where using an expression as a server side default
       required that it be contained within parenthesis to be accepted by the
       sqlite parser.  Pull request courtesy Bartlomiej Biernacki.

    .. change::
       :tags: bug, mysql
       :tickets: 4492

       Fixed a second regression caused by :ticket:`4344` (the first was
       :ticket:`4361`), which works around MySQL issue 88718, where the lower
       casing function used was not correct for Python 2 with OSX/Windows casing
       conventions, which would then raise ``TypeError``.  Full coverage has been
       added to this logic so that every codepath is exercised in a mock style for
       all three casing conventions on all versions of Python. MySQL 8.0 has
       meanwhile fixed issue 88718 so the workaround is only applies to a
       particular span of MySQL 8.0 versions.

.. changelog::
    :version: 1.2.17
    :released: January 25, 2019

    .. change::
       :tags: feature, orm
       :tickets: 4461

       Added new event hooks :meth:`.QueryEvents.before_compile_update` and
       :meth:`.QueryEvents.before_compile_delete` which complement
       :meth:`.QueryEvents.before_compile` in the case of the :meth:`_query.Query.update`
       and :meth:`_query.Query.delete` methods.


    .. change::
       :tags: bug, postgresql
       :tickets: 4463

       Revised the query used when reflecting CHECK constraints to make use of the
       ``pg_get_constraintdef`` function, as the ``consrc`` column is being
       deprecated in PG 12.  Thanks to John A Stevenson for the tip.


    .. change::
       :tags: bug, orm
       :tickets: 4454

       Fixed issue where when using single-table inheritance in conjunction with a
       joined inheritance hierarchy that uses "with polymorphic" loading, the
       "single table criteria" for that single-table entity could get confused for
       that of other entities from the same hierarchy used in the same query.The
       adaption of the "single table criteria" is made more specific to the target
       entity to avoid it accidentally getting adapted to other tables in the
       query.


    .. change::
       :tags: bug, oracle
       :tickets: 4457

       Fixed regression in integer precision logic due to the refactor of the
       cx_Oracle dialect in 1.2.  We now no longer apply the cx_Oracle.NATIVE_INT
       type to result columns sending integer values (detected as positive
       precision with scale ==0) which encounters integer overflow issues with
       values that go beyond the 32 bit boundary.  Instead, the output variable
       is left untyped so that cx_Oracle can choose the best option.

.. changelog::
    :version: 1.2.16
    :released: January 11, 2019

    .. change::
       :tag: bug, sql
       :tickets: 4394

       Fixed issue in "expanding IN" feature where using the same bound parameter
       name more than once in a query would lead to a KeyError within the process
       of rewriting the parameters in the query.

    .. change::
       :tags: bug, postgresql
       :tickets: 4416

       Fixed issue where a :class:`_postgresql.ENUM` or a custom domain present
       in a remote schema would not be recognized within column reflection if
       the name of the enum/domain or the name of the schema required quoting.
       A new parsing scheme now fully parses out quoted or non-quoted tokens
       including support for SQL-escaped quotes.

    .. change::
       :tags: bug, postgresql

       Fixed issue where multiple :class:`_postgresql.ENUM` objects referred to
       by the same :class:`_schema.MetaData` object would fail to be created if
       multiple objects had the same name under different schema names.  The
       internal memoization the PostgreSQL dialect uses to track if it has
       created a particular :class:`_postgresql.ENUM` in the database during
       a DDL creation sequence now takes schema name into account.

    .. change::
       :tags: bug, engine
       :tickets: 4429

       Fixed a regression introduced in version 1.2 where a refactor
       of the :class:`.SQLAlchemyError` base exception class introduced an
       inappropriate coercion of a plain string message into Unicode under
       python 2k, which is not handled by the Python interpreter for characters
       outside of the platform's encoding (typically ascii).  The
       :class:`.SQLAlchemyError` class now passes a bytestring through under
       Py2K for ``__str__()`` as is the behavior of exception objects in general
       under Py2K, does a safe coercion to unicode utf-8 with
       backslash fallback for ``__unicode__()``.  For Py3K the message is
       typically unicode already, but if not is again safe-coerced with utf-8
       with backslash fallback for the ``__str__()`` method.

    .. change::
       :tags: bug, sql, oracle, mysql
       :tickets: 4436

       Fixed issue where the DDL emitted for :class:`.DropTableComment`, which
       will be used by an upcoming version of Alembic, was incorrect for the MySQL
       and Oracle databases.

    .. change::
       :tags: bug, sqlite
       :tickets: 4431

       Reflection of an index based on SQL expressions are now skipped with a
       warning, in the same way as that of the Postgresql dialect, where we currently
       do not support reflecting indexes that have SQL expressions within them.
       Previously, an index with columns of None were produced which would break
       tools like Alembic.

.. changelog::
    :version: 1.2.15
    :released: December 11, 2018

    .. change::
        :tags: bug, orm
        :tickets: 4367

        Fixed bug where the ORM annotations could be incorrect for the
        primaryjoin/secondaryjoin a relationship if one used the pattern
        ``ForeignKey(SomeClass.id)`` in the declarative mappings.   This pattern
        would leak undesired annotations into the join conditions which can break
        aliasing operations done within :class:`_query.Query` that are not supposed to
        impact elements in that join condition.  These annotations are now removed
        up front if present.

    .. change::
       :tags: bug, orm, declarative
       :tickets: 4374

       A warning is emitted in the case that a :func:`_expression.column` object is applied to
       a declarative class, as it seems likely this intended to be a
       :class:`_schema.Column` object.

    .. change::
        :tags: bug, orm
        :tickets: 4366

        In continuing with a similar theme as that of very recent :ticket:`4349`,
        repaired issue with :meth:`.RelationshipProperty.Comparator.any` and
        :meth:`.RelationshipProperty.Comparator.has` where the "secondary"
        selectable needs to be explicitly part of the FROM clause in the
        EXISTS subquery to suit the case where this "secondary" is a :class:`_expression.Join`
        object.

    .. change::
        :tags: bug, orm
        :tickets: 4363

        Fixed regression caused by :ticket:`4349` where adding the "secondary"
        table to the FROM clause for a dynamic loader would affect the ability of
        the :class:`_query.Query` to make a subsequent join to another entity.   The fix
        adds the primary entity as the first element of the FROM list since
        :meth:`_query.Query.join` wants to jump from that.   Version 1.3 will have
        a more comprehensive solution to this problem as well (:ticket:`4365`).




    .. change::
       :tags: bug, orm
       :tickets: 4400

       Fixed bug where chaining of mapper options using
       :meth:`.RelationshipProperty.of_type` in conjunction with a chained option
       that refers to an attribute name by string only would fail to locate the
       attribute.

    .. change::
        :tag: feature, mysql
        :tickets: 4381

        Added support for the ``write_timeout`` flag accepted by mysqlclient and
        pymysql to  be passed in the URL string.

    .. change::
       :tag: bug, postgresql
       :tickets: 4377, 4380

       Fixed issue where reflection of a PostgreSQL domain that is expressed as an
       array would fail to be recognized.  Pull request courtesy Jakub Synowiec.


.. changelog::
    :version: 1.2.14
    :released: November 10, 2018

    .. change::
       :tags: bug, orm
       :tickets: 4357

       Fixed bug in :meth:`.Session.bulk_update_mappings` where alternate mapped
       attribute names would result in the primary key column of the UPDATE
       statement being included in the SET clause, as well as the WHERE clause;
       while usually harmless, for SQL Server this can raise an error due to the
       IDENTITY column.  This is a continuation of the same bug that was fixed in
       :ticket:`3849`, where testing was insufficient to catch this additional
       flaw.

    .. change::
        :tags: bug, mysql
        :tickets: 4361

        Fixed regression caused by :ticket:`4344` released in 1.2.13, where the fix
        for MySQL 8.0's case sensitivity problem with referenced column names when
        reflecting foreign key referents is worked around using the
        ``information_schema.columns`` view.  The workaround was failing on OSX /
        ``lower_case_table_names=2`` which produces non-matching casing for the
        ``information_schema.columns`` vs. that of ``SHOW CREATE TABLE``, so in
        case-insensitive SQL modes case-insensitive matching is now used.

    .. change::
       :tags: bug, orm
       :tickets: 4347

       Fixed a minor performance issue which could in some cases add unnecessary
       overhead to result fetching, involving the use of ORM columns and entities
       that include those same columns at the same time within a query.  The issue
       has to do with hash / eq overhead when referring to the column in different
       ways.

.. changelog::
    :version: 1.2.13
    :released: October 31, 2018

    .. change::
       :tags: bug, postgresql
       :tickets: 4337

       Added support for the :class:`.aggregate_order_by` function to receive
       multiple ORDER BY elements, previously only a single element was accepted.


    .. change::
       :tags: bug, mysql
       :tickets: 4348

       Added word ``function`` to the list of reserved words for MySQL, which is
       now a keyword in MySQL 8.0

    .. change::
        :tags: feature, sql
        :versions: 1.3.0b1

        Refactored :class:`.SQLCompiler` to expose a
        :meth:`.SQLCompiler.group_by_clause` method similar to the
        :meth:`.SQLCompiler.order_by_clause` and :meth:`.SQLCompiler.limit_clause`
        methods, which can be overridden by dialects to customize how GROUP BY
        renders.  Pull request courtesy Samuel Chou.

    .. change::
       :tags: bug, misc

       Fixed issue where part of the utility language helper internals was passing
       the wrong kind of argument to the Python ``__import__`` builtin as the list
       of modules to be imported.  The issue produced no symptoms within the core
       library but could cause issues with external applications that redefine the
       ``__import__`` builtin or otherwise instrument it. Pull request courtesy Joe
       Urciuoli.

    .. change::
       :tags: bug, orm
       :tickets: 4349

       Fixed bug where "dynamic" loader needs to explicitly set the "secondary"
       table in the FROM clause of the query, to suit the case where the secondary
       is a join object that is otherwise not pulled into the query from its
       columns alone.


    .. change::
       :tags: bug, orm, declarative
       :tickets: 4350

       Fixed regression caused by :ticket:`4326` in version 1.2.12 where using
       :class:`.declared_attr` with a mixin in conjunction with
       :func:`_orm.synonym` would fail to map the synonym properly to an inherited
       subclass.

    .. change::
       :tags: bug, misc, py3k
       :tickets: 4339

       Fixed additional warnings generated by Python 3.7 due to changes in the
       organization of the Python ``collections`` and ``collections.abc`` packages.
       Previous ``collections`` warnings were fixed in version 1.2.11. Pull request
       courtesy xtreak.

    .. change::
       :tags: bug, ext

       Added missing ``.index()`` method to list-based association collections
       in the association proxy extension.

    .. change::
       :tags: bug, mysql
       :tickets: 4344

       Added a workaround for a MySQL bug #88718 introduced in the 8.0 series,
       where the reflection of a foreign key constraint is not reporting the
       correct case sensitivity for the referred column, leading to errors during
       use of the reflected constraint such as when using the automap extension.
       The workaround emits an additional query to the information_schema tables in
       order to retrieve the correct case sensitive name.

    .. change::
       :tags: bug, sql
       :tickets: 4341

       Fixed bug where the :paramref:`.Enum.create_constraint` flag on  the
       :class:`.Enum` datatype would not be propagated to copies of the type, which
       affects use cases such as declarative mixins and abstract bases.

    .. change::
       :tags: bug, orm, declarative
       :tickets: 4352

       The column conflict resolution technique discussed at
       :ref:`declarative_column_conflicts` is now functional for a :class:`_schema.Column`
       that is also a primary key column.  Previously, a check for primary key
       columns declared on a single-inheritance subclass would occur before the
       column copy were allowed to pass.


.. changelog::
    :version: 1.2.12
    :released: September 19, 2018

    .. change::
        :tags: bug, postgresql
        :tickets: 4325

        Fixed bug in PostgreSQL dialect where compiler keyword arguments such as
        ``literal_binds=True`` were not being propagated to a DISTINCT ON
        expression.

    .. change::
        :tags: bug, ext
        :tickets: 4328

        Fixed issue where :class:`.BakedQuery` did not include the specific query
        class used by the :class:`.Session` as part of the cache key, leading to
        incompatibilities when using custom query classes, in particular the
        :class:`.ShardedQuery` which has some different argument signatures.

    .. change::
        :tags: bug, postgresql
        :tickets: 4324

        Fixed the :func:`_postgresql.array_agg` function, which is a slightly
        altered version of the usual :func:`_functions.array_agg` function, to also
        accept an incoming "type" argument without forcing an ARRAY around it,
        essentially the same thing that was fixed for the generic function in 1.1
        in :ticket:`4107`.

    .. change::
        :tags: bug, postgresql
        :tickets: 4323

        Fixed bug in PostgreSQL ENUM reflection where a case-sensitive, quoted name
        would be reported by the query including quotes, which would not match a
        target column during table reflection as the quotes needed to be stripped
        off.


    .. change::
       :tags: bug, orm

       Added a check within the weakref cleanup for the :class:`.InstanceState`
       object to check for the presence of the ``dict`` builtin, in an effort to
       reduce error messages generated when these cleanups occur during interpreter
       shutdown.  Pull request courtesy Romuald Brunet.

    .. change::
        :tags: bug, orm, declarative
        :tickets: 4326

        Fixed bug where the declarative scan for attributes would receive the
        expression proxy delivered by a hybrid attribute at the class level, and
        not the hybrid attribute itself, when receiving the descriptor via the
        ``@declared_attr`` callable on a subclass of an already-mapped class. This
        would lead to an attribute that did not report itself as a hybrid when
        viewed within :attr:`_orm.Mapper.all_orm_descriptors`.


    .. change::
        :tags: bug, orm
        :tickets: 4334
        :versions: 1.3.0b1

        Fixed bug where use of :class:`_expression.Lateral` construct in conjunction with
        :meth:`_query.Query.join` as well as :meth:`_query.Query.select_entity_from` would not
        apply clause adaption to the right side of the join.   "lateral" introduces
        the use case of the right side of a join being correlatable.  Previously,
        adaptation of this clause wasn't considered.   Note that in 1.2 only,
        a selectable introduced by :meth:`_query.Query.subquery` is still not adapted
        due to :ticket:`4304`; the selectable needs to be produced by the
        :func:`_expression.select` function to be the right side of the "lateral" join.

    .. change::
       :tags: bug, oracle
       :tickets: 4335

       Fixed issue for cx_Oracle 7.0 where the behavior of Oracle param.getvalue()
       now returns a list, rather than a single scalar value, breaking
       autoincrement logic throughout the Core and ORM. The dml_ret_array_val
       compatibility flag is used for cx_Oracle 6.3 and 6.4 to establish compatible
       behavior with 7.0 and forward, for cx_Oracle 6.2.1 and prior a version
       number check falls back to the old logic.


    .. change::
        :tags: bug, orm
        :tickets: 4327

        Fixed 1.2 regression caused by :ticket:`3472` where the handling of an
        "updated_at" style column within the context of a post-update operation
        would also occur for a row that is to be deleted following the update,
        meaning both that a column with a Python-side value generator would show
        the now-deleted value that was emitted for the UPDATE before the DELETE
        (which was not the previous behavior), as well as that a SQL- emitted value
        generator would have the attribute expired, meaning the previous value
        would be unreachable due to the row having been deleted and the object
        detached from the session.The "postfetch" logic that was added as part of
        :ticket:`3472` is now skipped entirely for an object that ultimately is to
        be deleted.

.. changelog::
    :version: 1.2.11
    :released: August 20, 2018

    .. change::
        :tags: bug, py3k

        Started importing "collections" from "collections.abc" under Python 3.3 and
        greater for Python 3.8 compatibility.  Pull request courtesy Nathaniel
        Knight.

    .. change::
        :tag: bug, sqlite

        Fixed issue where the "schema" name used for a SQLite database within table
        reflection would not quote the schema name correctly.  Pull request
        courtesy Phillip Cloud.

    .. change::
        :tags: bug, sql
        :tickets: 4320

        Fixed issue that is closely related to :ticket:`3639` where an expression
        rendered in a boolean context on a non-native boolean backend would
        be compared to 1/0 even though it is already an implicitly boolean
        expression, when :meth:`_expression.ColumnElement.self_group` were used.  While this
        does not affect the user-friendly backends (MySQL, SQLite) it was not
        handled by Oracle (and possibly SQL Server).   Whether or not the
        expression is implicitly boolean on any database is now determined
        up front as an additional check to not generate the integer comparison
        within the compilation of the statement.

    .. change::
        :tags: bug, oracle
        :tickets: 4309

        For cx_Oracle, Integer datatypes will now be bound to "int", per advice
        from the cx_Oracle developers.  Previously, using cx_Oracle.NUMBER caused a
        loss in precision within the cx_Oracle 6.x series.


    .. change::
        :tags: bug, orm, declarative
        :tickets: 4321

        Fixed issue in previously untested use case, allowing a declarative mapped
        class to inherit from a classically-mapped class outside of the declarative
        base, including that it accommodates for unmapped intermediate classes. An
        unmapped intermediate class may specify ``__abstract__``, which is now
        interpreted correctly, or the intermediate class can remain unmarked, and
        the classically mapped base class will be detected within the hierarchy
        regardless. In order to anticipate existing scenarios which may be mixing
        in classical mappings into existing declarative hierarchies, an error is
        now raised if multiple mapped bases are detected for a given class.

    .. change::
        :tags: bug, sql
        :tickets: 4322

        Added missing window function parameters
        :paramref:`.WithinGroup.over.range_` and :paramref:`.WithinGroup.over.rows`
        parameters to the :meth:`.WithinGroup.over` and
        :meth:`.FunctionFilter.over` methods, to correspond to the range/rows
        feature added to the "over" method of SQL functions as part of
        :ticket:`3049` in version 1.1.

    .. change::
        :tags: bug, sql
        :tickets: 4313

        Fixed bug where the multi-table support for UPDATE and DELETE statements
        did not consider the additional FROM elements as targets for correlation,
        when a correlated SELECT were also combined with the statement.  This
        change now includes that a SELECT statement in the WHERE clause for such a
        statement will try to auto-correlate back to these additional tables in the
        parent UPDATE/DELETE or unconditionally correlate if
        :meth:`_expression.Select.correlate` is used.  Note that auto-correlation raises an
        error if the SELECT statement would have no FROM clauses as a result, which
        can now occur if the parent UPDATE/DELETE specifies the same tables in its
        additional set of tables; specify :meth:`_expression.Select.correlate` explicitly to
        resolve.

.. changelog::
    :version: 1.2.10
    :released: July 13, 2018

    .. change::
        :tags: bug, sql
        :tickets: 4300

        Fixed bug where a :class:`.Sequence` would be dropped explicitly before any
        :class:`_schema.Table` that refers to it, which breaks in the case when the
        sequence is also involved in a server-side default for that table, when
        using :meth:`_schema.MetaData.drop_all`.   The step which processes sequences
        to be dropped via non server-side column default functions is now invoked
        after the table itself is dropped.

    .. change::
        :tags: bug, orm
        :tickets: 4295

        Fixed bug in :class:`.Bundle` construct where placing two columns of the
        same name would be de-duplicated, when the :class:`.Bundle` were used as
        part of the rendered SQL, such as in the ORDER BY or GROUP BY of the statement.


    .. change::
        :tags: bug, orm
        :tickets: 4298

        Fixed regression in 1.2.9 due to :ticket:`4287` where using a
        :class:`_orm.Load` option in conjunction with a string wildcard would result
        in a TypeError.

.. changelog::
    :version: 1.2.9
    :released: June 29, 2018

    .. change::
        :tags: bug, mysql

        Fixed percent-sign doubling in mysql-connector-python dialect, which does
        not require de-doubling of percent signs.   Additionally, the  mysql-
        connector-python driver is inconsistent in how it passes the column names
        in cursor.description, so a workaround decoder has been added to
        conditionally decode these randomly-sometimes-bytes values to unicode only
        if needed.  Also improved test support for mysql-connector-python, however
        it should be noted that this driver still has issues with unicode that
        continue to be unresolved as of yet.


    .. change::
        :tags: bug, mssql
        :tickets: 4288

        Fixed bug in MSSQL reflection where when two same-named tables in different
        schemas had same-named primary key constraints, foreign key constraints
        referring to one of the tables would have their columns doubled, causing
        errors.   Pull request courtesy Sean Dunn.

    .. change::
        :tags: bug, sql
        :tickets: 4279

        Fixed regression in 1.2 due to :ticket:`4147` where a :class:`_schema.Table` that
        has had some of its indexed columns redefined with new ones, as would occur
        when overriding columns during reflection or when using
        :paramref:`_schema.Table.extend_existing`, such that the :meth:`_schema.Table.tometadata`
        method would fail when attempting to copy those indexes as they still
        referred to the replaced column.   The copy logic now accommodates for this
        condition.


    .. change::
        :tags: bug, mysql
        :tickets: 4293

        Fixed bug in index reflection where on MySQL 8.0 an index that includes
        ASC or DESC in an indexed column specification would not be correctly
        reflected, as MySQL 8.0 introduces support for returning this information
        in a table definition string.

    .. change::
        :tags: bug, orm
        :tickets: 3505

        Fixed issue where chaining multiple join elements inside of
        :meth:`_query.Query.join` might not correctly adapt to the previous left-hand
        side, when chaining joined inheritance classes that share the same base
        class.

    .. change::
        :tags: bug, orm
        :tickets: 4287

        Fixed bug in cache key generation for baked queries which could cause a
        too-short cache key to be generated for the case of eager loads across
        subclasses.  This could in turn cause the eagerload query to be cached in
        place of a non-eagerload query, or vice versa, for a polymorphic "selectin"
        load, or possibly for lazy loads or selectin loads as well.

    .. change::
        :tags: bug, sqlite

        Fixed issue in test suite where SQLite 3.24 added a new reserved word that
        conflicted with a usage in TypeReflectionTest.  Pull request courtesy Nils
        Philippsen.

    .. change::
        :tags: feature, oracle
        :tickets: 4290
        :versions: 1.3.0b1

        Added a new event currently used only by the cx_Oracle dialect,
        :meth:`.DialectEvents.setiputsizes`.  The event passes a dictionary of
        :class:`.BindParameter` objects to DBAPI-specific type objects that will be
        passed, after conversion to parameter names, to the cx_Oracle
        ``cursor.setinputsizes()`` method.  This allows both visibility into the
        setinputsizes process as well as the ability to alter the behavior of what
        datatypes are passed to this method.

        .. seealso::

            :ref:`cx_oracle_setinputsizes`

    .. change::
        :tags: bug, orm
        :tickets: 4286

        Fixed bug in new polymorphic selectin loading where the BakedQuery used
        internally would be mutated by the given loader options, which would both
        inappropriately mutate the subclass query as well as carry over the effect
        to subsequent queries.

    .. change::
        :tags: bug, py3k
        :tickets: 4291

        Replaced the usage of inspect.formatargspec() with a vendored version
        copied from the Python standard library, as inspect.formatargspec()
        is deprecated and as of Python 3.7.0 is emitting a warning.

    .. change::
        :tags: feature, ext
        :tickets: 4243
        :versions: 1.3.0b1

        Added new attribute :attr:`_query.Query.lazy_loaded_from` which is populated
        with an :class:`.InstanceState` that is using this :class:`_query.Query` in
        order to lazy load a relationship.  The rationale for this is that
        it serves as a hint for the horizontal sharding feature to use, such that
        the identity token of the state can be used as the default identity token
        to use for the query within id_chooser().

    .. change::
        :tags: bug, mysql
        :tickets: 4283

        Fixed bug in MySQLdb dialect and variants such as PyMySQL where an
        additional "unicode returns" check upon connection makes explicit use of
        the "utf8" character set, which in MySQL 8.0 emits a warning that utf8mb4
        should be used.  This is now replaced with a utf8mb4 equivalent.
        Documentation is also updated for the MySQL dialect to specify utf8mb4 in
        all examples.  Additional changes have been made to the test suite to use
        utf8mb3 charsets and databases (there seem to be collation issues in some
        edge cases with utf8mb4), and to support configuration default changes made
        in MySQL 8.0 such as explicit_defaults_for_timestamp as well as new errors
        raised for invalid MyISAM indexes.



    .. change::
        :tags: bug, mysql
        :tickets: 3645

        The :class:`_expression.Update` construct now accommodates a :class:`_expression.Join` object
        as supported by MySQL for UPDATE..FROM.  As the construct already
        accepted an alias object for a similar purpose, the feature of UPDATE
        against a non-table was already implied so this has been added.

    .. change::
        :tags: bug, mssql, py3k
        :tickets: 4273

        Fixed issue within the SQL Server dialect under Python 3 where when running
        against a non-standard SQL server database that does not contain either the
        "sys.dm_exec_sessions" or "sys.dm_pdw_nodes_exec_sessions" views, leading
        to a failure to fetch the isolation level, the error raise would fail due
        to an UnboundLocalError.



    .. change::
        :tags: bug, orm
        :tickets: 4269

        Fixed regression caused by :ticket:`4256` (itself a regression fix for
        :ticket:`4228`) which breaks an undocumented behavior which converted for a
        non-sequence of entities passed directly to the :class:`_query.Query` constructor
        into a single-element sequence.  While this behavior was never supported or
        documented, it's already in use so has been added as a behavioral contract
        to :class:`_query.Query`.

    .. change::
        :tags: bug, orm
        :tickets: 4270

        Fixed an issue that was both a performance regression in 1.2 as well as an
        incorrect result regarding the "baked" lazy loader, involving the
        generation of cache keys from the original :class:`_query.Query` object's loader
        options.  If the loader options were built up in a "branched" style using
        common base elements for multiple options, the same options would be
        rendered into the cache key repeatedly, causing both a performance issue as
        well as generating the wrong cache key.  This is fixed, along with a
        performance improvement when such "branched" options are applied via
        :meth:`_query.Query.options` to prevent the same option objects from being
        applied repeatedly.

    .. change::
        :tags: bug, oracle, mysql
        :tickets: 4275

        Fixed INSERT FROM SELECT with CTEs for the Oracle and MySQL dialects, where
        the CTE was being placed above the entire statement as is typical with
        other databases, however Oracle and MariaDB 10.2 wants the CTE underneath
        the "INSERT" segment. Note that the Oracle and MySQL dialects don't yet
        work when a CTE is applied to a subquery inside of an UPDATE or DELETE
        statement, as the CTE is still applied to the top rather than inside the
        subquery.


.. changelog::
    :version: 1.2.8
    :released: May 28, 2018

    .. change::
      :tags: bug, orm
      :tickets: 4256

      Fixed regression in 1.2.7 caused by :ticket:`4228`, which itself was fixing
      a 1.2-level regression, where the ``query_cls`` callable passed to a
      :class:`.Session` was assumed to be a subclass of :class:`_query.Query`  with
      class method availability, as opposed to an arbitrary callable.    In
      particular, the dogpile caching example illustrates ``query_cls`` as a
      function and not a :class:`_query.Query` subclass.

    .. change::
        :tags: bug, engine
        :tickets: 4252

        Fixed connection pool issue whereby if a disconnection error were raised
        during the connection pool's "reset on return" sequence in conjunction with
        an explicit transaction opened against the enclosing :class:`_engine.Connection`
        object (such as from calling :meth:`.Session.close` without a rollback or
        commit, or calling :meth:`_engine.Connection.close` without first closing a
        transaction declared with :meth:`_engine.Connection.begin`), a double-checkin would
        result, which could then lead towards concurrent checkouts of the same
        connection. The double-checkin condition is now prevented overall by an
        assertion, as well as the specific double-checkin scenario has been
        fixed.

    .. change::
        :tags: bug, oracle
        :tickets: 4264

        The Oracle BINARY_FLOAT and BINARY_DOUBLE datatypes now participate within
        cx_Oracle.setinputsizes(), passing along NATIVE_FLOAT, so as to support the
        NaN value.  Additionally, :class:`_oracle.BINARY_FLOAT`,
        :class:`_oracle.BINARY_DOUBLE` and :class:`_oracle.DOUBLE_PRECISION` now
        subclass :class:`.Float`, since these are floating point datatypes, not
        decimal.  These datatypes were already defaulting the
        :paramref:`.Float.asdecimal` flag to False in line with what
        :class:`.Float` already does.

    .. change::
        :tags: bug, oracle

        Added reflection capabilities for the :class:`_oracle.BINARY_FLOAT`,
        :class:`_oracle.BINARY_DOUBLE` datatypes.


    .. change::
      :tags: bug, ext
      :tickets: 4247

      The horizontal sharding extension now makes use of the identity token
      added to ORM identity keys as part of :ticket:`4137`, when an object
      refresh or column-based deferred load or unexpiration operation occurs.
      Since we know the "shard" that the object originated from, we make
      use of this value when refreshing, thereby avoiding queries against
      other shards that don't match this object's identity in any case.

    .. change::
        :tags: bug, sql

        Fixed issue where the "ambiguous literal" error message used when
        interpreting literal values as SQL expression values would encounter a
        tuple value, and fail to format the message properly. Pull request courtesy
        Miguel Ventura.

    .. change::
        :tags: bug, mssql
        :tickets: 4250

        Fixed a 1.2 regression caused by :ticket:`4061` where the SQL Server
        "BIT" type would be considered to be "native boolean".  The goal here
        was to avoid creating a CHECK constraint on the column, however the bigger
        issue is that the BIT value does not behave like a true/false constant
        and cannot be interpreted as a standalone expression, e.g.
        "WHERE <column>".   The SQL Server dialect now goes back to being
        non-native boolean, but with an extra flag that still avoids creating
        the CHECK constraint.

    .. change::
        :tags: bug, oracle
        :tickets: 4259

        Altered the Oracle dialect such that when an :class:`.Integer` type is in
        use, the cx_Oracle.NUMERIC type is set up for setinputsizes().  In
        SQLAlchemy 1.1 and earlier, cx_Oracle.NUMERIC was passed for all numeric
        types unconditionally, and in 1.2 this was removed to allow for better
        numeric precision.  However, for integers, some database/client setups
        will fail to coerce boolean values True/False into integers which introduces
        regressive behavior when using SQLAlchemy 1.2.  Overall, the setinputsizes
        logic seems like it will need a lot more flexibility going forward so this
        is a start for that.

    .. change::
        :tags: bug, engine

        Fixed a reference leak issue where the values of the parameter dictionary
        used in a statement execution would remain referenced by the "compiled
        cache", as a result of storing the key view used by Python 3 dictionary
        keys().  Pull request courtesy Olivier Grisel.

    .. change::
        :tags: bug, orm
        :tickets: 4128

        Fixed a long-standing regression that occurred in version
        1.0, which prevented the use of a custom :class:`.MapperOption`
        that alters the _params of a :class:`_query.Query` object for a
        lazy load, since the lazy loader itself would overwrite those
        parameters.   This applies to the "temporal range" example
        on the wiki.  Note however that the
        :meth:`_query.Query.populate_existing` method is now required in
        order to rewrite the mapper options associated with an object
        already loaded in the identity map.

        As part of this change, a custom defined
        :class:`.MapperOption` will now cause lazy loaders related to
        the target object to use a non-baked query by default unless
        the :meth:`.MapperOption._generate_cache_key` method is implemented.
        In particular, this repairs one regression which occurred when
        using the dogpile.cache "advanced" example, which was not
        returning cached results and instead emitting SQL due to an
        incompatibility with the baked query loader; with the change,
        the ``RelationshipCache`` option included for many releases
        in the dogpile example will disable the "baked" query altogether.
        Note that the dogpile example is also modernized to avoid both
        of these issues as part of issue :ticket:`4258`.

    .. change::
      :tags: bug, ext
      :tickets: 4266

      Fixed a race condition which could occur if automap
      :meth:`.AutomapBase.prepare` were used within a multi-threaded context
      against other threads which  may call :func:`.configure_mappers` as a
      result of use of other mappers.  The unfinished mapping work of automap
      is particularly sensitive to being pulled in by a
      :func:`.configure_mappers` step leading to errors.

    .. change::
        :tags: bug, orm

        Fixed bug where the new :meth:`.baked.Result.with_post_criteria`
        method would not interact with a subquery-eager loader correctly,
        in that the "post criteria" would not be applied to embedded
        subquery eager loaders.   This is related to :ticket:`4128` in that
        the post criteria feature is now used by the lazy loader.

    .. change::
      :tags: bug, tests
      :tickets: 4249

      Fixed a bug in the test suite where if an external dialect returned
      ``None`` for ``server_version_info``, the exclusion logic would raise an
      ``AttributeError``.

    .. change::
        :tags: bug, orm
        :tickets: 4258

        Updated the dogpile.caching example to include new structures that
        accommodate for the "baked" query system, which is used by default within
        lazy loaders and some eager relationship loaders. The dogpile.caching
        "relationship_caching" and "advanced" examples were also broken due to
        :ticket:`4256`.  The issue here is also worked-around by the fix in
        :ticket:`4128`.

.. changelog::
    :version: 1.2.7
    :released: April 20, 2018

    .. change::
        :tags: bug, orm
        :tickets: 4228

        Fixed regression in 1.2 within sharded query feature where the
        new "identity_token" element was not being correctly considered within
        the scope of a lazy load operation, when searching the identity map
        for a related many-to-one element.   The new behavior will allow for
        making use of the "id_chooser" in order to determine the best identity
        key to retrieve from the identity map.  In order to achieve this, some
        refactoring of 1.2's "identity_token" approach has made some slight changes
        to the implementation of ``ShardedQuery`` which should be noted for other
        derivations of this class.

    .. change::
        :tags: bug, postgresql
        :tickets: 4229

        Fixed bug where the special "not equals" operator for the PostgreSQL
        "range" datatypes such as DATERANGE would fail to render "IS NOT NULL" when
        compared to the Python ``None`` value.



    .. change::
        :tags: bug, mssql
        :tickets: 4234

        Fixed 1.2 regression caused by :ticket:`4060` where the query used to
        reflect SQL Server cross-schema foreign keys was limiting the criteria
        incorrectly.



    .. change::
        :tags: bug, oracle

        The Oracle NUMBER datatype is reflected as INTEGER if the precision is NULL
        and the scale is zero, as this is how INTEGER values come back when
        reflected from Oracle's tables.  Pull request courtesy Kent Bower.

    .. change::
        :tags: feature, postgresql
        :tickets: 4160
        :versions: 1.3.0b1

        Added new PG type :class:`_postgresql.REGCLASS` which assists in casting
        table names to OID values.  Pull request courtesy Sebastian Bank.

    .. change::
        :tags: bug, sql
        :tickets: 4231

        Fixed issue where the compilation of an INSERT statement with the
        "literal_binds" option that also uses an explicit sequence and "inline"
        generation, as on PostgreSQL and Oracle, would fail to accommodate the
        extra keyword argument within the sequence processing routine.

    .. change::
        :tags: bug, orm
        :tickets: 4241

        Fixed issue in single-inheritance loading where the use of an aliased
        entity against a single-inheritance subclass in conjunction with the
        :meth:`_query.Query.select_from` method would cause the SQL to be rendered with
        the unaliased table mixed in to the query, causing a cartesian product.  In
        particular this was affecting the new "selectin" loader when used against a
        single-inheritance subclass.

.. changelog::
    :version: 1.2.6
    :released: March 30, 2018

    .. change::
        :tags: bug, mssql
        :tickets: 4227

        Adjusted the SQL Server version detection for pyodbc to only allow for
        numeric tokens, filtering out non-integers, since the dialect does tuple-
        numeric comparisons with this value.  This is normally true for all known
        SQL Server / pyodbc drivers in any case.

    .. change::
        :tags: feature, postgresql

        Added support for "PARTITION BY" in PostgreSQL table definitions,
        using "postgresql_partition_by".  Pull request courtesy
        Vsevolod Solovyov.

    .. change::
        :tags: bug, sql
        :tickets: 4204

        Fixed a regression that occurred from the previous fix to :ticket:`4204` in
        version 1.2.5, where a CTE that refers to itself after the
        :meth:`_expression.CTE.alias` method has been called would not refer to itself
        correctly.

    .. change::
        :tags: bug, engine
        :tickets: 4225

        Fixed bug in connection pool where a connection could be present in the
        pool without all of its "connect" event handlers called, if a previous
        "connect" handler threw an exception; note that the dialects themselves
        have connect handlers that emit SQL, such as those which set transaction
        isolation, which can fail if the database is in a non-available state, but
        still allows a connection.  The connection is now invalidated first if any
        of the connect handlers fail.

    .. change::
        :tags: bug, oracle
        :tickets: 4211

        The minimum cx_Oracle version supported is 5.2 (June 2015).  Previously,
        the dialect asserted against version 5.0 but as of 1.2.2 we are using some
        symbols that did not appear until 5.2.

    .. change::
        :tags: bug, declarative
        :tickets: 4221

        Removed a warning that would be emitted when calling upon
        ``__table_args__``, ``__mapper_args__`` as named with a ``@declared_attr``
        method, when called from a non-mapped declarative mixin.  Calling these
        directly is documented as the approach to use when one is overriding one
        of these methods on a mapped class.  The warning still emits for regular
        attribute names.

    .. change::
        :tags: bug, orm
        :tickets: 4215

        Fixed bug where using :meth:`.Mutable.associate_with` or
        :meth:`.Mutable.as_mutable` in conjunction with a class that has non-
        primary mappers set up with alternatively-named attributes would produce an
        attribute error.  Since non-primary mappers are not used for persistence,
        the mutable extension now excludes non-primary mappers from its
        instrumentation steps.


.. changelog::
    :version: 1.2.5
    :released: March 6, 2018

    .. change::
        :tags: bug, sql
        :tickets: 4210

        Fixed bug in :class:.`CTE` construct along the same lines as that of
        :ticket:`4204` where a :class:`_expression.CTE` that was aliased would not copy itself
        correctly during a "clone" operation as is frequent within the ORM as well
        as when using the :meth:`_expression.ClauseElement.params` method.

    .. change::
        :tags: bug, orm
        :tickets: 4199

        Fixed bug in new "polymorphic selectin" loading when a selection of
        polymorphic objects were to be partially loaded from a relationship
        lazy loader, leading to an "empty IN" condition within the load that
        raises an error for the "inline" form of "IN".

    .. change::
        :tags: bug, sql
        :tickets: 4204

        Fixed bug in CTE rendering where a :class:`_expression.CTE` that was also turned into
        an :class:`_expression.Alias` would not render its "ctename AS aliasname" clause
        appropriately if there were more than one reference to the CTE in a FROM
        clause.

    .. change::
        :tags: bug, orm
        :tickets: 4209

        Fixed 1.2 regression where a mapper option that contains an
        :class:`.AliasedClass` object, as is typical when using the
        :meth:`.QueryableAttribute.of_type` method, could not be pickled.   1.1's
        behavior was to omit the aliased class objects from the path, so this
        behavior is restored.

    .. change::
        :tags: feature, orm
        :versions: 1.3.0b1

        Added new feature :meth:`_query.Query.only_return_tuples`.  Causes the
        :class:`_query.Query` object to return keyed tuple objects unconditionally even
        if the query is against a single entity.   Pull request courtesy Eric
        Atkin.


    .. change::
        :tags: bug, sql
        :tickets: 4198

        Fixed bug in new "expanding IN parameter" feature where the bind parameter
        processors for values wasn't working at all, tests failed to cover this
        pretty basic case which includes that ENUM values weren't working.

.. changelog::
    :version: 1.2.4
    :released: February 22, 2018

    .. change::
        :tags: bug, orm
        :tickets: 4193

        Fixed 1.2 regression in ORM versioning feature where a mapping against a
        :func:`_expression.select` or :func:`.alias` that also used a versioning column
        against the underlying table would fail due to the check added as part of
        :ticket:`3673`.

    .. change::
        :tags: bug, engine
        :tickets: 4190

        Fixed regression caused in 1.2.3 due to fix from :ticket:`4181` where
        the changes to the event system involving :class:`_engine.Engine` and
        :class:`.OptionEngine` did not accommodate for event removals, which
        would raise an ``AttributeError`` when invoked at the class
        level.

    .. change::
        :tags: bug, sql
        :tickets: 4197

        Fixed bug where CTE expressions would not have their name or alias name
        quoted when the given name is case sensitive or otherwise requires quoting.
        Pull request courtesy Eric Atkin.

.. changelog::
    :version: 1.2.3
    :released: February 16, 2018

    .. change::
        :tags: bug, oracle
        :tickets: 4182

        Fixed bug in cx_Oracle disconnect detection, used by pre_ping and other
        features, where an error could be raised as DatabaseError which includes a
        numeric error code; previously we weren't checking in this case for a
        disconnect code.

    .. change::
        :tags: bug, sqlite

        Fixed the import error raised when a platform
        has neither pysqlite2 nor sqlite3 installed, such
        that the sqlite3-related import error is raised,
        not the pysqlite2 one which is not the actual
        failure mode.  Pull request courtesy Robin.

    .. change::
        :tags: bug, orm
        :tickets: 4175

        Fixed bug where the :class:`.Bundle` object did not
        correctly report upon the primary :class:`_orm.Mapper` object
        represented by the bundle, if any.   An immediate
        side effect of this issue was that the new selectinload
        loader strategy wouldn't work with the horizontal sharding
        extension.

    .. change::
        :tags: bug, sql
        :tickets: 4180

        Fixed bug where the :class:`.Enum` type wouldn't handle
        enum "aliases" correctly, when more than one key refers to the
        same value.  Pull request courtesy Daniel Knell.


    .. change::
        :tags: bug, engine
        :tickets: 4181

        Fixed bug where events associated with an :class:`Engine`
        at the class level would be doubled when the
        :meth:`_engine.Engine.execution_options` method were used.  To
        achieve this, the semi-private class :class:`.OptionEngine`
        no longer accepts events directly at the class level
        and will raise an error; the class only propagates class-level
        events from its parent :class:`_engine.Engine`.   Instance-level
        events continue to work as before.

    .. change::
        :tags: bug, tests
        :tickets: 3265

        A test added in 1.2 thought to confirm a Python 2.7 behavior turns out to
        be confirming the behavior only as of Python 2.7.8. Python bug #8743 still
        impacts set comparison in Python 2.7.7 and earlier, so the test in question
        involving AssociationSet no longer runs for these older Python 2.7
        versions.

    .. change::
        :tags: feature, oracle

        The ON DELETE options for foreign keys are now part of
        Oracle reflection.  Oracle does not support ON UPDATE
        cascades.  Pull request courtesy Miroslav Shubernetskiy.



    .. change::
        :tags: bug, orm
        :tickets: 4188

        Fixed bug in concrete inheritance mapping where user-defined
        attributes such as hybrid properties that mirror the names
        of mapped attributes from sibling classes would be overwritten by
        the mapper as non-accessible at the instance level.   Additionally
        ensured that user-bound descriptors are not implicitly invoked at the class
        level during the mapper configuration stage.

    .. change::
        :tags: bug, orm
        :tickets: 4178

        Fixed bug where the :func:`_orm.reconstructor` event
        helper would not be recognized if it were applied to the
        ``__init__()`` method of the mapped class.

    .. change::
        :tags: bug, engine
        :tickets: 4170

        The :class:`.URL` object now allows query keys to be specified multiple
        times where their values will be joined into a list.  This is to support
        the plugins feature documented at :class:`.CreateEnginePlugin` which
        documents that "plugin" can be passed multiple times. Additionally, the
        plugin names can be passed to :func:`_sa.create_engine` outside of the URL
        using the new :paramref:`_sa.create_engine.plugins` parameter.

    .. change::
        :tags: feature, sql
        :tickets: 3906

        Added support for :class:`.Enum` to persist the values of the enumeration,
        rather than the keys, when using a Python pep-435 style enumerated object.
        The user supplies a callable function that will return the string values to
        be persisted.  This allows enumerations against non-string values to be
        value-persistable as well.  Pull request courtesy Jon Snyder.

    .. change::
        :tags: feature, orm

        Added new argument :paramref:`.attributes.set_attribute.inititator`
        to the :func:`.attributes.set_attribute` function, allowing an
        event token received from a listener function to be propagated
        to subsequent set events.

.. changelog::
    :version: 1.2.2
    :released: January 24, 2018

    .. change::
        :tags: bug, mssql
        :tickets: 4164

        Added ODBC error code 10054 to the list of error
        codes that count as a disconnect for ODBC / MSSQL server.


    .. change::
        :tags: bug, orm
        :tickets: 4171

        Fixed 1.2 regression regarding new bulk_replace event
        where a backref would fail to remove an object from the
        previous owner when a bulk-assignment assigned the
        object to a new owner.

    .. change::
        :tags: bug, oracle
        :tickets: 4163

        The cx_Oracle dialect now calls setinputsizes() with cx_Oracle.NCHAR
        unconditionally when the NVARCHAR2 datatype, in SQLAlchemy corresponding
        to sqltypes.Unicode(), is in use.  Per cx_Oracle's author this allows
        the correct conversions to occur within the Oracle client regardless
        of the setting for NLS_NCHAR_CHARACTERSET.

    .. change::
        :tags: bug, mysql

        Added more MySQL 8.0 reserved words to the MySQL dialect
        for quoting purposes.  Pull request courtesy
        Riccardo Magliocchetti.

.. changelog::
    :version: 1.2.1
    :released: January 15, 2018

    .. change::
        :tags: bug, orm
        :tickets: 4159

        Fixed regression where pickle format of a Load / _UnboundLoad object (e.g.
        loader options) changed and ``__setstate__()`` was raising an
        UnboundLocalError for an object received from the legacy format, even
        though an attempt was made to do so.  tests are now added to ensure this
        works.

    .. change::
        :tags: bug, ext
        :tickets: 4150

        Fixed regression in association proxy due to :ticket:`3769`
        (allow for chained any() / has()) where contains() against
        an association proxy chained in the form
        (o2m relationship, associationproxy(m2o relationship, m2o relationship))
        would raise an error regarding the re-application of contains()
        on the final link of the chain.

    .. change::
        :tags: bug, orm
        :tickets: 4153

        Fixed regression caused by new lazyload caching scheme in :ticket:`3954`
        where a query that makes use of loader options with of_type would cause
        lazy loads of unrelated paths to fail with a TypeError.

    .. change::
        :tags: bug, oracle
        :tickets: 4157

        Fixed regression where the removal of most setinputsizes
        rules from cx_Oracle dialect impacted the TIMESTAMP
        datatype's ability to retrieve fractional seconds.



    .. change::
        :tags: bug, tests

        Removed an oracle-specific requirements rule from the public
        test suite that was interfering with third party dialect
        suites.

    .. change::
        :tags: bug, mssql
        :tickets: 4154

        Fixed regression in 1.2 where newly repaired quoting
        of collation names in :ticket:`3785` breaks SQL Server,
        which explicitly does not understand a quoted collation
        name.   Whether or not mixed-case collation names are
        quoted or not is now deferred down to a dialect-level
        decision so that each dialect can prepare these identifiers
        directly.

    .. change::
        :tags: bug, orm
        :tickets: 4156

        Fixed bug in new "selectin" relationship loader where the loader could try
        to load a non-existent relationship when loading a collection of
        polymorphic objects, where only some of the mappers include that
        relationship, typically when :meth:`.PropComparator.of_type` is being used.

    .. change::
        :tags: bug, tests

        Added a new exclusion rule group_by_complex_expression
        which disables tests that use "GROUP BY <expr>", which seems
        to be not viable for at least two third party dialects.

    .. change::
        :tags: bug, oracle

        Fixed regression in Oracle imports where a missing comma caused
        an undefined symbol to be present.  Pull request courtesy
        Miroslav Shubernetskiy.

.. changelog::
    :version: 1.2.0
    :released: December 27, 2017

    .. change::
        :tags: orm, feature
        :tickets: 4137

        Added a new data member to the identity key tuple
        used by the ORM's identity map, known as the
        "identity_token".  This token defaults to None but
        may be used by database sharding schemes to differentiate
        objects in memory with the same primary key that come
        from different databases.   The horizontal sharding
        extension integrates this token applying the shard
        identifier to it, thus allowing primary keys to be
        duplicated across horizontally sharded backends.

        .. seealso::

            :ref:`change_4137`

    .. change::
        :tags: bug, mysql
        :tickets: 4115

        Fixed regression from issue 1.2.0b3 where "MariaDB" version comparison can
        fail for some particular MariaDB version strings under Python 3.

    .. change::
        :tags: enhancement, sql
        :tickets: 959

        Implemented "DELETE..FROM" syntax for PostgreSQL, MySQL, MS SQL Server
        (as well as within the unsupported Sybase dialect) in a manner similar
        to how "UPDATE..FROM" works.  A DELETE statement that refers to more than
        one table will switch into "multi-table" mode and render the appropriate
        "USING" or multi-table "FROM" clause as understood by the database.
        Pull request courtesy Pieter Mulder.

        .. seealso::

            :ref:`change_959`

    .. change::
       :tags: bug, sql
       :tickets: 2694

       Reworked the new "autoescape" feature introduced in
       :ref:`change_2694` in 1.2.0b2 to be fully automatic; the escape
       character now defaults to a forwards slash ``"/"`` and
       is applied to percent, underscore, as well as the escape
       character itself, for fully automatic escaping.  The
       character can also be changed using the "escape" parameter.

       .. seealso::

            :ref:`change_2694`


    .. change::
        :tags: bug, sql
        :tickets: 4147

        Fixed bug where the :meth:`_schema.Table.tometadata` method would not properly
        accommodate :class:`.Index` objects that didn't consist of simple
        column expressions, such as indexes against a :func:`_expression.text` construct,
        indexes that used SQL expressions or :attr:`.func`, etc.   The routine
        now copies expressions fully to a new :class:`.Index` object while
        substituting all table-bound :class:`_schema.Column` objects for those
        of the target table.

    .. change::
        :tags: bug, sql
        :tickets: 4142

        Changed the "visit name" of :class:`_expression.ColumnElement` from "column" to
        "column_element", so that when this element is used as the basis for a
        user-defined SQL element, it is not assumed to behave like a table-bound
        :class:`.ColumnClause` when processed by various SQL traversal utilities,
        as are commonly used by the ORM.

    .. change::
        :tags: bug, sql, ext
        :tickets: 4141

        Fixed issue in :class:`_types.ARRAY` datatype which is essentially the same
        issue as that of :ticket:`3832`, except not a regression, where
        column attachment events on top of :class:`_types.ARRAY` would not fire
        correctly, thus interfering with systems which rely upon this.   A key
        use case that was broken by this is the use of mixins to declare
        columns that make use of :meth:`.MutableList.as_mutable`.

    .. change::
        :tags: feature, engine
        :tickets: 4089

        The "password" attribute of the :class:`.url.URL` object can now be
        any user-defined or user-subclassed string object that responds to the
        Python ``str()`` builtin.   The object passed will be maintained as the
        datamember :attr:`.url.URL.password_original` and will be consulted
        when the :attr:`.url.URL.password` attribute is read to produce the
        string value.

    .. change::
        :tags: bug, orm
        :tickets: 4130

        Fixed bug in :func:`.contains_eager` query option where making use of a
        path that used :meth:`.PropComparator.of_type` to refer to a subclass
        across more than one level of joins would also require that the "alias"
        argument were provided with the same subtype in order to avoid adding
        unwanted FROM clauses to the query; additionally,  using
        :func:`.contains_eager` across subclasses that use :func:`.aliased` objects
        of subclasses as the :meth:`.PropComparator.of_type` argument will also
        render correctly.




    .. change::
        :tags: feature, postgresql

        Added new :class:`_postgresql.MONEY` datatype.  Pull request courtesy
        Cleber J Santos.

    .. change::
        :tags: bug, sql
        :tickets: 4140

        Fixed bug in new "expanding bind parameter" feature whereby if multiple
        params were used in one statement, the regular expression would not
        match the parameter name correctly.

    .. change::
        :tags: enhancement, ext
        :tickets: 4135

        Added new method :meth:`.baked.Result.with_post_criteria` to baked
        query system, allowing non-SQL-modifying transformations to take place
        after the query has been pulled from the cache.  Among other things,
        this method can be used with :class:`.horizontal_shard.ShardedQuery`
        to set the shard identifier.   :class:`.horizontal_shard.ShardedQuery`
        has also been modified such that its :meth:`.ShardedQuery.get` method
        interacts correctly with that of :class:`_baked.Result`.

    .. change::
        :tags: bug, oracle
        :tickets: 4064

        Added some additional rules to fully handle ``Decimal('Infinity')``,
        ``Decimal('-Infinity')`` values with cx_Oracle numerics when using
        ``asdecimal=True``.

    .. change::
        :tags: bug, mssql
        :tickets: 4121

        Fixed bug where sqltypes.BINARY and sqltypes.VARBINARY datatypes
        would not include correct bound-value handlers for pyodbc,
        which allows the pyodbc.NullParam value to be passed that
        helps with FreeTDS.




    .. change::
        :tags: feature, misc

        Added a new errors section to the documentation with background
        about common error messages.   Selected exceptions within SQLAlchemy
        will include a link in their string output to the relevant section
        within this page.

    .. change::
        :tags: bug, orm
        :tickets: 4032

        The :meth:`_query.Query.exists` method will now disable eager loaders for when
        the query is rendered.  Previously, joined-eager load joins would be rendered
        unnecessarily as well as subquery eager load queries would be needlessly
        generated.   The new behavior matches that of the :meth:`_query.Query.subquery`
        method.

.. changelog::
    :version: 1.2.0b3
    :released: December 27, 2017
    :released: October 13, 2017

    .. change::
        :tags: feature, postgresql
        :tickets: 4109

        Added a new flag ``use_batch_mode`` to the psycopg2 dialect.  This flag
        enables the use of psycopg2's ``psycopg2.extras.execute_batch``
        extension when the :class:`_engine.Engine` calls upon
        ``cursor.executemany()``. This extension provides a critical
        performance increase by over an order of magnitude when running INSERT
        statements in batch.  The flag is False by default as it is considered
        to be experimental for now.

        .. seealso::

            :ref:`change_4109`

    .. change::
        :tags: bug, mssql
        :tickets: 4061

        SQL Server supports what SQLAlchemy calls "native boolean"
        with its BIT type, as this type only accepts 0 or 1 and the
        DBAPIs return its value as True/False.   So the SQL Server
        dialects now enable "native boolean" support, in that a
        CHECK constraint is not generated for a :class:`.Boolean`
        datatype.  The only difference vs. other native boolean
        is that there are no "true" / "false" constants so "1" and
        "0" are still rendered here.


    .. change::
        :tags: bug, oracle
        :tickets: 4064

        Partial support for persisting and retrieving the Oracle value
        "infinity" is implemented with cx_Oracle, using Python float values
        only, e.g. ``float("inf")``.  Decimal support is not yet fulfilled by
        the cx_Oracle DBAPI driver.

    .. change::
        :tags: bug, oracle

        The cx_Oracle dialect has been reworked and modernized to take advantage of
        new patterns that weren't present in the old 4.x series of cx_Oracle. This
        includes that the minimum cx_Oracle version is the 5.x series and that
        cx_Oracle 6.x is now fully tested. The most significant change involves
        type conversions, primarily regarding the numeric / floating point and LOB
        datatypes, making more effective use of cx_Oracle type handling hooks to
        simplify how bind parameter and result data is processed.

        .. seealso::

            :ref:`change_cxoracle_12`

    .. change::
        :tags: bug, oracle
        :tickets: 3997

        two phase support for cx_Oracle has been completely removed for all
        versions of cx_Oracle, whereas in 1.2.0b1 this change only took effect for
        the 6.x series of cx_Oracle.  This feature never worked correctly
        in any version of cx_Oracle and in cx_Oracle 6.x, the API which SQLAlchemy
        relied upon was removed.

        .. seealso::

            :ref:`change_cxoracle_12`

    .. change::
        :tags: bug, oracle

        The column keys present in a result set when using :meth:`_expression.Insert.returning`
        with the cx_Oracle backend now use the correct column / label names
        like that of all other dialects.  Previously, these came out as
        ``ret_nnn``.

        .. seealso::

            :ref:`change_cxoracle_12`

    .. change::
        :tags: bug, oracle

        Several parameters to the cx_Oracle dialect are now deprecated and will
        have no effect: ``auto_setinputsizes``, ``exclude_setinputsizes``,
        ``allow_twophase``.

        .. seealso::

            :ref:`change_cxoracle_12`


    .. change::
        :tags: bug, sql
        :tickets: 4075

        Added a new method :meth:`.DefaultExecutionContext.get_current_parameters`
        which is used within a function-based default value generator in
        order to retrieve the current parameters being passed to the statement.
        The new function differs from the
        :attr:`.DefaultExecutionContext.current_parameters` attribute in
        that it also provides for optional grouping of parameters that
        correspond to a multi-valued "insert" construct.  Previously it was not
        possible to identify the subset of parameters that were relevant to
        the function call.

        .. seealso::

            :ref:`change_4075`

            :ref:`context_default_functions`

    .. change::
        :tags: bug, orm
        :tickets: 4050

        Fixed regression introduced in 1.2.0b1 due to :ticket:`3934` where the
        :class:`.Session` would fail to "deactivate" the transaction, if a
        rollback failed (the target issue is when MySQL loses track of a SAVEPOINT).
        This would cause a subsequent call to :meth:`.Session.rollback` to raise
        an error a second time, rather than completing and bringing the
        :class:`.Session` back to ACTIVE.

    .. change::
        :tags: bug, postgresql
        :tickets: 4041

        Fixed bug where the pg8000 driver would fail if using
        :meth:`_schema.MetaData.reflect` with a schema name, since the schema name would
        be sent as a "quoted_name" object that's a string subclass, which pg8000
        doesn't recognize.   The quoted_name type is added to pg8000's
        py_types collection on connect.

    .. change::
        :tags: bug, postgresql
        :tickets: 4016

        Enabled UUID support for the pg8000 driver, which supports native Python
        uuid round trips for this datatype.  Arrays of UUID are still not supported,
        however.

    .. change::
        :tags: mssql, bug
        :tickets: 4057

        Fixed the pymssql dialect so that percent signs in SQL text, such
        as used in modulus expressions or literal textual values, are
        **not** doubled up, as seems to be what pymssql expects.  This is
        despite the fact that the pymssql DBAPI uses the "pyformat" parameter
        style which itself considers the percent sign to be significant.

    .. change::
        :tags: bug, orm, declarative
        :tickets: 4091

        A warning is emitted if a subclass attempts to override an attribute
        that was declared on a superclass using ``@declared_attr.cascading``
        that the overridden attribute will be ignored. This use
        case cannot be fully supported down to further subclasses without more
        complex development efforts, so for consistency the "cascading" is
        honored all the way down regardless of overriding attributes.

    .. change::
        :tags: bug, orm, declarative
        :tickets: 4092

        A warning is emitted if the ``@declared_attr.cascading`` attribute is
        used with a special declarative name such as ``__tablename__``, as this
        has no effect.

    .. change::
        :tags: feature, engine
        :tickets: 4077

        Added ``__next__()`` and ``next()`` methods to :class:`_engine.ResultProxy`,
        so that the ``next()`` builtin function works on the object directly.
        :class:`_engine.ResultProxy` has long had an ``__iter__()`` method which already
        allows it to respond to the ``iter()`` builtin.   The implementation
        for ``__iter__()`` is unchanged, as performance testing has indicated
        that iteration using a ``__next__()`` method with ``StopIteration``
        is about 20% slower in both Python 2.7 and 3.6.

    .. change::
        :tags: feature, mssql
        :tickets: 4086

        Added a new :class:`_mssql.TIMESTAMP` datatype, that
        correctly acts like a binary datatype for SQL Server
        rather than a datetime type, as SQL Server breaks the
        SQL standard here.  Also added :class:`_mssql.ROWVERSION`,
        as the "TIMESTAMP" type in SQL Server is deprecated in
        favor of ROWVERSION.

    .. change::
        :tags: bug, orm
        :tickets: 4084

        Fixed issue where the :func:`.make_transient_to_detached` function
        would expire all attributes on the target object, including "deferred"
        attributes, which has the effect of the attribute being undeferred
        for the next refresh, causing an unexpected load of the attribute.

    .. change::
        :tags: bug, orm
        :tickets: 4040

        Fixed bug involving delete-orphan cascade where a related item
        that becomes an orphan before the parent object is part of a
        session is still tracked as moving into orphan status, which results
        in it being expunged from the session rather than being flushed.

        .. note::  This fix was inadvertently merged during the 1.2.0b3
           release and was **not added to the changelog** at that time.
           This changelog note was added to the release retroactively as of
           version 1.2.13.

    .. change::
        :tags: bug, orm
        :tickets: 4026

        Fixed bug in :ref:`change_3948` which prevented "selectin" and
        "inline" settings in a multi-level class hierarchy from interacting
        together as expected.    A new example is added to the documentation.

        .. seealso::

            :ref:`polymorphic_selectin_and_withpoly`

    .. change::
        :tags: bug, oracle
        :tickets: 4042

        Fixed bug where an index reflected under Oracle with an expression like
        "column DESC" would not be returned, if the table also had no primary
        key, as a result of logic that attempts to filter out the
        index implicitly added by Oracle onto the primary key columns.

    .. change::
      :tags: bug, orm
      :tickets: 4071

      Removed the warnings that are emitted when the LRU caches employed
      by the mapper as well as loader strategies reach their threshold; the
      purpose of this warning was at first a guard against excess cache keys
      being generated but became basically a check on the "creating many
      engines" antipattern.   While this is still an antipattern, the presence
      of test suites which both create an engine per test as well as raise
      on all warnings will be an inconvenience; it should not be critical
      that such test suites change their architecture just for this warning
      (though engine-per-test suite is always better).

    .. change::
        :tags: bug, orm
        :tickets: 4049

        Fixed regression where the use of a :func:`.undefer_group` option
        in conjunction with a lazy loaded relationship option would cause
        an attribute error, due to a bug in the SQL cache key generation
        added in 1.2 as part of :ticket:`3954`.

    .. change::
        :tags: bug, oracle
        :tickets: 4045

        Fixed more regressions caused by cx_Oracle 6.0; at the moment, the only
        behavioral change for users is disconnect detection now detects for
        cx_Oracle.DatabaseError in addition to cx_Oracle.InterfaceError, as
        this behavior seems to have changed.   Other issues regarding numeric
        precision and uncloseable connections are pending with the upstream
        cx_Oracle issue tracker.

    .. change::
        :tags: bug, mssql
        :tickets: 4060

        Fixed bug where the SQL Server dialect could pull columns from multiple
        schemas when reflecting a self-referential foreign key constraint, if
        multiple schemas contained a constraint of the same name against a
        table of the same name.


    .. change::
        :tags: feature, mssql
        :tickets: 4058

        Added support for "AUTOCOMMIT" isolation level, as established
        via :meth:`_engine.Connection.execution_options`, to the
        PyODBC and pymssql dialects.   This isolation level sets the
        appropriate DBAPI-specific flags on the underlying
        connection object.

    .. change::
        :tags: bug, orm
        :tickets: 4073

        Modified the change made to the ORM update/delete evaluator in
        :ticket:`3366` such that if an unmapped column expression is present
        in the update or delete, if the evaluator can match its name to the
        mapped columns of the target class, a warning is emitted, rather than
        raising UnevaluatableError.  This is essentially the pre-1.2 behavior,
        and is to allow migration for applications that are currently relying
        upon this pattern.  However, if the given attribute name cannot be
        matched to the columns of the mapper, the UnevaluatableError is
        still raised, which is what was fixed in :ticket:`3366`.

    .. change::
        :tags: bug, sql
        :tickets: 4087

        Fixed bug in new SQL comments feature where table and column comment
        would not be copied when using :meth:`_schema.Table.tometadata`.

    .. change::
        :tags: bug, sql
        :tickets: 4102

        In release 1.1, the :class:`.Boolean` type was broken in that
        boolean coercion via ``bool()`` would occur for backends that did not
        feature "native boolean", but would not occur for native boolean backends,
        meaning the string ``"0"`` now behaved inconsistently. After a poll, a
        consensus was reached that non-boolean values should be raising an error,
        especially in the ambiguous case of string ``"0"``; so the :class:`.Boolean`
        datatype will now raise ``ValueError`` if an incoming value is not
        within the range ``None, True, False, 1, 0``.

        .. seealso::

            :ref:`change_4102`

    .. change::
        :tags: bug, sql
        :tickets: 4063

        Refined the behavior of :meth:`.Operators.op` such that in all cases,
        if the :paramref:`.Operators.op.is_comparison` flag is set to True,
        the return type of the resulting expression will be
        :class:`.Boolean`, and if the flag is False, the return type of the
        resulting expression will be the same type as that of the left-hand
        expression, which is the typical default behavior of other operators.
        Also added a new parameter :paramref:`.Operators.op.return_type` as well
        as a helper method :meth:`.Operators.bool_op`.

        .. seealso::

            :ref:`change_4063`

    .. change::
        :tags: bug, mysql
        :tickets: 4072

        Changed the name of the ``.values`` attribute of the new MySQL
        INSERT..ON DUPLICATE KEY UPDATE construct to ``.inserted``, as
        :class:`_expression.Insert` already has a method called :meth:`_expression.Insert.values`.
        The ``.inserted`` attribute ultimately renders the MySQL ``VALUES()``
        function.

    .. change::
        :tags: bug, mssql, orm
        :tickets: 4062

        Added a new class of "rowcount support" for dialects that is specific to
        when "RETURNING", which on SQL Server looks like "OUTPUT inserted", is in
        use, as the PyODBC backend isn't able to give us rowcount on an UPDATE or
        DELETE statement when OUTPUT is in effect.  This primarily affects the ORM
        when a flush is updating a row that contains server-calculated values,
        raising an error if the backend does not return the expected row count.
        PyODBC now states that it supports rowcount except if OUTPUT.inserted is
        present, which is taken into account by the ORM during a flush as to
        whether it will look for a rowcount.

    .. change::
        :tags: bug, sql
        :tickets: 4088

        Internal refinements to the :class:`.Enum`, :class:`.Interval`, and
        :class:`.Boolean` types, which now extend a common mixin
        :class:`.Emulated` that indicates a type that provides Python-side
        emulation of a DB native type, switching out to the DB native type when
        a supporting backend is in use.   The PostgreSQL
        :class:`_postgresql.INTERVAL` type when used directly will now include
        the correct type coercion rules for SQL expressions that also take
        effect for :class:`_types.Interval` (such as adding a date to an
        interval yields a datetime).


    .. change::
        :tags: bug, mssql, orm

        Enabled the "sane_rowcount" flag for the pymssql dialect, indicating
        that the DBAPI now reports the correct number of rows affected from
        an UPDATE or DELETE statement.  This impacts mostly the ORM versioning
        feature in that it now can verify the number of rows affected on a
        target version.

    .. change:: 4028
        :tags: bug, engine
        :tickets: 4028

        Made some adjustments to :class:`_pool.Pool` and :class:`_engine.Connection` such
        that recovery logic is not run underneath exception catches for
        ``pool.Empty``, ``AttributeError``, since when the recovery operation
        itself fails, Python 3 creates a misleading stack trace referring to the
        ``Empty`` / ``AttributeError`` as the cause, when in fact these exception
        catches are part of control flow.


    .. change::
        :tags: bug, oracle
        :tickets: 4076

        Fixed bug where Oracle 8 "non ansi" join mode would not add the
        ``(+)`` operator to expressions that used an operator other than the
        ``=`` operator.  The ``(+)`` needs to be on all columns that are part
        of the right-hand side.

    .. change::
        :tags: bug, mssql
        :tickets: 4059

        Added a rule to SQL Server index reflection to ignore the so-called
        "heap" index that is implicitly present on a table that does not
        specify a clustered index.


.. changelog::
    :version: 1.2.0b2
    :released: December 27, 2017
    :released: July 24, 2017

    .. change:: 4033
        :tags: bug, orm
        :tickets: 4033

        Fixed regression from 1.1.11 where adding additional non-entity
        columns to a query that includes an entity with subqueryload
        relationships would fail, due to an inspection added in 1.1.11 as a
        result of :ticket:`4011`.


.. changelog::
    :version: 1.2.0b1
    :released: December 27, 2017
    :released: July 10, 2017

    .. change:: scoped_autocommit
        :tags: feature, orm

        Added ``.autocommit`` attribute to :class:`.scoped_session`, proxying
        the ``.autocommit`` attribute of the underling :class:`.Session`
        currently assigned to the thread.  Pull request courtesy
        Ben Fagin.

    .. change:: 4009
        :tags: feature, mysql
        :tickets: 4009

        Added support for MySQL's ON DUPLICATE KEY UPDATE
        MySQL-specific :class:`.mysql.dml.Insert` object.
        Pull request courtesy Michael Doronin.

        .. seealso::

            :ref:`change_4009`

    .. change:: 4018
        :tags: bug, sql
        :tickets: 4018

        The rules for type coercion between :class:`.Numeric`, :class:`.Integer`,
        and date-related types now include additional logic that will attempt
        to preserve the settings of the incoming type on the "resolved" type.
        Currently the target for this is the ``asdecimal`` flag, so that
        a math operation between :class:`.Numeric` or :class:`.Float` and
        :class:`.Integer` will preserve the "asdecimal" flag as well as
        if the type should be the :class:`.Float` subclass.

        .. seealso::

            :ref:`change_floats_12`

    .. change:: 4020
        :tags: bug, sql, mysql
        :tickets: 4020

        The result processor for the :class:`.Float` type now unconditionally
        runs values through the ``float()`` processor if the dialect
        specifies that it also supports "native decimal" mode.  While most
        backends will deliver Python ``float`` objects for a floating point
        datatype, the MySQL backends in some cases lack the typing information
        in order to provide this and return ``Decimal`` unless the float
        conversion is done.

        .. seealso::

            :ref:`change_floats_12`

    .. change:: 4017
        :tags: bug, sql
        :tickets: 4017

        Added some extra strictness to the handling of Python "float" values
        passed to SQL statements.  A "float" value will be associated with the
        :class:`.Float` datatype and not the Decimal-coercing :class:`.Numeric`
        datatype as was the case before, eliminating a confusing warning
        emitted on SQLite as well as unnecessary coercion to Decimal.

        .. seealso::

            :ref:`change_floats_12`

    .. change:: 3058
        :tags: feature, orm
        :tickets: 3058

        Added a new feature :func:`_orm.with_expression` that allows an ad-hoc
        SQL expression to be added to a specific entity in a query at result
        time.  This is an alternative to the SQL expression being delivered as
        a separate element in the result tuple.

        .. seealso::

            :ref:`change_3058`

    .. change:: 3496
        :tags: bug, orm
        :tickets: 3496

        An UPDATE emitted as a result of the
        :paramref:`_orm.relationship.post_update` feature will now integrate with
        the versioning feature to both bump the version id of the row as well
        as assert that the existing version number was matched.

        .. seealso::

            :ref:`change_3496`

    .. change:: 3769
        :tags: bug, ext
        :tickets: 3769

        The :meth:`.AssociationProxy.any`, :meth:`.AssociationProxy.has`
        and :meth:`.AssociationProxy.contains` comparison methods now support
        linkage to an attribute that is itself also an
        :class:`.AssociationProxy`, recursively.

        .. seealso::

            :ref:`change_3769`

    .. change:: 3853
        :tags: bug, ext
        :tickets: 3853

        Implemented in-place mutation operators ``__ior__``, ``__iand__``,
        ``__ixor__`` and ``__isub__`` for :class:`.mutable.MutableSet`
        and ``__iadd__`` for :class:`.mutable.MutableList` so that change
        events are fired off when these mutator methods are used to alter the
        collection.

        .. seealso::

            :ref:`change_3853`

    .. change:: 3847
        :tags: bug, declarative
        :tickets: 3847

        A warning is emitted if the :attr:`.declared_attr.cascading` modifier
        is used with a declarative attribute that is itself declared on
        a class that is to be mapped, as opposed to a declarative mixin
        class or ``__abstract__`` class.  The :attr:`.declared_attr.cascading`
        modifier currently only applies to mixin/abstract classes.

    .. change:: 4003
        :tags: feature, oracle
        :tickets: 4003

        The Oracle dialect now inspects unique and check constraints when using
        :meth:`_reflection.Inspector.get_unique_constraints`,
        :meth:`_reflection.Inspector.get_check_constraints`.
        As Oracle does not have unique constraints that are separate from a unique
        :class:`.Index`, a :class:`_schema.Table` that's reflected will still continue
        to not have :class:`.UniqueConstraint` objects associated with it.
        Pull requests courtesy Eloy Felix.

        .. seealso::

            :ref:`change_4003`

    .. change:: 3948
        :tags: feature, orm
        :tickets: 3948

        Added a new style of mapper-level inheritance loading
        "polymorphic selectin".  This style of loading
        emits queries for each subclass in an inheritance
        hierarchy subsequent to the load of the base
        object type, using IN to specify the desired
        primary key values.

        .. seealso::

            :ref:`change_3948`

    .. change:: 3472
        :tags: bug, orm
        :tickets: 3471, 3472

        Repaired several use cases involving the
        :paramref:`_orm.relationship.post_update` feature when used in conjunction
        with a column that has an "onupdate" value.   When the UPDATE emits,
        the corresponding object attribute is now expired or refreshed so that
        the newly generated "onupdate" value can populate on the object;
        previously the stale value would remain.  Additionally, if the target
        attribute is set in Python for the INSERT of the object, the value is
        now re-sent during the UPDATE so that the "onupdate" does not overwrite
        it (note this works just as well for server-generated onupdates).
        Finally, the :meth:`.SessionEvents.refresh_flush` event is now emitted
        for these attributes when refreshed within the flush.

        .. seealso::

            :ref:`change_3471`

    .. change:: 3996
        :tags: bug, orm
        :tickets: 3996

        Fixed bug where programmatic version_id counter in conjunction with
        joined table inheritance would fail if the version_id counter
        were not actually incremented and no other values on the base table
        were modified, as the UPDATE would have an empty SET clause.  Since
        programmatic version_id where version counter is not incremented
        is a documented use case, this specific condition is now detected
        and the UPDATE now sets the version_id value to itself, so that
        concurrency checks still take place.

    .. change:: 3848
        :tags: bug, orm, declarative
        :tickets: 3848

        Fixed bug where using :class:`.declared_attr` on an
        :class:`.AbstractConcreteBase` where a particular return value were some
        non-mapped symbol, including ``None``, would cause the attribute
        to hard-evaluate just once and store the value to the object
        dictionary, not allowing it to invoke for subclasses.   This behavior
        is normal when :class:`.declared_attr` is on a mapped class, and
        does not occur on a mixin or abstract class.  Since
        :class:`.AbstractConcreteBase` is both "abstract" and actually
        "mapped", a special exception case is made here so that the
        "abstract" behavior takes precedence for :class:`.declared_attr`.

    .. change:: 3673
        :tags: bug, orm
        :tickets: 3673

        The versioning feature does not support NULL for the version counter.
        An exception is now raised if the version id is programmatic and
        was set to NULL for an UPDATE.  Pull request courtesy Diana Clarke.

    .. change:: 3999
        :tags: bug, sql
        :tickets: 3999

        The operator precedence for all comparison operators such as LIKE, IS,
        IN, MATCH, equals, greater than, less than, etc. has all been merged
        into one level, so that expressions which make use of these against
        each other will produce parentheses between them.   This suits the
        stated operator precedence of databases like Oracle, MySQL and others
        which place all of these operators as equal precedence, as well as
        PostgreSQL as of 9.5 which has also flattened its operator precedence.

        .. seealso::

            :ref:`change_3999`


    .. change:: 3796
        :tags: bug, orm
        :tickets: 3796

        Removed a very old keyword argument from :class:`.scoped_session`
        called ``scope``.  This keyword was never documented and was an
        early attempt at allowing for variable scopes.

        .. seealso::

            :ref:`change_3796`

    .. change:: 3871
        :tags: bug, mysql
        :tickets: 3871

        Added support for views that are unreflectable due to stale
        table definitions, when calling :meth:`_schema.MetaData.reflect`; a warning
        is emitted for the table that cannot respond to ``DESCRIBE``,
        but the operation succeeds.

    .. change:: baked_opts
        :tags: feature, ext

        Added new flag :paramref:`.Session.enable_baked_queries` to the
        :class:`.Session` to allow baked queries to be disabled
        session-wide, reducing memory use.   Also added new :class:`.Bakery`
        wrapper so that the bakery returned by :paramref:`.BakedQuery.bakery`
        can be inspected.

    .. change:: 3988
        :tags: bug, orm
        :tickets: 3988

        Fixed bug where combining a "with_polymorphic" load in conjunction
        with subclass-linked relationships that specify joinedload with
        innerjoin=True, would fail to demote those "innerjoins" to
        "outerjoins" to suit the other polymorphic classes that don't
        support that relationship.   This applies to both a single and a
        joined inheritance polymorphic load.

    .. change:: 3991
        :tags: bug, orm
        :tickets: 3991

        Added new argument :paramref:`.with_for_update` to the
        :meth:`.Session.refresh` method.  When the :meth:`_query.Query.with_lockmode`
        method were deprecated in favor of :meth:`_query.Query.with_for_update`,
        the :meth:`.Session.refresh` method was never updated to reflect
        the new option.

        .. seealso::

            :ref:`change_3991`

    .. change:: 3984
        :tags: bug, orm
        :tickets: 3984

        Fixed bug where a :func:`.column_property` that is also marked as
        "deferred" would be marked as "expired" during a flush, causing it
        to be loaded along with the unexpiry of regular attributes even
        though this attribute was never accessed.

    .. change:: 3873
        :tags: bug, sql
        :tickets: 3873

        Repaired issue where the type of an expression that used
        :meth:`.ColumnOperators.is_` or similar would not be a "boolean" type,
        instead the type would be "nulltype", as well as when using custom
        comparison operators against an untyped expression.   This typing can
        impact how the expression behaves in larger contexts as well as
        in result-row-handling.

    .. change:: 3941
        :tags: bug, ext
        :tickets: 3941

        Improved the association proxy list collection so that premature
        autoflush against a newly created association object can be prevented
        in the case where ``list.append()`` is being used, and a lazy load
        would be invoked when the association proxy accesses the endpoint
        collection.  The endpoint collection is now accessed first before
        the creator is invoked to produce the association object.

    .. change:: 3969
        :tags: bug, sql
        :tickets: 3969

        Fixed the negation of a :class:`.Label` construct so that the
        inner element is negated correctly, when the :func:`.not_` modifier
        is applied to the labeled expression.

    .. change:: 3944
        :tags: feature, orm
        :tickets: 3944

        Added a new kind of eager loading called "selectin" loading.  This
        style of loading is very similar to "subquery" eager loading,
        except that it uses an IN expression given a list of primary key
        values from the loaded parent objects, rather than re-stating the
        original query.   This produces a more efficient query that is
        "baked" (e.g. the SQL string is cached) and also works in the
        context of :meth:`_query.Query.yield_per`.

        .. seealso::

            :ref:`change_3944`

    .. change::
        :tags: bug, orm
        :tickets: 3967

        Fixed bug in subquery eager loading where the "join_depth" parameter
        for self-referential relationships would not be correctly honored,
        loading all available levels deep rather than correctly counting
        the specified number of levels for eager loading.

    .. change::
        :tags: bug, orm

        Added warnings to the LRU "compiled cache" used by the :class:`_orm.Mapper`
        (and ultimately will be for other ORM-based LRU caches) such that
        when the cache starts hitting its size limits, the application will
        emit a warning that this is a performance-degrading situation that
        may require attention.   The LRU caches can reach their size limits
        primarily if an application is making use of an unbounded number
        of :class:`_engine.Engine` objects, which is an antipattern.  Otherwise,
        this may suggest an issue that should be brought to the SQLAlchemy
        developer's attention.

    .. change:: 3964
        :tags: bug, postgresql
        :tickets: 3964

        Fixed bug where the base :class:`_types.ARRAY` datatype would not
        invoke the bind/result processors of :class:`_postgresql.ARRAY`.

    .. change:: 3963
        :tags: bug, orm
        :tickets: 3963

        Fixed bug to improve upon the specificity of loader options that
        take effect subsequent to the lazy load of a related entity, so
        that the loader options will match to an aliased or non-aliased
        entity more specifically if those options include entity information.

    .. change:: 3954
        :tags: feature, orm
        :tickets: 3954

        The ``lazy="select"`` loader strategy now makes used of the
        :class:`.BakedQuery` query caching system in all cases.  This
        removes most overhead of generating a :class:`_query.Query` object and
        running it into a :func:`_expression.select` and then string SQL statement from
        the process of lazy-loading related collections and objects.  The
        "baked" lazy loader has also been improved such that it can now
        cache in most cases where query load options are used.

        .. seealso::

            :ref:`change_3954`

    .. change:: 3740
        :tags: bug, sql
        :tickets: 3740

        The system by which percent signs in SQL statements are "doubled"
        for escaping purposes has been refined.   The "doubling" of percent
        signs mostly associated with the :obj:`_expression.literal_column` construct
        as well as operators like :meth:`.ColumnOperators.contains` now
        occurs based on the stated paramstyle of the DBAPI in use; for
        percent-sensitive paramstyles as are common with the PostgreSQL
        and MySQL drivers the doubling will occur, for others like that
        of SQLite it will not.   This allows more database-agnostic use
        of the :obj:`_expression.literal_column` construct to be possible.

        .. seealso::

            :ref:`change_3740`

    .. change:: 3959
        :tags: bug, postgresql
        :tickets: 3959

        Added support for all possible "fields" identifiers when reflecting the
        PostgreSQL ``INTERVAL`` datatype, e.g. "YEAR", "MONTH", "DAY TO
        MINUTE", etc..   In addition, the :class:`_postgresql.INTERVAL`
        datatype itself now includes a new parameter
        :paramref:`.postgresql.INTERVAL.fields` where these qualifiers can be
        specified; the qualifier is also reflected back into the resulting
        datatype upon reflection / inspection.

        .. seealso::

            :ref:`change_3959`

    .. change:: 3957
        :tags: bug, sql
        :tickets: 3957

        Fixed bug where a column-level :class:`.CheckConstraint` would fail
        to compile the SQL expression using the underlying dialect compiler
        as well as apply proper flags to generate literal values as
        inline, in the case that the sqltext is a Core expression and
        not just a plain string.   This was long-ago fixed for table-level
        check constraints in 0.9 as part of :ticket:`2742`, which more commonly
        feature Core SQL expressions as opposed to plain string expressions.

    .. change:: 2626
        :tags: bug, mssql
        :tickets: 2626

        The SQL Server dialect now allows for a database and/or owner name
        with a dot inside of it, using brackets explicitly in the string around
        the owner and optionally the database name as well.  In addition,
        sending the :class:`.quoted_name` construct for the schema name will
        not split on the dot and will deliver the full string as the "owner".
        :class:`.quoted_name` is also now available from the ``sqlalchemy.sql``
        import space.

        .. seealso::

            :ref:`change_2626`

    .. change:: 3953
        :tags: feature, sql
        :tickets: 3953

        Added a new kind of :func:`.bindparam` called "expanding".  This is
        for use in ``IN`` expressions where the list of elements is rendered
        into individual bound parameters at statement execution time, rather
        than at statement compilation time.  This allows both a single bound
        parameter name to be linked to an IN expression of multiple elements,
        as well as allows query caching to be used with IN expressions.  The
        new feature allows the related features of "select in" loading and
        "polymorphic in" loading to make use of the baked query extension
        to reduce call overhead.   This feature should be considered to be
        **experimental** for 1.2.

        .. seealso::

            :ref:`change_3953`

    .. change:: 3923
        :tags: bug, sql
        :tickets: 3923

        Fixed bug where a SQL-oriented Python-side column default could fail to
        be executed properly upon INSERT in the "pre-execute" codepath, if the
        SQL itself were an untyped expression, such as plain text.  The "pre-
        execute" codepath is fairly uncommon however can apply to non-integer
        primary key columns with SQL defaults when RETURNING is not used.

    .. change:: 3785
        :tags: bug, sql
        :tickets: 3785

        The expression used for COLLATE as rendered by the column-level
        :func:`_expression.collate` and :meth:`.ColumnOperators.collate` is now
        quoted as an identifier when the name is case sensitive, e.g. has
        uppercase characters.  Note that this does not impact type-level
        collation, which is already quoted.

        .. seealso::

            :ref:`change_3785`

    .. change:: 3229
        :tags: feature, orm, ext
        :tickets: 3229

        The :meth:`_query.Query.update` method can now accommodate both
        hybrid attributes as well as composite attributes as a source
        of the key to be placed in the SET clause.   For hybrids, an
        additional decorator :meth:`.hybrid_property.update_expression`
        is supplied for which the user supplies a tuple-returning function.

        .. seealso::

            :ref:`change_3229`

    .. change:: 3753
        :tags: bug, orm
        :tickets: 3753

        The :func:`.attributes.flag_modified` function now raises
        :class:`.InvalidRequestError` if the named attribute key is not
        present within the object, as this is assumed to be present
        in the flush process.  To mark an object "dirty" for a flush
        without referring to any specific attribute, the
        :func:`.attributes.flag_dirty` function may be used.

        .. seealso::

            :ref:`change_3753`

    .. change:: 3911_3912
        :tags: bug, ext
        :tickets: 3911, 3912

        The :class:`sqlalchemy.ext.hybrid.hybrid_property` class now supports
        calling mutators like ``@setter``, ``@expression`` etc. multiple times
        across subclasses, and now provides a ``@getter`` mutator, so that
        a particular hybrid can be repurposed across subclasses or other
        classes.  This now matches the behavior of ``@property`` in standard
        Python.

        .. seealso::

            :ref:`change_3911_3912`



    .. change:: 1546
        :tags: feature, sql, postgresql, mysql, oracle
        :tickets: 1546

        Added support for SQL comments on :class:`_schema.Table` and :class:`_schema.Column`
        objects, via the new :paramref:`_schema.Table.comment` and
        :paramref:`_schema.Column.comment` arguments.   The comments are included
        as part of DDL on table creation, either inline or via an appropriate
        ALTER statement, and are also reflected back within table reflection,
        as well as via the :class:`_reflection.Inspector`.   Supported backends currently
        include MySQL, PostgreSQL, and Oracle.  Many thanks to Frazer McLean
        for a large amount of effort on this.

        .. seealso::

            :ref:`change_1546`

    .. change:: 3919
        :tags: feature, engine
        :tickets: 3919

        Added native "pessimistic disconnection" handling to the :class:`_pool.Pool`
        object.  The new parameter :paramref:`_pool.Pool.pre_ping`, available from
        the engine as :paramref:`_sa.create_engine.pool_pre_ping`, applies an
        efficient form of the "pre-ping" recipe featured in the pooling
        documentation, which upon each connection check out, emits a simple
        statement, typically "SELECT 1", to test the connection for liveness.
        If the existing connection is no longer able to respond to commands,
        the connection is transparently recycled, and all other connections
        made prior to the current timestamp are invalidated.

        .. seealso::

            :ref:`pool_disconnects_pessimistic`

            :ref:`change_3919`

    .. change:: 3939
        :tags: bug, sql
        :tickets: 3939

        Fixed bug where the use of an :class:`_expression.Alias` object in a column
        context would raise an argument error when it tried to group itself
        into a parenthesized expression.   Using :class:`_expression.Alias` in this way
        is not yet a fully supported API, however it applies to some end-user
        recipes and may have a more prominent role in support of some
        future PostgreSQL features.

    .. change:: 3366
        :tags: bug, orm
        :tickets: 3366

        The "evaluate" strategy used by :meth:`_query.Query.update` and
        :meth:`_query.Query.delete` can now accommodate a simple
        object comparison from a many-to-one relationship to an instance,
        when the attribute names of the primary key / foreign key columns
        don't match the actual names of the columns.  Previously this would
        do a simple name-based match and fail with an AttributeError.

    .. change:: 3896_a
        :tags: feature, orm
        :tickets: 3896

        Added new attribute event :meth:`.AttributeEvents.bulk_replace`.
        This event is triggered when a collection is assigned to a
        relationship, before the incoming collection is compared with the
        existing one.  This early event allows for conversion of incoming
        non-ORM objects as well.  The event is integrated with the
        ``@validates`` decorator.

        .. seealso::

            :ref:`change_3896_event`

    .. change:: 3896_b
        :tags: bug, orm
        :tickets: 3896

        The ``@validates`` decorator now allows the decorated method to receive
        objects from a "bulk collection set" operation that have not yet
        been compared to the existing collection.  This allows incoming values
        to be converted to compatible ORM objects as is already allowed
        from an "append" event.   Note that this means that the
        ``@validates`` method is called for **all** values during a collection
        assignment, rather than just the ones that are new.

        .. seealso::

            :ref:`change_3896_validates`

    .. change:: 3938
        :tags: bug, engine
        :tickets: 3938

        Fixed bug where in the unusual case of passing a
        :class:`.Compiled` object directly to :meth:`_engine.Connection.execute`,
        the dialect with which the :class:`.Compiled` object were generated
        was not consulted for the paramstyle of the string statement, instead
        assuming it would match the dialect-level paramstyle, causing
        mismatches to occur.

    .. change:: 3303
        :tags: feature, orm
        :tickets: 3303

        Added new event handler :meth:`.AttributeEvents.modified` which is
        triggered when the func:`.attributes.flag_modified` function is
        invoked, which is common when using the :mod:`sqlalchemy.ext.mutable`
        extension module.

        .. seealso::

            :ref:`change_3303`

    .. change:: 3918
        :tags: bug, ext
        :tickets: 3918

        Fixed a bug in the ``sqlalchemy.ext.serializer`` extension whereby
        an "annotated" SQL element (as produced by the ORM for many types
        of SQL expressions) could not be reliably serialized.  Also bumped
        the default pickle level for the serializer to "HIGHEST_PROTOCOL".

    .. change:: 3891
        :tags: bug, orm
        :tickets: 3891

        Fixed bug in single-table inheritance where the select_from()
        argument would not be taken into account when limiting rows
        to a subclass.  Previously, only expressions in the
        columns requested would be taken into account.

        .. seealso::

            :ref:`change_3891`

    .. change:: 3913
        :tags: bug, orm
        :tickets: 3913

        When assigning a collection to an attribute mapped by a relationship,
        the previous collection is no longer mutated.  Previously, the old
        collection would be emptied out in conjunction with the "item remove"
        events that fire off; the events now fire off without affecting
        the old collection.

        .. seealso::

            :ref:`change_3913`

    .. change:: 3932
        :tags: bug, oracle
        :tickets: 3932

        The cx_Oracle dialect now supports "sane multi rowcount", that is,
        when a series of parameter sets are executed via DBAPI
        ``cursor.executemany()``, we can make use of ``cursor.rowcount`` to
        verify the number of rows matched.  This has an impact within the
        ORM when detecting concurrent modification scenarios, in that
        some simple conditions can now be detected even when the ORM
        is batching statements, as well as when the more strict versioning
        feature is used, the ORM can still use statement batching.  The
        flag is enabled for cx_Oracle assuming at least version 5.0, which
        is now commonplace.

    .. change:: 3907
        :tags: feature, sql
        :tickets: 3907

        The longstanding behavior of the :meth:`.ColumnOperators.in_` and
        :meth:`.ColumnOperators.notin_` operators emitting a warning when
        the right-hand condition is an empty sequence has been revised;
        a simple "static" expression of "1 != 1" or "1 = 1" is now rendered
        by default, rather than pulling in the original left-hand
        expression.  This causes the result for a NULL column comparison
        against an empty set to change from NULL to true/false.  The
        behavior is configurable, and the old behavior can be enabled
        using the :paramref:`_sa.create_engine.empty_in_strategy` parameter
        to :func:`_sa.create_engine`.

        .. seealso::

            :ref:`change_3907`

    .. change:: 3276
        :tags: bug, oracle
        :tickets: 3276

        Oracle reflection now "normalizes" the name given to a foreign key
        constraint, that is, returns it as all lower case for a case
        insensitive name.  This was already the behavior for indexes
        and primary key constraints as well as all table and column names.
        This will allow Alembic autogenerate scripts to compare and render
        foreign key constraint names correctly when initially specified
        as case insensitive.

        .. seealso::

            :ref:`change_3276`

    .. change:: 2694
        :tags: feature, sql
        :tickets: 2694

        Added a new option ``autoescape`` to the "startswith" and
        "endswith" classes of comparators; this supplies an escape character
        also applies it to all occurrences of the wildcard characters "%"
        and "_" automatically.  Pull request courtesy Diana Clarke.

        .. note::  This feature has been changed as of 1.2.0 from its initial
           implementation in 1.2.0b2 such that autoescape is now passed as a
           boolean value, rather than a specific character to use as the escape
           character.

        .. seealso::

            :ref:`change_2694`

    .. change:: 3934
        :tags: bug, orm
        :tickets: 3934

        The state of the :class:`.Session` is now present when the
        :meth:`.SessionEvents.after_rollback` event is emitted, that is,  the
        attribute state of objects prior to their being expired.   This is now
        consistent with the  behavior of the
        :meth:`.SessionEvents.after_commit` event which  also emits before the
        attribute state of objects is expired.

        .. seealso::

            :ref:`change_3934`

    .. change:: 3607
        :tags: bug, orm
        :tickets: 3607

        Fixed bug where :meth:`_query.Query.with_parent` would not work if the
        :class:`_query.Query` were against an :func:`.aliased` construct rather than
        a regular mapped class.  Also adds a new parameter
        :paramref:`.util.with_parent.from_entity` to the standalone
        :func:`.util.with_parent` function as well as
        :meth:`_query.Query.with_parent`.
