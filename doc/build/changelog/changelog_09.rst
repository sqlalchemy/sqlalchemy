
==============
0.9 Changelog
==============

.. changelog::
    :version: 0.9.0

    .. change::
        :tags: bug, ext

        Fixed bug whereby if a composite type were set up
        with a function instead of a class, the mutable extension
        would trip up when it tried to check that column
        for being a :class:`.MutableComposite` (which it isn't).
        Courtesy asldevi.  Also in 0.8.2.

    .. change::
        :tags: bug, sql
        :tickets: 1765

        The resolution of :class:`.ForeignKey` objects to their
        target :class:`.Column` has been reworked to be as
        immediate as possible, based on the moment that the
        target :class:`.Column` is associated with the same
        :class:`.MetaData` as this :class:`.ForeignKey`, rather
        than waiting for the first time a join is constructed,
        or similar. This along with other improvements allows
        earlier detection of some foreign key configuration
        issues.  Also included here is a rework of the
        type-propagation system, so that
        it should be reliable now to set the type as ``None``
        on any :class:`.Column` that refers to another via
        :class:`.ForeignKey` - the type will be copied from the
        target column as soon as that other column is associated,
        and now works for composite foreign keys as well.

    .. change::
        :tags: feature, sql
        :tickets: 2744, 2734

        Provided a new attribute for :class:`.TypeDecorator`
        called :attr:`.TypeDecorator.coerce_to_is_types`,
        to make it easier to control how comparisons using
        ``==`` or ``!=`` to ``None`` and boolean types goes
        about producing an ``IS`` expression, or a plain
        equality expression with a bound parameter.


    .. change::
        :tags: feature, postgresql

        Support for Postgresql 9.2 range types has been added.
        Currently, no type translation is provided, so works
        directly with strings or psycopg2 2.5 range extension types
        at the moment.  Patch courtesy Chris Withers.

    .. change::
        :tags: bug, examples

        Fixed an issue with the "versioning" recipe whereby a many-to-one
        reference could produce a meaningless version for the target,
        even though it was not changed, when backrefs were present.
        Patch courtesy Matt Chisholm.  Also in 0.8.2.

    .. change::
        :tags: feature, postgresql
        :tickets: 2072

        Added support for "AUTOCOMMIT" isolation when using the psycopg2
        DBAPI.   The keyword is available via the ``isolation_level``
        execution option.  Patch courtesy Roman Podolyaka.
        Also in 0.8.2.

    .. change::
        :tags: bug, orm
        :tickets: 2759

        Fixed bug in polymorphic SQL generation where multiple joined-inheritance
        entities against the same base class joined to each other as well
        would not track columns on the base table independently of each other if
        the string of joins were more than two entities long.  Also in 0.8.2.

    .. change::
        :tags: bug, engine
        :pullreq: 6

        Fixed bug where the ``reset_on_return`` argument to various :class:`.Pool`
        implementations would not be propagated when the pool was regenerated.
        Courtesy Eevee.  Also in 0.8.2.

    .. change::
        :tags: bug, orm
        :tickets: 2754

        Fixed bug where sending a composite attribute into :meth:`.Query.order_by`
        would produce a parenthesized expression not accepted by some databases.
        Also in 0.8.2.

    .. change::
        :tags: bug, orm
        :tickets: 2755

        Fixed the interaction between composite attributes and
        the :func:`.aliased` function.  Previously, composite attributes
        wouldn't work correctly in comparison operations when aliasing
        was applied.  Also in 0.8.2.

    .. change::
        :tags: feature, sql
        :tickets: 1443

        Added support for "unique constraint" reflection, via the
        :meth:`.Inspector.get_unique_constraints` method.
        Thanks for Roman Podolyaka for the patch.

    .. change::
        :tags: feature, pool
        :tickets: 2752

        Added pool logging for "rollback-on-return" and the less used
        "commit-on-return".  This is enabled with the rest of pool
        "debug" logging.

    .. change::
        :tags: bug, mysql
        :tickets: 2715

        Added another conditional to the ``mysql+gaerdbms`` dialect to
        detect so-called "development" mode, where we should use the
        ``rdbms_mysqldb`` DBAPI.  Patch courtesy Brett Slatkin.
        Also in 0.8.2.

    .. change::
        :tags: feature, mysql
        :tickets: 2704

        The ``mysql_length`` parameter used with :class:`.Index` can now
        be passed as a dictionary of column names/lengths, for use
        with composite indexes.  Big thanks to Roman Podolyaka for the
        patch.  Also in 0.8.2.

    .. change::
        :tags: bug, orm, associationproxy
        :tickets: 2751

        Added additional criterion to the ==, != comparators, used with
        scalar values, for comparisons to None to also take into account
        the association record itself being non-present, in addition to the
        existing test for the scalar endpoint on the association record
        being NULL.  Previously, comparing ``Cls.scalar == None`` would return
        records for which ``Cls.associated`` were present and
        ``Cls.associated.scalar`` is None, but not rows for which
        ``Cls.associated`` is non-present.  More significantly, the
        inverse operation ``Cls.scalar != None`` *would* return ``Cls``
        rows for which ``Cls.associated`` was non-present.

        The case for ``Cls.scalar != 'somevalue'`` is also modified
        to act more like a direct SQL comparison; only rows for
        which ``Cls.associated`` is present and ``Associated.scalar``
        is non-NULL and not equal to ``'somevalue'`` are returned.
        Previously, this would be a simple ``NOT EXISTS``.

        Also added a special use case where you
        can call ``Cls.scalar.has()`` with no arguments,
        when ``Cls.scalar`` is a column-based value - this returns whether or
        not ``Cls.associated`` has any rows present, regardless of whether
        or not ``Cls.associated.scalar`` is NULL or not.

        .. seealso::

            :ref:`migration_2751`


    .. change::
        :tags: feature, orm
        :tickets: 2587

        A major change regarding how the ORM constructs joins where
        the right side is itself a join or left outer join.   The ORM
        is now configured to allow simple nesting of joins of
        the form ``a JOIN (b JOIN c ON b.id=c.id) ON a.id=b.id``,
        rather than forcing the right side into a ``SELECT`` subquery.
        This should allow significant performance improvements on most
        backends, most particularly MySQL.   The one database backend
        that has for many years held back this change, SQLite, is now addressed by
        moving the production of the ``SELECT`` subquery from the
        ORM to the SQL compiler; so that a right-nested join on SQLite will still
        ultimately render with a ``SELECT``, while all other backends
        are no longer impacted by this workaround.

        As part of this change, a new argument ``flat=True`` has been added
        to the :func:`.orm.aliased`, :meth:`.Join.alias`, and
        :func:`.orm.with_polymorphic` functions, which allows an "alias" of a
        JOIN to be produced which applies an anonymous alias to each component
        table within the join, rather than producing a subquery.

        .. seealso::

            :ref:`feature_joins_09`


    .. change::
        :tags: bug, orm
        :tickets: 2369

        Fixed an obscure bug where the wrong results would be
        fetched when joining/joinedloading across a many-to-many
        relationship to a single-table-inheriting
        subclass with a specific discriminator value, due to "secondary"
        rows that would come back.  The "secondary" and right-side
        tables are now inner joined inside of parenthesis for all
        ORM joins on many-to-many relationships so that the left->right
        join can accurately filtered.  This change was made possible
        by finally addressing the issue with right-nested joins
        outlined in :ticket:`2587`.

        .. seealso::

            :ref:`feature_joins_09`

    .. change::
        :tags: bug, mssql
        :tickets: 2747

        When querying the information schema on SQL Server 2000, removed
        a CAST call that was added in 0.8.1 to help with driver issues,
        which apparently is not compatible on 2000.
        The CAST remains in place for SQL Server 2005 and greater.
        Also in 0.8.2.

    .. change::
        :tags: bug, mysql
        :tickets: 2721

        The ``deferrable`` keyword argument on :class:`.ForeignKey` and
        :class:`.ForeignKeyConstraint` will not render the ``DEFERRABLE`` keyword
        on the MySQL dialect.  For a long time we left this in place because
        a non-deferrable foreign key would act very differently than a deferrable
        one, but some environments just disable FKs on MySQL, so we'll be less
        opinionated here.  Also in 0.8.2.

    .. change::
        :tags: bug, ext, orm
        :tickets: 2730

        Fixed bug where :class:`.MutableDict` didn't report a change event
        when ``clear()`` was called.  Also in 0.8.2

    .. change::
        :tags: bug, sql
        :tickets: 2738

        Fixed bug whereby joining a select() of a table "A" with multiple
        foreign key paths to a table "B", to that table "B", would fail
        to produce the "ambiguous join condition" error that would be
        reported if you join table "A" directly to "B"; it would instead
        produce a join condition with multiple criteria.  Also in 0.8.2.

    .. change::
        :tags: bug, sql, reflection
        :tickets: 2728

        Fixed bug whereby using :meth:`.MetaData.reflect` across a remote
        schema as well as a local schema could produce wrong results
        in the case where both schemas had a table of the same name.
        Also in 0.8.2.

    .. change::
        :tags: bug, sql
        :tickets: 2726

        Removed the "not implemented" ``__iter__()`` call from the base
        :class:`.ColumnOperators` class, while this was introduced
        in 0.8.0 to prevent an endless, memory-growing loop when one also
        implements a ``__getitem__()`` method on a custom
        operator and then calls erroneously ``list()`` on that object,
        it had the effect of causing column elements to report that they
        were in fact iterable types which then throw an error when you try
        to iterate.   There's no real way to have both sides here so we
        stick with Python best practices.  Careful with implementing
        ``__getitem__()`` on your custom operators! Also in 0.8.2.

    .. change::
        :tags: feature, sql
        :tickets: 1068

        A :class:`.Label` construct will now render as its name alone
        in an ``ORDER BY`` clause, if that label is also referred to
        in the columns clause of the select, instead of rewriting the
        full expression.  This gives the database a better chance to
        optimize the evaulation of the same expression in two different
        contexts.

    .. change::
        :tags: feature, firebird
        :tickets: 2504

        The ``fdb`` dialect is now the default dialect when
        specified without a dialect qualifier, i.e. ``firebird://``,
        per the Firebird project publishing ``fdb`` as their
        official Python driver.

    .. change::
    	:tags: feature, general
      	:tickets: 2671

        The codebase is now "in-place" for Python
        2 and 3, the need to run 2to3 has been removed.
        Compatibility is now against Python 2.6 on forward.

    .. change::
    	:tags: feature, oracle, py3k

    	The Oracle unit tests with cx_oracle now pass
    	fully under Python 3.

    .. change::
        :tags: bug, orm
        :tickets: 2736

        The "auto-aliasing" behavior of the :class:`.Query.select_from`
        method has been turned off.  The specific behavior is now
        availble via a new method :class:`.Query.select_entity_from`.
        The auto-aliasing behavior here was never well documented and
        is generally not what's desired, as :class:`.Query.select_from`
        has become more oriented towards controlling how a JOIN is
        rendered.  :class:`.Query.select_entity_from` will also be made
        available in 0.8 so that applications which rely on the auto-aliasing
        can shift their applications to use this method.

        .. seealso::

            :ref:`migration_2736`