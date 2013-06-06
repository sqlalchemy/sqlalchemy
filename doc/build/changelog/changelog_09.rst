
==============
0.9 Changelog
==============

.. changelog::
    :version: 0.9.0

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