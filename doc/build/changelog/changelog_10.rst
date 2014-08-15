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

    .. change::
        :tags: orm, feature

        UPDATE statements can now be batched within an ORM flush
        into more performant executemany() call, similarly to how INSERT
        statements can be batched; this will be invoked within flush
        to the degree that subsequent UPDATE statements for the
        same mapping and table involve the identical columns within the
        VALUES clause, as well as that no VALUES-level SQL expressions
        are embedded.

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
		capabilities.  Work on this feature is courtesy of Dobes Vandermeer.

		.. seealso::

			:ref:`feature_3034`.
