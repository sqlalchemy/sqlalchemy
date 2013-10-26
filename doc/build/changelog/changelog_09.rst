
==============
0.9 Changelog
==============

.. changelog_imports::

    .. include:: changelog_08.rst

    .. include:: changelog_07.rst

.. changelog::
    :version: 0.9.0b1
    :released: October 26, 2013

    .. change::
        :tags: feature, orm
        :tickets: 2810

        The association proxy now returns ``None`` when fetching a scalar
        attribute off of a scalar relationship, where the scalar relationship
        itself points to ``None``, instead of raising an ``AttributeError``.

        .. seealso::

            :ref:`migration_2810`

    .. change::
        :tags: feature, sql, postgresql, mysql
        :tickets: 2183

        The Postgresql and MySQL dialects now support reflection/inspection
        of foreign key options, including ON UPDATE, ON DELETE.  Postgresql
        also reflects MATCH, DEFERRABLE, and INITIALLY.  Coutesy ijl.

    .. change::
        :tags: bug, mysql
        :tickets: 2839

        Fix and test parsing of MySQL foreign key options within reflection;
        this complements the work in :ticket:`2183` where we begin to support
        reflection of foreign key options such as ON UPDATE/ON DELETE
        cascade.

    .. change::
        :tags: bug, orm
        :tickets: 2787

        :func:`.attributes.get_history()` when used with a scalar column-mapped
        attribute will now honor the "passive" flag
        passed to it; as this defaults to ``PASSIVE_OFF``, the function will
        by default query the database if the value is not present.
        This is a behavioral change vs. 0.8.

        .. seealso::

            :ref:`change_2787`

    .. change::
        :tags: feature, orm
        :tickets: 2787

        Added new method :meth:`.AttributeState.load_history`, works like
        :attr:`.AttributeState.history` but also fires loader callables.

        .. seealso::

            :ref:`change_2787`


    .. change::
        :tags: feature, sql
        :tickets: 2850

        A :func:`.bindparam` construct with a "null" type (e.g. no type
        specified) is now copied when used in a typed expression, and the
        new copy is assigned the actual type of the compared column.  Previously,
        this logic would occur on the given :func:`.bindparam` in place.
        Additionally, a similar process now occurs for :func:`.bindparam` constructs
        passed to :meth:`.ValuesBase.values` for an :class:`.Insert` or
        :class:`.Update` construct, within the compilation phase of the
        construct.

        These are both subtle behavioral changes which may impact some
        usages.

        .. seealso::

            :ref:`migration_2850`

    .. change::
        :tags: feature, sql
        :tickets: 2804, 2823, 2734

        An overhaul of expression handling for special symbols particularly
        with conjunctions, e.g.
        ``None`` :func:`.expression.null` :func:`.expression.true`
        :func:`.expression.false`, including consistency in rendering NULL
        in conjunctions, "short-circuiting" of :func:`.and_` and :func:`.or_`
        expressions which contain boolean constants, and rendering of
        boolean constants and expressions as compared to "1" or "0" for backends
        that don't feature ``true``/``false`` constants.

        .. seealso::

            :ref:`migration_2804`

    .. change::
        :tags: feature, sql
        :tickets: 2838

        The typing system now handles the task of rendering "literal bind" values,
        e.g. values that are normally bound parameters but due to context must
        be rendered as strings, typically within DDL constructs such as
        CHECK constraints and indexes (note that "literal bind" values
        become used by DDL as of :ticket:`2742`).  A new method
        :meth:`.TypeEngine.literal_processor` serves as the base, and
        :meth:`.TypeDecorator.process_literal_param` is added to allow wrapping
        of a native literal rendering method.

        .. seealso::

            :ref:`change_2838`

    .. change::
        :tags: feature, sql
        :tickets: 2716

        The :meth:`.Table.tometadata` method now produces copies of
        all :attr:`.SchemaItem.info` dictionaries from all :class:`.SchemaItem`
        objects within the structure including columns, constraints,
        foreign keys, etc.   As these dictionaries
        are copies, they are independent of the original dictionary.
        Previously, only the ``.info`` dictionary of :class:`.Column` was transferred
        within this operation, and it was only linked in place, not copied.

    .. change::
        :tags: feature, postgresql
        :tickets: 2840

        Added support for rendering ``SMALLSERIAL`` when a :class:`.SmallInteger`
        type is used on a primary key autoincrement column, based on server
        version detection of Postgresql version 9.2 or greater.

    .. change::
        :tags: feature, mysql
        :tickets: 2817

        The MySQL :class:`.mysql.SET` type now features the same auto-quoting
        behavior as that of :class:`.mysql.ENUM`.  Quotes are not required when
        setting up the value, but quotes that are present will be auto-detected
        along with a warning.  This also helps with Alembic where
        the SET type doesn't render with quotes.

    .. change::
        :tags: feature, sql

        The ``default`` argument of :class:`.Column` now accepts a class
        or object method as an argument, in addition to a standalone function;
        will properly detect if the "context" argument is accepted or not.

    .. change::
        :tags: bug, sql
        :tickets: 2835

        The "name" attribute is set on :class:`.Index` before the "attach"
        events are called, so that attachment events can be used to dynamically
        generate a name for the index based on the parent table and/or
        columns.

    .. change::
        :tags: bug, engine
        :tickets: 2748

        The method signature of :meth:`.Dialect.reflecttable`, which in
        all known cases is provided by :class:`.DefaultDialect`, has been
        tightened to expect ``include_columns`` and ``exclude_columns``
        arguments without any kw option, reducing ambiguity - previously
        ``exclude_columns`` was missing.

    .. change::
        :tags: bug, sql
        :tickets: 2831

        The erroneous kw arg "schema" has been removed from the :class:`.ForeignKey`
        object. this was an accidental commit that did nothing; a warning is raised
        in 0.8.3 when this kw arg is used.

    .. change::
        :tags: feature, orm
        :tickets: 1418

        Added a new load option :func:`.orm.load_only`.  This allows a series
        of column names to be specified as loading "only" those attributes,
        deferring the rest.

    .. change::
        :tags: feature, orm
        :tickets: 1418

        The system of loader options has been entirely rearchitected to build
        upon a much more comprehensive base, the :class:`.Load` object.  This
        base allows any common loader option like :func:`.joinedload`,
        :func:`.defer`, etc. to be used in a "chained" style for the purpose
        of specifying options down a path, such as ``joinedload("foo").subqueryload("bar")``.
        The new system supersedes the usage of dot-separated path names,
        multiple attributes within options, and the usage of ``_all()`` options.

        .. seealso::

            :ref:`feature_1418`

    .. change::
        :tags: feature, orm
        :tickets: 2824

        The :func:`.composite` construct now maintains the return object
        when used in a column-oriented :class:`.Query`, rather than expanding
        out into individual columns.  This makes use of the new :class:`.Bundle`
        feature internally.  This behavior is backwards incompatible; to
        select from a composite column which will expand out, use
        ``MyClass.some_composite.clauses``.

        .. seealso::

            :ref:`migration_2824`

    .. change::
        :tags: feature, orm
        :tickets: 2824

        A new construct :class:`.Bundle` is added, which allows for specification
        of groups of column expressions to a :class:`.Query` construct.
        The group of columns are returned as a single tuple by default.  The
        behavior of :class:`.Bundle` can be overridden however to provide
        any sort of result processing to the returned row.  The behavior
        of :class:`.Bundle` is also embedded into composite attributes now
        when they are used in a column-oriented :class:`.Query`.

        .. seealso::

            :ref:`change_2824`

            :ref:`migration_2824`

    .. change::
        :tags: bug, sql
        :tickets: 2812

        A rework to the way that "quoted" identifiers are handled, in that
        instead of relying upon various ``quote=True`` flags being passed around,
        these flags are converted into rich string objects with quoting information
        included at the point at which they are passed to common schema constructs
        like :class:`.Table`, :class:`.Column`, etc.   This solves the issue
        of various methods that don't correctly honor the "quote" flag such
        as :meth:`.Engine.has_table` and related methods.  The :class:`.quoted_name`
        object is a string subclass that can also be used explicitly if needed;
        the object will hold onto the quoting preferences passed and will
        also bypass the "name normalization" performed by dialects that
        standardize on uppercase symbols, such as Oracle, Firebird and DB2.
        The upshot is that the "uppercase" backends can now work with force-quoted
        names, such as lowercase-quoted names and new reserved words.

        .. seealso::

            :ref:`change_2812`

    .. change::
        :tags: feature, orm
        :tickets: 2793

        The ``version_id_generator`` parameter of ``Mapper`` can now be specified
        to rely upon server generated version identifiers, using triggers
        or other database-provided versioning features, or via an optional programmatic
        value, by setting ``version_id_generator=False``.
        When using a server-generated version identfier, the ORM will use RETURNING when
        available to immediately
        load the new version value, else it will emit a second SELECT.

    .. change::
        :tags: feature, orm
        :tickets: 2793

        The ``eager_defaults`` flag of :class:`.Mapper` will now allow the
        newly generated default values to be fetched using an inline
        RETURNING clause, rather than a second SELECT statement, for backends
        that support RETURNING.

    .. change::
        :tags: feature, core
        :tickets: 2793

        Added a new variant to :meth:`.ValuesBase.returning` called
        :meth:`.ValuesBase.return_defaults`; this allows arbitrary columns
        to be added to the RETURNING clause of the statement without interfering
        with the compilers usual "implicit returning" feature, which is used to
        efficiently fetch newly generated primary key values.  For supporting
        backends, a dictionary of all fetched values is present at
        :attr:`.ResultProxy.returned_defaults`.

    .. change::
        :tags: bug, mysql

        Improved support for the cymysql driver, supporting version 0.6.5,
        courtesy Hajime Nakagami.

    .. change::
        :tags: general

        A large refactoring of packages has reorganized
        the import structure of many Core modules as well as some aspects
        of the ORM modules.  In particular ``sqlalchemy.sql`` has been broken
        out into several more modules than before so that the very large size
        of ``sqlalchemy.sql.expression`` is now pared down.   The effort
        has focused on a large reduction in import cycles.   Additionally,
        the system of API functions in ``sqlalchemy.sql.expression`` and
        ``sqlalchemy.orm`` has been reorganized to eliminate redundancy
        in documentation between the functions vs. the objects they produce.

    .. change::
        :tags: orm, feature, orm

        Added a new attribute :attr:`.Session.info` to :class:`.Session`;
        this is a dictionary where applications can store arbitrary
        data local to a :class:`.Session`.
        The contents of :attr:`.Session.info` can be also be initialized
        using the ``info`` argument of :class:`.Session` or
        :class:`.sessionmaker`.


    .. change::
        :tags: feature, general, py3k
        :tickets: 2161

        The C extensions are ported to Python 3 and will build under
        any supported CPython 2 or 3 environment.

    .. change::
        :tags: feature, orm
        :tickets: 2268

        Removal of event listeners is now implemented.    The feature is
        provided via the :func:`.event.remove` function.

        .. seealso::

            :ref:`feature_2268`

    .. change::
        :tags: feature, orm
        :tickets: 2789

        The mechanism by which attribute events pass along an
        :class:`.AttributeImpl` as an "initiator" token has been changed;
        the object is now an event-specific object called :class:`.attributes.Event`.
        Additionally, the attribute system no longer halts events based
        on a matching "initiator" token; this logic has been moved to be
        specific to ORM backref event handlers, which are the typical source
        of the re-propagation of an attribute event onto subsequent append/set/remove
        operations.  End user code which emulates the behavior of backrefs
        must now ensure that recursive event propagation schemes are halted,
        if the scheme does not use the backref handlers.   Using this new system,
        backref handlers can now peform a
        "two-hop" operation when an object is appended to a collection,
        associated with a new many-to-one, de-associated with the previous
        many-to-one, and then removed from a previous collection.   Before this
        change, the last step of removal from the previous collection would
        not occur.

        .. seealso::

            :ref:`migration_2789`

    .. change::
        :tags: feature, sql
        :tickets: 722

        Added new method to the :func:`.insert` construct
        :meth:`.Insert.from_select`.  Given a list of columns and
        a selectable, renders ``INSERT INTO (table) (columns) SELECT ..``.
        While this feature is highlighted as part of 0.9 it is also
        backported to 0.8.3.

        .. seealso::

            :ref:`feature_722`

    .. change::
        :tags: feature, engine
        :tickets: 2770

        New events added to :class:`.ConnectionEvents`:

        * :meth:`.ConnectionEvents.engine_connect`
        * :meth:`.ConnectionEvents.set_connection_execution_options`
        * :meth:`.ConnectionEvents.set_engine_execution_options`

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

        .. seealso::

            :ref:`migration_1765`

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
        :tags: bug, mssql, pyodbc
        :tickets: 2355

        Fixes to MSSQL with Python 3 + pyodbc, including that statements
        are passed correctly.

    .. change::
        :tags: feature, sql
        :tickets: 1068

        A :class:`.Label` construct will now render as its name alone
        in an ``ORDER BY`` clause, if that label is also referred to
        in the columns clause of the select, instead of rewriting the
        full expression.  This gives the database a better chance to
        optimize the evaulation of the same expression in two different
        contexts.

        .. seealso::

            :ref:`migration_1068`

    .. change::
        :tags: feature, firebird
        :tickets: 2504

        The ``fdb`` dialect is now the default dialect when
        specified without a dialect qualifier, i.e. ``firebird://``,
        per the Firebird project publishing ``fdb`` as their
        official Python driver.

    .. change::
    	:tags: feature, general, py3k
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
