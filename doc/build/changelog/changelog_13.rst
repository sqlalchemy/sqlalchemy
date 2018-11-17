=============
1.3 Changelog
=============

.. changelog_imports::

    .. include:: changelog_12.rst
        :start-line: 5

    .. include:: changelog_11.rst
        :start-line: 5

.. changelog::
    :version: 1.3.0b1
    :released: November 16, 2018

    .. change::
       :tags: bug, ext
       :tickets: 3423

       Reworked :class:`.AssociationProxy` to store state that's specific to a
       parent class in a separate object, so that a single
       :class:`.AssocationProxy` can serve for multiple parent classes, as is
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
        clause used primarily with Postgresql, the FOR UPDATE is rendered only on
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

       Added rudimental support for reflection of Postgresql
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
        already been called and memoized.  Addtionally, a ``NotImplementedError``
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
        to the :class:`.ShardedQuery` class within the horiziontal sharding
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
