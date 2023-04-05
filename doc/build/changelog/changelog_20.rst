=============
2.0 Changelog
=============

.. changelog_imports::

    .. include:: changelog_14.rst
        :start-line: 5


.. changelog::
    :version: 2.0.9
    :released: April 5, 2023

    .. change::
        :tags: bug, mssql
        :tickets: 9603

        Due to a critical bug identified in SQL Server, the SQLAlchemy
        "insertmanyvalues" feature which allows fast INSERT of many rows while also
        supporting RETURNING unfortunately needs to be disabled for SQL Server. SQL
        Server is apparently unable to guarantee that the order of rows inserted
        matches the order in which they are sent back by OUTPUT inserted when
        table-valued rows are used with INSERT in conjunction with OUTPUT inserted.
        We are trying to see if Microsoft is able to confirm this undocumented
        behavior however there is no known workaround, other than it's not safe to
        use table-valued expressions with OUTPUT inserted for now.


    .. change::
        :tags: bug, mariadb
        :tickets: 9588

        Added ``row_number`` as reserved word in MariaDb.

    .. change::
        :tags: bug, mssql
        :tickets: 9586

        Changed the bulk INSERT strategy used for SQL Server "executemany" with
        pyodbc when ``fast_executemany`` is set to ``True`` by using
        ``fast_executemany`` / ``cursor.executemany()`` for bulk INSERT that does
        not include RETURNING, restoring the same behavior as was used in
        SQLAlchemy 1.4 when this parameter is set.

        New performance details from end users have shown that ``fast_executemany``
        is still much faster for very large datasets as it uses ODBC commands that
        can receive all rows in a single round trip, allowing for much larger
        datasizes than the batches that can be sent by "insertmanyvalues"
        as was implemented for SQL Server.

        While this change was made such that "insertmanyvalues" continued to be
        used for INSERT that includes RETURNING, as well as if ``fast_executemany``
        were not set, due to :ticket:`9603`, the "insertmanyvalues" strategy has
        been disabled for SQL Server across the board in any case.

.. changelog::
    :version: 2.0.8
    :released: March 31, 2023

    .. change::
        :tags: bug, orm
        :tickets: 9553

        Fixed issue in ORM Annotated Declarative where using a recursive type (e.g.
        using a nested Dict type) would result in a recursion overflow in the ORM's
        annotation resolution logic, even if this datatype were not necessary to
        map the column.

    .. change::
        :tags: bug, examples

        Fixed issue in "versioned history" example where using a declarative base
        that is derived from :class:`_orm.DeclarativeBase` would fail to be mapped.
        Additionally, repaired the given test suite so that the documented
        instructions for running the example using Python unittest now work again.

    .. change::
        :tags: bug, orm
        :tickets: 9550

        Fixed issue where the :func:`_orm.mapped_column` construct would raise an
        internal error if used on a Declarative mixin and included the
        :paramref:`_orm.mapped_column.deferred` parameter.

    .. change::
        :tags: bug, mysql
        :tickets: 9544

        Fixed issue where string datatypes such as :class:`_sqltypes.CHAR`,
        :class:`_sqltypes.VARCHAR`, :class:`_sqltypes.TEXT`, as well as binary
        :class:`_sqltypes.BLOB`, could not be produced with an explicit length of
        zero, which has special meaning for MySQL. Pull request courtesy J. Nick
        Koston.

    .. change::
        :tags: bug, orm
        :tickets: 9537

        Expanded the warning emitted when a plain :func:`_sql.column` object is
        present in a Declarative mapping to include any arbitrary SQL expression
        that is not declared within an appropriate property type such as
        :func:`_orm.column_property`, :func:`_orm.deferred`, etc. These attributes
        are otherwise not mapped at all and remain unchanged within the class
        dictionary. As it seems likely that such an expression is usually not
        what's intended, this case now warns for all such otherwise ignored
        expressions, rather than just the :func:`_sql.column` case.

    .. change::
        :tags: bug, orm
        :tickets: 9519

        Fixed regression where accessing the expression value of a hybrid property
        on a class that was either unmapped or not-yet-mapped (such as calling upon
        it within a :func:`_orm.declared_attr` method) would raise an internal
        error, as an internal fetch for the parent class' mapper would fail and an
        instruction for this failure to be ignored were inadvertently removed in
        2.0.

    .. change::
        :tags: bug, orm
        :tickets: 9350

        Fields that are declared on Declarative Mixins and then combined with
        classes that make use of :class:`_orm.MappedAsDataclass`, where those mixin
        fields are not themselves part of a dataclass, now emit a deprecation
        warning as these fields will be ignored in a future release, as Python
        dataclasses behavior is to ignore these fields. Type checkers will not see
        these fields under pep-681.

        .. seealso::

            :ref:`error_dcmx` - background on rationale

            :ref:`orm_declarative_dc_mixins`

    .. change::
        :tags: bug, postgresql
        :tickets: 9511

        Fixed critical regression in PostgreSQL dialects such as asyncpg which rely
        upon explicit casts in SQL in order for datatypes to be passed to the
        driver correctly, where a :class:`.String` datatype would be cast along
        with the exact column length being compared, leading to implicit truncation
        when comparing a ``VARCHAR`` of a smaller length to a string of greater
        length regardless of operator in use (e.g. LIKE, MATCH, etc.). The
        PostgreSQL dialect now omits the length from ``VARCHAR`` when rendering
        these casts.

    .. change::
        :tags: bug, util
        :tickets: 9487

        Implemented missing methods ``copy`` and ``pop`` in
        OrderedSet class.

    .. change::
        :tags: bug, typing
        :tickets: 9536

        Fixed typing for :func:`_orm.deferred` and :func:`_orm.query_expression`
        to work correctly with 2.0 style mappings.

    .. change::
        :tags: bug, orm
        :tickets: 9526

        Fixed issue where the :meth:`_sql.BindParameter.render_literal_execute`
        method would fail when called on a parameter that also had ORM annotations
        associated with it. In practice, this would be observed as a failure of SQL
        compilation when using some combinations of a dialect that uses "FETCH
        FIRST" such as Oracle along with a :class:`_sql.Select` construct that uses
        :meth:`_sql.Select.limit`, within some ORM contexts, including if the
        statement were embedded within a relationship primaryjoin expression.


    .. change::
        :tags: usecase, orm
        :tickets: 9563

        Exceptions such as ``TypeError`` and ``ValueError`` raised by Python
        dataclasses when making use of the :class:`_orm.MappedAsDataclass` mixin
        class or :meth:`_orm.registry.mapped_as_dataclass` decorator are now
        wrapped within an :class:`.InvalidRequestError` wrapper along with
        informative context about the error message, referring to the Python
        dataclasses documentation as the authoritative source of background
        information on the cause of the exception.

        .. seealso::

            :ref:`error_dcte`


    .. change::
        :tags: bug, orm
        :tickets: 9549

        Towards maintaining consistency with unit-of-work changes made for
        :ticket:`5984` and :ticket:`8862`, both of which disable "lazy='raise'"
        handling within :class:`_orm.Session` processes that aren't triggered by
        attribute access, the :meth:`_orm.Session.delete` method will now also
        disable "lazy='raise'" handling when it traverses relationship paths in
        order to process the "delete" and "delete-orphan" cascade rules.
        Previously, there was no easy way to generically call
        :meth:`_orm.Session.delete` on an object that had "lazy='raise'" set up
        such that only the necessary relationships would be loaded. As
        "lazy='raise'" is primarily intended to catch SQL loading that emits on
        attribute access, :meth:`_orm.Session.delete` is now made to behave like
        other :class:`_orm.Session` methods including :meth:`_orm.Session.merge` as
        well as :meth:`_orm.Session.flush` along with autoflush.

    .. change::
        :tags: bug, orm
        :tickets: 9564

        Fixed issue where an annotation-only :class:`_orm.Mapped` directive could
        not be used in a Declarative mixin class, without that attribute attempting
        to take effect for single- or joined-inheritance subclasses of mapped
        classes that had already mapped that attribute on a superclass, producing
        conflicting column errors and/or warnings.


    .. change::
        :tags: bug, orm, typing
        :tickets: 9514

        Properly type :paramref:`_dml.Insert.from_select.names` to accept
        a list of string or columns or mapped attributes.

.. changelog::
    :version: 2.0.7
    :released: March 18, 2023

    .. change::
        :tags: usecase, postgresql
        :tickets: 9416

        Added new PostgreSQL type :class:`_postgresql.CITEXT`. Pull request
        courtesy Julian David Rath.

    .. change::
        :tags: bug, typing
        :tickets: 9502

        Fixed typing issue where :func:`_orm.composite` would not allow an
        arbitrary callable as the source of the composite class.

    .. change::
          :tags: usecase, postgresql
          :tickets: 9442

          Modifications to the base PostgreSQL dialect to allow for better integration with the
          sqlalchemy-redshift third party dialect for SQLAlchemy 2.0. Pull request courtesy
          matthewgdv.

.. changelog::
    :version: 2.0.6
    :released: March 13, 2023

    .. change::
        :tags: bug, sql, regression
        :tickets: 9461

        Fixed regression where the fix for :ticket:`8098`, which was released in
        the 1.4 series and provided a layer of concurrency-safe checks for the
        lambda SQL API, included additional fixes in the patch that failed to be
        applied to the main branch. These additional fixes have been applied.

    .. change::
        :tags: bug, typing
        :tickets: 9451

        Fixed typing issue where :meth:`.ColumnElement.cast` did not allow a
        :class:`.TypeEngine` argument independent of the type of the
        :class:`.ColumnElement` itself, which is the purpose of
        :meth:`.ColumnElement.cast`.

    .. change::
        :tags: bug, orm
        :tickets: 9460

        Fixed bug where the "active history" feature was not fully
        implemented for composite attributes, making it impossible to receive
        events that included the "old" value.   This seems to have been the case
        with older SQLAlchemy versions as well, where "active_history" would
        be propagated to the underlying column-based attributes, but an event
        handler listening to the composite attribute itself would not be given
        the "old" value being replaced, even if the composite() were set up
        with active_history=True.

        Additionally, fixed a regression that's local to 2.0 which disallowed
        active_history on composite from being assigned to the impl with
        ``attr.impl.active_history=True``.


    .. change::
        :tags: bug, oracle
        :tickets: 9459

        Fixed reflection bug where Oracle "name normalize" would not work correctly
        for reflection of symbols that are in the "PUBLIC" schema, such as
        synonyms, meaning the PUBLIC name could not be indicated as lower case on
        the Python side for the :paramref:`_schema.Table.schema` argument. Using
        uppercase "PUBLIC" would work, but would then lead to awkward SQL queries
        including a quoted ``"PUBLIC"`` name as well as indexing the table under
        uppercase "PUBLIC", which was inconsistent.

    .. change::
        :tags: bug, typing

        Fixed issues to allow typing tests to pass under Mypy 1.1.1.

    .. change::
        :tags: bug, sql
        :tickets: 9440

        Fixed regression where the :func:`_sql.select` construct would not be able
        to render if it were given no columns and then used in the context of an
        EXISTS, raising an internal exception instead. While an empty "SELECT" is
        not typically valid SQL, in the context of EXISTS databases such as
        PostgreSQL allow it, and in any case the condition now no longer raises
        an internal exception.


    .. change::
        :tags: bug, orm
        :tickets: 9418

        Fixed regression involving pickling of Python rows between the cython and
        pure Python implementations of :class:`.Row`, which occurred as part of
        refactoring code for version 2.0 with typing. A particular constant were
        turned into a string based ``Enum`` for the pure Python version of
        :class:`.Row` whereas the cython version continued to use an integer
        constant, leading to deserialization failures.

.. changelog::
    :version: 2.0.5.post1
    :released: March 5, 2023

    .. change::
        :tags: bug, orm
        :tickets: 9418

        Added constructor arguments to the built-in mapping collection types
        including :class:`.KeyFuncDict`, :func:`_orm.attribute_keyed_dict`,
        :func:`_orm.column_keyed_dict` so that these dictionary types may be
        constructed in place given the data up front; this provides further
        compatibility with tools such as Python dataclasses ``.asdict()`` which
        relies upon invoking these classes directly as ordinary dictionary classes.

    .. change::
        :tags: bug, orm, regression
        :tickets: 9424

        Fixed multiple regressions due to :ticket:`8372`, involving
        :func:`_orm.attribute_mapped_collection` (now called
        :func:`_orm.attribute_keyed_dict`).

        First, the collection was no longer usable with "key" attributes that were
        not themselves ordinary mapped attributes; attributes linked to descriptors
        and/or association proxy attributes have been fixed.

        Second, if an event or other operation needed access to the "key" in order
        to populate the dictionary from an mapped attribute that was not
        loaded, this also would raise an error inappropriately, rather than
        trying to load the attribute as was the behavior in 1.4.  This is also
        fixed.

        For both cases, the behavior of :ticket:`8372` has been expanded.
        :ticket:`8372` introduced an error that raises when the derived key that
        would be used as a mapped dictionary key is effectively unassigned. In this
        change, a warning only is emitted if the effective value of the ".key"
        attribute is ``None``, where it cannot be unambiguously determined if this
        ``None`` was intentional or not. ``None`` will be not supported as mapped
        collection dictionary keys going forward (as it typically refers to NULL
        which means "unknown"). Setting
        :paramref:`_orm.attribute_keyed_dict.ignore_unpopulated_attribute` will now
        cause such ``None`` keys to be ignored as well.

    .. change::
        :tags: engine, performance
        :tickets: 9343

        A small optimization to the Cython implementation of :class:`.Result`
        using a cdef for a particular int value to avoid Python overhead. Pull
        request courtesy Matus Valo.


    .. change::
        :tags: bug, mssql
        :tickets: 9414

        Fixed issue in the new :class:`.Uuid` datatype which prevented it from
        working with the pymssql driver. As pymssql seems to be maintained again,
        restored testing support for pymssql.

    .. change::
        :tags: bug, mssql

        Tweaked the pymssql dialect to take better advantage of
        RETURNING for INSERT statements in order to retrieve last inserted primary
        key values, in the same way as occurs for the mssql+pyodbc dialect right
        now.

    .. change::
        :tags: bug, orm

        Identified that the ``sqlite`` and ``mssql+pyodbc`` dialects are now
        compatible with the SQLAlchemy ORM's "versioned rows" feature, since
        SQLAlchemy now computes rowcount for a RETURNING statement in this specific
        case by counting the rows returned, rather than relying upon
        ``cursor.rowcount``.  In particular, the ORM versioned rows use case
        (documented at :ref:`mapper_version_counter`) should now be fully
        supported with the SQL Server pyodbc dialect.


    .. change::
        :tags: bug, postgresql
        :tickets: 9349

        Fixed issue in PostgreSQL :class:`_postgresql.ExcludeConstraint` where
        literal values were being compiled as bound parameters and not direct
        inline values as is required for DDL.

    .. change::
        :tags: bug, typing

        Fixed bug where the :meth:`_engine.Connection.scalars` method was not typed
        as allowing a multiple-parameters list, which is now supported using
        insertmanyvalues operations.

    .. change::
        :tags: bug, typing
        :tickets: 9376

        Improved typing for the mapping passed to :meth:`.Insert.values` and
        :meth:`.Update.values` to be more open-ended about collection type, by
        indicating read-only ``Mapping`` instead of writeable ``Dict`` which would
        error out on too limited of a key type.

    .. change::
        :tags: schema

        Validate that when provided the :paramref:`_schema.MetaData.schema`
        argument of :class:`_schema.MetaData` is a string.

    .. change::
        :tags: typing, usecase
        :tickets: 9338

        Exported the type returned by
        :meth:`_orm.scoped_session.query_property` using a new public type
        :class:`.orm.QueryPropertyDescriptor`.

    .. change::
        :tags: bug, mysql, postgresql
        :tickets: 5648

        The support for pool ping listeners to receive exception events via the
        :meth:`.DialectEvents.handle_error` event added in 2.0.0b1 for
        :ticket:`5648` failed to take into account dialect-specific ping routines
        such as that of MySQL and PostgreSQL. The dialect feature has been reworked
        so that all dialects participate within event handling.   Additionally,
        a new boolean element :attr:`.ExceptionContext.is_pre_ping` is added
        which identifies if this operation is occurring within the pre-ping
        operation.

        For this release, third party dialects which implement a custom
        :meth:`_engine.Dialect.do_ping` method can opt in to the newly improved
        behavior by having their method no longer catch exceptions or check
        exceptions for "is_disconnect", instead just propagating all exceptions
        outwards. Checking the exception for "is_disconnect" is now done by an
        enclosing method on the default dialect, which ensures that the event hook
        is invoked for all exception scenarios before testing the exception as a
        "disconnect" exception. If an existing ``do_ping()`` method continues to
        catch exceptions and check "is_disconnect", it will continue to work as it
        did previously, but ``handle_error`` hooks will not have access to the
        exception if it isn't propagated outwards.

    .. change::
        :tags: bug, ext
        :tickets: 9367

        Fixed issue in automap where calling :meth:`_automap.AutomapBase.prepare`
        from a specific mapped class, rather than from the
        :class:`_automap.AutomapBase` directly, would not use the correct base
        class when automap detected new tables, instead using the given class,
        leading to mappers trying to configure inheritance. While one should
        normally call :meth:`_automap.AutomapBase.prepare` from the base in any
        case, it shouldn't misbehave that badly when called from a subclass.


    .. change::
        :tags: bug, sqlite, regression
        :tickets: 9379

        Fixed regression for SQLite connections where use of the ``deterministic``
        parameter when establishing database functions would fail for older SQLite
        versions, those prior to version 3.8.3. The version checking logic has been
        improved to accommodate for this case.

    .. change::
        :tags: bug, typing
        :tickets: 9391

        Added missing init overload to the :class:`_types.Numeric` type object so
        that pep-484 type checkers may properly resolve the complete type, deriving
        from the :paramref:`_types.Numeric.asdecimal` parameter whether ``Decimal``
        or ``float`` objects will be represented.

    .. change::
        :tags: bug, typing
        :tickets: 9398

        Fixed typing bug where :meth:`_sql.Select.from_statement` would not accept
        :func:`_sql.text` or :class:`.TextualSelect` objects as a valid type.
        Additionally repaired the :class:`.TextClause.columns` method to have a
        return type, which was missing.

    .. change::
        :tags: bug, orm declarative
        :tickets: 9332

        Fixed issue where new :paramref:`_orm.mapped_column.use_existing_column`
        feature would not work if the two same-named columns were mapped under
        attribute names that were differently-named from an explicit name given to
        the column itself. The attribute names can now be differently named when
        using this parameter.

    .. change::
        :tags: bug, orm
        :tickets: 9373

        Added support for the :paramref:`_orm.Mapper.polymorphic_load` parameter to
        be applied to each mapper in an inheritance hierarchy more than one level
        deep, allowing columns to load for all classes in the hierarchy that
        indicate ``"selectin"`` using a single statement, rather than ignoring
        elements on those intermediary classes that nonetheless indicate they also
        would participate in ``"selectin"`` loading and were not part of the
        base-most SELECT statement.

    .. change::
        :tags: bug, orm
        :tickets: 8853, 9335

        Continued the fix for :ticket:`8853`, allowing the :class:`_orm.Mapped`
        name to be fully qualified regardless of whether or not
        ``from __annotations__ import future`` were present. This issue first fixed
        in 2.0.0b3 confirmed that this case worked via the test suite, however the
        test suite apparently was not testing the behavior for the name
        :class:`_orm.Mapped` not being locally present at all; string resolution
        has been updated to ensure the :class:`_orm.Mapped` symbol is locatable as
        applies to how the ORM uses these functions.

    .. change::
        :tags: bug, typing
        :tickets: 9340

        Fixed typing issue where :func:`_orm.with_polymorphic` would not
        record the class type correctly.

    .. change::
        :tags: bug, ext, regression
        :tickets: 9380

        Fixed regression caused by typing added to ``sqlalchemy.ext.mutable`` for
        :ticket:`8667`, where the semantics of the ``.pop()`` method changed such
        that the method was non-working. Pull request courtesy Nils Philippsen.

    .. change::
        :tags: bug, sql, regression
        :tickets: 9390

        Restore the :func:`.nullslast` and :func:`.nullsfirst` legacy functions
        into the ``sqlalchemy`` import namespace. Previously, the newer
        :func:`.nulls_last` and :func:`.nulls_first` functions were available, but
        the legacy ones were inadvertently removed.

    .. change::
        :tags: bug, postgresql
        :tickets: 9401

        Fixed issue where the PostgreSQL :class:`_postgresql.ExcludeConstraint`
        construct would not be copyable within operations such as
        :meth:`_schema.Table.to_metadata` as well as within some Alembic scenarios,
        if the constraint contained textual expression elements.

    .. change::
        :tags: bug, engine
        :tickets: 9423

        Fixed bug where :class:`_engine.Row` objects could not be reliably unpickled
        across processes due to an accidental reliance on an unstable hash value.

.. changelog::
    :version: 2.0.4
    :released: February 17, 2023

    .. change::
        :tags: bug, orm, regression
        :tickets: 9273

        Fixed regression introduced in version 2.0.2 due to :ticket:`9217` where
        using DML RETURNING statements, as well as
        :meth:`_sql.Select.from_statement` constructs as was "fixed" in
        :ticket:`9217`, in conjunction with ORM mapped classes that used
        expressions such as with :func:`_orm.column_property`, would lead to an
        internal error within Core where it would attempt to match the expression
        by name. The fix repairs the Core issue, and also adjusts the fix in
        :ticket:`9217` to not take effect for the DML RETURNING use case, where it
        adds unnecessary overhead.

    .. change::
        :tags: usecase, typing
        :tickets: 9321

        Improved the typing support for the :ref:`hybrids_toplevel`
        extension, updated all documentation to use ORM Annotated Declarative
        mappings, and added a new modifier called :attr:`.hybrid_property.inplace`.
        This modifier provides a way to alter the state of a :class:`.hybrid_property`
        **in place**, which is essentially what very early versions of hybrids
        did, before SQLAlchemy version 1.2.0 :ticket:`3912` changed this to
        remove in-place mutation.  This in-place mutation is now restored on an
        **opt-in** basis to allow a single hybrid to have multiple methods
        set up, without the need to name all the methods the same and without the
        need to carefully "chain" differently-named methods in order to maintain
        the composition.  Typing tools such as Mypy and Pyright do not allow
        same-named methods on a class, so with this change a succinct method
        of setting up hybrids with typing support is restored.

        .. seealso::

            :ref:`hybrid_pep484_naming`

    .. change::
        :tags: bug, orm

        Marked the internal ``EvaluatorCompiler`` module as private to the ORM, and
        renamed it to ``_EvaluatorCompiler``. For users that may have been relying
        upon this, the name ``EvaluatorCompiler`` is still present, however this
        use is not supported and will be removed in a future release.

    .. change::
        :tags: orm, use_case
        :tickets: 9297

        To accommodate a change in column ordering used by ORM Declarative in
        SQLAlchemy 2.0, a new parameter :paramref:`_orm.mapped_column.sort_order`
        has been added that can be used to control the order of the columns defined
        in the table by the ORM, for common use cases such as mixins with primary
        key columns that should appear first in tables. The change notes at
        :ref:`change_9297` illustrate the default change in ordering behavior
        (which is part of all SQLAlchemy 2.0 releases) as well as use of the
        :paramref:`_orm.mapped_column.sort_order` to control column ordering when
        using mixins and multiple classes (new in 2.0.4).

        .. seealso::

            :ref:`change_9297`

    .. change::
        :tags: sql
        :tickets: 9277

        Added public property :attr:`_schema.Table.autoincrement_column` that
        returns the column identified as autoincrementing in the column.

    .. change::
        :tags: oracle, bug
        :tickets: 9295

        Adjusted the behavior of the ``thick_mode`` parameter for the
        :ref:`oracledb` dialect to correctly accept ``False`` as a value.
        Previously, only ``None`` would indicate that thick mode should be
        disabled.

    .. change::
        :tags: usecase, orm
        :tickets: 9298

        The :meth:`_orm.Session.refresh` method will now immediately load a
        relationship-bound attribute that is explicitly named within the
        :paramref:`_orm.Session.refresh.attribute_names` collection even if it is
        currently linked to the "select" loader, which normally is a "lazy" loader
        that does not fire off during a refresh. The "lazy loader" strategy will
        now detect that the operation is specifically a user-initiated
        :meth:`_orm.Session.refresh` operation which named this attribute
        explicitly, and will then call upon the "immediateload" strategy to
        actually emit SQL to load the attribute. This should be helpful in
        particular for some asyncio situations where the loading of an unloaded
        lazy-loaded attribute must be forced, without using the actual lazy-loading
        attribute pattern not supported in asyncio.


    .. change::
        :tags: bug, sql
        :tickets: 9313

        Fixed issue where element types of a tuple value would be hardcoded to take
        on the types from a compared-to tuple, when the comparison were using the
        :meth:`.ColumnOperators.in_` operator. This was inconsistent with the usual
        way that types are determined for a binary expression, which is that the
        actual element type on the right side is considered first before applying
        the left-hand-side type.

    .. change::
        :tags: usecase, orm declarative
        :tickets: 9266

        Added new parameter ``dataclasses_callable`` to both the
        :class:`_orm.MappedAsDataclass` class as well as the
        :meth:`_orm.registry.mapped_as_dataclass` method which allows an
        alternative callable to Python ``dataclasses.dataclass`` to be used in
        order to produce dataclasses. The use case here is to drop in Pydantic's
        dataclass function instead. Adjustments have been made to the mixin support
        added for :ticket:`9179` in version 2.0.1 so that the ``__annotations__``
        collection of the mixin is rewritten to not include the
        :class:`_orm.Mapped` container, in the same way as occurs with mapped
        classes, so that the Pydantic dataclasses constructor is not exposed to
        unknown types.

        .. seealso::

            :ref:`dataclasses_pydantic`


.. changelog::
    :version: 2.0.3
    :released: February 9, 2023

    .. change::
        :tags: typing, bug
        :tickets: 9254

        Remove ``typing.Self`` workaround, now using :pep:`673` for most methods
        that return ``Self``. As a consequence of this change ``mypy>=1.0.0`` is
        now required to type check SQLAlchemy code.
        Pull request courtesy Yurii Karabas.

    .. change::
        :tags: bug, sql, regression
        :tickets: 9271

        Fixed critical regression in SQL expression formulation in the 2.0 series
        due to :ticket:`7744` which improved support for SQL expressions that
        contained many elements against the same operator repeatedly; parenthesis
        grouping would be lost with expression elements beyond the first two
        elements.


.. changelog::
    :version: 2.0.2
    :released: February 6, 2023

    .. change::
        :tags: bug, orm declarative
        :tickets: 9249

        Fixed regression caused by the fix for :ticket:`9171`, which itself was
        fixing a regression, involving the mechanics of ``__init__()`` on classes
        that extend from :class:`_orm.DeclarativeBase`. The change made it such
        that ``__init__()`` was applied to the user-defined base if there were no
        ``__init__()`` method directly on the class. This has been adjusted so that
        ``__init__()`` is applied only if no other class in the hierarchy of the
        user-defined base has an ``__init__()`` method. This again allows
        user-defined base classes based on :class:`_orm.DeclarativeBase` to include
        mixins that themselves include a custom ``__init__()`` method.

    .. change::
        :tags: bug, mysql, regression
        :tickets: 9251

        Fixed regression caused by issue :ticket:`9058` which adjusted the MySQL
        dialect's ``has_table()`` to again use "DESCRIBE", where the specific error
        code raised by MySQL version 8 when using a non-existent schema name was
        unexpected and failed to be interpreted as a boolean result.



    .. change::
        :tags: bug, sqlite
        :tickets: 9251

        Fixed the SQLite dialect's ``has_table()`` function to correctly report
        False for queries that include a non-None schema name for a schema that
        doesn't exist; previously, a database error was raised.


    .. change::
        :tags: bug, orm declarative
        :tickets: 9226

        Fixed issue in ORM Declarative Dataclass mappings related to newly added
        support for mixins added in 2.0.1 via :ticket:`9179`, where a combination
        of using mixins plus ORM inheritance would mis-classify fields in some
        cases leading to field-level dataclass arguments such as ``init=False`` being
        lost.

    .. change::
        :tags: bug, orm, ression
        :tickets: 9232

        Fixed obscure ORM inheritance issue caused by :ticket:`8705` where some
        scenarios of inheriting mappers that indicated groups of columns from the
        local table and the inheriting table together under a
        :func:`_orm.column_property` would nonetheless warn that properties of the
        same name were being combined implicitly.

    .. change::
        :tags: orm, bug, regression
        :tickets: 9228

        Fixed regression where using the :paramref:`_orm.Mapper.version_id_col`
        feature with a regular Python-side incrementing column would fail to work
        for SQLite and other databases that don't support "rowcount" with
        "RETURNING", as "RETURNING" would be assumed for such columns even though
        that's not what actually takes place.

    .. change::
        :tags: bug, orm declarative
        :tickets: 9240

        Repaired ORM Declarative mappings to allow for the
        :paramref:`_orm.Mapper.primary_key` parameter to be specified within
        ``__mapper_args__`` when using :func:`_orm.mapped_column`. Despite this
        usage being directly in the 2.0 documentation, the :class:`_orm.Mapper` was
        not accepting the :func:`_orm.mapped_column` construct in this context. Ths
        feature was already working for the :paramref:`_orm.Mapper.version_id_col`
        and :paramref:`_orm.Mapper.polymorphic_on` parameters.

        As part of this change, the ``__mapper_args__`` attribute may be specified
        without using :func:`_orm.declared_attr` on a non-mapped mixin class,
        including a ``"primary_key"`` entry that refers to :class:`_schema.Column`
        or :func:`_orm.mapped_column` objects locally present on the mixin;
        Declarative will also translate these columns into the correct ones for a
        particular mapped class. This again was working already for the
        :paramref:`_orm.Mapper.version_id_col` and
        :paramref:`_orm.Mapper.polymorphic_on` parameters.  Additionally,
        elements within ``"primary_key"`` may be indicated as string names of
        existing mapped properties.

    .. change::
        :tags: usecase, sql
        :tickets: 8780

        Added a full suite of new SQL bitwise operators, for performing
        database-side bitwise expressions on appropriate data values such as
        integers, bit-strings, and similar. Pull request courtesy Yegor Statkevich.

        .. seealso::

            :ref:`operators_bitwise`


    .. change::
        :tags: bug, orm declarative
        :tickets: 9211

        An explicit error is raised if a mapping attempts to mix the use of
        :class:`_orm.MappedAsDataclass` with
        :meth:`_orm.registry.mapped_as_dataclass` within the same class hierarchy,
        as this produces issues with the dataclass function being applied at the
        wrong time to the mapped class, leading to errors during the mapping
        process.

    .. change::
        :tags: bug, orm, regression
        :tickets: 9217

        Fixed regression when using :meth:`_sql.Select.from_statement` in an ORM
        context, where matching of columns to SQL labels based on name alone was
        disabled for ORM-statements that weren't fully textual. This would prevent
        arbitrary SQL expressions with column-name labels from matching up to the
        entity to be loaded, which previously would work within the 1.4
        and previous series, so the previous behavior has been restored.

    .. change::
        :tags: bug, asyncio
        :tickets: 9237

        Repaired a regression caused by the fix for :ticket:`8419` which caused
        asyncpg connections to be reset (i.e. transaction ``rollback()`` called)
        and returned to the pool normally in the case that the connection were not
        explicitly returned to the connection pool and was instead being
        intercepted by Python garbage collection, which would fail if the garbage
        collection operation were being called outside of the asyncio event loop,
        leading to a large amount of stack trace activity dumped into logging
        and standard output.

        The correct behavior is restored, which is that all asyncio connections
        that are garbage collected due to not being explicitly returned to the
        connection pool are detached from the pool and discarded, along with a
        warning, rather than being returned the pool, as they cannot be reliably
        reset. In the case of asyncpg connections, the asyncpg-specific
        ``terminate()`` method will be used to end the connection more gracefully
        within this process as opposed to just dropping it.

        This change includes a small behavioral change that is hoped to be useful
        for debugging asyncio applications, where the warning that's emitted in the
        case of asyncio connections being unexpectedly garbage collected has been
        made slightly more aggressive by moving it outside of a ``try/except``
        block and into a ``finally:`` block, where it will emit unconditionally
        regardless of whether the detach/termination operation succeeded or not. It
        will also have the effect that applications or test suites which promote
        Python warnings to exceptions will see this as a full exception raise,
        whereas previously it was not possible for this warning to actually
        propagate as an exception. Applications and test suites which need to
        tolerate this warning in the interim should adjust the Python warnings
        filter to allow these warnings to not raise.

        The behavior for traditional sync connections remains unchanged, that
        garbage collected connections continue to be returned to the pool normally
        without emitting a warning. This will likely be changed in a future major
        release to at least emit a similar warning as is emitted for asyncio
        drivers, as it is a usage error for pooled connections to be intercepted by
        garbage collection without being properly returned to the pool.

    .. change::
        :tags: usecase, orm
        :tickets: 9220

        Added new event hook :meth:`_orm.MapperEvents.after_mapper_constructed`,
        which supplies an event hook to take place right as the
        :class:`_orm.Mapper` object has been fully constructed, but before the
        :meth:`_orm.registry.configure` call has been called. This allows code that
        can create additional mappings and table structures based on the initial
        configuration of a :class:`_orm.Mapper`, which also integrates within
        Declarative configuration. Previously, when using Declarative, where the
        :class:`_orm.Mapper` object is created within the class creation process,
        there was no documented means of running code at this point.  The change
        is to immediately benefit custom mapping schemes such as that
        of the :ref:`examples_versioned_history` example, which generate additional
        mappers and tables in response to the creation of mapped classes.


    .. change::
        :tags: usecase, orm
        :tickets: 9220

        The infrequently used :attr:`_orm.Mapper.iterate_properties` attribute and
        :meth:`_orm.Mapper.get_property` method, which are primarily used
        internally, no longer implicitly invoke the :meth:`_orm.registry.configure`
        process. Public access to these methods is extremely rare and the only
        benefit to having :meth:`_orm.registry.configure` would have been allowing
        "backref" properties be present in these collections. In order to support
        the new :meth:`_orm.MapperEvents.after_mapper_constructed` event, iteration
        and access to the internal :class:`_orm.MapperProperty` objects is now
        possible without triggering an implicit configure of the mapper itself.

        The more-public facing route to iteration of all mapper attributes, the
        :attr:`_orm.Mapper.attrs` collection and similar, will still implicitly
        invoke the :meth:`_orm.registry.configure` step thus making backref
        attributes available.

        In all cases, the :meth:`_orm.registry.configure` is always available to
        be called directly.

    .. change::
        :tags: bug, examples
        :tickets: 9220

        Reworked the :ref:`examples_versioned_history` to work with
        version 2.0, while at the same time improving the overall working of
        this example to use newer APIs, including a newly added hook
        :meth:`_orm.MapperEvents.after_mapper_constructed`.



    .. change::
        :tags: bug, mysql
        :tickets: 8626

        Added support for MySQL 8's new ``AS <name> ON DUPLICATE KEY`` syntax when
        using :meth:`_mysql.Insert.on_duplicate_key_update`, which is required for
        newer versions of MySQL 8 as the previous syntax using ``VALUES()`` now
        emits a deprecation warning with those versions. Server version detection
        is employed to determine if traditional MariaDB / MySQL < 8 ``VALUES()``
        syntax should be used, vs. the newer MySQL 8 required syntax. Pull request
        courtesy Caspar Wylie.

.. changelog::
    :version: 2.0.1
    :released: February 1, 2023

    .. change::
        :tags: bug, typing
        :tickets: 9174

        Opened up typing on :paramref:`.Select.with_for_update.of` to also accept
        table and mapped class arguments, as seems to be available for the MySQL
        dialect.

    .. change::
        :tags: bug, orm, regression
        :tickets: 9164

        Fixed regression where ORM models that used joined table inheritance with a
        composite foreign key would encounter an internal error in the mapper
        internals.



    .. change::
        :tags: bug, sql
        :tickets: 7664

        Corrected the fix for :ticket:`7664`, released in version 2.0.0, to also
        include :class:`.DropSchema` which was inadvertently missed in this fix,
        allowing stringification without a dialect. The fixes for both constructs
        is backported to the 1.4 series as of 1.4.47.


    .. change::
        :tags: bug, orm declarative
        :tickets: 9175

        Added support for :pep:`484` ``NewType`` to be used in the
        :paramref:`_orm.registry.type_annotation_map` as well as within
        :class:`.Mapped` constructs. These types will behave in the same way as
        custom subclasses of types right now; they must appear explicitly within
        the :paramref:`_orm.registry.type_annotation_map` to be mapped.

    .. change::
        :tags: bug, typing
        :tickets: 9183

        Fixed typing for limit/offset methods including :meth:`.Select.limit`,
        :meth:`.Select.offset`, :meth:`_orm.Query.limit`, :meth:`_orm.Query.offset`
        to allow ``None``, which is the documented API to "cancel" the current
        limit/offset.



    .. change::
        :tags: bug, orm declarative
        :tickets: 9179

        When using the :class:`.MappedAsDataclass` superclass, all classes within
        the hierarchy that are subclasses of this class will now be run through the
        ``@dataclasses.dataclass`` function whether or not they are actually
        mapped, so that non-ORM fields declared on non-mapped classes within the
        hierarchy will be used when mapped subclasses are turned into dataclasses.
        This behavior applies both to intermediary classes mapped with
        ``__abstract__ = True`` as well as to the user-defined declarative base
        itself, assuming :class:`.MappedAsDataclass` is present as a superclass for
        these classes.

        This allows non-mapped attributes such as ``InitVar`` declarations on
        superclasses to be used, without the need to run the
        ``@dataclasses.dataclass`` decorator explicitly on each non-mapped class.
        The new behavior is considered as correct as this is what the :pep:`681`
        implementation expects when using a superclass to indicate dataclass
        behavior.

    .. change::
        :tags: bug, typing
        :tickets: 9170

        Fixed typing issue where :func:`_orm.mapped_column` objects typed as
        :class:`_orm.Mapped` wouldn't be accepted in schema constraints such as
        :class:`_schema.ForeignKey`, :class:`_schema.UniqueConstraint` or
        :class:`_schema.Index`.

    .. change::
        :tags: bug, orm declarative
        :tickets: 9187

        Added support for :pep:`586` ``Literal[]`` to be used in the
        :paramref:`_orm.registry.type_annotation_map` as well as within
        :class:`.Mapped` constructs. To use custom types such as these, they must
        appear explicitly within the :paramref:`_orm.registry.type_annotation_map`
        to be mapped.  Pull request courtesy Frederik Aalund.

        As part of this change, the support for :class:`.sqltypes.Enum` in the
        :paramref:`_orm.registry.type_annotation_map` has been expanded to include
        support for ``Literal[]`` types consisting of string values to be used,
        in addition to ``enum.Enum`` datatypes.    If a ``Literal[]`` datatype
        is used within ``Mapped[]`` that is not linked in
        :paramref:`_orm.registry.type_annotation_map` to a specific datatype,
        a :class:`.sqltypes.Enum` will be used by default.

        .. seealso::

            :ref:`orm_declarative_mapped_column_enums`


    .. change::
        :tags: bug, orm declarative
        :tickets: 9200

        Fixed issue involving the use of :class:`.sqltypes.Enum` within the
        :paramref:`_orm.registry.type_annotation_map` where the
        :paramref:`_sqltypes.Enum.native_enum` parameter would not be correctly
        copied to the mapped column datatype, if it were overridden
        as stated in the documentation to set this parameter to False.



    .. change::
        :tags: bug, orm declarative, regression
        :tickets: 9171

        Fixed regression in :class:`.DeclarativeBase` class where the registry's
        default constructor would not be applied to the base itself, which is
        different from how the previous :func:`_orm.declarative_base` construct
        works. This would prevent a mapped class with its own ``__init__()`` method
        from calling ``super().__init__()`` in order to access the registry's
        default constructor and automatically populate attributes, instead hitting
        ``object.__init__()`` which would raise a ``TypeError`` on any arguments.




    .. change::
        :tags: bug, sql, regression
        :tickets: 9173

        Fixed regression related to the implementation for the new
        "insertmanyvalues" feature where an internal ``TypeError`` would occur in
        arrangements where a :func:`_sql.insert` would be referred towards inside
        of another :func:`_sql.insert` via a CTE; made additional repairs for this
        use case for positional dialects such as asyncpg when using
        "insertmanyvalues".



    .. change::
        :tags: bug, typing
        :tickets: 9156

        Fixed typing for :meth:`_expression.ColumnElement.cast` to accept
        both ``Type[TypeEngine[T]]`` and ``TypeEngine[T]``; previously
        only ``TypeEngine[T]`` was accepted.  Pull request courtesy Yurii Karabas.

    .. change::
        :tags: bug, orm declarative
        :tickets: 9177

        Improved the ruleset used to interpret :pep:`593` ``Annotated`` types when
        used with Annotated Declarative mapping, the inner type will be checked for
        "Optional" in all cases which will be added to the criteria by which the
        column is set as "nullable" or not; if the type within the ``Annotated``
        container is optional (or unioned with ``None``), the column will be
        considered nullable if there are no explicit
        :paramref:`_orm.mapped_column.nullable` parameters overriding it.

    .. change::
        :tags: bug, orm
        :tickets: 9182

        Improved the error reporting when linking strategy options from a base
        class to another attribute that's off a subclass, where ``of_type()``
        should be used. Previously, when :meth:`.Load.options` is used, the message
        would lack informative detail that ``of_type()`` should be used, which was
        not the case when linking the options directly. The informative detail now
        emits even if :meth:`.Load.options` is used.



.. changelog::
    :version: 2.0.0
    :released: January 26, 2023

    .. change::
        :tags: bug, sql
        :tickets: 7664

        Fixed stringify for a the :class:`.CreateSchema` DDL construct, which
        would fail with an ``AttributeError`` when stringified without a
        dialect. Update: Note this fix failed to accommodate for
        :class:`.DropSchema`; a followup fix in version 2.0.1 repairs this
        case. The fix for both elements is backported to 1.4.47.

    .. change::
        :tags: usecase, orm extensions
        :tickets: 5145

        Added new feature to :class:`.AutomapBase` for autoload of classes across
        multiple schemas which may have overlapping names, by providing a
        :paramref:`.AutomapBase.prepare.modulename_for_table` parameter which
        allows customization of the ``__module__`` attribute of newly generated
        classes, as well as a new collection :attr:`.AutomapBase.by_module`, which
        stores a dot-separated namespace of module names linked to classes based on
        the ``__module__`` attribute.

        Additionally, the :meth:`.AutomapBase.prepare` method may now be invoked
        any number of times, with or without reflection enabled; only newly
        added tables that were not previously mapped will be processed on each
        call.   Previously, the :meth:`.MetaData.reflect` method would need to be
        called explicitly each time.

        .. seealso::

            :ref:`automap_by_module` - illustrates use of both techniques at once.

    .. change::
        :tags: orm, bug
        :tickets: 7305

        Improved the notification of warnings that are emitted within the configure
        mappers or flush process, which are often invoked as part of a different
        operation, to add additional context to the message that indicates one of
        these operations as the source of the warning within operations that may
        not be obviously related.

    .. change::
        :tags: bug, typing
        :tickets: 9129

        Added typing for the built-in generic functions that are available from the
        :data:`_sql.func` namespace, which accept a particular set of arguments and
        return a particular type, such as for :class:`_sql.count`,
        :class:`_sql.current_timestamp`, etc.

    .. change::
        :tags: bug, typing
        :tickets: 9120

        Corrected the type passed for "lambda statements" so that a plain lambda is
        accepted by mypy, pyright, others without any errors about argument types.
        Additionally implemented typing for more of the public API for lambda
        statements and ensured :class:`.StatementLambdaElement` is part of the
        :class:`.Executable` hierarchy so it's typed as accepted by
        :meth:`_engine.Connection.execute`.

    .. change::
        :tags: typing, bug
        :tickets: 9122

        The :meth:`_sql.ColumnOperators.in_` and
        :meth:`_sql.ColumnOperators.not_in` methods are typed to include
        ``Iterable[Any]`` rather than ``Sequence[Any]`` for more flexibility in
        argument type.


    .. change::
        :tags: typing, bug
        :tickets: 9123

        The :func:`_sql.or_` and :func:`_sql.and_` from a typing perspective
        require the first argument to be present, however these functions still
        accept zero arguments which will emit a deprecation warning at runtime.
        Typing is also added to support sending the fixed literal ``False`` for
        :func:`_sql.or_` and ``True`` for :func:`_sql.and_` as the first argument
        only, however the documentation now indicates sending the
        :func:`_sql.false` and :func:`_sql.true` constructs in these cases as a
        more explicit approach.


    .. change::
        :tags: typing, bug
        :tickets: 9125

        Fixed typing issue where iterating over a :class:`_orm.Query` object
        was not correctly typed.

    .. change::
        :tags: typing, bug
        :tickets: 9136

        Fixed typing issue where the object type when using :class:`_engine.Result`
        as a context manager were not preserved, indicating :class:`_engine.Result`
        in all cases rather than the specific :class:`_engine.Result` sub-type.
        Pull request courtesy Martin Baláž.

    .. change::
        :tags: typing, bug
        :tickets: 9150

        Fixed issue where using the :paramref:`_orm.relationship.remote_side`
        and similar parameters, passing an annotated declarative object typed as
        :class:`_orm.Mapped`, would not be accepted by the type checker.

    .. change::
        :tags: typing, bug
        :tickets: 9148

        Added typing to legacy operators such as ``isnot()``, ``notin_()``, etc.
        which previously were referencing the newer operators but were not
        themselves typed.

    .. change::
        :tags: feature, orm extensions
        :tickets: 7226

        Added new option to horizontal sharding API
        :class:`_horizontal.set_shard_id` which sets the effective shard identifier
        to query against, for both the primary query as well as for all secondary
        loaders including relationship eager loaders as well as relationship and
        column lazy loaders.

    .. change::
        :tags: bug, mssql, regression
        :tickets: 9142

        The newly added comment reflection and rendering capability of the MSSQL
        dialect, added in :ticket:`7844`, will now be disabled by default if it
        cannot be determined that an unsupported backend such as Azure Synapse may
        be in use; this backend does not support table and column comments and does
        not support the SQL Server routines in use to generate them as well as to
        reflect them. A new parameter ``supports_comments`` is added to the dialect
        which defaults to ``None``, indicating that comment support should be
        auto-detected. When set to ``True`` or ``False``, the comment support is
        either enabled or disabled unconditionally.

        .. seealso::

            :ref:`mssql_comment_support`


.. changelog::
    :version: 2.0.0rc3
    :released: January 26, 2023
    :released: January 18, 2023

    .. change::
        :tags: bug, typing
        :tickets: 9096

        Fixes to the annotations within the ``sqlalchemy.ext.hybrid`` extension for
        more effective typing of user-defined methods. The typing now uses
        :pep:`612` features, now supported by recent versions of Mypy, to maintain
        argument signatures for :class:`.hybrid_method`. Return values for hybrid
        methods are accepted as SQL expressions in contexts such as
        :meth:`_sql.Select.where` while still supporting SQL methods.

    .. change::
        :tags: bug, orm
        :tickets: 9099

        Fixed issue where using a pep-593 ``Annotated`` type in the
        :paramref:`_orm.registry.type_annotation_map` which itself contained a
        generic plain container or ``collections.abc`` type (e.g. ``list``,
        ``dict``, ``collections.abc.Sequence``, etc. ) as the target type would
        produce an internal error when the ORM were trying to interpret the
        ``Annotated`` instance.



    .. change::
        :tags: bug, orm
        :tickets: 9100

        Added an error message when a :func:`_orm.relationship` is mapped against
        an abstract container type, such as ``Mapped[Sequence[B]]``, without
        providing the :paramref:`_orm.relationship.container_class` parameter which
        is necessary when the type is abstract. Previously the the abstract
        container would attempt to be instantiated at a later step and fail.



    .. change::
        :tags: orm, feature
        :tickets: 9060

        Added a new parameter to :class:`_orm.Mapper` called
        :paramref:`_orm.Mapper.polymorphic_abstract`. The purpose of this directive
        is so that the ORM will not consider the class to be instantiated or loaded
        directly, only subclasses. The actual effect is that the
        :class:`_orm.Mapper` will prevent direct instantiation of instances
        of the class and will expect that the class does not have a distinct
        polymorphic identity configured.

        In practice, the class that is mapped with
        :paramref:`_orm.Mapper.polymorphic_abstract` can be used as the target of a
        :func:`_orm.relationship` as well as be used in queries; subclasses must of
        course include polymorphic identities in their mappings.

        The new parameter is automatically applied to classes that subclass
        the :class:`.AbstractConcreteBase` class, as this class is not intended
        to be instantiated.

        .. seealso::

            :ref:`orm_inheritance_abstract_poly`


    .. change::
        :tags: bug, postgresql
        :tickets: 9106

        Fixed regression where psycopg3 changed an API call as of version 3.1.8 to
        expect a specific object type that was previously not enforced, breaking
        connectivity for the psycopg3 dialect.

    .. change::
        :tags: oracle, usecase
        :tickets: 9086

        Added support for the Oracle SQL type ``TIMESTAMP WITH LOCAL TIME ZONE``,
        using a newly added Oracle-specific :class:`_oracle.TIMESTAMP` datatype.

.. changelog::
    :version: 2.0.0rc2
    :released: January 26, 2023
    :released: January 9, 2023

    .. change::
        :tags: bug, typing
        :tickets: 9067

        The Data Class Transforms argument ``field_descriptors`` was renamed
        to ``field_specifiers`` in the accepted version of PEP 681.

    .. change::
        :tags: bug, oracle
        :tickets: 9059

        Supported use case for foreign key constraints where the local column is
        marked as "invisible". The errors normally generated when a
        :class:`.ForeignKeyConstraint` is created that check for the target column
        are disabled when reflecting, and the constraint is skipped with a warning
        in the same way which already occurs for an :class:`.Index` with a similar
        issue.

    .. change::
        :tags: bug, orm
        :tickets: 9071

        Fixed issue where an overly restrictive ORM mapping rule were added in 2.0
        which prevented mappings against :class:`.TableClause` objects, such as
        those used in the view recipe on the wiki.

    .. change::
        :tags: bug, mysql
        :tickets: 9058

        Restored the behavior of :meth:`.Inspector.has_table` to report on
        temporary tables for MySQL / MariaDB. This is currently the behavior for
        all other included dialects, but was removed for MySQL in 1.4 due to no
        longer using the DESCRIBE command; there was no documented support for temp
        tables being reported by the :meth:`.Inspector.has_table` method in this
        version or on any previous version, so the previous behavior was undefined.

        As SQLAlchemy 2.0 has added formal support for temp table status via
        :meth:`.Inspector.has_table`, the MySQL /MariaDB dialect has been reverted
        to use the "DESCRIBE" statement as it did in the SQLAlchemy 1.3 series and
        previously, and test support is added to include MySQL / MariaDB for
        this behavior.   The previous issues with ROLLBACK being emitted which
        1.4 sought to improve upon don't apply in SQLAlchemy 2.0 due to
        simplifications in how :class:`.Connection` handles transactions.

        DESCRIBE is necessary as MariaDB in particular has no consistently
        available public information schema of any kind in order to report on temp
        tables other than DESCRIBE/SHOW COLUMNS, which rely on throwing an error
        in order to report no results.

    .. change::
        :tags: json, postgresql
        :tickets: 7147

        Implemented missing ``JSONB`` operations:

        * ``@@`` using :meth:`_postgresql.JSONB.Comparator.path_match`
        * ``@?`` using :meth:`_postgresql.JSONB.Comparator.path_exists`
        * ``#-`` using :meth:`_postgresql.JSONB.Comparator.delete_path`

        Pull request curtesy of Guilherme Martins Crocetti.

.. changelog::
    :version: 2.0.0rc1
    :released: January 26, 2023
    :released: December 28, 2022

    .. change::
        :tags: bug, typing
        :tickets: 6810, 9025

        pep-484 typing has been completed for the
        ``sqlalchemy.ext.horizontal_shard`` extension as well as the
        ``sqlalchemy.orm.events`` module. Thanks to Gleb Kisenkov for their
        efforts.


    .. change::
        :tags: postgresql, bug
        :tickets: 8977
        :versions: 2.0.0rc1

        Added support for explicit use of PG full text functions with asyncpg and
        psycopg (SQLAlchemy 2.0 only), with regards to the ``REGCONFIG`` type cast
        for the first argument, which previously would be incorrectly cast to a
        VARCHAR, causing failures on these dialects that rely upon explicit type
        casts. This includes support for :class:`_postgresql.to_tsvector`,
        :class:`_postgresql.to_tsquery`, :class:`_postgresql.plainto_tsquery`,
        :class:`_postgresql.phraseto_tsquery`,
        :class:`_postgresql.websearch_to_tsquery`,
        :class:`_postgresql.ts_headline`, each of which will determine based on
        number of arguments passed if the first string argument should be
        interpreted as a PostgreSQL "REGCONFIG" value; if so, the argument is typed
        using a newly added type object :class:`_postgresql.REGCONFIG` which is
        then explicitly cast in the SQL expression.


    .. change::
        :tags: bug, orm
        :tickets: 4629

        A warning is emitted if a backref name used in :func:`_orm.relationship`
        names an attribute on the target class which already has a method or
        attribute assigned to that name, as the backref declaration will replace
        that attribute.

    .. change::
        :tags: bug, postgresql
        :tickets: 9020

        Fixed regression where newly revised PostgreSQL range types such as
        :class:`_postgresql.INT4RANGE` could not be set up as the impl of a
        :class:`.TypeDecorator` custom type, instead raising a ``TypeError``.

    .. change::
        :tags: usecase, orm
        :tickets: 7837

        Adjustments to the :class:`_orm.Session` in terms of extensibility,
        as well as updates to the :class:`.ShardedSession` extension:

        * :meth:`_orm.Session.get` now accepts
          :paramref:`_orm.Session.get.bind_arguments`, which in particular may be
          useful when using the horizontal sharding extension.

        * :meth:`_orm.Session.get_bind` accepts arbitrary kw arguments, which
          assists in developing code that uses a :class:`_orm.Session` class which
          overrides this method with additional arguments.

        * Added a new ORM execution option ``identity_token`` which may be used
          to directly affect the "identity token" that will be associated with
          newly loaded ORM objects.  This token is how sharding approaches
          (namely the :class:`.ShardedSession`, but can be used in other cases
          as well) separate object identities across different "shards".

          .. seealso::

              :ref:`queryguide_identity_token`

        * The :meth:`_orm.SessionEvents.do_orm_execute` event hook may now be used
          to affect all ORM-related options, including ``autoflush``,
          ``populate_existing``, and ``yield_per``; these options are re-consumed
          subsequent to event hooks being invoked before they are acted upon.
          Previously, options like ``autoflush`` would have been already evaluated
          at this point. The new ``identity_token`` option is also supported in
          this mode and is now used by the horizontal sharding extension.


        * The :class:`.ShardedSession` class replaces the
          :paramref:`.ShardedSession.id_chooser` hook with a new hook
          :paramref:`.ShardedSession.identity_chooser`, which no longer relies upon
          the legacy :class:`_orm.Query` object.
          :paramref:`.ShardedSession.id_chooser` is still accepted in place of
          :paramref:`.ShardedSession.identity_chooser` with a deprecation warning.

    .. change::
        :tags: usecase, orm
        :tickets: 9015

        The behavior of "joining an external transaction into a Session" has been
        revised and improved, allowing explicit control over how the
        :class:`_orm.Session` will accommodate an incoming
        :class:`_engine.Connection` that already has a transaction and possibly a
        savepoint already established. The new parameter
        :paramref:`_orm.Session.join_transaction_mode` includes a series of option
        values which can accommodate the existing transaction in several ways, most
        importantly allowing a :class:`_orm.Session` to operate in a fully
        transactional style using savepoints exclusively, while leaving the
        externally initiated transaction non-committed and active under all
        circumstances, allowing test suites to rollback all changes that take place
        within tests.

        Additionally, revised the :meth:`_orm.Session.close` method to fully close
        out savepoints that may still be present, which also allows the
        "external transaction" recipe to proceed without warnings if the
        :class:`_orm.Session` did not explicitly end its own SAVEPOINT
        transactions.

        .. seealso::

            :ref:`change_9015`


    .. change::
        :tags: bug, sql
        :tickets: 8988

        Added test support to ensure that all compiler ``visit_xyz()`` methods
        across all :class:`.Compiler` implementations in SQLAlchemy accept a
        ``**kw`` parameter, so that all compilers accept additional keyword
        arguments under all circumstances.

    .. change::
        :tags: bug, postgresql
        :tickets: 8984

        The :meth:`_postgresql.Range.__eq___` will now return ``NotImplemented``
        when comparing with an instance of a different class, instead of raising
        an :exc:`AttributeError` exception.

    .. change::
        :tags: bug, sql
        :tickets: 6114

        The :meth:`.SQLCompiler.construct_params` method, as well as the
        :attr:`.SQLCompiler.params` accessor, will now return the
        exact parameters that correspond to a compiled statement that used
        the ``render_postcompile`` parameter to compile.   Previously,
        the method returned a parameter structure that by itself didn't correspond
        to either the original parameters or the expanded ones.

        Passing a new dictionary of parameters to
        :meth:`.SQLCompiler.construct_params` for a :class:`.SQLCompiler` that was
        constructed with ``render_postcompile`` is now disallowed; instead, to make
        a new SQL string and parameter set for an alternate set of parameters, a
        new method :meth:`.SQLCompiler.construct_expanded_state` is added which
        will produce a new expanded form for the given parameter set, using the
        :class:`.ExpandedState` container which includes a new SQL statement
        and new parameter dictionary, as well as a positional parameter tuple.


    .. change::
        :tags: bug, orm
        :tickets: 8703, 8997, 8996

        A series of changes and improvements regarding
        :meth:`_orm.Session.refresh`. The overall change is that primary key
        attributes for an object are now included in a refresh operation
        unconditionally when relationship-bound attributes are to be refreshed,
        even if not expired and even if not specified in the refresh.

        * Improved :meth:`_orm.Session.refresh` so that if autoflush is enabled
          (as is the default for :class:`_orm.Session`), the autoflush takes place
          at an earlier part of the refresh process so that pending primary key
          changes are applied without errors being raised.  Previously, this
          autoflush took place too late in the process and the SELECT statement
          would not use the correct key to locate the row and an
          :class:`.InvalidRequestError` would be raised.

        * When the above condition is present, that is, unflushed primary key
          changes are present on the object, but autoflush is not enabled,
          the refresh() method now explicitly disallows the operation to proceed,
          and an informative :class:`.InvalidRequestError` is raised asking that
          the pending primary key changes be flushed first.  Previously,
          this use case was simply broken and :class:`.InvalidRequestError`
          would be raised anyway. This restriction is so that it's safe for the
          primary key attributes to be refreshed, as is necessary for the case of
          being able to refresh the object with relationship-bound secondary
          eagerloaders also being emitted. This rule applies in all cases to keep
          API behavior consistent regardless of whether or not the PK cols are
          actually needed in the refresh, as it is unusual to be refreshing
          some attributes on an object while keeping other attributes "pending"
          in any case.

        * The :meth:`_orm.Session.refresh` method has been enhanced such that
          attributes which are :func:`_orm.relationship`-bound and linked to an
          eager loader, either at mapping time or via last-used loader options,
          will be refreshed in all cases even when a list of attributes is passed
          that does not include any columns on the parent row. This builds upon the
          feature first implemented for non-column attributes as part of
          :ticket:`1763` fixed in 1.4 allowing eagerly-loaded relationship-bound
          attributes to participate in the :meth:`_orm.Session.refresh` operation.
          If the refresh operation does not indicate any columns on the parent row
          to be refreshed, the primary key columns will nonetheless be included
          in the refresh operation, which allows the load to proceed into the
          secondary relationship loaders indicated as it does normally.
          Previously an :class:`.InvalidRequestError` error would be raised
          for this condition (:ticket:`8703`)

        * Fixed issue where an unnecessary additional SELECT would be emitted in
          the case where :meth:`_orm.Session.refresh` were called with a
          combination of expired attributes, as well as an eager loader such as
          :func:`_orm.selectinload` that emits a "secondary" query, if the primary
          key attributes were also in an expired state.  As the primary key
          attributes are now included in the refresh automatically, there is no
          additional load for these attributes when a relationship loader
          goes to select for them (:ticket:`8997`)

        * Fixed regression caused by :ticket:`8126` released in 2.0.0b1 where the
          :meth:`_orm.Session.refresh` method would fail with an
          ``AttributeError``, if passed both an expired column name as well as the
          name of a relationship-bound attribute that was linked to a "secondary"
          eagerloader such as the :func:`_orm.selectinload` eager loader
          (:ticket:`8996`)

    .. change::
        :tags: bug, sql
        :tickets: 8994

        To accommodate for third party dialects with different character escaping
        needs regarding bound parameters, the system by which SQLAlchemy "escapes"
        (i.e., replaces with another character in its place) special characters in
        bound parameter names has been made extensible for third party dialects,
        using the :attr:`.SQLCompiler.bindname_escape_chars` dictionary which can
        be overridden at the class declaration level on any :class:`.SQLCompiler`
        subclass. As part of this change, also added the dot ``"."`` as a default
        "escaped" character.


    .. change::
        :tags: orm, feature
        :tickets: 8889

        Added a new default value for the :paramref:`.Mapper.eager_defaults`
        parameter "auto", which will automatically fetch table default values
        during a unit of work flush, if the dialect supports RETURNING for the
        INSERT being run, as well as
        :ref:`insertmanyvalues <engine_insertmanyvalues>` available. Eager fetches
        for server-side UPDATE defaults, which are very uncommon, continue to only
        take place if :paramref:`.Mapper.eager_defaults` is set to ``True``, as
        there is no batch-RETURNING form for UPDATE statements.


    .. change::
        :tags: usecase, orm
        :tickets: 8973

        Removed the requirement that the ``__allow_unmapped__`` attribute be used
        on Declarative Dataclass Mapped class when non-``Mapped[]`` annotations are
        detected; previously, an error message that was intended to support legacy
        ORM typed mappings would be raised, which additionally did not mention
        correct patterns to use with Dataclasses specifically. This error message
        is now no longer raised if :meth:`_orm.registry.mapped_as_dataclass` or
        :class:`_orm.MappedAsDataclass` is used.

        .. seealso::

            :ref:`orm_declarative_native_dataclasses_non_mapped_fields`


    .. change::
        :tags: bug, orm
        :tickets: 8168

        Improved a fix first made in version 1.4 for :ticket:`8456` which scaled
        back the usage of internal "polymorphic adapters", that are used to render
        ORM queries when the :paramref:`_orm.Mapper.with_polymorphic` parameter is
        used. These adapters, which are very complex and error prone, are now used
        only in those cases where an explicit user-supplied subquery is used for
        :paramref:`_orm.Mapper.with_polymorphic`, which includes only the use case
        of concrete inheritance mappings that use the
        :func:`_orm.polymorphic_union` helper, as well as the legacy use case of
        using an aliased subquery for joined inheritance mappings, which is not
        needed in modern use.

        For the most common case of joined inheritance mappings that use the
        built-in polymorphic loading scheme, which includes those which make use of
        the :paramref:`_orm.Mapper.polymorphic_load` parameter set to ``inline``,
        polymorphic adapters are now no longer used. This has both a positive
        performance impact on the construction of queries as well as a
        substantial simplification of the internal query rendering process.

        The specific issue targeted was to allow a :func:`_orm.column_property`
        to refer to joined-inheritance classes within a scalar subquery, which now
        works as intuitively as is feasible.



.. changelog::
    :version: 2.0.0b4
    :released: January 26, 2023
    :released: December 5, 2022

    .. change::
        :tags: usecase, orm
        :tickets: 8859

        Added support custom user-defined types which extend the Python
        ``enum.Enum`` base class to be resolved automatically
        to SQLAlchemy :class:`.Enum` SQL types, when using the Annotated
        Declarative Table feature.  The feature is made possible through new
        lookup features added to the ORM type map feature, and includes support
        for changing the arguments of the :class:`.Enum` that's generated by
        default as well as setting up specific ``enum.Enum`` types within
        the map with specific arguments.

        .. seealso::

            :ref:`orm_declarative_mapped_column_enums`

    .. change::
        :tags: bug, typing
        :tickets: 8783

        Adjusted internal use of the Python ``enum.IntFlag`` class which changed
        its behavioral contract in Python 3.11. This was not causing runtime
        failures however caused typing runs to fail under Python 3.11.

    .. change::
        :tags: usecase, typing
        :tickets: 8847

        Added a new type :class:`.SQLColumnExpression` which may be indicated in
        user code to represent any SQL column oriented expression, including both
        those based on :class:`.ColumnElement` as well as on ORM
        :class:`.QueryableAttribute`. This type is a real class, not an alias, so
        can also be used as the foundation for other objects.  An additional
        ORM-specific subclass :class:`.SQLORMExpression` is also included.


    .. change::
        :tags: bug, typing
        :tickets: 8667, 6810

        The ``sqlalchemy.ext.mutable`` extension and ``sqlalchemy.ext.automap``
        extensions are now fully pep-484 typed. Huge thanks to Gleb Kisenkov for
        their efforts on this.



    .. change::
        :tags: bug, sql
        :tickets: 8849

        The approach to the ``numeric`` pep-249 paramstyle has been rewritten, and
        is now fully supported, including by features such as "expanding IN" and
        "insertmanyvalues". Parameter names may also be repeated in the source SQL
        construct which will be correctly represented within the numeric format
        using a single parameter. Introduced an additional numeric paramstyle
        called ``numeric_dollar``, which is specifically what's used by the asyncpg
        dialect; the paramstyle is equivalent to ``numeric`` except numeric
        indicators are indicated by a dollar-sign rather than a colon. The asyncpg
        dialect now uses ``numeric_dollar`` paramstyle directly, rather than
        compiling to ``format`` style first.

        The ``numeric`` and ``numeric_dollar`` paramstyles assume that the target
        backend is capable of receiving the numeric parameters in any order,
        and will match the given parameter values to the statement based on
        matching their position (1-based) to the numeric indicator.  This is the
        normal behavior of "numeric" paramstyles, although it was observed that
        the SQLite DBAPI implements a not-used "numeric" style that does not honor
        parameter ordering.

    .. change::
        :tags: usecase, postgresql
        :tickets: 8765

        Complementing :ticket:`8690`, new comparison methods such as
        :meth:`_postgresql.Range.adjacent_to`,
        :meth:`_postgresql.Range.difference`, :meth:`_postgresql.Range.union`,
        etc., were added to the PG-specific range objects, bringing them in par
        with the standard operators implemented by the underlying
        :attr:`_postgresql.AbstractRange.comparator_factory`.

        In addition, the ``__bool__()`` method of the class has been corrected to
        be consistent with the common Python containers behavior as well as how
        other popular PostgreSQL drivers do: it now tells whether the range
        instance is *not* empty, rather than the other way around.

        Pull request courtesy Lele Gaifax.

    .. change::
        :tags: bug, sql
        :tickets: 8770

        Adjusted the rendering of ``RETURNING``, in particular when using
        :class:`_sql.Insert`, such that it now renders columns using the same logic
        as that of the :class:`.Select` construct to generate labels, which will
        include disambiguating labels, as well as that a SQL function surrounding a
        named column will be labeled using the column name itself. This establishes
        better cross-compatibility when selecting rows from either :class:`.Select`
        constructs or from DML statements that use :meth:`.UpdateBase.returning`. A
        narrower scale change was also made for the 1.4 series that adjusted the
        function label issue only.

    .. change::
        :tags: change, postgresql, asyncpg
        :tickets: 8926

        Changed the paramstyle used by asyncpg from ``format`` to
        ``numeric_dollar``. This has two main benefits since it does not require
        additional processing of the statement and allows for duplicate parameters
        to be present in the statements.

    .. change::
        :tags: bug, orm
        :tickets: 8888

        Fixed issue where use of an unknown datatype within a :class:`.Mapped`
        annotation for a column-based attribute would silently fail to map the
        attribute, rather than reporting an exception; an informative exception
        message is now raised.

    .. change::
        :tags: bug, orm
        :tickets: 8777

        Fixed a suite of issues involving :class:`.Mapped` use with dictionary
        types, such as ``Mapped[Dict[str, str] | None]``, would not be correctly
        interpreted in Declarative ORM mappings. Support to correctly
        "de-optionalize" this type including for lookup in ``type_annotation_map``
        has been fixed.

    .. change::
        :tags: feature, orm
        :tickets: 8822

        Added a new parameter :paramref:`_orm.mapped_column.use_existing_column` to
        accommodate the use case of a single-table inheritance mapping that uses
        the pattern of more than one subclass indicating the same column to take
        place on the superclass. This pattern was previously possible by using
        :func:`_orm.declared_attr` in conjunction with locating the existing column
        in the ``.__table__`` of the superclass, however is now updated to work
        with :func:`_orm.mapped_column` as well as with pep-484 typing, in a
        simple and succinct way.

        .. seealso::

           :ref:`orm_inheritance_column_conflicts`




    .. change::
        :tags: bug, mssql
        :tickets: 8917

        Fixed regression caused by the combination of :ticket:`8177`, re-enable
        setinputsizes for SQL server unless fast_executemany + DBAPI executemany is
        used for a statement, along with :ticket:`6047`, implement
        "insertmanyvalues", which bypasses DBAPI executemany in place of a custom
        DBAPI execute for INSERT statements. setinputsizes would incorrectly not be
        used for a multiple parameter-set INSERT statement that used
        "insertmanyvalues" if fast_executemany were turned on, as the check would
        incorrectly assume this is a DBAPI executemany call.  The "regression"
        would then be that the "insertmanyvalues" statement format is apparently
        slightly more sensitive to multiple rows that don't use the same types
        for each row, so in such a case setinputsizes is especially needed.

        The fix repairs the fast_executemany check so that it only disables
        setinputsizes if true DBAPI executemany is to be used.

    .. change::
        :tags: bug, orm, performance
        :tickets: 8796

        Additional performance enhancements within ORM-enabled SQL statements,
        specifically targeting callcounts within the construction of ORM
        statements, using combinations of :func:`_orm.aliased` with
        :func:`_sql.union` and similar "compound" constructs, in addition to direct
        performance improvements to the ``corresponding_column()`` internal method
        that is used heavily by the ORM by constructs like :func:`_orm.aliased` and
        similar.


    .. change::
        :tags: bug, postgresql
        :tickets: 8884

        Added additional type-detection for the new PostgreSQL
        :class:`_postgresql.Range` type, where previous cases that allowed the
        psycopg2-native range objects to be received directly by the DBAPI without
        SQLAlchemy intercepting them stopped working, as we now have our own value
        object. The :class:`_postgresql.Range` object has been enhanced such that
        SQLAlchemy Core detects it in otherwise ambiguous situations (such as
        comparison to dates) and applies appropriate bind handlers. Pull request
        courtesy Lele Gaifax.

    .. change::
        :tags: bug, orm
        :tickets: 8880

        Fixed bug in :ref:`orm_declarative_native_dataclasses` feature where using
        plain dataclass fields with the ``__allow_unmapped__`` directive in a
        mapping would not create a dataclass with the correct class-level state for
        those fields, copying the raw ``Field`` object to the class inappropriately
        after dataclasses itself had replaced the ``Field`` object with the
        class-level default value.

    .. change::
        :tags: usecase, orm extensions
        :tickets: 8878

        Added support for the :func:`.association_proxy` extension function to
        take part within Python ``dataclasses`` configuration, when using
        the native dataclasses feature described at
        :ref:`orm_declarative_native_dataclasses`.  Included are attribute-level
        arguments including :paramref:`.association_proxy.init` and
        :paramref:`.association_proxy.default_factory`.

        Documentation for association proxy has also been updated to use
        "Annotated Declarative Table" forms within examples, including type
        annotations used for :class:`.AssocationProxy` itself.


    .. change::
        :tags: bug, typing

        Corrected typing support for the :paramref:`_orm.relationship.secondary`
        argument which may also accept a callable (lambda) that returns a
        :class:`.FromClause`.

    .. change::
        :tags: bug, orm, regression
        :tickets: 8812

        Fixed regression where flushing a mapped class that's mapped against a
        subquery, such as a direct mapping or some forms of concrete table
        inheritance, would fail if the :paramref:`_orm.Mapper.eager_defaults`
        parameter were used.

    .. change::
        :tags: bug, schema
        :tickets: 8925

        Stricter rules are in place for appending of :class:`.Column` objects to
        :class:`.Table` objects, both moving some previous deprecation warnings to
        exceptions, and preventing some previous scenarios that would cause
        duplicate columns to appear in tables, when
        :paramref:`.Table.extend_existing` were set to ``True``, for both
        programmatic :class:`.Table` construction as well as during reflection
        operations.

        See :ref:`change_8925` for a rundown of these changes.

        .. seealso::

            :ref:`change_8925`

    .. change::
        :tags: usecase, orm
        :tickets: 8905

        Added :paramref:`_orm.mapped_column.compare` parameter to relevant ORM
        attribute constructs including :func:`_orm.mapped_column`,
        :func:`_orm.relationship` etc. to provide for the Python dataclasses
        ``compare`` parameter on ``field()``, when using the
        :ref:`orm_declarative_native_dataclasses` feature. Pull request courtesy
        Simon Schiele.

    .. change::
        :tags: sql, usecase
        :tickets: 6289

        Added :class:`_expression.ScalarValues` that can be used as a column
        element allowing using :class:`_expression.Values` inside ``IN`` clauses
        or in conjunction with ``ANY`` or ``ALL`` collection aggregates.
        This new class is generated using the method
        :meth:`_expression.Values.scalar_values`.
        The :class:`_expression.Values` instance is now coerced to a
        :class:`_expression.ScalarValues` when used in a ``IN`` or ``NOT IN``
        operation.

    .. change::
        :tags: bug, orm
        :tickets: 8853

        Fixed regression in 2.0.0b3 caused by :ticket:`8759` where indicating the
        :class:`.Mapped` name using a qualified name such as
        ``sqlalchemy.orm.Mapped`` would fail to be recognized by Declarative as
        indicating the :class:`.Mapped` construct.

    .. change::
        :tags: bug, typing
        :tickets: 8842

        Improved the typing for :class:`.sessionmaker` and
        :class:`.async_sessionmaker`, so that the default type of their return value
        will be :class:`.Session` or :class:`.AsyncSession`, without the need to
        type this explicitly. Previously, Mypy would not automaticaly infer these
        return types from its generic base.

        As part of this change, arguments for :class:`.Session`,
        :class:`.AsyncSession`, :class:`.sessionmaker` and
        :class:`.async_sessionmaker` beyond the initial "bind" argument have been
        made keyword-only, which includes parameters that have always been
        documented as keyword arguments, such as :paramref:`.Session.autoflush`,
        :paramref:`.Session.class_`, etc.

        Pull request courtesy Sam Bull.


    .. change::
        :tags: bug, typing
        :tickets: 8776

        Fixed issue where passing a callbale function returning an iterable
        of column elements to :paramref:`_orm.relationship.order_by` was
        flagged as an error in type checkers.

.. changelog::
    :version: 2.0.0b3
    :released: January 26, 2023
    :released: November 4, 2022

    .. change::
        :tags: bug, orm, declarative
        :tickets: 8759

        Added support in ORM declarative annotations for class names specified for
        :func:`_orm.relationship`, as well as the name of the :class:`_orm.Mapped`
        symbol itself, to be different names than their direct class name, to
        support scenarios such as where :class:`_orm.Mapped` is imported as
        ``from sqlalchemy.orm import Mapped as M``, or where related class names
        are imported with an alternate name in a similar fashion. Additionally, a
        target class name given as the lead argument for :func:`_orm.relationship`
        will always supersede the name given in the left hand annotation, so that
        otherwise un-importable names that also don't match the class name can
        still be used in annotations.

    .. change::
        :tags: bug, orm, declarative
        :tickets: 8692

        Improved support for legacy 1.4 mappings that use annotations which don't
        include ``Mapped[]``, by ensuring the ``__allow_unmapped__`` attribute can
        be used to allow such legacy annotations to pass through Annotated
        Declarative without raising an error and without being interpreted in an
        ORM runtime context. Additionally improved the error message generated when
        this condition is detected, and added more documentation for how this
        situation should be handled. Unfortunately the 1.4 WARN_SQLALCHEMY_20
        migration warning cannot detect this particular configurational issue at
        runtime with its current architecture.

    .. change::
        :tags: usecase, postgresql
        :tickets: 8690

        Refined the new approach to range objects described at :ref:`change_7156`
        to accommodate driver-specific range and multirange objects, to better
        accommodate both legacy code as well as when passing results from raw SQL
        result sets back into new range or multirange expressions.

    .. change::
        :tags: usecase, engine
        :tickets: 8717

        Added new parameter :paramref:`.PoolEvents.reset.reset_state` parameter to
        the :meth:`.PoolEvents.reset` event, with deprecation logic in place that
        will continue to accept event hooks using the previous set of arguments.
        This indicates various state information about how the reset is taking
        place and is used to allow custom reset schemes to take place with full
        context given.

        Within this change a fix that's also backported to 1.4 is included which
        re-enables the :meth:`.PoolEvents.reset` event to continue to take place
        under all circumstances, including when :class:`.Connection` has already
        "reset" the connection.

        The two changes together allow custom reset schemes to be implemented using
        the :meth:`.PoolEvents.reset` event, instead of the
        :meth:`.PoolEvents.checkin` event (which continues to function as it always
        has).

    .. change::
        :tags: bug, orm, declarative
        :tickets: 8705

        Changed a fundamental configuration behavior of :class:`.Mapper`, where
        :class:`_schema.Column` objects that are explicitly present in the
        :paramref:`_orm.Mapper.properties` dictionary, either directly or enclosed
        within a mapper property object, will now be mapped within the order of how
        they appear within the mapped :class:`.Table` (or other selectable) itself
        (assuming they are in fact part of that table's list of columns), thereby
        maintaining the same order of columns in the mapped selectable as is
        instrumented on the mapped class, as well as what renders in an ORM SELECT
        statement for that mapper. Previously (where "previously" means since
        version 0.0.1), :class:`.Column` objects in the
        :paramref:`_orm.Mapper.properties` dictionary would always be mapped first,
        ahead of when the other columns in the mapped :class:`.Table` would be
        mapped, causing a discrepancy in the order in which the mapper would
        assign attributes to the mapped class as well as the order in which they
        would render in statements.

        The change most prominently takes place in the way that Declarative
        assigns declared columns to the :class:`.Mapper`, specifically how
        :class:`.Column` (or :func:`_orm.mapped_column`) objects are handled
        when they have a DDL name that is explicitly different from the mapped
        attribute name, as well as when constructs such as :func:`_orm.deferred`
        etc. are used.   The new behavior will see the column ordering within
        the mapped :class:`.Table` being the same order in which the attributes
        are mapped onto the class, assigned within the :class:`.Mapper` itself,
        and rendered in ORM statements such as SELECT statements, independent
        of how the :class:`_schema.Column` was configured against the
        :class:`.Mapper`.

    .. change::
        :tags: feature, engine
        :tickets: 8710

        To better support the use case of iterating :class:`.Result` and
        :class:`.AsyncResult` objects where user-defined exceptions may interrupt
        the iteration, both objects as well as variants such as
        :class:`.ScalarResult`, :class:`.MappingResult`,
        :class:`.AsyncScalarResult`, :class:`.AsyncMappingResult` now support
        context manager usage, where the result will be closed at the end of
        the context manager block.

        In addition, ensured that all the above
        mentioned :class:`.Result` objects include a :meth:`.Result.close` method
        as well as :attr:`.Result.closed` accessors, including
        :class:`.ScalarResult` and :class:`.MappingResult` which previously did
        not have a ``.close()`` method.

        .. seealso::

            :ref:`change_8710`


    .. change::
        :tags: bug, typing

        Corrected various typing issues within the engine and async engine
        packages.

    .. change::
        :tags: bug, orm, declarative
        :tickets: 8718

        Fixed issue in new dataclass mapping feature where a column declared on the
        decalrative base / abstract base / mixin would leak into the constructor
        for an inheriting subclass under some circumstances.

    .. change::
        :tags: bug, orm declarative
        :tickets: 8742

        Fixed issues within the declarative typing resolver (i.e. which resolves
        ``ForwardRef`` objects) where types that were declared for columns in one
        particular source file would raise ``NameError`` when the ultimate mapped
        class were in another source file.  The types are now resolved in terms
        of the module for each class in which the types are used.

    .. change::
        :tags: feature, postgresql
        :tickets: 8706

        Added new methods :meth:`_postgresql.Range.contains` and
        :meth:`_postgresql.Range.contained_by` to the new :class:`.Range` data
        object, which mirror the behavior of the PostgreSQL ``@>`` and ``<@``
        operators, as well as the
        :meth:`_postgresql.AbstractRange.comparator_factory.contains` and
        :meth:`_postgresql.AbstractRange.comparator_factory.contained_by` SQL
        operator methods. Pull request courtesy Lele Gaifax.

.. changelog::
    :version: 2.0.0b2
    :released: January 26, 2023
    :released: October 20, 2022

    .. change::
        :tags: bug, orm
        :tickets: 8656

        Removed the warning that emits when using ORM-enabled update/delete
        regarding evaluation of columns by name, first added in :ticket:`4073`;
        this warning actually covers up a scenario that otherwise could populate
        the wrong Python value for an ORM mapped attribute depending on what the
        actual column is, so this deprecated case is removed. In 2.0, ORM enabled
        update/delete uses "auto" for "synchronize_session", which should do the
        right thing automatically for any given UPDATE expression.

    .. change::
        :tags: bug, mssql
        :tickets: 8661

        Fixed regression caused by SQL Server pyodbc change :ticket:`8177` where we
        now use ``setinputsizes()`` by default; for VARCHAR, this fails if the
        character size is greater than 4000 (or 2000, depending on data) characters
        as the incoming datatype is NVARCHAR, which has a limit of 4000 characters,
        despite the fact that VARCHAR can handle unlimited characters. Additional
        pyodbc-specific typing information is now passed to ``setinputsizes()``
        when the datatype's size is > 2000 characters. The change is also applied
        to the :class:`_types.JSON` type which was also impacted by this issue for large
        JSON serializations.

    .. change::
        :tags: bug, typing
        :tickets: 8645

        Fixed typing issue where pylance strict mode would report "instance
        variable overrides class variable" when using a method to define
        ``__tablename__``, ``__mapper_args__`` or ``__table_args__``.

    .. change::
        :tags: mssql, bug
        :tickets: 7211

        The :class:`.Sequence` construct restores itself to the DDL behavior it
        had prior to the 1.4 series, where creating a :class:`.Sequence` with
        no additional arguments will emit a simple ``CREATE SEQUENCE`` instruction
        **without** any additional parameters for "start value".   For most backends,
        this is how things worked previously in any case; **however**, for
        MS SQL Server, the default value on this database is
        ``-2**63``; to prevent this generally impractical default
        from taking effect on SQL Server, the :paramref:`.Sequence.start` parameter
        should be provided.   As usage of :class:`.Sequence` is unusual
        for SQL Server which for many years has standardized on ``IDENTITY``,
        it is hoped that this change has minimal impact.

        .. seealso::

            :ref:`change_7211`

    .. change::
        :tags: bug, declarative, orm
        :tickets: 8665

        Improved the :class:`.DeclarativeBase` class so that when combined with
        other mixins like :class:`.MappedAsDataclass`, the order of the classes may
        be in either order.


    .. change::
        :tags: usecase, declarative, orm
        :tickets: 8665

        Added support for mapped classes that are also ``Generic`` subclasses,
        to be specified as a ``GenericAlias`` object (e.g. ``MyClass[str]``)
        within statements and calls to :func:`_sa.inspect`.



    .. change::
        :tags: bug, orm, declarative
        :tickets: 8668

        Fixed bug in new ORM typed declarative mappings where the ability
        to use ``Optional[MyClass]`` or similar forms such as ``MyClass | None``
        in the type annotation for a many-to-one relationship was not implemented,
        leading to errors.   Documentation has also been added for this use
        case to the relationship configuration documentation.

    .. change::
        :tags: bug, typing
        :tickets: 8644

        Fixed typing issue where pylance strict mode would report "partially
        unknown" datatype for the :func:`_orm.mapped_column` construct.

    .. change::
        :tags: bug, regression, sql
        :tickets: 8639

        Fixed bug in new "insertmanyvalues" feature where INSERT that included a
        subquery with :func:`_sql.bindparam` inside of it would fail to render
        correctly in "insertmanyvalues" format. This affected psycopg2 most
        directly as "insertmanyvalues" is used unconditionally with this driver.


    .. change::
        :tags: bug, orm, declarative
        :tickets: 8688

        Fixed issue with new dataclass mapping feature where arguments passed to
        the dataclasses API could sometimes be mis-ordered when dealing with mixins
        that override :func:`_orm.mapped_column` declarations, leading to
        initializer problems.

.. changelog::
    :version: 2.0.0b1
    :released: January 26, 2023
    :released: October 13, 2022

    .. change::
        :tags: bug, sql
        :tickets: 7888

        The FROM clauses that are established on a :func:`_sql.select` construct
        when using the :meth:`_sql.Select.select_from` method will now render first
        in the FROM clause of the rendered SELECT, which serves to maintain the
        ordering of clauses as was passed to the :meth:`_sql.Select.select_from`
        method itself without being affected by the presence of those clauses also
        being mentioned in other parts of the query. If other elements of the
        :class:`_sql.Select` also generate FROM clauses, such as the columns clause
        or WHERE clause, these will render after the clauses delivered by
        :meth:`_sql.Select.select_from` assuming they were not explictly passed to
        :meth:`_sql.Select.select_from` also. This improvement is useful in those
        cases where a particular database generates a desirable query plan based on
        a particular ordering of FROM clauses and allows full control over the
        ordering of FROM clauses.

    .. change::
        :tags: usecase, sql
        :tickets: 7998

        Altered the compilation mechanics of the :class:`_dml.Insert` construct
        such that the "autoincrement primary key" column value will be fetched via
        ``cursor.lastrowid`` or RETURNING even if present in the parameter set or
        within the :meth:`_dml.Insert.values` method as a plain bound value, for
        single-row INSERT statements on specific backends that are known to
        generate autoincrementing values even when explicit NULL is passed. This
        restores a behavior that was in the 1.3 series for both the use case of
        separate parameter set as well as :meth:`_dml.Insert.values`. In 1.4, the
        parameter set behavior unintentionally changed to no longer do this, but
        the :meth:`_dml.Insert.values` method would still fetch autoincrement
        values up until 1.4.21 where :ticket:`6770` changed the behavior yet again
        again unintentionally as this use case was never covered.

        The behavior is now defined as "working" to suit the case where databases
        such as SQLite, MySQL and MariaDB will ignore an explicit NULL primary key
        value and nonetheless invoke an autoincrement generator.

    .. change::
        :tags: change, postgresql

        SQLAlchemy now requires PostgreSQL version 9 or greater.
        Older versions may still work in some limited use cases.

    .. change::
        :tags: bug, orm

        Fixed issue where the :meth:`_orm.registry.map_declaratively` method
        would return an internal "mapper config" object and not the
        :class:`.Mapper` object as stated in the API documentation.

    .. change::
        :tags: sybase, removed
        :tickets: 7258

        Removed the "sybase" internal dialect that was deprecated in previous
        SQLAlchemy versions.  Third party dialect support is available.

        .. seealso::

            :ref:`external_toplevel`

    .. change::
        :tags: bug, orm
        :tickets: 7463

        Fixed performance regression which appeared at least in version 1.3 if not
        earlier (sometime after 1.0) where the loading of deferred columns, those
        explicitly mapped with :func:`_orm.defer` as opposed to non-deferred
        columns that were expired, from a joined inheritance subclass would not use
        the "optimized" query which only queried the immediate table that contains
        the unloaded columns, instead running a full ORM query which would emit a
        JOIN for all base tables, which is not necessary when only loading columns
        from the subclass.


    .. change::
        :tags: bug, sql
        :tickets: 7791

        The :paramref:`.Enum.length` parameter, which sets the length of the
        ``VARCHAR`` column for non-native enumeration types, is now used
        unconditionally when emitting DDL for the ``VARCHAR`` datatype, including
        when the :paramref:`.Enum.native_enum` parameter is set to ``True`` for
        target backends that continue to use ``VARCHAR``. Previously the parameter
        would be erroneously ignored in this case. The warning previously emitted
        for this case is now removed.

    .. change::
        :tags: bug, orm
        :tickets: 6986

        The internals for the :class:`_orm.Load` object and related loader strategy
        patterns have been mostly rewritten, to take advantage of the fact that
        only attribute-bound paths, not strings, are now supported. The rewrite
        hopes to make it more straightforward to address new use cases and subtle
        issues within the loader strategy system going forward.

    .. change::
        :tags: usecase, orm

        Added :paramref:`_orm.load_only.raiseload` parameter to the
        :func:`_orm.load_only` loader option, so that the unloaded attributes may
        have "raise" behavior rather than lazy loading. Previously there wasn't
        really a way to do this with the :func:`_orm.load_only` option directly.

    .. change::
        :tags: change, engine
        :tickets: 7122

        Some small API changes regarding engines and dialects:

        * The :meth:`.Dialect.set_isolation_level`, :meth:`.Dialect.get_isolation_level`,
          :meth:
          dialect methods will always be passed the raw DBAPI connection

        * The :class:`.Connection` and :class:`.Engine` classes no longer share a base
          ``Connectable`` superclass, which has been removed.

        * Added a new interface class :class:`.PoolProxiedConnection` - this is the
          public facing interface for the familiar :class:`._ConnectionFairy`
          class which is nonetheless a private class.

    .. change::
        :tags: feature, sql
        :tickets: 3482

          Added long-requested case-insensitive string operators
          :meth:`_sql.ColumnOperators.icontains`,
          :meth:`_sql.ColumnOperators.istartswith`,
          :meth:`_sql.ColumnOperators.iendswith`, which produce case-insensitive
          LIKE compositions (using ILIKE on PostgreSQL, and the LOWER() function on
          all other backends) to complement the existing LIKE composition operators
          :meth:`_sql.ColumnOperators.contains`,
          :meth:`_sql.ColumnOperators.startswith`, etc. Huge thanks to Matias
          Martinez Rebori for their meticulous and complete efforts in implementing
          these new methods.

    .. change::
        :tags: usecase, postgresql
        :tickets: 8138

        Added literal type rendering for the :class:`_sqltypes.ARRAY` and
        :class:`_postgresql.ARRAY` datatypes. The generic stringify will render
        using brackets, e.g. ``[1, 2, 3]`` and the PostgreSQL specific will use the
        ARRAY literal e.g. ``ARRAY[1, 2, 3]``.   Multiple dimensions and quoting
        are also taken into account.

    .. change::
        :tags: bug, orm
        :tickets: 8166

        Made an improvement to the "deferred" / "load_only" set of strategy options
        where if a certain object is loaded from two different logical paths within
        one query, attributes that have been configured by at least one of the
        options to be populated will be populated in all cases, even if other load
        paths for that same object did not set this option. previously, it was
        based on randomness as to which "path" addressed the object first.

    .. change::
        :tags: feature, orm, sql
        :tickets: 6047

        Added new feature to all included dialects that support RETURNING
        called "insertmanyvalues".  This is a generalization of the
        "fast executemany" feature first introduced for the psycopg2 driver
        in 1.4 at :ref:`change_5263`, which allows the ORM to batch INSERT
        statements into a much more efficient SQL structure while still being
        able to fetch newly generated primary key and SQL default values
        using RETURNING.

        The feature now applies to the many dialects that support RETURNING along
        with multiple VALUES constructs for INSERT, including all PostgreSQL
        drivers, SQLite, MariaDB, MS SQL Server. Separately, the Oracle dialect
        also gains the same capability using native cx_Oracle or OracleDB features.

    .. change::
        :tags: bug, engine
        :tickets: 8523

        The :class:`_pool.QueuePool` now ignores ``max_overflow`` when
        ``pool_size=0``, properly making the pool unlimited in all cases.

    .. change::
        :tags: bug, sql
        :tickets: 7909

        The in-place type detection for Python integers, as occurs with an
        expression such as ``literal(25)``, will now apply value-based adaption as
        well to accommodate Python large integers, where the datatype determined
        will be :class:`.BigInteger` rather than :class:`.Integer`. This
        accommodates for dialects such as that of asyncpg which both sends implicit
        typing information to the driver as well as is sensitive to numeric scale.

    .. change::
        :tags: postgresql, mssql, change
        :tickets: 7225

        The parameter :paramref:`_types.UUID.as_uuid` of :class:`_types.UUID`,
        previously specific to the PostgreSQL dialect but now generalized for Core
        (along with a new backend-agnostic :class:`_types.Uuid` datatype) now
        defaults to ``True``, indicating that Python ``UUID`` objects are accepted
        by this datatype by default. Additionally, the SQL Server
        :class:`_mssql.UNIQUEIDENTIFIER` datatype has been converted to be a
        UUID-receiving type; for legacy code that makes use of
        :class:`_mssql.UNIQUEIDENTIFIER` using string values, set the
        :paramref:`_mssql.UNIQUEIDENTIFIER.as_uuid` parameter to ``False``.

    .. change::
        :tags: bug, orm
        :tickets: 8344

        Fixed issue in ORM enabled UPDATE when the statement is created against a
        joined-inheritance subclass, updating only local table columns, where the
        "fetch" synchronization strategy would not render the correct RETURNING
        clause for databases that use RETURNING for fetch synchronization.
        Also adjusts the strategy used for RETURNING in UPDATE FROM and
        DELETE FROM statements.

    .. change::
        :tags: usecase, mariadb
        :tickets: 8344

        Added a new execution option ``is_delete_using=True``, which is consumed
        by the ORM when using an ORM-enabled DELETE statement in conjunction with
        the "fetch" synchronization strategy; this option indicates that the
        DELETE statement is expected to use multiple tables, which on MariaDB
        is the DELETE..USING syntax.   The option then indicates that
        RETURNING (newly implemented in SQLAlchemy 2.0 for MariaDB
        for  :ticket:`7011`) should not be used for databases that are known
        to not support "DELETE..USING..RETURNING" syntax, even though they
        support "DELETE..USING", which is MariaDB's current capability.

        The rationale for this option is that the current workings of ORM-enabled
        DELETE doesn't know up front if a DELETE statement is against multiple
        tables or not until compilation occurs, which is cached in any case, yet it
        needs to be known so that a SELECT for the to-be-deleted row can be emitted
        up front. Instead of applying an across-the-board performance penalty for
        all DELETE statements by proactively checking them all for this
        relatively unusual SQL pattern, the ``is_delete_using=True`` execution
        option is requested via a new exception message that is raised
        within the compilation step.  This exception message is specifically
        (and only) raised when:   the statement is an ORM-enabled DELETE where
        the "fetch" synchronization strategy has been requested; the
        backend is MariaDB or other backend with this specific limitation;
        the statement has been detected within the initial compilation
        that it would otherwise emit "DELETE..USING..RETURNING".   By applying
        the execution option, the ORM knows to run a SELECT upfront instead.
        A similar option is implemented for ORM-enabled UPDATE but there is not
        currently a backend where it is needed.



    .. change::
        :tags: bug, orm, asyncio
        :tickets: 7703

        Removed the unused ``**kw`` arguments from
        :class:`_asyncio.AsyncSession.begin` and
        :class:`_asyncio.AsyncSession.begin_nested`. These kw aren't used and
        appear to have been added to the API in error.

    .. change::
        :tags: feature, sql
        :tickets: 8285

        Added new syntax to the :attr:`.FromClause.c` collection on all
        :class:`.FromClause` objects allowing tuples of keys to be passed to
        ``__getitem__()``, along with support for the :func:`_sql.select` construct
        to handle the resulting tuple-like collection directly, allowing the syntax
        ``select(table.c['a', 'b', 'c'])`` to be possible. The sub-collection
        returned is itself a :class:`.ColumnCollection` which is also directly
        consumable by :func:`_sql.select` and similar now.

        .. seealso::

            :ref:`tutorial_selecting_columns`

    .. change::
        :tags: general, changed
        :tickets: 7257

        Migrated the codebase to remove all pre-2.0 behaviors and architectures
        that were previously noted as deprecated for removal in 2.0, including,
        but not limited to:

        * removal of all Python 2 code, minimum version is now Python 3.7

        * :class:`_engine.Engine` and :class:`_engine.Connection` now use the
          new 2.0 style of working, which includes "autobegin", library level
          autocommit removed, subtransactions and "branched" connections
          removed

        * Result objects use 2.0-style behaviors; :class:`_result.Row` is fully
          a named tuple without "mapping" behavior, use :class:`_result.RowMapping`
          for "mapping" behavior

        * All Unicode encoding/decoding architecture has been removed from
          SQLAlchemy.  All modern DBAPI implementations support Unicode
          transparently thanks to Python 3, so the ``convert_unicode`` feature
          as well as related mechanisms to look for bytestrings in
          DBAPI ``cursor.description`` etc. have been removed.

        * The ``.bind`` attribute and parameter from :class:`.MetaData`,
          :class:`.Table`, and from all DDL/DML/DQL elements that previously could
          refer to a "bound engine"

        * The standalone ``sqlalchemy.orm.mapper()`` function is removed; all
          classical mapping should be done through the
          :meth:`_orm.registry.map_imperatively` method of :class:`_orm.registry`.

        * The :meth:`_orm.Query.join` method no longer accepts strings for
          relationship names; the long-documented approach of using
          ``Class.attrname`` for join targets is now standard.

        * :meth:`_orm.Query.join` no longer accepts the "aliased" and
          "from_joinpoint" arguments

        * :meth:`_orm.Query.join` no longer accepts chains of multiple join
          targets in one method call.

        * ``Query.from_self()``, ``Query.select_entity_from()`` and
          ``Query.with_polymorphic()`` are removed.

        * The :paramref:`_orm.relationship.cascade_backrefs` parameter must now
          remain at its new default of ``False``; the ``save-update`` cascade
          no longer cascades along a backref.

        * the :paramref:`_orm.Session.future` parameter must always be set to
          ``True``.  2.0-style transactional patterns for :class:`_orm.Session`
          are now always in effect.

        * Loader options no longer accept strings for attribute names.  The
          long-documented approach of using ``Class.attrname`` for loader option
          targets is now standard.

        * Legacy forms of :func:`_sql.select` removed, including
          ``select([cols])``, the "whereclause" and keyword parameters of
          ``some_table.select()``.

        * Legacy "in-place mutator" methods on :class:`_sql.Select` such as
          ``append_whereclause()``, ``append_order_by()`` etc are removed.

        * Removed the very old "dbapi_proxy" module, which in very early
          SQLAlchemy releases was used to provide a transparent connection pool
          over a raw DBAPI connection.

    .. change::
        :tags: feature, orm
        :tickets: 8375

        Added new parameter :paramref:`_orm.AttributeEvents.include_key`, which
        will include the dictionary or list key for operations such as
        ``__setitem__()`` (e.g. ``obj[key] = value``) and ``__delitem__()`` (e.g.
        ``del obj[key]``), using a new keyword parameter "key" or "keys", depending
        on event, e.g. :paramref:`_orm.AttributeEvents.append.key`,
        :paramref:`_orm.AttributeEvents.bulk_replace.keys`. This allows event
        handlers to take into account the key that was passed to the operation and
        is of particular importance for dictionary operations working with
        :class:`_orm.MappedCollection`.


    .. change::
        :tags: postgresql, usecase
        :tickets: 7156, 8540

        Adds support for PostgreSQL multirange types, introduced in PostgreSQL 14.
        Support for PostgreSQL ranges and multiranges has now been generalized to
        the psycopg3, psycopg2 and asyncpg backends, with room for further dialect
        support, using a backend-agnostic :class:`_postgresql.Range` data object
        that's constructor-compatible with the previously used psycopg2 object. See
        the new documentation for usage patterns.

        In addition, range type handling has been enhanced so that it automatically
        renders type casts, so that in-place round trips for statements that don't
        provide the database with any context don't require the :func:`_sql.cast`
        construct to be explicit for the database to know the desired type
        (discussed at :ticket:`8540`).

        Thanks very much to @zeeeeeb for the pull request implementing and testing
        the new datatypes and psycopg support.

        .. seealso::

            :ref:`change_7156`

            :ref:`postgresql_ranges`

    .. change::
        :tags: usecase, oracle
        :tickets: 8221

        Oracle will now use FETCH FIRST N ROWS / OFFSET syntax for limit/offset
        support by default for Oracle 12c and above. This syntax was already
        available when :meth:`_sql.Select.fetch` were used directly, it's now
        implied for :meth:`_sql.Select.limit` and :meth:`_sql.Select.offset` as
        well.


    .. change::
        :tags: feature, orm
        :tickets: 3162

        Added new parameter :paramref:`_sql.Operators.op.python_impl`, available
        from :meth:`_sql.Operators.op` and also when using the
        :class:`_sql.Operators.custom_op` constructor directly, which allows an
        in-Python evaluation function to be provided along with the custom SQL
        operator. This evaluation function becomes the implementation used when the
        operator object is used given plain Python objects as operands on both
        sides, and in particular is compatible with the
        ``synchronize_session='evaluate'`` option used with
        :ref:`orm_expression_update_delete`.

    .. change::
        :tags: schema, postgresql
        :tickets: 5677

        Added support for comments on :class:`.Constraint` objects, including
        DDL and reflection; the field is added to the base :class:`.Constraint`
        class and corresponding constructors, however PostgreSQL is the only
        included backend to support the feature right now.
        See parameters such as :paramref:`.ForeignKeyConstraint.comment`,
        :paramref:`.UniqueConstraint.comment` or
        :paramref:`.CheckConstraint.comment`.

    .. change::
        :tags: sqlite, usecase
        :tickets: 8234

        Added new parameter to SQLite for reflection methods called
        ``sqlite_include_internal=True``; when omitted, local tables that start
        with the prefix ``sqlite_``, which per SQLite documentation are noted as
        "internal schema" tables such as the ``sqlite_sequence`` table generated to
        support "AUTOINCREMENT" columns, will not be included in reflection methods
        that return lists of local objects. This prevents issues for example when
        using Alembic autogenerate, which previously would consider these
        SQLite-generated tables as being remove from the model.

        .. seealso::

            :ref:`sqlite_include_internal`

    .. change::
        :tags: feature, postgresql
        :tickets: 7316

        Added a new PostgreSQL :class:`_postgresql.DOMAIN` datatype, which follows
        the same CREATE TYPE / DROP TYPE behaviors as that of PostgreSQL
        :class:`_postgresql.ENUM`. Much thanks to David Baumgold for the efforts on
        this.

        .. seealso::

            :class:`_postgresql.DOMAIN`

    .. change::
        :tags: change, postgresql

        The :paramref:`_postgresql.ENUM.name` parameter for the PostgreSQL-specific
        :class:`_postgresql.ENUM` datatype is now a required keyword argument. The
        "name" is necessary in any case in order for the :class:`_postgresql.ENUM`
        to be usable as an error would be raised at SQL/DDL render time if "name"
        were not present.

    .. change::
        :tags: oracle, feature
        :tickets: 8054

        Add support for the new oracle driver ``oracledb``.

        .. seealso::

            :ref:`ticket_8054`

            :ref:`oracledb`

    .. change::
        :tags: bug, engine
        :tickets: 8567

        For improved security, the :class:`_url.URL` object will now use password
        obfuscation by default when ``str(url)`` is called. To stringify a URL with
        cleartext password, the :meth:`_url.URL.render_as_string` may be used,
        passing the :paramref:`_url.URL.render_as_string.hide_password` parameter
        as ``False``. Thanks to our contributors for this pull request.

        .. seealso::

            :ref:`change_8567`

    .. change::
        :tags: change, orm

        To better accommodate explicit typing, the names of some ORM constructs
        that are typically constructed internally, but nonetheless are sometimes
        visible in messaging as well as typing, have been changed to more succinct
        names which also match the name of their constructing function (with
        different casing), in all cases maintaining aliases to the old names for
        the forseeable future:

        * :class:`_orm.RelationshipProperty` becomes an alias for the primary name
          :class:`_orm.Relationship`, which is constructed as always from the
          :func:`_orm.relationship` function
        * :class:`_orm.SynonymProperty` becomes an alias for the primary name
          :class:`_orm.Synonym`, constructed as always from the
          :func:`_orm.synonym` function
        * :class:`_orm.CompositeProperty` becomes an alias for the primary name
          :class:`_orm.Composite`, constructed as always from the
          :func:`_orm.composite` function

    .. change::
        :tags: orm, change
        :tickets: 8608

        For consistency with the prominent ORM concept :class:`_orm.Mapped`, the
        names of the dictionary-oriented collections,
        :func:`_orm.attribute_mapped_collection`,
        :func:`_orm.column_mapped_collection`, and :class:`_orm.MappedCollection`,
        are changed to :func:`_orm.attribute_keyed_dict`,
        :func:`_orm.column_keyed_dict` and :class:`_orm.KeyFuncDict`, using the
        phrase "dict" to minimize any confusion against the term "mapped". The old
        names will remain indefinitely with no schedule for removal.

    .. change::
        :tags: bug, sql
        :tickets: 7354

        Added ``if_exists`` and ``if_not_exists`` parameters for all "Create" /
        "Drop" constructs including :class:`.CreateSequence`,
        :class:`.DropSequence`, :class:`.CreateIndex`, :class:`.DropIndex`, etc.
        allowing generic "IF EXISTS" / "IF NOT EXISTS" phrases to be rendered
        within DDL. Pull request courtesy Jesse Bakker.


    .. change::
        :tags: engine, usecase
        :tickets: 6342

        Generalized the :paramref:`_sa.create_engine.isolation_level` parameter to
        the base dialect so that it is no longer dependent on individual dialects
        to be present. This parameter sets up the "isolation level" setting to
        occur for all new database connections as soon as they are created by the
        connection pool, where the value then stays set without being reset on
        every checkin.

        The :paramref:`_sa.create_engine.isolation_level` parameter is essentially
        equivalent in functionality to using the
        :paramref:`_engine.Engine.execution_options.isolation_level` parameter via
        :meth:`_engine.Engine.execution_options` for an engine-wide setting. The
        difference is in that the former setting assigns the isolation level just
        once when a connection is created, the latter sets and resets the given
        level on each connection checkout.

    .. change::
        :tags: bug, orm
        :tickets: 8372

        Changed the attribute access method used by
        :func:`_orm.attribute_mapped_collection` and
        :func:`_orm.column_mapped_collection` (now called
        :func:`_orm.attribute_keyed_dict` and :func:`_orm.column_keyed_dict`) ,
        used when populating the dictionary, to assert that the data value on
        the object to be used as the dictionary key is actually present, and is
        not instead using "None" due to the attribute never being actually
        assigned. This is used to prevent a mis-population of None for a key
        when assigning via a backref where the "key" attribute on the object is
        not yet assigned.

        As the failure mode here is a transitory condition that is not typically
        persisted to the database, and is easy to produce via the constructor of
        the class based on the order in which parameters are assigned, it is very
        possible that many applications include this behavior already which is
        silently passed over. To accommodate for applications where this error is
        now raised, a new parameter
        :paramref:`_orm.attribute_keyed_dict.ignore_unpopulated_attribute`
        is also added to both :func:`_orm.attribute_keyed_dict` and
        :func:`_orm.column_keyed_dict` that instead causes the erroneous
        backref assignment to be skipped.

    .. change::
        :tags: usecase, postgresql
        :tickets: 8491

        The "ping" query emitted when configuring
        :paramref:`_sa.create_engine.pool_pre_ping` for psycopg, asyncpg and
        pg8000, but not for psycopg2, has been changed to be an empty query (``;``)
        instead of ``SELECT 1``; additionally, for the asyncpg driver, the
        unnecessary use of a prepared statement for this query has been fixed.
        Rationale is to eliminate the need for PostgreSQL to produce a query plan
        when the ping is emitted. The operation is not currently supported by the
        ``psycopg2`` driver which continues to use ``SELECT 1``.

    .. change::
        :tags: bug, oracle
        :tickets: 7494

        Adjustments made to the BLOB / CLOB / NCLOB datatypes in the cx_Oracle and
        oracledb dialects, to improve performance based on recommendations from
        Oracle developers.

    .. change::
        :tags: feature, orm
        :tickets: 7433

        The :class:`_orm.Session` (and by extension :class:`.AsyncSession`) now has
        new state-tracking functionality that will proactively trap any unexpected
        state changes which occur as a particular transactional method proceeds.
        This is to allow situations where the :class:`_orm.Session` is being used
        in a thread-unsafe manner, where event hooks or similar may be calling
        unexpected methods within operations, as well as potentially under other
        concurrency situations such as asyncio or gevent to raise an informative
        message when the illegal access first occurs, rather than passing silently
        leading to secondary failures due to the :class:`_orm.Session` being in an
        invalid state.

        .. seealso::

            :ref:`change_7433`

    .. change::
        :tags: postgresql, dialect
        :tickets: 6842

        Added support for ``psycopg`` dialect supporting both sync and async
        execution. This dialect is available under the ``postgresql+psycopg`` name
        for both the :func:`_sa.create_engine` and
        :func:`_asyncio.create_async_engine` engine-creation functions.

        .. seealso::

            :ref:`ticket_6842`

            :ref:`postgresql_psycopg`



    .. change::
        :tags: usecase, sqlite
        :tickets: 6195

        Added RETURNING support for the SQLite dialect.  SQLite supports RETURNING
        since version 3.35.


    .. change::
        :tags: usecase, mariadb
        :tickets: 7011

        Added INSERT..RETURNING and DELETE..RETURNING support for the MariaDB
        dialect.  UPDATE..RETURNING is not yet supported by MariaDB.  MariaDB
        supports INSERT..RETURNING as of 10.5.0 and DELETE..RETURNING as of
        10.0.5.



    .. change::
        :tags: feature, orm

        The :func:`_orm.composite` mapping construct now supports automatic
        resolution of values when used with a Python ``dataclass``; the
        ``__composite_values__()`` method no longer needs to be implemented as this
        method is derived from inspection of the dataclass.

        Additionally, classes mapped by :class:`_orm.composite` now support
        ordering comparison operations, e.g. ``<``, ``>=``, etc.

        See the new documentation at :ref:`mapper_composite` for examples.

    .. change::
        :tags: engine, bug
        :tickets: 7161

        The :meth:`_engine.Inspector.has_table` method will now consistently check
        for views of the given name as well as tables. Previously this behavior was
        dialect dependent, with PostgreSQL, MySQL/MariaDB and SQLite supporting it,
        and Oracle and SQL Server not supporting it. Third party dialects should
        also seek to ensure their :meth:`_engine.Inspector.has_table` method
        searches for views as well as tables for the given name.

    .. change::
        :tags: feature, engine
        :tickets: 5648

        The :meth:`.DialectEvents.handle_error` event is now moved to the
        :class:`.DialectEvents` suite from the :class:`.EngineEvents` suite, and
        now participates in the connection pool "pre ping" event for those dialects
        that make use of disconnect codes in order to detect if the database is
        live. This allows end-user code to alter the state of "pre ping". Note that
        this does not include dialects which contain a native "ping" method such as
        that of psycopg2 or most MySQL dialects.

    .. change::
        :tags: feature, sql
        :tickets: 7212

        Added new backend-agnostic :class:`_types.Uuid` datatype generalized from
        the PostgreSQL dialects to now be a core type, as well as migrated
        :class:`_types.UUID` from the PostgreSQL dialect. The SQL Server
        :class:`_mssql.UNIQUEIDENTIFIER` datatype also becomes a UUID-handling
        datatype. Thanks to Trevor Gross for the help on this.

    .. change::
        :tags: feature, orm
        :tickets: 8126

        Added very experimental feature to the :func:`_orm.selectinload` and
        :func:`_orm.immediateload` loader options called
        :paramref:`_orm.selectinload.recursion_depth` /
        :paramref:`_orm.immediateload.recursion_depth` , which allows a single
        loader option to automatically recurse into self-referential relationships.
        Is set to an integer indicating depth, and may also be set to -1 to
        indicate to continue loading until no more levels deep are found.
        Major internal changes to :func:`_orm.selectinload` and
        :func:`_orm.immediateload` allow this feature to work while continuing
        to make correct use of the compilation cache, as well as not using
        arbitrary recursion, so any level of depth is supported (though would
        emit that many queries).  This may be useful for
        self-referential structures that must be loaded fully eagerly, such as when
        using asyncio.

        A warning is also emitted when loader options are connected together with
        arbitrary lengths (that is, without using the new ``recursion_depth``
        option) when excessive recursion depth is detected in related object
        loading. This operation continues to use huge amounts of memory and
        performs extremely poorly; the cache is disabled when this condition is
        detected to protect the cache from being flooded with arbitrary statements.

    .. change::
        :tags: bug, orm
        :tickets: 8403

        Added new parameter :paramref:`.AbstractConcreteBase.strict_attrs` to the
        :class:`.AbstractConcreteBase` declarative mixin class. The effect of this
        parameter is that the scope of attributes on subclasses is correctly
        limited to the subclass in which each attribute is declared, rather than
        the previous behavior where all attributes of the entire hierarchy are
        applied to the base "abstract" class. This produces a cleaner, more correct
        mapping where subclasses no longer have non-useful attributes on them which
        are only relevant to sibling classes. The default for this parameter is
        False, which leaves the previous behavior unchanged; this is to support
        existing code that makes explicit use of these attributes in queries.
        To migrate to the newer approach, apply explicit attributes to the abstract
        base class as needed.

    .. change::
        :tags: usecase, mysql, mariadb
        :tickets: 8503

        The ``ROLLUP`` function will now correctly render ``WITH ROLLUP`` on
        MySql and MariaDB, allowing the use of group by rollup with these
        backend.

    .. change::
        :tags: feature, orm
        :tickets: 6928

        Added new parameter :paramref:`_orm.Session.autobegin`, which when set to
        ``False`` will prevent the :class:`_orm.Session` from beginning a
        transaction implicitly. The :meth:`_orm.Session.begin` method must be
        called explicitly first in order to proceed with operations, otherwise an
        error is raised whenever any operation would otherwise have begun
        automatically. This option can be used to create a "safe"
        :class:`_orm.Session` that won't implicitly start new transactions.

        As part of this change, also added a new status variable
        :class:`_orm.SessionTransaction.origin` which may be useful for event
        handling code to be aware of the origin of a particular
        :class:`_orm.SessionTransaction`.



    .. change::
        :tags: feature, platform
        :tickets: 7256

        The SQLAlchemy C extensions have been replaced with all new implementations
        written in Cython.  Like the C extensions before, pre-built wheel files
        for a wide range of platforms are available on pypi so that building
        is not an issue for common platforms.  For custom builds, ``python setup.py build_ext``
        works as before, needing only the additional Cython install.  ``pyproject.toml``
        is also part of the source now which will establish the proper build dependencies
        when using pip.


        .. seealso::

            :ref:`change_7256`

    .. change::
        :tags: change, platform
        :tickets: 7311

        SQLAlchemy's source build and installation now includes a ``pyproject.toml`` file
        for full :pep:`517` support.

        .. seealso::

            :ref:`change_7311`

    .. change::
        :tags: feature, schema
        :tickets: 7631

        Expanded on the "conditional DDL" system implemented by the
        :class:`_schema.ExecutableDDLElement` class (renamed from
        :class:`_schema.DDLElement`) to be directly available on
        :class:`_schema.SchemaItem` constructs such as :class:`_schema.Index`,
        :class:`_schema.ForeignKeyConstraint`, etc. such that the conditional logic
        for generating these elements is included within the default DDL emitting
        process. This system can also be accommodated by a future release of
        Alembic to support conditional DDL elements within all schema-management
        systems.


        .. seealso::

            :ref:`ticket_7631`

    .. change::
        :tags: change, oracle
        :tickets:`4379`

        Materialized views on oracle are now reflected as views.
        On previous versions of SQLAlchemy the views were returned among
        the table names, not among the view names. As a side effect of
        this change they are not reflected by default by
        :meth:`_sql.MetaData.reflect`, unless ``views=True`` is set.
        To get a list of materialized views, use the new
        inspection method :meth:`.Inspector.get_materialized_view_names`.

    .. change::
        :tags: bug, sqlite
        :tickets: 7299

        Removed the warning that emits from the :class:`_types.Numeric` type about
        DBAPIs not supporting Decimal values natively. This warning was oriented
        towards SQLite, which does not have any real way without additional
        extensions or workarounds of handling precision numeric values more than 15
        significant digits as it only uses floating point math to represent
        numbers. As this is a known and documented limitation in SQLite itself, and
        not a quirk of the pysqlite driver, there's no need for SQLAlchemy to warn
        for this. The change does not otherwise modify how precision numerics are
        handled. Values can continue to be handled as ``Decimal()`` or ``float()``
        as configured with the :class:`_types.Numeric`, :class:`_types.Float` , and
        related datatypes, just without the ability to maintain precision beyond 15
        significant digits when using SQLite, unless alternate representations such
        as strings are used.

    .. change::
        :tags: mssql, bug
        :tickets: 8177

        The ``use_setinputsizes`` parameter for the ``mssql+pyodbc`` dialect now
        defaults to ``True``; this is so that non-unicode string comparisons are
        bound by pyodbc to pyodbc.SQL_VARCHAR rather than pyodbc.SQL_WVARCHAR,
        allowing indexes against VARCHAR columns to take effect. In order for the
        ``fast_executemany=True`` parameter to continue functioning, the
        ``use_setinputsizes`` mode now skips the ``cursor.setinputsizes()`` call
        specifically when ``fast_executemany`` is True and the specific method in
        use is ``cursor.executemany()``, which doesn't support setinputsizes. The
        change also adds appropriate pyodbc DBAPI typing to values that are typed
        as :class:`_types.Unicode` or :class:`_types.UnicodeText`, as well as
        altered the base :class:`_types.JSON` datatype to consider JSON string
        values as :class:`_types.Unicode` rather than :class:`_types.String`.

    .. change::
        :tags: bug, sqlite, performance
        :tickets: 7490

        The SQLite dialect now defaults to :class:`_pool.QueuePool` when a file
        based database is used. This is set along with setting the
        ``check_same_thread`` parameter to ``False``. It has been observed that the
        previous approach of defaulting to :class:`_pool.NullPool`, which does not
        hold onto database connections after they are released, did in fact have a
        measurable negative performance impact. As always, the pool class is
        customizable via the :paramref:`_sa.create_engine.poolclass` parameter.

        .. seealso::

            :ref:`change_7490`


    .. change::
        :tags: usecase, schema
        :tickets: 8141

        Added parameter :paramref:`_ddl.DropConstraint.if_exists` to the
        :class:`_ddl.DropConstraint` construct which result in "IF EXISTS" DDL
        being added to the DROP statement.
        This phrase is not accepted by all databases and the operation will fail
        on a database that does not support it as there is no similarly compatible
        fallback within the scope of a single DDL statement.
        Pull request courtesy Mike Fiedler.

    .. change::
        :tags: change, postgresql

        In support of new PostgreSQL features including the psycopg3 dialect as
        well as extended "fast insertmany" support, the system by which typing
        information for bound parameters is passed to the PostgreSQL database has
        been redesigned to use inline casts emitted by the SQL compiler, and is now
        applied to all PostgreSQL dialects. This is in contrast to the previous
        approach which would rely upon the DBAPI in use to render these casts
        itself, which in cases such as that of pg8000 and the adapted asyncpg
        driver, would use the pep-249 ``setinputsizes()`` method, or with the
        psycopg2 driver would rely on the driver itself in most cases, with some
        special exceptions made for ARRAY.

        The new approach now has all PostgreSQL dialects rendering these casts as
        needed using PostgreSQL double-colon style within the compiler, and the use
        of ``setinputsizes()`` is removed for PostgreSQL dialects, as this was not
        generally part of these DBAPIs in any case (pg8000 being the only
        exception, which added the method at the request of SQLAlchemy developers).

        Advantages to this approach include per-statement performance, as no second
        pass over the compiled statement is required at execution time, better
        support for all DBAPIs, as there is now one consistent system of applying
        typing information, and improved transparency, as the SQL logging output,
        as well as the string output of a compiled statement, will show these casts
        present in the statement directly, whereas previously these casts were not
        visible in logging output as they would occur after the statement were
        logged.



    .. change::
        :tags: engine, removed

        Removed the previously deprecated ``case_sensitive`` parameter from
        :func:`_sa.create_engine`, which would impact only the lookup of string
        column names in Core-only result set rows; it had no effect on the behavior
        of the ORM. The effective behavior of what ``case_sensitive`` refers
        towards remains at its default value of ``True``, meaning that string names
        looked up in ``row._mapping`` will match case-sensitively, just like any
        other Python mapping.

        Note that the ``case_sensitive`` parameter was not in any way related to
        the general subject of case sensitivity control, quoting, and "name
        normalization" (i.e. converting for databases that consider all uppercase
        words to be case insensitive) for DDL identifier names, which remains a
        normal core feature of SQLAlchemy.



    .. change::
        :tags: bug, sql
        :tickets: 7744

        Improved the construction of SQL binary expressions to allow for very long
        expressions against the same associative operator without special steps
        needed in order to avoid high memory use and excess recursion depth. A
        particular binary operation ``A op B`` can now be joined against another
        element ``op C`` and the resulting structure will be "flattened" so that
        the representation as well as SQL compilation does not require recursion.

        One effect of this change is that string concatenation expressions which
        use SQL functions come out as "flat", e.g. MySQL will now render
        ``concat('x', 'y', 'z', ...)``` rather than nesting together two-element
        functions like ``concat(concat('x', 'y'), 'z')``.  Third-party dialects
        which override the string concatenation operator will need to implement
        a new method ``def visit_concat_op_expression_clauselist()`` to
        accompany the existing ``def visit_concat_op_binary()`` method.

    .. change::
        :tags: feature, sql
        :tickets: 5465

        Added :class:`.Double`, :class:`.DOUBLE`, :class:`.DOUBLE_PRECISION`
        datatypes to the base ``sqlalchemy.`` module namespace, for explicit use of
        double/double precision as well as generic "double" datatypes. Use
        :class:`.Double` for generic support that will resolve to DOUBLE/DOUBLE
        PRECISION/FLOAT as needed for different backends.


    .. change::
        :tags: feature, oracle
        :tickets: 5465

        Implemented DDL and reflection support for ``FLOAT`` datatypes which
        include an explicit "binary_precision" value. Using the Oracle-specific
        :class:`_oracle.FLOAT` datatype, the new parameter
        :paramref:`_oracle.FLOAT.binary_precision` may be specified which will
        render Oracle's precision for floating point types directly. This value is
        interpreted during reflection. Upon reflecting back a ``FLOAT`` datatype,
        the datatype returned is one of :class:`_types.DOUBLE_PRECISION` for a
        ``FLOAT`` for a precision of 126 (this is also Oracle's default precision
        for ``FLOAT``), :class:`_types.REAL` for a precision of 63, and
        :class:`_oracle.FLOAT` for a custom precision, as per Oracle documentation.

        As part of this change, the generic :paramref:`_sqltypes.Float.precision`
        value is explicitly rejected when generating DDL for Oracle, as this
        precision cannot be accurately converted to "binary precision"; instead, an
        error message encourages the use of
        :meth:`_sqltypes.TypeEngine.with_variant` so that Oracle's specific form of
        precision may be chosen exactly. This is a backwards-incompatible change in
        behavior, as the previous "precision" value was silently ignored for
        Oracle.

        .. seealso::

            :ref:`change_5465_oracle`

    .. change::
        :tags: postgresql, psycopg2
        :tickets: 7238

        Update psycopg2 dialect to use the DBAPI interface to execute
        two phase transactions. Previously SQL commands were execute
        to handle this kind of transactions.

    .. change::
        :tags: deprecations, engine
        :tickets: 6962

        The :paramref:`_sa.create_engine.implicit_returning` parameter is
        deprecated on the :func:`_sa.create_engine` function only; the parameter
        remains available on the :class:`_schema.Table` object. This parameter was
        originally intended to enable the "implicit returning" feature of
        SQLAlchemy when it was first developed and was not enabled by default.
        Under modern use, there's no reason this parameter should be disabled, and
        it has been observed to cause confusion as it degrades performance and
        makes it more difficult for the ORM to retrieve recently inserted server
        defaults. The parameter remains available on :class:`_schema.Table` to
        specifically suit database-level edge cases which make RETURNING
        infeasible, the sole example currently being SQL Server's limitation that
        INSERT RETURNING may not be used on a table that has INSERT triggers on it.


    .. change::
        :tags: bug, oracle
        :tickets: 6962

        Related to the deprecation for
        :paramref:`_sa.create_engine.implicit_returning`, the "implicit_returning"
        feature is now enabled for the Oracle dialect in all cases; previously, the
        feature would be turned off when an Oracle 8/8i version were detected,
        however online documentation indicates both versions support the same
        RETURNING syntax as modern versions.

    .. change::
        :tags: bug, schema
        :tickets: 8102

        The warnings that are emitted regarding reflection of indexes or unique
        constraints, when the :paramref:`.Table.include_columns` parameter is used
        to exclude columns that are then found to be part of those constraints,
        have been removed. When the :paramref:`.Table.include_columns` parameter is
        used it should be expected that the resulting :class:`.Table` construct
        will not include constraints that rely upon omitted columns. This change
        was made in response to :ticket:`8100` which repaired
        :paramref:`.Table.include_columns` in conjunction with foreign key
        constraints that rely upon omitted columns, where the use case became
        clear that omitting such constraints should be expected.

    .. change::
        :tags: bug, postgresql
        :tickets: 7086

        The :meth:`.Operators.match` operator now uses ``plainto_tsquery()`` for
        PostgreSQL full text search, rather than ``to_tsquery()``. The rationale
        for this change is to provide better cross-compatibility with match on
        other database backends.    Full support for all PostgreSQL full text
        functions remains available through the use of :data:`.func` in
        conjunction with :meth:`.Operators.bool_op` (an improved version of
        :meth:`.Operators.op` for boolean operators).

        .. seealso::

            :ref:`change_7086`

    .. change::
        :tags: usecase, sql
        :tickets: 5052

        Added modified ISO-8601 rendering (i.e. ISO-8601 with the T converted to a
        space) when using ``literal_binds`` with the SQL compilers provided by the
        PostgreSQL, MySQL, MariaDB, MSSQL, Oracle dialects. For Oracle, the ISO
        format is wrapped inside of an appropriate TO_DATE() function call.
        Previously this rendering was not implemented for dialect-specific
        compilation.

        .. seealso::

            :ref:`change_5052`

    .. change::
        :tags: removed, engine
        :tickets: 7258

        Removed legacy and deprecated package ``sqlalchemy.databases``.
        Please use ``sqlalchemy.dialects`` instead.

    .. change::
        :tags: usecase, schema
        :tickets: 8394

        Implemented the DDL event hooks :meth:`.DDLEvents.before_create`,
        :meth:`.DDLEvents.after_create`, :meth:`.DDLEvents.before_drop`,
        :meth:`.DDLEvents.after_drop` for all :class:`.SchemaItem` objects that
        include a distinct CREATE or DROP step, when that step is invoked as a
        distinct SQL statement, including for :class:`.ForeignKeyConstraint`,
        :class:`.Sequence`, :class:`.Index`, and PostgreSQL's
        :class:`_postgresql.ENUM`.

    .. change::
        :tags: engine, feature

        The :meth:`.ConnectionEvents.set_connection_execution_options`
        and :meth:`.ConnectionEvents.set_engine_execution_options`
        event hooks now allow the given options dictionary to be modified
        in-place, where the new contents will be received as the ultimate
        execution options to be acted upon. Previously, in-place modifications to
        the dictionary were not supported.

    .. change::
        :tags: bug, sql
        :tickets: 4926

        Implemented full support for "truediv" and "floordiv" using the
        "/" and "//" operators.  A "truediv" operation between two expressions
        using :class:`_types.Integer` now considers the result to be
        :class:`_types.Numeric`, and the dialect-level compilation will cast
        the right operand to a numeric type on a dialect-specific basis to ensure
        truediv is achieved.  For floordiv, conversion is also added for those
        databases that don't already do floordiv by default (MySQL, Oracle) and
        the ``FLOOR()`` function is rendered in this case, as well as for
        cases where the right operand is not an integer (needed for PostgreSQL,
        others).

        The change resolves issues both with inconsistent behavior of the
        division operator on different backends and also fixes an issue where
        integer division on Oracle would fail to be able to fetch a result due
        to inappropriate outputtypehandlers.

        .. seealso::

            :ref:`change_4926`

    .. change::
        :tags: postgresql, schema
        :tickets: 8216

        Introduced the type :class:`_postgresql.JSONPATH` that can be used
        in cast expressions. This is required by some PostgreSQL dialects
        when using functions such as ``jsonb_path_exists`` or
        ``jsonb_path_match`` that accept a ``jsonpath`` as input.

        .. seealso::

            :ref:`postgresql_json_types` - PostgreSQL JSON types.

    .. change::
        :tags: schema, mysql, mariadb
        :tickets: 4038

        Add support for Partitioning and Sample pages on MySQL and MariaDB
        reflected options.
        The options are stored in the table dialect options dictionary, so
        the following keyword need to be prefixed with ``mysql_`` or ``mariadb_``
        depending on the backend.
        Supported options are:

        * ``stats_sample_pages``
        * ``partition_by``
        * ``partitions``
        * ``subpartition_by``

        These options are also reflected when loading a table from database,
        and will populate the table :attr:`_schema.Table.dialect_options`.
        Pull request courtesy of Ramon Will.

    .. change::
        :tags: usecase, mssql
        :tickets: 8288

        Implemented reflection of the "clustered index" flag ``mssql_clustered``
        for the SQL Server dialect. Pull request courtesy John Lennox.

    .. change::
        :tags: reflection, postgresql
        :tickets: 7442

        The PostgreSQL dialect now supports reflection of expression based indexes.
        The reflection is supported both when using
        :meth:`_engine.Inspector.get_indexes` and when reflecting a
        :class:`_schema.Table` using :paramref:`_schema.Table.autoload_with`.
        Thanks to immerrr and Aidan Kane for the help on this ticket.

    .. change::
        :tags: firebird, removed
        :tickets: 7258

        Removed the "firebird" internal dialect that was deprecated in previous
        SQLAlchemy versions.  Third party dialect support is available.

        .. seealso::

            :ref:`external_toplevel`

    .. change::
        :tags: bug, orm
        :tickets: 7495

        The behavior of :func:`_orm.defer` regarding primary key and "polymorphic
        discriminator" columns is revised such that these columns are no longer
        deferrable, either explicitly or when using a wildcard such as
        ``defer('*')``. Previously, a wildcard deferral would not load
        PK/polymorphic columns which led to errors in all cases, as the ORM relies
        upon these columns to produce object identities. The behavior of explicit
        deferral of primary key columns is unchanged as these deferrals already
        were implicitly ignored.

    .. change::
        :tags: bug, sql
        :tickets: 7471

        Added an additional lookup step to the compiler which will track all FROM
        clauses which are tables, that may have the same name shared in multiple
        schemas where one of the schemas is the implicit "default" schema; in this
        case, the table name when referring to that name without a schema
        qualification will be rendered with an anonymous alias name at the compiler
        level in order to disambiguate the two (or more) names. The approach of
        schema-qualifying the normally unqualified name with the server-detected
        "default schema name" value was also considered, however this approach
        doesn't apply to Oracle nor is it accepted by SQL Server, nor would it work
        with multiple entries in the PostgreSQL search path. The name collision
        issue resolved here has been identified as affecting at least Oracle,
        PostgreSQL, SQL Server, MySQL and MariaDB.


    .. change::
        :tags: improvement, typing
        :tickets: 6980

        The :meth:`_sqltypes.TypeEngine.with_variant` method now returns a copy of
        the original :class:`_sqltypes.TypeEngine` object, rather than wrapping it
        inside the ``Variant`` class, which is effectively removed (the import
        symbol remains for backwards compatibility with code that may be testing
        for this symbol). While the previous approach maintained in-Python
        behaviors, maintaining the original type allows for clearer type checking
        and debugging.

        :meth:`_sqltypes.TypeEngine.with_variant` also accepts multiple dialect
        names per call as well, in particular this is helpful for related
        backend names such as ``"mysql", "mariadb"``.

        .. seealso::

            :ref:`change_6980`




    .. change::
        :tags: usecase, sqlite, performance
        :tickets: 7029

        SQLite datetime, date, and time datatypes now use Python standard lib
        ``fromisoformat()`` methods in order to parse incoming datetime, date, and
        time string values. This improves performance vs. the previous regular
        expression-based approach, and also automatically accommodates for datetime
        and time formats that contain either a six-digit "microseconds" format or a
        three-digit "milliseconds" format.

    .. change::
        :tags: usecase, mssql
        :tickets: 7844

        Added support table and column comments on MSSQL when
        creating a table. Added support for reflecting table comments.
        Thanks to Daniel Hall for the help in this pull request.

    .. change::
        :tags: mssql, removed
        :tickets: 7258

        Removed support for the mxodbc driver due to lack of testing support. ODBC
        users may use the pyodbc dialect which is fully supported.

    .. change::
        :tags: mysql, removed
        :tickets: 7258

        Removed support for the OurSQL driver for MySQL and MariaDB, as this
        driver does not seem to be maintained.

    .. change::
        :tags: postgresql, removed
        :tickets: 7258

        Removed support for multiple deprecated drivers:

            - pypostgresql for PostgreSQL. This is available as an
              external driver at https://github.com/PyGreSQL
            - pygresql for PostgreSQL.

        Please switch to one of the supported drivers or to the external
        version of the same driver.

    .. change::
        :tags: bug, engine
        :tickets: 7953

        Fixed issue in :meth:`.Result.columns` method where calling upon
        :meth:`.Result.columns` with a single index could in some cases,
        particularly ORM result object cases, cause the :class:`.Result` to yield
        scalar objects rather than :class:`.Row` objects, as though the
        :meth:`.Result.scalars` method had been called. In SQLAlchemy 1.4, this
        scenario emits a warning that the behavior will change in SQLAlchemy 2.0.

    .. change::
        :tags: usecase, sql
        :tickets: 7759

        Added new parameter :paramref:`.HasCTE.add_cte.nest_here` to
        :meth:`.HasCTE.add_cte` which will "nest" a given :class:`.CTE` at the
        level of the parent statement. This parameter is equivalent to using the
        :paramref:`.HasCTE.cte.nesting` parameter, but may be more intuitive in
        some scenarios as it allows the nesting attribute to be set simultaneously
        along with the explicit level of the CTE.

        The :meth:`.HasCTE.add_cte` method also accepts multiple CTE objects.

    .. change::
        :tags: bug, orm
        :tickets: 7438

        Fixed bug in the behavior of the :paramref:`_orm.Mapper.eager_defaults`
        parameter such that client-side SQL default or onupdate expressions in the
        table definition alone will trigger a fetch operation using RETURNING or
        SELECT when the ORM emits an INSERT or UPDATE for the row. Previously, only
        server side defaults established as part of table DDL and/or server-side
        onupdate expressions would trigger this fetch, even though client-side SQL
        expressions would be included when the fetch was rendered.

    .. change::
        :tags: performance, schema
        :tickets: 4379

        Rearchitected the schema reflection API to allow participating dialects to
        make use of high performing batch queries to reflect the schemas of many
        tables at once using fewer queries by an order of magnitude. The
        new performance features are targeted first at the PostgreSQL and Oracle
        backends, and may be applied to any dialect that makes use of SELECT
        queries against system catalog tables to reflect tables. The change also
        includes new API features and behavioral improvements to the
        :class:`.Inspector` object, including consistent, cached behavior of
        methods like :meth:`.Inspector.has_table`,
        :meth:`.Inspector.get_table_names` and new methods
        :meth:`.Inspector.has_schema` and :meth:`.Inspector.has_index`.

        .. seealso::

            :ref:`change_4379` - full background


    .. change::
        :tags: bug, engine

        Passing a :class:`.DefaultGenerator` object such as a :class:`.Sequence` to
        the :meth:`.Connection.execute` method is deprecated, as this method is
        typed as returning a :class:`.CursorResult` object, and not a plain scalar
        value. The :meth:`.Connection.scalar` method should be used instead, which
        has been reworked with new internal codepaths to suit invoking a SELECT for
        default generation objects without going through the
        :meth:`.Connection.execute` method.

    .. change::
        :tags: usecase, sqlite
        :tickets: 7185

        The SQLite dialect now supports UPDATE..FROM syntax, for UPDATE statements
        that may refer to additional tables within the WHERE criteria of the
        statement without the need to use subqueries. This syntax is invoked
        automatically when using the :class:`_dml.Update` construct when more than
        one table or other entity or selectable is used.

    .. change::
        :tags: general, changed

        The :meth:`_orm.Query.instances` method is deprecated.  The behavioral
        contract of this method, which is that it can iterate objects through
        arbitrary result sets, is long obsolete and no longer tested.
        Arbitrary statements can return objects by using constructs such
        as :meth`.Select.from_statement` or :func:`_orm.aliased`.

    .. change::
        :tags: feature, orm

        Declarative mixins which use :class:`_schema.Column` objects that contain
        :class:`_schema.ForeignKey` references no longer need to use
        :func:`_orm.declared_attr` to achieve this mapping; the
        :class:`_schema.ForeignKey` object is copied along with the
        :class:`_schema.Column` itself when the column is applied to the declared
        mapping.

    .. change::
        :tags: oracle, feature
        :tickets: 6245

        Full "RETURNING" support is implemented for the cx_Oracle dialect, covering
        two individual types of functionality:

        * multi-row RETURNING is implemented, meaning multiple RETURNING rows are
          now received for DML statements that produce more than one row for
          RETURNING.
        * "executemany RETURNING" is also implemented - this allows RETURNING to
          yield row-per statement when ``cursor.executemany()`` is used.
          The implementation of this part of the feature delivers dramatic
          performance improvements to ORM inserts, in the same way as was
          added for psycopg2 in the SQLAlchemy 1.4 change :ref:`change_5263`.


    .. change::
        :tags: oracle

        cx_Oracle 7 is now the minimum version for cx_Oracle.

    .. change::
        :tags: bug, sql
        :tickets: 7551

        Python string values for which a SQL type is determined from the type of
        the value, mainly when using :func:`_sql.literal`, will now apply the
        :class:`_types.String` type, rather than the :class:`_types.Unicode`
        datatype, for Python string values that test as "ascii only" using Python
        ``str.isascii()``. If the string is not ``isascii()``, the
        :class:`_types.Unicode` datatype will be bound instead, which was used in
        all string detection previously. This behavior **only applies to in-place
        detection of datatypes when using ``literal()`` or other contexts that have
        no existing datatype**, which is not usually the case under normal
        :class:`_schema.Column` comparison operations, where the type of the
        :class:`_schema.Column` being compared always takes precedence.

        Use of the :class:`_types.Unicode` datatype can determine literal string
        formatting on backends such as SQL Server, where a literal value (i.e.
        using ``literal_binds``) will be rendered as ``N'<value>'`` instead of
        ``'value'``. For normal bound value handling, the :class:`_types.Unicode`
        datatype also may have implications for passing values to the DBAPI, again
        in the case of SQL Server, the pyodbc driver supports the use of
        :ref:`setinputsizes mode <mssql_pyodbc_setinputsizes>` which will handle
        :class:`_types.String` versus :class:`_types.Unicode` differently.


    .. change::
        :tags: bug, sql
        :tickets: 7083

        The :class:`_functions.array_agg` will now set the array dimensions to 1.
        Improved :class:`_types.ARRAY` processing to accept ``None`` values as
        value of a multi-array.
