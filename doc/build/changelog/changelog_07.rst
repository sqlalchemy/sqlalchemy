
==============
0.7 Changelog
==============

.. changelog::
    :version: 0.7.11

    .. change::
        :tags: bug, engine
        :tickets: 2851
        :versions: 0.8.3, 0.9.0b1

        The regexp used by the :func:`~sqlalchemy.engine.url.make_url` function now parses
        ipv6 addresses, e.g. surrounded by brackets.

    .. change::
        :tags: bug, orm
        :tickets: 2807
        :versions: 0.8.3, 0.9.0b1

        Fixed bug where list instrumentation would fail to represent a
        setslice of ``[0:0]`` correctly, which in particular could occur
        when using ``insert(0, item)`` with the association proxy.  Due
        to some quirk in Python collections, the issue was much more likely
        with Python 3 rather than 2.

    .. change::
        :tags: bug, sql
        :tickets: 2801
        :versions: 0.8.3, 0.9.0b1

        Fixed regression dating back to 0.7.9 whereby the name of a CTE might
        not be properly quoted if it was referred to in multiple FROM clauses.

    .. change::
        :tags: mysql, bug
        :tickets: 2791
        :versions: 0.8.3, 0.9.0b1

        Updates to MySQL reserved words for versions 5.5, 5.6, courtesy
        Hanno Schlichting.

    .. change::
        :tags: sql, bug, cte
        :tickets: 2783
        :versions: 0.8.3, 0.9.0b1

        Fixed bug in common table expression system where if the CTE were
        used only as an ``alias()`` construct, it would not render using the
        WITH keyword.

    .. change::
        :tags: bug, sql
        :tickets: 2784
        :versions: 0.8.3, 0.9.0b1

        Fixed bug in :class:`.CheckConstraint` DDL where the "quote" flag from a
        :class:`.Column` object would not be propagated.

    .. change::
        :tags: bug, orm
        :tickets: 2699
        :versions: 0.8.1

      Fixed bug when a query of the form:
      ``query(SubClass).options(subqueryload(Baseclass.attrname))``,
      where ``SubClass`` is a joined inh of ``BaseClass``,
      would fail to apply the ``JOIN`` inside the subquery
      on the attribute load, producing a cartesian product.
      The populated results still tended to be correct as additional
      rows are just ignored, so this issue may be present as a
      performance degradation in applications that are
      otherwise working correctly.

    .. change::
        :tags: bug, orm
        :tickets: 2689
        :versions: 0.8.1

      Fixed bug in unit of work whereby a joined-inheritance
      subclass could insert the row for the "sub" table
      before the parent table, if the two tables had no
      ForeignKey constraints set up between them.

    .. change::
        :tags: feature, postgresql
        :tickets: 2676
        :versions: 0.8.0

      Added support for PostgreSQL's traditional SUBSTRING
      function syntax, renders as "SUBSTRING(x FROM y FOR z)"
      when regular ``func.substring()`` is used.
      Courtesy Gunnlaugur Þór Briem.

    .. change::
        :tags: bug, tests
        :tickets: 2669
        :pullreq: 41

      Fixed an import of "logging" in test_execute which was not
      working on some linux platforms.

    .. change::
        :tags: bug, orm
        :tickets: 2674

      Improved the error message emitted when a "backref loop" is detected,
      that is when an attribute event triggers a bidirectional
      assignment between two other attributes with no end.
      This condition can occur not just when an object of the wrong
      type is assigned, but also when an attribute is mis-configured
      to backref into an existing backref pair.

    .. change::
      :tags: bug, orm
      :tickets: 2674

      A warning is emitted when a MapperProperty is assigned to a mapper
      that replaces an existing property, if the properties in question
      aren't plain column-based properties.   Replacement of relationship
      properties is rarely (ever?) what is intended and usually refers to a
      mapper mis-configuration.   This will also warn if a backref configures
      itself on top of an existing one in an inheritance relationship
      (which is an error in 0.8).

.. changelog::
    :version: 0.7.10
    :released: Thu Feb 7 2013

    .. change::
        :tags: engine, bug
        :tickets: 2604
        :versions: 0.8.0b2

      Fixed :meth:`.MetaData.reflect` to correctly use
      the given :class:`.Connection`, if given, without
      opening a second connection from that connection's
      :class:`.Engine`.

    .. change::
        :tags: mssql, bug
        :tickets:2607
        :versions: 0.8.0b2

      Fixed bug whereby using "key" with Column
      in conjunction with "schema" for the owning
      Table would fail to locate result rows due
      to the MSSQL dialect's "schema rendering"
      logic's failure to take .key into account.

    .. change::
        :tags: sql, mysql, gae
        :tickets: 2649

        Added a conditional import to the ``gaerdbms`` dialect which attempts
        to import rdbms_apiproxy vs. rdbms_googleapi to work
        on both dev and production platforms.  Also now honors the
        ``instance`` attribute.  Courtesy Sean Lynch.  Also backported
        enhancements to allow username/password as well as
        fixing error code interpretation from 0.8.

    .. change::
        :tags: sql, bug
        :tickets: 2594, 2584

        Backported adjustment to ``__repr__`` for
        :class:`.TypeDecorator` to 0.7, allows :class:`.PickleType`
        to produce a clean ``repr()`` to help with Alembic.

    .. change::
        :tags: sql, bug
        :tickets: 2643

        Fixed bug where :meth:`.Table.tometadata` would fail if a
        :class:`.Column` had both a foreign key as well as an
        alternate ".key" name for the column.

    .. change::
        :tags: mssql, bug
        :tickets: 2638

      Added a Py3K conditional around unnecessary .decode()
      call in mssql information schema, fixes reflection
      in Py3k.

    .. change::
        :tags: orm, bug
        :tickets: 2650

      Fixed potential memory leak which could occur if an
      arbitrary number of :class:`.sessionmaker` objects
      were created.   The anonymous subclass created by
      the sessionmaker, when dereferenced, would not be garbage
      collected due to remaining class-level references from the
      event package.  This issue also applies to any custom system
      that made use of ad-hoc subclasses in conjunction with
      an event dispatcher.

    .. change::
        :tags: orm, bug
        :tickets: 2640

      :meth:`.Query.merge_result` can now load rows from an outer join
      where an entity may be ``None`` without throwing an error.

    .. change::
        :tags: sqlite, bug
        :tickets: 2568
        :versions: 0.8.0b2

      More adjustment to this SQLite related issue which was released in
      0.7.9, to intercept legacy SQLite quoting characters when reflecting
      foreign keys.  In addition to intercepting double quotes, other
      quoting characters such as brackets, backticks, and single quotes
      are now also intercepted.

    .. change::
        :tags: sql, bug
        :tickets: 2631
        :versions: 0.8.0b2

      Fixed bug where using server_onupdate=<FetchedValue|DefaultClause>
      without passing the "for_update=True" flag would apply the default
      object to the server_default, blowing away whatever was there.
      The explicit for_update=True argument shouldn't be needed with this usage
      (especially since the documentation shows an example without it being
      used) so it is now arranged internally using a copy of the given default
      object, if the flag isn't set to what corresponds to that argument.

    .. change::
        :tags: oracle, bug
        :tickets: 2620

      The Oracle LONG type, while an unbounded text type, does not appear
      to use the cx_Oracle.LOB type when result rows are returned,
      so the dialect has been repaired to exclude LONG from
      having cx_Oracle.LOB filtering applied.

    .. change::
        :tags: oracle, bug
        :tickets: 2611

      Repaired the usage of ``.prepare()`` in conjunction with
      cx_Oracle so that a return value of ``False`` will result
      in no call to ``connection.commit()``, hence avoiding
      "no transaction" errors.   Two-phase transactions have
      now been shown to work in a rudimental fashion with
      SQLAlchemy and cx_oracle, however are subject to caveats
      observed with the driver; check the documentation
      for details.

    .. change::
        :tags: orm, bug
        :tickets: 2624

      The :class:`.MutableComposite` type did not allow for the
      :meth:`.MutableBase.coerce` method to be used, even though
      the code seemed to indicate this intent, so this now works
      and a brief example is added.  As a side-effect,
      the mechanics of this event handler have been changed so that
      new :class:`.MutableComposite` types no longer add per-type
      global event handlers.  Also in 0.8.0b2.

    .. change::
        :tags: orm, bug
        :tickets: 2583

      Fixed Session accounting bug whereby replacing
      a deleted object in the identity map with another
      object of the same primary key would raise a
      "conflicting state" error on rollback(),
      if the replaced primary key were established either
      via non-unitofwork-established INSERT statement
      or by primary key switch of another instance.

    .. change::
        :tags: oracle, bug
        :tickets: 2561

      changed the list of cx_oracle types that are
      excluded from the setinputsizes() step to only include
      STRING and UNICODE; CLOB and NCLOB are removed.  This
      is to work around cx_oracle behavior which is broken
      for the executemany() call.  In 0.8, this same change
      is applied however it is also configurable via the
      exclude_setinputsizes argument.

    .. change::
        :tags: feature, mysql
        :tickets: 2523

      Added "raise_on_warnings" flag to OurSQL
      dialect.

    .. change::
        :tags: feature, mysql
        :tickets: 2554

      Added "read_timeout" flag to MySQLdb
      dialect.

.. changelog::
    :version: 0.7.9
    :released: Mon Oct 01 2012

    .. change::
        :tags: orm, bug
        :tickets:

      Fixed bug mostly local to new
      AbstractConcreteBase helper where the "type"
      attribute from the superclass would not
      be overridden on the subclass to produce the
      "reserved for base" error message, instead placing
      a do-nothing attribute there.  This was inconsistent
      vs. using ConcreteBase as well as all the behavior
      of classical concrete mappings, where the "type"
      column from the polymorphic base would be explicitly
      disabled on subclasses, unless overridden
      explicitly.

    .. change::
        :tags: orm, bug
        :tickets:

      A warning is emitted when lazy='dynamic'
      is combined with uselist=False.  This is an
      exception raise in 0.8.

    .. change::
        :tags: orm, bug
        :tickets:

      Fixed bug whereby user error in related-object
      assignment could cause recursion overflow if the
      assignment triggered a backref of the same name
      as a bi-directional attribute on the incorrect
      class to the same target.  An informative
      error is raised now.

    .. change::
        :tags: orm, bug
        :tickets: 2539

      Fixed bug where incorrect type information
      would be passed when the ORM would bind the
      "version" column, when using the "version" feature.
      Tests courtesy Daniel Miller.

    .. change::
        :tags: orm, bug
        :tickets: 2566

      Extra logic has been added to the "flush"
      that occurs within Session.commit(), such that the
      extra state added by an after_flush() or
      after_flush_postexec() hook is also flushed in a
      subsequent flush, before the "commit" completes.
      Subsequent calls to flush() will continue until
      the after_flush hooks stop adding new state.
      An "overflow" counter of 100 is also in place,
      in the event of a broken after_flush() hook
      adding new content each time.

    .. change::
        :tags: bug, sql
        :tickets: 2571

      Fixed the DropIndex construct to support
      an Index associated with a Table in a remote
      schema.

    .. change::
        :tags: bug, sql
        :tickets: 2574

      Fixed bug in over() construct whereby
      passing an empty list for either partition_by
      or order_by, as opposed to None, would fail
      to generate correctly.
      Courtesy Gunnlaugur Þór Briem.

    .. change::
        :tags: bug, sql
        :tickets: 2521

      Fixed CTE bug whereby positional
      bound parameters present in the CTEs themselves
      would corrupt the overall ordering of
      bound parameters.  This primarily
      affected SQL Server as the platform with
      positional binds + CTE support.

    .. change::
        :tags: bug, sql
        :tickets:

      Fixed more un-intuitivenesses in CTEs
      which prevented referring to a CTE in a union
      of itself without it being aliased.
      CTEs now render uniquely
      on name, rendering the outermost CTE of a given
      name only - all other references are rendered
      just as the name.   This even includes other
      CTE/SELECTs that refer to different versions
      of the same CTE object, such as a SELECT
      or a UNION ALL of that SELECT. We are
      somewhat loosening the usual link between object
      identity and lexical identity in this case.
      A true name conflict between two unrelated
      CTEs now raises an error.

    .. change::
        :tags: bug, sql
        :tickets: 2512

      quoting is applied to the column names
      inside the WITH RECURSIVE clause of a
      common table expression according to the
      quoting rules for the originating Column.

    .. change::
        :tags: bug, sql
        :tickets: 2518

      Fixed regression introduced in 0.7.6
      whereby the FROM list of a SELECT statement
      could be incorrect in certain "clone+replace"
      scenarios.

    .. change::
        :tags: bug, sql
        :tickets: 2552

      Fixed bug whereby usage of a UNION
      or similar inside of an embedded subquery
      would interfere with result-column targeting,
      in the case that a result-column had the same
      ultimate name as a name inside the embedded
      UNION.

    .. change::
        :tags: bug, sql
        :tickets: 2558

      Fixed a regression since 0.6 regarding
      result-row targeting.   It should be possible
      to use a select() statement with string
      based columns in it, that is
      select(['id', 'name']).select_from('mytable'),
      and have this statement be targetable by
      Column objects with those names; this is the
      mechanism by which
      query(MyClass).from_statement(some_statement)
      works.  At some point the specific case of
      using select(['id']), which is equivalent to
      select([literal_column('id')]), stopped working
      here, so this has been re-instated and of
      course tested.

    .. change::
        :tags: bug, sql
        :tickets: 2544

      Added missing operators is_(), isnot()
      to the ColumnOperators base, so that these long-available
      operators are present as methods like all
      the other operators.

    .. change::
        :tags: engine, bug
        :tickets: 2522

      Fixed bug whereby
      a disconnect detect + dispose that occurs
      when the QueuePool has threads waiting
      for connections would leave those
      threads waiting for the duration of
      the timeout on the old pool (or indefinitely
      if timeout was disabled).  The fix
      now notifies those waiters with a special
      exception case and has them move onto
      the new pool.

    .. change::
        :tags: engine, feature
        :tickets: 2516

      Dramatic improvement in memory
      usage of the event system; instance-level
      collections are no longer created for a
      particular type of event until
      instance-level listeners are established
      for that event.

    .. change::
        :tags: engine, bug
        :tickets: 2529

      Added gaerdbms import to mysql/__init__.py,
      the absence of which was preventing the new
      GAE dialect from being loaded.

    .. change::
        :tags: engine, bug
        :tickets: 2553

      Fixed cextension bug whereby the
      "ambiguous column error" would fail to
      function properly if the given index were
      a Column object and not a string.
      Note there are still some column-targeting
      issues here which are fixed in 0.8.

    .. change::
        :tags: engine, bug
        :tickets:

      Fixed the repr() of Enum to include
      the "name" and "native_enum" flags.  Helps
      Alembic autogenerate.

    .. change::
        :tags: sqlite, bug
        :tickets: 2568

      Adjusted a very old bugfix which attempted
      to work around a SQLite issue that itself was
      "fixed" as of sqlite 3.6.14, regarding quotes
      surrounding a table name when using
      the "foreign_key_list" pragma.  The fix has been
      adjusted to not interfere with quotes that
      are *actually in the name* of a column or table,
      to as much a degree as possible; sqlite still
      doesn't return the correct result for foreign_key_list()
      if the target table actually has quotes surrounding
      its name, as *part* of its name (i.e. """mytable""").

    .. change::
        :tags: sqlite, bug
        :tickets: 2265

      Adjusted column default reflection code to
      convert non-string values to string, to accommodate
      old SQLite versions that don't deliver
      default info as a string.

    .. change::
        :tags: sqlite, feature
        :tickets:

      Added support for the localtimestamp()
      SQL function implemented in SQLite, courtesy
      Richard Mitchell.

    .. change::
        :tags: postgresql, bug
        :tickets: 2531

      Columns in reflected primary key constraint
      are now returned in the order in which the constraint
      itself defines them, rather than how the table
      orders them.  Courtesy Gunnlaugur Þór Briem..

    .. change::
        :tags: postgresql, bug
        :tickets: 2570

      Added 'terminating connection' to the list
      of messages we use to detect a disconnect with PG, which
      appears to be present in some versions when the server
      is restarted.

    .. change::
        :tags: bug, mysql
        :tickets:

      Updated mysqlconnector interface to use
      updated "client flag" and "charset" APIs,
      courtesy David McNelis.

    .. change::
        :tags: mssql, bug
        :tickets: 2538

      Fixed compiler bug whereby using a correlated
      subquery within an ORDER BY would fail to render correctly
      if the stament also used LIMIT/OFFSET, due to mis-rendering
      within the ROW_NUMBER() OVER clause.  Fix courtesy
      sayap

    .. change::
        :tags: mssql, bug
        :tickets: 2545

      Fixed compiler bug whereby a given
      select() would be modified if it had an "offset"
      attribute, causing the construct to not compile
      correctly a second time.

    .. change::
        :tags: mssql, bug
        :tickets:

      Fixed bug where reflection of primary key constraint
      would double up columns if the same constraint/table
      existed in multiple schemas.

.. changelog::
    :version: 0.7.8
    :released: Sat Jun 16 2012

    .. change::
        :tags: orm, bug
        :tickets: 2480

      Fixed bug whereby subqueryload() from
      a polymorphic mapping to a target would incur
      a new invocation of the query for each
      distinct class encountered in the polymorphic
      result.

    .. change::
        :tags: orm, bug
        :tickets: 2491, 1892

      Fixed bug in declarative
      whereby the precedence of columns
      in a joined-table, composite
      column (typically for id) would fail to
      be correct if the columns contained
      names distinct from their attribute
      names.  This would cause things like
      primaryjoin conditions made against the
      entity attributes to be incorrect.  Related
      to as this was supposed
      to be part of that, this is.

    .. change::
        :tags: orm, feature
        :tickets:

      The 'objects' argument to
      flush() is no longer deprecated, as some
      valid use cases have been identified.

    .. change::
        :tags: orm, bug
        :tickets: 2508

      Fixed identity_key() function which
      was not accepting a scalar argument
      for the identity. .

    .. change::
        :tags: orm, bug
        :tickets: 2497

      Fixed bug whereby populate_existing
      option would not propagate to subquery
      eager loaders. .

    .. change::
        :tags: bug, sql
        :tickets: 2499

      added BIGINT to types.__all__,
      BIGINT, BINARY, VARBINARY to sqlalchemy
      module namespace, plus test to ensure
      this breakage doesn't occur again.

    .. change::
        :tags: bug, sql
        :tickets: 2490

      Repaired common table expression
      rendering to function correctly when the
      SELECT statement contains UNION or other
      compound expressions, courtesy btbuilder.

    .. change::
        :tags: bug, sql
        :tickets: 2482

      Fixed bug whereby append_column()
      wouldn't function correctly on a cloned
      select() construct, courtesy
      Gunnlaugur Þór Briem.

    .. change::
        :tags: engine, bug
        :tickets: 2489

      Fixed memory leak in C version of
      result proxy whereby DBAPIs which don't deliver
      pure Python tuples for result rows would
      fail to decrement refcounts correctly.
      The most prominently affected DBAPI
      is pyodbc.

    .. change::
        :tags: engine, bug
        :tickets: 2503

      Fixed bug affecting Py3K whereby
      string positional parameters passed to
      engine/connection execute() would fail to be
      interpreted correctly, due to __iter__
      being present on Py3K string..

    .. change::
        :tags: postgresql, bug
        :tickets: 2510

      removed unnecessary table clause when
      reflecting enums,.  Courtesy
      Gunnlaugur Þór Briem.

    .. change::
        :tags: oracle, bug
        :tickets: 2483

      Added ROWID to oracle.*.

    .. change::
        :tags: feature, mysql
        :tickets: 2484

      Added a new dialect for Google App
      Engine.  Courtesy Richie Foreman.

.. changelog::
    :version: 0.7.7
    :released: Sat May 05 2012

    .. change::
        :tags: orm, bug
        :tickets: 2477

      Fixed issue in unit of work
      whereby setting a non-None self-referential
      many-to-one relationship to None
      would fail to persist the change if the
      former value was not already loaded..

    .. change::
        :tags: orm, feature
        :tickets: 2443

      Added prefix_with() method
      to Query, calls upon select().prefix_with()
      to allow placement of MySQL SELECT
      directives in statements.  Courtesy
      Diana Clarke

    .. change::
        :tags: orm, bug
        :tickets: 2409

      Fixed bug in 0.7.6 introduced by whereby column_mapped_collection
      used against columns that were mapped as
      joins or other indirect selectables
      would fail to function.

    .. change::
        :tags: orm, feature
        :tickets:

      Added new flag to @validates
      include_removes.  When True, collection
      remove and attribute del events
      will also be sent to the validation function,
      which accepts an additional argument
      "is_remove" when this flag is used.

    .. change::
        :tags: orm, bug
        :tickets: 2449

      Fixed bug whereby polymorphic_on
      column that's not otherwise mapped on the
      class would be incorrectly included
      in a merge() operation, raising an error.

    .. change::
        :tags: orm, bug
        :tickets: 2453

      Fixed bug in expression annotation
      mechanics which could lead to incorrect
      rendering of SELECT statements with aliases
      and joins, particularly when using
      column_property().

    .. change::
        :tags: orm, bug
        :tickets: 2454

      Fixed bug which would prevent
      OrderingList from being pickleable.  Courtesy Jeff Dairiki

    .. change::
        :tags: orm, bug
        :tickets:

      Fixed bug in relationship comparisons
      whereby calling unimplemented methods like
      SomeClass.somerelationship.like() would
      produce a recursion overflow, instead
      of NotImplementedError.

    .. change::
        :tags: bug, sql
        :tickets:

      Removed warning when Index is created
      with no columns; while this might not be what
      the user intended, it is a valid use case
      as an Index could be a placeholder for just an
      index of a certain name.

    .. change::
        :tags: feature, sql
        :tickets:

      Added new connection event
      dbapi_error(). Is called for all DBAPI-level
      errors passing the original DBAPI exception
      before SQLAlchemy modifies the state
      of the cursor.

    .. change::
        :tags: bug, sql
        :tickets:

      If conn.begin() fails when calling
      "with engine.begin()", the newly acquired
      Connection is closed explicitly before
      propagating the exception onward normally.

    .. change::
        :tags: bug, sql
        :tickets: 2474

      Add BINARY, VARBINARY to types.__all__.

    .. change::
        :tags: mssql, feature
        :tickets:

      Added interim create_engine flag
      supports_unicode_binds to PyODBC dialect,
      to force whether or not the dialect
      passes Python unicode literals to PyODBC
      or not.

    .. change::
        :tags: mssql, bug
        :tickets:

      Repaired the use_scope_identity
      create_engine() flag when using the pyodbc
      dialect.  Previously this flag would be
      ignored if set to False.  When set to False,
      you'll get "SELECT @@identity" after each
      INSERT to get at the last inserted ID,
      for those tables which have "implicit_returning"
      set to False.

    .. change::
        :tags: mssql, bug
        :tickets: 2468

      UPDATE..FROM syntax with SQL Server
      requires that the updated table be present
      in the FROM clause when an alias of that
      table is also present in the FROM clause.
      The updated table is now always present
      in the FROM, when FROM is present
      in the first place.  Courtesy sayap.

    .. change::
        :tags: postgresql, feature
        :tickets: 2445

      Added new for_update/with_lockmode()
      options for PostgreSQL: for_update="read"/
      with_lockmode("read"),
      for_update="read_nowait"/
      with_lockmode("read_nowait").
      These emit "FOR SHARE" and "FOR SHARE NOWAIT",
      respectively.  Courtesy Diana Clarke

    .. change::
        :tags: postgresql, bug
        :tickets: 2473

      removed unnecessary table clause
      when reflecting domains.

    .. change::
        :tags: bug, mysql
        :tickets: 2460

      Fixed bug whereby column name inside
      of "KEY" clause for autoincrement composite
      column with InnoDB would double quote a
      name that's a reserved word.  Courtesy Jeff
      Dairiki.

    .. change::
        :tags: bug, mysql
        :tickets:

      Fixed bug whereby get_view_names() for
      "information_schema" schema would fail
      to retrieve views marked as "SYSTEM VIEW".
      courtesy Matthew Turland.

    .. change::
        :tags: bug, mysql
        :tickets: 2467

      Fixed bug whereby if cast() is used
      on a SQL expression whose type is not supported
      by cast() and therefore CAST isn't rendered by
      the dialect, the order of evaluation could change
      if the casted expression required that it be
      grouped; grouping is now applied to those
      expressions.

    .. change::
        :tags: sqlite, feature
        :tickets: 2475

      Added SQLite execution option
      "sqlite_raw_colnames=True", will bypass
      attempts to remove "." from column names
      returned by SQLite cursor.description.

    .. change::
        :tags: sqlite, bug
        :tickets: 2525

      When the primary key column of a Table
      is replaced, such as via extend_existing,
      the "auto increment" column used by insert()
      constructs is reset.  Previously it would
      remain referring to the previous primary
      key column.

.. changelog::
    :version: 0.7.6
    :released: Wed Mar 14 2012

    .. change::
        :tags: orm, bug
        :tickets: 2424

      Fixed event registration bug
      which would primarily show up as
      events not being registered with
      sessionmaker() instances created
      after the event was associated
      with the Session class.

    .. change::
        :tags: orm, bug
        :tickets: 2425

      Fixed bug whereby a primaryjoin
      condition with a "literal" in it would
      raise an error on compile with certain
      kinds of deeply nested expressions
      which also needed to render the same
      bound parameter name more than once.

    .. change::
        :tags: orm, feature
        :tickets:

      Added "no_autoflush" context
      manager to Session, used with with:
      will temporarily disable autoflush.

    .. change::
        :tags: orm, feature
        :tickets: 1859

      Added cte() method to Query,
      invokes common table expression support
      from the Core (see below).

    .. change::
        :tags: orm, bug
        :tickets: 2403

      Removed the check for number of
      rows affected when doing a multi-delete
      against mapped objects.   If an ON DELETE
      CASCADE exists between two rows, we can't
      get an accurate rowcount from the DBAPI;
      this particular count is not supported
      on most DBAPIs in any case, MySQLdb
      is the notable case where it is.

    .. change::
        :tags: orm, bug
        :tickets: 2409

      Fixed bug whereby objects using
      attribute_mapped_collection or
      column_mapped_collection could not be
      pickled.

    .. change::
        :tags: orm, bug
        :tickets: 2406

      Fixed bug whereby MappedCollection
      would not get the appropriate collection
      instrumentation if it were only used
      in a custom subclass that used
      @collection.internally_instrumented.

    .. change::
        :tags: orm, bug
        :tickets: 2419

      Fixed bug whereby SQL adaption mechanics
      would fail in a very nested scenario involving
      joined-inheritance, joinedload(), limit(), and a
      derived function in the columns clause.

    .. change::
        :tags: orm, bug
        :tickets: 2417

      Fixed the repr() for CascadeOptions to
      include refresh-expire.  Also reworked
      CascadeOptions to be a <frozenset>.

    .. change::
        :tags: orm, feature
        :tickets: 2400

      Added the ability to query for
      Table-bound column names when using
      query(sometable).filter_by(colname=value).

    .. change::
        :tags: orm, bug
        :tickets:

      Improved the "declarative reflection"
      example to support single-table inheritance,
      multiple calls to prepare(), tables that
      are present in alternate schemas,
      establishing only a subset of classes
      as reflected.

    .. change::
        :tags: orm, bug
        :tickets: 2390

      Scaled back the test applied within
      flush() to check for UPDATE against partially
      NULL PK within one table to only actually
      happen if there's really an UPDATE to occur.

    .. change::
        :tags: orm, bug
        :tickets: 2352

      Fixed bug whereby if a method name
      conflicted with a column name, a
      TypeError would be raised when the mapper
      tried to inspect the __get__() method
      on the method object.

    .. change::
        :tags: bug, sql
        :tickets: 2427

      Fixed memory leak in core which would
      occur when C extensions were used with
      particular types of result fetches,
      in particular when orm query.count()
      were called.

    .. change::
        :tags: bug, sql
        :tickets: 2398

      Fixed issue whereby attribute-based
      column access on a row would raise
      AttributeError with non-C version,
      NoSuchColumnError with C version.  Now
      raises AttributeError in both cases.

    .. change::
        :tags: feature, sql
        :tickets: 1859

      Added support for SQL standard
      common table expressions (CTE), allowing
      SELECT objects as the CTE source (DML
      not yet supported).  This is invoked via
      the cte() method on any select() construct.

    .. change::
        :tags: bug, sql
        :tickets: 2392

      Added support for using the .key
      of a Column as a string identifier in a
      result set row.   The .key is currently
      listed as an "alternate" name for a column,
      and is superseded by the name of a column
      which has that key value as its regular name.
      For the next major release
      of SQLAlchemy we may reverse this precedence
      so that .key takes precedence, but this
      is not decided on yet.

    .. change::
        :tags: bug, sql
        :tickets: 2413

      A warning is emitted when a not-present
      column is stated in the values() clause
      of an insert() or update() construct.
      Will move to an exception in 0.8.

    .. change::
        :tags: bug, sql
        :tickets: 2396

      A significant change to how labeling
      is applied to columns in SELECT statements
      allows "truncated" labels, that is label names
      that are generated in Python which exceed
      the maximum identifier length (note this is
      configurable via label_length on create_engine()),
      to be properly referenced when rendered inside
      of a subquery, as well as to be present
      in a result set row using their original
      in-Python names.

    .. change::
        :tags: bug, sql
        :tickets: 2402

      Fixed bug in new "autoload_replace" flag
      which would fail to preserve the primary
      key constraint of the reflected table.

    .. change::
        :tags: bug, sql
        :tickets: 2380

      Index will raise when arguments passed
      cannot be interpreted as columns or expressions.
      Will warn when Index is created
      with no columns at all.

    .. change::
        :tags: engine, feature
        :tickets: 2407

      Added "no_parameters=True" execution
      option for connections.   If no parameters
      are present, will pass the statement
      as cursor.execute(statement), thereby invoking
      the DBAPIs behavior when no parameter collection
      is present; for psycopg2 and mysql-python, this
      means not interpreting % signs in the string.
      This only occurs with this option, and not
      just if the param list is blank, as otherwise
      this would produce inconsistent behavior
      of SQL expressions that normally escape percent
      signs (and while compiling, can't know ahead of
      time if parameters will be present in
      some cases).

    .. change::
        :tags: engine, bug
        :tickets:

      Added execution_options() call to
      MockConnection (i.e., that used with
      strategy="mock") which acts as a pass through
      for arguments.

    .. change::
        :tags: engine, feature
        :tickets: 2378

      Added pool_reset_on_return argument
      to create_engine, allows control over
      "connection return" behavior.  Also added
      new arguments 'rollback', 'commit', None
      to pool.reset_on_return to allow more control
      over connection return activity.

    .. change::
        :tags: engine, feature
        :tickets:

      Added some decent context managers
      to Engine, Connection::

          with engine.begin() as conn:
              <work with conn in a transaction>

      and::

          with engine.connect() as conn:
              <work with conn>

      Both close out the connection when done,
      commit or rollback transaction with errors
      on engine.begin().

    .. change::
        :tags: sqlite, bug
        :tickets: 2432

      Fixed bug in C extensions whereby
      string format would not be applied to a
      Numeric value returned as integer; this
      affected primarily SQLite which does
      not maintain numeric scale settings.

    .. change::
        :tags: mssql, feature
        :tickets: 2430

      Added support for MSSQL INSERT,
      UPDATE, and DELETE table hints, using
      new with_hint() method on UpdateBase.

    .. change::
        :tags: feature, mysql
        :tickets: 2386

      Added support for MySQL index and
      primary key constraint types
      (i.e. USING) via new mysql_using parameter
      to Index and PrimaryKeyConstraint,
      courtesy Diana Clarke.

    .. change::
        :tags: feature, mysql
        :tickets: 2394

      Added support for the "isolation_level"
      parameter to all MySQL dialects.  Thanks
      to mu_mind for the patch here.

    .. change::
        :tags: oracle, feature
        :tickets: 2399

      Added a new create_engine() flag
      coerce_to_decimal=False, disables the precision
      numeric handling which can add lots of overhead
      by converting all numeric values to
      Decimal.

    .. change::
        :tags: oracle, bug
        :tickets: 2401

      Added missing compilation support for
      LONG

    .. change::
        :tags: oracle, bug
        :tickets: 2435

      Added 'LEVEL' to the list of reserved
      words for Oracle.

    .. change::
        :tags: examples, bug
        :tickets:

      Altered _params_from_query() function
      in Beaker example to pull bindparams from the
      fully compiled statement, as a quick means
      to get everything including subqueries in the
      columns clause, etc.

.. changelog::
    :version: 0.7.5
    :released: Sat Jan 28 2012

    .. change::
        :tags: orm, bug
        :tickets: 2389

      Fixed issue where modified session state
      established after a failed flush would be committed
      as part of the subsequent transaction that
      begins automatically after manual call
      to rollback().   The state of the session is
      checked within rollback(), and if new state
      is present, a warning is emitted and
      restore_snapshot() is called a second time,
      discarding those changes.

    .. change::
        :tags: orm, bug
        :tickets: 2345

      Fixed regression from 0.7.4 whereby
      using an already instrumented column from a
      superclass as "polymorphic_on" failed to resolve
      the underlying Column.

    .. change::
        :tags: orm, bug
        :tickets: 2370

      Raise an exception if xyzload_all() is
      used inappropriately with two non-connected
      relationships.

    .. change::
        :tags: orm, feature
        :tickets:

      Added "class_registry" argument to
      declarative_base().  Allows two or more declarative
      bases to share the same registry of class names.

    .. change::
        :tags: orm, feature
        :tickets:

      query.filter() accepts multiple
      criteria which will join via AND, i.e.
      query.filter(x==y, z>q, ...)

    .. change::
        :tags: orm, feature
        :tickets: 2351

      Added new capability to relationship
      loader options to allow "default" loader strategies.
      Pass '*' to any of joinedload(), lazyload(),
      subqueryload(), or noload() and that becomes the
      loader strategy used for all relationships,
      except for those explicitly stated in the
      Query.  Thanks to up-and-coming contributor
      Kent Bower for an exhaustive and well
      written test suite !

    .. change::
        :tags: orm, bug
        :tickets: 2367

      Fixed bug whereby event.listen(SomeClass)
      forced an entirely unnecessary compile of the
      mapper, making events very hard to set up
      at module import time (nobody noticed this ??)

    .. change::
        :tags: orm, bug
        :tickets:

      Fixed bug whereby hybrid_property didn't
      work as a kw arg in any(), has().

    .. change::
        :tags: orm
        :tickets:

      Fixed regression from 0.6 whereby if
      "load_on_pending" relationship() flag were used
      where a non-"get()" lazy clause needed to be
      emitted on a pending object, it would fail
      to load.

    .. change::
        :tags: orm, bug
        :tickets: 2371

      ensure pickleability of all ORM exceptions
      for multiprocessing compatibility.

    .. change::
        :tags: orm, bug
        :tickets: 2353

      implemented standard "can't set attribute" /
      "can't delete attribute" AttributeError when
      setattr/delattr used on a hybrid that doesn't
      define fset or fdel.

    .. change::
        :tags: orm, bug
        :tickets: 2362

      Fixed bug where unpickled object didn't
      have enough of its state set up to work
      correctly within the unpickle() event established
      by the mutable object extension, if the object
      needed ORM attribute access within
      __eq__() or similar.

    .. change::
        :tags: orm, bug
        :tickets: 2374

      Fixed bug where "merge" cascade could
      mis-interpret an unloaded attribute, if the
      load_on_pending flag were used with
      relationship().  Thanks to Kent Bower
      for tests.

    .. change::
        :tags: orm, feature
        :tickets: 2356

      New declarative reflection example
      added, illustrates how best to mix table reflection
      with declarative as well as uses some new features
      from.

    .. change::
        :tags: feature, sql
        :tickets: 2356

      New reflection feature "autoload_replace";
      when set to False on Table, the Table can be autoloaded
      without existing columns being replaced.  Allows
      more flexible chains of Table construction/reflection
      to be constructed, including that it helps with
      combining Declarative with table reflection.
      See the new example on the wiki.

    .. change::
        :tags: bug, sql
        :tickets: 2356

      Improved the API for add_column() such that
      if the same column is added to its own table,
      an error is not raised and the constraints
      don't get doubled up.  Also helps with some
      reflection/declarative patterns.

    .. change::
        :tags: feature, sql
        :tickets:

      Added "false()" and "true()" expression
      constructs to sqlalchemy.sql namespace, though
      not part of __all__ as of yet.

    .. change::
        :tags: feature, sql
        :tickets: 2361

      Dialect-specific compilers now raise
      CompileError for all type/statement compilation
      issues, instead of InvalidRequestError or ArgumentError.
      The DDL for CREATE TABLE will re-raise
      CompileError to include table/column information
      for the problematic column.

    .. change::
        :tags: bug, sql
        :tickets: 2381

      Fixed issue where the "required" exception
      would not be raised for bindparam() with required=True,
      if the statement were given no parameters at all.

    .. change::
        :tags: engine, bug
        :tickets: 2371

      Added __reduce__ to StatementError,
      DBAPIError, column errors so that exceptions
      are pickleable, as when using multiprocessing.
      However, not
      all DBAPIs support this yet, such as
      psycopg2.

    .. change::
        :tags: engine, bug
        :tickets: 2382

      Improved error messages when a non-string
      or invalid string is passed to any of the
      date/time processors used by SQLite, including
      C and Python versions.

    .. change::
        :tags: engine, bug
        :tickets: 2377

      Fixed bug whereby a table-bound Column
      object named "<a>_<b>" which matched a column
      labeled as "<tablename>_<colname>" could match
      inappropriately when targeting in a result
      set row.

    .. change::
        :tags: engine, bug
        :tickets: 2384

      Fixed bug in "mock" strategy whereby
      correct DDL visit method wasn't called, resulting
      in "CREATE/DROP SEQUENCE" statements being
      duplicated

    .. change::
        :tags: sqlite, bug
        :tickets: 2364

      the "name" of an FK constraint in SQLite
      is reflected as "None", not "0" or other
      integer value.
      SQLite does not appear to support constraint
      naming in any case.

    .. change::
        :tags: sqlite, bug
        :tickets: 2368

      sql.false() and sql.true() compile to
      0 and 1, respectively in sqlite

    .. change::
        :tags: sqlite, bug
        :tickets:

      removed an erroneous "raise" in the
      SQLite dialect when getting table names
      and view names, where logic is in place
      to fall back to an older version of
      SQLite that doesn't have the
      "sqlite_temp_master" table.

    .. change::
        :tags: bug, mysql
        :tickets: 2376

      fixed regexp that filters out warnings
      for non-reflected "PARTITION" directives,
      thanks to George Reilly

    .. change::
        :tags: mssql, bug
        :tickets: 2340

      Adjusted the regexp used in the
      mssql.TIME type to ensure only six digits
      are received for the "microseconds" portion
      of the value, which is expected by
      Python's datetime.time().  Note that
      support for sending microseconds doesn't
      seem to be possible yet with pyodbc
      at least.

    .. change::
        :tags: mssql, bug
        :tickets: 2347

      Dropped the "30 char" limit on pymssql,
      based on reports that it's doing things
      better these days.  pymssql hasn't been
      well tested and as the DBAPI is in flux
      it's still not clear what the status
      is on this driver and how SQLAlchemy's
      implementation should adapt.

    .. change::
        :tags: oracle, bug
        :tickets: 2388

      Added ORA-03135 to the never ending
      list of oracle "connection lost" errors

    .. change::
        :tags: core, bug
        :tickets: 2379

      Changed LRUCache, used by the mapper
      to cache INSERT/UPDATE/DELETE statements,
      to use an incrementing counter instead
      of a timestamp to track entries, for greater
      reliability versus using time.time(), which
      can cause test failures on some platforms.

    .. change::
        :tags: core, bug
        :tickets: 2383

      Added a boolean check for the "finalize"
      function within the pool connection proxy's
      weakref callback before calling it, so that a
      warning isn't emitted that this function is None
      when the application is exiting and gc has
      removed the function from the module before the
      weakref callback was invoked.

    .. change::
        :tags: bug, py3k
        :tickets: 2348

      Fixed inappropriate usage of util.py3k
      flag and renamed it to util.py3k_warning, since
      this flag is intended to detect the -3 flag
      series of import restrictions only.

    .. change::
        :tags: examples, feature
        :tickets: 2313

      Simplified the versioning example
      a bit to use a declarative mixin as well
      as an event listener, instead of a metaclass +
      SessionExtension.

    .. change::
        :tags: examples, bug
        :tickets: 2346

      Fixed large_collection.py to close the
      session before dropping tables.

.. changelog::
    :version: 0.7.4
    :released: Fri Dec 09 2011

    .. change::
        :tags: orm, bug
        :tickets: 2315

      Fixed backref behavior when "popping" the
      value off of a many-to-one in response to
      a removal from a stale one-to-many - the operation
      is skipped, since the many-to-one has since
      been updated.

    .. change::
        :tags: orm, bug
        :tickets: 2264

      After some years of not doing this, added
      more granularity to the "is X a parent of Y"
      functionality, which is used when determining
      if the FK on "Y" needs to be "nulled out" as well
      as if "Y" should be deleted with delete-orphan
      cascade.   The test now takes into account the
      Python identity of the parent as well its identity
      key, to see if the last known parent of Y is
      definitely X.   If a decision
      can't be made, a StaleDataError is raised.  The
      conditions where this error is raised are fairly
      rare, requiring that the previous parent was
      garbage collected, and previously
      could very well inappropriately update/delete
      a record that's since moved onto a new parent,
      though there may be some cases where
      "silent success" occurred previously that will now
      raise in the face of ambiguity.
      Expiring "Y" resets the "parent" tracker, meaning
      X.remove(Y) could then end up deleting Y even
      if X is stale, but this is the same behavior
      as before; it's advised to expire X also in that
      case.

    .. change::
        :tags: orm, bug
        :tickets: 2310

      fixed inappropriate evaluation of user-mapped
      object in a boolean context within query.get().  Also in 0.6.9.

    .. change::
        :tags: orm, bug
        :tickets: 2304

      Added missing comma to PASSIVE_RETURN_NEVER_SET
      symbol

    .. change::
        :tags: orm, bug
        :tickets: 1776

      Cls.column.collate("some collation") now
      works.   Also in 0.6.9

    .. change::
        :tags: orm, bug
        :tickets: 2309

      the value of a composite attribute is now
      expired after an insert or update operation, instead
      of regenerated in place.  This ensures that a
      column value which is expired within a flush
      will be loaded first, before the composite
      is regenerated using that value.

    .. change::
        :tags: orm, bug
        :tickets: 2309, 2308

      The fix in also emits the
      "refresh" event when the composite value is
      loaded on access, even if all column
      values were already present, as is appropriate.
      This fixes the "mutable" extension which relies
      upon the "load" event to ensure the _parents
      dictionary is up to date, fixes.
      Thanks to Scott Torborg for the test case here.

    .. change::
        :tags: orm, bug
        :tickets: 2312

      Fixed bug whereby a subclass of a subclass
      using concrete inheritance in conjunction with
      the new ConcreteBase or AbstractConcreteBase
      would fail to apply the subclasses deeper than
      one level to the "polymorphic loader" of each
      base

    .. change::
        :tags: orm, bug
        :tickets: 2312

      Fixed bug whereby a subclass of a subclass
      using the new AbstractConcreteBase would fail
      to acquire the correct "base_mapper" attribute
      when the "base" mapper was generated, thereby
      causing failures later on.

    .. change::
        :tags: orm, bug
        :tickets: 2316

      Fixed bug whereby column_property() created
      against ORM-level column could be treated as
      a distinct entity when producing certain
      kinds of joined-inh joins.

    .. change::
        :tags: orm, bug
        :tickets: 2297

      Fixed the error formatting raised when
      a tuple is inadvertently passed to session.query().  Also in 0.6.9.

    .. change::
        :tags: orm, bug
        :tickets: 2328

      Calls to query.join() to a single-table
      inheritance subclass are now tracked, and
      are used to eliminate the additional WHERE..
      IN criterion normally tacked on with single
      table inheritance, since the join should
      accommodate it.  This allows OUTER JOIN
      to a single table subclass to produce
      the correct results, and overall will produce
      fewer WHERE criterion when dealing with
      single table inheritance joins.

    .. change::
        :tags: orm, bug
        :tickets: 2339

      __table_args__ can now be passed as
      an empty tuple as well as an empty dict..  Thanks to Fayaz Yusuf Khan
      for the patch.

    .. change::
        :tags: orm, bug
        :tickets: 2325

      Updated warning message when setting
      delete-orphan without delete to no longer
      refer to 0.6, as we never got around to
      upgrading this to an exception.  Ideally
      this might be better as an exception but
      it's not critical either way.

    .. change::
        :tags: orm, feature
        :tickets: 2345, 2238

      polymorphic_on now accepts many
      new kinds of values:

        * standalone expressions that aren't
          otherwise mapped
        * column_property() objects
        * string names of any column_property()
          or attribute name of a mapped Column

      The docs include an example using
      the case() construct, which is likely to be
      a common constructed used here. and part of

      Standalone expressions in polymorphic_on
      propagate to single-table inheritance
      subclasses so that they are used in the
      WHERE /JOIN clause to limit rows to that
      subclass as is the usual behavior.

    .. change::
        :tags: orm, feature
        :tickets: 2301

      IdentitySet supports the - operator
      as the same as difference(), handy when dealing
      with Session.dirty etc.

    .. change::
        :tags: orm, feature
        :tickets:

      Added new value for Column autoincrement
      called "ignore_fk", can be used to force autoincrement
      on a column that's still part of a ForeignKeyConstraint.
      New example in the relationship docs illustrates
      its use.

    .. change::
        :tags: orm, bug
        :tickets:

      Fixed bug in get_history() when referring
      to a composite attribute that has no value;
      added coverage for get_history() regarding
      composites which is otherwise just a userland
      function.

    .. change::
        :tags: bug, sql
        :tickets: 2316, 2261

      related to, made some
      adjustments to the change from
      regarding the "from" list on a select(). The
      _froms collection is no longer memoized, as this
      simplifies various use cases and removes the
      need for a "warning" if a column is attached
      to a table after it was already used in an
      expression - the select() construct will now
      always produce the correct expression.
      There's probably no real-world
      performance hit here; select() objects are
      almost always made ad-hoc, and systems that
      wish to optimize the re-use of a select()
      would be using the "compiled_cache" feature.
      A hit which would occur when calling select.bind
      has been reduced, but the vast majority
      of users shouldn't be using "bound metadata"
      anyway :).

    .. change::
        :tags: feature, sql
        :tickets: 2166, 1944

      The update() construct can now accommodate
      multiple tables in the WHERE clause, which will
      render an "UPDATE..FROM" construct, recognized by
      PostgreSQL and MSSQL.  When compiled on MySQL,
      will instead generate "UPDATE t1, t2, ..".  MySQL
      additionally can render against multiple tables in the
      SET clause, if Column objects are used as keys
      in the "values" parameter or generative method.

    .. change::
        :tags: feature, sql
        :tickets: 77

      Added accessor to types called "python_type",
      returns the rudimentary Python type object
      for a particular TypeEngine instance, if known,
      else raises NotImplementedError.

    .. change::
        :tags: bug, sql
        :tickets: 2261, 2319

      further tweak to the fix from,
      so that generative methods work a bit better
      off of cloned (this is almost a non-use case though).
      In particular this allows with_only_columns()
      to behave more consistently.   Added additional
      documentation to with_only_columns() to clarify
      expected behavior, which changed as a result
      of.

    .. change::
        :tags: engine, bug
        :tickets: 2317

      Fixed bug whereby transaction.rollback()
      would throw an error on an invalidated
      connection if the transaction were a
      two-phase or savepoint transaction.
      For plain transactions, rollback() is a no-op
      if the connection is invalidated, so while
      it wasn't 100% clear if it should be a no-op,
      at least now the interface is consistent.

    .. change::
        :tags: feature, schema
        :tickets:

      Added new support for remote "schemas":

    .. change::
        :tags: schema
        :tickets:

      MetaData() accepts "schema" and "quote_schema"
      arguments, which will be applied to the same-named
      arguments of a Table
      or Sequence which leaves these at their default
      of ``None``.

    .. change::
        :tags: schema
        :tickets:

      Sequence accepts "quote_schema" argument

    .. change::
        :tags: schema
        :tickets:

      tometadata() for Table will use the "schema"
      of the incoming MetaData for the new Table
      if the schema argument is explicitly "None"

    .. change::
        :tags: schema
        :tickets:

      Added CreateSchema and DropSchema DDL
      constructs - these accept just the string
      name of a schema and a "quote" flag.

    .. change::
        :tags: schema
        :tickets:

      When using default "schema" with MetaData,
      ForeignKey will also assume the "default" schema
      when locating remote table.  This allows the "schema"
      argument on MetaData to be applied to any
      set of Table objects that otherwise don't have
      a "schema".

    .. change::
        :tags: schema
        :tickets: 1679

      a "has_schema" method has been implemented
      on dialect, but only works on PostgreSQL so far.
      Courtesy Manlio Perillo.

    .. change::
        :tags: feature, schema
        :tickets: 1410

      The "extend_existing" flag on Table
      now allows for the reflection process to take
      effect for a Table object that's already been
      defined; when autoload=True and extend_existing=True
      are both set, the full set of columns will be
      reflected from the Table which will then
      *overwrite* those columns already present,
      rather than no activity occurring.  Columns that
      are present directly in the autoload run
      will be used as always, however.

    .. change::
        :tags: bug, schema
        :tickets:

      Fixed bug whereby TypeDecorator would
      return a stale value for _type_affinity, when
      using a TypeDecorator that "switches" types,
      like the CHAR/UUID type.

    .. change::
        :tags: bug, schema
        :tickets:

      Fixed bug whereby "order_by='foreign_key'"
      option to Inspector.get_table_names
      wasn't implementing the sort properly, replaced
      with the existing sort algorithm

    .. change::
        :tags: bug, schema
        :tickets: 2305

      the "name" of a column-level CHECK constraint,
      if present, is now rendered in the CREATE TABLE
      statement using "CONSTRAINT <name> CHECK <expression>".

    .. change::
        :tags: pyodbc, bug
        :tickets: 2318

      pyodbc-based dialects now parse the
      pyodbc accurately as far as observed
      pyodbc strings, including such gems
      as "py3-3.0.1-beta4"

    .. change::
        :tags: postgresql, bug
        :tickets: 2311

      PostgreSQL dialect memoizes that an ENUM of a
      particular name was processed
      during a create/drop sequence.  This allows
      a create/drop sequence to work without any
      calls to "checkfirst", and also means with
      "checkfirst" turned on it only needs to
      check for the ENUM once.

    .. change::
        :tags: postgresql, feature
        :tickets:

      Added create_type constructor argument
      to pg.ENUM.  When False, no CREATE/DROP or
      checking for the type will be performed as part
      of a table create/drop event; only the
      create()/drop)() methods called directly
      will do this.  Helps with Alembic "offline"
      scripts.

    .. change::
        :tags: mssql, feature
        :tickets: 822

      lifted the restriction on SAVEPOINT
      for SQL Server.  All tests pass using it,
      it's not known if there are deeper issues
      however.

    .. change::
        :tags: mssql, bug
        :tickets: 2336

      repaired the with_hint() feature which
      wasn't implemented correctly on MSSQL -
      usually used for the "WITH (NOLOCK)" hint
      (which you shouldn't be using anyway !
      use snapshot isolation instead :) )

    .. change::
        :tags: mssql, bug
        :tickets: 2318

      use new pyodbc version detection for
      _need_decimal_fix option.

    .. change::
        :tags: mssql, bug
        :tickets: 2343

      don't cast "table name" as NVARCHAR
      on SQL Server 2000.  Still mostly in the dark
      what incantations are needed to make PyODBC
      work fully with FreeTDS 0.91 here, however.

    .. change::
        :tags: mssql, bug
        :tickets: 2269

      Decode incoming values when retrieving
      list of index names and the names of columns
      within those indexes.

    .. change::
        :tags: bug, mysql
        :tickets:

      Unicode adjustments allow latest pymysql
      (post 0.4) to pass 100% on Python 2.

    .. change::
        :tags: ext, feature
        :tickets:

      Added an example to the hybrid docs
      of a "transformer" - a hybrid that returns a
      query-transforming callable in combination
      with a custom comparator.   Uses a new method
      on Query called with_transformation().  The use
      case here is fairly experimental, but only
      adds one line of code to Query.

    .. change::
        :tags: ext, bug
        :tickets:

      the @compiles decorator raises an
      informative error message when no "default"
      compilation handler is present, rather
      than KeyError.

    .. change::
        :tags: examples, bug
        :tickets:

      Fixed bug in history_meta.py example where
      the "unique" flag was not removed from a
      single-table-inheritance subclass which
      generates columns to put up onto the base.

.. changelog::
    :version: 0.7.3
    :released: Sun Oct 16 2011

    .. change::
        :tags: general
        :tickets: 2279

      Adjusted the "importlater" mechanism, which is
      used internally to resolve import cycles,
      such that the usage of __import__ is completed
      when the import of sqlalchemy or sqlalchemy.orm
      is done, thereby avoiding any usage of __import__
      after the application starts new threads,
      fixes.  Also in 0.6.9.

    .. change::
        :tags: orm
        :tickets: 2298

      Improved query.join() such that the "left" side
      can more flexibly be a non-ORM selectable,
      such as a subquery.   A selectable placed
      in select_from() will now be used as the left
      side, favored over implicit usage
      of a mapped entity.
      If the join still fails based on lack of
      foreign keys, the error message includes
      this detail.  Thanks to brianrhude
      on IRC for the test case.

    .. change::
        :tags: orm
        :tickets: 2241

      Added after_soft_rollback() Session event.  This
      event fires unconditionally whenever rollback()
      is called, regardless of if an actual DBAPI
      level rollback occurred.  This event
      is specifically designed to allow operations
      with the Session to proceed after a rollback
      when the Session.is_active is True.

    .. change::
        :tags: orm
        :tickets:

      added "adapt_on_names" boolean flag to orm.aliased()
      construct.  Allows an aliased() construct
      to link the ORM entity to a selectable that contains
      aggregates or other derived forms of a particular
      attribute, provided the name is the same as that
      of the entity mapped column.

    .. change::
        :tags: orm
        :tickets:

      Added new flag expire_on_flush=False to column_property(),
      marks those properties that would otherwise be considered
      to be "readonly", i.e. derived from SQL expressions,
      to retain their value after a flush has occurred, including
      if the parent object itself was involved in an update.

    .. change::
        :tags: orm
        :tickets: 2237

      Enhanced the instrumentation in the ORM to support
      Py3K's new argument style of "required kw arguments",
      i.e. fn(a, b, \*, c, d), fn(a, b, \*args, c, d).
      Argument signatures of mapped object's __init__
      method will be preserved, including required kw rules.

    .. change::
        :tags: orm
        :tickets: 2282

      Fixed bug in unit of work whereby detection of
      "cycles" among classes in highly interlinked patterns
      would not produce a deterministic
      result; thereby sometimes missing some nodes that
      should be considered cycles and causing further
      issues down the road.  Note this bug is in 0.6
      also; not backported at the moment.

    .. change::
        :tags: orm
        :tickets:

      Fixed a variety of synonym()-related regressions
      from 0.6:

          * making a synonym against a synonym now works.
          * synonyms made against a relationship() can
            be passed to query.join(), options sent
            to query.options(), passed by name
            to query.with_parent().

    .. change::
        :tags: orm
        :tickets: 2287

      Fixed bug whereby mapper.order_by attribute would
      be ignored in the "inner" query within a
      subquery eager load. .
      Also in 0.6.9.

    .. change::
        :tags: orm
        :tickets: 2267

      Identity map .discard() uses dict.pop(,None)
      internally instead of "del" to avoid KeyError/warning
      during a non-determinate gc teardown

    .. change::
        :tags: orm
        :tickets: 2253

      Fixed regression in new composite rewrite where
      deferred=True option failed due to missing
      import

    .. change::
        :tags: orm
        :tickets: 2248

      Reinstated "comparator_factory" argument to
      composite(), removed when 0.7 was released.

    .. change::
        :tags: orm
        :tickets: 2247

      Fixed bug in query.join() which would occur
      in a complex multiple-overlapping path scenario,
      where the same table could be joined to
      twice.  Thanks *much* to Dave Vitek
      for the excellent fix here.

    .. change::
        :tags: orm
        :tickets:

      Query will convert an OFFSET of zero when
      slicing into None, so that needless OFFSET
      clauses are not invoked.

    .. change::
        :tags: orm
        :tickets:

      Repaired edge case where mapper would fail
      to fully update internal state when a relationship
      on a new mapper would establish a backref on the
      first mapper.

    .. change::
        :tags: orm
        :tickets: 2260

      Fixed bug whereby if __eq__() was
      redefined, a relationship many-to-one lazyload
      would hit the __eq__() and fail.
      Does not apply to 0.6.9.

    .. change::
        :tags: orm
        :tickets: 2196

      Calling class_mapper() and passing in an object
      that is not a "type" (i.e. a class that could
      potentially be mapped) now raises an informative
      ArgumentError, rather than UnmappedClassError.

    .. change::
        :tags: orm
        :tickets:

      New event hook, MapperEvents.after_configured().
      Called after a configure() step has completed and
      mappers were in fact affected.   Theoretically this
      event is called once per application, unless new mappings
      are constructed after existing ones have been used
      already.

    .. change::
        :tags: orm
        :tickets: 2281

      When an open Session is garbage collected, the objects
      within it which remain are considered detached again
      when they are add()-ed to a new Session.
      This is accomplished by an extra check that the previous
      "session_key" doesn't actually exist among the pool
      of Sessions.

    .. change::
        :tags: orm
        :tickets: 2239

      New declarative features:

          * __declare_last__() method, establishes an event
            listener for the class method that will be called
            when mappers are completed with the final "configure"
            step.
          * __abstract__ flag.   The class will not be mapped
            at all when this flag is present on the class.
          * New helper classes ConcreteBase, AbstractConcreteBase.
            Allow concrete mappings using declarative which automatically
            set up the "polymorphic_union" when the "configure"
            mapper step is invoked.
          * The mapper itself has semi-private methods that allow
            the "with_polymorphic" selectable to be assigned
            to the mapper after it has already been configured.

    .. change::
        :tags: orm
        :tickets: 2283

      Declarative will warn when a subclass' base uses
      @declared_attr for a regular column - this attribute
      does not propagate to subclasses.

    .. change::
        :tags: orm
        :tickets: 2280

      The integer "id" used to link a mapped instance with
      its owning Session is now generated by a sequence
      generation function rather than id(Session), to
      eliminate the possibility of recycled id() values
      causing an incorrect result, no need to check that
      object actually in the session.

    .. change::
        :tags: orm
        :tickets: 2257

      Behavioral improvement: empty
      conjunctions such as and_() and or_() will be
      flattened in the context of an enclosing conjunction,
      i.e. and_(x, or_()) will produce 'X' and not 'X AND
      ()'..

    .. change::
        :tags: orm
        :tickets: 2261

      Fixed bug regarding calculation of "from" list
      for a select() element.  The "from" calc is now
      delayed, so that if the construct uses a Column
      object that is not yet attached to a Table,
      but is later associated with a Table, it generates
      SQL using the table as a FROM.   This change
      impacted fairly deeply the mechanics of how
      the FROM list as well as the "correlates" collection
      is calculated, as some "clause adaption" schemes
      (these are used very heavily in the ORM)
      were relying upon the fact that the "froms"
      collection would typically be cached before the
      adaption completed.   The rework allows it
      such that the "froms" collection can be cleared
      and re-generated at any time.

    .. change::
        :tags: orm
        :tickets: 2270

      Fixed bug whereby with_only_columns() method of
      Select would fail if a selectable were passed..  Also in 0.6.9.

    .. change::
        :tags: schema
        :tickets: 2284

      Modified Column.copy() to use _constructor(),
      which defaults to self.__class__, in order to
      create the new object.  This allows easier support
      of subclassing Column.

    .. change::
        :tags: schema
        :tickets: 2223

      Added a slightly nicer __repr__() to SchemaItem
      classes.  Note the repr here can't fully support
      the "repr is the constructor" idea since schema
      items can be very deeply nested/cyclical, have
      late initialization of some things, etc.

    .. change::
        :tags: engine
        :tickets: 2254

      The recreate() method in all pool classes uses
      self.__class__ to get at the type of pool
      to produce, in the case of subclassing.  Note
      there's no usual need to subclass pools.

    .. change::
        :tags: engine
        :tickets: 2243

      Improvement to multi-param statement logging,
      long lists of bound parameter sets will be
      compressed with an informative indicator
      of the compression taking place.  Exception
      messages use the same improved formatting.

    .. change::
        :tags: engine
        :tickets:

      Added optional "sa_pool_key" argument to
      pool.manage(dbapi).connect() so that serialization
      of args is not necessary.

    .. change::
        :tags: engine
        :tickets: 2286

      The entry point resolution supported by
      create_engine() now supports resolution of
      individual DBAPI drivers on top of a built-in
      or entry point-resolved dialect, using the
      standard '+' notation - it's converted to
      a '.' before being resolved as an entry
      point.

    .. change::
        :tags: engine
        :tickets: 2299

      Added an exception catch + warning for the
      "return unicode detection" step within connect,
      allows databases that crash on NVARCHAR to
      continue initializing, assuming no NVARCHAR
      type implemented.

    .. change::
        :tags: types
        :tickets: 2258

      Extra keyword arguments to the base Float
      type beyond "precision" and "asdecimal" are ignored;
      added a deprecation warning here and additional
      docs, related to

    .. change::
        :tags: sqlite
        :tickets:

      Ensured that the same ValueError is raised for
      illegal date/time/datetime string parsed from
      the database regardless of whether C
      extensions are in use or not.

    .. change::
        :tags: postgresql
        :tickets: 2290

      Added "postgresql_using" argument to Index(), produces
      USING clause to specify index implementation for
      PG. .  Thanks to Ryan P. Kelly for
      the patch.

    .. change::
        :tags: postgresql
        :tickets: 1839

      Added client_encoding parameter to create_engine()
      when the postgresql+psycopg2 dialect is used;
      calls the psycopg2 set_client_encoding() method
      with the value upon connect.

    .. change::
        :tags: postgresql
        :tickets: 2291, 2141

      Fixed bug related to whereby the
      same modified index behavior in PG 9 affected
      primary key reflection on a renamed column..  Also in 0.6.9.

    .. change::
        :tags: postgresql
        :tickets: 2256

      Reflection functions for Table, Sequence no longer
      case insensitive.  Names can be differ only in case
      and will be correctly distinguished.

    .. change::
        :tags: postgresql
        :tickets:

      Use an atomic counter as the "random number"
      source for server side cursor names;
      conflicts have been reported in rare cases.

    .. change::
        :tags: postgresql
        :tickets: 2249

      Narrowed the assumption made when reflecting
      a foreign-key referenced table with schema in
      the current search path; an explicit schema will
      be applied to the referenced table only if
      it actually matches that of the referencing table,
      which also has an explicit schema.   Previously
      it was assumed that "current" schema was synonymous
      with the full search_path.

    .. change::
        :tags: mysql
        :tickets: 2225

      a CREATE TABLE will put the COLLATE option
      after CHARSET, which appears to be part of
      MySQL's arbitrary rules regarding if it will actually
      work or not.   Also in 0.6.9.

    .. change::
        :tags: mysql
        :tickets: 2293

      Added mysql_length parameter to Index construct,
      specifies "length" for indexes.

    .. change::
        :tags: mssql
        :tickets: 2273

      Changes to attempt support of FreeTDS 0.91 with
      Pyodbc.  This includes that string binds are sent as
      Python unicode objects when FreeTDS 0.91 is detected,
      and a CAST(? AS NVARCHAR) is used when we detect
      for a table.   However, I'd continue
      to characterize Pyodbc + FreeTDS 0.91 behavior as
      pretty crappy, there are still many queries such
      as used in reflection which cause a core dump on
      Linux, and it is not really usable at all
      on OSX, MemoryErrors abound and just plain broken
      unicode support.

    .. change::
        :tags: mssql
        :tickets: 2277

      The behavior of =/!= when comparing a scalar select
      to a value will no longer produce IN/NOT IN as of 0.8;
      this behavior is a little too heavy handed (use in_() if
      you want to emit IN) and now emits a deprecation warning.
      To get the 0.8 behavior immediately and remove the warning,
      a compiler recipe is given at
      http://www.sqlalchemy.org/docs/07/dialects/mssql.html#scalar-select-comparisons
      to override the behavior of visit_binary().

    .. change::
        :tags: mssql
        :tickets: 2222

      "0" is accepted as an argument for limit() which
      will produce "TOP 0".

    .. change::
        :tags: oracle
        :tickets: 2272

      Fixed ReturningResultProxy for zxjdbc dialect..  Regression from 0.6.

    .. change::
        :tags: oracle
        :tickets: 2252

      The String type now generates VARCHAR2 on Oracle
      which is recommended as the default VARCHAR.
      Added an explicit VARCHAR2 and NVARCHAR2 to the Oracle
      dialect as well.   Using NVARCHAR still generates
      "NVARCHAR2" - there is no "NVARCHAR" on Oracle -
      this remains a slight breakage of the "uppercase types
      always give exactly that" policy.  VARCHAR still
      generates "VARCHAR", keeping with the policy.   If
      Oracle were to ever define "VARCHAR" as something
      different as they claim (IMHO this will never happen),
      the type would be available.

    .. change::
        :tags: ext
        :tickets: 2262

      SQLSoup will not be included in version 0.8
      of SQLAlchemy; while useful, we would like to
      keep SQLAlchemy itself focused on one ORM
      usage paradigm.  SQLSoup will hopefully
      soon be superseded by a third party
      project.

    .. change::
        :tags: ext
        :tickets: 2236

      Added local_attr, remote_attr, attr accessors
      to AssociationProxy, providing quick access
      to the proxied attributes at the class
      level.

    .. change::
        :tags: ext
        :tickets: 2275

      Changed the update() method on association proxy
      dictionary to use a duck typing approach, i.e.
      checks for "keys", to discern between update({})
      and update((a, b)).   Previously, passing a
      dictionary that had tuples as keys would be misinterpreted
      as a sequence.

    .. change::
        :tags: examples
        :tickets: 2266

      Adjusted dictlike-polymorphic.py example
      to apply the CAST such that it works on
      PG, other databases.
      Also in 0.6.9.

.. changelog::
    :version: 0.7.2
    :released: Sun Jul 31 2011

    .. change::
        :tags: orm
        :tickets: 2213

      Feature enhancement: joined and subquery
      loading will now traverse already-present related
      objects and collections in search of unpopulated
      attributes throughout the scope of the eager load
      being defined, so that the eager loading that is
      specified via mappings or query options
      unconditionally takes place for the full depth,
      populating whatever is not already populated.
      Previously, this traversal would stop if a related
      object or collection were already present leading
      to inconsistent behavior (though would save on
      loads/cycles for an already-loaded graph). For a
      subqueryload, this means that the additional
      SELECT statements emitted by subqueryload will
      invoke unconditionally, no matter how much of the
      existing graph is already present (hence the
      controversy). The previous behavior of "stopping"
      is still in effect when a query is the result of
      an attribute-initiated lazyload, as otherwise an
      "N+1" style of collection iteration can become
      needlessly expensive when the same related object
      is encountered repeatedly. There's also an
      as-yet-not-public generative Query method
      _with_invoke_all_eagers()
      which selects old/new behavior

    .. change::
        :tags: orm
        :tickets: 2195

      A rework of "replacement traversal" within
      the ORM as it alters selectables to be against
      aliases of things (i.e. clause adaption) includes
      a fix for multiply-nested any()/has() constructs
      against a joined table structure.

    .. change::
        :tags: orm
        :tickets: 2234

      Fixed bug where query.join() + aliased=True
      from a joined-inh structure to itself on
      relationship() with join condition on the child
      table would convert the lead entity into the
      joined one inappropriately.
      Also in 0.6.9.

    .. change::
        :tags: orm
        :tickets: 2205

      Fixed regression from 0.6 where Session.add()
      against an object which contained None in a
      collection would raise an internal exception.
      Reverted this to 0.6's behavior which is to
      accept the None but obviously nothing is
      persisted.  Ideally, collections with None
      present or on append() should at least emit a
      warning, which is being considered for 0.8.

    .. change::
        :tags: orm
        :tickets: 2191

      Load of a deferred() attribute on an object
      where row can't be located raises
      ObjectDeletedError instead of failing later
      on; improved the message in ObjectDeletedError
      to include other conditions besides a simple
      "delete".

    .. change::
        :tags: orm
        :tickets: 2224

      Fixed regression from 0.6 where a get history
      operation on some relationship() based attributes
      would fail when a lazyload would emit; this could
      trigger within a flush() under certain conditions.  Thanks to the user who submitted
      the great test for this.

    .. change::
        :tags: orm
        :tickets: 2228

      Fixed bug apparent only in Python 3 whereby
      sorting of persistent + pending objects during
      flush would produce an illegal comparison,
      if the persistent object primary key
      is not a single integer.
      Also in 0.6.9

    .. change::
        :tags: orm
        :tickets: 2197

      Fixed bug whereby the source clause
      used by query.join() would be inconsistent
      if against a column expression that combined
      multiple entities together.
      Also in 0.6.9

    .. change::
        :tags: orm
        :tickets: 2215

      Fixed bug whereby if a mapped class
      redefined __hash__() or __eq__() to something
      non-standard, which is a supported use case
      as SQLA should never consult these,
      the methods would be consulted if the class
      was part of a "composite" (i.e. non-single-entity)
      result set.
      Also in 0.6.9.

    .. change::
        :tags: orm
        :tickets: 2240

      Added public attribute ".validators" to
      Mapper, an immutable dictionary view of
      all attributes that have been decorated
      with the @validates decorator. courtesy Stefano Fontanelli

    .. change::
        :tags: orm
        :tickets: 2188

      Fixed subtle bug that caused SQL to blow
      up if: column_property() against subquery +
      joinedload + LIMIT + order by the column
      property() occurred. .
      Also in 0.6.9

    .. change::
        :tags: orm
        :tickets: 2207

      The join condition produced by with_parent
      as well as when using a "dynamic" relationship
      against a parent will generate unique
      bindparams, rather than incorrectly repeating
      the same bindparam. .
      Also in 0.6.9.

    .. change::
        :tags: orm
        :tickets:

      Added the same "columns-only" check to
      mapper.polymorphic_on as used when
      receiving user arguments to
      relationship.order_by, foreign_keys,
      remote_side, etc.

    .. change::
        :tags: orm
        :tickets: 2190

      Fixed bug whereby comparison of column
      expression to a Query() would not call
      as_scalar() on the underlying SELECT
      statement to produce a scalar subquery,
      in the way that occurs if you called
      it on Query().subquery().

    .. change::
        :tags: orm
        :tickets: 2194

      Fixed declarative bug where a class inheriting
      from a superclass of the same name would fail
      due to an unnecessary lookup of the name
      in the _decl_class_registry.

    .. change::
        :tags: orm
        :tickets: 2199

      Repaired the "no statement condition"
      assertion in Query which would attempt
      to raise if a generative method were called
      after from_statement() were called..  Also in 0.6.9.

    .. change::
        :tags: sql
        :tickets: 2188

      Fixed two subtle bugs involving column
      correspondence in a selectable,
      one with the same labeled subquery repeated, the other
      when the label has been "grouped" and
      loses itself.  Affects.

    .. change::
        :tags: schema
        :tickets: 2187

      New feature: with_variant() method on
      all types.  Produces an instance of Variant(),
      a special TypeDecorator which will select
      the usage of a different type based on the
      dialect in use.

    .. change::
        :tags: schema
        :tickets:

      Added an informative error message when
      ForeignKeyConstraint refers to a column name in
      the parent that is not found.  Also in 0.6.9.

    .. change::
        :tags: schema
        :tickets: 2206

      Fixed bug whereby adaptation of old append_ddl_listener()
      function was passing unexpected \**kw through
      to the Table event.   Table gets no kws, the MetaData
      event in 0.6 would get "tables=somecollection",
      this behavior is preserved.

    .. change::
        :tags: schema
        :tickets:

      Fixed bug where "autoincrement" detection on
      Table would fail if the type had no "affinity"
      value, in particular this would occur when using
      the UUID example on the site that uses TypeEngine
      as the "impl".

    .. change::
        :tags: schema
        :tickets: 2209

      Added an improved repr() to TypeEngine objects
      that will only display constructor args which
      are positional or kwargs that deviate
      from the default.

    .. change::
        :tags: engine
        :tickets:

      Context manager provided by Connection.begin()
      will issue rollback() if the commit() fails,
      not just if an exception occurs.

    .. change::
        :tags: engine
        :tickets: 1682

      Use urllib.parse_qsl() in Python 2.6 and above,
      no deprecation warning about cgi.parse_qsl()

    .. change::
        :tags: engine
        :tickets:

      Added mixin class sqlalchemy.ext.DontWrapMixin.
      User-defined exceptions of this type are never
      wrapped in StatementException when they
      occur in the context of a statement
      execution.

    .. change::
        :tags: engine
        :tickets:

      StatementException wrapping will display the
      original exception class in the message.

    .. change::
        :tags: engine
        :tickets: 2201

      Failures on connect which raise dbapi.Error
      will forward the error to dialect.is_disconnect()
      and set the "connection_invalidated" flag if
      the dialect knows this to be a potentially
      "retryable" condition.  Only Oracle ORA-01033
      implemented for now.

    .. change::
        :tags: sqlite
        :tickets: 2189

      SQLite dialect no longer strips quotes
      off of reflected default value, allowing
      a round trip CREATE TABLE to work.
      This is consistent with other dialects
      that also maintain the exact form of
      the default.

    .. change::
        :tags: postgresql
        :tickets: 2198

      Added new "postgresql_ops" argument to
      Index, allows specification of PostgreSQL
      operator classes for indexed columns.  Courtesy Filip Zyzniewski.

    .. change::
        :tags: mysql
        :tickets: 2186

      Fixed OurSQL dialect to use ansi-neutral
      quote symbol "'" for XA commands instead
      of '"'. .  Also in 0.6.9.

    .. change::
        :tags: mssql
        :tickets:

      Adjusted the pyodbc dialect such that bound
      values are passed as bytes and not unicode
      if the "Easysoft" unix drivers are detected.
      This is the same behavior as occurs with
      FreeTDS.  Easysoft appears to segfault
      if Python unicodes are passed under
      certain circumstances.

    .. change::
        :tags: oracle
        :tickets: 2200

      Added ORA-00028 to disconnect codes, use
      cx_oracle _Error.code to get at the code,.  Also in 0.6.9.

    .. change::
        :tags: oracle
        :tickets: 2201

      Added ORA-01033 to disconnect codes, which
      can be caught during a connection
      event.

    .. change::
        :tags: oracle
        :tickets: 2220

      repaired the oracle.RAW type which did not
      generate the correct DDL.
      Also in 0.6.9.

    .. change::
        :tags: oracle
        :tickets: 2212

      added CURRENT to reserved word list. Also in 0.6.9.

    .. change::
        :tags: oracle
        :tickets:

      Fixed bug in the mutable extension whereby
      if the same type were used twice in one
      mapping, the attributes beyond the first
      would not get instrumented.

    .. change::
        :tags: oracle
        :tickets:

      Fixed bug in the mutable extension whereby
      if None or a non-corresponding type were set,
      an error would be raised.  None is now accepted
      which assigns None to all attributes,
      illegal values raise ValueError.

    .. change::
        :tags: examples
        :tickets:

      Repaired the examples/versioning test runner
      to not rely upon SQLAlchemy test libs,
      nosetests must be run from within
      examples/versioning to get around setup.cfg
      breaking it.

    .. change::
        :tags: examples
        :tickets:

      Tweak to examples/versioning to pick the
      correct foreign key in a multi-level
      inheritance situation.

    .. change::
        :tags: examples
        :tickets:

      Fixed the attribute shard example to check
      for bind param callable correctly in 0.7
      style.

.. changelog::
    :version: 0.7.1
    :released: Sun Jun 05 2011

    .. change::
        :tags: general
        :tickets: 2184

      Added a workaround for Python bug 7511 where
      failure of C extension build does not
      raise an appropriate exception on Windows 64
      bit + VC express

    .. change::
        :tags: orm
        :tickets: 1912

      "delete-orphan" cascade is now allowed on
      self-referential relationships - this since
      SQLA 0.7 no longer enforces "parent with no
      child" at the ORM level; this check is left
      up to foreign key nullability.
      Related to

    .. change::
        :tags: orm
        :tickets: 2180

      Repaired new "mutable" extension to propagate
      events to subclasses correctly; don't
      create multiple event listeners for
      subclasses either.

    .. change::
        :tags: orm
        :tickets: 2170

      Modify the text of the message which occurs
      when the "identity" key isn't detected on
      flush, to include the common cause that
      the Column isn't set up to detect
      auto-increment correctly;.
      Also in 0.6.8.

    .. change::
        :tags: orm
        :tickets: 2182

      Fixed bug where transaction-level "deleted"
      collection wouldn't be cleared of expunged
      states, raising an error if they later
      became transient.
      Also in 0.6.8.

    .. change::
        :tags: sql
        :tickets:

      Fixed bug whereby metadata.reflect(bind)
      would close a Connection passed as a
      bind argument.  Regression from 0.6.

    .. change::
        :tags: sql
        :tickets:

      Streamlined the process by which a Select
      determines what's in its '.c' collection.
      Behaves identically, except that a
      raw ClauseList() passed to select([])
      (which is not a documented case anyway) will
      now be expanded into its individual column
      elements instead of being ignored.

    .. change::
        :tags: engine
        :tickets:

      Deprecate schema/SQL-oriented methods on
      Connection/Engine that were never well known
      and are redundant:  reflecttable(), create(),
      drop(), text(), engine.func

    .. change::
        :tags: engine
        :tickets: 2178

      Adjusted the __contains__() method of
      a RowProxy result row such that no exception
      throw is generated internally;
      NoSuchColumnError() also will generate its
      message regardless of whether or not the column
      construct can be coerced to a string..  Also in 0.6.8.

    .. change::
        :tags: sqlite
        :tickets: 2173

      Accept None from cursor.fetchone() when
      "PRAGMA read_uncommitted" is called to determine
      current isolation mode at connect time and
      default to SERIALIZABLE; this to support SQLite
      versions pre-3.3.0 that did not have this
      feature.

    .. change::
        :tags: postgresql
        :tickets: 2175

      Some unit test fixes regarding numeric arrays,
      MATCH operator.   A potential floating-point
      inaccuracy issue was fixed, and certain tests
      of the MATCH operator only execute within an
      EN-oriented locale for now. .
      Also in 0.6.8.

    .. change::
        :tags: mysql
        :tickets:

      Unit tests pass 100% on MySQL installed
      on windows.

    .. change::
        :tags: mysql
        :tickets: 2181

      Removed the "adjust casing" step that would
      fail when reflecting a table on MySQL
      on windows with a mixed case name.  After some
      experimenting with a windows MySQL server, it's
      been determined that this step wasn't really
      helping the situation much; MySQL does not return
      FK names with proper casing on non-windows
      platforms either, and removing the step at
      least allows the reflection to act more like
      it does on other OSes.   A warning here
      has been considered but its difficult to
      determine under what conditions such a warning
      can be raised, so punted on that for now -
      added some docs instead.

    .. change::
        :tags: mysql
        :tickets:

      supports_sane_rowcount will be set to False
      if using MySQLdb and the DBAPI doesn't provide
      the constants.CLIENT module.

.. changelog::
    :version: 0.7.0
    :released: Fri May 20 2011

    .. change::
        :tags:
        :tickets:

      This section documents those changes from 0.7b4
      to 0.7.0.  For an overview of what's new in
      SQLAlchemy 0.7, see
      http://docs.sqlalchemy.org/en/latest/changelog/migration_07.html

    .. change::
        :tags: orm
        :tickets: 2069

      Fixed regression introduced in 0.7b4 (!) whereby
      query.options(someoption("nonexistent name")) would
      fail to raise an error.  Also added additional
      error catching for cases where the option would
      try to build off a column-based element, further
      fixed up some of the error messages tailored
      in

    .. change::
        :tags: orm
        :tickets: 2162

      query.count() emits "count(*)" instead of
      "count(1)".

    .. change::
        :tags: orm
        :tickets: 2155

      Fine tuning of Query clause adaptation when
      from_self(), union(), or other "select from
      myself" operation, such that plain SQL expression
      elements added to filter(), order_by() etc.
      which are present in the nested "from myself"
      query *will* be adapted in the same way an ORM
      expression element will, since these
      elements are otherwise not easily accessible.

    .. change::
        :tags: orm
        :tickets: 2149

      Fixed bug where determination of "self referential"
      relationship would fail with no workaround
      for joined-inh subclass related to itself,
      or joined-inh subclass related to a subclass
      of that with no cols in the sub-sub class
      in the join condition.
      Also in 0.6.8.

    .. change::
        :tags: orm
        :tickets: 2153

      mapper() will ignore non-configured foreign keys
      to unrelated tables when determining inherit
      condition between parent and child class,
      but will raise as usual for unresolved
      columns and table names regarding the inherited
      table.  This is an enhanced generalization of
      behavior that was already applied to declarative
      previously.    0.6.8 has a more
      conservative version of this which doesn't
      fundamentally alter how join conditions
      are determined.

    .. change::
        :tags: orm
        :tickets: 2144

      It is an error to call query.get() when the
      given entity is not a single, full class
      entity or mapper (i.e. a column).  This is
      a deprecation warning in 0.6.8.

    .. change::
        :tags: orm
        :tickets: 2148

      Fixed a potential KeyError which under some
      circumstances could occur with the identity
      map, part of

    .. change::
        :tags: orm
        :tickets:

      added Query.with_session() method, switches
      Query to use a different session.

    .. change::
        :tags: orm
        :tickets: 2131

      horizontal shard query should use execution
      options per connection as per

    .. change::
        :tags: orm
        :tickets: 2151

      a non_primary mapper will inherit the _identity_class
      of the primary mapper.  This so that a non_primary
      established against a class that's normally in an
      inheritance mapping will produce results that are
      identity-map compatible with that of the primary
      mapper (also in 0.6.8)

    .. change::
        :tags: orm
        :tickets: 2163

      Fixed the error message emitted for "can't
      execute syncrule for destination column 'q';
      mapper 'X' does not map this column" to
      reference the correct mapper. .
      Also in 0.6.8.

    .. change::
        :tags: orm
        :tickets: 1502

      polymorphic_union() gets a "cast_nulls" option,
      disables the usage of CAST when it renders
      the labeled NULL columns.

    .. change::
        :tags: orm
        :tickets:

      polymorphic_union() renders the columns in their
      original table order, as according to the first
      table/selectable in the list of polymorphic
      unions in which they appear.  (which is itself
      an unordered mapping unless you pass an OrderedDict).

    .. change::
        :tags: orm
        :tickets: 2171

      Fixed bug whereby mapper mapped to an anonymous
      alias would fail if logging were used, due to
      unescaped % sign in the alias name.
      Also in 0.6.8.

    .. change::
        :tags: sql
        :tickets: 2167

      Fixed bug whereby nesting a label of a select()
      with another label in it would produce incorrect
      exported columns.   Among other things this would
      break an ORM column_property() mapping against
      another column_property(). .
      Also in 0.6.8

    .. change::
        :tags: sql
        :tickets:

      Changed the handling in determination of join
      conditions such that foreign key errors are
      only considered between the two given tables.
      That is, t1.join(t2) will report FK errors
      that involve 't1' or 't2', but anything
      involving 't3' will be skipped.   This affects
      join(), as well as ORM relationship and
      inherit condition logic.

    .. change::
        :tags: sql
        :tickets:

      Some improvements to error handling inside
      of the execute procedure to ensure auto-close
      connections are really closed when very
      unusual DBAPI errors occur.

    .. change::
        :tags: sql
        :tickets:

      metadata.reflect() and reflection.Inspector()
      had some reliance on GC to close connections
      which were internally procured, fixed this.

    .. change::
        :tags: sql
        :tickets: 2140

      Added explicit check for when Column .name
      is assigned as blank string

    .. change::
        :tags: sql
        :tickets: 2147

      Fixed bug whereby if FetchedValue was passed
      to column server_onupdate, it would not
      have its parent "column" assigned, added
      test coverage for all column default assignment
      patterns.   also in 0.6.8

    .. change::
        :tags: postgresql
        :tickets:

      Fixed the psycopg2_version parsing in the
      psycopg2 dialect.

    .. change::
        :tags: postgresql
        :tickets: 2141

      Fixed bug affecting PG 9 whereby index reflection
      would fail if against a column whose name
      had changed. .  Also in 0.6.8.

    .. change::
        :tags: mssql
        :tickets: 2169

      Fixed bug in MSSQL dialect whereby the aliasing
      applied to a schema-qualified table would leak
      into enclosing select statements.
      Also in 0.6.8.

    .. change::
        :tags: documentation
        :tickets: 2152

      Removed the usage of the "collections.MutableMapping"
      abc from the ext.mutable docs as it was being used
      incorrectly and makes the example more difficult
      to understand in any case.

    .. change::
        :tags: examples
        :tickets:

      removed the ancient "polymorphic association"
      examples and replaced with an updated set of
      examples that use declarative mixins,
      "generic_associations".   Each presents an alternative
      table layout.

    .. change::
        :tags: ext
        :tickets: 2143

      Fixed bugs in sqlalchemy.ext.mutable extension where
      `None` was not appropriately handled, replacement
      events were not appropriately handled.

.. changelog::
    :version: 0.7.0b4
    :released: Sun Apr 17 2011

    .. change::
        :tags: general
        :tickets:

      Changes to the format of CHANGES, this file.
      The format changes have been applied to
      the 0.7 releases.

    .. change::
        :tags: general
        :tickets:

      The "-declarative" changes will now be listed
      directly under the "-orm" section, as these
      are closely related.

    .. change::
        :tags: general
        :tickets:

      The 0.5 series changes have been moved to
      the file CHANGES_PRE_06 which replaces
      CHANGES_PRE_05.

    .. change::
        :tags: general
        :tickets:

      The changelog for 0.6.7 and subsequent within
      the 0.6 series is now listed only in the
      CHANGES file within the 0.6 branch.
      In the 0.7 CHANGES file (i.e. this file), all the
      0.6 changes are listed inline within the 0.7
      section in which they were also applied
      (since all 0.6 changes are in 0.7 as well).
      Changes that apply to an 0.6 version here
      are noted as are if any differences in
      implementation/behavior are present.

    .. change::
        :tags: orm
        :tickets: 2122

      Some fixes to "evaluate" and "fetch" evaluation
      when query.update(), query.delete() are called.
      The retrieval of records is done after autoflush
      in all cases, and before update/delete is
      emitted, guarding against unflushed data present
      as well as expired objects failing during
      the evaluation.

    .. change::
        :tags: orm
        :tickets: 2063

      Reworded the exception raised when a flush
      is attempted of a subclass that is not polymorphic
      against the supertype.

    .. change::
        :tags: orm
        :tickets:

      Still more wording adjustments when a query option
      can't find the target entity.  Explain that the
      path must be from one of the root entities.

    .. change::
        :tags: orm
        :tickets: 2123

      Some fixes to the state handling regarding
      backrefs, typically when autoflush=False, where
      the back-referenced collection wouldn't
      properly handle add/removes with no net
      change.  Thanks to Richard Murri for the
      test case + patch.
      (also in 0.6.7).

    .. change::
        :tags: orm
        :tickets: 2127

      Added checks inside the UOW to detect the unusual
      condition of being asked to UPDATE or DELETE
      on a primary key value that contains NULL
      in it.

    .. change::
        :tags: orm
        :tickets: 2127

      Some refinements to attribute history.  More
      changes are pending possibly in 0.8, but
      for now history has been modified such that
      scalar history doesn't have a "side effect"
      of populating None for a non-present value.
      This allows a slightly better ability to
      distinguish between a None set and no actual
      change, affects as well.

    .. change::
        :tags: orm
        :tickets: 2130

      a "having" clause would be copied from the
      inside to the outside query if from_self()
      were used; in particular this would break
      an 0.7 style count() query.
      (also in 0.6.7)

    .. change::
        :tags: orm
        :tickets: 2131

      the Query.execution_options() method now passes
      those options to the Connection rather than
      the SELECT statement, so that all available
      options including isolation level and
      compiled cache may be used.

    .. change::
        :tags: sql
        :tickets: 2131

      The "compiled_cache" execution option now raises
      an error when passed to a SELECT statement
      rather than a Connection.  Previously it was
      being ignored entirely.   We may look into
      having this option work on a per-statement
      level at some point.

    .. change::
        :tags: sql
        :tickets:

      Restored the "catchall" constructor on the base
      TypeEngine class, with a deprecation warning.
      This so that code which does something like
      Integer(11) still succeeds.

    .. change::
        :tags: sql
        :tickets: 2104

      Fixed regression whereby MetaData() coming
      back from unpickling did not keep track of
      new things it keeps track of now, i.e.
      collection of Sequence objects, list
      of schema names.

    .. change::
        :tags: sql
        :tickets: 2116

      The limit/offset keywords to select() as well
      as the value passed to select.limit()/offset()
      will be coerced to integer.
      (also in 0.6.7)

    .. change::
        :tags: sql
        :tickets:

      fixed bug where "from" clause gathering from an
      over() clause would be an itertools.chain() and
      not a list, causing "can only concatenate list"
      TypeError when combined with other clauses.

    .. change::
        :tags: sql
        :tickets: 2134

      Fixed incorrect usage of "," in over() clause
      being placed between the "partition" and "order by"
      clauses.

    .. change::
        :tags: sql
        :tickets: 2105

      Before/after attach events for PrimaryKeyConstraint
      now function, tests added for before/after events
      on all constraint types.

    .. change::
        :tags: sql
        :tickets: 2117

      Added explicit true()/false() constructs to expression
      lib - coercion rules will intercept "False"/"True"
      into these constructs.  In 0.6, the constructs were
      typically converted straight to string, which was
      no longer accepted in 0.7.

    .. change::
        :tags: engine
        :tickets: 2129

      The C extension is now enabled by default on CPython
      2.x with a fallback to pure python if it fails to
      compile.

    .. change::
        :tags: schema
        :tickets: 2109

      The 'useexisting' flag on Table has been superseded
      by a new pair of flags 'keep_existing' and
      'extend_existing'.   'extend_existing' is equivalent
      to 'useexisting' - the existing Table is returned,
      and additional constructor elements are added.
      With 'keep_existing', the existing Table is returned,
      but additional constructor elements are not added -
      these elements are only applied when the Table
      is newly created.

    .. change::
        :tags: types
        :tickets: 2081

      REAL has been added to the core types.  Supported
      by PostgreSQL, SQL Server, MySQL, SQLite.  Note
      that the SQL Server and MySQL versions, which
      add extra arguments, are also still available
      from those dialects.

    .. change::
        :tags: types
        :tickets: 2106

      Added @event.listens_for() decorator, given
      target + event name, applies the decorated
      function as a listener.

    .. change::
        :tags: pool
        :tickets: 2103

      AssertionPool now stores the traceback indicating
      where the currently checked out connection was
      acquired; this traceback is reported within
      the assertion raised upon a second concurrent
      checkout; courtesy Gunnlaugur Briem

    .. change::
        :tags: pool
        :tickets:

      The "pool.manage" feature doesn't use pickle
      anymore to hash the arguments for each pool.

    .. change::
        :tags: sqlite
        :tickets: 2115

      Fixed bug where reflection of foreign key
      created as "REFERENCES <tablename>" without
      col name would fail.
      (also in 0.6.7)

    .. change::
        :tags: postgresql
        :tickets:

      Psycopg2 for Python 3 is now supported.

    .. change::
        :tags: postgresql
        :tickets: 2132

      Fixed support for precision numerics when using
      pg8000.

    .. change::
        :tags: oracle
        :tickets: 2100

      Using column names that would require quotes
      for the column itself or for a name-generated
      bind parameter, such as names with special
      characters, underscores, non-ascii characters,
      now properly translate bind parameter keys when
      talking to cx_oracle.   (Also
      in 0.6.7)

    .. change::
        :tags: oracle
        :tickets: 2116

      Oracle dialect adds use_binds_for_limits=False
      create_engine() flag, will render the LIMIT/OFFSET
      values inline instead of as binds, reported to
      modify the execution plan used by Oracle. (Also in 0.6.7)

    .. change::
        :tags: documentation
        :tickets: 2029

      Documented SQLite DATE/TIME/DATETIME types. (also in 0.6.7)

    .. change::
        :tags: documentation
        :tickets: 2118

      Fixed mutable extension docs to show the
      correct type-association methods.

.. changelog::
    :version: 0.7.0b3
    :released: Sun Mar 20 2011

    .. change::
        :tags: general
        :tickets:

      Lots of fixes to unit tests when run under Pypy
      (courtesy Alex Gaynor).

    .. change::
        :tags: orm
        :tickets: 2093

      Changed the underlying approach to query.count().
      query.count() is now in all cases exactly:

          query.
              from_self(func.count(literal_column('1'))).
              scalar()

      That is, "select count(1) from (<full query>)".
      This produces a subquery in all cases, but
      vastly simplifies all the guessing count()
      tried to do previously, which would still
      fail in many scenarios particularly when
      joined table inheritance and other joins
      were involved.  If the subquery produced
      for an otherwise very simple count is really
      an issue, use query(func.count()) as an
      optimization.

    .. change::
        :tags: orm
        :tickets: 2087

      some changes to the identity map regarding
      rare weakref callbacks during iterations.
      The mutex has been removed as it apparently
      can cause a reentrant (i.e. in one thread) deadlock,
      perhaps when gc collects objects at the point of
      iteration in order to gain more memory.  It is hoped
      that "dictionary changed during iteration" will
      be exceedingly rare as iteration methods internally
      acquire the full list of objects in a single values()
      call. Note 0.6.7 has a more conservative fix here
      which still keeps the mutex in place.

    .. change::
        :tags: orm
        :tickets: 2082

      A tweak to the unit of work causes it to order
      the flush along relationship() dependencies even if
      the given objects don't have any inter-attribute
      references in memory, which was the behavior in
      0.5 and earlier, so a flush of Parent/Child with
      only foreign key/primary key set will succeed.
      This while still maintaining 0.6 and above's not
      generating a ton of useless internal dependency
      structures within the flush that don't correspond
      to state actually within the current flush.

    .. change::
        :tags: orm
        :tickets: 2069

      Improvements to the error messages emitted when
      querying against column-only entities in conjunction
      with (typically incorrectly) using loader options,
      where the parent entity is not fully present.

    .. change::
        :tags: orm
        :tickets: 2098

      Fixed bug in query.options() whereby a path
      applied to a lazyload using string keys could
      overlap a same named attribute on the wrong
      entity.  Note 0.6.7 has a more conservative fix
      to this.

    .. change::
        :tags: declarative
        :tickets: 2091

      Arguments in __mapper_args__ that aren't "hashable"
      aren't mistaken for always-hashable, possibly-column
      arguments.  (also in 0.6.7)

    .. change::
        :tags: sql
        :tickets:

      Added a fully descriptive error message for the
      case where Column is subclassed and _make_proxy()
      fails to make a copy due to TypeError on the
      constructor.   The method _constructor should
      be implemented in this case.

    .. change::
        :tags: sql
        :tickets: 2095

      Added new event "column_reflect" for Table objects.
      Receives the info dictionary about a Column before
      the object is generated within reflection, and allows
      modification to the dictionary for control over
      most aspects of the resulting Column including
      key, name, type, info dictionary.

    .. change::
        :tags: sql
        :tickets:

      To help with the "column_reflect" event being used
      with specific Table objects instead of all instances
      of Table, listeners can be added to a Table object
      inline with its construction using a new argument
      "listeners", a list of tuples of the form
      (<eventname>, <fn>), which are applied to the Table
      before the reflection process begins.

    .. change::
        :tags: sql
        :tickets: 2085

      Added new generic function "next_value()", accepts
      a Sequence object as its argument and renders the
      appropriate "next value" generation string on the
      target platform, if supported.  Also provides
      ".next_value()" method on Sequence itself.

    .. change::
        :tags: sql
        :tickets: 2084

      func.next_value() or other SQL expression can
      be embedded directly into an insert() construct,
      and if implicit or explicit "returning" is used
      in conjunction with a primary key column,
      the newly generated value will be present in
      result.inserted_primary_key.

    .. change::
        :tags: sql
        :tickets: 2089

      Added accessors to ResultProxy "returns_rows",
      "is_insert" (also in 0.6.7)

    .. change::
        :tags: engine
        :tickets: 2097

      Fixed AssertionPool regression bug.

    .. change::
        :tags: engine
        :tickets: 2060

      Changed exception raised to ArgumentError when an
      invalid dialect is specified.

    .. change::
        :tags: postgresql
        :tickets: 2092

      Added RESERVED_WORDS for postgresql dialect.
      (also in 0.6.7)

    .. change::
        :tags: postgresql
        :tickets: 2073

      Fixed the BIT type to allow a "length" parameter, "varying"
      parameter.  Reflection also fixed.
      (also in 0.6.7)

    .. change::
        :tags: mssql
        :tickets: 2071

      Rewrote the query used to get the definition of a view,
      typically when using the Inspector interface, to
      use sys.sql_modules instead of the information schema,
      thereby allowing views definitions longer than 4000
      characters to be fully returned.
      (also in 0.6.7)

    .. change::
        :tags: firebird
        :tickets: 2083

      The "implicit_returning" flag on create_engine() is
      honored if set to False.  (also in 0.6.7)

    .. change::
        :tags: informix
        :tickets: 2092

      Added RESERVED_WORDS informix dialect.
      (also in 0.6.7)

    .. change::
        :tags: ext
        :tickets: 2090

      The horizontal_shard ShardedSession class accepts the common
      Session argument "query_cls" as a constructor argument,
      to enable further subclassing of ShardedQuery. (also in 0.6.7)

    .. change::
        :tags: examples
        :tickets:

      Updated the association, association proxy examples
      to use declarative, added a new example
      dict_of_sets_with_default.py, a "pushing the envelope"
      example of association proxy.

    .. change::
        :tags: examples
        :tickets: 2090

      The Beaker caching example allows a "query_cls" argument
      to the query_callable() function.
      (also in 0.6.7)

.. changelog::
    :version: 0.7.0b2
    :released: Sat Feb 19 2011

    .. change::
        :tags: orm
        :tickets: 2053

      Fixed bug whereby Session.merge() would call the
      load() event with one too few arguments.

    .. change::
        :tags: orm
        :tickets: 2052

      Added logic which prevents the generation of
      events from a MapperExtension or SessionExtension
      from generating do-nothing events for all the methods
      not overridden.

    .. change::
        :tags: declarative
        :tickets: 2058

      Fixed regression whereby composite() with
      Column objects placed inline would fail
      to initialize.  The Column objects can now
      be inline with the composite() or external
      and pulled in via name or object ref.

    .. change::
        :tags: declarative
        :tickets: 2061

      Fix error message referencing old @classproperty
      name to reference @declared_attr
      (also in 0.6.7)

    .. change::
        :tags: declarative
        :tickets: 1468

      the dictionary at the end of the __table_args__
      tuple is now optional.

    .. change::
        :tags: sql
        :tickets: 2059

      Renamed the EngineEvents event class to
      ConnectionEvents.  As these classes are never
      accessed directly by end-user code, this strictly
      is a documentation change for end users.  Also
      simplified how events get linked to engines
      and connections internally.

    .. change::
        :tags: sql
        :tickets: 2055

      The Sequence() construct, when passed a MetaData()
      object via its 'metadata' argument, will be
      included in CREATE/DROP statements within
      metadata.create_all() and metadata.drop_all(),
      including "checkfirst" logic.

    .. change::
        :tags: sql
        :tickets: 2064

      The Column.references() method now returns True
      if it has a foreign key referencing the
      given column exactly, not just its parent
      table.

    .. change::
        :tags: postgresql
        :tickets: 2065

      Fixed regression from 0.6 where SMALLINT and
      BIGINT types would both generate SERIAL
      on an integer PK column, instead of
      SMALLINT and BIGSERIAL

    .. change::
        :tags: ext
        :tickets: 2054

      Association proxy now has correct behavior for
      any(), has(), and contains() when proxying
      a many-to-one scalar attribute to a one-to-many
      collection (i.e. the reverse of the 'typical'
      association proxy use case)

    .. change::
        :tags: examples
        :tickets:

      Beaker example now takes into account 'limit'
      and 'offset', bind params within embedded
      FROM clauses (like when you use union() or
      from_self()) when generating a cache key.

.. changelog::
    :version: 0.7.0b1
    :released: Sat Feb 12 2011

    .. change::
        :tags:
        :tickets:

      Detailed descriptions of each change below are
      described at:
      http://docs.sqlalchemy.org/en/latest/changelog/migration_07.html

    .. change::
        :tags: general
        :tickets: 1902

      New event system, supersedes all extensions, listeners,
      etc.

    .. change::
        :tags: general
        :tickets: 1926

      Logging enhancements

    .. change::
        :tags: general
        :tickets: 1949

      Setup no longer installs a Nose plugin

    .. change::
        :tags: general
        :tickets:

      The "sqlalchemy.exceptions" alias in sys.modules
      has been removed.   Base SQLA exceptions are
      available via "from sqlalchemy import exc".
      The "exceptions" alias for "exc" remains in
      "sqlalchemy" for now, it's just not patched into
      sys.modules.

    .. change::
        :tags: orm
        :tickets: 1923

      More succinct form of query.join(target, onclause)

    .. change::
        :tags: orm
        :tickets: 1903

      Hybrid Attributes, implements/supersedes synonym()

    .. change::
        :tags: orm
        :tickets: 2008

      Rewrite of composites

    .. change::
        :tags: orm
        :tickets:

      Mutation Event Extension, supersedes "mutable=True"

      .. seealso::

          :ref:`07_migration_mutation_extension`

    .. change::
        :tags: orm
        :tickets: 1980

      PickleType and ARRAY mutability turned off by default

    .. change::
        :tags: orm
        :tickets: 1895

      Simplified polymorphic_on assignment

    .. change::
        :tags: orm
        :tickets: 1912

      Flushing of Orphans that have no parent is allowed

    .. change::
        :tags: orm
        :tickets: 2041

      Adjusted flush accounting step to occur before
      the commit in the case of autocommit=True.  This allows
      autocommit=True to work appropriately with
      expire_on_commit=True, and also allows post-flush session
      hooks to operate in the same transactional context
      as when autocommit=False.

    .. change::
        :tags: orm
        :tickets: 1973

      Warnings generated when collection members, scalar referents
      not part of the flush

    .. change::
        :tags: orm
        :tickets: 1876

      Non-`Table`-derived constructs can be mapped

    .. change::
        :tags: orm
        :tickets: 1942

      Tuple label names in Query Improved

    .. change::
        :tags: orm
        :tickets: 1892

      Mapped column attributes reference the most specific
      column first

    .. change::
        :tags: orm
        :tickets: 1896

      Mapping to joins with two or more same-named columns
      requires explicit declaration

    .. change::
        :tags: orm
        :tickets: 1875

      Mapper requires that polymorphic_on column be present
      in the mapped selectable

    .. change::
        :tags: orm
        :tickets: 1966

      compile_mappers() renamed configure_mappers(), simplified
      configuration internals

    .. change::
        :tags: orm
        :tickets: 2018

      the aliased() function, if passed a SQL FromClause element
      (i.e. not a mapped class), will return element.alias()
      instead of raising an error on AliasedClass.

    .. change::
        :tags: orm
        :tickets: 2027

      Session.merge() will check the version id of the incoming
      state against that of the database, assuming the mapping
      uses version ids and incoming state has a version_id
      assigned, and raise StaleDataError if they don't
      match.

    .. change::
        :tags: orm
        :tickets: 1996

      Session.connection(), Session.execute() accept 'bind',
      to allow execute/connection operations to participate
      in the open transaction of an engine explicitly.

    .. change::
        :tags: orm
        :tickets:

      Query.join(), Query.outerjoin(), eagerload(),
      eagerload_all(), others no longer allow lists
      of attributes as arguments (i.e. option([x, y, z])
      form, deprecated since 0.5)

    .. change::
        :tags: orm
        :tickets:

      ScopedSession.mapper is removed (deprecated since 0.5).

    .. change::
        :tags: orm
        :tickets: 2031

      Horizontal shard query places 'shard_id' in
      context.attributes where it's accessible by the
      "load()" event.

    .. change::
        :tags: orm
        :tickets: 2032

      A single contains_eager() call across
      multiple entities will indicate all collections
      along that path should load, instead of requiring
      distinct contains_eager() calls for each endpoint
      (which was never correctly documented).

    .. change::
        :tags: orm
        :tickets:

      The "name" field used in orm.aliased() now renders
      in the resulting SQL statement.

    .. change::
        :tags: orm
        :tickets: 1473

      Session weak_instance_dict=False is deprecated.

    .. change::
        :tags: orm
        :tickets: 2046

      An exception is raised in the unusual case that an
      append or similar event on a collection occurs after
      the parent object has been dereferenced, which
      prevents the parent from being marked as "dirty"
      in the session.  Was a warning in 0.6.6.

    .. change::
        :tags: orm
        :tickets: 1069

      Query.distinct() now accepts column expressions
      as \*args, interpreted by the PostgreSQL dialect
      as DISTINCT ON (<expr>).

    .. change::
        :tags: orm
        :tickets: 2049

      Additional tuning to "many-to-one" relationship
      loads during a flush().   A change in version 0.6.6
      ([ticket:2002]) required that more "unnecessary" m2o
      loads during a flush could occur.   Extra loading modes have
      been added so that the SQL emitted in this
      specific use case is trimmed back, while still
      retrieving the information the flush needs in order
      to not miss anything.

    .. change::
        :tags: orm
        :tickets:

      the value of "passive" as passed to
      attributes.get_history() should be one of the
      constants defined in the attributes package.  Sending
      True or False is deprecated.

    .. change::
        :tags: orm
        :tickets: 2030

      Added a `name` argument to `Query.subquery()`, to allow
      a fixed name to be assigned to the alias object. (also in 0.6.7)

    .. change::
        :tags: orm
        :tickets: 2019

      A warning is emitted when a joined-table inheriting mapper
      has no primary keys on the locally mapped table
      (but has pks on the superclass table).
      (also in 0.6.7)

    .. change::
        :tags: orm
        :tickets: 2038

      Fixed bug where "middle" class in a polymorphic hierarchy
      would have no 'polymorphic_on' column if it didn't also
      specify a 'polymorphic_identity', leading to strange
      errors upon refresh, wrong class loaded when querying
      from that target. Also emits the correct WHERE criterion
      when using single table inheritance.
      (also in 0.6.7)

    .. change::
        :tags: orm
        :tickets: 1995

      Fixed bug where a column with a SQL or server side default
      that was excluded from a mapping with include_properties
      or exclude_properties would result in UnmappedColumnError. (also in 0.6.7)

    .. change::
        :tags: orm
        :tickets: 2046

      A warning is emitted in the unusual case that an
      append or similar event on a collection occurs after
      the parent object has been dereferenced, which
      prevents the parent from being marked as "dirty"
      in the session.  This will be an exception in 0.7. (also in 0.6.7)

    .. change::
        :tags: declarative
        :tickets: 2050

      Added an explicit check for the case that the name
      'metadata' is used for a column attribute on a
      declarative class. (also in 0.6.7)

    .. change::
        :tags: sql
        :tickets: 1844

      Added over() function, method to FunctionElement
      classes, produces the _Over() construct which
      in turn generates "window functions", i.e.
      "<window function> OVER (PARTITION BY <partition by>,
      ORDER BY <order by>)".

    .. change::
        :tags: sql
        :tickets: 805

      LIMIT/OFFSET clauses now use bind parameters

    .. change::
        :tags: sql
        :tickets: 1069

      select.distinct() now accepts column expressions
      as \*args, interpreted by the PostgreSQL dialect
      as DISTINCT ON (<expr>).  Note this was already
      available via passing a list to the `distinct`
      keyword argument to select().

    .. change::
        :tags: sql
        :tickets:

      select.prefix_with() accepts multiple expressions
      (i.e. \*expr), 'prefix' keyword argument to select()
      accepts a list or tuple.

    .. change::
        :tags: sql
        :tickets:

      Passing a string to the `distinct` keyword argument
      of `select()` for the purpose of emitting special
      MySQL keywords (DISTINCTROW etc.) is deprecated -
      use `prefix_with()` for this.

    .. change::
        :tags: sql
        :tickets: 2006, 2005

      TypeDecorator works with primary key columns

    .. change::
        :tags: sql
        :tickets: 1897

      DDL() constructs now escape percent signs

    .. change::
        :tags: sql
        :tickets: 1917, 1893

      Table.c / MetaData.tables refined a bit, don't allow direct
      mutation

    .. change::
        :tags: sql
        :tickets: 1950

      Callables passed to `bindparam()` don't get evaluated

    .. change::
        :tags: sql
        :tickets: 1870

      types.type_map is now private, types._type_map

    .. change::
        :tags: sql
        :tickets: 1982

      Non-public Pool methods underscored

    .. change::
        :tags: sql
        :tickets: 723

      Added NULLS FIRST and NULLS LAST support. It's implemented
      as an extension to the asc() and desc() operators, called
      nullsfirst() and nullslast().

    .. change::
        :tags: sql
        :tickets:

      The Index() construct can be created inline with a Table
      definition, using strings as column names, as an alternative
      to the creation of the index outside of the Table.

    .. change::
        :tags: sql
        :tickets: 2001

      execution_options() on Connection accepts
      "isolation_level" argument, sets transaction isolation
      level for that connection only until returned to the
      connection pool, for those backends which support it
      (SQLite, PostgreSQL)

    .. change::
        :tags: sql
        :tickets: 2005

      A TypeDecorator of Integer can be used with a primary key
      column, and the "autoincrement" feature of various dialects
      as well as the "sqlite_autoincrement" flag will honor
      the underlying database type as being Integer-based.

    .. change::
        :tags: sql
        :tickets: 2020, 2021

      Established consistency when server_default is present
      on an Integer PK column.  SQLA doesn't pre-fetch these,
      nor do they come back in cursor.lastrowid (DBAPI).
      Ensured all backends consistently return None
      in result.inserted_primary_key for these. Regarding
      reflection for this case, reflection of an int PK col
      with a server_default sets the "autoincrement" flag to False,
      except in the case of a PG SERIAL col where we detected a
      sequence default.

    .. change::
        :tags: sql
        :tickets: 2006

      Result-row processors are applied to pre-executed SQL
      defaults, as well as cursor.lastrowid, when determining
      the contents of result.inserted_primary_key.

    .. change::
        :tags: sql
        :tickets:

      Bind parameters present in the "columns clause" of a select
      are now auto-labeled like other "anonymous" clauses,
      which among other things allows their "type" to be meaningful
      when the row is fetched, as in result row processors.

    .. change::
        :tags: sql
        :tickets:

      TypeDecorator is present in the "sqlalchemy" import space.

    .. change::
        :tags: sql
        :tickets: 2015

      Non-DBAPI errors which occur in the scope of an `execute()`
      call are now wrapped in sqlalchemy.exc.StatementError,
      and the text of the SQL statement and repr() of params
      is included.  This makes it easier to identify statement
      executions which fail before the DBAPI becomes
      involved.

    .. change::
        :tags: sql
        :tickets: 2048

      The concept of associating a ".bind" directly with a
      ClauseElement has been explicitly moved to Executable,
      i.e. the mixin that describes ClauseElements which represent
      engine-executable constructs.  This change is an improvement
      to internal organization and is unlikely to affect any
      real-world usage.

    .. change::
        :tags: sql
        :tickets: 2028

      Column.copy(), as used in table.tometadata(), copies the
      'doc' attribute.  (also in 0.6.7)

    .. change::
        :tags: sql
        :tickets: 2023

      Added some defs to the resultproxy.c extension so that
      the extension compiles and runs on Python 2.4. (also in 0.6.7)

    .. change::
        :tags: sql
        :tickets: 2042

      The compiler extension now supports overriding the default
      compilation of expression._BindParamClause including that
      the auto-generated binds within the VALUES/SET clause
      of an insert()/update() statement will also use the new
      compilation rules. (also in 0.6.7)

    .. change::
        :tags: sql
        :tickets: 1921

      SQLite dialect now uses `NullPool` for file-based databases

    .. change::
        :tags: sql
        :tickets: 2036

      The path given as the location of a sqlite database is now
      normalized via os.path.abspath(), so that directory changes
      within the process don't affect the ultimate location
      of a relative file path.

    .. change::
        :tags: postgresql
        :tickets: 1083

      When explicit sequence execution derives the name
      of the auto-generated sequence of a SERIAL column,
      which currently only occurs if implicit_returning=False,
      now accommodates if the table + column name is greater
      than 63 characters using the same logic PostgreSQL uses. (also in 0.6.7)

    .. change::
        :tags: postgresql
        :tickets: 2044

      Added an additional libpq message to the list of "disconnect"
      exceptions, "could not receive data from server" (also in 0.6.7)

    .. change::
        :tags: mssql
        :tickets: 1833

      the String/Unicode types, and their counterparts VARCHAR/
      NVARCHAR, emit "max" as the length when no length is
      specified, so that the default length, normally '1'
      as per SQL server documentation, is instead
      'unbounded'.  This also occurs for the VARBINARY type..

      This behavior makes these types more closely compatible
      with PostgreSQL's VARCHAR type which is similarly unbounded
      when no length is specified.

    .. change::
        :tags: mysql
        :tickets: 1991

      New DBAPI support for pymysql, a pure Python port
      of MySQL-python.

    .. change::
        :tags: mysql
        :tickets: 2047

      oursql dialect accepts the same "ssl" arguments in
      create_engine() as that of MySQLdb.
      (also in 0.6.7)

    .. change::
        :tags: firebird
        :tickets: 1885

      Some adjustments so that Interbase is supported as well.
      FB/Interbase version idents are parsed into a structure
      such as (8, 1, 1, 'interbase') or (2, 1, 588, 'firebird')
      so they can be distinguished.
