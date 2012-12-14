
==============
0.8 Changelog
==============

.. changelog::
    :version: 0.8.0b2
    :released: December 14, 2012

    .. change::
        :tags: sqlite, bug
        :tickets: 2568

      More adjustment to this SQLite related issue which was released in
      0.7.9, to intercept legacy SQLite quoting characters when reflecting
      foreign keys.  In addition to intercepting double quotes, other
      quoting characters such as brackets, backticks, and single quotes
      are now also intercepted.  Also in 0.7.10.

    .. change::
        :tags: orm, bug
        :tickets: 2635

      The :meth:`.Query.select_from` method can now be used with a
      :func:`.aliased` construct without it interfering with the entities
      being selected.   Basically, a statement like this::

        ua = aliased(User)
        session.query(User.name).select_from(ua).join(User, User.name > ua.name)

      Will maintain the columns clause of the SELECT as coming from the
      unaliased "user", as specified; the select_from only takes place in the
      FROM clause::

        SELECT users.name AS users_name FROM users AS users_1
        JOIN users ON users.name < users_1.name

      Note that this behavior is in contrast
      to the original, older use case for :meth:`.Query.select_from`, which is that
      of restating the mapped entity in terms of a different selectable::

        session.query(User.name).\
          select_from(user_table.select().where(user_table.c.id > 5))

      Which produces::

        SELECT anon_1.name AS anon_1_name FROM (SELECT users.id AS id,
        users.name AS name FROM users WHERE users.id > :id_1) AS anon_1

      It was the "aliasing" behavior of the latter use case that was
      getting in the way of the former use case.   The method now
      specifically considers a SQL expression like
      :func:`.expression.select` or :func:`.expression.alias`
      separately from a mapped entity like a :func:`.aliased`
      construct.

    .. change::
        :tags: sql, bug
        :tickets: 2633

      Fixed a regression caused by :ticket:`2410` whereby a
      :class:`.CheckConstraint` would apply itself back to the
      original table during a :meth:`.Table.tometadata` operation, as
      it would parse the SQL expression for a parent table. The
      operation now copies the given expression to correspond to the
      new table.

    .. change::
        :tags: oracle, bug
        :tickets: 2619

      Fixed table reflection for Oracle when accessing a synonym that refers
      to a DBLINK remote database; while the syntax has been present in the
      Oracle dialect for some time, up until now it has never been tested.
      The syntax has been tested against a sample database linking to itself,
      however there's still some uncertainty as to what should be used for the
      "owner" when querying the remote database for table information.
      Currently, the value of "username" from user_db_links is used to
      match the "owner".

    .. change::
        :tags: orm, feature
        :tickets: 2601

      Added :meth:`.KeyedTuple._asdict` and :attr:`.KeyedTuple._fields`
      to the :class:`.KeyedTuple` class to provide some degree of compatibility
      with the Python standard library ``collections.namedtuple()``.

    .. change::
        :tags: sql, bug
        :tickets: 2631

      Fixed bug where using server_onupdate=<FetchedValue|DefaultClause>
      without passing the "for_update=True" flag would apply the default
      object to the server_default, blowing away whatever was there.
      The explicit for_update=True argument shouldn't be needed with this usage
      (especially since the documentation shows an example without it being
      used) so it is now arranged internally using a copy of the given default
      object, if the flag isn't set to what corresponds to that argument.
      Also in 0.7.10.

    .. change::
        :tags: sql, bug
        :tickets: 2610

      Fixed bug whereby using a label_length on dialect that was smaller
      than the size of actual column identifiers would fail to render
      the columns correctly in a SELECT statement.

    .. change::
        :tags: sql, feature
        :tickets: 2623

      The :class:`.Insert` construct now supports multi-valued inserts,
      that is, an INSERT that renders like
      "INSERT INTO table VALUES (...), (...), ...".
      Supported by Postgresql, SQLite, and MySQL.
      Big thanks to Idan Kamara for doing the legwork on this one.

    .. change::
        :tags: oracle, bug
        :tickets: 2620

      The Oracle LONG type, while an unbounded text type, does not appear
      to use the cx_Oracle.LOB type when result rows are returned,
      so the dialect has been repaired to exclude LONG from
      having cx_Oracle.LOB filtering applied.  Also in 0.7.10.

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
      for details.  Also in 0.7.10.

    .. change::
        :tags: sql, bug
        :tickets: 2618

      The :class:`.DECIMAL` type now honors the "precision" and
      "scale" arguments when rendering DDL.

    .. change::
        :tags: orm, bug
        :tickets: 2624

      The :class:`.MutableComposite` type did not allow for the
      :meth:`.MutableBase.coerce` method to be used, even though
      the code seemed to indicate this intent, so this now works
      and a brief example is added.  As a side-effect,
      the mechanics of this event handler have been changed so that
      new :class:`.MutableComposite` types no longer add per-type
      global event handlers.  Also in 0.7.10.

    .. change::
        :tags: sql, bug
        :tickets: 2621

      Made an adjustment to the "boolean", (i.e. ``__nonzero__``)
      evaluation of binary expressions, i.e. ``x1 == x2``, such
      that the "auto-grouping" applied by :class:`.BinaryExpression`
      in some cases won't get in the way of this comparison.
      Previously, an expression like::

        expr1 = mycolumn > 2
        bool(expr1 == expr1)

      Would evaulate as ``False``, even though this is an identity
      comparison, because ``mycolumn > 2`` would be "grouped" before
      being placed into the :class:`.BinaryExpression`, thus changing
      its identity.   :class:`.BinaryExpression` now keeps track
      of the "original" objects passed in.
      Additionally the ``__nonzero__`` method now only returns if
      the operator is ``==`` or ``!=`` - all others raise ``TypeError``.

    .. change::
        :tags: firebird, bug
        :tickets: 2622

      Added missing import for "fdb" to the experimental
      "firebird+fdb" dialect.

    .. change::
        :tags: orm, feature

      Allow synonyms to be used when defining primary and secondary
      joins for relationships.

    .. change::
        :tags: orm, bug
        :tickets: 2614

      A second overhaul of aliasing/internal pathing mechanics
      now allows two subclasses to have different relationships
      of the same name, supported with subquery or joined eager
      loading on both simultaneously when a full polymorphic
      load is used.

    .. change::
        :tags: orm, bug
        :tickets: 2617

      Fixed bug whereby a multi-hop subqueryload within
      a particular with_polymorphic load would produce a KeyError.
      Takes advantage of the same internal pathing overhaul
      as :ticket:`2614`.

    .. change::
        :tags: sql, bug

      Fixed a gotcha where inadvertently calling list() on a
      :class:`.ColumnElement` would go into an endless loop, if
      :meth:`.ColumnOperators.__getitem__` were implemented.
      A new NotImplementedError is emitted via ``__iter__()``.

    .. change::
        :tags: orm, extensions, feature

      The :mod:`sqlalchemy.ext.mutable` extension now includes the
      example :class:`.MutableDict` class as part of the extension.

    .. change::
        :tags: postgresql, feature
        :tickets: 2606

      :class:`.HSTORE` is now available in the Postgresql dialect.
      Will also use psycopg2's extensions if available.  Courtesy
      Audrius Kažukauskas.

    .. change::
        :tags: sybase, feature
        :tickets: 1753

      Reflection support has been added to the Sybase dialect.
      Big thanks to Ben Trofatter for all the work developing and
      testing this.

    .. change::
        :tags: engine, feature

      The :meth:`.Connection.connect` and :meth:`.Connection.contextual_connect`
      methods now return a "branched" version so that the :meth:`.Connection.close`
      method can be called on the returned connection without affecting the
      original.   Allows symmetry when using :class:`.Engine` and
      :class:`.Connection` objects as context managers::

        with conn.connect() as c: # leaves the Connection open
          c.execute("...")

        with engine.connect() as c:  # closes the Connection
          c.execute("...")

    .. change::
        :tags: engine

      The "reflect=True" argument to :class:`MetaData` is deprecated.
      Please use the :meth:`.MetaData.reflect` method.

    .. change::
        :tags: engine, bug
        :tickets: 2604

      Fixed :meth:`.MetaData.reflect` to correctly use
      the given :class:`.Connection`, if given, without
      opening a second connection from that connection's
      :class:`.Engine`.  Also in 0.7.10.

    .. change::
        :tags: mssql, bug
        :tickets: 2607

      Fixed bug whereby using "key" with Column
      in conjunction with "schema" for the owning
      Table would fail to locate result rows due
      to the MSSQL dialect's "schema rendering"
      logic's failure to take .key into account.
      Also in 0.7.10.

    .. change::
        :tags: sql, bug
        :tickets: 2603

        Fixed bug in type_coerce() whereby typing information
        could be lost if the statement were used as a subquery
        inside of another statement, as well as other similar
        situations.  Among other things, would cause
        typing information to be lost when the Oracle/mssql dialects
        would apply limit/offset wrappings.

    .. change::
        :tags: orm, bug
        :tickets: 2602

        Fixed regression where query.update() would produce
        an error if an object matched by the "fetch"
        synchronization strategy wasn't locally present.
        Courtesy Scott Torborg.

    .. change::
        :tags: sql, bug
        :tickets: 2597

        Fixed bug whereby the ".key" of a Column wasn't being
        used when producing a "proxy" of the column against
        a selectable.   This probably didn't occur in 0.7
        since 0.7 doesn't respect the ".key" in a wider
        range of scenarios.

    .. change::
        :tags: mssql, feature
        :tickets: 2600

        Support for reflection of the "name" of primary key
        constraints added, courtesy Dave Moore.

    .. change::
        :tags: informix

        Some cruft regarding informix transaction handling has been
        removed, including a feature that would skip calling
        commit()/rollback() as well as some hardcoded isolation level
        assumptions on begin()..   The status of this dialect is not
        well understood as we don't have any users working with it,
        nor any access to an Informix database.   If someone with
        access to Informix wants to help test this dialect, please
        let us know.

    .. change::
        :tags: pool, feature

        The :class:`.Pool` will now log all connection.close()
        operations equally, including closes which occur for
        invalidated connections, detached connections, and connections
        beyond the pool capacity.

    .. change::
        :tags: pool, feature
        :tickets: 2611

        The :class:`.Pool` now consults the :class:`.Dialect` for
        functionality regarding how the connection should be
        "auto rolled back", as well as closed.   This grants more
        control of transaction scope to the dialect, so that we
        will be better able to implement transactional workarounds
        like those potentially needed for pysqlite and cx_oracle.

    .. change::
        :tags: pool, feature

        Added new :meth:`.PoolEvents.reset` hook to capture
        the event before a connection is auto-rolled back, upon
        return to the pool.   Together with
        :meth:`.ConnectionEvents.rollback` this allows all rollback
        events to be intercepted.

.. changelog::
    :version: 0.8.0b1
    :released: October 30, 2012

    .. change::
        :tags: sql, bug
        :tickets: 2593

        Fixed bug where keyword arguments passed to
        :meth:`.Compiler.process` wouldn't get propagated
        to the column expressions present in the columns
        clause of a SELECT statement.  In particular this would
        come up when used by custom compilation schemes that
        relied upon special flags.

    .. change::
        :tags: sql, feature

      Added a new method :meth:`.Engine.execution_options`
      to :class:`.Engine`.  This method works similarly to
      :meth:`.Connection.execution_options` in that it creates
      a copy of the parent object which will refer to the new
      set of options.   The method can be used to build
      sharding schemes where each engine shares the same
      underlying pool of connections.   The method
      has been tested against the horizontal shard
      recipe in the ORM as well.

      .. seealso::

          :meth:`.Engine.execution_options`

    .. change::
        :tags: sql, orm, bug
        :tickets: 2595

      The auto-correlation feature of :func:`.select`, and
      by proxy that of :class:`.orm.Query`, will not
      take effect for a SELECT statement that is being
      rendered directly in the FROM list of the enclosing
      SELECT.  Correlation in SQL only applies to column
      expressions such as those in the WHERE, ORDER BY,
      columns clause.

    .. change::
        :tags: sqlite
        :pullreq: 23
        :changeset: c3addcc9ffad

      Added :class:`.types.NCHAR`, :class:`.types.NVARCHAR`
      to the SQLite dialect's list of recognized type names
      for reflection.   SQLite returns the name given
      to a type as the name returned.

    .. change::
        :tags: examples
        :tickets: 2589

      The Beaker caching example has been converted
      to use `dogpile.cache <http://dogpilecache.readthedocs.org/>`_.
      This is a new caching library written by the same
      creator of Beaker's caching internals, and represents a
      vastly improved, simplified, and modernized system of caching.

      .. seealso::

          :ref:`examples_caching`

    .. change::
        :tags: general
        :tickets:

      SQLAlchemy 0.8 now targets Python 2.5 and
      above.  Python 2.4 is no longer supported.

    .. change::
        :tags: removed, general
        :tickets: 2433

      The "sqlalchemy.exceptions"
      synonym for "sqlalchemy.exc" is removed
      fully.

    .. change::
        :tags: removed, orm
        :tickets: 2442

      The legacy "mutable" system of the
      ORM, including the MutableType class as well
      as the mutable=True flag on PickleType
      and postgresql.ARRAY has been removed.
      In-place mutations are detected by the ORM
      using the sqlalchemy.ext.mutable extension,
      introduced in 0.7.   The removal of MutableType
      and associated constructs removes a great
      deal of complexity from SQLAlchemy's internals.
      The approach performed poorly as it would incur
      a scan of the full contents of the Session
      when in use.

    .. change::
        :tags: orm, moved
        :tickets:

      The InstrumentationManager interface
      and the entire related system of alternate
      class implementation is now moved out
      to sqlalchemy.ext.instrumentation.   This is
      a seldom used system that adds significant
      complexity and overhead to the mechanics of
      class instrumentation.  The new architecture
      allows it to remain unused until
      InstrumentationManager is actually imported,
      at which point it is bootstrapped into
      the core.

    .. change::
        :tags: orm, feature
        :tickets: 1401

      Major rewrite of relationship()
      internals now allow join conditions which
      include columns pointing to themselves
      within composite foreign keys.   A new
      API for very specialized primaryjoin conditions
      is added, allowing conditions based on
      SQL functions, CAST, etc. to be handled
      by placing the annotation functions
      remote() and foreign() inline within the
      expression when necessary.  Previous recipes
      using the semi-private _local_remote_pairs
      approach can be upgraded to this new
      approach.

      .. seealso::

          :ref:`feature_relationship_08`

    .. change::
        :tags: orm, bug
        :tickets: 2527

      ORM will perform extra effort to determine
      that an FK dependency between two tables is
      not significant during flush if the tables
      are related via joined inheritance and the FK
      dependency is not part of the inherit_condition,
      saves the user a use_alter directive.

    .. change::
        :tags: orm, feature
        :tickets: 2333

      New standalone function with_polymorphic()
      provides the functionality of query.with_polymorphic()
      in a standalone form.   It can be applied to any
      entity within a query, including as the target
      of a join in place of the "of_type()" modifier.

    .. change::
        :tags: orm, feature
        :tickets: 1106, 2438

      The of_type() construct on attributes
      now accepts aliased() class constructs as well
      as with_polymorphic constructs, and works with
      query.join(), any(), has(), and also
      eager loaders subqueryload(), joinedload(),
      contains_eager()

    .. change::
        :tags: orm, feature
        :tickets: 2585

      Improvements to event listening for
      mapped classes allows that unmapped classes
      can be specified for instance- and mapper-events.
      The established events will be automatically
      set up on subclasses of that class when the
      propagate=True flag is passed, and the
      events will be set up for that class itself
      if and when it is ultimately mapped.

    .. change::
        :tags: orm, bug
        :tickets: 2590

      The instrumentation events class_instrument(),
      class_uninstrument(), and attribute_instrument()
      will now fire off only for descendant classes
      of the class assigned to listen().  Previously,
      an event listener would be assigned to listen
      for all classes in all cases regardless of the
      "target" argument passed.

    .. change::
        :tags: orm, bug
        :tickets: 1900

      with_polymorphic() produces JOINs
      in the correct order and with correct inheriting
      tables in the case of sending multi-level
      subclasses in an arbitrary order or with
      intermediary classes missing.

    .. change::
        :tags: orm, feature
        :tickets: 2485

      The "deferred declarative
      reflection" system has been moved into the
      declarative extension itself, using the
      new DeferredReflection class.  This
      class is now tested with both single
      and joined table inheritance use cases.

    .. change::
        :tags: orm, feature
        :tickets: 2208

      Added new core function "inspect()",
      which serves as a generic gateway to
      introspection into mappers, objects,
      others.   The Mapper and InstanceState
      objects have been enhanced with a public
      API that allows inspection of mapped
      attributes, including filters for column-bound
      or relationship-bound properties, inspection
      of current object state, history of
      attributes, etc.

    .. change::
        :tags: orm, feature
        :tickets: 2452

      Calling rollback() within a
      session.begin_nested() will now only expire
      those objects that had net changes within the
      scope of that transaction, that is objects which
      were dirty or were modified on a flush.  This
      allows the typical use case for begin_nested(),
      that of altering a small subset of objects, to
      leave in place the data from the larger enclosing
      set of objects that weren't modified in
      that sub-transaction.

    .. change::
        :tags: orm, feature
        :tickets: 2372

      Added utility feature
      Session.enable_relationship_loading(),
      supersedes relationship.load_on_pending.
      Both features should be avoided, however.

    .. change::
        :tags: orm, feature
        :tickets:

      Added support for .info dictionary argument to
      column_property(), relationship(), composite().
      All MapperProperty classes have an auto-creating .info
      dict available overall.

    .. change::
        :tags: orm, feature
        :tickets: 2229

      Adding/removing None from a mapped collection
      now generates attribute events.  Previously, a None
      append would be ignored in some cases.  Related
      to.

    .. change::
        :tags: orm, feature
        :tickets: 2229

      The presence of None in a mapped collection
      now raises an error during flush.   Previously,
      None values in collections would be silently ignored.

    .. change::
        :tags: orm, feature
        :tickets:

      The Query.update() method is now
      more lenient as to the table
      being updated.  Plain Table objects are better
      supported now, and additional a joined-inheritance
      subclass may be used with update(); the subclass
      table will be the target of the update,
      and if the parent table is referenced in the
      WHERE clause, the compiler will call upon
      UPDATE..FROM syntax as allowed by the dialect
      to satisfy the WHERE clause.  MySQL's multi-table
      update feature is also supported if columns
      are specified by object in the "values" dicitionary.
      PG's DELETE..USING is also not available
      in Core yet.

    .. change::
        :tags: orm, feature
        :tickets:

      New session events after_transaction_create
      and after_transaction_end
      allows tracking of new SessionTransaction objects.
      If the object is inspected, can be used to determine
      when a session first becomes active and when
      it deactivates.

    .. change::
        :tags: orm, feature
        :tickets: 2592

      The Query can now load entity/scalar-mixed
      "tuple" rows that contain
      types which aren't hashable, by setting the flag
      "hashable=False" on the corresponding TypeEngine object
      in use.  Custom types that return unhashable types
      (typically lists) can set this flag to False.

    .. change::
        :tags: orm, bug
        :tickets: 2481

      Improvements to joined/subquery eager
      loading dealing with chains of subclass entities
      sharing a common base, with no specific "join depth"
      provided.  Will chain out to
      each subclass mapper individually before detecting
      a "cycle", rather than considering the base class
      to be the source of the "cycle".

    .. change::
        :tags: orm, bug
        :tickets: 2320

      The "passive" flag on Session.is_modified()
      no longer has any effect. is_modified() in
      all cases looks only at local in-memory
      modified flags and will not emit any
      SQL or invoke loader callables/initializers.

    .. change::
        :tags: orm, bug
        :tickets: 2405

      The warning emitted when using
      delete-orphan cascade with one-to-many
      or many-to-many without single-parent=True
      is now an error.  The ORM
      would fail to function subsequent to this
      warning in any case.

    .. change::
        :tags: orm, bug
        :tickets: 2350

      Lazy loads emitted within flush events
      such as before_flush(), before_update(),
      etc. will now function as they would
      within non-event code, regarding consideration
      of the PK/FK values used in the lazy-emitted
      query.   Previously,
      special flags would be established that
      would cause lazy loads to load related items
      based on the "previous" value of the
      parent PK/FK values specifically when called
      upon within a flush; the signal to load
      in this way is now localized to where the
      unit of work actually needs to load that
      way.  Note that the UOW does
      sometimes load these collections before
      the before_update() event is called,
      so the usage of "passive_updates" or not
      can affect whether or not a collection will
      represent the "old" or "new" data, when
      accessed within a flush event, based
      on when the lazy load was emitted.
      The change is backwards incompatible in
      the exceedingly small chance that
      user event code depended on the old
      behavior.

    .. change::
        :tags: orm, feature
        :tickets: 2179

      Query now "auto correlates" by
      default in the same way as select() does.
      Previously, a Query used as a subquery
      in another would require the correlate()
      method be called explicitly in order to
      correlate a table on the inside to the
      outside.  As always, correlate(None)
      disables correlation.

    .. change::
        :tags: orm, feature
        :tickets: 2464

      The after_attach event is now
      emitted after the object is established
      in Session.new or Session.identity_map
      upon Session.add(), Session.merge(),
      etc., so that the object is represented
      in these collections when the event
      is called.  Added before_attach
      event to accommodate use cases that
      need autoflush w pre-attached object.

    .. change::
        :tags: orm, feature
        :tickets:

      The Session will produce warnings
      when unsupported methods are used inside the
      "execute" portion of the flush.   These are
      the familiar methods add(), delete(), etc.
      as well as collection and related-object
      manipulations, as called within mapper-level
      flush events
      like after_insert(), after_update(), etc.
      It's been prominently documented for a long
      time that  SQLAlchemy cannot guarantee
      results when the Session is manipulated within
      the execution of the flush plan,
      however users are still doing it, so now
      there's a warning.   Maybe someday the Session
      will be enhanced to support these operations
      inside of the flush, but for now, results
      can't be guaranteed.

    .. change::
        :tags: orm, bug
        :tickets: 2582, 2566

      Continuing regarding extra
      state post-flush due to event listeners;
      any states that are marked as "dirty" from an
      attribute perspective, usually via column-attribute
      set events within after_insert(), after_update(),
      etc., will get the "history" flag reset
      in all cases, instead of only those instances
      that were part of the flush.  This has the effect
      that this "dirty" state doesn't carry over
      after the flush and won't result in UPDATE
      statements.   A warning is emitted to this
      effect; the set_committed_state()
      method can be used to assign attributes on objects
      without producing history events.

    .. change::
        :tags: orm, feature
        :tickets: 2245

      ORM entities can be passed
      to the core select() construct as well
      as to the select_from(),
      correlate(), and correlate_except()
      methods of select(), where they will be unwrapped
      into selectables.

    .. change::
        :tags: orm, feature
        :tickets: 2245

      Some support for auto-rendering of a
      relationship join condition based on the mapped
      attribute, with usage of core SQL constructs.
      E.g. select([SomeClass]).where(SomeClass.somerelationship)
      would render SELECT from "someclass" and use the
      primaryjoin of "somerelationship" as the WHERE
      clause.   This changes the previous meaning
      of "SomeClass.somerelationship" when used in a
      core SQL context; previously, it would "resolve"
      to the parent selectable, which wasn't generally
      useful.  Also works with query.filter().
      Related to.

    .. change::
        :tags: orm, feature
        :tickets: 2526

      The registry of classes
      in declarative_base() is now a
      WeakValueDictionary.  So subclasses of
      "Base" that are dereferenced will be
      garbage collected, *if they are not
      referred to by any other mappers/superclass
      mappers*. See the next note for this ticket.

    .. change::
        :tags: orm, feature
        :tickets: 2472

      Conflicts between columns on
      single-inheritance declarative subclasses,
      with or without using a mixin, can be resolved
      using a new @declared_attr usage described
      in the documentation.

    .. change::
        :tags: orm, feature
        :tickets: 2472

      declared_attr can now be used
      on non-mixin classes, even though this is generally
      only useful for single-inheritance subclass
      column conflict resolution.

    .. change::
        :tags: orm, feature
        :tickets: 2517

      declared_attr can now be used with
      attributes that are not Column or MapperProperty;
      including any user-defined value as well
      as association proxy objects.

    .. change::
        :tags: orm, bug
        :tickets: 2565

      Fixed a disconnect that slowly evolved
      between a @declared_attr Column and a
      directly-defined Column on a mixin. In both
      cases, the Column will be applied to the
      declared class' table, but not to that of a
      joined inheritance subclass.   Previously,
      the directly-defined Column would be placed
      on both the base and the sub table, which isn't
      typically what's desired.

    .. change::
        :tags: orm, feature
        :tickets: 2526

      *Very limited* support for
      inheriting mappers to be GC'ed when the
      class itself is deferenced.  The mapper
      must not have its own table (i.e.
      single table inh only) without polymorphic
      attributes in place.
      This allows for the use case of
      creating a temporary subclass of a declarative
      mapped class, with no table or mapping
      directives of its own, to be garbage collected
      when dereferenced by a unit test.

    .. change::
        :tags: orm, feature
        :tickets: 2338

      Declarative now maintains a registry
      of classes by string name as well as by full
      module-qualified name.   Multiple classes with the
      same name can now be looked up based on a module-qualified
      string within relationship().   Simple class name
      lookups where more than one class shares the same
      name now raises an informative error message.

    .. change::
        :tags: orm, feature
        :tickets: 2535

      Can now provide class-bound attributes
      that override columns which are of any
      non-ORM type, not just descriptors.

    .. change::
        :tags: orm, feature
        :tickets: 1729

      Added with_labels and
      reduce_columns keyword arguments to
      Query.subquery(), to provide two alternate
      strategies for producing queries with uniquely-
      named columns. .

    .. change::
        :tags: orm, feature
        :tickets: 2476

      A warning is emitted when a reference
      to an instrumented collection is no longer
      associated with the parent class due to
      expiration/attribute refresh/collection
      replacement, but an append
      or remove operation is received on the
      now-detached collection.

    .. change::
        :tags: orm, bug
        :tickets: 2549

      Declarative can now propagate a column
      declared on a single-table inheritance subclass
      up to the parent class' table, when the parent
      class is itself mapped to a join() or select()
      statement, directly or via joined inheritance,
      and not just a Table.

    .. change::
        :tags: orm, bug
        :tickets:

      An error is emitted when uselist=False
      is combined with a "dynamic" loader.
      This is a warning in 0.7.9.

    .. change::
        :tags: removed, orm
        :tickets:

      Deprecated identifiers removed:

      * allow_null_pks mapper() argument
        (use allow_partial_pks)

      * _get_col_to_prop() mapper method
        (use get_property_by_column())

      * dont_load argument to Session.merge()
        (use load=True)

      * sqlalchemy.orm.shard module
        (use sqlalchemy.ext.horizontal_shard)

    .. change::
        :tags: engine, feature
        :tickets: 2511

      Connection event listeners can
      now be associated with individual
      Connection objects, not just Engine
      objects.

    .. change::
        :tags: engine, feature
        :tickets: 2459

      The before_cursor_execute event
      fires off for so-called "_cursor_execute"
      events, which are usually special-case
      executions of primary-key bound sequences
      and default-generation SQL
      phrases that invoke separately when RETURNING
      is not used with INSERT.

    .. change::
        :tags: engine, feature
        :tickets:

      The libraries used by the test suite
      have been moved around a bit so that they are
      part of the SQLAlchemy install again.  In addition,
      a new suite of tests is present in the
      new sqlalchemy.testing.suite package.  This is
      an under-development system that hopes to provide
      a universal testing suite for external dialects.
      Dialects which are maintained outside of SQLAlchemy
      can use the new test fixture as the framework
      for their own tests, and will get for free a
      "compliance" suite of dialect-focused tests,
      including an improved "requirements" system
      where specific capabilities and features can
      be enabled or disabled for testing.

    .. change::
        :tags: engine, bug
        :tickets:

      The Inspector.get_table_names()
      order_by="foreign_key" feature now sorts
      tables by dependee first, to be consistent
      with util.sort_tables and metadata.sorted_tables.

    .. change::
        :tags: engine, bug
        :tickets: 2522

      Fixed bug whereby if a database restart
      affected multiple connections, each
      connection would individually invoke a new
      disposal of the pool, even though only
      one disposal is needed.

    .. change::
        :tags: engine, feature
        :tickets: 2462

      Added a new system
      for registration of new dialects in-process
      without using an entrypoint.  See the
      docs for "Registering New Dialects".

    .. change::
        :tags: engine, feature
        :tickets: 2556

      The "required" flag is set to
      True by default, if not passed explicitly,
      on bindparam() if the "value" or "callable"
      parameters are not passed.
      This will cause statement execution to check
      for the parameter being present in the final
      collection of bound parameters, rather than
      implicitly assigning None.

    .. change::
        :tags: engine, feature
        :tickets:

      Various API tweaks to the "dialect"
      API to better support highly specialized
      systems such as the Akiban database, including
      more hooks to allow an execution context to
      access type processors.

    .. change::
        :tags: engine, bug
        :tickets: 2397

      The names of the columns on the
      .c. attribute of a select().apply_labels()
      is now based on <tablename>_<colkey> instead
      of <tablename>_<colname>, for those columns
      that have a distinctly named .key.

    .. change::
        :tags: engine, feature
        :tickets: 2422

      Inspector.get_primary_keys() is
      deprecated; use Inspector.get_pk_constraint().
      Courtesy Diana Clarke.

    .. change::
        :tags: engine, bug
        :tickets:

      The autoload_replace flag on Table,
      when False, will cause any reflected foreign key
      constraints which refer to already-declared
      columns to be skipped, assuming that the
      in-Python declared column will take over
      the task of specifying in-Python ForeignKey
      or ForeignKeyConstraint declarations.

    .. change::
        :tags: engine, bug
        :tickets: 2498

      The ResultProxy methods inserted_primary_key,
      last_updated_params(), last_inserted_params(),
      postfetch_cols(), prefetch_cols() all
      assert that the given statement is a compiled
      construct, and is an insert() or update()
      statement as is appropriate, else
      raise InvalidRequestError.

    .. change::
        :tags: engine, feature
        :tickets:

      New C extension module "utils" has
      been added for additional function speedups
      as we have time to implement.

    .. change::
        :tags: engine
        :tickets:

      ResultProxy.last_inserted_ids is removed,
      replaced by inserted_primary_key.

    .. change::
        :tags: feature, sql
        :tickets: 2547

      Major rework of operator system
      in Core, to allow redefinition of existing
      operators as well as addition of new operators
      at the type level.  New types can be created
      from existing ones which add or redefine
      operations that are exported out to column
      expressions, in a similar manner to how the
      ORM has allowed comparator_factory.   The new
      architecture moves this capability into the
      Core so that it is consistently usable in
      all cases, propagating cleanly using existing
      type propagation behavior.

    .. change::
        :tags: feature, sql
        :tickets: 1534, 2547

      To complement, types
      can now provide "bind expressions" and
      "column expressions" which allow compile-time
      injection of SQL expressions into statements
      on a per-column or per-bind level.   This is
      to suit the use case of a type which needs
      to augment bind- and result- behavior at the
      SQL level, as opposed to in the Python level.
      Allows for schemes like transparent encryption/
      decryption, usage of Postgis functions, etc.

    .. change::
        :tags: feature, sql
        :tickets:

      The Core oeprator system now includes
      the `getitem` operator, i.e. the bracket
      operator in Python.  This is used at first
      to provide index and slice behavior to the
      Postgresql ARRAY type, and also provides a hook
      for end-user definition of custom __getitem__
      schemes which can be applied at the type
      level as well as within ORM-level custom
      operator schemes.   `lshift` (<<)
      and `rshift` (>>) are also supported as
      optional operators.

      Note that this change has the effect that
      descriptor-based __getitem__ schemes used by
      the ORM in conjunction with synonym() or other
      "descriptor-wrapped" schemes will need
      to start using a custom comparator in order
      to maintain this behavior.

    .. change::
        :tags: feature, sql
        :tickets: 2537

      Revised the rules used to determine
      the operator precedence for the user-defined
      operator, i.e. that granted using the ``op()``
      method.   Previously, the smallest precedence
      was applied in all cases, now the default
      precedence is zero, lower than all operators
      except "comma" (such as, used in the argument
      list of a ``func`` call) and "AS", and is
      also customizable via the "precedence" argument
      on the ``op()`` method.

    .. change::
        :tags: feature, sql
        :tickets: 2276

      Added "collation" parameter to all
      String types.  When present, renders as
      COLLATE <collation>.  This to support the
      COLLATE keyword now supported by several
      databases including MySQL, SQLite, and Postgresql.

    .. change::
        :tags: change, sql
        :tickets:

      The Text() type renders the length
      given to it, if a length was specified.

    .. change::
        :tags: feature, sql
        :tickets:

      Custom unary operators can now be
      used by combining operators.custom_op() with
      UnaryExpression().

    .. change::
        :tags: bug, sql
        :tickets: 2564

      A tweak to column precedence which moves the
      "concat" and "match" operators to be the same as
      that of "is", "like", and others; this helps with
      parenthesization rendering when used in conjunction
      with "IS".

    .. change::
        :tags: feature, sql
        :tickets:

      Enhanced GenericFunction and func.*
      to allow for user-defined GenericFunction
      subclasses to be available via the func.*
      namespace automatically by classname,
      optionally using a package name, as well
      as with the ability to have the rendered
      name different from the identified name
      in func.*.

    .. change::
        :tags: feature, sql
        :tickets: 2562

      The cast() and extract() constructs
      will now be produced via the func.* accessor
      as well, as users naturally try to access these
      names from func.* they might as well do
      what's expected, even though the returned
      object is not a FunctionElement.

    .. change::
        :tags: changed, sql
        :tickets:

      Most classes in expression.sql
      are no longer preceded with an underscore,
      i.e. Label, SelectBase, Generative, CompareMixin.
      _BindParamClause is also renamed to
      BindParameter.   The old underscore names for
      these classes will remain available as synonyms
      for the foreseeable future.

    .. change::
        :tags: feature, sql
        :tickets: 2208

      The Inspector object can now be
      acquired using the new inspect() service,
      part of

    .. change::
        :tags: feature, sql
        :tickets: 2418

      The column_reflect event now
      accepts the Inspector object as the first
      argument, preceding "table".   Code which
      uses the 0.7 version of this very new
      event will need modification to add the
      "inspector" object as the first argument.

    .. change::
        :tags: feature, sql
        :tickets: 2423

      The behavior of column targeting
      in result sets is now case sensitive by
      default.   SQLAlchemy for many years would
      run a case-insensitive conversion on these values,
      probably to alleviate early case sensitivity
      issues with dialects like Oracle and
      Firebird.   These issues have been more cleanly
      solved in more modern versions so the performance
      hit of calling lower() on identifiers is removed.
      The case insensitive comparisons can be re-enabled
      by setting "case_insensitive=False" on
      create_engine().

    .. change::
        :tags: bug, sql
        :tickets: 2591

      Applying a column expression to a select
      statement using a label with or without other
      modifying constructs will no longer "target" that
      expression to the underlying Column; this affects
      ORM operations that rely upon Column targeting
      in order to retrieve results.  That is, a query
      like query(User.id, User.id.label('foo')) will now
      track the value of each "User.id" expression separately
      instead of munging them together.  It is not expected
      that any users will be impacted by this; however,
      a usage that uses select() in conjunction with
      query.from_statement() and attempts to load fully
      composed ORM entities may not function as expected
      if the select() named Column objects with arbitrary
      .label() names, as these will no longer target to
      the Column objects mapped by that entity.

    .. change::
        :tags: feature, sql
        :tickets: 2415

      The "unconsumed column names" warning emitted
      when keys are present in insert.values() or update.values()
      that aren't in the target table is now an exception.

    .. change::
        :tags: feature, sql
        :tickets: 2502

      Added "MATCH" clause to ForeignKey,
      ForeignKeyConstraint, courtesy Ryan Kelly.

    .. change::
        :tags: feature, sql
        :tickets: 2507

      Added support for DELETE and UPDATE from
      an alias of a table, which would assumedly
      be related to itself elsewhere in the query,
      courtesy Ryan Kelly.

    .. change::
        :tags: feature, sql
        :tickets:

      select() features a correlate_except()
      method, auto correlates all selectables except those
      passed.

    .. change::
        :tags: feature, sql
        :tickets: 2431

      The prefix_with() method is now available
      on each of select(), insert(), update(), delete(),
      all with the same API, accepting multiple
      prefix calls, as well as a "dialect name" so that
      the prefix can be limited to one kind of dialect.

    .. change::
        :tags: feature, sql
        :tickets: 1729

      Added reduce_columns() method
      to select() construct, replaces columns inline
      using the util.reduce_columns utility function
      to remove equivalent columns.  reduce_columns()
      also adds "with_only_synonyms" to limit the
      reduction just to those columns which have the same
      name.  The deprecated fold_equivalents() feature is
      removed.

    .. change::
        :tags: feature, sql
        :tickets: 2470

      Reworked the startswith(), endswith(),
      contains() operators to do a better job with
      negation (NOT LIKE), and also to assemble them
      at compilation time so that their rendered SQL
      can be altered, such as in the case for Firebird
      STARTING WITH

    .. change::
        :tags: feature, sql
        :tickets: 2463

      Added a hook to the system of rendering
      CREATE TABLE that provides access to the render for each
      Column individually, by constructing a @compiles
      function against the new schema.CreateColumn
      construct.

    .. change::
        :tags: feature, sql
        :tickets:

      "scalar" selects now have a WHERE method
      to help with generative building.  Also slight adjustment
      regarding how SS "correlates" columns; the new methodology
      no longer applies meaning to the underlying
      Table column being selected.  This improves
      some fairly esoteric situations, and the logic
      that was there didn't seem to have any purpose.

    .. change::
        :tags: bug, sql
        :tickets: 2520

      Fixes to the interpretation of the
      Column "default" parameter as a callable
      to not pass ExecutionContext into a keyword
      argument parameter.

    .. change::
        :tags: bug, sql
        :tickets: 2410

      All of UniqueConstraint, ForeignKeyConstraint,
      CheckConstraint, and PrimaryKeyConstraint will
      attach themselves to their parent table automatically
      when they refer to a Table-bound Column object directly
      (i.e. not just string column name), and refer to
      one and only one Table.   Prior to 0.8 this behavior
      occurred for UniqueConstraint and PrimaryKeyConstraint,
      but not ForeignKeyConstraint or CheckConstraint.

    .. change::
        :tags: bug, sql
        :tickets: 2594

      TypeDecorator now includes a generic repr()
      that works in terms of the "impl" type by default.
      This is a behavioral change for those TypeDecorator
      classes that specify a custom __init__ method; those
      types will need to re-define __repr__() if they need
      __repr__() to provide a faithful constructor representation.

    .. change::
        :tags: bug, sql
        :tickets: 2168

      column.label(None) now produces an
      anonymous label, instead of returning the
      column object itself, consistent with the behavior
      of label(column, None).

    .. change::
        :tags: feature, sql
        :tickets: 2455

      An explicit error is raised when
      a ForeignKeyConstraint() that was
      constructed to refer to multiple remote tables
      is first used.

    .. change::
        :tags: access, feature
        :tickets:

      the MS Access dialect has been
      moved to its own project on Bitbucket,
      taking advantage of the new SQLAlchemy
      dialect compliance suite.   The dialect is
      still in very rough shape and probably not
      ready for general use yet, however
      it does have *extremely* rudimental
      functionality now.
      https://bitbucket.org/zzzeek/sqlalchemy-access

    .. change::
        :tags: maxdb, moved
        :tickets:

      The MaxDB dialect, which hasn't been
      functional for several years, is
      moved out to a pending bitbucket project,
      https://bitbucket.org/zzzeek/sqlalchemy-maxdb.

    .. change::
        :tags: sqlite, feature
        :tickets: 2363

      the SQLite date and time types
      have been overhauled to support a more open
      ended format for input and output, using
      name based format strings and regexps.  A
      new argument "microseconds" also provides
      the option to omit the "microseconds"
      portion of timestamps.  Thanks to
      Nathan Wright for the work and tests on
      this.

    .. change::
        :tags: mssql, feature
        :tickets:

      SQL Server dialect can be given
      database-qualified schema names,
      i.e. "schema='mydatabase.dbo'"; reflection
      operations will detect this, split the schema
      among the "." to get the owner separately,
      and emit a "USE mydatabase" statement before
      reflecting targets within the "dbo" owner;
      the existing database returned from
      DB_NAME() is then restored.

    .. change::
        :tags: mssql, bug
        :tickets: 2277

      removed legacy behavior whereby
      a column comparison to a scalar SELECT via
      == would coerce to an IN with the SQL server
      dialect.  This is implicit
      behavior which fails in other scenarios
      so is removed.  Code which relies on this
      needs to be modified to use column.in_(select)
      explicitly.

    .. change::
        :tags: mssql, feature
        :tickets:

      updated support for the mxodbc
      driver; mxodbc 3.2.1 is recommended for full
      compatibility.

    .. change::
        :tags: postgresql, feature
        :tickets: 2441

      postgresql.ARRAY features an optional
      "dimension" argument, will assign a specific
      number of dimensions to the array which will
      render in DDL as ARRAY[][]..., also improves
      performance of bind/result processing.

    .. change::
        :tags: postgresql, feature
        :tickets:

      postgresql.ARRAY now supports
      indexing and slicing.  The Python [] operator
      is available on all SQL expressions that are
      of type ARRAY; integer or simple slices can be
      passed.  The slices can also be used on the
      assignment side in the SET clause of an UPDATE
      statement by passing them into Update.values();
      see the docs for examples.

    .. change::
        :tags: postgresql, feature
        :tickets:

      Added new "array literal" construct
      postgresql.array().  Basically a "tuple" that
      renders as ARRAY[1,2,3].

    .. change::
        :tags: postgresql, feature
        :tickets: 2506

      Added support for the Postgresql ONLY
      keyword, which can appear corresponding to a
      table in a SELECT, UPDATE, or DELETE statement.
      The phrase is established using with_hint().
      Courtesy Ryan Kelly

    .. change::
        :tags: postgresql, feature
        :tickets:

      The "ischema_names" dictionary of the
      Postgresql dialect is "unofficially" customizable.
      Meaning, new types such as PostGIS types can
      be added into this dictionary, and the PG type
      reflection code should be able to handle simple
      types with variable numbers of arguments.
      The functionality here is "unofficial" for
      three reasons:

      1. this is not an "official" API.  Ideally
         an "official" API would allow custom type-handling
         callables at the dialect or global level
         in a generic way.
      2. This is only implemented for the PG dialect,
         in particular because PG has broad support
         for custom types vs. other database backends.
         A real API would be implemented at the
         default dialect level.
      3. The reflection code here is only tested against
         simple types and probably has issues with more
         compositional types.

      patch courtesy Éric Lemoine.

    .. change::
        :tags: firebird, feature
        :tickets: 2470

      The "startswith()" operator renders
      as "STARTING WITH", "~startswith()" renders
      as "NOT STARTING WITH", using FB's more efficient
      operator.

    .. change::
        :tags: firebird, bug
        :tickets: 2505

      CompileError is raised when VARCHAR with
      no length is attempted to be emitted, same
      way as MySQL.

    .. change::
        :tags: firebird, bug
        :tickets:

      Firebird now uses strict "ansi bind rules"
      so that bound parameters don't render in the
      columns clause of a statement - they render
      literally instead.

    .. change::
        :tags: firebird, bug
        :tickets:

      Support for passing datetime as date when
      using the DateTime type with Firebird; other
      dialects support this.

    .. change::
        :tags: firebird, feature
        :tickets: 2504

      An experimental dialect for the fdb
      driver is added, but is untested as I cannot
      get the fdb package to build.

    .. change::
        :tags: bug, mysql
        :tickets: 2404

      Dialect no longer emits expensive server
      collations query, as well as server casing,
      on first connect.  These functions are still
      available as semi-private.

    .. change::
        :tags: feature, mysql
        :tickets: 2534

      Added TIME type to mysql dialect,
      accepts "fst" argument which is the new
      "fractional seconds" specifier for recent
      MySQL versions.  The datatype will interpret
      a microseconds portion received from the driver,
      however note that at this time most/all MySQL
      DBAPIs do not support returning this value.

    .. change::
        :tags: oracle, bug
        :tickets: 2437

      Quoting information is now passed along
      from a Column with quote=True when generating
      a same-named bound parameter to the bindparam()
      object, as is the case in generated INSERT and UPDATE
      statements, so that unknown reserved names can
      be fully supported.

    .. change::
        :tags: oracle, feature
        :tickets: 2561

      The types of columns excluded from the
      setinputsizes() set can be customized by sending
      a list of string DBAPI type names to exclude,
      using the exclude_setinputsizes dialect parameter.
      This list was previously fixed.  The list also
      now defaults to STRING, UNICODE, removing
      CLOB, NCLOB from the list.

    .. change::
        :tags: oracle, bug
        :tickets:

      The CreateIndex construct in Oracle
      will now schema-qualify the name of the index
      to be that of the parent table.  Previously this
      name was omitted which apparently creates the
      index in the default schema, rather than that
      of the table.

    .. change::
        :tags: sql, feature
        :tickets: 2580

        Added :meth:`.ColumnOperators.notin_`,
        :meth:`.ColumnOperators.notlike`,
        :meth:`.ColumnOperators.notilike` to :class:`.ColumnOperators`.

    .. change::
        :tags: sql, removed

        The long-deprecated and non-functional ``assert_unicode`` flag on
        :func:`.create_engine` as well as :class:`.String` is removed.
