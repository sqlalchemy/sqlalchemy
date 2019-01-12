
=============
0.5 Changelog
=============

                
.. changelog::
    :version: 0.5.9
    :released: 

    .. change::
        :tags: sql
        :tickets: 1661

      Fixed erroneous self_group() call in expression package.

.. changelog::
    :version: 0.5.8
    :released: Sat Jan 16 2010

    .. change::
        :tags: sql
        :tickets: 

      The copy() method on Column now supports uninitialized,
      unnamed Column objects. This allows easy creation of
      declarative helpers which place common columns on multiple
      subclasses.

    .. change::
        :tags: sql
        :tickets: 

      Default generators like Sequence() translate correctly
      across a copy() operation.

    .. change::
        :tags: sql
        :tickets: 

      Sequence() and other DefaultGenerator objects are accepted
      as the value for the "default" and "onupdate" keyword
      arguments of Column, in addition to being accepted
      positionally.

    .. change::
        :tags: sql
        :tickets: 1568, 1617

      Fixed a column arithmetic bug that affected column
      correspondence for cloned selectables which contain
      free-standing column expressions.   This bug is
      generally only noticeable when exercising newer
      ORM behavior only available in 0.6 via,
      but is more correct at the SQL expression level
      as well.

    .. change::
        :tags: postgresql
        :tickets: 1647

      The extract() function, which was slightly improved in
      0.5.7, needed a lot more work to generate the correct
      typecast (the typecasts appear to be necessary in PG's
      EXTRACT quite a lot of the time).  The typecast is
      now generated using a rule dictionary based
      on PG's documentation for date/time/interval arithmetic.
      It also accepts text() constructs again, which was broken
      in 0.5.7.

    .. change::
        :tags: firebird
        :tickets: 1646

      Recognize more errors as disconnections.

.. changelog::
    :version: 0.5.7
    :released: Sat Dec 26 2009

    .. change::
        :tags: orm
        :tickets: 1543

      contains_eager() now works with the automatically
      generated subquery that results when you say
      "query(Parent).join(Parent.somejoinedsubclass)", i.e.
      when Parent joins to a joined-table-inheritance subclass.
      Previously contains_eager() would erroneously add the
      subclass table to the query separately producing a
      cartesian product.  An example is in the ticket
      description.

    .. change::
        :tags: orm
        :tickets: 1553

      query.options() now only propagate to loaded objects
      for potential further sub-loads only for options where
      such behavior is relevant, keeping
      various unserializable options like those generated
      by contains_eager() out of individual instance states.

    .. change::
        :tags: orm
        :tickets: 1054

      Session.execute() now locates table- and
      mapper-specific binds based on a passed
      in expression which is an insert()/update()/delete()
      construct.

    .. change::
        :tags: orm
        :tickets: 

      Session.merge() now properly overwrites a many-to-one or
      uselist=False attribute to None if the attribute
      is also None in the given object to be merged.

    .. change::
        :tags: orm
        :tickets: 1618

      Fixed a needless select which would occur when merging
      transient objects that contained a null primary key
      identifier.

    .. change::
        :tags: orm
        :tickets: 1585

      Mutable collection passed to the "extension" attribute
      of relation(), column_property() etc. will not be mutated
      or shared among multiple instrumentation calls, preventing
      duplicate extensions, such as backref populators,
      from being inserted into the list.

    .. change::
        :tags: orm
        :tickets: 1504

      Fixed the call to get_committed_value() on CompositeProperty.

    .. change::
        :tags: orm
        :tickets: 1602

      Fixed bug where Query would crash if a join() with no clear
      "left" side were called when a non-mapped column entity
      appeared in the columns list.

    .. change::
        :tags: orm
        :tickets: 1616, 1480

      Fixed bug whereby composite columns wouldn't load properly
      when configured on a joined-table subclass, introduced in
      version 0.5.6 as a result of the fix for. thx to Scott Torborg.

    .. change::
        :tags: orm
        :tickets: 1556

      The "use get" behavior of many-to-one relations, i.e. that a
      lazy load will fallback to the possibly cached query.get()
      value, now works across join conditions where the two compared
      types are not exactly the same class, but share the same
      "affinity" - i.e. Integer and SmallInteger.  Also allows
      combinations of reflected and non-reflected types to work
      with 0.5 style type reflection, such as PGText/Text (note 0.6
      reflects types as their generic versions).

    .. change::
        :tags: orm
        :tickets: 1436

      Fixed bug in query.update() when passing Cls.attribute
      as keys in the value dict and using synchronize_session='expire'
      ('fetch' in 0.6).

    .. change::
        :tags: sql
        :tickets: 1603

      Fixed bug in two-phase transaction whereby commit() method
      didn't set the full state which allows subsequent close()
      call to succeed.

    .. change::
        :tags: sql
        :tickets: 

      Fixed the "numeric" paramstyle, which apparently is the
      default paramstyle used by Informixdb.

    .. change::
        :tags: sql
        :tickets: 1574

      Repeat expressions in the columns clause of a select
      are deduped based on the identity of each clause element,
      not the actual string.  This allows positional
      elements to render correctly even if they all render
      identically, such as "qmark" style bind parameters.

    .. change::
        :tags: sql
        :tickets: 1632

      The cursor associated with connection pool connections
      (i.e. _CursorFairy) now proxies `__iter__()` to the
      underlying cursor correctly.

    .. change::
        :tags: sql
        :tickets: 1556

      types now support an "affinity comparison" operation, i.e.
      that an Integer/SmallInteger are "compatible", or
      a Text/String, PickleType/Binary, etc.  Part of.

    .. change::
        :tags: sql
        :tickets: 1641

      Fixed bug preventing alias() of an alias() from being
      cloned or adapted (occurs frequently in ORM operations).

    .. change::
        :tags: sqlite
        :tickets: 1439

      sqlite dialect properly generates CREATE INDEX for a table
      that is in an alternate schema.

    .. change::
        :tags: postgresql
        :tickets: 1085

      Added support for reflecting the DOUBLE PRECISION type,
      via a new postgres.PGDoublePrecision object.
      This is postgresql.DOUBLE_PRECISION in 0.6.

    .. change::
        :tags: postgresql
        :tickets: 460

      Added support for reflecting the INTERVAL YEAR TO MONTH
      and INTERVAL DAY TO SECOND syntaxes of the INTERVAL
      type.

    .. change::
        :tags: postgresql
        :tickets: 1576

      Corrected the "has_sequence" query to take current schema,
      or explicit sequence-stated schema, into account.

    .. change::
        :tags: postgresql
        :tickets: 1611

      Fixed the behavior of extract() to apply operator
      precedence rules to the "::" operator when applying
      the "timestamp" cast - ensures proper parenthesization.

    .. change::
        :tags: mssql
        :tickets: 1561

      Changed the name of TrustedConnection to
      Trusted_Connection when constructing pyodbc connect
      arguments

    .. change::
        :tags: oracle
        :tickets: 1637

      The "table_names" dialect function, used by MetaData
      .reflect(), omits "index overflow tables", a system
      table generated by Oracle when "index only tables"
      with overflow are used.  These tables aren't accessible
      via SQL and can't be reflected.

    .. change::
        :tags: ext
        :tickets: 1570, 1523

      A column can be added to a joined-table declarative
      superclass after the class has been constructed
      (i.e. via class-level attribute assignment), and
      the column will be propagated down to
      subclasses.  This is the reverse
      situation as that of, fixed in 0.5.6.

    .. change::
        :tags: ext
        :tickets: 1491

      Fixed a slight inaccuracy in the sharding example.
      Comparing equivalence of columns in the ORM is best
      accomplished using col1.shares_lineage(col2).

    .. change::
        :tags: ext
        :tickets: 1606

      Removed unused `load()` method from ShardedQuery.

.. changelog::
    :version: 0.5.6
    :released: Sat Sep 12 2009

    .. change::
        :tags: orm
        :tickets: 1300

      Fixed bug whereby inheritance discriminator part of a
      composite primary key would fail on updates.
      Continuation of.

    .. change::
        :tags: orm
        :tickets: 1507

      Fixed bug which disallowed one side of a many-to-many
      bidirectional reference to declare itself as "viewonly"

    .. change::
        :tags: orm
        :tickets: 1526

      Added an assertion that prevents a @validates function
      or other AttributeExtension from loading an unloaded
      collection such that internal state may be corrupted.

    .. change::
        :tags: orm
        :tickets: 1519

      Fixed bug which prevented two entities from mutually
      replacing each other's primary key values within a single
      flush() for some orderings of operations.

    .. change::
        :tags: orm
        :tickets: 1485

      Fixed an obscure issue whereby a joined-table subclass
      with a self-referential eager load on the base class
      would populate the related object's "subclass" table with
      data from the "subclass" table of the parent.

    .. change::
        :tags: orm
        :tickets: 1477

      relations() now have greater ability to be "overridden",
      meaning a subclass that explicitly specifies a relation()
      overriding that of the parent class will be honored
      during a flush.  This is currently to support
      many-to-many relations from concrete inheritance setups.
      Outside of that use case, YMMV.

    .. change::
        :tags: orm
        :tickets: 1483

      Squeezed a few more unnecessary "lazy loads" out of
      relation().  When a collection is mutated, many-to-one
      backrefs on the other side will not fire off to load
      the "old" value, unless "single_parent=True" is set.
      A direct assignment of a many-to-one still loads
      the "old" value in order to update backref collections
      on that value, which may be present in the session
      already, thus maintaining the 0.5 behavioral contract.

    .. change::
        :tags: orm
        :tickets: 1480

      Fixed bug whereby a load/refresh of joined table
      inheritance attributes which were based on
      column_property() or similar would fail to evaluate.

    .. change::
        :tags: orm
        :tickets: 1488

      Improved support for MapperProperty objects overriding
      that of an inherited mapper for non-concrete
      inheritance setups - attribute extensions won't randomly
      collide with each other.

    .. change::
        :tags: orm
        :tickets: 1487

      UPDATE and DELETE do not support ORDER BY, LIMIT, OFFSET,
      etc. in standard SQL.  Query.update() and Query.delete()
      now raise an exception if any of limit(), offset(),
      order_by(), group_by(), or distinct() have been
      called.

    .. change::
        :tags: orm
        :tickets: 

      Added AttributeExtension to sqlalchemy.orm.__all__

    .. change::
        :tags: orm
        :tickets: 1476

      Improved error message when query() is called with
      a non-SQL /entity expression.

    .. change::
        :tags: orm
        :tickets: 1440

      Using False or 0 as a polymorphic discriminator now
      works on the base class as well as a subclass.

    .. change::
        :tags: orm
        :tickets: 1424

      Added enable_assertions(False) to Query which disables
      the usual assertions for expected state - used
      by Query subclasses to engineer custom state..  See
      http://www.sqlalchemy.org/trac/wiki/UsageRecipes/PreFilteredQuery
      for an example.

    .. change::
        :tags: orm
        :tickets: 1501

      Fixed recursion issue which occurred if a mapped object's
      `__len__()` or `__nonzero__()` method resulted in state
      changes.

    .. change::
        :tags: orm
        :tickets: 1506

      Fixed incorrect exception raise in
      Weak/StrongIdentityMap.add()

    .. change::
        :tags: orm
        :tickets: 1522

      Fixed the error message for "could not find a FROM clause"
      in query.join() which would fail to issue correctly
      if the query was against a pure SQL construct.

    .. change::
        :tags: orm
        :tickets: 1486

      Fixed a somewhat hypothetical issue which would result
      in the wrong primary key being calculated for a mapper
      using the old polymorphic_union function - but this
      is old stuff.

    .. change::
        :tags: sql
        :tickets: 1373

      Fixed column.copy() to copy defaults and onupdates.

    .. change::
        :tags: sql
        :tickets: 

      Fixed a bug in extract() introduced in 0.5.4 whereby
      the string "field" argument was getting treated as a
      ClauseElement, causing various errors within more
      complex SQL transformations.

    .. change::
        :tags: sql
        :tickets: 1420

      Unary expressions such as DISTINCT propagate their
      type handling to result sets, allowing conversions like
      unicode and such to take place.

    .. change::
        :tags: sql
        :tickets: 1482

      Fixed bug in Table and Column whereby passing empty
      dict for "info" argument would raise an exception.

    .. change::
        :tags: oracle
        :tickets: 1309

      Backported 0.6 fix for Oracle alias names not getting
      truncated.

    .. change::
        :tags: ext
        :tickets: 1446

      The collection proxies produced by associationproxy are now
      pickleable.  A user-defined proxy_factory however
      is still not pickleable unless it defines __getstate__
      and __setstate__.

    .. change::
        :tags: ext
        :tickets: 1468

      Declarative will raise an informative exception if
      __table_args__ is passed as a tuple with no dict argument.
      Improved documentation.

    .. change::
        :tags: ext
        :tickets: 1527

      Table objects declared in the MetaData can now be used
      in string expressions sent to primaryjoin/secondaryjoin/
      secondary - the name is pulled from the MetaData of the
      declarative base.

    .. change::
        :tags: ext
        :tickets: 1523

      A column can be added to a joined-table subclass after
      the class has been constructed (i.e. via class-level
      attribute assignment).  The column is added to the underlying
      Table as always, but now the mapper will rebuild its
      "join" to include the new column, instead of raising
      an error about "no such column, use column_property()
      instead".

    .. change::
        :tags: test
        :tickets: 

      Added examples into the test suite so they get exercised
      regularly and cleaned up a couple deprecation warnings.

.. changelog::
    :version: 0.5.5
    :released: Mon Jul 13 2009

    .. change::
        :tags: general
        :tickets: 970

      unit tests have been migrated from unittest to nose.  See
      README.unittests for information on how to run the tests.

    .. change::
        :tags: orm
        :tickets: 

      The "foreign_keys" argument of relation() will now propagate
      automatically to the backref in the same way that primaryjoin
      and secondaryjoin do.  For the extremely rare use case where
      the backref of a relation() has intentionally different
      "foreign_keys" configured, both sides now need to be
      configured explicitly (if they do in fact require this setting,
      see the next note...).

    .. change::
        :tags: orm
        :tickets: 

      ...the only known (and really, really rare) use case where a
      different foreign_keys setting was used on the
      forwards/backwards side, a composite foreign key that
      partially points to its own columns, has been enhanced such
      that the fk->itself aspect of the relation won't be used to
      determine relation direction.

    .. change::
        :tags: orm
        :tickets: 

      Session.mapper is now *deprecated*.
      
      Call session.add() if you'd like a free-standing object to be
      part of your session.  Otherwise, a DIY version of
      Session.mapper is now documented at
      http://www.sqlalchemy.org/trac/wiki/UsageRecipes/SessionAwareMapper
      The method will remain deprecated throughout 0.6.

    .. change::
        :tags: orm
        :tickets: 1431

      Fixed Query being able to join() from individual columns of a
      joined-table subclass entity, i.e.  query(SubClass.foo,
      SubClass.bar).join(<anything>).  In most cases, an error
      "Could not find a FROM clause to join from" would be
      raised. In a few others, the result would be returned in terms
      of the base class rather than the subclass - so applications
      which relied on this erroneous result need to be
      adjusted.

    .. change::
        :tags: orm
        :tickets: 1461

      Fixed a bug involving contains_eager(), which would apply
      itself to a secondary (i.e. lazy) load in a particular rare
      case, producing cartesian products.  improved the targeting of
      query.options() on secondary loads overall.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug introduced in 0.5.4 whereby Composite types fail
      when default-holding columns are flushed.

    .. change::
        :tags: orm
        :tickets: 1426

      Fixed another 0.5.4 bug whereby mutable attributes
      (i.e. PickleType) wouldn't be deserialized correctly when the
      whole object was serialized.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug whereby session.is_modified() would raise an
      exception if any synonyms were in use.

    .. change::
        :tags: orm
        :tickets: 

      Fixed potential memory leak whereby previously pickled objects
      placed back in a session would not be fully garbage collected
      unless the Session were explicitly closed out.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug whereby list-based attributes, like pickletype and
      PGArray, failed to be merged() properly.

    .. change::
        :tags: orm
        :tickets: 

      Repaired non-working attributes.set_committed_value function.

    .. change::
        :tags: orm
        :tickets: 

      Trimmed the pickle format for InstanceState which should
      further reduce the memory footprint of pickled instances.  The
      format should be backwards compatible with that of 0.5.4 and
      previous.

    .. change::
        :tags: orm
        :tickets: 1463

      sqlalchemy.orm.join and sqlalchemy.orm.outerjoin are now
      added to __all__ in sqlalchemy.orm.*.

    .. change::
        :tags: orm
        :tickets: 1458

      Fixed bug where Query exception raise would fail when
      a too-short composite primary key value were passed to
      get().

    .. change::
        :tags: sql
        :tickets: 

      Removed an obscure feature of execute() (including connection,
      engine, Session) whereby a bindparam() construct can be sent
      as a key to the params dictionary.  This usage is undocumented
      and is at the core of an issue whereby the bindparam() object
      created implicitly by a text() construct may have the same
      hash value as a string placed in the params dictionary and may
      result in an inappropriate match when computing the final bind
      parameters.  Internal checks for this condition would add
      significant latency to the critical task of parameter
      rendering, so the behavior is removed.  This is a backwards
      incompatible change for any application that may have been
      using this feature, however the feature has never been
      documented.

    .. change::
        :tags: engine/pool
        :tickets: 

      Implemented recreate() for StaticPool.

.. changelog::
    :version: 0.5.4p2
    :released: Tue May 26 2009

    .. change::
        :tags: sql
        :tickets: 

      Repaired the printing of SQL exceptions which are not
      based on parameters or are not executemany() style.

    .. change::
        :tags: postgresql
        :tickets: 

      Deprecated the hardcoded TIMESTAMP function, which when
      used as func.TIMESTAMP(value) would render "TIMESTAMP value".
      This breaks on some platforms as PostgreSQL doesn't allow
      bind parameters to be used in this context.  The hard-coded
      uppercase is also inappropriate and there's lots of other
      PG casts that we'd need to support.  So instead, use
      text constructs i.e. select(["timestamp '12/05/09'"]).

.. changelog::
    :version: 0.5.4p1
    :released: Mon May 18 2009

    .. change::
        :tags: orm
        :tickets: 

      Fixed an attribute error introduced in 0.5.4 which would
      occur when merge() was used with an incomplete object.

.. changelog::
    :version: 0.5.4
    :released: Sun May 17 2009

    .. change::
        :tags: orm
        :tickets: 1398

      Significant performance enhancements regarding Sessions/flush()
      in conjunction with large mapper graphs, large numbers of
      objects:
      
      - Removed all* O(N) scanning behavior from the flush() process,
        i.e. operations that were scanning the full session,
        including an extremely expensive one that was erroneously
        assuming primary key values were changing when this
        was not the case.
      
        * one edge case remains which may invoke a full scan,
          if an existing primary key attribute is modified
          to a new value.
      
      - The Session's "weak referencing" behavior is now *full* -
        no strong references whatsoever are made to a mapped object
        or related items/collections in its __dict__.  Backrefs and
        other cycles in objects no longer affect the Session's ability
        to lose all references to unmodified objects.  Objects with
        pending changes still are maintained strongly until flush.
       
      
        The implementation also improves performance by moving
        the "resurrection" process of garbage collected items
        to only be relevant for mappings that map "mutable"
        attributes (i.e. PickleType, composite attrs).  This removes
        overhead from the gc process and simplifies internal
        behavior.
      
        If a "mutable" attribute change is the sole change on an object
        which is then dereferenced, the mapper will not have access to
        other attribute state when the UPDATE is issued.  This may present
        itself differently to some MapperExtensions.
      
        The change also affects the internal attribute API, but not
        the AttributeExtension interface nor any of the publicly
        documented attribute functions.
      
      - The unit of work no longer generates a graph of "dependency"
        processors for the full graph of mappers during flush(), instead
        creating such processors only for those mappers which represent
        objects with pending changes.  This saves a tremendous number
        of method calls in the context of a large interconnected
        graph of mappers.
      
      - Cached a wasteful "table sort" operation that previously
        occurred multiple times per flush, also removing significant
        method call count from flush().
      
      - Other redundant behaviors have been simplified in
        mapper._save_obj().

    .. change::
        :tags: orm
        :tickets: 

      Modified query_cls on DynamicAttributeImpl to accept a full
      mixin version of the AppenderQuery, which allows subclassing
      the AppenderMixin.

    .. change::
        :tags: orm
        :tickets: 1300

      The "polymorphic discriminator" column may be part of a
      primary key, and it will be populated with the correct
      discriminator value.

    .. change::
        :tags: orm
        :tickets: 

      Fixed the evaluator not being able to evaluate IS NULL clauses.

    .. change::
        :tags: orm
        :tickets: 1352

      Fixed the "set collection" function on "dynamic" relations to
      initiate events correctly.  Previously a collection could only
      be assigned to a pending parent instance, otherwise modified
      events would not be fired correctly.  Set collection is now
      compatible with merge(), fixes.

    .. change::
        :tags: orm
        :tickets: 

      Allowed pickling of PropertyOption objects constructed with
      instrumented descriptors; previously, pickle errors would occur
      when pickling an object which was loaded with a descriptor-based
      option, such as query.options(eagerload(MyClass.foo)).

    .. change::
        :tags: orm
        :tickets: 1357

      Lazy loader will not use get() if the "lazy load" SQL clause
      matches the clause used by get(), but contains some parameters
      hardcoded.  Previously the lazy strategy would fail with the
      get().  Ideally get() would be used with the hardcoded
      parameters but this would require further development.

    .. change::
        :tags: orm
        :tickets: 1391

      MapperOptions and other state associated with query.options()
      is no longer bundled within callables associated with each
      lazy/deferred-loading attribute during a load.
      The options are now associated with the instance's
      state object just once when it's populated.  This removes
      the need in most cases for per-instance/attribute loader
      objects, improving load speed and memory overhead for
      individual instances.

    .. change::
        :tags: orm
        :tickets: 1360

      Fixed another location where autoflush was interfering
      with session.merge().  autoflush is disabled completely
      for the duration of merge() now.

    .. change::
        :tags: orm
        :tickets: 1406

      Fixed bug which prevented "mutable primary key" dependency
      logic from functioning properly on a one-to-one
      relation().

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug in relation(), introduced in 0.5.3,
      whereby a self referential relation
      from a base class to a joined-table subclass would
      not configure correctly.

    .. change::
        :tags: orm
        :tickets: 

      Fixed obscure mapper compilation issue when inheriting
      mappers are used which would result in un-initialized
      attributes.

    .. change::
        :tags: orm
        :tickets: 

      Fixed documentation for session weak_identity_map -
      the default value is True, indicating a weak
      referencing map in use.

    .. change::
        :tags: orm
        :tickets: 1376

      Fixed a unit of work issue whereby the foreign
      key attribute on an item contained within a collection
      owned by an object being deleted would not be set to
      None if the relation() was self-referential.

    .. change::
        :tags: orm
        :tickets: 1378

      Fixed Query.update() and Query.delete() failures with eagerloaded
      relations.

    .. change::
        :tags: orm
        :tickets: 

      It is now an error to specify both columns of a binary primaryjoin
      condition in the foreign_keys or remote_side collection.  Whereas
      previously it was just nonsensical, but would succeed in a
      non-deterministic way.

    .. change::
        :tags: ticket: 594, 1341, schema
        :tickets: 

      Added a quote_schema() method to the IdentifierPreparer class
      so that dialects can override how schemas get handled. This
      enables the MSSQL dialect to treat schemas as multipart
      identifiers, such as 'database.owner'.

    .. change::
        :tags: sql
        :tickets: 

      Back-ported the "compiler" extension from SQLA 0.6.  This
      is a standardized interface which allows the creation of custom
      ClauseElement subclasses and compilers.  In particular it's
      handy as an alternative to text() when you'd like to
      build a construct that has database-specific compilations.
      See the extension docs for details.

    .. change::
        :tags: sql
        :tickets: 1413

      Exception messages are truncated when the list of bound
      parameters is larger than 10, preventing enormous
      multi-page exceptions from filling up screens and logfiles
      for large executemany() statements.

    .. change::
        :tags: sql
        :tickets: 

      ``sqlalchemy.extract()`` is now dialect sensitive and can
      extract components of timestamps idiomatically across the
      supported databases, including SQLite.

    .. change::
        :tags: sql
        :tickets: 1353

      Fixed __repr__() and other _get_colspec() methods on
      ForeignKey constructed from __clause_element__() style
      construct (i.e. declarative columns).

    .. change::
        :tags: mysql
        :tickets: 1405

      Reflecting a FOREIGN KEY construct will take into account
      a dotted schema.tablename combination, if the foreign key
      references a table in a remote schema.

    .. change::
        :tags: mssql
        :tickets: 

      Modified how savepoint logic works to prevent it from
      stepping on non-savepoint oriented routines. Savepoint
      support is still very experimental.

    .. change::
        :tags: mssql
        :tickets: 1310

      Added in reserved words for MSSQL that covers version 2008
      and all prior versions.

    .. change::
        :tags: mssql
        :tickets: 1343

      Corrected problem with information schema not working with a
      binary collation based database. Cleaned up information schema
      since it is only used by mssql now.

    .. change::
        :tags: sqlite
        :tickets: 1402

      Corrected the SLBoolean type so that it properly treats only 1
      as True.

    .. change::
        :tags: sqlite
        :tickets: 1273

      Corrected the float type so that it correctly maps to a
      SLFloat type when being reflected.

    .. change::
        :tags: extensions
        :tickets: 1379

      Fixed adding of deferred or other column properties to a
      declarative class.

.. changelog::
    :version: 0.5.3
    :released: Tue Mar 24 2009

    .. change::
        :tags: orm
        :tickets: 1315

      The "objects" argument to session.flush() is deprecated.
      State which represents the linkage between a parent and
      child object does not support "flushed" status on
      one side of the link and not the other, so supporting
      this operation leads to misleading results.

    .. change::
        :tags: orm
        :tickets: 

      Query now implements __clause_element__() which produces
      its selectable, which means a Query instance can be accepted
      in many SQL expressions, including col.in_(query),
      union(query1, query2), select([foo]).select_from(query),
      etc.

    .. change::
        :tags: orm
        :tickets: 1337

      Query.join() can now construct multiple FROM clauses, if
      needed.  Such as, query(A, B).join(A.x).join(B.y)
      might say SELECT A.*, B.* FROM A JOIN X, B JOIN Y.
      Eager loading can also tack its joins onto those
      multiple FROM clauses.

    .. change::
        :tags: orm
        :tickets: 1347

      Fixed bug in dynamic_loader() where append/remove events
      after construction time were not being propagated to the
      UOW to pick up on flush().

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug where column_prefix wasn't being checked before
      not mapping an attribute that already had class-level
      name present.

    .. change::
        :tags: orm
        :tickets: 1315

      a session.expire() on a particular collection attribute
      will clear any pending backref additions as well, so that
      the next access correctly returns only what was present
      in the database.  Presents some degree of a workaround for, although we are considering removing the
      flush([objects]) feature altogether.

    .. change::
        :tags: orm
        :tickets: 

      Session.scalar() now converts raw SQL strings to text()
      the same way Session.execute() does and accepts same
      alternative \**kw args.

    .. change::
        :tags: orm
        :tickets: 

      improvements to the "determine direction" logic of
      relation() such that the direction of tricky situations
      like mapper(A.join(B)) -> relation-> mapper(B) can be
      determined.

    .. change::
        :tags: orm
        :tickets: 1306

      When flushing partial sets of objects using session.flush([somelist]),
      pending objects which remain pending after the operation won't
      inadvertently be added as persistent.

    .. change::
        :tags: orm
        :tickets: 1314

      Added "post_configure_attribute" method to InstrumentationManager,
      so that the "listen_for_events.py" example works again.

    .. change::
        :tags: orm
        :tickets: 

      a forward and complementing backwards reference which are both
      of the same direction, i.e. ONETOMANY or MANYTOONE,
      is now detected, and an error message is raised.
      Saves crazy CircularDependencyErrors later on.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bugs in Query regarding simultaneous selection of
      multiple joined-table inheritance entities with common base
      classes:
      
      - previously the adaption applied to "B" on
        "A JOIN B" would be erroneously partially applied
        to "A".
      
      - comparisons on relations (i.e. A.related==someb)
        were not getting adapted when they should.
      
      - Other filterings, like
        query(A).join(A.bs).filter(B.foo=='bar'), were erroneously
        adapting "B.foo" as though it were an "A".

    .. change::
        :tags: orm
        :tickets: 1325

      Fixed adaptation of EXISTS clauses via any(), has(), etc.
      in conjunction with an aliased object on the left and
      of_type() on the right.

    .. change::
        :tags: orm
        :tickets: 

      Added an attribute helper method ``set_committed_value`` in
      sqlalchemy.orm.attributes.  Given an object, attribute name,
      and value, will set the value on the object as part of its
      "committed" state, i.e. state that is understood to have
      been loaded from the database.   Helps with the creation of
      homegrown collection loaders and such.

    .. change::
        :tags: orm
        :tickets: 

      Query won't fail with weakref error when a non-mapper/class
      instrumented descriptor is passed, raises
      "Invalid column expression".

    .. change::
        :tags: orm
        :tickets: 

      Query.group_by() properly takes into account aliasing applied
      to the FROM clause, such as with select_from(), using
      with_polymorphic(), or using from_self().

    .. change::
        :tags: sql
        :tickets: 

      An alias() of a select() will convert to a "scalar subquery"
      when used in an unambiguously scalar context, i.e. it's used
      in a comparison operation.  This applies to
      the ORM when using query.subquery() as well.

    .. change::
        :tags: sql
        :tickets: 1302

      Fixed missing _label attribute on Function object, others
      when used in a select() with use_labels (such as when used
      in an ORM column_property()).

    .. change::
        :tags: sql
        :tickets: 1309

      anonymous alias names now truncate down to the max length
      allowed by the dialect.  More significant on DBs like
      Oracle with very small character limits.

    .. change::
        :tags: sql
        :tickets: 

      the __selectable__() interface has been replaced entirely
      by __clause_element__().

    .. change::
        :tags: sql
        :tickets: 1299

      The per-dialect cache used by TypeEngine to cache
      dialect-specific types is now a WeakKeyDictionary.
      This to prevent dialect objects from
      being referenced forever for an application that
      creates an arbitrarily large number of engines
      or dialects.   There is a small performance penalty
      which will be resolved in 0.6.

    .. change::
        :tags: sqlite
        :tickets: 

      Fixed SQLite reflection methods so that non-present
      cursor.description, which triggers an auto-cursor
      close, will be detected so that no results doesn't
      fail on recent versions of pysqlite which raise
      an error when fetchone() called with no rows present.

    .. change::
        :tags: postgresql
        :tickets: 

      Index reflection won't fail when an index with
      multiple expressions is encountered.

    .. change::
        :tags: postgresql
        :tickets: 1327

      Added PGUuid and PGBit types to
      sqlalchemy.databases.postgres.

    .. change::
        :tags: postgresql
        :tickets: 1327

      Refection of unknown PG types won't crash when those
      types are specified within a domain.

    .. change::
        :tags: mssql
        :tickets: 

      Preliminary support for pymssql 1.0.1

    .. change::
        :tags: mssql
        :tickets: 

      Corrected issue on mssql where max_identifier_length was
      not being respected.

    .. change::
        :tags: extensions
        :tickets: 

      Fixed a recursive pickling issue in serializer, triggered
      by an EXISTS or other embedded FROM construct.

    .. change::
        :tags: extensions
        :tickets: 

      Declarative locates the "inherits" class using a search
      through __bases__, to skip over mixins that are local
      to subclasses.

    .. change::
        :tags: extensions
        :tickets: 

      Declarative figures out joined-table inheritance primary join
      condition even if "inherits" mapper argument is given
      explicitly.

    .. change::
        :tags: extensions
        :tickets: 

      Declarative will properly interpret the "foreign_keys" argument
      on a backref() if it's a string.

    .. change::
        :tags: extensions
        :tickets: 

      Declarative will accept a table-bound column as a property
      when used in conjunction with __table__, if the column is already
      present in __table__.  The column will be remapped to the given
      key the same way as when added to the mapper() properties dict.

.. changelog::
    :version: 0.5.2
    :released: Sat Jan 24 2009

    .. change::
        :tags: orm
        :tickets: 

      Further refined 0.5.1's warning about delete-orphan cascade
      placed on a many-to-many relation.   First, the bad news:
      the warning will apply to both many-to-many as well as
      many-to-one relations.  This is necessary since in both
      cases, SQLA does not scan the full set of potential parents
      when determining "orphan" status - for a persistent object
      it only detects an in-python de-association event to establish
      the object as an "orphan".  Next, the good news: to support
      one-to-one via a foreign key or association table, or to
      support one-to-many via an association table, a new flag
      single_parent=True may be set which indicates objects
      linked to the relation are only meant to have a single parent.
      The relation will raise an error if multiple parent-association
      events occur within Python.

    .. change::
        :tags: orm
        :tickets: 1292

      Adjusted the attribute instrumentation change from 0.5.1 to
      fully establish instrumentation for subclasses where the mapper
      was created after the superclass had already been fully
      instrumented.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug in delete-orphan cascade whereby two one-to-one
      relations from two different parent classes to the same target
      class would prematurely expunge the instance.

    .. change::
        :tags: orm
        :tickets: 

      Fixed an eager loading bug whereby self-referential eager
      loading would prevent other eager loads, self referential or not,
      from joining to the parent JOIN properly.  Thanks to Alex K
      for creating a great test case.

    .. change::
        :tags: orm
        :tickets: 

      session.expire() and related methods will not expire() unloaded
      deferred attributes.  This prevents them from being needlessly
      loaded when the instance is refreshed.

    .. change::
        :tags: orm
        :tickets: 1293

      query.join()/outerjoin() will now properly join an aliased()
      construct to the existing left side, even if query.from_self()
      or query.select_from(someselectable) has been called.

    .. change::
        :tags: sql
        :tickets: 1284

      Further fixes to the "percent signs and spaces in column/table
       names" functionality.

    .. change::
        :tags: mssql
        :tickets: 1291

      Restored convert_unicode handling. Results were being passed
      on through without conversion.

    .. change::
        :tags: mssql
        :tickets: 1282

      Really fixing the decimal handling this time..

    .. change::
        :tags: Ticket:1289, mssql
        :tickets: 

      Modified table reflection code to use only kwargs when
      constructing tables.

.. changelog::
    :version: 0.5.1
    :released: Sat Jan 17 2009

    .. change::
        :tags: orm
        :tickets: 

      Removed an internal join cache which could potentially leak
      memory when issuing query.join() repeatedly to ad-hoc
      selectables.

    .. change::
        :tags: orm
        :tickets: 

      The "clear()", "save()", "update()", "save_or_update()"
      Session methods have been deprecated, replaced by
      "expunge_all()" and "add()".  "expunge_all()" has also
      been added to ScopedSession.

    .. change::
        :tags: orm
        :tickets: 

      Modernized the "no mapped table" exception and added a more
      explicit __table__/__tablename__ exception to declarative.

    .. change::
        :tags: orm
        :tickets: 1237

      Concrete inheriting mappers now instrument attributes which
      are inherited from the superclass, but are not defined for
      the concrete mapper itself, with an InstrumentedAttribute that
      issues a descriptive error when accessed.

    .. change::
        :tags: orm
        :tickets: 1237, 781

      Added a new `relation()` keyword `back_populates`. This
      allows configuration of backreferences using explicit
      relations. This is required when creating
      bidirectional relations between a hierarchy of concrete
      mappers and another class.

    .. change::
        :tags: orm
        :tickets: 1237

      Test coverage added for `relation()` objects specified on
      concrete mappers.

    .. change::
        :tags: orm
        :tickets: 1276

      Query.from_self() as well as query.subquery() both disable
      the rendering of eager joins inside the subquery produced.
      The "disable all eager joins" feature is available publicly
      via a new query.enable_eagerloads() generative.

    .. change::
        :tags: orm
        :tickets: 

      Added a rudimental series of set operations to Query that
      receive Query objects as arguments, including union(),
      union_all(), intersect(), except_(), intersect_all(),
      except_all().  See the API documentation for
      Query.union() for examples.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug that prevented Query.join() and eagerloads from
      attaching to a query that selected from a union or aliased union.

    .. change::
        :tags: orm
        :tickets: 1237

      A short documentation example added for bidirectional
      relations specified on concrete mappers.

    .. change::
        :tags: orm
        :tickets: 1269

      Mappers now instrument class attributes upon construction
      with the final InstrumentedAttribute object which remains
      persistent. The `_CompileOnAttr`/`__getattribute__()`
      methodology has been removed. The net effect is that
      Column-based mapped class attributes can now be used fully
      at the class level without invoking a mapper compilation
      operation, greatly simplifying typical usage patterns
      within declarative.

    .. change::
        :tags: orm
        :tickets: 

      ColumnProperty (and front-end helpers such as ``deferred``) no
      longer ignores unknown \**keyword arguments.

    .. change::
        :tags: orm
        :tickets: 

      Fixed a bug with the unitofwork's "row switch" mechanism,
      i.e. the conversion of INSERT/DELETE into an UPDATE, when
      combined with joined-table inheritance and an object
      which contained no defined values for the child table where
      an UPDATE with no SET clause would be rendered.

    .. change::
        :tags: orm
        :tickets: 1281

      Using delete-orphan on a many-to-many relation is deprecated.
      This produces misleading or erroneous results since SQLA does
      not retrieve the full list of "parents" for m2m.  To get delete-orphan
      behavior with an m2m table, use an explicit association class
      so that the individual association row is treated as a parent.

    .. change::
        :tags: orm
        :tickets: 1281

      delete-orphan cascade always requires delete cascade.  Specifying
      delete-orphan without delete now raises a deprecation warning.

    .. change::
        :tags: sql
        :tickets: 1256

      Improved the methodology to handling percent signs in column
      names from.  Added more tests.  MySQL and
      PostgreSQL dialects still do not issue correct CREATE TABLE
      statements for identifiers with percent signs in them.

    .. change::
        :tags: schema
        :tickets: 1214

      Index now accepts column-oriented InstrumentedAttributes
      (i.e. column-based mapped class attributes) as column
      arguments.

    .. change::
        :tags: schema
        :tickets: 

      Column with no name (as in declarative) won't raise a
      NoneType error when its string output is requested
      (such as in a stack trace).

    .. change::
        :tags: schema
        :tickets: 1278

      Fixed bug when overriding a Column with a ForeignKey
      on a reflected table, where derived columns (i.e. the
      "virtual" columns of a select, etc.) would inadvertently
      call upon schema-level cleanup logic intended only
      for the original column.

    .. change::
        :tags: declarative
        :tickets: 

      Can now specify Column objects on subclasses which have no
      table of their own (i.e. use single table inheritance).
      The columns will be appended to the base table, but only
      mapped by the subclass.

    .. change::
        :tags: declarative
        :tickets: 

      For both joined and single inheriting subclasses, the subclass
      will only map those columns which are already mapped on the
      superclass and those explicit on the subclass.  Other
      columns that are present on the `Table` will be excluded
      from the mapping by default, which can be disabled
      by passing a blank `exclude_properties` collection to the
      `__mapper_args__`.  This is so that single-inheriting
      classes which define their own columns are the only classes
      to map those columns.   The effect is actually a more organized
      mapping than you'd normally get with explicit `mapper()`
      calls unless you set up the `exclude_properties` arguments
      explicitly.

    .. change::
        :tags: declarative
        :tickets: 

      It's an error to add new Column objects to a declarative class
      that specified an existing table using __table__.

    .. change::
        :tags: mysql
        :tickets: 

      Added the missing keywords from MySQL 4.1 so they get escaped
      properly.

    .. change::
        :tags: mssql
        :tickets: 1280

      Corrected handling of large decimal values with more robust
      tests. Removed string manipulation on floats.

    .. change::
        :tags: mssql
        :tickets: 

      Modified the do_begin handling in mssql to use the Cursor not
      the Connection so it is DBAPI compatible.

    .. change::
        :tags: mssql
        :tickets: 

      Corrected SAVEPOINT support on adodbapi by changing the
      handling of savepoint_release, which is unsupported on mssql.

.. changelog::
    :version: 0.5.0
    :released: Tue Jan 06 2009

    .. change::
        :tags: general
        :tickets: 

      Documentation has been converted to Sphinx.  In particular,
      the generated API documentation has been constructed into a
      full blown "API Reference" section which organizes editorial
      documentation combined with generated docstrings.  Cross
      linking between sections and API docs are vastly improved, a
      javascript-powered search feature is provided, and a full
      index of all classes, functions and members is provided.

    .. change::
        :tags: general
        :tickets: 

      setup.py now imports setuptools only optionally.  If not
      present, distutils is used.  The new "pip" installer is
      recommended over easy_install as it installs in a more
      simplified way.

    .. change::
        :tags: general
        :tickets: 

      added an extremely basic illustration of a PostGIS integration
      to the examples folder.

    .. change::
        :tags: orm
        :tickets: 

      Query.with_polymorphic() now accepts a third argument
      "discriminator" which will replace the value of
      mapper.polymorphic_on for that query.  Mappers themselves no
      longer require polymorphic_on to be set, even if the mapper
      has a polymorphic_identity.  When not set, the mapper will
      load non-polymorphically by default. Together, these two
      features allow a non-polymorphic concrete inheritance setup to
      use polymorphic loading on a per-query basis, since concrete
      setups are prone to many issues when used polymorphically in
      all cases.

    .. change::
        :tags: orm
        :tickets: 

      dynamic_loader accepts a query_class= to customize the Query
      classes used for both the dynamic collection and the queries
      built from it.

    .. change::
        :tags: orm
        :tickets: 1079

      query.order_by() accepts None which will remove any pending
      order_by state from the query, as well as cancel out any
      mapper/relation configured ordering. This is primarily useful
      for overriding the ordering specified on a dynamic_loader().

    .. change::
        :tags: sql
        :tickets: 935

      RowProxy objects can be used in place of dictionary arguments
      sent to connection.execute() and friends.

    .. change::
        :tags: dialect
        :tickets: 

      Added a new description_encoding attribute on the dialect that
      is used for encoding the column name when processing the
      metadata. This usually defaults to utf-8.

    .. change::
        :tags: mssql
        :tickets: 

      Added in a new MSGenericBinary type. This maps to the Binary
      type so it can implement the specialized behavior of treating
      length specified types as fixed-width Binary types and
      non-length types as an unbound variable length Binary type.

    .. change::
        :tags: mssql
        :tickets: 1249

      Added in new types: MSVarBinary and MSImage.

    .. change::
        :tags: mssql
        :tickets: 

      Added in the MSReal, MSNText, MSSmallDateTime, MSTime,
      MSDateTimeOffset, and MSDateTime2 types

    .. change::
        :tags: sqlite
        :tickets: 1266

      Table reflection now stores the actual DefaultClause value for
      the column.

    .. change::
        :tags: sqlite
        :tickets: 

      bugfixes, behavioral changes

    .. change::
        :tags: orm
        :tickets: 

      Exceptions raised during compile_mappers() are now preserved
      to provide "sticky behavior" - if a hasattr() call on a
      pre-compiled mapped attribute triggers a failing compile and
      suppresses the exception, subsequent compilation is blocked
      and the exception will be reiterated on the next compile()
      call.  This issue occurs frequently when using declarative.

    .. change::
        :tags: orm
        :tickets: 

      property.of_type() is now recognized on a single-table
      inheriting target, when used in the context of
      prop.of_type(..).any()/has(), as well as
      query.join(prop.of_type(...)).

    .. change::
        :tags: orm
        :tickets: 

      query.join() raises an error when the target of the join
      doesn't match the property-based attribute - while it's
      unlikely anyone is doing this, the SQLAlchemy author was
      guilty of this particular loosey-goosey behavior.

    .. change::
        :tags: orm
        :tickets: 1272

      Fixed bug when using weak_instance_map=False where modified
      events would not be intercepted for a flush().

    .. change::
        :tags: orm
        :tickets: 1268

      Fixed some deep "column correspondence" issues which could
      impact a Query made against a selectable containing multiple
      versions of the same table, as well as unions and similar
      which contained the same table columns in different column
      positions at different levels.

    .. change::
        :tags: orm
        :tickets: 

      Custom comparator classes used in conjunction with
      column_property(), relation() etc. can define new comparison
      methods on the Comparator, which will become available via
      __getattr__() on the InstrumentedAttribute.  In the case of
      synonym() or comparable_property(), attributes are resolved
      first on the user-defined descriptor, then on the user-defined
      comparator.

    .. change::
        :tags: orm
        :tickets: 976

      Added ScopedSession.is_active accessor.

    .. change::
        :tags: orm
        :tickets: 1262

      Can pass mapped attributes and column objects as keys to
      query.update({}).

    .. change::
        :tags: orm
        :tickets: 

      Mapped attributes passed to the values() of an expression
      level insert() or update() will use the keys of the mapped
      columns, not that of the mapped attribute.

    .. change::
        :tags: orm
        :tickets: 1242

      Corrected problem with Query.delete() and Query.update() not
      working properly with bind parameters.

    .. change::
        :tags: orm
        :tickets: 

      Query.select_from(), from_statement() ensure that the given
      argument is a FromClause, or Text/Select/Union, respectively.

    .. change::
        :tags: orm
        :tickets: 1253

      Query() can be passed a "composite" attribute as a column
      expression and it will be expanded.  Somewhat related to.

    .. change::
        :tags: orm
        :tickets: 

      Query() is a little more robust when passed various column
      expressions such as strings, clauselists, text() constructs
      (which may mean it just raises an error more nicely).

    .. change::
        :tags: orm
        :tickets: 

      first() works as expected with Query.from_statement().

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug introduced in 0.5rc4 involving eager loading not
      functioning for properties which were added to a mapper
      post-compile using add_property() or equivalent.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug where many-to-many relation() with viewonly=True
      would not correctly reference the link between
      secondary->remote.

    .. change::
        :tags: orm
        :tickets: 1232

      Duplicate items in a list-based collection will be maintained
      when issuing INSERTs to a "secondary" table in a many-to-many
      relation.  Assuming the m2m table has a unique or primary key
      constraint on it, this will raise the expected constraint
      violation instead of silently dropping the duplicate
      entries. Note that the old behavior remains for a one-to-many
      relation since collection entries in that case don't result in
      INSERT statements and SQLA doesn't manually police
      collections.

    .. change::
        :tags: orm
        :tickets: 

      Query.add_column() can accept FromClause objects in the same
      manner as session.query() can.

    .. change::
        :tags: orm
        :tickets: 

      Comparison of many-to-one relation to NULL is properly
      converted to IS NOT NULL based on not_().

    .. change::
        :tags: orm
        :tickets: 1087

      Extra checks added to ensure explicit
      primaryjoin/secondaryjoin are ClauseElement instances, to
      prevent more confusing errors later on.

    .. change::
        :tags: orm
        :tickets: 1236

      Improved mapper() check for non-class classes.

    .. change::
        :tags: orm
        :tickets: 5051

      comparator_factory argument is now documented and supported by
      all MapperProperty types, including column_property(),
      relation(), backref(), and synonym().

    .. change::
        :tags: orm
        :tickets: 

      Changed the name of PropertyLoader to RelationProperty, to be
      consistent with all the other names.  PropertyLoader is still
      present as a synonym.

    .. change::
        :tags: orm
        :tickets: 1099, 1228

      fixed "double iter()" call causing bus errors in shard API,
      removed errant result.close() left over from the 0.4
      version.

    .. change::
        :tags: orm
        :tickets: 

      made Session.merge cascades not trigger autoflush.  Fixes
      merged instances getting prematurely inserted with missing
      values.

    .. change::
        :tags: orm
        :tickets: 

      Two fixes to help prevent out-of-band columns from being
      rendered in polymorphic_union inheritance scenarios (which
      then causes extra tables to be rendered in the FROM clause
      causing cartesian products):
      
        - improvements to "column adaption" for a->b->c inheritance
          situations to better locate columns that are related to
          one another via multiple levels of indirection, rather
          than rendering the non-adapted column.
      
        - the "polymorphic discriminator" column is only rendered
          for the actual mapper being queried against. The column
          won't be "pulled in" from a subclass or superclass mapper
          since it's not needed.

    .. change::
        :tags: orm
        :tickets: 1072

      Fixed shard_id argument on ShardedSession.execute().

    .. change::
        :tags: sql
        :tickets: 1256

      Columns can again contain percent signs within their
      names.

    .. change::
        :tags: sql
        :tickets: 

      sqlalchemy.sql.expression.Function is now a public class.  It
      can be subclassed to provide user-defined SQL functions in an
      imperative style, including with pre-established behaviors.
      The postgis.py example illustrates one usage of this.

    .. change::
        :tags: sql
        :tickets: 

      PickleType now favors == comparison by default, if the
      incoming object (such as a dict) implements __eq__().  If the
      object does not implement __eq__() and mutable=True, a
      deprecation warning is raised.

    .. change::
        :tags: sql
        :tickets: 1215

      Fixed the import weirdness in sqlalchemy.sql to not export
      __names__.

    .. change::
        :tags: sql
        :tickets: 1238

      Using the same ForeignKey object repeatedly raises an error
      instead of silently failing later.

    .. change::
        :tags: sql
        :tickets: 

      Added NotImplementedError for params() method on
      Insert/Update/Delete constructs.  These items currently don't
      support this functionality, which also would be a little
      misleading compared to values().

    .. change::
        :tags: sql
        :tickets: 650

      Reflected foreign keys will properly locate their referenced
      column, even if the column was given a "key" attribute
      different from the reflected name.  This is achieved via a new
      flag on ForeignKey/ForeignKeyConstraint called "link_to_name",
      if True means the given name is the referred-to column's name,
      not its assigned key.

    .. change::
        :tags: sql
        :tickets: 1253

      select() can accept a ClauseList as a column in the same way
      as a Table or other selectable and the interior expressions
      will be used as column elements.

    .. change::
        :tags: sql
        :tickets: 

      the "passive" flag on session.is_modified() is correctly
      propagated to the attribute manager.

    .. change::
        :tags: sql
        :tickets: 

      union() and union_all() will not whack any order_by() that has
      been applied to the select()s inside.  If you union() a
      select() with order_by() (presumably to support LIMIT/OFFSET),
      you should also call self_group() on it to apply parenthesis.

    .. change::
        :tags: engine/pool
        :tickets: 1246

      Connection.invalidate() checks for closed status to avoid
      attribute errors.

    .. change::
        :tags: engine/pool
        :tickets: 1094

      NullPool supports reconnect on failure behavior.

    .. change::
        :tags: engine/pool
        :tickets: 799

      Added a mutex for the initial pool creation when using
      pool.manage(dbapi).  This prevents a minor case of "dogpile"
      behavior which would otherwise occur upon a heavy load
      startup.

    .. change::
        :tags: engine/pool
        :tickets: 

      _execute_clauseelement() goes back to being a private method.
      Subclassing Connection is not needed now that ConnectionProxy
      is available.

    .. change::
        :tags: documentation
        :tickets: 1149, 1200

      Tickets.

    .. change::
        :tags: documentation
        :tickets: 

      Added note about create_session() defaults.

    .. change::
        :tags: documentation
        :tickets: 

      Added section about metadata.reflect().

    .. change::
        :tags: documentation
        :tickets: 

      Updated `TypeDecorator` section.

    .. change::
        :tags: documentation
        :tickets: 

      Rewrote the "threadlocal" strategy section of the docs due to
      recent confusion over this feature.

    .. change::
        :tags: documentation
        :tickets: 

      Removed badly out of date 'polymorphic_fetch' and
      'select_table' docs from inheritance, reworked the second half
      of "joined table inheritance".

    .. change::
        :tags: documentation
        :tickets: 

      Documented `comparator_factory` kwarg, added new doc section
      "Custom Comparators".

    .. change::
        :tags: mssql
        :tickets: 1254

      Refactored the Date/Time types. The ``smalldatetime`` data
      type no longer truncates to a date only, and will now be
      mapped to the MSSmallDateTime type.

    .. change::
        :tags: mssql
        :tickets: 

      Corrected an issue with Numerics to accept an int.

    .. change::
        :tags: mssql
        :tickets: 

      Mapped ``char_length`` to the ``LEN()`` function.

    .. change::
        :tags: mssql
        :tickets: 

      If an ``INSERT`` includes a subselect the ``INSERT`` is
      converted from an ``INSERT INTO VALUES`` construct to a
      ``INSERT INTO SELECT`` construct.

    .. change::
        :tags: mssql
        :tickets: 

      If the column is part of a ``primary_key`` it will be ``NOT
      NULL`` since MSSQL doesn't allow ``NULL`` in primary_key
      columns.

    .. change::
        :tags: mssql
        :tickets: 1249

      ``MSBinary`` now returns a ``BINARY`` instead of an
      ``IMAGE``. This is a backwards incompatible change in that
      ``BINARY`` is a fixed length data type whereas ``IMAGE`` is a
      variable length data type.

    .. change::
        :tags: mssql
        :tickets: 1258

      ``get_default_schema_name`` is now reflected from the database
      based on the user's default schema. This only works with MSSQL
      2005 and later.

    .. change::
        :tags: mssql
        :tickets: 1248

      Added collation support through the use of a new collation
      argument. This is supported on the following types: char,
      nchar, varchar, nvarchar, text, ntext.

    .. change::
        :tags: mssql
        :tickets: 

      Changes to the connection string parameters favor DSN as the
      default specification for pyodbc. See the mssql.py docstring
      for detailed usage instructions.

    .. change::
        :tags: mssql
        :tickets: 

      Added experimental support of savepoints. It currently does
      not work fully with sessions.

    .. change::
        :tags: mssql
        :tickets: 1243

      Support for three levels of column nullability: NULL, NOT
      NULL, and the database's configured default.  The default
      Column configuration (nullable=True) will now generate NULL in
      the DDL. Previously no specification was emitted and the
      database default would take effect (usually NULL, but not
      always).  To explicitly request the database default,
      configure columns with nullable=None and no specification will
      be emitted in DDL. This is backwards incompatible
      behavior.

    .. change::
        :tags: postgres
        :tickets: 1267

      "%" signs in text() constructs are automatically escaped to
      "%%".  Because of the backwards incompatible nature of this
      change, a warning is emitted if '%%' is detected in the
      string.

    .. change::
        :tags: postgres
        :tickets: 

      Calling alias.execute() in conjunction with
      server_side_cursors won't raise AttributeError.

    .. change::
        :tags: postgres
        :tickets: 714

      Added Index reflection support to PostgreSQL, using a great
      patch we long neglected, submitted by Ken
      Kuhlman.

    .. change::
        :tags: oracle
        :tickets: 

      Adjusted the format of create_xid() to repair two-phase
      commit.  We now have field reports of Oracle two-phase commit
      working properly with this change.

    .. change::
        :tags: oracle
        :tickets: 1233

      Added OracleNVarchar type, produces NVARCHAR2, and also
      subclasses Unicode so that convert_unicode=True by default.
      NVARCHAR2 reflects into this type automatically so these
      columns pass unicode on a reflected table with no explicit
      convert_unicode=True flags.

    .. change::
        :tags: oracle
        :tickets: 1265

      Fixed bug which was preventing out params of certain types
      from being received; thanks a ton to huddlej at wwu.edu !

    .. change::
        :tags: mysql
        :tickets: 

      "%" signs in text() constructs are automatically escaped to
      "%%".  Because of the backwards incompatible nature of this
      change, a warning is emitted if '%%' is detected in the
      string.

    .. change::
        :tags: mysql
        :tickets: 1241

      Fixed bug in exception raise when FK columns not present
      during reflection.

    .. change::
        :tags: mysql
        :tickets: 

      Fixed bug involving reflection of a remote-schema table with a
      foreign key ref to another table in that schema.

    .. change::
        :tags: associationproxy
        :tickets: 

      The association proxy properties are make themselves available
      at the class level, e.g. MyClass.aproxy.  Previously this
      evaluated to None.

    .. change::
        :tags: declarative
        :tickets: 

      The full list of arguments accepted as string by backref()
      includes 'primaryjoin', 'secondaryjoin', 'secondary',
      'foreign_keys', 'remote_side', 'order_by'.

.. changelog::
    :version: 0.5.0rc4
    :released: Fri Nov 14 2008

    .. change::
        :tags: orm
        :tickets: 

      Query.count() has been enhanced to do the "right thing" in a
      wider variety of cases. It can now count multiple-entity
      queries, as well as column-based queries. Note that this means
      if you say query(A, B).count() without any joining criterion,
      it's going to count the cartesian product of A*B. Any query
      which is against column-based entities will automatically
      issue "SELECT count(1) FROM (SELECT...)" so that the real
      rowcount is returned, meaning a query such as
      query(func.count(A.name)).count() will return a value of one,
      since that query would return one row.

    .. change::
        :tags: orm
        :tickets: 

      Lots of performance tuning.  A rough guesstimate over various
      ORM operations places it 10% faster over 0.5.0rc3, 25-30% over
      0.4.8.

    .. change::
        :tags: orm
        :tickets: 

      bugfixes and behavioral changes

    .. change::
        :tags: general
        :tickets: 

      global "propigate"->"propagate" change.

    .. change::
        :tags: orm
        :tickets: 

      Adjustments to the enhanced garbage collection on
      InstanceState to better guard against errors due to lost
      state.

    .. change::
        :tags: orm
        :tickets: 1220

      Query.get() returns a more informative error message when
      executed against multiple entities.

    .. change::
        :tags: orm
        :tickets: 1140, 1221

      Restored NotImplementedError on Cls.relation.in_()

    .. change::
        :tags: orm
        :tickets: 1226

      Fixed PendingDeprecationWarning involving order_by parameter
      on relation().

    .. change::
        :tags: sql
        :tickets: 

      Removed the 'properties' attribute of the Connection object,
      Connection.info should be used.

    .. change::
        :tags: sql
        :tickets: 

      Restored "active rowcount" fetch before ResultProxy autocloses
      the cursor.  This was removed in 0.5rc3.

    .. change::
        :tags: sql
        :tickets: 

      Rearranged the `load_dialect_impl()` method in `TypeDecorator`
      such that it will take effect even if the user-defined
      `TypeDecorator` uses another `TypeDecorator` as its impl.

    .. change::
        :tags: access
        :tickets: 

      Added support for Currency type.

    .. change::
        :tags: access
        :tickets: 1017

      Functions were not return their result.

    .. change::
        :tags: access
        :tickets: 1017

      Corrected problem with joins. Access only support LEFT OUTER
      or INNER not just JOIN by itself.

    .. change::
        :tags: mssql
        :tickets: 

      Lots of cleanup and fixes to correct problems with limit and
      offset.

    .. change::
        :tags: mssql
        :tickets: 

      Correct situation where subqueries as part of a binary
      expression need to be translated to use the IN and NOT IN
      syntax.

    .. change::
        :tags: mssql
        :tickets: 1216

      Fixed E Notation issue that prevented the ability to insert
      decimal values less than 1E-6.

    .. change::
        :tags: mssql
        :tickets: 1217

      Corrected problems with reflection when dealing with schemas,
      particularly when those schemas are the default
      schema.

    .. change::
        :tags: mssql
        :tickets: 

      Corrected problem with casting a zero length item to a
      varchar. It now correctly adjusts the CAST.

    .. change::
        :tags: ext
        :tickets: 

      Can now use a custom "inherit_condition" in __mapper_args__
      when using declarative.

    .. change::
        :tags: ext
        :tickets: 

      fixed string-based "remote_side", "order_by" and others not
      propagating correctly when used in backref().

.. changelog::
    :version: 0.5.0rc3
    :released: Fri Nov 07 2008

    .. change::
        :tags: orm
        :tickets: 

      Added two new hooks to SessionExtension: after_bulk_delete()
      and after_bulk_update().  after_bulk_delete() is called after
      a bulk delete() operation on a query. after_bulk_update() is
      called after a bulk update() operation on a query.

    .. change::
        :tags: sql
        :tickets: 

      SQL compiler optimizations and complexity reduction. The call
      count for compiling a typical select() construct is 20% less
      versus 0.5.0rc2.

    .. change::
        :tags: sql
        :tickets: 1211

      Dialects can now generate label names of adjustable
      length. Pass in the argument "label_length=<value>" to
      create_engine() to adjust how many characters max will be
      present in dynamically generated column labels, i.e.
      "somecolumn AS somelabel". Any value less than 6 will result
      in a label of minimal size, consisting of an underscore and a
      numeric counter. The compiler uses the value of
      dialect.max_identifier_length as a default.

    .. change::
        :tags: ext
        :tickets: 

      Added a new extension sqlalchemy.ext.serializer.  Provides
      Serializer/Deserializer "classes" which mirror
      Pickle/Unpickle, as well as dumps() and loads(). This
      serializer implements an "external object" pickler which keeps
      key context-sensitive objects, including engines, sessions,
      metadata, Tables/Columns, and mappers, outside of the pickle
      stream, and can later restore the pickle using any
      engine/metadata/session provider. This is used not for
      pickling regular object instances, which are pickleable
      without any special logic, but for pickling expression objects
      and full Query objects, such that all mapper/engine/session
      dependencies can be restored at unpickle time.

    .. change::
        :tags: oracle
        :tickets: 

      Wrote a docstring for Oracle dialect. Apparently that Ohloh
      "few source code comments" label is starting to sting :).

    .. change::
        :tags: oracle
        :tickets: 536

      Removed FIRST_ROWS() optimize flag when using LIMIT/OFFSET,
      can be reenabled with optimize_limits=True create_engine()
      flag.

    .. change::
        :tags: oracle
        :tickets: 

      bugfixes and behavioral changes

    .. change::
        :tags: orm
        :tickets: 

      "not equals" comparisons of simple many-to-one relation to an
      instance will not drop into an EXISTS clause and will compare
      foreign key columns instead.

    .. change::
        :tags: orm
        :tickets: 

      Removed not-really-working use cases of comparing a collection
      to an iterable. Use contains() to test for collection
      membership.

    .. change::
        :tags: orm
        :tickets: 1171

      Improved the behavior of aliased() objects such that they more
      accurately adapt the expressions generated, which helps
      particularly with self-referential comparisons.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug involving primaryjoin/secondaryjoin conditions
      constructed from class-bound attributes (as often occurs when
      using declarative), which later would be inappropriately
      aliased by Query, particularly with the various EXISTS based
      comparators.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug when using multiple query.join() with an
      aliased-bound descriptor which would lose the left alias.

    .. change::
        :tags: orm
        :tickets: 

      Improved weakref identity map memory management to no longer
      require mutexing, resurrects garbage collected instance on a
      lazy basis for an InstanceState with pending changes.

    .. change::
        :tags: orm
        :tickets: 

      InstanceState object now removes circular references to itself
      upon disposal to keep it outside of cyclic garbage collection.

    .. change::
        :tags: orm
        :tickets: 

      relation() won't hide unrelated ForeignKey errors inside of
      the "please specify primaryjoin" message when determining join
      condition.

    .. change::
        :tags: orm
        :tickets: 1218

      Fixed bug in Query involving order_by() in conjunction with
      multiple aliases of the same class (will add tests in)

    .. change::
        :tags: orm
        :tickets: 

      When using Query.join() with an explicit clause for the ON
      clause, the clause will be aliased in terms of the left side
      of the join, allowing scenarios like query(Source).
      from_self().join((Dest, Source.id==Dest.source_id)) to work
      properly.

    .. change::
        :tags: orm
        :tickets: 

      polymorphic_union() function respects the "key" of each Column
      if they differ from the column's name.

    .. change::
        :tags: orm
        :tickets: 1183

      Repaired support for "passive-deletes" on a many-to-one
      relation() with "delete" cascade.

    .. change::
        :tags: orm
        :tickets: 1213

      Fixed bug in composite types which prevented a primary-key
      composite type from being mutated.

    .. change::
        :tags: orm
        :tickets: 1202

      Added more granularity to internal attribute access, such that
      cascade and flush operations will not initialize unloaded
      attributes and collections, leaving them intact for a
      lazy-load later on. Backref events still initialize attributes
      and collections for pending instances.

    .. change::
        :tags: sql
        :tickets: 1212

      Simplified the check for ResultProxy "autoclose without
      results" to be based solely on presence of
      cursor.description. All the regexp-based guessing about
      statements returning rows has been removed.

    .. change::
        :tags: sql
        :tickets: 1194

      Direct execution of a union() construct will properly set up
      result-row processing.

    .. change::
        :tags: sql
        :tickets: 

      The internal notion of an "OID" or "ROWID" column has been
      removed. It's basically not used by any dialect, and the
      possibility of its usage with psycopg2's cursor.lastrowid is
      basically gone now that INSERT..RETURNING is available.

    .. change::
        :tags: sql
        :tickets: 

      Removed "default_order_by()" method on all FromClause objects.

    .. change::
        :tags: sql
        :tickets: 

      Repaired the table.tometadata() method so that a passed-in
      schema argument is propagated to ForeignKey constructs.

    .. change::
        :tags: sql
        :tickets: 

      Slightly changed behavior of IN operator for comparing to
      empty collections. Now results in inequality comparison
      against self. More portable, but breaks with stored procedures
      that aren't pure functions.

    .. change::
        :tags: oracle
        :tickets: 

      Setting the auto_convert_lobs to False on create_engine() will
      also instruct the OracleBinary type to return the cx_oracle
      LOB object unchanged.

    .. change::
        :tags: mysql
        :tickets: 

      Fixed foreign key reflection in the edge case where a Table's
      explicit schema= is the same as the schema (database) the
      connection is attached to.

    .. change::
        :tags: mysql
        :tickets: 

      No longer expects include_columns in table reflection to be
      lower case.

    .. change::
        :tags: ext
        :tickets: 1174

      Fixed bug preventing declarative-bound "column" objects from
      being used in column_mapped_collection().

    .. change::
        :tags: misc
        :tickets: 1077

      util.flatten_iterator() func doesn't interpret strings with
      __iter__() methods as iterators, such as in pypy.

.. changelog::
    :version: 0.5.0rc2
    :released: Sun Oct 12 2008

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug involving read/write relation()s that contain
      literal or other non-column expressions within their
      primaryjoin condition equated to a foreign key column.

    .. change::
        :tags: orm
        :tickets: 

      "non-batch" mode in mapper(), a feature which allows mapper
      extension methods to be called as each instance is
      updated/inserted, now honors the insert order of the objects
      given.

    .. change::
        :tags: orm
        :tickets: 

      Fixed RLock-related bug in mapper which could deadlock upon
      reentrant mapper compile() calls, something that occurs when
      using declarative constructs inside of ForeignKey objects.

    .. change::
        :tags: orm
        :tickets: 

      ScopedSession.query_property now accepts a query_cls factory,
      overriding the session's configured query_cls.

    .. change::
        :tags: orm
        :tickets: 

      Fixed shared state bug interfering with ScopedSession.mapper's
      ability to apply default __init__ implementations on object
      subclasses.

    .. change::
        :tags: orm
        :tickets: 1177

      Fixed up slices on Query (i.e. query[x:y]) to work properly
      for zero length slices, slices with None on either end.

    .. change::
        :tags: orm
        :tickets: 

      Added an example illustrating Celko's "nested sets" as a
      SQLA mapping.

    .. change::
        :tags: orm
        :tickets: 

      contains_eager() with an alias argument works even when
      the alias is embedded in a SELECT, as when sent to the
      Query via query.select_from().

    .. change::
        :tags: orm
        :tickets: 1180

      contains_eager() usage is now compatible with a Query that
      also contains a regular eager load and limit/offset, in that
      the columns are added to the Query-generated subquery.

    .. change::
        :tags: orm
        :tickets: 

      session.execute() will execute a Sequence object passed to
      it (regression from 0.4).

    .. change::
        :tags: orm
        :tickets: 

      Removed the "raiseerror" keyword argument from object_mapper()
      and class_mapper().  These functions raise in all cases
      if the given class/instance is not mapped.

    .. change::
        :tags: orm
        :tickets: 

      Fixed session.transaction.commit() on a autocommit=False
      session not starting a new transaction.

    .. change::
        :tags: orm
        :tickets: 

      Some adjustments to Session.identity_map's weak referencing
      behavior to reduce asynchronous GC side effects.

    .. change::
        :tags: orm
        :tickets: 1182

      Adjustment to Session's post-flush accounting of newly
      "clean" objects to better protect against operating on
      objects as they're asynchronously gc'ed.

    .. change::
        :tags: sql
        :tickets: 1074

      column.in_(someselect) can now be used as a columns-clause
      expression without the subquery bleeding into the FROM clause

    .. change::
        :tags: sqlite
        :tickets: 968

      Overhauled SQLite date/time bind/result processing to use
      regular expressions and format strings, rather than
      strptime/strftime, to generically support pre-1900 dates,
      dates with microseconds.

    .. change::
        :tags: sqlite
        :tickets: 

      String's (and Unicode's, UnicodeText's, etc.) convert_unicode
      logic disabled in the sqlite dialect, to adjust for pysqlite
      2.5.0's new requirement that only Python unicode objects are
      accepted;
      http://itsystementwicklung.de/pipermail/list-pysqlite/2008-March/000018.html

    .. change::
        :tags: mysql
        :tickets: 

      Temporary tables are now reflectable.

    .. change::
        :tags: oracle
        :tickets: 1187

      Oracle will detect string-based statements which contain
      comments at the front before a SELECT as SELECT statements.

.. changelog::
    :version: 0.5.0rc1
    :released: Thu Sep 11 2008

    .. change::
        :tags: orm
        :tickets: 

      Query now has delete() and update(values) methods. This allows
      to perform bulk deletes/updates with the Query object.

    .. change::
        :tags: orm
        :tickets: 

      The RowTuple object returned by Query(\*cols) now features
      keynames which prefer mapped attribute names over column keys,
      column keys over column names, i.e.  Query(Class.foo,
      Class.bar) will have names "foo" and "bar" even if those are
      not the names of the underlying Column objects.  Direct Column
      objects such as Query(table.c.col) will return the "key"
      attribute of the Column.

    .. change::
        :tags: orm
        :tickets: 

      Added scalar() and value() methods to Query, each return a
      single scalar value.  scalar() takes no arguments and is
      roughly equivalent to first()[0], value()
      takes a single column expression and is roughly equivalent to
      values(expr).next()[0].

    .. change::
        :tags: orm
        :tickets: 

      Improved the determination of the FROM clause when placing SQL
      expressions in the query() list of entities.  In particular
      scalar subqueries should not "leak" their inner FROM objects
      out into the enclosing query.

    .. change::
        :tags: orm
        :tickets: 

      Joins along a relation() from a mapped class to a mapped
      subclass, where the mapped subclass is configured with single
      table inheritance, will include an IN clause which limits the
      subtypes of the joined class to those requested, within the ON
      clause of the join.  This takes effect for eager load joins as
      well as query.join().  Note that in some scenarios the IN
      clause will appear in the WHERE clause of the query as well
      since this discrimination has multiple trigger points.

    .. change::
        :tags: orm
        :tickets: 

      AttributeExtension has been refined such that the event
      is fired before the mutation actually occurs.  Additionally,
      the append() and set() methods must now return the given value,
      which is used as the value to be used in the mutation operation.
      This allows creation of validating AttributeListeners which
      raise before the action actually occurs, and which can change
      the given value into something else before its used.

    .. change::
        :tags: orm
        :tickets: 

      column_property(), composite_property(), and relation() now
      accept a single or list of AttributeExtensions using the
      "extension" keyword argument.

    .. change::
        :tags: orm
        :tickets: 

      query.order_by().get() silently drops the "ORDER BY" from
      the query issued by GET but does not raise an exception.

    .. change::
        :tags: orm
        :tickets: 

      Added a Validator AttributeExtension, as well as a
      @validates decorator which is used in a similar fashion
      as @reconstructor, and marks a method as validating
      one or more mapped attributes.

    .. change::
        :tags: orm
        :tickets: 1140

      class.someprop.in_() raises NotImplementedError pending the
      implementation of "in\_" for relation

    .. change::
        :tags: orm
        :tickets: 1127

      Fixed primary key update for many-to-many collections where
      the collection had not been loaded yet

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug whereby deferred() columns with a group in conjunction
      with an otherwise unrelated synonym() would produce
      an AttributeError during deferred load.

    .. change::
        :tags: orm
        :tickets: 1128

      The before_flush() hook on SessionExtension takes place before
      the list of new/dirty/deleted is calculated for the final
      time, allowing routines within before_flush() to further
      change the state of the Session before the flush proceeds.

    .. change::
        :tags: orm
        :tickets: 

      The "extension" argument to Session and others can now
      optionally be a list, supporting events sent to multiple
      SessionExtension instances.  Session places SessionExtensions
      in Session.extensions.

    .. change::
        :tags: orm
        :tickets: 

      Reentrant calls to flush() raise an error.  This also serves
      as a rudimentary, but not foolproof, check against concurrent
      calls to Session.flush().

    .. change::
        :tags: orm
        :tickets: 

      Improved the behavior of query.join() when joining to
      joined-table inheritance subclasses, using explicit join
      criteria (i.e. not on a relation).

    .. change::
        :tags: orm
        :tickets: 

      @orm.attributes.reconstitute and
      MapperExtension.reconstitute have been renamed to
      @orm.reconstructor and MapperExtension.reconstruct_instance

    .. change::
        :tags: orm
        :tickets: 1129

      Fixed @reconstructor hook for subclasses which inherit from a
      base class.

    .. change::
        :tags: orm
        :tickets: 1132

      The composite() property type now supports a
      __set_composite_values__() method on the composite class which
      is required if the class represents state using attribute
      names other than the column's keynames; default-generated
      values now get populated properly upon flush.  Also,
      composites with attributes set to None compare correctly.

    .. change::
        :tags: orm
        :tickets: 

      The 3-tuple of iterables returned by attributes.get_history()
      may now be a mix of lists and tuples.  (Previously members
      were always lists.)

    .. change::
        :tags: orm
        :tickets: 1151

      Fixed bug whereby changing a primary key attribute on an
      entity where the attribute's previous value had been expired
      would produce an error upon flush().

    .. change::
        :tags: orm
        :tickets: 

      Fixed custom instrumentation bug whereby get_instance_dict()
      was not called for newly constructed instances not loaded
      by the ORM.

    .. change::
        :tags: orm
        :tickets: 1150

      Session.delete() adds the given object to the session if
      not already present.  This was a regression bug from 0.4.

    .. change::
        :tags: orm
        :tickets: 

      The `echo_uow` flag on `Session` is deprecated, and unit-of-work
      logging is now application-level only, not per-session level.

    .. change::
        :tags: orm
        :tickets: 1153

      Removed conflicting `contains()` operator from
      `InstrumentedAttribute` which didn't accept `escape` kwaarg.

    .. change::
        :tags: declarative
        :tickets: 1161

      Fixed bug whereby mapper couldn't initialize if a composite
      primary key referenced another table that was not defined
      yet.

    .. change::
        :tags: declarative
        :tickets: 

      Fixed exception throw which would occur when string-based
      primaryjoin condition was used in conjunction with backref.

    .. change::
        :tags: schema
        :tickets: 1033

      Added "sorted_tables" accessor to MetaData, which returns
      Table objects sorted in order of dependency as a list.
      This deprecates the MetaData.table_iterator() method.
      The "reverse=False" keyword argument has also been
      removed from util.sort_tables(); use the Python
      'reversed' function to reverse the results.

    .. change::
        :tags: schema
        :tickets: 

      The 'length' argument to all Numeric types has been renamed
      to 'scale'.  'length' is deprecated and is still accepted
      with a warning.

    .. change::
        :tags: schema
        :tickets: 

      Dropped 0.3-compatibility for user defined types
      (convert_result_value, convert_bind_param).

    .. change::
        :tags: sql
        :tickets: 1068

      Temporarily rolled back the "ORDER BY" enhancement from.  This feature is on hold pending further
      development.

    .. change::
        :tags: sql
        :tickets: 

      The exists() construct won't "export" its contained list
      of elements as FROM clauses, allowing them to be used more
      effectively in the columns clause of a SELECT.

    .. change::
        :tags: sql
        :tickets: 798

      and_() and or_() now generate a ColumnElement, allowing
      boolean expressions as result columns, i.e.
      select([and_(1, 0)]).

    .. change::
        :tags: sql
        :tickets: 

      Bind params now subclass ColumnElement which allows them to be
      selectable by orm.query (they already had most ColumnElement
      semantics).

    .. change::
        :tags: sql
        :tickets: 

      Added select_from() method to exists() construct, which becomes
      more and more compatible with a regular select().

    .. change::
        :tags: sql
        :tickets: 1160

      Added func.min(), func.max(), func.sum() as "generic functions",
      which basically allows for their return type to be determined
      automatically.  Helps with dates on SQLite, decimal types,
      others.

    .. change::
        :tags: sql
        :tickets: 

      added decimal.Decimal as an "auto-detect" type; bind parameters
      and generic functions will set their type to Numeric when a
      Decimal is used.

    .. change::
        :tags: mysql
        :tickets: 

      The 'length' argument to MSInteger, MSBigInteger, MSTinyInteger,
      MSSmallInteger and MSYear has been renamed to 'display_width'.

    .. change::
        :tags: mysql
        :tickets: 1146

      Added MSMediumInteger type.

    .. change::
        :tags: mysql
        :tickets: 

      the function func.utc_timestamp() compiles to UTC_TIMESTAMP, without
      the parenthesis, which seem to get in the way when using in
      conjunction with executemany().

    .. change::
        :tags: oracle
        :tickets: 536

      limit/offset no longer uses ROW NUMBER OVER to limit rows,
      and instead uses subqueries in conjunction with a special
      Oracle optimization comment.  Allows LIMIT/OFFSET to work
      in conjunction with DISTINCT.

    .. change::
        :tags: oracle
        :tickets: 1155

      has_sequence() now takes the current "schema" argument into
      account

    .. change::
        :tags: oracle
        :tickets: 1121

      added BFILE to reflected type names

.. changelog::
    :version: 0.5.0beta3
    :released: Mon Aug 04 2008

    .. change::
        :tags: orm
        :tickets: 

      The "entity_name" feature of SQLAlchemy mappers has been
      removed.  For rationale, see http://tinyurl.com/6nm2ne

    .. change::
        :tags: orm
        :tickets: 

      the "autoexpire" flag on Session, sessionmaker(), and
      scoped_session() has been renamed to "expire_on_commit".  It
      does not affect the expiration behavior of rollback().

    .. change::
        :tags: orm
        :tickets: 

      fixed endless loop bug which could occur within a mapper's
      deferred load of inherited attributes.

    .. change::
        :tags: orm
        :tickets: 

      a legacy-support flag "_enable_transaction_accounting" flag
      added to Session which when False, disables all
      transaction-level object accounting, including expire on
      rollback, expire on commit, new/deleted list maintenance, and
      autoflush on begin.

    .. change::
        :tags: orm
        :tickets: 

      The 'cascade' parameter to relation() accepts None as a value,
      which is equivalent to no cascades.

    .. change::
        :tags: orm
        :tickets: 

      A critical fix to dynamic relations allows the "modified"
      history to be properly cleared after a flush().

    .. change::
        :tags: orm
        :tickets: 

      user-defined @properties on a class are detected and left in
      place during mapper initialization.  This means that a
      table-bound column of the same name will not be mapped at all
      if a @property is in the way (and the column is not remapped
      to a different name), nor will an instrumented attribute from
      an inherited class be applied.  The same rules apply for names
      excluded using the include_properties/exclude_properties
      collections.

    .. change::
        :tags: orm
        :tickets: 

      Added a new SessionExtension hook called after_attach().  This
      is called at the point of attachment for objects via add(),
      add_all(), delete(), and merge().

    .. change::
        :tags: orm
        :tickets: 1111

      A mapper which inherits from another, when inheriting the
      columns of its inherited mapper, will use any reassigned
      property names specified in that inheriting mapper.
      Previously, if "Base" had reassigned "base_id" to the name
      "id", "SubBase(Base)" would still get an attribute called
      "base_id".  This could be worked around by explicitly stating
      the column in each submapper as well but this is fairly
      unworkable and also impossible when using declarative.

    .. change::
        :tags: orm
        :tickets: 

      Fixed a series of potential race conditions in Session whereby
      asynchronous GC could remove unmodified, no longer referenced
      items from the session as they were present in a list of items
      to be processed, typically during session.expunge_all() and
      dependent methods.

    .. change::
        :tags: orm
        :tickets: 

      Some improvements to the _CompileOnAttr mechanism which should
      reduce the probability of "Attribute x was not replaced during
      compile" warnings. (this generally applies to SQLA hackers,
      like Elixir devs).

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug whereby the "unsaved, pending instance" FlushError
      raised for a pending orphan would not take superclass mappers
      into account when generating the list of relations responsible
      for the error.

    .. change::
        :tags: sql
        :tickets: 

      func.count() with no arguments renders as COUNT(*), equivalent
      to func.count(text('*')).

    .. change::
        :tags: sql
        :tickets: 1068

      simple label names in ORDER BY expressions render as
      themselves, and not as a re-statement of their corresponding
      expression.  This feature is currently enabled only for
      SQLite, MySQL, and PostgreSQL.  It can be enabled on other
      dialects as each is shown to support this
      behavior.

    .. change::
        :tags: ext
        :tickets: 

      Class-bound attributes sent as arguments to relation()'s
      remote_side and foreign_keys parameters are now accepted,
      allowing them to be used with declarative.  Additionally fixed
      bugs involving order_by being specified as a class-bound
      attribute in conjunction with eager loading.

    .. change::
        :tags: ext
        :tickets: 

      declarative initialization of Columns adjusted so that
      non-renamed columns initialize in the same way as a non
      declarative mapper.  This allows an inheriting mapper to set
      up its same-named "id" columns in particular such that the
      parent "id" column is favored over the child column, reducing
      database round trips when this value is requested.

    .. change::
        :tags: mysql
        :tickets: 1110

      Quoting of MSEnum values for use in CREATE TABLE is now
      optional & will be quoted on demand as required.  (Quoting was
      always optional for use with existing tables.)

.. changelog::
    :version: 0.5.0beta2
    :released: Mon Jul 14 2008

    .. change::
        :tags: orm
        :tickets: 870

      In addition to expired attributes, deferred attributes also
      load if their data is present in the result set.

    .. change::
        :tags: orm
        :tickets: 

      session.refresh() raises an informative error message if the
      list of attributes does not include any column-based
      attributes.

    .. change::
        :tags: orm
        :tickets: 

      query() raises an informative error message if no columns or
      mappers are specified.

    .. change::
        :tags: orm
        :tickets: 

      lazy loaders now trigger autoflush before proceeding.  This
      allows expire() of a collection or scalar relation to function
      properly in the context of autoflush.

    .. change::
        :tags: orm
        :tickets: 887

      column_property() attributes which represent SQL expressions
      or columns that are not present in the mapped tables (such as
      those from views) are automatically expired after an INSERT or
      UPDATE, assuming they have not been locally modified, so that
      they are refreshed with the most recent data upon access.

    .. change::
        :tags: orm
        :tickets: 1082

      Fixed explicit, self-referential joins between two
      joined-table inheritance mappers when using query.join(cls,
      aliased=True).

    .. change::
        :tags: orm
        :tickets: 

      Fixed query.join() when used in conjunction with a
      columns-only clause and a SQL-expression ON clause in the
      join.

    .. change::
        :tags: orm
        :tickets: 

      The "allow_column_override" flag from mapper() has been
      removed.  This flag is virtually always misunderstood.  Its
      specific functionality is available via the
      include_properties/exclude_properties mapper arguments.

    .. change::
        :tags: orm
        :tickets: 1066

      Repaired `__str__()` method on Query.

    .. change::
        :tags: orm
        :tickets: 

      Session.bind gets used as a default even when table/mapper
      specific binds are defined.

    .. change::
        :tags: schema
        :tickets: 1075

      Added prefixes option to `Table` that accepts a list of
      strings to insert after CREATE in the CREATE TABLE statement.

    .. change::
        :tags: schema
        :tickets: 

      Unicode, UnicodeText types now set "assert_unicode" and
      "convert_unicode" by default, but accept overriding
      \**kwargs for these values.

    .. change::
        :tags: sql
        :tickets: 

      Added new match() operator that performs a full-text search.
      Supported on PostgreSQL, SQLite, MySQL, MS-SQL, and Oracle
      backends.

    .. change::
        :tags: sqlite
        :tickets: 1090

      Modified SQLite's representation of "microseconds" to match
      the output of str(somedatetime), i.e. in that the microseconds
      are represented as fractional seconds in string format.  This
      makes SQLA's SQLite date type compatible with datetimes that
      were saved directly using Pysqlite (which just calls str()).
      Note that this is incompatible with the existing microseconds
      values in a SQLA 0.4 generated SQLite database file.
      
      To get the old behavior globally:
      
           from sqlalchemy.databases.sqlite import DateTimeMixin
           DateTimeMixin.__legacy_microseconds__ = True
      
      To get the behavior on individual DateTime types:
      
            t = sqlite.SLDateTime()
            t.__legacy_microseconds__ = True
      
      Then use "t" as the type on the Column.

    .. change::
        :tags: sqlite
        :tickets: 

      SQLite Date, DateTime, and Time types only accept Python
      datetime objects now, not strings.  If you'd like to format
      dates as strings yourself with SQLite, use a String type.  If
      you'd like them to return datetime objects anyway despite
      their accepting strings as input, make a TypeDecorator around
      String - SQLA doesn't encourage this pattern.

    .. change::
        :tags: extensions
        :tickets: 1096

      Declarative supports a __table_args__ class variable, which is
      either a dictionary, or tuple of the form (arg1, arg2, ...,
      {kwarg1:value, ...}) which contains positional + kw arguments
      to be passed to the Table constructor.

.. changelog::
    :version: 0.5.0beta1
    :released: Thu Jun 12 2008

    .. change::
        :tags: 
        :tickets: 

      The "__init__" trigger/decorator added by mapper now attempts
      to exactly mirror the argument signature of the original
      __init__.  The pass-through for '_sa_session' is no longer
      implicit- you must allow for this keyword argument in your
      constructor.

    .. change::
        :tags: 
        :tickets: 

      ClassState is renamed to ClassManager.

    .. change::
        :tags: 
        :tickets: 

      Classes may supply their own InstrumentationManager by
      providing a __sa_instrumentation_manager__ property.

    .. change::
        :tags: 
        :tickets: 

      Custom instrumentation may use any mechanism to associate a
      ClassManager with a class and an InstanceState with an
      instance.  Attributes on those objects are still the default
      association mechanism used by SQLAlchemy's native
      instrumentation.

    .. change::
        :tags: 
        :tickets: 

      Moved entity_name, _sa_session_id, and _instance_key from the
      instance object to the instance state.  These values are still
      available in the old way, which is now deprecated, using
      descriptors attached to the class.  A deprecation warning will
      be issued when accessed.

    .. change::
        :tags: 
        :tickets: 

      The _prepare_instrumentation alias for prepare_instrumentation
      has been removed.

    .. change::
        :tags: 
        :tickets: 

      sqlalchemy.exceptions has been renamed to sqlalchemy.exc.  The
      module may be imported under either name.

    .. change::
        :tags: 
        :tickets: 

      ORM-related exceptions are now defined in sqlalchemy.orm.exc.
      ConcurrentModificationError, FlushError, and
      UnmappedColumnError compatibility aliases are installed in
      sqlalchemy.exc during the import of sqlalchemy.orm.

    .. change::
        :tags: 
        :tickets: 

      sqlalchemy.logging has been renamed to sqlalchemy.log.

    .. change::
        :tags: 
        :tickets: 

      The transitional sqlalchemy.log.SADeprecationWarning alias for
      the warning's definition in sqlalchemy.exc has been removed.

    .. change::
        :tags: 
        :tickets: 

      exc.AssertionError has been removed and usage replaced with
      Python's built-in AssertionError.

    .. change::
        :tags: 
        :tickets: 

      The behavior of MapperExtensions attached to multiple,
      entity_name= primary mappers for a single class has been
      altered.  The first mapper() defined for a class is the only
      mapper eligible for the MapperExtension 'instrument_class',
      'init_instance' and 'init_failed' events.  This is backwards
      incompatible; previously the extensions of last mapper defined
      would receive these events.

    .. change::
        :tags: firebird
        :tickets: 

      Added support for returning values from inserts (2.0+ only),
      updates and deletes (2.1+ only).

    .. change::
        :tags: general
        :tickets: 

      global "propigate"->"propagate" change.

    .. change::
        :tags: orm
        :tickets: 

      polymorphic_union() function respects the "key" of each
      Column if they differ from the column's name.

    .. change::
        :tags: orm
        :tickets: 1199

      Fixed 0.4-only bug preventing composite columns
      from working properly with inheriting mappers

    .. change::
        :tags: orm
        :tickets: 

      Fixed RLock-related bug in mapper which could deadlock upon
      reentrant mapper compile() calls, something that occurs when
      using declarative constructs inside of ForeignKey objects.
      Ported from 0.5.

    .. change::
        :tags: orm
        :tickets: 1213

      Fixed bug in composite types which prevented a primary-key
      composite type from being mutated.

    .. change::
        :tags: orm
        :tickets: 976

      Added ScopedSession.is_active accessor.

    .. change::
        :tags: orm
        :tickets: 939

      Class-bound accessor can be used as the argument to
      relation() order_by.

    .. change::
        :tags: orm
        :tickets: 1072

      Fixed shard_id argument on ShardedSession.execute().

    .. change::
        :tags: sql
        :tickets: 1246

      Connection.invalidate() checks for closed status
      to avoid attribute errors.

    .. change::
        :tags: sql
        :tickets: 1094

      NullPool supports reconnect on failure behavior.

    .. change::
        :tags: sql
        :tickets: 1299

      The per-dialect cache used by TypeEngine to cache
      dialect-specific types is now a WeakKeyDictionary.
      This to prevent dialect objects from
      being referenced forever for an application that
      creates an arbitrarily large number of engines
      or dialects.   There is a small performance penalty
      which will be resolved in 0.6.

    .. change::
        :tags: sql
        :tickets: 

      Fixed SQLite reflection methods so that non-present
      cursor.description, which triggers an auto-cursor
      close, will be detected so that no results doesn't
      fail on recent versions of pysqlite which raise
      an error when fetchone() called with no rows present.

    .. change::
        :tags: postgres
        :tickets: 714

      Added Index reflection support to Postgres, using a
      great patch we long neglected, submitted by
      Ken Kuhlman.

    .. change::
        :tags: mysql
        :tickets: 1241

      Fixed bug in exception raise when FK columns not present
      during reflection.

    .. change::
        :tags: oracle
        :tickets: 1265

      Fixed bug which was preventing out params of certain types
      from being received; thanks a ton to huddlej at wwu.edu !
