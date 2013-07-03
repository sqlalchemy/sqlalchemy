
==============
0.8 Changelog
==============

.. changelog::
    :version: 0.8.2
    :released: July 3, 2013

    .. change::
        :tags: bug, mysql
        :tickets: 2768

        Fixed bug when using multi-table UPDATE where a supplemental
        table is a SELECT with its own bound parameters, where the positioning
        of the bound parameters would be reversed versus the statement
        itself when using MySQL's special syntax.

    .. change::
        :tags: bug, sqlite
        :tickets: 2764

        Added :class:`.BIGINT` to the list of type names that can be
        reflected by the SQLite dialect; courtesy Russell Stuart.

    .. change::
        :tags: feature, orm, declarative
        :tickets: 2761

        ORM descriptors such as hybrid properties can now be referenced
        by name in a string argument used with ``order_by``,
        ``primaryjoin``, or similar in :func:`.relationship`,
        in addition to column-bound attributes.

    .. change::
        :tags: feature, firebird
        :tickets: 2763

        Added new flag ``retaining=True`` to the kinterbasdb and fdb dialects.
        This controls the value of the ``retaining`` flag sent to the
        ``commit()`` and ``rollback()`` methods of the DBAPI connection.
        Due to historical concerns, this flag defaults to ``True``, however
        in 0.9 this flag will be defaulted to ``False``.

    .. change::
        :tags: requirements

        The Python `mock <https://pypi.python.org/pypi/mock>`_ library
        is now required in order to run the unit test suite.  While part
        of the standard library as of Python 3.3, previous Python installations
        will need to install this in order to run unit tests or to
        use the ``sqlalchemy.testing`` package for external dialects.

    .. change::
        :tags: bug, orm
        :tickets: 2750

        A warning is emitted when trying to flush an object of an inherited
        class where the polymorphic discriminator has been assigned
        to a value that is invalid for the class.

    .. change::
        :tags: bug, postgresql
        :tickets: 2740

        The behavior of :func:`.extract` has been simplified on the
        Postgresql dialect to no longer inject a hardcoded ``::timestamp``
        or similar cast into the given expression, as this interfered
        with types such as timezone-aware datetimes, but also
        does not appear to be at all necessary with modern versions
        of psycopg2.


    .. change::
        :tags: bug, firebird
        :tickets: 2757

        Type lookup when reflecting the Firebird types LONG and
        INT64 has been fixed so that LONG is treated as INTEGER,
        INT64 treated as BIGINT, unless the type has a "precision"
        in which case it's treated as NUMERIC.  Patch courtesy
        Russell Stuart.

    .. change::
        :tags: bug, postgresql
        :tickets: 2766

        Fixed bug in HSTORE type where keys/values that contained
        backslashed quotes would not be escaped correctly when
        using the "non native" (i.e. non-psycopg2) means
        of translating HSTORE data.  Patch courtesy Ryan Kelly.

    .. change::
        :tags: bug, postgresql
        :tickets: 2767

        Fixed bug where the order of columns in a multi-column
        Postgresql index would be reflected in the wrong order.
        Courtesy Roman Podolyaka.

    .. change::
        :tags: bug, sql
        :tickets: 2746, 2668

        Multiple fixes to the correlation behavior of
        :class:`.Select` constructs, first introduced in 0.8.0:

        * To satisfy the use case where FROM entries should be
          correlated outwards to a SELECT that encloses another,
          which then encloses this one, correlation now works
          across multiple levels when explicit correlation is
          established via :meth:`.Select.correlate`, provided
          that the target select is somewhere along the chain
          contained by a WHERE/ORDER BY/columns clause, not
          just nested FROM clauses. This makes
          :meth:`.Select.correlate` act more compatibly to
          that of 0.7 again while still maintaining the new
          "smart" correlation.

        * When explicit correlation is not used, the usual
          "implicit" correlation limits its behavior to just
          the immediate enclosing SELECT, to maximize compatibility
          with 0.7 applications, and also prevents correlation
          across nested FROMs in this case, maintaining compatibility
          with 0.8.0/0.8.1.

        * The :meth:`.Select.correlate_except` method was not
          preventing the given FROM clauses from correlation in
          all cases, and also would cause FROM clauses to be incorrectly
          omitted entirely (more like what 0.7 would do),
          this has been fixed.

        * Calling `select.correlate_except(None)` will enter
          all FROM clauses into correlation as would be expected.

    .. change::
        :tags: bug, ext

        Fixed bug whereby if a composite type were set up
        with a function instead of a class, the mutable extension
        would trip up when it tried to check that column
        for being a :class:`.MutableComposite` (which it isn't).
        Courtesy asldevi.

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
        Patch courtesy Matt Chisholm.

    .. change::
        :tags: feature, postgresql
        :tickets: 2072

        Added support for "AUTOCOMMIT" isolation when using the psycopg2
        DBAPI.   The keyword is available via the ``isolation_level``
        execution option.  Patch courtesy Roman Podolyaka.

    .. change::
        :tags: bug, orm
        :tickets: 2759

        Fixed bug in polymorphic SQL generation where multiple joined-inheritance
        entities against the same base class joined to each other as well
        would not track columns on the base table independently of each other if
        the string of joins were more than two entities long.

    .. change::
        :tags: bug, engine
        :pullreq: 6

        Fixed bug where the ``reset_on_return`` argument to various :class:`.Pool`
        implementations would not be propagated when the pool was regenerated.
        Courtesy Eevee.

    .. change::
        :tags: bug, orm
        :tickets: 2754

        Fixed bug where sending a composite attribute into :meth:`.Query.order_by`
        would produce a parenthesized expression not accepted by some databases.

    .. change::
        :tags: bug, orm
        :tickets: 2755

        Fixed the interaction between composite attributes and
        the :func:`.aliased` function.  Previously, composite attributes
        wouldn't work correctly in comparison operations when aliasing
        was applied.

    .. change::
        :tags: bug, mysql
        :tickets: 2715

        Added another conditional to the ``mysql+gaerdbms`` dialect to
        detect so-called "development" mode, where we should use the
        ``rdbms_mysqldb`` DBAPI.  Patch courtesy Brett Slatkin.

    .. change::
        :tags: feature, mysql
        :tickets: 2704

        The ``mysql_length`` parameter used with :class:`.Index` can now
        be passed as a dictionary of column names/lengths, for use
        with composite indexes.  Big thanks to Roman Podolyaka for the
        patch.

    .. change::
        :tags: bug, mssql
        :tickets: 2747

        When querying the information schema on SQL Server 2000, removed
        a CAST call that was added in 0.8.1 to help with driver issues,
        which apparently is not compatible on 2000.
        The CAST remains in place for SQL Server 2005 and greater.

    .. change::
        :tags: bug, mysql
        :tickets: 2721

        The ``deferrable`` keyword argument on :class:`.ForeignKey` and
        :class:`.ForeignKeyConstraint` will not render the ``DEFERRABLE`` keyword
        on the MySQL dialect.  For a long time we left this in place because
        a non-deferrable foreign key would act very differently than a deferrable
        one, but some environments just disable FKs on MySQL, so we'll be less
        opinionated here.

    .. change::
        :tags: bug, ext, orm
        :tickets: 2730

        Fixed bug where :class:`.MutableDict` didn't report a change event
        when ``clear()`` was called.

    .. change::
        :tags: bug, sql
        :tickets: 2738

        Fixed bug whereby joining a select() of a table "A" with multiple
        foreign key paths to a table "B", to that table "B", would fail
        to produce the "ambiguous join condition" error that would be
        reported if you join table "A" directly to "B"; it would instead
        produce a join condition with multiple criteria.

    .. change::
        :tags: bug, sql, reflection
        :tickets: 2728

        Fixed bug whereby using :meth:`.MetaData.reflect` across a remote
        schema as well as a local schema could produce wrong results
        in the case where both schemas had a table of the same name.

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
        ``__getitem__()`` on your custom operators!

    .. change::
      :tags: feature, orm
      :tickets: 2736

      Added a new method :meth:`.Query.select_entity_from` which
      will in 0.9 replace part of the functionality of
      :meth:`.Query.select_from`.  In 0.8, the two methods perform
      the same function, so that code can be migrated to use the
      :meth:`.Query.select_entity_from` method as appropriate.
      See the 0.9 migration guide for details.

    .. change::
      :tags: bug, orm
      :tickets: 2737

      Fixed a regression caused by :ticket:`2682` whereby the
      evaluation invoked by :meth:`.Query.update` and :meth:`.Query.delete`
      would hit upon unsupported ``True`` and ``False`` symbols
      which now appear due to the usage of ``IS``.

    .. change::
      :tags: bug, postgresql
      :tickets: 2735

      Fixed the HSTORE type to correctly encode/decode for unicode.
      This is always on, as the hstore is a textual type, and
      matches the behavior of psycopg2 when using Python 3.
      Courtesy Dmitry Mugtasimov.

    .. change::
      :tags: bug, examples

      Fixed a small bug in the dogpile example where the generation
      of SQL cache keys wasn't applying deduping labels to the
      statement the same way :class:`.Query` normally does.

    .. change::
      :tags: bug, engine, sybase
      :tickets: 2732

      Fixed a bug where the routine to detect the correct kwargs
      being sent to :func:`.create_engine` would fail in some cases,
      such as with the Sybase dialect.

    .. change::
      :tags: bug, orm
      :tickets: 2481

      Fixed a regression from 0.7 caused by this ticket, which
      made the check for recursion overflow in self-referential
      eager joining too loose, missing a particular circumstance
      where a subclass had lazy="joined" or "subquery" configured
      and the load was a "with_polymorphic" against the base.

    .. change::
      :tags: bug, orm
      :tickets: 2718

      Fixed a regression from 0.7 where the contextmanager feature
      of :meth:`.Session.begin_nested` would fail to correctly
      roll back the transaction when a flush error occurred, instead
      raising its own exception while leaving the session still
      pending a rollback.

    .. change::
      :tags: bug, mysql

      Updated mysqlconnector dialect to check for disconnect based
      on the apparent string message sent in the exception; tested
      against mysqlconnector 1.0.9.

    .. change::
      :tags: bug, sql, mssql
      :tickets: 2682

      Regression from this ticket caused the unsupported keyword
      "true" to render, added logic to convert this to 1/0
      for SQL server.

.. changelog::
    :version: 0.8.1
    :released: April 27, 2013

    .. change::
      :tags: bug, orm
      :tickets: 2698

      Fixes to the ``sqlalchemy.ext.serializer`` extension, including
      that the "id" passed from the pickler is turned into a string
      to prevent against bytes being parsed on Py3K, as well as that
      ``relationship()`` and ``orm.join()`` constructs are now properly
      serialized.

    .. change::
      :tags: bug, orm
      :tickets: 2714

      A significant improvement to the inner workings of query.join(),
      such that the decisionmaking involved on how to join has been
      dramatically simplified.  New test cases now pass such as
      multiple joins extending from the middle of an already complex
      series of joins involving inheritance and such.   Joining from
      deeply nested subquery structures is still complicated and
      not without caveats, but with these improvements the edge
      cases are hopefully pushed even farther out to the edges.

    .. change::
      :tags: feature, orm
      :tickets: 2673

      Added a convenience method to Query that turns a query into an
      EXISTS subquery of the form
      ``EXISTS (SELECT 1 FROM ... WHERE ...)``.

    .. change::
      :tags: bug, orm

      Added a conditional to the unpickling process for ORM
      mapped objects, such that if the reference to the object
      were lost when the object was pickled, we don't
      erroneously try to set up _sa_instance_state - fixes
      a NoneType error.

    .. change::
      :tags: bug, postgresql
      :tickets: 2712

      Opened up the checking for "disconnect" with psycopg2/libpq
      to check for all the various "disconnect" messages within
      the full exception hierarchy.  Specifically the
      "closed the connection unexpectedly" message has now been
      seen in at least three different exception types.
      Courtesy Eli Collins.

    .. change::
      :tags: bug, sql, mysql
      :tickets: 2682

      Fully implemented the IS and IS NOT operators with
      regards to the True/False constants.  An expression like
      ``col.is_(True)`` will now render ``col IS true``
      on the target platform, rather than converting the True/
      False constant to an integer bound parameter.
      This allows the ``is_()`` operator to work on MySQL when
      given True/False constants.

    .. change::
      :tags: bug, postgresql
      :tickets: 2681

      The operators for the Postgresql ARRAY type supports
      input types of sets, generators, etc. even when
      a dimension is not specified, by turning the given
      iterable into a collection unconditionally.

    .. change::
      :tags: bug, mysql
      :pullreq: 55

      Fixes to support the latest cymysql DBAPI, courtesy
      Hajime Nakagami.

    .. change::
      :tags: bug, mysql
      :tickets: 2663

      Improvements to the operation of the pymysql dialect on
      Python 3, including some important decode/bytes steps.
      Issues remain with BLOB types due to driver issues.
      Courtesy Ben Trofatter.

    .. change::
      :tags: bug, orm
      :tickets: 2710

      Fixed bug where many-to-many relationship with uselist=False
      would fail to delete the association row and raise an error
      if the scalar attribute were set to None.  This was a
      regression introduced by the changes for :ticket:`2229`.

    .. change::
      :tags: bug, orm
      :tickets: 2708

      Improved the behavior of instance management regarding
      the creation of strong references within the Session;
      an object will no longer have an internal reference cycle
      created if it's in the transient state or moves into the
      detached state - the strong ref is created only when the
      object is attached to a Session and is removed when the
      object is detached.  This makes it somewhat safer for an
      object to have a `__del__()` method, even though this is
      not recommended, as relationships with backrefs produce
      cycles too.  A warning has been added when a class with
      a `__del__()` method is mapped.

    .. change::
      :tags: bug, sql
      :tickets: 2702

      A major fix to the way in which a select() object produces
      labeled columns when apply_labels() is used; this mode
      produces a SELECT where each column is labeled as in
      <tablename>_<columnname>, to remove column name collisions
      for a multiple table select.   The fix is that if two labels
      collide when combined with the table name, i.e.
      "foo.bar_id" and "foo_bar.id", anonymous aliasing will be
      applied to one of the dupes.  This allows the ORM to handle
      both columns independently; previously, 0.7
      would in some cases silently emit a second SELECT for the
      column that was "duped", and in 0.8 an ambiguous column error
      would be emitted.   The "keys" applied to the .c. collection
      of the select() will also be deduped, so that the "column
      being replaced" warning will no longer emit for any select()
      that specifies use_labels, though the dupe key will be given
      an anonymous label which isn't generally user-friendly.

    .. change::
      :tags: bug, mysql
      :pullreq: 54

      Updated a regexp to correctly extract error code on
      google app engine v1.7.5 and newer.  Courtesy
      Dan Ring.

    .. change::
      :tags: bug, examples

      Fixed a long-standing bug in the caching example, where
      the limit/offset parameter values wouldn't be taken into
      account when computing the cache key.  The
      _key_from_query() function has been simplified to work
      directly from the final compiled statement in order to get
      at both the full statement as well as the fully processed
      parameter list.

    .. change::
      :tags: bug, mssql
      :tickets: 2355

      Part of a longer series of fixes needed for pyodbc+
      mssql, a CAST to NVARCHAR(max) has been added to the bound
      parameter for the table name and schema name in all information schema
      queries to avoid the issue of comparing NVARCHAR to NTEXT,
      which seems to be rejected by the ODBC driver in some cases,
      such as FreeTDS (0.91 only?) plus unicode bound parameters being passed.
      The issue seems to be specific to the SQL Server information
      schema tables and the workaround is harmless for those cases
      where the problem doesn't exist in the first place.

    .. change::
      :tags: bug, sql
      :tickets: 2691

      Fixed bug where disconnect detect on error would
      raise an attribute error if the error were being
      raised after the Connection object had already
      been closed.

    .. change::
      :tags: bug, sql
      :tickets: 2703

      Reworked internal exception raises that emit
      a rollback() before re-raising, so that the stack
      trace is preserved from sys.exc_info() before entering
      the rollback.  This so that the traceback is preserved
      when using coroutine frameworks which may have switched
      contexts before the rollback function returns.

    .. change::
      :tags: bug, orm
      :tickets: 2697

      Fixed bug whereby ORM would run the wrong kind of
      query when refreshing an inheritance-mapped class
      where the superclass was mapped to a non-Table
      object, like a custom join() or a select(),
      running a query that assumed a hierarchy that's
      mapped to individual Table-per-class.

    .. change::
      :tags: bug, orm

      Fixed `__repr__()` on mapper property constructs
      to work before the object is initialized, so
      that Sphinx builds with recent Sphinx versions
      can read them.

    .. change::
      :tags: bug, sql, postgresql

      The _Binary base type now converts values through
      the bytes() callable when run on Python 3; in particular
      psycopg2 2.5 with Python 3.3 seems to now be returning
      the "memoryview" type, so this is converted to bytes
      before return.

    .. change::
      :tags: bug, sql
      :tickets: 2695

      Improvements to Connection auto-invalidation
      handling.  If a non-disconnect error occurs,
      but leads to a delayed disconnect error within error
      handling (happens with MySQL), the disconnect condition
      is detected.  The Connection can now also be closed
      when in an invalid state, meaning it will raise "closed"
      on next usage, and additionally the "close with result"
      feature will work even if the autorollback in an error
      handling routine fails and regardless of whether the
      condition is a disconnect or not.


    .. change::
      :tags: bug, orm, declarative
      :tickets: 2656

      Fixed indirect regression regarding :func:`.has_inherited_table`,
      where since it considers the current class' ``__table__``, was
      sensitive to when it was called.  This is 0.7's behavior also,
      but in 0.7 things tended to "work out" within events like
      ``__mapper_args__()``.  :func:`.has_inherited_table` now only
      considers superclasses, so should return the same answer
      regarding the current class no matter when it's called
      (obviously assuming the state of the superclass).

    .. change::
      :tags: bug, orm
      :tickets: 2699

      Fixed bug when a query of the form:
      ``query(SubClass).options(subqueryload(Baseclass.attrname))``,
      where ``SubClass`` is a joined inh of ``BaseClass``,
      would fail to apply the ``JOIN`` inside the subquery
      on the attribute load, producing a cartesian product.
      The populated results still tended to be correct as additional
      rows are just ignored, so this issue may be present as a
      performance degradation in applications that are
      otherwise working correctly.  Also in 0.7.11.

    .. change::
      :tags: bug, orm
      :tickets: 2689

      Fixed bug in unit of work whereby a joined-inheritance
      subclass could insert the row for the "sub" table
      before the parent table, if the two tables had no
      ForeignKey constraints set up between them.
      Also in 0.7.11.

    .. change::
      :tags: bug, mssql
      :pullreq: 47

      Added support for additional "disconnect" messages
      to the pymssql dialect.  Courtesy John Anderson.

    .. change::
      :tags: feature, sql

      Loosened the check on dialect-specific argument names
      passed to Table(); since we want to support external dialects
      and also want to support args without a certain dialect
      being installed, it only checks the format of the arg now,
      rather than looking for that dialect in sqlalchemy.dialects.

    .. change::
      :tags: bug, sql

      Fixed bug whereby a DBAPI that can return "0"
      for cursor.lastrowid would not function correctly
      in conjunction with :attr:`.ResultProxy.inserted_primary_key`.

    .. change::
      :tags: bug, mssql
      :tickets: 2683
      :pullreq: 46

      Fixed Py3K bug regarding "binary" types and
      pymssql.  Courtesy Marc Abramowitz.

    .. change::
      :tags: bug, postgresql
      :tickets: 2680

      Added missing HSTORE type to postgresql type names
      so that the type can be reflected.

.. changelog::
    :version: 0.8.0
    :released: March 9, 2013

    .. note::

      There are some new behavioral changes as of 0.8.0
      not present in 0.8.0b2.  They are present in the
      migration document as follows:

      * :ref:`legacy_is_orphan_addition`

      * :ref:`metadata_create_drop_tables`

      * :ref:`correlation_context_specific`

    .. change::
        :tags: feature, postgresql
        :tickets: 2676

      Added support for Postgresql's traditional SUBSTRING
      function syntax, renders as "SUBSTRING(x FROM y FOR z)"
      when regular ``func.substring()`` is used.
      Also in 0.7.11.  Courtesy Gunnlaugur Þór Briem.

    .. change::
        :tags: feature, orm
        :tickets: 2675

      A meaningful :attr:`.QueryableAttribute.info` attribute is
      added, which proxies down to the ``.info`` attribute on either
      the :class:`.schema.Column` object if directly present, or
      the :class:`.MapperProperty` otherwise.  The full behavior
      is documented and ensured by tests to remain stable.

    .. change::
        :tags: bug, sql
        :tickets: 2668

      The behavior of SELECT correlation has been improved such that
      the :meth:`.Select.correlate` and :meth:`.Select.correlate_except`
      methods, as well as their ORM analogues, will still retain
      "auto-correlation" behavior in that the FROM clause is modified
      only if the output would be legal SQL; that is, the FROM clause
      is left intact if the correlated SELECT is not used in the context
      of an enclosing SELECT inside of the WHERE, columns, or HAVING clause.
      The two methods now only specify conditions to the default
      "auto correlation", rather than absolute FROM lists.

    .. change::
        :tags: feature, mysql
        :pullreq: 42

      New dialect for CyMySQL added, courtesy Hajime Nakagami.

    .. change::
        :tags: bug, orm
        :tickets: 2674

      Improved checking for an existing backref name conflict during
      mapper configuration; will now test for name conflicts on
      superclasses and subclasses, in addition to the current mapper,
      as these conflicts break things just as much.  This is new for
      0.8, but see below for a warning that will also be triggered
      in 0.7.11.

    .. change::
        :tags: bug, orm
        :tickets: 2674

      Improved the error message emitted when a "backref loop" is detected,
      that is when an attribute event triggers a bidirectional
      assignment between two other attributes with no end.
      This condition can occur not just when an object of the wrong
      type is assigned, but also when an attribute is mis-configured
      to backref into an existing backref pair.  Also in 0.7.11.

    .. change::
      :tags: bug, orm
      :tickets: 2674

      A warning is emitted when a MapperProperty is assigned to a mapper
      that replaces an existing property, if the properties in question
      aren't plain column-based properties.   Replacement of relationship
      properties is rarely (ever?) what is intended and usually refers to a
      mapper mis-configuration.   Also in 0.7.11.

    .. change::
        :tags: feature, orm

      Can set/change the "cascade" attribute on a :func:`.relationship`
      construct after it's been constructed already.  This is not
      a pattern for normal use but we like to change the setting
      for demonstration purposes in tutorials.

    .. change::
        :tags: bug, schema
        :tickets: 2664

      :meth:`.MetaData.create_all` and :meth:`.MetaData.drop_all` will
      now accommodate an empty list as an instruction to not create/drop
      any items, rather than ignoring the collection.


    .. change::
        :tags: bug, tests
        :tickets: 2669
        :pullreq: 41

      Fixed an import of "logging" in test_execute which was not
      working on some linux platforms.  Also in 0.7.11.

    .. change::
        :tags: bug, orm
        :tickets: 2662

      A clear error message is emitted if an event handler
      attempts to emit SQL on a Session within the after_commit()
      handler, where there is not a viable transaction in progress.

    .. change::
        :tags: bug, orm
        :tickets: 2665

      Detection of a primary key change within the process
      of cascading a natural primary key update will succeed
      even if the key is composite and only some of the
      attributes have changed.

    .. change::
        :tags: feature, orm
        :tickets: 2658

      Added new helper function :func:`.was_deleted`, returns True
      if the given object was the subject of a :meth:`.Session.delete`
      operation.

    .. change::
        :tags: bug, orm
        :tickets: 2658

      An object that's deleted from a session will be de-associated with
      that session fully after the transaction is committed, that is
      the :func:`.object_session` function will return None.

    .. change::
        :tags: bug, oracle

      The cx_oracle dialect will no longer run the bind parameter names
      through ``encode()``, as this is not valid on Python 3, and prevented
      statements from functioning correctly on Python 3.  We now
      encode only if ``supports_unicode_binds`` is False, which is not
      the case for cx_oracle when at least version 5 of cx_oracle is used.

    .. change::
        :tags: bug, orm
        :tickets: 2661

      Fixed bug whereby :meth:`.Query.yield_per` would set the execution
      options incorrectly, thereby breaking subsequent usage of the
      :meth:`.Query.execution_options` method.  Courtesy Ryan Kelly.

    .. change::
        :tags: bug, orm
        :tickets: 1768

      Fixed the consideration of the ``between()`` operator
      so that it works correctly with the new relationship local/remote
      system.

    .. change::
        :tags: bug, sql
        :tickets: 2660, 1768

      Fixed a bug regarding column annotations which in particular
      could impact some usages of the new :func:`.orm.remote` and
      :func:`.orm.local` annotation functions, where annotations
      could be lost when the column were used in a subsequent
      expression.

    .. change::
        :tags: bug, mysql, gae
        :tickets: 2649

      Added a conditional import to the ``gaerdbms`` dialect which attempts
      to import rdbms_apiproxy vs. rdbms_googleapi to work
      on both dev and production platforms.  Also now honors the
      ``instance`` attribute.  Courtesy Sean Lynch.
      Also in 0.7.10.

    .. change::
        :tags: bug, sql
        :tickets: 2496

      The :meth:`.ColumnOperators.in_` operator will now coerce
      values of ``None`` to :func:`.null`.

    .. change::
        :tags: feature, sql
        :tickets: 2657

      Added a new argument to :class:`.Enum` and its base
      :class:`.SchemaType` ``inherit_schema``.  When set to ``True``,
      the type will set its ``schema`` attribute of that of the
      :class:`.Table` to which it is associated.  This also occurs
      during a :meth:`.Table.tometadata` operation; the :class:`.SchemaType`
      is now copied in all cases when :meth:`.Table.tometadata` happens,
      and if ``inherit_schema=True``, the type will take on the new
      schema name passed to the method.   The ``schema`` is important
      when used with the Postgresql backend, as the type results in
      a ``CREATE TYPE`` statement.

    .. change::
        :tags: feature, postgresql
        :pullreq: 40

      Added :meth:`.postgresql.ARRAY.Comparator.any` and
      :meth:`.postgresql.ARRAY.Comparator.all`
      methods, as well as standalone expression constructs.   Big thanks
      to Audrius Kažukauskas for the terrific work here.

    .. change::
        :tags: sql, bug
        :tickets: 2643

        Fixed bug where :meth:`.Table.tometadata` would fail if a
        :class:`.Column` had both a foreign key as well as an
        alternate ".key" name for the column.   Also in 0.7.10.

    .. change::
        :tags: sql, bug
        :tickets: 2629

        insert().returning() raises an informative CompileError if attempted
        to compile on a dialect that doesn't support RETURNING.

    .. change::
        :tags: orm, bug
        :tickets: 2655

        the consideration of a pending object as
        an "orphan" has been modified to more closely match the
        behavior as that of persistent objects, which is that the object
        is expunged from the :class:`.Session` as soon as it is
        de-associated from any of its orphan-enabled parents.  Previously,
        the pending object would be expunged only if de-associated
        from all of its orphan-enabled parents.  The new flag ``legacy_is_orphan``
        is added to :func:`.orm.mapper` which re-establishes the
        legacy behavior.

        See the change note and example case at :ref:`legacy_is_orphan_addition`
        for a detailed discussion of this change.

    .. change::
        :tags: orm, bug
        :tickets: 2653

      Fixed the (most likely never used) "@collection.link" collection
      method, which fires off each time the collection is associated
      or de-associated with a mapped object - the decorator
      was not tested or functional.  The decorator method
      is now named :meth:`.collection.linker` though the name "link"
      remains for backwards compatibility.  Courtesy Luca Wehrstedt.

    .. change::
        :tags: orm, bug
        :tickets: 2654

      Made some fixes to the system of producing custom instrumented
      collections, mainly that the usage of the @collection decorators
      will now honor the __mro__ of the given class, applying the
      logic of the sub-most classes' version of a particular collection
      method.   Previously, it wasn't predictable when subclassing
      an existing instrumented class such as :class:`.MappedCollection`
      whether or not custom methods would resolve correctly.

    .. change::
      :tags: orm, removed

      The undocumented (and hopefully unused) system of producing
      custom collections using an ``__instrumentation__`` datastructure
      associated with the collection has been removed, as this was a complex
      and untested feature which was also essentially redundant versus the
      decorator approach.   Other internal simplifcations to the
      orm.collections module have been made as well.

    .. change::
        :tags: mssql, feature
        :pullreq: 35

      Added ``mssql_include`` and ``mssql_clustered`` options to
      :class:`.Index`, renders the ``INCLUDE`` and ``CLUSTERED`` keywords,
      respectively.  Courtesy Derek Harland.

    .. change::
        :tags: sql, feature
        :tickets: 695

      :class:`.Index` now supports arbitrary SQL expressions and/or
      functions, in addition to straight columns.   Common modifiers
      include using ``somecolumn.desc()`` for a descending index and
      ``func.lower(somecolumn)`` for a case-insensitive index, depending on the
      capabilities of the target backend.

    .. change::
        :tags: mssql, bug
        :tickets: 2638

      Added a py3K conditional around unnecessary .decode()
      call in mssql information schema, fixes reflection
      in Py3K. Also in 0.7.10.

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
      an event dispatcher.  Also in 0.7.10.

    .. change::
        :tags: mssql, bug

      Fixed a regression whereby the "collation" parameter
      of the character types CHAR, NCHAR, etc. stopped working,
      as "collation" is now supported by the base string types.
      The TEXT, NCHAR, CHAR, VARCHAR types within the
      MSSQL dialect are now synonyms for the base types.

    .. change::
        :tags: mssql, feature
        :tickets: 2644
        :pullreq: 32

      DDL for IDENTITY columns is now supported on
      non-primary key columns, by establishing a
      :class:`.Sequence` construct on any
      integer column.  Courtesy Derek Harland.

    .. change::
        :tags: examples, bug

      Fixed a regression in the examples/dogpile_caching example
      which was due to the change in :ticket:`2614`.

    .. change::
        :tags: orm, bug
        :tickets: 2640

      :meth:`.Query.merge_result` can now load rows from an outer join
      where an entity may be ``None`` without throwing an error.
      Also in 0.7.10.

    .. change::
        :tags: sql, bug
        :tickets: 2648

      Tweaked the "REQUIRED" symbol used by the compiler to identify
      INSERT/UPDATE bound parameters that need to be passed, so that
      it's more easily identifiable when writing custom bind-handling
      code.

    .. change::
        :tags: postgresql, bug

      Fixed bug in :func:`.postgresql.array` construct whereby using it
      inside of an :func:`.expression.insert` construct would produce an
      error regarding a parameter issue in the ``self_group()`` method.

    .. change::
        :tags: orm, feature

      Extended the :doc:`/core/inspection` system so that all Python descriptors
      associated with the ORM or its extensions can be retrieved.
      This fulfills the common request of being able to inspect
      all :class:`.QueryableAttribute` descriptors in addition to
      extension types such as :class:`.hybrid_property` and
      :class:`.AssociationProxy`.  See :attr:`.Mapper.all_orm_descriptors`.

    .. change::
        :tags: mysql, feature
        :pullreq: 33

      GAE dialect now accepts username/password arguments in the URL,
      courtesy Owen Nelson.

    .. change::
        :tags: mysql, bug
        :pullreq: 33

      GAE dialect won't fail on None match if the error code can't be extracted
      from the exception throw; courtesy Owen Nelson.

    .. change::
        :tags: orm, bug
        :tickets: 2637

      Fixes to the "dynamic" loader on :func:`.relationship`, includes
      that backrefs will work properly even when autoflush is disabled,
      history events are more accurate in scenarios where multiple add/remove
      of the same object occurs.

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
