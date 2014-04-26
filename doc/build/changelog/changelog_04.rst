
==============
0.4 Changelog
==============

                
.. changelog::
    :version: 0.4.8
    :released: Sun Oct 12 2008

    .. change::
        :tags: orm
        :tickets: 1039

      Fixed bug regarding inherit_condition passed
      with "A=B" versus "B=A" leading to errors

    .. change::
        :tags: orm
        :tickets: 

      Changes made to new, dirty and deleted
      collections in
      SessionExtension.before_flush() will take
      effect for that flush.

    .. change::
        :tags: orm
        :tickets: 

      Added label() method to InstrumentedAttribute
      to establish forwards compatibility with 0.5.

    .. change::
        :tags: sql
        :tickets: 1074

      column.in_(someselect) can now be used as
      a columns-clause expression without the subquery
      bleeding into the FROM clause

    .. change::
        :tags: mysql
        :tickets: 1146

      Added MSMediumInteger type.

    .. change::
        :tags: sqlite
        :tickets: 968

      Supplied a custom strftime() function which
      handles dates before 1900.

    .. change::
        :tags: sqlite
        :tickets: 

      String's (and Unicode's, UnicodeText's, etc.)
      convert_unicode logic disabled in the sqlite dialect,
      to adjust for pysqlite 2.5.0's new requirement that
      only Python unicode objects are accepted;
      http://itsystementwicklung.de/pipermail/list-pysqlite/2008-March/000018.html

    .. change::
        :tags: oracle
        :tickets: 1155

      has_sequence() now takes schema name into account

    .. change::
        :tags: oracle
        :tickets: 1121

      added BFILE to the list of reflected types

.. changelog::
    :version: 0.4.7p1
    :released: Thu Jul 31 2008

    .. change::
        :tags: orm
        :tickets: 

      Added "add()" and "add_all()" to scoped_session
      methods.  Workaround for 0.4.7::
      
        from sqlalchemy.orm.scoping import ScopedSession, instrument
        setattr(ScopedSession, "add", instrument("add"))
        setattr(ScopedSession, "add_all", instrument("add_all"))

    .. change::
        :tags: orm
        :tickets: 

      Fixed non-2.3 compatible usage of set() and generator
      expression within relation().

.. changelog::
    :version: 0.4.7
    :released: Sat Jul 26 2008

    .. change::
        :tags: orm
        :tickets: 1058

      The contains() operator when used with many-to-many
      will alias() the secondary (association) table so
      that multiple contains() calls will not conflict
      with each other

    .. change::
        :tags: orm
        :tickets: 

      fixed bug preventing merge() from functioning in
      conjunction with a comparable_property()

    .. change::
        :tags: orm
        :tickets: 

      the enable_typechecks=False setting on relation()
      now only allows subtypes with inheriting mappers.
      Totally unrelated types, or subtypes not set up with
      mapper inheritance against the target mapper are
      still not allowed.

    .. change::
        :tags: orm
        :tickets: 976

      Added is_active flag to Sessions to detect when
      a transaction is in progress.  This
      flag is always True with a "transactional"
      (in 0.5 a non-"autocommit") Session.

    .. change::
        :tags: sql
        :tickets: 

      Fixed bug when calling select([literal('foo')])
      or select([bindparam('foo')]).

    .. change::
        :tags: schema
        :tickets: 571

      create_all(), drop_all(), create(), drop() all raise
      an error if the table name or schema name contains
      more characters than that dialect's configured
      character limit.  Some DB's can handle too-long
      table names during usage, and SQLA can handle this
      as well. But various reflection/
      checkfirst-during-create scenarios fail since we are
      looking for the name within the DB's catalog tables.

    .. change::
        :tags: schema
        :tickets: 571, 820

      The index name generated when you say "index=True"
      on a Column is truncated to the length appropriate
      for the dialect. Additionally, an Index with a too-
      long name cannot be explicitly dropped with
      Index.drop(), similar to.

    .. change::
        :tags: postgres
        :tickets: 

      Repaired server_side_cursors to properly detect
      text() clauses.

    .. change::
        :tags: postgres
        :tickets: 1092

      Added PGCidr type.

    .. change::
        :tags: mysql
        :tickets: 

      Added 'CALL' to the list of SQL keywords which return
      result rows.

    .. change::
        :tags: oracle
        :tickets: 

      Oracle get_default_schema_name() "normalizes" the name
      before returning, meaning it returns a lower-case name
      when the identifier is detected as case insensitive.

    .. change::
        :tags: oracle
        :tickets: 709

      creating/dropping tables takes schema name into account
      when searching for the existing table, so that tables
      in other owner namespaces with the same name do not
      conflict

    .. change::
        :tags: oracle
        :tickets: 1062

      Cursors now have "arraysize" set to 50 by default on
      them, the value of which is configurable using the
      "arraysize" argument to create_engine() with the
      Oracle dialect.  This to account for cx_oracle's default
      setting of "1", which has the effect of many round trips
      being sent to Oracle.  This actually works well in
      conjunction with BLOB/CLOB-bound cursors, of which
      there are any number available but only for the life of
      that row request (so BufferedColumnRow is still needed,
      but less so).

    .. change::
        :tags: oracle
        :tickets: 

      sqlite
          - add SLFloat type, which matches the SQLite REAL
            type affinity.  Previously, only SLNumeric was provided
            which fulfills NUMERIC affinity, but that's not the
            same as REAL.

.. changelog::
    :version: 0.4.6
    :released: Sat May 10 2008

    .. change::
        :tags: orm
        :tickets: 

      Fix to the recent relation() refactoring which fixes
      exotic viewonly relations which join between local and
      remote table multiple times, with a common column shared
      between the joins.

    .. change::
        :tags: orm
        :tickets: 

      Also re-established viewonly relation() configurations
      that join across multiple tables.

    .. change::
        :tags: orm
        :tickets: 610

      Added experimental relation() flag to help with
      primaryjoins across functions, etc.,
      _local_remote_pairs=[tuples].  This complements a complex
      primaryjoin condition allowing you to provide the
      individual column pairs which comprise the relation's
      local and remote sides.  Also improved lazy load SQL
      generation to handle placing bind params inside of
      functions and other expressions.  (partial progress
      towards)

    .. change::
        :tags: orm
        :tickets: 1036

      repaired single table inheritance such that you
      can single-table inherit from a joined-table inherting
      mapper without issue.

    .. change::
        :tags: orm
        :tickets: 1027

      Fixed "concatenate tuple" bug which could occur with
      Query.order_by() if clause adaption had taken place.

    .. change::
        :tags: orm
        :tickets: 

      Removed ancient assertion that mapped selectables require
      "alias names" - the mapper creates its own alias now if
      none is present.  Though in this case you need to use the
      class, not the mapped selectable, as the source of column
      attributes - so a warning is still issued.

    .. change::
        :tags: orm
        :tickets: 

      fixes to the "exists" function involving inheritance (any(),
      has(), ~contains()); the full target join will be rendered
      into the EXISTS clause for relations that link to subclasses.

    .. change::
        :tags: orm
        :tickets: 

      restored usage of append_result() extension method for primary
      query rows, when the extension is present and only a single-
      entity result is being returned.

    .. change::
        :tags: orm
        :tickets: 

      Also re-established viewonly relation() configurations that
      join across multiple tables.

    .. change::
        :tags: orm
        :tickets: 

      removed ancient assertion that mapped selectables require
      "alias names" - the mapper creates its own alias now if
      none is present.  Though in this case you need to use
      the class, not the mapped selectable, as the source of
      column attributes - so a warning is still issued.

    .. change::
        :tags: orm
        :tickets: 1015

      refined mapper._save_obj() which was unnecessarily calling
      __ne__() on scalar values during flush

    .. change::
        :tags: orm
        :tickets: 1019

      added a feature to eager loading whereby subqueries set
      as column_property() with explicit label names (which is not
      necessary, btw) will have the label anonymized when
      the instance is part of the eager join, to prevent
      conflicts with a subquery or column of the same name
      on the parent object.

    .. change::
        :tags: orm
        :tickets: 

      set-based collections \|=, -=, ^= and &= are stricter about
      their operands and only operate on sets, frozensets or
      subclasses of the collection type. Previously, they would
      accept any duck-typed set.

    .. change::
        :tags: orm
        :tickets: 

      added an example dynamic_dict/dynamic_dict.py, illustrating
      a simple way to place dictionary behavior on top of
      a dynamic_loader.

    .. change::
        :tags: declarative, extension
        :tickets: 

      Joined table inheritance mappers use a slightly relaxed
      function to create the "inherit condition" to the parent
      table, so that other foreign keys to not-yet-declared
      Table objects don't trigger an error.

    .. change::
        :tags: declarative, extension
        :tickets: 

      fixed reentrant mapper compile hang when
      a declared attribute is used within ForeignKey,
      ie. ForeignKey(MyOtherClass.someattribute)

    .. change::
        :tags: sql
        :tickets: 

      Added COLLATE support via the .collate(<collation>)
      expression operator and collate(<expr>, <collation>) sql
      function.

    .. change::
        :tags: sql
        :tickets: 

      Fixed bug with union() when applied to non-Table connected
      select statements

    .. change::
        :tags: sql
        :tickets: 1014

      improved behavior of text() expressions when used as
      FROM clauses, such as select().select_from(text("sometext"))

    .. change::
        :tags: sql
        :tickets: 1021

      Column.copy() respects the value of "autoincrement",
      fixes usage with Migrate

    .. change::
        :tags: engines
        :tickets: 

      Pool listeners can now be provided as a dictionary of
      callables or a (possibly partial) duck-type of
      PoolListener, your choice.

    .. change::
        :tags: engines
        :tickets: 

      added "rollback_returned" option to Pool which will
      disable the rollback() issued when connections are
      returned.  This flag is only safe to use with a database
      which does not support transactions (i.e. MySQL/MyISAM).

    .. change::
        :tags: ext
        :tickets: 

      set-based association proxies \|=, -=, ^= and &= are
      stricter about their operands and only operate on sets,
      frozensets or other association proxies. Previously, they
      would accept any duck-typed set.

    .. change::
        :tags: mssql
        :tickets: 1005

      Added "odbc_autotranslate" parameter to engine / dburi
      parameters. Any given string will be passed through to the
      ODBC connection string as:
      
            "AutoTranslate=%s" % odbc_autotranslate

    .. change::
        :tags: mssql
        :tickets: 

      Added "odbc_options" parameter to engine / dburi
      parameters. The given string is simply appended to the
      SQLAlchemy-generated odbc connection string.
      
      This should obviate the need of adding a myriad of ODBC
      options in the future.

    .. change::
        :tags: firebird
        :tickets: 

      Handle the "SUBSTRING(:string FROM :start FOR :length)"
      builtin.

.. changelog::
    :version: 0.4.5
    :released: Fri Apr 04 2008

    .. change::
        :tags: orm
        :tickets: 

      A small change in behavior to session.merge() - existing
      objects are checked for based on primary key attributes, not
      necessarily _instance_key.  So the widely requested
      capability, that:
      
            x = MyObject(id=1)
            x = sess.merge(x)
      
      will in fact load MyObject with id #1 from the database if
      present, is now available.  merge() still copies the state
      of the given object to the persistent one, so an example
      like the above would typically have copied "None" from all
      attributes of "x" onto the persistent copy.  These can be
      reverted using session.expire(x).

    .. change::
        :tags: orm
        :tickets: 

      Also fixed behavior in merge() whereby collection elements
      present on the destination but not the merged collection
      were not being removed from the destination.

    .. change::
        :tags: orm
        :tickets: 995

      Added a more aggressive check for "uncompiled mappers",
      helps particularly with declarative layer

    .. change::
        :tags: orm
        :tickets: 

      The methodology behind "primaryjoin"/"secondaryjoin" has
      been refactored.  Behavior should be slightly more
      intelligent, primarily in terms of error messages which
      have been pared down to be more readable.  In a slight
      number of scenarios it can better resolve the correct
      foreign key than before.

    .. change::
        :tags: orm
        :tickets: 

      Added comparable_property(), adds query Comparator
      behavior to regular, unmanaged Python properties

    .. change::
        :tags: orm, Company.employees.of_type(Engineer), 'machines'
        :tickets: 

      the functionality of query.with_polymorphic() has
      been added to mapper() as a configuration option.
      
      It's set via several forms:
            with_polymorphic='*'
            with_polymorphic=[mappers]
            with_polymorphic=('*', selectable)
            with_polymorphic=([mappers], selectable)
      
      This controls the default polymorphic loading strategy
      for inherited mappers. When a selectable is not given,
      outer joins are created for all joined-table inheriting
      mappers requested. Note that the auto-create of joins
      is not compatible with concrete table inheritance.
      
      The existing select_table flag on mapper() is now
      deprecated and is synonymous with
      with_polymorphic('*', select_table).  Note that the
      underlying "guts" of select_table have been
      completely removed and replaced with the newer,
      more flexible approach.
      
      The new approach also automatically allows eager loads
      to work for subclasses, if they are present, for
      example::

        sess.query(Company).options(
         eagerload_all(
        ))

      to load Company objects, their employees, and the
      'machines' collection of employees who happen to be
      Engineers. A "with_polymorphic" Query option should be
      introduced soon as well which would allow per-Query
      control of with_polymorphic() on relations.

    .. change::
        :tags: orm
        :tickets: 

      added two "experimental" features to Query,
      "experimental" in that their specific name/behavior
      is not carved in stone just yet:  _values() and
      _from_self().  We'd like feedback on these.
      
      - _values(\*columns) is given a list of column
        expressions, and returns a new Query that only
        returns those columns. When evaluated, the return
        value is a list of tuples just like when using
        add_column() or add_entity(), the only difference is
        that "entity zero", i.e. the mapped class, is not
        included in the results. This means it finally makes
        sense to use group_by() and having() on Query, which
        have been sitting around uselessly until now.
      
        A future change to this method may include that its
        ability to join, filter and allow other options not
        related to a "resultset" are removed, so the feedback
        we're looking for is how people want to use
        _values()...i.e. at the very end, or do people prefer
        to continue generating after it's called.
      
      - _from_self() compiles the SELECT statement for the
        Query (minus any eager loaders), and returns a new
        Query that selects from that SELECT. So basically you
        can query from a Query without needing to extract the
        SELECT statement manually. This gives meaning to
        operations like query[3:5]._from_self().filter(some
        criterion). There's not much controversial here
        except that you can quickly create highly nested
        queries that are less efficient, and we want feedback
        on the naming choice.

    .. change::
        :tags: orm
        :tickets: 

      query.order_by() and query.group_by() will accept
      multiple arguments using \*args (like select()
      already does).

    .. change::
        :tags: orm
        :tickets: 

      Added some convenience descriptors to Query:
      query.statement returns the full SELECT construct,
      query.whereclause returns just the WHERE part of the
      SELECT construct.

    .. change::
        :tags: orm
        :tickets: 

      Fixed/covered case when using a False/0 value as a
      polymorphic discriminator.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug which was preventing synonym() attributes from
      being used with inheritance

    .. change::
        :tags: orm
        :tickets: 996

      Fixed SQL function truncation of trailing underscores

    .. change::
        :tags: orm
        :tickets: 

      When attributes are expired on a pending instance, an
      error will not be raised when the "refresh" action is
      triggered and no result is found.

    .. change::
        :tags: orm
        :tickets: 

      Session.execute can now find binds from metadata

    .. change::
        :tags: orm
        :tickets: 

      Adjusted the definition of "self-referential" to be any
      two mappers with a common parent (this affects whether or
      not aliased=True is required when joining with Query).

    .. change::
        :tags: orm
        :tickets: 

      Made some fixes to the "from_joinpoint" argument to
      query.join() so that if the previous join was aliased and
      this one isn't, the join still happens successfully.

    .. change::
        :tags: orm
        :tickets: 895

      Assorted "cascade deletes" fixes:
        - Fixed "cascade delete" operation of dynamic relations,
          which had only been implemented for foreign-key
          nulling behavior in 0.4.2 and not actual cascading
          deletes
      
        - Delete cascade without delete-orphan cascade on a
          many-to-one will not delete orphans which were
          disconnected from the parent before session.delete()
          is called on the parent (one-to-many already had
          this).
      
        - Delete cascade with delete-orphan will delete orphans
          whether or not it remains attached to its also-deleted
          parent.
      
        - delete-orphan casacde is properly detected on relations
          that are present on superclasses when using inheritance.

    .. change::
        :tags: orm
        :tickets: 

      Fixed order_by calculation in Query to properly alias
      mapper-config'ed order_by when using select_from()

    .. change::
        :tags: orm
        :tickets: 

      Refactored the diffing logic that kicks in when replacing
      one collection with another into collections.bulk_replace,
      useful to anyone building multi-level collections.

    .. change::
        :tags: orm
        :tickets: 

      Cascade traversal algorithm converted from recursive to
      iterative to support deep object graphs.

    .. change::
        :tags: sql
        :tickets: 999

      schema-qualified tables now will place the schemaname
      ahead of the tablename in all column expressions as well
      as when generating column labels.  This prevents cross-
      schema name collisions in all cases

    .. change::
        :tags: sql
        :tickets: 

      can now allow selects which correlate all FROM clauses
      and have no FROM themselves.  These are typically
      used in a scalar context, i.e. SELECT x, (SELECT x WHERE y)
      FROM table.  Requires explicit correlate() call.

    .. change::
        :tags: sql
        :tickets: 

      'name' is no longer a required constructor argument for
      Column().  It (and .key) may now be deferred until the
      column is added to a Table.

    .. change::
        :tags: sql
        :tickets: 791, 993

      like(), ilike(), contains(), startswith(), endswith() take
      an optional keyword argument "escape=<somestring>", which
      is set as the escape character using the syntax "x LIKE y
      ESCAPE '<somestring>'".

    .. change::
        :tags: sql
        :tickets: 

      random() is now a generic sql function and will compile to
      the database's random implementation, if any.

    .. change::
        :tags: sql
        :tickets: 

      update().values() and insert().values() take keyword
      arguments.

    .. change::
        :tags: sql
        :tickets: 

      Fixed an issue in select() regarding its generation of
      FROM clauses, in rare circumstances two clauses could be
      produced when one was intended to cancel out the other.
      Some ORM queries with lots of eager loads might have seen
      this symptom.

    .. change::
        :tags: sql
        :tickets: 

      The case() function now also takes a dictionary as its
      whens parameter.  It also interprets the "THEN"
      expressions as values by default, meaning case([(x==y,
      "foo")]) will interpret "foo" as a bound value, not a SQL
      expression.  use text(expr) for literal SQL expressions in
      this case.  For the criterion itself, these may be literal
      strings only if the "value" keyword is present, otherwise
      SA will force explicit usage of either text() or
      literal().

    .. change::
        :tags: oracle
        :tickets: 

      The "owner" keyword on Table is now deprecated, and is
      exactly synonymous with the "schema" keyword.  Tables can
      now be reflected with alternate "owner" attributes,
      explicitly stated on the Table object or not using
      "schema".

    .. change::
        :tags: oracle
        :tickets: 

      All of the "magic" searching for synonyms, DBLINKs etc.
      during table reflection are disabled by default unless you
      specify "oracle_resolve_synonyms=True" on the Table
      object.  Resolving synonyms necessarily leads to some
      messy guessing which we'd rather leave off by default.
      When the flag is set, tables and related tables will be
      resolved against synonyms in all cases, meaning if a
      synonym exists for a particular table, reflection will use
      it when reflecting related tables.  This is stickier
      behavior than before which is why it's off by default.

    .. change::
        :tags: declarative, extension
        :tickets: 

      The "synonym" function is now directly usable with
      "declarative".  Pass in the decorated property using the
      "descriptor" keyword argument, e.g.: somekey =
      synonym('_somekey', descriptor=property(g, s))

    .. change::
        :tags: declarative, extension
        :tickets: 

      The "deferred" function is usable with "declarative".
      Simplest usage is to declare deferred and Column together,
      e.g.: data = deferred(Column(Text))

    .. change::
        :tags: declarative, extension
        :tickets: 

      Declarative also gained @synonym_for(...) and
      @comparable_using(...), front-ends for synonym and
      comparable_property.

    .. change::
        :tags: declarative, extension
        :tickets: 995

      Improvements to mapper compilation when using declarative;
      already-compiled mappers will still trigger compiles of
      other uncompiled mappers when used

    .. change::
        :tags: declarative, extension
        :tickets: 

      Declarative will complete setup for Columns lacking names,
      allows a more DRY syntax.
      
        class Foo(Base):
            __tablename__ = 'foos'
            id = Column(Integer, primary_key=True)

    .. change::
        :tags: declarative, extension
        :tickets: 

      inheritance in declarative can be disabled when sending
      "inherits=None" to __mapper_args__.

    .. change::
        :tags: declarative, extension
        :tickets: 

      declarative_base() takes optional kwarg "mapper", which
      is any callable/class/method that produces a mapper,
      such as declarative_base(mapper=scopedsession.mapper).
      This property can also be set on individual declarative
      classes using the "__mapper_cls__" property.

    .. change::
        :tags: postgres
        :tickets: 1001

      Got PG server side cursors back into shape, added fixed
      unit tests as part of the default test suite.  Added
      better uniqueness to the cursor ID

    .. change::
        :tags: oracle
        :tickets: 

      The "owner" keyword on Table is now deprecated, and is
      exactly synonymous with the "schema" keyword.  Tables can
      now be reflected with alternate "owner" attributes,
      explicitly stated on the Table object or not using
      "schema".

    .. change::
        :tags: oracle
        :tickets: 

      All of the "magic" searching for synonyms, DBLINKs etc.
      during table reflection are disabled by default unless you
      specify "oracle_resolve_synonyms=True" on the Table
      object.  Resolving synonyms necessarily leads to some
      messy guessing which we'd rather leave off by default.
      When the flag is set, tables and related tables will be
      resolved against synonyms in all cases, meaning if a
      synonym exists for a particular table, reflection will use
      it when reflecting related tables.  This is stickier
      behavior than before which is why it's off by default.

    .. change::
        :tags: mssql
        :tickets: 979

      Reflected tables will now automatically load other tables
      which are referenced by Foreign keys in the auto-loaded
      table,.

    .. change::
        :tags: mssql
        :tickets: 916

      Added executemany check to skip identity fetch,.

    .. change::
        :tags: mssql
        :tickets: 884

      Added stubs for small date type.

    .. change::
        :tags: mssql
        :tickets: 

      Added a new 'driver' keyword parameter for the pyodbc dialect.
      Will substitute into the ODBC connection string if given,
      defaults to 'SQL Server'.

    .. change::
        :tags: mssql
        :tickets: 

      Added a new 'max_identifier_length' keyword parameter for
      the pyodbc dialect.

    .. change::
        :tags: mssql
        :tickets: 

      Improvements to pyodbc + Unix. If you couldn't get that
      combination to work before, please try again.

    .. change::
        :tags: mysql
        :tickets: 

      The connection.info keys the dialect uses to cache server
      settings have changed and are now namespaced.

.. changelog::
    :version: 0.4.4
    :released: Wed Mar 12 2008

    .. change::
        :tags: sql
        :tickets: 975

      Can again create aliases of selects against textual FROM
      clauses.

    .. change::
        :tags: sql
        :tickets: 

      The value of a bindparam() can be a callable, in which
      case it's evaluated at statement execution time to get the
      value.

    .. change::
        :tags: sql
        :tickets: 978

      Added exception wrapping/reconnect support to result set
      fetching.  Reconnect works for those databases that raise
      a catchable data error during results (i.e. doesn't work
      on MySQL)

    .. change::
        :tags: sql
        :tickets: 936

      Implemented two-phase API for "threadlocal" engine, via
      engine.begin_twophase(), engine.prepare()

    .. change::
        :tags: sql
        :tickets: 986

      Fixed bug which was preventing UNIONS from being
      cloneable.

    .. change::
        :tags: sql
        :tickets: 

      Added "bind" keyword argument to insert(), update(),
      delete() and DDL(). The .bind property is now assignable
      on those statements as well as on select().

    .. change::
        :tags: sql
        :tickets: 

      Insert statements can now be compiled with extra "prefix"
      words between INSERT and INTO, for vendor extensions like
      MySQL's INSERT IGNORE INTO table.

    .. change::
        :tags: orm
        :tickets: 

      any(), has(), contains(), ~contains(), attribute level ==
      and != now work properly with self-referential relations -
      the clause inside the EXISTS is aliased on the "remote"
      side to distinguish it from the parent table.  This
      applies to single table self-referential as well as
      inheritance-based self-referential.

    .. change::
        :tags: orm
        :tickets: 985

      Repaired behavior of == and != operators at the relation()
      level when compared against NULL for one-to-one relations

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug whereby session.expire() attributes were not
      loading on an polymorphically-mapped instance mapped by a
      select_table mapper.

    .. change::
        :tags: orm
        :tickets: 

      Added query.with_polymorphic() - specifies a list of
      classes which descend from the base class, which will be
      added to the FROM clause of the query.  Allows subclasses
      to be used within filter() criterion as well as eagerly
      loads the attributes of those subclasses.

    .. change::
        :tags: orm
        :tickets: 

      Your cries have been heard: removing a pending item from
      an attribute or collection with delete-orphan expunges the
      item from the session; no FlushError is raised.  Note that
      if you session.save()'ed the pending item explicitly, the
      attribute/collection removal still knocks it out.

    .. change::
        :tags: orm
        :tickets: 

      session.refresh() and session.expire() raise an error when
      called on instances which are not persistent within the
      session

    .. change::
        :tags: orm
        :tickets: 

      Fixed potential generative bug when the same Query was
      used to generate multiple Query objects using join().

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug which was introduced in 0.4.3, whereby loading
      an already-persistent instance mapped with joined table
      inheritance would trigger a useless "secondary" load from
      its joined table, when using the default "select"
      polymorphic_fetch.  This was due to attributes being
      marked as expired during its first load and not getting
      unmarked from the previous "secondary" load.  Attributes
      are now unexpired based on presence in __dict__ after any
      load or commit operation succeeds.

    .. change::
        :tags: orm
        :tickets: 

      Deprecated Query methods apply_sum(), apply_max(),
      apply_min(), apply_avg().  Better methodologies are
      coming....

    .. change::
        :tags: orm
        :tickets: 

      relation() can accept a callable for its first argument,
      which returns the class to be related.  This is in place
      to assist declarative packages to define relations without
      classes yet being in place.

    .. change::
        :tags: orm
        :tickets: 

      Added a new "higher level" operator called "of_type()":
      used in join() as well as with any() and has(), qualifies
      the subclass which will be used in filter criterion, e.g.:
      
        query.filter(Company.employees.of_type(Engineer).
          any(Engineer.name=='foo'))
      
        or
      
        query.join(Company.employees.of_type(Engineer)).
          filter(Engineer.name=='foo')

    .. change::
        :tags: orm
        :tickets: 

      Preventive code against a potential lost-reference bug in
      flush().

    .. change::
        :tags: orm
        :tickets: 

      Expressions used in filter(), filter_by() and others, when
      they make usage of a clause generated from a relation
      using the identity of a child object (e.g.,
      filter(Parent.child==<somechild>)), evaluate the actual
      primary key value of <somechild> at execution time so that
      the autoflush step of the Query can complete, thereby
      populating the PK value of <somechild> in the case that
      <somechild> was pending.

    .. change::
        :tags: orm
        :tickets: 

      setting the relation()-level order by to a column in the
      many-to-many "secondary" table will now work with eager
      loading, previously the "order by" wasn't aliased against
      the secondary table's alias.

    .. change::
        :tags: orm
        :tickets: 

      Synonyms riding on top of existing descriptors are now
      full proxies to those descriptors.

    .. change::
        :tags: dialects
        :tickets: 

      Invalid SQLite connection URLs now raise an error.

    .. change::
        :tags: dialects
        :tickets: 981

      postgres TIMESTAMP renders correctly

    .. change::
        :tags: dialects
        :tickets: 

      postgres PGArray is a "mutable" type by default; when used
      with the ORM, mutable-style equality/ copy-on-write
      techniques are used to test for changes.

    .. change::
        :tags: extensions
        :tickets: 

      a new super-small "declarative" extension has been added,
      which allows Table and mapper() configuration to take
      place inline underneath a class declaration.  This
      extension differs from ActiveMapper and Elixir in that it
      does not redefine any SQLAlchemy semantics at all; literal
      Column, Table and relation() constructs are used to define
      the class behavior and table definition.

.. changelog::
    :version: 0.4.3
    :released: Thu Feb 14 2008

    .. change::
        :tags: sql
        :tickets: 

      Added "schema.DDL", an executable free-form DDL statement.
      DDLs can be executed in isolation or attached to Table or
      MetaData instances and executed automatically when those
      objects are created and/or dropped.

    .. change::
        :tags: sql
        :tickets: 

      Table columns and constraints can be overridden on a an
      existing table (such as a table that was already reflected)
      using the 'useexisting=True' flag, which now takes into
      account the arguments passed along with it.

    .. change::
        :tags: sql
        :tickets: 

      Added a callable-based DDL events interface, adds hooks
      before and after Tables and MetaData create and drop.

    .. change::
        :tags: sql
        :tickets: 

      Added generative where(<criterion>) method to delete() and
      update() constructs which return a new object with criterion
      joined to existing criterion via AND, just like
      select().where().

    .. change::
        :tags: sql
        :tickets: 727

      Added "ilike()" operator to column operations.  Compiles to
      ILIKE on postgres, lower(x) LIKE lower(y) on all
      others.

    .. change::
        :tags: sql
        :tickets: 943

      Added "now()" as a generic function; on SQLite, Oracle
      and MSSQL compiles as "CURRENT_TIMESTAMP"; "now()" on
      all others.

    .. change::
        :tags: sql
        :tickets: 962

      The startswith(), endswith(), and contains() operators now
      concatenate the wildcard operator with the given operand in
      SQL, i.e. "'%' || <bindparam>" in all cases, accept
      text('something') operands properly

    .. change::
        :tags: sql
        :tickets: 962

      cast() accepts text('something') and other non-literal
      operands properly

    .. change::
        :tags: sql
        :tickets: 

      fixed bug in result proxy where anonymously generated
      column labels would not be accessible using their straight
      string name

    .. change::
        :tags: sql
        :tickets: 

      Deferrable constraints can now be defined.

    .. change::
        :tags: sql
        :tickets: 915

      Added "autocommit=True" keyword argument to select() and
      text(), as well as generative autocommit() method on
      select(); for statements which modify the database through
      some user-defined means other than the usual INSERT/UPDATE/
      DELETE etc.  This flag will enable "autocommit" behavior
      during execution if no transaction is in progress.

    .. change::
        :tags: sql
        :tickets: 

      The '.c.' attribute on a selectable now gets an entry for
      every column expression in its columns clause.  Previously,
      "unnamed" columns like functions and CASE statements weren't
      getting put there.  Now they will, using their full string
      representation if no 'name' is available.

    .. change::
        :tags: sql
        :tickets: 

      a CompositeSelect, i.e. any union(), union_all(),
      intersect(), etc. now asserts that each selectable contains
      the same number of columns.  This conforms to the
      corresponding SQL requirement.

    .. change::
        :tags: sql
        :tickets: 

      The anonymous 'label' generated for otherwise unlabeled
      functions and expressions now propagates outwards at compile
      time for expressions like select([select([func.foo()])]).

    .. change::
        :tags: sql
        :tickets: 

      Building on the above ideas, CompositeSelects now build up
      their ".c." collection based on the names present in the
      first selectable only; corresponding_column() now works
      fully for all embedded selectables.

    .. change::
        :tags: sql
        :tickets: 

      Oracle and others properly encode SQL used for defaults like
      sequences, etc., even if no unicode idents are used since
      identifier preparer may return a cached unicode identifier.

    .. change::
        :tags: sql
        :tickets: 

      Column and clause comparisons to datetime objects on the
      left hand side of the expression now work (d < table.c.col).
      (datetimes on the RHS have always worked, the LHS exception
      is a quirk of the datetime implementation.)

    .. change::
        :tags: orm
        :tickets: 

      Every Session.begin() must now be accompanied by a
      corresponding commit() or rollback() unless the session is
      closed with Session.close().  This also includes the begin()
      which is implicit to a session created with
      transactional=True.  The biggest change introduced here is
      that when a Session created with transactional=True raises
      an exception during flush(), you must call
      Session.rollback() or Session.close() in order for that
      Session to continue after an exception.

    .. change::
        :tags: orm
        :tickets: 961

      Fixed merge() collection-doubling bug when merging transient
      entities with backref'ed collections.

    .. change::
        :tags: orm
        :tickets: 

      merge(dont_load=True) does not accept transient entities,
      this is in continuation with the fact that
      merge(dont_load=True) does not accept any "dirty" objects
      either.

    .. change::
        :tags: orm
        :tickets: 

      Added standalone "query" class attribute generated by a
      scoped_session.  This provides MyClass.query without using
      Session.mapper.  Use via:
      
        MyClass.query = Session.query_property()

    .. change::
        :tags: orm
        :tickets: 

      The proper error message is raised when trying to access
      expired instance attributes with no session present

    .. change::
        :tags: orm
        :tickets: 

      dynamic_loader() / lazy="dynamic" now accepts and uses
      the order_by parameter in the same way in which it works
      with relation().

    .. change::
        :tags: orm
        :tickets: 

      Added expire_all() method to Session.  Calls expire() for
      all persistent instances.  This is handy in conjunction
      with...

    .. change::
        :tags: orm
        :tickets: 

      Instances which have been partially or fully expired will
      have their expired attributes populated during a regular
      Query operation which affects those objects, preventing a
      needless second SQL statement for each instance.

    .. change::
        :tags: orm
        :tickets: 938

      Dynamic relations, when referenced, create a strong
      reference to the parent object so that the query still has a
      parent to call against even if the parent is only created
      (and otherwise dereferenced) within the scope of a single
      expression.

    .. change::
        :tags: orm
        :tickets: 

      Added a mapper() flag "eager_defaults". When set to True,
      defaults that are generated during an INSERT or UPDATE
      operation are post-fetched immediately, instead of being
      deferred until later.  This mimics the old 0.3 behavior.

    .. change::
        :tags: orm
        :tickets: 

      query.join() can now accept class-mapped attributes as
      arguments. These can be used in place or in any combination
      with strings.  In particular this allows construction of
      joins to subclasses on a polymorphic relation, i.e.:
      
        query(Company).join(['employees', Engineer.name])

    .. change::
        :tags: orm, ('employees', people.join(engineer)), Engineer.name
        :tickets: 

      query.join() can also accept tuples of attribute name/some
      selectable as arguments.  This allows construction of joins
      *from* subclasses of a polymorphic relation, i.e.:
      
        query(Company).\
        join(
         
        )

    .. change::
        :tags: orm
        :tickets: 

      General improvements to the behavior of join() in
      conjunction with polymorphic mappers, i.e. joining from/to
      polymorphic mappers and properly applying aliases.

    .. change::
        :tags: orm
        :tickets: 933

      Fixed/improved behavior when a mapper determines the natural
      "primary key" of a mapped join, it will more effectively
      reduce columns which are equivalent via foreign key
      relation.  This affects how many arguments need to be sent
      to query.get(), among other things.

    .. change::
        :tags: orm
        :tickets: 946

      The lazy loader can now handle a join condition where the
      "bound" column (i.e. the one that gets the parent id sent as
      a bind parameter) appears more than once in the join
      condition.  Specifically this allows the common task of a
      relation() which contains a parent-correlated subquery, such
      as "select only the most recent child item".

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug in polymorphic inheritance where an incorrect
      exception is raised when base polymorphic_on column does not
      correspond to any columns within the local selectable of an
      inheriting mapper more than one level deep

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug in polymorphic inheritance which made it difficult
      to set a working "order_by" on a polymorphic mapper.

    .. change::
        :tags: orm
        :tickets: 

      Fixed a rather expensive call in Query that was slowing down
      polymorphic queries.

    .. change::
        :tags: orm
        :tickets: 954

      "Passive defaults" and other "inline" defaults can now be
      loaded during a flush() call if needed; in particular, this
      allows constructing relations() where a foreign key column
      references a server-side-generated, non-primary-key
      column.

    .. change::
        :tags: orm
        :tickets: 

      Additional Session transaction fixes/changes:
        - Fixed bug with session transaction management: parent
          transactions weren't started on the connection when
          adding a connection to a nested transaction.
      
        - session.transaction now always refers to the innermost
          active transaction, even when commit/rollback are called
          directly on the session transaction object.
      
        - Two-phase transactions can now be prepared.
      
        - When preparing a two-phase transaction fails on one
          connection, all the connections are rolled back.
      
        - session.close() didn't close all transactions when
          nested transactions were used.
      
        - rollback() previously erroneously set the current
          transaction directly to the parent of the transaction
          that could be rolled back to. Now it rolls back the next
          transaction up that can handle it, but sets the current
          transaction to its parent and inactivates the
          transactions in between. Inactive transactions can only
          be rolled back or closed, any other call results in an
          error.
      
        - autoflush for commit() wasn't flushing for simple
          subtransactions.
      
        - unitofwork flush didn't close the failed transaction
          when the session was not in a transaction and committing
          the transaction failed.

    .. change::
        :tags: orm
        :tickets: 964, 940

      Miscellaneous tickets:

    .. change::
        :tags: general
        :tickets: 

      Fixed a variety of hidden and some not-so-hidden
      compatibility issues for Python 2.3, thanks to new support
      for running the full test suite on 2.3.

    .. change::
        :tags: general
        :tickets: 

      Warnings are now issued as type exceptions.SAWarning.

    .. change::
        :tags: dialects
        :tickets: 

      Better support for schemas in SQLite (linked in by ATTACH
      DATABASE ... AS name).  In some cases in the past, schema
      names were omitted from generated SQL for SQLite.  This is
      no longer the case.

    .. change::
        :tags: dialects
        :tickets: 

      table_names on SQLite now picks up temporary tables as well.

    .. change::
        :tags: dialects
        :tickets: 

      Auto-detect an unspecified MySQL ANSI_QUOTES mode during
      reflection operations, support for changing the mode
      midstream.  Manual mode setting is still required if no
      reflection is used.

    .. change::
        :tags: dialects
        :tickets: 

      Fixed reflection of TIME columns on SQLite.

    .. change::
        :tags: dialects
        :tickets: 580

      Finally added PGMacAddr type to postgres

    .. change::
        :tags: dialects
        :tickets: 

      Reflect the sequence associated to a PK field (typically
      with a BEFORE INSERT trigger) under Firebird

    .. change::
        :tags: dialects
        :tickets: 941

      Oracle assembles the correct columns in the result set
      column mapping when generating a LIMIT/OFFSET subquery,
      allows columns to map properly to result sets even if
      long-name truncation kicks in

    .. change::
        :tags: dialects
        :tickets: 

      MSSQL now includes EXEC in the _is_select regexp, which
      should allow row-returning stored procedures to be used.

    .. change::
        :tags: dialects
        :tickets: 

      MSSQL now includes an experimental implementation of
      LIMIT/OFFSET using the ANSI SQL row_number() function, so it
      requires MSSQL-2005 or higher. To enable the feature, add
      "has_window_funcs" to the keyword arguments for connect, or
      add "?has_window_funcs=1" to your dburi query arguments.

    .. change::
        :tags: ext
        :tickets: 

      Changed ext.activemapper to use a non-transactional session
      for the objectstore.

    .. change::
        :tags: ext
        :tickets: 

      Fixed output order of "['a'] + obj.proxied" binary operation
      on association-proxied lists.

.. changelog::
    :version: 0.4.2p3
    :released: Wed Jan 09 2008

    .. change::
        :tags: general
        :tickets: 

      sub version numbering scheme changed to suite
      setuptools version number rules; easy_install -u
      should now get this version over 0.4.2.

    .. change::
        :tags: sql
        :tickets: 912

      Text type is properly exported now and does not
      raise a warning on DDL create; String types with no
      length only raise warnings during CREATE TABLE

    .. change::
        :tags: sql
        :tickets: 

      new UnicodeText type is added, to specify an
      encoded, unlengthed Text type

    .. change::
        :tags: sql
        :tickets: 

      fixed bug in union() so that select() statements
      which don't derive from FromClause objects can be
      unioned

    .. change::
        :tags: orm
        :tickets: 

      fixed bug with session.dirty when using "mutable
      scalars" (such as PickleTypes)

    .. change::
        :tags: orm
        :tickets: 

      added a more descriptive error message when flushing
      on a relation() that has non-locally-mapped columns
      in its primary or secondary join condition

    .. change::
        :tags: dialects
        :tickets: 

      Fixed reflection of mysql empty string column
      defaults.

    .. change::
        :tags: sql
        :tickets: 912

      changed name of TEXT to Text since its a "generic"
      type; TEXT name is deprecated until 0.5. The
      "upgrading" behavior of String to Text when no
      length is present is also deprecated until 0.5; will
      issue a warning when used for CREATE TABLE
      statements (String with no length for SQL expression
      purposes is still fine)

    .. change::
        :tags: sql
        :tickets: 924

      generative select.order_by(None) / group_by(None)
      was not managing to reset order by/group by
      criterion, fixed

    .. change::
        :tags: orm
        :tickets: 

      suppressing *all* errors in
      InstanceState.__cleanup() now.

    .. change::
        :tags: orm
        :tickets: 922

      fixed an attribute history bug whereby assigning a
      new collection to a collection-based attribute which
      already had pending changes would generate incorrect
      history

    .. change::
        :tags: orm
        :tickets: 925

      fixed delete-orphan cascade bug whereby setting the
      same object twice to a scalar attribute could log it
      as an orphan

    .. change::
        :tags: orm
        :tickets: 

      Fixed cascades on a += assignment to a list-based
      relation.

    .. change::
        :tags: orm
        :tickets: 919

      synonyms can now be created against props that don't
      exist yet, which are later added via add_property().
      This commonly includes backrefs. (i.e. you can make
      synonyms for backrefs without worrying about the
      order of operations)

    .. change::
        :tags: orm
        :tickets: 

      fixed bug which could occur with polymorphic "union"
      mapper which falls back to "deferred" loading of
      inheriting tables

    .. change::
        :tags: orm
        :tickets: 

      the "columns" collection on a mapper/mapped class
      (i.e. 'c') is against the mapped table, not the
      select_table in the case of polymorphic "union"
      loading (this shouldn't be noticeable).

    .. change::
        :tags: ext
        :tickets: 

      '+', '*', '+=' and '\*=' support for association
      proxied lists.

    .. change::
        :tags: dialects
        :tickets: 923

      mssql - narrowed down the test for "date"/"datetime"
      in MSDate/ MSDateTime subclasses so that incoming
      "datetime" objects don't get mis-interpreted as
      "date" objects and vice versa.

    .. change::
        :tags: orm
        :tickets: 

      fixed fairly critical bug whereby the same instance could be listed
      more than once in the unitofwork.new collection; most typically
      reproduced when using a combination of inheriting mappers and
      ScopedSession.mapper, as the multiple __init__ calls per instance
      could save() the object with distinct _state objects

    .. change::
        :tags: orm
        :tickets: 

      added very rudimentary yielding iterator behavior to Query.  Call
      query.yield_per(<number of rows>) and evaluate the Query in an
      iterative context; every collection of N rows will be packaged up
      and yielded.  Use this method with extreme caution since it does
      not attempt to reconcile eagerly loaded collections across
      result batch boundaries, nor will it behave nicely if the same
      instance occurs in more than one batch.  This means that an eagerly
      loaded collection will get cleared out if it's referenced in more than
      one batch, and in all cases attributes will be overwritten on instances
      that occur in more than one batch.

    .. change::
        :tags: orm
        :tickets: 920

      Fixed in-place set mutation operators for set collections and association
      proxied sets.

    .. change::
        :tags: dialects
        :tickets: 913

      Fixed the missing call to subtype result processor for the PGArray
      type.

.. changelog::
    :version: 0.4.2
    :released: Wed Jan 02 2008

    .. change::
        :tags: sql
        :tickets: 615

      generic functions ! we introduce a database of known SQL functions, such
      as current_timestamp, coalesce, and create explicit function objects
      representing them. These objects have constrained argument lists, are
      type aware, and can compile in a dialect-specific fashion. So saying
      func.char_length("foo", "bar") raises an error (too many args),
      func.coalesce(datetime.date(2007, 10, 5), datetime.date(2005, 10, 15))
      knows that its return type is a Date. We only have a few functions
      represented so far but will continue to add to the system

    .. change::
        :tags: sql
        :tickets: 

      auto-reconnect support improved; a Connection can now automatically
      reconnect after its underlying connection is invalidated, without
      needing to connect() again from the engine.  This allows an ORM session
      bound to a single Connection to not need a reconnect.
      Open transactions on the Connection must be rolled back after an invalidation
      of the underlying connection else an error is raised.  Also fixed
      bug where disconnect detect was not being called for cursor(), rollback(),
      or commit().

    .. change::
        :tags: sql
        :tickets: 

      added new flag to String and create_engine(),
      assert_unicode=(True|False|'warn'\|None). Defaults to `False` or `None` on
      create_engine() and String, `'warn'` on the Unicode type. When `True`,
      results in all unicode conversion operations raising an exception when a
      non-unicode bytestring is passed as a bind parameter. 'warn' results
      in a warning. It is strongly advised that all unicode-aware applications
      make proper use of Python unicode objects (i.e. u'hello' and not 'hello')
      so that data round trips accurately.

    .. change::
        :tags: sql
        :tickets: 

      generation of "unique" bind parameters has been simplified to use the same
      "unique identifier" mechanisms as everything else.  This doesn't affect
      user code, except any code that might have been hardcoded against the generated
      names.  Generated bind params now have the form "<paramname>_<num>",
      whereas before only the second bind of the same name would have this form.

    .. change::
        :tags: sql
        :tickets: 

      select().as_scalar() will raise an exception if the select does not have
      exactly one expression in its columns clause.

    .. change::
        :tags: sql
        :tickets: 

      bindparam() objects themselves can be used as keys for execute(), i.e.
      statement.execute({bind1:'foo', bind2:'bar'})

    .. change::
        :tags: sql
        :tickets: 

      added new methods to TypeDecorator, process_bind_param() and
      process_result_value(), which automatically take advantage of the processing
      of the underlying type.  Ideal for using with Unicode or Pickletype.
      TypeDecorator should now be the primary way to augment the behavior of any
      existing type including other TypeDecorator subclasses such as PickleType.

    .. change::
        :tags: sql
        :tickets: 

      selectables (and others) will issue a warning when two columns in
      their exported columns collection conflict based on name.

    .. change::
        :tags: sql
        :tickets: 890

      tables with schemas can still be used in sqlite, firebird,
      schema name just gets dropped

    .. change::
        :tags: sql
        :tickets: 

      changed the various "literal" generation functions to use an anonymous
      bind parameter.  not much changes here except their labels now look
      like ":param_1", ":param_2" instead of ":literal"

    .. change::
        :tags: sql
        :tickets: 

      column labels in the form "tablename.columname", i.e. with a dot, are now
      supported.

    .. change::
        :tags: sql
        :tickets: 

      from_obj keyword argument to select() can be a scalar or a list.

    .. change::
        :tags: orm
        :tickets: 871

      a major behavioral change to collection-based backrefs: they no
      longer trigger lazy loads !  "reverse" adds and removes
      are queued up and are merged with the collection when it is
      actually read from and loaded; but do not trigger a load beforehand.
      For users who have noticed this behavior, this should be much more
      convenient than using dynamic relations in some cases; for those who
      have not, you might notice your apps using a lot fewer queries than
      before in some situations.

    .. change::
        :tags: orm
        :tickets: 

      mutable primary key support is added. primary key columns can be
      changed freely, and the identity of the instance will change upon
      flush. In addition, update cascades of foreign key referents (primary
      key or not) along relations are supported, either in tandem with the
      database's ON UPDATE CASCADE (required for DB's like Postgres) or
      issued directly by the ORM in the form of UPDATE statements, by setting
      the flag "passive_cascades=False".

    .. change::
        :tags: orm
        :tickets: 490

      inheriting mappers now inherit the MapperExtensions of their parent
      mapper directly, so that all methods for a particular MapperExtension
      are called for subclasses as well.  As always, any MapperExtension
      can return either EXT_CONTINUE to continue extension processing
      or EXT_STOP to stop processing.  The order of mapper resolution is:
      <extensions declared on the classes mapper> <extensions declared on the
      classes' parent mapper> <globally declared extensions>.
      
      Note that if you instantiate the same extension class separately
      and then apply it individually for two mappers in the same inheritance
      chain, the extension will be applied twice to the inheriting class,
      and each method will be called twice.
      
      To apply a mapper extension explicitly to each inheriting class but
      have each method called only once per operation, use the same
      instance of the extension for both mappers.

    .. change::
        :tags: orm
        :tickets: 907

      MapperExtension.before_update() and after_update() are now called
      symmetrically; previously, an instance that had no modified column
      attributes (but had a relation() modification) could be called with
      before_update() but not after_update()

    .. change::
        :tags: orm
        :tickets: 

      columns which are missing from a Query's select statement
      now get automatically deferred during load.

    .. change::
        :tags: orm
        :tickets: 908

      mapped classes which extend "object" and do not provide an
      __init__() method will now raise TypeError if non-empty \*args
      or \**kwargs are present at instance construction time (and are
      not consumed by any extensions such as the scoped_session mapper),
      consistent with the behavior of normal Python classes

    .. change::
        :tags: orm
        :tickets: 899

      fixed Query bug when filter_by() compares a relation against None

    .. change::
        :tags: orm
        :tickets: 

      improved support for pickling of mapped entities.  Per-instance
      lazy/deferred/expired callables are now serializable so that
      they serialize and deserialize with _state.

    .. change::
        :tags: orm
        :tickets: 801

      new synonym() behavior: an attribute will be placed on the mapped
      class, if one does not exist already, in all cases. if a property
      already exists on the class, the synonym will decorate the property
      with the appropriate comparison operators so that it can be used in
      column expressions just like any other mapped attribute (i.e. usable in
      filter(), etc.) the "proxy=True" flag is deprecated and no longer means
      anything. Additionally, the flag "map_column=True" will automatically
      generate a ColumnProperty corresponding to the name of the synonym,
      i.e.: 'somename':synonym('_somename', map_column=True) will map the
      column named 'somename' to the attribute '_somename'. See the example
      in the mapper docs.

    .. change::
        :tags: orm
        :tickets: 

      Query.select_from() now replaces all existing FROM criterion with
      the given argument; the previous behavior of constructing a list
      of FROM clauses was generally not useful as is required
      filter() calls to create join criterion, and new tables introduced
      within filter() already add themselves to the FROM clause.  The
      new behavior allows not just joins from the main table, but select
      statements as well.  Filter criterion, order bys, eager load
      clauses will be "aliased" against the given statement.

    .. change::
        :tags: orm
        :tickets: 

      this month's refactoring of attribute instrumentation changes
      the "copy-on-load" behavior we've had since midway through 0.3
      with "copy-on-modify" in most cases.  This takes a sizable chunk
      of latency out of load operations and overall does less work
      as only attributes which are actually modified get their
      "committed state" copied.  Only "mutable scalar" attributes
      (i.e. a pickled object or other mutable item), the reason for
      the copy-on-load change in the first place, retain the old
      behavior.

    .. change::
        :tags: attrname, orm
        :tickets: 

      a slight behavioral change to attributes is, del'ing an attribute
      does *not* cause the lazyloader of that attribute to fire off again;
      the "del" makes the effective value of the attribute "None".  To
      re-trigger the "loader" for an attribute, use
      session.expire(instance,).

    .. change::
        :tags: orm
        :tickets: 

      query.filter(SomeClass.somechild == None), when comparing
      a many-to-one property to None, properly generates "id IS NULL"
      including that the NULL is on the right side.

    .. change::
        :tags: orm
        :tickets: 

      query.order_by() takes into account aliased joins, i.e.
      query.join('orders', aliased=True).order_by(Order.id)

    .. change::
        :tags: orm
        :tickets: 

      eagerload(), lazyload(), eagerload_all() take an optional
      second class-or-mapper argument, which will select the mapper
      to apply the option towards.  This can select among other
      mappers which were added using add_entity().

    .. change::
        :tags: orm
        :tickets: 

      eagerloading will work with mappers added via add_entity().

    .. change::
        :tags: orm
        :tickets: 

      added "cascade delete" behavior to "dynamic" relations just like
      that of regular relations.  if passive_deletes flag (also just added)
      is not set, a delete of the parent item will trigger a full load of
      the child items so that they can be deleted or updated accordingly.

    .. change::
        :tags: orm
        :tickets: 

      also with dynamic, implemented correct count() behavior as well
      as other helper methods.

    .. change::
        :tags: orm
        :tickets: 

      fix to cascades on polymorphic relations, such that cascades
      from an object to a polymorphic collection continue cascading
      along the set of attributes specific to each element in the collection.

    .. change::
        :tags: orm
        :tickets: 893

      query.get() and query.load() do not take existing filter or other
      criterion into account; these methods *always* look up the given id
      in the database or return the current instance from the identity map,
      disregarding any existing filter, join, group_by or other criterion
      which has been configured.

    .. change::
        :tags: orm
        :tickets: 883

      added support for version_id_col in conjunction with inheriting mappers.
      version_id_col is typically set on the base mapper in an inheritance
      relationship where it takes effect for all inheriting mappers.

    .. change::
        :tags: orm
        :tickets: 

      relaxed rules on column_property() expressions having labels; any
      ColumnElement is accepted now, as the compiler auto-labels non-labeled
      ColumnElements now.  a selectable, like a select() statement, still
      requires conversion to ColumnElement via as_scalar() or label().

    .. change::
        :tags: orm
        :tickets: 

      fixed backref bug where you could not del instance.attr if attr
      was None

    .. change::
        :tags: orm
        :tickets: 

      several ORM attributes have been removed or made private:
      mapper.get_attr_by_column(), mapper.set_attr_by_column(),
      mapper.pks_by_table, mapper.cascade_callable(),
      MapperProperty.cascade_callable(), mapper.canload(),
      mapper.save_obj(), mapper.delete_obj(), mapper._mapper_registry,
      attributes.AttributeManager

    .. change::
        :tags: orm
        :tickets: 

      Assigning an incompatible collection type to a relation attribute now
      raises TypeError instead of sqlalchemy's ArgumentError.

    .. change::
        :tags: orm
        :tickets: 886

      Bulk assignment of a MappedCollection now raises an error if a key in the
      incoming dictionary does not match the key that the collection's keyfunc
      would use for that value.

    .. change::
        :tags: orm, newval1, newval2
        :tickets: 

      Custom collections can now specify a @converter method to translate
      objects used in "bulk" assignment into a stream of values, as in::
      
         obj.col =
         # or
         obj.dictcol = {'foo': newval1, 'bar': newval2}
      
      The MappedCollection uses this hook to ensure that incoming key/value
      pairs are sane from the collection's perspective.

    .. change::
        :tags: orm
        :tickets: 872

      fixed endless loop issue when using lazy="dynamic" on both
      sides of a bi-directional relationship

    .. change::
        :tags: orm
        :tickets: 904

      more fixes to the LIMIT/OFFSET aliasing applied with Query + eagerloads,
      in this case when mapped against a select statement

    .. change::
        :tags: orm
        :tickets: 

      fix to self-referential eager loading such that if the same mapped
      instance appears in two or more distinct sets of columns in the same
      result set, its eagerly loaded collection will be populated regardless
      of whether or not all of the rows contain a set of "eager" columns for
      that collection.  this would also show up as a KeyError when fetching
      results with join_depth turned on.

    .. change::
        :tags: orm
        :tickets: 

      fixed bug where Query would not apply a subquery to the SQL when LIMIT
      was used in conjunction with an inheriting mapper where the eager
      loader was only in the parent mapper.

    .. change::
        :tags: orm
        :tickets: 

      clarified the error message which occurs when you try to update()
      an instance with the same identity key as an instance already present
      in the session.

    .. change::
        :tags: orm
        :tickets: 

      some clarifications and fixes to merge(instance, dont_load=True).
      fixed bug where lazy loaders were getting disabled on returned instances.
      Also, we currently do not support merging an instance which has uncommitted
      changes on it, in the case that dont_load=True is used....this will
      now raise an error.  This is due to complexities in merging the
      "committed state" of the given instance to correctly correspond to the
      newly copied instance, as well as other modified state.
      Since the use case for dont_load=True is caching, the given instances
      shouldn't have any uncommitted changes on them anyway.
      We also copy the instances over without using any events now, so that
      the 'dirty' list on the new session remains unaffected.

    .. change::
        :tags: orm
        :tickets: 

      fixed bug which could arise when using session.begin_nested() in conjunction
      with more than one level deep of enclosing session.begin() statements

    .. change::
        :tags: orm
        :tickets: 914

      fixed session.refresh() with instance that has custom entity_name

    .. change::
        :tags: dialects
        :tickets: 

      sqlite SLDate type will not erroneously render "microseconds" portion
      of a datetime or time object.

    .. change::
        :tags: dialects
        :tickets: 902

      oracle
       - added disconnect detection support for Oracle
       - some cleanup to binary/raw types so that cx_oracle.LOB is detected
         on an ad-hoc basis

    .. change::
        :tags: dialects
        :tickets: 824, 839, 842, 901

      MSSQL
       - PyODBC no longer has a global "set nocount on".
       - Fix non-identity integer PKs on autload
       - Better support for convert_unicode
       - Less strict date conversion for pyodbc/adodbapi
       - Schema-qualified tables / autoload

    .. change::
        :tags: firebird, backend
        :tickets: 410

      does properly reflect domains (partially fixing) and
      PassiveDefaults

    .. change::
        :tags: 3562, firebird, backend
        :tickets: 

      reverted to use default poolclass (was set to SingletonThreadPool in
      0.4.0 for test purposes)

    .. change::
        :tags: firebird, backend
        :tickets: 

      map func.length() to 'char_length' (easily overridable with the UDF
      'strlen' on old versions of Firebird)

.. changelog::
    :version: 0.4.1
    :released: Sun Nov 18 2007

    .. change::
        :tags: sql
        :tickets: 

      the "shortname" keyword parameter on bindparam() has been
      deprecated.

    .. change::
        :tags: sql
        :tickets: 

      Added contains operator (generates a "LIKE %<other>%" clause).

    .. change::
        :tags: sql
        :tickets: 

      anonymous column expressions are automatically labeled.
      e.g. select([x* 5]) produces "SELECT x * 5 AS anon_1".
      This allows the labelname to be present in the cursor.description
      which can then be appropriately matched to result-column processing
      rules. (we can't reliably use positional tracking for result-column
      matches since text() expressions may represent multiple columns).

    .. change::
        :tags: sql
        :tickets: 

      operator overloading is now controlled by TypeEngine objects - the
      one built-in operator overload so far is String types overloading
      '+' to be the string concatenation operator.
      User-defined types can also define their own operator overloading
      by overriding the adapt_operator(self, op) method.

    .. change::
        :tags: sql
        :tickets: 819

      untyped bind parameters on the right side of a binary expression
      will be assigned the type of the left side of the operation, to better
      enable the appropriate bind parameter processing to take effect

    .. change::
        :tags: sql
        :tickets: 833

      Removed regular expression step from most statement compilations.
      Also fixes

    .. change::
        :tags: sql
        :tickets: 

      Fixed empty (zero column) sqlite inserts, allowing inserts on
      autoincrementing single column tables.

    .. change::
        :tags: sql
        :tickets: 

      Fixed expression translation of text() clauses; this repairs various
      ORM scenarios where literal text is used for SQL expressions

    .. change::
        :tags: sql
        :tickets: 

      Removed ClauseParameters object; compiled.params returns a regular
      dictionary now, as well as result.last_inserted_params() /
      last_updated_params().

    .. change::
        :tags: sql
        :tickets: 

      Fixed INSERT statements w.r.t. primary key columns that have
      SQL-expression based default generators on them; SQL expression
      executes inline as normal but will not trigger a "postfetch" condition
      for the column, for those DB's who provide it via cursor.lastrowid

    .. change::
        :tags: sql
        :tickets: 844

      func. objects can be pickled/unpickled

    .. change::
        :tags: sql
        :tickets: 

      rewrote and simplified the system used to "target" columns across
      selectable expressions.  On the SQL side this is represented by the
      "corresponding_column()" method. This method is used heavily by the ORM
      to "adapt" elements of an expression to similar, aliased expressions,
      as well as to target result set columns originally bound to a
      table or selectable to an aliased, "corresponding" expression.  The new
      rewrite features completely consistent and accurate behavior.

    .. change::
        :tags: sql
        :tickets: 573

      Added a field ("info") for storing arbitrary data on schema items

    .. change::
        :tags: sql
        :tickets: 

      The "properties" collection on Connections has been renamed "info" to
      match schema's writable collections.  Access is still available via
      the "properties" name until 0.5.

    .. change::
        :tags: sql
        :tickets: 

      fixed the close() method on Transaction when using strategy='threadlocal'

    .. change::
        :tags: sql
        :tickets: 853

      fix to compiled bind parameters to not mistakenly populate None

    .. change::
        :tags: sql
        :tickets: 

      <Engine|Connection>._execute_clauseelement becomes a public method
      Connectable.execute_clauseelement

    .. change::
        :tags: orm
        :tickets: 843

      eager loading with LIMIT/OFFSET applied no longer adds the primary
      table joined to a limited subquery of itself; the eager loads now
      join directly to the subquery which also provides the primary table's
      columns to the result set.  This eliminates a JOIN from all eager loads
      with LIMIT/OFFSET.

    .. change::
        :tags: orm
        :tickets: 802

      session.refresh() and session.expire() now support an additional argument
      "attribute_names", a list of individual attribute keynames to be refreshed
      or expired, allowing partial reloads of attributes on an already-loaded
      instance.

    .. change::
        :tags: orm
        :tickets: 767

      added op() operator to instrumented attributes; i.e.
      User.name.op('ilike')('%somename%')

    .. change::
        :tags: orm
        :tickets: 676

      Mapped classes may now define __eq__, __hash__, and __nonzero__ methods
      with arbitrary semantics.  The orm now handles all mapped instances on
      an identity-only basis. (e.g. 'is' vs '==')

    .. change::
        :tags: orm
        :tickets: 

      the "properties" accessor on Mapper is removed; it now throws an informative
      exception explaining the usage of mapper.get_property() and
      mapper.iterate_properties

    .. change::
        :tags: orm
        :tickets: 

      added having() method to Query, applies HAVING to the generated statement
      in the same way as filter() appends to the WHERE clause.

    .. change::
        :tags: orm
        :tickets: 777

      The behavior of query.options() is now fully based on paths, i.e. an
      option such as eagerload_all('x.y.z.y.x') will apply eagerloading to
      only those paths, i.e. and not 'x.y.x'; eagerload('children.children')
      applies only to exactly two-levels deep, etc.

    .. change::
        :tags: orm
        :tickets: 

      PickleType will compare using `==` when set up with mutable=False,
      and not the `is` operator.  To use `is` or any other comparator, send
      in a custom comparison function using PickleType(comparator=my_custom_comparator).

    .. change::
        :tags: orm
        :tickets: 848

      query doesn't throw an error if you use distinct() and an order_by()
      containing UnaryExpressions (or other) together

    .. change::
        :tags: orm
        :tickets: 786

      order_by() expressions from joined tables are properly added to columns
      clause when using distinct()

    .. change::
        :tags: orm
        :tickets: 858

      fixed error where Query.add_column() would not accept a class-bound
      attribute as an argument; Query also raises an error if an invalid
      argument was sent to add_column() (at instances() time)

    .. change::
        :tags: orm
        :tickets: 

      added a little more checking for garbage-collection dereferences in
      InstanceState.__cleanup() to reduce "gc ignored" errors on app
      shutdown

    .. change::
        :tags: orm
        :tickets: 

      The session API has been solidified:

    .. change::
        :tags: orm
        :tickets: 840

      It's an error to session.save() an object which is already
      persistent

    .. change::
        :tags: orm
        :tickets: 

      It's an error to session.delete() an object which is *not*
      persistent.

    .. change::
        :tags: orm
        :tickets: 

      session.update() and session.delete() raise an error when updating
      or deleting an instance that is already in the session with a
      different identity.

    .. change::
        :tags: orm
        :tickets: 

      The session checks more carefully when determining "object X already
      in another session"; e.g. if you pickle a series of objects and
      unpickle (i.e. as in a Pylons HTTP session or similar), they can go
      into a new session without any conflict

    .. change::
        :tags: orm
        :tickets: 

      merge() includes a keyword argument "dont_load=True".  setting this
      flag will cause the merge operation to not load any data from the
      database in response to incoming detached objects, and will accept
      the incoming detached object as though it were already present in
      that session.  Use this to merge detached objects from external
      caching systems into the session.

    .. change::
        :tags: orm
        :tickets: 

      Deferred column attributes no longer trigger a load operation when the
      attribute is assigned to.  In those cases, the newly assigned value
      will be present in the flushes' UPDATE statement unconditionally.

    .. change::
        :tags: orm
        :tickets: 834

      Fixed a truncation error when re-assigning a subset of a collection
      (obj.relation = obj.relation[1:])

    .. change::
        :tags: orm
        :tickets: 832

      De-cruftified backref configuration code, backrefs which step on
      existing properties now raise an error

    .. change::
        :tags: orm
        :tickets: 831

      Improved behavior of add_property() etc., fixed involving
      synonym/deferred.

    .. change::
        :tags: orm
        :tickets: 

      Fixed clear_mappers() behavior to better clean up after itself.

    .. change::
        :tags: orm
        :tickets: 841

      Fix to "row switch" behavior, i.e. when an INSERT/DELETE is combined
      into a single UPDATE; many-to-many relations on the parent object
      update properly.

    .. change::
        :tags: orm
        :tickets: 

      Fixed __hash__ for association proxy- these collections are unhashable,
      just like their mutable Python counterparts.

    .. change::
        :tags: orm
        :tickets: 

      Added proxying of save_or_update, __contains__ and __iter__ methods for
      scoped sessions.

    .. change::
        :tags: orm
        :tickets: 852

      fixed very hard-to-reproduce issue where by the FROM clause of Query
      could get polluted by certain generative calls

    .. change::
        :tags: dialects
        :tickets: 

      Added experimental support for MaxDB (versions >= 7.6.03.007 only).

    .. change::
        :tags: dialects
        :tickets: 

      oracle will now reflect "DATE" as an OracleDateTime column, not
      OracleDate

    .. change::
        :tags: dialects
        :tickets: 847

      added awareness of schema name in oracle table_names() function,
      fixes metadata.reflect(schema='someschema')

    .. change::
        :tags: dialects
        :tickets: 

      MSSQL anonymous labels for selection of functions made deterministic

    .. change::
        :tags: dialects
        :tickets: 

      sqlite will reflect "DECIMAL" as a numeric column.

    .. change::
        :tags: dialects
        :tickets: 828

      Made access dao detection more reliable

    .. change::
        :tags: dialects
        :tickets: 

      Renamed the Dialect attribute 'preexecute_sequences' to
      'preexecute_pk_sequences'.  An attribute porxy is in place for
      out-of-tree dialects using the old name.

    .. change::
        :tags: dialects
        :tickets: 

      Added test coverage for unknown type reflection. Fixed sqlite/mysql
      handling of type reflection for unknown types.

    .. change::
        :tags: dialects
        :tickets: 

      Added REAL for mysql dialect (for folks exploiting the
      REAL_AS_FLOAT sql mode).

    .. change::
        :tags: dialects
        :tickets: 

      mysql Float, MSFloat and MSDouble constructed without arguments
      now produce no-argument DDL, e.g.'FLOAT'.

    .. change::
        :tags: misc
        :tickets: 

      Removed unused util.hash().

.. changelog::
    :version: 0.4.0
    :released: Wed Oct 17 2007

    .. change::
        :tags: 
        :tickets: 

      (see 0.4.0beta1 for the start of major changes against 0.3,
      as well as http://www.sqlalchemy.org/trac/wiki/WhatsNewIn04 )

    .. change::
        :tags: 
        :tickets: 785

      Added initial Sybase support (mxODBC so far)

    .. change::
        :tags: 
        :tickets: 

      Added partial index support for PostgreSQL. Use the postgres_where keyword
      on the Index.

    .. change::
        :tags: 
        :tickets: 817

      string-based query param parsing/config file parser understands
      wider range of string values for booleans

    .. change::
        :tags: 
        :tickets: 813

      backref remove object operation doesn't fail if the other-side
      collection doesn't contain the item, supports noload collections

    .. change::
        :tags: 
        :tickets: 818

      removed __len__ from "dynamic" collection as it would require issuing
      a SQL "count()" operation, thus forcing all list evaluations to issue
      redundant SQL

    .. change::
        :tags: 
        :tickets: 816

      inline optimizations added to locate_dirty() which can greatly speed up
      repeated calls to flush(), as occurs with autoflush=True

    .. change::
        :tags: 
        :tickets: 

      The IdentifierPreprarer's _requires_quotes test is now regex based.  Any
      out-of-tree dialects that provide custom sets of legal_characters or
      illegal_initial_characters will need to move to regexes or override
      _requires_quotes.

    .. change::
        :tags: 
        :tickets: 

      Firebird has supports_sane_rowcount and supports_sane_multi_rowcount set
      to False due to ticket #370 (right way).

    .. change::
        :tags: 
        :tickets: 

      Improvements and fixes on Firebird reflection:
        * FBDialect now mimics OracleDialect, regarding case-sensitivity of TABLE and
          COLUMN names (see 'case_sensitive remotion' topic on this current file).
        * FBDialect.table_names() doesn't bring system tables (ticket:796).
        * FB now reflects Column's nullable property correctly.

    .. change::
        :tags: 
        :tickets: 

      Fixed SQL compiler's awareness of top-level column labels as used
      in result-set processing; nested selects which contain the same column
      names don't affect the result or conflict with result-column metadata.

    .. change::
        :tags: 
        :tickets: 

      query.get() and related functions (like many-to-one lazyloading)
      use compile-time-aliased bind parameter names, to prevent
      name conflicts with bind parameters that already exist in the
      mapped selectable.

    .. change::
        :tags: 
        :tickets: 795

      Fixed three- and multi-level select and deferred inheritance loading
      (i.e. abc inheritance with no select_table).

    .. change::
        :tags: 
        :tickets: 

      Ident passed to id_chooser in shard.py always a list.

    .. change::
        :tags: 
        :tickets: 

      The no-arg ResultProxy._row_processor() is now the class attribute
      `_process_row`.

    .. change::
        :tags: 
        :tickets: 797

      Added support for returning values from inserts and updates for
      PostgreSQL 8.2+.

    .. change::
        :tags: 
        :tickets: 

      PG reflection, upon seeing the default schema name being used explicitly
      as the "schema" argument in a Table, will assume that this is the
      user's desired convention, and will explicitly set the "schema" argument
      in foreign-key-related reflected tables, thus making them match only
      with Table constructors that also use the explicit "schema" argument
      (even though its the default schema).
      In other words, SA assumes the user is being consistent in this usage.

    .. change::
        :tags: 
        :tickets: 808

      fixed sqlite reflection of BOOL/BOOLEAN

    .. change::
        :tags: 
        :tickets: 

      Added support for UPDATE with LIMIT on mysql.

    .. change::
        :tags: 
        :tickets: 803

      null foreign key on a m2o doesn't trigger a lazyload

    .. change::
        :tags: 
        :tickets: 800

      oracle does not implicitly convert to unicode for non-typed result
      sets (i.e. when no TypeEngine/String/Unicode type is even being used;
      previously it was detecting DBAPI types and converting regardless).
      should fix

    .. change::
        :tags: 
        :tickets: 806

      fix to anonymous label generation of long table/column names

    .. change::
        :tags: 
        :tickets: 

      Firebird dialect now uses SingletonThreadPool as poolclass.

    .. change::
        :tags: 
        :tickets: 

      Firebird now uses dialect.preparer to format sequences names

    .. change::
        :tags: 
        :tickets: 810

      Fixed breakage with postgres and multiple two-phase transactions. Two-phase
      commits and rollbacks didn't automatically end up with a new transaction
      as the usual dbapi commits/rollbacks do.

    .. change::
        :tags: 
        :tickets: 

      Added an option to the _ScopedExt mapper extension to not automatically
      save new objects to session on object initialization.

    .. change::
        :tags: 
        :tickets: 

      fixed Oracle non-ansi join syntax

    .. change::
        :tags: 
        :tickets: 

      PickleType and Interval types (on db not supporting it natively) are now
      slightly faster.

    .. change::
        :tags: 
        :tickets: 

      Added Float and Time types to Firebird (FBFloat and FBTime). Fixed
      BLOB SUB_TYPE for TEXT and Binary types.

    .. change::
        :tags: 
        :tickets: 

      Changed the API for the in\_ operator. in_() now accepts a single argument
      that is a sequence of values or a selectable. The old API of passing in
      values as varargs still works but is deprecated.

.. changelog::
    :version: 0.4.0beta6
    :released: Thu Sep 27 2007

    .. change::
        :tags: 
        :tickets: 

      The Session identity map is now *weak referencing* by default, use
      weak_identity_map=False to use a regular dict.  The weak dict we are using
      is customized to detect instances which are "dirty" and maintain a
      temporary strong reference to those instances until changes are flushed.

    .. change::
        :tags: 
        :tickets: 758

      Mapper compilation has been reorganized such that most compilation occurs
      upon mapper construction.  This allows us to have fewer calls to
      mapper.compile() and also to allow class-based properties to force a
      compilation (i.e. User.addresses == 7 will compile all mappers; this is).  The only caveat here is that an inheriting mapper now
      looks for its inherited mapper upon construction; so mappers within
      inheritance relationships need to be constructed in inheritance order
      (which should be the normal case anyway).

    .. change::
        :tags: 
        :tickets: 

      added "FETCH" to the keywords detected by Postgres to indicate a
      result-row holding statement (i.e. in addition to "SELECT").

    .. change::
        :tags: 
        :tickets: 

      Added full list of SQLite reserved keywords so that they get escaped
      properly.

    .. change::
        :tags: 
        :tickets: 

      Tightened up the relationship between the Query's generation of "eager
      load" aliases, and Query.instances() which actually grabs the eagerly
      loaded rows.  If the aliases were not specifically generated for that
      statement by EagerLoader, the EagerLoader will not take effect when the
      rows are fetched.  This prevents columns from being grabbed accidentally
      as being part of an eager load when they were not meant for such, which
      can happen with textual SQL as well as some inheritance situations.  It's
      particularly important since the "anonymous aliasing" of columns uses
      simple integer counts now to generate labels.

    .. change::
        :tags: 
        :tickets: 

      Removed "parameters" argument from clauseelement.compile(), replaced with
      "column_keys".  The parameters sent to execute() only interact with the
      insert/update statement compilation process in terms of the column names
      present but not the values for those columns.  Produces more consistent
      execute/executemany behavior, simplifies things a bit internally.

    .. change::
        :tags: 
        :tickets: 560

      Added 'comparator' keyword argument to PickleType.  By default, "mutable"
      PickleType does a "deep compare" of objects using their dumps()
      representation.  But this doesn't work for dictionaries.  Pickled objects
      which provide an adequate __eq__() implementation can be set up with
      "PickleType(comparator=operator.eq)"

    .. change::
        :tags: 
        :tickets: 

      Added session.is_modified(obj) method; performs the same "history"
      comparison operation as occurs within a flush operation; setting
      include_collections=False gives the same result as is used when the flush
      determines whether or not to issue an UPDATE for the instance's row.

    .. change::
        :tags: 
        :tickets: 584, 761

      Added "schema" argument to Sequence; use this with Postgres /Oracle when
      the sequence is located in an alternate schema.  Implements part of, should fix.

    .. change::
        :tags: 
        :tickets: 

      Fixed reflection of the empty string for mysql enums.

    .. change::
        :tags: 
        :tickets: 794

      Changed MySQL dialect to use the older LIMIT <offset>, <limit> syntax
      instead of LIMIT <l> OFFSET <o> for folks using 3.23.

    .. change::
        :tags: 
        :tickets: 

      Added 'passive_deletes="all"' flag to relation(), disables all nulling-out
      of foreign key attributes during a flush where the parent object is
      deleted.

    .. change::
        :tags: 
        :tickets: 

      Column defaults and onupdates, executing inline, will add parenthesis for
      subqueries and other parenthesis-requiring expressions

    .. change::
        :tags: 
        :tickets: 793

      The behavior of String/Unicode types regarding that they auto-convert to
      TEXT/CLOB when no length is present now occurs *only* for an exact type of
      String or Unicode with no arguments.  If you use VARCHAR or NCHAR
      (subclasses of String/Unicode) with no length, they will be interpreted by
      the dialect as VARCHAR/NCHAR; no "magic" conversion happens there.  This
      is less surprising behavior and in particular this helps Oracle keep
      string-based bind parameters as VARCHARs and not CLOBs.

    .. change::
        :tags: 
        :tickets: 771

      Fixes to ShardedSession to work with deferred columns.

    .. change::
        :tags: 
        :tickets: 

      User-defined shard_chooser() function must accept "clause=None" argument;
      this is the ClauseElement passed to session.execute(statement) and can be
      used to determine correct shard id (since execute() doesn't take an
      instance.)

    .. change::
        :tags: 
        :tickets: 764

      Adjusted operator precedence of NOT to match '==' and others, so that
      ~(x <operator> y) produces NOT (x <op> y), which is better compatible
      with older MySQL versions..  This doesn't apply to "~(x==y)"
      as it does in 0.3 since ~(x==y) compiles to "x != y", but still applies
      to operators like BETWEEN.

    .. change::
        :tags: 
        :tickets: 757, 768, 779, 728

      Other tickets:,,.

.. changelog::
    :version: 0.4.0beta5
    :released: 

    .. change::
        :tags: 
        :tickets: 754

      Connection pool fixes; the better performance of beta4 remains but fixes
      "connection overflow" and other bugs which were present (like).

    .. change::
        :tags: 
        :tickets: 769

      Fixed bugs in determining proper sync clauses from custom inherit
      conditions.

    .. change::
        :tags: 
        :tickets: 763

      Extended 'engine_from_config' coercion for QueuePool size / overflow.

    .. change::
        :tags: 
        :tickets: 748

      mysql views can be reflected again.

    .. change::
        :tags: 
        :tickets: 

      AssociationProxy can now take custom getters and setters.

    .. change::
        :tags: 
        :tickets: 

      Fixed malfunctioning BETWEEN in orm queries.

    .. change::
        :tags: 
        :tickets: 762

      Fixed OrderedProperties pickling

    .. change::
        :tags: 
        :tickets: 

      SQL-expression defaults and sequences now execute "inline" for all
      non-primary key columns during an INSERT or UPDATE, and for all columns
      during an executemany()-style call. inline=True flag on any insert/update
      statement also forces the same behavior with a single execute().
      result.postfetch_cols() is a collection of columns for which the previous
      single insert or update statement contained a SQL-side default expression.

    .. change::
        :tags: 
        :tickets: 759

      Fixed PG executemany() behavior.

    .. change::
        :tags: 
        :tickets: 

      postgres reflects tables with autoincrement=False for primary key columns
      which have no defaults.

    .. change::
        :tags: 
        :tickets: 

      postgres no longer wraps executemany() with individual execute() calls,
      instead favoring performance.  "rowcount"/"concurrency" checks with
      deleted items (which use executemany) are disabled with PG since psycopg2
      does not report proper rowcount for executemany().

    .. change::
        :tags: tickets, fixed
        :tickets: 742

      

    .. change::
        :tags: tickets, fixed
        :tickets: 748

      

    .. change::
        :tags: tickets, fixed
        :tickets: 760

      

    .. change::
        :tags: tickets, fixed
        :tickets: 762

      

    .. change::
        :tags: tickets, fixed
        :tickets: 763

      

.. changelog::
    :version: 0.4.0beta4
    :released: Wed Aug 22 2007

    .. change::
        :tags: 
        :tickets: 

      Tidied up what ends up in your namespace when you 'from sqlalchemy import \*':

    .. change::
        :tags: 
        :tickets: 

      'table' and 'column' are no longer imported.  They remain available by
      direct reference (as in 'sql.table' and 'sql.column') or a glob import
      from the sql package.  It was too easy to accidentally use a
      sql.expressions.table instead of schema.Table when just starting out
      with SQLAlchemy, likewise column.

    .. change::
        :tags: 
        :tickets: 

      Internal-ish classes like ClauseElement, FromClause, NullTypeEngine,
      etc., are also no longer imported into your namespace

    .. change::
        :tags: 
        :tickets: 

      The 'Smallinteger' compatibility name (small i!) is no longer imported,
      but remains in schema.py for now.  SmallInteger (big I!) is still
      imported.

    .. change::
        :tags: 
        :tickets: 

      The connection pool uses a "threadlocal" strategy internally to return
      the same connection already bound to a thread, for "contextual" connections;
      these are the connections used when you do a "connectionless" execution
      like insert().execute().  This is like a "partial" version of the
      "threadlocal" engine strategy but without the thread-local transaction part
      of it.  We're hoping it reduces connection pool overhead as well as
      database usage.  However, if it proves to impact stability in a negative way,
      we'll roll it right back.

    .. change::
        :tags: 
        :tickets: 

      Fix to bind param processing such that "False" values (like blank strings)
      still get processed/encoded.

    .. change::
        :tags: 
        :tickets: 752

      Fix to select() "generative" behavior, such that calling column(),
      select_from(), correlate(), and with_prefix() does not modify the
      original select object

    .. change::
        :tags: 
        :tickets: 

      Added a "legacy" adapter to types, such that user-defined TypeEngine
      and TypeDecorator classes which define convert_bind_param() and/or
      convert_result_value() will continue to function.  Also supports
      calling the super() version of those methods.

    .. change::
        :tags: 
        :tickets: 

      Added session.prune(), trims away instances cached in a session that
      are no longer referenced elsewhere. (A utility for strong-ref
      identity maps).

    .. change::
        :tags: 
        :tickets: 

      Added close() method to Transaction.  Closes out a transaction using
      rollback if it's the outermost transaction, otherwise just ends
      without affecting the outer transaction.

    .. change::
        :tags: 
        :tickets: 

      Transactional and non-transactional Session integrates better with
      bound connection; a close() will ensure that connection
      transactional state is the same as that which existed on it before
      being bound to the Session.

    .. change::
        :tags: 
        :tickets: 735

      Modified SQL operator functions to be module-level operators,
      allowing SQL expressions to be pickleable.

    .. change::
        :tags: 
        :tickets: 

      Small adjustment to mapper class.__init__ to allow for Py2.6
      object.__init__() behavior.

    .. change::
        :tags: 
        :tickets: 

      Fixed 'prefix' argument for select()

    .. change::
        :tags: 
        :tickets: 

      Connection.begin() no longer accepts nested=True, this logic is now
      all in begin_nested().

    .. change::
        :tags: 
        :tickets: 

      Fixes to new "dynamic" relation loader involving cascades

    .. change::
        :tags: tickets, fixed
        :tickets: 735

      

    .. change::
        :tags: tickets, fixed
        :tickets: 752

      

.. changelog::
    :version: 0.4.0beta3
    :released: Thu Aug 16 2007

    .. change::
        :tags: 
        :tickets: 

      SQL types optimization:

    .. change::
        :tags: 
        :tickets: 

      New performance tests show a combined mass-insert/mass-select test as
      having 68% fewer function calls than the same test run against 0.3.

    .. change::
        :tags: 
        :tickets: 

      General performance improvement of result set iteration is around 10-20%.

    .. change::
        :tags: 
        :tickets: 

      In types.AbstractType, convert_bind_param() and convert_result_value()
      have migrated to callable-returning bind_processor() and
      result_processor() methods.  If no callable is returned, no pre/post
      processing function is called.

    .. change::
        :tags: 
        :tickets: 

      Hooks added throughout base/sql/defaults to optimize the calling of bind
      aram/result processors so that method call overhead is minimized.

    .. change::
        :tags: 
        :tickets: 

      Support added for executemany() scenarios such that unneeded "last row id"
      logic doesn't kick in, parameters aren't excessively traversed.

    .. change::
        :tags: 
        :tickets: 

      Added 'inherit_foreign_keys' arg to mapper().

    .. change::
        :tags: 
        :tickets: 

      Added support for string date passthrough in sqlite.

    .. change::
        :tags: tickets, fixed
        :tickets: 738

      

    .. change::
        :tags: tickets, fixed
        :tickets: 739

      

    .. change::
        :tags: tickets, fixed
        :tickets: 743

      

    .. change::
        :tags: tickets, fixed
        :tickets: 744

      

.. changelog::
    :version: 0.4.0beta2
    :released: Tue Aug 14 2007

    .. change::
        :tags: oracle, improvements.
        :tickets: 

      Auto-commit after LOAD DATA INFILE for mysql.

    .. change::
        :tags: oracle, improvements.
        :tickets: 

      A rudimental SessionExtension class has been added, allowing user-defined
      functionality to take place at flush(), commit(), and rollback() boundaries.

    .. change::
        :tags: oracle, improvements.
        :tickets: 

      Added engine_from_config() function for helping to create_engine() from an
      .ini style config.

    .. change::
        :tags: oracle, improvements.
        :tickets: 

      base_mapper() becomes a plain attribute.

    .. change::
        :tags: oracle, improvements.
        :tickets: 

      session.execute() and scalar() can search for a Table with which to bind from
      using the given ClauseElement.

    .. change::
        :tags: oracle, improvements.
        :tickets: 

      Session automatically extrapolates tables from mappers with binds, also uses
      base_mapper so that inheritance hierarchies bind automatically.

    .. change::
        :tags: oracle, improvements.
        :tickets: 

      Moved ClauseVisitor traversal back to inlined non-recursive.

    .. change::
        :tags: tickets, fixed
        :tickets: 730

      

    .. change::
        :tags: tickets, fixed
        :tickets: 732

      

    .. change::
        :tags: tickets, fixed
        :tickets: 733

      

    .. change::
        :tags: tickets, fixed
        :tickets: 734

      

.. changelog::
    :version: 0.4.0beta1
    :released: Sun Aug 12 2007

    .. change::
        :tags: orm
        :tickets: 

      Speed! Along with recent speedups to ResultProxy, total number of function
      calls significantly reduced for large loads.

    .. change::
        :tags: orm
        :tickets: 

      test/perf/masseagerload.py reports 0.4 as having the fewest number of
      function calls across all SA versions (0.1, 0.2, and 0.3).

    .. change::
        :tags: orm
        :tickets: 213

      New collection_class api and implementation. Collections are
      now instrumented via decorations rather than proxying.  You can now have
      collections that manage their own membership, and your class instance will
      be directly exposed on the relation property.  The changes are transparent
      for most users.

    .. change::
        :tags: orm
        :tickets: 

      InstrumentedList (as it was) is removed, and relation properties no
      longer have 'clear()', '.data', or any other added methods beyond those
      provided by the collection type. You are free, of course, to add them to
      a custom class.

    .. change::
        :tags: orm
        :tickets: 

      __setitem__-like assignments now fire remove events for the existing
      value, if any.

    .. change::
        :tags: orm
        :tickets: 

      dict-likes used as collection classes no longer need to change __iter__
      semantics- itervalues() is used by default instead. This is a backwards
      incompatible change.

    .. change::
        :tags: orm
        :tickets: 

      Subclassing dict for a mapped collection is no longer needed in most
      cases. orm.collections provides canned implementations that key objects
      by a specified column or a custom function of your choice.

    .. change::
        :tags: orm
        :tickets: 

      Collection assignment now requires a compatible type- assigning None to
      clear a collection or assigning a list to a dict collection will now
      raise an argument error.

    .. change::
        :tags: orm
        :tickets: 

      AttributeExtension moved to interfaces, and .delete is now .remove The
      event method signature has also been swapped around.

    .. change::
        :tags: orm
        :tickets: 

      Major overhaul for Query:

    .. change::
        :tags: orm
        :tickets: 

      All selectXXX methods are deprecated.  Generative methods are now the
      standard way to do things, i.e. filter(), filter_by(), all(), one(),
      etc.  Deprecated methods are docstring'ed with their new replacements.

    .. change::
        :tags: orm
        :tickets: 643

      Class-level properties are now usable as query elements... no more
      '.c.'!  "Class.c.propname" is now superseded by "Class.propname".  All
      clause operators are supported, as well as higher level operators such
      as Class.prop==<some instance> for scalar attributes,
      Class.prop.contains(<some instance>) and Class.prop.any(<some
      expression>) for collection-based attributes (all are also
      negatable).  Table-based column expressions as well as columns mounted
      on mapped classes via 'c' are of course still fully available and can be
      freely mixed with the new attributes.

    .. change::
        :tags: orm
        :tickets: 

      Removed ancient query.select_by_attributename() capability.

    .. change::
        :tags: orm
        :tickets: 

      The aliasing logic used by eager loading has been generalized, so that
      it also adds full automatic aliasing support to Query.  It's no longer
      necessary to create an explicit Alias to join to the same tables
      multiple times; *even for self-referential relationships*.
      
      - join() and outerjoin() take arguments "aliased=True".  Yhis causes
        their joins to be built on aliased tables; subsequent calls to
        filter() and filter_by() will translate all table expressions (yes,
        real expressions using the original mapped Table) to be that of the
        Alias for the duration of that join() (i.e. until reset_joinpoint() or
        another join() is called).
      
      - join() and outerjoin() take arguments "id=<somestring>".  When used
        with "aliased=True", the id can be referenced by add_entity(cls,
        id=<somestring>) so that you can select the joined instances even if
        they're from an alias.
      
      - join() and outerjoin() now work with self-referential relationships!
        Using "aliased=True", you can join as many levels deep as desired,
        i.e. query.join(['children', 'children'], aliased=True); filter
        criterion will be against the rightmost joined table

    .. change::
        :tags: orm
        :tickets: 660

      Added query.populate_existing(), marks the query to reload all
      attributes and collections of all instances touched in the query,
      including eagerly-loaded entities.

    .. change::
        :tags: orm
        :tickets: 

      Added eagerload_all(), allows eagerload_all('x.y.z') to specify eager
      loading of all properties in the given path.

    .. change::
        :tags: orm
        :tickets: 

      Major overhaul for Session:

    .. change::
        :tags: orm
        :tickets: 

      New function which "configures" a session called "sessionmaker()".  Send
      various keyword arguments to this function once, returns a new class
      which creates a Session against that stereotype.

    .. change::
        :tags: orm
        :tickets: 

      SessionTransaction removed from "public" API.  You now can call begin()/
      commit()/rollback() on the Session itself.

    .. change::
        :tags: orm
        :tickets: 

      Session also supports SAVEPOINT transactions; call begin_nested().

    .. change::
        :tags: orm
        :tickets: 

      Session supports two-phase commit behavior when vertically or
      horizontally partitioning (i.e., using more than one engine).  Use
      twophase=True.

    .. change::
        :tags: orm
        :tickets: 

      Session flag "transactional=True" produces a session which always places
      itself into a transaction when first used.  Upon commit(), rollback() or
      close(), the transaction ends; but begins again on the next usage.

    .. change::
        :tags: orm
        :tickets: 

      Session supports "autoflush=True".  This issues a flush() before each
      query.  Use in conjunction with transactional, and you can just
      save()/update() and then query, the new objects will be there.  Use
      commit() at the end (or flush() if non-transactional) to flush remaining
      changes.

    .. change::
        :tags: orm
        :tickets: 

      New scoped_session() function replaces SessionContext and assignmapper.
      Builds onto "sessionmaker()" concept to produce a class whos Session()
      construction returns the thread-local session.  Or, call all Session
      methods as class methods, i.e. Session.save(foo); Session.commit().
      just like the old "objectstore" days.

    .. change::
        :tags: orm
        :tickets: 

      Added new "binds" argument to Session to support configuration of
      multiple binds with sessionmaker() function.

    .. change::
        :tags: orm
        :tickets: 

      A rudimental SessionExtension class has been added, allowing
      user-defined functionality to take place at flush(), commit(), and
      rollback() boundaries.

    .. change::
        :tags: orm
        :tickets: 

      Query-based relation()s available with dynamic_loader().  This is a
      *writable* collection (supporting append() and remove()) which is also a
      live Query object when accessed for reads.  Ideal for dealing with very
      large collections where only partial loading is desired.

    .. change::
        :tags: orm
        :tickets: 

      flush()-embedded inline INSERT/UPDATE expressions.  Assign any SQL
      expression, like "sometable.c.column + 1", to an instance's attribute.
      Upon flush(), the mapper detects the expression and embeds it directly in
      the INSERT or UPDATE statement; the attribute gets deferred on the
      instance so it loads the new value the next time you access it.

    .. change::
        :tags: orm
        :tickets: 618

      A rudimental sharding (horizontal scaling) system is introduced.  This
      system uses a modified Session which can distribute read and write
      operations among multiple databases, based on user-defined functions
      defining the "sharding strategy".  Instances and their dependents can be
      distributed and queried among multiple databases based on attribute
      values, round-robin approaches or any other user-defined
      system.

    .. change::
        :tags: orm
        :tickets: 659

      Eager loading has been enhanced to allow even more joins in more places.
      It now functions at any arbitrary depth along self-referential and
      cyclical structures.  When loading cyclical structures, specify
      "join_depth" on relation() indicating how many times you'd like the table
      to join to itself; each level gets a distinct table alias.  The alias
      names themselves are generated at compile time using a simple counting
      scheme now and are a lot easier on the eyes, as well as of course
      completely deterministic.

    .. change::
        :tags: orm
        :tickets: 211

      Added composite column properties.  This allows you to create a type which
      is represented by more than one column, when using the ORM.  Objects of
      the new type are fully functional in query expressions, comparisons,
      query.get() clauses, etc. and act as though they are regular single-column
      scalars... except they're not!  Use the function composite(cls, \*columns)
      inside of the mapper's "properties" dict, and instances of cls will be
      created/mapped to a single attribute, comprised of the values corresponding
      to \*columns.

    .. change::
        :tags: orm
        :tickets: 

      Improved support for custom column_property() attributes which feature
      correlated subqueries, works better with eager loading now.

    .. change::
        :tags: orm
        :tickets: 611

      Primary key "collapse" behavior; the mapper will analyze all columns in
      its given selectable for primary key "equivalence", that is, columns which
      are equivalent via foreign key relationship or via an explicit
      inherit_condition. primarily for joined-table inheritance scenarios where
      different named PK columns in inheriting tables should "collapse" into a
      single-valued (or fewer-valued) primary key.  Fixes things like.

    .. change::
        :tags: orm
        :tickets: 

      Joined-table inheritance will now generate the primary key columns of all
      inherited classes against the root table of the join only.  This implies
      that each row in the root table is distinct to a single instance.  If for
      some rare reason this is not desirable, explicit primary_key settings on
      individual mappers will override it.

    .. change::
        :tags: orm
        :tickets: 

      When "polymorphic" flags are used with joined-table or single-table
      inheritance, all identity keys are generated against the root class of the
      inheritance hierarchy; this allows query.get() to work polymorphically
      using the same caching semantics as a non-polymorphic get.  Note that this
      currently does not work with concrete inheritance.

    .. change::
        :tags: orm
        :tickets: 

      Secondary inheritance loading: polymorphic mappers can be constructed
      *without* a select_table argument. inheriting mappers whose tables were
      not represented in the initial load will issue a second SQL query
      immediately, once per instance (i.e. not very efficient for large lists),
      in order to load the remaining columns.

    .. change::
        :tags: orm
        :tickets: 

      Secondary inheritance loading can also move its second query into a
      column-level "deferred" load, via the "polymorphic_fetch" argument, which
      can be set to 'select' or 'deferred'

    .. change::
        :tags: orm
        :tickets: 696

      It's now possible to map only a subset of available selectable columns
      onto mapper properties, using include_columns/exclude_columns..

    .. change::
        :tags: orm
        :tickets: 

      Added undefer_group() MapperOption, sets a set of "deferred" columns
      joined by a "group" to load as "undeferred".

    .. change::
        :tags: orm
        :tickets: 

      Rewrite of the "deterministic alias name" logic to be part of the SQL
      layer, produces much simpler alias and label names more in the style of
      Hibernate

    .. change::
        :tags: sql
        :tickets: 

      Speed!  Clause compilation as well as the mechanics of SQL constructs have
      been streamlined and simplified to a significant degree, for a 20-30%
      improvement of the statement construction/compilation overhead of 0.3.

    .. change::
        :tags: sql
        :tickets: 

      All "type" keyword arguments, such as those to bindparam(), column(),
      Column(), and func.<something>(), renamed to "type\_".  Those objects still
      name their "type" attribute as "type".

    .. change::
        :tags: sql
        :tickets: 

      case_sensitive=(True|False) setting removed from schema items, since
      checking this state added a lot of method call overhead and there was no
      decent reason to ever set it to False.  Table and column names which are
      all lower case will be treated as case-insensitive (yes we adjust for
      Oracle's UPPERCASE style too).

    .. change::
        :tags: transactions
        :tickets: 

      Added context manager (with statement) support for transactions.

    .. change::
        :tags: transactions
        :tickets: 

      Added support for two phase commit, works with mysql and postgres so far.

    .. change::
        :tags: transactions
        :tickets: 

      Added a subtransaction implementation that uses savepoints.

    .. change::
        :tags: transactions
        :tickets: 

      Added support for savepoints.

    .. change::
        :tags: metadata
        :tickets: 

      Tables can be reflected from the database en-masse without declaring
      them in advance.  MetaData(engine, reflect=True) will load all tables
      present in the database, or use metadata.reflect() for finer control.

    .. change::
        :tags: metadata
        :tickets: 

      DynamicMetaData has been renamed to ThreadLocalMetaData

    .. change::
        :tags: metadata
        :tickets: 

      The ThreadLocalMetaData constructor now takes no arguments.

    .. change::
        :tags: metadata
        :tickets: 

      BoundMetaData has been removed- regular MetaData is equivalent

    .. change::
        :tags: metadata
        :tickets: 646

      Numeric and Float types now have an "asdecimal" flag; defaults to True for
      Numeric, False for Float.  When True, values are returned as
      decimal.Decimal objects; when False, values are returned as float().  The
      defaults of True/False are already the behavior for PG and MySQL's DBAPI
      modules.

    .. change::
        :tags: metadata
        :tickets: 475

      New SQL operator implementation which removes all hardcoded operators from
      expression structures and moves them into compilation; allows greater
      flexibility of operator compilation; for example, "+" compiles to "||"
      when used in a string context, or "concat(a,b)" on MySQL; whereas in a
      numeric context it compiles to "+".  Fixes.

    .. change::
        :tags: metadata
        :tickets: 

      "Anonymous" alias and label names are now generated at SQL compilation
      time in a completely deterministic fashion... no more random hex IDs

    .. change::
        :tags: metadata
        :tickets: 

      Significant architectural overhaul to SQL elements (ClauseElement).  All
      elements share a common "mutability" framework which allows a consistent
      approach to in-place modifications of elements as well as generative
      behavior.  Improves stability of the ORM which makes heavy usage of
      mutations to SQL expressions.

    .. change::
        :tags: metadata
        :tickets: 

      select() and union()'s now have "generative" behavior.  Methods like
      order_by() and group_by() return a *new* instance - the original instance
      is left unchanged.  Non-generative methods remain as well.

    .. change::
        :tags: metadata
        :tickets: 569, 52

      The internals of select/union vastly simplified- all decision making
      regarding "is subquery" and "correlation" pushed to SQL generation phase.
      select() elements are now *never* mutated by their enclosing containers or
      by any dialect's compilation process

    .. change::
        :tags: metadata
        :tickets: 

      select(scalar=True) argument is deprecated; use select(..).as_scalar().
      The resulting object obeys the full "column" interface and plays better
      within expressions.

    .. change::
        :tags: metadata
        :tickets: 504

      Added select().with_prefix('foo') allowing any set of keywords to be
      placed before the columns clause of the SELECT

    .. change::
        :tags: metadata
        :tickets: 686

      Added array slice support to row[<index>]

    .. change::
        :tags: metadata
        :tickets: 

      Result sets make a better attempt at matching the DBAPI types present in
      cursor.description to the TypeEngine objects defined by the dialect, which
      are then used for result-processing. Note this only takes effect for
      textual SQL; constructed SQL statements always have an explicit type map.

    .. change::
        :tags: metadata
        :tickets: 

      Result sets from CRUD operations close their underlying cursor immediately
      and will also autoclose the connection if defined for the operation; this
      allows more efficient usage of connections for successive CRUD operations
      with less chance of "dangling connections".

    .. change::
        :tags: metadata
        :tickets: 559

      Column defaults and onupdate Python functions (i.e. passed to
      ColumnDefault) may take zero or one arguments; the one argument is the
      ExecutionContext, from which you can call "context.parameters[someparam]"
      to access the other bind parameter values affixed to the statement.  The connection used for the execution is available as well
      so that you can pre-execute statements.

    .. change::
        :tags: metadata
        :tickets: 

      Added "explcit" create/drop/execute support for sequences (i.e. you can
      pass a "connectable" to each of those methods on Sequence).

    .. change::
        :tags: metadata
        :tickets: 

      Better quoting of identifiers when manipulating schemas.

    .. change::
        :tags: metadata
        :tickets: 

      Standardized the behavior for table reflection where types can't be
      located; NullType is substituted instead, warning is raised.

    .. change::
        :tags: metadata
        :tickets: 606

      ColumnCollection (i.e. the 'c' attribute on tables) follows dictionary
      semantics for "__contains__"

    .. change::
        :tags: engines
        :tickets: 

      Speed! The mechanics of result processing and bind parameter processing
      have been overhauled, streamlined and optimized to issue as little method
      calls as possible.  Bench tests for mass INSERT and mass rowset iteration
      both show 0.4 to be over twice as fast as 0.3, using 68% fewer function
      calls.

    .. change::
        :tags: engines
        :tickets: 

      You can now hook into the pool lifecycle and run SQL statements or other
      logic at new each DBAPI connection, pool check-out and check-in.

    .. change::
        :tags: engines
        :tickets: 

      Connections gain a .properties collection, with contents scoped to the
      lifetime of the underlying DBAPI connection

    .. change::
        :tags: engines
        :tickets: 

      Removed auto_close_cursors and disallow_open_cursors arguments from Pool;
      reduces overhead as cursors are normally closed by ResultProxy and
      Connection.

    .. change::
        :tags: extensions
        :tickets: 

      proxyengine is temporarily removed, pending an actually working
      replacement.

    .. change::
        :tags: extensions
        :tickets: 

      SelectResults has been replaced by Query.  SelectResults /
      SelectResultsExt still exist but just return a slightly modified Query
      object for backwards-compatibility.  join_to() method from SelectResults
      isn't present anymore, need to use join().

    .. change::
        :tags: mysql
        :tickets: 

      Table and column names loaded via reflection are now Unicode.

    .. change::
        :tags: mysql
        :tickets: 

      All standard column types are now supported, including SET.

    .. change::
        :tags: mysql
        :tickets: 

      Table reflection can now be performed in as little as one round-trip.

    .. change::
        :tags: mysql
        :tickets: 

      ANSI and ANSI_QUOTES sql modes are now supported.

    .. change::
        :tags: mysql
        :tickets: 

      Indexes are now reflected.

    .. change::
        :tags: postgres
        :tickets: 

      Added PGArray datatype for using postgres array datatypes.

    .. change::
        :tags: oracle
        :tickets: 507

      Very rudimental support for OUT parameters added; use sql.outparam(name,
      type) to set up an OUT parameter, just like bindparam(); after execution,
      values are available via result.out_parameters dictionary.
