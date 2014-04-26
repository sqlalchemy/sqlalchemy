
==============
0.6 Changelog
==============

                
.. changelog::
    :version: 0.6.9
    :released: Sat May 05 2012

    .. change::
        :tags: general
        :tickets: 2279

      Adjusted the "importlater" mechanism, which is
      used internally to resolve import cycles,
      such that the usage of __import__ is completed
      when the import of sqlalchemy or sqlalchemy.orm
      is done, thereby avoiding any usage of __import__
      after the application starts new threads,
      fixes.

    .. change::
        :tags: orm
        :tickets: 2197

      Fixed bug whereby the source clause
      used by query.join() would be inconsistent
      if against a column expression that combined
      multiple entities together.

    .. change::
        :tags: orm, bug
        :tickets: 2310

      fixed inappropriate evaluation of user-mapped
      object in a boolean context within query.get().

    .. change::
        :tags: orm
        :tickets: 2228

      Fixed bug apparent only in Python 3 whereby
      sorting of persistent + pending objects during
      flush would produce an illegal comparison,
      if the persistent object primary key
      is not a single integer.

    .. change::
        :tags: orm
        :tickets: 2234

      Fixed bug where query.join() + aliased=True
      from a joined-inh structure to itself on
      relationship() with join condition on the child
      table would convert the lead entity into the
      joined one inappropriately.

    .. change::
        :tags: orm
        :tickets: 2287

      Fixed bug whereby mapper.order_by attribute would
      be ignored in the "inner" query within a
      subquery eager load. .

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

    .. change::
        :tags: orm
        :tickets: 2188

      Fixed subtle bug that caused SQL to blow
      up if: column_property() against subquery +
      joinedload + LIMIT + order by the column
      property() occurred. .

    .. change::
        :tags: orm
        :tickets: 2207

      The join condition produced by with_parent
      as well as when using a "dynamic" relationship
      against a parent will generate unique
      bindparams, rather than incorrectly repeating
      the same bindparam. .

    .. change::
        :tags: orm
        :tickets: 2199

      Repaired the "no statement condition"
      assertion in Query which would attempt
      to raise if a generative method were called
      after from_statement() were called..

    .. change::
        :tags: orm
        :tickets: 1776

      Cls.column.collate("some collation") now
      works.

    .. change::
        :tags: orm, bug
        :tickets: 2297

      Fixed the error formatting raised when
      a tuple is inadvertently passed to session.query().

    .. change::
        :tags: engine
        :tickets: 2317

      Backported the fix for introduced
      in 0.7.4, which ensures that the connection
      is in a valid state before attempting to call
      rollback()/prepare()/release() on savepoint
      and two-phase transactions.

    .. change::
        :tags: sql
        :tickets: 2188

      Fixed two subtle bugs involving column
      correspondence in a selectable,
      one with the same labeled subquery repeated, the other
      when the label has been "grouped" and
      loses itself.  Affects.

    .. change::
        :tags: sql
        :tickets: 

      Fixed bug whereby "warn on unicode" flag
      would get set for the String type
      when used with certain dialects.  This
      bug is not in 0.7.

    .. change::
        :tags: sql
        :tickets: 2270

      Fixed bug whereby with_only_columns() method of
      Select would fail if a selectable were passed..   However, the FROM behavior is
      still incorrect here, so you need 0.7 in
      any case for this use case to be usable.

    .. change::
        :tags: schema
        :tickets: 

      Added an informative error message when
      ForeignKeyConstraint refers to a column name in
      the parent that is not found.

    .. change::
        :tags: postgresql
        :tickets: 2291, 2141

      Fixed bug related to whereby the
      same modified index behavior in PG 9 affected
      primary key reflection on a renamed column..

    .. change::
        :tags: mysql
        :tickets: 2186

      Fixed OurSQL dialect to use ansi-neutral
      quote symbol "'" for XA commands instead
      of '"'. .

    .. change::
        :tags: mysql
        :tickets: 2225

      a CREATE TABLE will put the COLLATE option
      after CHARSET, which appears to be part of
      MySQL's arbitrary rules regarding if it will actually
      work or not.

    .. change::
        :tags: mssql, bug
        :tickets: 2269

      Decode incoming values when retrieving
      list of index names and the names of columns
      within those indexes.

    .. change::
        :tags: oracle
        :tickets: 2200

      Added ORA-00028 to disconnect codes, use
      cx_oracle _Error.code to get at the code,.

    .. change::
        :tags: oracle
        :tickets: 2220

      repaired the oracle.RAW type which did not
      generate the correct DDL.

    .. change::
        :tags: oracle
        :tickets: 2212

      added CURRENT to reserved word list.

    .. change::
        :tags: examples
        :tickets: 2266

      Adjusted dictlike-polymorphic.py example
      to apply the CAST such that it works on
      PG, other databases.

.. changelog::
    :version: 0.6.8
    :released: Sun Jun 05 2011

    .. change::
        :tags: orm
        :tickets: 2144

      Calling query.get() against a column-based entity is
      invalid, this condition now raises a deprecation warning.

    .. change::
        :tags: orm
        :tickets: 2151

      a non_primary mapper will inherit the _identity_class
      of the primary mapper.  This so that a non_primary
      established against a class that's normally in an
      inheritance mapping will produce results that are
      identity-map compatible with that of the primary
      mapper

    .. change::
        :tags: orm
        :tickets: 2148

      Backported 0.7's identity map implementation, which
      does not use a mutex around removal.  This as some users
      were still getting deadlocks despite the adjustments
      in 0.6.7; the 0.7 approach that doesn't use a mutex
      does not appear to produce "dictionary changed size"
      issues, the original rationale for the mutex.

    .. change::
        :tags: orm
        :tickets: 2163

      Fixed the error message emitted for "can't
      execute syncrule for destination column 'q';
      mapper 'X' does not map this column" to
      reference the correct mapper. .

    .. change::
        :tags: orm
        :tickets: 2149

      Fixed bug where determination of "self referential"
      relationship would fail with no workaround
      for joined-inh subclass related to itself,
      or joined-inh subclass related to a subclass
      of that with no cols in the sub-sub class
      in the join condition.

    .. change::
        :tags: orm
        :tickets: 2153

      mapper() will ignore non-configured foreign keys
      to unrelated tables when determining inherit
      condition between parent and child class.
      This is equivalent to behavior already
      applied to declarative.  Note that 0.7 has a
      more comprehensive solution to this, altering
      how join() itself determines an FK error.

    .. change::
        :tags: orm
        :tickets: 2171

      Fixed bug whereby mapper mapped to an anonymous
      alias would fail if logging were used, due to
      unescaped % sign in the alias name.

    .. change::
        :tags: orm
        :tickets: 2170

      Modify the text of the message which occurs
      when the "identity" key isn't detected on
      flush, to include the common cause that
      the Column isn't set up to detect
      auto-increment correctly;.

    .. change::
        :tags: orm
        :tickets: 2182

      Fixed bug where transaction-level "deleted"
      collection wouldn't be cleared of expunged
      states, raising an error if they later
      became transient.

    .. change::
        :tags: sql
        :tickets: 2147

      Fixed bug whereby if FetchedValue was passed
      to column server_onupdate, it would not
      have its parent "column" assigned, added
      test coverage for all column default assignment
      patterns.

    .. change::
        :tags: sql
        :tickets: 2167

      Fixed bug whereby nesting a label of a select()
      with another label in it would produce incorrect
      exported columns.   Among other things this would
      break an ORM column_property() mapping against
      another column_property(). .

    .. change::
        :tags: engine
        :tickets: 2178

      Adjusted the __contains__() method of
      a RowProxy result row such that no exception
      throw is generated internally;
      NoSuchColumnError() also will generate its
      message regardless of whether or not the column
      construct can be coerced to a string..

    .. change::
        :tags: postgresql
        :tickets: 2141

      Fixed bug affecting PG 9 whereby index reflection
      would fail if against a column whose name
      had changed. .

    .. change::
        :tags: postgresql
        :tickets: 2175

      Some unit test fixes regarding numeric arrays,
      MATCH operator.   A potential floating-point
      inaccuracy issue was fixed, and certain tests
      of the MATCH operator only execute within an
      EN-oriented locale for now. .

    .. change::
        :tags: mssql
        :tickets: 2169

      Fixed bug in MSSQL dialect whereby the aliasing
      applied to a schema-qualified table would leak
      into enclosing select statements.

    .. change::
        :tags: mssql
        :tickets: 2159

      Fixed bug whereby DATETIME2 type would fail on
      the "adapt" step when used in result sets or
      bound parameters.  This issue is not in 0.7.

.. changelog::
    :version: 0.6.7
    :released: Wed Apr 13 2011

    .. change::
        :tags: orm
        :tickets: 2087

      Tightened the iterate vs. remove mutex around the
      identity map iteration, attempting to reduce the
      chance of an (extremely rare) reentrant gc operation
      causing a deadlock.  Might remove the mutex in
      0.7.

    .. change::
        :tags: orm
        :tickets: 2030

      Added a `name` argument to `Query.subquery()`, to allow
      a fixed name to be assigned to the alias object.

    .. change::
        :tags: orm
        :tickets: 2019

      A warning is emitted when a joined-table inheriting mapper
      has no primary keys on the locally mapped table
      (but has pks on the superclass table).

    .. change::
        :tags: orm
        :tickets: 2038

      Fixed bug where "middle" class in a polymorphic hierarchy
      would have no 'polymorphic_on' column if it didn't also
      specify a 'polymorphic_identity', leading to strange
      errors upon refresh, wrong class loaded when querying
      from that target. Also emits the correct WHERE criterion
      when using single table inheritance.

    .. change::
        :tags: orm
        :tickets: 1995

      Fixed bug where a column with a SQL or server side default
      that was excluded from a mapping with include_properties
      or exclude_properties would result in UnmappedColumnError.

    .. change::
        :tags: orm
        :tickets: 2046

      A warning is emitted in the unusual case that an
      append or similar event on a collection occurs after
      the parent object has been dereferenced, which
      prevents the parent from being marked as "dirty"
      in the session.  This will be an exception in 0.7.

    .. change::
        :tags: orm
        :tickets: 2098

      Fixed bug in query.options() whereby a path
      applied to a lazyload using string keys could
      overlap a same named attribute on the wrong
      entity.  Note 0.7 has an updated version of this
      fix.

    .. change::
        :tags: orm
        :tickets: 2063

      Reworded the exception raised when a flush
      is attempted of a subclass that is not polymorphic
      against the supertype.

    .. change::
        :tags: orm
        :tickets: 2123

      Some fixes to the state handling regarding
      backrefs, typically when autoflush=False, where
      the back-referenced collection wouldn't
      properly handle add/removes with no net
      change.  Thanks to Richard Murri for the
      test case + patch.

    .. change::
        :tags: orm
        :tickets: 2130

      a "having" clause would be copied from the
      inside to the outside query if from_self()
      were used..

    .. change::
        :tags: sql
        :tickets: 2028

      Column.copy(), as used in table.tometadata(), copies the
      'doc' attribute.

    .. change::
        :tags: sql
        :tickets: 2023

      Added some defs to the resultproxy.c extension so that
      the extension compiles and runs on Python 2.4.

    .. change::
        :tags: sql
        :tickets: 2042

      The compiler extension now supports overriding the default
      compilation of expression._BindParamClause including that
      the auto-generated binds within the VALUES/SET clause
      of an insert()/update() statement will also use the new
      compilation rules.

    .. change::
        :tags: sql
        :tickets: 2089

      Added accessors to ResultProxy "returns_rows", "is_insert"

    .. change::
        :tags: sql
        :tickets: 2116

      The limit/offset keywords to select() as well
      as the value passed to select.limit()/offset()
      will be coerced to integer.

    .. change::
        :tags: engine
        :tickets: 2102

      Fixed bug in QueuePool, SingletonThreadPool whereby
      connections that were discarded via overflow or periodic
      cleanup() were not explicitly closed, leaving garbage
      collection to the task instead.   This generally only
      affects non-reference-counting backends like Jython
      and Pypy.  Thanks to Jaimy Azle for spotting
      this.

    .. change::
        :tags: sqlite
        :tickets: 2115

      Fixed bug where reflection of foreign key
      created as "REFERENCES <tablename>" without
      col name would fail.

    .. change::
        :tags: postgresql
        :tickets: 1083

      When explicit sequence execution derives the name
      of the auto-generated sequence of a SERIAL column,
      which currently only occurs if implicit_returning=False,
      now accommodates if the table + column name is greater
      than 63 characters using the same logic Postgresql uses.

    .. change::
        :tags: postgresql
        :tickets: 2044

      Added an additional libpq message to the list of "disconnect"
      exceptions, "could not receive data from server"

    .. change::
        :tags: postgresql
        :tickets: 2092

      Added RESERVED_WORDS for postgresql dialect.

    .. change::
        :tags: postgresql
        :tickets: 2073

      Fixed the BIT type to allow a "length" parameter, "varying"
      parameter.  Reflection also fixed.

    .. change::
        :tags: informix
        :tickets: 2092

      Added RESERVED_WORDS informix dialect.

    .. change::
        :tags: mssql
        :tickets: 2071

      Rewrote the query used to get the definition of a view,
      typically when using the Inspector interface, to
      use sys.sql_modules instead of the information schema,
      thereby allowing views definitions longer than 4000
      characters to be fully returned.

    .. change::
        :tags: mysql
        :tickets: 2047

      oursql dialect accepts the same "ssl" arguments in
      create_engine() as that of MySQLdb.

    .. change::
        :tags: firebird
        :tickets: 2083

      The "implicit_returning" flag on create_engine() is
      honored if set to False.

    .. change::
        :tags: oracle
        :tickets: 2100

      Using column names that would require quotes
      for the column itself or for a name-generated
      bind parameter, such as names with special
      characters, underscores, non-ascii characters,
      now properly translate bind parameter keys when
      talking to cx_oracle.

    .. change::
        :tags: oracle
        :tickets: 2116

      Oracle dialect adds use_binds_for_limits=False
      create_engine() flag, will render the LIMIT/OFFSET
      values inline instead of as binds, reported to
      modify the execution plan used by Oracle.

    .. change::
        :tags: ext
        :tickets: 2090

      The horizontal_shard ShardedSession class accepts the common
      Session argument "query_cls" as a constructor argument,
      to enable further subclassing of ShardedQuery.

    .. change::
        :tags: declarative
        :tickets: 2050

      Added an explicit check for the case that the name
      'metadata' is used for a column attribute on a
      declarative class.

    .. change::
        :tags: declarative
        :tickets: 2061

      Fix error message referencing old @classproperty
      name to reference @declared_attr

    .. change::
        :tags: declarative
        :tickets: 2091

      Arguments in __mapper_args__ that aren't "hashable"
      aren't mistaken for always-hashable, possibly-column
      arguments.

    .. change::
        :tags: documentation
        :tickets: 2029

      Documented SQLite DATE/TIME/DATETIME types.

    .. change::
        :tags: examples
        :tickets: 2090

      The Beaker caching example allows a "query_cls" argument
      to the query_callable() function.

.. changelog::
    :version: 0.6.6
    :released: Sat Jan 08 2011

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug whereby a non-"mutable" attribute modified event
      which occurred on an object that was clean except for
      preceding mutable attribute changes would fail to strongly
      reference itself in the identity map. This would cause the
      object to be garbage collected, losing track of any changes
      that weren't previously saved in the "mutable changes"
      dictionary.

    .. change::
        :tags: orm
        :tickets: 2013

      Fixed bug whereby "passive_deletes='all'" wasn't passing
      the correct symbols to lazy loaders during flush, thereby
      causing an unwarranted load.

    .. change::
        :tags: orm
        :tickets: 1997

      Fixed bug which prevented composite mapped
      attributes from being used on a mapped select statement.. Note the workings of composite are slated to
      change significantly in 0.7.

    .. change::
        :tags: orm
        :tickets: 1976

      active_history flag also added to composite().
      The flag has no effect in 0.6, but is instead
      a placeholder flag for forwards compatibility,
      as it applies in 0.7 for composites.

    .. change::
        :tags: orm
        :tickets: 2002

      Fixed uow bug whereby expired objects passed to
      Session.delete() would not have unloaded references
      or collections taken into account when deleting
      objects, despite passive_deletes remaining at
      its default of False.

    .. change::
        :tags: orm
        :tickets: 1987

      A warning is emitted when version_id_col is specified
      on an inheriting mapper when the inherited mapper
      already has one, if those column expressions are not
      the same.

    .. change::
        :tags: orm
        :tickets: 1954

      "innerjoin" flag doesn't take effect along the chain
      of joinedload() joins if a previous join in that chain
      is an outer join, thus allowing primary rows without
      a referenced child row to be correctly returned
      in results.

    .. change::
        :tags: orm
        :tickets: 1964

      Fixed bug regarding "subqueryload" strategy whereby
      strategy would fail if the entity was an aliased()
      construct.

    .. change::
        :tags: orm
        :tickets: 2014

      Fixed bug regarding "subqueryload" strategy whereby
      the join would fail if using a multi-level load
      of the form from A->joined-subclass->C

    .. change::
        :tags: orm
        :tickets: 1968

      Fixed indexing of Query objects by -1. It was erroneously
      transformed to the empty slice -1:0 that resulted in
      IndexError.

    .. change::
        :tags: orm
        :tickets: 1971

      The mapper argument "primary_key" can be passed as a
      single column as well as a list or tuple. 
      The documentation examples that illustrated it as a
      scalar value have been changed to lists.

    .. change::
        :tags: orm
        :tickets: 1961

      Added active_history flag to relationship()
      and column_property(), forces attribute events to
      always load the "old" value, so that it's available to
      attributes.get_history().

    .. change::
        :tags: orm
        :tickets: 1977

      Query.get() will raise if the number of params
      in a composite key is too large, as well as too
      small.

    .. change::
        :tags: orm
        :tickets: 1992

      Backport of "optimized get" fix from 0.7,
      improves the generation of joined-inheritance
      "load expired row" behavior.

    .. change::
        :tags: orm
        :tickets: 

      A little more verbiage to the "primaryjoin" error,
      in an unusual condition that the join condition
      "works" for viewonly but doesn't work for non-viewonly,
      and foreign_keys wasn't used - adds "foreign_keys" to
      the suggestion.  Also add "foreign_keys" to the
      suggestion for the generic "direction" error.

    .. change::
        :tags: sql
        :tickets: 1984

      Fixed operator precedence rules for multiple
      chains of a single non-associative operator.
      I.e. "x - (y - z)" will compile as "x - (y - z)"
      and not "x - y - z".  Also works with labels,
      i.e. "x - (y - z).label('foo')"

    .. change::
        :tags: sql
        :tickets: 1967

      The 'info' attribute of Column is copied during
      Column.copy(), i.e. as occurs when using columns
      in declarative mixins.

    .. change::
        :tags: sql
        :tickets: 

      Added a bind processor for booleans which coerces
      to int, for DBAPIs such as pymssql that naively call
      str() on values.

    .. change::
        :tags: sql
        :tickets: 2000

      CheckConstraint will copy its 'initially', 'deferrable',
      and '_create_rule' attributes within a copy()/tometadata()

    .. change::
        :tags: engine
        :tickets: 

      The "unicode warning" against non-unicode bind data
      is now raised only when the
      Unicode type is used explicitly; not when
      convert_unicode=True is used on the engine
      or String type.

    .. change::
        :tags: engine
        :tickets: 1978

      Fixed memory leak in C version of Decimal result
      processor.

    .. change::
        :tags: engine
        :tickets: 1871

      Implemented sequence check capability for the C
      version of RowProxy, as well as 2.7 style
      "collections.Sequence" registration for RowProxy.

    .. change::
        :tags: engine
        :tickets: 1998

      Threadlocal engine methods rollback(), commit(),
      prepare() won't raise if no transaction is in progress;
      this was a regression introduced in 0.6.

    .. change::
        :tags: engine
        :tickets: 2004

      Threadlocal engine returns itself upon begin(),
      begin_nested(); engine then implements contextmanager
      methods to allow the "with" statement.

    .. change::
        :tags: postgresql
        :tickets: 1984

      Single element tuple expressions inside an IN clause
      parenthesize correctly, also from

    .. change::
        :tags: postgresql
        :tickets: 1955

      Ensured every numeric, float, int code, scalar + array,
      are recognized by psycopg2 and pg8000's "numeric"
      base type.

    .. change::
        :tags: postgresql
        :tickets: 1956

      Added as_uuid=True flag to the UUID type, will receive
      and return values as Python UUID() objects rather than
      strings.  Currently, the UUID type is only known to
      work with psycopg2.

    .. change::
        :tags: postgresql
        :tickets: 1989

      Fixed bug whereby KeyError would occur with non-ENUM
      supported PG versions after a pool dispose+recreate
      would occur.

    .. change::
        :tags: mysql
        :tickets: 1960

      Fixed error handling for Jython + zxjdbc, such that
      has_table() property works again.  Regression from
      0.6.3 (we don't have a Jython buildbot, sorry)

    .. change::
        :tags: sqlite
        :tickets: 1851

      The REFERENCES clause in a CREATE TABLE that includes
      a remote schema to another table with the same schema
      name now renders the remote name without
      the schema clause, as required by SQLite.

    .. change::
        :tags: sqlite
        :tickets: 

      On the same theme, the REFERENCES clause in a CREATE TABLE
      that includes a remote schema to a *different* schema
      than that of the parent table doesn't render at all,
      as cross-schema references do not appear to be supported.

    .. change::
        :tags: mssql
        :tickets: 1770

      The rewrite of index reflection in was
      unfortunately not tested correctly, and returned incorrect
      results.   This regression is now fixed.

    .. change::
        :tags: oracle
        :tickets: 1953

      The cx_oracle "decimal detection" logic, which takes place
      for result set columns with ambiguous numeric characteristics,
      now uses the decimal point character determined by the locale/
      NLS_LANG setting, using an on-first-connect detection of
      this character.  cx_oracle 5.0.3 or greater is also required
      when using a non-period-decimal-point NLS_LANG setting..

    .. change::
        :tags: firebird
        :tickets: 2012

      Firebird numeric type now checks for Decimal explicitly,
      lets float() pass right through, thereby allowing
      special values such as float('inf').

    .. change::
        :tags: declarative
        :tickets: 1972

      An error is raised if __table_args__ is not in tuple
      or dict format, and is not None.

    .. change::
        :tags: sqlsoup
        :tickets: 1975

      Added "map_to()" method to SqlSoup, which is a "master"
      method which accepts explicit arguments for each aspect of
      the selectable and mapping, including a base class per
      mapping.

    .. change::
        :tags: sqlsoup
        :tickets: 

      Mapped selectables used with the map(), with_labels(),
      join() methods no longer put the given argument into the
      internal "cache" dictionary.  Particularly since the
      join() and select() objects are created in the method
      itself this was pretty much a pure memory leaking behavior.

    .. change::
        :tags: examples
        :tickets: 

      The versioning example now supports detection of changes
      in an associated relationship().

.. changelog::
    :version: 0.6.5
    :released: Sun Oct 24 2010

    .. change::
        :tags: orm
        :tickets: 1914

      Added a new "lazyload" option "immediateload".
      Issues the usual "lazy" load operation automatically
      as the object is populated.   The use case
      here is when loading objects to be placed in
      an offline cache, or otherwise used after
      the session isn't available, and straight 'select'
      loading, not 'joined' or 'subquery', is desired.

    .. change::
        :tags: orm
        :tickets: 1920

      New Query methods: query.label(name), query.as_scalar(),
      return the query's statement as a scalar subquery
      with /without label;
      query.with_entities(\*ent), replaces the SELECT list of
      the query with new entities.
      Roughly equivalent to a generative form of query.values()
      which accepts mapped entities as well as column
      expressions.

    .. change::
        :tags: orm
        :tickets: 

      Fixed recursion bug which could occur when moving
      an object from one reference to another, with
      backrefs involved, where the initiating parent
      was a subclass (with its own mapper) of the
      previous parent.

    .. change::
        :tags: orm
        :tickets: 1918

      Fixed a regression in 0.6.4 which occurred if you
      passed an empty list to "include_properties" on
      mapper()

    .. change::
        :tags: orm
        :tickets: 

      Fixed labeling bug in Query whereby the NamedTuple
      would mis-apply labels if any of the column
      expressions were un-labeled.

    .. change::
        :tags: orm
        :tickets: 1925

      Patched a case where query.join() would adapt the
      right side to the right side of the left's join
      inappropriately

    .. change::
        :tags: orm
        :tickets: 

      Query.select_from() has been beefed up to help
      ensure that a subsequent call to query.join()
      will use the select_from() entity, assuming it's
      a mapped entity and not a plain selectable,
      as the default "left" side, not the first entity
      in the Query object's list of entities.

    .. change::
        :tags: orm
        :tickets: 

      The exception raised by Session when it is used
      subsequent to a subtransaction rollback (which is what
      happens when a flush fails in autocommit=False mode) has
      now been reworded (this is the "inactive due to a
      rollback in a subtransaction" message). In particular,
      if the rollback was due to an exception during flush(),
      the message states this is the case, and reiterates the
      string form of the original exception that occurred
      during flush. If the session is closed due to explicit
      usage of subtransactions (not very common), the message
      just states this is the case.

    .. change::
        :tags: orm
        :tickets: 

      The exception raised by Mapper when repeated requests to
      its initialization are made after initialization already
      failed no longer assumes the "hasattr" case, since
      there's other scenarios in which this message gets
      emitted, and the message also does not compound onto
      itself multiple times - you get the same message for
      each attempt at usage. The misnomer "compiles" is being
      traded out for "initialize".

    .. change::
        :tags: orm
        :tickets: 1935

      Fixed bug in query.update() where 'evaluate' or 'fetch'
      expiration would fail if the column expression key was
      a class attribute with a different keyname as the
      actual column name.

    .. change::
        :tags: orm
        :tickets: 

      Added an assertion during flush which ensures
      that no NULL-holding identity keys were generated
      on "newly persistent" objects.
      This can occur when user defined code inadvertently
      triggers flushes on not-fully-loaded objects.

    .. change::
        :tags: orm
        :tickets: 1910

      lazy loads for relationship attributes now use
      the current state, not the "committed" state,
      of foreign and primary key attributes
      when issuing SQL, if a flush is not in process.
      Previously, only the database-committed state would
      be used.  In particular, this would cause a many-to-one
      get()-on-lazyload operation to fail, as autoflush
      is not triggered on these loads when the attributes are
      determined and the "committed" state may not be
      available.

    .. change::
        :tags: orm
        :tickets: 

      A new flag on relationship(), load_on_pending, allows
      the lazy loader to fire off on pending objects without a
      flush taking place, as well as a transient object that's
      been manually "attached" to the session. Note that this
      flag blocks attribute events from taking place when an
      object is loaded, so backrefs aren't available until
      after a flush. The flag is only intended for very
      specific use cases.

    .. change::
        :tags: orm
        :tickets: 

      Another new flag on relationship(), cascade_backrefs,
      disables the "save-update" cascade when the event was
      initiated on the "reverse" side of a bidirectional
      relationship.   This is a cleaner behavior so that
      many-to-ones can be set on a transient object without
      it getting sucked into the child object's session,
      while still allowing the forward collection to
      cascade.   We *might* default this to False in 0.7.

    .. change::
        :tags: orm
        :tickets: 

      Slight improvement to the behavior of
      "passive_updates=False" when placed only on the
      many-to-one side of a relationship; documentation has
      been clarified that passive_updates=False should really
      be on the one-to-many side.

    .. change::
        :tags: orm
        :tickets: 

      Placing passive_deletes=True on a many-to-one emits
      a warning, since you probably intended to put it on
      the one-to-many side.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug that would prevent "subqueryload" from
      working correctly with single table inheritance
      for a relationship from a subclass - the "where
      type in (x, y, z)" only gets placed on the inside,
      instead of repeatedly.

    .. change::
        :tags: orm
        :tickets: 

      When using from_self() with single table inheritance,
      the "where type in (x, y, z)" is placed on the outside
      of the query only, instead of repeatedly.   May make
      some more adjustments to this.

    .. change::
        :tags: orm
        :tickets: 1924

      scoped_session emits a warning when configure() is
      called if a Session is already present (checks only the
      current thread)

    .. change::
        :tags: orm
        :tickets: 1932

      reworked the internals of mapper.cascade_iterator() to
      cut down method calls by about 9% in some circumstances.

    .. change::
        :tags: sql
        :tickets: 

      Fixed bug in TypeDecorator whereby the dialect-specific
      type was getting pulled in to generate the DDL for a
      given type, which didn't always return the correct result.

    .. change::
        :tags: sql
        :tickets: 

      TypeDecorator can now have a fully constructed type
      specified as its "impl", in addition to a type class.

    .. change::
        :tags: sql
        :tickets: 

      TypeDecorator will now place itself as the resulting
      type for a binary expression where the type coercion
      rules would normally return its impl type - previously,
      a copy of the impl type would be returned which would
      have the TypeDecorator embedded into it as the "dialect"
      impl, this was probably an unintentional way of achieving
      the desired effect.

    .. change::
        :tags: sql
        :tickets: 

      TypeDecorator.load_dialect_impl() returns "self.impl" by
      default, i.e. not the dialect implementation type of
      "self.impl".   This to support compilation correctly.
      Behavior can be user-overridden in exactly the same way
      as before to the same effect.

    .. change::
        :tags: sql
        :tickets: 

      Added type_coerce(expr, type\_) expression element.
      Treats the given expression as the given type when evaluating
      expressions and processing result rows, but does not
      affect the generation of SQL, other than an anonymous
      label.

    .. change::
        :tags: sql
        :tickets: 

      Table.tometadata() now copies Index objects associated
      with the Table as well.

    .. change::
        :tags: sql
        :tickets: 

      Table.tometadata() issues a warning if the given Table
      is already present in the target MetaData - the existing
      Table object is returned.

    .. change::
        :tags: sql
        :tickets: 

      An informative error message is raised if a Column
      which has not yet been assigned a name, i.e. as in
      declarative, is used in a context where it is
      exported to the columns collection of an enclosing
      select() construct, or if any construct involving
      that column is compiled before its name is
      assigned.

    .. change::
        :tags: sql
        :tickets: 1862

      as_scalar(), label() can be called on a selectable
      which contains a Column that is not yet named.

    .. change::
        :tags: sql
        :tickets: 1907

      Fixed recursion overflow which could occur when operating
      with two expressions both of type "NullType", but
      not the singleton NULLTYPE instance.

    .. change::
        :tags: declarative
        :tickets: 1922

      @classproperty (soon/now @declared_attr) takes effect for
      __mapper_args__, __table_args__, __tablename__ on
      a base class that is not a mixin, as well as mixins.

    .. change::
        :tags: declarative
        :tickets: 1915

      @classproperty 's official name/location for usage
      with declarative is sqlalchemy.ext.declarative.declared_attr.
      Same thing, but moving there since it is more of a
      "marker" that's specific to declararative,
      not just an attribute technique.

    .. change::
        :tags: declarative
        :tickets: 1931, 1930

      Fixed bug whereby columns on a mixin wouldn't propagate
      correctly to a single-table, or joined-table,
      inheritance scheme where the attribute name is
      different than that of the column.,.

    .. change::
        :tags: declarative
        :tickets: 

      A mixin can now specify a column that overrides
      a column of the same name associated with a superclass.
      Thanks to Oystein Haaland.

    .. change::
        :tags: engine
        :tickets: 

      Fixed a regression in 0.6.4 whereby the change that
      allowed cursor errors to be raised consistently broke
      the result.lastrowid accessor.   Test coverage has
      been added for result.lastrowid.   Note that lastrowid
      is only supported by Pysqlite and some MySQL drivers,
      so isn't super-useful in the general case.

    .. change::
        :tags: engine
        :tickets: 

      the logging message emitted by the engine when
      a connection is first used is now "BEGIN (implicit)"
      to emphasize that DBAPI has no explicit begin().

    .. change::
        :tags: engine
        :tickets: 1936

      added "views=True" option to metadata.reflect(),
      will add the list of available views to those
      being reflected.

    .. change::
        :tags: engine
        :tickets: 1899

      engine_from_config() now accepts 'debug' for
      'echo', 'echo_pool', 'force' for 'convert_unicode',
      boolean values for 'use_native_unicode'.

    .. change::
        :tags: postgresql
        :tickets: 

      Added "as_tuple" flag to ARRAY type, returns results
      as tuples instead of lists to allow hashing.

    .. change::
        :tags: postgresql
        :tickets: 1933

      Fixed bug which prevented "domain" built from a
      custom type such as "enum" from being reflected.

    .. change::
        :tags: mysql
        :tickets: 1940

      Fixed bug involving reflection of CURRENT_TIMESTAMP
      default used with ON UPDATE clause, thanks to
      Taavi Burns

    .. change::
        :tags: oracle
        :tickets: 1878

      The implicit_retunring argument to create_engine()
      is now honored regardless of detected version of
      Oracle.  Previously, the flag would be forced
      to False if server version info was < 10.

    .. change::
        :tags: mssql
        :tickets: 1946

      Fixed reflection bug which did not properly handle
      reflection of unknown types.

    .. change::
        :tags: mssql
        :tickets: 1943

      Fixed bug where aliasing of tables with "schema" would
      fail to compile properly.

    .. change::
        :tags: mssql
        :tickets: 1770

      Rewrote the reflection of indexes to use sys.
      catalogs, so that column names of any configuration
      (spaces, embedded commas, etc.) can be reflected.
      Note that reflection of indexes requires SQL
      Server 2005 or greater.

    .. change::
        :tags: mssql
        :tickets: 1952

      mssql+pymssql dialect now honors the "port" portion
      of the URL instead of discarding it.

    .. change::
        :tags: informix
        :tickets: 1906

      *Major* cleanup / modernization of the Informix
      dialect for 0.6, courtesy Florian Apolloner.

    .. change::
        :tags: tests
        :tickets: 

      the NoseSQLAlchemyPlugin has been moved to a
      new package "sqlalchemy_nose" which installs
      along with "sqlalchemy".  This so that the "nosetests"
      script works as always but also allows the
      --with-coverage option to turn on coverage before
      SQLAlchemy modules are imported, allowing coverage
      to work correctly.

    .. change::
        :tags: misc
        :tickets: 1890

      CircularDependencyError now has .cycles and .edges
      members, which are the set of elements involved in
      one or more cycles, and the set of edges as 2-tuples.

.. changelog::
    :version: 0.6.4
    :released: Tue Sep 07 2010

    .. change::
        :tags: orm
        :tickets: 

      The name ConcurrentModificationError has been
      changed to StaleDataError, and descriptive
      error messages have been revised to reflect
      exactly what the issue is.   Both names will
      remain available for the forseeable future
      for schemes that may be specifying
      ConcurrentModificationError in an "except:"
      clause.

    .. change::
        :tags: orm
        :tickets: 1891

      Added a mutex to the identity map which mutexes
      remove operations against iteration methods,
      which now pre-buffer before returning an
      iterable.   This because asyncrhonous gc
      can remove items via the gc thread at any time.

    .. change::
        :tags: orm
        :tickets: 

      The Session class is now present in sqlalchemy.orm.*.
      We're moving away from the usage of create_session(),
      which has non-standard defaults, for those situations
      where a one-step Session constructor is desired. Most
      users should stick with sessionmaker() for general use,
      however.

    .. change::
        :tags: orm
        :tickets: 

      query.with_parent() now accepts transient objects
      and will use the non-persistent values of their pk/fk
      attributes in order to formulate the criterion.
      Docs are also clarified as to the purpose of with_parent().

    .. change::
        :tags: orm
        :tickets: 

      The include_properties and exclude_properties arguments
      to mapper() now accept Column objects as members in
      addition to strings.  This so that same-named Column
      objects, such as those within a join(), can be
      disambiguated.

    .. change::
        :tags: orm
        :tickets: 1896

      A warning is now emitted if a mapper is created against a
      join or other single selectable that includes multiple
      columns with the same name in its .c. collection,
      and those columns aren't explicitly named as part of
      the same or separate attributes (or excluded).
      In 0.7 this warning will be an exception.   Note that
      this warning is not emitted when the combination occurs
      as a result of inheritance, so that attributes
      still allow being overridden naturally..  In 0.7 this will be improved further.

    .. change::
        :tags: orm
        :tickets: 1896

      The primary_key argument to mapper() can now specify
      a series of columns that are only a subset of
      the calculated "primary key" columns of the mapped
      selectable, without an error being raised.  This
      helps for situations where a selectable's effective
      primary key is simpler than the number of columns
      in the selectable that are actually marked as
      "primary_key", such as a join against two
      tables on their primary key columns.

    .. change::
        :tags: orm
        :tickets: 

      An object that's been deleted now gets a flag
      'deleted', which prohibits the object from
      being re-add()ed to the session, as previously
      the object would live in the identity map
      silently until its attributes were accessed.
      The make_transient() function now resets this
      flag along with the "key" flag.

    .. change::
        :tags: orm
        :tickets: 

      make_transient() can be safely called on an
      already transient instance.

    .. change::
        :tags: orm
        :tickets: 

      a warning is emitted in mapper() if the polymorphic_on
      column is not present either in direct or derived
      form in the mapped selectable or in the
      with_polymorphic selectable, instead of silently
      ignoring it.  Look for this to become an
      exception in 0.7.

    .. change::
        :tags: orm
        :tickets: 

      Another pass through the series of error messages
      emitted when relationship() is configured with
      ambiguous arguments.   The "foreign_keys"
      setting is no longer mentioned, as it is almost
      never needed and it is preferable users set up
      correct ForeignKey metadata, which is now the
      recommendation.  If 'foreign_keys'
      is used and is incorrect, the message suggests
      the attribute is probably unnecessary.  Docs
      for the attribute are beefed up.  This
      because all confused relationship() users on the
      ML appear to be attempting to use foreign_keys
      due to the message, which only confuses them
      further since Table metadata is much clearer.

    .. change::
        :tags: orm
        :tickets: 1877

      If the "secondary" table has no ForeignKey metadata
      and no foreign_keys is set, even though the
      user is passing screwed up information, it is assumed
      that primary/secondaryjoin expressions should
      consider only and all cols in "secondary" to be
      foreign.  It's not possible with "secondary" for
      the foreign keys to be elsewhere in any case.
      A warning is now emitted instead of an error,
      and the mapping succeeds.

    .. change::
        :tags: orm
        :tickets: 1856

      Moving an o2m object from one collection to
      another, or vice versa changing the referenced
      object by an m2o, where the foreign key is also a
      member of the primary key, will now be more
      carefully checked during flush if the change in
      value of the foreign key on the "many" side is the
      result of a change in the primary key of the "one"
      side, or if the "one" is just a different object.
      In one case, a cascade-capable DB would have
      cascaded the value already and we need to look at
      the "new" PK value to do an UPDATE, in the other we
      need to continue looking at the "old". We now look
      at the "old", assuming passive_updates=True,
      unless we know it was a PK switch that
      triggered the change.

    .. change::
        :tags: orm
        :tickets: 1857

      The value of version_id_col can be changed
      manually, and this will result in an UPDATE
      of the row.  Versioned UPDATEs and DELETEs
      now use the "committed" value of the
      version_id_col in the WHERE clause and
      not the pending changed value. The
      version generator is also bypassed if
      manual changes are present on the attribute.

    .. change::
        :tags: orm
        :tickets: 

      Repaired the usage of merge() when used with
      concrete inheriting mappers.  Such mappers frequently
      have so-called "concrete" attributes, which are
      subclass attributes that "disable" propagation from
      the parent - these needed to allow a merge()
      operation to pass through without effect.

    .. change::
        :tags: orm
        :tickets: 1863

      Specifying a non-column based argument
      for column_mapped_collection, including string,
      text() etc., will raise an error message that
      specifically asks for a column element, no longer
      misleads with incorrect information about
      text() or literal().

    .. change::
        :tags: orm
        :tickets: 

      Similarly, for relationship(), foreign_keys,
      remote_side, order_by - all column-based
      expressions are enforced - lists of strings
      are explicitly disallowed since this is a
      very common error

    .. change::
        :tags: orm
        :tickets: 1864

      Dynamic attributes don't support collection
      population - added an assertion for when
      set_committed_value() is called, as well as
      when joinedload() or subqueryload() options
      are applied to a dynamic attribute, instead
      of failure / silent failure.

    .. change::
        :tags: orm
        :tickets: 1852

      Fixed bug whereby generating a Query derived
      from one which had the same column repeated
      with different label names, typically
      in some UNION situations, would fail to
      propagate the inner columns completely to
      the outer query.

    .. change::
        :tags: orm
        :tickets: 1881

      object_session() raises the proper
      UnmappedInstanceError when presented with an
      unmapped instance.

    .. change::
        :tags: orm
        :tickets: 

      Applied further memoizations to calculated Mapper
      properties, with significant (~90%) runtime mapper.py
      call count reduction in heavily polymorphic mapping
      configurations.

    .. change::
        :tags: orm
        :tickets: 

      mapper _get_col_to_prop private method used
      by the versioning example is deprecated;
      now use mapper.get_property_by_column() which
      will remain the public method for this.

    .. change::
        :tags: orm
        :tickets: 

      the versioning example works correctly now
      if versioning on a col that was formerly
      NULL.

    .. change::
        :tags: sql
        :tickets: 

      Calling execute() on an alias() construct is pending
      deprecation for 0.7, as it is not itself an
      "executable" construct. It currently "proxies" its
      inner element and is conditionally "executable" but
      this is not the kind of ambiguity we like these days.

    .. change::
        :tags: sql
        :tickets: 

      The execute() and scalar() methods of ClauseElement
      are now moved appropriately to the Executable
      subclass. ClauseElement.execute()/ scalar() are still
      present and are pending deprecation in 0.7, but note
      these would always raise an error anyway if you were
      not an Executable (unless you were an alias(), see
      previous note).

    .. change::
        :tags: sql
        :tickets: 

      Added basic math expression coercion for
      Numeric->Integer,
      so that resulting type is Numeric regardless
      of the direction of the expression.

    .. change::
        :tags: sql
        :tickets: 1855

      Changed the scheme used to generate truncated
      "auto" index names when using the "index=True"
      flag on Column.   The truncation only takes
      place with the auto-generated name, not one
      that is user-defined (an error would be
      raised instead), and the truncation scheme
      itself is now based on a fragment of an md5
      hash of the identifier name, so that multiple
      indexes on columns with similar names still
      have unique names.

    .. change::
        :tags: sql
        :tickets: 1412

      The generated index name also is based on
      a "max index name length" attribute which is
      separate from the "max identifier length" -
      this to appease MySQL who has a max length
      of 64 for index names, separate from their
      overall max length of 255.

    .. change::
        :tags: sql
        :tickets: 

      the text() construct, if placed in a column
      oriented situation, will at least return NULLTYPE
      for its type instead of None, allowing it to
      be used a little more freely for ad-hoc column
      expressions than before.   literal_column()
      is still the better choice, however.

    .. change::
        :tags: sql
        :tickets: 

      Added full description of parent table/column,
      target table/column in error message raised when
      ForeignKey can't resolve target.

    .. change::
        :tags: sql
        :tickets: 1865

      Fixed bug whereby replacing composite foreign key
      columns in a reflected table would cause an attempt
      to remove the reflected constraint from the table
      a second time, raising a KeyError.

    .. change::
        :tags: sql
        :tickets: 

      the _Label construct, i.e. the one that is produced
      whenever you say somecol.label(), now counts itself
      in its "proxy_set" unioned with that of its
      contained column's proxy set, instead of
      directly returning that of the contained column.
      This allows column correspondence
      operations which depend on the identity of the
      _Labels themselves to return the correct result

    .. change::
        :tags: sql
        :tickets: 1852

      fixes ORM bug.

    .. change::
        :tags: engine
        :tickets: 

      Calling fetchone() or similar on a result that
      has already been exhausted, has been closed,
      or is not a result-returning result now
      raises ResourceClosedError, a subclass of
      InvalidRequestError, in all cases, regardless
      of backend.  Previously, some DBAPIs would
      raise ProgrammingError (i.e. pysqlite), others
      would return None leading to downstream breakages
      (i.e. MySQL-python).

    .. change::
        :tags: engine
        :tickets: 1894

      Fixed bug in Connection whereby if a "disconnect"
      event occurred in the "initialize" phase of the
      first connection pool connect, an AttributeError
      would be raised when the Connection would attempt
      to invalidate the DBAPI connection.

    .. change::
        :tags: engine
        :tickets: 

      Connection, ResultProxy, as well as Session use
      ResourceClosedError for all "this
      connection/transaction/result is closed" types of
      errors.

    .. change::
        :tags: engine
        :tickets: 

      Connection.invalidate() can be called more than
      once and subsequent calls do nothing.

    .. change::
        :tags: declarative
        :tickets: 

      if @classproperty is used with a regular class-bound
      mapper property attribute, it will be called to get the
      actual attribute value during initialization. Currently,
      there's no advantage to using @classproperty on a column
      or relationship attribute of a declarative class that
      isn't a mixin - evaluation is at the same time as if
      @classproperty weren't used. But here we at least allow
      it to function as expected.

    .. change::
        :tags: declarative
        :tickets: 

      Fixed bug where "Can't add additional column" message
      would display the wrong name.

    .. change::
        :tags: postgresql
        :tickets: 

      Fixed the psycopg2 dialect to use its
      set_isolation_level() method instead of relying
      upon the base "SET SESSION ISOLATION" command,
      as psycopg2 resets the isolation level on each new
      transaction otherwise.

    .. change::
        :tags: mssql
        :tickets: 

      Fixed "default schema" query to work with
      pymssql backend.

    .. change::
        :tags: firebird
        :tickets: 

      Fixed bug whereby a column default would fail to
      reflect if the "default" keyword were lower case.

    .. change::
        :tags: oracle
        :tickets: 1879

      Added ROWID type to the Oracle dialect, for those
      cases where an explicit CAST might be needed.

    .. change::
        :tags: oracle
        :tickets: 1867

      Oracle reflection of indexes has been tuned so
      that indexes which include some or all primary
      key columns, but not the same set of columns
      as that of the primary key, are reflected.
      Indexes which contain the identical columns
      as that of the primary key are skipped within
      reflection, as the index in that case is assumed
      to be the auto-generated primary key index.
      Previously, any index with PK columns present
      would be skipped.  Thanks to Kent Bower
      for the patch.

    .. change::
        :tags: oracle
        :tickets: 1868

      Oracle now reflects the names of primary key
      constraints - also thanks to Kent Bower.

    .. change::
        :tags: informix
        :tickets: 1904

      Applied patches from to get
      basic Informix functionality up again.  We
      rely upon end-user testing to ensure that
      Informix is working to some degree.

    .. change::
        :tags: documentation
        :tickets: 

      The docs have been reorganized such that the "API
      Reference" section is gone - all the docstrings from
      there which were public API are moved into the
      context of the main doc section that talks about it.
      Main docs divided into "SQLAlchemy Core" and
      "SQLAlchemy ORM" sections, mapper/relationship docs
      have been broken out. Lots of sections rewritten
      and/or reorganized.

    .. change::
        :tags: examples
        :tickets: 

      The beaker_caching example has been reorganized
      such that the Session, cache manager,
      declarative_base are part of environment, and
      custom cache code is portable and now within
      "caching_query.py".  This allows the example to
      be easier to "drop in" to existing projects.

    .. change::
        :tags: examples
        :tickets: 1887

      the history_meta versioning recipe sets "unique=False"
      when copying columns, so that the versioning
      table handles multiple rows with repeating values.

.. changelog::
    :version: 0.6.3
    :released: Thu Jul 15 2010

    .. change::
        :tags: orm
        :tickets: 1845

      Removed errant many-to-many load in unitofwork
      which triggered unnecessarily on expired/unloaded
      collections. This load now takes place only if
      passive_updates is False and the parent primary
      key has changed, or if passive_deletes is False
      and a delete of the parent has occurred.

    .. change::
        :tags: orm
        :tickets: 1853

      Column-entities (i.e. query(Foo.id)) copy their
      state more fully when queries are derived from
      themselves + a selectable (i.e. from_self(),
      union(), etc.), so that join() and such have the
      correct state to work from.

    .. change::
        :tags: orm
        :tickets: 1853

      Fixed bug where Query.join() would fail if
      querying a non-ORM column then joining without
      an on clause when a FROM clause is already
      present, now raises a checked exception the
      same way it does when the clause is not
      present.

    .. change::
        :tags: orm
        :tickets: 1142

      Improved the check for an "unmapped class",
      including the case where the superclass is mapped
      but the subclass is not.  Any attempts to access
      cls._sa_class_manager.mapper now raise
      UnmappedClassError().

    .. change::
        :tags: orm
        :tickets: 

      Added "column_descriptions" accessor to Query,
      returns a list of dictionaries containing
      naming/typing information about the entities
      the Query will return.  Can be helpful for
      building GUIs on top of ORM queries.

    .. change::
        :tags: mysql
        :tickets: 1848

      The _extract_error_code() method now works
      correctly with each MySQL dialect (
      MySQL-python, OurSQL, MySQL-Connector-Python,
      PyODBC).  Previously,
      the reconnect logic would fail for OperationalError
      conditions, however since MySQLdb and OurSQL
      have their own reconnect feature, there was no
      symptom for these drivers here unless one
      watched the logs.

    .. change::
        :tags: oracle
        :tickets: 1840

      More tweaks to cx_oracle Decimal handling.
      "Ambiguous" numerics with no decimal place
      are coerced to int at the connection handler
      level.  The advantage here is that ints
      come back as ints without SQLA type
      objects being involved and without needless
      conversion to Decimal first.
      
      Unfortunately, some exotic subquery cases
      can even see different types between
      individual result rows, so the Numeric
      handler, when instructed to return Decimal,
      can't take full advantage of "native decimal"
      mode and must run isinstance() on every value
      to check if its Decimal already. Reopen of

.. changelog::
    :version: 0.6.2
    :released: Tue Jul 06 2010

    .. change::
        :tags: orm
        :tickets: 

      Query.join() will check for a call of the
      form query.join(target, clause_expression),
      i.e. missing the tuple, and raise an informative
      error message that this is the wrong calling form.

    .. change::
        :tags: orm
        :tickets: 1824

      Fixed bug regarding flushes on self-referential
      bi-directional many-to-many relationships, where
      two objects made to mutually reference each other
      in one flush would fail to insert a row for both
      sides.  Regression from 0.5.

    .. change::
        :tags: orm
        :tickets: 

      the post_update feature of relationship() has been
      reworked architecturally to integrate more closely
      with the new 0.6 unit of work.  The motivation
      for the change is so that multiple "post update"
      calls, each affecting different foreign key
      columns of the same row, are executed in a single
      UPDATE statement, rather than one UPDATE
      statement per column per row.   Multiple row
      updates are also batched into executemany()s as
      possible, while maintaining consistent row ordering.

    .. change::
        :tags: orm
        :tickets: 

      Query.statement, Query.subquery(), etc. now transfer
      the values of bind parameters, i.e. those specified
      by query.params(), into the resulting SQL expression.
      Previously the values would not be transferred
      and bind parameters would come out as None.

    .. change::
        :tags: orm
        :tickets: 

      Subquery-eager-loading now works with Query objects
      which include params(), as well as get() Queries.

    .. change::
        :tags: orm
        :tickets: 

      Can now call make_transient() on an instance that
      is referenced by parent objects via many-to-one,
      without the parent's foreign key value getting
      temporarily set to None - this was a function
      of the "detect primary key switch" flush handler.
      It now ignores objects that are no longer
      in the "persistent" state, and the parent's
      foreign key identifier is left unaffected.

    .. change::
        :tags: orm
        :tickets: 

      query.order_by() now accepts False, which cancels
      any existing order_by() state on the Query, allowing
      subsequent generative methods to be called which do
      not support ORDER BY.  This is not the same as the
      already existing feature of passing None, which
      suppresses any existing order_by() settings, including
      those configured on the mapper.  False will make it
      as though order_by() was never called, while
      None is an active setting.

    .. change::
        :tags: orm
        :tickets: 

      An instance which is moved to "transient", has
      an incomplete or missing set of primary key
      attributes, and contains expired attributes, will
      raise an InvalidRequestError if an expired attribute
      is accessed, instead of getting a recursion overflow.

    .. change::
        :tags: orm
        :tickets: 

      The make_transient() function is now in the generated
      documentation.

    .. change::
        :tags: orm
        :tickets: 

      make_transient() removes all "loader" callables from
      the state being made transient, removing any
      "expired" state - all unloaded attributes reset back
      to undefined, None/empty on access.

    .. change::
        :tags: sql
        :tickets: 1822

      The warning emitted by the Unicode and String types
      with convert_unicode=True no longer embeds the actual
      value passed.   This so that the Python warning
      registry does not continue to grow in size, the warning
      is emitted once as per the warning filter settings,
      and large string values don't pollute the output.

    .. change::
        :tags: sql
        :tickets: 

      Fixed bug that would prevent overridden clause
      compilation from working for "annotated" expression
      elements, which are often generated by the ORM.

    .. change::
        :tags: sql
        :tickets: 1400

      The argument to "ESCAPE" of a LIKE operator or similar
      is passed through render_literal_value(), which may
      implement escaping of backslashes.

    .. change::
        :tags: sql
        :tickets: 

      Fixed bug in Enum type which blew away native_enum
      flag when used with TypeDecorators or other adaption
      scenarios.

    .. change::
        :tags: sql
        :tickets: 

      Inspector hits bind.connect() when invoked to ensure
      initialize has been called.  the internal name ".conn"
      is changed to ".bind", since that's what it is.

    .. change::
        :tags: sql
        :tickets: 

      Modified the internals of "column annotation" such that
      a custom Column subclass can safely override
      _constructor to return Column, for the purposes of
      making "configurational" column classes that aren't
      involved in proxying, etc.

    .. change::
        :tags: sql
        :tickets: 1829

      Column.copy() takes along the "unique" attribute
      among others, fixes regarding declarative
      mixins

    .. change::
        :tags: postgresql
        :tickets: 1400

      render_literal_value() is overridden which escapes
      backslashes, currently applies to the ESCAPE clause
      of LIKE and similar expressions.
      Ultimately this will have to detect the value of
      "standard_conforming_strings" for full behavior.

    .. change::
        :tags: postgresql
        :tickets: 1836

      Won't generate "CREATE TYPE" / "DROP TYPE" if
      using types.Enum on a PG version prior to 8.3 -
      the supports_native_enum flag is fully
      honored.

    .. change::
        :tags: mysql
        :tickets: 1826

      MySQL dialect doesn't emit CAST() for MySQL version
      detected < 4.0.2.  This allows the unicode
      check on connect to proceed.

    .. change::
        :tags: mysql
        :tickets: 

      MySQL dialect now detects NO_BACKSLASH_ESCAPES sql
      mode, in addition to ANSI_QUOTES.

    .. change::
        :tags: mysql
        :tickets: 1400

      render_literal_value() is overridden which escapes
      backslashes, currently applies to the ESCAPE clause
      of LIKE and similar expressions.   This behavior
      is derived from detecting the value of
      NO_BACKSLASH_ESCAPES.

    .. change::
        :tags: oracle
        :tickets: 1819

      Fixed ora-8 compatibility flags such that they
      don't cache a stale value from before the first
      database connection actually occurs.

    .. change::
        :tags: oracle
        :tickets: 1840

      Oracle's "native decimal" metadata begins to return
      ambiguous typing information about numerics
      when columns are embedded in subqueries as well
      as when ROWNUM is consulted with subqueries, as we
      do for limit/offset.  We've added these ambiguous
      conditions to the cx_oracle "convert to Decimal()"
      handler, so that we receive numerics as Decimal
      in more cases instead of as floats.  These are
      then converted, if requested, into Integer
      or Float, or otherwise kept as the lossless
      Decimal.

    .. change::
        :tags: mssql
        :tickets: 1825

      If server_version_info is outside the usual
      range of (8, ), (9, ), (10, ), a warning is emitted
      which suggests checking that the FreeTDS version
      configuration is using 7.0 or 8.0, not 4.2.

    .. change::
        :tags: firebird
        :tickets: 1823

      Fixed incorrect signature in do_execute(), error
      introduced in 0.6.1.

    .. change::
        :tags: firebird
        :tickets: 1813

      Firebird dialect adds CHAR, VARCHAR types which
      accept a "charset" flag, to support Firebird
      "CHARACTER SET" clause.

    .. change::
        :tags: declarative
        :tickets: 1805, 1796, 1751

      Added support for @classproperty to provide
      any kind of schema/mapping construct from a
      declarative mixin, including columns with foreign
      keys, relationships, column_property, deferred.
      This solves all such issues on declarative mixins.
      An error is raised if any MapperProperty subclass
      is specified on a mixin without using @classproperty.

    .. change::
        :tags: declarative
        :tickets: 1821

      a mixin class can now define a column that matches
      one which is present on a __table__ defined on a
      subclass.  It cannot, however, define one that is
      not present in the __table__, and the error message
      here now works.

    .. change::
        :tags: extension, compiler
        :tickets: 1838

      The 'default' compiler is automatically copied over
      when overriding the compilation of a built in
      clause construct, so no KeyError is raised if the
      user-defined compiler is specific to certain
      backends and compilation for a different backend
      is invoked.

    .. change::
        :tags: documentation
        :tickets: 1820

      Added documentation for the Inspector.

    .. change::
        :tags: documentation
        :tickets: 1830

      Fixed @memoized_property and @memoized_instancemethod
      decorators so that Sphinx documentation picks up
      these attributes and methods, such as
      ResultProxy.inserted_primary_key.

.. changelog::
    :version: 0.6.1
    :released: Mon May 31 2010

    .. change::
        :tags: orm
        :tickets: 1782

      Fixed regression introduced in 0.6.0 involving improper
      history accounting on mutable attributes.

    .. change::
        :tags: orm
        :tickets: 1807

      Fixed regression introduced in 0.6.0 unit of work refactor
      that broke updates for bi-directional relationship()
      with post_update=True.

    .. change::
        :tags: orm
        :tickets: 1789

      session.merge() will not expire attributes on the returned
      instance if that instance is "pending".

    .. change::
        :tags: orm
        :tickets: 1802

      fixed __setstate__ method of CollectionAdapter to not
      fail during deserialize where parent InstanceState not
      yet unserialized.

    .. change::
        :tags: orm
        :tickets: 1797

      Added internal warning in case an instance without a
      full PK happened to be expired and then was asked
      to refresh.

    .. change::
        :tags: orm
        :tickets: 

      Added more aggressive caching to the mapper's usage of
      UPDATE, INSERT, and DELETE expressions.  Assuming the
      statement has no per-object SQL expressions attached,
      the expression objects are cached by the mapper after
      the first create, and their compiled form is stored
      persistently in a cache dictionary for the duration of
      the related Engine.  The cache is an LRUCache for the
      rare case that a mapper receives an extremely
      high number of different column patterns as UPDATEs.

    .. change::
        :tags: sql
        :tickets: 1793

      expr.in_() now accepts a text() construct as the argument.
      Grouping parenthesis are added automatically, i.e. usage
      is like `col.in_(text("select id from table"))`.

    .. change::
        :tags: sql
        :tickets: 

      Columns of _Binary type (i.e. LargeBinary, BLOB, etc.)
      will coerce a "basestring" on the right side into a
      _Binary as well so that required DBAPI processing
      takes place.

    .. change::
        :tags: sql
        :tickets: 1801

      Added table.add_is_dependent_on(othertable), allows manual
      placement of dependency rules between two Table objects
      for use within create_all(), drop_all(), sorted_tables.

    .. change::
        :tags: sql
        :tickets: 1778

      Fixed bug that prevented implicit RETURNING from functioning
      properly with composite primary key that contained zeroes.

    .. change::
        :tags: sql
        :tickets: 

      Fixed errant space character when generating ADD CONSTRAINT
      for a named UNIQUE constraint.

    .. change::
        :tags: sql
        :tickets: 1571

      Fixed "table" argument on constructor of ForeginKeyConstraint

    .. change::
        :tags: sql
        :tickets: 1786

      Fixed bug in connection pool cursor wrapper whereby if a
      cursor threw an exception on close(), the logging of the
      message would fail.

    .. change::
        :tags: sql
        :tickets: 

      the _make_proxy() method of ColumnClause and Column now use
      self.__class__ to determine the class of object to be returned
      instead of hardcoding to ColumnClause/Column, making it slightly
      easier to produce specific subclasses of these which work in
      alias/subquery situations.

    .. change::
        :tags: sql
        :tickets: 1798

      func.XXX() doesn't inadvertently resolve to non-Function
      classes (e.g. fixes func.text()).

    .. change::
        :tags: engines
        :tickets: 1781

      Fixed building the C extensions on Python 2.4.

    .. change::
        :tags: engines
        :tickets: 

      Pool classes will reuse the same "pool_logging_name" setting
      after a dispose() occurs.

    .. change::
        :tags: engines
        :tickets: 

      Engine gains an "execution_options" argument and
      update_execution_options() method, which will apply to
      all connections generated by this engine.

    .. change::
        :tags: mysql
        :tickets: 1794

      func.sysdate() emits "SYSDATE()", i.e. with the ending
      parenthesis, on MySQL.

    .. change::
        :tags: sqlite
        :tickets: 1812

      Fixed concatenation of constraints when "PRIMARY KEY"
      constraint gets moved to column level due to SQLite
      AUTOINCREMENT keyword being rendered.

    .. change::
        :tags: oracle
        :tickets: 1775

      Added a check for cx_oracle versions lower than version 5,
      in which case the incompatible "output type handler" won't
      be used.   This will impact decimal accuracy and some
      unicode handling issues.

    .. change::
        :tags: oracle
        :tickets: 1790

      Fixed use_ansi=False mode, which was producing broken
      WHERE clauses in pretty much all cases.

    .. change::
        :tags: oracle
        :tickets: 1808

      Re-established support for Oracle 8 with cx_oracle,
      including that use_ansi is set to False automatically,
      NVARCHAR2 and NCLOB are not rendered for Unicode,
      "native unicode" check doesn't fail, cx_oracle
      "native unicode" mode is disabled, VARCHAR() is emitted
      with bytes count instead of char count.

    .. change::
        :tags: oracle
        :tickets: 1670

      oracle_xe 5 doesn't accept a Python unicode object in
      its connect string in normal Python 2.x mode - so we coerce
      to str() directly.  non-ascii characters aren't supported
      in connect strings here since we don't know what encoding
      we could use.

    .. change::
        :tags: oracle
        :tickets: 1815

      FOR UPDATE is emitted in the syntactically correct position
      when limit/offset is used, i.e. the ROWNUM subquery.
      However, Oracle can't really handle FOR UPDATE with ORDER BY
      or with subqueries, so its still not very usable, but at
      least SQLA gets the SQL past the Oracle parser.

    .. change::
        :tags: firebird
        :tickets: 1521

      Added a label to the query used within has_table() and
      has_sequence() to work with older versions of Firebird
      that don't provide labels for result columns.

    .. change::
        :tags: firebird
        :tickets: 1779

      Added integer coercion to the "type_conv" attribute when
      passed via query string, so that it is properly interpreted
      by Kinterbasdb.

    .. change::
        :tags: firebird
        :tickets: 1646

      Added 'connection shutdown' to the list of exception strings
      which indicate a dropped connection.

    .. change::
        :tags: sqlsoup
        :tickets: 1783

      the SqlSoup constructor accepts a `base` argument which specifies
      the base class to use for mapped classes, the default being
      `object`.

.. changelog::
    :version: 0.6.0
    :released: Sun Apr 18 2010

    .. change::
        :tags: orm
        :tickets: 1742, 1081

      Unit of work internals have been rewritten.  Units of work
      with large numbers of objects interdependent objects
      can now be flushed without recursion overflows
      as there is no longer reliance upon recursive calls.  The number of internal structures now stays
      constant for a particular session state, regardless of
      how many relationships are present on mappings.  The flow
      of events now corresponds to a linear list of steps,
      generated by the mappers and relationships based on actual
      work to be done, filtered through a single topological sort
      for correct ordering.  Flush actions are assembled using
      far fewer steps and less memory.

    .. change::
        :tags: orm
        :tickets: 

      Along with the UOW rewrite, this also removes an issue
      introduced in 0.6beta3 regarding topological cycle detection
      for units of work with long dependency cycles.  We now use
      an algorithm written by Guido (thanks Guido!).

    .. change::
        :tags: orm
        :tickets: 1764

      one-to-many relationships now maintain a list of positive
      parent-child associations within the flush, preventing
      previous parents marked as deleted from cascading a
      delete or NULL foreign key set on those child objects,
      despite the end-user not removing the child from the old
      association.

    .. change::
        :tags: orm
        :tickets: 1495

      A collection lazy load will switch off default
      eagerloading on the reverse many-to-one side, since
      that loading is by definition unnecessary.

    .. change::
        :tags: orm
        :tickets: 

      Session.refresh() now does an equivalent expire()
      on the given instance first, so that the "refresh-expire"
      cascade is propagated.   Previously, refresh() was
      not affected in any way by the presence of "refresh-expire"
      cascade.   This is a change in behavior versus that
      of 0.6beta2, where the "lockmode" flag passed to refresh()
      would cause a version check to occur.  Since the instance
      is first expired, refresh() always upgrades the object
      to the most recent version.

    .. change::
        :tags: orm
        :tickets: 1754

      The 'refresh-expire' cascade, when reaching a pending object,
      will expunge the object if the cascade also includes
      "delete-orphan", or will simply detach it otherwise.

    .. change::
        :tags: orm
        :tickets: 1756

      id(obj) is no longer used internally within topological.py,
      as the sorting functions now require hashable objects
      only.

    .. change::
        :tags: orm
        :tickets: 

      The ORM will set the docstring of all generated descriptors
      to None by default.  This can be overridden using 'doc'
      (or if using Sphinx, attribute docstrings work too).

    .. change::
        :tags: orm
        :tickets: 

      Added kw argument 'doc' to all mapper property callables
      as well as Column().  Will assemble the string 'doc' as
      the '__doc__' attribute on the descriptor.

    .. change::
        :tags: orm
        :tickets: 1761

      Usage of version_id_col on a backend that supports
      cursor.rowcount for execute() but not executemany() now works
      when a delete is issued (already worked for saves, since those
      don't use executemany()). For a backend that doesn't support
      cursor.rowcount at all, a warning is emitted the same
      as with saves.

    .. change::
        :tags: orm
        :tickets: 

      The ORM now short-term caches the "compiled" form of
      insert() and update() constructs when flushing lists of
      objects of all the same class, thereby avoiding redundant
      compilation per individual INSERT/UPDATE within an
      individual flush() call.

    .. change::
        :tags: orm
        :tickets: 

      internal getattr(), setattr(), getcommitted() methods
      on ColumnProperty, CompositeProperty, RelationshipProperty
      have been underscored (i.e. are private), signature has
      changed.

    .. change::
        :tags: engines
        :tickets: 1757

      The C extension now also works with DBAPIs which use custom
      sequences as row (and not only tuples).

    .. change::
        :tags: sql
        :tickets: 1755

      Restored some bind-labeling logic from 0.5 which ensures
      that tables with column names that overlap another column
      of the form "<tablename>_<columnname>" won't produce
      errors if column._label is used as a bind name during
      an UPDATE.  Test coverage which wasn't present in 0.5
      has been added.

    .. change::
        :tags: sql
        :tickets: 1729

      somejoin.select(fold_equivalents=True) is no longer
      deprecated, and will eventually be rolled into a more
      comprehensive version of the feature for.

    .. change::
        :tags: sql
        :tickets: 1759

      the Numeric type raises an *enormous* warning when expected
      to convert floats to Decimal from a DBAPI that returns floats.
      This includes SQLite, Sybase, MS-SQL.

    .. change::
        :tags: sql
        :tickets: 

      Fixed an error in expression typing which caused an endless
      loop for expressions with two NULL types.

    .. change::
        :tags: sql
        :tickets: 

      Fixed bug in execution_options() feature whereby the existing
      Transaction and other state information from the parent
      connection would not be propagated to the sub-connection.

    .. change::
        :tags: sql
        :tickets: 

      Added new 'compiled_cache' execution option.  A dictionary
      where Compiled objects will be cached when the Connection
      compiles a clause expression into a dialect- and parameter-
      specific Compiled object.  It is the user's responsibility to
      manage the size of this dictionary, which will have keys
      corresponding to the dialect, clause element, the column
      names within the VALUES or SET clause of an INSERT or UPDATE,
      as well as the "batch" mode for an INSERT or UPDATE statement.

    .. change::
        :tags: sql
        :tickets: 1769

      Added get_pk_constraint() to reflection.Inspector, similar
      to get_primary_keys() except returns a dict that includes the
      name of the constraint, for supported backends (PG so far).

    .. change::
        :tags: sql
        :tickets: 1771

      Table.create() and Table.drop() no longer apply metadata-
      level create/drop events.

    .. change::
        :tags: ext
        :tickets: 

      the compiler extension now allows @compiles decorators
      on base classes that extend to child classes, @compiles
      decorators on child classes that aren't broken by a
      @compiles decorator on the base class.

    .. change::
        :tags: ext
        :tickets: 

      Declarative will raise an informative error message
      if a non-mapped class attribute is referenced in the
      string-based relationship() arguments.

    .. change::
        :tags: ext
        :tickets: 

      Further reworked the "mixin" logic in declarative to
      additionally allow __mapper_args__ as a @classproperty
      on a mixin, such as to dynamically assign polymorphic_identity.

    .. change::
        :tags: postgresql
        :tickets: 1071

      Postgresql now reflects sequence names associated with
      SERIAL columns correctly, after the name of the sequence
      has been changed.  Thanks to Kumar McMillan for the patch.

    .. change::
        :tags: postgresql
        :tickets: 

      Repaired missing import in psycopg2._PGNumeric type when
      unknown numeric is received.

    .. change::
        :tags: postgresql
        :tickets: 

      psycopg2/pg8000 dialects now aware of REAL[], FLOAT[],
      DOUBLE_PRECISION[], NUMERIC[] return types without
      raising an exception.

    .. change::
        :tags: postgresql
        :tickets: 1769

      Postgresql reflects the name of primary key constraints,
      if one exists.

    .. change::
        :tags: oracle
        :tickets: 

      Now using cx_oracle output converters so that the
      DBAPI returns natively the kinds of values we prefer:

    .. change::
        :tags: oracle
        :tickets: 1759

      NUMBER values with positive precision + scale convert
      to cx_oracle.STRING and then to Decimal.   This
      allows perfect precision for the Numeric type when
      using cx_oracle.

    .. change::
        :tags: oracle
        :tickets: 

      STRING/FIXED_CHAR now convert to unicode natively.
      SQLAlchemy's String types then don't need to
      apply any kind of conversions.

    .. change::
        :tags: firebird
        :tickets: 

      The functionality of result.rowcount can be disabled on a
      per-engine basis by setting 'enable_rowcount=False'
      on create_engine().  Normally, cursor.rowcount is called
      after any UPDATE or DELETE statement unconditionally,
      because the cursor is then closed and Firebird requires
      an open cursor in order to get a rowcount.  This
      call is slightly expensive however so it can be disabled.
      To re-enable on a per-execution basis, the
      'enable_rowcount=True' execution option may be used.

    .. change::
        :tags: examples
        :tickets: 

      Updated attribute_shard.py example to use a more robust
      method of searching a Query for binary expressions which
      compare columns against literal values.

.. changelog::
    :version: 0.6beta3
    :released: Sun Mar 28 2010

    .. change::
        :tags: orm
        :tickets: 1675

      Major feature: Added new "subquery" loading capability to
      relationship().   This is an eager loading option which
      generates a second SELECT for each collection represented
      in a query, across all parents at once.  The query
      re-issues the original end-user query wrapped in a subquery,
      applies joins out to the target collection, and loads
      all those collections fully in one result, similar to
      "joined" eager loading but using all inner joins and not
      re-fetching full parent rows repeatedly (as most DBAPIs seem
      to do, even if columns are skipped).   Subquery loading is
      available at mapper config level using "lazy='subquery'" and
      at the query options level using "subqueryload(props..)",
      "subqueryload_all(props...)".

    .. change::
        :tags: orm
        :tickets: 

      To accommodate the fact that there are now two kinds of eager
      loading available, the new names for eagerload() and
      eagerload_all() are joinedload() and joinedload_all().  The
      old names will remain as synonyms for the foreseeable future.

    .. change::
        :tags: orm
        :tickets: 

      The "lazy" flag on the relationship() function now accepts
      a string argument for all kinds of loading: "select", "joined",
      "subquery", "noload" and "dynamic", where the default is now
      "select".  The old values of True/
      False/None still retain their usual meanings and will remain
      as synonyms for the foreseeable future.

    .. change::
        :tags: orm
        :tickets: 921

      Added with_hint() method to Query() construct.  This calls
      directly down to select().with_hint() and also accepts
      entities as well as tables and aliases.  See with_hint() in the
      SQL section below.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug in Query whereby calling q.join(prop).from_self(...).
      join(prop) would fail to render the second join outside the
      subquery, when joining on the same criterion as was on the
      inside.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug in Query whereby the usage of aliased() constructs
      would fail if the underlying table (but not the actual alias)
      were referenced inside the subquery generated by
      q.from_self() or q.select_from().

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug which affected all eagerload() and similar options
      such that "remote" eager loads, i.e. eagerloads off of a lazy
      load such as query(A).options(eagerload(A.b, B.c))
      wouldn't eagerload anything, but using eagerload("b.c") would
      work fine.

    .. change::
        :tags: orm
        :tickets: 

      Query gains an add_columns(\*columns) method which is a multi-
      version of add_column(col).  add_column(col) is future
      deprecated.

    .. change::
        :tags: orm
        :tickets: 

      Query.join() will detect if the end result will be
      "FROM A JOIN A", and will raise an error if so.

    .. change::
        :tags: orm
        :tickets: 

      Query.join(Cls.propname, from_joinpoint=True) will check more
      carefully that "Cls" is compatible with the current joinpoint,
      and act the same way as Query.join("propname", from_joinpoint=True)
      in that regard.

    .. change::
        :tags: sql
        :tickets: 921

      Added with_hint() method to select() construct.  Specify
      a table/alias, hint text, and optional dialect name, and
      "hints" will be rendered in the appropriate place in the
      statement.  Works for Oracle, Sybase, MySQL.

    .. change::
        :tags: sql
        :tickets: 1747

      Fixed bug introduced in 0.6beta2 where column labels would
      render inside of column expressions already assigned a label.

    .. change::
        :tags: postgresql
        :tickets: 877

      The psycopg2 dialect will log NOTICE messages via the
      "sqlalchemy.dialects.postgresql" logger name.

    .. change::
        :tags: postgresql
        :tickets: 997

      the TIME and TIMESTAMP types are now available from the
      postgresql dialect directly, which add the PG-specific
      argument 'precision' to both.   'precision' and
      'timezone' are correctly reflected for both TIME and
      TIMEZONE types.

    .. change::
        :tags: mysql
        :tickets: 1752

      No longer guessing that TINYINT(1) should be BOOLEAN
      when reflecting - TINYINT(1) is returned.  Use Boolean/
      BOOLEAN in table definition to get boolean conversion
      behavior.

    .. change::
        :tags: oracle
        :tickets: 1744

      The Oracle dialect will issue VARCHAR type definitions
      using character counts, i.e. VARCHAR2(50 CHAR), so that
      the column is sized in terms of characters and not bytes.
      Column reflection of character types will also use
      ALL_TAB_COLUMNS.CHAR_LENGTH instead of
      ALL_TAB_COLUMNS.DATA_LENGTH.  Both of these behaviors take
      effect when the server version is 9 or higher - for
      version 8, the old behaviors are used.

    .. change::
        :tags: declarative
        :tickets: 1746

      Using a mixin won't break if the mixin implements an
      unpredictable __getattribute__(), i.e. Zope interfaces.

    .. change::
        :tags: declarative
        :tickets: 1749

      Using @classdecorator and similar on mixins to define
      __tablename__, __table_args__, etc. now works if
      the method references attributes on the ultimate
      subclass.

    .. change::
        :tags: declarative
        :tickets: 1751

      relationships and columns with foreign keys aren't
      allowed on declarative mixins, sorry.

    .. change::
        :tags: ext
        :tickets: 

      The sqlalchemy.orm.shard module now becomes an extension,
      sqlalchemy.ext.horizontal_shard.   The old import
      works with a deprecation warning.

.. changelog::
    :version: 0.6beta2
    :released: Sat Mar 20 2010

    .. change::
        :tags: py3k
        :tickets: 

      Improved the installation/test setup regarding Python 3,
      now that Distribute runs on Py3k.   distribute_setup.py
      is now included.  See README.py3k for Python 3 installation/
      testing instructions.

    .. change::
        :tags: orm
        :tickets: 1740

      The official name for the relation() function is now
      relationship(), to eliminate confusion over the relational
      algebra term.  relation() however will remain available
      in equal capacity for the foreseeable future.

    .. change::
        :tags: orm
        :tickets: 1692

      Added "version_id_generator" argument to Mapper, this is a
      callable that, given the current value of the "version_id_col",
      returns the next version number.  Can be used for alternate
      versioning schemes such as uuid, timestamps.

    .. change::
        :tags: orm
        :tickets: 

      added "lockmode" kw argument to Session.refresh(), will
      pass through the string value to Query the same as
      in with_lockmode(), will also do version check for a
      version_id_col-enabled mapping.

    .. change::
        :tags: orm
        :tickets: 1188

      Fixed bug whereby calling query(A).join(A.bs).add_entity(B)
      in a joined inheritance scenario would double-add B as a
      target and produce an invalid query.

    .. change::
        :tags: orm
        :tickets: 1674

      Fixed bug in session.rollback() which involved not removing
      formerly "pending" objects from the session before
      re-integrating "deleted" objects, typically occurred with
      natural primary keys. If there was a primary key conflict
      between them, the attach of the deleted would fail
      internally. The formerly "pending" objects are now expunged
      first.

    .. change::
        :tags: orm
        :tickets: 1719

      Removed a lot of logging that nobody really cares about,
      logging that remains will respond to live changes in the
      log level.  No significant overhead is added.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug in session.merge() which prevented dict-like
      collections from merging.

    .. change::
        :tags: orm
        :tickets: 

      session.merge() works with relations that specifically
      don't include "merge" in their cascade options - the target
      is ignored completely.

    .. change::
        :tags: orm
        :tickets: 1681

      session.merge() will not expire existing scalar attributes
      on an existing target if the target has a value for that
      attribute, even if the incoming merged doesn't have
      a value for the attribute.  This prevents unnecessary loads
      on existing items.  Will still mark the attr as expired
      if the destination doesn't have the attr, though, which
      fulfills some contracts of deferred cols.

    .. change::
        :tags: orm
        :tickets: 1680

      The "allow_null_pks" flag is now called "allow_partial_pks",
      defaults to True, acts like it did in 0.5 again.  Except,
      it also is implemented within merge() such that a SELECT
      won't be issued for an incoming instance with partially
      NULL primary key if the flag is False.

    .. change::
        :tags: orm
        :tickets: 1737

      Fixed bug in 0.6-reworked "many-to-one" optimizations
      such that a many-to-one that is against a non-primary key
      column on the remote table (i.e. foreign key against a
      UNIQUE column) will pull the "old" value in from the
      database during a change, since if it's in the session
      we will need it for proper history/backref accounting,
      and we can't pull from the local identity map on a
      non-primary key column.

    .. change::
        :tags: orm
        :tickets: 1731

      fixed internal error which would occur if calling has()
      or similar complex expression on a single-table inheritance
      relation().

    .. change::
        :tags: orm
        :tickets: 1688

      query.one() no longer applies LIMIT to the query, this to
      ensure that it fully counts all object identities present
      in the result, even in the case where joins may conceal
      multiple identities for two or more rows.  As a bonus,
      one() can now also be called with a query that issued
      from_statement() to start with since it no longer modifies
      the query.

    .. change::
        :tags: orm
        :tickets: 1727

      query.get() now returns None if queried for an identifier
      that is present in the identity map with a different class
      than the one requested, i.e. when using polymorphic loading.

    .. change::
        :tags: orm
        :tickets: 1706

      A major fix in query.join(), when the "on" clause is an
      attribute of an aliased() construct, but there is already
      an existing join made out to a compatible target, query properly
      joins to the right aliased() construct instead of sticking
      onto the right side of the existing join.

    .. change::
        :tags: orm
        :tickets: 1362

      Slight improvement to the fix for to not issue
      needless updates of the primary key column during a so-called
      "row switch" operation, i.e. add + delete of two objects
      with the same PK.

    .. change::
        :tags: orm
        :tickets: 

      Now uses sqlalchemy.orm.exc.DetachedInstanceError when an
      attribute load or refresh action fails due to object
      being detached from any Session.   UnboundExecutionError
      is specific to engines bound to sessions and statements.

    .. change::
        :tags: orm
        :tickets: 

      Query called in the context of an expression will render
      disambiguating labels in all cases.    Note that this does
      not apply to the existing .statement and .subquery()
      accessor/method, which still honors the .with_labels()
      setting that defaults to False.

    .. change::
        :tags: orm
        :tickets: 1676

      Query.union() retains disambiguating labels within the
      returned statement, thus avoiding various SQL composition
      errors which can result from column name conflicts.

    .. change::
        :tags: orm
        :tickets: 

      Fixed bug in attribute history that inadvertently invoked
      __eq__ on mapped instances.

    .. change::
        :tags: orm
        :tickets: 

      Some internal streamlining of object loading grants a
      small speedup for large results, estimates are around
      10-15%.   Gave the "state" internals a good solid
      cleanup with less complexity, datamembers,
      method calls, blank dictionary creates.

    .. change::
        :tags: orm
        :tickets: 1689

      Documentation clarification for query.delete()

    .. change::
        :tags: orm
        :tickets: 

      Fixed cascade bug in many-to-one relation() when attribute
      was set to None, introduced in r6711 (cascade deleted
      items into session during add()).

    .. change::
        :tags: orm
        :tickets: 1736

      Calling query.order_by() or query.distinct() before calling
      query.select_from(), query.with_polymorphic(), or
      query.from_statement() raises an exception now instead of
      silently dropping those criterion.

    .. change::
        :tags: orm
        :tickets: 1735

      query.scalar() now raises an exception if more than one
      row is returned.  All other behavior remains the same.

    .. change::
        :tags: orm
        :tickets: 1692

      Fixed bug which caused "row switch" logic, that is an
      INSERT and DELETE replaced by an UPDATE, to fail when
      version_id_col was in use.

    .. change::
        :tags: sql
        :tickets: 1714

      join() will now simulate a NATURAL JOIN by default.  Meaning,
      if the left side is a join, it will attempt to join the right
      side to the rightmost side of the left first, and not raise
      any exceptions about ambiguous join conditions if successful
      even if there are further join targets across the rest of
      the left.

    .. change::
        :tags: sql
        :tickets: 

      The most common result processors conversion function were
      moved to the new "processors" module.  Dialect authors are
      encouraged to use those functions whenever they correspond
      to their needs instead of implementing custom ones.

    .. change::
        :tags: sql
        :tickets: 1694, 1698

      SchemaType and subclasses Boolean, Enum are now serializable,
      including their ddl listener and other event callables.

    .. change::
        :tags: sql
        :tickets: 

      Some platforms will now interpret certain literal values
      as non-bind parameters, rendered literally into the SQL
      statement.   This to support strict SQL-92 rules that are
      enforced by some platforms including MS-SQL and Sybase.
      In this model, bind parameters aren't allowed in the
      columns clause of a SELECT, nor are certain ambiguous
      expressions like "?=?".  When this mode is enabled, the base
      compiler will render the binds as inline literals, but only across
      strings and numeric values.  Other types such as dates
      will raise an error, unless the dialect subclass defines
      a literal rendering function for those.  The bind parameter
      must have an embedded literal value already or an error
      is raised (i.e. won't work with straight bindparam('x')).
      Dialects can also expand upon the areas where binds are not
      accepted, such as within argument lists of functions
      (which don't work on MS-SQL when native SQL binding is used).

    .. change::
        :tags: sql
        :tickets: 

      Added "unicode_errors" parameter to String, Unicode, etc.
      Behaves like the 'errors' keyword argument to
      the standard library's string.decode() functions.   This flag
      requires that `convert_unicode` is set to `"force"` - otherwise,
      SQLAlchemy is not guaranteed to handle the task of unicode
      conversion.   Note that this flag adds significant performance
      overhead to row-fetching operations for backends that already
      return unicode objects natively (which most DBAPIs do).  This
      flag should only be used as an absolute last resort for reading
      strings from a column with varied or corrupted encodings,
      which only applies to databases that accept invalid encodings
      in the first place (i.e. MySQL. *not* PG, Sqlite, etc.)

    .. change::
        :tags: sql
        :tickets: 

      Added math negation operator support, -x.

    .. change::
        :tags: sql
        :tickets: 

      FunctionElement subclasses are now directly executable the
      same way any func.foo() construct is, with automatic
      SELECT being applied when passed to execute().

    .. change::
        :tags: sql
        :tickets: 

      The "type" and "bind" keyword arguments of a func.foo()
      construct are now local to "func." constructs and are
      not part of the FunctionElement base class, allowing
      a "type" to be handled in a custom constructor or
      class-level variable.

    .. change::
        :tags: sql
        :tickets: 

      Restored the keys() method to ResultProxy.

    .. change::
        :tags: sql
        :tickets: 1647, 1683

      The type/expression system now does a more complete job
      of determining the return type from an expression
      as well as the adaptation of the Python operator into
      a SQL operator, based on the full left/right/operator
      of the given expression.  In particular
      the date/time/interval system created for Postgresql
      EXTRACT in has now been generalized into
      the type system.   The previous behavior which often
      occurred of an expression "column + literal" forcing
      the type of "literal" to be the same as that of "column"
      will now usually not occur - the type of
      "literal" is first derived from the Python type of the
      literal, assuming standard native Python types + date
      types, before falling back to that of the known type
      on the other side of the expression.  If the
      "fallback" type is compatible (i.e. CHAR from String),
      the literal side will use that.  TypeDecorator
      types override this by default to coerce the "literal"
      side unconditionally, which can be changed by implementing
      the coerce_compared_value() method. Also part of.

    .. change::
        :tags: sql
        :tickets: 

      Made sqlalchemy.sql.expressions.Executable part of public
      API, used for any expression construct that can be sent to
      execute().  FunctionElement now inherits Executable so that
      it gains execution_options(), which are also propagated
      to the select() that's generated within execute().
      Executable in turn subclasses _Generative which marks
      any ClauseElement that supports the @_generative
      decorator - these may also become "public" for the benefit
      of the compiler extension at some point.

    .. change::
        :tags: sql
        :tickets: 1579

      A change to the solution for - an end-user
      defined bind parameter name that directly conflicts with
      a column-named bind generated directly from the SET or
      VALUES clause of an update/insert generates a compile error.
      This reduces call counts and eliminates some cases where
      undesirable name conflicts could still occur.

    .. change::
        :tags: sql
        :tickets: 1705

      Column() requires a type if it has no foreign keys (this is
      not new).  An error is now raised if a Column() has no type
      and no foreign keys.

    .. change::
        :tags: sql
        :tickets: 1717

      the "scale" argument of the Numeric() type is honored when
      coercing a returned floating point value into a string
      on its way to Decimal - this allows accuracy to function
      on SQLite, MySQL.

    .. change::
        :tags: sql
        :tickets: 

      the copy() method of Column now copies over uninitialized
      "on table attach" events.  Helps with the new declarative
      "mixin" capability.

    .. change::
        :tags: engines
        :tickets: 

      Added an optional C extension to speed up the sql layer by
      reimplementing RowProxy and the most common result processors.
      The actual speedups will depend heavily on your DBAPI and
      the mix of datatypes used in your tables, and can vary from
      a 30% improvement to more than 200%.  It also provides a modest
      (~15-20%) indirect improvement to ORM speed for large queries.
      Note that it is *not* built/installed by default.
      See README for installation instructions.

    .. change::
        :tags: engines
        :tickets: 

      the execution sequence pulls all rowcount/last inserted ID
      info from the cursor before commit() is called on the
      DBAPI connection in an "autocommit" scenario.  This helps
      mxodbc with rowcount and is probably a good idea overall.

    .. change::
        :tags: engines
        :tickets: 1719

      Opened up logging a bit such that isEnabledFor() is called
      more often, so that changes to the log level for engine/pool
      will be reflected on next connect.   This adds a small
      amount of method call overhead.  It's negligible and will make
      life a lot easier for all those situations when logging
      just happens to be configured after create_engine() is called.

    .. change::
        :tags: engines
        :tickets: 

      The assert_unicode flag is deprecated.  SQLAlchemy will raise
      a warning in all cases where it is asked to encode a non-unicode
      Python string, as well as when a Unicode or UnicodeType type
      is explicitly passed a bytestring.  The String type will do nothing
      for DBAPIs that already accept Python unicode objects.

    .. change::
        :tags: engines
        :tickets: 

      Bind parameters are sent as a tuple instead of a list. Some
      backend drivers will not accept bind parameters as a list.

    .. change::
        :tags: engines
        :tickets: 

      threadlocal engine wasn't properly closing the connection
      upon close() - fixed that.

    .. change::
        :tags: engines
        :tickets: 

      Transaction object doesn't rollback or commit if it isn't
      "active", allows more accurate nesting of begin/rollback/commit.

    .. change::
        :tags: engines
        :tickets: 

      Python unicode objects as binds result in the Unicode type,
      not string, thus eliminating a certain class of unicode errors
      on drivers that don't support unicode binds.

    .. change::
        :tags: engines
        :tickets: 1555

      Added "logging_name" argument to create_engine(), Pool() constructor
      as well as "pool_logging_name" argument to create_engine() which
      filters down to that of Pool.   Issues the given string name
      within the "name" field of logging messages instead of the default
      hex identifier string.

    .. change::
        :tags: engines
        :tickets: 

      The visit_pool() method of Dialect is removed, and replaced with
      on_connect().  This method returns a callable which receives
      the raw DBAPI connection after each one is created.   The callable
      is assembled into a first_connect/connect pool listener by the
      connection strategy if non-None.   Provides a simpler interface
      for dialects.

    .. change::
        :tags: engines
        :tickets: 1728

      StaticPool now initializes, disposes and recreates without
      opening a new connection - the connection is only opened when
      first requested. dispose() also works on AssertionPool now.

    .. change::
        :tags: ticket: 1673, metadata
        :tickets: 

      Added the ability to strip schema information when using
      "tometadata" by passing "schema=None" as an argument. If schema
      is not specified then the table's schema is retained.

    .. change::
        :tags: declarative
        :tickets: 

      DeclarativeMeta exclusively uses cls.__dict__ (not dict\_)
      as the source of class information; _as_declarative exclusively
      uses the  dict\_ passed to it as the source of class information
      (which when using DeclarativeMeta is cls.__dict__).  This should
      in theory make it easier for custom metaclasses to modify
      the state passed into _as_declarative.

    .. change::
        :tags: declarative
        :tickets: 1707

      declarative now accepts mixin classes directly, as a means
      to provide common functional and column-based elements on
      all subclasses, as well as a means to propagate a fixed
      set of __table_args__ or __mapper_args__ to subclasses.
      For custom combinations of __table_args__/__mapper_args__ from
      an inherited mixin to local, descriptors can now be used.
      New details are all up in the Declarative documentation.
      Thanks to Chris Withers for putting up with my strife
      on this.

    .. change::
        :tags: declarative
        :tickets: 1393

      the __mapper_args__ dict is copied when propagating to a subclass,
      and is taken straight off the class __dict__ to avoid any
      propagation from the parent.  mapper inheritance already
      propagates the things you want from the parent mapper.

    .. change::
        :tags: declarative
        :tickets: 1732

      An exception is raised when a single-table subclass specifies
      a column that is already present on the base class.

    .. change::
        :tags: mysql
        :tickets: 1655

      Fixed reflection bug whereby when COLLATE was present,
      nullable flag and server defaults would not be reflected.

    .. change::
        :tags: mysql
        :tickets: 

      Fixed reflection of TINYINT(1) "boolean" columns defined with
      integer flags like UNSIGNED.

    .. change::
        :tags: mysql
        :tickets: 1668

      Further fixes for the mysql-connector dialect.

    .. change::
        :tags: mysql
        :tickets: 1496

      Composite PK table on InnoDB where the "autoincrement" column
      isn't first will emit an explicit "KEY" phrase within
      CREATE TABLE thereby avoiding errors.

    .. change::
        :tags: mysql
        :tickets: 1634

      Added reflection/create table support for a wide range
      of MySQL keywords.

    .. change::
        :tags: mysql
        :tickets: 1580

      Fixed import error which could occur reflecting tables on
      a Windows host

    .. change::
        :tags: mssql
        :tickets: 

      Re-established support for the pymssql dialect.

    .. change::
        :tags: mssql
        :tickets: 

      Various fixes for implicit returning, reflection,
      etc. - the MS-SQL dialects aren't quite complete
      in 0.6 yet (but are close)

    .. change::
        :tags: mssql
        :tickets: 1710

      Added basic support for mxODBC.

    .. change::
        :tags: mssql
        :tickets: 

      Removed the text_as_varchar option.

    .. change::
        :tags: oracle
        :tickets: 

      "out" parameters require a type that is supported by
      cx_oracle.  An error will be raised if no cx_oracle
      type can be found.

    .. change::
        :tags: oracle
        :tickets: 

      Oracle 'DATE' now does not perform any result processing,
      as the DATE type in Oracle stores full date+time objects,
      that's what you'll get.  Note that the generic types.Date
      type *will* still call value.date() on incoming values,
      however.  When reflecting a table, the reflected type
      will be 'DATE'.

    .. change::
        :tags: oracle
        :tickets: 1670

      Added preliminary support for Oracle's WITH_UNICODE
      mode.  At the very least this establishes initial
      support for cx_Oracle with Python 3.  When WITH_UNICODE
      mode is used in Python 2.xx, a large and scary warning
      is emitted asking that the user seriously consider
      the usage of this difficult mode of operation.

    .. change::
        :tags: oracle
        :tickets: 1712

      The except_() method now renders as MINUS on Oracle,
      which is more or less equivalent on that platform.

    .. change::
        :tags: oracle
        :tickets: 651

      Added support for rendering and reflecting
      TIMESTAMP WITH TIME ZONE, i.e. TIMESTAMP(timezone=True).

    .. change::
        :tags: oracle
        :tickets: 

      Oracle INTERVAL type can now be reflected.

    .. change::
        :tags: sqlite
        :tickets: 1685

      Added "native_datetime=True" flag to create_engine().
      This will cause the DATE and TIMESTAMP types to skip
      all bind parameter and result row processing, under
      the assumption that PARSE_DECLTYPES has been enabled
      on the connection.  Note that this is not entirely
      compatible with the "func.current_date()", which
      will be returned as a string.

    .. change::
        :tags: sybase
        :tickets: 

      Implemented a preliminary working dialect for Sybase,
      with sub-implementations for Python-Sybase as well
      as Pyodbc.  Handles table
      creates/drops and basic round trip functionality.
      Does not yet include reflection or comprehensive
      support of unicode/special expressions/etc.

    .. change::
        :tags: examples
        :tickets: 

      Changed the beaker cache example a bit to have a separate
      RelationCache option for lazyload caching.  This object
      does a lookup among any number of potential attributes
      more efficiently by grouping several into a common structure.
      Both FromCache and RelationCache are simpler individually.

    .. change::
        :tags: documentation
        :tickets: 1700

      Major cleanup work in the docs to link class, function, and
      method names into the API docs.

.. changelog::
    :version: 0.6beta1
    :released: Wed Feb 03 2010

    .. change::
        :tags: release, major
        :tickets: 

      For the full set of feature descriptions, see
      http://www.sqlalchemy.org/trac/wiki/06Migration .
      This document is a work in progress.

    .. change::
        :tags: release, major
        :tickets: 

      All bug fixes and feature enhancements from the most
      recent 0.5 version and below are also included within 0.6.

    .. change::
        :tags: release, major
        :tickets: 

      Platforms targeted now include Python 2.4/2.5/2.6, Python
      3.1, Jython2.5.

    .. change::
        :tags: orm
        :tickets: 

      Changes to query.update() and query.delete():
        - the 'expire' option on query.update() has been renamed to
          'fetch', thus matching that of query.delete().
          'expire' is deprecated and issues a warning.
      
        - query.update() and query.delete() both default to
          'evaluate' for the synchronize strategy.
      
        - the 'synchronize' strategy for update() and delete()
          raises an error on failure. There is no implicit fallback
          onto "fetch". Failure of evaluation is based on the
          structure of criteria, so success/failure is deterministic
          based on code structure.

    .. change::
        :tags: orm
        :tickets: 1186, 1492, 1544

      Enhancements on many-to-one relations:
        - many-to-one relations now fire off a lazyload in fewer
          cases, including in most cases will not fetch the "old"
          value when a new one is replaced.
      
        - many-to-one relation to a joined-table subclass now uses
          get() for a simple load (known as the "use_get"
          condition), i.e. Related->Sub(Base), without the need to
          redefine the primaryjoin condition in terms of the base
          table.
      
        - specifying a foreign key with a declarative column, i.e.
          ForeignKey(MyRelatedClass.id) doesn't break the "use_get"
          condition from taking place
      
        - relation(), eagerload(), and eagerload_all() now feature
          an option called "innerjoin". Specify `True` or `False` to
          control whether an eager join is constructed as an INNER
          or OUTER join. Default is `False` as always. The mapper
          options will override whichever setting is specified on
          relation(). Should generally be set for many-to-one, not
          nullable foreign key relations to allow improved join
          performance.
      
        - the behavior of eagerloading such that the main query is
          wrapped in a subquery when LIMIT/OFFSET are present now
          makes an exception for the case when all eager loads are
          many-to-one joins. In those cases, the eager joins are
          against the parent table directly along with the
          limit/offset without the extra overhead of a subquery,
          since a many-to-one join does not add rows to the result.

    .. change::
        :tags: orm
        :tickets: 

      Enhancements / Changes on Session.merge():

    .. change::
        :tags: orm
        :tickets: 

      the "dont_load=True" flag on Session.merge() is deprecated
      and is now "load=False".

    .. change::
        :tags: orm
        :tickets: 

      Session.merge() is performance optimized, using half the
      call counts for "load=False" mode compared to 0.5 and
      significantly fewer SQL queries in the case of collections
      for "load=True" mode.

    .. change::
        :tags: orm
        :tickets: 

      merge() will not issue a needless merge of attributes if the
      given instance is the same instance which is already present.

    .. change::
        :tags: orm
        :tickets: 

      merge() now also merges the "options" associated with a given
      state, i.e. those passed through query.options() which follow
      along with an instance, such as options to eagerly- or
      lazyily- load various attributes.   This is essential for
      the construction of highly integrated caching schemes.  This
      is a subtle behavioral change vs. 0.5.

    .. change::
        :tags: orm
        :tickets: 

      A bug was fixed regarding the serialization of the "loader
      path" present on an instance's state, which is also necessary
      when combining the usage of merge() with serialized state
      and associated options that should be preserved.

    .. change::
        :tags: orm
        :tickets: 

      The all new merge() is showcased in a new comprehensive
      example of how to integrate Beaker with SQLAlchemy.  See
      the notes in the "examples" note below.

    .. change::
        :tags: orm
        :tickets: 1362

      Primary key values can now be changed on a joined-table inheritance
      object, and ON UPDATE CASCADE will be taken into account when
      the flush happens.  Set the new "passive_updates" flag to False
      on mapper() when using SQLite or MySQL/MyISAM.

    .. change::
        :tags: orm
        :tickets: 1671

      flush() now detects when a primary key column was updated by
      an ON UPDATE CASCADE operation from another primary key, and
      can then locate the row for a subsequent UPDATE on the new PK
      value.  This occurs when a relation() is there to establish
      the relationship as well as passive_updates=True.

    .. change::
        :tags: orm
        :tickets: 

      the "save-update" cascade will now cascade the pending *removed*
      values from a scalar or collection attribute into the new session
      during an add() operation.  This so that the flush() operation
      will also delete or modify rows of those disconnected items.

    .. change::
        :tags: orm
        :tickets: 1531

      Using a "dynamic" loader with a "secondary" table now produces
      a query where the "secondary" table is *not* aliased.  This
      allows the secondary Table object to be used in the "order_by"
      attribute of the relation(), and also allows it to be used
      in filter criterion against the dynamic relation.

    .. change::
        :tags: orm
        :tickets: 1643

      relation() with uselist=False will emit a warning when
      an eager or lazy load locates more than one valid value for
      the row.  This may be due to primaryjoin/secondaryjoin
      conditions which aren't appropriate for an eager LEFT OUTER
      JOIN or for other conditions.

    .. change::
        :tags: orm
        :tickets: 1633

      an explicit check occurs when a synonym() is used with
      map_column=True, when a ColumnProperty (deferred or otherwise)
      exists separately in the properties dictionary sent to mapper
      with the same keyname.   Instead of silently replacing
      the existing property (and possible options on that property),
      an error is raised.

    .. change::
        :tags: orm
        :tickets: 

      a "dynamic" loader sets up its query criterion at construction
      time so that the actual query is returned from non-cloning
      accessors like "statement".

    .. change::
        :tags: orm
        :tickets: 

      the "named tuple" objects returned when iterating a
      Query() are now pickleable.

    .. change::
        :tags: orm
        :tickets: 1542

      mapping to a select() construct now requires that you
      make an alias() out of it distinctly.   This to eliminate
      confusion over such issues as

    .. change::
        :tags: orm
        :tickets: 1537

      query.join() has been reworked to provide more consistent
      behavior and more flexibility (includes)

    .. change::
        :tags: orm
        :tickets: 

      query.select_from() accepts multiple clauses to produce
      multiple comma separated entries within the FROM clause.
      Useful when selecting from multiple-homed join() clauses.

    .. change::
        :tags: orm
        :tickets: 

      query.select_from() also accepts mapped classes, aliased()
      constructs, and mappers as arguments.  In particular this
      helps when querying from multiple joined-table classes to ensure
      the full join gets rendered.

    .. change::
        :tags: orm
        :tickets: 1135

      query.get() can be used with a mapping to an outer join
      where one or more of the primary key values are None.

    .. change::
        :tags: orm
        :tickets: 1568

      query.from_self(), query.union(), others which do a
      "SELECT * from (SELECT...)" type of nesting will do
      a better job translating column expressions within the subquery
      to the columns clause of the outer query.  This is
      potentially backwards incompatible with 0.5, in that this
      may break queries with literal expressions that do not have labels
      applied (i.e. literal('foo'), etc.)

    .. change::
        :tags: orm
        :tickets: 1622

      relation primaryjoin and secondaryjoin now check that they
      are column-expressions, not just clause elements.  this prohibits
      things like FROM expressions being placed there directly.

    .. change::
        :tags: orm
        :tickets: 1415

      `expression.null()` is fully understood the same way
      None is when comparing an object/collection-referencing
      attribute within query.filter(), filter_by(), etc.

    .. change::
        :tags: orm
        :tickets: 1052

      added "make_transient()" helper function which transforms a
      persistent/ detached instance into a transient one (i.e.
      deletes the instance_key and removes from any session.)

    .. change::
        :tags: orm
        :tickets: 1339

      the allow_null_pks flag on mapper() is deprecated, and
      the feature is turned "on" by default.  This means that
      a row which has a non-null value for any of its primary key
      columns will be considered an identity.  The need for this
      scenario typically only occurs when mapping to an outer join.

    .. change::
        :tags: orm
        :tickets: 

      the mechanics of "backref" have been fully merged into the
      finer grained "back_populates" system, and take place entirely
      within the _generate_backref() method of RelationProperty.  This
      makes the initialization procedure of RelationProperty
      simpler and allows easier propagation of settings (such as from
      subclasses of RelationProperty) into the reverse reference.
      The internal BackRef() is gone and backref() returns a plain
      tuple that is understood by RelationProperty.

    .. change::
        :tags: orm
        :tickets: 1569

      The version_id_col feature on mapper() will raise a warning when
      used with dialects that don't support "rowcount" adequately.

    .. change::
        :tags: orm
        :tickets: 

      added "execution_options()" to Query, to so options can be
      passed to the resulting statement. Currently only
      Select-statements have these options, and the only option
      used is "stream_results", and the only dialect which knows
      "stream_results" is psycopg2.

    .. change::
        :tags: orm
        :tickets: 

      Query.yield_per() will set the "stream_results" statement
      option automatically.

    .. change::
        :tags: orm
        :tickets: 

      Deprecated or removed:
       * 'allow_null_pks' flag on mapper() is deprecated.  It does
         nothing now and the setting is "on" in all cases.
       * 'transactional' flag on sessionmaker() and others is
         removed. Use 'autocommit=True' to indicate 'transactional=False'.
       * 'polymorphic_fetch' argument on mapper() is removed.
         Loading can be controlled using the 'with_polymorphic'
         option.
       * 'select_table' argument on mapper() is removed.  Use
         'with_polymorphic=("*", <some selectable>)' for this
         functionality.
       * 'proxy' argument on synonym() is removed.  This flag
         did nothing throughout 0.5, as the "proxy generation"
         behavior is now automatic.
       * Passing a single list of elements to eagerload(),
         eagerload_all(), contains_eager(), lazyload(),
         defer(), and undefer() instead of multiple positional
         \*args is deprecated.
       * Passing a single list of elements to query.order_by(),
         query.group_by(), query.join(), or query.outerjoin()
         instead of multiple positional \*args is deprecated.
       * query.iterate_instances() is removed.  Use query.instances().
       * Query.query_from_parent() is removed.  Use the
         sqlalchemy.orm.with_parent() function to produce a
         "parent" clause, or alternatively query.with_parent().
       * query._from_self() is removed, use query.from_self()
         instead.
       * the "comparator" argument to composite() is removed.
         Use "comparator_factory".
       * RelationProperty._get_join() is removed.
       * the 'echo_uow' flag on Session is removed.  Use
         logging on the "sqlalchemy.orm.unitofwork" name.
       * session.clear() is removed.  use session.expunge_all().
       * session.save(), session.update(), session.save_or_update()
         are removed.  Use session.add() and session.add_all().
       * the "objects" flag on session.flush() remains deprecated.
       * the "dont_load=True" flag on session.merge() is deprecated
         in favor of "load=False".
       * ScopedSession.mapper remains deprecated.  See the
         usage recipe at
         http://www.sqlalchemy.org/trac/wiki/UsageRecipes/SessionAwareMapper
       * passing an InstanceState (internal SQLAlchemy state object) to
         attributes.init_collection() or attributes.get_history() is
         deprecated.  These functions are public API and normally
         expect a regular mapped object instance.
       * the 'engine' parameter to declarative_base() is removed.
         Use the 'bind' keyword argument.

    .. change::
        :tags: sql
        :tickets: 

      the "autocommit" flag on select() and text() as well
      as select().autocommit() are deprecated - now call
      .execution_options(autocommit=True) on either of those
      constructs, also available directly on Connection and orm.Query.

    .. change::
        :tags: sql
        :tickets: 

      the autoincrement flag on column now indicates the column
      which should be linked to cursor.lastrowid, if that method
      is used.  See the API docs for details.

    .. change::
        :tags: sql
        :tickets: 1566

      an executemany() now requires that all bound parameter
      sets require that all keys are present which are
      present in the first bound parameter set.  The structure
      and behavior of an insert/update statement is very much
      determined by the first parameter set, including which
      defaults are going to fire off, and a minimum of
      guesswork is performed with all the rest so that performance
      is not impacted.  For this reason defaults would otherwise
      silently "fail" for missing parameters, so this is now guarded
      against.

    .. change::
        :tags: sql
        :tickets: 

      returning() support is native to insert(), update(),
      delete(). Implementations of varying levels of
      functionality exist for Postgresql, Firebird, MSSQL and
      Oracle. returning() can be called explicitly with column
      expressions which are then returned in the resultset,
      usually via fetchone() or first().
      
      insert() constructs will also use RETURNING implicitly to
      get newly generated primary key values, if the database
      version in use supports it (a version number check is
      performed). This occurs if no end-user returning() was
      specified.

    .. change::
        :tags: sql
        :tickets: 1665

      union(), intersect(), except() and other "compound" types
      of statements have more consistent behavior w.r.t.
      parenthesizing.   Each compound element embedded within
      another will now be grouped with parenthesis - previously,
      the first compound element in the list would not be grouped,
      as SQLite doesn't like a statement to start with
      parenthesis.   However, Postgresql in particular has
      precedence rules regarding INTERSECT, and it is
      more consistent for parenthesis to be applied equally
      to all sub-elements.   So now, the workaround for SQLite
      is also what the workaround for PG was previously -
      when nesting compound elements, the first one usually needs
      ".alias().select()" called on it to wrap it inside
      of a subquery.

    .. change::
        :tags: sql
        :tickets: 1579

      insert() and update() constructs can now embed bindparam()
      objects using names that match the keys of columns.  These
      bind parameters will circumvent the usual route to those
      keys showing up in the VALUES or SET clause of the generated
      SQL.

    .. change::
        :tags: sql
        :tickets: 1524

      the Binary type now returns data as a Python string
      (or a "bytes" type in Python 3), instead of the built-
      in "buffer" type.  This allows symmetric round trips
      of binary data.

    .. change::
        :tags: sql
        :tickets: 

      Added a tuple_() construct, allows sets of expressions
      to be compared to another set, typically with IN against
      composite primary keys or similar.  Also accepts an
      IN with multiple columns.   The "scalar select can
      have only one column" error message is removed - will
      rely upon the database to report problems with
      col mismatch.

    .. change::
        :tags: sql
        :tickets: 

      User-defined "default" and "onupdate" callables which
      accept a context should now call upon
      "context.current_parameters" to get at the dictionary
      of bind parameters currently being processed.  This
      dict is available in the same way regardless of
      single-execute or executemany-style statement execution.

    .. change::
        :tags: sql
        :tickets: 1428

      multi-part schema names, i.e. with dots such as
      "dbo.master", are now rendered in select() labels
      with underscores for dots, i.e. "dbo_master_table_column".
      This is a "friendly" label that behaves better
      in result sets.

    .. change::
        :tags: sql
        :tickets: 

      removed needless "counter" behavior with select()
      labelnames that match a column name in the table,
      i.e. generates "tablename_id" for "id", instead of
      "tablename_id_1" in an attempt to avoid naming
      conflicts, when the table has a column actually
      named "tablename_id" - this is because
      the labeling logic is always applied to all columns
      so a naming conflict will never occur.

    .. change::
        :tags: sql
        :tickets: 1628

      calling expr.in_([]), i.e. with an empty list, emits a warning
      before issuing the usual "expr != expr" clause.  The
      "expr != expr" can be very expensive, and it's preferred
      that the user not issue in_() if the list is empty,
      instead simply not querying, or modifying the criterion
      as appropriate for more complex situations.

    .. change::
        :tags: sql
        :tickets: 

      Added "execution_options()" to select()/text(), which set the
      default options for the Connection.  See the note in "engines".

    .. change::
        :tags: sql
        :tickets: 1131

      Deprecated or removed:
        * "scalar" flag on select() is removed, use
          select.as_scalar().
        * "shortname" attribute on bindparam() is removed.
        * postgres_returning, firebird_returning flags on
          insert(), update(), delete() are deprecated, use
          the new returning() method.
        * fold_equivalents flag on join is deprecated (will remain
          until is implemented)

    .. change::
        :tags: engines
        :tickets: 443

      transaction isolation level may be specified with
      create_engine(... isolation_level="..."); available on
      postgresql and sqlite.

    .. change::
        :tags: engines
        :tickets: 

      Connection has execution_options(), generative method
      which accepts keywords that affect how the statement
      is executed w.r.t. the DBAPI.   Currently supports
      "stream_results", causes psycopg2 to use a server
      side cursor for that statement, as well as
      "autocommit", which is the new location for the "autocommit"
      option from select() and text().   select() and
      text() also have .execution_options() as well as
      ORM Query().

    .. change::
        :tags: engines
        :tickets: 1630

      fixed the import for entrypoint-driven dialects to
      not rely upon silly tb_info trick to determine import
      error status.

    .. change::
        :tags: engines
        :tickets: 

      added first() method to ResultProxy, returns first row and
      closes result set immediately.

    .. change::
        :tags: engines
        :tickets: 

      RowProxy objects are now pickleable, i.e. the object returned
      by result.fetchone(), result.fetchall() etc.

    .. change::
        :tags: engines
        :tickets: 

      RowProxy no longer has a close() method, as the row no longer
      maintains a reference to the parent.  Call close() on
      the parent ResultProxy instead, or use autoclose.

    .. change::
        :tags: engines
        :tickets: 1586

      ResultProxy internals have been overhauled to greatly reduce
      method call counts when fetching columns.  Can provide a large
      speed improvement (up to more than 100%) when fetching large
      result sets.  The improvement is larger when fetching columns
      that have no type-level processing applied and when using
      results as tuples (instead of as dictionaries).  Many
      thanks to Elixir's Gaëtan de Menten for this dramatic
      improvement !

    .. change::
        :tags: engines
        :tickets: 

      Databases which rely upon postfetch of "last inserted id"
      to get at a generated sequence value (i.e. MySQL, MS-SQL)
      now work correctly when there is a composite primary key
      where the "autoincrement" column is not the first primary
      key column in the table.

    .. change::
        :tags: engines
        :tickets: 

      the last_inserted_ids() method has been renamed to the
      descriptor "inserted_primary_key".

    .. change::
        :tags: engines
        :tickets: 1554

      setting echo=False on create_engine() now sets the loglevel
      to WARN instead of NOTSET.  This so that logging can be
      disabled for a particular engine even if logging
      for "sqlalchemy.engine" is enabled overall.  Note that the
      default setting of "echo" is `None`.

    .. change::
        :tags: engines
        :tickets: 

      ConnectionProxy now has wrapper methods for all transaction
      lifecycle events, including begin(), rollback(), commit()
      begin_nested(), begin_prepared(), prepare(), release_savepoint(),
      etc.

    .. change::
        :tags: engines
        :tickets: 

      Connection pool logging now uses both INFO and DEBUG
      log levels for logging.  INFO is for major events such
      as invalidated connections, DEBUG for all the acquire/return
      logging.  `echo_pool` can be False, None, True or "debug"
      the same way as `echo` works.

    .. change::
        :tags: engines
        :tickets: 1621

      All pyodbc-dialects now support extra pyodbc-specific
      kw arguments 'ansi', 'unicode_results', 'autocommit'.

    .. change::
        :tags: engines
        :tickets: 

      the "threadlocal" engine has been rewritten and simplified
      and now supports SAVEPOINT operations.

    .. change::
        :tags: engines
        :tickets: 

      deprecated or removed
        * result.last_inserted_ids() is deprecated.  Use
          result.inserted_primary_key
        * dialect.get_default_schema_name(connection) is now
          public via dialect.default_schema_name.
        * the "connection" argument from engine.transaction() and
          engine.run_callable() is removed - Connection itself
          now has those methods.   All four methods accept
          \*args and \**kwargs which are passed to the given callable,
          as well as the operating connection.

    .. change::
        :tags: schema
        :tickets: 1541

      the `__contains__()` method of `MetaData` now accepts
      strings or `Table` objects as arguments.  If given
      a `Table`, the argument is converted to `table.key` first,
      i.e. "[schemaname.]<tablename>"

    .. change::
        :tags: schema
        :tickets: 

      deprecated MetaData.connect() and
      ThreadLocalMetaData.connect() have been removed - send
      the "bind" attribute to bind a metadata.

    .. change::
        :tags: schema
        :tickets: 

      deprecated metadata.table_iterator() method removed (use
      sorted_tables)

    .. change::
        :tags: schema
        :tickets: 

      deprecated PassiveDefault - use DefaultClause.

    .. change::
        :tags: schema
        :tickets: 

      the "metadata" argument is removed from DefaultGenerator
      and subclasses, but remains locally present on Sequence,
      which is a standalone construct in DDL.

    .. change::
        :tags: schema
        :tickets: 

      Removed public mutability from Index and Constraint
      objects:

        * ForeignKeyConstraint.append_element()
        * Index.append_column()
        * UniqueConstraint.append_column()
        * PrimaryKeyConstraint.add()
        * PrimaryKeyConstraint.remove()

      These should be constructed declaratively (i.e. in one
      construction).

    .. change::
        :tags: schema
        :tickets: 1545

      The "start" and "increment" attributes on Sequence now
      generate "START WITH" and "INCREMENT BY" by default,
      on Oracle and Postgresql.  Firebird doesn't support
      these keywords right now.

    .. change::
        :tags: schema
        :tickets: 

      UniqueConstraint, Index, PrimaryKeyConstraint all accept
      lists of column names or column objects as arguments.

    .. change::
        :tags: schema
        :tickets: 

      Other removed things:
        - Table.key (no idea what this was for)
        - Table.primary_key is not assignable - use
          table.append_constraint(PrimaryKeyConstraint(...))
        - Column.bind       (get via column.table.bind)
        - Column.metadata   (get via column.table.metadata)
        - Column.sequence   (use column.default)
        - ForeignKey(constraint=some_parent) (is now private _constraint)

    .. change::
        :tags: schema
        :tickets: 

      The use_alter flag on ForeignKey is now a shortcut option
      for operations that can be hand-constructed using the
      DDL() event system. A side effect of this refactor is
      that ForeignKeyConstraint objects with use_alter=True
      will *not* be emitted on SQLite, which does not support
      ALTER for foreign keys.

    .. change::
        :tags: schema
        :tickets: 1605

      ForeignKey and ForeignKeyConstraint objects now correctly
      copy() all their public keyword arguments.

    .. change::
        :tags: reflection/inspection
        :tickets: 

      Table reflection has been expanded and generalized into
      a new API called "sqlalchemy.engine.reflection.Inspector".
      The Inspector object provides fine-grained information about
      a wide variety of schema information, with room for expansion,
      including table names, column names, view definitions, sequences,
      indexes, etc.

    .. change::
        :tags: reflection/inspection
        :tickets: 

      Views are now reflectable as ordinary Table objects.  The same
      Table constructor is used, with the caveat that "effective"
      primary and foreign key constraints aren't part of the reflection
      results; these have to be specified explicitly if desired.

    .. change::
        :tags: reflection/inspection
        :tickets: 

      The existing autoload=True system now uses Inspector underneath
      so that each dialect need only return "raw" data about tables
      and other objects - Inspector is the single place that information
      is compiled into Table objects so that consistency is at a maximum.

    .. change::
        :tags: ddl
        :tickets: 

      the DDL system has been greatly expanded.  the DDL() class
      now extends the more generic DDLElement(), which forms the basis
      of many new constructs:
      
        - CreateTable()
        - DropTable()
        - AddConstraint()
        - DropConstraint()
        - CreateIndex()
        - DropIndex()
        - CreateSequence()
        - DropSequence()
      
       These support "on" and "execute-at()" just like plain DDL()
       does.  User-defined DDLElement subclasses can be created and
       linked to a compiler using the sqlalchemy.ext.compiler extension.

    .. change::
        :tags: ddl
        :tickets: 

      The signature of the "on" callable passed to DDL() and
      DDLElement() is revised as follows:
      
        ddl
            the DDLElement object itself
        event
            the string event name.
        target
            previously "schema_item", the Table or MetaData object triggering the event.
        connection
            the Connection object in use for the operation.
        \**kw
            keyword arguments.  In the case of MetaData before/after
            create/drop, the list of Table objects for which
            CREATE/DROP DDL is to be issued is passed as the kw
            argument "tables". This is necessary for metadata-level
            DDL that is dependent on the presence of specific tables.
      
      The "schema_item" attribute of DDL has been renamed to
        "target".

    .. change::
        :tags: dialect, refactor
        :tickets: 

      Dialect modules are now broken into database dialects
      plus DBAPI implementations. Connect URLs are now
      preferred to be specified using dialect+driver://...,
      i.e. "mysql+mysqldb://scott:tiger@localhost/test". See
      the 0.6 documentation for examples.

    .. change::
        :tags: dialect, refactor
        :tickets: 

      the setuptools entrypoint for external dialects is now
      called "sqlalchemy.dialects".

    .. change::
        :tags: dialect, refactor
        :tickets: 

      the "owner" keyword argument is removed from Table. Use
      "schema" to represent any namespaces to be prepended to
      the table name.

    .. change::
        :tags: dialect, refactor
        :tickets: 

      server_version_info becomes a static attribute.

    .. change::
        :tags: dialect, refactor
        :tickets: 

      dialects receive an initialize() event on initial
      connection to determine connection properties.

    .. change::
        :tags: dialect, refactor
        :tickets: 

      dialects receive a visit_pool event have an opportunity
      to establish pool listeners.

    .. change::
        :tags: dialect, refactor
        :tickets: 

      cached TypeEngine classes are cached per-dialect class
      instead of per-dialect.

    .. change::
        :tags: dialect, refactor
        :tickets: 

      new UserDefinedType should be used as a base class for
      new types, which preserves the 0.5 behavior of
      get_col_spec().

    .. change::
        :tags: dialect, refactor
        :tickets: 

      The result_processor() method of all type classes now
      accepts a second argument "coltype", which is the DBAPI
      type argument from cursor.description.  This argument
      can help some types decide on the most efficient processing
      of result values.

    .. change::
        :tags: dialect, refactor
        :tickets: 

      Deprecated Dialect.get_params() removed.

    .. change::
        :tags: dialect, refactor
        :tickets: 

      Dialect.get_rowcount() has been renamed to a descriptor
      "rowcount", and calls cursor.rowcount directly. Dialects
      which need to hardwire a rowcount in for certain calls
      should override the method to provide different behavior.

    .. change::
        :tags: dialect, refactor
        :tickets: 1566

      DefaultRunner and subclasses have been removed.  The job
      of this object has been simplified and moved into
      ExecutionContext.  Dialects which support sequences should
      add a `fire_sequence()` method to their execution context
      implementation.

    .. change::
        :tags: dialect, refactor
        :tickets: 

      Functions and operators generated by the compiler now use
      (almost) regular dispatch functions of the form
      "visit_<opname>" and "visit_<funcname>_fn" to provide
      customed processing. This replaces the need to copy the
      "functions" and "operators" dictionaries in compiler
      subclasses with straightforward visitor methods, and also
      allows compiler subclasses complete control over
      rendering, as the full _Function or _BinaryExpression
      object is passed in.

    .. change::
        :tags: postgresql
        :tickets: 

      New dialects: pg8000, zxjdbc, and pypostgresql
      on py3k.

    .. change::
        :tags: postgresql
        :tickets: 

      The "postgres" dialect is now named "postgresql" !
      Connection strings look like:
      
           postgresql://scott:tiger@localhost/test
           postgresql+pg8000://scott:tiger@localhost/test
      
       The "postgres" name remains for backwards compatibility
       in the following ways:
      
           - There is a "postgres.py" dummy dialect which
             allows old URLs to work, i.e.
             postgres://scott:tiger@localhost/test
      
           - The "postgres" name can be imported from the old
             "databases" module, i.e. "from
             sqlalchemy.databases import postgres" as well as
             "dialects", "from sqlalchemy.dialects.postgres
             import base as pg", will send a deprecation
             warning.
      
           - Special expression arguments are now named
             "postgresql_returning" and "postgresql_where", but
             the older "postgres_returning" and
             "postgres_where" names still work with a
             deprecation warning.

    .. change::
        :tags: postgresql
        :tickets: 

      "postgresql_where" now accepts SQL expressions which
      can also include literals, which will be quoted as needed.

    .. change::
        :tags: postgresql
        :tickets: 

      The psycopg2 dialect now uses psycopg2's "unicode extension"
      on all new connections, which allows all String/Text/etc.
      types to skip the need to post-process bytestrings into
      unicode (an expensive step due to its volume).  Other
      dialects which return unicode natively (pg8000, zxjdbc)
      also skip unicode post-processing.

    .. change::
        :tags: postgresql
        :tickets: 1511

      Added new ENUM type, which exists as a schema-level
      construct and extends the generic Enum type.  Automatically
      associates itself with tables and their parent metadata
      to issue the appropriate CREATE TYPE/DROP TYPE
      commands as needed, supports unicode labels, supports
      reflection.

    .. change::
        :tags: postgresql
        :tickets: 

      INTERVAL supports an optional "precision" argument
      corresponding to the argument that PG accepts.

    .. change::
        :tags: postgresql
        :tickets: 

      using new dialect.initialize() feature to set up
      version-dependent behavior.

    .. change::
        :tags: postgresql
        :tickets: 1279

      somewhat better support for % signs in table/column names;
      psycopg2 can't handle a bind parameter name of
      %(foobar)s however and SQLA doesn't want to add overhead
      just to treat that one non-existent use case.

    .. change::
        :tags: postgresql
        :tickets: 1516

      Inserting NULL into a primary key + foreign key column
      will allow the "not null constraint" error to raise,
      not an attempt to execute a nonexistent "col_id_seq"
      sequence.

    .. change::
        :tags: postgresql
        :tickets: 

      autoincrement SELECT statements, i.e. those which
      select from a procedure that modifies rows, now work
      with server-side cursor mode (the named cursor isn't
      used for such statements.)

    .. change::
        :tags: postgresql
        :tickets: 1636

      postgresql dialect can properly detect pg "devel" version
      strings, i.e. "8.5devel"

    .. change::
        :tags: postgresql
        :tickets: 1619

      The psycopg2 now respects the statement option
      "stream_results". This option overrides the connection setting
      "server_side_cursors". If true, server side cursors will be
      used for the statement. If false, they will not be used, even
      if "server_side_cursors" is true on the
      connection.

    .. change::
        :tags: mysql
        :tickets: 

      New dialects: oursql, a new native dialect,
      MySQL Connector/Python, a native Python port of MySQLdb,
      and of course zxjdbc on Jython.

    .. change::
        :tags: mysql
        :tickets: 

      VARCHAR/NVARCHAR will not render without a length, raises
      an error before passing to MySQL.   Doesn't impact
      CAST since VARCHAR is not allowed in MySQL CAST anyway,
      the dialect renders CHAR/NCHAR in those cases.

    .. change::
        :tags: mysql
        :tickets: 

      all the _detect_XXX() functions now run once underneath
      dialect.initialize()

    .. change::
        :tags: mysql
        :tickets: 1279

      somewhat better support for % signs in table/column names;
      MySQLdb can't handle % signs in SQL when executemany() is used,
      and SQLA doesn't want to add overhead just to treat that one
      non-existent use case.

    .. change::
        :tags: mysql
        :tickets: 

      the BINARY and MSBinary types now generate "BINARY" in all
      cases.  Omitting the "length" parameter will generate
      "BINARY" with no length.  Use BLOB to generate an unlengthed
      binary column.

    .. change::
        :tags: mysql
        :tickets: 

      the "quoting='quoted'" argument to MSEnum/ENUM is deprecated.
      It's best to rely upon the automatic quoting.

    .. change::
        :tags: mysql
        :tickets: 

      ENUM now subclasses the new generic Enum type, and also handles
      unicode values implicitly, if the given labelnames are unicode
      objects.

    .. change::
        :tags: mysql
        :tickets: 1539

      a column of type TIMESTAMP now defaults to NULL if
      "nullable=False" is not passed to Column(), and no default
      is present. This is now consistent with all other types,
      and in the case of TIMESTAMP explicitly renders "NULL"
      due to MySQL's "switching" of default nullability
      for TIMESTAMP columns.

    .. change::
        :tags: oracle
        :tickets: 

      unit tests pass 100% with cx_oracle !

    .. change::
        :tags: oracle
        :tickets: 

      support for cx_Oracle's "native unicode" mode which does
      not require NLS_LANG to be set. Use the latest 5.0.2 or
      later of cx_oracle.

    .. change::
        :tags: oracle
        :tickets: 

      an NCLOB type is added to the base types.

    .. change::
        :tags: oracle
        :tickets: 

      use_ansi=False won't leak into the FROM/WHERE clause of
      a statement that's selecting from a subquery that also
      uses JOIN/OUTERJOIN.

    .. change::
        :tags: oracle
        :tickets: 1467

      added native INTERVAL type to the dialect.  This supports
      only the DAY TO SECOND interval type so far due to lack
      of support in cx_oracle for YEAR TO MONTH.

    .. change::
        :tags: oracle
        :tickets: 

      usage of the CHAR type results in cx_oracle's
      FIXED_CHAR dbapi type being bound to statements.

    .. change::
        :tags: oracle
        :tickets: 885

      the Oracle dialect now features NUMBER which intends
      to act justlike Oracle's NUMBER type.  It is the primary
      numeric type returned by table reflection and attempts
      to return Decimal()/float/int based on the precision/scale
      parameters.

    .. change::
        :tags: oracle
        :tickets: 

      func.char_length is a generic function for LENGTH

    .. change::
        :tags: oracle
        :tickets: 

      ForeignKey() which includes onupdate=<value> will emit a
      warning, not emit ON UPDATE CASCADE which is unsupported
      by oracle

    .. change::
        :tags: oracle
        :tickets: 

      the keys() method of RowProxy() now returns the result
      column names *normalized* to be SQLAlchemy case
      insensitive names. This means they will be lower case for
      case insensitive names, whereas the DBAPI would normally
      return them as UPPERCASE names. This allows row keys() to
      be compatible with further SQLAlchemy operations.

    .. change::
        :tags: oracle
        :tickets: 

      using new dialect.initialize() feature to set up
      version-dependent behavior.

    .. change::
        :tags: oracle
        :tickets: 1125

      using types.BigInteger with Oracle will generate
      NUMBER(19)

    .. change::
        :tags: oracle
        :tickets: 

      "case sensitivity" feature will detect an all-lowercase
      case-sensitive column name during reflect and add
      "quote=True" to the generated Column, so that proper
      quoting is maintained.

    .. change::
        :tags: firebird
        :tickets: 

      the keys() method of RowProxy() now returns the result
      column names *normalized* to be SQLAlchemy case
      insensitive names. This means they will be lower case for
      case insensitive names, whereas the DBAPI would normally
      return them as UPPERCASE names. This allows row keys() to
      be compatible with further SQLAlchemy operations.

    .. change::
        :tags: firebird
        :tickets: 

      using new dialect.initialize() feature to set up
      version-dependent behavior.

    .. change::
        :tags: firebird
        :tickets: 

      "case sensitivity" feature will detect an all-lowercase
      case-sensitive column name during reflect and add
      "quote=True" to the generated Column, so that proper
      quoting is maintained.

    .. change::
        :tags: mssql
        :tickets: 

      MSSQL + Pyodbc + FreeTDS now works for the most part,
      with possible exceptions regarding binary data as well as
      unicode schema identifiers.

    .. change::
        :tags: mssql
        :tickets: 

      the "has_window_funcs" flag is removed. LIMIT/OFFSET
      usage will use ROW NUMBER as always, and if on an older
      version of SQL Server, the operation fails. The behavior
      is exactly the same except the error is raised by SQL
      server instead of the dialect, and no flag setting is
      required to enable it.

    .. change::
        :tags: mssql
        :tickets: 

      the "auto_identity_insert" flag is removed. This feature
      always takes effect when an INSERT statement overrides a
      column that is known to have a sequence on it. As with
      "has_window_funcs", if the underlying driver doesn't
      support this, then you can't do this operation in any
      case, so there's no point in having a flag.

    .. change::
        :tags: mssql
        :tickets: 

      using new dialect.initialize() feature to set up
      version-dependent behavior.

    .. change::
        :tags: mssql
        :tickets: 

      removed references to sequence which is no longer used.
      implicit identities in mssql work the same as implicit
      sequences on any other dialects. Explicit sequences are
      enabled through the use of "default=Sequence()". See
      the MSSQL dialect documentation for more information.

    .. change::
        :tags: sqlite
        :tickets: 

      DATE, TIME and DATETIME types can now take optional storage_format
      and regexp argument. storage_format can be used to store those types
      using a custom string format. regexp allows to use a custom regular
      expression to match string values from the database.

    .. change::
        :tags: sqlite
        :tickets: 

      Time and DateTime types now use by a default a stricter regular
      expression to match strings from the database. Use the regexp
      argument if you are using data stored in a legacy format.

    .. change::
        :tags: sqlite
        :tickets: 

      __legacy_microseconds__ on SQLite Time and DateTime types is not
      supported anymore. You should use the storage_format argument
      instead.

    .. change::
        :tags: sqlite
        :tickets: 

      Date, Time and DateTime types are now stricter in what they accept as
      bind parameters: Date type only accepts date objects (and datetime
      ones, because they inherit from date), Time only accepts time
      objects, and DateTime only accepts date and datetime objects.

    .. change::
        :tags: sqlite
        :tickets: 1016

      Table() supports a keyword argument "sqlite_autoincrement", which
      applies the SQLite keyword "AUTOINCREMENT" to the single integer
      primary key column when generating DDL. Will prevent generation of
      a separate PRIMARY KEY constraint.

    .. change::
        :tags: types
        :tickets: 

      The construction of types within dialects has been totally
      overhauled.  Dialects now define publically available types
      as UPPERCASE names exclusively, and internal implementation
      types using underscore identifiers (i.e. are private).
      The system by which types are expressed in SQL and DDL
      has been moved to the compiler system.  This has the
      effect that there are much fewer type objects within
      most dialects. A detailed document on this architecture
      for dialect authors is in
      lib/sqlalchemy/dialects/type_migration_guidelines.txt .

    .. change::
        :tags: types
        :tickets: 

      Types no longer make any guesses as to default
      parameters. In particular, Numeric, Float, NUMERIC,
      FLOAT, DECIMAL don't generate any length or scale unless
      specified.

    .. change::
        :tags: types
        :tickets: 1664

      types.Binary is renamed to types.LargeBinary, it only
      produces BLOB, BYTEA, or a similar "long binary" type.
      New base BINARY and VARBINARY
      types have been added to access these MySQL/MS-SQL specific
      types in an agnostic way.

    .. change::
        :tags: types
        :tickets: 

      String/Text/Unicode types now skip the unicode() check
      on each result column value if the dialect has
      detected the DBAPI as returning Python unicode objects
      natively.  This check is issued on first connect
      using "SELECT CAST 'some text' AS VARCHAR(10)" or
      equivalent, then checking if the returned object
      is a Python unicode.   This allows vast performance
      increases for native-unicode DBAPIs, including
      pysqlite/sqlite3, psycopg2, and pg8000.

    .. change::
        :tags: types
        :tickets: 

      Most types result processors have been checked for possible speed
      improvements. Specifically, the following generic types have been
      optimized, resulting in varying speed improvements:
      Unicode, PickleType, Interval, TypeDecorator, Binary.
      Also the following dbapi-specific implementations have been improved:
      Time, Date and DateTime on Sqlite, ARRAY on Postgresql,
      Time on MySQL, Numeric(as_decimal=False) on MySQL, oursql and
      pypostgresql, DateTime on cx_oracle and LOB-based types on cx_oracle.

    .. change::
        :tags: types
        :tickets: 

      Reflection of types now returns the exact UPPERCASE
      type within types.py, or the UPPERCASE type within
      the dialect itself if the type is not a standard SQL
      type.  This means reflection now returns more accurate
      information about reflected types.

    .. change::
        :tags: types
        :tickets: 1511, 1109

      Added a new Enum generic type. Enum is a schema-aware object
      to support databases which require specific DDL in order to
      use enum or equivalent; in the case of PG it handles the
      details of `CREATE TYPE`, and on other databases without
      native enum support will by generate VARCHAR + an inline CHECK
      constraint to enforce the enum.

    .. change::
        :tags: types
        :tickets: 1467

      The Interval type includes a "native" flag which controls
      if native INTERVAL types (postgresql + oracle) are selected
      if available, or not.  "day_precision" and "second_precision"
      arguments are also added which propagate as appropriately
      to these native types. Related to.

    .. change::
        :tags: types
        :tickets: 1589

      The Boolean type, when used on a backend that doesn't
      have native boolean support, will generate a CHECK
      constraint "col IN (0, 1)" along with the int/smallint-
      based column type.  This can be switched off if
      desired with create_constraint=False.
      Note that MySQL has no native boolean *or* CHECK constraint
      support so this feature isn't available on that platform.

    .. change::
        :tags: types
        :tickets: 

      PickleType now uses == for comparison of values when
      mutable=True, unless the "comparator" argument with a
      comparsion function is specified to the type. Objects
      being pickled will be compared based on identity (which
      defeats the purpose of mutable=True) if __eq__() is not
      overridden or a comparison function is not provided.

    .. change::
        :tags: types
        :tickets: 

      The default "precision" and "scale" arguments of Numeric
      and Float have been removed and now default to None.
      NUMERIC and FLOAT will be rendered with no numeric
      arguments by default unless these values are provided.

    .. change::
        :tags: types
        :tickets: 

      AbstractType.get_search_list() is removed - the games
      that was used for are no longer necessary.

    .. change::
        :tags: types
        :tickets: 1125

      Added a generic BigInteger type, compiles to
      BIGINT or NUMBER(19).

    .. change::
        :tags: types
        :tickets: 

      sqlsoup has been overhauled to explicitly support an 0.5 style
      session, using autocommit=False, autoflush=True. Default
      behavior of SQLSoup now requires the usual usage of commit()
      and rollback(), which have been added to its interface. An
      explcit Session or scoped_session can be passed to the
      constructor, allowing these arguments to be overridden.

    .. change::
        :tags: types
        :tickets: 

      sqlsoup db.<sometable>.update() and delete() now call
      query(cls).update() and delete(), respectively.

    .. change::
        :tags: types
        :tickets: 

      sqlsoup now has execute() and connection(), which call upon
      the Session methods of those names, ensuring that the bind is
      in terms of the SqlSoup object's bind.

    .. change::
        :tags: types
        :tickets: 

      sqlsoup objects no longer have the 'query' attribute - it's
      not needed for sqlsoup's usage paradigm and it gets in the
      way of a column that is actually named 'query'.

    .. change::
        :tags: types
        :tickets: 1259

      The signature of the proxy_factory callable passed to
      association_proxy is now (lazy_collection, creator,
      value_attr, association_proxy), adding a fourth argument
      that is the parent AssociationProxy argument.  Allows
      serializability and subclassing of the built in collections.

    .. change::
        :tags: types
        :tickets: 1372

      association_proxy now has basic comparator methods .any(),
      .has(), .contains(), ==, !=, thanks to Scott Torborg.
