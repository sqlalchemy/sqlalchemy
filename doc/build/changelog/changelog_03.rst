
==============
0.3 Changelog
==============

                
.. changelog::
    :version: 0.3.11
    :released: Sun Oct 14 2007

    .. change::
        :tags: sql
        :tickets: 

      tweak DISTINCT precedence for clauses like
      `func.count(t.c.col.distinct())`

    .. change::
        :tags: sql
        :tickets: 719

      Fixed detection of internal '$' characters in :bind$params

    .. change::
        :tags: sql
        :tickets: 768

      don't assume join criterion consists only of column objects

    .. change::
        :tags: sql
        :tickets: 764

      adjusted operator precedence of NOT to match '==' and others, so that
      ~(x==y) produces NOT (x=y), which is compatible with MySQL < 5.0
      (doesn't like "NOT x=y")

    .. change::
        :tags: orm
        :tickets: 687

      added a check for joining from A->B using join(), along two
      different m2m tables.  this raises an error in 0.3 but is
      possible in 0.4 when aliases are used.

    .. change::
        :tags: orm
        :tickets: 

      fixed small exception throw bug in Session.merge()

    .. change::
        :tags: orm
        :tickets: 

      fixed bug where mapper, being linked to a join where one table had
      no PK columns, would not detect that the joined table had no PK.

    .. change::
        :tags: orm
        :tickets: 769

      fixed bugs in determining proper sync clauses from custom inherit
      conditions

    .. change::
        :tags: orm
        :tickets: 813

      backref remove object operation doesn't fail if the other-side
      collection doesn't contain the item, supports noload collections

    .. change::
        :tags: engine
        :tickets: 

      fixed another occasional race condition which could occur
      when using pool with threadlocal setting

    .. change::
        :tags: mysql
        :tickets: 

      fixed specification of YEAR columns when generating schema

    .. change::
        :tags: mssql
        :tickets: 679

      added support for TIME columns (simulated using DATETIME)

    .. change::
        :tags: mssql
        :tickets: 721

      added support for BIGINT, MONEY, SMALLMONEY, UNIQUEIDENTIFIER and
      SQL_VARIANT

    .. change::
        :tags: mssql
        :tickets: 684

      index names are now quoted when dropping from reflected tables

    .. change::
        :tags: mssql
        :tickets: 

      can now specify a DSN for PyODBC, using a URI like mssql:///?dsn=bob

    .. change::
        :tags: postgres
        :tickets: 

      when reflecting tables from alternate schemas, the "default" placed upon
      the primary key, i.e. usually a sequence name, has the "schema" name
      unconditionally quoted, so that schema names which need quoting are fine.
      its slightly unnecessary for schema names which don't need quoting
      but not harmful.

    .. change::
        :tags: sqlite
        :tickets: 

      passthrough for stringified dates

    .. change::
        :tags: firebird
        :tickets: 

      supports_sane_rowcount() set to False due to ticket #370 (right way).

    .. change::
        :tags: firebird
        :tickets: 

      fixed reflection of Column's nullable property.

    .. change::
        :tags: oracle
        :tickets: 622, 751

      removed LONG_STRING, LONG_BINARY from "binary" types, so type objects
      don't try to read their values as LOB.

.. changelog::
    :version: 0.3.10
    :released: Fri Jul 20 2007

    .. change::
        :tags: general
        :tickets: 

      a new mutex that was added in 0.3.9 causes the pool_timeout
      feature to fail during a race condition; threads would
      raise TimeoutError immediately with no delay if many threads
      push the pool into overflow at the same time.  this issue has been
      fixed.

    .. change::
        :tags: sql
        :tickets: 

      got connection-bound metadata to work with implicit execution

    .. change::
        :tags: sql
        :tickets: 667

      foreign key specs can have any chararcter in their identifiers

    .. change::
        :tags: sql
        :tickets: 664

      added commutativity-awareness to binary clause comparisons to
      each other, improves ORM lazy load optimization

    .. change::
        :tags: orm
        :tickets: 

      cleanup to connection-bound sessions, SessionTransaction

    .. change::
        :tags: postgres
        :tickets: 571

      fixed max identifier length (63)

.. changelog::
    :version: 0.3.9
    :released: Sun Jul 15 2007

    .. change::
        :tags: general
        :tickets: 607

      better error message for NoSuchColumnError

    .. change::
        :tags: general
        :tickets: 428

      finally figured out how to get setuptools version in, available
      as sqlalchemy.__version__

    .. change::
        :tags: general
        :tickets: 

      the various "engine" arguments, such as "engine", "connectable",
      "engine_or_url", "bind_to", etc. are all present, but deprecated.
      they all get replaced by the single term "bind".  you also
      set the "bind" of MetaData using
      metadata.bind = <engine or connection>

    .. change::
        :tags: ext
        :tickets: 

      iteration over dict association proxies is now dict-like, not
      InstrumentedList-like (e.g. over keys instead of values)

    .. change::
        :tags: ext
        :tickets: 597

      association proxies no longer bind tightly to source collections, and are constructed with a thunk instead

    .. change::
        :tags: ext
        :tickets: 

      added selectone_by() to assignmapper

    .. change::
        :tags: orm
        :tickets: 

      forwards-compatibility with 0.4: added one(), first(), and
      all() to Query.  almost all Query functionality from 0.4 is
      present in 0.3.9 for forwards-compat purposes.

    .. change::
        :tags: orm
        :tickets: 

      reset_joinpoint() really really works this time, promise ! lets
      you re-join from the root:
      query.join(['a', 'b']).filter(<crit>).reset_joinpoint().\
      join(['a', 'c']).filter(<some other crit>).all()
      in 0.4 all join() calls start from the "root"

    .. change::
        :tags: orm
        :tickets: 613

      added synchronization to the mapper() construction step, to avoid
      thread collisions when pre-existing mappers are compiling in a
      different thread

    .. change::
        :tags: orm
        :tickets: 

      a warning is issued by Mapper when two primary key columns of the
      same name are munged into a single attribute.  this happens frequently
      when mapping to joins (or inheritance).

    .. change::
        :tags: orm
        :tickets: 598

      synonym() properties are fully supported by all Query joining/
      with_parent operations

    .. change::
        :tags: orm
        :tickets: 

      fixed very stupid bug when deleting items with many-to-many
      uselist=False relations

    .. change::
        :tags: orm
        :tickets: 

      remember all that stuff about polymorphic_union ?  for
      joined table inheritance ?  Funny thing...
      You sort of don't need it for joined table inheritance, you
      can just string all the tables together via outerjoin().
      The UNION still applies if concrete tables are involved,
      though (since nothing to join them on).

    .. change::
        :tags: orm
        :tickets: 

      small fix to eager loading to better work with eager loads
      to polymorphic mappers that are using a straight "outerjoin"
      clause

    .. change::
        :tags: sql
        :tickets: 

      ForeignKey to a table in a schema that's not the default schema
      requires the schema to be explicit; i.e. ForeignKey('alt_schema.users.id')

    .. change::
        :tags: sql
        :tickets: 

      MetaData can now be constructed with an engine or url as the first
      argument, just like BoundMetaData

    .. change::
        :tags: sql
        :tickets: 

      BoundMetaData is now deprecated, and MetaData is a direct substitute.

    .. change::
        :tags: sql
        :tickets: 

      DynamicMetaData has been renamed to ThreadLocalMetaData.  the
      DynamicMetaData name is deprecated and is an alias for ThreadLocalMetaData
      or a regular MetaData if threadlocal=False

    .. change::
        :tags: sql
        :tickets: 

      composite primary key is represented as a non-keyed set to allow for
      composite keys consisting of cols with the same name; occurs within a
      Join.  helps inheritance scenarios formulate correct PK.

    .. change::
        :tags: sql
        :tickets: 185

      improved ability to get the "correct" and most minimal set of primary key
      columns from a join, equating foreign keys and otherwise equated columns.
      this is also mostly to help inheritance scenarios formulate the best
      choice of primary key columns.

    .. change::
        :tags: sql
        :tickets: 

      added 'bind' argument to Sequence.create()/drop(), ColumnDefault.execute()

    .. change::
        :tags: sql
        :tickets: 650

      columns can be overridden in a reflected table with a "key"
      attribute different than the column's name, including for primary key
      columns

    .. change::
        :tags: sql
        :tickets: 657

      fixed "ambiguous column" result detection, when dupe col names exist
      in a result

    .. change::
        :tags: sql
        :tickets: 

      some enhancements to "column targeting", the ability to match a column
      to a "corresponding" column in another selectable.  this affects mostly
      ORM ability to map to complex joins

    .. change::
        :tags: sql
        :tickets: 619

      MetaData and all SchemaItems are safe to use with pickle.  slow
      table reflections can be dumped into a pickled file to be reused later.
      Just reconnect the engine to the metadata after unpickling.

    .. change::
        :tags: sql
        :tickets: 

      added a mutex to QueuePool's "overflow" calculation to prevent a race
      condition that can bypass max_overflow

    .. change::
        :tags: sql
        :tickets: 623

      fixed grouping of compound selects to give correct results. will break
      on sqlite in some cases, but those cases were producing incorrect
      results anyway, sqlite doesn't support grouped compound selects

    .. change::
        :tags: sql
        :tickets: 620

      fixed precedence of operators so that parenthesis are correctly applied

    .. change::
        :tags: sql
        :tickets: 545

      calling <column>.in_() (i.e. with no arguments) will return
      "CASE WHEN (<column> IS NULL) THEN NULL ELSE 0 END = 1)", so that
      NULL or False is returned in all cases, rather than throwing an error

    .. change::
        :tags: sql
        :tickets: 

      fixed "where"/"from" criterion of select() to accept a unicode string
      in addition to regular string - both convert to text()

    .. change::
        :tags: sql
        :tickets: 558

      added standalone distinct() function in addition to column.distinct()

    .. change::
        :tags: sql
        :tickets: 

      result.last_inserted_ids() should return a list that is identically
      sized to the primary key constraint of the table.  values that were
      "passively" created and not available via cursor.lastrowid will be None.

    .. change::
        :tags: sql
        :tickets: 589

      long-identifier detection fixed to use > rather than >= for
      max ident length

    .. change::
        :tags: sql
        :tickets: 593

      fixed bug where selectable.corresponding_column(selectable.c.col)
      would not return selectable.c.col, if the selectable is a join
      of a table and another join involving the same table.  messed
      up ORM decision making

    .. change::
        :tags: sql
        :tickets: 595

      added Interval type to types.py

    .. change::
        :tags: mysql
        :tickets: 625

      fixed catching of some errors that imply a dropped connection

    .. change::
        :tags: mysql
        :tickets: 624

      fixed escaping of the modulo operator

    .. change::
        :tags: mysql
        :tickets: 590

      added 'fields' to reserved words

    .. change::
        :tags: mysql
        :tickets: 

      various reflection enhancement/fixes

    .. change::
        :tags: oracle
        :tickets: 604

      datetime fixes: got subsecond TIMESTAMP to work,
      added OracleDate which supports types.Date with only year/month/day

    .. change::
        :tags: oracle
        :tickets: 

      added dialect flag "auto_convert_lobs", defaults to True; will cause any
      LOB objects detected in a result set to be forced into OracleBinary
      so that the LOB is read() automatically, if no typemap was present
      (i.e., if a textual execute() was issued).

    .. change::
        :tags: oracle
        :tickets: 624

      mod operator '%' produces MOD

    .. change::
        :tags: oracle
        :tickets: 542

      converts cx_oracle datetime objects to Python datetime.datetime when
      Python 2.3 used

    .. change::
        :tags: oracle
        :tickets: 

      fixed unicode conversion in Oracle TEXT type

    .. change::
        :tags: postgres
        :tickets: 624

      fixed escaping of the modulo operator

    .. change::
        :tags: postgres
        :tickets: 570

      added support for reflection of domains

    .. change::
        :tags: postgres
        :tickets: 

      types which are missing during reflection resolve to Null type
      instead of raising an error

    .. change::
        :tags: postgres
        :tickets: 

      the fix in "schema" above fixes reflection of foreign keys from an
      alt-schema table to a public schema table

    .. change::
        :tags: sqlite
        :tickets: 

      rearranged dialect initialization so it has time to warn about pysqlite1
      being too old.

    .. change::
        :tags: sqlite
        :tickets: 

      sqlite better handles datetime/date/time objects mixed and matched
      with various Date/Time/DateTime columns

    .. change::
        :tags: sqlite
        :tickets: 603

      string PK column inserts don't get overwritten with OID

    .. change::
        :tags: mssql
        :tickets: 634

      fix port option handling for pyodbc

    .. change::
        :tags: mssql
        :tickets: 

      now able to reflect start and increment values for identity columns

    .. change::
        :tags: mssql
        :tickets: 

      preliminary support for using scope_identity() with pyodbc

.. changelog::
    :version: 0.3.8
    :released: Sat Jun 02 2007

    .. change::
        :tags: engines
        :tickets: 

      added detach() to Connection, allows underlying DBAPI connection
      to be detached from its pool, closing on dereference/close()
      instead of being reused by the pool.

    .. change::
        :tags: engines
        :tickets: 

      added invalidate() to Connection, immediately invalidates the
      Connection and its underlying DBAPI connection.

    .. change::
        :tags: sql
        :tickets: 

      _Label class overrides compare_self to return its ultimate
      object. meaning, if you say someexpr.label('foo') == 5, it
      produces the correct "someexpr == 5".

    .. change::
        :tags: sql
        :tickets: 

      _Label propagates "_hide_froms()" so that scalar selects
      behave more properly with regards to FROM clause #574

    .. change::
        :tags: sql
        :tickets: 

      fix to long name generation when using oid_column as an order by
      (oids used heavily in mapper queries)

    .. change::
        :tags: sql
        :tickets: 

      significant speed improvement to ResultProxy, pre-caches
      TypeEngine dialect implementations and saves on function calls
      per column

    .. change::
        :tags: sql
        :tickets: 

      parenthesis are applied to clauses via a new _Grouping
      construct. uses operator precedence to more intelligently apply
      parenthesis to clauses, provides cleaner nesting of clauses
      (doesn't mutate clauses placed in other clauses, i.e. no 'parens'
      flag)

    .. change::
        :tags: sql
        :tickets: 

      added 'modifier' keyword, works like func.<foo> except does not
      add parenthesis.  e.g. select([modifier.DISTINCT(...)]) etc.

    .. change::
        :tags: sql
        :tickets: 578

      removed "no group by's in a select that's part of a UNION"
      restriction

    .. change::
        :tags: orm
        :tickets: 

      added reset_joinpoint() method to Query, moves the "join point"
      back to the starting mapper. 0.4 will change the behavior of
      join() to reset the "join point" in all cases so this is an
      interim method. for forwards compatibility, ensure joins across
      multiple relations are specified using a single join(), i.e.
      join(['a', 'b', 'c']).

    .. change::
        :tags: orm
        :tickets: 

      fixed bug in query.instances() that wouldn't handle more than
      on additional mapper or one additional column.

    .. change::
        :tags: orm
        :tickets: 

      "delete-orphan" no longer implies "delete". ongoing effort to
      separate the behavior of these two operations.

    .. change::
        :tags: orm
        :tickets: 

      many-to-many relationships properly set the type of bind params
      for delete operations on the association table

    .. change::
        :tags: orm
        :tickets: 

      many-to-many relationships check that the number of rows deleted
      from the association table by a delete operation matches the
      expected results

    .. change::
        :tags: orm
        :tickets: 

      session.get() and session.load() propagate \**kwargs through to
      query

    .. change::
        :tags: orm
        :tickets: 577

      fix to polymorphic query which allows the original
      polymorphic_union to be embedded into a correlated subquery

    .. change::
        :tags: orm
        :tickets: 

      fix to select_by(<propname>=<object instance>) -style joins in
      conjunction with many-to-many relationships, bug introduced in
      r2556

    .. change::
        :tags: orm
        :tickets: 

      the "primary_key" argument to mapper() is propagated to the
      "polymorphic" mapper. primary key columns in this list get
      normalized to that of the mapper's local table.

    .. change::
        :tags: orm
        :tickets: 

      restored logging of "lazy loading clause" under
      sa.orm.strategies logger, got removed in 0.3.7

    .. change::
        :tags: orm
        :tickets: 

      improved support for eagerloading of properties off of mappers
      that are mapped to select() statements; i.e. eagerloader is
      better at locating the correct selectable with which to attach
      its LEFT OUTER JOIN.

    .. change::
        :tags: mysql
        :tickets: 

      Nearly all MySQL column types are now supported for declaration
      and reflection. Added NCHAR, NVARCHAR, VARBINARY, TINYBLOB,
      LONGBLOB, YEAR

    .. change::
        :tags: mysql
        :tickets: 

      The sqltypes.Binary passthrough now always builds a BLOB,
      avoiding problems with very old database versions

    .. change::
        :tags: mysql
        :tickets: 

      support for column-level CHARACTER SET and COLLATE declarations,
      as well as ASCII, UNICODE, NATIONAL and BINARY shorthand.

    .. change::
        :tags: firebird
        :tickets: 

      set max identifier length to 31

    .. change::
        :tags: firebird
        :tickets: 

      supports_sane_rowcount() set to False due to ticket #370.
      versioned_id_col feature wont work in FB.

    .. change::
        :tags: firebird
        :tickets: 

      some execution fixes

    .. change::
        :tags: firebird
        :tickets: 

      new association proxy implementation, implementing complete
      proxies to list, dict and set-based relation collections

    .. change::
        :tags: firebird
        :tickets: 

      added orderinglist, a custom list class that synchronizes an
      object attribute with that object's position in the list

    .. change::
        :tags: firebird
        :tickets: 

      small fix to SelectResultsExt to not bypass itself during
      select().

    .. change::
        :tags: firebird
        :tickets: 

      added filter(), filter_by() to assignmapper

.. changelog::
    :version: 0.3.7
    :released: Sun Apr 29 2007

    .. change::
        :tags: engines
        :tickets: 

      warnings module used for issuing warnings (instead of logging)

    .. change::
        :tags: engines
        :tickets: 480

      cleanup of DBAPI import strategies across all engines

    .. change::
        :tags: engines
        :tickets: 

      refactoring of engine internals which reduces complexity,
      number of codepaths; places more state inside of ExecutionContext
      to allow more dialect control of cursor handling, result sets.
      ResultProxy totally refactored and also has two versions of
      "buffered" result sets used for different purposes.

    .. change::
        :tags: engines
        :tickets: 514

      server side cursor support fully functional in postgres.

    .. change::
        :tags: engines
        :tickets: 

      improved framework for auto-invalidation of connections that have
      lost their underlying database, via dialect-specific detection
      of exceptions corresponding to that database's disconnect
      related error messages.  Additionally, when a "connection no
      longer open" condition is detected, the entire connection pool
      is discarded and replaced with a new instance.  #516

    .. change::
        :tags: engines
        :tickets: 521

      the dialects within sqlalchemy.databases become a setuptools
      entry points. loading the built-in database dialects works the
      same as always, but if none found will fall back to trying
      pkg_resources to load an external module

    .. change::
        :tags: engines
        :tickets: 

      Engine contains a "url" attribute referencing the url.URL object
      used by create_engine().

    .. change::
        :tags: sql
        :tickets: 

      keys() of result set columns are not lowercased, come back
      exactly as they're expressed in cursor.description.  note this
      causes colnames to be all caps in oracle.

    .. change::
        :tags: sql
        :tickets: 

      preliminary support for unicode table names, column names and
      SQL statements added, for databases which can support them.
      Works with sqlite and postgres so far.  Mysql *mostly* works
      except the has_table() function does not work.  Reflection
      works too.

    .. change::
        :tags: sql
        :tickets: 522

      the Unicode type is now a direct subclass of String, which now
      contains all the "convert_unicode" logic.  This helps the variety
      of unicode situations that occur in db's such as MS-SQL to be
      better handled and allows subclassing of the Unicode datatype.

    .. change::
        :tags: sql
        :tickets: 

      ClauseElements can be used in in_() clauses now, such as bind
      parameters, etc. #476

    .. change::
        :tags: sql
        :tickets: 

      reverse operators implemented for `CompareMixin` elements,
      allows expressions like "5 + somecolumn" etc. #474

    .. change::
        :tags: sql
        :tickets: 

      the "where" criterion of an update() and delete() now correlates
      embedded select() statements against the table being updated or
      deleted.  this works the same as nested select() statement
      correlation, and can be disabled via the correlate=False flag on
      the embedded select().

    .. change::
        :tags: sql
        :tickets: 512

      column labels are now generated in the compilation phase, which
      means their lengths are dialect-dependent.  So on oracle a label
      that gets truncated to 30 chars will go out to 63 characters
      on postgres.  Also, the true labelname is always attached as the
      accessor on the parent Selectable so there's no need to be aware
      of the "truncated" label names.

    .. change::
        :tags: sql
        :tickets: 

      column label and bind param "truncation" also generate
      deterministic names now, based on their ordering within the
      full statement being compiled.  this means the same statement
      will produce the same string across application restarts and
      allowing DB query plan caching to work better.

    .. change::
        :tags: sql
        :tickets: 513

      the "mini" column labels generated when using subqueries, which
      are to work around glitchy SQLite behavior that doesn't understand
      "foo.id" as equivalent to "id", are now only generated in the case
      that those named columns are selected from (part of)

    .. change::
        :tags: sql
        :tickets: 

      the label() method on ColumnElement will properly propagate the
      TypeEngine of the base element out to the label, including a label()
      created from a scalar=True select() statement.

    .. change::
        :tags: sql
        :tickets: 513

      MS-SQL better detects when a query is a subquery and knows not to
      generate ORDER BY phrases for those

    .. change::
        :tags: sql
        :tickets: 505

      fix for fetchmany() "size" argument being positional in most
      dbapis

    .. change::
        :tags: sql
        :tickets: 

      sending None as an argument to func.<something> will produce
      an argument of NULL

    .. change::
        :tags: sql
        :tickets: 

      query strings in unicode URLs get keys encoded to ascii
      for \**kwargs compat

    .. change::
        :tags: sql
        :tickets: 523

      slight tweak to raw execute() change to also support tuples
      for positional parameters, not just lists

    .. change::
        :tags: sql
        :tickets: 

      fix to case() construct to propagate the type of the first
      WHEN condition as the return type of the case statement

    .. change::
        :tags: orm
        :tickets: 

      fixed critical issue when, after options(eagerload()) is used,
      the mapper would then always apply query "wrapping" behavior
      for all subsequent LIMIT/OFFSET/DISTINCT queries, even if no
      eager loading was applied on those subsequent queries.

    .. change::
        :tags: orm
        :tickets: 541

      added query.with_parent(someinstance) method.  searches for
      target instance using lazy join criterion from parent instance.
      takes optional string "property" to isolate the desired relation.
      also adds static Query.query_from_parent(instance, property)
      version.

    .. change::
        :tags: orm
        :tickets: 554

      improved query.XXX_by(someprop=someinstance) querying to use
      similar methodology to with_parent, i.e. using the "lazy" clause
      which prevents adding the remote instance's table to the SQL,
      thereby making more complex conditions possible

    .. change::
        :tags: orm
        :tickets: 

      added generative versions of aggregates, i.e. sum(), avg(), etc.
      to query. used via query.apply_max(), apply_sum(), etc.
      #552

    .. change::
        :tags: orm
        :tickets: 

      fix to using distinct() or distinct=True in combination with
      join() and similar

    .. change::
        :tags: orm
        :tickets: 

      corresponding to label/bindparam name generation, eager loaders
      generate deterministic names for the aliases they create using
      md5 hashes.

    .. change::
        :tags: orm
        :tickets: 

      improved/fixed custom collection classes when giving it "set"/
      "sets.Set" classes or subclasses (was still looking for append()
      methods on them during lazy loads)

    .. change::
        :tags: orm
        :tickets: 

      restored old "column_property()" ORM function (used to be called
      "column()") to force any column expression to be added as a property
      on a mapper, particularly those that aren't present in the mapped
      selectable.  this allows "scalar expressions" of any kind to be
      added as relations (though they have issues with eager loads).

    .. change::
        :tags: orm
        :tickets: 533

      fix to many-to-many relationships targeting polymorphic mappers

    .. change::
        :tags: orm
        :tickets: 543

      making progress with session.merge() as well as combining its
      usage with entity_name

    .. change::
        :tags: orm
        :tickets: 

      the usual adjustments to relationships between inheriting mappers,
      in this case establishing relation()s to subclass mappers where
      the join conditions come from the superclass' table

    .. change::
        :tags: informix
        :tickets: 

      informix support added !  courtesy James Zhang, who put a ton
      of effort in.

    .. change::
        :tags: sqlite
        :tickets: 

      removed silly behavior where sqlite would reflect UNIQUE indexes
      as part of the primary key (?!)

    .. change::
        :tags: oracle
        :tickets: 

      small fix to allow successive compiles of the same SELECT object
      which features LIMIT/OFFSET.  oracle dialect needs to modify
      the object to have ROW_NUMBER OVER and wasn't performing
      the full series of steps on successive compiles.

    .. change::
        :tags: mysql
        :tickets: 

      support for SSL arguments given as inline within URL query string,
      prefixed with "ssl\_", courtesy terjeros@gmail.com.

    .. change::
        :tags: <schemaname>, mysql
        :tickets: 

      mysql uses "DESCRIBE.<tablename>", catching exceptions
      if table doesn't exist, in order to determine if a table exists.
      this supports unicode table names as well as schema names. tested
      with MySQL5 but should work with 4.1 series as well. (#557)

    .. change::
        :tags: extensions
        :tickets: 

      big fix to AssociationProxy so that multiple AssociationProxy
      objects can be associated with a single association collection.

    .. change::
        :tags: extensions
        :tickets: 

      assign_mapper names methods according to their keys (i.e. __name__)
      #551

    .. change::
        :tags: mssql
        :tickets: 

      pyodbc is now the preferred DB-API for MSSQL, and if no module is
      specifically requested, will be loaded first on a module probe.

    .. change::
        :tags: mssql
        :tickets: 

      The @@SCOPE_IDENTITY is now used instead of @@IDENTITY. This
      behavior may be overridden with the engine_connect
      "use_scope_identity" keyword parameter, which may also be specified
      in the dburi.

.. changelog::
    :version: 0.3.6
    :released: Fri Mar 23 2007

    .. change::
        :tags: sql
        :tickets: 

      bindparam() names are now repeatable!  specify two
      distinct bindparam()s with the same name in a single statement,
      and the key will be shared.  proper positional/named args translate
      at compile time.  for the old behavior of "aliasing" bind parameters
      with conflicting names, specify "unique=True" - this option is
      still used internally for all the auto-genererated (value-based)
      bind parameters.

    .. change::
        :tags: sql
        :tickets: 

      slightly better support for bind params as column clauses, either
      via bindparam() or via literal(), i.e. select([literal('foo')])

    .. change::
        :tags: sql
        :tickets: 

      MetaData can bind to an engine either via "url" or "engine" kwargs
      to constructor, or by using connect() method. BoundMetaData is
      identical to MetaData except engine_or_url param is required.
      DynamicMetaData is the same and provides thread-local connections be
      default.

    .. change::
        :tags: sql
        :tickets: 

      exists() becomes useable as a standalone selectable, not just in a
      WHERE clause, i.e. exists([columns], criterion).select()

    .. change::
        :tags: sql
        :tickets: 

      correlated subqueries work inside of ORDER BY, GROUP BY

    .. change::
        :tags: sql
        :tickets: 

      fixed function execution with explicit connections, i.e.
      conn.execute(func.dosomething())

    .. change::
        :tags: sql
        :tickets: 

      use_labels flag on select() wont auto-create labels for literal text
      column elements, since we can make no assumptions about the text. to
      create labels for literal columns, you can say "somecol AS
      somelabel", or use literal_column("somecol").label("somelabel")

    .. change::
        :tags: sql
        :tickets: 

      quoting wont occur for literal columns when they are "proxied" into
      the column collection for their selectable (is_literal flag is
      propagated). literal columns are specified via
      literal_column("somestring").

    .. change::
        :tags: sql
        :tickets: 

      added "fold_equivalents" boolean argument to Join.select(), which
      removes 'duplicate' columns from the resulting column clause that
      are known to be equivalent based on the join condition. this is of
      great usage when constructing subqueries of joins which Postgres
      complains about if duplicate column names are present.

    .. change::
        :tags: sql
        :tickets: 503

      fixed use_alter flag on ForeignKeyConstraint

    .. change::
        :tags: sql
        :tickets: 506

      fixed usage of 2.4-only "reversed" in topological.py

    .. change::
        :tags: sql
        :tickets: 501

      for hackers, refactored the "visitor" system of ClauseElement and
      SchemaItem so that the traversal of items is controlled by the
      ClauseVisitor itself, using the method visitor.traverse(item).
      accept_visitor() methods can still be called directly but will not
      do any traversal of child items. ClauseElement/SchemaItem now have a
      configurable get_children() method to return the collection of child
      elements for each parent object. This allows the full traversal of
      items to be clear and unambiguous (as well as loggable), with an
      easy method of limiting a traversal (just pass flags which are
      picked up by appropriate get_children() methods).

    .. change::
        :tags: sql
        :tickets: 

      the "else\_" parameter to the case statement now properly works when
      set to zero.

    .. change::
        :tags: orm
        :tickets: 

      the full featureset of the SelectResults extension has been merged
      into a new set of methods available off of Query.  These methods
      all provide "generative" behavior, whereby the Query is copied
      and a new one returned with additional criterion added.
      The new methods include:
      
          * filter() - applies select criterion to the query
          * filter_by() - applies "by"-style criterion to the query
          * avg() - return the avg() function on the given column
          * join() - join to a property (or across a list of properties)
          * outerjoin() - like join() but uses LEFT OUTER JOIN
          * limit()/offset() - apply LIMIT/OFFSET range-based access 
            which applies limit/offset: session.query(Foo)[3:5]
          * distinct() - apply DISTINCT
          * list() - evaluate the criterion and return results
      
      no incompatible changes have been made to Query's API and no methods
      have been deprecated.  Existing methods like select(), select_by(),
      get(), get_by() all execute the query at once and return results
      like they always did.  join_to()/join_via() are still there although
      the generative join()/outerjoin() methods are easier to use.

    .. change::
        :tags: orm
        :tickets: 

      the return value for multiple mappers used with instances() now
      returns a cartesian product of the requested list of mappers,
      represented as a list of tuples. this corresponds to the documented
      behavior. So that instances match up properly, the "uniquing" is
      disabled when this feature is used.

    .. change::
        :tags: orm
        :tickets: 

      Query has add_entity() and add_column() generative methods. these
      will add the given mapper/class or ColumnElement to the query at
      compile time, and apply them to the instances() method. the user is
      responsible for constructing reasonable join conditions (otherwise
      you can get full cartesian products). result set is the list of
      tuples, non-uniqued.

    .. change::
        :tags: orm
        :tickets: 

      strings and columns can also be sent to the \*args of instances()
      where those exact result columns will be part of the result tuples.

    .. change::
        :tags: orm
        :tickets: 

      a full select() construct can be passed to query.select() (which
      worked anyway), but also query.selectfirst(), query.selectone()
      which will be used as is (i.e. no query is compiled). works
      similarly to sending the results to instances().

    .. change::
        :tags: orm
        :tickets: 495

      eager loading will not "aliasize" "order by" clauses that were
      placed in the select statement by something other than the eager
      loader itself, to fix possibility of dupe columns as illustrated in. however, this means you have to be more careful with
      the columns placed in the "order by" of Query.select(), that you
      have explicitly named them in your criterion (i.e. you cant rely on
      the eager loader adding them in for you)

    .. change::
        :tags: orm
        :tickets: 

      added a handy multi-use "identity_key()" method to Session, allowing
      the generation of identity keys for primary key values, instances,
      and rows, courtesy Daniel Miller

    .. change::
        :tags: orm
        :tickets: 249

      many-to-many table will be properly handled even for operations that
      occur on the "backref" side of the operation

    .. change::
        :tags: orm
        :tickets: 492

      added "refresh-expire" cascade.  allows refresh() and
      expire() calls to propagate along relationships.

    .. change::
        :tags: orm
        :tickets: 493

      more fixes to polymorphic relations, involving proper lazy-clause
      generation on many-to-one relationships to polymorphic mappers. also fixes to detection of "direction", more specific
      targeting of columns that belong to the polymorphic union vs. those
      that don't.

    .. change::
        :tags: orm
        :tickets: 

      some fixes to relationship calcs when using "viewonly=True" to pull
      in other tables into the join condition which arent parent of the
      relationship's parent/child mappings

    .. change::
        :tags: orm
        :tickets: 

      flush fixes on cyclical-referential relationships that contain
      references to other instances outside of the cyclical chain, when
      some of the objects in the cycle are not actually part of the flush

    .. change::
        :tags: orm
        :tickets: 500

      put an aggressive check for "flushing object A with a collection of
      B's, but you put a C in the collection" error condition - **even if
      C is a subclass of B**, unless B's mapper loads polymorphically.
      Otherwise, the collection will later load a "B" which should be a
      "C" (since its not polymorphic) which breaks in bi-directional
      relationships (i.e. C has its A, but A's backref will lazyload it as
      a different instance of type "B") This check is going
      to bite some of you who do this without issues, so the error message
      will also document a flag "enable_typechecks=False" to disable this
      checking. But be aware that bi-directional relationships in
      particular become fragile without this check.

    .. change::
        :tags: extensions
        :tickets: 472

      options() method on SelectResults now implemented "generatively"
      like the rest of the SelectResults methods.  But
      you're going to just use Query now anyway.

    .. change::
        :tags: extensions
        :tickets: 

      query() method is added by assignmapper.  this helps with
      navigating to all the new generative methods on Query.

    .. change::
        :tags: ms-sql
        :tickets: 

      removed seconds input on DATE column types (probably
        should remove the time altogether)

    .. change::
        :tags: ms-sql
        :tickets: 

      null values in float fields no longer raise errors

    .. change::
        :tags: ms-sql
        :tickets: 

      LIMIT with OFFSET now raises an error (MS-SQL has no OFFSET support)

    .. change::
        :tags: ms-sql
        :tickets: 509

      added an facility to use the MSSQL type VARCHAR(max) instead of TEXT
      for large unsized string fields. Use the new "text_as_varchar" to
      turn it on.

    .. change::
        :tags: ms-sql
        :tickets: 

      ORDER BY clauses without a LIMIT are now stripped in subqueries, as
      MS-SQL forbids this usage

    .. change::
        :tags: ms-sql
        :tickets: 480

      cleanup of module importing code; specifiable DB-API module; more
      explicit ordering of module preferences.

    .. change::
        :tags: oracle
        :tickets: 

      got binary working for any size input !  cx_oracle works fine,
      it was my fault as BINARY was being passed and not BLOB for
      setinputsizes (also unit tests weren't even setting input sizes).

    .. change::
        :tags: oracle
        :tickets: 

      also fixed CLOB read/write on a separate changeset.

    .. change::
        :tags: oracle
        :tickets: 

      auto_setinputsizes defaults to True for Oracle, fixed cases where
      it improperly propagated bad types.

    .. change::
        :tags: mysql
        :tickets: 

      added a catchall \**kwargs to MSString, to help reflection of
      obscure types (like "varchar() binary" in MS 4.0)

    .. change::
        :tags: mysql
        :tickets: 

      added explicit MSTimeStamp type which takes effect when using
      types.TIMESTAMP.

.. changelog::
    :version: 0.3.5
    :released: Thu Feb 22 2007

    .. change::
        :tags: sql
        :tickets: 

      the value of "case_sensitive" defaults to True now, regardless of the
      casing of the identifier, unless specifically set to False. this is
      because the object might be label'ed as something else which does
      contain mixed case, and propigating "case_sensitive=False" breaks that.
      Other fixes to quoting when using labels and "fake" column objects

    .. change::
        :tags: sql
        :tickets: 

      added a "supports_execution()" method to ClauseElement, so that
      individual kinds of clauses can express if they are appropriate for
      executing...such as, you can execute a "select", but not a "Table" or a
      "Join".

    .. change::
        :tags: sql
        :tickets: 

      fixed argument passing to straight textual execute() on engine,
      connection. can handle \*args or a list instance for positional, \**kwargs
      or a dict instance for named args, or a list of list or dicts to invoke
      executemany()

    .. change::
        :tags: sql
        :tickets: 

      small fix to BoundMetaData to accept unicode or string URLs

    .. change::
        :tags: sql
        :tickets: 466

      fixed named PrimaryKeyConstraint generation courtesy
      andrija at gmail

    .. change::
        :tags: sql
        :tickets: 464

      fixed generation of CHECK constraints on columns

    .. change::
        :tags: sql
        :tickets: 

      fixes to tometadata() operation to propagate Constraints at column and
      table level

    .. change::
        :tags: oracle
        :tickets: 436

      when returning "rowid" as the ORDER BY column or in use with ROW_NUMBER
      OVER, oracle dialect checks the selectable its being applied to and will
      switch to table PK if not applicable, i.e. for a UNION. checking for
      DISTINCT, GROUP BY (other places that rowid is invalid) still a TODO.
      allows polymorphic mappings to function.

    .. change::
        :tags: oracle
        :tickets: 

      sequences on a non-pk column will properly fire off on INSERT

    .. change::
        :tags: oracle
        :tickets: 435

      added PrefetchingResultProxy support to pre-fetch LOB columns when they
      are known to be present, fixes

    .. change::
        :tags: oracle
        :tickets: 379

      implemented reflection of tables based on synonyms, including across
      dblinks

    .. change::
        :tags: oracle
        :tickets: 363

      issues a log warning when a related table cant be reflected due to
      certain permission errors

    .. change::
        :tags: mysql
        :tickets: 

      fix to reflection on older DB's that might return array() type for
      "show variables like" statements

    .. change::
        :tags: postgres
        :tickets: 442

      better reflection of sequences for alternate-schema Tables

    .. change::
        :tags: postgres
        :tickets: 

      sequences on a non-pk column will properly fire off on INSERT

    .. change::
        :tags: postgres
        :tickets: 460, 444

      added PGInterval type, PGInet type

    .. change::
        :tags: mssql
        :tickets: 419

      preliminary support for pyodbc (Yay!)

    .. change::
        :tags: mssql
        :tickets: 298

      better support for NVARCHAR types added

    .. change::
        :tags: mssql
        :tickets: 

      fix for commit logic on pymssql

    .. change::
        :tags: mssql
        :tickets: 456

      fix for query.get() with schema

    .. change::
        :tags: mssql
        :tickets: 473

      fix for non-integer relationships

    .. change::
        :tags: mssql
        :tickets: 419

      DB-API module now selectable at run-time

    .. change::
        :tags: tickets:422, 481, 415, mssql
        :tickets: 

      now passes many more unit tests

    .. change::
        :tags: mssql
        :tickets: 479

      better unittest compatibility with ANSI functions

    .. change::
        :tags: mssql
        :tickets: 415

      improved support for implicit sequence PK columns with auto-insert

    .. change::
        :tags: mssql
        :tickets: 371

      fix for blank password in adodbapi

    .. change::
        :tags: mssql
        :tickets: 481

      fixes to get unit tests working with pyodbc

    .. change::
        :tags: mssql
        :tickets: 

      fix to auto_identity_insert on db-url query

    .. change::
        :tags: mssql
        :tickets: 

      added query_timeout to db-url query parms. currently works only for
      pymssql

    .. change::
        :tags: mssql
        :tickets: 

      tested with pymssql 0.8.0 (which is now LGPL)

    .. change::
        :tags: orm, bugs
        :tickets: 441, 448, 439

      another refactoring to relationship calculation. Allows more accurate
      ORM behavior with relationships from/to/between mappers, particularly
      polymorphic mappers, also their usage with Query, SelectResults. tickets
      include,,.

    .. change::
        :tags: orm, bugs
        :tickets: 

      removed deprecated method of specifying custom collections on classes;
      you must now use the "collection_class" option. the old way was
      beginning to produce conflicts when people used assign_mapper(), which
      now patches an "options" method, in conjunction with a relationship
      named "options". (relationships take precedence over monkeypatched
      assign_mapper methods).

    .. change::
        :tags: orm, bugs
        :tickets: 454

      extension() query option propagates to Mapper._instance() method so that
      all loading-related methods get called

    .. change::
        :tags: orm, bugs
        :tickets: 

      eager relation to an inheriting mapper wont fail if no rows returned for
      the relationship.

    .. change::
        :tags: orm, bugs
        :tickets: 486

      eager relation loading bug fixed for eager relation on multiple
      descendant classes

    .. change::
        :tags: orm, bugs
        :tickets: 423

      fix for very large topological sorts, courtesy ants.aasma at gmail

    .. change::
        :tags: orm, bugs
        :tickets: 

      eager loading is slightly more strict about detecting "self-referential"
      relationships, specifically between polymorphic mappers. this results in
      an "eager degrade" to lazy loading.

    .. change::
        :tags: orm, bugs
        :tickets: 449

      improved support for complex queries embedded into "where" criterion for
      query.select()

    .. change::
        :tags: orm, bugs
        :tickets: 485

      mapper options like eagerload(), lazyload(), deferred(), will work for
      "synonym()" relationships

    .. change::
        :tags: orm, bugs
        :tickets: 445

      fixed bug where cascade operations incorrectly included deleted
      collection items in the cascade

    .. change::
        :tags: orm, bugs
        :tickets: 478

      fixed relationship deletion error when one-to-many child item is moved
      to a new parent in a single unit of work

    .. change::
        :tags: orm, bugs
        :tickets: 

      fixed relationship deletion error where parent/child with a single
      column as PK/FK on the child would raise a "blank out the primary key"
      error, if manually deleted or "delete" cascade without "delete-orphan"
      was used

    .. change::
        :tags: orm, bugs
        :tickets: 

      fix to deferred so that load operation doesn't mistakenly occur when only
      PK col attributes are set

    .. change::
        :tags: orm, enhancements
        :tickets: 385

      implemented foreign_keys argument to mapper. use in
      conjunction with primaryjoin/secondaryjoin arguments to specify/override
      foreign keys defined on the Table instance.

    .. change::
        :tags: orm, enhancements
        :tickets: 

      contains_eager('foo') automatically implies eagerload('foo')

    .. change::
        :tags: orm, enhancements
        :tickets: 

      added "alias" argument to contains_eager(). use it to specify the string
      name or Alias instance of an alias used in the query for the eagerly
      loaded child items. easier to use than "decorator"

    .. change::
        :tags: orm, enhancements
        :tickets: 

      added "contains_alias()" option for result set mapping to an alias of
      the mapped table

    .. change::
        :tags: orm, enhancements
        :tickets: 468

      added support for py2.5 "with" statement with SessionTransaction

    .. change::
        :tags: extensions
        :tickets: 

      added distinct() method to SelectResults. generally should only make a
      difference when using count().

    .. change::
        :tags: extensions
        :tickets: 472

      added options() method to SelectResults, equivalent to query.options()

    .. change::
        :tags: extensions
        :tickets: 462

      added optional __table_opts__ dictionary to ActiveMapper, will send kw
      options to Table objects

    .. change::
        :tags: extensions
        :tickets: 467

      added selectfirst(), selectfirst_by() to assign_mapper

.. changelog::
    :version: 0.3.4
    :released: Tue Jan 23 2007

    .. change::
        :tags: general
        :tickets: 

      global "insure"->"ensure" change. in US english "insure" is actually
      largely interchangeable with "ensure" (so says the dictionary), so I'm not
      completely illiterate, but its definitely sub-optimal to "ensure" which is
      non-ambiguous.

    .. change::
        :tags: sql
        :tickets: 

      added "fetchmany()" support to ResultProxy

    .. change::
        :tags: sql
        :tickets: 

      added support for column "key" attribute to be useable in
      row[<key>]/row.<key>

    .. change::
        :tags: sql
        :tickets: 

      changed "BooleanExpression" to subclass from "BinaryExpression", so that
      boolean expressions can also follow column-clause behaviors (i.e. label(),
      etc).

    .. change::
        :tags: sql
        :tickets: 

      trailing underscores are trimmed from func.<xxx> calls, such as func.if_()

    .. change::
        :tags: sql
        :tickets: 

      fix to correlation of subqueries when the column list of the select
      statement is constructed with individual calls to append_column(); this
      fixes an ORM bug whereby nested select statements were not getting
      correlated with the main select generated by the Query object.

    .. change::
        :tags: sql
        :tickets: 

      another fix to subquery correlation so that a subquery which has only one
      FROM element will *not* correlate that single element, since at least one
      FROM element is required in a query.

    .. change::
        :tags: sql
        :tickets: 414

      default "timezone" setting is now False. this corresponds to Python's
      datetime behavior as well as Postgres' timestamp/time types (which is the
      only timezone-sensitive dialect at the moment)

    .. change::
        :tags: sql
        :tickets: 

      the "op()" function is now treated as an "operation", rather than a
      "comparison". the difference is, an operation produces a BinaryExpression
      from which further operations can occur whereas comparison produces the
      more restrictive BooleanExpression

    .. change::
        :tags: sql
        :tickets: 

      trying to redefine a reflected primary key column as non-primary key raises
      an error

    .. change::
        :tags: sql
        :tickets: 

      type system slightly modified to support TypeDecorators that can be
      overridden by the dialect (ok, that's not very clear, it allows the mssql
      tweak below to be possible)

    .. change::
        :tags: mssql
        :tickets: 

      added an NVarchar type (produces NVARCHAR), also MSUnicode which provides
      Unicode-translation for the NVarchar regardless of dialect convert_unicode
      setting.

    .. change::
        :tags: postgres
        :tickets: 424

      fix to the initial checkfirst for tables to take current schema into
      account

    .. change::
        :tags: postgres
        :tickets: 

      postgres has an optional "server_side_cursors=True" flag which will utilize
      server side cursors. these are appropriate for fetching only partial
      results and are necessary for working with very large unbounded result
      sets. While we'd like this to be the default behavior, different
      environments seem to have different results and the causes have not been
      isolated so we are leaving the feature off by default for now. Uses an
      apparently undocumented psycopg2 behavior recently discovered on the
      psycopg mailing list.

    .. change::
        :tags: postgres
        :tickets: 

      added "BIGSERIAL" support for postgres table with
      PGBigInteger/autoincrement

    .. change::
        :tags: postgres
        :tickets: 402

      fixes to postgres reflection to better handle when schema names are
      present; thanks to jason (at) ncsmags.com

    .. change::
        :tags: mysql
        :tickets: 420

      mysql is inconsistent with what kinds of quotes it uses in foreign keys
      during a SHOW CREATE TABLE, reflection updated to accommodate for all three
      styles

    .. change::
        :tags: mysql
        :tickets: 418

      mysql table create options work on a generic passthru now, i.e. Table(...,
      mysql_engine='InnoDB', mysql_collate="latin1_german2_ci",
      mysql_auto_increment="5", mysql_<somearg>...), helps

    .. change::
        :tags: firebird
        :tickets: 408

      order of constraint creation puts primary key first before all other
      constraints; required for firebird, not a bad idea for others

    .. change::
        :tags: firebird
        :tickets: 409

      Firebird fix to autoload multifield foreign keys

    .. change::
        :tags: firebird
        :tickets: 409

      Firebird NUMERIC type properly handles a type without precision

    .. change::
        :tags: oracle
        :tickets: 

      *slight* support for binary, but still need to figure out how to insert
      reasonably large values (over 4K). requires auto_setinputsizes=True sent to
      create_engine(), rows must be fully fetched individually, etc.

    .. change::
        :tags: orm
        :tickets: 

      poked the first hole in the can of worms: saying
      query.select_by(somerelationname=someinstance) will create the join of the
      primary key columns represented by "somerelationname"'s mapper to the
      actual primary key in "someinstance".

    .. change::
        :tags: orm
        :tickets: 

      reworked how relations interact with "polymorphic" mappers, i.e. mappers
      that have a select_table as well as polymorphic flags. better determination
      of proper join conditions, interaction with user- defined join conditions,
      and support for self-referential polymorphic mappers.

    .. change::
        :tags: orm
        :tickets: 

      related to polymorphic mapping relations, some deeper error checking when
      compiling relations, to detect an ambiguous "primaryjoin" in the case that
      both sides of the relationship have foreign key references in the primary
      join condition. also tightened down conditions used to locate "relation
      direction", associating the "foreignkey" of the relationship with the
      "primaryjoin"

    .. change::
        :tags: orm
        :tickets: 

      a little bit of improvement to the concept of a "concrete" inheritance
      mapping, though that concept is not well fleshed out yet (added test case
      to support concrete mappers on top of a polymorphic base).

    .. change::
        :tags: orm
        :tickets: 

      fix to "proxy=True" behavior on synonym()

    .. change::
        :tags: orm
        :tickets: 427

      fixed bug where delete-orphan basically didn't work with many-to-many
      relationships, backref presence generally hid the symptom

    .. change::
        :tags: orm
        :tickets: 

      added a mutex to the mapper compilation step. ive been reluctant to add any
      kind of threading anything to SA but this is one spot that its really
      needed since mappers are typically "global", and while their state does not
      change during normal operation, the initial compilation step does modify
      internal state significantly, and this step usually occurs not at
      module-level initialization time (unless you call compile()) but at
      first-request time

    .. change::
        :tags: orm
        :tickets: 

      basic idea of "session.merge()" actually implemented.  needs more testing.

    .. change::
        :tags: orm
        :tickets: 

      added "compile_mappers()" function as a shortcut to compiling all mappers

    .. change::
        :tags: orm
        :tickets: 

      fix to MapperExtension create_instance so that entity_name properly
      associated with new instance

    .. change::
        :tags: orm
        :tickets: 

      speed enhancements to ORM object instantiation, eager loading of rows

    .. change::
        :tags: orm
        :tickets: 406

      invalid options sent to 'cascade' string will raise an exception

    .. change::
        :tags: orm
        :tickets: 407

      fixed bug in mapper refresh/expire whereby eager loaders didn't properly
      re-populate item lists

    .. change::
        :tags: orm
        :tickets: 413

      fix to post_update to ensure rows are updated even for non insert/delete
      scenarios

    .. change::
        :tags: orm
        :tickets: 412

      added an error message if you actually try to modify primary key values on
      an entity and then flush it

    .. change::
        :tags: extensions
        :tickets: 426

      added "validate=False" argument to assign_mapper, if True will ensure that
      only mapped attributes are named

    .. change::
        :tags: extensions
        :tickets: 

      assign_mapper gets "options", "instances" functions added (i.e.
      MyClass.instances())

.. changelog::
    :version: 0.3.3
    :released: Fri Dec 15 2006

    .. change::
        :tags: 
        :tickets: 

      string-based FROM clauses fixed, i.e. select(..., from_obj=["sometext"])

    .. change::
        :tags: 
        :tickets: 

      fixes to passive_deletes flag, lazy=None (noload) flag

    .. change::
        :tags: 
        :tickets: 

      added example/docs for dealing with large collections

    .. change::
        :tags: 
        :tickets: 

      added object_session() method to sqlalchemy namespace

    .. change::
        :tags: 
        :tickets: 

      fixed QueuePool bug whereby its better able to reconnect to a database
      that was not reachable (thanks to SÃ©bastien Lelong), also fixed dispose()
      method

    .. change::
        :tags: 
        :tickets: 396

      patch that makes MySQL rowcount work correctly!

    .. change::
        :tags: 
        :tickets: 

      fix to MySQL catch of 2006/2014 errors to properly re-raise OperationalError
      exception

.. changelog::
    :version: 0.3.2
    :released: Sun Dec 10 2006

    .. change::
        :tags: 
        :tickets: 387

      major connection pool bug fixed.  fixes MySQL out of sync
      errors, will also prevent transactions getting rolled back
      accidentally in all DBs

    .. change::
        :tags: 
        :tickets: 

      major speed enhancements vs. 0.3.1, to bring speed
      back to 0.2.8 levels

    .. change::
        :tags: 
        :tickets: 

      made conditional dozens of debug log calls that were
      time-intensive to generate log messages

    .. change::
        :tags: 
        :tickets: 

      fixed bug in cascade rules whereby the entire object graph
      could be unnecessarily cascaded on the save/update cascade

    .. change::
        :tags: 
        :tickets: 

      various speedups in attributes module

    .. change::
        :tags: 
        :tickets: 388

      identity map in Session is by default *no longer weak referencing*.
      to have it be weak referencing, use create_session(weak_identity_map=True)
      fixes

    .. change::
        :tags: 
        :tickets: 

      MySQL detects errors 2006 (server has gone away) and 2014
      (commands out of sync) and invalidates the connection on which it occurred.

    .. change::
        :tags: 
        :tickets: 307

      MySQL bool type fix:

    .. change::
        :tags: 
        :tickets: 382, 349

      postgres reflection fixes:

    .. change::
        :tags: 
        :tickets: 247

      added keywords for EXCEPT, INTERSECT, EXCEPT ALL, INTERSECT ALL

    .. change::
        :tags: 
        :tickets: 2110

      assign_mapper in assignmapper extension returns the created mapper

    .. change::
        :tags: 
        :tickets: 

      added label() function to Select class, when scalar=True is used
      to create a scalar subquery
      i.e. "select x, y, (select max(foo) from table) AS foomax from table"

    .. change::
        :tags: 
        :tickets: 

      added onupdate and ondelete keyword arguments to ForeignKey; propagate
      to underlying ForeignKeyConstraint if present.  (don't propagate in the
      other direction, however)

    .. change::
        :tags: 
        :tickets: 

      fix to session.update() to preserve "dirty" status of incoming object

    .. change::
        :tags: 
        :tickets: 

      sending a selectable to an IN via the in_() function no longer creates
      a "union" out of multiple selects; only one selectable to a the in_() function
      is allowed now (make a union yourself if union is needed)

    .. change::
        :tags: 
        :tickets: 

      improved support for disabling save-update cascade via cascade="none" etc.

    .. change::
        :tags: 
        :tickets: 

      added "remote_side" argument to relation(), used only with self-referential
      mappers to force the direction of the parent/child relationship.  replaces
      the usage of the "foreignkey" parameter for "switching" the direction.
      "foreignkey" argument is deprecated for all uses and will eventually
      be replaced by an argument dedicated to ForeignKey specification on mappers.

.. changelog::
    :version: 0.3.1
    :released: Mon Nov 13 2006

    .. change::
        :tags: engine/pool
        :tickets: 

      some new Pool utility classes, updated docs

    .. change::
        :tags: engine/pool
        :tickets: 

      "use_threadlocal" on Pool defaults to False (same as create_engine)

    .. change::
        :tags: engine/pool
        :tickets: 

      fixed direct execution of Compiled objects

    .. change::
        :tags: engine/pool
        :tickets: 

      create_engine() reworked to be strict about incoming \**kwargs.  all keyword
      arguments must be consumed by one of the dialect, connection pool, and engine
      constructors, else a TypeError is thrown which describes the full set of
      invalid kwargs in relation to the selected dialect/pool/engine configuration.

    .. change::
        :tags: databases/types
        :tickets: 

      MySQL catches exception on "describe" and reports as NoSuchTableError

    .. change::
        :tags: databases/types
        :tickets: 

      further fixes to sqlite booleans, weren't working as defaults

    .. change::
        :tags: databases/types
        :tickets: 

      fix to postgres sequence quoting when using schemas

    .. change::
        :tags: orm
        :tickets: 

      the "delete" cascade will load in all child objects, if they were not
      loaded already.  this can be turned off (i.e. the old behavior) by setting
      passive_deletes=True on a relation().

    .. change::
        :tags: orm
        :tickets: 

      adjustments to reworked eager query generation to not fail on circular
      eager-loaded relationships (like backrefs)

    .. change::
        :tags: orm
        :tickets: 

      fixed bug where eagerload() (nor lazyload()) option didn't properly
      instruct the Query whether or not to use "nesting" when producing a
      LIMIT query.

    .. change::
        :tags: orm
        :tickets: 360

      fixed bug in circular dependency sorting at flush time; if object A
      contained a cyclical many-to-one relationship to object B, and object B
      was just attached to object A, *but* object B itself wasn't changed,
      the many-to-one synchronize of B's primary key attribute to A's foreign key
      attribute wouldn't occur.

    .. change::
        :tags: orm
        :tickets: 325

      implemented from_obj argument for query.count, improves count function
      on selectresults

    .. change::
        :tags: orm
        :tickets: 

      added an assertion within the "cascade" step of ORM relationships to check
      that the class of object attached to a parent object is appropriate
      (i.e. if A.items stores B objects, raise an error if a C is appended to A.items)

    .. change::
        :tags: orm
        :tickets: 

      new extension sqlalchemy.ext.associationproxy, provides transparent
      "association object" mappings.  new example
      examples/association/proxied_association.py illustrates.

    .. change::
        :tags: orm
        :tickets: 

      improvement to single table inheritance to load full hierarchies beneath
      the target class

    .. change::
        :tags: orm
        :tickets: 362

      fix to subtle condition in topological sort where a node could appear twice,
      for

    .. change::
        :tags: orm
        :tickets: 365

      additional rework to topological sort, refactoring, for

    .. change::
        :tags: orm
        :tickets: 

      "delete-orphan" for a certain type can be set on more than one parent class;
      the instance is an "orphan" only if its not attached to *any* of those parents

.. changelog::
    :version: 0.3.0
    :released: Sun Oct 22 2006

    .. change::
        :tags: general
        :tickets: 

      logging is now implemented via standard python "logging" module.
      "echo" keyword parameters are still functional but set/unset
      log levels for their respective classes/instances.  all logging
      can be controlled directly through the Python API by setting
      INFO and DEBUG levels for loggers in the "sqlalchemy" namespace.
      class-level logging is under "sqlalchemy.<module>.<classname>",
      instance-level logging under "sqlalchemy.<module>.<classname>.0x..<00-FF>".
      Test suite includes "--log-info" and "--log-debug" arguments
      which work independently of --verbose/--quiet.  Logging added
      to orm to allow tracking of mapper configurations, row iteration.

    .. change::
        :tags: general
        :tickets: 

      the documentation-generation system has been overhauled to be
      much simpler in design and more integrated with Markdown

    .. change::
        :tags: sqlite
        :tickets: 

      sqlite boolean datatype converts False/True to 0/1 by default

    .. change::
        :tags: sqlite
        :tickets: 335

      fixes to Date/Time (SLDate/SLTime) types; works as good as postgres
      now

    .. change::
        :tags: ms-sql
        :tickets: 

      fixes bug 261 (table reflection broken for MS-SQL case-sensitive
      databases)

    .. change::
        :tags: ms-sql
        :tickets: 

      can now specify port for pymssql

    .. change::
        :tags: ms-sql
        :tickets: 

      introduces new "auto_identity_insert" option for auto-switching
      between "SET IDENTITY_INSERT" mode when values specified for IDENTITY columns

    .. change::
        :tags: ms-sql
        :tickets: 

      now supports multi-column foreign keys

    .. change::
        :tags: ms-sql
        :tickets: 

      fix to reflecting date/datetime columns

    .. change::
        :tags: ms-sql
        :tickets: 

      NCHAR and NVARCHAR type support added

    .. change::
        :tags: oracle
        :tickets: 

      Oracle has experimental support for cx_Oracle.TIMESTAMP, which requires
      a setinputsizes() call on the cursor that is now enabled via the
      'auto_setinputsizes' flag to the oracle dialect.

    .. change::
        :tags: firebird
        :tickets: 

      aliases do not use "AS"

    .. change::
        :tags: firebird
        :tickets: 

      correctly raises NoSuchTableError when reflecting non-existent table

    .. change::
        :tags: schema
        :tickets: 

      a fair amount of cleanup to the schema package, removal of ambiguous
      methods, methods that are no longer needed.  slightly more constrained
      usage, greater emphasis on explicitness

    .. change::
        :tags: schema
        :tickets: 

      the "primary_key" attribute of Table and other selectables becomes
      a setlike ColumnCollection object; is ordered but not numerically
      indexed.  a comparison clause between two pks that are derived from the
      same underlying tables (i.e. such as two Alias objects) can be generated
      via table1.primary_key==table2.primary_key

    .. change::
        :tags: schema
        :tickets: 

      ForeignKey(Constraint) supports "use_alter=True", to create/drop a foreign key
      via ALTER.  this allows circular foreign key relationships to be set up.

    .. change::
        :tags: schema
        :tickets: 

      append_item() methods removed from Table and Column; preferably
      construct Table/Column/related objects inline, but if needed use
      append_column(), append_foreign_key(), append_constraint(), etc.

    .. change::
        :tags: schema
        :tickets: 

      table.create() no longer returns the Table object, instead has no
      return value.  the usual case is that tables are created via metadata,
      which is preferable since it will handle table dependencies.

    .. change::
        :tags: schema
        :tickets: 

      added UniqueConstraint (goes at Table level), CheckConstraint
      (goes at Table or Column level).

    .. change::
        :tags: schema
        :tickets: 

      index=False/unique=True on Column now creates a UniqueConstraint,
      index=True/unique=False creates a plain Index,
      index=True/unique=True on Column creates a unique Index.  'index'
      and 'unique' keyword arguments to column are now boolean only; for
      explcit names and groupings of indexes or unique constraints, use the
      UniqueConstraint/Index constructs explicitly.

    .. change::
        :tags: schema
        :tickets: 

      added autoincrement=True to Column; will disable schema generation
      of SERIAL/AUTO_INCREMENT/identity seq for postgres/mysql/mssql if
      explicitly set to False

    .. change::
        :tags: schema
        :tickets: 

      TypeEngine objects now have methods to deal with copying and comparing
      values of their specific type.  Currently used by the ORM, see below.

    .. change::
        :tags: schema
        :tickets: 

      fixed condition that occurred during reflection when a primary key
      column was explciitly overridden, where the PrimaryKeyConstraint would
      get both the reflected and the programmatic column doubled up

    .. change::
        :tags: schema
        :tickets: 

      the "foreign_key" attribute on Column and ColumnElement in general
      is deprecated, in favor of the "foreign_keys" list/set-based attribute,
      which takes into account multiple foreign keys on one column.
      "foreign_key" will return the first element in the "foreign_keys" list/set
      or None if the list is empty.

    .. change::
        :tags: connections/pooling/execution
        :tickets: 

      connection pool tracks open cursors and automatically closes them
      if connection is returned to pool with cursors still opened.  Can be
      affected by options which cause it to raise an error instead, or to
      do nothing.  fixes issues with MySQL, others

    .. change::
        :tags: connections/pooling/execution
        :tickets: 

      fixed bug where Connection wouldn't lose its Transaction
      after commit/rollback

    .. change::
        :tags: connections/pooling/execution
        :tickets: 

      added scalar() method to ComposedSQLEngine, ResultProxy

    .. change::
        :tags: connections/pooling/execution
        :tickets: 

      ResultProxy will close() the underlying cursor when the ResultProxy
      itself is closed.  this will auto-close cursors for ResultProxy objects
      that have had all their rows fetched (or had scalar() called).

    .. change::
        :tags: connections/pooling/execution
        :tickets: 

      ResultProxy.fetchall() internally uses DBAPI fetchall() for better efficiency,
      added to mapper iteration as well (courtesy Michael Twomey)

    .. change::
        :tags: construction, sql
        :tickets: 292

      changed "for_update" parameter to accept False/True/"nowait"
      and "read", the latter two of which are interpreted only by
      Oracle and Mysql

    .. change::
        :tags: construction, sql
        :tickets: 

      added extract() function to sql dialect
      (SELECT extract(field FROM expr))

    .. change::
        :tags: construction, sql
        :tickets: 

      BooleanExpression includes new "negate" argument to specify
      the appropriate negation operator if one is available.

    .. change::
        :tags: construction, sql
        :tickets: 

      calling a negation on an "IN" or "IS" clause will result in
      "NOT IN", "IS NOT" (as opposed to NOT (x IN y)).

    .. change::
        :tags: construction, sql
        :tickets: 172

      Function objects know what to do in a FROM clause now.  their
      behavior should be the same, except now you can also do things like
      select(['*'], from_obj=[func.my_function()]) to get multiple
      columns from the result, or even use sql.column() constructs to name the
      return columns

    .. change::
        :tags: orm
        :tickets: 

      attribute tracking modified to be more intelligent about detecting
      changes, particularly with mutable types.  TypeEngine objects now
      take a greater role in defining how to compare two scalar instances,
      including the addition of a MutableType mixin which is implemented by
      PickleType.  unit-of-work now tracks the "dirty" list as an expression
      of all persistent objects where the attribute manager detects changes.
      The basic issue that's fixed is detecting changes on PickleType
      objects, but also generalizes type handling and "modified" object
      checking to be more complete and extensible.

    .. change::
        :tags: orm
        :tickets: 

      a wide refactoring to "attribute loader" and "options" architectures.
      ColumnProperty and PropertyLoader define their loading behaivor via switchable
      "strategies", and MapperOptions no longer use mapper/property copying
      in order to function; they are instead propagated via QueryContext
      and SelectionContext objects at query/instances time.
      All of the internal copying of mappers and properties that was used to handle
      inheritance as well as options() has been removed; the structure
      of mappers and properties is much simpler than before and is clearly laid out
      in the new 'interfaces' module.

    .. change::
        :tags: orm
        :tickets: 

      related to the mapper/property overhaul, internal refactoring to
      mapper instances() method to use a SelectionContext object to track
      state during the operation.
      SLIGHT API BREAKAGE: the append_result() and populate_instances()
      methods on MapperExtension have a slightly different method signature
      now as a result of the change; hoping that these methods are not
      in widespread use as of yet.

    .. change::
        :tags: orm
        :tickets: 

      instances() method moved to Query now, backwards-compatible
      version remains on Mapper.

    .. change::
        :tags: orm
        :tickets: 

      added contains_eager() MapperOption, used in conjunction with
      instances() to specify properties that should be eagerly loaded
      from the result set, using their plain column names by default, or translated
      given an custom row-translation function.

    .. change::
        :tags: orm
        :tickets: 

      more rearrangements of unit-of-work commit scheme to better allow
      dependencies within circular flushes to work properly...updated
      task traversal/logging implementation

    .. change::
        :tags: orm
        :tickets: 321

      polymorphic mappers (i.e. using inheritance) now produces INSERT
      statements in order of tables across all inherited classes

    .. change::
        :tags: orm
        :tickets: 

      added an automatic "row switch" feature to mapping, which will
      detect a pending instance/deleted instance pair with the same
      identity key and convert the INSERT/DELETE to a single UPDATE

    .. change::
        :tags: orm
        :tickets: 

      "association" mappings simplified to take advantage of
      automatic "row switch" feature

    .. change::
        :tags: orm
        :tickets: 212

      "custom list classes" is now implemented via the "collection_class"
      keyword argument to relation().  the old way still works but is
      deprecated

    .. change::
        :tags: orm
        :tickets: 

      added "viewonly" flag to relation(), allows construction of
      relations that have no effect on the flush() process.

    .. change::
        :tags: orm
        :tickets: 292

      added "lockmode" argument to base Query select/get functions,
      including "with_lockmode" function to get a Query copy that has
      a default locking mode.  Will translate "read"/"update"
      arguments into a for_update argument on the select side.

    .. change::
        :tags: orm
        :tickets: 

      implemented "version check" logic in Query/Mapper, used
      when version_id_col is in effect and query.with_lockmode()
      is used to get() an instance that's already loaded

    .. change::
        :tags: orm
        :tickets: 208

      post_update behavior improved; does a better job at not
      updating too many rows, updates only required columns

    .. change::
        :tags: orm
        :tickets: 308

      adjustments to eager loading so that its "eager chain" is
      kept separate from the normal mapper setup, thereby
      preventing conflicts with lazy loader operation, fixes

    .. change::
        :tags: orm
        :tickets: 

      fix to deferred group loading

    .. change::
        :tags: orm
        :tickets: 346

      session.flush() wont close a connection it opened

    .. change::
        :tags: orm
        :tickets: 

      added "batch=True" flag to mapper; if False, save_obj
      will fully save one object at a time including calls
      to before_XXXX and after_XXXX

    .. change::
        :tags: orm
        :tickets: 

      added "column_prefix=None" argument to mapper; prepends the
      given string (typically '_') to column-based attributes automatically
      set up from the mapper's Table

    .. change::
        :tags: orm
        :tickets: 315

      specifying joins in the from_obj argument of query.select() will
      replace the main table of the query, if the table is somewhere within
      the given from_obj.  this makes it possible to produce custom joins and
      outerjoins in queries without the main table getting added twice.

    .. change::
        :tags: orm
        :tickets: 

      eagerloading is adjusted to more thoughtfully attach its LEFT OUTER JOINs
      to the given query, looking for custom "FROM" clauses that may have
      already been set up.

    .. change::
        :tags: orm
        :tickets: 

      added join_to and outerjoin_to transformative methods to SelectResults,
      to build up join/outerjoin conditions based on property names. also
      added select_from to explicitly set from_obj parameter.

    .. change::
        :tags: orm
        :tickets: 

      removed "is_primary" flag from mapper.
