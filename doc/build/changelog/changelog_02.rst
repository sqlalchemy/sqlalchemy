
==============
0.2 Changelog
==============


.. changelog::
    :version: 0.2.8
    :released: Tue Sep 05 2006

    .. change::
        :tags:
        :tickets:

      cleanup on connection methods + documentation.  custom DBAPI
      arguments specified in query string, 'connect_args' argument
      to 'create_engine', or custom creation function via 'creator'
      function to 'create_engine'.

    .. change::
        :tags:
        :tickets: 274

      added "recycle" argument to Pool, is "pool_recycle" on create_engine,
      defaults to 3600 seconds; connections after this age will be closed and
      replaced with a new one, to handle db's that automatically close
      stale connections

    .. change::
        :tags:
        :tickets: 121

      changed "invalidate" semantics with pooled connection; will
      instruct the underlying connection record to reconnect the next
      time its called.  "invalidate" will also automatically be called
      if any error is thrown in the underlying call to connection.cursor().
      this will hopefully allow the connection pool to reconnect to a
      database that had been stopped and started without restarting
      the connecting application

    .. change::
        :tags:
        :tickets:

      eesh !  the tutorial doctest was broken for quite some time.

    .. change::
        :tags:
        :tickets:

      add_property() method on mapper does a "compile all mappers"
      step in case the given property references a non-compiled mapper
      (as it did in the case of the tutorial !)

    .. change::
        :tags:
        :tickets: 277

      check for pg sequence already existing before create

    .. change::
        :tags:
        :tickets:

      if a contextual session is established via MapperExtension.get_session
      (as it is using the sessioncontext plugin, etc), a lazy load operation
      will use that session by default if the parent object is not
      persistent with a session already.

    .. change::
        :tags:
        :tickets:

      lazy loads will not fire off for an object that does not have a
      database identity (why?
      see http://www.sqlalchemy.org/trac/wiki/WhyDontForeignKeysLoadData)

    .. change::
        :tags:
        :tickets:

      unit-of-work does a better check for "orphaned" objects that are
      part of a "delete-orphan" cascade, for certain conditions where the
      parent isn't available to cascade from.

    .. change::
        :tags:
        :tickets:

      mappers can tell if one of their objects is an "orphan" based
      on interactions with the attribute package. this check is based
      on a status flag maintained for each relationship
      when objects are attached and detached from each other.

    .. change::
        :tags:
        :tickets:

      it is now invalid to declare a self-referential relationship with
      "delete-orphan" (as the abovementioned check would make them impossible
      to save)

    .. change::
        :tags:
        :tickets:

      improved the check for objects being part of a session when the
      unit of work seeks to flush() them as part of a relationship..

    .. change::
        :tags:
        :tickets: 280

      statement execution supports using the same BindParam
      object more than once in an expression; simplified handling of positional
      parameters.  nice job by Bill Noon figuring out the basic idea.

    .. change::
        :tags:
        :tickets: 60, 71

      postgres reflection moved to use pg_schema tables, can be overridden
      with use_information_schema=True argument to create_engine.

    .. change::
        :tags:
        :tickets: 155

      added case_sensitive argument to MetaData, Table, Column, determines
      itself automatically based on if a parent schemaitem has a non-None
      setting for the flag, or if not, then whether the identifier name is all lower
      case or not.  when set to True, quoting is applied to identifiers with mixed or
      uppercase identifiers.  quoting is also applied automatically in all cases to
      identifiers that are known to be reserved words or contain other non-standard
      characters. various database dialects can override all of this behavior, but
      currently they are all using the default behavior.  tested with postgres, mysql,
      sqlite, oracle.  needs more testing with firebird, ms-sql. part of the ongoing
      work with

    .. change::
        :tags:
        :tickets:

      unit tests updated to run without any pysqlite installed; pool
      test uses a mock DBAPI

    .. change::
        :tags:
        :tickets: 281

      urls support escaped characters in passwords

    .. change::
        :tags:
        :tickets:

      added limit/offset to UNION queries (though not yet in oracle)

    .. change::
        :tags:
        :tickets:

      added "timezone=True" flag to DateTime and Time types.  postgres
      so far will convert this to "TIME[STAMP] (WITH|WITHOUT) TIME ZONE",
      so that control over timezone presence is more controllable (psycopg2
      returns datetimes with tzinfo's if available, which can create confusion
      against datetimes that don't).

    .. change::
        :tags:
        :tickets: 287

      fix to using query.count() with distinct, \**kwargs with SelectResults
      count()

    .. change::
        :tags:
        :tickets: 289

      deregister Table from MetaData when autoload fails;

    .. change::
        :tags:
        :tickets: 293

      import of py2.5s sqlite3

    .. change::
        :tags:
        :tickets: 296

      unicode fix for startswith()/endswith()

.. changelog::
    :version: 0.2.7
    :released: Sat Aug 12 2006

    .. change::
        :tags:
        :tickets:

      quoting facilities set up so that database-specific quoting can be
      turned on for individual table, schema, and column identifiers when
      used in all queries/creates/drops.  Enabled via "quote=True" in
      Table or Column, as well as "quote_schema=True" in Table.  Thanks to
      Aaron Spike for the excellent efforts.

    .. change::
        :tags:
        :tickets:

      assignmapper was setting is_primary=True, causing all sorts of mayhem
      by not raising an error when redundant mappers were set up, fixed

    .. change::
        :tags:
        :tickets:

      added allow_null_pks option to Mapper, allows rows where some
      primary key columns are null (i.e. when mapping to outer joins etc)

    .. change::
        :tags:
        :tickets:

      modifcation to unitofwork to not maintain ordering within the
      "new" list or within the UOWTask "objects" list; instead, new objects
      are tagged with an ordering identifier as they are registered as new
      with the session, and the INSERT statements are then sorted within the
      mapper save_obj.  the INSERT ordering has basically been pushed all
      the way to the end of the flush cycle. that way the various sorts and
      organizations occurring within UOWTask (particularly the circular task
      sort) don't have to worry about maintaining order (which they weren't anyway)

    .. change::
        :tags:
        :tickets:

      fixed reflection of foreign keys to autoload the referenced table
      if it was not loaded already

    .. change::
        :tags:
        :tickets: 256

      - pass URL query string arguments to connect() function

    .. change::
        :tags:
        :tickets: 257

      - oracle boolean type

    .. change::
        :tags:
        :tickets:

      custom primary/secondary join conditions in a relation *will* be propagated
      to backrefs by default.  specifying a backref() will override this behavior.

    .. change::
        :tags:
        :tickets:

      better check for ambiguous join conditions in sql.Join; propagates to a
      better error message in PropertyLoader (i.e. relation()/backref()) for when
      the join condition can't be reasonably determined.

    .. change::
        :tags:
        :tickets:

      sqlite creates ForeignKeyConstraint objects properly upon table
      reflection.

    .. change::
        :tags:
        :tickets: 224

      adjustments to pool stemming from changes made for.
      overflow counter should only be decremented if the connection actually
      succeeded.  added a test script to attempt testing this.

    .. change::
        :tags:
        :tickets:

      fixed mysql reflection of default values to be PassiveDefault

    .. change::
        :tags:
        :tickets: 263, 264

      added reflected 'tinyint', 'mediumint' type to MS-SQL.

    .. change::
        :tags:
        :tickets:

      SingletonThreadPool has a size and does a cleanup pass, so that
      only a given number of thread-local connections stay around (needed
      for sqlite applications that dispose of threads en masse)

    .. change::
        :tags:
        :tickets: 267, 265

      fixed small pickle bug(s) with lazy loaders

    .. change::
        :tags:
        :tickets:

      fixed possible error in mysql reflection where certain versions
      return an array instead of string for SHOW CREATE TABLE call

    .. change::
        :tags:
        :tickets: 1770

      fix to lazy loads when mapping to joins

    .. change::
        :tags:
        :tickets:

      all create()/drop() calls have a keyword argument of "connectable".
      "engine" is deprecated.

    .. change::
        :tags:
        :tickets:

      fixed ms-sql connect() to work with adodbapi

    .. change::
        :tags:
        :tickets:

      added "nowait" flag to Select()

    .. change::
        :tags:
        :tickets: 271

      inheritance check uses issubclass() instead of direct __mro__ check
      to make sure class A inherits from B, allowing mapper inheritance to more
      flexibly correspond to class inheritance

    .. change::
        :tags:
        :tickets: 252

      SelectResults will use a subselect, when calling an aggregate (i.e.
      max, min, etc.) on a SelectResults that has an ORDER BY clause

    .. change::
        :tags:
        :tickets: 269

      fixes to types so that database-specific types more easily used;
      fixes to mysql text types to work with this methodology

    .. change::
        :tags:
        :tickets:

      some fixes to sqlite date type organization

    .. change::
        :tags:
        :tickets: 263

      added MSTinyInteger to MS-SQL

.. changelog::
    :version: 0.2.6
    :released: Thu Jul 20 2006

    .. change::
        :tags:
        :tickets: 76

      big overhaul to schema to allow truly composite primary and foreign
      key constraints, via new ForeignKeyConstraint and PrimaryKeyConstraint
      objects.
      Existing methods of primary/foreign key creation have not been changed
      but use these new objects behind the scenes.  table creation
      and reflection is now more table oriented rather than column oriented.

    .. change::
        :tags:
        :tickets:

      overhaul to MapperExtension calling scheme, wasn't working very well
      previously

    .. change::
        :tags:
        :tickets:

      tweaks to ActiveMapper, supports self-referential relationships

    .. change::
        :tags:
        :tickets:

      slight rearrangement to objectstore (in activemapper/threadlocal)
      so that the SessionContext is referenced by '.context' instead
      of subclassed directly.

    .. change::
        :tags:
        :tickets:

      activemapper will use threadlocal's objectstore if the mod is
      activated when activemapper is imported

    .. change::
        :tags:
        :tickets:

      small fix to URL regexp to allow filenames with '@' in them

    .. change::
        :tags:
        :tickets:

      fixes to Session expunge/update/etc...needs more cleanup.

    .. change::
        :tags:
        :tickets:

      select_table mappers *still* weren't always compiling

    .. change::
        :tags:
        :tickets:

      fixed up Boolean datatype

    .. change::
        :tags:
        :tickets:

      added count()/count_by() to list of methods proxied by assignmapper;
      this also adds them to activemapper

    .. change::
        :tags:
        :tickets:

      connection exceptions wrapped in DBAPIError

    .. change::
        :tags:
        :tickets:

      ActiveMapper now supports autoloading column definitions from the
      database if you supply a __autoload__ = True attribute in your
      mapping inner-class.  Currently this does not support reflecting
      any relationships.

    .. change::
        :tags:
        :tickets:

      deferred column load could screw up the connection status in
      a flush() under some circumstances, this was fixed

    .. change::
        :tags:
        :tickets:

      expunge() was not working with cascade, fixed.

    .. change::
        :tags:
        :tickets:

      potential endless loop in cascading operations fixed.

    .. change::
        :tags:
        :tickets:

      added "synonym()" function, applied to properties to have a
      propname the same as another, for the purposes of overriding props
      and allowing the original propname to be accessible in select_by().

    .. change::
        :tags:
        :tickets:

      fix to typing in clause construction which specifically helps
      type issues with polymorphic_union (CAST/ColumnClause propagates
      its type to proxy columns)

    .. change::
        :tags:
        :tickets:

      mapper compilation work ongoing, someday it'll work....moved
      around the initialization of MapperProperty objects to be after
      all mappers are created to better handle circular compilations.
      do_init() method is called on all properties now which are more
      aware of their "inherited" status if so.

    .. change::
        :tags:
        :tickets:

      eager loads explicitly disallowed on self-referential relationships, or
      relationships to an inheriting mapper (which is also self-referential)

    .. change::
        :tags:
        :tickets: 244

      reduced bind param size in query._get to appease the picky oracle

    .. change::
        :tags:
        :tickets: 234

      added 'checkfirst' argument to table.create()/table.drop(), as
      well as table.exists()

    .. change::
        :tags:
        :tickets: 245

      some other ongoing fixes to inheritance

    .. change::
        :tags:
        :tickets:

      attribute/backref/orphan/history-tracking tweaks as usual...

.. changelog::
    :version: 0.2.5
    :released: Sat Jul 08 2006

    .. change::
        :tags:
        :tickets:

      fixed endless loop bug in select_by(), if the traversal hit
      two mappers that referenced each other

    .. change::
        :tags:
        :tickets:

      upgraded all unittests to insert './lib/' into sys.path,
      working around new setuptools PYTHONPATH-killing behavior

    .. change::
        :tags:
        :tickets:

      further fixes with attributes/dependencies/etc....

    .. change::
        :tags:
        :tickets:

      improved error handling for when DynamicMetaData is not connected

    .. change::
        :tags:
        :tickets:

      MS-SQL support largely working (tested with pymssql)

    .. change::
        :tags:
        :tickets:

      ordering of UPDATE and DELETE statements within groups is now
      in order of primary key values, for more deterministic ordering

    .. change::
        :tags:
        :tickets:

      after_insert/delete/update mapper extensions now called per object,
      not per-object-per-table

    .. change::
        :tags:
        :tickets:

      further fixes/refactorings to mapper compilation

.. changelog::
    :version: 0.2.4
    :released: Tue Jun 27 2006

    .. change::
        :tags:
        :tickets:

      try/except when the mapper sets init.__name__ on a mapped class,
      supports python 2.3

    .. change::
        :tags:
        :tickets:

      fixed bug where threadlocal engine would still autocommit
      despite a transaction in progress

    .. change::
        :tags:
        :tickets:

      lazy load and deferred load operations require the parent object
      to be in a Session to do the operation; whereas before the operation
      would just return a blank list or None, it now raises an exception.

    .. change::
        :tags:
        :tickets:

      Session.update() is slightly more lenient if the session to which
      the given object was formerly attached to was garbage collected;
      otherwise still requires you explicitly remove the instance from
      the previous Session.

    .. change::
        :tags:
        :tickets:

      fixes to mapper compilation, checking for more error conditions

    .. change::
        :tags:
        :tickets:

      small fix to eager loading combined with ordering/limit/offset

    .. change::
        :tags:
        :tickets: 206

      utterly remarkable:  added a single space between 'CREATE TABLE'
      and '(<the rest of it>' since *that's how MySQL indicates a non-
      reserved word tablename.....*

    .. change::
        :tags:
        :tickets:

      more fixes to inheritance, related to many-to-many relations
      properly saving

    .. change::
        :tags:
        :tickets:

      fixed bug when specifying explicit module to mysql dialect

    .. change::
        :tags:
        :tickets:

      when QueuePool times out it raises a TimeoutError instead of
      erroneously making another connection

    .. change::
        :tags:
        :tickets:

      Queue.Queue usage in pool has been replaced with a locally
      modified version (works in py2.3/2.4!) that uses a threading.RLock
      for a mutex.  this is to fix a reported case where a ConnectionFairy's
      __del__() method got called within the Queue's get() method, which
      then returns its connection to the Queue via the put() method,
      causing a reentrant hang unless threading.RLock is used.

    .. change::
        :tags:
        :tickets:

      postgres will not place SERIAL keyword on a primary key column
      if it has a foreign key constraint

    .. change::
        :tags:
        :tickets: 221

      cursor() method on ConnectionFairy allows db-specific extension
      arguments to be propagated

    .. change::
        :tags:
        :tickets: 225

      lazy load bind params properly propagate column type

    .. change::
        :tags:
        :tickets:

      new MySQL types: MSEnum, MSTinyText, MSMediumText, MSLongText, etc.
      more support for MS-specific length/precision params in numeric types
      patch courtesy Mike Bernson

    .. change::
        :tags:
        :tickets: 224

      some fixes to connection pool invalidate()

.. changelog::
    :version: 0.2.3
    :released: Sat Jun 17 2006

    .. change::
        :tags:
        :tickets:

      overhaul to mapper compilation to be deferred.  this allows mappers
      to be constructed in any order, and their relationships to each
      other are compiled when the mappers are first used.

    .. change::
        :tags:
        :tickets:

      fixed a pretty big speed bottleneck in cascading behavior particularly
      when backrefs were in use

    .. change::
        :tags:
        :tickets:

      the attribute instrumentation module has been completely rewritten; its
      now a large degree simpler and clearer, slightly faster.  the "history"
      of an attribute is no longer micromanaged with each change and is
      instead part of a "CommittedState" object created when the
      instance is first loaded.  HistoryArraySet is gone, the behavior of
      list attributes is now more open ended (i.e. they're not sets anymore).

    .. change::
        :tags:
        :tickets:

      py2.4 "set" construct used internally, falls back to sets.Set when
      "set" not available/ordering is needed.

    .. change::
        :tags:
        :tickets:

      fix to transaction control, so that repeated rollback() calls
      don't fail (was failing pretty badly when flush() would raise
      an exception in a larger try/except transaction block)

    .. change::
        :tags:
        :tickets: 151

      "foreignkey" argument to relation() can also be a list.  fixed
      auto-foreignkey detection

    .. change::
        :tags:
        :tickets:

      fixed bug where tables with schema names weren't getting indexed in
      the MetaData object properly

    .. change::
        :tags:
        :tickets: 207

      fixed bug where Column with redefined "key" property wasn't getting
      type conversion happening in the ResultProxy

    .. change::
        :tags:
        :tickets:

      fixed 'port' attribute of URL to be an integer if present

    .. change::
        :tags:
        :tickets:

      fixed old bug where if a many-to-many table mapped as "secondary"
      had extra columns, delete operations didn't work

    .. change::
        :tags:
        :tickets:

      bugfixes for mapping against UNION queries

    .. change::
        :tags:
        :tickets:

      fixed incorrect exception class thrown when no DB driver present

    .. change::
        :tags:
        :tickets: 138

      added NonExistentTable exception thrown when reflecting a table
      that doesn't exist

    .. change::
        :tags:
        :tickets:

      small fix to ActiveMapper regarding one-to-one backrefs, other
      refactorings

    .. change::
        :tags:
        :tickets:

      overridden constructor in mapped classes gets __name__ and
      __doc__ from the original class

    .. change::
        :tags:
        :tickets: 200

      fixed small bug in selectresult.py regarding mapper extension

    .. change::
        :tags:
        :tickets:

      small tweak to cascade_mappers, not very strongly supported
      function at the moment

    .. change::
        :tags:
        :tickets: 202

      some fixes to between(), column.between() to propagate typing
      information better

    .. change::
        :tags:
        :tickets: 203

      if an object fails to be constructed, is not added to the
      session

    .. change::
        :tags:
        :tickets:

      CAST function has been made into its own clause object with
      its own compilation function in ansicompiler; allows MySQL
      to silently ignore most CAST calls since MySQL
      seems to only support the standard CAST syntax with Date types.
      MySQL-compatible CAST support for strings, ints, etc. a TODO

.. changelog::
    :version: 0.2.2
    :released: Mon Jun 05 2006

    .. change::
        :tags:
        :tickets: 190

      big improvements to polymorphic inheritance behavior, enabling it
      to work with adjacency list table structures

    .. change::
        :tags:
        :tickets:

      major fixes and refactorings to inheritance relationships overall,
      more unit tests

    .. change::
        :tags:
        :tickets:

      fixed "echo_pool" flag on create_engine()

    .. change::
        :tags:
        :tickets:

      fix to docs, removed incorrect info that close() is unsafe to use
      with threadlocal strategy (its totally safe !)

    .. change::
        :tags:
        :tickets: 188

      create_engine() can take URLs as string or unicode

    .. change::
        :tags:
        :tickets:

      firebird support partially completed;
      thanks to James Ralston and Brad Clements for their efforts.

    .. change::
        :tags:
        :tickets:

      Oracle url translation was broken, fixed, will feed host/port/sid
      into cx_oracle makedsn() if 'database' field is present, else uses
      straight TNS name from the 'host' field

    .. change::
        :tags:
        :tickets:

      fix to using unicode criterion for query.get()/query.load()

    .. change::
        :tags:
        :tickets:

      count() function on selectables now uses table primary key or
      first column instead of "1" for criterion, also uses label "rowcount"
      instead of "count".

    .. change::
        :tags:
        :tickets:

      got rudimental "mapping to multiple tables" functionality cleaned up,
      more correctly documented

    .. change::
        :tags:
        :tickets:

      restored global_connect() function, attaches to a DynamicMetaData
      instance called "default_metadata".  leaving MetaData arg to Table
      out will use the default metadata.

    .. change::
        :tags:
        :tickets:

      fixes to session cascade behavior, entity_name propigation

    .. change::
        :tags:
        :tickets:

      reorganized unittests into subdirectories

    .. change::
        :tags:
        :tickets:

      more fixes to threadlocal connection nesting patterns

.. changelog::
    :version: 0.2.1
    :released: Mon May 29 2006

    .. change::
        :tags:
        :tickets:

      "pool" argument to create_engine() properly propagates

    .. change::
        :tags:
        :tickets:

      fixes to URL, raises exception if not parsed, does not pass blank
      fields along to the DB connect string (a string such as
      user:host@/db was breaking on postgres)

    .. change::
        :tags:
        :tickets:

      small fixes to Mapper when it inserts and tries to get
      new primary key values back

    .. change::
        :tags:
        :tickets:

      rewrote half of TLEngine, the ComposedSQLEngine used with
      'strategy="threadlocal"'.  it now properly implements engine.begin()/
      engine.commit(), which nest fully with connection.begin()/trans.commit().
      added about six unittests.

    .. change::
        :tags:
        :tickets:

      major "duh" in pool.Pool, forgot to put back the WeakValueDictionary.
      unittest which was supposed to check for this was also silently missing
      it.  fixed unittest to ensure that ConnectionFairy properly falls out
      of scope.

    .. change::
        :tags:
        :tickets:

      placeholder dispose() method added to SingletonThreadPool, doesn't
      do anything yet

    .. change::
        :tags:
        :tickets:

      rollback() is automatically called when an exception is raised,
      but only if there's no transaction in process (i.e. works more like
      autocommit).

    .. change::
        :tags:
        :tickets:

      fixed exception raise in sqlite if no sqlite module present

    .. change::
        :tags:
        :tickets:

      added extra example detail for association object doc

    .. change::
        :tags:
        :tickets:

      Connection adds checks for already being closed

.. changelog::
    :version: 0.2.0
    :released: Sat May 27 2006

    .. change::
        :tags:
        :tickets:

      overhaul to Engine system so that what was formerly the SQLEngine
      is now a ComposedSQLEngine which consists of a variety of components,
      including a Dialect, ConnectionProvider, etc. This impacted all the
      db modules as well as Session and Mapper.

    .. change::
        :tags:
        :tickets:

      create_engine now takes only RFC-1738-style strings:
      driver://user:password@host:port/database

    .. change::
        :tags:
        :tickets: 152

      total rewrite of connection-scoping methodology, Connection objects
      can now execute clause elements directly, added explicit "close" as
      well as support throughout Engine/ORM to handle closing properly,
      no longer relying upon __del__ internally to return connections
      to the pool.

    .. change::
        :tags:
        :tickets:

      overhaul to Session interface and scoping.  uses hibernate-style
      methods, including query(class), save(), save_or_update(), etc.
      no threadlocal scope is installed by default.  Provides a binding
      interface to specific Engines and/or Connections so that underlying
      Schema objects do not need to be bound to an Engine.  Added a basic
      SessionTransaction object that can simplistically aggregate transactions
      across multiple engines.

    .. change::
        :tags:
        :tickets:

      overhaul to mapper's dependency and "cascade" behavior; dependency logic
      factored out of properties.py into a separate module "dependency.py".
      "cascade" behavior is now explicitly controllable, proper implementation
      of "delete", "delete-orphan", etc.  dependency system can now determine at
      flush time if a child object has a parent or not so that it makes better
      decisions on how that child should be updated in the DB with regards to deletes.

    .. change::
        :tags:
        :tickets:

      overhaul to Schema to build upon MetaData object instead of an Engine.
      Entire SQL/Schema system can be used with no Engines whatsoever, executed
      solely by an explicit Connection object.  the "bound" methodlogy exists via the
      BoundMetaData for schema objects.  ProxyEngine is generally not needed
      anymore and is replaced by DynamicMetaData.

    .. change::
        :tags:
        :tickets: 167

      true polymorphic behavior implemented, fixes

    .. change::
        :tags:
        :tickets: 147

      "oid" system has been totally moved into compile-time behavior;
      if they are used in an order_by where they are not available, the order_by
      doesn't get compiled, fixes

    .. change::
        :tags:
        :tickets:

      overhaul to packaging; "mapping" is now "orm", "objectstore" is now
      "session", the old "objectstore" namespace gets loaded in via the
      "threadlocal" mod if used

    .. change::
        :tags:
        :tickets:

      mods now called in via "import <modname>".  extensions favored over
      mods as mods are globally-monkeypatching

    .. change::
        :tags:
        :tickets: 154

      fix to add_property so that it propagates properties to inheriting
      mappers

    .. change::
        :tags:
        :tickets:

      backrefs create themselves against primary mapper of its originating
      property, priamry/secondary join arguments can be specified to override.
      helps their usage with polymorphic mappers

    .. change::
        :tags:
        :tickets: 31

      "table exists" function has been implemented

    .. change::
        :tags:
        :tickets: 98

      "create_all/drop_all" added to MetaData object

    .. change::
        :tags:
        :tickets:

      improvements and fixes to topological sort algorithm, as well as more
      unit tests

    .. change::
        :tags:
        :tickets:

      tutorial page added to docs which also can be run with a custom doctest
      runner to ensure its properly working.  docs generally overhauled to
      deal with new code patterns

    .. change::
        :tags:
        :tickets:

      many more fixes, refactorings.

    .. change::
        :tags:
        :tickets:

      migration guide is available on the Wiki at
      http://www.sqlalchemy.org/trac/wiki/02Migration
