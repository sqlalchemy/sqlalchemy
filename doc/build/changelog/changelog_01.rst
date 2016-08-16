
==============
0.1 Changelog
==============

                
.. changelog::
    :version: 0.1.7
    :released: Fri May 05 2006

    .. change::
        :tags: 
        :tickets: 

      some fixes to topological sort algorithm

    .. change::
        :tags: 
        :tickets: 

      added DISTINCT ON support to Postgres (just supply distinct=[col1,col2..])

    .. change::
        :tags: 
        :tickets: 

      added __mod__ (% operator) to sql expressions

    .. change::
        :tags: 
        :tickets: 

      "order_by" mapper property inherited from inheriting mapper

    .. change::
        :tags: 
        :tickets: 

      fix to column type used when mapper UPDATES/DELETEs

    .. change::
        :tags: 
        :tickets: 

      with convert_unicode=True, reflection was failing, has been fixed

    .. change::
        :tags: 
        :tickets: 

      types types types!  still weren't working....have to use TypeDecorator again :(

    .. change::
        :tags: 
        :tickets: 

      mysql binary type converts array output to buffer, fixes PickleType

    .. change::
        :tags: 
        :tickets: 

      fixed the attributes.py memory leak once and for all

    .. change::
        :tags: 
        :tickets: 

      unittests are qualified based on the databases that support each one

    .. change::
        :tags: 
        :tickets: 

      fixed bug where column defaults would clobber VALUES clause of insert objects

    .. change::
        :tags: 
        :tickets: 

      fixed bug where table def w/ schema name would force engine connection

    .. change::
        :tags: 
        :tickets: 

      fix for parenthesis to work correctly with subqueries in INSERT/UPDATE

    .. change::
        :tags: 
        :tickets: 

      HistoryArraySet gets extend() method

    .. change::
        :tags: 
        :tickets: 

      fixed lazyload support for other comparison operators besides =

    .. change::
        :tags: 
        :tickets: 

      lazyload fix where two comparisons in the join condition point to the
      samem column

    .. change::
        :tags: 
        :tickets: 

      added "construct_new" flag to mapper, will use __new__ to create instances
      instead of __init__ (standard in 0.2)

    .. change::
        :tags: 
        :tickets: 

      added selectresults.py to SVN, missed it last time

    .. change::
        :tags: 
        :tickets: 

      tweak to allow a many-to-many relationship from a table to itself via
      an association table

    .. change::
        :tags: 
        :tickets: 

      small fix to "translate_row" function used by polymorphic example

    .. change::
        :tags: 
        :tickets: 

      create_engine uses cgi.parse_qsl to read query string (out the window in 0.2)

    .. change::
        :tags: 
        :tickets: 

      tweaks to CAST operator

    .. change::
        :tags: 
        :tickets: 

      fixed function names LOCAL_TIME/LOCAL_TIMESTAMP -> LOCALTIME/LOCALTIMESTAMP

    .. change::
        :tags: 
        :tickets: 

      fixed order of ORDER BY/HAVING in compile

.. changelog::
    :version: 0.1.6
    :released: Wed Apr 12 2006

    .. change::
        :tags: 
        :tickets: 

      support for MS-SQL added courtesy Rick Morrison, Runar Petursson

    .. change::
        :tags: 
        :tickets: 

      the latest SQLSoup from J. Ellis

    .. change::
        :tags: 
        :tickets: 

      ActiveMapper has preliminary support for inheritance (Jeff Watkins)

    .. change::
        :tags: 
        :tickets: 

      added a "mods" system which allows pluggable modules that modify/augment
      core functionality, using the function "install_mods(\*modnames)".

    .. change::
        :tags: 
        :tickets: 

      added the first "mod", SelectResults, which modifies mapper selects to
      return generators that turn ranges into LIMIT/OFFSET queries
      (Jonas Borgstr?

    .. change::
        :tags: 
        :tickets: 

      factored out querying capabilities of Mapper into a separate Query object
      which is Session-centric.  this improves the performance of mapper.using(session)
      and makes other things possible.

    .. change::
        :tags: 
        :tickets: 

      objectstore/Session refactored, the official way to save objects is now
      via the flush() method.  The begin/commit functionality of Session is factored
      into LegacySession which is still established as the default behavior, until
      the 0.2 series.

    .. change::
        :tags: 
        :tickets: 

      types system is bound to an engine at query compile time, not schema
      construction time.  this simplifies the types system as well as the ProxyEngine.

    .. change::
        :tags: 
        :tickets: 

      added 'version_id' keyword argument to mapper. this keyword should reference a
      Column object with type Integer, preferably non-nullable, which will be used on
      the mapped table to track version numbers. this number is incremented on each
      save operation and is specified in the UPDATE/DELETE conditions so that it
      factors into the returned row count, which results in a ConcurrencyError if the
      value received is not the expected count.

    .. change::
        :tags: 
        :tickets: 

      added 'entity_name' keyword argument to mapper. a mapper is now associated
      with a class via the class object as well as an optional entity_name parameter,
      which is a string defaulting to None. any number of primary mappers can be
      created for a class, qualified by the entity name. instances of those classes
      will issue all of their load and save operations through their
      entity_name-qualified mapper, and maintain separate a identity in the identity
      map for an otherwise equilvalent object.

    .. change::
        :tags: 
        :tickets: 

      overhaul to the attributes system. code has been clarified, and also fixed to
      support proper polymorphic behavior on object attributes.

    .. change::
        :tags: 
        :tickets: 

      added "for_update" flag to Select objects

    .. change::
        :tags: 
        :tickets: 

      some fixes for backrefs

    .. change::
        :tags: 
        :tickets: 

      fix for postgres1 DateTime type

    .. change::
        :tags: 
        :tickets: 

      documentation pages mostly switched over to Markdown syntax

.. changelog::
    :version: 0.1.5
    :released: Mon Mar 27 2006

    .. change::
        :tags: 
        :tickets: 

      added SQLSession concept to SQLEngine. this object keeps track of retrieving a
      connection from the connection pool as well as an in-progress transaction.
      methods push_session() and pop_session() added to SQLEngine which push/pop a new
      SQLSession onto the engine, allowing operation upon a second connection "nested"
      within the previous one, allowing nested transactions. Other tricks are sure to
      come later regarding SQLSession.

    .. change::
        :tags: 
        :tickets: 

      added nest_on argument to objectstore.Session. This is a single SQLEngine or
      list of engines for which push_session()/pop_session() will be called each time
      this Session becomes the active session (via objectstore.push_session() or
      equivalent). This allows a unit of work Session to take advantage of the nested
      transaction feature without explicitly calling push_session/pop_session on the
      engine.

    .. change::
        :tags: 
        :tickets: 

      factored apart objectstore/unitofwork to separate "Session scoping" from
      "uow commit heavy lifting"

    .. change::
        :tags: 
        :tickets: 

      added populate_instance() method to MapperExtension. allows an extension to
      modify the population of object attributes. this method can call the
      populate_instance() method on another mapper to proxy the attribute population
      from one mapper to another; some row translation logic is also built in to help
      with this.

    .. change::
        :tags: 
        :tickets: 

      fixed Oracle8-compatibility "use_ansi" flag which converts JOINs to
      comparisons with the = and (+) operators, passes basic unittests

    .. change::
        :tags: 
        :tickets: 

      tweaks to Oracle LIMIT/OFFSET support

    .. change::
        :tags: 
        :tickets: 

      Oracle reflection uses ALL_** views instead of USER_** to get larger
      list of stuff to reflect from

    .. change::
        :tags: 
        :tickets: 105

      fixes to Oracle foreign key reflection

    .. change::
        :tags: 
        :tickets: 

      objectstore.commit(obj1, obj2,...) adds an extra step to seek out private
      relations on properties and delete child objects, even though its not a global
      commit

    .. change::
        :tags: 
        :tickets: 

      lots and lots of fixes to mappers which use inheritance, strengthened the
      concept of relations on a mapper being made towards the "local" table for that
      mapper, not the tables it inherits.  allows more complex compositional patterns
      to work with lazy/eager loading.

    .. change::
        :tags: 
        :tickets: 

      added support for mappers to inherit from others based on the same table,
      just specify the same table as that of both parent/child mapper.

    .. change::
        :tags: 
        :tickets: 

      some minor speed improvements to the attributes system with regards to
      instantiating and populating new objects.

    .. change::
        :tags: 
        :tickets: 

      fixed MySQL binary unit test

    .. change::
        :tags: 
        :tickets: 

      INSERTs can receive clause elements as VALUES arguments, not just literal
      values

    .. change::
        :tags: 
        :tickets: 

      support for calling multi-tokened functions, i.e. schema.mypkg.func()

    .. change::
        :tags: 
        :tickets: 

      added J. Ellis' SQLSoup module to extensions package

    .. change::
        :tags: 
        :tickets: 

      added "polymorphic" examples illustrating methods to load multiple object types
      from one mapper, the second of which uses the new populate_instance() method.
      small improvements to mapper, UNION construct to help the examples along

    .. change::
        :tags: 
        :tickets: 

      improvements/fixes to session.refresh()/session.expire() (which may have
      been called "invalidate" earlier..)

    .. change::
        :tags: 
        :tickets: 

      added session.expunge() which totally removes an object from the current
      session

    .. change::
        :tags: 
        :tickets: 

      added \*args, \**kwargs pass-thru to engine.transaction(func) allowing easier
      creation of transactionalizing decorator functions

    .. change::
        :tags: 
        :tickets: 

      added iterator interface to ResultProxy:  "for row in result:..."

    .. change::
        :tags: 
        :tickets: 

      added assertion to tx = session.begin(); tx.rollback(); tx.begin(), i.e. cant
      use it after a rollback()

    .. change::
        :tags: 
        :tickets: 

      added date conversion on bind parameter fix to SQLite enabling dates to
      work with pysqlite1

    .. change::
        :tags: 
        :tickets: 116

      improvements to subqueries to more intelligently construct their FROM
      clauses

    .. change::
        :tags: 
        :tickets: 

      added PickleType to types.

    .. change::
        :tags: 
        :tickets: 

      fixed two bugs with column labels with regards to bind parameters: bind param
      keynames they are now generated from a column "label" in all relevant cases to
      take advantage of excess-name-length rules, and checks for a peculiar collision
      against a column named the same as "tablename_colname" added

    .. change::
        :tags: 
        :tickets: 

      major overhaul to unit of work documentation, other documentation sections.

    .. change::
        :tags: 
        :tickets: 

      fixed attributes bug where if an object is committed, its lazy-loaded list got
      blown away if it hadn't been loaded

    .. change::
        :tags: 
        :tickets: 

      added unique_connection() method to engine, connection pool to return a
      connection that is not part of the thread-local context or any current
      transaction

    .. change::
        :tags: 
        :tickets: 

      added invalidate() function to pooled connection.  will remove the connection
      from the pool.  still need work for engines to auto-reconnect to a stale DB
      though.

    .. change::
        :tags: 
        :tickets: 

      added distinct() function to column elements so you can do
      func.count(mycol.distinct())

    .. change::
        :tags: 
        :tickets: 

      added "always_refresh" flag to Mapper, creates a mapper that will always
      refresh the attributes of objects it gets/selects from the DB, overwriting any
      changes made.

.. changelog::
    :version: 0.1.4
    :released: Mon Mar 13 2006

    .. change::
        :tags: 
        :tickets: 

      create_engine() now uses genericized parameters; host/hostname,
      db/dbname/database, password/passwd, etc. for all engine connections. makes
      engine URIs much more "universal"

    .. change::
        :tags: 
        :tickets: 

      added support for SELECT statements embedded into a column clause, using the
      flag "scalar=True"

    .. change::
        :tags: 
        :tickets: 

      another overhaul to EagerLoading when used in conjunction with mappers that
      inherit; improvements to eager loads figuring out their aliased queries
      correctly, also relations set up against a mapper with inherited mappers will
      create joins against the table that is specific to the mapper itself (i.e. and
      not any tables that are inherited/are further down the inheritance chain),
      this can be overridden by using custom primary/secondary joins.

    .. change::
        :tags: 
        :tickets: 

      added J.Ellis patch to mapper.py so that selectone() throws an exception
      if query returns more than one object row, selectfirst() to not throw the
      exception. also adds selectfirst_by (synonymous with get_by) and selectone_by

    .. change::
        :tags: 
        :tickets: 

      added onupdate parameter to Column, will exec SQL/python upon an update
      statement.Also adds "for_update=True" to all DefaultGenerator subclasses

    .. change::
        :tags: 
        :tickets: 

      added support for Oracle table reflection contributed by Andrija Zaric;
      still some bugs to work out regarding composite primary keys/dictionary selection

    .. change::
        :tags: 
        :tickets: 

      checked in an initial Firebird module, awaiting testing.

    .. change::
        :tags: 
        :tickets: 

      added sql.ClauseParameters dictionary object as the result for
      compiled.get_params(), does late-typeprocessing of bind parameters so
      that the original values are easier to access

    .. change::
        :tags: 
        :tickets: 

      more docs for indexes, column defaults, connection pooling, engine construction

    .. change::
        :tags: 
        :tickets: 

      overhaul to the construction of the types system. uses a simpler inheritance
      pattern so that any of the generic types can be easily subclassed, with no need
      for TypeDecorator.

    .. change::
        :tags: 
        :tickets: 

      added "convert_unicode=False" parameter to SQLEngine, will cause all String
      types to perform unicode encoding/decoding (makes Strings act like Unicodes)

    .. change::
        :tags: 
        :tickets: 

      added 'encoding="utf8"' parameter to engine.  the given encoding will be
      used for all encode/decode calls within Unicode types as well as Strings
      when convert_unicode=True.

    .. change::
        :tags: 
        :tickets: 

      improved support for mapping against UNIONs, added polymorph.py example
      to illustrate multi-class mapping against a UNION

    .. change::
        :tags: 
        :tickets: 

      fix to SQLite LIMIT/OFFSET syntax

    .. change::
        :tags: 
        :tickets: 

      fix to Oracle LIMIT syntax

    .. change::
        :tags: 
        :tickets: 

      added backref() function, allows backreferences to have keyword arguments
      that will be passed to the backref.

    .. change::
        :tags: 
        :tickets: 

      Sequences and ColumnDefault objects can do execute()/scalar() standalone

    .. change::
        :tags: 
        :tickets: 

      SQL functions (i.e. func.foo()) can do execute()/scalar() standalone

    .. change::
        :tags: 
        :tickets: 

      fix to SQL functions so that the ANSI-standard functions, i.e. current_timestamp
      etc., do not specify parenthesis.  all other functions do.

    .. change::
        :tags: 
        :tickets: 

      added settattr_clean and append_clean to SmartProperty, which set
      attributes without triggering a "dirty" event or any history. used as:
      myclass.prop1.setattr_clean(myobject, 'hi')

    .. change::
        :tags: 
        :tickets: 

      improved support to column defaults when used by mappers; mappers will pull
      pre-executed defaults from statement's executed bind parameters
      (pre-conversion) to populate them into a saved object's attributes; if any
      PassiveDefaults have fired off, will instead post-fetch the row from the DB to
      populate the object.

    .. change::
        :tags: 
        :tickets: 

      added 'get_session().invalidate(\*obj)' method to objectstore, instances will
      refresh() themselves upon the next attribute access.

    .. change::
        :tags: 
        :tickets: 

      improvements to SQL func calls including an "engine" keyword argument so
      they can be execute()d or scalar()ed standalone, also added func accessor to
      SQLEngine

    .. change::
        :tags: 
        :tickets: 

      fix to MySQL4 custom table engines, i.e. TYPE instead of ENGINE

    .. change::
        :tags: 
        :tickets: 

      slightly enhanced logging, includes timestamps and a somewhat configurable
      formatting system, in lieu of a full-blown logging system

    .. change::
        :tags: 
        :tickets: 

      improvements to the ActiveMapper class from the TG gang, including
      many-to-many relationships

    .. change::
        :tags: 
        :tickets: 

      added Double and TinyInt support to mysql

.. changelog::
    :version: 0.1.3
    :released: Thu Mar 02 2006

    .. change::
        :tags: 
        :tickets: 

      completed "post_update" feature, will add a second update statement before
      inserts and after deletes in order to reconcile a relationship without any
      dependencies being created; used when persisting two rows that are dependent
      on each other

    .. change::
        :tags: 
        :tickets: 

      completed mapper.using(session) function, localized per-object Session
      functionality; objects can be declared and manipulated as local to any
      user-defined Session

    .. change::
        :tags: 
        :tickets: 

      fix to Oracle "row_number over" clause with multiple tables

    .. change::
        :tags: 
        :tickets: 

      mapper.get() was not selecting multiple-keyed objects if the mapper's table was a join,
      such as in an inheritance relationship, this is fixed.

    .. change::
        :tags: 
        :tickets: 

      overhaul to sql/schema packages so that the sql package can run all on its own,
      producing selects, inserts, etc. without any engine dependencies.  builds upon
      new TableClause/ColumnClause lexical objects.  Schema's Table/Column objects
      are the "physical" subclasses of them.  simplifies schema/sql relationship,
      extensions (like proxyengine), and speeds overall performance by a large margin.
      removes the entire getattr() behavior that plagued 0.1.1.

    .. change::
        :tags: 
        :tickets: 

      refactoring of how the mapper "synchronizes" data between two objects into a
      separate module, works better with properties attached to a mapper that has an
      additional inheritance relationship to one of the related tables, also the same
      methodology used to synchronize parent/child objects now used by mapper to
      synchronize between inherited and inheriting mappers.

    .. change::
        :tags: 
        :tickets: 

      made objectstore "check for out-of-identitymap" more aggressive, will perform the
      check when object attributes are modified or the object is deleted

    .. change::
        :tags: 
        :tickets: 

      Index object fully implemented, can be constructed standalone, or via
      "index" and "unique" arguments on Columns.

    .. change::
        :tags: 
        :tickets: 

      added "convert_unicode" flag to SQLEngine, will treat all String/CHAR types
      as Unicode types, with raw-byte/utf-8 translation on the bind parameter and
      result set side.

    .. change::
        :tags: 
        :tickets: 

      postgres maintains a list of ANSI functions that must have no parenthesis so
      function calls with no arguments work consistently

    .. change::
        :tags: 
        :tickets: 

      tables can be created with no engine specified.  this will default their engine
      to a module-scoped "default engine" which is a ProxyEngine.  this engine can
      be connected via the function "global_connect".

    .. change::
        :tags: 
        :tickets: 

      added "refresh(\*obj)" method to objectstore / Session to reload the attributes of
      any set of objects from the database unconditionally

.. changelog::
    :version: 0.1.2
    :released: Fri Feb 24 2006

    .. change::
        :tags: 
        :tickets: 

      fixed a recursive call in schema that was somehow running 994 times then returning
      normally.  broke nothing, slowed down everything.  thanks to jpellerin for finding this.

.. changelog::
    :version: 0.1.1
    :released: Thu Feb 23 2006

    .. change::
        :tags: 
        :tickets: 

      small fix to Function class so that expressions with a func.foo() use the type of the
      Function object (i.e. the left side) as the type of the boolean expression, not the
      other side which is more of a moving target (changeset 1020).

    .. change::
        :tags: 
        :tickets: 

      creating self-referring mappers with backrefs slightly easier (but still not that easy -
      changeset 1019)

    .. change::
        :tags: 
        :tickets: 

      fixes to one-to-one mappings (changeset 1015)

    .. change::
        :tags: 
        :tickets: 

      psycopg1 date/time issue with None fixed (changeset 1005)

    .. change::
        :tags: 
        :tickets: 

      two issues related to postgres, which doesn't want to give you the "lastrowid"
      since oids are deprecated:

        * postgres database-side defaults that are on primary key cols *do* execute
          explicitly beforehand, even though that's not the idea of a PassiveDefault.  this is
          because sequences on columns get reflected as PassiveDefaults, but need to be explicitly
          executed on a primary key col so we know what we just inserted.
        * if you did add a row that has a bunch of database-side defaults on it,
          and the PassiveDefault thing was working the old way, i.e. they just execute on
          the DB side, the "cant get the row back without an OID" exception that occurred
          also will not happen unless someone (usually the ORM) explicitly asks for it.

    .. change::
        :tags: 
        :tickets: 

      fixed a glitch with engine.execute_compiled where it was making a second
      ResultProxy that just got thrown away.

    .. change::
        :tags: 
        :tickets: 

      began to implement newer logic in object properities.  you can now say
      myclass.attr.property, which will give you the PropertyLoader corresponding to that
      attribute, i.e. myclass.mapper.props['attr']

    .. change::
        :tags: 
        :tickets: 

      eager loading has been internally overhauled to use aliases at all times.  more
      complicated chains of eager loads can now be created without any need for explicit
      "use aliases"-type instructions.  EagerLoader code is also much simpler now.

    .. change::
        :tags: 
        :tickets: 

      a new somewhat experimental flag "use_update" added to relations, indicates that
      this relationship should be handled by a second UPDATE statement, either after a
      primary INSERT or before a primary DELETE.  handles circular row dependencies.

    .. change::
        :tags: 
        :tickets: 

      added exceptions module, all raised exceptions (except for some
      KeyError/AttributeError exceptions) descend from these classes.

    .. change::
        :tags: 
        :tickets: 

      fix to date types with MySQL, returned timedelta converted to datetime.time

    .. change::
        :tags: 
        :tickets: 

      two-phase objectstore.commit operations (i.e. begin/commit) now return a
      transactional object (SessionTrans), to more clearly indicate transaction boundaries.

    .. change::
        :tags: 
        :tickets: 

      Index object with create/drop support added to schema

    .. change::
        :tags: 
        :tickets: 

      fix to postgres, where it will explicitly pre-execute a PassiveDefault on a table
      if it is a primary key column, pursuant to the ongoing "we cant get inserted rows
      back from postgres" issue

    .. change::
        :tags: 
        :tickets: 

      change to information_schema query that gets back postgres table defs, now
      uses explicit JOIN keyword, since one user had faster performance with 8.1

    .. change::
        :tags: 
        :tickets: 

      fix to engine.process_defaults so it works correctly with a table that has
      different column name/column keys (changset 982)

    .. change::
        :tags: 
        :tickets: 

      a column can only be attached to one table - this is now asserted

    .. change::
        :tags: 
        :tickets: 

      postgres time types descend from Time type

    .. change::
        :tags: 
        :tickets: 

      fix to alltests so that it runs types test (now named testtypes)

    .. change::
        :tags: 
        :tickets: 

      fix to Join object so that it correctly exports its foreign keys (cs 973)

    .. change::
        :tags: 
        :tickets: 

      creating relationships against mappers that use inheritance fixed (cs 973)
