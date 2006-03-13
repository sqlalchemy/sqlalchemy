<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Unit of Work'</%attr>

<&|doclib.myt:item, name="unitofwork", description="Unit of Work" &>
    <&|doclib.myt:item, name="overview", description="Overview" &>
    <p>The concept behind Unit of Work is to track modifications to a field of objects, and then be able to commit those changes to the database in a single operation.  Theres a lot of advantages to this, including that your application doesn't need to worry about individual save operations on objects, nor about the required order for those operations, nor about excessive repeated calls to save operations that would be more efficiently aggregated into one step.  It also simplifies database transactions, providing a neat package with which to insert into the traditional database begin/commit phase.
    </p>
    <p>SQLAlchemy's unit of work includes these functions:
    <ul>
        <li>The ability to monitor scalar and list attributes on object instances, as well as object creates.  This is handled via the attributes package.</li>
        <li>The ability to maintain and process a list of modified objects, and based on the relationships set up by the mappers for those objects as well as the foreign key relationships of the underlying tables, figure out the proper order of operations so that referential integrity is maintained, and also so that on-the-fly values such as newly created primary keys can be propigated to dependent objects that need them before they are saved.  The central algorithm for this is the <b>topological sort</b>.</li>
        <li>The ability to "roll back" the attributes that have changed on an object instance since the last commit() operation.  this is also handled by the attributes package.</li>
        <li>The ability to define custom functionality that occurs within the unit-of-work commit phase, such as "before insert", "after insert", etc.  This is accomplished via MapperExtension.</li>
        <li>an Identity Map, which is a dictionary storing the one and only instance of an object for a particular table/primary key combination.  This allows many parts of an application to get a handle to a particular object without any chance of modifications going to two different places.</li>
        <li>Thread-local operation.  the Identity map as well as the Unit of work itself are normally instantiated and accessed in a manner that is local to the current thread.  Another concurrently executing thread will therefore have its own Identity Map/Unit of Work, so unless an application explicitly shares objects between threads, the operation of the object relational mapping is automatically threadsafe.  Unit of Work objects can also be constructed manually to allow any user-defined scoping.</li>
    </ul></p>
    </&>
    <&|doclib.myt:item, name="getting", description="Accessing UnitOfWork Instances" &>
    <p>The current unit of work is accessed via the Session interface.  The Session is available in a thread-local context from the objectstore module as follows:</p>
        <&|formatting.myt:code&>
            # get the current thread's session
            s = objectstore.get_session()
        </&>
    <p>The Session object acts as a proxy to an underlying UnitOfWork object.  Common methods include commit(), begin(), clear(), and delete().  Most of these methods are available at the module level in the objectstore module, which operate upon the Session returned by the get_session() function.
    </p>
    <p>To clear out the current thread's UnitOfWork, which has the effect of discarding the Identity Map and the lists of all objects that have been modified, just issue a clear:
    </p>
    <&|formatting.myt:code&>
        # via module
        objectstore.clear()
        
        # or via Session
        objectstore.get_session().clear()
    </&>
    <p>This is the easiest way to "start fresh", as in a web application that wants to have a newly loaded graph of objects on each request.  Any object instances before the clear operation should be discarded.</p>
    </&>
    <&|doclib.myt:item, name="begincommit", description="Begin/Commit" &>
    <p>The current thread's UnitOfWork object keeps track of objects that are modified.  It maintains the following lists:</p>
    <&|formatting.myt:code&>
        # new objects that were just constructed
        objectstore.get_session().new
        
        # objects that exist in the database, that were modified
        objectstore.get_session().dirty
        
        # objects that have been marked as deleted via objectstore.delete()
        objectstore.get_session().deleted
    </&>
    <p>To commit the changes stored in those lists, just issue a commit.  This can be called via <span class="codeline">objectstore.session().commit()</span>, or through the module-level convenience method in the objectstore module:</p>
    <&|formatting.myt:code&>
        objectstore.commit()
    </&>
    <p>The commit operation takes place within a SQL-level transaction, so any failures that occur will roll back the state of everything to before the commit took place.</p>
    <p>When mappers are created for classes, new object construction automatically places objects in the "new" list on the UnitOfWork, and object modifications automatically place objects in the "dirty" list.  To mark objects as to be deleted, use the "delete" method on UnitOfWork, or the module level version:</p>
    <&|formatting.myt:code&>
        objectstore.delete(myobj1, myobj2, ...)
    </&>
    
    <p>Commit() can also take a list of objects which narrow its scope to looking at just those objects to save:</p>
    <&|formatting.myt:code&>
        objectstore.commit(myobj1, myobj2, ...)
    </&>
    <p>Committing just a subset of instances should be used carefully, as it may result in an inconsistent save state between dependent objects (it should manage to locate loaded dependencies and save those also, but it hasnt been tested much).</p>
    
    <&|doclib.myt:item, name="begin", description="Controlling Scope with begin()" &>
    <p><b>status</b> - release 0.1.1/SVN head</p>
    <p>The "scope" of the unit of work commit can be controlled further by issuing a begin().  A begin operation constructs a new UnitOfWork object and sets it as the currently used UOW.  It maintains a reference to the original UnitOfWork as its "parent", and shares the same "identity map" of objects that have been loaded from the database within the scope of the parent UnitOfWork.  However, the "new", "dirty", and "deleted" lists are empty.  This has the effect that only changes that take place after the begin() operation get logged to the current UnitOfWork, and therefore those are the only changes that get commit()ted.  When the commit is complete, the "begun" UnitOfWork removes itself and places the parent UnitOfWork as the current one again.</p>
<p>The begin() method returns a transactional object, upon which you can call commit() or rollback().  <b>Only this transactional object controls the transaction</b> - commit() upon the Session will do nothing until commit() or rollback() is called upon the transactional object.</p>
    <&|formatting.myt:code&>
        # modify an object
        myobj1.foo = "something new"
        
        # begin an objectstore scope
        # this is equivalent to objectstore.get_session().begin()
        trans = objectstore.begin()
        
        # modify another object
        myobj2.lala = "something new"
        
        # only 'myobj2' is saved
        trans.commit()
    </&>
    <p>begin/commit supports the same "nesting" behavior as the SQLEngine (note this behavior is not the original "nested" behavior), meaning that many begin() calls can be made, but only the outermost transactional object will actually perform a commit().  Similarly, calls to the commit() method on the Session, which might occur in function calls within the transaction, will not do anything; this allows an external function caller to control the scope of transactions used within the functions.</p>
    </&>
    <&|doclib.myt:item, name="transactionnesting", description="Nesting UnitOfWork in a Database Transaction" &>
    <p>The UOW commit operation places its INSERT/UPDATE/DELETE operations within the scope of a database transaction controlled by a SQLEngine:
    <&|formatting.myt:code&>
    engine.begin()
    try:
        # run objectstore update operations
    except:
        engine.rollback()
        raise
    engine.commit()
    </&>
    <p>If you recall from the <&formatting.myt:link, path="dbengine_transactions"&> section, the engine's begin()/commit() methods support reentrant behavior.  This means you can nest begin and commits and only have the outermost begin/commit pair actually take effect (rollbacks however, abort the whole operation at any stage).  From this it follows that the UnitOfWork commit operation can be nested within a transaction as well:</p>
    <&|formatting.myt:code&>
    engine.begin()
    try:
        # perform custom SQL operations
        objectstore.commit()
        # perform custom SQL operations
    except:
        engine.rollback()
        raise
    engine.commit()
    </&>
    
    </&>
    </&>
    <&|doclib.myt:item, name="identity", description="The Identity Map" &>
    <p>All object instances which are saved to the database, or loaded from the database, are given an identity by the mapper/objectstore.  This identity is available via the _instance_key property attached to each object instance, and is a tuple consisting of the table's class, the SQLAlchemy-specific "hash key" of the table its persisted to, and an additional tuple of primary key values, in the order that they appear within the table definition:</p>
    <&|formatting.myt:code&>
        >>> obj._instance_key 
        (<class 'test.tables.User'>, "Table('users',SQLiteSQLEngine(([':memory:'], {})),schema=None)", (7,))
    </&>
    <p>Note that this identity is a database identity, not an in-memory identity.  An application can have several different objects in different unit-of-work scopes that have the same database identity, or an object can be removed from memory, and constructed again later, with the same database identity.  What can <b>never</b> happen is for two copies of the same object to exist in the same unit-of-work scope with the same database identity; this is guaranteed by the <b>identity map</b>.
    </p>
    <p>
    At the moment that an object is assigned this key, it is also added to the current thread's unit-of-work's identity map.  The identity map is just a WeakValueDictionary which maintains the one and only reference to a particular object within the current unit of work scope.  It is used when result rows are fetched from the database to insure that only one copy of a particular object actually comes from that result set in the case that eager loads or other joins are used, or if the object had already been loaded from a previous result set.  The get() method on a mapper, which retrieves an object based on primary key identity, also checks in the current identity map first to save a database round-trip if possible.  In the case of an object lazy-loading a single child object, the get() method is also used.
    </p>
    <p>Methods on mappers and the objectstore module, which are relevant to identity include the following:</p>
    <&|formatting.myt:code&>
        # assume 'm' is a mapper
        m = mapper(User, users)
        
        # get the identity key corresponding to a primary key
        key = m.identity_key(7)
        
        # for composite key, list out the values in the order they
        # appear in the table
        key = m.identity_key(12, 'rev2')

        # get the identity key given a primary key 
        # value as a tuple, a class, and a table
        key = objectstore.get_id_key((12, 'rev2'), User, users)
        
        # get the identity key for an object, whether or not it actually
        # has one attached to it (m is the mapper for obj's class)
        key = m.instance_key(obj)
        
        # same thing, from the objectstore (works for any obj type)
        key = objectstore.instance_key(obj)
        
        # is this key in the current identity map?
        objectstore.has_key(key)
        
        # is this object in the current identity map?
        objectstore.has_instance(obj)

        # get this object from the current identity map based on 
        # singular/composite primary key, or if not go 
        # and load from the database
        obj = m.get(12, 'rev2')
    </&>
    </&>
    <&|doclib.myt:item, name="import", description="Bringing External Instances into the UnitOfWork" &>
    <p>The _instance_key attribute is designed to work with objects that are serialized into strings and brought back again.  As it contains no references to internal structures or database connections, applications that use caches or session storage which require serialization (i.e. pickling) can store SQLAlchemy-loaded objects.  However, as mentioned earlier, an object with a particular database identity is only allowed to exist uniquely within the current unit-of-work scope.  So, upon deserializing such an object, it has to "check in" with the current unit-of-work/identity map combination, to insure that it is the only unique instance.  This is achieved via the <span class="codeline">import_instance()</span> function in objectstore:</p>
    <&|formatting.myt:code&>
        # deserialize an object
        myobj = pickle.loads(mystring)
        
        # "import" it.  if the objectstore already had this object in the 
        # identity map, then you get back the one from the current session.
        myobj = objectstore.import_instance(myobj)
    </&>
<p>Note that the import_instance() function will either mark the deserialized object as the official copy in the current identity map, which includes updating its _instance_key with the current application's class instance, or it will discard it and return the corresponding object that was already present.</p>
    </&>

    <&|doclib.myt:item, name="advscope", description="Advanced UnitOfWork Management"&>

    <&|doclib.myt:item, name="object", description="Per-Object Sessions" &>
    <p><b>status</b> - 'using' function not yet released</p>
    <p>Sessions can be created on an ad-hoc basis and used for individual groups of objects and operations.  This has the effect of bypassing the entire "global"/"threadlocal" UnitOfWork system and explicitly using a particular Session:</p>
    <&|formatting.myt:code&>
        # make a new Session with a global UnitOfWork
        s = objectstore.Session()
        
        # make objects bound to this Session
        x = MyObj(_sa_session=s)
        
        # perform mapper operations bound to this Session
        # (this function coming soon)
        r = MyObj.mapper.using(s).select_by(id=12)
            
        # get the session that corresponds to an instance
        s = objectstore.get_session(x)
        
        # commit 
        s.commit()

        # perform a block of operations with this session set within the current scope
        objectstore.push_session(s)
        try:
            r = mapper.select_by(id=12)
            x = new MyObj()
            objectstore.commit()
        finally:
            objectstore.pop_session()
    </&>
    </&>

    <&|doclib.myt:item, name="scope", description="Custom Session Objects/Custom Scopes" &>

    <p>For users who want to make their own Session subclass, or replace the algorithm used to return scoped Session objects (i.e. the objectstore.get_session() method):</p>
    <&|formatting.myt:code&>
        # make a new Session
        s = objectstore.Session()
        
        # set it as the current thread-local session
        objectstore.session_registry.set(s)

        # set the objectstore's session registry to a different algorithm
        
        def create_session():
            """creates new sessions"""
            return objectstore.Session()
        def mykey():
            """creates contextual keys to store scoped sessions"""
            return "mykey"
            
        objectstore.session_registry = sqlalchemy.util.ScopedRegistry(createfunc=create_session, scopefunc=mykey)
    </&>
    </&>

    <&|doclib.myt:item, name="logging", description="Analyzing Object Commits" &>
    <p>The objectstore module can log an extensive display of its "commit plans", which is a graph of its internal representation of objects before they are committed to the database.  To turn this logging on:
    <&|formatting.myt:code&>
        # make an engine with echo_uow
        engine = create_engine('myengine...', echo_uow=True)
        
        # globally turn on echo
        objectstore.LOG = True
    </&>
    <p>Commits will then dump to the standard output displays like the following:</p>
    <&|formatting.myt:code, syntaxtype=None&>
    Task dump:
     UOWTask(6034768) 'User/users/6015696'
      |
      |- Save elements
      |- Save: UOWTaskElement(6034800): User(6016624) (save)
      |
      |- Save dependencies
      |- UOWDependencyProcessor(6035024) 'addresses' attribute on saved User's (UOWTask(6034768) 'User/users/6015696')
      |       |-UOWTaskElement(6034800): User(6016624) (save)
      |
      |- Delete dependencies
      |- UOWDependencyProcessor(6035056) 'addresses' attribute on User's to be deleted (UOWTask(6034768) 'User/users/6015696')
      |       |-(no objects)
      |
      |- Child tasks
      |- UOWTask(6034832) 'Address/email_addresses/6015344'
      |   |
      |   |- Save elements
      |   |- Save: UOWTaskElement(6034864): Address(6034384) (save)
      |   |- Save: UOWTaskElement(6034896): Address(6034256) (save)
      |   |----
      | 
      |----
    </&>
    <p>The above graph can be read straight downwards to determine the order of operations.  It indicates "save User 6016624, process each element in the 'addresses' list on User 6016624, save Address 6034384, Address 6034256".
    </&>
    
    </&>
</&>
