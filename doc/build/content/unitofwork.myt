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
        <li>The ability to define custom functionality that occurs within the unit-of-work commit phase, such as "before insert", "after insert", etc.  This is accomplished via MapperExtension.</li>
        <li>an Identity Map, which is a dictionary storing the one and only instance of an object for a particular table/primary key combination.  This allows many parts of an application to get a handle to a particular object without any chance of modifications going to two different places.</li>
        <li>Thread-local operation.  the Identity map as well as its enclosing Unit of Work are normally instantiated and accessed in a manner that is local to the current thread, within an object called a Session.  Another concurrently executing thread will therefore have its own Session, so unless an application explicitly shares objects between threads, the operation of the object relational mapping is automatically threadsafe.  Session objects can also be constructed manually to allow any user-defined scoping.</li>
    </ul></p>
    </&>
    <&|doclib.myt:item, name="session", description="The Session Interface" &>
    <p>The current unit of work is accessed via a Session object.  The Session is available in a thread-local context from the objectstore module as follows:</p>
        <&|formatting.myt:code&>
            # get the current thread's session
            session = objectstore.get_session()
        </&>
    <p>The Session object acts as a proxy to an underlying UnitOfWork object.  Common methods include commit(), begin(), clear(), and delete().  Most of these methods are available at the module level in the objectstore module, which operate upon the Session returned by the get_session() function:
    </p>
    <&|formatting.myt:code&>
        # this...
        objectstore.get_session().commit()
        
        # is the same as this:
        objectstore.commit()
    </&>

    <p>A description of the most important methods and concepts follows.</p>

    <&|doclib.myt:item, name="identitymap", description="Identity Map" &>
    <p>The first concept to understand about the Unit of Work is that it is keeping track of all mapped objects which have been loaded from the database, as well as all mapped objects which have been saved to the database in the current session.  This means that everytime you issue a <code>select</code> call to a mapper which returns results, all of those objects are now installed within the current Session, mapped to their identity.</p>
    
    <p>In particular, it is insuring that only <b>one</b> instance of a particular object, corresponding to a particular database identity, exists within the Session at one time.  By "database identity" we mean a table or relational concept in the database combined with a particular primary key in that table. The session accomplishes this task using a dictionary known as an <b>Identity Map</b>.  When <code>select</code> or <code>get</code> calls on mappers issue queries to the database, they will in nearly all cases go out to the database on each call to fetch results.  However, when the mapper <b>instantiates</b> objects corresponding to the result set rows it receives, it will <b>check the current identity map first</b> before instantating a new object, and return <b>the same instance</b> already present in the identiy map if it already exists.</p>  
    
    <p>Example:</p>
    <&|formatting.myt:code&>
        mymapper = mapper(MyClass, mytable)
        
        obj1 = mymapper.selectfirst(mytable.c.id==15)
        obj2 = mymapper.selectfirst(mytable.c.id==15)
        
        >>> obj1 is obj2
        True
    </&>
    <p>The Identity Map is an instance of <code>weakref.WeakValueDictionary</code>, so that when an in-memory object falls out of scope, it will be removed automatically.  However, this may not be instant if there are circular references upon the object.  The current SA attributes implementation places some circular refs upon objects, although this may change in the future.  There are other ways to remove object instances from the current session, as well as to clear the current session entirely, which are described later in this section.</p>
    <p>To view the Session's identity map, it is accessible via the <code>identity_map</code> accessor, and is an instance of <code>weakref.WeakValueDictionary</code>:</p>
    <&|formatting.myt:code&><% """
        >>> objectstore.get_session().identity_map.values()
        [<__main__.User object at 0x712630>, <__main__.Address object at 0x712a70>]
        """  %>
    </&>

    <p>The identity of each object instance is available via the _instance_key property attached to each object instance, and is a tuple consisting of the object's class and an additional tuple of primary key values, in the order that they appear within the table definition:</p>
    <&|formatting.myt:code&>
        >>> obj._instance_key 
        (<class 'test.tables.User'>, (7,))
    </&>

    <p>
    At the moment that an object is assigned this key, it is also added to the current thread's unit-of-work's identity map.  
    </p>
    
    <p>The get() method on a mapper, which retrieves an object based on primary key identity, also checks in the current identity map first to save a database round-trip if possible.  In the case of an object lazy-loading a single child object, the get() method is used as well, so scalar-based lazy loads may in some cases not query the database; this is particularly important for backreference relationships as it can save a lot of queries.</p>
    
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
        # value as a tuple and a class
        key = objectstore.get_id_key((12, 'rev2'), User)
        
        # get the identity key for an object, whether or not it actually
        # has one attached to it (m is the mapper for obj's class)
        key = m.instance_key(obj)
                
        # is this key in the current identity map?
        session.has_key(key)
        
        # is this object in the current identity map?
        session.has_instance(obj)

        # get this object from the current identity map based on 
        # singular/composite primary key, or if not go 
        # and load from the database
        obj = m.get(12, 'rev2')
    </&>
    
    </&>
    
    <&|doclib.myt:item, name="changed", description="Whats Changed ?" &>
    <p>The next concept is that in addition to the Session storing a record of all objects loaded or saved, it also stores records of all <b>newly created</b> objects,  records of all objects whose attributes have been <b>modified</b>, records of all objects that have been marked as <b>deleted</b>, and records of all <b>modified list-based attributes</b> where additions or deletions have occurred.  These lists are used when a <code>commit()</code> call is issued to save all changes.  After the commit occurs, these lists are all cleared out.</p>
    
    <p>These records are all tracked by a collection of <code>Set</code> objects (which are a SQLAlchemy-specific  instance called a <code>HashSet</code>) that are also viewable off the Session:</p>
    <&|formatting.myt:code&>
        # new objects that were just constructed
        session.new
        
        # objects that exist in the database, that were modified
        session.dirty
        
        # objects that have been marked as deleted via session.delete(obj)
        session.deleted
        
        # list-based attributes thave been appended
        session.modified_lists
    </&>
    <p>Heres an interactive example, assuming the <code>User</code> and <code>Address</code> mapper setup first outlined in <&formatting.myt:link, path="datamapping_relations"&>:</p>
    <&|formatting.myt:code&>
    ">>>" # get the current thread's session
    ">>>" session = objectstore.get_session()

    ">>>" # create a new object, with a list-based attribute 
    ">>>" # containing two more new objects
    ">>>" u = User(user_name='Fred')
    ">>>" u.addresses.append(Address(city='New York'))
    ">>>" u.addresses.append(Address(city='Boston'))
    
    ">>>" # objects are in the "new" list
    ">>>" session.new
    [<__main__.User object at 0x713630>, 
    <__main__.Address object at 0x713a70>, 
    <__main__.Address object at 0x713b30>]
    
    ">>>" # view the "modified lists" member, 
    ">>>" # reveals our two Address objects as well, inside of a list
    ">>>" session.modified_lists
    [[<__main__.Address object at 0x713a70>, <__main__.Address object at 0x713b30>]]

    ">>>" # lets view what the class/ID is for the list object
    ">>>" ["%s %s" % (l.__class__, id(l)) for l in session.modified_lists]
    ['sqlalchemy.mapping.unitofwork.UOWListElement 7391872']
    
    ">>>" # now commit
    ">>>" session.commit()
    
    ">>>" # the "new" list is now empty
    ">>>" session.new
    []
    
    ">>>" # the "modified lists" list is now empty
    ">>>" session.modified_lists
    []
    
    ">>>" # now lets modify an object
    ">>>" u.user_name='Ed'
    
    ">>>" # it gets placed in the "dirty" list
    ">>>" session.dirty
    [<__main__.User object at 0x713630>]
    
    ">>>" # delete one of the addresses 
    ">>>" session.delete(u.addresses[0])
    
    ">>>" # and also delete it off the User object, note that
    ">>>" # this is *not automatic* when using session.delete()
    ">>>" del u.addresses[0]
    ">>>" session.deleted
    [<__main__.Address object at 0x713a70>]    
    
    ">>>" # commit
    ">>>" session.commit()
    
    ">>>" # all lists are cleared out
    ">>>" session.new, session.dirty, session.modified_lists, session.deleted
    ([], [], [], [])
    
    ">>>" # identity map has the User and the one remaining Address
    ">>>" session.identity_map.values()
    [<__main__.Address object at 0x713b30>, <__main__.User object at 0x713630>]
    </&>
    <p>Unlike the identity map, the <code>new</code>, <code>dirty</code>, <code>modified_lists</code>, and <code>deleted</code> lists are <b>not weak referencing.</b>  This means if you abandon all references to new or modified objects within a session, <b>they are still present</b> and will be saved on the next commit operation, unless they are removed from the Session explicitly (more on that later).  The <code>new</code> list may change in a future release to be weak-referencing, however for the <code>deleted</code> list, one can see that its quite natural for a an object marked as deleted to have no references in the application, yet a DELETE operation is still required.</p>
    </&>
        
    <&|doclib.myt:item, name="commit", description="Commit" &>
    <p>This is the main gateway to what the Unit of Work does best, which is save everything !  It should be clear by now that a commit looks like:
    </p>
    <&|formatting.myt:code&>
        objectstore.get_session().commit()
    </&>
    <p>It also can be called with a list of objects; in this form, the commit operation will be limited only to the objects specified in the list, as well as any child objects within <code>private</code> relationships for a delete operation:</p>
    <&|formatting.myt:code&>
        # saves only user1 and address2.  all other modified
        # objects remain present in the session.
        objectstore.get_session().commit(user1, address2)
    </&>
    <p>This second form of commit should be used more carefully as it will not necessarily locate other dependent objects within the session, whose database representation may have foreign constraint relationships with the objects being operated upon.</p>
    
        <&|doclib.myt:item, name="whatis", description="What Commit is, and Isn't" &>
        <p>The purpose of the Commit operation is to instruct the Unit of Work to analyze its lists of modified objects, assemble them into a dependency graph, fire off the appopriate INSERT, UPDATE, and DELETE statements via the mappers related to those objects, and to synchronize column-based object attributes that correspond directly to updated/inserted database columns.  <b>And thats it.</b>  It does not affect any <code>relation</code>-based object attributes, that is attributes that reference other objects or lists of other objects, in any way.  A brief list of what will <b>not</b> happen includes:</p>
            <ul>
                <li>It will not append or delete any object instances to/from any list-based object attributes.  Any objects that have been created or marked as deleted will be updated as such in the database, but if a newly deleted object instance is still attached to a parent object's list, the object itself will remain in that list.</li>
                <li>It will not set or remove any scalar references to other objects, even if the corresponding database identifier columns have been committed.</li>
            </ul>
            <p>This means, if you set <code>address.user_id</code> to 5, that integer attribute will be saved, but it will not place an <code>Address</code> object in the <code>addresses</code> attribute of the corresponding  <code>User</code> object.  In some cases there may be a lazy-loader still attached to an object attribute which when first accesed performs a fresh load from the database and creates the appearance of this behavior, but this behavior should not be relied upon as it is specific to lazy loading and also may disappear in a future release.  Similarly, if the <code>Address</code> object is marked as deleted and a commit is issued, the correct DELETE statements will be issued, but if the object instance itself is still attached to the <code>User</code>, it will remain.</p>
        <P>So the primary guideline for dealing with commit() is, <b>the developer is responsible for maintaining in-memory objects and their relationships to each other, the unit of work is responsible for maintaining the database representation of the in-memory objects.</b>  The typical pattern is that the manipulation of objects *is* the way that changes get communicated to the unit of work, so that when the commit occurs, the objects are already in their correct in-memory representation and problems dont arise.  The manipulation of identifier attributes like integer key values as well as deletes in particular are a frequent source of confusion.</p>
        
        <p>A terrific feature of SQLAlchemy which is also a supreme source of confusion is the backreference feature, described in <&formatting.myt:link, path="datamapping_relations_backreferences"&>.  This feature allows two types of objects to maintain attributes that reference each other, typically one object maintaining a list of elements of the other side, which contains a scalar reference to the list-holding object.  When you append an element to the list, the element gets a "backreference" back to the object which has the list.  When you attach the list-holding element to the child element, the child element gets attached to the list.  <b>This feature has nothing to do whatsoever with the Unit of Work.*</b>  It is strictly a small convenience feature intended to support the developer's manual manipulation of in-memory objects, and the backreference operation happens at the moment objects are attached or removed to/from each other, independent of any kind of database operation.  It does not change the golden rule, that the developer is reponsible for maintaining in-memory object relationships.</p>
        <p>* there is an internal relationship between two <code>relations</code> that have a backreference, which state that a change operation is only logged once to the unit of work instead of two separate changes since the two changes are "equivalent", so a backreference does affect the information that is sent to the Unit of Work.  But the Unit of Work itself has no knowledge of this arrangement and has no ability to affect it.</p>
        </&>
    </&>

    <&|doclib.myt:item, name="delete", description="Delete" &>
    <P>The delete call places an object or objects into the Unit of Work's list of objects to be marked as deleted:</p>
    <&|formatting.myt:code&>
        # mark three objects to be deleted
        objectstore.get_session().delete(obj1, obj2, obj3)
        
        # commit
        objectstore.get_session().commit()
    </&>
    <p>When objects which contain references to other objects are deleted, the mappers for those related objects will issue UPDATE statements for those objects that should no longer contain references to the deleted object, setting foreign key identifiers to NULL.  Similarly, when a mapper contains relations with the <code>private=True</code> option, DELETE statements will be issued for objects within that relationship in addition to that of the primary deleted object; this is called a <b>cascading delete</b>.</p>
    <p>As stated before, the purpose of delete is strictly to issue DELETE statements to the database.  It does not affect the in-memory structure of objects, other than changing the identifying attributes on objects, such as setting foreign key identifiers on updated rows to None.  It has no effect on the status of references between object instances, nor any effect on the Python garbage-collection status of objects.</p>
    </&>

    <&|doclib.myt:item, name="clear", description="Clear" &>
    <p>To clear out the current thread's UnitOfWork, which has the effect of discarding the Identity Map and the lists of all objects that have been modified, just issue a clear:
    </p>
    <&|formatting.myt:code&>
        # via module
        objectstore.clear()
        
        # or via Session
        objectstore.get_session().clear()
    </&>
    <p>This is the easiest way to "start fresh", as in a web application that wants to have a newly loaded graph of objects on each request.  Any object instances created before the clear operation should either be discarded or at least not used with any Mapper or Unit Of Work operations (with the exception of <code>import_instance()</code>), as they no longer have any relationship to the current Unit of Work, and their behavior with regards to the current session is undefined.</p>
    </&>

    <&|doclib.myt:item, name="refreshexpire", description="Refresh / Expire" &>
    <p>To assist with the Unit of Work's "sticky" behavior, individual objects can have all of their attributes immediately re-loaded from the database, or marked as "expired" which will cause a re-load to occur upon the next access of any of the object's mapped attributes.  This includes all relationships, so lazy-loaders will be re-initialized, eager relationships will be repopulated.  Any changes marked on the object are discarded:</p>
    <&|formatting.myt:code&>
        # immediately re-load attributes on obj1, obj2
        session.refresh(obj1, obj2)
        
        # expire objects obj1, obj2, attributes will be reloaded
        # on the next access:
        session.expire(obj1, obj2, obj3)
    </&>
    </&>
    
    <&|doclib.myt:item, name="expunge", description="Expunge" &>
    <P>Expunge simply removes all record of an object from the current Session.  This includes the identity map, and all history-tracking lists:</p>
    <&|formatting.myt:code&>
        session.expunge(obj1)
    </&>
    <p>Use <code>expunge</code> when youd like to remove an object altogether from memory, such as before calling <code>del</code> on it, which will prevent any "ghost" operations occuring when the session is committed.</p>
    </&>

    <&|doclib.myt:item, name="import", description="Import Instance" &>
        <p>The _instance_key attribute placed on object instances is designed to work with objects that are serialized into strings and brought back again.  As it contains no references to internal structures or database connections, applications that use caches or session storage which require serialization (i.e. pickling) can store SQLAlchemy-loaded objects.  However, as mentioned earlier, an object with a particular database identity is only allowed to exist uniquely within the current unit-of-work scope.  So, upon deserializing such an object, it has to "check in" with the current Session.  This is achieved via the <code>import_instance()</code> method:</p>
        <&|formatting.myt:code&>
            # deserialize an object
            myobj = pickle.loads(mystring)

            # "import" it.  if the objectstore already had this object in the 
            # identity map, then you get back the one from the current session.
            myobj = session.import_instance(myobj)
        </&>
    <p>Note that the import_instance() function will either mark the deserialized object as the official copy in the current identity map, which includes updating its _instance_key with the current application's class instance, or it will discard it and return the corresponding object that was already present.  Thats why its important to receive the return results from the method and use the result as the official object instance.</p>
    </&>

    <&|doclib.myt:item, name="begin", description="Begin" &>
    <p>The "scope" of the unit of work commit can be controlled further by issuing a begin().  A begin operation constructs a new UnitOfWork object and sets it as the currently used UOW.  It maintains a reference to the original UnitOfWork as its "parent", and shares the same identity map of objects that have been loaded from the database within the scope of the parent UnitOfWork.  However, the "new", "dirty", and "deleted" lists are empty.  This has the effect that only changes that take place after the begin() operation get logged to the current UnitOfWork, and therefore those are the only changes that get commit()ted.  When the commit is complete, the "begun" UnitOfWork removes itself and places the parent UnitOfWork as the current one again.</p>
<p>The begin() method returns a transactional object, upon which you can call commit() or rollback().  <b>Only this transactional object controls the transaction</b> - commit() upon the Session will do nothing until commit() or rollback() is called upon the transactional object.</p>
    <&|formatting.myt:code&>
        # modify an object
        myobj1.foo = "something new"
        
        # begin 
        trans = session.begin()
        
        # modify another object
        myobj2.lala = "something new"
        
        # only 'myobj2' is saved
        trans.commit()
    </&>
    <p>begin/commit supports the same "nesting" behavior as the SQLEngine (note this behavior is not the original "nested" behavior), meaning that many begin() calls can be made, but only the outermost transactional object will actually perform a commit().  Similarly, calls to the commit() method on the Session, which might occur in function calls within the transaction, will not do anything; this allows an external function caller to control the scope of transactions used within the functions.</p>
    </&>
    
    </&>

    <&|doclib.myt:item, name="advscope", description="Advanced UnitOfWork Management"&>

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


    <&|doclib.myt:item, name="object", description="Per-Object Sessions" &>
    <p>Sessions can be created on an ad-hoc basis and used for individual groups of objects and operations.  This has the effect of bypassing the normal thread-local Session and explicitly using a particular Session:</p>
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
    <&|doclib.myt:item, name="nested", description="Nested Transaction Sessions" &>
    <p>Sessions also now support a "nested transaction" feature whereby a second Session can use a different database connection.  This can be used inside of a larger database transaction to issue commits to the database that will be committed independently of the larger transaction's status:</p>
    <&|formatting.myt:code&>
        engine.begin()
        try:
            a = MyObj()
            b = MyObj()
            
            sess = Session(nest_on=engine)
            objectstore.push_session(sess)
            try:
                c = MyObj()
                objectstore.commit()    # will commit "c" to the database,
                                        # even if the external transaction rolls back
            finally:
                objectstore.pop_session()
            
            objectstore.commit()  # commit "a" and "b" to the database
            engine.commit()
        except:
            engine.rollback()
            raise
    </&>
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
