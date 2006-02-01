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
    <p>To get a hold of the current unit of work, its available inside a thread local registry object (an instance of <span class="codeline">sqlalchemy.util.ScopedRegistry</span>) in the objectstore package:</p>
        <&|formatting.myt:code&>
            u = objectstore.uow()
        </&>
    <p>You can also construct your own UnitOfWork object.  However, to get your mappers to talk to it, it has to be placed in the current thread-local scope:</p>
    <&|formatting.myt:code&>
        u = objectstore.UnitOfWork()
        objectstore.uow.set(u)
    </&>
    <p>Whatever unit of work is present in the registry can be cleared out, which will create a new one upon the next access:</p>
    <&|formatting.myt:code&>
        objectstore.uow.clear()
    </&>
    <p>The uow attribute also can be made to use "application" scope, instead of "thread" scope, meaning all threads will access the same instance of UnitOfWork:</p>
    <&|formatting.myt:code&>
        objectstore.uow.defaultscope = 'application'
    </&>
    <p>Although theres not much advantage to doing so, and also would make mapper usage not thread safe.</p>
    
    <p>The objectstore package includes many module-level methods which all operate upon the current UnitOfWork object.  These include begin(), commit(), clear(), delete(), has_key(), and import_instance(), which are described below.</p>
    </&>
    <&|doclib.myt:item, name="begincommit", description="Begin/Commit" &>
    <p>The current thread's UnitOfWork object keeps track of objects that are modified.  It maintains the following lists:</p>
    <&|formatting.myt:code&>
        # new objects that were just constructed
        objectstore.uow().new
        
        # objects that exist in the database, that were modified
        objectstore.uow().dirty
        
        # objects that have been marked as deleted via objectstore.delete()
        objectstore.uow().deleted
    </&>
    <p>To commit the changes stored in those lists, just issue a commit.  This can be called via <span class="codeline">objectstore.uow().commit()</span>, or through the module-level convenience method in the objectstore module:</p>
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
    <p>This feature should be used carefully, as it may result in an inconsistent save state between dependent objects (it should manage to locate loaded dependencies and save those also, but it hasnt been tested much).</p>
    
    <&|doclib.myt:item, name="begin", description="Controlling Scope with begin()" &>
    
    <p>The "scope" of the unit of work commit can be controlled further by issuing a begin().  A begin operation constructs a new UnitOfWork object and sets it as the currently used UOW.  It maintains a reference to the original UnitOfWork as its "parent", and shares the same "identity map" of objects that have been loaded from the database within the scope of the parent UnitOfWork.  However, the "new", "dirty", and "deleted" lists are empty.  This has the effect that only changes that take place after the begin() operation get logged to the current UnitOfWork, and therefore those are the only changes that get commit()ted.  When the commit is complete, the "begun" UnitOfWork removes itself and places the parent UnitOfWork as the current one again.</p>
    <&|formatting.myt:code&>
        # modify an object
        myobj1.foo = "something new"
        
        # begin an objectstore scope
        # this is equivalent to objectstore.uow().begin()
        objectstore.begin()
        
        # modify another object
        myobj2.lala = "something new"
        
        # only 'myobj2' is saved
        objectstore.commit()
    </&>
    <p>As always, the actual database transaction begin/commit occurs entirely within the objectstore.commit() operation.</p>
    
    <p>Since the begin/commit paradigm works in a stack-based manner, it follows that any level of nesting of begin/commit can be used:</p>
    <&|formatting.myt:code&>
        # start with UOW #1 as the thread-local UnitOfWork
        a = Foo()
        objectstore.begin()  # push UOW #2 on the stack
        b = Foo()
        objectstore.begin()  # push UOW #3 on the stack
        c = Foo()

        # saves 'c'
        objectstore.commit() # commit UOW #3

        d = Foo()

        # saves 'b' and 'd'
        objectstore.commit() # commit UOW #2
        
        # saves 'a', everything else prior to it
        objectstore.commit() # commit thread-local UOW #1
    </&>
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
    </&>
    <&|doclib.myt:item, name="import", description="Bringing External Instances into the UnitOfWork" &>
    </&>
    <&|doclib.myt:item, name="rollback", description="Rollback" &>
    </&>
</&>
