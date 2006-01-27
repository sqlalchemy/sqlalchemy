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
    <&|doclib.myt:item, name="begincommit", description="Begin/Commit" &>
    </&>
    <&|doclib.myt:item, name="rollback", description="Rollback" &>
    </&>
</&>
