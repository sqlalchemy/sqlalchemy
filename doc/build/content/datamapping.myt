<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Data Mapping'</%attr>

<&|doclib.myt:item, name="datamapping", description="Basic Data Mapping" &>
<p>Data mapping describes the process of defining <b>Mapper</b> objects, which associate table metadata with user-defined classes.  

The Mapper's role is to perform SQL operations upon the database, associating individual table rows with instances of those classes, and individual database columns with properties upon those instances, to transparently associate in-memory objects with a persistent database representation. </p>

<p>When a Mapper is created to associate a Table object with a class, all of the columns defined in the Table object are associated with the class via property accessors, which add overriding functionality to the normal process of setting and getting object attributes.  These property accessors also keep track of changes to object attributes; these changes will be stored to the database when the application "commits" the current transactional context (known as a <b>Unit of Work</b>).  The <span class="codeline">__init__()</span> method of the object is also decorated to communicate changes when new instances of the object are created.</p>

<p>The Mapper also provides the interface by which instances of the object are loaded from the database.  The primary method for this is its <span class="codeline">select()</span> method, which has similar arguments to a <span class="codeline">sqlalchemy.sql.Select</span> object.  But this select method executes automatically and returns results, instead of awaiting an execute() call.  Instead of returning a cursor-like object, it returns an array of objects.</p>

<p>The three elements to be defined, i.e. the Table metadata, the user-defined class, and the Mapper, are typically defined as module-level variables, and may be defined in any fashion suitable to the application, with the only requirement being that the class and table metadata are described before the mapper.  For the sake of example, we will be defining these elements close together, but this should not be construed as a requirement; since SQLAlchemy is not a framework, those decisions are left to the developer or an external framework.
</p>
<&|doclib.myt:item, name="synopsis", description="Synopsis" &>
  <p>This is the simplest form of a full "round trip" of creating table meta data, creating a class, mapping the class to the table, getting some results, and saving changes.  For each concept, the following sections will dig in deeper to the available capabilities.</p>
        <&|formatting.myt:code&>
        from sqlalchemy import *
        
        # engine
        engine = create_engine("sqlite://mydb.db")
        
        # table metadata
        users = Table('users', engine, 
            Column('user_id', Integer, primary_key=True),
            Column('user_name', String(16)),
            Column('password', String(20))
        )

        # class definition 
        class User(object):
            pass
    
        # create a mapper
        usermapper = mapper(User, users)
        
        # select
<&formatting.myt:poplink&>user = usermapper.select_by(user_name='fred')[0]  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id AS users_user_id, users.user_name AS users_user_name, 
users.password AS users_password 
FROM users 
WHERE users.user_name = :users_user_name ORDER BY users.oid

{'users_user_name': 'fred'}
        </&>
        # modify
        user.user_name = 'fred jones'
        
        # commit - saves everything that changed
<&formatting.myt:poplink&>objectstore.commit() 
<&|formatting.myt:codepopper, link="sql" &>
UPDATE users SET user_name=:user_name 
 WHERE users.user_id = :user_id

[{'user_name': 'fred jones', 'user_id': 1}]        
        </&>
        
        
    </&>
    <&|doclib.myt:item, name="attaching", description="Attaching Mappers to their Class"&>
    <p>For convenience's sake, the Mapper can be attached as an attribute on the class itself as well:</p>
        <&|formatting.myt:code&>
            User.mapper = mapper(User, users)
            
            userlist = User.mapper.select_by(user_id=12)
        </&>
    <p>There is also a full-blown "monkeypatch" function that creates a primary mapper, attaches the above mapper class property, and also the  methods <span class="codeline">get, get_by, select, select_by, selectone, commit</span> and <span class="codeline">delete</span>:</p>
    <&|formatting.myt:code&>
        assign_mapper(User, users)
        userlist = User.select_by(user_id=12)
    </&>
    <p>Other methods of associating mappers and finder methods with their corresponding classes, such as via common base classes or mixins, can be devised as well.  SQLAlchemy does not aim to dictate application architecture and will always allow the broadest variety of architectural patterns, but may include more helper objects and suggested architectures in the future.</p>
    </&>
    <&|doclib.myt:item, name="overriding", description="Overriding Properties"&>
    <p>A common request is the ability to create custom class properties that override the behavior of setting/getting an attribute.  Currently, the easiest way to do this in SQLAlchemy is just how its done normally; define your attribute with a different name, such as "_attribute", and use a property to get/set its value.  The mapper just needs to be told of the special name:</p>
    <&|formatting.myt:code&>
        class MyClass(object):
            def _set_email(self, email):
                self._email = email
            def _get_email(self, email):
                return self._email
            email = property(_get_email, _set_email)
            
        m = mapper(MyClass, mytable, properties = {
                # map the '_email' attribute to the "email" column
                # on the table
                '_email': mytable.c.email
        })
    </&>
    <p>In a later release, SQLAlchemy will also allow _get_email and _set_email to be attached directly to the "email" property created by the mapper, and will also allow this association to occur via decorators.</p>
    </&>
</&>
<&|doclib.myt:item, name="selecting", description="Selecting from a Mapper" &>
    <p>There are a variety of ways to select from a mapper.  These range from minimalist to explicit.  Below is a synopsis of the these methods:</p>
        <&|formatting.myt:code&>
        # select_by, using property names or column names as keys
        # the keys are grouped together by an AND operator
        result = mapper.select_by(name='john', street='123 green street')

        # select_by can also combine SQL criterion with key/value properties
        result = mapper.select_by(users.c.user_name=='john', 
                addresses.c.zip_code=='12345, street='123 green street')
        
        # get_by, which takes the same arguments as select_by
        # returns a single scalar result or None if no results
        user = mapper.get_by(id=12)
        
        # "dynamic" versions of select_by and get_by - everything past the 
        # "select_by_" or "get_by_" is used as the key, and the function argument
        # as the value
        result = mapper.select_by_name('fred')
        u = mapper.get_by_name('fred')
        
        # get an object directly from its primary key.  this will bypass the SQL
        # call if the object has already been loaded
        u = mapper.get(15)
        
        # get an object that has a composite primary key of three columns.
        # the order of the arguments matches that of the table meta data.
        myobj = mapper.get(27, 3, 'receipts')
        
        # using a WHERE criterion
        result = mapper.select(or_(users.c.user_name == 'john', users.c.user_name=='fred'))
        
        # using a WHERE criterion to get a scalar
        u = mapper.selectone(users.c.user_name=='john')
        
        # using a full select object
        result = mapper.select(users.select(users.c.user_name=='john'))
        
        # using straight text  
        result = mapper.select_text("select * from users where user_name='fred'")

        # or using a "text" object
        result = mapper.select(text("select * from users where user_name='fred'", engine=engine))
        </&>    
    <p>The last few examples above show the usage of the mapper's table object to provide the columns for a WHERE Clause.  These columns are also accessible off of the mapped class directly.  When a mapper is assigned to a class, it also attaches a special property accessor <span class="codeline">c</span> to the class itself, which can be used just like the table metadata to access the columns of the table:</p>
        <&|formatting.myt:code&>
            User.mapper = mapper(User, users)
            
            userlist = User.mapper.select(User.c.user_id==12)
        </&>    
</&>
<&|doclib.myt:item, name="saving", description="Saving Objects" &>
    <p>When objects corresponding to mapped classes are created or manipulated, all changes are logged by a package called <span class="codeline">sqlalchemy.mapping.objectstore</span>.   The changes are then written to the database when an application calls <span class="codeline">objectstore.commit()</span>.  This pattern is known as a <b>Unit of Work</b>, and has many advantages over saving individual objects or attributes on those objects with individual method invocations.  Domain models can be built with far greater complexity with no concern over the order of saves and deletes, excessive database round-trips and write operations, or deadlocking issues.  The commit() operation uses a transaction as well, and will also perform "concurrency checking" to insure the proper number of rows were in fact affected (not supported with the current MySQL drivers). Transactional resources are used effectively in all cases; the unit of work handles all the details.</p>
    
    <p>When a mapper is created, the target class has its mapped properties decorated by specialized property accessors that track changes, and its <span class="codeline">__init__()</span> method is also decorated to mark new objects as "new".</p>
        <&|formatting.myt:code&>
            User.mapper = mapper(User, users)

            # create a new User
            myuser = User()
            myuser.user_name = 'jane'
            myuser.password = 'hello123'

            # create another new User      
            myuser2 = User()
            myuser2.user_name = 'ed'
            myuser2.password = 'lalalala'

            # load a third User from the database            
<&formatting.myt:poplink&>myuser3 = User.mapper.select(User.c.user_name=='fred')[0]  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id AS users_user_id, 
users.user_name AS users_user_name, users.password AS users_password
FROM users WHERE users.user_name = :users_user_name
{'users_user_name': 'fred'}
</&>
            myuser3.user_name = 'fredjones'

            # save all changes            
<&formatting.myt:poplink&>objectstore.commit()   
<&|formatting.myt:codepopper, link="sql" &>
UPDATE users SET user_name=:user_name
WHERE users.user_id =:users_user_id
[{'users_user_id': 1, 'user_name': 'fredjones'}]

INSERT INTO users (user_name, password) VALUES (:user_name, :password)
{'password': 'hello123', 'user_name': 'jane'}

INSERT INTO users (user_name, password) VALUES (:user_name, :password)
{'password': 'lalalala', 'user_name': 'ed'}
</&>
        </&>
    <p>In the examples above, we defined a User class with basically no properties or methods.  Theres no particular reason it has to be this way, the class can explicitly set up whatever properties it wants, whether or not they will be managed by the mapper.  It can also specify a constructor, with the restriction that the constructor is able to function with no arguments being passed to it (this restriction can be lifted with some extra parameters to the mapper; more on that later):</p>
        <&|formatting.myt:code&>
            class User(object):
                def __init__(self, user_name = None, password = None):
                    self.user_id = None
                    self.user_name = user_name
                    self.password = password
                def get_name(self):
                    return self.user_name
                def __repr__(self):
                    return "User id %s name %s password %s" % (repr(self.user_id), 
                        repr(self.user_name), repr(self.password))
            User.mapper = mapper(User, users)

            u = User('john', 'foo')
<&formatting.myt:poplink&>objectstore.commit()  
<&|formatting.myt:codepopper, link="sql" &>
INSERT INTO users (user_name, password) VALUES (:user_name, :password)
{'password': 'foo', 'user_name': 'john'}
</&>
            >>> u
            User id 1 name 'john' password 'foo'
                
        </&>

<p>Recent versions of SQLAlchemy will only put modified object attributes columns into the UPDATE statements generated upon commit.  This is to conserve database traffic and also to successfully interact with a "deferred" attribute, which is a mapped object attribute against the mapper's primary table that isnt loaded until referenced by the application.</p>
</&>

<&|doclib.myt:item, name="relations", description="Defining and Using Relationships" &>
<p>So that covers how to map the columns in a table to an object, how to load objects, create new ones, and save changes.  The next step is how to define an object's relationships to other database-persisted objects.  This is done via the <span class="codeline">relation</span> function provided by the mapper module.  So with our User class, lets also define the User has having one or more mailing addresses.  First, the table metadata:</p>
        <&|formatting.myt:code&>
        from sqlalchemy import *
        engine = create_engine('sqlite', {'filename':'mydb'})
        
        # define user table
        users = Table('users', engine, 
            Column('user_id', Integer, primary_key=True),
            Column('user_name', String(16)),
            Column('password', String(20))
        )
        
        # define user address table
        addresses = Table('addresses', engine,
            Column('address_id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey("users.user_id")),
            Column('street', String(100)),
            Column('city', String(80)),
            Column('state', String(2)),
            Column('zip', String(10))
        )
        </&>
<p>Of importance here is the addresses table's definition of a <b>foreign key</b> relationship to the users table, relating the user_id column into a parent-child relationship.  When a Mapper wants to indicate a relation of one object to another, this ForeignKey object is the default method by which the relationship is determined (although if you didn't define ForeignKeys, or you want to specify explicit relationship columns, that is available as well).   </p>
<p>So then lets define two classes, the familiar User class, as well as an Address class:

        <&|formatting.myt:code&>
            class User(object):
                def __init__(self, user_name = None, password = None):
                    self.user_name = user_name
                    self.password = password
                    
            class Address(object):
                def __init__(self, street=None, city=None, state=None, zip=None):
                    self.street = street
                    self.city = city
                    self.state = state
                    self.zip = zip
        </&>
<p>And then a Mapper that will define a relationship of the User and the Address classes to each other as well as their table metadata.  We will add an additional mapper keyword argument <span class="codeline">properties</span> which is a dictionary relating the name of an object property to a database relationship, in this case a <span class="codeline">relation</span> object against a newly defined  mapper for the Address class:</p>
        <&|formatting.myt:code&>
            User.mapper = mapper(User, users, properties = {
                                'addresses' : relation(mapper(Address, addresses))
                            }
                          )
        </&>
<p>Lets do some operations with these classes and see what happens:</p>

        <&|formatting.myt:code&>
            u = User('jane', 'hihilala')
            u.addresses.append(Address('123 anywhere street', 'big city', 'UT', '76543'))
            u.addresses.append(Address('1 Park Place', 'some other city', 'OK', '83923'))

            objectstore.commit()   
<&|formatting.myt:poppedcode, link="sql" &>INSERT INTO users (user_name, password) VALUES (:user_name, :password)
{'password': 'hihilala', 'user_name': 'jane'}

INSERT INTO addresses (user_id, street, city, state, zip) VALUES (:user_id, :street, :city, :state, :zip)
{'city': 'big city', 'state': 'UT', 'street': '123 anywhere street', 'user_id':1, 'zip': '76543'}

INSERT INTO addresses (user_id, street, city, state, zip) VALUES (:user_id, :street, :city, :state, :zip)
{'city': 'some other city', 'state': 'OK', 'street': '1 Park Place', 'user_id':1, 'zip': '83923'}
</&>
        </&>
<p>A lot just happened there!  The Mapper object figured out how to relate rows in the addresses table to the users table, and also upon commit had to determine the proper order in which to insert rows.  After the insert, all the User and Address objects have all their new primary and foreign keys populated.</p>

<p>Also notice that when we created a Mapper on the User class which defined an 'addresses' relation, the newly created User instance magically had an "addresses" attribute which behaved like a list.   This list is in reality a property accessor function, which returns an instance of <span class="codeline">sqlalchemy.util.HistoryArraySet</span>, which fulfills the full set of Python list accessors, but maintains a <b>unique</b> set of objects (based on their in-memory identity), and also tracks additions and deletions to the list:</p>
        <&|formatting.myt:code&>
            del u.addresses[1]
            u.addresses.append(Address('27 New Place', 'Houston', 'TX', '34839'))

            objectstore.commit()    

<&|formatting.myt:poppedcode, link="sql" &>UPDATE addresses SET user_id=:user_id
 WHERE addresses.address_id = :addresses_address_id
[{'user_id': None, 'addresses_address_id': 2}]

INSERT INTO addresses (user_id, street, city, state, zip) 
VALUES (:user_id, :street, :city, :state, :zip)
{'city': 'Houston', 'state': 'TX', 'street': '27 New Place', 'user_id': 1, 'zip': '34839'}
</&>            

        </&>
<&|doclib.myt:item, name="private", description="Useful Feature: Private Relations" &>
<p>So our one address that was removed from the list, was updated to have a user_id of <span class="codeline">None</span>, and a new address object was inserted to correspond to the new Address added to the User.  But now, theres a mailing address with no user_id floating around in the database of no use to anyone.  How can we avoid this ?  This is acheived by using the <span class="codeline">private=True</span> parameter of <span class="codeline">relation</span>:

        <&|formatting.myt:code&>
            User.mapper = mapper(User, users, properties = {
                                'addresses' : relation(mapper(Address, addresses), private=True)
                            }
                          )
            del u.addresses[1]
            u.addresses.append(Address('27 New Place', 'Houston', 'TX', '34839'))

            objectstore.commit()    <&|formatting.myt:poppedcode, link="sql" &>
INSERT INTO addresses (user_id, street, city, state, zip) 
VALUES (:user_id, :street, :city, :state, :zip)
{'city': 'Houston', 'state': 'TX', 'street': '27 New Place', 'user_id': 1, 'zip': '34839'}

DELETE FROM addresses WHERE addresses.address_id = :address_id
[{'address_id': 2}]
</&>            

        </&>
<p>In this case, with the private flag set, the element that was removed from the addresses list was also removed from the database.  By specifying the <span class="codeline">private</span> flag on a relation, it is indicated to the Mapper that these related objects exist only as children of the parent object, otherwise should be deleted.</p>
</&>
<&|doclib.myt:item, name="backreferences", description="Useful Feature: Backreferences" &>
<p>By creating relations with the <span class="codeline">backref</span> keyword, a bi-directional relationship can be created which will keep both ends of the relationship updated automatically, even without any database queries being executed.  Below, the User mapper is created with an "addresses" property, and the corresponding Address mapper receives a "backreference" to the User object via the property name "user":
        <&|formatting.myt:code&>
            Address.mapper = mapper(Address, addresses)
            User.mapper = mapper(User, users, properties = {
                                'addresses' : relation(Address.mapper, backref='user')
                            }
                          )

            u = User('fred', 'hi')
            a1 = Address('123 anywhere street', 'big city', 'UT', '76543')
            a2 = Address('1 Park Place', 'some other city', 'OK', '83923')
            
            # append a1 to u
            u.addresses.append(a1)
            
            # attach u to a2
            a2.user = u
            
            # the bi-directional relation is maintained
            >>> u.addresses == [a1, a2]
            True
            >>> a1.user is user and a2.user is user
            True
        </&>

+<p>The backreference feature also works with many-to-many relationships, which are described later.  When creating a backreference, a corresponding property is placed on the child mapper.  The default arguments to this property can be overridden using the <span class="codeline">backref()</span> function:
        <&|formatting.myt:code&>
            Address.mapper = mapper(Address, addresses)
            
            User.mapper = mapper(User, users, properties = {
                                'addresses' : relation(Address.mapper, backref=backref('user', lazy=False, private=True))
                            }
                          )
                </&>
<p>Note that when overriding a backreferenced property, we re-specify the backreference as well.  This will not override the existing 'addresses' property on the User class, but just sends a message to the attribute-management system that it should continue to maintain this backreference.</p>
</&>
<&|doclib.myt:item, name="cascade", description="Creating Relationships Automatically with cascade_mappers" &>
<p>The mapper package has a helper function <span class="codeline">cascade_mappers()</span> which can simplify the task of linking several mappers together.  Given a list of classes and/or mappers, it identifies the foreign key relationships between the given mappers or corresponding class mappers, and creates relation() objects representing those relationships, including a backreference.  Attempts to find
the "secondary" table in a many-to-many relationship as well.  The names of the relations
are a lowercase version of the related class.  In the case of one-to-many or many-to-many,
the name is "pluralized", which currently is based on the English language (i.e. an 's' or 
'es' added to it):</p>
    <&|formatting.myt:code&>
        # create two mappers.  the 'users' and 'addresses' tables have a foreign key
        # relationship
        mapper1 = mapper(User, users)
        mapper2 = mapper(Address, addresses)
        
        # cascade the two mappers together (can also specify User, Address as the arguments)
        cascade_mappers(mapper1, mapper2)
        
        # two new object instances
        u = User('user1')
        a = Address('test')
        
        # "addresses" and "user" property are automatically added
        u.addresses.append(a)
        print a.user
    </&>

</&>
    <&|doclib.myt:item, name="lazyload", description="Selecting from Relationships: Lazy Load" &>
    <P>We've seen how the <span class="codeline">relation</span> specifier affects the saving of an object and its child items, how does it affect selecting them?  By default, the relation keyword indicates that the related property should be attached a <b>Lazy Loader</b> when instances of the parent object are loaded from the database; this is just a callable function that when accessed will invoke a second SQL query to load the child objects of the parent.</p>
    
        <&|formatting.myt:code&>
            # define a mapper
            User.mapper = mapper(User, users, properties = {
                              'addresses' : relation(mapper(Address, addresses), private=True)
                            })
        
            # select users where username is 'jane', get the first element of the list
            # this will incur a load operation for the parent table
            user = User.mapper.select(user_name='jane')[0]   
            
<&|formatting.myt:poppedcode, link="sql" &>SELECT users.user_id AS users_user_id, 
users.user_name AS users_user_name, users.password AS users_password
FROM users WHERE users.user_name = :users_user_name ORDER BY users.oid
{'users_user_name': 'jane'}
</&>

            # iterate through the User object's addresses.  this will incur an
            # immediate load of those child items
            for a in user.addresses:    
<&|formatting.myt:poppedcode, link="sql" &>SELECT addresses.address_id AS addresses_address_id, 
addresses.user_id AS addresses_user_id, addresses.street AS addresses_street, 
addresses.city AS addresses_city, addresses.state AS addresses_state, 
addresses.zip AS addresses_zip FROM addresses
WHERE addresses.user_id = :users_user_id ORDER BY addresses.oid
{'users_user_id': 1}</&>            
                print repr(a)

        </&>    
        <&|doclib.myt:item, name="relselectby", description="Useful Feature: Creating Joins via select_by" &>
        <p>In mappers that have relationships, the <span class="codeline">select_by</span> method and its cousins include special functionality that can be used to create joins.  Just specify a key in the argument list which is not present in the primary mapper's list of properties or columns, but *is* present in the property list of one of its relationships:
        <&|formatting.myt:code&>
            <&formatting.myt:poplink&>l = User.mapper.select_by(street='123 Green Street')
<&|formatting.myt:codepopper, link="sql" &>SELECT users.user_id AS users_user_id, 
users.user_name AS users_user_name, users.password AS users_password
FROM users, addresses 
WHERE users.user_id=addresses.user_id
AND addresses.street=:addresses_street
ORDER BY users.oid
{'addresses_street', '123 Green Street'}
</&>
                </&>
                <p>The above example is shorthand for:</p>
        <&|formatting.myt:code&>
           l = User.mapper.select(and_(
                     Address.c.user_id==User.c.user_id, 
                     Address.c.street=='123 Green Street')
               )
                </&>
        
                </&>
        <&|doclib.myt:item, name="refreshing", description="How to Refresh the List?" &>
        <p>Once the child list of Address objects is loaded, it is done loading for the lifetime of the object instance.  Changes to the list will not be interfered with by subsequent loads, and upon commit those changes will be saved.  Similarly, if a new User object is created and child Address objects added, a subsequent select operation which happens to touch upon that User instance, will also not affect the child list, since it is already loaded.</p>
        
        <p>The issue of when the mapper actually gets brand new objects from the database versus when it assumes the in-memory version is fine the way it is, is a subject of <b>transactional scope</b>.  Described in more detail in the Unit of Work section, for now it should be noted that the total storage of all newly created and selected objects, <b>within the scope of the current thread</b>, can be reset via releasing or otherwise disregarding all current object instances, and calling:</p>
        <&|formatting.myt:code&>
            objectstore.clear()
        </&>
        <p>This operation will clear out all currently mapped object instances, and subsequent select statements will load fresh copies from the databse.</p>
        
        <p>To operate upon a single object, just use the <span class="codeline">remove</span> function:</p>
        <&|formatting.myt:code&>
            # (this function coming soon)
            objectstore.remove(myobject)
        </&>
        
        </&>
    </&>
    <&|doclib.myt:item, name="eagerload", description="Selecting from Relationships: Eager Load" &>
        <p>With just a single parameter "lazy=False" specified to the relation object, the parent and child SQL queries can be joined together.

        <&|formatting.myt:code&>
    Address.mapper = mapper(Address, addresses)
    User.mapper = mapper(User, users, properties = {
                        'addresses' : relation(Address.mapper, lazy=False)
                    }
                  )

    user = User.mapper.get_by(user_name='jane')

<&|formatting.myt:poppedcode, link="sql" &>SELECT users.user_id AS users_user_id, users.user_name AS users_user_name, 
users.password AS users_password, 
addresses.address_id AS addresses_address_id, addresses.user_id AS addresses_user_id, 
addresses.street AS addresses_street, addresses.city AS addresses_city, 
addresses.state AS addresses_state, addresses.zip AS addresses_zip
FROM users LEFT OUTER JOIN addresses ON users.user_id = addresses.user_id
WHERE users.user_name = :users_user_name ORDER BY users.oid, addresses.oid
{'users_user_name': 'jane'}
</&>
    for a in user.addresses:  
        print repr(a)

        </&>
        <P>Above, a pretty ambitious query is generated just by specifying that the User should be loaded with its child Addresses in one query.  When the mapper processes the results, it uses an <b>Identity Map</b> to keep track of objects that were already loaded, based on their primary key identity.  Through this method, the redundant rows produced by the join are organized into the distinct object instances they represent.</p>
        
        <p>The generation of this query is also immune to the effects of additional joins being specified in the original query.  To use our select_by example above, joining against the "addresses" table to locate users with a certain street results in this behavior:
        <&|formatting.myt:code&>
    users = User.mapper.select_by(street='123 Green Street')

<&|formatting.myt:poppedcode, link="sql" &>SELECT users.user_id AS users_user_id, 
users.user_name AS users_user_name, users.password AS users_password, 
addresses.address_id AS addresses_address_id, 
addresses.user_id AS addresses_user_id, addresses.street AS addresses_street, 
addresses.city AS addresses_city, addresses.state AS addresses_state, 
addresses.zip AS addresses_zip
FROM addresses AS addresses_417c, 
users LEFT OUTER JOIN addresses ON users.user_id = addresses.user_id
WHERE addresses_417c.street = :addresses_street 
AND users.user_id = addresses_417c.user_id 
ORDER BY users.oid, addresses.oid
{'addresses_street': '123 Green Street'}
</&>
            </&>        
            <p>The join implied by passing the "street" parameter is converted into an "aliasized" clause by the eager loader, so that it does not conflict with the join used to eager load the child address objects.</p>
    </&>
    <&|doclib.myt:item, name="options", description="Switching Lazy/Eager, No Load" &>
    <p>The <span class="codeline">options</span> method of mapper provides an easy way to get alternate forms of a mapper from an original one.  The most common use of this feature is to change the "eager/lazy" loading behavior of a particular mapper, via the functions <span class="codeline">eagerload()</span>, <span class="codeline">lazyload()</span> and <span class="codeline">noload()</span>:
    </p>
        <&|formatting.myt:code&>
          # user mapper with lazy addresses
          User.mapper = mapper(User, users, properties = {
                                 'addresses' : relation(mapper(Address, addresses))
                             }
                    )
                    
          # make an eager loader                    
          eagermapper = User.mapper.options(eagerload('addresses'))
          u = eagermapper.select()
          
          # make another mapper that wont load the addresses at all
          plainmapper = User.mapper.options(noload('addresses'))
          
          # multiple options can be specified
          mymapper = oldmapper.options(lazyload('tracker'), noload('streets'), eagerload('members'))

          # to specify a relation on a relation, separate the property names by a "."
          mymapper = oldmapper.options(eagerload('orders.items'))

                </&>    
    
    </&>

</&>


<&|doclib.myt:item, name="onetoone", description="One to One/Many to One" &>
<p>The above examples focused on the "one-to-many" relationship.  To do other forms of relationship is easy, as the <span class="codeline">relation</span> function can usually figure out what you want:</p>

        <&|formatting.myt:code&>
        # a table to store a user's preferences for a site
        prefs = Table('user_prefs', engine,
            Column('pref_id', Integer, primary_key = True),
            Column('stylename', String(20)),
            Column('save_password', Boolean, nullable = False),
            Column('timezone', CHAR(3), nullable = False)
        )

        # user table gets 'preference_id' column added
        users = Table('users', engine, 
            Column('user_id', Integer, primary_key = True),
            Column('user_name', String(16), nullable = False),
            Column('password', String(20), nullable = False),
            Column('preference_id', Integer, ForeignKey("prefs.pref_id"))
        )
        
        # class definition for preferences
        class UserPrefs(object):
            pass
        UserPrefs.mapper = mapper(UserPrefs, prefs)
        
        # address mapper
        Address.mapper = mapper(Address, addresses)
        
        # make a new mapper referencing everything.
        m = mapper(User, users, properties = dict(
            addresses = relation(Address.mapper, lazy=True, private=True),
            preferences = relation(UserPrefs.mapper, lazy=False, private=True),
        ))
        
        # select
<&formatting.myt:poplink&>user = m.get_by(user_name='fred')
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id AS users_user_id, users.user_name AS users_user_name, 
users.password AS users_password, users.preference_id AS users_preference_id, 
user_prefs.pref_id AS user_prefs_pref_id, user_prefs.stylename AS user_prefs_stylename, 
user_prefs.save_password AS user_prefs_save_password, user_prefs.timezone AS user_prefs_timezone 
FROM users LEFT OUTER JOIN user_prefs ON user_prefs.pref_id = users.preference_id 
WHERE users.user_name = :users_user_name ORDER BY users.oid, user_prefs.oid

{'users_user_name': 'fred'}
        </&>
        save_password = user.preferences.save_password
        
        # modify
        user.preferences.stylename = 'bluesteel'
<&formatting.myt:poplink&>user.addresses.append(Address('freddy@hi.org')) 
<&|formatting.myt:codepopper, link="sql" &>
SELECT email_addresses.address_id AS email_addresses_address_id, 
email_addresses.user_id AS email_addresses_user_id, 
email_addresses.email_address AS email_addresses_email_address 
FROM email_addresses 
WHERE email_addresses.user_id = :users_user_id 
ORDER BY email_addresses.oid, email_addresses.oid

{'users_user_id': 1}
        </&>
        # commit
        <&formatting.myt:poplink&>objectstore.commit() 
<&|formatting.myt:codepopper, link="sql" &>
UPDATE user_prefs SET stylename=:stylename
WHERE user_prefs.pref_id = :pref_id

[{'stylename': 'bluesteel', 'pref_id': 1}]

INSERT INTO email_addresses (address_id, user_id, email_address) 
VALUES (:address_id, :user_id, :email_address)

{'email_address': 'freddy@hi.org', 'address_id': None, 'user_id': 1}
</&>
    </&>
</&>

<&|doclib.myt:item, name="manytomany", description="Many to Many" &>
<p>The <span class="codeline">relation</span> function handles a basic many-to-many relationship when you specify the association table:</p>
        <&|formatting.myt:code&>
    articles = Table('articles', engine,
        Column('article_id', Integer, primary_key = True),
        Column('headline', String(150), key='headline'),
        Column('body', TEXT, key='body'),
    )

    keywords = Table('keywords', engine,
        Column('keyword_id', Integer, primary_key = True),
        Column('keyword_name', String(50))
    )

    itemkeywords = Table('article_keywords', engine,
        Column('article_id', Integer, ForeignKey("articles.article_id")),
        Column('keyword_id', Integer, ForeignKey("keywords.keyword_id"))
    )

    # class definitions
    class Keyword(object):
        def __init__(self, name = None):
            self.keyword_name = name

    class Article(object):
        pass
    
    # define a mapper that does many-to-many on the 'itemkeywords' association 
    # table
    Article.mapper = mapper(Article, articles, properties = dict(
            keywords = relation(mapper(Keyword, keywords), itemkeywords, lazy=False)
            )
        )

    article = Article()
    article.headline = 'a headline'
    article.body = 'this is the body'
    article.keywords.append(Keyword('politics'))
    article.keywords.append(Keyword('entertainment'))
            <&formatting.myt:poplink&>
    objectstore.commit()   <&|formatting.myt:codepopper, link="sql" &>
INSERT INTO keywords (name) VALUES (:name)

{'name': 'politics'}

INSERT INTO keywords (name) VALUES (:name)

{'name': 'entertainment'}

INSERT INTO articles (article_headline, article_body) VALUES (:article_headline, :article_body)

{'article_body': 'this is the body', 'article_headline': 'a headline'}

INSERT INTO article_keywords (article_id, keyword_id) VALUES (:article_id, :keyword_id)

[{'keyword_id': 1, 'article_id': 1}, {'keyword_id': 2, 'article_id': 1}]
</&>
    # select articles based on a keyword.  select_by will handle the extra joins.
    <&formatting.myt:poplink&>articles = Article.mapper.select_by(keyword_name='politics')
<&|formatting.myt:codepopper, link="sql" &>
SELECT articles.article_id AS articles_article_id, 
articles.article_headline AS articles_article_headline, 
articles.article_body AS articles_article_body, 
keywords.keyword_id AS keywords_keyword_id, 
keywords.keyword_name AS keywords_keyword_name 
FROM keywords AS keywords_f008, 
article_keywords AS article_keywords_dbf0, 
articles LEFT OUTER JOIN article_keywords ON 
articles.article_id = article_keywords.article_id 
LEFT OUTER JOIN keywords ON 
keywords.keyword_id = article_keywords.keyword_id 
WHERE (keywords_f008.keyword_name = :keywords_keyword_name 
AND articles.article_id = article_keywords_dbf0.article_id) 
AND keywords_f008.keyword_id = article_keywords_dbf0.keyword_id 
ORDER BY articles.oid, article_keywords.oid 
{'keywords_keyword_name': 'politics'}
</&>
    # modify
    a = articles[0]
    del a.keywords[:]
    a.keywords.append(Keyword('topstories'))
    a.keywords.append(Keyword('government'))

    # commit.  individual INSERT/DELETE operations will take place only for the list
    # elements that changed.
<&formatting.myt:poplink&>    
    objectstore.commit()   
<&|formatting.myt:codepopper &>
INSERT INTO keywords (name) VALUES (:name)

{'name': 'topstories'}

INSERT INTO keywords (name) VALUES (:name)

{'name': 'government'}

DELETE FROM article_keywords 
WHERE article_keywords.article_id = :article_id 
AND article_keywords.keyword_id = :keyword_id

[{'keyword_id': 1, 'article_id': 1}, {'keyword_id': 2, 'article_id': 1}]

INSERT INTO article_keywords (article_id, keyword_id) VALUES (:article_id, :keyword_id)

[{'keyword_id': 3, 'article_id': 1}, {'keyword_id': 4, 'article_id': 1}]
</&>

    
        </&>
</&>
<&|doclib.myt:item, name="association", description="Association Object" &>

        <p>Many to Many can also be done with an association object, that adds additional information about how two items are related.  This association object is set up in basically the same way as any other mapped object.  However, since an association table typically has no primary key columns, you have to tell the mapper what columns will compose its "primary key", which are the two (or more) columns involved in the association.  Also, the relation function needs an additional hint as to the fact that this mapped object is an association object, via the "association" argument which points to the class or mapper representing the other side of the association.</p>
        <&|formatting.myt:code&>
            # add "attached_by" column which will reference the user who attached this keyword
            itemkeywords = Table('article_keywords', engine,
                Column('article_id', Integer, ForeignKey("articles.article_id")),
                Column('keyword_id', Integer, ForeignKey("keywords.keyword_id")),
                Column('attached_by', Integer, ForeignKey("users.user_id"))
            )

            # define an association class
            class KeywordAssociation(object):
                pass

            # mapper for KeywordAssociation
            # specify "primary key" columns manually
            KeywordAssociation.mapper = mapper(KeywordAssociation, itemkeywords,
                    primary_key = [itemkeywords.c.article_id, itemkeywords.c.keyword_id],
                    properties={
                        'keyword' : relation(Keyword, lazy = False), # uses primary Keyword mapper
                        'user' : relation(User, lazy = True) # uses primary User mapper
                    }
            )
            
            # mappers for Users, Keywords
            User.mapper = mapper(User, users)
            Keyword.mapper = mapper(Keyword, keywords)
            
            # define the mapper. 
            m = mapper(Article, articles, properties={
                'keywords':relation(KeywordAssociation.mapper, lazy=False, association=Keyword)
		}
            )
            
            # bonus step - well, we do want to load the users in one shot, 
            # so modify the mapper via an option.
            # this returns a new mapper with the option switched on.
            m2 = mapper.options(eagerload('keywords.user'))
            
            # select by keyword again
            <&formatting.myt:poplink&>alist = m2.select_by(keyword_name='jacks_stories')
<&|formatting.myt:codepopper, link="sql" &>
SELECT articles.article_id AS articles_article_id, 
articles.article_headline AS articles_article_headline, 
articles.article_body AS articles_article_body, 
article_keywords.article_id AS article_keywords_article_id, 
article_keywords.keyword_id AS article_keywords_keyword_id, 
article_keywords.attached_by AS article_keywords_attached_by, 
users.user_id AS users_user_id, users.user_name AS users_user_name, 
users.password AS users_password, users.preference_id AS users_preference_id, 
keywords.keyword_id AS keywords_keyword_id, keywords.name AS keywords_name 
FROM article_keywords article_keywords_3a64, keywords keywords_11b7, 
articles LEFT OUTER JOIN article_keywords ON articles.article_id = article_keywords.article_id 
LEFT OUTER JOIN users ON users.user_id = article_keywords.attached_by 
LEFT OUTER JOIN keywords ON keywords.keyword_id = article_keywords.keyword_id 
WHERE keywords_11b7.keyword_id = article_keywords_3a64.keyword_id 
AND article_keywords_3a64.article_id = articles.article_id 
AND keywords_11b7.name = :keywords_name 
ORDER BY articles.oid, article_keywords.oid, users.oid, keywords.oid

{'keywords_name': 'jacks_stories'}
</&>
            # user is available
            for a in alist:
                for k in a.keywords:
                    if k.keyword.name == 'jacks_stories':
                        print k.user.user_name
            
</&>
        
</&>

</&>
