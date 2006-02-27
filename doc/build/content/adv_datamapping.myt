<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Advanced Data Mapping'</%attr>
<&|doclib.myt:item, name="adv_datamapping", description="Advanced Data Mapping" &>
<p>This section details all the options available to Mappers, as well as advanced patterns.</p>

<p>To start, heres the tables we will work with again:</p>
       <&|formatting.myt:code&>
        from sqlalchemy import *
        db = create_engine('sqlite://filename=mydb', echo=True)
        
        # a table to store users
        users = Table('users', db,
            Column('user_id', Integer, primary_key = True),
            Column('user_name', String(40)),
            Column('password', String(80))
        )

        # a table that stores mailing addresses associated with a specific user
        addresses = Table('addresses', db,
            Column('address_id', Integer, primary_key = True),
            Column('user_id', Integer, ForeignKey("users.user_id")),
            Column('street', String(100)),
            Column('city', String(80)),
            Column('state', String(2)),
            Column('zip', String(10))
        )

        # a table that stores keywords
        keywords = Table('keywords', db,
            Column('keyword_id', Integer, primary_key = True),
            Column('name', VARCHAR(50))
        )

        # a table that associates keywords with users
        userkeywords = Table('userkeywords', db,
            Column('user_id', INT, ForeignKey("users")),
            Column('keyword_id', INT, ForeignKey("keywords"))
        )
       
       </&>

<&|doclib.myt:item, name="relations", description="More On Relations" &>
    <&|doclib.myt:item, name="customjoin", description="Custom Join Conditions" &>
        <p>When creating relations on a mapper, most examples so far have illustrated the mapper and relationship joining up based on the foreign keys of the tables they represent.  in fact, this "automatic" inspection can be completely circumvented using the <span class="codeline">primaryjoin</span> and <span class="codeline">secondaryjoin</span> arguments to <span class="codeline">relation</span>, as in this example which creates a User object which has a relationship to all of its Addresses which are in Boston:
        <&|formatting.myt:code&>
            class User(object):
                pass
            class Address(object):
                pass
            Address.mapper = mapper(Address, addresses)
            User.mapper = mapper(User, users, properties={
                'boston_addreses' : relation(Address.mapper, primaryjoin=
                            and_(users.c.user_id==Address.c.user_id, 
                            Addresses.c.city=='Boston'))
            })
        </&>
        <P>Many to many relationships can be customized by one or both of <span class="codeline">primaryjoin</span> and <span class="codeline">secondaryjoin</span>, shown below with just the default many-to-many relationship explicitly set:</p>
        <&|formatting.myt:code&>
        class User(object):
            pass
        class Keyword(object):
            pass
        Keyword.mapper = mapper(Keyword, keywords)
        User.mapper = mapper(User, users, properties={
            'keywords':relation(Keyword.mapper, 
                primaryjoin=users.c.user_id==userkeywords.c.user_id,
                secondaryjoin=userkeywords.c.keyword_id==keywords.c.keyword_id
                )
        })
        </&>
    </&>
    <&|doclib.myt:item, name="multiplejoin", description="Lazy/Eager Joins Multiple Times to One Table" &>

        <p>The previous example leads in to the idea of joining against the same table multiple times.  Below is a User object that has lists of its Boston and New York addresses, both lazily loaded when they are first accessed:</p>
        <&|formatting.myt:code&>
        User.mapper = mapper(User, users, properties={
            'boston_addreses' : relation(Address.mapper, primaryjoin=
                        and_(users.c.user_id==Address.c.user_id, 
                        Addresses.c.city=='Boston')),
            'newyork_addresses' : relation(Address.mapper, primaryjoin=
                        and_(users.c.user_id==Address.c.user_id, 
                        Addresses.c.city=='New York')),
        })
        </&>
        <p>A complication arises with the above pattern if you want the relations to be eager loaded.  Since there will be two separate joins to the addresses table during an eager load, an alias needs to be used to separate them.  You can create an alias of the addresses table to separate them, but then you are in effect creating a brand new mapper for each property, unrelated to the main Address mapper, which can create problems with commit operations.  So an additional argument <span class="codeline">use_alias</span> can be used with an eager relationship to specify the alias to be used just within the eager query:</p>
        <&|formatting.myt:code&>
        User.mapper = mapper(User, users, properties={
            'boston_addreses' : relation(Address.mapper, primaryjoin=
                        and_(User.c.user_id==Address.c.user_id, 
                        Addresses.c.city=='Boston'), lazy=False, use_alias=True),
            'newyork_addresses' : relation(Address.mapper, primaryjoin=
                        and_(User.c.user_id==Address.c.user_id, 
                        Addresses.c.city=='New York'), lazy=False, use_alias=True),
        })
        
        <&formatting.myt:poplink&>u = User.mapper.select()

        <&|formatting.myt:codepopper, link="sql" &>
        SELECT users.user_id AS users_user_id, users.user_name AS users_user_name, 
        users.password AS users_password, 
        addresses_EF45.address_id AS addresses_EF45_address_id, addresses_EF45.user_id AS addresses_EF45_user_id, 
        addresses_EF45.street AS addresses_EF45_street, addresses_EF45.city AS addresses_EF45_city, 
        addresses_EF45.state AS addresses_EF45_state, addresses_EF45.zip AS addresses_EF45_zip, 
        addresses_63C5.address_id AS addresses_63C5_address_id, addresses_63C5.user_id AS addresses_63C5_user_id, 
        addresses_63C5.street AS addresses_63C5_street, addresses_63C5.city AS addresses_63C5_city, 
        addresses_63C5.state AS addresses_63C5_state, addresses_63C5.zip AS addresses_63C5_zip 
        FROM users 
        LEFT OUTER JOIN addresses AS addresses_EF45 ON users.user_id = addresses_EF45.user_id 
        AND addresses_EF45.city = :addresses_city 
        LEFT OUTER JOIN addresses AS addresses_63C5 ON users.user_id = addresses_63C5.user_id 
        AND addresses_63C5.city = :addresses_city_1
        ORDER BY users.oid, addresses_EF45.oid, addresses_63C5.oid
        {'addresses_city_1': 'New York', 'addresses_city': 'Boston'}
        </&>
        </&>
    </&>

    <&|doclib.myt:item, name="relationoptions", description="Relation Options" &>
    Keyword options to the <span class="codeline">relation</span> function include:
    <ul>
        <li>lazy=(True|False|None) - specifies how the related items should be loaded.  a value of True indicates they should be loaded when the property is first accessed.  A value of False indicates they should be loaded by joining against the parent object query, so parent and child are loaded in one round trip.  A value of None indicates the related items are not loaded by the mapper in any case; the application will manually insert items into the list in some other way.  A relationship with lazy=None is still important; items added to the list or removed will cause the appropriate updates and deletes upon commit().</li>
        <li>primaryjoin - a ClauseElement that will be used as the primary join of this child object against the parent object, or in a many-to-many relationship the join of the primary object to the association table.  By default, this value is computed based on the foreign key relationships of the parent and child tables (or association table).</li>
        <li>secondaryjoin - a ClauseElement that will be used as the join of an association table to the child object.  By default, this value is computed based on the foreign key relationships of the association and child tables.</li>
        <li>foreignkey - specifies which column in this relationship is "foreign", i.e. which column refers to the parent object.  This value is automatically determined in all cases, based on the primary and secondary join conditions, except in the case of a self-referential mapper, where it is needed to indicate the child object's reference back to it's parent.</li>
        <li>uselist - a boolean that indicates if this property should be loaded as a list or a scalar.  In most cases, this value is determined based on the type and direction of the relationship - one to many forms a list, one to one forms a scalar, many to many is a list.  If a scalar is desired where normally a list would be present, set uselist to False.</li>
        <li>private - indicates if these child objects are "private" to the parent; removed items will also be deleted, and if the parent item is deleted, all child objects are deleted as well.  See the example in <&formatting.myt:link, path="datamapping_relations_private"&>.</li>
        <li>backreference - indicates the name of a property to be placed on the related mapper's class that will handle this relationship in the other direction, including synchronizing the object attributes on both sides of the relation.  See the example in <&formatting.myt:link, path="datamapping_relations_backreferences"&>.</li>
        <li>order_by - indicates the ordering that should be applied when loading these items.  See the section <&formatting.myt:link, path="adv_datamapping_orderby" &> for details.</li>
        <li>association - When specifying a many to many relationship with an association object, this keyword should reference the mapper of the target object of the association.  See the example in <&formatting.myt:link, path="datamapping_association"&>.</li>
        <li>post_update - this indicates that the relationship should be handled by a second UPDATE statement after an INSERT, or before a DELETE.  using this flag essentially means the relationship will not incur any "dependency" between parent and child item, as the particular foreign key relationship between them is handled by a second statement.  use this flag when a particular mapping arrangement will incur two rows that are dependent on each other, such as a table that has a one-to-many relationship to a set of child rows, and also has a column that references a single child row within that list (i.e. both tables contain a foreign key to each other).  If a commit() operation returns an error that a "cyclical dependency" was detected, this is a cue that you might want to use post_update.</li>
    </ul>
    </&>

</&>
<&|doclib.myt:item, name="orderby", description="Controlling Ordering" &>
<p>By default, mappers will not supply any ORDER BY clause when selecting rows.  This can be modified in several ways.</p>

<p>A "default ordering" can be supplied by all mappers, by enabling the "default_ordering" flag to the engine, which indicates that table primary keys or object IDs should be used as the default ordering:</p>
<&|formatting.myt:code&>
    db = create_engine('postgres://username=scott&password=tiger', default_ordering=True)
</&>
<p>The "order_by" parameter can be sent to a mapper, overriding the per-engine ordering if any.  A value of None means that the mapper should not use any ordering, even if the engine's default_ordering property is True.  A non-None value, which can be a column, an <span class="codeline">asc</span> or <span class="codeline">desc</span> clause, or an array of either one, indicates the ORDER BY clause that should be added to all select queries:</p>
<&|formatting.myt:code&>
    # disable all ordering
    mapper = mapper(User, users, order_by=None)

    # order by a column
    mapper = mapper(User, users, order_by=users.c.user_id)
    
    # order by multiple items
    mapper = mapper(User, users, order_by=[users.c.user_id, desc(users.c.user_name)])
</&>
<p>"order_by" can also be specified to an individual <span class="codeline">select</span> method, overriding all other per-engine/per-mapper orderings:
<&|formatting.myt:code&>
    # order by a column
    l = mapper.select(users.c.user_name=='fred', order_by=users.c.user_id)
    
    # order by multiple criterion
    l = mapper.select(users.c.user_name=='fred', order_by=[users.c.user_id, desc(users.c.user_name)])
</&>
<p>For relations, the "order_by" property can also be specified to all forms of relation:</p>
<&|formatting.myt:code&>
    # order address objects by address id
    mapper = mapper(User, users, properties = {
        'addresses' : relation(mapper(Address, addresses), order_by=addresses.c.address_id)
    })
    
    # eager load with ordering - the ORDER BY clauses of parent/child will be organized properly
    mapper = mapper(User, users, properties = {
        'addresses' : relation(mapper(Address, addresses), order_by=desc(addresses.c.email_address), eager=True)
    }, order_by=users.c.user_id)
    
</&>
</&>
<&|doclib.myt:item, name="limits", description="Limiting Rows" &>
<p>You can limit rows in a regular SQL query by specifying <span class="codeline">limit</span> and <span class="codeline">offset</span>.  A Mapper can handle the same concepts:</p>
<&|formatting.myt:code&>
    class User(object):
        pass
    
    m = mapper(User, users)
<&formatting.myt:poplink&>r = m.select(limit=20, offset=10)
<&|formatting.myt:codepopper, link="sql" &>SELECT users.user_id AS users_user_id, 
users.user_name AS users_user_name, users.password AS users_password 
FROM users ORDER BY users.oid 
 LIMIT 20 OFFSET 10
{}
</&>
</&>
However, things get tricky when dealing with eager relationships, since a straight LIMIT of rows does not represent the count of items when joining against other tables to load related items as well.  So here is what SQLAlchemy will do when you use limit or offset with an eager relationship:
    <&|formatting.myt:code&>
        class User(object):
            pass
        class Address(object):
            pass
        m = mapper(User, users, properties={
            'addresses' : relation(mapper(Address, addresses), lazy=False)
        })
        r = m.select(User.c.user_name.like('F%'), limit=20, offset=10)
<&|formatting.myt:poppedcode, link="sql" &>
SELECT users.user_id AS users_user_id, users.user_name AS users_user_name, 
users.password AS users_password, addresses.address_id AS addresses_address_id, 
addresses.user_id AS addresses_user_id, addresses.street AS addresses_street, 
addresses.city AS addresses_city, addresses.state AS addresses_state, 
addresses.zip AS addresses_zip 
FROM 
(SELECT users.user_id FROM users WHERE users.user_name LIKE %(users_user_name)s
ORDER BY users.oid LIMIT 20 OFFSET 10) AS rowcount, 
 users LEFT OUTER JOIN addresses ON users.user_id = addresses.user_id 
WHERE rowcount.user_id = users.user_id ORDER BY users.oid, addresses.oid
{'users_user_name': 'F%'}
    
    </&>
    </&>
    <p>The main WHERE clause as well as the limiting clauses are coerced into a subquery; this subquery represents the desired result of objects.  A containing query, which handles the eager relationships, is joined against the subquery to produce the result.</p>
</&>
<&|doclib.myt:item, name="colname", description="Overriding Column Names" &>
<p>When mappers are constructed, by default the column names in the Table metadata are used as the names of attributes on the mapped class.  This can be customzed within the properties by stating the key/column combinations explicitly:</p>
<&|formatting.myt:code&>
    user_mapper = mapper(User, users, properties={
        'id' : users.c.user_id,
        'name' : users.c.user_name,
    })
</&>
<p>In the situation when column names overlap in a mapper against multiple tables, columns may be referenced together with a list:
<&|formatting.myt:code&>
    # join users and addresses
    usersaddresses = sql.join(users, addresses, users.c.user_id == addresses.c.user_id)
    m = mapper(User, usersaddresses,   
        properties = {
            'id' : [users.c.user_id, addresses.c.user_id],
        }
        )
</&>
</&>
<&|doclib.myt:item, name="deferred", description="Deferred Column Loading" &>
<p>This feature allows particular columns of a table to not be loaded by default, instead being loaded later on when first referenced.  It is essentailly "column-level lazy loading".   This feature is useful when one wants to avoid loading a large text or binary field into memory when its not needed.  Individual columns can be lazy loaded by themselves or placed into groups that lazy-load together.</p>
<&|formatting.myt:code&>
    book_excerpts = Table('books', db, 
        Column('book_id', Integer, primary_key=True),
        Column('title', String(200), nullable=False),
        Column('summary', String(2000)),
        Column('excerpt', String),
        Column('photo', Binary)
    )

    class Book(object):
        pass
    
    # define a mapper that will load each of 'excerpt' and 'photo' in 
    # separate, individual-row SELECT statements when each attribute
    # is first referenced on the individual object instance
    book_mapper = mapper(Book, book_excerpts, properties = {
        'excerpt' : deferred(book_excerpts.c.excerpt),
        'photo' : deferred(book_excerpts.c.photo)
    })
</&>
<p>Deferred columns can be placed into groups so that they load together:</p>
<&|formatting.myt:code&>
    book_excerpts = Table('books', db, 
        Column('book_id', Integer, primary_key=True),
        Column('title', String(200), nullable=False),
        Column('summary', String(2000)),
        Column('excerpt', String),
        Column('photo1', Binary),
        Column('photo2', Binary),
        Column('photo3', Binary)
    )

    class Book(object):
        pass

    # define a mapper with a 'photos' deferred group.  when one photo is referenced,
    # all three photos will be loaded in one SELECT statement.  The 'excerpt' will 
    # be loaded separately when it is first referenced.
    book_mapper = mapper(Book, book_excerpts, properties = {
        'excerpt' : deferred(book_excerpts.c.excerpt),
        'photo1' : deferred(book_excerpts.c.photo1, group='photos'),
        'photo2' : deferred(book_excerpts.c.photo2, group='photos'),
        'photo3' : deferred(book_excerpts.c.photo3, group='photos')
    })
</&>
</&>
<&|doclib.myt:item, name="options", description="More on Mapper Options" &>
    <p>The <span class="codeline">options</span> method of mapper, first introduced in <&formatting.myt:link, path="datamapping_relations_options" &>, supports the copying of a mapper into a new one, with any number of its relations replaced by new ones.  The method takes a variable number of <span class="codeline">MapperOption</span> objects which know how to change specific things about the mapper.  The five available options are <span class="codeline">eagerload</span>, <span class="codeline">lazyload</span>, <span class="codeline">noload</span>, <span class="codeline">deferred</span> and <span class="codeline">extension</span>.</p>
    <P>An example of a mapper with a lazy load relationship, upgraded to an eager load relationship:
        <&|formatting.myt:code&>
        class User(object):
            pass
        class Address(object):
            pass
        
        # a 'lazy' relationship
        User.mapper = mapper(User, users, properties = {
            'addreses':relation(mapper(Address, addresses), lazy=True)
        })
    
        # copy the mapper and convert 'addresses' to be eager
        eagermapper = User.mapper.options(eagerload('addresses'))
        </&>
    
    <p>The load options also can take keyword arguments that apply to the new relationship.  To take the "double" address lazy relationship from the previous section and upgrade it to eager, adding the "selectalias" keywords as well:</p>
    <&|formatting.myt:code&>
        m = User.mapper.options(
                eagerload('boston_addresses', selectalias='boston_ad'), 
                eagerload('newyork_addresses', selectalias='newyork_ad')
            )
    </&>
    <p>The <span class="codeline">defer</span> and <span class="codeline">undefer</span> options can control the deferred loading of attributes:</p>
    <&|formatting.myt:code&>
        # set the 'excerpt' deferred attribute to load normally
        m = book_mapper.options(undefer('excerpt'))

        # set the referenced mapper 'photos' to defer its loading of the column 'imagedata'
        m = book_mapper.options(defer('photos.imagedata'))
    </&>
    
     
</&>


<&|doclib.myt:item, name="inheritance", description="Mapping a Class with Table Inheritance" &>

    <p>Table Inheritance indicates the pattern where two tables, in a parent-child relationship, are mapped to an inheritance chain of classes.  If a table "employees" contains additional information about managers in the table "managers", a corresponding object inheritance pattern would have an Employee class and a Manager class.  Loading a Manager object means you are joining managers to employees.  For SQLAlchemy, this pattern is just a special case of a mapper that maps against a joined relationship, and is provided via the <span class="codeline">inherits</span> keyword.
    <&|formatting.myt:code&>
        class User(object):
            """a user object."""
            pass
        User.mapper = mapper(User, users)

        class AddressUser(User):
            """a user object that also has the users mailing address."""
            pass

        # define a mapper for AddressUser that inherits the User.mapper, and joins on the user_id column
        AddressUser.mapper = mapper(
		AddressUser,
                addresses, inherits=User.mapper
                )
        
        items = AddressUser.mapper.select()
    </&>
<P>Above, the join condition is determined via the foreign keys between the users and the addresses table.  To specify the join condition explicitly, use <span class="codeline">inherit_condition</span>:
<&|formatting.myt:code&>
    AddressUser.mapper = mapper(
            AddressUser,
            addresses, inherits=User.mapper, 
            inherit_condition=users.c.user_id==addresses.c.user_id
        )
</&>    
</&>

<&|doclib.myt:item, name="joins", description="Mapping a Class against Multiple Tables" &>
    <P>The more general case of the pattern described in "table inheritance" is a mapper that maps against more than one table.  The <span class="codeline">join</span> keyword from the SQL package creates a neat selectable unit comprised of multiple tables, complete with its own composite primary key, which can be passed in to a mapper as the table.</p>
    <&|formatting.myt:code&>
        # a class
        class AddressUser(object):
            pass

        # define a Join
        j = join(users, addresses)
        
        # map to it - the identity of an AddressUser object will be 
        # based on (user_id, address_id) since those are the primary keys involved
        m = mapper(AddressUser, j)
    </&>    

    A second example:        
    <&|formatting.myt:code&>
        # many-to-many join on an association table
        j = join(users, userkeywords, 
                users.c.user_id==userkeywords.c.user_id).join(keywords, 
                   userkeywords.c.keyword_id==keywords.c.keyword_id)
         
        # a class 
        class KeywordUser(object):
            pass

        # map to it - the identity of a KeywordUser object will be
        # (user_id, keyword_id) since those are the primary keys involved
        m = mapper(KeywordUser, j)
    </&>    
</&>
<&|doclib.myt:item, name="selects", description="Mapping a Class against Arbitary Selects" &>
<p>Similar to mapping against a join, a plain select() object can be used with a mapper as well.  Below, an example select which contains two aggregate functions and a group_by is mapped to a class:</p>
    <&|formatting.myt:code&>
        s = select([customers, 
                    func.count(orders).label('order_count'), 
                    func.max(orders.price).label('highest_order')],
                    customers.c.customer_id==orders.c.customer_id,
                    group_by=[c for c in customers.c]
                    )
        class Customer(object):
            pass
        
        mapper = mapper(Customer, s)
    </&>
<p>Above, the "customers" table is joined against the "orders" table to produce a full row for each customer row, the total count of related rows in the "orders" table, and the highest price in the "orders" table, grouped against the full set of columns in the "customers" table.  That query is then mapped against the Customer class.  New instances of Customer will contain attributes for each column in the "customers" table as well as an "order_count" and "highest_order" attribute.  Updates to the Customer object will only be reflected in the "customers" table and not the "orders" table.  This is because the primary keys of the "orders" table are not represented in this mapper and therefore the table is not affected by save or delete operations.</p>
</&>
<&|doclib.myt:item, name="multiple", description="Multiple Mappers for One Class" &>
    <p>By now it should be apparent that the mapper defined for a class is in no way the only mapper that exists for that class.  Other mappers can be created at any time; either explicitly or via the <span class="codeline">options</span> method, to provide different loading behavior.</p>
    
    <p>However, its not as simple as that.  The mapper serves a dual purpose; one is to generate select statements and load objects from executing those statements; the other is to keep track of the defined dependencies of that object when save and delete operations occur, and to extend the attributes of the object so that they store information about their history and communicate with the unit of work system.  For this reason, it is a good idea to be aware of the behavior of multiple mappers.  When creating dependency relationships between objects, one should insure that only the primary mappers are used in those relationships, else deep object traversal operations will fail to load in the expected properties, and update operations will not take all the dependencies into account.  </p>
    
    <p>Generally its as simple as, the <i>first</i> mapper that is defined for a particular class is the one that gets to define that classes' relationships to other mapped classes, and also decorates its attributes and constructors with special behavior.  Any subsequent mappers created for that class will be able to load new instances, but object manipulation operations will still function via the original mapper.  The special keyword <span class="codeline">is_primary</span> will override this behavior, and make any mapper the new "primary" mapper.
    </p>
    <&|formatting.myt:code&>
        class User(object):
            pass
        
        # mapper one - mark it as "primary", meaning this mapper will handle
        # saving and class-level properties
        m1 = mapper(User, users, is_primary=True)
        
        # mapper two - this one will also eager-load address objects in
        m2 = mapper(User, users, properties={
                'addresses' : relation(mapper(Address, addresses), lazy=False)
            })
        
        # get a user.  this user will not have an 'addreses' property
        u1 = m1.select(User.c.user_id==10)
        
        # get another user.  this user will have an 'addreses' property.
        u2 = m2.select(User.c.user_id==27)
        
        # make some modifications, including adding an Address object.
        u1.user_name = 'jack'
        u2.user_name = 'jane'
        u2.addresses.append(Address('123 green street'))
        
        # upon commit, the User objects will be saved. 
        # the Address object will not, since the primary mapper for User
        # does not have an 'addresses' relationship defined
        objectstore.commit()
    </&>    
</&>
<&|doclib.myt:item, name="circular", description="Circular Mapping" &>
<p>Oftentimes it is necessary for two mappers to be related to each other.  With a datamodel that consists of Users that store Addresses, you might have an Address object and want to access the "user" attribute on it, or have a User object and want to get the list of Address objects.  The easiest way to do this is via the <span class="codeline">backreference</span> keyword described in <&formatting.myt:link, path="datamapping_relations_backreferences"&>.  Although even when backreferences are used, it is sometimes necessary to explicitly specify the relations on both mappers pointing to each other.</p>
<p>To achieve this involves creating the first mapper by itself, then creating the second mapper referencing the first, then adding references to the first mapper to reference the second:</p>
<&|formatting.myt:code&>
    class User(object):
        pass
    class Address(object):
        pass
    User.mapper = mapper(User, users)
    Address.mapper = mapper(Address, addresses, properties={
        'user':relation(User.mapper)
    })
    User.mapper.add_property('addresses', relation(Address.mapper))
</&>
<p>Note that with a circular relationship as above, you cannot declare both relationships as "eager" relationships, since that produces a circular query situation which will generate a recursion exception.  So what if you want to load an Address and its User eagerly?  Just make a second mapper using options:
<&|formatting.myt:code&>
    eagermapper = Address.mapper.options(eagerload('user'))
    s = eagermapper.select(Address.c.address_id==12)
</&>
</&>
<&|doclib.myt:item, name="recursive", description="Self Referential Mappers" &>
<p>A self-referential mapper is a mapper that is designed to operate with an <b>adjacency list</b> table.  This is a table that contains one or more foreign keys back to itself, and is usually used to create hierarchical tree structures.  SQLAlchemy's default model of saving items based on table dependencies is not sufficient in this case, as an adjacency list table introduces dependencies between individual rows.  Fortunately, SQLAlchemy will automatically detect a self-referential mapper and do the extra lifting to make it work. </p> 
    <&|formatting.myt:code&>
        # define a self-referential table
        trees = Table('treenodes', engine,
            Column('node_id', Integer, primary_key=True),
            Column('parent_node_id', Integer, ForeignKey('treenodes.node_id'), nullable=True),
            Column('node_name', String(50), nullable=False),
            )

        # treenode class
        class TreeNode(object):
            pass

        # mapper defines "children" property, pointing back to TreeNode class,
        # with the mapper unspecified.  it will point back to the primary 
        # mapper on the TreeNode class.
        TreeNode.mapper = mapper(TreeNode, trees, properties={
                'children' : relation(
                                TreeNode, 
                                private=True
                             ),
                }
            )
            
        # or, specify the circular relationship after establishing the original mapper:
        mymapper = mapper(TreeNode, trees)
        
        mymapper.add_property('children', relation(
                                mymapper, 
                                private=True
                             ))
        
    </&>    
    <p>This kind of mapper goes through a lot of extra effort when saving and deleting items, to determine the correct dependency graph of nodes within the tree.</p>
    
    <p>A self-referential mapper where there is more than one relationship on the table requires that all join conditions be explicitly spelled out.  Below is a self-referring table that contains a "parent_node_id" column to reference parent/child relationships, and a "root_node_id" column which points child nodes back to the ultimate root node:</p>
    <&|formatting.myt:code&>
    # define a self-referential table with several relations
    trees = Table('treenodes', engine,
        Column('node_id', Integer, primary_key=True),
        Column('parent_node_id', Integer, ForeignKey('treenodes.node_id'), nullable=True),
        Column('root_node_id', Integer, ForeignKey('treenodes.node_id'), nullable=True),
        Column('node_name', String(50), nullable=False),
        )

    # treenode class
    class TreeNode(object):
        pass

    # define the "children" property as well as the "root" property
    TreeNode.mapper = mapper(TreeNode, trees, properties={
            'children' : relation(
                            TreeNode, 
                            primaryjoin=trees.c.parent_node_id==trees.c.node_id
                            private=True
                         ),
            'root' : relation(
                    TreeNode,
                    primaryjoin=trees.c.root_node_id=trees.c.node_id, 
                    foreignkey=trees.c.node_id,
                    uselist=False
                )
            }
        )
    </&>    
<p>The "root" property on a TreeNode is a many-to-one relationship.  By default, a self-referential mapper declares relationships as one-to-many, so the extra parameter <span class="codeline">foreignkey</span>, pointing to the "many" side of a relationship, is needed to indicate a "many-to-one" self-referring relationship.</p>
<p>Both TreeNode examples above are available in functional form in the <span class="codeline">examples/adjacencytree</span> directory of the distribution.</p>    
</&>
<&|doclib.myt:item, name="resultset", description="Result-Set Mapping" &>
    <p>Take any result set and feed it into a mapper to produce objects.  Multiple mappers can be combined to retrieve unrelated objects from the same row in one step.  The <span class="codeline">instances</span> method on mapper takes a ResultProxy object, which is the result type generated from SQLEngine, and delivers object instances.</p>
    <&|formatting.myt:code, title="single object"&>
        class User(object):
            pass

        User.mapper = mapper(User, users)
        
        # select users
        c = users.select().execute()

        # get objects
        userlist = User.mapper.instances(c)
    </&>
    
    <&|formatting.myt:code, title="multiple objects"&>
        # define a second class/mapper
        class Address(object):
            pass
            
        Address.mapper = mapper(Address, addresses)

        # select users and addresses in one query
        s = select([users, addresses], users.c.user_id==addresses.c.user_id)

        # execute it, and process the results with the User mapper, chained to the Address mapper
        r = User.mapper.instances(s.execute(), Address.mapper)
        
        # result rows are an array of objects, one for each mapper used
        for entry in r:
            user = r[0]
            address = r[1]
    </&>    
</&>

<&|doclib.myt:item, name="extending", description="Extending Mapper" &>
<p>Mappers can have functionality augmented or replaced at many points in its execution via the usage of the MapperExtension class.  This class is just a series of "hooks" where various functionality takes place.  An application can make its own MapperExtension objects, overriding only the methods it needs.
        <&|formatting.myt:code&>
        class MapperExtension(object):
            def create_instance(self, mapper, row, imap, class_):
                """called when a new object instance is about to be created from a row.  
                the method can choose to create the instance itself, or it can return 
                None to indicate normal object creation should take place.
                
                mapper - the mapper doing the operation
                row - the result row from the database
                imap - a dictionary that is storing the running set of objects collected from the
                current result set
                class_ - the class we are mapping.
                """
            def append_result(self, mapper, row, imap, result, instance, isnew, populate_existing=False):
                """called when an object instance is being appended to a result list.
                
                If it returns True, it is assumed that this method handled the appending itself.

                mapper - the mapper doing the operation
                row - the result row from the database
                imap - a dictionary that is storing the running set of objects collected from the
                current result set
                result - an instance of util.HistoryArraySet(), which may be an attribute on an
                object if this is a related object load (lazy or eager).  use result.append_nohistory(value)
                to append objects to this list.
                instance - the object instance to be appended to the result
                isnew - indicates if this is the first time we have seen this object instance in the current result
                set.  if you are selecting from a join, such as an eager load, you might see the same object instance
                many times in the same result set.
                populate_existing - usually False, indicates if object instances that were already in the main 
                identity map, i.e. were loaded by a previous select(), get their attributes overwritten
                """
            def before_insert(self, mapper, instance):
                """called before an object instance is INSERTed into its table.
                
                this is a good place to set up primary key values and such that arent handled otherwise."""
            def after_insert(self, mapper, instance):
                """called after an object instance has been INSERTed"""
            def before_delete(self, mapper, instance):
                """called before an object instance is DELETEed"""
        
        </&>
        <p>To use MapperExtension, make your own subclass of it and just send it off to a mapper:</p>
        <&|formatting.myt:code&>
            mapper = mapper(User, users, extension=MyExtension())
        </&>
        <p>An existing mapper can create a copy of itself using an extension via the <span class="codeline">extension</span> option:
        <&|formatting.myt:code&>
            extended_mapper = mapper.options(extension(MyExtension()))
        </&>
        
</&>
<&|doclib.myt:item, name="class", description="How Mapper Modifies Mapped Classes" &>
<p>This section is a quick summary of what's going on when you send a class to the <span class="codeline">mapper()</span> function.  This material, not required to be able to use SQLAlchemy, is a little more dense and should be approached patiently!</p>

<p>The primary changes to a class that is mapped involve attaching property objects to it which represent table columns.  These property objects essentially track changes.  In addition, the __init__ method of the object is decorated to track object creates.</p>
<p>Here is a quick rundown of all the changes in code form:
    <&|formatting.myt:code&>
        # step 1 - override __init__ to 'register_new' with the Unit of Work
        oldinit = myclass.__init__
        def init(self, *args, **kwargs):
            nohist = kwargs.pop('_mapper_nohistory', False)
            oldinit(self, *args, **kwargs)
            if not nohist:
                # register_new with Unit Of Work
                objectstore.uow().register_new(self)
        myclass.__init__ = init
        
        # step 2 - set a string identifier that will 
        # locate the classes' primary mapper
        myclass._mapper = mapper.hashkey
        
        # step 3 - add column accessor
        myclass.c = mapper.columns

        # step 4 - attribute decorating.  
        # this happens mostly within the package sqlalchemy.attributes
        
        # this dictionary will store a series of callables 
        # that generate "history" containers for
        # individual object attributes
        myclass._class_managed_attributes = {}

        # create individual properties for each column - 
        # these objects know how to talk 
        # to the attribute package to create appropriate behavior.
        # the next example examines the attributes package more closely.
        myclass.column1 = SmartProperty().property('column1', uselist=False)
        myclass.column2 = SmartProperty().property('column2', uselist=True)
    </&>
<p>The attribute package is used when save operations occur to get a handle on modified values.  In the example below,
a full round-trip attribute tracking operation is illustrated:</p>
<&|formatting.myt:code&>
    import sqlalchemy.attributes as attributes
    
    # create an attribute manager.  
    # the sqlalchemy.mapping package keeps one of these around as 
    # 'objectstore.global_attributes'
    manager = attributes.AttributeManager()

    # regular old new-style class
    class MyClass(object):
        pass
    
    # register a scalar and a list attribute
    manager.register_attribute(MyClass, 'column1', uselist=False)
    manager.register_attribute(MyClass, 'column2', uselist=True)
        
    # create/modify an object
    obj = MyClass()
    obj.column1 = 'this is a new value'
    obj.column2.append('value 1')
    obj.column2.append('value 2')

    # get history objects
    col1_history = manager.get_history(obj, 'column1')
    col2_history = manager.get_history(obj, 'column2')

    # whats new ?
    >>> col1_history.added_items()
    ['this is a new value']
    
    >>> col2_history.added_items()
    ['value1', 'value2']
    
    # commit changes
    manager.commit(obj)

    # the new values become the "unchanged" values
    >>> col1_history.added_items()
    []

    >>> col1_history.unchanged_items()
    ['this is a new value']
    
    >>> col2_history.added_items()
    []

    >>> col2_history.unchanged_items()
    ['value1', 'value2']
</&>
<p>The above AttributeManager also includes a method <span class="codeline">value_changed</span> which is triggered whenever change events occur on the managed object attributes.  The Unit of Work (objectstore) package overrides this method in order to receive change events; its essentially this:</p>
<&|formatting.myt:code&>
    import sqlalchemy.attributes as attributes
    class UOWAttributeManager(attributes.AttributeManager):
        def value_changed(self, obj, key, value):
            if hasattr(obj, '_instance_key'):
                uow().register_dirty(obj)
            else:
                uow().register_new(obj)
                
    global_attributes = UOWAttributeManager()
</&>
<p>Objects that contain the attribute "_instance_key" are already registered with the Identity Map, and are assumed to have come from the database.  They therefore get marked as "dirty" when changes happen.  Objects without an "_instance_key" are not from the database, and get marked as "new" when changes happen, although usually this will already have occured via the object's __init__ method.</p>
</&>
</&>
