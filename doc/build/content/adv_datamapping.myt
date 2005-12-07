<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Advanced Data Mapping'</%attr>
<&|doclib.myt:item, name="adv_datamapping", description="Advanced Data Mapping" &>
<p>This section is under construction.  For now, it has just the basic recipe for each concept without much else.  </p>

<p>To start, heres the tables w e will work with again:</p>
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

<&|doclib.myt:item, name="creatingrelations", description="Creating Mapper Relations" &>
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
        <p>A complication arises with the above pattern if you want the relations to be eager loaded.  Since there will be two separate joins to the addresses table during an eager load, an alias needs to be used to separate them.  You can create an alias of the addresses table to separate them, but then you are in effect creating a brand new mapper for each property, unrelated to the main Address mapper, which can create problems with commit operations.  So an additional argument <span class="codeline">selectalias</span> can be used with an eager relationship to specify the alias to be used just within the eager query:</p>
        <&|formatting.myt:code&>
        User.mapper = mapper(User, users, properties={
            'boston_addreses' : relation(Address.mapper, primaryjoin=
                        and_(User.c.user_id==Address.c.user_id, 
                        Addresses.c.city=='Boston'), lazy=False, selectalias='boston_ad'),
            'newyork_addresses' : relation(Address.mapper, primaryjoin=
                        and_(User.c.user_id==Address.c.user_id, 
                        Addresses.c.city=='New York'), lazy=False, selectalias='newyork_ad'),
        })
        
        <&formatting.myt:poplink&>u = User.mapper.select()

        <&|formatting.myt:codepopper, link="sql" &>
        SELECT users.user_id AS users_user_id, users.user_name AS users_user_name, 
        users.password AS users_password, 
        boston_ad.address_id AS boston_ad_address_id, boston_ad.user_id AS boston_ad_user_id, 
        boston_ad.street AS boston_ad_street, boston_ad.city AS boston_ad_city, 
        boston_ad.state AS boston_ad_state, boston_ad.zip AS boston_ad_zip, 
        newyork_ad.address_id AS newyork_ad_address_id, newyork_ad.user_id AS newyork_ad_user_id, 
        newyork_ad.street AS newyork_ad_street, newyork_ad.city AS newyork_ad_city, 
        newyork_ad.state AS newyork_ad_state, newyork_ad.zip AS newyork_ad_zip 
        FROM users 
        LEFT OUTER JOIN addresses AS boston_ad ON users.user_id = boston_ad.user_id 
        AND boston_ad.city = :addresses_city 
        LEFT OUTER JOIN addresses AS newyork_ad ON users.user_id = newyork_ad.user_id 
        AND newyork_ad.city = :addresses_city_1
        ORDER BY users.oid, boston_ad.oid, newyork_ad.oid
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
        <li>private - indicates if these child objects are "private" to the parent; removed items will also be deleted, and if the parent item is deleted, all child objects are deleted as well.</li>
        <li>live - a special type of "lazy load" where the list values will be loaded on every access.  A "live" property should be treated as read-only.  This type of property is useful in combination with "private" when used with a parent object which wants to force a delete of all its child items, attached or not, when it is deleted; since it always loads everything when accessed, you can be guaranteed that all child objects will be properly removed as well.</li>
        <li>association - When specifying a many to many relationship with an association object, this keyword should reference the mapper of the target object of the association.  See the example in <&formatting.myt:link, path="datamapping_association"&>.</li>
        <li>selectalias - Useful with eager loads, this specifies a table alias name that will be used when creating joins against the parent table.  The property is still created against the original table, and the aliased table is used only for the actual query.  Aliased columns in the result set are translated back to that of the original table when creating object instances.</li>
    </ul>
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
However, things get very tricky when dealing with eager relationships, since a straight LIMIT is not accurate with regards to child items.  So here is what SQLAlchemy will do when you use limit or offset with an eager relationship:
    <&|formatting.myt:code&>
        class User(object):
            pass
        class Address(object):
            pass
        m = mapper(User, users, properties={
            'addresses' : relation(Address, addresses, lazy=False)
        })
        r = m.select(limit=20, offset=10)
<&|formatting.myt:poppedcode, link="sql" &>
SELECT users.user_id AS users_user_id, users.user_name AS users_user_name, 
users.password AS users_password, addresses.address_id AS addresses_address_id, 
addresses.user_id AS addresses_user_id, addresses.street AS addresses_street, 
addresses.city AS addresses_city, addresses.state AS addresses_state, 
addresses.zip AS addresses_zip 
FROM 
(SELECT users.user_id FROM users ORDER BY users.oid LIMIT 20 OFFSET 10) AS rowcount, 
 users LEFT OUTER JOIN addresses ON users.user_id = addresses.user_id 
WHERE rowcount.user_id = users.user_id ORDER BY addresses.oid
{}
    
    </&>
    </&>
    <p>A subquery is used to create the limited set of rows, which is then joined to the larger eager query.</p>
</&>
<&|doclib.myt:item, name="options", description="Mapper Options" &>
    <P>The <span class="codeline">options</span> method of mapper produces a copy of the mapper, with modified properties and/or options.  This makes it easy to take a mapper and just change a few things on it.  The method takes a variable number of <span class="codeline">MapperOption</span> objects which know how to change specific things about the mapper.  The four available options are <span class="codeline">eagerload</span>, <span class="codeline">lazyload</span>, <span class="codeline">noload</span> and <span class="codeline">extension</span>.</p>
    <P>An example of a mapper with a lazy load relationship, upgraded to an eager load relationship:
        <&|formatting.myt:code&>
        class User(object):
            pass
        class Address(object):
            pass
        
        # a 'lazy' relationship
        User.mapper = mapper(User, users, properties = {
            'addreses':relation(Address, addresses, lazy=True)
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
</&>

<&|doclib.myt:item, name="custom", description="Custom Queries" &>
    <&|formatting.myt:code&>
        # a class
        class User(object):
            pass
            
        # basic mapper
        User.mapper = mapper(User, users)
        
        # basic select with criterion
        User.mapper.select(and_(users.c.user_name=='jane', users.c.user_id>12))
        
        # select with text criterion
        User.mapper.select("user_name='jane' and user_id>12")

        # select with totally textual query
        User.mapper.select_text("select user_id, user_name, password from users")
        
        # select with a Select object
        s = users.select(users, users.c.user_id==addresses.c.user_id)
        User.mapper.select(s)
    </&>    
</&>

<&|doclib.myt:item, name="inheritance", description="Mapping a Class with Table Inheritance" &>
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
                addresses, inherits = User.mapper, 
                inherit_condition=User.c.user_id==addresses.c.user_id
                )
        
        items = AddressUser.mapper.select()
    </&>    
</&>

<&|doclib.myt:item, name="joins", description="Mapping a Class against Multiple Tables" &>
    <&|formatting.myt:code&>
        # a class
        class AddressUser(object):
            pass

        # define a Join            
        j = join(users, addresses, users.c.address_id==addresses.c.address_id)
        
        # map to it - the identity of an AddressUser object will be 
        # based on (user_id, address_id) since those are the primary keys involved
        m = mapper(AddressUser, j)
        
        # more complex join
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
<&|doclib.myt:item, name="multiple", description="Multiple Mappers for One Class" &>
    <&|formatting.myt:code&>
        class User(object):
            pass
        
        # mapper one - mark it as "primary", meaning this mapper will handle
        # saving and class-level properties
        m1 = mapper(User, users, is_primary=True)
        
        # mapper two - this one will also eager-load address objects in
        m2 = mapper(User, users, properties={
                'addresses' : relation(Address, addresses, lazy=False)
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
<&|doclib.myt:item, name="recursive", description="Self Referential Mappers" &>
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
                                primaryjoin=tables.trees.c.parent_node_id==tables.trees.c.node_id, 
                                lazy=True, uselist=True, private=True
                             ),
                )
            )
            
        # or, specify the circular relationship after establishing the original mapper:
        mymapper = mapper(TreeNode, trees)
        
        mymapper.add_property('children', relation(
                                mymapper, 
                                primaryjoin=tables.trees.c.parent_node_id==tables.trees.c.node_id, 
                                lazy=True, uselist=True, private=True
                             ))
        
    </&>    
    <p>This kind of mapper goes through a lot of extra effort when saving and deleting items, to determine the correct dependency graph of nodes within the tree.</p>
</&>
<&|doclib.myt:item, name="circular", description="Circular Mapping" &>
<p>Oftentimes it is necessary for two mappers to be related to each other.  With a datamodel that consists of Users that store Addresses, you might have an Address object and want to access the "user" attribute on it, or have a User object and want to get the list of Address objects.  To achieve this involves creating the first mapper not referencing the second, then creating the second mapper referencing the first, then adding references to the first mapper to reference the second:</p>
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
<p>Note that with a circular relationship as above, you cannot declare both relationships as "eager" relationships, since that produces a circular query situation which will generate a recursion exception.  So what if you want to then load an Address and its User eagerly?  Just make a second mapper using options:
<&|formatting.myt:code&>
    eagermapper = Address.mapper.options(eagerload('user'))
    s = eagermapper.select(Address.c.address_id==12)
</&>
</&>
<&|doclib.myt:item, name="resultset", description="Result-Set Mapping" &>
    <p>Take any result set and feed it into a mapper to produce objects.  Multiple mappers can be combined to retrieve unrelated objects from the same row in one step.</p>
    <&|formatting.myt:code&>
        class User(object):
            pass
        class Address(object):
            pass
        User.mapper = mapper(User, users)
        Address.mapper = mapper(Address, addresses)
        
        # select users
        c = users.select().execute()
        # get objects
        userlist = User.mapper.instances(c)
        
        # select users and addresses in one query
        s = select([users, addresses], users.c.user_id==addresses.c.user_id)

        # execute it, and process the results with the User mapper, chained to the Address mapper
        r = User.mapper.instances(s.execute(), Address.mapper)
        # results rows are an array of objects, one for each mapper used
        for entry in r:
            user = r[0]
            address = r[1]
    </&>    
</&>

<&|doclib.myt:item, name="extending", description="Extending Mapper" &>
    <&|doclib.myt:item, name="class", description="How Mapper Modifies Mapped Classes" &>
        <&|formatting.myt:code&>
        </&>    
    </&>
    <&|doclib.myt:item, name="mapperextension", description="Adding/Replacing Functionality with Mapper Extension" &>
        <&|formatting.myt:code&>
        </&>    
    </&>
</&>

</&>