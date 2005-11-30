<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Advanced Data Mapping'</%attr>
<&|doclib.myt:item, name="adv_datamapping", description="Advanced Data Mapping" &>
<p>This section is under construction.  For now, it has just the basic recipe for each concept without much else.  </p>

<p>To start, heres the tables we will work with again:</p>
       <&|formatting.myt:code&>
        from sqlalchemy import *
        db = create_engine('sqlite', {'filename':'mydb'}, echo=True)
        
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

<&|doclib.myt:item, name="creating", description="Creating Mappers" &>
    <&|doclib.myt:item, name="customjoin", description="Custom Join Conditions" &>
        <&|formatting.myt:code&>
        </&>    
    </&>

    <&|doclib.myt:item, name="loadingoptions", description="Loading Options" &>
        <&|formatting.myt:code&>
        </&>    
    </&>

</&>

<&|doclib.myt:item, name="options", description="Mapper Options" &>
    <&|formatting.myt:code&>
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