<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Database Meta Data'</%attr>
<&|doclib.myt:item, name="metadata", description="Database Meta Data" &>
    <&|doclib.myt:item, name="tables", description="Describing Tables with MetaData" &>
    <p>The core of SQLAlchemy's query and object mapping operations is table metadata, which are Python objects that describe tables.  Metadata objects can be created by explicitly naming the table and all its properties, using the Table, Column, ForeignKey, and Sequence objects imported from <span class="codeline">sqlalchemy.schema</span>, and a database engine constructed as described in the previous section, or they can be automatically pulled from an existing database schema.  First, the explicit version: </p>
        <&|formatting.myt:code&>
        from sqlalchemy import *
        engine = create_engine('sqlite', {'filename':':memory:'}, **opts)
        
        users = Table('users', engine, 
            Column('user_id', Integer, primary_key = True),
            Column('user_name', String(16), nullable = False),
            Column('email_address', String(60), key='email'),
            Column('password', String(20), nullable = False)
        )
        
        user_prefs = Table('user_prefs', engine, 
            Column('pref_id', Integer, primary_key = True),
            Column('user_id', Integer, nullable = False, ForeignKey("users.user_id"))
            Column('pref_name', String(40), nullable = False),
            Column('pref_value', String(100))
        )
        </&>
        <p>The specific datatypes, such as Integer, String, etc. are defined in <&formatting.myt:link, path="types", text="sqlalchemy.types"&> and are automatically pulled in when you import * from <span class="codeline">sqlalchemy</span>.  Note that for Column objects, an altername name can be specified via the "key" parameter; if this parameter is given, then all programmatic references to this Column object will be based on its key, instead of its actual column name.</p>
        
        <p>Once constructed, the Table object provides a clean interface to the table's properties as well as that of its columns:
        
        <&|formatting.myt:code&>
        employees = Table('employees', engine, 
            Column('employee_id', Integer, primary_key=True),
            Column('employee_name', String(60), nullable=False, key='name'),
            Column('employee_dept', Integer, ForeignKey("departments.department_id"))
        )
        
        # access the column "EMPLOYEE_ID":
        employees.columns.employee_id
        
        # or just
        employees.c.employee_id
        
        # iterate through all columns
        for c in employees.c:
            # ...
            
        # get the table's primary key columns
        for primary_key in employees.primary_key:
            # ...
        
        # get the table's foreign key objects:
        for fkey in employees.foreign_keys:
            # ...
            
        # access the table's SQLEngine object:
        employees.engine
        
        # access a column's name, type, nullable, primary key, foreign key
        employees.c.employee_id.name
        employees.c.employee_id.type
        employees.c.employee_id.nullable
        employees.c.employee_id.primary_key
        employees.c.employee_dept.foreign_key
        
        # get the "key" of a column, which defaults to its name, but can 
        # be any user-defined string:
        employees.c.name.key
        
        # access a column's table:
        employees.c.employee_id.table is employees
        >>> True
        
        # get the table related by a foreign key
        fcolumn = employees.c.employee_dept.foreign_key.column.table
        </&>
        </p>
        
        <p>Metadata objects can also be <b>reflected</b> from tables that already exist in the database.  Reflection means based on a table name, the names, datatypes, and attributes of all columns, including foreign keys, will be loaded automatically.  This feature is supported by all database engines:</p>
        <&|formatting.myt:code&>
        >>> messages = Table('messages', engine, autoload = True)
        >>> [c.name for c in messages.columns]
        ['message_id', 'message_name', 'date']
        </&>
        
        <p>
        Note that if a reflected table has a foreign key referencing another table, then the metadata for the related table will be loaded as well, even if it has not been defined by the application:              
        </p>
        <&|formatting.myt:code&>
        >>> shopping_cart_items = Table('shopping_cart_items', engine, autoload = True)
        >>> print shopping_cart_items.c.cart_id.table.name
        shopping_carts
        </&>
        <p>To get direct access to 'shopping_carts', simply instantiate it via the Table constructor.  You'll get the same instance of the shopping cart Table as the one that is attached to shopping_cart_items:
        <&|formatting.myt:code&>
        >>> shopping_carts = Table('shopping_carts', engine)
        >>> shopping_carts is shopping_cart_items.c.cart_id.table.name
        True
        </&>
        <p>This works because when the Table constructor is called for a particular name and database engine, if the table has already been created then the instance returned will be the same as the original.  This is a <b>singleton</b> constructor:</p>
        <&|formatting.myt:code&>
        >>> news_articles = Table('news', engine, 
        ... Column('article_id', Integer, primary_key = True),
        ... Column('url', String(250), nullable = False)
        ... )
        >>> othertable = Table('news', engine)
        >>> othertable is news_articles
        True
        </&>
        
        

    </&>
    <&|doclib.myt:item, name="creating", description="Creating and Dropping Database Tables" &>
    <p>Creating and dropping is easy, just use the <span class="codeline">create()</span> and <span class="codeline">drop()</span> methods:
        <&|formatting.myt:code&>
            <&formatting.myt:poplink&>
            employees = Table('employees', engine, 
                Column('employee_id', Integer, primary_key=True),
                Column('employee_name', String(60), nullable=False, key='name'),
                Column('employee_dept', Integer, ForeignKey("departments.department_id"))
            )
            employees.create() <&|formatting.myt:codepopper, link="sql" &>
CREATE TABLE employees(
        employee_id SERIAL NOT NULL PRIMARY KEY,
        employee_name VARCHAR(60) NOT NULL,
        employee_dept INTEGER REFERENCES departments(department_id)
)

{}            </&>
            
            <&formatting.myt:poplink&>
            employees.drop() <&|formatting.myt:codepopper, link="sql" &>
DROP TABLE employees
{}            </&>
        </&>    
    </&>

    <&|doclib.myt:item, name="adapting", description="Adapting Tables to Alternate Engines" &>
    <p>Occasionally an application will need to reference the same tables within multiple databases simultaneously.  Since a Table object is specific to a SQLEngine, an extra method is provided to create copies of the Table object for a different SQLEngine instance, which can represent a different set of connection parameters, or a totally different database driver:
    
        <&|formatting.myt:code&>
        # create two engines
        sqlite_engine = create_engine('sqlite', {'filename':'querytest.db'})
        postgres_engine = create_engine('postgres', 
                            {'database':'test', 
                            'host':'127.0.0.1', 'user':'scott', 'password':'tiger'})
        
        # load 'users' from the sqlite engine
        users = Table('users', sqlite_engine, autoload=True)
        
        # create the same Table object for the other engine
        pg_users = users.toengine(postgres_engine)
        </&>
        
        <p>You can also create tables using a "database neutral" engine, which can serve as a starting point for tables that are then adapted to specific engines:</p>
        <&|formatting.myt:code&>
        import sqlalchemy.ansisql as ansisql
        generic_engine = ansisql.engine()

        users = Table('users', generic_engine, 
            Column('user_id', Integer),
            Column('user_name', String(50))
        )

        sqlite_engine = create_engine('sqlite', {'filename':'querytest.db'})
        sqlite_users = users.toengine(sqlite_engine)
        sqlite_users.create()
        </&>        
        
    </&>

    <&|doclib.myt:item, name="sequences", description="Defining Sequences" &>
    <P>A table with a sequence looks like:</p>
    <&|formatting.myt:code&>
        table = Table("cartitems", db, 
        Column("cart_id", Integer, Sequence('cart_id_seq'), primary_key=True),
        Column("description", String(40)),
        Column("createdate", DateTime())
    )
    </&>
    <p>Defining a Sequence means that it will be created along with the table.create() call, and more importantly the sequence will be explicitly used when inserting new rows for this table.  For databases that dont support sequences, the Sequence object has no effect.  A sequence can also be specified with <span class="codeline">optional=True</span> which indicates the Sequence should only be used on a database that requires an explicit sequence (which currently is just Oracle).  A database like Postgres, while it uses sequences to create primary keys, is often used via the SERIAL column option which removes the need for explicit access to the sequence.</p>
    </&>

</&>