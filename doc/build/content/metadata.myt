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
            Column('pref_id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey("users.user_id"), nullable=False),
            Column('pref_name', String(40), nullable=False),
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
        
        # via string
        employees.c['employee_id']
        
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
            <&formatting.myt:poplink&>employees = Table('employees', engine, 
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

            <&formatting.myt:poplink&>employees.drop() <&|formatting.myt:codepopper, link="sql" &>
DROP TABLE employees
{}            </&>
        </&>    
    </&>

    
    <&|doclib.myt:item, name="defaults", description="Column Defaults and OnUpdates" &>
    <p>SQLAlchemy includes flexible constructs in which to create default values for columns upon the insertion of rows, as well as upon update.  These defaults can take several forms: a constant, a Python callable to be pre-executed before the SQL is executed, a SQL expression or function to be pre-executed before the SQL is executed, a pre-executed Sequence (for databases that support sequences), or a "passive" default, which is a default function triggered by the database itself upon insert, the value of which can then be post-fetched by the engine, provided the row provides a primary key in which to call upon.</p>
        <&|doclib.myt:item, name="oninsert", description="Pre-Executed Insert Defaults" &>
        <p>A basic default is most easily specified by the "default" keyword argument to Column:</p>
            <&|formatting.myt:code&>
                # a function to create primary key ids
                i = 0
                def mydefault():
                    i += 1
                    return i
                
                t = Table("mytable", db, 
                    # function-based default
                    Column('id', Integer, primary_key=True, default=mydefault),
                    
                    # a scalar default
                    Column('key', String(10), default="default")
                )
            </&>
        <p>The "default" keyword can also take SQL expressions, including select statements or direct function calls:</p>
            <&|formatting.myt:code&>
                t = Table("mytable", db, 
                    Column('id', Integer, primary_key=True),
                    
                    # define 'create_date' to default to now()
                    Column('create_date', DateTime, default=func.now()),
                    
                    # define 'key' to pull its default from the 'keyvalues' table
                    Column('key', String(20), default=keyvalues.select(keyvalues.c.type='type1', limit=1))
                    )
            </&>
            <p>The "default" keyword argument is shorthand for using a ColumnDefault object in a column definition.  This syntax is optional, but is required for other types of defaults, futher described below:</p>
            <&|formatting.myt:code&>
                Column('mycolumn', String(30), ColumnDefault(func.get_data()))
            </&>
        </&>

        <&|doclib.myt:item, name="onupdate", description="Pre-Executed OnUpdate Defaults" &>
        <p>Similar to an on-insert default is an on-update default, which is most easily specified by the "onupdate" keyword to Column, which also can be a constanct, plain Python function or SQL expression:</p>
            <&|formatting.myt:code&>
            t = Table("mytable", db, 
                Column('id', Integer, primary_key=True),
                
                # define 'last_updated' to be populated with current_timestamp (the ANSI-SQL version of now())
                Column('last_updated', DateTime, onupdate=func.current_timestamp()),
                )
            </&>
            <p>To use a ColumnDefault explicitly for an on-update, use the "for_update" keyword argument:</p>
            <&|formatting.myt:code&>
                Column('mycolumn', String(30), ColumnDefault(func.get_data(), for_update=True))
            </&>
        </&>
        
        <&|doclib.myt:item, name="passive", description="Inline Default Execution: PassiveDefault" &>
        <p>A PassiveDefault indicates a column default or on-update value that is executed automatically by the database.  This construct is used to specify a SQL function that will be specified as "DEFAULT" when creating tables, and also to indicate the presence of new data that is available to be "post-fetched" after an insert or update execution.</p>
        <&|formatting.myt:code&>
            t = Table('test', e, 
                Column('mycolumn', DateTime, PassiveDefault("sysdate"))
            )
        </&>
        <p>A create call for the above table will produce:</p>
        <&|formatting.myt:code&>
            CREATE TABLE test (
                    mycolumn datetime default sysdate
                )
        </&>
        <p>PassiveDefaults also send a message to the SQLEngine that data is available after update or insert.  The object-relational mapper system uses this information to post-fetch rows after insert or update, so that instances can be refreshed with the new data.  Below is a simplified version:</p>
        <&|formatting.myt:code&>
            # table with passive defaults
            mytable = Table('mytable', engine, 
                Column('my_id', Integer, primary_key=True),
                
                # an on-insert database-side default
                Column('data1', Integer, PassiveDefault("d1_func")),
                
                # an on-update database-side default
                Column('data2', Integer, PassiveDefault("d2_func", for_update=True))
                )
            # insert a row
            mytable.insert().execute(name='fred')
            
            # ask the engine: were there defaults fired off on that row ?
            if table.engine.lastrow_has_defaults():
                # postfetch the row based on primary key.
                # this only works for a table with primary key columns defined
                primary_key = table.engine.last_inserted_ids()
                row = table.select(table.c.id == primary_key[0])
        </&>
        <p>Tables that are reflected from the database which have default values set on them, will receive those defaults as PassiveDefaults.</p>

        <&|doclib.myt:item, name="postgres", description="The Catch: Postgres Primary Key Defaults always Pre-Execute" &>
        <p>Current Postgres support does not rely upon OID's to determine the identity of a row.  This is because the usage of OIDs has been deprecated with Postgres and they are disabled by default for table creates as of PG version 8.  Pyscopg2's "cursor.lastrowid" function only returns OIDs.  Therefore, when inserting a new row which has passive defaults set on the primary key columns, the default function is <b>still pre-executed</b> since SQLAlchemy would otherwise have no way of retrieving the row just inserted.</p>
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
        <p>The Sequence is used with Postgres or Oracle to indicate the name of a Sequence that will be used to create default values for a column.  When a table with a Sequence on a column is created by SQLAlchemy, the Sequence object is also created.   Similarly, the Sequence is dropped when the table is dropped.  Sequences are typically used with primary key columns.  When using Postgres, if an integer primary key column defines no explicit Sequence or other default method, SQLAlchemy will create the column with the SERIAL keyword, and will pre-execute a sequence named "tablename_columnname_seq" in order to retrieve new primary key values.   Oracle, which has no "auto-increment" keyword, requires that a Sequence be created for a table if automatic primary key generation is desired.  Note that for all databases, primary key values can always be explicitly stated within the bind parameters for any insert statement as well, removing the need for any kind of default generation function.</p>
    
    <p>A Sequence object can be defined on a Table that is then used for a non-sequence-supporting database.  In that case, the Sequence object is simply ignored.  Note that a Sequence object is <b>entirely optional for all databases except Oracle</b>, as other databases offer options for auto-creating primary key values, such as AUTOINCREMENT, SERIAL, etc.  SQLAlchemy will use these default methods for creating primary key values if no Sequence is present on the table metadata.</p>
    
    <p>A sequence can also be specified with <span class="codeline">optional=True</span> which indicates the Sequence should only be used on a database that requires an explicit sequence, and not those that supply some other method of providing integer values.  At the moment, it essentially means "use this sequence only with Oracle and not Postgres".</p>
        </&>
    </&>
    <&|doclib.myt:item, name="indexes", description="Defining Indexes" &>
    <p>Indexes can be defined on table columns, including named indexes, non-unique or unique, multiple column.  Indexes are included along with table create and drop statements.  They are not used for any kind of run-time constraint checking...SQLAlchemy leaves that job to the expert on constraint checking, the database itself.</p>
    <&|formatting.myt:code&>
        mytable = Table('mytable', engine, 
        
            # define a unique index 
            Column('col1', Integer, unique=True),
            
            # define a unique index with a specific name
            Column('col2', Integer, unique='mytab_idx_1'),
            
            # define a non-unique index
            Column('col3', Integer, index=True),
            
            # define a non-unique index with a specific name
            Column('col4', Integer, index='mytab_idx_2'),
            
            # pass the same name to multiple columns to add them to the same index
            Column('col5', Integer, index='mytab_idx_2'),

            Column('col6', Integer),
            Column('col7', Integer)
            )
        
        # create the table.  all the indexes will be created along with it.
        mytable.create()
        
        # indexes can also be specified standalone
        i = Index('mytab_idx_3', mytable.c.col6, mytable.c.col7, unique=False)
        
        # which can then be created separately (will also get created with table creates)
        i.create()
        
    </&>
    </&>
    <&|doclib.myt:item, name="adapting", description="Adapting Tables to Alternate Engines" &>
    <p>A Table object created against a specific engine can be re-created against a new engine using the <span class="codeline">toengine</span> method:</p>
    
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
        
        <p>Also available is the "database neutral" ansisql engine:</p>
        <&|formatting.myt:code&>
        import sqlalchemy.ansisql as ansisql
        generic_engine = ansisql.engine()

        users = Table('users', generic_engine, 
            Column('user_id', Integer),
            Column('user_name', String(50))
        )
        </&>        
    <p>Flexible "multi-engined" tables can also be achieved via the proxy engine, described in the section <&formatting.myt:link, path="dbengine_proxy"&>.</p>

    <&|doclib.myt:item, name="primitives", description="Non-engine primitives: TableClause/ColumnClause" &>
    
    <p>TableClause and ColumnClause are "primitive" versions of the Table and Column objects which dont use engines at all; applications that just want to generate SQL strings but not directly communicate with a database can use TableClause and ColumnClause objects (accessed via 'table' and 'column'), which are non-singleton and serve as the "lexical" base class of Table and Column:</p>
        <&|formatting.myt:code&>
            tab1 = table('table1',
                column('id'),
                column('name'))
            
            tab2 = table('table2',
                column('id'),
                column('email'))
                
            tab1.select(tab1.c.name == 'foo')
        </&>
        
    <p>TableClause and ColumnClause are strictly lexical.  This means they are fully supported within the full range of SQL statement generation, but they don't support schema concepts like creates, drops, primary keys, defaults, nullable status, indexes, or foreign keys.</p>
    </&>
    </&>

    
</&>
