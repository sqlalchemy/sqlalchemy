<%flags>inherit='document_base.myt'</%flags>
<&|doclib.myt:item, name="metadata", description="Database Meta Data" &>
    <&|doclib.myt:item, name="tables", description="Describing Tables with MetaData" &>
    <p>The core of SQLAlchemy's query and object mapping operations is table metadata, which are Python objects that describe tables.  Metadata objects can be created by explicitly naming the table and all its properties, using the Table, Column, and ForeignKey objects: </p>
        <&|formatting.myt:code&>
        from sqlalchemy.schema import *
        import sqlalchemy.sqlite as sqlite
        engine = sqllite.engine(':memory:', {})
        
        users = Table('users', engine, 
            Column('user_id', Integer, primary_key = True),
            Column('user_name', String(16), nullable = False),
            Column('email_address', String(60), key='email'),
            Column('password', String(20), nullable = False)
        )
        
        user_prefs = Table('user_prefs', engine, 
            Column('pref_id', Integer, primary_key = True),
            Column('user_id', Integer, nullable = False, foreign_key = ForeignKey(users.c.user_id))
            Column('pref_name', String(40), nullable = False),
            Column('pref_value', String(100))
        )
        </&>

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
    <&|doclib.myt:item, name="building", description="Building and Dropping Database Tables" &>
    </&>
</&>