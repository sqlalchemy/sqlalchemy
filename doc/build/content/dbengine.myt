<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Database Engines'</%attr>

<&|doclib.myt:item, name="dbengine", description="Database Engines" &>
    <p>A database engine is a subclass of <span class="codeline">sqlalchemy.engine.SQLEngine</span>, and is the starting point for where SQLAlchemy provides a layer of abstraction on top of the various DBAPI2 database modules.  It serves as an abstract factory for database-specific implementation objects as well as a layer of abstraction over the most essential tasks of a database connection, including connecting, executing queries, returning result sets, and managing transactions.</p>
    
    <p>
    The average developer doesn't need to know anything about the interface or workings of a SQLEngine in order to use it.  Simply creating one, and then specifying it when constructing tables and other SQL objects is all that's needed. </p>
    
    <p>A SQLEngine is also a layer of abstraction on top of the connection pooling described in the previous section.  While a DBAPI connection pool can be used explicitly alongside a SQLEngine, its not really necessary.  Once you have a SQLEngine, you can retrieve pooled connections directly from its underlying connection pool via its own <span class="codeline">connection()</span> method.  However, if you're exclusively using SQLALchemy's SQL construction objects and/or object-relational mappers, all the details of connecting are handled by those libraries automatically.
    </p>
    <&|doclib.myt:item, name="establishing", description="Establishing a Database Engine" &>
    <p>
    Engines exist for SQLite, Postgres, MySQL, and Oracle, using the Pysqlite, Psycopg (1 or 2), MySQLDB, and cx_Oracle modules.  Each engine imports its corresponding module which is required to be installed.  For Postgres and Oracle, an alternate module may be specified at construction time as well.
    </p>
    <p>The string based argument names for connecting are translated to the appropriate names when the connection is made; argument names include "host" or "hostname" for database host, "database", "db", or "dbname" for the database name (also is dsn for Oracle), "user" or "username" for the user, and "password", "pw", or "passwd" for the password.  SQLite expects "filename" or "file" for the filename, or if None it defaults to "":memory:".</p>
    <p>The connection arguments can be specified as a string + dictionary pair, or a single URL-encoded string, as follows:</p>
    
    <&|formatting.myt:code&>
    from sqlalchemy import *

    # sqlite in memory    
    sqlite_engine = create_engine('sqlite', {'filename':':memory:'}, **opts)

    # via URL
    sqlite_engine = create_engine('sqlite://', **opts)
    
    # sqlite using a file
    sqlite_engine = create_engine('sqlite', {'filename':'querytest.db'}, **opts)

    # via URL
    sqlite_engine = create_engine('sqlite://filename=querytest.db', **opts)

    # postgres
    postgres_engine = create_engine('postgres', 
                            {'database':'test', 
                            'host':'127.0.0.1', 
                            'user':'scott', 
                            'password':'tiger'}, **opts)

    # via URL
    postgres_engine = create_engine('postgres://database=test&amp;host=127.0.0.1&amp;user=scott&amp;password=tiger')
    
    # mysql
    mysql_engine = create_engine('mysql',
                            {
                                'db':'mydb',
                                'user':'scott',
                                'passwd':'tiger',
                                'host':'127.0.0.1'
                            }
                            **opts)
    # oracle
    oracle_engine = create_engine('oracle', 
                            {'dsn':'mydsn', 
                            'user':'scott', 
                            'password':'tiger'}, **opts)
    

    </&>
    <p>Note that the general form of connecting to an engine is:</p>
    <&|formatting.myt:code &>
            # separate arguments
           engine = create_engine(
                        <enginename>, 
                        {<named DBAPI arguments>}, 
                        <sqlalchemy options>;
                    )
            
            # url
            engine = create_engine('&lt;enginename&gt;://&lt;named DBAPI arguments&gt;', <sqlalchemy options>)
    </&>
    </&>
    <&|doclib.myt:item, name="methods", description="Database Engine Methods" &>
    <p>A few useful methods off the SQLEngine are described here:</p>
        <&|formatting.myt:code&>
            engine = create_engine('postgres://hostname=localhost&user=scott&password=tiger&database=test')
            
            # get a pooled DBAPI connection
            conn = engine.connection()
            
            # create/drop tables based on table metadata objects
            # (see the next section, Table Metadata, for info on table metadata)
            engine.create(mytable)
            engine.drop(mytable)
            
            # get the DBAPI module being used
            dbapi = engine.dbapi()
            
            # get the default schema name
            name = engine.get_default_schema_name()
            
            # execute some SQL directly, returns a ResultProxy (see the SQL Construction section for details)
            result = engine.execute("select * from table where col1=:col1", {'col1':'foo'})
            
            # log a message to the engine's log stream
            engine.log('this is a message')
               
        </&>    
    </&>
    
    <&|doclib.myt:item, name="options", description="Database Engine Options" &>
    <p>The remaining arguments to <span class="codeline">create_engine</span> are keyword arguments that are passed to the specific subclass of <span class="codeline">sqlalchemy.engine.SQLEngine</span> being used,  as well as the underlying <span class="codeline">sqlalchemy.pool.Pool</span> instance.  All of the options described in the previous section <&formatting.myt:link, path="pooling_configuration"&> can be specified, as well as engine-specific options:</p>
    <ul>
        <li><p>pool=None : an instance of <span class="codeline">sqlalchemy.pool.Pool</span> to be used as the underlying source for connections, overriding the engine's connect arguments (pooling is described in the previous section).  If None, a default Pool (QueuePool or SingletonThreadPool as appropriate) will be created using the engine's connect arguments.</p>
        <p>Example:</p>
        <&|formatting.myt:code&>
            from sqlalchemy import *
            import sqlalchemy.pool as pool
            import MySQLdb
            
            def getconn():
                return MySQLdb.connect(user='ed', dbname='mydb')
                
            engine = create_engine('mysql', pool=pool.QueuePool(getconn, pool_size=20, max_overflow=40))
        </&></li>
        <li>echo=False : if True, the SQLEngine will log all statements as well as a repr() of their parameter lists to the engines logger, which defaults to sys.stdout.  A SQLEngine instances' "echo" data member can be modified at any time to turn logging on and off.  If set to the string 'debug', result rows will be printed to the standard output as well.</li>
        <li>logger=None : a file-like object where logging output can be sent, if echo is set to True.  This defaults to sys.stdout.</li>
        <li>module=None : used by Oracle and Postgres, this is a reference to a DBAPI2 module to be used instead of the engine's default module.  For Postgres, the default is psycopg2, or psycopg1 if 2 cannot be found.  For Oracle, its cx_Oracle.</li>
        <li>default_ordering=False : if True, table objects and associated joins and aliases will generate information used for ordering by primary keys (or OIDs, if the database supports OIDs).  This information is used by the Mapper system to when it constructs select queries to supply a default ordering to mapped objects.</li>
        <li>use_ansi=True : used only by Oracle;  when False, the Oracle driver attempts to support a particular "quirk" of some Oracle databases, that the LEFT OUTER JOIN SQL syntax is not supported, and the "Oracle join" syntax of using <% "<column1>(+)=<column2>" |h%> must be used in order to achieve a LEFT OUTER JOIN.  Its advised that the Oracle database be configured to have full ANSI support instead of using this feature.</li>
        <li>use_oids=False : used only by Postgres, will enable the column name "oid" as the object ID column.  Postgres as of 8.1 has object IDs disabled by default.</li>
        <li>convert_unicode=False : if set to True, all String/character based types will convert Unicode values to raw byte values going into the database, and all raw byte values to Python Unicode coming out in result sets.  This is an engine-wide method to provide unicode across the board.  For unicode conversion on a column-by-column level, use the Unicode column type instead.</li>
	<li>encoding='utf-8' : the encoding to use for Unicode translations - passed to all encode/decode methods.</li>
	<li>echo_uow=False : when True, logs unit of work commit plans to the standard output.</li>
    </ul>
    </&>
    <&|doclib.myt:item, name="proxy", description="Using the Proxy Engine" &>
    <p>The ProxyEngine is useful for applications that need to swap engines
    at runtime, or to create their tables and mappers before they know
    what engine they will use. One use case is an application meant to be
    pluggable into a mix of other applications, such as a WSGI
    application. Well-behaved WSGI applications should be relocatable; and
    since that means that two versions of the same application may be
    running in the same process (or in the same thread at different
    times), WSGI applications ought not to depend on module-level or
    global configuration. Using the ProxyEngine allows a WSGI application
    to define tables and mappers in a module, but keep the specific
    database connection uri as an application instance or thread-local
    value.</p>

    <p>The ProxyEngine is used in the same way as any other engine, with one
    additional method:</p>
    
    <&|formatting.myt:code&>
    # define the tables and mappers
    from sqlalchemy import *
    from sqlalchemy.ext.proxy import ProxyEngine

    engine = ProxyEngine()

    users = Table('users', engine, ... )

    class Users(object):
        pass

    assign_mapper(Users, users)

    def app(environ, start_response):
        # later, connect the proxy engine to a real engine via the connect() method
        engine.connect(environ['db_uri'])
        # now you have a real db connection and can select, insert, etc.
    </&>

    <&|doclib.myt:item, name="defaultproxy", description="Using the Global Proxy" &>
    <p>There is an instance of ProxyEngine available within the schema package as "default_engine".  You can construct Table objects and not specify the engine parameter, and they will connect to this engine by default.  To connect the default_engine, use the <span class="codeline">global_connect</span> function.</p>
    <&|formatting.myt:code&>
    # define the tables and mappers
    from sqlalchemy import *

    # specify a table with no explicit engine
    users = Table('users', 
            Column('user_id', Integer, primary_key=True),
            Column('user_name', String)
        )
        
    # connect the global proxy engine
    global_connect('sqlite://filename=foo.db')
    
    # create the table in the selected database
    users.create()
    </&>

    </&>
    </&>
    <&|doclib.myt:item, name="transactions", description="Transactions" &>
    <p>A SQLEngine also provides an interface to the transactional capabilities of the underlying DBAPI connection object, as well as the connection object itself.  Note that when using the object-relational-mapping package, described in a later section, basic transactional operation is handled for you automatically by its "Unit of Work" system;  the methods described here will usually apply just to literal SQL update/delete/insert operations or those performed via the SQL construction library.</p>
    
    <p>Typically, a connection is opened with "autocommit=False".  So to perform SQL operations and just commit as you go, you can simply pull out a connection from the connection pool, keep it in the local scope, and call commit() on it as needed.  As long as the connection remains referenced, all other SQL operations within the same thread will use this same connection, including those used by the SQL construction system as well as the object-relational mapper, both described in later sections:</p>
        <&|formatting.myt:code&>
            conn = engine.connection()

            # execute SQL via the engine
            engine.execute("insert into mytable values ('foo', 'bar')")
            conn.commit()

            # execute SQL via the SQL construction library            
            mytable.insert().execute(col1='bat', col2='lala')
            conn.commit()
            
        </&>
        
    <p>There is a more automated way to do transactions, and that is to use the engine's begin()/commit() functionality.  When the begin() method is called off the engine, a connection is checked out from the pool and stored in a thread-local context.  That way, all subsequent SQL operations within the same thread will use that same connection.  Subsequent commit() or rollback() operations are performed against that same connection.  In effect, its a more automated way to perform the "commit as you go" example above.  </p>
    
        <&|formatting.myt:code&>
            engine.begin()
            engine.execute("insert into mytable values ('foo', 'bar')")
            mytable.insert().execute(col1='foo', col2='bar')
            engine.commit()
        </&>

    <P>A traditional "rollback on exception" pattern looks like this:</p>    

        <&|formatting.myt:code&>
            engine.begin()
            try:
                engine.execute("insert into mytable values ('foo', 'bar')")
                mytable.insert().execute(col1='foo', col2='bar')
            except:
                engine.rollback()
                raise
            engine.commit()
        </&>
    
    <p>An shortcut which is equivalent to the above is provided by the <span class="codeline">transaction</span> method:</p>
    
        <&|formatting.myt:code&>
            def do_stuff():
                engine.execute("insert into mytable values ('foo', 'bar')")
                mytable.insert().execute(col1='foo', col2='bar')

            engine.transaction(do_stuff)
        </&>
    <p>An added bonus to the engine's transaction methods is "reentrant" functionality; once you call begin(), subsequent calls to begin() will increment a counter that must be decremented corresponding to each commit() statement before an actual commit can happen.  This way, any number of methods that want to insure a transaction can call begin/commit, and be nested arbitrarily:</p>
        <&|formatting.myt:code&>
            
            # method_a starts a transaction and calls method_b
            def method_a():
                engine.begin()
                try:
                    method_b()
                except:
                    engine.rollback()
                    raise
                engine.commit()

            # method_b starts a transaction, or joins the one already in progress,
            # and does some SQL
            def method_b():
                engine.begin()
                try:
                    engine.execute("insert into mytable values ('bat', 'lala')")
                    mytable.insert().execute(col1='bat', col2='lala')
                except:
                    engine.rollback()
                    raise
                engine.commit()
                
            # call method_a                
            method_a()                
            
        </&>
       <p>Above, method_a is called first, which calls engine.begin().  Then it calls method_b. When method_b calls engine.begin(), it just increments a counter that is decremented when it calls commit().  If either method_a or method_b calls rollback(), the whole transaction is rolled back.  The transaction is not committed until method_a calls the commit() method.</p>
       
       <p>The object-relational-mapper capability of SQLAlchemy includes its own <span class="codeline">commit()</span> method that gathers SQL statements into a batch and runs them within one transaction.  That transaction is also invokved within the scope of the "reentrant" methodology above; so multiple objectstore.commit() operations can also be bundled into a larger database transaction via the above methodology.</p>
    </&>
</&>
