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
    <p>An example of connecting to each engine is as follows:</p>
    
    <&|formatting.myt:code&>
    from sqlalchemy import *

    # sqlite in memory    
    sqlite_engine = create_engine('sqlite', {'filename':':memory:'}, **opts)
    
    # sqlite using a file
    sqlite_engine = create_engine('sqlite', {'filename':'querytest.db'}, **opts)

    # postgres
    postgres_engine = create_engine('postgres', 
                            {'database':'test', 
                            'host':'127.0.0.1', 
                            'user':'scott', 
                            'password':'tiger'}, **opts)

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
    <&|formatting.myt:code&>
           engine = create_engine(
                        <enginename>, 
                        {<named DBAPI arguments>}, 
                        <sqlalchemy options>
                    )
    </&>
    <p>The second argument is a dictionary whose key/value pairs will be passed to the underlying DBAPI connect() method as keyword arguments.  Any keyword argument supported by the DBAPI module can be in this dictionary.</p>
    <p>Engines can also be loaded by URL.  The above format is converted into <span class="codeline"><% '<enginename>://key=val&key=val' |h %></span>:
        <&|formatting.myt:code&>
            sqlite_engine = create_engine('sqlite://filename=querytest.db')
            postgres_engine = create_engine('postgres://database=test&user=scott&password=tiger')
        </&>
    </p>
    </&>
    <&|doclib.myt:item, name="options", description="Database Engine Options" &>
    <p>The remaining arguments to <span class="codeline">create_engine</span> are keyword arguments that are passed to the specific subclass of <span class="codeline">sqlalchemy.engine.SQLEngine</span> being used,  as well as the underlying <span class="codeline">sqlalchemy.pool.Pool</span> instance.  All of the options described in the previous section <&formatting.myt:link, path="pooling_configuration"&> can be specified, as well as engine-specific options:</p>
    <ul>
        <li>pool=None : an instance of <span class="codeline">sqlalchemy.pool.DBProxy</span> to be used as the underlying source for connections (DBProxy is described in the previous section).  If None, a default DBProxy will be created using the engine's own database module with the given arguments.</li>
        <li>echo=False : if True, the SQLEngine will log all statements as well as a repr() of their parameter lists to the engines logger, which defaults to sys.stdout.  A SQLEngine instances' "echo" data member can be modified at any time to turn logging on and off.  If set to the string 'debug', result rows will be printed to the standard output as well.</li>
        <li>logger=None : a file-like object where logging output can be sent, if echo is set to True.  This defaults to sys.stdout.</li>
        <li>module=None : used by Oracle and Postgres, this is a reference to a DBAPI2 module to be used instead of the engine's default module.  For Postgres, the default is psycopg2, or psycopg1 if 2 cannot be found.  For Oracle, its cx_Oracle.</li>
        <li>use_ansi=True : used only by Oracle;  when False, the Oracle driver attempts to support a particular "quirk" of some Oracle databases, that the LEFT OUTER JOIN SQL syntax is not supported, and the "Oracle join" syntax of using <% "<column1>(+)=<column2>" |h%> must be used in order to achieve a LEFT OUTER JOIN.  Its advised that the Oracle database be configured to have full ANSI support instead of using this feature.</li>
    </ul>
    </&>
</&>
