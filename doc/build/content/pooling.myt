<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Connection Pooling'</%attr>
<&|doclib.myt:item, name="pooling", description="Connection Pooling" &>
    <P><b>Note:</b>This section describes the connection pool module of SQLAlchemy, which is the smallest component of the library that can be used on its own.  If you are interested in using SQLAlchemy for query construction or Object Relational Mapping, this module is automatically managed behind the scenes; you can skip ahead to <&formatting.myt:link,path="dbengine"&> in that case.</p>
    <p>At the base of any database helper library is a system of efficiently acquiring connections to the database.  Since the establishment of a database connection is typically a somewhat expensive operation, an application needs a way to get at database connections repeatedly without incurring the full overhead each time.  Particularly for server-side web applications, a connection pool is the standard way to maintain a "pool" of database connections which are used over and over again among many requests.  Connection pools typically are configured to maintain a certain "size", which represents how many connections can be used simultaneously without resorting to creating more newly-established connections.
    </p>
    <p>SQLAlchemy includes a pooling module that can be used completely independently of the rest of the toolset.  This section describes how it can be used on its own, as well as the available options.  If SQLAlchemy is being used more fully, the connection pooling described below occurs automatically.  The options are still available, though, so this core feature is a good place to start.
    </p>
    <&|doclib.myt:item, name="establishing", description="Establishing a Transparent Connection Pool" &>
    Any DBAPI module can be "proxied" through the connection pool using the following technique (note that the usage of 'psycopg2' is <b>just an example</b>; substitute whatever DBAPI module you'd like):
    
    <&|formatting.myt:code&>
    import sqlalchemy.pool as pool
    import psycopg2 as psycopg
    psycopg = pool.manage(psycopg)
    
    # then connect normally
    connection = psycopg.connect(database='test', username='scott', password='tiger')
    </&>
    <p>This produces a <span class="codeline">sqlalchemy.pool.DBProxy</span> object which supports the same <span class="codeline">connect()</span> function as the original DBAPI module.  Upon connection, a thread-local connection proxy object is returned, which delegates its calls to a real DBAPI connection object.  This connection object is stored persistently within a connection pool (an instance of <span class="codeline">sqlalchemy.pool.Pool</span>) that corresponds to the exact connection arguments sent to the <span class="codeline">connect()</span> function.  The connection proxy also returns a proxied cursor object upon calling <span class="codeline">connection.cursor()</span>.  When all cursors as well as the connection proxy are de-referenced, the connection is automatically made available again by the owning pool object.</p>
    
    <p>Basically, the <span class="codeline">connect()</span> function is used in its usual way, and the pool module transparently returns thread-local pooled connections.  Each distinct set of connect arguments corresponds to a brand new connection pool created; in this way, an application can maintain connections to multiple schemas and/or databases, and each unique connect argument set will be managed by a different pool object.</p>
    </&>

    <&|doclib.myt:item, name="configuration", description="Connection Pool Configuration" &>
    <p>When proxying a DBAPI module through the <span class="codeline">pool</span> module, options exist for how the connections should be pooled:
    </p>
    <ul>
        <li>echo=False : if set to True, connections being pulled and retrieved from/to the pool will be logged to the standard output, as well as pool sizing information.</li>
        <li>use_threadlocal=True : if set to True, repeated calls to connect() within the same application thread will be guaranteed to return the <b>same</b> connection object, if one has already been retrieved from the pool and has not been returned yet.  This allows code to retrieve a connection from the pool, and then while still holding on to that connection, to call other functions which also ask the pool for a connection of the same arguments;  those functions will act upon the same connection that the calling method is using.  Note that once the connection is returned to the pool, it then may be used by another thread.  To guarantee a single unique connection per thread that <b>never</b> changes, use the option <span class="codeline">poolclass=SingletonThreadPool</span>, in which case the use_threadlocal parameter is automatically set to False.</li>
        <li>poolclass=QueuePool :  the Pool class used by the pool module to provide pooling.  QueuePool uses the Python <span class="codeline">Queue.Queue</span> class to maintain a list of available connections.  A developer can supply his or her own Pool class to supply a different pooling algorithm.  Also included is the ThreadSingletonPool, which provides a single distinct connection per thread and is required with SQLite.</li>
        <li>pool_size=5 : used by QueuePool - the size of the pool to be maintained.  This is the largest number of connections that will be kept persistently in the pool.  Note that the pool begins with no connections; once this number of connections is requested, that number of connections will remain.</li>
        <li>max_overflow=10 : used by QueuePool - the maximum overflow size of the pool.  When the number of checked-out connections reaches the size set in pool_size, additional connections will be returned up to this limit.  When those additional connections are returned to the pool, they are disconnected and discarded.  It follows then that the total number of simultaneous connections the pool will allow is pool_size + max_overflow, and the total number of "sleeping" connections the pool will allow is pool_size.  max_overflow can be set to -1 to indicate no overflow limit; no limit will be placed on the total number of concurrent connections.</li>
    </ul>
    </&>
    
    <&|doclib.myt:item, name="custom", description="Custom Pool Construction" &>
    <p>One level below using a DBProxy to make transparent pools is creating the pool yourself.  The pool module comes with two implementations of connection pools: <span class="codeline">QueuePool</span> and <span class="codeline">SingletonThreadPool</span>.  While QueuePool uses Queue.Queue to provide connections, SingletonThreadPool provides a single per-thread connection which SQLite requires.</p>
    
    <p>Constructing your own pool involves passing a callable used to create a connection.  Through this method, custom connection schemes can be made, such as a connection that automatically executes some initialization commands to start.  The options from the previous section can be used as they apply to QueuePool or SingletonThreadPool.</p>
    <&|formatting.myt:code, title="Plain QueuePool"&>
        import sqlalchemy.pool as pool
        import psycopg2
        
        def getconn():
            c = psycopg2.connect(username='ed', host='127.0.0.1', dbname='test')
            # execute an initialization function on the connection before returning
            c.cursor.execute("setup_encodings()")
            return c
            
        p = pool.QueuePool(getconn, max_overflow=10, pool_size=5, use_threadlocal=True)
    </&>

    <&|formatting.myt:code, title="SingletonThreadPool"&>
        import sqlalchemy.pool as pool
        import sqlite
        
        def getconn():
            return sqlite.connect(filename='myfile.db')
        
        # SQLite connections require the SingletonThreadPool    
        p = pool.SingletonThreadPool(getconn)
    </&>

    </&>
</&>