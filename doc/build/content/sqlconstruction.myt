<%flags>inherit='document_base.myt'</%flags>
<&|doclib.myt:item, name="sql", description="Constructing SQL Queries via Python Expressions" &>
    <p><b>Note:</b> This section describes how to use SQLAlchemy to construct SQL queries and receive result sets.  It does <b>not</b> cover the object relational mapping capabilities of SQLAlchemy; that is covered later on in <&formatting.myt:link, path="datamapping"&>.  However, both areas of functionality work very similarly in how selection criterion is constructed, so if you are interested just in ORM, you should probably skim through basic <&formatting.myt:link, path="sql_select_whereclause"&> construction before moving on.</p>
    <p>Once you have used the <span class="codeline">sqlalchemy.schema</span> module to construct your tables and/or reflect them from the database, performing SQL queries using those table meta data objects is done via the <span class="codeline">sqlalchemy.sql</span> package.  This package defines a large set of classes, each of which represents a particular kind of lexical construct within a SQL query; all are descendants of the common base class <span class="codeline">sqlalchemy.sql.ClauseElement</span>.  A full query is represented via a structure of ClauseElements.  A set of reasonably intuitive creation functions is provided by the <span class="codeline">sqlalchemy.sql</span> package to create these structures; these functions are described in the rest of this section. </p>
    
    <p>To execute a query, you create its structure, then call the resulting structure's <span class="codeline">execute()</span> method, which returns a cursor-like object (more on that later).  This method can be repeated as necessary.  A ClauseElement is actually compiled into a string representation by an underlying SQLEngine object; this object is located by searching through the ClauseElement structure for a Table object, which provides a reference to its SQLEngine.  
    </p>    
    
    <p>For this section, we will assume the following tables:
       <&|formatting.myt:code&>
        from sqlalchemy.schema import *
        
        # a table to store users
        users = Table('users', db,
            Column('user_id', Integer, primary_key = True),
            Column('user_name', String(40)),
            Column('fullname', String(100)),
            Column('email_address', String(80))
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
    </p>
    
    <&|doclib.myt:item, name="select", description="Simple Select" &>
        <p>A select is done by constructing a <span class="codeline">Select</span> object with the proper arguments, adding any extra arguments if desired, then calling its <span class="codeline">execute()</span> method.
        <&|formatting.myt:code&>
            from sqlalchemy.sql import *
            
            # use the select() function defined in the sql package
            s = select([users])

            # or, call the select() method off of a Table object
            s = users.select()
            
            # then, call execute on the Select object:
            c = s.execute() <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address FROM users
{}
</&>
            # the SQL text of any clause object can also be viewed via the str() call:
            >>> str(s)
            SELECT users.user_id, users.user_name, users.fullname, users.email_address FROM users

        </&>
        <p>The object returned by the execute call is a <span class="codeline">sqlalchemy.engine.ResultProxy</span> object, which acts very much like a DBAPI <span class="codeline">cursor</span> object in the context of a result set, except that the rows returned can address their columns by ordinal position, column name, or even column object:</p>
        
        <&|formatting.myt:code&>
            # select rows, get resulting ResultProxy object
            c = users.select().execute()  <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address FROM users
{}
</&>
            # get one row
            row = c.fetchone()
            
            # get the 'user_id' column via integer index:
            user_id = row[0]
            
            # or column name
            user_name = row['user_name']
            
            # or column object
            fullname = row[users.c.fullname]
            
            # rowproxy object also supports fetchall()
            rows = c.fetchall()
            
            # or get the underlying DBAPI cursor object
            cursor = c.cursor
            
        </&>
        
        <&|doclib.myt:item, name="columns", description="Table/Column Specification" &>
            <P>Calling <span class="codeline">select</span> off a table automatically generates a column clause which includes all the table's columns, in the order they are specified in the source Table object.</p>
            <p>But in addition to selecting all the columns off a single table, any set of columns can be specified, as well as full tables, and any combination of the two:</p>
            <&|formatting.myt:code&>
                # individual columns
                c = select([users.c.user_id, users.c.user_name]).execute()  <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name FROM users
{}
</&>
                # full tables
                c = select([users, addresses]).execute()  <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, 
users.email_address, addresses.address_id, addresses.user_id, 
addresses.street, addresses.city, addresses.state, addresses.zip
FROM users, addresses
{}
</&>
                # combinations
                c = select([users, addresses.c.zip]).execute()  <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, 
users.email_address, addresses.zip FROM users, addresses
{}
</&>                
            </&>            
        </&>

        <&|doclib.myt:item, name="whereclause", description="WHERE Clause" &>
            <P>The WHERE condition is the named keyword argument <span class="codeline">whereclause</span>, or the second positional argument to the <span class="codeline">select()</span> constructor and the first positional argument to the <span class="codeline">select()</span> method of <span class="codeline">Table</span>.</p>
            
            <p>WHERE conditions are constructed using column objects, literal values, and functions defined in the <span class="codeline">sqlalchemy.sql</span> module.  Column objects override the standard Python operators to provide clause compositional objects, which compile down to SQL operations:
            
            <&|formatting.myt:code&>
                c = users.select(users.c.user_id == 7).execute()  <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, 
users.email_address FROM users
WHERE users.user_id = :users_user_id
{'users_user_id': 7}                
</&>                
            </&>
            <p>Notice that the literal value "7" was broken out of the query and placed into a bind parameter.  Databases such as Oracle must parse incoming SQL and create a "plan" when new queries are received, which is an expensive process.  By using bind parameters, the same query with various literal values can have its plan compiled only once, and used repeatedly with less overhead.
            </p>
            <P>More where clauses:</p>
            <&|formatting.myt:code&>
                # another comparison operator
                c = select([users], users.c.user_id>7).execute() <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, 
users.email_address FROM users WHERE users.user_id > :users_user_id
{'users_user_id': 7}
</&>

                # OR keyword
                c = users.select(or_(users.c.user_name=='jack', users.c.user_name=='ed')).execute()  <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address
FROM users WHERE users.user_name = :users_user_name 
OR users.user_name = :users_user_name_1
{'users_user_name_1': 'ed', 'users_user_name': 'jack'}

</&>

                # AND keyword
                c = users.select(and_(users.c.user_name=='jack', users.c.fullname=='ed')).execute()  <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address
FROM users WHERE users.user_name = :users_user_name 
AND users.fullname = :users_fullname
{'users_user_name': 'jack', 'users_fullname': 'ed'}
</&>

                # IN clause
                c = users.select(users.c.user_name.in_('jack', 'ed', 'fred')).execute()  <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address
FROM users WHERE users.user_name IN ('jack', 'ed', 'fred')
</&>
                
                # join users and addresses together
                c = select([users, addresses], users.c.user_id==addresses.c.address_id).execute()  <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address, 
addresses.address_id, addresses.user_id, addresses.street, addresses.city, 
addresses.state, addresses.zip
FROM users, addresses
WHERE users.user_id = addresses.address_id
{}
</&>
                
                # join users and addresses together, but dont specify "addresses" in the 
                # selection criterion.  The WHERE criterion adds it to the FROM list 
                # automatically.
                c = select([users], and_(
                    users.c.user_id==addresses.c.user_id,
                    users.c.user_name=='fred'
                )).execute()                      <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address
FROM users, addresses WHERE users.user_id = addresses.user_id 
AND users.user_name = :users_user_name
{'users_user_name': 'fred'}                
</&>
</&>            
            <&|doclib.myt:item, name="operators", description="Operators" &>
            <p>Supported column operators so far are all the numerical comparison operators, i.e. '==', '>', '>=', etc., as well as like(), startswith(), endswith(), and in().  Boolean operators include and_() and or_().</p>
            <&|formatting.myt:code &>
                users.select(users.c.user_name.like('%ter'))
                users.select(users.c.user_name == 'jane')
                users.select(users.c.user_id.in_(1,2,3))
                users.select(and_(addresses.c.street.endswith('green street'), addresses.c.zip=='11234'))
            </&>
            </&>

        </&>

        <&|doclib.myt:item, name="orderby", description="Order By" &>
        <P>The ORDER BY clause of a select statement can be specified as individual columns to order by within an array     specified via the <span class="codeline">order_by</span> parameter, and optional usage of the asc() and desc() functions:
            <&|formatting.myt:code &>
                # straight order by
                c = users.select(order_by=[users.c.fullname]).execute() <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address
FROM users ORDER BY users.fullname                
</&>        
                # descending/ascending order by on multiple columns
                c = users.select(
                        users.c.user_name>'J', 
                        order_by=[desc(users.c.fullname), asc(users.c.user_name)]).execute() <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address
FROM users WHERE users.user_name > :users_user_name 
ORDER BY users.fullname DESC, users.user_name ASC
{'users_user_name':'J'}
</&>        
            </&>        
        </&>

    </&>

    <&|doclib.myt:item, name="join", description="Inner and Outer Joins" &>
    </&>
    <&|doclib.myt:item, name="alias", description="Table Aliases" &>
    </&>
    <&|doclib.myt:item, name="subqueries", description="Subqueries" &>
        <&|doclib.myt:item, name="fromclause", description="Subqueries as FROM Clauses" &>
        </&>
        <&|doclib.myt:item, name="correlated", description="Correlated Subqueries" &>
        </&>
        <&|doclib.myt:item, name="exists", description="EXISTS Clauses" &>
        </&>
    </&>
    <&|doclib.myt:item, name="unions", description="Unions" &>
    </&>
    <&|doclib.myt:item, name="bindparams", description="Custom Bind Parameters" &>
    </&>
    <&|doclib.myt:item, name="textual", description="Literal Text Blocks" &>
    </&>
    <&|doclib.myt:item, name="insert", description="Inserts" &>
    </&>
    <&|doclib.myt:item, name="update", description="Updates" &>
        <&|doclib.myt:item, name="correlated", description="Correlated Updates" &>
        </&>
    </&>
    <&|doclib.myt:item, name="delete", description="Deletes" &>
    </&>
    <&|doclib.myt:item, name="precompile", description="Compiled Query Objects" &>
    </&>
</&>