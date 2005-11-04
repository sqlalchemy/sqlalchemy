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
<&formatting.myt:poplink&>c = s.execute() 
<&|formatting.myt:codepopper, link="sql" &>
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
<&formatting.myt:poplink&>c = users.select().execute()  
<&|formatting.myt:codepopper, link="sql" &>
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
        <&|doclib.myt:item, name="labels", description="Using Column Labels" &>
        <p>A common need when writing statements that reference multiple tables is to create labels for columns, thereby separating columns from different tables with the same name.  The Select construct supports automatic generation of column labels via the <span class="codeline">use_labels=True</span> parameter:</p>
        <&|formatting.myt:code&>
            
<&formatting.myt:poplink&>c = select([users, addresses], 
    users.c.user_id==addresses.c.address_id, 
    use_labels=True).execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id AS users_user_id, users.user_name AS users_user_name, 
users.password AS users_password, addresses.address_id AS addresses_address_id, 
addresses.user_id AS addresses_user_id, addresses.street AS addresses_street, 
addresses.city AS addresses_city, addresses.state AS addresses_state, 
addresses.zip AS addresses_zip
FROM users, addresses
WHERE users.user_id = addresses.address_id
{}
</&>
</&>
        <p>If you want to use a different label, you can also try using an alias:</p>
        <&|formatting.myt:code&>

        person = users.alias('person')
        <&formatting.myt:poplink&>c = select([person, addresses], 
            person.c.user_id==addresses.c.address_id, 
            use_labels=True).execute()  
        
<&|formatting.myt:codepopper, link="sql" &>
SELECT person.user_id AS person_user_id, person.user_name AS person_user_name, 
person.password AS person_password, addresses.address_id AS addresses_address_id,
 addresses.user_id AS addresses_user_id, addresses.street AS addresses_street, 
 addresses.city AS addresses_city, addresses.state AS addresses_state, 
 addresses.zip AS addresses_zip FROM users person, addresses
WHERE person.user_id = addresses.address_id
</&>
        </&>    
        </&>
        
        <&|doclib.myt:item, name="columns", description="Table/Column Specification" &>
            <P>Calling <span class="codeline">select</span> off a table automatically generates a column clause which includes all the table's columns, in the order they are specified in the source Table object.</p>
            <p>But in addition to selecting all the columns off a single table, any set of columns can be specified, as well as full tables, and any combination of the two:</p>
            <&|formatting.myt:code&>
                # individual columns
<&formatting.myt:poplink&>c = select([users.c.user_id, users.c.user_name]).execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name FROM users
{}
</&>
                # full tables
<&formatting.myt:poplink&>c = select([users, addresses]).execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, 
users.email_address, addresses.address_id, addresses.user_id, 
addresses.street, addresses.city, addresses.state, addresses.zip
FROM users, addresses
{}
</&>
                # combinations
<&formatting.myt:poplink&>c = select([users, addresses.c.zip]).execute()  
<&|formatting.myt:codepopper, link="sql" &>
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
<&formatting.myt:poplink&>c = users.select(users.c.user_id == 7).execute()  
<&|formatting.myt:codepopper, link="sql" &>
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
<&formatting.myt:poplink&>c = select([users], users.c.user_id>7).execute() 
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, 
users.email_address FROM users WHERE users.user_id > :users_user_id
{'users_user_id': 7}
</&>

                # OR keyword
<&formatting.myt:poplink&>c = users.select(or_(users.c.user_name=='jack', users.c.user_name=='ed')).execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address
FROM users WHERE users.user_name = :users_user_name 
OR users.user_name = :users_user_name_1
{'users_user_name_1': 'ed', 'users_user_name': 'jack'}

</&>

                # AND keyword
<&formatting.myt:poplink&>c = users.select(and_(users.c.user_name=='jack', users.c.fullname=='ed')).execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address
FROM users WHERE users.user_name = :users_user_name 
AND users.fullname = :users_fullname
{'users_user_name': 'jack', 'users_fullname': 'ed'}
</&>

                # IN clause
<&formatting.myt:poplink&>c = users.select(users.c.user_name.in_('jack', 'ed', 'fred')).execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address
FROM users WHERE users.user_name IN ('jack', 'ed', 'fred')
</&>
                
                # join users and addresses together
<&formatting.myt:poplink&>c = select([users, addresses], users.c.user_id==addresses.c.address_id).execute()  
<&|formatting.myt:codepopper, link="sql" &>
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
<&formatting.myt:poplink&>c = select([users], and_(
                    users.c.user_id==addresses.c.user_id,
                    users.c.user_name=='fred'
                )).execute()
<&|formatting.myt:codepopper, link="sql" &>
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
<&formatting.myt:poplink&>c = users.select(order_by=[users.c.fullname]).execute() 
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address
FROM users ORDER BY users.fullname                
</&>        
                # descending/ascending order by on multiple columns
<&formatting.myt:poplink&>c = users.select(
                        users.c.user_name>'J', 
                        order_by=[desc(users.c.fullname), asc(users.c.user_name)]).execute() 
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.fullname, users.email_address
FROM users WHERE users.user_name > :users_user_name 
ORDER BY users.fullname DESC, users.user_name ASC
{'users_user_name':'J'}
</&>        
            </&>        
        </&>
    </&>

    <&|doclib.myt:item, name="join", description="Inner and Outer Joins" &>
        <p>As some of the examples indicated above, a regular inner join can be implicitly stated, just like in a SQL expression, by just specifying the tables to be joined as well as their join conditions:</p>
            <&|formatting.myt:code &>
<&formatting.myt:poplink&>addresses.select(addresses.c.user_id==users.c.user_id).execute() 
<&|formatting.myt:codepopper, link="sql" &>
SELECT addresses.address_id, addresses.user_id, addresses.street, 
addresses.city, addresses.state, addresses.zip FROM addresses, users
WHERE addresses.user_id = users.user_id
{}                   
</&>                   
            </&>
        <p>There is also an explicit join constructor, which is used like this:</p>
            <&|formatting.myt:code &>
<&formatting.myt:poplink&>\
addresses.select(from_obj=[
            addresses.join(users, addresses.c.user_id==users.c.user_id)
        ]).execute() 
<&|formatting.myt:codepopper, link="sql" &>
SELECT addresses.address_id, addresses.user_id, addresses.street, addresses.city, 
addresses.state, addresses.zip 
FROM addresses JOIN users ON addresses.user_id = users.user_id
{}                
</&>                
            </&>
        <p>Notice that this is the first example where the FROM criterion of the select statement is explicitly specified.  In most cases, the FROM criterion is automatically determined from the columns requested as well as the WHERE clause.  The <span class="codeline">from_obj</span> keyword argument indicates a list of explicit FROM clauses to be used in the statement.</p>
        
        <p>A join can be created on its own using the <span class="codeline">join</span> or <span class="codeline">outerjoin</span> functions, or can be created off of an existing Table or other selectable unit via the <span class="codeline">join</span> or <span class="codeline">outerjoin</span> methods:</p>
        
            <&|formatting.myt:code &>
            <&formatting.myt:poplink&>select([users, addresses], from_obj=[
                outerjoin(users, addresses, users.c.user_id==addresses.c.address_id)
            ]).execute() 
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password, addresses.address_id, 
addresses.user_id, addresses.street, addresses.city, addresses.state, addresses.zip
FROM users LEFT OUTER JOIN addresses ON users.user_id = addresses.address_id
{}                
</&>

            <&formatting.myt:poplink&>users.select(keywords.c.name=='running', from_obj=[
            users.join(
                userkeywords, userkeywords.c.user_id==users.c.user_id).join(
                    keywords, keywords.c.keyword_id==userkeywords.c.keyword_id)
            ]).execute()   
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password FROM users 
JOIN userkeywords ON userkeywords.user_id = users.user_id 
JOIN keywords ON keywords.keyword_id = userkeywords.keyword_id
WHERE keywords.name = :keywords_name
{'keywords_name': 'running'}                
</&>
            </&>
            
    </&>
    <&|doclib.myt:item, name="alias", description="Table Aliases" &>
    <p>Aliases are used primarily when you want to use the same table more than once as a FROM expression in a statement:</p>
    
            <&|formatting.myt:code &>
                address_b = addresses.alias('addressb')

                <&formatting.myt:poplink&># select users who have an address on Green street as well as Orange street
                users.select(and_(
                    users.c.user_id==addresses.c.user_id,
                    addresses.c.street.like('%Green%'),
                    users.c.user_id==address_b.c.user_id,
                    address_b.c.street.like('%Orange%')
                    ))
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password
FROM users, addresses, addresses addressb
WHERE users.user_id = addresses.user_id 
AND addresses.street LIKE :addresses_street 
AND users.user_id = addressb.user_id 
AND addressb.street LIKE :addressb_street
{'addressb_street': '%Orange%', 'addresses_street': '%Green%'}
</&>
            </&>    
    </&>
    <&|doclib.myt:item, name="subqueries", description="Subqueries" &>
    <p>SQLAlchemy allows the creation of select statements from not just Table objects, but from a whole class of objects that implement the <span class="codeline">Selectable</span> interface.  This includes Tables, Aliases, Joins and Selects.  Therefore, if you have a Select, you can select from the Select:</p>
    
            <&|formatting.myt:code &>
                >>> s = users.select()
                >>> str(s)
                SELECT users.user_id, users.user_name, users.password FROM users

                >>> s = s.select()
                >>> str(s)
                SELECT user_id, user_name, password
                FROM (SELECT users.user_id, users.user_name, users.password FROM users)                
                
            </&>
        <p>From there, one can see that a Select object can be used within other Selects just like a Table:
        </p>
            <&|formatting.myt:code &>
                # select user ids for all users whos name starts with a "p"
                s = select([users.c.user_id], users.c.user_name.like('p%'))
            
                # now select all addresses for those users
                <&formatting.myt:poplink&>addresses.select(addresses.c.address_id.in_(s)).execute()
<&|formatting.myt:codepopper, link="sql" &>
SELECT addresses.address_id, addresses.user_id, addresses.street, 
addresses.city, addresses.state, addresses.zip
FROM addresses WHERE addresses.address_id IN 
(SELECT users.user_id FROM users WHERE users.user_name LIKE :users_user_name)
{'users_user_name': 'p%'}</&>
            </&>
        <p>Any Select, Join, or Alias object supports the same column accessors as a Table:
        </p>
        <&|formatting.myt:code &>
            >>> s = users.select()
            >>> [c.key for c in s.columns]
            ['user_id', 'user_name', 'password']            
        </&> 
        
        <p>
        When you use <span class="codeline">use_labels=True</span> in a Select object, the label version of the column names become the keys of the accessible columns.  In effect you can create your own "view objects":
        </p>
        <&|formatting.myt:code &>
                s = select([users, addresses], users.c.user_id==addresses.c.user_id, use_labels=True)

                <&formatting.myt:poplink&>select([
                        s.c.users_user_name, s.c.addresses_street, s.c.addresses_zip
                        ], s.c.addresses_city=='San Francisco').execute()
<&|formatting.myt:codepopper, link="sql" &>
SELECT users_user_name, addresses_street, addresses_zip
FROM (SELECT users.user_id AS users_user_id, users.user_name AS users_user_name,
 users.password AS users_password, addresses.address_id AS addresses_address_id,
 addresses.user_id AS addresses_user_id, addresses.street AS addresses_street, 
 addresses.city AS addresses_city, addresses.state AS addresses_state, 
 addresses.zip AS addresses_zip
FROM users, addresses
WHERE users.user_id = addresses.user_id)
WHERE addresses_city = :addresses_city
{'addresses_city': 'San Francisco'}
</&>
        </&>
        
        <&|doclib.myt:item, name="correlated", description="Correlated Subqueries" &>
        <&|formatting.myt:code &>
        <&formatting.myt:poplink&>s = select([addresses.c.street], addresses.c.user_id==users.c.user_id).alias('s')
        select([users, s.c.street], from_obj=[s]).execute()
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password, s.street
FROM users, (SELECT addresses.street FROM addresses
WHERE addresses.user_id = users.user_id) s
{}        
</&>
</&>
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