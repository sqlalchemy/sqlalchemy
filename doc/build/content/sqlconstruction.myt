<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Constructing SQL Queries via Python Expressions'</%attr>

<&|doclib.myt:item, name="sql", description="Constructing SQL Queries via Python Expressions" &>
    <p><b>Note:</b> This section describes how to use SQLAlchemy to construct SQL queries and receive result sets.  It does <b>not</b> cover the object relational mapping capabilities of SQLAlchemy; that is covered later on in <&formatting.myt:link, path="datamapping"&>.  However, both areas of functionality work similarly in how selection criterion is constructed, so if you are interested just in ORM, you should probably skim through basic <&formatting.myt:link, path="sql_select_whereclause"&> construction before moving on.</p>
    <p>Once you have used the <span class="codeline">sqlalchemy.schema</span> module to construct your tables and/or reflect them from the database, performing SQL queries using those table meta data objects is done via the <span class="codeline">sqlalchemy.sql</span> package.  This package defines a large set of classes, each of which represents a particular kind of lexical construct within a SQL query; all are descendants of the common base class <span class="codeline">sqlalchemy.sql.ClauseElement</span>.  A full query is represented via a structure of ClauseElements.  A set of reasonably intuitive creation functions is provided by the <span class="codeline">sqlalchemy.sql</span> package to create these structures; these functions are described in the rest of this section. </p>
    
    <p>To execute a query, you create its structure, then call the resulting structure's <span class="codeline">execute()</span> method, which returns a cursor-like object (more on that later).  The same clause structure can be used repeatedly.  A ClauseElement is compiled into a string representation by an underlying SQLEngine object, which is located by searching through the clause's child items for a Table object, which provides a reference to its SQLEngine. 
    </p>    
    <p>The examples below all include a dump of the generated SQL corresponding to the query object, as well as a dump of the statement's bind parameters.  In all cases, bind parameters are named parameters using the colon format (i.e. ':name').  A named parameter scheme, either ':name' or '%(name)s', is used with all databases, including those that use positional schemes.  For those, the named-parameter statement and its bind values are converted to the proper list-based format right before execution.  Therefore a SQLAlchemy application that uses ClauseElements can standardize on named parameters for all databases.</p>
    
    <p>For this section, we will assume the following tables:
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
    </p>
    
    <&|doclib.myt:item, name="select", description="Simple Select" &>
        <p>A select is done by constructing a <span class="codeline">Select</span> object with the proper arguments, adding any extra arguments if desired, then calling its <span class="codeline">execute()</span> method.
        <&|formatting.myt:code&>
            from sqlalchemy import *
            
            # use the select() function defined in the sql package
            s = select([users])

            # or, call the select() method off of a Table object
            s = users.select()
            
            # then, call execute on the Select object:
<&formatting.myt:poplink&>c = s.execute() 
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password FROM users
{}
</&>
            # the SQL text of any clause object can also be viewed via the str() call:
            >>> str(s)
            SELECT users.user_id, users.user_name, users.password FROM users

        </&>
        <p>The object returned by the execute call is a <span class="codeline">sqlalchemy.engine.ResultProxy</span> object, which acts much like a DBAPI <span class="codeline">cursor</span> object in the context of a result set, except that the rows returned can address their columns by ordinal position, column name, or even column object:</p>
        
        <&|formatting.myt:code&>
            # select rows, get resulting ResultProxy object
<&formatting.myt:poplink&>c = users.select().execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password FROM users
{}
</&>
            # get one row
            row = c.fetchone()
            
            # get the 'user_id' column via integer index:
            user_id = row[0]
            
            # or column name
            user_name = row['user_name']
            
            # or column object
            password = row[users.c.password]
            
            # or column accessor
            password = row.password
            
            # ResultProxy object also supports fetchall()
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
        <p>The table name part of the label is affected if you use a construct such as a table alias:</p>
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
 addresses.zip AS addresses_zip FROM users AS person, addresses
WHERE person.user_id = addresses.address_id
</&>
        </&>    
        <p>You can also specify custom labels on a per-column basis using the <span class="codeline">label()</span> function:
        <&|formatting.myt:code&>
        <&formatting.myt:poplink&>c = select([users.c.user_id.label('id'), users.c.user_name.label('name')]).execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id AS id, users.user_name AS name
FROM users
{}
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
SELECT users.user_id, users.user_name, users.password, 
addresses.address_id, addresses.user_id, 
addresses.street, addresses.city, addresses.state, addresses.zip
FROM users, addresses
{}
</&>
                # combinations
<&formatting.myt:poplink&>c = select([users, addresses.c.zip]).execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password, 
addresses.zip FROM users, addresses
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
SELECT users.user_id, users.user_name, users.password, 
FROM users WHERE users.user_id = :users_user_id
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
SELECT users.user_id, users.user_name, users.password, 
FROM users WHERE users.user_id > :users_user_id
{'users_user_id': 7}
</&>

                # OR keyword
<&formatting.myt:poplink&>c = users.select(or_(users.c.user_name=='jack', users.c.user_name=='ed')).execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password 
FROM users WHERE users.user_name = :users_user_name 
OR users.user_name = :users_user_name_1
{'users_user_name_1': 'ed', 'users_user_name': 'jack'}

</&>

                # AND keyword
<&formatting.myt:poplink&>c = users.select(and_(users.c.user_name=='jack', users.c.password=='dog')).execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password
FROM users WHERE users.user_name = :users_user_name 
AND users.password = :users_password
{'users_user_name': 'jack', 'users_password': 'dog'}
</&>

                # NOT keyword
                <&formatting.myt:poplink&>c = users.select(not_(
                        or_(users.c.user_name=='jack', users.c.password=='dog')
                    )).execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password 
FROM users 
WHERE NOT (users.user_name = :users_user_name 
    OR users.password = :users_password)
{'users_user_name': 'jack', 'users_password': 'dog'}
</&>
                
                # IN clause
<&formatting.myt:poplink&>c = users.select(users.c.user_name.in_('jack', 'ed', 'fred')).execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password
FROM users WHERE users.user_name 
IN (:users_user_name, :users_user_name_1, :users_user_name_2)
{'users_user_name': 'jack', 'users_user_name_1': 'ed', 
    'users_user_name_2': 'fred'}
</&>
                
                # join users and addresses together
<&formatting.myt:poplink&>c = select([users, addresses], users.c.user_id==addresses.c.address_id).execute()  
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password,  
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
SELECT users.user_id, users.user_name, users.password
FROM users, addresses WHERE users.user_id = addresses.user_id 
AND users.user_name = :users_user_name
{'users_user_name': 'fred'}                
</&>
</&>            

        <P>Select statements can also generate a WHERE clause based on the parameters you give it.  If a given parameter, which matches the name of a column or its "label" (the combined tablename + "_" + column name), and does not already correspond to a bind parameter in the select object, it will be added as a comparison against that column.  This is a shortcut to creating a full WHERE clause:</p>
        <&|formatting.myt:code&>
            # specify a match for the "user_name" column
            <&formatting.myt:poplink&>c = users.select().execute(user_name='ed')
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password
FROM users WHERE users.user_name = :users_user_name
{'users_user_name': 'ed'}
</&>
            # specify a full where clause for the "user_name" column, as well as a
            # comparison for the "user_id" column
            <&formatting.myt:poplink&>c = users.select(users.c.user_name=='ed').execute(user_id=10)
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password
FROM users WHERE users.user_name = :users_user_name AND users.user_id = :users_user_id
{'users_user_name': 'ed', 'users_user_id': 10}
</&>
        </&>
            <&|doclib.myt:item, name="operators", description="Operators" &>
            <p>Supported column operators so far are all the numerical comparison operators, i.e. '==', '>', '>=', etc., as well as like(), startswith(), endswith(), between(), and in().  Boolean operators include not_(), and_() and or_(), which also can be used inline via '~', '&', and '|'.  Math operators are '+', '-', '*', '/'.  Any custom operator can be specified via the op() function shown below.</p>
            <&|formatting.myt:code &>
                # "like" operator
                users.select(users.c.user_name.like('%ter'))
                
                # equality operator
                users.select(users.c.user_name == 'jane')
                
                # in opertator
                users.select(users.c.user_id.in_(1,2,3))
                
                # and_, endswith, equality operators
                users.select(and_(addresses.c.street.endswith('green street'), addresses.c.zip=='11234'))
                
                # & operator subsituting for 'and_'
                users.select(addresses.c.street.endswith('green street') & (addresses.c.zip=='11234'))
                
                # + concatenation operator
                select([users.c.user_name + '_name'])
                
                # NOT operator
                users.select(~(addresses.c.street == 'Green Street'))
                
                # any custom operator
                select([users.c.user_name.op('||')('_category')])
            </&>
            </&>

        </&>
        <&|doclib.myt:item, name="engine", description="Specifying the Engine" &>
        <p>For queries that don't contain any tables, the SQLEngine can be specified to any constructed statement via the <span class="codeline">engine</span> keyword parameter:</p>
        <&|formatting.myt:code &>
            # select a literal
            select(["hi"], engine=myengine)
            
            # select a function
            select([func.now()], engine=db)
        </&>
        </&>
        <&|doclib.myt:item, name="functions", description="Functions" &>
        <p>Functions can be specified using the <span class="codeline">func</span> keyword:</p>
        <&|formatting.myt:code &>
            <&formatting.myt:poplink&>select([func.count(users.c.user_id)]).execute()
            <&|formatting.myt:codepopper, link="sql" &>
SELECT count(users.user_id) FROM users
            </&>

            <&formatting.myt:poplink&>users.select(func.substr(users.c.user_name, 1) == 'J').execute()
            <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password FROM users 
WHERE substr(users.user_name, :substr) = :substr_1
{'substr_1': 'J', 'substr': 1}
            </&>

        </&>
        <p>Functions also are callable as standalone values:</p>
        <&|formatting.myt:code &>
            # call the "now()" function
            time = func.now(engine=myengine).scalar()
            
            # call myfunc(1,2,3)
            myvalue = func.myfunc(1, 2, 3, engine=db).execute()
            
            # or call them off the engine
            db.func.now().scalar()
        </&>
        </&>
        <&|doclib.myt:item, name="literals", description="Literals" &>
        <p>You can drop in a literal value anywhere there isnt a column to attach to via the <span class="codeline">literal</span> keyword:</p>
        <&|formatting.myt:code &>
        <&formatting.myt:poplink&>select([literal('foo') + literal('bar'), users.c.user_name]).execute()
        <&|formatting.myt:codepopper, link="sql" &>
        SELECT :literal + :literal_1, users.user_name 
        FROM users
        {'literal_1': 'bar', 'literal': 'foo'}
        </&>
        # literals have all the same comparison functions as columns
        <&formatting.myt:poplink&>select([literal('foo') == literal('bar')], engine=myengine).scalar()
        <&|formatting.myt:codepopper, link="sql" &>
        SELECT :literal = :literal_1
        {'literal_1': 'bar', 'literal': 'foo'}
        </&>
        </&>
        <p>Literals also take an optional <span class="codeline">type</span> parameter to give literals a type.  This can sometimes be significant, for example when using the "+" operator with SQLite, the String type is detected and the operator is converted to "||":</p>
        <&|formatting.myt:code &>
        <&formatting.myt:poplink&>select([literal('foo', type=String) + 'bar'], engine=e).execute()
        <&|formatting.myt:codepopper, link="sql" &>
        SELECT ? || ?
        ['foo', 'bar']
        </&>
        </&>
        
        </&>
        <&|doclib.myt:item, name="orderby", description="Order By" &>
        <P>The ORDER BY clause of a select statement can be specified as individual columns to order by within an array     specified via the <span class="codeline">order_by</span> parameter, and optional usage of the asc() and desc() functions:
            <&|formatting.myt:code &>
                # straight order by
<&formatting.myt:poplink&>c = users.select(order_by=[users.c.user_name]).execute() 
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password
FROM users ORDER BY users.user_name                
</&>        
                # descending/ascending order by on multiple columns
<&formatting.myt:poplink&>c = users.select(
                        users.c.user_name>'J', 
                        order_by=[desc(users.c.user_id), asc(users.c.user_name)]).execute() 
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password
FROM users WHERE users.user_name > :users_user_name 
ORDER BY users.user_id DESC, users.user_name ASC
{'users_user_name':'J'}
</&>        
            </&>        
        </&>
        <&|doclib.myt:item, name="options", description="DISTINCT, LIMIT and OFFSET" &>
        These are specified as keyword arguments:
        <&|formatting.myt:code &>
            <&formatting.myt:poplink&>c = select([users.c.user_name], distinct=True).execute()
<&|formatting.myt:codepopper, link="sql" &>
SELECT DISTINCT users.user_name FROM users
</&>
            <&formatting.myt:poplink&>c = users.select(limit=10, offset=20).execute()
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password FROM users LIMIT 10 OFFSET 20
</&>
        </&>
        The Oracle driver does not support LIMIT and OFFSET directly, but instead wraps the generated query into a subquery and uses the "rownum" variable to control the rows selected (this is somewhat experimental).
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
            <p>There is also an explicit join constructor, which can be embedded into a select query via the <span class="codeline">from_obj</span> parameter of the select statement:</p>

            <&|formatting.myt:code &>
            <&formatting.myt:poplink&>addresses.select(from_obj=[
                        addresses.join(users, addresses.c.user_id==users.c.user_id)
                    ]).execute() 
            <&|formatting.myt:codepopper, link="sql" &>
            SELECT addresses.address_id, addresses.user_id, addresses.street, addresses.city, 
            addresses.state, addresses.zip 
            FROM addresses JOIN users ON addresses.user_id = users.user_id
            {}                
            </&>                

            </&>

        <p>The join constructor can also be used by itself:</p>
                    <&|formatting.myt:code &>
        <&formatting.myt:poplink&>join(users, addresses, users.c.user_id==addresses.c.user_id).select().execute()
        <&|formatting.myt:codepopper, link="sql" &>
        SELECT users.user_id, users.user_name, users.password, 
        addresses.address_id, addresses.user_id, addresses.street, addresses.city, 
        addresses.state, addresses.zip 
        FROM addresses JOIN users ON addresses.user_id = users.user_id
        {}                
        </&>                
            </&>
        <p>The join criterion in a join() call is optional.  If not specified, the condition will be derived from the foreign key relationships of the two tables.  If no criterion can be constructed, an exception will be raised.</p>
        
                    <&|formatting.myt:code &>
        <&formatting.myt:poplink&>join(users, addresses).select().execute()
        <&|formatting.myt:codepopper, link="sql" &>
        SELECT users.user_id, users.user_name, users.password, 
        addresses.address_id, addresses.user_id, addresses.street, addresses.city, 
        addresses.state, addresses.zip 
        FROM addresses JOIN users ON addresses.user_id = users.user_id
        {}                
        </&>                
                    </&>
        
        <p>Notice that this is the first example where the FROM criterion of the select statement is explicitly specified.  In most cases, the FROM criterion is automatically determined from the columns requested as well as the WHERE clause.  The <span class="codeline">from_obj</span> keyword argument indicates a list of explicit FROM clauses to be used in the statement.</p>
        
        <p>A join can be created on its own using the <span class="codeline">join</span> or <span class="codeline">outerjoin</span> functions, or can be created off of an existing Table or other selectable unit via the <span class="codeline">join</span> or <span class="codeline">outerjoin</span> methods:</p>
        
            <&|formatting.myt:code &>
            <&formatting.myt:poplink&>outerjoin(users, addresses, users.c.user_id==addresses.c.address_id).select().execute()
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
                    )).execute()
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password
FROM users, addresses, addresses AS addressb
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
        <p>Any Select, Join, or Alias object supports the same column accessors as a Table:</p>
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
        <p>To specify a SELECT statement as one of the selectable units in a FROM clause, it usually should be given an alias.</p>
        <&|formatting.myt:code &>
            <&formatting.myt:poplink&>s = users.select().alias('u')
            select([addresses, s]).execute()

<&|formatting.myt:codepopper, link="sql" &>
SELECT addresses.address_id, addresses.user_id, addresses.street, addresses.city, 
addresses.state, addresses.zip, u.user_id, u.user_name, u.password 
FROM addresses, 
(SELECT users.user_id, users.user_name, users.password FROM users) AS u
{}
</&>
        </&>
        
        <p>Select objects can be used in a WHERE condition, in operators such as IN:</p>
            <&|formatting.myt:code &>
                # select user ids for all users whos name starts with a "p"
                s = select([users.c.user_id], users.c.user_name.like('p%'))
            
                # now select all addresses for those users
                <&formatting.myt:poplink&>addresses.select(addresses.c.user_id.in_(s)).execute()
<&|formatting.myt:codepopper, link="sql" &>
SELECT addresses.address_id, addresses.user_id, addresses.street, 
addresses.city, addresses.state, addresses.zip
FROM addresses WHERE addresses.address_id IN 
(SELECT users.user_id FROM users WHERE users.user_name LIKE :users_user_name)
{'users_user_name': 'p%'}</&>
            </&>

        <P>The sql package supports embedding select statements into other select statements as the criterion in a WHERE condition, or as one of the "selectable" objects in the FROM list of the query.  It does not at the moment directly support embedding a SELECT statement as one of the column criterion for a statement, although this can be achieved via direct text insertion, described later.</p>
        
        <&|doclib.myt:item, name="scalar", description="Scalar Column Subqueries"&>
        <p>Subqueries can be used in the column clause of a select statement by specifying the <span class="codeline">scalar=True</span> flag:</p>
        <&|formatting.myt:code &>
<&formatting.myt:poplink&>select([table2.c.col1, table2.c.col2, select([table1.c.col1], table1.c.col2==7, scalar=True)])
<&|formatting.myt:codepopper, link="sql" &>
SELECT table2.col1, table2.col2, 
(SELECT table1.col1 AS col1 FROM table1 WHERE col2=:table1_col2) 
FROM table2
{'table1_col2': 7}
</&>
        </&>
        </&>
        
        <&|doclib.myt:item, name="correlated", description="Correlated Subqueries" &>
        <P>When a select object is embedded inside of another select object, and both objects reference the same table, SQLAlchemy makes the assumption that the table should be correlated from the child query to the parent query.  To disable this behavior, specify the flag <span class="codeline">correlate=False</span> to the Select statement.</p>
        <&|formatting.myt:code &>
        # make an alias of a regular select.   
        s = select([addresses.c.street], addresses.c.user_id==users.c.user_id).alias('s')
        >>> str(s)
        SELECT addresses.street FROM addresses, users 
        WHERE addresses.user_id = users.user_id
        
        # now embed that select into another one.  the "users" table is removed from
        # the embedded query's FROM list and is instead correlated to the parent query
        s2 = select([users, s.c.street])
        >>> str(s2)
        SELECT users.user_id, users.user_name, users.password, s.street
        FROM users, (SELECT addresses.street FROM addresses
        WHERE addresses.user_id = users.user_id) s
</&>
        </&>
        <&|doclib.myt:item, name="exists", description="EXISTS Clauses" &>
        <p>An EXISTS clause can function as a higher-scaling version of an IN clause, and is usually used in a correlated fashion:</p>
        <&|formatting.myt:code &>
            # find all users who have an address on Green street:
            <&formatting.myt:poplink&>users.select(
                        exists(
                            [addresses.c.address_id], 
                            and_(
                                addresses.c.user_id==users.c.user_id, 
                                addresses.c.street.like('%Green%')
                            )
                        ))
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password 
FROM users WHERE EXISTS (SELECT addresses.address_id 
FROM addresses WHERE addresses.user_id = users.user_id 
AND addresses.street LIKE :addresses_street)
{'addresses_street': '%Green%'}
</&>
        </&>
        </&>
    </&>
    <&|doclib.myt:item, name="unions", description="Unions" &>
    <p>Unions come in two flavors, UNION and UNION ALL, which are available via module level functions or methods off a Selectable:</p>
        <&|formatting.myt:code &>
            <&formatting.myt:poplink&>union(
                    addresses.select(addresses.c.street=='123 Green Street'),
                    addresses.select(addresses.c.street=='44 Park Ave.'),
                    addresses.select(addresses.c.street=='3 Mill Road'),
                    order_by=[addresses.c.street]
                ).execute()\
<&|formatting.myt:codepopper, link="sql" &>
SELECT addresses.address_id, addresses.user_id, addresses.street, 
addresses.city, addresses.state, addresses.zip 
FROM addresses WHERE addresses.street = :addresses_street 
UNION 
SELECT addresses.address_id, addresses.user_id, addresses.street, 
addresses.city, addresses.state, addresses.zip 
FROM addresses WHERE addresses.street = :addresses_street_1 
UNION 
SELECT addresses.address_id, addresses.user_id, addresses.street, 
addresses.city, addresses.state, addresses.zip 
FROM addresses WHERE addresses.street = :addresses_street_2 
ORDER BY addresses.street
{'addresses_street_1': '44 Park Ave.', 
'addresses_street': '123 Green Street', 
'addresses_street_2': '3 Mill Road'}
</&>
            <&formatting.myt:poplink&>users.select(
                    users.c.user_id==7
                  ).union_all(
                      users.select(
                          users.c.user_id==9
                      ), 
                      order_by=[users.c.user_id]   # order_by is an argument to union_all()
                  ).execute() 
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password 
FROM users WHERE users.user_id = :users_user_id 
UNION ALL 
SELECT users.user_id, users.user_name, users.password 
FROM users WHERE users.user_id = :users_user_id_1 
ORDER BY users.user_id
{'users_user_id_1': 9, 'users_user_id': 7}
</&>
        </&>
    </&>
    <&|doclib.myt:item, name="bindparams", description="Custom Bind Parameters" &>
    <p>Throughout all these examples, SQLAlchemy is busy creating bind parameters wherever literal expressions occur.  You can also specify your own bind parameters with your own names, and use the same statement repeatedly.  As mentioned at the top of this section, named bind parameters are always used regardless of the type of DBAPI being used; for DBAPI's that expect positional arguments, bind parameters are converted to lists right before execution, and Pyformat strings in statements, i.e. '%(name)s', are converted to the appropriate positional style.</p>
    <&|formatting.myt:code &>
        s = users.select(users.c.user_name==bindparam('username'))
        <&formatting.myt:poplink&>s.execute(username='fred')\
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password 
FROM users WHERE users.user_name = :username
{'username': 'fred'}
</&>
        <&formatting.myt:poplink&>s.execute(username='jane')\
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password 
FROM users WHERE users.user_name = :username
{'username': 'jane'}
</&>
        <&formatting.myt:poplink&>s.execute(username='mary')\
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password 
FROM users WHERE users.user_name = :username
{'username': 'mary'}
</&>
    </&>
    <p><span class="codeline">executemany()</span> is also available, but that applies more to INSERT/UPDATE/DELETE, described later.</p>
    <P>The generation of bind parameters is performed specific to the engine being used.  The examples in this document all show "named" parameters like those used in sqlite and oracle.  Depending on the parameter type specified by the DBAPI module, the correct bind parameter scheme will be used.</p>
    <&|doclib.myt:item, name="precompiling", description="Precompiling a Query" &>
    <p>By throwing the <span class="codeline">compile()</span> method onto the end of any query object, the query can be "compiled" by the SQLEngine into a <span class="codeline">sqlalchemy.sql.Compiled</span> object just once, and the resulting compiled object reused, which eliminates repeated internal compilation of the SQL string:</p>
        <&|formatting.myt:code &>
            s = users.select(users.c.user_name==bindparam('username')).compile()
            s.execute(username='fred')
            s.execute(username='jane')
            s.execute(username='mary')
        </&>
    </&>
    </&>
    <&|doclib.myt:item, name="textual", description="Literal Text Blocks" &>
        <p>The sql package tries to allow free textual placement in as many ways as possible.  In the examples below, note that the from_obj parameter is used only when no other information exists within the select object with which to determine table metadata.  Also note that in a query where there isnt even table metadata used, the SQLEngine to be used for the query has to be explicitly specified:
        <&|formatting.myt:code &>
            # strings as column clauses
            <&formatting.myt:poplink&>select(["user_id", "user_name"], from_obj=[users]).execute()
<&|formatting.myt:codepopper, link="sql" &>
SELECT user_id, user_name FROM users
{}
</&>
            # strings for full column lists
            <&formatting.myt:poplink&>select(
                    ["user_id, user_name, password, addresses.*"], 
                    from_obj=[users.alias('u'), addresses]).execute()
<&|formatting.myt:codepopper, link="sql" &>
SELECT u.user_id, u.user_name, u.password, addresses.* 
FROM users AS u, addresses
{}
</&>
            # functions, etc.
            <&formatting.myt:poplink&>select([users.c.user_id, "process_string(user_name)"]).execute()
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, process_string(user_name) FROM users
{}
</&>
            # where clauses
            <&formatting.myt:poplink&>users.select(and_(users.c.user_id==7, "process_string(user_name)=27")).execute()
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password FROM users 
WHERE users.user_id = :users_user_id AND process_string(user_name)=27
{'users_user_id': 7}
</&>
            # subqueries
            <&formatting.myt:poplink&>users.select(
                "exists (select 1 from addresses where addresses.user_id=users.user_id)").execute()
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password FROM users 
WHERE exists (select 1 from addresses where addresses.user_id=users.user_id)
{}
</&>
            # custom FROM objects
            <&formatting.myt:poplink&>select(
                    ["*"], 
                    from_obj=["(select user_id, user_name from users)"], 
                    engine=db).execute()
<&|formatting.myt:codepopper, link="sql" &>
SELECT * FROM (select user_id, user_name from users)
{}
</&>
            # a full query
            <&formatting.myt:poplink&>text("select user_name from users", engine=db).execute()
<&|formatting.myt:codepopper, link="sql" &>
select user_name from users
{}
</&>
            # or call text() off of the engine
            engine.text("select user_name from users").execute()
            
            # execute off the engine directly - you must use the engine's native bind parameter
            # style (i.e. named, pyformat, positional, etc.)
            <&formatting.myt:poplink&>db.execute(
                    "select user_name from users where user_id=:user_id", 
                    {'user_id':7}).execute()
<&|formatting.myt:codepopper, link="sql" &>
select user_name from users where user_id=:user_id
{'user_id':7}
</&>

            
        </&>

        <&|doclib.myt:item, name="textual_binds", description="Using Bind Parameters in Text Blocks" &>
        <p>Use the format <span class="codeline"><% ':<paramname>' |h %></span> to define bind parameters inside of a text block.  They will be converted to the appropriate format upon compilation:</p>
        <&|formatting.myt:code &>
            t = engine.text("select foo from mytable where lala=:hoho")
            r = t.execute(hoho=7)
        </&>        
        <p>Bind parameters can also be explicit, which allows typing information to be added.  Just specify them as a list with
        keys that match those inside the textual statement:</p>
        <&|formatting.myt:code &>
            t = engine.text("select foo from mytable where lala=:hoho", 
                        bindparams=[bindparam('hoho', type=types.String)])
            r = t.execute(hoho="im hoho")
        </&>        
        <p>Result-row type processing can be added via the <span class="codeline">typemap</span> argument, which 
        is a dictionary of return columns mapped to types:</p>
        <&|formatting.myt:code &>
            # specify DateTime type for the 'foo' column in the result set
            # sqlite, for example, uses result-row post-processing to construct dates
            t = engine.text("select foo from mytable where lala=:hoho", 
                    bindparams=[bindparam('hoho', type=types.String)],
                    typemap={'foo':types.DateTime}
                    )
            r = t.execute(hoho="im hoho")
            
            # 'foo' is a datetime
            year = r.fetchone()['foo'].year
        </&>        
        
        </&>
    </&>
    <&|doclib.myt:item, name="building", description="Building Select Objects" &>
    <p>One of the primary motivations for a programmatic SQL library is to allow the piecemeal construction of a SQL statement based on program variables.  All the above examples typically show Select objects being created all at once.  The Select object also includes "builder" methods to allow building up an object.  The below example is a "user search" function, where users can be selected based on primary key, user name, street address, keywords, or any combination:
        <&|formatting.myt:code &>
            def find_users(id=None, name=None, street=None, keywords=None):
                statement = users.select()
                if id is not None:
                    statement.append_whereclause(users.c.user_id==id)
                if name is not None:
                    statement.append_whereclause(users.c.user_name==name)
                if street is not None:
                    # append_whereclause joins "WHERE" conditions together with AND
                    statement.append_whereclause(users.c.user_id==addresses.c.user_id)
                    statement.append_whereclause(addresses.c.street==street)
                if keywords is not None:
                    statement.append_from(
                            users.join(userkeywords, users.c.user_id==userkeywords.c.user_id).join(
                                    keywords, userkeywords.c.keyword_id==keywords.c.keyword_id))
                    statement.append_whereclause(keywords.c.name.in_(keywords))
                    # to avoid multiple repeats, set query to be DISTINCT:
                    statement.distinct=True
                return statement.execute()
                
            <&formatting.myt:poplink&>find_users(id=7)
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password 
FROM users 
WHERE users.user_id = :users_user_id
{'users_user_id': 7}
</&>
            <&formatting.myt:poplink&>find_users(street='123 Green Street')
<&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id, users.user_name, users.password 
FROM users, addresses 
WHERE users.user_id = addresses.user_id AND addresses.street = :addresses_street
{'addresses_street': '123 Green Street'}
</&>
            <&formatting.myt:poplink&>find_users(name='Jack', keywords=['jack','foo'])
<&|formatting.myt:codepopper, link="sql" &>
SELECT DISTINCT users.user_id, users.user_name, users.password 
FROM users JOIN userkeywords ON users.user_id = userkeywords.user_id 
JOIN keywords ON userkeywords.keyword_id = keywords.keyword_id 
WHERE users.user_name = :users_user_name AND keywords.name IN ('jack', 'foo')
{'users_user_name': 'Jack'}
</&>

        </&>
    </&>
    <&|doclib.myt:item, name="insert", description="Inserts" &>
    <p>An INSERT involves just one table.  The Insert object is used via the insert() function, and the specified columns determine what columns show up in the generated SQL.  If primary key columns are left out of the criterion, the SQL generator will try to populate them as specified by the particular database engine and sequences, i.e. relying upon an auto-incremented column or explicitly calling a sequence beforehand.  Insert statements, as well as updates and deletes, can also execute multiple parameters in one pass via specifying an array of dictionaries as parameters.</p>
        <p>The values to be populated for an INSERT or an UPDATE can be specified to the insert()/update() functions as the <span class="codeline">values</span> named argument, or the query will be compiled based on the values of the parameters sent to the execute() method.</p>
<&|formatting.myt:code &>
            # basic insert
            <&formatting.myt:poplink&>users.insert().execute(user_id=1, user_name='jack', password='asdfdaf')
<&|formatting.myt:codepopper, link="sql" &>
INSERT INTO users (user_id, user_name, password) 
VALUES (:user_id, :user_name, :password)
{'user_name': 'jack', 'password': 'asdfdaf', 'user_id': 1}
</&>
            # insert just user_name, NULL for others
            # will auto-populate primary key columns if they are configured
            # to do so
            <&formatting.myt:poplink&>users.insert().execute(user_name='ed')
<&|formatting.myt:codepopper, link="sql" &>
INSERT INTO users (user_name) VALUES (:user_name)
{'user_name': 'ed'}
</&>
            # INSERT with a list:
            <&formatting.myt:poplink&>users.insert(values=(3, 'jane', 'sdfadfas')).execute()
<&|formatting.myt:codepopper, link="sql" &>
INSERT INTO users (user_id, user_name, password) 
VALUES (:user_id, :user_name, :password)
{'user_id': 3, 'password': 'sdfadfas', 'user_name': 'jane'}
</&>
            # INSERT with user-defined bind parameters
            i = users.insert(
                values={'user_name':bindparam('name'), 'password':bindparam('pw')}
                )
            <&formatting.myt:poplink&>i.execute(name='mary', pw='adas5fs')
<&|formatting.myt:codepopper, link="sql" &>
INSERT INTO users (user_name, password) VALUES (:name, :pw)
{'name': 'mary', 'pw': 'adas5fs'}
</&>
            # INSERT many - if no explicit 'values' parameter is sent,
            # the first parameter list in the list determines
            # the generated SQL of the insert (i.e. what columns are present)
            # executemany() is used at the DBAPI level
            <&formatting.myt:poplink&>users.insert().execute(
                {'user_id':7, 'user_name':'jack', 'password':'asdfasdf'},
                {'user_id':8, 'user_name':'ed', 'password':'asdffcadf'},
                {'user_id':9, 'user_name':'fred', 'password':'asttf'},
            )
<&|formatting.myt:codepopper, link="sql" &>
INSERT INTO users (user_id, user_name, password) 
VALUES (:user_id, :user_name, :password)
[{'user_name': 'jack', 'password': 'asdfasdf', 'user_id': 7}, 
{'user_name': 'ed', 'password': 'asdffcadf', 'user_id': 8}, 
{'user_name': 'fred', 'password': 'asttf', 'user_id': 9}]
</&>            
        </&>
    </&>
    <&|doclib.myt:item, name="update", description="Updates" &>
        <p>Updates work a lot like INSERTS, except there is an additional WHERE clause that can be specified.</p>
        <&|formatting.myt:code &>
            # change 'jack' to 'ed'
            <&formatting.myt:poplink&>users.update(users.c.user_name=='jack').execute(user_name='ed')
            <&|formatting.myt:codepopper, link="sql" &>
            UPDATE users SET user_name=:user_name WHERE users.user_name = :users_user_name
            {'users_user_name': 'jack', 'user_name': 'ed'}
            </&>
            # use bind parameters
            u = users.update(users.c.user_name==bindparam('name'), 
                            values={'user_name':bindparam('newname')})
            <&formatting.myt:poplink&>u.execute(name='jack', newname='ed')
            <&|formatting.myt:codepopper, link="sql" &>
            UPDATE users SET user_name=:newname WHERE users.user_name = :name
            {'newname': 'ed', 'name': 'jack'}
            </&>

            # update a column to another column
            <&formatting.myt:poplink&>users.update(values={users.c.password:users.c.user_name}).execute()
            <&|formatting.myt:codepopper, link="sql" &>
            UPDATE users SET password=users.user_name
            {}
            </&>

            # multi-update
            <&formatting.myt:poplink&>users.update(users.c.user_id==bindparam('id')).execute(
                    {'id':7, 'user_name':'jack', 'password':'fh5jks'},
                    {'id':8, 'user_name':'ed', 'password':'fsr234ks'},
                    {'id':9, 'user_name':'mary', 'password':'7h5jse'},
                )
            <&|formatting.myt:codepopper, link="sql" &>
            UPDATE users SET user_name=:user_name, password=:password WHERE users.user_id = :id
            [{'password': 'fh5jks', 'user_name': 'jack', 'id': 7}, 
            {'password': 'fsr234ks', 'user_name': 'ed', 'id': 8}, 
            {'password': '7h5jse', 'user_name': 'mary', 'id': 9}]
            </&>


        </&>
        <&|doclib.myt:item, name="correlated", description="Correlated Updates" &>
        <p>A correlated update lets you update a table using selection from another table, or the same table:</p>
        <&|formatting.myt:code &>
            s = select([addresses.c.city], addresses.c.user_id==users.c.user_id)
            <&formatting.myt:poplink&>users.update(
                    and_(users.c.user_id>10, users.c.user_id<20), 
                    values={users.c.user_name:s}
                    ).execute() 

            <&|formatting.myt:codepopper, link="sql" &>
            UPDATE users SET user_name=(SELECT addresses.city 
            FROM addresses 
            WHERE addresses.user_id = users.user_id) 
            WHERE users.user_id > :users_user_id AND users.user_id < :users_user_id_1
            {'users_user_id_1': 20, 'users_user_id': 10}

            </&>

        </&>
        </&>
    </&>
    <&|doclib.myt:item, name="delete", description="Deletes" &>
    <p>A delete is formulated like an update, except theres no values:</p>
    <&|formatting.myt:code &>
        users.delete(users.c.user_id==7).execute()
        users.delete(users.c.user_name.like(bindparam('name'))).execute(
                {'name':'%Jack%'},
                {'name':'%Ed%'},
                {'name':'%Jane%'},
            )
        users.delete(exists())
    </&>
    </&>
</&>
