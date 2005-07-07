<%flags>inherit='document_base.myt'</%flags>



<&|doclib.myt:item, name="coolthings", description="Cool Things You Can Do With SQLAlchemy" &>

<&|formatting.myt:code, syntaxtype="python" &>
# first, some imports
from sqlalchemy.sql import *
from sqlalchemy.schema import *


# make a database engine based on sqlite
import sqlalchemy.databases.sqlite as sqlite_db
db = sqlite_db.engine('foo.db', pool_size = 10, max_overflow = 5)

# define metadata for a table

users = Table('users', db,
	Column('user_id', INT),
	Column('user_name', VARCHAR(20)),
	Column('password', CHAR(10))
)


# select rows from the table

query = users.select()
cursor = query.execute()
rows = cursor.fetchall()


# select rows from the table where user_name=='ed'
rows = users.select(users.c.user_name == 'ed').execute().fetchall()

# make a query  with a bind param
query = select([users], users.c.user_id == bindparam('userid'))

# execute with params
rows = query.execute(userid = 7).fetchall()


# make another table
addresses = Table('addresses', db, 
	Column('address_id', INT),
	Column('user_id', INT),
	Column('street', VARCHAR(20)),
	Column('city', VARCHAR(20)),
	Column('state', CHAR(2)),
	Column('zip', CHAR(5))
)

# no, really, make this table in the DB via CREATE
addresses.build()


# make a nonsensical query that selects from an outer join, and 
# throws in a literally-defined EXISTS clause 
query = select(
	[users, addresses],
	and_(
	    addresses.c.street == 'Green Street',
            addresses.c.city == 'New York',
	    users.c.user_id != 12,
	    "EXISTS (select 1 from special_table where user_id=users.user_id)"
	),
	from_obj = [ outerjoin(users, addresses, addresses.user_id==users.user_id) ]
	)


# insert into a table
users.insert().execute(user_id = 7, user_name = 'jack')

# update the table
users.update(users.c.user_id == 7).execute(user_name = 'fred')


# get DBAPI connections from the higher-level engine
c = db.connection()


# use the connection pooling directly:

# import a real DBAPI database
from pysqlite2 import dbapi2 as sqlite

# make an implicit pool around it
import sqlalchemy.pool as pool
sqlite = pool.manage(sqlite, pool_size = 10, max_overflow = 5, use_threadlocal = True)

# get a pooled connection local to the current thread
c = sqlite.connect('foo.db')
cursor = c.cursor()

# return the connection to the pool
cursor = None
c = None

</&>

</&>
