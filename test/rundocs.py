from sqlalchemy.schema import *
from sqlalchemy.mapper import *
import sqlalchemy.databases.sqlite as sqlite
engine = sqlite.engine(':memory:', {})

engine.echo = True

# table metadata
users = Table('users', engine, 
    Column('user_id', INTEGER, primary_key = True),
    Column('user_name', VARCHAR(16), nullable = False),
    Column('password', VARCHAR(20), nullable = False)
)
users.create()
users.insert().execute(
    dict(user_name = 'fred', password='45nfss')
)


# class definition
class User(object):
    mapper = assignmapper(users)
  
# select
user = User.mapper.select(User.c.user_name == 'fred')[0]      

# modify
user.user_name = 'fred jones'

# commit
objectstore.commit()

objectstore.clear()

addresses = Table('email_addresses', engine,
    Column('address_id', INT, primary_key = True),
    Column('user_id', INT, foreign_key = ForeignKey(users.c.user_id)),
    Column('email_address', VARCHAR(20)),
)
addresses.create()
addresses.insert().execute(
    dict(user_id = user.user_id, email_address='fred@bar.com')
)

# second class definition
class Address(object):
    def __init__(self, email_address = None):
        self.email_address = email_address

    mapper = assignmapper(addresses)
    
# obtain a Mapper.  "private=True" means deletions of the user
# will cascade down to the child Address objects
User.mapper = assignmapper(users, properties = dict(
    addresses = relation(Address.mapper, lazy=True, private=True)
))

# select
user = User.mapper.select(User.c.user_name == 'fred jones')[0]
print repr(user.__dict__['addresses'])
address = user.addresses[0]

# modify
user.user_name = 'fred'
user.addresses[0].email_address = 'fredjones@foo.com'
user.addresses.append(Address('freddy@hi.org'))

# commit
objectstore.commit()

# going to change tables, etc., start over with a new engine
objectstore.clear()
engine = None
engine = sqlite.engine(':memory:', {})
engine.echo = True

# a table to store a user's preferences for a site
prefs = Table('user_prefs', engine,
    Column('pref_id', INT, primary_key = True),
    Column('stylename', VARCHAR(20)),
    Column('save_password', BOOLEAN, nullable = False),
    Column('timezone', CHAR(3), nullable = False)
)
prefs.create()
prefs.insert().execute(
    dict(pref_id=1, stylename='green', save_password=1, timezone='EST')
)

# user table gets 'preference_id' column added
users = Table('users', engine, 
    Column('user_id', INTEGER, primary_key = True),
    Column('user_name', VARCHAR(16), nullable = False),
    Column('password', VARCHAR(20), nullable = False),
    Column('preference_id', INTEGER, foreign_key = ForeignKey(prefs.c.pref_id))
)
users.drop()
users.create()
users.insert().execute(
    dict(user_name = 'fred', password='45nfss', preference_id=1)
)


addresses = Table('email_addresses', engine,
    Column('address_id', INT, primary_key = True),
    Column('user_id', INT, foreign_key = ForeignKey(users.c.user_id)),
    Column('email_address', VARCHAR(20)),
)
addresses.drop()
addresses.create()

Address.mapper = assignmapper(addresses)

# class definition for preferences
class UserPrefs(object):
    mapper = assignmapper(prefs)
    
# set a new Mapper on the user
User.mapper = assignmapper(users, properties = dict(
    addresses = relation(Address.mapper, lazy=True, private=True),
    preferences = relation(UserPrefs.mapper, lazy=False, private=True),
))

# select
user = User.mapper.select(User.c.user_name == 'fred')[0]
save_password = user.preferences.save_password

# modify
user.preferences.stylename = 'bluesteel'
user.addresses.append(Address('freddy@hi.org'))

# commit
objectstore.commit()