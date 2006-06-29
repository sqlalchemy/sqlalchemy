from sqlalchemy import *
import sys
sys.path.insert(0, './lib/')

engine = create_engine('sqlite://')

engine.echo = True

# table metadata
users = Table('users', engine, 
    Column('user_id', Integer, primary_key = True),
    Column('user_name', String(16), nullable = False),
    Column('password', String(20), nullable = False)
)
users.create()
users.insert().execute(
    dict(user_name = 'fred', password='45nfss')
)


# class definition
class User(object):
    pass
assign_mapper(User, users)
  
# select
user = User.get_by(user_name = 'fred')

# modify
user.user_name = 'fred jones'

# commit
objectstore.commit()

objectstore.clear()



addresses = Table('email_addresses', engine,
    Column('address_id', Integer, primary_key = True),
    Column('user_id', Integer, ForeignKey(users.c.user_id)),
    Column('email_address', String(20)),
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
    Column('pref_id', Integer, primary_key = True),
    Column('stylename', String(20)),
    Column('save_password', Boolean, nullable = False),
    Column('timezone', CHAR(3), nullable = False)
)
prefs.create()
prefs.insert().execute(
    dict(pref_id=1, stylename='green', save_password=1, timezone='EST')
)

# user table gets 'preference_id' column added
users = Table('users', engine, 
    Column('user_id', Integer, primary_key = True),
    Column('user_name', String(16), nullable = False),
    Column('password', String(20), nullable = False),
    Column('preference_id', Integer, ForeignKey(prefs.c.pref_id))
)
users.drop()
users.create()
users.insert().execute(
    dict(user_name = 'fred', password='45nfss', preference_id=1)
)


addresses = Table('email_addresses', engine,
    Column('address_id', Integer, primary_key = True),
    Column('user_id', Integer, ForeignKey(users.c.user_id)),
    Column('email_address', String(20)),
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



articles = Table('articles', engine,
    Column('article_id', Integer, primary_key = True),
    Column('article_headline', String(150), key='headline'),
    Column('article_body', CLOB, key='body'),
)

keywords = Table('keywords', engine,
    Column('keyword_id', Integer, primary_key = True),
    Column('name', String(50))
)

itemkeywords = Table('article_keywords', engine,
    Column('article_id', Integer, ForeignKey(articles.c.article_id)),
    Column('keyword_id', Integer, ForeignKey(keywords.c.keyword_id))
)

articles.create()
keywords.create()
itemkeywords.create()

# class definitions
class Keyword(object):
    def __init__(self, name = None):
        self.name = name
    mapper = assignmapper(keywords)
    
class Article(object):
    def __init__(self):
        self.keywords = []
    mapper = assignmapper(articles, properties = dict(
        keywords = relation(Keyword.mapper, itemkeywords, lazy=False)
        ))
Article.mapper

article = Article()
article.headline = 'a headline'
article.body = 'this is the body'
article.keywords.append(Keyword('politics'))
article.keywords.append(Keyword('entertainment'))
objectstore.commit()

# select articles based on some keywords.  the extra selection criterion 
# won't get in the way of the separate eager load of all the article's keywords
alist = Article.mapper.select(sql.and_(
                keywords.c.keyword_id==itemkeywords.c.keyword_id, 
                itemkeywords.c.article_id==articles.c.article_id,
                keywords.c.name.in_('politics', 'entertainment')))

# modify
a = alist[0]
del a.keywords[:]
a.keywords.append(Keyword('topstories'))
a.keywords.append(Keyword('government'))

# commit.  individual INSERT/DELETE operations will take place only for the list
# elements that changed.
objectstore.commit()


clear_mappers()
itemkeywords.drop()
itemkeywords = Table('article_keywords', engine,
    Column('article_id', Integer, ForeignKey("articles.article_id")),
    Column('keyword_id', Integer, ForeignKey("keywords.keyword_id")),
    Column('attached_by', Integer, ForeignKey("users.user_id"))
, redefine=True)
itemkeywords.create()

# define an association class
class KeywordAssociation(object):pass

# define the mapper. when we load an article, we always want to get the keywords via
# eager loading.  but the user who added each keyword, we usually dont need so specify 
# lazy loading for that.
m = mapper(Article, articles, properties=dict(
    keywords = relation(KeywordAssociation, itemkeywords, lazy = False, 
        primary_key=[itemkeywords.c.article_id, itemkeywords.c.keyword_id], 
        properties=dict(
            keyword = relation(Keyword, keywords, lazy = False),
            user = relation(User, users, lazy = True)
        )
    )
    )
)

# bonus step - well, we do want to load the users in one shot, 
# so modify the mapper via an option.
# this returns a new mapper with the option switched on.
m2 = m.options(eagerload('keywords.user'))

# select by keyword again
alist = m2.select(
            sql.and_(
                keywords.c.keyword_id==itemkeywords.c.keyword_id, 
                itemkeywords.c.article_id==articles.c.article_id,
                keywords.c.name == 'jacks_stories'
            ))

# user is available
for a in alist:
    for k in a.keywords:
        if k.keyword.name == 'jacks_stories':
            print k.user.user_name

