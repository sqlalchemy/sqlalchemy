<%flags>inherit='document_base.myt'</%flags>
<&|doclib.myt:item, name="datamapping", description="Basic Data Mapping" &>

<&|doclib.myt:item, name="synopsis", description="Synopsis" &>

        <&|formatting.myt:code&>
        from sqlalchemy.schema import *
        from sqlalchemy.mapper import *
        import sqlalchemy.databases.sqlite as sqlite
        engine = sqlite.engine(':memory:', {})
        
        # table metadata
        users = Table('users', engine, 
            Column('user_id', Integer, primary_key = True),
            Column('user_name', String(16), nullable = False),
            Column('password', String(20), nullable = False)
        )
        
        # class definition with mapper (mapper can also be separate)
        class User(object):
            def __init__(self):
                pass
    
            mapper = assignmapper(users)
        
        # select
        user = User.mapper.select(User.c.user_name == 'fred')[0]  <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id AS users_user_id, users.user_name AS users_user_name, 
users.password AS users_password 
FROM users WHERE users.user_name = :users_user_name

{'users_user_name': 'fred'}

        </&>
        # modify
        user.user_name = 'fred jones'
        
        # commit
        objectstore.commit() <&|formatting.myt:codepopper, link="sql" &>

UPDATE users SET user_id=:user_id, user_name=:user_name, 
password=:password WHERE users.user_id = :user_id

[{'user_name': 'fred jones', 'password': u'45nfss', 'user_id': 1}]        
        </&>
        
    </&>
</&>

<&|doclib.myt:item, name="onetomany", description="One to Many" &>

        <&|formatting.myt:code&>
        # second table metadata
        addresses = Table('email_addresses', engine,
            Column('address_id', Integer, primary_key = True),
            Column('user_id', Integer, ForeignKey("users.user_id")),
            Column('email_address', String(20)),
        )
        
        # second class definition
        class Address(object):
            def __init__(self, email_address = None):
                self.email_address = email_address

            mapper = assignmapper(addresses)
        
    
        # give the User class a new Mapper referencing addresses.  
        # "private=True" means deletions of the user
        # will cascade down to the child Address objects
        User.mapper = assignmapper(users, properties = dict(
            relation(Address.mapper, lazy=True, private=True)
        ))
        
        # select
        user = User.mapper.select(User.c.user_name == 'fred jones')[0] <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id AS users_user_id, users.user_name AS users_user_name, 
users.password AS users_password FROM users
WHERE users.user_name = :users_user_name

{'users_user_name': 'fred jones'}
</&>
address = user.addresses[0] <&|formatting.myt:codepopper, link="sql" &>

SELECT email_addresses.address_id AS email_addresses_address_id, 
email_addresses.user_id AS email_addresses_user_id, 
email_addresses.email_address AS email_addresses_email_address 
FROM email_addresses
WHERE email_addresses.user_id = :users_user_id

{'users_user_id': 1}

</&>
        
        # modify
        user.user_name = 'fred'
        user.addresses[0].email_address = 'fredjones@foo.com'
        user.addresses.append(Address('freddy@hi.org'))
        
        # commit
        objectstore.commit() <&|formatting.myt:codepopper, link="sql" &>

UPDATE users SET user_id=:user_id, user_name=:user_name, 
password=:password WHERE users.user_id = :user_id

[{'user_name': u'fred', 'password': u'45nfss', 'user_id': 1}]

UPDATE email_addresses SET address_id=:address_id, user_id=:user_id, 
email_address=:email_address WHERE email_addresses.address_id = :address_id

[{'email_address': 'fredjones@foo.com', 'address_id': 1, 'user_id': 1}]

INSERT IntegerO email_addresses (address_id, user_id, email_address) 
VALUES (:address_id, :user_id, :email_address)

{'email_address': 'freddy@hi.org', 'address_id': None, 'user_id': 1}

</&>
    </&>
</&>

<&|doclib.myt:item, name="onetoone", description="One to One" &>

        <&|formatting.myt:code&>
        # a table to store a user's preferences for a site
        prefs = Table('user_prefs', engine,
            Column('pref_id', Integer, primary_key = True),
            Column('stylename', String(20)),
            Column('save_password', Boolean, nullable = False),
            Column('timezone', CHAR(3), nullable = False)
        )

        # user table gets 'preference_id' column added
        users = Table('users', engine, 
            Column('user_id', Integer, primary_key = True),
            Column('user_name', String(16), nullable = False),
            Column('password', String(20), nullable = False),
            Column('preference_id', Integer, ForeignKey("prefs.pref_id"))
        )
        
        # class definition for preferences
        class UserPrefs(object):
            mapper = assignmapper(prefs)
    
        # make a new mapper referencing everything.
        m = mapper(User, users, properties = dict(
            addresses = relation(Address.mapper, lazy=True, private=True),
            preferences = relation(UserPrefs.mapper, lazy=False, private=True),
        ))
        
        # select
        user = m.select(users.c.user_name == 'fred')[0] <&|formatting.myt:codepopper, link="sql" &>
SELECT users.user_id AS users_user_id, users.user_name AS users_user_name, 
users.password AS users_password, users.preference_id AS users_preference_id, 
user_prefs.pref_id AS user_prefs_pref_id, 
user_prefs.stylename AS user_prefs_stylename, 
user_prefs.save_password AS user_prefs_save_password, 
user_prefs.timezone AS user_prefs_timezone
FROM users 
LEFT OUTER JOIN user_prefs ON user_prefs.pref_id = users.preference_id
WHERE users.user_name = :users_user_name
        </&>
        save_password = user.preferences.save_password
        
        # modify
        user.preferences.stylename = 'bluesteel'
        user.addresses.append(Address('freddy@hi.org')) <&|formatting.myt:codepopper, link="sql" &>
        # put "lazy load addresses" sql here
        </&>
        
        # commit
        objectstore.commit() <&|formatting.myt:codepopper, link="sql" &>
UPDATE user_prefs SET pref_id=:pref_id, stylename=:stylename, 
save_password=:save_password, timezone=:timezone 
WHERE user_prefs.pref_id = :pref_id

[{'timezone': u'EST', 'stylename': 'bluesteel', 'save_password': 1, 'pref_id': 1}]

INSERT INTO email_addresses (address_id, user_id, email_address) 
VALUES (:address_id, :user_id, :email_address)

{'email_address': 'freddy@hi.org', 'address_id': None, 'user_id': 1}
</&>
    </&>
</&>

<&|doclib.myt:item, name="manytomany", description="Many to Many" &>
        <&|formatting.myt:code&>
    articles = Table('articles', engine,
        Column('article_id', Integer, primary_key = True),
        Column('article_headline', String(150), key='headline'),
        Column('article_body', Text, key='body'),
    )

    keywords = Table('keywords', engine,
        Column('keyword_id', Integer, primary_key = True),
        Column('name', String(50))
    )

    itemkeywords = Table('article_keywords', engine,
        Column('article_id', Integer, ForeignKey("articles.article_id")),
        Column('keyword_id', Integer, ForeignKey("keywords.keyword_id"))
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
        
        
    Article.mapper = mapper(Article, articles, properties = dict(
            keywords = relation(Keyword.mapper, itemkeywords, lazy=False)
            )
        )

    article = Article()
    article.headline = 'a headline'
    article.body = 'this is the body'
    article.keywords.append(Keyword('politics'))
    article.keywords.append(Keyword('entertainment'))
    objectstore.commit()

    # select articles based on some keywords.  the extra selection criterion 
    # won't get in the way of the separate eager load of all the article's keywords
    articles = Article.mapper.select(sql.and_(keywords.c.keyword_id==articles.c.article_id, keywords.c.name.in_('politics', 'entertainment')))

    # modify
    a = articles[0]
    del a.keywords[:]
    a.keywords.append(Keyword('topstories'))
    a.keywords.append(Keyword('government'))

    # commit.  individual INSERT/DELETE operations will take place only for the list
    # elements that changed.
    objectstore.commit()


        </&>
        
        <p>Many to Many can also be done with an Association object, that adds additional information about how two items are related:</p>
        <&|formatting.myt:code&>
            # add "attached_by" column which will reference the user who attached this keyword
            itemkeywords = Table('article_keywords', engine,
                Column('article_id', Integer, ForeignKey("articles.article_id")),
                Column('keyword_id', Integer, ForeignKey("keywords.keyword_id")),
                Column('attached_by', Integer, ForeignKey("users.user_id"))
            )

            # define an association class
            class KeywordAssociation(object):pass
            
            # define the mapper. when we load an article, we always want to get the keywords via
            # eager loading.  but the user who added each keyword, we usually dont need so specify 
            # lazy loading for that.
            m = mapper(Article, articles, properties=dict(
                keywords = relation(KeywordAssociation, itemkeywords, lazy = False, properties=dict(
                    keyword = relation(Keyword, keywords, lazy = False),
                    user = relation(User, users, lazy = True)
                    )
                )
                )
            )
            
            # bonus step - well, we do want to load the users in one shot, 
            # so modify the mapper via an option.
            # this returns a new mapper with the option switched on.
            m2 = mapper.options(eagerload('user'))
            
            # select by keyword again
            articles = m.select(
                        sql.and_(
                            keywords.c.keyword_id==articles.c.article_id, 
                            keywords.c.keyword_name == 'jacks_stories'
                        ))
            
            # user is available
            for a in articles:
                for k in a.keywords:
                    if k.keyword.name == 'jacks_stories':
                        print k.user.user_name
            
        </&>
        
</&>

</&>