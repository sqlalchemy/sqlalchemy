from sqlalchemy.orm.query import Query, _generative
from sqlalchemy.orm.session import Session

# the cache.  This would be replaced with the caching mechanism of
# choice, i.e. LRU cache, memcached, etc.
_cache = {}

class CachingQuery(Query):
    
    # generative method to set a "cache" key.  The method of "keying" the cache
    # here can be made more sophisticated, such as caching based on the query._criterion.
    @_generative()
    def with_cache_key(self, cachekey):
        self.cachekey = cachekey

    # single point of object loading is __iter__().  objects in the cache are not associated
    # with a session and are never returned directly; only merged copies.
    def __iter__(self):
        if hasattr(self, 'cachekey'):
            try:
                ret = _cache[self.cachekey]
            except KeyError:
                ret = list(Query.__iter__(self))
                for x in ret:
                    self.session.expunge(x)
                _cache[self.cachekey] = ret

            return iter(self.session.merge(x, dont_load=True) for x in ret)

        else:
            return Query.__iter__(self)

# example usage
if __name__ == '__main__':
    from sqlalchemy import Column, create_engine, Integer, String
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base
    
    Session = sessionmaker(query_cls=CachingQuery)
    
    Base = declarative_base(engine=create_engine('sqlite://', echo=True))
    
    class User(Base):
        __tablename__ = 'users'
        id = Column(Integer, primary_key=True)
        name = Column(String(100))
        
        def __repr__(self):
            return "User(name=%r)" % self.name

    Base.metadata.create_all()
    
    sess = Session()
    
    sess.add_all(
        [User(name='u1'), User(name='u2'), User(name='u3')]
    )
    sess.commit()
    
    # cache two user objects
    sess.query(User).with_cache_key('u2andu3').filter(User.name.in_(['u2', 'u3'])).all()
    
    sess.close()
    
    sess = Session()
    
    # pull straight from cache
    print sess.query(User).with_cache_key('u2andu3').all()
    
    
    
    
