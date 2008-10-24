"""Example of caching objects in a per-session cache.


This approach is faster in that objects don't need to be detached/remerged
between sessions, but is slower in that the cache is empty at the start
of each session's lifespan.

"""

from sqlalchemy.orm.query import Query, _generative
from sqlalchemy.orm.session import Session

class CachingQuery(Query):
    
    # generative method to set a "cache" key.  The method of "keying" the cache
    # here can be made more sophisticated, such as caching based on the query._criterion.
    @_generative()
    def with_cache_key(self, cachekey):
        self.cachekey = cachekey

    def __iter__(self):
        if hasattr(self, 'cachekey'):
            try:
                cache = self.session._cache
            except AttributeError:
                self.session._cache = cache = {}
                
            try:
                ret = cache[self.cachekey]
            except KeyError:
                ret = list(Query.__iter__(self))
                cache[self.cachekey] = ret

            return iter(ret)

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
    
    # pull straight from cache
    print sess.query(User).with_cache_key('u2andu3').all()
    
