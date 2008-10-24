"""Example of caching objects in a per-session cache,
including implicit usage of the statement and params as a key.

"""
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.session import Session

class CachingQuery(Query):
    
    def __iter__(self):
        try:
            cache = self.session._cache
        except AttributeError:
            self.session._cache = cache = {}
            
        stmt = self.statement.compile()
        params = stmt.params
        params.update(self._params)
        cachekey = str(stmt) + str(params)

        try:
            ret = cache[cachekey]
        except KeyError:
            ret = list(Query.__iter__(self))
            cache[cachekey] = ret

        return iter(ret)


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
    
    # issue a query
    print sess.query(User).filter(User.name.in_(['u2', 'u3'])).all()

    # issue another
    print sess.query(User).filter(User.name == 'u1').all()
    
    # pull straight from cache
    print sess.query(User).filter(User.name.in_(['u2', 'u3'])).all()

    print sess.query(User).filter(User.name == 'u1').all()
    

