"""
Ready for some really powerful stuff ?

We're going to use Beaker caching, and create functions that load and cache what we want, which
can be used in any scenario.  Then we're going to associate them with many-to-one relations
for individual queries.  

Think of it as lazy loading from a long term cache.  For rarely-mutated objects, this is a super
performing way to go.

"""

from sqlalchemy.orm.query import Query, _generative
from sqlalchemy.orm.interfaces import MapperOption
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import visitors

class CachingQuery(Query):
    """override __iter__ to pull results from a callable 
       that might have been attached to the Query.
        
    """
    def __iter__(self):
        if hasattr(self, 'cache_callable'):
            try:
                ret = self.cache_callable(self)
            except KeyError:
                ret = list(Query.__iter__(self))
                for x in ret:
                    self.session.expunge(x)

            return iter(self.session.merge(x, dont_load=True) for x in ret)

        else:
            return Query.__iter__(self)

class FromCallable(MapperOption):
    """A MapperOption that associates a callable with particular 'path' load.
    
    When a lazyload occurs, the Query has a "path" which is a tuple of
    (mapper, key, mapper, key) indicating the path along relations from
    the original mapper to the endpoint mapper.
    
    """
    
    propagate_to_loaders = True
    
    def __init__(self, key):
        self.cls_ = key.property.parent.class_
        self.propname = key.property.key
    
    def __call__(self, q):
        raise NotImplementedError()
        
    def process_query(self, query):
        if query._current_path:
            mapper, key = query._current_path[-2:]
            if mapper.class_ is self.cls_ and key == self.propname:
                query.cache_callable = self

def params_from_query(query):
    """Pull the bind parameter values from a query.
    
    This takes into account any scalar attribute bindparam set up.
    
    E.g. params_from_query(query.filter(Cls.foo==5).filter(Cls.bar==7)))
    would return [5, 7].
    
    
    """
    
    v = []
    def visit_bindparam(bind):
        value = query._params.get(bind.key, bind.value)
        v.append(value)
    visitors.traverse(query._criterion, {}, {'bindparam':visit_bindparam})
    return v
                
if __name__ == '__main__':
    """Usage example.  We'll use Beaker to set up a region and then a short def
    that loads a 'Widget' object by id.
    
    """
    from sqlalchemy.orm import sessionmaker, scoped_session
    from sqlalchemy import create_engine
    
    # beaker 1.4 or above
    from beaker.cache import CacheManager 

    # sample CacheManager.   In reality, use memcached.   (seriously, don't bother
    # with any other backend.)
    cache_manager = CacheManager(
                        cache_regions={
                            'default_region':{'type':'memory','expire':3600}
                        }
                    )

    # SQLA configuration
    engine=create_engine('sqlite://', echo=True)
    Session = scoped_session(sessionmaker(query_cls=CachingQuery, bind=engine))
    
    from sqlalchemy import Column, Integer, String, ForeignKey
    from sqlalchemy.orm import relation
    from sqlalchemy.ext.declarative import declarative_base

    # mappings
    Base = declarative_base()
    
    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String(100))
        widget_id = Column(Integer, ForeignKey('widget.id'))
        
        widget = relation("Widget")

    class Widget(Base):
        __tablename__ = 'widget'
        id = Column(Integer, primary_key=True)
        name = Column(String(100))

    # Widget loading.
    
    @cache_manager.region('default_region', 'byid')
    def load_widget(widget_id):
        """Load a widget by id, caching the result in Beaker."""
        
        return Session.query(Widget).filter(Widget.id==widget_id).first()

    class CachedWidget(FromCallable):
        """A MapperOption that will pull user widget links from Beaker.
        
        We build a subclass of FromCallable with a __call__ method
        so that the option itself is pickleable.
        
        """
        def __call__(self, q):
            return [load_widget(*params_from_query(q))]

    Base.metadata.create_all(engine)
    
    sess = Session()
    
    # create data.
    w1 = Widget(name='w1')
    w2 = Widget(name='w2')
    sess.add_all(
        [User(name='u1', widget=w1), User(name='u2', widget=w1), User(name='u3', widget=w2)]
    )
    sess.commit()
    
    # call load_widget with 1 and 2.  this will cache those widget objects in beaker.
    w1 = load_widget(1)
    w2 = load_widget(2)

    # clear session entirely.
    sess.expunge_all()
    
    # load users, sending over our option.
    u1, u2, u3 = sess.query(User).options(CachedWidget(User.widget)).order_by(User.id).all()
    
    print "------------------------"

    # access the "Widget".   No SQL occurs below this line !
    assert u1.widget.name == 'w1'

    # access the same "Widget" on u2.  Local w1 is reused, no extra cache roundtrip !
    assert u2.widget.name == 'w1'
    assert u2.widget is u1.widget
    
    assert u3.widget.name == 'w2'

    # user + the option (embedded in its state)
    # are pickleable themselves (important for further caching)
    import pickle
    assert pickle.dumps(u1)