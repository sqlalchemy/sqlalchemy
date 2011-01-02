"""environment.py

Establish data / cache file paths, and configurations, 
bootstrap fixture data if necessary.

"""
import caching_query
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from beaker import cache
import os

# Beaker CacheManager.  A home base for cache configurations.
cache_manager = cache.CacheManager()

# scoped_session.  Apply our custom CachingQuery class to it,
# using a callable that will associate the cache_manager
# with the Query.
Session = scoped_session(
                sessionmaker(
                    query_cls=caching_query.query_callable(cache_manager)
                )
            )

# global declarative base class.
Base = declarative_base()


root = "./beaker_data/"

if not os.path.exists(root):
    raw_input("Will create datafiles in %r.\n"
                "To reset the cache + database, delete this directory.\n"
                "Press enter to continue.\n" % root
                )
    os.makedirs(root)

dbfile = os.path.join(root, "beaker_demo.db")
engine = create_engine('sqlite:///%s' % dbfile, echo=True)
Session.configure(bind=engine)

# configure the "default" cache region.
cache_manager.regions['default'] ={

        # using type 'file' to illustrate
        # serialized persistence.  In reality,
        # use memcached.   Other backends
        # are much, much slower.
        'type':'file',
        'data_dir':root,
        'expire':3600,

        # set start_time to current time
        # to re-cache everything
        # upon application startup
        #'start_time':time.time()
    }

installed = False

def bootstrap():
    global installed
    import fixture_data
    if not os.path.exists(dbfile):
        fixture_data.install()
        installed = True