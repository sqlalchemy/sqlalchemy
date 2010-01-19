"""environment.py

Establish data / cache file paths, and configurations, 
bootstrap fixture data if necessary.

"""
import meta, model, fixture_data
from sqlalchemy import create_engine
import os

root = "./beaker_data/"

if not os.path.exists(root):
    raw_input("Will create datafiles in %r.\n"
                "To reset the cache + database, delete this directory.\n"
                "Press enter to continue.\n" % root
                )
    os.makedirs(root)
    
dbfile = os.path.join(root, "beaker_demo.db")
engine = create_engine('sqlite:///%s' % dbfile, echo=True)
meta.Session.configure(bind=engine)

# configure the "default" cache region.
meta.cache_manager.regions['default'] ={

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
if not os.path.exists(dbfile):
    fixture_data.install()
    installed = True