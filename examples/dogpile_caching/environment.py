"""environment.py

Establish data / cache file paths, and configurations,
bootstrap fixture data if necessary.

"""
from . import caching_query
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dogpile.cache.region import make_region
import os
from hashlib import md5
import sys
py2k = sys.version_info < (3, 0)

if py2k:
    input = raw_input

# dogpile cache regions.  A home base for cache configurations.
regions = {}


# scoped_session.  Apply our custom CachingQuery class to it,
# using a callable that will associate the dictionary
# of regions with the Query.
Session = scoped_session(
                sessionmaker(
                    query_cls=caching_query.query_callable(regions)
                )
            )

# global declarative base class.
Base = declarative_base()

root = "./dogpile_data/"

if not os.path.exists(root):
    input("Will create datafiles in %r.\n"
                "To reset the cache + database, delete this directory.\n"
                "Press enter to continue.\n" % root
                )
    os.makedirs(root)

dbfile = os.path.join(root, "dogpile_demo.db")
engine = create_engine('sqlite:///%s' % dbfile, echo=True)
Session.configure(bind=engine)


def md5_key_mangler(key):
    """Receive cache keys as long concatenated strings;
    distill them into an md5 hash.

    """
    return md5(key.encode('ascii')).hexdigest()

# configure the "default" cache region.
regions['default'] = make_region(
            # the "dbm" backend needs
            # string-encoded keys
            key_mangler=md5_key_mangler
        ).configure(
        # using type 'file' to illustrate
        # serialized persistence.  Normally
        # memcached or similar is a better choice
        # for caching.
        'dogpile.cache.dbm',
        expiration_time=3600,
        arguments={
            "filename": os.path.join(root, "cache.dbm")
        }
    )

# optional; call invalidate() on the region
# once created so that all data is fresh when
# the app is restarted.  Good for development,
# on a production system needs to be used carefully
# regions['default'].invalidate()


installed = False

def bootstrap():
    global installed
    from . import fixture_data
    if not os.path.exists(dbfile):
        fixture_data.install()
        installed = True