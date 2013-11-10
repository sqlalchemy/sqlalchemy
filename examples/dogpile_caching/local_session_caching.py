"""local_session_caching.py

Grok everything so far ?   This example
creates a new dogpile.cache backend that will persist data in a dictionary
which is local to the current session.   remove() the session
and the cache is gone.

Create a new Dogpile cache backend that will store
cached data local to the current Session.

This is an advanced example which assumes familiarity
with the basic operation of CachingQuery.

"""

from dogpile.cache.api import CacheBackend, NO_VALUE
from dogpile.cache.region import register_backend

class ScopedSessionBackend(CacheBackend):
    """A dogpile backend which will cache objects locally on
    the current session.

    When used with the query_cache system, the effect is that the objects
    in the cache are the same as that within the session - the merge()
    is a formality that doesn't actually create a second instance.
    This makes it safe to use for updates of data from an identity
    perspective (still not ideal for deletes though).

    When the session is removed, the cache is gone too, so the cache
    is automatically disposed upon session.remove().

    """

    def __init__(self, arguments):
        self.scoped_session = arguments['scoped_session']

    def get(self, key):
        return self._cache_dictionary.get(key, NO_VALUE)

    def set(self, key, value):
        self._cache_dictionary[key] = value

    def delete(self, key):
        self._cache_dictionary.pop(key, None)

    @property
    def _cache_dictionary(self):
        """Return the cache dictionary linked to the current Session."""

        sess = self.scoped_session()
        try:
            cache_dict = sess._cache_dictionary
        except AttributeError:
            sess._cache_dictionary = cache_dict = {}
        return cache_dict

register_backend("sqlalchemy.session", __name__, "ScopedSessionBackend")


if __name__ == '__main__':
    from .environment import Session, regions
    from .caching_query import FromCache
    from dogpile.cache import make_region

    # set up a region based on the ScopedSessionBackend,
    # pointing to the scoped_session declared in the example
    # environment.
    regions['local_session'] = make_region().configure(
        'sqlalchemy.session',
        arguments={
            "scoped_session": Session
        }
    )

    from .model import Person

    # query to load Person by name, with criterion
    # of "person 10"
    q = Session.query(Person).\
                    options(FromCache("local_session")).\
                    filter(Person.name == "person 10")

    # load from DB
    person10 = q.one()

    # next call, the query is cached.
    person10 = q.one()

    # clear out the Session.  The "_cache_dictionary" dictionary
    # disappears with it.
    Session.remove()

    # query calls from DB again
    person10 = q.one()

    # identity is preserved - person10 is the *same* object that's
    # ultimately inside the cache.   So it is safe to manipulate
    # the not-queried-for attributes of objects when using such a
    # cache without the need to invalidate - however, any change
    # that would change the results of a cached query, such as
    # inserts, deletes, or modification to attributes that are
    # part of query criterion, still require careful invalidation.
    cache, key = q._get_cache_plus_key()
    assert person10 is cache.get(key)[0]
