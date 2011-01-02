"""local_session_caching.py

Create a new Beaker cache type + a local region that will store
cached data local to the current Session.

This is an advanced example which assumes familiarity
with the basic operation of CachingQuery.

"""

from beaker import cache, container
import collections

class ScopedSessionNamespace(container.MemoryNamespaceManager):
    """A Beaker cache type which will cache objects locally on 
    the current session.

    When used with the query_cache system, the effect is that the objects
    in the cache are the same as that within the session - the merge()
    is a formality that doesn't actually create a second instance.
    This makes it safe to use for updates of data from an identity
    perspective (still not ideal for deletes though).

    When the session is removed, the cache is gone too, so the cache
    is automatically disposed upon session.remove().

    """

    def __init__(self, namespace, scoped_session, **kwargs):
        """__init__ is called by Beaker itself."""

        container.NamespaceManager.__init__(self, namespace)
        self.scoped_session = scoped_session

    @classmethod
    def create_session_container(cls, beaker_name, scoped_session):
        """Create a new session container for a given scoped_session."""

        def create_namespace(namespace, **kw):
            return cls(namespace, scoped_session, **kw)
        cache.clsmap[beaker_name] = create_namespace

    @property
    def dictionary(self):
        """Return the cache dictionary used by this MemoryNamespaceManager."""

        sess = self.scoped_session()
        try:
            nscache = sess._beaker_cache
        except AttributeError:
            sess._beaker_cache = nscache = collections.defaultdict(dict)
        return nscache[self.namespace]


if __name__ == '__main__':
    from environment import Session, cache_manager
    from caching_query import FromCache

    # create a Beaker container type called "ext:local_session".
    # it will reference the ScopedSession in meta.
    ScopedSessionNamespace.create_session_container("ext:local_session", Session)

    # set up a region based on this new container type.
    cache_manager.regions['local_session'] ={'type':'ext:local_session'}

    from model import Person

    # query to load Person by name, with criterion
    # of "person 10"
    q = Session.query(Person).\
                    options(FromCache("local_session", "by_name")).\
                    filter(Person.name=="person 10")

    # load from DB
    person10 = q.one()

    # next call, the query is cached.
    person10 = q.one()

    # clear out the Session.  The "_beaker_cache" dictionary
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
    from caching_query import _get_cache_parameters
    cache, key = _get_cache_parameters(q)
    assert person10 is cache.get(key)[0]
