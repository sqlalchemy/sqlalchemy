"""caching_query.py

Represent persistence structures which allow the usage of
Beaker caching with SQLAlchemy.

The three new concepts introduced here are:

 * CachingQuery - a Query subclass that caches and
   retrieves results in/from Beaker.
 * FromCache - a query option that establishes caching
   parameters on a Query
 * RelationshipCache - a variant of FromCache which is specific
   to a query invoked during a lazy load.
 * _params_from_query - extracts value parameters from 
   a Query.

The rest of what's here are standard SQLAlchemy and
Beaker constructs.

"""
from sqlalchemy.orm.interfaces import MapperOption
from sqlalchemy.orm.query import Query
from sqlalchemy.sql import visitors

class CachingQuery(Query):
    """A Query subclass which optionally loads full results from a Beaker 
    cache region.

    The CachingQuery stores additional state that allows it to consult
    a Beaker cache before accessing the database:

    * A "region", which is a cache region argument passed to a 
      Beaker CacheManager, specifies a particular cache configuration
      (including backend implementation, expiration times, etc.)
    * A "namespace", which is a qualifying name that identifies a
      group of keys within the cache.  A query that filters on a name 
      might use the name "by_name", a query that filters on a date range 
      to a joined table might use the name "related_date_range".

    When the above state is present, a Beaker cache is retrieved.

    The "namespace" name is first concatenated with 
    a string composed of the individual entities and columns the Query 
    requests, i.e. such as ``Query(User.id, User.name)``.

    The Beaker cache is then loaded from the cache manager based
    on the region and composed namespace.  The key within the cache
    itself is then constructed against the bind parameters specified
    by this query, which are usually literals defined in the 
    WHERE clause.

    The FromCache and RelationshipCache mapper options below represent
    the "public" method of configuring this state upon the CachingQuery.

    """

    def __init__(self, manager, *args, **kw):
        self.cache_manager = manager
        Query.__init__(self, *args, **kw)

    def __iter__(self):
        """override __iter__ to pull results from Beaker
           if particular attributes have been configured.

           Note that this approach does *not* detach the loaded objects from
           the current session. If the cache backend is an in-process cache
           (like "memory") and lives beyond the scope of the current session's
           transaction, those objects may be expired. The method here can be
           modified to first expunge() each loaded item from the current
           session before returning the list of items, so that the items
           in the cache are not the same ones in the current Session.

        """
        if hasattr(self, '_cache_parameters'):
            return self.get_value(createfunc=lambda: list(Query.__iter__(self)))
        else:
            return Query.__iter__(self)

    def invalidate(self):
        """Invalidate the value represented by this Query."""

        cache, cache_key = _get_cache_parameters(self)
        cache.remove(cache_key)

    def get_value(self, merge=True, createfunc=None):
        """Return the value from the cache for this query.

        Raise KeyError if no value present and no
        createfunc specified.

        """
        cache, cache_key = _get_cache_parameters(self)
        ret = cache.get_value(cache_key, createfunc=createfunc)
        if merge:
            ret = self.merge_result(ret, load=False)
        return ret

    def set_value(self, value):
        """Set the value in the cache for this query."""

        cache, cache_key = _get_cache_parameters(self)
        cache.put(cache_key, value)

def query_callable(manager, query_cls=CachingQuery):
    def query(*arg, **kw):
        return query_cls(manager, *arg, **kw)
    return query

def _get_cache_parameters(query):
    """For a query with cache_region and cache_namespace configured,
    return the correspoinding Cache instance and cache key, based
    on this query's current criterion and parameter values.

    """
    if not hasattr(query, '_cache_parameters'):
        raise ValueError("This Query does not have caching parameters configured.")

    region, namespace, cache_key = query._cache_parameters

    namespace = _namespace_from_query(namespace, query)

    if cache_key is None:
        # cache key - the value arguments from this query's parameters.
        args = [str(x) for x in _params_from_query(query)]
        args.extend([str(query._limit), str(query._offset)])
        cache_key = " ".join(args)

    assert cache_key is not None, "Cache key was None !"

    # get cache
    cache = query.cache_manager.get_cache_region(namespace, region)

    # optional - hash the cache_key too for consistent length
    # import uuid
    # cache_key= str(uuid.uuid5(uuid.NAMESPACE_DNS, cache_key))

    return cache, cache_key

def _namespace_from_query(namespace, query):
    # cache namespace - the token handed in by the 
    # option + class we're querying against
    namespace = " ".join([namespace] + [str(x) for x in query._entities])

    # memcached wants this
    namespace = namespace.replace(' ', '_')

    return namespace

def _set_cache_parameters(query, region, namespace, cache_key):

    if hasattr(query, '_cache_parameters'):
        region, namespace, cache_key = query._cache_parameters
        raise ValueError("This query is already configured "
                        "for region %r namespace %r" % 
                        (region, namespace)
                    )
    query._cache_parameters = region, namespace, cache_key

class FromCache(MapperOption):
    """Specifies that a Query should load results from a cache."""

    propagate_to_loaders = False

    def __init__(self, region, namespace, cache_key=None):
        """Construct a new FromCache.

        :param region: the cache region.  Should be a
        region configured in the Beaker CacheManager.

        :param namespace: the cache namespace.  Should
        be a name uniquely describing the target Query's
        lexical structure.

        :param cache_key: optional.  A string cache key 
        that will serve as the key to the query.   Use this
        if your query has a huge amount of parameters (such
        as when using in_()) which correspond more simply to 
        some other identifier.

        """
        self.region = region
        self.namespace = namespace
        self.cache_key = cache_key

    def process_query(self, query):
        """Process a Query during normal loading operation."""

        _set_cache_parameters(query, self.region, self.namespace, self.cache_key)

class RelationshipCache(MapperOption):
    """Specifies that a Query as called within a "lazy load" 
       should load results from a cache."""

    propagate_to_loaders = True

    def __init__(self, region, namespace, attribute):
        """Construct a new RelationshipCache.

        :param region: the cache region.  Should be a
        region configured in the Beaker CacheManager.

        :param namespace: the cache namespace.  Should
        be a name uniquely describing the target Query's
        lexical structure.

        :param attribute: A Class.attribute which
        indicates a particular class relationship() whose
        lazy loader should be pulled from the cache.

        """
        self.region = region
        self.namespace = namespace
        self._relationship_options = {
            ( attribute.property.parent.class_, attribute.property.key ) : self
        }

    def process_query_conditionally(self, query):
        """Process a Query that is used within a lazy loader.

        (the process_query_conditionally() method is a SQLAlchemy
        hook invoked only within lazyload.)

        """
        if query._current_path:
            mapper, key = query._current_path[-2:]

            for cls in mapper.class_.__mro__:
                if (cls, key) in self._relationship_options:
                    relationship_option = self._relationship_options[(cls, key)]
                    _set_cache_parameters(
                            query, 
                            relationship_option.region, 
                            relationship_option.namespace, 
                            None)

    def and_(self, option):
        """Chain another RelationshipCache option to this one.

        While many RelationshipCache objects can be specified on a single
        Query separately, chaining them together allows for a more efficient
        lookup during load.

        """
        self._relationship_options.update(option._relationship_options)
        return self


def _params_from_query(query):
    """Pull the bind parameter values from a query.

    This takes into account any scalar attribute bindparam set up.

    E.g. params_from_query(query.filter(Cls.foo==5).filter(Cls.bar==7)))
    would return [5, 7].

    """
    v = []
    def visit_bindparam(bind):

        if bind.key in query._params:
            value = query._params[bind.key]
        elif bind.callable:
            # lazyloader may dig a callable in here, intended
            # to late-evaluate params after autoflush is called.
            # convert to a scalar value.
            value = bind.callable()
        else:
            value = bind.value

        v.append(value)
    if query._criterion is not None:
        visitors.traverse(query._criterion, {}, {'bindparam':visit_bindparam})
    for f in query._from_obj:
        visitors.traverse(f, {}, {'bindparam':visit_bindparam})
    return v
