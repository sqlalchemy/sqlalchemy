"""meta.py

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
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.interfaces import MapperOption
from sqlalchemy.orm.query import Query
from sqlalchemy.sql import visitors
from sqlalchemy.ext.declarative import declarative_base
from beaker import cache

class CachingQuery(Query):
    """A Query subclass which optionally loads full results from a Beaker 
    cache region.
    
    The CachingQuery is instructed to load from cache based on two optional
    attributes configured on the instance, called 'cache_region' and 'cache_namespace'.
    
    When these attributes are present, any iteration of the Query will configure
    a Beaker cache against this region and a generated namespace, which takes
    into account the 'cache_namespace' name as well as the entities this query
    is created against (i.e. the columns and classes sent to the constructor).
    The 'cache_namespace' is a string name that represents a particular structure
    of query.  E.g. a query that filters on a name might use the name "by_name",
    a query that filters on a date range to a joined table might use the name
    "related_date_range".
    
    The Query then attempts to retrieved a cached value using a key, which
    is generated from all the parameterized values present in the Query.  In
    this way, the combination of "cache_namespace" and embedded parameter values
    correspond exactly to the lexical structure of a SQL statement combined
    with its bind parameters.   If no such key exists then the ultimate SQL
    is emitted and the objects loaded.
    
    The returned objects, if loaded from cache, are merged into the Query's
    session using Session.merge(load=False), which is a fast performing
    method to ensure state is present.

    The FromCache and RelationshipCache mapper options below represent
    the "public" method of 
    configuring the "cache_region" and "cache_namespace" attributes.
    RelationshipCache has the ability to be invoked upon lazy loaders embedded
    in an object graph.
    
    """
    
    def __iter__(self):
        """override __iter__ to pull results from Beaker
           if particular attributes have been configured.
        """
        if hasattr(self, '_cache_parameters'):
            cache, cache_key = _get_cache_parameters(self)
            ret = cache.get_value(cache_key, createfunc=lambda: list(Query.__iter__(self)))
            
            # merge the result in.  
            return self.merge_result(ret, load=False)
        else:
            return Query.__iter__(self)

    def invalidate(self):
        """Invalidate the cache represented in this Query."""

        cache, cache_key = _get_cache_parameters(self)
        cache.remove(cache_key)

    def set_value(self, value):
        """Set the value in the cache for this query."""

        cache, cache_key = _get_cache_parameters(self)
        cache.put(cache_key, value)        

def _get_cache_parameters(query):
    """For a query with cache_region and cache_namespace configured,
    return the correspoinding Cache instance and cache key, based
    on this query's current criterion and parameter values.

    """
    if not hasattr(query, '_cache_parameters'):
        raise ValueError("This Query does not have caching parameters configured.")

    region, namespace, cache_key = query._cache_parameters

    # cache namespace - the token handed in by the 
    # option + class we're querying against
    namespace = " ".join([namespace] + [str(x) for x in query._entities])

    # memcached wants this
    namespace = namespace.replace(' ', '_')

    if cache_key is None:
        # cache key - the value arguments from this query's parameters.
        args = _params_from_query(query)
        cache_key = " ".join([str(x) for x in args])

    # get cache
    cache = cache_manager.get_cache_region(namespace, region)

    # optional - hash the cache_key too for consistent length
    # import uuid
    # cache_key= str(uuid.uuid5(uuid.NAMESPACE_DNS, cache_key))

    return cache, cache_key

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
        value = query._params.get(bind.key, bind.value)
        
        # lazyloader may dig a callable in here, intended
        # to late-evaluate params after autoflush is called.
        # convert to a scalar value.
        if callable(value):
            value = value()
            
        v.append(value)
    if query._criterion is not None:
        visitors.traverse(query._criterion, {}, {'bindparam':visit_bindparam})
    return v

# Beaker CacheManager.  A home base for cache configurations.
# Configured at startup in __init__.py
cache_manager = cache.CacheManager()

# global application session.
# configured at startup in __init__.py
Session = scoped_session(sessionmaker(query_cls=CachingQuery))

# global declarative base class.
Base = declarative_base()

