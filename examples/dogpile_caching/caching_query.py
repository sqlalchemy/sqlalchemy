"""Represent functions and classes
which allow the usage of Dogpile caching with SQLAlchemy.
Introduces a query option called FromCache.

.. versionchanged:: 1.4  the caching approach has been altered to work
   based on a session event.


The three new concepts introduced here are:

 * ORMCache - an extension for an ORM :class:`.Session`
   retrieves results in/from dogpile.cache.
 * FromCache - a query option that establishes caching
   parameters on a Query
 * RelationshipCache - a variant of FromCache which is specific
   to a query invoked during a lazy load.

The rest of what's here are standard SQLAlchemy and
dogpile.cache constructs.

"""
from dogpile.cache.api import NO_VALUE

from sqlalchemy import event
from sqlalchemy.orm import loading
from sqlalchemy.orm.interfaces import UserDefinedOption


class ORMCache(object):

    """An add-on for an ORM :class:`.Session` optionally loads full results
    from a dogpile cache region.


    """

    def __init__(self, regions):
        self.cache_regions = regions
        self._statement_cache = {}

    def listen_on_session(self, session_factory):
        event.listen(session_factory, "do_orm_execute", self._do_orm_execute)

    def _do_orm_execute(self, orm_context):

        for opt in orm_context.user_defined_options:
            if isinstance(opt, RelationshipCache):
                opt = opt._process_orm_context(orm_context)
                if opt is None:
                    continue

            if isinstance(opt, FromCache):
                dogpile_region = self.cache_regions[opt.region]

                our_cache_key = opt._generate_cache_key(
                    orm_context.statement, orm_context.parameters, self
                )

                if opt.ignore_expiration:
                    cached_value = dogpile_region.get(
                        our_cache_key,
                        expiration_time=opt.expiration_time,
                        ignore_expiration=opt.ignore_expiration,
                    )
                else:

                    def createfunc():
                        return orm_context.invoke_statement().freeze()

                    cached_value = dogpile_region.get_or_create(
                        our_cache_key,
                        createfunc,
                        expiration_time=opt.expiration_time,
                    )

                if cached_value is NO_VALUE:
                    # keyerror?   this is bigger than a keyerror...
                    raise KeyError()

                orm_result = loading.merge_frozen_result(
                    orm_context.session,
                    orm_context.statement,
                    cached_value,
                    load=False,
                )
                return orm_result()

        else:
            return None

    def invalidate(self, statement, parameters, opt):
        """Invalidate the cache value represented by a statement."""

        statement = statement.__clause_element__()

        dogpile_region = self.cache_regions[opt.region]

        cache_key = opt._generate_cache_key(statement, parameters, self)

        dogpile_region.delete(cache_key)


class FromCache(UserDefinedOption):
    """Specifies that a Query should load results from a cache."""

    propagate_to_loaders = False

    def __init__(
        self,
        region="default",
        cache_key=None,
        expiration_time=None,
        ignore_expiration=False,
    ):
        """Construct a new FromCache.

        :param region: the cache region.  Should be a
         region configured in the dictionary of dogpile
         regions.

        :param cache_key: optional.  A string cache key
         that will serve as the key to the query.   Use this
         if your query has a huge amount of parameters (such
         as when using in_()) which correspond more simply to
         some other identifier.

        """
        self.region = region
        self.cache_key = cache_key
        self.expiration_time = expiration_time
        self.ignore_expiration = ignore_expiration

    def _gen_cache_key(self, anon_map, bindparams):
        return None

    def _generate_cache_key(self, statement, parameters, orm_cache):
        statement_cache_key = statement._generate_cache_key()

        key = statement_cache_key.to_offline_string(
            orm_cache._statement_cache, statement, parameters
        ) + repr(self.cache_key)

        # print("here's our key...%s" % key)
        return key


class RelationshipCache(FromCache):
    """Specifies that a Query as called within a "lazy load"
    should load results from a cache."""

    propagate_to_loaders = True

    def __init__(
        self,
        attribute,
        region="default",
        cache_key=None,
        expiration_time=None,
        ignore_expiration=False,
    ):
        """Construct a new RelationshipCache.

        :param attribute: A Class.attribute which
         indicates a particular class relationship() whose
         lazy loader should be pulled from the cache.

        :param region: name of the cache region.

        :param cache_key: optional.  A string cache key
         that will serve as the key to the query, bypassing
         the usual means of forming a key from the Query itself.

        """
        self.region = region
        self.cache_key = cache_key
        self.expiration_time = expiration_time
        self.ignore_expiration = ignore_expiration
        self._relationship_options = {
            (attribute.property.parent.class_, attribute.property.key): self
        }

    def _process_orm_context(self, orm_context):
        current_path = orm_context.loader_strategy_path

        if current_path:
            mapper, prop = current_path[-2:]
            key = prop.key

            for cls in mapper.class_.__mro__:
                if (cls, key) in self._relationship_options:
                    relationship_option = self._relationship_options[
                        (cls, key)
                    ]
                    return relationship_option

    def and_(self, option):
        """Chain another RelationshipCache option to this one.

        While many RelationshipCache objects can be specified on a single
        Query separately, chaining them together allows for a more efficient
        lookup during load.

        """
        self._relationship_options.update(option._relationship_options)
        return self
