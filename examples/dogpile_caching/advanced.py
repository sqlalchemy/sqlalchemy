"""Illustrate usage of Query combined with the FromCache option,
including front-end loading, cache invalidation and collection caching.

"""

from .caching_query import FromCache
from .caching_query import RelationshipCache
from .environment import cache
from .environment import Session
from .model import cache_address_bits
from .model import Person


def load_name_range(start, end, invalidate=False):
    """Load Person objects on a range of names.

    start/end are integers, range is then
    "person <start>" - "person <end>".

    The cache option we set up is called "name_range", indicating
    a range of names for the Person class.

    The `Person.addresses` collections are also cached.  Its basically
    another level of tuning here, as that particular cache option
    can be transparently replaced with joinedload(Person.addresses).
    The effect is that each Person and their Address collection
    is cached either together or separately, affecting the kind of
    SQL that emits for unloaded Person objects as well as the distribution
    of data within the cache.
    """
    q = (
        Session.query(Person)
        .filter(
            Person.name.between("person %.2d" % start, "person %.2d" % end)
        )
        .options(cache_address_bits)
        .options(FromCache("default", "name_range"))
    )

    # have the "addresses" collection cached separately
    # each lazyload of Person.addresses loads from cache.
    q = q.options(RelationshipCache(Person.addresses, "default"))

    # alternatively, eagerly load the "addresses" collection, so that they'd
    # be cached together.   This issues a bigger SQL statement and caches
    # a single, larger value in the cache per person rather than two
    # separate ones.
    # q = q.options(joinedload(Person.addresses))

    # if requested, invalidate the cache on current criterion.
    if invalidate:
        cache.invalidate(q, {}, FromCache("default", "name_range"))
        cache.invalidate(q, {}, RelationshipCache(Person.addresses, "default"))

    return q.all()


print("two through twelve, possibly from cache:\n")
print(", ".join([p.name for p in load_name_range(2, 12)]))

print("\ntwenty five through forty, possibly from cache:\n")
print(", ".join([p.name for p in load_name_range(25, 40)]))

# loading them again, no SQL is emitted
print("\ntwo through twelve, from the cache:\n")
print(", ".join([p.name for p in load_name_range(2, 12)]))

# but with invalidate, they are
print("\ntwenty five through forty, invalidate first:\n")
print(", ".join([p.name for p in load_name_range(25, 40, True)]))

# illustrate the address loading from either cache/already
# on the Person
print(
    "\n\nPeople plus addresses, two through twelve, addresses "
    "possibly from cache"
)
for p in load_name_range(2, 12):
    print(p.format_full())

# illustrate the address loading from either cache/already
# on the Person
print("\n\nPeople plus addresses, two through twelve, addresses from cache")
for p in load_name_range(2, 12):
    print(p.format_full())

print(
    "\n\nIf this was the first run of advanced.py, try "
    "a second run.  Only one SQL statement will be emitted."
)
