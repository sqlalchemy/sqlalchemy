.. change::
    :tags: bug, examples

    Fixed the "short_selects" performance example where the cache was being
    used in all the examples, making it impossible to compare performance with
    and without the cache.   Less important comparisons like "lambdas" and
    "baked queries" have been removed.

