.. change::
    :tags: bug, sql

    Fixed issue in lambda caching system where an element of a query that
    produces no cache key, like a custom option or clause element, would still
    populate the expression in the "lambda cache" inappropriately.
