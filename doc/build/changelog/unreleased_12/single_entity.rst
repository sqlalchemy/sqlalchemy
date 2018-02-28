.. change::
    :tags: feature, orm
    :versions: 1.3.0b1

    Added new feature :meth:`.Query.only_return_tuples`.  Causes the
    :class:`.Query` object to return keyed tuple objects unconditionally even
    if the query is against a single entity.   Pull request courtesy Eric
    Atkin.

