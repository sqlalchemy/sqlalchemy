.. change::
    :tags: bug, examples

    The dogpile_caching examples have been updated for 2.0 style queries.
    Within the "caching query" logic itself there is one conditional added to
    differentiate between ``Query`` and ``select()`` when performing an
    invalidation operation.
