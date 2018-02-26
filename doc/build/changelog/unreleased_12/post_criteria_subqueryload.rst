.. change::
    :tags: bug, orm

    Fixed bug where the new :meth:`.baked.Result.with_post_criteria`
    method would not interact with a subquery-eager loader correctly,
    in that the "post criteria" would not be applied to embedded
    subquery eager loaders.   This is related to :ticket:`4128` in that
    the post criteria feature is now used by the lazy loader.
