.. change:: cache_order_sequence
    :tags: feature, oracle, posgresql
    :versions: 1.2.0b1

    Added new keywords :paramref:`.Sequence.cache` and
    :paramref:`.Sequence.order` to :class:`.Sequence`, to allow rendering
    of the CACHE parameter understood by Oracle and PostgreSQL, and the
    ORDER parameter understood by Oracle.  Pull request
    courtesy David Moore.

