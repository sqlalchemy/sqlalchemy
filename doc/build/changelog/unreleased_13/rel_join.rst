.. change::
    :tags: performance, orm

    Identified a performance issue in the system by which a join is constructed
    based on a mapped relationship.   The clause adaption system would be used
    for the majority of join expressions including in the common case where no
    adaptation is needed.   The conditions under which this adaptation occur
    have been refined so that average non-aliased joins along a simple
    relationship without a "secondary" table use about 70% less function calls.

