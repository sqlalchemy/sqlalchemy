.. change::
    :tags: bug, orm, regression
    :tickets: 8133

    Fixed regression caused by :ticket:`8133` where the pickle format for
    mutable attributes was changed, without a fallback to recognize the old
    format, causing in-place upgrades of SQLAlchemy to no longer be able to
    read pickled data from previous versions. A check plus a fallback for the
    old format is now in place.
