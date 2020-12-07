.. change::
    :tags: bug, general

    Fixed a SQLite source file that had non-ascii characters inside of its
    docstring without a source encoding, introduced within the "INSERT..ON
    CONFLICT" feature, which would cause failures under Python 2.
