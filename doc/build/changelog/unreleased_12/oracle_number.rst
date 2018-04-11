.. change::
    :tags: bug, oracle
    :versions: 1.3.0b1

    The Oracle NUMBER datatype is reflected as INTEGER if the precision is NULL
    and the scale is zero, as this is how INTEGER values come back when
    reflected from Oracle's tables.  Pull request courtesy Kent Bower.
