.. change::
    :tags: bug, mysql

    Fixed percent-sign doubling in mysql-connector-python dialect, which does
    not require de-doubling of percent signs.   Additionally, the  mysql-
    connector-python driver is inconsistent in how it passes the column names
    in cursor.description, so a workaround decoder has been added to
    conditionally decode these randomly-sometimes-bytes values to unicode only
    if needed.  Also improved test support for mysql-connector-python, however
    it should be noted that this driver still has issues with unicode that
    continue to be unresolved as of yet.

