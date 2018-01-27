.. change::
    :tags: bug, sqlite

    Fixed the import error raised when a platform
    has neither pysqlite2 nor sqlite3 installed, such
    that the sqlite3-related import error is raised,
    not the pysqlite2 one which is not the actual
    failure mode.  Pull request courtesy Robin.
