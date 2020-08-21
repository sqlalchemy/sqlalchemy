.. change::
    :tags: bug, mysql

    The MySQL and MariaDB dialects now query from the information_schema.tables
    system view in order to determine if a particular table exists or not.
    Previously, the "DESCRIBE" command was used with an exception catch to
    detect non-existent,  which would have the undesirable effect of emitting a
    ROLLBACK on the connection. There appeared to be legacy encoding issues
    which prevented the use of "SHOW TABLES", for this, but as MySQL support is
    now at 5.0.2  or above due to :ticket:`4189`, the information_schema tables
    are now available in all cases.

