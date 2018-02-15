.. change::
    :tags: bug, postgresql
    :versions: 1.2.3

    Added "SSL SYSCALL error: Operation timed out" to the list
    of messages that trigger a "disconnect" scenario for the
    psycopg2 driver.  Pull request courtesy André Cruz.
