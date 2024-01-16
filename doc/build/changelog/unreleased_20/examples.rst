.. change::
    :tags: bug, examples

    Fixed the performance example scripts in examples/performance to mostly
    work with the Oracle database, by adding the :class:`.Identity` construct
    to all the tables and allowing primary generation to occur on this backend.
    A few of the "raw DBAPI" cases still are not compatible with Oracle.

