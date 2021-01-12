.. change::
    :tags: changed, postgresql

    Fixed issue where the psycopg2 dialect would silently pass the
    ``use_native_unicode=False`` flag without actually having any effect under
    Python 3, as the psycopg2 DBAPI uses Unicode unconditionally under Python
    3.  This usage now raises an :class:`_exc.ArgumentError` when used under
    Python 3. Added test support for Python 2.