.. change::
    :tags: bug, mssql

    Fixed issue where a multipart schema name for SQL Server that contained
    bracketed database tokens along with an unbracketed token, such as
    ``"[abc].def.[efg].hij"``, would render the unbracketed token without
    quoting, both in compiled statements as well as in the ``USE`` statement
    emitted during reflection.  Such tokens are now bracketed, so that schema
    names passed via ``schema_translate_map`` or similar can't introduce
    arbitrary SQL into the statement.
