.. change::
    :tags: feature, sql

    The Python builtin ``dir()`` is now supported for a SQLAlchemy "properties"
    object, such as that of a Core columns collection (e.g. ``.c``),
    ``mapper.attrs``, etc.  Allows iPython autocompletion to work as well.
    Pull request courtesy Uwe Korn.
