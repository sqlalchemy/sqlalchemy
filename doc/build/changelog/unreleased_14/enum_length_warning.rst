.. change::
    :tags: bug, sql

    The :class:`_sqltypes.Enum` datatype now emits a warning if the
    :paramref:`_sqltypes.Enum.length` argument is specified without also
    specifying :paramref:`_sqltypes.Enum.native_enum` as False, as the
    parameter is otherwise silently ignored in this case, despite the fact that
    the :class:`_sqltypes.Enum` datatype will still render VARCHAR DDL on
    backends that don't have a native ENUM datatype such as SQLite. This
    behavior may change in a future release so that "length" is honored for all
    non-native "enum" types regardless of the "native_enum" setting.

