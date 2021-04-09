.. change::
    :tags: change, tests

    Added a new flag to :class:`.DefaultDialect` called ``supports_schema``;
    third party dialects may set this flag to ``True`` to enable SQLAlchemy's
    schema-level tests when running the test suite for a third party dialect.
