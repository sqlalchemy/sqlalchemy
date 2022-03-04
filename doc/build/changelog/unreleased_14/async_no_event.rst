.. change::
    :tags: bug, asyncio

    Fixed issues where a descriptive error message was not raised for some
    classes of event listening with an async engine, which should instead be a
    sync engine instance.