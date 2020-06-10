.. change::
    :tags: change, engine

    Removed the concept of a bound engine from the :class:`.Compiler` object,
    and removed the ``.execute()`` and ``.scalar()`` methods from :class:`.Compiler`.
    These were essentially forgotten methods from over a decade ago and had no
    practical use, and it's not appropriate for the :class:`.Compiler` object
    itself to be maintaining a reference to an :class:`.Engine`.
