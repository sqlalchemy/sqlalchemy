.. change::
    :tags: bug, tests

    Implemented support for the test suite to run correctly under Pytest 7.
    Previously, only Pytest 6.x was supported for Python 3, however the version
    was not pinned on the upper bound in tox.ini. Pytest is not pinned in
    tox.ini to be lower than version 8 so that SQLAlchemy versions released
    with the current codebase will be able to be tested under tox without
    changes to the environment.   Much thanks to the Pytest developers for
    their help with this issue.

