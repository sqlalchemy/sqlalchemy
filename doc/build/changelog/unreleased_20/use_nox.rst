.. change::
    :tags: change, tests

    A noxfile.py has been added to allow testing with nox.  This is a direct
    port of 2.1's move to nox, however leaves the tox.ini file in place and
    retains all test documentation in terms of tox.   Version 2.1 will move to
    nox fully, including deprecation warnings for tox and new testing
    documentation.
