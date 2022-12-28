.. change::
    :tags: bug, tests
    :versions: 2.0.0rc1

    Fixed issue in tox.ini file where changes in the tox 4.0 series to the
    format of "passenv" caused tox to not function correctly, in particular
    raising an error as of tox 4.0.6.
