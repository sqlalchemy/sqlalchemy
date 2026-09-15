.. change::
    :tags: change, installation

    Binary wheels are no longer built for Python 3.7.  PyPI now rejects wheel
    files whose filename does not begin with the normalized project name, and
    the packaging tools that can be installed on Python 3.7 do not produce
    such a filename.  As a result, SQLAlchemy 2.0.44 was the last release to
    publish Python 3.7 wheels to PyPI, and releases 2.0.45 and later have
    been available on Python 3.7 only as a source distribution; the wheel
    builds for Python 3.7 are now removed.  Python 3.7 remains supported by
    the 2.0 series.
