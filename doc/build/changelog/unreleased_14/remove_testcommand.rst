.. change::
    :tags: change, general

    The setuptools "test" command is removed from the 1.4 series as modern
    versions of setuptools actively refuse to accommodate this extension being
    present.   This change was already part of the 2.0 series.   To run the
    test suite use the ``tox`` command.
