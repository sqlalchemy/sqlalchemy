.. change::
    :tags: bug, tests

    Adjusted the test suite which tests the Mypy plugin to accommodate for
    changes in Mypy 0.990 regarding how it handles message output, which affect
    how sys.path is interpreted when determining if notes and errors should be
    printed for particular files. The change broke the test suite as the files
    within the test directory itself no longer produced messaging when run
    under the mypy API.
