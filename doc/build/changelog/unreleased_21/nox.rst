.. change::
    :tags: change, tests

    The top-level test runner has been changed to use ``nox``, adding a
    ``noxfile.py`` as well as some included modules.   The ``tox.ini`` file
    remains in place so that ``tox`` runs will continue to function in the near
    term, however it will be eventually removed and improvements and
    maintenance going forward will be only towards ``noxfile.py``.


