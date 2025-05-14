.. change::
    :tags: bug, installation

    Removed the "license classifier" from setup.cfg for SQLAlchemy 2.0, which
    eliminates loud deprecation warnings when building the package.  SQLAlchemy
    2.1 will use a full :pep:`639` configuration in pyproject.toml while
    SQLAlchemy 2.0 remains using ``setup.cfg`` for setup.


