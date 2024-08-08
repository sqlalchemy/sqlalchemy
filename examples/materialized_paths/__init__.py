"""Illustrates the "materialized paths" pattern for hierarchical data using the
SQLAlchemy ORM.

.. autosource::

"""


from .._utils import DirectoryExamples


REQUIREMENTS: DirectoryExamples = {
    "materialized_paths.py": {
        "postgresql": True,
        "pypi": [
            "psycopg2",
        ],
    },
}
