from ._utils import DirectoryExamples


REQUIREMENTS: DirectoryExamples = {
    "asyncio_.py": {
        "pypi": [
            "aiosqlite",
        ],
    },
    "separate_schema_translates.py": {
        "cleanup_files": [
            "schema_1.db",
            "schema_2.db",
            "schema_3.db",
            "schema_4.db",
        ],
    },
}
