If an Example requires a specific database (not sqlite), additional PyPI
packages, or actions such as cleanup instructions, please ensure the
`__init__.py` file is updated to contain a
`REQUIREMENTS` variable to reflect this.

The metadata describing the requirements should be in the form of a structured
TypedDict:

    REQUIREMENTS: _utils.DirectoryExamples = {}
    
For example:

    from .._utils import DirectoryExamples

    REQUIREMENTS: DirectoryExamples = {
        "asyncio_.py": {
            "pypi": [
                "aiosqlite",
            ],
        },
    }    
