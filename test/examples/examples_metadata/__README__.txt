If an Example requires a specific database (not sqlite), additional PyPI
packages, or actions such as cleanup instructions, please ensure there is a
corresponding `{{DIRECTORY_NAME}}.py` file in this directory, and that file 
contains a `REQUIREMENTS` variable to reflect the metadata.

The metadata describing the requirements should be in the form of a structured
TypedDict:

    REQUIREMENTS: _utils.DirectoryExamples = {}
    
For example:

    from ._utils import DirectoryExamples

    REQUIREMENTS: DirectoryExamples = {
        "asyncio_.py": {
            "pypi": [
                "aiosqlite",
            ],
        },
    }    
