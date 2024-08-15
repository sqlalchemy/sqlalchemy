from typing_extensions import TypedDict

ExampleMetadata = TypedDict(
    "ExampleMetadata",
    {
        "pypi": list[str],  # list of pypi packages
        "postgresql": bool,  # does this require a postgresql database?
        "executable": bool,  # is this file executable? specify `False` if not
        "repeat_run": bool,  # if True, run twice in a row
        "cleanup_dir": str,  # if defined, delete this directory
        "cleanup_files": list[str],  # if defined, delete these files
        "persistent_subprocess": bool,  # if true, test as a subprocess
        "stdout_required": str,  # if supplied, tests require this in stdout
    },
    total=False,
)

DirectoryExamples = dict[str, ExampleMetadata]
