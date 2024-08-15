from ._utils import DirectoryExamples


REQUIREMENTS: DirectoryExamples = {
    "space_invaders.py": {
        "persistent_subprocess": True,
        "cleanup_files": [
            "space_invaders.log",
        ],
        "stdout_required": "Press any key to start",
    },
}
