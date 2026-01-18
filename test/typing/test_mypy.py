import os
from pathlib import Path

from sqlalchemy import testing
from sqlalchemy.testing import fixtures


class MypyPlainTest(fixtures.MypyTest):
    @testing.combinations(
        *(
            (os.path.basename(path), path)
            for path in fixtures.MypyTest.file_combinations("plain_files")
        ),
        argnames="path",
        id_="ia",
    )
    def test_mypy_no_plugin(self, mypy_typecheck_file, path):
        mypy_typecheck_file(path)


class MypyExamplesTest(fixtures.MypyTest):
    """Test that examples pass mypy strict mode."""

    # Path to examples/generic_associations relative to repo root
    _examples_path = Path(__file__).parent.parent.parent / "examples"

    @testing.combinations(
        *(
            (path.name, str(path))
            for path in (_examples_path / "generic_associations").glob("*.py")
            if path.name != "__init__.py"
        ),
        argnames="path",
        id_="ia",
    )
    def test_generic_associations_examples(self, mypy_typecheck_file, path):
        mypy_typecheck_file(path)
