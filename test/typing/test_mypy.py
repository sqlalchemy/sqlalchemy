import os

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
