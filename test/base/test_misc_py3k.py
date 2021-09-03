from sqlalchemy import Column
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import requires


class TestGenerics(fixtures.TestBase):
    @requires.builtin_generics
    def test_traversible_is_generic(self):
        col = Column[int]
        assert col is Column
