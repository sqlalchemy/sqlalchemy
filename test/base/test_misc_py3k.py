from sqlalchemy import Column
from sqlalchemy.testing import fixtures


class TestGenerics(fixtures.TestBase):
    def test_traversible_is_generic(self):
        col = Column[int]
        assert col is Column
