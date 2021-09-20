from sqlalchemy import Column
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import requires


class TestGenerics(fixtures.TestBase):
    @requires.generic_classes
    def test_traversible_is_generic(self):
        col = Column[int]
        assert col is Column
