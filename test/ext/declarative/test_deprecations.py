import sqlalchemy as sa
from sqlalchemy.ext.declarative import instrument_declarative
from sqlalchemy.orm import Mapper
from sqlalchemy.testing import expect_deprecated_20
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_true


class TestInstrumentDeclarative(fixtures.TestBase):
    def test_ok(self):
        class Foo(object):
            __tablename__ = "foo"
            id = sa.Column(sa.Integer, primary_key=True)

        meta = sa.MetaData()
        reg = {}
        with expect_deprecated_20(
            "the instrument_declarative function is deprecated"
        ):
            instrument_declarative(Foo, reg, meta)

        mapper = sa.inspect(Foo)
        is_true(isinstance(mapper, Mapper))
        is_(mapper.class_, Foo)
