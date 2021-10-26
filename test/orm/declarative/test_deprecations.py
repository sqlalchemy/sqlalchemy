from sqlalchemy import Integer
from sqlalchemy import testing
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import registry
from sqlalchemy.orm import Session
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing.schema import Column


class BoundMetadataDeclarativeTest(fixtures.MappedTest):
    def test_bound_declarative_base(self):
        with testing.expect_deprecated(
            "The ``bind`` argument to declarative_base"
        ):
            Base = declarative_base(testing.db)

        class User(Base):
            __tablename__ = "user"
            id = Column(Integer, primary_key=True)

        s = Session()

        with testing.expect_deprecated_20(
            "This Session located a target engine via bound metadata"
        ):
            is_(s.get_bind(User), testing.db)

    def test_bound_cls_registry_base(self):
        reg = registry(_bind=testing.db)

        Base = reg.generate_base()

        class User(Base):
            __tablename__ = "user"
            id = Column(Integer, primary_key=True)

        s = Session()
        with testing.expect_deprecated_20(
            "This Session located a target engine via bound metadata"
        ):
            is_(s.get_bind(User), testing.db)

    def test_bound_cls_registry_decorated(self):
        reg = registry(_bind=testing.db)

        @reg.mapped
        class User(object):
            __tablename__ = "user"
            id = Column(Integer, primary_key=True)

        s = Session()

        with testing.expect_deprecated_20(
            "This Session located a target engine via bound metadata"
        ):
            is_(s.get_bind(User), testing.db)
