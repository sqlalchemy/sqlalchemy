"""Unit tests illustrating usage of the ``history_meta.py``
module functions."""

import unittest
import warnings

from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Index
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import join
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import column_property
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import deferred
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import eq_ignore_whitespace
from sqlalchemy.testing import is_
from sqlalchemy.testing import ne_
from sqlalchemy.testing.entities import ComparableEntity
from .history_meta import Versioned
from .history_meta import versioned_session

warnings.simplefilter("error")


class TestVersioning(AssertsCompiledSQL):
    __dialect__ = "default"

    def setUp(self):
        self.engine = engine = create_engine("sqlite://")
        self.session = Session(engine)
        self.make_base()
        versioned_session(self.session)

    def tearDown(self):
        self.session.close()
        clear_mappers()
        self.Base.metadata.drop_all(self.engine)

    def make_base(self):
        self.Base = declarative_base()

    def create_tables(self):
        self.Base.metadata.create_all(self.engine)

    def test_plain(self):
        class SomeClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "sometable"

            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        self.create_tables()
        sess = self.session
        sc = SomeClass(name="sc1")
        sess.add(sc)
        sess.commit()

        sc.name = "sc1modified"
        sess.commit()

        assert sc.version == 2

        SomeClassHistory = SomeClass.__history_mapper__.class_

        eq_(
            sess.query(SomeClassHistory)
            .filter(SomeClassHistory.version == 1)
            .all(),
            [SomeClassHistory(version=1, name="sc1")],
        )

        sc.name = "sc1modified2"

        eq_(
            sess.query(SomeClassHistory)
            .order_by(SomeClassHistory.version)
            .all(),
            [
                SomeClassHistory(version=1, name="sc1"),
                SomeClassHistory(version=2, name="sc1modified"),
            ],
        )

        assert sc.version == 3

        sess.commit()

        sc.name = "temp"
        sc.name = "sc1modified2"

        sess.commit()

        eq_(
            sess.query(SomeClassHistory)
            .order_by(SomeClassHistory.version)
            .all(),
            [
                SomeClassHistory(version=1, name="sc1"),
                SomeClassHistory(version=2, name="sc1modified"),
            ],
        )

        sess.delete(sc)
        sess.commit()

        eq_(
            sess.query(SomeClassHistory)
            .order_by(SomeClassHistory.version)
            .all(),
            [
                SomeClassHistory(version=1, name="sc1"),
                SomeClassHistory(version=2, name="sc1modified"),
                SomeClassHistory(version=3, name="sc1modified2"),
            ],
        )

    @testing.variation(
        "constraint_type",
        [
            "index_single_col",
            "composite_index",
            "explicit_name_index",
            "unique_constraint",
            "unique_constraint_naming_conv",
            "unique_constraint_explicit_name",
            "fk_constraint",
            "fk_constraint_naming_conv",
            "fk_constraint_explicit_name",
        ],
    )
    def test_index_naming(self, constraint_type):
        """test #10920"""

        if (
            constraint_type.unique_constraint_naming_conv
            or constraint_type.fk_constraint_naming_conv
        ):
            self.Base.metadata.naming_convention = {
                "ix": "ix_%(column_0_label)s",
                "uq": "uq_%(table_name)s_%(column_0_name)s",
                "fk": (
                    "fk_%(table_name)s_%(column_0_name)s"
                    "_%(referred_table_name)s"
                ),
            }

        if (
            constraint_type.fk_constraint
            or constraint_type.fk_constraint_naming_conv
            or constraint_type.fk_constraint_explicit_name
        ):

            class Related(self.Base):
                __tablename__ = "related"

                id = Column(Integer, primary_key=True)

        class SomeClass(Versioned, self.Base):
            __tablename__ = "sometable"

            id = Column(Integer, primary_key=True)
            x = Column(Integer)
            y = Column(Integer)

            # Index objects are copied and these have to have a new name
            if constraint_type.index_single_col:
                __table_args__ = (
                    Index(
                        None,
                        x,
                    ),
                )
            elif constraint_type.composite_index:
                __table_args__ = (Index(None, x, y),)
            elif constraint_type.explicit_name_index:
                __table_args__ = (Index("my_index", x, y),)
            # unique constraint objects are discarded.
            elif (
                constraint_type.unique_constraint
                or constraint_type.unique_constraint_naming_conv
            ):
                __table_args__ = (UniqueConstraint(x, y),)
            elif constraint_type.unique_constraint_explicit_name:
                __table_args__ = (UniqueConstraint(x, y, name="my_uq"),)
            # foreign key constraint objects are copied and have the same
            # name, but no database in Core has any problem with this as the
            # names are local to the parent table.
            elif (
                constraint_type.fk_constraint
                or constraint_type.fk_constraint_naming_conv
            ):
                __table_args__ = (ForeignKeyConstraint([x], [Related.id]),)
            elif constraint_type.fk_constraint_explicit_name:
                __table_args__ = (
                    ForeignKeyConstraint([x], [Related.id], name="my_fk"),
                )
            else:
                constraint_type.fail()

        eq_(
            set(idx.name + "_history" for idx in SomeClass.__table__.indexes),
            set(
                idx.name
                for idx in SomeClass.__history_mapper__.local_table.indexes
            ),
        )
        self.create_tables()

    def test_discussion_9546(self):
        class ThingExternal(Versioned, self.Base):
            __tablename__ = "things_external"
            id = Column(Integer, primary_key=True)
            external_attribute = Column(String)

        class ThingLocal(Versioned, self.Base):
            __tablename__ = "things_local"
            id = Column(
                Integer, ForeignKey(ThingExternal.id), primary_key=True
            )
            internal_attribute = Column(String)

        is_(ThingExternal.__table__, inspect(ThingExternal).local_table)

        class Thing(self.Base):
            __table__ = join(ThingExternal, ThingLocal)
            id = column_property(ThingExternal.id, ThingLocal.id)
            version = column_property(
                ThingExternal.version, ThingLocal.version
            )

        eq_ignore_whitespace(
            str(select(Thing)),
            "SELECT things_external.id, things_local.id AS id_1, "
            "things_external.external_attribute, things_external.version, "
            "things_local.version AS version_1, "
            "things_local.internal_attribute FROM things_external "
            "JOIN things_local ON things_external.id = things_local.id",
        )

    def test_w_mapper_versioning(self):
        class SomeClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "sometable"
            use_mapper_versioning = True

            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        self.create_tables()
        sess = self.session
        sc = SomeClass(name="sc1")
        sess.add(sc)
        sess.commit()

        s2 = Session(sess.bind)
        sc2 = s2.query(SomeClass).first()
        sc2.name = "sc1modified"

        sc.name = "sc1modified_again"
        sess.commit()

        eq_(sc.version, 2)

        assert_raises(orm_exc.StaleDataError, s2.flush)

    def test_from_null(self):
        class SomeClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "sometable"

            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        self.create_tables()
        sess = self.session
        sc = SomeClass()
        sess.add(sc)
        sess.commit()

        sc.name = "sc1"
        sess.commit()

        assert sc.version == 2

    def test_insert_null(self):
        class SomeClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "sometable"

            id = Column(Integer, primary_key=True)
            boole = Column(Boolean, default=False)

        self.create_tables()
        sess = self.session
        sc = SomeClass(boole=True)
        sess.add(sc)
        sess.commit()

        sc.boole = None
        sess.commit()

        sc.boole = False
        sess.commit()

        SomeClassHistory = SomeClass.__history_mapper__.class_

        eq_(
            sess.query(SomeClassHistory.boole)
            .order_by(SomeClassHistory.id)
            .all(),
            [(True,), (None,)],
        )

        eq_(sc.version, 3)

    def test_deferred(self):
        """test versioning of unloaded, deferred columns."""

        class SomeClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "sometable"

            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            data = deferred(Column(String(25)))

        self.create_tables()
        sess = self.session
        sc = SomeClass(name="sc1", data="somedata")
        sess.add(sc)
        sess.commit()
        sess.close()

        sc = sess.query(SomeClass).first()
        assert "data" not in sc.__dict__

        sc.name = "sc1modified"
        sess.commit()

        assert sc.version == 2

        SomeClassHistory = SomeClass.__history_mapper__.class_

        eq_(
            sess.query(SomeClassHistory)
            .filter(SomeClassHistory.version == 1)
            .all(),
            [SomeClassHistory(version=1, name="sc1", data="somedata")],
        )

    def test_joined_inheritance(self):
        class BaseClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "basetable"

            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            type = Column(String(20))

            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "base",
            }

        class SubClassSeparatePk(BaseClass):
            __tablename__ = "subtable1"

            id = column_property(
                Column(Integer, primary_key=True), BaseClass.id
            )
            base_id = Column(Integer, ForeignKey("basetable.id"))
            subdata1 = Column(String(50))

            __mapper_args__ = {"polymorphic_identity": "sep"}

        class SubClassSamePk(BaseClass):
            __tablename__ = "subtable2"

            id = Column(Integer, ForeignKey("basetable.id"), primary_key=True)
            subdata2 = Column(String(50))

            __mapper_args__ = {"polymorphic_identity": "same"}

        self.create_tables()
        sess = self.session

        sep1 = SubClassSeparatePk(name="sep1", subdata1="sep1subdata")
        base1 = BaseClass(name="base1")
        same1 = SubClassSamePk(name="same1", subdata2="same1subdata")
        sess.add_all([sep1, base1, same1])
        sess.commit()

        base1.name = "base1mod"
        same1.subdata2 = "same1subdatamod"
        sep1.name = "sep1mod"
        sess.commit()

        BaseClassHistory = BaseClass.__history_mapper__.class_
        SubClassSeparatePkHistory = (
            SubClassSeparatePk.__history_mapper__.class_
        )
        SubClassSamePkHistory = SubClassSamePk.__history_mapper__.class_
        eq_(
            sess.query(BaseClassHistory).order_by(BaseClassHistory.id).all(),
            [
                SubClassSeparatePkHistory(
                    id=1, name="sep1", type="sep", version=1
                ),
                BaseClassHistory(id=2, name="base1", type="base", version=1),
                SubClassSamePkHistory(
                    id=3, name="same1", type="same", version=1
                ),
            ],
        )

        same1.subdata2 = "same1subdatamod2"

        eq_(
            sess.query(BaseClassHistory)
            .order_by(BaseClassHistory.id, BaseClassHistory.version)
            .all(),
            [
                SubClassSeparatePkHistory(
                    id=1, name="sep1", type="sep", version=1
                ),
                BaseClassHistory(id=2, name="base1", type="base", version=1),
                SubClassSamePkHistory(
                    id=3, name="same1", type="same", version=1
                ),
                SubClassSamePkHistory(
                    id=3, name="same1", type="same", version=2
                ),
            ],
        )

        base1.name = "base1mod2"
        eq_(
            sess.query(BaseClassHistory)
            .order_by(BaseClassHistory.id, BaseClassHistory.version)
            .all(),
            [
                SubClassSeparatePkHistory(
                    id=1, name="sep1", type="sep", version=1
                ),
                BaseClassHistory(id=2, name="base1", type="base", version=1),
                BaseClassHistory(
                    id=2, name="base1mod", type="base", version=2
                ),
                SubClassSamePkHistory(
                    id=3, name="same1", type="same", version=1
                ),
                SubClassSamePkHistory(
                    id=3, name="same1", type="same", version=2
                ),
            ],
        )

    def test_joined_inheritance_multilevel(self):
        class BaseClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "basetable"

            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            type = Column(String(20))

            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "base",
            }

        class SubClass(BaseClass):
            __tablename__ = "subtable"

            id = column_property(
                Column(Integer, primary_key=True), BaseClass.id
            )
            base_id = Column(Integer, ForeignKey("basetable.id"))
            subdata1 = Column(String(50))

            __mapper_args__ = {"polymorphic_identity": "sub"}

        class SubSubClass(SubClass):
            __tablename__ = "subsubtable"

            id = Column(Integer, ForeignKey("subtable.id"), primary_key=True)
            subdata2 = Column(String(50))

            __mapper_args__ = {"polymorphic_identity": "subsub"}

        self.create_tables()

        SubSubHistory = SubSubClass.__history_mapper__.class_
        sess = self.session
        q = sess.query(SubSubHistory)
        self.assert_compile(
            q,
            "SELECT "
            "basetable_history.name AS basetable_history_name, "
            "basetable_history.type AS basetable_history_type, "
            "subsubtable_history.version AS subsubtable_history_version, "
            "subtable_history.version AS subtable_history_version, "
            "basetable_history.version AS basetable_history_version, "
            "subtable_history.base_id AS subtable_history_base_id, "
            "subtable_history.subdata1 AS subtable_history_subdata1, "
            "subsubtable_history.id AS subsubtable_history_id, "
            "subtable_history.id AS subtable_history_id, "
            "basetable_history.id AS basetable_history_id, "
            "subsubtable_history.changed AS subsubtable_history_changed, "
            "subtable_history.changed AS subtable_history_changed, "
            "basetable_history.changed AS basetable_history_changed, "
            "subsubtable_history.subdata2 AS subsubtable_history_subdata2 "
            "FROM basetable_history "
            "JOIN subtable_history "
            "ON basetable_history.id = subtable_history.base_id "
            "AND basetable_history.version = subtable_history.version "
            "JOIN subsubtable_history ON subtable_history.id = "
            "subsubtable_history.id AND subtable_history.version = "
            "subsubtable_history.version",
        )

        ssc = SubSubClass(name="ss1", subdata1="sd1", subdata2="sd2")
        sess.add(ssc)
        sess.commit()
        eq_(sess.query(SubSubHistory).all(), [])
        ssc.subdata1 = "sd11"
        ssc.subdata2 = "sd22"
        sess.commit()
        eq_(
            sess.query(SubSubHistory).all(),
            [
                SubSubHistory(
                    name="ss1",
                    subdata1="sd1",
                    subdata2="sd2",
                    type="subsub",
                    version=1,
                )
            ],
        )
        eq_(
            ssc,
            SubSubClass(
                name="ss1", subdata1="sd11", subdata2="sd22", version=2
            ),
        )

    def test_joined_inheritance_changed(self):
        class BaseClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "basetable"

            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            type = Column(String(20))

            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "base",
            }

        class SubClass(BaseClass):
            __tablename__ = "subtable"

            id = Column(Integer, ForeignKey("basetable.id"), primary_key=True)

            __mapper_args__ = {"polymorphic_identity": "sep"}

        self.create_tables()

        BaseClassHistory = BaseClass.__history_mapper__.class_
        SubClassHistory = SubClass.__history_mapper__.class_
        sess = self.session
        s1 = SubClass(name="s1")
        sess.add(s1)
        sess.commit()

        s1.name = "s2"
        sess.commit()

        actual_changed_base = sess.scalar(
            select(BaseClass.__history_mapper__.local_table.c.changed)
        )
        actual_changed_sub = sess.scalar(
            select(SubClass.__history_mapper__.local_table.c.changed)
        )
        h1 = sess.query(BaseClassHistory).first()
        eq_(h1.changed, actual_changed_base)
        eq_(h1.changed, actual_changed_sub)

        h1 = sess.query(SubClassHistory).first()
        eq_(h1.changed, actual_changed_base)
        eq_(h1.changed, actual_changed_sub)

    def test_single_inheritance(self):
        class BaseClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "basetable"

            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            type = Column(String(50))
            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "base",
            }

        class SubClass(BaseClass):
            subname = Column(String(50), unique=True)
            __mapper_args__ = {"polymorphic_identity": "sub"}

        self.create_tables()
        sess = self.session

        b1 = BaseClass(name="b1")
        sc = SubClass(name="s1", subname="sc1")

        sess.add_all([b1, sc])

        sess.commit()

        b1.name = "b1modified"

        BaseClassHistory = BaseClass.__history_mapper__.class_
        SubClassHistory = SubClass.__history_mapper__.class_

        eq_(
            sess.query(BaseClassHistory)
            .order_by(BaseClassHistory.id, BaseClassHistory.version)
            .all(),
            [BaseClassHistory(id=1, name="b1", type="base", version=1)],
        )

        sc.name = "s1modified"
        b1.name = "b1modified2"

        eq_(
            sess.query(BaseClassHistory)
            .order_by(BaseClassHistory.id, BaseClassHistory.version)
            .all(),
            [
                BaseClassHistory(id=1, name="b1", type="base", version=1),
                BaseClassHistory(
                    id=1, name="b1modified", type="base", version=2
                ),
                SubClassHistory(id=2, name="s1", type="sub", version=1),
            ],
        )

        # test the unique constraint on the subclass
        # column
        sc.name = "modifyagain"
        sess.flush()

    def test_unique(self):
        class SomeClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "sometable"

            id = Column(Integer, primary_key=True)
            name = Column(String(50), unique=True)
            data = Column(String(50))

        self.create_tables()
        sess = self.session
        sc = SomeClass(name="sc1", data="sc1")
        sess.add(sc)
        sess.commit()

        sc.data = "sc1modified"
        sess.commit()

        assert sc.version == 2

        sc.data = "sc1modified2"
        sess.commit()

        assert sc.version == 3

    def test_relationship(self):
        class SomeRelated(self.Base, ComparableEntity):
            __tablename__ = "somerelated"

            id = Column(Integer, primary_key=True)

        class SomeClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "sometable"

            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            related_id = Column(Integer, ForeignKey("somerelated.id"))
            related = relationship("SomeRelated", backref="classes")

        SomeClassHistory = SomeClass.__history_mapper__.class_

        self.create_tables()
        sess = self.session
        sc = SomeClass(name="sc1")
        sess.add(sc)
        sess.commit()

        assert sc.version == 1

        sr1 = SomeRelated()
        sc.related = sr1
        sess.commit()

        assert sc.version == 2

        eq_(
            sess.query(SomeClassHistory)
            .filter(SomeClassHistory.version == 1)
            .all(),
            [SomeClassHistory(version=1, name="sc1", related_id=None)],
        )

        sc.related = None

        eq_(
            sess.query(SomeClassHistory)
            .order_by(SomeClassHistory.version)
            .all(),
            [
                SomeClassHistory(version=1, name="sc1", related_id=None),
                SomeClassHistory(version=2, name="sc1", related_id=sr1.id),
            ],
        )

        assert sc.version == 3

    def test_backref_relationship(self):
        class SomeRelated(self.Base, ComparableEntity):
            __tablename__ = "somerelated"

            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            related_id = Column(Integer, ForeignKey("sometable.id"))
            related = relationship("SomeClass", backref="related")

        class SomeClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "sometable"

            id = Column(Integer, primary_key=True)

        self.create_tables()
        sess = self.session
        sc = SomeClass()
        sess.add(sc)
        sess.commit()

        assert sc.version == 1

        sr = SomeRelated(name="sr", related=sc)
        sess.add(sr)
        sess.commit()

        assert sc.version == 1

        sr.name = "sr2"
        sess.commit()

        assert sc.version == 1

        sess.delete(sr)
        sess.commit()

        assert sc.version == 1

    def test_create_double_flush(self):
        class SomeClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "sometable"

            id = Column(Integer, primary_key=True)
            name = Column(String(30))
            other = Column(String(30))

        self.create_tables()

        sc = SomeClass()
        self.session.add(sc)
        self.session.flush()
        sc.name = "Foo"
        self.session.flush()

        assert sc.version == 2

    def test_mutate_plain_column(self):
        class Document(self.Base, Versioned):
            __tablename__ = "document"
            id = Column(Integer, primary_key=True, autoincrement=True)
            name = Column(String, nullable=True)
            description_ = Column("description", String, nullable=True)

        self.create_tables()

        document = Document()
        self.session.add(document)
        document.name = "Foo"
        self.session.commit()
        document.name = "Bar"
        self.session.commit()

        DocumentHistory = Document.__history_mapper__.class_
        v2 = self.session.query(Document).one()
        v1 = self.session.query(DocumentHistory).one()
        eq_(v1.id, v2.id)
        eq_(v2.name, "Bar")
        eq_(v1.name, "Foo")

    def test_mutate_named_column(self):
        class Document(self.Base, Versioned):
            __tablename__ = "document"
            id = Column(Integer, primary_key=True, autoincrement=True)
            name = Column(String, nullable=True)
            description_ = Column("description", String, nullable=True)

        self.create_tables()

        document = Document()
        self.session.add(document)
        document.description_ = "Foo"
        self.session.commit()
        document.description_ = "Bar"
        self.session.commit()

        DocumentHistory = Document.__history_mapper__.class_
        v2 = self.session.query(Document).one()
        v1 = self.session.query(DocumentHistory).one()
        eq_(v1.id, v2.id)
        eq_(v2.description_, "Bar")
        eq_(v1.description_, "Foo")

    def test_unique_identifiers_across_deletes(self):
        """Ensure unique integer values are used for the primary table.

        Checks whether the database assigns the same identifier twice
        within the span of a table.  SQLite will do this if
        sqlite_autoincrement is not set (e.g. SQLite's AUTOINCREMENT flag).

        """

        class SomeClass(Versioned, self.Base, ComparableEntity):
            __tablename__ = "sometable"

            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        self.create_tables()
        sess = self.session
        sc = SomeClass(name="sc1")
        sess.add(sc)
        sess.commit()

        sess.delete(sc)
        sess.commit()

        sc2 = SomeClass(name="sc2")
        sess.add(sc2)
        sess.commit()

        SomeClassHistory = SomeClass.__history_mapper__.class_

        # only one entry should exist in the history table; one()
        # ensures that
        scdeleted = sess.query(SomeClassHistory).one()

        # If sc2 has the same id that deleted sc1 had,
        # it will fail when modified or deleted
        # because of the violation of the uniqueness of the primary key on
        # sometable_history
        ne_(sc2.id, scdeleted.id)

        # If previous assertion fails, this will also fail:
        sc2.name = "sc2 modified"
        sess.commit()

    def test_external_id(self):
        class ObjectExternal(Versioned, self.Base, ComparableEntity):
            __tablename__ = "externalobjects"

            id1 = Column(String(3), primary_key=True)
            id2 = Column(String(3), primary_key=True)
            name = Column(String(50))

        self.create_tables()
        sess = self.session
        sc = ObjectExternal(id1="aaa", id2="bbb", name="sc1")
        sess.add(sc)
        sess.commit()

        sc.name = "sc1modified"
        sess.commit()

        assert sc.version == 2

        ObjectExternalHistory = ObjectExternal.__history_mapper__.class_

        eq_(
            sess.query(ObjectExternalHistory).all(),
            [
                ObjectExternalHistory(
                    version=1, id1="aaa", id2="bbb", name="sc1"
                ),
            ],
        )

        sess.delete(sc)
        sess.commit()

        assert sess.query(ObjectExternal).count() == 0

        eq_(
            sess.query(ObjectExternalHistory).all(),
            [
                ObjectExternalHistory(
                    version=1, id1="aaa", id2="bbb", name="sc1"
                ),
                ObjectExternalHistory(
                    version=2, id1="aaa", id2="bbb", name="sc1modified"
                ),
            ],
        )

        sc = ObjectExternal(id1="aaa", id2="bbb", name="sc1reappeared")
        sess.add(sc)
        sess.commit()

        assert sc.version == 3

        sc.name = "sc1reappearedmodified"
        sess.commit()

        assert sc.version == 4

        eq_(
            sess.query(ObjectExternalHistory).all(),
            [
                ObjectExternalHistory(
                    version=1, id1="aaa", id2="bbb", name="sc1"
                ),
                ObjectExternalHistory(
                    version=2, id1="aaa", id2="bbb", name="sc1modified"
                ),
                ObjectExternalHistory(
                    version=3, id1="aaa", id2="bbb", name="sc1reappeared"
                ),
            ],
        )


class TestVersioningNewBase(TestVersioning):
    def make_base(self):
        class Base(DeclarativeBase):
            pass

        self.Base = Base


class TestVersioningUnittest(TestVersioning, unittest.TestCase):
    pass


class TestVersioningNewBaseUnittest(TestVersioningNewBase, unittest.TestCase):
    pass


if __name__ == "__main__":
    unittest.main()
