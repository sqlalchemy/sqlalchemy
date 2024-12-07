import datetime

import sqlalchemy as sa
from sqlalchemy import and_
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import aliased
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import column_property
from sqlalchemy.orm import composite
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import foreign
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import relationship
from sqlalchemy.orm import remote
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import synonym
from sqlalchemy.orm.interfaces import MANYTOONE
from sqlalchemy.orm.interfaces import ONETOMANY
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_
from sqlalchemy.testing.assertsql import assert_engine
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.entities import BasicEntity
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from test.orm import _fixtures


class _RelationshipErrors:
    def _assert_raises_no_relevant_fks(
        self, fn, expr, relname, primary, *arg, **kw
    ):
        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not locate any relevant foreign key columns "
            "for %s join condition '%s' on relationship %s.  "
            "Ensure that referencing columns are associated with "
            "a ForeignKey or ForeignKeyConstraint, or are annotated "
            r"in the join condition with the foreign\(\) annotation."
            % (primary, expr, relname),
            fn,
            *arg,
            **kw,
        )

    def _assert_raises_no_equality(
        self, fn, expr, relname, primary, *arg, **kw
    ):
        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not locate any simple equality expressions "
            "involving locally mapped foreign key columns for %s join "
            "condition '%s' on relationship %s.  "
            "Ensure that referencing columns are associated with a "
            "ForeignKey or ForeignKeyConstraint, or are annotated in "
            r"the join condition with the foreign\(\) annotation. "
            "To allow comparison operators other than '==', "
            "the relationship can be marked as viewonly=True."
            % (primary, expr, relname),
            fn,
            *arg,
            **kw,
        )

    def _assert_raises_ambig_join(
        self, fn, relname, secondary_arg, *arg, **kw
    ):
        if secondary_arg is not None:
            assert_raises_message(
                exc.ArgumentError,
                "Could not determine join condition between "
                "parent/child tables on relationship %s - "
                "there are multiple foreign key paths linking the "
                "tables via secondary table '%s'.  "
                "Specify the 'foreign_keys' argument, providing a list "
                "of those columns which should be counted as "
                "containing a foreign key reference from the "
                "secondary table to each of the parent and child tables."
                % (relname, secondary_arg),
                fn,
                *arg,
                **kw,
            )
        else:
            assert_raises_message(
                exc.ArgumentError,
                "Could not determine join "
                "condition between parent/child tables on "
                "relationship %s - there are multiple foreign key "
                "paths linking the tables.  Specify the "
                "'foreign_keys' argument, providing a list of those "
                "columns which should be counted as containing a "
                "foreign key reference to the parent table." % (relname,),
                fn,
                *arg,
                **kw,
            )

    def _assert_raises_no_join(self, fn, relname, secondary_arg, *arg, **kw):
        if secondary_arg is not None:
            assert_raises_message(
                exc.NoForeignKeysError,
                "Could not determine join condition between "
                "parent/child tables on relationship %s - "
                "there are no foreign keys linking these tables "
                "via secondary table '%s'.  "
                "Ensure that referencing columns are associated with a "
                "ForeignKey "
                "or ForeignKeyConstraint, or specify 'primaryjoin' and "
                "'secondaryjoin' expressions" % (relname, secondary_arg),
                fn,
                *arg,
                **kw,
            )
        else:
            assert_raises_message(
                exc.NoForeignKeysError,
                "Could not determine join condition between "
                "parent/child tables on relationship %s - "
                "there are no foreign keys linking these tables.  "
                "Ensure that referencing columns are associated with a "
                "ForeignKey "
                "or ForeignKeyConstraint, or specify a 'primaryjoin' "
                "expression." % (relname,),
                fn,
                *arg,
                **kw,
            )

    def _assert_raises_ambiguous_direction(self, fn, relname, *arg, **kw):
        assert_raises_message(
            sa.exc.ArgumentError,
            "Can't determine relationship"
            " direction for relationship '%s' - foreign "
            "key columns within the join condition are present "
            "in both the parent and the child's mapped tables.  "
            "Ensure that only those columns referring to a parent column "
            r"are marked as foreign, either via the foreign\(\) annotation or "
            "via the foreign_keys argument." % relname,
            fn,
            *arg,
            **kw,
        )

    def _assert_raises_no_local_remote(self, fn, relname, *arg, **kw):
        assert_raises_message(
            sa.exc.ArgumentError,
            "Relationship %s could not determine "
            "any unambiguous local/remote column "
            "pairs based on join condition and remote_side arguments.  "
            r"Consider using the remote\(\) annotation to "
            "accurately mark those elements of the join "
            "condition that are on the remote side of the relationship."
            % (relname),
            fn,
            *arg,
            **kw,
        )


class DependencyTwoParentTest(fixtures.MappedTest):
    """Test flush() when a mapper is dependent on multiple relationships"""

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "tbl_a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(128)),
        )
        Table(
            "tbl_b",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(128)),
        )
        Table(
            "tbl_c",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column(
                "tbl_a_id", Integer, ForeignKey("tbl_a.id"), nullable=False
            ),
            Column("name", String(128)),
        )
        Table(
            "tbl_d",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column(
                "tbl_c_id", Integer, ForeignKey("tbl_c.id"), nullable=False
            ),
            Column("tbl_b_id", Integer, ForeignKey("tbl_b.id")),
            Column("name", String(128)),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

        class C(cls.Basic):
            pass

        class D(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        A, C, B, D, tbl_b, tbl_c, tbl_a, tbl_d = (
            cls.classes.A,
            cls.classes.C,
            cls.classes.B,
            cls.classes.D,
            cls.tables.tbl_b,
            cls.tables.tbl_c,
            cls.tables.tbl_a,
            cls.tables.tbl_d,
        )

        cls.mapper_registry.map_imperatively(
            A,
            tbl_a,
            properties=dict(
                c_rows=relationship(
                    C, cascade="all, delete-orphan", backref="a_row"
                )
            ),
        )
        cls.mapper_registry.map_imperatively(B, tbl_b)
        cls.mapper_registry.map_imperatively(
            C,
            tbl_c,
            properties=dict(
                d_rows=relationship(
                    D, cascade="all, delete-orphan", backref="c_row"
                )
            ),
        )
        cls.mapper_registry.map_imperatively(
            D, tbl_d, properties=dict(b_row=relationship(B))
        )

    @classmethod
    def insert_data(cls, connection):
        A, C, B, D = (
            cls.classes.A,
            cls.classes.C,
            cls.classes.B,
            cls.classes.D,
        )

        session = Session(connection)
        a = A(name="a1")
        b = B(name="b1")
        c = C(name="c1", a_row=a)

        d1 = D(name="d1", b_row=b, c_row=c)  # noqa
        d2 = D(name="d2", b_row=b, c_row=c)  # noqa
        d3 = D(name="d3", b_row=b, c_row=c)  # noqa
        session.add(a)
        session.add(b)
        session.flush()

    def test_DeleteRootTable(self):
        A = self.classes.A

        session = fixture_session()
        a = session.query(A).filter_by(name="a1").one()

        session.delete(a)
        session.flush()

    def test_DeleteMiddleTable(self):
        C = self.classes.C

        session = fixture_session()
        c = session.query(C).filter_by(name="c1").one()

        session.delete(c)
        session.flush()


class M2ODontOverwriteFKTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("bid", ForeignKey("b.id")),
        )
        Table("b", metadata, Column("id", Integer, primary_key=True))

    def _fixture(self, uselist=False):
        a, b = self.tables.a, self.tables.b

        class A(BasicEntity):
            pass

        class B(BasicEntity):
            pass

        self.mapper_registry.map_imperatively(
            A, a, properties={"b": relationship(B, uselist=uselist)}
        )
        self.mapper_registry.map_imperatively(B, b)
        return A, B

    def test_joinedload_doesnt_produce_bogus_event(self):
        A, B = self._fixture()
        sess = fixture_session()

        b1 = B()
        sess.add(b1)
        sess.flush()

        a1 = A()
        sess.add(a1)
        sess.commit()

        # test that was broken by #3060
        a1 = sess.query(A).options(joinedload(A.b)).first()
        a1.bid = b1.id
        sess.flush()

        eq_(a1.bid, b1.id)

    def test_init_doesnt_produce_scalar_event(self):
        A, B = self._fixture()
        sess = fixture_session()

        b1 = B()
        sess.add(b1)
        sess.flush()

        a1 = A()
        assert a1.b is None
        a1.bid = b1.id
        sess.add(a1)
        sess.flush()
        assert a1.bid is not None

    def test_init_doesnt_produce_collection_event(self):
        A, B = self._fixture(uselist=True)
        sess = fixture_session()

        b1 = B()
        sess.add(b1)
        sess.flush()

        a1 = A()
        assert a1.b == []
        a1.bid = b1.id
        sess.add(a1)
        sess.flush()
        assert a1.bid is not None

    def test_scalar_relationship_overrides_fk(self):
        A, B = self._fixture()
        sess = fixture_session()

        b1 = B()
        sess.add(b1)
        sess.flush()

        a1 = A()
        a1.bid = b1.id
        a1.b = None
        sess.add(a1)
        sess.flush()
        assert a1.bid is None

    def test_collection_relationship_overrides_fk(self):
        A, B = self._fixture(uselist=True)
        sess = fixture_session()

        b1 = B()
        sess.add(b1)
        sess.flush()

        a1 = A()
        a1.bid = b1.id
        a1.b = []
        sess.add(a1)
        sess.flush()
        # this is weird
        assert a1.bid is not None


class DirectSelfRefFKTest(fixtures.MappedTest, AssertsCompiledSQL):
    """Tests the ultimate join condition, a single column
    that points to itself, e.g. within a SQL function or similar.
    The test is against a materialized path setup.

    this is an **extremely** unusual case:

    .. sourcecode:: text

        Entity
        ------
         path -------+
           ^         |
           +---------+

    In this case, one-to-many and many-to-one are no longer accurate.
    Both relationships return collections.   I'm not sure if this is a good
    idea.

    """

    __dialect__ = "default"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "entity", metadata, Column("path", String(100), primary_key=True)
        )

    @classmethod
    def setup_classes(cls):
        class Entity(cls.Basic):
            def __init__(self, path):
                self.path = path

    def _descendants_fixture(self, data=True):
        Entity = self.classes.Entity
        entity = self.tables.entity

        m = self.mapper_registry.map_imperatively(
            Entity,
            entity,
            properties={
                "descendants": relationship(
                    Entity,
                    primaryjoin=remote(foreign(entity.c.path)).like(
                        entity.c.path.concat("/%")
                    ),
                    viewonly=True,
                    order_by=entity.c.path,
                )
            },
        )
        configure_mappers()
        assert m.get_property("descendants").direction is ONETOMANY
        if data:
            return self._fixture()

    def _anscestors_fixture(self, data=True):
        Entity = self.classes.Entity
        entity = self.tables.entity

        m = self.mapper_registry.map_imperatively(
            Entity,
            entity,
            properties={
                "anscestors": relationship(
                    Entity,
                    primaryjoin=entity.c.path.like(
                        remote(foreign(entity.c.path)).concat("/%")
                    ),
                    viewonly=True,
                    order_by=entity.c.path,
                )
            },
        )
        configure_mappers()
        assert m.get_property("anscestors").direction is ONETOMANY
        if data:
            return self._fixture()

    def _fixture(self):
        Entity = self.classes.Entity
        sess = fixture_session()
        sess.add_all(
            [
                Entity("/foo"),
                Entity("/foo/bar1"),
                Entity("/foo/bar2"),
                Entity("/foo/bar2/bat1"),
                Entity("/foo/bar2/bat2"),
                Entity("/foo/bar3"),
                Entity("/bar"),
                Entity("/bar/bat1"),
            ]
        )
        return sess

    def test_descendants_lazyload_clause(self):
        self._descendants_fixture(data=False)
        Entity = self.classes.Entity
        self.assert_compile(
            Entity.descendants.property.strategy._lazywhere,
            "entity.path LIKE (:param_1 || :path_1)",
        )

        self.assert_compile(
            Entity.descendants.property.strategy._rev_lazywhere,
            ":param_1 LIKE (entity.path || :path_1)",
        )

    def test_ancestors_lazyload_clause(self):
        self._anscestors_fixture(data=False)
        Entity = self.classes.Entity
        # :param_1 LIKE (:param_1 || :path_1)
        self.assert_compile(
            Entity.anscestors.property.strategy._lazywhere,
            ":param_1 LIKE (entity.path || :path_1)",
        )

        self.assert_compile(
            Entity.anscestors.property.strategy._rev_lazywhere,
            "entity.path LIKE (:param_1 || :path_1)",
        )

    def test_descendants_lazyload(self):
        sess = self._descendants_fixture()
        Entity = self.classes.Entity
        e1 = sess.query(Entity).filter_by(path="/foo").first()
        eq_(
            [e.path for e in e1.descendants],
            [
                "/foo/bar1",
                "/foo/bar2",
                "/foo/bar2/bat1",
                "/foo/bar2/bat2",
                "/foo/bar3",
            ],
        )

    def test_anscestors_lazyload(self):
        sess = self._anscestors_fixture()
        Entity = self.classes.Entity
        e1 = sess.query(Entity).filter_by(path="/foo/bar2/bat1").first()
        eq_([e.path for e in e1.anscestors], ["/foo", "/foo/bar2"])

    def test_descendants_joinedload(self):
        sess = self._descendants_fixture()
        Entity = self.classes.Entity
        e1 = (
            sess.query(Entity)
            .filter_by(path="/foo")
            .options(joinedload(Entity.descendants))
            .first()
        )

        eq_(
            [e.path for e in e1.descendants],
            [
                "/foo/bar1",
                "/foo/bar2",
                "/foo/bar2/bat1",
                "/foo/bar2/bat2",
                "/foo/bar3",
            ],
        )

    def test_descendants_subqueryload(self):
        sess = self._descendants_fixture()
        Entity = self.classes.Entity
        e1 = (
            sess.query(Entity)
            .filter_by(path="/foo")
            .options(subqueryload(Entity.descendants))
            .first()
        )

        eq_(
            [e.path for e in e1.descendants],
            [
                "/foo/bar1",
                "/foo/bar2",
                "/foo/bar2/bat1",
                "/foo/bar2/bat2",
                "/foo/bar3",
            ],
        )

    def test_anscestors_joinedload(self):
        sess = self._anscestors_fixture()
        Entity = self.classes.Entity
        e1 = (
            sess.query(Entity)
            .filter_by(path="/foo/bar2/bat1")
            .options(joinedload(Entity.anscestors))
            .first()
        )
        eq_([e.path for e in e1.anscestors], ["/foo", "/foo/bar2"])

    def test_plain_join_descendants(self):
        self._descendants_fixture(data=False)
        Entity = self.classes.Entity
        sess = fixture_session()

        da = aliased(Entity)
        self.assert_compile(
            sess.query(Entity).join(Entity.descendants.of_type(da)),
            "SELECT entity.path AS entity_path FROM entity JOIN entity AS "
            "entity_1 ON entity_1.path LIKE (entity.path || :path_1)",
        )


class OverlappingFksSiblingTest(fixtures.MappedTest):
    """Test multiple relationships that use sections of the same
    composite foreign key.

    """

    run_define_tables = "each"

    def _fixture_one(
        self,
        add_b_a=False,
        add_b_a_viewonly=False,
        add_b_amember=False,
        add_bsub1_a=False,
        add_bsub2_a_viewonly=False,
        add_b_a_overlaps=None,
    ):
        Base = self.mapper_registry.generate_base()

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            a_members = relationship("AMember", backref="a")

        class AMember(Base):
            __tablename__ = "a_member"

            a_id = Column(Integer, ForeignKey("a.id"), primary_key=True)
            a_member_id = Column(Integer, primary_key=True)

        class B(Base):
            __tablename__ = "b"

            __mapper_args__ = {"polymorphic_on": "type"}

            id = Column(Integer, primary_key=True)
            type = Column(String(20))

            a_id = Column(Integer, ForeignKey("a.id"), nullable=False)
            a_member_id = Column(Integer)

            __table_args__ = (
                ForeignKeyConstraint(
                    ("a_id", "a_member_id"),
                    ("a_member.a_id", "a_member.a_member_id"),
                ),
            )

            # if added and viewonly is not true, this relationship
            # writes to B.a_id, which conflicts with BSub2.a_member,
            # so should warn
            if add_b_a:
                a = relationship(
                    "A", viewonly=add_b_a_viewonly, overlaps=add_b_a_overlaps
                )

            # if added, this relationship writes to B.a_id, which conflicts
            # with BSub1.a
            if add_b_amember:
                a_member = relationship("AMember")

        # however, *no* warning should be emitted otherwise.

        class BSub1(B):
            if add_bsub1_a:
                a = relationship("A")

            __mapper_args__ = {"polymorphic_identity": "bsub1"}

        class BSub2(B):
            if add_bsub2_a_viewonly:
                a = relationship("A", viewonly=True)

            a_member = relationship("AMember")

            __mapper_args__ = {"polymorphic_identity": "bsub2"}

        configure_mappers()
        assert self.tables_test_metadata is Base.metadata
        self.tables_test_metadata.create_all(testing.db)

        return A, AMember, B, BSub1, BSub2

    def _fixture_two(self, setup_backrefs=False, setup_overlaps=False):
        Base = self.mapper_registry.generate_base()

        # purposely using the comma to make sure parsing the comma works

        class Parent(Base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)
            children = relationship(
                "Child",
                back_populates=("parent" if setup_backrefs else None),
                overlaps="foo, bar, parent" if setup_overlaps else None,
            )

        class Child(Base):
            __tablename__ = "child"
            id = Column(Integer, primary_key=True)
            num = Column(Integer)
            parent_id = Column(
                Integer, ForeignKey("parent.id"), nullable=False
            )
            parent = relationship(
                "Parent",
                back_populates=("children" if setup_backrefs else None),
                overlaps="bar, bat, children" if setup_overlaps else None,
            )

        configure_mappers()

    def _fixture_three(self, use_same_mappers, setup_overlaps):
        Base = self.mapper_registry.generate_base()

        class Child(Base):
            __tablename__ = "child"
            id = Column(Integer, primary_key=True)
            num = Column(Integer)
            parent_id = Column(
                Integer, ForeignKey("parent.id"), nullable=False
            )

        if not use_same_mappers:
            c1 = aliased(Child)
            c2 = aliased(Child)

        class Parent(Base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)
            if use_same_mappers:
                child1 = relationship(
                    Child,
                    primaryjoin=lambda: and_(
                        Child.parent_id == Parent.id, Child.num == 1
                    ),
                    overlaps="child2" if setup_overlaps else None,
                )
                child2 = relationship(
                    Child,
                    primaryjoin=lambda: and_(
                        Child.parent_id == Parent.id, Child.num == 2
                    ),
                    overlaps="child1" if setup_overlaps else None,
                )
            else:
                child1 = relationship(
                    c1,
                    primaryjoin=lambda: and_(
                        c1.parent_id == Parent.id, c1.num == 1
                    ),
                    overlaps="child2" if setup_overlaps else None,
                )

                child2 = relationship(
                    c2,
                    primaryjoin=lambda: and_(
                        c2.parent_id == Parent.id, c2.num == 1
                    ),
                    overlaps="child1" if setup_overlaps else None,
                )

        configure_mappers()

    def _fixture_four(self):
        Base = self.mapper_registry.generate_base()

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)

            c_id = Column(ForeignKey("c.id"))

        class B1(A):
            pass

        class B2(A):
            pass

        class C(Base):
            __tablename__ = "c"

            id = Column(Integer, primary_key=True)
            b1 = relationship(B1, backref="c")
            b2 = relationship(B2, backref="c")

    @testing.provide_metadata
    def _test_fixture_one_run(self, **kw):
        A, AMember, B, BSub1, BSub2 = self._fixture_one(**kw)

        bsub2 = BSub2()
        am1 = AMember(a_member_id=1)

        a1 = A(a_members=[am1])
        bsub2.a_member = am1

        bsub1 = BSub1()
        a2 = A()
        bsub1.a = a2

        session = Session(testing.db)
        session.add_all([bsub1, bsub2, am1, a1, a2])
        session.commit()

        assert bsub1.a is a2
        assert bsub2.a is a1

        # meaningless, because BSub1 doesn't have a_member
        bsub1.a_member = am1

        # meaningless, because BSub2's ".a" is viewonly=True
        bsub2.a = a2

        session.commit()
        assert bsub1.a is a2  # because bsub1.a_member is not a relationship

        assert BSub2.__mapper__.attrs.a.viewonly
        assert bsub2.a is a1  # because bsub2.a is viewonly=True

        # everyone has a B.a relationship
        eq_(
            session.query(B, A).outerjoin(B.a).order_by(B.id).all(),
            [(bsub1, a2), (bsub2, a1)],
        )

    @testing.provide_metadata
    def test_simple_warn(self):
        with expect_warnings(
            r"relationship '(?:Child.parent|Parent.children)' will copy "
            r"column parent.id to column child.parent_id, which conflicts "
            r"with relationship\(s\): '(?:Parent.children|Child.parent)' "
            r"\(copies parent.id to child.parent_id\)."
        ):
            self._fixture_two(setup_backrefs=False)

    @testing.combinations((True,), (False,), argnames="set_overlaps")
    def test_fixture_five(self, metadata, set_overlaps):
        Base = self.mapper_registry.generate_base()

        if set_overlaps:
            overlaps = "as,cs"
        else:
            overlaps = None

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            cs = relationship("C", secondary="b", backref="as")
            bs = relationship("B", back_populates="a", overlaps=overlaps)

        class B(Base):
            __tablename__ = "b"

            a_id = Column(ForeignKey("a.id"), primary_key=True)
            c_id = Column(ForeignKey("c.id"), primary_key=True)
            a = relationship("A", back_populates="bs", overlaps=overlaps)
            c = relationship("C", back_populates="bs", overlaps=overlaps)

        class C(Base):
            __tablename__ = "c"

            id = Column(Integer, primary_key=True)
            bs = relationship("B", back_populates="c", overlaps=overlaps)

        if set_overlaps:
            configure_mappers()
        else:
            with expect_warnings(
                r"relationship 'A.bs' will copy column a.id to column b.a_id, "
                r"which conflicts with relationship\(s\): "
                r"'A.cs' \(copies a.id to b.a_id\), "
                r"'C.as' \(copies a.id to b.a_id\)"
                r".*add the parameter 'overlaps=\"as,cs\"' to the 'A.bs' "
                r"relationship",
                #
                #
                r"relationship 'B.a' will copy column a.id to column b.a_id, "
                r"which conflicts with relationship\(s\): "
                r"'A.cs' \(copies a.id to b.a_id\), "
                r"'C.as' \(copies a.id to b.a_id\)"
                r".*add the parameter 'overlaps=\"as,cs\"' to the 'B.a' "
                r"relationship",
                #
                #
                r"relationship 'B.c' will copy column c.id to column b.c_id, "
                r"which conflicts with relationship\(s\): "
                r"'A.cs' \(copies c.id to b.c_id\), "
                r"'C.as' \(copies c.id to b.c_id\)"
                r".*add the parameter 'overlaps=\"as,cs\"' to the 'B.c' "
                r"relationship",
                #
                #
                r"relationship 'C.bs' will copy column c.id to column b.c_id, "
                r"which conflicts with relationship\(s\): "
                r"'A.cs' \(copies c.id to b.c_id\), "
                r"'C.as' \(copies c.id to b.c_id\)"
                r".*add the parameter 'overlaps=\"as,cs\"' to the 'C.bs' "
                r"relationship",
            ):
                configure_mappers()

    @testing.provide_metadata
    def test_fixture_four(self):
        self._fixture_four()

    @testing.provide_metadata
    def test_simple_backrefs_works(self):
        self._fixture_two(setup_backrefs=True)

    @testing.provide_metadata
    def test_simple_overlaps_works(self):
        self._fixture_two(setup_overlaps=True)

    @testing.provide_metadata
    def test_double_rel_same_mapper_warns(self):
        with expect_warnings(
            r"relationship 'Parent.child[12]' will copy column parent.id to "
            r"column child.parent_id, which conflicts with relationship\(s\): "
            r"'Parent.child[12]' \(copies parent.id to child.parent_id\)"
        ):
            self._fixture_three(use_same_mappers=True, setup_overlaps=False)

    @testing.provide_metadata
    def test_double_rel_same_mapper_overlaps_works(self):
        self._fixture_three(use_same_mappers=True, setup_overlaps=True)

    @testing.provide_metadata
    def test_double_rel_aliased_mapper_works(self):
        self._fixture_three(use_same_mappers=False, setup_overlaps=False)

    @testing.provide_metadata
    def test_warn_one(self):
        with expect_warnings(
            r"relationship '(?:BSub1.a|BSub2.a_member|B.a)' will copy column "
            r"(?:a.id|a_member.a_id) to column b.a_id"
        ):
            self._fixture_one(add_b_a=True, add_bsub1_a=True)

    @testing.provide_metadata
    def test_warn_two(self):
        with expect_warnings(
            r"relationship '(?:BSub1.a|B.a_member)' will copy column "
            r"(?:a.id|a_member.a_id) to column b.a_id"
        ):
            self._fixture_one(add_b_amember=True, add_bsub1_a=True)

    @testing.provide_metadata
    def test_warn_three(self):
        with expect_warnings(
            r"relationship '(?:BSub1.a|B.a_member|BSub2.a_member|B.a)' "
            r"will copy column (?:a.id|a_member.a_id) to column b.a_id",
        ):
            self._fixture_one(
                add_b_amember=True, add_bsub1_a=True, add_b_a=True
            )

    @testing.provide_metadata
    def test_warn_four(self):
        with expect_warnings(
            r"relationship '(?:B.a|BSub2.a_member|B.a)' will copy column "
            r"(?:a.id|a_member.a_id) to column b.a_id"
        ):
            self._fixture_one(add_bsub2_a_viewonly=True, add_b_a=True)

    @testing.provide_metadata
    def test_works_one(self):
        self._test_fixture_one_run(
            add_b_a=True, add_b_a_viewonly=True, add_bsub1_a=True
        )

    @testing.provide_metadata
    def test_works_two(self):
        # doesn't actually work with real FKs because it creates conflicts :)
        self._fixture_one(
            add_b_a=True, add_b_a_overlaps="a_member", add_bsub1_a=True
        )


class CompositeSelfRefFKTest(fixtures.MappedTest, AssertsCompiledSQL):
    """Tests a composite FK where, in
    the relationship(), one col points
    to itself in the same table.

    this is a very unusual case:

    .. sourcecode:: text

        company         employee
        ----------      ----------
        company_id <--- company_id ------+
        name                ^            |
                            +------------+

                        emp_id <---------+
                        name             |
                        reports_to_id ---+

    employee joins to its sub-employees
    both on reports_to_id, *and on company_id to itself*.

    """

    __dialect__ = "default"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "company_t",
            metadata,
            Column(
                "company_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(30)),
        )

        Table(
            "employee_t",
            metadata,
            Column("company_id", Integer, primary_key=True),
            Column("emp_id", Integer, primary_key=True),
            Column("name", String(30)),
            Column("reports_to_id", Integer),
            sa.ForeignKeyConstraint(["company_id"], ["company_t.company_id"]),
            sa.ForeignKeyConstraint(
                ["company_id", "reports_to_id"],
                ["employee_t.company_id", "employee_t.emp_id"],
            ),
        )

    @classmethod
    def setup_classes(cls):
        class Company(cls.Basic):
            def __init__(self, name):
                self.name = name

        class Employee(cls.Basic):
            def __init__(self, name, company, emp_id, reports_to=None):
                self.name = name
                self.company = company
                self.emp_id = emp_id
                self.reports_to = reports_to

    def test_explicit(self):
        Employee, Company, employee_t, company_t = (
            self.classes.Employee,
            self.classes.Company,
            self.tables.employee_t,
            self.tables.company_t,
        )

        self.mapper_registry.map_imperatively(Company, company_t)
        self.mapper_registry.map_imperatively(
            Employee,
            employee_t,
            properties={
                "company": relationship(
                    Company,
                    primaryjoin=employee_t.c.company_id
                    == company_t.c.company_id,
                    backref="employees",
                ),
                "reports_to": relationship(
                    Employee,
                    primaryjoin=sa.and_(
                        employee_t.c.emp_id == employee_t.c.reports_to_id,
                        employee_t.c.company_id == employee_t.c.company_id,
                    ),
                    remote_side=[employee_t.c.emp_id, employee_t.c.company_id],
                    foreign_keys=[
                        employee_t.c.reports_to_id,
                        employee_t.c.company_id,
                    ],
                    backref=backref(
                        "employees",
                        foreign_keys=[
                            employee_t.c.reports_to_id,
                            employee_t.c.company_id,
                        ],
                    ),
                ),
            },
        )

        self._test()

    def test_implicit(self):
        Employee, Company, employee_t, company_t = (
            self.classes.Employee,
            self.classes.Company,
            self.tables.employee_t,
            self.tables.company_t,
        )

        self.mapper_registry.map_imperatively(Company, company_t)
        self.mapper_registry.map_imperatively(
            Employee,
            employee_t,
            properties={
                "company": relationship(Company, backref="employees"),
                "reports_to": relationship(
                    Employee,
                    remote_side=[employee_t.c.emp_id, employee_t.c.company_id],
                    foreign_keys=[
                        employee_t.c.reports_to_id,
                        employee_t.c.company_id,
                    ],
                    backref=backref(
                        "employees",
                        foreign_keys=[
                            employee_t.c.reports_to_id,
                            employee_t.c.company_id,
                        ],
                    ),
                ),
            },
        )

        self._test()

    def test_very_implicit(self):
        Employee, Company, employee_t, company_t = (
            self.classes.Employee,
            self.classes.Company,
            self.tables.employee_t,
            self.tables.company_t,
        )

        self.mapper_registry.map_imperatively(Company, company_t)
        self.mapper_registry.map_imperatively(
            Employee,
            employee_t,
            properties={
                "company": relationship(Company, backref="employees"),
                "reports_to": relationship(
                    Employee,
                    remote_side=[employee_t.c.emp_id, employee_t.c.company_id],
                    backref="employees",
                ),
            },
        )

        self._test()

    def test_very_explicit(self):
        Employee, Company, employee_t, company_t = (
            self.classes.Employee,
            self.classes.Company,
            self.tables.employee_t,
            self.tables.company_t,
        )

        self.mapper_registry.map_imperatively(Company, company_t)
        self.mapper_registry.map_imperatively(
            Employee,
            employee_t,
            properties={
                "company": relationship(Company, backref="employees"),
                "reports_to": relationship(
                    Employee,
                    _local_remote_pairs=[
                        (employee_t.c.reports_to_id, employee_t.c.emp_id),
                        (employee_t.c.company_id, employee_t.c.company_id),
                    ],
                    foreign_keys=[
                        employee_t.c.reports_to_id,
                        employee_t.c.company_id,
                    ],
                    backref=backref(
                        "employees",
                        foreign_keys=[
                            employee_t.c.reports_to_id,
                            employee_t.c.company_id,
                        ],
                    ),
                ),
            },
        )

        self._test()

    def test_annotated(self):
        Employee, Company, employee_t, company_t = (
            self.classes.Employee,
            self.classes.Company,
            self.tables.employee_t,
            self.tables.company_t,
        )

        self.mapper_registry.map_imperatively(Company, company_t)
        self.mapper_registry.map_imperatively(
            Employee,
            employee_t,
            properties={
                "company": relationship(Company, backref="employees"),
                "reports_to": relationship(
                    Employee,
                    primaryjoin=sa.and_(
                        remote(employee_t.c.emp_id)
                        == employee_t.c.reports_to_id,
                        remote(employee_t.c.company_id)
                        == employee_t.c.company_id,
                    ),
                    backref=backref("employees"),
                ),
            },
        )

        self._assert_lazy_clauses()
        self._test()

    def test_overlapping_warning(self):
        Employee, Company, employee_t, company_t = (
            self.classes.Employee,
            self.classes.Company,
            self.tables.employee_t,
            self.tables.company_t,
        )

        self.mapper_registry.map_imperatively(Company, company_t)
        self.mapper_registry.map_imperatively(
            Employee,
            employee_t,
            properties={
                "company": relationship(Company, backref="employees"),
                "reports_to": relationship(
                    Employee,
                    primaryjoin=sa.and_(
                        remote(employee_t.c.emp_id)
                        == employee_t.c.reports_to_id,
                        remote(employee_t.c.company_id)
                        == employee_t.c.company_id,
                    ),
                    backref=backref("employees"),
                ),
            },
        )

        with expect_warnings(
            r"relationship .* will copy column .* to column "
            r"employee_t.company_id, which conflicts with relationship\(s\)"
        ):
            configure_mappers()

    def test_annotated_no_overwriting(self):
        Employee, Company, employee_t, company_t = (
            self.classes.Employee,
            self.classes.Company,
            self.tables.employee_t,
            self.tables.company_t,
        )

        self.mapper_registry.map_imperatively(Company, company_t)
        self.mapper_registry.map_imperatively(
            Employee,
            employee_t,
            properties={
                "company": relationship(Company, backref="employees"),
                "reports_to": relationship(
                    Employee,
                    primaryjoin=sa.and_(
                        remote(employee_t.c.emp_id)
                        == foreign(employee_t.c.reports_to_id),
                        remote(employee_t.c.company_id)
                        == employee_t.c.company_id,
                    ),
                    backref=backref("employees"),
                ),
            },
        )

        self._assert_lazy_clauses()
        self._test_no_warning()

    def _test_no_overwrite(self, sess, expect_failure):
        # test [ticket:3230]

        Employee, Company = self.classes.Employee, self.classes.Company

        c1 = sess.query(Company).filter_by(name="c1").one()
        e3 = sess.query(Employee).filter_by(name="emp3").one()
        e3.reports_to = None

        if expect_failure:
            # if foreign() isn't applied specifically to
            # employee_t.c.reports_to_id only, then
            # employee_t.c.company_id goes foreign as well and then
            # this happens
            assert_raises_message(
                AssertionError,
                "Dependency rule on column 'employee_t.company_id' "
                "tried to blank-out primary key column "
                "'employee_t.company_id'",
                sess.flush,
            )
        else:
            sess.flush()
            eq_(e3.company, c1)

    @testing.emits_warning("relationship .* will copy column ")
    def _test(self):
        self._test_no_warning(overwrites=True)

    def _test_no_warning(self, overwrites=False):
        configure_mappers()
        self._test_relationships()
        sess = fixture_session()
        self._setup_data(sess)
        self._test_lazy_relations(sess)
        self._test_join_aliasing(sess)
        self._test_no_overwrite(sess, expect_failure=overwrites)

    @testing.emits_warning("relationship .* will copy column ")
    def _assert_lazy_clauses(self):
        configure_mappers()
        Employee = self.classes.Employee
        self.assert_compile(
            Employee.employees.property.strategy._lazywhere,
            ":param_1 = employee_t.reports_to_id AND "
            ":param_2 = employee_t.company_id",
        )

        self.assert_compile(
            Employee.employees.property.strategy._rev_lazywhere,
            "employee_t.emp_id = :param_1 AND "
            "employee_t.company_id = :param_2",
        )

    def _test_relationships(self):
        Employee = self.classes.Employee
        employee_t = self.tables.employee_t
        eq_(
            set(Employee.employees.property.local_remote_pairs),
            {
                (employee_t.c.company_id, employee_t.c.company_id),
                (employee_t.c.emp_id, employee_t.c.reports_to_id),
            },
        )
        eq_(
            Employee.employees.property.remote_side,
            {employee_t.c.company_id, employee_t.c.reports_to_id},
        )
        eq_(
            set(Employee.reports_to.property.local_remote_pairs),
            {
                (employee_t.c.company_id, employee_t.c.company_id),
                (employee_t.c.reports_to_id, employee_t.c.emp_id),
            },
        )

    def _setup_data(self, sess):
        Employee, Company = self.classes.Employee, self.classes.Company

        c1 = Company("c1")
        c2 = Company("c2")

        e1 = Employee("emp1", c1, 1)
        e2 = Employee("emp2", c1, 2, e1)  # noqa
        e3 = Employee("emp3", c1, 3, e1)
        e4 = Employee("emp4", c1, 4, e3)  # noqa
        e5 = Employee("emp5", c2, 1)
        e6 = Employee("emp6", c2, 2, e5)  # noqa
        e7 = Employee("emp7", c2, 3, e5)  # noqa

        sess.add_all((c1, c2))
        sess.commit()
        sess.close()

    def _test_lazy_relations(self, sess):
        Employee, Company = self.classes.Employee, self.classes.Company

        c1 = sess.query(Company).filter_by(name="c1").one()
        c2 = sess.query(Company).filter_by(name="c2").one()
        e1 = sess.query(Employee).filter_by(name="emp1").one()
        e5 = sess.query(Employee).filter_by(name="emp5").one()

        test_e1 = sess.get(Employee, [c1.company_id, e1.emp_id])
        assert test_e1.name == "emp1", test_e1.name
        test_e5 = sess.get(Employee, [c2.company_id, e5.emp_id])
        assert test_e5.name == "emp5", test_e5.name
        assert [x.name for x in test_e1.employees] == ["emp2", "emp3"]
        assert sess.get(Employee, [c1.company_id, 3]).reports_to.name == "emp1"
        assert sess.get(Employee, [c2.company_id, 3]).reports_to.name == "emp5"

    def _test_join_aliasing(self, sess):
        Employee = self.classes.Employee
        ea = aliased(Employee)
        eq_(
            [
                n
                for n, in sess.query(Employee.name)
                .join(Employee.reports_to.of_type(ea))
                .filter(ea.name == "emp5")
                # broken until #7244 is fixed due to of_type() usage
                # .filter_by(name="emp5")
                .order_by(Employee.name)
            ],
            ["emp6", "emp7"],
        )


class CompositeJoinPartialFK(fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = "default"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column("x", Integer, primary_key=True),
            Column("y", Integer, primary_key=True),
            Column("z", Integer),
        )
        Table(
            "child",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("x", Integer),
            Column("y", Integer),
            Column("z", Integer),
            # note 'z' is not here
            sa.ForeignKeyConstraint(["x", "y"], ["parent.x", "parent.y"]),
        )

    @classmethod
    def setup_mappers(cls):
        parent, child = cls.tables.parent, cls.tables.child

        class Parent(cls.Comparable):
            pass

        class Child(cls.Comparable):
            pass

        cls.mapper_registry.map_imperatively(
            Parent,
            parent,
            properties={
                "children": relationship(
                    Child,
                    primaryjoin=and_(
                        parent.c.x == child.c.x,
                        parent.c.y == child.c.y,
                        parent.c.z == child.c.z,
                    ),
                )
            },
        )
        cls.mapper_registry.map_imperatively(Child, child)

    def test_joins_fully(self):
        Parent = self.classes.Parent

        self.assert_compile(
            Parent.children.property.strategy._lazywhere,
            ":param_1 = child.x AND :param_2 = child.y AND :param_3 = child.z",
        )


class SynonymsAsFKsTest(fixtures.MappedTest):
    """Syncrules on foreign keys that are also primary"""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "tableA",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer),
            test_needs_fk=True,
        )

        Table(
            "tableB",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("_a_id", Integer, key="a_id", primary_key=True),
            test_needs_fk=True,
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            @property
            def a_id(self):
                return self._a_id

    def test_synonym_fk(self):
        """test that active history is enabled on a
        one-to-many/one that has use_get==True"""

        tableB, A, B, tableA = (
            self.tables.tableB,
            self.classes.A,
            self.classes.B,
            self.tables.tableA,
        )

        self.mapper_registry.map_imperatively(
            B, tableB, properties={"a_id": synonym("_a_id", map_column=True)}
        )
        self.mapper_registry.map_imperatively(
            A,
            tableA,
            properties={
                "b": relationship(
                    B,
                    primaryjoin=(tableA.c.id == foreign(B.a_id)),
                    uselist=False,
                )
            },
        )

        sess = fixture_session()

        b = B(id=0)
        a = A(id=0, b=b)
        sess.add(a)
        sess.add(b)
        sess.flush()
        sess.expunge_all()

        assert a.b == b
        assert a.id == b.a_id
        assert a.id == b._a_id


class FKsAsPksTest(fixtures.MappedTest):
    """Syncrules on foreign keys that are also primary"""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "tableA",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("foo", Integer),
            test_needs_fk=True,
        )

        Table(
            "tableB",
            metadata,
            Column("id", Integer, ForeignKey("tableA.id"), primary_key=True),
            test_needs_fk=True,
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

    def test_onetoone_switch(self):
        """test that active history is enabled on a
        one-to-many/one that has use_get==True"""

        tableB, A, B, tableA = (
            self.tables.tableB,
            self.classes.A,
            self.classes.B,
            self.tables.tableA,
        )

        self.mapper_registry.map_imperatively(
            A,
            tableA,
            properties={
                "b": relationship(
                    B, cascade="all,delete-orphan", uselist=False
                )
            },
        )
        self.mapper_registry.map_imperatively(B, tableB)

        configure_mappers()
        assert A.b.property.strategy.use_get

        with fixture_session() as sess:
            a1 = A()
            sess.add(a1)
            sess.commit()

        with fixture_session() as sess:
            a1 = sess.query(A).first()
            a1.b = B()
            sess.commit()

    def test_no_delete_PK_AtoB(self):
        """A can't be deleted without B because B would have no PK value."""

        tableB, A, B, tableA = (
            self.tables.tableB,
            self.classes.A,
            self.classes.B,
            self.tables.tableA,
        )

        self.mapper_registry.map_imperatively(
            A,
            tableA,
            properties={"bs": relationship(B, cascade="save-update")},
        )
        self.mapper_registry.map_imperatively(B, tableB)

        a1 = A()
        a1.bs.append(B())
        with fixture_session() as sess:
            sess.add(a1)
            sess.flush()

            sess.delete(a1)

            assert_raises_message(
                AssertionError,
                "Dependency rule on column 'tableA.id' tried to blank-out "
                "primary key column 'tableB.id' on instance ",
                sess.flush,
            )

    def test_no_delete_PK_BtoA(self):
        tableB, A, B, tableA = (
            self.tables.tableB,
            self.classes.A,
            self.classes.B,
            self.tables.tableA,
        )

        self.mapper_registry.map_imperatively(
            B, tableB, properties={"a": relationship(A, cascade="save-update")}
        )
        self.mapper_registry.map_imperatively(A, tableA)

        b1 = B()
        a1 = A()
        b1.a = a1
        with fixture_session() as sess:
            sess.add(b1)
            sess.flush()
            b1.a = None
            assert_raises_message(
                AssertionError,
                "Dependency rule on column 'tableA.id' tried to blank-out "
                "primary key column 'tableB.id' on instance ",
                sess.flush,
            )

    @testing.fails_on_everything_except(
        "sqlite", testing.requires.mysql_non_strict
    )
    def test_nullPKsOK_BtoA(self, metadata, connection):
        A, tableA = self.classes.A, self.tables.tableA

        # postgresql can't handle a nullable PK column...?
        tableC = Table(
            "tablec",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "a_id",
                Integer,
                ForeignKey(tableA.c.id),
                primary_key=True,
                nullable=True,
            ),
        )
        tableC.create(connection)

        class C(BasicEntity):
            pass

        self.mapper_registry.map_imperatively(
            C, tableC, properties={"a": relationship(A, cascade="save-update")}
        )
        self.mapper_registry.map_imperatively(A, tableA)

        c1 = C()
        c1.id = 5
        c1.a = None
        with Session(connection) as sess:
            sess.add(c1)
            # test that no error is raised.
            sess.flush()

    @testing.combinations(
        "save-update, delete",
        # "save-update, delete-orphan",
        "save-update, delete, delete-orphan",
    )
    def test_delete_cascade_BtoA(self, cascade):
        """No 'blank the PK' error when the child is to
        be deleted as part of a cascade"""

        tableB, A, B, tableA = (
            self.tables.tableB,
            self.classes.A,
            self.classes.B,
            self.tables.tableA,
        )

        self.mapper_registry.map_imperatively(
            B,
            tableB,
            properties={
                "a": relationship(A, cascade=cascade, single_parent=True)
            },
        )
        self.mapper_registry.map_imperatively(A, tableA)

        b1 = B()
        a1 = A()
        b1.a = a1
        with fixture_session() as sess:
            sess.add(b1)
            sess.flush()
            sess.delete(b1)
            sess.flush()
            assert a1 not in sess
            assert b1 not in sess

    @testing.combinations(
        "save-update, delete",
        # "save-update, delete-orphan",
        "save-update, delete, delete-orphan",
    )
    def test_delete_cascade_AtoB(self, cascade):
        """No 'blank the PK' error when the child is to
        be deleted as part of a cascade"""

        tableB, A, B, tableA = (
            self.tables.tableB,
            self.classes.A,
            self.classes.B,
            self.tables.tableA,
        )

        self.mapper_registry.map_imperatively(
            A, tableA, properties={"bs": relationship(B, cascade=cascade)}
        )
        self.mapper_registry.map_imperatively(B, tableB)

        a1 = A()
        b1 = B()
        a1.bs.append(b1)
        with fixture_session() as sess:
            sess.add(a1)
            sess.flush()

            sess.delete(a1)
            sess.flush()
            assert a1 not in sess
            assert b1 not in sess

    def test_delete_manual_AtoB(self):
        tableB, A, B, tableA = (
            self.tables.tableB,
            self.classes.A,
            self.classes.B,
            self.tables.tableA,
        )

        self.mapper_registry.map_imperatively(
            A, tableA, properties={"bs": relationship(B, cascade="none")}
        )
        self.mapper_registry.map_imperatively(B, tableB)

        a1 = A()
        b1 = B()
        a1.bs.append(b1)
        with fixture_session() as sess:
            sess.add(a1)
            sess.add(b1)
            sess.flush()

            sess.delete(a1)
            sess.delete(b1)
            sess.flush()
            assert a1 not in sess
            assert b1 not in sess

    def test_delete_manual_BtoA(self):
        tableB, A, B, tableA = (
            self.tables.tableB,
            self.classes.A,
            self.classes.B,
            self.tables.tableA,
        )

        self.mapper_registry.map_imperatively(
            B, tableB, properties={"a": relationship(A, cascade="none")}
        )
        self.mapper_registry.map_imperatively(A, tableA)

        b1 = B()
        a1 = A()
        b1.a = a1
        with fixture_session() as sess:
            sess.add(b1)
            sess.add(a1)
            sess.flush()
            sess.delete(b1)
            sess.delete(a1)
            sess.flush()
            assert a1 not in sess
            assert b1 not in sess


class UniqueColReferenceSwitchTest(fixtures.MappedTest):
    """test a relationship based on a primary
    join against a unique non-pk column"""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "table_a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("ident", String(10), nullable=False, unique=True),
        )

        Table(
            "table_b",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column(
                "a_ident",
                String(10),
                ForeignKey("table_a.ident"),
                nullable=False,
            ),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

        class B(cls.Comparable):
            pass

    def test_switch_parent(self):
        A, B, table_b, table_a = (
            self.classes.A,
            self.classes.B,
            self.tables.table_b,
            self.tables.table_a,
        )

        self.mapper_registry.map_imperatively(A, table_a)
        self.mapper_registry.map_imperatively(
            B, table_b, properties={"a": relationship(A, backref="bs")}
        )

        session = fixture_session()
        a1, a2 = A(ident="uuid1"), A(ident="uuid2")
        session.add_all([a1, a2])
        a1.bs = [B(), B()]
        session.flush()
        session.expire_all()
        a1, a2 = session.query(A).all()

        for b in list(a1.bs):
            b.a = a2
        session.delete(a1)
        session.flush()


class RelationshipToSelectableTest(fixtures.MappedTest):
    """Test a map to a select that relates to a map to the table."""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "items",
            metadata,
            Column(
                "item_policy_num",
                String(10),
                primary_key=True,
                key="policyNum",
            ),
            Column(
                "item_policy_eff_date",
                sa.Date,
                primary_key=True,
                key="policyEffDate",
            ),
            Column("item_type", String(20), primary_key=True, key="type"),
            Column(
                "item_id",
                Integer,
                primary_key=True,
                key="id",
                autoincrement=False,
            ),
        )

    def test_basic(self):
        items = self.tables.items

        class Container(BasicEntity):
            pass

        class LineItem(BasicEntity):
            pass

        container_select = (
            sa.select(items.c.policyNum, items.c.policyEffDate, items.c.type)
            .distinct()
            .alias("container_select")
        )

        self.mapper_registry.map_imperatively(LineItem, items)

        self.mapper_registry.map_imperatively(
            Container,
            container_select,
            properties=dict(
                lineItems=relationship(
                    LineItem,
                    lazy="select",
                    cascade="all, delete-orphan",
                    order_by=sa.asc(items.c.id),
                    primaryjoin=sa.and_(
                        container_select.c.policyNum == items.c.policyNum,
                        container_select.c.policyEffDate
                        == items.c.policyEffDate,
                        container_select.c.type == items.c.type,
                    ),
                    foreign_keys=[
                        items.c.policyNum,
                        items.c.policyEffDate,
                        items.c.type,
                    ],
                )
            ),
        )

        session = fixture_session()
        con = Container()
        con.policyNum = "99"
        con.policyEffDate = datetime.date.today()
        con.type = "TESTER"
        session.add(con)
        for i in range(0, 10):
            li = LineItem()
            li.id = i
            con.lineItems.append(li)
            session.add(li)
        session.flush()
        session.expunge_all()
        newcon = (
            session.query(Container).order_by(container_select.c.type).first()
        )
        assert con.policyNum == newcon.policyNum
        assert len(newcon.lineItems) == 10
        for old, new in zip(con.lineItems, newcon.lineItems):
            eq_(old.id, new.id)


class FKEquatedToConstantTest(fixtures.MappedTest):
    """test a relationship with a non-column entity in the primary join,
    is not viewonly, and also has the non-column's clause mentioned in the
    foreign keys list.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "tags",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
        )

        Table(
            "tag_foo",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("tagid", Integer),
            Column("data", String(50)),
        )

    def test_basic(self):
        tag_foo, tags = self.tables.tag_foo, self.tables.tags

        class Tag(ComparableEntity):
            pass

        class TagInstance(ComparableEntity):
            pass

        self.mapper_registry.map_imperatively(
            Tag,
            tags,
            properties={
                "foo": relationship(
                    TagInstance,
                    primaryjoin=sa.and_(
                        tag_foo.c.data == "iplc_case",
                        tag_foo.c.tagid == tags.c.id,
                    ),
                    foreign_keys=[tag_foo.c.tagid, tag_foo.c.data],
                )
            },
        )

        self.mapper_registry.map_imperatively(TagInstance, tag_foo)

        sess = fixture_session()
        t1 = Tag(data="some tag")
        t1.foo.append(TagInstance(data="iplc_case"))
        t1.foo.append(TagInstance(data="not_iplc_case"))
        sess.add(t1)
        sess.flush()
        sess.expunge_all()

        # relationship works
        eq_(
            sess.query(Tag).all(),
            [Tag(data="some tag", foo=[TagInstance(data="iplc_case")])],
        )

        # both TagInstances were persisted
        eq_(
            sess.query(TagInstance).order_by(TagInstance.data).all(),
            [TagInstance(data="iplc_case"), TagInstance(data="not_iplc_case")],
        )


class BackrefPropagatesForwardsArgs(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
        )
        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", Integer),
            Column("email", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

    def test_backref(self):
        User, Address, users, addresses = (
            self.classes.User,
            self.classes.Address,
            self.tables.users,
            self.tables.addresses,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    primaryjoin=addresses.c.user_id == users.c.id,
                    foreign_keys=addresses.c.user_id,
                    backref="user",
                )
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sess = fixture_session()
        u1 = User(name="u1", addresses=[Address(email="a1")])
        sess.add(u1)
        sess.commit()
        eq_(
            sess.query(Address).all(),
            [Address(email="a1", user=User(name="u1"))],
        )


class AmbiguousJoinInterpretedAsSelfRef(fixtures.MappedTest):
    """test ambiguous joins due to FKs on both sides treated as
    self-referential.

    this mapping is very similar to that of
    test/orm/inheritance/query.py
    SelfReferentialTestJoinedToBase , except that inheritance is
    not used here.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "subscriber",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )

        Table(
            "address",
            metadata,
            Column(
                "subscriber_id",
                Integer,
                ForeignKey("subscriber.id"),
                primary_key=True,
            ),
            Column("type", String(1), primary_key=True),
        )

    @classmethod
    def setup_mappers(cls):
        subscriber, address = cls.tables.subscriber, cls.tables.address

        subscriber_and_address = subscriber.join(
            address,
            and_(
                address.c.subscriber_id == subscriber.c.id,
                address.c.type.in_(["A", "B", "C"]),
            ),
        )

        class Address(cls.Comparable):
            pass

        class Subscriber(cls.Comparable):
            pass

        cls.mapper_registry.map_imperatively(Address, address)

        cls.mapper_registry.map_imperatively(
            Subscriber,
            subscriber_and_address,
            properties={
                "id": [subscriber.c.id, address.c.subscriber_id],
                "addresses": relationship(
                    Address, backref=backref("customer")
                ),
            },
        )

    def test_mapping(self):
        Subscriber, Address = self.classes.Subscriber, self.classes.Address

        sess = fixture_session()
        assert Subscriber.addresses.property.direction is ONETOMANY
        assert Address.customer.property.direction is MANYTOONE

        s1 = Subscriber(
            type="A", addresses=[Address(type="D"), Address(type="E")]
        )
        a1 = Address(type="B", customer=Subscriber(type="C"))

        assert s1.addresses[0].customer is s1
        assert a1.customer.addresses[0] is a1

        sess.add_all([s1, a1])

        sess.flush()
        sess.expunge_all()

        eq_(
            sess.query(Subscriber).order_by(Subscriber.type).all(),
            [
                Subscriber(id=1, type="A"),
                Subscriber(id=2, type="B"),
                Subscriber(id=2, type="C"),
            ],
        )


class ManualBackrefTest(_fixtures.FixtureTest):
    """Test explicit relationships that are backrefs to each other."""

    run_inserts = None

    def test_o2m(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(Address, back_populates="user")
            },
        )

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "user": relationship(User, back_populates="addresses")
            },
        )

        sess = fixture_session()

        u1 = User(name="u1")
        a1 = Address(email_address="foo")
        u1.addresses.append(a1)
        assert a1.user is u1

        sess.add(u1)
        sess.flush()
        sess.expire_all()
        assert sess.query(Address).one() is a1
        assert a1.user is u1
        assert a1 in u1.addresses

    @testing.variation(
        "argtype", ["str", "callable_str", "prop", "callable_prop"]
    )
    def test_o2m_with_callable(self, argtype):
        """test #10050"""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        if argtype.str:
            abp, ubp = "user", "addresses"
        elif argtype.callable_str:
            abp, ubp = lambda: "user", lambda: "addresses"
        elif argtype.prop:
            abp, ubp = lambda: "user", lambda: "addresses"
        elif argtype.callable_prop:
            abp, ubp = lambda: Address.user, lambda: User.addresses
        else:
            argtype.fail()

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(Address, back_populates=abp)
            },
        )

        if argtype.prop:
            ubp = User.addresses

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={"user": relationship(User, back_populates=ubp)},
        )

        sess = fixture_session()

        u1 = User(name="u1")
        a1 = Address(email_address="foo")
        u1.addresses.append(a1)
        assert a1.user is u1

        sess.add(u1)
        sess.flush()
        sess.expire_all()
        assert sess.query(Address).one() is a1
        assert a1.user is u1
        assert a1 in u1.addresses

    @testing.variation("argtype", ["plain", "callable"])
    def test_invalid_backref_type(self, argtype):
        """test #10050"""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        if argtype.plain:
            abp, ubp = object(), "addresses"
        elif argtype.callable:
            abp, ubp = lambda: object(), lambda: "addresses"
        else:
            argtype.fail()

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(Address, back_populates=abp)
            },
        )

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={"user": relationship(User, back_populates=ubp)},
        )

        with expect_raises_message(
            exc.ArgumentError, r"Invalid back_populates value: <object"
        ):
            self.mapper_registry.configure()

    def test_invalid_key(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(Address, back_populates="userr")
            },
        )

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "user": relationship(User, back_populates="addresses")
            },
        )

        assert_raises(sa.exc.InvalidRequestError, configure_mappers)

    def test_invalid_target(self):
        addresses, Dingaling, User, dingalings, Address, users = (
            self.tables.addresses,
            self.classes.Dingaling,
            self.classes.User,
            self.tables.dingalings,
            self.classes.Address,
            self.tables.users,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(Address, back_populates="dingaling")
            },
        )

        self.mapper_registry.map_imperatively(Dingaling, dingalings)
        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={"dingaling": relationship(Dingaling)},
        )

        assert_raises_message(
            sa.exc.ArgumentError,
            r"reverse_property 'dingaling' on relationship "
            r"User.addresses references "
            r"relationship Address.dingaling, "
            r"which does not "
            r"reference mapper Mapper\[User\(users\)\]",
            configure_mappers,
        )

    def test_back_propagates_not_relationship(self):
        addr, Addr, users, User = (
            self.tables.addresses,
            self.classes.Address,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(Addr, back_populates="user_id")
            },
        )

        self.mapper_registry.map_imperatively(
            Addr,
            addr,
            properties={
                "users": relationship(User, back_populates="addresses")
            },
        )

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "back_populates on relationship 'User.addresses' refers to "
            "attribute 'Address.user_id' that is not a relationship.  "
            "The back_populates parameter should refer to the name of "
            "a relationship on the target class.",
            configure_mappers,
        )


class NoLoadBackPopulates(_fixtures.FixtureTest):
    """test the noload stratgegy which unlike others doesn't use
    lazyloader to set up instrumentation"""

    def test_o2m(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, back_populates="user", lazy="noload"
                )
            },
        )

        self.mapper_registry.map_imperatively(
            Address, addresses, properties={"user": relationship(User)}
        )

        u1 = User()
        a1 = Address()
        u1.addresses.append(a1)
        is_(a1.user, u1)

    def test_m2o(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User, users, properties={"addresses": relationship(Address)}
        )

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User, back_populates="addresses", lazy="noload"
                )
            },
        )

        u1 = User()
        a1 = Address()
        a1.user = u1
        in_(a1, u1.addresses)


class JoinConditionErrorTest(fixtures.TestBase):
    def test_clauseelement_pj(self, registry):
        Base = registry.generate_base()

        class C1(Base):
            __tablename__ = "c1"
            id = Column("id", Integer, primary_key=True)

        class C2(Base):
            __tablename__ = "c2"
            id = Column("id", Integer, primary_key=True)
            c1id = Column("c1id", Integer, ForeignKey("c1.id"))
            c2 = relationship(C1, primaryjoin=C1.id)

        assert_raises(sa.exc.ArgumentError, configure_mappers)

    def test_clauseelement_pj_false(self, registry):
        Base = registry.generate_base()

        class C1(Base):
            __tablename__ = "c1"
            id = Column("id", Integer, primary_key=True)

        class C2(Base):
            __tablename__ = "c2"
            id = Column("id", Integer, primary_key=True)
            c1id = Column("c1id", Integer, ForeignKey("c1.id"))
            c2 = relationship(C1, primaryjoin="x" == "y")

        assert_raises(sa.exc.ArgumentError, configure_mappers)

    def test_only_column_elements(self, registry):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("id", Integer, primary_key=True),
            Column("foo_id", Integer, ForeignKey("t2.id")),
        )
        t2 = Table("t2", m, Column("id", Integer, primary_key=True))

        class C1:
            pass

        class C2:
            pass

        registry.map_imperatively(
            C1,
            t1,
            properties={"c2": relationship(C2, primaryjoin=t1.join(t2))},
        )
        registry.map_imperatively(C2, t2)
        assert_raises(sa.exc.ArgumentError, configure_mappers)

    @testing.combinations(
        ("remote_side", ["c1.id"]),
        ("remote_side", ["id"]),
        ("foreign_keys", ["c1id"]),
        ("foreign_keys", ["C2.c1id"]),
        ("order_by", ["id"]),
        argnames="argname, arg",
    )
    def test_invalid_string_args(self, registry, argname, arg):
        kw = {argname: arg}
        Base = registry.generate_base()

        class C1(Base):
            __tablename__ = "c1"
            id = Column("id", Integer, primary_key=True)

        class C2(Base):
            __tablename__ = "c2"
            id_ = Column("id", Integer, primary_key=True)
            c1id = Column("c1id", Integer, ForeignKey("c1.id"))
            c2 = relationship(C1, **kw)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Column expression expected "
            "for argument '%s'; got '%s'" % (argname, arg[0]),
            configure_mappers,
        )

    def test_fk_error_not_raised_unrelated(self, registry):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("id", Integer, primary_key=True),
            Column("foo_id", Integer, ForeignKey("t2.nonexistent_id")),
        )
        t2 = Table("t2", m, Column("id", Integer, primary_key=True))  # noqa

        t3 = Table(
            "t3",
            m,
            Column("id", Integer, primary_key=True),
            Column("t1id", Integer, ForeignKey("t1.id")),
        )

        class C1:
            pass

        class C2:
            pass

        registry.map_imperatively(C1, t1, properties={"c2": relationship(C2)})
        registry.map_imperatively(C2, t3)
        assert C1.c2.property.primaryjoin.compare(t1.c.id == t3.c.t1id)

    @testing.combinations(
        "annotation", "local_remote", argnames="remote_anno_type"
    )
    @testing.combinations("orm_col", "core_col", argnames="use_col_from")
    def test_no_remote_on_local_only_cols(
        self, decl_base, remote_anno_type, use_col_from
    ):
        """test #7094.

        a warning should be emitted for an inappropriate remote_side argument

        """

        class A(decl_base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            data = Column(String)

            if remote_anno_type == "annotation":
                if use_col_from == "core_col":
                    bs = relationship(
                        "B",
                        primaryjoin=lambda: remote(A.__table__.c.id)
                        == B.__table__.c.a_id,
                    )
                elif use_col_from == "orm_col":
                    bs = relationship(
                        "B", primaryjoin="remote(A.id) == B.a_id"
                    )
            elif remote_anno_type == "local_remote":
                if use_col_from == "core_col":
                    bs = relationship(
                        "B", remote_side=lambda: A.__table__.c.id
                    )
                elif use_col_from == "orm_col":
                    bs = relationship("B", remote_side="A.id")

        class B(decl_base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))

        with expect_warnings(
            r"Expression a.id is marked as 'remote', but these column\(s\) "
            r"are local to the local side. "
        ):
            decl_base.registry.configure()

    def test_join_error_raised(self, registry):
        m = MetaData()
        t1 = Table("t1", m, Column("id", Integer, primary_key=True))
        t2 = Table("t2", m, Column("id", Integer, primary_key=True))  # noqa

        t3 = Table(
            "t3",
            m,
            Column("id", Integer, primary_key=True),
            Column("t1id", Integer),
        )

        class C1:
            pass

        class C2:
            pass

        registry.map_imperatively(C1, t1, properties={"c2": relationship(C2)})
        registry.map_imperatively(C2, t3)

        assert_raises(sa.exc.ArgumentError, configure_mappers)

    def teardown_test(self):
        clear_mappers()


class TypeMatchTest(fixtures.MappedTest):
    """test errors raised when trying to add items
    whose type is not handled by a relationship"""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column(
                "aid", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("adata", String(30)),
        )
        Table(
            "b",
            metadata,
            Column(
                "bid", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("a_id", Integer, ForeignKey("a.aid")),
            Column("bdata", String(30)),
        )
        Table(
            "c",
            metadata,
            Column(
                "cid", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("b_id", Integer, ForeignKey("b.bid")),
            Column("cdata", String(30)),
        )
        Table(
            "d",
            metadata,
            Column(
                "did", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("a_id", Integer, ForeignKey("a.aid")),
            Column("ddata", String(30)),
        )

    def test_o2m_oncascade(self):
        a, c, b = (self.tables.a, self.tables.c, self.tables.b)

        class A(BasicEntity):
            pass

        class B(BasicEntity):
            pass

        class C(BasicEntity):
            pass

        self.mapper_registry.map_imperatively(
            A, a, properties={"bs": relationship(B)}
        )
        self.mapper_registry.map_imperatively(B, b)
        self.mapper_registry.map_imperatively(C, c)

        a1 = A()
        b1 = B()
        c1 = C()
        a1.bs.append(b1)
        a1.bs.append(c1)
        sess = fixture_session()
        try:
            sess.add(a1)
            assert False
        except AssertionError as err:
            eq_(
                str(err),
                "Attribute 'bs' on class '%s' doesn't handle "
                "objects of type '%s'" % (A, C),
            )

    def test_o2m_onflush(self):
        a, c, b = (self.tables.a, self.tables.c, self.tables.b)

        class A(BasicEntity):
            pass

        class B(BasicEntity):
            pass

        class C(BasicEntity):
            pass

        self.mapper_registry.map_imperatively(
            A, a, properties={"bs": relationship(B, cascade="none")}
        )
        self.mapper_registry.map_imperatively(B, b)
        self.mapper_registry.map_imperatively(C, c)

        a1 = A()
        b1 = B()
        c1 = C()
        a1.bs.append(b1)
        a1.bs.append(c1)
        sess = fixture_session()
        sess.add(a1)
        sess.add(b1)
        sess.add(c1)
        assert_raises_message(
            sa.orm.exc.FlushError, "Attempting to flush an item", sess.flush
        )

    def test_o2m_nopoly_onflush(self):
        a, c, b = (self.tables.a, self.tables.c, self.tables.b)

        class A(BasicEntity):
            pass

        class B(BasicEntity):
            pass

        class C(B):
            pass

        self.mapper_registry.map_imperatively(
            A, a, properties={"bs": relationship(B, cascade="none")}
        )
        self.mapper_registry.map_imperatively(B, b)
        self.mapper_registry.map_imperatively(C, c, inherits=B)

        a1 = A()
        b1 = B()
        c1 = C()
        a1.bs.append(b1)
        a1.bs.append(c1)
        sess = fixture_session()
        sess.add(a1)
        sess.add(b1)
        sess.add(c1)
        assert_raises_message(
            sa.orm.exc.FlushError, "Attempting to flush an item", sess.flush
        )

    def test_m2o_nopoly_onflush(self):
        a, b, d = (self.tables.a, self.tables.b, self.tables.d)

        class A(BasicEntity):
            pass

        class B(A):
            pass

        class D(BasicEntity):
            pass

        self.mapper_registry.map_imperatively(A, a)
        self.mapper_registry.map_imperatively(B, b, inherits=A)
        self.mapper_registry.map_imperatively(
            D, d, properties={"a": relationship(A, cascade="none")}
        )
        b1 = B()
        d1 = D()
        d1.a = b1
        sess = fixture_session()
        sess.add(b1)
        sess.add(d1)
        assert_raises_message(
            sa.orm.exc.FlushError, "Attempting to flush an item", sess.flush
        )

    def test_m2o_oncascade(self):
        a, b, d = (self.tables.a, self.tables.b, self.tables.d)

        class A(BasicEntity):
            pass

        class B(BasicEntity):
            pass

        class D(BasicEntity):
            pass

        self.mapper_registry.map_imperatively(A, a)
        self.mapper_registry.map_imperatively(B, b)
        self.mapper_registry.map_imperatively(
            D, d, properties={"a": relationship(A)}
        )
        b1 = B()
        d1 = D()
        d1.a = b1
        sess = fixture_session()
        assert_raises_message(
            AssertionError, "doesn't handle objects of type", sess.add, d1
        )


class TypedAssociationTable(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        class MySpecialType(sa.types.TypeDecorator):
            impl = String
            cache_ok = True

            def process_bind_param(self, value, dialect):
                return "lala" + value

            def process_result_value(self, value, dialect):
                return value[4:]

        Table(
            "t1",
            metadata,
            Column("col1", MySpecialType(30), primary_key=True),
            Column("col2", String(30)),
        )
        Table(
            "t2",
            metadata,
            Column("col1", MySpecialType(30), primary_key=True),
            Column("col2", String(30)),
        )
        Table(
            "t3",
            metadata,
            Column("t1c1", MySpecialType(30), ForeignKey("t1.col1")),
            Column("t2c1", MySpecialType(30), ForeignKey("t2.col1")),
        )

    def test_m2m(self):
        """Many-to-many tables with special types for candidate keys."""

        t2, t3, t1 = (self.tables.t2, self.tables.t3, self.tables.t1)

        class T1(BasicEntity):
            pass

        class T2(BasicEntity):
            pass

        self.mapper_registry.map_imperatively(T2, t2)
        self.mapper_registry.map_imperatively(
            T1,
            t1,
            properties={"t2s": relationship(T2, secondary=t3, backref="t1s")},
        )

        a = T1()
        a.col1 = "aid"
        b = T2()
        b.col1 = "bid"
        c = T2()
        c.col1 = "cid"
        a.t2s.append(b)
        a.t2s.append(c)
        sess = fixture_session()
        sess.add(a)
        sess.flush()

        eq_(
            sess.connection().scalar(select(func.count("*")).select_from(t3)),
            2,
        )

        a.t2s.remove(c)
        sess.flush()

        eq_(
            sess.connection().scalar(select(func.count("*")).select_from(t3)),
            1,
        )


class CustomOperatorTest(fixtures.MappedTest, AssertsCompiledSQL):
    """test op() in conjunction with join conditions"""

    run_create_tables = run_deletes = None

    __dialect__ = "default"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", String(50)),
        )
        Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", String(50)),
        )

    def test_join_on_custom_op_legacy_is_comparison(self):
        class A(BasicEntity):
            pass

        class B(BasicEntity):
            pass

        self.mapper_registry.map_imperatively(
            A,
            self.tables.a,
            properties={
                "bs": relationship(
                    B,
                    primaryjoin=self.tables.a.c.foo.op(
                        "&*", is_comparison=True
                    )(foreign(self.tables.b.c.foo)),
                    viewonly=True,
                )
            },
        )
        self.mapper_registry.map_imperatively(B, self.tables.b)
        self.assert_compile(
            fixture_session().query(A).join(A.bs),
            "SELECT a.id AS a_id, a.foo AS a_foo "
            "FROM a JOIN b ON a.foo &* b.foo",
        )

    def test_join_on_custom_bool_op(self):
        class A(BasicEntity):
            pass

        class B(BasicEntity):
            pass

        self.mapper_registry.map_imperatively(
            A,
            self.tables.a,
            properties={
                "bs": relationship(
                    B,
                    primaryjoin=self.tables.a.c.foo.bool_op("&*")(
                        foreign(self.tables.b.c.foo)
                    ),
                    viewonly=True,
                )
            },
        )
        self.mapper_registry.map_imperatively(B, self.tables.b)
        self.assert_compile(
            fixture_session().query(A).join(A.bs),
            "SELECT a.id AS a_id, a.foo AS a_foo "
            "FROM a JOIN b ON a.foo &* b.foo",
        )


class ViewOnlyHistoryTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(40)),
        )
        Table(
            "t2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(40)),
            Column("t1id", Integer, ForeignKey("t1.id")),
        )

    def _assert_fk(self, a1, b1, is_set):
        s = Session(testing.db)
        s.add_all([a1, b1])
        s.flush()

        if is_set:
            eq_(b1.t1id, a1.id)
        else:
            eq_(b1.t1id, None)

        return s

    def test_o2m_viewonly_oneside(self):
        class A(ComparableEntity):
            pass

        class B(ComparableEntity):
            pass

        self.mapper_registry.map_imperatively(
            A,
            self.tables.t1,
            properties={
                "bs": relationship(
                    B, viewonly=True, backref=backref("a", viewonly=False)
                )
            },
        )
        self.mapper_registry.map_imperatively(B, self.tables.t2)

        configure_mappers()

        a1 = A()
        b1 = B()
        a1.bs.append(b1)
        assert b1.a is None
        assert not inspect(a1).attrs.bs.history.has_changes()
        assert not inspect(b1).attrs.a.history.has_changes()

        sess = self._assert_fk(a1, b1, False)

        a1.bs.remove(b1)
        assert a1 not in sess.dirty
        assert b1 not in sess.dirty

    def test_m2o_viewonly_oneside(self):
        class A(ComparableEntity):
            pass

        class B(ComparableEntity):
            pass

        self.mapper_registry.map_imperatively(
            A,
            self.tables.t1,
            properties={
                "bs": relationship(
                    B, viewonly=False, backref=backref("a", viewonly=True)
                )
            },
        )
        self.mapper_registry.map_imperatively(B, self.tables.t2)

        configure_mappers()

        a1 = A()
        b1 = B()
        b1.a = a1
        assert b1 not in a1.bs
        assert not inspect(a1).attrs.bs.history.has_changes()
        assert not inspect(b1).attrs.a.history.has_changes()

        sess = self._assert_fk(a1, b1, False)

        b1.a = None
        assert a1 not in sess.dirty
        assert b1 not in sess.dirty

    def test_o2m_viewonly_only(self):
        class A(ComparableEntity):
            pass

        class B(ComparableEntity):
            pass

        self.mapper_registry.map_imperatively(
            A,
            self.tables.t1,
            properties={"bs": relationship(B, viewonly=True)},
        )
        self.mapper_registry.map_imperatively(B, self.tables.t2)

        a1 = A()
        b1 = B()
        a1.bs.append(b1)
        assert not inspect(a1).attrs.bs.history.has_changes()

        self._assert_fk(a1, b1, False)

    def test_m2o_viewonly_only(self):
        class A(ComparableEntity):
            pass

        class B(ComparableEntity):
            pass

        self.mapper_registry.map_imperatively(A, self.tables.t1)
        self.mapper_registry.map_imperatively(
            B, self.tables.t2, properties={"a": relationship(A, viewonly=True)}
        )

        a1 = A()
        b1 = B()
        b1.a = a1
        assert not inspect(b1).attrs.a.history.has_changes()

        self._assert_fk(a1, b1, False)


class ViewOnlyM2MBackrefTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(40)),
        )
        Table(
            "t2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(40)),
        )
        Table(
            "t1t2",
            metadata,
            Column("t1id", Integer, ForeignKey("t1.id"), primary_key=True),
            Column("t2id", Integer, ForeignKey("t2.id"), primary_key=True),
        )

    def test_viewonly(self):
        t1t2, t2, t1 = (self.tables.t1t2, self.tables.t2, self.tables.t1)

        class A(ComparableEntity):
            pass

        class B(ComparableEntity):
            pass

        self.mapper_registry.map_imperatively(
            A,
            t1,
            properties={
                "bs": relationship(
                    B, secondary=t1t2, backref=backref("as_", viewonly=True)
                )
            },
        )
        self.mapper_registry.map_imperatively(B, t2)

        configure_mappers()

        sess = fixture_session()
        a1 = A()
        b1 = B(as_=[a1])

        assert not inspect(b1).attrs.as_.history.has_changes()

        sess.add(a1)
        sess.flush()
        eq_(sess.query(A).first(), A(bs=[]))
        eq_(sess.query(B).first(), None)


class ViewOnlyOverlappingNames(fixtures.MappedTest):
    """'viewonly' mappings with overlapping PK column names."""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(40)),
        )
        Table(
            "t2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(40)),
            Column("t1id", Integer, ForeignKey("t1.id")),
        )
        Table(
            "t3",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(40)),
            Column("t2id", Integer, ForeignKey("t2.id")),
        )

    def test_three_table_view(self):
        """A three table join with overlapping PK names.

        A third table is pulled into the primary join condition using
        overlapping PK column names and should not produce 'conflicting column'
        error.

        """

        t2, t3, t1 = (self.tables.t2, self.tables.t3, self.tables.t1)

        class C1(BasicEntity):
            pass

        class C2(BasicEntity):
            pass

        class C3(BasicEntity):
            pass

        self.mapper_registry.map_imperatively(
            C1,
            t1,
            properties={
                "t2s": relationship(C2),
                "t2_view": relationship(
                    C2,
                    viewonly=True,
                    primaryjoin=sa.and_(
                        t1.c.id == t2.c.t1id,
                        t3.c.t2id == t2.c.id,
                        t3.c.data == t1.c.data,
                    ),
                ),
            },
        )
        self.mapper_registry.map_imperatively(C2, t2)
        self.mapper_registry.map_imperatively(
            C3, t3, properties={"t2": relationship(C2)}
        )

        c1 = C1()
        c1.data = "c1data"
        c2a = C2()
        c1.t2s.append(c2a)
        c2b = C2()
        c1.t2s.append(c2b)
        c3 = C3()
        c3.data = "c1data"
        c3.t2 = c2b
        sess = fixture_session()
        sess.add(c1)
        sess.add(c3)
        sess.flush()
        sess.expunge_all()

        c1 = sess.get(C1, c1.id)
        assert {x.id for x in c1.t2s} == {c2a.id, c2b.id}
        assert {x.id for x in c1.t2_view} == {c2b.id}


class ViewOnlySyncBackref(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(40)),
        )
        Table(
            "t2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(40)),
            Column("t1id", Integer, ForeignKey("t1.id")),
        )

    class Case:
        def __init__(
            self,
            Ba_err=False,
            Abs_err=False,
            map_err=False,
            Ba_evt=False,
            Abs_evt=False,
        ):
            self.B_a_init_error = Ba_err
            self.A_bs_init_error = Abs_err
            self.map_error = map_err
            self.B_a_event = Ba_evt
            self.A_bs_event = Abs_evt

        def __repr__(self):
            return str(self.__dict__)

    cases = {
        # (B_a_view, B_a_sync, A_bs_view, A_bs_sync)
        (0, 0, 0, 0): Case(),
        (0, 0, 0, 1): Case(Abs_evt=1),
        (0, 0, 1, 0): Case(),
        (0, 0, 1, 1): Case(Abs_err=1),
        (0, 1, 0, 0): Case(Ba_evt=1),
        (0, 1, 0, 1): Case(Ba_evt=1, Abs_evt=1),
        (0, 1, 1, 0): Case(map_err="BA"),
        (0, 1, 1, 1): Case(Abs_err=1),
        (1, 0, 0, 0): Case(),
        (1, 0, 0, 1): Case(map_err="AB"),
        (1, 0, 1, 0): Case(),
        (1, 0, 1, 1): Case(Abs_err=1),
        (1, 1, 0, 0): Case(Ba_err=1),
        (1, 1, 0, 1): Case(Ba_err=1),
        (1, 1, 1, 0): Case(Ba_err=1),
        (1, 1, 1, 1): Case(Abs_err=1),
        (0, None, 0, 0): Case(Ba_evt=1),
        (0, None, 0, 1): Case(Ba_evt=1, Abs_evt=1),
        (0, None, 1, 0): Case(),
        (0, None, 1, 1): Case(Abs_err=1),
        (1, None, 0, 0): Case(),
        (1, None, 0, 1): Case(map_err="AB"),
        (1, None, 1, 0): Case(),
        (1, None, 1, 1): Case(Abs_err=1),
        (0, 0, 0, None): Case(Abs_evt=1),
        (0, 0, 1, None): Case(),
        (0, 1, 0, None): Case(Ba_evt=1, Abs_evt=1),
        (0, 1, 1, None): Case(map_err="BA"),
        (1, 0, 0, None): Case(),
        (1, 0, 1, None): Case(),
        (1, 1, 0, None): Case(Ba_err=1),
        (1, 1, 1, None): Case(Ba_err=1),
        (0, None, 0, None): Case(Ba_evt=1, Abs_evt=1),
        (0, None, 1, None): Case(),
        (1, None, 0, None): Case(),
        (1, None, 1, None): Case(),
    }

    @testing.combinations(True, False, None, argnames="A_bs_sync")
    @testing.combinations(True, False, argnames="A_bs_view")
    @testing.combinations(True, False, None, argnames="B_a_sync")
    @testing.combinations(True, False, argnames="B_a_view")
    def test_case(self, B_a_view, B_a_sync, A_bs_view, A_bs_sync):
        class A(ComparableEntity):
            pass

        class B(ComparableEntity):
            pass

        case = self.cases[(B_a_view, B_a_sync, A_bs_view, A_bs_sync)]
        print(
            {
                "B_a_view": B_a_view,
                "B_a_sync": B_a_sync,
                "A_bs_view": A_bs_view,
                "A_bs_sync": A_bs_sync,
            },
            case,
        )

        def rel():
            return relationship(
                B,
                viewonly=A_bs_view,
                sync_backref=A_bs_sync,
                backref=backref("a", viewonly=B_a_view, sync_backref=B_a_sync),
            )

        if case.A_bs_init_error:
            assert_raises_message(
                exc.ArgumentError,
                "sync_backref and viewonly cannot both be True",
                rel,
            )
            return

        self.mapper_registry.map_imperatively(
            A,
            self.tables.t1,
            properties={"bs": rel()},
        )
        self.mapper_registry.map_imperatively(B, self.tables.t2)

        if case.B_a_init_error:
            assert_raises_message(
                exc.ArgumentError,
                "sync_backref and viewonly cannot both be True",
                configure_mappers,
            )
            return

        if case.map_error:
            if case.map_error == "AB":
                args = ("A.bs", "B.a")
            else:
                args = ("B.a", "A.bs")
            assert_raises_message(
                exc.InvalidRequestError,
                "Relationship %s cannot specify sync_backref=True since %s "
                % args,
                configure_mappers,
            )
            return

        configure_mappers()

        a1 = A()
        b1 = B()
        b1.a = a1
        assert (b1 in a1.bs) == case.B_a_event
        assert inspect(a1).attrs.bs.history.has_changes() == case.B_a_event
        assert inspect(b1).attrs.a.history.has_changes() == (not B_a_view)

        a2 = A()
        b2 = B()
        a2.bs.append(b2)
        assert (b2.a == a2) == case.A_bs_event
        assert inspect(a2).attrs.bs.history.has_changes() == (not A_bs_view)
        assert inspect(b2).attrs.a.history.has_changes() == case.A_bs_event


class ViewOnlyUniqueNames(fixtures.MappedTest):
    """'viewonly' mappings with unique PK column names."""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "t1id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("data", String(40)),
        )
        Table(
            "t2",
            metadata,
            Column(
                "t2id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("data", String(40)),
            Column("t1id_ref", Integer, ForeignKey("t1.t1id")),
        )
        Table(
            "t3",
            metadata,
            Column(
                "t3id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("data", String(40)),
            Column("t2id_ref", Integer, ForeignKey("t2.t2id")),
        )

    def test_three_table_view(self):
        """A three table join with overlapping PK names.

        A third table is pulled into the primary join condition using unique
        PK column names and should not produce 'mapper has no columnX' error.

        """

        t2, t3, t1 = (self.tables.t2, self.tables.t3, self.tables.t1)

        class C1(BasicEntity):
            pass

        class C2(BasicEntity):
            pass

        class C3(BasicEntity):
            pass

        self.mapper_registry.map_imperatively(
            C1,
            t1,
            properties={
                "t2s": relationship(C2),
                "t2_view": relationship(
                    C2,
                    viewonly=True,
                    primaryjoin=sa.and_(
                        t1.c.t1id == t2.c.t1id_ref,
                        t3.c.t2id_ref == t2.c.t2id,
                        t3.c.data == t1.c.data,
                    ),
                ),
            },
        )
        self.mapper_registry.map_imperatively(C2, t2)
        self.mapper_registry.map_imperatively(
            C3, t3, properties={"t2": relationship(C2)}
        )

        c1 = C1()
        c1.data = "c1data"
        c2a = C2()
        c1.t2s.append(c2a)
        c2b = C2()
        c1.t2s.append(c2b)
        c3 = C3()
        c3.data = "c1data"
        c3.t2 = c2b
        sess = fixture_session()

        sess.add_all((c1, c3))
        sess.flush()
        sess.expunge_all()

        c1 = sess.get(C1, c1.t1id)
        assert {x.t2id for x in c1.t2s} == {c2a.t2id, c2b.t2id}
        assert {x.t2id for x in c1.t2_view} == {c2b.t2id}


class ViewOnlyLocalRemoteM2M(fixtures.TestBase):
    """test that local-remote is correctly determined for m2m"""

    def test_local_remote(self, registry):
        meta = MetaData()

        t1 = Table("t1", meta, Column("id", Integer, primary_key=True))
        t2 = Table("t2", meta, Column("id", Integer, primary_key=True))
        t12 = Table(
            "tab",
            meta,
            Column("t1_id", Integer, ForeignKey("t1.id")),
            Column("t2_id", Integer, ForeignKey("t2.id")),
        )

        class A:
            pass

        class B:
            pass

        registry.map_imperatively(B, t2)
        m = registry.map_imperatively(
            A,
            t1,
            properties=dict(
                b_view=relationship(B, secondary=t12, viewonly=True),
                b_plain=relationship(B, secondary=t12),
            ),
        )
        configure_mappers()
        assert (
            m.get_property("b_view").local_remote_pairs
            == m.get_property("b_plain").local_remote_pairs
            == [(t1.c.id, t12.c.t1_id), (t2.c.id, t12.c.t2_id)]
        )


class ViewOnlyNonEquijoin(fixtures.MappedTest):
    """'viewonly' mappings based on non-equijoins."""

    @classmethod
    def define_tables(cls, metadata):
        Table("foos", metadata, Column("id", Integer, primary_key=True))
        Table(
            "bars",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("fid", Integer),
        )

    def test_viewonly_join(self):
        bars, foos = self.tables.bars, self.tables.foos

        class Foo(ComparableEntity):
            pass

        class Bar(ComparableEntity):
            pass

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    primaryjoin=foos.c.id > bars.c.fid,
                    foreign_keys=[bars.c.fid],
                    viewonly=True,
                )
            },
        )

        self.mapper_registry.map_imperatively(Bar, bars)

        with fixture_session() as sess:
            sess.add_all(
                (
                    Foo(id=4),
                    Foo(id=9),
                    Bar(id=1, fid=2),
                    Bar(id=2, fid=3),
                    Bar(id=3, fid=6),
                    Bar(id=4, fid=7),
                )
            )
            sess.commit()

        sess = fixture_session()
        eq_(
            sess.query(Foo).filter_by(id=4).one(),
            Foo(id=4, bars=[Bar(fid=2), Bar(fid=3)]),
        )
        eq_(
            sess.query(Foo).filter_by(id=9).one(),
            Foo(id=9, bars=[Bar(fid=2), Bar(fid=3), Bar(fid=6), Bar(fid=7)]),
        )


class ViewOnlyRepeatedRemoteColumn(fixtures.MappedTest):
    """'viewonly' mappings that contain the same 'remote' column twice"""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foos",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("bid1", Integer, ForeignKey("bars.id")),
            Column("bid2", Integer, ForeignKey("bars.id")),
        )

        Table(
            "bars",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
        )

    def test_relationship_on_or(self):
        bars, foos = self.tables.bars, self.tables.foos

        class Foo(ComparableEntity):
            pass

        class Bar(ComparableEntity):
            pass

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    primaryjoin=sa.or_(
                        bars.c.id == foos.c.bid1, bars.c.id == foos.c.bid2
                    ),
                    uselist=True,
                    viewonly=True,
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        sess = fixture_session()
        b1 = Bar(id=1, data="b1")
        b2 = Bar(id=2, data="b2")
        b3 = Bar(id=3, data="b3")
        f1 = Foo(bid1=1, bid2=2)
        f2 = Foo(bid1=3, bid2=None)

        sess.add_all((b1, b2, b3))
        sess.flush()

        sess.add_all((f1, f2))
        sess.flush()

        sess.expunge_all()
        eq_(
            sess.query(Foo).filter_by(id=f1.id).one(),
            Foo(bars=[Bar(data="b1"), Bar(data="b2")]),
        )
        eq_(
            sess.query(Foo).filter_by(id=f2.id).one(),
            Foo(bars=[Bar(data="b3")]),
        )


class ViewOnlyRepeatedLocalColumn(fixtures.MappedTest):
    """'viewonly' mappings that contain the same 'local' column twice"""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foos",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
        )

        Table(
            "bars",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("fid1", Integer, ForeignKey("foos.id")),
            Column("fid2", Integer, ForeignKey("foos.id")),
            Column("data", String(50)),
        )

    def test_relationship_on_or(self):
        bars, foos = self.tables.bars, self.tables.foos

        class Foo(ComparableEntity):
            pass

        class Bar(ComparableEntity):
            pass

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    primaryjoin=sa.or_(
                        bars.c.fid1 == foos.c.id, bars.c.fid2 == foos.c.id
                    ),
                    viewonly=True,
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        sess = fixture_session()
        f1 = Foo(id=1, data="f1")
        f2 = Foo(id=2, data="f2")
        b1 = Bar(fid1=1, data="b1")
        b2 = Bar(fid2=1, data="b2")
        b3 = Bar(fid1=2, data="b3")
        b4 = Bar(fid1=1, fid2=2, data="b4")

        sess.add_all((f1, f2))
        sess.flush()

        sess.add_all((b1, b2, b3, b4))
        sess.flush()

        sess.expunge_all()
        eq_(
            sess.query(Foo).filter_by(id=f1.id).one(),
            Foo(bars=[Bar(data="b1"), Bar(data="b2"), Bar(data="b4")]),
        )
        eq_(
            sess.query(Foo).filter_by(id=f2.id).one(),
            Foo(bars=[Bar(data="b3"), Bar(data="b4")]),
        )


class ViewOnlyComplexJoin(_RelationshipErrors, fixtures.MappedTest):
    """'viewonly' mappings with a complex join condition."""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
        )
        Table(
            "t2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
            Column("t1id", Integer, ForeignKey("t1.id")),
        )
        Table(
            "t3",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
        )
        Table(
            "t2tot3",
            metadata,
            Column("t2id", Integer, ForeignKey("t2.id")),
            Column("t3id", Integer, ForeignKey("t3.id")),
        )

    @classmethod
    def setup_classes(cls):
        class T1(cls.Comparable):
            pass

        class T2(cls.Comparable):
            pass

        class T3(cls.Comparable):
            pass

    def test_basic(self):
        T1, t2, T2, T3, t3, t2tot3, t1 = (
            self.classes.T1,
            self.tables.t2,
            self.classes.T2,
            self.classes.T3,
            self.tables.t3,
            self.tables.t2tot3,
            self.tables.t1,
        )

        self.mapper_registry.map_imperatively(
            T1,
            t1,
            properties={
                "t3s": relationship(
                    T3,
                    primaryjoin=sa.and_(
                        t1.c.id == t2.c.t1id,
                        t2.c.id == t2tot3.c.t2id,
                        t3.c.id == t2tot3.c.t3id,
                    ),
                    viewonly=True,
                    foreign_keys=t3.c.id,
                    remote_side=t2.c.t1id,
                )
            },
        )
        self.mapper_registry.map_imperatively(
            T2,
            t2,
            properties={
                "t1": relationship(T1),
                "t3s": relationship(T3, secondary=t2tot3),
            },
        )
        self.mapper_registry.map_imperatively(T3, t3)

        sess = fixture_session()
        sess.add(T2(data="t2", t1=T1(data="t1"), t3s=[T3(data="t3")]))
        sess.flush()
        sess.expunge_all()

        a = sess.query(T1).first()
        eq_(a.t3s, [T3(data="t3")])

    def test_remote_side_escalation(self):
        T1, t2, T2, T3, t3, t2tot3, t1 = (
            self.classes.T1,
            self.tables.t2,
            self.classes.T2,
            self.classes.T3,
            self.tables.t3,
            self.tables.t2tot3,
            self.tables.t1,
        )

        self.mapper_registry.map_imperatively(
            T1,
            t1,
            properties={
                "t3s": relationship(
                    T3,
                    primaryjoin=sa.and_(
                        t1.c.id == t2.c.t1id,
                        t2.c.id == t2tot3.c.t2id,
                        t3.c.id == t2tot3.c.t3id,
                    ),
                    viewonly=True,
                    foreign_keys=t3.c.id,
                )
            },
        )
        self.mapper_registry.map_imperatively(
            T2,
            t2,
            properties={
                "t1": relationship(T1),
                "t3s": relationship(T3, secondary=t2tot3),
            },
        )
        self.mapper_registry.map_imperatively(T3, t3)
        self._assert_raises_no_local_remote(configure_mappers, "T1.t3s")


class FunctionAsPrimaryJoinTest(fixtures.DeclarativeMappedTest):
    """test :ticket:`3831`"""

    __only_on__ = "sqlite"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Venue(Base):
            __tablename__ = "venue"
            id = Column(Integer, primary_key=True)
            name = Column(String)

            descendants = relationship(
                "Venue",
                primaryjoin=func.instr(
                    remote(foreign(name)), name + "/"
                ).as_comparison(1, 2)
                == 1,
                viewonly=True,
                order_by=name,
            )

    @classmethod
    def insert_data(cls, connection):
        Venue = cls.classes.Venue
        s = Session(connection)
        s.add_all(
            [
                Venue(name="parent1"),
                Venue(name="parent2"),
                Venue(name="parent1/child1"),
                Venue(name="parent1/child2"),
                Venue(name="parent2/child1"),
            ]
        )
        s.commit()

    def test_lazyload(self):
        Venue = self.classes.Venue
        s = fixture_session()
        v1 = s.query(Venue).filter_by(name="parent1").one()
        eq_(
            [d.name for d in v1.descendants],
            ["parent1/child1", "parent1/child2"],
        )

    def test_joinedload(self):
        Venue = self.classes.Venue
        s = fixture_session()

        def go():
            v1 = (
                s.query(Venue)
                .filter_by(name="parent1")
                .options(joinedload(Venue.descendants))
                .one()
            )

            eq_(
                [d.name for d in v1.descendants],
                ["parent1/child1", "parent1/child2"],
            )

        self.assert_sql_count(testing.db, go, 1)


class RemoteForeignBetweenColsTest(fixtures.DeclarativeMappedTest):
    """test a complex annotation using between().

    Using declarative here as an integration test for the local()
    and remote() annotations in conjunction with already annotated
    instrumented attributes, etc.

    """

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Network(ComparableEntity, Base):
            __tablename__ = "network"

            id = Column(
                sa.Integer, primary_key=True, test_needs_autoincrement=True
            )
            ip_net_addr = Column(Integer)
            ip_broadcast_addr = Column(Integer)

            addresses = relationship(
                "Address",
                primaryjoin="remote(foreign(Address.ip_addr)).between("
                "Network.ip_net_addr,"
                "Network.ip_broadcast_addr)",
                viewonly=True,
            )

        class Address(ComparableEntity, Base):
            __tablename__ = "address"

            ip_addr = Column(Integer, primary_key=True)

    @classmethod
    def insert_data(cls, connection):
        Network, Address = cls.classes.Network, cls.classes.Address
        s = Session(connection)

        s.add_all(
            [
                Network(ip_net_addr=5, ip_broadcast_addr=10),
                Network(ip_net_addr=15, ip_broadcast_addr=25),
                Network(ip_net_addr=30, ip_broadcast_addr=35),
                Address(ip_addr=17),
                Address(ip_addr=18),
                Address(ip_addr=9),
                Address(ip_addr=27),
            ]
        )
        s.commit()

    def test_col_query(self):
        Network, Address = self.classes.Network, self.classes.Address

        session = Session(testing.db)
        eq_(
            session.query(Address.ip_addr)
            .select_from(Network)
            .join(Network.addresses)
            .filter(Network.ip_net_addr == 15)
            .all(),
            [(17,), (18,)],
        )

    def test_lazyload(self):
        Network = self.classes.Network

        session = Session(testing.db)

        n3 = session.query(Network).filter(Network.ip_net_addr == 5).one()
        eq_([a.ip_addr for a in n3.addresses], [9])


class ExplicitLocalRemoteTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column("id", String(50), primary_key=True),
            Column("data", String(50)),
        )
        Table(
            "t2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
            Column("t1id", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class T1(cls.Comparable):
            pass

        class T2(cls.Comparable):
            pass

    def test_onetomany_funcfk_oldstyle(self):
        T2, T1, t2, t1 = (
            self.classes.T2,
            self.classes.T1,
            self.tables.t2,
            self.tables.t1,
        )

        # old _local_remote_pairs
        self.mapper_registry.map_imperatively(
            T1,
            t1,
            properties={
                "t2s": relationship(
                    T2,
                    primaryjoin=t1.c.id == sa.func.lower(t2.c.t1id),
                    _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
                    foreign_keys=[t2.c.t1id],
                )
            },
        )
        self.mapper_registry.map_imperatively(T2, t2)
        self._test_onetomany()

    def test_onetomany_funcfk_annotated(self):
        T2, T1, t2, t1 = (
            self.classes.T2,
            self.classes.T1,
            self.tables.t2,
            self.tables.t1,
        )

        # use annotation
        self.mapper_registry.map_imperatively(
            T1,
            t1,
            properties={
                "t2s": relationship(
                    T2,
                    primaryjoin=t1.c.id == foreign(sa.func.lower(t2.c.t1id)),
                )
            },
        )
        self.mapper_registry.map_imperatively(T2, t2)
        self._test_onetomany()

    def _test_onetomany(self):
        T2, T1, t2, t1 = (
            self.classes.T2,
            self.classes.T1,
            self.tables.t2,
            self.tables.t1,
        )
        is_(T1.t2s.property.direction, ONETOMANY)
        eq_(T1.t2s.property.local_remote_pairs, [(t1.c.id, t2.c.t1id)])
        sess = fixture_session()
        a1 = T1(id="number1", data="a1")
        a2 = T1(id="number2", data="a2")
        b1 = T2(data="b1", t1id="NuMbEr1")
        b2 = T2(data="b2", t1id="Number1")
        b3 = T2(data="b3", t1id="Number2")
        sess.add_all((a1, a2, b1, b2, b3))
        sess.flush()
        sess.expunge_all()

        eq_(
            sess.query(T1).first(),
            T1(
                id="number1",
                data="a1",
                t2s=[
                    T2(data="b1", t1id="NuMbEr1"),
                    T2(data="b2", t1id="Number1"),
                ],
            ),
        )

    def test_manytoone_funcfk(self):
        T2, T1, t2, t1 = (
            self.classes.T2,
            self.classes.T1,
            self.tables.t2,
            self.tables.t1,
        )

        self.mapper_registry.map_imperatively(T1, t1)
        self.mapper_registry.map_imperatively(
            T2,
            t2,
            properties={
                "t1": relationship(
                    T1,
                    primaryjoin=t1.c.id == sa.func.lower(t2.c.t1id),
                    _local_remote_pairs=[(t2.c.t1id, t1.c.id)],
                    foreign_keys=[t2.c.t1id],
                    uselist=True,
                )
            },
        )

        sess = fixture_session()
        a1 = T1(id="number1", data="a1")
        a2 = T1(id="number2", data="a2")
        b1 = T2(data="b1", t1id="NuMbEr1")
        b2 = T2(data="b2", t1id="Number1")
        b3 = T2(data="b3", t1id="Number2")
        sess.add_all((a1, a2, b1, b2, b3))
        sess.flush()
        sess.expunge_all()

        eq_(
            sess.query(T2).filter(T2.data.in_(["b1", "b2"])).all(),
            [
                T2(data="b1", t1=[T1(id="number1", data="a1")]),
                T2(data="b2", t1=[T1(id="number1", data="a1")]),
            ],
        )

    def test_onetomany_func_referent(self):
        T2, T1, t2, t1 = (
            self.classes.T2,
            self.classes.T1,
            self.tables.t2,
            self.tables.t1,
        )

        self.mapper_registry.map_imperatively(
            T1,
            t1,
            properties={
                "t2s": relationship(
                    T2,
                    primaryjoin=sa.func.lower(t1.c.id) == t2.c.t1id,
                    _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
                    foreign_keys=[t2.c.t1id],
                )
            },
        )
        self.mapper_registry.map_imperatively(T2, t2)

        sess = fixture_session()
        a1 = T1(id="NuMbeR1", data="a1")
        a2 = T1(id="NuMbeR2", data="a2")
        b1 = T2(data="b1", t1id="number1")
        b2 = T2(data="b2", t1id="number1")
        b3 = T2(data="b2", t1id="number2")
        sess.add_all((a1, a2, b1, b2, b3))
        sess.flush()
        sess.expunge_all()

        eq_(
            sess.query(T1).first(),
            T1(
                id="NuMbeR1",
                data="a1",
                t2s=[
                    T2(data="b1", t1id="number1"),
                    T2(data="b2", t1id="number1"),
                ],
            ),
        )

    def test_manytoone_func_referent(self):
        T2, T1, t2, t1 = (
            self.classes.T2,
            self.classes.T1,
            self.tables.t2,
            self.tables.t1,
        )

        self.mapper_registry.map_imperatively(T1, t1)
        self.mapper_registry.map_imperatively(
            T2,
            t2,
            properties={
                "t1": relationship(
                    T1,
                    primaryjoin=sa.func.lower(t1.c.id) == t2.c.t1id,
                    _local_remote_pairs=[(t2.c.t1id, t1.c.id)],
                    foreign_keys=[t2.c.t1id],
                    uselist=True,
                )
            },
        )

        sess = fixture_session()
        a1 = T1(id="NuMbeR1", data="a1")
        a2 = T1(id="NuMbeR2", data="a2")
        b1 = T2(data="b1", t1id="number1")
        b2 = T2(data="b2", t1id="number1")
        b3 = T2(data="b3", t1id="number2")
        sess.add_all((a1, a2, b1, b2, b3))
        sess.flush()
        sess.expunge_all()

        eq_(
            sess.query(T2).filter(T2.data.in_(["b1", "b2"])).all(),
            [
                T2(data="b1", t1=[T1(id="NuMbeR1", data="a1")]),
                T2(data="b2", t1=[T1(id="NuMbeR1", data="a1")]),
            ],
        )

    def test_escalation_1(self):
        T2, T1, t2, t1 = (
            self.classes.T2,
            self.classes.T1,
            self.tables.t2,
            self.tables.t1,
        )

        self.mapper_registry.map_imperatively(
            T1,
            t1,
            properties={
                "t2s": relationship(
                    T2,
                    primaryjoin=t1.c.id == sa.func.lower(t2.c.t1id),
                    _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
                    foreign_keys=[t2.c.t1id],
                    remote_side=[t2.c.t1id],
                )
            },
        )
        self.mapper_registry.map_imperatively(T2, t2)
        assert_raises(sa.exc.ArgumentError, sa.orm.configure_mappers)

    def test_escalation_2(self):
        T2, T1, t2, t1 = (
            self.classes.T2,
            self.classes.T1,
            self.tables.t2,
            self.tables.t1,
        )

        self.mapper_registry.map_imperatively(
            T1,
            t1,
            properties={
                "t2s": relationship(
                    T2,
                    primaryjoin=t1.c.id == sa.func.lower(t2.c.t1id),
                    _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
                )
            },
        )
        self.mapper_registry.map_imperatively(T2, t2)
        assert_raises(sa.exc.ArgumentError, sa.orm.configure_mappers)


class InvalidRemoteSideTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
            Column("t_id", Integer, ForeignKey("t1.id")),
        )

    @classmethod
    def setup_classes(cls):
        class T1(cls.Comparable):
            pass

    def test_o2m_backref(self):
        T1, t1 = self.classes.T1, self.tables.t1

        self.mapper_registry.map_imperatively(
            T1, t1, properties={"t1s": relationship(T1, backref="parent")}
        )

        assert_raises_message(
            sa.exc.ArgumentError,
            "T1.t1s and back-reference T1.parent are "
            r"both of the same "
            r"direction .*RelationshipDirection.ONETOMANY.*.  Did you "
            "mean to set remote_side on the many-to-one side ?",
            configure_mappers,
        )

    def test_m2o_backref(self):
        T1, t1 = self.classes.T1, self.tables.t1

        self.mapper_registry.map_imperatively(
            T1,
            t1,
            properties={
                "t1s": relationship(
                    T1,
                    backref=backref("parent", remote_side=t1.c.id),
                    remote_side=t1.c.id,
                )
            },
        )

        assert_raises_message(
            sa.exc.ArgumentError,
            "T1.t1s and back-reference T1.parent are "
            r"both of the same direction .*RelationshipDirection.MANYTOONE.*."
            "Did you "
            "mean to set remote_side on the many-to-one side ?",
            configure_mappers,
        )

    def test_o2m_explicit(self):
        T1, t1 = self.classes.T1, self.tables.t1

        self.mapper_registry.map_imperatively(
            T1,
            t1,
            properties={
                "t1s": relationship(T1, back_populates="parent"),
                "parent": relationship(T1, back_populates="t1s"),
            },
        )

        # can't be sure of ordering here
        assert_raises_message(
            sa.exc.ArgumentError,
            r"both of the same direction "
            r".*RelationshipDirection.ONETOMANY.*.  Did you "
            "mean to set remote_side on the many-to-one side ?",
            configure_mappers,
        )

    def test_m2o_explicit(self):
        T1, t1 = self.classes.T1, self.tables.t1

        self.mapper_registry.map_imperatively(
            T1,
            t1,
            properties={
                "t1s": relationship(
                    T1, back_populates="parent", remote_side=t1.c.id
                ),
                "parent": relationship(
                    T1, back_populates="t1s", remote_side=t1.c.id
                ),
            },
        )

        # can't be sure of ordering here
        assert_raises_message(
            sa.exc.ArgumentError,
            r"both of the same direction "
            r".*RelationshipDirection.MANYTOONE.*.  Did you "
            "mean to set remote_side on the many-to-one side ?",
            configure_mappers,
        )


class AmbiguousFKResolutionTest(_RelationshipErrors, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table("a", metadata, Column("id", Integer, primary_key=True))
        Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("aid_1", Integer, ForeignKey("a.id")),
            Column("aid_2", Integer, ForeignKey("a.id")),
        )
        Table("atob", metadata, Column("aid", Integer), Column("bid", Integer))
        Table(
            "atob_ambiguous",
            metadata,
            Column("aid1", Integer, ForeignKey("a.id")),
            Column("bid1", Integer, ForeignKey("b.id")),
            Column("aid2", Integer, ForeignKey("a.id")),
            Column("bid2", Integer, ForeignKey("b.id")),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

    def test_ambiguous_fks_o2m(self):
        A, B = self.classes.A, self.classes.B
        a, b = self.tables.a, self.tables.b
        self.mapper_registry.map_imperatively(
            A, a, properties={"bs": relationship(B)}
        )
        self.mapper_registry.map_imperatively(B, b)
        self._assert_raises_ambig_join(configure_mappers, "A.bs", None)

    def test_with_fks_o2m(self):
        A, B = self.classes.A, self.classes.B
        a, b = self.tables.a, self.tables.b
        self.mapper_registry.map_imperatively(
            A, a, properties={"bs": relationship(B, foreign_keys=b.c.aid_1)}
        )
        self.mapper_registry.map_imperatively(B, b)
        sa.orm.configure_mappers()
        assert A.bs.property.primaryjoin.compare(a.c.id == b.c.aid_1)
        eq_(A.bs.property._calculated_foreign_keys, {b.c.aid_1})

    def test_with_pj_o2m(self):
        A, B = self.classes.A, self.classes.B
        a, b = self.tables.a, self.tables.b
        self.mapper_registry.map_imperatively(
            A,
            a,
            properties={
                "bs": relationship(B, primaryjoin=a.c.id == b.c.aid_1)
            },
        )
        self.mapper_registry.map_imperatively(B, b)
        sa.orm.configure_mappers()
        assert A.bs.property.primaryjoin.compare(a.c.id == b.c.aid_1)
        eq_(A.bs.property._calculated_foreign_keys, {b.c.aid_1})

    def test_with_annotated_pj_o2m(self):
        A, B = self.classes.A, self.classes.B
        a, b = self.tables.a, self.tables.b
        self.mapper_registry.map_imperatively(
            A,
            a,
            properties={
                "bs": relationship(B, primaryjoin=a.c.id == foreign(b.c.aid_1))
            },
        )
        self.mapper_registry.map_imperatively(B, b)
        sa.orm.configure_mappers()
        assert A.bs.property.primaryjoin.compare(a.c.id == b.c.aid_1)
        eq_(A.bs.property._calculated_foreign_keys, {b.c.aid_1})

    def test_no_fks_m2m(self):
        A, B = self.classes.A, self.classes.B
        a, b, a_to_b = self.tables.a, self.tables.b, self.tables.atob
        self.mapper_registry.map_imperatively(
            A, a, properties={"bs": relationship(B, secondary=a_to_b)}
        )
        self.mapper_registry.map_imperatively(B, b)
        self._assert_raises_no_join(sa.orm.configure_mappers, "A.bs", a_to_b)

    def test_ambiguous_fks_m2m(self):
        A, B = self.classes.A, self.classes.B
        a, b, a_to_b = self.tables.a, self.tables.b, self.tables.atob_ambiguous
        self.mapper_registry.map_imperatively(
            A, a, properties={"bs": relationship(B, secondary=a_to_b)}
        )
        self.mapper_registry.map_imperatively(B, b)

        self._assert_raises_ambig_join(
            configure_mappers, "A.bs", "atob_ambiguous"
        )

    def test_with_fks_m2m(self):
        A, B = self.classes.A, self.classes.B
        a, b, a_to_b = self.tables.a, self.tables.b, self.tables.atob_ambiguous
        self.mapper_registry.map_imperatively(
            A,
            a,
            properties={
                "bs": relationship(
                    B,
                    secondary=a_to_b,
                    foreign_keys=[a_to_b.c.aid1, a_to_b.c.bid1],
                )
            },
        )
        self.mapper_registry.map_imperatively(B, b)
        sa.orm.configure_mappers()


class SecondaryArgTest(fixtures.TestBase):
    def teardown_test(self):
        clear_mappers()

    @testing.variation("arg_style", ["string", "table", "lambda_"])
    def test_secondary_arg_styles(self, arg_style):
        Base = declarative_base()

        c = Table(
            "c",
            Base.metadata,
            Column("a_id", ForeignKey("a.id")),
            Column("b_id", ForeignKey("b.id")),
        )

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            data = Column(String)

            if arg_style.string:
                bs = relationship("B", secondary="c")
            elif arg_style.table:
                bs = relationship("B", secondary=c)
            elif arg_style.lambda_:
                bs = relationship("B", secondary=lambda: c)
            else:
                arg_style.fail()

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)

        is_(inspect(A).relationships.bs.secondary, c)

    def test_no_eval_in_secondary(self):
        """test #10564"""
        Base = declarative_base()

        Table(
            "c",
            Base.metadata,
            Column("a_id", ForeignKey("a.id")),
            Column("b_id", ForeignKey("b.id")),
        )

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            data = Column(String)

            bs = relationship("B", secondary="c.c.a_id.table")

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)

        with expect_raises_message(
            exc.InvalidRequestError,
            r"When initializing mapper Mapper\[A\(a\)\], expression "
            r"'c.c.a_id.table' failed to locate a name \('c.c.a_id.table'\). ",
        ):
            Base.registry.configure()

    @testing.combinations((True,), (False,))
    def test_informative_message_on_cls_as_secondary(self, string):
        Base = declarative_base()

        class C(Base):
            __tablename__ = "c"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            b_id = Column(ForeignKey("b.id"))

        if string:
            c_arg = "C"
        else:
            c_arg = C

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            data = Column(String)
            bs = relationship("B", secondary=c_arg)

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)

        assert_raises_message(
            exc.ArgumentError,
            r"secondary argument <class .*C.*> passed to to "
            r"relationship\(\) A.bs "
            "must be a Table object or other FROM clause; can't send a "
            "mapped class directly as rows in 'secondary' are persisted "
            "independently of a class that is mapped to that same table.",
            configure_mappers,
        )


class SecondaryNestedJoinTest(
    fixtures.MappedTest, AssertsCompiledSQL, testing.AssertsExecutionResults
):
    """test support for a relationship where the 'secondary' table is a
    compound join().

    join() and joinedload() should use a "flat" alias, lazyloading needs
    to ensure the join renders.

    """

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30)),
            Column("b_id", ForeignKey("b.id")),
        )
        Table(
            "b",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30)),
            Column("d_id", ForeignKey("d.id")),
        )
        Table(
            "c",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30)),
            Column("a_id", ForeignKey("a.id")),
            Column("d_id", ForeignKey("d.id")),
        )
        Table(
            "d",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

        class B(cls.Comparable):
            pass

        class C(cls.Comparable):
            pass

        class D(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B, C, D = cls.classes.A, cls.classes.B, cls.classes.C, cls.classes.D
        a, b, c, d = cls.tables.a, cls.tables.b, cls.tables.c, cls.tables.d
        j = sa.join(b, d, b.c.d_id == d.c.id).join(c, c.c.d_id == d.c.id)
        # j = join(b, d, b.c.d_id == d.c.id).join(c, c.c.d_id == d.c.id) \
        # .alias()
        cls.mapper_registry.map_imperatively(
            A,
            a,
            properties={
                "b": relationship(B),
                "d": relationship(
                    D,
                    secondary=j,
                    primaryjoin=and_(a.c.b_id == b.c.id, a.c.id == c.c.a_id),
                    secondaryjoin=d.c.id == b.c.d_id,
                    uselist=False,
                    viewonly=True,
                ),
            },
        )
        cls.mapper_registry.map_imperatively(
            B, b, properties={"d": relationship(D)}
        )
        cls.mapper_registry.map_imperatively(
            C, c, properties={"a": relationship(A), "d": relationship(D)}
        )
        cls.mapper_registry.map_imperatively(D, d)

    @classmethod
    def insert_data(cls, connection):
        A, B, C, D = cls.classes.A, cls.classes.B, cls.classes.C, cls.classes.D
        sess = Session(connection)
        a1, a2, a3, a4 = A(name="a1"), A(name="a2"), A(name="a3"), A(name="a4")
        b1, b2, b3, b4 = B(name="b1"), B(name="b2"), B(name="b3"), B(name="b4")
        c1, c2, c3, c4 = C(name="c1"), C(name="c2"), C(name="c3"), C(name="c4")
        d1, d2 = D(name="d1"), D(name="d2")

        a1.b = b1
        a2.b = b2
        a3.b = b3
        a4.b = b4

        c1.a = a1
        c2.a = a2
        c3.a = a2
        c4.a = a4

        c1.d = d1
        c2.d = d2
        c3.d = d1
        c4.d = d2

        b1.d = d1
        b2.d = d1
        b3.d = d2
        b4.d = d2

        sess.add_all([a1, a2, a3, a4, b1, b2, b3, b4, c1, c2, c4, c4, d1, d2])
        sess.commit()

    def test_render_join(self):
        A = self.classes.A
        sess = fixture_session()
        self.assert_compile(
            sess.query(A).join(A.d),
            "SELECT a.id AS a_id, a.name AS a_name, a.b_id AS a_b_id "
            "FROM a JOIN (b AS b_1 JOIN d AS d_1 ON b_1.d_id = d_1.id "
            "JOIN c AS c_1 ON c_1.d_id = d_1.id) ON a.b_id = b_1.id "
            "AND a.id = c_1.a_id JOIN d ON d.id = b_1.d_id",
            dialect="postgresql",
        )

    def test_render_joinedload(self):
        A = self.classes.A
        sess = fixture_session()
        self.assert_compile(
            sess.query(A).options(joinedload(A.d)),
            "SELECT a.id AS a_id, a.name AS a_name, a.b_id AS a_b_id, "
            "d_1.id AS d_1_id, d_1.name AS d_1_name FROM a LEFT OUTER JOIN "
            "(b AS b_1 JOIN d AS d_2 ON b_1.d_id = d_2.id JOIN c AS c_1 "
            "ON c_1.d_id = d_2.id JOIN d AS d_1 ON d_1.id = b_1.d_id) "
            "ON a.b_id = b_1.id AND a.id = c_1.a_id",
            dialect="postgresql",
        )

    def test_render_lazyload(self):
        A = self.classes.A
        sess = fixture_session()
        a1 = sess.query(A).filter(A.name == "a1").first()

        def go():
            a1.d

        # here, the "lazy" strategy has to ensure the "secondary"
        # table is part of the "select_from()", since it's a join().
        # referring to just the columns won't actually render all those
        # join conditions.
        self.assert_sql_execution(
            testing.db,
            go,
            CompiledSQL(
                "SELECT d.id AS d_id, d.name AS d_name FROM b "
                "JOIN d ON b.d_id = d.id JOIN c ON c.d_id = d.id "
                "WHERE :param_1 = b.id AND :param_2 = c.a_id "
                "AND d.id = b.d_id",
                {"param_1": a1.id, "param_2": a1.id},
            ),
        )

    mapping = {"a1": "d1", "a2": None, "a3": None, "a4": "d2"}

    def test_join(self):
        A, D = self.classes.A, self.classes.D
        sess = fixture_session()

        for a, d in sess.query(A, D).outerjoin(A.d):
            eq_(self.mapping[a.name], d.name if d is not None else None)

    def test_joinedload(self):
        A = self.classes.A
        sess = fixture_session()

        for a in sess.query(A).options(joinedload(A.d)):
            d = a.d
            eq_(self.mapping[a.name], d.name if d is not None else None)

    def test_lazyload(self):
        A = self.classes.A
        sess = fixture_session()

        for a in sess.query(A):
            d = a.d
            eq_(self.mapping[a.name], d.name if d is not None else None)


class InvalidRelationshipEscalationTest(
    _RelationshipErrors, fixtures.MappedTest
):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foos",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("fid", Integer),
        )
        Table(
            "bars",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("fid", Integer),
        )

        Table(
            "foos_with_fks",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("fid", Integer, ForeignKey("foos_with_fks.id")),
        )
        Table(
            "bars_with_fks",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("fid", Integer, ForeignKey("foos_with_fks.id")),
        )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

        class Bar(cls.Basic):
            pass

    def test_no_join(self):
        bars, Foo, Bar, foos = (
            self.tables.bars,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo, foos, properties={"bars": relationship(Bar)}
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_join(sa.orm.configure_mappers, "Foo.bars", None)

    def test_no_join_self_ref(self):
        bars, Foo, Bar, foos = (
            self.tables.bars,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo, foos, properties={"foos": relationship(Foo)}
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_join(configure_mappers, "Foo.foos", None)

    def test_no_equated(self):
        bars, Foo, Bar, foos = (
            self.tables.bars,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(Bar, primaryjoin=foos.c.id > bars.c.fid)
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_relevant_fks(
            configure_mappers, "foos.id > bars.fid", "Foo.bars", "primary"
        )

    def test_no_equated_fks(self):
        bars, Foo, Bar, foos = (
            self.tables.bars,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    primaryjoin=foos.c.id > bars.c.fid,
                    foreign_keys=bars.c.fid,
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)
        self._assert_raises_no_equality(
            sa.orm.configure_mappers,
            "foos.id > bars.fid",
            "Foo.bars",
            "primary",
        )

    def test_no_equated_wo_fks_works_on_relaxed(self):
        foos_with_fks, Foo, Bar, bars_with_fks, foos = (
            self.tables.foos_with_fks,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.bars_with_fks,
            self.tables.foos,
        )

        # very unique - the join between parent/child
        # has no fks, but there is an fk join between two other
        # tables in the join condition, for those users that try creating
        # these big-long-string-of-joining-many-tables primaryjoins.
        # in this case we don't get eq_pairs, but we hit the
        # "works if viewonly" rule.  so here we add another clause regarding
        # "try foreign keys".
        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    primaryjoin=and_(
                        bars_with_fks.c.fid == foos_with_fks.c.id,
                        foos_with_fks.c.id == foos.c.id,
                    ),
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars_with_fks)

        self._assert_raises_no_equality(
            sa.orm.configure_mappers,
            "bars_with_fks.fid = foos_with_fks.id "
            "AND foos_with_fks.id = foos.id",
            "Foo.bars",
            "primary",
        )

    def test_ambiguous_fks(self):
        bars, Foo, Bar, foos = (
            self.tables.bars,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    primaryjoin=foos.c.id == bars.c.fid,
                    foreign_keys=[foos.c.id, bars.c.fid],
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_ambiguous_direction(
            sa.orm.configure_mappers, "Foo.bars"
        )

    def test_ambiguous_remoteside_o2m(self):
        bars, Foo, Bar, foos = (
            self.tables.bars,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    primaryjoin=foos.c.id == bars.c.fid,
                    foreign_keys=[bars.c.fid],
                    remote_side=[foos.c.id, bars.c.fid],
                    viewonly=True,
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_local_remote(configure_mappers, "Foo.bars")

    def test_ambiguous_remoteside_m2o(self):
        bars, Foo, Bar, foos = (
            self.tables.bars,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    primaryjoin=foos.c.id == bars.c.fid,
                    foreign_keys=[foos.c.id],
                    remote_side=[foos.c.id, bars.c.fid],
                    viewonly=True,
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_local_remote(configure_mappers, "Foo.bars")

    def test_no_equated_self_ref_no_fks(self):
        bars, Foo, Bar, foos = (
            self.tables.bars,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "foos": relationship(Foo, primaryjoin=foos.c.id > foos.c.fid)
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_relevant_fks(
            configure_mappers, "foos.id > foos.fid", "Foo.foos", "primary"
        )

    def test_no_equated_self_ref_no_equality(self):
        bars, Foo, Bar, foos = (
            self.tables.bars,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "foos": relationship(
                    Foo,
                    primaryjoin=foos.c.id > foos.c.fid,
                    foreign_keys=[foos.c.fid],
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_equality(
            configure_mappers, "foos.id > foos.fid", "Foo.foos", "primary"
        )

    def test_no_equated_viewonly(self):
        bars, Bar, bars_with_fks, foos_with_fks, Foo, foos = (
            self.tables.bars,
            self.classes.Bar,
            self.tables.bars_with_fks,
            self.tables.foos_with_fks,
            self.classes.Foo,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar, primaryjoin=foos.c.id > bars.c.fid, viewonly=True
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_relevant_fks(
            sa.orm.configure_mappers,
            "foos.id > bars.fid",
            "Foo.bars",
            "primary",
        )

        self.mapper_registry.dispose()
        self.mapper_registry.map_imperatively(
            Foo,
            foos_with_fks,
            properties={
                "bars": relationship(
                    Bar,
                    primaryjoin=foos_with_fks.c.id > bars_with_fks.c.fid,
                    viewonly=True,
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars_with_fks)
        sa.orm.configure_mappers()

    def test_no_equated_self_ref_viewonly(self):
        bars, Bar, bars_with_fks, foos_with_fks, Foo, foos = (
            self.tables.bars,
            self.classes.Bar,
            self.tables.bars_with_fks,
            self.tables.foos_with_fks,
            self.classes.Foo,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "foos": relationship(
                    Foo, primaryjoin=foos.c.id > foos.c.fid, viewonly=True
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_relevant_fks(
            sa.orm.configure_mappers,
            "foos.id > foos.fid",
            "Foo.foos",
            "primary",
        )

        self.mapper_registry.dispose()
        self.mapper_registry.map_imperatively(
            Foo,
            foos_with_fks,
            properties={
                "foos": relationship(
                    Foo,
                    primaryjoin=foos_with_fks.c.id > foos_with_fks.c.fid,
                    viewonly=True,
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars_with_fks)
        sa.orm.configure_mappers()

    def test_no_equated_self_ref_viewonly_fks(self):
        Foo, foos = self.classes.Foo, self.tables.foos

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "foos": relationship(
                    Foo,
                    primaryjoin=foos.c.id > foos.c.fid,
                    viewonly=True,
                    foreign_keys=[foos.c.fid],
                )
            },
        )

        sa.orm.configure_mappers()
        eq_(Foo.foos.property.local_remote_pairs, [(foos.c.id, foos.c.fid)])

    def test_equated(self):
        bars, Bar, bars_with_fks, foos_with_fks, Foo, foos = (
            self.tables.bars,
            self.classes.Bar,
            self.tables.bars_with_fks,
            self.tables.foos_with_fks,
            self.classes.Foo,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(Bar, primaryjoin=foos.c.id == bars.c.fid)
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_relevant_fks(
            configure_mappers, "foos.id = bars.fid", "Foo.bars", "primary"
        )

        self.mapper_registry.dispose()

        self.mapper_registry.map_imperatively(
            Foo,
            foos_with_fks,
            properties={
                "bars": relationship(
                    Bar, primaryjoin=foos_with_fks.c.id == bars_with_fks.c.fid
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars_with_fks)
        sa.orm.configure_mappers()

    def test_equated_self_ref(self):
        Foo, foos = self.classes.Foo, self.tables.foos

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "foos": relationship(Foo, primaryjoin=foos.c.id == foos.c.fid)
            },
        )

        self._assert_raises_no_relevant_fks(
            configure_mappers, "foos.id = foos.fid", "Foo.foos", "primary"
        )

    def test_equated_self_ref_wrong_fks(self):
        bars, Foo, foos = (
            self.tables.bars,
            self.classes.Foo,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "foos": relationship(
                    Foo,
                    primaryjoin=foos.c.id == foos.c.fid,
                    foreign_keys=[bars.c.id],
                )
            },
        )

        self._assert_raises_no_relevant_fks(
            configure_mappers, "foos.id = foos.fid", "Foo.foos", "primary"
        )


class InvalidRelationshipEscalationTestM2M(
    _RelationshipErrors, fixtures.MappedTest
):
    @classmethod
    def define_tables(cls, metadata):
        Table("foos", metadata, Column("id", Integer, primary_key=True))
        Table(
            "foobars", metadata, Column("fid", Integer), Column("bid", Integer)
        )
        Table("bars", metadata, Column("id", Integer, primary_key=True))

        Table(
            "foobars_with_fks",
            metadata,
            Column("fid", Integer, ForeignKey("foos.id")),
            Column("bid", Integer, ForeignKey("bars.id")),
        )

        Table(
            "foobars_with_many_columns",
            metadata,
            Column("fid", Integer),
            Column("bid", Integer),
            Column("fid1", Integer),
            Column("bid1", Integer),
            Column("fid2", Integer),
            Column("bid2", Integer),
        )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

        class Bar(cls.Basic):
            pass

    def test_no_join(self):
        foobars, bars, Foo, Bar, foos = (
            self.tables.foobars,
            self.tables.bars,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={"bars": relationship(Bar, secondary=foobars)},
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_join(configure_mappers, "Foo.bars", "foobars")

    def test_no_secondaryjoin(self):
        foobars, bars, Foo, Bar, foos = (
            self.tables.foobars,
            self.tables.bars,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    secondary=foobars,
                    primaryjoin=foos.c.id > foobars.c.fid,
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_join(configure_mappers, "Foo.bars", "foobars")

    def test_no_fks(self):
        foobars_with_many_columns, bars, Bar, foobars, Foo, foos = (
            self.tables.foobars_with_many_columns,
            self.tables.bars,
            self.classes.Bar,
            self.tables.foobars,
            self.classes.Foo,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    secondary=foobars,
                    primaryjoin=foos.c.id == foobars.c.fid,
                    secondaryjoin=foobars.c.bid == bars.c.id,
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)
        sa.orm.configure_mappers()
        eq_(Foo.bars.property.synchronize_pairs, [(foos.c.id, foobars.c.fid)])
        eq_(
            Foo.bars.property.secondary_synchronize_pairs,
            [(bars.c.id, foobars.c.bid)],
        )

        self.mapper_registry.dispose()
        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    secondary=foobars_with_many_columns,
                    primaryjoin=foos.c.id == foobars_with_many_columns.c.fid,
                    secondaryjoin=foobars_with_many_columns.c.bid == bars.c.id,
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)
        sa.orm.configure_mappers()
        eq_(
            Foo.bars.property.synchronize_pairs,
            [(foos.c.id, foobars_with_many_columns.c.fid)],
        )
        eq_(
            Foo.bars.property.secondary_synchronize_pairs,
            [(bars.c.id, foobars_with_many_columns.c.bid)],
        )

    def test_local_col_setup(self):
        foobars_with_fks, bars, Bar, Foo, foos = (
            self.tables.foobars_with_fks,
            self.tables.bars,
            self.classes.Bar,
            self.classes.Foo,
            self.tables.foos,
        )

        # ensure m2m backref is set up with correct annotations
        # [ticket:2578]
        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar, secondary=foobars_with_fks, backref="foos"
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)
        sa.orm.configure_mappers()
        eq_(Foo.bars.property._join_condition.local_columns, {foos.c.id})
        eq_(Bar.foos.property._join_condition.local_columns, {bars.c.id})

    def test_bad_primaryjoin(self):
        foobars_with_fks, bars, Bar, foobars, Foo, foos = (
            self.tables.foobars_with_fks,
            self.tables.bars,
            self.classes.Bar,
            self.tables.foobars,
            self.classes.Foo,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    secondary=foobars,
                    primaryjoin=foos.c.id > foobars.c.fid,
                    secondaryjoin=foobars.c.bid <= bars.c.id,
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_equality(
            configure_mappers, "foos.id > foobars.fid", "Foo.bars", "primary"
        )

        self.mapper_registry.dispose()
        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    secondary=foobars_with_fks,
                    primaryjoin=foos.c.id > foobars_with_fks.c.fid,
                    secondaryjoin=foobars_with_fks.c.bid <= bars.c.id,
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)
        self._assert_raises_no_equality(
            configure_mappers,
            "foos.id > foobars_with_fks.fid",
            "Foo.bars",
            "primary",
        )

        self.mapper_registry.dispose()
        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    secondary=foobars_with_fks,
                    primaryjoin=foos.c.id > foobars_with_fks.c.fid,
                    secondaryjoin=foobars_with_fks.c.bid <= bars.c.id,
                    viewonly=True,
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)
        sa.orm.configure_mappers()

    def test_bad_secondaryjoin(self):
        foobars, bars, Foo, Bar, foos = (
            self.tables.foobars,
            self.tables.bars,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    secondary=foobars,
                    primaryjoin=foos.c.id == foobars.c.fid,
                    secondaryjoin=foobars.c.bid <= bars.c.id,
                    foreign_keys=[foobars.c.fid],
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)
        self._assert_raises_no_relevant_fks(
            configure_mappers,
            "foobars.bid <= bars.id",
            "Foo.bars",
            "secondary",
        )

    def test_no_equated_secondaryjoin(self):
        foobars, bars, Foo, Bar, foos = (
            self.tables.foobars,
            self.tables.bars,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.foos,
        )

        self.mapper_registry.map_imperatively(
            Foo,
            foos,
            properties={
                "bars": relationship(
                    Bar,
                    secondary=foobars,
                    primaryjoin=foos.c.id == foobars.c.fid,
                    secondaryjoin=foobars.c.bid <= bars.c.id,
                    foreign_keys=[foobars.c.fid, foobars.c.bid],
                )
            },
        )
        self.mapper_registry.map_imperatively(Bar, bars)

        self._assert_raises_no_equality(
            configure_mappers,
            "foobars.bid <= bars.id",
            "Foo.bars",
            "secondary",
        )


class ActiveHistoryFlagTest(_fixtures.FixtureTest):
    run_inserts = None
    run_deletes = None

    def _test_attribute(self, obj, attrname, newvalue):
        sess = fixture_session()
        sess.add(obj)
        oldvalue = getattr(obj, attrname)
        sess.commit()

        # expired
        assert attrname not in obj.__dict__

        setattr(obj, attrname, newvalue)
        eq_(
            attributes.get_history(obj, attrname), ([newvalue], (), [oldvalue])
        )

    def test_column_property_flag(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "name": column_property(users.c.name, active_history=True)
            },
        )
        u1 = User(name="jack")
        self._test_attribute(u1, "name", "ed")

    def test_relationship_property_flag(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={"user": relationship(User, active_history=True)},
        )
        self.mapper_registry.map_imperatively(User, users)
        u1 = User(name="jack")
        u2 = User(name="ed")
        a1 = Address(email_address="a1", user=u1)
        self._test_attribute(a1, "user", u2)

    def test_composite_property_flag(self):
        Order, orders = self.classes.Order, self.tables.orders

        class MyComposite:
            def __init__(self, description, isopen):
                self.description = description
                self.isopen = isopen

            def __composite_values__(self):
                return [self.description, self.isopen]

            def __eq__(self, other):
                return (
                    isinstance(other, MyComposite)
                    and other.description == self.description
                )

        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={
                "composite": composite(
                    MyComposite,
                    orders.c.description,
                    orders.c.isopen,
                    active_history=True,
                )
            },
        )
        o1 = Order(composite=MyComposite("foo", 1))
        self._test_attribute(o1, "composite", MyComposite("bar", 1))


class InactiveHistoryNoRaiseTest(_fixtures.FixtureTest):
    run_inserts = None

    @testing.flag_combinations(
        dict(
            detached=False,
            raiseload=False,
            backref=False,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=True,
            raiseload=False,
            backref=False,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=False,
            raiseload=True,
            backref=False,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=True,
            raiseload=True,
            backref=False,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=False,
            raiseload=False,
            backref=True,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=True,
            raiseload=False,
            backref=True,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=False,
            raiseload=True,
            backref=True,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=True,
            raiseload=True,
            backref=True,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=True,
        ),
        #####
        dict(
            detached=False,
            raiseload=False,
            backref=False,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=False,
        ),
        dict(
            detached=True,
            raiseload=False,
            backref=False,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=False,
        ),
        dict(
            detached=False,
            raiseload=True,
            backref=False,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=False,
        ),
        dict(
            detached=True,
            raiseload=True,
            backref=False,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=False,
        ),
        dict(
            detached=False,
            raiseload=False,
            backref=True,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=False,
        ),
        dict(
            detached=True,
            raiseload=False,
            backref=True,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=False,
        ),
        dict(
            detached=False,
            raiseload=True,
            backref=True,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=False,
        ),
        dict(
            detached=True,
            raiseload=True,
            backref=True,
            delete=False,
            active_history=False,
            legacy_inactive_history_style=False,
        ),
        dict(
            detached=False,
            raiseload=False,
            backref=False,
            delete=False,
            active_history=True,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=True,
            raiseload=False,
            backref=False,
            delete=False,
            active_history=True,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=False,
            raiseload=True,
            backref=False,
            delete=False,
            active_history=True,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=True,
            raiseload=True,
            backref=False,
            delete=False,
            active_history=True,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=False,
            raiseload=False,
            backref=True,
            delete=False,
            active_history=True,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=True,
            raiseload=False,
            backref=True,
            delete=False,
            active_history=True,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=False,
            raiseload=True,
            backref=True,
            delete=False,
            active_history=True,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=True,
            raiseload=True,
            backref=True,
            delete=False,
            active_history=True,
            legacy_inactive_history_style=True,
        ),
        ####
        dict(
            detached=False,
            raiseload=False,
            backref=False,
            delete=True,
            active_history=False,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=True,
            raiseload=False,
            backref=False,
            delete=True,
            active_history=False,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=False,
            raiseload=True,
            backref=False,
            delete=True,
            active_history=False,
            legacy_inactive_history_style=True,
        ),
        dict(
            detached=True,
            raiseload=True,
            backref=False,
            delete=True,
            active_history=False,
            legacy_inactive_history_style=True,
        ),
        ###
        dict(
            detached=False,
            raiseload=False,
            backref=False,
            delete=True,
            active_history=False,
            legacy_inactive_history_style=False,
        ),
        dict(
            detached=True,
            raiseload=False,
            backref=False,
            delete=True,
            active_history=False,
            legacy_inactive_history_style=False,
        ),
        dict(
            detached=False,
            raiseload=True,
            backref=False,
            delete=True,
            active_history=False,
            legacy_inactive_history_style=False,
        ),
        dict(
            detached=True,
            raiseload=True,
            backref=False,
            delete=True,
            active_history=False,
            legacy_inactive_history_style=False,
        ),
        #
        dict(
            detached=False,
            raiseload=False,
            backref=False,
            delete=True,
            active_history=True,
        ),
        dict(
            detached=True,
            raiseload=False,
            backref=False,
            delete=True,
            active_history=True,
        ),
        dict(
            detached=False,
            raiseload=True,
            backref=False,
            delete=True,
            active_history=True,
        ),
        dict(
            detached=True,
            raiseload=True,
            backref=False,
            delete=True,
            active_history=True,
        ),
    )
    def test_m2o(
        self,
        detached,
        raiseload,
        backref,
        active_history,
        delete,
        legacy_inactive_history_style,
    ):
        if delete:
            assert not backref, "delete and backref are mutually exclusive"

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        opts = {}
        if active_history:
            opts["active_history"] = True
        if raiseload:
            opts["lazy"] = "raise"
        opts["_legacy_inactive_history_style"] = legacy_inactive_history_style

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "user": relationship(User, back_populates="addresses", **opts)
            },
        )
        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(Address, back_populates="user")
            },
        )

        s = fixture_session()

        a1 = Address(email_address="a1")
        u1 = User(name="u1", addresses=[a1])
        s.add_all([a1, u1])
        s.commit()

        if backref:
            u1.addresses

            if detached:
                s.expunge(a1)

            def go():
                u1.addresses = []

            if active_history:
                if raiseload:
                    assert_raises_message(
                        exc.InvalidRequestError,
                        "'Address.user' is not available due to lazy='raise'",
                        go,
                    )
                    return
                elif detached:
                    assert_raises_message(
                        orm_exc.DetachedInstanceError,
                        "lazy load operation of attribute 'user' "
                        "cannot proceed",
                        go,
                    )
                    return
            go()
        else:
            if detached:
                s.expunge(a1)

            if delete:

                def go():
                    del a1.user

            else:

                def go():
                    a1.user = None

            if active_history:
                if raiseload:
                    assert_raises_message(
                        exc.InvalidRequestError,
                        "'Address.user' is not available due to lazy='raise'",
                        go,
                    )
                    return
                elif detached:
                    assert_raises_message(
                        orm_exc.DetachedInstanceError,
                        "lazy load operation of attribute 'user' "
                        "cannot proceed",
                        go,
                    )
                    return
            go()

        if detached:
            s.add(a1)

        s.commit()

        eq_(s.query(Address).count(), 1)
        eq_(s.query(User).count(), 1)

        # test for issue #4997
        # delete of Address should proceed, as User object does not
        # need to be loaded
        s.delete(a1)
        s.commit()
        eq_(s.query(Address).count(), 0)
        eq_(s.query(User).count(), 1)


class RaiseLoadTest(_fixtures.FixtureTest):
    run_inserts = "once"
    run_deletes = None

    def test_o2m_raiseload_mapper(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(Address, addresses)
        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(addresses=relationship(Address, lazy="raise")),
        )
        q = fixture_session().query(User)
        result = [None]

        def go():
            x = q.filter(User.id == 7).all()
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "'User.addresses' is not available due to lazy='raise'",
                lambda: x[0].addresses,
            )
            result[0] = x

        self.assert_sql_count(testing.db, go, 1)

        self.assert_result(result[0], User, {"id": 7})

    def test_o2m_raiseload_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(Address, addresses)
        self.mapper_registry.map_imperatively(
            User, users, properties=dict(addresses=relationship(Address))
        )
        q = fixture_session().query(User)
        result = [None]

        def go():
            x = (
                q.options(sa.orm.raiseload(User.addresses))
                .filter(User.id == 7)
                .all()
            )
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "'User.addresses' is not available due to lazy='raise'",
                lambda: x[0].addresses,
            )
            result[0] = x

        self.assert_sql_count(testing.db, go, 1)

        self.assert_result(result[0], User, {"id": 7})

    def test_o2m_raiseload_lazyload_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(Address, addresses)
        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(addresses=relationship(Address, lazy="raise")),
        )
        q = (
            fixture_session()
            .query(User)
            .options(sa.orm.lazyload(User.addresses))
        )
        result = [None]

        def go():
            x = q.filter(User.id == 7).all()
            x[0].addresses
            result[0] = x

        self.sql_count_(2, go)

        self.assert_result(
            result[0], User, {"id": 7, "addresses": (Address, [{"id": 1}])}
        )

    def test_m2o_raiseload_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )
        self.mapper_registry.map_imperatively(
            Address, addresses, properties={"user": relationship(User)}
        )
        self.mapper_registry.map_imperatively(User, users)
        s = fixture_session()
        a1 = (
            s.query(Address)
            .filter_by(id=1)
            .options(sa.orm.raiseload(Address.user))
            .first()
        )

        def go():
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "'Address.user' is not available due to lazy='raise'",
                lambda: a1.user,
            )

        self.sql_count_(0, go)

    def test_m2o_raise_on_sql_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )
        self.mapper_registry.map_imperatively(
            Address, addresses, properties={"user": relationship(User)}
        )
        self.mapper_registry.map_imperatively(User, users)
        s = fixture_session()
        a1 = (
            s.query(Address)
            .filter_by(id=1)
            .options(sa.orm.raiseload(Address.user, sql_only=True))
            .first()
        )

        def go():
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "'Address.user' is not available due to lazy='raise_on_sql'",
                lambda: a1.user,
            )

        self.sql_count_(0, go)

        s.close()

        u1 = s.query(User).first()
        a1 = (
            s.query(Address)
            .filter_by(id=1)
            .options(sa.orm.raiseload(Address.user, sql_only=True))
            .first()
        )
        assert "user" not in a1.__dict__
        is_(a1.user, u1)

    def test_m2o_non_use_get_raise_on_sql_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )
        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User,
                    primaryjoin=sa.and_(
                        addresses.c.user_id == users.c.id,
                        users.c.name != None,  # noqa
                    ),
                )
            },
        )
        self.mapper_registry.map_imperatively(User, users)
        s = fixture_session()
        u1 = s.query(User).first()  # noqa
        a1 = (
            s.query(Address)
            .filter_by(id=1)
            .options(sa.orm.raiseload(Address.user, sql_only=True))
            .first()
        )

        def go():
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "'Address.user' is not available due to lazy='raise_on_sql'",
                lambda: a1.user,
            )

    def test_raiseload_from_eager_load(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )
        Dingaling, dingalings = self.classes.Dingaling, self.tables.dingalings
        self.mapper_registry.map_imperatively(Dingaling, dingalings)

        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties=dict(dingaling=relationship(Dingaling)),
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(addresses=relationship(Address)),
        )

        q = (
            fixture_session()
            .query(User)
            .options(joinedload(User.addresses).raiseload("*"))
            .filter_by(id=7)
        )
        u1 = q.first()
        assert "addresses" in u1.__dict__
        with expect_raises_message(
            sa.exc.InvalidRequestError,
            "'Address.dingaling' is not available due to lazy='raise'",
        ):
            u1.addresses[0].dingaling

    def test_raiseload_wildcard_all_classes_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(Address, addresses)
        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(addresses=relationship(Address, backref="user")),
        )
        q = (
            fixture_session()
            .query(User, Address)
            .join(Address, User.id == Address.user_id)
        )

        u1, a1 = q.options(sa.orm.raiseload("*")).filter(User.id == 7).first()

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "'User.addresses' is not available due to lazy='raise'",
            lambda: u1.addresses,
        )

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "'Address.user' is not available due to lazy='raise'",
            lambda: a1.user,
        )

        # columns still work
        eq_(u1.id, 7)
        eq_(a1.id, 1)

    def test_raiseload_wildcard_specific_class_option(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(Address, addresses)
        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(addresses=relationship(Address, backref="user")),
        )
        q = (
            fixture_session()
            .query(User, Address)
            .join(Address, User.id == Address.user_id)
        )

        u1, a1 = (
            q.options(sa.orm.Load(Address).raiseload("*"))
            .filter(User.id == 7)
            .first()
        )

        # User doesn't raise
        def go():
            eq_(u1.addresses, [a1])

        self.assert_sql_count(testing.db, go, 1)

        # Address does
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "'Address.user' is not available due to lazy='raise'",
            lambda: a1.user,
        )

        # columns still work
        eq_(u1.id, 7)
        eq_(a1.id, 1)


class RelationDeprecationTest(fixtures.MappedTest):
    """test usage of the old 'relation' function."""

    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(64)),
        )

        Table(
            "addresses_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer, ForeignKey("users_table.id")),
            Column("email_address", String(128)),
            Column("purpose", String(16)),
            Column("bounces", Integer, default=0),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Basic):
            pass

        class Address(cls.Basic):
            pass

    @classmethod
    def fixtures(cls):
        return dict(
            users_table=(
                ("id", "name"),
                (1, "jack"),
                (2, "ed"),
                (3, "fred"),
                (4, "chuck"),
            ),
            addresses_table=(
                ("id", "user_id", "email_address", "purpose", "bounces"),
                (1, 1, "jack@jack.home", "Personal", 0),
                (2, 1, "jack@jack.bizz", "Work", 1),
                (3, 2, "ed@foo.bar", "Personal", 0),
                (4, 3, "fred@the.fred", "Personal", 10),
            ),
        )

    def test_relationship(self):
        addresses_table, User, users_table, Address = (
            self.tables.addresses_table,
            self.classes.User,
            self.tables.users_table,
            self.classes.Address,
        )

        self.mapper_registry.map_imperatively(
            User,
            users_table,
            properties=dict(addresses=relationship(Address, backref="user")),
        )
        self.mapper_registry.map_imperatively(Address, addresses_table)

        session = fixture_session()

        session.query(User).filter(
            User.addresses.any(Address.email_address == "ed@foo.bar")
        ).one()


class SecondaryIncludesLocalColsTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("b_ids", String(50)),
        )

        Table("b", metadata, Column("id", String(10), primary_key=True))

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

        class B(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B = cls.classes("A", "B")
        a, b = cls.tables("a", "b")

        secondary = (
            select(a.c.id.label("aid"), b)
            .select_from(a.join(b, a.c.b_ids.like("%" + b.c.id + "%")))
            .alias()
        )

        cls.mapper_registry.map_imperatively(
            A,
            a,
            properties=dict(
                bs=relationship(
                    B,
                    secondary=secondary,
                    primaryjoin=a.c.id == secondary.c.aid,
                    secondaryjoin=b.c.id == secondary.c.id,
                )
            ),
        )
        cls.mapper_registry.map_imperatively(B, b)

    @classmethod
    def insert_data(cls, connection):
        A, B = cls.classes("A", "B")

        s = Session(connection)
        s.add_all(
            [
                A(id=1, b_ids="1"),
                A(id=2, b_ids="2 3"),
                B(id="1"),
                B(id="2"),
                B(id="3"),
            ]
        )
        s.commit()

    def test_query_join(self):
        A, B = self.classes("A", "B")

        s = fixture_session()

        with assert_engine(testing.db) as asserter_:
            rows = s.query(A.id, B.id).join(A.bs).order_by(A.id, B.id).all()

            eq_(rows, [(1, "1"), (2, "2"), (2, "3")])

        asserter_.assert_(
            CompiledSQL(
                "SELECT a.id AS a_id, b.id AS b_id FROM a JOIN "
                "(SELECT a.id AS "
                "aid, b.id AS id FROM a JOIN b ON a.b_ids LIKE (:id_1 || "
                "b.id || :param_1)) AS anon_1 ON a.id = anon_1.aid "
                "JOIN b ON b.id = anon_1.id ORDER BY a.id, b.id"
            )
        )

    def test_eager_join(self):
        A, B = self.classes("A", "B")

        s = fixture_session()

        with assert_engine(testing.db) as asserter_:
            a2 = (
                s.query(A).options(joinedload(A.bs)).filter(A.id == 2).all()[0]
            )

            eq_({b.id for b in a2.bs}, {"2", "3"})

        asserter_.assert_(
            CompiledSQL(
                "SELECT a.id AS a_id, a.b_ids AS a_b_ids, b_1.id AS b_1_id "
                "FROM a LEFT OUTER JOIN ((SELECT a.id AS aid, b.id AS id "
                "FROM a JOIN b ON a.b_ids LIKE (:id_1 || b.id || :param_1)) "
                "AS anon_1 JOIN b AS b_1 ON b_1.id = anon_1.id) "
                "ON a.id = anon_1.aid WHERE a.id = :id_2",
                params=[{"id_1": "%", "param_1": "%", "id_2": 2}],
            )
        )

    def test_exists(self):
        A, B = self.classes("A", "B")

        s = fixture_session()

        with assert_engine(testing.db) as asserter_:
            eq_({id_ for id_, in s.query(A.id).filter(A.bs.any())}, {1, 2})

        asserter_.assert_(
            CompiledSQL(
                "SELECT a.id AS a_id FROM a WHERE "
                "EXISTS (SELECT 1 FROM b, (SELECT a.id AS aid, b.id AS id "
                "FROM a JOIN b ON a.b_ids LIKE (:id_1 || b.id || :param_1)) "
                "AS anon_1 WHERE a.id = anon_1.aid AND b.id = anon_1.id)",
                params=[],
            )
        )

    def test_eager_selectin(self):
        A, B = self.classes("A", "B")

        s = fixture_session()

        with assert_engine(testing.db) as asserter_:
            a2 = (
                s.query(A)
                .options(selectinload(A.bs))
                .filter(A.id == 2)
                .all()[0]
            )

            eq_({b.id for b in a2.bs}, {"2", "3"})

        asserter_.assert_(
            CompiledSQL(
                "SELECT a.id AS a_id, a.b_ids AS a_b_ids "
                "FROM a WHERE a.id = :id_1",
                params=[{"id_1": 2}],
            ),
            CompiledSQL(
                "SELECT a_1.id AS a_1_id, b.id AS b_id FROM a AS a_1 JOIN "
                "(SELECT a.id AS aid, b.id AS id FROM a JOIN b ON a.b_ids "
                "LIKE (:id_1 || b.id || :param_1)) AS anon_1 "
                "ON a_1.id = anon_1.aid JOIN b ON b.id = anon_1.id "
                "WHERE a_1.id IN (__[POSTCOMPILE_primary_keys])",
                params=[{"id_1": "%", "param_1": "%", "primary_keys": [2]}],
            ),
        )
