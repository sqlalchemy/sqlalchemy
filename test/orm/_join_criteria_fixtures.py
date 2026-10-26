"""Loader-agnostic tests for relationships whose primaryjoin or
secondaryjoin includes criteria beyond the foreign key comparison.

Each relationship loader suite subclasses
:class:`._JoinCriteriaLoaderTest`, establishing the loader option under
test and the number of statements it is expected to emit.

Collection tests additionally run against an aliased target, established
using :meth:`.PropComparator.of_type` with either an :func:`.aliased`
construct or a single-inheritance subclass, both of which require that the
join criteria be adapted to the aliased target.

"""

from sqlalchemy import and_
from sqlalchemy import Boolean
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import aliased
from sqlalchemy.orm import relationship
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class _JoinCriteriaLoaderTest(fixtures.MappedTest):
    """Fixture data is arranged such that every criteria excludes rows
    for at least one parent, and a3 has an empty collection for most
    relationships.

    Each table has a boolean column "x" that's the target of the criteria;
    rows below are listed as (x=True) or (x=False).  Rows marked with
    ``*`` are of the single-inheritance subclass BSub or CSub:

    .. sourcecode:: text

        a:   a1 (x=True), a2 (x=False), a3 (x=True)
        b:   b1* (x=True), b2 (x=False), b3* (x=True)
        a_b: a1 -> b1 (x=True), b2 (x=True), b3 (x=False)
             a2 -> b1 (x=False), b2 (x=True), b3 (x=True)
        c:   c1* -> a1 (x=True), c2 -> a1 (x=False),
             c3* -> a2 (x=True), c4 -> a2 (x=False),
             c5* -> a3 (x=True)

    """

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    loader_option = None
    """loader option function, e.g. ``selectinload``."""

    statement_count = None
    """number of statements expected to load three parent rows and their
    related rows; lazy-style loaders emit one statement per parent."""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", Boolean, nullable=False),
        )
        Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("type", String(10), nullable=False),
            Column("x", Boolean, nullable=False),
        )
        Table(
            "a_b",
            metadata,
            Column("a_id", ForeignKey("a.id"), primary_key=True),
            Column("b_id", ForeignKey("b.id"), primary_key=True),
            Column("x", Boolean, nullable=False),
        )
        Table(
            "c",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("a_id", ForeignKey("a.id")),
            Column("type", String(10), nullable=False),
            Column("x", Boolean, nullable=False),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

        class B(cls.Comparable):
            pass

        class BSub(B):
            pass

        class C(cls.Comparable):
            pass

        class CSub(C):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B, BSub, C, CSub = cls.classes("A", "B", "BSub", "C", "CSub")
        a, b, a_b, c = cls.tables("a", "b", "a_b", "c")

        cls.mapper_registry.map_imperatively(
            A,
            a,
            properties={
                "bs": relationship(
                    B, secondary=a_b, order_by=b.c.id, viewonly=True
                ),
                "bs_pj_secondary": relationship(
                    B,
                    secondary=a_b,
                    primaryjoin=and_(
                        a.c.id == a_b.c.a_id,
                        a_b.c.x == True,  # noqa: E712
                    ),
                    order_by=b.c.id,
                    viewonly=True,
                ),
                "bs_pj_parent": relationship(
                    B,
                    secondary=a_b,
                    primaryjoin=and_(
                        a.c.id == a_b.c.a_id,
                        a.c.x == True,  # noqa: E712
                    ),
                    order_by=b.c.id,
                    viewonly=True,
                ),
                "bs_pj_parent_to_secondary": relationship(
                    B,
                    secondary=a_b,
                    primaryjoin=and_(
                        a.c.id == a_b.c.a_id,
                        a.c.x == a_b.c.x,
                    ),
                    order_by=b.c.id,
                    viewonly=True,
                ),
                "bs_sj_secondary": relationship(
                    B,
                    secondary=a_b,
                    secondaryjoin=and_(
                        b.c.id == a_b.c.b_id,
                        a_b.c.x == True,  # noqa: E712
                    ),
                    order_by=b.c.id,
                    viewonly=True,
                ),
                "bs_sj_target": relationship(
                    B,
                    secondary=a_b,
                    secondaryjoin=and_(
                        b.c.id == a_b.c.b_id,
                        b.c.x == True,  # noqa: E712
                    ),
                    order_by=b.c.id,
                    viewonly=True,
                ),
                "cs": relationship(C, order_by=c.c.id, viewonly=True),
                "cs_pj_target": relationship(
                    C,
                    primaryjoin=and_(
                        a.c.id == c.c.a_id,
                        c.c.x == True,  # noqa: E712
                    ),
                    order_by=c.c.id,
                    viewonly=True,
                ),
                "cs_pj_parent": relationship(
                    C,
                    primaryjoin=and_(
                        a.c.id == c.c.a_id,
                        a.c.x == True,  # noqa: E712
                    ),
                    order_by=c.c.id,
                    viewonly=True,
                ),
            },
        )
        cls.mapper_registry.map_imperatively(
            B, b, polymorphic_on=b.c.type, polymorphic_identity="b"
        )
        cls.mapper_registry.map_imperatively(
            BSub, inherits=B, polymorphic_identity="bsub"
        )
        cls.mapper_registry.map_imperatively(
            C,
            c,
            polymorphic_on=c.c.type,
            polymorphic_identity="c",
            properties={
                "a": relationship(A, viewonly=True),
                "a_pj_target": relationship(
                    A,
                    primaryjoin=and_(
                        c.c.a_id == a.c.id,
                        a.c.x == True,  # noqa: E712
                    ),
                    viewonly=True,
                ),
            },
        )
        cls.mapper_registry.map_imperatively(
            CSub, inherits=C, polymorphic_identity="csub"
        )

    @classmethod
    def insert_data(cls, connection):
        a, b, a_b, c = cls.tables("a", "b", "a_b", "c")

        connection.execute(
            a.insert(),
            [
                {"id": 1, "x": True},
                {"id": 2, "x": False},
                {"id": 3, "x": True},
            ],
        )
        connection.execute(
            b.insert(),
            [
                {"id": 1, "type": "bsub", "x": True},
                {"id": 2, "type": "b", "x": False},
                {"id": 3, "type": "bsub", "x": True},
            ],
        )
        connection.execute(
            a_b.insert(),
            [
                {"a_id": 1, "b_id": 1, "x": True},
                {"a_id": 1, "b_id": 2, "x": True},
                {"a_id": 1, "b_id": 3, "x": False},
                {"a_id": 2, "b_id": 1, "x": False},
                {"a_id": 2, "b_id": 2, "x": True},
                {"a_id": 2, "b_id": 3, "x": True},
            ],
        )
        connection.execute(
            c.insert(),
            [
                {"id": 1, "a_id": 1, "type": "csub", "x": True},
                {"id": 2, "a_id": 1, "type": "c", "x": False},
                {"id": 3, "a_id": 2, "type": "csub", "x": True},
                {"id": 4, "a_id": 2, "type": "c", "x": False},
                {"id": 5, "a_id": 3, "type": "csub", "x": True},
            ],
        )

    def _assert_collection(self, attrname, target, expected):
        A = self.classes.A
        attr = getattr(A, attrname)

        B, BSub, C, CSub = self.classes("B", "BSub", "C", "CSub")

        if target.aliased:
            attr = attr.of_type(aliased(attr.entity.class_))
        elif target.subclass:
            attr = attr.of_type({B: BSub, C: CSub}[attr.entity.class_])
        elif target.plain:
            pass
        else:
            target.fail()

        sess = fixture_session()

        def go():
            stmt = select(A).options(self.loader_option(attr)).order_by(A.id)
            return {
                a.id: [rel.id for rel in getattr(a, attrname)]
                for a in sess.scalars(stmt).unique()
            }

        result = self.assert_sql_count(testing.db, go, self.statement_count)
        eq_(result, expected)

    def _assert_scalar(self, attrname, expected):
        C = self.classes.C
        attr = getattr(C, attrname)

        sess = fixture_session()

        def go():
            # load c1, c3, c5, which each refer to a different A, so that
            # loaders which use get() against the identity map emit the
            # same number of statements as those which can't
            stmt = (
                select(C)
                .options(self.loader_option(attr))
                .filter(C.id.in_([1, 3, 5]))
                .order_by(C.id)
            )
            return {
                c.id: (
                    getattr(c, attrname).id
                    if getattr(c, attrname) is not None
                    else None
                )
                for c in sess.scalars(stmt).unique()
            }

        result = self.assert_sql_count(testing.db, go, self.statement_count)
        eq_(result, expected)

    @testing.variation("target", ["plain", "aliased", "subclass"])
    def test_m2m_plain(self, target):
        self._assert_collection(
            "bs", target, {1: [1, 2, 3], 2: [1, 2, 3], 3: []}
        )

    @testing.variation("target", ["plain", "aliased", "subclass"])
    def test_m2m_primaryjoin_criteria_on_secondary(self, target):
        """test #13626"""
        self._assert_collection(
            "bs_pj_secondary", target, {1: [1, 2], 2: [2, 3], 3: []}
        )

    @testing.variation("target", ["plain", "aliased", "subclass"])
    def test_m2m_primaryjoin_criteria_on_parent(self, target):
        """test #13626"""
        self._assert_collection(
            "bs_pj_parent", target, {1: [1, 2, 3], 2: [], 3: []}
        )

    @testing.variation("target", ["plain", "aliased", "subclass"])
    def test_m2m_primaryjoin_criteria_parent_to_secondary(self, target):
        """test #13626"""
        self._assert_collection(
            "bs_pj_parent_to_secondary", target, {1: [1, 2], 2: [1], 3: []}
        )

    @testing.variation("target", ["plain", "aliased", "subclass"])
    def test_m2m_secondaryjoin_criteria_on_secondary(self, target):
        self._assert_collection(
            "bs_sj_secondary", target, {1: [1, 2], 2: [2, 3], 3: []}
        )

    @testing.variation("target", ["plain", "aliased", "subclass"])
    def test_m2m_secondaryjoin_criteria_on_target(self, target):
        self._assert_collection(
            "bs_sj_target", target, {1: [1, 3], 2: [1, 3], 3: []}
        )

    @testing.variation("target", ["plain", "aliased", "subclass"])
    def test_o2m_plain(self, target):
        self._assert_collection("cs", target, {1: [1, 2], 2: [3, 4], 3: [5]})

    @testing.variation("target", ["plain", "aliased", "subclass"])
    def test_o2m_primaryjoin_criteria_on_target(self, target):
        self._assert_collection(
            "cs_pj_target", target, {1: [1], 2: [3], 3: [5]}
        )

    @testing.variation("target", ["plain", "aliased", "subclass"])
    def test_o2m_primaryjoin_criteria_on_parent(self, target):
        self._assert_collection(
            "cs_pj_parent", target, {1: [1, 2], 2: [], 3: [5]}
        )

    def test_m2o_plain(self):
        self._assert_scalar("a", {1: 1, 3: 2, 5: 3})

    def test_m2o_primaryjoin_criteria_on_target(self):
        self._assert_scalar("a_pj_target", {1: 1, 3: None, 5: 3})
