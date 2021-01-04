from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import mapper
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class ABCTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global a, b, c
        a = Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("adata", String(30)),
            Column("type", String(30)),
        )
        b = Table(
            "b",
            metadata,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
            Column("bdata", String(30)),
        )
        c = Table(
            "c",
            metadata,
            Column("id", Integer, ForeignKey("b.id"), primary_key=True),
            Column("cdata", String(30)),
        )

    @testing.combinations(("union",), ("none",))
    def test_abc_poly_roundtrip(self, fetchtype):
        class A(fixtures.ComparableEntity):
            pass

        class B(A):
            pass

        class C(B):
            pass

        if fetchtype == "union":
            abc = a.outerjoin(b).outerjoin(c)
            bc = a.join(b).outerjoin(c)
        else:
            abc = bc = None

        mapper(
            A,
            a,
            with_polymorphic=("*", abc),
            polymorphic_on=a.c.type,
            polymorphic_identity="a",
        )
        mapper(
            B,
            b,
            with_polymorphic=("*", bc),
            inherits=A,
            polymorphic_identity="b",
        )
        mapper(C, c, inherits=B, polymorphic_identity="c")

        a1 = A(adata="a1")
        b1 = B(bdata="b1", adata="b1")
        b2 = B(bdata="b2", adata="b2")
        b3 = B(bdata="b3", adata="b3")
        c1 = C(cdata="c1", bdata="c1", adata="c1")
        c2 = C(cdata="c2", bdata="c2", adata="c2")
        c3 = C(cdata="c2", bdata="c2", adata="c2")

        sess = fixture_session()
        for x in (a1, b1, b2, b3, c1, c2, c3):
            sess.add(x)
        sess.flush()
        sess.expunge_all()

        # for obj in sess.query(A).all():
        #    print obj
        eq_(
            [
                A(adata="a1"),
                B(bdata="b1", adata="b1"),
                B(bdata="b2", adata="b2"),
                B(bdata="b3", adata="b3"),
                C(cdata="c1", bdata="c1", adata="c1"),
                C(cdata="c2", bdata="c2", adata="c2"),
                C(cdata="c2", bdata="c2", adata="c2"),
            ],
            sess.query(A).order_by(A.id).all(),
        )

        eq_(
            [
                B(bdata="b1", adata="b1"),
                B(bdata="b2", adata="b2"),
                B(bdata="b3", adata="b3"),
                C(cdata="c1", bdata="c1", adata="c1"),
                C(cdata="c2", bdata="c2", adata="c2"),
                C(cdata="c2", bdata="c2", adata="c2"),
            ],
            sess.query(B).order_by(A.id).all(),
        )

        eq_(
            [
                C(cdata="c1", bdata="c1", adata="c1"),
                C(cdata="c2", bdata="c2", adata="c2"),
                C(cdata="c2", bdata="c2", adata="c2"),
            ],
            sess.query(C).order_by(A.id).all(),
        )
