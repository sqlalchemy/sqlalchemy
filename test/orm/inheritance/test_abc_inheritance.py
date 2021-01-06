from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import mapper
from sqlalchemy.orm import polymorphic_union
from sqlalchemy.orm import relationship
from sqlalchemy.orm.interfaces import MANYTOONE
from sqlalchemy.orm.interfaces import ONETOMANY
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


def _combinations():
    for parent in ["a", "b", "c"]:
        for child in ["a", "b", "c"]:
            for direction in [ONETOMANY, MANYTOONE]:

                name = "Test%sTo%s%s" % (
                    parent,
                    child,
                    (direction is ONETOMANY and "O2M" or "M2O"),
                )
                yield (name, parent, child, direction)


@testing.combinations(
    *list(_combinations()), argnames="name,parent,child,direction", id_="saaa"
)
class ABCTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        parent, child, direction = cls.parent, cls.child, cls.direction

        ta = ["a", metadata]
        ta.append(
            Column(
                "id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            )
        ),
        ta.append(Column("a_data", String(30)))
        if "a" == parent and direction == MANYTOONE:
            ta.append(
                Column(
                    "child_id",
                    Integer,
                    ForeignKey("%s.id" % child, use_alter=True, name="foo"),
                )
            )
        elif "a" == child and direction == ONETOMANY:
            ta.append(
                Column(
                    "parent_id",
                    Integer,
                    ForeignKey("%s.id" % parent, use_alter=True, name="foo"),
                )
            )
        ta = Table(*ta)

        tb = ["b", metadata]
        tb.append(Column("id", Integer, ForeignKey("a.id"), primary_key=True))

        tb.append(Column("b_data", String(30)))

        if "b" == parent and direction == MANYTOONE:
            tb.append(
                Column(
                    "child_id",
                    Integer,
                    ForeignKey("%s.id" % child, use_alter=True, name="foo"),
                )
            )
        elif "b" == child and direction == ONETOMANY:
            tb.append(
                Column(
                    "parent_id",
                    Integer,
                    ForeignKey("%s.id" % parent, use_alter=True, name="foo"),
                )
            )
        tb = Table(*tb)

        tc = ["c", metadata]
        tc.append(Column("id", Integer, ForeignKey("b.id"), primary_key=True))

        tc.append(Column("c_data", String(30)))

        if "c" == parent and direction == MANYTOONE:
            tc.append(
                Column(
                    "child_id",
                    Integer,
                    ForeignKey("%s.id" % child, use_alter=True, name="foo"),
                )
            )
        elif "c" == child and direction == ONETOMANY:
            tc.append(
                Column(
                    "parent_id",
                    Integer,
                    ForeignKey("%s.id" % parent, use_alter=True, name="foo"),
                )
            )
        tc = Table(*tc)

    @classmethod
    def setup_mappers(cls):
        parent, child, direction = cls.parent, cls.child, cls.direction
        ta, tb, tc = cls.tables("a", "b", "c")
        parent_table = {"a": ta, "b": tb, "c": tc}[parent]
        child_table = {"a": ta, "b": tb, "c": tc}[child]

        remote_side = None

        if direction == MANYTOONE:
            foreign_keys = [parent_table.c.child_id]
        elif direction == ONETOMANY:
            foreign_keys = [child_table.c.parent_id]

        atob = ta.c.id == tb.c.id
        btoc = tc.c.id == tb.c.id

        if direction == ONETOMANY:
            relationshipjoin = parent_table.c.id == child_table.c.parent_id
        elif direction == MANYTOONE:
            relationshipjoin = parent_table.c.child_id == child_table.c.id
            if parent is child:
                remote_side = [child_table.c.id]

        abcjoin = polymorphic_union(
            {
                "a": ta.select()
                .where(tb.c.id == None)  # noqa
                .select_from(ta.outerjoin(tb, onclause=atob))
                .subquery(),
                "b": ta.join(tb, onclause=atob)
                .outerjoin(tc, onclause=btoc)
                .select()
                .where(tc.c.id == None)
                .reduce_columns()
                .subquery(),  # noqa
                "c": tc.join(tb, onclause=btoc).join(ta, onclause=atob),
            },
            "type",
            "abcjoin",
        )

        bcjoin = polymorphic_union(
            {
                "b": ta.join(tb, onclause=atob)
                .outerjoin(tc, onclause=btoc)
                .select()
                .where(tc.c.id == None)
                .reduce_columns()
                .subquery(),  # noqa
                "c": tc.join(tb, onclause=btoc).join(ta, onclause=atob),
            },
            "type",
            "bcjoin",
        )

        class A(cls.Comparable):
            def __init__(self, name):
                self.a_data = name

        class B(A):
            pass

        class C(B):
            pass

        mapper(
            A,
            ta,
            polymorphic_on=abcjoin.c.type,
            with_polymorphic=("*", abcjoin),
            polymorphic_identity="a",
        )
        mapper(
            B,
            tb,
            polymorphic_on=bcjoin.c.type,
            with_polymorphic=("*", bcjoin),
            polymorphic_identity="b",
            inherits=A,
            inherit_condition=atob,
        )
        mapper(
            C,
            tc,
            polymorphic_identity="c",
            with_polymorphic=("*", tc.join(tb, btoc).join(ta, atob)),
            inherits=B,
            inherit_condition=btoc,
        )

        parent_mapper = class_mapper({ta: A, tb: B, tc: C}[parent_table])
        child_mapper = class_mapper({ta: A, tb: B, tc: C}[child_table])

        parent_mapper.add_property(
            "collection",
            relationship(
                child_mapper,
                primaryjoin=relationshipjoin,
                foreign_keys=foreign_keys,
                order_by=child_mapper.c.id,
                remote_side=remote_side,
                uselist=True,
            ),
        )

    def test_roundtrip(self):
        parent, child, direction = self.parent, self.child, self.direction
        A, B, C = self.classes("A", "B", "C")
        parent_class = {"a": A, "b": B, "c": C}[parent]
        child_class = {"a": A, "b": B, "c": C}[child]

        sess = fixture_session(autoflush=False, expire_on_commit=False)

        parent_obj = parent_class("parent1")
        child_obj = child_class("child1")
        somea = A("somea")
        someb = B("someb")
        somec = C("somec")

        # print "APPENDING", parent.__class__.__name__ , "TO",
        # child.__class__.__name__

        sess.add(parent_obj)
        parent_obj.collection.append(child_obj)
        if direction == ONETOMANY:
            child2 = child_class("child2")
            parent_obj.collection.append(child2)
            sess.add(child2)
        elif direction == MANYTOONE:
            parent2 = parent_class("parent2")
            parent2.collection.append(child_obj)
            sess.add(parent2)
        sess.add(somea)
        sess.add(someb)
        sess.add(somec)
        sess.commit()
        sess.close()

        # assert result via direct get() of parent object
        result = sess.get(parent_class, parent_obj.id)
        assert result.id == parent_obj.id
        assert result.collection[0].id == child_obj.id
        if direction == ONETOMANY:
            assert result.collection[1].id == child2.id
        elif direction == MANYTOONE:
            result2 = sess.get(parent_class, parent2.id)
            assert result2.id == parent2.id
            assert result2.collection[0].id == child_obj.id

        sess.expunge_all()

        # assert result via polymorphic load of parent object
        result = sess.query(A).filter_by(id=parent_obj.id).one()
        assert result.id == parent_obj.id
        assert result.collection[0].id == child_obj.id
        if direction == ONETOMANY:
            assert result.collection[1].id == child2.id
        elif direction == MANYTOONE:
            result2 = sess.query(A).filter_by(id=parent2.id).one()
            assert result2.id == parent2.id
            assert result2.collection[0].id == child_obj.id
