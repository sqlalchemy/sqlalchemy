from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import aliased
from sqlalchemy.orm import backref
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.sql.selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class Company(fixtures.ComparableEntity):
    pass


class Person(fixtures.ComparableEntity):
    pass


class Engineer(Person):
    pass


class Manager(Person):
    pass


class Boss(Manager):
    pass


class Machine(fixtures.ComparableEntity):
    pass


class Paperwork(fixtures.ComparableEntity):
    pass


class SelfReferentialTestJoinedToBase(fixtures.MappedTest):

    run_setup_mappers = "once"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("type", String(30)),
        )

        Table(
            "engineers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("primary_language", String(50)),
            Column("reports_to_id", Integer, ForeignKey("people.person_id")),
        )

    @classmethod
    def setup_mappers(cls):
        engineers, people = cls.tables.engineers, cls.tables.people

        mapper(
            Person,
            people,
            polymorphic_on=people.c.type,
            polymorphic_identity="person",
        )

        mapper(
            Engineer,
            engineers,
            inherits=Person,
            inherit_condition=engineers.c.person_id == people.c.person_id,
            polymorphic_identity="engineer",
            properties={
                "reports_to": relationship(
                    Person,
                    primaryjoin=(
                        people.c.person_id == engineers.c.reports_to_id
                    ),
                )
            },
        )

    def test_has(self):
        p1 = Person(name="dogbert")
        e1 = Engineer(name="dilbert", primary_language="java", reports_to=p1)
        sess = fixture_session()
        sess.add(p1)
        sess.add(e1)
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(Engineer)
            .filter(Engineer.reports_to.has(Person.name == "dogbert"))
            .first(),
            Engineer(name="dilbert"),
        )

    def test_oftype_aliases_in_exists(self):
        e1 = Engineer(name="dilbert", primary_language="java")
        e2 = Engineer(name="wally", primary_language="c++", reports_to=e1)
        sess = fixture_session()
        sess.add_all([e1, e2])
        sess.flush()
        eq_(
            sess.query(Engineer)
            .filter(
                Engineer.reports_to.of_type(Engineer).has(
                    Engineer.name == "dilbert"
                )
            )
            .first(),
            e2,
        )

    def test_join(self):
        p1 = Person(name="dogbert")
        e1 = Engineer(name="dilbert", primary_language="java", reports_to=p1)
        sess = fixture_session()
        sess.add(p1)
        sess.add(e1)
        sess.flush()
        sess.expunge_all()
        pa = aliased(Person)
        eq_(
            sess.query(Engineer)
            .join(pa, "reports_to")
            .filter(pa.name == "dogbert")
            .first(),
            Engineer(name="dilbert"),
        )


class SelfReferentialJ2JTest(fixtures.MappedTest):

    run_setup_mappers = "once"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("type", String(30)),
        )

        Table(
            "engineers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("primary_language", String(50)),
            Column("reports_to_id", Integer, ForeignKey("managers.person_id")),
        )

        Table(
            "managers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
        )

    @classmethod
    def setup_mappers(cls):
        engineers = cls.tables.engineers
        managers = cls.tables.managers
        people = cls.tables.people

        mapper(
            Person,
            people,
            polymorphic_on=people.c.type,
            polymorphic_identity="person",
        )

        mapper(
            Manager, managers, inherits=Person, polymorphic_identity="manager"
        )

        mapper(
            Engineer,
            engineers,
            inherits=Person,
            polymorphic_identity="engineer",
            properties={
                "reports_to": relationship(
                    Manager,
                    primaryjoin=(
                        managers.c.person_id == engineers.c.reports_to_id
                    ),
                    backref="engineers",
                )
            },
        )

    def test_has(self):
        m1 = Manager(name="dogbert")
        e1 = Engineer(name="dilbert", primary_language="java", reports_to=m1)
        sess = fixture_session()
        sess.add(m1)
        sess.add(e1)
        sess.flush()
        sess.expunge_all()

        eq_(
            sess.query(Engineer)
            .filter(Engineer.reports_to.has(Manager.name == "dogbert"))
            .first(),
            Engineer(name="dilbert"),
        )

    def test_join(self):
        m1 = Manager(name="dogbert")
        e1 = Engineer(name="dilbert", primary_language="java", reports_to=m1)
        sess = fixture_session()
        sess.add(m1)
        sess.add(e1)
        sess.flush()
        sess.expunge_all()

        ma = aliased(Manager)

        eq_(
            sess.query(Engineer)
            .join(ma, "reports_to")
            .filter(ma.name == "dogbert")
            .first(),
            Engineer(name="dilbert"),
        )

    def test_filter_aliasing(self):
        m1 = Manager(name="dogbert")
        m2 = Manager(name="foo")
        e1 = Engineer(name="wally", primary_language="java", reports_to=m1)
        e2 = Engineer(name="dilbert", primary_language="c++", reports_to=m2)
        e3 = Engineer(name="etc", primary_language="c++")

        sess = fixture_session()
        sess.add_all([m1, m2, e1, e2, e3])
        sess.flush()
        sess.expunge_all()

        # filter aliasing applied to Engineer doesn't whack Manager
        eq_(
            sess.query(Manager)
            .join(Manager.engineers)
            .filter(Manager.name == "dogbert")
            .all(),
            [m1],
        )

        eq_(
            sess.query(Manager)
            .join(Manager.engineers)
            .filter(Engineer.name == "dilbert")
            .all(),
            [m2],
        )

        eq_(
            sess.query(Manager, Engineer)
            .join(Manager.engineers)
            .order_by(Manager.name.desc())
            .all(),
            [(m2, e2), (m1, e1)],
        )

    def test_relationship_compare(self):
        m1 = Manager(name="dogbert")
        m2 = Manager(name="foo")
        e1 = Engineer(name="dilbert", primary_language="java", reports_to=m1)
        e2 = Engineer(name="wally", primary_language="c++", reports_to=m2)
        e3 = Engineer(name="etc", primary_language="c++")

        sess = fixture_session()
        sess.add(m1)
        sess.add(m2)
        sess.add(e1)
        sess.add(e2)
        sess.add(e3)
        sess.flush()
        sess.expunge_all()

        eq_(
            sess.query(Manager)
            .join(Manager.engineers)
            .filter(Engineer.reports_to == None)
            .all(),  # noqa
            [],
        )

        eq_(
            sess.query(Manager)
            .join(Manager.engineers)
            .filter(Engineer.reports_to == m1)
            .all(),
            [m1],
        )


class SelfReferentialJ2JSelfTest(fixtures.MappedTest):

    run_setup_mappers = "once"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("type", String(30)),
        )

        Table(
            "engineers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column(
                "reports_to_id", Integer, ForeignKey("engineers.person_id")
            ),
        )

    @classmethod
    def setup_mappers(cls):
        engineers = cls.tables.engineers
        people = cls.tables.people

        mapper(
            Person,
            people,
            polymorphic_on=people.c.type,
            polymorphic_identity="person",
        )

        mapper(
            Engineer,
            engineers,
            inherits=Person,
            polymorphic_identity="engineer",
            properties={
                "reports_to": relationship(
                    Engineer,
                    primaryjoin=(
                        engineers.c.person_id == engineers.c.reports_to_id
                    ),
                    backref="engineers",
                    remote_side=engineers.c.person_id,
                )
            },
        )

    def _two_obj_fixture(self):
        e1 = Engineer(name="wally")
        e2 = Engineer(name="dilbert", reports_to=e1)
        sess = fixture_session()
        sess.add_all([e1, e2])
        sess.commit()
        return sess

    def _five_obj_fixture(self):
        sess = fixture_session()
        e1, e2, e3, e4, e5 = [Engineer(name="e%d" % (i + 1)) for i in range(5)]
        e3.reports_to = e1
        e4.reports_to = e2
        sess.add_all([e1, e2, e3, e4, e5])
        sess.commit()
        return sess

    def test_has(self):
        sess = self._two_obj_fixture()
        eq_(
            sess.query(Engineer)
            .filter(Engineer.reports_to.has(Engineer.name == "wally"))
            .first(),
            Engineer(name="dilbert"),
        )

    def test_join_explicit_alias(self):
        sess = self._five_obj_fixture()
        ea = aliased(Engineer)
        eq_(
            sess.query(Engineer)
            .join(ea, Engineer.engineers)
            .filter(Engineer.name == "e1")
            .all(),
            [Engineer(name="e1")],
        )

    def test_join_aliased_one(self):
        sess = self._two_obj_fixture()
        ea = aliased(Engineer)
        eq_(
            sess.query(Engineer)
            .join(ea, "reports_to")
            .filter(ea.name == "wally")
            .first(),
            Engineer(name="dilbert"),
        )

    def test_join_aliased_two(self):
        sess = self._five_obj_fixture()
        ea = aliased(Engineer)
        eq_(
            sess.query(Engineer)
            .join(ea, Engineer.engineers)
            .filter(ea.name == "e4")
            .all(),
            [Engineer(name="e2")],
        )

    def test_relationship_compare(self):
        sess = self._five_obj_fixture()
        e1 = sess.query(Engineer).filter_by(name="e1").one()
        e2 = sess.query(Engineer).filter_by(name="e2").one()

        ea = aliased(Engineer)
        eq_(
            sess.query(Engineer)
            .join(ea, Engineer.engineers)
            .filter(ea.reports_to == None)
            .all(),  # noqa
            [],
        )

        eq_(
            sess.query(Engineer)
            .join(ea, Engineer.engineers)
            .filter(ea.reports_to == e1)
            .all(),
            [e1],
        )

        eq_(
            sess.query(Engineer)
            .join(ea, Engineer.engineers)
            .filter(ea.reports_to != None)
            .all(),  # noqa
            [e1, e2],
        )


class M2MFilterTest(fixtures.MappedTest):

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "organizations",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
        )

        Table(
            "engineers_to_org",
            metadata,
            Column("org_id", Integer, ForeignKey("organizations.id")),
            Column("engineer_id", Integer, ForeignKey("engineers.person_id")),
        )

        Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("type", String(30)),
        )

        Table(
            "engineers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("primary_language", String(50)),
        )

    @classmethod
    def setup_mappers(cls):
        organizations = cls.tables.organizations
        people = cls.tables.people
        engineers = cls.tables.engineers
        engineers_to_org = cls.tables.engineers_to_org

        class Organization(cls.Comparable):
            pass

        mapper(
            Organization,
            organizations,
            properties={
                "engineers": relationship(
                    Engineer,
                    secondary=engineers_to_org,
                    backref="organizations",
                )
            },
        )

        mapper(
            Person,
            people,
            polymorphic_on=people.c.type,
            polymorphic_identity="person",
        )

        mapper(
            Engineer,
            engineers,
            inherits=Person,
            polymorphic_identity="engineer",
        )

    @classmethod
    def insert_data(cls, connection):
        Organization = cls.classes.Organization
        e1 = Engineer(name="e1")
        e2 = Engineer(name="e2")
        e3 = Engineer(name="e3")
        e4 = Engineer(name="e4")
        org1 = Organization(name="org1", engineers=[e1, e2])
        org2 = Organization(name="org2", engineers=[e3, e4])
        with sessionmaker(connection).begin() as sess:
            sess.add(org1)
            sess.add(org2)

    def test_not_contains(self):
        Organization = self.classes.Organization
        sess = fixture_session()
        e1 = sess.query(Person).filter(Engineer.name == "e1").one()

        eq_(
            sess.query(Organization)
            .filter(~Organization.engineers.of_type(Engineer).contains(e1))
            .all(),
            [Organization(name="org2")],
        )

        # this had a bug
        eq_(
            sess.query(Organization)
            .filter(~Organization.engineers.contains(e1))
            .all(),
            [Organization(name="org2")],
        )

    def test_any(self):
        sess = fixture_session()
        Organization = self.classes.Organization

        eq_(
            sess.query(Organization)
            .filter(
                Organization.engineers.of_type(Engineer).any(
                    Engineer.name == "e1"
                )
            )
            .all(),
            [Organization(name="org1")],
        )

        eq_(
            sess.query(Organization)
            .filter(Organization.engineers.any(Engineer.name == "e1"))
            .all(),
            [Organization(name="org1")],
        )


class SelfReferentialM2MTest(fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = "default"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "secondary",
            metadata,
            Column(
                "left_id", Integer, ForeignKey("parent.id"), nullable=False
            ),
            Column(
                "right_id", Integer, ForeignKey("parent.id"), nullable=False
            ),
        )

        Table(
            "parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("cls", String(50)),
        )

        Table(
            "child1",
            metadata,
            Column("id", Integer, ForeignKey("parent.id"), primary_key=True),
        )

        Table(
            "child2",
            metadata,
            Column("id", Integer, ForeignKey("parent.id"), primary_key=True),
        )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

        class Child1(Parent):
            pass

        class Child2(Parent):
            pass

    @classmethod
    def setup_mappers(cls):
        child1 = cls.tables.child1
        child2 = cls.tables.child2
        Parent = cls.classes.Parent
        parent = cls.tables.parent
        Child1 = cls.classes.Child1
        Child2 = cls.classes.Child2
        secondary = cls.tables.secondary

        mapper(Parent, parent, polymorphic_on=parent.c.cls)

        mapper(
            Child1,
            child1,
            inherits=Parent,
            polymorphic_identity="child1",
            properties={
                "left_child2": relationship(
                    Child2,
                    secondary=secondary,
                    primaryjoin=parent.c.id == secondary.c.right_id,
                    secondaryjoin=parent.c.id == secondary.c.left_id,
                    uselist=False,
                    backref="right_children",
                )
            },
        )

        mapper(Child2, child2, inherits=Parent, polymorphic_identity="child2")

    def test_query_crit(self):
        Child1, Child2 = self.classes.Child1, self.classes.Child2
        sess = fixture_session()
        c11, c12, c13 = Child1(), Child1(), Child1()
        c21, c22, c23 = Child2(), Child2(), Child2()
        c11.left_child2 = c22
        c12.left_child2 = c22
        c13.left_child2 = c23
        sess.add_all([c11, c12, c13, c21, c22, c23])
        sess.flush()

        # test that the join to Child2 doesn't alias Child1 in the select

        stmt = select(Child1).join(Child1.left_child2)
        eq_(
            set(sess.execute(stmt).scalars().unique()),
            set([c11, c12, c13]),
        )

        eq_(
            set(sess.query(Child1, Child2).join(Child1.left_child2)),
            set([(c11, c22), (c12, c22), (c13, c23)]),
        )

        # test __eq__() on property is annotating correctly

        stmt = (
            select(Child2)
            .join(Child2.right_children)
            .where(Child1.left_child2 == c22)
        )
        eq_(
            set(sess.execute(stmt).scalars().unique()),
            set([c22]),
        )

        # test the same again
        self.assert_compile(
            sess.query(Child2)
            .join(Child2.right_children)
            .filter(Child1.left_child2 == c22)
            .statement,
            "SELECT child2.id, parent.id AS id_1, parent.cls "
            "FROM secondary AS secondary_1, parent "
            "JOIN child2 ON parent.id = child2.id "
            "JOIN secondary AS secondary_2 ON parent.id = secondary_2.left_id "
            "JOIN (parent AS parent_1 JOIN child1 AS child1_1 "
            "ON parent_1.id = child1_1.id) ON parent_1.id = "
            "secondary_2.right_id "
            "WHERE parent_1.id = secondary_1.right_id "
            "AND :param_1 = secondary_1.left_id",
        )

    def test_query_crit_core_workaround(self):
        # do a test in the style of orm/test_core_compilation.py

        Child1, Child2 = self.classes.Child1, self.classes.Child2
        secondary = self.tables.secondary

        configure_mappers()

        from sqlalchemy.sql import join

        C1 = aliased(Child1, flat=True)

        # this was "figure out all the things we need to do in Core to make
        # the identical query that the ORM renders.", however as of
        # I765a0b912b3dcd0e995426427d8bb7997cbffd51 this is using the ORM
        # to create the query in any case

        salias = secondary.alias()
        stmt = (
            select(Child2)
            .select_from(
                join(
                    Child2,
                    salias,
                    Child2.id.expressions[1] == salias.c.left_id,
                ).join(C1, salias.c.right_id == C1.id.expressions[1])
            )
            .where(C1.left_child2 == Child2(id=1))
        )

        self.assert_compile(
            stmt.set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            "SELECT child2.id AS child2_id, parent.id AS parent_id, "
            "parent.cls AS parent_cls "
            "FROM secondary AS secondary_1, "
            "parent JOIN child2 ON parent.id = child2.id JOIN secondary AS "
            "secondary_2 ON parent.id = secondary_2.left_id JOIN "
            "(parent AS parent_1 JOIN child1 AS child1_1 "
            "ON parent_1.id = child1_1.id) "
            "ON parent_1.id = secondary_2.right_id WHERE "
            "parent_1.id = secondary_1.right_id AND :param_1 = "
            "secondary_1.left_id",
        )

    def test_eager_join(self):
        Child1, Child2 = self.classes.Child1, self.classes.Child2
        sess = fixture_session()
        c1 = Child1()
        c1.left_child2 = Child2()
        sess.add(c1)
        sess.flush()

        # test that the splicing of the join works here, doesn't break in
        # the middle of "parent join child1"
        q = sess.query(Child1).options(joinedload("left_child2"))
        self.assert_compile(
            q.limit(1).statement,
            "SELECT child1.id, parent.id AS id_1, parent.cls, "
            "child2_1.id AS id_2, parent_1.id AS id_3, parent_1.cls AS cls_1 "
            "FROM parent JOIN child1 ON parent.id = child1.id "
            "LEFT OUTER JOIN (secondary AS secondary_1 "
            "JOIN (parent AS parent_1 JOIN child2 AS child2_1 "
            "ON parent_1.id = child2_1.id) ON parent_1.id = "
            "secondary_1.left_id) ON parent.id = secondary_1.right_id "
            "LIMIT :param_1",
            checkparams={"param_1": 1},
        )

        # another way to check
        eq_(
            sess.scalar(
                select(func.count("*")).select_from(q.limit(1).subquery())
            ),
            1,
        )
        assert q.first() is c1

    def test_subquery_load(self):
        Child1, Child2 = self.classes.Child1, self.classes.Child2
        sess = fixture_session()
        c1 = Child1()
        c1.left_child2 = Child2()
        sess.add(c1)
        sess.flush()
        sess.expunge_all()

        query_ = sess.query(Child1).options(subqueryload("left_child2"))
        for row in query_.all():
            assert row.left_child2


class EagerToSubclassTest(fixtures.MappedTest):
    """Test eager loads to subclass mappers"""

    run_setup_classes = "once"
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(10)),
        )

        Table(
            "base",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("type", String(10)),
            Column("related_id", Integer, ForeignKey("related.id")),
        )

        Table(
            "sub",
            metadata,
            Column("id", Integer, ForeignKey("base.id"), primary_key=True),
            Column("data", String(10)),
            Column(
                "parent_id", Integer, ForeignKey("parent.id"), nullable=False
            ),
        )

        Table(
            "related",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(10)),
        )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Comparable):
            pass

        class Base(cls.Comparable):
            pass

        class Sub(Base):
            pass

        class Related(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        sub = cls.tables.sub
        Sub = cls.classes.Sub
        base = cls.tables.base
        Base = cls.classes.Base
        parent = cls.tables.parent
        Parent = cls.classes.Parent
        related = cls.tables.related
        Related = cls.classes.Related

        mapper(
            Parent,
            parent,
            properties={"children": relationship(Sub, order_by=sub.c.data)},
        )

        mapper(
            Base,
            base,
            polymorphic_on=base.c.type,
            polymorphic_identity="b",
            properties={"related": relationship(Related)},
        )

        mapper(Sub, sub, inherits=Base, polymorphic_identity="s")

        mapper(Related, related)

    @classmethod
    def insert_data(cls, connection):
        global p1, p2

        Parent = cls.classes.Parent
        Sub = cls.classes.Sub
        Related = cls.classes.Related
        sess = Session(connection)
        r1, r2 = Related(data="r1"), Related(data="r2")
        s1 = Sub(data="s1", related=r1)
        s2 = Sub(data="s2", related=r2)
        s3 = Sub(data="s3")
        s4 = Sub(data="s4", related=r2)
        s5 = Sub(data="s5")
        p1 = Parent(data="p1", children=[s1, s2, s3])
        p2 = Parent(data="p2", children=[s4, s5])
        sess.add(p1)
        sess.add(p2)
        sess.commit()

    def test_joinedload(self):
        Parent = self.classes.Parent
        sess = fixture_session()

        def go():
            eq_(
                sess.query(Parent).options(joinedload(Parent.children)).all(),
                [p1, p2],
            )

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager(self):
        Parent = self.classes.Parent
        Sub = self.classes.Sub
        sess = fixture_session()

        def go():
            eq_(
                sess.query(Parent)
                .join(Parent.children)
                .options(contains_eager(Parent.children))
                .order_by(Parent.data, Sub.data)
                .all(),
                [p1, p2],
            )

        self.assert_sql_count(testing.db, go, 1)

    def test_subq_through_related(self):
        Parent = self.classes.Parent
        Base = self.classes.Base
        sess = fixture_session()

        def go():
            eq_(
                sess.query(Parent)
                .options(
                    subqueryload(Parent.children).subqueryload(Base.related)
                )
                .order_by(Parent.data)
                .all(),
                [p1, p2],
            )

        self.assert_sql_count(testing.db, go, 3)

    def test_subq_through_related_aliased(self):
        Parent = self.classes.Parent
        Base = self.classes.Base
        pa = aliased(Parent)
        sess = fixture_session()

        def go():
            eq_(
                sess.query(pa)
                .options(subqueryload(pa.children).subqueryload(Base.related))
                .order_by(pa.data)
                .all(),
                [p1, p2],
            )

        self.assert_sql_count(testing.db, go, 3)


class SubClassEagerToSubClassTest(fixtures.MappedTest):
    """Test joinedloads from subclass to subclass mappers"""

    run_setup_classes = "once"
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("type", String(10)),
        )

        Table(
            "subparent",
            metadata,
            Column("id", Integer, ForeignKey("parent.id"), primary_key=True),
            Column("data", String(10)),
        )

        Table(
            "base",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("type", String(10)),
        )

        Table(
            "sub",
            metadata,
            Column("id", Integer, ForeignKey("base.id"), primary_key=True),
            Column("data", String(10)),
            Column(
                "subparent_id",
                Integer,
                ForeignKey("subparent.id"),
                nullable=False,
            ),
        )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Comparable):
            pass

        class Subparent(Parent):
            pass

        class Base(cls.Comparable):
            pass

        class Sub(Base):
            pass

    @classmethod
    def setup_mappers(cls):
        sub = cls.tables.sub
        Sub = cls.classes.Sub
        base = cls.tables.base
        Base = cls.classes.Base
        parent = cls.tables.parent
        Parent = cls.classes.Parent
        subparent = cls.tables.subparent
        Subparent = cls.classes.Subparent

        mapper(
            Parent,
            parent,
            polymorphic_on=parent.c.type,
            polymorphic_identity="b",
        )

        mapper(
            Subparent,
            subparent,
            inherits=Parent,
            polymorphic_identity="s",
            properties={"children": relationship(Sub, order_by=base.c.id)},
        )

        mapper(
            Base, base, polymorphic_on=base.c.type, polymorphic_identity="b"
        )

        mapper(Sub, sub, inherits=Base, polymorphic_identity="s")

    @classmethod
    def insert_data(cls, connection):
        global p1, p2

        Sub, Subparent = cls.classes.Sub, cls.classes.Subparent
        with sessionmaker(connection).begin() as sess:
            p1 = Subparent(
                data="p1",
                children=[Sub(data="s1"), Sub(data="s2"), Sub(data="s3")],
            )
            p2 = Subparent(
                data="p2", children=[Sub(data="s4"), Sub(data="s5")]
            )
            sess.add(p1)
            sess.add(p2)

    def test_joinedload(self):
        Subparent = self.classes.Subparent

        sess = fixture_session()

        def go():
            eq_(
                sess.query(Subparent)
                .options(joinedload(Subparent.children))
                .all(),
                [p1, p2],
            )

        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()

        def go():
            eq_(
                sess.query(Subparent).options(joinedload("children")).all(),
                [p1, p2],
            )

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager(self):
        Subparent = self.classes.Subparent

        sess = fixture_session()

        def go():
            eq_(
                sess.query(Subparent)
                .join(Subparent.children)
                .options(contains_eager(Subparent.children))
                .all(),
                [p1, p2],
            )

        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()

        def go():
            eq_(
                sess.query(Subparent)
                .join(Subparent.children)
                .options(contains_eager("children"))
                .all(),
                [p1, p2],
            )

        self.assert_sql_count(testing.db, go, 1)

    def test_subqueryload(self):
        Subparent = self.classes.Subparent

        sess = fixture_session()

        def go():
            eq_(
                sess.query(Subparent)
                .options(subqueryload(Subparent.children))
                .all(),
                [p1, p2],
            )

        self.assert_sql_count(testing.db, go, 2)

        sess.expunge_all()

        def go():
            eq_(
                sess.query(Subparent).options(subqueryload("children")).all(),
                [p1, p2],
            )

        self.assert_sql_count(testing.db, go, 2)


class SameNamedPropTwoPolymorphicSubClassesTest(fixtures.MappedTest):
    """test pathing when two subclasses contain a different property
    for the same name, and polymorphic loading is used.

    #2614

    """

    run_setup_classes = "once"
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
            Column("type", String(10)),
        )
        Table(
            "b",
            metadata,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
        )
        Table(
            "btod",
            metadata,
            Column("bid", Integer, ForeignKey("b.id"), nullable=False),
            Column("did", Integer, ForeignKey("d.id"), nullable=False),
        )
        Table(
            "c",
            metadata,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
        )
        Table(
            "ctod",
            metadata,
            Column("cid", Integer, ForeignKey("c.id"), nullable=False),
            Column("did", Integer, ForeignKey("d.id"), nullable=False),
        )
        Table(
            "d",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

        class B(A):
            pass

        class C(A):
            pass

        class D(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        A = cls.classes.A
        B = cls.classes.B
        C = cls.classes.C
        D = cls.classes.D

        mapper(A, cls.tables.a, polymorphic_on=cls.tables.a.c.type)
        mapper(
            B,
            cls.tables.b,
            inherits=A,
            polymorphic_identity="b",
            properties={"related": relationship(D, secondary=cls.tables.btod)},
        )
        mapper(
            C,
            cls.tables.c,
            inherits=A,
            polymorphic_identity="c",
            properties={"related": relationship(D, secondary=cls.tables.ctod)},
        )
        mapper(D, cls.tables.d)

    @classmethod
    def insert_data(cls, connection):
        B = cls.classes.B
        C = cls.classes.C
        D = cls.classes.D

        session = Session(connection)

        d = D()
        session.add_all([B(related=[d]), C(related=[d])])
        session.commit()

    def test_free_w_poly_subquery(self):
        A = self.classes.A
        B = self.classes.B
        C = self.classes.C
        D = self.classes.D

        session = fixture_session()
        d = session.query(D).one()
        a_poly = with_polymorphic(A, [B, C])

        def go():
            for a in session.query(a_poly).options(
                subqueryload(a_poly.B.related), subqueryload(a_poly.C.related)
            ):
                eq_(a.related, [d])

        self.assert_sql_count(testing.db, go, 3)

    def test_fixed_w_poly_subquery(self):
        A = self.classes.A
        B = self.classes.B
        C = self.classes.C
        D = self.classes.D

        session = fixture_session()
        d = session.query(D).one()

        def go():
            # NOTE: subqueryload is broken for this case, first found
            # when cartesian product detection was added.
            for a in (
                session.query(A)
                .with_polymorphic([B, C])
                .options(selectinload(B.related), selectinload(C.related))
            ):
                eq_(a.related, [d])

        self.assert_sql_count(testing.db, go, 3)

    def test_free_w_poly_joined(self):
        A = self.classes.A
        B = self.classes.B
        C = self.classes.C
        D = self.classes.D

        session = fixture_session()
        d = session.query(D).one()
        a_poly = with_polymorphic(A, [B, C])

        def go():
            for a in session.query(a_poly).options(
                joinedload(a_poly.B.related), joinedload(a_poly.C.related)
            ):
                eq_(a.related, [d])

        self.assert_sql_count(testing.db, go, 1)

    def test_fixed_w_poly_joined(self):
        A = self.classes.A
        B = self.classes.B
        C = self.classes.C
        D = self.classes.D

        session = fixture_session()
        d = session.query(D).one()

        def go():
            for a in (
                session.query(A)
                .with_polymorphic([B, C])
                .options(joinedload(B.related), joinedload(C.related))
            ):
                eq_(a.related, [d])

        self.assert_sql_count(testing.db, go, 1)


class SubClassToSubClassFromParentTest(fixtures.MappedTest):
    """test #2617"""

    run_setup_classes = "once"
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "z",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )
        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("type", String(10)),
            Column("z_id", Integer, ForeignKey("z.id")),
        )
        Table(
            "b",
            metadata,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
        )
        Table(
            "d",
            metadata,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
            Column("b_id", Integer, ForeignKey("b.id")),
        )

    @classmethod
    def setup_classes(cls):
        class Z(cls.Comparable):
            pass

        class A(cls.Comparable):
            pass

        class B(A):
            pass

        class D(A):
            pass

    @classmethod
    def setup_mappers(cls):
        Z = cls.classes.Z
        A = cls.classes.A
        B = cls.classes.B
        D = cls.classes.D

        mapper(Z, cls.tables.z)
        mapper(
            A,
            cls.tables.a,
            polymorphic_on=cls.tables.a.c.type,
            with_polymorphic="*",
            properties={"zs": relationship(Z, lazy="subquery")},
        )
        mapper(
            B,
            cls.tables.b,
            inherits=A,
            polymorphic_identity="b",
            properties={
                "related": relationship(
                    D,
                    lazy="subquery",
                    primaryjoin=cls.tables.d.c.b_id == cls.tables.b.c.id,
                )
            },
        )
        mapper(D, cls.tables.d, inherits=A, polymorphic_identity="d")

    @classmethod
    def insert_data(cls, connection):
        B = cls.classes.B

        session = Session(connection)
        session.add(B())
        session.commit()

    def test_2617(self):
        A = self.classes.A
        session = fixture_session()

        def go():
            a1 = session.query(A).first()
            eq_(a1.related, [])

        self.assert_sql_count(testing.db, go, 3)


class SubClassToSubClassMultiTest(AssertsCompiledSQL, fixtures.MappedTest):
    """
    Two different joined-inh subclasses, led by a
    parent, with two distinct endpoints:

    parent -> subcl1 -> subcl2 -> (ep1, ep2)

    the join to ep2 indicates we need to join
    from the middle of the joinpoint, skipping ep1

    """

    run_create_tables = None
    run_deletes = None
    __dialect__ = "default"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
        )
        Table(
            "base1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
        )
        Table(
            "sub1",
            metadata,
            Column("id", Integer, ForeignKey("base1.id"), primary_key=True),
            Column("parent_id", ForeignKey("parent.id")),
            Column("subdata", String(30)),
        )

        Table(
            "base2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("base1_id", ForeignKey("base1.id")),
            Column("data", String(30)),
        )
        Table(
            "sub2",
            metadata,
            Column("id", Integer, ForeignKey("base2.id"), primary_key=True),
            Column("subdata", String(30)),
        )
        Table(
            "ep1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("base2_id", Integer, ForeignKey("base2.id")),
            Column("data", String(30)),
        )
        Table(
            "ep2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("base2_id", Integer, ForeignKey("base2.id")),
            Column("data", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Comparable):
            pass

        class Base1(cls.Comparable):
            pass

        class Sub1(Base1):
            pass

        class Base2(cls.Comparable):
            pass

        class Sub2(Base2):
            pass

        class EP1(cls.Comparable):
            pass

        class EP2(cls.Comparable):
            pass

    @classmethod
    def _classes(cls):
        return (
            cls.classes.Parent,
            cls.classes.Base1,
            cls.classes.Base2,
            cls.classes.Sub1,
            cls.classes.Sub2,
            cls.classes.EP1,
            cls.classes.EP2,
        )

    @classmethod
    def setup_mappers(cls):
        Parent, Base1, Base2, Sub1, Sub2, EP1, EP2 = cls._classes()

        mapper(
            Parent, cls.tables.parent, properties={"sub1": relationship(Sub1)}
        )
        mapper(
            Base1, cls.tables.base1, properties={"sub2": relationship(Sub2)}
        )
        mapper(Sub1, cls.tables.sub1, inherits=Base1)
        mapper(
            Base2,
            cls.tables.base2,
            properties={"ep1": relationship(EP1), "ep2": relationship(EP2)},
        )
        mapper(Sub2, cls.tables.sub2, inherits=Base2)
        mapper(EP1, cls.tables.ep1)
        mapper(EP2, cls.tables.ep2)

    def test_one(self):
        Parent, Base1, Base2, Sub1, Sub2, EP1, EP2 = self._classes()

        s = fixture_session()
        self.assert_compile(
            s.query(Parent)
            .join(Parent.sub1, Sub1.sub2)
            .join(Sub2.ep1)
            .join(Sub2.ep2),
            "SELECT parent.id AS parent_id, parent.data AS parent_data "
            "FROM parent JOIN (base1 JOIN sub1 ON base1.id = sub1.id) "
            "ON parent.id = sub1.parent_id JOIN "
            "(base2 JOIN sub2 "
            "ON base2.id = sub2.id) "
            "ON base1.id = base2.base1_id "
            "JOIN ep1 ON base2.id = ep1.base2_id "
            "JOIN ep2 ON base2.id = ep2.base2_id",
        )

    def test_two(self):
        Parent, Base1, Base2, Sub1, Sub2, EP1, EP2 = self._classes()

        s2a = aliased(Sub2, flat=True)

        s = fixture_session()
        self.assert_compile(
            s.query(Parent).join(Parent.sub1).join(s2a, Sub1.sub2),
            "SELECT parent.id AS parent_id, parent.data AS parent_data "
            "FROM parent JOIN (base1 JOIN sub1 ON base1.id = sub1.id) "
            "ON parent.id = sub1.parent_id JOIN "
            "(base2 AS base2_1 JOIN sub2 AS sub2_1 "
            "ON base2_1.id = sub2_1.id) "
            "ON base1.id = base2_1.base1_id",
        )

    def test_three(self):
        Parent, Base1, Base2, Sub1, Sub2, EP1, EP2 = self._classes()

        s = fixture_session()
        self.assert_compile(
            s.query(Base1).join(Base1.sub2).join(Sub2.ep1).join(Sub2.ep2),
            "SELECT base1.id AS base1_id, base1.data AS base1_data "
            "FROM base1 JOIN (base2 JOIN sub2 "
            "ON base2.id = sub2.id) ON base1.id = "
            "base2.base1_id "
            "JOIN ep1 ON base2.id = ep1.base2_id "
            "JOIN ep2 ON base2.id = ep2.base2_id",
        )

    def test_four(self):
        Parent, Base1, Base2, Sub1, Sub2, EP1, EP2 = self._classes()

        s = fixture_session()
        self.assert_compile(
            s.query(Sub2)
            .join(Base1, Base1.id == Sub2.base1_id)
            .join(Sub2.ep1)
            .join(Sub2.ep2),
            "SELECT sub2.id AS sub2_id, base2.id AS base2_id, "
            "base2.base1_id AS base2_base1_id, base2.data AS base2_data, "
            "sub2.subdata AS sub2_subdata "
            "FROM base2 JOIN sub2 ON base2.id = sub2.id "
            "JOIN base1 ON base1.id = base2.base1_id "
            "JOIN ep1 ON base2.id = ep1.base2_id "
            "JOIN ep2 ON base2.id = ep2.base2_id",
        )

    def test_five(self):
        Parent, Base1, Base2, Sub1, Sub2, EP1, EP2 = self._classes()

        s = fixture_session()
        self.assert_compile(
            s.query(Sub2)
            .join(Sub1, Sub1.id == Sub2.base1_id)
            .join(Sub2.ep1)
            .join(Sub2.ep2),
            "SELECT sub2.id AS sub2_id, base2.id AS base2_id, "
            "base2.base1_id AS base2_base1_id, base2.data AS base2_data, "
            "sub2.subdata AS sub2_subdata "
            "FROM base2 JOIN sub2 ON base2.id = sub2.id "
            "JOIN "
            "(base1 JOIN sub1 ON base1.id = sub1.id) "
            "ON sub1.id = base2.base1_id "
            "JOIN ep1 ON base2.id = ep1.base2_id "
            "JOIN ep2 ON base2.id = ep2.base2_id",
        )

    def test_six_legacy(self):
        Parent, Base1, Base2, Sub1, Sub2, EP1, EP2 = self._classes()

        s = fixture_session()

        # as of from_self() changing in
        # I3abfb45dd6e50f84f29d39434caa0b550ce27864,
        # this query is coming out instead which is equivalent, but not
        # totally sure where this happens

        with testing.expect_deprecated(r"The Query.from_self\(\) method"):
            self.assert_compile(
                s.query(Sub2).from_self().join(Sub2.ep1).join(Sub2.ep2),
                "SELECT anon_1.sub2_id AS anon_1_sub2_id, "
                "anon_1.base2_base1_id AS anon_1_base2_base1_id, "
                "anon_1.base2_data AS anon_1_base2_data, "
                "anon_1.sub2_subdata AS anon_1_sub2_subdata "
                "FROM (SELECT sub2.id AS sub2_id, base2.id AS base2_id, "
                "base2.base1_id AS base2_base1_id, base2.data AS base2_data, "
                "sub2.subdata AS sub2_subdata "
                "FROM base2 JOIN sub2 ON base2.id = sub2.id) AS anon_1 "
                "JOIN ep1 ON anon_1.sub2_id = ep1.base2_id "
                "JOIN ep2 ON anon_1.sub2_id = ep2.base2_id",
            )

    def test_six(self):
        Parent, Base1, Base2, Sub1, Sub2, EP1, EP2 = self._classes()

        # as of from_self() changing in
        # I3abfb45dd6e50f84f29d39434caa0b550ce27864,
        # this query is coming out instead which is equivalent, but not
        # totally sure where this happens

        stmt = select(Sub2)

        subq = aliased(
            Sub2,
            stmt.set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL).subquery(),
        )

        stmt = (
            select(subq)
            .join(subq.ep1)
            .join(Sub2.ep2)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        )
        self.assert_compile(
            stmt,
            "SELECT anon_1.sub2_id AS anon_1_sub2_id, "
            "anon_1.base2_base1_id AS anon_1_base2_base1_id, "
            "anon_1.base2_data AS anon_1_base2_data, "
            "anon_1.sub2_subdata AS anon_1_sub2_subdata "
            "FROM (SELECT sub2.id AS sub2_id, base2.id AS base2_id, "
            "base2.base1_id AS base2_base1_id, base2.data AS base2_data, "
            "sub2.subdata AS sub2_subdata "
            "FROM base2 JOIN sub2 ON base2.id = sub2.id) AS anon_1 "
            "JOIN ep1 ON anon_1.sub2_id = ep1.base2_id "
            "JOIN ep2 ON anon_1.sub2_id = ep2.base2_id",
        )

    def test_seven_legacy(self):
        Parent, Base1, Base2, Sub1, Sub2, EP1, EP2 = self._classes()

        s = fixture_session()

        # as of from_self() changing in
        # I3abfb45dd6e50f84f29d39434caa0b550ce27864,
        # this query is coming out instead which is equivalent, but not
        # totally sure where this happens
        with testing.expect_deprecated(r"The Query.from_self\(\) method"):

            self.assert_compile(
                # adding Sub2 to the entities list helps it,
                # otherwise the joins for Sub2.ep1/ep2 don't have columns
                # to latch onto.   Can't really make it better than this
                s.query(Parent, Sub2)
                .join(Parent.sub1)
                .join(Sub1.sub2)
                .from_self()
                .join(Sub2.ep1)
                .join(Sub2.ep2),
                "SELECT anon_1.parent_id AS anon_1_parent_id, "
                "anon_1.parent_data AS anon_1_parent_data, "
                "anon_1.sub2_id AS anon_1_sub2_id, "
                "anon_1.base2_base1_id AS anon_1_base2_base1_id, "
                "anon_1.base2_data AS anon_1_base2_data, "
                "anon_1.sub2_subdata AS anon_1_sub2_subdata "
                "FROM (SELECT parent.id AS parent_id, "
                "parent.data AS parent_data, "
                "sub2.id AS sub2_id, "
                "base2.id AS base2_id, "
                "base2.base1_id AS base2_base1_id, "
                "base2.data AS base2_data, "
                "sub2.subdata AS sub2_subdata "
                "FROM parent JOIN (base1 JOIN sub1 ON base1.id = sub1.id) "
                "ON parent.id = sub1.parent_id JOIN "
                "(base2 JOIN sub2 ON base2.id = sub2.id) "
                "ON base1.id = base2.base1_id) AS anon_1 "
                "JOIN ep1 ON anon_1.sub2_id = ep1.base2_id "
                "JOIN ep2 ON anon_1.sub2_id = ep2.base2_id",
            )

    def test_seven(self):
        Parent, Base1, Base2, Sub1, Sub2, EP1, EP2 = self._classes()

        # as of from_self() changing in
        # I3abfb45dd6e50f84f29d39434caa0b550ce27864,
        # this query is coming out instead which is equivalent, but not
        # totally sure where this happens

        subq = (
            select(Parent, Sub2)
            .join(Parent.sub1)
            .join(Sub1.sub2)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )

        # another 1.4 supercharged select() statement ;)

        palias = aliased(Parent, subq)
        sub2alias = aliased(Sub2, subq)

        stmt = (
            select(palias, sub2alias)
            .join(sub2alias.ep1)
            .join(sub2alias.ep2)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        )

        self.assert_compile(
            # adding Sub2 to the entities list helps it,
            # otherwise the joins for Sub2.ep1/ep2 don't have columns
            # to latch onto.   Can't really make it better than this
            stmt,
            "SELECT anon_1.parent_id AS anon_1_parent_id, "
            "anon_1.parent_data AS anon_1_parent_data, "
            "anon_1.sub2_id AS anon_1_sub2_id, "
            "anon_1.base2_base1_id AS anon_1_base2_base1_id, "
            "anon_1.base2_data AS anon_1_base2_data, "
            "anon_1.sub2_subdata AS anon_1_sub2_subdata "
            "FROM (SELECT parent.id AS parent_id, parent.data AS parent_data, "
            "sub2.id AS sub2_id, "
            "base2.id AS base2_id, "
            "base2.base1_id AS base2_base1_id, "
            "base2.data AS base2_data, "
            "sub2.subdata AS sub2_subdata "
            "FROM parent JOIN (base1 JOIN sub1 ON base1.id = sub1.id) "
            "ON parent.id = sub1.parent_id JOIN "
            "(base2 JOIN sub2 ON base2.id = sub2.id) "
            "ON base1.id = base2.base1_id) AS anon_1 "
            "JOIN ep1 ON anon_1.sub2_id = ep1.base2_id "
            "JOIN ep2 ON anon_1.sub2_id = ep2.base2_id",
        )


class JoinedloadWPolyOfTypeContinued(
    fixtures.DeclarativeMappedTest, testing.AssertsCompiledSQL
):
    """test for #5082 """

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class User(Base):
            __tablename__ = "users"

            id = Column(Integer, primary_key=True)
            foos = relationship("Foo", back_populates="owner")

        class Foo(Base):
            __tablename__ = "foos"
            __mapper_args__ = {"polymorphic_on": "type"}

            id = Column(Integer, primary_key=True)
            type = Column(String(10), nullable=False)
            owner_id = Column(Integer, ForeignKey("users.id"))
            owner = relationship("User", back_populates="foos")
            bar_id = Column(ForeignKey("bars.id"))
            bar = relationship("Bar")

        class SubFoo(Foo):
            __tablename__ = "foos_sub"
            __mapper_args__ = {"polymorphic_identity": "SUB"}

            id = Column(Integer, ForeignKey("foos.id"), primary_key=True)
            baz = Column(Integer)
            sub_bar_id = Column(Integer, ForeignKey("sub_bars.id"))
            sub_bar = relationship("SubBar")

        class Bar(Base):
            __tablename__ = "bars"

            id = Column(Integer, primary_key=True)
            fred_id = Column(Integer, ForeignKey("freds.id"), nullable=False)
            fred = relationship("Fred")

        class SubBar(Base):
            __tablename__ = "sub_bars"

            id = Column(Integer, primary_key=True)
            fred_id = Column(Integer, ForeignKey("freds.id"), nullable=False)
            fred = relationship("Fred")

        class Fred(Base):
            __tablename__ = "freds"

            id = Column(Integer, primary_key=True)

    @classmethod
    def insert_data(cls, connection):
        User, Fred, SubBar, Bar, SubFoo = cls.classes(
            "User", "Fred", "SubBar", "Bar", "SubFoo"
        )
        user = User(id=1)
        fred = Fred(id=1)
        bar = Bar(fred=fred)
        sub_bar = SubBar(fred=fred)
        rectangle = SubFoo(owner=user, baz=10, bar=bar, sub_bar=sub_bar)

        s = Session(connection)
        s.add_all([user, fred, bar, sub_bar, rectangle])
        s.commit()

    def test_joined_load_lastlink_subclass(self):
        Foo, User, SubBar = self.classes("Foo", "User", "SubBar")

        s = fixture_session()

        foo_polymorphic = with_polymorphic(Foo, "*", aliased=True)

        foo_load = joinedload(User.foos.of_type(foo_polymorphic))
        query = s.query(User).options(
            foo_load.joinedload(foo_polymorphic.SubFoo.sub_bar).joinedload(
                SubBar.fred
            )
        )

        self.assert_compile(
            query,
            "SELECT users.id AS users_id, anon_1.foos_id AS anon_1_foos_id, "
            "anon_1.foos_type AS anon_1_foos_type, anon_1.foos_owner_id "
            "AS anon_1_foos_owner_id, "
            "anon_1.foos_bar_id AS anon_1_foos_bar_id, "
            "freds_1.id AS freds_1_id, sub_bars_1.id "
            "AS sub_bars_1_id, sub_bars_1.fred_id AS sub_bars_1_fred_id, "
            "anon_1.foos_sub_id AS anon_1_foos_sub_id, "
            "anon_1.foos_sub_baz AS anon_1_foos_sub_baz, "
            "anon_1.foos_sub_sub_bar_id AS anon_1_foos_sub_sub_bar_id "
            "FROM users LEFT OUTER JOIN "
            "(SELECT foos.id AS foos_id, foos.type AS foos_type, "
            "foos.owner_id AS foos_owner_id, foos.bar_id AS foos_bar_id, "
            "foos_sub.id AS foos_sub_id, "
            "foos_sub.baz AS foos_sub_baz, "
            "foos_sub.sub_bar_id AS foos_sub_sub_bar_id "
            "FROM foos LEFT OUTER JOIN foos_sub ON foos.id = foos_sub.id) "
            "AS anon_1 ON users.id = anon_1.foos_owner_id "
            "LEFT OUTER JOIN sub_bars AS sub_bars_1 "
            "ON sub_bars_1.id = anon_1.foos_sub_sub_bar_id "
            "LEFT OUTER JOIN freds AS freds_1 "
            "ON freds_1.id = sub_bars_1.fred_id",
        )

        def go():
            user = query.one()
            user.foos[0].sub_bar
            user.foos[0].sub_bar.fred

        self.assert_sql_count(testing.db, go, 1)

    def test_joined_load_lastlink_baseclass(self):
        Foo, User, Bar = self.classes("Foo", "User", "Bar")

        s = fixture_session()

        foo_polymorphic = with_polymorphic(Foo, "*", aliased=True)

        foo_load = joinedload(User.foos.of_type(foo_polymorphic))
        query = s.query(User).options(
            foo_load.joinedload(foo_polymorphic.bar).joinedload(Bar.fred)
        )

        self.assert_compile(
            query,
            "SELECT users.id AS users_id, freds_1.id AS freds_1_id, "
            "bars_1.id AS bars_1_id, "
            "bars_1.fred_id AS bars_1_fred_id, "
            "anon_1.foos_id AS anon_1_foos_id, "
            "anon_1.foos_type AS anon_1_foos_type, anon_1.foos_owner_id AS "
            "anon_1_foos_owner_id, anon_1.foos_bar_id AS anon_1_foos_bar_id, "
            "anon_1.foos_sub_id AS anon_1_foos_sub_id, anon_1.foos_sub_baz AS "
            "anon_1_foos_sub_baz, "
            "anon_1.foos_sub_sub_bar_id AS anon_1_foos_sub_sub_bar_id "
            "FROM users LEFT OUTER JOIN (SELECT foos.id AS foos_id, "
            "foos.type AS foos_type, "
            "foos.owner_id AS foos_owner_id, foos.bar_id AS foos_bar_id, "
            "foos_sub.id AS "
            "foos_sub_id, foos_sub.baz AS foos_sub_baz, "
            "foos_sub.sub_bar_id AS "
            "foos_sub_sub_bar_id FROM foos "
            "LEFT OUTER JOIN foos_sub ON foos.id = "
            "foos_sub.id) AS anon_1 ON users.id = anon_1.foos_owner_id "
            "LEFT OUTER JOIN bars "
            "AS bars_1 ON bars_1.id = anon_1.foos_bar_id "
            "LEFT OUTER JOIN freds AS freds_1 ON freds_1.id = bars_1.fred_id",
        )

        def go():
            user = query.one()
            user.foos[0].bar
            user.foos[0].bar.fred

        self.assert_sql_count(testing.db, go, 1)


class ContainsEagerMultipleOfType(
    fixtures.DeclarativeMappedTest, testing.AssertsCompiledSQL
):
    """test for #5107 """

    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class X(Base):
            __tablename__ = "x"
            id = Column(Integer, primary_key=True)
            a_id = Column(Integer, ForeignKey("a.id"))
            a = relationship("A", back_populates="x")

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            b = relationship("B", back_populates="a")
            kind = Column(String(30))
            x = relationship("X", back_populates="a")
            __mapper_args__ = {
                "polymorphic_identity": "a",
                "polymorphic_on": kind,
                "with_polymorphic": "*",
            }

        class B(A):
            a_id = Column(Integer, ForeignKey("a.id"))
            a = relationship(
                "A", back_populates="b", uselist=False, remote_side=A.id
            )
            __mapper_args__ = {"polymorphic_identity": "b"}

    def test_contains_eager_multi_alias(self):
        X, B, A = self.classes("X", "B", "A")
        s = fixture_session()

        a_b_alias = aliased(B, name="a_b")
        b_x_alias = aliased(X, name="b_x")

        q = (
            s.query(A)
            .outerjoin(A.b.of_type(a_b_alias))
            .outerjoin(a_b_alias.x.of_type(b_x_alias))
            .options(
                contains_eager(A.b.of_type(a_b_alias)).contains_eager(
                    a_b_alias.x.of_type(b_x_alias)
                )
            )
        )
        self.assert_compile(
            q,
            "SELECT b_x.id AS b_x_id, b_x.a_id AS b_x_a_id, a_b.id AS a_b_id, "
            "a_b.kind AS a_b_kind, a_b.a_id AS a_b_a_id, a.id AS a_id_1, "
            "a.kind AS a_kind, a.a_id AS a_a_id FROM a "
            "LEFT OUTER JOIN a AS a_b ON a.id = a_b.a_id AND a_b.kind IN "
            "([POSTCOMPILE_kind_1]) LEFT OUTER JOIN x AS b_x "
            "ON a_b.id = b_x.a_id",
        )


class JoinedloadSinglePolysubSingle(
    fixtures.DeclarativeMappedTest, testing.AssertsCompiledSQL
):
    """exercise issue #3611, using the test from dupe issue 3614"""

    run_define_tables = None
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)

        class UserRole(Base):
            __tablename__ = "user_roles"

            id = Column(Integer, primary_key=True)

            row_type = Column(String(50), nullable=False)
            __mapper_args__ = {"polymorphic_on": row_type}

            user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
            user = relationship("User", lazy=False)

        class Admin(UserRole):
            __tablename__ = "admins"
            __mapper_args__ = {"polymorphic_identity": "admin"}

            id = Column(Integer, ForeignKey("user_roles.id"), primary_key=True)

        class Thing(Base):
            __tablename__ = "things"

            id = Column(Integer, primary_key=True)

            admin_id = Column(Integer, ForeignKey("admins.id"))
            admin = relationship("Admin", lazy=False)

    def test_query(self):
        Thing = self.classes.Thing
        sess = fixture_session()
        self.assert_compile(
            sess.query(Thing),
            "SELECT things.id AS things_id, "
            "things.admin_id AS things_admin_id, "
            "users_1.id AS users_1_id, admins_1.id AS admins_1_id, "
            "user_roles_1.id AS user_roles_1_id, "
            "user_roles_1.row_type AS user_roles_1_row_type, "
            "user_roles_1.user_id AS user_roles_1_user_id FROM things "
            "LEFT OUTER JOIN (user_roles AS user_roles_1 JOIN admins "
            "AS admins_1 ON user_roles_1.id = admins_1.id) ON "
            "admins_1.id = things.admin_id "
            "LEFT OUTER JOIN users AS "
            "users_1 ON users_1.id = user_roles_1.user_id",
        )


class JoinedloadOverWPolyAliased(
    fixtures.DeclarativeMappedTest, testing.AssertsCompiledSQL
):
    """exercise issues in #3593 and #3611"""

    run_setup_mappers = "each"
    run_setup_classes = "each"
    run_define_tables = "each"
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Owner(Base):
            __tablename__ = "owner"

            id = Column(Integer, primary_key=True)
            type = Column(String(20))

            __mapper_args__ = {
                "polymorphic_on": type,
                "with_polymorphic": ("*", None),
            }

        class SubOwner(Owner):
            __mapper_args__ = {"polymorphic_identity": "so"}

        class Parent(Base):
            __tablename__ = "parent"

            id = Column(Integer, primary_key=True)
            type = Column(String(20))

            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "parent",
                "with_polymorphic": ("*", None),
            }

        class Sub1(Parent):
            __mapper_args__ = {"polymorphic_identity": "s1"}

        class Link(Base):
            __tablename__ = "link"

            parent_id = Column(
                Integer, ForeignKey("parent.id"), primary_key=True
            )
            child_id = Column(
                Integer, ForeignKey("parent.id"), primary_key=True
            )

    def _fixture_from_base(self):
        Parent = self.classes.Parent
        Link = self.classes.Link
        Link.child = relationship(
            Parent, primaryjoin=Link.child_id == Parent.id
        )

        Parent.links = relationship(
            Link, primaryjoin=Parent.id == Link.parent_id
        )
        return Parent

    def _fixture_from_subclass(self):
        Sub1 = self.classes.Sub1
        Link = self.classes.Link
        Parent = self.classes.Parent
        Link.child = relationship(
            Parent, primaryjoin=Link.child_id == Parent.id
        )

        Sub1.links = relationship(Link, primaryjoin=Sub1.id == Link.parent_id)
        return Sub1

    def _fixture_to_subclass_to_base(self):
        Owner = self.classes.Owner
        Parent = self.classes.Parent
        Sub1 = self.classes.Sub1
        Link = self.classes.Link

        # Link -> Sub1 -> Owner

        Link.child = relationship(Sub1, primaryjoin=Link.child_id == Sub1.id)

        Parent.owner_id = Column(ForeignKey("owner.id"))

        Parent.owner = relationship(Owner)
        return Parent

    def _fixture_to_base_to_base(self):
        Owner = self.classes.Owner
        Parent = self.classes.Parent
        Link = self.classes.Link

        # Link -> Parent -> Owner

        Link.child = relationship(
            Parent, primaryjoin=Link.child_id == Parent.id
        )

        Parent.owner_id = Column(ForeignKey("owner.id"))

        Parent.owner = relationship(Owner)
        return Parent

    def test_from_base(self):
        self._test_poly_single_poly(self._fixture_from_base)

    def test_from_sub(self):
        self._test_poly_single_poly(self._fixture_from_subclass)

    def test_to_sub_to_base(self):
        self._test_single_poly_poly(self._fixture_to_subclass_to_base)

    def test_to_base_to_base(self):
        self._test_single_poly_poly(self._fixture_to_base_to_base)

    def _test_poly_single_poly(self, fn):
        cls = fn()
        Link = self.classes.Link

        session = fixture_session()
        q = session.query(cls).options(
            joinedload(cls.links).joinedload(Link.child).joinedload(cls.links)
        )
        if cls is self.classes.Sub1:
            extra = " WHERE parent.type IN ([POSTCOMPILE_type_1])"
        else:
            extra = ""

        self.assert_compile(
            q,
            "SELECT parent.id AS parent_id, parent.type AS parent_type, "
            "link_1.parent_id AS link_1_parent_id, "
            "link_1.child_id AS link_1_child_id, "
            "parent_1.id AS parent_1_id, parent_1.type AS parent_1_type, "
            "link_2.parent_id AS link_2_parent_id, "
            "link_2.child_id AS link_2_child_id "
            "FROM parent "
            "LEFT OUTER JOIN link AS link_1 ON parent.id = link_1.parent_id "
            "LEFT OUTER JOIN parent "
            "AS parent_1 ON link_1.child_id = parent_1.id "
            "LEFT OUTER JOIN link AS link_2 "
            "ON parent_1.id = link_2.parent_id" + extra,
        )

    def _test_single_poly_poly(self, fn):
        parent_cls = fn()
        Link = self.classes.Link

        session = fixture_session()
        q = session.query(Link).options(
            joinedload(Link.child).joinedload(parent_cls.owner)
        )

        if Link.child.property.mapper.class_ is self.classes.Sub1:
            extra = "AND parent_1.type IN ([POSTCOMPILE_type_1]) "
        else:
            extra = ""

        self.assert_compile(
            q,
            "SELECT link.parent_id AS link_parent_id, "
            "link.child_id AS link_child_id, parent_1.id AS parent_1_id, "
            "parent_1.type AS parent_1_type, "
            "parent_1.owner_id AS parent_1_owner_id, "
            "owner_1.id AS owner_1_id, owner_1.type AS owner_1_type "
            "FROM link LEFT OUTER JOIN parent AS parent_1 "
            "ON link.child_id = parent_1.id "
            + extra
            + "LEFT OUTER JOIN owner AS owner_1 "
            "ON owner_1.id = parent_1.owner_id",
        )

    def test_local_wpoly(self):
        Sub1 = self._fixture_from_subclass()
        Parent = self.classes.Parent
        Link = self.classes.Link

        poly = with_polymorphic(Parent, [Sub1])

        session = fixture_session()
        q = session.query(poly).options(
            joinedload(poly.Sub1.links)
            .joinedload(Link.child.of_type(Sub1))
            .joinedload(Sub1.links)
        )
        self.assert_compile(
            q,
            "SELECT parent.id AS parent_id, parent.type AS parent_type, "
            "link_1.parent_id AS link_1_parent_id, "
            "link_1.child_id AS link_1_child_id, "
            "parent_1.id AS parent_1_id, parent_1.type AS parent_1_type, "
            "link_2.parent_id AS link_2_parent_id, "
            "link_2.child_id AS link_2_child_id FROM parent "
            "LEFT OUTER JOIN link AS link_1 ON parent.id = link_1.parent_id "
            "LEFT OUTER JOIN parent AS parent_1 "
            "ON link_1.child_id = parent_1.id "
            "LEFT OUTER JOIN link AS link_2 ON parent_1.id = link_2.parent_id",
        )

    def test_local_wpoly_innerjoins(self):
        # test for issue #3988
        Sub1 = self._fixture_from_subclass()
        Parent = self.classes.Parent
        Link = self.classes.Link

        poly = with_polymorphic(Parent, [Sub1])

        session = fixture_session()
        q = session.query(poly).options(
            joinedload(poly.Sub1.links, innerjoin=True)
            .joinedload(Link.child.of_type(Sub1), innerjoin=True)
            .joinedload(Sub1.links, innerjoin=True)
        )
        self.assert_compile(
            q,
            "SELECT parent.id AS parent_id, parent.type AS parent_type, "
            "link_1.parent_id AS link_1_parent_id, "
            "link_1.child_id AS link_1_child_id, "
            "parent_1.id AS parent_1_id, parent_1.type AS parent_1_type, "
            "link_2.parent_id AS link_2_parent_id, "
            "link_2.child_id AS link_2_child_id FROM parent "
            "LEFT OUTER JOIN link AS link_1 ON parent.id = link_1.parent_id "
            "LEFT OUTER JOIN parent AS parent_1 "
            "ON link_1.child_id = parent_1.id "
            "LEFT OUTER JOIN link AS link_2 ON parent_1.id = link_2.parent_id",
        )

    def test_local_wpoly_innerjoins_roundtrip(self):
        # test for issue #3988
        Sub1 = self._fixture_from_subclass()
        Parent = self.classes.Parent
        Link = self.classes.Link

        session = fixture_session()
        session.add_all([Parent(), Parent()])

        # represents "Parent" and "Sub1" rows
        poly = with_polymorphic(Parent, [Sub1])

        # innerjoin for Sub1 only, but this needs
        # to be cancelled because the Parent rows
        # would be omitted
        q = session.query(poly).options(
            joinedload(poly.Sub1.links, innerjoin=True).joinedload(
                Link.child.of_type(Sub1), innerjoin=True
            )
        )
        eq_(len(q.all()), 2)


class JoinAcrossJoinedInhMultiPath(
    fixtures.DeclarativeMappedTest, testing.AssertsCompiledSQL
):
    """test long join paths with a joined-inh in the middle, where we go multiple
    times across the same joined-inh to the same target but with other classes
    in the middle.    E.g. test [ticket:2908]
    """

    run_setup_mappers = "once"
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Root(Base):
            __tablename__ = "root"

            id = Column(Integer, primary_key=True)
            sub1_id = Column(Integer, ForeignKey("sub1.id"))

            intermediate = relationship("Intermediate")
            sub1 = relationship("Sub1")

        class Intermediate(Base):
            __tablename__ = "intermediate"

            id = Column(Integer, primary_key=True)
            sub1_id = Column(Integer, ForeignKey("sub1.id"))
            root_id = Column(Integer, ForeignKey("root.id"))
            sub1 = relationship("Sub1")

        class Parent(Base):
            __tablename__ = "parent"

            id = Column(Integer, primary_key=True)

        class Sub1(Parent):
            __tablename__ = "sub1"
            id = Column(Integer, ForeignKey("parent.id"), primary_key=True)

            target = relationship("Target")

        class Target(Base):
            __tablename__ = "target"
            id = Column(Integer, primary_key=True)
            sub1_id = Column(Integer, ForeignKey("sub1.id"))

    def test_join(self):
        Root, Intermediate, Sub1, Target = (
            self.classes.Root,
            self.classes.Intermediate,
            self.classes.Sub1,
            self.classes.Target,
        )
        s1_alias = aliased(Sub1)
        s2_alias = aliased(Sub1)
        t1_alias = aliased(Target)
        t2_alias = aliased(Target)

        sess = fixture_session()
        q = (
            sess.query(Root)
            .join(s1_alias, Root.sub1)
            .join(t1_alias, s1_alias.target)
            .join(Root.intermediate)
            .join(s2_alias, Intermediate.sub1)
            .join(t2_alias, s2_alias.target)
        )
        self.assert_compile(
            q,
            "SELECT root.id AS root_id, root.sub1_id AS root_sub1_id "
            "FROM root "
            "JOIN (SELECT parent.id AS parent_id, sub1.id AS sub1_id "
            "FROM parent JOIN sub1 ON parent.id = sub1.id) AS anon_1 "
            "ON anon_1.sub1_id = root.sub1_id "
            "JOIN target AS target_1 ON anon_1.sub1_id = target_1.sub1_id "
            "JOIN intermediate ON root.id = intermediate.root_id "
            "JOIN (SELECT parent.id AS parent_id, sub1.id AS sub1_id "
            "FROM parent JOIN sub1 ON parent.id = sub1.id) AS anon_2 "
            "ON anon_2.sub1_id = intermediate.sub1_id "
            "JOIN target AS target_2 ON anon_2.sub1_id = target_2.sub1_id",
        )

    def test_join_flat(self):
        Root, Intermediate, Sub1, Target = (
            self.classes.Root,
            self.classes.Intermediate,
            self.classes.Sub1,
            self.classes.Target,
        )
        s1_alias = aliased(Sub1, flat=True)
        s2_alias = aliased(Sub1, flat=True)
        t1_alias = aliased(Target)
        t2_alias = aliased(Target)

        sess = fixture_session()
        q = (
            sess.query(Root)
            .join(s1_alias, Root.sub1)
            .join(t1_alias, s1_alias.target)
            .join(Root.intermediate)
            .join(s2_alias, Intermediate.sub1)
            .join(t2_alias, s2_alias.target)
        )
        self.assert_compile(
            q,
            "SELECT root.id AS root_id, root.sub1_id AS root_sub1_id "
            "FROM root "
            "JOIN (parent AS parent_1 JOIN sub1 AS sub1_1 "
            "ON parent_1.id = sub1_1.id) "
            "ON sub1_1.id = root.sub1_id "
            "JOIN target AS target_1 ON sub1_1.id = target_1.sub1_id "
            "JOIN intermediate ON root.id = intermediate.root_id "
            "JOIN (parent AS parent_2 JOIN sub1 AS sub1_2 "
            "ON parent_2.id = sub1_2.id) "
            "ON sub1_2.id = intermediate.sub1_id "
            "JOIN target AS target_2 ON sub1_2.id = target_2.sub1_id",
        )

    def test_joinedload(self):
        Root, Intermediate, Sub1 = (
            self.classes.Root,
            self.classes.Intermediate,
            self.classes.Sub1,
        )

        sess = fixture_session()
        q = sess.query(Root).options(
            joinedload(Root.sub1).joinedload(Sub1.target),
            joinedload(Root.intermediate)
            .joinedload(Intermediate.sub1)
            .joinedload(Sub1.target),
        )
        self.assert_compile(
            q,
            "SELECT root.id AS root_id, root.sub1_id AS root_sub1_id, "
            "target_1.id AS target_1_id, "
            "target_1.sub1_id AS target_1_sub1_id, "
            "sub1_1.id AS sub1_1_id, parent_1.id AS parent_1_id, "
            "intermediate_1.id AS intermediate_1_id, "
            "intermediate_1.sub1_id AS intermediate_1_sub1_id, "
            "intermediate_1.root_id AS intermediate_1_root_id, "
            "target_2.id AS target_2_id, "
            "target_2.sub1_id AS target_2_sub1_id, "
            "sub1_2.id AS sub1_2_id, parent_2.id AS parent_2_id "
            "FROM root "
            "LEFT OUTER JOIN intermediate AS intermediate_1 "
            "ON root.id = intermediate_1.root_id "
            "LEFT OUTER JOIN (parent AS parent_1 JOIN sub1 AS sub1_1 "
            "ON parent_1.id = sub1_1.id) "
            "ON sub1_1.id = intermediate_1.sub1_id "
            "LEFT OUTER JOIN target AS target_1 "
            "ON sub1_1.id = target_1.sub1_id "
            "LEFT OUTER JOIN (parent AS parent_2 JOIN sub1 AS sub1_2 "
            "ON parent_2.id = sub1_2.id) ON sub1_2.id = root.sub1_id "
            "LEFT OUTER JOIN target AS target_2 "
            "ON sub1_2.id = target_2.sub1_id",
        )


class MultipleAdaptUsesEntityOverTableTest(
    AssertsCompiledSQL, fixtures.MappedTest
):
    __dialect__ = "default"
    run_create_tables = None
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
        )
        Table(
            "b",
            metadata,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
        )
        Table(
            "c",
            metadata,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
            Column("bid", Integer, ForeignKey("b.id")),
        )
        Table(
            "d",
            metadata,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
            Column("cid", Integer, ForeignKey("c.id")),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

        class B(A):
            pass

        class C(A):
            pass

        class D(A):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B, C, D = cls.classes.A, cls.classes.B, cls.classes.C, cls.classes.D
        a, b, c, d = cls.tables.a, cls.tables.b, cls.tables.c, cls.tables.d
        mapper(A, a)
        mapper(B, b, inherits=A)
        mapper(C, c, inherits=A)
        mapper(D, d, inherits=A)

    def _two_join_fixture(self):
        B, C, D = (self.classes.B, self.classes.C, self.classes.D)
        s = fixture_session()
        return (
            s.query(B.name, C.name, D.name)
            .select_from(B)
            .join(C, C.bid == B.id)
            .join(D, D.cid == C.id)
        )

    def test_two_joins_adaption(self):
        a, c, d = self.tables.a, self.tables.c, self.tables.d
        q = self._two_join_fixture()._compile_state()

        btoc = q.from_clauses[0].left

        ac_adapted = btoc.right.element.left
        c_adapted = btoc.right.element.right

        is_(ac_adapted.element, a)
        is_(c_adapted.element, c)

        ctod = q.from_clauses[0].right
        ad_adapted = ctod.element.left
        d_adapted = ctod.element.right
        is_(ad_adapted.element, a)
        is_(d_adapted.element, d)

        bname, cname, dname = q._entities

        adapter = q._get_current_adapter()
        b_name_adapted = adapter(bname.column, False)
        c_name_adapted = adapter(cname.column, False)
        d_name_adapted = adapter(dname.column, False)

        assert bool(b_name_adapted == a.c.name)
        assert bool(c_name_adapted == ac_adapted.c.name)
        assert bool(d_name_adapted == ad_adapted.c.name)

    def test_two_joins_sql(self):
        q = self._two_join_fixture()
        self.assert_compile(
            q,
            "SELECT a.name AS a_name, a_1.name AS a_1_name, "
            "a_2.name AS a_2_name "
            "FROM a JOIN b ON a.id = b.id JOIN "
            "(a AS a_1 JOIN c AS c_1 ON a_1.id = c_1.id) ON c_1.bid = b.id "
            "JOIN (a AS a_2 JOIN d AS d_1 ON a_2.id = d_1.id) "
            "ON d_1.cid = c_1.id",
        )


class SameNameOnJoined(fixtures.MappedTest):

    run_setup_mappers = "once"
    run_inserts = None
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("t", String(5)),
        )
        Table(
            "a_sub",
            metadata,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
        )
        Table(
            "b",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("a_id", Integer, ForeignKey("a.id")),
        )

    @classmethod
    def setup_mappers(cls):
        class A(cls.Comparable):
            pass

        class ASub(A):
            pass

        class B(cls.Comparable):
            pass

        mapper(
            A,
            cls.tables.a,
            polymorphic_on=cls.tables.a.c.t,
            polymorphic_identity="a",
            properties={"bs": relationship(B, cascade="all, delete-orphan")},
        )

        mapper(
            ASub,
            cls.tables.a_sub,
            inherits=A,
            polymorphic_identity="asub",
            properties={"bs": relationship(B, cascade="all, delete-orphan")},
        )

        mapper(B, cls.tables.b)

    def test_persist(self):
        A, ASub, B = self.classes("A", "ASub", "B")

        s = Session(testing.db)

        s.add_all([A(bs=[B(), B(), B()]), ASub(bs=[B(), B(), B()])])
        s.commit()

        eq_(s.query(B).count(), 6)

        for a in s.query(A):
            eq_(len(a.bs), 3)
            s.delete(a)

        s.commit()

        eq_(s.query(B).count(), 0)


class BetweenSubclassJoinWExtraJoinedLoad(
    fixtures.DeclarativeMappedTest, testing.AssertsCompiledSQL
):
    """test for [ticket:3884]"""

    run_define_tables = None
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Person(Base):
            __tablename__ = "people"
            id = Column(Integer, primary_key=True)
            discriminator = Column("type", String(50))
            __mapper_args__ = {"polymorphic_on": discriminator}

        class Manager(Person):
            __tablename__ = "managers"
            __mapper_args__ = {"polymorphic_identity": "manager"}
            id = Column(Integer, ForeignKey("people.id"), primary_key=True)

        class Engineer(Person):
            __tablename__ = "engineers"
            __mapper_args__ = {"polymorphic_identity": "engineer"}
            id = Column(Integer, ForeignKey("people.id"), primary_key=True)
            primary_language = Column(String(50))
            manager_id = Column(Integer, ForeignKey("managers.id"))
            manager = relationship(
                Manager, primaryjoin=(Manager.id == manager_id)
            )

        class LastSeen(Base):
            __tablename__ = "seen"
            id = Column(Integer, ForeignKey("people.id"), primary_key=True)
            timestamp = Column(Integer)
            taggable = relationship(
                Person,
                primaryjoin=(Person.id == id),
                backref=backref("last_seen", lazy=False),
            )

    def test_query(self):
        Engineer, Manager = self.classes("Engineer", "Manager")

        sess = fixture_session()

        # eager join is both from Enginer->LastSeen as well as
        # Manager->LastSeen.  In the case of Manager->LastSeen,
        # Manager is internally aliased, and comes to JoinedEagerLoader
        # with no "parent" entity but an adapter.
        q = sess.query(Engineer, Manager).join(Engineer.manager)
        self.assert_compile(
            q,
            "SELECT people.type AS people_type, engineers.id AS engineers_id, "
            "people.id AS people_id, "
            "engineers.primary_language AS engineers_primary_language, "
            "engineers.manager_id AS engineers_manager_id, "
            "people_1.type AS people_1_type, managers_1.id AS managers_1_id, "
            "people_1.id AS people_1_id, seen_1.id AS seen_1_id, "
            "seen_1.timestamp AS seen_1_timestamp, seen_2.id AS seen_2_id, "
            "seen_2.timestamp AS seen_2_timestamp "
            "FROM people JOIN engineers ON people.id = engineers.id "
            "JOIN (people AS people_1 JOIN managers AS managers_1 "
            "ON people_1.id = managers_1.id) "
            "ON managers_1.id = engineers.manager_id LEFT OUTER JOIN "
            "seen AS seen_1 ON people.id = seen_1.id LEFT OUTER JOIN "
            "seen AS seen_2 ON people_1.id = seen_2.id",
        )


class M2ODontLoadSiblingTest(fixtures.DeclarativeMappedTest):
    """test for #5210"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Parent(Base, ComparableEntity):
            __tablename__ = "parents"

            id = Column(Integer, primary_key=True)
            child_type = Column(String(50), nullable=False)

            __mapper_args__ = {
                "polymorphic_on": child_type,
            }

        class Child1(Parent):
            __tablename__ = "children_1"

            id = Column(Integer, ForeignKey(Parent.id), primary_key=True)

            __mapper_args__ = {
                "polymorphic_identity": "child1",
            }

        class Child2(Parent):
            __tablename__ = "children_2"

            id = Column(Integer, ForeignKey(Parent.id), primary_key=True)

            __mapper_args__ = {
                "polymorphic_identity": "child2",
            }

        class Other(Base):
            __tablename__ = "others"

            id = Column(Integer, primary_key=True)
            parent_id = Column(Integer, ForeignKey(Parent.id))

            parent = relationship(Parent)
            child2 = relationship(Child2, viewonly=True)

    @classmethod
    def insert_data(cls, connection):
        Other, Child1 = cls.classes("Other", "Child1")
        s = Session(connection)
        obj = Other(parent=Child1())
        s.add(obj)
        s.commit()

    def test_load_m2o_emit_query(self):
        Other, Child1 = self.classes("Other", "Child1")
        s = fixture_session()

        obj = s.query(Other).first()

        is_(obj.child2, None)
        eq_(obj.parent, Child1())

    def test_load_m2o_use_get(self):
        Other, Child1 = self.classes("Other", "Child1")
        s = fixture_session()

        obj = s.query(Other).first()
        c1 = s.query(Child1).first()

        is_(obj.child2, None)
        is_(obj.parent, c1)
