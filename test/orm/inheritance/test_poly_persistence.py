"""tests basic polymorphic mapper loading/saving, minimal relationships"""

from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy.orm import mapper
from sqlalchemy.orm import polymorphic_union
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column


class Person(fixtures.ComparableEntity):
    pass


class Engineer(Person):
    pass


class Manager(Person):
    pass


class Boss(Manager):
    pass


class Company(fixtures.ComparableEntity):
    pass


class PolymorphTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global companies, people, engineers, managers, boss

        companies = Table(
            "companies",
            metadata,
            Column(
                "company_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
        )

        people = Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column(
                "company_id",
                Integer,
                ForeignKey("companies.company_id"),
                nullable=False,
            ),
            Column("name", String(50)),
            Column("type", String(30)),
        )

        engineers = Table(
            "engineers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("status", String(30)),
            Column("engineer_name", String(50)),
            Column("primary_language", String(50)),
        )

        managers = Table(
            "managers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("status", String(30)),
            Column("manager_name", String(50)),
        )

        boss = Table(
            "boss",
            metadata,
            Column(
                "boss_id",
                Integer,
                ForeignKey("managers.person_id"),
                primary_key=True,
            ),
            Column("golf_swing", String(30)),
        )


class InsertOrderTest(PolymorphTest):
    def test_insert_order(self):
        """test that classes of multiple types mix up mapper inserts
        so that insert order of individual tables is maintained"""

        person_join = polymorphic_union(
            {
                "engineer": people.join(engineers),
                "manager": people.join(managers),
                "person": people.select()
                .where(people.c.type == "person")
                .subquery(),
            },
            None,
            "pjoin",
        )

        person_mapper = mapper(
            Person,
            people,
            with_polymorphic=("*", person_join),
            polymorphic_on=person_join.c.type,
            polymorphic_identity="person",
        )

        mapper(
            Engineer,
            engineers,
            inherits=person_mapper,
            polymorphic_identity="engineer",
        )
        mapper(
            Manager,
            managers,
            inherits=person_mapper,
            polymorphic_identity="manager",
        )
        mapper(
            Company,
            companies,
            properties={
                "employees": relationship(
                    Person, backref="company", order_by=person_join.c.person_id
                )
            },
        )

        session = fixture_session()
        c = Company(name="company1")
        c.employees.append(
            Manager(
                status="AAB",
                manager_name="manager1",
                name="pointy haired boss",
            )
        )
        c.employees.append(
            Engineer(
                status="BBA",
                engineer_name="engineer1",
                primary_language="java",
                name="dilbert",
            )
        )
        c.employees.append(Person(status="HHH", name="joesmith"))
        c.employees.append(
            Engineer(
                status="CGG",
                engineer_name="engineer2",
                primary_language="python",
                name="wally",
            )
        )
        c.employees.append(
            Manager(status="ABA", manager_name="manager2", name="jsmith")
        )
        session.add(c)
        session.flush()
        session.expunge_all()
        eq_(session.get(Company, c.company_id), c)


@testing.combinations(
    ("lazy", True), ("nonlazy", False), argnames="lazy_relationship", id_="ia"
)
@testing.combinations(
    ("redefine", True),
    ("noredefine", False),
    argnames="redefine_colprop",
    id_="ia",
)
@testing.combinations(
    ("unions", True),
    ("unions", False),
    ("joins", False),
    ("auto", False),
    ("none", False),
    argnames="with_polymorphic,include_base",
    id_="rr",
)
class RoundTripTest(PolymorphTest):
    lazy_relationship = None
    include_base = None
    redefine_colprop = None
    with_polymorphic = None

    run_inserts = "once"
    run_deletes = None
    run_setup_mappers = "once"

    @classmethod
    def setup_mappers(cls):
        include_base = cls.include_base
        lazy_relationship = cls.lazy_relationship
        redefine_colprop = cls.redefine_colprop
        with_polymorphic = cls.with_polymorphic

        if with_polymorphic == "unions":
            if include_base:
                person_join = polymorphic_union(
                    {
                        "engineer": people.join(engineers),
                        "manager": people.join(managers),
                        "person": people.select()
                        .where(people.c.type == "person")
                        .subquery(),
                    },
                    None,
                    "pjoin",
                )
            else:
                person_join = polymorphic_union(
                    {
                        "engineer": people.join(engineers),
                        "manager": people.join(managers),
                    },
                    None,
                    "pjoin",
                )

            manager_join = people.join(managers).outerjoin(boss)
            person_with_polymorphic = ["*", person_join]
            manager_with_polymorphic = ["*", manager_join]
        elif with_polymorphic == "joins":
            person_join = (
                people.outerjoin(engineers).outerjoin(managers).outerjoin(boss)
            )
            manager_join = people.join(managers).outerjoin(boss)
            person_with_polymorphic = ["*", person_join]
            manager_with_polymorphic = ["*", manager_join]
        elif with_polymorphic == "auto":
            person_with_polymorphic = "*"
            manager_with_polymorphic = "*"
        else:
            person_with_polymorphic = None
            manager_with_polymorphic = None

        if redefine_colprop:
            person_mapper = mapper(
                Person,
                people,
                with_polymorphic=person_with_polymorphic,
                polymorphic_on=people.c.type,
                polymorphic_identity="person",
                properties={"person_name": people.c.name},
            )
        else:
            person_mapper = mapper(
                Person,
                people,
                with_polymorphic=person_with_polymorphic,
                polymorphic_on=people.c.type,
                polymorphic_identity="person",
            )

        mapper(
            Engineer,
            engineers,
            inherits=person_mapper,
            polymorphic_identity="engineer",
        )
        mapper(
            Manager,
            managers,
            inherits=person_mapper,
            with_polymorphic=manager_with_polymorphic,
            polymorphic_identity="manager",
        )

        mapper(Boss, boss, inherits=Manager, polymorphic_identity="boss")

        mapper(
            Company,
            companies,
            properties={
                "employees": relationship(
                    Person,
                    lazy=lazy_relationship,
                    cascade="all, delete-orphan",
                    backref="company",
                    order_by=people.c.person_id,
                )
            },
        )

    @classmethod
    def insert_data(cls, connection):
        redefine_colprop = cls.redefine_colprop
        include_base = cls.include_base

        if redefine_colprop:
            person_attribute_name = "person_name"
        else:
            person_attribute_name = "name"

        employees = [
            Manager(
                status="AAB",
                manager_name="manager1",
                **{person_attribute_name: "pointy haired boss"}
            ),
            Engineer(
                status="BBA",
                engineer_name="engineer1",
                primary_language="java",
                **{person_attribute_name: "dilbert"}
            ),
        ]
        if include_base:
            employees.append(Person(**{person_attribute_name: "joesmith"}))
        employees += [
            Engineer(
                status="CGG",
                engineer_name="engineer2",
                primary_language="python",
                **{person_attribute_name: "wally"}
            ),
            Manager(
                status="ABA",
                manager_name="manager2",
                **{person_attribute_name: "jsmith"}
            ),
        ]

        session = Session(connection)
        c = Company(name="company1")
        c.employees = employees
        session.add(c)

        session.commit()

    @testing.fixture
    def get_dilbert(self):
        def run(session):
            if self.redefine_colprop:
                person_attribute_name = "person_name"
            else:
                person_attribute_name = "name"

            dilbert = (
                session.query(Engineer)
                .filter_by(**{person_attribute_name: "dilbert"})
                .one()
            )
            return dilbert

        return run

    def test_lazy_load(self):
        lazy_relationship = self.lazy_relationship
        with_polymorphic = self.with_polymorphic

        if self.redefine_colprop:
            person_attribute_name = "person_name"
        else:
            person_attribute_name = "name"

        session = fixture_session()

        dilbert = (
            session.query(Engineer)
            .filter_by(**{person_attribute_name: "dilbert"})
            .one()
        )
        employees = session.query(Person).order_by(Person.person_id).all()
        company = session.query(Company).first()

        eq_(session.get(Person, dilbert.person_id), dilbert)
        session.expunge_all()

        eq_(
            session.query(Person)
            .filter(Person.person_id == dilbert.person_id)
            .one(),
            dilbert,
        )
        session.expunge_all()

        def go():
            cc = session.get(Company, company.company_id)
            eq_(cc.employees, employees)

        if not lazy_relationship:
            if with_polymorphic != "none":
                self.assert_sql_count(testing.db, go, 1)
            else:
                self.assert_sql_count(testing.db, go, 2)

        else:
            if with_polymorphic != "none":
                self.assert_sql_count(testing.db, go, 2)
            else:
                self.assert_sql_count(testing.db, go, 3)

    def test_baseclass_lookup(self, get_dilbert):
        session = fixture_session()
        dilbert = get_dilbert(session)

        if self.redefine_colprop:
            person_attribute_name = "person_name"
        else:
            person_attribute_name = "name"

        # test selecting from the query, using the base
        # mapped table (people) as the selection criterion.
        # in the case of the polymorphic Person query,
        # the "people" selectable should be adapted to be "person_join"
        eq_(
            session.query(Person)
            .filter(getattr(Person, person_attribute_name) == "dilbert")
            .first(),
            dilbert,
        )

    def test_subclass_lookup(self, get_dilbert):
        session = fixture_session()
        dilbert = get_dilbert(session)

        if self.redefine_colprop:
            person_attribute_name = "person_name"
        else:
            person_attribute_name = "name"

        eq_(
            session.query(Engineer)
            .filter(getattr(Person, person_attribute_name) == "dilbert")
            .first(),
            dilbert,
        )

    def test_baseclass_base_alias_filter(self, get_dilbert):
        session = fixture_session()
        dilbert = get_dilbert(session)

        # test selecting from the query, joining against
        # an alias of the base "people" table.  test that
        # the "palias" alias does *not* get sucked up
        # into the "person_join" conversion.
        palias = people.alias("palias")
        dilbert = session.get(Person, dilbert.person_id)
        is_(
            dilbert,
            session.query(Person)
            .filter(
                (palias.c.name == "dilbert")
                & (palias.c.person_id == Person.person_id)
            )
            .first(),
        )

    def test_subclass_base_alias_filter(self, get_dilbert):
        session = fixture_session()
        dilbert = get_dilbert(session)

        palias = people.alias("palias")

        is_(
            dilbert,
            session.query(Engineer)
            .filter(
                (palias.c.name == "dilbert")
                & (palias.c.person_id == Person.person_id)
            )
            .first(),
        )

    def test_baseclass_sub_table_filter(self, get_dilbert):
        session = fixture_session()
        dilbert = get_dilbert(session)

        # this unusual test is selecting from the plain people/engineers
        # table at the same time as the polymorphic entity
        is_(
            dilbert,
            session.query(Person)
            .filter(
                (Engineer.engineer_name == "engineer1")
                & (engineers.c.person_id == people.c.person_id)
                & (people.c.person_id == Person.person_id)
            )
            .first(),
        )

    def test_subclass_getitem(self, get_dilbert):
        session = fixture_session()
        dilbert = get_dilbert(session)

        is_(
            dilbert,
            session.query(Engineer).filter(
                Engineer.engineer_name == "engineer1"
            )[0],
        )

    def test_primary_table_only_for_requery(self):

        session = fixture_session()

        if self.redefine_colprop:
            person_attribute_name = "person_name"
        else:
            person_attribute_name = "name"

        dilbert = (  # noqa
            session.query(Person)
            .filter(getattr(Person, person_attribute_name) == "dilbert")
            .first()
        )

        def go():
            # assert that only primary table is queried for
            # already-present-in-session
            (
                session.query(Person)
                .filter(getattr(Person, person_attribute_name) == "dilbert")
                .first()
            )

        self.assert_sql_count(testing.db, go, 1)

    def test_standalone_orphans(self):
        if self.redefine_colprop:
            person_attribute_name = "person_name"
        else:
            person_attribute_name = "name"

        session = fixture_session()

        daboss = Boss(
            status="BBB",
            manager_name="boss",
            golf_swing="fore",
            **{person_attribute_name: "daboss"}
        )
        session.add(daboss)
        assert_raises(sa_exc.DBAPIError, session.flush)
