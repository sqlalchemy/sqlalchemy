from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy import null
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import aliased
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import polymorphic_union
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import expect_deprecated_20
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from ._poly_fixtures import _Polymorphic
from ._poly_fixtures import _PolymorphicAliasedJoins
from ._poly_fixtures import _PolymorphicJoins
from ._poly_fixtures import _PolymorphicPolymorphic
from ._poly_fixtures import _PolymorphicUnions
from ._poly_fixtures import Boss
from ._poly_fixtures import Company
from ._poly_fixtures import Engineer
from ._poly_fixtures import Machine
from ._poly_fixtures import Manager
from ._poly_fixtures import Paperwork
from ._poly_fixtures import Person


aliased_jp_dep = (
    r"The ``aliased`` and ``from_joinpoint`` keyword arguments "
    r"to Query.join\(\) are deprecated"
)

with_polymorphic_dep = (
    r"The Query.with_polymorphic\(\) method is considered legacy as of "
    r"the 1.x series of SQLAlchemy and will be removed in 2.0"
)


class _PolymorphicTestBase(fixtures.NoCache):
    __backend__ = True
    __dialect__ = "default_enhanced"

    @classmethod
    def setup_mappers(cls):
        super(_PolymorphicTestBase, cls).setup_mappers()
        global people, engineers, managers, boss
        global companies, paperwork, machines
        people, engineers, managers, boss, companies, paperwork, machines = (
            cls.tables.people,
            cls.tables.engineers,
            cls.tables.managers,
            cls.tables.boss,
            cls.tables.companies,
            cls.tables.paperwork,
            cls.tables.machines,
        )

    @classmethod
    def insert_data(cls, connection):
        super(_PolymorphicTestBase, cls).insert_data(connection)

        global all_employees, c1_employees, c2_employees
        global c1, c2, e1, e2, e3, b1, m1
        c1, c2, all_employees, c1_employees, c2_employees = (
            cls.c1,
            cls.c2,
            cls.all_employees,
            cls.c1_employees,
            cls.c2_employees,
        )
        e1, e2, e3, b1, m1 = cls.e1, cls.e2, cls.e3, cls.b1, cls.m1

    def test_join_from_polymorphic_flag_aliased_one(self):
        sess = fixture_session()
        with expect_deprecated_20(aliased_jp_dep):
            eq_(
                sess.query(Person)
                .order_by(Person.person_id)
                .join(Person.paperwork, aliased=True)
                .filter(Paperwork.description.like("%review%"))
                .all(),
                [b1, m1],
            )

    def test_join_from_polymorphic_flag_aliased_two(self):
        sess = fixture_session()
        with expect_deprecated_20(aliased_jp_dep):
            eq_(
                sess.query(Person)
                .order_by(Person.person_id)
                .join(Person.paperwork, aliased=True)
                .filter(Paperwork.description.like("%#2%"))
                .all(),
                [e1, m1],
            )

    def test_join_from_with_polymorphic_flag_aliased_one(self):
        sess = fixture_session()
        with expect_deprecated_20(aliased_jp_dep, with_polymorphic_dep):
            eq_(
                sess.query(Person)
                .with_polymorphic(Manager)
                .join(Person.paperwork, aliased=True)
                .filter(Paperwork.description.like("%review%"))
                .all(),
                [b1, m1],
            )

    def test_join_from_with_polymorphic_flag_aliased_two(self):
        sess = fixture_session()
        with expect_deprecated_20(aliased_jp_dep, with_polymorphic_dep):
            eq_(
                sess.query(Person)
                .with_polymorphic([Manager, Engineer])
                .order_by(Person.person_id)
                .join(Person.paperwork, aliased=True)
                .filter(Paperwork.description.like("%#2%"))
                .all(),
                [e1, m1],
            )

    def test_join_to_polymorphic_flag_aliased(self):
        sess = fixture_session()
        with expect_deprecated_20(aliased_jp_dep):
            eq_(
                sess.query(Company)
                .join(Company.employees, aliased=True)
                .filter(Person.name == "vlad")
                .one(),
                c2,
            )

    def test_polymorphic_any_flag_alias_two(self):
        sess = fixture_session()
        # test that the aliasing on "Person" does not bleed into the
        # EXISTS clause generated by any()
        any_ = Company.employees.any(Person.name == "wally")
        with expect_deprecated_20(aliased_jp_dep):
            eq_(
                sess.query(Company)
                .join(Company.employees, aliased=True)
                .filter(Person.name == "dilbert")
                .filter(any_)
                .all(),
                [c1],
            )

    def test_join_from_polymorphic_flag_aliased_three(self):
        sess = fixture_session()
        with expect_deprecated_20(aliased_jp_dep):
            eq_(
                sess.query(Engineer)
                .order_by(Person.person_id)
                .join(Person.paperwork, aliased=True)
                .filter(Paperwork.description.like("%#2%"))
                .all(),
                [e1],
            )

    def test_with_polymorphic_one(self):
        sess = fixture_session()

        def go():
            with expect_deprecated_20(with_polymorphic_dep):
                eq_(
                    sess.query(Person)
                    .with_polymorphic(Engineer)
                    .filter(Engineer.primary_language == "java")
                    .all(),
                    self._emps_wo_relationships_fixture()[0:1],
                )

        self.assert_sql_count(testing.db, go, 1)

    def test_with_polymorphic_two(self):
        sess = fixture_session()

        def go():
            with expect_deprecated_20(with_polymorphic_dep):
                eq_(
                    sess.query(Person)
                    .with_polymorphic("*")
                    .order_by(Person.person_id)
                    .all(),
                    self._emps_wo_relationships_fixture(),
                )

        self.assert_sql_count(testing.db, go, 1)

    def test_with_polymorphic_three(self):
        sess = fixture_session()

        def go():
            with expect_deprecated_20(with_polymorphic_dep):
                eq_(
                    sess.query(Person)
                    .with_polymorphic(Engineer)
                    .order_by(Person.person_id)
                    .all(),
                    self._emps_wo_relationships_fixture(),
                )

        self.assert_sql_count(testing.db, go, 3)

    def test_with_polymorphic_four(self):
        sess = fixture_session()

        def go():
            with expect_deprecated_20(with_polymorphic_dep):
                eq_(
                    sess.query(Person)
                    .with_polymorphic(Engineer, people.outerjoin(engineers))
                    .order_by(Person.person_id)
                    .all(),
                    self._emps_wo_relationships_fixture(),
                )

        self.assert_sql_count(testing.db, go, 3)

    def test_with_polymorphic_five(self):
        sess = fixture_session()

        def go():
            # limit the polymorphic join down to just "Person",
            # overriding select_table
            with expect_deprecated_20(with_polymorphic_dep):
                eq_(
                    sess.query(Person).with_polymorphic(Person).all(),
                    self._emps_wo_relationships_fixture(),
                )

        self.assert_sql_count(testing.db, go, 6)

    def test_with_polymorphic_six(self):
        sess = fixture_session()

        with expect_deprecated_20(with_polymorphic_dep):
            assert_raises(
                sa_exc.InvalidRequestError,
                sess.query(Person).with_polymorphic,
                Paperwork,
            )
        with expect_deprecated_20(with_polymorphic_dep):
            assert_raises(
                sa_exc.InvalidRequestError,
                sess.query(Engineer).with_polymorphic,
                Boss,
            )
        with expect_deprecated_20(with_polymorphic_dep):
            assert_raises(
                sa_exc.InvalidRequestError,
                sess.query(Engineer).with_polymorphic,
                Person,
            )

    def test_with_polymorphic_seven(self):
        sess = fixture_session()
        # compare to entities without related collections to prevent
        # additional lazy SQL from firing on loaded entities
        with expect_deprecated_20(with_polymorphic_dep):
            eq_(
                sess.query(Person)
                .with_polymorphic("*")
                .order_by(Person.person_id)
                .all(),
                self._emps_wo_relationships_fixture(),
            )

    def test_joinedload_on_subclass(self):
        sess = fixture_session()
        expected = [
            Engineer(
                name="dilbert",
                engineer_name="dilbert",
                primary_language="java",
                status="regular engineer",
                machines=[
                    Machine(name="IBM ThinkPad"),
                    Machine(name="IPhone"),
                ],
            )
        ]

        def go():
            # test load People with joinedload to engineers + machines
            with expect_deprecated_20(with_polymorphic_dep):
                eq_(
                    sess.query(Person)
                    .with_polymorphic("*")
                    .options(joinedload(Engineer.machines))
                    .filter(Person.name == "dilbert")
                    .all(),
                    expected,
                )

        self.assert_sql_count(testing.db, go, 1)

    def test_primary_eager_aliasing_three(self):

        # assert the JOINs don't over JOIN

        sess = fixture_session()

        def go():
            with expect_deprecated_20(with_polymorphic_dep):
                eq_(
                    sess.query(Person)
                    .with_polymorphic("*")
                    .order_by(Person.person_id)
                    .options(joinedload(Engineer.machines))[1:3],
                    all_employees[1:3],
                )

        self.assert_sql_count(testing.db, go, 3)

        with expect_deprecated_20(with_polymorphic_dep):
            eq_(
                sess.scalar(
                    select(func.count("*")).select_from(
                        sess.query(Person)
                        .with_polymorphic("*")
                        .options(joinedload(Engineer.machines))
                        .order_by(Person.person_id)
                        .limit(2)
                        .offset(1)
                        .subquery()
                    )
                ),
                2,
            )

    def test_join_from_with_polymorphic_nonaliased_one(self):
        sess = fixture_session()
        with expect_deprecated_20(with_polymorphic_dep):
            eq_(
                sess.query(Person)
                .with_polymorphic(Manager)
                .order_by(Person.person_id)
                .join(Person.paperwork)
                .filter(Paperwork.description.like("%review%"))
                .all(),
                [b1, m1],
            )

    def test_join_from_with_polymorphic_nonaliased_two(self):
        sess = fixture_session()
        with expect_deprecated_20(with_polymorphic_dep):
            eq_(
                sess.query(Person)
                .with_polymorphic([Manager, Engineer])
                .order_by(Person.person_id)
                .join(Person.paperwork)
                .filter(Paperwork.description.like("%#2%"))
                .all(),
                [e1, m1],
            )

    def test_join_from_with_polymorphic_nonaliased_three(self):
        sess = fixture_session()
        with expect_deprecated_20(with_polymorphic_dep):
            eq_(
                sess.query(Person)
                .with_polymorphic([Manager, Engineer])
                .order_by(Person.person_id)
                .join(Person.paperwork)
                .filter(Person.name.like("%dog%"))
                .filter(Paperwork.description.like("%#2%"))
                .all(),
                [m1],
            )

    def test_join_from_with_polymorphic_explicit_aliased_one(self):
        sess = fixture_session()
        pa = aliased(Paperwork)

        with expect_deprecated_20(with_polymorphic_dep):
            eq_(
                sess.query(Person)
                .with_polymorphic(Manager)
                .join(pa, Person.paperwork)
                .filter(pa.description.like("%review%"))
                .all(),
                [b1, m1],
            )

    def test_join_from_with_polymorphic_explicit_aliased_two(self):
        sess = fixture_session()
        pa = aliased(Paperwork)

        with expect_deprecated_20(with_polymorphic_dep):
            eq_(
                sess.query(Person)
                .with_polymorphic([Manager, Engineer])
                .order_by(Person.person_id)
                .join(pa, Person.paperwork)
                .filter(pa.description.like("%#2%"))
                .all(),
                [e1, m1],
            )

    def test_join_from_with_polymorphic_aliased_three(self):
        sess = fixture_session()
        pa = aliased(Paperwork)

        with expect_deprecated_20(with_polymorphic_dep):
            eq_(
                sess.query(Person)
                .with_polymorphic([Manager, Engineer])
                .order_by(Person.person_id)
                .join(pa, Person.paperwork)
                .filter(Person.name.like("%dog%"))
                .filter(pa.description.like("%#2%"))
                .all(),
                [m1],
            )


class PolymorphicTest(_PolymorphicTestBase, _Polymorphic):
    pass


class PolymorphicPolymorphicTest(
    _PolymorphicTestBase, _PolymorphicPolymorphic
):
    pass


class PolymorphicUnionsTest(_PolymorphicTestBase, _PolymorphicUnions):
    pass


class PolymorphicAliasedJoinsTest(
    _PolymorphicTestBase, _PolymorphicAliasedJoins
):
    pass


class PolymorphicJoinsTest(_PolymorphicTestBase, _PolymorphicJoins):
    pass


class RelationshipToSingleTest(
    testing.AssertsCompiledSQL, fixtures.MappedTest
):
    __dialect__ = "default"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "employees",
            metadata,
            Column(
                "employee_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("manager_data", String(50)),
            Column("engineer_info", String(50)),
            Column("type", String(20)),
            Column("company_id", Integer, ForeignKey("companies.company_id")),
        )

        Table(
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

    @classmethod
    def setup_classes(cls):
        class Company(cls.Comparable):
            pass

        class Employee(cls.Comparable):
            pass

        class Manager(Employee):
            pass

        class Engineer(Employee):
            pass

        class JuniorEngineer(Engineer):
            pass

    def test_of_type_aliased_fromjoinpoint(self):
        Company, Employee, Engineer = (
            self.classes.Company,
            self.classes.Employee,
            self.classes.Engineer,
        )
        companies, employees = self.tables.companies, self.tables.employees

        self.mapper_registry.map_imperatively(
            Company, companies, properties={"employee": relationship(Employee)}
        )
        self.mapper_registry.map_imperatively(
            Employee, employees, polymorphic_on=employees.c.type
        )
        self.mapper_registry.map_imperatively(
            Engineer, inherits=Employee, polymorphic_identity="engineer"
        )

        sess = fixture_session()

        with expect_deprecated_20(aliased_jp_dep):
            self.assert_compile(
                sess.query(Company).outerjoin(
                    Company.employee.of_type(Engineer),
                    aliased=True,
                    from_joinpoint=True,
                ),
                "SELECT companies.company_id AS companies_company_id, "
                "companies.name AS companies_name FROM companies "
                "LEFT OUTER JOIN employees AS employees_1 ON "
                "companies.company_id = employees_1.company_id "
                "AND employees_1.type IN (__[POSTCOMPILE_type_1])",
            )


class SingleOnJoinedTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global persons_table, employees_table

        persons_table = Table(
            "persons",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("type", String(20), nullable=False),
        )

        employees_table = Table(
            "employees",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("persons.person_id"),
                primary_key=True,
            ),
            Column("employee_data", String(50)),
            Column("manager_data", String(50)),
        )

    def test_single_on_joined(self):
        class Person(fixtures.ComparableEntity):
            pass

        class Employee(Person):
            pass

        class Manager(Employee):
            pass

        self.mapper_registry.map_imperatively(
            Person,
            persons_table,
            polymorphic_on=persons_table.c.type,
            polymorphic_identity="person",
        )
        self.mapper_registry.map_imperatively(
            Employee,
            employees_table,
            inherits=Person,
            polymorphic_identity="engineer",
        )
        self.mapper_registry.map_imperatively(
            Manager, inherits=Employee, polymorphic_identity="manager"
        )

        sess = fixture_session()
        sess.add(Person(name="p1"))
        sess.add(Employee(name="e1", employee_data="ed1"))
        sess.add(Manager(name="m1", employee_data="ed2", manager_data="md1"))
        sess.flush()
        sess.expunge_all()

        eq_(
            sess.query(Person).order_by(Person.person_id).all(),
            [
                Person(name="p1"),
                Employee(name="e1", employee_data="ed1"),
                Manager(name="m1", employee_data="ed2", manager_data="md1"),
            ],
        )
        sess.expunge_all()

        eq_(
            sess.query(Employee).order_by(Person.person_id).all(),
            [
                Employee(name="e1", employee_data="ed1"),
                Manager(name="m1", employee_data="ed2", manager_data="md1"),
            ],
        )
        sess.expunge_all()

        eq_(
            sess.query(Manager).order_by(Person.person_id).all(),
            [Manager(name="m1", employee_data="ed2", manager_data="md1")],
        )
        sess.expunge_all()

        def go():
            with expect_deprecated_20(with_polymorphic_dep):
                eq_(
                    sess.query(Person)
                    .with_polymorphic("*")
                    .order_by(Person.person_id)
                    .all(),
                    [
                        Person(name="p1"),
                        Employee(name="e1", employee_data="ed1"),
                        Manager(
                            name="m1", employee_data="ed2", manager_data="md1"
                        ),
                    ],
                )

        self.assert_sql_count(testing.db, go, 1)


class SingleFromPolySelectableTest(
    fixtures.DeclarativeMappedTest, AssertsCompiledSQL
):
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls, with_polymorphic=None, include_sub_defaults=False):
        Base = cls.DeclarativeBasic

        class Employee(Base):
            __tablename__ = "employee"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            type = Column(String(50))

            __mapper_args__ = {
                "polymorphic_identity": "employee",
                "polymorphic_on": type,
            }

        class Engineer(Employee):
            __tablename__ = "engineer"
            id = Column(Integer, ForeignKey("employee.id"), primary_key=True)
            engineer_info = Column(String(50))
            manager_id = Column(ForeignKey("manager.id"))
            __mapper_args__ = {"polymorphic_identity": "engineer"}

        class Manager(Employee):
            __tablename__ = "manager"
            id = Column(Integer, ForeignKey("employee.id"), primary_key=True)
            manager_data = Column(String(50))
            __mapper_args__ = {"polymorphic_identity": "manager"}

        class Boss(Manager):
            __mapper_args__ = {"polymorphic_identity": "boss"}

    def _with_poly_fixture(self):
        employee = self.classes.Employee.__table__
        engineer = self.classes.Engineer.__table__
        manager = self.classes.Manager.__table__

        poly = (
            select(
                employee.c.id,
                employee.c.type,
                employee.c.name,
                manager.c.manager_data,
                null().label("engineer_info"),
                null().label("manager_id"),
            )
            .select_from(employee.join(manager))
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .union_all(
                select(
                    employee.c.id,
                    employee.c.type,
                    employee.c.name,
                    null().label("manager_data"),
                    engineer.c.engineer_info,
                    engineer.c.manager_id,
                )
                .select_from(employee.join(engineer))
                .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            )
            .alias()
        )

        return poly

    def test_query_wpoly_single_inh_subclass(self):
        Boss = self.classes.Boss

        poly = self._with_poly_fixture()

        s = fixture_session()

        with expect_deprecated_20(with_polymorphic_dep):
            q = s.query(Boss).with_polymorphic(Boss, poly)
        self.assert_compile(
            q,
            "SELECT anon_1.employee_id AS anon_1_employee_id, "
            "anon_1.employee_name AS anon_1_employee_name, "
            "anon_1.employee_type AS anon_1_employee_type, "
            "anon_1.manager_manager_data AS anon_1_manager_manager_data "
            "FROM (SELECT employee.id AS employee_id, employee.type "
            "AS employee_type, employee.name AS employee_name, "
            "manager.manager_data AS manager_manager_data, "
            "NULL AS engineer_info, NULL AS manager_id FROM employee "
            "JOIN manager ON employee.id = manager.id "
            "UNION ALL SELECT employee.id AS employee_id, "
            "employee.type AS employee_type, employee.name AS employee_name, "
            "NULL AS manager_data, "
            "engineer.engineer_info AS engineer_engineer_info, "
            "engineer.manager_id AS engineer_manager_id "
            "FROM employee JOIN engineer ON employee.id = engineer.id) "
            "AS anon_1 WHERE anon_1.employee_type IN (__[POSTCOMPILE_type_1])",
        )


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

        cls.mapper_registry.map_imperatively(
            A, cls.tables.a, polymorphic_on=cls.tables.a.c.type
        )
        cls.mapper_registry.map_imperatively(
            B,
            cls.tables.b,
            inherits=A,
            polymorphic_identity="b",
            properties={"related": relationship(D, secondary=cls.tables.btod)},
        )
        cls.mapper_registry.map_imperatively(
            C,
            cls.tables.c,
            inherits=A,
            polymorphic_identity="c",
            properties={"related": relationship(D, secondary=cls.tables.ctod)},
        )
        cls.mapper_registry.map_imperatively(D, cls.tables.d)

    @classmethod
    def insert_data(cls, connection):
        B = cls.classes.B
        C = cls.classes.C
        D = cls.classes.D

        session = Session(connection)

        d = D()
        session.add_all([B(related=[d]), C(related=[d])])
        session.commit()

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
            with expect_deprecated_20(with_polymorphic_dep):
                for a in (
                    session.query(A)
                    .with_polymorphic([B, C])
                    .options(selectinload(B.related), selectinload(C.related))
                ):
                    eq_(a.related, [d])

        self.assert_sql_count(testing.db, go, 3)

    def test_fixed_w_poly_joined(self):
        A = self.classes.A
        B = self.classes.B
        C = self.classes.C
        D = self.classes.D

        session = fixture_session()
        d = session.query(D).one()

        def go():
            with expect_deprecated_20(with_polymorphic_dep):
                for a in (
                    session.query(A)
                    .with_polymorphic([B, C])
                    .options(joinedload(B.related), joinedload(C.related))
                ):
                    eq_(a.related, [d])

        self.assert_sql_count(testing.db, go, 1)


class ConcreteTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "companies",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
        )
        Table(
            "employees",
            metadata,
            Column(
                "employee_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("company_id", Integer, ForeignKey("companies.id")),
        )
        Table(
            "managers",
            metadata,
            Column(
                "employee_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("manager_data", String(50)),
            Column("company_id", Integer, ForeignKey("companies.id")),
        )
        Table(
            "engineers",
            metadata,
            Column(
                "employee_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("engineer_info", String(50)),
            Column("company_id", Integer, ForeignKey("companies.id")),
        )
        Table(
            "hackers",
            metadata,
            Column(
                "employee_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("engineer_info", String(50)),
            Column("company_id", Integer, ForeignKey("companies.id")),
            Column("nickname", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class Employee(cls.Basic):
            def __init__(self, name):
                self.name = name

            def __repr__(self):
                return self.__class__.__name__ + " " + self.name

        class Manager(Employee):
            def __init__(self, name, manager_data):
                self.name = name
                self.manager_data = manager_data

            def __repr__(self):
                return (
                    self.__class__.__name__
                    + " "
                    + self.name
                    + " "
                    + self.manager_data
                )

        class Engineer(Employee):
            def __init__(self, name, engineer_info):
                self.name = name
                self.engineer_info = engineer_info

            def __repr__(self):
                return (
                    self.__class__.__name__
                    + " "
                    + self.name
                    + " "
                    + self.engineer_info
                )

        class Hacker(Engineer):
            def __init__(self, name, nickname, engineer_info):
                self.name = name
                self.nickname = nickname
                self.engineer_info = engineer_info

            def __repr__(self):
                return (
                    self.__class__.__name__
                    + " "
                    + self.name
                    + " '"
                    + self.nickname
                    + "' "
                    + self.engineer_info
                )

        class Company(cls.Basic):
            pass

    def test_without_default_polymorphic(self):
        Employee, Engineer, Manager = self.classes(
            "Employee", "Engineer", "Manager"
        )
        (Hacker,) = self.classes("Hacker")
        (employees_table,) = self.tables("employees")
        engineers_table, managers_table = self.tables("engineers", "managers")
        (hackers_table,) = self.tables("hackers")

        pjoin = polymorphic_union(
            {
                "employee": employees_table,
                "manager": managers_table,
                "engineer": engineers_table,
                "hacker": hackers_table,
            },
            "type",
            "pjoin",
        )
        pjoin2 = polymorphic_union(
            {"engineer": engineers_table, "hacker": hackers_table},
            "type",
            "pjoin2",
        )
        employee_mapper = self.mapper_registry.map_imperatively(
            Employee, employees_table, polymorphic_identity="employee"
        )
        self.mapper_registry.map_imperatively(
            Manager,
            managers_table,
            inherits=employee_mapper,
            concrete=True,
            polymorphic_identity="manager",
        )
        engineer_mapper = self.mapper_registry.map_imperatively(
            Engineer,
            engineers_table,
            inherits=employee_mapper,
            concrete=True,
            polymorphic_identity="engineer",
        )
        self.mapper_registry.map_imperatively(
            Hacker,
            hackers_table,
            inherits=engineer_mapper,
            concrete=True,
            polymorphic_identity="hacker",
        )
        session = fixture_session()
        jdoe = Employee("Jdoe")
        tom = Manager("Tom", "knows how to manage things")
        jerry = Engineer("Jerry", "knows how to program")
        hacker = Hacker("Kurt", "Badass", "knows how to hack")
        session.add_all((jdoe, tom, jerry, hacker))
        session.flush()

        with expect_deprecated_20(with_polymorphic_dep):
            eq_(
                len(
                    session.connection()
                    .execute(
                        session.query(Employee)
                        .with_polymorphic("*", pjoin, pjoin.c.type)
                        .statement
                    )
                    .fetchall()
                ),
                4,
            )
        eq_(session.get(Employee, jdoe.employee_id), jdoe)
        eq_(session.get(Engineer, jerry.employee_id), jerry)
        with expect_deprecated_20(with_polymorphic_dep):
            eq_(
                set(
                    [
                        repr(x)
                        for x in session.query(Employee).with_polymorphic(
                            "*", pjoin, pjoin.c.type
                        )
                    ]
                ),
                set(
                    [
                        "Employee Jdoe",
                        "Engineer Jerry knows how to program",
                        "Manager Tom knows how to manage things",
                        "Hacker Kurt 'Badass' knows how to hack",
                    ]
                ),
            )
        eq_(
            set([repr(x) for x in session.query(Manager)]),
            set(["Manager Tom knows how to manage things"]),
        )
        with expect_deprecated_20(with_polymorphic_dep):
            eq_(
                set(
                    [
                        repr(x)
                        for x in session.query(Engineer).with_polymorphic(
                            "*", pjoin2, pjoin2.c.type
                        )
                    ]
                ),
                set(
                    [
                        "Engineer Jerry knows how to program",
                        "Hacker Kurt 'Badass' knows how to hack",
                    ]
                ),
            )
        eq_(
            set([repr(x) for x in session.query(Hacker)]),
            set(["Hacker Kurt 'Badass' knows how to hack"]),
        )

        # test adaption of the column by wrapping the query in a
        # subquery

        with testing.expect_deprecated(
            r"The Query.from_self\(\) method", with_polymorphic_dep
        ):
            eq_(
                len(
                    session.connection()
                    .execute(
                        session.query(Engineer)
                        .with_polymorphic("*", pjoin2, pjoin2.c.type)
                        .from_self()
                        .statement
                    )
                    .fetchall()
                ),
                2,
            )
        with testing.expect_deprecated(
            r"The Query.from_self\(\) method", with_polymorphic_dep
        ):
            eq_(
                set(
                    [
                        repr(x)
                        for x in session.query(Engineer)
                        .with_polymorphic("*", pjoin2, pjoin2.c.type)
                        .from_self()
                    ]
                ),
                set(
                    [
                        "Engineer Jerry knows how to program",
                        "Hacker Kurt 'Badass' knows how to hack",
                    ]
                ),
            )
