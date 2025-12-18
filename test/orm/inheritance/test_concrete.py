from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import null
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import union
from sqlalchemy import union_all
from sqlalchemy.ext.declarative import AbstractConcreteBase
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import aliased
from sqlalchemy.orm import attributes
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import composite
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import polymorphic_union
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.fixtures import RemoveORMEventsGlobally
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class ConcreteTest(AssertsCompiledSQL, fixtures.MappedTest):
    __dialect__ = "default"

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

    def test_basic(self):
        Employee, Engineer, Manager = self.classes(
            "Employee", "Engineer", "Manager"
        )
        engineers_table, managers_table = self.tables("engineers", "managers")

        pjoin = polymorphic_union(
            {"manager": managers_table, "engineer": engineers_table},
            "type",
            "pjoin",
        )
        employee_mapper = self.mapper_registry.map_imperatively(
            Employee, pjoin, polymorphic_on=pjoin.c.type
        )
        self.mapper_registry.map_imperatively(
            Manager,
            managers_table,
            inherits=employee_mapper,
            concrete=True,
            polymorphic_identity="manager",
        )
        self.mapper_registry.map_imperatively(
            Engineer,
            engineers_table,
            inherits=employee_mapper,
            concrete=True,
            polymorphic_identity="engineer",
        )
        session = fixture_session()
        session.add(Manager("Sally", "knows how to manage things"))
        session.add(Engineer("Karina", "knows how to hack"))
        session.flush()
        session.expunge_all()
        assert {repr(x) for x in session.query(Employee)} == {
            "Engineer Karina knows how to hack",
            "Manager Sally knows how to manage things",
        }

        assert {repr(x) for x in session.query(Manager)} == {
            "Manager Sally knows how to manage things"
        }
        assert {repr(x) for x in session.query(Engineer)} == {
            "Engineer Karina knows how to hack"
        }
        manager = session.query(Manager).one()
        session.expire(manager, ["manager_data"])
        eq_(manager.manager_data, "knows how to manage things")

    def test_multi_level_no_base(self):
        Employee, Engineer, Manager = self.classes(
            "Employee", "Engineer", "Manager"
        )
        (Hacker,) = self.classes("Hacker")
        engineers_table, managers_table = self.tables("engineers", "managers")
        (hackers_table,) = self.tables("hackers")

        pjoin = polymorphic_union(
            {
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
            Employee, pjoin, polymorphic_on=pjoin.c.type
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
            with_polymorphic=("*", pjoin2),
            polymorphic_on=pjoin2.c.type,
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
        sally = Manager("Sally", "knows how to manage things")

        assert_raises_message(
            AttributeError,
            "does not implement attribute .?'type' at the instance level.",
            setattr,
            sally,
            "type",
            "sometype",
        )

        # found_during_type_annotation
        # test the comparator returned by ConcreteInheritedProperty
        self.assert_compile(Manager.type == "x", "pjoin.type = :type_1")

        jenn = Engineer("Jenn", "knows how to program")
        hacker = Hacker("Karina", "Badass", "knows how to hack")

        assert_raises_message(
            AttributeError,
            "does not implement attribute .?'type' at the instance level.",
            setattr,
            hacker,
            "type",
            "sometype",
        )

        session.add_all((sally, jenn, hacker))
        session.flush()

        # ensure "readonly" on save logic didn't pollute the
        # expired_attributes collection

        assert (
            "nickname"
            not in attributes.instance_state(jenn).expired_attributes
        )
        assert "name" not in attributes.instance_state(jenn).expired_attributes
        assert (
            "name" not in attributes.instance_state(hacker).expired_attributes
        )
        assert (
            "nickname"
            not in attributes.instance_state(hacker).expired_attributes
        )

        def go():
            eq_(jenn.name, "Jenn")
            eq_(hacker.nickname, "Badass")

        self.assert_sql_count(testing.db, go, 0)
        session.expunge_all()
        assert (
            repr(
                session.query(Employee).filter(Employee.name == "Sally").one()
            )
            == "Manager Sally knows how to manage things"
        )
        assert (
            repr(session.query(Manager).filter(Manager.name == "Sally").one())
            == "Manager Sally knows how to manage things"
        )
        assert {repr(x) for x in session.query(Employee).all()} == {
            "Engineer Jenn knows how to program",
            "Manager Sally knows how to manage things",
            "Hacker Karina 'Badass' knows how to hack",
        }
        assert {repr(x) for x in session.query(Manager).all()} == {
            "Manager Sally knows how to manage things"
        }
        assert {repr(x) for x in session.query(Engineer).all()} == {
            "Engineer Jenn knows how to program",
            "Hacker Karina 'Badass' knows how to hack",
        }
        assert {repr(x) for x in session.query(Hacker).all()} == {
            "Hacker Karina 'Badass' knows how to hack"
        }

    def test_multi_level_no_base_w_hybrid(self):
        Employee, Engineer, Manager = self.classes(
            "Employee", "Engineer", "Manager"
        )
        (Hacker,) = self.classes("Hacker")
        engineers_table, managers_table = self.tables("engineers", "managers")
        (hackers_table,) = self.tables("hackers")

        pjoin = polymorphic_union(
            {
                "manager": managers_table,
                "engineer": engineers_table,
                "hacker": hackers_table,
            },
            "type",
            "pjoin",
        )

        test_calls = mock.Mock()

        class ManagerWHybrid(Employee):
            def __init__(self, name, manager_data):
                self.name = name
                self.manager_data = manager_data

            @hybrid_property
            def engineer_info(self):
                test_calls.engineer_info_instance()
                return self.manager_data

            @engineer_info.expression
            def engineer_info(cls):
                test_calls.engineer_info_class()
                return cls.manager_data

        employee_mapper = self.mapper_registry.map_imperatively(
            Employee, pjoin, polymorphic_on=pjoin.c.type
        )
        self.mapper_registry.map_imperatively(
            ManagerWHybrid,
            managers_table,
            inherits=employee_mapper,
            concrete=True,
            polymorphic_identity="manager",
        )
        self.mapper_registry.map_imperatively(
            Engineer,
            engineers_table,
            inherits=employee_mapper,
            concrete=True,
            polymorphic_identity="engineer",
        )

        session = fixture_session()
        sally = ManagerWHybrid("Sally", "mgrdata")

        # mapping did not impact the engineer_info
        # hybrid in any way
        eq_(test_calls.mock_calls, [])

        eq_(sally.engineer_info, "mgrdata")
        eq_(test_calls.mock_calls, [mock.call.engineer_info_instance()])

        session.add(sally)
        session.commit()

        session.close()

        Sally = (
            session.query(ManagerWHybrid)
            .filter(ManagerWHybrid.engineer_info == "mgrdata")
            .one()
        )
        eq_(
            test_calls.mock_calls,
            [
                mock.call.engineer_info_instance(),
                mock.call.engineer_info_class(),
            ],
        )
        eq_(Sally.engineer_info, "mgrdata")

    def test_multi_level_with_base(self):
        Employee, Engineer, Manager = self.classes(
            "Employee", "Engineer", "Manager"
        )
        employees_table, engineers_table, managers_table = self.tables(
            "employees", "engineers", "managers"
        )
        (hackers_table,) = self.tables("hackers")
        (Hacker,) = self.classes("Hacker")

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
            Employee,
            employees_table,
            with_polymorphic=("*", pjoin),
            polymorphic_on=pjoin.c.type,
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
            with_polymorphic=("*", pjoin2),
            polymorphic_on=pjoin2.c.type,
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
        sally = Manager("Sally", "knows how to manage things")
        jenn = Engineer("Jenn", "knows how to program")
        hacker = Hacker("Karina", "Badass", "knows how to hack")
        session.add_all((sally, jenn, hacker))
        session.flush()

        def go():
            eq_(jenn.name, "Jenn")
            eq_(hacker.nickname, "Badass")

        self.assert_sql_count(testing.db, go, 0)
        session.expunge_all()

        # check that we aren't getting a cartesian product in the raw
        # SQL. this requires that Engineer's polymorphic discriminator
        # is not rendered in the statement which is only against
        # Employee's "pjoin"

        assert (
            len(
                session.connection()
                .execute(session.query(Employee).statement)
                .fetchall()
            )
            == 3
        )
        assert {repr(x) for x in session.query(Employee)} == {
            "Engineer Jenn knows how to program",
            "Manager Sally knows how to manage things",
            "Hacker Karina 'Badass' knows how to hack",
        }
        assert {repr(x) for x in session.query(Manager)} == {
            "Manager Sally knows how to manage things"
        }
        assert {repr(x) for x in session.query(Engineer)} == {
            "Engineer Jenn knows how to program",
            "Hacker Karina 'Badass' knows how to hack",
        }
        assert {repr(x) for x in session.query(Hacker)} == {
            "Hacker Karina 'Badass' knows how to hack"
        }

    @testing.fixture
    def two_pjoin_fixture(self):
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

        session = fixture_session(expire_on_commit=False)
        jdoe = Employee("Jdoe")
        sally = Manager("Sally", "knows how to manage things")
        jenn = Engineer("Jenn", "knows how to program")
        hacker = Hacker("Karina", "Badass", "knows how to hack")
        session.add_all((jdoe, sally, jenn, hacker))
        session.commit()

        return (
            session,
            Employee,
            Engineer,
            Manager,
            Hacker,
            pjoin,
            pjoin2,
            jdoe,
            sally,
            jenn,
            hacker,
        )

    def test_without_default_polymorphic_one(self, two_pjoin_fixture):
        (
            session,
            Employee,
            Engineer,
            Manager,
            Hacker,
            pjoin,
            pjoin2,
            jdoe,
            sally,
            jenn,
            hacker,
        ) = two_pjoin_fixture

        wp = with_polymorphic(
            Employee, "*", pjoin, polymorphic_on=pjoin.c.type
        )

        eq_(
            sorted([repr(x) for x in session.query(wp)]),
            [
                "Employee Jdoe",
                "Engineer Jenn knows how to program",
                "Hacker Karina 'Badass' knows how to hack",
                "Manager Sally knows how to manage things",
            ],
        )
        eq_(session.get(Employee, jdoe.employee_id), jdoe)
        eq_(session.get(Engineer, jenn.employee_id), jenn)

    def test_without_default_polymorphic_two(self, two_pjoin_fixture):
        (
            session,
            Employee,
            Engineer,
            Manager,
            Hacker,
            pjoin,
            pjoin2,
            jdoe,
            sally,
            jenn,
            hacker,
        ) = two_pjoin_fixture
        wp = with_polymorphic(
            Employee, "*", pjoin, polymorphic_on=pjoin.c.type
        )

        eq_(
            sorted([repr(x) for x in session.query(wp)]),
            [
                "Employee Jdoe",
                "Engineer Jenn knows how to program",
                "Hacker Karina 'Badass' knows how to hack",
                "Manager Sally knows how to manage things",
            ],
        )

    def test_without_default_polymorphic_three(self, two_pjoin_fixture):
        (
            session,
            Employee,
            Engineer,
            Manager,
            Hacker,
            pjoin,
            pjoin2,
            jdoe,
            sally,
            jenn,
            hacker,
        ) = two_pjoin_fixture
        eq_(
            sorted([repr(x) for x in session.query(Manager)]),
            ["Manager Sally knows how to manage things"],
        )

    def test_without_default_polymorphic_four(self, two_pjoin_fixture):
        (
            session,
            Employee,
            Engineer,
            Manager,
            Hacker,
            pjoin,
            pjoin2,
            jdoe,
            sally,
            jenn,
            hacker,
        ) = two_pjoin_fixture
        wp2 = with_polymorphic(
            Engineer, "*", pjoin2, polymorphic_on=pjoin2.c.type
        )
        eq_(
            sorted([repr(x) for x in session.query(wp2)]),
            [
                "Engineer Jenn knows how to program",
                "Hacker Karina 'Badass' knows how to hack",
            ],
        )

    def test_without_default_polymorphic_five(self, two_pjoin_fixture):
        (
            session,
            Employee,
            Engineer,
            Manager,
            Hacker,
            pjoin,
            pjoin2,
            jdoe,
            sally,
            jenn,
            hacker,
        ) = two_pjoin_fixture
        eq_(
            [repr(x) for x in session.query(Hacker)],
            ["Hacker Karina 'Badass' knows how to hack"],
        )

    def test_without_default_polymorphic_six(self, two_pjoin_fixture):
        (
            session,
            Employee,
            Engineer,
            Manager,
            Hacker,
            pjoin,
            pjoin2,
            jdoe,
            sally,
            jenn,
            hacker,
        ) = two_pjoin_fixture

        # this test is adapting what used to use from_self().
        # it's a weird test but how we would do this would be we would only
        # apply with_polymorprhic once, after we've created whatever
        # subquery we want.

        subq = pjoin2.select().subquery()

        wp2 = with_polymorphic(Engineer, "*", subq, polymorphic_on=subq.c.type)

        eq_(
            sorted([repr(x) for x in session.query(wp2)]),
            [
                "Engineer Jenn knows how to program",
                "Hacker Karina 'Badass' knows how to hack",
            ],
        )

    @testing.combinations(True, False, argnames="use_star")
    def test_without_default_polymorphic_buildit_newstyle(
        self, two_pjoin_fixture, use_star
    ):
        """how would we do these concrete polymorphic queries using 2.0 style,
        and not any old and esoteric features like "polymorphic_union" ?

        """
        (
            session,
            Employee,
            Engineer,
            Manager,
            Hacker,
            pjoin,
            pjoin2,
            jdoe,
            sally,
            jenn,
            hacker,
        ) = two_pjoin_fixture

        # make a union using the entities as given and wpoly from it.
        # a UNION is a UNION.  there is no way around having to write
        # out filler columns.  concrete inh is really not a good choice
        # when you need to select heterogeneously
        stmt = union_all(
            select(
                literal("engineer").label("type"),
                Engineer,
                null().label("nickname"),
            ),
            select(literal("hacker").label("type"), Hacker),
        ).subquery()

        # issue: if we make this with_polymorphic(Engineer, [Hacker], ...),
        # it blows up and tries to add the "engineer" table for unknown reasons

        if use_star:
            wp = with_polymorphic(
                Engineer, "*", stmt, polymorphic_on=stmt.c.type
            )
        else:
            wp = with_polymorphic(
                Engineer, [Engineer, Hacker], stmt, polymorphic_on=stmt.c.type
            )

        result = session.execute(select(wp)).scalars()

        eq_(
            sorted(repr(obj) for obj in result),
            [
                "Engineer Jenn knows how to program",
                "Hacker Karina 'Badass' knows how to hack",
            ],
        )

    def test_relationship(self):
        Employee, Engineer, Manager = self.classes(
            "Employee", "Engineer", "Manager"
        )
        (Company,) = self.classes("Company")
        (companies,) = self.tables("companies")
        engineers_table, managers_table = self.tables("engineers", "managers")

        pjoin = polymorphic_union(
            {"manager": managers_table, "engineer": engineers_table},
            "type",
            "pjoin",
        )
        self.mapper_registry.map_imperatively(
            Company,
            companies,
            properties={"employees": relationship(Employee)},
        )
        employee_mapper = self.mapper_registry.map_imperatively(
            Employee, pjoin, polymorphic_on=pjoin.c.type
        )
        self.mapper_registry.map_imperatively(
            Manager,
            managers_table,
            inherits=employee_mapper,
            concrete=True,
            polymorphic_identity="manager",
        )
        self.mapper_registry.map_imperatively(
            Engineer,
            engineers_table,
            inherits=employee_mapper,
            concrete=True,
            polymorphic_identity="engineer",
        )
        session = fixture_session()
        c = Company()
        c.employees.append(Manager("Sally", "knows how to manage things"))
        c.employees.append(Engineer("Karina", "knows how to hack"))
        session.add(c)
        session.flush()
        session.expunge_all()

        def go():
            c2 = session.get(Company, c.id)
            assert {repr(x) for x in c2.employees} == {
                "Engineer Karina knows how to hack",
                "Manager Sally knows how to manage things",
            }

        self.assert_sql_count(testing.db, go, 2)
        session.expunge_all()

        def go():
            c2 = session.get(
                Company, c.id, options=[joinedload(Company.employees)]
            )
            assert {repr(x) for x in c2.employees} == {
                "Engineer Karina knows how to hack",
                "Manager Sally knows how to manage things",
            }

        self.assert_sql_count(testing.db, go, 1)


class PropertyInheritanceTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("some_dest_id", Integer, ForeignKey("dest_table.id")),
            Column("aname", String(50)),
        )
        Table(
            "b_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("some_dest_id", Integer, ForeignKey("dest_table.id")),
            Column("bname", String(50)),
        )

        Table(
            "c_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("some_dest_id", Integer, ForeignKey("dest_table.id")),
            Column("cname", String(50)),
        )

        Table(
            "dest_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

        class B(A):
            pass

        class C(A):
            pass

        class Dest(cls.Comparable):
            pass

    def test_noninherited_warning(self):
        A, B, b_table, a_table, Dest, dest_table = (
            self.classes.A,
            self.classes.B,
            self.tables.b_table,
            self.tables.a_table,
            self.classes.Dest,
            self.tables.dest_table,
        )

        self.mapper_registry.map_imperatively(
            A, a_table, properties={"some_dest": relationship(Dest)}
        )
        self.mapper_registry.map_imperatively(
            B, b_table, inherits=A, concrete=True
        )
        self.mapper_registry.map_imperatively(Dest, dest_table)
        b = B()
        dest = Dest()
        assert_raises(AttributeError, setattr, b, "some_dest", dest)
        clear_mappers()

        self.mapper_registry.map_imperatively(
            A, a_table, properties={"a_id": a_table.c.id}
        )
        self.mapper_registry.map_imperatively(
            B, b_table, inherits=A, concrete=True
        )
        self.mapper_registry.map_imperatively(Dest, dest_table)
        b = B()
        assert_raises(AttributeError, setattr, b, "a_id", 3)
        clear_mappers()

        self.mapper_registry.map_imperatively(
            A, a_table, properties={"a_id": a_table.c.id}
        )
        self.mapper_registry.map_imperatively(
            B, b_table, inherits=A, concrete=True
        )
        self.mapper_registry.map_imperatively(Dest, dest_table)

    def test_inheriting(self):
        A, B, b_table, a_table, Dest, dest_table = (
            self.classes.A,
            self.classes.B,
            self.tables.b_table,
            self.tables.a_table,
            self.classes.Dest,
            self.tables.dest_table,
        )

        self.mapper_registry.map_imperatively(
            A,
            a_table,
            properties={
                "some_dest": relationship(Dest, back_populates="many_a")
            },
        )
        self.mapper_registry.map_imperatively(
            B,
            b_table,
            inherits=A,
            concrete=True,
            properties={
                "some_dest": relationship(Dest, back_populates="many_b")
            },
        )

        self.mapper_registry.map_imperatively(
            Dest,
            dest_table,
            properties={
                "many_a": relationship(A, back_populates="some_dest"),
                "many_b": relationship(B, back_populates="some_dest"),
            },
        )
        sess = fixture_session()
        dest1 = Dest(name="c1")
        dest2 = Dest(name="c2")
        a1 = A(some_dest=dest1, aname="a1")
        a2 = A(some_dest=dest2, aname="a2")
        b1 = B(some_dest=dest1, bname="b1")
        b2 = B(some_dest=dest1, bname="b2")
        assert_raises(AttributeError, setattr, b1, "aname", "foo")
        assert_raises(AttributeError, getattr, A, "bname")
        assert dest2.many_a == [a2]
        assert dest1.many_a == [a1]
        assert dest1.many_b == [b1, b2]
        sess.add_all([dest1, dest2])
        sess.commit()
        assert sess.query(Dest).filter(Dest.many_a.contains(a2)).one() is dest2
        assert dest2.many_a == [a2]
        assert dest1.many_a == [a1]
        assert dest1.many_b == [b1, b2]
        assert sess.query(B).filter(B.bname == "b1").one() is b1

    def test_overlapping_backref_relationship(self):
        """test #3630.

        was revisited in #4629 (not fixed until 2.0.0rc1 despite the old
        issue number)

        """
        A, B, b_table, a_table, Dest, dest_table = (
            self.classes.A,
            self.classes.B,
            self.tables.b_table,
            self.tables.a_table,
            self.classes.Dest,
            self.tables.dest_table,
        )

        # test issue #3630, no error or warning is generated
        self.mapper_registry.map_imperatively(A, a_table)
        self.mapper_registry.map_imperatively(
            B, b_table, inherits=A, concrete=True
        )
        self.mapper_registry.map_imperatively(
            Dest,
            dest_table,
            properties={
                "a": relationship(A, backref="dest"),
                "a1": relationship(B, backref="dest"),
            },
        )
        configure_mappers()

    def test_overlapping_forwards_relationship(self):
        A, B, b_table, a_table, Dest, dest_table = (
            self.classes.A,
            self.classes.B,
            self.tables.b_table,
            self.tables.a_table,
            self.classes.Dest,
            self.tables.dest_table,
        )

        # this is the opposite mapping as that of #3630, never generated
        # an error / warning
        self.mapper_registry.map_imperatively(
            A, a_table, properties={"dest": relationship(Dest, backref="a")}
        )
        self.mapper_registry.map_imperatively(
            B,
            b_table,
            inherits=A,
            concrete=True,
            properties={"dest": relationship(Dest, backref="a1")},
        )
        self.mapper_registry.map_imperatively(Dest, dest_table)
        configure_mappers()

    def test_polymorphic_backref(self):
        """test multiple backrefs to the same polymorphically-loading
        attribute."""

        A, C, B, c_table, b_table, a_table, Dest, dest_table = (
            self.classes.A,
            self.classes.C,
            self.classes.B,
            self.tables.c_table,
            self.tables.b_table,
            self.tables.a_table,
            self.classes.Dest,
            self.tables.dest_table,
        )

        ajoin = polymorphic_union(
            {"a": a_table, "b": b_table, "c": c_table}, "type", "ajoin"
        )
        self.mapper_registry.map_imperatively(
            A,
            a_table,
            with_polymorphic=("*", ajoin),
            polymorphic_on=ajoin.c.type,
            polymorphic_identity="a",
            properties={
                "some_dest": relationship(Dest, back_populates="many_a")
            },
        )
        self.mapper_registry.map_imperatively(
            B,
            b_table,
            inherits=A,
            concrete=True,
            polymorphic_identity="b",
            properties={
                "some_dest": relationship(Dest, back_populates="many_a")
            },
        )

        self.mapper_registry.map_imperatively(
            C,
            c_table,
            inherits=A,
            concrete=True,
            polymorphic_identity="c",
            properties={
                "some_dest": relationship(Dest, back_populates="many_a")
            },
        )

        self.mapper_registry.map_imperatively(
            Dest,
            dest_table,
            properties={
                "many_a": relationship(
                    A, back_populates="some_dest", order_by=ajoin.c.id
                )
            },
        )

        sess = fixture_session()
        dest1 = Dest(name="c1")
        dest2 = Dest(name="c2")
        a1 = A(some_dest=dest1, aname="a1", id=1)
        a2 = A(some_dest=dest2, aname="a2", id=2)
        b1 = B(some_dest=dest1, bname="b1", id=3)
        b2 = B(some_dest=dest1, bname="b2", id=4)
        c1 = C(some_dest=dest1, cname="c1", id=5)
        c2 = C(some_dest=dest2, cname="c2", id=6)

        eq_([a2, c2], dest2.many_a)
        eq_([a1, b1, b2, c1], dest1.many_a)
        sess.add_all([dest1, dest2])
        sess.commit()

        assert sess.query(Dest).filter(Dest.many_a.contains(a2)).one() is dest2
        assert sess.query(Dest).filter(Dest.many_a.contains(b1)).one() is dest1
        assert sess.query(Dest).filter(Dest.many_a.contains(c2)).one() is dest2

        eq_(dest2.many_a, [a2, c2])
        eq_(dest1.many_a, [a1, b1, b2, c1])
        sess.expire_all()

        def go():
            eq_(
                [
                    Dest(
                        many_a=[
                            A(aname="a1"),
                            B(bname="b1"),
                            B(bname="b2"),
                            C(cname="c1"),
                        ]
                    ),
                    Dest(many_a=[A(aname="a2"), C(cname="c2")]),
                ],
                sess.query(Dest)
                .options(joinedload(Dest.many_a))
                .order_by(Dest.id)
                .all(),
            )

        self.assert_sql_count(testing.db, go, 1)

    def test_merge_w_relationship(self):
        A, C, B, c_table, b_table, a_table, Dest, dest_table = (
            self.classes.A,
            self.classes.C,
            self.classes.B,
            self.tables.c_table,
            self.tables.b_table,
            self.tables.a_table,
            self.classes.Dest,
            self.tables.dest_table,
        )

        ajoin = polymorphic_union(
            {"a": a_table, "b": b_table, "c": c_table}, "type", "ajoin"
        )
        self.mapper_registry.map_imperatively(
            A,
            a_table,
            with_polymorphic=("*", ajoin),
            polymorphic_on=ajoin.c.type,
            polymorphic_identity="a",
            properties={
                "some_dest": relationship(Dest, back_populates="many_a")
            },
        )
        self.mapper_registry.map_imperatively(
            B,
            b_table,
            inherits=A,
            concrete=True,
            polymorphic_identity="b",
            properties={
                "some_dest": relationship(Dest, back_populates="many_a")
            },
        )

        self.mapper_registry.map_imperatively(
            C,
            c_table,
            inherits=A,
            concrete=True,
            polymorphic_identity="c",
            properties={
                "some_dest": relationship(Dest, back_populates="many_a")
            },
        )

        self.mapper_registry.map_imperatively(
            Dest,
            dest_table,
            properties={
                "many_a": relationship(
                    A, back_populates="some_dest", order_by=ajoin.c.id
                )
            },
        )

        assert C.some_dest.property.parent is class_mapper(C)
        assert B.some_dest.property.parent is class_mapper(B)
        assert A.some_dest.property.parent is class_mapper(A)

        sess = fixture_session()
        dest1 = Dest(name="d1")
        dest2 = Dest(name="d2")
        a1 = A(some_dest=dest2, aname="a1")
        b1 = B(some_dest=dest1, bname="b1")
        c1 = C(some_dest=dest2, cname="c1")
        sess.add_all([dest1, dest2, c1, a1, b1])
        sess.commit()

        sess2 = fixture_session()
        merged_c1 = sess2.merge(c1)
        eq_(merged_c1.some_dest.name, "d2")
        eq_(merged_c1.some_dest_id, c1.some_dest_id)


class ManySallyanyTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "base",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )
        Table(
            "sub",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )
        Table(
            "base_mSally",
            metadata,
            Column(
                "base_id", Integer, ForeignKey("base.id"), primary_key=True
            ),
            Column(
                "related_id",
                Integer,
                ForeignKey("related.id"),
                primary_key=True,
            ),
        )
        Table(
            "sub_mSally",
            metadata,
            Column("base_id", Integer, ForeignKey("sub.id"), primary_key=True),
            Column(
                "related_id",
                Integer,
                ForeignKey("related.id"),
                primary_key=True,
            ),
        )
        Table(
            "related",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )

    @classmethod
    def setup_classes(cls):
        class Base(cls.Comparable):
            pass

        class Sub(Base):
            pass

        class Related(cls.Comparable):
            pass

    def test_selective_relationships(self):
        sub, base_mSally, Related, Base, related, sub_mSally, base, Sub = (
            self.tables.sub,
            self.tables.base_mSally,
            self.classes.Related,
            self.classes.Base,
            self.tables.related,
            self.tables.sub_mSally,
            self.tables.base,
            self.classes.Sub,
        )

        self.mapper_registry.map_imperatively(
            Base,
            base,
            properties={
                "related": relationship(
                    Related,
                    secondary=base_mSally,
                    backref="bases",
                    order_by=related.c.id,
                )
            },
        )
        self.mapper_registry.map_imperatively(
            Sub,
            sub,
            inherits=Base,
            concrete=True,
            properties={
                "related": relationship(
                    Related,
                    secondary=sub_mSally,
                    backref="subs",
                    order_by=related.c.id,
                )
            },
        )
        self.mapper_registry.map_imperatively(Related, related)
        sess = fixture_session()
        b1, s1, r1, r2, r3 = Base(), Sub(), Related(), Related(), Related()
        b1.related.append(r1)
        b1.related.append(r2)
        s1.related.append(r2)
        s1.related.append(r3)
        sess.add_all([b1, s1])
        sess.commit()
        eq_(s1.related, [r2, r3])
        eq_(b1.related, [r1, r2])


class ColKeysTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global offices_table, refugees_table
        refugees_table = Table(
            "refugee",
            metadata,
            Column(
                "refugee_fid",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("refugee_name", String(30), key="name"),
        )
        offices_table = Table(
            "office",
            metadata,
            Column(
                "office_fid",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("office_name", String(30), key="name"),
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            refugees_table.insert(),
            [
                dict(refugee_fid=1, name="refugee1"),
                dict(refugee_fid=2, name="refugee2"),
            ],
        )
        connection.execute(
            offices_table.insert(),
            [
                dict(office_fid=1, name="office1"),
                dict(office_fid=2, name="office2"),
            ],
        )

    def test_keys(self):
        pjoin = polymorphic_union(
            {"refugee": refugees_table, "office": offices_table},
            "type",
            "pjoin",
        )

        class Location:
            pass

        class Refugee(Location):
            pass

        class Office(Location):
            pass

        location_mapper = self.mapper_registry.map_imperatively(
            Location,
            pjoin,
            polymorphic_on=pjoin.c.type,
            polymorphic_identity="location",
        )
        self.mapper_registry.map_imperatively(
            Office,
            offices_table,
            inherits=location_mapper,
            concrete=True,
            polymorphic_identity="office",
        )
        self.mapper_registry.map_imperatively(
            Refugee,
            refugees_table,
            inherits=location_mapper,
            concrete=True,
            polymorphic_identity="refugee",
        )
        sess = fixture_session()
        eq_(sess.get(Refugee, 1).name, "refugee1")
        eq_(sess.get(Refugee, 2).name, "refugee2")
        eq_(sess.get(Office, 1).name, "office1")
        eq_(sess.get(Office, 2).name, "office2")


class AdaptOnNamesTest(
    RemoveORMEventsGlobally, fixtures.DeclarativeMappedTest
):
    """test the full integration case for #7805"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic
        Basic = cls.Basic

        class Metadata(ComparableEntity, Base):
            __tablename__ = "metadata"
            id = Column(
                Integer,
                primary_key=True,
            )

            some_data = Column(String(50))

        class BaseObj(ComparableEntity, AbstractConcreteBase, Base):
            """abstract concrete base with a custom polymorphic_union.

            Additionally, at query time it needs to use a new version of this
            union each time in order to add filter criteria.  this is because
            polymorphic_union() is of course very inefficient in its form
            and if someone actually has to use this, it's likely better for
            filter criteria to be within each sub-select.   The current use
            case here does not really have easy answers as we don't have
            a built-in widget that does this.  The complexity / little use
            ratio doesn't justify it unfortunately.

            This use case might be easier if we were mapped to something that
            can be adapted. however, we are using adapt_on_names here as this
            is usually what's more accessible to someone trying to get into
            this, or at least we should make that feature work as well as it
            can.

            """

            strict_attrs = True

            @declared_attr
            def id(cls):
                return Column(Integer, primary_key=True)

            @declared_attr
            def metadata_id(cls):
                return Column(ForeignKey(Metadata.id), nullable=False)

            @classmethod
            def _create_polymorphic_union(cls, mappers, discriminator_name):
                return cls.make_statement().subquery()

            @declared_attr
            def related_metadata(cls):
                return relationship(Metadata)

            @classmethod
            def make_statement(cls, *filter_cond, include_metadata=False):
                a_stmt = (
                    select(
                        A.id,
                        A.metadata_id,
                        A.thing1,
                        A.x1,
                        A.y1,
                        null().label("thing2"),
                        null().label("x2"),
                        null().label("y2"),
                        literal("a").label("type"),
                    )
                    .join(Metadata)
                    .filter(*filter_cond)
                )
                if include_metadata:
                    a_stmt = a_stmt.add_columns(Metadata.__table__)

                b_stmt = (
                    select(
                        B.id,
                        B.metadata_id,
                        null().label("thing1"),
                        null().label("x1"),
                        null().label("y1"),
                        B.thing2,
                        B.x2,
                        B.y2,
                        literal("b").label("type"),
                    )
                    .join(Metadata)
                    .filter(*filter_cond)
                )
                if include_metadata:
                    b_stmt = b_stmt.add_columns(Metadata.__table__)

                return union(a_stmt, b_stmt)

        class XYThing(Basic):
            def __init__(self, x, y):
                self.x = x
                self.y = y

            def __composite_values__(self):
                return (self.x, self.y)

            def __eq__(self, other):
                return (
                    isinstance(other, XYThing)
                    and other.x == self.x
                    and other.y == self.y
                )

            def __ne__(self, other):
                return not self.__eq__(other)

        class A(BaseObj):
            __tablename__ = "a"
            thing1 = Column(String(50))
            comp1 = composite(
                XYThing, Column("x1", Integer), Column("y1", Integer)
            )

            __mapper_args__ = {"polymorphic_identity": "a", "concrete": True}

        class B(BaseObj):
            __tablename__ = "b"
            thing2 = Column(String(50))
            comp2 = composite(
                XYThing, Column("x2", Integer), Column("y2", Integer)
            )

            __mapper_args__ = {"polymorphic_identity": "b", "concrete": True}

    @classmethod
    def insert_data(cls, connection):
        Metadata, A, B = cls.classes("Metadata", "A", "B")
        XYThing = cls.classes.XYThing

        with Session(connection) as sess:
            sess.add_all(
                [
                    Metadata(id=1, some_data="m1"),
                    Metadata(id=2, some_data="m2"),
                ]
            )
            sess.flush()

            sess.add_all(
                [
                    A(
                        id=5,
                        metadata_id=1,
                        thing1="thing1",
                        comp1=XYThing(1, 2),
                    ),
                    B(
                        id=6,
                        metadata_id=2,
                        thing2="thing2",
                        comp2=XYThing(3, 4),
                    ),
                ]
            )
            sess.commit()

    def test_contains_eager(self):
        Metadata, A, B = self.classes("Metadata", "A", "B")
        BaseObj = self.classes.BaseObj
        XYThing = self.classes.XYThing

        alias = BaseObj.make_statement(
            Metadata.id < 3, include_metadata=True
        ).subquery()
        ac = with_polymorphic(
            BaseObj,
            [A, B],
            selectable=alias,
            adapt_on_names=True,
        )

        mt = aliased(Metadata, alias=alias)

        sess = fixture_session()

        with self.sql_execution_asserter() as asserter:
            objects = sess.scalars(
                select(ac)
                .options(
                    contains_eager(ac.A.related_metadata.of_type(mt)),
                    contains_eager(ac.B.related_metadata.of_type(mt)),
                )
                .order_by(ac.id)
            ).all()

            eq_(
                objects,
                [
                    A(
                        id=5,
                        metadata_id=1,
                        thing1="thing1",
                        comp1=XYThing(1, 2),
                        related_metadata=Metadata(id=1, some_data="m1"),
                    ),
                    B(
                        id=6,
                        metadata_id=2,
                        thing2="thing2",
                        comp2=XYThing(3, 4),
                        related_metadata=Metadata(id=2, some_data="m2"),
                    ),
                ],
            )
        asserter.assert_(
            CompiledSQL(
                "SELECT anon_1.id, anon_1.metadata_id, anon_1.type, "
                "anon_1.id_1, anon_1.some_data, anon_1.thing1, "
                "anon_1.x1, anon_1.y1, anon_1.thing2, anon_1.x2, anon_1.y2 "
                "FROM "
                "(SELECT a.id AS id, a.metadata_id AS metadata_id, "
                "a.thing1 AS thing1, a.x1 AS x1, a.y1 AS y1, "
                "NULL AS thing2, NULL AS x2, NULL AS y2, :param_1 AS type, "
                "metadata.id AS id_1, metadata.some_data AS some_data "
                "FROM a JOIN metadata ON metadata.id = a.metadata_id "
                "WHERE metadata.id < :id_2 UNION SELECT b.id AS id, "
                "b.metadata_id AS metadata_id, NULL AS thing1, NULL AS x1, "
                "NULL AS y1, b.thing2 AS thing2, b.x2 AS x2, b.y2 AS y2, "
                ":param_2 AS type, metadata.id AS id_1, "
                "metadata.some_data AS some_data FROM b "
                "JOIN metadata ON metadata.id = b.metadata_id "
                "WHERE metadata.id < :id_3) AS anon_1 ORDER BY anon_1.id",
                # tip: whether or not there is "id_2" and "id_3" here,
                # or just "id_2", is based on whether or not the two
                # queries had polymorphic adaption proceed, so that the
                # two filter criteria are different vs. the same object.  see
                # mapper._should_select_with_poly_adapter added in #8456.
                [{"param_1": "a", "id_2": 3, "param_2": "b", "id_3": 3}],
            )
        )
