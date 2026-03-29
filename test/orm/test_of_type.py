from sqlalchemy import and_
from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import aliased
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from .inheritance._poly_fixtures import _PolymorphicAliasedJoins
from .inheritance._poly_fixtures import _PolymorphicJoins
from .inheritance._poly_fixtures import _PolymorphicPolymorphic
from .inheritance._poly_fixtures import _PolymorphicUnions
from .inheritance._poly_fixtures import Boss
from .inheritance._poly_fixtures import Company
from .inheritance._poly_fixtures import Engineer
from .inheritance._poly_fixtures import Machine
from .inheritance._poly_fixtures import Manager
from .inheritance._poly_fixtures import Person


class _PolymorphicTestBase:
    __dialect__ = "default"

    def test_any_one(self):
        sess = fixture_session()
        any_ = Company.employees.of_type(Engineer).any(
            Engineer.primary_language == "cobol"
        )
        eq_(sess.query(Company).filter(any_).one(), self.c2)

    def test_any_two(self):
        sess = fixture_session()
        calias = aliased(Company)
        any_ = calias.employees.of_type(Engineer).any(
            Engineer.primary_language == "cobol"
        )
        eq_(sess.query(calias).filter(any_).one(), self.c2)

    def test_any_three(self):
        sess = fixture_session()
        any_ = Company.employees.of_type(Boss).any(Boss.golf_swing == "fore")
        eq_(sess.query(Company).filter(any_).one(), self.c1)

    def test_any_four(self):
        sess = fixture_session()
        any_ = Company.employees.of_type(Manager).any(
            Manager.manager_name == "pointy"
        )
        eq_(sess.query(Company).filter(any_).one(), self.c1)

    def test_any_five(self):
        sess = fixture_session()
        any_ = Company.employees.of_type(Engineer).any(
            and_(Engineer.primary_language == "cobol")
        )
        eq_(sess.query(Company).filter(any_).one(), self.c2)

    def test_join_to_subclass_one(self):
        sess = fixture_session()
        eq_(
            sess.query(Company)
            .join(Company.employees.of_type(Engineer))
            .filter(Engineer.primary_language == "java")
            .all(),
            [self.c1],
        )

    def test_join_to_subclass_two(self):
        sess = fixture_session()
        eq_(
            sess.query(Company)
            .join(Company.employees.of_type(Engineer))
            .join(Engineer.machines)
            .filter(Machine.name.ilike("%thinkpad%"))
            .all(),
            [self.c1],
        )

    def test_join_to_subclass_three(self):
        sess = fixture_session()
        eq_(
            sess.query(Company, Engineer)
            .join(Company.employees.of_type(Engineer))
            .filter(Engineer.primary_language == "java")
            .count(),
            1,
        )

    def test_join_to_subclass_four(self):
        sess = fixture_session()
        # test [ticket:2093]
        eq_(
            sess.query(Company.company_id, Engineer)
            .join(Company.employees.of_type(Engineer))
            .filter(Engineer.primary_language == "java")
            .count(),
            1,
        )

    def test_join_to_subclass_five(self):
        sess = fixture_session()
        eq_(
            sess.query(Company)
            .join(Company.employees.of_type(Engineer))
            .filter(Engineer.primary_language == "java")
            .count(),
            1,
        )

    def test_with_polymorphic_join_compile_one(self):
        sess = fixture_session()

        self.assert_compile(
            sess.query(Company).join(
                Company.employees.of_type(
                    with_polymorphic(
                        Person, [Engineer, Manager], aliased=True, flat=True
                    )
                )
            ),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name FROM companies "
            "JOIN %s" % (self._polymorphic_join_target([Engineer, Manager])),
        )

    def test_with_polymorphic_join_exec_contains_eager_one(self):
        sess = fixture_session()

        def go():
            wp = with_polymorphic(
                Person, [Engineer, Manager], aliased=True, flat=True
            )
            eq_(
                sess.query(Company)
                .join(Company.employees.of_type(wp))
                .order_by(Company.company_id, wp.person_id)
                .options(contains_eager(Company.employees.of_type(wp)))
                .all(),
                [self.c1, self.c2],
            )

        self.assert_sql_count(testing.db, go, 1)

    @testing.combinations(
        # this form is not expected to work in all cases, ultimately
        # the "alias" parameter should be deprecated entirely
        # lambda Company, wp: contains_eager(Company.employees, alias=wp),
        lambda Company, wp: contains_eager(Company.employees.of_type(wp)),
        lambda Company, wp: contains_eager(
            Company.employees.of_type(wp), alias=wp
        ),
    )
    def test_with_polymorphic_join_exec_contains_eager_two(
        self, contains_eager_option
    ):
        sess = fixture_session()

        wp = with_polymorphic(Person, [Engineer, Manager], aliased=True)
        contains_eager_option = testing.resolve_lambda(
            contains_eager_option, Company=Company, wp=wp
        )
        q = (
            sess.query(Company)
            .join(Company.employees.of_type(wp))
            .order_by(Company.company_id, wp.person_id)
            .options(contains_eager_option)
        )

        def go():
            eq_(q.all(), [self.c1, self.c2])

        self.assert_sql_count(testing.db, go, 1)

        self.assert_compile(
            q,
            self._test_with_polymorphic_join_exec_contains_eager_two_result(),
        )

    def test_with_polymorphic_any(self):
        sess = fixture_session()
        wp = with_polymorphic(Person, [Engineer], aliased=True)
        eq_(
            sess.query(Company.company_id)
            .filter(
                Company.employees.of_type(wp).any(
                    wp.Engineer.primary_language == "java"
                )
            )
            .all(),
            [(1,)],
        )

    def test_subqueryload_implicit_withpoly(self):
        sess = fixture_session()

        def go():
            eq_(
                sess.query(Company)
                .filter_by(company_id=1)
                .options(subqueryload(Company.employees.of_type(Engineer)))
                .all(),
                [self._company_with_emps_fixture()[0]],
            )

        self.assert_sql_count(testing.db, go, 4)

    def test_joinedload_implicit_withpoly(self):
        sess = fixture_session()

        def go():
            eq_(
                sess.query(Company)
                .filter_by(company_id=1)
                .options(joinedload(Company.employees.of_type(Engineer)))
                .all(),
                [self._company_with_emps_fixture()[0]],
            )

        self.assert_sql_count(testing.db, go, 3)

    def test_subqueryload_explicit_withpoly(self):
        sess = fixture_session()

        def go():
            target = with_polymorphic(Person, Engineer)
            eq_(
                sess.query(Company)
                .filter_by(company_id=1)
                .options(subqueryload(Company.employees.of_type(target)))
                .all(),
                [self._company_with_emps_fixture()[0]],
            )

        self.assert_sql_count(testing.db, go, 4)

    def test_joinedload_explicit_withpoly(self):
        sess = fixture_session()

        def go():
            target = with_polymorphic(Person, Engineer, flat=True)
            eq_(
                sess.query(Company)
                .filter_by(company_id=1)
                .options(joinedload(Company.employees.of_type(target)))
                .all(),
                [self._company_with_emps_fixture()[0]],
            )

        self.assert_sql_count(testing.db, go, 3)

    def test_joinedload_stacked_of_type(self):
        sess = fixture_session()

        def go():
            eq_(
                sess.query(Company)
                .filter_by(company_id=1)
                .options(
                    joinedload(Company.employees.of_type(Manager)),
                    joinedload(Company.employees.of_type(Engineer)),
                )
                .all(),
                [self._company_with_emps_fixture()[0]],
            )

        self.assert_sql_count(testing.db, go, 2)


class PolymorphicPolymorphicTest(
    _PolymorphicTestBase, _PolymorphicPolymorphic
):
    def _polymorphic_join_target(self, cls):
        return (
            "(people AS people_1 LEFT OUTER JOIN engineers AS engineers_1 "
            "ON people_1.person_id = engineers_1.person_id "
            "LEFT OUTER JOIN managers AS managers_1 "
            "ON people_1.person_id = managers_1.person_id) "
            "ON companies.company_id = people_1.company_id"
        )

    def _test_with_polymorphic_join_exec_contains_eager_two_result(self):
        return (
            "SELECT anon_1.people_person_id AS anon_1_people_person_id, "
            "anon_1.people_company_id AS anon_1_people_company_id, "
            "anon_1.people_name AS anon_1_people_name, "
            "anon_1.people_type AS anon_1_people_type, "
            "anon_1.engineers_person_id AS anon_1_engineers_person_id, "
            "anon_1.engineers_status AS anon_1_engineers_status, "
            "anon_1.engineers_engineer_name "
            "AS anon_1_engineers_engineer_name, "
            "anon_1.engineers_primary_language "
            "AS anon_1_engineers_primary_language, anon_1.managers_person_id "
            "AS anon_1_managers_person_id, anon_1.managers_status "
            "AS anon_1_managers_status, anon_1.managers_manager_name "
            "AS anon_1_managers_manager_name, companies.company_id "
            "AS companies_company_id, companies.name AS companies_name "
            "FROM companies JOIN (SELECT people.person_id AS "
            "people_person_id, people.company_id AS people_company_id, "
            "people.name AS people_name, people.type AS people_type, "
            "engineers.person_id AS engineers_person_id, engineers.status "
            "AS engineers_status, engineers.engineer_name "
            "AS engineers_engineer_name, engineers.primary_language "
            "AS engineers_primary_language, managers.person_id "
            "AS managers_person_id, managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name FROM people "
            "LEFT OUTER JOIN engineers ON people.person_id = "
            "engineers.person_id LEFT OUTER JOIN managers "
            "ON people.person_id = managers.person_id) AS anon_1 "
            "ON companies.company_id = anon_1.people_company_id "
            "ORDER BY companies.company_id, anon_1.people_person_id"
        )


class PolymorphicUnionsTest(_PolymorphicTestBase, _PolymorphicUnions):
    def _polymorphic_join_target(self, cls):
        return (
            "(SELECT engineers.person_id AS person_id, people.company_id "
            "AS company_id, people.name AS name, people.type AS type, "
            "engineers.status AS status, "
            "engineers.engineer_name AS engineer_name, "
            "engineers.primary_language AS primary_language, "
            "CAST(NULL AS VARCHAR(50)) AS manager_name FROM people "
            "JOIN engineers ON people.person_id = engineers.person_id "
            "UNION ALL SELECT managers.person_id AS person_id, "
            "people.company_id AS company_id, people.name AS name, "
            "people.type AS type, managers.status AS status, "
            "CAST(NULL AS VARCHAR(50)) AS engineer_name, "
            "CAST(NULL AS VARCHAR(50)) AS primary_language, "
            "managers.manager_name AS manager_name FROM people "
            "JOIN managers ON people.person_id = managers.person_id) "
            "AS pjoin_1 ON companies.company_id = pjoin_1.company_id"
        )

    def _test_with_polymorphic_join_exec_contains_eager_two_result(self):
        return (
            "SELECT pjoin_1.person_id AS pjoin_1_person_id, "
            "pjoin_1.company_id AS pjoin_1_company_id, pjoin_1.name AS "
            "pjoin_1_name, pjoin_1.type AS pjoin_1_type, pjoin_1.status "
            "AS pjoin_1_status, pjoin_1.engineer_name AS "
            "pjoin_1_engineer_name, pjoin_1.primary_language AS "
            "pjoin_1_primary_language, pjoin_1.manager_name AS "
            "pjoin_1_manager_name, companies.company_id AS "
            "companies_company_id, companies.name AS companies_name "
            "FROM companies JOIN (SELECT engineers.person_id AS "
            "person_id, people.company_id AS company_id, people.name AS name, "
            "people.type AS type, engineers.status AS status, "
            "engineers.engineer_name AS engineer_name, "
            "engineers.primary_language AS primary_language, "
            "CAST(NULL AS VARCHAR(50)) AS manager_name FROM people "
            "JOIN engineers ON people.person_id = engineers.person_id "
            "UNION ALL SELECT managers.person_id AS person_id, "
            "people.company_id AS company_id, people.name AS name, "
            "people.type AS type, managers.status AS status, "
            "CAST(NULL AS VARCHAR(50)) AS engineer_name, "
            "CAST(NULL AS VARCHAR(50)) AS primary_language, "
            "managers.manager_name AS manager_name FROM people "
            "JOIN managers ON people.person_id = managers.person_id) AS "
            "pjoin_1 ON companies.company_id = pjoin_1.company_id "
            "ORDER BY companies.company_id, pjoin_1.person_id"
        )


class PolymorphicAliasedJoinsTest(
    _PolymorphicTestBase, _PolymorphicAliasedJoins
):
    def _polymorphic_join_target(self, cls):
        return (
            "(SELECT people.person_id AS people_person_id, "
            "people.company_id AS people_company_id, "
            "people.name AS people_name, people.type AS people_type, "
            "engineers.person_id AS engineers_person_id, "
            "engineers.status AS engineers_status, "
            "engineers.engineer_name AS engineers_engineer_name, "
            "engineers.primary_language AS engineers_primary_language, "
            "managers.person_id AS managers_person_id, "
            "managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name "
            "FROM people LEFT OUTER JOIN engineers "
            "ON people.person_id = engineers.person_id "
            "LEFT OUTER JOIN managers "
            "ON people.person_id = managers.person_id) AS pjoin_1 "
            "ON companies.company_id = pjoin_1.people_company_id"
        )

    def _test_with_polymorphic_join_exec_contains_eager_two_result(self):
        return (
            "SELECT pjoin_1.people_person_id AS pjoin_1_people_person_id, "
            "pjoin_1.people_company_id AS pjoin_1_people_company_id, "
            "pjoin_1.people_name AS pjoin_1_people_name, pjoin_1.people_type "
            "AS pjoin_1_people_type, pjoin_1.engineers_person_id AS "
            "pjoin_1_engineers_person_id, pjoin_1.engineers_status AS "
            "pjoin_1_engineers_status, pjoin_1.engineers_engineer_name "
            "AS pjoin_1_engineers_engineer_name, "
            "pjoin_1.engineers_primary_language AS "
            "pjoin_1_engineers_primary_language, pjoin_1.managers_person_id "
            "AS pjoin_1_managers_person_id, pjoin_1.managers_status "
            "AS pjoin_1_managers_status, pjoin_1.managers_manager_name "
            "AS pjoin_1_managers_manager_name, companies.company_id "
            "AS companies_company_id, companies.name AS companies_name "
            "FROM companies JOIN (SELECT people.person_id AS "
            "people_person_id, people.company_id AS people_company_id, "
            "people.name AS people_name, people.type AS people_type, "
            "engineers.person_id AS engineers_person_id, engineers.status "
            "AS engineers_status, engineers.engineer_name AS "
            "engineers_engineer_name, engineers.primary_language "
            "AS engineers_primary_language, managers.person_id AS "
            "managers_person_id, managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name FROM people "
            "LEFT OUTER JOIN engineers ON people.person_id = "
            "engineers.person_id LEFT OUTER JOIN managers "
            "ON people.person_id = managers.person_id) AS pjoin_1 "
            "ON companies.company_id = pjoin_1.people_company_id "
            "ORDER BY companies.company_id, pjoin_1.people_person_id"
        )


class PolymorphicJoinsTest(_PolymorphicTestBase, _PolymorphicJoins):
    def _polymorphic_join_target(self, cls):
        return (
            "(people AS people_1 LEFT OUTER JOIN engineers "
            "AS engineers_1 ON people_1.person_id = engineers_1.person_id "
            "LEFT OUTER JOIN managers AS managers_1 "
            "ON people_1.person_id = managers_1.person_id) "
            "ON companies.company_id = people_1.company_id"
        )

    def _test_with_polymorphic_join_exec_contains_eager_two_result(self):
        return (
            "SELECT anon_1.people_person_id AS anon_1_people_person_id, "
            "anon_1.people_company_id AS anon_1_people_company_id, "
            "anon_1.people_name AS anon_1_people_name, "
            "anon_1.people_type AS anon_1_people_type, "
            "anon_1.engineers_person_id AS anon_1_engineers_person_id, "
            "anon_1.engineers_status AS anon_1_engineers_status, "
            "anon_1.engineers_engineer_name "
            "AS anon_1_engineers_engineer_name, "
            "anon_1.engineers_primary_language "
            "AS anon_1_engineers_primary_language, anon_1.managers_person_id "
            "AS anon_1_managers_person_id, anon_1.managers_status "
            "AS anon_1_managers_status, anon_1.managers_manager_name "
            "AS anon_1_managers_manager_name, companies.company_id "
            "AS companies_company_id, companies.name AS companies_name "
            "FROM companies JOIN (SELECT people.person_id AS "
            "people_person_id, people.company_id AS people_company_id, "
            "people.name AS people_name, people.type AS people_type, "
            "engineers.person_id AS engineers_person_id, engineers.status "
            "AS engineers_status, engineers.engineer_name "
            "AS engineers_engineer_name, engineers.primary_language "
            "AS engineers_primary_language, managers.person_id "
            "AS managers_person_id, managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name FROM people "
            "LEFT OUTER JOIN engineers ON people.person_id = "
            "engineers.person_id LEFT OUTER JOIN managers "
            "ON people.person_id = managers.person_id) AS anon_1 "
            "ON companies.company_id = anon_1.people_company_id "
            "ORDER BY companies.company_id, anon_1.people_person_id"
        )

    def test_joinedload_explicit_with_unaliased_poly_compile(self):
        sess = fixture_session()
        target = with_polymorphic(Person, Engineer)
        q = (
            sess.query(Company)
            .filter_by(company_id=1)
            .options(joinedload(Company.employees.of_type(target)))
        )
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Detected unaliased columns when generating joined load.",
            q._compile_context,
        )

    def test_joinedload_explicit_with_flataliased_poly_compile(self):
        sess = fixture_session()
        target = with_polymorphic(Person, Engineer, flat=True)
        q = (
            sess.query(Company)
            .filter_by(company_id=1)
            .options(joinedload(Company.employees.of_type(target)))
        )
        self.assert_compile(
            q,
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name, "
            "people_1.person_id AS people_1_person_id, "
            "people_1.company_id AS people_1_company_id, "
            "people_1.name AS people_1_name, people_1.type AS people_1_type, "
            "engineers_1.person_id AS engineers_1_person_id, "
            "engineers_1.status AS engineers_1_status, "
            "engineers_1.engineer_name AS engineers_1_engineer_name, "
            "engineers_1.primary_language AS engineers_1_primary_language "
            "FROM companies LEFT OUTER JOIN (people AS people_1 "
            "LEFT OUTER JOIN engineers AS engineers_1 "
            "ON people_1.person_id = engineers_1.person_id "
            "LEFT OUTER JOIN managers AS managers_1 "
            "ON people_1.person_id = managers_1.person_id) "
            "ON companies.company_id = people_1.company_id "
            "WHERE companies.company_id = :company_id_1 "
            "ORDER BY people_1.person_id",
        )


class SubclassRelationshipTest(
    testing.AssertsCompiledSQL, fixtures.DeclarativeMappedTest
):
    """There's overlap here vs. the ones above."""

    run_setup_classes = "once"
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Job(ComparableEntity, Base):
            __tablename__ = "job"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            type = Column(String(10))
            widget_id = Column(ForeignKey("widget.id"))
            widget = relationship("Widget")
            container_id = Column(Integer, ForeignKey("data_container.id"))
            __mapper_args__ = {"polymorphic_on": type}

        class SubJob(Job):
            __tablename__ = "subjob"
            id = Column(Integer, ForeignKey("job.id"), primary_key=True)
            attr = Column(String(10))
            __mapper_args__ = {"polymorphic_identity": "sub"}

        class ParentThing(ComparableEntity, Base):
            __tablename__ = "parent"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            container_id = Column(Integer, ForeignKey("data_container.id"))
            container = relationship("DataContainer")

        class DataContainer(ComparableEntity, Base):
            __tablename__ = "data_container"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(10))
            jobs = relationship(Job, order_by=Job.id)

        class Widget(ComparableEntity, Base):
            __tablename__ = "widget"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(10))

    @classmethod
    def insert_data(cls, connection):
        s = Session(connection)

        s.add_all(cls._fixture())
        s.commit()

    @classmethod
    def _fixture(cls):
        ParentThing, DataContainer, SubJob, Widget = (
            cls.classes.ParentThing,
            cls.classes.DataContainer,
            cls.classes.SubJob,
            cls.classes.Widget,
        )
        return [
            ParentThing(
                container=DataContainer(
                    name="d1",
                    jobs=[
                        SubJob(attr="s1", widget=Widget(name="w1")),
                        SubJob(attr="s2", widget=Widget(name="w2")),
                    ],
                )
            ),
            ParentThing(
                container=DataContainer(
                    name="d2",
                    jobs=[
                        SubJob(attr="s3", widget=Widget(name="w3")),
                        SubJob(attr="s4", widget=Widget(name="w4")),
                    ],
                )
            ),
        ]

    @classmethod
    def _dc_fixture(cls):
        return [p.container for p in cls._fixture()]

    def test_contains_eager_wpoly(self):
        DataContainer, Job, SubJob = (
            self.classes.DataContainer,
            self.classes.Job,
            self.classes.SubJob,
        )

        Job_P = with_polymorphic(Job, SubJob, aliased=True)

        s = Session(testing.db)
        q = (
            s.query(DataContainer)
            .join(DataContainer.jobs.of_type(Job_P))
            .options(contains_eager(DataContainer.jobs.of_type(Job_P)))
        )

        def go():
            eq_(q.all(), self._dc_fixture())

        self.assert_sql_count(testing.db, go, 5)

    def test_joinedload_wpoly(self):
        DataContainer, Job, SubJob = (
            self.classes.DataContainer,
            self.classes.Job,
            self.classes.SubJob,
        )

        Job_P = with_polymorphic(Job, SubJob, aliased=True)

        s = Session(testing.db)
        q = s.query(DataContainer).options(
            joinedload(DataContainer.jobs.of_type(Job_P))
        )

        def go():
            eq_(q.all(), self._dc_fixture())

        self.assert_sql_count(testing.db, go, 5)

    def test_joinedload_wsubclass(self):
        DataContainer, SubJob = (
            self.classes.DataContainer,
            self.classes.SubJob,
        )
        s = Session(testing.db)
        q = s.query(DataContainer).options(
            joinedload(DataContainer.jobs.of_type(SubJob))
        )

        def go():
            eq_(q.all(), self._dc_fixture())

        self.assert_sql_count(testing.db, go, 5)

    def test_lazyload(self):
        DataContainer = self.classes.DataContainer
        s = Session(testing.db)
        q = s.query(DataContainer)

        def go():
            eq_(q.all(), self._dc_fixture())

        # SELECT data container
        # SELECT job * 2 container rows
        # SELECT subjob * 4 rows
        # SELECT widget * 4 rows
        self.assert_sql_count(testing.db, go, 11)

    def test_subquery_wsubclass(self):
        DataContainer, SubJob = (
            self.classes.DataContainer,
            self.classes.SubJob,
        )
        s = Session(testing.db)
        q = s.query(DataContainer).options(
            subqueryload(DataContainer.jobs.of_type(SubJob))
        )

        def go():
            eq_(q.all(), self._dc_fixture())

        self.assert_sql_count(testing.db, go, 6)

    def test_twolevel_subqueryload_wsubclass(self):
        ParentThing, DataContainer, SubJob = (
            self.classes.ParentThing,
            self.classes.DataContainer,
            self.classes.SubJob,
        )
        s = Session(testing.db)
        q = s.query(ParentThing).options(
            subqueryload(ParentThing.container).subqueryload(
                DataContainer.jobs.of_type(SubJob)
            )
        )

        def go():
            eq_(q.all(), self._fixture())

        self.assert_sql_count(testing.db, go, 7)

    def test_twolevel_subqueryload_wsubclass_mapper_term(self):
        DataContainer, SubJob = self.classes.DataContainer, self.classes.SubJob
        s = Session(testing.db)
        sj_alias = aliased(SubJob)
        q = s.query(DataContainer).options(
            subqueryload(DataContainer.jobs.of_type(sj_alias)).subqueryload(
                sj_alias.widget
            )
        )

        def go():
            eq_(q.all(), self._dc_fixture())

        self.assert_sql_count(testing.db, go, 3)

    def test_twolevel_joinedload_wsubclass(self):
        ParentThing, DataContainer, SubJob = (
            self.classes.ParentThing,
            self.classes.DataContainer,
            self.classes.SubJob,
        )
        s = Session(testing.db)
        q = s.query(ParentThing).options(
            joinedload(ParentThing.container).joinedload(
                DataContainer.jobs.of_type(SubJob)
            )
        )

        def go():
            eq_(q.all(), self._fixture())

        self.assert_sql_count(testing.db, go, 5)

    def test_any_wpoly(self):
        DataContainer, Job, SubJob = (
            self.classes.DataContainer,
            self.classes.Job,
            self.classes.SubJob,
        )

        Job_P = with_polymorphic(Job, SubJob, aliased=True, flat=True)

        s = fixture_session()
        q = (
            s.query(Job)
            .join(DataContainer.jobs)
            .filter(DataContainer.jobs.of_type(Job_P).any(Job_P.id < Job.id))
        )

        self.assert_compile(
            q,
            "SELECT job.id AS job_id, job.type AS job_type, "
            "job.widget_id AS job_widget_id, "
            "job.container_id "
            "AS job_container_id "
            "FROM data_container "
            "JOIN job ON data_container.id = job.container_id "
            "WHERE EXISTS (SELECT 1 "
            "FROM job AS job_1 LEFT OUTER JOIN subjob AS subjob_1 "
            "ON job_1.id = subjob_1.id "
            "WHERE data_container.id = job_1.container_id "
            "AND job_1.id < job.id)",
        )

    def test_any_walias(self):
        (
            DataContainer,
            Job,
        ) = (self.classes.DataContainer, self.classes.Job)

        Job_A = aliased(Job)

        s = fixture_session()
        q = (
            s.query(Job)
            .join(DataContainer.jobs)
            .filter(
                DataContainer.jobs.of_type(Job_A).any(
                    and_(Job_A.id < Job.id, Job_A.type == "fred")
                )
            )
        )
        self.assert_compile(
            q,
            "SELECT job.id AS job_id, job.type AS job_type, "
            "job.widget_id AS job_widget_id, "
            "job.container_id AS job_container_id "
            "FROM data_container JOIN job "
            "ON data_container.id = job.container_id "
            "WHERE EXISTS (SELECT 1 "
            "FROM job AS job_1 "
            "WHERE data_container.id = job_1.container_id "
            "AND job_1.id < job.id AND job_1.type = :type_1)",
        )

    def test_join_wpoly(self):
        DataContainer, Job, SubJob = (
            self.classes.DataContainer,
            self.classes.Job,
            self.classes.SubJob,
        )

        Job_P = with_polymorphic(Job, SubJob)

        s = fixture_session()
        q = s.query(DataContainer).join(DataContainer.jobs.of_type(Job_P))
        self.assert_compile(
            q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN "
            "(job LEFT OUTER JOIN subjob "
            "ON job.id = subjob.id) "
            "ON data_container.id = job.container_id",
        )

    def test_join_wsubclass(self):
        DataContainer, SubJob = (
            self.classes.DataContainer,
            self.classes.SubJob,
        )

        s = fixture_session()
        q = s.query(DataContainer).join(DataContainer.jobs.of_type(SubJob))
        # note the of_type() here renders JOIN for the Job->SubJob.
        # this is because it's using the SubJob mapper directly within
        # query.join().  When we do joinedload() etc., we're instead
        # doing a with_polymorphic(), and there we need the join to be
        # outer by default.
        self.assert_compile(
            q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN (job JOIN subjob ON job.id = subjob.id) "
            "ON data_container.id = job.container_id",
        )

    def test_join_wpoly_innerjoin(self):
        DataContainer, Job, SubJob = (
            self.classes.DataContainer,
            self.classes.Job,
            self.classes.SubJob,
        )

        Job_P = with_polymorphic(Job, SubJob, innerjoin=True)

        s = fixture_session()
        q = s.query(DataContainer).join(DataContainer.jobs.of_type(Job_P))
        self.assert_compile(
            q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN "
            "(job JOIN subjob ON job.id = subjob.id) "
            "ON data_container.id = job.container_id",
        )

    def test_join_walias(self):
        (
            DataContainer,
            Job,
        ) = (self.classes.DataContainer, self.classes.Job)

        Job_A = aliased(Job)

        s = fixture_session()
        q = s.query(DataContainer).join(DataContainer.jobs.of_type(Job_A))
        self.assert_compile(
            q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN job AS job_1 "
            "ON data_container.id = job_1.container_id",
        )

    def test_join_explicit_wpoly_noalias(self):
        DataContainer, Job, SubJob = (
            self.classes.DataContainer,
            self.classes.Job,
            self.classes.SubJob,
        )

        Job_P = with_polymorphic(Job, SubJob)

        s = fixture_session()
        q = s.query(DataContainer).join(Job_P, DataContainer.jobs)
        self.assert_compile(
            q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN "
            "(job LEFT OUTER JOIN subjob "
            "ON job.id = subjob.id) "
            "ON data_container.id = job.container_id",
        )

    def test_join_explicit_wpoly_flat(self):
        DataContainer, Job, SubJob = (
            self.classes.DataContainer,
            self.classes.Job,
            self.classes.SubJob,
        )

        Job_P = with_polymorphic(Job, SubJob, flat=True)

        s = fixture_session()
        q = s.query(DataContainer).join(Job_P, DataContainer.jobs)
        self.assert_compile(
            q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN "
            "(job AS job_1 LEFT OUTER JOIN subjob AS subjob_1 "
            "ON job_1.id = subjob_1.id) "
            "ON data_container.id = job_1.container_id",
        )

    def test_join_explicit_wpoly_full_alias(self):
        DataContainer, Job, SubJob = (
            self.classes.DataContainer,
            self.classes.Job,
            self.classes.SubJob,
        )

        Job_P = with_polymorphic(Job, SubJob, aliased=True)

        s = fixture_session()
        q = s.query(DataContainer).join(Job_P, DataContainer.jobs)
        self.assert_compile(
            q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN "
            "(SELECT job.id AS job_id, job.type AS job_type, "
            "job.widget_id AS job_widget_id, "
            "job.container_id AS job_container_id, "
            "subjob.id AS subjob_id, subjob.attr AS subjob_attr "
            "FROM job LEFT OUTER JOIN subjob ON job.id = subjob.id) "
            "AS anon_1 ON data_container.id = anon_1.job_container_id",
        )


class SubclassRelationshipTest2(
    testing.AssertsCompiledSQL, fixtures.DeclarativeMappedTest
):
    run_setup_classes = "once"
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "t_a"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )

        class B(Base):
            __tablename__ = "t_b"

            type = Column(String(2))
            __mapper_args__ = {
                "polymorphic_identity": "b",
                "polymorphic_on": type,
            }

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )

            # Relationship to A
            a_id = Column(Integer, ForeignKey("t_a.id"))
            a = relationship("A", backref="bs")

        class B2(B):
            __tablename__ = "t_b2"

            __mapper_args__ = {"polymorphic_identity": "b2"}

            id = Column(Integer, ForeignKey("t_b.id"), primary_key=True)

        class C(Base):
            __tablename__ = "t_c"

            type = Column(String(2))
            __mapper_args__ = {
                "polymorphic_identity": "c",
                "polymorphic_on": type,
            }

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )

            # Relationship to B
            b_id = Column(Integer, ForeignKey("t_b.id"))
            b = relationship("B", backref="cs")

        class C2(C):
            __tablename__ = "t_c2"

            __mapper_args__ = {"polymorphic_identity": "c2"}

            id = Column(Integer, ForeignKey("t_c.id"), primary_key=True)

        class D(Base):
            __tablename__ = "t_d"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )

            # Relationship to B
            c_id = Column(Integer, ForeignKey("t_c.id"))
            c = relationship("C", backref="ds")

    @classmethod
    def insert_data(cls, connection):
        s = Session(connection)

        s.add_all(cls._fixture())
        s.commit()

    @classmethod
    def _fixture(cls):
        A, B, B2, C, C2, D = cls.classes("A", "B", "B2", "C", "C2", "D")

        return [A(bs=[B2(cs=[C2(ds=[D()])])]), A(bs=[B2(cs=[C2(ds=[D()])])])]

    def test_all_subq_query(self):
        A, B, B2, C, C2, D = self.classes("A", "B", "B2", "C", "C2", "D")

        session = Session(testing.db)

        b_b2 = with_polymorphic(B, [B2], flat=True)
        c_c2 = with_polymorphic(C, [C2], flat=True)

        q = session.query(A).options(
            subqueryload(A.bs.of_type(b_b2))
            .subqueryload(b_b2.cs.of_type(c_c2))
            .subqueryload(c_c2.ds)
        )

        self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL("SELECT t_a.id AS t_a_id FROM t_a", {}),
            CompiledSQL(
                "SELECT t_b_1.type AS t_b_1_type, t_b_1.id AS t_b_1_id, "
                "t_b_1.a_id AS t_b_1_a_id, t_b2_1.id AS t_b2_1_id, "
                "anon_1.t_a_id AS anon_1_t_a_id FROM "
                "(SELECT t_a.id AS t_a_id FROM t_a) AS anon_1 "
                "JOIN (t_b AS t_b_1 LEFT OUTER JOIN t_b2 AS t_b2_1 "
                "ON t_b_1.id = t_b2_1.id) ON anon_1.t_a_id = t_b_1.a_id",
                {},
            ),
            CompiledSQL(
                "SELECT t_c_1.type AS t_c_1_type, t_c_1.id AS t_c_1_id, "
                "t_c_1.b_id AS t_c_1_b_id, t_c2_1.id AS t_c2_1_id, "
                "t_b_1.id AS t_b_1_id FROM (SELECT t_a.id AS t_a_id FROM t_a) "
                "AS anon_1 JOIN (t_b AS t_b_1 LEFT OUTER JOIN t_b2 AS t_b2_1 "
                "ON t_b_1.id = t_b2_1.id) ON anon_1.t_a_id = t_b_1.a_id "
                "JOIN (t_c AS t_c_1 LEFT OUTER JOIN t_c2 AS t_c2_1 ON "
                "t_c_1.id = t_c2_1.id) ON t_b_1.id = t_c_1.b_id",
                {},
            ),
            CompiledSQL(
                "SELECT t_d.id AS t_d_id, t_d.c_id AS t_d_c_id, "
                "t_c_1.id AS t_c_1_id "
                "FROM (SELECT t_a.id AS t_a_id FROM t_a) AS anon_1 "
                "JOIN (t_b AS t_b_1 LEFT OUTER JOIN t_b2 AS t_b2_1 "
                "ON t_b_1.id = t_b2_1.id) "
                "ON anon_1.t_a_id = t_b_1.a_id "
                "JOIN (t_c AS t_c_1 LEFT OUTER JOIN t_c2 AS t_c2_1 "
                "ON t_c_1.id = t_c2_1.id) "
                "ON t_b_1.id = t_c_1.b_id "
                "JOIN t_d ON t_c_1.id = t_d.c_id",
                {},
            ),
        )


class SubclassRelationshipTest3(
    testing.AssertsCompiledSQL, fixtures.DeclarativeMappedTest
):
    run_setup_classes = "once"
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class _A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            type = Column(String(50), nullable=False)
            b = relationship("_B", back_populates="a")
            __mapper_args__ = {"polymorphic_on": type}

        class _B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            type = Column(String(50), nullable=False)
            a_id = Column(Integer, ForeignKey(_A.id))
            a = relationship(_A, back_populates="b")
            __mapper_args__ = {"polymorphic_on": type}

        class _C(Base):
            __tablename__ = "c"
            id = Column(Integer, primary_key=True)
            type = Column(String(50), nullable=False)
            b_id = Column(Integer, ForeignKey(_B.id))
            __mapper_args__ = {"polymorphic_on": type}

        class A1(_A):
            __mapper_args__ = {"polymorphic_identity": "A1"}

        class B1(_B):
            __mapper_args__ = {"polymorphic_identity": "B1"}

        class C1(_C):
            __mapper_args__ = {"polymorphic_identity": "C1"}
            b1 = relationship(B1, backref="c1")

    _query1 = (
        "SELECT b.id AS b_id, b.type AS b_type, b.a_id AS b_a_id, "
        "c.id AS c_id, c.type AS c_type, c.b_id AS c_b_id, a.id AS a_id, "
        "a.type AS a_type "
        "FROM a LEFT OUTER JOIN b ON "
        "a.id = b.a_id AND b.type IN (__[POSTCOMPILE_type_1]) "
        "LEFT OUTER JOIN c ON "
        "b.id = c.b_id AND c.type IN (__[POSTCOMPILE_type_2]) "
        "WHERE a.type IN (__[POSTCOMPILE_type_3])"
    )

    _query2 = (
        "SELECT bbb.id AS bbb_id, bbb.type AS bbb_type, bbb.a_id AS bbb_a_id, "
        "ccc.id AS ccc_id, ccc.type AS ccc_type, ccc.b_id AS ccc_b_id, "
        "aaa.id AS aaa_id, aaa.type AS aaa_type "
        "FROM a AS aaa LEFT OUTER JOIN b AS bbb "
        "ON aaa.id = bbb.a_id AND bbb.type IN (__[POSTCOMPILE_type_1]) "
        "LEFT OUTER JOIN c AS ccc ON "
        "bbb.id = ccc.b_id AND ccc.type IN (__[POSTCOMPILE_type_2]) "
        "WHERE aaa.type IN (__[POSTCOMPILE_type_3])"
    )

    _query3 = (
        "SELECT bbb.id AS bbb_id, bbb.type AS bbb_type, bbb.a_id AS bbb_a_id, "
        "c.id AS c_id, c.type AS c_type, c.b_id AS c_b_id, "
        "aaa.id AS aaa_id, aaa.type AS aaa_type "
        "FROM a AS aaa LEFT OUTER JOIN b AS bbb "
        "ON aaa.id = bbb.a_id AND bbb.type IN (__[POSTCOMPILE_type_1]) "
        "LEFT OUTER JOIN c ON "
        "bbb.id = c.b_id AND c.type IN (__[POSTCOMPILE_type_2]) "
        "WHERE aaa.type IN (__[POSTCOMPILE_type_3])"
    )

    def _test(self, join_of_type, of_type_for_c1, aliased_):
        A1, B1, C1 = self.classes("A1", "B1", "C1")

        if aliased_:
            A1 = aliased(A1, name="aaa")
            B1 = aliased(B1, name="bbb")
            C1 = aliased(C1, name="ccc")

        sess = fixture_session()
        abc = sess.query(A1)

        if join_of_type:
            abc = abc.outerjoin(A1.b.of_type(B1)).options(
                contains_eager(A1.b.of_type(B1))
            )

            if of_type_for_c1:
                abc = abc.outerjoin(B1.c1.of_type(C1)).options(
                    contains_eager(A1.b.of_type(B1), B1.c1.of_type(C1))
                )
            else:
                abc = abc.outerjoin(B1.c1).options(
                    contains_eager(A1.b.of_type(B1), B1.c1)
                )
        else:
            abc = abc.outerjoin(B1, A1.b).options(
                contains_eager(A1.b.of_type(B1))
            )

            if of_type_for_c1:
                abc = abc.outerjoin(C1, B1.c1).options(
                    contains_eager(A1.b.of_type(B1), B1.c1.of_type(C1))
                )
            else:
                abc = abc.outerjoin(B1.c1).options(
                    contains_eager(A1.b.of_type(B1), B1.c1)
                )

        if aliased_:
            if of_type_for_c1:
                self.assert_compile(abc, self._query2)
            else:
                self.assert_compile(abc, self._query3)
        else:
            self.assert_compile(abc, self._query1)

    def test_join_of_type_contains_eager_of_type_b1_c1(self):
        self._test(join_of_type=True, of_type_for_c1=True, aliased_=False)

    def test_join_flat_contains_eager_of_type_b1_c1(self):
        self._test(join_of_type=False, of_type_for_c1=True, aliased_=False)

    def test_join_of_type_contains_eager_of_type_b1(self):
        self._test(join_of_type=True, of_type_for_c1=False, aliased_=False)

    def test_join_flat_contains_eager_of_type_b1(self):
        self._test(join_of_type=False, of_type_for_c1=False, aliased_=False)

    def test_aliased_join_of_type_contains_eager_of_type_b1_c1(self):
        self._test(join_of_type=True, of_type_for_c1=True, aliased_=True)

    def test_aliased_join_flat_contains_eager_of_type_b1_c1(self):
        self._test(join_of_type=False, of_type_for_c1=True, aliased_=True)

    def test_aliased_join_of_type_contains_eager_of_type_b1(self):
        self._test(join_of_type=True, of_type_for_c1=False, aliased_=True)

    def test_aliased_join_flat_contains_eager_of_type_b1(self):
        self._test(join_of_type=False, of_type_for_c1=False, aliased_=True)


class JoinedloadOfTypeOptionsTest(
    testing.AssertsCompiledSQL, fixtures.DeclarativeMappedTest
):
    """Regression test for issue #13202."""

    run_setup_classes = "once"
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(ComparableEntity, Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            bs = relationship("B")

        class B(ComparableEntity, Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(Integer, ForeignKey("a.id"))
            cs = relationship("C", lazy="joined")

        class C(ComparableEntity, Base):
            __tablename__ = "c"
            id = Column(Integer, primary_key=True)
            b_id = Column(Integer, ForeignKey("b.id"))
            type = Column(String(50))
            __mapper_args__ = {"polymorphic_on": type}

        class CSub(C):
            __tablename__ = "c_sub"
            id = Column(Integer, ForeignKey("c.id"), primary_key=True)
            data = Column(String(50))
            __mapper_args__ = {"polymorphic_identity": "sub"}

    @classmethod
    def insert_data(cls, connection):
        with Session(connection) as sess:
            A, B, CSub = cls.classes("A", "B", "CSub")
            sess.add(A(bs=[B(cs=[CSub(data="csub1")])]))
            sess.commit()

    @testing.variation("format_", ["chained", "suboption"])
    @testing.variation("loader", ["joined", "selectin"])
    def test_joinedload_of_type_chained_vs_options(
        self, format_: testing.Variation, loader: testing.Variation
    ):
        """Test that joinedload().joinedload(...of_type()) and
        joinedload().options(joinedload(...of_type())) generate equivalent SQL.

        Regression test for issue #13202 where using .options() to apply
        a nested joinedload with of_type() would not propagate the
        polymorphic loading strategy correctly, resulting in missing
        polymorphic LEFT OUTER JOIN clauses.
        """
        A, B, C, CSub = self.classes("A", "B", "C", "CSub")

        c_poly = with_polymorphic(C, "*", flat=True)

        if format_.chained:
            if loader.selectin:
                stmt = select(A).options(
                    selectinload(A.bs).selectinload(B.cs.of_type(c_poly))
                )
            elif loader.joined:
                stmt = select(A).options(
                    joinedload(A.bs).joinedload(B.cs.of_type(c_poly))
                )
            else:
                loader.fail()
        elif format_.suboption:
            if loader.selectin:
                stmt = select(A).options(
                    selectinload(A.bs).options(
                        selectinload(B.cs.of_type(c_poly))
                    )
                )
            elif loader.joined:
                stmt = select(A).options(
                    joinedload(A.bs).options(joinedload(B.cs.of_type(c_poly)))
                )
            else:
                loader.fail()
        else:
            format_.fail()

        session = fixture_session()
        with self.sql_execution_asserter(testing.db) as asserter_:
            eq_(
                session.scalars(stmt).unique().all(),
                [A(bs=[B(cs=[CSub(data="csub1")])])],
            )

        if loader.selectin:
            asserter_.assert_(
                CompiledSQL("SELECT a.id FROM a"),
                CompiledSQL(
                    "SELECT b.a_id AS b_a_id, b.id AS b_id FROM b WHERE b.a_id"
                    " IN (__[POSTCOMPILE_primary_keys])"
                ),
                CompiledSQL(
                    "SELECT c_1.b_id AS c_1_b_id, c_1.id AS c_1_id, c_1.type"
                    " AS c_1_type, c_sub_1.id AS c_sub_1_id, c_sub_1.data AS"
                    " c_sub_1_data FROM c AS c_1 LEFT OUTER JOIN c_sub AS"
                    " c_sub_1 ON c_1.id = c_sub_1.id WHERE c_1.b_id IN"
                    " (__[POSTCOMPILE_primary_keys])"
                ),
            )
        elif loader.joined:
            asserter_.assert_(
                CompiledSQL(
                    "SELECT a.id, c_1.id AS id_1, c_1.b_id, c_1.type,"
                    " c_sub_1.id AS id_2, c_sub_1.data, b_1.id AS id_3,"
                    " b_1.a_id FROM a LEFT OUTER JOIN b AS b_1 ON a.id ="
                    " b_1.a_id LEFT OUTER JOIN (c AS c_1 LEFT OUTER JOIN c_sub"
                    " AS c_sub_1 ON c_1.id = c_sub_1.id) ON b_1.id = c_1.b_id"
                )
            )


class ChainedLoaderAfterOfTypeTest(
    testing.AssertsCompiledSQL, fixtures.DeclarativeMappedTest
):
    """Regression test for issue #13209.

    Tests that loader options chained after of_type() are properly applied.
    """

    run_setup_classes = "once"
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class TopABC(ComparableEntity, Base):
            __tablename__ = "top_abc"
            id = Column(Integer, primary_key=True)

        class Top(ComparableEntity, Base):
            __tablename__ = "top"
            id = Column(Integer, ForeignKey("top_abc.id"), primary_key=True)
            top_abc_id = Column(Integer, ForeignKey("top_abc.id"))
            type = Column(String(50))
            __mapper_args__ = {"polymorphic_on": type}

        class Foo(Top):
            __tablename__ = "foo"
            id = Column(Integer, ForeignKey("top.id"), primary_key=True)
            foo_name = Column(String(50))
            __mapper_args__ = {"polymorphic_identity": "FOO"}

        class Bar(Top):
            __tablename__ = "bar"
            id = Column(Integer, ForeignKey("top.id"), primary_key=True)
            bar_name = Column(String(50))
            foo_id = Column(Integer, ForeignKey("foo.id"))
            __mapper_args__ = {"polymorphic_identity": "BAR"}

        TopABC.top = relationship(
            Top, foreign_keys=[Top.top_abc_id], uselist=False
        )
        Bar.foo = relationship(Foo, foreign_keys=[Bar.foo_id], uselist=False)

    @classmethod
    def insert_data(cls, connection):
        with Session(connection) as sess:
            TopABC, Foo, Bar = cls.classes("TopABC", "Foo", "Bar")
            sess.add(
                TopABC(id=1, top=Foo(id=1, top_abc_id=1, foo_name="foo1"))
            )
            sess.add(
                TopABC(
                    id=2,
                    top=Bar(id=2, top_abc_id=2, bar_name="bar1", foo_id=1),
                )
            )
            sess.commit()

    @testing.variation("loader", ["joined", "selectin", "subquery"])
    def test_chained_loader_after_of_type(self, loader: testing.Variation):
        """Test that selectinload/joinedload/subqueryload works when chained
        after joinedload with of_type().

        Regression test for issue #13209 where chaining a loader option
        after joinedload(...of_type(poly)) would not properly apply the
        chained loader, resulting in lazy loads.
        """
        TopABC, Top, Foo, Bar = self.classes("TopABC", "Top", "Foo", "Bar")

        top_poly = with_polymorphic(Top, "*", flat=True)

        if loader.selectin:
            stmt = select(TopABC).options(
                joinedload(TopABC.top.of_type(top_poly)).selectinload(
                    top_poly.Bar.foo
                )
            )
        elif loader.joined:
            stmt = select(TopABC).options(
                joinedload(TopABC.top.of_type(top_poly)).joinedload(
                    top_poly.Bar.foo
                )
            )
        elif loader.subquery:
            stmt = select(TopABC).options(
                joinedload(TopABC.top.of_type(top_poly)).subqueryload(
                    top_poly.Bar.foo
                )
            )
        else:
            loader.fail()

        session = fixture_session()
        with self.sql_execution_asserter(testing.db) as asserter_:
            result = session.scalars(stmt).unique().all()
            # Access the chained relationship - should not trigger lazy load
            for obj in result:
                if isinstance(obj.top, Bar):
                    _ = obj.top.foo

        if loader.selectin:
            asserter_.assert_(
                CompiledSQL(
                    "SELECT top_abc.id, top_1.id AS id_1, top_1.top_abc_id,"
                    " top_1.type, foo_1.id AS id_2, foo_1.foo_name,"
                    " bar_1.id AS id_3, bar_1.bar_name, bar_1.foo_id"
                    " FROM top_abc LEFT OUTER JOIN (top AS top_1 LEFT"
                    " OUTER JOIN foo AS foo_1 ON top_1.id = foo_1.id"
                    " LEFT OUTER JOIN bar AS bar_1 ON top_1.id ="
                    " bar_1.id) ON top_abc.id = top_1.top_abc_id"
                ),
                CompiledSQL(
                    "SELECT top.id AS top_id, foo.id AS foo_id,"
                    " top.top_abc_id AS top_top_abc_id, top.type AS"
                    " top_type, foo.foo_name AS foo_foo_name FROM top"
                    " JOIN foo ON top.id = foo.id WHERE top.id IN"
                    " (__[POSTCOMPILE_primary_keys])"
                ),
            )
        elif loader.subquery:
            asserter_.assert_(
                CompiledSQL(
                    "SELECT top_abc.id, top_1.id AS id_1, top_1.top_abc_id,"
                    " top_1.type, foo_1.id AS id_2, foo_1.foo_name,"
                    " bar_1.id AS id_3, bar_1.bar_name, bar_1.foo_id"
                    " FROM top_abc LEFT OUTER JOIN (top AS top_1 LEFT"
                    " OUTER JOIN foo AS foo_1 ON top_1.id = foo_1.id"
                    " LEFT OUTER JOIN bar AS bar_1 ON top_1.id ="
                    " bar_1.id) ON top_abc.id = top_1.top_abc_id"
                ),
                CompiledSQL(
                    "SELECT foo.id AS foo_id, top.id AS top_id,"
                    " top.top_abc_id AS top_top_abc_id, top.type AS"
                    " top_type, foo.foo_name AS foo_foo_name,"
                    " anon_1.bar_foo_id AS anon_1_bar_foo_id FROM"
                    " (SELECT top_abc.id AS top_abc_id FROM top_abc)"
                    " AS anon_2 JOIN (SELECT top.id AS top_id,"
                    " top.top_abc_id AS top_top_abc_id, top.type AS"
                    " top_type, bar.id AS bar_id, bar.bar_name AS"
                    " bar_bar_name, bar.foo_id AS bar_foo_id FROM top"
                    " JOIN bar ON top.id = bar.id) AS anon_1 ON"
                    " anon_2.top_abc_id = anon_1.top_top_abc_id JOIN"
                    " (top JOIN foo ON top.id = foo.id) ON foo.id ="
                    " anon_1.bar_foo_id"
                ),
            )
        elif loader.joined:
            asserter_.assert_(
                CompiledSQL(
                    "SELECT top_abc.id, top_1.id AS id_1, top_1.top_abc_id,"
                    " top_1.type, foo_1.id AS id_2, foo_1.foo_name,"
                    " bar_1.id AS id_3, bar_1.bar_name, bar_1.foo_id,"
                    " foo_2.id AS id_4, top_2.id AS id_5, top_2.top_abc_id"
                    " AS top_abc_id_1, top_2.type AS type_1, foo_2.foo_name"
                    " AS foo_name_1 FROM top_abc LEFT OUTER JOIN (top AS"
                    " top_1 LEFT OUTER JOIN foo AS foo_1 ON top_1.id ="
                    " foo_1.id LEFT OUTER JOIN bar AS bar_1 ON top_1.id ="
                    " bar_1.id) ON top_abc.id = top_1.top_abc_id LEFT"
                    " OUTER JOIN (top AS top_2 JOIN foo AS foo_2 ON"
                    " top_2.id = foo_2.id) ON foo_2.id = bar_1.foo_id"
                )
            )
