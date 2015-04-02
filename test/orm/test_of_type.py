from sqlalchemy.orm import Session, aliased, with_polymorphic, \
    contains_eager, joinedload, subqueryload, relationship,\
    subqueryload_all, joinedload_all
from sqlalchemy import and_
from sqlalchemy import testing, exc as sa_exc
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import assert_raises, assert_raises_message, eq_
from sqlalchemy.testing.schema import Column
from sqlalchemy.engine import default
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy import Integer, String, ForeignKey
from .inheritance._poly_fixtures import Company, Person, Engineer, Manager, Boss, \
    Machine, Paperwork, _PolymorphicFixtureBase, _Polymorphic,\
    _PolymorphicPolymorphic, _PolymorphicUnions, _PolymorphicJoins,\
    _PolymorphicAliasedJoins


class _PolymorphicTestBase(object):
    __dialect__ = 'default'

    def test_any_one(self):
        sess = Session()
        any_ = Company.employees.of_type(Engineer).any(
            Engineer.primary_language == 'cobol')
        eq_(sess.query(Company).filter(any_).one(), self.c2)

    def test_any_two(self):
        sess = Session()
        calias = aliased(Company)
        any_ = calias.employees.of_type(Engineer).any(
            Engineer.primary_language == 'cobol')
        eq_(sess.query(calias).filter(any_).one(), self.c2)

    def test_any_three(self):
        sess = Session()
        any_ = Company.employees.of_type(Boss).any(
            Boss.golf_swing == 'fore')
        eq_(sess.query(Company).filter(any_).one(), self.c1)

    def test_any_four(self):
        sess = Session()
        any_ = Company.employees.of_type(Boss).any(
            Manager.manager_name == 'pointy')
        eq_(sess.query(Company).filter(any_).one(), self.c1)

    def test_any_five(self):
        sess = Session()
        any_ = Company.employees.of_type(Engineer).any(
            and_(Engineer.primary_language == 'cobol'))
        eq_(sess.query(Company).filter(any_).one(), self.c2)

    def test_join_to_subclass_one(self):
        sess = Session()
        eq_(sess.query(Company)
                .join(Company.employees.of_type(Engineer))
                .filter(Engineer.primary_language == 'java').all(),
            [self.c1])

    def test_join_to_subclass_two(self):
        sess = Session()
        eq_(sess.query(Company)
                .join(Company.employees.of_type(Engineer), 'machines')
                .filter(Machine.name.ilike("%thinkpad%")).all(),
            [self.c1])

    def test_join_to_subclass_three(self):
        sess = Session()
        eq_(sess.query(Company, Engineer)
                .join(Company.employees.of_type(Engineer))
                .filter(Engineer.primary_language == 'java').count(),
            1)

    def test_join_to_subclass_four(self):
        sess = Session()
        # test [ticket:2093]
        eq_(sess.query(Company.company_id, Engineer)
                .join(Company.employees.of_type(Engineer))
                .filter(Engineer.primary_language == 'java').count(),
            1)

    def test_join_to_subclass_five(self):
        sess = Session()
        eq_(sess.query(Company)
                .join(Company.employees.of_type(Engineer))
                .filter(Engineer.primary_language == 'java').count(),
            1)

    def test_with_polymorphic_join_compile_one(self):
        sess = Session()

        self.assert_compile(
            sess.query(Company).join(
                    Company.employees.of_type(
                        with_polymorphic(Person, [Engineer, Manager],
                                    aliased=True, flat=True)
                    )
                ),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name FROM companies "
            "JOIN %s"
             % (
                self._polymorphic_join_target([Engineer, Manager])
            )
        )

    def test_with_polymorphic_join_exec_contains_eager_one(self):
        sess = Session()
        def go():
            wp = with_polymorphic(Person, [Engineer, Manager],
                                    aliased=True, flat=True)
            eq_(
                sess.query(Company).join(
                    Company.employees.of_type(wp)
                ).order_by(Company.company_id, wp.person_id).\
                options(contains_eager(Company.employees.of_type(wp))).all(),
                [self.c1, self.c2]
            )
        self.assert_sql_count(testing.db, go, 1)

    def test_with_polymorphic_join_exec_contains_eager_two(self):
        sess = Session()
        def go():
            wp = with_polymorphic(Person, [Engineer, Manager], aliased=True)
            eq_(
                sess.query(Company).join(
                    Company.employees.of_type(wp)
                ).order_by(Company.company_id, wp.person_id).\
                options(contains_eager(Company.employees, alias=wp)).all(),
                [self.c1, self.c2]
            )
        self.assert_sql_count(testing.db, go, 1)

    def test_with_polymorphic_any(self):
        sess = Session()
        wp = with_polymorphic(Person, [Engineer], aliased=True)
        eq_(
            sess.query(Company.company_id).\
                filter(
                    Company.employees.of_type(wp).any(
                            wp.Engineer.primary_language == 'java')
                ).all(),
            [(1, )]
        )

    def test_subqueryload_implicit_withpoly(self):
        sess = Session()
        def go():
            eq_(
                sess.query(Company).\
                    filter_by(company_id=1).\
                    options(subqueryload(Company.employees.of_type(Engineer))).\
                    all(),
                [self._company_with_emps_fixture()[0]]
            )
        self.assert_sql_count(testing.db, go, 4)

    def test_joinedload_implicit_withpoly(self):
        sess = Session()
        def go():
            eq_(
                sess.query(Company).\
                    filter_by(company_id=1).\
                    options(joinedload(Company.employees.of_type(Engineer))).\
                    all(),
                [self._company_with_emps_fixture()[0]]
            )
        self.assert_sql_count(testing.db, go, 3)

    def test_subqueryload_explicit_withpoly(self):
        sess = Session()
        def go():
            target = with_polymorphic(Person, Engineer)
            eq_(
                sess.query(Company).\
                    filter_by(company_id=1).\
                    options(subqueryload(Company.employees.of_type(target))).\
                    all(),
                [self._company_with_emps_fixture()[0]]
            )
        self.assert_sql_count(testing.db, go, 4)

    def test_joinedload_explicit_withpoly(self):
        sess = Session()
        def go():
            target = with_polymorphic(Person, Engineer, flat=True)
            eq_(
                sess.query(Company).\
                    filter_by(company_id=1).\
                    options(joinedload(Company.employees.of_type(target))).\
                    all(),
                [self._company_with_emps_fixture()[0]]
            )
        self.assert_sql_count(testing.db, go, 3)

    def test_joinedload_stacked_of_type(self):
        sess = Session()

        def go():
            eq_(
                sess.query(Company).
                filter_by(company_id=1).
                options(
                    joinedload(Company.employees.of_type(Manager)),
                    joinedload(Company.employees.of_type(Engineer))
                ).all(),
                [self._company_with_emps_fixture()[0]]
            )
        self.assert_sql_count(testing.db, go, 2)


class PolymorphicPolymorphicTest(_PolymorphicTestBase, _PolymorphicPolymorphic):
    def _polymorphic_join_target(self, cls):
        from sqlalchemy.orm import class_mapper

        from sqlalchemy.sql.expression import FromGrouping
        m, sel = class_mapper(Person)._with_polymorphic_args(cls)
        sel = FromGrouping(sel.alias(flat=True))
        comp_sel = sel.compile(dialect=default.DefaultDialect())

        return \
            comp_sel.process(sel, asfrom=True).replace("\n", "") + \
            " ON companies.company_id = people_1.company_id"

class PolymorphicUnionsTest(_PolymorphicTestBase, _PolymorphicUnions):

    def _polymorphic_join_target(self, cls):
        from sqlalchemy.orm import class_mapper

        sel = class_mapper(Person)._with_polymorphic_selectable.element
        comp_sel = sel.compile(dialect=default.DefaultDialect())

        return \
            comp_sel.process(sel, asfrom=True).replace("\n", "") + \
            " AS anon_1 ON companies.company_id = anon_1.company_id"

class PolymorphicAliasedJoinsTest(_PolymorphicTestBase, _PolymorphicAliasedJoins):
    def _polymorphic_join_target(self, cls):
        from sqlalchemy.orm import class_mapper

        sel = class_mapper(Person)._with_polymorphic_selectable.element
        comp_sel = sel.compile(dialect=default.DefaultDialect())

        return \
            comp_sel.process(sel, asfrom=True).replace("\n", "") + \
            " AS anon_1 ON companies.company_id = anon_1.people_company_id"

class PolymorphicJoinsTest(_PolymorphicTestBase, _PolymorphicJoins):
    def _polymorphic_join_target(self, cls):
        from sqlalchemy.orm import class_mapper
        from sqlalchemy.sql.expression import FromGrouping

        sel = FromGrouping(class_mapper(Person)._with_polymorphic_selectable.alias(flat=True))
        comp_sel = sel.compile(dialect=default.DefaultDialect())

        return \
            comp_sel.process(sel, asfrom=True).replace("\n", "") + \
            " ON companies.company_id = people_1.company_id"

    def test_joinedload_explicit_with_unaliased_poly_compile(self):
        sess = Session()
        target = with_polymorphic(Person, Engineer)
        q = sess.query(Company).\
            filter_by(company_id=1).\
            options(joinedload(Company.employees.of_type(target)))
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Detected unaliased columns when generating joined load.",
            q._compile_context
        )


    def test_joinedload_explicit_with_flataliased_poly_compile(self):
        sess = Session()
        target = with_polymorphic(Person, Engineer, flat=True)
        q = sess.query(Company).\
            filter_by(company_id=1).\
            options(joinedload(Company.employees.of_type(target)))
        self.assert_compile(q,
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
            "ORDER BY people_1.person_id"
        )

class SubclassRelationshipTest(testing.AssertsCompiledSQL, fixtures.DeclarativeMappedTest):
    """There's overlap here vs. the ones above."""

    run_setup_classes = 'once'
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None
    __dialect__ = 'default'

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic
        class Job(ComparableEntity, Base):
            __tablename__ = "job"

            id = Column(Integer, primary_key=True,
                                        test_needs_autoincrement=True)
            type = Column(String(10))
            container_id = Column(Integer, ForeignKey('data_container.id'))
            __mapper_args__ = {"polymorphic_on": type}

        class SubJob(Job):
            __tablename__ = 'subjob'
            id = Column(Integer, ForeignKey('job.id'), primary_key=True)
            attr = Column(String(10))
            __mapper_args__ = {"polymorphic_identity": "sub"}

        class ParentThing(ComparableEntity, Base):
            __tablename__ = 'parent'
            id = Column(Integer, primary_key=True,
                                            test_needs_autoincrement=True)
            container_id = Column(Integer, ForeignKey('data_container.id'))
            container = relationship("DataContainer")

        class DataContainer(ComparableEntity, Base):
            __tablename__ = "data_container"

            id = Column(Integer, primary_key=True,
                                            test_needs_autoincrement=True)
            name = Column(String(10))
            jobs = relationship(Job, order_by=Job.id)

    @classmethod
    def insert_data(cls):
        s = Session(testing.db)

        s.add_all(cls._fixture())
        s.commit()

    @classmethod
    def _fixture(cls):
        ParentThing, DataContainer, SubJob = \
            cls.classes.ParentThing,\
            cls.classes.DataContainer,\
            cls.classes.SubJob
        return [
            ParentThing(
                container=DataContainer(name="d1",
                    jobs=[
                        SubJob(attr="s1"),
                        SubJob(attr="s2")
                    ])
            ),
            ParentThing(
                container=DataContainer(name="d2",
                    jobs=[
                        SubJob(attr="s3"),
                        SubJob(attr="s4")
                    ])
            ),
        ]

    @classmethod
    def _dc_fixture(cls):
        return [p.container for p in cls._fixture()]

    def test_contains_eager_wpoly(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob

        Job_P = with_polymorphic(Job, SubJob, aliased=True)

        s = Session(testing.db)
        q = s.query(DataContainer).\
                    join(DataContainer.jobs.of_type(Job_P)).\
                        options(contains_eager(DataContainer.jobs.of_type(Job_P)))
        def go():
            eq_(
                q.all(),
                self._dc_fixture()
            )
        self.assert_sql_count(testing.db, go, 1)

    def test_joinedload_wpoly(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob

        Job_P = with_polymorphic(Job, SubJob, aliased=True)

        s = Session(testing.db)
        q = s.query(DataContainer).\
                        options(joinedload(DataContainer.jobs.of_type(Job_P)))
        def go():
            eq_(
                q.all(),
                self._dc_fixture()
            )
        self.assert_sql_count(testing.db, go, 1)

    def test_joinedload_wsubclass(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob
        s = Session(testing.db)
        q = s.query(DataContainer).\
                        options(joinedload(DataContainer.jobs.of_type(SubJob)))
        def go():
            eq_(
                q.all(),
                self._dc_fixture()
            )
        self.assert_sql_count(testing.db, go, 1)

    def test_lazyload(self):
        DataContainer = self.classes.DataContainer
        s = Session(testing.db)
        q = s.query(DataContainer)
        def go():
            eq_(
                q.all(),
                self._dc_fixture()
            )
        # SELECT data container
        # SELECT job * 2 container rows
        # SELECT subjob * 4 rows
        self.assert_sql_count(testing.db, go, 7)

    def test_subquery_wsubclass(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob
        s = Session(testing.db)
        q = s.query(DataContainer).\
                        options(subqueryload(DataContainer.jobs.of_type(SubJob)))
        def go():
            eq_(
                q.all(),
                self._dc_fixture()
            )
        self.assert_sql_count(testing.db, go, 2)

    def test_twolevel_subqueryload_wsubclass(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob
        s = Session(testing.db)
        q = s.query(ParentThing).\
                        options(
                            subqueryload_all(
                                ParentThing.container,
                                DataContainer.jobs.of_type(SubJob)
                        ))
        def go():
            eq_(
                q.all(),
                self._fixture()
            )
        self.assert_sql_count(testing.db, go, 3)

    def test_twolevel_joinedload_wsubclass(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob
        s = Session(testing.db)
        q = s.query(ParentThing).\
                        options(
                            joinedload_all(
                                ParentThing.container,
                                DataContainer.jobs.of_type(SubJob)
                        ))
        def go():
            eq_(
                q.all(),
                self._fixture()
            )
        self.assert_sql_count(testing.db, go, 1)

    def test_any_wpoly(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob

        Job_P = with_polymorphic(Job, SubJob, aliased=True, flat=True)

        s = Session()
        q = s.query(Job).join(DataContainer.jobs).\
                        filter(
                            DataContainer.jobs.of_type(Job_P).\
                                any(Job_P.id < Job.id)
                        )

        self.assert_compile(q,
            "SELECT job.id AS job_id, job.type AS job_type, "
            "job.container_id "
            "AS job_container_id "
            "FROM data_container "
            "JOIN job ON data_container.id = job.container_id "
            "WHERE EXISTS (SELECT 1 "
            "FROM job AS job_1 LEFT OUTER JOIN subjob AS subjob_1 "
                "ON job_1.id = subjob_1.id "
            "WHERE data_container.id = job_1.container_id "
            "AND job_1.id < job.id)"
        )

    def test_any_walias(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob

        Job_A = aliased(Job)

        s = Session()
        q = s.query(Job).join(DataContainer.jobs).\
                        filter(
                            DataContainer.jobs.of_type(Job_A).\
                                any(and_(Job_A.id < Job.id, Job_A.type=='fred'))
                        )
        self.assert_compile(q,
            "SELECT job.id AS job_id, job.type AS job_type, "
            "job.container_id AS job_container_id "
            "FROM data_container JOIN job ON data_container.id = job.container_id "
            "WHERE EXISTS (SELECT 1 "
            "FROM job AS job_1 "
            "WHERE data_container.id = job_1.container_id "
            "AND job_1.id < job.id AND job_1.type = :type_1)"
        )

    def test_join_wpoly(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob

        Job_P = with_polymorphic(Job, SubJob)

        s = Session()
        q = s.query(DataContainer).join(DataContainer.jobs.of_type(Job_P))
        self.assert_compile(q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN "
            "(job LEFT OUTER JOIN subjob "
                "ON job.id = subjob.id) "
            "ON data_container.id = job.container_id")

    def test_join_wsubclass(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob

        s = Session()
        q = s.query(DataContainer).join(DataContainer.jobs.of_type(SubJob))
        # note the of_type() here renders JOIN for the Job->SubJob.
        # this is because it's using the SubJob mapper directly within
        # query.join().  When we do joinedload() etc., we're instead
        # doing a with_polymorphic(), and there we need the join to be
        # outer by default.
        self.assert_compile(q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN (job JOIN subjob ON job.id = subjob.id) "
            "ON data_container.id = job.container_id"
        )

    def test_join_wpoly_innerjoin(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob

        Job_P = with_polymorphic(Job, SubJob, innerjoin=True)

        s = Session()
        q = s.query(DataContainer).join(DataContainer.jobs.of_type(Job_P))
        self.assert_compile(q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN "
            "(job JOIN subjob ON job.id = subjob.id) "
            "ON data_container.id = job.container_id")

    def test_join_walias(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob

        Job_A = aliased(Job)

        s = Session()
        q = s.query(DataContainer).join(DataContainer.jobs.of_type(Job_A))
        self.assert_compile(q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN job AS job_1 "
            "ON data_container.id = job_1.container_id")

    def test_join_explicit_wpoly_noalias(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob

        Job_P = with_polymorphic(Job, SubJob)

        s = Session()
        q = s.query(DataContainer).join(Job_P, DataContainer.jobs)
        self.assert_compile(q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN "
            "(job LEFT OUTER JOIN subjob "
            "ON job.id = subjob.id) "
            "ON data_container.id = job.container_id")


    def test_join_explicit_wpoly_flat(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob

        Job_P = with_polymorphic(Job, SubJob, flat=True)

        s = Session()
        q = s.query(DataContainer).join(Job_P, DataContainer.jobs)
        self.assert_compile(q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN "
            "(job AS job_1 LEFT OUTER JOIN subjob AS subjob_1 "
            "ON job_1.id = subjob_1.id) "
            "ON data_container.id = job_1.container_id")

    def test_join_explicit_wpoly_full_alias(self):
        ParentThing, DataContainer, Job, SubJob = \
            self.classes.ParentThing,\
            self.classes.DataContainer,\
            self.classes.Job,\
            self.classes.SubJob

        Job_P = with_polymorphic(Job, SubJob, aliased=True)

        s = Session()
        q = s.query(DataContainer).join(Job_P, DataContainer.jobs)
        self.assert_compile(q,
            "SELECT data_container.id AS data_container_id, "
            "data_container.name AS data_container_name "
            "FROM data_container JOIN "
            "(SELECT job.id AS job_id, job.type AS job_type, "
                "job.container_id AS job_container_id, "
                "subjob.id AS subjob_id, subjob.attr AS subjob_attr "
                "FROM job LEFT OUTER JOIN subjob ON job.id = subjob.id) "
                "AS anon_1 ON data_container.id = anon_1.job_container_id"
        )

