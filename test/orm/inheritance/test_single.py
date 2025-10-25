from __future__ import annotations

from contextlib import nullcontext
from typing import List

from sqlalchemy import and_
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import null
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import true
from sqlalchemy.orm import aliased
from sqlalchemy.orm import Bundle
from sqlalchemy.orm import column_property
from sqlalchemy.orm import join as orm_join
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.sql.selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


def _aliased_join_warning(arg):
    return testing.expect_warnings(
        "An alias is being generated automatically against joined entity "
        "%s due to overlapping tables" % (arg,)
    )


class SingleInheritanceTest(testing.AssertsCompiledSQL, fixtures.MappedTest):
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
        )

        Table(
            "reports",
            metadata,
            Column(
                "report_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("employee_id", ForeignKey("employees.employee_id")),
            Column("name", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        global Employee, Manager, Engineer, JuniorEngineer

        class Employee(cls.Comparable):
            pass

        class Manager(Employee):
            pass

        class Engineer(Employee):
            pass

        class JuniorEngineer(Engineer):
            pass

        class Report(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        (
            Report,
            reports,
            Employee,
            Manager,
            JuniorEngineer,
            employees,
            Engineer,
        ) = (
            cls.classes.Report,
            cls.tables.reports,
            cls.classes.Employee,
            cls.classes.Manager,
            cls.classes.JuniorEngineer,
            cls.tables.employees,
            cls.classes.Engineer,
        )

        cls.mapper_registry.map_imperatively(
            Report, reports, properties={"employee": relationship(Employee)}
        )
        cls.mapper_registry.map_imperatively(
            Employee,
            employees,
            polymorphic_identity="employee",
            polymorphic_on=employees.c.type,
            properties={
                "reports": relationship(Report, back_populates="employee")
            },
        )
        cls.mapper_registry.map_imperatively(
            Manager, inherits=Employee, polymorphic_identity="manager"
        )
        cls.mapper_registry.map_imperatively(
            Engineer, inherits=Employee, polymorphic_identity="engineer"
        )
        cls.mapper_registry.map_imperatively(
            JuniorEngineer,
            inherits=Engineer,
            polymorphic_identity="juniorengineer",
        )

    def _fixture_one(self):
        JuniorEngineer, Manager, Engineer = (
            self.classes.JuniorEngineer,
            self.classes.Manager,
            self.classes.Engineer,
        )

        session = fixture_session()

        m1 = Manager(name="Tom", manager_data="knows how to manage things")
        e1 = Engineer(name="Kurt", engineer_info="knows how to hack")
        e2 = JuniorEngineer(name="Ed", engineer_info="oh that ed")
        session.add_all([m1, e1, e2])
        session.flush()
        return session, m1, e1, e2

    def test_single_inheritance(self):
        Employee, JuniorEngineer, Manager, Engineer = (
            self.classes.Employee,
            self.classes.JuniorEngineer,
            self.classes.Manager,
            self.classes.Engineer,
        )

        session, m1, e1, e2 = self._fixture_one()

        assert session.query(Employee).all() == [m1, e1, e2]
        assert session.query(Engineer).all() == [e1, e2]
        assert session.query(Manager).all() == [m1]
        assert session.query(JuniorEngineer).all() == [e2]

        m1 = session.query(Manager).one()
        session.expire(m1, ["manager_data"])
        eq_(m1.manager_data, "knows how to manage things")

        row = (
            session.query(Engineer.name, Engineer.employee_id)
            .filter(Engineer.name == "Kurt")
            .first()
        )
        assert row.name == "Kurt"
        assert row.employee_id == e1.employee_id

    def test_discrim_bound_param_cloned_ok(self):
        """Test #6824

        note this changes a bit with #12395"""

        Manager = self.classes.Manager

        subq1 = select(Manager.employee_id).label("foo")
        subq2 = select(Manager.employee_id).label("bar")
        self.assert_compile(
            select(subq1, subq2),
            "SELECT (SELECT employees.employee_id FROM employees "
            "WHERE employees.type IN (__[POSTCOMPILE_type_1])) AS foo, "
            "(SELECT employees.employee_id FROM employees "
            "WHERE employees.type IN (__[POSTCOMPILE_type_2])) AS bar",
            checkparams={"type_1": ["manager"], "type_2": ["manager"]},
        )

    def test_multi_qualification(self):
        Manager, Engineer = (self.classes.Manager, self.classes.Engineer)

        session, m1, e1, e2 = self._fixture_one()

        ealias = aliased(Engineer)
        eq_(
            session.query(Manager, ealias).join(ealias, true()).all(),
            [(m1, e1), (m1, e2)],
        )

        eq_(session.query(Manager.name).all(), [("Tom",)])

        eq_(
            session.query(Manager.name, ealias.name)
            .join(ealias, true())
            .all(),
            [("Tom", "Kurt"), ("Tom", "Ed")],
        )

        eq_(
            session.query(func.upper(Manager.name), func.upper(ealias.name))
            .join(ealias, true())
            .all(),
            [("TOM", "KURT"), ("TOM", "ED")],
        )

        eq_(
            session.query(Manager)
            .add_entity(ealias)
            .join(ealias, true())
            .all(),
            [(m1, e1), (m1, e2)],
        )

        eq_(
            session.query(Manager.name)
            .add_columns(ealias.name)
            .join(ealias, true())
            .all(),
            [("Tom", "Kurt"), ("Tom", "Ed")],
        )

        # TODO: I think raise error on this for now
        # self.assertEquals(
        #    session.query(Employee.name, Manager.manager_data,
        #    Engineer.engineer_info).all(),
        #    []
        # )

    def test_column_qualification(self):
        Employee, JuniorEngineer, Manager, Engineer = (
            self.classes.Employee,
            self.classes.JuniorEngineer,
            self.classes.Manager,
            self.classes.Engineer,
        )

        session, m1, e1, e2 = self._fixture_one()

        m1id, e1id, e2id = m1.employee_id, e1.employee_id, e2.employee_id

        def scalar(q):
            return [x for x, in q]

        eq_(scalar(session.query(Employee.employee_id)), [m1id, e1id, e2id])

        eq_(scalar(session.query(Engineer.employee_id)), [e1id, e2id])

        eq_(scalar(session.query(Manager.employee_id)), [m1id])

        # this currently emits "WHERE type IN (?, ?) AND type IN (?, ?)",
        # so no result.
        eq_(session.query(Manager.employee_id, Engineer.employee_id).all(), [])

        # however, with #12395, a with_polymorphic will merge the IN
        # together
        wp = with_polymorphic(Employee, [Manager, Engineer])
        eq_(
            session.query(
                wp.Manager.employee_id, wp.Engineer.employee_id
            ).all(),
            [(m1id, m1id), (e1id, e1id), (e2id, e2id)],
        )

        eq_(scalar(session.query(JuniorEngineer.employee_id)), [e2id])

    def test_bundle_qualification(self):
        Employee, JuniorEngineer, Manager, Engineer = (
            self.classes.Employee,
            self.classes.JuniorEngineer,
            self.classes.Manager,
            self.classes.Engineer,
        )

        session, m1, e1, e2 = self._fixture_one()

        m1id, e1id, e2id = m1.employee_id, e1.employee_id, e2.employee_id

        def scalar(q):
            return [x[0] for x, in q]

        eq_(
            scalar(session.query(Bundle("name", Employee.employee_id))),
            [m1id, e1id, e2id],
        )

        eq_(
            scalar(session.query(Bundle("name", Engineer.employee_id))),
            [e1id, e2id],
        )

        eq_(scalar(session.query(Bundle("name", Manager.employee_id))), [m1id])

        # this currently emits "WHERE type IN (?, ?) AND type IN (?, ?)",
        # so no result.
        eq_(
            session.query(
                Bundle("name", Manager.employee_id, Engineer.employee_id)
            ).all(),
            [],
        )

        # however, with #12395, a with_polymorphic will merge the IN
        # together
        wp = with_polymorphic(Employee, [Manager, Engineer])
        eq_(
            session.query(
                Bundle("name", wp.Manager.employee_id, wp.Engineer.employee_id)
            ).all(),
            [((m1id, m1id),), ((e1id, e1id),), ((e2id, e2id),)],
        )

        eq_(
            scalar(session.query(Bundle("name", JuniorEngineer.employee_id))),
            [e2id],
        )

    def test_from_subq(self):
        Engineer = self.classes.Engineer

        stmt = select(Engineer)
        subq = aliased(
            Engineer,
            stmt.set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL).subquery(),
        )

        # so here we have an extra "WHERE type in ()", because
        # both the inner and the outer queries have the Engineer entity.
        # this is expected at the moment but it would be nice if
        # _enable_single_crit or something similar could propagate here.
        # legacy from_self() takes care of this because it applies
        # _enable_single_crit at that moment.

        stmt = select(subq).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        self.assert_compile(
            stmt,
            "SELECT anon_1.employees_employee_id AS "
            "anon_1_employees_employee_id, "
            "anon_1.employees_name AS "
            "anon_1_employees_name, "
            "anon_1.employees_manager_data AS "
            "anon_1_employees_manager_data, "
            "anon_1.employees_engineer_info AS "
            "anon_1_employees_engineer_info, "
            "anon_1.employees_type AS "
            "anon_1_employees_type FROM (SELECT "
            "employees.employee_id AS "
            "employees_employee_id, employees.name AS "
            "employees_name, employees.manager_data AS "
            "employees_manager_data, "
            "employees.engineer_info AS "
            "employees_engineer_info, employees.type "
            "AS employees_type FROM employees WHERE "
            "employees.type IN (__[POSTCOMPILE_type_1])) AS "
            "anon_1 WHERE anon_1.employees_type IN (__[POSTCOMPILE_type_2])",
            use_default_dialect=True,
        )

    def test_select_from_aliased_w_subclass(self):
        Engineer = self.classes.Engineer

        sess = fixture_session()

        a1 = aliased(Engineer)
        self.assert_compile(
            sess.query(a1.employee_id).select_from(a1),
            "SELECT employees_1.employee_id AS employees_1_employee_id "
            "FROM employees AS employees_1 WHERE employees_1.type "
            "IN (__[POSTCOMPILE_type_1])",
        )

        self.assert_compile(
            sess.query(literal("1")).select_from(a1),
            "SELECT :param_1 AS anon_1 FROM employees AS employees_1 "
            "WHERE employees_1.type IN (__[POSTCOMPILE_type_1])",
        )

    @testing.combinations(
        (
            lambda Engineer, Report: select(Report.report_id)
            .select_from(Engineer)
            .join(Engineer.reports),
        ),
        (
            lambda Engineer, Report: select(Report.report_id).select_from(
                orm_join(Engineer, Report, Engineer.reports)
            ),
        ),
        (
            lambda Engineer, Report: select(Report.report_id).join_from(
                Engineer, Report, Engineer.reports
            ),
        ),
        (
            lambda Engineer, Report: select(Report.report_id)
            .select_from(Engineer)
            .join(Report),
        ),
        argnames="stmt_fn",
    )
    @testing.combinations(True, False, argnames="alias_engineer")
    def test_select_col_only_from_w_join(self, stmt_fn, alias_engineer):
        """test #11412 which seems to have been fixed by #10365"""

        Engineer = self.classes.Engineer
        Report = self.classes.Report

        if alias_engineer:
            Engineer = aliased(Engineer)
        stmt = testing.resolve_lambda(
            stmt_fn, Engineer=Engineer, Report=Report
        )

        if alias_engineer:
            self.assert_compile(
                stmt,
                "SELECT reports.report_id FROM employees AS employees_1 "
                "JOIN reports ON employees_1.employee_id = "
                "reports.employee_id WHERE employees_1.type "
                "IN (__[POSTCOMPILE_type_1])",
            )
        else:
            self.assert_compile(
                stmt,
                "SELECT reports.report_id FROM employees JOIN reports "
                "ON employees.employee_id = reports.employee_id "
                "WHERE employees.type IN (__[POSTCOMPILE_type_1])",
            )

    @testing.combinations(
        (
            lambda Engineer, Report: select(Report)
            .select_from(Engineer)
            .join(Engineer.reports),
        ),
        (
            lambda Engineer, Report: select(Report).select_from(
                orm_join(Engineer, Report, Engineer.reports)
            ),
        ),
        (
            lambda Engineer, Report: select(Report).join_from(
                Engineer, Report, Engineer.reports
            ),
        ),
        argnames="stmt_fn",
    )
    @testing.combinations(True, False, argnames="alias_engineer")
    def test_select_from_w_join_left(self, stmt_fn, alias_engineer):
        """test #8721"""

        Engineer = self.classes.Engineer
        Report = self.classes.Report

        if alias_engineer:
            Engineer = aliased(Engineer)
        stmt = testing.resolve_lambda(
            stmt_fn, Engineer=Engineer, Report=Report
        )

        if alias_engineer:
            self.assert_compile(
                stmt,
                "SELECT reports.report_id, reports.employee_id, reports.name "
                "FROM employees AS employees_1 JOIN reports "
                "ON employees_1.employee_id = reports.employee_id "
                "WHERE employees_1.type IN (__[POSTCOMPILE_type_1])",
            )
        else:
            self.assert_compile(
                stmt,
                "SELECT reports.report_id, reports.employee_id, reports.name "
                "FROM employees JOIN reports ON employees.employee_id = "
                "reports.employee_id "
                "WHERE employees.type IN (__[POSTCOMPILE_type_1])",
            )

    @testing.combinations(
        (
            lambda Engineer, Report: select(
                Report.report_id, Engineer.employee_id
            )
            .select_from(Engineer)
            .join(Engineer.reports),
        ),
        (
            lambda Engineer, Report: select(
                Report.report_id, Engineer.employee_id
            ).select_from(orm_join(Engineer, Report, Engineer.reports)),
        ),
        (
            lambda Engineer, Report: select(
                Report.report_id, Engineer.employee_id
            ).join_from(Engineer, Report, Engineer.reports),
        ),
    )
    def test_select_from_w_join_left_including_entity(self, stmt_fn):
        """test #8721"""

        Engineer = self.classes.Engineer
        Report = self.classes.Report
        stmt = testing.resolve_lambda(
            stmt_fn, Engineer=Engineer, Report=Report
        )

        self.assert_compile(
            stmt,
            "SELECT reports.report_id, employees.employee_id "
            "FROM employees JOIN reports ON employees.employee_id = "
            "reports.employee_id "
            "WHERE employees.type IN (__[POSTCOMPILE_type_1])",
        )

    @testing.combinations(
        (
            lambda Engineer, Report: select(Report).join(
                Report.employee.of_type(Engineer)
            ),
        ),
        (
            lambda Engineer, Report: select(Report).select_from(
                orm_join(Report, Engineer, Report.employee.of_type(Engineer))
            )
        ),
        (
            lambda Engineer, Report: select(Report).join_from(
                Report, Engineer, Report.employee.of_type(Engineer)
            ),
        ),
    )
    def test_select_from_w_join_right(self, stmt_fn):
        """test #8721"""

        Engineer = self.classes.Engineer
        Report = self.classes.Report
        stmt = testing.resolve_lambda(
            stmt_fn, Engineer=Engineer, Report=Report
        )

        self.assert_compile(
            stmt,
            "SELECT reports.report_id, reports.employee_id, reports.name "
            "FROM reports JOIN employees ON employees.employee_id = "
            "reports.employee_id AND employees.type "
            "IN (__[POSTCOMPILE_type_1])",
        )

    def test_from_statement_select(self):
        Engineer = self.classes.Engineer

        stmt = select(Engineer)

        q = select(Engineer).from_statement(stmt)

        self.assert_compile(
            q,
            "SELECT employees.employee_id, employees.name, "
            "employees.manager_data, employees.engineer_info, "
            "employees.type FROM employees WHERE employees.type "
            "IN (__[POSTCOMPILE_type_1])",
        )

    def test_from_statement_update(self):
        """test #6591"""

        Engineer = self.classes.Engineer

        from sqlalchemy import update

        stmt = (
            update(Engineer)
            .values(engineer_info="bar")
            .returning(Engineer.employee_id)
        )

        q = select(Engineer).from_statement(stmt)

        self.assert_compile(
            q,
            "UPDATE employees SET engineer_info=:engineer_info "
            "WHERE employees.type IN (__[POSTCOMPILE_type_1]) "
            "RETURNING employees.employee_id",
            dialect="default_enhanced",
        )

    def test_union_modifiers(self):
        Engineer, Manager = self.classes("Engineer", "Manager")

        sess = fixture_session()
        q1 = sess.query(Engineer).filter(Engineer.engineer_info == "foo")
        q2 = sess.query(Manager).filter(Manager.manager_data == "bar")

        assert_sql = (
            "SELECT anon_1.employees_employee_id AS "
            "anon_1_employees_employee_id, "
            "anon_1.employees_name AS anon_1_employees_name, "
            "anon_1.employees_manager_data AS anon_1_employees_manager_data, "
            "anon_1.employees_engineer_info AS anon_1_employees_engineer_info, "  # noqa
            "anon_1.employees_type AS anon_1_employees_type "
            "FROM (SELECT employees.employee_id AS employees_employee_id, "
            "employees.name AS employees_name, "
            "employees.manager_data AS employees_manager_data, "
            "employees.engineer_info AS employees_engineer_info, "
            "employees.type AS employees_type FROM employees "
            "WHERE employees.engineer_info = :engineer_info_1 "
            "AND employees.type IN (__[POSTCOMPILE_type_1]) "
            "%(token)s "
            "SELECT employees.employee_id AS employees_employee_id, "
            "employees.name AS employees_name, "
            "employees.manager_data AS employees_manager_data, "
            "employees.engineer_info AS employees_engineer_info, "
            "employees.type AS employees_type FROM employees "
            "WHERE employees.manager_data = :manager_data_1 "
            "AND employees.type IN (__[POSTCOMPILE_type_2])) AS anon_1"
        )

        for meth, token in [
            (q1.union, "UNION"),
            (q1.union_all, "UNION ALL"),
            (q1.except_, "EXCEPT"),
            (q1.except_all, "EXCEPT ALL"),
            (q1.intersect, "INTERSECT"),
            (q1.intersect_all, "INTERSECT ALL"),
        ]:
            self.assert_compile(
                meth(q2),
                assert_sql % {"token": token},
                checkparams={
                    "engineer_info_1": "foo",
                    "type_1": ["engineer", "juniorengineer"],
                    "manager_data_1": "bar",
                    "type_2": ["manager"],
                },
            )

    def test_having(self):
        Engineer, Manager = self.classes("Engineer", "Manager")

        sess = fixture_session()

        self.assert_compile(
            sess.query(Engineer)
            .group_by(Engineer.employee_id)
            .having(Engineer.name == "js"),
            "SELECT employees.employee_id AS employees_employee_id, "
            "employees.name AS employees_name, employees.manager_data "
            "AS employees_manager_data, employees.engineer_info "
            "AS employees_engineer_info, employees.type AS employees_type "
            "FROM employees WHERE employees.type IN (__[POSTCOMPILE_type_1]) "
            "GROUP BY employees.employee_id HAVING employees.name = :name_1",
        )

    def test_select_from_count(self):
        Manager, Engineer = (self.classes.Manager, self.classes.Engineer)

        sess = fixture_session()
        m1 = Manager(name="Tom", manager_data="data1")
        e1 = Engineer(name="Kurt", engineer_info="knows how to hack")
        sess.add_all([m1, e1])
        sess.flush()

        eq_(sess.query(func.count(1)).select_from(Manager).all(), [(1,)])

    def test_select_from_subquery(self):
        Manager, JuniorEngineer, employees, Engineer = (
            self.classes.Manager,
            self.classes.JuniorEngineer,
            self.tables.employees,
            self.classes.Engineer,
        )

        sess = fixture_session()
        m1 = Manager(name="Tom", manager_data="data1")
        m2 = Manager(name="Tom2", manager_data="data2")
        e1 = Engineer(name="Kurt", engineer_info="knows how to hack")
        e2 = JuniorEngineer(name="Ed", engineer_info="oh that ed")
        sess.add_all([m1, m2, e1, e2])
        sess.flush()

        ma = aliased(
            Manager,
            employees.select()
            .where(employees.c.type == "manager")
            .order_by(employees.c.employee_id)
            .limit(10)
            .subquery(),
        )
        eq_(
            sess.query(ma).all(),
            [m1, m2],
        )

    def test_select_from_subquery_with_composed_union(self):
        Report, reports, Manager, JuniorEngineer, employees, Engineer = (
            self.classes.Report,
            self.tables.reports,
            self.classes.Manager,
            self.classes.JuniorEngineer,
            self.tables.employees,
            self.classes.Engineer,
        )

        sess = fixture_session()
        r1, r2, r3, r4 = (
            Report(name="r1"),
            Report(name="r2"),
            Report(name="r3"),
            Report(name="r4"),
        )
        m1 = Manager(name="manager1", manager_data="data1", reports=[r1])
        m2 = Manager(name="manager2", manager_data="data2", reports=[r2])
        e1 = Engineer(name="engineer1", engineer_info="einfo1", reports=[r3])
        e2 = JuniorEngineer(
            name="engineer2", engineer_info="einfo2", reports=[r4]
        )
        sess.add_all([m1, m2, e1, e2])
        sess.flush()

        stmt = select(reports, employees).select_from(
            reports.outerjoin(
                employees,
                and_(
                    employees.c.employee_id == reports.c.employee_id,
                    employees.c.type == "manager",
                ),
            )
        )

        subq = stmt.subquery()

        ra = aliased(Report, subq)

        # this test previously used select_entity_from().  the standard
        # conversion to use aliased() needs to be adjusted to be against
        # Employee, not Manager, otherwise the ORM will add the manager single
        # inh criteria to the outside which will break the outer join
        ma = aliased(Employee, subq)

        eq_(
            sess.query(ra, ma).order_by(ra.name).all(),
            [(r1, m1), (r2, m2), (r3, None), (r4, None)],
        )

        # however if someone really wants to run that SELECT statement and
        # get back these two entities, they can use from_statement() more
        # directly.  in 1.4 we don't even need tablename label style for the
        # select(), automatic disambiguation works great
        eq_(
            sess.query(Report, Manager)
            .from_statement(stmt.order_by(reports.c.name))
            .all(),
            [(r1, m1), (r2, m2), (r3, None), (r4, None)],
        )

    def test_count(self):
        Employee = self.classes.Employee
        JuniorEngineer = self.classes.JuniorEngineer
        Manager = self.classes.Manager
        Engineer = self.classes.Engineer

        sess = fixture_session()
        m1 = Manager(name="Tom", manager_data="data1")
        m2 = Manager(name="Tom2", manager_data="data2")
        e1 = Engineer(name="Kurt", engineer_info="data3")
        e2 = JuniorEngineer(name="marvin", engineer_info="data4")
        sess.add_all([m1, m2, e1, e2])
        sess.flush()

        eq_(sess.query(Manager).count(), 2)
        eq_(sess.query(Engineer).count(), 2)
        eq_(sess.query(Employee).count(), 4)

        eq_(sess.query(Manager).filter(Manager.name.like("%m%")).count(), 2)
        eq_(sess.query(Employee).filter(Employee.name.like("%m%")).count(), 3)

    def test_exists_standalone(self):
        Engineer = self.classes.Engineer

        sess = fixture_session()

        self.assert_compile(
            sess.query(
                sess.query(Engineer).filter(Engineer.name == "foo").exists()
            ),
            "SELECT EXISTS (SELECT 1 FROM employees WHERE "
            "employees.name = :name_1 AND employees.type "
            "IN (__[POSTCOMPILE_type_1])) AS anon_1",
        )

    def test_type_filtering(self):
        Report, Manager, Engineer = (
            self.classes.Report,
            self.classes.Manager,
            self.classes.Engineer,
        )

        sess = fixture_session()

        m1 = Manager(name="Tom", manager_data="data1")
        r1 = Report(employee=m1)
        sess.add_all([m1, r1])
        sess.flush()
        rq = sess.query(Report)

        assert (
            len(rq.filter(Report.employee.of_type(Manager).has()).all()) == 1
        )
        assert (
            len(rq.filter(Report.employee.of_type(Engineer).has()).all()) == 0
        )

    def test_type_joins(self):
        Report, Manager, Engineer = (
            self.classes.Report,
            self.classes.Manager,
            self.classes.Engineer,
        )

        sess = fixture_session()

        m1 = Manager(name="Tom", manager_data="data1")
        r1 = Report(employee=m1)
        sess.add_all([m1, r1])
        sess.flush()

        rq = sess.query(Report)

        assert len(rq.join(Report.employee.of_type(Manager)).all()) == 1
        assert len(rq.join(Report.employee.of_type(Engineer)).all()) == 0


class WPolySingleJoinedParityTest:
    """a suite to test that with_polymorphic behaves identically
    with joined or single inheritance as of 2.1, issue #12395

    """

    @classmethod
    def insert_data(cls, connection):
        Employee, Manager, Engineer, Boss, JuniorEngineer = cls.classes(
            "Employee", "Manager", "Engineer", "Boss", "JuniorEngineer"
        )
        with Session(connection) as session:
            session.add(Employee(name="Employee 1"))
            session.add(Manager(name="Manager 1", manager_data="manager data"))
            session.add(
                Engineer(name="Engineer 1", engineer_info="engineer_info")
            )
            session.add(
                JuniorEngineer(
                    name="Junior Engineer 1",
                    engineer_info="junior info",
                    junior_name="junior name",
                )
            )
            session.add(Boss(name="Boss 1", manager_data="boss data"))

            session.commit()

    @testing.variation("wpoly_type", ["star", "classes"])
    def test_with_polymorphic_sibling_classes_base(
        self, wpoly_type: testing.Variation
    ):
        Employee, Manager, Engineer, JuniorEngineer, Boss = self.classes(
            "Employee", "Manager", "Engineer", "JuniorEngineer", "Boss"
        )

        if wpoly_type.star:
            wp = with_polymorphic(Employee, "*")
        elif wpoly_type.classes:
            wp = with_polymorphic(
                Employee, [Manager, Engineer, JuniorEngineer]
            )
        else:
            wpoly_type.fail()

        stmt = select(wp).order_by(wp.id)
        session = fixture_session()
        eq_(
            session.scalars(stmt).all(),
            [
                Employee(name="Employee 1"),
                Manager(name="Manager 1", manager_data="manager data"),
                Engineer(engineer_info="engineer_info"),
                JuniorEngineer(engineer_info="junior info"),
                Boss(name="Boss 1", manager_data="boss data"),
            ],
        )

        # this raises, because we get rows that are not Manager or
        # JuniorEngineer

        stmt = select(wp, wp.Manager, wp.JuniorEngineer).order_by(wp.id)
        with expect_raises_message(
            exc.InvalidRequestError,
            r"Row with identity key \(<.*Employee'>, .*\) can't be loaded "
            r"into an object; the polymorphic discriminator column "
            r"'employee.type' refers to Mapper\[Employee\(.*\)\], "
            r"which is "
            r"not a sub-mapper of the requested "
            r"Mapper\[Manager\(.*\)\]",
        ):
            session.scalars(stmt).all()

    @testing.variation("wpoly_type", ["star", "classes"])
    def test_with_polymorphic_sibling_classes_middle(
        self, wpoly_type: testing.Variation
    ):
        Employee, Manager, Engineer, JuniorEngineer = self.classes(
            "Employee", "Manager", "Engineer", "JuniorEngineer"
        )

        if wpoly_type.star:
            wp = with_polymorphic(Engineer, "*")
        elif wpoly_type.classes:
            wp = with_polymorphic(Engineer, [Engineer, JuniorEngineer])
        else:
            wpoly_type.fail()

        stmt = select(wp).order_by(wp.id)

        session = fixture_session()
        eq_(
            session.scalars(stmt).all(),
            [
                Engineer(engineer_info="engineer_info"),
                JuniorEngineer(engineer_info="junior info"),
            ],
        )

        # this raises, because we get rows that are not JuniorEngineer

        stmt = select(wp.JuniorEngineer).order_by(wp.id)
        with expect_raises_message(
            exc.InvalidRequestError,
            r"Row with identity key \(<.*Employee'>, .*\) can't be loaded "
            r"into an object; the polymorphic discriminator column "
            r"'employee.type' refers to Mapper\[Engineer\(.*\)\], "
            r"which is "
            r"not a sub-mapper of the requested "
            r"Mapper\[JuniorEngineer\(.*\)\]",
        ):
            session.scalars(stmt).all()

    @testing.variation("wpoly_type", ["star", "classes"])
    def test_with_polymorphic_sibling_columns(
        self, wpoly_type: testing.Variation
    ):
        Employee, Manager, Engineer, JuniorEngineer = self.classes(
            "Employee", "Manager", "Engineer", "JuniorEngineer"
        )

        if wpoly_type.star:
            wp = with_polymorphic(Employee, "*")
        elif wpoly_type.classes:
            wp = with_polymorphic(Employee, [Manager, Engineer])
        else:
            wpoly_type.fail()

        stmt = select(
            wp.name, wp.Manager.manager_data, wp.Engineer.engineer_info
        ).order_by(wp.id)

        session = fixture_session()

        eq_(
            session.execute(stmt).all(),
            [
                ("Employee 1", None, None),
                ("Manager 1", "manager data", None),
                ("Engineer 1", None, "engineer_info"),
                ("Junior Engineer 1", None, "junior info"),
                ("Boss 1", "boss data", None),
            ],
        )

    @testing.variation("wpoly_type", ["star", "classes"])
    def test_with_polymorphic_sibling_columns_middle(
        self, wpoly_type: testing.Variation
    ):
        Employee, Manager, Engineer, JuniorEngineer = self.classes(
            "Employee", "Manager", "Engineer", "JuniorEngineer"
        )

        if wpoly_type.star:
            wp = with_polymorphic(Engineer, "*")
        elif wpoly_type.classes:
            wp = with_polymorphic(Engineer, [JuniorEngineer])
        else:
            wpoly_type.fail()

        stmt = select(wp.name, wp.engineer_info, wp.JuniorEngineer.junior_name)

        session = fixture_session()

        eq_(
            session.execute(stmt).all(),
            [
                ("Engineer 1", "engineer_info", None),
                ("Junior Engineer 1", "junior info", "junior name"),
            ],
        )


class JoinedWPolyParityTest(
    WPolySingleJoinedParityTest, fixtures.DeclarativeMappedTest
):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Employee(ComparableEntity, Base):
            __tablename__ = "employee"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str]
            type: Mapped[str]

            __mapper_args__ = {
                "polymorphic_on": "type",
                "polymorphic_identity": "employee",
            }

        class Manager(Employee):
            __tablename__ = "manager"
            id = mapped_column(
                Integer, ForeignKey("employee.id"), primary_key=True
            )
            manager_data: Mapped[str] = mapped_column(nullable=True)

            __mapper_args__ = {
                "polymorphic_identity": "manager",
            }

        class Boss(Manager):
            __tablename__ = "boss"
            id = mapped_column(
                Integer, ForeignKey("manager.id"), primary_key=True
            )

            __mapper_args__ = {
                "polymorphic_identity": "boss",
            }

        class Engineer(Employee):
            __tablename__ = "engineer"
            id = mapped_column(
                Integer, ForeignKey("employee.id"), primary_key=True
            )
            engineer_info: Mapped[str] = mapped_column(nullable=True)

            __mapper_args__ = {
                "polymorphic_identity": "engineer",
            }

        class JuniorEngineer(Engineer):
            __tablename__ = "juniorengineer"
            id = mapped_column(
                Integer, ForeignKey("engineer.id"), primary_key=True
            )
            junior_name: Mapped[str] = mapped_column(nullable=True)
            __mapper_args__ = {
                "polymorphic_identity": "juniorengineer",
                "polymorphic_load": "inline",
            }


class SingleWPolyParityTest(
    WPolySingleJoinedParityTest, fixtures.DeclarativeMappedTest
):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Employee(ComparableEntity, Base):
            __tablename__ = "employee"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str]
            type: Mapped[str]

            __mapper_args__ = {
                "polymorphic_on": "type",
                "polymorphic_identity": "employee",
            }

        class Manager(Employee):
            manager_data: Mapped[str] = mapped_column(nullable=True)

            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "polymorphic_load": "inline",
            }

        class Boss(Manager):

            __mapper_args__ = {
                "polymorphic_identity": "boss",
                "polymorphic_load": "inline",
            }

        class Engineer(Employee):
            engineer_info: Mapped[str] = mapped_column(nullable=True)

            __mapper_args__ = {
                "polymorphic_identity": "engineer",
                "polymorphic_load": "inline",
            }

        class JuniorEngineer(Engineer):
            junior_name: Mapped[str] = mapped_column(nullable=True)

            __mapper_args__ = {
                "polymorphic_identity": "juniorengineer",
                "polymorphic_load": "inline",
            }


class RelationshipFromSingleTest(
    testing.AssertsCompiledSQL, fixtures.MappedTest
):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "employee",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
            Column("type", String(20)),
        )

        Table(
            "employee_stuff",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("employee_id", Integer, ForeignKey("employee.id")),
            Column("name", String(50)),
        )

    @classmethod
    def setup_mappers(cls):
        employee, employee_stuff, Employee, Stuff, Manager = (
            cls.tables.employee,
            cls.tables.employee_stuff,
            cls.classes.Employee,
            cls.classes.Stuff,
            cls.classes.Manager,
        )

        cls.mapper_registry.map_imperatively(
            Employee,
            employee,
            polymorphic_on=employee.c.type,
            polymorphic_identity="employee",
        )
        cls.mapper_registry.map_imperatively(
            Manager,
            inherits=Employee,
            polymorphic_identity="manager",
            properties={"stuff": relationship(Stuff)},
        )
        cls.mapper_registry.map_imperatively(Stuff, employee_stuff)

    @classmethod
    def setup_classes(cls):
        class Employee(cls.Comparable):
            pass

        class Manager(Employee):
            pass

        class Stuff(cls.Comparable):
            pass

    @classmethod
    def insert_data(cls, connection):
        Employee, Stuff, Manager = cls.classes("Employee", "Stuff", "Manager")
        s = Session(connection)

        s.add_all(
            [
                Employee(
                    name="e1", stuff=[Stuff(name="es1"), Stuff(name="es2")]
                ),
                Manager(
                    name="m1", stuff=[Stuff(name="ms1"), Stuff(name="ms2")]
                ),
            ]
        )
        s.commit()

    def test_subquery_load(self):
        Employee, Stuff, Manager = self.classes("Employee", "Stuff", "Manager")

        sess = fixture_session()

        with self.sql_execution_asserter(testing.db) as asserter:
            sess.query(Manager).options(subqueryload(Manager.stuff)).all()

        asserter.assert_(
            CompiledSQL(
                "SELECT employee.id AS employee_id, employee.name AS "
                "employee_name, employee.type AS employee_type "
                "FROM employee WHERE employee.type IN "
                "(__[POSTCOMPILE_type_1])",
                params=[{"type_1": ["manager"]}],
            ),
            CompiledSQL(
                "SELECT employee_stuff.id AS "
                "employee_stuff_id, employee_stuff.employee"
                "_id AS employee_stuff_employee_id, "
                "employee_stuff.name AS "
                "employee_stuff_name, anon_1.employee_id "
                "AS anon_1_employee_id FROM (SELECT "
                "employee.id AS employee_id FROM employee "
                "WHERE employee.type IN (__[POSTCOMPILE_type_1])) AS anon_1 "
                "JOIN employee_stuff ON anon_1.employee_id "
                "= employee_stuff.employee_id",
                params=[{"type_1": ["manager"]}],
            ),
        )


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

    def test_of_type(self):
        (
            JuniorEngineer,
            Company,
            companies,
            Manager,
            Employee,
            employees,
            Engineer,
        ) = (
            self.classes.JuniorEngineer,
            self.classes.Company,
            self.tables.companies,
            self.classes.Manager,
            self.classes.Employee,
            self.tables.employees,
            self.classes.Engineer,
        )

        self.mapper_registry.map_imperatively(
            Company,
            companies,
            properties={
                "employees": relationship(Employee, backref="company")
            },
        )
        self.mapper_registry.map_imperatively(
            Employee, employees, polymorphic_on=employees.c.type
        )
        self.mapper_registry.map_imperatively(
            Manager, inherits=Employee, polymorphic_identity="manager"
        )
        self.mapper_registry.map_imperatively(
            Engineer, inherits=Employee, polymorphic_identity="engineer"
        )
        self.mapper_registry.map_imperatively(
            JuniorEngineer,
            inherits=Engineer,
            polymorphic_identity="juniorengineer",
        )
        sess = fixture_session()

        c1 = Company(name="c1")
        c2 = Company(name="c2")

        m1 = Manager(name="Tom", manager_data="data1", company=c1)
        m2 = Manager(name="Tom2", manager_data="data2", company=c2)
        e1 = Engineer(
            name="Kurt", engineer_info="knows how to hack", company=c2
        )
        e2 = JuniorEngineer(name="Ed", engineer_info="oh that ed", company=c1)
        sess.add_all([c1, c2, m1, m2, e1, e2])
        sess.commit()
        sess.expunge_all()
        eq_(
            sess.query(Company)
            .filter(Company.employees.of_type(JuniorEngineer).any())
            .all(),
            [Company(name="c1")],
        )

        eq_(
            sess.query(Company)
            .join(Company.employees.of_type(JuniorEngineer))
            .all(),
            [Company(name="c1")],
        )

    def test_join_explicit_onclause_no_discriminator(self):
        # test issue #3462
        Company, Employee, Engineer = (
            self.classes.Company,
            self.classes.Employee,
            self.classes.Engineer,
        )
        companies, employees = self.tables.companies, self.tables.employees

        self.mapper_registry.map_imperatively(
            Company,
            companies,
            properties={"employees": relationship(Employee)},
        )
        self.mapper_registry.map_imperatively(Employee, employees)
        self.mapper_registry.map_imperatively(Engineer, inherits=Employee)

        sess = fixture_session()
        self.assert_compile(
            sess.query(Company, Engineer.name).join(
                Engineer, Company.company_id == Engineer.company_id
            ),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name, "
            "employees.name AS employees_name "
            "FROM companies JOIN "
            "employees ON companies.company_id = employees.company_id",
        )

    def test_outer_join_prop(self):
        Company, Employee, Engineer = (
            self.classes.Company,
            self.classes.Employee,
            self.classes.Engineer,
        )
        companies, employees = self.tables.companies, self.tables.employees

        self.mapper_registry.map_imperatively(
            Company,
            companies,
            properties={"engineers": relationship(Engineer)},
        )
        self.mapper_registry.map_imperatively(
            Employee, employees, polymorphic_on=employees.c.type
        )
        self.mapper_registry.map_imperatively(
            Engineer, inherits=Employee, polymorphic_identity="engineer"
        )

        sess = fixture_session()
        self.assert_compile(
            sess.query(Company, Engineer.name).outerjoin(Company.engineers),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name, "
            "employees.name AS employees_name "
            "FROM companies LEFT OUTER JOIN employees ON companies.company_id "
            "= employees.company_id "
            "AND employees.type IN (__[POSTCOMPILE_type_1])",
        )

    def test_outer_join_prop_alias(self):
        Company, Employee, Engineer = (
            self.classes.Company,
            self.classes.Employee,
            self.classes.Engineer,
        )
        companies, employees = self.tables.companies, self.tables.employees

        self.mapper_registry.map_imperatively(
            Company,
            companies,
            properties={"engineers": relationship(Engineer)},
        )
        self.mapper_registry.map_imperatively(
            Employee, employees, polymorphic_on=employees.c.type
        )
        self.mapper_registry.map_imperatively(
            Engineer, inherits=Employee, polymorphic_identity="engineer"
        )

        eng_alias = aliased(Engineer)
        sess = fixture_session()
        self.assert_compile(
            sess.query(Company, eng_alias.name).outerjoin(
                eng_alias, Company.engineers
            ),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name, employees_1.name AS "
            "employees_1_name FROM companies LEFT OUTER "
            "JOIN employees AS employees_1 ON companies.company_id "
            "= employees_1.company_id "
            "AND employees_1.type IN (__[POSTCOMPILE_type_1])",
        )

    def test_outer_join_literal_onclause(self):
        Company, Employee, Engineer = (
            self.classes.Company,
            self.classes.Employee,
            self.classes.Engineer,
        )
        companies, employees = self.tables.companies, self.tables.employees

        self.mapper_registry.map_imperatively(
            Company,
            companies,
            properties={"engineers": relationship(Engineer)},
        )
        self.mapper_registry.map_imperatively(
            Employee, employees, polymorphic_on=employees.c.type
        )
        self.mapper_registry.map_imperatively(
            Engineer, inherits=Employee, polymorphic_identity="engineer"
        )

        sess = fixture_session()
        self.assert_compile(
            sess.query(Company, Engineer).outerjoin(
                Engineer, Company.company_id == Engineer.company_id
            ),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name, "
            "employees.employee_id AS employees_employee_id, "
            "employees.name AS employees_name, "
            "employees.manager_data AS employees_manager_data, "
            "employees.engineer_info AS employees_engineer_info, "
            "employees.type AS employees_type, "
            "employees.company_id AS employees_company_id FROM companies "
            "LEFT OUTER JOIN employees ON "
            "companies.company_id = employees.company_id "
            "AND employees.type IN (__[POSTCOMPILE_type_1])",
        )

    def test_outer_join_literal_onclause_alias(self):
        Company, Employee, Engineer = (
            self.classes.Company,
            self.classes.Employee,
            self.classes.Engineer,
        )
        companies, employees = self.tables.companies, self.tables.employees

        self.mapper_registry.map_imperatively(
            Company,
            companies,
            properties={"engineers": relationship(Engineer)},
        )
        self.mapper_registry.map_imperatively(
            Employee, employees, polymorphic_on=employees.c.type
        )
        self.mapper_registry.map_imperatively(
            Engineer, inherits=Employee, polymorphic_identity="engineer"
        )

        eng_alias = aliased(Engineer)
        sess = fixture_session()
        self.assert_compile(
            sess.query(Company, eng_alias).outerjoin(
                eng_alias, Company.company_id == eng_alias.company_id
            ),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name, "
            "employees_1.employee_id AS employees_1_employee_id, "
            "employees_1.name AS employees_1_name, "
            "employees_1.manager_data AS employees_1_manager_data, "
            "employees_1.engineer_info AS employees_1_engineer_info, "
            "employees_1.type AS employees_1_type, "
            "employees_1.company_id AS employees_1_company_id "
            "FROM companies LEFT OUTER JOIN employees AS employees_1 ON "
            "companies.company_id = employees_1.company_id "
            "AND employees_1.type IN (__[POSTCOMPILE_type_1])",
        )

    def test_outer_join_no_onclause(self):
        Company, Employee, Engineer = (
            self.classes.Company,
            self.classes.Employee,
            self.classes.Engineer,
        )
        companies, employees = self.tables.companies, self.tables.employees

        self.mapper_registry.map_imperatively(
            Company,
            companies,
            properties={"engineers": relationship(Engineer)},
        )
        self.mapper_registry.map_imperatively(
            Employee, employees, polymorphic_on=employees.c.type
        )
        self.mapper_registry.map_imperatively(
            Engineer, inherits=Employee, polymorphic_identity="engineer"
        )

        sess = fixture_session()
        self.assert_compile(
            sess.query(Company, Engineer).outerjoin(Engineer),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name, "
            "employees.employee_id AS employees_employee_id, "
            "employees.name AS employees_name, "
            "employees.manager_data AS employees_manager_data, "
            "employees.engineer_info AS employees_engineer_info, "
            "employees.type AS employees_type, "
            "employees.company_id AS employees_company_id "
            "FROM companies LEFT OUTER JOIN employees ON "
            "companies.company_id = employees.company_id "
            "AND employees.type IN (__[POSTCOMPILE_type_1])",
        )

    def test_outer_join_no_onclause_alias(self):
        Company, Employee, Engineer = (
            self.classes.Company,
            self.classes.Employee,
            self.classes.Engineer,
        )
        companies, employees = self.tables.companies, self.tables.employees

        self.mapper_registry.map_imperatively(
            Company,
            companies,
            properties={"engineers": relationship(Engineer)},
        )
        self.mapper_registry.map_imperatively(
            Employee, employees, polymorphic_on=employees.c.type
        )
        self.mapper_registry.map_imperatively(
            Engineer, inherits=Employee, polymorphic_identity="engineer"
        )

        eng_alias = aliased(Engineer)
        sess = fixture_session()
        self.assert_compile(
            sess.query(Company, eng_alias).outerjoin(eng_alias),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name, "
            "employees_1.employee_id AS employees_1_employee_id, "
            "employees_1.name AS employees_1_name, "
            "employees_1.manager_data AS employees_1_manager_data, "
            "employees_1.engineer_info AS employees_1_engineer_info, "
            "employees_1.type AS employees_1_type, "
            "employees_1.company_id AS employees_1_company_id "
            "FROM companies LEFT OUTER JOIN employees AS employees_1 ON "
            "companies.company_id = employees_1.company_id "
            "AND employees_1.type IN (__[POSTCOMPILE_type_1])",
        )

    def test_correlated_column_select(self):
        Company, Employee, Engineer = (
            self.classes.Company,
            self.classes.Employee,
            self.classes.Engineer,
        )
        companies, employees = self.tables.companies, self.tables.employees

        self.mapper_registry.map_imperatively(Company, companies)
        self.mapper_registry.map_imperatively(
            Employee,
            employees,
            polymorphic_on=employees.c.type,
            properties={"company": relationship(Company)},
        )
        self.mapper_registry.map_imperatively(
            Engineer, inherits=Employee, polymorphic_identity="engineer"
        )

        sess = fixture_session()
        engineer_count = (
            sess.query(func.count(Engineer.employee_id))
            .select_from(Engineer)
            .filter(Engineer.company_id == Company.company_id)
            .correlate(Company)
            .scalar_subquery()
        )

        self.assert_compile(
            sess.query(Company.company_id, engineer_count),
            "SELECT companies.company_id AS companies_company_id, "
            "(SELECT count(employees.employee_id) AS count_1 "
            "FROM employees WHERE employees.company_id = "
            "companies.company_id "
            "AND employees.type IN (__[POSTCOMPILE_type_1])) AS anon_1 "
            "FROM companies",
        )

    def test_no_aliasing_from_overlap(self):
        # test [ticket:3233]

        Company, Employee, Engineer, Manager = (
            self.classes.Company,
            self.classes.Employee,
            self.classes.Engineer,
            self.classes.Manager,
        )

        companies, employees = self.tables.companies, self.tables.employees

        self.mapper_registry.map_imperatively(
            Company,
            companies,
            properties={
                "employees": relationship(Employee, backref="company")
            },
        )
        self.mapper_registry.map_imperatively(
            Employee, employees, polymorphic_on=employees.c.type
        )
        self.mapper_registry.map_imperatively(
            Engineer, inherits=Employee, polymorphic_identity="engineer"
        )
        self.mapper_registry.map_imperatively(
            Manager, inherits=Employee, polymorphic_identity="manager"
        )

        s = fixture_session()

        q1 = (
            s.query(Engineer)
            .join(Engineer.company)
            .join(Manager, Company.employees)
        )

        q2 = (
            s.query(Engineer)
            .join(Engineer.company)
            .join(Manager, Company.company_id == Manager.company_id)
        )

        q3 = (
            s.query(Engineer)
            .join(Engineer.company)
            .join(Manager, Company.employees.of_type(Manager))
        )

        q4 = (
            s.query(Engineer)
            .join(Company, Company.company_id == Engineer.company_id)
            .join(Manager, Company.employees.of_type(Manager))
        )

        q5 = (
            s.query(Engineer)
            .join(Company, Company.company_id == Engineer.company_id)
            .join(Manager, Company.company_id == Manager.company_id)
        )

        # note that the query is incorrect SQL; we JOIN to
        # employees twice.   However, this is what's expected so we seek
        # to be consistent; previously, aliasing would sneak in due to the
        # nature of the "left" side.
        for q in [q1, q2, q3, q4, q5]:
            self.assert_compile(
                q,
                "SELECT employees.employee_id AS employees_employee_id, "
                "employees.name AS employees_name, "
                "employees.manager_data AS employees_manager_data, "
                "employees.engineer_info AS employees_engineer_info, "
                "employees.type AS employees_type, "
                "employees.company_id AS employees_company_id "
                "FROM employees JOIN companies "
                "ON companies.company_id = employees.company_id "
                "JOIN employees "
                "ON companies.company_id = employees.company_id "
                "AND employees.type IN (__[POSTCOMPILE_type_1]) "
                "WHERE employees.type IN (__[POSTCOMPILE_type_2])",
            )

    def test_relationship_to_subclass(self):
        (
            JuniorEngineer,
            Company,
            companies,
            Manager,
            Employee,
            employees,
            Engineer,
        ) = (
            self.classes.JuniorEngineer,
            self.classes.Company,
            self.tables.companies,
            self.classes.Manager,
            self.classes.Employee,
            self.tables.employees,
            self.classes.Engineer,
        )

        self.mapper_registry.map_imperatively(
            Company,
            companies,
            properties={
                "engineers": relationship(Engineer, back_populates="company")
            },
        )
        self.mapper_registry.map_imperatively(
            Employee,
            employees,
            polymorphic_on=employees.c.type,
            properties={"company": relationship(Company)},
        )
        self.mapper_registry.map_imperatively(
            Manager, inherits=Employee, polymorphic_identity="manager"
        )
        self.mapper_registry.map_imperatively(
            Engineer, inherits=Employee, polymorphic_identity="engineer"
        )
        self.mapper_registry.map_imperatively(
            JuniorEngineer,
            inherits=Engineer,
            polymorphic_identity="juniorengineer",
        )
        sess = fixture_session()

        c1 = Company(name="c1")
        c2 = Company(name="c2")

        m1 = Manager(name="Tom", manager_data="data1", company=c1)
        m2 = Manager(name="Tom2", manager_data="data2", company=c2)
        e1 = Engineer(
            name="Kurt", engineer_info="knows how to hack", company=c2
        )
        e2 = JuniorEngineer(name="Ed", engineer_info="oh that ed", company=c1)
        sess.add_all([c1, c2, m1, m2, e1, e2])
        sess.commit()

        eq_(c1.engineers, [e2])
        eq_(c2.engineers, [e1])

        sess.expunge_all()
        eq_(
            sess.query(Company).order_by(Company.name).all(),
            [
                Company(name="c1", engineers=[JuniorEngineer(name="Ed")]),
                Company(name="c2", engineers=[Engineer(name="Kurt")]),
            ],
        )

        # eager load join should limit to only "Engineer"
        sess.expunge_all()
        eq_(
            sess.query(Company)
            .options(joinedload(Company.engineers))
            .order_by(Company.name)
            .all(),
            [
                Company(name="c1", engineers=[JuniorEngineer(name="Ed")]),
                Company(name="c2", engineers=[Engineer(name="Kurt")]),
            ],
        )

        # join() to Company.engineers, Employee as the requested entity
        sess.expunge_all()
        eq_(
            sess.query(Company, Employee)
            .join(Company.engineers)
            .order_by(Company.name)
            .all(),
            [
                (Company(name="c1"), JuniorEngineer(name="Ed")),
                (Company(name="c2"), Engineer(name="Kurt")),
            ],
        )

        # join() to Company.engineers, Engineer as the requested entity.
        # this actually applies the IN criterion twice which is less than
        # ideal.
        sess.expunge_all()
        eq_(
            sess.query(Company, Engineer)
            .join(Company.engineers)
            .order_by(Company.name)
            .all(),
            [
                (Company(name="c1"), JuniorEngineer(name="Ed")),
                (Company(name="c2"), Engineer(name="Kurt")),
            ],
        )

        # join() to Company.engineers without any Employee/Engineer entity
        sess.expunge_all()
        eq_(
            sess.query(Company)
            .join(Company.engineers)
            .filter(Engineer.name.in_(["Tom", "Kurt"]))
            .all(),
            [Company(name="c2")],
        )

        # this however fails as it does not limit the subtypes to just
        # "Engineer". with joins constructed by filter(), we seem to be
        # following a policy where we don't try to make decisions on how to
        # join to the target class, whereas when using join() we seem to have
        # a lot more capabilities. we might want to document
        # "advantages of join() vs. straight filtering", or add a large
        # section to "inheritance" laying out all the various behaviors Query
        # has.
        @testing.fails_on_everything_except()
        def go():
            sess.expunge_all()
            eq_(
                sess.query(Company)
                .filter(Company.company_id == Engineer.company_id)
                .filter(Engineer.name.in_(["Tom", "Kurt"]))
                .all(),
                [Company(name="c2")],
            )

        go()


class ManyToManyToSingleTest(fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = "default"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
        )
        Table(
            "m2m",
            metadata,
            Column(
                "parent_id", Integer, ForeignKey("parent.id"), primary_key=True
            ),
            Column(
                "child_id", Integer, ForeignKey("child.id"), primary_key=True
            ),
        )
        Table(
            "child",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("discriminator", String(20)),
            Column("name", String(20)),
        )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Comparable):
            pass

        class Child(cls.Comparable):
            pass

        class SubChild1(Child):
            pass

        class SubChild2(Child):
            pass

    @classmethod
    def setup_mappers(cls):
        cls.mapper_registry.map_imperatively(
            cls.classes.Parent,
            cls.tables.parent,
            properties={
                "s1": relationship(
                    cls.classes.SubChild1,
                    secondary=cls.tables.m2m,
                    uselist=False,
                ),
                "s2": relationship(
                    cls.classes.SubChild2, secondary=cls.tables.m2m
                ),
            },
        )
        cls.mapper_registry.map_imperatively(
            cls.classes.Child,
            cls.tables.child,
            polymorphic_on=cls.tables.child.c.discriminator,
        )
        cls.mapper_registry.map_imperatively(
            cls.classes.SubChild1,
            inherits=cls.classes.Child,
            polymorphic_identity="sub1",
        )
        cls.mapper_registry.map_imperatively(
            cls.classes.SubChild2,
            inherits=cls.classes.Child,
            polymorphic_identity="sub2",
        )

    @classmethod
    def insert_data(cls, connection):
        Parent = cls.classes.Parent
        SubChild1 = cls.classes.SubChild1
        SubChild2 = cls.classes.SubChild2
        s = Session(connection)
        s.add_all(
            [
                Parent(
                    s1=SubChild1(name="sc1_1"),
                    s2=[SubChild2(name="sc2_1"), SubChild2(name="sc2_2")],
                )
            ]
        )
        s.commit()

    def test_eager_join(self):
        Parent = self.classes.Parent
        SubChild1 = self.classes.SubChild1

        s = fixture_session()

        p1 = s.query(Parent).options(joinedload(Parent.s1)).all()[0]
        eq_(p1.__dict__["s1"], SubChild1(name="sc1_1"))

    def test_manual_join(self):
        Parent = self.classes.Parent
        Child = self.classes.Child
        SubChild1 = self.classes.SubChild1

        s = fixture_session()

        p1, c1 = s.query(Parent, Child).outerjoin(Parent.s1).all()[0]
        eq_(c1, SubChild1(name="sc1_1"))

    def test_assert_join_sql(self):
        Parent = self.classes.Parent
        Child = self.classes.Child

        s = fixture_session()

        self.assert_compile(
            s.query(Parent, Child).outerjoin(Parent.s1),
            "SELECT parent.id AS parent_id, child.id AS child_id, "
            "child.discriminator AS child_discriminator, "
            "child.name AS child_name "
            "FROM parent LEFT OUTER JOIN (m2m AS m2m_1 "
            "JOIN child ON child.id = m2m_1.child_id "
            "AND child.discriminator IN (__[POSTCOMPILE_discriminator_1])) "
            "ON parent.id = m2m_1.parent_id",
        )

    def test_assert_joinedload_sql(self):
        Parent = self.classes.Parent

        s = fixture_session()

        self.assert_compile(
            s.query(Parent).options(joinedload(Parent.s1)),
            "SELECT parent.id AS parent_id, child_1.id AS child_1_id, "
            "child_1.discriminator AS child_1_discriminator, "
            "child_1.name AS child_1_name "
            "FROM parent LEFT OUTER JOIN "
            "(m2m AS m2m_1 JOIN child AS child_1 "
            "ON child_1.id = m2m_1.child_id AND child_1.discriminator "
            "IN (__[POSTCOMPILE_discriminator_1])) "
            "ON parent.id = m2m_1.parent_id",
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
        class Person(ComparableEntity):
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
            wp = with_polymorphic(Person, "*")
            eq_(
                sess.query(wp).order_by(wp.person_id).all(),
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

    def test_wpoly_single_inh_subclass(self):
        poly = with_polymorphic(
            self.classes.Employee,
            [self.classes.Boss, self.classes.Manager, self.classes.Engineer],
            self._with_poly_fixture(),
        )
        s = fixture_session()
        q = s.query(poly.Boss)
        self.assert_compile(
            q,
            "SELECT "
            "anon_1.employee_id AS anon_1_employee_id, "
            "anon_1.employee_name AS anon_1_employee_name, "
            "anon_1.employee_type AS anon_1_employee_type, "
            "anon_1.manager_manager_data AS anon_1_manager_manager_data "
            "FROM "
            "(SELECT "
            "employee.id AS employee_id, employee.type AS employee_type, "
            "employee.name AS employee_name, "
            "manager.manager_data AS manager_manager_data, "
            "NULL AS engineer_info, NULL AS manager_id FROM employee "
            "JOIN manager ON employee.id = manager.id "
            "UNION ALL "
            "SELECT employee.id AS employee_id, "
            "employee.type AS employee_type, "
            "employee.name AS employee_name, NULL AS manager_data, "
            "engineer.engineer_info AS engineer_engineer_info, "
            "engineer.manager_id AS engineer_manager_id "
            "FROM employee JOIN engineer ON employee.id = engineer.id) "
            "AS anon_1",
        )

    def test_query_wpoly_single_inh_subclass(self):
        Boss = self.classes.Boss

        poly = self._with_poly_fixture()

        s = fixture_session()

        wp = with_polymorphic(Boss, [], poly)
        q = s.query(wp)
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

    @testing.combinations((True,), (False,), argnames="autoalias")
    def test_single_inh_subclass_join_joined_inh_subclass(self, autoalias):
        Boss, Engineer = self.classes("Boss", "Engineer")
        s = fixture_session()

        if autoalias:
            q = s.query(Boss).join(Engineer, Engineer.manager_id == Boss.id)
        else:
            e1 = aliased(Engineer, flat=True)
            q = s.query(Boss).join(e1, e1.manager_id == Boss.id)

        with (
            _aliased_join_warning(r"Mapper\[Engineer\(engineer\)\]")
            if autoalias
            else nullcontext()
        ):
            self.assert_compile(
                q,
                "SELECT manager.id AS manager_id, employee.id AS employee_id, "
                "employee.name AS employee_name, "
                "employee.type AS employee_type, "
                "manager.manager_data AS manager_manager_data "
                "FROM employee JOIN manager ON employee.id = manager.id "
                "JOIN (employee AS employee_1 JOIN engineer AS engineer_1 "
                "ON employee_1.id = engineer_1.id) "
                "ON engineer_1.manager_id = manager.id "
                "WHERE employee.type IN (__[POSTCOMPILE_type_1])",
            )

    def test_single_inh_subclass_join_wpoly_joined_inh_subclass(self):
        Boss = self.classes.Boss
        poly = with_polymorphic(
            self.classes.Employee,
            [self.classes.Boss, self.classes.Manager, self.classes.Engineer],
            self._with_poly_fixture(),
        )

        s = fixture_session()

        q = s.query(Boss).join(
            poly.Engineer, poly.Engineer.manager_id == Boss.id
        )

        self.assert_compile(
            q,
            "SELECT manager.id AS manager_id, employee.id AS employee_id, "
            "employee.name AS employee_name, employee.type AS employee_type, "
            "manager.manager_data AS manager_manager_data "
            "FROM employee JOIN manager ON employee.id = manager.id "
            "JOIN (SELECT employee.id AS employee_id, "
            "employee.type AS employee_type, employee.name AS employee_name, "
            "manager.manager_data AS manager_manager_data, "
            "NULL AS engineer_info, NULL AS manager_id "
            "FROM employee JOIN manager ON employee.id = manager.id "
            "UNION ALL "
            "SELECT employee.id AS employee_id, "
            "employee.type AS employee_type, employee.name AS employee_name, "
            "NULL AS manager_data, "
            "engineer.engineer_info AS engineer_engineer_info, "
            "engineer.manager_id AS engineer_manager_id "
            "FROM employee "
            "JOIN engineer ON employee.id = engineer.id) AS anon_1 "
            "ON anon_1.manager_id = manager.id "
            "WHERE employee.type IN (__[POSTCOMPILE_type_1])",
        )

    @testing.combinations((True,), (False,), argnames="autoalias")
    def test_joined_inh_subclass_join_single_inh_subclass(self, autoalias):
        Engineer = self.classes.Engineer
        Boss = self.classes.Boss
        s = fixture_session()

        if autoalias:
            q = s.query(Engineer).join(Boss, Engineer.manager_id == Boss.id)
        else:
            b1 = aliased(Boss, flat=True)
            q = s.query(Engineer).join(b1, Engineer.manager_id == b1.id)

        with (
            _aliased_join_warning(r"Mapper\[Boss\(manager\)\]")
            if autoalias
            else nullcontext()
        ):
            self.assert_compile(
                q,
                "SELECT engineer.id AS engineer_id, "
                "employee.id AS employee_id, "
                "employee.name AS employee_name, "
                "employee.type AS employee_type, "
                "engineer.engineer_info AS engineer_engineer_info, "
                "engineer.manager_id AS engineer_manager_id "
                "FROM employee JOIN engineer ON employee.id = engineer.id "
                "JOIN (employee AS employee_1 JOIN manager AS manager_1 "
                "ON employee_1.id = manager_1.id) "
                "ON engineer.manager_id = manager_1.id "
                "AND employee_1.type IN (__[POSTCOMPILE_type_1])",
            )


class EagerDefaultEvalTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls, with_polymorphic=None, include_sub_defaults=False):
        Base = cls.DeclarativeBasic

        class Foo(Base):
            __tablename__ = "foo"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            type = Column(String(50))
            created_at = Column(Integer, server_default="5")

            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "foo",
                "eager_defaults": True,
                "with_polymorphic": with_polymorphic,
            }

        class Bar(Foo):
            bar = Column(String(50))
            if include_sub_defaults:
                bat = Column(Integer, server_default="10")

            __mapper_args__ = {"polymorphic_identity": "bar"}

    def test_persist_foo(self):
        Foo = self.classes.Foo

        foo = Foo()

        session = fixture_session()
        session.add(foo)
        session.flush()

        eq_(foo.__dict__["created_at"], 5)

        assert "bat" not in foo.__dict__

        session.close()

    def test_persist_bar(self):
        Bar = self.classes.Bar
        bar = Bar()
        session = fixture_session()
        session.add(bar)
        session.flush()

        eq_(bar.__dict__["created_at"], 5)

        if "bat" in inspect(Bar).attrs:
            eq_(bar.__dict__["bat"], 10)

        session.close()


class EagerDefaultEvalTestSubDefaults(EagerDefaultEvalTest):
    @classmethod
    def setup_classes(cls):
        super().setup_classes(include_sub_defaults=True)


class EagerDefaultEvalTestPolymorphic(EagerDefaultEvalTest):
    @classmethod
    def setup_classes(cls):
        super().setup_classes(with_polymorphic="*")


class ColExprTest(AssertsCompiledSQL, fixtures.TestBase):
    def test_discrim_on_column_prop(self, registry):
        Base = registry.generate_base()

        class Employee(Base):
            __tablename__ = "employee"
            id = Column(Integer, primary_key=True)
            type = Column(String(20))

            __mapper_args__ = {
                "polymorphic_on": "type",
                "polymorphic_identity": "employee",
            }

        class Engineer(Employee):
            __mapper_args__ = {"polymorphic_identity": "engineer"}

        class Company(Base):
            __tablename__ = "company"
            id = Column(Integer, primary_key=True)

            max_engineer_id = column_property(
                select(func.max(Engineer.id)).scalar_subquery()
            )

        self.assert_compile(
            select(Company.max_engineer_id),
            "SELECT (SELECT max(employee.id) AS max_1 FROM employee "
            "WHERE employee.type IN (__[POSTCOMPILE_type_1])) AS anon_1",
        )


class AbstractPolymorphicTest(
    AssertsCompiledSQL, fixtures.DeclarativeMappedTest
):
    """test new polymorphic_abstract feature added as of #9060"""

    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Company(Base):
            __tablename__ = "company"
            id = Column(Integer, primary_key=True)

            executives: Mapped[List[Executive]] = relationship()
            technologists: Mapped[List[Technologist]] = relationship()

        class Employee(Base):
            __tablename__ = "employee"
            id: Mapped[int] = mapped_column(primary_key=True)
            company_id: Mapped[int] = mapped_column(ForeignKey("company.id"))
            name: Mapped[str]
            type: Mapped[str]

            __mapper_args__ = {
                "polymorphic_on": "type",
            }

        class Executive(Employee):
            """An executive of the company"""

            executive_background: Mapped[str] = mapped_column(nullable=True)
            __mapper_args__ = {"polymorphic_abstract": True}

        class Technologist(Employee):
            """An employee who works with technology"""

            competencies: Mapped[str] = mapped_column(nullable=True)
            __mapper_args__ = {"polymorphic_abstract": True}

        class Manager(Executive):
            """a manager"""

            __mapper_args__ = {"polymorphic_identity": "manager"}

        class Principal(Executive):
            """a principal of the company"""

            __mapper_args__ = {"polymorphic_identity": "principal"}

        class Engineer(Technologist):
            """an engineer"""

            __mapper_args__ = {"polymorphic_identity": "engineer"}

        class SysAdmin(Technologist):
            """a systems administrator"""

            __mapper_args__ = {"polymorphic_identity": "sysadmin"}

    def test_select_against_abstract(self):
        Technologist = self.classes.Technologist

        self.assert_compile(
            select(Technologist).where(
                Technologist.competencies.like("%Java%")
            ),
            "SELECT employee.id, employee.company_id, employee.name, "
            "employee.type, employee.competencies FROM employee "
            "WHERE employee.competencies LIKE :competencies_1 "
            "AND employee.type IN (:type_1_1, :type_1_2)",
            checkparams={
                "competencies_1": "%Java%",
                "type_1_1": "engineer",
                "type_1_2": "sysadmin",
            },
            render_postcompile=True,
        )

    def test_relationship_join(self):
        Technologist = self.classes.Technologist
        Company = self.classes.Company

        self.assert_compile(
            select(Company)
            .join(Company.technologists)
            .where(Technologist.competencies.like("%Java%")),
            "SELECT company.id FROM company JOIN employee "
            "ON company.id = employee.company_id AND employee.type "
            "IN (:type_1_1, :type_1_2) WHERE employee.competencies "
            "LIKE :competencies_1",
            checkparams={
                "competencies_1": "%Java%",
                "type_1_1": "engineer",
                "type_1_2": "sysadmin",
            },
            render_postcompile=True,
        )

    @testing.fixture
    def data_fixture(self, connection):
        Company = self.classes.Company
        Engineer = self.classes.Engineer
        Manager = self.classes.Manager
        Principal = self.classes.Principal

        with Session(connection) as sess:
            sess.add(
                Company(
                    technologists=[
                        Engineer(name="e1", competencies="Java programming")
                    ],
                    executives=[
                        Manager(name="m1", executive_background="eb1"),
                        Principal(name="p1", executive_background="eb2"),
                    ],
                )
            )
            sess.flush()

            yield sess

    def test_relationship_join_w_eagerload(self, data_fixture):
        Company = self.classes.Company
        Technologist = self.classes.Technologist

        session = data_fixture

        with self.sql_execution_asserter() as asserter:
            session.scalars(
                select(Company)
                .join(Company.technologists)
                .where(Technologist.competencies.ilike("%java%"))
                .options(selectinload(Company.executives))
            ).all()

        asserter.assert_(
            CompiledSQL(
                "SELECT company.id FROM company JOIN employee ON "
                "company.id = employee.company_id AND employee.type "
                "IN (__[POSTCOMPILE_type_1]) WHERE "
                "lower(employee.competencies) LIKE lower(:competencies_1)",
                [
                    {
                        "type_1": ["engineer", "sysadmin"],
                        "competencies_1": "%java%",
                    }
                ],
            ),
            CompiledSQL(
                "SELECT employee.company_id AS employee_company_id, "
                "employee.id AS employee_id, employee.name AS employee_name, "
                "employee.type AS employee_type, "
                "employee.executive_background AS "
                "employee_executive_background "
                "FROM employee WHERE employee.company_id "
                "IN (__[POSTCOMPILE_primary_keys]) "
                "AND employee.type IN (__[POSTCOMPILE_type_1])",
                [
                    {
                        "primary_keys": [mock.ANY],
                        "type_1": ["manager", "principal"],
                    }
                ],
            ),
        )

    @testing.variation("given_type", ["none", "invalid", "valid"])
    def test_no_instantiate(self, given_type):
        Technologist = self.classes.Technologist

        with expect_raises_message(
            exc.InvalidRequestError,
            r"Can't instantiate class for Mapper\[Technologist\(employee\)\]; "
            r"mapper is marked polymorphic_abstract=True",
        ):
            if given_type.none:
                Technologist()
            elif given_type.invalid:
                Technologist(type="madeup")
            elif given_type.valid:
                Technologist(type="engineer")
            else:
                given_type.fail()

    def test_not_supported_wo_poly_inheriting(self, decl_base):
        class MyClass(decl_base):
            __tablename__ = "my_table"

            id: Mapped[int] = mapped_column(primary_key=True)

        with expect_raises_message(
            exc.InvalidRequestError,
            "The Mapper.polymorphic_abstract parameter may only be used "
            "on a mapper hierarchy which includes the Mapper.polymorphic_on",
        ):

            class Nope(MyClass):
                __mapper_args__ = {"polymorphic_abstract": True}

    def test_not_supported_wo_poly_base(self, decl_base):
        with expect_raises_message(
            exc.InvalidRequestError,
            "The Mapper.polymorphic_abstract parameter may only be used "
            "on a mapper hierarchy which includes the Mapper.polymorphic_on",
        ):

            class Nope(decl_base):
                __tablename__ = "my_table"

                id: Mapped[int] = mapped_column(primary_key=True)
                __mapper_args__ = {"polymorphic_abstract": True}
