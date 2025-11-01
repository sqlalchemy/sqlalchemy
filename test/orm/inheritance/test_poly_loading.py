from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import union
from sqlalchemy.orm import aliased
from sqlalchemy.orm import backref
from sqlalchemy.orm import column_property
from sqlalchemy.orm import composite
from sqlalchemy.orm import defaultload
from sqlalchemy.orm import immediateload
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import lazyload
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectin_polymorphic
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.orm.interfaces import CompileStateOption
from sqlalchemy.sql.selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.testing import assertsql
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import expect_raises_message
from sqlalchemy.testing.assertsql import AllOf
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.assertsql import Conditional
from sqlalchemy.testing.assertsql import EachOf
from sqlalchemy.testing.assertsql import Or
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from ._poly_fixtures import _Polymorphic
from ._poly_fixtures import Company
from ._poly_fixtures import Engineer
from ._poly_fixtures import GeometryFixtureBase
from ._poly_fixtures import Manager
from ._poly_fixtures import Person


class BaseAndSubFixture:
    use_options = False

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            adata = Column(String(50))
            bs = relationship("B")
            type = Column(String(50))

            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "a",
            }

        class ASub(A):
            __tablename__ = "asub"
            id = Column(ForeignKey("a.id"), primary_key=True)
            asubdata = Column(String(50))

            cs = relationship("C")

            if cls.use_options:
                __mapper_args__ = {"polymorphic_identity": "asub"}
            else:
                __mapper_args__ = {
                    "polymorphic_load": "selectin",
                    "polymorphic_identity": "asub",
                }

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))

        class C(Base):
            __tablename__ = "c"
            id = Column(Integer, primary_key=True)
            a_sub_id = Column(ForeignKey("asub.id"))

    @classmethod
    def insert_data(cls, connection):
        A, B, ASub, C = cls.classes("A", "B", "ASub", "C")
        s = Session(connection)
        s.add(A(id=1, adata="adata", bs=[B(), B()]))
        s.add(
            ASub(
                id=2,
                adata="adata",
                asubdata="asubdata",
                bs=[B(), B()],
                cs=[C(), C()],
            )
        )

        s.commit()

    def _run_query(self, q):
        ASub = self.classes.ASub
        for a in q:
            a.bs
            if isinstance(a, ASub):
                a.cs

    def _assert_all_selectin(self, q):
        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT a.id AS a_id, a.adata AS a_adata, "
                "a.type AS a_type FROM a ORDER BY a.id",
                {},
            ),
            AllOf(
                EachOf(
                    CompiledSQL(
                        "SELECT asub.id AS asub_id, a.id AS a_id, "
                        "a.type AS a_type, "
                        "asub.asubdata AS asub_asubdata FROM a JOIN asub "
                        "ON a.id = asub.id "
                        "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) "
                        "ORDER BY a.id",
                        {"primary_keys": [2]},
                    ),
                    CompiledSQL(
                        # note this links c.a_sub_id to a.id, even though
                        # primaryjoin is to asub.id.  this is because the
                        # cols a.id / asub.id are listed in the mapper's
                        # equivalent_columns so they are guaranteed to store
                        # the same value.
                        "SELECT c.a_sub_id, c.id "
                        "FROM c WHERE c.a_sub_id "
                        "IN (__[POSTCOMPILE_primary_keys])",
                        {"primary_keys": [2]},
                    ),
                ),
                CompiledSQL(
                    "SELECT b.a_id, b.id FROM b "
                    "WHERE b.a_id IN (__[POSTCOMPILE_primary_keys])",
                    {"primary_keys": [1, 2]},
                ),
            ),
        )

        self.assert_sql_execution(testing.db, lambda: self._run_query(result))


class LoadBaseAndSubWEagerRelOpt(
    BaseAndSubFixture,
    fixtures.DeclarativeMappedTest,
    testing.AssertsExecutionResults,
):
    use_options = True

    def test_load(self):
        A, B, ASub, C = self.classes("A", "B", "ASub", "C")
        s = fixture_session()

        q = (
            s.query(A)
            .order_by(A.id)
            .options(
                selectin_polymorphic(A, [ASub]),
                selectinload(ASub.cs),
                selectinload(A.bs),
            )
        )

        self._assert_all_selectin(q)


class LoadBaseAndSubWEagerRelMapped(
    BaseAndSubFixture,
    fixtures.DeclarativeMappedTest,
    testing.AssertsExecutionResults,
):
    use_options = False

    def test_load(self):
        A, B, ASub, C = self.classes("A", "B", "ASub", "C")
        s = fixture_session()

        q = (
            s.query(A)
            .order_by(A.id)
            .options(selectinload(ASub.cs), selectinload(A.bs))
        )

        self._assert_all_selectin(q)


class ChunkingTest(
    fixtures.DeclarativeMappedTest, testing.AssertsExecutionResults
):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            adata = Column(String(50))
            type = Column(String(50))

            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "a",
            }

        class ASub(A):
            __tablename__ = "asub"
            id = Column(ForeignKey("a.id"), primary_key=True)
            asubdata = Column(String(50))

            __mapper_args__ = {
                "polymorphic_load": "selectin",
                "polymorphic_identity": "asub",
            }

    @classmethod
    def insert_data(cls, connection):
        ASub = cls.classes.ASub
        s = Session(connection)
        s.add_all(
            [
                ASub(id=i, adata=f"adata {i}", asubdata=f"asubdata {i}")
                for i in range(1, 1255)
            ]
        )

        s.commit()

    def test_chunking(self):
        A = self.classes.A
        s = fixture_session()

        with self.sql_execution_asserter(testing.db) as asserter:
            asubs = s.scalars(select(A).order_by(A.id))
            eq_(len(asubs.all()), 1254)

        poly_load_sql = (
            "SELECT asub.id AS asub_id, a.id AS a_id, a.type AS a_type, "
            "asub.asubdata AS asub_asubdata FROM a JOIN asub "
            "ON a.id = asub.id WHERE a.id "
            "IN (__[POSTCOMPILE_primary_keys]) ORDER BY a.id"
        )
        asserter.assert_(
            CompiledSQL(
                "SELECT a.id, a.adata, a.type FROM a ORDER BY a.id", []
            ),
            CompiledSQL(
                poly_load_sql, [{"primary_keys": list(range(1, 501))}]
            ),
            CompiledSQL(
                poly_load_sql, [{"primary_keys": list(range(501, 1001))}]
            ),
            CompiledSQL(
                poly_load_sql, [{"primary_keys": list(range(1001, 1255))}]
            ),
        )


class FixtureLoadTest(_Polymorphic, testing.AssertsExecutionResults):
    def test_person_selectin_subclasses(self):
        s = fixture_session()

        q = s.query(Person).options(
            selectin_polymorphic(Person, [Engineer, Manager])
        )

        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT people.person_id AS people_person_id, "
                "people.company_id AS people_company_id, "
                "people.name AS people_name, "
                "people.type AS people_type FROM people",
                {},
            ),
            AllOf(
                CompiledSQL(
                    "SELECT engineers.person_id AS engineers_person_id, "
                    "people.person_id AS people_person_id, "
                    "people.type AS people_type, "
                    "engineers.status AS engineers_status, "
                    "engineers.engineer_name AS engineers_engineer_name, "
                    "engineers.primary_language AS engineers_primary_language "
                    "FROM people JOIN engineers "
                    "ON people.person_id = engineers.person_id "
                    "WHERE people.person_id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY people.person_id",
                    {"primary_keys": [1, 2, 5]},
                ),
                CompiledSQL(
                    "SELECT managers.person_id AS managers_person_id, "
                    "people.person_id AS people_person_id, "
                    "people.type AS people_type, "
                    "managers.status AS managers_status, "
                    "managers.manager_name AS managers_manager_name "
                    "FROM people JOIN managers "
                    "ON people.person_id = managers.person_id "
                    "WHERE people.person_id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY people.person_id",
                    {"primary_keys": [3, 4]},
                ),
            ),
        )
        eq_(result, self.all_employees)

    def test_load_company_plus_employees(self):
        s = fixture_session()
        q = (
            s.query(Company)
            .options(
                selectinload(Company.employees).selectin_polymorphic(
                    [Engineer, Manager]
                )
            )
            .order_by(Company.company_id)
        )

        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT companies.company_id AS companies_company_id, "
                "companies.name AS companies_name FROM companies "
                "ORDER BY companies.company_id",
                {},
            ),
            CompiledSQL(
                "SELECT people.company_id, "
                "people.person_id, "
                "people.name, people.type "
                "FROM people WHERE people.company_id "
                "IN (__[POSTCOMPILE_primary_keys]) "
                "ORDER BY people.person_id",
                {"primary_keys": [1, 2]},
            ),
            AllOf(
                CompiledSQL(
                    "SELECT managers.person_id AS managers_person_id, "
                    "people.person_id AS people_person_id, "
                    "people.type AS people_type, "
                    "managers.status AS managers_status, "
                    "managers.manager_name AS managers_manager_name "
                    "FROM people JOIN managers "
                    "ON people.person_id = managers.person_id "
                    "WHERE people.person_id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY people.person_id",
                    {"primary_keys": [3, 4]},
                ),
                CompiledSQL(
                    "SELECT engineers.person_id AS engineers_person_id, "
                    "people.person_id AS people_person_id, "
                    "people.type AS people_type, "
                    "engineers.status AS engineers_status, "
                    "engineers.engineer_name AS engineers_engineer_name, "
                    "engineers.primary_language AS engineers_primary_language "
                    "FROM people JOIN engineers "
                    "ON people.person_id = engineers.person_id "
                    "WHERE people.person_id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY people.person_id",
                    {"primary_keys": [1, 2, 5]},
                ),
            ),
        )
        eq_(result, [self.c1, self.c2])

    def test_load_company_plus_employees_w_paperwork(self):
        s = fixture_session()
        q = (
            s.query(Company)
            .options(
                selectinload(Company.employees).options(
                    selectin_polymorphic(Person, [Engineer, Manager]),
                    selectinload(Engineer.machines),
                    # NOTE: if this is selectinload(Person.paperwork),
                    # we get duplicate loads from the subclasses which is
                    # not ideal
                )
            )
            .order_by(Company.company_id)
        )

        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT companies.company_id AS companies_company_id, "
                "companies.name AS companies_name FROM companies "
                "ORDER BY companies.company_id",
                {},
            ),
            CompiledSQL(
                "SELECT people.company_id, "
                "people.person_id, "
                "people.name, people.type "
                "FROM people WHERE people.company_id "
                "IN (__[POSTCOMPILE_primary_keys]) "
                "ORDER BY people.person_id",
                {"primary_keys": [1, 2]},
            ),
            AllOf(
                CompiledSQL(
                    "SELECT managers.person_id AS managers_person_id, "
                    "people.person_id AS people_person_id, "
                    "people.type AS people_type, "
                    "managers.status AS managers_status, "
                    "managers.manager_name AS managers_manager_name "
                    "FROM people JOIN managers "
                    "ON people.person_id = managers.person_id "
                    "WHERE people.person_id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY people.person_id",
                    {"primary_keys": [3, 4]},
                ),
                CompiledSQL(
                    "SELECT engineers.person_id AS engineers_person_id, "
                    "people.person_id AS people_person_id, "
                    "people.type AS people_type, "
                    "engineers.status AS engineers_status, "
                    "engineers.engineer_name AS engineers_engineer_name, "
                    "engineers.primary_language AS engineers_primary_language "
                    "FROM people JOIN engineers "
                    "ON people.person_id = engineers.person_id "
                    "WHERE people.person_id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY people.person_id",
                    {"primary_keys": [1, 2, 5]},
                ),
                CompiledSQL(
                    "SELECT machines.engineer_id, "
                    "machines.machine_id, "
                    "machines.name "
                    "FROM machines "
                    "WHERE machines.engineer_id "
                    "IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY machines.machine_id",
                    {"primary_keys": [1, 2, 5]},
                ),
            ),
        )
        eq_(result, [self.c1, self.c2])


class TestGeometries(GeometryFixtureBase):
    def test_threelevel_selectin_to_inline_mapped(self):
        self._fixture_from_geometry(
            {
                "a": {
                    "subclasses": {
                        "b": {"polymorphic_load": "selectin"},
                        "c": {
                            "subclasses": {
                                "d": {
                                    "polymorphic_load": "inline",
                                    "single": True,
                                },
                                "e": {
                                    "polymorphic_load": "inline",
                                    "single": True,
                                },
                            },
                            "polymorphic_load": "selectin",
                        },
                    }
                }
            }
        )

        a, b, c, d, e = self.classes("a", "b", "c", "d", "e")
        sess = fixture_session()
        sess.add_all([d(d_data="d1"), e(e_data="e1")])
        sess.commit()

        q = sess.query(a)

        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT a.id AS a_id, a.type AS a_type, "
                "a.a_data AS a_a_data FROM a",
                {},
            ),
            Or(
                CompiledSQL(
                    "SELECT c.id AS c_id, a.id AS a_id, a.type AS a_type, "
                    "c.c_data AS c_c_data, c.e_data AS c_e_data, "
                    "c.d_data AS c_d_data "
                    "FROM a JOIN c ON a.id = c.id "
                    "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY a.id",
                    [{"primary_keys": [1, 2]}],
                ),
                CompiledSQL(
                    "SELECT c.id AS c_id, a.id AS a_id, a.type AS a_type, "
                    "c.c_data AS c_c_data, "
                    "c.d_data AS c_d_data, c.e_data AS c_e_data "
                    "FROM a JOIN c ON a.id = c.id "
                    "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY a.id",
                    [{"primary_keys": [1, 2]}],
                ),
            ),
        )
        with self.assert_statement_count(testing.db, 0):
            eq_(result, [d(d_data="d1"), e(e_data="e1")])

    @testing.fixture
    def threelevel_all_selectin_fixture(self):
        self._fixture_from_geometry(
            {
                "a": {
                    "subclasses": {
                        "b": {"polymorphic_load": "selectin"},
                        "c": {
                            "subclasses": {
                                "d": {
                                    "polymorphic_load": "selectin",
                                },
                                "e": {
                                    "polymorphic_load": "selectin",
                                },
                                "f": {},
                            },
                            "polymorphic_load": "selectin",
                        },
                    }
                }
            }
        )

    def test_threelevel_all_selectin_l1_load_l3(
        self, threelevel_all_selectin_fixture
    ):
        """test for #9373 - load base to receive level 3 endpoints"""

        a, b, c, d, e = self.classes("a", "b", "c", "d", "e")
        sess = fixture_session()
        sess.add_all(
            [d(c_data="cd1", d_data="d1"), e(c_data="ce1", e_data="e1")]
        )
        sess.commit()

        for i in range(3):
            sess.close()

            q = sess.query(a)

            result = self.assert_sql_execution(
                testing.db,
                q.all,
                CompiledSQL(
                    "SELECT a.id AS a_id, a.type AS a_type, "
                    "a.a_data AS a_a_data FROM a",
                    {},
                ),
                CompiledSQL(
                    "SELECT d.id AS d_id, c.id AS c_id, a.id AS a_id, "
                    "a.type AS a_type, c.c_data AS c_c_data, "
                    "d.d_data AS d_d_data "
                    "FROM a JOIN c ON a.id = c.id JOIN d ON c.id = d.id "
                    "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY a.id",
                    [{"primary_keys": [1]}],
                ),
                CompiledSQL(
                    "SELECT e.id AS e_id, c.id AS c_id, a.id AS a_id, "
                    "a.type AS a_type, c.c_data AS c_c_data, "
                    "e.e_data AS e_e_data "
                    "FROM a JOIN c ON a.id = c.id JOIN e ON c.id = e.id "
                    "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY a.id",
                    [{"primary_keys": [2]}],
                ),
            )
            with self.assert_statement_count(testing.db, 0):
                eq_(
                    result,
                    [
                        d(c_data="cd1", d_data="d1"),
                        e(c_data="ce1", e_data="e1"),
                    ],
                )

    def test_threelevel_partial_selectin_l1_load_l3(
        self, threelevel_all_selectin_fixture
    ):
        """test for #9373 - load base to receive level 3 endpoints"""

        a, b, c, d, f = self.classes("a", "b", "c", "d", "f")
        sess = fixture_session()
        sess.add_all(
            [d(c_data="cd1", d_data="d1"), f(c_data="ce1", f_data="e1")]
        )
        sess.commit()

        for i in range(3):
            sess.close()
            q = sess.query(a)

            result = self.assert_sql_execution(
                testing.db,
                q.all,
                CompiledSQL(
                    "SELECT a.id AS a_id, a.type AS a_type, "
                    "a.a_data AS a_a_data FROM a",
                    {},
                ),
                CompiledSQL(
                    "SELECT d.id AS d_id, c.id AS c_id, a.id AS a_id, "
                    "a.type AS a_type, c.c_data AS c_c_data, "
                    "d.d_data AS d_d_data "
                    "FROM a JOIN c ON a.id = c.id JOIN d ON c.id = d.id "
                    "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY a.id",
                    [{"primary_keys": [1]}],
                ),
                # only loads pk 2 - this is the filtering inside of do_load
                CompiledSQL(
                    "SELECT c.id AS c_id, a.id AS a_id, a.type AS a_type, "
                    "c.c_data AS c_c_data "
                    "FROM a JOIN c ON a.id = c.id "
                    "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY a.id",
                    [{"primary_keys": [2]}],
                ),
                # no more SQL; if we hit pk 1 again, it would re-do the d here
            )

            with self.sql_execution_asserter(testing.db) as asserter_:
                eq_(
                    result,
                    [
                        d(c_data="cd1", d_data="d1"),
                        f(c_data="ce1", f_data="e1"),
                    ],
                )

            # f was told not to load its attrs, so they load here
            asserter_.assert_(
                CompiledSQL(
                    "SELECT f.f_data AS f_f_data FROM f WHERE :param_1 = f.id",
                    [{"param_1": 2}],
                ),
            )

    def test_threelevel_all_selectin_l1_load_l2(
        self, threelevel_all_selectin_fixture
    ):
        """test for #9373 - load base to receive level 2 endpoint"""
        a, b, c, d, e = self.classes("a", "b", "c", "d", "e")
        sess = fixture_session()
        sess.add_all([c(c_data="c1", a_data="a1")])
        sess.commit()

        q = sess.query(a)

        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT a.id AS a_id, a.type AS a_type, "
                "a.a_data AS a_a_data FROM a",
                {},
            ),
            CompiledSQL(
                "SELECT c.id AS c_id, a.id AS a_id, a.type AS a_type, "
                "c.c_data AS c_c_data FROM a JOIN c ON a.id = c.id "
                "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) ORDER BY a.id",
                {"primary_keys": [1]},
            ),
        )
        with self.assert_statement_count(testing.db, 0):
            eq_(
                result,
                [c(c_data="c1", a_data="a1")],
            )

    @testing.variation("use_aliased_class", [True, False])
    def test_threelevel_all_selectin_l2_load_l3(
        self, threelevel_all_selectin_fixture, use_aliased_class
    ):
        """test for #9373 - load level 2 endpoing to receive level 3
        endpoints"""
        a, b, c, d, e = self.classes("a", "b", "c", "d", "e")
        sess = fixture_session()
        sess.add_all(
            [d(c_data="cd1", d_data="d1"), e(c_data="ce1", e_data="e1")]
        )
        sess.commit()

        if use_aliased_class:
            q = sess.query(aliased(c, flat=True))
        else:
            q = sess.query(c)
        result = self.assert_sql_execution(
            testing.db,
            q.all,
            Conditional(
                bool(use_aliased_class),
                [
                    CompiledSQL(
                        "SELECT c_1.id AS c_1_id, a_1.id AS a_1_id, "
                        "a_1.type AS a_1_type, a_1.a_data AS a_1_a_data, "
                        "c_1.c_data AS c_1_c_data "
                        "FROM a AS a_1 JOIN c AS c_1 ON a_1.id = c_1.id",
                        {},
                    )
                ],
                [
                    CompiledSQL(
                        "SELECT c.id AS c_id, a.id AS a_id, a.type AS a_type, "
                        "a.a_data AS a_a_data, c.c_data AS c_c_data "
                        "FROM a JOIN c ON a.id = c.id",
                        {},
                    )
                ],
            ),
            CompiledSQL(
                "SELECT d.id AS d_id, c.id AS c_id, a.id AS a_id, "
                "a.type AS a_type, d.d_data AS d_d_data "
                "FROM a JOIN c ON a.id = c.id JOIN d ON c.id = d.id "
                "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) ORDER BY a.id",
                [{"primary_keys": [1]}],
            ),
            CompiledSQL(
                "SELECT e.id AS e_id, c.id AS c_id, a.id AS a_id, "
                "a.type AS a_type, e.e_data AS e_e_data "
                "FROM a JOIN c ON a.id = c.id JOIN e ON c.id = e.id "
                "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) ORDER BY a.id",
                [{"primary_keys": [2]}],
            ),
        )
        with self.assert_statement_count(testing.db, 0):
            eq_(
                result,
                [d(c_data="cd1", d_data="d1"), e(c_data="ce1", e_data="e1")],
            )

    def test_threelevel_selectin_to_inline_options(self):
        self._fixture_from_geometry(
            {
                "a": {
                    "subclasses": {
                        "b": {},
                        "c": {
                            "subclasses": {
                                "d": {"single": True},
                                "e": {"single": True},
                            }
                        },
                    }
                }
            }
        )

        a, b, c, d, e = self.classes("a", "b", "c", "d", "e")
        sess = fixture_session()
        sess.add_all([d(d_data="d1"), e(e_data="e1")])
        sess.commit()

        c_alias = with_polymorphic(c, (d, e))
        q = sess.query(a).options(selectin_polymorphic(a, [b, c_alias]))

        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT a.id AS a_id, a.type AS a_type, "
                "a.a_data AS a_a_data FROM a",
                {},
            ),
            Or(
                CompiledSQL(
                    "SELECT a.id AS a_id, a.type AS a_type, c.id AS c_id, "
                    "c.c_data AS c_c_data, c.e_data AS c_e_data, "
                    "c.d_data AS c_d_data "
                    "FROM a JOIN c ON a.id = c.id "
                    "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY a.id",
                    [{"primary_keys": [1, 2]}],
                ),
                CompiledSQL(
                    "SELECT c.id AS c_id, a.id AS a_id, a.type AS a_type, "
                    "c.c_data AS c_c_data, c.d_data AS c_d_data, "
                    "c.e_data AS c_e_data "
                    "FROM a JOIN c ON a.id = c.id "
                    "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY a.id",
                    [{"primary_keys": [1, 2]}],
                ),
            ),
        )
        with self.assert_statement_count(testing.db, 0):
            eq_(result, [d(d_data="d1"), e(e_data="e1")])

    @testing.variation("include_intermediary_row", [True, False])
    def test_threelevel_load_only_3lev(self, include_intermediary_row):
        """test issue #11327"""

        self._fixture_from_geometry(
            {
                "a": {
                    "subclasses": {
                        "b": {"subclasses": {"c": {}}},
                    }
                }
            }
        )

        a, b, c = self.classes("a", "b", "c")
        sess = fixture_session()
        sess.add(c(a_data="a1", b_data="b1", c_data="c1"))
        if include_intermediary_row:
            sess.add(b(a_data="a1", b_data="b1"))
        sess.commit()

        sess = fixture_session()

        pks = []
        c_pks = []
        with self.sql_execution_asserter(testing.db) as asserter:

            for obj in sess.scalars(
                select(a)
                .options(selectin_polymorphic(a, classes=[b, c]))
                .order_by(a.id)
            ):
                assert "b_data" in obj.__dict__
                if isinstance(obj, c):
                    assert "c_data" in obj.__dict__
                    c_pks.append(obj.id)
                pks.append(obj.id)

        asserter.assert_(
            CompiledSQL(
                "SELECT a.id, a.type, a.a_data FROM a ORDER BY a.id", {}
            ),
            AllOf(
                CompiledSQL(
                    "SELECT c.id AS c_id, b.id AS b_id, a.id AS a_id, "
                    "a.type AS a_type, c.c_data AS c_c_data FROM a JOIN b "
                    "ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id IN "
                    "(__[POSTCOMPILE_primary_keys]) ORDER BY a.id",
                    [{"primary_keys": c_pks}],
                ),
                CompiledSQL(
                    "SELECT b.id AS b_id, a.id AS a_id, a.type AS a_type, "
                    "b.b_data AS b_b_data FROM a JOIN b ON a.id = b.id "
                    "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY a.id",
                    [{"primary_keys": pks}],
                ),
            ),
        )

    @testing.combinations((True,), (False,))
    def test_threelevel_selectin_to_inline_awkward_alias_options(
        self, use_aliased_class
    ):
        self._fixture_from_geometry(
            {
                "a": {
                    "subclasses": {
                        "b": {},
                        "c": {"subclasses": {"d": {}, "e": {}}},
                    }
                }
            }
        )

        a, b, c, d, e = self.classes("a", "b", "c", "d", "e")
        sess = fixture_session()
        sess.add_all(
            [d(c_data="c1", d_data="d1"), e(c_data="c2", e_data="e1")]
        )
        sess.commit()

        from sqlalchemy import select

        a_table, c_table, d_table, e_table = self.tables("a", "c", "d", "e")

        poly = (
            select(a_table.c.id, a_table.c.type, c_table, d_table, e_table)
            .select_from(
                a_table.join(c_table).outerjoin(d_table).outerjoin(e_table)
            )
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .alias("poly")
        )

        c_alias = with_polymorphic(c, (d, e), poly)

        if use_aliased_class:
            opt = selectin_polymorphic(a, [b, c_alias])
        else:
            opt = selectin_polymorphic(
                a,
                [b, c_alias, d, e],
            )
        q = sess.query(a).options(opt).order_by(a.id)

        if use_aliased_class:
            result = self.assert_sql_execution(
                testing.db,
                q.all,
                CompiledSQL(
                    "SELECT a.id AS a_id, a.type AS a_type, "
                    "a.a_data AS a_a_data FROM a ORDER BY a.id",
                    {},
                ),
                Or(
                    # here, the test is that the adaptation of "a" takes place
                    CompiledSQL(
                        "SELECT poly.c_id AS poly_c_id, "
                        "poly.a_type AS poly_a_type, "
                        "poly.a_id AS poly_a_id, poly.c_c_data "
                        "AS poly_c_c_data, "
                        "poly.e_id AS poly_e_id, poly.e_e_data "
                        "AS poly_e_e_data, "
                        "poly.d_id AS poly_d_id, poly.d_d_data "
                        "AS poly_d_d_data "
                        "FROM (SELECT a.id AS a_id, a.type AS a_type, "
                        "c.id AS c_id, "
                        "c.c_data AS c_c_data, d.id AS d_id, "
                        "d.d_data AS d_d_data, "
                        "e.id AS e_id, e.e_data AS e_e_data FROM a JOIN c "
                        "ON a.id = c.id LEFT OUTER JOIN d ON c.id = d.id "
                        "LEFT OUTER JOIN e ON c.id = e.id) AS poly "
                        "WHERE poly.a_id IN (__[POSTCOMPILE_primary_keys]) "
                        "ORDER BY poly.a_id",
                        [{"primary_keys": [1, 2]}],
                    ),
                    CompiledSQL(
                        "SELECT poly.c_id AS poly_c_id, "
                        "poly.a_id AS poly_a_id, poly.a_type AS poly_a_type, "
                        "poly.c_c_data AS poly_c_c_data, "
                        "poly.d_id AS poly_d_id, poly.d_d_data "
                        "AS poly_d_d_data, "
                        "poly.e_id AS poly_e_id, poly.e_e_data "
                        "AS poly_e_e_data "
                        "FROM (SELECT a.id AS a_id, a.type AS a_type, "
                        "c.id AS c_id, c.c_data AS c_c_data, d.id AS d_id, "
                        "d.d_data AS d_d_data, e.id AS e_id, "
                        "e.e_data AS e_e_data FROM a JOIN c ON a.id = c.id "
                        "LEFT OUTER JOIN d ON c.id = d.id "
                        "LEFT OUTER JOIN e ON c.id = e.id) AS poly "
                        "WHERE poly.a_id IN (__[POSTCOMPILE_primary_keys]) "
                        "ORDER BY poly.a_id",
                        [{"primary_keys": [1, 2]}],
                    ),
                ),
            )
        else:
            result = self.assert_sql_execution(
                testing.db,
                q.all,
                CompiledSQL(
                    "SELECT a.id AS a_id, a.type AS a_type, "
                    "a.a_data AS a_a_data FROM a ORDER BY a.id",
                    {},
                ),
                AllOf(
                    # note this query is added due to the fix made in
                    # #11327
                    CompiledSQL(
                        "SELECT c.id AS c_id, a.id AS a_id, a.type AS a_type, "
                        "c.c_data AS c_c_data FROM a JOIN c ON a.id = c.id "
                        "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) "
                        "ORDER BY a.id",
                        [{"primary_keys": [1, 2]}],
                    ),
                    CompiledSQL(
                        "SELECT d.id AS d_id, c.id AS c_id, a.id AS a_id, "
                        "a.type AS a_type, d.d_data AS d_d_data FROM a "
                        "JOIN c ON a.id = c.id JOIN d ON c.id = d.id "
                        "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) "
                        "ORDER BY a.id",
                        [{"primary_keys": [1]}],
                    ),
                    CompiledSQL(
                        "SELECT e.id AS e_id, c.id AS c_id, a.id AS a_id, "
                        "a.type AS a_type, e.e_data AS e_e_data FROM a "
                        "JOIN c ON a.id = c.id JOIN e ON c.id = e.id "
                        "WHERE a.id IN (__[POSTCOMPILE_primary_keys]) "
                        "ORDER BY a.id",
                        [{"primary_keys": [2]}],
                    ),
                ),
            )

        with self.assert_statement_count(testing.db, 0):
            eq_(
                result,
                [d(c_data="c1", d_data="d1"), e(c_data="c2", e_data="e1")],
            )

    def test_partial_load_no_invoke_eagers(self):
        # test issue #4199

        self._fixture_from_geometry(
            {
                "a": {
                    "subclasses": {
                        "a1": {"polymorphic_load": "selectin"},
                        "a2": {"polymorphic_load": "selectin"},
                    }
                }
            }
        )

        a, a1, a2 = self.classes("a", "a1", "a2")
        sess = fixture_session()

        a1_obj = a1()
        a2_obj = a2()
        sess.add_all([a1_obj, a2_obj])

        del a2_obj
        sess.flush()
        sess.expire_all()

        # _with_invoke_all_eagers(False), used by the lazy loader
        # strategy, will cause one less state to be present such that
        # the poly loader won't locate a state limited to the "a1" mapper,
        # needs to test that it has states
        sess.query(a)._with_invoke_all_eagers(False).all()


class LoaderOptionsTest(
    fixtures.DeclarativeMappedTest, testing.AssertsExecutionResults
):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Parent(ComparableEntity, Base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)

        class Child(ComparableEntity, Base):
            __tablename__ = "child"
            id = Column(Integer, primary_key=True)
            parent_id = Column(Integer, ForeignKey("parent.id"))
            parent = relationship("Parent", backref=backref("children"))

            type = Column(String(50), nullable=False)
            __mapper_args__ = {"polymorphic_on": type}

        class ChildSubclass1(Child):
            __tablename__ = "child_subclass1"
            id = Column(Integer, ForeignKey("child.id"), primary_key=True)
            __mapper_args__ = {
                "polymorphic_identity": "subclass1",
                "polymorphic_load": "selectin",
            }

        class Other(ComparableEntity, Base):
            __tablename__ = "other"

            id = Column(Integer, primary_key=True)
            child_subclass_id = Column(
                Integer, ForeignKey("child_subclass1.id")
            )
            child_subclass = relationship(
                "ChildSubclass1", backref=backref("others")
            )

    @classmethod
    def insert_data(cls, connection):
        Parent, ChildSubclass1, Other = cls.classes(
            "Parent", "ChildSubclass1", "Other"
        )
        session = Session(connection)

        parent = Parent(id=1)
        subclass1 = ChildSubclass1(id=1, parent=parent)
        other = Other(id=1, child_subclass=subclass1)
        session.add_all([parent, subclass1, other])
        session.commit()

    def test_options_dont_pollute(self):
        Parent, ChildSubclass1, Other = self.classes(
            "Parent", "ChildSubclass1", "Other"
        )
        session = fixture_session()

        def no_opt():
            q = session.query(Parent).options(
                joinedload(Parent.children.of_type(ChildSubclass1))
            )

            return self.assert_sql_execution(
                testing.db,
                q.all,
                CompiledSQL(
                    "SELECT parent.id AS parent_id, "
                    "anon_1.child_id AS anon_1_child_id, "
                    "anon_1.child_parent_id AS anon_1_child_parent_id, "
                    "anon_1.child_type AS anon_1_child_type, "
                    "anon_1.child_subclass1_id AS anon_1_child_subclass1_id "
                    "FROM parent "
                    "LEFT OUTER JOIN (SELECT child.id AS child_id, "
                    "child.parent_id AS child_parent_id, "
                    "child.type AS child_type, "
                    "child_subclass1.id AS child_subclass1_id "
                    "FROM child "
                    "LEFT OUTER JOIN child_subclass1 "
                    "ON child.id = child_subclass1.id) AS anon_1 "
                    "ON parent.id = anon_1.child_parent_id",
                    {},
                ),
                CompiledSQL(
                    "SELECT child_subclass1.id AS child_subclass1_id, "
                    "child.id AS child_id, "
                    "child.type AS child_type "
                    "FROM child JOIN child_subclass1 "
                    "ON child.id = child_subclass1.id "
                    "WHERE child.id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY child.id",
                    [{"primary_keys": [1]}],
                ),
            )

        result = no_opt()
        with self.assert_statement_count(testing.db, 1):
            eq_(result, [Parent(children=[ChildSubclass1(others=[Other()])])])

        session.expunge_all()

        q = session.query(Parent).options(
            joinedload(Parent.children.of_type(ChildSubclass1)).joinedload(
                ChildSubclass1.others
            )
        )

        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT parent.id AS parent_id, "
                "anon_1.child_id AS anon_1_child_id, "
                "anon_1.child_parent_id AS anon_1_child_parent_id, "
                "anon_1.child_type AS anon_1_child_type, "
                "anon_1.child_subclass1_id AS anon_1_child_subclass1_id, "
                "other_1.id AS other_1_id, "
                "other_1.child_subclass_id AS other_1_child_subclass_id "
                "FROM parent LEFT OUTER JOIN "
                "(SELECT child.id AS child_id, "
                "child.parent_id AS child_parent_id, "
                "child.type AS child_type, "
                "child_subclass1.id AS child_subclass1_id "
                "FROM child LEFT OUTER JOIN child_subclass1 "
                "ON child.id = child_subclass1.id) AS anon_1 "
                "ON parent.id = anon_1.child_parent_id "
                "LEFT OUTER JOIN other AS other_1 "
                "ON anon_1.child_subclass1_id = other_1.child_subclass_id",
                {},
            ),
            CompiledSQL(
                "SELECT child_subclass1.id AS child_subclass1_id, "
                "child.id AS child_id, "
                "child.type AS child_type, other_1.id AS other_1_id, "
                "other_1.child_subclass_id AS other_1_child_subclass_id "
                "FROM child JOIN child_subclass1 "
                "ON child.id = child_subclass1.id "
                "LEFT OUTER JOIN other AS other_1 "
                "ON child_subclass1.id = other_1.child_subclass_id "
                "WHERE child.id IN (__[POSTCOMPILE_primary_keys]) "
                "ORDER BY child.id",
                [{"primary_keys": [1]}],
            ),
        )

        with self.assert_statement_count(testing.db, 0):
            eq_(result, [Parent(children=[ChildSubclass1(others=[Other()])])])

        session.expunge_all()

        result = no_opt()
        with self.assert_statement_count(testing.db, 1):
            eq_(result, [Parent(children=[ChildSubclass1(others=[Other()])])])


class IgnoreOptionsOnSubclassAttrLoad(fixtures.DeclarativeMappedTest):
    """test #7304 and related cases

    in this case we trigger the subclass attribute load, while at the same
    time there will be a deferred loader option present in the state's
    options that was established by the previous loader.

    test both that the option takes effect (i.e. raiseload) and that a deferred
    loader doesn't interfere with the mapper's load of the attribute.

    """

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Parent(Base):
            __tablename__ = "parent"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )

            entity_id = Column(ForeignKey("entity.id"))
            entity = relationship("Entity")

        class Entity(Base):
            __tablename__ = "entity"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            type = Column(String(32))

            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "entity",
            }

        class SubEntity(Entity):
            __tablename__ = "sub_entity"

            id = Column(ForeignKey(Entity.id), primary_key=True)

            name = Column(String(32))

            __mapper_args__ = {"polymorphic_identity": "entity_two"}

    @classmethod
    def insert_data(cls, connection):
        Parent, SubEntity = cls.classes("Parent", "SubEntity")

        with Session(connection) as session:
            session.add(Parent(entity=SubEntity(name="some name")))
            session.commit()

    @testing.combinations(
        defaultload,
        joinedload,
        selectinload,
        lazyload,
        argnames="first_option",
    )
    @testing.combinations(
        ("load_only", "id", True),
        ("defer", "name", True),
        ("undefer", "name", True),
        ("raise", "name", False),
        (None, None, True),
        # these don't seem possible at the moment as the "type" column
        # doesn't load and it can't recognize the polymorphic identity.
        # we assume load_only() is smart enough to include this column
        # ("defer", '*', True),
        # ("undefer", '*', True),
        # ("raise", '*', False),
        argnames="second_option,second_argument,expect_load",
    )
    def test_subclass_loadattr(
        self, first_option, second_option, second_argument, expect_load
    ):
        Parent, Entity, SubEntity = self.classes(
            "Parent", "Entity", "SubEntity"
        )

        stmt = select(Parent)

        will_lazyload = first_option in (defaultload, lazyload)

        if second_argument == "name":
            second_argument = SubEntity.name
            opt = first_option(Parent.entity.of_type(SubEntity))
        elif second_argument == "id":
            opt = first_option(Parent.entity)
            second_argument = Entity.id
        else:
            opt = first_option(Parent.entity)

        if second_option is None:
            sub_opt = opt
        elif second_option == "raise":
            sub_opt = opt.defer(second_argument, raiseload=True)
        else:
            sub_opt = getattr(opt, second_option)(second_argument)

        stmt = stmt.options(sub_opt)

        session = fixture_session()
        result = session.execute(stmt).scalars()

        parent_obj = result.first()

        entity_id = parent_obj.__dict__["entity_id"]

        with assertsql.assert_engine(testing.db) as asserter_:
            if expect_load:
                eq_(parent_obj.entity.name, "some name")
            else:
                with expect_raises_message(
                    exc.InvalidRequestError,
                    "'SubEntity.name' is not available due to raiseload=True",
                ):
                    parent_obj.entity.name

        expected = []

        if will_lazyload:
            expected.append(
                CompiledSQL(
                    "SELECT entity.id, "
                    "entity.type FROM entity "
                    "WHERE entity.id = :pk_1",
                    [{"pk_1": entity_id}],
                )
            )

        if second_option in ("load_only", None) or (
            second_option == "undefer"
            and first_option in (defaultload, lazyload)
        ):
            # load will be a mapper optimized load for the name alone
            expected.append(
                CompiledSQL(
                    "SELECT sub_entity.name AS sub_entity_name "
                    "FROM sub_entity "
                    "WHERE :param_1 = sub_entity.id",
                    [{"param_1": entity_id}],
                )
            )
        elif second_option == "defer":
            # load will be a deferred load.  this is because the explicit
            # call to the deferred load put a deferred loader on the attribute
            expected.append(
                CompiledSQL(
                    "SELECT sub_entity.name AS sub_entity_name "
                    "FROM sub_entity "
                    "WHERE :param_1 = sub_entity.id",
                    [{"param_1": entity_id}],
                )
            )

        asserter_.assert_(*expected)


class LazyLoaderTransfersOptsTest(fixtures.DeclarativeMappedTest):
    """test #7557"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Address(Base):
            __tablename__ = "address"

            id = Column(Integer, primary_key=True)
            user_id = Column(Integer, ForeignKey("user.id"))
            address_type = Column(String(50))
            __mapper_args__ = {
                "polymorphic_identity": "base_address",
                "polymorphic_on": address_type,
            }

        class EmailAddress(Address):
            __tablename__ = "email_address"
            email = Column(String(50))
            address_id = Column(
                Integer,
                ForeignKey(Address.id),
                primary_key=True,
            )

            __mapper_args__ = {
                "polymorphic_identity": "email",
                "polymorphic_load": "selectin",
            }

        class User(Base):
            __tablename__ = "user"

            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            address = relationship(Address, uselist=False)

    @classmethod
    def insert_data(cls, connection):
        User, EmailAddress = cls.classes("User", "EmailAddress")
        with Session(connection) as sess:
            sess.add_all(
                [User(name="u1", address=EmailAddress(email="foo", user_id=1))]
            )

            sess.commit()

    @testing.combinations(
        None, selectinload, joinedload, lazyload, subqueryload, immediateload
    )
    def test_opt_propagates(self, strat):
        User, EmailAddress = self.classes("User", "EmailAddress")
        sess = fixture_session()

        class AnyOpt(CompileStateOption):
            _cache_key_traversal = ()
            propagate_to_loaders = True

        any_opt = AnyOpt()
        if strat is None:
            opts = (any_opt,)
        else:
            opts = (strat(User.address), any_opt)

        u = sess.execute(select(User).options(*opts)).scalars().one()
        address = u.address
        eq_(inspect(address).load_options, opts)


class NoBaseWPPlusAliasedTest(
    testing.AssertsExecutionResults, fixtures.TestBase
):
    """test for #7799"""

    @testing.fixture
    def mapping_fixture(self, registry, connection):
        Base = registry.generate_base()

        class BaseClass(Base):
            __tablename__ = "baseclass"
            id = Column(
                Integer,
                primary_key=True,
                unique=True,
            )

        class A(BaseClass):
            __tablename__ = "a"

            id = Column(ForeignKey(BaseClass.id), primary_key=True)
            thing1 = Column(String(50))

            __mapper_args__ = {"polymorphic_identity": "a"}

        class B(BaseClass):
            __tablename__ = "b"

            id = Column(ForeignKey(BaseClass.id), primary_key=True)
            thing2 = Column(String(50))

            __mapper_args__ = {"polymorphic_identity": "b"}

        registry.metadata.create_all(connection)
        with Session(connection) as sess:
            sess.add_all(
                [
                    A(thing1="thing1_1"),
                    A(thing1="thing1_2"),
                    B(thing2="thing2_2"),
                    B(thing2="thing2_3"),
                    A(thing1="thing1_3"),
                    A(thing1="thing1_4"),
                    B(thing2="thing2_1"),
                    B(thing2="thing2_4"),
                ]
            )

            sess.commit()

        return BaseClass, A, B

    def test_wp(self, mapping_fixture, connection):
        BaseClass, A, B = mapping_fixture

        stmt = union(
            select(A.id, literal("a").label("type")),
            select(B.id, literal("b").label("type")),
        ).subquery()

        wp = with_polymorphic(
            BaseClass,
            [A, B],
            selectable=stmt,
            polymorphic_on=stmt.c.type,
        )

        session = Session(connection)

        with self.sql_execution_asserter() as asserter:
            result = session.scalars(
                select(wp)
                .options(selectin_polymorphic(wp, [A, B]))
                .order_by(wp.id)
            )
            for obj in result:
                if isinstance(obj, A):
                    obj.thing1
                else:
                    obj.thing2

        asserter.assert_(
            CompiledSQL(
                "SELECT anon_1.id, anon_1.type FROM "
                "(SELECT a.id AS id, :param_1 AS type FROM baseclass "
                "JOIN a ON baseclass.id = a.id "
                "UNION SELECT b.id AS id, :param_2 AS type "
                "FROM baseclass JOIN b ON baseclass.id = b.id) AS anon_1 "
                "ORDER BY anon_1.id",
                [{"param_1": "a", "param_2": "b"}],
            ),
            AllOf(
                CompiledSQL(
                    "SELECT a.id AS a_id, baseclass.id AS baseclass_id, "
                    "a.thing1 AS a_thing1 FROM baseclass "
                    "JOIN a ON baseclass.id = a.id "
                    "WHERE baseclass.id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY baseclass.id",
                    {"primary_keys": [1, 2, 5, 6]},
                ),
                CompiledSQL(
                    "SELECT b.id AS b_id, baseclass.id AS baseclass_id, "
                    "b.thing2 AS b_thing2 FROM baseclass "
                    "JOIN b ON baseclass.id = b.id "
                    "WHERE baseclass.id IN (__[POSTCOMPILE_primary_keys]) "
                    "ORDER BY baseclass.id",
                    {"primary_keys": [3, 4, 7, 8]},
                ),
            ),
        )


class CompositeAttributesTest(fixtures.TestBase):

    @testing.fixture(params=("base", "sub"))
    def mapping_fixture(self, request, registry, connection):
        Base = registry.generate_base()

        class XYThing:
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

        class BaseCls(Base):
            __tablename__ = "base"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            type = Column(String(50))

            if request.param == "base":
                comp1 = composite(
                    XYThing, Column("x1", Integer), Column("y1", Integer)
                )

            __mapper_args__ = {"polymorphic_on": type}

        class A(ComparableEntity, BaseCls):
            __tablename__ = "a"
            id = Column(ForeignKey(BaseCls.id), primary_key=True)
            thing1 = Column(String(50))
            if request.param == "sub":
                comp1 = composite(
                    XYThing, Column("x1", Integer), Column("y1", Integer)
                )

            __mapper_args__ = {
                "polymorphic_identity": "a",
                "polymorphic_load": "selectin",
            }

        class B(ComparableEntity, BaseCls):
            __tablename__ = "b"
            id = Column(ForeignKey(BaseCls.id), primary_key=True)
            thing2 = Column(String(50))
            comp2 = composite(
                XYThing, Column("x2", Integer), Column("y2", Integer)
            )

            __mapper_args__ = {
                "polymorphic_identity": "b",
                "polymorphic_load": "selectin",
            }

        registry.metadata.create_all(connection)

        with Session(connection) as sess:
            sess.add_all(
                [
                    A(id=1, thing1="thing1", comp1=XYThing(1, 2)),
                    B(id=2, thing2="thing2", comp2=XYThing(3, 4)),
                ]
            )
            sess.commit()

        return BaseCls, A, B, XYThing

    def test_load_composite(self, mapping_fixture, connection):
        BaseCls, A, B, XYThing = mapping_fixture

        with Session(connection) as sess:
            rows = sess.scalars(select(BaseCls).order_by(BaseCls.id)).all()

            eq_(
                rows,
                [
                    A(id=1, thing1="thing1", comp1=XYThing(1, 2)),
                    B(id=2, thing2="thing2", comp2=XYThing(3, 4)),
                ],
            )


class PolymorphicOnExprTest(
    testing.AssertsExecutionResults, fixtures.TestBase
):
    """test for #8704"""

    @testing.fixture()
    def poly_fixture(self, connection, decl_base):
        def fixture(create_prop, use_load):
            class TypeTable(decl_base):
                __tablename__ = "type"

                id = Column(Integer, primary_key=True)
                name = Column(String(30))

            class PolyBase(ComparableEntity, decl_base):
                __tablename__ = "base"

                id = Column(Integer, primary_key=True)
                type_id = Column(ForeignKey(TypeTable.id))

                if create_prop == "create_prop":
                    polymorphic = column_property(
                        select(TypeTable.name)
                        .where(TypeTable.id == type_id)
                        .scalar_subquery()
                    )
                    __mapper_args__ = {
                        "polymorphic_on": polymorphic,
                    }
                elif create_prop == "dont_create_prop":
                    __mapper_args__ = {
                        "polymorphic_on": select(TypeTable.name)
                        .where(TypeTable.id == type_id)
                        .scalar_subquery()
                    }
                elif create_prop == "arg_level_prop":
                    __mapper_args__ = {
                        "polymorphic_on": column_property(
                            select(TypeTable.name)
                            .where(TypeTable.id == type_id)
                            .scalar_subquery()
                        )
                    }

            class Foo(PolyBase):
                __tablename__ = "foo"

                if use_load == "use_polymorphic_load":
                    __mapper_args__ = {
                        "polymorphic_identity": "foo",
                        "polymorphic_load": "selectin",
                    }
                else:
                    __mapper_args__ = {
                        "polymorphic_identity": "foo",
                    }

                id = Column(ForeignKey(PolyBase.id), primary_key=True)
                foo_attr = Column(String(30))

            decl_base.metadata.create_all(connection)

            with Session(connection) as session:
                foo_type = TypeTable(name="foo")
                session.add(foo_type)
                session.flush()

                foo = Foo(type_id=foo_type.id, foo_attr="foo value")
                session.add(foo)

                session.commit()

            return PolyBase, Foo, TypeTable

        yield fixture

    @testing.combinations(
        "create_prop",
        "dont_create_prop",
        "arg_level_prop",
        argnames="create_prop",
    )
    @testing.combinations(
        "use_polymorphic_load",
        "use_loader_option",
        "none",
        argnames="use_load",
    )
    def test_load_selectin(
        self, poly_fixture, connection, create_prop, use_load
    ):
        PolyBase, Foo, TypeTable = poly_fixture(create_prop, use_load)

        sess = Session(connection)

        foo_type = sess.scalars(select(TypeTable)).one()

        stmt = select(PolyBase)
        if use_load == "use_loader_option":
            stmt = stmt.options(selectin_polymorphic(PolyBase, [Foo]))
        obj = sess.scalars(stmt).all()

        def go():
            eq_(obj, [Foo(type_id=foo_type.id, foo_attr="foo value")])

        self.assert_sql_count(testing.db, go, 0 if use_load != "none" else 1)
