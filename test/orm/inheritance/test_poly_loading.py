from sqlalchemy import String, Integer, Column, ForeignKey
from sqlalchemy.orm import relationship, Session, \
    selectin_polymorphic, selectinload, with_polymorphic
from sqlalchemy.testing import fixtures
from sqlalchemy import testing
from sqlalchemy.testing import eq_
from sqlalchemy.testing.assertsql import AllOf, CompiledSQL, EachOf, Or
from ._poly_fixtures import Company, Person, Engineer, Manager, \
    _Polymorphic, GeometryFixtureBase


class BaseAndSubFixture(object):
    use_options = False

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = 'a'
            id = Column(Integer, primary_key=True)
            adata = Column(String(50))
            bs = relationship("B")
            type = Column(String(50))

            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "a"
            }

        class ASub(A):
            __tablename__ = 'asub'
            id = Column(ForeignKey('a.id'), primary_key=True)
            asubdata = Column(String(50))

            cs = relationship("C")

            if cls.use_options:
                __mapper_args__ = {
                    "polymorphic_identity": "asub"
                }
            else:
                __mapper_args__ = {
                    "polymorphic_load": "selectin",
                    "polymorphic_identity": "asub"
                }

        class B(Base):
            __tablename__ = 'b'
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey('a.id'))

        class C(Base):
            __tablename__ = 'c'
            id = Column(Integer, primary_key=True)
            a_sub_id = Column(ForeignKey('asub.id'))

    @classmethod
    def insert_data(cls):
        A, B, ASub, C = cls.classes("A", "B", "ASub", "C")
        s = Session()
        s.add(A(id=1, adata='adata', bs=[B(), B()]))
        s.add(ASub(id=2, adata='adata', asubdata='asubdata',
              bs=[B(), B()], cs=[C(), C()]))

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
                {}
            ),
            AllOf(
                EachOf(
                    CompiledSQL(
                        "SELECT asub.id AS asub_id, a.id AS a_id, a.type AS a_type, "
                        "asub.asubdata AS asub_asubdata FROM a JOIN asub "
                        "ON a.id = asub.id WHERE a.id IN ([EXPANDING_primary_keys]) "
                        "ORDER BY a.id",
                        {"primary_keys": [2]}
                    ),
                    CompiledSQL(
                        "SELECT anon_1.a_id AS anon_1_a_id, c.id AS c_id, "
                        "c.a_sub_id AS c_a_sub_id FROM (SELECT a.id AS a_id, a.adata "
                        "AS a_adata, a.type AS a_type, asub.id AS asub_id, "
                        "asub.asubdata AS asub_asubdata FROM a JOIN asub "
                        "ON a.id = asub.id) AS anon_1 JOIN c "
                        "ON anon_1.asub_id = c.a_sub_id "
                        "WHERE anon_1.a_id IN ([EXPANDING_primary_keys]) "
                        "ORDER BY anon_1.a_id",
                        {"primary_keys": [2]}
                    ),
                ),
                CompiledSQL(
                    "SELECT a_1.id AS a_1_id, b.id AS b_id, b.a_id AS b_a_id "
                    "FROM a AS a_1 JOIN b ON a_1.id = b.a_id "
                    "WHERE a_1.id IN ([EXPANDING_primary_keys]) ORDER BY a_1.id",
                    {"primary_keys": [1, 2]}
                )
            )

        )

        self.assert_sql_execution(
            testing.db,
            lambda: self._run_query(result),
        )


class LoadBaseAndSubWEagerRelOpt(
        BaseAndSubFixture, fixtures.DeclarativeMappedTest,
        testing.AssertsExecutionResults):
    use_options = True

    def test_load(self):
        A, B, ASub, C = self.classes("A", "B", "ASub", "C")
        s = Session()

        q = s.query(A).order_by(A.id).options(
            selectin_polymorphic(A, [ASub]),
            selectinload(ASub.cs),
            selectinload(A.bs)
        )

        self._assert_all_selectin(q)


class LoadBaseAndSubWEagerRelMapped(
        BaseAndSubFixture, fixtures.DeclarativeMappedTest,
        testing.AssertsExecutionResults):
    use_options = False

    def test_load(self):
        A, B, ASub, C = self.classes("A", "B", "ASub", "C")
        s = Session()

        q = s.query(A).order_by(A.id).options(
            selectinload(ASub.cs),
            selectinload(A.bs)
        )

        self._assert_all_selectin(q)


class FixtureLoadTest(_Polymorphic, testing.AssertsExecutionResults):
    def test_person_selectin_subclasses(self):
        s = Session()
        q = s.query(Person).options(
            selectin_polymorphic(Person, [Engineer, Manager]))

        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT people.person_id AS people_person_id, "
                "people.company_id AS people_company_id, "
                "people.name AS people_name, "
                "people.type AS people_type FROM people",
                {}
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
                    "WHERE people.person_id IN ([EXPANDING_primary_keys]) "
                    "ORDER BY people.person_id",
                    {"primary_keys": [1, 2, 5]}
                ),
                CompiledSQL(
                    "SELECT managers.person_id AS managers_person_id, "
                    "people.person_id AS people_person_id, "
                    "people.type AS people_type, "
                    "managers.status AS managers_status, "
                    "managers.manager_name AS managers_manager_name "
                    "FROM people JOIN managers "
                    "ON people.person_id = managers.person_id "
                    "WHERE people.person_id IN ([EXPANDING_primary_keys]) "
                    "ORDER BY people.person_id",
                    {"primary_keys": [3, 4]}
                )
            ),
        )
        eq_(result, self.all_employees)

    def test_load_company_plus_employees(self):
        s = Session()
        q = s.query(Company).options(
            selectinload(Company.employees).
            selectin_polymorphic([Engineer, Manager])
        ).order_by(Company.company_id)

        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT companies.company_id AS companies_company_id, "
                "companies.name AS companies_name FROM companies "
                "ORDER BY companies.company_id",
                {}
            ),
            CompiledSQL(
                "SELECT companies_1.company_id AS companies_1_company_id, "
                "people.person_id AS people_person_id, "
                "people.company_id AS people_company_id, "
                "people.name AS people_name, people.type AS people_type "
                "FROM companies AS companies_1 JOIN people "
                "ON companies_1.company_id = people.company_id "
                "WHERE companies_1.company_id IN ([EXPANDING_primary_keys]) "
                "ORDER BY companies_1.company_id, people.person_id",
                {"primary_keys": [1, 2]}
            ),
            AllOf(
                CompiledSQL(
                    "SELECT managers.person_id AS managers_person_id, "
                    "people.person_id AS people_person_id, "
                    "people.company_id AS people_company_id, "
                    "people.name AS people_name, people.type AS people_type, "
                    "managers.status AS managers_status, "
                    "managers.manager_name AS managers_manager_name "
                    "FROM people JOIN managers "
                    "ON people.person_id = managers.person_id "
                    "WHERE people.person_id IN ([EXPANDING_primary_keys]) "
                    "ORDER BY people.person_id",
                    {"primary_keys": [3, 4]}
                ),
                CompiledSQL(
                    "SELECT engineers.person_id AS engineers_person_id, "
                    "people.person_id AS people_person_id, "
                    "people.company_id AS people_company_id, "
                    "people.name AS people_name, people.type AS people_type, "
                    "engineers.status AS engineers_status, "
                    "engineers.engineer_name AS engineers_engineer_name, "
                    "engineers.primary_language AS engineers_primary_language "
                    "FROM people JOIN engineers "
                    "ON people.person_id = engineers.person_id "
                    "WHERE people.person_id IN ([EXPANDING_primary_keys]) "
                    "ORDER BY people.person_id",
                    {"primary_keys": [1, 2, 5]}
                )
            )
        )
        eq_(result, [self.c1, self.c2])


class TestGeometries(GeometryFixtureBase):

    def test_threelevel_selectin_to_inline_mapped(self):
        self._fixture_from_geometry({
            "a": {
                "subclasses": {
                    "b": {"polymorphic_load": "selectin"},
                    "c": {
                        "subclasses": {
                            "d": {
                                "polymorphic_load": "inline", "single": True
                            },
                            "e": {
                                "polymorphic_load": "inline", "single": True
                            },
                        },
                        "polymorphic_load": "selectin",
                    }
                }
            }
        })

        a, b, c, d, e = self.classes("a", "b", "c", "d", "e")
        sess = Session()
        sess.add_all([d(d_data="d1"), e(e_data="e1")])
        sess.commit()

        q = sess.query(a)

        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT a.type AS a_type, a.id AS a_id, "
                "a.a_data AS a_a_data FROM a",
                {}
            ),
            Or(
                CompiledSQL(
                    "SELECT a.type AS a_type, c.id AS c_id, a.id AS a_id, "
                    "c.c_data AS c_c_data, c.e_data AS c_e_data, "
                    "c.d_data AS c_d_data "
                    "FROM a JOIN c ON a.id = c.id "
                    "WHERE a.id IN ([EXPANDING_primary_keys]) ORDER BY a.id",
                    [{'primary_keys': [1, 2]}]
                ),
                CompiledSQL(
                    "SELECT a.type AS a_type, c.id AS c_id, a.id AS a_id, "
                    "c.c_data AS c_c_data, "
                    "c.d_data AS c_d_data, c.e_data AS c_e_data "
                    "FROM a JOIN c ON a.id = c.id "
                    "WHERE a.id IN ([EXPANDING_primary_keys]) ORDER BY a.id",
                    [{'primary_keys': [1, 2]}]
                )
            )
        )
        with self.assert_statement_count(testing.db, 0):
            eq_(
                result,
                [d(d_data="d1"), e(e_data="e1")]
            )

    def test_threelevel_selectin_to_inline_options(self):
        self._fixture_from_geometry({
            "a": {
                "subclasses": {
                    "b": {},
                    "c": {
                        "subclasses": {
                            "d": {
                                "single": True
                            },
                            "e": {
                                "single": True
                            },
                        },
                    }
                }
            }
        })

        a, b, c, d, e = self.classes("a", "b", "c", "d", "e")
        sess = Session()
        sess.add_all([d(d_data="d1"), e(e_data="e1")])
        sess.commit()

        c_alias = with_polymorphic(c, (d, e))
        q = sess.query(a).options(
            selectin_polymorphic(a, [b, c_alias])
        )

        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT a.type AS a_type, a.id AS a_id, "
                "a.a_data AS a_a_data FROM a",
                {}
            ),
            Or(
                CompiledSQL(
                    "SELECT a.type AS a_type, c.id AS c_id, a.id AS a_id, "
                    "c.c_data AS c_c_data, c.e_data AS c_e_data, "
                    "c.d_data AS c_d_data "
                    "FROM a JOIN c ON a.id = c.id "
                    "WHERE a.id IN ([EXPANDING_primary_keys]) ORDER BY a.id",
                    [{'primary_keys': [1, 2]}]
                ),
                CompiledSQL(
                    "SELECT a.type AS a_type, c.id AS c_id, a.id AS a_id, "
                    "c.c_data AS c_c_data, c.d_data AS c_d_data, "
                    "c.e_data AS c_e_data "
                    "FROM a JOIN c ON a.id = c.id "
                    "WHERE a.id IN ([EXPANDING_primary_keys]) ORDER BY a.id",
                    [{'primary_keys': [1, 2]}]
                ),
            )
        )
        with self.assert_statement_count(testing.db, 0):
            eq_(
                result,
                [d(d_data="d1"), e(e_data="e1")]
            )

    def test_threelevel_selectin_to_inline_awkward_alias_options(self):
        self._fixture_from_geometry({
            "a": {
                "subclasses": {
                    "b": {},
                    "c": {
                        "subclasses": {
                            "d": {},
                            "e": {},
                        },
                    }
                }
            }
        })

        a, b, c, d, e = self.classes("a", "b", "c", "d", "e")
        sess = Session()
        sess.add_all([d(d_data="d1"), e(e_data="e1")])
        sess.commit()

        from sqlalchemy import select

        a_table, c_table, d_table, e_table = self.tables("a", "c", "d", "e")

        poly = select([
            a_table.c.id, a_table.c.type, c_table, d_table, e_table
        ]).select_from(
            a_table.join(c_table).outerjoin(d_table).outerjoin(e_table)
        ).apply_labels().alias('poly')

        c_alias = with_polymorphic(c, (d, e), poly)
        q = sess.query(a).options(
            selectin_polymorphic(a, [b, c_alias])
        ).order_by(a.id)

        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT a.type AS a_type, a.id AS a_id, "
                "a.a_data AS a_a_data FROM a ORDER BY a.id",
                {}
            ),
            Or(
                # here, the test is that the adaptation of "a" takes place
                CompiledSQL(
                    "SELECT poly.a_type AS poly_a_type, "
                    "poly.c_id AS poly_c_id, "
                    "poly.a_id AS poly_a_id, poly.c_c_data AS poly_c_c_data, "
                    "poly.e_id AS poly_e_id, poly.e_e_data AS poly_e_e_data, "
                    "poly.d_id AS poly_d_id, poly.d_d_data AS poly_d_d_data "
                    "FROM (SELECT a.id AS a_id, a.type AS a_type, "
                    "c.id AS c_id, "
                    "c.c_data AS c_c_data, d.id AS d_id, "
                    "d.d_data AS d_d_data, "
                    "e.id AS e_id, e.e_data AS e_e_data FROM a JOIN c "
                    "ON a.id = c.id LEFT OUTER JOIN d ON c.id = d.id "
                    "LEFT OUTER JOIN e ON c.id = e.id) AS poly "
                    "WHERE poly.a_id IN ([EXPANDING_primary_keys]) "
                    "ORDER BY poly.a_id",
                    [{'primary_keys': [1, 2]}]
                ),
                CompiledSQL(
                    "SELECT poly.a_type AS poly_a_type, "
                    "poly.c_id AS poly_c_id, "
                    "poly.a_id AS poly_a_id, poly.c_c_data AS poly_c_c_data, "
                    "poly.d_id AS poly_d_id, poly.d_d_data AS poly_d_d_data, "
                    "poly.e_id AS poly_e_id, poly.e_e_data AS poly_e_e_data "
                    "FROM (SELECT a.id AS a_id, a.type AS a_type, "
                    "c.id AS c_id, c.c_data AS c_c_data, d.id AS d_id, "
                    "d.d_data AS d_d_data, e.id AS e_id, "
                    "e.e_data AS e_e_data FROM a JOIN c ON a.id = c.id "
                    "LEFT OUTER JOIN d ON c.id = d.id "
                    "LEFT OUTER JOIN e ON c.id = e.id) AS poly "
                    "WHERE poly.a_id IN ([EXPANDING_primary_keys]) "
                    "ORDER BY poly.a_id",
                    [{'primary_keys': [1, 2]}]
                )
            )
        )
        with self.assert_statement_count(testing.db, 0):
            eq_(
                result,
                [d(d_data="d1"), e(e_data="e1")]
            )

    def test_partial_load_no_invoke_eagers(self):
        # test issue #4199

        self._fixture_from_geometry({
            "a": {
                "subclasses": {
                    "a1": {"polymorphic_load": "selectin"},
                    "a2": {"polymorphic_load": "selectin"}
                }
            }
        })

        a, a1, a2 = self.classes("a", "a1", "a2")
        sess = Session()

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
