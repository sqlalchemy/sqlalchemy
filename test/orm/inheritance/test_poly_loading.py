from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import backref
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectin_polymorphic
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.sql.selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertsql import AllOf
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.assertsql import EachOf
from sqlalchemy.testing.assertsql import Or
from sqlalchemy.testing.fixtures import fixture_session
from ._poly_fixtures import _Polymorphic
from ._poly_fixtures import Company
from ._poly_fixtures import Engineer
from ._poly_fixtures import GeometryFixtureBase
from ._poly_fixtures import Manager
from ._poly_fixtures import Person


class BaseAndSubFixture(object):
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
                        "WHERE a.id IN ([POSTCOMPILE_primary_keys]) "
                        "ORDER BY a.id",
                        {"primary_keys": [2]},
                    ),
                    CompiledSQL(
                        # note this links c.a_sub_id to a.id, even though
                        # primaryjoin is to asub.id.  this is because the
                        # cols a.id / asub.id are listed in the mapper's
                        # equivalent_columns so they are guaranteed to store
                        # the same value.
                        "SELECT c.a_sub_id AS c_a_sub_id, "
                        "c.id AS c_id "
                        "FROM c WHERE c.a_sub_id "
                        "IN ([POSTCOMPILE_primary_keys])",
                        {"primary_keys": [2]},
                    ),
                ),
                CompiledSQL(
                    "SELECT b.a_id AS b_a_id, b.id AS b_id FROM b "
                    "WHERE b.a_id IN ([POSTCOMPILE_primary_keys])",
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
                    "WHERE people.person_id IN ([POSTCOMPILE_primary_keys]) "
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
                    "WHERE people.person_id IN ([POSTCOMPILE_primary_keys]) "
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
                "SELECT people.company_id AS people_company_id, "
                "people.person_id AS people_person_id, "
                "people.name AS people_name, people.type AS people_type "
                "FROM people WHERE people.company_id "
                "IN ([POSTCOMPILE_primary_keys]) "
                "ORDER BY people.person_id",
                {"primary_keys": [1, 2]},
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
                    "WHERE people.person_id IN ([POSTCOMPILE_primary_keys]) "
                    "ORDER BY people.person_id",
                    {"primary_keys": [3, 4]},
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
                    "WHERE people.person_id IN ([POSTCOMPILE_primary_keys]) "
                    "ORDER BY people.person_id",
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
                "SELECT a.type AS a_type, a.id AS a_id, "
                "a.a_data AS a_a_data FROM a",
                {},
            ),
            Or(
                CompiledSQL(
                    "SELECT a.type AS a_type, c.id AS c_id, a.id AS a_id, "
                    "c.c_data AS c_c_data, c.e_data AS c_e_data, "
                    "c.d_data AS c_d_data "
                    "FROM a JOIN c ON a.id = c.id "
                    "WHERE a.id IN ([POSTCOMPILE_primary_keys]) ORDER BY a.id",
                    [{"primary_keys": [1, 2]}],
                ),
                CompiledSQL(
                    "SELECT a.type AS a_type, c.id AS c_id, a.id AS a_id, "
                    "c.c_data AS c_c_data, "
                    "c.d_data AS c_d_data, c.e_data AS c_e_data "
                    "FROM a JOIN c ON a.id = c.id "
                    "WHERE a.id IN ([POSTCOMPILE_primary_keys]) ORDER BY a.id",
                    [{"primary_keys": [1, 2]}],
                ),
            ),
        )
        with self.assert_statement_count(testing.db, 0):
            eq_(result, [d(d_data="d1"), e(e_data="e1")])

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
                "SELECT a.type AS a_type, a.id AS a_id, "
                "a.a_data AS a_a_data FROM a",
                {},
            ),
            Or(
                CompiledSQL(
                    "SELECT a.type AS a_type, c.id AS c_id, a.id AS a_id, "
                    "c.c_data AS c_c_data, c.e_data AS c_e_data, "
                    "c.d_data AS c_d_data "
                    "FROM a JOIN c ON a.id = c.id "
                    "WHERE a.id IN ([POSTCOMPILE_primary_keys]) ORDER BY a.id",
                    [{"primary_keys": [1, 2]}],
                ),
                CompiledSQL(
                    "SELECT a.type AS a_type, c.id AS c_id, a.id AS a_id, "
                    "c.c_data AS c_c_data, c.d_data AS c_d_data, "
                    "c.e_data AS c_e_data "
                    "FROM a JOIN c ON a.id = c.id "
                    "WHERE a.id IN ([POSTCOMPILE_primary_keys]) ORDER BY a.id",
                    [{"primary_keys": [1, 2]}],
                ),
            ),
        )
        with self.assert_statement_count(testing.db, 0):
            eq_(result, [d(d_data="d1"), e(e_data="e1")])

    def test_threelevel_selectin_to_inline_awkward_alias_options(self):
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
        sess.add_all([d(d_data="d1"), e(e_data="e1")])
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
        q = (
            sess.query(a)
            .options(selectin_polymorphic(a, [b, c_alias]))
            .order_by(a.id)
        )

        result = self.assert_sql_execution(
            testing.db,
            q.all,
            CompiledSQL(
                "SELECT a.type AS a_type, a.id AS a_id, "
                "a.a_data AS a_a_data FROM a ORDER BY a.id",
                {},
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
                    "WHERE poly.a_id IN ([POSTCOMPILE_primary_keys]) "
                    "ORDER BY poly.a_id",
                    [{"primary_keys": [1, 2]}],
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
                    "WHERE poly.a_id IN ([POSTCOMPILE_primary_keys]) "
                    "ORDER BY poly.a_id",
                    [{"primary_keys": [1, 2]}],
                ),
            ),
        )
        with self.assert_statement_count(testing.db, 0):
            eq_(result, [d(d_data="d1"), e(e_data="e1")])

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

        class Parent(fixtures.ComparableEntity, Base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)

        class Child(fixtures.ComparableEntity, Base):
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

        class Other(fixtures.ComparableEntity, Base):
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

    def test_options_dont_pollute_baked(self):
        self._test_options_dont_pollute(True)

    def test_options_dont_pollute_unbaked(self):
        self._test_options_dont_pollute(False)

    def _test_options_dont_pollute(self, enable_baked):
        Parent, ChildSubclass1, Other = self.classes(
            "Parent", "ChildSubclass1", "Other"
        )
        session = fixture_session(enable_baked_queries=enable_baked)

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
                    "child.parent_id AS child_parent_id, "
                    "child.type AS child_type "
                    "FROM child JOIN child_subclass1 "
                    "ON child.id = child_subclass1.id "
                    "WHERE child.id IN ([POSTCOMPILE_primary_keys]) "
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
                "child.id AS child_id, child.parent_id AS child_parent_id, "
                "child.type AS child_type, other_1.id AS other_1_id, "
                "other_1.child_subclass_id AS other_1_child_subclass_id "
                "FROM child JOIN child_subclass1 "
                "ON child.id = child_subclass1.id "
                "LEFT OUTER JOIN other AS other_1 "
                "ON child_subclass1.id = other_1.child_subclass_id "
                "WHERE child.id IN ([POSTCOMPILE_primary_keys]) "
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
