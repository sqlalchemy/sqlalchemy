from __future__ import annotations

from sqlalchemy import delete
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import expect_raises_message
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column


class InheritTest(fixtures.DeclarativeMappedTest):
    run_inserts = "each"

    run_deletes = "each"
    __backend__ = True

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Person(Base):
            __tablename__ = "person"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            type = Column(String(50))
            name = Column(String(50))

        class Engineer(Person):
            __tablename__ = "engineer"
            id = Column(Integer, ForeignKey("person.id"), primary_key=True)
            engineer_name = Column(String(50))

        class Programmer(Engineer):
            __tablename__ = "programmer"
            id = Column(Integer, ForeignKey("engineer.id"), primary_key=True)
            primary_language = Column(String(50))

        class Manager(Person):
            __tablename__ = "manager"
            id = Column(Integer, ForeignKey("person.id"), primary_key=True)
            manager_name = Column(String(50))

    @classmethod
    def insert_data(cls, connection):
        Engineer, Person, Manager, Programmer = (
            cls.classes.Engineer,
            cls.classes.Person,
            cls.classes.Manager,
            cls.classes.Programmer,
        )
        s = Session(connection)
        s.add_all(
            [
                Engineer(name="e1", engineer_name="e1"),
                Manager(name="m1", manager_name="m1"),
                Engineer(name="e2", engineer_name="e2"),
                Person(name="p1"),
                Programmer(
                    name="pp1", engineer_name="pp1", primary_language="python"
                ),
            ]
        )
        s.commit()

    @testing.only_on(["mysql", "mariadb"], "Multi table update")
    def test_update_from_join_no_problem(self):
        person = self.classes.Person.__table__
        engineer = self.classes.Engineer.__table__

        sess = fixture_session()
        sess.query(person.join(engineer)).filter(person.c.name == "e2").update(
            {person.c.name: "updated", engineer.c.engineer_name: "e2a"},
        )
        obj = sess.execute(
            select(self.classes.Engineer).filter(
                self.classes.Engineer.name == "updated"
            )
        ).scalar()
        eq_(obj.name, "updated")
        eq_(obj.engineer_name, "e2a")

    @testing.combinations(None, "fetch", "evaluate")
    def test_update_sub_table_only(self, synchronize_session):
        Engineer = self.classes.Engineer
        s = Session(testing.db)
        s.query(Engineer).update(
            {"engineer_name": "e5"}, synchronize_session=synchronize_session
        )

        eq_(s.query(Engineer.engineer_name).all(), [("e5",), ("e5",), ("e5",)])

    @testing.combinations(None, "fetch", "evaluate")
    def test_update_sub_sub_table_only(self, synchronize_session):
        Programmer = self.classes.Programmer
        s = Session(testing.db)
        s.query(Programmer).update(
            {"primary_language": "c++"},
            synchronize_session=synchronize_session,
        )

        eq_(
            s.query(Programmer.primary_language).all(),
            [
                ("c++",),
            ],
        )

    @testing.requires.update_from
    @testing.combinations(None, "fetch", "fetch_w_hint", "evaluate")
    def test_update_from(self, synchronize_session):
        """test an UPDATE that uses multiple tables.

        The limitation that MariaDB has with DELETE does not apply here at the
        moment as MariaDB doesn't support UPDATE..RETURNING at all. However,
        the logic from DELETE is still implemented in persistence.py. If
        MariaDB adds UPDATE...RETURNING, then it may be useful. SQLite,
        PostgreSQL, MSSQL all support UPDATE..FROM however RETURNING seems to
        function correctly for all three.

        """
        Engineer = self.classes.Engineer
        Person = self.classes.Person
        s = Session(testing.db)

        # we don't have any backends with this combination right now.
        db_has_hypothetical_limitation = (
            testing.db.dialect.update_returning
            and not testing.db.dialect.update_returning_multifrom
        )

        e2 = s.query(Engineer).filter_by(name="e2").first()

        with self.sql_execution_asserter() as asserter:
            eq_(e2.engineer_name, "e2")
            q = (
                s.query(Engineer)
                .filter(Engineer.id == Person.id)
                .filter(Person.name == "e2")
            )
            if synchronize_session == "fetch_w_hint":
                q.execution_options(is_update_from=True).update(
                    {"engineer_name": "e5"},
                    synchronize_session="fetch",
                )
            elif (
                synchronize_session == "fetch"
                and db_has_hypothetical_limitation
            ):
                with expect_raises_message(
                    exc.CompileError,
                    'Dialect ".*" does not support RETURNING with '
                    "UPDATE..FROM;",
                ):
                    q.update(
                        {"engineer_name": "e5"},
                        synchronize_session=synchronize_session,
                    )
                return
            else:
                q.update(
                    {"engineer_name": "e5"},
                    synchronize_session=synchronize_session,
                )

            if synchronize_session is None:
                eq_(e2.engineer_name, "e2")
            else:
                eq_(e2.engineer_name, "e5")

        if synchronize_session in ("fetch", "fetch_w_hint") and (
            db_has_hypothetical_limitation
            or not testing.db.dialect.update_returning
        ):
            asserter.assert_(
                CompiledSQL(
                    "SELECT person.id FROM person INNER JOIN engineer "
                    "ON person.id = engineer.id WHERE engineer.id = person.id "
                    "AND person.name = %s",
                    [{"name_1": "e2"}],
                    dialect="mariadb",
                ),
                CompiledSQL(
                    "UPDATE engineer, person SET engineer.engineer_name=%s "
                    "WHERE engineer.id = person.id AND person.name = %s",
                    [{"engineer_name": "e5", "name_1": "e2"}],
                    dialect="mariadb",
                ),
            )
        elif synchronize_session in ("fetch", "fetch_w_hint"):
            asserter.assert_(
                CompiledSQL(
                    "UPDATE engineer SET engineer_name=%(engineer_name)s "
                    "FROM person WHERE engineer.id = person.id "
                    "AND person.name = %(name_1)s RETURNING engineer.id",
                    [{"engineer_name": "e5", "name_1": "e2"}],
                    dialect="postgresql",
                ),
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE engineer SET engineer_name=%(engineer_name)s "
                    "FROM person WHERE engineer.id = person.id "
                    "AND person.name = %(name_1)s",
                    [{"engineer_name": "e5", "name_1": "e2"}],
                    dialect="postgresql",
                ),
            )

        eq_(
            set(s.query(Person.name, Engineer.engineer_name)),
            {("e1", "e1"), ("e2", "e5"), ("pp1", "pp1")},
        )

    @testing.requires.delete_using
    @testing.combinations(None, "fetch", "fetch_w_hint", "evaluate")
    def test_delete_using(self, synchronize_session):
        """test a DELETE that uses multiple tables.

        due to a limitation in MariaDB, we have an up front "hint" that needs
        to be passed for this backend if DELETE USING is to be used in
        conjunction with "fetch" strategy, so that we know before compilation
        that we won't be able to use RETURNING.

        """

        Engineer = self.classes.Engineer
        Person = self.classes.Person
        s = Session(testing.db)

        db_has_mariadb_limitation = (
            testing.db.dialect.delete_returning
            and not testing.db.dialect.delete_returning_multifrom
        )

        e2 = s.query(Engineer).filter_by(name="e2").first()

        with self.sql_execution_asserter() as asserter:
            assert e2 in s

            q = (
                s.query(Engineer)
                .filter(Engineer.id == Person.id)
                .filter(Person.name == "e2")
            )

            if synchronize_session == "fetch_w_hint":
                q.execution_options(is_delete_using=True).delete(
                    synchronize_session="fetch"
                )
            elif synchronize_session == "fetch" and db_has_mariadb_limitation:
                with expect_raises_message(
                    exc.CompileError,
                    'Dialect ".*" does not support RETURNING with '
                    "DELETE..USING;",
                ):
                    q.delete(synchronize_session=synchronize_session)
                return
            else:
                q.delete(synchronize_session=synchronize_session)

            if synchronize_session is None:
                assert e2 in s
            else:
                assert e2 not in s

        if synchronize_session in ("fetch", "fetch_w_hint") and (
            db_has_mariadb_limitation
            or not testing.db.dialect.delete_returning
        ):
            asserter.assert_(
                CompiledSQL(
                    "SELECT person.id FROM person INNER JOIN engineer ON "
                    "person.id = engineer.id WHERE engineer.id = person.id "
                    "AND person.name = %s",
                    [{"name_1": "e2"}],
                    dialect="mariadb",
                ),
                CompiledSQL(
                    "DELETE FROM engineer USING engineer, person WHERE "
                    "engineer.id = person.id AND person.name = %s",
                    [{"name_1": "e2"}],
                    dialect="mariadb",
                ),
            )
        elif synchronize_session in ("fetch", "fetch_w_hint"):
            asserter.assert_(
                CompiledSQL(
                    "DELETE FROM engineer USING person WHERE "
                    "engineer.id = person.id AND person.name = %(name_1)s "
                    "RETURNING engineer.id",
                    [{"name_1": "e2"}],
                    dialect="postgresql",
                ),
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "DELETE FROM engineer USING person WHERE "
                    "engineer.id = person.id AND person.name = %(name_1)s",
                    [{"name_1": "e2"}],
                    dialect="postgresql",
                ),
            )

        # delete actually worked
        eq_(
            set(s.query(Person.name, Engineer.engineer_name)),
            {("pp1", "pp1"), ("e1", "e1")},
        )

    @testing.only_on(["mysql", "mariadb"], "Multi table update")
    @testing.requires.delete_using
    @testing.combinations(None, "fetch", "evaluate")
    def test_update_from_multitable(self, synchronize_session):
        Engineer = self.classes.Engineer
        Person = self.classes.Person
        s = Session(testing.db)
        s.query(Engineer).filter(Engineer.id == Person.id).filter(
            Person.name == "e2"
        ).update(
            {Person.name: "e22", Engineer.engineer_name: "e55"},
            synchronize_session=synchronize_session,
        )

        eq_(
            set(s.query(Person.name, Engineer.engineer_name)),
            {("e1", "e1"), ("e22", "e55"), ("pp1", "pp1")},
        )


class InheritWPolyTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.fixture
    def inherit_fixture(self, decl_base):
        def go(poly_type):

            class Person(decl_base):
                __tablename__ = "person"
                id = Column(Integer, primary_key=True)
                type = Column(String(50))
                name = Column(String(50))

                if poly_type.wpoly:
                    __mapper_args__ = {"with_polymorphic": "*"}

            class Engineer(Person):
                __tablename__ = "engineer"
                id = Column(Integer, ForeignKey("person.id"), primary_key=True)
                engineer_name = Column(String(50))

                if poly_type.inline:
                    __mapper_args__ = {"polymorphic_load": "inline"}

            return Person, Engineer

        return go

    @testing.variation("poly_type", ["wpoly", "inline", "none"])
    def test_update_base_only(self, poly_type, inherit_fixture):
        Person, Engineer = inherit_fixture(poly_type)

        self.assert_compile(
            update(Person).values(name="n1"), "UPDATE person SET name=:name"
        )

    @testing.variation("poly_type", ["wpoly", "inline", "none"])
    def test_delete_base_only(self, poly_type, inherit_fixture):
        Person, Engineer = inherit_fixture(poly_type)

        self.assert_compile(delete(Person), "DELETE FROM person")

        self.assert_compile(
            delete(Person).where(Person.id == 7),
            "DELETE FROM person WHERE person.id = :id_1",
        )


class SingleTablePolymorphicTest(fixtures.DeclarativeMappedTest):
    __backend__ = True

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Staff(Base):
            __tablename__ = "staff"
            position = Column(String(10), nullable=False)
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(5))
            stats = Column(String(5))
            __mapper_args__ = {"polymorphic_on": position}

        class Sales(Staff):
            sales_stats = Column(String(5))
            __mapper_args__ = {"polymorphic_identity": "sales"}

        class Support(Staff):
            support_stats = Column(String(5))
            __mapper_args__ = {"polymorphic_identity": "support"}

    @classmethod
    def insert_data(cls, connection):
        with sessionmaker(connection).begin() as session:
            Sales, Support = (
                cls.classes.Sales,
                cls.classes.Support,
            )
            session.add_all(
                [
                    Sales(name="n1", sales_stats="1", stats="a"),
                    Sales(name="n2", sales_stats="2", stats="b"),
                    Support(name="n1", support_stats="3", stats="c"),
                    Support(name="n2", support_stats="4", stats="d"),
                ]
            )

    @testing.combinations(
        ("fetch", False),
        ("fetch", True),
        ("evaluate", False),
        ("evaluate", True),
    )
    def test_update(self, fetchstyle, newstyle):
        Staff, Sales, Support = self.classes("Staff", "Sales", "Support")

        sess = fixture_session()

        en1, en2 = (
            sess.execute(select(Sales).order_by(Sales.sales_stats))
            .scalars()
            .all()
        )
        mn1, mn2 = (
            sess.execute(select(Support).order_by(Support.support_stats))
            .scalars()
            .all()
        )

        if newstyle:
            sess.execute(
                update(Sales)
                .filter_by(name="n1")
                .values(stats="p")
                .execution_options(synchronize_session=fetchstyle)
            )
        else:
            sess.query(Sales).filter_by(name="n1").update(
                {"stats": "p"}, synchronize_session=fetchstyle
            )

        eq_(en1.stats, "p")
        eq_(mn1.stats, "c")
        eq_(
            sess.execute(
                select(Staff.position, Staff.name, Staff.stats).order_by(
                    Staff.id
                )
            ).all(),
            [
                ("sales", "n1", "p"),
                ("sales", "n2", "b"),
                ("support", "n1", "c"),
                ("support", "n2", "d"),
            ],
        )

    @testing.combinations(
        ("fetch", False),
        ("fetch", True),
        ("evaluate", False),
        ("evaluate", True),
    )
    def test_delete(self, fetchstyle, newstyle):
        Staff, Sales, Support = self.classes("Staff", "Sales", "Support")

        sess = fixture_session()
        en1, en2 = sess.query(Sales).order_by(Sales.sales_stats).all()
        mn1, mn2 = sess.query(Support).order_by(Support.support_stats).all()

        if newstyle:
            sess.execute(
                delete(Sales)
                .filter_by(name="n1")
                .execution_options(synchronize_session=fetchstyle)
            )
        else:
            sess.query(Sales).filter_by(name="n1").delete(
                synchronize_session=fetchstyle
            )
        assert en1 not in sess
        assert en2 in sess
        assert mn1 in sess
        assert mn2 in sess

        eq_(
            sess.execute(
                select(Staff.position, Staff.name, Staff.stats).order_by(
                    Staff.id
                )
            ).all(),
            [
                ("sales", "n2", "b"),
                ("support", "n1", "c"),
                ("support", "n2", "d"),
            ],
        )
