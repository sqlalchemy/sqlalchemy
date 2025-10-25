from __future__ import annotations

import uuid

from sqlalchemy import Boolean
from sqlalchemy import case
from sqlalchemy import column
from sqlalchemy import Computed
from sqlalchemy import delete
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import FetchedValue
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import lambda_stmt
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy import values
from sqlalchemy.orm import aliased
from sqlalchemy.orm import backref
from sqlalchemy.orm import Bundle
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import immediateload
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import synonym
from sqlalchemy.orm import with_loader_criteria
from sqlalchemy.sql.dml import Delete
from sqlalchemy.sql.dml import Update
from sqlalchemy.sql.selectable import Select
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import not_in
from sqlalchemy.testing.assertions import expect_raises_message
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class UpdateDeleteTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(32)),
            Column("age_int", Integer),
        )
        Table(
            "addresses",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", ForeignKey("users.id")),
            Column("email_address", String(50)),
        )

        m = MetaData()
        users_no_returning = Table(
            "users",
            m,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(32)),
            Column("age_int", Integer),
            implicit_returning=False,
        )
        cls.tables.users_no_returning = users_no_returning

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

        class UserNoReturning(cls.Comparable):
            pass

    @classmethod
    def insert_data(cls, connection):
        users = cls.tables.users

        connection.execute(
            users.insert(),
            [
                dict(id=1, name="john", age_int=25),
                dict(id=2, name="jack", age_int=47),
                dict(id=3, name="jill", age_int=29),
                dict(id=4, name="jane", age_int=37),
            ],
        )

    @testing.fixture
    def addresses_data(
        self,
    ):
        addresses = self.tables.addresses

        with testing.db.begin() as connection:
            connection.execute(
                addresses.insert(),
                [
                    dict(id=1, user_id=1, email_address="jo1"),
                    dict(id=2, user_id=1, email_address="jo2"),
                    dict(id=3, user_id=2, email_address="ja1"),
                    dict(id=4, user_id=3, email_address="ji1"),
                    dict(id=5, user_id=4, email_address="jan1"),
                ],
            )

    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User
        users = cls.tables.users

        Address = cls.classes.Address
        addresses = cls.tables.addresses

        cls.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "age": users.c.age_int,
                "addresses": relationship(Address),
            },
        )
        cls.mapper_registry.map_imperatively(Address, addresses)

        UserNoReturning = cls.classes.UserNoReturning
        users_no_returning = cls.tables.users_no_returning
        cls.mapper_registry.map_imperatively(
            UserNoReturning,
            users_no_returning,
            properties={
                "age": users_no_returning.c.age_int,
            },
        )

    @testing.combinations("default", "session_disable", "opt_disable")
    def test_autoflush(self, autoflush_option):
        User = self.classes.User

        s = fixture_session()

        u1 = User(id=5, name="x1")
        s.add(u1)

        assert_stmt = (
            select(User.name)
            .where(User.name.startswith("x"))
            .order_by(User.id)
        )
        if autoflush_option == "default":
            s.execute(update(User).values(age=5))
            assert inspect(u1).persistent
            eq_(
                s.scalars(assert_stmt).all(),
                ["x1"],
            )
        elif autoflush_option == "session_disable":
            with s.no_autoflush:
                s.execute(update(User).values(age=5))
                assert inspect(u1).pending
                eq_(
                    s.scalars(assert_stmt).all(),
                    [],
                )
        elif autoflush_option == "opt_disable":
            s.execute(
                update(User).values(age=5),
                execution_options={"autoflush": False},
            )
            assert inspect(u1).pending
            with s.no_autoflush:
                eq_(
                    s.scalars(assert_stmt).all(),
                    [],
                )
        else:
            assert False

    def test_update_dont_use_col_key(self):
        User = self.classes.User

        s = fixture_session()

        # make sure objects are present to synchronize
        _ = s.query(User).all()

        with expect_raises_message(
            exc.InvalidRequestError,
            "Attribute name not found, can't be synchronized back "
            "to objects: 'age_int'",
        ):
            s.execute(update(User).values(age_int=5))

        stmt = update(User).values(age=5)
        s.execute(stmt)
        eq_(s.scalars(select(User.age)).all(), [5, 5, 5, 5])

    @testing.combinations("table", "mapper", "both", argnames="bind_type")
    @testing.combinations(
        "update", "insert", "delete", argnames="statement_type"
    )
    def test_get_bind_scenarios(self, connection, bind_type, statement_type):
        """test for #7936"""

        User = self.classes.User

        if statement_type == "insert":
            stmt = insert(User).values(
                {User.id: 5, User.age: 25, User.name: "spongebob"}
            )
        elif statement_type == "update":
            stmt = (
                update(User)
                .where(User.id == 2)
                .values({User.name: "spongebob"})
            )
        elif statement_type == "delete":
            stmt = delete(User)

        binds = {}
        if bind_type == "both":
            binds = {User: connection, User.__table__: connection}
        elif bind_type == "mapper":
            binds = {User: connection}
        elif bind_type == "table":
            binds = {User.__table__: connection}

        with Session(binds=binds) as sess:
            sess.execute(stmt)

    def test_illegal_eval(self):
        User = self.classes.User
        s = fixture_session()
        assert_raises_message(
            exc.ArgumentError,
            "Valid strategies for session synchronization "
            "are 'auto', 'evaluate', 'fetch', False",
            s.query(User).update,
            {},
            synchronize_session="fake",
        )

    @testing.requires.table_value_constructor
    def test_update_against_external_non_mapped_cols(self):
        """test #8656"""
        User = self.classes.User

        data = values(
            column("id", Integer),
            column("name", String),
            column("age_int", Integer),
            name="myvalues",
        ).data(
            [
                (1, "new john", 35),
                (3, "new jill", 39),
            ]
        )

        # this statement will use "fetch" strategy, as "evaluate" will
        # not be available
        stmt = (
            update(User)
            .where(User.id == data.c.id)
            .values(age=data.c.age_int, name=data.c.name)
        )
        s = fixture_session()

        john, jack, jill, jane = s.scalars(
            select(User).order_by(User.id)
        ).all()

        s.execute(stmt)

        eq_(john.name, "new john")
        eq_(jill.name, "new jill")
        eq_(john.age, 35)
        eq_(jill.age, 39)

    def test_illegal_operations(self):
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()

        for q, mname in (
            (s.query(User).limit(2), r"limit\(\)"),
            (s.query(User).offset(2), r"offset\(\)"),
            (s.query(User).limit(2).offset(2), r"limit\(\)"),
            (s.query(User).order_by(User.id), r"order_by\(\)"),
            (s.query(User).group_by(User.id), r"group_by\(\)"),
            (s.query(User).distinct(), r"distinct\(\)"),
            (
                s.query(User).join(User.addresses),
                r"join\(\), outerjoin\(\), select_from\(\), or from_self\(\)",
            ),
            (
                s.query(User).outerjoin(User.addresses),
                r"join\(\), outerjoin\(\), select_from\(\), or from_self\(\)",
            ),
            (
                s.query(User).select_from(Address),
                r"join\(\), outerjoin\(\), select_from\(\), or from_self\(\)",
            ),
        ):
            assert_raises_message(
                exc.InvalidRequestError,
                r"Can't call Query.update\(\) or Query.delete\(\) when "
                "%s has been called" % mname,
                q.update,
                {"name": "ed"},
            )
            assert_raises_message(
                exc.InvalidRequestError,
                r"Can't call Query.update\(\) or Query.delete\(\) when "
                "%s has been called" % mname,
                q.delete,
            )

    def test_update_w_unevaluatable_value_evaluate(self):
        """test that the "evaluate" strategy falls back to 'expire' for an
        update SET that is not evaluable in Python."""

        User = self.classes.User

        s = fixture_session()

        jill = s.query(User).filter(User.name == "jill").one()

        s.execute(
            update(User)
            .filter(User.name == "jill")
            .values({"name": User.name + User.name}),
            execution_options={"synchronize_session": "evaluate"},
        )

        eq_(jill.name, "jilljill")

    def test_update_w_unevaluatable_value_fetch(self):
        """test that the "fetch" strategy falls back to 'expire' for an
        update SET that is not evaluable in Python.

        Prior to 1.4 the "fetch" strategy used expire for everything
        but now it tries to evaluate a SET clause to avoid a round
        trip.

        """

        User = self.classes.User

        s = fixture_session()

        jill = s.query(User).filter(User.name == "jill").one()

        s.execute(
            update(User)
            .filter(User.name == "jill")
            .values({"name": User.name + User.name}),
            execution_options={"synchronize_session": "fetch"},
        )

        eq_(jill.name, "jilljill")

    def test_evaluate_clauseelement(self):
        User = self.classes.User

        class Thing:
            def __clause_element__(self):
                return User.name.__clause_element__()

        s = fixture_session()
        jill = s.get(User, 3)
        s.query(User).update(
            {Thing(): "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.name, "moonbeam")

    def test_evaluate_invalid(self):
        User = self.classes.User

        class Thing:
            def __clause_element__(self):
                return 5

        s = fixture_session()

        assert_raises_message(
            exc.ArgumentError,
            "SET/VALUES column expression or string key expected, got .*Thing",
            s.query(User).update,
            {Thing(): "moonbeam"},
            synchronize_session="evaluate",
        )

    def test_evaluate_unmapped_col(self):
        User = self.classes.User

        s = fixture_session()
        jill = s.get(User, 3)
        s.query(User).update(
            {column("name"): "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.name, "jill")
        s.expire(jill)
        eq_(jill.name, "moonbeam")

    def test_evaluate_synonym_string(self):
        class Foo:
            pass

        self.mapper_registry.map_imperatively(
            Foo, self.tables.users, properties={"uname": synonym("name")}
        )

        s = fixture_session()
        jill = s.get(Foo, 3)
        s.query(Foo).update(
            {"uname": "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.uname, "moonbeam")

    def test_evaluate_synonym_attr(self):
        class Foo:
            pass

        self.mapper_registry.map_imperatively(
            Foo, self.tables.users, properties={"uname": synonym("name")}
        )

        s = fixture_session()
        jill = s.get(Foo, 3)
        s.query(Foo).update(
            {Foo.uname: "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.uname, "moonbeam")

    def test_evaluate_double_synonym_attr(self):
        class Foo:
            pass

        self.mapper_registry.map_imperatively(
            Foo,
            self.tables.users,
            properties={"uname": synonym("name"), "ufoo": synonym("uname")},
        )

        s = fixture_session()
        jill = s.get(Foo, 3)
        s.query(Foo).update(
            {Foo.ufoo: "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.ufoo, "moonbeam")

    @testing.combinations(
        (False, False),
        (False, True),
        (True, False),
        (True, True),
    )
    def test_evaluate_dont_refresh_expired_objects(
        self, expire_jane_age, add_filter_criteria
    ):
        """test #5664.

        approach is revised in SQLAlchemy 2.0 to not pre-emptively
        unexpire the involved attributes

        """
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        sess.expire(john)
        sess.expire(jill)

        if expire_jane_age:
            sess.expire(jane, ["name", "age"])
        else:
            sess.expire(jane, ["name"])

        with self.sql_execution_asserter() as asserter:
            # using 1.x style for easier backport
            if add_filter_criteria:
                sess.query(User).filter(User.name != None).update(
                    {"age": User.age + 10}, synchronize_session="evaluate"
                )
            else:
                sess.query(User).update(
                    {"age": User.age + 10}, synchronize_session="evaluate"
                )

        if add_filter_criteria:
            if expire_jane_age:
                asserter.assert_(
                    # previously, this would unexpire the attribute and
                    # cause an additional SELECT.  The
                    # 2.0 approach is that if the object has expired attrs
                    # we just expire the whole thing, avoiding SQL up front
                    CompiledSQL(
                        "UPDATE users "
                        "SET age_int=(users.age_int + :age_int_1) "
                        "WHERE users.name IS NOT NULL",
                        [{"age_int_1": 10}],
                    ),
                )
            else:
                asserter.assert_(
                    # previously, this would unexpire the attribute and
                    # cause an additional SELECT.  The
                    # 2.0 approach is that if the object has expired attrs
                    # we just expire the whole thing, avoiding SQL up front
                    CompiledSQL(
                        "UPDATE users SET "
                        "age_int=(users.age_int + :age_int_1) "
                        "WHERE users.name IS NOT NULL",
                        [{"age_int_1": 10}],
                    ),
                )
        else:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE users SET age_int=(users.age_int + :age_int_1)",
                    [{"age_int_1": 10}],
                ),
            )

        with self.sql_execution_asserter() as asserter:
            eq_(john.age, 35)  # needs refresh
            eq_(jack.age, 57)  # no SQL needed
            eq_(jill.age, 39)  # needs refresh
            eq_(jane.age, 47)  # needs refresh

        to_assert = [
            # refresh john
            CompiledSQL(
                "SELECT users.id, users.name, users.age_int "
                "FROM users "
                "WHERE users.id = :pk_1",
                [{"pk_1": 1}],
            ),
            # refresh jill
            CompiledSQL(
                "SELECT users.id, users.name, users.age_int "
                "FROM users "
                "WHERE users.id = :pk_1",
                [{"pk_1": 3}],
            ),
        ]

        if expire_jane_age:
            to_assert.append(
                # refresh jane for partial attributes
                CompiledSQL(
                    "SELECT users.name, users.age_int "
                    "FROM users "
                    "WHERE users.id = :pk_1",
                    [{"pk_1": 4}],
                )
            )
        asserter.assert_(*to_assert)

    @testing.combinations(True, False, argnames="is_evaluable")
    def test_auto_synchronize(self, is_evaluable):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        if is_evaluable:
            crit = or_(User.name == "jack", User.name == "jane")
        else:
            crit = case((User.name.in_(["jack", "jane"]), True), else_=False)

        with self.sql_execution_asserter() as asserter:
            sess.execute(update(User).where(crit).values(age=User.age + 10))

        if is_evaluable:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE users SET age_int=(users.age_int + :age_int_1) "
                    "WHERE users.name = :name_1 OR users.name = :name_2",
                    [{"age_int_1": 10, "name_1": "jack", "name_2": "jane"}],
                ),
            )
        elif testing.db.dialect.update_returning:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE users SET age_int=(users.age_int + :age_int_1) "
                    "WHERE CASE WHEN (users.name IN (__[POSTCOMPILE_name_1])) "
                    "THEN :param_1 ELSE :param_2 END = 1 RETURNING users.id",
                    [
                        {
                            "age_int_1": 10,
                            "name_1": ["jack", "jane"],
                            "param_1": True,
                            "param_2": False,
                        }
                    ],
                ),
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id FROM users WHERE CASE WHEN "
                    "(users.name IN (__[POSTCOMPILE_name_1])) "
                    "THEN :param_1 ELSE :param_2 END = 1",
                    [
                        {
                            "name_1": ["jack", "jane"],
                            "param_1": True,
                            "param_2": False,
                        }
                    ],
                ),
                CompiledSQL(
                    "UPDATE users SET age_int=(users.age_int + :age_int_1) "
                    "WHERE CASE WHEN (users.name IN (__[POSTCOMPILE_name_1])) "
                    "THEN :param_1 ELSE :param_2 END = 1",
                    [
                        {
                            "age_int_1": 10,
                            "name_1": ["jack", "jane"],
                            "param_1": True,
                            "param_2": False,
                        }
                    ],
                ),
            )

    def test_fetch_dont_refresh_expired_objects(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        sess.expire(john)
        sess.expire(jill)
        sess.expire(jane, ["name"])

        with self.sql_execution_asserter() as asserter:
            # using 1.x style for easier backport
            sess.query(User).filter(User.name != None).update(
                {"age": User.age + 10}, synchronize_session="fetch"
            )

        if testing.db.dialect.update_returning:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE users SET age_int=(users.age_int + %(age_int_1)s) "
                    "WHERE users.name IS NOT NULL "
                    "RETURNING users.id",
                    [{"age_int_1": 10}],
                    dialect="postgresql",
                ),
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id FROM users "
                    "WHERE users.name IS NOT NULL"
                ),
                CompiledSQL(
                    "UPDATE users SET age_int=(users.age_int + :age_int_1) "
                    "WHERE users.name IS NOT NULL",
                    [{"age_int_1": 10}],
                ),
            )

        with self.sql_execution_asserter() as asserter:
            eq_(john.age, 35)  # needs refresh
            eq_(jack.age, 57)  # no SQL needed
            eq_(jill.age, 39)  # needs refresh
            eq_(jane.age, 47)  # no SQL needed

        asserter.assert_(
            # refresh john
            CompiledSQL(
                "SELECT users.id, users.name, users.age_int "
                "FROM users "
                "WHERE users.id = :pk_1",
                [{"pk_1": 1}],
            ),
            # refresh jill
            CompiledSQL(
                "SELECT users.id, users.name, users.age_int "
                "FROM users "
                "WHERE users.id = :pk_1",
                [{"pk_1": 3}],
            ),
        )

    @testing.combinations(False, None, "auto", "evaluate", "fetch")
    def test_delete(self, synchronize_session):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        stmt = delete(User).filter(
            or_(User.name == "john", User.name == "jill")
        )
        if synchronize_session is not None:
            stmt = stmt.execution_options(
                synchronize_session=synchronize_session
            )
        sess.execute(stmt)

        if synchronize_session not in (False, None):
            assert john not in sess and jill not in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack, jane])

    def test_delete_against_metadata(self):
        User = self.classes.User
        users = self.tables.users

        sess = fixture_session()
        sess.query(users).delete(synchronize_session=False)
        eq_(sess.query(User).count(), 0)

    def test_delete_with_bindparams(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(text("name = :name")).params(
            name="john"
        ).delete("fetch")
        assert john not in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack, jill, jane])

    def test_delete_rollback(self):
        User = self.classes.User

        sess = fixture_session()
        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(
            or_(User.name == "john", User.name == "jill")
        ).delete(synchronize_session="evaluate")
        assert john not in sess and jill not in sess
        sess.rollback()
        assert john in sess and jill in sess

    def test_delete_rollback_with_fetch(self):
        User = self.classes.User

        sess = fixture_session()
        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(
            or_(User.name == "john", User.name == "jill")
        ).delete(synchronize_session="fetch")
        assert john not in sess and jill not in sess
        sess.rollback()
        assert john in sess and jill in sess

    def test_delete_without_session_sync(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(
            or_(User.name == "john", User.name == "jill")
        ).delete(synchronize_session=False)

        assert john in sess and jill in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack, jane])

    def test_delete_with_fetch_strategy(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(
            or_(User.name == "john", User.name == "jill")
        ).delete(synchronize_session="fetch")

        assert john not in sess and jill not in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack, jane])

    @testing.requires.update_where_target_in_subquery
    def test_delete_invalid_evaluation(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        assert_raises(
            exc.InvalidRequestError,
            sess.query(User)
            .filter(User.name == select(func.max(User.name)).scalar_subquery())
            .delete,
            synchronize_session="evaluate",
        )

        sess.query(User).filter(
            User.name == select(func.max(User.name)).scalar_subquery()
        ).delete(synchronize_session="fetch")

        assert john not in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack, jill, jane])

    def test_delete_bulk_not_supported(self):
        User = self.classes.User

        sess = fixture_session()

        with expect_raises_message(
            exc.InvalidRequestError, "Bulk ORM DELETE not supported right now."
        ):
            sess.execute(
                delete(User),
                [{"id": 1}, {"id": 2}],
            )

    def test_update(self):
        User, users = self.classes.User, self.tables.users

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        sess.query(User).filter(User.age > 29).update(
            {"age": User.age - 10}, synchronize_session="evaluate"
        )

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])

        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([25, 37, 29, 27])),
        )

        sess.query(User).filter(User.age > 29).update(
            {User.age: User.age - 10}, synchronize_session="evaluate"
        )
        eq_([john.age, jack.age, jill.age, jane.age], [25, 27, 29, 27])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([25, 27, 29, 27])),
        )

        sess.query(User).filter(User.age > 27).update(
            {users.c.age_int: User.age - 10}, synchronize_session="evaluate"
        )
        eq_([john.age, jack.age, jill.age, jane.age], [25, 27, 19, 27])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([25, 27, 19, 27])),
        )

        sess.query(User).filter(User.age == 25).update(
            {User.age: User.age - 10}, synchronize_session="fetch"
        )
        eq_([john.age, jack.age, jill.age, jane.age], [15, 27, 19, 27])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([15, 27, 19, 27])),
        )

    def test_update_newstyle(self):
        User, users = self.classes.User, self.tables.users

        sess = fixture_session()

        john, jack, jill, jane = (
            sess.execute(select(User).order_by(User.id)).scalars().all()
        )

        result = sess.execute(
            update(User)
            .where(User.age > 29)
            .values({"age": User.age - 10})
            .execution_options(synchronize_session="evaluate"),
        )

        eq_(result.rowcount, 2)
        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])
        eq_(
            sess.execute(select(User.age).order_by(User.id)).all(),
            list(zip([25, 37, 29, 27])),
        )

        result = sess.execute(
            update(User)
            .where(User.age > 29)
            .values({User.age: User.age - 10})
            .execution_options(synchronize_session="fetch")
        )
        eq_([john.age, jack.age, jill.age, jane.age], [25, 27, 29, 27])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([25, 27, 29, 27])),
        )

        sess.query(User).filter(User.age > 27).update(
            {users.c.age_int: User.age - 10}, synchronize_session="evaluate"
        )
        eq_([john.age, jack.age, jill.age, jane.age], [25, 27, 19, 27])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([25, 27, 19, 27])),
        )

        sess.query(User).filter(User.age == 25).update(
            {User.age: User.age - 10}, synchronize_session="fetch"
        )
        eq_([john.age, jack.age, jill.age, jane.age], [15, 27, 19, 27])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([15, 27, 19, 27])),
        )

    @testing.variation("values_first", [True, False])
    def test_update_newstyle_lambda(self, values_first):
        User, users = self.classes.User, self.tables.users

        sess = fixture_session()

        john, jack, jill, jane = (
            sess.execute(select(User).order_by(User.id)).scalars().all()
        )

        new_value = 10

        if values_first:
            stmt = lambda_stmt(lambda: update(User))
            stmt += lambda s: s.values({"age": User.age - new_value})
            stmt += lambda s: s.where(User.age > 29).execution_options(
                synchronize_session="evaluate"
            )
        else:
            stmt = lambda_stmt(
                lambda: update(User)
                .where(User.age > 29)
                .values({"age": User.age - new_value})
                .execution_options(synchronize_session="evaluate")
            )
        sess.execute(stmt)

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])
        eq_(
            sess.execute(select(User.age).order_by(User.id)).all(),
            list(zip([25, 37, 29, 27])),
        )

        if values_first:
            stmt = lambda_stmt(lambda: update(User))
            stmt += lambda s: s.values({"age": User.age - new_value})
            stmt += lambda s: s.where(User.age > 29).execution_options(
                synchronize_session="evaluate"
            )
        else:
            stmt = lambda_stmt(
                lambda: update(User)
                .where(User.age > 29)
                .values({User.age: User.age - 10})
                .execution_options(synchronize_session="evaluate")
            )

        sess.execute(stmt)
        eq_([john.age, jack.age, jill.age, jane.age], [25, 27, 29, 27])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([25, 27, 29, 27])),
        )

        sess.query(User).filter(User.age > 27).update(
            {users.c.age_int: User.age - 10}, synchronize_session="evaluate"
        )
        eq_([john.age, jack.age, jill.age, jane.age], [25, 27, 19, 27])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([25, 27, 19, 27])),
        )

        sess.query(User).filter(User.age == 25).update(
            {User.age: User.age - 10}, synchronize_session="fetch"
        )
        eq_([john.age, jack.age, jill.age, jane.age], [15, 27, 19, 27])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([15, 27, 19, 27])),
        )

    @testing.combinations(
        ("fetch",),
        ("evaluate",),
    )
    def test_update_with_loader_criteria(self, fetchstyle):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = (
            sess.execute(select(User).order_by(User.id)).scalars().all()
        )

        sess.execute(
            update(User)
            .options(
                with_loader_criteria(User, User.name.in_(["jill", "jane"]))
            )
            .where(User.age > 29)
            .values(age=User.age - 10)
            .execution_options(synchronize_session=fetchstyle)
        )

        eq_([john.age, jack.age, jill.age, jane.age], [25, 47, 29, 27])
        eq_(
            sess.execute(select(User.age).order_by(User.id)).all(),
            list(zip([25, 47, 29, 27])),
        )

    @testing.combinations(
        ("fetch",),
        ("evaluate",),
    )
    def test_delete_with_loader_criteria(self, fetchstyle):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = (
            sess.execute(select(User).order_by(User.id)).scalars().all()
        )

        sess.execute(
            delete(User)
            .options(
                with_loader_criteria(User, User.name.in_(["jill", "jane"]))
            )
            .where(User.age > 29)
            .execution_options(synchronize_session=fetchstyle)
        )

        assert jane not in sess
        assert jack in sess
        eq_(
            sess.execute(select(User.age).order_by(User.id)).all(),
            list(zip([25, 47, 29])),
        )

    def test_update_against_table_col(self):
        User, users = self.classes.User, self.tables.users

        sess = fixture_session()
        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        eq_([john.age, jack.age, jill.age, jane.age], [25, 47, 29, 37])
        sess.query(User).filter(User.age > 27).update(
            {users.c.age_int: User.age - 10}, synchronize_session="evaluate"
        )
        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 19, 27])

    def test_update_against_metadata(self):
        User, users = self.classes.User, self.tables.users

        sess = fixture_session()

        sess.query(users).update(
            {users.c.age_int: 29}, synchronize_session=False
        )
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([29, 29, 29, 29])),
        )

    def test_update_with_bindparams(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        sess.query(User).filter(text("age_int > :x")).params(x=29).update(
            {"age": User.age - 10}, synchronize_session="fetch"
        )

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([25, 37, 29, 27])),
        )

    @testing.requires.update_returning
    @testing.requires.returning_star
    def test_update_returning_star(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        stmt = (
            update(User)
            .where(User.age > 29)
            .values({"age": User.age - 10})
            .returning(literal_column("*"))
        )

        result = sess.execute(stmt)
        eq_(set(result), {(2, "jack", 37), (4, "jane", 27)})

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([25, 37, 29, 27])),
        )

    @testing.requires.update_returning
    @testing.combinations(
        selectinload,
        immediateload,
        argnames="loader_fn",
    )
    @testing.variation("opt_location", ["statement", "execute"])
    def test_update_returning_eagerload_propagate(
        self, loader_fn, connection, opt_location
    ):
        User = self.classes.User

        catch_opts = []

        @event.listens_for(connection, "before_cursor_execute")
        def before_cursor_execute(
            conn, cursor, statement, parameters, context, executemany
        ):
            catch_opts.append(
                {
                    k: v
                    for k, v in context.execution_options.items()
                    if isinstance(k, str)
                    and k[0] != "_"
                    and k not in ("sa_top_level_orm_context",)
                }
            )

        sess = Session(connection)

        stmt = (
            update(User)
            .where(User.age > 29)
            .values({"age": User.age - 10})
            .returning(User)
            .options(loader_fn(User.addresses))
        )

        if opt_location.execute:
            opts = {
                "compiled_cache": None,
                "user_defined": "opt1",
                "schema_translate_map": {"foo": "bar"},
            }
            result = sess.scalars(
                stmt,
                execution_options=opts,
            )
        elif opt_location.statement:
            opts = {
                "user_defined": "opt1",
                "schema_translate_map": {"foo": "bar"},
            }
            stmt = stmt.execution_options(**opts)
            result = sess.scalars(stmt)
        else:
            result = ()
            opts = None
            opt_location.fail()

        for u1 in result:
            u1.addresses

        for elem in catch_opts:
            eq_(elem, opts)

    @testing.combinations(True, False, argnames="implicit_returning")
    def test_update_fetch_returning(self, implicit_returning):
        if implicit_returning:
            User = self.classes.User
        else:
            User = self.classes.UserNoReturning

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        with self.sql_execution_asserter() as asserter:
            sess.query(User).filter(User.age > 29).update(
                {"age": User.age - 10}, synchronize_session="fetch"
            )

            # these are simple values, these are now evaluated even with
            # the "fetch" strategy, new in 1.4, so there is no expiry
            eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])

        if implicit_returning and testing.db.dialect.update_returning:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE users SET age_int=(users.age_int - %(age_int_1)s) "
                    "WHERE users.age_int > %(age_int_2)s RETURNING users.id",
                    [{"age_int_1": 10, "age_int_2": 29}],
                    dialect="postgresql",
                ),
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id FROM users "
                    "WHERE users.age_int > :age_int_1",
                    [{"age_int_1": 29}],
                ),
                CompiledSQL(
                    "UPDATE users SET age_int=(users.age_int - :age_int_1) "
                    "WHERE users.age_int > :age_int_2",
                    [{"age_int_1": 10, "age_int_2": 29}],
                ),
            )

    def test_update_fetch_returning_lambda(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = (
            sess.execute(select(User).order_by(User.id)).scalars().all()
        )

        with self.sql_execution_asserter() as asserter:
            stmt = lambda_stmt(
                lambda: update(User)
                .where(User.age > 29)
                .values({"age": User.age - 10})
            )
            sess.execute(
                stmt, execution_options={"synchronize_session": "fetch"}
            )

            # these are simple values, these are now evaluated even with
            # the "fetch" strategy, new in 1.4, so there is no expiry
            eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])

        if testing.db.dialect.update_returning:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE users SET age_int=(users.age_int - %(age_int_1)s) "
                    "WHERE users.age_int > %(age_int_2)s RETURNING users.id",
                    [{"age_int_1": 10, "age_int_2": 29}],
                    dialect="postgresql",
                ),
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id FROM users "
                    "WHERE users.age_int > :age_int_1",
                    [{"age_int_1": 29}],
                ),
                CompiledSQL(
                    "UPDATE users SET age_int=(users.age_int - :age_int_1) "
                    "WHERE users.age_int > :age_int_2",
                    [{"age_int_1": 10, "age_int_2": 29}],
                ),
            )

    @testing.requires.update_returning
    def test_update_evaluate_w_explicit_returning(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        with self.sql_execution_asserter() as asserter:
            stmt = (
                update(User)
                .filter(User.age > 29)
                .values({"age": User.age - 10})
                .returning(User.id)
                .execution_options(synchronize_session="evaluate")
            )

            rows = sess.execute(stmt).all()
            eq_(set(rows), {(2,), (4,)})

            # these are simple values, these are now evaluated even with
            # the "fetch" strategy, new in 1.4, so there is no expiry
            eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])

        asserter.assert_(
            CompiledSQL(
                "UPDATE users SET age_int=(users.age_int - %(age_int_1)s) "
                "WHERE users.age_int > %(age_int_2)s RETURNING users.id",
                [{"age_int_1": 10, "age_int_2": 29}],
                dialect="postgresql",
            ),
        )

    @testing.requires.update_from_returning
    # can't use evaluate because it can't match the col->col in the WHERE
    @testing.combinations("fetch", "auto", argnames="synchronize_session")
    def test_update_from_multi_returning(
        self, synchronize_session, addresses_data
    ):
        """test #12327"""
        User = self.classes.User
        Address = self.classes.Address

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        with self.sql_execution_asserter() as asserter:
            stmt = (
                update(User)
                .where(User.id == Address.user_id)
                .filter(User.age > 29)
                .values({"age": User.age - 10})
                .returning(
                    User.id, Address.email_address, func.char_length(User.name)
                )
                .execution_options(synchronize_session=synchronize_session)
            )

            rows = sess.execute(stmt).all()
            eq_(set(rows), {(2, "ja1", 4), (4, "jan1", 4)})

            # these are simple values, these are now evaluated even with
            # the "fetch" strategy, new in 1.4, so there is no expiry
            eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])

        asserter.assert_(
            CompiledSQL(
                "UPDATE users SET age_int=(users.age_int - %(age_int_1)s) "
                "FROM addresses "
                "WHERE users.id = addresses.user_id AND "
                "users.age_int > %(age_int_2)s "
                "RETURNING users.id, addresses.email_address, "
                "char_length(users.name) AS char_length_1",
                [{"age_int_1": 10, "age_int_2": 29}],
                dialect="postgresql",
            ),
        )

    @testing.requires.update_returning
    @testing.combinations("update", "delete", argnames="crud_type")
    def test_fetch_w_explicit_returning(self, crud_type):
        User = self.classes.User

        sess = fixture_session()

        if crud_type == "update":
            stmt = (
                update(User)
                .filter(User.age > 29)
                .values({"age": User.age - 10})
                .execution_options(synchronize_session="fetch")
                .returning(User, User.name)
            )
            expected = {
                (User(age=37), "jack"),
                (User(age=27), "jane"),
            }
        elif crud_type == "delete":
            stmt = (
                delete(User)
                .filter(User.age > 29)
                .execution_options(synchronize_session="fetch")
                .returning(User, User.name)
            )
            expected = {
                (User(age=47), "jack"),
                (User(age=37), "jane"),
            }
        else:
            assert False

        result = sess.execute(stmt)

        # note that ComparableEntity sets up __hash__ for mapped objects
        # to point to the class, so you can test eq with sets
        eq_(set(result.all()), expected)

    @testing.requires.update_returning
    @testing.variation("crud_type", ["update", "delete"])
    @testing.combinations(
        "auto",
        "evaluate",
        "fetch",
        False,
        argnames="synchronize_session",
    )
    def test_crud_returning_bundle(self, crud_type, synchronize_session):
        """test #10776"""
        User = self.classes.User

        sess = fixture_session()

        if crud_type.update:
            stmt = (
                update(User)
                .filter(User.age > 29)
                .values({"age": User.age - 10})
                .execution_options(synchronize_session=synchronize_session)
                .returning(Bundle("mybundle", User.id, User.age), User.name)
            )
            expected = {((4, 27), "jane"), ((2, 37), "jack")}
        elif crud_type.delete:
            stmt = (
                delete(User)
                .filter(User.age > 29)
                .execution_options(synchronize_session=synchronize_session)
                .returning(Bundle("mybundle", User.id, User.age), User.name)
            )
            expected = {((2, 47), "jack"), ((4, 37), "jane")}
        else:
            crud_type.fail()

        result = sess.execute(stmt)

        eq_(set(result.all()), expected)

    @testing.requires.delete_returning
    @testing.requires.returning_star
    def test_delete_returning_star(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        in_(john, sess)
        in_(jack, sess)

        stmt = delete(User).where(User.age > 29).returning(literal_column("*"))

        result = sess.execute(stmt)
        eq_(result.all(), [(2, "jack", 47), (4, "jane", 37)])

        in_(john, sess)
        not_in(jack, sess)
        in_(jill, sess)
        not_in(jane, sess)

    @testing.combinations(True, False, argnames="implicit_returning")
    def test_delete_fetch_returning(self, implicit_returning):
        if implicit_returning:
            User = self.classes.User
        else:
            User = self.classes.UserNoReturning

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        in_(john, sess)
        in_(jack, sess)

        with self.sql_execution_asserter() as asserter:
            sess.query(User).filter(User.age > 29).delete(
                synchronize_session="fetch"
            )

        if implicit_returning and testing.db.dialect.delete_returning:
            asserter.assert_(
                CompiledSQL(
                    "DELETE FROM users WHERE users.age_int > %(age_int_1)s "
                    "RETURNING users.id",
                    [{"age_int_1": 29}],
                    dialect="postgresql",
                ),
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id FROM users "
                    "WHERE users.age_int > :age_int_1",
                    [{"age_int_1": 29}],
                ),
                CompiledSQL(
                    "DELETE FROM users WHERE users.age_int > :age_int_1",
                    [{"age_int_1": 29}],
                ),
            )

        in_(john, sess)
        not_in(jack, sess)
        in_(jill, sess)
        not_in(jane, sess)

    def test_delete_fetch_returning_lambda(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = (
            sess.execute(select(User).order_by(User.id)).scalars().all()
        )

        in_(john, sess)
        in_(jack, sess)

        with self.sql_execution_asserter() as asserter:
            stmt = lambda_stmt(lambda: delete(User).where(User.age > 29))
            sess.execute(
                stmt, execution_options={"synchronize_session": "fetch"}
            )

        if testing.db.dialect.delete_returning:
            asserter.assert_(
                CompiledSQL(
                    "DELETE FROM users WHERE users.age_int > %(age_int_1)s "
                    "RETURNING users.id",
                    [{"age_int_1": 29}],
                    dialect="postgresql",
                ),
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "SELECT users.id FROM users "
                    "WHERE users.age_int > :age_int_1",
                    [{"age_int_1": 29}],
                ),
                CompiledSQL(
                    "DELETE FROM users WHERE users.age_int > :age_int_1",
                    [{"age_int_1": 29}],
                ),
            )

        in_(john, sess)
        not_in(jack, sess)
        in_(jill, sess)
        not_in(jane, sess)

    def test_update_with_filter_statement(self):
        """test for [ticket:4556]"""

        User = self.classes.User

        sess = fixture_session()
        assert_raises(
            exc.ArgumentError,
            lambda: sess.query(User.name == "filter").update(
                {"name": "update"}
            ),
        )

    def test_update_without_load(self):
        User = self.classes.User

        sess = fixture_session()

        sess.query(User).filter(User.id == 3).update(
            {"age": 44}, synchronize_session="fetch"
        )
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([25, 47, 44, 37])),
        )

    @testing.combinations("orm", "bulk")
    def test_update_changes_resets_dirty(self, update_type):
        User = self.classes.User

        sess = fixture_session(autoflush=False)

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        john.age = 50
        jack.age = 37

        # autoflush is false.  therefore our '50' and '37' are getting
        # blown away by this operation.

        if update_type == "orm":
            sess.execute(
                update(User)
                .filter(User.age > 29)
                .values({"age": User.age - 10}),
                execution_options=dict(synchronize_session="evaluate"),
            )
        elif update_type == "bulk":
            data = [
                {"id": john.id, "age": 25},
                {"id": jack.id, "age": 37},
                {"id": jill.id, "age": 29},
                {"id": jane.id, "age": 27},
            ]

            sess.execute(
                update(User),
                data,
                execution_options=dict(synchronize_session="evaluate"),
            )

        else:
            assert False

        for x in (john, jack, jill, jane):
            assert not sess.is_modified(x)

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])

        john.age = 25
        assert john in sess.dirty
        assert jack in sess.dirty
        assert jill not in sess.dirty
        assert not sess.is_modified(john)
        assert not sess.is_modified(jack)

    @testing.combinations(
        None, False, "evaluate", "fetch", argnames="synchronize_session"
    )
    @testing.combinations(True, False, argnames="homogeneous_keys")
    def test_bulk_update_synchronize_session(
        self, synchronize_session, homogeneous_keys
    ):
        User = self.classes.User

        sess = fixture_session(expire_on_commit=False)

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        if homogeneous_keys:
            data = [
                {"id": john.id, "age": 35},
                {"id": jack.id, "age": 27},
                {"id": jill.id, "age": 30},
            ]
        else:
            data = [
                {"id": john.id, "age": 35},
                {"id": jack.id, "name": "new jack"},
                {"id": jill.id, "age": 30, "name": "new jill"},
            ]

        with self.sql_execution_asserter() as asserter:
            if synchronize_session is not None:
                opts = {"synchronize_session": synchronize_session}
            else:
                opts = {}

            if synchronize_session == "fetch":
                with expect_raises_message(
                    exc.InvalidRequestError,
                    "The 'fetch' synchronization strategy is not available "
                    "for 'bulk' ORM updates",
                ):
                    sess.execute(update(User), data, execution_options=opts)
                return
            else:
                sess.execute(update(User), data, execution_options=opts)

        if homogeneous_keys:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE users SET age_int=:age_int "
                    "WHERE users.id = :users_id",
                    [
                        {"age_int": 35, "users_id": 1},
                        {"age_int": 27, "users_id": 2},
                        {"age_int": 30, "users_id": 3},
                    ],
                )
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE users SET age_int=:age_int "
                    "WHERE users.id = :users_id",
                    [{"age_int": 35, "users_id": 1}],
                ),
                CompiledSQL(
                    "UPDATE users SET name=:name WHERE users.id = :users_id",
                    [{"name": "new jack", "users_id": 2}],
                ),
                CompiledSQL(
                    "UPDATE users SET name=:name, age_int=:age_int "
                    "WHERE users.id = :users_id",
                    [{"name": "new jill", "age_int": 30, "users_id": 3}],
                ),
            )

        if synchronize_session is False:
            eq_(jill.name, "jill")
            eq_(jack.name, "jack")
            eq_(jill.age, 29)
            eq_(jack.age, 47)
        else:
            if not homogeneous_keys:
                eq_(jill.name, "new jill")
                eq_(jack.name, "new jack")
                eq_(jack.age, 47)
            else:
                eq_(jack.age, 27)
            eq_(jill.age, 30)

    def test_update_changes_with_autoflush(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        john.age = 50
        jack.age = 37

        sess.query(User).filter(User.age > 29).update(
            {"age": User.age - 10}, synchronize_session="evaluate"
        )

        for x in (john, jack, jill, jane):
            assert not sess.is_modified(x)

        eq_([john.age, jack.age, jill.age, jane.age], [40, 27, 29, 27])

        john.age = 25
        assert john in sess.dirty
        assert jack not in sess.dirty
        assert jill not in sess.dirty
        assert sess.is_modified(john)
        assert not sess.is_modified(jack)

    def test_update_with_expire_strategy(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(User.age > 29).update(
            {"age": User.age - 10}, synchronize_session="fetch"
        )

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([25, 37, 29, 27])),
        )

    @testing.fails_if(lambda: not testing.db.dialect.supports_sane_rowcount)
    @testing.combinations("auto", "fetch", "evaluate")
    def test_update_returns_rowcount(self, synchronize_session):
        User = self.classes.User

        sess = fixture_session()

        rowcount = (
            sess.query(User)
            .filter(User.age > 29)
            .update(
                {"age": User.age + 0}, synchronize_session=synchronize_session
            )
        )
        eq_(rowcount, 2)

        rowcount = (
            sess.query(User)
            .filter(User.age > 29)
            .update(
                {"age": User.age - 10}, synchronize_session=synchronize_session
            )
        )
        eq_(rowcount, 2)

        result = sess.execute(
            update(User).where(User.age > 19).values({"age": User.age - 10}),
            execution_options={"synchronize_session": synchronize_session},
        )
        eq_(result.rowcount, 4)

    @testing.fails_if(lambda: not testing.db.dialect.supports_sane_rowcount)
    def test_delete_returns_rowcount(self):
        User = self.classes.User

        sess = fixture_session()

        rowcount = (
            sess.query(User)
            .filter(User.age > 26)
            .delete(synchronize_session=False)
        )
        eq_(rowcount, 3)

    def test_update_all(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).update({"age": 42}, synchronize_session="evaluate")

        eq_([john.age, jack.age, jill.age, jane.age], [42, 42, 42, 42])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([42, 42, 42, 42])),
        )

    def test_delete_all(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).delete(synchronize_session="evaluate")

        assert not (
            john in sess or jack in sess or jill in sess or jane in sess
        )
        eq_(sess.query(User).count(), 0)

    def test_autoflush_before_evaluate_update(self):
        User = self.classes.User

        sess = fixture_session()
        john = sess.query(User).filter_by(name="john").one()
        john.name = "j2"

        sess.query(User).filter_by(name="j2").update(
            {"age": 42}, synchronize_session="evaluate"
        )
        eq_(john.age, 42)

    def test_autoflush_before_fetch_update(self):
        User = self.classes.User

        sess = fixture_session()
        john = sess.query(User).filter_by(name="john").one()
        john.name = "j2"

        sess.query(User).filter_by(name="j2").update(
            {"age": 42}, synchronize_session="fetch"
        )
        eq_(john.age, 42)

    def test_autoflush_before_evaluate_delete(self):
        User = self.classes.User

        sess = fixture_session()
        john = sess.query(User).filter_by(name="john").one()
        john.name = "j2"

        sess.query(User).filter_by(name="j2").delete(
            synchronize_session="evaluate"
        )
        assert john not in sess

    def test_autoflush_before_fetch_delete(self):
        User = self.classes.User

        sess = fixture_session()
        john = sess.query(User).filter_by(name="john").one()
        john.name = "j2"

        sess.query(User).filter_by(name="j2").delete(
            synchronize_session="fetch"
        )
        assert john not in sess

    @testing.combinations(True, False)
    def test_evaluate_before_update(self, full_expiration):
        User = self.classes.User

        sess = fixture_session()
        john = sess.query(User).filter_by(name="john").one()

        if full_expiration:
            sess.expire(john)
        else:
            sess.expire(john, ["age"])

        # eval must be before the update.  otherwise
        # we eval john, age has been expired and doesn't
        # match the new value coming in
        sess.query(User).filter_by(name="john").filter_by(age=25).update(
            {"name": "j2", "age": 40}, synchronize_session="evaluate"
        )
        eq_(john.name, "j2")
        eq_(john.age, 40)

    def test_fetch_before_update(self):
        User = self.classes.User

        sess = fixture_session()
        john = sess.query(User).filter_by(name="john").one()
        sess.expire(john, ["age"])

        sess.query(User).filter_by(name="john").filter_by(age=25).update(
            {"name": "j2", "age": 40}, synchronize_session="fetch"
        )
        eq_(john.name, "j2")
        eq_(john.age, 40)

    @testing.combinations(True, False)
    def test_evaluate_before_delete(self, full_expiration):
        User = self.classes.User

        sess = fixture_session()
        john = sess.query(User).filter_by(name="john").one()
        jill = sess.query(User).filter_by(name="jill").one()
        jane = sess.query(User).filter_by(name="jane").one()

        if full_expiration:
            sess.expire(jill)
            sess.expire(john)
        else:
            sess.expire(jill, ["age"])
            sess.expire(john, ["age"])

        sess.query(User).filter(or_(User.age == 25, User.age == 37)).delete(
            synchronize_session="evaluate"
        )

        # was fully deleted
        assert jane not in sess

        # deleted object was expired, but not otherwise affected
        assert jill in sess

        # deleted object was expired, but not otherwise affected
        assert john in sess

        # partially expired row fully expired
        assert inspect(jill).expired

        # non-deleted row still present
        eq_(jill.age, 29)

        # partially expired row fully expired
        assert inspect(john).expired

        # is deleted
        with expect_raises(orm_exc.ObjectDeletedError):
            john.name

    def test_fetch_before_delete(self):
        User = self.classes.User

        sess = fixture_session()
        john = sess.query(User).filter_by(name="john").one()
        sess.expire(john, ["age"])

        sess.query(User).filter_by(name="john").filter_by(age=25).delete(
            synchronize_session="fetch"
        )

        assert john not in sess

    def test_update_unordered_dict(self):
        User = self.classes.User
        session = fixture_session()

        # Do an update using unordered dict and check that the parameters used
        # are ordered in table order

        m1 = testing.mock.Mock()

        @event.listens_for(session, "after_bulk_update")
        def do_orm_execute(bulk_ud):
            m1(bulk_ud.result.context.compiled.compile_state.statement)

        q = session.query(User)
        q.filter(User.id == 15).update({"name": "foob", "age": 123})
        assert m1.mock_calls[0][1][0]._values

    def test_update_preserve_parameter_order_query(self):
        User = self.classes.User
        session = fixture_session()

        # Do update using a tuple and check that order is preserved

        m1 = testing.mock.Mock()

        @event.listens_for(session, "after_bulk_update")
        def do_orm_execute(bulk_ud):
            cols = [
                c.key
                for c in (
                    (
                        bulk_ud.result.context
                    ).compiled.compile_state.statement._values
                )
            ]
            m1(cols)

        q = session.query(User)
        q.filter(User.id == 15).update(
            (("age", 123), ("name", "foob")),
            update_args={"preserve_parameter_order": True},
        )

        eq_(m1.mock_calls[0][1][0], ["age_int", "name"])

        m1.mock_calls = []

        q = session.query(User)
        q.filter(User.id == 15).update(
            [("name", "foob"), ("age", 123)],
            update_args={"preserve_parameter_order": True},
        )
        eq_(m1.mock_calls[0][1][0], ["name", "age_int"])

    def test_update_multi_values_error(self):
        User = self.classes.User
        session = fixture_session()

        # Do update using a tuple and check that order is preserved

        stmt = (
            update(User)
            .filter(User.id == 15)
            .values([("id", 123), ("name", "foob")])
        )

        assert_raises_message(
            exc.InvalidRequestError,
            "UPDATE construct does not support multiple parameter sets.",
            session.execute,
            stmt,
        )

    def test_update_preserve_parameter_order(self):
        User = self.classes.User
        session = fixture_session()

        # Do update using a tuple and check that order is preserved

        stmt = (
            update(User)
            .filter(User.id == 15)
            .ordered_values(("age", 123), ("name", "foob"))
        )
        result = session.execute(stmt)
        cols = [
            c.key
            for c in (
                (result.context).compiled.compile_state.statement._values
            )
        ]
        eq_(["age_int", "name"], cols)

        # Now invert the order and use a list instead, and check that order is
        # also preserved
        stmt = (
            update(User)
            .filter(User.id == 15)
            .ordered_values(
                ("name", "foob"),
                ("age", 123),
            )
        )
        result = session.execute(stmt)
        cols = [
            c.key
            for c in (result.context).compiled.compile_state.statement._values
        ]
        eq_(["name", "age_int"], cols)

    @testing.requires.sqlite
    def test_sharding_extension_returning_mismatch(self, testing_engine):
        """test one horizontal shard case where the given binds don't match
        for RETURNING support; we dont support this.

        See test/ext/test_horizontal_shard.py for complete round trip
        test cases for ORM update/delete

        """
        e1 = testing_engine("sqlite://")
        e2 = testing_engine("sqlite://")
        e1.connect().close()
        e2.connect().close()

        e1.dialect.update_returning = True
        e2.dialect.update_returning = False

        engines = [e1, e2]

        # a simulated version of the horizontal sharding extension
        def execute_and_instances(orm_context):
            execution_options = dict(orm_context.local_execution_options)
            partial = []
            for engine in engines:
                bind_arguments = dict(orm_context.bind_arguments)
                bind_arguments["bind"] = engine
                result_ = orm_context.invoke_statement(
                    bind_arguments=bind_arguments,
                    execution_options=execution_options,
                )

                partial.append(result_)
            return partial[0].merge(*partial[1:])

        User = self.classes.User
        session = Session()

        event.listen(
            session, "do_orm_execute", execute_and_instances, retval=True
        )

        stmt = (
            update(User)
            .filter(User.id == 15)
            .values(age=123)
            .execution_options(synchronize_session="fetch")
        )
        with expect_raises_message(
            exc.InvalidRequestError,
            "For synchronize_session='fetch', can't mix multiple backends "
            "where some support RETURNING and others don't",
        ):
            session.execute(stmt)

    @testing.combinations(("update",), ("delete",), argnames="stmt_type")
    @testing.combinations(
        ("evaluate",), ("fetch",), (None,), argnames="sync_type"
    )
    def test_routing_session(self, stmt_type, sync_type, connection):
        User = self.classes.User

        if stmt_type == "update":
            stmt = update(User).values(age=123)
            expected = [Update]
        elif stmt_type == "delete":
            stmt = delete(User)
            expected = [Delete]
        else:
            assert False

        received = []

        class RoutingSession(Session):
            def get_bind(self, **kw):
                received.append(type(kw["clause"]))
                return super().get_bind(**kw)

        stmt = stmt.execution_options(synchronize_session=sync_type)

        if sync_type == "fetch":
            expected.insert(0, Select)

            if (
                stmt_type == "update"
                and not connection.dialect.update_returning
            ):
                expected.insert(0, Select)
            elif (
                stmt_type == "delete"
                and not connection.dialect.delete_returning
            ):
                expected.insert(0, Select)

        with RoutingSession(bind=connection) as sess:
            sess.execute(stmt)

        eq_(received, expected)


class UpdateDeleteIgnoresLoadersTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(32)),
            Column("age", Integer),
        )

        Table(
            "documents",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id")),
            Column("title", String(32)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Document(cls.Comparable):
            pass

    @classmethod
    def insert_data(cls, connection):
        users = cls.tables.users

        connection.execute(
            users.insert(),
            [
                dict(id=1, name="john", age=25),
                dict(id=2, name="jack", age=47),
                dict(id=3, name="jill", age=29),
                dict(id=4, name="jane", age=37),
            ],
        )

        documents = cls.tables.documents

        connection.execute(
            documents.insert(),
            [
                dict(id=1, user_id=1, title="foo"),
                dict(id=2, user_id=1, title="bar"),
                dict(id=3, user_id=2, title="baz"),
            ],
        )

    @classmethod
    def setup_mappers(cls):
        documents, Document, User, users = (
            cls.tables.documents,
            cls.classes.Document,
            cls.classes.User,
            cls.tables.users,
        )

        cls.mapper_registry.map_imperatively(User, users)
        cls.mapper_registry.map_imperatively(
            Document,
            documents,
            properties={
                "user": relationship(
                    User,
                    lazy="joined",
                    backref=backref("documents", lazy="select"),
                )
            },
        )

    def test_update_with_eager_relationships(self):
        Document = self.classes.Document

        sess = fixture_session()

        foo, bar, baz = sess.query(Document).order_by(Document.id).all()
        sess.query(Document).filter(Document.user_id == 1).update(
            {"title": Document.title + Document.title},
            synchronize_session="fetch",
        )

        eq_([foo.title, bar.title, baz.title], ["foofoo", "barbar", "baz"])
        eq_(
            sess.query(Document.title).order_by(Document.id).all(),
            list(zip(["foofoo", "barbar", "baz"])),
        )

    def test_update_with_explicit_joinedload(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).options(joinedload(User.documents)).filter(
            User.age > 29
        ).update({"age": User.age - 10}, synchronize_session="fetch")

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])
        eq_(
            sess.query(User.age).order_by(User.id).all(),
            list(zip([25, 37, 29, 27])),
        )

    def test_delete_with_eager_relationships(self):
        Document = self.classes.Document

        sess = fixture_session()

        sess.query(Document).filter(Document.user_id == 1).delete(
            synchronize_session=False
        )

        eq_(sess.query(Document.title).all(), list(zip(["baz"])))


class UpdateDeleteFromTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("samename", String(10)),
        )
        Table(
            "documents",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", None, ForeignKey("users.id")),
            Column("title", String(32)),
            Column("flag", Boolean),
            Column("samename", String(10)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Document(cls.Comparable):
            pass

    @classmethod
    def insert_data(cls, connection):
        users = cls.tables.users

        connection.execute(
            users.insert(), [dict(id=1), dict(id=2), dict(id=3), dict(id=4)]
        )

        documents = cls.tables.documents

        connection.execute(
            documents.insert(),
            [
                dict(id=1, user_id=1, title="foo"),
                dict(id=2, user_id=1, title="bar"),
                dict(id=3, user_id=2, title="baz"),
                dict(id=4, user_id=2, title="hoho"),
                dict(id=5, user_id=3, title="lala"),
                dict(id=6, user_id=3, title="bleh"),
            ],
        )

    @classmethod
    def setup_mappers(cls):
        documents, Document, User, users = (
            cls.tables.documents,
            cls.classes.Document,
            cls.classes.User,
            cls.tables.users,
        )

        cls.mapper_registry.map_imperatively(User, users)
        cls.mapper_registry.map_imperatively(
            Document,
            documents,
            properties={"user": relationship(User, backref="documents")},
        )

    @testing.requires.update_from_using_alias
    @testing.combinations(
        False,
        ("fetch", testing.requires.update_returning),
        ("auto", testing.requires.update_returning),
        argnames="synchronize_session",
    )
    def test_update_from_alias(self, synchronize_session):
        Document = self.classes.Document
        s = fixture_session()

        d1 = aliased(Document)

        with self.sql_execution_asserter() as asserter:
            s.execute(
                update(d1).where(d1.title == "baz").values(flag=True),
                execution_options={"synchronize_session": synchronize_session},
            )

        if True:
            # TODO: see note in crud.py line 770.  RETURNING should be here
            # if synchronize_session="fetch" however there are more issues
            # with this.
            # if synchronize_session is False:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE documents AS documents_1 SET flag=:flag "
                    "WHERE documents_1.title = :title_1",
                    [{"flag": True, "title_1": "baz"}],
                )
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "UPDATE documents AS documents_1 SET flag=:flag "
                    "WHERE documents_1.title = :title_1 "
                    "RETURNING documents_1.id",
                    [{"flag": True, "title_1": "baz"}],
                )
            )

    @testing.requires.delete_using_alias
    @testing.combinations(
        False,
        ("fetch", testing.requires.delete_returning),
        ("auto", testing.requires.delete_returning),
        argnames="synchronize_session",
    )
    def test_delete_using_alias(self, synchronize_session):
        Document = self.classes.Document
        s = fixture_session()

        d1 = aliased(Document)

        with self.sql_execution_asserter() as asserter:
            s.execute(
                delete(d1).where(d1.title == "baz"),
                execution_options={"synchronize_session": synchronize_session},
            )

        if synchronize_session is False:
            asserter.assert_(
                CompiledSQL(
                    "DELETE FROM documents AS documents_1 "
                    "WHERE documents_1.title = :title_1",
                    [{"title_1": "baz"}],
                )
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "DELETE FROM documents AS documents_1 "
                    "WHERE documents_1.title = :title_1 "
                    "RETURNING documents_1.id",
                    [{"title_1": "baz"}],
                )
            )

    @testing.requires.update_from
    def test_update_from_joined_subq_test(self):
        Document = self.classes.Document
        s = fixture_session()

        subq = (
            s.query(func.max(Document.title).label("title"))
            .group_by(Document.user_id)
            .subquery()
        )

        s.query(Document).filter(Document.title == subq.c.title).update(
            {"flag": True}, synchronize_session=False
        )

        eq_(
            set(s.query(Document.id, Document.flag)),
            {
                (1, True),
                (2, None),
                (3, None),
                (4, True),
                (5, True),
                (6, None),
            },
        )

    @testing.requires.delete_using
    def test_delete_using_joined_subq_test(self):
        Document = self.classes.Document
        s = fixture_session()

        subq = (
            s.query(func.max(Document.title).label("title"))
            .group_by(Document.user_id)
            .subquery()
        )

        s.query(Document).filter(Document.title == subq.c.title).delete(
            synchronize_session=False
        )

        eq_(
            set(s.query(Document.id, Document.flag)),
            {(2, None), (3, None), (6, None)},
        )

    def test_no_eval_against_multi_table_criteria(self):
        User = self.classes.User
        Document = self.classes.Document

        s = fixture_session()

        q = s.query(User).filter(User.id == Document.user_id)

        assert_raises_message(
            exc.InvalidRequestError,
            "Could not evaluate current criteria in Python.",
            q.update,
            {"samename": "ed"},
            synchronize_session="evaluate",
        )

    @testing.requires.multi_table_update
    def test_multi_table_criteria_ok_wo_eval(self):
        User = self.classes.User
        Document = self.classes.Document

        s = fixture_session()

        q = s.query(User).filter(User.id == Document.user_id)

        q.update({Document.samename: "ed"}, synchronize_session="fetch")
        eq_(
            s.query(User.id, Document.samename, User.samename)
            .filter(User.id == Document.user_id)
            .order_by(User.id)
            .all(),
            [
                (1, "ed", None),
                (1, "ed", None),
                (2, "ed", None),
                (2, "ed", None),
                (3, "ed", None),
                (3, "ed", None),
            ],
        )

    @testing.requires.update_where_target_in_subquery
    def test_update_using_in(self):
        Document = self.classes.Document
        s = fixture_session()

        subq = (
            s.query(func.max(Document.title).label("title"))
            .group_by(Document.user_id)
            .scalar_subquery()
        )

        s.query(Document).filter(Document.title.in_(subq)).update(
            {"flag": True}, synchronize_session=False
        )

        eq_(
            set(s.query(Document.id, Document.flag)),
            {
                (1, True),
                (2, None),
                (3, None),
                (4, True),
                (5, True),
                (6, None),
            },
        )

    @testing.requires.update_where_target_in_subquery
    @testing.requires.standalone_binds
    def test_update_using_case(self):
        Document = self.classes.Document
        s = fixture_session()

        subq = (
            s.query(func.max(Document.title).label("title"))
            .group_by(Document.user_id)
            .scalar_subquery()
        )

        # this would work with Firebird if you do literal_column('1')
        # instead
        case_stmt = case((Document.title.in_(subq), True), else_=False)

        s.query(Document).update(
            {"flag": case_stmt}, synchronize_session=False
        )

        eq_(
            set(s.query(Document.id, Document.flag)),
            {
                (1, True),
                (2, False),
                (3, False),
                (4, True),
                (5, True),
                (6, False),
            },
        )

    @testing.requires.multi_table_update
    def test_update_from_multitable_same_names(self):
        Document = self.classes.Document
        User = self.classes.User

        s = fixture_session()

        s.query(Document).filter(User.id == Document.user_id).filter(
            User.id == 2
        ).update(
            {Document.samename: "d_samename", User.samename: "u_samename"},
            synchronize_session=False,
        )
        eq_(
            s.query(User.id, Document.samename, User.samename)
            .filter(User.id == Document.user_id)
            .order_by(User.id)
            .all(),
            [
                (1, None, None),
                (1, None, None),
                (2, "d_samename", "u_samename"),
                (2, "d_samename", "u_samename"),
                (3, None, None),
                (3, None, None),
            ],
        )


class ExpressionUpdateDeleteTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("counter", Integer, nullable=False, default=0),
        )

    @classmethod
    def setup_classes(cls):
        class Data(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        data = cls.tables.data
        cls.mapper_registry.map_imperatively(
            cls.classes.Data, data, properties={"cnt": data.c.counter}
        )

    @testing.provide_metadata
    def test_update_attr_names(self):
        Data = self.classes.Data

        d1 = Data()
        sess = fixture_session()
        sess.add(d1)
        sess.commit()
        eq_(d1.cnt, 0)

        sess.query(Data).update({Data.cnt: Data.cnt + 1}, "evaluate")
        sess.flush()

        eq_(d1.cnt, 1)

        sess.query(Data).update({Data.cnt: Data.cnt + 1}, "fetch")
        sess.flush()

        eq_(d1.cnt, 2)
        sess.close()

    def test_update_args(self):
        Data = self.classes.Data
        session = fixture_session()
        update_args = {"mysql_limit": 1}

        m1 = testing.mock.Mock()

        @event.listens_for(session, "after_bulk_update")
        def do_orm_execute(bulk_ud):
            update_stmt = (
                bulk_ud.result.context.compiled.compile_state.statement
            )
            m1(update_stmt)

        q = session.query(Data)
        q.update({Data.cnt: Data.cnt + 1}, update_args=update_args)

        update_stmt = m1.mock_calls[0][1][0]

        eq_(update_stmt.dialect_kwargs, update_args)

    def test_delete_args(self):
        Data = self.classes.Data
        session = fixture_session()
        delete_args = {"mysql_limit": 1}

        m1 = testing.mock.Mock()

        @event.listens_for(session, "after_bulk_delete")
        def do_orm_execute(bulk_ud):
            delete_stmt = (
                bulk_ud.result.context.compiled.compile_state.statement
            )
            m1(delete_stmt)

        q = session.query(Data)
        q.delete(delete_args=delete_args)

        delete_stmt = m1.mock_calls[0][1][0]

        eq_(delete_stmt.dialect_kwargs, delete_args)


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


class LoadFromReturningTest(fixtures.MappedTest):
    __backend__ = True
    __requires__ = ("insert_returning",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(32)),
            Column("age_int", Integer),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

    @classmethod
    def insert_data(cls, connection):
        users = cls.tables.users

        connection.execute(
            users.insert(),
            [
                dict(id=1, name="john", age_int=25),
                dict(id=2, name="jack", age_int=47),
                dict(id=3, name="jill", age_int=29),
                dict(id=4, name="jane", age_int=37),
            ],
        )

    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User
        users = cls.tables.users

        cls.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "age": users.c.age_int,
            },
        )

    @testing.requires.update_returning
    @testing.combinations(True, False, argnames="use_from_statement")
    def test_load_from_update(self, connection, use_from_statement):
        User = self.classes.User

        stmt = (
            update(User)
            .where(User.name.in_(["jack", "jill"]))
            .values(age=User.age + 5)
            .returning(User)
        )

        if use_from_statement:
            # this is now a legacy-ish case, because as of 2.0 you can just
            # use returning() directly to get the objects back.
            #
            # when from_statement is used, the UPDATE statement is no
            # longer interpreted by
            # BulkUDCompileState.orm_pre_session_exec or
            # BulkUDCompileState.orm_setup_cursor_result.  The compilation
            # level routines still take place though
            stmt = select(User).from_statement(stmt)

        with Session(connection) as sess:
            rows = sess.execute(stmt).scalars().all()

            eq_(
                rows,
                [User(name="jack", age=52), User(name="jill", age=34)],
            )

    @testing.combinations(
        ("single",),
        ("multiple", testing.requires.multivalues_inserts),
        argnames="params",
    )
    @testing.combinations(True, False, argnames="use_from_statement")
    def test_load_from_insert(self, connection, params, use_from_statement):
        User = self.classes.User

        if params == "multiple":
            values = [
                {User.id: 5, User.age: 25, User.name: "spongebob"},
                {User.id: 6, User.age: 30, User.name: "patrick"},
                {User.id: 7, User.age: 35, User.name: "squidward"},
            ]
        elif params == "single":
            values = {User.id: 5, User.age: 25, User.name: "spongebob"}
        else:
            assert False

        stmt = insert(User).values(values).returning(User)

        if use_from_statement:
            stmt = select(User).from_statement(stmt)

        with Session(connection) as sess:
            rows = sess.execute(stmt).scalars().all()

            if params == "multiple":
                eq_(
                    rows,
                    [
                        User(name="spongebob", age=25),
                        User(name="patrick", age=30),
                        User(name="squidward", age=35),
                    ],
                )
            elif params == "single":
                eq_(
                    rows,
                    [User(name="spongebob", age=25)],
                )
            else:
                assert False

    @testing.requires.delete_returning
    @testing.combinations(True, False, argnames="use_from_statement")
    def test_load_from_delete(self, connection, use_from_statement):
        User = self.classes.User

        stmt = (
            delete(User).where(User.name.in_(["jack", "jill"])).returning(User)
        )

        if use_from_statement:
            stmt = select(User).from_statement(stmt)

        with Session(connection) as sess:
            rows = sess.execute(stmt).scalars().all()

            eq_(
                rows,
                [User(name="jack", age=47), User(name="jill", age=29)],
            )

            # TODO: state of above objects should be "deleted"


class OnUpdatePopulationTest(fixtures.TestBase):
    __backend__ = True

    @testing.variation("populate_existing", [True, False])
    @testing.variation(
        "use_onupdate",
        [
            "none",
            "server",
            "callable",
            "clientsql",
            ("computed", testing.requires.computed_columns),
        ],
    )
    @testing.variation(
        "use_returning",
        [
            ("returning", testing.requires.update_returning),
            ("defaults", testing.requires.update_returning),
            "none",
        ],
    )
    @testing.variation("synchronize", ["auto", "fetch", "evaluate"])
    @testing.variation("pk_order", ["first", "middle"])
    def test_update_populate_existing(
        self,
        decl_base,
        populate_existing,
        use_onupdate,
        use_returning,
        synchronize,
        pk_order,
    ):
        """test #11912 and #11917"""

        class Employee(ComparableEntity, decl_base):
            __tablename__ = "employee"

            if pk_order.first:
                uuid: Mapped[uuid.UUID] = mapped_column(primary_key=True)
            user_name: Mapped[str] = mapped_column(String(200), nullable=False)

            if pk_order.middle:
                uuid: Mapped[uuid.UUID] = mapped_column(primary_key=True)

            if use_onupdate.server:
                some_server_value: Mapped[str] = mapped_column(
                    server_onupdate=FetchedValue()
                )
            elif use_onupdate.callable:
                some_server_value: Mapped[str] = mapped_column(
                    onupdate=lambda: "value 2"
                )
            elif use_onupdate.clientsql:
                some_server_value: Mapped[str] = mapped_column(
                    onupdate=literal("value 2")
                )
            elif use_onupdate.computed:
                some_server_value: Mapped[str] = mapped_column(
                    String(255),
                    Computed(user_name + " computed value"),
                    nullable=True,
                )
            else:
                some_server_value: Mapped[str]

        decl_base.metadata.create_all(testing.db)
        s = fixture_session()

        uuid1 = uuid.uuid4()

        if use_onupdate.computed:
            server_old_value, server_new_value = (
                "e1 old name computed value",
                "e1 new name computed value",
            )
            e1 = Employee(uuid=uuid1, user_name="e1 old name")
        else:
            server_old_value, server_new_value = ("value 1", "value 2")
            e1 = Employee(
                uuid=uuid1,
                user_name="e1 old name",
                some_server_value="value 1",
            )
        s.add(e1)
        s.flush()

        stmt = (
            update(Employee)
            .values(user_name="e1 new name")
            .where(Employee.uuid == uuid1)
        )

        if use_returning.returning:
            stmt = stmt.returning(Employee)
        elif use_returning.defaults:
            # NOTE: the return_defaults case here has not been analyzed for
            # #11912 or #11917.   future enhancements may change its behavior
            stmt = stmt.return_defaults()

        # perform out of band UPDATE on server value to simulate
        # a computed col
        if use_onupdate.none or use_onupdate.server:
            s.connection().execute(
                update(Employee.__table__).values(some_server_value="value 2")
            )

        execution_options = {}

        if populate_existing:
            execution_options["populate_existing"] = True

        if synchronize.evaluate:
            execution_options["synchronize_session"] = "evaluate"
        if synchronize.fetch:
            execution_options["synchronize_session"] = "fetch"

        if use_returning.returning:
            rows = s.scalars(stmt, execution_options=execution_options)
        else:
            s.execute(stmt, execution_options=execution_options)

        if (
            use_onupdate.clientsql
            or use_onupdate.server
            or use_onupdate.computed
        ):
            if not use_returning.defaults:
                # if server-side onupdate was generated, the col should have
                # been expired
                assert "some_server_value" not in e1.__dict__

                # and refreshes when called.  this is even if we have RETURNING
                # rows we didn't fetch yet.
                eq_(e1.some_server_value, server_new_value)
            else:
                # using return defaults here is not expiring.   have not
                # researched why, it may be because the explicit
                # return_defaults interferes with the ORMs call
                assert "some_server_value" in e1.__dict__
                eq_(e1.some_server_value, server_old_value)

        elif use_onupdate.callable:
            if not use_returning.defaults or not synchronize.fetch:
                # for python-side onupdate, col is populated with local value
                assert "some_server_value" in e1.__dict__

                # and is refreshed
                eq_(e1.some_server_value, server_new_value)
            else:
                assert "some_server_value" in e1.__dict__

                # and is not refreshed
                eq_(e1.some_server_value, server_old_value)

        else:
            # no onupdate, then the value was not touched yet,
            # even if we used RETURNING with populate_existing, because
            # we did not fetch the rows yet
            assert "some_server_value" in e1.__dict__
            eq_(e1.some_server_value, server_old_value)

        # now see if we can fetch rows
        if use_returning.returning:

            if populate_existing or not use_onupdate.none:
                eq_(
                    set(rows),
                    {
                        Employee(
                            uuid=uuid1,
                            user_name="e1 new name",
                            some_server_value=server_new_value,
                        ),
                    },
                )

            else:
                # if no populate existing and no server default, that column
                # is not touched at all
                eq_(
                    set(rows),
                    {
                        Employee(
                            uuid=uuid1,
                            user_name="e1 new name",
                            some_server_value=server_old_value,
                        ),
                    },
                )

        if use_returning.defaults:
            # as mentioned above, the return_defaults() case here remains
            # unanalyzed.
            if synchronize.fetch or (
                use_onupdate.clientsql
                or use_onupdate.server
                or use_onupdate.computed
                or use_onupdate.none
            ):
                eq_(e1.some_server_value, server_old_value)
            else:
                eq_(e1.some_server_value, server_new_value)

        elif (
            populate_existing and use_returning.returning
        ) or not use_onupdate.none:
            eq_(e1.some_server_value, server_new_value)
        else:
            # no onupdate specified, and no populate existing with returning,
            # the attribute is not refreshed
            eq_(e1.some_server_value, server_old_value)

        # do a full expire, now the new value is definitely there
        s.commit()
        s.expire_all()
        eq_(e1.some_server_value, server_new_value)


class PGIssue11849Test(fixtures.DeclarativeMappedTest):
    __backend__ = True
    __only_on__ = ("postgresql",)

    @classmethod
    def setup_classes(cls):

        from sqlalchemy.dialects.postgresql import JSONB

        Base = cls.DeclarativeBasic

        class TestTbl(Base):
            __tablename__ = "testtbl"

            test_id = Column(Integer, primary_key=True)
            test_field = Column(JSONB)

    def test_issue_11849(self):
        TestTbl = self.classes.TestTbl

        session = fixture_session()

        obj = TestTbl(
            test_id=1, test_field={"test1": 1, "test2": "2", "test3": [3, "3"]}
        )
        session.add(obj)

        query = (
            update(TestTbl)
            .where(TestTbl.test_id == 1)
            .values(test_field=TestTbl.test_field + {"test3": {"test4": 4}})
        )
        session.execute(query)

        # not loaded
        assert "test_field" not in obj.__dict__

        # synchronizes on load
        eq_(obj.test_field, {"test1": 1, "test2": "2", "test3": {"test4": 4}})
