from sqlalchemy import Boolean
from sqlalchemy import case
from sqlalchemy import column
from sqlalchemy import delete
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import lambda_stmt
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.orm import backref
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import synonym
from sqlalchemy.orm import with_loader_criteria
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import not_in
from sqlalchemy.testing.assertions import expect_raises_message
from sqlalchemy.testing.assertsql import CompiledSQL
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

        Address = cls.classes.Address
        addresses = cls.tables.addresses

        mapper(
            User,
            users,
            properties={
                "age": users.c.age_int,
                "addresses": relationship(Address),
            },
        )
        mapper(Address, addresses)

    def test_illegal_eval(self):
        User = self.classes.User
        s = fixture_session()
        assert_raises_message(
            exc.ArgumentError,
            "Valid strategies for session synchronization "
            "are 'evaluate', 'fetch', False",
            s.query(User).update,
            {},
            synchronize_session="fake",
        )

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

        s = Session(testing.db, future=True)

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

        s = Session(testing.db, future=True)

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

        class Thing(object):
            def __clause_element__(self):
                return User.name.__clause_element__()

        s = fixture_session()
        jill = s.query(User).get(3)
        s.query(User).update(
            {Thing(): "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.name, "moonbeam")

    def test_evaluate_invalid(self):
        User = self.classes.User

        class Thing(object):
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
        jill = s.query(User).get(3)
        s.query(User).update(
            {column("name"): "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.name, "jill")
        s.expire(jill)
        eq_(jill.name, "moonbeam")

    def test_evaluate_synonym_string(self):
        class Foo(object):
            pass

        mapper(Foo, self.tables.users, properties={"uname": synonym("name")})

        s = fixture_session()
        jill = s.query(Foo).get(3)
        s.query(Foo).update(
            {"uname": "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.uname, "moonbeam")

    def test_evaluate_synonym_attr(self):
        class Foo(object):
            pass

        mapper(Foo, self.tables.users, properties={"uname": synonym("name")})

        s = fixture_session()
        jill = s.query(Foo).get(3)
        s.query(Foo).update(
            {Foo.uname: "moonbeam"}, synchronize_session="evaluate"
        )
        eq_(jill.uname, "moonbeam")

    def test_evaluate_double_synonym_attr(self):
        class Foo(object):
            pass

        mapper(
            Foo,
            self.tables.users,
            properties={"uname": synonym("name"), "ufoo": synonym("uname")},
        )

        s = fixture_session()
        jill = s.query(Foo).get(3)
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
                    # it has to unexpire jane.name, because jane is not fully
                    # expired and the critiera needs to look at this particular
                    # key
                    CompiledSQL(
                        "SELECT users.age_int AS users_age_int, "
                        "users.name AS users_name FROM users "
                        "WHERE users.id = :pk_1",
                        [{"pk_1": 4}],
                    ),
                    CompiledSQL(
                        "UPDATE users "
                        "SET age_int=(users.age_int + :age_int_1) "
                        "WHERE users.name IS NOT NULL",
                        [{"age_int_1": 10}],
                    ),
                )
            else:
                asserter.assert_(
                    # it has to unexpire jane.name, because jane is not fully
                    # expired and the critiera needs to look at this particular
                    # key
                    CompiledSQL(
                        "SELECT users.name AS users_name FROM users "
                        "WHERE users.id = :pk_1",
                        [{"pk_1": 4}],
                    ),
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
                "SELECT users.age_int AS users_age_int, "
                "users.id AS users_id, users.name AS users_name FROM users "
                "WHERE users.id = :pk_1",
                [{"pk_1": 1}],
            ),
            # refresh jill
            CompiledSQL(
                "SELECT users.age_int AS users_age_int, "
                "users.id AS users_id, users.name AS users_name FROM users "
                "WHERE users.id = :pk_1",
                [{"pk_1": 3}],
            ),
        ]

        if expire_jane_age and not add_filter_criteria:
            to_assert.append(
                # refresh jane
                CompiledSQL(
                    "SELECT users.age_int AS users_age_int, "
                    "users.name AS users_name FROM users "
                    "WHERE users.id = :pk_1",
                    [{"pk_1": 4}],
                )
            )
        asserter.assert_(*to_assert)

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

        if testing.db.dialect.full_returning:
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
                "SELECT users.age_int AS users_age_int, "
                "users.id AS users_id, users.name AS users_name FROM users "
                "WHERE users.id = :pk_1",
                [{"pk_1": 1}],
            ),
            # refresh jill
            CompiledSQL(
                "SELECT users.age_int AS users_age_int, "
                "users.id AS users_id, users.name AS users_name FROM users "
                "WHERE users.id = :pk_1",
                [{"pk_1": 3}],
            ),
        )

    def test_delete(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(
            or_(User.name == "john", User.name == "jill")
        ).delete()

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

    def test_update_future(self):
        User, users = self.classes.User, self.tables.users

        sess = Session(testing.db, future=True)

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

    def test_update_future_lambda(self):
        User, users = self.classes.User, self.tables.users

        sess = Session(testing.db, future=True)

        john, jack, jill, jane = (
            sess.execute(select(User).order_by(User.id)).scalars().all()
        )

        sess.execute(
            lambda_stmt(
                lambda: update(User)
                .where(User.age > 29)
                .values({"age": User.age - 10})
                .execution_options(synchronize_session="evaluate")
            ),
        )

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])
        eq_(
            sess.execute(select(User.age).order_by(User.id)).all(),
            list(zip([25, 37, 29, 27])),
        )

        sess.execute(
            lambda_stmt(
                lambda: update(User)
                .where(User.age > 29)
                .values({User.age: User.age - 10})
                .execution_options(synchronize_session="evaluate")
            )
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

    @testing.combinations(
        ("fetch", False),
        ("fetch", True),
        ("evaluate", False),
        ("evaluate", True),
    )
    def test_update_with_loader_criteria(self, fetchstyle, future):
        User = self.classes.User

        sess = Session(testing.db, future=True)

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
        ("fetch", False),
        ("fetch", True),
        ("evaluate", False),
        ("evaluate", True),
    )
    def test_delete_with_loader_criteria(self, fetchstyle, future):
        User = self.classes.User

        sess = Session(testing.db, future=True)

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

    def test_update_fetch_returning(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        with self.sql_execution_asserter() as asserter:
            sess.query(User).filter(User.age > 29).update(
                {"age": User.age - 10}, synchronize_session="fetch"
            )

            # these are simple values, these are now evaluated even with
            # the "fetch" strategy, new in 1.4, so there is no expiry
            eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])

        if testing.db.dialect.full_returning:
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

        sess = Session(testing.db, future=True)

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

        if testing.db.dialect.full_returning:
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

    @testing.requires.full_returning
    def test_update_explicit_returning(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        with self.sql_execution_asserter() as asserter:
            stmt = (
                update(User)
                .filter(User.age > 29)
                .values({"age": User.age - 10})
                .returning(User.id)
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

    @testing.requires.full_returning
    def test_no_fetch_w_explicit_returning(self):
        User = self.classes.User

        sess = fixture_session()

        stmt = (
            update(User)
            .filter(User.age > 29)
            .values({"age": User.age - 10})
            .execution_options(synchronize_session="fetch")
            .returning(User.id)
        )
        with expect_raises_message(
            exc.InvalidRequestError,
            r"Can't use synchronize_session='fetch' "
            r"with explicit returning\(\)",
        ):
            sess.execute(stmt)

    def test_delete_fetch_returning(self):
        User = self.classes.User

        sess = fixture_session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        in_(john, sess)
        in_(jack, sess)

        with self.sql_execution_asserter() as asserter:
            sess.query(User).filter(User.age > 29).delete(
                synchronize_session="fetch"
            )

        if testing.db.dialect.full_returning:
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

        sess = Session(testing.db, future=True)

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

        if testing.db.dialect.full_returning:
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
        """test for [ticket:4556] """

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

    def test_update_changes_resets_dirty(self):
        User = self.classes.User

        sess = fixture_session(autoflush=False)

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        john.age = 50
        jack.age = 37

        # autoflush is false.  therefore our '50' and '37' are getting
        # blown away by this operation.

        sess.query(User).filter(User.age > 29).update(
            {"age": User.age - 10}, synchronize_session="evaluate"
        )

        for x in (john, jack, jill, jane):
            assert not sess.is_modified(x)

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])

        john.age = 25
        assert john in sess.dirty
        assert jack in sess.dirty
        assert jill not in sess.dirty
        assert not sess.is_modified(john)
        assert not sess.is_modified(jack)

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
    def test_update_returns_rowcount(self):
        User = self.classes.User

        sess = fixture_session()

        rowcount = (
            sess.query(User)
            .filter(User.age > 29)
            .update({"age": User.age + 0})
        )
        eq_(rowcount, 2)

        rowcount = (
            sess.query(User)
            .filter(User.age > 29)
            .update({"age": User.age - 10})
        )
        eq_(rowcount, 2)

        # test future
        result = sess.execute(
            update(User).where(User.age > 19).values({"age": User.age - 10})
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

    def test_evaluate_before_update(self):
        User = self.classes.User

        sess = fixture_session()
        john = sess.query(User).filter_by(name="john").one()
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

    def test_evaluate_before_delete(self):
        User = self.classes.User

        sess = fixture_session()
        john = sess.query(User).filter_by(name="john").one()
        sess.expire(john, ["age"])

        sess.query(User).filter_by(name="john").filter_by(age=25).delete(
            synchronize_session="evaluate"
        )
        assert john not in sess

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
                for c, v in (
                    (
                        bulk_ud.result.context
                    ).compiled.compile_state.statement._ordered_values
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

    def test_update_multi_values_error_future(self):
        User = self.classes.User
        session = Session(testing.db, future=True)

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

    def test_update_preserve_parameter_order_future(self):
        User = self.classes.User
        session = Session(testing.db, future=True)

        # Do update using a tuple and check that order is preserved

        stmt = (
            update(User)
            .filter(User.id == 15)
            .ordered_values(("age", 123), ("name", "foob"))
        )
        result = session.execute(stmt)
        cols = [
            c.key
            for c, v in (
                (
                    result.context
                ).compiled.compile_state.statement._ordered_values
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
            for c, v in (
                result.context
            ).compiled.compile_state.statement._ordered_values
        ]
        eq_(["name", "age_int"], cols)


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

        mapper(User, users)
        mapper(
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

        mapper(User, users)
        mapper(
            Document,
            documents,
            properties={"user": relationship(User, backref="documents")},
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
            set(
                [
                    (1, True),
                    (2, None),
                    (3, None),
                    (4, True),
                    (5, True),
                    (6, None),
                ]
            ),
        )

    @testing.requires.delete_from
    def test_delete_from_joined_subq_test(self):
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
            set([(2, None), (3, None), (6, None)]),
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
            {"name": "ed"},
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
            set(
                [
                    (1, True),
                    (2, None),
                    (3, None),
                    (4, True),
                    (5, True),
                    (6, None),
                ]
            ),
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
        case_stmt = case([(Document.title.in_(subq), True)], else_=False)

        s.query(Document).update(
            {"flag": case_stmt}, synchronize_session=False
        )

        eq_(
            set(s.query(Document.id, Document.flag)),
            set(
                [
                    (1, True),
                    (2, False),
                    (3, False),
                    (4, True),
                    (5, True),
                    (6, False),
                ]
            ),
        )

    @testing.only_on("mysql", "Multi table update")
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


class ExpressionUpdateTest(fixtures.MappedTest):
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
        mapper(cls.classes.Data, data, properties={"cnt": data.c.counter})

    @testing.provide_metadata
    def test_update_attr_names(self):
        Data = self.classes.Data

        d1 = Data()
        sess = fixture_session()
        sess.add(d1)
        sess.commit()
        eq_(d1.cnt, 0)

        sess.query(Data).update({Data.cnt: Data.cnt + 1})
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

        class Manager(Person):
            __tablename__ = "manager"
            id = Column(Integer, ForeignKey("person.id"), primary_key=True)
            manager_name = Column(String(50))

    @classmethod
    def insert_data(cls, connection):
        Engineer, Person, Manager = (
            cls.classes.Engineer,
            cls.classes.Person,
            cls.classes.Manager,
        )
        s = Session(connection)
        s.add_all(
            [
                Engineer(name="e1", engineer_name="e1"),
                Manager(name="m1", manager_name="m1"),
                Engineer(name="e2", engineer_name="e2"),
                Person(name="p1"),
            ]
        )
        s.commit()

    @testing.only_on("mysql", "Multi table update")
    def test_update_from_join_no_problem(self):
        person = self.classes.Person.__table__
        engineer = self.classes.Engineer.__table__

        sess = Session(testing.db, future=True)
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

    def test_update_subtable_only(self):
        Engineer = self.classes.Engineer
        s = Session(testing.db)
        s.query(Engineer).update({"engineer_name": "e5"})

        eq_(s.query(Engineer.engineer_name).all(), [("e5",), ("e5",)])

    @testing.requires.update_from
    def test_update_from(self):
        Engineer = self.classes.Engineer
        Person = self.classes.Person
        s = Session(testing.db)
        s.query(Engineer).filter(Engineer.id == Person.id).filter(
            Person.name == "e2"
        ).update({"engineer_name": "e5"})

        eq_(
            set(s.query(Person.name, Engineer.engineer_name)),
            set([("e1", "e1"), ("e2", "e5")]),
        )

    @testing.requires.delete_from
    def test_delete_from(self):
        Engineer = self.classes.Engineer
        Person = self.classes.Person
        s = Session(testing.db)
        s.query(Engineer).filter(Engineer.id == Person.id).filter(
            Person.name == "e2"
        ).delete()

        eq_(
            set(s.query(Person.name, Engineer.engineer_name)),
            set([("e1", "e1")]),
        )

    @testing.only_on("mysql", "Multi table update")
    def test_update_from_multitable(self):
        Engineer = self.classes.Engineer
        Person = self.classes.Person
        s = Session(testing.db)
        s.query(Engineer).filter(Engineer.id == Person.id).filter(
            Person.name == "e2"
        ).update({Person.name: "e22", Engineer.engineer_name: "e55"})

        eq_(
            set(s.query(Person.name, Engineer.engineer_name)),
            set([("e1", "e1"), ("e22", "e55")]),
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
    def test_update(self, fetchstyle, future):
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

        if future:
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
    def test_delete(self, fetchstyle, future):
        Staff, Sales, Support = self.classes("Staff", "Sales", "Support")

        sess = fixture_session()
        en1, en2 = sess.query(Sales).order_by(Sales.sales_stats).all()
        mn1, mn2 = sess.query(Support).order_by(Support.support_stats).all()

        if future:
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
    __requires__ = ("full_returning",)

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

        mapper(
            User,
            users,
            properties={
                "age": users.c.age_int,
            },
        )

    def test_load_from_update(self, connection):
        User = self.classes.User

        stmt = (
            update(User)
            .where(User.name.in_(["jack", "jill"]))
            .values(age=User.age + 5)
            .returning(User)
        )

        stmt = select(User).from_statement(stmt)

        with Session(connection) as sess:
            rows = sess.execute(stmt).scalars().all()

            eq_(
                rows,
                [User(name="jack", age=52), User(name="jill", age=34)],
            )

    def test_load_from_insert(self, connection):
        User = self.classes.User

        stmt = (
            insert(User)
            .values({User.id: 5, User.age: 25, User.name: "spongebob"})
            .returning(User)
        )

        stmt = select(User).from_statement(stmt)

        with Session(connection) as sess:
            rows = sess.execute(stmt).scalars().all()

            eq_(
                rows,
                [User(name="spongebob", age=25)],
            )
