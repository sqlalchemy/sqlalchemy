"""Test the TString construct in ORM context for Python 3.14+
template strings."""

from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import tstring
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.fixtures import fixture_session
from test.orm import _fixtures


class QueryTest(_fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()


class TStringTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_select_star(self):
        User = self.classes.User

        eq_(
            fixture_session()
            .query(User)
            .from_statement(
                tstring(t"select * from users where users.id={7} order by id")
            )
            .first(),
            User(id=7),
        )
        eq_(
            fixture_session()
            .query(User)
            .from_statement(
                tstring(t"select * from users where name={'nonexistent'}")
            )
            .first(),
            None,
        )

    def test_select_star_future(self):
        User = self.classes.User

        sess = fixture_session()
        eq_(
            sess.execute(
                select(User).from_statement(
                    tstring(
                        t"select * from users where users.id={7} order by id"
                    )
                )
            )
            .scalars()
            .first(),
            User(id=7),
        )
        eq_(
            sess.execute(
                select(User).from_statement(
                    tstring(t"select * from users where name={'nonexistent'}")
                )
            )
            .scalars()
            .first(),
            None,
        )

    def test_entity_interpolation(self):
        User = self.classes.User

        # Test interpolating entity columns and table in select and from clause
        sess = fixture_session()
        result = (
            sess.execute(
                select(User).from_statement(
                    tstring(t"select * from {User} order by {User.id}")
                )
            )
            .scalars()
            .all()
        )

        eq_([u.name for u in result], ["jack", "ed", "fred", "chuck"])

    def test_whereclause(self):
        User = self.classes.User

        eq_(
            fixture_session()
            .query(User)
            .filter(tstring(t"id in (8, 9)"))
            .all(),
            [User(id=8), User(id=9)],
        )

        eq_(
            fixture_session()
            .query(User)
            .filter(tstring(t"name='fred'"))
            .filter(tstring(t"id=9"))
            .all(),
            [User(id=9)],
        )
        eq_(
            fixture_session()
            .query(User)
            .filter(tstring(t"name='fred'"))
            .filter(User.id == 9)
            .all(),
            [User(id=9)],
        )

    def test_whereclause_future(self):
        User = self.classes.User

        s = fixture_session()
        eq_(
            s.execute(select(User).filter(tstring(t"id in (8, 9)")))
            .scalars()
            .all(),
            [User(id=8), User(id=9)],
        )

        eq_(
            s.execute(
                select(User)
                .filter(tstring(t"name='fred'"))
                .filter(tstring(t"id=9"))
            )
            .scalars()
            .all(),
            [User(id=9)],
        )
        eq_(
            s.execute(
                select(User)
                .filter(tstring(t"name='fred'"))
                .filter(User.id == 9)
            )
            .scalars()
            .all(),
            [User(id=9)],
        )

    def test_via_textasfrom_from_statement(self):
        User = self.classes.User
        s = fixture_session()

        eq_(
            s.query(User)
            .from_statement(
                tstring(t"select * from users order by id").columns(
                    id=Integer, name=String
                )
            )
            .all(),
            [User(id=7), User(id=8), User(id=9), User(id=10)],
        )

    def test_columns_via_textasfrom_from_statement(self):
        User = self.classes.User
        s = fixture_session()

        eq_(
            s.query(User.id, User.name)
            .from_statement(
                tstring(t"select * from users order by id").columns(
                    id=Integer, name=String
                )
            )
            .all(),
            [(7, "jack"), (8, "ed"), (9, "fred"), (10, "chuck")],
        )

    def test_via_textasfrom_use_mapped_columns(self):
        User = self.classes.User
        s = fixture_session()

        eq_(
            s.query(User)
            .from_statement(
                tstring(t"select * from users order by id").columns(
                    User.id, User.name
                )
            )
            .all(),
            [User(id=7), User(id=8), User(id=9), User(id=10)],
        )

    def test_group_by_accepts_tstring(self):
        User = self.classes.User
        s = fixture_session()

        q = s.query(User).group_by(tstring(t"name"))
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users GROUP BY name",
        )
