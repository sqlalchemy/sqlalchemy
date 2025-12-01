import sqlalchemy as sa
from sqlalchemy import column
from sqlalchemy import desc
from sqlalchemy import exc as sa_exc
from sqlalchemy import Integer
from sqlalchemy import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy import literal_column
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import util
from sqlalchemy.orm import aliased
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import subqueryload
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing.assertions import assert_raises_message
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


class TextTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_needs_text(self):
        User = self.classes.User

        assert_raises_message(
            sa_exc.ArgumentError,
            "Textual SQL expression",
            fixture_session().query(User).from_statement,
            "select * from users order by id",
        )

    def test_select_star(self):
        User = self.classes.User

        eq_(
            fixture_session()
            .query(User)
            .from_statement(text("select * from users order by id"))
            .first(),
            User(id=7),
        )
        eq_(
            fixture_session()
            .query(User)
            .from_statement(
                text("select * from users where name='nonexistent'")
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
                    text("select * from users order by id")
                )
            )
            .scalars()
            .first(),
            User(id=7),
        )
        eq_(
            sess.execute(
                select(User).from_statement(
                    text("select * from users where name='nonexistent'")
                )
            )
            .scalars()
            .first(),
            None,
        )

    def test_columns_mismatched(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter
        User = self.classes.User

        s = fixture_session()
        q = s.query(User).from_statement(
            text(
                "select name, 27 as foo, id as users_id from users order by id"
            )
        )
        eq_(
            q.all(),
            [
                User(id=7, name="jack"),
                User(id=8, name="ed"),
                User(id=9, name="fred"),
                User(id=10, name="chuck"),
            ],
        )

    def test_columns_mismatched_future(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter
        User = self.classes.User

        s = fixture_session()
        q = select(User).from_statement(
            text(
                "select name, 27 as foo, id as users_id from users order by id"
            )
        )
        eq_(
            s.execute(q).scalars().all(),
            [
                User(id=7, name="jack"),
                User(id=8, name="ed"),
                User(id=9, name="fred"),
                User(id=10, name="chuck"),
            ],
        )

    def test_columns_multi_table_uselabels(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = s.query(User, Address).from_statement(
            text(
                "select users.name AS users_name, users.id AS users_id, "
                "addresses.id AS addresses_id FROM users JOIN addresses "
                "ON users.id = addresses.user_id WHERE users.id=8 "
                "ORDER BY addresses.id"
            )
        )

        eq_(
            q.all(),
            [
                (User(id=8), Address(id=2)),
                (User(id=8), Address(id=3)),
                (User(id=8), Address(id=4)),
            ],
        )

    def test_columns_multi_table_uselabels_future(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = select(User, Address).from_statement(
            text(
                "select users.name AS users_name, users.id AS users_id, "
                "addresses.id AS addresses_id FROM users JOIN addresses "
                "ON users.id = addresses.user_id WHERE users.id=8 "
                "ORDER BY addresses.id"
            )
        )

        eq_(
            s.execute(q).all(),
            [
                (User(id=8), Address(id=2)),
                (User(id=8), Address(id=3)),
                (User(id=8), Address(id=4)),
            ],
        )

    def test_columns_multi_table_uselabels_contains_eager(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = (
            s.query(User)
            .from_statement(
                text(
                    "select users.name AS users_name, users.id AS users_id, "
                    "addresses.id AS addresses_id FROM users JOIN addresses "
                    "ON users.id = addresses.user_id WHERE users.id=8 "
                    "ORDER BY addresses.id"
                )
            )
            .options(contains_eager(User.addresses))
        )

        def go():
            r = q.all()
            eq_(r[0].addresses, [Address(id=2), Address(id=3), Address(id=4)])

        self.assert_sql_count(testing.db, go, 1)

    def test_columns_multi_table_uselabels_contains_eager_future(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = (
            select(User)
            .from_statement(
                text(
                    "select users.name AS users_name, users.id AS users_id, "
                    "addresses.id AS addresses_id FROM users JOIN addresses "
                    "ON users.id = addresses.user_id WHERE users.id=8 "
                    "ORDER BY addresses.id"
                )
            )
            .options(contains_eager(User.addresses))
        )

        def go():
            r = s.execute(q).unique().scalars().all()
            eq_(r[0].addresses, [Address(id=2), Address(id=3), Address(id=4)])

        self.assert_sql_count(testing.db, go, 1)

    def test_columns_multi_table_uselabels_cols_contains_eager(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = (
            s.query(User)
            .from_statement(
                text(
                    "select users.name AS users_name, users.id AS users_id, "
                    "addresses.id AS addresses_id FROM users JOIN addresses "
                    "ON users.id = addresses.user_id WHERE users.id=8 "
                    "ORDER BY addresses.id"
                ).columns(User.name, User.id, Address.id)
            )
            .options(contains_eager(User.addresses))
        )

        def go():
            r = q.all()
            eq_(r[0].addresses, [Address(id=2), Address(id=3), Address(id=4)])

        self.assert_sql_count(testing.db, go, 1)

    def test_columns_multi_table_uselabels_cols_contains_eager_future(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = (
            select(User)
            .from_statement(
                text(
                    "select users.name AS users_name, users.id AS users_id, "
                    "addresses.id AS addresses_id FROM users JOIN addresses "
                    "ON users.id = addresses.user_id WHERE users.id=8 "
                    "ORDER BY addresses.id"
                ).columns(User.name, User.id, Address.id)
            )
            .options(contains_eager(User.addresses))
        )

        def go():
            r = s.execute(q).unique().scalars().all()
            eq_(r[0].addresses, [Address(id=2), Address(id=3), Address(id=4)])

        self.assert_sql_count(testing.db, go, 1)

    def test_textual_select_orm_columns(self):
        # test that columns using column._label match, as well as that
        # ordering doesn't matter.
        User = self.classes.User
        Address = self.classes.Address
        users = self.tables.users
        addresses = self.tables.addresses

        s = fixture_session()
        q = s.query(User.name, User.id, Address.id).from_statement(
            text(
                "select users.name AS users_name, users.id AS users_id, "
                "addresses.id AS addresses_id FROM users JOIN addresses "
                "ON users.id = addresses.user_id WHERE users.id=8 "
                "ORDER BY addresses.id"
            ).columns(users.c.name, users.c.id, addresses.c.id)
        )

        eq_(q.all(), [("ed", 8, 2), ("ed", 8, 3), ("ed", 8, 4)])

    @testing.combinations(
        (
            False,
            subqueryload,
        ),
        (
            True,
            subqueryload,
        ),
        (False, selectinload),
        (True, selectinload),
    )
    def test_related_eagerload_against_text(self, add_columns, loader_option):
        # new in 1.4.   textual selects have columns so subqueryloaders
        # and selectinloaders can join onto them.   we add columns
        # automatiacally to TextClause as well, however subqueryloader
        # is not working at the moment due to execution model refactor,
        # it creates a subquery w/ adapter before those columns are
        # available.  this is a super edge case and as we want to rewrite
        # the loaders to use select(), maybe we can get it then.
        User = self.classes.User

        text_clause = text("select * from users")
        if add_columns:
            text_clause = text_clause.columns(User.id, User.name)

        s = fixture_session()
        q = (
            s.query(User)
            .from_statement(text_clause)
            .options(loader_option(User.addresses))
        )

        def go():
            eq_(set(q.all()), set(self.static.user_address_result))

        if loader_option is subqueryload:
            # subqueryload necessarily degrades to lazy loads for a text
            # statement.
            self.assert_sql_count(testing.db, go, 5)
        else:
            self.assert_sql_count(testing.db, go, 2)

    def test_whereclause(self):
        User = self.classes.User

        eq_(
            fixture_session().query(User).filter(text("id in (8, 9)")).all(),
            [User(id=8), User(id=9)],
        )

        eq_(
            fixture_session()
            .query(User)
            .filter(text("name='fred'"))
            .filter(text("id=9"))
            .all(),
            [User(id=9)],
        )
        eq_(
            fixture_session()
            .query(User)
            .filter(text("name='fred'"))
            .filter(User.id == 9)
            .all(),
            [User(id=9)],
        )

    def test_whereclause_future(self):
        User = self.classes.User

        s = fixture_session()
        eq_(
            s.execute(select(User).filter(text("id in (8, 9)")))
            .scalars()
            .all(),
            [User(id=8), User(id=9)],
        )

        eq_(
            s.execute(
                select(User).filter(text("name='fred'")).filter(text("id=9"))
            )
            .scalars()
            .all(),
            [User(id=9)],
        )
        eq_(
            s.execute(
                select(User).filter(text("name='fred'")).filter(User.id == 9)
            )
            .scalars()
            .all(),
            [User(id=9)],
        )

    def test_binds_coerce(self):
        User = self.classes.User

        assert_raises_message(
            sa_exc.ArgumentError,
            r"Textual SQL expression 'id in \(:id1, :id2\)' "
            "should be explicitly declared",
            fixture_session().query(User).filter,
            "id in (:id1, :id2)",
        )

    def test_plain_textual_column(self):
        User = self.classes.User

        s = fixture_session()

        self.assert_compile(
            s.query(User.id, text("users.name")),
            "SELECT users.id AS users_id, users.name FROM users",
        )

        eq_(
            s.query(User.id, text("users.name")).all(),
            [(7, "jack"), (8, "ed"), (9, "fred"), (10, "chuck")],
        )

        eq_(
            s.query(User.id, literal_column("name")).order_by(User.id).all(),
            [(7, "jack"), (8, "ed"), (9, "fred"), (10, "chuck")],
        )

    def test_via_select(self):
        User = self.classes.User
        s = fixture_session()
        eq_(
            s.query(User)
            .from_statement(
                select(column("id"), column("name"))
                .select_from(table("users"))
                .order_by("id")
            )
            .all(),
            [User(id=7), User(id=8), User(id=9), User(id=10)],
        )

    def test_via_textasfrom_from_statement(self):
        User = self.classes.User
        s = fixture_session()

        eq_(
            s.query(User)
            .from_statement(
                text("select * from users order by id").columns(
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
                text("select * from users order by id").columns(
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
                text("select * from users order by id").columns(
                    User.id, User.name
                )
            )
            .all(),
            [User(id=7), User(id=8), User(id=9), User(id=10)],
        )

    def test_via_textasfrom_aliased(self):
        User = self.classes.User
        s = fixture_session()

        ua = aliased(
            User,
            text("select * from users").columns(User.id, User.name).subquery(),
        )

        eq_(
            s.query(ua).order_by(ua.id).all(),
            [User(id=7), User(id=8), User(id=9), User(id=10)],
        )

    def test_group_by_accepts_text(self):
        User = self.classes.User
        s = fixture_session()

        q = s.query(User).group_by(text("name"))
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users GROUP BY name",
        )

    def test_order_by_w_eager_one(self):
        User = self.classes.User
        s = fixture_session()

        # from 1.0.0 thru 1.0.2, the "name" symbol here was considered
        # to be part of the things we need to ORDER BY and it was being
        # placed into the inner query's columns clause, as part of
        # query._compound_eager_statement where we add unwrap_order_by()
        # to the columns clause.  However, as #3392 illustrates, unlocatable
        # string expressions like "name desc" will only fail in this scenario,
        # so in general the changing of the query structure with string labels
        # is dangerous.
        #
        # the queries here are again "invalid" from a SQL perspective, as the
        # "name" field isn't matched up to anything.
        #

        q = (
            s.query(User)
            .options(joinedload(User.addresses))
            .order_by(desc("name"))
            .limit(1)
        )
        assert_raises_message(
            sa_exc.CompileError,
            "Can't resolve label reference for ORDER BY / GROUP BY.",
            q.set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            ).statement.compile,
        )

    def test_order_by_w_eager_two(self):
        User = self.classes.User
        s = fixture_session()

        q = (
            s.query(User)
            .options(joinedload(User.addresses))
            .order_by("name")
            .limit(1)
        )
        assert_raises_message(
            sa_exc.CompileError,
            "Can't resolve label reference for ORDER BY / GROUP BY.",
            q.set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            ).statement.compile,
        )

    def test_order_by_w_eager_three(self):
        User = self.classes.User
        s = fixture_session()

        self.assert_compile(
            s.query(User)
            .options(joinedload(User.addresses))
            .order_by("users_name")
            .limit(1),
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM (SELECT users.id AS users_id, users.name AS users_name "
            "FROM users ORDER BY users.name "
            "LIMIT :param_1) AS anon_1 "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id "
            "ORDER BY anon_1.users_name, addresses_1.id",
        )

        # however! this works (again?)
        eq_(
            s.query(User)
            .options(joinedload(User.addresses))
            .order_by("users_name")
            .first(),
            User(name="chuck", addresses=[]),
        )

    def test_order_by_w_eager_four(self):
        User = self.classes.User
        Address = self.classes.Address
        s = fixture_session()

        self.assert_compile(
            s.query(User)
            .options(joinedload(User.addresses))
            .order_by(desc("users_name"))
            .limit(1),
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM (SELECT users.id AS users_id, users.name AS users_name "
            "FROM users ORDER BY users.name DESC "
            "LIMIT :param_1) AS anon_1 "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id "
            "ORDER BY anon_1.users_name DESC, addresses_1.id",
        )

        # however! this works (again?)
        eq_(
            s.query(User)
            .options(joinedload(User.addresses))
            .order_by(desc("users_name"))
            .first(),
            User(name="jack", addresses=[Address()]),
        )

    def test_order_by_w_eager_five(self):
        """essentially the same as test_eager_relations -> test_limit_3,
        but test for textual label elements that are freeform.
        this is again #3392."""

        User = self.classes.User
        Address = self.classes.Address

        sess = fixture_session()

        q = sess.query(User, Address.email_address.label("email_address"))

        result = (
            q.join(User.addresses)
            .options(joinedload(User.orders))
            .order_by("email_address desc")
            .limit(1)
            .offset(0)
        )

        assert_raises_message(
            sa_exc.CompileError,
            "Can't resolve label reference for ORDER BY / GROUP BY",
            result.all,
        )


class TextErrorTest(QueryTest, AssertsCompiledSQL):
    def _test(self, fn, arg, offending_clause):
        assert_raises_message(
            sa.exc.ArgumentError,
            r"Textual (?:SQL|column|SQL FROM) expression %(stmt)r should be "
            r"explicitly declared (?:with|as) text\(%(stmt)r\)"
            % {"stmt": util.ellipses_string(offending_clause)},
            fn,
            arg,
        )

    def test_filter(self):
        User = self.classes.User
        self._test(
            fixture_session().query(User.id).filter, "myid == 5", "myid == 5"
        )

    def test_having(self):
        User = self.classes.User
        self._test(
            fixture_session().query(User.id).having, "myid == 5", "myid == 5"
        )

    def test_from_statement(self):
        User = self.classes.User
        self._test(
            fixture_session().query(User.id).from_statement,
            "select id from user",
            "select id from user",
        )
