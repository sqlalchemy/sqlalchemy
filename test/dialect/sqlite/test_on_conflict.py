"""SQLite-specific tests."""

from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import schema
from sqlalchemy import sql
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import types as sqltypes
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import fixtures
from sqlalchemy.types import Integer
from sqlalchemy.types import String


class OnConflictTest(fixtures.TablesTest):
    __only_on__ = ("sqlite >= 3.24.0",)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )

        Table(
            "users_w_key",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50), key="name_keyed"),
        )

        class SpecialType(sqltypes.TypeDecorator):
            impl = String
            cache_ok = True

            def process_bind_param(self, value, dialect):
                return value + " processed"

        Table(
            "bind_targets",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", SpecialType()),
        )

        users_xtra = Table(
            "users_xtra",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("login_email", String(50)),
            Column("lets_index_this", String(50)),
        )
        cls.unique_partial_index = schema.Index(
            "idx_unique_partial_name",
            users_xtra.c.name,
            users_xtra.c.lets_index_this,
            unique=True,
            sqlite_where=users_xtra.c.lets_index_this == "unique_name",
        )

        cls.unique_constraint = schema.UniqueConstraint(
            users_xtra.c.login_email, name="uq_login_email"
        )
        cls.bogus_index = schema.Index(
            "idx_special_ops",
            users_xtra.c.lets_index_this,
            sqlite_where=users_xtra.c.lets_index_this > "m",
        )

    def test_bad_args(self):
        with expect_raises(ValueError):
            insert(self.tables.users).on_conflict_do_update()

    def test_on_conflict_do_no_call_twice(self):
        users = self.tables.users

        for stmt in (
            insert(users).on_conflict_do_nothing(),
            insert(users).on_conflict_do_update(
                index_elements=[users.c.id], set_=dict(name="foo")
            ),
        ):
            for meth in (
                stmt.on_conflict_do_nothing,
                stmt.on_conflict_do_update,
            ):
                with testing.expect_raises_message(
                    exc.InvalidRequestError,
                    "This Insert construct already has an "
                    "ON CONFLICT clause established",
                ):
                    meth()

    def test_on_conflict_do_nothing(self, connection):
        users = self.tables.users

        conn = connection
        result = conn.execute(
            insert(users).on_conflict_do_nothing(),
            dict(id=1, name="name1"),
        )
        eq_(result.inserted_primary_key, (1,))

        result = conn.execute(
            insert(users).on_conflict_do_nothing(),
            dict(id=1, name="name2"),
        )
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name1")],
        )

    def test_on_conflict_do_nothing_connectionless(self, connection):
        users = self.tables.users_xtra

        result = connection.execute(
            insert(users).on_conflict_do_nothing(
                index_elements=["login_email"]
            ),
            dict(name="name1", login_email="email1"),
        )
        eq_(result.inserted_primary_key, (1,))

        result = connection.execute(
            insert(users).on_conflict_do_nothing(
                index_elements=["login_email"]
            ),
            dict(name="name2", login_email="email1"),
        )
        eq_(result.inserted_primary_key, (1,))

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
            [(1, "name1", "email1", None)],
        )

    @testing.provide_metadata
    def test_on_conflict_do_nothing_target(self, connection):
        users = self.tables.users

        conn = connection

        result = conn.execute(
            insert(users).on_conflict_do_nothing(
                index_elements=users.primary_key.columns
            ),
            dict(id=1, name="name1"),
        )
        eq_(result.inserted_primary_key, (1,))

        result = conn.execute(
            insert(users).on_conflict_do_nothing(
                index_elements=users.primary_key.columns
            ),
            dict(id=1, name="name2"),
        )
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name1")],
        )

    @testing.combinations(
        ("with_dict", True),
        ("issue_5939", False),
        id_="ia",
        argnames="with_dict",
    )
    def test_on_conflict_do_update_one(self, connection, with_dict):
        users = self.tables.users

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.id],
            set_=dict(name=i.excluded.name) if with_dict else i.excluded,
        )
        result = conn.execute(i, dict(id=1, name="name1"))

        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name1")],
        )

    def test_on_conflict_do_update_two(self, connection):
        users = self.tables.users

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.id],
            set_=dict(id=i.excluded.id, name=i.excluded.name),
        )

        result = conn.execute(i, dict(id=1, name="name2"))
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name2")],
        )

    def test_on_conflict_do_update_three(self, connection):
        users = self.tables.users

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(name=i.excluded.name),
        )
        result = conn.execute(i, dict(id=1, name="name3"))
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name3")],
        )

    def test_on_conflict_do_update_four(self, connection):
        users = self.tables.users

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(id=i.excluded.id, name=i.excluded.name),
        ).values(id=1, name="name4")

        result = conn.execute(i)
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name4")],
        )

    def test_on_conflict_do_update_five(self, connection):
        users = self.tables.users

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(id=10, name="I'm a name"),
        ).values(id=1, name="name4")

        result = conn.execute(i)
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 10)).fetchall(),
            [(10, "I'm a name")],
        )

    def test_on_conflict_do_update_column_keys(self, connection):
        users = self.tables.users

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_={users.c.id: 10, users.c.name: "I'm a name"},
        ).values(id=1, name="name4")

        result = conn.execute(i)
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 10)).fetchall(),
            [(10, "I'm a name")],
        )

    def test_on_conflict_do_update_clauseelem_keys(self, connection):
        users = self.tables.users

        class MyElem:
            def __init__(self, expr):
                self.expr = expr

            def __clause_element__(self):
                return self.expr

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_={MyElem(users.c.id): 10, MyElem(users.c.name): "I'm a name"},
        ).values({MyElem(users.c.id): 1, MyElem(users.c.name): "name4"})

        result = conn.execute(i)
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 10)).fetchall(),
            [(10, "I'm a name")],
        )

    def test_on_conflict_do_update_multivalues(self, connection):
        users = self.tables.users

        conn = connection

        conn.execute(users.insert(), dict(id=1, name="name1"))
        conn.execute(users.insert(), dict(id=2, name="name2"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(name="updated"),
            where=(i.excluded.name != "name12"),
        ).values(
            [
                dict(id=1, name="name11"),
                dict(id=2, name="name12"),
                dict(id=3, name="name13"),
                dict(id=4, name="name14"),
            ]
        )

        result = conn.execute(i)
        eq_(result.inserted_primary_key, (None,))

        eq_(
            conn.execute(users.select().order_by(users.c.id)).fetchall(),
            [(1, "updated"), (2, "name2"), (3, "name13"), (4, "name14")],
        )

    def _exotic_targets_fixture(self, conn):
        users = self.tables.users_xtra

        conn.execute(
            insert(users),
            dict(
                id=1,
                name="name1",
                login_email="name1@gmail.com",
                lets_index_this="not",
            ),
        )
        conn.execute(
            users.insert(),
            dict(
                id=2,
                name="name2",
                login_email="name2@gmail.com",
                lets_index_this="not",
            ),
        )

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name1", "name1@gmail.com", "not")],
        )

    def test_on_conflict_do_update_exotic_targets_two(self, connection):
        users = self.tables.users_xtra

        conn = connection
        self._exotic_targets_fixture(conn)
        # try primary key constraint: cause an upsert on unique id column
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(
                name=i.excluded.name, login_email=i.excluded.login_email
            ),
        )
        result = conn.execute(
            i,
            dict(
                id=1,
                name="name2",
                login_email="name1@gmail.com",
                lets_index_this="not",
            ),
        )
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name2", "name1@gmail.com", "not")],
        )

    def test_on_conflict_do_update_exotic_targets_three(self, connection):
        users = self.tables.users_xtra

        conn = connection
        self._exotic_targets_fixture(conn)
        # try unique constraint: cause an upsert on target
        # login_email, not id
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=["login_email"],
            set_=dict(
                id=i.excluded.id,
                name=i.excluded.name,
                login_email=i.excluded.login_email,
            ),
        )
        # note: lets_index_this value totally ignored in SET clause.
        result = conn.execute(
            i,
            dict(
                id=42,
                name="nameunique",
                login_email="name2@gmail.com",
                lets_index_this="unique",
            ),
        )
        eq_(result.inserted_primary_key, (42,))

        eq_(
            conn.execute(
                users.select().where(users.c.login_email == "name2@gmail.com")
            ).fetchall(),
            [(42, "nameunique", "name2@gmail.com", "not")],
        )

    def test_on_conflict_do_update_exotic_targets_four(self, connection):
        users = self.tables.users_xtra

        conn = connection
        self._exotic_targets_fixture(conn)
        # try unique constraint by name: cause an
        # upsert on target login_email, not id
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=["login_email"],
            set_=dict(
                id=i.excluded.id,
                name=i.excluded.name,
                login_email=i.excluded.login_email,
            ),
        )
        # note: lets_index_this value totally ignored in SET clause.

        result = conn.execute(
            i,
            dict(
                id=43,
                name="nameunique2",
                login_email="name2@gmail.com",
                lets_index_this="unique",
            ),
        )
        eq_(result.inserted_primary_key, (43,))

        eq_(
            conn.execute(
                users.select().where(users.c.login_email == "name2@gmail.com")
            ).fetchall(),
            [(43, "nameunique2", "name2@gmail.com", "not")],
        )

    def test_on_conflict_do_update_exotic_targets_four_no_pk(self, connection):
        users = self.tables.users_xtra

        conn = connection
        self._exotic_targets_fixture(conn)
        # try unique constraint by name: cause an
        # upsert on target login_email, not id
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.login_email],
            set_=dict(
                id=i.excluded.id,
                name=i.excluded.name,
                login_email=i.excluded.login_email,
            ),
        )

        conn.execute(i, dict(name="name3", login_email="name1@gmail.com"))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [],
        )

        eq_(
            conn.execute(users.select().order_by(users.c.id)).fetchall(),
            [
                (2, "name2", "name2@gmail.com", "not"),
                (3, "name3", "name1@gmail.com", "not"),
            ],
        )

    def test_on_conflict_do_update_exotic_targets_five(self, connection):
        users = self.tables.users_xtra

        conn = connection
        self._exotic_targets_fixture(conn)
        # try bogus index
        i = insert(users)

        i = i.on_conflict_do_update(
            index_elements=self.bogus_index.columns,
            index_where=self.bogus_index.dialect_options["sqlite"]["where"],
            set_=dict(
                name=i.excluded.name, login_email=i.excluded.login_email
            ),
        )

        assert_raises(
            exc.OperationalError,
            conn.execute,
            i,
            dict(
                id=1,
                name="namebogus",
                login_email="bogus@gmail.com",
                lets_index_this="bogus",
            ),
        )

    def test_on_conflict_do_update_exotic_targets_six(self, connection):
        users = self.tables.users_xtra

        conn = connection
        conn.execute(
            insert(users),
            dict(
                id=1,
                name="name1",
                login_email="mail1@gmail.com",
                lets_index_this="unique_name",
            ),
        )
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=self.unique_partial_index.columns,
            index_where=self.unique_partial_index.dialect_options["sqlite"][
                "where"
            ],
            set_=dict(
                name=i.excluded.name, login_email=i.excluded.login_email
            ),
        )

        conn.execute(
            i,
            [
                dict(
                    name="name1",
                    login_email="mail2@gmail.com",
                    lets_index_this="unique_name",
                )
            ],
        )

        eq_(
            conn.execute(users.select()).fetchall(),
            [(1, "name1", "mail2@gmail.com", "unique_name")],
        )

    def test_on_conflict_do_update_no_row_actually_affected(self, connection):
        users = self.tables.users_xtra

        conn = connection
        self._exotic_targets_fixture(conn)
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.login_email],
            set_=dict(name="new_name"),
            where=(i.excluded.name == "other_name"),
        )
        result = conn.execute(
            i, dict(name="name2", login_email="name1@gmail.com")
        )

        # The last inserted primary key should be 2 here
        # it is taking the result from the exotic fixture
        eq_(result.inserted_primary_key, (2,))

        eq_(
            conn.execute(users.select()).fetchall(),
            [
                (1, "name1", "name1@gmail.com", "not"),
                (2, "name2", "name2@gmail.com", "not"),
            ],
        )

    def test_on_conflict_do_update_special_types_in_set(self, connection):
        bind_targets = self.tables.bind_targets

        conn = connection
        i = insert(bind_targets)
        conn.execute(i, {"id": 1, "data": "initial data"})

        eq_(
            conn.scalar(sql.select(bind_targets.c.data)),
            "initial data processed",
        )

        i = insert(bind_targets)
        i = i.on_conflict_do_update(
            index_elements=[bind_targets.c.id],
            set_=dict(data="new updated data"),
        )
        conn.execute(i, {"id": 1, "data": "new inserted data"})

        eq_(
            conn.scalar(sql.select(bind_targets.c.data)),
            "new updated data processed",
        )

    @testing.variation("use_returning", [True, False])
    @testing.variation("bindtype", ["samename", "differentname", "fixed"])
    def test_on_conflict_do_update_bindparam(
        self, connection, metadata, use_returning, bindtype
    ):
        """Test issue #13130 - ON CONFLICT DO UPDATE with various bindparam
        patterns.

        Tests insertmanyvalues batching behavior with ON CONFLICT DO UPDATE:

        - samename: bindparam with same name in VALUES and SET
        - differentname: bindparam with different names in VALUES vs SET
        - fixed: bindparam with fixed internal value in SET - should batch
          normally

        Expected insertmanyvalues behavior:

        - samename/differentname + use_returning: row-at-a-time (batch_size=1)
        - samename/differentname + !use_returning: insertmanyvalues disabled
        - fixed + use_returning: normal batching
        - fixed + !use_returning: insertmanyvalues disabled
        """
        t = Table(
            "test_upsert_params",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("data", String(50)),
            UniqueConstraint("name", name="uq_test_upsert_params"),
        )
        t.create(connection)

        # Build the statement based on bindtype
        stmt = insert(t).values({"name": bindparam("name")})

        if bindtype.samename:
            stmt = stmt.on_conflict_do_update(
                set_={"name": bindparam("name")}, index_elements=["name"]
            )
            params_insert = [{"name": "Foo"}, {"name": "Bar"}]
            params_update = [{"name": "Foo"}, {"name": "Bar"}]
            expected_initial = [("Bar", None), ("Foo", None)]
            expected_updated = [("Bar", None), ("Foo", None)]
        elif bindtype.differentname:
            stmt = insert(t).values({"name": bindparam("name1")})
            stmt = stmt.on_conflict_do_update(
                set_={"name": bindparam("name2")}, index_elements=["name"]
            )
            params_insert = [
                {"name1": "Foo", "name2": "Foo"},
                {"name1": "Bar", "name2": "Bar"},
            ]
            params_update = [
                {"name1": "Foo", "name2": "Foo_updated"},
                {"name1": "Bar", "name2": "Bar_updated"},
            ]
            expected_initial = [("Bar", None), ("Foo", None)]
            expected_updated = [("Bar_updated", None), ("Foo_updated", None)]
        else:  # bindtype.fixed
            stmt = stmt.on_conflict_do_update(
                set_={"data": "newdata"}, index_elements=["name"]
            )
            params_insert = [{"name": "Foo"}, {"name": "Bar"}]
            params_update = [{"name": "Foo"}, {"name": "Bar"}]
            expected_initial = [("Bar", None), ("Foo", None)]
            expected_updated = [("Bar", "newdata"), ("Foo", "newdata")]

        if use_returning:
            stmt = stmt.returning(t.c.id, t.c.name, t.c.data)

        # Initial insert
        result = connection.execute(stmt, params_insert)

        # Verify _insertmanyvalues state
        compiled = result.context.compiled
        if use_returning:
            # With RETURNING, insertmanyvalues should be enabled
            assert compiled._insertmanyvalues is not None
            if bindtype.samename or bindtype.differentname:
                # Parametrized bindparams - flag should be True
                eq_(
                    compiled._insertmanyvalues.has_upsert_bound_parameters,
                    True,
                )
            else:  # bindtype.fixed
                # Fixed value bindparam - flag should be False
                eq_(
                    compiled._insertmanyvalues.has_upsert_bound_parameters,
                    False,
                )
        else:
            # Without RETURNING, insertmanyvalues is disabled for ON CONFLICT
            eq_(compiled._insertmanyvalues, None)

        if use_returning:
            rows = result.all()
            eq_(len(rows), 2)
            eq_(sorted([(r[1], r[2]) for r in rows]), expected_initial)

        eq_(
            connection.execute(
                sql.select(t.c.name, t.c.data).order_by(t.c.name)
            ).fetchall(),
            expected_initial,
        )

        # Test the conflict scenario - update existing rows
        result = connection.execute(stmt, params_update)

        # Verify _insertmanyvalues state for update scenario
        compiled = result.context.compiled
        if use_returning:
            assert compiled._insertmanyvalues is not None
            if bindtype.samename or bindtype.differentname:
                eq_(
                    compiled._insertmanyvalues.has_upsert_bound_parameters,
                    True,
                )
            else:  # bindtype.fixed
                eq_(
                    compiled._insertmanyvalues.has_upsert_bound_parameters,
                    False,
                )
        else:
            eq_(compiled._insertmanyvalues, None)

        if use_returning:
            rows = result.all()
            eq_(len(rows), 2)
            eq_(sorted([(r[1], r[2]) for r in rows]), expected_updated)

        eq_(
            connection.execute(
                sql.select(t.c.name, t.c.data).order_by(t.c.name)
            ).fetchall(),
            expected_updated,
        )
