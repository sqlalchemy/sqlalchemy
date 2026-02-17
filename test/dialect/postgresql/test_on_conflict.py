from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import Integer
from sqlalchemy import schema
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import types as sqltypes
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.testing import config
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import AssertsExecutionResults
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertsql import CursorSQL


class OnConflictTest(fixtures.TablesTest, AssertsExecutionResults):
    __only_on__ = ("postgresql >= 9.5",)
    __backend__ = True
    run_define_tables = "each"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )

        Table(
            "users_schema",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            schema=config.test_schema,
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
            postgresql_where=users_xtra.c.lets_index_this == "unique_name",
        )

        cls.unique_constraint = schema.UniqueConstraint(
            users_xtra.c.login_email, name="uq_login_email"
        )
        cls.bogus_index = schema.Index(
            "idx_special_ops",
            users_xtra.c.lets_index_this,
            postgresql_where=users_xtra.c.lets_index_this > "m",
        )

    def test_bad_args(self):
        assert_raises(
            ValueError,
            insert(self.tables.users).on_conflict_do_nothing,
            constraint="id",
            index_elements=["id"],
        )
        assert_raises(
            ValueError,
            insert(self.tables.users).on_conflict_do_update,
            constraint="id",
            index_elements=["id"],
        )
        assert_raises(
            ValueError,
            insert(self.tables.users).on_conflict_do_update,
            constraint="id",
        )
        assert_raises(
            ValueError, insert(self.tables.users).on_conflict_do_update
        )

    def test_on_conflict_do_nothing(self, connection):
        users = self.tables.users

        result = connection.execute(
            insert(users).on_conflict_do_nothing(),
            dict(id=1, name="name1"),
        )
        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        result = connection.execute(
            insert(users).on_conflict_do_nothing(),
            dict(id=1, name="name2"),
        )
        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
            [(1, "name1")],
        )

    def test_on_conflict_do_nothing_connectionless(self, connection):
        users = self.tables.users_xtra

        result = connection.execute(
            insert(users).on_conflict_do_nothing(constraint="uq_login_email"),
            dict(name="name1", login_email="email1"),
        )
        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, (1,))

        result = connection.execute(
            insert(users).on_conflict_do_nothing(constraint="uq_login_email"),
            dict(name="name2", login_email="email1"),
        )
        eq_(result.inserted_primary_key, None)
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
            [(1, "name1", "email1", None)],
        )

    @testing.provide_metadata
    def test_on_conflict_do_nothing_target(self, connection):
        users = self.tables.users

        result = connection.execute(
            insert(users).on_conflict_do_nothing(
                index_elements=users.primary_key.columns
            ),
            dict(id=1, name="name1"),
        )
        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        result = connection.execute(
            insert(users).on_conflict_do_nothing(
                index_elements=users.primary_key.columns
            ),
            dict(id=1, name="name2"),
        )
        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
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

        connection.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.id],
            set_=dict(name=i.excluded.name) if with_dict else i.excluded,
        )
        result = connection.execute(i, dict(id=1, name="name1"))

        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
            [(1, "name1")],
        )

    @testing.combinations(True, False, argnames="use_returning")
    def test_on_conflict_do_update_set_executemany(
        self, connection, use_returning
    ):
        """test #6581"""

        users = self.tables.users

        connection.execute(
            users.insert(),
            [dict(id=1, name="name1"), dict(id=2, name="name2")],
        )

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.id],
            set_={"id": i.excluded.id, "name": i.excluded.name + ".5"},
        )
        if use_returning:
            i = i.returning(users.c.id, users.c.name)

        result = connection.execute(
            i,
            [
                dict(id=1, name="name1"),
                dict(id=2, name="name2"),
                dict(id=3, name="name3"),
            ],
        )

        if use_returning:
            eq_(result.all(), [(1, "name1.5"), (2, "name2.5"), (3, "name3")])

        eq_(
            connection.execute(users.select().order_by(users.c.id)).fetchall(),
            [(1, "name1.5"), (2, "name2.5"), (3, "name3")],
        )

    def test_on_conflict_do_update_schema(self, connection):
        users = self.tables.get("%s.users_schema" % config.test_schema)

        connection.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.id], set_=dict(name=i.excluded.name)
        )
        result = connection.execute(i, dict(id=1, name="name1"))

        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
            [(1, "name1")],
        )

    def test_on_conflict_do_update_column_as_key_set(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.id],
            set_={users.c.name: i.excluded.name},
        )
        result = connection.execute(i, dict(id=1, name="name1"))

        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
            [(1, "name1")],
        )

    def test_on_conflict_do_update_clauseelem_as_key_set(self, connection):
        users = self.tables.users

        class MyElem:
            def __init__(self, expr):
                self.expr = expr

            def __clause_element__(self):
                return self.expr

        connection.execute(
            users.insert(),
            {"id": 1, "name": "name1"},
        )

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.id],
            set_={MyElem(users.c.name): i.excluded.name},
        ).values({MyElem(users.c.id): 1, MyElem(users.c.name): "name1"})
        result = connection.execute(i)

        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
            [(1, "name1")],
        )

    def test_on_conflict_do_update_column_as_key_set_schema(self, connection):
        users = self.tables.get("%s.users_schema" % config.test_schema)

        connection.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.id],
            set_={users.c.name: i.excluded.name},
        )
        result = connection.execute(i, dict(id=1, name="name1"))

        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
            [(1, "name1")],
        )

    def test_on_conflict_do_update_two(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.id],
            set_=dict(id=i.excluded.id, name=i.excluded.name),
        )

        result = connection.execute(i, dict(id=1, name="name2"))
        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
            [(1, "name2")],
        )

    def test_on_conflict_do_update_three(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(name=i.excluded.name),
        )
        result = connection.execute(i, dict(id=1, name="name3"))
        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
            [(1, "name3")],
        )

    def test_on_conflict_do_update_four(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(id=i.excluded.id, name=i.excluded.name),
        ).values(id=1, name="name4")

        result = connection.execute(i)
        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
            [(1, "name4")],
        )

    def test_on_conflict_do_update_five(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(id=10, name="I'm a name"),
        ).values(id=1, name="name4")

        result = connection.execute(i)
        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.id == 10)
            ).fetchall(),
            [(10, "I'm a name")],
        )

    def test_on_conflict_do_update_multivalues(self, connection):
        users = self.tables.users

        connection.execute(users.insert(), dict(id=1, name="name1"))
        connection.execute(users.insert(), dict(id=2, name="name2"))

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

        result = connection.execute(i)
        eq_(result.inserted_primary_key, (None,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(users.select().order_by(users.c.id)).fetchall(),
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

        self._exotic_targets_fixture(connection)
        # try primary key constraint: cause an upsert on unique id column
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(
                name=i.excluded.name, login_email=i.excluded.login_email
            ),
        )
        result = connection.execute(
            i,
            dict(
                id=1,
                name="name2",
                login_email="name1@gmail.com",
                lets_index_this="not",
            ),
        )
        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
            [(1, "name2", "name1@gmail.com", "not")],
        )

    def test_on_conflict_do_update_exotic_targets_three(self, connection):
        users = self.tables.users_xtra

        self._exotic_targets_fixture(connection)
        # try unique constraint: cause an upsert on target
        # login_email, not id
        i = insert(users)
        i = i.on_conflict_do_update(
            constraint=self.unique_constraint,
            set_=dict(
                id=i.excluded.id,
                name=i.excluded.name,
                login_email=i.excluded.login_email,
            ),
        )
        # note: lets_index_this value totally ignored in SET clause.
        result = connection.execute(
            i,
            dict(
                id=42,
                name="nameunique",
                login_email="name2@gmail.com",
                lets_index_this="unique",
            ),
        )
        eq_(result.inserted_primary_key, (42,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.login_email == "name2@gmail.com")
            ).fetchall(),
            [(42, "nameunique", "name2@gmail.com", "not")],
        )

    def test_on_conflict_do_update_exotic_targets_four(self, connection):
        users = self.tables.users_xtra

        self._exotic_targets_fixture(connection)
        # try unique constraint by name: cause an
        # upsert on target login_email, not id
        i = insert(users)
        i = i.on_conflict_do_update(
            constraint=self.unique_constraint.name,
            set_=dict(
                id=i.excluded.id,
                name=i.excluded.name,
                login_email=i.excluded.login_email,
            ),
        )
        # note: lets_index_this value totally ignored in SET clause.

        result = connection.execute(
            i,
            dict(
                id=43,
                name="nameunique2",
                login_email="name2@gmail.com",
                lets_index_this="unique",
            ),
        )
        eq_(result.inserted_primary_key, (43,))
        eq_(result.returned_defaults, None)

        eq_(
            connection.execute(
                users.select().where(users.c.login_email == "name2@gmail.com")
            ).fetchall(),
            [(43, "nameunique2", "name2@gmail.com", "not")],
        )

    @testing.variation("string_index_elements", [True, False])
    def test_on_conflict_do_update_exotic_targets_four_no_pk(
        self, connection, string_index_elements
    ):
        users = self.tables.users_xtra

        self._exotic_targets_fixture(connection)
        # try unique constraint by name: cause an
        # upsert on target login_email, not id
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=(
                ["login_email"]
                if string_index_elements
                else [users.c.login_email]
            ),
            set_=dict(
                id=i.excluded.id,
                name=i.excluded.name,
                login_email=i.excluded.login_email,
            ),
        )

        result = connection.execute(
            i, dict(name="name3", login_email="name1@gmail.com")
        )
        eq_(result.inserted_primary_key, (1,))
        eq_(result.returned_defaults, (1,))

        eq_(
            connection.execute(users.select().order_by(users.c.id)).fetchall(),
            [
                (1, "name3", "name1@gmail.com", "not"),
                (2, "name2", "name2@gmail.com", "not"),
            ],
        )

    def test_on_conflict_do_update_exotic_targets_five(self, connection):
        users = self.tables.users_xtra

        self._exotic_targets_fixture(connection)
        # try bogus index
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=self.bogus_index.columns,
            index_where=self.bogus_index.dialect_options["postgresql"][
                "where"
            ],
            set_=dict(
                name=i.excluded.name, login_email=i.excluded.login_email
            ),
        )

        assert_raises(
            exc.ProgrammingError,
            connection.execute,
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

        connection.execute(
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
            index_where=self.unique_partial_index.dialect_options[
                "postgresql"
            ]["where"],
            set_=dict(
                name=i.excluded.name, login_email=i.excluded.login_email
            ),
        )

        connection.execute(
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
            connection.execute(users.select()).fetchall(),
            [(1, "name1", "mail2@gmail.com", "unique_name")],
        )

    def test_on_conflict_do_update_constraint_can_be_index(self, connection):
        """test #9023"""

        users = self.tables.users_xtra

        connection.execute(
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
            constraint=self.unique_partial_index,
            set_=dict(
                name=i.excluded.name, login_email=i.excluded.login_email
            ),
        )

        connection.execute(
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
            connection.execute(users.select()).fetchall(),
            [(1, "name1", "mail2@gmail.com", "unique_name")],
        )

    def test_on_conflict_do_update_no_row_actually_affected(self, connection):
        users = self.tables.users_xtra

        self._exotic_targets_fixture(connection)
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.login_email],
            set_=dict(name="new_name"),
            where=(i.excluded.name == "other_name"),
        )
        result = connection.execute(
            i, dict(name="name2", login_email="name1@gmail.com")
        )

        eq_(result.returned_defaults, None)
        eq_(result.inserted_primary_key, None)

        eq_(
            connection.execute(users.select()).fetchall(),
            [
                (1, "name1", "name1@gmail.com", "not"),
                (2, "name2", "name2@gmail.com", "not"),
            ],
        )

    def test_on_conflict_do_update_special_types_in_set(self, connection):
        bind_targets = self.tables.bind_targets

        i = insert(bind_targets)
        connection.execute(i, {"id": 1, "data": "initial data"})

        eq_(
            connection.scalar(sql.select(bind_targets.c.data)),
            "initial data processed",
        )

        i = insert(bind_targets)
        i = i.on_conflict_do_update(
            index_elements=[bind_targets.c.id],
            set_=dict(data="new updated data"),
        )
        connection.execute(i, {"id": 1, "data": "new inserted data"})

        eq_(
            connection.scalar(sql.select(bind_targets.c.data)),
            "new updated data processed",
        )

    def test_on_conflict_do_update_multirow_returning_ordered(
        self, connection
    ):
        """Test that ON CONFLICT works with multiple rows,
        RETURNING, and sort_by_parameter_order=True.

        This is a regression test for issue #13107 where the
        insertmanyvalues sentinel counter was not being added
        to the VALUES clause when on_conflict_do_update was
        present with sort_by_parameter_order=True and the
        primary key was autoincrement (not provided in data).
        """
        users_xtra = self.tables.users_xtra

        stmt = insert(users_xtra)
        stmt = stmt.on_conflict_do_update(
            index_elements=["login_email"],
            set_={
                "name": stmt.excluded.name,
            },
        )

        result = connection.execute(
            stmt.returning(
                users_xtra.c.id,
                users_xtra.c.name,
                sort_by_parameter_order=True,
            ),
            [
                {
                    "name": "name1",
                    "login_email": "user1@example.com",
                    "lets_index_this": "a",
                },
                {
                    "name": "name2",
                    "login_email": "user2@example.com",
                    "lets_index_this": "b",
                },
                {
                    "name": "name3",
                    "login_email": "user3@example.com",
                    "lets_index_this": "c",
                },
            ],
        )

        # Verify rows are returned in parameter order (names match)
        rows = result.all()
        eq_([row[1] for row in rows], ["name1", "name2", "name3"])
        # Store IDs for later verification
        id1, id2, id3 = [row[0] for row in rows]

        # Verify data was inserted
        all_rows = connection.execute(
            sql.select(users_xtra.c.id, users_xtra.c.name).order_by(
                users_xtra.c.id
            )
        ).all()
        eq_(all_rows, [(id1, "name1"), (id2, "name2"), (id3, "name3")])

        # Now update one and insert a new one

        with self.sql_execution_asserter() as asserter:
            result = connection.execute(
                stmt.returning(
                    users_xtra.c.id,
                    users_xtra.c.name,
                    sort_by_parameter_order=True,
                ),
                [
                    {
                        "name": "name2_updated",
                        "login_email": "user2@example.com",
                        "lets_index_this": "b",
                    },
                    {
                        "name": "name4",
                        "login_email": "user4@example.com",
                        "lets_index_this": "d",
                    },
                ],
            )

        if testing.against("+psycopg"):
            asserter.assert_(
                CursorSQL(
                    "INSERT INTO users_xtra (name, login_email,"
                    " lets_index_this) SELECT p0::VARCHAR, p1::VARCHAR,"
                    " p2::VARCHAR FROM (VALUES (%(name__0)s::VARCHAR,"
                    " %(login_email__0)s::VARCHAR,"
                    " %(lets_index_this__0)s::VARCHAR, 0),"
                    " (%(name__1)s::VARCHAR, %(login_email__1)s::VARCHAR,"
                    " %(lets_index_this__1)s::VARCHAR, 1)) AS imp_sen(p0, p1,"
                    " p2, sen_counter) ORDER BY sen_counter ON CONFLICT"
                    " (login_email) DO UPDATE SET name = excluded.name"
                    " RETURNING users_xtra.id, users_xtra.name, users_xtra.id"
                    " AS id__1",
                    {
                        "name__0": "name2_updated",
                        "login_email__0": "user2@example.com",
                        "lets_index_this__0": "b",
                        "name__1": "name4",
                        "login_email__1": "user4@example.com",
                        "lets_index_this__1": "d",
                    },
                )
            )
        # Verify rows are returned in parameter order
        rows = result.all()
        eq_([row[1] for row in rows], ["name2_updated", "name4"])
        # First should be update (same ID), second is insert (new ID)
        eq_(rows[0][0], id2)
        id4 = rows[1][0]

        # Verify final state
        eq_(
            connection.execute(
                sql.select(users_xtra.c.id, users_xtra.c.name).order_by(
                    users_xtra.c.id
                )
            ).all(),
            [
                (id1, "name1"),
                (id2, "name2_updated"),
                (id3, "name3"),
                (id4, "name4"),
            ],
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
                set_={"name": bindparam("name")},
                constraint="uq_test_upsert_params",
            )
            params_insert = [{"name": "Foo"}, {"name": "Bar"}]
            params_update = [{"name": "Foo"}, {"name": "Bar"}]
            expected_initial = [("Bar", None), ("Foo", None)]
            expected_updated = [("Bar", None), ("Foo", None)]
        elif bindtype.differentname:
            stmt = insert(t).values({"name": bindparam("name1")})
            stmt = stmt.on_conflict_do_update(
                set_={"name": bindparam("name2")},
                constraint="uq_test_upsert_params",
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
                set_={"data": "newdata"},
                constraint="uq_test_upsert_params",
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
