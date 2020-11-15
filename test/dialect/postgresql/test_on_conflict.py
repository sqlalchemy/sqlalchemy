# coding: utf-8

from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import Integer
from sqlalchemy import schema
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.testing import config
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import eq_


class OnConflictTest(fixtures.TablesTest):

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

    def test_on_conflict_do_update_one(self, connection):
        users = self.tables.users

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

        class MyElem(object):
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

    def test_on_conflict_do_update_exotic_targets_four_no_pk(self, connection):
        users = self.tables.users_xtra

        self._exotic_targets_fixture(connection)
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
