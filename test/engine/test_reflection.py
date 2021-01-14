import unicodedata

import sqlalchemy as sa
from sqlalchemy import Computed
from sqlalchemy import DefaultClause
from sqlalchemy import event
from sqlalchemy import FetchedValue
from sqlalchemy import ForeignKey
from sqlalchemy import Identity
from sqlalchemy import Index
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import schema
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import UniqueConstraint
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import ComparesTables
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import eq_regex
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_instance_of
from sqlalchemy.testing import is_not
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing import not_in
from sqlalchemy.testing import skip
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.util import ue


class ReflectionTest(fixtures.TestBase, ComparesTables):
    __backend__ = True

    def test_basic_reflection(self, connection, metadata):
        meta = metadata

        users = Table(
            "engine_users",
            meta,
            Column("user_id", sa.INT, primary_key=True),
            Column("user_name", sa.VARCHAR(20), nullable=False),
            Column("test1", sa.CHAR(5), nullable=False),
            Column("test2", sa.Float(5), nullable=False),
            Column("test3", sa.Text),
            Column("test4", sa.Numeric(10, 2), nullable=False),
            Column("test5", sa.Date),
            Column(
                "parent_user_id",
                sa.Integer,
                sa.ForeignKey("engine_users.user_id"),
            ),
            Column("test6", sa.Date, nullable=False),
            Column("test7", sa.Text),
            Column("test8", sa.LargeBinary),
            Column("test_passivedefault2", sa.Integer, server_default="5"),
            Column("test9", sa.LargeBinary(100)),
            Column("test10", sa.Numeric(10, 2)),
            test_needs_fk=True,
        )

        addresses = Table(
            "engine_email_addresses",
            meta,
            Column("address_id", sa.Integer, primary_key=True),
            Column(
                "remote_user_id", sa.Integer, sa.ForeignKey(users.c.user_id)
            ),
            Column("email_address", sa.String(20)),
            test_needs_fk=True,
        )
        meta.create_all(connection)

        meta2 = MetaData()
        reflected_users = Table(
            "engine_users", meta2, autoload_with=connection
        )
        reflected_addresses = Table(
            "engine_email_addresses",
            meta2,
            autoload_with=connection,
        )
        self.assert_tables_equal(users, reflected_users)
        self.assert_tables_equal(addresses, reflected_addresses)

    def test_autoload_with_imply_autoload(self, metadata, connection):
        meta = metadata
        t = Table(
            "t",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("x", sa.String(20)),
            Column("y", sa.Integer),
        )
        meta.create_all(connection)

        meta2 = MetaData()
        reflected_t = Table("t", meta2, autoload_with=connection)
        self.assert_tables_equal(t, reflected_t)

    def test_two_foreign_keys(self, metadata, connection):
        meta = metadata
        Table(
            "t1",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("t2id", sa.Integer, sa.ForeignKey("t2.id")),
            Column("t3id", sa.Integer, sa.ForeignKey("t3.id")),
            test_needs_fk=True,
        )
        Table(
            "t2",
            meta,
            Column("id", sa.Integer, primary_key=True),
            test_needs_fk=True,
        )
        Table(
            "t3",
            meta,
            Column("id", sa.Integer, primary_key=True),
            test_needs_fk=True,
        )
        meta.create_all(connection)
        meta2 = MetaData()
        t1r, t2r, t3r = [
            Table(x, meta2, autoload_with=connection)
            for x in ("t1", "t2", "t3")
        ]
        assert t1r.c.t2id.references(t2r.c.id)
        assert t1r.c.t3id.references(t3r.c.id)

    def test_resolve_fks_false_table(self, connection, metadata):
        meta = metadata
        Table(
            "t1",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("t2id", sa.Integer, sa.ForeignKey("t2.id")),
            test_needs_fk=True,
        )
        Table(
            "t2",
            meta,
            Column("id", sa.Integer, primary_key=True),
            test_needs_fk=True,
        )
        meta.create_all(connection)
        meta2 = MetaData()
        t1 = Table("t1", meta2, resolve_fks=False, autoload_with=connection)
        in_("t1", meta2.tables)
        not_in("t2", meta2.tables)

        assert_raises(
            sa.exc.NoReferencedTableError,
            lambda: list(t1.c.t2id.foreign_keys)[0].column,
        )

        t2 = Table("t2", meta2, autoload_with=connection)

        # now it resolves
        is_true(t1.c.t2id.references(t2.c.id))

    def test_resolve_fks_false_extend_existing(self, connection, metadata):
        meta = metadata
        Table(
            "t1",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("t2id", sa.Integer, sa.ForeignKey("t2.id")),
            test_needs_fk=True,
        )
        Table(
            "t2",
            meta,
            Column("id", sa.Integer, primary_key=True),
            test_needs_fk=True,
        )
        meta.create_all(connection)
        meta2 = MetaData()
        Table("t1", meta2)
        in_("t1", meta2.tables)

        t1 = Table(
            "t1",
            meta2,
            resolve_fks=False,
            autoload_with=connection,
            extend_existing=True,
        )
        not_in("t2", meta2.tables)

        assert_raises(
            sa.exc.NoReferencedTableError,
            lambda: list(t1.c.t2id.foreign_keys)[0].column,
        )

        t2 = Table("t2", meta2, autoload_with=connection)

        # now it resolves
        is_true(t1.c.t2id.references(t2.c.id))

    def test_resolve_fks_false_metadata(self, connection, metadata):
        meta = metadata
        Table(
            "t1",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("t2id", sa.Integer, sa.ForeignKey("t2.id")),
            test_needs_fk=True,
        )
        Table(
            "t2",
            meta,
            Column("id", sa.Integer, primary_key=True),
            test_needs_fk=True,
        )
        meta.create_all(connection)
        meta2 = MetaData()
        meta2.reflect(connection, resolve_fks=False, only=["t1"])
        in_("t1", meta2.tables)
        not_in("t2", meta2.tables)

        t1 = meta2.tables["t1"]

        assert_raises(
            sa.exc.NoReferencedTableError,
            lambda: list(t1.c.t2id.foreign_keys)[0].column,
        )

        meta2.reflect(connection, resolve_fks=False)

        t2 = meta2.tables["t2"]
        is_true(t1.c.t2id.references(t2.c.id))

    def test_nonexistent(self, connection):
        meta = MetaData()
        assert_raises(
            sa.exc.NoSuchTableError,
            Table,
            "nonexistent",
            meta,
            autoload_with=connection,
        )
        assert "nonexistent" not in meta.tables

    def test_include_columns(self, connection, metadata):
        meta = metadata
        foo = Table(
            "foo",
            meta,
            *[Column(n, sa.String(30)) for n in ["a", "b", "c", "d", "e", "f"]]
        )
        meta.create_all(connection)
        meta2 = MetaData()
        foo = Table(
            "foo",
            meta2,
            autoload_with=connection,
            include_columns=["b", "f", "e"],
        )
        # test that cols come back in original order
        eq_([c.name for c in foo.c], ["b", "e", "f"])
        for c in ("b", "f", "e"):
            assert c in foo.c
        for c in ("a", "c", "d"):
            assert c not in foo.c

        # test against a table which is already reflected
        meta3 = MetaData()
        foo = Table("foo", meta3, autoload_with=connection)

        foo = Table(
            "foo", meta3, include_columns=["b", "f", "e"], extend_existing=True
        )
        eq_([c.name for c in foo.c], ["b", "e", "f"])
        for c in ("b", "f", "e"):
            assert c in foo.c
        for c in ("a", "c", "d"):
            assert c not in foo.c

    def test_extend_existing(self, connection, metadata):
        meta = metadata

        Table(
            "t",
            meta,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
            Column("y", Integer),
            Column("z", Integer, server_default="5"),
        )
        meta.create_all(connection)

        m2 = MetaData()
        old_z = Column("z", String, primary_key=True)
        old_y = Column("y", String)
        old_q = Column("q", Integer)
        t2 = Table("t", m2, old_z, old_q)
        eq_(list(t2.primary_key.columns), [t2.c.z])
        t2 = Table(
            "t",
            m2,
            old_y,
            extend_existing=True,
            autoload_with=connection,
        )
        eq_(set(t2.columns.keys()), set(["x", "y", "z", "q", "id"]))

        # this has been the actual behavior, the cols are added together,
        # however the test wasn't checking this correctly
        eq_(list(t2.primary_key.columns), [t2.c.z, t2.c.id])

        assert t2.c.z is not old_z
        assert t2.c.y is old_y
        assert t2.c.z.type._type_affinity is Integer
        assert t2.c.q is old_q

        m3 = MetaData()
        t3 = Table("t", m3, Column("z", Integer))
        t3 = Table(
            "t",
            m3,
            extend_existing=False,
            autoload_with=connection,
        )
        eq_(set(t3.columns.keys()), set(["z"]))

        m4 = MetaData()
        old_z = Column("z", String, primary_key=True)
        old_y = Column("y", String)
        old_q = Column("q", Integer)
        t4 = Table("t", m4, old_z, old_q)
        eq_(list(t4.primary_key.columns), [t4.c.z])
        t4 = Table(
            "t",
            m4,
            old_y,
            extend_existing=True,
            autoload_replace=False,
            autoload_with=connection,
        )
        eq_(set(t4.columns.keys()), set(["x", "y", "z", "q", "id"]))
        eq_(list(t4.primary_key.columns), [t4.c.z, t4.c.id])
        assert t4.c.z is old_z
        assert t4.c.y is old_y
        assert t4.c.z.type._type_affinity is String
        assert t4.c.q is old_q

    def test_extend_existing_reflect_all_dont_dupe_index(
        self, connection, metadata
    ):
        m = metadata
        d = Table(
            "d",
            m,
            Column("id", Integer, primary_key=True),
            Column("foo", String(50)),
            Column("bar", String(50)),
            UniqueConstraint("bar"),
        )
        Index("foo_idx", d.c.foo)
        Table(
            "b",
            m,
            Column("id", Integer, primary_key=True),
            Column("aid", ForeignKey("d.id")),
        )
        m.create_all(connection)

        m2 = MetaData()
        m2.reflect(connection, extend_existing=True)

        eq_(
            len(
                [
                    idx
                    for idx in m2.tables["d"].indexes
                    if idx.name == "foo_idx"
                ]
            ),
            1,
        )
        if (
            # fmt: off
            testing.requires.
            unique_constraint_reflection_no_index_overlap.enabled
            # fmt: on
        ):
            eq_(
                len(
                    [
                        const
                        for const in m2.tables["d"].constraints
                        if isinstance(const, UniqueConstraint)
                    ]
                ),
                1,
            )

    @testing.emits_warning(r".*omitted columns")
    def test_include_columns_indexes(self, connection, metadata):
        m = metadata

        t1 = Table("t1", m, Column("a", sa.Integer), Column("b", sa.Integer))
        sa.Index("foobar", t1.c.a, t1.c.b)
        sa.Index("bat", t1.c.a)
        m.create_all(connection)
        m2 = MetaData()
        t2 = Table("t1", m2, autoload_with=connection)
        assert len(t2.indexes) == 2

        m2 = MetaData()
        t2 = Table("t1", m2, autoload_with=connection, include_columns=["a"])
        assert len(t2.indexes) == 1

        m2 = MetaData()
        t2 = Table(
            "t1", m2, autoload_with=connection, include_columns=["a", "b"]
        )
        assert len(t2.indexes) == 2

    def test_autoload_replace_foreign_key_nonpresent(
        self, connection, metadata
    ):
        """test autoload_replace=False with col plus FK
        establishes the FK not present in the DB.

        """
        Table("a", metadata, Column("id", Integer, primary_key=True))
        Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("a_id", Integer),
        )
        metadata.create_all(connection)

        m2 = MetaData()
        b2 = Table("b", m2, Column("a_id", Integer, sa.ForeignKey("a.id")))
        a2 = Table("a", m2, autoload_with=connection)
        b2 = Table(
            "b",
            m2,
            extend_existing=True,
            autoload_with=connection,
            autoload_replace=False,
        )

        assert b2.c.id is not None
        assert b2.c.a_id.references(a2.c.id)
        eq_(len(b2.constraints), 2)

    def test_autoload_replace_foreign_key_ispresent(
        self, connection, metadata
    ):
        """test autoload_replace=False with col plus FK mirroring
        DB-reflected FK skips the reflected FK and installs
        the in-python one only.

        """
        Table("a", metadata, Column("id", Integer, primary_key=True))
        Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("a_id", Integer, sa.ForeignKey("a.id")),
        )
        metadata.create_all(connection)

        m2 = MetaData()
        b2 = Table("b", m2, Column("a_id", Integer, sa.ForeignKey("a.id")))
        a2 = Table("a", m2, autoload_with=connection)
        b2 = Table(
            "b",
            m2,
            extend_existing=True,
            autoload_with=connection,
            autoload_replace=False,
        )

        assert b2.c.id is not None
        assert b2.c.a_id.references(a2.c.id)
        eq_(len(b2.constraints), 2)

    def test_autoload_replace_foreign_key_removed(self, connection, metadata):
        """test autoload_replace=False with col minus FK that's in the
        DB means the FK is skipped and doesn't get installed at all.

        """
        Table("a", metadata, Column("id", Integer, primary_key=True))
        Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("a_id", Integer, sa.ForeignKey("a.id")),
        )
        metadata.create_all(connection)

        m2 = MetaData()
        b2 = Table("b", m2, Column("a_id", Integer))
        a2 = Table("a", m2, autoload_with=connection)
        b2 = Table(
            "b",
            m2,
            extend_existing=True,
            autoload_with=connection,
            autoload_replace=False,
        )

        assert b2.c.id is not None
        assert not b2.c.a_id.references(a2.c.id)
        eq_(len(b2.constraints), 1)

    def test_autoload_replace_primary_key(self, connection, metadata):
        Table("a", metadata, Column("id", Integer))
        metadata.create_all(connection)

        m2 = MetaData()
        a2 = Table("a", m2, Column("id", Integer, primary_key=True))

        Table(
            "a",
            m2,
            autoload_with=connection,
            autoload_replace=False,
            extend_existing=True,
        )
        eq_(list(a2.primary_key), [a2.c.id])

    def test_autoload_replace_arg(self):
        Table("t", MetaData(), autoload_replace=False)

    def test_autoincrement_col(self, connection, metadata):
        """test that 'autoincrement' is reflected according to sqla's policy.

        Don't mark this test as unsupported for any backend !

        """

        meta = metadata
        Table(
            "test",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("data", sa.String(50)),
            mysql_engine="InnoDB",
        )
        Table(
            "test2",
            meta,
            Column(
                "id", sa.Integer, sa.ForeignKey("test.id"), primary_key=True
            ),
            Column("id2", sa.Integer, primary_key=True),
            Column("data", sa.String(50)),
            mysql_engine="InnoDB",
        )
        meta.create_all(connection)
        m2 = MetaData()
        t1a = Table("test", m2, autoload_with=connection)
        assert t1a._autoincrement_column is t1a.c.id

        t2a = Table("test2", m2, autoload_with=connection)
        assert t2a._autoincrement_column is None

    @skip("sqlite")
    def test_unknown_types(self, connection, metadata):
        """Test the handling of unknown types for the given dialect.

        sqlite is skipped because it has special rules for unknown types using
        'affinity types' - this feature is tested in that dialect's test spec.
        """
        meta = metadata
        t = Table("test", meta, Column("foo", sa.DateTime))

        t.create(connection)

        with mock.patch.object(connection.dialect, "ischema_names", {}):
            m2 = MetaData()

            with testing.expect_warnings("Did not recognize type"):
                t3 = Table("test", m2, autoload_with=connection)
                is_(t3.c.foo.type.__class__, sa.types.NullType)

    def test_basic_override(self, connection, metadata):
        meta = metadata
        table = Table(
            "override_test",
            meta,
            Column("col1", sa.Integer, primary_key=True),
            Column("col2", sa.String(20)),
            Column("col3", sa.Numeric),
        )
        table.create(connection)

        meta2 = MetaData()
        table = Table(
            "override_test",
            meta2,
            Column("col2", sa.Unicode()),
            Column("col4", sa.String(30)),
            autoload_with=connection,
        )

        self.assert_(isinstance(table.c.col1.type, sa.Integer))
        self.assert_(isinstance(table.c.col2.type, sa.Unicode))
        self.assert_(isinstance(table.c.col4.type, sa.String))

    def test_override_upgrade_pk_flag(self, connection, metadata):
        meta = metadata
        table = Table(
            "override_test",
            meta,
            Column("col1", sa.Integer),
            Column("col2", sa.String(20)),
            Column("col3", sa.Numeric),
        )
        table.create(connection)

        meta2 = MetaData()
        table = Table(
            "override_test",
            meta2,
            Column("col1", sa.Integer, primary_key=True),
            autoload_with=connection,
        )

        eq_(list(table.primary_key), [table.c.col1])
        eq_(table.c.col1.primary_key, True)

    def test_override_pkfk(self, connection, metadata):
        """test that you can override columns which contain foreign keys
        to other reflected tables, where the foreign key column is also
        a primary key column"""

        meta = metadata
        Table(
            "users",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("name", sa.String(30)),
        )
        Table(
            "addresses",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("street", sa.String(30)),
        )

        meta.create_all(connection)
        meta2 = MetaData()
        a2 = Table(
            "addresses",
            meta2,
            Column(
                "id", sa.Integer, sa.ForeignKey("users.id"), primary_key=True
            ),
            autoload_with=connection,
        )
        u2 = Table("users", meta2, autoload_with=connection)

        assert list(a2.primary_key) == [a2.c.id]
        assert list(u2.primary_key) == [u2.c.id]
        assert u2.join(a2).onclause.compare(u2.c.id == a2.c.id)

        meta3 = MetaData()
        u3 = Table("users", meta3, autoload_with=connection)
        a3 = Table(
            "addresses",
            meta3,
            Column(
                "id", sa.Integer, sa.ForeignKey("users.id"), primary_key=True
            ),
            autoload_with=connection,
        )

        assert list(a3.primary_key) == [a3.c.id]
        assert list(u3.primary_key) == [u3.c.id]
        assert u3.join(a3).onclause.compare(u3.c.id == a3.c.id)

    def test_override_nonexistent_fk(self, connection, metadata):
        """test that you can override columns and create new foreign
        keys to other reflected tables which have no foreign keys.  this
        is common with MySQL MyISAM tables."""

        meta = metadata
        Table(
            "users",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("name", sa.String(30)),
        )
        Table(
            "addresses",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("street", sa.String(30)),
            Column("user_id", sa.Integer),
        )

        meta.create_all(connection)
        meta2 = MetaData()
        a2 = Table(
            "addresses",
            meta2,
            Column("user_id", sa.Integer, sa.ForeignKey("users.id")),
            autoload_with=connection,
        )
        u2 = Table("users", meta2, autoload_with=connection)
        assert len(a2.c.user_id.foreign_keys) == 1
        assert len(a2.foreign_keys) == 1
        assert [c.parent for c in a2.foreign_keys] == [a2.c.user_id]
        assert [c.parent for c in a2.c.user_id.foreign_keys] == [a2.c.user_id]
        assert list(a2.c.user_id.foreign_keys)[0].parent is a2.c.user_id
        assert u2.join(a2).onclause.compare(u2.c.id == a2.c.user_id)
        meta3 = MetaData()

        u3 = Table("users", meta3, autoload_with=connection)

        a3 = Table(
            "addresses",
            meta3,
            Column("user_id", sa.Integer, sa.ForeignKey("users.id")),
            autoload_with=connection,
        )
        assert u3.join(a3).onclause.compare(u3.c.id == a3.c.user_id)

        meta4 = MetaData()

        u4 = Table(
            "users",
            meta4,
            Column("id", sa.Integer, key="u_id", primary_key=True),
            autoload_with=connection,
        )

        a4 = Table(
            "addresses",
            meta4,
            Column("id", sa.Integer, key="street", primary_key=True),
            Column("street", sa.String(30), key="user_id"),
            Column(
                "user_id", sa.Integer, sa.ForeignKey("users.u_id"), key="id"
            ),
            autoload_with=connection,
        )

        # for the thing happening here with the column collection,
        # see test/base/test_utils.py-> test_replace_switch_key_name.
        assert u4.join(a4).onclause.compare(u4.c.u_id == a4.c.id)
        assert list(u4.primary_key) == [u4.c.u_id]
        assert len(u4.columns) == 2
        assert len(u4.constraints) == 1
        assert len(a4.columns) == 3
        assert len(a4.constraints) == 2

    def test_override_composite_fk(self, connection, metadata):
        """Test double-remove of composite foreign key, when replaced."""

        Table(
            "a",
            metadata,
            Column("x", sa.Integer, primary_key=True),
            Column("y", sa.Integer, primary_key=True),
        )

        Table(
            "b",
            metadata,
            Column("x", sa.Integer, primary_key=True),
            Column("y", sa.Integer, primary_key=True),
            sa.ForeignKeyConstraint(["x", "y"], ["a.x", "a.y"]),
        )

        metadata.create_all(connection)

        meta2 = MetaData()

        c1 = Column("x", sa.Integer, primary_key=True)
        c2 = Column("y", sa.Integer, primary_key=True)
        f1 = sa.ForeignKeyConstraint(["x", "y"], ["a.x", "a.y"])
        b1 = Table("b", meta2, c1, c2, f1, autoload_with=connection)

        assert b1.c.x is c1
        assert b1.c.y is c2
        assert f1 in b1.constraints
        assert len(b1.constraints) == 2

    def test_override_keys(self, metadata, connection):
        """test that columns can be overridden with a 'key',
        and that ForeignKey targeting during reflection still works."""

        meta = metadata
        Table(
            "a",
            meta,
            Column("x", sa.Integer, primary_key=True),
            Column("z", sa.Integer),
            test_needs_fk=True,
        )
        Table(
            "b",
            meta,
            Column("y", sa.Integer, sa.ForeignKey("a.x")),
            test_needs_fk=True,
        )
        meta.create_all(connection)
        m2 = MetaData()
        a2 = Table(
            "a",
            m2,
            Column("x", sa.Integer, primary_key=True, key="x1"),
            autoload_with=connection,
        )
        b2 = Table("b", m2, autoload_with=connection)
        assert a2.join(b2).onclause.compare(a2.c.x1 == b2.c.y)
        assert b2.c.y.references(a2.c.x1)

    def test_nonreflected_fk_raises(self, connection, metadata):
        """test that a NoReferencedColumnError is raised when reflecting
        a table with an FK to another table which has not included the target
        column in its reflection.

        """

        meta = metadata
        Table(
            "a",
            meta,
            Column("x", sa.Integer, primary_key=True),
            Column("z", sa.Integer),
            test_needs_fk=True,
        )
        Table(
            "b",
            meta,
            Column("y", sa.Integer, sa.ForeignKey("a.x")),
            test_needs_fk=True,
        )
        meta.create_all(connection)
        m2 = MetaData()
        a2 = Table("a", m2, include_columns=["z"], autoload_with=connection)
        b2 = Table("b", m2, autoload_with=connection)

        assert_raises(sa.exc.NoReferencedColumnError, a2.join, b2)

    def test_override_existing_fk(self, connection, metadata):
        """test that you can override columns and specify new foreign
        keys to other reflected tables, on columns which *do* already
        have that foreign key, and that the FK is not duped."""

        meta = metadata
        Table(
            "users",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("name", sa.String(30)),
            test_needs_fk=True,
        )
        Table(
            "addresses",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("user_id", sa.Integer, sa.ForeignKey("users.id")),
            test_needs_fk=True,
        )

        meta.create_all(connection)
        meta2 = MetaData()
        a2 = Table(
            "addresses",
            meta2,
            Column("user_id", sa.Integer, sa.ForeignKey("users.id")),
            autoload_with=connection,
        )
        u2 = Table("users", meta2, autoload_with=connection)
        s = sa.select(a2).subquery()

        assert s.c.user_id is not None
        assert len(a2.foreign_keys) == 1
        assert len(a2.c.user_id.foreign_keys) == 1
        assert len(a2.constraints) == 2
        assert [c.parent for c in a2.foreign_keys] == [a2.c.user_id]
        assert [c.parent for c in a2.c.user_id.foreign_keys] == [a2.c.user_id]
        assert list(a2.c.user_id.foreign_keys)[0].parent is a2.c.user_id
        assert u2.join(a2).onclause.compare(u2.c.id == a2.c.user_id)

        meta2 = MetaData()
        u2 = Table(
            "users",
            meta2,
            Column("id", sa.Integer, primary_key=True),
            autoload_with=connection,
        )
        a2 = Table(
            "addresses",
            meta2,
            Column("id", sa.Integer, primary_key=True),
            Column("user_id", sa.Integer, sa.ForeignKey("users.id")),
            autoload_with=connection,
        )
        s = sa.select(a2).subquery()

        assert s.c.user_id is not None
        assert len(a2.foreign_keys) == 1
        assert len(a2.c.user_id.foreign_keys) == 1
        assert len(a2.constraints) == 2
        assert [c.parent for c in a2.foreign_keys] == [a2.c.user_id]
        assert [c.parent for c in a2.c.user_id.foreign_keys] == [a2.c.user_id]
        assert list(a2.c.user_id.foreign_keys)[0].parent is a2.c.user_id
        assert u2.join(a2).onclause.compare(u2.c.id == a2.c.user_id)

    @testing.only_on(["postgresql", "mysql"])
    def test_fk_options(self, connection, metadata):
        """test that foreign key reflection includes options (on
        backends with {dialect}.get_foreign_keys() support)"""

        if testing.against("postgresql"):
            test_attrs = (
                "match",
                "onupdate",
                "ondelete",
                "deferrable",
                "initially",
            )
            addresses_user_id_fkey = sa.ForeignKey(
                # Each option is specifically not a Postgres default, or
                # it won't be returned by PG's inspection
                "users.id",
                name="addresses_user_id_fkey",
                match="FULL",
                onupdate="RESTRICT",
                ondelete="RESTRICT",
                deferrable=True,
                initially="DEFERRED",
            )
        elif testing.against("mysql"):
            # MATCH, DEFERRABLE, and INITIALLY cannot be defined for MySQL
            # ON UPDATE and ON DELETE have defaults of RESTRICT, which are
            # elided by MySQL's inspection
            addresses_user_id_fkey = sa.ForeignKey(
                "users.id",
                name="addresses_user_id_fkey",
                onupdate="CASCADE",
                ondelete="CASCADE",
            )
            test_attrs = ("onupdate", "ondelete")

        meta = metadata
        Table(
            "users",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("name", sa.String(30)),
            test_needs_fk=True,
        )
        Table(
            "addresses",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("user_id", sa.Integer, addresses_user_id_fkey),
            test_needs_fk=True,
        )
        meta.create_all(connection)

        meta2 = MetaData()
        meta2.reflect(connection)
        for fk in meta2.tables["addresses"].foreign_keys:
            ref = addresses_user_id_fkey
            for attr in test_attrs:
                eq_(getattr(fk, attr), getattr(ref, attr))

    def test_pks_not_uniques(self, connection, metadata):
        """test that primary key reflection not tripped up by unique
        indexes"""

        conn = connection
        conn.exec_driver_sql(
            """
            CREATE TABLE book (
                id INTEGER NOT NULL,
                title VARCHAR(100) NOT NULL,
                series INTEGER,
                series_id INTEGER,
                UNIQUE(series, series_id),
                PRIMARY KEY(id)
            )"""
        )

        book = Table("book", metadata, autoload_with=connection)
        assert book.primary_key.contains_column(book.c.id)
        assert not book.primary_key.contains_column(book.c.series)
        eq_(len(book.primary_key), 1)

    def test_fk_error(self, connection, metadata):
        Table(
            "slots",
            metadata,
            Column("slot_id", sa.Integer, primary_key=True),
            Column("pkg_id", sa.Integer, sa.ForeignKey("pkgs.pkg_id")),
            Column("slot", sa.String(128)),
        )

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Foreign key associated with column 'slots.pkg_id' "
            "could not find table 'pkgs' with which to generate "
            "a foreign key to target column 'pkg_id'",
            metadata.create_all,
            connection,
        )

    def test_composite_pks(self, connection, metadata):
        """test reflection of a composite primary key"""

        conn = connection
        conn.exec_driver_sql(
            """
            CREATE TABLE book (
                id INTEGER NOT NULL,
                isbn VARCHAR(50) NOT NULL,
                title VARCHAR(100) NOT NULL,
                series INTEGER NOT NULL,
                series_id INTEGER NOT NULL,
                UNIQUE(series, series_id),
                PRIMARY KEY(id, isbn)
            )"""
        )
        book = Table("book", metadata, autoload_with=connection)
        assert book.primary_key.contains_column(book.c.id)
        assert book.primary_key.contains_column(book.c.isbn)
        assert not book.primary_key.contains_column(book.c.series)
        eq_(len(book.primary_key), 2)

    def test_composite_fk(self, connection, metadata):
        """test reflection of composite foreign keys"""

        meta = metadata
        multi = Table(
            "multi",
            meta,
            Column("multi_id", sa.Integer, primary_key=True),
            Column("multi_rev", sa.Integer, primary_key=True),
            Column("multi_hoho", sa.Integer, primary_key=True),
            Column("name", sa.String(50), nullable=False),
            Column("val", sa.String(100)),
            test_needs_fk=True,
        )
        multi2 = Table(
            "multi2",
            meta,
            Column("id", sa.Integer, primary_key=True),
            Column("foo", sa.Integer),
            Column("bar", sa.Integer),
            Column("lala", sa.Integer),
            Column("data", sa.String(50)),
            sa.ForeignKeyConstraint(
                ["foo", "bar", "lala"],
                ["multi.multi_id", "multi.multi_rev", "multi.multi_hoho"],
            ),
            test_needs_fk=True,
        )
        meta.create_all(connection)

        meta2 = MetaData()
        table = Table("multi", meta2, autoload_with=connection)
        table2 = Table("multi2", meta2, autoload_with=connection)
        self.assert_tables_equal(multi, table)
        self.assert_tables_equal(multi2, table2)
        j = sa.join(table, table2)

        self.assert_(
            sa.and_(
                table.c.multi_id == table2.c.foo,
                table.c.multi_rev == table2.c.bar,
                table.c.multi_hoho == table2.c.lala,
            ).compare(j.onclause)
        )

    @testing.crashes("oracle", "FIXME: unknown, confirm not fails_on")
    @testing.requires.check_constraints
    def test_reserved(self, connection, metadata):

        # check a table that uses a SQL reserved name doesn't cause an
        # error

        meta = metadata
        table_a = Table(
            "select",
            meta,
            Column("not", sa.Integer, primary_key=True),
            Column("from", sa.String(12), nullable=False),
            sa.UniqueConstraint("from", name="when"),
        )
        sa.Index("where", table_a.c["from"])

        if connection.dialect.requires_name_normalize:
            check_col = "TRUE"
        else:
            check_col = "true"
        quoter = connection.dialect.identifier_preparer.quote_identifier

        Table(
            "false",
            meta,
            Column("create", sa.Integer, primary_key=True),
            Column("true", sa.Integer, sa.ForeignKey("select.not")),
            sa.CheckConstraint("%s <> 1" % quoter(check_col), name="limit"),
        )

        table_c = Table(
            "is",
            meta,
            Column("or", sa.Integer, nullable=False, primary_key=True),
            Column("join", sa.Integer, nullable=False, primary_key=True),
            sa.PrimaryKeyConstraint("or", "join", name="to"),
        )
        index_c = sa.Index("else", table_c.c.join)
        meta.create_all(connection)
        index_c.drop(connection)
        meta2 = MetaData()
        Table("select", meta2, autoload_with=connection)
        Table("false", meta2, autoload_with=connection)
        Table("is", meta2, autoload_with=connection)

    def test_reflect_all(self, connection, metadata):
        names = ["rt_%s" % name for name in ("a", "b", "c", "d", "e")]
        nameset = set(names)

        baseline = metadata
        for name in names:
            Table(name, baseline, Column("id", sa.Integer, primary_key=True))
        baseline.create_all(connection)

        m1 = MetaData()
        is_false(m1.tables)
        m1.reflect(connection)
        is_true(nameset.issubset(set(m1.tables.keys())))

        m2 = MetaData()
        m2.reflect(connection, only=["rt_a", "rt_b"])
        eq_(set(m2.tables.keys()), set(["rt_a", "rt_b"]))

        m3 = MetaData()
        m3.reflect(connection, only=lambda name, meta: name == "rt_c")
        eq_(set(m3.tables.keys()), set(["rt_c"]))

        m4 = MetaData()

        assert_raises_message(
            sa.exc.InvalidRequestError,
            r"Could not reflect: requested table\(s\) not available in "
            r"Engine\(.*?\): \(rt_f\)",
            m4.reflect,
            connection,
            only=["rt_a", "rt_f"],
        )

        m5 = MetaData()
        m5.reflect(connection, only=[])
        is_false(m5.tables)

        m6 = MetaData()
        m6.reflect(connection, only=lambda n, m: False)
        is_false(m6.tables)

        m7 = MetaData()
        m7.reflect(connection)
        is_true(nameset.issubset(set(m7.tables.keys())))

        m8_e1 = MetaData()
        rt_c = Table("rt_c", m8_e1)
        m8_e1.reflect(connection, extend_existing=True)
        eq_(set(m8_e1.tables.keys()), set(names))
        eq_(rt_c.c.keys(), ["id"])

        m8_e2 = MetaData()
        rt_c = Table("rt_c", m8_e2)
        m8_e2.reflect(connection, extend_existing=True, only=["rt_a", "rt_c"])
        eq_(set(m8_e2.tables.keys()), set(["rt_a", "rt_c"]))
        eq_(rt_c.c.keys(), ["id"])

        baseline.drop_all(connection)
        m9 = MetaData()
        m9.reflect(connection)
        is_false(m9.tables)

    def test_reflect_all_unreflectable_table(self, connection, metadata):
        names = ["rt_%s" % name for name in ("a", "b", "c", "d", "e")]

        for name in names:
            Table(name, metadata, Column("id", sa.Integer, primary_key=True))
        metadata.create_all(connection)

        m = MetaData()

        inspector = sa.engine.reflection.Inspector
        reflect_table = inspector.reflect_table

        def patched(self, table, *arg, **kw):
            if table.name == "rt_c":
                raise sa.exc.UnreflectableTableError("Can't reflect rt_c")
            else:
                return reflect_table(self, table, *arg, **kw)

        with mock.patch.object(inspector, "reflect_table", patched):
            with expect_warnings("Skipping table rt_c: Can't reflect rt_c"):
                m.reflect(connection)

            assert_raises_message(
                sa.exc.UnreflectableTableError,
                "Can't reflect rt_c",
                Table,
                "rt_c",
                m,
                autoload_with=connection,
            )

    def test_index_reflection(self, connection, metadata):
        m1 = metadata
        t1 = Table(
            "party",
            m1,
            Column("id", sa.Integer, nullable=False),
            Column("name", sa.String(20), index=True),
        )
        sa.Index("idx1", t1.c.id, unique=True)
        sa.Index("idx2", t1.c.name, t1.c.id, unique=False)
        m1.create_all(connection)
        m2 = MetaData()
        t2 = Table("party", m2, autoload_with=connection)

        assert len(t2.indexes) == 3
        # Make sure indexes are in the order we expect them in
        tmp = [(idx.name, idx) for idx in t2.indexes]
        tmp.sort()
        r1, r2, r3 = [idx[1] for idx in tmp]

        assert r1.name == "idx1"
        assert r2.name == "idx2"
        assert r1.unique == True  # noqa
        assert r2.unique == False  # noqa
        assert r3.unique == False  # noqa
        assert set([t2.c.id]) == set(r1.columns)
        assert set([t2.c.name, t2.c.id]) == set(r2.columns)
        assert set([t2.c.name]) == set(r3.columns)

    @testing.requires.comment_reflection
    def test_comment_reflection(self, connection, metadata):
        m1 = metadata
        Table(
            "sometable",
            m1,
            Column("id", sa.Integer, comment="c1 comment"),
            comment="t1 comment",
        )
        m1.create_all(connection)
        m2 = MetaData()
        t2 = Table("sometable", m2, autoload_with=connection)

        eq_(t2.comment, "t1 comment")
        eq_(t2.c.id.comment, "c1 comment")

        t3 = Table("sometable", m2, extend_existing=True)
        eq_(t3.comment, "t1 comment")
        eq_(t3.c.id.comment, "c1 comment")

    @testing.requires.check_constraint_reflection
    def test_check_constraint_reflection(self, connection, metadata):
        m1 = metadata
        Table(
            "x",
            m1,
            Column("q", Integer),
            sa.CheckConstraint("q > 10", name="ck1"),
        )
        m1.create_all(connection)
        m2 = MetaData()
        t2 = Table("x", m2, autoload_with=connection)

        ck = [
            const
            for const in t2.constraints
            if isinstance(const, sa.CheckConstraint)
        ][0]

        eq_regex(ck.sqltext.text, r"[\(`]*q[\)`]* > 10")
        eq_(ck.name, "ck1")

    def test_index_reflection_cols_busted(self, connection, metadata):
        t = Table("x", metadata, Column("a", Integer), Column("b", Integer))
        sa.Index("x_ix", t.c.a, t.c.b)
        metadata.create_all(connection)

        def mock_get_columns(self, connection, table_name, **kw):
            return [{"name": "b", "type": Integer, "primary_key": False}]

        with testing.mock.patch.object(
            connection.dialect, "get_columns", mock_get_columns
        ):
            m = MetaData()
            with testing.expect_warnings(
                "index key 'a' was not located in columns"
            ):
                t = Table("x", m, autoload_with=connection)

        eq_(list(t.indexes)[0].columns, [t.c.b])

    @testing.requires.views
    def test_views(self, connection, metadata):
        users, addresses, dingalings = createTables(metadata)
        try:
            metadata.create_all(connection)
            _create_views(connection, None)
            m2 = MetaData()
            users_v = Table("users_v", m2, autoload_with=connection)
            addresses_v = Table(
                "email_addresses_v", m2, autoload_with=connection
            )

            for c1, c2 in zip(users_v.c, users.c):
                eq_(c1.name, c2.name)
                self.assert_types_base(c1, c2)

            for c1, c2 in zip(addresses_v.c, addresses.c):
                eq_(c1.name, c2.name)
                self.assert_types_base(c1, c2)
        finally:
            _drop_views(connection)

    @testing.requires.views
    def test_reflect_all_with_views(self, connection, metadata):
        users, addresses, dingalings = createTables(metadata, None)
        try:
            metadata.create_all(connection)
            _create_views(connection, None)
            m2 = MetaData()

            m2.reflect(connection, views=False)
            eq_(
                set(m2.tables), set(["users", "email_addresses", "dingalings"])
            )

            m2 = MetaData()
            m2.reflect(connection, views=True)
            eq_(
                set(m2.tables),
                set(
                    [
                        "email_addresses_v",
                        "users_v",
                        "users",
                        "dingalings",
                        "email_addresses",
                    ]
                ),
            )
        finally:
            _drop_views(connection)


class CreateDropTest(fixtures.TablesTest):
    __backend__ = True

    run_create_tables = None

    @classmethod
    def teardown_test_class(cls):
        # TablesTest is used here without
        # run_create_tables, so add an explicit drop of whatever is in
        # metadata
        cls._tables_metadata.drop_all(testing.db)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "user_id",
                sa.Integer,
                sa.Sequence("user_id_seq", optional=True),
                primary_key=True,
            ),
            Column("user_name", sa.String(40)),
        )

        Table(
            "email_addresses",
            metadata,
            Column(
                "address_id",
                sa.Integer,
                sa.Sequence("address_id_seq", optional=True),
                primary_key=True,
            ),
            Column("user_id", sa.Integer, sa.ForeignKey("users.user_id")),
            Column("email_address", sa.String(40)),
        )

        Table(
            "orders",
            metadata,
            Column(
                "order_id",
                sa.Integer,
                sa.Sequence("order_id_seq", optional=True),
                primary_key=True,
            ),
            Column("user_id", sa.Integer, sa.ForeignKey("users.user_id")),
            Column("description", sa.String(50)),
            Column("isopen", sa.Integer),
        )
        Table(
            "items",
            metadata,
            Column(
                "item_id",
                sa.INT,
                sa.Sequence("items_id_seq", optional=True),
                primary_key=True,
            ),
            Column("order_id", sa.INT, sa.ForeignKey("orders")),
            Column("item_name", sa.VARCHAR(50)),
        )

    def test_sorter(self):
        tables = self.tables_test_metadata.sorted_tables
        table_names = [t.name for t in tables]
        ua = [n for n in table_names if n in ("users", "email_addresses")]
        oi = [n for n in table_names if n in ("orders", "items")]

        eq_(ua, ["users", "email_addresses"])
        eq_(oi, ["orders", "items"])

    def test_checkfirst(self, connection):
        insp = inspect(connection)
        users = self.tables.users

        is_false(insp.has_table("users"))
        users.create(connection)
        is_true(insp.has_table("users"))
        users.create(connection, checkfirst=True)
        users.drop(connection)
        users.drop(connection, checkfirst=True)
        is_false(insp.has_table("users"))
        users.create(connection, checkfirst=True)
        users.drop(connection)

    def test_createdrop(self, connection):
        insp = inspect(connection)

        metadata = self.tables_test_metadata

        metadata.create_all(connection)
        is_true(insp.has_table("items"))
        is_true(insp.has_table("email_addresses"))
        metadata.create_all(connection)
        is_true(insp.has_table("items"))

        metadata.drop_all(connection)
        is_false(insp.has_table("items"))
        is_false(insp.has_table("email_addresses"))
        metadata.drop_all(connection)
        is_false(insp.has_table("items"))

    def test_tablenames(self, connection):
        metadata = self.tables_test_metadata
        metadata.create_all(bind=connection)
        insp = inspect(connection)

        # ensure all tables we created are in the list.
        is_true(set(insp.get_table_names()).issuperset(metadata.tables))


class SchemaManipulationTest(fixtures.TestBase):
    __backend__ = True

    def test_append_constraint_unique(self):
        meta = MetaData()

        users = Table("users", meta, Column("id", sa.Integer))
        addresses = Table(
            "addresses",
            meta,
            Column("id", sa.Integer),
            Column("user_id", sa.Integer),
        )

        fk = sa.ForeignKeyConstraint(["user_id"], [users.c.id])

        addresses.append_constraint(fk)
        addresses.append_constraint(fk)
        assert len(addresses.c.user_id.foreign_keys) == 1
        assert addresses.constraints == set([addresses.primary_key, fk])


class UnicodeReflectionTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):

        no_multibyte_period = set([("plain", "col_plain", "ix_plain")])
        no_has_table = [
            (
                "no_has_table_1",
                ue("col_Unit\u00e9ble"),
                ue("ix_Unit\u00e9ble"),
            ),
            ("no_has_table_2", ue("col_\u6e2c\u8a66"), ue("ix_\u6e2c\u8a66")),
        ]
        no_case_sensitivity = [
            (
                ue("\u6e2c\u8a66"),
                ue("col_\u6e2c\u8a66"),
                ue("ix_\u6e2c\u8a66"),
            ),
            (
                ue("unit\u00e9ble"),
                ue("col_unit\u00e9ble"),
                ue("ix_unit\u00e9ble"),
            ),
        ]
        full = [
            (
                ue("Unit\u00e9ble"),
                ue("col_Unit\u00e9ble"),
                ue("ix_Unit\u00e9ble"),
            ),
            (
                ue("\u6e2c\u8a66"),
                ue("col_\u6e2c\u8a66"),
                ue("ix_\u6e2c\u8a66"),
            ),
        ]

        # as you can see, our options for this kind of thing
        # are really limited unless you're on PG or SQLite

        # forget about it on these backends
        if not testing.requires.unicode_ddl.enabled:
            names = no_multibyte_period
        # mysql can't handle casing usually
        elif (
            testing.against("mysql")
            and not testing.requires.mysql_fully_case_sensitive.enabled
        ):
            names = no_multibyte_period.union(no_case_sensitivity)
        # mssql + pyodbc + freetds can't compare multibyte names to
        # information_schema.tables.table_name
        elif testing.against("mssql"):
            names = no_multibyte_period.union(no_has_table)
        else:
            names = no_multibyte_period.union(full)

        for tname, cname, ixname in names:
            t = Table(
                tname,
                metadata,
                Column(
                    "id",
                    sa.Integer,
                    sa.Sequence(cname + "_id_seq"),
                    primary_key=True,
                ),
                Column(cname, Integer),
            )
            schema.Index(ixname, t.c[cname])

        cls.names = names

    @testing.requires.unicode_connections
    def test_has_table(self, connection):
        insp = inspect(connection)
        for tname, cname, ixname in self.names:
            assert insp.has_table(tname), "Can't detect name %s" % tname

    @testing.requires.unicode_connections
    def test_basic(self, connection):
        # the 'convert_unicode' should not get in the way of the
        # reflection process.  reflect_table for oracle, postgresql
        # (others?) expect non-unicode strings in result sets/bind
        # params

        names = set([rec[0] for rec in self.names])

        reflected = set(inspect(connection).get_table_names())

        if not names.issubset(reflected) and hasattr(unicodedata, "normalize"):

            # Python source files in the utf-8 coding seem to
            # normalize literals as NFC (and the above are
            # explicitly NFC).  Maybe this database normalizes NFD
            # on reflection.

            nfc = set([unicodedata.normalize("NFC", n) for n in names])
            self.assert_(nfc == names)

            # Yep.  But still ensure that bulk reflection and
            # create/drop work with either normalization.

        r = MetaData()
        r.reflect(connection)
        r.drop_all(connection, checkfirst=False)
        r.create_all(connection, checkfirst=False)

    @testing.requires.unicode_connections
    def test_get_names(self, connection):
        inspector = inspect(connection)
        names = dict(
            (tname, (cname, ixname)) for tname, cname, ixname in self.names
        )
        for tname in inspector.get_table_names():
            assert tname in names
            eq_(
                [
                    (rec["name"], rec["column_names"][0])
                    for rec in inspector.get_indexes(tname)
                ],
                [(names[tname][1], names[tname][0])],
            )


class SchemaTest(fixtures.TestBase):
    __backend__ = True

    @testing.requires.schemas
    @testing.requires.cross_schema_fk_reflection
    def test_has_schema(self):
        if not hasattr(testing.db.dialect, "has_schema"):
            testing.config.skip_test(
                "dialect %s doesn't have a has_schema method"
                % testing.db.dialect.name
            )
        with testing.db.connect() as conn:
            eq_(
                testing.db.dialect.has_schema(
                    conn, testing.config.test_schema
                ),
                True,
            )
            eq_(
                testing.db.dialect.has_schema(conn, "sa_fake_schema_123"),
                False,
            )

    @testing.requires.schemas
    @testing.requires.cross_schema_fk_reflection
    @testing.requires.implicit_default_schema
    def test_blank_schema_arg(self, connection, metadata):

        Table(
            "some_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("sid", Integer, sa.ForeignKey("some_other_table.id")),
            schema=testing.config.test_schema,
            test_needs_fk=True,
        )
        Table(
            "some_other_table",
            metadata,
            Column("id", Integer, primary_key=True),
            schema=None,
            test_needs_fk=True,
        )
        metadata.create_all(connection)
        meta2 = MetaData(schema=testing.config.test_schema)
        meta2.reflect(connection)

        eq_(
            set(meta2.tables),
            set(
                [
                    "some_other_table",
                    "%s.some_table" % testing.config.test_schema,
                ]
            ),
        )

    @testing.requires.schemas
    def test_explicit_default_schema(self, connection, metadata):

        schema = connection.dialect.default_schema_name

        assert bool(schema)

        Table(
            "table1",
            metadata,
            Column("col1", sa.Integer, primary_key=True),
            test_needs_fk=True,
            schema=schema,
        )
        Table(
            "table2",
            metadata,
            Column("col1", sa.Integer, primary_key=True),
            Column(
                "col2", sa.Integer, sa.ForeignKey("%s.table1.col1" % schema)
            ),
            test_needs_fk=True,
            schema=schema,
        )
        metadata.create_all(connection)
        metadata.create_all(connection, checkfirst=True)
        eq_(len(metadata.tables), 2)

        m1 = MetaData()
        Table("table1", m1, autoload_with=connection, schema=schema)
        Table("table2", m1, autoload_with=connection, schema=schema)
        eq_(len(m1.tables), 2)

    @testing.requires.schemas
    def test_schema_translation(self, connection, metadata):
        Table(
            "foob",
            metadata,
            Column("q", Integer),
            schema=config.test_schema,
        )
        metadata.create_all(connection)

        m = MetaData()
        map_ = {"foob": config.test_schema}

        c2 = connection.execution_options(schema_translate_map=map_)
        t = Table("foob", m, schema="foob", autoload_with=c2)
        eq_(t.schema, "foob")
        eq_(t.c.keys(), ["q"])

    @testing.requires.schemas
    @testing.fails_on("sybase", "FIXME: unknown")
    def test_explicit_default_schema_metadata(self, connection, metadata):
        schema = connection.dialect.default_schema_name

        is_true(schema)

        metadata.schema = schema
        Table(
            "table1",
            metadata,
            Column("col1", sa.Integer, primary_key=True),
            test_needs_fk=True,
        )
        Table(
            "table2",
            metadata,
            Column("col1", sa.Integer, primary_key=True),
            Column("col2", sa.Integer, sa.ForeignKey("table1.col1")),
            test_needs_fk=True,
        )
        metadata.create_all(connection)
        metadata.create_all(connection, checkfirst=True)

        m1 = MetaData(schema=schema)

        Table("table1", m1, autoload_with=connection)
        Table("table2", m1, autoload_with=connection)
        eq_(len(m1.tables), 2)

    @testing.requires.schemas
    def test_metadata_reflect_schema(self, connection, metadata):
        createTables(metadata, testing.config.test_schema)
        metadata.create_all(connection)
        m2 = MetaData(schema=testing.config.test_schema)
        m2.reflect(connection)
        eq_(
            set(m2.tables),
            set(
                [
                    "%s.dingalings" % testing.config.test_schema,
                    "%s.users" % testing.config.test_schema,
                    "%s.email_addresses" % testing.config.test_schema,
                ]
            ),
        )

    @testing.requires.schemas
    @testing.requires.cross_schema_fk_reflection
    @testing.requires.implicit_default_schema
    def test_reflect_all_schemas_default_overlap(self, connection, metadata):
        Table("t", metadata, Column("id", Integer, primary_key=True))

        Table(
            "t",
            metadata,
            Column("id1", sa.ForeignKey("t.id")),
            schema=testing.config.test_schema,
        )

        metadata.create_all(connection)
        m2 = MetaData()
        m2.reflect(connection, schema=testing.config.test_schema)

        m3 = MetaData()
        m3.reflect(connection)
        m3.reflect(connection, schema=testing.config.test_schema)

        eq_(
            set((t.name, t.schema) for t in m2.tables.values()),
            set((t.name, t.schema) for t in m3.tables.values()),
        )


# Tests related to engine.reflection


def createTables(meta, schema=None):
    if schema:
        schema_prefix = schema + "."
    else:
        schema_prefix = ""

    users = Table(
        "users",
        meta,
        Column("user_id", sa.INT, primary_key=True),
        Column("user_name", sa.VARCHAR(20), nullable=False),
        Column("test1", sa.CHAR(5), nullable=False),
        Column("test2", sa.Float(5), nullable=False),
        Column("test3", sa.Text),
        Column("test4", sa.Numeric(10, 2), nullable=False),
        Column("test5", sa.Date),
        Column(
            "parent_user_id",
            sa.Integer,
            sa.ForeignKey("%susers.user_id" % schema_prefix),
        ),
        Column("test6", sa.Date, nullable=False),
        Column("test7", sa.Text),
        Column("test8", sa.LargeBinary),
        Column("test_passivedefault2", sa.Integer, server_default="5"),
        Column("test9", sa.LargeBinary(100)),
        Column("test10", sa.Numeric(10, 2)),
        schema=schema,
        test_needs_fk=True,
    )
    dingalings = Table(
        "dingalings",
        meta,
        Column("dingaling_id", sa.Integer, primary_key=True),
        Column(
            "address_id",
            sa.Integer,
            sa.ForeignKey("%semail_addresses.address_id" % schema_prefix),
        ),
        Column("data", sa.String(30)),
        schema=schema,
        test_needs_fk=True,
    )
    addresses = Table(
        "email_addresses",
        meta,
        Column("address_id", sa.Integer),
        Column("remote_user_id", sa.Integer, sa.ForeignKey(users.c.user_id)),
        Column("email_address", sa.String(20)),
        sa.PrimaryKeyConstraint("address_id", name="email_ad_pk"),
        schema=schema,
        test_needs_fk=True,
    )

    return (users, addresses, dingalings)


def createIndexes(con, schema=None):
    fullname = "users"
    if schema:
        fullname = "%s.%s" % (schema, "users")
    query = "CREATE INDEX users_t_idx ON %s (test1, test2)" % fullname
    con.execute(sa.sql.text(query))


@testing.requires.views
def _create_views(conn, schema=None):
    for table_name in ("users", "email_addresses"):
        fullname = table_name
        if schema:
            fullname = "%s.%s" % (schema, table_name)
        view_name = fullname + "_v"
        query = "CREATE VIEW %s AS SELECT * FROM %s" % (
            view_name,
            fullname,
        )
        conn.execute(sa.sql.text(query))


@testing.requires.views
def _drop_views(conn, schema=None):
    for table_name in ("email_addresses", "users"):
        fullname = table_name
        if schema:
            fullname = "%s.%s" % (schema, table_name)
        view_name = fullname + "_v"
        query = "DROP VIEW %s" % view_name
        conn.execute(sa.sql.text(query))


class ReverseCasingReflectTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"
    __backend__ = True

    @testing.requires.denormalized_names
    def setup_test(self):
        with testing.db.begin() as conn:
            conn.exec_driver_sql(
                """
            CREATE TABLE weird_casing(
                    col1 char(20),
                    "Col2" char(20),
                    "col3" char(20)
            )
            """
            )

    @testing.requires.denormalized_names
    def teardown_test(self):
        with testing.db.begin() as conn:
            conn.exec_driver_sql("drop table weird_casing")

    @testing.requires.denormalized_names
    def test_direct_quoting(self, connection):
        m = MetaData()
        t = Table("weird_casing", m, autoload_with=connection)
        self.assert_compile(
            t.select(),
            "SELECT weird_casing.col1, "
            'weird_casing."Col2", weird_casing."col3" '
            "FROM weird_casing",
        )


class CaseSensitiveTest(fixtures.TablesTest):
    """Nail down case sensitive behaviors, mostly on MySQL."""

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "SomeTable",
            metadata,
            Column("x", Integer, primary_key=True),
            test_needs_fk=True,
        )
        Table(
            "SomeOtherTable",
            metadata,
            Column("x", Integer, primary_key=True),
            Column("y", Integer, sa.ForeignKey("SomeTable.x")),
            test_needs_fk=True,
        )

    @testing.fails_if(testing.requires._has_mysql_on_windows)
    def test_table_names(self, connection):
        x = inspect(connection).get_table_names()
        assert set(["SomeTable", "SomeOtherTable"]).issubset(x)

    def test_reflect_exact_name(self, connection):
        m = MetaData()
        t1 = Table("SomeTable", m, autoload_with=connection)
        eq_(t1.name, "SomeTable")
        assert t1.c.x is not None

    @testing.fails_if(
        lambda: testing.against(("mysql", "<", (5, 5)))
        and not testing.requires._has_mysql_fully_case_sensitive()
    )
    def test_reflect_via_fk(self, connection):
        m = MetaData()
        t2 = Table("SomeOtherTable", m, autoload_with=connection)
        eq_(t2.name, "SomeOtherTable")
        assert "SomeTable" in m.tables

    @testing.fails_if(testing.requires._has_mysql_fully_case_sensitive)
    @testing.fails_on_everything_except("sqlite", "mysql", "mssql")
    def test_reflect_case_insensitive(self, connection):
        m = MetaData()
        t2 = Table("sOmEtAbLe", m, autoload_with=connection)
        eq_(t2.name, "sOmEtAbLe")


class ColumnEventsTest(fixtures.RemovesEvents, fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        to_reflect = Table(
            "to_reflect",
            metadata,
            Column("x", sa.Integer, primary_key=True, autoincrement=False),
            Column("y", sa.Integer),
            test_needs_fk=True,
        )
        Table(
            "related",
            metadata,
            Column("q", sa.Integer, sa.ForeignKey("to_reflect.x")),
            test_needs_fk=True,
        )
        sa.Index("some_index", to_reflect.c.y)

    def _do_test(
        self, connection, col, update, assert_, tablename="to_reflect"
    ):
        # load the actual Table class, not the test
        # wrapper
        from sqlalchemy.schema import Table

        m = MetaData()

        def column_reflect(insp, table, column_info):
            if column_info["name"] == col:
                column_info.update(update)

        t = Table(
            tablename,
            m,
            autoload_with=connection,
            listeners=[("column_reflect", column_reflect)],
        )
        assert_(t)

        m = MetaData()
        self.event_listen(Table, "column_reflect", column_reflect)
        t2 = Table(tablename, m, autoload_with=connection)
        assert_(t2)

    def test_override_key(self, connection):
        def assertions(table):
            eq_(table.c.YXZ.name, "x")
            eq_(set(table.primary_key), set([table.c.YXZ]))

        self._do_test(connection, "x", {"key": "YXZ"}, assertions)

    def test_override_index(self, connection):
        def assertions(table):
            idx = list(table.indexes)[0]
            eq_(idx.columns, [table.c.YXZ])

        self._do_test(connection, "y", {"key": "YXZ"}, assertions)

    def test_override_key_fk(self, connection):
        m = MetaData()

        def column_reflect(insp, table, column_info):

            if column_info["name"] == "q":
                column_info["key"] = "qyz"
            elif column_info["name"] == "x":
                column_info["key"] = "xyz"

        to_reflect = Table(
            "to_reflect",
            m,
            autoload_with=connection,
            listeners=[("column_reflect", column_reflect)],
        )
        related = Table(
            "related",
            m,
            autoload_with=connection,
            listeners=[("column_reflect", column_reflect)],
        )

        assert related.c.qyz.references(to_reflect.c.xyz)

    def test_override_type(self, connection):
        def assert_(table):
            assert isinstance(table.c.x.type, sa.String)

        self._do_test(connection, "x", {"type": sa.String}, assert_)

    def test_override_info(self, connection):
        self._do_test(
            connection,
            "x",
            {"info": {"a": "b"}},
            lambda table: eq_(table.c.x.info, {"a": "b"}),
        )

    def test_override_server_default_fetchedvalue(self, connection):
        my_default = FetchedValue()
        self._do_test(
            connection,
            "x",
            {"default": my_default},
            lambda table: eq_(table.c.x.server_default, my_default),
        )

    def test_override_server_default_default_clause(self, connection):
        my_default = DefaultClause("1")
        self._do_test(
            connection,
            "x",
            {"default": my_default},
            lambda table: eq_(table.c.x.server_default, my_default),
        )

    def test_override_server_default_plain_text(self, connection):
        my_default = "1"

        def assert_text_of_one(table):
            is_true(
                isinstance(
                    table.c.x.server_default.arg, sql.elements.TextClause
                )
            )
            eq_(str(table.c.x.server_default.arg), "1")

        self._do_test(
            connection, "x", {"default": my_default}, assert_text_of_one
        )

    def test_override_server_default_textclause(self, connection):
        my_default = sa.text("1")

        def assert_text_of_one(table):
            is_true(
                isinstance(
                    table.c.x.server_default.arg, sql.elements.TextClause
                )
            )
            eq_(str(table.c.x.server_default.arg), "1")

        self._do_test(
            connection, "x", {"default": my_default}, assert_text_of_one
        )

    def test_listen_metadata_obj(self, connection):
        m1 = MetaData()

        m2 = MetaData()

        canary = []

        @event.listens_for(m1, "column_reflect")
        def go(insp, table, info):
            canary.append(info["name"])

        Table("related", m1, autoload_with=connection)

        Table("related", m2, autoload_with=connection)

        eq_(canary, ["q", "x", "y"])

    def test_listen_metadata_cls(self, connection):
        m1 = MetaData()

        m2 = MetaData()

        canary = []

        def go(insp, table, info):
            canary.append(info["name"])

        self.event_listen(MetaData, "column_reflect", go)

        Table("related", m1, autoload_with=connection)

        Table("related", m2, autoload_with=connection)

        eq_(canary, ["q", "x", "y", "q", "x", "y"])


class ComputedColumnTest(fixtures.ComputedReflectionFixtureTest):
    def check_table_column(self, table, name, text, persisted):
        is_true(name in table.columns)
        col = table.columns[name]
        is_not(col.computed, None)
        is_instance_of(col.computed, Computed)

        eq_(self.normalize(str(col.computed.sqltext)), text)
        if testing.requires.computed_columns_reflect_persisted.enabled:
            eq_(col.computed.persisted, persisted)
        else:
            is_(col.computed.persisted, None)

    def test_table_reflection(self):
        meta = MetaData()
        table = Table("computed_column_table", meta, autoload_with=config.db)

        self.check_table_column(
            table,
            "computed_no_flag",
            "normal+42",
            testing.requires.computed_columns_default_persisted.enabled,
        )
        if testing.requires.computed_columns_virtual.enabled:
            self.check_table_column(
                table,
                "computed_virtual",
                "normal+2",
                False,
            )
        if testing.requires.computed_columns_stored.enabled:
            self.check_table_column(
                table,
                "computed_stored",
                "normal-42",
                True,
            )


class IdentityColumnTest(fixtures.TablesTest):
    run_inserts = run_deletes = None

    __backend__ = True
    __requires__ = ("identity_columns", "table_reflection")

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column("normal", Integer),
            Column("id1", Integer, Identity(start=2, increment=3)),
        )

    def test_table_reflection(self):
        meta = MetaData()
        table = Table("t1", meta, autoload_with=config.db)

        eq_(table.c.normal.identity, None)
        is_true(table.c.id1.identity is not None)
        eq_(table.c.id1.identity.start, 2)
        eq_(table.c.id1.identity.increment, 3)
