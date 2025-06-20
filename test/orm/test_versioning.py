import contextlib
import datetime
from unittest.mock import patch
import uuid

import sqlalchemy as sa
from sqlalchemy import Date
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import orm
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import TypeDecorator
from sqlalchemy import util
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assert_warns
from sqlalchemy.testing import assert_warns_message
from sqlalchemy.testing import config
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


def make_uuid():
    return uuid.uuid4().hex


@contextlib.contextmanager
def conditional_sane_rowcount_warnings(
    update=False, delete=False, only_returning=False
):
    warnings = ()
    if not testing.db.dialect.supports_sane_rowcount:
        if update:
            warnings += (
                "Dialect .* does not support "
                "updated rowcount - versioning cannot be verified.",
            )
        if delete:
            warnings += (
                "Dialect .* does not support "
                "deleted rowcount - versioning cannot be verified.",
            )

        with expect_warnings(*warnings):
            yield
    else:
        yield


class NullVersionIdTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "version_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("version_id", Integer),
            Column("value", String(40), nullable=False),
        )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

    def _fixture(self):
        Foo, version_table = self.classes.Foo, self.tables.version_table

        self.mapper_registry.map_imperatively(
            Foo,
            version_table,
            version_id_col=version_table.c.version_id,
            version_id_generator=False,
        )

        s1 = fixture_session()
        return s1

    def test_null_version_id_insert(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value="f1")
        s1.add(f1)

        # Prior to the fix for #3673, you would have been allowed to insert
        # the above record with a NULL version_id and you would have gotten
        # the following error when you tried to update it. Now you should
        # get a FlushError on the initial insert.
        #
        # A value is required for bind parameter 'version_table_version_id'
        # UPDATE version_table SET value=?
        #    WHERE version_table.id = ?
        #    AND version_table.version_id = ?
        # parameters: [{'version_table_id': 1, 'value': 'f1rev2'}]]

        assert_raises_message(
            sa.orm.exc.FlushError,
            "Instance does not contain a non-NULL version value",
            s1.commit,
        )

    def test_null_version_id_update(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value="f1", version_id=1)
        s1.add(f1)
        s1.commit()

        # Prior to the fix for #3673, you would have been allowed to update
        # the above record with a NULL version_id, and it would look like
        # this, post commit: Foo(id=1, value='f1rev2', version_id=None). Now
        # you should get a FlushError on update.

        f1.value = "f1rev2"

        with conditional_sane_rowcount_warnings(update=True):
            f1.version_id = None
            assert_raises_message(
                sa.orm.exc.FlushError,
                "Instance does not contain a non-NULL version value",
                s1.commit,
            )


class VersioningTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "version_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("version_id", Integer, nullable=False),
            Column("value", String(40), nullable=False),
            test_needs_acid=True,
        )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

    def _fixture(self):
        Foo, version_table = self.classes.Foo, self.tables.version_table

        self.mapper_registry.map_imperatively(
            Foo, version_table, version_id_col=version_table.c.version_id
        )
        s1 = fixture_session()
        return s1

    @engines.close_open_connections
    def test_notsane_warning(self):
        Foo = self.classes.Foo

        save = testing.db.dialect.supports_sane_rowcount
        testing.db.dialect.supports_sane_rowcount = False
        try:
            s1 = self._fixture()
            f1 = Foo(value="f1")
            f2 = Foo(value="f2")
            s1.add_all((f1, f2))
            s1.commit()

            f1.value = "f1rev2"
            assert_warns(sa.exc.SAWarning, s1.commit)
        finally:
            testing.db.dialect.supports_sane_rowcount = save

    def test_basic(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value="f1")
        f2 = Foo(value="f2")
        s1.add_all((f1, f2))
        s1.commit()

        f1.value = "f1rev2"
        with conditional_sane_rowcount_warnings(update=True):
            s1.commit()

        s2 = fixture_session()
        f1_s = s2.get(Foo, f1.id)
        f1_s.value = "f1rev3"
        with conditional_sane_rowcount_warnings(update=True):
            s2.commit()

        f1.value = "f1rev3mine"

        # Only dialects with a sane rowcount can detect the
        # StaleDataError
        if testing.db.dialect.supports_sane_rowcount:
            assert_raises_message(
                sa.orm.exc.StaleDataError,
                r"UPDATE statement on table 'version_table' expected "
                r"to update 1 row\(s\); 0 were matched.",
                s1.commit,
            ),
            s1.rollback()
        else:
            with conditional_sane_rowcount_warnings(update=True):
                s1.commit()

        # new in 0.5 !  don't need to close the session
        f1 = s1.get(Foo, f1.id)
        f2 = s1.get(Foo, f2.id)

        f1_s.value = "f1rev4"
        with conditional_sane_rowcount_warnings(update=True):
            s2.commit()

        s1.delete(f1)
        s1.delete(f2)

        if testing.db.dialect.supports_sane_multi_rowcount:
            assert_raises_message(
                sa.orm.exc.StaleDataError,
                r"DELETE statement on table 'version_table' expected "
                r"to delete 2 row\(s\); 1 were matched.",
                s1.commit,
            )
        else:
            with conditional_sane_rowcount_warnings(delete=True):
                s1.commit()

    def test_multiple_updates(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value="f1")
        f2 = Foo(value="f2")
        s1.add_all((f1, f2))
        s1.commit()

        f1.value = "f1rev2"
        f2.value = "f2rev2"
        with conditional_sane_rowcount_warnings(update=True):
            s1.commit()

        eq_(
            s1.query(Foo.id, Foo.value, Foo.version_id).order_by(Foo.id).all(),
            [(f1.id, "f1rev2", 2), (f2.id, "f2rev2", 2)],
        )

    def test_bulk_insert(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        s1.bulk_insert_mappings(
            Foo, [{"id": 1, "value": "f1"}, {"id": 2, "value": "f2"}]
        )
        eq_(
            s1.query(Foo.id, Foo.value, Foo.version_id).order_by(Foo.id).all(),
            [(1, "f1", 1), (2, "f2", 1)],
        )

    def test_bulk_update(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value="f1")
        f2 = Foo(value="f2")
        s1.add_all((f1, f2))
        s1.commit()

        with conditional_sane_rowcount_warnings(update=True):
            s1.bulk_update_mappings(
                Foo,
                [
                    {"id": f1.id, "value": "f1rev2", "version_id": 1},
                    {"id": f2.id, "value": "f2rev2", "version_id": 1},
                ],
            )
        s1.commit()

        eq_(
            s1.query(Foo.id, Foo.value, Foo.version_id).order_by(Foo.id).all(),
            [(f1.id, "f1rev2", 2), (f2.id, "f2rev2", 2)],
        )

    def test_bump_version(self):
        """test that version number can be bumped.

        Ensures that the UPDATE or DELETE is against the
        last committed version of version_id_col, not the modified
        state.

        """

        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value="f1")
        s1.add(f1)
        s1.commit()
        eq_(f1.version_id, 1)
        f1.version_id = 2
        with conditional_sane_rowcount_warnings(update=True):
            s1.commit()
        eq_(f1.version_id, 2)

        # skip an id, test that history
        # is honored
        f1.version_id = 4
        f1.value = "something new"
        with conditional_sane_rowcount_warnings(update=True):
            s1.commit()
        eq_(f1.version_id, 4)

        f1.version_id = 5
        s1.delete(f1)
        with conditional_sane_rowcount_warnings(delete=True):
            s1.commit()
        eq_(s1.query(Foo).count(), 0)

    @engines.close_open_connections
    def test_versioncheck(self):
        """query.with_lockmode performs a 'version check' on an already loaded
        instance"""

        Foo = self.classes.Foo

        s1 = self._fixture()
        f1s1 = Foo(value="f1 value")
        s1.add(f1s1)
        s1.commit()

        s2 = fixture_session()
        f1s2 = s2.get(Foo, f1s1.id)
        f1s2.value = "f1 new value"
        with conditional_sane_rowcount_warnings(update=True):
            s2.commit()

        # load, version is wrong
        assert_raises_message(
            sa.orm.exc.StaleDataError,
            r"Instance .* has version id '\d+' which does not "
            r"match database-loaded version id '\d+'",
            s1.get,
            Foo,
            f1s1.id,
            with_for_update=dict(read=True),
        )

        # reload it - this expires the old version first
        s1.refresh(f1s1, with_for_update={"read": True})

        # now assert version OK
        s1.get(Foo, f1s1.id, with_for_update=dict(read=True))

        # assert brand new load is OK too
        s1.close()
        s1.get(Foo, f1s1.id, with_for_update=dict(read=True))

    def test_versioncheck_not_versioned(self):
        """ensure the versioncheck logic skips if there isn't a
        version_id_col actually configured"""

        Foo = self.classes.Foo
        version_table = self.tables.version_table

        self.mapper_registry.map_imperatively(Foo, version_table)
        s1 = fixture_session()
        f1s1 = Foo(value="f1 value", version_id=1)
        s1.add(f1s1)
        s1.commit()
        s1.query(Foo).with_for_update(read=True).where(Foo.id == f1s1.id).one()

    @engines.close_open_connections
    @testing.requires.update_nowait
    def test_versioncheck_for_update(self):
        """query.with_lockmode performs a 'version check' on an already loaded
        instance"""

        Foo = self.classes.Foo

        s1 = self._fixture()
        f1s1 = Foo(value="f1 value")
        s1.add(f1s1)
        s1.commit()

        s2 = fixture_session()
        f1s2 = s2.get(Foo, f1s1.id)
        # not sure if I like this API
        s2.refresh(f1s2, with_for_update=True)
        f1s2.value = "f1 new value"

        assert_raises(
            exc.DBAPIError, s1.refresh, f1s1, with_for_update={"nowait": True}
        )
        s1.rollback()

        with conditional_sane_rowcount_warnings(update=True):
            s2.commit()
        s1.refresh(f1s1, with_for_update={"nowait": True})
        assert f1s1.version_id == f1s2.version_id

    def test_update_multi_missing_broken_multi_rowcount(self):
        @util.memoized_property
        def rowcount(self):
            if len(self.context.compiled_parameters) > 1:
                return -1
            else:
                return self.context.rowcount

        with (
            patch.object(
                config.db.dialect, "supports_sane_multi_rowcount", False
            ),
            patch("sqlalchemy.engine.cursor.CursorResult.rowcount", rowcount),
        ):
            Foo = self.classes.Foo
            s1 = self._fixture()
            f1s1 = Foo(value="f1 value")
            s1.add(f1s1)
            s1.commit()

            f1s1.value = "f2 value"
            with conditional_sane_rowcount_warnings(update=True):
                s1.flush()
            eq_(f1s1.version_id, 2)

    def test_update_delete_no_plain_rowcount(self):
        with (
            patch.object(config.db.dialect, "supports_sane_rowcount", False),
            patch.object(
                config.db.dialect, "supports_sane_multi_rowcount", False
            ),
        ):
            Foo = self.classes.Foo
            s1 = self._fixture()
            f1s1 = Foo(value="f1 value")
            s1.add(f1s1)
            s1.commit()

            f1s1.value = "f2 value"

            with expect_warnings(
                "Dialect .* does not support updated rowcount - "
                "versioning cannot be verified."
            ):
                s1.flush()
            eq_(f1s1.version_id, 2)

            s1.delete(f1s1)
            with expect_warnings(
                "Dialect .* does not support deleted rowcount - "
                "versioning cannot be verified."
            ):
                s1.flush()

    @engines.close_open_connections
    def test_noversioncheck(self):
        """test query.with_lockmode works when the mapper has no version id
        col"""

        Foo, version_table = self.classes.Foo, self.tables.version_table

        s1 = fixture_session()
        self.mapper_registry.map_imperatively(Foo, version_table)
        f1s1 = Foo(value="foo", version_id=0)
        s1.add(f1s1)
        s1.commit()

        s2 = fixture_session()
        f1s2 = (
            s2.query(Foo)
            .with_for_update(read=True)
            .where(Foo.id == f1s1.id)
            .one()
        )
        assert f1s2.id == f1s1.id
        assert f1s2.value == f1s1.value

    def test_merge_no_version(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value="f1")
        s1.add(f1)
        s1.commit()

        f1.value = "f2"
        with conditional_sane_rowcount_warnings(update=True):
            s1.commit()

        f2 = Foo(id=f1.id, value="f3")
        f3 = s1.merge(f2)
        assert f3 is f1
        with conditional_sane_rowcount_warnings(update=True):
            s1.commit()
        eq_(f3.version_id, 3)

    def test_merge_correct_version(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value="f1")
        s1.add(f1)
        s1.commit()

        f1.value = "f2"
        with conditional_sane_rowcount_warnings(update=True):
            s1.commit()

        f2 = Foo(id=f1.id, value="f3", version_id=2)
        f3 = s1.merge(f2)
        assert f3 is f1
        with conditional_sane_rowcount_warnings(update=True):
            s1.commit()
        eq_(f3.version_id, 3)

    def test_merge_incorrect_version(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value="f1")
        s1.add(f1)
        s1.commit()

        f1.value = "f2"
        with conditional_sane_rowcount_warnings(update=True):
            s1.commit()

        f2 = Foo(id=f1.id, value="f3", version_id=1)
        assert_raises_message(
            orm_exc.StaleDataError,
            "Version id '1' on merged state "
            "<Foo at .*?> does not match existing version '2'. "
            "Leave the version attribute unset when "
            "merging to update the most recent version.",
            s1.merge,
            f2,
        )

    def test_merge_incorrect_version_not_in_session(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(value="f1")
        s1.add(f1)
        s1.commit()

        f1.value = "f2"
        with conditional_sane_rowcount_warnings(update=True):
            s1.commit()

        f2 = Foo(id=f1.id, value="f3", version_id=1)
        s1.close()

        assert_raises_message(
            orm_exc.StaleDataError,
            "Version id '1' on merged state "
            "<Foo at .*?> does not match existing version '2'. "
            "Leave the version attribute unset when "
            "merging to update the most recent version.",
            s1.merge,
            f2,
        )


class VersionOnPostUpdateTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "node",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("version_id", Integer),
            Column("parent_id", ForeignKey("node.id")),
        )

    @classmethod
    def setup_classes(cls):
        class Node(cls.Basic):
            pass

    def _fixture(self, o2m, post_update, insert=True):
        Node = self.classes.Node
        node = self.tables.node

        self.mapper_registry.map_imperatively(
            Node,
            node,
            properties={
                "related": relationship(
                    Node,
                    remote_side=node.c.id if not o2m else node.c.parent_id,
                    post_update=post_update,
                )
            },
            version_id_col=node.c.version_id,
        )

        s = fixture_session()
        n1 = Node(id=1)
        n2 = Node(id=2)

        if insert:
            s.add_all([n1, n2])
            s.flush()
        return s, n1, n2

    def test_o2m_plain(self):
        s, n1, n2 = self._fixture(o2m=True, post_update=False)

        n1.related.append(n2)
        with conditional_sane_rowcount_warnings(update=True):
            s.flush()

        eq_(n1.version_id, 1)
        eq_(n2.version_id, 2)

    def test_m2o_plain(self):
        s, n1, n2 = self._fixture(o2m=False, post_update=False)

        n1.related = n2
        with conditional_sane_rowcount_warnings(update=True):
            s.flush()

        eq_(n1.version_id, 2)
        eq_(n2.version_id, 1)

    def test_o2m_post_update(self):
        s, n1, n2 = self._fixture(o2m=True, post_update=True)

        n1.related.append(n2)
        with conditional_sane_rowcount_warnings(update=True):
            s.flush()

        eq_(n1.version_id, 1)
        eq_(n2.version_id, 2)

    def test_m2o_post_update(self):
        s, n1, n2 = self._fixture(o2m=False, post_update=True)

        n1.related = n2
        with conditional_sane_rowcount_warnings(update=True):
            s.flush()

        eq_(n1.version_id, 2)
        eq_(n2.version_id, 1)

    def test_o2m_post_update_not_assoc_w_insert(self):
        s, n1, n2 = self._fixture(o2m=True, post_update=True, insert=False)

        n1.related.append(n2)
        s.add_all([n1, n2])
        with conditional_sane_rowcount_warnings(update=True):
            s.flush()

        eq_(n1.version_id, 1)
        eq_(n2.version_id, 1)

    def test_m2o_post_update_not_assoc_w_insert(self):
        s, n1, n2 = self._fixture(o2m=False, post_update=True, insert=False)

        n1.related = n2
        s.add_all([n1, n2])
        with conditional_sane_rowcount_warnings(update=True):
            s.flush()

        eq_(n1.version_id, 1)
        eq_(n2.version_id, 1)

    @testing.requires.sane_rowcount
    def test_o2m_post_update_version_assert(self):
        Node = self.classes.Node
        s, n1, n2 = self._fixture(o2m=True, post_update=True)

        n1.related.append(n2)

        # outwit the database transaction isolation and SQLA's
        # expiration at the same time by using different Session on
        # same transaction
        s2 = Session(bind=s.connection(bind_arguments=dict(mapper=Node)))
        s2.query(Node).filter(Node.id == n2.id).update({"version_id": 3})
        s2.commit()

        assert_raises_message(
            orm_exc.StaleDataError,
            "UPDATE statement on table 'node' expected to "
            r"update 1 row\(s\); 0 were matched.",
            s.flush,
        )

    def test_o2m_post_update_no_sane_rowcount(self):
        Node = self.classes.Node
        s, n1, n2 = self._fixture(o2m=True, post_update=True)

        n1.related.append(n2)

        with (
            patch.object(config.db.dialect, "supports_sane_rowcount", False),
            patch.object(
                config.db.dialect, "supports_sane_multi_rowcount", False
            ),
        ):
            s2 = Session(bind=s.connection(bind_arguments=dict(mapper=Node)))
            s2.query(Node).filter(Node.id == n2.id).update({"version_id": 3})
            s2.commit()

            with expect_warnings(
                "Dialect .* does not support updated rowcount - "
                "versioning cannot be verified."
            ):
                s.flush()

    @testing.requires.sane_rowcount
    def test_m2o_post_update_version_assert(self):
        Node = self.classes.Node

        s, n1, n2 = self._fixture(o2m=False, post_update=True)

        n1.related = n2

        # outwit the database transaction isolation and SQLA's
        # expiration at the same time by using different Session on
        # same transaction
        s2 = Session(bind=s.connection(bind_arguments=dict(mapper=Node)))
        s2.query(Node).filter(Node.id == n1.id).update({"version_id": 3})
        s2.commit()

        assert_raises_message(
            orm_exc.StaleDataError,
            "UPDATE statement on table 'node' expected to "
            r"update 1 row\(s\); 0 were matched.",
            s.flush,
        )


class NoBumpOnRelationshipTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("version_id", Integer),
        )
        Table(
            "b",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("a_id", ForeignKey("a.id")),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

    def _run_test(self, auto_version_counter=True):
        A, B = self.classes("A", "B")
        s = fixture_session(future=True)
        if auto_version_counter:
            a1 = A()
        else:
            a1 = A(version_id=1)
        s.add(a1)
        s.commit()
        eq_(a1.version_id, 1)

        b1 = B()
        b1.a = a1
        s.add(b1)
        s.commit()

        eq_(a1.version_id, 1)

    def test_plain_counter(self):
        A, B = self.classes("A", "B")
        a, b = self.tables("a", "b")

        self.mapper_registry.map_imperatively(
            A,
            a,
            properties={"bs": relationship(B, backref="a")},
            version_id_col=a.c.version_id,
        )
        self.mapper_registry.map_imperatively(B, b)

        self._run_test()

    def test_functional_counter(self):
        A, B = self.classes("A", "B")
        a, b = self.tables("a", "b")

        self.mapper_registry.map_imperatively(
            A,
            a,
            properties={"bs": relationship(B, backref="a")},
            version_id_col=a.c.version_id,
            version_id_generator=lambda num: (num or 0) + 1,
        )
        self.mapper_registry.map_imperatively(B, b)

        self._run_test()

    def test_no_counter(self):
        A, B = self.classes("A", "B")
        a, b = self.tables("a", "b")

        self.mapper_registry.map_imperatively(
            A,
            a,
            properties={"bs": relationship(B, backref="a")},
            version_id_col=a.c.version_id,
            version_id_generator=False,
        )
        self.mapper_registry.map_imperatively(B, b)

        self._run_test(False)


class ColumnTypeTest(fixtures.MappedTest):
    __backend__ = True
    __requires__ = ("sane_rowcount",)

    @classmethod
    def define_tables(cls, metadata):
        class SpecialType(TypeDecorator):
            impl = Date
            cache_ok = True

            def process_bind_param(self, value, dialect):
                assert isinstance(value, datetime.date)
                return value

        Table(
            "version_table",
            metadata,
            Column("id", SpecialType, primary_key=True),
            Column("version_id", Integer, nullable=False),
            Column("value", String(40), nullable=False),
        )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

    def _fixture(self):
        Foo, version_table = self.classes.Foo, self.tables.version_table

        self.mapper_registry.map_imperatively(
            Foo, version_table, version_id_col=version_table.c.version_id
        )
        s1 = fixture_session()
        return s1

    @engines.close_open_connections
    def test_update(self):
        Foo = self.classes.Foo

        s1 = self._fixture()
        f1 = Foo(id=datetime.date.today(), value="f1")
        s1.add(f1)
        s1.commit()

        f1.value = "f1rev2"
        with conditional_sane_rowcount_warnings(update=True):
            s1.commit()


class RowSwitchTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "p",
            metadata,
            Column("id", String(10), primary_key=True),
            Column("version_id", Integer, default=1, nullable=False),
            Column("data", String(50)),
        )
        Table(
            "c",
            metadata,
            Column("id", String(10), ForeignKey("p.id"), primary_key=True),
            Column("version_id", Integer, default=1, nullable=False),
            Column("data", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class P(cls.Basic):
            pass

        class C(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        p, c, C, P = cls.tables.p, cls.tables.c, cls.classes.C, cls.classes.P

        cls.mapper_registry.map_imperatively(
            P,
            p,
            version_id_col=p.c.version_id,
            properties={
                "c": relationship(
                    C, uselist=False, cascade="all, delete-orphan"
                )
            },
        )
        cls.mapper_registry.map_imperatively(
            C, c, version_id_col=c.c.version_id
        )

    def test_row_switch(self):
        P = self.classes.P

        session = fixture_session()
        session.add(P(id="P1", data="P version 1"))
        session.commit()
        session.close()

        p = session.query(P).first()
        session.delete(p)
        session.add(P(id="P1", data="really a row-switch"))
        with conditional_sane_rowcount_warnings(update=True):
            session.commit()

    def test_child_row_switch(self):
        P, C = self.classes.P, self.classes.C

        assert P.c.property.strategy.use_get

        session = fixture_session()
        session.add(P(id="P1", data="P version 1"))
        session.commit()
        session.close()

        p = session.query(P).first()
        p.c = C(data="child version 1")
        session.commit()

        p = session.query(P).first()
        p.c = C(data="child row-switch")
        with conditional_sane_rowcount_warnings(update=True):
            session.commit()


class AlternateGeneratorTest(fixtures.MappedTest):
    __backend__ = True
    __requires__ = ("sane_rowcount",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "p",
            metadata,
            Column("id", String(10), primary_key=True),
            Column("version_id", String(32), nullable=False),
            Column("data", String(50)),
        )
        Table(
            "c",
            metadata,
            Column("id", String(10), ForeignKey("p.id"), primary_key=True),
            Column("version_id", String(32), nullable=False),
            Column("data", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class P(cls.Basic):
            pass

        class C(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        p, c, C, P = cls.tables.p, cls.tables.c, cls.classes.C, cls.classes.P

        cls.mapper_registry.map_imperatively(
            P,
            p,
            version_id_col=p.c.version_id,
            version_id_generator=lambda x: make_uuid(),
            properties={
                "c": relationship(
                    C, uselist=False, cascade="all, delete-orphan"
                )
            },
        )
        cls.mapper_registry.map_imperatively(
            C,
            c,
            version_id_col=c.c.version_id,
            version_id_generator=lambda x: make_uuid(),
        )

    def test_row_switch(self):
        P = self.classes.P

        session = fixture_session()
        session.add(P(id="P1", data="P version 1"))
        session.commit()
        session.close()

        p = session.query(P).first()
        session.delete(p)
        session.add(P(id="P1", data="really a row-switch"))
        with conditional_sane_rowcount_warnings(update=True):
            session.commit()

    def test_child_row_switch_one(self):
        P, C = self.classes.P, self.classes.C

        assert P.c.property.strategy.use_get

        session = fixture_session()
        session.add(P(id="P1", data="P version 1"))
        session.commit()
        session.close()

        p = session.query(P).first()
        p.c = C(data="child version 1")
        session.commit()

        p = session.query(P).first()
        p.c = C(data="child row-switch")
        with conditional_sane_rowcount_warnings(update=True):
            session.commit()

    @testing.requires.sane_rowcount
    def test_child_row_switch_two(self):
        P = self.classes.P

        # TODO: not sure this test is
        # testing exactly what its looking for

        sess1 = fixture_session()
        sess1.add(P(id="P1", data="P version 1"))
        sess1.commit()
        sess1.close()

        p1 = sess1.query(P).first()

        sess2 = fixture_session()
        p2 = sess2.query(P).first()

        sess1.delete(p1)
        sess1.commit()

        # this can be removed and it still passes
        sess1.add(P(id="P1", data="P version 2"))
        sess1.commit()

        p2.data = "P overwritten by concurrent tx"
        if testing.db.dialect.supports_sane_rowcount:
            assert_raises_message(
                orm.exc.StaleDataError,
                r"UPDATE statement on table 'p' expected to update "
                r"1 row\(s\); 0 were matched.",
                sess2.commit,
            )
        else:
            sess2.commit()


class PlainInheritanceTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "base",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("version_id", Integer, nullable=True),
            Column("data", String(50)),
        )
        Table(
            "sub",
            metadata,
            Column("id", Integer, ForeignKey("base.id"), primary_key=True),
            Column("sub_data", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class Base(cls.Basic):
            pass

        class Sub(Base):
            pass

    def test_update_child_table_only(self):
        Base, sub, base, Sub = (
            self.classes.Base,
            self.tables.sub,
            self.tables.base,
            self.classes.Sub,
        )

        self.mapper_registry.map_imperatively(
            Base, base, version_id_col=base.c.version_id
        )
        self.mapper_registry.map_imperatively(Sub, sub, inherits=Base)

        s = fixture_session()
        s1 = Sub(data="b", sub_data="s")
        s.add(s1)
        s.commit()

        s1.sub_data = "s2"
        with conditional_sane_rowcount_warnings(update=True):
            s.commit()

        eq_(s1.version_id, 2)


class InheritanceTwoVersionIdsTest(fixtures.MappedTest):
    """Test versioning where both parent/child table have a
    versioning column.

    """

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "base",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("version_id", Integer, nullable=True),
            Column("data", String(50)),
        )
        Table(
            "sub",
            metadata,
            Column("id", Integer, ForeignKey("base.id"), primary_key=True),
            Column("version_id", Integer, nullable=False),
            Column("sub_data", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class Base(cls.Basic):
            pass

        class Sub(Base):
            pass

    def test_base_both(self):
        Base, sub, base, Sub = (
            self.classes.Base,
            self.tables.sub,
            self.tables.base,
            self.classes.Sub,
        )

        self.mapper_registry.map_imperatively(
            Base, base, version_id_col=base.c.version_id
        )
        self.mapper_registry.map_imperatively(Sub, sub, inherits=Base)

        session = fixture_session()
        b1 = Base(data="b1")
        session.add(b1)
        session.commit()
        eq_(b1.version_id, 1)
        # base is populated
        eq_(session.connection().scalar(select(base.c.version_id)), 1)

    def test_sub_both(self):
        Base, sub, base, Sub = (
            self.classes.Base,
            self.tables.sub,
            self.tables.base,
            self.classes.Sub,
        )

        self.mapper_registry.map_imperatively(
            Base, base, version_id_col=base.c.version_id
        )
        self.mapper_registry.map_imperatively(Sub, sub, inherits=Base)

        session = fixture_session()
        s1 = Sub(data="s1", sub_data="s1")
        session.add(s1)
        session.commit()

        # table is populated
        eq_(session.connection().scalar(select(sub.c.version_id)), 1)

        # base is populated
        eq_(session.connection().scalar(select(base.c.version_id)), 1)

    def test_sub_only(self):
        Base, sub, base, Sub = (
            self.classes.Base,
            self.tables.sub,
            self.tables.base,
            self.classes.Sub,
        )

        self.mapper_registry.map_imperatively(Base, base)
        self.mapper_registry.map_imperatively(
            Sub, sub, inherits=Base, version_id_col=sub.c.version_id
        )

        session = fixture_session()
        s1 = Sub(data="s1", sub_data="s1")
        session.add(s1)
        session.commit()

        # table is populated
        eq_(session.connection().scalar(select(sub.c.version_id)), 1)

        # base is not
        eq_(session.connection().scalar(select(base.c.version_id)), None)

    def test_mismatch_version_col_warning(self):
        Base, sub, base, Sub = (
            self.classes.Base,
            self.tables.sub,
            self.tables.base,
            self.classes.Sub,
        )

        self.mapper_registry.map_imperatively(
            Base, base, version_id_col=base.c.version_id
        )

        assert_warns_message(
            exc.SAWarning,
            "Inheriting version_id_col 'version_id' does not "
            "match inherited version_id_col 'version_id' and will not "
            "automatically populate the inherited versioning column. "
            "version_id_col should only be specified on "
            "the base-most mapper that includes versioning.",
            self.mapper_registry.map_imperatively,
            Sub,
            sub,
            inherits=Base,
            version_id_col=sub.c.version_id,
        )


class ServerVersioningTest(fixtures.MappedTest):
    run_define_tables = "each"

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        from sqlalchemy.sql import ColumnElement
        from sqlalchemy.ext.compiler import compiles
        import itertools

        counter = itertools.count(1)

        class IncDefault(ColumnElement):
            pass

        @compiles(IncDefault)
        def compile_(element, compiler, **kw):
            # cache the counter value on the statement
            # itself so the assertsql system gets the same
            # value when it compiles the statement a second time
            stmt = compiler.statement
            if hasattr(stmt, "_counter"):
                return stmt._counter
            else:
                stmt._counter = str(next(counter))
                return stmt._counter

        Table(
            "version_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column(
                "version_id",
                Integer,
                nullable=False,
                default=IncDefault(),
                onupdate=IncDefault(),
            ),
            Column("value", String(40), nullable=False),
        )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

        class Bar(cls.Basic):
            pass

    def _fixture(self, expire_on_commit=True, eager_defaults=False):
        Foo, version_table = self.classes.Foo, self.tables.version_table

        self.mapper_registry.map_imperatively(
            Foo,
            version_table,
            version_id_col=version_table.c.version_id,
            version_id_generator=False,
            eager_defaults=eager_defaults,
        )

        s1 = fixture_session(expire_on_commit=expire_on_commit)
        return s1

    def test_insert_col(self):
        self._test_insert_col()

    def test_insert_col_eager_defaults(self):
        self._test_insert_col(eager_defaults=True)

    def _test_insert_col(self, **kw):
        sess = self._fixture(**kw)

        f1 = self.classes.Foo(value="f1")
        sess.add(f1)

        statements = [
            CompiledSQL(
                "INSERT INTO version_table (version_id, value) "
                "VALUES (1, :value) "
                "RETURNING version_table.id, version_table.version_id",
                lambda ctx: [{"value": "f1"}],
            )
        ]
        if not testing.db.dialect.insert_returning:
            # DBs without implicit returning, we must immediately
            # SELECT for the new version id
            statements.append(
                CompiledSQL(
                    "SELECT version_table.version_id "
                    "AS version_table_version_id "
                    "FROM version_table WHERE version_table.id = :pk_1",
                    lambda ctx: [{"pk_1": 1}],
                )
            )
        self.assert_sql_execution(testing.db, sess.flush, *statements)

    def test_update_col(self):
        self._test_update_col()

    def test_update_col_eager_defaults(self):
        self._test_update_col(eager_defaults=True)

    def _test_update_col(self, **kw):
        sess = self._fixture(**kw)

        f1 = self.classes.Foo(value="f1")
        sess.add(f1)
        sess.flush()

        f1.value = "f2"

        if testing.db.dialect.update_returning:
            statements = [
                CompiledSQL(
                    "UPDATE version_table SET version_id=2, value=:value "
                    "WHERE version_table.id = :version_table_id AND "
                    "version_table.version_id = :version_table_version_id "
                    "RETURNING version_table.version_id",
                    lambda ctx: [
                        {
                            "version_table_id": 1,
                            "version_table_version_id": 1,
                            "value": "f2",
                        }
                    ],
                    enable_returning=True,
                )
            ]
        else:
            # DBs without implicit returning, we must immediately
            # SELECT for the new version id
            statements = [
                CompiledSQL(
                    "UPDATE version_table SET version_id=2, value=:value "
                    "WHERE version_table.id = :version_table_id AND "
                    "version_table.version_id = :version_table_version_id",
                    lambda ctx: [
                        {
                            "version_table_id": 1,
                            "version_table_version_id": 1,
                            "value": "f2",
                        }
                    ],
                    enable_returning=False,
                ),
                CompiledSQL(
                    "SELECT version_table.version_id "
                    "AS version_table_version_id "
                    "FROM version_table WHERE version_table.id = :pk_1",
                    lambda ctx: [{"pk_1": 1}],
                ),
            ]
        with conditional_sane_rowcount_warnings(
            update=True, only_returning=True
        ):
            self.assert_sql_execution(testing.db, sess.flush, *statements)

    @testing.requires.updateable_autoincrement_pks
    def test_sql_expr_bump(self):
        sess = self._fixture()

        f1 = self.classes.Foo(value="f1")
        sess.add(f1)
        sess.flush()

        eq_(f1.version_id, 1)

        f1.id = self.classes.Foo.id + 0

        with conditional_sane_rowcount_warnings(
            update=True, only_returning=True
        ):
            sess.flush()

        eq_(f1.version_id, 2)

    @testing.requires.updateable_autoincrement_pks
    @testing.requires.update_returning
    def test_sql_expr_w_mods_bump(self):
        sess = self._fixture()

        f1 = self.classes.Foo(id=2, value="f1")
        sess.add(f1)
        sess.flush()

        eq_(f1.version_id, 1)

        f1.id = self.classes.Foo.id + 3

        with conditional_sane_rowcount_warnings(update=True):
            sess.flush()

        eq_(f1.id, 5)
        eq_(f1.version_id, 2)

    def test_multi_update(self):
        sess = self._fixture()

        f1 = self.classes.Foo(value="f1")
        f2 = self.classes.Foo(value="f2")
        f3 = self.classes.Foo(value="f3")
        sess.add_all([f1, f2, f3])
        sess.flush()

        f1.value = "f1a"
        f2.value = "f2a"
        f3.value = "f3a"

        if testing.db.dialect.update_returning:
            statements = [
                CompiledSQL(
                    "UPDATE version_table SET version_id=2, value=:value "
                    "WHERE version_table.id = :version_table_id AND "
                    "version_table.version_id = :version_table_version_id "
                    "RETURNING version_table.version_id",
                    lambda ctx: [
                        {
                            "version_table_id": 1,
                            "version_table_version_id": 1,
                            "value": "f1a",
                        }
                    ],
                    enable_returning=True,
                ),
                CompiledSQL(
                    "UPDATE version_table SET version_id=2, value=:value "
                    "WHERE version_table.id = :version_table_id AND "
                    "version_table.version_id = :version_table_version_id "
                    "RETURNING version_table.version_id",
                    lambda ctx: [
                        {
                            "version_table_id": 2,
                            "version_table_version_id": 1,
                            "value": "f2a",
                        }
                    ],
                    enable_returning=True,
                ),
                CompiledSQL(
                    "UPDATE version_table SET version_id=2, value=:value "
                    "WHERE version_table.id = :version_table_id AND "
                    "version_table.version_id = :version_table_version_id "
                    "RETURNING version_table.version_id",
                    lambda ctx: [
                        {
                            "version_table_id": 3,
                            "version_table_version_id": 1,
                            "value": "f3a",
                        }
                    ],
                    enable_returning=True,
                ),
            ]
        else:
            # DBs without update returning, we must immediately
            # SELECT for the new version id
            statements = [
                CompiledSQL(
                    "UPDATE version_table SET version_id=2, value=:value "
                    "WHERE version_table.id = :version_table_id AND "
                    "version_table.version_id = :version_table_version_id",
                    lambda ctx: [
                        {
                            "version_table_id": 1,
                            "version_table_version_id": 1,
                            "value": "f1a",
                        }
                    ],
                    enable_returning=False,
                ),
                CompiledSQL(
                    "UPDATE version_table SET version_id=2, value=:value "
                    "WHERE version_table.id = :version_table_id AND "
                    "version_table.version_id = :version_table_version_id",
                    lambda ctx: [
                        {
                            "version_table_id": 2,
                            "version_table_version_id": 1,
                            "value": "f2a",
                        }
                    ],
                    enable_returning=False,
                ),
                CompiledSQL(
                    "UPDATE version_table SET version_id=2, value=:value "
                    "WHERE version_table.id = :version_table_id AND "
                    "version_table.version_id = :version_table_version_id",
                    lambda ctx: [
                        {
                            "version_table_id": 3,
                            "version_table_version_id": 1,
                            "value": "f3a",
                        }
                    ],
                    enable_returning=False,
                ),
                CompiledSQL(
                    "SELECT version_table.version_id "
                    "AS version_table_version_id "
                    "FROM version_table WHERE version_table.id = :pk_1",
                    lambda ctx: [{"pk_1": 1}],
                ),
                CompiledSQL(
                    "SELECT version_table.version_id "
                    "AS version_table_version_id "
                    "FROM version_table WHERE version_table.id = :pk_1",
                    lambda ctx: [{"pk_1": 2}],
                ),
                CompiledSQL(
                    "SELECT version_table.version_id "
                    "AS version_table_version_id "
                    "FROM version_table WHERE version_table.id = :pk_1",
                    lambda ctx: [{"pk_1": 3}],
                ),
            ]

        with conditional_sane_rowcount_warnings(
            update=True, only_returning=True
        ):
            self.assert_sql_execution(testing.db, sess.flush, *statements)

    def test_delete_col(self):
        sess = self._fixture()

        f1 = self.classes.Foo(value="f1")
        sess.add(f1)
        sess.flush()

        sess.delete(f1)

        statements = [
            # note that the assertsql tests the rule against
            # "default" - on a "returning" backend, the statement
            # includes "RETURNING"
            CompiledSQL(
                "DELETE FROM version_table "
                "WHERE version_table.id = :id AND "
                "version_table.version_id = :version_id",
                lambda ctx: [{"id": 1, "version_id": 1}],
            )
        ]
        with conditional_sane_rowcount_warnings(delete=True):
            self.assert_sql_execution(testing.db, sess.flush, *statements)

    @testing.requires.independent_connections
    def test_concurrent_mod_err_expire_on_commit(self):
        sess = self._fixture()

        f1 = self.classes.Foo(value="f1")
        sess.add(f1)
        sess.commit()

        f1.value

        s2 = fixture_session()
        f2 = s2.query(self.classes.Foo).first()
        f2.value = "f2"
        s2.commit()

        f1.value = "f3"

        assert_raises_message(
            orm.exc.StaleDataError,
            r"UPDATE statement on table 'version_table' expected to "
            r"update 1 row\(s\); 0 were matched.",
            sess.commit,
        )

    @testing.requires.independent_connections
    def test_concurrent_mod_err_noexpire_on_commit(self):
        sess = self._fixture(expire_on_commit=False)

        f1 = self.classes.Foo(value="f1")
        sess.add(f1)
        sess.commit()

        # here, we're not expired overall, so no load occurs and we
        # stay without a version id, unless we've emitted
        # a SELECT for it within the flush.
        f1.value

        s2 = fixture_session(expire_on_commit=False)
        f2 = s2.query(self.classes.Foo).first()
        f2.value = "f2"
        s2.commit()

        f1.value = "f3"

        assert_raises_message(
            orm.exc.StaleDataError,
            r"UPDATE statement on table 'version_table' expected to "
            r"update 1 row\(s\); 0 were matched.",
            sess.commit,
        )


class ManualVersionTest(fixtures.MappedTest):
    run_define_tables = "each"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
            Column("vid", Integer),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        cls.mapper_registry.map_imperatively(
            cls.classes.A,
            cls.tables.a,
            version_id_col=cls.tables.a.c.vid,
            version_id_generator=False,
        )

    def test_insert(self):
        sess = fixture_session()
        a1 = self.classes.A()

        a1.vid = 1
        sess.add(a1)
        sess.commit()

        eq_(a1.vid, 1)

    def test_update(self):
        sess = fixture_session()
        a1 = self.classes.A()

        a1.vid = 1
        a1.data = "d1"
        sess.add(a1)
        sess.commit()

        a1.vid = 2
        a1.data = "d2"

        with conditional_sane_rowcount_warnings(update=True):
            sess.commit()

        eq_(a1.vid, 2)

    @testing.requires.sane_rowcount
    def test_update_concurrent_check(self):
        sess = fixture_session()
        a1 = self.classes.A()

        a1.vid = 1
        a1.data = "d1"
        sess.add(a1)
        sess.commit()

        a1.vid = 2
        sess.execute(self.tables.a.update().values(vid=3))
        a1.data = "d2"
        assert_raises(orm_exc.StaleDataError, sess.commit)

    def test_update_version_conditional(self):
        sess = fixture_session()
        a1 = self.classes.A()

        a1.vid = 1
        a1.data = "d1"
        sess.add(a1)
        sess.commit()

        # change the data and UPDATE without
        # incrementing version id
        a1.data = "d2"
        with conditional_sane_rowcount_warnings(update=True):
            sess.commit()

        eq_(a1.vid, 1)

        a1.data = "d3"
        a1.vid = 2
        with conditional_sane_rowcount_warnings(update=True):
            sess.commit()

        eq_(a1.vid, 2)


class ManualInheritanceVersionTest(fixtures.MappedTest):
    run_define_tables = "each"
    __backend__ = True
    __requires__ = ("sane_rowcount",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
            Column("vid", Integer, nullable=False),
        )

        Table(
            "b",
            metadata,
            Column("id", Integer, ForeignKey("a.id"), primary_key=True),
            Column("b_data", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(A):
            pass

    @classmethod
    def setup_mappers(cls):
        cls.mapper_registry.map_imperatively(
            cls.classes.A,
            cls.tables.a,
            version_id_col=cls.tables.a.c.vid,
            version_id_generator=False,
        )

        cls.mapper_registry.map_imperatively(
            cls.classes.B, cls.tables.b, inherits=cls.classes.A
        )

    def test_no_increment(self):
        sess = fixture_session()
        b1 = self.classes.B()

        b1.vid = 1
        b1.data = "d1"
        sess.add(b1)
        sess.commit()

        # change col on subtable only without
        # incrementing version id
        b1.b_data = "bd2"
        with conditional_sane_rowcount_warnings(update=True):
            sess.commit()

        eq_(b1.vid, 1)

        b1.b_data = "d3"
        b1.vid = 2
        with conditional_sane_rowcount_warnings(update=True):
            sess.commit()

        eq_(b1.vid, 2)


class VersioningMappedSelectTest(fixtures.MappedTest):
    # test for #4193, see also #4194 for related notes

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "version_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("version_id", Integer, nullable=False),
            Column("value", String(40), nullable=False),
        )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

    def _implicit_version_fixture(self):
        Foo, version_table = self.classes.Foo, self.tables.version_table

        current = (
            version_table.select()
            .where(version_table.c.id > 0)
            .alias("current_table")
        )

        self.mapper_registry.map_imperatively(
            Foo, current, version_id_col=version_table.c.version_id
        )
        s1 = fixture_session()
        return s1

    def _explicit_version_fixture(self):
        Foo, version_table = self.classes.Foo, self.tables.version_table

        current = (
            version_table.select()
            .where(version_table.c.id > 0)
            .alias("current_table")
        )

        self.mapper_registry.map_imperatively(
            Foo,
            current,
            version_id_col=version_table.c.version_id,
            version_id_generator=False,
        )
        s1 = fixture_session()
        return s1

    def test_implicit(self):
        Foo = self.classes.Foo

        s1 = self._implicit_version_fixture()
        f1 = Foo(value="f1")
        f2 = Foo(value="f2")
        s1.add_all((f1, f2))
        s1.commit()

        f1.value = "f1rev2"
        f2.value = "f2rev2"
        with conditional_sane_rowcount_warnings(update=True):
            s1.commit()

        eq_(
            s1.query(Foo.id, Foo.value, Foo.version_id).order_by(Foo.id).all(),
            [(f1.id, "f1rev2", 2), (f2.id, "f2rev2", 2)],
        )

    def test_explicit(self):
        Foo = self.classes.Foo

        s1 = self._explicit_version_fixture()
        f1 = Foo(value="f1", version_id=1)
        f2 = Foo(value="f2", version_id=1)
        s1.add_all((f1, f2))
        s1.flush()

        # note this requires that the Session was not expired until
        # we fix #4195
        f1.value = "f1rev2"
        f1.version_id = 2
        f2.value = "f2rev2"
        f2.version_id = 2
        with conditional_sane_rowcount_warnings(update=True):
            s1.flush()

        eq_(
            s1.query(Foo.id, Foo.value, Foo.version_id).order_by(Foo.id).all(),
            [(f1.id, "f1rev2", 2), (f2.id, "f2rev2", 2)],
        )

    def test_implicit_no_readonly(self):
        # test issue 4194
        Foo = self.classes.Foo

        s1 = self._implicit_version_fixture()
        f1 = Foo(value="f1")
        s1.add(f1)
        s1.flush()

        is_false(bool(inspect(Foo)._readonly_props))

        def go():
            eq_(f1.version_id, 1)

        self.assert_sql_count(testing.db, go, 0)

    def test_explicit_assign_from_expired(self):
        # test issue 4195
        Foo = self.classes.Foo

        s1 = self._explicit_version_fixture()

        configure_mappers()
        is_true(Foo.version_id.impl.active_history)

        f1 = Foo(value="f1", version_id=1)
        s1.add(f1)

        s1.flush()

        s1.expire_all()

        with conditional_sane_rowcount_warnings(update=True):
            f1.value = "f2"
            f1.version_id = 2
            s1.flush()


class QuotedBindVersioningTest(fixtures.MappedTest):
    """test for #8056"""

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "version_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            # will need parameter quoting for Oracle and PostgreSQL
            # dont use 'key' to make sure no the awkward name is definitely
            # in the params
            Column("_version%id", Integer, nullable=False),
            Column("value", String(40), nullable=False),
        )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Foo = cls.classes.Foo
        vt = cls.tables.version_table
        cls.mapper_registry.map_imperatively(
            Foo,
            vt,
            version_id_col=vt.c["_version%id"],
            properties={"version": vt.c["_version%id"]},
        )

    def test_round_trip(self, fixture_session):
        Foo = self.classes.Foo

        f1 = Foo(value="v1")
        fixture_session.add(f1)
        fixture_session.commit()

        f1.value = "v2"
        with conditional_sane_rowcount_warnings(update=True):
            fixture_session.commit()

        eq_(f1.version, 2)


class PostUpdateVersioningTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class User(Base):
            __tablename__ = "user"

            id = Column(Integer, primary_key=True)

        class Parent(Base):
            __tablename__ = "parent"

            id = Column(Integer, primary_key=True)
            version_id = Column(Integer)
            updated_by_id = Column(
                Integer,
                ForeignKey("user.id"),
            )

            updated_by = relationship(
                "User",
                foreign_keys=[updated_by_id],
                post_update=True,
            )

            __mapper_args__ = {
                "version_id_col": version_id,
            }

    def test_bumped_version_id_on_update(self):
        """test for #10800"""
        User, Parent = self.classes("User", "Parent")

        session = fixture_session()
        u1 = User(id=1)
        u2 = User(id=2)
        p1 = Parent(id=1, updated_by=u1)
        session.add(u1)
        session.add(u2)
        session.add(p1)

        u2id = u2.id
        session.commit()
        session.close()

        p1 = session.get(Parent, 1)
        p1.updated_by
        p1.version_id = p1.version_id
        p1.updated_by_id = u2id
        assert "version_id" in inspect(p1).committed_state

        with self.sql_execution_asserter(testing.db) as asserter:
            session.commit()

        asserter.assert_(
            CompiledSQL(
                "UPDATE parent SET version_id=:version_id, "
                "updated_by_id=:updated_by_id WHERE parent.id = :parent_id "
                "AND parent.version_id = :parent_version_id",
                [
                    {
                        "version_id": 2,
                        "updated_by_id": 2,
                        "parent_id": 1,
                        "parent_version_id": 1,
                    }
                ],
            ),
            CompiledSQL(
                "UPDATE parent SET version_id=:version_id, "
                "updated_by_id=:updated_by_id WHERE parent.id = :parent_id "
                "AND parent.version_id = :parent_version_id",
                [
                    {
                        "version_id": 3,
                        "updated_by_id": 2,
                        "parent_id": 1,
                        "parent_version_id": 2,
                    }
                ],
            ),
        )

    def test_bumped_version_id_on_delete(self):
        """test for #10967"""

        User, Parent = self.classes("User", "Parent")

        session = fixture_session()
        u1 = User(id=1)
        p1 = Parent(id=1, updated_by=u1)
        session.add(u1)
        session.add(p1)

        session.flush()

        session.delete(p1)

        with self.sql_execution_asserter(testing.db) as asserter:
            session.commit()

        asserter.assert_(
            CompiledSQL(
                "UPDATE parent SET version_id=:version_id, "
                "updated_by_id=:updated_by_id WHERE parent.id = :parent_id "
                "AND parent.version_id = :parent_version_id",
                [
                    {
                        "version_id": 2,
                        "updated_by_id": None,
                        "parent_id": 1,
                        "parent_version_id": 1,
                    }
                ],
            ),
            CompiledSQL(
                "DELETE FROM parent WHERE parent.id = :id AND "
                "parent.version_id = :version_id",
                [{"id": 1, "version_id": 2}],
            ),
        )
