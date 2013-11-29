from sqlalchemy.engine import default
from sqlalchemy.databases import *
from sqlalchemy.orm import mapper
from sqlalchemy.orm import Session
from sqlalchemy.testing import AssertsCompiledSQL, eq_
from sqlalchemy.testing import assert_raises_message
from sqlalchemy import exc
from test.orm import _fixtures


class LegacyLockModeTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        User, users = cls.classes.User, cls.tables.users
        mapper(User, users)

    def _assert_legacy(self, arg, read=False, nowait=False):
        User = self.classes.User
        s = Session()
        q = s.query(User).with_lockmode(arg)
        sel = q._compile_context().statement

        if arg is None:
            assert q._for_update_arg is None
            assert sel._for_update_arg is None
            return

        assert q._for_update_arg.read is read
        assert q._for_update_arg.nowait is nowait

        assert sel._for_update_arg.read is read
        assert sel._for_update_arg.nowait is nowait

    def test_false_legacy(self):
        self._assert_legacy(None)

    def test_plain_legacy(self):
        self._assert_legacy("update")

    def test_nowait_legacy(self):
        self._assert_legacy("update_nowait", nowait=True)

    def test_read_legacy(self):
        self._assert_legacy("read", read=True)

    def test_unknown_legacy_lock_mode(self):
        User = self.classes.User
        sess = Session()
        assert_raises_message(
            exc.ArgumentError, "Unknown with_lockmode argument: 'unknown_mode'",
            sess.query(User.id).with_lockmode, 'unknown_mode'
        )

class ForUpdateTest(_fixtures.FixtureTest):
    @classmethod
    def setup_mappers(cls):
        User, users = cls.classes.User, cls.tables.users
        mapper(User, users)

    def _assert(self, read=False, nowait=False, of=None,
                    assert_q_of=None, assert_sel_of=None):
        User = self.classes.User
        s = Session()
        q = s.query(User).with_for_update(read=read, nowait=nowait, of=of)
        sel = q._compile_context().statement

        assert q._for_update_arg.read is read
        assert sel._for_update_arg.read is read

        assert q._for_update_arg.nowait is nowait
        assert sel._for_update_arg.nowait is nowait

        eq_(q._for_update_arg.of, assert_q_of)
        eq_(sel._for_update_arg.of, assert_sel_of)

    def test_read(self):
        self._assert(read=True)

    def test_plain(self):
        self._assert()

    def test_nowait(self):
        self._assert(nowait=True)

    def test_of_single_col(self):
        User, users = self.classes.User, self.tables.users
        self._assert(
            of=User.id,
            assert_q_of=[users.c.id],
            assert_sel_of=[users.c.id]
        )

class CompileTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    """run some compile tests, even though these are redundant."""
    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        User, users = cls.classes.User, cls.tables.users
        Address, addresses = cls.classes.Address, cls.tables.addresses
        mapper(User, users)
        mapper(Address, addresses)

    def test_default_update(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_for_update(),
            "SELECT users.id AS users_id FROM users FOR UPDATE",
            dialect=default.DefaultDialect()
        )

    def test_not_supported_by_dialect_should_just_use_update(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_for_update(read=True),
            "SELECT users.id AS users_id FROM users FOR UPDATE",
            dialect=default.DefaultDialect()
        )

    def test_postgres_read(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_for_update(read=True),
            "SELECT users.id AS users_id FROM users FOR SHARE",
            dialect="postgresql"
        )

    def test_postgres_read_nowait(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).
                with_for_update(read=True, nowait=True),
            "SELECT users.id AS users_id FROM users FOR SHARE NOWAIT",
            dialect="postgresql"
        )

    def test_postgres_update(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_for_update(),
            "SELECT users.id AS users_id FROM users FOR UPDATE",
            dialect="postgresql"
        )

    def test_postgres_update_of(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_for_update(of=User.id),
            "SELECT users.id AS users_id FROM users FOR UPDATE OF users",
            dialect="postgresql"
        )

    def test_postgres_update_of_entity(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_for_update(of=User),
            "SELECT users.id AS users_id FROM users FOR UPDATE OF users",
            dialect="postgresql"
        )

    def test_postgres_update_of_entity_list(self):
        User = self.classes.User
        Address = self.classes.Address

        sess = Session()
        self.assert_compile(sess.query(User.id, Address.id).
                with_for_update(of=[User, Address]),
            "SELECT users.id AS users_id, addresses.id AS addresses_id "
            "FROM users, addresses FOR UPDATE OF users, addresses",
            dialect="postgresql"
        )

    def test_postgres_update_of_list(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).
                with_for_update(of=[User.id, User.id, User.id]),
            "SELECT users.id AS users_id FROM users FOR UPDATE OF users",
            dialect="postgresql"
        )


    def test_oracle_update(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_for_update(),
            "SELECT users.id AS users_id FROM users FOR UPDATE",
            dialect="oracle"
        )

    def test_mysql_read(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_for_update(read=True),
            "SELECT users.id AS users_id FROM users LOCK IN SHARE MODE",
            dialect="mysql"
        )
