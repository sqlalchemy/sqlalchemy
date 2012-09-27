from sqlalchemy.engine import default
from sqlalchemy.databases import *
from sqlalchemy.orm import mapper
from sqlalchemy.orm import Session
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import assert_raises_message
from test.orm import _fixtures


class LockModeTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        User, users = cls.classes.User, cls.tables.users
        mapper(User, users)

    def test_default_update(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_lockmode('update'),
            "SELECT users.id AS users_id FROM users FOR UPDATE",
            dialect=default.DefaultDialect()
        )

    def test_not_supported_by_dialect_should_just_use_update(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_lockmode('read'),
            "SELECT users.id AS users_id FROM users FOR UPDATE",
            dialect=default.DefaultDialect()
        )

    def test_none_lock_mode(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_lockmode(None),
            "SELECT users.id AS users_id FROM users",
            dialect=default.DefaultDialect()
        )

    def test_unknown_lock_mode(self):
        User = self.classes.User
        sess = Session()
        assert_raises_message(
            Exception, "Unknown lockmode 'unknown_mode'",
            self.assert_compile,
            sess.query(User.id).with_lockmode('unknown_mode'), None,
            dialect=default.DefaultDialect()
        )

    def test_postgres_read(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_lockmode('read'),
            "SELECT users.id AS users_id FROM users FOR SHARE",
            dialect=postgresql.dialect()
        )

    def test_postgres_read_nowait(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_lockmode('read_nowait'),
            "SELECT users.id AS users_id FROM users FOR SHARE NOWAIT",
            dialect=postgresql.dialect()
        )

    def test_postgres_update(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_lockmode('update'),
            "SELECT users.id AS users_id FROM users FOR UPDATE",
            dialect=postgresql.dialect()
        )

    def test_postgres_update_nowait(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_lockmode('update_nowait'),
            "SELECT users.id AS users_id FROM users FOR UPDATE NOWAIT",
            dialect=postgresql.dialect()
        )

    def test_oracle_update(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_lockmode('update'),
            "SELECT users.id AS users_id FROM users FOR UPDATE",
            dialect=oracle.dialect()
        )

    def test_oracle_update_nowait(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_lockmode('update_nowait'),
            "SELECT users.id AS users_id FROM users FOR UPDATE NOWAIT",
            dialect=oracle.dialect()
        )

    def test_mysql_read(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_lockmode('read'),
            "SELECT users.id AS users_id FROM users LOCK IN SHARE MODE",
            dialect=mysql.dialect()
        )

    def test_mysql_update(self):
        User = self.classes.User
        sess = Session()
        self.assert_compile(sess.query(User.id).with_lockmode('update'),
            "SELECT users.id AS users_id FROM users FOR UPDATE",
            dialect=mysql.dialect()
        )
