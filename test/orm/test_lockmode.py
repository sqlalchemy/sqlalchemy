from sqlalchemy import exc
from sqlalchemy import testing
from sqlalchemy.engine import default
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing.fixtures import fixture_session
from test.orm import _fixtures


class ForUpdateTest(_fixtures.FixtureTest):
    @classmethod
    def setup_mappers(cls):
        User, users = cls.classes.User, cls.tables.users
        mapper(User, users)

    def _assert(
        self,
        read=False,
        nowait=False,
        of=None,
        key_share=None,
        assert_q_of=None,
        assert_sel_of=None,
    ):
        User = self.classes.User
        s = fixture_session()
        q = s.query(User).with_for_update(
            read=read, nowait=nowait, of=of, key_share=key_share
        )
        sel = q._compile_state().statement

        assert q._for_update_arg.read is read
        assert sel._for_update_arg.read is read

        assert q._for_update_arg.nowait is nowait
        assert sel._for_update_arg.nowait is nowait

        assert q._for_update_arg.key_share is key_share
        assert sel._for_update_arg.key_share is key_share

        eq_(q._for_update_arg.of, assert_q_of)
        eq_(sel._for_update_arg.of, assert_sel_of)

    def test_key_share(self):
        self._assert(key_share=True)

    def test_read(self):
        self._assert(read=True)

    def test_plain(self):
        self._assert()

    def test_nowait(self):
        self._assert(nowait=True)

    def test_of_single_col(self):
        User, users = self.classes.User, self.tables.users
        self._assert(
            of=User.id, assert_q_of=[users.c.id], assert_sel_of=[users.c.id]
        )


class BackendTest(_fixtures.FixtureTest):
    __backend__ = True

    # test against the major backends.   We are naming specific databases
    # here rather than using requirements rules since the behavior of
    # "FOR UPDATE" as well as "OF" is very specific to each DB, and we need
    # to run the query differently based on backend.

    @classmethod
    def setup_mappers(cls):
        User, users = cls.classes.User, cls.tables.users
        Address, addresses = cls.classes.Address, cls.tables.addresses
        mapper(User, users, properties={"addresses": relationship(Address)})
        mapper(Address, addresses)

    def test_inner_joinedload_w_limit(self):
        User = self.classes.User
        sess = fixture_session()
        q = (
            sess.query(User)
            .options(joinedload(User.addresses, innerjoin=True))
            .with_for_update()
            .limit(1)
        )

        if testing.against("oracle"):
            assert_raises_message(exc.DatabaseError, "ORA-02014", q.all)
        else:
            q.all()
        sess.close()

    def test_inner_joinedload_wo_limit(self):
        User = self.classes.User
        sess = fixture_session()
        sess.query(User).options(
            joinedload(User.addresses, innerjoin=True)
        ).with_for_update().all()
        sess.close()

    def test_outer_joinedload_w_limit(self):
        User = self.classes.User
        sess = fixture_session()
        q = sess.query(User).options(
            joinedload(User.addresses, innerjoin=False)
        )

        if testing.against("postgresql"):
            q = q.with_for_update(of=User)
        else:
            q = q.with_for_update()

        q = q.limit(1)

        if testing.against("oracle"):
            assert_raises_message(exc.DatabaseError, "ORA-02014", q.all)
        else:
            q.all()
        sess.close()

    def test_outer_joinedload_wo_limit(self):
        User = self.classes.User
        sess = fixture_session()
        q = sess.query(User).options(
            joinedload(User.addresses, innerjoin=False)
        )

        if testing.against("postgresql"):
            q = q.with_for_update(of=User)
        else:
            q = q.with_for_update()

        q.all()
        sess.close()

    def test_join_w_subquery(self):
        User = self.classes.User
        Address = self.classes.Address
        sess = fixture_session()
        q1 = sess.query(User).with_for_update().subquery()
        sess.query(q1).join(Address).all()
        sess.close()

    def test_plain(self):
        User = self.classes.User
        sess = fixture_session()
        sess.query(User).with_for_update().all()
        sess.close()


class CompileTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    """run some compile tests, even though these are redundant."""

    run_inserts = None

    @classmethod
    def setup_mappers(cls):
        User, users = cls.classes.User, cls.tables.users
        Address, addresses = cls.classes.Address, cls.tables.addresses
        mapper(User, users, properties={"addresses": relationship(Address)})
        mapper(Address, addresses)

    def test_default_update(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(),
            "SELECT users.id AS users_id FROM users FOR UPDATE",
            dialect=default.DefaultDialect(),
        )

    def test_not_supported_by_dialect_should_just_use_update(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(read=True),
            "SELECT users.id AS users_id FROM users FOR UPDATE",
            dialect=default.DefaultDialect(),
        )

    def test_postgres_read(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(read=True),
            "SELECT users.id AS users_id FROM users FOR SHARE",
            dialect="postgresql",
        )

    def test_postgres_read_nowait(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(read=True, nowait=True),
            "SELECT users.id AS users_id FROM users FOR SHARE NOWAIT",
            dialect="postgresql",
        )

    def test_postgres_update(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(),
            "SELECT users.id AS users_id FROM users FOR UPDATE",
            dialect="postgresql",
        )

    def test_postgres_update_of(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(of=User.id),
            "SELECT users.id AS users_id FROM users FOR UPDATE OF users",
            dialect="postgresql",
        )

    def test_postgres_update_of_entity(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(of=User),
            "SELECT users.id AS users_id FROM users FOR UPDATE OF users",
            dialect="postgresql",
        )

    def test_postgres_update_of_entity_list(self):
        User = self.classes.User
        Address = self.classes.Address

        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id, Address.id).with_for_update(
                of=[User, Address]
            ),
            "SELECT users.id AS users_id, addresses.id AS addresses_id "
            "FROM users, addresses FOR UPDATE OF users, addresses",
            dialect="postgresql",
        )

    def test_postgres_for_no_key_update(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(key_share=True),
            "SELECT users.id AS users_id FROM users FOR NO KEY UPDATE",
            dialect="postgresql",
        )

    def test_postgres_for_no_key_nowait_update(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(key_share=True, nowait=True),
            "SELECT users.id AS users_id FROM users FOR NO KEY UPDATE NOWAIT",
            dialect="postgresql",
        )

    def test_postgres_update_of_list(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(
                of=[User.id, User.id, User.id]
            ),
            "SELECT users.id AS users_id FROM users FOR UPDATE OF users",
            dialect="postgresql",
        )

    def test_postgres_update_skip_locked(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(skip_locked=True),
            "SELECT users.id AS users_id FROM users FOR UPDATE SKIP LOCKED",
            dialect="postgresql",
        )

    def test_oracle_update(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(),
            "SELECT users.id AS users_id FROM users FOR UPDATE",
            dialect="oracle",
        )

    def test_oracle_update_skip_locked(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(skip_locked=True),
            "SELECT users.id AS users_id FROM users FOR UPDATE SKIP LOCKED",
            dialect="oracle",
        )

    def test_mysql_read(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User.id).with_for_update(read=True),
            "SELECT users.id AS users_id FROM users LOCK IN SHARE MODE",
            dialect="mysql",
        )

    def test_for_update_on_inner_w_joinedload(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User)
            .options(joinedload(User.addresses))
            .with_for_update()
            .limit(1),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name "
            "AS anon_1_users_name, addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM (SELECT users.id AS users_id, users.name AS users_name "
            "FROM users  LIMIT %s FOR UPDATE) AS anon_1 "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id FOR UPDATE",
            dialect="mysql",
        )

    def test_for_update_on_inner_w_joinedload_no_render_oracle(self):
        User = self.classes.User
        sess = fixture_session()
        self.assert_compile(
            sess.query(User)
            .options(joinedload(User.addresses))
            .with_for_update()
            .limit(1),
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM (SELECT anon_2.users_id AS users_id, "
            "anon_2.users_name AS users_name FROM "
            "(SELECT users.id AS users_id, users.name AS users_name "
            "FROM users) anon_2 WHERE ROWNUM <= [POSTCOMPILE_param_1]) anon_1 "
            "LEFT OUTER JOIN addresses addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id FOR UPDATE",
            dialect="oracle",
        )
