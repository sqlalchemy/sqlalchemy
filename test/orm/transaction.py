import testenv; testenv.configure_for_tests()
import operator
from sqlalchemy import *
from sqlalchemy import exc as sa_exc
from sqlalchemy.orm import *
from testlib import *
from testlib.fixtures import *


class TransactionTest(FixtureTest):
    keep_mappers = True
    session = sessionmaker()

    def setup_mappers(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user',
                                 cascade="all, delete-orphan"),
            })
        mapper(Address, addresses)


class FixtureDataTest(TransactionTest):
    refresh_data = True

    def test_attrs_on_rollback(self):
        sess = self.session()
        u1 = sess.query(User).get(7)
        u1.name = 'ed'
        sess.rollback()
        self.assertEquals(u1.name, 'jack')

    def test_commit_persistent(self):
        sess = self.session()
        u1 = sess.query(User).get(7)
        u1.name = 'ed'
        sess.flush()
        sess.commit()
        self.assertEquals(u1.name, 'ed')

    def test_concurrent_commit_persistent(self):
        s1 = self.session()
        u1 = s1.query(User).get(7)
        u1.name = 'ed'
        s1.commit()

        s2 = self.session()
        u2 = s2.query(User).get(7)
        assert u2.name == 'ed'
        u2.name = 'will'
        s2.commit()

        assert u1.name == 'will'

class AutoExpireTest(TransactionTest):
    tables_only = True

    def test_expunge_pending_on_rollback(self):
        sess = self.session()
        u2= User(name='newuser')
        sess.add(u2)
        assert u2 in sess
        sess.rollback()
        assert u2 not in sess

    def test_trans_pending_cleared_on_commit(self):
        sess = self.session()
        u2= User(name='newuser')
        sess.add(u2)
        assert u2 in sess
        sess.commit()
        assert u2 in sess
        u3 = User(name='anotheruser')
        sess.add(u3)
        sess.rollback()
        assert u3 not in sess
        assert u2 in sess

    def test_update_deleted_on_rollback(self):
        s = self.session()
        u1 = User(name='ed')
        s.add(u1)
        s.commit()

        s.delete(u1)
        assert u1 in s.deleted
        s.rollback()
        assert u1 in s
        assert u1 not in s.deleted

    def test_trans_deleted_cleared_on_rollback(self):
        s = self.session()
        u1 = User(name='ed')
        s.add(u1)
        s.commit()

        s.delete(u1)
        s.commit()
        assert u1 not in s
        s.rollback()
        assert u1 not in s

    def test_update_deleted_on_rollback_cascade(self):
        s = self.session()
        u1 = User(name='ed', addresses=[Address(email_address='foo')])
        s.add(u1)
        s.commit()

        s.delete(u1)
        assert u1 in s.deleted
        assert u1.addresses[0] in s.deleted
        s.rollback()
        assert u1 in s
        assert u1 not in s.deleted
        assert u1.addresses[0] not in s.deleted

    def test_update_deleted_on_rollback_orphan(self):
        s = self.session()
        u1 = User(name='ed', addresses=[Address(email_address='foo')])
        s.add(u1)
        s.commit()

        a1 = u1.addresses[0]
        u1.addresses.remove(a1)

        s.flush()
        self.assertEquals(s.query(Address).filter(Address.email_address=='foo').all(), [])
        s.rollback()
        assert a1 not in s.deleted
        assert u1.addresses == [a1]

    def test_commit_pending(self):
        sess = self.session()
        u1 = User(name='newuser')
        sess.add(u1)
        sess.flush()
        sess.commit()
        self.assertEquals(u1.name, 'newuser')


    def test_concurrent_commit_pending(self):
        s1 = self.session()
        u1 = User(name='edward')
        s1.add(u1)
        s1.commit()

        s2 = self.session()
        u2 = s2.query(User).filter(User.name=='edward').one()
        u2.name = 'will'
        s2.commit()

        assert u1.name == 'will'

class RollbackRecoverTest(TransactionTest):
    only_tables = True

    def test_pk_violation(self):
        s = self.session()
        a1 = Address(email_address='foo')
        u1 = User(id=1, name='ed', addresses=[a1])
        s.add(u1)
        s.commit()

        a2 = Address(email_address='bar')
        u2 = User(id=1, name='jack', addresses=[a2])

        u1.name = 'edward'
        a1.email_address = 'foober'
        s.add(u2)
        self.assertRaises(sa_exc.FlushError, s.commit)
        self.assertRaises(sa_exc.InvalidRequestError, s.commit)
        s.rollback()
        assert u2 not in s
        assert a2 not in s
        assert u1 in s
        assert a1 in s
        assert u1.name == 'ed'
        assert a1.email_address == 'foo'
        u1.name = 'edward'
        a1.email_address = 'foober'
        s.commit()
        assert s.query(User).all() == [User(id=1, name='edward', addresses=[Address(email_address='foober')])]

    @testing.requires.savepoints
    def test_pk_violation_with_savepoint(self):
        s = self.session()
        a1 = Address(email_address='foo')
        u1 = User(id=1, name='ed', addresses=[a1])
        s.add(u1)
        s.commit()

        a2 = Address(email_address='bar')
        u2 = User(id=1, name='jack', addresses=[a2])

        u1.name = 'edward'
        a1.email_address = 'foober'
        s.begin_nested()
        s.add(u2)
        self.assertRaises(sa_exc.FlushError, s.commit)
        self.assertRaises(sa_exc.InvalidRequestError, s.commit)
        s.rollback()
        assert u2 not in s
        assert a2 not in s
        assert u1 in s
        assert a1 in s

        s.commit()
        assert s.query(User).all() == [User(id=1, name='edward', addresses=[Address(email_address='foober')])]


class SavepointTest(TransactionTest):

    only_tables = True

    @testing.requires.savepoints
    def test_savepoint_rollback(self):
        s = self.session()
        u1 = User(name='ed')
        u2 = User(name='jack')
        s.add_all([u1, u2])

        s.begin_nested()
        u3 = User(name='wendy')
        u4 = User(name='foo')
        u1.name = 'edward'
        u2.name = 'jackward'
        s.add_all([u3, u4])
        self.assertEquals(s.query(User.name).order_by(User.id).all(), [('edward',), ('jackward',), ('wendy',), ('foo',)])
        s.rollback()
        assert u1.name == 'ed'
        assert u2.name == 'jack'
        self.assertEquals(s.query(User.name).order_by(User.id).all(), [('ed',), ('jack',)])
        s.commit()
        assert u1.name == 'ed'
        assert u2.name == 'jack'
        self.assertEquals(s.query(User.name).order_by(User.id).all(), [('ed',), ('jack',)])

    @testing.requires.savepoints
    def test_savepoint_commit(self):
        s = self.session()
        u1 = User(name='ed')
        u2 = User(name='jack')
        s.add_all([u1, u2])

        s.begin_nested()
        u3 = User(name='wendy')
        u4 = User(name='foo')
        u1.name = 'edward'
        u2.name = 'jackward'
        s.add_all([u3, u4])
        self.assertEquals(s.query(User.name).order_by(User.id).all(), [('edward',), ('jackward',), ('wendy',), ('foo',)])
        s.commit()
        def go():
            assert u1.name == 'edward'
            assert u2.name == 'jackward'
            self.assertEquals(s.query(User.name).order_by(User.id).all(), [('edward',), ('jackward',), ('wendy',), ('foo',)])
        self.assert_sql_count(testing.db, go, 1)

        s.commit()
        self.assertEquals(s.query(User.name).order_by(User.id).all(), [('edward',), ('jackward',), ('wendy',), ('foo',)])

    @testing.requires.savepoints
    def test_savepoint_rollback_collections(self):
        s = self.session()
        u1 = User(name='ed', addresses=[Address(email_address='foo')])
        s.add(u1)
        s.commit()

        u1.name='edward'
        u1.addresses.append(Address(email_address='bar'))
        s.begin_nested()
        u2 = User(name='jack', addresses=[Address(email_address='bat')])
        s.add(u2)
        self.assertEquals(s.query(User).order_by(User.id).all(),
            [
                User(name='edward', addresses=[Address(email_address='foo'), Address(email_address='bar')]),
                User(name='jack', addresses=[Address(email_address='bat')])
            ]
        )
        s.rollback()
        self.assertEquals(s.query(User).order_by(User.id).all(),
            [
                User(name='edward', addresses=[Address(email_address='foo'), Address(email_address='bar')]),
            ]
        )
        s.commit()
        self.assertEquals(s.query(User).order_by(User.id).all(),
            [
                User(name='edward', addresses=[Address(email_address='foo'), Address(email_address='bar')]),
            ]
        )

    @testing.requires.savepoints
    def test_savepoint_commit_collections(self):
        s = self.session()
        u1 = User(name='ed', addresses=[Address(email_address='foo')])
        s.add(u1)
        s.commit()

        u1.name='edward'
        u1.addresses.append(Address(email_address='bar'))
        s.begin_nested()
        u2 = User(name='jack', addresses=[Address(email_address='bat')])
        s.add(u2)
        self.assertEquals(s.query(User).order_by(User.id).all(),
            [
                User(name='edward', addresses=[Address(email_address='foo'), Address(email_address='bar')]),
                User(name='jack', addresses=[Address(email_address='bat')])
            ]
        )
        s.commit()
        self.assertEquals(s.query(User).order_by(User.id).all(),
            [
                User(name='edward', addresses=[Address(email_address='foo'), Address(email_address='bar')]),
                User(name='jack', addresses=[Address(email_address='bat')])
            ]
        )
        s.commit()
        self.assertEquals(s.query(User).order_by(User.id).all(),
            [
                User(name='edward', addresses=[Address(email_address='foo'), Address(email_address='bar')]),
                User(name='jack', addresses=[Address(email_address='bat')])
            ]
        )

    @testing.requires.savepoints
    def test_expunge_pending_on_rollback(self):
        sess = self.session()

        sess.begin_nested()
        u2= User(name='newuser')
        sess.add(u2)
        assert u2 in sess
        sess.rollback()
        assert u2 not in sess

    @testing.requires.savepoints
    def test_update_deleted_on_rollback(self):
        s = self.session()
        u1 = User(name='ed')
        s.add(u1)
        s.commit()

        s.begin_nested()
        s.delete(u1)
        assert u1 in s.deleted
        s.rollback()
        assert u1 in s
        assert u1 not in s.deleted



class AutocommitTest(TransactionTest):
    def test_begin_nested_requires_trans(self):
        sess = create_session(autocommit=True)
        self.assertRaises(sa_exc.InvalidRequestError, sess.begin_nested)

    def test_begin_preflush(self):
        sess = create_session(autocommit=True)

        u1 = User(name='ed')
        sess.add(u1)
        
        sess.begin()
        u2 = User(name='some other user')
        sess.add(u2)
        sess.rollback()
        assert u2 not in sess
        assert u1 in sess
        assert sess.query(User).filter_by(name='ed').one() is u1
        
        


if __name__ == '__main__':
    testenv.main()
