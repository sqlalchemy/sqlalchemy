from . import _fixtures
from sqlalchemy.orm import loading, Session, aliased
from sqlalchemy.testing.assertions import eq_, \
    assert_raises, assert_raises_message
from sqlalchemy.util import KeyedTuple
from sqlalchemy.testing import mock
from sqlalchemy import select
from sqlalchemy import exc
# class GetFromIdentityTest(_fixtures.FixtureTest):
# class LoadOnIdentTest(_fixtures.FixtureTest):
# class InstanceProcessorTest(_fixture.FixtureTest):


class InstancesTest(_fixtures.FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_cursor_close_w_failed_rowproc(self):
        User = self.classes.User
        s = Session()

        q = s.query(User)

        ctx = q._compile_context()
        cursor = mock.Mock()
        q._entities = [
            mock.Mock(row_processor=mock.Mock(side_effect=Exception("boom")))
        ]
        assert_raises(
            Exception,
            list, loading.instances(q, cursor, ctx)
        )
        assert cursor.close.called, "Cursor wasn't closed"

    def test_row_proc_not_created(self):
        User = self.classes.User
        s = Session()

        q = s.query(User.id, User.name)
        stmt = select([User.id])

        assert_raises_message(
            exc.NoSuchColumnError,
            "Could not locate column in row for column 'users.name'",
            q.from_statement(stmt).all
        )


class MergeResultTest(_fixtures.FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def _fixture(self):
        User = self.classes.User

        s = Session()
        u1, u2, u3, u4 = User(id=1, name='u1'), User(id=2, name='u2'), \
                            User(id=7, name='u3'), User(id=8, name='u4')
        s.query(User).filter(User.id.in_([7, 8])).all()
        s.close()
        return s, [u1, u2, u3, u4]

    def test_single_entity(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        q = s.query(User)
        collection = [u1, u2, u3, u4]
        it = loading.merge_result(
            q,
            collection
        )
        eq_(
            [x.id for x in it],
            [1, 2, 7, 8]
        )

    def test_single_column(self):
        User = self.classes.User

        s = Session()

        q = s.query(User.id)
        collection = [(1, ), (2, ), (7, ), (8, )]
        it = loading.merge_result(
            q,
            collection
        )
        eq_(
            list(it),
            [(1, ), (2, ), (7, ), (8, )]
        )

    def test_entity_col_mix_plain_tuple(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        q = s.query(User, User.id)
        collection = [(u1, 1), (u2, 2), (u3, 7), (u4, 8)]
        it = loading.merge_result(
            q,
            collection
        )
        it = list(it)
        eq_(
            [(x.id, y) for x, y in it],
            [(1, 1), (2, 2), (7, 7), (8, 8)]
        )
        eq_(list(it[0].keys()), ['User', 'id'])

    def test_entity_col_mix_keyed_tuple(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        q = s.query(User, User.id)
        kt = lambda *x: KeyedTuple(x, ['User', 'id'])
        collection = [kt(u1, 1), kt(u2, 2), kt(u3, 7), kt(u4, 8)]
        it = loading.merge_result(
            q,
            collection
        )
        it = list(it)
        eq_(
            [(x.id, y) for x, y in it],
            [(1, 1), (2, 2), (7, 7), (8, 8)]
        )
        eq_(list(it[0].keys()), ['User', 'id'])

    def test_none_entity(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        ua = aliased(User)
        q = s.query(User, ua)
        kt = lambda *x: KeyedTuple(x, ['User', 'useralias'])
        collection = [kt(u1, u2), kt(u1, None), kt(u2, u3)]
        it = loading.merge_result(
            q,
            collection
        )
        eq_(
            [
                (x and x.id or None, y and y.id or None)
                for x, y in it
            ],
            [(u1.id, u2.id), (u1.id, None), (u2.id, u3.id)]
        )


