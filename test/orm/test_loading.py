from sqlalchemy import exc
from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy.engine import result_tuple
from sqlalchemy.orm import aliased
from sqlalchemy.orm import loading
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import assert_raises_message
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.fixtures import fixture_session
from . import _fixtures

# class GetFromIdentityTest(_fixtures.FixtureTest):
# class LoadOnIdentTest(_fixtures.FixtureTest):


class InstanceProcessorTest(_fixtures.FixtureTest):
    def test_state_no_load_path_comparison(self):
        # test issue #5110
        User, Order, Address = self.classes("User", "Order", "Address")
        users, orders, addresses = self.tables("users", "orders", "addresses")

        mapper(
            User,
            users,
            properties={
                "addresses": relationship(Address, lazy="joined"),
                "orders": relationship(
                    Order, lazy="joined", order_by=orders.c.id
                ),
            },
        )
        mapper(
            Order,
            orders,
            properties={"address": relationship(Address, lazy="joined")},
        )
        mapper(Address, addresses)

        s = fixture_session()

        def go():
            eq_(
                User(
                    id=7,
                    orders=[
                        Order(id=1, address=Address(id=1)),
                        Order(id=3, address=Address(id=1)),
                        Order(id=5, address=None),
                    ],
                ),
                s.query(User).populate_existing().get(7),
            )

        self.assert_sql_count(testing.db, go, 1)


class InstancesTest(_fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_cursor_close_w_failed_rowproc(self):
        User = self.classes.User
        s = fixture_session()

        q = s.query(User)

        ctx = q._compile_context()
        cursor = mock.Mock()
        ctx.compile_state._entities = [
            mock.Mock(row_processor=mock.Mock(side_effect=Exception("boom")))
        ]
        assert_raises(Exception, loading.instances, cursor, ctx)
        assert cursor.close.called, "Cursor wasn't closed"

    def test_row_proc_not_created(self):
        User = self.classes.User
        s = fixture_session()

        q = s.query(User.id, User.name)
        stmt = select(User.id)

        assert_raises_message(
            exc.NoSuchColumnError,
            "Could not locate column in row for column 'users.name'",
            q.from_statement(stmt).all,
        )


class MergeResultTest(_fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def _fixture(self):
        User = self.classes.User

        s = fixture_session()
        u1, u2, u3, u4 = (
            User(id=1, name="u1"),
            User(id=2, name="u2"),
            User(id=7, name="u3"),
            User(id=8, name="u4"),
        )
        s.query(User).filter(User.id.in_([7, 8])).all()
        s.close()
        return s, [u1, u2, u3, u4]

    def test_single_entity(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        q = s.query(User)
        collection = [u1, u2, u3, u4]
        it = loading.merge_result(q, collection)
        eq_([x.id for x in it], [1, 2, 7, 8])

    def test_single_column(self):
        User = self.classes.User

        s = fixture_session()

        q = s.query(User.id)
        collection = [(1,), (2,), (7,), (8,)]
        it = loading.merge_result(q, collection)
        eq_(list(it), [(1,), (2,), (7,), (8,)])

    def test_entity_col_mix_plain_tuple(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        q = s.query(User, User.id)
        collection = [(u1, 1), (u2, 2), (u3, 7), (u4, 8)]
        it = loading.merge_result(q, collection)
        it = list(it)
        eq_([(x.id, y) for x, y in it], [(1, 1), (2, 2), (7, 7), (8, 8)])
        eq_(list(it[0]._mapping.keys()), ["User", "id"])

    def test_entity_col_mix_keyed_tuple(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        q = s.query(User, User.id)

        row = result_tuple(["User", "id"])

        def kt(*x):
            return row(x)

        collection = [kt(u1, 1), kt(u2, 2), kt(u3, 7), kt(u4, 8)]
        it = loading.merge_result(q, collection)
        it = list(it)
        eq_([(x.id, y) for x, y in it], [(1, 1), (2, 2), (7, 7), (8, 8)])
        eq_(list(it[0]._mapping.keys()), ["User", "id"])

    def test_none_entity(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        ua = aliased(User)
        q = s.query(User, ua)

        row = result_tuple(["User", "useralias"])

        def kt(*x):
            return row(x)

        collection = [kt(u1, u2), kt(u1, None), kt(u2, u3)]
        it = loading.merge_result(q, collection)
        eq_(
            [(x and x.id or None, y and y.id or None) for x, y in it],
            [(u1.id, u2.id), (u1.id, None), (u2.id, u3.id)],
        )
