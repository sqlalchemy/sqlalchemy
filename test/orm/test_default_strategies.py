from test.orm import _fixtures
from sqlalchemy import testing
from sqlalchemy.orm import mapper, relationship, create_session
from sqlalchemy import util
import sqlalchemy as sa
from sqlalchemy.testing import eq_, assert_raises_message

class DefaultStrategyOptionsTest(_fixtures.FixtureTest):

    def _assert_fully_loaded(self, users):
        # verify everything loaded, with no additional sql needed
        def go():
            # comparison with no additional sql
            eq_(users, self.static.user_all_result)
            # keywords are not part of self.static.user_all_result, so
            # verify all the item keywords were loaded, with no more sql.
            # 'any' verifies at least some items have keywords; we build
            # a list for any([...]) instead of any(...) to prove we've
            # iterated all the items with no sql.
            f = util.flatten_iterator
            assert any([i.keywords for i in
                f([o.items for o in f([u.orders for u in users])])])
        self.assert_sql_count(testing.db, go, 0)

    def _assert_addresses_loaded(self, users):
        # verify all the addresses were joined loaded with no more sql
        def go():
            for u, static in zip(users, self.static.user_all_result):
                eq_(u.addresses, static.addresses)
        self.assert_sql_count(testing.db, go, 0)

    def _downgrade_fixture(self):
        users, Keyword, items, order_items, orders, Item, User, \
            Address, keywords, item_keywords, Order, addresses = \
            self.tables.users, self.classes.Keyword, self.tables.items, \
            self.tables.order_items, self.tables.orders, \
            self.classes.Item, self.classes.User, self.classes.Address, \
            self.tables.keywords, self.tables.item_keywords, \
            self.classes.Order, self.tables.addresses

        mapper(Address, addresses)

        mapper(Keyword, keywords)

        mapper(Item, items, properties=dict(
            keywords=relationship(Keyword, secondary=item_keywords,
                              lazy='subquery',
                              order_by=item_keywords.c.keyword_id)))

        mapper(Order, orders, properties=dict(
            items=relationship(Item, secondary=order_items, lazy='subquery',
                           order_by=order_items.c.item_id)))

        mapper(User, users, properties=dict(
            addresses=relationship(Address, lazy='joined',
                               order_by=addresses.c.id),
            orders=relationship(Order, lazy='joined',
                            order_by=orders.c.id)))

        return create_session()

    def _upgrade_fixture(self):
        users, Keyword, items, order_items, orders, Item, User, \
            Address, keywords, item_keywords, Order, addresses = \
            self.tables.users, self.classes.Keyword, self.tables.items, \
            self.tables.order_items, self.tables.orders, \
            self.classes.Item, self.classes.User, self.classes.Address, \
            self.tables.keywords, self.tables.item_keywords, \
            self.classes.Order, self.tables.addresses

        mapper(Address, addresses)

        mapper(Keyword, keywords)

        mapper(Item, items, properties=dict(
            keywords=relationship(Keyword, secondary=item_keywords,
                              lazy='select',
                              order_by=item_keywords.c.keyword_id)))

        mapper(Order, orders, properties=dict(
            items=relationship(Item, secondary=order_items, lazy=True,
                           order_by=order_items.c.item_id)))

        mapper(User, users, properties=dict(
            addresses=relationship(Address, lazy=True,
                               order_by=addresses.c.id),
            orders=relationship(Order,
                            order_by=orders.c.id)))

        return create_session()

    def test_downgrade_baseline(self):
        """Mapper strategy defaults load as expected
        (compare to rest of DefaultStrategyOptionsTest downgrade tests)."""
        sess = self._downgrade_fixture()
        users = []

        # test _downgrade_fixture mapper defaults, 3 queries (2 subquery loads).
        def go():
            users[:] = sess.query(self.classes.User)\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 3)

        # all loaded with no additional sql
        self._assert_fully_loaded(users)

    def test_disable_eagerloads(self):
        """Mapper eager load strategy defaults can be shut off
        with enable_eagerloads(False)."""

        # While this isn't testing a mapper option, it is included
        # as baseline reference for how XYZload('*') option
        # should work, namely, it shouldn't affect later queries
        # (see other test_select_s)
        sess = self._downgrade_fixture()
        users = []

        # demonstrate that enable_eagerloads loads with only 1 sql
        def go():
            users[:] = sess.query(self.classes.User)\
                .enable_eagerloads(False)\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 1)

        # demonstrate that users[0].orders must now be loaded with 3 sql
        # (need to lazyload, and 2 subquery: 3 total)
        def go():
            users[0].orders
        self.assert_sql_count(testing.db, go, 3)

    def test_last_one_wins(self):
        sess = self._downgrade_fixture()
        users = []

        def go():
            users[:] = sess.query(self.classes.User)\
                .options(sa.orm.subqueryload('*'))\
                .options(sa.orm.joinedload(self.classes.User.addresses))\
                .options(sa.orm.lazyload('*'))\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 1)

        # verify all the addresses were joined loaded (no more sql)
        self._assert_addresses_loaded(users)

    def test_star_must_be_alone(self):
        sess = self._downgrade_fixture()
        User = self.classes.User
        opt = sa.orm.subqueryload('*', User.addresses)
        assert_raises_message(
            sa.exc.ArgumentError,
            "Wildcard token cannot be followed by another entity",
            sess.query(User).options, opt
        )

    def test_global_star_ignored_no_entities_unbound(self):
        sess = self._downgrade_fixture()
        User = self.classes.User
        opt = sa.orm.lazyload('*')
        q = sess.query(User.name).options(opt)
        eq_(q.all(), [('jack',), ('ed',), ('fred',), ('chuck',)])

    def test_global_star_ignored_no_entities_bound(self):
        sess = self._downgrade_fixture()
        User = self.classes.User
        opt = sa.orm.Load(User).lazyload('*')
        q = sess.query(User.name).options(opt)
        eq_(q.all(), [('jack',), ('ed',), ('fred',), ('chuck',)])

    def test_select_with_joinedload(self):
        """Mapper load strategy defaults can be downgraded with
        lazyload('*') option, while explicit joinedload() option
        is still honored"""
        sess = self._downgrade_fixture()
        users = []

        # lazyload('*') shuts off 'orders' subquery: only 1 sql
        def go():
            users[:] = sess.query(self.classes.User)\
                .options(sa.orm.lazyload('*'))\
                .options(sa.orm.joinedload(self.classes.User.addresses))\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 1)

        # verify all the addresses were joined loaded (no more sql)
        self._assert_addresses_loaded(users)

        # users[0] has orders, which need to lazy load, and 2 subquery:
        # (same as with test_disable_eagerloads): 3 total sql
        def go():
            users[0].orders
        self.assert_sql_count(testing.db, go, 3)

    def test_select_with_subqueryload(self):
        """Mapper load strategy defaults can be downgraded with
        lazyload('*') option, while explicit subqueryload() option
        is still honored"""
        sess = self._downgrade_fixture()
        users = []

        # now test 'default_strategy' option combined with 'subquery'
        # shuts off 'addresses' load AND orders.items load: 2 sql expected
        def go():
            users[:] = sess.query(self.classes.User)\
                .options(sa.orm.lazyload('*'))\
                .options(sa.orm.subqueryload(self.classes.User.orders))\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 2)

        # Verify orders have already been loaded: 0 sql
        def go():
            for u, static in zip(users, self.static.user_all_result):
                assert len(u.orders) == len(static.orders)
        self.assert_sql_count(testing.db, go, 0)

        # Verify lazyload('*') prevented orders.items load
        # users[0].orders[0] has 3 items, each with keywords: 2 sql
        # ('items' and 'items.keywords' subquery)
        def go():
            for i in users[0].orders[0].items:
                i.keywords
        self.assert_sql_count(testing.db, go, 2)

        # lastly, make sure they actually loaded properly
        eq_(users, self.static.user_all_result)

    def test_noload_with_joinedload(self):
        """Mapper load strategy defaults can be downgraded with
        noload('*') option, while explicit joinedload() option
        is still honored"""
        sess = self._downgrade_fixture()
        users = []

        # test noload('*') shuts off 'orders' subquery, only 1 sql
        def go():
            users[:] = sess.query(self.classes.User)\
                .options(sa.orm.noload('*'))\
                .options(sa.orm.joinedload(self.classes.User.addresses))\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 1)

        # verify all the addresses were joined loaded (no more sql)
        self._assert_addresses_loaded(users)

        # User.orders should have loaded "noload" (meaning [])
        def go():
            for u in users:
                assert u.orders == []
        self.assert_sql_count(testing.db, go, 0)

    def test_noload_with_subqueryload(self):
        """Mapper load strategy defaults can be downgraded with
        noload('*') option, while explicit subqueryload() option
        is still honored"""
        sess = self._downgrade_fixture()
        users = []

        # test noload('*') option combined with subqueryload()
        # shuts off 'addresses' load AND orders.items load: 2 sql expected
        def go():
            users[:] = sess.query(self.classes.User)\
                .options(sa.orm.noload('*'))\
                .options(sa.orm.subqueryload(self.classes.User.orders))\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 2)

        def go():
            # Verify orders have already been loaded: 0 sql
            for u, static in zip(users, self.static.user_all_result):
                assert len(u.orders) == len(static.orders)
            # Verify noload('*') prevented orders.items load
            # and set 'items' to []
            for u in users:
                for o in u.orders:
                    assert o.items == []
        self.assert_sql_count(testing.db, go, 0)

    def test_joined(self):
        """Mapper load strategy defaults can be upgraded with
        joinedload('*') option."""
        sess = self._upgrade_fixture()
        users = []

        # test upgrade all to joined: 1 sql
        def go():
            users[:] = sess.query(self.classes.User)\
                .options(sa.orm.joinedload('*'))\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 1)

        # verify everything loaded, with no additional sql needed
        self._assert_fully_loaded(users)

    def test_joined_path_wildcards(self):
        sess = self._upgrade_fixture()
        users = []

        # test upgrade all to joined: 1 sql
        def go():
            users[:] = sess.query(self.classes.User)\
                .options(sa.orm.joinedload('.*'))\
                .options(sa.orm.joinedload("addresses.*"))\
                .options(sa.orm.joinedload("orders.*"))\
                .options(sa.orm.joinedload("orders.items.*"))\
                .order_by(self.classes.User.id)\
                .all()

        self.assert_sql_count(testing.db, go, 1)
        self._assert_fully_loaded(users)

    def test_joined_with_lazyload(self):
        """Mapper load strategy defaults can be upgraded with
        joinedload('*') option, while explicit lazyload() option
        is still honored"""
        sess = self._upgrade_fixture()
        users = []

        # test joined all but 'keywords': upgraded to 1 sql
        def go():
            users[:] = sess.query(self.classes.User)\
                .options(sa.orm.lazyload('orders.items.keywords'))\
                .options(sa.orm.joinedload('*'))\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 1)

        # everything (but keywords) loaded ok
        # (note self.static.user_all_result contains no keywords)
        def go():
            eq_(users, self.static.user_all_result)
        self.assert_sql_count(testing.db, go, 0)

        # verify the items were loaded, while item.keywords were not
        def go():
            # redundant with last test, but illustrative
            users[0].orders[0].items[0]
        self.assert_sql_count(testing.db, go, 0)
        def go():
            users[0].orders[0].items[0].keywords
        self.assert_sql_count(testing.db, go, 1)

    def test_joined_with_subqueryload(self):
        """Mapper load strategy defaults can be upgraded with
        joinedload('*') option, while explicit subqueryload() option
        is still honored"""
        sess = self._upgrade_fixture()
        users = []

        # test upgrade all but 'addresses', which is subquery loaded (2 sql)
        def go():
            users[:] = sess.query(self.classes.User)\
                .options(sa.orm.subqueryload(self.classes.User.addresses))\
                .options(sa.orm.joinedload('*'))\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 2)

        # verify everything loaded, with no additional sql needed
        self._assert_fully_loaded(users)

    def test_subquery(self):
        """Mapper load strategy defaults can be upgraded with
        subqueryload('*') option."""
        sess = self._upgrade_fixture()
        users = []

        # test upgrade all to subquery: 1 sql + 4 relationships = 5
        def go():
            users[:] = sess.query(self.classes.User)\
                .options(sa.orm.subqueryload('*'))\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 5)

        # verify everything loaded, with no additional sql needed
        self._assert_fully_loaded(users)

    def test_subquery_path_wildcards(self):
        sess = self._upgrade_fixture()
        users = []

        # test upgrade all to subquery: 1 sql + 4 relationships = 5
        def go():
            users[:] = sess.query(self.classes.User)\
                .options(sa.orm.subqueryload('.*'))\
                .options(sa.orm.subqueryload('addresses.*'))\
                .options(sa.orm.subqueryload('orders.*'))\
                .options(sa.orm.subqueryload('orders.items.*'))\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 5)

        # verify everything loaded, with no additional sql needed
        self._assert_fully_loaded(users)

    def test_subquery_with_lazyload(self):
        """Mapper load strategy defaults can be upgraded with
        subqueryload('*') option, while explicit lazyload() option
        is still honored"""
        sess = self._upgrade_fixture()
        users = []

        # test subquery all but 'keywords' (1 sql + 3 relationships = 4)
        def go():
            users[:] = sess.query(self.classes.User)\
                .options(sa.orm.lazyload('orders.items.keywords'))\
                .options(sa.orm.subqueryload('*'))\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 4)

        # no more sql
        # (note self.static.user_all_result contains no keywords)
        def go():
            eq_(users, self.static.user_all_result)
        self.assert_sql_count(testing.db, go, 0)

        # verify the item.keywords were not loaded
        def go():
            users[0].orders[0].items[0]
        self.assert_sql_count(testing.db, go, 0)
        def go():
            users[0].orders[0].items[0].keywords
        self.assert_sql_count(testing.db, go, 1)

    def test_subquery_with_joinedload(self):
        """Mapper load strategy defaults can be upgraded with
        subqueryload('*') option, while multiple explicit
        joinedload() options are still honored"""
        sess = self._upgrade_fixture()
        users = []

        # test upgrade all but 'addresses' & 'orders', which are joinedloaded
        # (1 sql + items + keywords = 3)
        def go():
            users[:] = sess.query(self.classes.User)\
                .options(sa.orm.joinedload(self.classes.User.addresses))\
                .options(sa.orm.joinedload(self.classes.User.orders))\
                .options(sa.orm.subqueryload('*'))\
                .order_by(self.classes.User.id)\
                .all()
        self.assert_sql_count(testing.db, go, 3)

        # verify everything loaded, with no additional sql needed
        self._assert_fully_loaded(users)
