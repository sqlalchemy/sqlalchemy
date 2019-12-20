from sqlalchemy import inspect
from sqlalchemy.orm import aliased
from sqlalchemy.orm import defaultload
from sqlalchemy.orm import defer
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Load
from sqlalchemy.orm import subqueryload
from sqlalchemy.testing import eq_
from test.orm import _fixtures
from ..sql.test_compare import CacheKeyFixture


class CacheKeyTest(CacheKeyFixture, _fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = None
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_mapper_and_aliased(self):
        User, Address, Keyword = self.classes("User", "Address", "Keyword")

        self._run_cache_key_fixture(
            lambda: (inspect(User), inspect(Address), inspect(aliased(User))),
            compare_values=True,
        )

    def test_attributes(self):
        User, Address, Keyword = self.classes("User", "Address", "Keyword")

        self._run_cache_key_fixture(
            lambda: (
                User.id,
                Address.id,
                aliased(User).id,
                aliased(User, name="foo").id,
                aliased(User, name="bar").id,
                User.name,
                User.addresses,
                Address.email_address,
                aliased(User).addresses,
            ),
            compare_values=True,
        )

    def test_unbound_options(self):
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        self._run_cache_key_fixture(
            lambda: (
                joinedload(User.addresses),
                joinedload("addresses"),
                joinedload(User.orders).selectinload("items"),
                joinedload(User.orders).selectinload(Order.items),
                defer(User.id),
                defer("id"),
                defer(Address.id),
                joinedload(User.addresses).defer(Address.id),
                joinedload(aliased(User).addresses).defer(Address.id),
                joinedload(User.addresses).defer("id"),
                joinedload(User.orders).joinedload(Order.items),
                joinedload(User.orders).subqueryload(Order.items),
                subqueryload(User.orders).subqueryload(Order.items),
                subqueryload(User.orders)
                .subqueryload(Order.items)
                .defer(Item.description),
                defaultload(User.orders).defaultload(Order.items),
                defaultload(User.orders),
            ),
            compare_values=True,
        )

    def test_bound_options(self):
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        self._run_cache_key_fixture(
            lambda: (
                Load(User).joinedload(User.addresses),
                Load(User).joinedload(User.orders),
                Load(User).defer(User.id),
                Load(User).subqueryload("addresses"),
                Load(Address).defer("id"),
                Load(aliased(Address)).defer("id"),
                Load(User).joinedload(User.addresses).defer(Address.id),
                Load(User).joinedload(User.orders).joinedload(Order.items),
                Load(User).joinedload(User.orders).subqueryload(Order.items),
                Load(User).subqueryload(User.orders).subqueryload(Order.items),
                Load(User)
                .subqueryload(User.orders)
                .subqueryload(Order.items)
                .defer(Item.description),
                Load(User).defaultload(User.orders).defaultload(Order.items),
                Load(User).defaultload(User.orders),
            ),
            compare_values=True,
        )

    def test_bound_options_equiv_on_strname(self):
        """Bound loader options resolve on string name so test that the cache
        key for the string version matches the resolved version.

        """
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        for left, right in [
            (Load(User).defer(User.id), Load(User).defer("id")),
            (
                Load(User).joinedload(User.addresses),
                Load(User).joinedload("addresses"),
            ),
            (
                Load(User).joinedload(User.orders).joinedload(Order.items),
                Load(User).joinedload("orders").joinedload("items"),
            ),
        ]:
            eq_(left._generate_cache_key(), right._generate_cache_key())
