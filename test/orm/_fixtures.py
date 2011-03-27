from sqlalchemy import MetaData, Integer, String, ForeignKey
from sqlalchemy import util
from test.lib.schema import Table
from test.lib.schema import Column
from sqlalchemy.orm import attributes
from test.lib import fixtures

__all__ = ()


class FixtureTest(fixtures.MappedTest):
    """A MappedTest pre-configured with a common set of fixtures.

    """

    run_define_tables = 'once'
    run_setup_classes = 'once'
    run_setup_mappers = 'each'
    run_inserts = 'each'
    run_deletes = 'each'

    @classmethod
    def setup_classes(cls):
        class Base(cls.Comparable):
            pass

        class User(Base):
            pass

        class Order(Base):
            pass

        class Item(Base):
            pass

        class Keyword(Base):
            pass

        class Address(Base):
            pass

        class Dingaling(Base):
            pass

        class Node(Base):
            pass

        class CompositePk(Base):
            pass

    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('name', String(30), nullable=False),
              test_needs_acid=True,
              test_needs_fk=True
        )

        Table('addresses', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('user_id', None, ForeignKey('users.id')),
              Column('email_address', String(50), nullable=False),
              test_needs_acid=True,
              test_needs_fk=True
        )

        Table('email_bounces', metadata,
              Column('id', Integer, ForeignKey('addresses.id')),
              Column('bounces', Integer)
        )

        Table('orders', metadata,
                  Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
                  Column('user_id', None, ForeignKey('users.id')),
                  Column('address_id', None, ForeignKey('addresses.id')),
                  Column('description', String(30)),
                  Column('isopen', Integer),
                  test_needs_acid=True,
                  test_needs_fk=True
        )

        Table("dingalings", metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('address_id', None, ForeignKey('addresses.id')),
              Column('data', String(30)),
              test_needs_acid=True,
              test_needs_fk=True
        )

        Table('items', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('description', String(30), nullable=False),
              test_needs_acid=True,
              test_needs_fk=True
        )

        Table('order_items', metadata,
              Column('item_id', None, ForeignKey('items.id')),
              Column('order_id', None, ForeignKey('orders.id')),
              test_needs_acid=True,
              test_needs_fk=True
        )

        Table('keywords', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('name', String(30), nullable=False),
              test_needs_acid=True,
              test_needs_fk=True
        )

        Table('item_keywords', metadata,
              Column('item_id', None, ForeignKey('items.id')),
              Column('keyword_id', None, ForeignKey('keywords.id')),
              test_needs_acid=True,
              test_needs_fk=True
        )

        Table('nodes', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(30)),
            test_needs_acid=True,
            test_needs_fk=True
        )

        Table('composite_pk_table', metadata,
            Column('i', Integer, primary_key=True),
            Column('j', Integer, primary_key=True),
            Column('k', Integer, nullable=False),
        )

    @classmethod
    def setup_mappers(cls):
        pass

    @classmethod
    def fixtures(cls):
        return dict(
            users = (
                ('id', 'name'),
                (7, 'jack'),
                (8, 'ed'),
                (9, 'fred'),
                (10, 'chuck')
            ),

            addresses = (
                ('id', 'user_id', 'email_address'),
                (1, 7, "jack@bean.com"),
                (2, 8, "ed@wood.com"),
                (3, 8, "ed@bettyboop.com"),
                (4, 8, "ed@lala.com"),
                (5, 9, "fred@fred.com")
            ),

            email_bounces = (
                ('id', 'bounces'),
                (1, 1),
                (2, 0),
                (3, 5),
                (4, 0),
                (5, 0)
            ),

            orders = (
                ('id', 'user_id', 'description', 'isopen', 'address_id'),
                (1, 7, 'order 1', 0, 1),
                (2, 9, 'order 2', 0, 4),
                (3, 7, 'order 3', 1, 1),
                (4, 9, 'order 4', 1, 4),
                (5, 7, 'order 5', 0, None)
            ),

            dingalings = (
                ('id', 'address_id', 'data'),
                (1, 2, 'ding 1/2'),
                (2, 5, 'ding 2/5')
            ),

            items = (
                ('id', 'description'),
                (1, 'item 1'),
                (2, 'item 2'),
                (3, 'item 3'),
                (4, 'item 4'),
                (5, 'item 5')
            ),

            order_items = (
                ('item_id', 'order_id'),
                (1, 1),
                (2, 1),
                (3, 1),

                (1, 2),
                (2, 2),
                (3, 2),

                (3, 3),
                (4, 3),
                (5, 3),

                (1, 4),
                (5, 4),

                (5, 5)
            ),

            keywords = (
                ('id', 'name'),
                (1, 'blue'),
                (2, 'red'),
                (3, 'green'),
                (4, 'big'),
                (5, 'small'),
                (6, 'round'),
                (7, 'square')
            ),

            item_keywords = (
                ('keyword_id', 'item_id'),
                (2, 1),
                (2, 2),
                (4, 1),
                (6, 1),
                (5, 2),
                (3, 3),
                (4, 3),
                (7, 2),
                (6, 3)
            ),

            nodes = (
                ('id', 'parent_id', 'data')
            ),

            composite_pk_table = (
                ('i', 'j', 'k'),
                (1, 2, 3),
                (2, 1, 4),
                (1, 1, 5),
                (2, 2,6)
            )
        )

    @util.memoized_property
    def static(self):
        return CannedResults(self)

class CannedResults(object):
    """Built on demand, instances use mappers in effect at time of call."""

    def __init__(self, test):
        self.test = test

    @property
    def user_result(self):
        User = self.test.classes.User

        return [
            User(id=7),
            User(id=8),
            User(id=9),
            User(id=10)]

    @property
    def user_address_result(self):
        User, Address = self.test.classes.User, self.test.classes.Address

        return [
            User(id=7, addresses=[
                Address(id=1)
            ]),
            User(id=8, addresses=[
                Address(id=2, email_address='ed@wood.com'),
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])]

    @property
    def user_all_result(self):
        User, Address, Order, Item = self.test.classes.User, \
            self.test.classes.Address, self.test.classes.Order, \
            self.test.classes.Item

        return [
            User(id=7,
                 addresses=[
                   Address(id=1)],
                 orders=[
                   Order(description='order 1',
                         items=[
                           Item(description='item 1'),
                           Item(description='item 2'),
                           Item(description='item 3')]),
                   Order(description='order 3'),
                   Order(description='order 5')]),
            User(id=8,
                 addresses=[
                   Address(id=2),
                   Address(id=3),
                   Address(id=4)]),
            User(id=9,
                 addresses=[
                   Address(id=5)],
                 orders=[
                   Order(description='order 2',
                         items=[
                           Item(description='item 1'),
                           Item(description='item 2'),
                           Item(description='item 3')]),
                   Order(description='order 4',
                         items=[
                           Item(description='item 1'),
                           Item(description='item 5')])]),
            User(id=10, addresses=[])]

    @property
    def user_order_result(self):
        User, Order, Item = self.test.classes.User, \
            self.test.classes.Order, self.test.classes.Item
        return [
            User(id=7,
                 orders=[
                   Order(id=1,
                         items=[
                           Item(id=1),
                           Item(id=2),
                           Item(id=3)]),
                   Order(id=3,
                         items=[
                           Item(id=3),
                           Item(id=4),
                           Item(id=5)]),
                   Order(id=5,
                         items=[
                           Item(id=5)])]),
            User(id=8,
                 orders=[]),
            User(id=9,
                 orders=[
                   Order(id=2,
                         items=[
                           Item(id=1),
                           Item(id=2),
                           Item(id=3)]),
                   Order(id=4,
                         items=[
                           Item(id=1),
                           Item(id=5)])]),
            User(id=10)]

    @property
    def item_keyword_result(self):
        Item, Keyword = self.test.classes.Item, self.test.classes.Keyword
        return [
            Item(id=1,
                 keywords=[
                   Keyword(name='red'),
                   Keyword(name='big'),
                   Keyword(name='round')]),
            Item(id=2,
                 keywords=[
                   Keyword(name='red'),
                   Keyword(name='small'),
                   Keyword(name='square')]),
            Item(id=3,
                 keywords=[
                   Keyword(name='green'),
                   Keyword(name='big'),
                   Keyword(name='round')]),
            Item(id=4,
                 keywords=[]),
            Item(id=5,
                 keywords=[])]

    @property
    def user_item_keyword_result(self):
        Item, Keyword = self.test.classes.Item, self.test.classes.Keyword
        User, Order = self.test.classes.User, self.test.classes.Order

        item1, item2, item3, item4, item5 = \
             Item(id=1,
                  keywords=[
                    Keyword(name='red'),
                    Keyword(name='big'),
                    Keyword(name='round')]),\
             Item(id=2,
                  keywords=[
                    Keyword(name='red'),
                    Keyword(name='small'),
                    Keyword(name='square')]),\
             Item(id=3,
                  keywords=[
                    Keyword(name='green'),
                    Keyword(name='big'),
                    Keyword(name='round')]),\
             Item(id=4,
                  keywords=[]),\
             Item(id=5,
                  keywords=[])

        user_result = [
                User(id=7,
                   orders=[
                     Order(id=1,
                           items=[item1, item2, item3]),
                     Order(id=3,
                           items=[item3, item4, item5]),
                     Order(id=5,
                           items=[item5])]),
                User(id=8, orders=[]),
                User(id=9,
                   orders=[
                     Order(id=2,
                           items=[item1, item2, item3]),
                     Order(id=4,
                           items=[item1, item5])]),
                User(id=10, orders=[])]
        return user_result


