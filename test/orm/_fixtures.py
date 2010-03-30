from sqlalchemy import MetaData, Integer, String, ForeignKey
from sqlalchemy.test.schema import Table
from sqlalchemy.test.schema import Column
from sqlalchemy.orm import attributes
from sqlalchemy.test.testing import fixture
from test.orm import _base

__all__ = ()

fixture_metadata = MetaData()

def fixture_table(table, columns, *rows):
    def load_fixture(bind=None):
        bind = bind or table.bind
        if rows:
            bind.execute(
                table.insert(),
                [dict(zip(columns, column_values)) for column_values in rows])
    table.info[('fixture', 'loader')] = load_fixture
    table.info[('fixture', 'columns')] = columns
    table.info[('fixture', 'rows')] = rows
    return table

users = fixture_table(
    Table('users', fixture_metadata,
          Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
          Column('name', String(30), nullable=False),
          test_needs_acid=True,
          test_needs_fk=True),
    ('id', 'name'),
    (7, 'jack'),
    (8, 'ed'),
    (9, 'fred'),
    (10, 'chuck'))

addresses = fixture_table(
    Table('addresses', fixture_metadata,
          Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
          Column('user_id', None, ForeignKey('users.id')),
          Column('email_address', String(50), nullable=False),
          test_needs_acid=True,
          test_needs_fk=True),
    ('id', 'user_id', 'email_address'),
    (1, 7, "jack@bean.com"),
    (2, 8, "ed@wood.com"),
    (3, 8, "ed@bettyboop.com"),
    (4, 8, "ed@lala.com"),
    (5, 9, "fred@fred.com"))

email_bounces = fixture_table(
    Table('email_bounces', fixture_metadata,
          Column('id', Integer, ForeignKey('addresses.id')),
          Column('bounces', Integer)),
    ('id', 'bounces'),
    (1, 1),
    (2, 0),
    (3, 5),
    (4, 0),
    (5, 0))

orders = fixture_table(
    Table('orders', fixture_metadata,
          Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
          Column('user_id', None, ForeignKey('users.id')),
          Column('address_id', None, ForeignKey('addresses.id')),
          Column('description', String(30)),
          Column('isopen', Integer),
          test_needs_acid=True,
          test_needs_fk=True),
    ('id', 'user_id', 'description', 'isopen', 'address_id'),
    (1, 7, 'order 1', 0, 1),
    (2, 9, 'order 2', 0, 4),
    (3, 7, 'order 3', 1, 1),
    (4, 9, 'order 4', 1, 4),
    (5, 7, 'order 5', 0, None))

dingalings = fixture_table(
    Table("dingalings", fixture_metadata,
          Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
          Column('address_id', None, ForeignKey('addresses.id')),
          Column('data', String(30)),
          test_needs_acid=True,
          test_needs_fk=True),
    ('id', 'address_id', 'data'),
    (1, 2, 'ding 1/2'),
    (2, 5, 'ding 2/5'))

items = fixture_table(
    Table('items', fixture_metadata,
          Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
          Column('description', String(30), nullable=False),
          test_needs_acid=True,
          test_needs_fk=True),
    ('id', 'description'),
    (1, 'item 1'),
    (2, 'item 2'),
    (3, 'item 3'),
    (4, 'item 4'),
    (5, 'item 5'))

order_items = fixture_table(
    Table('order_items', fixture_metadata,
          Column('item_id', None, ForeignKey('items.id')),
          Column('order_id', None, ForeignKey('orders.id')),
          test_needs_acid=True,
          test_needs_fk=True),
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

    (5, 5))

keywords = fixture_table(
    Table('keywords', fixture_metadata,
          Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
          Column('name', String(30), nullable=False),
          test_needs_acid=True,
          test_needs_fk=True),
    ('id', 'name'),
    (1, 'blue'),
    (2, 'red'),
    (3, 'green'),
    (4, 'big'),
    (5, 'small'),
    (6, 'round'),
    (7, 'square'))

item_keywords = fixture_table(
    Table('item_keywords', fixture_metadata,
          Column('item_id', None, ForeignKey('items.id')),
          Column('keyword_id', None, ForeignKey('keywords.id')),
          test_needs_acid=True,
          test_needs_fk=True),
    ('keyword_id', 'item_id'),
    (2, 1),
    (2, 2),
    (4, 1),
    (6, 1),
    (5, 2),
    (3, 3),
    (4, 3),
    (7, 2),
    (6, 3))

nodes = fixture_table(
    Table('nodes', fixture_metadata,
        Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
        Column('parent_id', Integer, ForeignKey('nodes.id')),
        Column('data', String(30)),
        test_needs_acid=True,
        test_needs_fk=True
    ),
    ('id', 'parent_id', 'data')
)

composite_pk_table = fixture_table(
    Table('composite_pk_table', fixture_metadata,
        Column('i', Integer, primary_key=True),
        Column('j', Integer, primary_key=True),
        Column('k', Integer, nullable=False),                    
    ),
    ('i', 'j', 'k'),
    (1, 2, 3),
    (2, 1, 4),
    (1, 1, 5),
    (2, 2,6))


def _load_fixtures():
    for table in fixture_metadata.sorted_tables:
        table.info[('fixture', 'loader')]()

def run_inserts_for(table, bind=None):
    table.info[('fixture', 'loader')](bind)

class Base(_base.ComparableEntity):
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
    
class FixtureTest(_base.MappedTest):
    """A MappedTest pre-configured for fixtures.

    All fixture tables are pre-loaded into cls.tables, as are all fixture
    lasses in cls.classes and as cls.ClassName.

    Fixture.mapper() still functions and willregister non-fixture classes into
    cls.classes.

    """

    run_define_tables = 'once'
    run_setup_classes = 'once'
    run_setup_mappers = 'each'
    run_inserts = 'each'
    run_deletes = 'each'

    metadata = fixture_metadata
    fixture_classes = dict(User=User,
                           Order=Order,
                           Item=Item,
                           Keyword=Keyword,
                           Address=Address,
                           Dingaling=Dingaling)

    @classmethod
    def define_tables(cls, metadata):
        pass

    @classmethod
    def setup_classes(cls):
        for cl in cls.fixture_classes.values():
            cls.register_class(cl)

    @classmethod
    def setup_mappers(cls):
        pass

    @classmethod
    def insert_data(cls):
        _load_fixtures()


class CannedResults(object):
    """Built on demand, instances use mappers in effect at time of call."""

    @property
    def user_result(self):
        return [
            User(id=7),
            User(id=8),
            User(id=9),
            User(id=10)]

    @property
    def user_address_result(self):
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
        
FixtureTest.static = CannedResults()

