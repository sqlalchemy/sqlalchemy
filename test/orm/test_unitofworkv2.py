from sqlalchemy.test.testing import eq_, assert_raises, assert_raises_message
from sqlalchemy.test import testing
from test.orm import _fixtures
from sqlalchemy.orm import mapper, relationship, backref, create_session
from sqlalchemy.test.assertsql import AllOf, CompiledSQL

from test.orm._fixtures import keywords, addresses, Base, Keyword,  \
           Dingaling, item_keywords, dingalings, User, items,\
           orders, Address, users, nodes, \
            order_items, Item, Order, Node, \
            composite_pk_table, CompositePk

class UOWTest(_fixtures.FixtureTest, testing.AssertsExecutionResults):
    run_inserts = None

class RudimentaryFlushTest(UOWTest):

    def test_one_to_many(self):
        mapper(User, users, properties={
            'addresses':relationship(Address),
        })
        mapper(Address, addresses)
        sess = create_session()

        a1, a2 = Address(email_address='a1'), Address(email_address='a2')
        u1 = User(name='u1', addresses=[a1, a2])
        sess.add(u1)
    
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL(
                    "INSERT INTO users (name) VALUES (:name)",
                    {'name': 'u1'} 
                ),
                CompiledSQL(
                    "INSERT INTO addresses (user_id, email_address) "
                    "VALUES (:user_id, :email_address)",
                    lambda ctx: {'email_address': 'a1', 'user_id':u1.id} 
                ),
                CompiledSQL(
                    "INSERT INTO addresses (user_id, email_address) "
                    "VALUES (:user_id, :email_address)",
                    lambda ctx: {'email_address': 'a2', 'user_id':u1.id} 
                ),
            )
        
        sess.delete(u1)
        sess.delete(a1)
        sess.delete(a2)
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL(
                    "DELETE FROM addresses WHERE addresses.id = :id",
                    [{'id':a1.id},{'id':a2.id}]
                ),
                CompiledSQL(
                    "DELETE FROM users WHERE users.id = :id",
                    {'id':u1.id}
                ),
        )
        
    def test_many_to_one(self):
        
        mapper(User, users)
        mapper(Address, addresses, properties={
            'user':relationship(User)
        })
        sess = create_session()

        u1 = User(name='u1')
        a1, a2 = Address(email_address='a1', user=u1), \
                    Address(email_address='a2', user=u1)
        sess.add_all([a1, a2])
    
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL(
                    "INSERT INTO users (name) VALUES (:name)",
                    {'name': 'u1'} 
                ),
                CompiledSQL(
                    "INSERT INTO addresses (user_id, email_address) "
                    "VALUES (:user_id, :email_address)",
                    lambda ctx: {'email_address': 'a1', 'user_id':u1.id} 
                ),
                CompiledSQL(
                    "INSERT INTO addresses (user_id, email_address) "
                    "VALUES (:user_id, :email_address)",
                    lambda ctx: {'email_address': 'a2', 'user_id':u1.id} 
                ),
            )

        sess.delete(u1)
        sess.delete(a1)
        sess.delete(a2)
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL(
                    "DELETE FROM addresses WHERE addresses.id = :id",
                    [{'id':a1.id},{'id':a2.id}]
                ),
                CompiledSQL(
                    "DELETE FROM users WHERE users.id = :id",
                    {'id':u1.id}
                ),
        )
        