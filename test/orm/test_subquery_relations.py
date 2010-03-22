from sqlalchemy.test.testing import eq_, is_, is_not_
from sqlalchemy.test import testing
from sqlalchemy.orm import backref, subqueryload, subqueryload_all
from sqlalchemy.orm import mapper, relationship, create_session, lazyload, aliased
from sqlalchemy.test.testing import eq_, assert_raises
from sqlalchemy.test.assertsql import CompiledSQL
from test.orm import _base, _fixtures
import sqlalchemy as sa

class EagerTest(_fixtures.FixtureTest, testing.AssertsCompiledSQL):
    run_inserts = 'once'
    run_deletes = None

    @testing.resolve_artifact_names
    def test_basic(self):
        mapper(User, users, properties={
            'addresses':relationship(mapper(Address, addresses), order_by=Address.id)
        })
        sess = create_session()
        
        q = sess.query(User).options(subqueryload(User.addresses))
        
        def go():
            eq_(
                    [User(id=7, addresses=[Address(id=1, email_address='jack@bean.com')])],
                    q.filter(User.id==7).all()
            )
        
        self.assert_sql_count(testing.db, go, 2)
        
        def go(): 
            eq_(
                self.static.user_address_result, 
                q.order_by(User.id).all()
            )
        self.assert_sql_count(testing.db, go, 2)

    @testing.resolve_artifact_names
    def test_many_to_many(self):
        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relationship(Keyword, secondary=item_keywords,
                                    lazy='subquery', order_by=keywords.c.id)))

        q = create_session().query(Item).order_by(Item.id)
        def go():
            eq_(self.static.item_keyword_result, q.all())
        self.assert_sql_count(testing.db, go, 2)

        def go():
            eq_(self.static.item_keyword_result[0:2],
                q.join('keywords').filter(Keyword.name == 'red').all())
        self.assert_sql_count(testing.db, go, 2)

        def go():
            eq_(self.static.item_keyword_result[0:2],
                (q.join('keywords', aliased=True).
                 filter(Keyword.name == 'red')).all())
        self.assert_sql_count(testing.db, go, 2)

    @testing.resolve_artifact_names
    def test_orderby(self):
        mapper(User, users, properties = {
            'addresses':relationship(mapper(Address, addresses), 
                        lazy='subquery', order_by=addresses.c.email_address),
        })
        q = create_session().query(User)
        eq_([
            User(id=7, addresses=[
                Address(id=1)
            ]),
            User(id=8, addresses=[
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
                Address(id=2, email_address='ed@wood.com')
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])
        ], q.order_by(User.id).all())

    @testing.resolve_artifact_names
    def test_orderby_multi(self):
        mapper(User, users, properties = {
            'addresses':relationship(mapper(Address, addresses), 
                            lazy='subquery', 
                            order_by=[addresses.c.email_address, addresses.c.id]),
        })
        q = create_session().query(User)
        eq_([
            User(id=7, addresses=[
                Address(id=1)
            ]),
            User(id=8, addresses=[
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
                Address(id=2, email_address='ed@wood.com')
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])
        ], q.order_by(User.id).all())

    @testing.resolve_artifact_names
    def test_orderby_related(self):
        """A regular mapper select on a single table can 
            order by a relationship to a second table"""

        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relationship(Address, lazy='subquery', order_by=addresses.c.id),
        ))

        q = create_session().query(User)
        l = q.filter(User.id==Address.user_id).order_by(Address.email_address).all()

        eq_([
            User(id=8, addresses=[
                Address(id=2, email_address='ed@wood.com'),
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=7, addresses=[
                Address(id=1)
            ]),
        ], l)

    @testing.resolve_artifact_names
    def test_orderby_desc(self):
        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relationship(Address, lazy='subquery',
                                 order_by=[sa.desc(addresses.c.email_address)]),
        ))
        sess = create_session()
        eq_([
            User(id=7, addresses=[
                Address(id=1)
            ]),
            User(id=8, addresses=[
                Address(id=2, email_address='ed@wood.com'),
                Address(id=4, email_address='ed@lala.com'),
                Address(id=3, email_address='ed@bettyboop.com'),
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])
        ], sess.query(User).order_by(User.id).all())

    # TODO: all the tests in test_eager_relations
    
    # TODO: ensure state stuff works out OK, existing objects/collections
    # don't get inappropriately whacked, etc.
    
    # TODO: subquery loading with eagerloads on those collections ???
    
    # TODO: eagerloading of child objects with subquery loading on those ???
    
    # TODO: lazy loads leading into subq loads ??
    
    # TODO: e.g. all kinds of path combos need to be tested
    
    # TODO: joined table inh !  
    
