# coding: utf-8

from sqlalchemy.ext import serializer
from sqlalchemy import testing
from sqlalchemy import Integer, String, ForeignKey, select, \
    desc, func, util, MetaData, literal_column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import Column
from sqlalchemy.orm import relationship, sessionmaker, scoped_session, \
    class_mapper, mapper, joinedload, configure_mappers, aliased
from sqlalchemy.testing import eq_, AssertsCompiledSQL
from sqlalchemy.util import u, ue

from sqlalchemy.testing import fixtures


class User(fixtures.ComparableEntity):
    pass


class Address(fixtures.ComparableEntity):
    pass


users = addresses = Session = None


class SerializeTest(AssertsCompiledSQL, fixtures.MappedTest):

    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        global users, addresses
        users = Table('users', metadata, Column('id', Integer,
                      primary_key=True), Column('name', String(50)))
        addresses = Table('addresses', metadata, Column('id', Integer,
                          primary_key=True), Column('email',
                          String(50)), Column('user_id', Integer,
                          ForeignKey('users.id')))

    @classmethod
    def setup_mappers(cls):
        global Session
        Session = scoped_session(sessionmaker())
        mapper(User, users,
               properties={'addresses': relationship(Address, backref='user',
                                                     order_by=addresses.c.id)})
        mapper(Address, addresses)
        configure_mappers()

    @classmethod
    def insert_data(cls):
        params = [dict(list(zip(('id', 'name'), column_values)))
                  for column_values in [(7, 'jack'), (8, 'ed'), (9, 'fred'),
                                        (10, 'chuck')]]
        users.insert().execute(params)
        addresses.insert().execute([dict(list(zip(('id', 'user_id', 'email'),
                                                  column_values)))
                                   for column_values in [
                                           (1, 7, 'jack@bean.com'),
                                           (2, 8, 'ed@wood.com'),
                                           (3, 8, 'ed@bettyboop.com'),
                                           (4, 8, 'ed@lala.com'),
                                           (5, 9, 'fred@fred.com')]])

    def test_tables(self):
        assert serializer.loads(serializer.dumps(users, -1),
                                users.metadata, Session) is users

    def test_columns(self):
        assert serializer.loads(serializer.dumps(users.c.name, -1),
                                users.metadata, Session) is users.c.name

    def test_mapper(self):
        user_mapper = class_mapper(User)
        assert serializer.loads(serializer.dumps(user_mapper, -1),
                                None, None) is user_mapper

    def test_attribute(self):
        assert serializer.loads(serializer.dumps(User.name, -1), None,
                                None) is User.name

    def test_expression(self):
        expr = \
            select([users]).select_from(users.join(addresses)).limit(5)
        re_expr = serializer.loads(serializer.dumps(expr, -1),
                                   users.metadata, None)
        eq_(str(expr), str(re_expr))
        assert re_expr.bind is testing.db
        eq_(re_expr.execute().fetchall(), [(7, 'jack'), (8, 'ed'),
            (8, 'ed'), (8, 'ed'), (9, 'fred')])

    def test_query_one(self):
        q = Session.query(User).filter(User.name == 'ed').\
            options(joinedload(User.addresses))

        q2 = serializer.loads(serializer.dumps(q, -1), users.metadata, Session)

        def go():
            eq_(q2.all(),
                [User(name='ed',
                      addresses=[Address(id=2), Address(id=3), Address(id=4)])]
                )

        self.assert_sql_count(testing.db, go, 1)

        eq_(q2.join(User.addresses).filter(Address.email
            == 'ed@bettyboop.com').value(func.count(literal_column('*'))), 1)
        u1 = Session.query(User).get(8)
        q = Session.query(Address).filter(Address.user == u1)\
            .order_by(desc(Address.email))
        q2 = serializer.loads(serializer.dumps(q, -1), users.metadata,
                              Session)
        eq_(q2.all(), [Address(email='ed@wood.com'),
            Address(email='ed@lala.com'),
            Address(email='ed@bettyboop.com')])

    @testing.requires.non_broken_pickle
    def test_query_two(self):
        q = Session.query(User).join(User.addresses).\
            filter(Address.email.like('%fred%'))
        q2 = serializer.loads(serializer.dumps(q, -1), users.metadata,
                              Session)
        eq_(q2.all(), [User(name='fred')])
        eq_(list(q2.values(User.id, User.name)), [(9, 'fred')])

    # fails too often/randomly
    # @testing.requires.non_broken_pickle
    # def test_query_three(self):
    #    ua = aliased(User)
    #    q = \
    #        Session.query(ua).join(ua.addresses).\
    #           filter(Address.email.like('%fred%'))
    #    q2 = serializer.loads(serializer.dumps(q, -1), users.metadata,
    #                          Session)
    #    eq_(q2.all(), [User(name='fred')])
    #
        # try to pull out the aliased entity here...
    #    ua_2 = q2._entities[0].entity_zero.entity
    #    eq_(list(q2.values(ua_2.id, ua_2.name)), [(9, 'fred')])

    @testing.requires.non_broken_pickle
    def test_orm_join(self):
        from sqlalchemy.orm.util import join

        j = join(User, Address, User.addresses)

        j2 = serializer.loads(serializer.dumps(j, -1), users.metadata)
        assert j2.left is j.left
        assert j2.right is j.right
        assert j2._target_adapter._next

    @testing.exclude('sqlite', '<=', (3, 5, 9),
                     'id comparison failing on the buildbot')
    def test_aliases(self):
        u7, u8, u9, u10 = Session.query(User).order_by(User.id).all()
        ualias = aliased(User)
        q = Session.query(User, ualias)\
            .join(ualias, User.id < ualias.id)\
            .filter(User.id < 9)\
            .order_by(User.id, ualias.id)
        eq_(list(q.all()), [(u7, u8), (u7, u9), (u7, u10), (u8, u9),
            (u8, u10)])
        q2 = serializer.loads(serializer.dumps(q, -1), users.metadata,
                              Session)
        eq_(list(q2.all()), [(u7, u8), (u7, u9), (u7, u10), (u8, u9),
            (u8, u10)])

    @testing.requires.non_broken_pickle
    def test_any(self):
        r = User.addresses.any(Address.email == 'x')
        ser = serializer.dumps(r, -1)
        x = serializer.loads(ser, users.metadata)
        eq_(str(r), str(x))

    def test_unicode(self):
        m = MetaData()
        t = Table(ue('\u6e2c\u8a66'), m,
                  Column(ue('\u6e2c\u8a66_id'), Integer))

        expr = select([t]).where(t.c[ue('\u6e2c\u8a66_id')] == 5)

        expr2 = serializer.loads(serializer.dumps(expr, -1), m)

        self.assert_compile(
            expr2,
            ue('SELECT "\u6e2c\u8a66"."\u6e2c\u8a66_id" FROM "\u6e2c\u8a66" '
                'WHERE "\u6e2c\u8a66"."\u6e2c\u8a66_id" = :\u6e2c\u8a66_id_1'),
            dialect="default"
        )


if __name__ == '__main__':
    testing.main()
