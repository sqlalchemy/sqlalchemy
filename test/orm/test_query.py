from test.lib.testing import eq_, assert_raises, assert_raises_message
import operator
from sqlalchemy import *
from sqlalchemy import exc as sa_exc, util
from sqlalchemy.sql import compiler, table, column
from sqlalchemy.engine import default
from sqlalchemy.orm import *
from sqlalchemy.orm import attributes

from test.lib.testing import eq_

import sqlalchemy as sa
from test.lib import testing, AssertsCompiledSQL, Column, engines

from test.orm import _fixtures
from test.orm._fixtures import keywords, addresses, Base, \
            Keyword, FixtureTest, \
           Dingaling, item_keywords, dingalings, User, items,\
           orders, Address, users, nodes, \
            order_items, Item, Order, Node, \
            composite_pk_table, CompositePk

from test.orm import _base

from sqlalchemy.orm.util import join, outerjoin, with_parent

class QueryTest(_fixtures.FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None


    @classmethod
    def setup_mappers(cls):
        mapper(User, users, properties={
            'addresses':relationship(Address, backref='user', order_by=addresses.c.id),
            'orders':relationship(Order, backref='user', order_by=orders.c.id), # o2m, m2o
        })
        mapper(Address, addresses, properties={
            'dingaling':relationship(Dingaling, uselist=False, backref="address")  #o2o
        })
        mapper(Dingaling, dingalings)
        mapper(Order, orders, properties={
            'items':relationship(Item, secondary=order_items, order_by=items.c.id),  #m2m
            'address':relationship(Address),  # m2o
        })
        mapper(Item, items, properties={
            'keywords':relationship(Keyword, secondary=item_keywords) #m2m
        })
        mapper(Keyword, keywords)

        mapper(Node, nodes, properties={
            'children':relationship(Node, 
                backref=backref('parent', remote_side=[nodes.c.id])
            )
        })

        mapper(CompositePk, composite_pk_table)

        configure_mappers()

class RowTupleTest(QueryTest):
    run_setup_mappers = None

    def test_custom_names(self):
        mapper(User, users, properties={
            'uname':users.c.name
        })

        row  = create_session().\
                    query(User.id, User.uname).\
                    filter(User.id==7).first()
        assert row.id == 7
        assert row.uname == 'jack'

    def test_column_metadata(self):
        mapper(User, users)
        mapper(Address, addresses)
        sess = create_session()
        user_alias = aliased(User)
        address_alias = aliased(Address, name='aalias')
        fn = func.count(User.id)
        name_label = User.name.label('uname')
        for q, asserted in [
            (
                sess.query(User),
                [{'name':'User', 'type':User, 'aliased':False, 'expr':User}]
            ),
            (
                sess.query(User.id, User),
                [
                    {'name':'id', 'type':users.c.id.type, 'aliased':False,
                        'expr':User.id},
                    {'name':'User', 'type':User, 'aliased':False, 'expr':User}
                ]
            ),
            (
                sess.query(User.id, user_alias),
                [
                    {'name':'id', 'type':users.c.id.type, 'aliased':False,
                        'expr':User.id},
                    {'name':None, 'type':User, 'aliased':True,
                        'expr':user_alias}
                ]
            ),
            (
                sess.query(address_alias),
                [
                    {'name':'aalias', 'type':Address, 'aliased':True,
                        'expr':address_alias}
                ]
            ),
            (
                sess.query(name_label, fn),
                [
                    {'name':'uname', 'type':users.c.name.type,
                                        'aliased':False,'expr':name_label},
                    {'name':None, 'type':fn.type, 'aliased':False,
                        'expr':fn
                    },
                ]
            )
        ]:
            eq_(
                q.column_descriptions,
                asserted
            )

class GetTest(QueryTest):
    def test_get(self):
        s = create_session()
        assert s.query(User).get(19) is None
        u = s.query(User).get(7)
        u2 = s.query(User).get(7)
        assert u is u2
        s.expunge_all()
        u2 = s.query(User).get(7)
        assert u is not u2

    def test_get_composite_pk_no_result(self):
        s = Session()
        assert s.query(CompositePk).get((100,100)) is None

    def test_get_composite_pk_result(self):
        s = Session()
        one_two = s.query(CompositePk).get((1,2))
        assert one_two.i == 1
        assert one_two.j == 2
        assert one_two.k == 3

    def test_get_too_few_params(self):
        s = Session()
        q = s.query(CompositePk)
        assert_raises(sa_exc.InvalidRequestError, q.get, 7)

    def test_get_too_few_params_tuple(self):
        s = Session()
        q = s.query(CompositePk)
        assert_raises(sa_exc.InvalidRequestError, q.get, (7,))

    def test_get_too_many_params(self):
        s = Session()
        q = s.query(CompositePk)
        assert_raises(sa_exc.InvalidRequestError, q.get, (7, 10, 100))

    def test_get_null_pk(self):
        """test that a mapping which can have None in a 
        PK (i.e. map to an outerjoin) works with get()."""

        s = users.outerjoin(addresses)

        class UserThing(_base.ComparableEntity):
            pass

        mapper(UserThing, s, properties={
            'id':(users.c.id, addresses.c.user_id),
            'address_id':addresses.c.id,
        })
        sess = create_session()
        u10 = sess.query(UserThing).get((10, None))
        eq_(u10,
            UserThing(id=10)
        )

    def test_no_criterion(self):
        """test that get()/load() does not use preexisting filter/etc. criterion"""

        s = create_session()

        q = s.query(User).join('addresses').filter(Address.user_id==8)
        assert_raises(sa_exc.InvalidRequestError, q.get, 7)
        assert_raises(sa_exc.InvalidRequestError, s.query(User).filter(User.id==7).get, 19)

        # order_by()/get() doesn't raise
        s.query(User).order_by(User.id).get(8)

    def test_unique_param_names(self):
        class SomeUser(object):
            pass
        s = users.select(users.c.id!=12).alias('users')
        m = mapper(SomeUser, s)
        assert s.primary_key == m.primary_key

        sess = create_session()
        assert sess.query(SomeUser).get(7).name == 'jack'

    def test_load(self):
        s = create_session()

        assert s.query(User).populate_existing().get(19) is None

        u = s.query(User).populate_existing().get(7)
        u2 = s.query(User).populate_existing().get(7)
        assert u is u2
        s.expunge_all()
        u2 = s.query(User).populate_existing().get(7)
        assert u is not u2

        u2.name = 'some name'
        a = Address(email_address='some other name')
        u2.addresses.append(a)
        assert u2 in s.dirty
        assert a in u2.addresses

        s.query(User).populate_existing().get(7)
        assert u2 not in s.dirty
        assert u2.name =='jack'
        assert a not in u2.addresses

    @testing.requires.unicode_connections
    def test_unicode(self):
        """test that Query.get properly sets up the type for the bind
        parameter. using unicode would normally fail on postgresql, mysql and
        oracle unless it is converted to an encoded string"""

        metadata = MetaData(engines.utf8_engine())
        table = Table('unicode_data', metadata,
            Column('id', Unicode(40), primary_key=True, test_needs_autoincrement=True),
            Column('data', Unicode(40)))
        try:
            metadata.create_all()
            # Py3K
            #ustring = b'petit voix m\xe2\x80\x99a'.decode('utf-8')
            # Py2K
            ustring = 'petit voix m\xe2\x80\x99a'.decode('utf-8')
            # end Py2K

            table.insert().execute(id=ustring, data=ustring)
            class LocalFoo(Base):
                pass
            mapper(LocalFoo, table)
            eq_(create_session().query(LocalFoo).get(ustring),
                              LocalFoo(id=ustring, data=ustring))
        finally:
            metadata.drop_all()

    def test_populate_existing(self):
        s = create_session()

        userlist = s.query(User).all()

        u = userlist[0]
        u.name = 'foo'
        a = Address(name='ed')
        u.addresses.append(a)

        self.assert_(a in u.addresses)

        s.query(User).populate_existing().all()

        self.assert_(u not in s.dirty)

        self.assert_(u.name == 'jack')

        self.assert_(a not in u.addresses)

        u.addresses[0].email_address = 'lala'
        u.orders[1].items[2].description = 'item 12'
        # test that lazy load doesnt change child items
        s.query(User).populate_existing().all()
        assert u.addresses[0].email_address == 'lala'
        assert u.orders[1].items[2].description == 'item 12'

        # eager load does
        s.query(User).options(joinedload('addresses'), joinedload_all('orders.items')).populate_existing().all()
        assert u.addresses[0].email_address == 'jack@bean.com'
        assert u.orders[1].items[2].description == 'item 5'

    @testing.fails_on_everything_except('sqlite', '+pyodbc', '+zxjdbc', 'mysql+oursql')
    def test_query_str(self):
        s = create_session()
        q = s.query(User).filter(User.id==1)
        eq_(
            str(q).replace('\n',''), 
            'SELECT users.id AS users_id, users.name AS users_name FROM users WHERE users.id = ?'
            )

class InvalidGenerationsTest(QueryTest, AssertsCompiledSQL):
    def test_no_limit_offset(self):
        s = create_session()

        for q in (
            s.query(User).limit(2),
            s.query(User).offset(2),
            s.query(User).limit(2).offset(2)
        ):
            assert_raises(sa_exc.InvalidRequestError, q.join, "addresses")

            assert_raises(sa_exc.InvalidRequestError, q.filter, User.name=='ed')

            assert_raises(sa_exc.InvalidRequestError, q.filter_by, name='ed')

            assert_raises(sa_exc.InvalidRequestError, q.order_by, 'foo')

            assert_raises(sa_exc.InvalidRequestError, q.group_by, 'foo')

            assert_raises(sa_exc.InvalidRequestError, q.having, 'foo')

            q.enable_assertions(False).join("addresses")
            q.enable_assertions(False).filter(User.name=='ed')
            q.enable_assertions(False).order_by('foo')
            q.enable_assertions(False).group_by('foo')

    def test_no_from(self):
        s = create_session()

        q = s.query(User).select_from(users)
        assert_raises(sa_exc.InvalidRequestError, q.select_from, users)

        q = s.query(User).join('addresses')
        assert_raises(sa_exc.InvalidRequestError, q.select_from, users)

        q = s.query(User).order_by(User.id)
        assert_raises(sa_exc.InvalidRequestError, q.select_from, users)

        assert_raises(sa_exc.InvalidRequestError, q.select_from, users)

        q.enable_assertions(False).select_from(users)

        # this is fine, however
        q.from_self()

    def test_invalid_select_from(self):
        s = create_session()
        q = s.query(User)
        assert_raises(sa_exc.ArgumentError, q.select_from, User.id==5)
        assert_raises(sa_exc.ArgumentError, q.select_from, User.id)

    def test_invalid_from_statement(self):
        s = create_session()
        q = s.query(User)
        assert_raises(sa_exc.ArgumentError, q.from_statement, User.id==5)
        assert_raises(sa_exc.ArgumentError, q.from_statement, users.join(addresses))

    def test_invalid_column(self):
        s = create_session()
        q = s.query(User)
        assert_raises(sa_exc.InvalidRequestError, q.add_column, object())

    def test_distinct(self):
        """test that a distinct() call is not valid before 'clauseelement' conditions."""

        s = create_session()
        q = s.query(User).distinct()
        assert_raises(sa_exc.InvalidRequestError, q.select_from, User)
        assert_raises(sa_exc.InvalidRequestError, q.from_statement, text("select * from table"))
        assert_raises(sa_exc.InvalidRequestError, q.with_polymorphic, User)

    def test_order_by(self):
        """test that an order_by() call is not valid before 'clauseelement' conditions."""

        s = create_session()
        q = s.query(User).order_by(User.id)
        assert_raises(sa_exc.InvalidRequestError, q.select_from, User)
        assert_raises(sa_exc.InvalidRequestError, q.from_statement, text("select * from table"))
        assert_raises(sa_exc.InvalidRequestError, q.with_polymorphic, User)

    def test_cancel_order_by(self):
        s = create_session()

        q = s.query(User).order_by(User.id)
        self.assert_compile(q, 
            "SELECT users.id AS users_id, users.name AS users_name FROM users ORDER BY users.id",
            use_default_dialect=True)

        assert_raises(sa_exc.InvalidRequestError, q._no_select_modifiers, "foo")

        q = q.order_by(None)
        self.assert_compile(q, 
                "SELECT users.id AS users_id, users.name AS users_name FROM users",
                use_default_dialect=True)

        assert_raises(sa_exc.InvalidRequestError, q._no_select_modifiers, "foo")

        q = q.order_by(False)
        self.assert_compile(q, 
                "SELECT users.id AS users_id, users.name AS users_name FROM users",
                use_default_dialect=True)

        # after False was set, this should pass
        q._no_select_modifiers("foo")

    def test_mapper_zero(self):
        s = create_session()

        q = s.query(User, Address)
        assert_raises(sa_exc.InvalidRequestError, q.get, 5)

    def test_from_statement(self):
        s = create_session()

        q = s.query(User).filter(User.id==5)
        assert_raises(sa_exc.InvalidRequestError, q.from_statement, "x")

        q = s.query(User).filter_by(id=5)
        assert_raises(sa_exc.InvalidRequestError, q.from_statement, "x")

        q = s.query(User).limit(5)
        assert_raises(sa_exc.InvalidRequestError, q.from_statement, "x")

        q = s.query(User).group_by(User.name)
        assert_raises(sa_exc.InvalidRequestError, q.from_statement, "x")

        q = s.query(User).order_by(User.name)
        assert_raises(sa_exc.InvalidRequestError, q.from_statement, "x")

class OperatorTest(QueryTest, AssertsCompiledSQL):
    """test sql.Comparator implementation for MapperProperties"""

    def _test(self, clause, expected):
        self.assert_compile(clause, expected, dialect=default.DefaultDialect())

    def test_arithmetic(self):
        create_session().query(User)
        for (py_op, sql_op) in ((operator.add, '+'), (operator.mul, '*'),
                                (operator.sub, '-'), 
                                # Py3k
                                #(operator.truediv, '/'),
                                # Py2K
                                (operator.div, '/'),
                                # end Py2K
                                ):
            for (lhs, rhs, res) in (
                (5, User.id, ':id_1 %s users.id'),
                (5, literal(6), ':param_1 %s :param_2'),
                (User.id, 5, 'users.id %s :id_1'),
                (User.id, literal('b'), 'users.id %s :param_1'),
                (User.id, User.id, 'users.id %s users.id'),
                (literal(5), 'b', ':param_1 %s :param_2'),
                (literal(5), User.id, ':param_1 %s users.id'),
                (literal(5), literal(6), ':param_1 %s :param_2'),
                ):
                self._test(py_op(lhs, rhs), res % sql_op)

    def test_comparison(self):
        create_session().query(User)
        ualias = aliased(User)

        for (py_op, fwd_op, rev_op) in ((operator.lt, '<', '>'),
                                        (operator.gt, '>', '<'),
                                        (operator.eq, '=', '='),
                                        (operator.ne, '!=', '!='),
                                        (operator.le, '<=', '>='),
                                        (operator.ge, '>=', '<=')):
            for (lhs, rhs, l_sql, r_sql) in (
                ('a', User.id, ':id_1', 'users.id'),
                ('a', literal('b'), ':param_2', ':param_1'), # note swap!
                (User.id, 'b', 'users.id', ':id_1'),
                (User.id, literal('b'), 'users.id', ':param_1'),
                (User.id, User.id, 'users.id', 'users.id'),
                (literal('a'), 'b', ':param_1', ':param_2'),
                (literal('a'), User.id, ':param_1', 'users.id'),
                (literal('a'), literal('b'), ':param_1', ':param_2'),
                (ualias.id, literal('b'), 'users_1.id', ':param_1'),
                (User.id, ualias.name, 'users.id', 'users_1.name'),
                (User.name, ualias.name, 'users.name', 'users_1.name'),
                (ualias.name, User.name, 'users_1.name', 'users.name'),
                ):

                # the compiled clause should match either (e.g.):
                # 'a' < 'b' -or- 'b' > 'a'.
                compiled = str(py_op(lhs, rhs).compile(dialect=default.DefaultDialect()))
                fwd_sql = "%s %s %s" % (l_sql, fwd_op, r_sql)
                rev_sql = "%s %s %s" % (r_sql, rev_op, l_sql)

                self.assert_(compiled == fwd_sql or compiled == rev_sql,
                             "\n'" + compiled + "'\n does not match\n'" +
                             fwd_sql + "'\n or\n'" + rev_sql + "'")

    def test_negated_null(self):
        self._test(User.id == None, "users.id IS NULL")
        self._test(~(User.id==None), "users.id IS NOT NULL")
        self._test(None == User.id, "users.id IS NULL")
        self._test(~(None == User.id), "users.id IS NOT NULL")
        self._test(Address.user == None, "addresses.user_id IS NULL")
        self._test(~(Address.user==None), "addresses.user_id IS NOT NULL")
        self._test(None == Address.user, "addresses.user_id IS NULL")
        self._test(~(None == Address.user), "addresses.user_id IS NOT NULL")

    def test_relationship(self):
        self._test(User.addresses.any(Address.id==17), 
                        "EXISTS (SELECT 1 "
                        "FROM addresses "
                        "WHERE users.id = addresses.user_id AND addresses.id = :id_1)"
                    )

        u7 = User(id=7)
        attributes.instance_state(u7).commit_all(attributes.instance_dict(u7))

        self._test(Address.user == u7, ":param_1 = addresses.user_id")

        self._test(Address.user != u7, "addresses.user_id != :user_id_1 OR addresses.user_id IS NULL")

        self._test(Address.user == None, "addresses.user_id IS NULL")

        self._test(Address.user != None, "addresses.user_id IS NOT NULL")

    def test_selfref_relationship(self):
        nalias = aliased(Node)

        # auto self-referential aliasing
        self._test(
            Node.children.any(Node.data=='n1'), 
                "EXISTS (SELECT 1 FROM nodes AS nodes_1 WHERE "
                "nodes.id = nodes_1.parent_id AND nodes_1.data = :data_1)"
        )

        # needs autoaliasing
        self._test(
            Node.children==None, 
            "NOT (EXISTS (SELECT 1 FROM nodes AS nodes_1 WHERE nodes.id = nodes_1.parent_id))"
        )

        self._test(
            Node.parent==None,
            "nodes.parent_id IS NULL"
        )

        self._test(
            nalias.parent==None,
            "nodes_1.parent_id IS NULL"
        )

        self._test(
            nalias.children==None, 
            "NOT (EXISTS (SELECT 1 FROM nodes WHERE nodes_1.id = nodes.parent_id))"
        )

        self._test(
                nalias.children.any(Node.data=='some data'), 
                "EXISTS (SELECT 1 FROM nodes WHERE "
                "nodes_1.id = nodes.parent_id AND nodes.data = :data_1)")

        # fails, but I think I want this to fail
        #self._test(
        #        Node.children.any(nalias.data=='some data'), 
        #        "EXISTS (SELECT 1 FROM nodes AS nodes_1 WHERE "
        #        "nodes.id = nodes_1.parent_id AND nodes_1.data = :data_1)"
        #        )

        self._test(
            nalias.parent.has(Node.data=='some data'), 
           "EXISTS (SELECT 1 FROM nodes WHERE nodes.id = nodes_1.parent_id AND nodes.data = :data_1)"
        )

        self._test(
            Node.parent.has(Node.data=='some data'), 
           "EXISTS (SELECT 1 FROM nodes AS nodes_1 WHERE nodes_1.id = nodes.parent_id AND nodes_1.data = :data_1)"
        )

        self._test(
            Node.parent == Node(id=7), 
            ":param_1 = nodes.parent_id"
        )

        self._test(
            nalias.parent == Node(id=7), 
            ":param_1 = nodes_1.parent_id"
        )

        self._test(
            nalias.parent != Node(id=7), 
            'nodes_1.parent_id != :parent_id_1 OR nodes_1.parent_id IS NULL'
        )

        self._test(
            nalias.children.contains(Node(id=7)), "nodes_1.id = :param_1"
        )

    def test_op(self):
        self._test(User.name.op('ilike')('17'), "users.name ilike :name_1")

    def test_in(self):
         self._test(User.id.in_(['a', 'b']),
                    "users.id IN (:id_1, :id_2)")

    def test_in_on_relationship_not_supported(self):
        assert_raises(NotImplementedError, Address.user.in_, [User(id=5)])

    def test_neg(self):
        self._test(-User.id, "-users.id")
        self._test(User.id + -User.id, "users.id + -users.id")

    def test_between(self):
        self._test(User.id.between('a', 'b'),
                   "users.id BETWEEN :id_1 AND :id_2")

    def test_selfref_between(self):
        ualias = aliased(User)
        self._test(User.id.between(ualias.id, ualias.id), "users.id BETWEEN users_1.id AND users_1.id")
        self._test(ualias.id.between(User.id, User.id), "users_1.id BETWEEN users.id AND users.id")

    def test_clauses(self):
        for (expr, compare) in (
            (func.max(User.id), "max(users.id)"),
            (User.id.desc(), "users.id DESC"),
            (between(5, User.id, Address.id), ":param_1 BETWEEN users.id AND addresses.id"),
            # this one would require adding compile() to InstrumentedScalarAttribute.  do we want this ?
            #(User.id, "users.id")
        ):
            c = expr.compile(dialect=default.DefaultDialect())
            assert str(c) == compare, "%s != %s" % (str(c), compare)


class ExpressionTest(QueryTest, AssertsCompiledSQL):

    def test_deferred_instances(self):
        session = create_session()
        s = session.query(User).filter(and_(addresses.c.email_address == bindparam('emailad'), 
                                        Address.user_id==User.id)).statement

        l = list(session.query(User).instances(s.execute(emailad = 'jack@bean.com')))
        eq_([User(id=7)], l)

    def test_aliased_sql_construct(self):
        j = join(User, Address)
        a1 = aliased(j)
        self.assert_compile(
            a1.select(),
            "SELECT anon_1.users_id, anon_1.users_name, anon_1.addresses_id, "
            "anon_1.addresses_user_id, anon_1.addresses_email_address "
            "FROM (SELECT users.id AS users_id, users.name AS users_name, "
            "addresses.id AS addresses_id, addresses.user_id AS "
            "addresses_user_id, addresses.email_address AS "
            "addresses_email_address FROM users JOIN addresses "
            "ON users.id = addresses.user_id) AS anon_1",
            use_default_dialect=True
        )

    def test_scalar_subquery(self):
        session = create_session()

        q = session.query(User.id).filter(User.id==7).subquery()

        q = session.query(User).filter(User.id==q)

        eq_(User(id=7), q.one())

    def test_named_subquery(self):
        session = create_session()
        a1 = session.query(User.id).filter(User.id==7).subquery('foo1')
        a2 = session.query(User.id).filter(User.id==7).subquery(name='foo2')
        a3 = session.query(User.id).filter(User.id==7).subquery()

        eq_(a1.name, 'foo1')
        eq_(a2.name, 'foo2')
        eq_(a3.name, '%%(%d anon)s' % id(a3))


    def test_label(self):
        session = create_session()

        q = session.query(User.id).filter(User.id==7).label('foo')
        self.assert_compile(
            session.query(q), 
            "SELECT (SELECT users.id FROM users WHERE users.id = :id_1) AS foo", 
            use_default_dialect=True
        )

    def test_as_scalar(self):
        session = create_session()

        q = session.query(User.id).filter(User.id==7).as_scalar()

        self.assert_compile(session.query(User).filter(User.id.in_(q)),
                            'SELECT users.id AS users_id, users.name '
                            'AS users_name FROM users WHERE users.id '
                            'IN (SELECT users.id FROM users WHERE '
                            'users.id = :id_1)',
                            use_default_dialect=True)


    def test_param_transfer(self):
        session = create_session()

        q = session.query(User.id).filter(User.id==bindparam('foo')).params(foo=7).subquery()

        q = session.query(User).filter(User.id==q)

        eq_(User(id=7), q.one())

    def test_in(self):
        session = create_session()
        s = session.query(User.id).join(User.addresses).group_by(User.id).having(func.count(Address.id) > 2)
        eq_(
            session.query(User).filter(User.id.in_(s)).all(),
            [User(id=8)]
        )

    def test_union(self):
        s = create_session()

        q1 = s.query(User).filter(User.name=='ed').with_labels()
        q2 = s.query(User).filter(User.name=='fred').with_labels()
        eq_(
            s.query(User).from_statement(union(q1, q2).order_by('users_name')).all(),
            [User(name='ed'), User(name='fred')]
        )

    def test_select(self):
        s = create_session()

        # this is actually not legal on most DBs since the subquery has no alias
        q1 = s.query(User).filter(User.name=='ed')


        self.assert_compile(
            select([q1]),
            "SELECT users_id, users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users WHERE users.name = :name_1)",
            dialect=default.DefaultDialect()
        )

    def test_join(self):
        s = create_session()

        # TODO: do we want aliased() to detect a query and convert to subquery() 
        # automatically ?
        q1 = s.query(Address).filter(Address.email_address=='jack@bean.com')
        adalias = aliased(Address, q1.subquery())
        eq_(
            s.query(User, adalias).join(adalias, User.id==adalias.user_id).all(),
            [(User(id=7,name=u'jack'), Address(email_address=u'jack@bean.com',user_id=7,id=1))]
        )

# more slice tests are available in test/orm/generative.py
class SliceTest(QueryTest):
    def test_first(self):
        assert  User(id=7) == create_session().query(User).first()

        assert create_session().query(User).filter(User.id==27).first() is None

    @testing.only_on('sqlite', 'testing execution but db-specific syntax')
    def test_limit_offset_applies(self):
        """Test that the expected LIMIT/OFFSET is applied for slices.

        The LIMIT/OFFSET syntax differs slightly on all databases, and
        query[x:y] executes immediately, so we are asserting against
        SQL strings using sqlite's syntax.

        """
        sess = create_session()
        q = sess.query(User)

        self.assert_sql(testing.db, lambda: q[10:20], [
            ("SELECT users.id AS users_id, users.name AS users_name FROM users LIMIT :param_1 OFFSET :param_2", {'param_1':10, 'param_2':10})
        ])

        self.assert_sql(testing.db, lambda: q[:20], [
            ("SELECT users.id AS users_id, users.name AS users_name FROM users LIMIT :param_1 OFFSET :param_2", {'param_1':20, 'param_2':0})
        ])

        self.assert_sql(testing.db, lambda: q[5:], [
            ("SELECT users.id AS users_id, users.name AS users_name FROM users LIMIT :param_1 OFFSET :param_2", {'param_1':-1, 'param_2':5})
        ])

        self.assert_sql(testing.db, lambda: q[2:2], [])

        self.assert_sql(testing.db, lambda: q[-2:-5], [])

        self.assert_sql(testing.db, lambda: q[-5:-2], [
            ("SELECT users.id AS users_id, users.name AS users_name FROM users", {})
        ])

        self.assert_sql(testing.db, lambda: q[-5:], [
            ("SELECT users.id AS users_id, users.name AS users_name FROM users", {})
        ])

        self.assert_sql(testing.db, lambda: q[:], [
            ("SELECT users.id AS users_id, users.name AS users_name FROM users", {})
        ])



class FilterTest(QueryTest):
    def test_basic(self):
        assert [User(id=7), User(id=8), User(id=9),User(id=10)] == create_session().query(User).all()

    @testing.fails_on('maxdb', 'FIXME: unknown')
    def test_limit(self):
        assert [User(id=8), User(id=9)] == create_session().query(User).order_by(User.id).limit(2).offset(1).all()

        assert [User(id=8), User(id=9)] == list(create_session().query(User).order_by(User.id)[1:3])

        assert User(id=8) == create_session().query(User).order_by(User.id)[1]

        assert [] == create_session().query(User).order_by(User.id)[3:3]
        assert [] == create_session().query(User).order_by(User.id)[0:0]

    @testing.requires.boolean_col_expressions
    def test_exists(self):
        sess = create_session(testing.db)

        assert sess.query(exists().where(User.id==9)).scalar()
        assert not sess.query(exists().where(User.id==29)).scalar()

    def test_one_filter(self):
        assert [User(id=8), User(id=9)] == create_session().query(User).filter(User.name.endswith('ed')).all()

    def test_contains(self):
        """test comparing a collection to an object instance."""

        sess = create_session()
        address = sess.query(Address).get(3)
        assert [User(id=8)] == sess.query(User).filter(User.addresses.contains(address)).all()

        try:
            sess.query(User).filter(User.addresses == address)
            assert False
        except sa_exc.InvalidRequestError:
            assert True

        assert [User(id=10)] == sess.query(User).filter(User.addresses==None).all()

        try:
            assert [User(id=7), User(id=9), User(id=10)] == sess.query(User).filter(User.addresses!=address).all()
            assert False
        except sa_exc.InvalidRequestError:
            assert True

        #assert [User(id=7), User(id=9), User(id=10)] == sess.query(User).filter(User.addresses!=address).all()

    def test_any(self):
        sess = create_session()

        assert [User(id=8), User(id=9)] == sess.query(User).filter(User.addresses.any(Address.email_address.like('%ed%'))).all()

        assert [User(id=8)] == sess.query(User).filter(User.addresses.any(Address.email_address.like('%ed%'), id=4)).all()

        assert [User(id=8)] == sess.query(User).filter(User.addresses.any(Address.email_address.like('%ed%'))).\
            filter(User.addresses.any(id=4)).all()

        assert [User(id=9)] == sess.query(User).filter(User.addresses.any(email_address='fred@fred.com')).all()

        # test that any() doesn't overcorrelate
        assert [User(id=7), User(id=8)] == sess.query(User).join("addresses").filter(~User.addresses.any(Address.email_address=='fred@fred.com')).all()

        # test that the contents are not adapted by the aliased join
        assert [User(id=7), User(id=8)] == sess.query(User).join("addresses", aliased=True).filter(~User.addresses.any(Address.email_address=='fred@fred.com')).all()

        assert [User(id=10)] == sess.query(User).outerjoin("addresses", aliased=True).filter(~User.addresses.any()).all()

    @testing.crashes('maxdb', 'can dump core')
    def test_has(self):
        sess = create_session()
        assert [Address(id=5)] == sess.query(Address).filter(Address.user.has(name='fred')).all()

        assert [Address(id=2), Address(id=3), Address(id=4), Address(id=5)] == \
                sess.query(Address).filter(Address.user.has(User.name.like('%ed%'))).order_by(Address.id).all()

        assert [Address(id=2), Address(id=3), Address(id=4)] == \
            sess.query(Address).filter(Address.user.has(User.name.like('%ed%'), id=8)).order_by(Address.id).all()

        # test has() doesn't overcorrelate
        assert [Address(id=2), Address(id=3), Address(id=4)] == \
            sess.query(Address).join("user").filter(Address.user.has(User.name.like('%ed%'), id=8)).order_by(Address.id).all()

        # test has() doesnt' get subquery contents adapted by aliased join
        assert [Address(id=2), Address(id=3), Address(id=4)] == \
            sess.query(Address).join("user", aliased=True).filter(Address.user.has(User.name.like('%ed%'), id=8)).order_by(Address.id).all()

        dingaling = sess.query(Dingaling).get(2)
        assert [User(id=9)] == sess.query(User).filter(User.addresses.any(Address.dingaling==dingaling)).all()

    def test_contains_m2m(self):
        sess = create_session()
        item = sess.query(Item).get(3)
        assert [Order(id=1), Order(id=2), Order(id=3)] == sess.query(Order).filter(Order.items.contains(item)).all()

        assert [Order(id=4), Order(id=5)] == sess.query(Order).filter(~Order.items.contains(item)).all()

        item2 = sess.query(Item).get(5)
        assert [Order(id=3)] == sess.query(Order).filter(Order.items.contains(item)).filter(Order.items.contains(item2)).all()


    def test_comparison(self):
        """test scalar comparison to an object instance"""

        sess = create_session()
        user = sess.query(User).get(8)
        assert [Address(id=2), Address(id=3), Address(id=4)] == sess.query(Address).filter(Address.user==user).all()

        assert [Address(id=1), Address(id=5)] == sess.query(Address).filter(Address.user!=user).all()

        # generates an IS NULL
        assert [] == sess.query(Address).filter(Address.user == None).all()
        assert [] == sess.query(Address).filter(Address.user == null()).all()

        assert [Order(id=5)] == sess.query(Order).filter(Order.address == None).all()

        # o2o
        dingaling = sess.query(Dingaling).get(2)
        assert [Address(id=5)] == sess.query(Address).filter(Address.dingaling==dingaling).all()

        # m2m
        eq_(sess.query(Item).filter(Item.keywords==None).order_by(Item.id).all(), [Item(id=4), Item(id=5)])
        eq_(sess.query(Item).filter(Item.keywords!=None).order_by(Item.id).all(), [Item(id=1),Item(id=2), Item(id=3)])

    def test_filter_by(self):
        sess = create_session()
        user = sess.query(User).get(8)
        assert [Address(id=2), Address(id=3), Address(id=4)] == sess.query(Address).filter_by(user=user).all()

        # many to one generates IS NULL
        assert [] == sess.query(Address).filter_by(user = None).all()
        assert [] == sess.query(Address).filter_by(user = null()).all()

        # one to many generates WHERE NOT EXISTS
        assert [User(name='chuck')] == sess.query(User).filter_by(addresses = None).all()
        assert [User(name='chuck')] == sess.query(User).filter_by(addresses = null()).all()

    def test_none_comparison(self):
        sess = create_session()

        # scalar
        eq_(
            [Order(description="order 5")],
            sess.query(Order).filter(Order.address_id==None).all()
        )
        eq_(
            [Order(description="order 5")],
            sess.query(Order).filter(Order.address_id==null()).all()
        )

        # o2o
        eq_([Address(id=1), Address(id=3), Address(id=4)], 
            sess.query(Address).filter(Address.dingaling==None).order_by(Address.id).all())
        eq_([Address(id=1), Address(id=3), Address(id=4)], 
            sess.query(Address).filter(Address.dingaling==null()).order_by(Address.id).all())
        eq_([Address(id=2), Address(id=5)], sess.query(Address).filter(Address.dingaling != None).order_by(Address.id).all())
        eq_([Address(id=2), Address(id=5)], sess.query(Address).filter(Address.dingaling != null()).order_by(Address.id).all())

        # m2o
        eq_([Order(id=5)], sess.query(Order).filter(Order.address==None).all())
        eq_([Order(id=1), Order(id=2), Order(id=3), Order(id=4)], sess.query(Order).order_by(Order.id).filter(Order.address!=None).all())

        # o2m
        eq_([User(id=10)], sess.query(User).filter(User.addresses==None).all())
        eq_([User(id=7),User(id=8),User(id=9)], sess.query(User).filter(User.addresses!=None).order_by(User.id).all())

    def test_blank_filter_by(self):
        eq_(
            [(7,), (8,), (9,), (10,)],
            create_session().query(User.id).filter_by().order_by(User.id).all()
        )
        eq_(
            [(7,), (8,), (9,), (10,)],
            create_session().query(User.id).filter_by(**{}).order_by(User.id).all()
        )



class SetOpsTest(QueryTest, AssertsCompiledSQL):

    def test_union(self):
        s = create_session()

        fred = s.query(User).filter(User.name=='fred')
        ed = s.query(User).filter(User.name=='ed')
        jack = s.query(User).filter(User.name=='jack')

        eq_(fred.union(ed).order_by(User.name).all(), 
            [User(name='ed'), User(name='fred')]
        )

        eq_(fred.union(ed, jack).order_by(User.name).all(), 
            [User(name='ed'), User(name='fred'), User(name='jack')]
        )

    def test_statement_labels(self):
        """test that label conflicts don't occur with joins etc."""

        s = create_session()
        q1 = s.query(User, Address).join(User.addresses).\
                                    filter(Address.email_address=="ed@wood.com")
        q2 = s.query(User, Address).join(User.addresses).\
                                    filter(Address.email_address=="jack@bean.com")
        q3 = q1.union(q2).order_by(User.name)

        eq_(
            q3.all(),
            [
                (User(name='ed'), Address(email_address="ed@wood.com")),
                (User(name='jack'), Address(email_address="jack@bean.com")),
            ]
        )

    def test_union_literal_expressions_compile(self):
        """test that column expressions translate during 
            the _from_statement() portion of union(), others"""

        s = Session()
        q1 = s.query(User, literal("x"))
        q2 = s.query(User, literal_column("'y'"))
        q3 = q1.union(q2)

        self.assert_compile(
            q3,
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name AS anon_1_users_name,"
            " anon_1.anon_2 AS anon_1_anon_2 FROM (SELECT users.id AS users_id, users.name AS"
            " users_name, :param_1 AS anon_2 FROM users UNION SELECT users.id AS users_id, "
            "users.name AS users_name, 'y' FROM users) AS anon_1"
            , use_default_dialect = True
        )

    def test_union_literal_expressions_results(self):
        s = Session()

        q1 = s.query(User, literal("x"))
        q2 = s.query(User, literal_column("'y'"))
        q3 = q1.union(q2)

        q4 = s.query(User, literal_column("'x'").label('foo'))
        q5 = s.query(User, literal("y"))
        q6 = q4.union(q5)

        eq_(
            [x['name'] for x in q6.column_descriptions],
            ['User', 'foo']
        )

        for q in (q3.order_by(User.id, "anon_1_anon_2"), q6.order_by(User.id, "foo")):
            eq_(q.all(),
                [
                    (User(id=7, name=u'jack'), u'x'), 
                    (User(id=7, name=u'jack'), u'y'), 
                    (User(id=8, name=u'ed'), u'x'), 
                    (User(id=8, name=u'ed'), u'y'), 
                    (User(id=9, name=u'fred'), u'x'), 
                    (User(id=9, name=u'fred'), u'y'), 
                    (User(id=10, name=u'chuck'), u'x'), 
                    (User(id=10, name=u'chuck'), u'y')
                ]
            )

    def test_union_labeled_anonymous_columns(self):
        s = Session()

        c1, c2 = column('c1'), column('c2')
        q1 = s.query(User, c1.label('foo'), c1.label('bar'))
        q2 = s.query(User, c1.label('foo'), c2.label('bar'))
        q3 = q1.union(q2)

        eq_(
            [x['name'] for x in q3.column_descriptions],
            ['User', 'foo', 'bar']
        )

        self.assert_compile(
            q3,
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "anon_1.foo AS anon_1_foo, anon_1.bar AS anon_1_bar "
            "FROM (SELECT users.id AS users_id, users.name AS users_name, "
            "c1 AS foo, c1 AS bar FROM users UNION SELECT users.id AS "
            "users_id, users.name AS users_name, c1 AS foo, c2 AS bar "
            "FROM users) AS anon_1",
            use_default_dialect=True
        )

    def test_union_mapped_colnames_preserved_across_subquery(self):
        s = Session()
        q1 = s.query(User.name)
        q2 = s.query(User.name)

        # the label names in the subquery are the typical anonymized ones
        self.assert_compile(
            q1.union(q2),
            "SELECT anon_1.users_name AS anon_1_users_name "
            "FROM (SELECT users.name AS users_name FROM users "
            "UNION SELECT users.name AS users_name FROM users) AS anon_1",
            use_default_dialect=True
        )

        # but in the returned named tuples,
        # due to [ticket:1942], this should be 'name', not 'users_name'
        eq_(
            [x['name'] for x in q1.union(q2).column_descriptions],
            ['name']
        )


    @testing.fails_on('mysql', "mysql doesn't support intersect")
    def test_intersect(self):
        s = create_session()

        fred = s.query(User).filter(User.name=='fred')
        ed = s.query(User).filter(User.name=='ed')
        jack = s.query(User).filter(User.name=='jack')
        eq_(fred.intersect(ed, jack).all(), 
            []
        )

        eq_(fred.union(ed).intersect(ed.union(jack)).all(), 
            [User(name='ed')]
        )

    def test_eager_load(self):
        s = create_session()

        fred = s.query(User).filter(User.name=='fred')
        ed = s.query(User).filter(User.name=='ed')
        jack = s.query(User).filter(User.name=='jack')

        def go():
            eq_(
                fred.union(ed).order_by(User.name).options(joinedload(User.addresses)).all(), 
                [
                    User(name='ed', addresses=[Address(), Address(), Address()]), 
                    User(name='fred', addresses=[Address()])
                ]
            )
        self.assert_sql_count(testing.db, go, 1)


class AggregateTest(QueryTest):

    def test_sum(self):
        sess = create_session()
        orders = sess.query(Order).filter(Order.id.in_([2, 3, 4]))
        eq_(orders.values(func.sum(Order.user_id * Order.address_id)).next(), (79,))
        eq_(orders.value(func.sum(Order.user_id * Order.address_id)), 79)

    def test_apply(self):
        sess = create_session()
        assert sess.query(func.sum(Order.user_id * Order.address_id)).filter(Order.id.in_([2, 3, 4])).one() == (79,)

    def test_having(self):
        sess = create_session()
        assert [User(name=u'ed',id=8)] == sess.query(User).order_by(User.id).group_by(User).join('addresses').having(func.count(Address.id)> 2).all()

        assert [User(name=u'jack',id=7), User(name=u'fred',id=9)] == sess.query(User).order_by(User.id).group_by(User).join('addresses').having(func.count(Address.id)< 2).all()

class CountTest(QueryTest):
    def test_basic(self):
        s = create_session()

        eq_(s.query(User).count(), 4)

        eq_(s.query(User).filter(users.c.name.endswith('ed')).count(), 2)

    def test_multiple_entity(self):
        s = create_session()
        q = s.query(User, Address)
        eq_(q.count(), 20)  # cartesian product

        q = s.query(User, Address).join(User.addresses)
        eq_(q.count(), 5)

    def test_nested(self):
        s = create_session()
        q = s.query(User, Address).limit(2)
        eq_(q.count(), 2)

        q = s.query(User, Address).limit(100)
        eq_(q.count(), 20)

        q = s.query(User, Address).join(User.addresses).limit(100)
        eq_(q.count(), 5)

    def test_cols(self):
        """test that column-based queries always nest."""

        s = create_session()

        q = s.query(func.count(distinct(User.name)))
        eq_(q.count(), 1)

        q = s.query(func.count(distinct(User.name))).distinct()
        eq_(q.count(), 1)

        q = s.query(User.name)
        eq_(q.count(), 4)

        q = s.query(User.name, Address)
        eq_(q.count(), 20)

        q = s.query(Address.user_id)
        eq_(q.count(), 5)
        eq_(q.distinct().count(), 3)


class DistinctTest(QueryTest):
    def test_basic(self):
        eq_(
            [User(id=7), User(id=8), User(id=9),User(id=10)],
            create_session().query(User).order_by(User.id).distinct().all()
        )
        eq_(
            [User(id=7), User(id=9), User(id=8),User(id=10)], 
            create_session().query(User).distinct().order_by(desc(User.name)).all()
        ) 

    def test_joined(self):
        """test that orderbys from a joined table get placed into the columns clause when DISTINCT is used"""

        sess = create_session()
        q = sess.query(User).join('addresses').distinct().order_by(desc(Address.email_address))

        assert [User(id=7), User(id=9), User(id=8)] == q.all()

        sess.expunge_all()

        # test that it works on embedded joinedload/LIMIT subquery
        q = sess.query(User).join('addresses').distinct().options(joinedload('addresses')).order_by(desc(Address.email_address)).limit(2)

        def go():
            assert [
                User(id=7, addresses=[
                    Address(id=1)
                ]),
                User(id=9, addresses=[
                    Address(id=5)
                ]),
            ] == q.all()
        self.assert_sql_count(testing.db, go, 1)


class YieldTest(QueryTest):
    def test_basic(self):
        sess = create_session()
        q = iter(sess.query(User).yield_per(1).from_statement("select * from users"))

        ret = []
        eq_(len(sess.identity_map), 0)
        ret.append(q.next())
        ret.append(q.next())
        eq_(len(sess.identity_map), 2)
        ret.append(q.next())
        ret.append(q.next())
        eq_(len(sess.identity_map), 4)
        try:
            q.next()
            assert False
        except StopIteration:
            pass

class HintsTest(QueryTest, AssertsCompiledSQL):
    def test_hints(self):
        from sqlalchemy.dialects import mysql
        dialect = mysql.dialect()

        sess = create_session()

        self.assert_compile(
            sess.query(User).with_hint(User, 'USE INDEX (col1_index,col2_index)'),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users USE INDEX (col1_index,col2_index)",
            dialect=dialect
        )

        self.assert_compile(
            sess.query(User).with_hint(User, 'WITH INDEX col1_index', 'sybase'),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users",
            dialect=dialect
        )

        ualias = aliased(User)
        self.assert_compile(
            sess.query(User, ualias).with_hint(ualias, 'USE INDEX (col1_index,col2_index)').
                join(ualias, ualias.id > User.id),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users INNER JOIN users AS users_1 USE INDEX (col1_index,col2_index) "
            "ON users.id < users_1.id",
            dialect=dialect
        )


class TextTest(QueryTest):
    def test_fulltext(self):
        assert [User(id=7), User(id=8), User(id=9),User(id=10)] == create_session().query(User).from_statement("select * from users order by id").all()

        assert User(id=7) == create_session().query(User).from_statement("select * from users order by id").first()
        assert None == create_session().query(User).from_statement("select * from users where name='nonexistent'").first()

    def test_fragment(self):
        assert [User(id=8), User(id=9)] == create_session().query(User).filter("id in (8, 9)").all()

        assert [User(id=9)] == create_session().query(User).filter("name='fred'").filter("id=9").all()

        assert [User(id=9)] == create_session().query(User).filter("name='fred'").filter(User.id==9).all()

    def test_binds(self):
        assert [User(id=8), User(id=9)] == create_session().query(User).filter("id in (:id1, :id2)").params(id1=8, id2=9).all()

    def test_as_column(self):
        s = create_session()
        assert_raises(sa_exc.InvalidRequestError, s.query, User.id, text("users.name"))

        eq_(s.query(User.id, "name").order_by(User.id).all(), [(7, u'jack'), (8, u'ed'), (9, u'fred'), (10, u'chuck')])

class ParentTest(QueryTest):
    def test_o2m(self):
        sess = create_session()
        q = sess.query(User)

        u1 = q.filter_by(name='jack').one()

        # test auto-lookup of property
        o = sess.query(Order).with_parent(u1).all()
        assert [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")] == o

        # test with explicit property
        o = sess.query(Order).with_parent(u1, property='orders').all()
        assert [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")] == o

        o = sess.query(Order).with_parent(u1, property=User.orders).all()
        assert [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")] == o

        o = sess.query(Order).filter(with_parent(u1, User.orders)).all()
        assert [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")] == o

        # test generative criterion
        o = sess.query(Order).with_parent(u1).filter(orders.c.id>2).all()
        assert [Order(description="order 3"), Order(description="order 5")] == o

        # test against None for parent? this can't be done with the current API since we don't know
        # what mapper to use
        #assert sess.query(Order).with_parent(None, property='addresses').all() == [Order(description="order 5")]

    def test_noparent(self):
        sess = create_session()
        q = sess.query(User)

        u1 = q.filter_by(name='jack').one()

        try:
            q = sess.query(Item).with_parent(u1)
            assert False
        except sa_exc.InvalidRequestError, e:
            assert str(e) \
                == "Could not locate a property which relates "\
                "instances of class 'Item' to instances of class 'User'"

    def test_m2m(self):
        sess = create_session()
        i1 = sess.query(Item).filter_by(id=2).one()
        k = sess.query(Keyword).with_parent(i1).all()
        assert [Keyword(name='red'), Keyword(name='small'), Keyword(name='square')] == k

    def test_with_transient(self):
        sess = Session()

        q = sess.query(User)
        u1 = q.filter_by(name='jack').one()
        utrans = User(id=u1.id)
        o = sess.query(Order).with_parent(utrans, 'orders')
        eq_(
            [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")],
            o.all()
        )

        o = sess.query(Order).filter(with_parent(utrans, 'orders'))
        eq_(
            [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")],
            o.all()
        )

    def test_with_pending_autoflush(self):
        sess = Session()

        o1 = sess.query(Order).first()
        opending = Order(id=20, user_id=o1.user_id)
        sess.add(opending)
        eq_(
            sess.query(User).with_parent(opending, 'user').one(),
            User(id=o1.user_id)
        )
        eq_(
            sess.query(User).filter(with_parent(opending, 'user')).one(),
            User(id=o1.user_id)
        )

    def test_with_pending_no_autoflush(self):
        sess = Session(autoflush=False)

        o1 = sess.query(Order).first()
        opending = Order(user_id=o1.user_id)
        sess.add(opending)
        eq_(
            sess.query(User).with_parent(opending, 'user').one(),
            User(id=o1.user_id)
        )




class SynonymTest(QueryTest):

    @classmethod
    def setup_mappers(cls):
        mapper(User, users, properties={
            'name_syn':synonym('name'),
            'addresses':relationship(Address),
            'orders':relationship(Order, backref='user'), # o2m, m2o
            'orders_syn':synonym('orders')
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items':relationship(Item, secondary=order_items),  #m2m
            'address':relationship(Address),  # m2o
            'items_syn':synonym('items')
        })
        mapper(Item, items, properties={
            'keywords':relationship(Keyword, secondary=item_keywords) #m2m
        })
        mapper(Keyword, keywords)

    @testing.fails_if(lambda: True, "0.7 regression, may not support "
                                "synonyms for relationship")
    def test_joins(self):
        for j in (
            ['orders', 'items'],
            ['orders_syn', 'items'],
            ['orders', 'items_syn'],
            ['orders_syn', 'items_syn'],
        ):
            result = create_session().query(User).join(*j).filter_by(id=3).all()
            assert [User(id=7, name='jack'), User(id=9, name='fred')] == result

    @testing.fails_if(lambda: True, "0.7 regression, may not support "
                                "synonyms for relationship")
    def test_with_parent(self):
        for nameprop, orderprop in (
            ('name', 'orders'),
            ('name_syn', 'orders'),
            ('name', 'orders_syn'),
            ('name_syn', 'orders_syn'),
        ):
            sess = create_session()
            q = sess.query(User)

            u1 = q.filter_by(**{nameprop:'jack'}).one()

            o = sess.query(Order).with_parent(u1, property=orderprop).all()
            assert [Order(description="order 1"), Order(description="order 3"), Order(description="order 5")] == o


class ImmediateTest(_fixtures.FixtureTest):
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(Address, addresses)

        mapper(User, users, properties=dict(
            addresses=relationship(Address)))

    @testing.resolve_artifact_names
    def test_one(self):
        sess = create_session()

        assert_raises(sa.orm.exc.NoResultFound,
                          sess.query(User).filter(User.id == 99).one)

        eq_(sess.query(User).filter(User.id == 7).one().id, 7)

        assert_raises(sa.orm.exc.MultipleResultsFound,
                          sess.query(User).one)

        assert_raises(
            sa.orm.exc.NoResultFound,
            sess.query(User.id, User.name).filter(User.id == 99).one)

        eq_(sess.query(User.id, User.name).filter(User.id == 7).one(),
            (7, 'jack'))

        assert_raises(sa.orm.exc.MultipleResultsFound,
                          sess.query(User.id, User.name).one)

        assert_raises(sa.orm.exc.NoResultFound,
                          (sess.query(User, Address).
                           join(User.addresses).
                           filter(Address.id == 99)).one)

        eq_((sess.query(User, Address).
            join(User.addresses).
            filter(Address.id == 4)).one(),
           (User(id=8), Address(id=4)))

        assert_raises(sa.orm.exc.MultipleResultsFound,
                         sess.query(User, Address).join(User.addresses).one)

        # this result returns multiple rows, the first
        # two rows being the same.  but uniquing is 
        # not applied for a column based result.
        assert_raises(sa.orm.exc.MultipleResultsFound,
                       sess.query(User.id).
                       join(User.addresses).
                       filter(User.id.in_([8, 9])).
                       order_by(User.id).
                       one)

        # test that a join which ultimately returns 
        # multiple identities across many rows still 
        # raises, even though the first two rows are of 
        # the same identity and unique filtering 
        # is applied ([ticket:1688])
        assert_raises(sa.orm.exc.MultipleResultsFound,
                        sess.query(User).
                        join(User.addresses).
                        filter(User.id.in_([8, 9])).
                        order_by(User.id).
                        one)


    @testing.future
    def test_getslice(self):
        assert False

    @testing.resolve_artifact_names
    def test_scalar(self):
        sess = create_session()

        eq_(sess.query(User.id).filter_by(id=7).scalar(), 7)
        eq_(sess.query(User.id, User.name).filter_by(id=7).scalar(), 7)
        eq_(sess.query(User.id).filter_by(id=0).scalar(), None)
        eq_(sess.query(User).filter_by(id=7).scalar(),
            sess.query(User).filter_by(id=7).one())

        assert_raises(sa.orm.exc.MultipleResultsFound, sess.query(User).scalar)
        assert_raises(sa.orm.exc.MultipleResultsFound, sess.query(User.id, User.name).scalar)

    @testing.resolve_artifact_names
    def test_value(self):
        sess = create_session()

        eq_(sess.query(User).filter_by(id=7).value(User.id), 7)
        eq_(sess.query(User.id, User.name).filter_by(id=7).value(User.id), 7)
        eq_(sess.query(User).filter_by(id=0).value(User.id), None)

        sess.bind = testing.db
        eq_(sess.query().value(sa.literal_column('1').label('x')), 1)

class UpdateDeleteTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('name', String(32)),
              Column('age', Integer))

        Table('documents', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('user_id', None, ForeignKey('users.id')),
              Column('title', String(32)))

    @classmethod
    def setup_classes(cls):
        class User(_base.ComparableEntity):
            pass

        class Document(_base.ComparableEntity):
            pass

    @classmethod
    @testing.resolve_artifact_names
    def insert_data(cls):
        users.insert().execute([
            dict(id=1, name='john', age=25),
            dict(id=2, name='jack', age=47),
            dict(id=3, name='jill', age=29),
            dict(id=4, name='jane', age=37),
        ])

    @testing.resolve_artifact_names
    def insert_documents(self):
        documents.insert().execute([
            dict(id=1, user_id=1, title='foo'),
            dict(id=2, user_id=1, title='bar'),
            dict(id=3, user_id=2, title='baz'),
        ])

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(User, users)
        mapper(Document, documents, properties={
            'user': relationship(User, lazy='joined', 
                        backref=backref('documents', lazy='select'))
        })

    @testing.resolve_artifact_names
    def test_illegal_operations(self):
        s = create_session()

        for q, mname in (
            (s.query(User).limit(2), "limit"),
            (s.query(User).offset(2), "offset"),
            (s.query(User).limit(2).offset(2), "limit"),
            (s.query(User).order_by(User.id), "order_by"),
            (s.query(User).group_by(User.id), "group_by"),
            (s.query(User).distinct(), "distinct")
        ):
            assert_raises_message(sa_exc.InvalidRequestError, r"Can't call Query.update\(\) when %s\(\) has been called" % mname, q.update, {'name':'ed'})
            assert_raises_message(sa_exc.InvalidRequestError, r"Can't call Query.delete\(\) when %s\(\) has been called" % mname, q.delete)


    @testing.resolve_artifact_names
    def test_delete(self):
        sess = create_session(bind=testing.db, autocommit=False)

        john,jack,jill,jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(or_(User.name == 'john', User.name == 'jill')).delete()

        assert john not in sess and jill not in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack,jane])

    @testing.resolve_artifact_names
    def test_delete_with_bindparams(self):
        sess = create_session(bind=testing.db, autocommit=False)

        john,jack,jill,jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter('name = :name').params(name='john').delete('fetch')
        assert john not in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack,jill,jane])

    @testing.resolve_artifact_names
    def test_delete_rollback(self):
        sess = sessionmaker()()
        john,jack,jill,jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(or_(User.name == 'john', User.name == 'jill')).delete(synchronize_session='evaluate')
        assert john not in sess and jill not in sess
        sess.rollback()
        assert john in sess and jill in sess

    @testing.resolve_artifact_names
    def test_delete_rollback_with_fetch(self):
        sess = sessionmaker()()
        john,jack,jill,jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(or_(User.name == 'john', User.name == 'jill')).delete(synchronize_session='fetch')
        assert john not in sess and jill not in sess
        sess.rollback()
        assert john in sess and jill in sess

    @testing.resolve_artifact_names
    def test_delete_without_session_sync(self):
        sess = create_session(bind=testing.db, autocommit=False)

        john,jack,jill,jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(or_(User.name == 'john', User.name == 'jill')).delete(synchronize_session=False)

        assert john in sess and jill in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack,jane])

    @testing.resolve_artifact_names
    def test_delete_with_fetch_strategy(self):
        sess = create_session(bind=testing.db, autocommit=False)

        john,jack,jill,jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(or_(User.name == 'john', User.name == 'jill')).delete(synchronize_session='fetch')

        assert john not in sess and jill not in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack,jane])

    @testing.fails_on('mysql', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_delete_invalid_evaluation(self):
        sess = create_session(bind=testing.db, autocommit=False)

        john,jack,jill,jane = sess.query(User).order_by(User.id).all()

        assert_raises(sa_exc.InvalidRequestError,
            sess.query(User).filter(User.name == select([func.max(User.name)])).delete, synchronize_session='evaluate'
        )

        sess.query(User).filter(User.name == select([func.max(User.name)])).delete(synchronize_session='fetch')

        assert john not in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack,jill,jane])

    @testing.resolve_artifact_names
    def test_update(self):
        sess = create_session(bind=testing.db, autocommit=False)

        john,jack,jill,jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(User.age > 29).update({'age': User.age - 10}, synchronize_session='evaluate')

        eq_([john.age, jack.age, jill.age, jane.age], [25,37,29,27])
        eq_(sess.query(User.age).order_by(User.id).all(), zip([25,37,29,27]))

        sess.query(User).filter(User.age > 29).update({User.age: User.age - 10}, synchronize_session='evaluate')
        eq_([john.age, jack.age, jill.age, jane.age], [25,27,29,27])
        eq_(sess.query(User.age).order_by(User.id).all(), zip([25,27,29,27]))

        sess.query(User).filter(User.age > 27).update({users.c.age: User.age - 10}, synchronize_session='evaluate')
        eq_([john.age, jack.age, jill.age, jane.age], [25,27,19,27])
        eq_(sess.query(User.age).order_by(User.id).all(), zip([25,27,19,27]))

        sess.query(User).filter(User.age == 25).update({User.age: User.age - 10}, synchronize_session='fetch')
        eq_([john.age, jack.age, jill.age, jane.age], [15,27,19,27])
        eq_(sess.query(User.age).order_by(User.id).all(), zip([15,27,19,27]))

    @testing.resolve_artifact_names
    @testing.provide_metadata
    def test_update_attr_names(self):
        data = Table('data', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('counter', Integer, nullable=False, default=0)
        )
        class Data(_base.ComparableEntity):
            pass

        mapper(Data, data, properties={'cnt':data.c.counter})
        metadata.create_all()
        d1 = Data()
        sess = Session()
        sess.add(d1)
        sess.commit()
        eq_(d1.cnt, 0)

        sess.query(Data).update({Data.cnt:Data.cnt + 1})
        sess.flush()

        eq_(d1.cnt, 1)

        sess.query(Data).update({Data.cnt:Data.cnt + 1}, 'fetch')
        sess.flush()

        eq_(d1.cnt, 2)
        sess.close()

    @testing.resolve_artifact_names
    def test_update_with_bindparams(self):
        sess = create_session(bind=testing.db, autocommit=False)

        john,jack,jill,jane = sess.query(User).order_by(User.id).all()

        sess.query(User).filter('age > :x').params(x=29).update({'age': User.age - 10}, synchronize_session='fetch')

        eq_([john.age, jack.age, jill.age, jane.age], [25,37,29,27])
        eq_(sess.query(User.age).order_by(User.id).all(), zip([25,37,29,27]))

    @testing.resolve_artifact_names
    def test_update_changes_resets_dirty(self):
        sess = create_session(bind=testing.db, autocommit=False, autoflush=False)

        john,jack,jill,jane = sess.query(User).order_by(User.id).all()

        john.age = 50
        jack.age = 37

        # autoflush is false.  therefore our '50' and '37' are getting blown away by this operation.

        sess.query(User).filter(User.age > 29).update({'age': User.age - 10}, synchronize_session='evaluate')

        for x in (john, jack, jill, jane):
            assert not sess.is_modified(x)

        eq_([john.age, jack.age, jill.age, jane.age], [25,37,29,27])

        john.age = 25
        assert john in sess.dirty
        assert jack in sess.dirty
        assert jill not in sess.dirty
        assert not sess.is_modified(john)
        assert not sess.is_modified(jack)

    @testing.resolve_artifact_names
    def test_update_changes_with_autoflush(self):
        sess = create_session(bind=testing.db, autocommit=False, autoflush=True)

        john,jack,jill,jane = sess.query(User).order_by(User.id).all()

        john.age = 50
        jack.age = 37

        sess.query(User).filter(User.age > 29).update({'age': User.age - 10}, synchronize_session='evaluate')

        for x in (john, jack, jill, jane):
            assert not sess.is_modified(x)

        eq_([john.age, jack.age, jill.age, jane.age], [40, 27, 29, 27])

        john.age = 25
        assert john in sess.dirty
        assert jack not in sess.dirty
        assert jill not in sess.dirty
        assert sess.is_modified(john)
        assert not sess.is_modified(jack)



    @testing.resolve_artifact_names
    def test_update_with_expire_strategy(self):
        sess = create_session(bind=testing.db, autocommit=False)

        john,jack,jill,jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(User.age > 29).update({'age': User.age - 10}, synchronize_session='fetch')

        eq_([john.age, jack.age, jill.age, jane.age], [25,37,29,27])
        eq_(sess.query(User.age).order_by(User.id).all(), zip([25,37,29,27]))

    @testing.fails_if(lambda: not testing.db.dialect.supports_sane_rowcount)
    @testing.resolve_artifact_names
    def test_update_returns_rowcount(self):
        sess = create_session(bind=testing.db, autocommit=False)

        rowcount = sess.query(User).filter(User.age > 29).update({'age': User.age + 0})
        eq_(rowcount, 2)

        rowcount = sess.query(User).filter(User.age > 29).update({'age': User.age - 10})
        eq_(rowcount, 2)

    @testing.fails_if(lambda: not testing.db.dialect.supports_sane_rowcount)
    @testing.resolve_artifact_names
    def test_delete_returns_rowcount(self):
        sess = create_session(bind=testing.db, autocommit=False)

        rowcount = sess.query(User).filter(User.age > 26).delete(synchronize_session=False)
        eq_(rowcount, 3)

    @testing.resolve_artifact_names
    def test_update_with_eager_relationships(self):
        self.insert_documents()

        sess = create_session(bind=testing.db, autocommit=False)

        foo,bar,baz = sess.query(Document).order_by(Document.id).all()
        sess.query(Document).filter(Document.user_id == 1).update({'title': Document.title+Document.title}, synchronize_session='fetch')

        eq_([foo.title, bar.title, baz.title], ['foofoo','barbar', 'baz'])
        eq_(sess.query(Document.title).order_by(Document.id).all(), zip(['foofoo','barbar', 'baz']))

    @testing.resolve_artifact_names
    def test_update_with_explicit_joinedload(self):
        sess = create_session(bind=testing.db, autocommit=False)

        john,jack,jill,jane = sess.query(User).order_by(User.id).all()
        sess.query(User).options(joinedload(User.documents)).filter(User.age > 29).update({'age': User.age - 10}, synchronize_session='fetch')

        eq_([john.age, jack.age, jill.age, jane.age], [25,37,29,27])
        eq_(sess.query(User.age).order_by(User.id).all(), zip([25,37,29,27]))

    @testing.resolve_artifact_names
    def test_delete_with_eager_relationships(self):
        self.insert_documents()

        sess = create_session(bind=testing.db, autocommit=False)

        sess.query(Document).filter(Document.user_id == 1).delete(synchronize_session=False)

        eq_(sess.query(Document.title).all(), zip(['baz']))

    @testing.resolve_artifact_names
    def test_update_all(self):
        sess = create_session(bind=testing.db, autocommit=False)

        john,jack,jill,jane = sess.query(User).order_by(User.id).all()
        sess.query(User).update({'age': 42}, synchronize_session='evaluate')

        eq_([john.age, jack.age, jill.age, jane.age], [42,42,42,42])
        eq_(sess.query(User.age).order_by(User.id).all(), zip([42,42,42,42]))

    @testing.resolve_artifact_names
    def test_delete_all(self):
        sess = create_session(bind=testing.db, autocommit=False)

        john,jack,jill,jane = sess.query(User).order_by(User.id).all()
        sess.query(User).delete(synchronize_session='evaluate')

        assert not (john in sess or jack in sess or jill in sess or jane in sess)
        eq_(sess.query(User).count(), 0)


class StatementOptionsTest(QueryTest):

    def test_query_with_statement_option(self):
        sess = create_session(bind=testing.db, autocommit=False)

        q1 = sess.query(User)
        assert q1._execution_options == dict()
        q2 = q1.execution_options(foo='bar', stream_results=True)
        # q1's options should be unchanged.
        assert q1._execution_options == dict()
        # q2 should have them set.
        assert q2._execution_options == dict(foo='bar', stream_results=True)
        q3 = q2.execution_options(foo='not bar', answer=42)
        assert q2._execution_options == dict(foo='bar', stream_results=True)

        q3_options = dict(foo='not bar', stream_results=True, answer=42)
        assert q3._execution_options == q3_options
        assert q3.statement._execution_options == q3_options
        assert q3._compile_context().statement._execution_options == q3_options
        assert q3.subquery().original._execution_options == q3_options

    # TODO: Test that statement options are passed on to
    # updates/deletes, but currently there are no such options
    # applicable for them.

class OptionsTest(QueryTest):
    """Test the _get_paths() method of PropertyOption."""

    def _option_fixture(self, *arg):
        from sqlalchemy.orm import interfaces
        class Opt(interfaces.PropertyOption):
            pass
        return Opt(arg)

    def _make_path(self, path):
        r = []
        for i, item in enumerate(path):
            if i % 2 == 0:
                if isinstance(item, type):
                    item = class_mapper(item)
            r.append(item)
        return tuple(r)

    def _assert_path_result(self, opt, q, paths, mappers):
        eq_(
            opt._get_paths(q, False),
            ([self._make_path(p) for p in paths], 
            [class_mapper(c) for c in mappers])
        )

    def test_get_path_one_level_string(self):
        sess = Session()
        q = sess.query(User)

        opt = self._option_fixture("addresses")
        self._assert_path_result(opt, q, [(User, 'addresses')], [User])

    def test_get_path_one_level_attribute(self):
        sess = Session()
        q = sess.query(User)

        opt = self._option_fixture(User.addresses)
        self._assert_path_result(opt, q, [(User, 'addresses')], [User])

    def test_get_path_one_level_with_unrelated(self):
        sess = Session()
        q = sess.query(Order)

        opt = self._option_fixture("addresses")
        self._assert_path_result(opt, q, [], [])

    def test_path_multilevel_string(self):
        sess = Session()
        q = sess.query(User)

        opt = self._option_fixture("orders.items.keywords")
        self._assert_path_result(opt, q, [
            (User, 'orders'), 
            (User, 'orders', Order, 'items'),
            (User, 'orders', Order, 'items', Item, 'keywords')
        ], 
        [User, Order, Item])

    def test_path_multilevel_attribute(self):
        sess = Session()
        q = sess.query(User)

        opt = self._option_fixture(User.orders, Order.items, Item.keywords)
        self._assert_path_result(opt, q, [
            (User, 'orders'), 
            (User, 'orders', Order, 'items'),
            (User, 'orders', Order, 'items', Item, 'keywords')
        ], 
        [User, Order, Item])

    def test_with_current_matching_string(self):
        sess = Session()
        q = sess.query(Item)._with_current_path(
                self._make_path([User, 'orders', Order, 'items'])
            )

        opt = self._option_fixture("orders.items.keywords")
        self._assert_path_result(opt, q, [
            (Item, 'keywords')
        ], [Item])

    def test_with_current_matching_attribute(self):
        sess = Session()
        q = sess.query(Item)._with_current_path(
                self._make_path([User, 'orders', Order, 'items'])
            )

        opt = self._option_fixture(User.orders, Order.items, Item.keywords)
        self._assert_path_result(opt, q, [
            (Item, 'keywords')
        ], [Item])

    def test_with_current_nonmatching_string(self):
        sess = Session()
        q = sess.query(Item)._with_current_path(
                self._make_path([User, 'orders', Order, 'items'])
            )

        opt = self._option_fixture("keywords")
        self._assert_path_result(opt, q, [], [])

        opt = self._option_fixture("items.keywords")
        self._assert_path_result(opt, q, [], [])

    def test_with_current_nonmatching_attribute(self):
        sess = Session()
        q = sess.query(Item)._with_current_path(
                self._make_path([User, 'orders', Order, 'items'])
            )

        opt = self._option_fixture(Item.keywords)
        self._assert_path_result(opt, q, [], [])

        opt = self._option_fixture(Order.items, Item.keywords)
        self._assert_path_result(opt, q, [], [])

    def test_from_base_to_subclass_attr(self):
        sess = Session()
        class SubAddr(Address):
            pass
        mapper(SubAddr, inherits=Address, properties={
            'flub':relationship(Dingaling)
        })

        q = sess.query(Address)
        opt = self._option_fixture(SubAddr.flub)

        self._assert_path_result(opt, q, [(Address, 'flub')], [SubAddr])

    def test_from_subclass_to_subclass_attr(self):
        sess = Session()
        class SubAddr(Address):
            pass
        mapper(SubAddr, inherits=Address, properties={
            'flub':relationship(Dingaling)
        })

        q = sess.query(SubAddr)
        opt = self._option_fixture(SubAddr.flub)

        self._assert_path_result(opt, q, [(SubAddr, 'flub')], [SubAddr])

    def test_from_base_to_base_attr_via_subclass(self):
        sess = Session()
        class SubAddr(Address):
            pass
        mapper(SubAddr, inherits=Address, properties={
            'flub':relationship(Dingaling)
        })

        q = sess.query(Address)
        opt = self._option_fixture(SubAddr.user)

        self._assert_path_result(opt, q, [(Address, 'user')], [Address])

    def test_of_type(self):
        sess = Session()
        class SubAddr(Address):
            pass
        mapper(SubAddr, inherits=Address)

        q = sess.query(User)
        opt = self._option_fixture(User.addresses.of_type(SubAddr), SubAddr.user)

        self._assert_path_result(opt, q, [
            (User, 'addresses'),
            (User, 'addresses', SubAddr, 'user')
        ], [User, Address])

    def test_of_type_plus_level(self):
        sess = Session()
        class SubAddr(Address):
            pass
        mapper(SubAddr, inherits=Address, properties={
            'flub':relationship(Dingaling)
        })

        q = sess.query(User)
        opt = self._option_fixture(User.addresses.of_type(SubAddr), SubAddr.flub)

        self._assert_path_result(opt, q, [
            (User, 'addresses'),
            (User, 'addresses', SubAddr, 'flub')
        ], [User, SubAddr])

    def test_aliased_single(self):
        sess = Session()
        ualias = aliased(User)
        q = sess.query(ualias)
        opt = self._option_fixture(ualias.addresses)
        self._assert_path_result(opt, q, [(ualias, 'addresses')], [User])

    def test_with_current_aliased_single(self):
        sess = Session()
        ualias = aliased(User)
        q = sess.query(ualias)._with_current_path(
                        self._make_path([Address, 'user'])
                )
        opt = self._option_fixture(Address.user, ualias.addresses)
        self._assert_path_result(opt, q, [(ualias, 'addresses')], [User])

    def test_with_current_aliased_single_nonmatching_option(self):
        sess = Session()
        ualias = aliased(User)
        q = sess.query(User)._with_current_path(
                        self._make_path([Address, 'user'])
                )
        opt = self._option_fixture(Address.user, ualias.addresses)
        self._assert_path_result(opt, q, [], [])

    @testing.fails_if(lambda: True, "Broken feature")
    def test_with_current_aliased_single_nonmatching_entity(self):
        sess = Session()
        ualias = aliased(User)
        q = sess.query(ualias)._with_current_path(
                        self._make_path([Address, 'user'])
                )
        opt = self._option_fixture(Address.user, User.addresses)
        self._assert_path_result(opt, q, [], [])

