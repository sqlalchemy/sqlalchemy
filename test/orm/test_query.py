from sqlalchemy.test.testing import eq_, assert_raises, assert_raises_message
import operator
from sqlalchemy import *
from sqlalchemy import exc as sa_exc, util
from sqlalchemy.sql import compiler, table, column
from sqlalchemy.engine import default
from sqlalchemy.orm import *
from sqlalchemy.orm import attributes

from sqlalchemy.test.testing import eq_

import sqlalchemy as sa
from sqlalchemy.test import testing, AssertsCompiledSQL, Column, engines

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

        compile_mappers()

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

    def test_get_composite_pk(self):
        s = create_session()
        assert s.query(CompositePk).get((100,100)) is None
        one_two = s.query(CompositePk).get((1,2))
        assert one_two.i == 1
        assert one_two.j == 2
        assert one_two.k == 3
        q = s.query(CompositePk)
        assert_raises(sa_exc.InvalidRequestError, q.get, 7)        
    
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
        """test that Query.get properly sets up the type for the bind parameter.  using unicode would normally fail
        on postgresql, mysql and oracle unless it is converted to an encoded string"""

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


class RawSelectTest(QueryTest, AssertsCompiledSQL):
    """compare a bunch of select() tests with the equivalent Query using straight table/columns.
    
    Results should be the same as Query should act as a select() pass-thru for ClauseElement entities.
    
    """
    def test_select(self):
        sess = create_session()

        self.assert_compile(sess.query(users).select_from(users.select()).with_labels().statement, 
            "SELECT users.id AS users_id, users.name AS users_name FROM users, "
            "(SELECT users.id AS id, users.name AS name FROM users) AS anon_1",
            dialect=default.DefaultDialect()
            )

        self.assert_compile(sess.query(users, exists([1], from_obj=addresses)).with_labels().statement, 
            "SELECT users.id AS users_id, users.name AS users_name, EXISTS "
            "(SELECT 1 FROM addresses) AS anon_1 FROM users",
            dialect=default.DefaultDialect()
            )

        # a little tedious here, adding labels to work around Query's auto-labelling.
        # also correlate needed explicitly.  hmmm.....
        # TODO: can we detect only one table in the "froms" and then turn off use_labels ?
        s = sess.query(addresses.c.id.label('id'), addresses.c.email_address.label('email')).\
            filter(addresses.c.user_id==users.c.id).correlate(users).statement.alias()
            
        self.assert_compile(sess.query(users, s.c.email).select_from(users.join(s, s.c.id==users.c.id)).with_labels().statement, 
                "SELECT users.id AS users_id, users.name AS users_name, anon_1.email AS anon_1_email "
                "FROM users JOIN (SELECT addresses.id AS id, addresses.email_address AS email FROM addresses "
                "WHERE addresses.user_id = users.id) AS anon_1 ON anon_1.id = users.id",
                dialect=default.DefaultDialect()
            )

        x = func.lala(users.c.id).label('foo')
        self.assert_compile(sess.query(x).filter(x==5).statement, 
            "SELECT lala(users.id) AS foo FROM users WHERE lala(users.id) = :param_1", dialect=default.DefaultDialect())

        self.assert_compile(sess.query(func.sum(x).label('bar')).statement,  
            "SELECT sum(lala(users.id)) AS bar FROM users", dialect=default.DefaultDialect()) 

class ExpressionTest(QueryTest, AssertsCompiledSQL):
        
    def test_deferred_instances(self):
        session = create_session()
        s = session.query(User).filter(and_(addresses.c.email_address == bindparam('emailad'), Address.user_id==User.id)).statement

        l = list(session.query(User).instances(s.execute(emailad = 'jack@bean.com')))
        eq_([User(id=7)], l)

    def test_scalar_subquery(self):
        session = create_session()
        
        q = session.query(User.id).filter(User.id==7).subquery()
        
        q = session.query(User).filter(User.id==q)
        
        eq_(User(id=7), q.one())
        
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
            s.query(User, adalias).join((adalias, User.id==adalias.user_id)).all(),
            [(User(id=7,name=u'jack'), Address(email_address=u'jack@bean.com',user_id=7,id=1))]
        )
        
# more slice tests are available in test/orm/generative.py
class SliceTest(QueryTest):
    def test_first(self):
        assert  User(id=7) == create_session().query(User).first()

        assert create_session().query(User).filter(User.id==27).first() is None

    @testing.fails_on_everything_except('sqlite')
    def test_limit_offset_applies(self):
        """Test that the expected LIMIT/OFFSET is applied for slices.
        
        The LIMIT/OFFSET syntax differs slightly on all databases, and
        query[x:y] executes immediately, so we are asserting against
        SQL strings using sqlite's syntax.
        
        """
        sess = create_session()
        q = sess.query(User)
        
        self.assert_sql(testing.db, lambda: q[10:20], [
            ("SELECT users.id AS users_id, users.name AS users_name FROM users  LIMIT 10 OFFSET 10", {})
        ])

        self.assert_sql(testing.db, lambda: q[:20], [
            ("SELECT users.id AS users_id, users.name AS users_name FROM users  LIMIT 20 OFFSET 0", {})
        ])

        self.assert_sql(testing.db, lambda: q[5:], [
            ("SELECT users.id AS users_id, users.name AS users_name FROM users  LIMIT -1 OFFSET 5", {})
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


class FromSelfTest(QueryTest, AssertsCompiledSQL):
    def test_filter(self):

        assert [User(id=8), User(id=9)] == create_session().query(User).filter(User.id.in_([8,9])).from_self().all()

        assert [User(id=8), User(id=9)] == create_session().query(User).order_by(User.id).slice(1,3).from_self().all()
        assert [User(id=8)] == list(create_session().query(User).filter(User.id.in_([8,9])).from_self().order_by(User.id)[0:1])
    
    def test_join(self):
        assert [
            (User(id=8), Address(id=2)),
            (User(id=8), Address(id=3)),
            (User(id=8), Address(id=4)),
            (User(id=9), Address(id=5))
        ] == create_session().query(User).filter(User.id.in_([8,9])).from_self().\
            join('addresses').add_entity(Address).order_by(User.id, Address.id).all()
    
    def test_group_by(self):
        eq_(
            create_session().query(Address.user_id, func.count(Address.id).label('count')).\
                            group_by(Address.user_id).order_by(Address.user_id).all(),
            [(7, 1), (8, 3), (9, 1)]
        )

        eq_(
            create_session().query(Address.user_id, Address.id).\
                            from_self(Address.user_id, func.count(Address.id)).\
                            group_by(Address.user_id).order_by(Address.user_id).all(),
            [(7, 1), (8, 3), (9, 1)]
        )
        
    def test_no_joinedload(self):
        """test that joinedloads are pushed outwards and not rendered in subqueries."""
        
        s = create_session()
        
        oracle_as = not testing.against('oracle') and "AS " or ""
        
        self.assert_compile(
            s.query(User).options(joinedload(User.addresses)).from_self().statement,
            "SELECT anon_1.users_id, anon_1.users_name, addresses_1.id, addresses_1.user_id, "\
            "addresses_1.email_address FROM (SELECT users.id AS users_id, users.name AS users_name FROM users) %(oracle_as)sanon_1 "\
            "LEFT OUTER JOIN addresses %(oracle_as)saddresses_1 ON anon_1.users_id = addresses_1.user_id ORDER BY addresses_1.id" % {
                'oracle_as':oracle_as
            }
        )
            
    def test_aliases(self):
        """test that aliased objects are accessible externally to a from_self() call."""
        
        s = create_session()
        
        ualias = aliased(User)
        eq_(
            s.query(User, ualias).filter(User.id > ualias.id).from_self(User.name, ualias.name).
                    order_by(User.name, ualias.name).all(),
            [
                (u'chuck', u'ed'), 
                (u'chuck', u'fred'), 
                (u'chuck', u'jack'), 
                (u'ed', u'jack'), 
                (u'fred', u'ed'), 
                (u'fred', u'jack')
            ]
        )

        eq_(
            s.query(User, ualias).filter(User.id > ualias.id).from_self(User.name, ualias.name).filter(ualias.name=='ed')\
                .order_by(User.name, ualias.name).all(),
            [(u'chuck', u'ed'), (u'fred', u'ed')]
        )

        eq_(
            s.query(User, ualias).filter(User.id > ualias.id).from_self(ualias.name, Address.email_address).
                    join(ualias.addresses).order_by(ualias.name, Address.email_address).all(),
            [
                (u'ed', u'fred@fred.com'), 
                (u'jack', u'ed@bettyboop.com'), 
                (u'jack', u'ed@lala.com'), 
                (u'jack', u'ed@wood.com'), 
                (u'jack', u'fred@fred.com')]
        )
        
        
    def test_multiple_entities(self):
        sess = create_session()

        eq_(
            sess.query(User, Address).filter(User.id==Address.user_id).filter(Address.id.in_([2, 5])).from_self().all(),
            [
                (User(id=8), Address(id=2)),
                (User(id=9), Address(id=5))
            ]
        )

        eq_(
            sess.query(User, Address).filter(User.id==Address.user_id).filter(Address.id.in_([2, 5])).from_self().options(joinedload('addresses')).first(),
            
            #    order_by(User.id, Address.id).first(),
            (User(id=8, addresses=[Address(), Address(), Address()]), Address(id=2)),
        )

    def test_multiple_with_column_entities(self):
        sess = create_session()
        
        eq_(
            sess.query(User.id).from_self().\
                add_column(func.count().label('foo')).\
                group_by(User.id).\
                order_by(User.id).\
                from_self().all(),
            [
                (7,1), (8, 1), (9, 1), (10, 1)
            ]
            
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
        
    def test_union_labels(self):
        """test that column expressions translate during 
            the _from_statement() portion of union(), others"""
        
        s = create_session()
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

        q4 = s.query(User, literal_column("'x'").label('foo'))
        q5 = s.query(User, literal("y"))
        q6 = q4.union(q5)
        
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
                join((ualias, ualias.id > User.id)),
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
            assert str(e) == "Could not locate a property which relates instances of class 'Item' to instances of class 'User'"

    def test_m2m(self):
        sess = create_session()
        i1 = sess.query(Item).filter_by(id=2).one()
        k = sess.query(Keyword).with_parent(i1).all()
        assert [Keyword(name='red'), Keyword(name='small'), Keyword(name='square')] == k

class InheritedJoinTest(_base.MappedTest, AssertsCompiledSQL):
    run_setup_mappers = 'once'
    
    @classmethod
    def define_tables(cls, metadata):
        Table('companies', metadata,
           Column('company_id', Integer, primary_key=True, test_needs_autoincrement=True),
           Column('name', String(50)))

        Table('people', metadata,
           Column('person_id', Integer, primary_key=True, test_needs_autoincrement=True),
           Column('company_id', Integer, ForeignKey('companies.company_id')),
           Column('name', String(50)),
           Column('type', String(30)))

        Table('engineers', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
           Column('engineer_name', String(50)),
           Column('primary_language', String(50)),
          )
     
        Table('machines', metadata,
            Column('machine_id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('name', String(50)),
            Column('engineer_id', Integer, ForeignKey('engineers.person_id')))
        
        Table('managers', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
           Column('manager_name', String(50))
           )

        Table('boss', metadata,
            Column('boss_id', Integer, ForeignKey('managers.person_id'), primary_key=True),
            Column('golf_swing', String(30)),
            )

        Table('paperwork', metadata,
            Column('paperwork_id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('description', String(50)),
            Column('person_id', Integer, ForeignKey('people.person_id')))
    
    @classmethod
    @testing.resolve_artifact_names
    def setup_classes(cls):
        class Company(_fixtures.Base):
            pass
        class Person(_fixtures.Base):
            pass
        class Engineer(Person):
            pass
        class Manager(Person):
            pass
        class Boss(Manager):
            pass
        class Machine(_fixtures.Base):
            pass
        class Paperwork(_fixtures.Base):
            pass

        mapper(Company, companies, properties={
            'employees':relationship(Person, order_by=people.c.person_id)
        })

        mapper(Machine, machines)

        mapper(Person, people, 
            polymorphic_on=people.c.type, 
            polymorphic_identity='person', 
            order_by=people.c.person_id, 
            properties={
                'paperwork':relationship(Paperwork, order_by=paperwork.c.paperwork_id)
            })
        mapper(Engineer, engineers, inherits=Person, polymorphic_identity='engineer', properties={
                'machines':relationship(Machine, order_by=machines.c.machine_id)
            })
        mapper(Manager, managers, 
                    inherits=Person, polymorphic_identity='manager')
        mapper(Boss, boss, inherits=Manager, polymorphic_identity='boss')
        mapper(Paperwork, paperwork)
    
    @testing.resolve_artifact_names
    def test_single_prop(self):
        sess = create_session()
    
        self.assert_compile(
            sess.query(Company).join(Company.employees),
            "SELECT companies.company_id AS companies_company_id, companies.name AS companies_name "
            "FROM companies JOIN people ON companies.company_id = people.company_id"
            , use_default_dialect = True
        )

    @testing.resolve_artifact_names
    def test_force_via_select_from(self):
        sess = create_session()

        self.assert_compile(
            sess.query(Company).\
                filter(Company.company_id==Engineer.company_id).\
                filter(Engineer.primary_language=='java'),
            "SELECT companies.company_id AS companies_company_id, companies.name AS companies_name "
            "FROM companies, people, engineers "
            "WHERE companies.company_id = people.company_id AND engineers.primary_language "
            "= :primary_language_1",
            use_default_dialect=True
        )

        self.assert_compile(
            sess.query(Company).select_from(Company, Engineer).\
                filter(Company.company_id==Engineer.company_id).\
                filter(Engineer.primary_language=='java'),
            "SELECT companies.company_id AS companies_company_id, companies.name AS companies_name "
            "FROM companies, people JOIN engineers ON people.person_id = engineers.person_id "
            "WHERE companies.company_id = people.company_id AND engineers.primary_language ="
            " :primary_language_1",
            use_default_dialect=True
            
        )
            
    @testing.resolve_artifact_names
    def test_single_prop_of_type(self):
        sess = create_session()

        self.assert_compile(
            sess.query(Company).join(Company.employees.of_type(Engineer)),
            "SELECT companies.company_id AS companies_company_id, companies.name AS companies_name "
            "FROM companies JOIN (SELECT people.person_id AS people_person_id, "
            "people.company_id AS people_company_id, people.name AS people_name, "
            "people.type AS people_type, engineers.person_id AS "
            "engineers_person_id, engineers.status AS engineers_status, "
            "engineers.engineer_name AS engineers_engineer_name, "
            "engineers.primary_language AS engineers_primary_language "
            "FROM people JOIN engineers ON people.person_id = engineers.person_id) AS "
            "anon_1 ON companies.company_id = anon_1.people_company_id"
            , use_default_dialect = True
        )

    @testing.resolve_artifact_names
    def test_prop_with_polymorphic(self):
        sess = create_session()
        
        self.assert_compile(
            sess.query(Person).with_polymorphic(Manager).
                    join('paperwork').filter(Paperwork.description.like('%review%')),
                "SELECT people.person_id AS people_person_id, people.company_id AS"
                " people_company_id, "
                "people.name AS people_name, people.type AS people_type, managers.person_id "
                "AS managers_person_id, "
                "managers.status AS managers_status, managers.manager_name AS "
                "managers_manager_name FROM people "
                "LEFT OUTER JOIN managers ON people.person_id = managers.person_id JOIN "
                "paperwork ON people.person_id = "
                "paperwork.person_id WHERE paperwork.description LIKE :description_1 "
                "ORDER BY people.person_id"
                , use_default_dialect=True
            )
        
        self.assert_compile(
            sess.query(Person).with_polymorphic(Manager).
                    join('paperwork', aliased=True).
                    filter(Paperwork.description.like('%review%')),
            "SELECT people.person_id AS people_person_id, people.company_id AS people_company_id, "
            "people.name AS people_name, people.type AS people_type, managers.person_id "
            "AS managers_person_id, "
            "managers.status AS managers_status, managers.manager_name AS managers_manager_name "
            "FROM people LEFT OUTER JOIN managers ON people.person_id = managers.person_id JOIN "
            "paperwork AS paperwork_1 ON people.person_id = paperwork_1.person_id "
            "WHERE paperwork_1.description LIKE :description_1 ORDER BY people.person_id"
            , use_default_dialect=True
        )

    @testing.resolve_artifact_names
    def test_explicit_polymorphic_join(self):
        sess = create_session()
        
        self.assert_compile(
            sess.query(Company).join(Engineer).filter(Engineer.engineer_name=='vlad'),
            "SELECT companies.company_id AS companies_company_id, companies.name AS "
            "companies_name "
            "FROM companies JOIN (SELECT people.person_id AS people_person_id, "
            "people.company_id AS "
            "people_company_id, people.name AS people_name, people.type AS people_type,"
            " engineers.person_id AS "
            "engineers_person_id, engineers.status AS engineers_status, "
            "engineers.engineer_name AS engineers_engineer_name, "
            "engineers.primary_language AS engineers_primary_language "
            "FROM people JOIN engineers ON people.person_id = engineers.person_id) "
            "AS anon_1 ON "
            "companies.company_id = anon_1.people_company_id "
            "WHERE anon_1.engineers_engineer_name = :engineer_name_1"
            , use_default_dialect=True
        )
        self.assert_compile(
            sess.query(Company).join((Engineer, Company.company_id==Engineer.company_id)).
                    filter(Engineer.engineer_name=='vlad'),
            "SELECT companies.company_id AS companies_company_id, companies.name "
            "AS companies_name "
            "FROM companies JOIN (SELECT people.person_id AS people_person_id, "
            "people.company_id AS "
            "people_company_id, people.name AS people_name, people.type AS "
            "people_type, engineers.person_id AS "
            "engineers_person_id, engineers.status AS engineers_status, "
            "engineers.engineer_name AS engineers_engineer_name, "
            "engineers.primary_language AS engineers_primary_language "
            "FROM people JOIN engineers ON people.person_id = engineers.person_id) AS "
            "anon_1 ON "
            "companies.company_id = anon_1.people_company_id "
            "WHERE anon_1.engineers_engineer_name = :engineer_name_1"
            , use_default_dialect=True
        )

    @testing.resolve_artifact_names
    def test_multiple_adaption(self):
        """test that multiple filter() adapters get chained together "
        and work correctly within a multiple-entry join()."""
        
        sess = create_session()

        self.assert_compile(
            sess.query(Company).join((people.join(engineers), Company.employees)).
                filter(Engineer.name=='dilbert'),
            "SELECT companies.company_id AS companies_company_id, companies.name AS "
            "companies_name "
            "FROM companies JOIN (SELECT people.person_id AS people_person_id, "
            "people.company_id AS "
            "people_company_id, people.name AS people_name, people.type AS "
            "people_type, engineers.person_id "
            "AS engineers_person_id, engineers.status AS engineers_status, "
            "engineers.engineer_name AS engineers_engineer_name, "
            "engineers.primary_language AS engineers_primary_language FROM people "
            "JOIN engineers ON people.person_id = "
            "engineers.person_id) AS anon_1 ON companies.company_id = "
            "anon_1.people_company_id WHERE anon_1.people_name = :name_1"
            , use_default_dialect = True
        )
        
        mach_alias = machines.select()
        self.assert_compile(
            sess.query(Company).join((people.join(engineers), Company.employees), 
                                        (mach_alias, Engineer.machines)).
                filter(Engineer.name=='dilbert').filter(Machine.name=='foo'),
            "SELECT companies.company_id AS companies_company_id, companies.name AS "
            "companies_name "
            "FROM companies JOIN (SELECT people.person_id AS people_person_id, "
            "people.company_id AS "
            "people_company_id, people.name AS people_name, people.type AS people_type,"
            " engineers.person_id "
            "AS engineers_person_id, engineers.status AS engineers_status, "
            "engineers.engineer_name AS engineers_engineer_name, "
            "engineers.primary_language AS engineers_primary_language FROM people "
            "JOIN engineers ON people.person_id = "
            "engineers.person_id) AS anon_1 ON companies.company_id = "
            "anon_1.people_company_id JOIN "
            "(SELECT machines.machine_id AS machine_id, machines.name AS name, "
            "machines.engineer_id AS engineer_id "
            "FROM machines) AS anon_2 ON anon_1.engineers_person_id = anon_2.engineer_id "
            "WHERE anon_1.people_name = :name_1 AND anon_2.name = :name_2"
            , use_default_dialect = True
        )

class AddEntityEquivalenceTest(_base.MappedTest, AssertsCompiledSQL):
    run_setup_mappers = 'once'

    @classmethod
    def define_tables(cls, metadata):
        Table('a', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('name', String(50)),
            Column('type', String(20)),
            Column('bid', Integer, ForeignKey('b.id'))
        )

        Table('b', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('name', String(50)),
            Column('type', String(20))
        )

        Table('c', metadata,
            Column('id', Integer, ForeignKey('b.id'), primary_key=True),
            Column('age', Integer)
        )

        Table('d', metadata,
            Column('id', Integer, ForeignKey('a.id'), primary_key=True),
            Column('dede', Integer)
        )

    @classmethod
    @testing.resolve_artifact_names
    def setup_classes(cls):
        class A(_fixtures.Base):
            pass
            
        class B(_fixtures.Base):
            pass
        
        class C(B):
            pass
        
        class D(A):
            pass
            
        mapper(A, a, 
                    polymorphic_identity='a', 
                    polymorphic_on=a.c.type,
                    with_polymorphic= ('*', None),
                    properties={
                        'link':relation( B, uselist=False, backref='back')
                    })
        mapper(B, b, 
                    polymorphic_identity='b', 
                    polymorphic_on=b.c.type,
                    with_polymorphic= ('*', None)
                    )
        mapper(C, c, inherits=B, polymorphic_identity='c')
        mapper(D, d, inherits=A, polymorphic_identity='d')
        
    @classmethod
    @testing.resolve_artifact_names
    def insert_data(cls):
        sess = create_session()
        sess.add_all([
            B(name='b1'), 
            A(name='a1', link= C(name='c1',age=3)), 
            C(name='c2',age=6), 
            A(name='a2')
            ])
        sess.flush()
    
    @testing.resolve_artifact_names
    def test_add_entity_equivalence(self):
        sess = create_session()
        
        for q in [
            sess.query( A,B).join( A.link),
            sess.query( A).join( A.link).add_entity(B),
        ]:
            eq_(
                q.all(),
                [(
                    A(bid=2, id=1, name=u'a1', type=u'a'), 
                    C(age=3, id=2, name=u'c1', type=u'c')
                )]
            )

        for q in [
            sess.query( B,A).join( B.back),
            sess.query( B).join( B.back).add_entity(A),
            sess.query( B).add_entity(A).join( B.back)
        ]:
            eq_(
                q.all(),
                [(
                    C(age=3, id=2, name=u'c1', type=u'c'), 
                    A(bid=2, id=1, name=u'a1', type=u'a')
                )]
            )
        
class JoinTest(QueryTest, AssertsCompiledSQL):
    
    def test_single_name(self):
        sess = create_session()

        self.assert_compile(
            sess.query(User).join("orders"),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id"
            , use_default_dialect = True
        )

        assert_raises(
            sa_exc.InvalidRequestError,
            sess.query(User).join, "user",
        )

        self.assert_compile(
            sess.query(User).join("orders", "items"),
            "SELECT users.id AS users_id, users.name AS users_name FROM users "
            "JOIN orders ON users.id = orders.user_id JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id JOIN items ON items.id = order_items_1.item_id"
            , use_default_dialect=True
        )

        # test overlapping paths.   User->orders is used by both joins, but rendered once.
        self.assert_compile(
            sess.query(User).join("orders", "items").join("orders", "address"),
            "SELECT users.id AS users_id, users.name AS users_name FROM users JOIN orders "
            "ON users.id = orders.user_id JOIN order_items AS order_items_1 ON orders.id = "
            "order_items_1.order_id JOIN items ON items.id = order_items_1.item_id JOIN addresses "
            "ON addresses.id = orders.address_id"
            , use_default_dialect=True
        )
    
    def test_common_mistake(self):
        sess = create_session()
        
        subq = sess.query(User).subquery()
        assert_raises_message(
            sa_exc.ArgumentError, "You appear to be passing a clause expression",
            sess.query(User).join, subq, User.name==subq.c.name)

        subq = sess.query(Order).subquery()
        assert_raises_message(
            sa_exc.ArgumentError, "You appear to be passing a clause expression",
            sess.query(User).join, subq, User.id==subq.c.user_id)

        assert_raises_message(
            sa_exc.ArgumentError, "You appear to be passing a clause expression",
            sess.query(User).join, Order, User.id==Order.user_id)
        
    def test_single_prop(self):
        sess = create_session()
        self.assert_compile(
            sess.query(User).join(User.orders),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id"
            , use_default_dialect=True
        )

        self.assert_compile(
            sess.query(User).join(Order.user),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM orders JOIN users ON users.id = orders.user_id"
            , use_default_dialect=True
        )

        oalias1 = aliased(Order)
        oalias2 = aliased(Order)

        self.assert_compile(
            sess.query(User).join(oalias1.user),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM orders AS orders_1 JOIN users ON users.id = orders_1.user_id"
            , use_default_dialect=True
        )
        
        # another nonsensical query.  (from [ticket:1537]).
        # in this case, the contract of "left to right" is honored
        self.assert_compile(
            sess.query(User).join(oalias1.user).join(oalias2.user),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM orders AS orders_1 JOIN users ON users.id = orders_1.user_id, "
            "orders AS orders_2 JOIN users ON users.id = orders_2.user_id"
            , use_default_dialect=True
        )
        
        self.assert_compile(
            sess.query(User).join(User.orders, Order.items),
            "SELECT users.id AS users_id, users.name AS users_name FROM users "
            "JOIN orders ON users.id = orders.user_id JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id JOIN items ON items.id = order_items_1.item_id"
            , use_default_dialect=True
        )
        
        ualias = aliased(User)
        self.assert_compile(
            sess.query(ualias).join(ualias.orders),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 JOIN orders ON users_1.id = orders.user_id"
            , use_default_dialect=True
        )
        
        # this query is somewhat nonsensical.  the old system didn't render a correct
        # query for this.   In this case its the most faithful to what was asked -
        # there's no linkage between User.orders and "oalias", so two FROM elements
        # are generated.
        oalias = aliased(Order)
        self.assert_compile(
            sess.query(User).join(User.orders, oalias.items),
            "SELECT users.id AS users_id, users.name AS users_name FROM users "
            "JOIN orders ON users.id = orders.user_id, "
            "orders AS orders_1 JOIN order_items AS order_items_1 ON orders_1.id = order_items_1.order_id "
            "JOIN items ON items.id = order_items_1.item_id"
            , use_default_dialect=True
        )

        # same as before using an aliased() for User as well
        ualias = aliased(User)
        self.assert_compile(
            sess.query(ualias).join(ualias.orders, oalias.items),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name FROM users AS users_1 "
            "JOIN orders ON users_1.id = orders.user_id, "
            "orders AS orders_1 JOIN order_items AS order_items_1 ON orders_1.id = order_items_1.order_id "
            "JOIN items ON items.id = order_items_1.item_id"
            , use_default_dialect=True
        )

        self.assert_compile(
            sess.query(User).filter(User.name=='ed').from_self().join(User.orders),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name AS anon_1_users_name "
            "FROM (SELECT users.id AS users_id, users.name AS users_name "
            "FROM users "
            "WHERE users.name = :name_1) AS anon_1 JOIN orders ON anon_1.users_id = orders.user_id"
            , use_default_dialect=True
        )
        
        self.assert_compile(
            sess.query(User).join(User.addresses, aliased=True).filter(Address.email_address=='foo'),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id "
            "WHERE addresses_1.email_address = :email_address_1"
            , use_default_dialect=True
        )

        self.assert_compile(
            sess.query(User).join(User.orders, Order.items, aliased=True).filter(Item.id==10),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders AS orders_1 ON users.id = orders_1.user_id "
            "JOIN order_items AS order_items_1 ON orders_1.id = order_items_1.order_id "
            "JOIN items AS items_1 ON items_1.id = order_items_1.item_id "
            "WHERE items_1.id = :id_1"
            , use_default_dialect=True
        )
        
        # test #1 for [ticket:1706]
        ualias = aliased(User)
        self.assert_compile(
            sess.query(ualias).
                    join((oalias1, ualias.orders)).\
                    join((Address, ualias.addresses)),
            "SELECT users_1.id AS users_1_id, users_1.name AS "
            "users_1_name FROM users AS users_1 JOIN orders AS orders_1 "
            "ON users_1.id = orders_1.user_id JOIN addresses ON users_1.id "
            "= addresses.user_id"
            , use_default_dialect=True
        )
        
        # test #2 for [ticket:1706]
        ualias2 = aliased(User)
        self.assert_compile(
            sess.query(ualias).
                    join((Address, ualias.addresses)).
                    join((ualias2, Address.user)).
                    join((Order, ualias.orders)),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name FROM users "
            "AS users_1 JOIN addresses ON users_1.id = addresses.user_id JOIN users AS users_2 "
            "ON users_2.id = addresses.user_id JOIN orders ON users_1.id = orders.user_id"
            , use_default_dialect=True
        )
        
    def test_overlapping_paths(self):
        for aliased in (True,False):
            # load a user who has an order that contains item id 3 and address id 1 (order 3, owned by jack)
            result = create_session().query(User).join('orders', 'items', aliased=aliased).\
                    filter_by(id=3).join('orders','address', aliased=aliased).filter_by(id=1).all()
            assert [User(id=7, name='jack')] == result

    def test_overlapping_paths_outerjoin(self):
        result = create_session().query(User).outerjoin('orders', 'items').\
                filter_by(id=3).outerjoin('orders','address').filter_by(id=1).all()
        assert [User(id=7, name='jack')] == result
    
    def test_from_joinpoint(self):
        sess = create_session()
        
        for oalias,ialias in [(True, True), (False, False), (True, False), (False, True)]:
            eq_(
                sess.query(User).join('orders', aliased=oalias).join('items', from_joinpoint=True, aliased=ialias).filter(Item.description == 'item 4').all(),
                [User(name='jack')]
            )

            # use middle criterion
            eq_(
                sess.query(User).join('orders', aliased=oalias).filter(Order.user_id==9).join('items', from_joinpoint=True, aliased=ialias).filter(Item.description=='item 4').all(),
                []
            )
        
        orderalias = aliased(Order)
        itemalias = aliased(Item)
        eq_(
            sess.query(User).join(('orders', orderalias), ('items', itemalias)).filter(itemalias.description == 'item 4').all(),
            [User(name='jack')]
        )
        eq_(
            sess.query(User).join(('orders', orderalias), ('items', itemalias)).filter(orderalias.user_id==9).filter(itemalias.description=='item 4').all(),
            []
        )
    
    def test_join_nonmapped_column(self):
        """test that the search for a 'left' doesn't trip on non-mapped cols"""
        sess = create_session()
        
        # intentionally join() with a non-existent "left" side
        self.assert_compile(
            sess.query(User.id, literal_column('foo')).join(Order.user),
            "SELECT users.id AS users_id, foo FROM orders JOIN users ON users.id = orders.user_id"
            , use_default_dialect=True
        )
        
        
        
    def test_backwards_join(self):
        # a more controversial feature.  join from
        # User->Address, but the onclause is Address.user.
        
        sess = create_session()

        eq_(
            sess.query(User).join(Address.user).filter(Address.email_address=='ed@wood.com').all(),
            [User(id=8,name=u'ed')]
        )

        # its actually not so controversial if you view it in terms
        # of multiple entities.
        eq_(
            sess.query(User, Address).join(Address.user).filter(Address.email_address=='ed@wood.com').all(),
            [(User(id=8,name=u'ed'), Address(email_address='ed@wood.com'))]
        )
        
        # this was the controversial part.  now, raise an error if the feature is abused.
        # before the error raise was added, this would silently work.....
        assert_raises(
            sa_exc.InvalidRequestError,
            sess.query(User).join, (Address, Address.user),
        )

        # but this one would silently fail 
        adalias = aliased(Address)
        assert_raises(
            sa_exc.InvalidRequestError,
            sess.query(User).join, (adalias, Address.user),
        )
        
    def test_multiple_with_aliases(self):
        sess = create_session()
        
        ualias = aliased(User)
        oalias1 = aliased(Order)
        oalias2 = aliased(Order)
        self.assert_compile(
            sess.query(ualias).join((oalias1, ualias.orders), (oalias2, ualias.orders)).\
                    filter(or_(oalias1.user_id==9, oalias2.user_id==7)),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name FROM users AS users_1 "
            "JOIN orders AS orders_1 ON users_1.id = orders_1.user_id JOIN orders AS orders_2 ON "
            "users_1.id = orders_2.user_id WHERE orders_1.user_id = :user_id_1 OR orders_2.user_id = :user_id_2",
            use_default_dialect=True
        )

    def test_select_from_orm_joins(self):
        sess = create_session()
        
        ualias = aliased(User)
        oalias1 = aliased(Order)
        oalias2 = aliased(Order)

        self.assert_compile(
            join(User, oalias2, User.id==oalias2.user_id),
            "users JOIN orders AS orders_1 ON users.id = orders_1.user_id",
            use_default_dialect=True
        )

        self.assert_compile(
            join(ualias, oalias1, ualias.orders),
            "users AS users_1 JOIN orders AS orders_1 ON users_1.id = orders_1.user_id",
            use_default_dialect=True
        )

        self.assert_compile(
            sess.query(ualias).select_from(join(ualias, oalias1, ualias.orders)),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name FROM users AS users_1 "
            "JOIN orders AS orders_1 ON users_1.id = orders_1.user_id",
            use_default_dialect=True
        )

        self.assert_compile(
            sess.query(User, ualias).select_from(join(ualias, oalias1, ualias.orders)),
            "SELECT users.id AS users_id, users.name AS users_name, users_1.id AS users_1_id, "
            "users_1.name AS users_1_name FROM users, users AS users_1 JOIN orders AS orders_1 ON users_1.id = orders_1.user_id",
            use_default_dialect=True
        )

        # this fails (and we cant quite fix right now).
        if False:
            self.assert_compile(
                sess.query(User, ualias).\
                        join((oalias1, ualias.orders)).\
                        join((oalias2, User.id==oalias2.user_id)).\
                        filter(or_(oalias1.user_id==9, oalias2.user_id==7)),
                "SELECT users.id AS users_id, users.name AS users_name, users_1.id AS users_1_id, users_1.name AS "
                "users_1_name FROM users JOIN orders AS orders_2 ON users.id = orders_2.user_id, "
                "users AS users_1 JOIN orders AS orders_1 ON users_1.id = orders_1.user_id  "
                "WHERE orders_1.user_id = :user_id_1 OR orders_2.user_id = :user_id_2",
                use_default_dialect=True
            )

        # this is the same thing using explicit orm.join() (which now offers multiple again)
        self.assert_compile(
            sess.query(User, ualias).\
                    select_from(
                        join(ualias, oalias1, ualias.orders),
                        join(User, oalias2, User.id==oalias2.user_id),
                    ).\
                    filter(or_(oalias1.user_id==9, oalias2.user_id==7)),
            "SELECT users.id AS users_id, users.name AS users_name, users_1.id AS users_1_id, users_1.name AS "
            "users_1_name FROM users AS users_1 JOIN orders AS orders_1 ON users_1.id = orders_1.user_id, "
            "users JOIN orders AS orders_2 ON users.id = orders_2.user_id "
            "WHERE orders_1.user_id = :user_id_1 OR orders_2.user_id = :user_id_2",
            
            use_default_dialect=True
        )
        
        
    def test_overlapping_backwards_joins(self):
        sess = create_session()

        oalias1 = aliased(Order)
        oalias2 = aliased(Order)
        
        # this is invalid SQL - joins from orders_1/orders_2 to User twice.  
        # but that is what was asked for so they get it !
        self.assert_compile(
            sess.query(User).join(oalias1.user).join(oalias2.user),
            "SELECT users.id AS users_id, users.name AS users_name FROM orders AS orders_1 "
            "JOIN users ON users.id = orders_1.user_id, orders AS orders_2 JOIN users ON users.id = orders_2.user_id",
            use_default_dialect=True,
        )

    def test_replace_multiple_from_clause(self):
        """test adding joins onto multiple FROM clauses"""
        
        sess = create_session()
        
        self.assert_compile(
            sess.query(Address, User).join(Address.dingaling).join(User.orders, Order.items),
            "SELECT addresses.id AS addresses_id, addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address, users.id AS users_id, "
            "users.name AS users_name FROM addresses JOIN dingalings ON addresses.id = dingalings.address_id, "
            "users JOIN orders ON users.id = orders.user_id JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id JOIN items ON items.id = order_items_1.item_id",
            use_default_dialect = True
        )
    
    def test_multiple_adaption(self):
        sess = create_session()

        self.assert_compile(
            sess.query(User).join(User.orders, Order.items, aliased=True).filter(Order.id==7).filter(Item.id==8),
            "SELECT users.id AS users_id, users.name AS users_name FROM users JOIN orders AS orders_1 "
            "ON users.id = orders_1.user_id JOIN order_items AS order_items_1 ON orders_1.id = order_items_1.order_id "
            "JOIN items AS items_1 ON items_1.id = order_items_1.item_id WHERE orders_1.id = :id_1 AND items_1.id = :id_2",
            use_default_dialect=True
        )
    
    def test_onclause_conditional_adaption(self):
        sess = create_session()

        self.assert_compile(
            sess.query(User).join(User.orders, 
                (Item, 
                    and_(Order.id==order_items.c.order_id, order_items.c.item_id==Item.id)
                ),aliased=True
                ),
            "SELECT users.id AS users_id, users.name AS users_name FROM users JOIN "
            "orders AS orders_1 ON users.id = orders_1.user_id JOIN items AS items_1 "
            "ON orders_1.id = order_items.order_id AND order_items.item_id = items_1.id",
            use_default_dialect=True
        )
        
        oalias = orders.select()
        self.assert_compile(
            sess.query(User).join((oalias, User.orders), 
                (Item, 
                    and_(Order.id==order_items.c.order_id, order_items.c.item_id==Item.id)
                ),
                ),
            "SELECT users.id AS users_id, users.name AS users_name FROM users JOIN "
            "(SELECT orders.id AS id, orders.user_id AS user_id, orders.address_id AS address_id, orders.description "
            "AS description, orders.isopen AS isopen FROM orders) AS anon_1 ON users.id = anon_1.user_id JOIN items "
            "ON anon_1.id = order_items.order_id AND order_items.item_id = items.id",
            use_default_dialect=True
        )
        
        
        # query.join(<stuff>, aliased=True).join((target, sql_expression))
        # or: query.join(path_to_some_joined_table_mapper).join((target, sql_expression))
        
    def test_pure_expression_error(self):
        sess = create_session()
        
        assert_raises_message(sa.exc.InvalidRequestError, "Could not find a FROM clause to join from", sess.query(users).join, addresses)
        
        
    def test_orderby_arg_bug(self):
        sess = create_session()
        # no arg error
        result = sess.query(User).join('orders', aliased=True).order_by(Order.id).reset_joinpoint().order_by(users.c.id).all()
    
    def test_no_onclause(self):
        sess = create_session()

        eq_(
            sess.query(User).select_from(join(User, Order).join(Item, Order.items)).filter(Item.description == 'item 4').all(),
            [User(name='jack')]
        )

        eq_(
            sess.query(User.name).select_from(join(User, Order).join(Item, Order.items)).filter(Item.description == 'item 4').all(),
            [('jack',)]
        )

        eq_(
            sess.query(User).join(Order, (Item, Order.items)).filter(Item.description == 'item 4').all(),
            [User(name='jack')]
        )
        
    def test_clause_onclause(self):
        sess = create_session()

        eq_(
            sess.query(User).join(
                (Order, User.id==Order.user_id), 
                (order_items, Order.id==order_items.c.order_id), 
                (Item, order_items.c.item_id==Item.id)
            ).filter(Item.description == 'item 4').all(),
            [User(name='jack')]
        )

        eq_(
            sess.query(User.name).join(
                (Order, User.id==Order.user_id), 
                (order_items, Order.id==order_items.c.order_id), 
                (Item, order_items.c.item_id==Item.id)
            ).filter(Item.description == 'item 4').all(),
            [('jack',)]
        )

        ualias = aliased(User)
        eq_(
            sess.query(ualias.name).join(
                (Order, ualias.id==Order.user_id), 
                (order_items, Order.id==order_items.c.order_id), 
                (Item, order_items.c.item_id==Item.id)
            ).filter(Item.description == 'item 4').all(),
            [('jack',)]
        )

        # explicit onclause with from_self(), means
        # the onclause must be aliased against the query's custom
        # FROM object
        eq_(
            sess.query(User).order_by(User.id).offset(2).from_self().join(
                (Order, User.id==Order.user_id)
            ).all(),
            [User(name='fred')]
        )

        # same with an explicit select_from()
        eq_(
            sess.query(User).select_from(select([users]).order_by(User.id).offset(2).alias()).join(
                (Order, User.id==Order.user_id)
            ).all(),
            [User(name='fred')]
        )
        
        
    def test_aliased_classes(self):
        sess = create_session()

        (user7, user8, user9, user10) = sess.query(User).all()
        (address1, address2, address3, address4, address5) = sess.query(Address).all()
        expected = [(user7, address1),
            (user8, address2),
            (user8, address3),
            (user8, address4),
            (user9, address5),
            (user10, None)]

        q = sess.query(User)
        AdAlias = aliased(Address)
        q = q.add_entity(AdAlias).select_from(outerjoin(User, AdAlias))
        l = q.order_by(User.id, AdAlias.id).all()
        eq_(l, expected)

        sess.expunge_all()

        q = sess.query(User).add_entity(AdAlias)
        l = q.select_from(outerjoin(User, AdAlias)).filter(AdAlias.email_address=='ed@bettyboop.com').all()
        eq_(l, [(user8, address3)])

        l = q.select_from(outerjoin(User, AdAlias, 'addresses')).filter(AdAlias.email_address=='ed@bettyboop.com').all()
        eq_(l, [(user8, address3)])

        l = q.select_from(outerjoin(User, AdAlias, User.id==AdAlias.user_id)).filter(AdAlias.email_address=='ed@bettyboop.com').all()
        eq_(l, [(user8, address3)])

        # this is the first test where we are joining "backwards" - from AdAlias to User even though
        # the query is against User
        q = sess.query(User, AdAlias)
        l = q.join(AdAlias.user).filter(User.name=='ed')
        eq_(l.all(), [(user8, address2),(user8, address3),(user8, address4),])

        q = sess.query(User, AdAlias).select_from(join(AdAlias, User, AdAlias.user)).filter(User.name=='ed')
        eq_(l.all(), [(user8, address2),(user8, address3),(user8, address4),])
        
    def test_implicit_joins_from_aliases(self):
        sess = create_session()
        OrderAlias = aliased(Order)

        eq_(
            sess.query(OrderAlias).join('items').filter_by(description='item 3').\
                order_by(OrderAlias.id).all(),
            [
                Order(address_id=1,description=u'order 1',isopen=0,user_id=7,id=1), 
                Order(address_id=4,description=u'order 2',isopen=0,user_id=9,id=2), 
                Order(address_id=1,description=u'order 3',isopen=1,user_id=7,id=3)
            ]
        )
         
        eq_(
            sess.query(User, OrderAlias, Item.description).join(('orders', OrderAlias), 'items').filter_by(description='item 3').\
                order_by(User.id, OrderAlias.id).all(),
            [
                (User(name=u'jack',id=7), Order(address_id=1,description=u'order 1',isopen=0,user_id=7,id=1), u'item 3'), 
                (User(name=u'jack',id=7), Order(address_id=1,description=u'order 3',isopen=1,user_id=7,id=3), u'item 3'), 
                (User(name=u'fred',id=9), Order(address_id=4,description=u'order 2',isopen=0,user_id=9,id=2), u'item 3')
            ]
        )   
        
    def test_aliased_classes_m2m(self):
        sess = create_session()
        
        (order1, order2, order3, order4, order5) = sess.query(Order).all()
        (item1, item2, item3, item4, item5) = sess.query(Item).all()
        expected = [
            (order1, item1),
            (order1, item2),
            (order1, item3),
            (order2, item1),
            (order2, item2),
            (order2, item3),
            (order3, item3),
            (order3, item4),
            (order3, item5),
            (order4, item1),
            (order4, item5),
            (order5, item5),
        ]
        
        q = sess.query(Order)
        q = q.add_entity(Item).select_from(join(Order, Item, 'items')).order_by(Order.id, Item.id)
        l = q.all()
        eq_(l, expected)

        IAlias = aliased(Item)
        q = sess.query(Order, IAlias).select_from(join(Order, IAlias, 'items')).filter(IAlias.description=='item 3')
        l = q.all()
        eq_(l, 
            [
                (order1, item3),
                (order2, item3),
                (order3, item3),
            ]
        )
    
    def test_joins_from_adapted_entities(self):

        # test for #1853

        session = create_session()
        first = session.query(User)
        second = session.query(User)
        unioned = first.union(second)
        subquery = session.query(User.id).subquery()
        join = subquery, subquery.c.id == User.id
        joined = unioned.outerjoin(join)
        self.assert_compile(joined,
                            'SELECT anon_1.users_id AS '
                            'anon_1_users_id, anon_1.users_name AS '
                            'anon_1_users_name FROM (SELECT users.id '
                            'AS users_id, users.name AS users_name '
                            'FROM users UNION SELECT users.id AS '
                            'users_id, users.name AS users_name FROM '
                            'users) AS anon_1 LEFT OUTER JOIN (SELECT '
                            'users.id AS id FROM users) AS anon_2 ON '
                            'anon_2.id = anon_1.users_id',
                            use_default_dialect=True)

        first = session.query(User.id)
        second = session.query(User.id)
        unioned = first.union(second)
        subquery = session.query(User.id).subquery()
        join = subquery, subquery.c.id == User.id
        joined = unioned.outerjoin(join)
        self.assert_compile(joined,
                            'SELECT anon_1.users_id AS anon_1_users_id '
                            'FROM (SELECT users.id AS users_id FROM '
                            'users UNION SELECT users.id AS users_id '
                            'FROM users) AS anon_1 LEFT OUTER JOIN '
                            '(SELECT users.id AS id FROM users) AS '
                            'anon_2 ON anon_2.id = anon_1.users_id',
                            use_default_dialect=True)
        
    def test_reset_joinpoint(self):
        for aliased in (True, False):
            # load a user who has an order that contains item id 3 and address id 1 (order 3, owned by jack)
            result = create_session().query(User).join('orders', 'items', aliased=aliased).filter_by(id=3).reset_joinpoint().join('orders','address', aliased=aliased).filter_by(id=1).all()
            assert [User(id=7, name='jack')] == result

            result = create_session().query(User).outerjoin('orders', 'items', aliased=aliased).filter_by(id=3).reset_joinpoint().outerjoin('orders','address', aliased=aliased).filter_by(id=1).all()
            assert [User(id=7, name='jack')] == result
    
    def test_overlap_with_aliases(self):
        oalias = orders.alias('oalias')

        result = create_session().query(User).select_from(users.join(oalias)).filter(oalias.c.description.in_(["order 1", "order 2", "order 3"])).join('orders', 'items').order_by(User.id).all()
        assert [User(id=7, name='jack'), User(id=9, name='fred')] == result

        result = create_session().query(User).select_from(users.join(oalias)).filter(oalias.c.description.in_(["order 1", "order 2", "order 3"])).join('orders', 'items').filter_by(id=4).all()
        assert [User(id=7, name='jack')] == result

    def test_aliased(self):
        """test automatic generation of aliased joins."""

        sess = create_session()

        # test a basic aliasized path
        q = sess.query(User).join('addresses', aliased=True).filter_by(email_address='jack@bean.com')
        assert [User(id=7)] == q.all()

        q = sess.query(User).join('addresses', aliased=True).filter(Address.email_address=='jack@bean.com')
        assert [User(id=7)] == q.all()

        q = sess.query(User).join('addresses', aliased=True).filter(or_(Address.email_address=='jack@bean.com', Address.email_address=='fred@fred.com'))
        assert [User(id=7), User(id=9)] == q.all()

        # test two aliasized paths, one to 'orders' and the other to 'orders','items'.
        # one row is returned because user 7 has order 3 and also has order 1 which has item 1
        # this tests a o2m join and a m2m join.
        q = sess.query(User).join('orders', aliased=True).filter(Order.description=="order 3").join('orders', 'items', aliased=True).filter(Item.description=="item 1")
        assert q.count() == 1
        assert [User(id=7)] == q.all()

        # test the control version - same joins but not aliased.  rows are not returned because order 3 does not have item 1
        q = sess.query(User).join('orders').filter(Order.description=="order 3").join('orders', 'items').filter(Item.description=="item 1")
        assert [] == q.all()
        assert q.count() == 0

        # the left half of the join condition of the any() is aliased.
        q = sess.query(User).join('orders', aliased=True).filter(Order.items.any(Item.description=='item 4'))
        assert [User(id=7)] == q.all()
        
        # test that aliasing gets reset when join() is called
        q = sess.query(User).join('orders', aliased=True).filter(Order.description=="order 3").join('orders', aliased=True).filter(Order.description=="order 5")
        assert q.count() == 1
        assert [User(id=7)] == q.all()

    def test_aliased_order_by(self):
        sess = create_session()

        ualias = aliased(User)
        eq_(
            sess.query(User, ualias).filter(User.id > ualias.id).order_by(desc(ualias.id), User.name).all(),
            [
                (User(id=10,name=u'chuck'), User(id=9,name=u'fred')), 
                (User(id=10,name=u'chuck'), User(id=8,name=u'ed')), 
                (User(id=9,name=u'fred'), User(id=8,name=u'ed')), 
                (User(id=10,name=u'chuck'), User(id=7,name=u'jack')), 
                (User(id=8,name=u'ed'), User(id=7,name=u'jack')),
                (User(id=9,name=u'fred'), User(id=7,name=u'jack'))
            ]
        )

    def test_plain_table(self):
            
        sess = create_session()
        
        eq_(
            sess.query(User.name).join((addresses, User.id==addresses.c.user_id)).order_by(User.id).all(),
            [(u'jack',), (u'ed',), (u'ed',), (u'ed',), (u'fred',)]
        )
    
    def test_no_joinpoint_expr(self):
        sess = create_session()
        
        # these are consistent regardless of
        # select_from() being present.
        
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Could not find a FROM",
            sess.query(users.c.id).join, User
        )
        
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Could not find a FROM",
            sess.query(users.c.id).select_from(users).join, User
        )
        
    def test_from_self_resets_joinpaths(self):
        """test a join from from_self() doesn't confuse joins inside the subquery
        with the outside.
        """
        sess = create_session()
        
        self.assert_compile(
            sess.query(Item).join(Item.keywords).from_self(Keyword).join(Item.keywords),
            "SELECT keywords.id AS keywords_id, keywords.name AS keywords_name FROM "
            "(SELECT items.id AS items_id, items.description AS items_description "
            "FROM items JOIN item_keywords AS item_keywords_1 ON items.id = "
            "item_keywords_1.item_id JOIN keywords ON keywords.id = item_keywords_1.keyword_id) "
            "AS anon_1 JOIN item_keywords AS item_keywords_2 ON "
            "anon_1.items_id = item_keywords_2.item_id "
            "JOIN keywords ON "
            "keywords.id = item_keywords_2.keyword_id",
            use_default_dialect=True
        )
        
        
class MultiplePathTest(_base.MappedTest, AssertsCompiledSQL):
    @classmethod
    def define_tables(cls, metadata):
        global t1, t2, t1t2_1, t1t2_2
        t1 = Table('t1', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(30))
            )
        t2 = Table('t2', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(30))
            )

        t1t2_1 = Table('t1t2_1', metadata,
            Column('t1id', Integer, ForeignKey('t1.id')),
            Column('t2id', Integer, ForeignKey('t2.id'))
            )

        t1t2_2 = Table('t1t2_2', metadata,
            Column('t1id', Integer, ForeignKey('t1.id')),
            Column('t2id', Integer, ForeignKey('t2.id'))
            )

    def test_basic(self):
        class T1(object):pass
        class T2(object):pass

        mapper(T1, t1, properties={
            't2s_1':relationship(T2, secondary=t1t2_1),
            't2s_2':relationship(T2, secondary=t1t2_2),
        })
        mapper(T2, t2)

        q = create_session().query(T1).join('t2s_1').filter(t2.c.id==5).reset_joinpoint().join('t2s_2')
        self.assert_compile(
            q,
            "SELECT t1.id AS t1_id, t1.data AS t1_data FROM t1 JOIN t1t2_1 AS t1t2_1_1 "
            "ON t1.id = t1t2_1_1.t1id JOIN t2 ON t2.id = t1t2_1_1.t2id JOIN t1t2_2 AS t1t2_2_1 "
            "ON t1.id = t1t2_2_1.t1id JOIN t2 ON t2.id = t1t2_2_1.t2id WHERE t2.id = :id_1"
            , use_default_dialect=True
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

    def test_joins(self):
        for j in (
            ['orders', 'items'],
            ['orders_syn', 'items'],
            ['orders', 'items_syn'],
            ['orders_syn', 'items_syn'],
        ):
            result = create_session().query(User).join(*j).filter_by(id=3).all()
            assert [User(id=7, name='jack'), User(id=9, name='fred')] == result

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

class InstancesTest(QueryTest, AssertsCompiledSQL):

    def test_from_alias(self):

        query = users.select(users.c.id==7).union(users.select(users.c.id>7)).alias('ulist').outerjoin(addresses).select(use_labels=True,order_by=['ulist.id', addresses.c.id])
        sess =create_session()
        q = sess.query(User)

        def go():
            l = list(q.options(contains_alias('ulist'), contains_eager('addresses')).instances(query.execute()))
            assert self.static.user_address_result == l
        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()

        def go():
            l = q.options(contains_alias('ulist'), contains_eager('addresses')).from_statement(query).all()
            assert self.static.user_address_result == l
        self.assert_sql_count(testing.db, go, 1)

        # better way.  use select_from()
        def go():
            l = sess.query(User).select_from(query).options(contains_eager('addresses')).all()
            assert self.static.user_address_result == l
        self.assert_sql_count(testing.db, go, 1)

        # same thing, but alias addresses, so that the adapter generated by select_from() is wrapped within
        # the adapter created by contains_eager()
        adalias = addresses.alias()
        query = users.select(users.c.id==7).union(users.select(users.c.id>7)).alias('ulist').outerjoin(adalias).select(use_labels=True,order_by=['ulist.id', adalias.c.id])
        def go():
            l = sess.query(User).select_from(query).options(contains_eager('addresses', alias=adalias)).all()
            assert self.static.user_address_result == l
        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager(self):
        sess = create_session()

        # test that contains_eager suppresses the normal outer join rendering
        q = sess.query(User).outerjoin(User.addresses).options(contains_eager(User.addresses)).order_by(User.id)
        self.assert_compile(q.with_labels().statement, 
            "SELECT addresses.id AS addresses_id, addresses.user_id AS addresses_user_id, "\
            "addresses.email_address AS addresses_email_address, users.id AS users_id, "\
            "users.name AS users_name FROM users LEFT OUTER JOIN addresses "\
            "ON users.id = addresses.user_id ORDER BY users.id"
            , dialect=default.DefaultDialect())
                
        def go():
            assert self.static.user_address_result == q.all()
        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        adalias = addresses.alias()
        q = sess.query(User).select_from(users.outerjoin(adalias)).options(contains_eager(User.addresses, alias=adalias))
        def go():
            eq_(self.static.user_address_result, q.order_by(User.id).all())
        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        selectquery = users.outerjoin(addresses).select(users.c.id<10, use_labels=True, order_by=[users.c.id, addresses.c.id])
        q = sess.query(User)

        def go():
            l = list(q.options(contains_eager('addresses')).instances(selectquery.execute()))
            assert self.static.user_address_result[0:3] == l
        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()

        def go():
            l = list(q.options(contains_eager(User.addresses)).instances(selectquery.execute()))
            assert self.static.user_address_result[0:3] == l
        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        def go():
            l = q.options(contains_eager('addresses')).from_statement(selectquery).all()
            assert self.static.user_address_result[0:3] == l
        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager_alias(self):
        adalias = addresses.alias('adalias')
        selectquery = users.outerjoin(adalias).select(use_labels=True, order_by=[users.c.id, adalias.c.id])
        sess = create_session()
        q = sess.query(User)
    
        # string alias name
        def go():
            l = list(q.options(contains_eager('addresses', alias="adalias")).instances(selectquery.execute()))
            assert self.static.user_address_result == l
        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        # expression.Alias object
        def go():
            l = list(q.options(contains_eager('addresses', alias=adalias)).instances(selectquery.execute()))
            assert self.static.user_address_result == l
        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()

        # Aliased object
        adalias = aliased(Address)
        def go():
            l = q.options(contains_eager('addresses', alias=adalias)).outerjoin((adalias, User.addresses)).order_by(User.id, adalias.id)
            assert self.static.user_address_result == l.all()
        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        oalias = orders.alias('o1')
        ialias = items.alias('i1')
        query = users.outerjoin(oalias).outerjoin(order_items).outerjoin(ialias).select(use_labels=True).order_by(users.c.id, oalias.c.id, ialias.c.id)
        q = create_session().query(User)
        # test using string alias with more than one level deep
        def go():
            l = list(q.options(contains_eager('orders', alias='o1'), contains_eager('orders.items', alias='i1')).instances(query.execute()))
            assert self.static.user_order_result == l
        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()

        # test using Alias with more than one level deep
        def go():
            l = list(q.options(contains_eager('orders', alias=oalias), contains_eager('orders.items', alias=ialias)).instances(query.execute()))
            assert self.static.user_order_result == l
        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        # test using Aliased with more than one level deep
        oalias = aliased(Order)
        ialias = aliased(Item)
        def go():
            l = q.options(contains_eager(User.orders, alias=oalias), contains_eager(User.orders, Order.items, alias=ialias)).\
                outerjoin((oalias, User.orders), (ialias, oalias.items)).order_by(User.id, oalias.id, ialias.id)
            assert self.static.user_order_result == l.all()
        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

    def test_mixed_eager_contains_with_limit(self):
        sess = create_session()
    
        q = sess.query(User)
        def go():
            # outerjoin to User.orders, offset 1/limit 2 so we get user 7 + second two orders.
            # then joinedload the addresses.  User + Order columns go into the subquery, address
            # left outer joins to the subquery, joinedloader for User.orders applies context.adapter 
            # to result rows.  This was [ticket:1180].
            l = q.outerjoin(User.orders).options(joinedload(User.addresses), contains_eager(User.orders)).order_by(User.id, Order.id).offset(1).limit(2).all()
            eq_(l, [User(id=7,
            addresses=[Address(email_address=u'jack@bean.com',user_id=7,id=1)],
            name=u'jack',
            orders=[
                Order(address_id=1,user_id=7,description=u'order 3',isopen=1,id=3), 
                Order(address_id=None,user_id=7,description=u'order 5',isopen=0,id=5)
            ])])
        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        def go():
            # same as above, except Order is aliased, so two adapters are applied by the
            # eager loader
            oalias = aliased(Order)
            l = q.outerjoin((User.orders, oalias)).options(joinedload(User.addresses), contains_eager(User.orders, alias=oalias)).order_by(User.id, oalias.id).offset(1).limit(2).all()
            eq_(l, [User(id=7,
            addresses=[Address(email_address=u'jack@bean.com',user_id=7,id=1)],
            name=u'jack',
            orders=[
                Order(address_id=1,user_id=7,description=u'order 3',isopen=1,id=3), 
                Order(address_id=None,user_id=7,description=u'order 5',isopen=0,id=5)
            ])])
        self.assert_sql_count(testing.db, go, 1)
    
    
class MixedEntitiesTest(QueryTest, AssertsCompiledSQL):

    def test_values(self):
        sess = create_session()

        assert list(sess.query(User).values()) == list()

        sel = users.select(User.id.in_([7, 8])).alias()
        q = sess.query(User)
        q2 = q.select_from(sel).values(User.name)
        eq_(list(q2), [(u'jack',), (u'ed',)])
    
        q = sess.query(User)
        q2 = q.order_by(User.id).values(User.name, User.name + " " + cast(User.id, String(50)))
        eq_(list(q2), [(u'jack', u'jack 7'), (u'ed', u'ed 8'), (u'fred', u'fred 9'), (u'chuck', u'chuck 10')])
    
        q2 = q.join('addresses').filter(User.name.like('%e%')).order_by(User.id, Address.id).values(User.name, Address.email_address)
        eq_(list(q2), [(u'ed', u'ed@wood.com'), (u'ed', u'ed@bettyboop.com'), (u'ed', u'ed@lala.com'), (u'fred', u'fred@fred.com')])
    
        q2 = q.join('addresses').filter(User.name.like('%e%')).order_by(desc(Address.email_address)).slice(1, 3).values(User.name, Address.email_address)
        eq_(list(q2), [(u'ed', u'ed@wood.com'), (u'ed', u'ed@lala.com')])
    
        adalias = aliased(Address)
        q2 = q.join(('addresses', adalias)).filter(User.name.like('%e%')).values(User.name, adalias.email_address)
        eq_(list(q2), [(u'ed', u'ed@wood.com'), (u'ed', u'ed@bettyboop.com'), (u'ed', u'ed@lala.com'), (u'fred', u'fred@fred.com')])
    
        q2 = q.values(func.count(User.name))
        assert q2.next() == (4,)

        q2 = q.select_from(sel).filter(User.id==8).values(User.name, sel.c.name, User.name)
        eq_(list(q2), [(u'ed', u'ed', u'ed')])

        # using User.xxx is alised against "sel", so this query returns nothing
        q2 = q.select_from(sel).filter(User.id==8).filter(User.id>sel.c.id).values(User.name, sel.c.name, User.name)
        eq_(list(q2), [])

        # whereas this uses users.c.xxx, is not aliased and creates a new join
        q2 = q.select_from(sel).filter(users.c.id==8).filter(users.c.id>sel.c.id).values(users.c.name, sel.c.name, User.name)
        eq_(list(q2), [(u'ed', u'jack', u'jack')])

    @testing.fails_on('mssql', 'FIXME: unknown')
    def test_values_specific_order_by(self):
        sess = create_session()

        assert list(sess.query(User).values()) == list()

        sel = users.select(User.id.in_([7, 8])).alias()
        q = sess.query(User)
        u2 = aliased(User)
        q2 = q.select_from(sel).filter(u2.id>1).order_by(User.id, sel.c.id, u2.id).values(User.name, sel.c.name, u2.name)
        eq_(list(q2), [(u'jack', u'jack', u'jack'), (u'jack', u'jack', u'ed'), (u'jack', u'jack', u'fred'), (u'jack', u'jack', u'chuck'), (u'ed', u'ed', u'jack'), (u'ed', u'ed', u'ed'), (u'ed', u'ed', u'fred'), (u'ed', u'ed', u'chuck')])

    @testing.fails_on('mssql', 'FIXME: unknown')
    @testing.fails_on('oracle', "Oracle doesn't support boolean expressions as columns")
    @testing.fails_on('postgresql+pg8000', "pg8000 parses the SQL itself before passing on to PG, doesn't parse this")
    @testing.fails_on('postgresql+zxjdbc', "zxjdbc parses the SQL itself before passing on to PG, doesn't parse this")
    def test_values_with_boolean_selects(self):
        """Tests a values clause that works with select boolean evaluations"""
        sess = create_session()

        q = sess.query(User)
        q2 = q.group_by(User.name.like('%j%')).order_by(desc(User.name.like('%j%'))).values(User.name.like('%j%'), func.count(User.name.like('%j%')))
        eq_(list(q2), [(True, 1), (False, 3)])

        q2 = q.order_by(desc(User.name.like('%j%'))).values(User.name.like('%j%'))
        eq_(list(q2), [(True,), (False,), (False,), (False,)])


    def test_correlated_subquery(self):
        """test that a subquery constructed from ORM attributes doesn't leak out 
        those entities to the outermost query.
    
        """
        sess = create_session()
    
        subq = select([func.count()]).\
            where(User.id==Address.user_id).\
            correlate(users).\
            label('count')

        # we don't want Address to be outside of the subquery here
        eq_(
            list(sess.query(User, subq)[0:3]),
            [(User(id=7,name=u'jack'), 1), (User(id=8,name=u'ed'), 3), (User(id=9,name=u'fred'), 1)]
            )

        # same thing without the correlate, as it should
        # not be needed
        subq = select([func.count()]).\
            where(User.id==Address.user_id).\
            label('count')

        # we don't want Address to be outside of the subquery here
        eq_(
            list(sess.query(User, subq)[0:3]),
            [(User(id=7,name=u'jack'), 1), (User(id=8,name=u'ed'), 3), (User(id=9,name=u'fred'), 1)]
            )

    def test_tuple_labeling(self):
        sess = create_session()
        
        # test pickle + all the protocols !
        for pickled in False, -1, 0, 1, 2:
            for row in sess.query(User, Address).join(User.addresses).all():
                if pickled is not False:
                    row = util.pickle.loads(util.pickle.dumps(row, pickled))
                    
                eq_(set(row.keys()), set(['User', 'Address']))
                eq_(row.User, row[0])
                eq_(row.Address, row[1])
        
            for row in sess.query(User.name, User.id.label('foobar')):
                if pickled is not False:
                    row = util.pickle.loads(util.pickle.dumps(row, pickled))
                eq_(set(row.keys()), set(['name', 'foobar']))
                eq_(row.name, row[0])
                eq_(row.foobar, row[1])

            for row in sess.query(User).values(User.name, User.id.label('foobar')):
                if pickled is not False:
                    row = util.pickle.loads(util.pickle.dumps(row, pickled))
                eq_(set(row.keys()), set(['name', 'foobar']))
                eq_(row.name, row[0])
                eq_(row.foobar, row[1])

            oalias = aliased(Order)
            for row in sess.query(User, oalias).join(User.orders).all():
                if pickled is not False:
                    row = util.pickle.loads(util.pickle.dumps(row, pickled))
                eq_(set(row.keys()), set(['User']))
                eq_(row.User, row[0])

            oalias = aliased(Order, name='orders')
            for row in sess.query(User, oalias).join(User.orders).all():
                if pickled is not False:
                    row = util.pickle.loads(util.pickle.dumps(row, pickled))
                eq_(set(row.keys()), set(['User', 'orders']))
                eq_(row.User, row[0])
                eq_(row.orders, row[1])
            
            if pickled is not False:
                ret = sess.query(User, Address).join(User.addresses).all()
                util.pickle.loads(util.pickle.dumps(ret, pickled))
                
    def test_column_queries(self):
        sess = create_session()

        eq_(sess.query(User.name).all(), [(u'jack',), (u'ed',), (u'fred',), (u'chuck',)])
    
        sel = users.select(User.id.in_([7, 8])).alias()
        q = sess.query(User.name)
        q2 = q.select_from(sel).all()
        eq_(list(q2), [(u'jack',), (u'ed',)])

        eq_(sess.query(User.name, Address.email_address).filter(User.id==Address.user_id).all(), [
            (u'jack', u'jack@bean.com'), (u'ed', u'ed@wood.com'), 
            (u'ed', u'ed@bettyboop.com'), (u'ed', u'ed@lala.com'), 
            (u'fred', u'fred@fred.com')
        ])
    
        eq_(sess.query(User.name, func.count(Address.email_address)).outerjoin(User.addresses).group_by(User.id, User.name).order_by(User.id).all(), 
            [(u'jack', 1), (u'ed', 3), (u'fred', 1), (u'chuck', 0)]
        )

        eq_(sess.query(User, func.count(Address.email_address)).outerjoin(User.addresses).group_by(User).order_by(User.id).all(), 
            [(User(name='jack',id=7), 1), (User(name='ed',id=8), 3), (User(name='fred',id=9), 1), (User(name='chuck',id=10), 0)]
        )

        eq_(sess.query(func.count(Address.email_address), User).outerjoin(User.addresses).group_by(User).order_by(User.id).all(), 
            [(1, User(name='jack',id=7)), (3, User(name='ed',id=8)), (1, User(name='fred',id=9)), (0, User(name='chuck',id=10))]
        )
    
        adalias = aliased(Address)
        eq_(sess.query(User, func.count(adalias.email_address)).outerjoin(('addresses', adalias)).group_by(User).order_by(User.id).all(), 
            [(User(name='jack',id=7), 1), (User(name='ed',id=8), 3), (User(name='fred',id=9), 1), (User(name='chuck',id=10), 0)]
        )

        eq_(sess.query(func.count(adalias.email_address), User).outerjoin((User.addresses, adalias)).group_by(User).order_by(User.id).all(),
            [(1, User(name=u'jack',id=7)), (3, User(name=u'ed',id=8)), (1, User(name=u'fred',id=9)), (0, User(name=u'chuck',id=10))]
        )

        # select from aliasing + explicit aliasing
        eq_(
            sess.query(User, adalias.email_address, adalias.id).outerjoin((User.addresses, adalias)).from_self(User, adalias.email_address).order_by(User.id, adalias.id).all(),
            [
                (User(name=u'jack',id=7), u'jack@bean.com'), 
                (User(name=u'ed',id=8), u'ed@wood.com'), 
                (User(name=u'ed',id=8), u'ed@bettyboop.com'),
                (User(name=u'ed',id=8), u'ed@lala.com'), 
                (User(name=u'fred',id=9), u'fred@fred.com'), 
                (User(name=u'chuck',id=10), None)
            ]
        )
    
        # anon + select from aliasing
        eq_(
            sess.query(User).join(User.addresses, aliased=True).filter(Address.email_address.like('%ed%')).from_self().all(),
            [
                User(name=u'ed',id=8), 
                User(name=u'fred',id=9), 
            ]
        )

        # test eager aliasing, with/without select_from aliasing
        for q in [
            sess.query(User, adalias.email_address).outerjoin((User.addresses, adalias)).options(joinedload(User.addresses)).order_by(User.id, adalias.id).limit(10),
            sess.query(User, adalias.email_address, adalias.id).outerjoin((User.addresses, adalias)).from_self(User, adalias.email_address).options(joinedload(User.addresses)).order_by(User.id, adalias.id).limit(10),
        ]:
            eq_(

                q.all(),
                [(User(addresses=[Address(user_id=7,email_address=u'jack@bean.com',id=1)],name=u'jack',id=7), u'jack@bean.com'), 
                (User(addresses=[
                                    Address(user_id=8,email_address=u'ed@wood.com',id=2), 
                                    Address(user_id=8,email_address=u'ed@bettyboop.com',id=3), 
                                    Address(user_id=8,email_address=u'ed@lala.com',id=4)],name=u'ed',id=8), u'ed@wood.com'), 
                (User(addresses=[
                            Address(user_id=8,email_address=u'ed@wood.com',id=2), 
                            Address(user_id=8,email_address=u'ed@bettyboop.com',id=3), 
                            Address(user_id=8,email_address=u'ed@lala.com',id=4)],name=u'ed',id=8), u'ed@bettyboop.com'), 
                (User(addresses=[
                            Address(user_id=8,email_address=u'ed@wood.com',id=2), 
                            Address(user_id=8,email_address=u'ed@bettyboop.com',id=3), 
                            Address(user_id=8,email_address=u'ed@lala.com',id=4)],name=u'ed',id=8), u'ed@lala.com'), 
                (User(addresses=[Address(user_id=9,email_address=u'fred@fred.com',id=5)],name=u'fred',id=9), u'fred@fred.com'), 

                (User(addresses=[],name=u'chuck',id=10), None)]
        )

    def test_column_from_limited_joinedload(self):
        sess = create_session()
    
        def go():
            results = sess.query(User).limit(1).options(joinedload('addresses')).add_column(User.name).all()
            eq_(results, [(User(name='jack'), 'jack')])
        self.assert_sql_count(testing.db, go, 1)
    
    @testing.fails_on('postgresql+pg8000', "'type oid 705 not mapped to py type' (due to literal)")
    def test_self_referential(self):
    
        sess = create_session()
        oalias = aliased(Order)

        for q in [
            sess.query(Order, oalias).filter(Order.user_id==oalias.user_id).filter(Order.user_id==7).filter(Order.id>oalias.id).order_by(Order.id, oalias.id),
            sess.query(Order, oalias).from_self().filter(Order.user_id==oalias.user_id).filter(Order.user_id==7).filter(Order.id>oalias.id).order_by(Order.id, oalias.id),
        
            # same thing, but reversed.  
            sess.query(oalias, Order).from_self().filter(oalias.user_id==Order.user_id).filter(oalias.user_id==7).filter(Order.id<oalias.id).order_by(oalias.id, Order.id),
        
            # here we go....two layers of aliasing
            sess.query(Order, oalias).filter(Order.user_id==oalias.user_id).filter(Order.user_id==7).filter(Order.id>oalias.id).from_self().order_by(Order.id, oalias.id).limit(10).options(joinedload(Order.items)),

            # gratuitous four layers
            sess.query(Order, oalias).filter(Order.user_id==oalias.user_id).filter(Order.user_id==7).filter(Order.id>oalias.id).from_self().from_self().from_self().order_by(Order.id, oalias.id).limit(10).options(joinedload(Order.items)),

        ]:
    
            eq_(
            q.all(),
            [
                (Order(address_id=1,description=u'order 3',isopen=1,user_id=7,id=3), Order(address_id=1,description=u'order 1',isopen=0,user_id=7,id=1)), 
                (Order(address_id=None,description=u'order 5',isopen=0,user_id=7,id=5), Order(address_id=1,description=u'order 1',isopen=0,user_id=7,id=1)), 
                (Order(address_id=None,description=u'order 5',isopen=0,user_id=7,id=5), Order(address_id=1,description=u'order 3',isopen=1,user_id=7,id=3))                
            ]
        )
        
        
        # ensure column expressions are taken from inside the subquery, not restated at the top
        q = sess.query(Order.id, Order.description, literal_column("'q'").label('foo')).\
            filter(Order.description == u'order 3').from_self()
        self.assert_compile(q, 
            "SELECT anon_1.orders_id AS anon_1_orders_id, anon_1.orders_description AS anon_1_orders_description, "
            "anon_1.foo AS anon_1_foo FROM (SELECT orders.id AS orders_id, orders.description AS orders_description, "
            "'q' AS foo FROM orders WHERE orders.description = :description_1) AS anon_1", use_default_dialect=True)
        eq_(
            q.all(),
            [(3, u'order 3', 'q')]
        )
        
    
    def test_multi_mappers(self):

        test_session = create_session()

        (user7, user8, user9, user10) = test_session.query(User).all()
        (address1, address2, address3, address4, address5) = test_session.query(Address).all()

        expected = [(user7, address1),
            (user8, address2),
            (user8, address3),
            (user8, address4),
            (user9, address5),
            (user10, None)]

        sess = create_session()

        selectquery = users.outerjoin(addresses).select(use_labels=True, order_by=[users.c.id, addresses.c.id])
        eq_(list(sess.query(User, Address).instances(selectquery.execute())), expected)
        sess.expunge_all()

        for address_entity in (Address, aliased(Address)):
            q = sess.query(User).add_entity(address_entity).outerjoin(('addresses', address_entity)).order_by(User.id, address_entity.id)
            eq_(q.all(), expected)
            sess.expunge_all()

            q = sess.query(User).add_entity(address_entity)
            q = q.join(('addresses', address_entity)).filter_by(email_address='ed@bettyboop.com')
            eq_(q.all(), [(user8, address3)])
            sess.expunge_all()

            q = sess.query(User, address_entity).join(('addresses', address_entity)).filter_by(email_address='ed@bettyboop.com')
            eq_(q.all(), [(user8, address3)])
            sess.expunge_all()

            q = sess.query(User, address_entity).join(('addresses', address_entity)).options(joinedload('addresses')).filter_by(email_address='ed@bettyboop.com')
            eq_(list(util.OrderedSet(q.all())), [(user8, address3)])
            sess.expunge_all()

    def test_aliased_multi_mappers(self):
        sess = create_session()

        (user7, user8, user9, user10) = sess.query(User).all()
        (address1, address2, address3, address4, address5) = sess.query(Address).all()

        expected = [(user7, address1),
            (user8, address2),
            (user8, address3),
            (user8, address4),
            (user9, address5),
            (user10, None)]

        q = sess.query(User)
        adalias = addresses.alias('adalias')
        q = q.add_entity(Address, alias=adalias).select_from(users.outerjoin(adalias))
        l = q.order_by(User.id, adalias.c.id).all()
        assert l == expected

        sess.expunge_all()

        q = sess.query(User).add_entity(Address, alias=adalias)
        l = q.select_from(users.outerjoin(adalias)).filter(adalias.c.email_address=='ed@bettyboop.com').all()
        assert l == [(user8, address3)]

    def test_multi_columns(self):
        sess = create_session()

        expected = [(u, u.name) for u in sess.query(User).all()]

        for add_col in (User.name, users.c.name):
            assert sess.query(User).add_column(add_col).all() == expected
            sess.expunge_all()

        assert_raises(sa_exc.InvalidRequestError, sess.query(User).add_column, object())

    def test_add_multi_columns(self):
        """test that add_column accepts a FROM clause."""
    
        sess = create_session()
    
        eq_(
            sess.query(User.id).add_column(users).all(),
            [(7, 7, u'jack'), (8, 8, u'ed'), (9, 9, u'fred'), (10, 10, u'chuck')]
        )
    
    def test_multi_columns_2(self):
        """test aliased/nonalised joins with the usage of add_column()"""
        sess = create_session()

        (user7, user8, user9, user10) = sess.query(User).all()
        expected = [(user7, 1),
            (user8, 3),
            (user9, 1),
            (user10, 0)
            ]

        q = sess.query(User)
        q = q.group_by(users).order_by(User.id).outerjoin('addresses').add_column(func.count(Address.id).label('count'))
        eq_(q.all(), expected)
        sess.expunge_all()
    
        adalias = aliased(Address)
        q = sess.query(User)
        q = q.group_by(users).order_by(User.id).outerjoin(('addresses', adalias)).add_column(func.count(adalias.id).label('count'))
        eq_(q.all(), expected)
        sess.expunge_all()

        # TODO: figure out why group_by(users) doesn't work here
        s = select([users, func.count(addresses.c.id).label('count')]).select_from(users.outerjoin(addresses)).group_by(*[c for c in users.c]).order_by(User.id)
        q = sess.query(User)
        l = q.add_column("count").from_statement(s).all()
        assert l == expected


    def test_raw_columns(self):
        sess = create_session()
        (user7, user8, user9, user10) = sess.query(User).all()
        expected = [
            (user7, 1, "Name:jack"),
            (user8, 3, "Name:ed"),
            (user9, 1, "Name:fred"),
            (user10, 0, "Name:chuck")]

        adalias = addresses.alias()
        q = create_session().query(User).add_column(func.count(adalias.c.id))\
            .add_column(("Name:" + users.c.name)).outerjoin(('addresses', adalias))\
            .group_by(users).order_by(users.c.id)

        assert q.all() == expected

        # test with a straight statement
        s = select([users, func.count(addresses.c.id).label('count'), ("Name:" + users.c.name).label('concat')], from_obj=[users.outerjoin(addresses)], group_by=[c for c in users.c], order_by=[users.c.id])
        q = create_session().query(User)
        l = q.add_column("count").add_column("concat").from_statement(s).all()
        assert l == expected

        sess.expunge_all()

        # test with select_from()
        q = create_session().query(User).add_column(func.count(addresses.c.id))\
            .add_column(("Name:" + users.c.name)).select_from(users.outerjoin(addresses))\
            .group_by(users).order_by(users.c.id)

        assert q.all() == expected
        sess.expunge_all()

        q = create_session().query(User).add_column(func.count(addresses.c.id))\
            .add_column(("Name:" + users.c.name)).outerjoin('addresses')\
            .group_by(users).order_by(users.c.id)

        assert q.all() == expected
        sess.expunge_all()

        q = create_session().query(User).add_column(func.count(adalias.c.id))\
            .add_column(("Name:" + users.c.name)).outerjoin(('addresses', adalias))\
            .group_by(users).order_by(users.c.id)

        assert q.all() == expected
        sess.expunge_all()

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


class SelectFromTest(QueryTest, AssertsCompiledSQL):
    run_setup_mappers = None

    def test_replace_with_select(self):
        mapper(User, users, properties = {
            'addresses':relationship(Address)
        })
        mapper(Address, addresses)

        sel = users.select(users.c.id.in_([7, 8])).alias()
        sess = create_session()

        eq_(sess.query(User).select_from(sel).all(), [User(id=7), User(id=8)])

        eq_(sess.query(User).select_from(sel).filter(User.id==8).all(), [User(id=8)])

        eq_(sess.query(User).select_from(sel).order_by(desc(User.name)).all(), [
            User(name='jack',id=7), User(name='ed',id=8)
        ])

        eq_(sess.query(User).select_from(sel).order_by(asc(User.name)).all(), [
            User(name='ed',id=8), User(name='jack',id=7)
        ])

        eq_(sess.query(User).select_from(sel).options(joinedload('addresses')).first(),
            User(name='jack', addresses=[Address(id=1)])
        )

    def test_join_mapper_order_by(self):
        """test that mapper-level order_by is adapted to a selectable."""
    
        mapper(User, users, order_by=users.c.id)

        sel = users.select(users.c.id.in_([7, 8]))
        sess = create_session()

        eq_(sess.query(User).select_from(sel).all(),
            [
                User(name='jack',id=7), User(name='ed',id=8)
            ]
        )

    def test_differentiate_self_external(self):
        """test some different combinations of joining a table to a subquery of itself."""
        
        mapper(User, users)
        
        sess = create_session()

        sel = sess.query(User).filter(User.id.in_([7, 8])).subquery()
        ualias = aliased(User)
        
        self.assert_compile(
            sess.query(User).join((sel, User.id>sel.c.id)),
            "SELECT users.id AS users_id, users.name AS users_name FROM "
            "users JOIN (SELECT users.id AS id, users.name AS name FROM "
            "users WHERE users.id IN (:id_1, :id_2)) AS anon_1 ON users.id > anon_1.id",
            use_default_dialect=True
        )
    
        self.assert_compile(
            sess.query(ualias).select_from(sel).filter(ualias.id>sel.c.id),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name FROM "
            "users AS users_1, (SELECT users.id AS id, users.name AS name FROM "
            "users WHERE users.id IN (:id_1, :id_2)) AS anon_1 WHERE users_1.id > anon_1.id",
            use_default_dialect=True
        )

        # these two are essentially saying, "join ualias to ualias", so an 
        # error is raised.  join() deals with entities, not what's in
        # select_from().
        assert_raises(sa_exc.InvalidRequestError,
            sess.query(ualias).select_from(sel).join, (ualias, ualias.id>sel.c.id)
        )

        assert_raises(sa_exc.InvalidRequestError,
            sess.query(ualias).select_from(sel).join, (ualias, ualias.id>User.id)
        )

        salias = aliased(User, sel)
        self.assert_compile(
            sess.query(salias).join((ualias, ualias.id>salias.id)),
            "SELECT anon_1.id AS anon_1_id, anon_1.name AS anon_1_name FROM "
            "(SELECT users.id AS id, users.name AS name FROM users WHERE users.id "
            "IN (:id_1, :id_2)) AS anon_1 JOIN users AS users_1 ON users_1.id > anon_1.id",
            use_default_dialect=True
        )
        
        
        # this one uses an explicit join(left, right, onclause) so works
        self.assert_compile(
            sess.query(ualias).select_from(join(sel, ualias, ualias.id>sel.c.id)),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name FROM "
            "(SELECT users.id AS id, users.name AS name FROM users WHERE users.id "
            "IN (:id_1, :id_2)) AS anon_1 JOIN users AS users_1 ON users_1.id > anon_1.id",
            use_default_dialect=True
        )
        
        
        
    def test_join_no_order_by(self):
        mapper(User, users)

        sel = users.select(users.c.id.in_([7, 8]))
        sess = create_session()

        eq_(sess.query(User).select_from(sel).all(),
            [
                User(name='jack',id=7), User(name='ed',id=8)
            ]
        )

    def test_join(self):
        mapper(User, users, properties = {
            'addresses':relationship(Address)
        })
        mapper(Address, addresses)

        sel = users.select(users.c.id.in_([7, 8]))
        sess = create_session()

        eq_(sess.query(User).select_from(sel).join('addresses').
                    add_entity(Address).order_by(User.id).order_by(Address.id).all(),
            [
                (User(name='jack',id=7), Address(user_id=7,email_address='jack@bean.com',id=1)),
                (User(name='ed',id=8), Address(user_id=8,email_address='ed@wood.com',id=2)),
                (User(name='ed',id=8), Address(user_id=8,email_address='ed@bettyboop.com',id=3)),
                (User(name='ed',id=8), Address(user_id=8,email_address='ed@lala.com',id=4))
            ]
        )

        adalias = aliased(Address)
        eq_(sess.query(User).select_from(sel).join(('addresses', adalias)).
                    add_entity(adalias).order_by(User.id).order_by(adalias.id).all(),
            [
                (User(name='jack',id=7), Address(user_id=7,email_address='jack@bean.com',id=1)),
                (User(name='ed',id=8), Address(user_id=8,email_address='ed@wood.com',id=2)),
                (User(name='ed',id=8), Address(user_id=8,email_address='ed@bettyboop.com',id=3)),
                (User(name='ed',id=8), Address(user_id=8,email_address='ed@lala.com',id=4))
            ]
        )
    

    def test_more_joins(self):
        mapper(User, users, properties={
            'orders':relationship(Order, backref='user'), # o2m, m2o
        })
        mapper(Order, orders, properties={
            'items':relationship(Item, secondary=order_items, order_by=items.c.id),  #m2m
        })
        mapper(Item, items, properties={
            'keywords':relationship(Keyword, secondary=item_keywords, order_by=keywords.c.id) #m2m
        })
        mapper(Keyword, keywords)

        sel = users.select(users.c.id.in_([7, 8]))
        sess = create_session()
    
        eq_(sess.query(User).select_from(sel).join('orders', 'items', 'keywords').filter(Keyword.name.in_(['red', 'big', 'round'])).all(), [
            User(name=u'jack',id=7)
        ])

        eq_(sess.query(User).select_from(sel).join('orders', 'items', 'keywords', aliased=True).filter(Keyword.name.in_(['red', 'big', 'round'])).all(), [
            User(name=u'jack',id=7)
        ])

        def go():
            eq_(
                sess.query(User).select_from(sel).
                            options(joinedload_all('orders.items.keywords')).
                            join('orders', 'items', 'keywords', aliased=True).
                            filter(Keyword.name.in_(['red', 'big', 'round'])).all(), 
                [
                User(name=u'jack',orders=[
                    Order(description=u'order 1',items=[
                        Item(description=u'item 1',keywords=[Keyword(name=u'red'), Keyword(name=u'big'), Keyword(name=u'round')]),
                        Item(description=u'item 2',keywords=[Keyword(name=u'red',id=2), Keyword(name=u'small',id=5), Keyword(name=u'square')]),
                        Item(description=u'item 3',keywords=[Keyword(name=u'green',id=3), Keyword(name=u'big',id=4), Keyword(name=u'round',id=6)])
                    ]),
                    Order(description=u'order 3',items=[
                        Item(description=u'item 3',keywords=[Keyword(name=u'green',id=3), Keyword(name=u'big',id=4), Keyword(name=u'round',id=6)]),
                        Item(description=u'item 4',keywords=[],id=4),
                        Item(description=u'item 5',keywords=[],id=5)
                        ]),
                    Order(description=u'order 5',items=[Item(description=u'item 5',keywords=[])])])
                ])
        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()
        sel2 = orders.select(orders.c.id.in_([1,2,3]))
        eq_(sess.query(Order).select_from(sel2).join('items', 'keywords').filter(Keyword.name == 'red').order_by(Order.id).all(), [
            Order(description=u'order 1',id=1),
            Order(description=u'order 2',id=2),
        ])
        eq_(sess.query(Order).select_from(sel2).join('items', 'keywords', aliased=True).filter(Keyword.name == 'red').order_by(Order.id).all(), [
            Order(description=u'order 1',id=1),
            Order(description=u'order 2',id=2),
        ])


    def test_replace_with_eager(self):
        mapper(User, users, properties = {
            'addresses':relationship(Address, order_by=addresses.c.id)
        })
        mapper(Address, addresses)

        sel = users.select(users.c.id.in_([7, 8]))
        sess = create_session()

        def go():
            eq_(sess.query(User).options(joinedload('addresses')).select_from(sel).order_by(User.id).all(),
                [
                    User(id=7, addresses=[Address(id=1)]),
                    User(id=8, addresses=[Address(id=2), Address(id=3), Address(id=4)])
                ]
            )
        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        def go():
            eq_(sess.query(User).options(joinedload('addresses')).select_from(sel).filter(User.id==8).order_by(User.id).all(),
                [User(id=8, addresses=[Address(id=2), Address(id=3), Address(id=4)])]
            )
        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        def go():
            eq_(sess.query(User).options(joinedload('addresses')).select_from(sel).order_by(User.id)[1], User(id=8, addresses=[Address(id=2), Address(id=3), Address(id=4)]))
        self.assert_sql_count(testing.db, go, 1)

class CustomJoinTest(QueryTest):
    run_setup_mappers = None

    def test_double_same_mappers(self):
        """test aliasing of joins with a custom join condition"""
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items':relationship(Item, secondary=order_items, lazy='select', order_by=items.c.id),
        })
        mapper(Item, items)
        mapper(User, users, properties = dict(
            addresses = relationship(Address, lazy='select'),
            open_orders = relationship(Order, primaryjoin = and_(orders.c.isopen == 1, users.c.id==orders.c.user_id), lazy='select'),
            closed_orders = relationship(Order, primaryjoin = and_(orders.c.isopen == 0, users.c.id==orders.c.user_id), lazy='select')
        ))
        q = create_session().query(User)
        
        eq_(
            q.join('open_orders', 'items', aliased=True).filter(Item.id==4).\
                        join('closed_orders', 'items', aliased=True).filter(Item.id==3).all(),
            [User(id=7)]
        )

class SelfReferentialTest(_base.MappedTest, AssertsCompiledSQL):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        global nodes
        nodes = Table('nodes', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(30)))

    @classmethod
    def insert_data(cls):
        global Node
    
        class Node(Base):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relationship(Node, lazy='select', join_depth=3,
                backref=backref('parent', remote_side=[nodes.c.id])
            )
        })
        sess = create_session()
        n1 = Node(data='n1')
        n1.append(Node(data='n11'))
        n1.append(Node(data='n12'))
        n1.append(Node(data='n13'))
        n1.children[1].append(Node(data='n121'))
        n1.children[1].append(Node(data='n122'))
        n1.children[1].append(Node(data='n123'))
        sess.add(n1)
        sess.flush()
        sess.close()
    
    def test_join(self):
        sess = create_session()

        node = sess.query(Node).join('children', aliased=True).filter_by(data='n122').first()
        assert node.data=='n12'

        ret = sess.query(Node.data).join(Node.children, aliased=True).filter_by(data='n122').all()
        assert ret == [('n12',)]

    
        node = sess.query(Node).join('children', 'children', aliased=True).filter_by(data='n122').first()
        assert node.data=='n1'

        node = sess.query(Node).filter_by(data='n122').join('parent', aliased=True).filter_by(data='n12').\
            join('parent', aliased=True, from_joinpoint=True).filter_by(data='n1').first()
        assert node.data == 'n122'
    
    def test_string_or_prop_aliased(self):
        """test that join('foo') behaves the same as join(Cls.foo) in a self
        referential scenario.
        
        """
        
        sess = create_session()
        nalias = aliased(Node, sess.query(Node).filter_by(data='n1').subquery())
        
        q1 = sess.query(nalias).join(nalias.children, aliased=True).\
                join(Node.children, from_joinpoint=True)

        q2 = sess.query(nalias).join(nalias.children, aliased=True).\
                join("children", from_joinpoint=True)

        for q in (q1, q2):
            self.assert_compile(
                q,
                "SELECT anon_1.id AS anon_1_id, anon_1.parent_id AS "
                "anon_1_parent_id, anon_1.data AS anon_1_data FROM "
                "(SELECT nodes.id AS id, nodes.parent_id AS parent_id, "
                "nodes.data AS data FROM nodes WHERE nodes.data = :data_1) "
                "AS anon_1 JOIN nodes AS nodes_1 ON anon_1.id = "
                "nodes_1.parent_id JOIN nodes ON nodes_1.id = nodes.parent_id",
                use_default_dialect=True
            )
        
        q1 = sess.query(Node).join(nalias.children, aliased=True).\
                join(Node.children, aliased=True, from_joinpoint=True).\
                join(Node.children, from_joinpoint=True)

        q2 = sess.query(Node).join(nalias.children, aliased=True).\
                join("children", aliased=True, from_joinpoint=True).\
                join("children", from_joinpoint=True)
                
        for q in (q1, q2):
            self.assert_compile(
                q,
                "SELECT nodes.id AS nodes_id, nodes.parent_id AS "
                "nodes_parent_id, nodes.data AS nodes_data FROM (SELECT "
                "nodes.id AS id, nodes.parent_id AS parent_id, nodes.data "
                "AS data FROM nodes WHERE nodes.data = :data_1) AS anon_1 "
                "JOIN nodes AS nodes_1 ON anon_1.id = nodes_1.parent_id "
                "JOIN nodes AS nodes_2 ON nodes_1.id = nodes_2.parent_id "
                "JOIN nodes ON nodes_2.id = nodes.parent_id",
                use_default_dialect=True
            )
        
    def test_from_self_inside_excludes_outside(self):
        """test the propagation of aliased() from inside to outside
        on a from_self()..
        """
        sess = create_session()
        
        n1 = aliased(Node)
        
        # n1 is not inside the from_self(), so all cols must be maintained
        # on the outside
        self.assert_compile(
            sess.query(Node).filter(Node.data=='n122').from_self(n1, Node.id),
            "SELECT nodes_1.id AS nodes_1_id, nodes_1.parent_id AS nodes_1_parent_id, "
            "nodes_1.data AS nodes_1_data, anon_1.nodes_id AS anon_1_nodes_id "
            "FROM nodes AS nodes_1, (SELECT nodes.id AS nodes_id, "
            "nodes.parent_id AS nodes_parent_id, nodes.data AS nodes_data FROM "
            "nodes WHERE nodes.data = :data_1) AS anon_1",
            use_default_dialect=True
        )

        parent = aliased(Node)
        grandparent = aliased(Node)
        q = sess.query(Node, parent, grandparent).\
            join((Node.parent, parent), (parent.parent, grandparent)).\
                filter(Node.data=='n122').filter(parent.data=='n12').\
                filter(grandparent.data=='n1').from_self().limit(1)
        
        # parent, grandparent *are* inside the from_self(), so they 
        # should get aliased to the outside.
        self.assert_compile(
            q,
            "SELECT anon_1.nodes_id AS anon_1_nodes_id, "
            "anon_1.nodes_parent_id AS anon_1_nodes_parent_id, "
            "anon_1.nodes_data AS anon_1_nodes_data, "
            "anon_1.nodes_1_id AS anon_1_nodes_1_id, "
            "anon_1.nodes_1_parent_id AS anon_1_nodes_1_parent_id, "
            "anon_1.nodes_1_data AS anon_1_nodes_1_data, "
            "anon_1.nodes_2_id AS anon_1_nodes_2_id, "
            "anon_1.nodes_2_parent_id AS anon_1_nodes_2_parent_id, "
            "anon_1.nodes_2_data AS anon_1_nodes_2_data "
            "FROM (SELECT nodes.id AS nodes_id, nodes.parent_id "
            "AS nodes_parent_id, nodes.data AS nodes_data, "
            "nodes_1.id AS nodes_1_id, nodes_1.parent_id AS nodes_1_parent_id, "
            "nodes_1.data AS nodes_1_data, nodes_2.id AS nodes_2_id, "
            "nodes_2.parent_id AS nodes_2_parent_id, nodes_2.data AS "
            "nodes_2_data FROM nodes JOIN nodes AS nodes_1 ON "
            "nodes_1.id = nodes.parent_id JOIN nodes AS nodes_2 "
            "ON nodes_2.id = nodes_1.parent_id "
            "WHERE nodes.data = :data_1 AND nodes_1.data = :data_2 AND "
            "nodes_2.data = :data_3) AS anon_1  LIMIT 1",
            use_default_dialect=True
        )
        
    def test_explicit_join(self):
        sess = create_session()
    
        n1 = aliased(Node)
        n2 = aliased(Node)
    
        node = sess.query(Node).select_from(join(Node, n1, 'children')).filter(n1.data=='n122').first()
        assert node.data=='n12'
    
        node = sess.query(Node).select_from(join(Node, n1, 'children').join(n2, 'children')).\
            filter(n2.data=='n122').first()
        assert node.data=='n1'
    
        # mix explicit and named onclauses
        node = sess.query(Node).select_from(join(Node, n1, Node.id==n1.parent_id).join(n2, 'children')).\
            filter(n2.data=='n122').first()
        assert node.data=='n1'

        node = sess.query(Node).select_from(join(Node, n1, 'parent').join(n2, 'parent')).\
            filter(and_(Node.data=='n122', n1.data=='n12', n2.data=='n1')).first()
        assert node.data == 'n122'

        eq_(
            list(sess.query(Node).select_from(join(Node, n1, 'parent').join(n2, 'parent')).\
            filter(and_(Node.data=='n122', n1.data=='n12', n2.data=='n1')).values(Node.data, n1.data, n2.data)),
            [('n122', 'n12', 'n1')])

    def test_join_to_nonaliased(self):
        sess = create_session()
    
        n1 = aliased(Node)

        # using 'n1.parent' implicitly joins to unaliased Node
        eq_(
            sess.query(n1).join(n1.parent).filter(Node.data=='n1').all(),
            [Node(parent_id=1,data=u'n11',id=2), Node(parent_id=1,data=u'n12',id=3), Node(parent_id=1,data=u'n13',id=4)]
        )
    
        # explicit (new syntax)
        eq_(
            sess.query(n1).join((Node, n1.parent)).filter(Node.data=='n1').all(),
            [Node(parent_id=1,data=u'n11',id=2), Node(parent_id=1,data=u'n12',id=3), Node(parent_id=1,data=u'n13',id=4)]
        )
    
        
    def test_multiple_explicit_entities(self):
        sess = create_session()
    
        parent = aliased(Node)
        grandparent = aliased(Node)
        eq_(
            sess.query(Node, parent, grandparent).\
                join((Node.parent, parent), (parent.parent, grandparent)).\
                    filter(Node.data=='n122').filter(parent.data=='n12').\
                    filter(grandparent.data=='n1').first(),
            (Node(data='n122'), Node(data='n12'), Node(data='n1'))
        )

        eq_(
            sess.query(Node, parent, grandparent).\
                join((Node.parent, parent), (parent.parent, grandparent)).\
                    filter(Node.data=='n122').filter(parent.data=='n12').\
                    filter(grandparent.data=='n1').from_self().first(),
            (Node(data='n122'), Node(data='n12'), Node(data='n1'))
        )

        # same, change order around
        eq_(
            sess.query(parent, grandparent, Node).\
                join((Node.parent, parent), (parent.parent, grandparent)).\
                    filter(Node.data=='n122').filter(parent.data=='n12').\
                    filter(grandparent.data=='n1').from_self().first(),
            (Node(data='n12'), Node(data='n1'), Node(data='n122'))
        )

        eq_(
            sess.query(Node, parent, grandparent).\
                join((Node.parent, parent), (parent.parent, grandparent)).\
                    filter(Node.data=='n122').filter(parent.data=='n12').\
                    filter(grandparent.data=='n1').\
                    options(joinedload(Node.children)).first(),
            (Node(data='n122'), Node(data='n12'), Node(data='n1'))
        )

        eq_(
            sess.query(Node, parent, grandparent).\
                join((Node.parent, parent), (parent.parent, grandparent)).\
                    filter(Node.data=='n122').filter(parent.data=='n12').\
                    filter(grandparent.data=='n1').from_self().\
                    options(joinedload(Node.children)).first(),
            (Node(data='n122'), Node(data='n12'), Node(data='n1'))
        )
    
    
    def test_any(self):
        sess = create_session()
        eq_(sess.query(Node).filter(Node.children.any(Node.data=='n1')).all(), [])
        eq_(sess.query(Node).filter(Node.children.any(Node.data=='n12')).all(), [Node(data='n1')])
        eq_(sess.query(Node).filter(~Node.children.any()).order_by(Node.id).all(), 
                [Node(data='n11'), Node(data='n13'),Node(data='n121'),Node(data='n122'),Node(data='n123'),])

    def test_has(self):
        sess = create_session()
    
        eq_(sess.query(Node).filter(Node.parent.has(Node.data=='n12')).order_by(Node.id).all(), 
            [Node(data='n121'),Node(data='n122'),Node(data='n123')])
        eq_(sess.query(Node).filter(Node.parent.has(Node.data=='n122')).all(), [])
        eq_(sess.query(Node).filter(~Node.parent.has()).all(), [Node(data='n1')])

    def test_contains(self):
        sess = create_session()
    
        n122 = sess.query(Node).filter(Node.data=='n122').one()
        eq_(sess.query(Node).filter(Node.children.contains(n122)).all(), [Node(data='n12')])

        n13 = sess.query(Node).filter(Node.data=='n13').one()
        eq_(sess.query(Node).filter(Node.children.contains(n13)).all(), [Node(data='n1')])

    def test_eq_ne(self):
        sess = create_session()
    
        n12 = sess.query(Node).filter(Node.data=='n12').one()
        eq_(sess.query(Node).filter(Node.parent==n12).all(), [Node(data='n121'),Node(data='n122'),Node(data='n123')])
    
        eq_(sess.query(Node).filter(Node.parent != n12).all(), [Node(data='n1'), Node(data='n11'), Node(data='n12'), Node(data='n13')])

class SelfReferentialM2MTest(_base.MappedTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        global nodes, node_to_nodes
        nodes = Table('nodes', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(30)))
        
        node_to_nodes =Table('node_to_nodes', metadata,
            Column('left_node_id', Integer, ForeignKey('nodes.id'),primary_key=True),
            Column('right_node_id', Integer, ForeignKey('nodes.id'),primary_key=True),
            )

    @classmethod
    def insert_data(cls):
        global Node
    
        class Node(Base):
            pass

        mapper(Node, nodes, properties={
            'children':relationship(Node, lazy='select', secondary=node_to_nodes,
                primaryjoin=nodes.c.id==node_to_nodes.c.left_node_id,
                secondaryjoin=nodes.c.id==node_to_nodes.c.right_node_id,
            )
        })
        sess = create_session()
        n1 = Node(data='n1')
        n2 = Node(data='n2')
        n3 = Node(data='n3')
        n4 = Node(data='n4')
        n5 = Node(data='n5')
        n6 = Node(data='n6')
        n7 = Node(data='n7')
    
        n1.children = [n2, n3, n4]
        n2.children = [n3, n6, n7]
        n3.children = [n5, n4]

        sess.add(n1)
        sess.add(n2)
        sess.add(n3)
        sess.add(n4)
        sess.flush()
        sess.close()

    def test_any(self):
        sess = create_session()
        eq_(sess.query(Node).filter(Node.children.any(Node.data=='n3')).all(), [Node(data='n1'), Node(data='n2')])

    def test_contains(self):
        sess = create_session()
        n4 = sess.query(Node).filter_by(data='n4').one()

        eq_(sess.query(Node).filter(Node.children.contains(n4)).order_by(Node.data).all(), [Node(data='n1'), Node(data='n3')])
        eq_(sess.query(Node).filter(not_(Node.children.contains(n4))).order_by(Node.data).all(), [Node(data='n2'), Node(data='n4'), Node(data='n5'), Node(data='n6'), Node(data='n7')])

    def test_explicit_join(self):
        sess = create_session()
    
        n1 = aliased(Node)
        eq_(
            sess.query(Node).select_from(join(Node, n1, 'children')).filter(n1.data.in_(['n3', 'n7'])).order_by(Node.id).all(),
            [Node(data='n1'), Node(data='n2')]
        )
    
class ExternalColumnsTest(QueryTest):
    """test mappers with SQL-expressions added as column properties."""

    run_setup_mappers = None

    def test_external_columns_bad(self):

        assert_raises_message(sa_exc.ArgumentError, "not represented in the mapper's table", mapper, User, users, properties={
            'concat': (users.c.id * 2),
        })
        clear_mappers()

    def test_external_columns(self):
        """test querying mappings that reference external columns or selectables."""
    
        mapper(User, users, properties={
            'concat': column_property((users.c.id * 2)),
            'count': column_property(select([func.count(addresses.c.id)], users.c.id==addresses.c.user_id).correlate(users).as_scalar())
        })

        mapper(Address, addresses, properties={
            'user':relationship(User)
        })

        sess = create_session()
    
        sess.query(Address).options(joinedload('user')).all()

        eq_(sess.query(User).all(), 
            [
                User(id=7, concat=14, count=1),
                User(id=8, concat=16, count=3),
                User(id=9, concat=18, count=1),
                User(id=10, concat=20, count=0),
            ]
        )

        address_result = [
            Address(id=1, user=User(id=7, concat=14, count=1)),
            Address(id=2, user=User(id=8, concat=16, count=3)),
            Address(id=3, user=User(id=8, concat=16, count=3)),
            Address(id=4, user=User(id=8, concat=16, count=3)),
            Address(id=5, user=User(id=9, concat=18, count=1))
        ]
        eq_(sess.query(Address).all(), address_result)

        # run the eager version twice to test caching of aliased clauses
        for x in range(2):
            sess.expunge_all()
            def go():
               eq_(sess.query(Address).options(joinedload('user')).order_by(Address.id).all(), address_result)
            self.assert_sql_count(testing.db, go, 1)
    
        ualias = aliased(User)
        eq_(
            sess.query(Address, ualias).join(('user', ualias)).all(), 
            [(address, address.user) for address in address_result]
        )

        eq_(
                sess.query(Address, ualias.count).join(('user', ualias)).join('user', aliased=True).order_by(Address.id).all(),
                [
                    (Address(id=1), 1),
                    (Address(id=2), 3),
                    (Address(id=3), 3),
                    (Address(id=4), 3),
                    (Address(id=5), 1)
                ]
            )

        eq_(sess.query(Address, ualias.concat, ualias.count).join(('user', ualias)).join('user', aliased=True).order_by(Address.id).all(),
            [
                (Address(id=1), 14, 1),
                (Address(id=2), 16, 3),
                (Address(id=3), 16, 3),
                (Address(id=4), 16, 3),
                (Address(id=5), 18, 1)
            ]
        )

        ua = aliased(User)
        eq_(sess.query(Address, ua.concat, ua.count).
                    select_from(join(Address, ua, 'user')).
                    options(joinedload(Address.user)).order_by(Address.id).all(),
            [
                (Address(id=1, user=User(id=7, concat=14, count=1)), 14, 1),
                (Address(id=2, user=User(id=8, concat=16, count=3)), 16, 3),
                (Address(id=3, user=User(id=8, concat=16, count=3)), 16, 3),
                (Address(id=4, user=User(id=8, concat=16, count=3)), 16, 3),
                (Address(id=5, user=User(id=9, concat=18, count=1)), 18, 1)
            ]
        )

        eq_(list(sess.query(Address).join('user').values(Address.id, User.id, User.concat, User.count)), 
            [(1, 7, 14, 1), (2, 8, 16, 3), (3, 8, 16, 3), (4, 8, 16, 3), (5, 9, 18, 1)]
        )

        eq_(list(sess.query(Address, ua).select_from(join(Address,ua, 'user')).values(Address.id, ua.id, ua.concat, ua.count)), 
            [(1, 7, 14, 1), (2, 8, 16, 3), (3, 8, 16, 3), (4, 8, 16, 3), (5, 9, 18, 1)]
        )

    def test_external_columns_joinedload(self):
        # in this test, we have a subquery on User that accesses "addresses", underneath
        # an joinedload for "addresses".  So the "addresses" alias adapter needs to *not* hit 
        # the "addresses" table within the "user" subquery, but "user" still needs to be adapted.
        # therefore the long standing practice of eager adapters being "chained" has been removed
        # since its unnecessary and breaks this exact condition.
        mapper(User, users, properties={
            'addresses':relationship(Address, backref='user', order_by=addresses.c.id),
            'concat': column_property((users.c.id * 2)),
            'count': column_property(select([func.count(addresses.c.id)], users.c.id==addresses.c.user_id).correlate(users))
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'address':relationship(Address),  # m2o
        })

        sess = create_session()
        def go():
            o1 = sess.query(Order).options(joinedload_all('address.user')).get(1)
            eq_(o1.address.user.count, 1)
        self.assert_sql_count(testing.db, go, 1)

        sess = create_session()
        def go():
            o1 = sess.query(Order).options(joinedload_all('address.user')).first()
            eq_(o1.address.user.count, 1)
        self.assert_sql_count(testing.db, go, 1)

class TestOverlyEagerEquivalentCols(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global base, sub1, sub2
        base = Table('base', metadata, 
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(50))
        )

        sub1 = Table('sub1', metadata, 
            Column('id', Integer, ForeignKey('base.id'), primary_key=True),
            Column('data', String(50))
        )

        sub2 = Table('sub2', metadata, 
            Column('id', Integer, ForeignKey('base.id'), ForeignKey('sub1.id'), primary_key=True),
            Column('data', String(50))
        )

    def test_equivs(self):
        class Base(_base.ComparableEntity):
            pass
        class Sub1(_base.ComparableEntity):
            pass
        class Sub2(_base.ComparableEntity):
            pass
    
        mapper(Base, base, properties={
            'sub1':relationship(Sub1),
            'sub2':relationship(Sub2)
        })
    
        mapper(Sub1, sub1)
        mapper(Sub2, sub2)
        sess = create_session()
    
        s11 = Sub1(data='s11')
        s12 = Sub1(data='s12')
        s2 = Sub2(data='s2')
        b1 = Base(data='b1', sub1=[s11], sub2=[])
        b2 = Base(data='b1', sub1=[s12], sub2=[])
        sess.add(b1)
        sess.add(b2)
        sess.flush()
    
        # theres an overlapping ForeignKey here, so not much option except
        # to artifically control the flush order
        b2.sub2 = [s2]
        sess.flush()
    
        q = sess.query(Base).outerjoin('sub2', aliased=True)
        assert sub1.c.id not in q._filter_aliases.equivalents

        eq_(
            sess.query(Base).join('sub1').outerjoin('sub2', aliased=True).\
                filter(Sub1.id==1).one(),
                b1
        )
    
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
            'user': relationship(User, lazy='joined', backref=backref('documents', lazy='select'))
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
    """ Make sure a Query's execution_options are passed on to the
    resulting statement. """

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
