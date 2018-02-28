from sqlalchemy import (
    testing, null, exists, text, union, literal, literal_column, func, between,
    Unicode, desc, and_, bindparam, select, distinct, or_, collate, insert,
    Integer, String, Boolean, exc as sa_exc, util, cast, MetaData)
from sqlalchemy.sql import operators, expression
from sqlalchemy import column, table
from sqlalchemy.engine import default
from sqlalchemy.orm import (
    attributes, mapper, relationship, create_session, synonym, Session,
    aliased, column_property, joinedload_all, joinedload, Query, Bundle,
    subqueryload, backref, lazyload, defer)
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.schema import Table, Column
import sqlalchemy as sa
from sqlalchemy.testing.assertions import (
    eq_, assert_raises, assert_raises_message, expect_warnings,
    eq_ignore_whitespace)
from sqlalchemy.testing import fixtures, AssertsCompiledSQL, assert_warnings
from test.orm import _fixtures
from sqlalchemy.orm.util import join, with_parent
import contextlib
from sqlalchemy.testing import mock, is_, is_not_
from sqlalchemy import inspect


class QueryTest(_fixtures.FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()


class MiscTest(QueryTest):
    run_create_tables = None
    run_inserts = None

    def test_with_session(self):
        User = self.classes.User
        s1 = Session()
        s2 = Session()
        q1 = s1.query(User)
        q2 = q1.with_session(s2)
        assert q2.session is s2
        assert q1.session is s1


class OnlyReturnTuplesTest(QueryTest):
    def test_single_entity_false(self):
        User = self.classes.User
        row = create_session().query(User).only_return_tuples(False).first()
        assert isinstance(row, User)

    def test_single_entity_true(self):
        User = self.classes.User
        row = create_session().query(User).only_return_tuples(True).first()
        assert isinstance(row, tuple)

    def test_multiple_entity_false(self):
        User = self.classes.User
        row = create_session().query(User.id, User).only_return_tuples(False).first()
        assert isinstance(row, tuple)

    def test_multiple_entity_true(self):
        User = self.classes.User
        row = create_session().query(User.id, User).only_return_tuples(True).first()
        assert isinstance(row, tuple)


class RowTupleTest(QueryTest):
    run_setup_mappers = None

    def test_custom_names(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users, properties={'uname': users.c.name})

        row = create_session().query(User.id, User.uname).\
            filter(User.id == 7).first()
        assert row.id == 7
        assert row.uname == 'jack'

    def test_column_metadata(self):
        users, Address, addresses, User = (self.tables.users,
                                           self.classes.Address,
                                           self.tables.addresses,
                                           self.classes.User)

        mapper(User, users)
        mapper(Address, addresses)
        sess = create_session()
        user_alias = aliased(User)
        user_alias_id_label = user_alias.id.label('foo')
        address_alias = aliased(Address, name='aalias')
        fn = func.count(User.id)
        name_label = User.name.label('uname')
        bundle = Bundle('b1', User.id, User.name)
        cte = sess.query(User.id).cte()
        for q, asserted in [
            (
                sess.query(User),
                [
                    {
                        'name': 'User', 'type': User, 'aliased': False,
                        'expr': User, 'entity': User}]
            ),
            (
                sess.query(User.id, User),
                [
                    {
                        'name': 'id', 'type': users.c.id.type,
                        'aliased': False, 'expr': User.id, 'entity': User},
                    {
                        'name': 'User', 'type': User, 'aliased': False,
                        'expr': User, 'entity': User}
                ]
            ),
            (
                sess.query(User.id, user_alias),
                [
                    {
                        'name': 'id', 'type': users.c.id.type,
                        'aliased': False, 'expr': User.id, 'entity': User},
                    {
                        'name': None, 'type': User, 'aliased': True,
                        'expr': user_alias, 'entity': user_alias}
                ]
            ),
            (
                sess.query(user_alias.id),
                [
                    {
                        'name': 'id', 'type': users.c.id.type,
                        'aliased': True, 'expr': user_alias.id,
                        'entity': user_alias},
                ]
            ),
            (
                sess.query(user_alias_id_label),
                [
                    {
                        'name': 'foo', 'type': users.c.id.type,
                        'aliased': True, 'expr': user_alias_id_label,
                        'entity': user_alias},
                ]
            ),
            (
                sess.query(address_alias),
                [
                    {
                        'name': 'aalias', 'type': Address, 'aliased': True,
                        'expr': address_alias, 'entity': address_alias}
                ]
            ),
            (
                sess.query(name_label, fn),
                [
                    {
                        'name': 'uname', 'type': users.c.name.type,
                        'aliased': False, 'expr': name_label, 'entity': User},
                    {
                        'name': None, 'type': fn.type, 'aliased': False,
                        'expr': fn, 'entity': User},
                ]
            ),
            (
                sess.query(cte),
                [
                {
                    'aliased': False,
                    'expr': cte.c.id, 'type': cte.c.id.type,
                    'name': 'id', 'entity': None
                }]
            ),
            (
                sess.query(users),
                [
                    {'aliased': False,
                     'expr': users.c.id, 'type': users.c.id.type,
                     'name': 'id', 'entity': None},
                    {'aliased': False,
                     'expr': users.c.name, 'type': users.c.name.type,
                     'name': 'name', 'entity': None}
                ]
            ),
            (
                sess.query(users.c.name),
                [{
                    "name": "name", "type": users.c.name.type,
                    "aliased": False, "expr": users.c.name, "entity": None
                }]
            ),
            (
                sess.query(bundle),
                [
                    {
                        'aliased': False,
                        'expr': bundle,
                        'type': Bundle,
                        'name': 'b1', 'entity': User
                    }
                ]
            )
        ]:
            eq_(
                q.column_descriptions,
                asserted
            )

    def test_unhashable_type(self):
        from sqlalchemy.types import TypeDecorator, Integer
        from sqlalchemy.sql import type_coerce

        class MyType(TypeDecorator):
            impl = Integer
            hashable = False

            def process_result_value(self, value, dialect):
                return [value]

        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        s = Session()
        q = s.query(User, type_coerce(users.c.id, MyType).label('foo')).\
            filter(User.id == 7)
        row = q.first()
        eq_(
            row, (User(id=7), [7])
        )


class BindSensitiveStringifyTest(fixtures.TestBase):
    def _fixture(self, bind_to=None):
        # building a totally separate metadata /mapping here
        # because we need to control if the MetaData is bound or not

        class User(object):
            pass

        m = MetaData(bind=bind_to)
        user_table = Table(
            'users', m,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)))

        mapper(User, user_table)
        return User

    def _dialect_fixture(self):
        class MyDialect(default.DefaultDialect):
            default_paramstyle = 'qmark'

        from sqlalchemy.engine import base
        return base.Engine(mock.Mock(), MyDialect(), mock.Mock())

    def _test(
            self, bound_metadata, bound_session,
            session_present, expect_bound):
        if bound_metadata or bound_session:
            eng = self._dialect_fixture()
        else:
            eng = None

        User = self._fixture(bind_to=eng if bound_metadata else None)

        s = Session(eng if bound_session else None)
        q = s.query(User).filter(User.id == 7)
        if not session_present:
            q = q.with_session(None)

        eq_ignore_whitespace(
            str(q),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users WHERE users.id = ?" if expect_bound else
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users WHERE users.id = :id_1"
        )

    def test_query_unbound_metadata_bound_session(self):
        self._test(False, True, True, True)

    def test_query_bound_metadata_unbound_session(self):
        self._test(True, False, True, True)

    def test_query_unbound_metadata_no_session(self):
        self._test(False, False, False, False)

    def test_query_unbound_metadata_unbound_session(self):
        self._test(False, False, True, False)

    def test_query_bound_metadata_bound_session(self):
        self._test(True, True, True, True)


class RawSelectTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_select_from_entity(self):
        User = self.classes.User

        self.assert_compile(
            select(['*']).select_from(User),
            "SELECT * FROM users"
        )

    def test_where_relationship(self):
        User = self.classes.User

        self.assert_compile(
            select([User]).where(User.addresses),
            "SELECT users.id, users.name FROM users, addresses "
            "WHERE users.id = addresses.user_id"
        )

    def test_where_m2m_relationship(self):
        Item = self.classes.Item

        self.assert_compile(
            select([Item]).where(Item.keywords),
            "SELECT items.id, items.description FROM items, "
            "item_keywords AS item_keywords_1, keywords "
            "WHERE items.id = item_keywords_1.item_id "
            "AND keywords.id = item_keywords_1.keyword_id"
        )

    def test_inline_select_from_entity(self):
        User = self.classes.User

        self.assert_compile(
            select(['*'], from_obj=User),
            "SELECT * FROM users"
        )

    def test_select_from_aliased_entity(self):
        User = self.classes.User
        ua = aliased(User, name="ua")
        self.assert_compile(
            select(['*']).select_from(ua),
            "SELECT * FROM users AS ua"
        )

    def test_correlate_entity(self):
        User = self.classes.User
        Address = self.classes.Address

        self.assert_compile(
            select(
                [
                    User.name, Address.id,
                    select([func.count(Address.id)]).
                    where(User.id == Address.user_id).
                    correlate(User).as_scalar()]),
            "SELECT users.name, addresses.id, "
            "(SELECT count(addresses.id) AS count_1 "
            "FROM addresses WHERE users.id = addresses.user_id) AS anon_1 "
            "FROM users, addresses"
        )

    def test_correlate_aliased_entity(self):
        User = self.classes.User
        Address = self.classes.Address
        uu = aliased(User, name="uu")

        self.assert_compile(
            select(
                [
                    uu.name, Address.id,
                    select([func.count(Address.id)]).
                    where(uu.id == Address.user_id).
                    correlate(uu).as_scalar()]),
            # for a long time, "uu.id = address.user_id" was reversed;
            # this was resolved as of #2872 and had to do with
            # InstrumentedAttribute.__eq__() taking precedence over
            # QueryableAttribute.__eq__()
            "SELECT uu.name, addresses.id, "
            "(SELECT count(addresses.id) AS count_1 "
            "FROM addresses WHERE uu.id = addresses.user_id) AS anon_1 "
            "FROM users AS uu, addresses"
        )

    def test_columns_clause_entity(self):
        User = self.classes.User

        self.assert_compile(
            select([User]),
            "SELECT users.id, users.name FROM users"
        )

    def test_columns_clause_columns(self):
        User = self.classes.User

        self.assert_compile(
            select([User.id, User.name]),
            "SELECT users.id, users.name FROM users"
        )

    def test_columns_clause_aliased_columns(self):
        User = self.classes.User
        ua = aliased(User, name='ua')
        self.assert_compile(
            select([ua.id, ua.name]),
            "SELECT ua.id, ua.name FROM users AS ua"
        )

    def test_columns_clause_aliased_entity(self):
        User = self.classes.User
        ua = aliased(User, name='ua')
        self.assert_compile(
            select([ua]),
            "SELECT ua.id, ua.name FROM users AS ua"
        )

    def test_core_join(self):
        User = self.classes.User
        Address = self.classes.Address
        from sqlalchemy.sql import join
        self.assert_compile(
            select([User]).select_from(join(User, Address)),
            "SELECT users.id, users.name FROM users "
            "JOIN addresses ON users.id = addresses.user_id"
        )

    def test_insert_from_query(self):
        User = self.classes.User
        Address = self.classes.Address

        s = Session()
        q = s.query(User.id, User.name).filter_by(name='ed')
        self.assert_compile(
            insert(Address).from_select(('id', 'email_address'), q),
            "INSERT INTO addresses (id, email_address) "
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users WHERE users.name = :name_1"
        )

    def test_insert_from_query_col_attr(self):
        User = self.classes.User
        Address = self.classes.Address

        s = Session()
        q = s.query(User.id, User.name).filter_by(name='ed')
        self.assert_compile(
            insert(Address).from_select(
                (Address.id, Address.email_address), q),
            "INSERT INTO addresses (id, email_address) "
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users WHERE users.name = :name_1"
        )

    def test_update_from_entity(self):
        from sqlalchemy.sql import update
        User = self.classes.User
        self.assert_compile(
            update(User),
            "UPDATE users SET id=:id, name=:name"
        )

        self.assert_compile(
            update(User).values(name='ed').where(User.id == 5),
            "UPDATE users SET name=:name WHERE users.id = :id_1",
            checkparams={"id_1": 5, "name": "ed"}
        )

    def test_delete_from_entity(self):
        from sqlalchemy.sql import delete
        User = self.classes.User
        self.assert_compile(
            delete(User),
            "DELETE FROM users"
        )

        self.assert_compile(
            delete(User).where(User.id == 5),
            "DELETE FROM users WHERE users.id = :id_1",
            checkparams={"id_1": 5}
        )

    def test_insert_from_entity(self):
        from sqlalchemy.sql import insert
        User = self.classes.User
        self.assert_compile(
            insert(User),
            "INSERT INTO users (id, name) VALUES (:id, :name)"
        )

        self.assert_compile(
            insert(User).values(name="ed"),
            "INSERT INTO users (name) VALUES (:name)",
            checkparams={"name": "ed"}
        )

    def test_col_prop_builtin_function(self):
        class Foo(object):
            pass

        mapper(
            Foo, self.tables.users, properties={
                'foob': column_property(
                    func.coalesce(self.tables.users.c.name))
            })

        self.assert_compile(
            select([Foo]).where(Foo.foob == 'somename').order_by(Foo.foob),
            "SELECT users.id, users.name FROM users "
            "WHERE coalesce(users.name) = :param_1 "
            "ORDER BY coalesce(users.name)"
        )


class GetTest(QueryTest):
    def test_get(self):
        User = self.classes.User

        s = create_session()
        assert s.query(User).get(19) is None
        u = s.query(User).get(7)
        u2 = s.query(User).get(7)
        assert u is u2
        s.expunge_all()
        u2 = s.query(User).get(7)
        assert u is not u2

    def test_get_composite_pk_no_result(self):
        CompositePk = self.classes.CompositePk

        s = Session()
        assert s.query(CompositePk).get((100, 100)) is None

    def test_get_composite_pk_result(self):
        CompositePk = self.classes.CompositePk

        s = Session()
        one_two = s.query(CompositePk).get((1, 2))
        assert one_two.i == 1
        assert one_two.j == 2
        assert one_two.k == 3

    def test_get_too_few_params(self):
        CompositePk = self.classes.CompositePk

        s = Session()
        q = s.query(CompositePk)
        assert_raises(sa_exc.InvalidRequestError, q.get, 7)

    def test_get_too_few_params_tuple(self):
        CompositePk = self.classes.CompositePk

        s = Session()
        q = s.query(CompositePk)
        assert_raises(sa_exc.InvalidRequestError, q.get, (7,))

    def test_get_too_many_params(self):
        CompositePk = self.classes.CompositePk

        s = Session()
        q = s.query(CompositePk)
        assert_raises(sa_exc.InvalidRequestError, q.get, (7, 10, 100))

    def test_get_against_col(self):
        User = self.classes.User

        s = Session()
        q = s.query(User.id)
        assert_raises(sa_exc.InvalidRequestError, q.get, (5, ))

    def test_get_null_pk(self):
        """test that a mapping which can have None in a
        PK (i.e. map to an outerjoin) works with get()."""

        users, addresses = self.tables.users, self.tables.addresses

        s = users.outerjoin(addresses)

        class UserThing(fixtures.ComparableEntity):
            pass

        mapper(
            UserThing, s, properties={
                'id': (users.c.id, addresses.c.user_id),
                'address_id': addresses.c.id,
            })
        sess = create_session()
        u10 = sess.query(UserThing).get((10, None))
        eq_(u10, UserThing(id=10))

    def test_no_criterion(self):
        """test that get()/load() does not use preexisting filter/etc.
        criterion"""

        User, Address = self.classes.User, self.classes.Address

        s = create_session()

        q = s.query(User).join('addresses').filter(Address.user_id == 8)
        assert_raises(sa_exc.InvalidRequestError, q.get, 7)
        assert_raises(
            sa_exc.InvalidRequestError,
            s.query(User).filter(User.id == 7).get, 19)

        # order_by()/get() doesn't raise
        s.query(User).order_by(User.id).get(8)

    def test_no_criterion_when_already_loaded(self):
        """test that get()/load() does not use preexisting filter/etc.
        criterion, even when we're only using the identity map."""

        User, Address = self.classes.User, self.classes.Address

        s = create_session()

        s.query(User).get(7)

        q = s.query(User).join('addresses').filter(Address.user_id == 8)
        assert_raises(sa_exc.InvalidRequestError, q.get, 7)

    def test_unique_param_names(self):
        users = self.tables.users

        class SomeUser(object):
            pass
        s = users.select(users.c.id != 12).alias('users')
        m = mapper(SomeUser, s)
        assert s.primary_key == m.primary_key

        sess = create_session()
        assert sess.query(SomeUser).get(7).name == 'jack'

    def test_load(self):
        User, Address = self.classes.User, self.classes.Address

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
        assert u2.name == 'jack'
        assert a not in u2.addresses

    @testing.provide_metadata
    @testing.requires.unicode_connections
    def test_unicode(self):
        """test that Query.get properly sets up the type for the bind
        parameter. using unicode would normally fail on postgresql, mysql and
        oracle unless it is converted to an encoded string"""

        metadata = self.metadata
        table = Table(
            'unicode_data', metadata,
            Column(
                'id', Unicode(40), primary_key=True),
            Column('data', Unicode(40)))
        metadata.create_all()
        ustring = util.b('petit voix m\xe2\x80\x99a').decode('utf-8')

        table.insert().execute(id=ustring, data=ustring)

        class LocalFoo(self.classes.Base):
            pass
        mapper(LocalFoo, table)
        eq_(
            create_session().query(LocalFoo).get(ustring),
            LocalFoo(id=ustring, data=ustring))

    def test_populate_existing(self):
        User, Address = self.classes.User, self.classes.Address

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
        # test that lazy load doesn't change child items
        s.query(User).populate_existing().all()
        assert u.addresses[0].email_address == 'lala'
        assert u.orders[1].items[2].description == 'item 12'

        # eager load does
        s.query(User). \
            options(joinedload('addresses'), joinedload_all('orders.items')). \
            populate_existing().all()
        assert u.addresses[0].email_address == 'jack@bean.com'
        assert u.orders[1].items[2].description == 'item 5'


class InvalidGenerationsTest(QueryTest, AssertsCompiledSQL):
    def test_no_limit_offset(self):
        User = self.classes.User

        s = create_session()

        for q in (
            s.query(User).limit(2),
            s.query(User).offset(2),
            s.query(User).limit(2).offset(2)
        ):
            assert_raises(sa_exc.InvalidRequestError, q.join, "addresses")

            assert_raises(
                sa_exc.InvalidRequestError, q.filter, User.name == 'ed')

            assert_raises(sa_exc.InvalidRequestError, q.filter_by, name='ed')

            assert_raises(sa_exc.InvalidRequestError, q.order_by, 'foo')

            assert_raises(sa_exc.InvalidRequestError, q.group_by, 'foo')

            assert_raises(sa_exc.InvalidRequestError, q.having, 'foo')

            q.enable_assertions(False).join("addresses")
            q.enable_assertions(False).filter(User.name == 'ed')
            q.enable_assertions(False).order_by('foo')
            q.enable_assertions(False).group_by('foo')

    def test_no_from(self):
        users, User = self.tables.users, self.classes.User

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
        User = self.classes.User

        s = create_session()
        q = s.query(User)
        assert_raises(sa_exc.ArgumentError, q.select_from, User.id == 5)
        assert_raises(sa_exc.ArgumentError, q.select_from, User.id)

    def test_invalid_from_statement(self):
        User, addresses, users = (self.classes.User,
                                  self.tables.addresses,
                                  self.tables.users)

        s = create_session()
        q = s.query(User)
        assert_raises(sa_exc.ArgumentError, q.from_statement, User.id == 5)
        assert_raises(
            sa_exc.ArgumentError, q.from_statement, users.join(addresses))

    def test_invalid_column(self):
        User = self.classes.User

        s = create_session()
        q = s.query(User)
        assert_raises(sa_exc.InvalidRequestError, q.add_column, object())

    def test_invalid_column_tuple(self):
        User = self.classes.User

        s = create_session()
        q = s.query(User)
        assert_raises(sa_exc.InvalidRequestError, q.add_column, (1, 1))

    def test_distinct(self):
        """test that a distinct() call is not valid before 'clauseelement'
        conditions."""

        User = self.classes.User

        s = create_session()
        q = s.query(User).distinct()
        assert_raises(sa_exc.InvalidRequestError, q.select_from, User)
        assert_raises(
            sa_exc.InvalidRequestError, q.from_statement,
            text("select * from table"))
        assert_raises(sa_exc.InvalidRequestError, q.with_polymorphic, User)

    def test_order_by(self):
        """test that an order_by() call is not valid before 'clauseelement'
        conditions."""

        User = self.classes.User

        s = create_session()
        q = s.query(User).order_by(User.id)
        assert_raises(sa_exc.InvalidRequestError, q.select_from, User)
        assert_raises(
            sa_exc.InvalidRequestError, q.from_statement,
            text("select * from table"))
        assert_raises(sa_exc.InvalidRequestError, q.with_polymorphic, User)

    def test_only_full_mapper_zero(self):
        User, Address = self.classes.User, self.classes.Address

        s = create_session()

        q = s.query(User, Address)
        assert_raises(sa_exc.InvalidRequestError, q.get, 5)

    def test_entity_or_mapper_zero(self):
        User, Address = self.classes.User, self.classes.Address
        s = create_session()

        q = s.query(User, Address)
        is_(q._mapper_zero(), inspect(User))
        is_(q._entity_zero(), inspect(User))

        u1 = aliased(User)
        q = s.query(u1, Address)
        is_(q._mapper_zero(), inspect(User))
        is_(q._entity_zero(), inspect(u1))

        q = s.query(User).select_from(Address)
        is_(q._mapper_zero(), inspect(User))
        is_(q._entity_zero(), inspect(Address))

        q = s.query(User.name, Address)
        is_(q._mapper_zero(), inspect(User))
        is_(q._entity_zero(), inspect(User))

        q = s.query(u1.name, Address)
        is_(q._mapper_zero(), inspect(User))
        is_(q._entity_zero(), inspect(u1))

        q1 = s.query(User).exists()
        q = s.query(q1)
        is_(q._mapper_zero(), None)
        is_(q._entity_zero(), None)

        q1 = s.query(Bundle('b1', User.id, User.name))
        is_(q1._mapper_zero(), inspect(User))
        is_(q1._entity_zero(), inspect(User))

    def test_from_statement(self):
        User = self.classes.User

        s = create_session()

        for meth, arg, kw in [
            (Query.filter, (User.id == 5,), {}),
            (Query.filter_by, (), {'id': 5}),
            (Query.limit, (5, ), {}),
            (Query.group_by, (User.name,), {}),
            (Query.order_by, (User.name,), {})
        ]:
            q = s.query(User)
            q = meth(q, *arg, **kw)
            assert_raises(
                sa_exc.InvalidRequestError,
                q.from_statement, text("x")
            )

            q = s.query(User)
            q = q.from_statement(text("x"))
            assert_raises(
                sa_exc.InvalidRequestError,
                meth, q, *arg, **kw
            )

    def test_illegal_coercions(self):
        User = self.classes.User

        assert_raises_message(
            sa_exc.ArgumentError,
            "Object .*User.* is not legal as a SQL literal value",
            distinct, User
        )

        ua = aliased(User)
        assert_raises_message(
            sa_exc.ArgumentError,
            "Object .*User.* is not legal as a SQL literal value",
            distinct, ua
        )

        s = Session()
        assert_raises_message(
            sa_exc.ArgumentError,
            "Object .*User.* is not legal as a SQL literal value",
            lambda: s.query(User).filter(User.name == User)
        )

        u1 = User()
        assert_raises_message(
            sa_exc.ArgumentError,
            "Object .*User.* is not legal as a SQL literal value",
            distinct, u1
        )

        assert_raises_message(
            sa_exc.ArgumentError,
            "Object .*User.* is not legal as a SQL literal value",
            lambda: s.query(User).filter(User.name == u1)
        )


class OperatorTest(QueryTest, AssertsCompiledSQL):
    """test sql.Comparator implementation for MapperProperties"""

    __dialect__ = 'default'

    def _test(self, clause, expected, entity=None, checkparams=None):
        dialect = default.DefaultDialect()
        if entity is not None:
            # specify a lead entity, so that when we are testing
            # correlation, the correlation actually happens
            sess = Session()
            lead = sess.query(entity)
            context = lead._compile_context()
            context.statement.use_labels = True
            lead = context.statement.compile(dialect=dialect)
            expected = (str(lead) + " WHERE " + expected).replace("\n", "")
            clause = sess.query(entity).filter(clause)
        self.assert_compile(clause, expected, checkparams=checkparams)

    def _test_filter_aliases(
            self,
            clause, expected, from_, onclause, checkparams=None):
        dialect = default.DefaultDialect()
        sess = Session()
        lead = sess.query(from_).join(onclause, aliased=True)
        full = lead.filter(clause)
        context = lead._compile_context()
        context.statement.use_labels = True
        lead = context.statement.compile(dialect=dialect)
        expected = (str(lead) + " WHERE " + expected).replace("\n", "")

        self.assert_compile(full, expected, checkparams=checkparams)

    def test_arithmetic(self):
        User = self.classes.User

        create_session().query(User)
        for (py_op, sql_op) in ((operators.add, '+'), (operators.mul, '*'),
                                (operators.sub, '-'),
                                (operators.truediv, '/'),
                                (operators.div, '/'),
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
        User = self.classes.User

        create_session().query(User)
        ualias = aliased(User)

        for (py_op, fwd_op, rev_op) in ((operators.lt, '<', '>'),
                                        (operators.gt, '>', '<'),
                                        (operators.eq, '=', '='),
                                        (operators.ne, '!=', '!='),
                                        (operators.le, '<=', '>='),
                                        (operators.ge, '>=', '<=')):
            for (lhs, rhs, l_sql, r_sql) in (
                    ('a', User.id, ':id_1', 'users.id'),
                    ('a', literal('b'), ':param_2', ':param_1'),  # note swap!
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
                compiled = str(py_op(lhs, rhs).compile(
                    dialect=default.DefaultDialect()))
                fwd_sql = "%s %s %s" % (l_sql, fwd_op, r_sql)
                rev_sql = "%s %s %s" % (r_sql, rev_op, l_sql)

                self.assert_(compiled == fwd_sql or compiled == rev_sql,
                             "\n'" + compiled + "'\n does not match\n'" +
                             fwd_sql + "'\n or\n'" + rev_sql + "'")

    def test_o2m_compare_to_null(self):
        User = self.classes.User

        self._test(User.id == None, "users.id IS NULL")  # noqa
        self._test(User.id != None, "users.id IS NOT NULL")  # noqa
        self._test(~(User.id == None), "users.id IS NOT NULL")  # noqa
        self._test(~(User.id != None), "users.id IS NULL")  # noqa
        self._test(None == User.id, "users.id IS NULL")  # noqa
        self._test(~(None == User.id), "users.id IS NOT NULL")  # noqa

    def test_m2o_compare_to_null(self):
        Address = self.classes.Address
        self._test(Address.user == None, "addresses.user_id IS NULL")  # noqa
        self._test(~(Address.user == None),  # noqa
                   "addresses.user_id IS NOT NULL")
        self._test(~(Address.user != None),  # noqa
                   "addresses.user_id IS NULL")
        self._test(None == Address.user, "addresses.user_id IS NULL")  # noqa
        self._test(~(None == Address.user),  # noqa
                   "addresses.user_id IS NOT NULL")

    def test_o2m_compare_to_null_orm_adapt(self):
        User, Address = self.classes.User, self.classes.Address
        self._test_filter_aliases(
            User.id == None,  # noqa
            "users_1.id IS NULL", Address, Address.user),
        self._test_filter_aliases(
            User.id != None,  # noqa
            "users_1.id IS NOT NULL", Address, Address.user),
        self._test_filter_aliases(
            ~(User.id == None),  # noqa
            "users_1.id IS NOT NULL", Address, Address.user),
        self._test_filter_aliases(
            ~(User.id != None),  # noqa
            "users_1.id IS NULL", Address, Address.user),

    def test_m2o_compare_to_null_orm_adapt(self):
        User, Address = self.classes.User, self.classes.Address
        self._test_filter_aliases(
            Address.user == None,  # noqa
            "addresses_1.user_id IS NULL", User, User.addresses),
        self._test_filter_aliases(
            Address.user != None,  # noqa
            "addresses_1.user_id IS NOT NULL", User, User.addresses),
        self._test_filter_aliases(
            ~(Address.user == None),  # noqa
            "addresses_1.user_id IS NOT NULL", User, User.addresses),
        self._test_filter_aliases(
            ~(Address.user != None),  # noqa
            "addresses_1.user_id IS NULL", User, User.addresses),

    def test_o2m_compare_to_null_aliased(self):
        User = self.classes.User
        u1 = aliased(User)
        self._test(u1.id == None, "users_1.id IS NULL")  # noqa
        self._test(u1.id != None, "users_1.id IS NOT NULL")  # noqa
        self._test(~(u1.id == None), "users_1.id IS NOT NULL")  # noqa
        self._test(~(u1.id != None), "users_1.id IS NULL")  # noqa

    def test_m2o_compare_to_null_aliased(self):
        Address = self.classes.Address
        a1 = aliased(Address)
        self._test(a1.user == None, "addresses_1.user_id IS NULL")  # noqa
        self._test(~(a1.user == None),  # noqa
                   "addresses_1.user_id IS NOT NULL")
        self._test(a1.user != None, "addresses_1.user_id IS NOT NULL")  # noqa
        self._test(~(a1.user != None), "addresses_1.user_id IS NULL")  # noqa

    def test_relationship_unimplemented(self):
        User = self.classes.User
        for op in [
            User.addresses.like,
            User.addresses.ilike,
            User.addresses.__le__,
            User.addresses.__gt__,
        ]:
            assert_raises(NotImplementedError, op, "x")

    def test_o2m_any(self):
        User, Address = self.classes.User, self.classes.Address
        self._test(
            User.addresses.any(Address.id == 17),
            "EXISTS (SELECT 1 FROM addresses "
            "WHERE users.id = addresses.user_id AND addresses.id = :id_1)",
            entity=User
        )

    def test_o2m_any_aliased(self):
        User, Address = self.classes.User, self.classes.Address
        u1 = aliased(User)
        a1 = aliased(Address)
        self._test(
            u1.addresses.of_type(a1).any(a1.id == 17),
            "EXISTS (SELECT 1 FROM addresses AS addresses_1 "
            "WHERE users_1.id = addresses_1.user_id AND "
            "addresses_1.id = :id_1)",
            entity=u1
        )

    def test_o2m_any_orm_adapt(self):
        User, Address = self.classes.User, self.classes.Address
        self._test_filter_aliases(
            User.addresses.any(Address.id == 17),
            "EXISTS (SELECT 1 FROM addresses "
            "WHERE users_1.id = addresses.user_id AND addresses.id = :id_1)",
            Address, Address.user
        )

    def test_m2o_compare_instance(self):
        User, Address = self.classes.User, self.classes.Address
        u7 = User(id=5)
        attributes.instance_state(u7)._commit_all(attributes.instance_dict(u7))
        u7.id = 7

        self._test(Address.user == u7, ":param_1 = addresses.user_id")

    def test_m2o_compare_instance_negated(self):
        User, Address = self.classes.User, self.classes.Address
        u7 = User(id=5)
        attributes.instance_state(u7)._commit_all(attributes.instance_dict(u7))
        u7.id = 7

        self._test(
            Address.user != u7,
            "addresses.user_id != :user_id_1 OR addresses.user_id IS NULL",
            checkparams={'user_id_1': 7})

    def test_m2o_compare_instance_orm_adapt(self):
        User, Address = self.classes.User, self.classes.Address
        u7 = User(id=5)
        attributes.instance_state(u7)._commit_all(attributes.instance_dict(u7))
        u7.id = 7

        self._test_filter_aliases(
            Address.user == u7,
            ":param_1 = addresses_1.user_id", User, User.addresses,
            checkparams={'param_1': 7}
        )

    def test_m2o_compare_instance_negated_warn_on_none(self):
        User, Address = self.classes.User, self.classes.Address

        u7_transient = User(id=None)

        with expect_warnings("Got None for value of column users.id; "):
            self._test_filter_aliases(
                Address.user != u7_transient,
                "addresses_1.user_id != :user_id_1 "
                "OR addresses_1.user_id IS NULL",
                User, User.addresses,
                checkparams={'user_id_1': None}
            )

    def test_m2o_compare_instance_negated_orm_adapt(self):
        User, Address = self.classes.User, self.classes.Address
        u7 = User(id=5)
        attributes.instance_state(u7)._commit_all(attributes.instance_dict(u7))
        u7.id = 7

        u7_transient = User(id=7)

        self._test_filter_aliases(
            Address.user != u7,
            "addresses_1.user_id != :user_id_1 OR addresses_1.user_id IS NULL",
            User, User.addresses,
            checkparams={'user_id_1': 7}
        )

        self._test_filter_aliases(
            ~(Address.user == u7), ":param_1 != addresses_1.user_id",
            User, User.addresses,
            checkparams={'param_1': 7}
        )

        self._test_filter_aliases(
            ~(Address.user != u7),
            "NOT (addresses_1.user_id != :user_id_1 "
            "OR addresses_1.user_id IS NULL)", User, User.addresses,
            checkparams={'user_id_1': 7}
        )

        self._test_filter_aliases(
            Address.user != u7_transient,
            "addresses_1.user_id != :user_id_1 OR addresses_1.user_id IS NULL",
            User, User.addresses,
            checkparams={'user_id_1': 7}
        )

        self._test_filter_aliases(
            ~(Address.user == u7_transient), ":param_1 != addresses_1.user_id",
            User, User.addresses,
            checkparams={'param_1': 7}
        )

        self._test_filter_aliases(
            ~(Address.user != u7_transient),
            "NOT (addresses_1.user_id != :user_id_1 "
            "OR addresses_1.user_id IS NULL)", User, User.addresses,
            checkparams={'user_id_1': 7}
        )

    def test_m2o_compare_instance_aliased(self):
        User, Address = self.classes.User, self.classes.Address
        u7 = User(id=5)
        attributes.instance_state(u7)._commit_all(attributes.instance_dict(u7))
        u7.id = 7

        u7_transient = User(id=7)

        a1 = aliased(Address)
        self._test(
            a1.user == u7,
            ":param_1 = addresses_1.user_id",
            checkparams={'param_1': 7})

        self._test(
            a1.user != u7,
            "addresses_1.user_id != :user_id_1 OR addresses_1.user_id IS NULL",
            checkparams={'user_id_1': 7})

        a1 = aliased(Address)
        self._test(
            a1.user == u7_transient,
            ":param_1 = addresses_1.user_id",
            checkparams={'param_1': 7})

        self._test(
            a1.user != u7_transient,
            "addresses_1.user_id != :user_id_1 OR addresses_1.user_id IS NULL",
            checkparams={'user_id_1': 7})

    def test_selfref_relationship(self):

        Node = self.classes.Node

        nalias = aliased(Node)

        # auto self-referential aliasing
        self._test(
            Node.children.any(Node.data == 'n1'),
            "EXISTS (SELECT 1 FROM nodes AS nodes_1 WHERE "
            "nodes.id = nodes_1.parent_id AND nodes_1.data = :data_1)",
            entity=Node,
            checkparams={'data_1': 'n1'}
        )

        # needs autoaliasing
        self._test(
            Node.children == None,  # noqa
            "NOT (EXISTS (SELECT 1 FROM nodes AS nodes_1 "
            "WHERE nodes.id = nodes_1.parent_id))",
            entity=Node,
            checkparams={}
        )

        self._test(
            Node.parent == None,  # noqa
            "nodes.parent_id IS NULL",
            checkparams={}
        )

        self._test(
            nalias.parent == None,  # noqa
            "nodes_1.parent_id IS NULL",
            checkparams={}
        )

        self._test(
            nalias.parent != None,  # noqa
            "nodes_1.parent_id IS NOT NULL",
            checkparams={}
        )

        self._test(
            nalias.children == None,  # noqa
            "NOT (EXISTS ("
            "SELECT 1 FROM nodes WHERE nodes_1.id = nodes.parent_id))",
            entity=nalias,
            checkparams={}
        )

        self._test(
            nalias.children.any(Node.data == 'some data'),
            "EXISTS (SELECT 1 FROM nodes WHERE "
            "nodes_1.id = nodes.parent_id AND nodes.data = :data_1)",
            entity=nalias,
            checkparams={'data_1': 'some data'}
        )

        # this fails because self-referential any() is auto-aliasing;
        # the fact that we use "nalias" here means we get two aliases.
        # self._test(
        #        Node.children.any(nalias.data == 'some data'),
        #        "EXISTS (SELECT 1 FROM nodes AS nodes_1 WHERE "
        #        "nodes.id = nodes_1.parent_id AND nodes_1.data = :data_1)",
        #        entity=Node
        #        )

        self._test(
            nalias.parent.has(Node.data == 'some data'),
            "EXISTS (SELECT 1 FROM nodes WHERE nodes.id = nodes_1.parent_id "
            "AND nodes.data = :data_1)",
            entity=nalias,
            checkparams={'data_1': 'some data'}
        )

        self._test(
            Node.parent.has(Node.data == 'some data'),
            "EXISTS (SELECT 1 FROM nodes AS nodes_1 WHERE "
            "nodes_1.id = nodes.parent_id AND nodes_1.data = :data_1)",
            entity=Node,
            checkparams={'data_1': 'some data'}
        )

        self._test(
            Node.parent == Node(id=7),
            ":param_1 = nodes.parent_id",
            checkparams={"param_1": 7}
        )

        self._test(
            nalias.parent == Node(id=7),
            ":param_1 = nodes_1.parent_id",
            checkparams={"param_1": 7}
        )

        self._test(
            nalias.parent != Node(id=7),
            'nodes_1.parent_id != :parent_id_1 '
            'OR nodes_1.parent_id IS NULL',
            checkparams={"parent_id_1": 7}
        )

        self._test(
            nalias.parent != Node(id=7),
            'nodes_1.parent_id != :parent_id_1 '
            'OR nodes_1.parent_id IS NULL',
            checkparams={"parent_id_1": 7}
        )

        self._test(
            nalias.children.contains(Node(id=7, parent_id=12)),
            "nodes_1.id = :param_1",
            checkparams={"param_1": 12}
        )

    def test_multilevel_any(self):
        User, Address, Dingaling = \
            self.classes.User, self.classes.Address, self.classes.Dingaling
        sess = Session()

        q = sess.query(User).filter(
            User.addresses.any(
                and_(Address.id == Dingaling.address_id,
                     Dingaling.data == 'x')))
        # new since #2746 - correlate_except() now takes context into account
        # so its usage in any() is not as disrupting.
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users "
            "WHERE EXISTS (SELECT 1 "
            "FROM addresses, dingalings "
            "WHERE users.id = addresses.user_id AND "
            "addresses.id = dingalings.address_id AND "
            "dingalings.data = :data_1)"
        )

    def test_op(self):
        User = self.classes.User

        self._test(User.name.op('ilike')('17'), "users.name ilike :name_1")

    def test_in(self):
        User = self.classes.User

        self._test(User.id.in_(['a', 'b']), "users.id IN (:id_1, :id_2)")

    def test_in_on_relationship_not_supported(self):
        User, Address = self.classes.User, self.classes.Address

        assert_raises(NotImplementedError, Address.user.in_, [User(id=5)])

    def test_neg(self):
        User = self.classes.User

        self._test(-User.id, "-users.id")
        self._test(User.id + -User.id, "users.id + -users.id")

    def test_between(self):
        User = self.classes.User

        self._test(
            User.id.between('a', 'b'), "users.id BETWEEN :id_1 AND :id_2")

    def test_collate(self):
        User = self.classes.User

        self._test(collate(User.id, 'utf8_bin'), "users.id COLLATE utf8_bin")

        self._test(User.id.collate('utf8_bin'), "users.id COLLATE utf8_bin")

    def test_selfref_between(self):
        User = self.classes.User

        ualias = aliased(User)
        self._test(
            User.id.between(ualias.id, ualias.id),
            "users.id BETWEEN users_1.id AND users_1.id")
        self._test(
            ualias.id.between(User.id, User.id),
            "users_1.id BETWEEN users.id AND users.id")

    def test_clauses(self):
        User, Address = self.classes.User, self.classes.Address

        for (expr, compare) in (
            (func.max(User.id), "max(users.id)"),
            (User.id.desc(), "users.id DESC"),
            (between(5, User.id, Address.id),
             ":param_1 BETWEEN users.id AND addresses.id"),
            # this one would require adding compile() to
            # InstrumentedScalarAttribute.  do we want this ?
            # (User.id, "users.id")
        ):
            c = expr.compile(dialect=default.DefaultDialect())
            assert str(c) == compare, "%s != %s" % (str(c), compare)


class ExpressionTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_deferred_instances(self):
        User, addresses, Address = (self.classes.User,
                                    self.tables.addresses,
                                    self.classes.Address)

        session = create_session()
        s = session.query(User).filter(
            and_(addresses.c.email_address == bindparam('emailad'),
                 Address.user_id == User.id)).statement

        result = list(
            session.query(User).instances(s.execute(emailad='jack@bean.com')))
        eq_([User(id=7)], result)

    def test_aliased_sql_construct(self):
        User, Address = self.classes.User, self.classes.Address

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
            "ON users.id = addresses.user_id) AS anon_1"
        )

    def test_aliased_sql_construct_raises_adapt_on_names(self):
        User, Address = self.classes.User, self.classes.Address

        j = join(User, Address)
        assert_raises_message(
            sa_exc.ArgumentError,
            "adapt_on_names only applies to ORM elements",
            aliased, j, adapt_on_names=True
        )

    def test_scalar_subquery_compile_whereclause(self):
        User = self.classes.User
        Address = self.classes.Address

        session = create_session()

        q = session.query(User.id).filter(User.id == 7)

        q = session.query(Address).filter(Address.user_id == q)
        assert isinstance(q._criterion.right, expression.ColumnElement)
        self.assert_compile(
            q,
            "SELECT addresses.id AS addresses_id, addresses.user_id "
            "AS addresses_user_id, addresses.email_address AS "
            "addresses_email_address FROM addresses WHERE "
            "addresses.user_id = (SELECT users.id AS users_id "
            "FROM users WHERE users.id = :id_1)"
        )

    def test_subquery_no_eagerloads(self):
        User = self.classes.User
        s = Session()

        self.assert_compile(
            s.query(User).options(joinedload(User.addresses)).subquery(),
            "SELECT users.id, users.name FROM users"
        )

    def test_exists_no_eagerloads(self):
        User = self.classes.User
        s = Session()

        self.assert_compile(
            s.query(
                s.query(User).options(joinedload(User.addresses)).exists()
            ),
            "SELECT EXISTS (SELECT 1 FROM users) AS anon_1"
        )

    def test_named_subquery(self):
        User = self.classes.User

        session = create_session()
        a1 = session.query(User.id).filter(User.id == 7).subquery('foo1')
        a2 = session.query(User.id).filter(User.id == 7).subquery(name='foo2')
        a3 = session.query(User.id).filter(User.id == 7).subquery()

        eq_(a1.name, 'foo1')
        eq_(a2.name, 'foo2')
        eq_(a3.name, '%%(%d anon)s' % id(a3))

    def test_labeled_subquery(self):
        User = self.classes.User

        session = create_session()
        a1 = session.query(User.id).filter(User.id == 7). \
            subquery(with_labels=True)
        assert a1.c.users_id is not None

    def test_reduced_subquery(self):
        User = self.classes.User
        ua = aliased(User)

        session = create_session()
        a1 = session.query(User.id, ua.id, ua.name).\
            filter(User.id == ua.id).subquery(reduce_columns=True)
        self.assert_compile(a1,
                            "SELECT users.id, users_1.name FROM "
                            "users, users AS users_1 "
                            "WHERE users.id = users_1.id")

    def test_label(self):
        User = self.classes.User

        session = create_session()

        q = session.query(User.id).filter(User.id == 7).label('foo')
        self.assert_compile(
            session.query(q),
            "SELECT (SELECT users.id FROM users WHERE users.id = :id_1) AS foo"
        )

    def test_as_scalar(self):
        User = self.classes.User

        session = create_session()

        q = session.query(User.id).filter(User.id == 7).as_scalar()

        self.assert_compile(session.query(User).filter(User.id.in_(q)),
                            'SELECT users.id AS users_id, users.name '
                            'AS users_name FROM users WHERE users.id '
                            'IN (SELECT users.id FROM users WHERE '
                            'users.id = :id_1)')

    def test_param_transfer(self):
        User = self.classes.User

        session = create_session()

        q = session.query(User.id).filter(User.id == bindparam('foo')).\
            params(foo=7).subquery()

        q = session.query(User).filter(User.id.in_(q))

        eq_(User(id=7), q.one())

    def test_in(self):
        User, Address = self.classes.User, self.classes.Address

        session = create_session()
        s = session.query(User.id).join(User.addresses).group_by(User.id).\
            having(func.count(Address.id) > 2)
        eq_(session.query(User).filter(User.id.in_(s)).all(), [User(id=8)])

    def test_union(self):
        User = self.classes.User

        s = create_session()

        q1 = s.query(User).filter(User.name == 'ed').with_labels()
        q2 = s.query(User).filter(User.name == 'fred').with_labels()
        eq_(
            s.query(User).from_statement(union(q1, q2).
                                         order_by('users_name')).all(),
            [User(name='ed'), User(name='fred')]
        )

    def test_select(self):
        User = self.classes.User

        s = create_session()

        # this is actually not legal on most DBs since the subquery has no
        # alias
        q1 = s.query(User).filter(User.name == 'ed')

        self.assert_compile(
            select([q1]),
            "SELECT users_id, users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users WHERE users.name = :name_1)"
        )

    def test_join(self):
        User, Address = self.classes.User, self.classes.Address

        s = create_session()

        # TODO: do we want aliased() to detect a query and convert to
        # subquery() automatically ?
        q1 = s.query(Address).filter(Address.email_address == 'jack@bean.com')
        adalias = aliased(Address, q1.subquery())
        eq_(
            s.query(User, adalias).join(adalias, User.id == adalias.user_id).
            all(),
            [
                (
                    User(id=7, name='jack'),
                    Address(email_address='jack@bean.com', user_id=7, id=1))])

    def test_group_by_plain(self):
        User = self.classes.User
        s = create_session()

        q1 = s.query(User.id, User.name).group_by(User.name)
        self.assert_compile(
            select([q1]),
            "SELECT users_id, users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users GROUP BY users.name)"
        )

    def test_group_by_append(self):
        User = self.classes.User
        s = create_session()

        q1 = s.query(User.id, User.name).group_by(User.name)

        # test append something to group_by
        self.assert_compile(
            select([q1.group_by(User.id)]),
            "SELECT users_id, users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users "
            "GROUP BY users.name, users.id)"
        )

    def test_group_by_cancellation(self):
        User = self.classes.User
        s = create_session()

        q1 = s.query(User.id, User.name).group_by(User.name)
        # test cancellation by using None, replacement with something else
        self.assert_compile(
            select([q1.group_by(None).group_by(User.id)]),
            "SELECT users_id, users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users GROUP BY users.id)"
        )

        # test cancellation by using None, replacement with nothing
        self.assert_compile(
            select([q1.group_by(None)]),
            "SELECT users_id, users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users)"
        )

    def test_group_by_cancelled_still_present(self):
        User = self.classes.User
        s = create_session()

        q1 = s.query(User.id, User.name).group_by(User.name).group_by(None)

        q1._no_criterion_assertion("foo")

    def test_order_by_plain(self):
        User = self.classes.User
        s = create_session()

        q1 = s.query(User.id, User.name).order_by(User.name)
        self.assert_compile(
            select([q1]),
            "SELECT users_id, users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users ORDER BY users.name)"
        )

    def test_order_by_append(self):
        User = self.classes.User
        s = create_session()

        q1 = s.query(User.id, User.name).order_by(User.name)

        # test append something to order_by
        self.assert_compile(
            select([q1.order_by(User.id)]),
            "SELECT users_id, users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users "
            "ORDER BY users.name, users.id)"
        )

    def test_order_by_cancellation(self):
        User = self.classes.User
        s = create_session()

        q1 = s.query(User.id, User.name).order_by(User.name)
        # test cancellation by using None, replacement with something else
        self.assert_compile(
            select([q1.order_by(None).order_by(User.id)]),
            "SELECT users_id, users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users ORDER BY users.id)"
        )

        # test cancellation by using None, replacement with nothing
        self.assert_compile(
            select([q1.order_by(None)]),
            "SELECT users_id, users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users)"
        )

    def test_order_by_cancellation_false(self):
        User = self.classes.User
        s = create_session()

        q1 = s.query(User.id, User.name).order_by(User.name)
        # test cancellation by using None, replacement with something else
        self.assert_compile(
            select([q1.order_by(False).order_by(User.id)]),
            "SELECT users_id, users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users ORDER BY users.id)"
        )

        # test cancellation by using None, replacement with nothing
        self.assert_compile(
            select([q1.order_by(False)]),
            "SELECT users_id, users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users)"
        )

    def test_order_by_cancelled_allows_assertions(self):
        User = self.classes.User
        s = create_session()

        q1 = s.query(User.id, User.name).order_by(User.name).order_by(None)

        q1._no_criterion_assertion("foo")

    def test_legacy_order_by_cancelled_allows_assertions(self):
        User = self.classes.User
        s = create_session()

        q1 = s.query(User.id, User.name).order_by(User.name).order_by(False)

        q1._no_criterion_assertion("foo")


class ColumnPropertyTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = 'default'
    run_setup_mappers = 'each'

    def _fixture(self, label=True, polymorphic=False):
        User, Address = self.classes("User", "Address")
        users, addresses = self.tables("users", "addresses")
        stmt = select([func.max(addresses.c.email_address)]).\
            where(addresses.c.user_id == users.c.id).\
            correlate(users)
        if label:
            stmt = stmt.label("email_ad")

        mapper(User, users, properties={
            "ead": column_property(stmt)
        }, with_polymorphic="*" if polymorphic else None)
        mapper(Address, addresses)

    def _func_fixture(self, label=False):
        User = self.classes.User
        users = self.tables.users

        if label:
            mapper(User, users, properties={
                "foobar": column_property(
                    func.foob(users.c.name).label(None)
                )
            })
        else:
            mapper(User, users, properties={
                "foobar": column_property(
                    func.foob(users.c.name)
                )
            })

    def test_anon_label_function_auto(self):
        self._func_fixture()
        User = self.classes.User

        s = Session()

        u1 = aliased(User)
        self.assert_compile(
            s.query(User.foobar, u1.foobar),
            "SELECT foob(users.name) AS foob_1, foob(users_1.name) AS foob_2 "
            "FROM users, users AS users_1"
        )

    def test_anon_label_function_manual(self):
        self._func_fixture(label=True)
        User = self.classes.User

        s = Session()

        u1 = aliased(User)
        self.assert_compile(
            s.query(User.foobar, u1.foobar),
            "SELECT foob(users.name) AS foob_1, foob(users_1.name) AS foob_2 "
            "FROM users, users AS users_1"
        )

    def test_anon_label_ad_hoc_labeling(self):
        self._func_fixture()
        User = self.classes.User

        s = Session()

        u1 = aliased(User)
        self.assert_compile(
            s.query(User.foobar.label('x'), u1.foobar.label('y')),
            "SELECT foob(users.name) AS x, foob(users_1.name) AS y "
            "FROM users, users AS users_1"
        )

    def test_order_by_column_prop_string(self):
        User, Address = self.classes("User", "Address")
        self._fixture(label=True)

        s = Session()
        q = s.query(User).order_by("email_ad")
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses "
            "WHERE addresses.user_id = users.id) AS email_ad, "
            "users.id AS users_id, users.name AS users_name "
            "FROM users ORDER BY email_ad"
        )

    def test_order_by_column_prop_aliased_string(self):
        User, Address = self.classes("User", "Address")
        self._fixture(label=True)

        s = Session()
        ua = aliased(User)
        q = s.query(ua).order_by("email_ad")

        def go():
            self.assert_compile(
                q,
                "SELECT (SELECT max(addresses.email_address) AS max_1 "
                "FROM addresses WHERE addresses.user_id = users_1.id) "
                "AS anon_1, users_1.id AS users_1_id, "
                "users_1.name AS users_1_name FROM users AS users_1 "
                "ORDER BY email_ad"
            )
        assert_warnings(
            go,
            ["Can't resolve label reference 'email_ad'"], regex=True)

    def test_order_by_column_labeled_prop_attr_aliased_one(self):
        User = self.classes.User
        self._fixture(label=True)

        ua = aliased(User)
        s = Session()
        q = s.query(ua).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses WHERE addresses.user_id = users_1.id) AS anon_1, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 ORDER BY anon_1"
        )

    def test_order_by_column_labeled_prop_attr_aliased_two(self):
        User = self.classes.User
        self._fixture(label=True)

        ua = aliased(User)
        s = Session()
        q = s.query(ua.ead).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses, "
            "users AS users_1 WHERE addresses.user_id = users_1.id) "
            "AS anon_1 ORDER BY anon_1"
        )

        # we're also testing that the state of "ua" is OK after the
        # previous call, so the batching into one test is intentional
        q = s.query(ua).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses WHERE addresses.user_id = users_1.id) AS anon_1, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 ORDER BY anon_1"
        )

    def test_order_by_column_labeled_prop_attr_aliased_three(self):
        User = self.classes.User
        self._fixture(label=True)

        ua = aliased(User)
        s = Session()
        q = s.query(User.ead, ua.ead).order_by(User.ead, ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses, users WHERE addresses.user_id = users.id) "
            "AS email_ad, (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses, users AS users_1 WHERE addresses.user_id = "
            "users_1.id) AS anon_1 ORDER BY email_ad, anon_1"
        )

        q = s.query(User, ua).order_by(User.ead, ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses WHERE addresses.user_id = users.id) AS "
            "email_ad, users.id AS users_id, users.name AS users_name, "
            "(SELECT max(addresses.email_address) AS max_1 FROM addresses "
            "WHERE addresses.user_id = users_1.id) AS anon_1, users_1.id "
            "AS users_1_id, users_1.name AS users_1_name FROM users, "
            "users AS users_1 ORDER BY email_ad, anon_1"
        )

    def test_order_by_column_labeled_prop_attr_aliased_four(self):
        User = self.classes.User
        self._fixture(label=True, polymorphic=True)

        ua = aliased(User)
        s = Session()
        q = s.query(ua, User.id).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 FROM "
            "addresses WHERE addresses.user_id = users_1.id) AS anon_1, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name, "
            "users.id AS users_id FROM users AS users_1, users ORDER BY anon_1"
        )

    def test_order_by_column_unlabeled_prop_attr_aliased_one(self):
        User = self.classes.User
        self._fixture(label=False)

        ua = aliased(User)
        s = Session()
        q = s.query(ua).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses WHERE addresses.user_id = users_1.id) AS anon_1, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 ORDER BY anon_1"
        )

    def test_order_by_column_unlabeled_prop_attr_aliased_two(self):
        User = self.classes.User
        self._fixture(label=False)

        ua = aliased(User)
        s = Session()
        q = s.query(ua.ead).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses, "
            "users AS users_1 WHERE addresses.user_id = users_1.id) "
            "AS anon_1 ORDER BY anon_1"
        )

        # we're also testing that the state of "ua" is OK after the
        # previous call, so the batching into one test is intentional
        q = s.query(ua).order_by(ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses WHERE addresses.user_id = users_1.id) AS anon_1, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 ORDER BY anon_1"
        )

    def test_order_by_column_unlabeled_prop_attr_aliased_three(self):
        User = self.classes.User
        self._fixture(label=False)

        ua = aliased(User)
        s = Session()
        q = s.query(User.ead, ua.ead).order_by(User.ead, ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses, users WHERE addresses.user_id = users.id) "
            "AS anon_1, (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses, users AS users_1 "
            "WHERE addresses.user_id = users_1.id) AS anon_2 "
            "ORDER BY anon_1, anon_2"
        )

        q = s.query(User, ua).order_by(User.ead, ua.ead)
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses WHERE addresses.user_id = users.id) AS "
            "anon_1, users.id AS users_id, users.name AS users_name, "
            "(SELECT max(addresses.email_address) AS max_1 FROM addresses "
            "WHERE addresses.user_id = users_1.id) AS anon_2, users_1.id "
            "AS users_1_id, users_1.name AS users_1_name FROM users, "
            "users AS users_1 ORDER BY anon_1, anon_2"
        )

    def test_order_by_column_prop_attr(self):
        User, Address = self.classes("User", "Address")
        self._fixture(label=True)

        s = Session()
        q = s.query(User).order_by(User.ead)
        # this one is a bit of a surprise; this is compiler
        # label-order-by logic kicking in, but won't work in more
        # complex cases.
        self.assert_compile(
            q,
            "SELECT (SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses "
            "WHERE addresses.user_id = users.id) AS email_ad, "
            "users.id AS users_id, users.name AS users_name "
            "FROM users ORDER BY email_ad"
        )

    def test_order_by_column_prop_attr_non_present(self):
        User, Address = self.classes("User", "Address")
        self._fixture(label=True)

        s = Session()
        q = s.query(User).options(defer(User.ead)).order_by(User.ead)
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users ORDER BY "
            "(SELECT max(addresses.email_address) AS max_1 "
            "FROM addresses "
            "WHERE addresses.user_id = users.id)"
        )


class ComparatorTest(QueryTest):
    def test_clause_element_query_resolve(self):
        from sqlalchemy.orm.properties import ColumnProperty
        User = self.classes.User

        class Comparator(ColumnProperty.Comparator):
            def __init__(self, expr):
                self.expr = expr

            def __clause_element__(self):
                return self.expr

        sess = Session()
        eq_(
            sess.query(Comparator(User.id)).order_by(
                Comparator(User.id)).all(),
            [(7, ), (8, ), (9, ), (10, )]
        )


# more slice tests are available in test/orm/generative.py
class SliceTest(QueryTest):
    def test_first(self):
        User = self.classes.User

        assert User(id=7) == create_session().query(User).first()

        assert create_session().query(User).filter(User.id == 27). \
            first() is None

    def test_limit_offset_applies(self):
        """Test that the expected LIMIT/OFFSET is applied for slices.

        The LIMIT/OFFSET syntax differs slightly on all databases, and
        query[x:y] executes immediately, so we are asserting against
        SQL strings using sqlite's syntax.

        """

        User = self.classes.User

        sess = create_session()
        q = sess.query(User).order_by(User.id)

        self.assert_sql(
            testing.db, lambda: q[10:20], [
                (
                    "SELECT users.id AS users_id, users.name "
                    "AS users_name FROM users ORDER BY users.id "
                    "LIMIT :param_1 OFFSET :param_2",
                    {'param_1': 10, 'param_2': 10})])

        self.assert_sql(
            testing.db, lambda: q[:20], [
                (
                    "SELECT users.id AS users_id, users.name "
                    "AS users_name FROM users ORDER BY users.id "
                    "LIMIT :param_1",
                    {'param_1': 20})])

        self.assert_sql(
            testing.db, lambda: q[5:], [
                (
                    "SELECT users.id AS users_id, users.name "
                    "AS users_name FROM users ORDER BY users.id "
                    "LIMIT -1 OFFSET :param_1",
                    {'param_1': 5})])

        self.assert_sql(testing.db, lambda: q[2:2], [])

        self.assert_sql(testing.db, lambda: q[-2:-5], [])

        self.assert_sql(
            testing.db, lambda: q[-5:-2], [
                (
                    "SELECT users.id AS users_id, users.name AS users_name "
                    "FROM users ORDER BY users.id", {})])

        self.assert_sql(
            testing.db, lambda: q[-5:], [
                (
                    "SELECT users.id AS users_id, users.name AS users_name "
                    "FROM users ORDER BY users.id", {})])

        self.assert_sql(
            testing.db, lambda: q[:], [
                (
                    "SELECT users.id AS users_id, users.name AS users_name "
                    "FROM users ORDER BY users.id", {})])


class FilterTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_basic(self):
        User = self.classes.User

        users = create_session().query(User).all()
        eq_([User(id=7), User(id=8), User(id=9), User(id=10)], users)

    @testing.requires.offset
    def test_limit_offset(self):
        User = self.classes.User

        sess = create_session()

        assert [User(id=8), User(id=9)] == \
            sess.query(User).order_by(User.id).limit(2).offset(1).all()

        assert [User(id=8), User(id=9)] == \
            list(sess.query(User).order_by(User.id)[1:3])

        assert User(id=8) == sess.query(User).order_by(User.id)[1]

        assert [] == sess.query(User).order_by(User.id)[3:3]
        assert [] == sess.query(User).order_by(User.id)[0:0]

    @testing.requires.bound_limit_offset
    def test_select_with_bindparam_offset_limit(self):
        """Does a query allow bindparam for the limit?"""
        User = self.classes.User
        sess = create_session()
        q1 = sess.query(self.classes.User).\
            order_by(self.classes.User.id).limit(bindparam('n'))

        for n in range(1, 4):
            result = q1.params(n=n).all()
            eq_(len(result), n)

        eq_(
            sess.query(User).order_by(User.id).limit(bindparam('limit')).
            offset(bindparam('offset')).params(limit=2, offset=1).all(),
            [User(id=8), User(id=9)]
        )

    @testing.fails_on("mysql", "doesn't like CAST in the limit clause")
    @testing.requires.bound_limit_offset
    def test_select_with_bindparam_offset_limit_w_cast(self):
        User = self.classes.User
        sess = create_session()
        q1 = sess.query(self.classes.User).\
            order_by(self.classes.User.id).limit(bindparam('n'))
        eq_(
            list(
                sess.query(User).params(a=1, b=3).order_by(User.id)
                [cast(bindparam('a'), Integer):cast(bindparam('b'), Integer)]),
            [User(id=8), User(id=9)]
        )

    @testing.requires.boolean_col_expressions
    def test_exists(self):
        User = self.classes.User

        sess = create_session(testing.db)

        assert sess.query(exists().where(User.id == 9)).scalar()
        assert not sess.query(exists().where(User.id == 29)).scalar()

    def test_one_filter(self):
        User = self.classes.User

        assert [User(id=8), User(id=9)] == \
            create_session().query(User).filter(User.name.endswith('ed')).all()

    def test_contains(self):
        """test comparing a collection to an object instance."""

        User, Address = self.classes.User, self.classes.Address

        sess = create_session()
        address = sess.query(Address).get(3)
        assert [User(id=8)] == \
            sess.query(User).filter(User.addresses.contains(address)).all()

        try:
            sess.query(User).filter(User.addresses == address)
            assert False
        except sa_exc.InvalidRequestError:
            assert True

        assert [User(id=10)] == \
            sess.query(User).filter(User.addresses == None).all()  # noqa

        try:
            assert [User(id=7), User(id=9), User(id=10)] == \
                sess.query(User).filter(User.addresses != address).all()
            assert False
        except sa_exc.InvalidRequestError:
            assert True

        # assert [User(id=7), User(id=9), User(id=10)] ==
        # sess.query(User).filter(User.addresses!=address).all()

    def test_clause_element_ok(self):
        User = self.classes.User
        s = Session()
        self.assert_compile(
            s.query(User).filter(User.addresses),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users, addresses WHERE users.id = addresses.user_id"
        )

    def test_unique_binds_join_cond(self):
        """test that binds used when the lazyclause is used in criterion are
        unique"""

        User, Address = self.classes.User, self.classes.Address
        sess = Session()
        a1, a2 = sess.query(Address).order_by(Address.id)[0:2]
        self.assert_compile(
            sess.query(User).filter(User.addresses.contains(a1)).union(
                sess.query(User).filter(User.addresses.contains(a2))
            ),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name AS "
            "anon_1_users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users WHERE users.id = :param_1 "
            "UNION SELECT users.id AS users_id, users.name AS users_name "
            "FROM users WHERE users.id = :param_2) AS anon_1",
            checkparams={'param_1': 7, 'param_2': 8}
        )

    def test_any(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()

        assert [User(id=8), User(id=9)] == \
            sess.query(User). \
            filter(
                User.addresses.any(Address.email_address.like('%ed%'))).all()

        assert [User(id=8)] == \
            sess.query(User). \
            filter(
                User.addresses.any(
                    Address.email_address.like('%ed%'), id=4)).all()

        assert [User(id=8)] == \
            sess.query(User). \
            filter(User.addresses.any(Address.email_address.like('%ed%'))).\
            filter(User.addresses.any(id=4)).all()

        assert [User(id=9)] == \
            sess.query(User). \
            filter(User.addresses.any(email_address='fred@fred.com')).all()

        # test that the contents are not adapted by the aliased join
        assert [User(id=7), User(id=8)] == \
            sess.query(User).join("addresses", aliased=True). \
            filter(
                ~User.addresses.any(
                    Address.email_address == 'fred@fred.com')).all()

        assert [User(id=10)] == \
            sess.query(User).outerjoin("addresses", aliased=True). \
            filter(~User.addresses.any()).all()

    def test_any_doesnt_overcorrelate(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()

        # test that any() doesn't overcorrelate
        assert [User(id=7), User(id=8)] == \
            sess.query(User).join("addresses"). \
            filter(
                ~User.addresses.any(
                    Address.email_address == 'fred@fred.com')).all()

    def test_has(self):
        Dingaling, User, Address = (
            self.classes.Dingaling, self.classes.User, self.classes.Address)

        sess = create_session()
        assert [Address(id=5)] == \
            sess.query(Address).filter(Address.user.has(name='fred')).all()

        assert [Address(id=2), Address(id=3), Address(id=4), Address(id=5)] \
            == sess.query(Address). \
            filter(Address.user.has(User.name.like('%ed%'))). \
            order_by(Address.id).all()

        assert [Address(id=2), Address(id=3), Address(id=4)] == \
            sess.query(Address). \
            filter(Address.user.has(User.name.like('%ed%'), id=8)). \
            order_by(Address.id).all()

        # test has() doesn't overcorrelate
        assert [Address(id=2), Address(id=3), Address(id=4)] == \
            sess.query(Address).join("user"). \
            filter(Address.user.has(User.name.like('%ed%'), id=8)). \
            order_by(Address.id).all()

        # test has() doesn't get subquery contents adapted by aliased join
        assert [Address(id=2), Address(id=3), Address(id=4)] == \
            sess.query(Address).join("user", aliased=True). \
            filter(Address.user.has(User.name.like('%ed%'), id=8)). \
            order_by(Address.id).all()

        dingaling = sess.query(Dingaling).get(2)
        assert [User(id=9)] == \
            sess.query(User). \
            filter(User.addresses.any(Address.dingaling == dingaling)).all()

    def test_contains_m2m(self):
        Item, Order = self.classes.Item, self.classes.Order

        sess = create_session()
        item = sess.query(Item).get(3)

        eq_(
            sess.query(Order).filter(Order.items.contains(item)).
            order_by(Order.id).all(),
            [Order(id=1), Order(id=2), Order(id=3)]
        )
        eq_(
            sess.query(Order).filter(~Order.items.contains(item)).
            order_by(Order.id).all(),
            [Order(id=4), Order(id=5)]
        )

        item2 = sess.query(Item).get(5)
        eq_(
            sess.query(Order).filter(Order.items.contains(item)).
            filter(Order.items.contains(item2)).all(),
            [Order(id=3)]
        )

    def test_comparison(self):
        """test scalar comparison to an object instance"""

        Item, Order, Dingaling, User, Address = (
            self.classes.Item, self.classes.Order, self.classes.Dingaling,
            self.classes.User, self.classes.Address)

        sess = create_session()
        user = sess.query(User).get(8)
        assert [Address(id=2), Address(id=3), Address(id=4)] == \
            sess.query(Address).filter(Address.user == user).all()

        assert [Address(id=1), Address(id=5)] == \
            sess.query(Address).filter(Address.user != user).all()

        # generates an IS NULL
        assert [] == sess.query(Address).filter(Address.user == None).all()  # noqa
        assert [] == sess.query(Address).filter(Address.user == null()).all()

        assert [Order(id=5)] == \
            sess.query(Order).filter(Order.address == None).all()  # noqa

        # o2o
        dingaling = sess.query(Dingaling).get(2)
        assert [Address(id=5)] == \
            sess.query(Address).filter(Address.dingaling == dingaling).all()

        # m2m
        eq_(
            sess.query(Item).filter(Item.keywords == None).  # noqa
            order_by(Item.id).all(), [Item(id=4), Item(id=5)])
        eq_(
            sess.query(Item).filter(Item.keywords != None).  # noqa
            order_by(Item.id).all(), [Item(id=1), Item(id=2), Item(id=3)])

    def test_filter_by(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()
        user = sess.query(User).get(8)
        assert [Address(id=2), Address(id=3), Address(id=4)] == \
            sess.query(Address).filter_by(user=user).all()

        # many to one generates IS NULL
        assert [] == sess.query(Address).filter_by(user=None).all()
        assert [] == sess.query(Address).filter_by(user=null()).all()

        # one to many generates WHERE NOT EXISTS
        assert [User(name='chuck')] == \
            sess.query(User).filter_by(addresses=None).all()
        assert [User(name='chuck')] == \
            sess.query(User).filter_by(addresses=null()).all()

    def test_filter_by_tables(self):
        users = self.tables.users
        addresses = self.tables.addresses
        sess = create_session()
        self.assert_compile(
            sess.query(users).filter_by(name='ed').
            join(addresses, users.c.id == addresses.c.user_id).
            filter_by(email_address='ed@ed.com'),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses ON users.id = addresses.user_id "
            "WHERE users.name = :name_1 AND "
            "addresses.email_address = :email_address_1",
            checkparams={'email_address_1': 'ed@ed.com', 'name_1': 'ed'}
        )

    def test_filter_by_no_property(self):
        addresses = self.tables.addresses
        sess = create_session()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Entity 'addresses' has no property 'name'",
            sess.query(addresses).filter_by, name='ed'
        )

    def test_none_comparison(self):
        Order, User, Address = (
            self.classes.Order, self.classes.User, self.classes.Address)

        sess = create_session()

        # scalar
        eq_(
            [Order(description="order 5")],
            sess.query(Order).filter(Order.address_id == None).all()  # noqa
        )
        eq_(
            [Order(description="order 5")],
            sess.query(Order).filter(Order.address_id == null()).all()
        )

        # o2o
        eq_(
            [Address(id=1), Address(id=3), Address(id=4)],
            sess.query(Address).filter(Address.dingaling == None).  # noqa
            order_by(Address.id).all())
        eq_(
            [Address(id=1), Address(id=3), Address(id=4)],
            sess.query(Address).filter(Address.dingaling == null()).
            order_by(Address.id).all())
        eq_(
            [Address(id=2), Address(id=5)],
            sess.query(Address).filter(Address.dingaling != None).  # noqa
            order_by(Address.id).all())
        eq_(
            [Address(id=2), Address(id=5)],
            sess.query(Address).filter(Address.dingaling != null()).
            order_by(Address.id).all())

        # m2o
        eq_(
            [Order(id=5)],
            sess.query(Order).filter(Order.address == None).all())  # noqa
        eq_(
            [Order(id=1), Order(id=2), Order(id=3), Order(id=4)],
            sess.query(Order).order_by(Order.id).
            filter(Order.address != None).all())  # noqa

        # o2m
        eq_(
            [User(id=10)],
            sess.query(User).filter(User.addresses == None).all())  # noqa
        eq_(
            [User(id=7), User(id=8), User(id=9)],
            sess.query(User).filter(User.addresses != None).  # noqa
            order_by(User.id).all())

    def test_blank_filter_by(self):
        User = self.classes.User

        eq_(
            [(7,), (8,), (9,), (10,)],
            create_session().query(User.id).filter_by().order_by(User.id).all()
        )
        eq_(
            [(7,), (8,), (9,), (10,)],
            create_session().query(User.id).filter_by(**{}).
            order_by(User.id).all()
        )

    def test_text_coerce(self):
        User = self.classes.User
        s = create_session()
        self.assert_compile(
            s.query(User).filter(text("name='ed'")),
            "SELECT users.id AS users_id, users.name "
            "AS users_name FROM users WHERE name='ed'"
        )


class HasMapperEntitiesTest(QueryTest):
    def test_entity(self):
        User = self.classes.User
        s = Session()

        q = s.query(User)

        assert q._has_mapper_entities

    def test_cols(self):
        User = self.classes.User
        s = Session()

        q = s.query(User.id)

        assert not q._has_mapper_entities

    def test_cols_set_entities(self):
        User = self.classes.User
        s = Session()

        q = s.query(User.id)

        q._set_entities(User)
        assert q._has_mapper_entities

    def test_entity_set_entities(self):
        User = self.classes.User
        s = Session()

        q = s.query(User)

        q._set_entities(User.id)
        assert not q._has_mapper_entities


class SetOpsTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_union(self):
        User = self.classes.User

        s = create_session()

        fred = s.query(User).filter(User.name == 'fred')
        ed = s.query(User).filter(User.name == 'ed')
        jack = s.query(User).filter(User.name == 'jack')

        eq_(
            fred.union(ed).order_by(User.name).all(),
            [User(name='ed'), User(name='fred')]
        )

        eq_(
            fred.union(ed, jack).order_by(User.name).all(),
            [User(name='ed'), User(name='fred'), User(name='jack')]
        )

    def test_statement_labels(self):
        """test that label conflicts don't occur with joins etc."""

        User, Address = self.classes.User, self.classes.Address

        s = create_session()
        q1 = s.query(User, Address).join(User.addresses).\
            filter(Address.email_address == "ed@wood.com")
        q2 = s.query(User, Address).join(User.addresses).\
            filter(Address.email_address == "jack@bean.com")
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

        User = self.classes.User

        s = Session()
        q1 = s.query(User, literal("x"))
        q2 = s.query(User, literal_column("'y'"))
        q3 = q1.union(q2)

        self.assert_compile(
            q3,
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "anon_1.param_1 AS anon_1_param_1 "
            "FROM (SELECT users.id AS users_id, users.name AS "
            "users_name, :param_1 AS param_1 "
            "FROM users UNION SELECT users.id AS users_id, "
            "users.name AS users_name, 'y' FROM users) AS anon_1"
        )

    def test_union_literal_expressions_results(self):
        User = self.classes.User

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

        for q in (
                q3.order_by(User.id, text("anon_1_param_1")),
                q6.order_by(User.id, "foo")):
            eq_(
                q.all(),
                [
                    (User(id=7, name='jack'), 'x'),
                    (User(id=7, name='jack'), 'y'),
                    (User(id=8, name='ed'), 'x'),
                    (User(id=8, name='ed'), 'y'),
                    (User(id=9, name='fred'), 'x'),
                    (User(id=9, name='fred'), 'y'),
                    (User(id=10, name='chuck'), 'x'),
                    (User(id=10, name='chuck'), 'y')
                ]
            )

    def test_union_labeled_anonymous_columns(self):
        User = self.classes.User

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
            "FROM users) AS anon_1"
        )

    def test_order_by_anonymous_col(self):
        User = self.classes.User

        s = Session()

        c1, c2 = column('c1'), column('c2')
        f = c1.label('foo')
        q1 = s.query(User, f, c2.label('bar'))
        q2 = s.query(User, c1.label('foo'), c2.label('bar'))
        q3 = q1.union(q2)

        self.assert_compile(
            q3.order_by(c1),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name AS "
            "anon_1_users_name, anon_1.foo AS anon_1_foo, anon_1.bar AS "
            "anon_1_bar FROM (SELECT users.id AS users_id, users.name AS "
            "users_name, c1 AS foo, c2 AS bar "
            "FROM users UNION SELECT users.id "
            "AS users_id, users.name AS users_name, c1 AS foo, c2 AS bar "
            "FROM users) AS anon_1 ORDER BY anon_1.foo"
        )

        self.assert_compile(
            q3.order_by(f),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name AS "
            "anon_1_users_name, anon_1.foo AS anon_1_foo, anon_1.bar AS "
            "anon_1_bar FROM (SELECT users.id AS users_id, users.name AS "
            "users_name, c1 AS foo, c2 AS bar "
            "FROM users UNION SELECT users.id "
            "AS users_id, users.name AS users_name, c1 AS foo, c2 AS bar "
            "FROM users) AS anon_1 ORDER BY anon_1.foo"
        )

    def test_union_mapped_colnames_preserved_across_subquery(self):
        User = self.classes.User

        s = Session()
        q1 = s.query(User.name)
        q2 = s.query(User.name)

        # the label names in the subquery are the typical anonymized ones
        self.assert_compile(
            q1.union(q2),
            "SELECT anon_1.users_name AS anon_1_users_name "
            "FROM (SELECT users.name AS users_name FROM users "
            "UNION SELECT users.name AS users_name FROM users) AS anon_1"
        )

        # but in the returned named tuples,
        # due to [ticket:1942], this should be 'name', not 'users_name'
        eq_(
            [x['name'] for x in q1.union(q2).column_descriptions],
            ['name']
        )

    @testing.requires.intersect
    def test_intersect(self):
        User = self.classes.User

        s = create_session()

        fred = s.query(User).filter(User.name == 'fred')
        ed = s.query(User).filter(User.name == 'ed')
        jack = s.query(User).filter(User.name == 'jack')
        eq_(fred.intersect(ed, jack).all(), [])

        eq_(fred.union(ed).intersect(ed.union(jack)).all(), [User(name='ed')])

    def test_eager_load(self):
        User, Address = self.classes.User, self.classes.Address

        s = create_session()

        fred = s.query(User).filter(User.name == 'fred')
        ed = s.query(User).filter(User.name == 'ed')

        def go():
            eq_(
                fred.union(ed).order_by(User.name).
                options(joinedload(User.addresses)).all(), [
                    User(
                        name='ed', addresses=[Address(), Address(),
                                              Address()]),
                    User(name='fred', addresses=[Address()])]
            )
        self.assert_sql_count(testing.db, go, 1)


class AggregateTest(QueryTest):

    def test_sum(self):
        Order = self.classes.Order

        sess = create_session()
        orders = sess.query(Order).filter(Order.id.in_([2, 3, 4]))
        eq_(
            next(orders.values(func.sum(Order.user_id * Order.address_id))),
            (79,))
        eq_(orders.value(func.sum(Order.user_id * Order.address_id)), 79)

    def test_apply(self):
        Order = self.classes.Order

        sess = create_session()
        assert sess.query(func.sum(Order.user_id * Order.address_id)). \
            filter(Order.id.in_([2, 3, 4])).one() == (79,)

    def test_having(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()
        assert [User(name='ed', id=8)] == \
            sess.query(User).order_by(User.id).group_by(User). \
            join('addresses').having(func.count(Address.id) > 2).all()

        assert [User(name='jack', id=7), User(name='fred', id=9)] == \
            sess.query(User).order_by(User.id).group_by(User). \
            join('addresses').having(func.count(Address.id) < 2).all()


class ExistsTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_exists(self):
        User = self.classes.User
        sess = create_session()

        q1 = sess.query(User)
        self.assert_compile(
            sess.query(q1.exists()),
            'SELECT EXISTS ('
            'SELECT 1 FROM users'
            ') AS anon_1'
        )

        q2 = sess.query(User).filter(User.name == 'fred')
        self.assert_compile(
            sess.query(q2.exists()),
            'SELECT EXISTS ('
            'SELECT 1 FROM users WHERE users.name = :name_1'
            ') AS anon_1'
        )

    def test_exists_col_warning(self):
        User = self.classes.User
        Address = self.classes.Address
        sess = create_session()

        q1 = sess.query(User, Address).filter(User.id == Address.user_id)
        self.assert_compile(
            sess.query(q1.exists()),
            'SELECT EXISTS ('
            'SELECT 1 FROM users, addresses '
            'WHERE users.id = addresses.user_id'
            ') AS anon_1'
        )

    def test_exists_w_select_from(self):
        User = self.classes.User
        sess = create_session()

        q1 = sess.query().select_from(User).exists()
        self.assert_compile(
            sess.query(q1),
            'SELECT EXISTS (SELECT 1 FROM users) AS anon_1'
        )


class CountTest(QueryTest):
    def test_basic(self):
        users, User = self.tables.users, self.classes.User

        s = create_session()

        eq_(s.query(User).count(), 4)

        eq_(s.query(User).filter(users.c.name.endswith('ed')).count(), 2)

    def test_count_char(self):
        User = self.classes.User
        s = create_session()
        # '*' is favored here as the most common character,
        # it is reported that Informix doesn't like count(1),
        # rumors about Oracle preferring count(1) don't appear
        # to be well founded.
        self.assert_sql_execution(
            testing.db, s.query(User).count, CompiledSQL(
                "SELECT count(*) AS count_1 FROM "
                "(SELECT users.id AS users_id, users.name "
                "AS users_name FROM users) AS anon_1", {}
            )
        )

    def test_multiple_entity(self):
        User, Address = self.classes.User, self.classes.Address

        s = create_session()
        q = s.query(User, Address)
        eq_(q.count(), 20)  # cartesian product

        q = s.query(User, Address).join(User.addresses)
        eq_(q.count(), 5)

    def test_nested(self):
        User, Address = self.classes.User, self.classes.Address

        s = create_session()
        q = s.query(User, Address).limit(2)
        eq_(q.count(), 2)

        q = s.query(User, Address).limit(100)
        eq_(q.count(), 20)

        q = s.query(User, Address).join(User.addresses).limit(100)
        eq_(q.count(), 5)

    def test_cols(self):
        """test that column-based queries always nest."""

        User, Address = self.classes.User, self.classes.Address

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


class DistinctTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_basic(self):
        User = self.classes.User

        eq_(
            [User(id=7), User(id=8), User(id=9), User(id=10)],
            create_session().query(User).order_by(User.id).distinct().all()
        )
        eq_(
            [User(id=7), User(id=9), User(id=8), User(id=10)],
            create_session().query(User).distinct().
            order_by(desc(User.name)).all()
        )

    def test_columns_augmented_roundtrip_one(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()
        q = sess.query(User).join('addresses').distinct(). \
            order_by(desc(Address.email_address))

        eq_(
            [User(id=7), User(id=9), User(id=8)],
            q.all()
        )

    def test_columns_augmented_roundtrip_two(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()

        # test that it works on embedded joinedload/LIMIT subquery
        q = sess.query(User).join('addresses').distinct(). \
            options(joinedload('addresses')).\
            order_by(desc(Address.email_address)).limit(2)

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

    def test_columns_augmented_roundtrip_three(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()

        q = sess.query(User.id, User.name.label('foo'), Address.id).\
            filter(User.name == 'jack').\
            distinct().\
            order_by(User.id, User.name, Address.email_address)

        # even though columns are added, they aren't in the result
        eq_(
            q.all(),
            [(7, 'jack', 3), (7, 'jack', 4), (7, 'jack', 2),
             (7, 'jack', 5), (7, 'jack', 1)]
        )
        for row in q:
            eq_(row.keys(), ['id', 'foo', 'id'])

    def test_columns_augmented_sql_one(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()

        q = sess.query(User.id, User.name.label('foo'), Address.id).\
            distinct().\
            order_by(User.id, User.name, Address.email_address)

        # Address.email_address is added because of DISTINCT,
        # however User.id, User.name are not b.c. they're already there,
        # even though User.name is labeled
        self.assert_compile(
            q,
            "SELECT DISTINCT users.id AS users_id, users.name AS foo, "
            "addresses.id AS addresses_id, "
            "addresses.email_address AS addresses_email_address FROM users, "
            "addresses ORDER BY users.id, users.name, addresses.email_address"
        )

    def test_columns_augmented_sql_two(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()

        q = sess.query(User).\
            options(joinedload(User.addresses)).\
            distinct().\
            order_by(User.name, Address.email_address).\
            limit(5)

        # addresses.email_address is added to inner query so that
        # it is available in ORDER BY
        self.assert_compile(
            q,
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "anon_1.addresses_email_address AS "
            "anon_1_addresses_email_address, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM (SELECT DISTINCT users.id AS users_id, "
            "users.name AS users_name, "
            "addresses.email_address AS addresses_email_address "
            "FROM users, addresses "
            "ORDER BY users.name, addresses.email_address "
            "LIMIT :param_1) AS anon_1 LEFT OUTER JOIN "
            "addresses AS addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id "
            "ORDER BY anon_1.users_name, "
            "anon_1.addresses_email_address, addresses_1.id"
        )

    def test_columns_augmented_sql_three(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()

        q = sess.query(User.id, User.name.label('foo'), Address.id).\
            distinct(User.name).\
            order_by(User.id, User.name, Address.email_address)

        # no columns are added when DISTINCT ON is used
        self.assert_compile(
            q,
            "SELECT DISTINCT ON (users.name) users.id AS users_id, "
            "users.name AS foo, addresses.id AS addresses_id FROM users, "
            "addresses ORDER BY users.id, users.name, addresses.email_address",
            dialect='postgresql'
        )

    def test_columns_augmented_sql_four(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()

        q = sess.query(User).join('addresses').\
            distinct(Address.email_address). \
            options(joinedload('addresses')).\
            order_by(desc(Address.email_address)).limit(2)

        # but for the subquery / eager load case, we still need to make
        # the inner columns available for the ORDER BY even though its
        # a DISTINCT ON
        self.assert_compile(
            q,
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "anon_1.addresses_email_address AS "
            "anon_1_addresses_email_address, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM (SELECT DISTINCT ON (addresses.email_address) "
            "users.id AS users_id, users.name AS users_name, "
            "addresses.email_address AS addresses_email_address "
            "FROM users JOIN addresses ON users.id = addresses.user_id "
            "ORDER BY addresses.email_address DESC  "
            "LIMIT %(param_1)s) AS anon_1 "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id "
            "ORDER BY anon_1.addresses_email_address DESC, addresses_1.id",
            dialect='postgresql'
        )


class PrefixWithTest(QueryTest, AssertsCompiledSQL):

    def test_one_prefix(self):
        User = self.classes.User
        sess = create_session()
        query = sess.query(User.name)\
            .prefix_with('PREFIX_1')
        expected = "SELECT PREFIX_1 "\
            "users.name AS users_name FROM users"
        self.assert_compile(query, expected, dialect=default.DefaultDialect())

    def test_many_prefixes(self):
        User = self.classes.User
        sess = create_session()
        query = sess.query(User.name).prefix_with('PREFIX_1', 'PREFIX_2')
        expected = "SELECT PREFIX_1 PREFIX_2 "\
            "users.name AS users_name FROM users"
        self.assert_compile(query, expected, dialect=default.DefaultDialect())

    def test_chained_prefixes(self):
        User = self.classes.User
        sess = create_session()
        query = sess.query(User.name)\
            .prefix_with('PREFIX_1')\
            .prefix_with('PREFIX_2', 'PREFIX_3')
        expected = "SELECT PREFIX_1 PREFIX_2 PREFIX_3 "\
            "users.name AS users_name FROM users"
        self.assert_compile(query, expected, dialect=default.DefaultDialect())


class YieldTest(_fixtures.FixtureTest):
    run_setup_mappers = 'each'
    run_inserts = 'each'

    def _eagerload_mappings(self, addresses_lazy=True, user_lazy=True):
        User, Address = self.classes("User", "Address")
        users, addresses = self.tables("users", "addresses")
        mapper(User, users, properties={
            "addresses": relationship(
                Address, lazy=addresses_lazy,
                backref=backref("user", lazy=user_lazy)
            )
        })
        mapper(Address, addresses)

    def test_basic(self):
        self._eagerload_mappings()

        User = self.classes.User

        sess = create_session()
        q = iter(
            sess.query(User).yield_per(1).from_statement(
                text("select * from users")))

        ret = []
        eq_(len(sess.identity_map), 0)
        ret.append(next(q))
        ret.append(next(q))
        eq_(len(sess.identity_map), 2)
        ret.append(next(q))
        ret.append(next(q))
        eq_(len(sess.identity_map), 4)
        try:
            next(q)
            assert False
        except StopIteration:
            pass

    def test_yield_per_and_execution_options(self):
        self._eagerload_mappings()

        User = self.classes.User

        sess = create_session()
        q = sess.query(User).yield_per(15)
        q = q.execution_options(foo='bar')
        assert q._yield_per
        eq_(
            q._execution_options,
            {"stream_results": True, "foo": "bar", "max_row_buffer": 15})

    def test_no_joinedload_opt(self):
        self._eagerload_mappings()

        User = self.classes.User
        sess = create_session()
        q = sess.query(User).options(joinedload("addresses")).yield_per(1)
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "The yield_per Query option is currently not compatible with "
            "joined collection eager loading.  Please specify ",
            q.all
        )

    def test_no_subqueryload_opt(self):
        self._eagerload_mappings()

        User = self.classes.User
        sess = create_session()
        q = sess.query(User).options(subqueryload("addresses")).yield_per(1)
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "The yield_per Query option is currently not compatible with "
            "subquery eager loading.  Please specify ",
            q.all
        )

    def test_no_subqueryload_mapping(self):
        self._eagerload_mappings(addresses_lazy="subquery")

        User = self.classes.User
        sess = create_session()
        q = sess.query(User).yield_per(1)
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "The yield_per Query option is currently not compatible with "
            "subquery eager loading.  Please specify ",
            q.all
        )

    def test_joinedload_m2o_ok(self):
        self._eagerload_mappings(user_lazy="joined")
        Address = self.classes.Address
        sess = create_session()
        q = sess.query(Address).yield_per(1)
        q.all()

    def test_eagerload_opt_disable(self):
        self._eagerload_mappings()

        User = self.classes.User
        sess = create_session()
        q = sess.query(User).options(subqueryload("addresses")).\
            enable_eagerloads(False).yield_per(1)
        q.all()

        q = sess.query(User).options(joinedload("addresses")).\
            enable_eagerloads(False).yield_per(1)
        q.all()

    def test_m2o_joinedload_not_others(self):
        self._eagerload_mappings(addresses_lazy="joined")
        Address = self.classes.Address
        sess = create_session()
        q = sess.query(Address).options(
            lazyload('*'), joinedload("user")).yield_per(1).filter_by(id=1)

        def go():
            result = q.all()
            assert result[0].user
        self.assert_sql_count(testing.db, go, 1)


class HintsTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_hints(self):
        User = self.classes.User

        from sqlalchemy.dialects import mysql
        dialect = mysql.dialect()

        sess = create_session()

        self.assert_compile(
            sess.query(User).with_hint(
                User, 'USE INDEX (col1_index,col2_index)'),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users USE INDEX (col1_index,col2_index)",
            dialect=dialect
        )

        self.assert_compile(
            sess.query(User).with_hint(
                User, 'WITH INDEX col1_index', 'sybase'),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users", dialect=dialect
        )

        ualias = aliased(User)
        self.assert_compile(
            sess.query(User, ualias).with_hint(
                ualias, 'USE INDEX (col1_index,col2_index)').
            join(ualias, ualias.id > User.id),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users INNER JOIN users AS users_1 "
            "USE INDEX (col1_index,col2_index) "
            "ON users_1.id > users.id", dialect=dialect
        )

    def test_statement_hints(self):
        User = self.classes.User

        sess = create_session()
        stmt = sess.query(User).\
            with_statement_hint("test hint one").\
            with_statement_hint("test hint two").\
            with_statement_hint("test hint three", "postgresql")

        self.assert_compile(
            stmt,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users test hint one test hint two",
        )

        self.assert_compile(
            stmt,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users test hint one test hint two test hint three",
            dialect='postgresql'
        )


class TextTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_fulltext(self):
        User = self.classes.User

        with expect_warnings("Textual SQL"):
            eq_(
                create_session().query(User).
                from_statement("select * from users order by id").all(),
                [User(id=7), User(id=8), User(id=9), User(id=10)]
            )

        eq_(
            create_session().query(User).from_statement(
                text("select * from users order by id")).first(), User(id=7)
        )
        eq_(
            create_session().query(User).from_statement(
                text("select * from users where name='nonexistent'")).first(),
            None)

    def test_fragment(self):
        User = self.classes.User

        with expect_warnings("Textual SQL expression"):
            eq_(
                create_session().query(User).filter("id in (8, 9)").all(),
                [User(id=8), User(id=9)]

            )

            eq_(
                create_session().query(User).filter("name='fred'").
                filter("id=9").all(), [User(id=9)]
            )
            eq_(
                create_session().query(User).filter("name='fred'").
                filter(User.id == 9).all(), [User(id=9)]
            )

    def test_binds_coerce(self):
        User = self.classes.User

        with expect_warnings("Textual SQL expression"):
            eq_(
                create_session().query(User).filter("id in (:id1, :id2)").
                params(id1=8, id2=9).all(), [User(id=8), User(id=9)]
            )

    def test_as_column(self):
        User = self.classes.User

        s = create_session()
        assert_raises(
            sa_exc.InvalidRequestError, s.query,
            User.id, text("users.name"))

        eq_(
            s.query(User.id, "name").order_by(User.id).all(),
            [(7, 'jack'), (8, 'ed'), (9, 'fred'), (10, 'chuck')])

    def test_via_select(self):
        User = self.classes.User
        s = create_session()
        eq_(
            s.query(User).from_statement(
                select([column('id'), column('name')]).
                select_from(table('users')).order_by('id'),
            ).all(),
            [User(id=7), User(id=8), User(id=9), User(id=10)]
        )

    def test_via_textasfrom_from_statement(self):
        User = self.classes.User
        s = create_session()

        eq_(
            s.query(User).from_statement(
                text("select * from users order by id").
                columns(id=Integer, name=String)).all(),
            [User(id=7), User(id=8), User(id=9), User(id=10)]
        )

    def test_via_textasfrom_use_mapped_columns(self):
        User = self.classes.User
        s = create_session()

        eq_(
            s.query(User).from_statement(
                text("select * from users order by id").
                columns(User.id, User.name)).all(),
            [User(id=7), User(id=8), User(id=9), User(id=10)]
        )

    def test_via_textasfrom_select_from(self):
        User = self.classes.User
        s = create_session()

        eq_(
            s.query(User).select_from(
                text("select * from users").columns(id=Integer, name=String)
            ).order_by(User.id).all(),
            [User(id=7), User(id=8), User(id=9), User(id=10)]
        )

    def test_group_by_accepts_text(self):
        User = self.classes.User
        s = create_session()

        q = s.query(User).group_by(text("name"))
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users GROUP BY name"
        )

    def test_orm_columns_accepts_text(self):
        from sqlalchemy.orm.base import _orm_columns
        t = text("x")
        eq_(
            _orm_columns(t),
            [t]
        )

    def test_order_by_w_eager_one(self):
        User = self.classes.User
        s = create_session()

        # from 1.0.0 thru 1.0.2, the "name" symbol here was considered
        # to be part of the things we need to ORDER BY and it was being
        # placed into the inner query's columns clause, as part of
        # query._compound_eager_statement where we add unwrap_order_by()
        # to the columns clause.  However, as #3392 illustrates, unlocatable
        # string expressions like "name desc" will only fail in this scenario,
        # so in general the changing of the query structure with string labels
        # is dangerous.
        #
        # the queries here are again "invalid" from a SQL perspective, as the
        # "name" field isn't matched up to anything.
        #
        with expect_warnings("Can't resolve label reference 'name';"):
            self.assert_compile(
                s.query(User).options(joinedload("addresses")).
                order_by(desc("name")).limit(1),
                "SELECT anon_1.users_id AS anon_1_users_id, "
                "anon_1.users_name AS anon_1_users_name, "
                "addresses_1.id AS addresses_1_id, "
                "addresses_1.user_id AS addresses_1_user_id, "
                "addresses_1.email_address AS addresses_1_email_address "
                "FROM (SELECT users.id AS users_id, users.name AS users_name "
                "FROM users ORDER BY users.name "
                "DESC LIMIT :param_1) AS anon_1 "
                "LEFT OUTER JOIN addresses AS addresses_1 "
                "ON anon_1.users_id = addresses_1.user_id "
                "ORDER BY name DESC, addresses_1.id"
            )

    def test_order_by_w_eager_two(self):
        User = self.classes.User
        s = create_session()

        with expect_warnings("Can't resolve label reference 'name';"):
            self.assert_compile(
                s.query(User).options(joinedload("addresses")).
                order_by("name").limit(1),
                "SELECT anon_1.users_id AS anon_1_users_id, "
                "anon_1.users_name AS anon_1_users_name, "
                "addresses_1.id AS addresses_1_id, "
                "addresses_1.user_id AS addresses_1_user_id, "
                "addresses_1.email_address AS addresses_1_email_address "
                "FROM (SELECT users.id AS users_id, users.name AS users_name "
                "FROM users ORDER BY users.name "
                "LIMIT :param_1) AS anon_1 "
                "LEFT OUTER JOIN addresses AS addresses_1 "
                "ON anon_1.users_id = addresses_1.user_id "
                "ORDER BY name, addresses_1.id"
            )

    def test_order_by_w_eager_three(self):
        User = self.classes.User
        s = create_session()

        self.assert_compile(
            s.query(User).options(joinedload("addresses")).
            order_by("users_name").limit(1),
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM (SELECT users.id AS users_id, users.name AS users_name "
            "FROM users ORDER BY users.name "
            "LIMIT :param_1) AS anon_1 "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id "
            "ORDER BY anon_1.users_name, addresses_1.id"
        )

        # however! this works (again?)
        eq_(
            s.query(User).options(joinedload("addresses")).
            order_by("users_name").first(),
            User(name='chuck', addresses=[])
        )

    def test_order_by_w_eager_four(self):
        User = self.classes.User
        Address = self.classes.Address
        s = create_session()

        self.assert_compile(
            s.query(User).options(joinedload("addresses")).
            order_by(desc("users_name")).limit(1),
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM (SELECT users.id AS users_id, users.name AS users_name "
            "FROM users ORDER BY users.name DESC "
            "LIMIT :param_1) AS anon_1 "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id "
            "ORDER BY anon_1.users_name DESC, addresses_1.id"
        )

        # however! this works (again?)
        eq_(
            s.query(User).options(joinedload("addresses")).
            order_by(desc("users_name")).first(),
            User(name='jack', addresses=[Address()])
        )

    def test_order_by_w_eager_five(self):
        """essentially the same as test_eager_relations -> test_limit_3,
        but test for textual label elements that are freeform.
        this is again #3392."""

        User = self.classes.User
        Address = self.classes.Address
        Order = self.classes.Order

        sess = create_session()

        q = sess.query(User, Address.email_address.label('email_address'))

        result = q.join('addresses').options(joinedload(User.orders)).\
            order_by(
            "email_address desc").limit(1).offset(0)
        with expect_warnings(
                "Can't resolve label reference 'email_address desc'"):
            eq_(
                [
                    (User(
                        id=7,
                        orders=[Order(id=1), Order(id=3), Order(id=5)],
                        addresses=[Address(id=1)]
                    ), 'jack@bean.com')
                ],
                result.all())


class TextWarningTest(QueryTest, AssertsCompiledSQL):
    def _test(self, fn, arg, offending_clause, expected):
        assert_raises_message(
            sa.exc.SAWarning,
            r"Textual (?:SQL|column|SQL FROM) expression %(stmt)r should be "
            r"explicitly declared (?:with|as) text\(%(stmt)r\)" % {
                "stmt": util.ellipses_string(offending_clause),
            },
            fn, arg
        )

        with expect_warnings("Textual "):
            stmt = fn(arg)
            self.assert_compile(stmt, expected)

    def test_filter(self):
        User = self.classes.User
        self._test(
            Session().query(User.id).filter, "myid == 5", "myid == 5",
            "SELECT users.id AS users_id FROM users WHERE myid == 5"
        )

    def test_having(self):
        User = self.classes.User
        self._test(
            Session().query(User.id).having, "myid == 5", "myid == 5",
            "SELECT users.id AS users_id FROM users HAVING myid == 5"
        )

    def test_from_statement(self):
        User = self.classes.User
        self._test(
            Session().query(User.id).from_statement,
            "select id from user",
            "select id from user",
            "select id from user",
        )


class ParentTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_o2m(self):
        User, orders, Order = (
            self.classes.User, self.tables.orders, self.classes.Order)

        sess = create_session()
        q = sess.query(User)

        u1 = q.filter_by(name='jack').one()

        # test auto-lookup of property
        o = sess.query(Order).with_parent(u1).all()
        assert [Order(description="order 1"), Order(description="order 3"),
                Order(description="order 5")] == o

        # test with explicit property
        o = sess.query(Order).with_parent(u1, property='orders').all()
        assert [Order(description="order 1"), Order(description="order 3"),
                Order(description="order 5")] == o

        o = sess.query(Order).with_parent(u1, property=User.orders).all()
        assert [Order(description="order 1"), Order(description="order 3"),
                Order(description="order 5")] == o

        o = sess.query(Order).filter(with_parent(u1, User.orders)).all()
        assert [
            Order(description="order 1"), Order(description="order 3"),
            Order(description="order 5")] == o

        # test generative criterion
        o = sess.query(Order).with_parent(u1).filter(orders.c.id > 2).all()
        assert [
            Order(description="order 3"), Order(description="order 5")] == o

        # test against None for parent? this can't be done with the current
        # API since we don't know what mapper to use
        # assert
        #     sess.query(Order).with_parent(None, property='addresses').all()
        #     == [Order(description="order 5")]

    def test_select_from(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()
        u1 = sess.query(User).get(7)
        q = sess.query(Address).select_from(Address).with_parent(u1)
        self.assert_compile(
            q,
            "SELECT addresses.id AS addresses_id, "
            "addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM addresses WHERE :param_1 = addresses.user_id",
            {'param_1': 7}
        )

    def test_from_entity_standalone_fn(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()
        u1 = sess.query(User).get(7)
        q = sess.query(User, Address).filter(
            with_parent(u1, "addresses", from_entity=Address))
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "addresses.id AS addresses_id, addresses.user_id "
            "AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM users, addresses "
            "WHERE :param_1 = addresses.user_id",
            {'param_1': 7}
        )

    def test_from_entity_query_entity(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()
        u1 = sess.query(User).get(7)
        q = sess.query(User, Address).with_parent(
            u1, "addresses", from_entity=Address)
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "addresses.id AS addresses_id, addresses.user_id "
            "AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM users, addresses "
            "WHERE :param_1 = addresses.user_id",
            {'param_1': 7}
        )

    def test_select_from_alias(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()
        u1 = sess.query(User).get(7)
        a1 = aliased(Address)
        q = sess.query(a1).with_parent(u1)
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM addresses AS addresses_1 "
            "WHERE :param_1 = addresses_1.user_id",
            {'param_1': 7}
        )

    def test_select_from_alias_explicit_prop(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()
        u1 = sess.query(User).get(7)
        a1 = aliased(Address)
        q = sess.query(a1).with_parent(u1, "addresses")
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM addresses AS addresses_1 "
            "WHERE :param_1 = addresses_1.user_id",
            {'param_1': 7}
        )

    def test_noparent(self):
        Item, User = self.classes.Item, self.classes.User

        sess = create_session()
        q = sess.query(User)

        u1 = q.filter_by(name='jack').one()

        try:
            q = sess.query(Item).with_parent(u1)
            assert False
        except sa_exc.InvalidRequestError as e:
            assert str(e) \
                == "Could not locate a property which relates "\
                "instances of class 'Item' to instances of class 'User'"

    def test_m2m(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        sess = create_session()
        i1 = sess.query(Item).filter_by(id=2).one()
        k = sess.query(Keyword).with_parent(i1).all()
        assert [
            Keyword(name='red'), Keyword(name='small'),
            Keyword(name='square')] == k

    def test_with_transient(self):
        User, Order = self.classes.User, self.classes.Order

        sess = Session()

        q = sess.query(User)
        u1 = q.filter_by(name='jack').one()
        utrans = User(id=u1.id)
        o = sess.query(Order).with_parent(utrans, 'orders')
        eq_(
            [
                Order(description="order 1"), Order(description="order 3"),
                Order(description="order 5")],
            o.all()
        )

        o = sess.query(Order).filter(with_parent(utrans, 'orders'))
        eq_(
            [
                Order(description="order 1"), Order(description="order 3"),
                Order(description="order 5")],
            o.all()
        )

    def test_with_pending_autoflush(self):
        Order, User = self.classes.Order, self.classes.User

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
        Order, User = self.classes.Order, self.classes.User

        sess = Session(autoflush=False)

        o1 = sess.query(Order).first()
        opending = Order(user_id=o1.user_id)
        sess.add(opending)
        eq_(
            sess.query(User).with_parent(opending, 'user').one(),
            User(id=o1.user_id)
        )

    def test_unique_binds_union(self):
        """bindparams used in the 'parent' query are unique"""
        User, Address = self.classes.User, self.classes.Address

        sess = Session()
        u1, u2 = sess.query(User).order_by(User.id)[0:2]

        q1 = sess.query(Address).with_parent(u1, 'addresses')
        q2 = sess.query(Address).with_parent(u2, 'addresses')

        self.assert_compile(
            q1.union(q2),
            "SELECT anon_1.addresses_id AS anon_1_addresses_id, "
            "anon_1.addresses_user_id AS anon_1_addresses_user_id, "
            "anon_1.addresses_email_address AS "
            "anon_1_addresses_email_address FROM (SELECT addresses.id AS "
            "addresses_id, addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address FROM "
            "addresses WHERE :param_1 = addresses.user_id UNION SELECT "
            "addresses.id AS addresses_id, addresses.user_id AS "
            "addresses_user_id, addresses.email_address "
            "AS addresses_email_address "
            "FROM addresses WHERE :param_2 = addresses.user_id) AS anon_1",
            checkparams={'param_1': 7, 'param_2': 8},
        )

    def test_unique_binds_or(self):
        User, Address = self.classes.User, self.classes.Address

        sess = Session()
        u1, u2 = sess.query(User).order_by(User.id)[0:2]

        self.assert_compile(
            sess.query(Address).filter(
                or_(with_parent(u1, 'addresses'), with_parent(u2, 'addresses'))
            ),
            "SELECT addresses.id AS addresses_id, addresses.user_id AS "
            "addresses_user_id, addresses.email_address AS "
            "addresses_email_address FROM addresses WHERE "
            ":param_1 = addresses.user_id OR :param_2 = addresses.user_id",
            checkparams={'param_1': 7, 'param_2': 8},
        )


class WithTransientOnNone(_fixtures.FixtureTest, AssertsCompiledSQL):
    run_inserts = None
    __dialect__ = 'default'

    def _fixture1(self):
        User, Address = self.classes.User, self.classes.Address
        users, addresses = self.tables.users, self.tables.addresses

        mapper(User, users)
        mapper(Address, addresses, properties={
            'user': relationship(User),
            'special_user': relationship(
                User, primaryjoin=and_(
                    users.c.id == addresses.c.user_id,
                    users.c.name == addresses.c.email_address))
        })

    def test_filter_with_transient_assume_pk(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        sess = Session()

        q = sess.query(Address).filter(Address.user == User())
        with expect_warnings("Got None for value of column "):
            self.assert_compile(
                q,
                "SELECT addresses.id AS addresses_id, "
                "addresses.user_id AS addresses_user_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id",
                checkparams={'param_1': None}
            )

    def test_filter_with_transient_warn_for_none_against_non_pk(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        s = Session()
        q = s.query(Address).filter(Address.special_user == User())
        with expect_warnings("Got None for value of column"):

            self.assert_compile(
                q,
                "SELECT addresses.id AS addresses_id, "
                "addresses.user_id AS addresses_user_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses WHERE :param_1 = addresses.user_id "
                "AND :param_2 = addresses.email_address",
                checkparams={"param_1": None, "param_2": None}
            )

    def test_with_parent_with_transient_assume_pk(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        sess = Session()

        q = sess.query(User).with_parent(Address(), "user")
        with expect_warnings("Got None for value of column"):
            self.assert_compile(
                q,
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id = :param_1",
                checkparams={'param_1': None}
            )

    def test_with_parent_with_transient_warn_for_none_against_non_pk(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        s = Session()
        q = s.query(User).with_parent(Address(), "special_user")
        with expect_warnings("Got None for value of column"):

            self.assert_compile(
                q,
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id = :param_1 "
                "AND users.name = :param_2",
                checkparams={"param_1": None, "param_2": None}
            )

    def test_negated_contains_or_equals_plain_m2o(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        s = Session()
        q = s.query(Address).filter(Address.user != User())
        with expect_warnings("Got None for value of column"):
            self.assert_compile(
                q,

                "SELECT addresses.id AS addresses_id, "
                "addresses.user_id AS addresses_user_id, "
                "addresses.email_address AS addresses_email_address "
                "FROM addresses "
                "WHERE addresses.user_id != :user_id_1 "
                "OR addresses.user_id IS NULL",
                checkparams={'user_id_1': None}
            )

    def test_negated_contains_or_equals_complex_rel(self):
        self._fixture1()
        User, Address = self.classes.User, self.classes.Address

        s = Session()

        # this one does *not* warn because we do the criteria
        # without deferral
        q = s.query(Address).filter(Address.special_user != User())
        self.assert_compile(
            q,
            "SELECT addresses.id AS addresses_id, "
            "addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM addresses "
            "WHERE NOT (EXISTS (SELECT 1 "
            "FROM users "
            "WHERE users.id = addresses.user_id AND "
            "users.name = addresses.email_address AND users.id IS NULL))",
            checkparams={}
        )


class SynonymTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    @classmethod
    def setup_mappers(cls):
        users, Keyword, items, order_items, orders, Item, User, \
            Address, keywords, Order, item_keywords, addresses = \
            cls.tables.users, cls.classes.Keyword, cls.tables.items, \
            cls.tables.order_items, cls.tables.orders, \
            cls.classes.Item, cls.classes.User, cls.classes.Address, \
            cls.tables.keywords, cls.classes.Order, \
            cls.tables.item_keywords, cls.tables.addresses

        mapper(User, users, properties={
            'name_syn': synonym('name'),
            'addresses': relationship(Address),
            'orders': relationship(
                Order, backref='user', order_by=orders.c.id),  # o2m, m2o
            'orders_syn': synonym('orders'),
            'orders_syn_2': synonym('orders_syn')
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items': relationship(Item, secondary=order_items),  # m2m
            'address': relationship(Address),  # m2o
            'items_syn': synonym('items')
        })
        mapper(Item, items, properties={
            'keywords': relationship(Keyword, secondary=item_keywords)  # m2m
        })
        mapper(Keyword, keywords)

    def test_options(self):
        User, Order = self.classes.User, self.classes.Order

        s = create_session()

        def go():
            result = s.query(User).filter_by(name='jack').\
                options(joinedload(User.orders_syn)).all()
            eq_(result, [
                User(id=7, name='jack', orders=[
                    Order(description='order 1'),
                    Order(description='order 3'),
                    Order(description='order 5')
                ])
            ])
        self.assert_sql_count(testing.db, go, 1)

    def test_options_syn_of_syn(self):
        User, Order = self.classes.User, self.classes.Order

        s = create_session()

        def go():
            result = s.query(User).filter_by(name='jack').\
                options(joinedload(User.orders_syn_2)).all()
            eq_(result, [
                User(id=7, name='jack', orders=[
                    Order(description='order 1'),
                    Order(description='order 3'),
                    Order(description='order 5')
                ])
            ])
        self.assert_sql_count(testing.db, go, 1)

    def test_options_syn_of_syn_string(self):
        User, Order = self.classes.User, self.classes.Order

        s = create_session()

        def go():
            result = s.query(User).filter_by(name='jack').\
                options(joinedload('orders_syn_2')).all()
            eq_(result, [
                User(id=7, name='jack', orders=[
                    Order(description='order 1'),
                    Order(description='order 3'),
                    Order(description='order 5')
                ])
            ])
        self.assert_sql_count(testing.db, go, 1)

    def test_joins(self):
        User, Order = self.classes.User, self.classes.Order

        for j in (
            ['orders', 'items'],
            ['orders_syn', 'items'],
            [User.orders_syn, Order.items],
            ['orders_syn_2', 'items'],
            [User.orders_syn_2, 'items'],
            ['orders', 'items_syn'],
            ['orders_syn', 'items_syn'],
            ['orders_syn_2', 'items_syn'],
        ):
            result = create_session().query(User).join(*j).filter_by(id=3). \
                all()
            assert [User(id=7, name='jack'), User(id=9, name='fred')] == result

    def test_with_parent(self):
        Order, User = self.classes.Order, self.classes.User

        for nameprop, orderprop in (
            ('name', 'orders'),
            ('name_syn', 'orders'),
            ('name', 'orders_syn'),
            ('name', 'orders_syn_2'),
            ('name_syn', 'orders_syn'),
            ('name_syn', 'orders_syn_2'),
        ):
            sess = create_session()
            q = sess.query(User)

            u1 = q.filter_by(**{nameprop: 'jack'}).one()

            o = sess.query(Order).with_parent(u1, property=orderprop).all()
            assert [
                Order(description="order 1"), Order(description="order 3"),
                Order(description="order 5")] == o

    def test_froms_aliased_col(self):
        Address, User = self.classes.Address, self.classes.User

        sess = create_session()
        ua = aliased(User)

        q = sess.query(ua.name_syn).join(
            Address, ua.id == Address.user_id)
        self.assert_compile(
            q,
            "SELECT users_1.name AS users_1_name FROM "
            "users AS users_1 JOIN addresses ON users_1.id = addresses.user_id"
        )


class ImmediateTest(_fixtures.FixtureTest):
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        Address, addresses, users, User = (cls.classes.Address,
                                           cls.tables.addresses,
                                           cls.tables.users,
                                           cls.classes.User)

        mapper(Address, addresses)

        mapper(User, users, properties=dict(
            addresses=relationship(Address)))

    def test_one(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()

        assert_raises_message(
            sa.orm.exc.NoResultFound,
            r"No row was found for one\(\)",
            sess.query(User).filter(User.id == 99).one)

        eq_(sess.query(User).filter(User.id == 7).one().id, 7)

        assert_raises_message(
            sa.orm.exc.MultipleResultsFound,
            r"Multiple rows were found for one\(\)",
            sess.query(User).one)

        assert_raises(
            sa.orm.exc.NoResultFound,
            sess.query(User.id, User.name).filter(User.id == 99).one)

        eq_(sess.query(User.id, User.name).filter(User.id == 7).one(),
            (7, 'jack'))

        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User.id, User.name).one)

        assert_raises(
            sa.orm.exc.NoResultFound,
            (sess.query(User, Address).join(User.addresses).
             filter(Address.id == 99)).one)

        eq_((sess.query(User, Address).
             join(User.addresses).
             filter(Address.id == 4)).one(),
            (User(id=8), Address(id=4)))

        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User, Address).join(User.addresses).one)

        # this result returns multiple rows, the first
        # two rows being the same.  but uniquing is
        # not applied for a column based result.
        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User.id).join(User.addresses).
            filter(User.id.in_([8, 9])).order_by(User.id).one)

        # test that a join which ultimately returns
        # multiple identities across many rows still
        # raises, even though the first two rows are of
        # the same identity and unique filtering
        # is applied ([ticket:1688])
        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User).join(User.addresses).filter(User.id.in_([8, 9])).
            order_by(User.id).one)

    def test_one_or_none(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()

        eq_(sess.query(User).filter(User.id == 99).one_or_none(), None)

        eq_(sess.query(User).filter(User.id == 7).one_or_none().id, 7)

        assert_raises_message(
            sa.orm.exc.MultipleResultsFound,
            r"Multiple rows were found for one_or_none\(\)",
            sess.query(User).one_or_none)

        eq_(sess.query(User.id, User.name).filter(User.id == 99).one_or_none(),
            None)

        eq_(sess.query(User.id, User.name).filter(User.id == 7).one_or_none(),
            (7, 'jack'))

        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User.id, User.name).one_or_none)

        eq_(
            (sess.query(User, Address).join(User.addresses).
             filter(Address.id == 99)).one_or_none(), None)

        eq_((sess.query(User, Address).
             join(User.addresses).
             filter(Address.id == 4)).one_or_none(),
            (User(id=8), Address(id=4)))

        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User, Address).join(User.addresses).one_or_none)

        # this result returns multiple rows, the first
        # two rows being the same.  but uniquing is
        # not applied for a column based result.
        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User.id).join(User.addresses).
            filter(User.id.in_([8, 9])).order_by(User.id).one_or_none)

        # test that a join which ultimately returns
        # multiple identities across many rows still
        # raises, even though the first two rows are of
        # the same identity and unique filtering
        # is applied ([ticket:1688])
        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User).join(User.addresses).filter(User.id.in_([8, 9])).
            order_by(User.id).one_or_none)

    @testing.future
    def test_getslice(self):
        assert False

    def test_scalar(self):
        User = self.classes.User

        sess = create_session()

        eq_(sess.query(User.id).filter_by(id=7).scalar(), 7)
        eq_(sess.query(User.id, User.name).filter_by(id=7).scalar(), 7)
        eq_(sess.query(User.id).filter_by(id=0).scalar(), None)
        eq_(sess.query(User).filter_by(id=7).scalar(),
            sess.query(User).filter_by(id=7).one())

        assert_raises(sa.orm.exc.MultipleResultsFound, sess.query(User).scalar)
        assert_raises(
            sa.orm.exc.MultipleResultsFound,
            sess.query(User.id, User.name).scalar)

    def test_value(self):
        User = self.classes.User

        sess = create_session()

        eq_(sess.query(User).filter_by(id=7).value(User.id), 7)
        eq_(sess.query(User.id, User.name).filter_by(id=7).value(User.id), 7)
        eq_(sess.query(User).filter_by(id=0).value(User.id), None)

        sess.bind = testing.db
        eq_(sess.query().value(sa.literal_column('1').label('x')), 1)


class ExecutionOptionsTest(QueryTest):

    def test_option_building(self):
        User = self.classes.User

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

    def test_options_in_connection(self):
        User = self.classes.User

        execution_options = dict(foo='bar', stream_results=True)

        class TQuery(Query):
            def instances(self, result, ctx):
                try:
                    eq_(
                        result.connection._execution_options,
                        execution_options)
                finally:
                    result.close()
                return iter([])

        sess = create_session(
            bind=testing.db, autocommit=False, query_cls=TQuery)
        q1 = sess.query(User).execution_options(**execution_options)
        q1.all()


class BooleanEvalTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    """test standalone booleans being wrapped in an AsBoolean, as well
    as true/false compilation."""

    def _dialect(self, native_boolean):
        d = default.DefaultDialect()
        d.supports_native_boolean = native_boolean
        return d

    def test_one(self):
        s = Session()
        c = column('x', Boolean)
        self.assert_compile(
            s.query(c).filter(c),
            "SELECT x WHERE x",
            dialect=self._dialect(True)
        )

    def test_two(self):
        s = Session()
        c = column('x', Boolean)
        self.assert_compile(
            s.query(c).filter(c),
            "SELECT x WHERE x = 1",
            dialect=self._dialect(False)
        )

    def test_three(self):
        s = Session()
        c = column('x', Boolean)
        self.assert_compile(
            s.query(c).filter(~c),
            "SELECT x WHERE x = 0",
            dialect=self._dialect(False)
        )

    def test_four(self):
        s = Session()
        c = column('x', Boolean)
        self.assert_compile(
            s.query(c).filter(~c),
            "SELECT x WHERE NOT x",
            dialect=self._dialect(True)
        )

    def test_five(self):
        s = Session()
        c = column('x', Boolean)
        self.assert_compile(
            s.query(c).having(c),
            "SELECT x HAVING x = 1",
            dialect=self._dialect(False)
        )


class SessionBindTest(QueryTest):

    @contextlib.contextmanager
    def _assert_bind_args(self, session):
        get_bind = mock.Mock(side_effect=session.get_bind)
        with mock.patch.object(session, "get_bind", get_bind):
            yield
        for call_ in get_bind.mock_calls:
            is_(call_[1][0], inspect(self.classes.User))
            is_not_(call_[2]['clause'], None)

    def test_single_entity_q(self):
        User = self.classes.User
        session = Session()
        with self._assert_bind_args(session):
            session.query(User).all()

    def test_sql_expr_entity_q(self):
        User = self.classes.User
        session = Session()
        with self._assert_bind_args(session):
            session.query(User.id).all()

    def test_count(self):
        User = self.classes.User
        session = Session()
        with self._assert_bind_args(session):
            session.query(User).count()

    def test_aggregate_fn(self):
        User = self.classes.User
        session = Session()
        with self._assert_bind_args(session):
            session.query(func.max(User.name)).all()

    def test_bulk_update_no_sync(self):
        User = self.classes.User
        session = Session()
        with self._assert_bind_args(session):
            session.query(User).filter(User.id == 15).update(
                {"name": "foob"}, synchronize_session=False)

    def test_bulk_delete_no_sync(self):
        User = self.classes.User
        session = Session()
        with self._assert_bind_args(session):
            session.query(User).filter(User.id == 15).delete(
                synchronize_session=False)

    def test_bulk_update_fetch_sync(self):
        User = self.classes.User
        session = Session()
        with self._assert_bind_args(session):
            session.query(User).filter(User.id == 15).update(
                {"name": "foob"}, synchronize_session='fetch')

    def test_bulk_delete_fetch_sync(self):
        User = self.classes.User
        session = Session()
        with self._assert_bind_args(session):
            session.query(User).filter(User.id == 15).delete(
                synchronize_session='fetch')

    def test_column_property(self):
        User = self.classes.User

        mapper = inspect(User)
        mapper.add_property(
            "score",
            column_property(func.coalesce(self.tables.users.c.name, None)))
        session = Session()
        with self._assert_bind_args(session):
            session.query(func.max(User.score)).scalar()


    @testing.requires.nested_aggregates
    def test_column_property_select(self):
        User = self.classes.User
        Address = self.classes.Address

        mapper = inspect(User)
        mapper.add_property(
            "score",
            column_property(
                select([func.sum(Address.id)]).
                where(Address.user_id == User.id).as_scalar()
            )
        )
        session = Session()

        with self._assert_bind_args(session):
            session.query(func.max(User.score)).scalar()
