from sqlalchemy.orm import Session, subqueryload, \
    mapper, relationship, lazyload, clear_mappers
from sqlalchemy.testing import eq_, is_, is_not_
from sqlalchemy.testing import assert_raises, assert_raises_message
from sqlalchemy import testing
from test.orm import _fixtures
from sqlalchemy.ext.baked import BakedQuery, baked_lazyload, BakedLazyLoader
from sqlalchemy.ext import baked
from sqlalchemy import bindparam, func
from sqlalchemy.orm import exc as orm_exc
import itertools
from sqlalchemy.testing import mock


class BakedTest(_fixtures.FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    def setup(self):
        self.bakery = baked.bakery()


class StateChangeTest(BakedTest):
    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User

        mapper(User, cls.tables.users)

    def _assert_cache_key(self, key, elements):
        eq_(
            key,
            tuple(elem.__code__ for elem in elements)
        )

    def test_initial_key(self):
        User = self.classes.User
        session = Session()
        l1 = lambda: session.query(User)
        q1 = self.bakery(l1)
        self._assert_cache_key(
            q1._cache_key,
            [l1]
        )
        eq_(q1.steps, [l1])

    def test_inplace_add(self):
        User = self.classes.User
        session = Session()
        l1 = lambda: session.query(User)
        l2 = lambda q: q.filter(User.name == bindparam('name'))
        q1 = self.bakery(l1)
        self._assert_cache_key(
            q1._cache_key,
            [l1]
        )
        eq_(q1.steps, [l1])

        q2 = q1.add_criteria(l2)
        is_(q2, q1)

        self._assert_cache_key(
            q1._cache_key,
            [l1, l2]
        )
        eq_(q1.steps, [l1, l2])

    def test_inplace_add_operator(self):
        User = self.classes.User
        session = Session()
        l1 = lambda: session.query(User)
        l2 = lambda q: q.filter(User.name == bindparam('name'))
        q1 = self.bakery(l1)
        self._assert_cache_key(
            q1._cache_key,
            [l1]
        )

        q1 += l2

        self._assert_cache_key(
            q1._cache_key,
            [l1, l2]
        )

    def test_chained_add(self):
        User = self.classes.User
        session = Session()
        l1 = lambda: session.query(User)
        l2 = lambda q: q.filter(User.name == bindparam('name'))
        q1 = self.bakery(l1)

        q2 = q1.with_criteria(l2)
        is_not_(q2, q1)

        self._assert_cache_key(
            q1._cache_key,
            [l1]
        )
        self._assert_cache_key(
            q2._cache_key,
            [l1, l2]
        )

    def test_chained_add_operator(self):
        User = self.classes.User
        session = Session()
        l1 = lambda: session.query(User)
        l2 = lambda q: q.filter(User.name == bindparam('name'))
        q1 = self.bakery(l1)

        q2 = q1 + l2
        is_not_(q2, q1)

        self._assert_cache_key(
            q1._cache_key,
            [l1]
        )
        self._assert_cache_key(
            q2._cache_key,
            [l1, l2]
        )


class LikeQueryTest(BakedTest):
    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User

        mapper(User, cls.tables.users)

    def test_first_no_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name == 'asdf')

        eq_(
            bq(Session()).first(),
            None
        )

    def test_first_multiple_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User.id))
        bq += lambda q: q.filter(User.name.like('%ed%')).order_by(User.id)

        eq_(
            bq(Session()).first(),
            (8, )
        )

    def test_one_or_none_no_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name == 'asdf')

        eq_(
            bq(Session()).one_or_none(),
            None
        )

    def test_one_or_none_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name == 'ed')

        u1 = bq(Session()).one_or_none()
        eq_(u1.name, 'ed')

    def test_one_or_none_multiple_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name.like('%ed%'))

        assert_raises(
            orm_exc.MultipleResultsFound,
            bq(Session()).one_or_none
        )

    def test_one_no_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name == 'asdf')

        assert_raises_message(
            orm_exc.NoResultFound,
            "No row was found for one()",
            bq(Session()).one
        )

    def test_one_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name == 'ed')

        u1 = bq(Session()).one()
        eq_(u1.name, 'ed')

    def test_one_multiple_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name.like('%ed%'))

        assert_raises_message(
            orm_exc.MultipleResultsFound,
            "Multiple rows were found for one()",
            bq(Session()).one
        )

    def test_get(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))

        sess = Session()

        def go():
            u1 = bq(sess).get(7)
            eq_(u1.name, 'jack')
        self.assert_sql_count(testing.db, go, 1)

        u1 = sess.query(User).get(7)  # noqa

        def go():
            u2 = bq(sess).get(7)
            eq_(u2.name, 'jack')
        self.assert_sql_count(testing.db, go, 0)

        def go():
            u2 = bq(sess).get(8)
            eq_(u2.name, 'ed')
        self.assert_sql_count(testing.db, go, 1)

    def test_get_pk_w_null(self):
        """test the re-implementation of logic to do get with IS NULL."""

        class AddressUser(object):
            pass
        mapper(
            AddressUser,
            self.tables.users.outerjoin(self.tables.addresses),
            properties={
                "id": self.tables.users.c.id,
                "address_id": self.tables.addresses.c.id
            }
        )

        bq = self.bakery(lambda s: s.query(AddressUser))

        sess = Session()

        def go():
            u1 = bq(sess).get((10, None))
            eq_(u1.name, 'chuck')
        self.assert_sql_count(testing.db, go, 1)

        u1 = sess.query(AddressUser).get((10, None))  # noqa

        def go():
            u2 = bq(sess).get((10, None))
            eq_(u2.name, 'chuck')
        self.assert_sql_count(testing.db, go, 0)

    def test_get_includes_getclause(self):
        # test issue #3597
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))

        for i in range(5):
            sess = Session()
            u1 = bq(sess).get(7)
            eq_(u1.name, 'jack')
            sess.close()

        eq_(len(bq._bakery), 2)

        # simulate race where mapper._get_clause
        # may be generated more than once
        from sqlalchemy import inspect
        del inspect(User).__dict__['_get_clause']

        for i in range(5):
            sess = Session()
            u1 = bq(sess).get(7)
            eq_(u1.name, 'jack')
            sess.close()
        eq_(len(bq._bakery), 4)


class ResultTest(BakedTest):
    __backend__ = True

    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User
        Address = cls.classes.Address

        mapper(User, cls.tables.users, properties={
            "addresses": relationship(
                Address, order_by=cls.tables.addresses.c.id)
        })
        mapper(Address, cls.tables.addresses)

    def test_cachekeys_on_constructor(self):
        User = self.classes.User

        queue = [7, 8]
        fn = lambda s: s.query(User.id).filter_by(id=queue.pop(0))
        bq1 = self.bakery(fn, 7)
        bq2 = self.bakery(fn, 8)

        for i in range(3):
            session = Session(autocommit=True)
            eq_(
                bq1(session).all(),
                [(7,)]
            )

            eq_(
                bq2(session).all(),
                [(8,)]
            )

    def test_no_steps(self):
        User = self.classes.User

        bq = self.bakery(
            lambda s: s.query(User.id, User.name).order_by(User.id))

        for i in range(3):
            session = Session(autocommit=True)
            eq_(
                bq(session).all(),
                [(7, 'jack'), (8, 'ed'), (9, 'fred'), (10, 'chuck')]
            )

    def test_different_limits(self):
        User = self.classes.User

        bq = self.bakery(
            lambda s: s.query(User.id, User.name).order_by(User.id))

        bq += lambda q: q.limit(bindparam('limit')).offset(bindparam('offset'))
        session = Session(autocommit=True)

        for i in range(4):
            for limit, offset, exp in [
                (2, 1, [(8, 'ed'), (9, 'fred')]),
                (3, 0, [(7, 'jack'), (8, 'ed'), (9, 'fred')]),
                (1, 2, [(9, 'fred')])
            ]:
                eq_(
                    bq(session).params(limit=limit, offset=offset).all(),
                    exp
                )

    def test_spoiled_full_w_params(self):
        User = self.classes.User

        canary = mock.Mock()

        def fn1(s):
            canary.fn1()
            return s.query(User.id, User.name).order_by(User.id)

        def fn2(q):
            canary.fn2()
            return q.filter(User.id == bindparam('id'))

        def fn3(q):
            canary.fn3()
            return q

        for x in range(3):
            bq = self.bakery(fn1)

            bq += fn2

            sess = Session(autocommit=True)
            eq_(
                bq.spoil(full=True).add_criteria(fn3)(sess).params(id=7).all(),
                [(7, 'jack')]
            )

        eq_(
            canary.mock_calls,
            [mock.call.fn1(), mock.call.fn2(), mock.call.fn3(),
             mock.call.fn1(), mock.call.fn2(), mock.call.fn3(),
             mock.call.fn1(), mock.call.fn2(), mock.call.fn3()]
        )

    def test_spoiled_half_w_params(self):
        User = self.classes.User

        canary = mock.Mock()

        def fn1(s):
            canary.fn1()
            return s.query(User.id, User.name).order_by(User.id)

        def fn2(q):
            canary.fn2()
            return q.filter(User.id == bindparam('id'))

        def fn3(q):
            canary.fn3()
            return q

        bq = self.bakery(fn1)

        bq += fn2

        for x in range(3):
            bq = self.bakery(fn1)

            bq += fn2

            sess = Session(autocommit=True)
            eq_(
                bq.spoil().add_criteria(fn3)(sess).params(id=7).all(),
                [(7, 'jack')]
            )

        eq_(
            canary.mock_calls,
            [mock.call.fn1(), mock.call.fn2(),
             mock.call.fn3(), mock.call.fn3(), mock.call.fn3()]
        )

    def test_w_new_entities(self):
        """Test that the query can have its entities modified in
        an arbitrary callable, and that this new entity list is preserved
        when the query is invoked.

        """
        User = self.classes.User

        bq = self.bakery(
            lambda s: s.query(User.id, User.name))

        bq += lambda q: q.from_self().with_entities(
            func.count(User.id))

        for i in range(3):
            session = Session(autocommit=True)
            eq_(
                bq(session).all(),
                [(4, )]
            )

    def test_conditional_step(self):
        """Test a large series of conditionals and assert that
        results remain correct between all of them within a series
        of loops.

        """
        User = self.classes.User

        base_bq = self.bakery(
            lambda s: s.query(User.id, User.name))

        base_bq += lambda q: q.order_by(User.id)

        for i in range(4):
            for cond1, cond2, cond3, cond4 in itertools.product(
                    *[(False, True) for j in range(4)]):
                bq = base_bq._clone()
                if cond1:
                    bq += lambda q: q.filter(User.name != 'jack')
                    if cond2:
                        bq += lambda q: q.join(User.addresses)
                    else:
                        bq += lambda q: q.outerjoin(User.addresses)
                elif cond3:
                    bq += lambda q: q.filter(User.name.like('%ed%'))
                else:
                    bq += lambda q: q.filter(User.name == 'jack')

                if cond4:
                    bq += lambda q: q.from_self().with_entities(
                        func.count(User.id))
                sess = Session(autocommit=True)
                result = bq(sess).all()
                if cond4:
                    if cond1:
                        if cond2:
                            eq_(result, [(4,)])
                        else:
                            eq_(result, [(5,)])
                    elif cond3:
                        eq_(result, [(2,)])
                    else:
                        eq_(result, [(1,)])
                else:
                    if cond1:
                        if cond2:
                            eq_(
                                result,
                                [(8, 'ed'), (8, 'ed'), (8, 'ed'),
                                 (9, 'fred')]
                            )
                        else:
                            eq_(
                                result,
                                [(8, 'ed'), (8, 'ed'), (8, 'ed'),
                                 (9, 'fred'), (10, 'chuck')]
                            )
                    elif cond3:
                        eq_(result, [(8, 'ed'), (9, 'fred')])
                    else:
                        eq_(result, [(7, 'jack')])

                sess.close()

    def test_conditional_step_oneline(self):
        User = self.classes.User

        base_bq = self.bakery(
            lambda s: s.query(User.id, User.name))

        base_bq += lambda q: q.order_by(User.id)

        for i in range(4):
            for cond1 in (False, True):
                bq = base_bq._clone()

                # we were using (filename, firstlineno) as cache key,
                # which fails for this kind of thing!
                bq += (lambda q: q.filter(User.name != 'jack')) if cond1 else (lambda q: q.filter(User.name == 'jack'))  # noqa
                sess = Session(autocommit=True)
                result = bq(sess).all()

                if cond1:
                    eq_(result, [(8, u'ed'), (9, u'fred'), (10, u'chuck')])
                else:
                    eq_(result, [(7, 'jack')])

                sess.close()

    def test_subquery_eagerloading(self):
        User = self.classes.User
        Address = self.classes.Address

        base_bq = self.bakery(
            lambda s: s.query(User))

        base_bq += lambda q: q.options(subqueryload(User.addresses))
        base_bq += lambda q: q.order_by(User.id)

        assert_result = [
            User(id=7, addresses=[
                Address(id=1, email_address='jack@bean.com')]),
            User(id=8, addresses=[
                Address(id=2, email_address='ed@wood.com'),
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])
        ]

        for i in range(4):
            for cond1, cond2 in itertools.product(
                    *[(False, True) for j in range(2)]):
                bq = base_bq._clone()

                sess = Session()

                if cond1:
                    bq += lambda q: q.filter(User.name == 'jack')
                else:
                    bq += lambda q: q.filter(User.name.like('%ed%'))

                if cond2:
                    ct = func.count(Address.id).label('count')
                    subq = sess.query(
                        ct,
                        Address.user_id).group_by(Address.user_id).\
                        having(ct > 2).subquery()

                    bq += lambda q: q.join(subq)

                if cond2:
                    if cond1:
                        def go():
                            result = bq(sess).all()
                            eq_([], result)
                        self.assert_sql_count(testing.db, go, 1)
                    else:
                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[1:2], result)
                        self.assert_sql_count(testing.db, go, 2)
                else:
                    if cond1:
                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[0:1], result)
                        self.assert_sql_count(testing.db, go, 2)
                    else:
                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[1:3], result)
                        self.assert_sql_count(testing.db, go, 2)

                sess.close()


class LazyLoaderTest(BakedTest):
    run_setup_mappers = 'each'

    def _o2m_fixture(self, lazy="select", **kw):
        User = self.classes.User
        Address = self.classes.Address

        mapper(User, self.tables.users, properties={
            'addresses': relationship(
                Address, order_by=self.tables.addresses.c.id,
                lazy=lazy, **kw)
        })
        mapper(Address, self.tables.addresses)
        return User, Address

    def _m2o_fixture(self):
        User = self.classes.User
        Address = self.classes.Address

        mapper(User, self.tables.users)
        mapper(Address, self.tables.addresses, properties={
            'user': relationship(User)
        })
        return User, Address

    def test_strategy_lookup(self):
        """test that the lazy loader strategies aren't getting mixed up
        with BakedLazyLoader as a subclass.

        """
        User, Address = self._o2m_fixture()

        ll = User.addresses.property._get_strategy((('lazy', 'select'),))
        assert not isinstance(ll, BakedLazyLoader)
        eq_(ll._strategy_keys, [(('lazy', 'select'),), (('lazy', True),)])

        ll = User.addresses.property._get_strategy((('lazy', True),))
        assert not isinstance(ll, BakedLazyLoader)
        eq_(ll._strategy_keys, [(('lazy', 'select'),), (('lazy', True),)])

        bl = User.addresses.property._get_strategy((('lazy', 'baked_select'),))
        assert isinstance(bl, BakedLazyLoader)
        eq_(bl._strategy_keys, [(('lazy', 'baked_select'),)])

    def test_invocation_per_state(self):
        """test that BakedLazyLoader is getting invoked with the
        baked_lazyload() loader.

        """
        User, Address = self._o2m_fixture()

        sess = Session()
        q = sess.query(User)

        with mock.patch.object(BakedLazyLoader, "_emit_lazyload") as el:
            u1 = q.first()
            u1.addresses
            # not invoked
            eq_(el.mock_calls, [])

        sess = Session()
        q = sess.query(User).options(baked_lazyload(User.addresses))
        with mock.patch.object(BakedLazyLoader, "_emit_lazyload") as el:
            u1 = q.first()
            u1.addresses
            # invoked
            is_(
                el.mock_calls[0][1][1],
                u1._sa_instance_state
            )

    def test_invocation_per_mapper(self):
        """test that BakedLazyLoader is getting invoked with the
        "baked_select" lazy setting.

        """
        User, Address = self._o2m_fixture(lazy="baked_select")

        sess = Session()
        q = sess.query(User).options(lazyload(User.addresses))

        with mock.patch.object(BakedLazyLoader, "_emit_lazyload") as el:
            u1 = q.first()
            u1.addresses
            # not invoked
            eq_(el.mock_calls, [])

        sess = Session()
        q = sess.query(User)
        with mock.patch.object(BakedLazyLoader, "_emit_lazyload") as el:
            u1 = q.first()
            u1.addresses
            # invoked
            is_(
                el.mock_calls[0][1][1],
                u1._sa_instance_state
            )

    def test_systemwide_loaders_loadable_via_lazyloader(self):
        from sqlalchemy.orm import configure_mappers
        from sqlalchemy.orm.strategies import LazyLoader

        baked.bake_lazy_loaders()
        try:
            User, Address = self._o2m_fixture(lazy='joined')

            configure_mappers()

            is_(
                User.addresses.property.
                _get_strategy_by_cls(LazyLoader).__class__,
                BakedLazyLoader
            )
        finally:
            baked.unbake_lazy_loaders()

    def test_invocation_systemwide_loaders(self):
        baked.bake_lazy_loaders()
        try:
            User, Address = self._o2m_fixture()

            sess = Session()
            q = sess.query(User).options(lazyload(User.addresses))
            with mock.patch.object(BakedLazyLoader, "_emit_lazyload") as el:
                u1 = q.first()
                u1.addresses
                # invoked
                is_(
                    el.mock_calls[0][1][1],
                    u1._sa_instance_state
                )
        finally:
            baked.unbake_lazy_loaders()

        clear_mappers()
        User, Address = self._o2m_fixture()
        sess = Session()
        q = sess.query(User).options(lazyload(User.addresses))

        with mock.patch.object(BakedLazyLoader, "_emit_lazyload") as el:
            u1 = q.first()
            u1.addresses
            # not invoked
            eq_(el.mock_calls, [])

    def test_baked_lazy_loading_relationship_flag_true(self):
        self._test_baked_lazy_loading_relationship_flag(True)

    def test_baked_lazy_loading_relationship_flag_false(self):
        self._test_baked_lazy_loading_relationship_flag(False)

    def _test_baked_lazy_loading_relationship_flag(self, flag):
        baked.bake_lazy_loaders()
        try:
            User, Address = self._o2m_fixture(bake_queries=flag)

            sess = Session()
            u1 = sess.query(User).first()

            from sqlalchemy.orm import Query

            canary = mock.Mock()

            # I would think Mock can do this but apparently
            # it cannot (wrap / autospec don't work together)
            real_compile_context = Query._compile_context

            def _my_compile_context(*arg, **kw):
                if arg[0].column_descriptions[0]['entity'] is Address:
                    canary()
                return real_compile_context(*arg, **kw)

            with mock.patch.object(
                Query,
                "_compile_context",
                _my_compile_context
            ):
                u1.addresses

                sess.expire(u1)
                u1.addresses
        finally:
            baked.unbake_lazy_loaders()

        if flag:
            eq_(canary.call_count, 1)
        else:
            eq_(canary.call_count, 2)

    def test_baked_lazy_loading_option_o2m(self):
        User, Address = self._o2m_fixture()
        self._test_baked_lazy_loading(set_option=True)

    def test_baked_lazy_loading_mapped_o2m(self):
        User, Address = self._o2m_fixture(lazy="baked_select")
        self._test_baked_lazy_loading(set_option=False)

    def _test_baked_lazy_loading(self, set_option):
        User, Address = self.classes.User, self.classes.Address

        base_bq = self.bakery(
            lambda s: s.query(User))

        if set_option:
            base_bq += lambda q: q.options(baked_lazyload(User.addresses))

        base_bq += lambda q: q.order_by(User.id)

        assert_result = self.static.user_address_result

        for i in range(4):
            for cond1, cond2 in itertools.product(
                    *[(False, True) for j in range(2)]):
                bq = base_bq._clone()

                sess = Session()

                if cond1:
                    bq += lambda q: q.filter(User.name == 'jack')
                else:
                    bq += lambda q: q.filter(User.name.like('%ed%'))

                if cond2:
                    ct = func.count(Address.id).label('count')
                    subq = sess.query(
                        ct,
                        Address.user_id).group_by(Address.user_id).\
                        having(ct > 2).subquery()

                    bq += lambda q: q.join(subq)

                if cond2:
                    if cond1:
                        def go():
                            result = bq(sess).all()
                            eq_([], result)
                        self.assert_sql_count(testing.db, go, 1)
                    else:
                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[1:2], result)
                        self.assert_sql_count(testing.db, go, 2)
                else:
                    if cond1:
                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[0:1], result)
                        self.assert_sql_count(testing.db, go, 2)
                    else:
                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[1:3], result)
                        self.assert_sql_count(testing.db, go, 3)

                sess.close()

    def test_baked_lazy_loading_m2o(self):
        User, Address = self._m2o_fixture()

        base_bq = self.bakery(
            lambda s: s.query(Address))

        base_bq += lambda q: q.options(baked_lazyload(Address.user))
        base_bq += lambda q: q.order_by(Address.id)

        assert_result = self.static.address_user_result

        for i in range(4):
            for cond1 in (False, True):
                bq = base_bq._clone()

                sess = Session()

                if cond1:
                    bq += lambda q: q.filter(
                        Address.email_address == 'jack@bean.com')
                else:
                    bq += lambda q: q.filter(
                        Address.email_address.like('ed@%'))

                if cond1:
                    def go():
                        result = bq(sess).all()
                        eq_(assert_result[0:1], result)
                    self.assert_sql_count(testing.db, go, 2)
                else:
                    def go():
                        result = bq(sess).all()
                        eq_(assert_result[1:4], result)
                    self.assert_sql_count(testing.db, go, 2)

                sess.close()

    # additional tests:
    # 1. m2m w lazyload
    # 2. o2m lazyload where m2o backrefs have an eager load, test
    # that eager load is canceled out
    # 3. uselist = False, uselist=False assertion

