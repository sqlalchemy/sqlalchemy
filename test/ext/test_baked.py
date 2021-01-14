import contextlib
import itertools

from sqlalchemy import bindparam
from sqlalchemy import event
from sqlalchemy import exc as sa_exc
from sqlalchemy import func
from sqlalchemy import testing
from sqlalchemy.ext import baked
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm.query import Query
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_not
from sqlalchemy.testing import mock
from sqlalchemy.testing.fixtures import fixture_session
from test.orm import _fixtures


class BakedTest(_fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    def setup_test(self):
        self.bakery = baked.bakery()


class StateChangeTest(BakedTest):
    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User

        mapper(User, cls.tables.users)

    def _assert_cache_key(self, key, elements):
        eq_(key, tuple(elem.__code__ for elem in elements))

    def test_initial_key(self):
        User = self.classes.User
        session = fixture_session()

        def l1():
            return session.query(User)

        q1 = self.bakery(l1)
        self._assert_cache_key(q1._cache_key, [l1])
        eq_(q1.steps, [l1])

    def test_inplace_add(self):
        User = self.classes.User
        session = fixture_session()

        def l1():
            return session.query(User)

        def l2(q):
            return q.filter(User.name == bindparam("name"))

        q1 = self.bakery(l1)
        self._assert_cache_key(q1._cache_key, [l1])
        eq_(q1.steps, [l1])

        q2 = q1.add_criteria(l2)
        is_(q2, q1)

        self._assert_cache_key(q1._cache_key, [l1, l2])
        eq_(q1.steps, [l1, l2])

    def test_inplace_add_operator(self):
        User = self.classes.User
        session = fixture_session()

        def l1():
            return session.query(User)

        def l2(q):
            return q.filter(User.name == bindparam("name"))

        q1 = self.bakery(l1)
        self._assert_cache_key(q1._cache_key, [l1])

        q1 += l2

        self._assert_cache_key(q1._cache_key, [l1, l2])

    def test_chained_add(self):
        User = self.classes.User
        session = fixture_session()

        def l1():
            return session.query(User)

        def l2(q):
            return q.filter(User.name == bindparam("name"))

        q1 = self.bakery(l1)

        q2 = q1.with_criteria(l2)
        is_not(q2, q1)

        self._assert_cache_key(q1._cache_key, [l1])
        self._assert_cache_key(q2._cache_key, [l1, l2])

    def test_chained_add_operator(self):
        User = self.classes.User
        session = fixture_session()

        def l1():
            return session.query(User)

        def l2(q):
            return q.filter(User.name == bindparam("name"))

        q1 = self.bakery(l1)

        q2 = q1 + l2
        is_not(q2, q1)

        self._assert_cache_key(q1._cache_key, [l1])
        self._assert_cache_key(q2._cache_key, [l1, l2])


class LikeQueryTest(BakedTest):
    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User

        mapper(User, cls.tables.users)

    def test_first_no_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name == "asdf")

        eq_(bq(fixture_session()).first(), None)

    def test_first_multiple_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User.id))
        bq += lambda q: q.filter(User.name.like("%ed%")).order_by(User.id)

        eq_(bq(fixture_session()).first(), (8,))

    def test_one_or_none_no_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name == "asdf")

        eq_(bq(fixture_session()).one_or_none(), None)

    def test_one_or_none_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name == "ed")

        u1 = bq(fixture_session()).one_or_none()
        eq_(u1.name, "ed")

    def test_one_or_none_multiple_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name.like("%ed%"))

        assert_raises_message(
            orm_exc.MultipleResultsFound,
            "Multiple rows were found when one or none was required",
            bq(fixture_session()).one_or_none,
        )

    def test_one_no_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name == "asdf")

        assert_raises_message(
            orm_exc.NoResultFound,
            "No row was found when one was required",
            bq(fixture_session()).one,
        )

    def test_one_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name == "ed")

        u1 = bq(fixture_session()).one()
        eq_(u1.name, "ed")

    def test_one_multiple_result(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name.like("%ed%"))

        assert_raises_message(
            orm_exc.MultipleResultsFound,
            "Multiple rows were found when exactly one was required",
            bq(fixture_session()).one,
        )

    def test_get(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))

        sess = fixture_session()

        def go():
            u1 = bq(sess).get(7)
            eq_(u1.name, "jack")

        self.assert_sql_count(testing.db, go, 1)

        u1 = sess.query(User).get(7)  # noqa

        def go():
            u2 = bq(sess).get(7)
            eq_(u2.name, "jack")

        self.assert_sql_count(testing.db, go, 0)

        def go():
            u2 = bq(sess).get(8)
            eq_(u2.name, "ed")

        self.assert_sql_count(testing.db, go, 1)

    def test_scalar(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User.id))

        sess = fixture_session()

        bq += lambda q: q.filter(User.id == 7)

        eq_(bq(sess).scalar(), 7)

    def test_count(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))

        sess = fixture_session()

        eq_(bq(sess).count(), 4)

        bq += lambda q: q.filter(User.id.in_([8, 9]))

        eq_(bq(sess).count(), 2)

        # original query still works
        eq_(
            set([(u.id, u.name) for u in bq(sess).all()]),
            set([(8, "ed"), (9, "fred")]),
        )

    def test_count_with_bindparams(self):
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))

        sess = fixture_session()

        eq_(bq(sess).count(), 4)

        bq += lambda q: q.filter(User.name == bindparam("uname"))
        # calling with *args
        eq_(bq(sess).params(uname="fred").count(), 1)
        # with multiple params, the **kwargs will be used
        bq += lambda q: q.filter(User.id == bindparam("anid"))
        eq_(bq(sess).params(uname="fred", anid=9).count(), 1)

        eq_(
            # wrong id, so 0 results:
            bq(sess).params(uname="fred", anid=8).count(),
            0,
        )

    def test_get_pk_w_null(self):
        """test the re-implementation of logic to do get with IS NULL."""

        class AddressUser(object):
            pass

        mapper(
            AddressUser,
            self.tables.users.outerjoin(self.tables.addresses),
            properties={
                "id": self.tables.users.c.id,
                "address_id": self.tables.addresses.c.id,
            },
        )

        bq = self.bakery(lambda s: s.query(AddressUser))

        sess = fixture_session()

        def go():
            u1 = bq(sess).get((10, None))
            eq_(u1.name, "chuck")

        self.assert_sql_count(testing.db, go, 1)

        u1 = sess.query(AddressUser).get((10, None))  # noqa

        def go():
            u2 = bq(sess).get((10, None))
            eq_(u2.name, "chuck")

        self.assert_sql_count(testing.db, go, 0)

    def test_get_includes_getclause(self):
        # test issue #3597
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User))

        for i in range(5):
            sess = fixture_session()
            u1 = bq(sess).get(7)
            eq_(u1.name, "jack")
            sess.close()

        eq_(len(bq._bakery), 2)

        # simulate race where mapper._get_clause
        # may be generated more than once
        from sqlalchemy import inspect

        del inspect(User).__dict__["_get_clause"]

        for i in range(5):
            sess = fixture_session()
            u1 = bq(sess).get(7)
            eq_(u1.name, "jack")
            sess.close()
        eq_(len(bq._bakery), 4)


class ResultPostCriteriaTest(BakedTest):
    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User
        Address = cls.classes.Address
        Order = cls.classes.Order

        mapper(
            User,
            cls.tables.users,
            properties={
                "addresses": relationship(
                    Address, order_by=cls.tables.addresses.c.id
                ),
                "orders": relationship(Order, order_by=cls.tables.orders.c.id),
            },
        )
        mapper(Address, cls.tables.addresses)
        mapper(Order, cls.tables.orders)

    @contextlib.contextmanager
    def _fixture(self):
        from sqlalchemy import event

        User = self.classes.User

        with testing.db.connect() as conn:

            @event.listens_for(conn, "before_execute")
            def before_execute(
                conn, clauseelement, multiparams, params, execution_options
            ):
                # execution options are kind of moving around a bit,
                # test both places
                assert (
                    "yes" in clauseelement._execution_options
                    or "yes" in execution_options
                )

            bq = self.bakery(lambda s: s.query(User.id).order_by(User.id))

            sess = Session(conn)

            yield sess, bq

    def test_first(self):
        with self._fixture() as (sess, bq):
            result = bq(sess).with_post_criteria(
                lambda q: q.execution_options(yes=True)
            )
            eq_(result.first(), (7,))

    def test_iter(self):
        with self._fixture() as (sess, bq):
            result = bq(sess).with_post_criteria(
                lambda q: q.execution_options(yes=True)
            )
            eq_(list(result)[0], (7,))

    def test_spoiled(self):
        with self._fixture() as (sess, bq):

            result = bq.spoil()(sess).with_post_criteria(
                lambda q: q.execution_options(yes=True)
            )

            eq_(list(result)[0], (7,))

    def test_get(self):
        User = self.classes.User
        with self._fixture() as (sess, bq):
            bq = self.bakery(lambda s: s.query(User))

            result = bq(sess).with_post_criteria(
                lambda q: q.execution_options(yes=True)
            )
            eq_(result.get(7), User(id=7))


class ResultTest(BakedTest):
    __backend__ = True

    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User
        Address = cls.classes.Address
        Order = cls.classes.Order

        mapper(
            User,
            cls.tables.users,
            properties={
                "addresses": relationship(
                    Address, order_by=cls.tables.addresses.c.id
                ),
                "orders": relationship(Order, order_by=cls.tables.orders.c.id),
            },
        )
        mapper(Address, cls.tables.addresses)
        mapper(Order, cls.tables.orders)

    def test_cachekeys_on_constructor(self):
        User = self.classes.User

        queue = [7, 8]

        def fn(s):
            return s.query(User.id).filter_by(id=queue.pop(0))

        bq1 = self.bakery(fn, 7)
        bq2 = self.bakery(fn, 8)

        for i in range(3):
            session = fixture_session()
            eq_(bq1(session).all(), [(7,)])

            eq_(bq2(session).all(), [(8,)])

    def test_no_steps(self):
        User = self.classes.User

        bq = self.bakery(
            lambda s: s.query(User.id, User.name).order_by(User.id)
        )

        for i in range(3):
            session = fixture_session()
            eq_(
                bq(session).all(),
                [(7, "jack"), (8, "ed"), (9, "fred"), (10, "chuck")],
            )

    def test_different_limits(self):
        User = self.classes.User

        bq = self.bakery(
            lambda s: s.query(User.id, User.name).order_by(User.id)
        )

        bq += lambda q: q.limit(bindparam("limit")).offset(bindparam("offset"))
        session = fixture_session()

        for i in range(4):
            for limit, offset, exp in [
                (2, 1, [(8, "ed"), (9, "fred")]),
                (3, 0, [(7, "jack"), (8, "ed"), (9, "fred")]),
                (1, 2, [(9, "fred")]),
            ]:
                eq_(bq(session).params(limit=limit, offset=offset).all(), exp)

    def test_disable_on_session(self):
        User = self.classes.User

        canary = mock.Mock()

        def fn1(s):
            canary.fn1()
            return s.query(User.id, User.name).order_by(User.id)

        def fn2(q):
            canary.fn2()
            return q.filter(User.id == bindparam("id"))

        def fn3(q):
            canary.fn3()
            return q

        for x in range(3):
            bq = self.bakery(fn1)

            bq += fn2

            sess = fixture_session(autocommit=True, enable_baked_queries=False)
            eq_(bq.add_criteria(fn3)(sess).params(id=7).all(), [(7, "jack")])

        eq_(
            canary.mock_calls,
            [
                mock.call.fn1(),
                mock.call.fn2(),
                mock.call.fn3(),
                mock.call.fn1(),
                mock.call.fn2(),
                mock.call.fn3(),
                mock.call.fn1(),
                mock.call.fn2(),
                mock.call.fn3(),
            ],
        )

    def test_spoiled_full_w_params(self):
        User = self.classes.User

        canary = mock.Mock()

        def fn1(s):
            canary.fn1()
            return s.query(User.id, User.name).order_by(User.id)

        def fn2(q):
            canary.fn2()
            return q.filter(User.id == bindparam("id"))

        def fn3(q):
            canary.fn3()
            return q

        for x in range(3):
            bq = self.bakery(fn1)

            bq += fn2

            sess = fixture_session()
            eq_(
                bq.spoil(full=True).add_criteria(fn3)(sess).params(id=7).all(),
                [(7, "jack")],
            )

        eq_(
            canary.mock_calls,
            [
                mock.call.fn1(),
                mock.call.fn2(),
                mock.call.fn3(),
                mock.call.fn1(),
                mock.call.fn2(),
                mock.call.fn3(),
                mock.call.fn1(),
                mock.call.fn2(),
                mock.call.fn3(),
            ],
        )

    def test_spoiled_half_w_params(self):
        User = self.classes.User

        canary = mock.Mock()

        def fn1(s):
            canary.fn1()
            return s.query(User.id, User.name).order_by(User.id)

        def fn2(q):
            canary.fn2()
            return q.filter(User.id == bindparam("id"))

        def fn3(q):
            canary.fn3()
            return q

        bq = self.bakery(fn1)

        bq += fn2

        for x in range(3):
            bq = self.bakery(fn1)

            bq += fn2

            sess = fixture_session()
            eq_(
                bq.spoil().add_criteria(fn3)(sess).params(id=7).all(),
                [(7, "jack")],
            )

        eq_(
            canary.mock_calls,
            [
                mock.call.fn1(),
                mock.call.fn2(),
                mock.call.fn3(),
                mock.call.fn3(),
                mock.call.fn3(),
            ],
        )

    def test_w_new_entities(self):
        """Test that the query can have its entities modified in
        an arbitrary callable, and that this new entity list is preserved
        when the query is invoked.

        """
        User = self.classes.User

        bq = self.bakery(lambda s: s.query(User.id, User.name))

        bq += lambda q: q._from_self().with_entities(func.count(User.id))

        for i in range(3):
            session = fixture_session()
            eq_(bq(session).all(), [(4,)])

    def test_conditional_step(self):
        """Test a large series of conditionals and assert that
        results remain correct between all of them within a series
        of loops.

        """
        User = self.classes.User

        base_bq = self.bakery(lambda s: s.query(User.id, User.name))

        base_bq += lambda q: q.order_by(User.id)

        for i in range(4):
            for cond1, cond2, cond3, cond4 in itertools.product(
                *[(False, True) for j in range(4)]
            ):
                bq = base_bq._clone()
                if cond1:
                    bq += lambda q: q.filter(User.name != "jack")
                    if cond2:
                        bq += lambda q: q.join(User.addresses)
                    else:
                        bq += lambda q: q.outerjoin(User.addresses)
                elif cond3:
                    bq += lambda q: q.filter(User.name.like("%ed%"))
                else:
                    bq += lambda q: q.filter(User.name == "jack")

                if cond4:
                    bq += lambda q: q._from_self().with_entities(
                        func.count(User.id)
                    )
                sess = fixture_session()
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
                                [(8, "ed"), (8, "ed"), (8, "ed"), (9, "fred")],
                            )
                        else:
                            eq_(
                                result,
                                [
                                    (8, "ed"),
                                    (8, "ed"),
                                    (8, "ed"),
                                    (9, "fred"),
                                    (10, "chuck"),
                                ],
                            )
                    elif cond3:
                        eq_(result, [(8, "ed"), (9, "fred")])
                    else:
                        eq_(result, [(7, "jack")])

                sess.close()

    def test_conditional_step_oneline(self):
        User = self.classes.User

        base_bq = self.bakery(lambda s: s.query(User.id, User.name))

        base_bq += lambda q: q.order_by(User.id)

        for i in range(4):
            for cond1 in (False, True):
                bq = base_bq._clone()

                # we were using (filename, firstlineno) as cache key,
                # which fails for this kind of thing!
                bq += (
                    (lambda q: q.filter(User.name != "jack"))
                    if cond1
                    else (lambda q: q.filter(User.name == "jack"))
                )  # noqa
                sess = fixture_session()
                result = bq(sess).all()

                if cond1:
                    eq_(result, [(8, u"ed"), (9, u"fred"), (10, u"chuck")])
                else:
                    eq_(result, [(7, "jack")])

                sess.close()

    def test_to_query_query(self):
        User = self.classes.User
        Address = self.classes.Address

        sub_bq = self.bakery(lambda s: s.query(User.name))
        sub_bq += (
            lambda q: q.filter(User.id == Address.user_id)
            .filter(User.name == "ed")
            .correlate(Address)
        )

        main_bq = self.bakery(lambda s: s.query(Address.id))
        main_bq += lambda q: q.filter(sub_bq.to_query(q).exists())
        main_bq += lambda q: q.order_by(Address.id)

        sess = fixture_session()
        result = main_bq(sess).all()
        eq_(result, [(2,), (3,), (4,)])

    def test_to_query_session(self):
        User = self.classes.User
        Address = self.classes.Address

        sub_bq = self.bakery(lambda s: s.query(User.name))
        sub_bq += lambda q: q.filter(User.id == Address.user_id).correlate(
            Address
        )

        main_bq = self.bakery(
            lambda s: s.query(Address.id, sub_bq.to_query(s).scalar_subquery())
        )
        main_bq += lambda q: q.filter(
            sub_bq.to_query(q).scalar_subquery() == "ed"
        )
        main_bq += lambda q: q.order_by(Address.id)

        sess = fixture_session()
        result = main_bq(sess).all()
        eq_(result, [(2, "ed"), (3, "ed"), (4, "ed")])

    def test_to_query_args(self):
        User = self.classes.User
        sub_bq = self.bakery(lambda s: s.query(User.name))

        q = Query([], None)
        assert_raises_message(
            sa_exc.ArgumentError,
            "Given Query needs to be associated with a Session",
            sub_bq.to_query,
            q,
        )

        assert_raises_message(
            TypeError,
            "Query or Session object expected, got .*'int'.*",
            sub_bq.to_query,
            5,
        )

    def test_subquery_eagerloading(self):
        User = self.classes.User
        Address = self.classes.Address
        Order = self.classes.Order

        self.bakery = baked.bakery()
        base_bq = self.bakery(lambda s: s.query(User))

        base_bq += lambda q: q.options(
            subqueryload(User.addresses), subqueryload(User.orders)
        )
        base_bq += lambda q: q.order_by(User.id)

        assert_result = [
            User(
                id=7,
                addresses=[Address(id=1, email_address="jack@bean.com")],
                orders=[Order(id=1), Order(id=3), Order(id=5)],
            ),
            User(
                id=8,
                addresses=[
                    Address(id=2, email_address="ed@wood.com"),
                    Address(id=3, email_address="ed@bettyboop.com"),
                    Address(id=4, email_address="ed@lala.com"),
                ],
            ),
            User(
                id=9,
                addresses=[Address(id=5)],
                orders=[Order(id=2), Order(id=4)],
            ),
            User(id=10, addresses=[]),
        ]

        for i in range(4):
            for cond1, cond2 in itertools.product(
                *[(False, True) for j in range(2)]
            ):
                print("HI----")
                bq = base_bq._clone()

                sess = fixture_session()

                if cond1:
                    bq += lambda q: q.filter(User.name == "jack")
                else:
                    bq += lambda q: q.filter(User.name.like("%ed%"))

                if cond2:
                    ct = func.count(Address.id).label("count")
                    subq = (
                        sess.query(ct, Address.user_id)
                        .group_by(Address.user_id)
                        .having(ct > 2)
                        .subquery()
                    )

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

                        self.assert_sql_count(testing.db, go, 3)
                else:
                    if cond1:

                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[0:1], result)

                        self.assert_sql_count(testing.db, go, 3)
                    else:

                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[1:3], result)

                        self.assert_sql_count(testing.db, go, 3)

                sess.close()

    def test_subqueryload_post_context(self):
        User = self.classes.User
        Address = self.classes.Address

        assert_result = [
            User(
                id=7, addresses=[Address(id=1, email_address="jack@bean.com")]
            )
        ]

        self.bakery = baked.bakery()

        bq = self.bakery(lambda s: s.query(User))

        bq += lambda q: q.options(subqueryload(User.addresses))
        bq += lambda q: q.order_by(User.id)
        bq += lambda q: q.filter(User.name == bindparam("name"))
        sess = fixture_session()

        def set_params(q):
            return q.params(name="jack")

        # test that the changes we make using with_post_criteria()
        # are also applied to the subqueryload query.
        def go():
            result = bq(sess).with_post_criteria(set_params).all()
            eq_(assert_result, result)

        self.assert_sql_count(testing.db, go, 2)

    @testing.fixture()
    def before_compile_nobake_fixture(self):
        @event.listens_for(Query, "before_compile", retval=True)
        def _modify_query(query):
            query = query.enable_assertions(False)
            return query

        yield
        event.remove(Query, "before_compile", _modify_query)

    def test_subqueryload_post_context_w_cancelling_event(
        self, before_compile_nobake_fixture
    ):
        User = self.classes.User
        Address = self.classes.Address

        assert_result = [
            User(
                id=7, addresses=[Address(id=1, email_address="jack@bean.com")]
            )
        ]

        self.bakery = baked.bakery(size=3)

        bq = self.bakery(lambda s: s.query(User))

        bq += lambda q: q.options(subqueryload(User.addresses))
        bq += lambda q: q.order_by(User.id)
        bq += lambda q: q.filter(User.name == bindparam("name"))
        sess = fixture_session()

        def set_params(q):
            return q.params(name="jack")

        # test that the changes we make using with_post_criteria()
        # are also applied to the subqueryload query.
        def go():
            result = bq(sess).with_post_criteria(set_params).all()
            eq_(assert_result, result)

        self.assert_sql_count(testing.db, go, 2)


# assert that the integration style illustrated in the dogpile.cache
# example works w/ baked
class CustomIntegrationTest(testing.AssertsCompiledSQL, BakedTest):
    run_setup_mappers = "each"

    def _o2m_fixture(self, lazy="select", **kw):
        User = self.classes.User
        Address = self.classes.Address

        mapper(
            User,
            self.tables.users,
            properties={
                "addresses": relationship(
                    Address,
                    order_by=self.tables.addresses.c.id,
                    lazy=lazy,
                    **kw
                )
            },
        )
        mapper(Address, self.tables.addresses)
        return User, Address

    def _query_fixture(self):
        from sqlalchemy.orm.query import Query

        class CachingQuery(Query):
            cache = {}

            def set_cache_key(self, key):
                return self.execution_options(_cache_key=key)

            def set_cache_key_for_path(self, path, key):
                return self.execution_options(**{"_cache_key_%s" % path: key})

        def get_value(cache_key, cache, createfunc):
            if cache_key in cache:
                return cache[cache_key]()
            else:
                cache[cache_key] = retval = createfunc().freeze()
                return retval()

        s1 = fixture_session(query_cls=CachingQuery)

        @event.listens_for(s1, "do_orm_execute", retval=True)
        def do_orm_execute(orm_context):
            ckey = None
            for opt in orm_context.user_defined_options:
                ckey = opt.get_cache_key(orm_context)
                if ckey:
                    break
            else:
                if "_cache_key" in orm_context.execution_options:
                    ckey = orm_context.execution_options["_cache_key"]

            if ckey is not None:
                return get_value(
                    ckey,
                    CachingQuery.cache,
                    orm_context.invoke_statement,
                )

        return s1

    def _option_fixture(self):
        from sqlalchemy.orm.interfaces import UserDefinedOption

        class RelationshipCache(UserDefinedOption):

            propagate_to_loaders = True

            def get_cache_key(self, orm_context):
                if orm_context.loader_strategy_path:
                    return "user7_addresses"
                else:
                    return None

        return RelationshipCache()

    def test_non_baked(self):
        User, Address = self._o2m_fixture()

        sess = self._query_fixture()
        q = sess._query_cls
        eq_(q.cache, {})

        q = sess.query(User).filter(User.id == 7).set_cache_key("user7")

        eq_(q.all(), [User(id=7, addresses=[Address(id=1)])])

        eq_(list(q.cache), ["user7"])

        eq_(q.all(), [User(id=7, addresses=[Address(id=1)])])

    def test_non_baked_tuples(self):
        User, Address = self._o2m_fixture()

        sess = self._query_fixture()
        q = sess._query_cls
        eq_(q.cache, {})

        q = sess.query(User).filter(User.id == 7).set_cache_key("user7")

        eq_(
            sess.execute(q).all(),
            [(User(id=7, addresses=[Address(id=1)]),)],
        )

        eq_(list(q.cache), ["user7"])

        eq_(
            sess.execute(q).all(),
            [(User(id=7, addresses=[Address(id=1)]),)],
        )

    def test_use_w_baked(self):
        User, Address = self._o2m_fixture()

        sess = self._query_fixture()
        q = sess._query_cls
        eq_(q.cache, {})

        base_bq = self.bakery(lambda s: s.query(User))
        base_bq += lambda q: q.filter(User.id == 7)
        base_bq += lambda q: q.set_cache_key("user7")

        eq_(base_bq(sess).all(), [User(id=7, addresses=[Address(id=1)])])

        eq_(list(q.cache), ["user7"])

        eq_(base_bq(sess).all(), [User(id=7, addresses=[Address(id=1)])])

    def test_plain_w_baked_lazyload(self):
        User, Address = self._o2m_fixture()
        opt = self._option_fixture()

        sess = self._query_fixture()
        q = sess._query_cls
        eq_(q.cache, {})

        q = sess.query(User).filter(User.id == 7).options(opt)

        u = q.first()
        eq_(u.addresses, [Address(id=1)])

        eq_(list(q.cache), ["user7_addresses"])

        sess.close()

        # ensure caching logic works after query has been baked
        q.cache.clear()

        u = q.first()
        eq_(u.addresses, [Address(id=1)])

        eq_(list(q.cache), ["user7_addresses"])
