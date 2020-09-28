from sqlalchemy import exc
from sqlalchemy import testing
from sqlalchemy.engine import result
from sqlalchemy.engine.row import Row
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing.util import picklers


class ResultTupleTest(fixtures.TestBase):
    def _fixture(self, values, labels):
        return result.result_tuple(labels)(values)

    def test_empty(self):
        keyed_tuple = self._fixture([], [])
        eq_(str(keyed_tuple), "()")
        eq_(len(keyed_tuple), 0)

        eq_(list(keyed_tuple._mapping.keys()), [])
        eq_(keyed_tuple._fields, ())
        eq_(keyed_tuple._asdict(), {})

    def test_values_none_labels(self):
        keyed_tuple = self._fixture([1, 2], [None, None])
        eq_(str(keyed_tuple), "(1, 2)")
        eq_(len(keyed_tuple), 2)

        eq_(list(keyed_tuple._mapping.keys()), [])
        eq_(keyed_tuple._fields, ())
        eq_(keyed_tuple._asdict(), {})

        eq_(keyed_tuple[0], 1)
        eq_(keyed_tuple[1], 2)

    def test_creation(self):
        keyed_tuple = self._fixture([1, 2], ["a", "b"])
        eq_(str(keyed_tuple), "(1, 2)")
        eq_(list(keyed_tuple._mapping.keys()), ["a", "b"])
        eq_(keyed_tuple._fields, ("a", "b"))
        eq_(keyed_tuple._asdict(), {"a": 1, "b": 2})

    def test_index_access(self):
        keyed_tuple = self._fixture([1, 2], ["a", "b"])
        eq_(keyed_tuple[0], 1)
        eq_(keyed_tuple[1], 2)

        def should_raise():
            keyed_tuple[2]

        assert_raises(IndexError, should_raise)

    def test_negative_index_access(self):
        keyed_tuple = self._fixture([1, 2], ["a", "b"])
        eq_(keyed_tuple[-1], 2)
        eq_(keyed_tuple[-2:-1], (1,))

    def test_slice_access(self):
        keyed_tuple = self._fixture([1, 2], ["a", "b"])
        eq_(keyed_tuple[0:2], (1, 2))

    def test_slices_arent_in_mappings(self):
        keyed_tuple = self._fixture([1, 2], ["a", "b"])

        assert_raises(TypeError, lambda: keyed_tuple._mapping[0:2])

    def test_integers_arent_in_mappings(self):
        keyed_tuple = self._fixture([1, 2], ["a", "b"])

        assert_raises(KeyError, lambda: keyed_tuple._mapping[1])

    def test_getter(self):
        keyed_tuple = self._fixture([1, 2, 3], ["a", "b", "c"])

        getter = keyed_tuple._parent._getter("b")
        eq_(getter(keyed_tuple), 2)

        getter = keyed_tuple._parent._getter(2)
        eq_(getter(keyed_tuple), 3)

    def test_tuple_getter(self):
        keyed_tuple = self._fixture([1, 2, 3], ["a", "b", "c"])

        getter = keyed_tuple._parent._row_as_tuple_getter(["b", "c"])
        eq_(getter(keyed_tuple), (2, 3))

        # row as tuple getter doesn't accept ints.  for ints, just
        # use plain python
        import operator

        getter = operator.itemgetter(2, 0, 1)

        # getter = keyed_tuple._parent._row_as_tuple_getter([2, 0, 1])
        eq_(getter(keyed_tuple), (3, 1, 2))

    def test_attribute_access(self):
        keyed_tuple = self._fixture([1, 2], ["a", "b"])
        eq_(keyed_tuple.a, 1)
        eq_(keyed_tuple.b, 2)

        def should_raise():
            keyed_tuple.c

        assert_raises(AttributeError, should_raise)

    def test_contains(self):
        keyed_tuple = self._fixture(["x", "y"], ["a", "b"])

        is_true("x" in keyed_tuple)
        is_false("z" in keyed_tuple)

        is_true("z" not in keyed_tuple)
        is_false("x" not in keyed_tuple)

        # we don't do keys
        is_false("a" in keyed_tuple)
        is_false("z" in keyed_tuple)
        is_true("a" not in keyed_tuple)
        is_true("z" not in keyed_tuple)

    def test_contains_mapping(self):
        keyed_tuple = self._fixture(["x", "y"], ["a", "b"])._mapping

        is_false("x" in keyed_tuple)
        is_false("z" in keyed_tuple)

        is_true("z" not in keyed_tuple)
        is_true("x" not in keyed_tuple)

        # we do keys
        is_true("a" in keyed_tuple)
        is_true("b" in keyed_tuple)

    def test_none_label(self):
        keyed_tuple = self._fixture([1, 2, 3], ["a", None, "b"])
        eq_(str(keyed_tuple), "(1, 2, 3)")

        eq_(list(keyed_tuple._mapping.keys()), ["a", "b"])
        eq_(keyed_tuple._fields, ("a", "b"))
        eq_(keyed_tuple._asdict(), {"a": 1, "b": 3})

        # attribute access: can't get at value 2
        eq_(keyed_tuple.a, 1)
        eq_(keyed_tuple.b, 3)

        # index access: can get at value 2
        eq_(keyed_tuple[0], 1)
        eq_(keyed_tuple[1], 2)
        eq_(keyed_tuple[2], 3)

    def test_duplicate_labels(self):
        keyed_tuple = self._fixture([1, 2, 3], ["a", "b", "b"])
        eq_(str(keyed_tuple), "(1, 2, 3)")

        eq_(list(keyed_tuple._mapping.keys()), ["a", "b", "b"])
        eq_(keyed_tuple._fields, ("a", "b", "b"))
        eq_(keyed_tuple._asdict(), {"a": 1, "b": 3})

        # attribute access: can't get at value 2
        eq_(keyed_tuple.a, 1)
        eq_(keyed_tuple.b, 3)

        # index access: can get at value 2
        eq_(keyed_tuple[0], 1)
        eq_(keyed_tuple[1], 2)
        eq_(keyed_tuple[2], 3)

    def test_immutable(self):
        keyed_tuple = self._fixture([1, 2], ["a", "b"])
        eq_(str(keyed_tuple), "(1, 2)")

        eq_(keyed_tuple.a, 1)

        # eh
        # assert_raises(AttributeError, setattr, keyed_tuple, "a", 5)

        def should_raise():
            keyed_tuple[0] = 100

        assert_raises(TypeError, should_raise)

    def test_serialize(self):

        keyed_tuple = self._fixture([1, 2, 3], ["a", None, "b"])

        for loads, dumps in picklers():
            kt = loads(dumps(keyed_tuple))

            eq_(str(kt), "(1, 2, 3)")

            eq_(list(kt._mapping.keys()), ["a", "b"])
            eq_(kt._fields, ("a", "b"))
            eq_(kt._asdict(), {"a": 1, "b": 3})


class ResultTest(fixtures.TestBase):
    def _fixture(
        self,
        extras=None,
        alt_row=None,
        num_rows=None,
        default_filters=None,
        data=None,
    ):

        if data is None:
            data = [(1, 1, 1), (2, 1, 2), (1, 3, 2), (4, 1, 2)]
        if num_rows is not None:
            data = data[:num_rows]

        res = result.IteratorResult(
            result.SimpleResultMetaData(["a", "b", "c"], extra=extras),
            iter(data),
        )
        if default_filters:
            res._metadata._unique_filters = default_filters

        if alt_row:
            res._process_row = alt_row

        return res

    def test_class_presented(self):
        """To support different kinds of objects returned vs. rows,
        there are two wrapper classes for Result.
        """

        r1 = self._fixture()

        r2 = r1.columns(0, 1, 2)
        assert isinstance(r2, result.Result)

        m1 = r1.mappings()
        assert isinstance(m1, result.MappingResult)

        s1 = r1.scalars(1)
        assert isinstance(s1, result.ScalarResult)

    def test_mapping_plus_base(self):
        r1 = self._fixture()

        m1 = r1.mappings()
        eq_(m1.fetchone(), {"a": 1, "b": 1, "c": 1})
        eq_(r1.fetchone(), (2, 1, 2))

    def test_scalar_plus_base(self):
        r1 = self._fixture()

        m1 = r1.scalars()

        # base is not affected
        eq_(r1.fetchone(), (1, 1, 1))

        # scalars
        eq_(m1.first(), 2)

    def test_index_extra(self):
        ex1a, ex1b, ex2, ex3a, ex3b = (
            object(),
            object(),
            object(),
            object(),
            object(),
        )

        result = self._fixture(
            extras=[
                (ex1a, ex1b),
                (ex2,),
                (
                    ex3a,
                    ex3b,
                ),
            ]
        )
        eq_(
            result.columns(ex2, ex3b).columns(ex3a).all(),
            [(1,), (2,), (2,), (2,)],
        )

        result = self._fixture(
            extras=[
                (ex1a, ex1b),
                (ex2,),
                (
                    ex3a,
                    ex3b,
                ),
            ]
        )
        eq_([row._mapping[ex1b] for row in result], [1, 2, 1, 4])

        result = self._fixture(
            extras=[
                (ex1a, ex1b),
                (ex2,),
                (
                    ex3a,
                    ex3b,
                ),
            ]
        )
        eq_(
            [
                dict(r)
                for r in result.columns(ex2, ex3b).columns(ex3a).mappings()
            ],
            [{"c": 1}, {"c": 2}, {"c": 2}, {"c": 2}],
        )

    def test_unique_default_filters(self):
        result = self._fixture(
            default_filters=[lambda x: x < 4, lambda x: x, lambda x: True]
        )

        eq_(result.unique().all(), [(1, 1, 1), (1, 3, 2), (4, 1, 2)])

    def test_unique_default_filters_rearrange_scalar(self):
        result = self._fixture(
            default_filters=[lambda x: x < 4, lambda x: x, lambda x: True]
        )

        eq_(result.unique().scalars(1).all(), [1, 3])

    def test_unique_default_filters_rearrange_order(self):
        result = self._fixture(
            default_filters=[lambda x: x < 4, lambda x: x, lambda x: True]
        )

        eq_(
            result.unique().columns("b", "a", "c").all(),
            [(1, 1, 1), (3, 1, 2), (1, 4, 2)],
        )

    def test_unique_default_filters_rearrange_twice(self):
        # test that the default uniqueness filter is reconfigured
        # each time columns() is called
        result = self._fixture(
            default_filters=[lambda x: x < 4, lambda x: x, lambda x: True]
        )

        result = result.unique()

        # 1, 1, 1  ->   True, 1, True
        eq_(result.fetchone(), (1, 1, 1))

        # now rearrange for b, a, c
        # 1, 2, 2  ->   1, True, True
        # 3, 1, 2  ->   3, True, True
        result = result.columns("b", "a", "c")
        eq_(result.fetchone(), (3, 1, 2))

        # now rearrange for c, a
        # 2, 4  -> True, False
        result = result.columns("c", "a")
        eq_(result.fetchall(), [(2, 4)])

    def test_unique_scalars_all(self):
        result = self._fixture()

        eq_(result.unique().scalars(1).all(), [1, 3])

    def test_unique_mappings_all(self):
        result = self._fixture()

        def uniq(row):
            return row[0]

        eq_(
            result.unique(uniq).mappings().all(),
            [
                {"a": 1, "b": 1, "c": 1},
                {"a": 2, "b": 1, "c": 2},
                {"a": 4, "b": 1, "c": 2},
            ],
        )

    def test_unique_filtered_all(self):
        result = self._fixture()

        def uniq(row):
            return row[0]

        eq_(result.unique(uniq).all(), [(1, 1, 1), (2, 1, 2), (4, 1, 2)])

    def test_unique_scalars_many(self):
        result = self._fixture()

        result = result.unique().scalars(1)

        eq_(result.fetchmany(2), [1, 3])
        eq_(result.fetchmany(2), [])

    def test_unique_filtered_many(self):
        result = self._fixture()

        def uniq(row):
            return row[0]

        result = result.unique(uniq)

        eq_(result.fetchmany(2), [(1, 1, 1), (2, 1, 2)])
        eq_(result.fetchmany(2), [(4, 1, 2)])
        eq_(result.fetchmany(2), [])

    def test_unique_scalars_many_none(self):
        result = self._fixture()

        result = result.unique().scalars(1)

        # this assumes the default fetchmany behavior of all() for
        # the ListFetchStrategy
        eq_(result.fetchmany(None), [1, 3])
        eq_(result.fetchmany(None), [])

    def test_unique_scalars_iterate(self):
        result = self._fixture()

        result = result.unique().scalars(1)

        eq_(list(result), [1, 3])

    def test_unique_filtered_iterate(self):
        result = self._fixture()

        def uniq(row):
            return row[0]

        result = result.unique(uniq)

        eq_(list(result), [(1, 1, 1), (2, 1, 2), (4, 1, 2)])

    def test_all(self):
        result = self._fixture()

        eq_(result.all(), [(1, 1, 1), (2, 1, 2), (1, 3, 2), (4, 1, 2)])

        eq_(result.all(), [])

    def test_many_then_all(self):
        result = self._fixture()

        eq_(result.fetchmany(3), [(1, 1, 1), (2, 1, 2), (1, 3, 2)])
        eq_(result.all(), [(4, 1, 2)])

        eq_(result.all(), [])

    def test_scalars(self):
        result = self._fixture()

        eq_(list(result.scalars()), [1, 2, 1, 4])

        result = self._fixture()

        eq_(list(result.scalars(2)), [1, 2, 2, 2])

    def test_scalars_mappings(self):
        result = self._fixture()

        eq_(
            list(result.columns(0).mappings()),
            [{"a": 1}, {"a": 2}, {"a": 1}, {"a": 4}],
        )

    def test_scalars_no_fetchone(self):
        result = self._fixture()

        s = result.scalars()

        assert not hasattr(s, "fetchone")

        # original result is unchanged
        eq_(result.mappings().fetchone(), {"a": 1, "b": 1, "c": 1})

        # scalars
        eq_(s.all(), [2, 1, 4])

    def test_first(self):
        result = self._fixture()

        row = result.first()
        eq_(row, (1, 1, 1))

        eq_(result.all(), [])

    def test_one_unique(self):
        # assert that one() counts rows after uniqueness has been applied.
        # this would raise if we didnt have unique
        result = self._fixture(data=[(1, 1, 1), (1, 1, 1)])

        row = result.unique().one()
        eq_(row, (1, 1, 1))

    def test_one_unique_tricky_one(self):
        # one() needs to keep consuming rows in order to find a non-unique
        # one.  unique() really slows things down
        result = self._fixture(
            data=[(1, 1, 1), (1, 1, 1), (1, 1, 1), (2, 1, 1)]
        )

        assert_raises(exc.MultipleResultsFound, result.unique().one)

    def test_one_unique_mapping(self):
        # assert that one() counts rows after uniqueness has been applied.
        # this would raise if we didnt have unique
        result = self._fixture(data=[(1, 1, 1), (1, 1, 1)])

        row = result.mappings().unique().one()
        eq_(row, {"a": 1, "b": 1, "c": 1})

    def test_one_mapping(self):
        result = self._fixture(num_rows=1)

        row = result.mappings().one()
        eq_(row, {"a": 1, "b": 1, "c": 1})

    def test_one(self):
        result = self._fixture(num_rows=1)

        row = result.one()
        eq_(row, (1, 1, 1))

    def test_scalar_one(self):
        result = self._fixture(num_rows=1)

        row = result.scalar_one()
        eq_(row, 1)

    def test_scalars_plus_one(self):
        result = self._fixture(num_rows=1)

        row = result.scalars().one()
        eq_(row, 1)

    def test_scalars_plus_one_none(self):
        result = self._fixture(num_rows=0)

        result = result.scalars()
        assert_raises_message(
            exc.NoResultFound,
            "No row was found when one was required",
            result.one,
        )

    def test_one_none(self):
        result = self._fixture(num_rows=0)

        assert_raises_message(
            exc.NoResultFound,
            "No row was found when one was required",
            result.one,
        )

    def test_one_or_none(self):
        result = self._fixture(num_rows=1)

        eq_(result.one_or_none(), (1, 1, 1))

    def test_scalar_one_or_none(self):
        result = self._fixture(num_rows=1)

        eq_(result.scalar_one_or_none(), 1)

    def test_scalar_one_or_none_none(self):
        result = self._fixture(num_rows=0)

        eq_(result.scalar_one_or_none(), None)

    def test_one_or_none_none(self):
        result = self._fixture(num_rows=0)

        eq_(result.one_or_none(), None)

    def test_one_raise_mutiple(self):
        result = self._fixture(num_rows=2)

        assert_raises_message(
            exc.MultipleResultsFound,
            "Multiple rows were found when exactly one was required",
            result.one,
        )

    def test_one_or_none_raise_multiple(self):
        result = self._fixture(num_rows=2)

        assert_raises_message(
            exc.MultipleResultsFound,
            "Multiple rows were found when one or none was required",
            result.one_or_none,
        )

    def test_scalar(self):
        result = self._fixture()

        eq_(result.scalar(), 1)

        eq_(result.all(), [])

    def test_partition(self):
        result = self._fixture()

        r = []
        for partition in result.partitions(2):
            r.append(list(partition))
        eq_(r, [[(1, 1, 1), (2, 1, 2)], [(1, 3, 2), (4, 1, 2)]])

        eq_(result.all(), [])

    def test_partition_unique_yield_per(self):
        result = self._fixture()

        r = []
        for partition in result.unique().yield_per(2).partitions():
            r.append(list(partition))
        eq_(r, [[(1, 1, 1), (2, 1, 2)], [(1, 3, 2), (4, 1, 2)]])

        eq_(result.all(), [])

    def test_partition_yield_per(self):
        result = self._fixture()

        r = []
        for partition in result.yield_per(2).partitions():
            r.append(list(partition))
        eq_(r, [[(1, 1, 1), (2, 1, 2)], [(1, 3, 2), (4, 1, 2)]])

        eq_(result.all(), [])

    def test_columns(self):
        result = self._fixture()

        result = result.columns("b", "c")
        eq_(result.keys(), ["b", "c"])
        eq_(result.all(), [(1, 1), (1, 2), (3, 2), (1, 2)])

    def test_columns_ints(self):
        result = self._fixture()

        eq_(result.columns(1, -2).all(), [(1, 1), (1, 1), (3, 3), (1, 1)])

    def test_columns_again(self):
        result = self._fixture()

        eq_(
            result.columns("b", "c", "a").columns(1, 2).all(),
            [(1, 1), (2, 2), (2, 1), (2, 4)],
        )

    def test_mappings(self):
        result = self._fixture()

        eq_(
            [dict(r) for r in result.mappings()],
            [
                {"a": 1, "b": 1, "c": 1},
                {"a": 2, "b": 1, "c": 2},
                {"a": 1, "b": 3, "c": 2},
                {"a": 4, "b": 1, "c": 2},
            ],
        )

    def test_columns_with_mappings(self):
        result = self._fixture()
        eq_(
            [dict(r) for r in result.columns("b", "c").mappings().all()],
            [
                {"b": 1, "c": 1},
                {"b": 1, "c": 2},
                {"b": 3, "c": 2},
                {"b": 1, "c": 2},
            ],
        )

    def test_mappings_with_columns(self):
        result = self._fixture()

        m1 = result.mappings().columns("b", "c")

        eq_(m1.fetchmany(2), [{"b": 1, "c": 1}, {"b": 1, "c": 2}])

        # no slice here
        eq_(result.fetchone(), (1, 3, 2))

        # still slices
        eq_(m1.fetchone(), {"b": 1, "c": 2})

    def test_alt_row_fetch(self):
        class AppleRow(Row):
            def apple(self):
                return "apple"

        result = self._fixture(alt_row=AppleRow)

        row = result.all()[0]
        eq_(row.apple(), "apple")

    def test_alt_row_transform(self):
        class AppleRow(Row):
            def apple(self):
                return "apple"

        result = self._fixture(alt_row=AppleRow)

        row = result.columns("c", "a").all()[2]
        eq_(row.apple(), "apple")
        eq_(row, (2, 1))

    def test_scalar_none_iterate(self):
        result = self._fixture(
            data=[
                (1, None, 2),
                (3, 4, 5),
                (3, None, 5),
                (3, None, 5),
                (3, 4, 5),
            ]
        )

        result = result.scalars(1)
        eq_(list(result), [None, 4, None, None, 4])

    def test_scalar_none_many(self):
        result = self._fixture(
            data=[
                (1, None, 2),
                (3, 4, 5),
                (3, None, 5),
                (3, None, 5),
                (3, 4, 5),
            ]
        )

        result = result.scalars(1)

        eq_(result.fetchmany(3), [None, 4, None])
        eq_(result.fetchmany(5), [None, 4])

    def test_scalar_none_all(self):
        result = self._fixture(
            data=[
                (1, None, 2),
                (3, 4, 5),
                (3, None, 5),
                (3, None, 5),
                (3, 4, 5),
            ]
        )

        result = result.scalars(1)
        eq_(result.all(), [None, 4, None, None, 4])

    def test_scalar_none_all_unique(self):
        result = self._fixture(
            data=[
                (1, None, 2),
                (3, 4, 5),
                (3, None, 5),
                (3, None, 5),
                (3, 4, 5),
            ]
        )

        result = result.scalars(1).unique()
        eq_(result.all(), [None, 4])

    def test_scalar_only_on_filter(self):
        # test a mixture of the "real" result and the
        # scalar filter, where scalar has unique and real result does not.

        # this is new as of [ticket:5503] where we have created
        # ScalarResult / MappingResult "filters" that don't modify
        # the Result
        result = self._fixture(
            data=[
                (1, 1, 2),
                (3, 4, 5),
                (1, 1, 2),
                (3, None, 5),
                (3, 4, 5),
                (3, None, 5),
            ]
        )

        # result is non-unique.   u_s is unique on column 0
        u_s = result.scalars(0).unique()

        eq_(next(u_s), 1)  # unique value 1 from first row
        eq_(next(result), (3, 4, 5))  # second row
        eq_(next(u_s), 3)  # skip third row, return 3 for fourth row
        eq_(next(result), (3, 4, 5))  # non-unique fifth row
        eq_(u_s.all(), [])  # unique set is done because only 3 is left

    def test_scalar_none_one(self):
        result = self._fixture(data=[(1, None, 2)])

        result = result.scalars(1).unique()

        # one is returning None, see?
        eq_(result.one(), None)

    def test_scalar_none_one_or_none(self):
        result = self._fixture(data=[(1, None, 2)])

        result = result.scalars(1).unique()

        # the orm.Query can actually do this right now, so we sort of
        # have to allow for this unforuntately, unless we want to raise?
        eq_(result.one_or_none(), None)

    def test_scalar_none_first(self):
        result = self._fixture(data=[(1, None, 2)])

        result = result.scalars(1).unique()
        eq_(result.first(), None)

    def test_freeze(self):
        result = self._fixture()

        frozen = result.freeze()

        r1 = frozen()
        eq_(r1.fetchall(), [(1, 1, 1), (2, 1, 2), (1, 3, 2), (4, 1, 2)])
        eq_(r1.fetchall(), [])

        r2 = frozen()
        eq_(r1.fetchall(), [])
        eq_(r2.fetchall(), [(1, 1, 1), (2, 1, 2), (1, 3, 2), (4, 1, 2)])
        eq_(r2.fetchall(), [])

    def test_columns_unique_freeze(self):
        result = self._fixture()

        result = result.columns("b", "c").unique()

        frozen = result.freeze()

        r1 = frozen()
        eq_(r1.fetchall(), [(1, 1), (1, 2), (3, 2)])

    def test_columns_freeze(self):
        result = self._fixture()

        result = result.columns("b", "c")

        frozen = result.freeze()

        r1 = frozen()
        eq_(r1.fetchall(), [(1, 1), (1, 2), (3, 2), (1, 2)])

        r2 = frozen().unique()
        eq_(r2.fetchall(), [(1, 1), (1, 2), (3, 2)])

    def test_scalars_freeze(self):
        result = self._fixture()

        frozen = result.freeze()

        r1 = frozen()
        eq_(r1.scalars(1).fetchall(), [1, 1, 3, 1])

        r2 = frozen().scalars(1).unique()
        eq_(r2.fetchall(), [1, 3])


class MergeResultTest(fixtures.TestBase):
    @testing.fixture
    def merge_fixture(self):

        r1 = result.IteratorResult(
            result.SimpleResultMetaData(["user_id", "user_name"]),
            iter([(7, "u1"), (8, "u2")]),
        )
        r2 = result.IteratorResult(
            result.SimpleResultMetaData(["user_id", "user_name"]),
            iter([(9, "u3")]),
        )
        r3 = result.IteratorResult(
            result.SimpleResultMetaData(["user_id", "user_name"]),
            iter([(10, "u4"), (11, "u5")]),
        )
        r4 = result.IteratorResult(
            result.SimpleResultMetaData(["user_id", "user_name"]),
            iter([(12, "u6")]),
        )

        return r1, r2, r3, r4

    @testing.fixture
    def dupe_fixture(self):

        r1 = result.IteratorResult(
            result.SimpleResultMetaData(["x", "y", "z"]),
            iter([(1, 2, 1), (2, 2, 1)]),
        )
        r2 = result.IteratorResult(
            result.SimpleResultMetaData(["x", "y", "z"]),
            iter([(3, 1, 2), (3, 3, 3)]),
        )

        return r1, r2

    def test_merge_results(self, merge_fixture):
        r1, r2, r3, r4 = merge_fixture

        result = r1.merge(r2, r3, r4)

        eq_(result.keys(), ["user_id", "user_name"])
        row = result.fetchone()
        eq_(row, (7, "u1"))
        result.close()

    def test_fetchall(self, merge_fixture):
        r1, r2, r3, r4 = merge_fixture

        result = r1.merge(r2, r3, r4)
        eq_(
            result.fetchall(),
            [
                (7, "u1"),
                (8, "u2"),
                (9, "u3"),
                (10, "u4"),
                (11, "u5"),
                (12, "u6"),
            ],
        )

    def test_first(self, merge_fixture):
        r1, r2, r3, r4 = merge_fixture

        result = r1.merge(r2, r3, r4)
        eq_(
            result.first(),
            (7, "u1"),
        )

    def test_columns(self, merge_fixture):
        r1, r2, r3, r4 = merge_fixture

        result = r1.merge(r2, r3, r4)
        eq_(
            result.columns("user_name").fetchmany(4),
            [("u1",), ("u2",), ("u3",), ("u4",)],
        )
        result.close()

    def test_merge_scalars(self, merge_fixture):
        r1, r2, r3, r4 = merge_fixture

        for r in (r1, r2, r3, r4):
            r.scalars(0)

        result = r1.merge(r2, r3, r4)

        eq_(result.scalars(0).all(), [7, 8, 9, 10, 11, 12])

    def test_merge_unique(self, dupe_fixture):
        r1, r2 = dupe_fixture

        r1.scalars("y")
        r2.scalars("y")
        result = r1.merge(r2)

        # uniqued 2, 2, 1, 3
        eq_(result.scalars("y").unique().all(), [2, 1, 3])

    def test_merge_preserve_unique(self, dupe_fixture):
        r1, r2 = dupe_fixture

        r1.unique().scalars("y")
        r2.unique().scalars("y")
        result = r1.merge(r2)

        # unique takes place
        eq_(result.scalars("y").all(), [2, 1, 3])


class OnlyScalarsTest(fixtures.TestBase):
    """the chunkediterator supports "non tuple mode", where we bypass
    the expense of generating rows when we have only scalar values.

    """

    @testing.fixture
    def no_tuple_fixture(self):
        data = [(1, 1, 1), (2, 1, 2), (1, 1, 1), (1, 3, 2), (4, 1, 2)]

        def chunks(num):
            while data:
                rows = data[0:num]
                data[:] = []

                yield [row[0] for row in rows]

        return chunks

    @testing.fixture
    def no_tuple_one_fixture(self):
        data = [(1, 1, 1)]

        def chunks(num):
            while data:
                rows = data[0:num]
                data[:] = []

                yield [row[0] for row in rows]

        return chunks

    @testing.fixture
    def normal_fixture(self):
        data = [(1, 1, 1), (2, 1, 2), (1, 1, 1), (1, 3, 2), (4, 1, 2)]

        def chunks(num):
            while data:
                rows = data[0:num]
                data[:] = []

                yield [row[0] for row in rows]

        return chunks

    def test_scalar_mode_columns0_mapping(self, no_tuple_fixture):
        metadata = result.SimpleResultMetaData(["a", "b", "c"])

        r = result.ChunkedIteratorResult(
            metadata, no_tuple_fixture, source_supports_scalars=True
        )

        r = r.columns(0).mappings()
        eq_(
            list(r),
            [{"a": 1}, {"a": 2}, {"a": 1}, {"a": 1}, {"a": 4}],
        )

    def test_scalar_mode_but_accessed_nonscalar_result(self, no_tuple_fixture):
        metadata = result.SimpleResultMetaData(["a", "b", "c"])

        r = result.ChunkedIteratorResult(
            metadata, no_tuple_fixture, source_supports_scalars=True
        )

        s1 = r.scalars()

        eq_(r.fetchone(), (1,))

        eq_(s1.all(), [2, 1, 1, 4])

    def test_scalar_mode_scalars_all(self, no_tuple_fixture):
        metadata = result.SimpleResultMetaData(["a", "b", "c"])

        r = result.ChunkedIteratorResult(
            metadata, no_tuple_fixture, source_supports_scalars=True
        )

        r = r.scalars()

        eq_(r.all(), [1, 2, 1, 1, 4])

    def test_scalar_mode_mfiltered_unique_rows_all(self, no_tuple_fixture):
        metadata = result.SimpleResultMetaData(
            ["a", "b", "c"], _unique_filters=[int]
        )

        r = result.ChunkedIteratorResult(
            metadata,
            no_tuple_fixture,
            source_supports_scalars=True,
        )

        r = r.unique()

        eq_(r.all(), [(1,), (2,), (4,)])

    def test_scalar_mode_mfiltered_unique_mappings_all(self, no_tuple_fixture):
        metadata = result.SimpleResultMetaData(
            ["a", "b", "c"], _unique_filters=[int]
        )

        r = result.ChunkedIteratorResult(
            metadata,
            no_tuple_fixture,
            source_supports_scalars=True,
        )

        r = r.unique()

        eq_(r.mappings().all(), [{"a": 1}, {"a": 2}, {"a": 4}])

    def test_scalar_mode_mfiltered_unique_scalars_all(self, no_tuple_fixture):
        metadata = result.SimpleResultMetaData(
            ["a", "b", "c"], _unique_filters=[int]
        )

        r = result.ChunkedIteratorResult(
            metadata,
            no_tuple_fixture,
            source_supports_scalars=True,
        )

        r = r.scalars().unique()

        eq_(r.all(), [1, 2, 4])

    def test_scalar_mode_unique_scalars_all(self, no_tuple_fixture):
        metadata = result.SimpleResultMetaData(["a", "b", "c"])

        r = result.ChunkedIteratorResult(
            metadata, no_tuple_fixture, source_supports_scalars=True
        )

        r = r.unique().scalars()

        eq_(r.all(), [1, 2, 4])

    def test_scalar_mode_scalars_fetchmany(self, normal_fixture):
        metadata = result.SimpleResultMetaData(["a", "b", "c"])

        r = result.ChunkedIteratorResult(
            metadata, normal_fixture, source_supports_scalars=True
        )

        r = r.scalars()
        eq_(list(r.partitions(2)), [[1, 2], [1, 1], [4]])

    def test_scalar_mode_unique_scalars_fetchmany(self, normal_fixture):
        metadata = result.SimpleResultMetaData(["a", "b", "c"])

        r = result.ChunkedIteratorResult(
            metadata, normal_fixture, source_supports_scalars=True
        )

        r = r.scalars().unique()
        eq_(list(r.partitions(2)), [[1, 2], [4]])

    def test_scalar_mode_unique_tuples_all(self, normal_fixture):
        metadata = result.SimpleResultMetaData(["a", "b", "c"])

        r = result.ChunkedIteratorResult(
            metadata, normal_fixture, source_supports_scalars=True
        )

        r = r.unique()

        eq_(r.all(), [(1,), (2,), (4,)])

    def test_scalar_mode_tuples_all(self, normal_fixture):
        metadata = result.SimpleResultMetaData(["a", "b", "c"])

        r = result.ChunkedIteratorResult(
            metadata, normal_fixture, source_supports_scalars=True
        )

        eq_(r.all(), [(1,), (2,), (1,), (1,), (4,)])

    def test_scalar_mode_scalars_iterate(self, no_tuple_fixture):
        metadata = result.SimpleResultMetaData(["a", "b", "c"])

        r = result.ChunkedIteratorResult(
            metadata, no_tuple_fixture, source_supports_scalars=True
        )

        r = r.scalars()

        eq_(list(r), [1, 2, 1, 1, 4])

    def test_scalar_mode_tuples_iterate(self, normal_fixture):
        metadata = result.SimpleResultMetaData(["a", "b", "c"])

        r = result.ChunkedIteratorResult(
            metadata, normal_fixture, source_supports_scalars=True
        )

        eq_(list(r), [(1,), (2,), (1,), (1,), (4,)])

    def test_scalar_mode_first(self, no_tuple_one_fixture):
        metadata = result.SimpleResultMetaData(["a", "b", "c"])

        r = result.ChunkedIteratorResult(
            metadata, no_tuple_one_fixture, source_supports_scalars=True
        )

        eq_(r.one(), (1,))

    def test_scalar_mode_scalar_one(self, no_tuple_one_fixture):
        metadata = result.SimpleResultMetaData(["a", "b", "c"])

        r = result.ChunkedIteratorResult(
            metadata, no_tuple_one_fixture, source_supports_scalars=True
        )

        eq_(r.scalar_one(), 1)
