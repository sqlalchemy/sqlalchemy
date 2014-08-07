# coding: utf-8
from sqlalchemy.testing.assertions import eq_, assert_raises, \
    assert_raises_message, is_, AssertsExecutionResults, \
    AssertsCompiledSQL, ComparesTables
from sqlalchemy.testing import engines, fixtures
from sqlalchemy import testing
import datetime
from sqlalchemy import Table, MetaData, Column, Integer, Enum, Float, select, \
    func, DateTime, Numeric, exc, String, cast, REAL, TypeDecorator, Unicode, \
    Text, null
from sqlalchemy.sql import operators
from sqlalchemy import types
from sqlalchemy.dialects.postgresql import base as postgresql
from sqlalchemy.dialects.postgresql import HSTORE, hstore, array, \
    INT4RANGE, INT8RANGE, NUMRANGE, DATERANGE, TSRANGE, TSTZRANGE, \
    JSON, JSONB
import decimal
from sqlalchemy import util
from sqlalchemy.testing.util import round_decimal
from sqlalchemy import inspect
from sqlalchemy import event

tztable = notztable = metadata = table = None


class FloatCoercionTest(fixtures.TablesTest, AssertsExecutionResults):
    __only_on__ = 'postgresql'
    __dialect__ = postgresql.dialect()
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        data_table = Table('data_table', metadata,
                           Column('id', Integer, primary_key=True),
                           Column('data', Integer)
                           )

    @classmethod
    def insert_data(cls):
        data_table = cls.tables.data_table

        data_table.insert().execute(
            {'data': 3},
            {'data': 5},
            {'data': 7},
            {'data': 2},
            {'data': 15},
            {'data': 12},
            {'data': 6},
            {'data': 478},
            {'data': 52},
            {'data': 9},
        )

    @testing.fails_on(
        'postgresql+zxjdbc',
        'XXX: postgresql+zxjdbc currently returns a Decimal result for Float')
    def test_float_coercion(self):
        data_table = self.tables.data_table

        for type_, result in [
            (Numeric, decimal.Decimal('140.381230939')),
            (Float, 140.381230939),
            (Float(asdecimal=True), decimal.Decimal('140.381230939')),
            (Numeric(asdecimal=False), 140.381230939),
        ]:
            ret = testing.db.execute(
                select([
                    func.stddev_pop(data_table.c.data, type_=type_)
                ])
            ).scalar()

            eq_(round_decimal(ret, 9), result)

            ret = testing.db.execute(
                select([
                    cast(func.stddev_pop(data_table.c.data), type_)
                ])
            ).scalar()
            eq_(round_decimal(ret, 9), result)

    @testing.fails_on('postgresql+zxjdbc',
                      'zxjdbc has no support for PG arrays')
    @testing.provide_metadata
    def test_arrays(self):
        metadata = self.metadata
        t1 = Table('t', metadata,
                   Column('x', postgresql.ARRAY(Float)),
                   Column('y', postgresql.ARRAY(REAL)),
                   Column('z', postgresql.ARRAY(postgresql.DOUBLE_PRECISION)),
                   Column('q', postgresql.ARRAY(Numeric))
                   )
        metadata.create_all()
        t1.insert().execute(x=[5], y=[5], z=[6], q=[decimal.Decimal("6.4")])
        row = t1.select().execute().first()
        eq_(
            row,
            ([5], [5], [6], [decimal.Decimal("6.4")])
        )


class EnumTest(fixtures.TestBase, AssertsExecutionResults):
    __backend__ = True

    __only_on__ = 'postgresql > 8.3'

    @testing.fails_on('postgresql+zxjdbc',
                      'zxjdbc fails on ENUM: column "XXX" is of type '
                      'XXX but expression is of type character varying')
    def test_create_table(self):
        metadata = MetaData(testing.db)
        t1 = Table(
            'table', metadata,
            Column(
                'id', Integer, primary_key=True),
            Column(
                'value', Enum(
                    'one', 'two', 'three', name='onetwothreetype')))
        t1.create()
        t1.create(checkfirst=True)  # check the create
        try:
            t1.insert().execute(value='two')
            t1.insert().execute(value='three')
            t1.insert().execute(value='three')
            eq_(t1.select().order_by(t1.c.id).execute().fetchall(),
                [(1, 'two'), (2, 'three'), (3, 'three')])
        finally:
            metadata.drop_all()
            metadata.drop_all()

    def test_name_required(self):
        metadata = MetaData(testing.db)
        etype = Enum('four', 'five', 'six', metadata=metadata)
        assert_raises(exc.CompileError, etype.create)
        assert_raises(exc.CompileError, etype.compile,
                      dialect=postgresql.dialect())

    @testing.fails_on('postgresql+zxjdbc',
                      'zxjdbc fails on ENUM: column "XXX" is of type '
                      'XXX but expression is of type character varying')
    @testing.provide_metadata
    def test_unicode_labels(self):
        metadata = self.metadata
        t1 = Table(
            'table',
            metadata,
            Column(
                'id',
                Integer,
                primary_key=True),
            Column(
                'value',
                Enum(
                    util.u('réveillé'),
                    util.u('drôle'),
                    util.u('S’il'),
                    name='onetwothreetype')))
        metadata.create_all()
        t1.insert().execute(value=util.u('drôle'))
        t1.insert().execute(value=util.u('réveillé'))
        t1.insert().execute(value=util.u('S’il'))
        eq_(t1.select().order_by(t1.c.id).execute().fetchall(),
            [(1, util.u('drôle')), (2, util.u('réveillé')),
             (3, util.u('S’il'))]
            )
        m2 = MetaData(testing.db)
        t2 = Table('table', m2, autoload=True)
        eq_(
            t2.c.value.type.enums,
            (util.u('réveillé'), util.u('drôle'), util.u('S’il'))
        )

    def test_non_native_type(self):
        metadata = MetaData()
        t1 = Table(
            'foo',
            metadata,
            Column(
                'bar',
                Enum(
                    'one',
                    'two',
                    'three',
                    name='myenum',
                    native_enum=False)))

        def go():
            t1.create(testing.db)

        try:
            self.assert_sql(
                testing.db, go, [], with_sequences=[
                    ("CREATE TABLE foo (\tbar "
                     "VARCHAR(5), \tCONSTRAINT myenum CHECK "
                     "(bar IN ('one', 'two', 'three')))", {})])
        finally:
            metadata.drop_all(testing.db)

    @testing.provide_metadata
    def test_disable_create(self):
        metadata = self.metadata

        e1 = postgresql.ENUM('one', 'two', 'three',
                             name="myenum",
                             create_type=False)

        t1 = Table('e1', metadata,
                   Column('c1', e1)
                   )
        # table can be created separately
        # without conflict
        e1.create(bind=testing.db)
        t1.create(testing.db)
        t1.drop(testing.db)
        e1.drop(bind=testing.db)

    @testing.provide_metadata
    def test_generate_multiple(self):
        """Test that the same enum twice only generates once
        for the create_all() call, without using checkfirst.

        A 'memo' collection held by the DDL runner
        now handles this.

        """
        metadata = self.metadata

        e1 = Enum('one', 'two', 'three',
                  name="myenum")
        t1 = Table('e1', metadata,
                   Column('c1', e1)
                   )

        t2 = Table('e2', metadata,
                   Column('c1', e1)
                   )

        metadata.create_all(checkfirst=False)
        metadata.drop_all(checkfirst=False)

    def test_non_native_dialect(self):
        engine = engines.testing_engine()
        engine.connect()
        engine.dialect.supports_native_enum = False
        metadata = MetaData()
        t1 = Table(
            'foo',
            metadata,
            Column(
                'bar',
                Enum(
                    'one',
                    'two',
                    'three',
                    name='myenum')))

        def go():
            t1.create(engine)

        try:
            self.assert_sql(
                engine, go, [], with_sequences=[
                    ("CREATE TABLE foo (\tbar "
                     "VARCHAR(5), \tCONSTRAINT myenum CHECK "
                     "(bar IN ('one', 'two', 'three')))", {})])
        finally:
            metadata.drop_all(engine)

    def test_standalone_enum(self):
        metadata = MetaData(testing.db)
        etype = Enum('four', 'five', 'six', name='fourfivesixtype',
                     metadata=metadata)
        etype.create()
        try:
            assert testing.db.dialect.has_type(testing.db,
                                               'fourfivesixtype')
        finally:
            etype.drop()
            assert not testing.db.dialect.has_type(testing.db,
                                                   'fourfivesixtype')
        metadata.create_all()
        try:
            assert testing.db.dialect.has_type(testing.db,
                                               'fourfivesixtype')
        finally:
            metadata.drop_all()
            assert not testing.db.dialect.has_type(testing.db,
                                                   'fourfivesixtype')

    def test_no_support(self):
        def server_version_info(self):
            return (8, 2)

        e = engines.testing_engine()
        dialect = e.dialect
        dialect._get_server_version_info = server_version_info

        assert dialect.supports_native_enum
        e.connect()
        assert not dialect.supports_native_enum

        # initialize is called again on new pool
        e.dispose()
        e.connect()
        assert not dialect.supports_native_enum

    def test_reflection(self):
        metadata = MetaData(testing.db)
        etype = Enum('four', 'five', 'six', name='fourfivesixtype',
                     metadata=metadata)
        t1 = Table(
            'table', metadata,
            Column(
                'id', Integer, primary_key=True),
            Column(
                'value', Enum(
                    'one', 'two', 'three', name='onetwothreetype')),
            Column('value2', etype))
        metadata.create_all()
        try:
            m2 = MetaData(testing.db)
            t2 = Table('table', m2, autoload=True)
            assert t2.c.value.type.enums == ('one', 'two', 'three')
            assert t2.c.value.type.name == 'onetwothreetype'
            assert t2.c.value2.type.enums == ('four', 'five', 'six')
            assert t2.c.value2.type.name == 'fourfivesixtype'
        finally:
            metadata.drop_all()

    def test_schema_reflection(self):
        metadata = MetaData(testing.db)
        etype = Enum(
            'four',
            'five',
            'six',
            name='fourfivesixtype',
            schema='test_schema',
            metadata=metadata,
        )
        t1 = Table(
            'table', metadata,
            Column(
                'id', Integer, primary_key=True),
            Column(
                'value', Enum(
                    'one', 'two', 'three',
                    name='onetwothreetype', schema='test_schema')),
            Column('value2', etype))
        metadata.create_all()
        try:
            m2 = MetaData(testing.db)
            t2 = Table('table', m2, autoload=True)
            assert t2.c.value.type.enums == ('one', 'two', 'three')
            assert t2.c.value.type.name == 'onetwothreetype'
            assert t2.c.value2.type.enums == ('four', 'five', 'six')
            assert t2.c.value2.type.name == 'fourfivesixtype'
            assert t2.c.value2.type.schema == 'test_schema'
        finally:
            metadata.drop_all()


class OIDTest(fixtures.TestBase):
    __only_on__ = 'postgresql'
    __backend__ = True

    @testing.provide_metadata
    def test_reflection(self):
        metadata = self.metadata
        Table('table', metadata, Column('x', Integer),
              Column('y', postgresql.OID))
        metadata.create_all()
        m2 = MetaData()
        t2 = Table('table', m2, autoload_with=testing.db, autoload=True)
        assert isinstance(t2.c.y.type, postgresql.OID)


class NumericInterpretationTest(fixtures.TestBase):
    __only_on__ = 'postgresql'
    __backend__ = True

    def test_numeric_codes(self):
        from sqlalchemy.dialects.postgresql import pg8000, psycopg2, base

        for dialect in (pg8000.dialect(), psycopg2.dialect()):

            typ = Numeric().dialect_impl(dialect)
            for code in base._INT_TYPES + base._FLOAT_TYPES + \
                    base._DECIMAL_TYPES:
                proc = typ.result_processor(dialect, code)
                val = 23.7
                if proc is not None:
                    val = proc(val)
                assert val in (23.7, decimal.Decimal("23.7"))

    @testing.provide_metadata
    def test_numeric_default(self):
        metadata = self.metadata
        # pg8000 appears to fail when the value is 0,
        # returns an int instead of decimal.
        t = Table('t', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('nd', Numeric(asdecimal=True), default=1),
                  Column('nf', Numeric(asdecimal=False), default=1),
                  Column('fd', Float(asdecimal=True), default=1),
                  Column('ff', Float(asdecimal=False), default=1),
                  )
        metadata.create_all()
        r = t.insert().execute()

        row = t.select().execute().first()
        assert isinstance(row[1], decimal.Decimal)
        assert isinstance(row[2], float)
        assert isinstance(row[3], decimal.Decimal)
        assert isinstance(row[4], float)
        eq_(
            row,
            (1, decimal.Decimal("1"), 1, decimal.Decimal("1"), 1)
        )


class TimezoneTest(fixtures.TestBase):
    __backend__ = True

    """Test timezone-aware datetimes.

    psycopg will return a datetime with a tzinfo attached to it, if
    postgresql returns it.  python then will not let you compare a
    datetime with a tzinfo to a datetime that doesn't have one.  this
    test illustrates two ways to have datetime types with and without
    timezone info. """

    __only_on__ = 'postgresql'

    @classmethod
    def setup_class(cls):
        global tztable, notztable, metadata
        metadata = MetaData(testing.db)

        # current_timestamp() in postgresql is assumed to return
        # TIMESTAMP WITH TIMEZONE

        tztable = Table(
            'tztable', metadata,
            Column(
                'id', Integer, primary_key=True),
            Column(
                'date', DateTime(
                    timezone=True), onupdate=func.current_timestamp()),
            Column('name', String(20)))
        notztable = Table(
            'notztable', metadata,
            Column(
                'id', Integer, primary_key=True),
            Column(
                'date', DateTime(
                    timezone=False), onupdate=cast(
                    func.current_timestamp(), DateTime(
                        timezone=False))),
            Column('name', String(20)))
        metadata.create_all()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    @testing.fails_on('postgresql+zxjdbc',
                      "XXX: postgresql+zxjdbc doesn't give a tzinfo back")
    def test_with_timezone(self):

        # get a date with a tzinfo

        somedate = \
            testing.db.connect().scalar(func.current_timestamp().select())
        assert somedate.tzinfo
        tztable.insert().execute(id=1, name='row1', date=somedate)
        row = select([tztable.c.date], tztable.c.id
                     == 1).execute().first()
        eq_(row[0], somedate)
        eq_(somedate.tzinfo.utcoffset(somedate),
            row[0].tzinfo.utcoffset(row[0]))
        result = tztable.update(tztable.c.id
                                == 1).returning(tztable.c.date).\
            execute(name='newname'
                    )
        row = result.first()
        assert row[0] >= somedate

    def test_without_timezone(self):

        # get a date without a tzinfo

        somedate = datetime.datetime(2005, 10, 20, 11, 52, 0, )
        assert not somedate.tzinfo
        notztable.insert().execute(id=1, name='row1', date=somedate)
        row = select([notztable.c.date], notztable.c.id
                     == 1).execute().first()
        eq_(row[0], somedate)
        eq_(row[0].tzinfo, None)
        result = notztable.update(notztable.c.id
                                  == 1).returning(notztable.c.date).\
            execute(name='newname'
                    )
        row = result.first()
        assert row[0] >= somedate


class TimePrecisionTest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = postgresql.dialect()
    __prefer__ = 'postgresql'
    __backend__ = True

    def test_compile(self):
        for type_, expected in [
            (postgresql.TIME(), 'TIME WITHOUT TIME ZONE'),
            (postgresql.TIME(precision=5), 'TIME(5) WITHOUT TIME ZONE'
             ),
            (postgresql.TIME(timezone=True, precision=5),
             'TIME(5) WITH TIME ZONE'),
            (postgresql.TIMESTAMP(), 'TIMESTAMP WITHOUT TIME ZONE'),
            (postgresql.TIMESTAMP(precision=5),
             'TIMESTAMP(5) WITHOUT TIME ZONE'),
            (postgresql.TIMESTAMP(timezone=True, precision=5),
             'TIMESTAMP(5) WITH TIME ZONE'),
        ]:
            self.assert_compile(type_, expected)

    @testing.only_on('postgresql', 'DB specific feature')
    @testing.provide_metadata
    def test_reflection(self):
        metadata = self.metadata
        t1 = Table(
            't1',
            metadata,
            Column('c1', postgresql.TIME()),
            Column('c2', postgresql.TIME(precision=5)),
            Column('c3', postgresql.TIME(timezone=True, precision=5)),
            Column('c4', postgresql.TIMESTAMP()),
            Column('c5', postgresql.TIMESTAMP(precision=5)),
            Column('c6', postgresql.TIMESTAMP(timezone=True,
                                              precision=5)),
        )
        t1.create()
        m2 = MetaData(testing.db)
        t2 = Table('t1', m2, autoload=True)
        eq_(t2.c.c1.type.precision, None)
        eq_(t2.c.c2.type.precision, 5)
        eq_(t2.c.c3.type.precision, 5)
        eq_(t2.c.c4.type.precision, None)
        eq_(t2.c.c5.type.precision, 5)
        eq_(t2.c.c6.type.precision, 5)
        eq_(t2.c.c1.type.timezone, False)
        eq_(t2.c.c2.type.timezone, False)
        eq_(t2.c.c3.type.timezone, True)
        eq_(t2.c.c4.type.timezone, False)
        eq_(t2.c.c5.type.timezone, False)
        eq_(t2.c.c6.type.timezone, True)


class ArrayTest(fixtures.TablesTest, AssertsExecutionResults):

    __only_on__ = 'postgresql'
    __backend__ = True
    __unsupported_on__ = 'postgresql+pg8000', 'postgresql+zxjdbc'

    @classmethod
    def define_tables(cls, metadata):

        class ProcValue(TypeDecorator):
            impl = postgresql.ARRAY(Integer, dimensions=2)

            def process_bind_param(self, value, dialect):
                if value is None:
                    return None
                return [
                    [x + 5 for x in v]
                    for v in value
                ]

            def process_result_value(self, value, dialect):
                if value is None:
                    return None
                return [
                    [x - 7 for x in v]
                    for v in value
                ]

        Table('arrtable', metadata,
              Column('id', Integer, primary_key=True),
              Column('intarr', postgresql.ARRAY(Integer)),
              Column('strarr', postgresql.ARRAY(Unicode())),
              Column('dimarr', ProcValue)
              )

        Table('dim_arrtable', metadata,
              Column('id', Integer, primary_key=True),
              Column('intarr', postgresql.ARRAY(Integer, dimensions=1)),
              Column('strarr', postgresql.ARRAY(Unicode(), dimensions=1)),
              Column('dimarr', ProcValue)
              )

    def _fixture_456(self, table):
        testing.db.execute(
            table.insert(),
            intarr=[4, 5, 6]
        )

    def test_reflect_array_column(self):
        metadata2 = MetaData(testing.db)
        tbl = Table('arrtable', metadata2, autoload=True)
        assert isinstance(tbl.c.intarr.type, postgresql.ARRAY)
        assert isinstance(tbl.c.strarr.type, postgresql.ARRAY)
        assert isinstance(tbl.c.intarr.type.item_type, Integer)
        assert isinstance(tbl.c.strarr.type.item_type, String)

    def test_insert_array(self):
        arrtable = self.tables.arrtable
        arrtable.insert().execute(intarr=[1, 2, 3], strarr=[util.u('abc'),
                                                            util.u('def')])
        results = arrtable.select().execute().fetchall()
        eq_(len(results), 1)
        eq_(results[0]['intarr'], [1, 2, 3])
        eq_(results[0]['strarr'], [util.u('abc'), util.u('def')])

    def test_array_where(self):
        arrtable = self.tables.arrtable
        arrtable.insert().execute(intarr=[1, 2, 3], strarr=[util.u('abc'),
                                                            util.u('def')])
        arrtable.insert().execute(intarr=[4, 5, 6], strarr=util.u('ABC'))
        results = arrtable.select().where(
            arrtable.c.intarr == [
                1,
                2,
                3]).execute().fetchall()
        eq_(len(results), 1)
        eq_(results[0]['intarr'], [1, 2, 3])

    def test_array_concat(self):
        arrtable = self.tables.arrtable
        arrtable.insert().execute(intarr=[1, 2, 3],
                                  strarr=[util.u('abc'), util.u('def')])
        results = select([arrtable.c.intarr + [4, 5,
                                               6]]).execute().fetchall()
        eq_(len(results), 1)
        eq_(results[0][0], [1, 2, 3, 4, 5, 6, ])

    def test_array_comparison(self):
        arrtable = self.tables.arrtable
        arrtable.insert().execute(intarr=[1, 2, 3],
                    strarr=[util.u('abc'), util.u('def')])
        results = select([arrtable.c.id]).\
                        where(arrtable.c.intarr < [4, 5, 6]).execute()\
                        .fetchall()
        eq_(len(results), 1)
        eq_(results[0][0], 3)

    def test_array_subtype_resultprocessor(self):
        arrtable = self.tables.arrtable
        arrtable.insert().execute(intarr=[4, 5, 6],
                                  strarr=[[util.ue('m\xe4\xe4')], [
                                      util.ue('m\xf6\xf6')]])
        arrtable.insert().execute(intarr=[1, 2, 3], strarr=[
            util.ue('m\xe4\xe4'), util.ue('m\xf6\xf6')])
        results = \
            arrtable.select(order_by=[arrtable.c.intarr]).execute().fetchall()
        eq_(len(results), 2)
        eq_(results[0]['strarr'], [util.ue('m\xe4\xe4'), util.ue('m\xf6\xf6')])
        eq_(results[1]['strarr'],
            [[util.ue('m\xe4\xe4')],
             [util.ue('m\xf6\xf6')]])

    def test_array_literal(self):
        eq_(
            testing.db.scalar(
                select([
                    postgresql.array([1, 2]) + postgresql.array([3, 4, 5])
                ])
            ), [1, 2, 3, 4, 5]
        )

    def test_array_literal_compare(self):
        eq_(
            testing.db.scalar(
                select([
                    postgresql.array([1, 2]) < [3, 4, 5]
                ])
                ), True
        )

    def test_array_getitem_single_type(self):
        arrtable = self.tables.arrtable
        is_(arrtable.c.intarr[1].type._type_affinity, Integer)
        is_(arrtable.c.strarr[1].type._type_affinity, String)

    def test_array_getitem_slice_type(self):
        arrtable = self.tables.arrtable
        is_(arrtable.c.intarr[1:3].type._type_affinity, postgresql.ARRAY)
        is_(arrtable.c.strarr[1:3].type._type_affinity, postgresql.ARRAY)

    def test_array_getitem_single_exec(self):
        arrtable = self.tables.arrtable
        self._fixture_456(arrtable)
        eq_(
            testing.db.scalar(select([arrtable.c.intarr[2]])),
            5
        )
        testing.db.execute(
            arrtable.update().values({arrtable.c.intarr[2]: 7})
        )
        eq_(
            testing.db.scalar(select([arrtable.c.intarr[2]])),
            7
        )

    def test_undim_array_empty(self):
        arrtable = self.tables.arrtable
        self._fixture_456(arrtable)
        eq_(
            testing.db.scalar(
                select([arrtable.c.intarr]).
                where(arrtable.c.intarr.contains([]))
            ),
            [4, 5, 6]
        )

    def test_array_getitem_slice_exec(self):
        arrtable = self.tables.arrtable
        testing.db.execute(
            arrtable.insert(),
            intarr=[4, 5, 6],
            strarr=[util.u('abc'), util.u('def')]
        )
        eq_(
            testing.db.scalar(select([arrtable.c.intarr[2:3]])),
            [5, 6]
        )
        testing.db.execute(
            arrtable.update().values({arrtable.c.intarr[2:3]: [7, 8]})
        )
        eq_(
            testing.db.scalar(select([arrtable.c.intarr[2:3]])),
            [7, 8]
        )

    def _test_undim_array_contains_typed_exec(self, struct):
        arrtable = self.tables.arrtable
        self._fixture_456(arrtable)
        eq_(
            testing.db.scalar(
                select([arrtable.c.intarr]).
                where(arrtable.c.intarr.contains(struct([4, 5])))
            ),
            [4, 5, 6]
        )

    def test_undim_array_contains_set_exec(self):
        self._test_undim_array_contains_typed_exec(set)

    def test_undim_array_contains_list_exec(self):
        self._test_undim_array_contains_typed_exec(list)

    def test_undim_array_contains_generator_exec(self):
        self._test_undim_array_contains_typed_exec(
            lambda elem: (x for x in elem))

    def _test_dim_array_contains_typed_exec(self, struct):
        dim_arrtable = self.tables.dim_arrtable
        self._fixture_456(dim_arrtable)
        eq_(
            testing.db.scalar(
                select([dim_arrtable.c.intarr]).
                where(dim_arrtable.c.intarr.contains(struct([4, 5])))
            ),
            [4, 5, 6]
        )

    def test_dim_array_contains_set_exec(self):
        self._test_dim_array_contains_typed_exec(set)

    def test_dim_array_contains_list_exec(self):
        self._test_dim_array_contains_typed_exec(list)

    def test_dim_array_contains_generator_exec(self):
        self._test_dim_array_contains_typed_exec(
            lambda elem: (
                x for x in elem))

    def test_array_contained_by_exec(self):
        arrtable = self.tables.arrtable
        with testing.db.connect() as conn:
            conn.execute(
                arrtable.insert(),
                intarr=[6, 5, 4]
            )
            eq_(
                conn.scalar(
                    select([arrtable.c.intarr.contained_by([4, 5, 6, 7])])
                ),
                True
            )

    def test_array_overlap_exec(self):
        arrtable = self.tables.arrtable
        with testing.db.connect() as conn:
            conn.execute(
                arrtable.insert(),
                intarr=[4, 5, 6]
            )
            eq_(
                conn.scalar(
                    select([arrtable.c.intarr]).
                    where(arrtable.c.intarr.overlap([7, 6]))
                ),
                [4, 5, 6]
            )

    def test_array_any_exec(self):
        arrtable = self.tables.arrtable
        with testing.db.connect() as conn:
            conn.execute(
                arrtable.insert(),
                intarr=[4, 5, 6]
            )
            eq_(
                conn.scalar(
                    select([arrtable.c.intarr]).
                    where(postgresql.Any(5, arrtable.c.intarr))
                ),
                [4, 5, 6]
            )

    def test_array_all_exec(self):
        arrtable = self.tables.arrtable
        with testing.db.connect() as conn:
            conn.execute(
                arrtable.insert(),
                intarr=[4, 5, 6]
            )
            eq_(
                conn.scalar(
                    select([arrtable.c.intarr]).
                    where(arrtable.c.intarr.all(4, operator=operators.le))
                ),
                [4, 5, 6]
            )

    @testing.provide_metadata
    def test_tuple_flag(self):
        metadata = self.metadata

        t1 = Table(
            't1', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', postgresql.ARRAY(String(5), as_tuple=True)),
            Column(
                'data2',
                postgresql.ARRAY(
                    Numeric(asdecimal=False), as_tuple=True)
            )
        )
        metadata.create_all()
        testing.db.execute(
            t1.insert(), id=1, data=[
                "1", "2", "3"], data2=[
                5.4, 5.6])
        testing.db.execute(
            t1.insert(),
            id=2,
            data=[
                "4",
                "5",
                "6"],
            data2=[1.0])
        testing.db.execute(t1.insert(), id=3, data=[["4", "5"], ["6", "7"]],
                           data2=[[5.4, 5.6], [1.0, 1.1]])

        r = testing.db.execute(t1.select().order_by(t1.c.id)).fetchall()
        eq_(
            r,
            [
                (1, ('1', '2', '3'), (5.4, 5.6)),
                (2, ('4', '5', '6'), (1.0,)),
                (3, (('4', '5'), ('6', '7')), ((5.4, 5.6), (1.0, 1.1)))
            ]
        )
        # hashable
        eq_(
            set(row[1] for row in r),
            set([('1', '2', '3'), ('4', '5', '6'), (('4', '5'), ('6', '7'))])
        )

    def test_dimension(self):
        arrtable = self.tables.arrtable
        testing.db.execute(arrtable.insert(), dimarr=[[1, 2, 3], [4, 5, 6]])
        eq_(
            testing.db.scalar(select([arrtable.c.dimarr])),
            [[-1, 0, 1], [2, 3, 4]]
        )


class TimestampTest(fixtures.TestBase, AssertsExecutionResults):
    __only_on__ = 'postgresql'
    __backend__ = True

    def test_timestamp(self):
        engine = testing.db
        connection = engine.connect()

        s = select(["timestamp '2007-12-25'"])
        result = connection.execute(s).first()
        eq_(result[0], datetime.datetime(2007, 12, 25, 0, 0))


class SpecialTypesTest(fixtures.TestBase, ComparesTables, AssertsCompiledSQL):

    """test DDL and reflection of PG-specific types """

    __only_on__ = 'postgresql'
    __excluded_on__ = (('postgresql', '<', (8, 3, 0)),)
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global metadata, table
        metadata = MetaData(testing.db)

        # create these types so that we can issue
        # special SQL92 INTERVAL syntax
        class y2m(types.UserDefinedType, postgresql.INTERVAL):

            def get_col_spec(self):
                return "INTERVAL YEAR TO MONTH"

        class d2s(types.UserDefinedType, postgresql.INTERVAL):

            def get_col_spec(self):
                return "INTERVAL DAY TO SECOND"

        table = Table(
            'sometable', metadata,
            Column(
                'id', postgresql.UUID, primary_key=True),
            Column(
                'flag', postgresql.BIT),
            Column(
                'bitstring', postgresql.BIT(4)),
            Column('addr', postgresql.INET),
            Column('addr2', postgresql.MACADDR),
            Column('addr3', postgresql.CIDR),
            Column('doubleprec', postgresql.DOUBLE_PRECISION),
            Column('plain_interval', postgresql.INTERVAL),
            Column('year_interval', y2m()),
            Column('month_interval', d2s()),
            Column('precision_interval', postgresql.INTERVAL(
                precision=3)),
            Column('tsvector_document', postgresql.TSVECTOR))

        metadata.create_all()

        # cheat so that the "strict type check"
        # works
        table.c.year_interval.type = postgresql.INTERVAL()
        table.c.month_interval.type = postgresql.INTERVAL()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_reflection(self):
        m = MetaData(testing.db)
        t = Table('sometable', m, autoload=True)

        self.assert_tables_equal(table, t, strict_types=True)
        assert t.c.plain_interval.type.precision is None
        assert t.c.precision_interval.type.precision == 3
        assert t.c.bitstring.type.length == 4

    def test_bit_compile(self):
        pairs = [(postgresql.BIT(), 'BIT(1)'),
                 (postgresql.BIT(5), 'BIT(5)'),
                 (postgresql.BIT(varying=True), 'BIT VARYING'),
                 (postgresql.BIT(5, varying=True), 'BIT VARYING(5)'),
                 ]
        for type_, expected in pairs:
            self.assert_compile(type_, expected)

    @testing.provide_metadata
    def test_tsvector_round_trip(self):
        t = Table('t1', self.metadata, Column('data', postgresql.TSVECTOR))
        t.create()
        testing.db.execute(t.insert(), data="a fat cat sat")
        eq_(testing.db.scalar(select([t.c.data])), "'a' 'cat' 'fat' 'sat'")

        testing.db.execute(t.update(), data="'a' 'cat' 'fat' 'mat' 'sat'")

        eq_(testing.db.scalar(select([t.c.data])),
            "'a' 'cat' 'fat' 'mat' 'sat'")

    @testing.provide_metadata
    def test_bit_reflection(self):
        metadata = self.metadata
        t1 = Table('t1', metadata,
                   Column('bit1', postgresql.BIT()),
                   Column('bit5', postgresql.BIT(5)),
                   Column('bitvarying', postgresql.BIT(varying=True)),
                   Column('bitvarying5', postgresql.BIT(5, varying=True)),
                   )
        t1.create()
        m2 = MetaData(testing.db)
        t2 = Table('t1', m2, autoload=True)
        eq_(t2.c.bit1.type.length, 1)
        eq_(t2.c.bit1.type.varying, False)
        eq_(t2.c.bit5.type.length, 5)
        eq_(t2.c.bit5.type.varying, False)
        eq_(t2.c.bitvarying.type.length, None)
        eq_(t2.c.bitvarying.type.varying, True)
        eq_(t2.c.bitvarying5.type.length, 5)
        eq_(t2.c.bitvarying5.type.varying, True)


class UUIDTest(fixtures.TestBase):

    """Test the bind/return values of the UUID type."""

    __only_on__ = 'postgresql >= 8.3'
    __backend__ = True

    @testing.fails_on(
        'postgresql+zxjdbc',
        'column "data" is of type uuid but expression '
        'is of type character varying')
    @testing.fails_on('postgresql+pg8000', 'No support for UUID type')
    def test_uuid_string(self):
        import uuid
        self._test_round_trip(
            Table('utable', MetaData(),
                  Column('data', postgresql.UUID())
                  ),
            str(uuid.uuid4()),
            str(uuid.uuid4())
        )

    @testing.fails_on(
        'postgresql+zxjdbc',
        'column "data" is of type uuid but expression is '
        'of type character varying')
    @testing.fails_on('postgresql+pg8000', 'No support for UUID type')
    def test_uuid_uuid(self):
        import uuid
        self._test_round_trip(
            Table('utable', MetaData(),
                  Column('data', postgresql.UUID(as_uuid=True))
                  ),
            uuid.uuid4(),
            uuid.uuid4()
        )

    def test_no_uuid_available(self):
        from sqlalchemy.dialects.postgresql import base
        uuid_type = base._python_UUID
        base._python_UUID = None
        try:
            assert_raises(
                NotImplementedError,
                postgresql.UUID, as_uuid=True
            )
        finally:
            base._python_UUID = uuid_type

    def setup(self):
        self.conn = testing.db.connect()
        trans = self.conn.begin()

    def teardown(self):
        self.conn.close()

    def _test_round_trip(self, utable, value1, value2):
        utable.create(self.conn)
        self.conn.execute(utable.insert(), {'data': value1})
        self.conn.execute(utable.insert(), {'data': value2})
        r = self.conn.execute(
            select([utable.c.data]).
            where(utable.c.data != value1)
        )
        eq_(r.fetchone()[0], value2)
        eq_(r.fetchone(), None)


class HStoreTest(AssertsCompiledSQL, fixtures.TestBase):
    __dialect__ = 'postgresql'

    def setup(self):
        metadata = MetaData()
        self.test_table = Table('test_table', metadata,
                                Column('id', Integer, primary_key=True),
                                Column('hash', HSTORE)
                                )
        self.hashcol = self.test_table.c.hash

    def _test_where(self, whereclause, expected):
        stmt = select([self.test_table]).where(whereclause)
        self.assert_compile(
            stmt,
            "SELECT test_table.id, test_table.hash FROM test_table "
            "WHERE %s" % expected
        )

    def _test_cols(self, colclause, expected, from_=True):
        stmt = select([colclause])
        self.assert_compile(
            stmt,
            (
                "SELECT %s" +
                (" FROM test_table" if from_ else "")
            ) % expected
        )

    def test_bind_serialize_default(self):

        dialect = postgresql.dialect()
        proc = self.test_table.c.hash.type._cached_bind_processor(dialect)
        eq_(
            proc(util.OrderedDict([("key1", "value1"), ("key2", "value2")])),
            '"key1"=>"value1", "key2"=>"value2"'
        )

    def test_bind_serialize_with_slashes_and_quotes(self):
        dialect = postgresql.dialect()
        proc = self.test_table.c.hash.type._cached_bind_processor(dialect)
        eq_(
            proc({'\\"a': '\\"1'}),
            '"\\\\\\"a"=>"\\\\\\"1"'
        )

    def test_parse_error(self):
        dialect = postgresql.dialect()
        proc = self.test_table.c.hash.type._cached_result_processor(
            dialect, None)
        assert_raises_message(
            ValueError,
            r'''After u?'\[\.\.\.\], "key1"=>"value1", ', could not parse '''
            '''residual at position 36: u?'crapcrapcrap, "key3"\[\.\.\.\]''',
            proc,
            '"key2"=>"value2", "key1"=>"value1", '
            'crapcrapcrap, "key3"=>"value3"'
        )

    def test_result_deserialize_default(self):
        dialect = postgresql.dialect()
        proc = self.test_table.c.hash.type._cached_result_processor(
            dialect, None)
        eq_(
            proc('"key2"=>"value2", "key1"=>"value1"'),
            {"key1": "value1", "key2": "value2"}
        )

    def test_result_deserialize_with_slashes_and_quotes(self):
        dialect = postgresql.dialect()
        proc = self.test_table.c.hash.type._cached_result_processor(
            dialect, None)
        eq_(
            proc('"\\\\\\"a"=>"\\\\\\"1"'),
            {'\\"a': '\\"1'}
        )

    def test_bind_serialize_psycopg2(self):
        from sqlalchemy.dialects.postgresql import psycopg2

        dialect = psycopg2.PGDialect_psycopg2()
        dialect._has_native_hstore = True
        proc = self.test_table.c.hash.type._cached_bind_processor(dialect)
        is_(proc, None)

        dialect = psycopg2.PGDialect_psycopg2()
        dialect._has_native_hstore = False
        proc = self.test_table.c.hash.type._cached_bind_processor(dialect)
        eq_(
            proc(util.OrderedDict([("key1", "value1"), ("key2", "value2")])),
            '"key1"=>"value1", "key2"=>"value2"'
        )

    def test_result_deserialize_psycopg2(self):
        from sqlalchemy.dialects.postgresql import psycopg2

        dialect = psycopg2.PGDialect_psycopg2()
        dialect._has_native_hstore = True
        proc = self.test_table.c.hash.type._cached_result_processor(
            dialect, None)
        is_(proc, None)

        dialect = psycopg2.PGDialect_psycopg2()
        dialect._has_native_hstore = False
        proc = self.test_table.c.hash.type._cached_result_processor(
            dialect, None)
        eq_(
            proc('"key2"=>"value2", "key1"=>"value1"'),
            {"key1": "value1", "key2": "value2"}
        )

    def test_where_has_key(self):
        self._test_where(
            # hide from 2to3
            getattr(self.hashcol, 'has_key')('foo'),
            "test_table.hash ? %(hash_1)s"
        )

    def test_where_has_all(self):
        self._test_where(
            self.hashcol.has_all(postgresql.array(['1', '2'])),
            "test_table.hash ?& ARRAY[%(param_1)s, %(param_2)s]"
        )

    def test_where_has_any(self):
        self._test_where(
            self.hashcol.has_any(postgresql.array(['1', '2'])),
            "test_table.hash ?| ARRAY[%(param_1)s, %(param_2)s]"
        )

    def test_where_defined(self):
        self._test_where(
            self.hashcol.defined('foo'),
            "defined(test_table.hash, %(param_1)s)"
        )

    def test_where_contains(self):
        self._test_where(
            self.hashcol.contains({'foo': '1'}),
            "test_table.hash @> %(hash_1)s"
        )

    def test_where_contained_by(self):
        self._test_where(
            self.hashcol.contained_by({'foo': '1', 'bar': None}),
            "test_table.hash <@ %(hash_1)s"
        )

    def test_where_getitem(self):
        self._test_where(
            self.hashcol['bar'] == None,
            "(test_table.hash -> %(hash_1)s) IS NULL"
        )

    def test_cols_get(self):
        self._test_cols(
            self.hashcol['foo'],
            "test_table.hash -> %(hash_1)s AS anon_1",
            True
        )

    def test_cols_delete_single_key(self):
        self._test_cols(
            self.hashcol.delete('foo'),
            "delete(test_table.hash, %(param_1)s) AS delete_1",
            True
        )

    def test_cols_delete_array_of_keys(self):
        self._test_cols(
            self.hashcol.delete(postgresql.array(['foo', 'bar'])),
            ("delete(test_table.hash, ARRAY[%(param_1)s, %(param_2)s]) "
             "AS delete_1"),
            True
        )

    def test_cols_delete_matching_pairs(self):
        self._test_cols(
            self.hashcol.delete(hstore('1', '2')),
            ("delete(test_table.hash, hstore(%(param_1)s, %(param_2)s)) "
             "AS delete_1"),
            True
        )

    def test_cols_slice(self):
        self._test_cols(
            self.hashcol.slice(postgresql.array(['1', '2'])),
            ("slice(test_table.hash, ARRAY[%(param_1)s, %(param_2)s]) "
             "AS slice_1"),
            True
        )

    def test_cols_hstore_pair_text(self):
        self._test_cols(
            hstore('foo', '3')['foo'],
            "hstore(%(param_1)s, %(param_2)s) -> %(hstore_1)s AS anon_1",
            False
        )

    def test_cols_hstore_pair_array(self):
        self._test_cols(
            hstore(postgresql.array(['1', '2']),
                   postgresql.array(['3', None]))['1'],
            ("hstore(ARRAY[%(param_1)s, %(param_2)s], "
             "ARRAY[%(param_3)s, NULL]) -> %(hstore_1)s AS anon_1"),
            False
        )

    def test_cols_hstore_single_array(self):
        self._test_cols(
            hstore(postgresql.array(['1', '2', '3', None]))['3'],
            ("hstore(ARRAY[%(param_1)s, %(param_2)s, %(param_3)s, NULL]) "
             "-> %(hstore_1)s AS anon_1"),
            False
        )

    def test_cols_concat(self):
        self._test_cols(
            self.hashcol.concat(hstore(cast(self.test_table.c.id, Text), '3')),
            ("test_table.hash || hstore(CAST(test_table.id AS TEXT), "
             "%(param_1)s) AS anon_1"),
            True
        )

    def test_cols_concat_op(self):
        self._test_cols(
            hstore('foo', 'bar') + self.hashcol,
            "hstore(%(param_1)s, %(param_2)s) || test_table.hash AS anon_1",
            True
        )

    def test_cols_concat_get(self):
        self._test_cols(
            (self.hashcol + self.hashcol)['foo'],
            "test_table.hash || test_table.hash -> %(param_1)s AS anon_1"
        )

    def test_cols_keys(self):
        self._test_cols(
            # hide from 2to3
            getattr(self.hashcol, 'keys')(),
            "akeys(test_table.hash) AS akeys_1",
            True
        )

    def test_cols_vals(self):
        self._test_cols(
            self.hashcol.vals(),
            "avals(test_table.hash) AS avals_1",
            True
        )

    def test_cols_array(self):
        self._test_cols(
            self.hashcol.array(),
            "hstore_to_array(test_table.hash) AS hstore_to_array_1",
            True
        )

    def test_cols_matrix(self):
        self._test_cols(
            self.hashcol.matrix(),
            "hstore_to_matrix(test_table.hash) AS hstore_to_matrix_1",
            True
        )


class HStoreRoundTripTest(fixtures.TablesTest):
    __requires__ = 'hstore',
    __dialect__ = 'postgresql'
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table('data_table', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(30), nullable=False),
              Column('data', HSTORE)
              )

    def _fixture_data(self, engine):
        data_table = self.tables.data_table
        engine.execute(
            data_table.insert(),
            {'name': 'r1', 'data': {"k1": "r1v1", "k2": "r1v2"}},
            {'name': 'r2', 'data': {"k1": "r2v1", "k2": "r2v2"}},
            {'name': 'r3', 'data': {"k1": "r3v1", "k2": "r3v2"}},
            {'name': 'r4', 'data': {"k1": "r4v1", "k2": "r4v2"}},
            {'name': 'r5', 'data': {"k1": "r5v1", "k2": "r5v2"}},
        )

    def _assert_data(self, compare):
        data = testing.db.execute(
            select([self.tables.data_table.c.data]).
            order_by(self.tables.data_table.c.name)
        ).fetchall()
        eq_([d for d, in data], compare)

    def _test_insert(self, engine):
        engine.execute(
            self.tables.data_table.insert(),
            {'name': 'r1', 'data': {"k1": "r1v1", "k2": "r1v2"}}
        )
        self._assert_data([{"k1": "r1v1", "k2": "r1v2"}])

    def _non_native_engine(self):
        if testing.against("postgresql+psycopg2"):
            engine = engines.testing_engine(
                options=dict(
                    use_native_hstore=False))
        else:
            engine = testing.db
        engine.connect()
        return engine

    def test_reflect(self):
        insp = inspect(testing.db)
        cols = insp.get_columns('data_table')
        assert isinstance(cols[2]['type'], HSTORE)

    @testing.only_on("postgresql+psycopg2")
    def test_insert_native(self):
        engine = testing.db
        self._test_insert(engine)

    def test_insert_python(self):
        engine = self._non_native_engine()
        self._test_insert(engine)

    @testing.only_on("postgresql+psycopg2")
    def test_criterion_native(self):
        engine = testing.db
        self._fixture_data(engine)
        self._test_criterion(engine)

    def test_criterion_python(self):
        engine = self._non_native_engine()
        self._fixture_data(engine)
        self._test_criterion(engine)

    def _test_criterion(self, engine):
        data_table = self.tables.data_table
        result = engine.execute(
            select([data_table.c.data]).where(
                data_table.c.data['k1'] == 'r3v1')).first()
        eq_(result, ({'k1': 'r3v1', 'k2': 'r3v2'},))

    def _test_fixed_round_trip(self, engine):
        s = select([
            hstore(
                array(['key1', 'key2', 'key3']),
                array(['value1', 'value2', 'value3'])
            )
        ])
        eq_(
            engine.scalar(s),
            {"key1": "value1", "key2": "value2", "key3": "value3"}
        )

    def test_fixed_round_trip_python(self):
        engine = self._non_native_engine()
        self._test_fixed_round_trip(engine)

    @testing.only_on("postgresql+psycopg2")
    def test_fixed_round_trip_native(self):
        engine = testing.db
        self._test_fixed_round_trip(engine)

    def _test_unicode_round_trip(self, engine):
        s = select([
            hstore(
                array([util.u('réveillé'), util.u('drôle'), util.u('S’il')]),
                array([util.u('réveillé'), util.u('drôle'), util.u('S’il')])
            )
        ])
        eq_(
            engine.scalar(s),
            {
                util.u('réveillé'): util.u('réveillé'),
                util.u('drôle'): util.u('drôle'),
                util.u('S’il'): util.u('S’il')
            }
        )

    @testing.only_on("postgresql+psycopg2")
    def test_unicode_round_trip_python(self):
        engine = self._non_native_engine()
        self._test_unicode_round_trip(engine)

    @testing.only_on("postgresql+psycopg2")
    def test_unicode_round_trip_native(self):
        engine = testing.db
        self._test_unicode_round_trip(engine)

    def test_escaped_quotes_round_trip_python(self):
        engine = self._non_native_engine()
        self._test_escaped_quotes_round_trip(engine)

    @testing.only_on("postgresql+psycopg2")
    def test_escaped_quotes_round_trip_native(self):
        engine = testing.db
        self._test_escaped_quotes_round_trip(engine)

    def _test_escaped_quotes_round_trip(self, engine):
        engine.execute(
            self.tables.data_table.insert(),
            {'name': 'r1', 'data': {r'key \"foo\"': r'value \"bar"\ xyz'}}
        )
        self._assert_data([{r'key \"foo\"': r'value \"bar"\ xyz'}])

    def test_orm_round_trip(self):
        from sqlalchemy import orm

        class Data(object):

            def __init__(self, name, data):
                self.name = name
                self.data = data
        orm.mapper(Data, self.tables.data_table)
        s = orm.Session(testing.db)
        d = Data(name='r1', data={"key1": "value1", "key2": "value2",
                                  "key3": "value3"})
        s.add(d)
        eq_(
            s.query(Data.data, Data).all(),
            [(d.data, d)]
        )


class _RangeTypeMixin(object):
    __requires__ = 'range_types',
    __dialect__ = 'postgresql+psycopg2'
    __backend__ = True

    def extras(self):
        # done this way so we don't get ImportErrors with
        # older psycopg2 versions.
        from psycopg2 import extras
        return extras

    @classmethod
    def define_tables(cls, metadata):
        # no reason ranges shouldn't be primary keys,
        # so lets just use them as such
        table = Table('data_table', metadata,
                      Column('range', cls._col_type, primary_key=True),
                      )
        cls.col = table.c.range

    def test_actual_type(self):
        eq_(str(self._col_type()), self._col_str)

    def test_reflect(self):
        from sqlalchemy import inspect
        insp = inspect(testing.db)
        cols = insp.get_columns('data_table')
        assert isinstance(cols[0]['type'], self._col_type)

    def _assert_data(self):
        data = testing.db.execute(
            select([self.tables.data_table.c.range])
        ).fetchall()
        eq_(data, [(self._data_obj(), )])

    def test_insert_obj(self):
        testing.db.engine.execute(
            self.tables.data_table.insert(),
            {'range': self._data_obj()}
        )
        self._assert_data()

    def test_insert_text(self):
        testing.db.engine.execute(
            self.tables.data_table.insert(),
            {'range': self._data_str}
        )
        self._assert_data()

    # operator tests

    def _test_clause(self, colclause, expected):
        dialect = postgresql.dialect()
        compiled = str(colclause.compile(dialect=dialect))
        eq_(compiled, expected)

    def test_where_equal(self):
        self._test_clause(
            self.col == self._data_str,
            "data_table.range = %(range_1)s"
        )

    def test_where_not_equal(self):
        self._test_clause(
            self.col != self._data_str,
            "data_table.range <> %(range_1)s"
        )

    def test_where_less_than(self):
        self._test_clause(
            self.col < self._data_str,
            "data_table.range < %(range_1)s"
        )

    def test_where_greater_than(self):
        self._test_clause(
            self.col > self._data_str,
            "data_table.range > %(range_1)s"
        )

    def test_where_less_than_or_equal(self):
        self._test_clause(
            self.col <= self._data_str,
            "data_table.range <= %(range_1)s"
        )

    def test_where_greater_than_or_equal(self):
        self._test_clause(
            self.col >= self._data_str,
            "data_table.range >= %(range_1)s"
        )

    def test_contains(self):
        self._test_clause(
            self.col.contains(self._data_str),
            "data_table.range @> %(range_1)s"
        )

    def test_contained_by(self):
        self._test_clause(
            self.col.contained_by(self._data_str),
            "data_table.range <@ %(range_1)s"
        )

    def test_overlaps(self):
        self._test_clause(
            self.col.overlaps(self._data_str),
            "data_table.range && %(range_1)s"
        )

    def test_strictly_left_of(self):
        self._test_clause(
            self.col << self._data_str,
            "data_table.range << %(range_1)s"
        )
        self._test_clause(
            self.col.strictly_left_of(self._data_str),
            "data_table.range << %(range_1)s"
        )

    def test_strictly_right_of(self):
        self._test_clause(
            self.col >> self._data_str,
            "data_table.range >> %(range_1)s"
        )
        self._test_clause(
            self.col.strictly_right_of(self._data_str),
            "data_table.range >> %(range_1)s"
        )

    def test_not_extend_right_of(self):
        self._test_clause(
            self.col.not_extend_right_of(self._data_str),
            "data_table.range &< %(range_1)s"
        )

    def test_not_extend_left_of(self):
        self._test_clause(
            self.col.not_extend_left_of(self._data_str),
            "data_table.range &> %(range_1)s"
        )

    def test_adjacent_to(self):
        self._test_clause(
            self.col.adjacent_to(self._data_str),
            "data_table.range -|- %(range_1)s"
        )

    def test_union(self):
        self._test_clause(
            self.col + self.col,
            "data_table.range + data_table.range"
        )

    def test_union_result(self):
        # insert
        testing.db.engine.execute(
            self.tables.data_table.insert(),
            {'range': self._data_str}
        )
        # select
        range = self.tables.data_table.c.range
        data = testing.db.execute(
            select([range + range])
        ).fetchall()
        eq_(data, [(self._data_obj(), )])

    def test_intersection(self):
        self._test_clause(
            self.col * self.col,
            "data_table.range * data_table.range"
        )

    def test_intersection_result(self):
        # insert
        testing.db.engine.execute(
            self.tables.data_table.insert(),
            {'range': self._data_str}
        )
        # select
        range = self.tables.data_table.c.range
        data = testing.db.execute(
            select([range * range])
        ).fetchall()
        eq_(data, [(self._data_obj(), )])

    def test_different(self):
        self._test_clause(
            self.col - self.col,
            "data_table.range - data_table.range"
        )

    def test_difference_result(self):
        # insert
        testing.db.engine.execute(
            self.tables.data_table.insert(),
            {'range': self._data_str}
        )
        # select
        range = self.tables.data_table.c.range
        data = testing.db.execute(
            select([range - range])
        ).fetchall()
        eq_(data, [(self._data_obj().__class__(empty=True), )])


class Int4RangeTests(_RangeTypeMixin, fixtures.TablesTest):

    _col_type = INT4RANGE
    _col_str = 'INT4RANGE'
    _data_str = '[1,2)'

    def _data_obj(self):
        return self.extras().NumericRange(1, 2)


class Int8RangeTests(_RangeTypeMixin, fixtures.TablesTest):

    _col_type = INT8RANGE
    _col_str = 'INT8RANGE'
    _data_str = '[9223372036854775806,9223372036854775807)'

    def _data_obj(self):
        return self.extras().NumericRange(
            9223372036854775806, 9223372036854775807
        )


class NumRangeTests(_RangeTypeMixin, fixtures.TablesTest):

    _col_type = NUMRANGE
    _col_str = 'NUMRANGE'
    _data_str = '[1.0,2.0)'

    def _data_obj(self):
        return self.extras().NumericRange(
            decimal.Decimal('1.0'), decimal.Decimal('2.0')
        )


class DateRangeTests(_RangeTypeMixin, fixtures.TablesTest):

    _col_type = DATERANGE
    _col_str = 'DATERANGE'
    _data_str = '[2013-03-23,2013-03-24)'

    def _data_obj(self):
        return self.extras().DateRange(
            datetime.date(2013, 3, 23), datetime.date(2013, 3, 24)
        )


class DateTimeRangeTests(_RangeTypeMixin, fixtures.TablesTest):

    _col_type = TSRANGE
    _col_str = 'TSRANGE'
    _data_str = '[2013-03-23 14:30,2013-03-23 23:30)'

    def _data_obj(self):
        return self.extras().DateTimeRange(
            datetime.datetime(2013, 3, 23, 14, 30),
            datetime.datetime(2013, 3, 23, 23, 30)
        )


class DateTimeTZRangeTests(_RangeTypeMixin, fixtures.TablesTest):

    _col_type = TSTZRANGE
    _col_str = 'TSTZRANGE'

    # make sure we use one, steady timestamp with timezone pair
    # for all parts of all these tests
    _tstzs = None

    def tstzs(self):
        if self._tstzs is None:
            lower = testing.db.connect().scalar(
                func.current_timestamp().select()
            )
            upper = lower + datetime.timedelta(1)
            self._tstzs = (lower, upper)
        return self._tstzs

    @property
    def _data_str(self):
        return '[%s,%s)' % self.tstzs()

    def _data_obj(self):
        return self.extras().DateTimeTZRange(*self.tstzs())


class JSONTest(AssertsCompiledSQL, fixtures.TestBase):
    __dialect__ = 'postgresql'

    def setup(self):
        metadata = MetaData()
        self.test_table = Table('test_table', metadata,
                                Column('id', Integer, primary_key=True),
                                Column('test_column', JSON),
                                )
        self.jsoncol = self.test_table.c.test_column

    def _test_where(self, whereclause, expected):
        stmt = select([self.test_table]).where(whereclause)
        self.assert_compile(
            stmt,
            "SELECT test_table.id, test_table.test_column FROM test_table "
            "WHERE %s" % expected
        )

    def _test_cols(self, colclause, expected, from_=True):
        stmt = select([colclause])
        self.assert_compile(
            stmt,
            (
                "SELECT %s" +
                (" FROM test_table" if from_ else "")
            ) % expected
        )

    def test_bind_serialize_default(self):
        dialect = postgresql.dialect()
        proc = self.test_table.c.test_column.type._cached_bind_processor(
            dialect)
        eq_(
            proc({"A": [1, 2, 3, True, False]}),
            '{"A": [1, 2, 3, true, false]}'
        )

    def test_bind_serialize_None(self):
        dialect = postgresql.dialect()
        proc = self.test_table.c.test_column.type._cached_bind_processor(
            dialect)
        eq_(
            proc(None),
            'null'
        )

    def test_bind_serialize_none_as_null(self):
        dialect = postgresql.dialect()
        proc = JSON(none_as_null=True)._cached_bind_processor(
            dialect)
        eq_(
            proc(None),
            None
        )
        eq_(
            proc(null()),
            None
        )

    def test_bind_serialize_null(self):
        dialect = postgresql.dialect()
        proc = self.test_table.c.test_column.type._cached_bind_processor(
            dialect)
        eq_(
            proc(null()),
            None
        )

    def test_result_deserialize_default(self):
        dialect = postgresql.dialect()
        proc = self.test_table.c.test_column.type._cached_result_processor(
            dialect, None)
        eq_(
            proc('{"A": [1, 2, 3, true, false]}'),
            {"A": [1, 2, 3, True, False]}
        )

    def test_result_deserialize_null(self):
        dialect = postgresql.dialect()
        proc = self.test_table.c.test_column.type._cached_result_processor(
            dialect, None)
        eq_(
            proc('null'),
            None
        )

    def test_result_deserialize_None(self):
        dialect = postgresql.dialect()
        proc = self.test_table.c.test_column.type._cached_result_processor(
            dialect, None)
        eq_(
            proc(None),
            None
        )

    # This test is a bit misleading -- in real life you will need to cast to
    # do anything
    def test_where_getitem(self):
        self._test_where(
            self.jsoncol['bar'] == None,
            "(test_table.test_column -> %(test_column_1)s) IS NULL"
        )

    def test_where_path(self):
        self._test_where(
            self.jsoncol[("foo", 1)] == None,
            "(test_table.test_column #> %(test_column_1)s) IS NULL"
        )

    def test_where_getitem_as_text(self):
        self._test_where(
            self.jsoncol['bar'].astext == None,
            "(test_table.test_column ->> %(test_column_1)s) IS NULL"
        )

    def test_where_getitem_as_cast(self):
        self._test_where(
            self.jsoncol['bar'].cast(Integer) == 5,
            "CAST(test_table.test_column ->> %(test_column_1)s AS INTEGER) "
            "= %(param_1)s"
        )

    def test_where_path_as_text(self):
        self._test_where(
            self.jsoncol[("foo", 1)].astext == None,
            "(test_table.test_column #>> %(test_column_1)s) IS NULL"
        )

    def test_cols_get(self):
        self._test_cols(
            self.jsoncol['foo'],
            "test_table.test_column -> %(test_column_1)s AS anon_1",
            True
        )


class JSONRoundTripTest(fixtures.TablesTest):
    __only_on__ = ('postgresql >= 9.3',)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table('data_table', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(30), nullable=False),
              Column('data', JSON),
              Column('nulldata', JSON(none_as_null=True))
              )

    def _fixture_data(self, engine):
        data_table = self.tables.data_table
        engine.execute(
            data_table.insert(),
            {'name': 'r1', 'data': {"k1": "r1v1", "k2": "r1v2"}},
            {'name': 'r2', 'data': {"k1": "r2v1", "k2": "r2v2"}},
            {'name': 'r3', 'data': {"k1": "r3v1", "k2": "r3v2"}},
            {'name': 'r4', 'data': {"k1": "r4v1", "k2": "r4v2"}},
            {'name': 'r5', 'data': {"k1": "r5v1", "k2": "r5v2", "k3": 5}},
        )

    def _assert_data(self, compare, column='data'):
        col = self.tables.data_table.c[column]

        data = testing.db.execute(
            select([col]).
            order_by(self.tables.data_table.c.name)
        ).fetchall()
        eq_([d for d, in data], compare)

    def _assert_column_is_NULL(self, column='data'):
        col = self.tables.data_table.c[column]

        data = testing.db.execute(
            select([col]).
            where(col.is_(null()))
        ).fetchall()
        eq_([d for d, in data], [None])

    def _test_insert(self, engine):
        engine.execute(
            self.tables.data_table.insert(),
            {'name': 'r1', 'data': {"k1": "r1v1", "k2": "r1v2"}}
        )
        self._assert_data([{"k1": "r1v1", "k2": "r1v2"}])

    def _test_insert_nulls(self, engine):
        engine.execute(
            self.tables.data_table.insert(),
            {'name': 'r1', 'data': null()}
        )
        self._assert_data([None])

    def _test_insert_none_as_null(self, engine):
        engine.execute(
            self.tables.data_table.insert(),
            {'name': 'r1', 'nulldata': None}
        )
        self._assert_column_is_NULL(column='nulldata')

    def _non_native_engine(self, json_serializer=None, json_deserializer=None):
        if json_serializer is not None or json_deserializer is not None:
            options = {
                "json_serializer": json_serializer,
                "json_deserializer": json_deserializer
            }
        else:
            options = {}

        if testing.against("postgresql+psycopg2"):
            from psycopg2.extras import register_default_json
            engine = engines.testing_engine(options=options)

            @event.listens_for(engine, "connect")
            def connect(dbapi_connection, connection_record):
                engine.dialect._has_native_json = False

                def pass_(value):
                    return value
                register_default_json(dbapi_connection, loads=pass_)
        elif options:
            engine = engines.testing_engine(options=options)
        else:
            engine = testing.db
        engine.connect()
        return engine

    def test_reflect(self):
        insp = inspect(testing.db)
        cols = insp.get_columns('data_table')
        assert isinstance(cols[2]['type'], JSON)

    @testing.only_on("postgresql+psycopg2")
    def test_insert_native(self):
        engine = testing.db
        self._test_insert(engine)

    @testing.only_on("postgresql+psycopg2")
    def test_insert_native_nulls(self):
        engine = testing.db
        self._test_insert_nulls(engine)

    @testing.only_on("postgresql+psycopg2")
    def test_insert_native_none_as_null(self):
        engine = testing.db
        self._test_insert_none_as_null(engine)

    def test_insert_python(self):
        engine = self._non_native_engine()
        self._test_insert(engine)

    def test_insert_python_nulls(self):
        engine = self._non_native_engine()
        self._test_insert_nulls(engine)

    def test_insert_python_none_as_null(self):
        engine = self._non_native_engine()
        self._test_insert_none_as_null(engine)

    def _test_custom_serialize_deserialize(self, native):
        import json

        def loads(value):
            value = json.loads(value)
            value['x'] = value['x'] + '_loads'
            return value

        def dumps(value):
            value = dict(value)
            value['x'] = 'dumps_y'
            return json.dumps(value)

        if native:
            engine = engines.testing_engine(options=dict(
                json_serializer=dumps,
                json_deserializer=loads
            ))
        else:
            engine = self._non_native_engine(
                json_serializer=dumps,
                json_deserializer=loads
            )

        s = select([
            cast(
                {
                    "key": "value",
                    "x": "q"
                },
                JSON
            )
        ])
        eq_(
            engine.scalar(s),
            {
                "key": "value",
                "x": "dumps_y_loads"
            },
        )

    @testing.only_on("postgresql+psycopg2")
    def test_custom_native(self):
        self._test_custom_serialize_deserialize(True)

    @testing.only_on("postgresql+psycopg2")
    def test_custom_python(self):
        self._test_custom_serialize_deserialize(False)

    @testing.only_on("postgresql+psycopg2")
    def test_criterion_native(self):
        engine = testing.db
        self._fixture_data(engine)
        self._test_criterion(engine)

    def test_criterion_python(self):
        engine = self._non_native_engine()
        self._fixture_data(engine)
        self._test_criterion(engine)

    def test_path_query(self):
        engine = testing.db
        self._fixture_data(engine)
        data_table = self.tables.data_table
        result = engine.execute(
            select([data_table.c.data]).where(
                data_table.c.data[('k1',)].astext == 'r3v1'
            )
        ).first()
        eq_(result, ({'k1': 'r3v1', 'k2': 'r3v2'},))

    def test_query_returned_as_text(self):
        engine = testing.db
        self._fixture_data(engine)
        data_table = self.tables.data_table
        result = engine.execute(
            select([data_table.c.data['k1'].astext])
        ).first()
        assert isinstance(result[0], util.text_type)

    def test_query_returned_as_int(self):
        engine = testing.db
        self._fixture_data(engine)
        data_table = self.tables.data_table
        result = engine.execute(
            select([data_table.c.data['k3'].cast(Integer)]).where(
                data_table.c.name == 'r5')
        ).first()
        assert isinstance(result[0], int)

    def _test_criterion(self, engine):
        data_table = self.tables.data_table
        result = engine.execute(
            select([data_table.c.data]).where(
                data_table.c.data['k1'].astext == 'r3v1'
            )
        ).first()
        eq_(result, ({'k1': 'r3v1', 'k2': 'r3v2'},))

    def _test_fixed_round_trip(self, engine):
        s = select([
            cast(
                {
                    "key": "value",
                    "key2": {"k1": "v1", "k2": "v2"}
                },
                JSON
            )
        ])
        eq_(
            engine.scalar(s),
            {
                "key": "value",
                "key2": {"k1": "v1", "k2": "v2"}
            },
        )

    def test_fixed_round_trip_python(self):
        engine = self._non_native_engine()
        self._test_fixed_round_trip(engine)

    @testing.only_on("postgresql+psycopg2")
    def test_fixed_round_trip_native(self):
        engine = testing.db
        self._test_fixed_round_trip(engine)

    def _test_unicode_round_trip(self, engine):
        s = select([
            cast(
                {
                    util.u('réveillé'): util.u('réveillé'),
                    "data": {"k1": util.u('drôle')}
                },
                JSON
            )
        ])
        eq_(
            engine.scalar(s),
            {
                util.u('réveillé'): util.u('réveillé'),
                "data": {"k1": util.u('drôle')}
            },
        )

    def test_unicode_round_trip_python(self):
        engine = self._non_native_engine()
        self._test_unicode_round_trip(engine)

    @testing.only_on("postgresql+psycopg2")
    def test_unicode_round_trip_native(self):
        engine = testing.db
        self._test_unicode_round_trip(engine)


class JSONBTest(JSONTest):

    def setup(self):
        metadata = MetaData()
        self.test_table = Table('test_table', metadata,
                                Column('id', Integer, primary_key=True),
                                Column('test_column', JSONB)
                                )
        self.jsoncol = self.test_table.c.test_column

    # Note - add fixture data for arrays []

    def test_where_has_key(self):
        self._test_where(
            # hide from 2to3
            getattr(self.jsoncol, 'has_key')('data'),
            "test_table.test_column ? %(test_column_1)s"
        )

    def test_where_has_all(self):
        self._test_where(
            self.jsoncol.has_all(
                {'name': 'r1', 'data': {"k1": "r1v1", "k2": "r1v2"}}),
            "test_table.test_column ?& %(test_column_1)s")

    def test_where_has_any(self):
        self._test_where(
            self.jsoncol.has_any(postgresql.array(['name', 'data'])),
            "test_table.test_column ?| ARRAY[%(param_1)s, %(param_2)s]"
        )

    def test_where_contains(self):
        self._test_where(
            self.jsoncol.contains({"k1": "r1v1"}),
            "test_table.test_column @> %(test_column_1)s"
        )

    def test_where_contained_by(self):
        self._test_where(
            self.jsoncol.contained_by({'foo': '1', 'bar': None}),
            "test_table.test_column <@ %(test_column_1)s"
        )


class JSONBRoundTripTest(JSONRoundTripTest):
    __only_on__ = ('postgresql >= 9.4',)
