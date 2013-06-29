# coding: utf-8

from sqlalchemy.testing.assertions import AssertsCompiledSQL, is_, assert_raises
from sqlalchemy.testing import engines, fixtures
from sqlalchemy import testing
import datetime
from sqlalchemy import Table, Column, select, MetaData, text, Integer, \
            String, Sequence, ForeignKey, join, Numeric, \
            PrimaryKeyConstraint, DateTime, tuple_, Float, BigInteger, \
            func, literal_column, literal, bindparam, cast, extract, \
            SmallInteger, Enum, REAL, update, insert, Index, delete, \
            and_, Date, TypeDecorator, Time, Unicode, Interval, or_, Text
from sqlalchemy.dialects.postgresql import ExcludeConstraint, array
from sqlalchemy import exc, schema
from sqlalchemy.dialects.postgresql import base as postgresql
from sqlalchemy.dialects.postgresql import TSRANGE
from sqlalchemy.orm import mapper, aliased, Session
from sqlalchemy.sql import table, column, operators

class SequenceTest(fixtures.TestBase, AssertsCompiledSQL):

    def test_format(self):
        seq = Sequence('my_seq_no_schema')
        dialect = postgresql.PGDialect()
        assert dialect.identifier_preparer.format_sequence(seq) \
            == 'my_seq_no_schema'
        seq = Sequence('my_seq', schema='some_schema')
        assert dialect.identifier_preparer.format_sequence(seq) \
            == 'some_schema.my_seq'
        seq = Sequence('My_Seq', schema='Some_Schema')
        assert dialect.identifier_preparer.format_sequence(seq) \
            == '"Some_Schema"."My_Seq"'

    @testing.only_on('postgresql', 'foo')
    @testing.provide_metadata
    def test_reverse_eng_name(self):
        metadata = self.metadata
        engine = engines.testing_engine(options=dict(implicit_returning=False))
        for tname, cname in [
            ('tb1' * 30, 'abc'),
            ('tb2', 'abc' * 30),
            ('tb3' * 30, 'abc' * 30),
            ('tb4', 'abc'),
        ]:
            t = Table(tname[:57],
                metadata,
                Column(cname[:57], Integer, primary_key=True)
            )
            t.create(engine)
            r = engine.execute(t.insert())
            assert r.inserted_primary_key == [1]

class CompileTest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = postgresql.dialect()

    def test_update_returning(self):
        dialect = postgresql.dialect()
        table1 = table('mytable', column('myid', Integer), column('name'
                       , String(128)), column('description',
                       String(128)))
        u = update(table1, values=dict(name='foo'
                   )).returning(table1.c.myid, table1.c.name)
        self.assert_compile(u,
                            'UPDATE mytable SET name=%(name)s '
                            'RETURNING mytable.myid, mytable.name',
                            dialect=dialect)
        u = update(table1, values=dict(name='foo')).returning(table1)
        self.assert_compile(u,
                            'UPDATE mytable SET name=%(name)s '
                            'RETURNING mytable.myid, mytable.name, '
                            'mytable.description', dialect=dialect)
        u = update(table1, values=dict(name='foo'
                   )).returning(func.length(table1.c.name))
        self.assert_compile(u,
                            'UPDATE mytable SET name=%(name)s '
                            'RETURNING length(mytable.name) AS length_1'
                            , dialect=dialect)


    def test_insert_returning(self):
        dialect = postgresql.dialect()
        table1 = table('mytable',
            column('myid', Integer),
            column('name', String(128)),
            column('description', String(128)),
        )

        i = insert(table1, values=dict(name='foo'
                   )).returning(table1.c.myid, table1.c.name)
        self.assert_compile(i,
                            'INSERT INTO mytable (name) VALUES '
                            '(%(name)s) RETURNING mytable.myid, '
                            'mytable.name', dialect=dialect)
        i = insert(table1, values=dict(name='foo')).returning(table1)
        self.assert_compile(i,
                            'INSERT INTO mytable (name) VALUES '
                            '(%(name)s) RETURNING mytable.myid, '
                            'mytable.name, mytable.description',
                            dialect=dialect)
        i = insert(table1, values=dict(name='foo'
                   )).returning(func.length(table1.c.name))
        self.assert_compile(i,
                            'INSERT INTO mytable (name) VALUES '
                            '(%(name)s) RETURNING length(mytable.name) '
                            'AS length_1', dialect=dialect)


    def test_create_partial_index(self):
        m = MetaData()
        tbl = Table('testtbl', m, Column('data', Integer))
        idx = Index('test_idx1', tbl.c.data,
                    postgresql_where=and_(tbl.c.data > 5, tbl.c.data
                    < 10))
        idx = Index('test_idx1', tbl.c.data,
                    postgresql_where=and_(tbl.c.data > 5, tbl.c.data
                    < 10))

        # test quoting and all that

        idx2 = Index('test_idx2', tbl.c.data,
                     postgresql_where=and_(tbl.c.data > 'a', tbl.c.data
                     < "b's"))
        self.assert_compile(schema.CreateIndex(idx),
                            'CREATE INDEX test_idx1 ON testtbl (data) '
                            'WHERE data > 5 AND data < 10',
                            dialect=postgresql.dialect())
        self.assert_compile(schema.CreateIndex(idx2),
                            "CREATE INDEX test_idx2 ON testtbl (data) "
                            "WHERE data > 'a' AND data < 'b''s'",
                            dialect=postgresql.dialect())

    def test_create_index_with_ops(self):
        m = MetaData()
        tbl = Table('testtbl', m,
                    Column('data', String),
                    Column('data2', Integer, key='d2'))

        idx = Index('test_idx1', tbl.c.data,
                    postgresql_ops={'data': 'text_pattern_ops'})

        idx2 = Index('test_idx2', tbl.c.data, tbl.c.d2,
                    postgresql_ops={'data': 'text_pattern_ops',
                                    'd2': 'int4_ops'})

        self.assert_compile(schema.CreateIndex(idx),
                            'CREATE INDEX test_idx1 ON testtbl '
                            '(data text_pattern_ops)',
                            dialect=postgresql.dialect())
        self.assert_compile(schema.CreateIndex(idx2),
                            'CREATE INDEX test_idx2 ON testtbl '
                            '(data text_pattern_ops, data2 int4_ops)',
                            dialect=postgresql.dialect())

    def test_create_index_with_using(self):
        m = MetaData()
        tbl = Table('testtbl', m, Column('data', String))

        idx1 = Index('test_idx1', tbl.c.data)
        idx2 = Index('test_idx2', tbl.c.data, postgresql_using='btree')
        idx3 = Index('test_idx3', tbl.c.data, postgresql_using='hash')

        self.assert_compile(schema.CreateIndex(idx1),
                            'CREATE INDEX test_idx1 ON testtbl '
                            '(data)',
                            dialect=postgresql.dialect())
        self.assert_compile(schema.CreateIndex(idx2),
                            'CREATE INDEX test_idx2 ON testtbl '
                            'USING btree (data)',
                            dialect=postgresql.dialect())
        self.assert_compile(schema.CreateIndex(idx3),
                            'CREATE INDEX test_idx3 ON testtbl '
                            'USING hash (data)',
                            dialect=postgresql.dialect())

    def test_exclude_constraint_min(self):
        m = MetaData()
        tbl = Table('testtbl', m,
                    Column('room', Integer, primary_key=True))
        cons = ExcludeConstraint(('room', '='))
        tbl.append_constraint(cons)
        self.assert_compile(schema.AddConstraint(cons),
                            'ALTER TABLE testtbl ADD EXCLUDE USING gist '
                            '(room WITH =)',
                            dialect=postgresql.dialect())

    def test_exclude_constraint_full(self):
        m = MetaData()
        room = Column('room', Integer, primary_key=True)
        tbl = Table('testtbl', m,
                    room,
                    Column('during', TSRANGE))
        room = Column('room', Integer, primary_key=True)
        cons = ExcludeConstraint((room, '='), ('during', '&&'),
                                 name='my_name',
                                 using='gist',
                                 where="room > 100",
                                 deferrable=True,
                                 initially='immediate')
        tbl.append_constraint(cons)
        self.assert_compile(schema.AddConstraint(cons),
                            'ALTER TABLE testtbl ADD CONSTRAINT my_name '
                            'EXCLUDE USING gist '
                            '(room WITH =, during WITH ''&&) WHERE '
                            '(room > 100) DEFERRABLE INITIALLY immediate',
                            dialect=postgresql.dialect())

    def test_exclude_constraint_copy(self):
        m = MetaData()
        cons = ExcludeConstraint(('room', '='))
        tbl = Table('testtbl', m,
              Column('room', Integer, primary_key=True),
              cons)
        # apparently you can't copy a ColumnCollectionConstraint until
        # after it has been bound to a table...
        cons_copy = cons.copy()
        tbl.append_constraint(cons_copy)
        self.assert_compile(schema.AddConstraint(cons_copy),
                            'ALTER TABLE testtbl ADD EXCLUDE USING gist '
                            '(room WITH =)',
                            dialect=postgresql.dialect())

    def test_substring(self):
        self.assert_compile(func.substring('abc', 1, 2),
                            'SUBSTRING(%(substring_1)s FROM %(substring_2)s '
                            'FOR %(substring_3)s)')
        self.assert_compile(func.substring('abc', 1),
                            'SUBSTRING(%(substring_1)s FROM %(substring_2)s)')




    def test_reserved_words(self):
        table = Table("pg_table", MetaData(),
            Column("col1", Integer),
            Column("variadic", Integer))
        x = select([table.c.col1, table.c.variadic])

        self.assert_compile(x,
            '''SELECT pg_table.col1, pg_table."variadic" FROM pg_table''')

    def test_array(self):
        c = Column('x', postgresql.ARRAY(Integer))

        self.assert_compile(
            cast(c, postgresql.ARRAY(Integer)),
            "CAST(x AS INTEGER[])"
        )
        self.assert_compile(
            c[5],
            "x[%(x_1)s]",
            checkparams={'x_1': 5}
        )

        self.assert_compile(
            c[5:7],
            "x[%(x_1)s:%(x_2)s]",
            checkparams={'x_2': 7, 'x_1': 5}
        )
        self.assert_compile(
            c[5:7][2:3],
            "x[%(x_1)s:%(x_2)s][%(param_1)s:%(param_2)s]",
            checkparams={'x_2': 7, 'x_1': 5, 'param_1':2, 'param_2':3}
        )
        self.assert_compile(
            c[5:7][3],
            "x[%(x_1)s:%(x_2)s][%(param_1)s]",
            checkparams={'x_2': 7, 'x_1': 5, 'param_1':3}
        )

        self.assert_compile(
            c.contains([1]),
            'x @> %(x_1)s',
            checkparams={'x_1': [1]}
        )
        self.assert_compile(
            c.contained_by([2]),
            'x <@ %(x_1)s',
            checkparams={'x_1': [2]}
        )
        self.assert_compile(
            c.overlap([3]),
            'x && %(x_1)s',
            checkparams={'x_1': [3]}
        )
        self.assert_compile(
            postgresql.Any(4, c),
            '%(param_1)s = ANY (x)',
            checkparams={'param_1': 4}
        )
        self.assert_compile(
            c.any(5, operator=operators.ne),
            '%(param_1)s != ANY (x)',
            checkparams={'param_1': 5}
        )
        self.assert_compile(
            postgresql.All(6, c, operator=operators.gt),
            '%(param_1)s > ALL (x)',
            checkparams={'param_1': 6}
        )
        self.assert_compile(
            c.all(7, operator=operators.lt),
            '%(param_1)s < ALL (x)',
            checkparams={'param_1': 7}
        )

    def test_array_literal_type(self):
        is_(postgresql.array([1, 2]).type._type_affinity, postgresql.ARRAY)
        is_(postgresql.array([1, 2]).type.item_type._type_affinity, Integer)

        is_(postgresql.array([1, 2], type_=String).
                    type.item_type._type_affinity, String)

    def test_array_literal(self):
        self.assert_compile(
            func.array_dims(postgresql.array([1, 2]) +
                        postgresql.array([3, 4, 5])),
            "array_dims(ARRAY[%(param_1)s, %(param_2)s] || "
                    "ARRAY[%(param_3)s, %(param_4)s, %(param_5)s])",
            checkparams={'param_5': 5, 'param_4': 4, 'param_1': 1,
                'param_3': 3, 'param_2': 2}
        )

    def test_array_literal_insert(self):
        m = MetaData()
        t = Table('t', m, Column('data', postgresql.ARRAY(Integer)))
        self.assert_compile(
            t.insert().values(data=array([1, 2, 3])),
            "INSERT INTO t (data) VALUES (ARRAY[%(param_1)s, "
                "%(param_2)s, %(param_3)s])"
        )

    def test_update_array_element(self):
        m = MetaData()
        t = Table('t', m, Column('data', postgresql.ARRAY(Integer)))
        self.assert_compile(
            t.update().values({t.c.data[5]: 1}),
            "UPDATE t SET data[%(data_1)s]=%(param_1)s",
            checkparams={'data_1': 5, 'param_1': 1}
        )

    def test_update_array_slice(self):
        m = MetaData()
        t = Table('t', m, Column('data', postgresql.ARRAY(Integer)))
        self.assert_compile(
            t.update().values({t.c.data[2:5]: 2}),
            "UPDATE t SET data[%(data_1)s:%(data_2)s]=%(param_1)s",
            checkparams={'param_1': 2, 'data_2': 5, 'data_1': 2}

        )

    def test_from_only(self):
        m = MetaData()
        tbl1 = Table('testtbl1', m, Column('id', Integer))
        tbl2 = Table('testtbl2', m, Column('id', Integer))

        stmt = tbl1.select().with_hint(tbl1, 'ONLY', 'postgresql')
        expected = 'SELECT testtbl1.id FROM ONLY testtbl1'
        self.assert_compile(stmt, expected)

        talias1 = tbl1.alias('foo')
        stmt = talias1.select().with_hint(talias1, 'ONLY', 'postgresql')
        expected = 'SELECT foo.id FROM ONLY testtbl1 AS foo'
        self.assert_compile(stmt, expected)

        stmt = select([tbl1, tbl2]).with_hint(tbl1, 'ONLY', 'postgresql')
        expected = ('SELECT testtbl1.id, testtbl2.id FROM ONLY testtbl1, '
                    'testtbl2')
        self.assert_compile(stmt, expected)

        stmt = select([tbl1, tbl2]).with_hint(tbl2, 'ONLY', 'postgresql')
        expected = ('SELECT testtbl1.id, testtbl2.id FROM testtbl1, ONLY '
                    'testtbl2')
        self.assert_compile(stmt, expected)

        stmt = select([tbl1, tbl2])
        stmt = stmt.with_hint(tbl1, 'ONLY', 'postgresql')
        stmt = stmt.with_hint(tbl2, 'ONLY', 'postgresql')
        expected = ('SELECT testtbl1.id, testtbl2.id FROM ONLY testtbl1, '
                    'ONLY testtbl2')
        self.assert_compile(stmt, expected)

        stmt = update(tbl1, values=dict(id=1))
        stmt = stmt.with_hint('ONLY', dialect_name='postgresql')
        expected = 'UPDATE ONLY testtbl1 SET id=%(id)s'
        self.assert_compile(stmt, expected)

        stmt = delete(tbl1).with_hint('ONLY', selectable=tbl1, dialect_name='postgresql')
        expected = 'DELETE FROM ONLY testtbl1'
        self.assert_compile(stmt, expected)

        tbl3 = Table('testtbl3', m, Column('id', Integer), schema='testschema')
        stmt = tbl3.select().with_hint(tbl3, 'ONLY', 'postgresql')
        expected = 'SELECT testschema.testtbl3.id FROM ONLY testschema.testtbl3'
        self.assert_compile(stmt, expected)

        assert_raises(
            exc.CompileError,
            tbl3.select().with_hint(tbl3, "FAKE", "postgresql").compile,
            dialect=postgresql.dialect()
        )



class DistinctOnTest(fixtures.TestBase, AssertsCompiledSQL):
    """Test 'DISTINCT' with SQL expression language and orm.Query with
    an emphasis on PG's 'DISTINCT ON' syntax.

    """
    __dialect__ = postgresql.dialect()

    def setup(self):
        self.table = Table('t', MetaData(),
                Column('id',Integer, primary_key=True),
                Column('a', String),
                Column('b', String),
            )

    def test_plain_generative(self):
        self.assert_compile(
            select([self.table]).distinct(),
            "SELECT DISTINCT t.id, t.a, t.b FROM t"
        )

    def test_on_columns_generative(self):
        self.assert_compile(
            select([self.table]).distinct(self.table.c.a),
            "SELECT DISTINCT ON (t.a) t.id, t.a, t.b FROM t"
        )

    def test_on_columns_generative_multi_call(self):
        self.assert_compile(
            select([self.table]).distinct(self.table.c.a).
                distinct(self.table.c.b),
            "SELECT DISTINCT ON (t.a, t.b) t.id, t.a, t.b FROM t"
        )

    def test_plain_inline(self):
        self.assert_compile(
            select([self.table], distinct=True),
            "SELECT DISTINCT t.id, t.a, t.b FROM t"
        )

    def test_on_columns_inline_list(self):
        self.assert_compile(
            select([self.table],
                    distinct=[self.table.c.a, self.table.c.b]).
                    order_by(self.table.c.a, self.table.c.b),
            "SELECT DISTINCT ON (t.a, t.b) t.id, "
            "t.a, t.b FROM t ORDER BY t.a, t.b"
        )

    def test_on_columns_inline_scalar(self):
        self.assert_compile(
            select([self.table], distinct=self.table.c.a),
            "SELECT DISTINCT ON (t.a) t.id, t.a, t.b FROM t"
        )

    def test_query_plain(self):
        sess = Session()
        self.assert_compile(
            sess.query(self.table).distinct(),
            "SELECT DISTINCT t.id AS t_id, t.a AS t_a, "
            "t.b AS t_b FROM t"
        )

    def test_query_on_columns(self):
        sess = Session()
        self.assert_compile(
            sess.query(self.table).distinct(self.table.c.a),
            "SELECT DISTINCT ON (t.a) t.id AS t_id, t.a AS t_a, "
            "t.b AS t_b FROM t"
        )

    def test_query_on_columns_multi_call(self):
        sess = Session()
        self.assert_compile(
            sess.query(self.table).distinct(self.table.c.a).
                    distinct(self.table.c.b),
            "SELECT DISTINCT ON (t.a, t.b) t.id AS t_id, t.a AS t_a, "
            "t.b AS t_b FROM t"
        )

    def test_query_on_columns_subquery(self):
        sess = Session()
        class Foo(object):
            pass
        mapper(Foo, self.table)
        sess = Session()
        self.assert_compile(
            sess.query(Foo).from_self().distinct(Foo.a, Foo.b),
            "SELECT DISTINCT ON (anon_1.t_a, anon_1.t_b) anon_1.t_id "
            "AS anon_1_t_id, anon_1.t_a AS anon_1_t_a, anon_1.t_b "
            "AS anon_1_t_b FROM (SELECT t.id AS t_id, t.a AS t_a, "
            "t.b AS t_b FROM t) AS anon_1"
        )

    def test_query_distinct_on_aliased(self):
        class Foo(object):
            pass
        mapper(Foo, self.table)
        a1 = aliased(Foo)
        sess = Session()
        self.assert_compile(
            sess.query(a1).distinct(a1.a),
            "SELECT DISTINCT ON (t_1.a) t_1.id AS t_1_id, "
            "t_1.a AS t_1_a, t_1.b AS t_1_b FROM t AS t_1"
        )

    def test_distinct_on_subquery_anon(self):

        sq = select([self.table]).alias()
        q = select([self.table.c.id,sq.c.id]).\
                    distinct(sq.c.id).\
                    where(self.table.c.id==sq.c.id)

        self.assert_compile(
            q,
            "SELECT DISTINCT ON (anon_1.id) t.id, anon_1.id "
            "FROM t, (SELECT t.id AS id, t.a AS a, t.b "
            "AS b FROM t) AS anon_1 WHERE t.id = anon_1.id"
            )

    def test_distinct_on_subquery_named(self):
        sq = select([self.table]).alias('sq')
        q = select([self.table.c.id,sq.c.id]).\
                    distinct(sq.c.id).\
                    where(self.table.c.id==sq.c.id)
        self.assert_compile(
            q,
            "SELECT DISTINCT ON (sq.id) t.id, sq.id "
            "FROM t, (SELECT t.id AS id, t.a AS a, "
            "t.b AS b FROM t) AS sq WHERE t.id = sq.id"
            )
