from sqlalchemy.testing import eq_
from sqlalchemy import testing
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.types import TypeDecorator
from sqlalchemy.testing import fixtures, AssertsExecutionResults, engines, \
    assert_raises_message
from sqlalchemy import exc as sa_exc
from sqlalchemy import MetaData, String, Integer, Boolean, func, select, \
    Sequence
import itertools

table = GoofyType = seq = None


class ReturningTest(fixtures.TestBase, AssertsExecutionResults):
    __requires__ = 'returning',
    __backend__ = True

    def setup(self):
        meta = MetaData(testing.db)
        global table, GoofyType

        class GoofyType(TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                if value is None:
                    return None
                return "FOO" + value

            def process_result_value(self, value, dialect):
                if value is None:
                    return None
                return value + "BAR"

        table = Table(
            'tables', meta,
            Column(
                'id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column('persons', Integer),
            Column('full', Boolean),
            Column('goofy', GoofyType(50)))
        table.create(checkfirst=True)

    def teardown(self):
        table.drop()

    def test_column_targeting(self):
        result = table.insert().returning(
            table.c.id, table.c.full).execute({'persons': 1, 'full': False})

        row = result.first()
        assert row[table.c.id] == row['id'] == 1
        assert row[table.c.full] == row['full']
        assert row['full'] is False

        result = table.insert().values(
            persons=5, full=True, goofy="somegoofy").\
            returning(table.c.persons, table.c.full, table.c.goofy).execute()
        row = result.first()
        assert row[table.c.persons] == row['persons'] == 5
        assert row[table.c.full] == row['full']

        eq_(row[table.c.goofy], row['goofy'])
        eq_(row['goofy'], "FOOsomegoofyBAR")

    @testing.fails_on('firebird', "fb can't handle returning x AS y")
    def test_labeling(self):
        result = table.insert().values(persons=6).\
            returning(table.c.persons.label('lala')).execute()
        row = result.first()
        assert row['lala'] == 6

    @testing.fails_on(
        'firebird',
        "fb/kintersbasdb can't handle the bind params")
    @testing.fails_on('oracle+zxjdbc', "JDBC driver bug")
    def test_anon_expressions(self):
        result = table.insert().values(goofy="someOTHERgoofy").\
            returning(func.lower(table.c.goofy, type_=GoofyType)).execute()
        row = result.first()
        eq_(row[0], "foosomeothergoofyBAR")

        result = table.insert().values(persons=12).\
            returning(table.c.persons + 18).execute()
        row = result.first()
        eq_(row[0], 30)

    def test_update_returning(self):
        table.insert().execute(
            [{'persons': 5, 'full': False}, {'persons': 3, 'full': False}])

        result = table.update(
            table.c.persons > 4, dict(
                full=True)).returning(
            table.c.id).execute()
        eq_(result.fetchall(), [(1,)])

        result2 = select([table.c.id, table.c.full]).order_by(
            table.c.id).execute()
        eq_(result2.fetchall(), [(1, True), (2, False)])

    def test_insert_returning(self):
        result = table.insert().returning(
            table.c.id).execute({'persons': 1, 'full': False})

        eq_(result.fetchall(), [(1,)])

    @testing.requires.multivalues_inserts
    def test_multirow_returning(self):
        ins = table.insert().returning(table.c.id, table.c.persons).values(
            [
                {'persons': 1, 'full': False},
                {'persons': 2, 'full': True},
                {'persons': 3, 'full': False},
            ]
        )
        result = testing.db.execute(ins)
        eq_(
            result.fetchall(),
            [(1, 1), (2, 2), (3, 3)]
        )

    def test_no_ipk_on_returning(self):
        result = testing.db.execute(
            table.insert().returning(table.c.id),
            {'persons': 1, 'full': False}
        )
        assert_raises_message(
            sa_exc.InvalidRequestError,
            r"Can't call inserted_primary_key when returning\(\) is used.",
            getattr, result, "inserted_primary_key"
        )

    @testing.fails_on_everything_except('postgresql', 'firebird')
    def test_literal_returning(self):
        if testing.against("postgresql"):
            literal_true = "true"
        else:
            literal_true = "1"

        result4 = testing.db.execute(
            'insert into tables (id, persons, "full") '
            'values (5, 10, %s) returning persons' %
            literal_true)
        eq_([dict(row) for row in result4], [{'persons': 10}])

    def test_delete_returning(self):
        table.insert().execute(
            [{'persons': 5, 'full': False}, {'persons': 3, 'full': False}])

        result = table.delete(
            table.c.persons > 4).returning(
            table.c.id).execute()
        eq_(result.fetchall(), [(1,)])

        result2 = select([table.c.id, table.c.full]).order_by(
            table.c.id).execute()
        eq_(result2.fetchall(), [(2, False), ])


class CompositeStatementTest(fixtures.TestBase):
    __requires__ = 'returning',
    __backend__ = True

    @testing.provide_metadata
    def test_select_doesnt_pollute_result(self):
        class MyType(TypeDecorator):
            impl = Integer

            def process_result_value(self, value, dialect):
                raise Exception("I have not been selected")

        t1 = Table(
            't1', self.metadata,
            Column('x', MyType())
        )

        t2 = Table(
            't2', self.metadata,
            Column('x', Integer)
        )

        self.metadata.create_all(testing.db)
        with testing.db.connect() as conn:
            conn.execute(t1.insert().values(x=5))

            stmt = t2.insert().values(
                x=select([t1.c.x]).as_scalar()).returning(t2.c.x)

            result = conn.execute(stmt)
            eq_(result.scalar(), 5)


class SequenceReturningTest(fixtures.TestBase):
    __requires__ = 'returning', 'sequences'
    __backend__ = True

    def setup(self):
        meta = MetaData(testing.db)
        global table, seq
        seq = Sequence('tid_seq')
        table = Table('tables', meta,
                      Column('id', Integer, seq, primary_key=True),
                      Column('data', String(50))
                      )
        table.create(checkfirst=True)

    def teardown(self):
        table.drop()

    def test_insert(self):
        r = table.insert().values(data='hi').returning(table.c.id).execute()
        assert r.first() == (1, )
        assert seq.execute() == 2


class KeyReturningTest(fixtures.TestBase, AssertsExecutionResults):

    """test returning() works with columns that define 'key'."""

    __requires__ = 'returning',
    __backend__ = True

    def setup(self):
        meta = MetaData(testing.db)
        global table

        table = Table(
            'tables',
            meta,
            Column(
                'id',
                Integer,
                primary_key=True,
                key='foo_id',
                test_needs_autoincrement=True),
            Column(
                'data',
                String(20)),
        )
        table.create(checkfirst=True)

    def teardown(self):
        table.drop()

    @testing.exclude('firebird', '<', (2, 0), '2.0+ feature')
    @testing.exclude('postgresql', '<', (8, 2), '8.2+ feature')
    def test_insert(self):
        result = table.insert().returning(
            table.c.foo_id).execute(
            data='somedata')
        row = result.first()
        assert row[table.c.foo_id] == row['id'] == 1

        result = table.select().execute().first()
        assert row[table.c.foo_id] == row['id'] == 1


class ReturnDefaultsTest(fixtures.TablesTest):
    __requires__ = ('returning', )
    run_define_tables = 'each'
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        from sqlalchemy.sql import ColumnElement
        from sqlalchemy.ext.compiler import compiles

        counter = itertools.count()

        class IncDefault(ColumnElement):
            pass

        @compiles(IncDefault)
        def compile(element, compiler, **kw):
            return str(next(counter))

        Table(
            "t1", metadata,
            Column(
                "id", Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column("data", String(50)),
            Column("insdef", Integer, default=IncDefault()),
            Column("upddef", Integer, onupdate=IncDefault()))

    def test_chained_insert_pk(self):
        t1 = self.tables.t1
        result = testing.db.execute(
            t1.insert().values(upddef=1).return_defaults(t1.c.insdef)
        )
        eq_(
            [result.returned_defaults[k] for k in (t1.c.id, t1.c.insdef)],
            [1, 0]
        )

    def test_arg_insert_pk(self):
        t1 = self.tables.t1
        result = testing.db.execute(
            t1.insert(return_defaults=[t1.c.insdef]).values(upddef=1)
        )
        eq_(
            [result.returned_defaults[k] for k in (t1.c.id, t1.c.insdef)],
            [1, 0]
        )

    def test_chained_update_pk(self):
        t1 = self.tables.t1
        testing.db.execute(
            t1.insert().values(upddef=1)
        )
        result = testing.db.execute(t1.update().values(data='d1').
                                    return_defaults(t1.c.upddef))
        eq_(
            [result.returned_defaults[k] for k in (t1.c.upddef,)],
            [1]
        )

    def test_arg_update_pk(self):
        t1 = self.tables.t1
        testing.db.execute(
            t1.insert().values(upddef=1)
        )
        result = testing.db.execute(t1.update(return_defaults=[t1.c.upddef]).
                                    values(data='d1'))
        eq_(
            [result.returned_defaults[k] for k in (t1.c.upddef,)],
            [1]
        )

    def test_insert_non_default(self):
        """test that a column not marked at all as a
        default works with this feature."""

        t1 = self.tables.t1
        result = testing.db.execute(
            t1.insert().values(upddef=1).return_defaults(t1.c.data)
        )
        eq_(
            [result.returned_defaults[k] for k in (t1.c.id, t1.c.data,)],
            [1, None]
        )

    def test_update_non_default(self):
        """test that a column not marked at all as a
        default works with this feature."""

        t1 = self.tables.t1
        testing.db.execute(
            t1.insert().values(upddef=1)
        )
        result = testing.db.execute(
            t1.update(). values(
                upddef=2).return_defaults(
                t1.c.data))
        eq_(
            [result.returned_defaults[k] for k in (t1.c.data,)],
            [None]
        )

    def test_insert_non_default_plus_default(self):
        t1 = self.tables.t1
        result = testing.db.execute(
            t1.insert().values(upddef=1).return_defaults(
                t1.c.data, t1.c.insdef)
        )
        eq_(
            dict(result.returned_defaults),
            {"id": 1, "data": None, "insdef": 0}
        )

    def test_update_non_default_plus_default(self):
        t1 = self.tables.t1
        testing.db.execute(
            t1.insert().values(upddef=1)
        )
        result = testing.db.execute(
            t1.update().
            values(insdef=2).return_defaults(
                t1.c.data, t1.c.upddef))
        eq_(
            dict(result.returned_defaults),
            {"data": None, 'upddef': 1}
        )

    def test_insert_all(self):
        t1 = self.tables.t1
        result = testing.db.execute(
            t1.insert().values(upddef=1).return_defaults()
        )
        eq_(
            dict(result.returned_defaults),
            {"id": 1, "data": None, "insdef": 0}
        )

    def test_update_all(self):
        t1 = self.tables.t1
        testing.db.execute(
            t1.insert().values(upddef=1)
        )
        result = testing.db.execute(
            t1.update().
            values(insdef=2).return_defaults()
        )
        eq_(
            dict(result.returned_defaults),
            {'upddef': 1}
        )


class ImplicitReturningFlag(fixtures.TestBase):
    __backend__ = True

    def test_flag_turned_off(self):
        e = engines.testing_engine(options={'implicit_returning': False})
        assert e.dialect.implicit_returning is False
        c = e.connect()
        c.close()
        assert e.dialect.implicit_returning is False

    def test_flag_turned_on(self):
        e = engines.testing_engine(options={'implicit_returning': True})
        assert e.dialect.implicit_returning is True
        c = e.connect()
        c.close()
        assert e.dialect.implicit_returning is True

    def test_flag_turned_default(self):
        supports = [False]

        def go():
            supports[0] = True
        testing.requires.returning(go)()
        e = engines.testing_engine()

        # starts as False.  This is because all of Firebird,
        # PostgreSQL, Oracle, SQL Server started supporting RETURNING
        # as of a certain version, and the flag is not set until
        # version detection occurs.  If some DB comes along that has
        # RETURNING in all cases, this test can be adjusted.
        assert e.dialect.implicit_returning is False

        # version detection on connect sets it
        c = e.connect()
        c.close()
        assert e.dialect.implicit_returning is supports[0]
