from test.lib.testing import eq_
from sqlalchemy import *
from test.lib import *
from test.lib.schema import Table, Column
from sqlalchemy.types import TypeDecorator
from test.lib import fixtures

class ReturningTest(fixtures.TestBase, AssertsExecutionResults):
    __requires__ = 'returning',

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

        table = Table('tables', meta,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('persons', Integer),
            Column('full', Boolean),
            Column('goofy', GoofyType(50))
        )
        table.create(checkfirst=True)

    def teardown(self):
        table.drop()

    @testing.exclude('firebird', '<', (2, 0), '2.0+ feature')
    @testing.exclude('postgresql', '<', (8, 2), '8.2+ feature')
    def test_column_targeting(self):
        result = table.insert().returning(table.c.id, table.c.full).execute({'persons': 1, 'full': False})

        row = result.first()
        assert row[table.c.id] == row['id'] == 1
        assert row[table.c.full] == row['full'] == False

        result = table.insert().values(persons=5, full=True, goofy="somegoofy").\
                            returning(table.c.persons, table.c.full, table.c.goofy).execute()
        row = result.first()
        assert row[table.c.persons] == row['persons'] == 5
        assert row[table.c.full] == row['full'] == True

        eq_(row[table.c.goofy], row['goofy'])
        eq_(row['goofy'], "FOOsomegoofyBAR")

    @testing.fails_on('firebird', "fb can't handle returning x AS y")
    @testing.exclude('firebird', '<', (2, 0), '2.0+ feature')
    @testing.exclude('postgresql', '<', (8, 2), '8.2+ feature')
    def test_labeling(self):
        result = table.insert().values(persons=6).\
                            returning(table.c.persons.label('lala')).execute()
        row = result.first()
        assert row['lala'] == 6

    @testing.fails_on('firebird', "fb/kintersbasdb can't handle the bind params")
    @testing.fails_on('oracle+zxjdbc', "JDBC driver bug")
    @testing.exclude('firebird', '<', (2, 0), '2.0+ feature')
    @testing.exclude('postgresql', '<', (8, 2), '8.2+ feature')
    def test_anon_expressions(self):
        result = table.insert().values(goofy="someOTHERgoofy").\
                            returning(func.lower(table.c.goofy, type_=GoofyType)).execute()
        row = result.first()
        assert row[0] == "foosomeothergoofyBAR"

        result = table.insert().values(persons=12).\
                            returning(table.c.persons + 18).execute()
        row = result.first()
        assert row[0] == 30

    @testing.exclude('firebird', '<', (2, 1), '2.1+ feature')
    @testing.exclude('postgresql', '<', (8, 2), '8.2+ feature')
    def test_update_returning(self):
        table.insert().execute([{'persons': 5, 'full': False}, {'persons': 3, 'full': False}])

        result = table.update(table.c.persons > 4, dict(full=True)).returning(table.c.id).execute()
        eq_(result.fetchall(), [(1,)])

        result2 = select([table.c.id, table.c.full]).order_by(table.c.id).execute()
        eq_(result2.fetchall(), [(1,True),(2,False)])

    @testing.exclude('firebird', '<', (2, 0), '2.0+ feature')
    @testing.exclude('postgresql', '<', (8, 2), '8.2+ feature')
    def test_insert_returning(self):
        result = table.insert().returning(table.c.id).execute({'persons': 1, 'full': False})

        eq_(result.fetchall(), [(1,)])

        @testing.fails_on('postgresql', '')
        @testing.fails_on('oracle+cx_oracle', '')
        @testing.crashes('mssql+mxodbc', 'Raises an error')
        def test_executemany():
            # return value is documented as failing with psycopg2/executemany
            result2 = table.insert().returning(table).execute(
                 [{'persons': 2, 'full': False}, {'persons': 3, 'full': True}])

            if testing.against('mssql+zxjdbc'):
                # jtds apparently returns only the first row
                eq_(result2.fetchall(), [(2, 2, False, None)])
            elif testing.against('firebird', 'mssql', 'oracle'):
                # Multiple inserts only return the last row
                eq_(result2.fetchall(), [(3, 3, True, None)])
            else:
                # nobody does this as far as we know (pg8000?)
                eq_(result2.fetchall(), [(2, 2, False, None), (3, 3, True, None)])

        test_executemany()



    @testing.exclude('firebird', '<', (2, 1), '2.1+ feature')
    @testing.exclude('postgresql', '<', (8, 2), '8.2+ feature')
    @testing.fails_on_everything_except('postgresql', 'firebird')
    def test_literal_returning(self):
        if testing.against("postgresql"):
            literal_true = "true"
        else:
            literal_true = "1"

        result4 = testing.db.execute('insert into tables (id, persons, "full") '
                                        'values (5, 10, %s) returning persons' % literal_true)
        eq_([dict(row) for row in result4], [{'persons': 10}])

    @testing.exclude('firebird', '<', (2, 1), '2.1+ feature')
    @testing.exclude('postgresql', '<', (8, 2), '8.2+ feature')
    def test_delete_returning(self):
        table.insert().execute([{'persons': 5, 'full': False}, {'persons': 3, 'full': False}])

        result = table.delete(table.c.persons > 4).returning(table.c.id).execute()
        eq_(result.fetchall(), [(1,)])

        result2 = select([table.c.id, table.c.full]).order_by(table.c.id).execute()
        eq_(result2.fetchall(), [(2,False),])

class SequenceReturningTest(fixtures.TestBase):
    __requires__ = 'returning',

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

    def setup(self):
        meta = MetaData(testing.db)
        global table

        table = Table('tables', meta,
            Column('id', Integer, primary_key=True, key='foo_id', test_needs_autoincrement=True),
            Column('data', String(20)),
        )
        table.create(checkfirst=True)

    def teardown(self):
        table.drop()

    @testing.exclude('firebird', '<', (2, 0), '2.0+ feature')
    @testing.exclude('postgresql', '<', (8, 2), '8.2+ feature')
    def test_insert(self):
        result = table.insert().returning(table.c.foo_id).execute(data='somedata')
        row = result.first()
        assert row[table.c.foo_id] == row['id'] == 1

        result = table.select().execute().first()
        assert row[table.c.foo_id] == row['id'] == 1


class ImplicitReturningFlag(fixtures.TestBase):
    def test_flag_turned_off(self):
        e = engines.testing_engine(options={'implicit_returning':False})
        assert e.dialect.implicit_returning is False
        c = e.connect()
        assert e.dialect.implicit_returning is False

    def test_flag_turned_on(self):
        e = engines.testing_engine(options={'implicit_returning':True})
        assert e.dialect.implicit_returning is True
        c = e.connect()
        assert e.dialect.implicit_returning is True

    def test_flag_turned_default(self):
        supports = [False]
        def go():
            supports[0] = True
        testing.requires.returning(go)()
        e = engines.testing_engine()

        # starts as False.  This is because all of Firebird,
        # Postgresql, Oracle, SQL Server started supporting RETURNING
        # as of a certain version, and the flag is not set until
        # version detection occurs.  If some DB comes along that has 
        # RETURNING in all cases, this test can be adjusted.
        assert e.dialect.implicit_returning is False 

        # version detection on connect sets it
        c = e.connect()
        assert e.dialect.implicit_returning is supports[0]
