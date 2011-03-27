"""MaxDB-specific tests."""

from test.lib.testing import eq_
import StringIO, sys
from sqlalchemy import *
from sqlalchemy import exc, sql
from sqlalchemy.util.compat import decimal
from sqlalchemy.databases import maxdb
from test.lib import *


# TODO
# - add "Database" test, a quick check for join behavior on different 
# max versions
# - full max-specific reflection suite
# - datetime tests
# - the orm/query 'test_has' destabilizes the server- cover here

class ReflectionTest(fixtures.TestBase, AssertsExecutionResults):
    """Extra reflection tests."""

    __only_on__ = 'maxdb'

    def _test_decimal(self, tabledef):
        """Checks a variety of FIXED usages.

        This is primarily for SERIAL columns, which can be FIXED (scale-less)
        or (SMALL)INT.  Ensures that FIXED id columns are converted to
        integers and that are assignable as such.  Also exercises general
        decimal assignment and selection behavior.
        """

        meta = MetaData(testing.db)
        try:
            if isinstance(tabledef, basestring):
                # run textual CREATE TABLE
                testing.db.execute(tabledef)
            else:
                _t = tabledef.tometadata(meta)
                _t.create()
            t = Table('dectest', meta, autoload=True)

            vals = [decimal.Decimal('2.2'), decimal.Decimal('23'), decimal.Decimal('2.4'), 25]
            cols = ['d1','d2','n1','i1']
            t.insert().execute(dict(zip(cols,vals)))
            roundtrip = list(t.select().execute())
            eq_(roundtrip, [tuple([1] + vals)])

            t.insert().execute(dict(zip(['id'] + cols,
                                        [2] + list(roundtrip[0][1:]))))
            roundtrip2 = list(t.select(order_by=t.c.id).execute())
            eq_(roundtrip2, [tuple([1] + vals),
                                           tuple([2] + vals)])
        finally:
            try:
                testing.db.execute("DROP TABLE dectest")
            except exc.DatabaseError:
                pass

    def test_decimal_fixed_serial(self):
        tabledef = """
        CREATE TABLE dectest (
          id FIXED(10) DEFAULT SERIAL PRIMARY KEY,
          d1 FIXED(10,2),
          d2 FIXED(12),
          n1 NUMERIC(12,2),
          i1 INTEGER)
          """
        return self._test_decimal(tabledef)

    def test_decimal_integer_serial(self):
        tabledef = """
        CREATE TABLE dectest (
          id INTEGER DEFAULT SERIAL PRIMARY KEY,
          d1 DECIMAL(10,2),
          d2 DECIMAL(12),
          n1 NUMERIC(12,2),
          i1 INTEGER)
          """
        return self._test_decimal(tabledef)

    def test_decimal_implicit_serial(self):
        tabledef = """
        CREATE TABLE dectest (
          id SERIAL PRIMARY KEY,
          d1 FIXED(10,2),
          d2 FIXED(12),
          n1 NUMERIC(12,2),
          i1 INTEGER)
          """
        return self._test_decimal(tabledef)

    def test_decimal_smallint_serial(self):
        tabledef = """
        CREATE TABLE dectest (
          id SMALLINT DEFAULT SERIAL PRIMARY KEY,
          d1 FIXED(10,2),
          d2 FIXED(12),
          n1 NUMERIC(12,2),
          i1 INTEGER)
          """
        return self._test_decimal(tabledef)

    def test_decimal_sa_types_1(self):
        tabledef = Table('dectest', MetaData(),
                         Column('id', Integer, primary_key=True),
                         Column('d1', DECIMAL(10, 2)),
                         Column('d2', DECIMAL(12)),
                         Column('n1', NUMERIC(12,2)),
                         Column('i1', Integer))
        return self._test_decimal(tabledef)

    def test_decimal_sa_types_2(self):
        tabledef = Table('dectest', MetaData(),
                         Column('id', Integer, primary_key=True),
                         Column('d1', maxdb.MaxNumeric(10, 2)),
                         Column('d2', maxdb.MaxNumeric(12)),
                         Column('n1', maxdb.MaxNumeric(12,2)),
                         Column('i1', Integer))
        return self._test_decimal(tabledef)

    def test_decimal_sa_types_3(self):
        tabledef = Table('dectest', MetaData(),
                         Column('id', Integer, primary_key=True),
                         Column('d1', maxdb.MaxNumeric(10, 2)),
                         Column('d2', maxdb.MaxNumeric),
                         Column('n1', maxdb.MaxNumeric(12,2)),
                         Column('i1', Integer))
        return self._test_decimal(tabledef)

    def test_assorted_type_aliases(self):
        """Ensures that aliased types are reflected properly."""

        meta = MetaData(testing.db)
        try:
            testing.db.execute("""
            CREATE TABLE assorted (
              c1 INT,
              c2 BINARY(2),
              c3 DEC(4,2),
              c4 DEC(4),
              c5 DEC,
              c6 DOUBLE PRECISION,
              c7 NUMERIC(4,2),
              c8 NUMERIC(4),
              c9 NUMERIC,
              c10 REAL(4),
              c11 REAL,
              c12 CHARACTER(2))
              """)
            table = Table('assorted', meta, autoload=True)
            expected = [maxdb.MaxInteger,
                        maxdb.MaxNumeric,
                        maxdb.MaxNumeric,
                        maxdb.MaxNumeric,
                        maxdb.MaxNumeric,
                        maxdb.MaxFloat,
                        maxdb.MaxNumeric,
                        maxdb.MaxNumeric,
                        maxdb.MaxNumeric,
                        maxdb.MaxFloat,
                        maxdb.MaxFloat,
                        maxdb.MaxChar,]
            for i, col in enumerate(table.columns):
                self.assert_(isinstance(col.type, expected[i]))
        finally:
            try:
                testing.db.execute("DROP TABLE assorted")
            except exc.DatabaseError:
                pass

class DBAPITest(fixtures.TestBase, AssertsExecutionResults):
    """Asserts quirks in the native Python DB-API driver.

    If any of these fail, that's good- the bug is fixed!
    """

    __only_on__ = 'maxdb'

    def test_dbapi_breaks_sequences(self):
        con = testing.db.connect().connection

        cr = con.cursor()
        cr.execute('CREATE SEQUENCE busto START WITH 1 INCREMENT BY 1')
        try:
            vals = []
            for i in xrange(3):
                cr.execute('SELECT busto.NEXTVAL FROM DUAL')
                vals.append(cr.first()[0])

            # should be 1,2,3, but no...
            self.assert_(vals != [1,2,3])
            # ...we get:
            self.assert_(vals == [2,4,6])
        finally:
            cr.execute('DROP SEQUENCE busto')

    def test_dbapi_breaks_mod_binds(self):
        con = testing.db.connect().connection

        cr = con.cursor()
        # OK
        cr.execute('SELECT MOD(3, 2) FROM DUAL')

        # Broken!
        try:
            cr.execute('SELECT MOD(3, ?) FROM DUAL', [2])
            self.assert_(False)
        except:
            self.assert_(True)

        # OK
        cr.execute('SELECT MOD(?, 2) FROM DUAL', [3])

    def test_dbapi_breaks_close(self):
        dialect = testing.db.dialect
        cargs, ckw = dialect.create_connect_args(testing.db.url)

        # There doesn't seem to be a way to test for this as it occurs in
        # regular usage- the warning doesn't seem to go through 'warnings'.
        con = dialect.dbapi.connect(*cargs, **ckw)
        con.close()
        del con  # <-- exception during __del__

        # But this does the same thing.
        con = dialect.dbapi.connect(*cargs, **ckw)
        self.assert_(con.close == con.__del__)
        con.close()
        try:
            con.close()
            self.assert_(False)
        except dialect.dbapi.DatabaseError:
            self.assert_(True)

    def test_modulo_operator(self):
        st = str(select([sql.column('col') % 5]).compile(testing.db))
        eq_(st, 'SELECT mod(col, ?) FROM DUAL')


