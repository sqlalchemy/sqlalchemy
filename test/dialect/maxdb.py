"""MaxDB-specific tests."""

import testbase
import StringIO, sys
from sqlalchemy import *
from sqlalchemy import sql
from sqlalchemy.databases import maxdb
from testlib import *


# TODO
# - add "Database" test, a quick check for join behavior on different max versions
# - full max-specific reflection suite
# - datetime tests
# - decimal etc. tests
# - the orm/query 'test_has' destabilizes the server- cover here

class BasicTest(AssertMixin):
    def test_import(self):
        return True

class DBAPITest(AssertMixin):
    """Asserts quirks in the native Python DB-API driver.

    If any of these fail, that's good- the bug is fixed!
    """
    
    @testing.supported('maxdb')
    def test_dbapi_breaks_sequences(self):
        con = testbase.db.connect().connection

        cr = con.cursor()
        cr.execute('CREATE SEQUENCE busto START WITH 1 INCREMENT BY 1')
        try:
            vals = []
            for i in xrange(3):
                cr.execute('SELECT busto.NEXTVAL FROM DUAL')
                vals.append(cr.fetchone()[0])

            # should be 1,2,3, but no...
            self.assert_(vals != [1,2,3])
            # ...we get:
            self.assert_(vals == [2,4,6])
        finally:
            cr.execute('DROP SEQUENCE busto')

    @testing.supported('maxdb')
    def test_dbapi_breaks_mod_binds(self):
        con = testbase.db.connect().connection

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

    @testing.supported('maxdb')
    def test_dbapi_breaks_close(self):
        dialect = testbase.db.dialect
        cargs, ckw = dialect.create_connect_args(testbase.db.url)

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

    @testing.supported('maxdb')
    def test_modulo_operator(self):
        st = str(select([sql.column('col') % 5]).compile(testbase.db))
        self.assertEquals(st, 'SELECT mod(col, ?) FROM DUAL')

if __name__ == "__main__":
    testbase.main()
