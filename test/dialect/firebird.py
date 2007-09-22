import testbase
from sqlalchemy import *
from sqlalchemy.databases import firebird
from sqlalchemy.sql import table, column
from testlib import *

class BasicTest(AssertMixin):
    # A simple import of the database/ module should work on all systems.
    def test_import(self):
        # we got this far, right?
        return True



class CompileTest(SQLCompileTest):
    __dialect__ = firebird.FBDialect()

    def test_alias(self):
        t = table('sometable', column('col1'), column('col2'))
        s = select([t.alias()])
        self.assert_compile(s, "SELECT sometable_1.col1, sometable_1.col2 FROM sometable sometable_1")


if __name__ == '__main__':
    testbase.main()
