import testbase
from sqlalchemy import *
from sqlalchemy.databases import mysql

from testlib import *


class OutParamTest(AssertMixin):
    @testing.supported('oracle')
    def setUpAll(self):
        testbase.db.execute("""
create or replace procedure foo(x_in IN number, x_out OUT number, y_out OUT number) IS
  retval number;
    begin
    retval := 6;
    x_out := 10;
    y_out := x_in * 15;
    end;
        """)

    @testing.supported('oracle')
    def test_out_params(self):
        result = testbase.db.execute(text("begin foo(:x, :y, :z); end;", bindparams=[bindparam('x', Numeric), outparam('y', Numeric), outparam('z', Numeric)]), x=5)
        assert result.out_parameters == {'y':10, 'z':75}, result.out_parameters
        print result.out_parameters

    @testing.supported('oracle')
    def tearDownAll(self):
         testbase.db.execute("DROP PROCEDURE foo")

if __name__ == '__main__':
    testbase.main()
