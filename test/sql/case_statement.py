import testenv; testenv.configure_for_tests()
import sys
from sqlalchemy import *
from testlib import *


class CaseTest(TestBase):

    def setUpAll(self):
        metadata = MetaData(testing.db)
        global info_table
        info_table = Table('infos', metadata,
        Column('pk', Integer, primary_key=True),
        Column('info', String(30)))

        info_table.create()

        info_table.insert().execute(
        {'pk':1, 'info':'pk_1_data'},
        {'pk':2, 'info':'pk_2_data'},
        {'pk':3, 'info':'pk_3_data'},
                {'pk':4, 'info':'pk_4_data'},
                {'pk':5, 'info':'pk_5_data'},
                {'pk':6, 'info':'pk_6_data'})
    def tearDownAll(self):
        info_table.drop()

    @testing.fails_on('maxdb')
    def testcase(self):
        inner = select([case([
                [info_table.c.pk < 3,
                        literal('lessthan3', type_=String)],
        [and_(info_table.c.pk >= 3, info_table.c.pk < 7),
                        literal('gt3', type_=String)]]).label('x'),
        info_table.c.pk, info_table.c.info],
                from_obj=[info_table]).alias('q_inner')

        inner_result = inner.execute().fetchall()

        # Outputs:
        # lessthan3 1 pk_1_data
        # lessthan3 2 pk_2_data
        # gt3 3 pk_3_data
        # gt3 4 pk_4_data
        # gt3 5 pk_5_data
        # gt3 6 pk_6_data
        assert inner_result == [
            ('lessthan3', 1, 'pk_1_data'),
            ('lessthan3', 2, 'pk_2_data'),
            ('gt3', 3, 'pk_3_data'),
            ('gt3', 4, 'pk_4_data'),
            ('gt3', 5, 'pk_5_data'),
            ('gt3', 6, 'pk_6_data')
        ]

        outer = select([inner])

        outer_result = outer.execute().fetchall()

        assert outer_result == [
            ('lessthan3', 1, 'pk_1_data'),
            ('lessthan3', 2, 'pk_2_data'),
            ('gt3', 3, 'pk_3_data'),
            ('gt3', 4, 'pk_4_data'),
            ('gt3', 5, 'pk_5_data'),
            ('gt3', 6, 'pk_6_data')
        ]

        w_else = select([case([
                [info_table.c.pk < 3,
                        literal(3, type_=Integer)],
        [and_(info_table.c.pk >= 3, info_table.c.pk < 6),
                        literal(6, type_=Integer)]],
                else_ = 0).label('x'),
        info_table.c.pk, info_table.c.info],
                from_obj=[info_table]).alias('q_inner')

        else_result = w_else.execute().fetchall()

        assert else_result == [
            (3, 1, 'pk_1_data'),
            (3, 2, 'pk_2_data'),
            (6, 3, 'pk_3_data'),
            (6, 4, 'pk_4_data'),
            (6, 5, 'pk_5_data'),
            (0, 6, 'pk_6_data')
        ]

if __name__ == "__main__":
    testenv.main()
