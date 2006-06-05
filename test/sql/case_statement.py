import sys
import testbase
from sqlalchemy import *


class CaseTest(testbase.PersistTest):

    def setUpAll(self):
        global info_table
        info_table = Table('infos', testbase.db,
        	Column('pk', Integer, primary_key=True),
        	Column('info', String))

        info_table.create()

        info_table.insert().execute(
        	{'pk':1, 'info':'pk_1_data'},
        	{'pk':2, 'info':'pk_2_data'},
        	{'pk':3, 'info':'pk_3_data'},
        	{'pk':4, 'info':'pk_4_data'},
    	    {'pk':5, 'info':'pk_5_data'})
    def tearDownAll(self):
        info_table.drop()
    
    def testcase(self):
        inner = select([case([[info_table.c.pk < 3, literal('lessthan3', type=String)],
        	[info_table.c.pk >= 3, literal('gt3', type=String)]]).label('x'),
        	info_table.c.pk, info_table.c.info], from_obj=[info_table]).alias('q_inner')

        inner_result = inner.execute().fetchall()

        # Outputs:
        # lessthan3 1 pk_1_data
        # lessthan3 2 pk_2_data
        # gt3 3 pk_3_data
        # gt3 4 pk_4_data
        # gt3 5 pk_5_data
        assert inner_result == [
            ('lessthan3', 1, 'pk_1_data'),
            ('lessthan3', 2, 'pk_2_data'),
            ('gt3', 3, 'pk_3_data'),
            ('gt3', 4, 'pk_4_data'),
            ('gt3', 5, 'pk_5_data'),
        ]

        outer = select([inner])

        outer_result = outer.execute().fetchall()

        assert outer_result == [
            ('lessthan3', 1, 'pk_1_data'),
            ('lessthan3', 2, 'pk_2_data'),
            ('gt3', 3, 'pk_3_data'),
            ('gt3', 4, 'pk_4_data'),
            ('gt3', 5, 'pk_5_data'),
        ]

if __name__ == "__main__":
    testbase.main()
