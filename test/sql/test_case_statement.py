from test.lib.testing import assert_raises, assert_raises_message, eq_
import sys
from sqlalchemy import *
from test.lib import *
from sqlalchemy import util, exc
from sqlalchemy.sql import table, column


class CaseTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    @classmethod
    def setup_class(cls):
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
    @classmethod
    def teardown_class(cls):
        info_table.drop()

    @testing.fails_on('firebird', 'FIXME: unknown')
    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.requires.subqueries
    def test_case(self):
        inner = select([case([
                [info_table.c.pk < 3,
                        'lessthan3'],
        [and_(info_table.c.pk >= 3, info_table.c.pk < 7),
                        'gt3']]).label('x'),
        info_table.c.pk, info_table.c.info],
                from_obj=[info_table])

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

        outer = select([inner.alias('q_inner')])

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
                        3],
        [and_(info_table.c.pk >= 3, info_table.c.pk < 6),
                        6]],
                else_ = 0).label('x'),
        info_table.c.pk, info_table.c.info],
                from_obj=[info_table])

        else_result = w_else.execute().fetchall()

        assert else_result == [
            (3, 1, 'pk_1_data'),
            (3, 2, 'pk_2_data'),
            (6, 3, 'pk_3_data'),
            (6, 4, 'pk_4_data'),
            (6, 5, 'pk_5_data'),
            (0, 6, 'pk_6_data')
        ]

    def test_literal_interpretation(self):
        t = table('test', column('col1'))

        assert_raises(exc.ArgumentError, case, [("x", "y")])

        self.assert_compile(case([("x", "y")], value=t.c.col1), "CASE test.col1 WHEN :param_1 THEN :param_2 END")
        self.assert_compile(case([(t.c.col1==7, "y")], else_="z"), "CASE WHEN (test.col1 = :col1_1) THEN :param_1 ELSE :param_2 END")

    def test_text_doesnt_explode(self):

        for s in [
            select([case([(info_table.c.info == 'pk_4_data',
                   text("'yes'"))], else_=text("'no'"
                   ))]).order_by(info_table.c.info),

           select([case([(info_table.c.info == 'pk_4_data',
                  literal_column("'yes'"))], else_=literal_column("'no'"
                  ))]).order_by(info_table.c.info),

        ]:
            eq_(s.execute().fetchall(), [
                (u'no', ), (u'no', ), (u'no', ), (u'yes', ),
                (u'no', ), (u'no', ),
                ])



    @testing.fails_on('firebird', 'FIXME: unknown')
    @testing.fails_on('maxdb', 'FIXME: unknown')
    def testcase_with_dict(self):
        query = select([case({
                    info_table.c.pk < 3: 'lessthan3',
                    info_table.c.pk >= 3: 'gt3',
                }, else_='other'),
                info_table.c.pk, info_table.c.info
            ],
            from_obj=[info_table])
        assert query.execute().fetchall() == [
            ('lessthan3', 1, 'pk_1_data'),
            ('lessthan3', 2, 'pk_2_data'),
            ('gt3', 3, 'pk_3_data'),
            ('gt3', 4, 'pk_4_data'),
            ('gt3', 5, 'pk_5_data'),
            ('gt3', 6, 'pk_6_data')
        ]

        simple_query = select([case({
                    1: 'one',
                    2: 'two',
                }, value=info_table.c.pk, else_='other'),
                info_table.c.pk
            ],
            whereclause=info_table.c.pk < 4,
            from_obj=[info_table])

        assert simple_query.execute().fetchall() == [
            ('one', 1),
            ('two', 2),
            ('other', 3),
        ]

