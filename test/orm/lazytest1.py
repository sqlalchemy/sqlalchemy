from testbase import PersistTest, AssertMixin
import testbase
import unittest, sys, os
from sqlalchemy import *
import datetime

class LazyTest(AssertMixin):
    def setUpAll(self):
        global info_table, data_table, rel_table, metadata
        metadata = MetaData(testbase.db)
        info_table = Table('infos', metadata,
        	Column('pk', Integer, primary_key=True),
        	Column('info', String))

        data_table = Table('data', metadata,
        	Column('data_pk', Integer, primary_key=True),
        	Column('info_pk', Integer, ForeignKey(info_table.c.pk)),
        	Column('timeval', Integer),
        	Column('data_val', String))

        rel_table = Table('rels', metadata,
        	Column('rel_pk', Integer, primary_key=True),
        	Column('info_pk', Integer, ForeignKey(info_table.c.pk)),
        	Column('start', Integer),
        	Column('finish', Integer))

        metadata.create_all()
        info_table.insert().execute(
        	{'pk':1, 'info':'pk_1_info'},
        	{'pk':2, 'info':'pk_2_info'},
        	{'pk':3, 'info':'pk_3_info'},
        	{'pk':4, 'info':'pk_4_info'},
        	{'pk':5, 'info':'pk_5_info'})

        rel_table.insert().execute(
        	{'rel_pk':1, 'info_pk':1, 'start':10, 'finish':19},
        	{'rel_pk':2, 'info_pk':1, 'start':100, 'finish':199},
        	{'rel_pk':3, 'info_pk':2, 'start':20, 'finish':29},
        	{'rel_pk':4, 'info_pk':3, 'start':13, 'finish':23},
        	{'rel_pk':5, 'info_pk':5, 'start':15, 'finish':25})

        data_table.insert().execute(
        	{'data_pk':1, 'info_pk':1, 'timeval':11, 'data_val':'11_data'},
        	{'data_pk':2, 'info_pk':1, 'timeval':9, 'data_val':'9_data'},
        	{'data_pk':3, 'info_pk':1, 'timeval':13, 'data_val':'13_data'},
        	{'data_pk':4, 'info_pk':2, 'timeval':23, 'data_val':'23_data'},
        	{'data_pk':5, 'info_pk':2, 'timeval':13, 'data_val':'13_data'},
        	{'data_pk':6, 'info_pk':1, 'timeval':15, 'data_val':'15_data'})


    def tearDownAll(self):
        metadata.drop_all()
    
    def testone(self):
        """tests a lazy load which has multiple join conditions, including two that are against
        the same column in the child table"""
        class Information(object):
        	pass

        class Relation(object):
        	pass

        class Data(object):
        	pass

        session = create_session()
        
        mapper(Data, data_table)
        mapper(Relation, rel_table, properties={
        
            'datas': relation(Data,
            	primaryjoin=and_(rel_table.c.info_pk==data_table.c.info_pk,
            	data_table.c.timeval >= rel_table.c.start,
            	data_table.c.timeval <= rel_table.c.finish),
            	foreignkey=data_table.c.info_pk)
        	}
        	
    	)
        mapper(Information, info_table, properties={
            'rels': relation(Relation)
        })

        info = session.query(Information).get(1)
        assert info
        assert len(info.rels) == 2
        assert len(info.rels[0].datas) == 3

if __name__ == "__main__":    
    testbase.main()


