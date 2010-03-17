import sqlalchemy as sa
from sqlalchemy.test import testing
from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.test.schema import Table
from sqlalchemy.test.schema import Column
from sqlalchemy.orm import mapper, relationship, create_session
from test.orm import _base


class LazyTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('infos', metadata,
              Column('pk', Integer, primary_key=True),
              Column('info', String(128)))

        Table('data', metadata,
              Column('data_pk', Integer, primary_key=True),
              Column('info_pk', Integer,
                     ForeignKey('infos.pk')),
              Column('timeval', Integer),
              Column('data_val', String(128)))

        Table('rels', metadata,
              Column('rel_pk', Integer, primary_key=True),
              Column('info_pk', Integer,
                     ForeignKey('infos.pk')),
              Column('start', Integer),
              Column('finish', Integer))

    @classmethod
    @testing.resolve_artifact_names
    def insert_data(cls):
        infos.insert().execute(
            {'pk':1, 'info':'pk_1_info'},
            {'pk':2, 'info':'pk_2_info'},
            {'pk':3, 'info':'pk_3_info'},
            {'pk':4, 'info':'pk_4_info'},
            {'pk':5, 'info':'pk_5_info'})

        rels.insert().execute(
            {'rel_pk':1, 'info_pk':1, 'start':10, 'finish':19},
            {'rel_pk':2, 'info_pk':1, 'start':100, 'finish':199},
            {'rel_pk':3, 'info_pk':2, 'start':20, 'finish':29},
            {'rel_pk':4, 'info_pk':3, 'start':13, 'finish':23},
            {'rel_pk':5, 'info_pk':5, 'start':15, 'finish':25})

        data.insert().execute(
            {'data_pk':1, 'info_pk':1, 'timeval':11, 'data_val':'11_data'},
            {'data_pk':2, 'info_pk':1, 'timeval':9, 'data_val':'9_data'},
            {'data_pk':3, 'info_pk':1, 'timeval':13, 'data_val':'13_data'},
            {'data_pk':4, 'info_pk':2, 'timeval':23, 'data_val':'23_data'},
            {'data_pk':5, 'info_pk':2, 'timeval':13, 'data_val':'13_data'},
            {'data_pk':6, 'info_pk':1, 'timeval':15, 'data_val':'15_data'})

    @testing.resolve_artifact_names
    def testone(self):
        """A lazy load which has multiple join conditions.

        Including two that are against the same column in the child table.

        """
        class Information(object):
            pass

        class Relationship(object):
            pass

        class Data(object):
            pass

        session = create_session()

        mapper(Data, data)
        mapper(Relationship, rels, properties={
            'datas': relationship(Data,
                              primaryjoin=sa.and_(
                                rels.c.info_pk ==
                                data.c.info_pk,
                                data.c.timeval >= rels.c.start,
                                data.c.timeval <= rels.c.finish),
                              foreign_keys=[data.c.info_pk])})
        mapper(Information, infos, properties={
            'rels': relationship(Relationship)
        })

        info = session.query(Information).get(1)
        assert info
        assert len(info.rels) == 2
        assert len(info.rels[0].datas) == 3


