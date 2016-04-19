import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, create_session
from sqlalchemy.testing import fixtures


class O2OTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('jack', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('number', String(50)),
              Column('status', String(20)),
              Column('subroom', String(5)))

        Table('port', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('name', String(30)),
              Column('description', String(100)),
              Column('jack_id', Integer, ForeignKey("jack.id")))

    @classmethod
    def setup_mappers(cls):
        class Jack(cls.Basic):
            pass
        class Port(cls.Basic):
            pass


    def test_basic(self):
        Port, port, jack, Jack = (self.classes.Port,
                                self.tables.port,
                                self.tables.jack,
                                self.classes.Jack)

        mapper(Port, port)
        mapper(Jack, jack,
               properties=dict(
                   port=relationship(Port, backref='jack',
                                 uselist=False,
                                 )),
               )

        session = create_session()

        j = Jack(number='101')
        session.add(j)
        p = Port(name='fa0/1')
        session.add(p)

        j.port=p
        session.flush()
        jid = j.id
        pid = p.id

        j=session.query(Jack).get(jid)
        p=session.query(Port).get(pid)
        assert p.jack is not None
        assert p.jack is  j
        assert j.port is not None
        p.jack = None
        assert j.port is None

        session.expunge_all()

        j = session.query(Jack).get(jid)
        p = session.query(Port).get(pid)

        j.port=None
        self.assert_(p.jack is None)
        session.flush()

        session.delete(j)
        session.flush()

