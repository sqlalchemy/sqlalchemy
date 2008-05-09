import testenv; testenv.configure_for_tests()
from testlib import sa, testing
from testlib.sa import Table, Column, Integer, String, ForeignKey
from testlib.sa.orm import mapper, relation
from orm import _base


class O2OTest(_base.MappedTest):
    def define_tables(self, metadata):
        Table('jack', metadata,
              Column('id', Integer, primary_key=True),
              Column('number', String(50)),
              Column('status', String(20)),
              Column('subroom', String(5)))

        Table('port', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(30)),
              Column('description', String(100)),
              Column('jack_id', Integer, ForeignKey("jack.id")))

    def setup_mappers(self):
        class Jack(_base.BasicEntity):
            pass
        class Port(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_1(self):
        ctx = sa.orm.scoped_session(sa.orm.create_session)

        mapper(Port, port, extension=ctx.extension)
        mapper(Jack, jack,
               order_by=[jack.c.number],
               properties=dict(
                   port=relation(Port, backref='jack',
                                 uselist=False, lazy=True)),
               extension=ctx.extension)

        j = Jack(number='101')
        p = Port(name='fa0/1')
        j.port=p
        ctx.flush()
        jid = j.id
        pid = p.id

        j=ctx.query(Jack).get(jid)
        p=ctx.query(Port).get(pid)
        assert p.jack is not None
        assert p.jack is  j
        assert j.port is not None
        p.jack = None
        assert j.port is None

        ctx.clear()

        j = ctx.query(Jack).get(jid)
        p = ctx.query(Port).get(pid)

        j.port=None
        self.assert_(p.jack is None)
        ctx.flush()

        ctx.delete(j)
        ctx.flush()

if __name__ == "__main__":
    testenv.main()
