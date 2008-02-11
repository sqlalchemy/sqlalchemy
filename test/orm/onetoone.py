import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.sessioncontext import SessionContext
from testlib import *

class Jack(object):
    def __repr__(self):
        return "{Jack %s - %s}" % (self.id, self.number)

    def __init__(self, room=None, subroom=None, number=None, status=None):
        self.id = None
        self.room = room
        self.subroom = subroom
        self.number = number
        self.status = status

class Port(object):
    def __repr__(self):
        return "{Port %s - %s}" % (self.id, self.name)

    def __init__(self, name=None, description=None):
        self.id=None
        self.name=name
        self.description = description

class O2OTest(TestBase, AssertsExecutionResults):
    @testing.uses_deprecated('SessionContext')
    def setUpAll(self):
        global jack, port, metadata, ctx
        metadata = MetaData(testing.db)
        ctx = SessionContext(create_session)
        jack = Table('jack', metadata,
            Column('id', Integer, primary_key=True),
            #Column('room_id', Integer, ForeignKey("room.id")),
            Column('number', String(50)),
            Column('status', String(20)),
            Column('subroom', String(5)),
        )


        port = Table('port', metadata,
            Column('id', Integer, primary_key=True),
            #Column('device_id', Integer, ForeignKey("device.id")),
            Column('name', String(30)),
            Column('description', String(100)),
            Column('jack_id', Integer, ForeignKey("jack.id")),
        )
        metadata.create_all()
    def setUp(self):
        pass
    def tearDown(self):
        clear_mappers()
    def tearDownAll(self):
        metadata.drop_all()

    @testing.uses_deprecated('SessionContext')
    def test1(self):
        mapper(Port, port, extension=ctx.mapper_extension)
        mapper(Jack, jack, order_by=[jack.c.number],properties = {
            'port': relation(Port, backref='jack', uselist=False, lazy=True),
        }, extension=ctx.mapper_extension)

        j=Jack(number='101')
        p=Port(name='fa0/1')
        j.port=p
        ctx.current.flush()
        jid = j.id
        pid = p.id

        j=ctx.current.query(Jack).get(jid)
        p=ctx.current.query(Port).get(pid)
        print p.jack
        assert p.jack is not None
        assert p.jack is  j
        assert j.port is not None
        p.jack=None
        assert j.port is None #works

        ctx.current.clear()

        j=ctx.current.query(Jack).get(jid)
        p=ctx.current.query(Port).get(pid)

        j.port=None
        self.assert_(p.jack is None)
        ctx.current.flush()

        ctx.current.delete(j)
        ctx.current.flush()

if __name__ == "__main__":
    testenv.main()
