from sqlalchemy import *
import testbase

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

class O2OTest(testbase.AssertMixin):
    def setUpAll(self):
        global jack, port
        jack = Table('jack', testbase.db, 
            Column('id', Integer, primary_key=True),
            #Column('room_id', Integer, ForeignKey("room.id")),
            Column('number', String(50)),
            Column('status', String(20)),
            Column('subroom', String(5)),
        )


        port = Table('port', testbase.db, 
            Column('id', Integer, primary_key=True),
            #Column('device_id', Integer, ForeignKey("device.id")),
            Column('name', String(30)),
            Column('description', String(100)),
            Column('jack_id', Integer, ForeignKey("jack.id")),
        )
        jack.create()
        port.create()
    def setUp(self):
        objectstore.clear()
    def tearDown(self):
        clear_mappers()
    def tearDownAll(self):
        port.drop()
        jack.drop()
            
    def test1(self):
        assign_mapper(Port, port)
        assign_mapper(Jack, jack, order_by=[jack.c.number],properties = {
            'port': relation(Port.mapper, backref='jack', uselist=False, lazy=True),
        }) 

        j=Jack(number='101')
        p=Port(name='fa0/1')
        j.port=p
        objectstore.commit()
        jid = j.id
        pid = p.id

        j=Jack.get(jid)
        p=Port.get(pid)
        print p.jack
        print j.port
        p.jack=None
        assert j.port is None #works

        objectstore.clear()

        j=Jack.get(jid)
        p=Port.get(pid)

        j.port=None
        self.assert_(p.jack is None)
        objectstore.commit() 

	j.delete()
	objectstore.commit()

if __name__ == "__main__":    
    testbase.main()
