import testbase
from sqlalchemy import *
import sys

class ConstraintTest(testbase.AssertMixin):
    
    def setUp(self):
        global metadata
        metadata = BoundMetaData(testbase.db)
        
    def tearDown(self):
        metadata.drop_all()
    
    def test_constraint(self):
        employees = Table('employees', metadata, 
            Column('id', Integer),
            Column('soc', String(40)),
            Column('name', String(30)),
            PrimaryKeyConstraint('id', 'soc')
            )
        elements = Table('elements', metadata,
            Column('id', Integer),
            Column('stuff', String(30)),
            Column('emp_id', Integer),
            Column('emp_soc', String(40)),
            PrimaryKeyConstraint('id', name='elements_primkey'),
            ForeignKeyConstraint(['emp_id', 'emp_soc'], ['employees.id', 'employees.soc'])
            )
        metadata.create_all()

    @testbase.unsupported('mysql')
    def test_check_constraint(self):
        foo = Table('foo', metadata, 
            Column('id', Integer, primary_key=True),
            Column('x', Integer),
            Column('y', Integer),
            CheckConstraint('x>y'))
        bar = Table('bar', metadata, 
            Column('id', Integer, primary_key=True),
            Column('x', Integer, CheckConstraint('x>7')),
            Column('z', Integer)
            )

        metadata.create_all()
        foo.insert().execute(id=1,x=9,y=5)
        try:
            foo.insert().execute(id=2,x=5,y=9)
            assert False
        except exceptions.SQLError:
            assert True

        bar.insert().execute(id=1,x=10)
        try:
            bar.insert().execute(id=2,x=5)
            assert False
        except exceptions.SQLError:
            assert True
    
    def test_unique_constraint(self):
        foo = Table('foo', metadata,
            Column('id', Integer, primary_key=True),
            Column('value', String(30), unique=True))
        bar = Table('bar', metadata,
            Column('id', Integer, primary_key=True),
            Column('value', String(30)),
            Column('value2', String(30)),
            UniqueConstraint('value', 'value2', name='uix1')
            )
        metadata.create_all()
        foo.insert().execute(id=1, value='value1')
        foo.insert().execute(id=2, value='value2')
        bar.insert().execute(id=1, value='a', value2='a')
        bar.insert().execute(id=2, value='a', value2='b')
        try:
            foo.insert().execute(id=3, value='value1')
            assert False
        except exceptions.SQLError:
            assert True
        try:
            bar.insert().execute(id=3, value='a', value2='b')
            assert False
        except exceptions.SQLError:
            assert True
        
    def test_index_create(self):
        employees = Table('employees', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('first_name', String(30)),
                          Column('last_name', String(30)),
                          Column('email_address', String(30)))
        employees.create()
        
        i = Index('employee_name_index',
                  employees.c.last_name, employees.c.first_name)
        i.create()
        assert i in employees.indexes
        
        i2 = Index('employee_email_index',
                   employees.c.email_address, unique=True)        
        i2.create()
        assert i2 in employees.indexes

    def test_index_create_camelcase(self):
        """test that mixed-case index identifiers are legal"""
        employees = Table('companyEmployees', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('firstName', String(30)),
                          Column('lastName', String(30)),
                          Column('emailAddress', String(30)))

        employees.create()
        
        i = Index('employeeNameIndex',
                  employees.c.lastName, employees.c.firstName)
        i.create()
        
        i = Index('employeeEmailIndex',
                  employees.c.emailAddress, unique=True)        
        i.create()

        # Check that the table is useable. This is mostly for pg,
        # which can be somewhat sticky with mixed-case identifiers
        employees.insert().execute(firstName='Joe', lastName='Smith', id=0)
        ss = employees.select().execute().fetchall()
        assert ss[0].firstName == 'Joe'
        assert ss[0].lastName == 'Smith'

    def test_index_create_inline(self):
        """Test indexes defined with tables"""

        events = Table('events', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('name', String(30), index=True, unique=True),
                       Column('location', String(30), index=True),
                       Column('sport', String(30)),
                       Column('announcer', String(30)),
                       Column('winner', String(30)))

        Index('sport_announcer', events.c.sport, events.c.announcer, unique=True)
        Index('idx_winners', events.c.winner)
        
        index_names = [ ix.name for ix in events.indexes ]
        assert 'ix_events_name' in index_names
        assert 'ix_events_location' in index_names
        assert 'sport_announcer' in index_names
        assert 'idx_winners' in index_names
        assert len(index_names) == 4

        capt = []
        connection = testbase.db.connect()
        def proxy(statement, parameters):
            capt.append(statement)
            capt.append(repr(parameters))
            connection.proxy(statement, parameters)
        schemagen = testbase.db.dialect.schemagenerator(testbase.db, proxy, connection)
        events.accept_schema_visitor(schemagen)
        
        assert capt[0].strip().startswith('CREATE TABLE events')
        
        s = set([capt[x].strip() for x in [2,4,6,8]])
        
        assert s == set([
            'CREATE UNIQUE INDEX ix_events_name ON events (name)',
            'CREATE INDEX ix_events_location ON events (location)',
            'CREATE UNIQUE INDEX sport_announcer ON events (sport, announcer)',
            'CREATE INDEX idx_winners ON events (winner)'
            ])
            
        # verify that the table is functional
        events.insert().execute(id=1, name='hockey finals', location='rink',
                                sport='hockey', announcer='some canadian',
                                winner='sweden')
        ss = events.select().execute().fetchall()

            
if __name__ == "__main__":    
    testbase.main()
