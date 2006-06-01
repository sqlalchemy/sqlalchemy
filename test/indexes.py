from sqlalchemy import *
import sys
import testbase

class IndexTest(testbase.AssertMixin):
    
    def setUp(self):
	global metadata
	metadata = BoundMetaData(testbase.db)
        self.echo = testbase.db.echo
        self.logger = testbase.db.logger
        
    def tearDown(self):
        testbase.db.echo = self.echo
        testbase.db.logger = testbase.db.engine.logger = self.logger
	metadata.drop_all()
    
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
        assert employees.indexes['employee_name_index'] is i
        
        i2 = Index('employee_email_index',
                   employees.c.email_address, unique=True)        
        i2.create()
        assert employees.indexes['employee_email_index'] is i2

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

        capt = []
        class dummy:
            pass
        stream = dummy()
        stream.write = capt.append
        testbase.db.logger = testbase.db.engine.logger = stream
        events = Table('events', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('name', String(30), unique=True),
                       Column('location', String(30), index=True),
                       Column('sport', String(30),
                              unique='sport_announcer'),
                       Column('announcer', String(30),
                              unique='sport_announcer'),
                       Column('winner', String(30), index='idx_winners'))
        
        index_names = [ ix.name for ix in events.indexes ]
        assert 'ux_events_name' in index_names
        assert 'ix_events_location' in index_names
        assert 'sport_announcer' in index_names
        assert 'idx_winners' in index_names
        assert len(index_names) == 4

        events.create()

        # verify that the table is functional
        events.insert().execute(id=1, name='hockey finals', location='rink',
                                sport='hockey', announcer='some canadian',
                                winner='sweden')
        ss = events.select().execute().fetchall()

        assert capt[0].strip().startswith('CREATE TABLE events')
        assert capt[3].strip() == \
            'CREATE UNIQUE INDEX ux_events_name ON events (name)'
        assert capt[6].strip() == \
            'CREATE INDEX ix_events_location ON events (location)'
        assert capt[9].strip() == \
            'CREATE UNIQUE INDEX sport_announcer ON events (sport, announcer)'
        assert capt[12].strip() == \
            'CREATE INDEX idx_winners ON events (winner)'
            
if __name__ == "__main__":    
    testbase.main()
