from sqlalchemy import *
import sys
import testbase

class IndexTest(testbase.AssertMixin):
    
    def setUp(self):
        self.created = []
        self.echo = testbase.db.echo
        self.logger = testbase.db.logger
        
    def tearDown(self):
        testbase.db.echo = self.echo
        testbase.db.logger = testbase.db.engine.logger = self.logger
        if self.created:
            self.created.reverse()
            for entity in self.created:
                entity.drop()
    
    def test_index_create(self):
        employees = Table('employees', testbase.db,
                          Column('id', Integer, primary_key=True),
                          Column('first_name', String(30)),
                          Column('last_name', String(30)),
                          Column('email_address', String(30)))
        employees.create()
        self.created.append(employees)
        
        i = Index('employee_name_index',
                  employees.c.last_name, employees.c.first_name)
        i.create()
        self.created.append(i)
        assert employees.indexes['employee_name_index'] is i
        
        i2 = Index('employee_email_index',
                   employees.c.email_address, unique=True)        
        i2.create()
        self.created.append(i2)
        assert employees.indexes['employee_email_index'] is i2

    def test_index_create_camelcase(self):
        """test that mixed-case index identifiers are legal"""
        employees = Table('companyEmployees', testbase.db,
                          Column('id', Integer, primary_key=True),
                          Column('firstName', String),
                          Column('lastName', String),
                          Column('emailAddress', String))        
        employees.create()
        self.created.append(employees)
        
        i = Index('employeeNameIndex',
                  employees.c.lastName, employees.c.firstName)
        i.create()
        self.created.append(i)
        
        i = Index('employeeEmailIndex',
                  employees.c.emailAddress, unique=True)        
        i.create()
        self.created.append(i)

        # Check that the table is useable. This is mostly for pg,
        # which can be somewhat sticky with mixed-case identifiers
        employees.insert().execute(firstName='Joe', lastName='Smith')
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
        
        events = Table('events', testbase.db,
                       Column('id', Integer, primary_key=True),
                       Column('name', String(30), unique=True),
                       Column('location', String(30), index=True),
                       Column('sport', String(30),
                              unique='sport_announcer'),
                       Column('announcer', String(30),
                              unique='sport_announcer'),
                       Column('winner', String(30), index='idx_winners'))
        
        index_names = [ ix.name for ix in events.indexes ]
        assert 'ux_name' in index_names
        assert 'ix_location' in index_names
        assert 'sport_announcer' in index_names
        assert 'idx_winners' in index_names
        assert len(index_names) == 4

        events.create()
        self.created.append(events)

        # verify that the table is functional
        events.insert().execute(id=1, name='hockey finals', location='rink',
                                sport='hockey', announcer='some canadian',
                                winner='sweden')
        ss = events.select().execute().fetchall()
        
        assert capt[0].strip().startswith('CREATE TABLE events')
        assert capt[2].strip() == \
            'CREATE UNIQUE INDEX ux_name ON events (name)'
        assert capt[4].strip() == \
            'CREATE INDEX ix_location ON events (location)'
        assert capt[6].strip() == \
            'CREATE UNIQUE INDEX sport_announcer ON events (sport, announcer)'
        assert capt[8].strip() == \
            'CREATE INDEX idx_winners ON events (winner)'
            
if __name__ == "__main__":    
    testbase.main()
