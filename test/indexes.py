from sqlalchemy import *
import sys
import testbase

class IndexTest(testbase.AssertMixin):
    
    def setUp(self):
        self.created = []

    def tearDown(self):
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
        
        i = Index('employee_email_index',
                  employees.c.email_address, unique=True)        
        i.create()
        self.created.append(i)
        
if __name__ == "__main__":    
    testbase.main()
