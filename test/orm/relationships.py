import testbase
import unittest, sys, datetime

db = testbase.db
#db. 

from sqlalchemy import *


class RelationTest(testbase.PersistTest):
    """this is essentially an extension of the "dependency.py" topological sort test.  
    in this test, a table is dependent on two other tables that are otherwise unrelated to each other.
    the dependency sort must insure that this childmost table is below both parent tables in the outcome
    (a bug existed where this was not always the case).
    while the straight topological sort tests should expose this, since the sorting can be different due
    to subtle differences in program execution, this test case was exposing the bug whereas the simpler tests
    were not."""
    def setUpAll(self):
        global tbl_a
        global tbl_b
        global tbl_c
        global tbl_d
        metadata = MetaData()
        tbl_a = Table("tbl_a", metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
        )
        tbl_b = Table("tbl_b", metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
        )
        tbl_c = Table("tbl_c", metadata,
            Column("id", Integer, primary_key=True),
            Column("tbl_a_id", Integer, ForeignKey("tbl_a.id"), nullable=False),
            Column("name", String),
        )
        tbl_d = Table("tbl_d", metadata,
            Column("id", Integer, primary_key=True),
            Column("tbl_c_id", Integer, ForeignKey("tbl_c.id"), nullable=False),
            Column("tbl_b_id", Integer, ForeignKey("tbl_b.id")),
            Column("name", String),
        )
    def setUp(self):
        global session
        session = create_session(bind_to=testbase.db)
        conn = session.connect()
        conn.create(tbl_a)
        conn.create(tbl_b)
        conn.create(tbl_c)
        conn.create(tbl_d)

        class A(object):
            pass
        class B(object):
            pass
        class C(object):
            pass
        class D(object):
            pass

        D.mapper = mapper(D, tbl_d)
        C.mapper = mapper(C, tbl_c, properties=dict(
            d_rows=relation(D, private=True, backref="c_row"),
        ))
        B.mapper = mapper(B, tbl_b)
        A.mapper = mapper(A, tbl_a, properties=dict(
            c_rows=relation(C, private=True, backref="a_row"),
        ))
        D.mapper.add_property("b_row", relation(B))

        global a
        global c
        a = A(); a.name = "a1"
        b = B(); b.name = "b1"
        c = C(); c.name = "c1"; c.a_row = a
        # we must have more than one d row or it won't fail
        d1 = D(); d1.name = "d1"; d1.b_row = b; d1.c_row = c
        d2 = D(); d2.name = "d2"; d2.b_row = b; d2.c_row = c
        d3 = D(); d3.name = "d3"; d3.b_row = b; d3.c_row = c
        session.save_or_update(a)
        session.save_or_update(b)
        
    def tearDown(self):
        conn = session.connect()
        conn.drop(tbl_d)
        conn.drop(tbl_c)
        conn.drop(tbl_b)
        conn.drop(tbl_a)

    def tearDownAll(self):
        testbase.metadata.tables.clear()
    
    def testDeleteRootTable(self):
        session.flush()
        session.delete(a) # works as expected
        session.flush()
        
    def testDeleteMiddleTable(self):
        session.flush()
        session.delete(c) # fails
        session.flush()
        
class RelationTest2(testbase.PersistTest):
    """this test tests a relationship on a column that is included in multiple foreign keys,
    as well as a self-referential relationship on a composite key where one column in the foreign key
    is 'joined to itself'."""
    def setUpAll(self):
        global metadata, company_tbl, employee_tbl
        metadata = BoundMetaData(testbase.db)
        
        company_tbl = Table('company', metadata,
             Column('company_id', Integer, primary_key=True),
             Column('name', Unicode(30)))

        employee_tbl = Table('employee', metadata,
             Column('company_id', Integer, primary_key=True),
             Column('emp_id', Integer, primary_key=True),
             Column('name', Unicode(30)),
            Column('reports_to_id', Integer),
             ForeignKeyConstraint(['company_id'], ['company.company_id']),
             ForeignKeyConstraint(['company_id', 'reports_to_id'],
         ['employee.company_id', 'employee.emp_id']))
        metadata.create_all()
         
    def tearDownAll(self):
        metadata.drop_all()    

    def testexplicit(self):
        """test with mappers that have fairly explicit join conditions"""
        class Company(object):
            pass
        class Employee(object):
            def __init__(self, name, company, emp_id, reports_to=None):
                self.name = name
                self.company = company
                self.emp_id = emp_id
                self.reports_to = reports_to
                
        mapper(Company, company_tbl)
        mapper(Employee, employee_tbl, properties= {
            'company':relation(Company, primaryjoin=employee_tbl.c.company_id==company_tbl.c.company_id, backref='employees'),
            'reports_to':relation(Employee, primaryjoin=
                and_(
                    employee_tbl.c.emp_id==employee_tbl.c.reports_to_id,
                    employee_tbl.c.company_id==employee_tbl.c.company_id
                ), 
                foreignkey=[employee_tbl.c.company_id, employee_tbl.c.emp_id],
                backref='employees')
        })

        sess = create_session()
        c1 = Company()
        c2 = Company()

        e1 = Employee('emp1', c1, 1)
        e2 = Employee('emp2', c1, 2, e1)
        e3 = Employee('emp3', c1, 3, e1)
        e4 = Employee('emp4', c1, 4, e3)
        e5 = Employee('emp5', c2, 1)
        e6 = Employee('emp6', c2, 2, e5)
        e7 = Employee('emp7', c2, 3, e5)

        [sess.save(x) for x in [c1,c2]]
        sess.flush()
        sess.clear()

        test_c1 = sess.query(Company).get(c1.company_id)
        test_e1 = sess.query(Employee).get([c1.company_id, e1.emp_id])
        assert test_e1.name == 'emp1'
        test_e5 = sess.query(Employee).get([c2.company_id, e5.emp_id])
        assert test_e5.name == 'emp5'
        assert [x.name for x in test_e1.employees] == ['emp2', 'emp3']
        assert sess.query(Employee).get([c1.company_id, 3]).reports_to.name == 'emp1'
        assert sess.query(Employee).get([c2.company_id, 3]).reports_to.name == 'emp5'

    def testimplict(self):
        """test with mappers that have the most minimal arguments"""
        class Company(object):
            pass
        class Employee(object):
            def __init__(self, name, company, emp_id, reports_to=None):
                self.name = name
                self.company = company
                self.emp_id = emp_id
                self.reports_to = reports_to

        mapper(Company, company_tbl)
        mapper(Employee, employee_tbl, properties= {
            'company':relation(Company, backref='employees'),
            'reports_to':relation(Employee, 
                foreignkey=[employee_tbl.c.company_id, employee_tbl.c.emp_id],
                backref='employees')
        })

        sess = create_session()
        c1 = Company()
        c2 = Company()

        e1 = Employee('emp1', c1, 1)
        e2 = Employee('emp2', c1, 2, e1)
        e3 = Employee('emp3', c1, 3, e1)
        e4 = Employee('emp4', c1, 4, e3)
        e5 = Employee('emp5', c2, 1)
        e6 = Employee('emp6', c2, 2, e5)
        e7 = Employee('emp7', c2, 3, e5)

        [sess.save(x) for x in [c1,c2]]
        sess.flush()
        sess.clear()

        test_c1 = sess.query(Company).get(c1.company_id)
        test_e1 = sess.query(Employee).get([c1.company_id, e1.emp_id])
        assert test_e1.name == 'emp1'
        test_e5 = sess.query(Employee).get([c2.company_id, e5.emp_id])
        assert test_e5.name == 'emp5'
        assert [x.name for x in test_e1.employees] == ['emp2', 'emp3']
        assert sess.query(Employee).get([c1.company_id, 3]).reports_to.name == 'emp1'
        assert sess.query(Employee).get([c2.company_id, 3]).reports_to.name == 'emp5'
        
        
        
if __name__ == "__main__":
    testbase.main()        
