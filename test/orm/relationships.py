import testbase
import datetime
from sqlalchemy import *
from sqlalchemy import exceptions, types
from sqlalchemy.orm import *
from sqlalchemy.orm import collections
from sqlalchemy.orm.collections import collection
from testlib import *

class RelationTest(PersistTest):
    """this is essentially an extension of the "dependency.py" topological sort test.  
    in this test, a table is dependent on two other tables that are otherwise unrelated to each other.
    the dependency sort must insure that this childmost table is below both parent tables in the outcome
    (a bug existed where this was not always the case).
    while the straight topological sort tests should expose this, since the sorting can be different due
    to subtle differences in program execution, this test case was exposing the bug whereas the simpler tests
    were not."""
    def setUpAll(self):
        global metadata, tbl_a, tbl_b, tbl_c, tbl_d

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
        session = create_session(bind=testbase.db)
        conn = testbase.db.connect()
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
        conn = testbase.db.connect()
        conn.drop(tbl_d)
        conn.drop(tbl_c)
        conn.drop(tbl_b)
        conn.drop(tbl_a)

    def tearDownAll(self):
        metadata.drop_all(testbase.db)
    
    def testDeleteRootTable(self):
        session.flush()
        session.delete(a) # works as expected
        session.flush()
        
    def testDeleteMiddleTable(self):
        session.flush()
        session.delete(c) # fails
        session.flush()
        
class RelationTest2(PersistTest):
    """this test tests a relationship on a column that is included in multiple foreign keys,
    as well as a self-referential relationship on a composite key where one column in the foreign key
    is 'joined to itself'."""
    def setUpAll(self):
        global metadata, company_tbl, employee_tbl
        metadata = MetaData(testbase.db)
        
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

    def testimplicit(self):
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
        
class RelationTest3(PersistTest):
    def setUpAll(self):
        global jobs, pageversions, pages, metadata, Job, Page, PageVersion, PageComment
        import datetime
        metadata = MetaData(testbase.db)  
        jobs = Table("jobs", metadata,
                        Column("jobno", Unicode(15), primary_key=True),
                        Column("created", DateTime, nullable=False, default=datetime.datetime.now),
                        Column("deleted", Boolean, nullable=False, default=False))
        pageversions = Table("pageversions", metadata,
                        Column("jobno", Unicode(15), primary_key=True),
                        Column("pagename", Unicode(30), primary_key=True),
                        Column("version", Integer, primary_key=True, default=1),
                        Column("created", DateTime, nullable=False, default=datetime.datetime.now),
                        Column("md5sum", String(32)),
                        Column("width", Integer, nullable=False, default=0),
                        Column("height", Integer, nullable=False, default=0),
                        ForeignKeyConstraint(["jobno", "pagename"], ["pages.jobno", "pages.pagename"])
                        )
        pages = Table("pages", metadata,
                        Column("jobno", Unicode(15), ForeignKey("jobs.jobno"), primary_key=True),
                        Column("pagename", Unicode(30), primary_key=True),
                        Column("created", DateTime, nullable=False, default=datetime.datetime.now),
                        Column("deleted", Boolean, nullable=False, default=False),
                        Column("current_version", Integer))
        pagecomments = Table("pagecomments", metadata,
            Column("jobno", Unicode(15), primary_key=True),
            Column("pagename", Unicode(30), primary_key=True),
            Column("comment_id", Integer, primary_key=True, autoincrement=False),
            Column("content", Unicode),
            ForeignKeyConstraint(["jobno", "pagename"], ["pages.jobno", "pages.pagename"])
        )

        metadata.create_all()
        class Job(object):
            def __init__(self, jobno=None):
                self.jobno = jobno
            def create_page(self, pagename, *args, **kwargs):
                return Page(job=self, pagename=pagename, *args, **kwargs)
        class PageVersion(object):
            def __init__(self, page=None, version=None):
                self.page = page
                self.version = version
        class Page(object):
            def __init__(self, job=None, pagename=None):
                self.job = job
                self.pagename = pagename
                self.currentversion = PageVersion(self, 1)
            def __repr__(self):
                return "Page jobno:%s pagename:%s %s" % (self.jobno, self.pagename, getattr(self, '_instance_key', None))
            def add_version(self):
                self.currentversion = PageVersion(self, self.currentversion.version+1)
                comment = self.add_comment()
                comment.closeable = False
                comment.content = u'some content'
                return self.currentversion
            def add_comment(self):
                nextnum = max([-1] + [c.comment_id for c in self.comments]) + 1
                newcomment = PageComment()
                newcomment.comment_id = nextnum
                self.comments.append(newcomment)
                newcomment.created_version = self.currentversion.version
                return newcomment
        class PageComment(object):
            pass
        mapper(Job, jobs)
        mapper(PageVersion, pageversions)
        mapper(Page, pages, properties={
            'job': relation(Job, backref=backref('pages', cascade="all, delete-orphan", order_by=pages.c.pagename)),
            'currentversion': relation(PageVersion,
                            foreignkey=pages.c.current_version,
                            primaryjoin=and_(pages.c.jobno==pageversions.c.jobno,
                                             pages.c.pagename==pageversions.c.pagename,
                                             pages.c.current_version==pageversions.c.version),
                            post_update=True),
            'versions': relation(PageVersion, cascade="all, delete-orphan",
                            primaryjoin=and_(pages.c.jobno==pageversions.c.jobno,
                                             pages.c.pagename==pageversions.c.pagename),
                            order_by=pageversions.c.version,
                            backref=backref('page', lazy=False,
                                            primaryjoin=and_(pages.c.jobno==pageversions.c.jobno,
                                                             pages.c.pagename==pageversions.c.pagename)))
        })
        mapper(PageComment, pagecomments, properties={
            'page': relation(Page, primaryjoin=and_(pages.c.jobno==pagecomments.c.jobno,
                                                    pages.c.pagename==pagecomments.c.pagename),
                                backref=backref("comments", cascade="all, delete-orphan",
                                                primaryjoin=and_(pages.c.jobno==pagecomments.c.jobno,
                                                                 pages.c.pagename==pagecomments.c.pagename),
                                                order_by=pagecomments.c.comment_id))
        })


    def tearDownAll(self):
        clear_mappers()
        metadata.drop_all()    

    def testbasic(self):
        """test the combination of complicated join conditions with post_update"""
        j1 = Job('somejob')
        j1.create_page('page1')
        j1.create_page('page2')
        j1.create_page('page3')

        j2 = Job('somejob2')
        j2.create_page('page1')
        j2.create_page('page2')
        j2.create_page('page3')

        j2.pages[0].add_version()
        j2.pages[0].add_version()
        j2.pages[1].add_version()
        print j2.pages
        print j2.pages[0].versions
        print j2.pages[1].versions
        s = create_session()

        s.save(j1)
        s.save(j2)
        s.flush()

        s.clear()
        j = s.query(Job).get_by(jobno='somejob')
        oldp = list(j.pages)
        j.pages = []

        s.flush()

        s.clear()
        j = s.query(Job).get_by(jobno='somejob2')
        j.pages[1].current_version = 12
        s.delete(j)
        s.flush()

class RelationTest4(ORMTest):
    """test syncrules on foreign keys that are also primary"""
    def define_tables(self, metadata):
        global tableA, tableB
        tableA = Table("A", metadata, 
            Column("id",Integer,primary_key=True),
            Column("foo",Integer,),
            )
        tableB = Table("B",metadata,
                Column("id",Integer,ForeignKey("A.id"),primary_key=True),
                )
    def test_no_delete_PK_AtoB(self):
        """test that A cant be deleted without B because B would have no PK value"""
        class A(object):pass
        class B(object):pass
        mapper(A, tableA, properties={
            'bs':relation(B, cascade="save-update")
        })
        mapper(B, tableB)
        a1 = A()
        a1.bs.append(B())
        sess = create_session()
        sess.save(a1)
        sess.flush()
        
        sess.delete(a1)
        try:
            sess.flush()
            assert False
        except exceptions.AssertionError, e:
            assert str(e).startswith("Dependency rule tried to blank-out primary key column 'B.id' on instance ")

    def test_no_delete_PK_BtoA(self):
        class A(object):pass
        class B(object):pass
        mapper(B, tableB, properties={
            'a':relation(A, cascade="save-update")
        })
        mapper(A, tableA)
        b1 = B()
        a1 = A()
        b1.a = a1
        sess = create_session()
        sess.save(b1)
        sess.flush()
        b1.a = None
        try:
            sess.flush()
            assert False
        except exceptions.AssertionError, e:
            assert str(e).startswith("Dependency rule tried to blank-out primary key column 'B.id' on instance ")

    def test_delete_cascade_BtoA(self):
        """test that the 'blank the PK' error doesnt get raised when the child is to be deleted as part of a 
        cascade"""
        class A(object):pass
        class B(object):pass
        for cascade in (
                    "save-update, delete",
                    #"save-update, delete-orphan",
                    "save-update, delete, delete-orphan"):

            mapper(B, tableB, properties={
                'a':relation(A, cascade=cascade)
            })
            mapper(A, tableA)
            b1 = B()
            a1 = A()
            b1.a = a1
            sess = create_session()
            sess.save(b1)
            sess.flush()
            sess.delete(b1)
            sess.flush()
            assert a1 not in sess
            assert b1 not in sess
            sess.clear()
            clear_mappers()
    
    def test_delete_cascade_AtoB(self):
        """test that the 'blank the PK' error doesnt get raised when the child is to be deleted as part of a 
        cascade"""
        class A(object):pass
        class B(object):pass
        for cascade in (
                    "save-update, delete",
                    #"save-update, delete-orphan",
                    "save-update, delete, delete-orphan"):
            mapper(A, tableA, properties={
                'bs':relation(B, cascade=cascade)
            })
            mapper(B, tableB)
            a1 = A()
            b1 = B()
            a1.bs.append(b1)
            sess = create_session()
            sess.save(a1)
            sess.flush()
        
            sess.delete(a1)
            sess.flush()
            assert a1 not in sess
            assert b1 not in sess
            sess.clear()
            clear_mappers()
    
    def test_delete_manual_AtoB(self):
        class A(object):pass
        class B(object):pass
        mapper(A, tableA, properties={
            'bs':relation(B, cascade="none")
        })
        mapper(B, tableB)
        a1 = A()
        b1 = B()
        a1.bs.append(b1)
        sess = create_session()
        sess.save(a1)
        sess.save(b1)
        sess.flush()
    
        sess.delete(a1)
        sess.delete(b1)
        sess.flush()
        assert a1 not in sess
        assert b1 not in sess
        sess.clear()

    def test_delete_manual_BtoA(self):
        class A(object):pass
        class B(object):pass
        mapper(B, tableB, properties={
            'a':relation(A, cascade="none")
        })
        mapper(A, tableA)
        b1 = B()
        a1 = A()
        b1.a = a1
        sess = create_session()
        sess.save(b1)
        sess.save(a1)
        sess.flush()
        sess.delete(b1)
        sess.delete(a1)
        sess.flush()
        assert a1 not in sess
        assert b1 not in sess

class RelationTest5(ORMTest):
    """test a map to a select that relates to a map to the table"""
    def define_tables(self, metadata):
        global items
        items = Table('items', metadata,
            Column('item_policy_num', String(10), primary_key=True, key='policyNum'),
            Column('item_policy_eff_date', Date, primary_key=True, key='policyEffDate'),
            Column('item_type', String(20), primary_key=True, key='type'),
            Column('item_id', Integer, primary_key=True, key='id', autoincrement=False),
        )

    def test_basic(self):
        class Container(object):pass
        class LineItem(object):pass
        
        container_select = select(
            [items.c.policyNum, items.c.policyEffDate, items.c.type],
            distinct=True, 
            ).alias('container_select')

        mapper(LineItem, items)

        mapper(Container, container_select, order_by=asc(container_select.c.type), properties=dict(
            lineItems = relation(LineItem, lazy=True, cascade='all, delete-orphan', order_by=asc(items.c.type),
                primaryjoin=and_(
                    container_select.c.policyNum==items.c.policyNum,
                    container_select.c.policyEffDate==items.c.policyEffDate,
                    container_select.c.type==items.c.type
                ),
                foreign_keys=[
                    items.c.policyNum,
                    items.c.policyEffDate,
                    items.c.type,
                ],
            )
        ))
        session = create_session()
        con = Container()
        con.policyNum = "99"
        con.policyEffDate = datetime.date.today()
        con.type = "TESTER"
        session.save(con)
        for i in range(0, 10):
            li = LineItem()
            li.id = i
            con.lineItems.append(li)
            session.save(li)
        session.flush()
        session.clear()
        newcon = session.query(Container).selectfirst()
        assert con.policyNum == newcon.policyNum
        assert len(newcon.lineItems) == 10
        for old, new in zip(con.lineItems, newcon.lineItems):
            assert old.id == new.id
        
        
class TypeMatchTest(ORMTest):
    """test errors raised when trying to add items whose type is not handled by a relation"""
    def define_tables(self, metadata):
        global a, b, c, d
        a = Table("a", metadata, 
            Column('aid', Integer, primary_key=True),
            Column('data', String(30)))
        b = Table("b", metadata, 
            Column('bid', Integer, primary_key=True),
            Column("a_id", Integer, ForeignKey("a.aid")),
            Column('data', String(30)))
        c = Table("c", metadata, 
            Column('cid', Integer, primary_key=True),
            Column("b_id", Integer, ForeignKey("b.bid")),
            Column('data', String(30)))
        d = Table("d", metadata, 
            Column('did', Integer, primary_key=True),
            Column("a_id", Integer, ForeignKey("a.aid")),
            Column('data', String(30)))
    def test_o2m_oncascade(self):
        class A(object):pass
        class B(object):pass
        class C(object):pass
        mapper(A, a, properties={'bs':relation(B)})
        mapper(B, b)
        mapper(C, c)
        
        a1 = A()
        b1 = B()
        c1 = C()
        a1.bs.append(b1)
        a1.bs.append(c1)
        sess = create_session()
        try:
            sess.save(a1)
            assert False
        except exceptions.AssertionError, err:
            assert str(err) == "Attribute 'bs' on class '%s' doesn't handle objects of type '%s'" % (A, C)
    def test_o2m_onflush(self):
        class A(object):pass
        class B(object):pass
        class C(object):pass
        mapper(A, a, properties={'bs':relation(B, cascade="none")})
        mapper(B, b)
        mapper(C, c)
        
        a1 = A()
        b1 = B()
        c1 = C()
        a1.bs.append(b1)
        a1.bs.append(c1)
        sess = create_session()
        sess.save(a1)
        sess.save(b1)
        sess.save(c1)
        try:
            sess.flush()
            assert False
        except exceptions.FlushError, err:
            assert str(err).startswith("Attempting to flush an item of type %s on collection 'A.bs (B)', which is handled by mapper 'Mapper|B|b' and does not load items of that type.  Did you mean to use a polymorphic mapper for this relationship ?" % C)
    def test_o2m_nopoly_onflush(self):
        class A(object):pass
        class B(object):pass
        class C(B):pass
        mapper(A, a, properties={'bs':relation(B, cascade="none")})
        mapper(B, b)
        mapper(C, c, inherits=B)
        
        a1 = A()
        b1 = B()
        c1 = C()
        a1.bs.append(b1)
        a1.bs.append(c1)
        sess = create_session()
        sess.save(a1)
        sess.save(b1)
        sess.save(c1)
        try:
            sess.flush()
            assert False
        except exceptions.FlushError, err:
            assert str(err).startswith("Attempting to flush an item of type %s on collection 'A.bs (B)', which is handled by mapper 'Mapper|B|b' and does not load items of that type.  Did you mean to use a polymorphic mapper for this relationship ?" % C)
    
    def test_m2o_nopoly_onflush(self):
        class A(object):pass
        class B(A):pass
        class D(object):pass
        mapper(A, a)
        mapper(B, b, inherits=A)
        mapper(D, d, properties={"a":relation(A, cascade="none")})
        b1 = B()
        d1 = D()
        d1.a = b1
        sess = create_session()
        sess.save(b1)
        sess.save(d1)
        try:
            sess.flush()
            assert False
        except exceptions.FlushError, err:
            assert str(err).startswith("Attempting to flush an item of type %s on collection 'D.a (A)', which is handled by mapper 'Mapper|A|a' and does not load items of that type.  Did you mean to use a polymorphic mapper for this relationship ?" % B)
    def test_m2o_oncascade(self):
        class A(object):pass
        class B(object):pass
        class D(object):pass
        mapper(A, a)
        mapper(B, b)
        mapper(D, d, properties={"a":relation(A)})
        b1 = B()
        d1 = D()
        d1.a = b1
        sess = create_session()
        try:
            sess.save(d1)
            assert False
        except exceptions.AssertionError, err:
            assert str(err) == "Attribute 'a' on class '%s' doesn't handle objects of type '%s'" % (D, B)

class TypedAssociationTable(ORMTest):
    def define_tables(self, metadata):
        global t1, t2, t3
        
        class MySpecialType(types.TypeDecorator):
            impl = String
            def convert_bind_param(self, value, dialect):
                return "lala" + value
            def convert_result_value(self, value, dialect):
                return value[4:]
            
        t1 = Table('t1', metadata, 
            Column('col1', MySpecialType(30), primary_key=True),
            Column('col2', String(30)))
        t2 = Table('t2', metadata, 
            Column('col1', MySpecialType(30), primary_key=True),
            Column('col2', String(30)))
        t3 = Table('t3', metadata,
            Column('t1c1', MySpecialType(30), ForeignKey('t1.col1')),
            Column('t2c1', MySpecialType(30), ForeignKey('t2.col1')),
        )
    def testm2m(self):
        """test many-to-many tables with special types for candidate keys"""
        
        class T1(object):pass
        class T2(object):pass
        mapper(T2, t2)
        mapper(T1, t1, properties={
            't2s':relation(T2, secondary=t3, backref='t1s')
        })
        a = T1()
        a.col1 = "aid"
        b = T2()
        b.col1 = "bid"
        c = T2()
        c.col1 = "cid"
        a.t2s.append(b)
        a.t2s.append(c)
        sess = create_session()
        sess.save(a)
        sess.flush()

        assert t3.count().scalar() == 2
        
        a.t2s.remove(c)
        sess.flush()
        
        assert t3.count().scalar() == 1
        
# TODO: move these tests to either attributes.py test or its own module
class CustomCollectionsTest(ORMTest):
    def define_tables(self, metadata):
        global sometable, someothertable
        sometable = Table('sometable', metadata,
            Column('col1',Integer, primary_key=True),
            Column('data', String(30)))
        someothertable = Table('someothertable', metadata, 
            Column('col1', Integer, primary_key=True),
            Column('scol1', Integer, ForeignKey(sometable.c.col1)),
            Column('data', String(20))
        )
    def testbasic(self):
        class MyList(list):
            pass
        class Foo(object):
            pass
        class Bar(object):
            pass
        mapper(Foo, sometable, properties={
            'bars':relation(Bar, collection_class=MyList)
        })
        mapper(Bar, someothertable)
        f = Foo()
        assert isinstance(f.bars, MyList)
    def testlazyload(self):
        """test that a 'set' can be used as a collection and can lazyload."""
        class Foo(object):
            pass
        class Bar(object):
            pass
        mapper(Foo, sometable, properties={
            'bars':relation(Bar, collection_class=set)
        })
        mapper(Bar, someothertable)
        f = Foo()
        f.bars.add(Bar())
        f.bars.add(Bar())
        sess = create_session()
        sess.save(f)
        sess.flush()
        sess.clear()
        f = sess.query(Foo).get(f.col1)
        assert len(list(f.bars)) == 2
        f.bars.clear()
        
    def testdict(self):
        """test that a 'dict' can be used as a collection and can lazyload."""

        class Foo(object):
            pass
        class Bar(object):
            pass
        class AppenderDict(dict):
            @collection.appender
            def set(self, item):
                self[id(item)] = item
            @collection.remover
            def remove(self, item):
                if id(item) in self:
                    del self[id(item)]
                
        mapper(Foo, sometable, properties={
            'bars':relation(Bar, collection_class=AppenderDict)
        })
        mapper(Bar, someothertable)
        f = Foo()
        f.bars.set(Bar())
        f.bars.set(Bar())
        sess = create_session()
        sess.save(f)
        sess.flush()
        sess.clear()
        f = sess.query(Foo).get(f.col1)
        assert len(list(f.bars)) == 2
        f.bars.clear()

    def testdictwrapper(self):
        """test that the supplied 'dict' wrapper can be used as a collection and can lazyload."""

        class Foo(object):
            pass
        class Bar(object):
            def __init__(self, data): self.data = data
                
        mapper(Foo, sometable, properties={
            'bars':relation(Bar,
                collection_class=collections.column_mapped_collection(someothertable.c.data))
        })
        mapper(Bar, someothertable)

        f = Foo()
        col = collections.collection_adapter(f.bars)
        col.append_with_event(Bar('a'))
        col.append_with_event(Bar('b'))
        sess = create_session()
        sess.save(f)
        sess.flush()
        sess.clear()
        f = sess.query(Foo).get(f.col1)
        assert len(list(f.bars)) == 2

        existing = set([id(b) for b in f.bars.values()])

        col = collections.collection_adapter(f.bars)
        col.append_with_event(Bar('b'))
        f.bars['a'] = Bar('a')
        sess.flush()
        sess.clear()
        f = sess.query(Foo).get(f.col1)
        assert len(list(f.bars)) == 2

        replaced = set([id(b) for b in f.bars.values()])
        self.assert_(existing != replaced)

    def testlist(self):
        class Parent(object):
            pass
        class Child(object):
            pass

        mapper(Parent, sometable, properties={
            'children':relation(Child, collection_class=list)
        })
        mapper(Child, someothertable)

        control = list()
        p = Parent()

        o = Child()
        control.append(o)
        p.children.append(o)
        assert control == p.children
        assert control == list(p.children)

        o = [Child(), Child(), Child(), Child()]
        control.extend(o)
        p.children.extend(o)
        assert control == p.children
        assert control == list(p.children)

        assert control[0] == p.children[0]
        assert control[-1] == p.children[-1]
        assert control[1:3] == p.children[1:3]

        del control[1]
        del p.children[1]
        assert control == p.children
        assert control == list(p.children)

        o = [Child()]
        control[1:3] = o
        p.children[1:3] = o
        assert control == p.children
        assert control == list(p.children)

        o = [Child(), Child(), Child(), Child()]
        control[1:3] = o
        p.children[1:3] = o
        assert control == p.children
        assert control == list(p.children)

        o = [Child(), Child(), Child(), Child()]
        control[-1:-2] = o
        p.children[-1:-2] = o
        assert control == p.children
        assert control == list(p.children)

        o = [Child(), Child(), Child(), Child()]
        control[4:] = o
        p.children[4:] = o
        assert control == p.children
        assert control == list(p.children)
        
        o = Child()
        control.insert(0, o)
        p.children.insert(0, o)
        assert control == p.children
        assert control == list(p.children)

        o = Child()
        control.insert(3, o)
        p.children.insert(3, o)
        assert control == p.children
        assert control == list(p.children)

        o = Child()
        control.insert(999, o)
        p.children.insert(999, o)
        assert control == p.children
        assert control == list(p.children)

        del control[0:1]
        del p.children[0:1]
        assert control == p.children
        assert control == list(p.children)

        del control[1:1]
        del p.children[1:1]
        assert control == p.children
        assert control == list(p.children)

        del control[1:3]
        del p.children[1:3]
        assert control == p.children
        assert control == list(p.children)

        del control[7:]
        del p.children[7:]
        assert control == p.children
        assert control == list(p.children)

        assert control.pop() == p.children.pop()
        assert control == p.children
        assert control == list(p.children)

        assert control.pop(0) == p.children.pop(0)
        assert control == p.children
        assert control == list(p.children)

        assert control.pop(2) == p.children.pop(2)
        assert control == p.children
        assert control == list(p.children)

        o = Child()
        control.insert(2, o)
        p.children.insert(2, o)
        assert control == p.children
        assert control == list(p.children)

        control.remove(o)
        p.children.remove(o)
        assert control == p.children
        assert control == list(p.children)

    def testobj(self):
        class Parent(object):
            pass
        class Child(object):
            pass

        class MyCollection(object):
            def __init__(self): self.data = []
            @collection.appender
            def append(self, value): self.data.append(value)
            @collection.remover
            def remove(self, value): self.data.remove(value)
            @collection.iterator
            def __iter__(self): return iter(self.data)

        mapper(Parent, sometable, properties={
            'children':relation(Child, collection_class=MyCollection)
        })
        mapper(Child, someothertable)

        control = list()
        p1 = Parent()

        o = Child()
        control.append(o)
        p1.children.append(o)
        assert control == list(p1.children)

        o = Child()
        control.append(o)
        p1.children.append(o)
        assert control == list(p1.children)

        o = Child()
        control.append(o)
        p1.children.append(o)
        assert control == list(p1.children)

        sess = create_session()
        sess.save(p1)
        sess.flush()
        sess.clear()
        
        p2 = sess.query(Parent).get(p1.col1)
        o = list(p2.children)
        assert len(o) == 3

class ViewOnlyTest(ORMTest):
    """test a view_only mapping where a third table is pulled into the primary join condition,
    using overlapping PK column names (should not produce "conflicting column" error)"""
    def define_tables(self, metadata):
        global t1, t2, t3
        t1 = Table("t1", metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(40)))
        t2 = Table("t2", metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(40)),
            Column('t1id', Integer, ForeignKey('t1.id')))
        t3 = Table("t3", metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(40)),
            Column('t2id', Integer, ForeignKey('t2.id'))
            )
            
    def test_basic(self):
        class C1(object):pass
        class C2(object):pass
        class C3(object):pass
        
        mapper(C1, t1, properties={
            't2s':relation(C2),
            't2_view':relation(C2, viewonly=True, primaryjoin=and_(t1.c.id==t2.c.t1id, t3.c.t2id==t2.c.id, t3.c.data==t1.c.data))
        })
        mapper(C2, t2)
        mapper(C3, t3, properties={
            't2':relation(C2)
        })
        
        c1 = C1()
        c1.data = 'c1data'
        c2a = C2()
        c1.t2s.append(c2a)
        c2b = C2()
        c1.t2s.append(c2b)
        c3 = C3()
        c3.data='c1data'
        c3.t2 = c2b
        sess = create_session()
        sess.save(c1)
        sess.save(c3)
        sess.flush()
        sess.clear()
        
        c1 = sess.query(C1).get(c1.id)
        assert set([x.id for x in c1.t2s]) == set([c2a.id, c2b.id])
        assert set([x.id for x in c1.t2_view]) == set([c2b.id])

class ViewOnlyTest2(ORMTest):
    """test a view_only mapping where a third table is pulled into the primary join condition,
    using non-overlapping PK column names (should not produce "mapper has no column X" error)"""
    def define_tables(self, metadata):
        global t1, t2, t3
        t1 = Table("t1", metadata,
            Column('t1id', Integer, primary_key=True),
            Column('data', String(40)))
        t2 = Table("t2", metadata,
            Column('t2id', Integer, primary_key=True),
            Column('data', String(40)),
            Column('t1id_ref', Integer, ForeignKey('t1.t1id')))
        t3 = Table("t3", metadata,
            Column('t3id', Integer, primary_key=True),
            Column('data', String(40)),
            Column('t2id_ref', Integer, ForeignKey('t2.t2id'))
            )
    def test_basic(self):
        class C1(object):pass
        class C2(object):pass
        class C3(object):pass

        mapper(C1, t1, properties={
            't2s':relation(C2),
            't2_view':relation(C2, viewonly=True, primaryjoin=and_(t1.c.t1id==t2.c.t1id_ref, t3.c.t2id_ref==t2.c.t2id, t3.c.data==t1.c.data))
        })
        mapper(C2, t2)
        mapper(C3, t3, properties={
            't2':relation(C2)
        })

        c1 = C1()
        c1.data = 'c1data'
        c2a = C2()
        c1.t2s.append(c2a)
        c2b = C2()
        c1.t2s.append(c2b)
        c3 = C3()
        c3.data='c1data'
        c3.t2 = c2b
        sess = create_session()
        sess.save(c1)
        sess.save(c3)
        sess.flush()
        sess.clear()

        c1 = sess.query(C1).get(c1.t1id)
        assert set([x.t2id for x in c1.t2s]) == set([c2a.t2id, c2b.t2id])
        assert set([x.t2id for x in c1.t2_view]) == set([c2b.t2id])
        
        
if __name__ == "__main__":
    testbase.main()        
