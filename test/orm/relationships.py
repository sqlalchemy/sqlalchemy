import testenv; testenv.configure_for_tests()
import datetime
from sqlalchemy import *
from sqlalchemy import exceptions, types
from sqlalchemy.orm import *
from sqlalchemy.orm import collections
from sqlalchemy.orm.collections import collection
from testlib import *
from testlib import fixtures

class RelationTest(TestBase):
    """An extended topological sort test

    This is essentially an extension of the "dependency.py" topological sort
    test.  In this test, a table is dependent on two other tables that are
    otherwise unrelated to each other.  The dependency sort must insure that
    this childmost table is below both parent tables in the outcome (a bug
    existed where this was not always the case).

    While the straight topological sort tests should expose this, since the
    sorting can be different due to subtle differences in program execution,
    this test case was exposing the bug whereas the simpler tests were not.
    """

    def setUpAll(self):
        global metadata, tbl_a, tbl_b, tbl_c, tbl_d

        metadata = MetaData()
        tbl_a = Table("tbl_a", metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(128)),
        )
        tbl_b = Table("tbl_b", metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(128)),
        )
        tbl_c = Table("tbl_c", metadata,
            Column("id", Integer, primary_key=True),
            Column("tbl_a_id", Integer, ForeignKey("tbl_a.id"), nullable=False),
            Column("name", String(128)),
        )
        tbl_d = Table("tbl_d", metadata,
            Column("id", Integer, primary_key=True),
            Column("tbl_c_id", Integer, ForeignKey("tbl_c.id"), nullable=False),
            Column("tbl_b_id", Integer, ForeignKey("tbl_b.id")),
            Column("name", String(128)),
        )
    def setUp(self):
        global session
        session = create_session(bind=testing.db)
        conn = testing.db.connect()
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
            d_rows=relation(D, cascade="all, delete-orphan", backref="c_row"),
        ))
        B.mapper = mapper(B, tbl_b)
        A.mapper = mapper(A, tbl_a, properties=dict(
            c_rows=relation(C, cascade="all, delete-orphan", backref="a_row"),
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
        conn = testing.db.connect()
        conn.drop(tbl_d)
        conn.drop(tbl_c)
        conn.drop(tbl_b)
        conn.drop(tbl_a)

    def tearDownAll(self):
        metadata.drop_all(testing.db)

    def testDeleteRootTable(self):
        session.flush()
        session.delete(a) # works as expected
        session.flush()

    def testDeleteMiddleTable(self):
        session.flush()
        session.delete(c) # fails
        session.flush()

class RelationTest2(TestBase):
    """Tests a relationship on a column included in multiple foreign keys.

    This test tests a relationship on a column that is included in multiple
    foreign keys, as well as a self-referential relationship on a composite
    key where one column in the foreign key is 'joined to itself'.
    """

    def setUpAll(self):
        global metadata, company_tbl, employee_tbl
        metadata = MetaData(testing.db)

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

    def test_explicit(self):
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
                remote_side=[employee_tbl.c.emp_id, employee_tbl.c.company_id],
                foreign_keys=[employee_tbl.c.reports_to_id],
                backref='employees')
        })

        sess = create_session()
        c1 = Company()
        c2 = Company()

        e1 = Employee(u'emp1', c1, 1)
        e2 = Employee(u'emp2', c1, 2, e1)
        e3 = Employee(u'emp3', c1, 3, e1)
        e4 = Employee(u'emp4', c1, 4, e3)
        e5 = Employee(u'emp5', c2, 1)
        e6 = Employee(u'emp6', c2, 2, e5)
        e7 = Employee(u'emp7', c2, 3, e5)

        [sess.save(x) for x in [c1,c2]]
        sess.flush()
        sess.clear()

        test_c1 = sess.query(Company).get(c1.company_id)
        test_e1 = sess.query(Employee).get([c1.company_id, e1.emp_id])
        assert test_e1.name == 'emp1', test_e1.name
        test_e5 = sess.query(Employee).get([c2.company_id, e5.emp_id])
        assert test_e5.name == 'emp5', test_e5.name
        assert [x.name for x in test_e1.employees] == ['emp2', 'emp3']
        assert sess.query(Employee).get([c1.company_id, 3]).reports_to.name == 'emp1'
        assert sess.query(Employee).get([c2.company_id, 3]).reports_to.name == 'emp5'

    def test_implicit(self):
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
                remote_side=[employee_tbl.c.emp_id, employee_tbl.c.company_id],
                foreign_keys=[employee_tbl.c.reports_to_id],
                backref='employees')
        })

        sess = create_session()
        c1 = Company()
        c2 = Company()

        e1 = Employee(u'emp1', c1, 1)
        e2 = Employee(u'emp2', c1, 2, e1)
        e3 = Employee(u'emp3', c1, 3, e1)
        e4 = Employee(u'emp4', c1, 4, e3)
        e5 = Employee(u'emp5', c2, 1)
        e6 = Employee(u'emp6', c2, 2, e5)
        e7 = Employee(u'emp7', c2, 3, e5)

        [sess.save(x) for x in [c1,c2]]
        sess.flush()
        sess.clear()

        test_c1 = sess.query(Company).get(c1.company_id)
        test_e1 = sess.query(Employee).get([c1.company_id, e1.emp_id])
        assert test_e1.name == 'emp1', test_e1.name
        test_e5 = sess.query(Employee).get([c2.company_id, e5.emp_id])
        assert test_e5.name == 'emp5', test_e5.name
        assert [x.name for x in test_e1.employees] == ['emp2', 'emp3']
        assert sess.query(Employee).get([c1.company_id, 3]).reports_to.name == 'emp1'
        assert sess.query(Employee).get([c2.company_id, 3]).reports_to.name == 'emp5'

class RelationTest3(TestBase):
    def setUpAll(self):
        global jobs, pageversions, pages, metadata, Job, Page, PageVersion, PageComment
        import datetime
        metadata = MetaData(testing.db)
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
            Column("content", UnicodeText),
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
                            foreign_keys=[pages.c.current_version],
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
        j1 = Job(u'somejob')
        j1.create_page(u'page1')
        j1.create_page(u'page2')
        j1.create_page(u'page3')

        j2 = Job(u'somejob2')
        j2.create_page(u'page1')
        j2.create_page(u'page2')
        j2.create_page(u'page3')

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
        j = s.query(Job).filter_by(jobno=u'somejob').one()
        oldp = list(j.pages)
        j.pages = []

        s.flush()

        s.clear()
        j = s.query(Job).filter_by(jobno=u'somejob2').one()
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
            test_needs_fk=True)
        tableB = Table("B",metadata,
                Column("id",Integer,ForeignKey("A.id"),primary_key=True),
                test_needs_fk=True)
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

    @testing.fails_on_everything_except('sqlite', 'mysql')
    def test_nullPKsOK_BtoA(self):
        # postgres cant handle a nullable PK column...?
        tableC = Table('tablec', tableA.metadata,
            Column('id', Integer, primary_key=True),
            Column('a_id', Integer, ForeignKey('A.id'), primary_key=True, autoincrement=False, nullable=True))
        tableC.create()

        class A(object):pass
        class C(object):pass
        mapper(C, tableC, properties={
            'a':relation(A, cascade="save-update")
        }, allow_null_pks=True)
        mapper(A, tableA)
        c1 = C()
        c1.id = 5
        c1.a = None
        sess = create_session()
        sess.save(c1)
        # test that no error is raised.
        sess.flush()

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
    """Test a map to a select that relates to a map to the table."""

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
        newcon = session.query(Container).first()
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
        self.assertRaises(exceptions.FlushError, sess.flush)

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
        self.assertRaises(exceptions.FlushError, sess.flush)

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
        self.assertRaises(exceptions.FlushError, sess.flush)

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

class ViewOnlyTest3(ORMTest):
    """test relating on a join that has no equated columns"""
    def define_tables(self, metadata):
        global foos, bars
        foos = Table('foos', metadata, Column('id', Integer, primary_key=True))
        bars = Table('bars', metadata, Column('id', Integer, primary_key=True), Column('fid', Integer))

    def test_viewonly_join(self):
        class Foo(fixtures.Base):
            pass
        class Bar(fixtures.Base):
            pass

        mapper(Foo, foos, properties={
            'bars':relation(Bar, primaryjoin=foos.c.id>bars.c.fid, foreign_keys=[bars.c.fid], viewonly=True)
        })

        mapper(Bar, bars)

        sess = create_session()
        sess.save(Foo(id=4))
        sess.save(Foo(id=9))
        sess.save(Bar(id=1, fid=2))
        sess.save(Bar(id=2, fid=3))
        sess.save(Bar(id=3, fid=6))
        sess.save(Bar(id=4, fid=7))
        sess.flush()

        sess = create_session()
        self.assertEquals(sess.query(Foo).filter_by(id=4).one(), Foo(id=4, bars=[Bar(fid=2), Bar(fid=3)]))
        self.assertEquals(sess.query(Foo).filter_by(id=9).one(), Foo(id=9, bars=[Bar(fid=2), Bar(fid=3), Bar(fid=6), Bar(fid=7)]))

class ViewOnlyTest4(ORMTest):
    """test relating on a join that contains the same 'remote' column twice"""
    def define_tables(self, metadata):
        global foos, bars
        foos = Table('foos', metadata, Column('id', Integer, primary_key=True), 
            Column('bid1', Integer,ForeignKey('bars.id')),
            Column('bid2', Integer,ForeignKey('bars.id')))
            
        bars = Table('bars', metadata, Column('id', Integer, primary_key=True), Column('data', String(50)))
        
    def test_relation_on_or(self):
        class Foo(fixtures.Base):
            pass
        class Bar(fixtures.Base):
            pass

        mapper(Foo, foos, properties={
            'bars':relation(Bar, primaryjoin=or_(bars.c.id==foos.c.bid1, bars.c.id==foos.c.bid2), uselist=True, viewonly=True)
        })

        mapper(Bar, bars)
        sess = create_session()
        b1 = Bar(id=1, data='b1')
        b2 = Bar(id=2, data='b2')
        b3 = Bar(id=3, data='b3')
        f1 = Foo(bid1=1, bid2=2)
        f2 = Foo(bid1=3, bid2=None)
        sess.save(b1)
        sess.save(b2)
        sess.save(b3)
        sess.flush()
        sess.save(f1)
        sess.save(f2)
        sess.flush()
            
        sess.clear()
        self.assertEquals(sess.query(Foo).filter_by(id=f1.id).one(), Foo(bars=[Bar(data='b1'), Bar(data='b2')]))
        self.assertEquals(sess.query(Foo).filter_by(id=f2.id).one(), Foo(bars=[Bar(data='b3')]))

class ViewOnlyTest5(ORMTest):
    """test relating on a join that contains the same 'local' column twice"""
    def define_tables(self, metadata):
        global foos, bars
        foos = Table('foos', metadata, Column('id', Integer, primary_key=True), 
            Column('data', String(50))
            )

        bars = Table('bars', metadata, Column('id', Integer, primary_key=True), 
                Column('fid1', Integer, ForeignKey('foos.id')),
                Column('fid2', Integer, ForeignKey('foos.id')),
                Column('data', String(50))
            )

    def test_relation_on_or(self):
        class Foo(fixtures.Base):
            pass
        class Bar(fixtures.Base):
            pass

        mapper(Foo, foos, properties={
            'bars':relation(Bar, primaryjoin=or_(bars.c.fid1==foos.c.id, bars.c.fid2==foos.c.id), viewonly=True)
        })

        mapper(Bar, bars)
        sess = create_session()
        f1 = Foo(id=1, data='f1')
        f2 = Foo(id=2, data='f2')
        b1 = Bar(fid1=1, data='b1')
        b2 = Bar(fid2=1, data='b2')
        b3 = Bar(fid1=2, data='b3')
        b4 = Bar(fid1=1, fid2=2, data='b4')
        sess.save(f1)
        sess.save(f2)
        sess.flush()
        sess.save(b1)
        sess.save(b2)
        sess.save(b3)
        sess.save(b4)
        sess.flush()

        sess.clear()
        self.assertEquals(sess.query(Foo).filter_by(id=f1.id).one(), Foo(bars=[Bar(data='b1'), Bar(data='b2'), Bar(data='b4')]))
        self.assertEquals(sess.query(Foo).filter_by(id=f2.id).one(), Foo(bars=[Bar(data='b3'), Bar(data='b4')]))

class ViewOnlyTest6(ORMTest):
    """test a long primaryjoin condition"""
    def define_tables(self, metadata):
        global t1, t2, t3, t2tot3
        t1 = Table('t1', metadata, 
            Column('id', Integer, primary_key=True),
            Column('data', String(50))
            )
        t2 = Table('t2', metadata,     
            Column('id', Integer, primary_key=True),
            Column('data', String(50)),
            Column('t1id', Integer, ForeignKey('t1.id')),
        )
        t3 = Table('t3', metadata, 
            Column('id', Integer, primary_key=True),
            Column('data', String(50))
            )
        t2tot3 = Table('t2tot3', metadata, 
            Column('t2id', Integer, ForeignKey('t2.id')),
            Column('t3id', Integer, ForeignKey('t3.id')),
        )
    
    def test_basic(self):
        class T1(fixtures.Base):
            pass
        class T2(fixtures.Base):
            pass
        class T3(fixtures.Base):
            pass
    
        mapper(T1, t1, properties={
            't3s':relation(T3, primaryjoin=and_(
                t1.c.id==t2.c.t1id,
                t2.c.id==t2tot3.c.t2id,
                t3.c.id==t2tot3.c.t3id
            ),
            viewonly=True, 
            foreign_keys=t3.c.id, remote_side=t2.c.t1id)
        })
        mapper(T2, t2, properties={
            't1':relation(T1),
            't3s':relation(T3, secondary=t2tot3)
        })
        mapper(T3, t3)
        
        sess = create_session()
        sess.save(T2(data='t2', t1=T1(data='t1'), t3s=[T3(data='t3')]))
        sess.flush()
        sess.clear()
        
        a = sess.query(T1).first()
        self.assertEquals(a.t3s, [T3(data='t3')])
        
    def test_remote_side_escalation(self):
        class T1(fixtures.Base):
            pass
        class T2(fixtures.Base):
            pass
        class T3(fixtures.Base):
            pass

        mapper(T1, t1, properties={
            't3s':relation(T3, primaryjoin=and_(
                t1.c.id==t2.c.t1id,
                t2.c.id==t2tot3.c.t2id,
                t3.c.id==t2tot3.c.t3id
            ),viewonly=True, foreign_keys=t3.c.id) 
        })
        mapper(T2, t2, properties={
            't1':relation(T1),
            't3s':relation(T3, secondary=t2tot3)
        })
        mapper(T3, t3)
        self.assertRaisesMessage(exceptions.ArgumentError, "Specify remote_side argument", compile_mappers)

class ExplicitLocalRemoteTest(ORMTest):
    def define_tables(self, metadata):
        global t1, t2
        t1 = Table('t1', metadata, 
            Column('id', String(50), primary_key=True),
            Column('data', String(50))
            )
        t2 = Table('t2', metadata,     
            Column('id', Integer, primary_key=True),
            Column('data', String(50)),
            Column('t1id', String(50)),
        )

    def test_onetomany_funcfk(self):
        class T1(fixtures.Base):
            pass
        class T2(fixtures.Base):
            pass
        
        # use a function within join condition.  but specifying
        # local_remote_pairs overrides all parsing of the join condition.
        mapper(T1, t1, properties={
            't2s':relation(T2, primaryjoin=t1.c.id==func.lower(t2.c.t1id), 
                _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
                foreign_keys=[t2.c.t1id]
            )
        })
        mapper(T2, t2)
        
        sess = create_session()
        a1 = T1(id='number1', data='a1')
        a2 = T1(id='number2', data='a2')
        b1 = T2(data='b1', t1id='NuMbEr1')
        b2 = T2(data='b2', t1id='Number1')
        b3 = T2(data='b3', t1id='Number2')
        sess.save(a1)
        sess.save(a2)
        sess.save(b1)
        sess.save(b2)
        sess.save(b3)
        sess.flush()
        sess.clear()
        
        self.assertEquals(sess.query(T1).first(), T1(id='number1', data='a1', t2s=[T2(data='b1', t1id='NuMbEr1'), T2(data='b2', t1id='Number1')]))
    
    def test_manytoone_funcfk(self):
        class T1(fixtures.Base):
            pass
        class T2(fixtures.Base):
            pass
        mapper(T1, t1)
        mapper(T2, t2, properties={
            't1':relation(T1, primaryjoin=t1.c.id==func.lower(t2.c.t1id),
                _local_remote_pairs=[(t2.c.t1id, t1.c.id)],
                foreign_keys=[t2.c.t1id],
                uselist=True
            )
        })
        sess = create_session()
        a1 = T1(id='number1', data='a1')
        a2 = T1(id='number2', data='a2')
        b1 = T2(data='b1', t1id='NuMbEr1')
        b2 = T2(data='b2', t1id='Number1')
        b3 = T2(data='b3', t1id='Number2')
        sess.save(a1)
        sess.save(a2)
        sess.save(b1)
        sess.save(b2)
        sess.save(b3)
        sess.flush()
        sess.clear()
        self.assertEquals(sess.query(T2).filter(T2.data.in_(['b1', 'b2'])).all(), 
            [
                T2(data='b1', t1=[T1(id='number1', data='a1')]),
                T2(data='b2', t1=[T1(id='number1', data='a1')])
            ]
        )
    
    def test_onetomany_func_referent(self):
        class T1(fixtures.Base):
            pass
        class T2(fixtures.Base):
            pass
        
        mapper(T1, t1, properties={
            't2s':relation(T2, primaryjoin=func.lower(t1.c.id)==t2.c.t1id, 
                _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
                foreign_keys=[t2.c.t1id]
            )
        })
        mapper(T2, t2)
        
        sess = create_session()
        a1 = T1(id='NuMbeR1', data='a1')
        a2 = T1(id='NuMbeR2', data='a2')
        b1 = T2(data='b1', t1id='number1')
        b2 = T2(data='b2', t1id='number1')
        b3 = T2(data='b2', t1id='number2')
        sess.save(a1)
        sess.save(a2)
        sess.save(b1)
        sess.save(b2)
        sess.save(b3)
        sess.flush()
        sess.clear()
        
        self.assertEquals(sess.query(T1).first(), T1(id='NuMbeR1', data='a1', t2s=[T2(data='b1', t1id='number1'), T2(data='b2', t1id='number1')]))

    def test_manytoone_func_referent(self):
        class T1(fixtures.Base):
            pass
        class T2(fixtures.Base):
            pass

        mapper(T1, t1)
        mapper(T2, t2, properties={
            't1':relation(T1, primaryjoin=func.lower(t1.c.id)==t2.c.t1id,
                _local_remote_pairs=[(t2.c.t1id, t1.c.id)],
                foreign_keys=[t2.c.t1id], uselist=True
            )
        })

        sess = create_session()
        a1 = T1(id='NuMbeR1', data='a1')
        a2 = T1(id='NuMbeR2', data='a2')
        b1 = T2(data='b1', t1id='number1')
        b2 = T2(data='b2', t1id='number1')
        b3 = T2(data='b3', t1id='number2')
        sess.save(a1)
        sess.save(a2)
        sess.save(b1)
        sess.save(b2)
        sess.save(b3)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(T2).filter(T2.data.in_(['b1', 'b2'])).all(), 
            [
                T2(data='b1', t1=[T1(id='NuMbeR1', data='a1')]),
                T2(data='b2', t1=[T1(id='NuMbeR1', data='a1')])
            ]
        )
        
    def test_escalation(self):
        class T1(fixtures.Base):
            pass
        class T2(fixtures.Base):
            pass
        
        mapper(T1, t1, properties={
            't2s':relation(T2, primaryjoin=t1.c.id==func.lower(t2.c.t1id), 
                _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
                foreign_keys=[t2.c.t1id],
                remote_side=[t2.c.t1id]
            )
        })
        mapper(T2, t2)
        self.assertRaises(exceptions.ArgumentError, compile_mappers)
        
        clear_mappers()
        mapper(T1, t1, properties={
            't2s':relation(T2, primaryjoin=t1.c.id==func.lower(t2.c.t1id), 
                _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
            )
        })
        mapper(T2, t2)
        self.assertRaises(exceptions.ArgumentError, compile_mappers)
        
class InvalidRelationEscalationTest(ORMTest):
    def define_tables(self, metadata):
        global foos, bars, Foo, Bar
        foos = Table('foos', metadata, Column('id', Integer, primary_key=True), Column('fid', Integer))
        bars = Table('bars', metadata, Column('id', Integer, primary_key=True), Column('fid', Integer))
        class Foo(object):
            pass
        class Bar(object):
            pass
            
    def test_no_join(self):
        mapper(Foo, foos, properties={
            'bars':relation(Bar)
        })

        mapper(Bar, bars)
        self.assertRaisesMessage(exceptions.ArgumentError, "Could not determine join condition between parent/child tables on relation", compile_mappers)

    def test_no_join_self_ref(self):
        mapper(Foo, foos, properties={
            'foos':relation(Foo)
        })

        mapper(Bar, bars)
        self.assertRaisesMessage(exceptions.ArgumentError, "Could not determine join condition between parent/child tables on relation", compile_mappers)
        
    def test_no_equated(self):
        mapper(Foo, foos, properties={
            'bars':relation(Bar, primaryjoin=foos.c.id>bars.c.fid)
        })

        mapper(Bar, bars)
        self.assertRaisesMessage(exceptions.ArgumentError, "Could not determine relation direction for primaryjoin condition", compile_mappers)

    def test_no_equated_fks(self):
        mapper(Foo, foos, properties={
            'bars':relation(Bar, primaryjoin=foos.c.id>bars.c.fid, foreign_keys=bars.c.fid)
        })

        mapper(Bar, bars)
        self.assertRaisesMessage(exceptions.ArgumentError, "Could not locate any equated, locally mapped column pairs for primaryjoin condition", compile_mappers)

    def test_no_equated_self_ref(self):
        mapper(Foo, foos, properties={
            'foos':relation(Foo, primaryjoin=foos.c.id>foos.c.fid)
        })

        mapper(Bar, bars)
        self.assertRaisesMessage(exceptions.ArgumentError, "Could not determine relation direction for primaryjoin condition", compile_mappers)

    def test_no_equated_self_ref(self):
        mapper(Foo, foos, properties={
            'foos':relation(Foo, primaryjoin=foos.c.id>foos.c.fid, foreign_keys=[foos.c.fid])
        })

        mapper(Bar, bars)
        self.assertRaisesMessage(exceptions.ArgumentError, "Could not locate any equated, locally mapped column pairs for primaryjoin condition", compile_mappers)

    def test_no_equated_viewonly(self):
        mapper(Foo, foos, properties={
            'bars':relation(Bar, primaryjoin=foos.c.id>bars.c.fid, viewonly=True)
        })

        mapper(Bar, bars)
        self.assertRaisesMessage(exceptions.ArgumentError, "Could not determine relation direction for primaryjoin condition", compile_mappers)

    def test_no_equated_self_ref_viewonly(self):
        mapper(Foo, foos, properties={
            'foos':relation(Foo, primaryjoin=foos.c.id>foos.c.fid, viewonly=True)
        })

        mapper(Bar, bars)

        self.assertRaisesMessage(exceptions.ArgumentError, "Specify the foreign_keys argument to indicate which columns on the relation are foreign.", compile_mappers)

    def test_no_equated_self_ref_viewonly_fks(self):
        mapper(Foo, foos, properties={
            'foos':relation(Foo, primaryjoin=foos.c.id>foos.c.fid, viewonly=True, foreign_keys=[foos.c.fid])
        })
        compile_mappers()
        self.assertEquals(Foo.foos.property.local_remote_pairs, [(foos.c.id, foos.c.fid)])

    def test_equated(self):
        mapper(Foo, foos, properties={
            'bars':relation(Bar, primaryjoin=foos.c.id==bars.c.fid)
        })
        mapper(Bar, bars)
        self.assertRaisesMessage(exceptions.ArgumentError, "Could not determine relation direction for primaryjoin condition", compile_mappers)
    
    def test_equated_self_ref(self):
        mapper(Foo, foos, properties={
            'foos':relation(Foo, primaryjoin=foos.c.id==foos.c.fid)
        })

        self.assertRaisesMessage(exceptions.ArgumentError, "Could not determine relation direction for primaryjoin condition", compile_mappers)

    def test_equated_self_ref_wrong_fks(self):
        mapper(Foo, foos, properties={
            'foos':relation(Foo, primaryjoin=foos.c.id==foos.c.fid, foreign_keys=[bars.c.id])
        })

        self.assertRaisesMessage(exceptions.ArgumentError, "Could not determine relation direction for primaryjoin condition", compile_mappers)

class InvalidRelationEscalationTestM2M(ORMTest):
    def define_tables(self, metadata):
        global foos, bars, Foo, Bar, foobars
        foos = Table('foos', metadata, Column('id', Integer, primary_key=True))
        foobars = Table('foobars', metadata, Column('fid', Integer), Column('bid', Integer))
        bars = Table('bars', metadata, Column('id', Integer, primary_key=True))
        class Foo(object):
            pass
        class Bar(object):
            pass

    def test_no_join(self):
        mapper(Foo, foos, properties={
            'bars':relation(Bar, secondary=foobars)
        })

        mapper(Bar, bars)
        self.assertRaisesMessage(exceptions.ArgumentError, "Could not determine join condition between parent/child tables on relation", compile_mappers)

    def test_no_secondaryjoin(self):
        mapper(Foo, foos, properties={
            'bars':relation(Bar, secondary=foobars, primaryjoin=foos.c.id>foobars.c.fid)
        })

        mapper(Bar, bars)
        self.assertRaisesMessage(exceptions.ArgumentError, "Could not determine join condition between parent/child tables on relation", compile_mappers)

    def test_bad_primaryjoin(self):
        mapper(Foo, foos, properties={
            'bars':relation(Bar, secondary=foobars, primaryjoin=foos.c.id>foobars.c.fid, secondaryjoin=foobars.c.bid<=bars.c.id)
        })

        mapper(Bar, bars)
        self.assertRaisesMessage(exceptions.ArgumentError, "Could not determine relation direction for primaryjoin condition", compile_mappers)

    def test_bad_secondaryjoin(self):
        mapper(Foo, foos, properties={
            'bars':relation(Bar, secondary=foobars, primaryjoin=foos.c.id==foobars.c.fid, secondaryjoin=foobars.c.bid<=bars.c.id, foreign_keys=[foobars.c.fid])
        })

        mapper(Bar, bars)
        self.assertRaisesMessage(exceptions.ArgumentError, "Could not determine relation direction for secondaryjoin condition", compile_mappers)

    def test_no_equated_secondaryjoin(self):
        mapper(Foo, foos, properties={
            'bars':relation(Bar, secondary=foobars, primaryjoin=foos.c.id==foobars.c.fid, secondaryjoin=foobars.c.bid<=bars.c.id, foreign_keys=[foobars.c.fid, foobars.c.bid])
        })

        mapper(Bar, bars)
        self.assertRaisesMessage(exceptions.ArgumentError, "Could not locate any equated, locally mapped column pairs for secondaryjoin condition", compile_mappers)


if __name__ == "__main__":
    testenv.main()
