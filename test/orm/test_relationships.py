from test.lib.testing import assert_raises, assert_raises_message
import datetime
import sqlalchemy as sa
from test.lib import testing
from sqlalchemy import Integer, String, ForeignKey, MetaData, and_
from test.lib.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, relation, \
                    backref, create_session, configure_mappers, \
                    clear_mappers, sessionmaker, attributes,\
                    Session, composite, column_property
from test.lib.testing import eq_, startswith_
from test.lib import fixtures
from test.orm import _fixtures


class DependencyTwoParentTest(fixtures.MappedTest):
    """Test flush() when a mapper is dependent on multiple relationships"""

    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table("tbl_a", metadata,
            Column("id", Integer, primary_key=True, 
                                test_needs_autoincrement=True),
            Column("name", String(128)))
        Table("tbl_b", metadata,
            Column("id", Integer, primary_key=True, 
                                test_needs_autoincrement=True),
            Column("name", String(128)))
        Table("tbl_c", metadata,
            Column("id", Integer, primary_key=True, 
                                test_needs_autoincrement=True),
            Column("tbl_a_id", Integer, ForeignKey("tbl_a.id"), 
                                nullable=False),
            Column("name", String(128)))
        Table("tbl_d", metadata,
            Column("id", Integer, primary_key=True, 
                                test_needs_autoincrement=True),
            Column("tbl_c_id", Integer, ForeignKey("tbl_c.id"), 
                                nullable=False),
            Column("tbl_b_id", Integer, ForeignKey("tbl_b.id")),
            Column("name", String(128)))

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass
        class B(cls.Basic):
            pass
        class C(cls.Basic):
            pass
        class D(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        A, C, B, D, tbl_b, tbl_c, tbl_a, tbl_d = (cls.classes.A,
                                cls.classes.C,
                                cls.classes.B,
                                cls.classes.D,
                                cls.tables.tbl_b,
                                cls.tables.tbl_c,
                                cls.tables.tbl_a,
                                cls.tables.tbl_d)

        mapper(A, tbl_a, properties=dict(
            c_rows=relationship(C, cascade="all, delete-orphan", 
                                    backref="a_row")))
        mapper(B, tbl_b)
        mapper(C, tbl_c, properties=dict(
            d_rows=relationship(D, cascade="all, delete-orphan", 
                                    backref="c_row")))
        mapper(D, tbl_d, properties=dict(
            b_row=relationship(B)))

    @classmethod
    def insert_data(cls):
        A, C, B, D = (cls.classes.A,
                                cls.classes.C,
                                cls.classes.B,
                                cls.classes.D)

        session = create_session()
        a = A(name='a1')
        b = B(name='b1')
        c = C(name='c1', a_row=a)

        d1 = D(name='d1', b_row=b, c_row=c)
        d2 = D(name='d2', b_row=b, c_row=c)
        d3 = D(name='d3', b_row=b, c_row=c)
        session.add(a)
        session.add(b)
        session.flush()

    def testDeleteRootTable(self):
        A = self.classes.A

        session = create_session()
        a = session.query(A).filter_by(name='a1').one()

        session.delete(a)
        session.flush()

    def testDeleteMiddleTable(self):
        C = self.classes.C

        session = create_session()
        c = session.query(C).filter_by(name='c1').one()

        session.delete(c)
        session.flush()


class CompositeSelfRefFKTest(fixtures.MappedTest):
    """Tests a composite FK where, in
    the relationship(), one col points 
    to itself in the same table.

    this is a very unusual case::

    company         employee
    ----------      ----------
    company_id <--- company_id ------+
    name                ^            |
                        +------------+

                    emp_id <---------+
                    name             |
                    reports_to_id ---+

    employee joins to its sub-employees
    both on reports_to_id, *and on company_id to itself*.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table('company_t', metadata,
              Column('company_id', Integer, primary_key=True, 
                                test_needs_autoincrement=True),
              Column('name', sa.Unicode(30)))

        Table('employee_t', metadata,
              Column('company_id', Integer, primary_key=True),
              Column('emp_id', Integer, primary_key=True),
              Column('name', sa.Unicode(30)),
              Column('reports_to_id', Integer),
              sa.ForeignKeyConstraint(
                  ['company_id'],
                  ['company_t.company_id']),
              sa.ForeignKeyConstraint(
                  ['company_id', 'reports_to_id'],
                  ['employee_t.company_id', 'employee_t.emp_id']))

    @classmethod
    def setup_classes(cls):
        class Company(cls.Basic):
            pass

        class Employee(cls.Basic):
            def __init__(self, name, company, emp_id, reports_to=None):
                self.name = name
                self.company = company
                self.emp_id = emp_id
                self.reports_to = reports_to

    def test_explicit(self):
        Employee, Company, employee_t, company_t = (self.classes.Employee,
                                self.classes.Company,
                                self.tables.employee_t,
                                self.tables.company_t)

        mapper(Company, company_t)
        mapper(Employee, employee_t, properties= {
            'company':relationship(Company, 
                            primaryjoin=employee_t.c.company_id==
                                                company_t.c.company_id, 
                                                backref='employees'),
            'reports_to':relationship(Employee, primaryjoin=
                sa.and_(
                    employee_t.c.emp_id==employee_t.c.reports_to_id,
                    employee_t.c.company_id==employee_t.c.company_id
                ),
                remote_side=[employee_t.c.emp_id, employee_t.c.company_id],
                foreign_keys=[employee_t.c.reports_to_id],
                backref=backref('employees', foreign_keys=None))
        })

        self._test()

    def test_implicit(self):
        Employee, Company, employee_t, company_t = (self.classes.Employee,
                                self.classes.Company,
                                self.tables.employee_t,
                                self.tables.company_t)

        mapper(Company, company_t)
        mapper(Employee, employee_t, properties= {
            'company':relationship(Company, backref='employees'),
            'reports_to':relationship(Employee,
                remote_side=[employee_t.c.emp_id, employee_t.c.company_id],
                foreign_keys=[employee_t.c.reports_to_id],
                backref=backref('employees', foreign_keys=None)
                )
        })

        self._test()

    def test_very_implicit(self):
        Employee, Company, employee_t, company_t = (self.classes.Employee,
                                self.classes.Company,
                                self.tables.employee_t,
                                self.tables.company_t)

        mapper(Company, company_t)
        mapper(Employee, employee_t, properties= {
            'company':relationship(Company, backref='employees'),
            'reports_to':relationship(Employee,
                remote_side=[employee_t.c.emp_id, employee_t.c.company_id],
                backref='employees'
                )
        })

        self._test()

    def test_very_explicit(self):
        Employee, Company, employee_t, company_t = (self.classes.Employee,
                                self.classes.Company,
                                self.tables.employee_t,
                                self.tables.company_t)

        mapper(Company, company_t)
        mapper(Employee, employee_t, properties= {
            'company':relationship(Company, backref='employees'),
            'reports_to':relationship(Employee,
                _local_remote_pairs = [
                        (employee_t.c.reports_to_id, employee_t.c.emp_id),
                        (employee_t.c.company_id, employee_t.c.company_id)
                ],
                foreign_keys=[employee_t.c.reports_to_id],
                backref=backref('employees', foreign_keys=None)
                )
        })

        self._test()

    def _test(self):
        Employee, Company = self.classes.Employee, self.classes.Company

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

        sess.add_all((c1, c2))
        sess.flush()
        sess.expunge_all()

        test_c1 = sess.query(Company).get(c1.company_id)
        test_e1 = sess.query(Employee).get([c1.company_id, e1.emp_id])
        assert test_e1.name == 'emp1', test_e1.name
        test_e5 = sess.query(Employee).get([c2.company_id, e5.emp_id])
        assert test_e5.name == 'emp5', test_e5.name
        assert [x.name for x in test_e1.employees] == ['emp2', 'emp3']
        assert sess.query(Employee).\
                get([c1.company_id, 3]).reports_to.name == 'emp1'
        assert sess.query(Employee).\
                get([c2.company_id, 3]).reports_to.name == 'emp5'

class ComplexPostUpdateTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table("jobs", metadata,
              Column("jobno", sa.Unicode(15), primary_key=True),
              Column("created", sa.DateTime, nullable=False,
                     default=datetime.datetime.now),
              Column("deleted", sa.Boolean, nullable=False, default=False))

        Table("pageversions", metadata,
              Column("jobno", sa.Unicode(15), primary_key=True),
              Column("pagename", sa.Unicode(30), primary_key=True),
              Column("version", Integer, primary_key=True, default=1),
              Column("created", sa.DateTime, nullable=False,
                     default=datetime.datetime.now),
              Column("md5sum", String(32)),
              Column("width", Integer, nullable=False, default=0),
              Column("height", Integer, nullable=False, default=0),
              sa.ForeignKeyConstraint(
                  ["jobno", "pagename"],
                  ["pages.jobno", "pages.pagename"]))

        Table("pages", metadata,
              Column("jobno", sa.Unicode(15), ForeignKey("jobs.jobno"),
                     primary_key=True),
              Column("pagename", sa.Unicode(30), primary_key=True),
              Column("created", sa.DateTime, nullable=False,
                     default=datetime.datetime.now),
              Column("deleted", sa.Boolean, nullable=False, default=False),
              Column("current_version", Integer))

        Table("pagecomments", metadata,
              Column("jobno", sa.Unicode(15), primary_key=True),
              Column("pagename", sa.Unicode(30), primary_key=True),
              Column("comment_id", Integer, primary_key=True,
                     autoincrement=False),
              Column("content", sa.UnicodeText),
              sa.ForeignKeyConstraint(
                  ["jobno", "pagename"],
                  ["pages.jobno", "pages.pagename"]))

    @classmethod
    def setup_mappers(cls):
        pageversions, pagecomments, jobs, pages = (cls.tables.pageversions,
                                cls.tables.pagecomments,
                                cls.tables.jobs,
                                cls.tables.pages)

        class Job(cls.Basic):
            def create_page(self, pagename):
                return Page(job=self, pagename=pagename)
        class PageVersion(cls.Basic):
            def __init__(self, page=None, version=None):
                self.page = page
                self.version = version
        class Page(cls.Basic):
            def __init__(self, job=None, pagename=None):
                self.job = job
                self.pagename = pagename
                self.currentversion = PageVersion(self, 1)
            def add_version(self):
                self.currentversion = PageVersion(
                    page=self, version=self.currentversion.version+1)
                comment = self.add_comment()
                comment.closeable = False
                comment.content = u'some content'
                return self.currentversion
            def add_comment(self):
                nextnum = max([-1] + 
                            [c.comment_id for c in self.comments]) + 1
                newcomment = PageComment()
                newcomment.comment_id = nextnum
                self.comments.append(newcomment)
                newcomment.created_version = self.currentversion.version
                return newcomment
        class PageComment(cls.Basic):
            pass

        mapper(Job, jobs)
        mapper(PageVersion, pageversions)
        mapper(Page, pages, properties={
            'job': relationship(
                 Job,
                 backref=backref('pages',
                                 cascade="all, delete-orphan",
                                 order_by=pages.c.pagename)),
            'currentversion': relationship(
                 PageVersion,
                 uselist=False,
                 primaryjoin=sa.and_(
                     pages.c.jobno==pageversions.c.jobno,
                     pages.c.pagename==pageversions.c.pagename,
                     pages.c.current_version==pageversions.c.version),
                 post_update=True),
            'versions': relationship(
                 PageVersion,
                 cascade="all, delete-orphan",
                 primaryjoin=sa.and_(pages.c.jobno==pageversions.c.jobno,
                                 pages.c.pagename==pageversions.c.pagename),
                 order_by=pageversions.c.version,
                 backref=backref('page',lazy='joined')
                )})
        mapper(PageComment, pagecomments, properties={
            'page': relationship(
                  Page,
                  primaryjoin=sa.and_(pages.c.jobno==pagecomments.c.jobno,
                                  pages.c.pagename==pagecomments.c.pagename),
                  backref=backref("comments",
                                  cascade="all, delete-orphan",
                                  order_by=pagecomments.c.comment_id))})

    def test_basic(self):
        """A combination of complicated join conditions with post_update."""

        Job = self.classes.Job


        j1 = Job(jobno=u'somejob')
        j1.create_page(u'page1')
        j1.create_page(u'page2')
        j1.create_page(u'page3')

        j2 = Job(jobno=u'somejob2')
        j2.create_page(u'page1')
        j2.create_page(u'page2')
        j2.create_page(u'page3')

        j2.pages[0].add_version()
        j2.pages[0].add_version()
        j2.pages[1].add_version()

        s = create_session()
        s.add_all((j1, j2))

        s.flush()

        s.expunge_all()
        j = s.query(Job).filter_by(jobno=u'somejob').one()
        oldp = list(j.pages)
        j.pages = []

        s.flush()

        s.expunge_all()
        j = s.query(Job).filter_by(jobno=u'somejob2').one()
        j.pages[1].current_version = 12
        s.delete(j)
        s.flush()

class FKsAsPksTest(fixtures.MappedTest):
    """Syncrules on foreign keys that are also primary"""

    @classmethod
    def define_tables(cls, metadata):
        Table("tableA", metadata,
              Column("id",Integer,primary_key=True, 
                            test_needs_autoincrement=True),
              Column("foo",Integer,),
              test_needs_fk=True)

        Table("tableB",metadata,
              Column("id",Integer,ForeignKey("tableA.id"),primary_key=True),
              test_needs_fk=True)

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

    def test_onetoone_switch(self):
        """test that active history is enabled on a 
        one-to-many/one that has use_get==True"""

        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)


        mapper(A, tableA, properties={
            'b':relationship(B, cascade="all,delete-orphan", uselist=False)})
        mapper(B, tableB)

        configure_mappers()
        assert A.b.property.strategy.use_get

        sess = create_session()

        a1 = A()
        sess.add(a1)
        sess.flush()
        sess.close()
        a1 = sess.query(A).first()
        a1.b = B()
        sess.flush()

    def test_no_delete_PK_AtoB(self):
        """A cant be deleted without B because B would have no PK value."""

        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)

        mapper(A, tableA, properties={
            'bs':relationship(B, cascade="save-update")})
        mapper(B, tableB)

        a1 = A()
        a1.bs.append(B())
        sess = create_session()
        sess.add(a1)
        sess.flush()

        sess.delete(a1)
        try:
            sess.flush()
            assert False
        except AssertionError, e:
            startswith_(str(e),
                        "Dependency rule tried to blank-out "
                        "primary key column 'tableB.id' on instance ")

    def test_no_delete_PK_BtoA(self):
        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)

        mapper(B, tableB, properties={
            'a':relationship(A, cascade="save-update")})
        mapper(A, tableA)

        b1 = B()
        a1 = A()
        b1.a = a1
        sess = create_session()
        sess.add(b1)
        sess.flush()
        b1.a = None
        try:
            sess.flush()
            assert False
        except AssertionError, e:
            startswith_(str(e),
                        "Dependency rule tried to blank-out "
                        "primary key column 'tableB.id' on instance ")

    @testing.fails_on_everything_except('sqlite', 'mysql')
    def test_nullPKsOK_BtoA(self):
        A, tableA = self.classes.A, self.tables.tableA

        # postgresql cant handle a nullable PK column...?
        tableC = Table('tablec', tableA.metadata,
            Column('id', Integer, primary_key=True),
            Column('a_id', Integer, ForeignKey('tableA.id'),
                   primary_key=True, autoincrement=False, nullable=True))
        tableC.create()

        class C(fixtures.BasicEntity):
            pass
        mapper(C, tableC, properties={
            'a':relationship(A, cascade="save-update")
        })
        mapper(A, tableA)

        c1 = C()
        c1.id = 5
        c1.a = None
        sess = create_session()
        sess.add(c1)
        # test that no error is raised.
        sess.flush()

    def test_delete_cascade_BtoA(self):
        """No 'blank the PK' error when the child is to 
        be deleted as part of a cascade"""

        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)


        for cascade in ("save-update, delete",
                        #"save-update, delete-orphan",
                        "save-update, delete, delete-orphan"):
            mapper(B, tableB, properties={
                'a':relationship(A, cascade=cascade, single_parent=True)
            })
            mapper(A, tableA)

            b1 = B()
            a1 = A()
            b1.a = a1
            sess = create_session()
            sess.add(b1)
            sess.flush()
            sess.delete(b1)
            sess.flush()
            assert a1 not in sess
            assert b1 not in sess
            sess.expunge_all()
            sa.orm.clear_mappers()

    def test_delete_cascade_AtoB(self):
        """No 'blank the PK' error when the child is to 
        be deleted as part of a cascade"""

        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)


        for cascade in ("save-update, delete",
                        #"save-update, delete-orphan",
                        "save-update, delete, delete-orphan"):
            mapper(A, tableA, properties={
                'bs':relationship(B, cascade=cascade)
            })
            mapper(B, tableB)

            a1 = A()
            b1 = B()
            a1.bs.append(b1)
            sess = create_session()
            sess.add(a1)
            sess.flush()

            sess.delete(a1)
            sess.flush()
            assert a1 not in sess
            assert b1 not in sess
            sess.expunge_all()
            sa.orm.clear_mappers()

    def test_delete_manual_AtoB(self):
        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)

        mapper(A, tableA, properties={
            'bs':relationship(B, cascade="none")})
        mapper(B, tableB)

        a1 = A()
        b1 = B()
        a1.bs.append(b1)
        sess = create_session()
        sess.add(a1)
        sess.add(b1)
        sess.flush()

        sess.delete(a1)
        sess.delete(b1)
        sess.flush()
        assert a1 not in sess
        assert b1 not in sess
        sess.expunge_all()

    def test_delete_manual_BtoA(self):
        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)

        mapper(B, tableB, properties={
            'a':relationship(A, cascade="none")})
        mapper(A, tableA)

        b1 = B()
        a1 = A()
        b1.a = a1
        sess = create_session()
        sess.add(b1)
        sess.add(a1)
        sess.flush()
        sess.delete(b1)
        sess.delete(a1)
        sess.flush()
        assert a1 not in sess
        assert b1 not in sess

class UniqueColReferenceSwitchTest(fixtures.MappedTest):
    """test a relationship based on a primary 
    join against a unique non-pk column"""

    @classmethod
    def define_tables(cls, metadata):
        Table("table_a", metadata,
                        Column("id", Integer, primary_key=True, 
                                        test_needs_autoincrement=True),
                        Column("ident", String(10), nullable=False, 
                                        unique=True),
                        )

        Table("table_b", metadata,
                        Column("id", Integer, primary_key=True, 
                                        test_needs_autoincrement=True),
                        Column("a_ident", String(10), 
                                        ForeignKey('table_a.ident'), 
                                        nullable=False),
                        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass
        class B(cls.Comparable):
            pass

    def test_switch_parent(self):
        A, B, table_b, table_a = (self.classes.A,
                                self.classes.B,
                                self.tables.table_b,
                                self.tables.table_a)

        mapper(A, table_a)
        mapper(B, table_b, properties={"a": relationship(A, backref="bs")})

        session = create_session()
        a1, a2 = A(ident="uuid1"), A(ident="uuid2")
        session.add_all([a1, a2])
        a1.bs = [
            B(), B()
        ]
        session.flush()
        session.expire_all()
        a1, a2 = session.query(A).all()

        for b in list(a1.bs):
            b.a = a2
        session.delete(a1)
        session.flush()

class RelationshipToSelectableTest(fixtures.MappedTest):
    """Test a map to a select that relates to a map to the table."""

    @classmethod
    def define_tables(cls, metadata):
        Table('items', metadata,
              Column('item_policy_num', String(10), primary_key=True,
                     key='policyNum'),
              Column('item_policy_eff_date', sa.Date, primary_key=True,
                     key='policyEffDate'),
              Column('item_type', String(20), primary_key=True,
                     key='type'),
              Column('item_id', Integer, primary_key=True,
                     key='id', autoincrement=False))

    def test_basic(self):
        items = self.tables.items

        class Container(fixtures.BasicEntity):
            pass
        class LineItem(fixtures.BasicEntity):
            pass

        container_select = sa.select(
            [items.c.policyNum, items.c.policyEffDate, items.c.type],
            distinct=True,
            ).alias('container_select')

        mapper(LineItem, items)

        mapper(Container,
               container_select,
               order_by=sa.asc(container_select.c.type),
               properties=dict(
                   lineItems=relationship(LineItem,
                       lazy='select',
                       cascade='all, delete-orphan',
                       order_by=sa.asc(items.c.id),
                       primaryjoin=sa.and_(
                         container_select.c.policyNum==items.c.policyNum,
                         container_select.c.policyEffDate==
                                                    items.c.policyEffDate,
                         container_select.c.type==items.c.type),
                       foreign_keys=[
                         items.c.policyNum,
                         items.c.policyEffDate,
                         items.c.type])))

        session = create_session()
        con = Container()
        con.policyNum = "99"
        con.policyEffDate = datetime.date.today()
        con.type = "TESTER"
        session.add(con)
        for i in range(0, 10):
            li = LineItem()
            li.id = i
            con.lineItems.append(li)
            session.add(li)
        session.flush()
        session.expunge_all()
        newcon = session.query(Container).first()
        assert con.policyNum == newcon.policyNum
        assert len(newcon.lineItems) == 10
        for old, new in zip(con.lineItems, newcon.lineItems):
            eq_(old.id, new.id)

class FKEquatedToConstantTest(fixtures.MappedTest):
    """test a relationship with a non-column entity in the primary join, 
    is not viewonly, and also has the non-column's clause mentioned in the 
    foreign keys list.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table('tags', metadata, Column("id", Integer, primary_key=True, 
                                            test_needs_autoincrement=True),
            Column("data", String(50)),
        )

        Table('tag_foo', metadata, 
            Column("id", Integer, primary_key=True, 
                                        test_needs_autoincrement=True),
            Column('tagid', Integer),
            Column("data", String(50)),
        )

    def test_basic(self):
        tag_foo, tags = self.tables.tag_foo, self.tables.tags

        class Tag(fixtures.ComparableEntity):
            pass
        class TagInstance(fixtures.ComparableEntity):
            pass

        mapper(Tag, tags, properties={
            'foo':relationship(TagInstance, 
               primaryjoin=sa.and_(tag_foo.c.data=='iplc_case',
                                tag_foo.c.tagid==tags.c.id),
               foreign_keys=[tag_foo.c.tagid, tag_foo.c.data],
               ),
        })

        mapper(TagInstance, tag_foo)

        sess = create_session()
        t1 = Tag(data='some tag')
        t1.foo.append(TagInstance(data='iplc_case'))
        t1.foo.append(TagInstance(data='not_iplc_case'))
        sess.add(t1)
        sess.flush()
        sess.expunge_all()

        # relationship works
        eq_(
            sess.query(Tag).all(), 
            [Tag(data='some tag', foo=[TagInstance(data='iplc_case')])]
        )

        # both TagInstances were persisted
        eq_(
            sess.query(TagInstance).order_by(TagInstance.data).all(), 
            [TagInstance(data='iplc_case'), TagInstance(data='not_iplc_case')]
        )

class BackrefPropagatesForwardsArgs(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata, 
            Column('id', Integer, primary_key=True, 
                                        test_needs_autoincrement=True),
            Column('name', String(50))
        )
        Table('addresses', metadata, 
            Column('id', Integer, primary_key=True, 
                                        test_needs_autoincrement=True),
            Column('user_id', Integer),
            Column('email', String(50))
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass
        class Address(cls.Comparable):
            pass

    def test_backref(self):
        User, Address, users, addresses = (self.classes.User,
                                self.classes.Address,
                                self.tables.users,
                                self.tables.addresses)


        mapper(User, users, properties={
            'addresses':relationship(Address, 
                        primaryjoin=addresses.c.user_id==users.c.id, 
                        foreign_keys=addresses.c.user_id,
                        backref='user')
        })
        mapper(Address, addresses)

        sess = sessionmaker()()
        u1 = User(name='u1', addresses=[Address(email='a1')])
        sess.add(u1)
        sess.commit()
        eq_(sess.query(Address).all(), [
            Address(email='a1', user=User(name='u1'))
        ])

class AmbiguousJoinInterpretedAsSelfRef(fixtures.MappedTest):
    """test ambiguous joins due to FKs on both sides treated as
    self-referential.

    this mapping is very similar to that of
    test/orm/inheritance/query.py
    SelfReferentialTestJoinedToBase , except that inheritance is
    not used here.

    """

    @classmethod
    def define_tables(cls, metadata):
        subscriber_table = Table('subscriber', metadata,
           Column('id', Integer, primary_key=True, 
                            test_needs_autoincrement=True),
           Column('dummy', String(10)) # to appease older sqlite version
          )

        address_table = Table('address',
                 metadata,
                 Column('subscriber_id', Integer, 
                            ForeignKey('subscriber.id'), primary_key=True),
                 Column('type', String(1), primary_key=True),
                 )

    @classmethod
    def setup_mappers(cls):
        subscriber, address = cls.tables.subscriber, cls.tables.address

        subscriber_and_address = subscriber.join(address, 
            and_(address.c.subscriber_id==subscriber.c.id, 
                address.c.type.in_(['A', 'B', 'C'])))

        class Address(cls.Comparable):
            pass

        class Subscriber(cls.Comparable):
            pass

        mapper(Address, address)

        mapper(Subscriber, subscriber_and_address, properties={
           'id':[subscriber.c.id, address.c.subscriber_id],
           'addresses' : relationship(Address, 
                backref=backref("customer"))
           })

    def test_mapping(self):
        Subscriber, Address = self.classes.Subscriber, self.classes.Address

        from sqlalchemy.orm.interfaces import ONETOMANY, MANYTOONE
        sess = create_session()
        assert Subscriber.addresses.property.direction is ONETOMANY
        assert Address.customer.property.direction is MANYTOONE

        s1 = Subscriber(type='A',
                addresses = [
                    Address(type='D'),
                    Address(type='E'),
                ]
        )
        a1 = Address(type='B', customer=Subscriber(type='C'))

        assert s1.addresses[0].customer is s1
        assert a1.customer.addresses[0] is a1

        sess.add_all([s1, a1])

        sess.flush()
        sess.expunge_all()

        eq_(
            sess.query(Subscriber).order_by(Subscriber.type).all(),
            [
                Subscriber(id=1, type=u'A'), 
                Subscriber(id=2, type=u'B'), 
                Subscriber(id=2, type=u'C')
            ]
        )


class ManualBackrefTest(_fixtures.FixtureTest):
    """Test explicit relationships that are backrefs to each other."""

    run_inserts = None

    def test_o2m(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':relationship(Address, back_populates='user')
        })

        mapper(Address, addresses, properties={
            'user':relationship(User, back_populates='addresses')
        })

        sess = create_session()

        u1 = User(name='u1')
        a1 = Address(email_address='foo')
        u1.addresses.append(a1)
        assert a1.user is u1

        sess.add(u1)
        sess.flush()
        sess.expire_all()
        assert sess.query(Address).one() is a1
        assert a1.user is u1
        assert a1 in u1.addresses

    def test_invalid_key(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':relationship(Address, back_populates='userr')
        })

        mapper(Address, addresses, properties={
            'user':relationship(User, back_populates='addresses')
        })

        assert_raises(sa.exc.InvalidRequestError, configure_mappers)

    def test_invalid_target(self):
        addresses, Dingaling, User, dingalings, Address, users = (self.tables.addresses,
                                self.classes.Dingaling,
                                self.classes.User,
                                self.tables.dingalings,
                                self.classes.Address,
                                self.tables.users)

        mapper(User, users, properties={
            'addresses':relationship(Address, back_populates='dingaling'),
        })

        mapper(Dingaling, dingalings)
        mapper(Address, addresses, properties={
            'dingaling':relationship(Dingaling)
        })

        assert_raises_message(sa.exc.ArgumentError, 
            r"reverse_property 'dingaling' on relationship "
            "User.addresses references "
            "relationship Address.dingaling, which does not "
            "reference mapper Mapper\|User\|users", 
            configure_mappers)

class JoinConditionErrorTest(fixtures.TestBase):

    def test_clauseelement_pj(self):
        from sqlalchemy.ext.declarative import declarative_base
        Base = declarative_base()
        class C1(Base):
            __tablename__ = 'c1'
            id = Column('id', Integer, primary_key=True)
        class C2(Base):
            __tablename__ = 'c2'
            id = Column('id', Integer, primary_key=True)
            c1id = Column('c1id', Integer, ForeignKey('c1.id'))
            c2 = relationship(C1, primaryjoin=C1.id)

        assert_raises(sa.exc.ArgumentError, configure_mappers)

    def test_clauseelement_pj_false(self):
        from sqlalchemy.ext.declarative import declarative_base
        Base = declarative_base()
        class C1(Base):
            __tablename__ = 'c1'
            id = Column('id', Integer, primary_key=True)
        class C2(Base):
            __tablename__ = 'c2'
            id = Column('id', Integer, primary_key=True)
            c1id = Column('c1id', Integer, ForeignKey('c1.id'))
            c2 = relationship(C1, primaryjoin="x"=="y")

        assert_raises(sa.exc.ArgumentError, configure_mappers)

    def test_only_column_elements(self):
        m = MetaData()
        t1 = Table('t1', m, 
            Column('id', Integer, primary_key=True),
            Column('foo_id', Integer, ForeignKey('t2.id')),
        )
        t2 = Table('t2', m,
            Column('id', Integer, primary_key=True),
            )
        class C1(object):
            pass
        class C2(object):
            pass

        mapper(C1, t1, properties={'c2':relationship(C2,
                                            primaryjoin=t1.join(t2))})
        mapper(C2, t2)
        assert_raises(sa.exc.ArgumentError, configure_mappers)

    def test_invalid_string_args(self):
        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy import util

        for argname, arg in [
            ('remote_side', ['c1.id']),
            ('remote_side', ['id']),
            ('foreign_keys', ['c1id']),
            ('foreign_keys', ['C2.c1id']),
            ('order_by', ['id']),
        ]:
            clear_mappers()
            kw = {argname:arg}
            Base = declarative_base()
            class C1(Base):
                __tablename__ = 'c1'
                id = Column('id', Integer, primary_key=True)

            class C2(Base):
                __tablename__ = 'c2'
                id_ = Column('id', Integer, primary_key=True)
                c1id = Column('c1id', Integer, ForeignKey('c1.id'))
                c2 = relationship(C1, **kw)

            assert_raises_message(
                sa.exc.ArgumentError, 
                "Column-based expression object expected "
                "for argument '%s'; got: '%s', type %r" % 
                (argname, arg[0], type(arg[0])),
                configure_mappers)


    def test_fk_error_not_raised_unrelated(self):
        m = MetaData()
        t1 = Table('t1', m, 
            Column('id', Integer, primary_key=True),
            Column('foo_id', Integer, ForeignKey('t2.nonexistent_id')),
        )
        t2 = Table('t2', m,
            Column('id', Integer, primary_key=True),
            )

        t3 = Table('t3', m,
            Column('id', Integer, primary_key=True),
            Column('t1id', Integer, ForeignKey('t1.id'))
        )

        class C1(object):
            pass
        class C2(object):
            pass

        mapper(C1, t1, properties={'c2':relationship(C2)})
        mapper(C2, t3)
        assert C1.c2.property.primaryjoin.compare(t1.c.id==t3.c.t1id)

    def test_join_error_raised(self):
        m = MetaData()
        t1 = Table('t1', m, 
            Column('id', Integer, primary_key=True),
        )
        t2 = Table('t2', m,
            Column('id', Integer, primary_key=True),
            )

        t3 = Table('t3', m,
            Column('id', Integer, primary_key=True),
            Column('t1id', Integer)
        )

        class C1(object):
            pass
        class C2(object):
            pass

        mapper(C1, t1, properties={'c2':relationship(C2)})
        mapper(C2, t3)

        assert_raises(sa.exc.ArgumentError, configure_mappers)

    def teardown(self):
        clear_mappers()

class TypeMatchTest(fixtures.MappedTest):
    """test errors raised when trying to add items 
        whose type is not handled by a relationship"""

    @classmethod
    def define_tables(cls, metadata):
        Table("a", metadata,
              Column('aid', Integer, primary_key=True, 
                                test_needs_autoincrement=True),
              Column('data', String(30)))
        Table("b", metadata,
               Column('bid', Integer, primary_key=True, 
                                test_needs_autoincrement=True),
               Column("a_id", Integer, ForeignKey("a.aid")),
               Column('data', String(30)))
        Table("c", metadata,
              Column('cid', Integer, primary_key=True, 
                                test_needs_autoincrement=True),
              Column("b_id", Integer, ForeignKey("b.bid")),
              Column('data', String(30)))
        Table("d", metadata,
              Column('did', Integer, primary_key=True, 
                                test_needs_autoincrement=True),
              Column("a_id", Integer, ForeignKey("a.aid")),
              Column('data', String(30)))

    def test_o2m_oncascade(self):
        a, c, b = (self.tables.a,
                                self.tables.c,
                                self.tables.b)

        class A(fixtures.BasicEntity): pass
        class B(fixtures.BasicEntity): pass
        class C(fixtures.BasicEntity): pass
        mapper(A, a, properties={'bs':relationship(B)})
        mapper(B, b)
        mapper(C, c)

        a1 = A()
        b1 = B()
        c1 = C()
        a1.bs.append(b1)
        a1.bs.append(c1)
        sess = create_session()
        try:
            sess.add(a1)
            assert False
        except AssertionError, err:
            eq_(str(err),
                "Attribute 'bs' on class '%s' doesn't handle "
                "objects of type '%s'" % (A, C))

    def test_o2m_onflush(self):
        a, c, b = (self.tables.a,
                                self.tables.c,
                                self.tables.b)

        class A(fixtures.BasicEntity): pass
        class B(fixtures.BasicEntity): pass
        class C(fixtures.BasicEntity): pass
        mapper(A, a, properties={'bs':relationship(B, cascade="none")})
        mapper(B, b)
        mapper(C, c)

        a1 = A()
        b1 = B()
        c1 = C()
        a1.bs.append(b1)
        a1.bs.append(c1)
        sess = create_session()
        sess.add(a1)
        sess.add(b1)
        sess.add(c1)
        assert_raises_message(sa.orm.exc.FlushError,
                                 "Attempting to flush an item", 
                                 sess.flush)

    def test_o2m_nopoly_onflush(self):
        a, c, b = (self.tables.a,
                                self.tables.c,
                                self.tables.b)

        class A(fixtures.BasicEntity): pass
        class B(fixtures.BasicEntity): pass
        class C(B): pass
        mapper(A, a, properties={'bs':relationship(B, cascade="none")})
        mapper(B, b)
        mapper(C, c, inherits=B)

        a1 = A()
        b1 = B()
        c1 = C()
        a1.bs.append(b1)
        a1.bs.append(c1)
        sess = create_session()
        sess.add(a1)
        sess.add(b1)
        sess.add(c1)
        assert_raises_message(sa.orm.exc.FlushError,
                                 "Attempting to flush an item", 
                                 sess.flush)

    def test_m2o_nopoly_onflush(self):
        a, b, d = (self.tables.a,
                                self.tables.b,
                                self.tables.d)

        class A(fixtures.BasicEntity): pass
        class B(A): pass
        class D(fixtures.BasicEntity): pass
        mapper(A, a)
        mapper(B, b, inherits=A)
        mapper(D, d, properties={"a":relationship(A, cascade="none")})
        b1 = B()
        d1 = D()
        d1.a = b1
        sess = create_session()
        sess.add(b1)
        sess.add(d1)
        assert_raises_message(sa.orm.exc.FlushError,
                                 "Attempting to flush an item", 
                                 sess.flush)

    def test_m2o_oncascade(self):
        a, b, d = (self.tables.a,
                                self.tables.b,
                                self.tables.d)

        class A(fixtures.BasicEntity): pass
        class B(fixtures.BasicEntity): pass
        class D(fixtures.BasicEntity): pass
        mapper(A, a)
        mapper(B, b)
        mapper(D, d, properties={"a":relationship(A)})
        b1 = B()
        d1 = D()
        d1.a = b1
        sess = create_session()
        assert_raises_message(AssertionError,
                             "doesn't handle objects of type", 
                             sess.add, d1)

class TypedAssociationTable(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        class MySpecialType(sa.types.TypeDecorator):
            impl = String
            def process_bind_param(self, value, dialect):
                return "lala" + value
            def process_result_value(self, value, dialect):
                return value[4:]

        Table('t1', metadata,
              Column('col1', MySpecialType(30), primary_key=True),
              Column('col2', String(30)))
        Table('t2', metadata,
              Column('col1', MySpecialType(30), primary_key=True),
              Column('col2', String(30)))
        Table('t3', metadata,
              Column('t1c1', MySpecialType(30), ForeignKey('t1.col1')),
              Column('t2c1', MySpecialType(30), ForeignKey('t2.col1')))

    def testm2m(self):
        """Many-to-many tables with special types for candidate keys."""

        t2, t3, t1 = (self.tables.t2,
                                self.tables.t3,
                                self.tables.t1)


        class T1(fixtures.BasicEntity): pass
        class T2(fixtures.BasicEntity): pass
        mapper(T2, t2)
        mapper(T1, t1, properties={
            't2s':relationship(T2, secondary=t3, backref='t1s')})

        a = T1()
        a.col1 = "aid"
        b = T2()
        b.col1 = "bid"
        c = T2()
        c.col1 = "cid"
        a.t2s.append(b)
        a.t2s.append(c)
        sess = create_session()
        sess.add(a)
        sess.flush()

        assert t3.count().scalar() == 2

        a.t2s.remove(c)
        sess.flush()

        assert t3.count().scalar() == 1

class ViewOnlyM2MBackrefTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table("t1", metadata,
            Column('id', Integer, primary_key=True, 
                                test_needs_autoincrement=True),
            Column('data', String(40)))
        Table("t2", metadata,
            Column('id', Integer, primary_key=True, 
                                test_needs_autoincrement=True),
            Column('data', String(40)),
        )
        Table("t1t2", metadata,
            Column('t1id', Integer, ForeignKey('t1.id'), primary_key=True),
            Column('t2id', Integer, ForeignKey('t2.id'), primary_key=True),
        )

    def test_viewonly(self):
        t1t2, t2, t1 = (self.tables.t1t2,
                                self.tables.t2,
                                self.tables.t1)

        class A(fixtures.ComparableEntity):pass
        class B(fixtures.ComparableEntity):pass

        mapper(A, t1, properties={
            'bs':relationship(B, secondary=t1t2, 
                                backref=backref('as_', viewonly=True))
        })
        mapper(B, t2)

        sess = create_session()
        a1 = A()
        b1 = B(as_=[a1])

        sess.add(a1)
        sess.flush()
        eq_(
            sess.query(A).first(), A(bs=[B(id=b1.id)])
        )
        eq_(
            sess.query(B).first(), B(as_=[A(id=a1.id)])
        )

class ViewOnlyOverlappingNames(fixtures.MappedTest):
    """'viewonly' mappings with overlapping PK column names."""

    @classmethod
    def define_tables(cls, metadata):
        Table("t1", metadata,
            Column('id', Integer, primary_key=True, 
                                    test_needs_autoincrement=True),
            Column('data', String(40)))
        Table("t2", metadata,
            Column('id', Integer, primary_key=True, 
                                    test_needs_autoincrement=True),
            Column('data', String(40)),
            Column('t1id', Integer, ForeignKey('t1.id')))
        Table("t3", metadata,
            Column('id', Integer, primary_key=True, 
                                    test_needs_autoincrement=True),
            Column('data', String(40)),
            Column('t2id', Integer, ForeignKey('t2.id')))

    def test_three_table_view(self):
        """A three table join with overlapping PK names.

        A third table is pulled into the primary join condition using
        overlapping PK column names and should not produce 'conflicting column'
        error.

        """

        t2, t3, t1 = (self.tables.t2,
                                self.tables.t3,
                                self.tables.t1)

        class C1(fixtures.BasicEntity): pass
        class C2(fixtures.BasicEntity): pass
        class C3(fixtures.BasicEntity): pass

        mapper(C1, t1, properties={
            't2s':relationship(C2),
            't2_view':relationship(C2,
                               viewonly=True,
                               primaryjoin=sa.and_(t1.c.id==t2.c.t1id,
                                                   t3.c.t2id==t2.c.id,
                                                   t3.c.data==t1.c.data))})
        mapper(C2, t2)
        mapper(C3, t3, properties={
            't2':relationship(C2)})

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
        sess.add(c1)
        sess.add(c3)
        sess.flush()
        sess.expunge_all()

        c1 = sess.query(C1).get(c1.id)
        assert set([x.id for x in c1.t2s]) == set([c2a.id, c2b.id])
        assert set([x.id for x in c1.t2_view]) == set([c2b.id])

class ViewOnlyUniqueNames(fixtures.MappedTest):
    """'viewonly' mappings with unique PK column names."""

    @classmethod
    def define_tables(cls, metadata):
        Table("t1", metadata,
            Column('t1id', Integer, primary_key=True, 
                                        test_needs_autoincrement=True),
            Column('data', String(40)))
        Table("t2", metadata,
            Column('t2id', Integer, primary_key=True, 
                                        test_needs_autoincrement=True),
            Column('data', String(40)),
            Column('t1id_ref', Integer, ForeignKey('t1.t1id')))
        Table("t3", metadata,
            Column('t3id', Integer, primary_key=True, 
                                        test_needs_autoincrement=True),
            Column('data', String(40)),
            Column('t2id_ref', Integer, ForeignKey('t2.t2id')))

    def test_three_table_view(self):
        """A three table join with overlapping PK names.

        A third table is pulled into the primary join condition using unique
        PK column names and should not produce 'mapper has no columnX' error.

        """

        t2, t3, t1 = (self.tables.t2,
                                self.tables.t3,
                                self.tables.t1)

        class C1(fixtures.BasicEntity): pass
        class C2(fixtures.BasicEntity): pass
        class C3(fixtures.BasicEntity): pass

        mapper(C1, t1, properties={
            't2s':relationship(C2),
            't2_view':relationship(C2,
                               viewonly=True,
                               primaryjoin=sa.and_(t1.c.t1id==t2.c.t1id_ref,
                                                   t3.c.t2id_ref==t2.c.t2id,
                                                   t3.c.data==t1.c.data))})
        mapper(C2, t2)
        mapper(C3, t3, properties={
            't2':relationship(C2)})

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

        sess.add_all((c1, c3))
        sess.flush()
        sess.expunge_all()

        c1 = sess.query(C1).get(c1.t1id)
        assert set([x.t2id for x in c1.t2s]) == set([c2a.t2id, c2b.t2id])
        assert set([x.t2id for x in c1.t2_view]) == set([c2b.t2id])

class ViewOnlyLocalRemoteM2M(fixtures.TestBase):
    """test that local-remote is correctly determined for m2m"""

    def test_local_remote(self):
        meta = MetaData()

        t1 = Table('t1', meta,
                Column('id', Integer, primary_key=True),
            )
        t2 = Table('t2', meta,
                Column('id', Integer, primary_key=True),
            )
        t12 = Table('tab', meta,
                Column('t1_id', Integer, ForeignKey('t1.id',)),
                Column('t2_id', Integer, ForeignKey('t2.id',)),
            )

        class A(object): pass
        class B(object): pass
        mapper( B, t2, )
        m = mapper( A, t1, properties=dict(
                b_view = relationship( B, secondary=t12, viewonly=True),
                b_plain= relationship( B, secondary=t12),
            )
        )
        configure_mappers()
        assert m.get_property('b_view').local_remote_pairs == \
            m.get_property('b_plain').local_remote_pairs == \
            [(t1.c.id, t12.c.t1_id), (t2.c.id, t12.c.t2_id)]



class ViewOnlyNonEquijoin(fixtures.MappedTest):
    """'viewonly' mappings based on non-equijoins."""

    @classmethod
    def define_tables(cls, metadata):
        Table('foos', metadata,
                     Column('id', Integer, primary_key=True))
        Table('bars', metadata,
                     Column('id', Integer, primary_key=True),
                     Column('fid', Integer))

    def test_viewonly_join(self):
        bars, foos = self.tables.bars, self.tables.foos

        class Foo(fixtures.ComparableEntity):
            pass
        class Bar(fixtures.ComparableEntity):
            pass

        mapper(Foo, foos, properties={
            'bars':relationship(Bar,
                            primaryjoin=foos.c.id > bars.c.fid,
                            foreign_keys=[bars.c.fid],
                            viewonly=True)})

        mapper(Bar, bars)

        sess = create_session()
        sess.add_all((Foo(id=4),
                      Foo(id=9),
                      Bar(id=1, fid=2),
                      Bar(id=2, fid=3),
                      Bar(id=3, fid=6),
                      Bar(id=4, fid=7)))
        sess.flush()

        sess = create_session()
        eq_(sess.query(Foo).filter_by(id=4).one(),
            Foo(id=4, bars=[Bar(fid=2), Bar(fid=3)]))
        eq_(sess.query(Foo).filter_by(id=9).one(),
            Foo(id=9, bars=[Bar(fid=2), Bar(fid=3), Bar(fid=6), Bar(fid=7)]))


class ViewOnlyRepeatedRemoteColumn(fixtures.MappedTest):
    """'viewonly' mappings that contain the same 'remote' column twice"""

    @classmethod
    def define_tables(cls, metadata):
        Table('foos', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('bid1', Integer,ForeignKey('bars.id')),
              Column('bid2', Integer,ForeignKey('bars.id')))

        Table('bars', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('data', String(50)))

    def test_relationship_on_or(self):
        bars, foos = self.tables.bars, self.tables.foos

        class Foo(fixtures.ComparableEntity):
            pass
        class Bar(fixtures.ComparableEntity):
            pass

        mapper(Foo, foos, properties={
            'bars':relationship(Bar,
                            primaryjoin=sa.or_(bars.c.id == foos.c.bid1,
                                               bars.c.id == foos.c.bid2),
                            uselist=True,
                            viewonly=True)})
        mapper(Bar, bars)

        sess = create_session()
        b1 = Bar(id=1, data='b1')
        b2 = Bar(id=2, data='b2')
        b3 = Bar(id=3, data='b3')
        f1 = Foo(bid1=1, bid2=2)
        f2 = Foo(bid1=3, bid2=None)

        sess.add_all((b1, b2, b3))
        sess.flush()

        sess.add_all((f1, f2))
        sess.flush()

        sess.expunge_all()
        eq_(sess.query(Foo).filter_by(id=f1.id).one(),
            Foo(bars=[Bar(data='b1'), Bar(data='b2')]))
        eq_(sess.query(Foo).filter_by(id=f2.id).one(),
            Foo(bars=[Bar(data='b3')]))

class ViewOnlyRepeatedLocalColumn(fixtures.MappedTest):
    """'viewonly' mappings that contain the same 'local' column twice"""

    @classmethod
    def define_tables(cls, metadata):
        Table('foos', metadata,
              Column('id', Integer, primary_key=True, 
                                        test_needs_autoincrement=True),
              Column('data', String(50)))

        Table('bars', metadata, Column('id', Integer, primary_key=True, 
                                        test_needs_autoincrement=True),
              Column('fid1', Integer, ForeignKey('foos.id')),
              Column('fid2', Integer, ForeignKey('foos.id')),
              Column('data', String(50)))

    def test_relationship_on_or(self):
        bars, foos = self.tables.bars, self.tables.foos

        class Foo(fixtures.ComparableEntity):
            pass
        class Bar(fixtures.ComparableEntity):
            pass

        mapper(Foo, foos, properties={
            'bars':relationship(Bar,
                            primaryjoin=sa.or_(bars.c.fid1 == foos.c.id,
                                               bars.c.fid2 == foos.c.id),
                            viewonly=True)})
        mapper(Bar, bars)

        sess = create_session()
        f1 = Foo(id=1, data='f1')
        f2 = Foo(id=2, data='f2')
        b1 = Bar(fid1=1, data='b1')
        b2 = Bar(fid2=1, data='b2')
        b3 = Bar(fid1=2, data='b3')
        b4 = Bar(fid1=1, fid2=2, data='b4')

        sess.add_all((f1, f2))
        sess.flush()

        sess.add_all((b1, b2, b3, b4))
        sess.flush()

        sess.expunge_all()
        eq_(sess.query(Foo).filter_by(id=f1.id).one(),
            Foo(bars=[Bar(data='b1'), Bar(data='b2'), Bar(data='b4')]))
        eq_(sess.query(Foo).filter_by(id=f2.id).one(),
            Foo(bars=[Bar(data='b3'), Bar(data='b4')]))

class ViewOnlyComplexJoin(fixtures.MappedTest):
    """'viewonly' mappings with a complex join condition."""

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
            Column('id', Integer, primary_key=True, 
                                    test_needs_autoincrement=True),
            Column('data', String(50)))
        Table('t2', metadata,
            Column('id', Integer, primary_key=True, 
                                    test_needs_autoincrement=True),
            Column('data', String(50)),
            Column('t1id', Integer, ForeignKey('t1.id')))
        Table('t3', metadata,
            Column('id', Integer, primary_key=True, 
                                    test_needs_autoincrement=True),
            Column('data', String(50)))
        Table('t2tot3', metadata,
            Column('t2id', Integer, ForeignKey('t2.id')),
            Column('t3id', Integer, ForeignKey('t3.id')))

    @classmethod
    def setup_classes(cls):
        class T1(cls.Comparable):
            pass
        class T2(cls.Comparable):
            pass
        class T3(cls.Comparable):
            pass

    def test_basic(self):
        T1, t2, T2, T3, t3, t2tot3, t1 = (self.classes.T1,
                                self.tables.t2,
                                self.classes.T2,
                                self.classes.T3,
                                self.tables.t3,
                                self.tables.t2tot3,
                                self.tables.t1)

        mapper(T1, t1, properties={
            't3s':relationship(T3, primaryjoin=sa.and_(
                t1.c.id==t2.c.t1id,
                t2.c.id==t2tot3.c.t2id,
                t3.c.id==t2tot3.c.t3id),
            viewonly=True,
            foreign_keys=t3.c.id, remote_side=t2.c.t1id)
        })
        mapper(T2, t2, properties={
            't1':relationship(T1),
            't3s':relationship(T3, secondary=t2tot3)
        })
        mapper(T3, t3)

        sess = create_session()
        sess.add(T2(data='t2', t1=T1(data='t1'), t3s=[T3(data='t3')]))
        sess.flush()
        sess.expunge_all()

        a = sess.query(T1).first()
        eq_(a.t3s, [T3(data='t3')])


    def test_remote_side_escalation(self):
        T1, t2, T2, T3, t3, t2tot3, t1 = (self.classes.T1,
                                self.tables.t2,
                                self.classes.T2,
                                self.classes.T3,
                                self.tables.t3,
                                self.tables.t2tot3,
                                self.tables.t1)

        mapper(T1, t1, properties={
            't3s':relationship(T3,
                           primaryjoin=sa.and_(t1.c.id==t2.c.t1id,
                                               t2.c.id==t2tot3.c.t2id,
                                               t3.c.id==t2tot3.c.t3id
                                               ),
                           viewonly=True,
                           foreign_keys=t3.c.id)})
        mapper(T2, t2, properties={
            't1':relationship(T1),
            't3s':relationship(T3, secondary=t2tot3)})
        mapper(T3, t3)
        assert_raises_message(sa.exc.ArgumentError,
                                 "Specify remote_side argument",
                                 sa.orm.configure_mappers)


class ExplicitLocalRemoteTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
            Column('id', String(50), primary_key=True, 
                                        test_needs_autoincrement=True),
            Column('data', String(50)))
        Table('t2', metadata,
            Column('id', Integer, primary_key=True, 
                                        test_needs_autoincrement=True),
            Column('data', String(50)),
            Column('t1id', String(50)))

    @classmethod
    def setup_classes(cls):
        class T1(cls.Comparable):
            pass
        class T2(cls.Comparable):
            pass

    def test_onetomany_funcfk(self):
        T2, T1, t2, t1 = (self.classes.T2,
                                self.classes.T1,
                                self.tables.t2,
                                self.tables.t1)

        # use a function within join condition.  but specifying
        # local_remote_pairs overrides all parsing of the join condition.
        mapper(T1, t1, properties={
            't2s':relationship(T2,
                           primaryjoin=t1.c.id==sa.func.lower(t2.c.t1id),
                           _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
                           foreign_keys=[t2.c.t1id])})
        mapper(T2, t2)

        sess = create_session()
        a1 = T1(id='number1', data='a1')
        a2 = T1(id='number2', data='a2')
        b1 = T2(data='b1', t1id='NuMbEr1')
        b2 = T2(data='b2', t1id='Number1')
        b3 = T2(data='b3', t1id='Number2')
        sess.add_all((a1, a2, b1, b2, b3))
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(T1).first(),
            T1(id='number1', data='a1', t2s=[
               T2(data='b1', t1id='NuMbEr1'),
               T2(data='b2', t1id='Number1')]))

    def test_manytoone_funcfk(self):
        T2, T1, t2, t1 = (self.classes.T2,
                                self.classes.T1,
                                self.tables.t2,
                                self.tables.t1)

        mapper(T1, t1)
        mapper(T2, t2, properties={
            't1':relationship(T1,
                          primaryjoin=t1.c.id==sa.func.lower(t2.c.t1id),
                          _local_remote_pairs=[(t2.c.t1id, t1.c.id)],
                          foreign_keys=[t2.c.t1id],
                          uselist=True)})

        sess = create_session()
        a1 = T1(id='number1', data='a1')
        a2 = T1(id='number2', data='a2')
        b1 = T2(data='b1', t1id='NuMbEr1')
        b2 = T2(data='b2', t1id='Number1')
        b3 = T2(data='b3', t1id='Number2')
        sess.add_all((a1, a2, b1, b2, b3))
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(T2).filter(T2.data.in_(['b1', 'b2'])).all(),
            [T2(data='b1', t1=[T1(id='number1', data='a1')]),
             T2(data='b2', t1=[T1(id='number1', data='a1')])])

    def test_onetomany_func_referent(self):
        T2, T1, t2, t1 = (self.classes.T2,
                                self.classes.T1,
                                self.tables.t2,
                                self.tables.t1)

        mapper(T1, t1, properties={
            't2s':relationship(T2,
                           primaryjoin=sa.func.lower(t1.c.id)==t2.c.t1id,
                           _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
                           foreign_keys=[t2.c.t1id])})
        mapper(T2, t2)

        sess = create_session()
        a1 = T1(id='NuMbeR1', data='a1')
        a2 = T1(id='NuMbeR2', data='a2')
        b1 = T2(data='b1', t1id='number1')
        b2 = T2(data='b2', t1id='number1')
        b3 = T2(data='b2', t1id='number2')
        sess.add_all((a1, a2, b1, b2, b3))
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(T1).first(),
            T1(id='NuMbeR1', data='a1', t2s=[
              T2(data='b1', t1id='number1'),
              T2(data='b2', t1id='number1')]))

    def test_manytoone_func_referent(self):
        T2, T1, t2, t1 = (self.classes.T2,
                                self.classes.T1,
                                self.tables.t2,
                                self.tables.t1)

        mapper(T1, t1)
        mapper(T2, t2, properties={
            't1':relationship(T1,
                          primaryjoin=sa.func.lower(t1.c.id)==t2.c.t1id,
                          _local_remote_pairs=[(t2.c.t1id, t1.c.id)],
                          foreign_keys=[t2.c.t1id], uselist=True)})

        sess = create_session()
        a1 = T1(id='NuMbeR1', data='a1')
        a2 = T1(id='NuMbeR2', data='a2')
        b1 = T2(data='b1', t1id='number1')
        b2 = T2(data='b2', t1id='number1')
        b3 = T2(data='b3', t1id='number2')
        sess.add_all((a1, a2, b1, b2, b3))
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(T2).filter(T2.data.in_(['b1', 'b2'])).all(),
            [T2(data='b1', t1=[T1(id='NuMbeR1', data='a1')]),
             T2(data='b2', t1=[T1(id='NuMbeR1', data='a1')])])

    def test_escalation_1(self):
        T2, T1, t2, t1 = (self.classes.T2,
                                self.classes.T1,
                                self.tables.t2,
                                self.tables.t1)

        mapper(T1, t1, properties={
            't2s':relationship(T2,
                           primaryjoin=t1.c.id==sa.func.lower(t2.c.t1id),
                           _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
                           foreign_keys=[t2.c.t1id],
                           remote_side=[t2.c.t1id])})
        mapper(T2, t2)
        assert_raises(sa.exc.ArgumentError, sa.orm.configure_mappers)

    def test_escalation_2(self):
        T2, T1, t2, t1 = (self.classes.T2,
                                self.classes.T1,
                                self.tables.t2,
                                self.tables.t1)

        mapper(T1, t1, properties={
            't2s':relationship(T2,
                           primaryjoin=t1.c.id==sa.func.lower(t2.c.t1id),
                           _local_remote_pairs=[(t1.c.id, t2.c.t1id)])})
        mapper(T2, t2)
        assert_raises(sa.exc.ArgumentError, sa.orm.configure_mappers)

class InvalidRemoteSideTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(50)),
            Column('t_id', Integer, ForeignKey('t1.id'))
            )

    @classmethod
    def setup_classes(cls):
        class T1(cls.Comparable):
            pass

    def test_o2m_backref(self):
        T1, t1 = self.classes.T1, self.tables.t1

        mapper(T1, t1, properties={
            't1s':relationship(T1, backref='parent')
        })

        assert_raises_message(sa.exc.ArgumentError, 
            "T1.t1s and back-reference T1.parent are "
            "both of the same direction <symbol 'ONETOMANY>.  Did you "
            "mean to set remote_side on the many-to-one side ?", 
            configure_mappers)

    def test_m2o_backref(self):
        T1, t1 = self.classes.T1, self.tables.t1

        mapper(T1, t1, properties={
            't1s':relationship(T1, 
                        backref=backref('parent', remote_side=t1.c.id), 
                        remote_side=t1.c.id)
        })

        assert_raises_message(sa.exc.ArgumentError, 
            "T1.t1s and back-reference T1.parent are "
            "both of the same direction <symbol 'MANYTOONE>.  Did you "
            "mean to set remote_side on the many-to-one side ?", 
            configure_mappers)

    def test_o2m_explicit(self):
        T1, t1 = self.classes.T1, self.tables.t1

        mapper(T1, t1, properties={
            't1s':relationship(T1, back_populates='parent'),
            'parent':relationship(T1, back_populates='t1s'),
        })

        # can't be sure of ordering here
        assert_raises_message(sa.exc.ArgumentError, 
            "both of the same direction <symbol 'ONETOMANY>.  Did you "
            "mean to set remote_side on the many-to-one side ?", 
            configure_mappers)

    def test_m2o_explicit(self):
        T1, t1 = self.classes.T1, self.tables.t1

        mapper(T1, t1, properties={
            't1s':relationship(T1, back_populates='parent', 
                                remote_side=t1.c.id),
            'parent':relationship(T1, back_populates='t1s', 
                                remote_side=t1.c.id)
        })

        # can't be sure of ordering here
        assert_raises_message(sa.exc.ArgumentError, 
            "both of the same direction <symbol 'MANYTOONE>.  Did you "
            "mean to set remote_side on the many-to-one side ?", 
            configure_mappers)


class InvalidRelationshipEscalationTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('foos', metadata,
              Column('id', Integer, primary_key=True),
              Column('fid', Integer))
        Table('bars', metadata,
              Column('id', Integer, primary_key=True),
              Column('fid', Integer))

        Table('foos_with_fks', metadata,
            Column('id', Integer, primary_key=True),
            Column('fid', Integer, ForeignKey('foos_with_fks.id')))
        Table('bars_with_fks', metadata,
            Column('id', Integer, primary_key=True),
            Column('fid', Integer, ForeignKey('foos_with_fks.id')))

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass
        class Bar(cls.Basic):
            pass

    def test_no_join(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars':relationship(Bar)})
        mapper(Bar, bars)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not determine join condition between parent/child "
            "tables on relationship", sa.orm.configure_mappers)

    def test_no_join_self_ref(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'foos':relationship(Foo)})
        mapper(Bar, bars)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not determine join condition between parent/child "
            "tables on relationship", sa.orm.configure_mappers)

    def test_no_equated(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars':relationship(Bar,
                            primaryjoin=foos.c.id>bars.c.fid)})
        mapper(Bar, bars)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not determine relationship direction "
            "for primaryjoin condition",
            configure_mappers)

    def test_no_equated_fks(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars':relationship(Bar,
                            primaryjoin=foos.c.id>bars.c.fid,
                            foreign_keys=bars.c.fid)})
        mapper(Bar, bars)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not locate any foreign-key-equated, "
            "locally mapped column pairs for primaryjoin "
            "condition 'foos.id > bars.fid' on relationship "
            "Foo.bars.  For more relaxed rules on join "
            "conditions, the relationship may be marked as viewonly=True.",
            sa.orm.configure_mappers)

    def test_no_equated_wo_fks_works_on_relaxed(self):
        foos_with_fks, Foo, Bar, bars_with_fks, foos = (self.tables.foos_with_fks,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.bars_with_fks,
                                self.tables.foos)

        # very unique - the join between parent/child 
        # has no fks, but there is an fk join between two other
        # tables in the join condition, for those users that try creating
        # these big-long-string-of-joining-many-tables primaryjoins.
        # in this case we don't get eq_pairs, but we hit the "works if viewonly"
        # rule.  so here we add another clause regarding "try foreign keys".
        mapper(Foo, foos, properties={
            'bars':relationship(Bar,
                            primaryjoin=and_(
                                        bars_with_fks.c.fid==foos_with_fks.c.id,
                                        foos_with_fks.c.id==foos.c.id,
                                    )
                            )})
        mapper(Bar, bars_with_fks)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not locate any foreign-key-equated, locally mapped "
             "column pairs for primaryjoin condition "
             "'bars_with_fks.fid = foos_with_fks.id AND "
             "foos_with_fks.id = foos.id' on relationship Foo.bars.  "
             "Ensure that the referencing Column objects have a "
             "ForeignKey present, or are otherwise part of a "
             "ForeignKeyConstraint on their parent Table, or specify "
             "the foreign_keys parameter to this relationship.  For "
             "more relaxed rules on join conditions, the relationship "
             "may be marked as viewonly=True.", 
            sa.orm.configure_mappers)

    def test_ambiguous_fks(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars':relationship(Bar,
                            primaryjoin=foos.c.id==bars.c.fid,
                            foreign_keys=[foos.c.id, bars.c.fid])})
        mapper(Bar, bars)

        assert_raises_message(sa.exc.ArgumentError,
                              "Could not determine relationship "
                              "direction for primaryjoin condition "
                              "'foos.id = bars.fid', on relationship "
                              "Foo.bars, using manual 'foreign_keys' "
                              "setting.  Do the columns in "
                              "'foreign_keys' represent all, and only, "
                              "the 'foreign' columns in this join "
                              r"condition\?  Does the mapped Table "
                              "already have adequate ForeignKey and/or "
                              "ForeignKeyConstraint objects "
                              r"established \(in which case "
                              r"'foreign_keys' is usually unnecessary\)\?"
                              , sa.orm.configure_mappers)

    def test_ambiguous_remoteside_o2m(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars':relationship(Bar,
                            primaryjoin=foos.c.id==bars.c.fid,
                            foreign_keys=[bars.c.fid],
                            remote_side=[foos.c.id, bars.c.fid],
                            viewonly=True
                            )})
        mapper(Bar, bars)

        assert_raises_message(
            sa.exc.ArgumentError, 
                "could not determine any local/remote column pairs",
                sa.orm.configure_mappers)

    def test_ambiguous_remoteside_m2o(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars':relationship(Bar,
                            primaryjoin=foos.c.id==bars.c.fid,
                            foreign_keys=[foos.c.id],
                            remote_side=[foos.c.id, bars.c.fid],
                            viewonly=True
                            )})
        mapper(Bar, bars)

        assert_raises_message(
            sa.exc.ArgumentError, 
                "could not determine any local/remote column pairs",
                sa.orm.configure_mappers)


    def test_no_equated_self_ref(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'foos':relationship(Foo,
                            primaryjoin=foos.c.id>foos.c.fid)})
        mapper(Bar, bars)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not determine relationship direction for primaryjoin "
            "condition",
            configure_mappers)

    def test_no_equated_self_ref(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'foos':relationship(Foo,
                            primaryjoin=foos.c.id>foos.c.fid,
                            foreign_keys=[foos.c.fid])})
        mapper(Bar, bars)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not locate any foreign-key-equated, "
            "locally mapped column pairs for primaryjoin "
            "condition 'foos.id > foos.fid' on relationship "
            "Foo.foos.  For more relaxed rules on join "
            "conditions, the relationship may be marked as viewonly=True.",
            sa.orm.configure_mappers)

    def test_no_equated_viewonly(self):
        bars, Bar, bars_with_fks, foos_with_fks, Foo, foos = (self.tables.bars,
                                self.classes.Bar,
                                self.tables.bars_with_fks,
                                self.tables.foos_with_fks,
                                self.classes.Foo,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars':relationship(Bar,
                            primaryjoin=foos.c.id>bars.c.fid,
                            viewonly=True)})
        mapper(Bar, bars)

        assert_raises_message(sa.exc.ArgumentError,
                              'Could not determine relationship '
                              'direction for primaryjoin condition',
                              sa.orm.configure_mappers)

        sa.orm.clear_mappers()
        mapper(Foo, foos_with_fks, properties={
            'bars':relationship(Bar,
                        primaryjoin=foos_with_fks.c.id>bars_with_fks.c.fid,
                        viewonly=True)})
        mapper(Bar, bars_with_fks)
        sa.orm.configure_mappers()

    def test_no_equated_self_ref_viewonly(self):
        bars, Bar, bars_with_fks, foos_with_fks, Foo, foos = (self.tables.bars,
                                self.classes.Bar,
                                self.tables.bars_with_fks,
                                self.tables.foos_with_fks,
                                self.classes.Foo,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'foos':relationship(Foo,
                            primaryjoin=foos.c.id>foos.c.fid,
                            viewonly=True)})
        mapper(Bar, bars)

        assert_raises_message(sa.exc.ArgumentError,
                              "Could not determine relationship "
                              "direction for primaryjoin condition "
                              "'foos.id > foos.fid', on relationship "
                              "Foo.foos. Ensure that the referencing "
                              "Column objects have a ForeignKey "
                              "present, or are otherwise part of a "
                              "ForeignKeyConstraint on their parent "
                              "Table.", sa.orm.configure_mappers)

        sa.orm.clear_mappers()
        mapper(Foo, foos_with_fks, properties={
          'foos':relationship(Foo,
                          primaryjoin=foos_with_fks.c.id>foos_with_fks.c.fid,
                          viewonly=True)})
        mapper(Bar, bars_with_fks)
        sa.orm.configure_mappers()

    def test_no_equated_self_ref_viewonly_fks(self):
        Foo, foos = self.classes.Foo, self.tables.foos

        mapper(Foo, foos, properties={
            'foos':relationship(Foo,
                            primaryjoin=foos.c.id>foos.c.fid,
                            viewonly=True,
                            foreign_keys=[foos.c.fid])})

        sa.orm.configure_mappers()
        eq_(Foo.foos.property.local_remote_pairs, [(foos.c.id, foos.c.fid)])

    def test_equated(self):
        bars, Bar, bars_with_fks, foos_with_fks, Foo, foos = (self.tables.bars,
                                self.classes.Bar,
                                self.tables.bars_with_fks,
                                self.tables.foos_with_fks,
                                self.classes.Foo,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars':relationship(Bar,
                            primaryjoin=foos.c.id==bars.c.fid)})
        mapper(Bar, bars)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not determine relationship direction for primaryjoin "
            "condition",
            configure_mappers)

        sa.orm.clear_mappers()
        mapper(Foo, foos_with_fks, properties={
            'bars':relationship(Bar,
                        primaryjoin=foos_with_fks.c.id==bars_with_fks.c.fid)})
        mapper(Bar, bars_with_fks)
        sa.orm.configure_mappers()

    def test_equated_self_ref(self):
        Foo, foos = self.classes.Foo, self.tables.foos

        mapper(Foo, foos, properties={
            'foos':relationship(Foo,
                            primaryjoin=foos.c.id==foos.c.fid)})

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not determine relationship direction for primaryjoin "
            "condition",
            configure_mappers)


    def test_equated_self_ref_wrong_fks(self):
        bars, Foo, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'foos':relationship(Foo,
                            primaryjoin=foos.c.id==foos.c.fid,
                            foreign_keys=[bars.c.id])})

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not determine relationship direction for primaryjoin "
            "condition",
            configure_mappers)


class InvalidRelationshipEscalationTestM2M(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('foos', metadata,
              Column('id', Integer, primary_key=True))
        Table('foobars', metadata,
              Column('fid', Integer), Column('bid', Integer))
        Table('bars', metadata,
              Column('id', Integer, primary_key=True))

        Table('foobars_with_fks', metadata,
            Column('fid', Integer, ForeignKey('foos.id')), 
            Column('bid', Integer, ForeignKey('bars.id'))
        )

        Table('foobars_with_many_columns', metadata,
              Column('fid', Integer), 
              Column('bid', Integer),
              Column('fid1', Integer), 
              Column('bid1', Integer),
              Column('fid2', Integer), 
              Column('bid2', Integer),
              )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass
        class Bar(cls.Basic):
            pass

    def test_no_join(self):
        foobars, bars, Foo, Bar, foos = (self.tables.foobars,
                                self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar, secondary=foobars)})
        mapper(Bar, bars)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not determine join condition between parent/child tables "
            "on relationship", sa.orm.configure_mappers)

    def test_no_secondaryjoin(self):
        foobars, bars, Foo, Bar, foos = (self.tables.foobars,
                                self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                            secondary=foobars,
                            primaryjoin=foos.c.id > foobars.c.fid)})
        mapper(Bar, bars)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not determine join condition between parent/child tables "
            "on relationship",
            sa.orm.configure_mappers)

    def test_no_fks_warning_1(self):
        foobars_with_many_columns, bars, Bar, foobars, Foo, foos = (self.tables.foobars_with_many_columns,
                                self.tables.bars,
                                self.classes.Bar,
                                self.tables.foobars,
                                self.classes.Foo,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar, secondary=foobars, 
                                primaryjoin=foos.c.id==foobars.c.fid,
                                secondaryjoin=foobars.c.bid==bars.c.id)})
        mapper(Bar, bars)

        assert_raises_message(sa.exc.SAWarning,
                              "No ForeignKey objects were present in "
                              "secondary table 'foobars'.  Assumed "
                              "referenced foreign key columns "
                              "'foobars.bid', 'foobars.fid' for join "
                              "condition 'foos.id = foobars.fid' on "
                              "relationship Foo.bars",
                              sa.orm.configure_mappers)

        sa.orm.clear_mappers()
        mapper(Foo, foos, properties={
                        'bars': relationship(Bar, 
                            secondary=foobars_with_many_columns, 
                          primaryjoin=foos.c.id==
                                            foobars_with_many_columns.c.fid,
                          secondaryjoin=foobars_with_many_columns.c.bid==
                                                    bars.c.id)})
        mapper(Bar, bars)

        assert_raises_message(sa.exc.SAWarning,
                              "No ForeignKey objects were present in "
                              "secondary table 'foobars_with_many_colum"
                              "ns'.  Assumed referenced foreign key "
                              "columns 'foobars_with_many_columns.bid',"
                              " 'foobars_with_many_columns.bid1', "
                              "'foobars_with_many_columns.bid2', "
                              "'foobars_with_many_columns.fid', "
                              "'foobars_with_many_columns.fid1', "
                              "'foobars_with_many_columns.fid2' for "
                              "join condition 'foos.id = "
                              "foobars_with_many_columns.fid' on "
                              "relationship Foo.bars",
                              sa.orm.configure_mappers)

    @testing.emits_warning(r'No ForeignKey objects.*')
    def test_no_fks_warning_2(self):
        foobars_with_many_columns, bars, Bar, foobars, Foo, foos = (self.tables.foobars_with_many_columns,
                                self.tables.bars,
                                self.classes.Bar,
                                self.tables.foobars,
                                self.classes.Foo,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar, secondary=foobars, 
                                primaryjoin=foos.c.id==foobars.c.fid,
                                secondaryjoin=foobars.c.bid==bars.c.id)})
        mapper(Bar, bars)
        sa.orm.configure_mappers()
        eq_(
            Foo.bars.property.synchronize_pairs,
            [(foos.c.id, foobars.c.fid)]
        )
        eq_(
            Foo.bars.property.secondary_synchronize_pairs,
            [(bars.c.id, foobars.c.bid)]
        )

        sa.orm.clear_mappers()
        mapper(Foo, foos, properties={
                        'bars': relationship(Bar, 
                                secondary=foobars_with_many_columns, 
                              primaryjoin=foos.c.id==
                                        foobars_with_many_columns.c.fid,
                              secondaryjoin=foobars_with_many_columns.c.bid==
                                        bars.c.id)})
        mapper(Bar, bars)
        sa.orm.configure_mappers()
        eq_(
            Foo.bars.property.synchronize_pairs,
            [(foos.c.id, foobars_with_many_columns.c.fid)]
        )
        eq_(
            Foo.bars.property.secondary_synchronize_pairs,
            [(bars.c.id, foobars_with_many_columns.c.bid)]
        )


    def test_bad_primaryjoin(self):
        foobars_with_fks, bars, Bar, foobars, Foo, foos = (self.tables.foobars_with_fks,
                                self.tables.bars,
                                self.classes.Bar,
                                self.tables.foobars,
                                self.classes.Foo,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                             secondary=foobars,
                             primaryjoin=foos.c.id > foobars.c.fid,
                             secondaryjoin=foobars.c.bid<=bars.c.id)})
        mapper(Bar, bars)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not determine relationship direction for "
            "primaryjoin condition",
            configure_mappers)

        sa.orm.clear_mappers()
        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                             secondary=foobars_with_fks,
                             primaryjoin=foos.c.id > foobars_with_fks.c.fid,
                             secondaryjoin=foobars_with_fks.c.bid<=bars.c.id)})
        mapper(Bar, bars)
        assert_raises_message(
            sa.exc.ArgumentError,
            r"Could not locate any foreign-key-equated, locally mapped "
             "column pairs for primaryjoin condition 'foos.id > "
             "foobars_with_fks.fid' on relationship Foo.bars.  Ensure "
             "that the referencing Column objects have a ForeignKey "
             "present, or are otherwise part of a ForeignKeyConstraint "
             "on their parent Table, or specify the foreign_keys "
             "parameter to this relationship.  For more relaxed "
             "rules on join conditions, the relationship may be marked "
             "as viewonly=True.", 
            configure_mappers)

        sa.orm.clear_mappers()
        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                             secondary=foobars_with_fks,
                             primaryjoin=foos.c.id > foobars_with_fks.c.fid,
                             secondaryjoin=foobars_with_fks.c.bid<=bars.c.id,
                             viewonly=True)})
        mapper(Bar, bars)
        sa.orm.configure_mappers()

    def test_bad_secondaryjoin(self):
        foobars, bars, Foo, Bar, foos = (self.tables.foobars,
                                self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars':relationship(Bar,
                            secondary=foobars,
                            primaryjoin=foos.c.id == foobars.c.fid,
                            secondaryjoin=foobars.c.bid <= bars.c.id,
                            foreign_keys=[foobars.c.fid])})
        mapper(Bar, bars)

        assert_raises_message(sa.exc.ArgumentError,
                              "Could not determine relationship "
                              "direction for secondaryjoin condition "
                              r"'foobars.bid \<\= bars.id', on "
                              "relationship Foo.bars, using manual "
                              "'foreign_keys' setting.  Do the columns "
                              "in 'foreign_keys' represent all, and only, the "
                              "'foreign' columns in this join "
                              r"condition\?  Does the "
                              "secondary Table already have adequate "
                              "ForeignKey and/or ForeignKeyConstraint "
                              r"objects established \(in which case "
                              r"'foreign_keys' is usually unnecessary\)?"
                              , sa.orm.configure_mappers)

    def test_no_equated_secondaryjoin(self):
        foobars, bars, Foo, Bar, foos = (self.tables.foobars,
                                self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars':relationship(Bar,
                            secondary=foobars,
                            primaryjoin=foos.c.id == foobars.c.fid,
                            secondaryjoin=foobars.c.bid <= bars.c.id,
                            foreign_keys=[foobars.c.fid, foobars.c.bid])})
        mapper(Bar, bars)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not locate any foreign-key-equated, locally mapped column pairs for "
            "secondaryjoin condition", sa.orm.configure_mappers)

class ActiveHistoryFlagTest(_fixtures.FixtureTest):
    run_inserts = None
    run_deletes = None

    def _test_attribute(self, obj, attrname, newvalue):
        sess = Session()
        sess.add(obj)
        oldvalue = getattr(obj, attrname)
        sess.commit()

        # expired
        assert attrname not in obj.__dict__

        setattr(obj, attrname, newvalue)
        eq_(
            attributes.get_history(obj, attrname),
            ([newvalue,], (), [oldvalue,])
        )

    def test_column_property_flag(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users, properties={
            'name':column_property(users.c.name, 
                                active_history=True)
        })
        u1 = User(name='jack')
        self._test_attribute(u1, 'name', 'ed')

    def test_relationship_property_flag(self):
        Address, addresses, users, User = (self.classes.Address,
                                self.tables.addresses,
                                self.tables.users,
                                self.classes.User)

        mapper(Address, addresses, properties={
            'user':relationship(User, active_history=True)
        })
        mapper(User, users)
        u1 = User(name='jack')
        u2 = User(name='ed')
        a1 = Address(email_address='a1', user=u1)
        self._test_attribute(a1, 'user', u2)

    def test_composite_property_flag(self):
        Order, orders = self.classes.Order, self.tables.orders

        class MyComposite(object):
            def __init__(self, description, isopen):
                self.description = description
                self.isopen = isopen
            def __composite_values__(self):
                return [self.description, self.isopen]
            def __eq__(self, other):
                return isinstance(other, MyComposite) and \
                    other.description == self.description
        mapper(Order, orders, properties={
            'composite':composite(
                                MyComposite, 
                                orders.c.description, 
                                orders.c.isopen,
                                active_history=True)
        })
        o1 = Order(composite=MyComposite('foo', 1))
        self._test_attribute(o1, "composite", MyComposite('bar', 1))



class RelationDeprecationTest(fixtures.MappedTest):
    """test usage of the old 'relation' function."""

    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table('users_table', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(64)))

        Table('addresses_table', metadata,
              Column('id', Integer, primary_key=True),
              Column('user_id', Integer, ForeignKey('users_table.id')),
              Column('email_address', String(128)),
              Column('purpose', String(16)),
              Column('bounces', Integer, default=0))

    @classmethod
    def setup_classes(cls):
        class User(cls.Basic):
            pass

        class Address(cls.Basic):
            pass

    @classmethod
    def fixtures(cls):
        return dict(
            users_table=(
            ('id', 'name'),
            (1, 'jack'),
            (2, 'ed'),
            (3, 'fred'),
            (4, 'chuck')),

            addresses_table=(
            ('id', 'user_id', 'email_address', 'purpose', 'bounces'),
            (1, 1, 'jack@jack.home', 'Personal', 0),
            (2, 1, 'jack@jack.bizz', 'Work', 1),
            (3, 2, 'ed@foo.bar', 'Personal', 0),
            (4, 3, 'fred@the.fred', 'Personal', 10)))

    def test_relation(self):
        addresses_table, User, users_table, Address = (self.tables.addresses_table,
                                self.classes.User,
                                self.tables.users_table,
                                self.classes.Address)

        mapper(User, users_table, properties=dict(
            addresses=relation(Address, backref='user'),
            ))
        mapper(Address, addresses_table)

        session = create_session()

        ed = session.query(User).filter(User.addresses.any(
            Address.email_address == 'ed@foo.bar')).one()



