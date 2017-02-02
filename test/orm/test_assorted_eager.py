"""Exercises for eager loading.

Derived from mailing list-reported problems and trac tickets.

These are generally very old 0.1-era tests and at some point should
be cleaned up and modernized.

"""
import datetime

import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy import Integer, String, ForeignKey, table, text
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, backref, create_session
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


class EagerTest(fixtures.MappedTest):
    run_deletes = None
    run_inserts = "once"
    run_setup_mappers = "once"

    @classmethod
    def define_tables(cls, metadata):

        if testing.db.dialect.supports_native_boolean:
            false = 'false'
        else:
            false = "0"

        cls.other['false'] = false

        Table('owners', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(30)))

        Table('categories', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(20)))

        Table('tests', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('owner_id', Integer, ForeignKey('owners.id'),
                     nullable=False),
              Column('category_id', Integer, ForeignKey('categories.id'),
                     nullable=False))

        Table('options', metadata,
              Column('test_id', Integer, ForeignKey('tests.id'),
                     primary_key=True),
              Column('owner_id', Integer, ForeignKey('owners.id'),
                     primary_key=True),
              Column('someoption', sa.Boolean, server_default=false,
                     nullable=False))

    @classmethod
    def setup_classes(cls):
        class Owner(cls.Basic):
            pass

        class Category(cls.Basic):
            pass

        class Thing(cls.Basic):
            pass

        class Option(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Category, owners, Option, tests, Thing, Owner, options, categories = (
            cls.classes.Category,
            cls.tables.owners,
            cls.classes.Option,
            cls.tables.tests,
            cls.classes.Thing,
            cls.classes.Owner,
            cls.tables.options,
            cls.tables.categories)

        mapper(Owner, owners)

        mapper(Category, categories)

        mapper(Option, options, properties=dict(
            owner=relationship(Owner, viewonly=True),
            test=relationship(Thing, viewonly=True)))

        mapper(Thing, tests, properties=dict(
            owner=relationship(Owner, backref='tests'),
            category=relationship(Category),
            owner_option=relationship(
                Option, primaryjoin=sa.and_(
                    tests.c.id == options.c.test_id,
                    tests.c.owner_id == options.c.owner_id),
                foreign_keys=[options.c.test_id, options.c.owner_id],
                uselist=False)))

    @classmethod
    def insert_data(cls):
        Owner, Category, Option, Thing = (cls.classes.Owner,
                                          cls.classes.Category,
                                          cls.classes.Option,
                                          cls.classes.Thing)

        session = create_session()

        o = Owner()
        c = Category(name='Some Category')
        session.add_all((
            Thing(owner=o, category=c),
            Thing(owner=o, category=c, owner_option=Option(someoption=True)),
            Thing(owner=o, category=c, owner_option=Option())))

        session.flush()

    def test_noorm(self):
        """test the control case"""

        tests, options, categories = (self.tables.tests,
                                      self.tables.options,
                                      self.tables.categories)

        # I want to display a list of tests owned by owner 1
        # if someoption is false or they haven't specified it yet (null)
        # but not if they set it to true (example someoption is for hiding)

        # desired output for owner 1
        # test_id, cat_name
        # 1 'Some Category'
        # 3  "

        # not orm style correct query
        print("Obtaining correct results without orm")
        result = sa.select(
            [tests.c.id, categories.c.name],
            sa.and_(tests.c.owner_id == 1,
                    sa.or_(options.c.someoption == None,  # noqa
                           options.c.someoption == False)),
            order_by=[tests.c.id],
            from_obj=[tests.join(categories).outerjoin(options, sa.and_(
                tests.c.id == options.c.test_id,
                tests.c.owner_id == options.c.owner_id))]
        ).execute().fetchall()
        eq_(result, [(1, 'Some Category'), (3, 'Some Category')])

    def test_withoutjoinedload(self):
        Thing, tests, options = (self.classes.Thing,
                                 self.tables.tests,
                                 self.tables.options)

        s = create_session()
        result = (s.query(Thing)
                  .select_from(tests.outerjoin(
                      options,
                      sa.and_(tests.c.id == options.c.test_id,
                              tests.c.owner_id == options.c.owner_id)))
                  .filter(sa.and_(
                      tests.c.owner_id == 1,
                      sa.or_(options.c.someoption == None,  # noqa
                             options.c.someoption == False))))

        result_str = ["%d %s" % (t.id, t.category.name) for t in result]
        eq_(result_str, ['1 Some Category', '3 Some Category'])

    def test_withjoinedload(self):
        """
        Test that an joinedload locates the correct "from" clause with which to
        attach to, when presented with a query that already has a complicated
        from clause.

        """

        Thing, tests, options = (self.classes.Thing,
                                 self.tables.tests,
                                 self.tables.options)

        s = create_session()
        q = s.query(Thing).options(sa.orm.joinedload('category'))

        result = (q.select_from(tests.outerjoin(options,
                                                sa.and_(tests.c.id ==
                                                        options.c.test_id,
                                                        tests.c.owner_id ==
                                                        options.c.owner_id))).
                  filter(sa.and_(tests.c.owner_id == 1,
                                 sa.or_(options.c.someoption == None,  # noqa
                                        options.c.someoption == False))))

        result_str = ["%d %s" % (t.id, t.category.name) for t in result]
        eq_(result_str, ['1 Some Category', '3 Some Category'])

    def test_dslish(self):
        """test the same as withjoinedload except using generative"""

        Thing, tests, options = (self.classes.Thing,
                                 self.tables.tests,
                                 self.tables.options)

        s = create_session()
        q = s.query(Thing).options(sa.orm.joinedload('category'))
        result = q.filter(
            sa.and_(tests.c.owner_id == 1,
                    sa.or_(options.c.someoption == None,  # noqa
                           options.c.someoption == False))
        ).outerjoin('owner_option')

        result_str = ["%d %s" % (t.id, t.category.name) for t in result]
        eq_(result_str, ['1 Some Category', '3 Some Category'])

    @testing.crashes('sybase', 'FIXME: unknown, verify not fails_on')
    def test_without_outerjoin_literal(self):
        Thing, tests, false = (self.classes.Thing,
                               self.tables.tests,
                               self.other.false)

        s = create_session()
        q = s.query(Thing).options(sa.orm.joinedload('category'))
        result = (q.filter(
                (tests.c.owner_id == 1) &
                text(
                    'options.someoption is null or options.someoption=%s' %
                    false)).join('owner_option'))

        result_str = ["%d %s" % (t.id, t.category.name) for t in result]
        eq_(result_str, ['3 Some Category'])

    def test_withoutouterjoin(self):
        Thing, tests, options = (self.classes.Thing,
                                 self.tables.tests,
                                 self.tables.options)

        s = create_session()
        q = s.query(Thing).options(sa.orm.joinedload('category'))
        result = q.filter(
            (tests.c.owner_id == 1) &
            ((options.c.someoption == None) | (options.c.someoption == False))  # noqa
        ).join('owner_option')

        result_str = ["%d %s" % (t.id, t.category.name) for t in result]
        eq_(result_str, ['3 Some Category'])


class EagerTest2(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('left', metadata,
              Column('id', Integer, ForeignKey('middle.id'), primary_key=True),
              Column('data', String(50), primary_key=True))

        Table('middle', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(50)))

        Table('right', metadata,
              Column('id', Integer, ForeignKey('middle.id'), primary_key=True),
              Column('data', String(50), primary_key=True))

    @classmethod
    def setup_classes(cls):
        class Left(cls.Basic):
            def __init__(self, data):
                self.data = data

        class Middle(cls.Basic):
            def __init__(self, data):
                self.data = data

        class Right(cls.Basic):
            def __init__(self, data):
                self.data = data

    @classmethod
    def setup_mappers(cls):
        Right, Middle, middle, right, left, Left = (cls.classes.Right,
                                                    cls.classes.Middle,
                                                    cls.tables.middle,
                                                    cls.tables.right,
                                                    cls.tables.left,
                                                    cls.classes.Left)

        # set up bi-directional eager loads
        mapper(Left, left)
        mapper(Right, right)
        mapper(Middle, middle, properties=dict(
            left=relationship(Left,
                              lazy='joined',
                              backref=backref('middle', lazy='joined')),
            right=relationship(Right,
                               lazy='joined',
                               backref=backref('middle', lazy='joined')))),

    def test_eager_terminate(self):
        """Eager query generation does not include the same mapper's table twice.

        Or, that bi-directional eager loads don't include each other in eager
        query generation.

        """

        Middle, Right, Left = (self.classes.Middle,
                               self.classes.Right,
                               self.classes.Left)

        p = Middle('m1')
        p.left.append(Left('l1'))
        p.right.append(Right('r1'))

        session = create_session()
        session.add(p)
        session.flush()
        session.expunge_all()
        obj = session.query(Left).filter_by(data='l1').one()


class EagerTest3(fixtures.MappedTest):
    """Eager loading combined with nested SELECT statements, functions, and
    aggregates."""

    @classmethod
    def define_tables(cls, metadata):
        Table('datas', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('a', Integer, nullable=False))

        Table('foo', metadata,
              Column('data_id', Integer, ForeignKey('datas.id'),
                     primary_key=True),
              Column('bar', Integer))

        Table('stats', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data_id', Integer, ForeignKey('datas.id')),
              Column('somedata', Integer, nullable=False))

    @classmethod
    def setup_classes(cls):
        class Data(cls.Basic):
            pass

        class Foo(cls.Basic):
            pass

        class Stat(cls.Basic):
            pass

    def test_nesting_with_functions(self):
        Stat, Foo, stats, foo, Data, datas = (self.classes.Stat,
                                              self.classes.Foo,
                                              self.tables.stats,
                                              self.tables.foo,
                                              self.classes.Data,
                                              self.tables.datas)

        mapper(Data, datas)
        mapper(Foo, foo, properties={
            'data': relationship(Data, backref=backref('foo', uselist=False))})

        mapper(Stat, stats, properties={
            'data': relationship(Data)})

        session = create_session()

        data = [Data(a=x) for x in range(5)]
        session.add_all(data)

        session.add_all((
            Stat(data=data[0], somedata=1),
            Stat(data=data[1], somedata=2),
            Stat(data=data[2], somedata=3),
            Stat(data=data[3], somedata=4),
            Stat(data=data[4], somedata=5),
            Stat(data=data[0], somedata=6),
            Stat(data=data[1], somedata=7),
            Stat(data=data[2], somedata=8),
            Stat(data=data[3], somedata=9),
            Stat(data=data[4], somedata=10)))
        session.flush()

        arb_data = sa.select(
            [stats.c.data_id, sa.func.max(stats.c.somedata).label('max')],
            stats.c.data_id <= 5,
            group_by=[stats.c.data_id])

        arb_result = arb_data.execute().fetchall()

        # order the result list descending based on 'max'
        arb_result.sort(key=lambda a: a['max'], reverse=True)

        # extract just the "data_id" from it
        arb_result = [row['data_id'] for row in arb_result]

        arb_data = arb_data.alias('arb')

        # now query for Data objects using that above select, adding the
        # "order by max desc" separately
        q = (session.query(Data).
             options(sa.orm.joinedload('foo')).
             select_from(datas.join(arb_data,
                                    arb_data.c.data_id == datas.c.id)).
             order_by(sa.desc(arb_data.c.max)).
             limit(10))

        # extract "data_id" from the list of result objects
        verify_result = [d.id for d in q]

        eq_(verify_result, arb_result)


class EagerTest4(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('departments', metadata,
              Column('department_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(50)))

        Table('employees', metadata,
              Column('person_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(50)),
              Column('department_id', Integer,
                     ForeignKey('departments.department_id')))

    @classmethod
    def setup_classes(cls):
        class Department(cls.Basic):
            pass

        class Employee(cls.Basic):
            pass

    def test_basic(self):
        Department, Employee, employees, departments = (
            self.classes.Department, self.classes.Employee,
            self.tables.employees, self.tables.departments)

        mapper(Employee, employees)
        mapper(Department, departments, properties=dict(
            employees=relationship(Employee,
                                   lazy='joined',
                                   backref='department')))

        d1 = Department(name='One')
        for e in 'Jim', 'Jack', 'John', 'Susan':
            d1.employees.append(Employee(name=e))

        d2 = Department(name='Two')
        for e in 'Joe', 'Bob', 'Mary', 'Wally':
            d2.employees.append(Employee(name=e))

        sess = create_session()
        sess.add_all((d1, d2))
        sess.flush()

        q = (sess.query(Department).
             join('employees').
             filter(Employee.name.startswith('J')).
             distinct().
             order_by(sa.desc(Department.name)))

        eq_(q.count(), 2)
        assert q[0] is d2


class EagerTest5(fixtures.MappedTest):
    """Construction of AliasedClauses for the same eager load property but
    different parent mappers, due to inheritance."""

    @classmethod
    def define_tables(cls, metadata):
        Table('base', metadata,
              Column('uid', String(30), primary_key=True),
              Column('x', String(30)))

        Table('derived', metadata,
              Column('uid', String(30),
                     ForeignKey('base.uid'),
                     primary_key=True),
              Column('y', String(30)))

        Table('derivedII', metadata,
              Column('uid', String(30),
                     ForeignKey('base.uid'),
                     primary_key=True),
              Column('z', String(30)))

        Table('comments', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('uid', String(30),
                     ForeignKey('base.uid')),
              Column('comment', String(30)))

    @classmethod
    def setup_classes(cls):
        class Base(cls.Basic):
            def __init__(self, uid, x):
                self.uid = uid
                self.x = x

        class Derived(Base):
            def __init__(self, uid, x, y):
                self.uid = uid
                self.x = x
                self.y = y

        class DerivedII(Base):
            def __init__(self, uid, x, z):
                self.uid = uid
                self.x = x
                self.z = z

        class Comment(cls.Basic):
            def __init__(self, uid, comment):
                self.uid = uid
                self.comment = comment

    def test_basic(self):
        Comment, Derived, derived, comments, \
            DerivedII, Base, base, derivedII = (self.classes.Comment,
                                                self.classes.Derived,
                                                self.tables.derived,
                                                self.tables.comments,
                                                self.classes.DerivedII,
                                                self.classes.Base,
                                                self.tables.base,
                                                self.tables.derivedII)

        commentMapper = mapper(Comment, comments)

        baseMapper = mapper(Base, base, properties=dict(
            comments=relationship(Comment, lazy='joined',
                                  cascade='all, delete-orphan')))

        mapper(Derived, derived, inherits=baseMapper)

        mapper(DerivedII, derivedII, inherits=baseMapper)

        sess = create_session()
        d = Derived('uid1', 'x', 'y')
        d.comments = [Comment('uid1', 'comment')]
        d2 = DerivedII('uid2', 'xx', 'z')
        d2.comments = [Comment('uid2', 'comment')]
        sess.add_all((d, d2))
        sess.flush()
        sess.expunge_all()

        # this eager load sets up an AliasedClauses for the "comment"
        # relationship, then stores it in clauses_by_lead_mapper[mapper for
        # Derived]
        d = sess.query(Derived).get('uid1')
        sess.expunge_all()
        assert len([c for c in d.comments]) == 1

        # this eager load sets up an AliasedClauses for the "comment"
        # relationship, and should store it in clauses_by_lead_mapper[mapper
        # for DerivedII].  the bug was that the previous AliasedClause create
        # prevented this population from occurring.
        d2 = sess.query(DerivedII).get('uid2')
        sess.expunge_all()

        # object is not in the session; therefore the lazy load cant trigger
        # here, eager load had to succeed
        assert len([c for c in d2.comments]) == 1


class EagerTest6(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('design_types', metadata,
              Column('design_type_id', Integer, primary_key=True,
                     test_needs_autoincrement=True))

        Table('design', metadata,
              Column('design_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('design_type_id', Integer,
                     ForeignKey('design_types.design_type_id')))

        Table('parts', metadata,
              Column('part_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('design_id', Integer, ForeignKey('design.design_id')),
              Column('design_type_id', Integer,
                     ForeignKey('design_types.design_type_id')))

        Table('inherited_part', metadata,
              Column('ip_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('part_id', Integer, ForeignKey('parts.part_id')),
              Column('design_id', Integer, ForeignKey('design.design_id')))

    @classmethod
    def setup_classes(cls):
        class Part(cls.Basic):
            pass

        class Design(cls.Basic):
            pass

        class DesignType(cls.Basic):
            pass

        class InheritedPart(cls.Basic):
            pass

    def test_one(self):
        Part, inherited_part, design_types, DesignType, \
            parts, design, Design, InheritedPart = (self.classes.Part,
                                                    self.tables.inherited_part,
                                                    self.tables.design_types,
                                                    self.classes.DesignType,
                                                    self.tables.parts,
                                                    self.tables.design,
                                                    self.classes.Design,
                                                    self.classes.InheritedPart)

        p_m = mapper(Part, parts)

        mapper(InheritedPart, inherited_part, properties=dict(
            part=relationship(Part, lazy='joined')))

        d_m = mapper(Design, design, properties=dict(
            inheritedParts=relationship(InheritedPart,
                                        cascade="all, delete-orphan",
                                        backref="design")))

        mapper(DesignType, design_types)

        d_m.add_property(
            "type", relationship(DesignType, lazy='joined', backref="designs"))

        p_m.add_property(
            "design", relationship(
                Design, lazy='joined',
                backref=backref("parts", cascade="all, delete-orphan")))

        d = Design()
        sess = create_session()
        sess.add(d)
        sess.flush()
        sess.expunge_all()
        x = sess.query(Design).get(1)
        x.inheritedParts


class EagerTest7(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('companies', metadata,
              Column('company_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('company_name', String(40)))

        Table('addresses', metadata,
              Column('address_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('company_id', Integer,
                     ForeignKey("companies.company_id")),
              Column('address', String(40)))

        Table('phone_numbers', metadata,
              Column('phone_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('address_id', Integer,
                     ForeignKey('addresses.address_id')),
              Column('type', String(20)),
              Column('number', String(10)))

        Table('invoices', metadata,
              Column('invoice_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('company_id', Integer,
                     ForeignKey("companies.company_id")),
              Column('date', sa.DateTime))

    @classmethod
    def setup_classes(cls):
        class Company(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

        class Phone(cls.Comparable):
            pass

        class Invoice(cls.Comparable):
            pass

    def test_load_m2o_attached_to_o2(self):
        """
        Tests eager load of a many-to-one attached to a one-to-many.  this
        testcase illustrated the bug, which is that when the single Company is
        loaded, no further processing of the rows occurred in order to load
        the Company's second Address object.

        """

        addresses, invoices, Company, companies, Invoice, Address = (
            self.tables.addresses, self.tables.invoices, self.classes.Company,
            self.tables.companies, self.classes.Invoice, self.classes.Address)

        mapper(Address, addresses)

        mapper(Company, companies, properties={
            'addresses': relationship(Address, lazy='joined')})

        mapper(Invoice, invoices, properties={
            'company': relationship(Company, lazy='joined')})

        a1 = Address(address='a1 address')
        a2 = Address(address='a2 address')
        c1 = Company(company_name='company 1', addresses=[a1, a2])
        i1 = Invoice(date=datetime.datetime.now(), company=c1)

        session = create_session()
        session.add(i1)
        session.flush()

        company_id = c1.company_id
        invoice_id = i1.invoice_id

        session.expunge_all()
        c = session.query(Company).get(company_id)

        session.expunge_all()
        i = session.query(Invoice).get(invoice_id)

        def go():
            eq_(c, i.company)
            eq_(c.addresses, i.company.addresses)
        self.assert_sql_count(testing.db, go, 0)


class EagerTest8(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('prj', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('created', sa.DateTime),
              Column('title', sa.String(100)))

        Table('task', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('status_id', Integer, ForeignKey('task_status.id'),
                     nullable=False),
              Column('title', sa.String(100)),
              Column('task_type_id', Integer, ForeignKey('task_type.id'),
                     nullable=False),
              Column('prj_id', Integer, ForeignKey('prj.id'),
                     nullable=False))

        Table('task_status', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True))

        Table('task_type', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True))

        Table('msg', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('posted', sa.DateTime, index=True,),
              Column('type_id', Integer, ForeignKey('msg_type.id')),
              Column('task_id', Integer, ForeignKey('task.id')))

        Table('msg_type', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', sa.String(20)),
              Column('display_name', sa.String(20)))

    @classmethod
    def fixtures(cls):
        return dict(
            prj=(('id',),
                 (1,)),

            task_status=(('id',),
                         (1,)),

            task_type=(('id',),
                       (1,),),

            task=(('title', 'task_type_id', 'status_id', 'prj_id'),
                  ('task 1', 1, 1, 1)))

    @classmethod
    def setup_classes(cls):
        class Task_Type(cls.Comparable):
            pass

        class Joined(cls.Comparable):
            pass

    def test_nested_joins(self):
        task, Task_Type, Joined, prj, task_type, msg = (self.tables.task,
                                                        self.classes.Task_Type,
                                                        self.classes.Joined,
                                                        self.tables.prj,
                                                        self.tables.task_type,
                                                        self.tables.msg)

        # this is testing some subtle column resolution stuff,
        # concerning corresponding_column() being extremely accurate
        # as well as how mapper sets up its column properties

        mapper(Task_Type, task_type)

        tsk_cnt_join = sa.outerjoin(prj, task, task.c.prj_id == prj.c.id)

        j = sa.outerjoin(task, msg, task.c.id == msg.c.task_id)
        jj = sa.select([task.c.id.label('task_id'),
                        sa.func.count(msg.c.id).label('props_cnt')],
                       from_obj=[j],
                       group_by=[task.c.id]).alias('prop_c_s')
        jjj = sa.join(task, jj, task.c.id == jj.c.task_id)

        mapper(Joined, jjj, properties=dict(
            type=relationship(Task_Type, lazy='joined')))

        session = create_session()

        eq_(session.query(Joined).limit(10).offset(0).one(),
            Joined(id=1, title='task 1', props_cnt=0))


class EagerTest9(fixtures.MappedTest):
    """Test the usage of query options to eagerly load specific paths.

    This relies upon the 'path' construct used by PropertyOption to relate
    LoaderStrategies to specific paths, as well as the path state maintained
    throughout the query setup/mapper instances process.

    """
    @classmethod
    def define_tables(cls, metadata):
        Table('accounts', metadata,
              Column('account_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(40)))

        Table('transactions', metadata,
              Column('transaction_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(40)))

        Table('entries', metadata,
              Column('entry_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(40)),
              Column('account_id', Integer, ForeignKey('accounts.account_id')),
              Column('transaction_id', Integer,
                     ForeignKey('transactions.transaction_id')))

    @classmethod
    def setup_classes(cls):
        class Account(cls.Basic):
            pass

        class Transaction(cls.Basic):
            pass

        class Entry(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Account, Transaction, transactions, accounts, entries, Entry = (
            cls.classes.Account, cls.classes.Transaction, cls.tables.
            transactions, cls.tables.accounts, cls.tables.entries, cls.classes.
            Entry)

        mapper(Account, accounts)

        mapper(Transaction, transactions)

        mapper(
            Entry, entries,
            properties=dict(
                account=relationship(
                    Account, uselist=False,
                    backref=backref(
                        'entries', lazy='select',
                        order_by=entries.c.entry_id)),
                transaction=relationship(
                    Transaction, uselist=False,
                    backref=backref(
                        'entries', lazy='joined',
                        order_by=entries.c.entry_id))))

    def test_joinedload_on_path(self):
        Entry, Account, Transaction = (self.classes.Entry,
                                       self.classes.Account,
                                       self.classes.Transaction)

        session = create_session()

        tx1 = Transaction(name='tx1')
        tx2 = Transaction(name='tx2')

        acc1 = Account(name='acc1')
        ent11 = Entry(name='ent11', account=acc1, transaction=tx1)
        ent12 = Entry(name='ent12', account=acc1, transaction=tx2)

        acc2 = Account(name='acc2')
        ent21 = Entry(name='ent21', account=acc2, transaction=tx1)
        ent22 = Entry(name='ent22', account=acc2, transaction=tx2)

        session.add(acc1)
        session.flush()
        session.expunge_all()

        def go():
            # load just the first Account.  eager loading will actually load
            # all objects saved thus far, but will not eagerly load the
            # "accounts" off the immediate "entries"; only the "accounts" off
            # the entries->transaction->entries
            acc = (session.query(Account).options(
                sa.orm.joinedload_all(
                    'entries.transaction.entries.account')).order_by(
                        Account.account_id)).first()

            # no sql occurs
            eq_(acc.name, 'acc1')
            eq_(acc.entries[0].transaction.entries[0].account.name, 'acc1')
            eq_(acc.entries[0].transaction.entries[1].account.name, 'acc2')

            # lazyload triggers but no sql occurs because many-to-one uses
            # cached query.get()
            for e in acc.entries:
                assert e.account is acc

        self.assert_sql_count(testing.db, go, 1)
