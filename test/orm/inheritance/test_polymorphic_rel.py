from sqlalchemy import Integer, String, ForeignKey, func, desc, and_
from sqlalchemy.orm import interfaces, relationship, mapper, \
    clear_mappers, create_session, joinedload, joinedload_all, \
    subqueryload, subqueryload_all, polymorphic_union, aliased,\
    class_mapper
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine import default

from test.lib import AssertsCompiledSQL, fixtures, testing
from test.lib.schema import Table, Column
from test.lib.testing import assert_raises, eq_

class Company(fixtures.ComparableEntity):
    pass
class Person(fixtures.ComparableEntity):
    pass
class Engineer(Person):
    pass
class Manager(Person):
    pass
class Boss(Manager):
    pass
class Machine(fixtures.ComparableEntity):
    pass
class Paperwork(fixtures.ComparableEntity):
    pass

def _produce_test(select_type):

    class PolymorphicQueryTest(fixtures.MappedTest, AssertsCompiledSQL):

        run_inserts = 'once'
        run_setup_mappers = 'once'
        run_deletes = None

        @classmethod
        def define_tables(cls, metadata):
            global people, engineers, managers, boss
            global companies, paperwork, machines

            companies = Table('companies', metadata,
                Column('company_id', Integer,
                    primary_key=True,
                    test_needs_autoincrement=True),
                Column('name', String(50)))

            people = Table('people', metadata,
                Column('person_id', Integer,
                    primary_key=True,
                    test_needs_autoincrement=True),
                Column('company_id', Integer,
                    ForeignKey('companies.company_id')),
                Column('name', String(50)),
                Column('type', String(30)))

            engineers = Table('engineers', metadata,
                Column('person_id', Integer,
                    ForeignKey('people.person_id'),
                    primary_key=True),
                Column('status', String(30)),
                Column('engineer_name', String(50)),
                Column('primary_language', String(50)))

            machines = Table('machines', metadata,
                 Column('machine_id',
                     Integer, primary_key=True,
                     test_needs_autoincrement=True),
                 Column('name', String(50)),
                 Column('engineer_id', Integer,
                ForeignKey('engineers.person_id')))

            managers = Table('managers', metadata,
                Column('person_id', Integer,
                    ForeignKey('people.person_id'),
                    primary_key=True),
                Column('status', String(30)),
                Column('manager_name', String(50)))

            boss = Table('boss', metadata,
                Column('boss_id', Integer,
                    ForeignKey('managers.person_id'),
                    primary_key=True),
                Column('golf_swing', String(30)))

            paperwork = Table('paperwork', metadata,
                Column('paperwork_id', Integer,
                    primary_key=True,
                    test_needs_autoincrement=True),
                Column('description', String(50)),
                Column('person_id', Integer,
                    ForeignKey('people.person_id')))

            clear_mappers()

            mapper(Company, companies,
                properties={
                    'employees':relationship(
                        Person,
                        order_by=people.c.person_id)})

            mapper(Machine, machines)

            if select_type == '':
                person_join = manager_join = None
                person_with_polymorphic = None
                manager_with_polymorphic = None
            elif select_type == 'Polymorphic':
                person_join = manager_join = None
                person_with_polymorphic = '*'
                manager_with_polymorphic = '*'
            elif select_type == 'Unions':
                person_join = polymorphic_union({
                        'engineer':people.join(engineers),
                        'manager':people.join(managers)},
                    None, 'pjoin')
                manager_join = people.join(managers).outerjoin(boss)
                person_with_polymorphic = (
                    [Person, Manager, Engineer], person_join)
                manager_with_polymorphic = ('*', manager_join)
            elif select_type == 'AliasedJoins':
                person_join = people \
                    .outerjoin(engineers) \
                    .outerjoin(managers) \
                    .select(use_labels=True) \
                    .alias('pjoin')
                manager_join = people \
                    .join(managers) \
                    .outerjoin(boss) \
                    .select(use_labels=True) \
                    .alias('mjoin')
                person_with_polymorphic = (
                    [Person, Manager, Engineer], person_join)
                manager_with_polymorphic = ('*', manager_join)
            elif select_type == 'Joins':
                person_join = people.outerjoin(engineers).outerjoin(managers)
                manager_join = people.join(managers).outerjoin(boss)
                person_with_polymorphic = (
                    [Person, Manager, Engineer], person_join)
                manager_with_polymorphic = ('*', manager_join)

            # testing a order_by here as well; 
            # the surrogate mapper has to adapt it
            mapper(Person, people,
                with_polymorphic=person_with_polymorphic,
                polymorphic_on=people.c.type,
                polymorphic_identity='person',
                order_by=people.c.person_id,
                properties={
                    'paperwork':relationship(
                        Paperwork,
                        order_by=paperwork.c.paperwork_id)})

            mapper(Engineer, engineers,
                inherits=Person,
                polymorphic_identity='engineer',
                properties={
                    'machines':relationship(
                        Machine,
                        order_by=machines.c.machine_id)})

            mapper(Manager, managers,
                with_polymorphic=manager_with_polymorphic,
                inherits=Person,
                polymorphic_identity='manager')

            mapper(Boss, boss,
                inherits=Manager,
                polymorphic_identity='boss')

            mapper(Paperwork, paperwork)

        @classmethod
        def insert_data(cls):
            global all_employees, c1_employees, c2_employees
            global c1, c2, e1, e2, e3, b1, m1

            e1 = Engineer(
                name="dilbert",
                engineer_name="dilbert",
                primary_language="java",
                status="regular engineer",
                paperwork=[
                    Paperwork(description="tps report #1"),
                    Paperwork(description="tps report #2")],
                machines=[
                    Machine(name='IBM ThinkPad'),
                    Machine(name='IPhone')])

            e2 = Engineer(
                name="wally",
                engineer_name="wally",
                primary_language="c++",
                status="regular engineer",
                paperwork=[
                    Paperwork(description="tps report #3"),
                    Paperwork(description="tps report #4")],
                machines=[Machine(name="Commodore 64")])

            b1 = Boss(
                name="pointy haired boss",
                golf_swing="fore",
                manager_name="pointy",
                status="da boss",
                paperwork=[Paperwork(description="review #1")])

            m1 = Manager(
                name="dogbert",
                manager_name="dogbert",
                status="regular manager",
                paperwork=[
                    Paperwork(description="review #2"),
                    Paperwork(description="review #3")])

            e3 = Engineer(
                name="vlad",
                engineer_name="vlad",
                primary_language="cobol",
                status="elbonian engineer",
                paperwork=[
                    Paperwork(description='elbonian missive #3')],
                machines=[
                    Machine(name="Commodore 64"),
                    Machine(name="IBM 3270")])

            c1 = Company(name="MegaCorp, Inc.")
            c1.employees = [e1, e2, b1, m1]
            c2 = Company(name="Elbonia, Inc.")
            c2.employees = [e3]

            sess = create_session()
            sess.add(c1)
            sess.add(c2)
            sess.flush()
            sess.expunge_all()

            all_employees = [e1, e2, b1, m1, e3]
            c1_employees = [e1, e2, b1, m1]
            c2_employees = [e3]

        def test_loads_at_once(self):
            """
            Test that all objects load from the full query, when 
            with_polymorphic is used.
            """

            sess = create_session()
            def go():
                eq_(sess.query(Person).all(), all_employees)
            count = {'':14, 'Polymorphic':9}.get(select_type, 10)
            self.assert_sql_count(testing.db, go, count)

        def test_primary_eager_aliasing_one(self):
            # For both joinedload() and subqueryload(), if the original q is 
            # not loading the subclass table, the joinedload doesn't happen.

            sess = create_session()
            def go():
                eq_(sess.query(Person)
                        .options(joinedload(Engineer.machines))[1:3],
                    all_employees[1:3])
            count = {'':6, 'Polymorphic':3}.get(select_type, 4)
            self.assert_sql_count(testing.db, go, count)

        def test_primary_eager_aliasing_two(self):
            sess = create_session()
            def go():
                eq_(sess.query(Person)
                        .options(subqueryload(Engineer.machines)).all(),
                    all_employees)
            count = {'':14, 'Polymorphic':7}.get(select_type, 8)
            self.assert_sql_count(testing.db, go, count)

        def test_primary_eager_aliasing_three(self):

            # assert the JOINs don't over JOIN

            sess = create_session()
            def go():
                eq_(sess.query(Person).with_polymorphic('*')
                        .options(joinedload(Engineer.machines))[1:3],
                    all_employees[1:3])
            self.assert_sql_count(testing.db, go, 3)

            eq_(sess.query(Person).with_polymorphic('*')
                    .options(joinedload(Engineer.machines))
                    .limit(2).offset(1).with_labels()
                    .subquery().count().scalar(),
                2)

        def test_get_one(self):
            """
            For all mappers, ensure the primary key has been calculated as 
            just the "person_id" column.
            """
            sess = create_session()
            eq_(sess.query(Person).get(e1.person_id),
                Engineer(name="dilbert", primary_language="java"))

        def test_get_two(self):
            sess = create_session()
            eq_(sess.query(Engineer).get(e1.person_id),
                Engineer(name="dilbert", primary_language="java"))

        def test_get_three(self):
            sess = create_session()
            eq_(sess.query(Manager).get(b1.person_id),
                Boss(name="pointy haired boss", golf_swing="fore"))

        def test_multi_join(self):
            sess = create_session()
            e = aliased(Person)
            c = aliased(Company)
            q = sess.query(Company, Person, c, e)\
                    .join(Person, Company.employees)\
                    .join(e, c.employees)\
                    .filter(Person.name == 'dilbert')\
                    .filter(e.name == 'wally')
            eq_(q.count(), 1)
            eq_(q.all(), [
                (
                    Company(company_id=1, name=u'MegaCorp, Inc.'),
                    Engineer(
                        status=u'regular engineer',
                        engineer_name=u'dilbert',
                        name=u'dilbert',
                        company_id=1,
                        primary_language=u'java',
                        person_id=1,
                        type=u'engineer'),
                    Company(company_id=1, name=u'MegaCorp, Inc.'),
                    Engineer(
                        status=u'regular engineer',
                        engineer_name=u'wally',
                        name=u'wally',
                        company_id=1,
                        primary_language=u'c++',
                        person_id=2,
                        type=u'engineer')
                )
            ])

        def test_filter_on_subclass_one(self):
            sess = create_session()
            eq_(sess.query(Engineer).all()[0], Engineer(name="dilbert"))

        def test_filter_on_subclass_two(self):
            sess = create_session()
            eq_(sess.query(Engineer).first(), Engineer(name="dilbert"))

        def test_filter_on_subclass_three(self):
            sess = create_session()
            eq_(sess.query(Engineer)
                    .filter(Engineer.person_id == e1.person_id).first(),
                Engineer(name="dilbert"))

        def test_filter_on_subclass_four(self):
            sess = create_session()
            eq_(sess.query(Manager)
                    .filter(Manager.person_id == m1.person_id).one(),
                Manager(name="dogbert"))

        def test_filter_on_subclass_five(self):
            sess = create_session()
            eq_(sess.query(Manager)
                    .filter(Manager.person_id == b1.person_id).one(),
                Boss(name="pointy haired boss"))

        def test_filter_on_subclass_six(self):
            sess = create_session()
            eq_(sess.query(Boss)
                    .filter(Boss.person_id == b1.person_id).one(),
                Boss(name="pointy haired boss"))

        def test_join_from_polymorphic_nonaliased_one(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .join('paperwork', aliased=False)
                    .filter(Paperwork.description.like('%review%')).all(),
                [b1, m1])

        def test_join_from_polymorphic_nonaliased_two(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .join('paperwork', aliased=False)
                    .filter(Paperwork.description.like('%#2%')).all(),
                [e1, m1])

        def test_join_from_polymorphic_nonaliased_three(self):
            sess = create_session()
            eq_(sess.query(Engineer)
                    .join('paperwork', aliased=False)
                    .filter(Paperwork.description.like('%#2%')).all(),
                [e1])

        def test_join_from_polymorphic_nonaliased_four(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .join('paperwork', aliased=False)
                    .filter(Person.name.like('%dog%'))
                    .filter(Paperwork.description.like('%#2%')).all(),
                [m1])

        def test_join_from_polymorphic_aliased_one(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .join('paperwork', aliased=True)
                    .filter(Paperwork.description.like('%review%')).all(),
                [b1, m1])

        def test_join_from_polymorphic_aliased_two(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .join('paperwork', aliased=True)
                    .filter(Paperwork.description.like('%#2%')).all(),
                [e1, m1])

        def test_join_from_polymorphic_aliased_three(self):
            sess = create_session()
            eq_(sess.query(Engineer)
                    .join('paperwork', aliased=True)
                    .filter(Paperwork.description.like('%#2%')).all(),
                [e1])

        def test_join_from_polymorphic_aliased_four(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .join('paperwork', aliased=True)
                    .filter(Person.name.like('%dog%'))
                    .filter(Paperwork.description.like('%#2%')).all(),
                [m1])

        def test_join_from_with_polymorphic_nonaliased_one(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .with_polymorphic(Manager)
                    .join('paperwork')
                    .filter(Paperwork.description.like('%review%')).all(),
                [b1, m1])

        def test_join_from_with_polymorphic_nonaliased_two(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .with_polymorphic([Manager, Engineer])
                    .join('paperwork')
                    .filter(Paperwork.description.like('%#2%')).all(),
                [e1, m1])

        def test_join_from_with_polymorphic_nonaliased_three(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .with_polymorphic([Manager, Engineer])
                    .join('paperwork')
                    .filter(Person.name.like('%dog%'))
                    .filter(Paperwork.description.like('%#2%')).all(),
                [m1])


        def test_join_from_with_polymorphic_aliased_one(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .with_polymorphic(Manager)
                    .join('paperwork', aliased=True)
                    .filter(Paperwork.description.like('%review%')).all(),
                [b1, m1])

        def test_join_from_with_polymorphic_aliased_two(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .with_polymorphic([Manager, Engineer])
                    .join('paperwork', aliased=True)
                    .filter(Paperwork.description.like('%#2%')).all(),
                [e1, m1])

        def test_join_from_with_polymorphic_aliased_three(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .with_polymorphic([Manager, Engineer])
                    .join('paperwork', aliased=True)
                    .filter(Person.name.like('%dog%'))
                    .filter(Paperwork.description.like('%#2%')).all(),
                [m1])

        def test_join_to_polymorphic_nonaliased(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees')
                    .filter(Person.name == 'vlad').one(),
                c2)

        def test_join_to_polymorphic_aliased(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', aliased=True)
                    .filter(Person.name == 'vlad').one(),
                c2)

        def test_polymorphic_any_one(self):
            sess = create_session()

            any_ = Company.employees.any(Person.name == 'vlad')
            eq_(sess.query(Company).filter(any_).all(), [c2])

        def test_polymorphic_any_two(self):
            sess = create_session()
            # test that the aliasing on "Person" does not bleed into the
            # EXISTS clause generated by any()
            any_ = Company.employees.any(Person.name == 'wally')
            eq_(sess.query(Company)
                    .join(Company.employees, aliased=True)
                    .filter(Person.name == 'dilbert')
                    .filter(any_).all(),
                [c1])

        def test_polymorphic_any_three(self):
            sess = create_session()
            any_ = Company.employees.any(Person.name == 'vlad')
            eq_(sess.query(Company)
                    .join(Company.employees, aliased=True)
                    .filter(Person.name == 'dilbert')
                    .filter(any_).all(),
                [])

        def test_polymorphic_any_four(self):
            sess = create_session()
            any_ = Company.employees.of_type(Engineer).any(
                Engineer.primary_language == 'cobol')
            eq_(sess.query(Company).filter(any_).one(), c2)

        def test_polymorphic_any_five(self):
            sess = create_session()
            calias = aliased(Company)
            any_ = calias.employees.of_type(Engineer).any(
                Engineer.primary_language == 'cobol')
            eq_(sess.query(calias).filter(any_).one(), c2)

        def test_polymorphic_any_six(self):
            sess = create_session()
            any_ = Company.employees.of_type(Boss).any(
                Boss.golf_swing == 'fore')
            eq_(sess.query(Company).filter(any_).one(), c1)

        def test_polymorphic_any_seven(self):
            sess = create_session()
            any_ = Company.employees.of_type(Boss).any(
                Manager.manager_name == 'pointy')
            eq_(sess.query(Company).filter(any_).one(), c1)

        def test_polymorphic_any_eight(self):
            sess = create_session()
            if select_type != '':
                any_ = Engineer.machines.any(
                    Machine.name == "Commodore 64")
                eq_(sess.query(Person).filter(any_).all(), [e2, e3])

        def test_polymorphic_any_nine(self):
            sess = create_session()
            any_ = Person.paperwork.any(
                Paperwork.description == "review #2")
            eq_(sess.query(Person).filter(any_).all(), [m1])

        def test_polymorphic_any_ten(self):
            sess = create_session()
            any_ = Company.employees.of_type(Engineer).any(
                and_(Engineer.primary_language == 'cobol'))
            eq_(sess.query(Company).filter(any_).one(), c2)

        def test_join_from_columns_or_subclass_one(self):
            sess = create_session()

            expected = [
                (u'dogbert',),
                (u'pointy haired boss',)]
            eq_(sess.query(Manager.name)
                    .order_by(Manager.name).all(),
                expected)

        def test_join_from_columns_or_subclass_two(self):
            sess = create_session()
            expected = [
                (u'dogbert',),
                (u'dogbert',),
                (u'pointy haired boss',)]
            eq_(sess.query(Manager.name)
                    .join(Paperwork, Manager.paperwork)
                    .order_by(Manager.name).all(),
                expected)

        def test_join_from_columns_or_subclass_three(self):
            sess = create_session()
            expected = [
                (u'dilbert',),
                (u'dilbert',),
                (u'dogbert',),
                (u'dogbert',),
                (u'pointy haired boss',),
                (u'vlad',),
                (u'wally',),
                (u'wally',)]
            eq_(sess.query(Person.name)
                    .join(Paperwork, Person.paperwork)
                    .order_by(Person.name).all(),
                expected)

        def test_join_from_columns_or_subclass_four(self):
            sess = create_session()
            # Load Person.name, joining from Person -> paperwork, get all
            # the people.
            expected = [
                (u'dilbert',),
                (u'dilbert',),
                (u'dogbert',),
                (u'dogbert',),
                (u'pointy haired boss',),
                (u'vlad',),
                (u'wally',),
                (u'wally',)]
            eq_(sess.query(Person.name)
                    .join(paperwork,
                          Person.person_id == paperwork.c.person_id)
                    .order_by(Person.name).all(),
                expected)

        def test_join_from_columns_or_subclass_five(self):
            sess = create_session()
            # same, on manager.  get only managers.
            expected = [
                (u'dogbert',),
                (u'dogbert',),
                (u'pointy haired boss',)]
            eq_(sess.query(Manager.name)
                    .join(paperwork,
                          Manager.person_id == paperwork.c.person_id)
                    .order_by(Person.name).all(),
                expected)

        def test_join_from_columns_or_subclass_six(self):
            sess = create_session()
            if select_type == '':
                # this now raises, due to [ticket:1892].  Manager.person_id 
                # is now the "person_id" column on Manager. SQL is incorrect.
                assert_raises(
                    sa_exc.DBAPIError,
                    sess.query(Person.name)
                        .join(paperwork,
                              Manager.person_id == paperwork.c.person_id)
                        .order_by(Person.name).all)
            elif select_type == 'Unions':
                # with the union, not something anyone would really be using 
                # here, it joins to the full result set.  This is 0.6's 
                # behavior and is more or less wrong.
                expected = [
                    (u'dilbert',),
                    (u'dilbert',),
                    (u'dogbert',),
                    (u'dogbert',),
                    (u'pointy haired boss',),
                    (u'vlad',),
                    (u'wally',),
                    (u'wally',)]
                eq_(sess.query(Person.name)
                        .join(paperwork,
                              Manager.person_id == paperwork.c.person_id)
                        .order_by(Person.name).all(),
                    expected)
            else:
                # when a join is present and managers.person_id is available, 
                # you get the managers.
                expected = [
                    (u'dogbert',),
                    (u'dogbert',),
                    (u'pointy haired boss',)]
                eq_(sess.query(Person.name)
                        .join(paperwork,
                              Manager.person_id == paperwork.c.person_id)
                        .order_by(Person.name).all(),
                    expected)

        def test_join_from_columns_or_subclass_seven(self):
            sess = create_session()
            eq_(sess.query(Manager)
                    .join(Paperwork, Manager.paperwork)
                    .order_by(Manager.name).all(),
                [m1, b1])

        def test_join_from_columns_or_subclass_eight(self):
            sess = create_session()
            expected = [
                (u'dogbert',),
                (u'dogbert',),
                (u'pointy haired boss',)]
            eq_(sess.query(Manager.name)
                    .join(paperwork,
                          Manager.person_id == paperwork.c.person_id)
                    .order_by(Manager.name).all(),
                expected)

        def test_join_from_columns_or_subclass_nine(self):
            sess = create_session()
            eq_(sess.query(Manager.person_id)
                    .join(paperwork,
                          Manager.person_id == paperwork.c.person_id)
                    .order_by(Manager.name).all(),
                [(4,), (4,), (3,)])

        def test_join_from_columns_or_subclass_ten(self):
            sess = create_session()
            expected = [
                (u'pointy haired boss', u'review #1'),
                (u'dogbert', u'review #2'),
                (u'dogbert', u'review #3')]
            eq_(sess.query(Manager.name, Paperwork.description)
                    .join(Paperwork,
                          Manager.person_id == Paperwork.person_id)
                    .order_by(Paperwork.paperwork_id).all(),
                expected)

        def test_join_from_columns_or_subclass_eleven(self):
            sess = create_session()
            expected = [
                (u'pointy haired boss',),
                (u'dogbert',),
                (u'dogbert',)]
            malias = aliased(Manager)
            eq_(sess.query(malias.name)
                    .join(paperwork,
                          malias.person_id == paperwork.c.person_id)
                    .all(),
                expected)

        def test_polymorphic_option(self):
            """
            Test that polymorphic loading sets state.load_path with its 
            actual mapper on a subclass, and not the superclass mapper.
            """

            paths = []
            class MyOption(interfaces.MapperOption):
                propagate_to_loaders = True
                def process_query_conditionally(self, query):
                    paths.append(query._current_path)

            sess = create_session()
            names = ['dilbert', 'pointy haired boss']
            dilbert, boss = (
                sess.query(Person)
                    .options(MyOption())
                    .filter(Person.name.in_(names))
                    .order_by(Person.name).all())

            dilbert.machines
            boss.paperwork

            eq_(paths,
                [(class_mapper(Engineer), 'machines'),
                (class_mapper(Boss), 'paperwork')])

        def test_expire(self):
            """
            Test that individual column refresh doesn't get tripped up by 
            the select_table mapper.
            """

            sess = create_session()

            name = 'dogbert'
            m1 = sess.query(Manager).filter(Manager.name == name).one()
            sess.expire(m1)
            assert m1.status == 'regular manager'

            name = 'pointy haired boss'
            m2 = sess.query(Manager).filter(Manager.name == name).one()
            sess.expire(m2, ['manager_name', 'golf_swing'])
            assert m2.golf_swing == 'fore'

        def _emps_wo_relationships_fixture(self):
            return [
                Engineer(
                    name="dilbert",
                    engineer_name="dilbert",
                    primary_language="java",
                    status="regular engineer"),
                Engineer(
                    name="wally",
                    engineer_name="wally",
                    primary_language="c++",
                    status="regular engineer"),
                Boss(
                    name="pointy haired boss",
                    golf_swing="fore",
                    manager_name="pointy",
                    status="da boss"),
                Manager(
                    name="dogbert",
                    manager_name="dogbert",
                    status="regular manager"),
                Engineer(
                    name="vlad",
                    engineer_name="vlad",
                    primary_language="cobol",
                    status="elbonian engineer")
            ]

        def test_with_polymorphic_one(self):
            sess = create_session()
            def go():
                eq_(sess.query(Person)
                        .with_polymorphic(Engineer)
                        .filter(Engineer.primary_language == 'java').all(),
                    self._emps_wo_relationships_fixture()[0:1])
            self.assert_sql_count(testing.db, go, 1)

        def test_with_polymorphic_two(self):
            sess = create_session()
            def go():
                eq_(sess.query(Person)
                        .with_polymorphic('*').all(),
                    self._emps_wo_relationships_fixture())
            self.assert_sql_count(testing.db, go, 1)

        def test_with_polymorphic_three(self):
            sess = create_session()
            def go():
                eq_(sess.query(Person)
                        .with_polymorphic(Engineer).all(),
                    self._emps_wo_relationships_fixture())
            self.assert_sql_count(testing.db, go, 3)

        def test_with_polymorphic_four(self):
            sess = create_session()
            def go():
                eq_(sess.query(Person)
                        .with_polymorphic(
                            Engineer,
                            people.outerjoin(engineers))
                        .all(),
                    self._emps_wo_relationships_fixture())
            self.assert_sql_count(testing.db, go, 3)

        def test_with_polymorphic_five(self):
            sess = create_session()
            def go():
                # limit the polymorphic join down to just "Person", 
                # overriding select_table
                eq_(sess.query(Person)
                        .with_polymorphic(Person).all(),
                    self._emps_wo_relationships_fixture())
            self.assert_sql_count(testing.db, go, 6)

        def test_with_polymorphic_six(self):
            sess = create_session()

            assert_raises(sa_exc.InvalidRequestError,
                sess.query(Person).with_polymorphic, Paperwork)
            assert_raises(sa_exc.InvalidRequestError,
                sess.query(Engineer).with_polymorphic, Boss)
            assert_raises(sa_exc.InvalidRequestError,
                sess.query(Engineer).with_polymorphic, Person)

        def test_with_polymorphic_seven(self):
            sess = create_session()
            # compare to entities without related collections to prevent 
            # additional lazy SQL from firing on loaded entities
            eq_(sess.query(Person).with_polymorphic('*').all(),
                self._emps_wo_relationships_fixture())


        def test_relationship_to_polymorphic(self):
            expected = [
                Company(
                    name="MegaCorp, Inc.",
                    employees=[
                        Engineer(
                            name="dilbert",
                            engineer_name="dilbert",
                            primary_language="java",
                            status="regular engineer",
                            machines=[
                                Machine(name="IBM ThinkPad"),
                                Machine(name="IPhone")]),
                        Engineer(
                            name="wally",
                            engineer_name="wally",
                            primary_language="c++",
                            status="regular engineer"),
                        Boss(
                            name="pointy haired boss",
                            golf_swing="fore",
                            manager_name="pointy",
                            status="da boss"),
                        Manager(
                            name="dogbert",
                            manager_name="dogbert",
                            status="regular manager"),
                    ]),
                Company(
                    name="Elbonia, Inc.",
                    employees=[
                        Engineer(
                            name="vlad",
                            engineer_name="vlad",
                            primary_language="cobol",
                            status="elbonian engineer")
                    ])
            ]

            sess = create_session()
            def go():
                # test load Companies with lazy load to 'employees'
                eq_(sess.query(Company).all(), expected)
            count = {'':9, 'Polymorphic':4}.get(select_type, 5)
            self.assert_sql_count(testing.db, go, count)

            sess = create_session()
            def go():
                # currently, it doesn't matter if we say Company.employees, 
                # or Company.employees.of_type(Engineer).  joinedloader 
                # doesn't pick up on the "of_type()" as of yet.
                eq_(sess.query(Company)
                        .options(joinedload_all(
                            Company.employees.of_type(Engineer),
                            Engineer.machines))
                        .all(),
                    expected)
            # in the case of select_type='', the joinedload 
            # doesn't take in this case; it joinedloads company->people, 
            # then a load for each of 5 rows, then lazyload of "machines"
            count = {'':7, 'Polymorphic':1}.get(select_type, 2)
            self.assert_sql_count(testing.db, go, count)

            sess = create_session()
            def go():
                eq_(sess.query(Company)
                        .options(subqueryload_all(
                            Company.employees.of_type(Engineer),
                            Engineer.machines))
                        .all(),
                    expected)
            count = {
                '':8,
                'Joins':4,
                'Unions':4,
                'Polymorphic':3,
                'AliasedJoins':4}[select_type]
            self.assert_sql_count(testing.db, go, count)

        def test_joinedload_on_subclass(self):
            sess = create_session()
            expected = [
                Engineer(
                    name="dilbert",
                    engineer_name="dilbert",
                    primary_language="java",
                    status="regular engineer",
                    machines=[
                        Machine(name="IBM ThinkPad"),
                        Machine(name="IPhone")])]

            def go():
                # test load People with joinedload to engineers + machines
                eq_(sess.query(Person)
                        .with_polymorphic('*')
                        .options(joinedload(Engineer.machines))
                        .filter(Person.name == 'dilbert').all(),
                  expected)
            self.assert_sql_count(testing.db, go, 1)

            sess = create_session()
            def go():
                # test load People with subqueryload to engineers + machines
                eq_(sess.query(Person)
                        .with_polymorphic('*')
                        .options(subqueryload(Engineer.machines))
                        .filter(Person.name == 'dilbert').all(),
                  expected)
            self.assert_sql_count(testing.db, go, 2)

        def test_query_subclass_join_to_base_relationship(self):
            sess = create_session()
            # non-polymorphic
            eq_(sess.query(Engineer)
                    .join(Person.paperwork).all(),
                [e1, e2, e3])

        def test_join_to_subclass(self):
            sess = create_session()

            eq_(sess.query(Company)
                    .join(people.join(engineers), 'employees')
                    .filter(Engineer.primary_language == 'java').all(),
                [c1])

        def test_join_to_subclass_one(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .select_from(companies.join(people).join(engineers))
                    .filter(Engineer.primary_language == 'java').all(),
                [c1])

        def test_join_to_subclass_two(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join(people.join(engineers), 'employees')
                    .filter(Engineer.primary_language == 'java').all(),
                [c1])

        def test_join_to_subclass_three(self):
            sess = create_session()
            ealias = aliased(Engineer)
            eq_(sess.query(Company)
                    .join(ealias, 'employees')
                    .filter(ealias.primary_language == 'java').all(),
                [c1])

        if select_type == '':
            def test_join_to_subclass_four(self):
                sess = create_session()
                eq_(sess.query(Person)
                        .select_from(people.join(engineers))
                        .join(Engineer.machines).all(),
                    [e1, e2, e3])

            def test_join_to_subclass_five(self):
                sess = create_session()
                eq_(sess.query(Person)
                        .select_from(people.join(engineers))
                        .join(Engineer.machines)
                        .filter(Machine.name.ilike("%ibm%")).all(),
                    [e1, e3])

        def test_join_to_subclass_six(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join(people.join(engineers), 'employees')
                    .join(Engineer.machines).all(),
                [c1, c2])

        def test_join_to_subclass_seven(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join(people.join(engineers), 'employees')
                    .join(Engineer.machines)
                    .filter(Machine.name.ilike("%thinkpad%")).all(),
                [c1])


        def test_join_to_subclass_eight(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .join(Engineer.machines).all(),
                [e1, e2, e3])

        def test_join_to_subclass_nine(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .select_from(companies.join(people).join(engineers))
                    .filter(Engineer.primary_language == 'java').all(),
                [c1])

        if select_type != '':
            def test_join_to_subclass_ten(self):
                sess = create_session()
                eq_(sess.query(Company)
                        .join('employees')
                        .filter(Engineer.primary_language == 'java').all(),
                    [c1])

        def test_join_to_subclass_eleven(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .select_from(companies.join(people).join(engineers))
                    .filter(Engineer.primary_language == 'java').all(),
                [c1])

        def test_join_to_subclass_twelve(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .join(Engineer.machines).all(),
                [e1, e2, e3])

        def test_join_to_subclass_thirteen(self):
            sess = create_session()
            eq_(sess.query(Person)
                    .join(Engineer.machines)
                    .filter(Machine.name.ilike("%ibm%")).all(),
                [e1, e3])

        def test_join_to_subclass_fourteen(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', Engineer.machines).all(),
                [c1, c2])

        def test_join_to_subclass_fifteen(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', Engineer.machines)
                    .filter(Machine.name.ilike("%thinkpad%")).all(),
                [c1])

        def test_join_to_subclass_sixteen(self):
            sess = create_session()
            # non-polymorphic
            eq_(sess.query(Engineer)
                    .join(Engineer.machines).all(),
                [e1, e2, e3])

        def test_join_to_subclass_seventeen(self):
            sess = create_session()
            eq_(sess.query(Engineer)
                    .join(Engineer.machines)
                    .filter(Machine.name.ilike("%ibm%")).all(),
                [e1, e3])

        def test_join_to_subclass_eightteen(self):
            sess = create_session()
            # here's the new way
            eq_(sess.query(Company)
                    .join(Company.employees.of_type(Engineer))
                    .filter(Engineer.primary_language == 'java').all(),
                [c1])

        def test_join_to_subclass_nineteen(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join(Company.employees.of_type(Engineer), 'machines')
                    .filter(Machine.name.ilike("%thinkpad%")).all(),
                [c1])

        def test_join_to_subclass_count(self):
            sess = create_session()

            eq_(sess.query(Company, Engineer)
                    .join(Company.employees.of_type(Engineer))
                    .filter(Engineer.primary_language == 'java').count(),
                1)

            # test [ticket:2093]
            eq_(sess.query(Company.company_id, Engineer)
                    .join(Company.employees.of_type(Engineer))
                    .filter(Engineer.primary_language == 'java').count(),
                1)

            eq_(sess.query(Company)
                    .join(Company.employees.of_type(Engineer))
                    .filter(Engineer.primary_language == 'java').count(),
                1)

        def test_join_through_polymorphic_nonaliased_one(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', 'paperwork', aliased=False)
                    .filter(Paperwork.description.like('%#2%')).all(),
                [c1])

        def test_join_through_polymorphic_nonaliased_two(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', 'paperwork', aliased=False)
                    .filter(Paperwork.description.like('%#%')).all(),
                [c1, c2])

        def test_join_through_polymorphic_nonaliased_three(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', 'paperwork', aliased=False)
                    .filter(Person.name.in_(['dilbert', 'vlad']))
                    .filter(Paperwork.description.like('%#2%')).all(),
                [c1])

        def test_join_through_polymorphic_nonaliased_four(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', 'paperwork', aliased=False)
                    .filter(Person.name.in_(['dilbert', 'vlad']))
                    .filter(Paperwork.description.like('%#%')).all(),
                [c1, c2])

        def test_join_through_polymorphic_nonaliased_five(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', aliased=aliased)
                    .filter(Person.name.in_(['dilbert', 'vlad']))
                    .join('paperwork', from_joinpoint=True, aliased=False)
                    .filter(Paperwork.description.like('%#2%')).all(),
                [c1])

        def test_join_through_polymorphic_nonaliased_six(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', aliased=aliased)
                    .filter(Person.name.in_(['dilbert', 'vlad']))
                    .join('paperwork', from_joinpoint=True, aliased=False)
                    .filter(Paperwork.description.like('%#%')).all(),
                [c1, c2])

        def test_join_through_polymorphic_aliased_one(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', 'paperwork', aliased=True)
                    .filter(Paperwork.description.like('%#2%')).all(),
                [c1])

        def test_join_through_polymorphic_aliased_two(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', 'paperwork', aliased=True)
                    .filter(Paperwork.description.like('%#%')).all(),
                [c1, c2])

        def test_join_through_polymorphic_aliased_three(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', 'paperwork', aliased=True)
                    .filter(Person.name.in_(['dilbert', 'vlad']))
                    .filter(Paperwork.description.like('%#2%')).all(),
                [c1])

        def test_join_through_polymorphic_aliased_four(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', 'paperwork', aliased=True)
                    .filter(Person.name.in_(['dilbert', 'vlad']))
                    .filter(Paperwork.description.like('%#%')).all(),
                [c1, c2])

        def test_join_through_polymorphic_aliased_five(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', aliased=aliased)
                    .filter(Person.name.in_(['dilbert', 'vlad']))
                    .join('paperwork', from_joinpoint=True, aliased=True)
                    .filter(Paperwork.description.like('%#2%')).all(),
                [c1])

        def test_join_through_polymorphic_aliased_six(self):
            sess = create_session()
            eq_(sess.query(Company)
                    .join('employees', aliased=aliased)
                    .filter(Person.name.in_(['dilbert', 'vlad']))
                    .join('paperwork', from_joinpoint=True, aliased=True)
                    .filter(Paperwork.description.like('%#%')).all(),
                [c1, c2])

        def test_explicit_polymorphic_join(self):
            sess = create_session()

            # join from Company to Engineer; join condition formulated by
            # ORMJoin using regular table foreign key connections.  Engineer
            # is expressed as "(select * people join engineers) as anon_1"
            # so the join is contained.
            eq_(sess.query(Company)
                    .join(Engineer)
                    .filter(Engineer.engineer_name == 'vlad').one(),
                c2)

            # same, using explicit join condition.  Query.join() must 
            # adapt the on clause here to match the subquery wrapped around 
            # "people join engineers".
            eq_(sess.query(Company)
                    .join(Engineer, Company.company_id == Engineer.company_id)
                    .filter(Engineer.engineer_name == 'vlad').one(),
                c2)

        def test_filter_on_baseclass(self):
            sess = create_session()
            eq_(sess.query(Person).all(), all_employees)
            eq_(sess.query(Person).first(), all_employees[0])
            eq_(sess.query(Person)
                    .filter(Person.person_id == e2.person_id).one(),
                e2)

        def test_from_alias(self):
            sess = create_session()
            palias = aliased(Person)
            eq_(sess.query(palias)
                    .filter(palias.name.in_(['dilbert', 'wally'])).all(),
                [e1, e2])

        def test_self_referential(self):
            sess = create_session()
            palias = aliased(Person)
            expected = [(m1, e1), (m1, e2), (m1, b1)]

            eq_(sess.query(Person, palias)
                    .filter(Person.company_id == palias.company_id)
                    .filter(Person.name == 'dogbert')
                    .filter(Person.person_id > palias.person_id)
                    .order_by(Person.person_id, palias.person_id).all(),
                expected)

            eq_(sess.query(Person, palias)
                    .filter(Person.company_id == palias.company_id)
                    .filter(Person.name == 'dogbert')
                    .filter(Person.person_id > palias.person_id)
                    .from_self()
                    .order_by(Person.person_id, palias.person_id).all(),
                expected)

        def test_nesting_queries(self):
            # query.statement places a flag "no_adapt" on the returned 
            # statement.  This prevents the polymorphic adaptation in the 
            # second "filter" from hitting it, which would pollute the 
            # subquery and usually results in recursion overflow errors 
            # within the adaption.
            sess = create_session()
            subq = (sess.query(engineers.c.person_id)
                        .filter(Engineer.primary_language == 'java')
                        .statement.as_scalar())
            eq_(sess.query(Person)
                    .filter(Person.person_id.in_(subq)).one(),
                e1)



        if select_type != '':
            def test_mixed_entities_one(self):
                sess = create_session()

                expected = [
                    (Engineer(
                        status=u'regular engineer',
                        engineer_name=u'dilbert',
                        name=u'dilbert',
                        company_id=1,
                        primary_language=u'java',
                        person_id=1,
                        type=u'engineer'),
                    u'MegaCorp, Inc.'),
                    (Engineer(
                        status=u'regular engineer',
                        engineer_name=u'wally',
                        name=u'wally',
                        company_id=1,
                        primary_language=u'c++',
                        person_id=2,
                        type=u'engineer'),
                    u'MegaCorp, Inc.'),
                    (Engineer(
                        status=u'elbonian engineer',
                        engineer_name=u'vlad',
                        name=u'vlad',
                        company_id=2,
                        primary_language=u'cobol',
                        person_id=5,
                        type=u'engineer'),
                    u'Elbonia, Inc.')]
                eq_(sess.query(Engineer, Company.name)
                        .join(Company.employees)
                        .filter(Person.type == 'engineer').all(),
                    expected)

        if select_type != '':
            def test_mixed_entities_two(self):
                sess = create_session()
                expected = [
                    (u'java', u'MegaCorp, Inc.'),
                    (u'cobol', u'Elbonia, Inc.'),
                    (u'c++', u'MegaCorp, Inc.')]
                eq_(sess.query(Engineer.primary_language, Company.name)
                        .join(Company.employees)
                        .filter(Person.type == 'engineer')
                        .order_by(desc(Engineer.primary_language)).all(),
                    expected)

        def test_mixed_entities_three(self):
            sess = create_session()
            palias = aliased(Person)
            expected = [(
                Engineer(
                    status=u'elbonian engineer',
                    engineer_name=u'vlad',
                    name=u'vlad',
                    primary_language=u'cobol'),
                u'Elbonia, Inc.',
                Engineer(
                    status=u'regular engineer',
                    engineer_name=u'dilbert',
                    name=u'dilbert',
                    company_id=1,
                    primary_language=u'java',
                    person_id=1,
                    type=u'engineer'))]
            eq_(sess.query(Person, Company.name, palias)
                    .join(Company.employees)
                    .filter(Company.name == 'Elbonia, Inc.')
                    .filter(palias.name == 'dilbert').all(),
                expected)

        def test_mixed_entities_four(self):
            sess = create_session()
            palias = aliased(Person)
            expected = [(
                Engineer(
                    status=u'regular engineer',
                    engineer_name=u'dilbert',
                    name=u'dilbert',
                    company_id=1,
                    primary_language=u'java',
                    person_id=1,
                    type=u'engineer'),
                u'Elbonia, Inc.',
                Engineer(
                    status=u'elbonian engineer',
                    engineer_name=u'vlad',
                    name=u'vlad',
                    primary_language=u'cobol'),)]
            eq_(sess.query(palias, Company.name, Person)
                    .join(Company.employees)
                    .filter(Company.name == 'Elbonia, Inc.')
                    .filter(palias.name == 'dilbert').all(),
                expected)

        def test_mixed_entities_five(self):
            sess = create_session()
            palias = aliased(Person)
            expected = [(u'vlad', u'Elbonia, Inc.', u'dilbert')]
            eq_(sess.query(Person.name, Company.name, palias.name)
                    .join(Company.employees)
                    .filter(Company.name == 'Elbonia, Inc.')
                    .filter(palias.name == 'dilbert').all(),
                expected)

        def test_mixed_entities_six(self):
            sess = create_session()
            palias = aliased(Person)
            expected = [
                (u'manager', u'dogbert', u'engineer', u'dilbert'),
                (u'manager', u'dogbert', u'engineer', u'wally'),
                (u'manager', u'dogbert', u'boss', u'pointy haired boss')]
            eq_(sess.query(Person.type, Person.name, palias.type, palias.name)
                    .filter(Person.company_id == palias.company_id)
                    .filter(Person.name == 'dogbert')
                    .filter(Person.person_id > palias.person_id)
                    .order_by(Person.person_id, palias.person_id).all(),
                expected)

        def test_mixed_entities_seven(self):
            sess = create_session()
            expected = [
                (u'dilbert', u'tps report #1'),
                (u'dilbert', u'tps report #2'),
                (u'dogbert', u'review #2'),
                (u'dogbert', u'review #3'),
                (u'pointy haired boss', u'review #1'),
                (u'vlad', u'elbonian missive #3'),
                (u'wally', u'tps report #3'),
                (u'wally', u'tps report #4')]
            eq_(sess.query(Person.name, Paperwork.description)
                    .filter(Person.person_id == Paperwork.person_id)
                    .order_by(Person.name, Paperwork.description).all(),
                expected)

        if select_type != '':
            def test_mixed_entities_eight(self):
                sess = create_session()
                eq_(sess.query(func.count(Person.person_id))
                        .filter(Engineer.primary_language == 'java').all(),
                    [(1,)])

        def test_mixed_entities_nine(self):
            sess = create_session()
            expected = [(u'Elbonia, Inc.', 1), (u'MegaCorp, Inc.', 4)]
            eq_(sess.query(Company.name, func.count(Person.person_id))
                    .filter(Company.company_id == Person.company_id)
                    .group_by(Company.name)
                    .order_by(Company.name).all(),
                expected)

        def test_mixed_entities_ten(self):
            sess = create_session()
            expected = [(u'Elbonia, Inc.', 1), (u'MegaCorp, Inc.', 4)]
            eq_(sess.query(Company.name, func.count(Person.person_id))
                    .join(Company.employees)
                    .group_by(Company.name)
                    .order_by(Company.name).all(),
                expected)

        #def test_mixed_entities(self):
        #    sess = create_session()
            # TODO: I think raise error on these for now.  different 
            # inheritance/loading schemes have different results here, 
            # all incorrect
            #
            # eq_(
            #    sess.query(Person.name, Engineer.primary_language).all(),
            #    [])

        #def test_mixed_entities(self):
        #    sess = create_session()
            # eq_(sess.query(
            #             Person.name, 
            #             Engineer.primary_language, 
            #             Manager.manager_name)
            #          .all(),
            #     [])

        def test_mixed_entities_eleven(self):
            sess = create_session()
            expected = [(u'java',), (u'c++',), (u'cobol',)]
            eq_(sess.query(Engineer.primary_language)
                    .filter(Person.type == 'engineer').all(),
                expected)

        def test_mixed_entities_twelve(self):
            sess = create_session()
            expected = [(u'vlad', u'Elbonia, Inc.')]
            eq_(sess.query(Person.name, Company.name)
                    .join(Company.employees)
                    .filter(Company.name == 'Elbonia, Inc.').all(),
                expected)

        def test_mixed_entities_thirteen(self):
            sess = create_session()
            expected = [(u'pointy haired boss', u'fore')]
            eq_(sess.query(Boss.name, Boss.golf_swing).all(), expected)

        def test_mixed_entities_fourteen(self):
            sess = create_session()
            expected = [
                (u'dilbert', u'java'),
                (u'wally', u'c++'),
                (u'vlad', u'cobol')]
            eq_(sess.query(Engineer.name, Engineer.primary_language).all(),
                expected)

        def test_mixed_entities_fifteen(self):
            sess = create_session()

            expected = [(
                u'Elbonia, Inc.',
                Engineer(
                    status=u'elbonian engineer',
                    engineer_name=u'vlad',
                    name=u'vlad',
                    primary_language=u'cobol'))]
            eq_(sess.query(Company.name, Person)
                    .join(Company.employees)
                    .filter(Company.name == 'Elbonia, Inc.').all(),
                expected)

        def test_mixed_entities_sixteen(self):
            sess = create_session()
            expected = [(
                Engineer(
                    status=u'elbonian engineer',
                    engineer_name=u'vlad',
                    name=u'vlad',
                    primary_language=u'cobol'),
                u'Elbonia, Inc.')]
            eq_(sess.query(Person, Company.name)
                    .join(Company.employees)
                    .filter(Company.name == 'Elbonia, Inc.').all(),
                expected)

        def test_mixed_entities_seventeen(self):
            sess = create_session()
            expected = [('pointy haired boss',), ('dogbert',)]
            eq_(sess.query(Manager.name).all(), expected)

        def test_mixed_entities_eighteen(self):
            sess = create_session()
            expected = [('pointy haired boss foo',), ('dogbert foo',)]
            eq_(sess.query(Manager.name + " foo").all(), expected)

        def test_mixed_entities_nineteen(self):
            sess = create_session()
            row = sess.query(Engineer.name, Engineer.primary_language) \
                      .filter(Engineer.name == 'dilbert').first()
            assert row.name == 'dilbert'
            assert row.primary_language == 'java'

    PolymorphicQueryTest.__name__ = "Polymorphic%sTest" % select_type
    return PolymorphicQueryTest

for select_type in ('', 'Polymorphic', 'Unions', 'AliasedJoins', 'Joins'):
    testclass = _produce_test(select_type)
    exec("%s = testclass" % testclass.__name__)

del testclass
