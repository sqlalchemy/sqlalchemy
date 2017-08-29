from sqlalchemy import func, desc, select
from sqlalchemy.orm import (interfaces, create_session, joinedload,
                            joinedload_all, subqueryload, subqueryload_all,
                            aliased, class_mapper, with_polymorphic)
from sqlalchemy import exc as sa_exc

from sqlalchemy import testing
from sqlalchemy.testing import assert_raises, eq_

from ._poly_fixtures import Company, Person, Engineer, Manager, Boss, \
    Machine, Paperwork, _Polymorphic,\
    _PolymorphicPolymorphic, _PolymorphicUnions, _PolymorphicJoins,\
    _PolymorphicAliasedJoins


class _PolymorphicTestBase(object):
    __backend__ = True

    @classmethod
    def setup_mappers(cls):
        super(_PolymorphicTestBase, cls).setup_mappers()
        global people, engineers, managers, boss
        global companies, paperwork, machines
        people, engineers, managers, boss,\
            companies, paperwork, machines = \
            cls.tables.people, cls.tables.engineers, \
            cls.tables.managers, cls.tables.boss,\
            cls.tables.companies, cls.tables.paperwork, cls.tables.machines

    @classmethod
    def insert_data(cls):
        super(_PolymorphicTestBase, cls).insert_data()

        global all_employees, c1_employees, c2_employees
        global c1, c2, e1, e2, e3, b1, m1
        c1, c2, all_employees, c1_employees, c2_employees = \
            cls.c1, cls.c2, cls.all_employees, \
            cls.c1_employees, cls.c2_employees
        e1, e2, e3, b1, m1 = \
            cls.e1, cls.e2, cls.e3, cls.b1, cls.m1

    def test_loads_at_once(self):
        """
        Test that all objects load from the full query, when
        with_polymorphic is used.
        """

        sess = create_session()

        def go():
            eq_(
                sess.query(Person).order_by(Person.person_id).all(),
                all_employees)
        count = {'': 14, 'Polymorphic': 9}.get(self.select_type, 10)
        self.assert_sql_count(testing.db, go, count)

    def test_primary_eager_aliasing_one(self):
        # For both joinedload() and subqueryload(), if the original q is
        # not loading the subclass table, the joinedload doesn't happen.

        sess = create_session()

        def go():
            eq_(sess.query(Person).order_by(Person.person_id)
                    .options(joinedload(Engineer.machines))[1:3],
                all_employees[1:3])
        count = {'': 6, 'Polymorphic': 3}.get(self.select_type, 4)
        self.assert_sql_count(testing.db, go, count)

    def test_primary_eager_aliasing_two(self):
        sess = create_session()

        def go():
            eq_(sess.query(Person).order_by(Person.person_id)
                    .options(subqueryload(Engineer.machines)).all(),
                all_employees)
        count = {'': 14, 'Polymorphic': 7}.get(self.select_type, 8)
        self.assert_sql_count(testing.db, go, count)

    def test_primary_eager_aliasing_three(self):

        # assert the JOINs don't over JOIN

        sess = create_session()

        def go():
            eq_(sess.query(Person).with_polymorphic('*')
                    .order_by(Person.person_id)
                    .options(joinedload(Engineer.machines))[1:3],
                all_employees[1:3])
        self.assert_sql_count(testing.db, go, 3)

        eq_(
            select([func.count('*')]).select_from(
                sess.query(Person).with_polymorphic('*')
                .options(joinedload(Engineer.machines))
                .order_by(Person.person_id).limit(2).offset(1)
                .with_labels().subquery()
            ).scalar(), 2)

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
                Company(company_id=1, name='MegaCorp, Inc.'),
                Engineer(
                    status='regular engineer',
                    engineer_name='dilbert',
                    name='dilbert',
                    company_id=1,
                    primary_language='java',
                    person_id=1,
                    type='engineer'),
                Company(company_id=1, name='MegaCorp, Inc.'),
                Engineer(
                    status='regular engineer',
                    engineer_name='wally',
                    name='wally',
                    company_id=1,
                    primary_language='c++',
                    person_id=2,
                    type='engineer')
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
                .order_by(Person.person_id)
                .join('paperwork', aliased=False)
                .filter(Paperwork.description.like('%#2%')).all(),
            [e1, m1])

    def test_join_from_polymorphic_nonaliased_three(self):
        sess = create_session()
        eq_(sess.query(Engineer)
                .order_by(Person.person_id)
                .join('paperwork', aliased=False)
                .filter(Paperwork.description.like('%#2%')).all(),
            [e1])

    def test_join_from_polymorphic_nonaliased_four(self):
        sess = create_session()
        eq_(sess.query(Person)
                .order_by(Person.person_id)
                .join('paperwork', aliased=False)
                .filter(Person.name.like('%dog%'))
                .filter(Paperwork.description.like('%#2%')).all(),
            [m1])

    def test_join_from_polymorphic_aliased_one(self):
        sess = create_session()
        eq_(sess.query(Person)
                .order_by(Person.person_id)
                .join('paperwork', aliased=True)
                .filter(Paperwork.description.like('%review%')).all(),
            [b1, m1])

    def test_join_from_polymorphic_aliased_two(self):
        sess = create_session()
        eq_(sess.query(Person)
                .order_by(Person.person_id)
                .join('paperwork', aliased=True)
                .filter(Paperwork.description.like('%#2%')).all(),
            [e1, m1])

    def test_join_from_polymorphic_aliased_three(self):
        sess = create_session()
        eq_(sess.query(Engineer)
                .order_by(Person.person_id)
                .join('paperwork', aliased=True)
                .filter(Paperwork.description.like('%#2%')).all(),
            [e1])

    def test_join_from_polymorphic_aliased_four(self):
        sess = create_session()
        eq_(sess.query(Person)
                .order_by(Person.person_id)
                .join('paperwork', aliased=True)
                .filter(Person.name.like('%dog%'))
                .filter(Paperwork.description.like('%#2%')).all(),
            [m1])

    def test_join_from_with_polymorphic_nonaliased_one(self):
        sess = create_session()
        eq_(sess.query(Person)
                .with_polymorphic(Manager)
                .order_by(Person.person_id)
                .join('paperwork')
                .filter(Paperwork.description.like('%review%')).all(),
            [b1, m1])

    def test_join_from_with_polymorphic_nonaliased_two(self):
        sess = create_session()
        eq_(sess.query(Person)
                .with_polymorphic([Manager, Engineer])
                .order_by(Person.person_id)
                .join('paperwork')
                .filter(Paperwork.description.like('%#2%')).all(),
            [e1, m1])

    def test_join_from_with_polymorphic_nonaliased_three(self):
        sess = create_session()
        eq_(sess.query(Person)
                .with_polymorphic([Manager, Engineer])
                .order_by(Person.person_id)
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
                .order_by(Person.person_id)
                .join('paperwork', aliased=True)
                .filter(Paperwork.description.like('%#2%')).all(),
            [e1, m1])

    def test_join_from_with_polymorphic_aliased_three(self):
        sess = create_session()
        eq_(sess.query(Person)
                .with_polymorphic([Manager, Engineer])
                .order_by(Person.person_id)
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

    def test_polymorphic_any_eight(self):
        sess = create_session()
        any_ = Engineer.machines.any(
            Machine.name == "Commodore 64")
        eq_(
            sess.query(Person).order_by(Person.person_id).filter(any_).all(),
            [e2, e3])

    def test_polymorphic_any_nine(self):
        sess = create_session()
        any_ = Person.paperwork.any(
            Paperwork.description == "review #2")
        eq_(
            sess.query(Person).order_by(Person.person_id).filter(any_).all(),
            [m1])

    def test_join_from_columns_or_subclass_one(self):
        sess = create_session()

        expected = [
            ('dogbert',),
            ('pointy haired boss',)]
        eq_(sess.query(Manager.name)
                .order_by(Manager.name).all(),
            expected)

    def test_join_from_columns_or_subclass_two(self):
        sess = create_session()
        expected = [
            ('dogbert',),
            ('dogbert',),
            ('pointy haired boss',)]
        eq_(sess.query(Manager.name)
                .join(Paperwork, Manager.paperwork)
                .order_by(Manager.name).all(),
            expected)

    def test_join_from_columns_or_subclass_three(self):
        sess = create_session()
        expected = [
            ('dilbert',),
            ('dilbert',),
            ('dogbert',),
            ('dogbert',),
            ('pointy haired boss',),
            ('vlad',),
            ('wally',),
            ('wally',)]
        eq_(sess.query(Person.name)
                .join(Paperwork, Person.paperwork)
                .order_by(Person.name).all(),
            expected)

    def test_join_from_columns_or_subclass_four(self):
        sess = create_session()
        # Load Person.name, joining from Person -> paperwork, get all
        # the people.
        expected = [
            ('dilbert',),
            ('dilbert',),
            ('dogbert',),
            ('dogbert',),
            ('pointy haired boss',),
            ('vlad',),
            ('wally',),
            ('wally',)]
        eq_(sess.query(Person.name)
                .join(paperwork,
                      Person.person_id == paperwork.c.person_id)
                .order_by(Person.name).all(),
            expected)

    def test_join_from_columns_or_subclass_five(self):
        sess = create_session()
        # same, on manager.  get only managers.
        expected = [
            ('dogbert',),
            ('dogbert',),
            ('pointy haired boss',)]
        eq_(sess.query(Manager.name)
                .join(paperwork,
                      Manager.person_id == paperwork.c.person_id)
                .order_by(Person.name).all(),
            expected)

    def test_join_from_columns_or_subclass_six(self):
        sess = create_session()
        if self.select_type == '':
            # this now raises, due to [ticket:1892].  Manager.person_id
            # is now the "person_id" column on Manager. SQL is incorrect.
            assert_raises(
                sa_exc.DBAPIError,
                sess.query(Person.name)
                    .join(paperwork,
                          Manager.person_id == paperwork.c.person_id)
                    .order_by(Person.name).all)
        elif self.select_type == 'Unions':
            # with the union, not something anyone would really be using
            # here, it joins to the full result set.  This is 0.6's
            # behavior and is more or less wrong.
            expected = [
                ('dilbert',),
                ('dilbert',),
                ('dogbert',),
                ('dogbert',),
                ('pointy haired boss',),
                ('vlad',),
                ('wally',),
                ('wally',)]
            eq_(sess.query(Person.name)
                    .join(paperwork,
                          Manager.person_id == paperwork.c.person_id)
                    .order_by(Person.name).all(),
                expected)
        else:
            # when a join is present and managers.person_id is available,
            # you get the managers.
            expected = [
                ('dogbert',),
                ('dogbert',),
                ('pointy haired boss',)]
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
            ('dogbert',),
            ('dogbert',),
            ('pointy haired boss',)]
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
            ('pointy haired boss', 'review #1'),
            ('dogbert', 'review #2'),
            ('dogbert', 'review #3')]
        eq_(sess.query(Manager.name, Paperwork.description)
                .join(Paperwork,
                      Manager.person_id == Paperwork.person_id)
                .order_by(Paperwork.paperwork_id).all(),
            expected)

    def test_join_from_columns_or_subclass_eleven(self):
        sess = create_session()
        expected = [
            ('pointy haired boss',),
            ('dogbert',),
            ('dogbert',)]
        malias = aliased(Manager)
        eq_(sess.query(malias.name)
                .join(paperwork,
                      malias.person_id == paperwork.c.person_id)
                .all(),
            expected)

    def test_subclass_option_pathing(self):
        from sqlalchemy.orm import defer
        sess = create_session()
        dilbert = sess.query(Person).\
            options(defer(Engineer.machines, Machine.name)).\
            filter(Person.name == 'dilbert').first()
        m = dilbert.machines[0]
        assert 'name' not in m.__dict__
        eq_(m.name, 'IBM ThinkPad')

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
                    .with_polymorphic('*').order_by(Person.person_id).all(),
                self._emps_wo_relationships_fixture())
        self.assert_sql_count(testing.db, go, 1)

    def test_with_polymorphic_three(self):
        sess = create_session()

        def go():
            eq_(sess.query(Person)
                    .with_polymorphic(Engineer).
                order_by(Person.person_id).all(),
                self._emps_wo_relationships_fixture())
        self.assert_sql_count(testing.db, go, 3)

    def test_with_polymorphic_four(self):
        sess = create_session()

        def go():
            eq_(sess.query(Person)
                    .with_polymorphic(
                        Engineer,
                        people.outerjoin(engineers))
                    .order_by(Person.person_id)
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
        eq_(sess.query(Person).with_polymorphic('*').
            order_by(Person.person_id).all(),
            self._emps_wo_relationships_fixture())

    def test_relationship_to_polymorphic_one(self):
        expected = self._company_with_emps_machines_fixture()
        sess = create_session()

        def go():
            # test load Companies with lazy load to 'employees'
            eq_(sess.query(Company).all(), expected)
        count = {'': 10, 'Polymorphic': 5}.get(self.select_type, 6)
        self.assert_sql_count(testing.db, go, count)

    def test_relationship_to_polymorphic_two(self):
        expected = self._company_with_emps_machines_fixture()
        sess = create_session()

        def go():
            # with #2438, of_type() is recognized.  This
            # overrides the with_polymorphic of the mapper
            # and we get a consistent 3 queries now.
            eq_(sess.query(Company)
                    .options(joinedload_all(
                        Company.employees.of_type(Engineer),
                        Engineer.machines))
                    .all(),
                expected)

        # in the old case, we would get this
        # count = {'':7, 'Polymorphic':1}.get(self.select_type, 2)

        # query one is company->Person/Engineer->Machines
        # query two is managers + boss for row #3
        # query three is managers for row #4
        count = 3
        self.assert_sql_count(testing.db, go, count)

    def test_relationship_to_polymorphic_three(self):
        expected = self._company_with_emps_machines_fixture()
        sess = create_session()

        sess = create_session()

        def go():
            eq_(sess.query(Company)
                    .options(subqueryload_all(
                        Company.employees.of_type(Engineer),
                        Engineer.machines))
                    .all(),
                expected)

        # the old case where subqueryload_all
        # didn't work with of_tyoe
        # count = { '':8, 'Joins':4, 'Unions':4, 'Polymorphic':3,
        #    'AliasedJoins':4}[self.select_type]

        # query one is company->Person/Engineer->Machines
        # query two is Person/Engineer subq
        # query three is Machines subq
        # (however this test can't tell if the Q was a
        # lazyload or subqload ...)
        # query four is managers + boss for row #3
        # query five is managers for row #4
        count = 5
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

    def test_subqueryload_on_subclass(self):
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

    def test_join_to_subclass_six(self):
        sess = create_session()
        eq_(sess.query(Company)
                .join(people.join(engineers), 'employees')
                .join(Engineer.machines).all(),
            [c1, c2])

    def test_join_to_subclass_six_point_five(self):
        sess = create_session()
        eq_(sess.query(Company)
                .join(people.join(engineers), 'employees')
                .join(Engineer.machines)
                .filter(Engineer.name == 'dilbert').all(),
            [c1])

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

    def test_explicit_polymorphic_join_one(self):
        sess = create_session()

        # join from Company to Engineer; join condition formulated by
        # ORMJoin using regular table foreign key connections.  Engineer
        # is expressed as "(select * people join engineers) as anon_1"
        # so the join is contained.
        eq_(sess.query(Company)
                .join(Engineer)
                .filter(Engineer.engineer_name == 'vlad').one(),
            c2)

    def test_explicit_polymorphic_join_two(self):
        sess = create_session()

        # same, using explicit join condition.  Query.join() must
        # adapt the on clause here to match the subquery wrapped around
        # "people join engineers".
        eq_(sess.query(Company)
                .join(Engineer, Company.company_id == Engineer.company_id)
                .filter(Engineer.engineer_name == 'vlad').one(),
            c2)

    def test_filter_on_baseclass(self):
        sess = create_session()
        eq_(sess.query(Person).order_by(Person.person_id).all(), all_employees)
        eq_(
            sess.query(Person).order_by(Person.person_id).first(),
            all_employees[0])
        eq_(sess.query(Person).order_by(Person.person_id)
                .filter(Person.person_id == e2.person_id).one(),
            e2)

    def test_from_alias(self):
        sess = create_session()
        palias = aliased(Person)
        eq_(sess.query(palias)
                .order_by(palias.person_id)
                .filter(palias.name.in_(['dilbert', 'wally'])).all(),
            [e1, e2])

    def test_self_referential_one(self):
        sess = create_session()
        palias = aliased(Person)
        expected = [(m1, e1), (m1, e2), (m1, b1)]

        eq_(sess.query(Person, palias)
                .filter(Person.company_id == palias.company_id)
                .filter(Person.name == 'dogbert')
                .filter(Person.person_id > palias.person_id)
                .order_by(Person.person_id, palias.person_id).all(),
            expected)

    def test_self_referential_two(self):
        sess = create_session()
        palias = aliased(Person)
        expected = [(m1, e1), (m1, e2), (m1, b1)]

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

    def test_mixed_entities_one(self):
        sess = create_session()

        expected = [
            (Engineer(
                status='regular engineer',
                engineer_name='dilbert',
                name='dilbert',
                company_id=1,
                primary_language='java',
                person_id=1,
                type='engineer'),
             'MegaCorp, Inc.'),
            (Engineer(
                status='regular engineer',
                engineer_name='wally',
                name='wally',
                company_id=1,
                primary_language='c++',
                person_id=2,
                type='engineer'),
             'MegaCorp, Inc.'),
            (Engineer(
                status='elbonian engineer',
                engineer_name='vlad',
                name='vlad',
                company_id=2,
                primary_language='cobol',
                person_id=5,
                type='engineer'),
             'Elbonia, Inc.')]
        eq_(sess.query(Engineer, Company.name)
                .join(Company.employees)
                .order_by(Person.person_id)
                .filter(Person.type == 'engineer').all(),
            expected)

    def test_mixed_entities_two(self):
        sess = create_session()
        expected = [
            ('java', 'MegaCorp, Inc.'),
            ('cobol', 'Elbonia, Inc.'),
            ('c++', 'MegaCorp, Inc.')]
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
                status='elbonian engineer',
                engineer_name='vlad',
                name='vlad',
                primary_language='cobol'),
            'Elbonia, Inc.',
            Engineer(
                status='regular engineer',
                engineer_name='dilbert',
                name='dilbert',
                company_id=1,
                primary_language='java',
                person_id=1,
                type='engineer'))]
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
                status='regular engineer',
                engineer_name='dilbert',
                name='dilbert',
                company_id=1,
                primary_language='java',
                person_id=1,
                type='engineer'),
            'Elbonia, Inc.',
            Engineer(
                status='elbonian engineer',
                engineer_name='vlad',
                name='vlad',
                primary_language='cobol'),)]
        eq_(sess.query(palias, Company.name, Person)
                .join(Company.employees)
                .filter(Company.name == 'Elbonia, Inc.')
                .filter(palias.name == 'dilbert').all(),
            expected)

    def test_mixed_entities_five(self):
        sess = create_session()
        palias = aliased(Person)
        expected = [('vlad', 'Elbonia, Inc.', 'dilbert')]
        eq_(sess.query(Person.name, Company.name, palias.name)
                .join(Company.employees)
                .filter(Company.name == 'Elbonia, Inc.')
                .filter(palias.name == 'dilbert').all(),
            expected)

    def test_mixed_entities_six(self):
        sess = create_session()
        palias = aliased(Person)
        expected = [
            ('manager', 'dogbert', 'engineer', 'dilbert'),
            ('manager', 'dogbert', 'engineer', 'wally'),
            ('manager', 'dogbert', 'boss', 'pointy haired boss')]
        eq_(sess.query(Person.type, Person.name, palias.type, palias.name)
                .filter(Person.company_id == palias.company_id)
                .filter(Person.name == 'dogbert')
                .filter(Person.person_id > palias.person_id)
                .order_by(Person.person_id, palias.person_id).all(),
            expected)

    def test_mixed_entities_seven(self):
        sess = create_session()
        expected = [
            ('dilbert', 'tps report #1'),
            ('dilbert', 'tps report #2'),
            ('dogbert', 'review #2'),
            ('dogbert', 'review #3'),
            ('pointy haired boss', 'review #1'),
            ('vlad', 'elbonian missive #3'),
            ('wally', 'tps report #3'),
            ('wally', 'tps report #4')]
        eq_(sess.query(Person.name, Paperwork.description)
                .filter(Person.person_id == Paperwork.person_id)
                .order_by(Person.name, Paperwork.description).all(),
            expected)

    def test_mixed_entities_eight(self):
        sess = create_session()
        eq_(sess.query(func.count(Person.person_id))
                .filter(Engineer.primary_language == 'java').all(),
            [(1,)])

    def test_mixed_entities_nine(self):
        sess = create_session()
        expected = [('Elbonia, Inc.', 1), ('MegaCorp, Inc.', 4)]
        eq_(sess.query(Company.name, func.count(Person.person_id))
                .filter(Company.company_id == Person.company_id)
                .group_by(Company.name)
                .order_by(Company.name).all(),
            expected)

    def test_mixed_entities_ten(self):
        sess = create_session()
        expected = [('Elbonia, Inc.', 1), ('MegaCorp, Inc.', 4)]
        eq_(sess.query(Company.name, func.count(Person.person_id))
                .join(Company.employees)
                .group_by(Company.name)
                .order_by(Company.name).all(),
            expected)

    # def test_mixed_entities(self):
    #    sess = create_session()
        # TODO: I think raise error on these for now.  different
        # inheritance/loading schemes have different results here,
        # all incorrect
        #
        # eq_(
        #    sess.query(Person.name, Engineer.primary_language).all(),
        #    [])

    # def test_mixed_entities(self):
    #    sess = create_session()
        # eq_(sess.query(
        #             Person.name,
        #             Engineer.primary_language,
        #             Manager.manager_name)
        #          .all(),
        #     [])

    def test_mixed_entities_eleven(self):
        sess = create_session()
        expected = [('java',), ('c++',), ('cobol',)]
        eq_(sess.query(Engineer.primary_language)
                .filter(Person.type == 'engineer').all(),
            expected)

    def test_mixed_entities_twelve(self):
        sess = create_session()
        expected = [('vlad', 'Elbonia, Inc.')]
        eq_(sess.query(Person.name, Company.name)
                .join(Company.employees)
                .filter(Company.name == 'Elbonia, Inc.').all(),
            expected)

    def test_mixed_entities_thirteen(self):
        sess = create_session()
        expected = [('pointy haired boss', 'fore')]
        eq_(sess.query(Boss.name, Boss.golf_swing).all(), expected)

    def test_mixed_entities_fourteen(self):
        sess = create_session()
        expected = [
            ('dilbert', 'java'),
            ('wally', 'c++'),
            ('vlad', 'cobol')]
        eq_(sess.query(Engineer.name, Engineer.primary_language).all(),
            expected)

    def test_mixed_entities_fifteen(self):
        sess = create_session()

        expected = [(
            'Elbonia, Inc.',
            Engineer(
                status='elbonian engineer',
                engineer_name='vlad',
                name='vlad',
                primary_language='cobol'))]
        eq_(sess.query(Company.name, Person)
                .join(Company.employees)
                .filter(Company.name == 'Elbonia, Inc.').all(),
            expected)

    def test_mixed_entities_sixteen(self):
        sess = create_session()
        expected = [(
            Engineer(
                status='elbonian engineer',
                engineer_name='vlad',
                name='vlad',
                primary_language='cobol'),
            'Elbonia, Inc.')]
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

    def test_correlation_one(self):
        sess = create_session()

        # unfortunately this pattern can't yet work for PolymorphicAliased
        # and PolymorphicUnions, because the subquery does not compile
        # out including the polymorphic selectable; only if Person is in
        # the query() list does that happen.
        eq_(sess.query(Person.name)
                .filter(
                    sess.query(Company.name).
                    filter(Company.company_id == Person.company_id).
                    correlate(Person).as_scalar() == "Elbonia, Inc.").all(),
            [(e3.name, )])

    def test_correlation_two(self):
        sess = create_session()

        paliased = aliased(Person)

        eq_(sess.query(paliased.name)
                .filter(
                    sess.query(Company.name).
                    filter(Company.company_id == paliased.company_id).
                    correlate(paliased).as_scalar() == "Elbonia, Inc.").all(),
            [(e3.name, )])

    def test_correlation_three(self):
        sess = create_session()

        paliased = aliased(Person, flat=True)

        eq_(sess.query(paliased.name)
                .filter(
                    sess.query(Company.name).
                    filter(Company.company_id == paliased.company_id).
                    correlate(paliased).as_scalar() == "Elbonia, Inc.").all(),
            [(e3.name, )])


class PolymorphicTest(_PolymorphicTestBase, _Polymorphic):
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

    def test_correlation_w_polymorphic(self):

        sess = create_session()

        p_poly = with_polymorphic(Person, '*')

        eq_(sess.query(p_poly.name)
                .filter(
                    sess.query(Company.name).
                    filter(Company.company_id == p_poly.company_id).
                    correlate(p_poly).as_scalar() == "Elbonia, Inc.").all(),
            [(e3.name, )])

    def test_correlation_w_polymorphic_flat(self):

        sess = create_session()

        p_poly = with_polymorphic(Person, '*', flat=True)

        eq_(sess.query(p_poly.name)
                .filter(
                    sess.query(Company.name).
                    filter(Company.company_id == p_poly.company_id).
                    correlate(p_poly).as_scalar() == "Elbonia, Inc.").all(),
            [(e3.name, )])

    def test_join_to_subclass_ten(self):
        pass

    def test_mixed_entities_one(self):
        pass

    def test_mixed_entities_two(self):
        pass

    def test_mixed_entities_eight(self):
        pass

    def test_polymorphic_any_eight(self):
        pass


class PolymorphicPolymorphicTest(
        _PolymorphicTestBase, _PolymorphicPolymorphic):
    __dialect__ = 'default'

    def test_aliased_not_polluted_by_join(self):
        # aliased(polymorphic) will normally do the old-school
        # "(SELECT * FROM a JOIN b ...) AS anon_1" thing.
        # this is the safest
        sess = create_session()
        palias = aliased(Person)
        self.assert_compile(
            sess.query(palias, Company.name).order_by(palias.person_id).
            join(Person, Company.employees).filter(palias.name == 'dilbert'),
            "SELECT anon_1.people_person_id AS anon_1_people_person_id, "
            "anon_1.people_company_id AS anon_1_people_company_id, "
            "anon_1.people_name AS anon_1_people_name, "
            "anon_1.people_type AS anon_1_people_type, "
            "anon_1.engineers_person_id AS anon_1_engineers_person_id, "
            "anon_1.engineers_status AS anon_1_engineers_status, "
            "anon_1.engineers_engineer_name AS anon_1_engineers_engineer_name, "  # noqa
            "anon_1.engineers_primary_language AS "
            "anon_1_engineers_primary_language, "
            "anon_1.managers_person_id AS anon_1_managers_person_id, "
            "anon_1.managers_status AS anon_1_managers_status, "
            "anon_1.managers_manager_name AS anon_1_managers_manager_name, "
            "anon_1.boss_boss_id AS anon_1_boss_boss_id, "
            "anon_1.boss_golf_swing AS anon_1_boss_golf_swing, "
            "companies.name AS companies_name "
            "FROM (SELECT people.person_id AS people_person_id, "
            "people.company_id AS people_company_id, "
            "people.name AS people_name, people.type AS people_type, "
            "engineers.person_id AS engineers_person_id, "
            "engineers.status AS engineers_status, "
            "engineers.engineer_name AS engineers_engineer_name, "
            "engineers.primary_language AS engineers_primary_language, "
            "managers.person_id AS managers_person_id, "
            "managers.status AS managers_status, "
            "managers.manager_name AS managers_manager_name, "
            "boss.boss_id AS boss_boss_id, "
            "boss.golf_swing AS boss_golf_swing "
            "FROM people LEFT OUTER JOIN engineers "
            "ON people.person_id = engineers.person_id "
            "LEFT OUTER JOIN managers "
            "ON people.person_id = managers.person_id LEFT OUTER JOIN boss "
            "ON managers.person_id = boss.boss_id) AS anon_1, "
            "companies JOIN "
            "(people LEFT OUTER JOIN engineers "
            "ON people.person_id = engineers.person_id "
            "LEFT OUTER JOIN managers "
            "ON people.person_id = managers.person_id "
            "LEFT OUTER JOIN boss ON managers.person_id = boss.boss_id) "
            "ON companies.company_id = people.company_id "
            "WHERE anon_1.people_name = :people_name_1 "
            "ORDER BY anon_1.people_person_id")

    def test_flat_aliased_w_select_from(self):
        sess = create_session()
        palias = aliased(Person, flat=True)
        self.assert_compile(
            sess.query(palias, Company.name).
            select_from(palias).order_by(palias.person_id).join(
                Person, Company.employees).filter(palias.name == 'dilbert'),
            "SELECT people_1.person_id AS people_1_person_id, "
            "people_1.company_id AS people_1_company_id, "
            "people_1.name AS people_1_name, people_1.type AS people_1_type, "
            "engineers_1.person_id AS engineers_1_person_id, "
            "engineers_1.status AS engineers_1_status, "
            "engineers_1.engineer_name AS engineers_1_engineer_name, "
            "engineers_1.primary_language AS engineers_1_primary_language, "
            "managers_1.person_id AS managers_1_person_id, "
            "managers_1.status AS managers_1_status, "
            "managers_1.manager_name AS managers_1_manager_name, "
            "boss_1.boss_id AS boss_1_boss_id, "
            "boss_1.golf_swing AS boss_1_golf_swing, "
            "companies.name AS companies_name "
            "FROM people AS people_1 "
            "LEFT OUTER JOIN engineers AS engineers_1 "
            "ON people_1.person_id = engineers_1.person_id "
            "LEFT OUTER JOIN managers AS managers_1 "
            "ON people_1.person_id = managers_1.person_id "
            "LEFT OUTER JOIN boss AS boss_1 "
            "ON managers_1.person_id = boss_1.boss_id, "
            "companies JOIN (people LEFT OUTER JOIN engineers "
            "ON people.person_id = engineers.person_id "
            "LEFT OUTER JOIN managers "
            "ON people.person_id = managers.person_id "
            "LEFT OUTER JOIN boss ON managers.person_id = boss.boss_id) "
            "ON companies.company_id = people.company_id "
            "WHERE people_1.name = :name_1 ORDER BY people_1.person_id")


class PolymorphicUnionsTest(_PolymorphicTestBase, _PolymorphicUnions):

    @testing.fails()
    def test_correlation_one(self):
        super(PolymorphicUnionsTest, self).test_correlation_one()


class PolymorphicAliasedJoinsTest(
        _PolymorphicTestBase, _PolymorphicAliasedJoins):
    @testing.fails()
    def test_correlation_one(self):
        super(PolymorphicAliasedJoinsTest, self).test_correlation_one()


class PolymorphicJoinsTest(_PolymorphicTestBase, _PolymorphicJoins):
    pass
