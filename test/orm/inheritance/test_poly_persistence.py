"""tests basic polymorphic mapper loading/saving, minimal relationships"""

from sqlalchemy.testing import eq_, is_, assert_raises, assert_raises_message
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy import exc as sa_exc
from sqlalchemy.testing.schema import Column
from sqlalchemy import testing
from sqlalchemy.testing.util import function_named
from test.orm import _fixtures
from sqlalchemy.testing import fixtures

class Person(fixtures.ComparableEntity):
    pass
class Engineer(Person):
    pass
class Manager(Person):
    pass
class Boss(Manager):
    pass
class Company(fixtures.ComparableEntity):
    pass

class PolymorphTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global companies, people, engineers, managers, boss

        companies = Table('companies', metadata,
           Column('company_id', Integer, primary_key=True,
                                        test_needs_autoincrement=True),
           Column('name', String(50)))

        people = Table('people', metadata,
           Column('person_id', Integer, primary_key=True,
                                        test_needs_autoincrement=True),
           Column('company_id', Integer, ForeignKey('companies.company_id'),
                                        nullable=False),
           Column('name', String(50)),
           Column('type', String(30)))

        engineers = Table('engineers', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'),
                                        primary_key=True),
           Column('status', String(30)),
           Column('engineer_name', String(50)),
           Column('primary_language', String(50)),
          )

        managers = Table('managers', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'),
                                        primary_key=True),
           Column('status', String(30)),
           Column('manager_name', String(50))
           )

        boss = Table('boss', metadata,
            Column('boss_id', Integer, ForeignKey('managers.person_id'),
                                        primary_key=True),
            Column('golf_swing', String(30)),
            )

        metadata.create_all()

class InsertOrderTest(PolymorphTest):
    def test_insert_order(self):
        """test that classes of multiple types mix up mapper inserts
        so that insert order of individual tables is maintained"""

        person_join = polymorphic_union(
            {
                'engineer':people.join(engineers),
                'manager':people.join(managers),
                'person':people.select(people.c.type=='person'),
            }, None, 'pjoin')

        person_mapper = mapper(Person, people,
                                with_polymorphic=('*', person_join),
                                polymorphic_on=person_join.c.type,
                                polymorphic_identity='person')

        mapper(Engineer, engineers, inherits=person_mapper,
                                polymorphic_identity='engineer')
        mapper(Manager, managers, inherits=person_mapper,
                                polymorphic_identity='manager')
        mapper(Company, companies, properties={
            'employees': relationship(Person,
                                  backref='company',
                                  order_by=person_join.c.person_id)
        })

        session = create_session()
        c = Company(name='company1')
        c.employees.append(Manager(status='AAB', manager_name='manager1'
                           , name='pointy haired boss'))
        c.employees.append(Engineer(status='BBA',
                           engineer_name='engineer1',
                           primary_language='java', name='dilbert'))
        c.employees.append(Person(status='HHH', name='joesmith'))
        c.employees.append(Engineer(status='CGG',
                           engineer_name='engineer2',
                           primary_language='python', name='wally'))
        c.employees.append(Manager(status='ABA', manager_name='manager2'
                           , name='jsmith'))
        session.add(c)
        session.flush()
        session.expunge_all()
        eq_(session.query(Company).get(c.company_id), c)

class RoundTripTest(PolymorphTest):
    pass

def _generate_round_trip_test(include_base, lazy_relationship,
                                    redefine_colprop, with_polymorphic):
    """generates a round trip test.

    include_base - whether or not to include the base 'person' type in
    the union.

    lazy_relationship - whether or not the Company relationship to
    People is lazy or eager.

    redefine_colprop - if we redefine the 'name' column to be
    'people_name' on the base Person class

    use_literal_join - primary join condition is explicitly specified
    """
    def test_roundtrip(self):
        if with_polymorphic == 'unions':
            if include_base:
                person_join = polymorphic_union(
                    {
                        'engineer':people.join(engineers),
                        'manager':people.join(managers),
                        'person':people.select(people.c.type=='person'),
                    }, None, 'pjoin')
            else:
                person_join = polymorphic_union(
                    {
                        'engineer':people.join(engineers),
                        'manager':people.join(managers),
                    }, None, 'pjoin')

            manager_join = people.join(managers).outerjoin(boss)
            person_with_polymorphic = ['*', person_join]
            manager_with_polymorphic = ['*', manager_join]
        elif with_polymorphic == 'joins':
            person_join = people.outerjoin(engineers).outerjoin(managers).\
                                        outerjoin(boss)
            manager_join = people.join(managers).outerjoin(boss)
            person_with_polymorphic = ['*', person_join]
            manager_with_polymorphic = ['*', manager_join]
        elif with_polymorphic == 'auto':
            person_with_polymorphic = '*'
            manager_with_polymorphic = '*'
        else:
            person_with_polymorphic = None
            manager_with_polymorphic = None

        if redefine_colprop:
            person_mapper = mapper(Person, people,
                                with_polymorphic=person_with_polymorphic,
                                polymorphic_on=people.c.type,
                                polymorphic_identity='person',
                                properties= {'person_name':people.c.name})
        else:
            person_mapper = mapper(Person, people,
                                with_polymorphic=person_with_polymorphic,
                                polymorphic_on=people.c.type,
                                polymorphic_identity='person')

        mapper(Engineer, engineers, inherits=person_mapper,
                                polymorphic_identity='engineer')
        mapper(Manager, managers, inherits=person_mapper,
                                with_polymorphic=manager_with_polymorphic,
                                polymorphic_identity='manager')

        mapper(Boss, boss, inherits=Manager, polymorphic_identity='boss')

        mapper(Company, companies, properties={
            'employees': relationship(Person, lazy=lazy_relationship,
                                  cascade="all, delete-orphan",
            backref="company", order_by=people.c.person_id
            )
        })

        if redefine_colprop:
            person_attribute_name = 'person_name'
        else:
            person_attribute_name = 'name'

        employees = [
                Manager(status='AAB', manager_name='manager1',
                            **{person_attribute_name:'pointy haired boss'}),
                Engineer(status='BBA', engineer_name='engineer1',
                            primary_language='java',
                            **{person_attribute_name:'dilbert'}),
            ]
        if include_base:
            employees.append(Person(**{person_attribute_name:'joesmith'}))
        employees += [
            Engineer(status='CGG', engineer_name='engineer2',
                            primary_language='python',
                            **{person_attribute_name:'wally'}),
            Manager(status='ABA', manager_name='manager2',
                            **{person_attribute_name:'jsmith'})
        ]

        pointy = employees[0]
        jsmith = employees[-1]
        dilbert = employees[1]

        session = create_session()
        c = Company(name='company1')
        c.employees = employees
        session.add(c)

        session.flush()
        session.expunge_all()

        eq_(session.query(Person).get(dilbert.person_id), dilbert)
        session.expunge_all()

        eq_(session.query(Person).filter(
                            Person.person_id==dilbert.person_id).one(),
                            dilbert)
        session.expunge_all()

        def go():
            cc = session.query(Company).get(c.company_id)
            eq_(cc.employees, employees)

        if not lazy_relationship:
            if with_polymorphic != 'none':
                self.assert_sql_count(testing.db, go, 1)
            else:
                self.assert_sql_count(testing.db, go, 5)

        else:
            if with_polymorphic != 'none':
                self.assert_sql_count(testing.db, go, 2)
            else:
                self.assert_sql_count(testing.db, go, 6)

        # test selecting from the query, using the base
        # mapped table (people) as the selection criterion.
        # in the case of the polymorphic Person query,
        # the "people" selectable should be adapted to be "person_join"
        eq_(
            session.query(Person).filter(
                            getattr(Person, person_attribute_name)=='dilbert'
                            ).first(),
            dilbert
        )

        assert session.query(Person).filter(
                            getattr(Person, person_attribute_name)=='dilbert'
                            ).first().person_id

        eq_(
            session.query(Engineer).filter(
                            getattr(Person, person_attribute_name)=='dilbert'
                            ).first(),
            dilbert
        )

        # test selecting from the query, joining against
        # an alias of the base "people" table.  test that
        # the "palias" alias does *not* get sucked up
        # into the "person_join" conversion.
        palias = people.alias("palias")
        dilbert = session.query(Person).get(dilbert.person_id)
        is_(
            dilbert,
            session.query(Person).filter(
                (palias.c.name == 'dilbert') &
                (palias.c.person_id == Person.person_id)).first()
        )
        is_(
            dilbert,
            session.query(Engineer).filter(
                (palias.c.name == 'dilbert') &
                (palias.c.person_id == Person.person_id)).first()
        )
        is_(
            dilbert,
            session.query(Person).filter(
                (Engineer.engineer_name == "engineer1") &
                (engineers.c.person_id == people.c.person_id)
            ).first()
        )
        is_(
            dilbert,
            session.query(Engineer).
            filter(Engineer.engineer_name == "engineer1")[0]
        )

        session.flush()
        session.expunge_all()

        def go():
            session.query(Person).filter(getattr(Person,
                            person_attribute_name)=='dilbert').first()
        self.assert_sql_count(testing.db, go, 1)
        session.expunge_all()
        dilbert = session.query(Person).filter(getattr(Person,
                            person_attribute_name)=='dilbert').first()
        def go():
            # assert that only primary table is queried for
            # already-present-in-session
            d = session.query(Person).filter(getattr(Person,
                            person_attribute_name)=='dilbert').first()
        self.assert_sql_count(testing.db, go, 1)

        # test standalone orphans
        daboss = Boss(status='BBB',
                        manager_name='boss',
                        golf_swing='fore',
                        **{person_attribute_name:'daboss'})
        session.add(daboss)
        assert_raises(sa_exc.DBAPIError, session.flush)

        c = session.query(Company).first()
        daboss.company = c
        manager_list = [e for e in c.employees
                            if isinstance(e, Manager)]
        session.flush()
        session.expunge_all()

        eq_(session.query(Manager).order_by(Manager.person_id).all(),
                                manager_list)
        c = session.query(Company).first()

        session.delete(c)
        session.flush()

        eq_(people.count().scalar(), 0)

    test_roundtrip = function_named(
        test_roundtrip, "test_%s%s%s_%s" % (
          (lazy_relationship and "lazy" or "eager"),
          (include_base and "_inclbase" or ""),
          (redefine_colprop and "_redefcol" or ""),
          with_polymorphic))
    setattr(RoundTripTest, test_roundtrip.__name__, test_roundtrip)

for lazy_relationship in [True, False]:
    for redefine_colprop in [True, False]:
        for with_polymorphic in ['unions', 'joins', 'auto', 'none']:
            if with_polymorphic == 'unions':
                for include_base in [True, False]:
                    _generate_round_trip_test(include_base,
                                    lazy_relationship,
                                    redefine_colprop, with_polymorphic)
            else:
                _generate_round_trip_test(False,
                                    lazy_relationship,
                                    redefine_colprop, with_polymorphic)

