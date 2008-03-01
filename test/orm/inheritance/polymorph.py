"""tests basic polymorphic mapper loading/saving, minimal relations"""

import testenv; testenv.configure_for_tests()
import sets
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *


class Person(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)
    def get_name(self):
        try:
            return getattr(self, 'person_name')
        except AttributeError:
            return getattr(self, 'name')
    def __repr__(self):
        return "Ordinary person %s" % self.get_name()
class Engineer(Person):
    def __repr__(self):
        return "Engineer %s, status %s, engineer_name %s, primary_language %s" % (self.get_name(), self.status, self.engineer_name, self.primary_language)
class Manager(Person):
    def __repr__(self):
        return "Manager %s, status %s, manager_name %s" % (self.get_name(), self.status, self.manager_name)
class Boss(Manager):
    def __repr__(self):
        return "Boss %s, status %s, manager_name %s golf swing %s" % (self.get_name(), self.status, self.manager_name, self.golf_swing)

class Company(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)
    def __repr__(self):
        return "Company %s" % self.name

class PolymorphTest(ORMTest):
    def define_tables(self, metadata):
        global companies, people, engineers, managers, boss

        # a table to store companies
        companies = Table('companies', metadata,
           Column('company_id', Integer, Sequence('company_id_seq', optional=True), primary_key=True),
           Column('name', String(50)))

        # we will define an inheritance relationship between the table "people" and "engineers",
        # and a second inheritance relationship between the table "people" and "managers"
        people = Table('people', metadata,
           Column('person_id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
           Column('company_id', Integer, ForeignKey('companies.company_id')),
           Column('name', String(50)),
           Column('type', String(30)))

        engineers = Table('engineers', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
           Column('engineer_name', String(50)),
           Column('primary_language', String(50)),
          )

        managers = Table('managers', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
           Column('manager_name', String(50))
           )

        boss = Table('boss', metadata,
            Column('boss_id', Integer, ForeignKey('managers.person_id'), primary_key=True),
            Column('golf_swing', String(30)),
            )

        metadata.create_all()

class CompileTest(PolymorphTest):
    def testcompile(self):
        person_join = polymorphic_union( {
            'engineer':people.join(engineers),
            'manager':people.join(managers),
            'person':people.select(people.c.type=='person'),
            }, None, 'pjoin')

        person_mapper = mapper(Person, people, select_table=person_join, polymorphic_on=person_join.c.type, polymorphic_identity='person')
        mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')

        session = create_session()
        session.save(Manager(name='Tom', status='knows how to manage things'))
        session.save(Engineer(name='Kurt', status='knows how to hack'))
        session.flush()
        print session.query(Engineer).all()

        print session.query(Person).all()

    def testcompile2(self):
        """test that a mapper can reference a property whose mapper inherits from this one."""
        person_join = polymorphic_union( {
            'engineer':people.join(engineers),
            'manager':people.join(managers),
            'person':people.select(people.c.type=='person'),
            }, None, 'pjoin')


        person_mapper = mapper(Person, people, select_table=person_join, polymorphic_on=person_join.c.type,
                    polymorphic_identity='person',
                    properties = dict(managers = relation(Manager, lazy=True))
                )

        mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')

        #person_mapper.compile()
        class_mapper(Manager).compile()

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

        person_mapper = mapper(Person, people, select_table=person_join, polymorphic_on=person_join.c.type, polymorphic_identity='person')

        mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')
        mapper(Company, companies, properties={
            'employees': relation(Person,
                                  cascade="all, delete-orphan",
                                  backref='company',
                                  order_by=person_join.c.person_id)
        })

        session = create_session()
        c = Company(name='company1')
        c.employees.append(Manager(status='AAB', manager_name='manager1', name='pointy haired boss'))
        c.employees.append(Engineer(status='BBA', engineer_name='engineer1', primary_language='java', name='dilbert'))
        c.employees.append(Person(status='HHH', name='joesmith'))
        c.employees.append(Engineer(status='CGG', engineer_name='engineer2', primary_language='python', name='wally'))
        c.employees.append(Manager(status='ABA', manager_name='manager2', name='jsmith'))
        session.save(c)
        session.flush()
        session.clear()
        c = session.query(Company).get(c.company_id)
        for e in c.employees:
            print e, e._instance_key, e.company

        assert [e.get_name() for e in c.employees] == ['pointy haired boss', 'dilbert', 'joesmith', 'wally', 'jsmith']

class RelationToSubclassTest(PolymorphTest):
    def testrelationtosubclass(self):
        """test a relation to an inheriting mapper where the relation is to a subclass
        but the join condition is expressed by the parent table.

        also test that backrefs work in this case.

        this test touches upon a lot of the join/foreign key determination code in properties.py
        and creates the need for properties.py to search for conditions individually within
        the mapper's local table as well as the mapper's 'mapped' table, so that relations
        requiring lots of specificity (like self-referential joins) as well as relations requiring
        more generalization (like the example here) both come up with proper results."""

        mapper(Person, people)

        mapper(Engineer, engineers, inherits=Person)
        mapper(Manager, managers, inherits=Person)

        mapper(Company, companies, properties={
            'managers': relation(Manager, lazy=True,backref="company")
        })

        sess = create_session()

        c = Company(name='company1')
        c.managers.append(Manager(status='AAB', manager_name='manager1', name='pointy haired boss'))
        sess.save(c)
        sess.flush()
        sess.clear()

        sess.query(Company).filter_by(company_id=c.company_id).one()
        assert sets.Set([e.get_name() for e in c.managers]) == sets.Set(['pointy haired boss'])
        assert c.managers[0].company is c

class RoundTripTest(PolymorphTest):
    pass

def generate_round_trip_test(include_base=False, lazy_relation=True, redefine_colprop=False, use_literal_join=False, polymorphic_fetch=None, use_outer_joins=False):
    """generates a round trip test.

    include_base - whether or not to include the base 'person' type in the union.
    lazy_relation - whether or not the Company relation to People is lazy or eager.
    redefine_colprop - if we redefine the 'name' column to be 'people_name' on the base Person class
    use_literal_join - primary join condition is explicitly specified
    """
    def test_roundtrip(self):
        # create a union that represents both types of joins.
        if not polymorphic_fetch == 'union':
            person_join = None
            manager_join = None
        elif include_base:
            if use_outer_joins:
                person_join = people.outerjoin(engineers).outerjoin(managers).outerjoin(boss)
                manager_join = people.join(managers).outerjoin(boss)
            else:
                person_join = polymorphic_union(
                    {
                        'engineer':people.join(engineers),
                        'manager':people.join(managers),
                        'person':people.select(people.c.type=='person'),
                    }, None, 'pjoin')

                manager_join = people.join(managers).outerjoin(boss)
        else:
            if use_outer_joins:
                person_join = people.outerjoin(engineers).outerjoin(managers).outerjoin(boss)
                manager_join = people.join(managers).outerjoin(boss)
            else:
                person_join = polymorphic_union(
                    {
                        'engineer':people.join(engineers),
                        'manager':people.join(managers),
                    }, None, 'pjoin')
                manager_join = people.join(managers).outerjoin(boss)

        if redefine_colprop:
            person_mapper = mapper(Person, people, select_table=person_join, polymorphic_fetch=polymorphic_fetch, polymorphic_on=people.c.type, polymorphic_identity='person', properties= {'person_name':people.c.name})
        else:
            person_mapper = mapper(Person, people, select_table=person_join, polymorphic_fetch=polymorphic_fetch, polymorphic_on=people.c.type, polymorphic_identity='person')

        mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        mapper(Manager, managers, inherits=person_mapper, select_table=manager_join, polymorphic_identity='manager')

        mapper(Boss, boss, inherits=Manager, polymorphic_identity='boss')

        if use_literal_join:
            mapper(Company, companies, properties={
                'employees': relation(Person, lazy=lazy_relation,
                                      primaryjoin=(people.c.company_id ==
                                                   companies.c.company_id),
                                      cascade="all,delete-orphan",
                                      backref="company"
                )
            })
        else:
            mapper(Company, companies, properties={
                'employees': relation(Person, lazy=lazy_relation,
                                      cascade="all, delete-orphan",
                backref="company"
                )
            })

        if redefine_colprop:
            person_attribute_name = 'person_name'
        else:
            person_attribute_name = 'name'

        session = create_session()
        c = Company(name='company1')
        c.employees.append(Manager(status='AAB', manager_name='manager1', **{person_attribute_name:'pointy haired boss'}))
        c.employees.append(Engineer(status='BBA', engineer_name='engineer1', primary_language='java', **{person_attribute_name:'dilbert'}))
        dilbert = c.employees[-1]

        if include_base:
            c.employees.append(Person(status='HHH', **{person_attribute_name:'joesmith'}))
        c.employees.append(Engineer(status='CGG', engineer_name='engineer2', primary_language='python', **{person_attribute_name:'wally'}))
        c.employees.append(Manager(status='ABA', manager_name='manager2', **{person_attribute_name:'jsmith'}))
        session.save(c)

        session.flush()
        session.clear()

        dilbert = session.query(Person).get(dilbert.person_id)
        assert getattr(dilbert, person_attribute_name) == 'dilbert'
        session.clear()

        dilbert = session.query(Person).filter(Person.person_id==dilbert.person_id).one()
        assert getattr(dilbert, person_attribute_name) == 'dilbert'
        session.clear()

        id = c.company_id
        def go():
            c = session.query(Company).get(id)
            for e in c.employees:
                assert e._instance_key[0] == Person
            if include_base:
                assert sets.Set([(e.get_name(), getattr(e, 'status', None)) for e in c.employees]) == sets.Set([('pointy haired boss', 'AAB'), ('dilbert', 'BBA'), ('joesmith', None), ('wally', 'CGG'), ('jsmith', 'ABA')])
            else:
                assert sets.Set([(e.get_name(), e.status) for e in c.employees]) == sets.Set([('pointy haired boss', 'AAB'), ('dilbert', 'BBA'), ('wally', 'CGG'), ('jsmith', 'ABA')])
            print "\n"

        if not lazy_relation:
            if polymorphic_fetch=='union':
                self.assert_sql_count(testing.db, go, 1)
            else:
                self.assert_sql_count(testing.db, go, 5)

        else:
            if polymorphic_fetch=='union':
                self.assert_sql_count(testing.db, go, 2)
            else:
                self.assert_sql_count(testing.db, go, 6)

        # test selecting from the query, using the base mapped table (people) as the selection criterion.
        # in the case of the polymorphic Person query, the "people" selectable should be adapted to be "person_join"
        dilbert = session.query(Person).filter(getattr(Person, person_attribute_name)=='dilbert').first()
        assert dilbert is session.query(Engineer).filter(getattr(Person, person_attribute_name)=='dilbert').first()

        # test selecting from the query, joining against an alias of the base "people" table.  test that
        # the "palias" alias does *not* get sucked up into the "person_join" conversion.
        palias = people.alias("palias")
        assert dilbert is session.query(Person).filter((palias.c.name=='dilbert') & (palias.c.person_id==Person.person_id)).first()
        assert dilbert is session.query(Engineer).filter((palias.c.name=='dilbert') & (palias.c.person_id==Person.person_id)).first()
        assert dilbert is session.query(Person).filter((Engineer.engineer_name=="engineer1") & (engineers.c.person_id==people.c.person_id)).first()
        assert dilbert is session.query(Engineer).filter(Engineer.engineer_name=="engineer1")[0]
        
        dilbert.engineer_name = 'hes dibert!'

        session.flush()
        session.clear()
        
        if polymorphic_fetch == 'select':
            def go():
                session.query(Person).filter(getattr(Person, person_attribute_name)=='dilbert').first()
            self.assert_sql_count(testing.db, go, 2)
            session.clear()
            dilbert = session.query(Person).filter(getattr(Person, person_attribute_name)=='dilbert').first()
            def go():
                # assert that only primary table is queried for already-present-in-session
                d = session.query(Person).filter(getattr(Person, person_attribute_name)=='dilbert').first()
            self.assert_sql_count(testing.db, go, 1)

        # save/load some managers/bosses
        b = Boss(status='BBB', manager_name='boss', golf_swing='fore', **{person_attribute_name:'daboss'})
        session.save(b)
        session.flush()
        session.clear()
        c = session.query(Manager).all()
        assert sets.Set([repr(x) for x in c]) == sets.Set(["Manager pointy haired boss, status AAB, manager_name manager1", "Manager jsmith, status ABA, manager_name manager2", "Boss daboss, status BBB, manager_name boss golf swing fore"]), repr([repr(x) for x in c])

        c = session.query(Company).get(id)
        for e in c.employees:
            print e, e._instance_key

        session.delete(c)
        session.flush()

    test_roundtrip = _function_named(
        test_roundtrip, "test_%s%s%s%s%s" % (
          (lazy_relation and "lazy" or "eager"),
          (include_base and "_inclbase" or ""),
          (redefine_colprop and "_redefcol" or ""),
          (polymorphic_fetch != 'union' and '_' + polymorphic_fetch or (use_literal_join and "_litjoin" or "")),
          (use_outer_joins and '_outerjoins' or '')))
    setattr(RoundTripTest, test_roundtrip.__name__, test_roundtrip)

for include_base in [True, False]:
    for lazy_relation in [True, False]:
        for redefine_colprop in [True, False]:
            for use_literal_join in [True, False]:
                for polymorphic_fetch in ['union', 'select', 'deferred']:
                    if polymorphic_fetch == 'union':
                        for use_outer_joins in [True, False]:
                            generate_round_trip_test(include_base, lazy_relation, redefine_colprop, use_literal_join, polymorphic_fetch, use_outer_joins)
                    else:
                        generate_round_trip_test(include_base, lazy_relation, redefine_colprop, use_literal_join, polymorphic_fetch, False)

if __name__ == "__main__":
    testenv.main()
