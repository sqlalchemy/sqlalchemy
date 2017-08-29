"""Miscellaneous inheritance-related tests, many very old.
These are generally tests derived from specific user issues.

"""

from sqlalchemy.testing import eq_
from sqlalchemy import *
from sqlalchemy import util
from sqlalchemy.orm import *
from sqlalchemy.orm.interfaces import MANYTOONE
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy import testing
from sqlalchemy.testing.util import function_named
from sqlalchemy.testing import fixtures
from test.orm import _fixtures
from sqlalchemy.testing import eq_
from sqlalchemy.testing.schema import Table, Column


class AttrSettable(object):
    def __init__(self, **kwargs):
        [setattr(self, k, v) for k, v in kwargs.items()]

    def __repr__(self):
        return self.__class__.__name__ + "(%s)" % (hex(id(self)))


class RelationshipTest1(fixtures.MappedTest):
    """test self-referential relationships on polymorphic mappers"""
    @classmethod
    def define_tables(cls, metadata):
        global people, managers

        people = Table('people', metadata,
                       Column('person_id', Integer, Sequence('person_id_seq',
                                                             optional=True),
                              primary_key=True),
                       Column('manager_id', Integer,
                              ForeignKey('managers.person_id',
                                         use_alter=True, name="mpid_fq")),
                       Column('name', String(50)),
                       Column('type', String(30)))

        managers = Table('managers', metadata,
                         Column('person_id', Integer,
                                ForeignKey('people.person_id'),
                                primary_key=True),
                         Column('status', String(30)),
                         Column('manager_name', String(50))
                         )

    def teardown(self):
        people.update(values={people.c.manager_id: None}).execute()
        super(RelationshipTest1, self).teardown()

    def test_parent_refs_descendant(self):
        class Person(AttrSettable):
            pass

        class Manager(Person):
            pass

        mapper(Person, people, properties={
            'manager': relationship(Manager, primaryjoin=(
                people.c.manager_id ==
                managers.c.person_id),
                uselist=False, post_update=True)
        })
        mapper(Manager, managers, inherits=Person,
               inherit_condition=people.c.person_id == managers.c.person_id)

        eq_(class_mapper(Person).get_property('manager').synchronize_pairs,
            [(managers.c.person_id, people.c.manager_id)])

        session = create_session()
        p = Person(name='some person')
        m = Manager(name='some manager')
        p.manager = m
        session.add(p)
        session.flush()
        session.expunge_all()

        p = session.query(Person).get(p.person_id)
        m = session.query(Manager).get(m.person_id)
        assert p.manager is m

    def test_descendant_refs_parent(self):
        class Person(AttrSettable):
            pass

        class Manager(Person):
            pass

        mapper(Person, people)
        mapper(Manager, managers, inherits=Person,
               inherit_condition=people.c.person_id ==
               managers.c.person_id,
               properties={
                   'employee': relationship(Person, primaryjoin=(
                       people.c.manager_id ==
                       managers.c.person_id),
                       foreign_keys=[people.c.manager_id],
                       uselist=False, post_update=True)})

        session = create_session()
        p = Person(name='some person')
        m = Manager(name='some manager')
        m.employee = p
        session.add(m)
        session.flush()
        session.expunge_all()

        p = session.query(Person).get(p.person_id)
        m = session.query(Manager).get(m.person_id)
        assert m.employee is p


class RelationshipTest2(fixtures.MappedTest):
    """test self-referential relationships on polymorphic mappers"""
    @classmethod
    def define_tables(cls, metadata):
        global people, managers, data
        people = Table('people', metadata,
                       Column('person_id', Integer, primary_key=True,
                              test_needs_autoincrement=True),
                       Column('name', String(50)),
                       Column('type', String(30)))

        managers = Table('managers', metadata,
                         Column('person_id', Integer,
                                ForeignKey('people.person_id'),
                                primary_key=True),
                         Column('manager_id', Integer,
                                ForeignKey('people.person_id')),
                         Column('status', String(30)))

        data = Table('data', metadata,
                     Column('person_id', Integer,
                            ForeignKey('managers.person_id'),
                            primary_key=True),
                     Column('data', String(30)))

    def test_relationshiponsubclass_j1_nodata(self):
        self._do_test("join1", False)

    def test_relationshiponsubclass_j2_nodata(self):
        self._do_test("join2", False)

    def test_relationshiponsubclass_j1_data(self):
        self._do_test("join1", True)

    def test_relationshiponsubclass_j2_data(self):
        self._do_test("join2", True)

    def test_relationshiponsubclass_j3_nodata(self):
        self._do_test("join3", False)

    def test_relationshiponsubclass_j3_data(self):
        self._do_test("join3", True)

    def _do_test(self, jointype="join1", usedata=False):
        class Person(AttrSettable):
            pass

        class Manager(Person):
            pass

        if jointype == "join1":
            poly_union = polymorphic_union({
                'person': people.select(people.c.type == 'person'),
                'manager': join(people, managers,
                                people.c.person_id == managers.c.person_id)
            }, None)
            polymorphic_on = poly_union.c.type
        elif jointype == "join2":
            poly_union = polymorphic_union({
                'person': people.select(people.c.type == 'person'),
                'manager': managers.join(
                    people,
                    people.c.person_id == managers.c.person_id)
            }, None)
            polymorphic_on = poly_union.c.type
        elif jointype == "join3":
            poly_union = None
            polymorphic_on = people.c.type

        if usedata:
            class Data(object):
                def __init__(self, data):
                    self.data = data
            mapper(Data, data)

        mapper(Person, people,
               with_polymorphic=('*', poly_union),
               polymorphic_identity='person',
               polymorphic_on=polymorphic_on)

        if usedata:
            mapper(Manager, managers,
                   inherits=Person,
                   inherit_condition=people.c.person_id ==
                   managers.c.person_id,
                   polymorphic_identity='manager',
                   properties={
                       'colleague': relationship(
                           Person,
                           primaryjoin=managers.c.manager_id ==
                           people.c.person_id,
                           lazy='select', uselist=False),
                       'data': relationship(Data, uselist=False)})
        else:
            mapper(Manager, managers, inherits=Person,
                   inherit_condition=people.c.person_id ==
                   managers.c.person_id,
                   polymorphic_identity='manager',
                   properties={
                       'colleague': relationship(
                           Person,
                           primaryjoin=managers.c.manager_id ==
                           people.c.person_id,
                           lazy='select', uselist=False)})

        sess = create_session()
        p = Person(name='person1')
        m = Manager(name='manager1')
        m.colleague = p
        if usedata:
            m.data = Data('ms data')
        sess.add(m)
        sess.flush()

        sess.expunge_all()
        p = sess.query(Person).get(p.person_id)
        m = sess.query(Manager).get(m.person_id)
        assert m.colleague is p
        if usedata:
            assert m.data.data == 'ms data'


class RelationshipTest3(fixtures.MappedTest):
    """test self-referential relationships on polymorphic mappers"""
    @classmethod
    def define_tables(cls, metadata):
        global people, managers, data
        people = Table('people', metadata,
                       Column('person_id', Integer, primary_key=True,
                              test_needs_autoincrement=True),
                       Column('colleague_id', Integer,
                              ForeignKey('people.person_id')),
                       Column('name', String(50)),
                       Column('type', String(30)))

        managers = Table('managers', metadata,
                         Column('person_id', Integer,
                                ForeignKey('people.person_id'),
                                primary_key=True),
                         Column('status', String(30)))

        data = Table('data', metadata,
                     Column('person_id', Integer,
                            ForeignKey('people.person_id'),
                            primary_key=True),
                     Column('data', String(30)))


def _generate_test(jointype="join1", usedata=False):
    def _do_test(self):
        class Person(AttrSettable):
            pass

        class Manager(Person):
            pass

        if usedata:
            class Data(object):
                def __init__(self, data):
                    self.data = data

        if jointype == "join1":
            poly_union = polymorphic_union({
                'manager': managers.join(
                    people,
                    people.c.person_id == managers.c.person_id),
                'person': people.select(people.c.type == 'person')
            }, None)
        elif jointype == "join2":
            poly_union = polymorphic_union({
                'manager': join(people, managers,
                                people.c.person_id == managers.c.person_id),
                'person': people.select(people.c.type == 'person')
            }, None)
        elif jointype == 'join3':
            poly_union = people.outerjoin(managers)
        elif jointype == "join4":
            poly_union = None

        if usedata:
            mapper(Data, data)

        if usedata:
            mapper(Person, people,
                   with_polymorphic=('*', poly_union),
                   polymorphic_identity='person',
                   polymorphic_on=people.c.type,
                   properties={
                       'colleagues': relationship(
                           Person,
                           primaryjoin=people.c.colleague_id ==
                           people.c.person_id,
                           remote_side=people.c.colleague_id,
                           uselist=True),
                       'data': relationship(Data, uselist=False)})
        else:
            mapper(Person, people,
                   with_polymorphic=('*', poly_union),
                   polymorphic_identity='person',
                   polymorphic_on=people.c.type,
                   properties={
                       'colleagues': relationship(
                           Person,
                           primaryjoin=people.c.colleague_id ==
                           people.c.person_id,
                           remote_side=people.c.colleague_id, uselist=True)})

        mapper(Manager, managers, inherits=Person,
               inherit_condition=people.c.person_id ==
               managers.c.person_id,
               polymorphic_identity='manager')

        sess = create_session()
        p = Person(name='person1')
        p2 = Person(name='person2')
        p3 = Person(name='person3')
        m = Manager(name='manager1')
        p.colleagues.append(p2)
        m.colleagues.append(p3)
        if usedata:
            p.data = Data('ps data')
            m.data = Data('ms data')

        sess.add(m)
        sess.add(p)
        sess.flush()

        sess.expunge_all()
        p = sess.query(Person).get(p.person_id)
        p2 = sess.query(Person).get(p2.person_id)
        p3 = sess.query(Person).get(p3.person_id)
        m = sess.query(Person).get(m.person_id)
        assert len(p.colleagues) == 1
        assert p.colleagues == [p2]
        assert m.colleagues == [p3]
        if usedata:
            assert p.data.data == 'ps data'
            assert m.data.data == 'ms data'

    do_test = function_named(
        _do_test, 'test_relationship_on_base_class_%s_%s' % (
            jointype, data and "nodata" or "data"))
    return do_test


for jointype in ["join1", "join2", "join3", "join4"]:
    for data in (True, False):
        func = _generate_test(jointype, data)
        setattr(RelationshipTest3, func.__name__, func)
del func


class RelationshipTest4(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global people, engineers, managers, cars
        people = Table('people', metadata,
                       Column('person_id', Integer, primary_key=True,
                              test_needs_autoincrement=True),
                       Column('name', String(50)))

        engineers = Table('engineers', metadata,
                          Column('person_id', Integer,
                                 ForeignKey('people.person_id'),
                                 primary_key=True),
                          Column('status', String(30)))

        managers = Table('managers', metadata,
                         Column('person_id', Integer,
                                ForeignKey('people.person_id'),
                                primary_key=True),
                         Column('longer_status', String(70)))

        cars = Table('cars', metadata,
                     Column('car_id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
                     Column('owner', Integer, ForeignKey('people.person_id')))

    def test_many_to_one_polymorphic(self):
        """in this test, the polymorphic union is between two subclasses, but
        does not include the base table by itself in the union. however, the
        primaryjoin condition is going to be against the base table, and its a
        many-to-one relationship (unlike the test in polymorph.py) so the
        column in the base table is explicit. Can the ClauseAdapter figure out
        how to alias the primaryjoin to the polymorphic union ?"""

        # class definitions
        class Person(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

            def __repr__(self):
                return "Ordinary person %s" % self.name

        class Engineer(Person):
            def __repr__(self):
                return "Engineer %s, status %s" % \
                    (self.name, self.status)

        class Manager(Person):
            def __repr__(self):
                return "Manager %s, status %s" % \
                    (self.name, self.longer_status)

        class Car(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

            def __repr__(self):
                return "Car number %d" % self.car_id

        # create a union that represents both types of joins.
        employee_join = polymorphic_union(
            {
                'engineer': people.join(engineers),
                'manager': people.join(managers),
            }, "type", 'employee_join')

        person_mapper = mapper(Person, people,
                               with_polymorphic=('*', employee_join),
                               polymorphic_on=employee_join.c.type,
                               polymorphic_identity='person')
        engineer_mapper = mapper(Engineer, engineers,
                                 inherits=person_mapper,
                                 polymorphic_identity='engineer')
        manager_mapper = mapper(Manager, managers,
                                inherits=person_mapper,
                                polymorphic_identity='manager')
        car_mapper = mapper(Car, cars,
                            properties={'employee':
                                        relationship(person_mapper)})

        session = create_session()

        # creating 5 managers named from M1 to E5
        for i in range(1, 5):
            session.add(Manager(name="M%d" % i,
                                longer_status="YYYYYYYYY"))
        # creating 5 engineers named from E1 to E5
        for i in range(1, 5):
            session.add(Engineer(name="E%d" % i, status="X"))

        session.flush()

        engineer4 = session.query(Engineer).\
            filter(Engineer.name == "E4").first()
        manager3 = session.query(Manager).\
            filter(Manager.name == "M3").first()

        car1 = Car(employee=engineer4)
        session.add(car1)
        car2 = Car(employee=manager3)
        session.add(car2)
        session.flush()

        session.expunge_all()

        def go():
            testcar = session.query(Car).options(
                joinedload('employee')
            ).get(car1.car_id)
            assert str(testcar.employee) == "Engineer E4, status X"
        self.assert_sql_count(testing.db, go, 1)

        car1 = session.query(Car).get(car1.car_id)
        usingGet = session.query(person_mapper).get(car1.owner)
        usingProperty = car1.employee

        assert str(engineer4) == "Engineer E4, status X"
        assert str(usingGet) == "Engineer E4, status X"
        assert str(usingProperty) == "Engineer E4, status X"

        session.expunge_all()
        # and now for the lightning round, eager !

        def go():
            testcar = session.query(Car).options(
                joinedload('employee')
            ).get(car1.car_id)
            assert str(testcar.employee) == "Engineer E4, status X"
        self.assert_sql_count(testing.db, go, 1)

        session.expunge_all()
        s = session.query(Car)
        c = s.join("employee").filter(Person.name == "E4")[0]
        assert c.car_id == car1.car_id


class RelationshipTest5(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global people, engineers, managers, cars
        people = Table('people', metadata,
                       Column('person_id', Integer, primary_key=True,
                              test_needs_autoincrement=True),
                       Column('name', String(50)),
                       Column('type', String(50)))

        engineers = Table('engineers', metadata,
                          Column('person_id', Integer,
                                 ForeignKey('people.person_id'),
                                 primary_key=True),
                          Column('status', String(30)))

        managers = Table('managers', metadata,
                         Column('person_id', Integer,
                                ForeignKey('people.person_id'),
                                primary_key=True),
                         Column('longer_status', String(70)))

        cars = Table('cars', metadata,
                     Column('car_id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
                     Column('owner', Integer, ForeignKey('people.person_id')))

    def test_eager_empty(self):
        """test parent object with child relationship to an inheriting mapper,
        using eager loads, works when there are no child objects present"""

        class Person(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

            def __repr__(self):
                return "Ordinary person %s" % self.name

        class Engineer(Person):
            def __repr__(self):
                return "Engineer %s, status %s" % \
                    (self.name, self.status)

        class Manager(Person):
            def __repr__(self):
                return "Manager %s, status %s" % \
                    (self.name, self.longer_status)

        class Car(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

            def __repr__(self):
                return "Car number %d" % self.car_id

        person_mapper = mapper(Person, people,
                               polymorphic_on=people.c.type,
                               polymorphic_identity='person')
        engineer_mapper = mapper(Engineer, engineers,
                                 inherits=person_mapper,
                                 polymorphic_identity='engineer')
        manager_mapper = mapper(Manager, managers,
                                inherits=person_mapper,
                                polymorphic_identity='manager')
        car_mapper = mapper(Car, cars, properties={
            'manager': relationship(
                manager_mapper, lazy='joined')})

        sess = create_session()
        car1 = Car()
        car2 = Car()
        car2.manager = Manager()
        sess.add(car1)
        sess.add(car2)
        sess.flush()
        sess.expunge_all()

        carlist = sess.query(Car).all()
        assert carlist[0].manager is None
        assert carlist[1].manager.person_id == car2.manager.person_id


class RelationshipTest6(fixtures.MappedTest):
    """test self-referential relationships on a single joined-table
    inheritance mapper"""

    @classmethod
    def define_tables(cls, metadata):
        global people, managers, data
        people = Table('people', metadata,
                       Column('person_id', Integer, primary_key=True,
                              test_needs_autoincrement=True),
                       Column('name', String(50)),
                       )

        managers = Table('managers', metadata,
                         Column('person_id', Integer,
                                ForeignKey('people.person_id'),
                                primary_key=True),
                         Column('colleague_id', Integer,
                                ForeignKey('managers.person_id')),
                         Column('status', String(30)),
                         )

    def test_basic(self):
        class Person(AttrSettable):
            pass

        class Manager(Person):
            pass

        mapper(Person, people)

        mapper(Manager, managers, inherits=Person,
               inherit_condition=people.c.person_id ==
               managers.c.person_id,
               properties={
                   'colleague': relationship(
                       Manager,
                       primaryjoin=managers.c.colleague_id ==
                       managers.c.person_id,
                       lazy='select', uselist=False)})

        sess = create_session()
        m = Manager(name='manager1')
        m2 = Manager(name='manager2')
        m.colleague = m2
        sess.add(m)
        sess.flush()

        sess.expunge_all()
        m = sess.query(Manager).get(m.person_id)
        m2 = sess.query(Manager).get(m2.person_id)
        assert m.colleague is m2


class RelationshipTest7(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global people, engineers, managers, cars, offroad_cars
        cars = Table('cars', metadata,
                     Column('car_id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
                     Column('name', String(30)))

        offroad_cars = Table('offroad_cars', metadata,
                             Column('car_id', Integer,
                                    ForeignKey('cars.car_id'),
                                    nullable=False, primary_key=True))

        people = Table('people', metadata,
                       Column('person_id', Integer, primary_key=True,
                              test_needs_autoincrement=True),
                       Column('car_id', Integer, ForeignKey('cars.car_id'),
                              nullable=False),
                       Column('name', String(50)))

        engineers = Table('engineers', metadata,
                          Column('person_id', Integer,
                                 ForeignKey('people.person_id'),
                                 primary_key=True),
                          Column('field', String(30)))

        managers = Table('managers', metadata,
                         Column('person_id', Integer,
                                ForeignKey('people.person_id'),
                                primary_key=True),
                         Column('category', String(70)))

    def test_manytoone_lazyload(self):
        """test that lazy load clause to a polymorphic child mapper generates
        correctly [ticket:493]"""

        class PersistentObject(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

        class Status(PersistentObject):
            def __repr__(self):
                return "Status %s" % self.name

        class Person(PersistentObject):
            def __repr__(self):
                return "Ordinary person %s" % self.name

        class Engineer(Person):
            def __repr__(self):
                return "Engineer %s, field %s" % (self.name,
                                                  self.field)

        class Manager(Person):
            def __repr__(self):
                return "Manager %s, category %s" % (self.name,
                                                    self.category)

        class Car(PersistentObject):
            def __repr__(self):
                return "Car number %d, name %s" % \
                    (self.car_id, self.name)

        class Offraod_Car(Car):
            def __repr__(self):
                return "Offroad Car number %d, name %s" % \
                    (self.car_id, self.name)

        employee_join = polymorphic_union(
            {
                'engineer': people.join(engineers),
                'manager': people.join(managers),
            }, "type", 'employee_join')

        car_join = polymorphic_union(
            {
                'car': cars.outerjoin(offroad_cars).
                select(offroad_cars.c.car_id == None).reduce_columns(),  # noqa
                'offroad': cars.join(offroad_cars)
            }, "type", 'car_join')

        car_mapper = mapper(Car, cars,
                            with_polymorphic=('*', car_join),
                            polymorphic_on=car_join.c.type,
                            polymorphic_identity='car')
        offroad_car_mapper = mapper(Offraod_Car, offroad_cars,
                                    inherits=car_mapper,
                                    polymorphic_identity='offroad')
        person_mapper = mapper(Person, people,
                               with_polymorphic=('*', employee_join),
                               polymorphic_on=employee_join.c.type,
                               polymorphic_identity='person',
                               properties={
                                   'car': relationship(car_mapper)})
        engineer_mapper = mapper(Engineer, engineers,
                                 inherits=person_mapper,
                                 polymorphic_identity='engineer')
        manager_mapper = mapper(Manager, managers,
                                inherits=person_mapper,
                                polymorphic_identity='manager')

        session = create_session()
        basic_car = Car(name="basic")
        offroad_car = Offraod_Car(name="offroad")

        for i in range(1, 4):
            if i % 2:
                car = Car()
            else:
                car = Offraod_Car()
            session.add(Manager(name="M%d" % i,
                                category="YYYYYYYYY", car=car))
            session.add(Engineer(name="E%d" % i, field="X", car=car))
            session.flush()
            session.expunge_all()

        r = session.query(Person).all()
        for p in r:
            assert p.car_id == p.car.car_id


class RelationshipTest8(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global taggable, users
        taggable = Table('taggable', metadata,
                         Column('id', Integer, primary_key=True,
                                test_needs_autoincrement=True),
                         Column('type', String(30)),
                         Column('owner_id', Integer,
                                ForeignKey('taggable.id')),
                         )
        users = Table('users', metadata,
                      Column('id', Integer, ForeignKey('taggable.id'),
                             primary_key=True),
                      Column('data', String(50)))

    def test_selfref_onjoined(self):
        class Taggable(fixtures.ComparableEntity):
            pass

        class User(Taggable):
            pass

        mapper(Taggable, taggable,
               polymorphic_on=taggable.c.type,
               polymorphic_identity='taggable',
               properties={
                   'owner': relationship(
                       User,
                       primaryjoin=taggable.c.owner_id == taggable.c.id,
                       remote_side=taggable.c.id)})

        mapper(User, users, inherits=Taggable,
               polymorphic_identity='user',
               inherit_condition=users.c.id == taggable.c.id)

        u1 = User(data='u1')
        t1 = Taggable(owner=u1)
        sess = create_session()
        sess.add(t1)
        sess.flush()

        sess.expunge_all()
        eq_(
            sess.query(Taggable).order_by(Taggable.id).all(),
            [User(data='u1'), Taggable(owner=User(data='u1'))]
        )


class GenerativeTest(fixtures.TestBase, AssertsExecutionResults):
    @classmethod
    def setup_class(cls):
        #  cars---owned by---  people (abstract) --- has a --- status
        #   |                  ^    ^                            |
        #   |                  |    |                            |
        #   |          engineers    managers                     |
        #   |                                                    |
        #   +--------------------------------------- has a ------+

        global metadata, status, people, engineers, managers, cars
        metadata = MetaData(testing.db)
        # table definitions
        status = Table('status', metadata,
                       Column('status_id', Integer, primary_key=True,
                              test_needs_autoincrement=True),
                       Column('name', String(20)))

        people = Table(
            'people', metadata,
            Column(
                'person_id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column(
                'status_id', Integer, ForeignKey('status.status_id'),
                nullable=False),
            Column('name', String(50)))

        engineers = Table(
            'engineers', metadata,
            Column(
                'person_id', Integer, ForeignKey('people.person_id'),
                primary_key=True),
            Column('field', String(30)))

        managers = Table(
            'managers', metadata,
            Column(
                'person_id', Integer, ForeignKey('people.person_id'),
                primary_key=True),
            Column('category', String(70)))

        cars = Table(
            'cars', metadata,
            Column(
                'car_id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column(
                'status_id', Integer, ForeignKey('status.status_id'),
                nullable=False),
            Column(
                'owner', Integer, ForeignKey('people.person_id'),
                nullable=False))

        metadata.create_all()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def teardown(self):
        clear_mappers()
        for t in reversed(metadata.sorted_tables):
            t.delete().execute()

    def test_join_to(self):
        # class definitions
        class PersistentObject(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

        class Status(PersistentObject):
            def __repr__(self):
                return "Status %s" % self.name

        class Person(PersistentObject):
            def __repr__(self):
                return "Ordinary person %s" % self.name

        class Engineer(Person):
            def __repr__(self):
                return "Engineer %s, field %s, status %s" % (
                    self.name, self.field, self.status)

        class Manager(Person):
            def __repr__(self):
                return "Manager %s, category %s, status %s" % (
                    self.name, self.category, self.status)

        class Car(PersistentObject):
            def __repr__(self):
                return "Car number %d" % self.car_id

        # create a union that represents both types of joins.
        employee_join = polymorphic_union(
            {
                'engineer': people.join(engineers),
                'manager': people.join(managers),
            }, "type", 'employee_join')

        status_mapper = mapper(Status, status)
        person_mapper = mapper(
            Person, people, with_polymorphic=('*', employee_join),
            polymorphic_on=employee_join.c.type, polymorphic_identity='person',
            properties={'status': relationship(status_mapper)})
        engineer_mapper = mapper(Engineer, engineers,
                                 inherits=person_mapper,
                                 polymorphic_identity='engineer')
        manager_mapper = mapper(Manager, managers,
                                inherits=person_mapper,
                                polymorphic_identity='manager')
        car_mapper = mapper(Car, cars, properties={
            'employee': relationship(person_mapper),
            'status': relationship(status_mapper)})

        session = create_session()

        active = Status(name="active")
        dead = Status(name="dead")

        session.add(active)
        session.add(dead)
        session.flush()

        # TODO: we haven't created assertions for all
        # the data combinations created here

        # creating 5 managers named from M1 to M5
        # and 5 engineers named from E1 to E5
        # M4, M5, E4 and E5 are dead
        for i in range(1, 5):
            if i < 4:
                st = active
            else:
                st = dead
            session.add(Manager(name="M%d" % i,
                                category="YYYYYYYYY", status=st))
            session.add(Engineer(name="E%d" % i, field="X", status=st))

        session.flush()

        # get E4
        engineer4 = session.query(engineer_mapper).filter_by(name="E4").one()

        # create 2 cars for E4, one active and one dead
        car1 = Car(employee=engineer4, status=active)
        car2 = Car(employee=engineer4, status=dead)
        session.add(car1)
        session.add(car2)
        session.flush()

        # this particular adapt used to cause a recursion overflow;
        # added here for testing
        e = exists([Car.owner], Car.owner == employee_join.c.person_id)
        Query(Person)._adapt_clause(employee_join, False, False)

        r = session.query(Person).filter(Person.name.like('%2')).\
            join('status').\
            filter_by(name="active").\
            order_by(Person.person_id)
        eq_(str(list(r)), "[Manager M2, category YYYYYYYYY, status "
            "Status active, Engineer E2, field X, "
            "status Status active]")
        r = session.query(Engineer).join('status').\
            filter(Person.name.in_(
                ['E2', 'E3', 'E4', 'M4', 'M2', 'M1']) &
            (status.c.name == "active")).order_by(Person.name)
        eq_(str(list(r)), "[Engineer E2, field X, status Status "
            "active, Engineer E3, field X, status "
            "Status active]")

        r = session.query(Person).filter(exists([1],
                                                Car.owner == Person.person_id))
        eq_(str(list(r)), "[Engineer E4, field X, status Status dead]")


class MultiLevelTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global table_Employee, table_Engineer, table_Manager
        table_Employee = Table('Employee', metadata,
                               Column('name', type_=String(100), ),
                               Column('id', primary_key=True, type_=Integer,
                                      test_needs_autoincrement=True),
                               Column('atype', type_=String(100), ),
                               )

        table_Engineer = Table(
            'Engineer', metadata, Column('machine', type_=String(100),),
            Column(
                'id', Integer, ForeignKey('Employee.id',),
                primary_key=True),)

        table_Manager = Table('Manager', metadata,
                              Column('duties', type_=String(100),),
                              Column('id', Integer, ForeignKey('Engineer.id'),
                                     primary_key=True))

    def test_threelevels(self):
        class Employee(object):
            def set(me, **kargs):
                for k, v in kargs.items():
                    setattr(me, k, v)
                return me

            def __str__(me):
                return str(me.__class__.__name__) + ':' + str(me.name)
            __repr__ = __str__

        class Engineer(Employee):
            pass

        class Manager(Engineer):
            pass

        pu_Employee = polymorphic_union({
            'Manager':  table_Employee.join(
                table_Engineer).join(table_Manager),
            'Engineer': select([table_Employee,
                                table_Engineer.c.machine],
                               table_Employee.c.atype == 'Engineer',
                               from_obj=[
                table_Employee.join(table_Engineer)]),
            'Employee': table_Employee.select(
                table_Employee.c.atype == 'Employee')
        }, None, 'pu_employee')

        mapper_Employee = mapper(Employee, table_Employee,
                                 polymorphic_identity='Employee',
                                 polymorphic_on=pu_Employee.c.atype,
                                 with_polymorphic=('*', pu_Employee),
                                 )

        pu_Engineer = polymorphic_union({
            'Manager':  table_Employee.join(table_Engineer).
            join(table_Manager),
            'Engineer': select([table_Employee,
                                table_Engineer.c.machine],
                               table_Employee.c.atype == 'Engineer',
                               from_obj=[
                table_Employee.join(table_Engineer)])
        }, None, 'pu_engineer')
        mapper_Engineer = mapper(Engineer, table_Engineer,
                                 inherit_condition=table_Engineer.c.id ==
                                 table_Employee.c.id,
                                 inherits=mapper_Employee,
                                 polymorphic_identity='Engineer',
                                 polymorphic_on=pu_Engineer.c.atype,
                                 with_polymorphic=('*', pu_Engineer))

        mapper_Manager = mapper(Manager, table_Manager,
                                inherit_condition=table_Manager.c.id ==
                                table_Engineer.c.id,
                                inherits=mapper_Engineer,
                                polymorphic_identity='Manager')

        a = Employee().set(name='one')
        b = Engineer().set(egn='two', machine='any')
        c = Manager().set(name='head', machine='fast', duties='many')

        session = create_session()
        session.add(a)
        session.add(b)
        session.add(c)
        session.flush()
        assert set(session.query(Employee).all()) == set([a, b, c])
        assert set(session.query(Engineer).all()) == set([b, c])
        assert session.query(Manager).all() == [c]


class ManyToManyPolyTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global base_item_table, item_table, base_item_collection_table, \
            collection_table
        base_item_table = Table(
            'base_item', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('child_name', String(255), default=None))

        item_table = Table(
            'item', metadata,
            Column('id', Integer, ForeignKey('base_item.id'),
                   primary_key=True),
            Column('dummy', Integer, default=0))

        base_item_collection_table = Table(
            'base_item_collection', metadata,
            Column('item_id', Integer, ForeignKey('base_item.id')),
            Column('collection_id', Integer, ForeignKey('collection.id')))

        collection_table = Table(
            'collection', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('name', Unicode(255)))

    def test_pjoin_compile(self):
        """test that remote_side columns in the secondary join table
        arent attempted to be matched to the target polymorphic
        selectable"""

        class BaseItem(object):
            pass

        class Item(BaseItem):
            pass

        class Collection(object):
            pass
        item_join = polymorphic_union({
            'BaseItem': base_item_table.select(
                base_item_table.c.child_name == 'BaseItem'),
            'Item': base_item_table.join(item_table),
        }, None, 'item_join')

        mapper(
            BaseItem, base_item_table, with_polymorphic=('*', item_join),
            polymorphic_on=base_item_table.c.child_name,
            polymorphic_identity='BaseItem',
            properties=dict(
                collections=relationship(
                    Collection, secondary=base_item_collection_table,
                    backref="items")))

        mapper(
            Item, item_table,
            inherits=BaseItem,
            polymorphic_identity='Item')

        mapper(Collection, collection_table)

        class_mapper(BaseItem)


class CustomPKTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global t1, t2
        t1 = Table('t1', metadata,
                   Column('id', Integer, primary_key=True,
                          test_needs_autoincrement=True),
                   Column('type', String(30), nullable=False),
                   Column('data', String(30)))
        # note that the primary key column in t2 is named differently
        t2 = Table('t2', metadata,
                   Column('t2id', Integer, ForeignKey(
                       't1.id'), primary_key=True),
                   Column('t2data', String(30)))

    def test_custompk(self):
        """test that the primary_key attribute is propagated to the
        polymorphic mapper"""

        class T1(object):
            pass

        class T2(T1):
            pass

        # create a polymorphic union with the select against the base table
        # first. with the join being second, the alias of the union will
        # pick up two "primary key" columns.  technically the alias should have
        # a 2-col pk in any case but the leading select has a NULL for the
        # "t2id" column
        d = util.OrderedDict()
        d['t1'] = t1.select(t1.c.type == 't1')
        d['t2'] = t1.join(t2)
        pjoin = polymorphic_union(d, None, 'pjoin')

        mapper(T1, t1, polymorphic_on=t1.c.type,
               polymorphic_identity='t1',
               with_polymorphic=('*', pjoin),
               primary_key=[pjoin.c.id])
        mapper(T2, t2, inherits=T1, polymorphic_identity='t2')
        ot1 = T1()
        ot2 = T2()
        sess = create_session()
        sess.add(ot1)
        sess.add(ot2)
        sess.flush()
        sess.expunge_all()

        # query using get(), using only one value.
        # this requires the select_table mapper
        # has the same single-col primary key.
        assert sess.query(T1).get(ot1.id).id == ot1.id

        ot1 = sess.query(T1).get(ot1.id)
        ot1.data = 'hi'
        sess.flush()

    def test_pk_collapses(self):
        """test that a composite primary key attribute formed by a join
        is "collapsed" into its minimal columns"""

        class T1(object):
            pass

        class T2(T1):
            pass

        # create a polymorphic union with the select against the base table
        # first. with the join being second, the alias of the union will
        # pick up two "primary key" columns.  technically the alias should have
        # a 2-col pk in any case but the leading select has a NULL for the
        # "t2id" column
        d = util.OrderedDict()
        d['t1'] = t1.select(t1.c.type == 't1')
        d['t2'] = t1.join(t2)
        pjoin = polymorphic_union(d, None, 'pjoin')

        mapper(T1, t1, polymorphic_on=t1.c.type,
               polymorphic_identity='t1',
               with_polymorphic=('*', pjoin))
        mapper(T2, t2, inherits=T1, polymorphic_identity='t2')
        assert len(class_mapper(T1).primary_key) == 1

        ot1 = T1()
        ot2 = T2()
        sess = create_session()
        sess.add(ot1)
        sess.add(ot2)
        sess.flush()
        sess.expunge_all()

        # query using get(), using only one value.  this requires the
        # select_table mapper
        # has the same single-col primary key.
        assert sess.query(T1).get(ot1.id).id == ot1.id

        ot1 = sess.query(T1).get(ot1.id)
        ot1.data = 'hi'
        sess.flush()


class InheritingEagerTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global people, employees, tags, peopleTags

        people = Table('people', metadata,
                       Column('id', Integer, primary_key=True,
                              test_needs_autoincrement=True),
                       Column('_type', String(30), nullable=False))

        employees = Table('employees', metadata,
                          Column('id', Integer, ForeignKey('people.id'),
                                 primary_key=True))

        tags = Table('tags', metadata,
                     Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
                     Column('label', String(50), nullable=False))

        peopleTags = Table('peopleTags', metadata,
                           Column('person_id', Integer,
                                  ForeignKey('people.id')),
                           Column('tag_id', Integer,
                                  ForeignKey('tags.id')))

    def test_basic(self):
        """test that Query uses the full set of mapper._eager_loaders
        when generating SQL"""

        class Person(fixtures.ComparableEntity):
            pass

        class Employee(Person):
            def __init__(self, name='bob'):
                self.name = name

        class Tag(fixtures.ComparableEntity):
            def __init__(self, label):
                self.label = label

        mapper(Person, people, polymorphic_on=people.c._type,
               polymorphic_identity='person', properties={
                   'tags': relationship(Tag,
                                        secondary=peopleTags,
                                        backref='people', lazy='joined')})
        mapper(Employee, employees, inherits=Person,
               polymorphic_identity='employee')
        mapper(Tag, tags)

        session = create_session()

        bob = Employee()
        session.add(bob)

        tag = Tag('crazy')
        bob.tags.append(tag)

        tag = Tag('funny')
        bob.tags.append(tag)
        session.flush()

        session.expunge_all()
        # query from Employee with limit, query needs to apply eager limiting
        # subquery
        instance = session.query(Employee).\
            filter_by(id=1).limit(1).first()
        assert len(instance.tags) == 2


class MissingPolymorphicOnTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        tablea = Table('tablea', metadata,
                       Column('id', Integer, primary_key=True,
                              test_needs_autoincrement=True),
                       Column('adata', String(50)))
        tableb = Table('tableb', metadata,
                       Column('id', Integer, primary_key=True,
                              test_needs_autoincrement=True),
                       Column('aid', Integer, ForeignKey('tablea.id')),
                       Column('data', String(50)))
        tablec = Table('tablec', metadata,
                       Column('id', Integer, ForeignKey('tablea.id'),
                              primary_key=True),
                       Column('cdata', String(50)))
        tabled = Table('tabled', metadata,
                       Column('id', Integer, ForeignKey('tablec.id'),
                              primary_key=True),
                       Column('ddata', String(50)))

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

        class B(cls.Comparable):
            pass

        class C(A):
            pass

        class D(C):
            pass

    def test_polyon_col_setsup(self):
        tablea, tableb, tablec, tabled = self.tables.tablea, \
            self.tables.tableb, self.tables.tablec, self.tables.tabled
        A, B, C, D = self.classes.A, self.classes.B, self.classes.C, \
            self.classes.D
        poly_select = select(
            [tablea, tableb.c.data.label('discriminator')],
            from_obj=tablea.join(tableb)).alias('poly')

        mapper(B, tableb)
        mapper(A, tablea,
               with_polymorphic=('*', poly_select),
               polymorphic_on=poly_select.c.discriminator,
               properties={'b': relationship(B, uselist=False)})
        mapper(C, tablec, inherits=A, polymorphic_identity='c')
        mapper(D, tabled, inherits=C, polymorphic_identity='d')

        c = C(cdata='c1', adata='a1', b=B(data='c'))
        d = D(cdata='c2', adata='a2', ddata='d2', b=B(data='d'))
        sess = create_session()
        sess.add(c)
        sess.add(d)
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(A).all(),
            [
                C(cdata='c1', adata='a1'),
                D(cdata='c2', adata='a2', ddata='d2')
            ]
        )


class JoinedInhAdjacencyTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('people', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('type', String(30)))
        Table('users', metadata,
              Column('id', Integer, ForeignKey('people.id'),
                     primary_key=True),
              Column('supervisor_id', Integer, ForeignKey('people.id')))
        Table('dudes', metadata,
              Column('id', Integer, ForeignKey('users.id'), primary_key=True))

    @classmethod
    def setup_classes(cls):
        class Person(cls.Comparable):
            pass

        class User(Person):
            pass

        class Dude(User):
            pass

    def _roundtrip(self):
        Person, User = self.classes.Person, self.classes.User
        sess = Session()
        u1 = User()
        u2 = User()
        u2.supervisor = u1
        sess.add_all([u1, u2])
        sess.commit()

        assert u2.supervisor is u1

    def _dude_roundtrip(self):
        Dude, User = self.classes.Dude, self.classes.User
        sess = Session()
        u1 = User()
        d1 = Dude()
        d1.supervisor = u1
        sess.add_all([u1, d1])
        sess.commit()

        assert d1.supervisor is u1

    def test_joined_to_base(self):
        people, users = self.tables.people, self.tables.users
        Person, User = self.classes.Person, self.classes.User

        mapper(Person, people,
               polymorphic_on=people.c.type,
               polymorphic_identity='person')
        mapper(User, users, inherits=Person,
               polymorphic_identity='user',
               inherit_condition=(users.c.id == people.c.id),
               properties={
                   'supervisor': relationship(
                       Person,
                       primaryjoin=users.c.supervisor_id == people.c.id)})

        assert User.supervisor.property.direction is MANYTOONE
        self._roundtrip()

    def test_joined_to_same_subclass(self):
        people, users = self.tables.people, self.tables.users
        Person, User = self.classes.Person, self.classes.User

        mapper(Person, people,
               polymorphic_on=people.c.type,
               polymorphic_identity='person')
        mapper(User, users, inherits=Person,
               polymorphic_identity='user',
               inherit_condition=(users.c.id == people.c.id),
               properties={
                   'supervisor': relationship(
                       User,
                       primaryjoin=users.c.supervisor_id == people.c.id,
                       remote_side=people.c.id,
                       foreign_keys=[
                           users.c.supervisor_id])})
        assert User.supervisor.property.direction is MANYTOONE
        self._roundtrip()

    def test_joined_subclass_to_superclass(self):
        people, users, dudes = self.tables.people, self.tables.users, \
            self.tables.dudes
        Person, User, Dude = self.classes.Person, self.classes.User, \
            self.classes.Dude

        mapper(Person, people,
               polymorphic_on=people.c.type,
               polymorphic_identity='person')
        mapper(User, users, inherits=Person,
               polymorphic_identity='user',
               inherit_condition=(users.c.id == people.c.id))
        mapper(Dude, dudes, inherits=User,
               polymorphic_identity='dude',
               inherit_condition=(dudes.c.id == users.c.id),
               properties={
                   'supervisor': relationship(
                       User,
                       primaryjoin=users.c.supervisor_id == people.c.id,
                       remote_side=people.c.id,
                       foreign_keys=[
                           users.c.supervisor_id])})
        assert Dude.supervisor.property.direction is MANYTOONE
        self._dude_roundtrip()


class Ticket2419Test(fixtures.DeclarativeMappedTest):
    """Test [ticket:2419]'s test case."""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

        class B(Base):
            __tablename__ = "b"

            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            ds = relationship("D")
            es = relationship("E")

        class C(A):
            __tablename__ = "c"

            id = Column(Integer, ForeignKey('a.id'), primary_key=True)
            b_id = Column(Integer, ForeignKey('b.id'))
            b = relationship("B", primaryjoin=b_id == B.id)

        class D(Base):
            __tablename__ = "d"

            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            b_id = Column(Integer, ForeignKey('b.id'))

        class E(Base):
            __tablename__ = 'e'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            b_id = Column(Integer, ForeignKey('b.id'))

    @testing.fails_on(["oracle", "mssql"],
                      "Oracle / SQL server engines can't handle this, "
                      "not clear if there's an expression-level bug on our "
                      "end though")
    def test_join_w_eager_w_any(self):
        A, B, C, D, E = (self.classes.A,
                         self.classes.B,
                         self.classes.C,
                         self.classes.D,
                         self.classes.E)
        s = Session(testing.db)

        b = B(ds=[D()])
        s.add_all([
            C(
                b=b
            )

        ])

        s.commit()

        q = s.query(B, B.ds.any(D.id == 1)).options(joinedload_all("es"))
        q = q.join(C, C.b_id == B.id)
        q = q.limit(5)
        eq_(
            q.all(),
            [(b, True)]
        )


class ColSubclassTest(fixtures.DeclarativeMappedTest,
                      testing.AssertsCompiledSQL):
    """Test [ticket:2918]'s test case."""

    run_create_tables = run_deletes = None
    __dialect__ = 'default'

    @classmethod
    def setup_classes(cls):
        from sqlalchemy.schema import Column
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = 'a'

            id = Column(Integer, primary_key=True)

        class MySpecialColumn(Column):
            pass

        class B(A):
            __tablename__ = 'b'

            id = Column(ForeignKey('a.id'), primary_key=True)
            x = MySpecialColumn(String)

    def test_polymorphic_adaptation(self):
        A, B = self.classes.A, self.classes.B

        s = Session()
        self.assert_compile(
            s.query(A).join(B).filter(B.x == 'test'),
            "SELECT a.id AS a_id FROM a JOIN "
            "(a AS a_1 JOIN b AS b_1 ON a_1.id = b_1.id) "
            "ON a.id = b_1.id WHERE b_1.x = :x_1"
        )
