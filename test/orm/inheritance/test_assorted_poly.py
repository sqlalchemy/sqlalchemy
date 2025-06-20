"""Miscellaneous inheritance-related tests, many very old.
These are generally tests derived from specific user issues.

"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import and_
from sqlalchemy import exists
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import Unicode
from sqlalchemy import util
from sqlalchemy.orm import aliased
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import column_property
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import immediateload
from sqlalchemy.orm import join
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import polymorphic_union
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.orm.interfaces import MANYTOONE
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import expect_noload_deprecation
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.provision import normalize_sequence
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class RelationshipTest1(fixtures.MappedTest):
    """test self-referential relationships on polymorphic mappers"""

    @classmethod
    def define_tables(cls, metadata):
        global people, managers

        people = Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                normalize_sequence(
                    config, Sequence("person_id_seq", optional=True)
                ),
                primary_key=True,
            ),
            Column(
                "manager_id",
                Integer,
                ForeignKey(
                    "managers.person_id", use_alter=True, name="mpid_fq"
                ),
            ),
            Column("name", String(50)),
            Column("type", String(30)),
        )

        managers = Table(
            "managers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("status", String(30)),
            Column("manager_name", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class Person(cls.Comparable):
            pass

        class Manager(Person):
            pass

    def test_parent_refs_descendant(self):
        Person, Manager = self.classes("Person", "Manager")

        self.mapper_registry.map_imperatively(
            Person,
            people,
            properties={
                "manager": relationship(
                    Manager,
                    primaryjoin=(people.c.manager_id == managers.c.person_id),
                    uselist=False,
                    post_update=True,
                )
            },
        )
        self.mapper_registry.map_imperatively(
            Manager,
            managers,
            inherits=Person,
            inherit_condition=people.c.person_id == managers.c.person_id,
        )

        eq_(
            class_mapper(Person).get_property("manager").synchronize_pairs,
            [(managers.c.person_id, people.c.manager_id)],
        )

        session = fixture_session()
        p = Person(name="some person")
        m = Manager(name="some manager")
        p.manager = m
        session.add(p)
        session.flush()
        session.expunge_all()

        p = session.get(Person, p.person_id)
        m = session.get(Manager, m.person_id)
        assert p.manager is m

    def test_descendant_refs_parent(self):
        Person, Manager = self.classes("Person", "Manager")

        self.mapper_registry.map_imperatively(Person, people)
        self.mapper_registry.map_imperatively(
            Manager,
            managers,
            inherits=Person,
            inherit_condition=people.c.person_id == managers.c.person_id,
            properties={
                "employee": relationship(
                    Person,
                    primaryjoin=(people.c.manager_id == managers.c.person_id),
                    foreign_keys=[people.c.manager_id],
                    uselist=False,
                    post_update=True,
                )
            },
        )

        session = fixture_session()
        p = Person(name="some person")
        m = Manager(name="some manager")
        m.employee = p
        session.add(m)
        session.flush()
        session.expunge_all()

        p = session.get(Person, p.person_id)
        m = session.get(Manager, m.person_id)
        assert m.employee is p


class RelationshipTest2(fixtures.MappedTest):
    """test self-referential relationships on polymorphic mappers"""

    @classmethod
    def define_tables(cls, metadata):
        global people, managers, data
        people = Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("type", String(30)),
        )

        managers = Table(
            "managers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("manager_id", Integer, ForeignKey("people.person_id")),
            Column("status", String(30)),
        )

        data = Table(
            "data",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("managers.person_id"),
                primary_key=True,
            ),
            Column("data", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class Person(cls.Comparable):
            pass

        class Manager(Person):
            pass

    @testing.combinations(
        ("join1",), ("join2",), ("join3",), argnames="jointype"
    )
    @testing.combinations(
        ("usedata", True), ("nodata", False), id_="ia", argnames="usedata"
    )
    def test_relationshiponsubclass(self, jointype, usedata):
        Person, Manager = self.classes("Person", "Manager")
        if jointype == "join1":
            poly_union = polymorphic_union(
                {
                    "person": people.select()
                    .where(people.c.type == "person")
                    .subquery(),
                    "manager": join(
                        people,
                        managers,
                        people.c.person_id == managers.c.person_id,
                    ),
                },
                None,
            )
            polymorphic_on = poly_union.c.type
        elif jointype == "join2":
            poly_union = polymorphic_union(
                {
                    "person": people.select()
                    .where(people.c.type == "person")
                    .subquery(),
                    "manager": managers.join(
                        people, people.c.person_id == managers.c.person_id
                    ),
                },
                None,
            )
            polymorphic_on = poly_union.c.type
        elif jointype == "join3":
            poly_union = None
            polymorphic_on = people.c.type

        if usedata:

            class Data:
                def __init__(self, data):
                    self.data = data

            self.mapper_registry.map_imperatively(Data, data)

        self.mapper_registry.map_imperatively(
            Person,
            people,
            with_polymorphic=("*", poly_union),
            polymorphic_identity="person",
            polymorphic_on=polymorphic_on,
        )

        if usedata:
            self.mapper_registry.map_imperatively(
                Manager,
                managers,
                inherits=Person,
                inherit_condition=people.c.person_id == managers.c.person_id,
                polymorphic_identity="manager",
                properties={
                    "colleague": relationship(
                        Person,
                        primaryjoin=managers.c.manager_id
                        == people.c.person_id,
                        lazy="select",
                        uselist=False,
                    ),
                    "data": relationship(Data, uselist=False),
                },
            )
        else:
            self.mapper_registry.map_imperatively(
                Manager,
                managers,
                inherits=Person,
                inherit_condition=people.c.person_id == managers.c.person_id,
                polymorphic_identity="manager",
                properties={
                    "colleague": relationship(
                        Person,
                        primaryjoin=managers.c.manager_id
                        == people.c.person_id,
                        lazy="select",
                        uselist=False,
                    )
                },
            )

        sess = fixture_session()
        p = Person(name="person1")
        m = Manager(name="manager1")
        m.colleague = p
        if usedata:
            m.data = Data("ms data")
        sess.add(m)
        sess.flush()

        sess.expunge_all()
        p = sess.get(Person, p.person_id)
        m = sess.get(Manager, m.person_id)
        assert m.colleague is p
        if usedata:
            assert m.data.data == "ms data"


class RelationshipTest3(fixtures.MappedTest):
    """test self-referential relationships on polymorphic mappers"""

    @classmethod
    def define_tables(cls, metadata):
        global people, managers, data
        people = Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("colleague_id", Integer, ForeignKey("people.person_id")),
            Column("name", String(50)),
            Column("type", String(30)),
        )

        managers = Table(
            "managers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("status", String(30)),
        )

        data = Table(
            "data",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("data", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class Person(cls.Comparable):
            pass

        class Manager(Person):
            pass

        class Data(cls.Comparable):
            def __init__(self, data):
                self.data = data

    def _setup_mappings(self, jointype, usedata):
        Person, Manager, Data = self.classes("Person", "Manager", "Data")
        if jointype == "join1":
            poly_union = polymorphic_union(
                {
                    "manager": managers.join(
                        people, people.c.person_id == managers.c.person_id
                    ),
                    "person": people.select()
                    .where(people.c.type == "person")
                    .subquery(),
                },
                None,
            )
        elif jointype == "join2":
            poly_union = polymorphic_union(
                {
                    "manager": join(
                        people,
                        managers,
                        people.c.person_id == managers.c.person_id,
                    ),
                    "person": people.select()
                    .where(people.c.type == "person")
                    .subquery(),
                },
                None,
            )
        elif jointype == "join3":
            poly_union = people.outerjoin(managers)
        elif jointype == "join4":
            poly_union = None
        else:
            assert False

        if usedata:
            self.mapper_registry.map_imperatively(Data, data)

        if usedata:
            self.mapper_registry.map_imperatively(
                Person,
                people,
                with_polymorphic=("*", poly_union),
                polymorphic_identity="person",
                polymorphic_on=people.c.type,
                properties={
                    "colleagues": relationship(
                        Person,
                        primaryjoin=people.c.colleague_id
                        == people.c.person_id,
                        remote_side=people.c.colleague_id,
                        uselist=True,
                    ),
                    "data": relationship(Data, uselist=False),
                },
            )
        else:
            self.mapper_registry.map_imperatively(
                Person,
                people,
                with_polymorphic=("*", poly_union),
                polymorphic_identity="person",
                polymorphic_on=people.c.type,
                properties={
                    "colleagues": relationship(
                        Person,
                        primaryjoin=people.c.colleague_id
                        == people.c.person_id,
                        remote_side=people.c.colleague_id,
                        uselist=True,
                    )
                },
            )

        self.mapper_registry.map_imperatively(
            Manager,
            managers,
            inherits=Person,
            inherit_condition=people.c.person_id == managers.c.person_id,
            polymorphic_identity="manager",
        )

    @testing.combinations(
        ("join1",), ("join2",), ("join3",), ("join4",), argnames="jointype"
    )
    @testing.combinations(
        ("usedata", True), ("nodata", False), id_="ia", argnames="usedata"
    )
    def test_relationship_on_base_class(self, jointype, usedata):
        self._setup_mappings(jointype, usedata)
        Person, Manager, Data = self.classes("Person", "Manager", "Data")

        sess = fixture_session()
        p = Person(name="person1")
        p2 = Person(name="person2")
        p3 = Person(name="person3")
        m = Manager(name="manager1")
        p.colleagues.append(p2)
        m.colleagues.append(p3)
        if usedata:
            p.data = Data("ps data")
            m.data = Data("ms data")

        sess.add(m)
        sess.add(p)
        sess.flush()

        sess.expunge_all()
        p = sess.get(Person, p.person_id)
        p2 = sess.get(Person, p2.person_id)
        p3 = sess.get(Person, p3.person_id)
        m = sess.get(Person, m.person_id)
        assert len(p.colleagues) == 1
        assert p.colleagues == [p2]
        assert m.colleagues == [p3]
        if usedata:
            assert p.data.data == "ps data"
            assert m.data.data == "ms data"


class RelationshipTest4(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global people, engineers, managers, cars
        people = Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
        )

        engineers = Table(
            "engineers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("status", String(30)),
        )

        managers = Table(
            "managers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("longer_status", String(70)),
        )

        cars = Table(
            "cars",
            metadata,
            Column(
                "car_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("owner", Integer, ForeignKey("people.person_id")),
        )

    def test_many_to_one_polymorphic(self):
        """in this test, the polymorphic union is between two subclasses, but
        does not include the base table by itself in the union. however, the
        primaryjoin condition is going to be against the base table, and its a
        many-to-one relationship (unlike the test in polymorph.py) so the
        column in the base table is explicit. Can the ClauseAdapter figure out
        how to alias the primaryjoin to the polymorphic union ?"""

        # class definitions
        class Person:
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

            def __repr__(self):
                return "Ordinary person %s" % self.name

        class Engineer(Person):
            def __repr__(self):
                return "Engineer %s, status %s" % (self.name, self.status)

        class Manager(Person):
            def __repr__(self):
                return "Manager %s, status %s" % (
                    self.name,
                    self.longer_status,
                )

        class Car:
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

            def __repr__(self):
                return "Car number %d" % self.car_id

        # create a union that represents both types of joins.
        employee_join = polymorphic_union(
            {
                "engineer": people.join(engineers),
                "manager": people.join(managers),
            },
            "type",
            "employee_join",
        )

        person_mapper = self.mapper_registry.map_imperatively(
            Person,
            people,
            with_polymorphic=("*", employee_join),
            polymorphic_on=employee_join.c.type,
            polymorphic_identity="person",
        )
        self.mapper_registry.map_imperatively(
            Engineer,
            engineers,
            with_polymorphic=([Engineer], people.join(engineers)),
            inherits=person_mapper,
            polymorphic_identity="engineer",
        )
        self.mapper_registry.map_imperatively(
            Manager,
            managers,
            with_polymorphic=([Manager], people.join(managers)),
            inherits=person_mapper,
            polymorphic_identity="manager",
        )
        self.mapper_registry.map_imperatively(
            Car, cars, properties={"employee": relationship(person_mapper)}
        )

        session = fixture_session()

        # creating 5 managers named from M1 to E5
        for i in range(1, 5):
            session.add(Manager(name="M%d" % i, longer_status="YYYYYYYYY"))
        # creating 5 engineers named from E1 to E5
        for i in range(1, 5):
            session.add(Engineer(name="E%d" % i, status="X"))

        session.flush()

        engineer4 = (
            session.query(Engineer).filter(Engineer.name == "E4").first()
        )
        manager3 = session.query(Manager).filter(Manager.name == "M3").first()

        car1 = Car(employee=engineer4)
        session.add(car1)
        car2 = Car(employee=manager3)
        session.add(car2)
        session.flush()

        session.expunge_all()

        def go():
            testcar = session.get(
                Car, car1.car_id, options=[joinedload(Car.employee)]
            )
            assert str(testcar.employee) == "Engineer E4, status X"

        self.assert_sql_count(testing.db, go, 1)

        car1 = session.get(Car, car1.car_id)
        usingGet = session.get(person_mapper, car1.owner)
        usingProperty = car1.employee

        assert str(engineer4) == "Engineer E4, status X"
        assert str(usingGet) == "Engineer E4, status X"
        assert str(usingProperty) == "Engineer E4, status X"

        session.expunge_all()
        # and now for the lightning round, eager !

        def go():
            testcar = session.get(
                Car,
                car1.car_id,
                options=[joinedload(Car.employee)],
            )
            assert str(testcar.employee) == "Engineer E4, status X"

        self.assert_sql_count(testing.db, go, 1)

        session.expunge_all()
        s = session.query(Car)
        c = s.join(Car.employee).filter(Person.name == "E4")[0]
        assert c.car_id == car1.car_id


class RelationshipTest5(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global people, engineers, managers, cars
        people = Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("type", String(50)),
        )

        engineers = Table(
            "engineers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("status", String(30)),
        )

        managers = Table(
            "managers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("longer_status", String(70)),
        )

        cars = Table(
            "cars",
            metadata,
            Column(
                "car_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("owner", Integer, ForeignKey("people.person_id")),
        )

    def test_eager_empty(self):
        """test parent object with child relationship to an inheriting mapper,
        using eager loads, works when there are no child objects present"""

        class Person:
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

            def __repr__(self):
                return "Ordinary person %s" % self.name

        class Engineer(Person):
            def __repr__(self):
                return "Engineer %s, status %s" % (self.name, self.status)

        class Manager(Person):
            def __repr__(self):
                return "Manager %s, status %s" % (
                    self.name,
                    self.longer_status,
                )

        class Car:
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

            def __repr__(self):
                return "Car number %d" % self.car_id

        person_mapper = self.mapper_registry.map_imperatively(
            Person,
            people,
            polymorphic_on=people.c.type,
            polymorphic_identity="person",
        )
        self.mapper_registry.map_imperatively(
            Engineer,
            engineers,
            inherits=person_mapper,
            polymorphic_identity="engineer",
        )
        manager_mapper = self.mapper_registry.map_imperatively(
            Manager,
            managers,
            inherits=person_mapper,
            polymorphic_identity="manager",
        )
        self.mapper_registry.map_imperatively(
            Car,
            cars,
            properties={
                "manager": relationship(manager_mapper, lazy="joined")
            },
        )

        sess = fixture_session()
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
        global people, managers
        people = Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
        )

        managers = Table(
            "managers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("colleague_id", Integer, ForeignKey("managers.person_id")),
            Column("status", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class Person(cls.Comparable):
            pass

        class Manager(Person):
            pass

    def test_basic(self):
        Person, Manager = self.classes("Person", "Manager")

        self.mapper_registry.map_imperatively(Person, people)

        self.mapper_registry.map_imperatively(
            Manager,
            managers,
            inherits=Person,
            inherit_condition=people.c.person_id == managers.c.person_id,
            properties={
                "colleague": relationship(
                    Manager,
                    primaryjoin=managers.c.colleague_id
                    == managers.c.person_id,
                    lazy="select",
                    uselist=False,
                )
            },
        )

        sess = fixture_session()
        m = Manager(name="manager1")
        m2 = Manager(name="manager2")
        m.colleague = m2
        sess.add(m)
        sess.flush()

        sess.expunge_all()
        m = sess.get(Manager, m.person_id)
        m2 = sess.get(Manager, m2.person_id)
        assert m.colleague is m2


class RelationshipTest7(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global people, engineers, managers, cars, offroad_cars
        cars = Table(
            "cars",
            metadata,
            Column(
                "car_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(30)),
        )

        offroad_cars = Table(
            "offroad_cars",
            metadata,
            Column(
                "car_id",
                Integer,
                ForeignKey("cars.car_id"),
                nullable=False,
                primary_key=True,
            ),
        )

        people = Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column(
                "car_id", Integer, ForeignKey("cars.car_id"), nullable=False
            ),
            Column("name", String(50)),
        )

        engineers = Table(
            "engineers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("field", String(30)),
        )

        managers = Table(
            "managers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("category", String(70)),
        )

    def test_manytoone_lazyload(self):
        """test that lazy load clause to a polymorphic child mapper generates
        correctly [ticket:493]"""

        class PersistentObject:
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
                return "Engineer %s, field %s" % (self.name, self.field)

        class Manager(Person):
            def __repr__(self):
                return "Manager %s, category %s" % (self.name, self.category)

        class Car(PersistentObject):
            def __repr__(self):
                return "Car number %d, name %s" % (self.car_id, self.name)

        class Offraod_Car(Car):
            def __repr__(self):
                return "Offroad Car number %d, name %s" % (
                    self.car_id,
                    self.name,
                )

        employee_join = polymorphic_union(
            {
                "engineer": people.join(engineers),
                "manager": people.join(managers),
            },
            "type",
            "employee_join",
        )

        car_join = polymorphic_union(
            {
                "car": cars.outerjoin(offroad_cars)
                .select()
                .where(offroad_cars.c.car_id == None)
                .reduce_columns()
                .subquery(),
                "offroad": cars.join(offroad_cars),
            },
            "type",
            "car_join",
        )

        car_mapper = self.mapper_registry.map_imperatively(
            Car,
            cars,
            with_polymorphic=("*", car_join),
            polymorphic_on=car_join.c.type,
            polymorphic_identity="car",
        )
        self.mapper_registry.map_imperatively(
            Offraod_Car,
            offroad_cars,
            inherits=car_mapper,
            polymorphic_identity="offroad",
        )
        person_mapper = self.mapper_registry.map_imperatively(
            Person,
            people,
            with_polymorphic=("*", employee_join),
            polymorphic_on=employee_join.c.type,
            polymorphic_identity="person",
            properties={"car": relationship(car_mapper)},
        )
        self.mapper_registry.map_imperatively(
            Engineer,
            engineers,
            inherits=person_mapper,
            polymorphic_identity="engineer",
        )
        self.mapper_registry.map_imperatively(
            Manager,
            managers,
            inherits=person_mapper,
            polymorphic_identity="manager",
        )

        session = fixture_session()

        for i in range(1, 4):
            if i % 2:
                car = Car()
            else:
                car = Offraod_Car()
            session.add(Manager(name="M%d" % i, category="YYYYYYYYY", car=car))
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
        taggable = Table(
            "taggable",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("type", String(30)),
            Column("owner_id", Integer, ForeignKey("taggable.id")),
        )
        users = Table(
            "users",
            metadata,
            Column("id", Integer, ForeignKey("taggable.id"), primary_key=True),
            Column("data", String(50)),
        )

    def test_selfref_onjoined(self):
        class Taggable(ComparableEntity):
            pass

        class User(Taggable):
            pass

        self.mapper_registry.map_imperatively(
            Taggable,
            taggable,
            polymorphic_on=taggable.c.type,
            polymorphic_identity="taggable",
            properties={
                "owner": relationship(
                    User,
                    primaryjoin=taggable.c.owner_id == taggable.c.id,
                    remote_side=taggable.c.id,
                )
            },
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            inherits=Taggable,
            polymorphic_identity="user",
            inherit_condition=users.c.id == taggable.c.id,
        )

        u1 = User(data="u1")
        t1 = Taggable(owner=u1)
        sess = fixture_session()
        sess.add(t1)
        sess.flush()

        sess.expunge_all()
        eq_(
            sess.query(Taggable).order_by(Taggable.id).all(),
            [User(data="u1"), Taggable(owner=User(data="u1"))],
        )


class ColPropWAliasJoinedToBaseTest(
    AssertsCompiledSQL, fixtures.DeclarativeMappedTest
):
    """test #6762"""

    __dialect__ = "default"
    run_create_tables = None

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Content(Base):
            __tablename__ = "content"

            id = Column(Integer, primary_key=True)
            type = Column(String)
            container_id = Column(Integer, ForeignKey("folder.id"))

            __mapper_args__ = {"polymorphic_on": type}

        class Folder(Content):
            __tablename__ = "folder"

            id = Column(ForeignKey("content.id"), primary_key=True)

            __mapper_args__ = {
                "polymorphic_identity": "f",
                "inherit_condition": id == Content.id,
            }

        _alias = aliased(Content)

        Content.__mapper__.add_property(
            "count_children",
            column_property(
                select(func.count("*"))
                .where(_alias.container_id == Content.id)
                .scalar_subquery()
            ),
        )

    def test_alias_omitted(self):
        Content = self.classes.Content
        Folder = self.classes.Folder

        sess = fixture_session()

        entity = with_polymorphic(Content, [Folder], innerjoin=True)

        self.assert_compile(
            sess.query(entity),
            "SELECT content.id AS content_id, content.type AS content_type, "
            "content.container_id AS content_container_id, "
            "(SELECT count(:count_2) AS count_1 FROM content AS content_1 "
            "WHERE content_1.container_id = content.id) AS anon_1, "
            "folder.id AS folder_id FROM content "
            "JOIN folder ON folder.id = content.id",
        )


class SelfRefWPolyJoinedLoadTest(fixtures.DeclarativeMappedTest):
    """test #6495"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Node(ComparableEntity, Base):
            __tablename__ = "nodes"

            id = Column(Integer, primary_key=True)

            parent_id = Column(ForeignKey("nodes.id"))
            type = Column(String(50))

            parent = relationship("Node", remote_side=id)

            local_groups = relationship("LocalGroup", lazy="joined")

            __mapper_args__ = {
                "polymorphic_on": type,
                "with_polymorphic": ("*"),
                "polymorphic_identity": "node",
            }

        class Content(Node):
            __tablename__ = "content"

            id = Column(ForeignKey("nodes.id"), primary_key=True)

            __mapper_args__ = {
                "polymorphic_identity": "content",
            }

        class File(Node):
            __tablename__ = "file"

            id = Column(ForeignKey("nodes.id"), primary_key=True)
            __mapper_args__ = {
                "polymorphic_identity": "file",
            }

        class LocalGroup(ComparableEntity, Base):
            __tablename__ = "local_group"
            id = Column(Integer, primary_key=True)

            node_id = Column(ForeignKey("nodes.id"))

    @classmethod
    def insert_data(cls, connection):
        Node, LocalGroup = cls.classes("Node", "LocalGroup")

        with Session(connection) as sess:
            f1 = Node(id=2, local_groups=[LocalGroup(), LocalGroup()])
            c1 = Node(id=1)
            c1.parent = f1

            sess.add_all([f1, c1])

            sess.commit()

    def test_emit_lazy_loadonpk_parent(self):
        Node, LocalGroup = self.classes("Node", "LocalGroup")

        s = fixture_session()
        c1 = s.query(Node).filter_by(id=1).first()

        def go():
            p1 = c1.parent
            eq_(p1, Node(id=2, local_groups=[LocalGroup(), LocalGroup()]))

        self.assert_sql_count(testing.db, go, 1)


class GenerativeTest(fixtures.MappedTest, AssertsExecutionResults):
    @classmethod
    def define_tables(cls, metadata):
        #  cars---owned by---  people (abstract) --- has a --- status
        #   |                  ^    ^                            |
        #   |                  |    |                            |
        #   |          engineers    managers                     |
        #   |                                                    |
        #   +--------------------------------------- has a ------+

        # table definitions
        Table(
            "status",
            metadata,
            Column(
                "status_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(20)),
        )

        Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column(
                "status_id",
                Integer,
                ForeignKey("status.status_id"),
                nullable=False,
            ),
            Column("name", String(50)),
        )

        Table(
            "engineers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("field", String(30)),
        )

        Table(
            "managers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("category", String(70)),
        )

        Table(
            "cars",
            metadata,
            Column(
                "car_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column(
                "status_id",
                Integer,
                ForeignKey("status.status_id"),
                nullable=False,
            ),
            Column(
                "owner",
                Integer,
                ForeignKey("people.person_id"),
                nullable=False,
            ),
        )

    @classmethod
    def setup_classes(cls):
        class Status(cls.Comparable):
            pass

        class Person(cls.Comparable):
            pass

        class Engineer(Person):
            pass

        class Manager(Person):
            pass

        class Car(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        status, people, engineers, managers, cars = cls.tables(
            "status", "people", "engineers", "managers", "cars"
        )
        Status, Person, Engineer, Manager, Car = cls.classes(
            "Status", "Person", "Engineer", "Manager", "Car"
        )
        # create a union that represents both types of joins.
        employee_join = polymorphic_union(
            {
                "engineer": people.join(engineers),
                "manager": people.join(managers),
            },
            "type",
            "employee_join",
        )

        status_mapper = cls.mapper_registry.map_imperatively(Status, status)
        person_mapper = cls.mapper_registry.map_imperatively(
            Person,
            people,
            with_polymorphic=("*", employee_join),
            polymorphic_on=employee_join.c.type,
            polymorphic_identity="person",
            properties={"status": relationship(status_mapper)},
        )
        cls.mapper_registry.map_imperatively(
            Engineer,
            engineers,
            with_polymorphic=([Engineer], people.join(engineers)),
            inherits=person_mapper,
            polymorphic_identity="engineer",
        )
        cls.mapper_registry.map_imperatively(
            Manager,
            managers,
            with_polymorphic=([Manager], people.join(managers)),
            inherits=person_mapper,
            polymorphic_identity="manager",
        )
        cls.mapper_registry.map_imperatively(
            Car,
            cars,
            properties={
                "employee": relationship(person_mapper),
                "status": relationship(status_mapper),
            },
        )

    @classmethod
    def insert_data(cls, connection):
        Status, Person, Engineer, Manager, Car = cls.classes(
            "Status", "Person", "Engineer", "Manager", "Car"
        )
        with sessionmaker(connection).begin() as session:
            active = Status(name="active")
            dead = Status(name="dead")

            session.add(active)
            session.add(dead)

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
                session.add(
                    Manager(name="M%d" % i, category="YYYYYYYYY", status=st)
                )
                session.add(Engineer(name="E%d" % i, field="X", status=st))

            # get E4
            engineer4 = session.query(Engineer).filter_by(name="E4").one()

            # create 2 cars for E4, one active and one dead
            car1 = Car(employee=engineer4, status=active)
            car2 = Car(employee=engineer4, status=dead)
            session.add(car1)
            session.add(car2)

    def test_join_to_q_person(self):
        Status, Person, Engineer, Manager, Car = self.classes(
            "Status", "Person", "Engineer", "Manager", "Car"
        )
        session = fixture_session()

        r = (
            session.query(Person)
            .filter(Person.name.like("%2"))
            .join(Person.status)
            .filter_by(name="active")
            .order_by(Person.person_id)
        )
        eq_(
            list(r),
            [
                Manager(
                    name="M2",
                    category="YYYYYYYYY",
                    status=Status(name="active"),
                ),
                Engineer(name="E2", field="X", status=Status(name="active")),
            ],
        )

    def test_join_to_q_engineer(self):
        Status, Person, Engineer, Manager, Car = self.classes(
            "Status", "Person", "Engineer", "Manager", "Car"
        )
        session = fixture_session()
        r = (
            session.query(Engineer)
            .join(Engineer.status)
            .filter(
                Person.name.in_(["E2", "E3", "E4", "M4", "M2", "M1"])
                & (Status.name == "active")
            )
            .order_by(Person.name)
        )

        eq_(
            list(r),
            [
                Engineer(name="E2", field="X", status=Status(name="active")),
                Engineer(name="E3", field="X", status=Status(name="active")),
            ],
        )

    def test_join_to_q_person_car(self):
        Status, Person, Engineer, Manager, Car = self.classes(
            "Status", "Person", "Engineer", "Manager", "Car"
        )
        session = fixture_session()
        r = session.query(Person).filter(
            exists().where(Car.owner == Person.person_id)
        )

        eq_(
            list(r),
            [Engineer(name="E4", field="X", status=Status(name="dead"))],
        )


class MultiLevelTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global table_Employee, table_Engineer, table_Manager
        table_Employee = Table(
            "Employee",
            metadata,
            Column("name", type_=String(100)),
            Column(
                "id",
                primary_key=True,
                type_=Integer,
                test_needs_autoincrement=True,
            ),
            Column("atype", type_=String(100)),
        )

        table_Engineer = Table(
            "Engineer",
            metadata,
            Column("machine", type_=String(100)),
            Column("id", Integer, ForeignKey("Employee.id"), primary_key=True),
        )

        table_Manager = Table(
            "Manager",
            metadata,
            Column("duties", type_=String(100)),
            Column("id", Integer, ForeignKey("Engineer.id"), primary_key=True),
        )

    def test_threelevels(self):
        class Employee:
            def set(me, **kargs):
                for k, v in kargs.items():
                    setattr(me, k, v)
                return me

            def __str__(me):
                return str(me.__class__.__name__) + ":" + str(me.name)

            __repr__ = __str__

        class Engineer(Employee):
            pass

        class Manager(Engineer):
            pass

        pu_Employee = polymorphic_union(
            {
                "Manager": table_Employee.join(table_Engineer).join(
                    table_Manager
                ),
                "Engineer": select(table_Employee, table_Engineer.c.machine)
                .where(table_Employee.c.atype == "Engineer")
                .select_from(table_Employee.join(table_Engineer))
                .subquery(),
                "Employee": table_Employee.select()
                .where(table_Employee.c.atype == "Employee")
                .subquery(),
            },
            None,
            "pu_employee",
        )

        mapper_Employee = self.mapper_registry.map_imperatively(
            Employee,
            table_Employee,
            polymorphic_identity="Employee",
            polymorphic_on=pu_Employee.c.atype,
            with_polymorphic=("*", pu_Employee),
        )

        pu_Engineer = polymorphic_union(
            {
                "Manager": table_Employee.join(table_Engineer).join(
                    table_Manager
                ),
                "Engineer": select(table_Employee, table_Engineer.c.machine)
                .where(table_Employee.c.atype == "Engineer")
                .select_from(table_Employee.join(table_Engineer))
                .subquery(),
            },
            None,
            "pu_engineer",
        )
        mapper_Engineer = self.mapper_registry.map_imperatively(
            Engineer,
            table_Engineer,
            inherit_condition=table_Engineer.c.id == table_Employee.c.id,
            inherits=mapper_Employee,
            polymorphic_identity="Engineer",
            polymorphic_on=pu_Engineer.c.atype,
            with_polymorphic=("*", pu_Engineer),
        )

        self.mapper_registry.map_imperatively(
            Manager,
            table_Manager,
            inherit_condition=table_Manager.c.id == table_Engineer.c.id,
            inherits=mapper_Engineer,
            polymorphic_identity="Manager",
        )

        a = Employee().set(name="one")
        b = Engineer().set(egn="two", machine="any")
        c = Manager().set(name="head", machine="fast", duties="many")

        session = fixture_session()
        session.add(a)
        session.add(b)
        session.add(c)
        session.flush()
        assert set(session.query(Employee).all()) == {a, b, c}
        assert set(session.query(Engineer).all()) == {b, c}
        assert session.query(Manager).all() == [c]


class ManyToManyPolyTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global base_item_table, item_table
        global base_item_collection_table, collection_table
        base_item_table = Table(
            "base_item",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("child_name", String(255), default=None),
        )

        item_table = Table(
            "item",
            metadata,
            Column(
                "id", Integer, ForeignKey("base_item.id"), primary_key=True
            ),
            Column("dummy", Integer, default=0),
        )

        base_item_collection_table = Table(
            "base_item_collection",
            metadata,
            Column("item_id", Integer, ForeignKey("base_item.id")),
            Column("collection_id", Integer, ForeignKey("collection.id")),
        )

        collection_table = Table(
            "collection",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", Unicode(255)),
        )

    def test_pjoin_compile(self):
        """test that remote_side columns in the secondary join table
        aren't attempted to be matched to the target polymorphic
        selectable"""

        class BaseItem:
            pass

        class Item(BaseItem):
            pass

        class Collection:
            pass

        item_join = polymorphic_union(
            {
                "BaseItem": base_item_table.select()
                .where(base_item_table.c.child_name == "BaseItem")
                .subquery(),
                "Item": base_item_table.join(item_table),
            },
            None,
            "item_join",
        )

        self.mapper_registry.map_imperatively(
            BaseItem,
            base_item_table,
            with_polymorphic=("*", item_join),
            polymorphic_on=base_item_table.c.child_name,
            polymorphic_identity="BaseItem",
            properties=dict(
                collections=relationship(
                    Collection,
                    secondary=base_item_collection_table,
                    backref="items",
                )
            ),
        )

        self.mapper_registry.map_imperatively(
            Item, item_table, inherits=BaseItem, polymorphic_identity="Item"
        )

        self.mapper_registry.map_imperatively(Collection, collection_table)

        class_mapper(BaseItem)


class CustomPKTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global t1, t2
        t1 = Table(
            "t1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("type", String(30), nullable=False),
            Column("data", String(30)),
        )
        # note that the primary key column in t2 is named differently
        t2 = Table(
            "t2",
            metadata,
            Column("t2id", Integer, ForeignKey("t1.id"), primary_key=True),
            Column("t2data", String(30)),
        )

    def test_custompk(self):
        """test that the primary_key attribute is propagated to the
        polymorphic mapper"""

        class T1:
            pass

        class T2(T1):
            pass

        # create a polymorphic union with the select against the base table
        # first. with the join being second, the alias of the union will
        # pick up two "primary key" columns.  technically the alias should have
        # a 2-col pk in any case but the leading select has a NULL for the
        # "t2id" column
        d = util.OrderedDict()
        d["t1"] = t1.select().where(t1.c.type == "t1").subquery()
        d["t2"] = t1.join(t2)
        pjoin = polymorphic_union(d, None, "pjoin")

        self.mapper_registry.map_imperatively(
            T1,
            t1,
            polymorphic_on=t1.c.type,
            polymorphic_identity="t1",
            with_polymorphic=("*", pjoin),
            primary_key=[pjoin.c.id],
        )
        self.mapper_registry.map_imperatively(
            T2, t2, inherits=T1, polymorphic_identity="t2"
        )
        ot1 = T1()
        ot2 = T2()
        sess = fixture_session()
        sess.add(ot1)
        sess.add(ot2)
        sess.flush()
        sess.expunge_all()

        # query using get(), using only one value.
        # this requires the select_table mapper
        # has the same single-col primary key.
        assert sess.get(T1, ot1.id).id == ot1.id

        ot1 = sess.get(T1, ot1.id)
        ot1.data = "hi"
        sess.flush()

    def test_pk_collapses(self):
        """test that a composite primary key attribute formed by a join
        is "collapsed" into its minimal columns"""

        class T1:
            pass

        class T2(T1):
            pass

        # create a polymorphic union with the select against the base table
        # first. with the join being second, the alias of the union will
        # pick up two "primary key" columns.  technically the alias should have
        # a 2-col pk in any case but the leading select has a NULL for the
        # "t2id" column
        d = util.OrderedDict()
        d["t1"] = t1.select().where(t1.c.type == "t1").subquery()
        d["t2"] = t1.join(t2)
        pjoin = polymorphic_union(d, None, "pjoin")

        self.mapper_registry.map_imperatively(
            T1,
            t1,
            polymorphic_on=t1.c.type,
            polymorphic_identity="t1",
            with_polymorphic=("*", pjoin),
        )
        self.mapper_registry.map_imperatively(
            T2, t2, inherits=T1, polymorphic_identity="t2"
        )
        assert len(class_mapper(T1).primary_key) == 1

        ot1 = T1()
        ot2 = T2()
        sess = fixture_session()
        sess.add(ot1)
        sess.add(ot2)
        sess.flush()
        sess.expunge_all()

        # query using get(), using only one value.  this requires the
        # select_table mapper
        # has the same single-col primary key.
        assert sess.get(T1, ot1.id).id == ot1.id

        ot1 = sess.get(T1, ot1.id)
        ot1.data = "hi"
        sess.flush()


class InheritingEagerTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global people, employees, tags, peopleTags

        people = Table(
            "people",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("_type", String(30), nullable=False),
        )

        employees = Table(
            "employees",
            metadata,
            Column("id", Integer, ForeignKey("people.id"), primary_key=True),
        )

        tags = Table(
            "tags",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("label", String(50), nullable=False),
        )

        peopleTags = Table(
            "peopleTags",
            metadata,
            Column("person_id", Integer, ForeignKey("people.id")),
            Column("tag_id", Integer, ForeignKey("tags.id")),
        )

    def test_basic(self):
        """test that Query uses the full set of mapper._eager_loaders
        when generating SQL"""

        class Person(ComparableEntity):
            pass

        class Employee(Person):
            def __init__(self, name="bob"):
                self.name = name

        class Tag(ComparableEntity):
            def __init__(self, label):
                self.label = label

        self.mapper_registry.map_imperatively(
            Person,
            people,
            polymorphic_on=people.c._type,
            polymorphic_identity="person",
            properties={
                "tags": relationship(
                    Tag, secondary=peopleTags, backref="people", lazy="joined"
                )
            },
        )
        self.mapper_registry.map_imperatively(
            Employee,
            employees,
            inherits=Person,
            polymorphic_identity="employee",
        )
        self.mapper_registry.map_imperatively(Tag, tags)

        session = fixture_session()

        bob = Employee()
        session.add(bob)

        tag = Tag("crazy")
        bob.tags.append(tag)

        tag = Tag("funny")
        bob.tags.append(tag)
        session.flush()

        session.expunge_all()
        # query from Employee with limit, query needs to apply eager limiting
        # subquery
        instance = session.query(Employee).filter_by(id=1).limit(1).first()
        assert len(instance.tags) == 2


class MissingPolymorphicOnTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "tablea",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("adata", String(50)),
        )
        Table(
            "tableb",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("aid", Integer, ForeignKey("tablea.id")),
            Column("data", String(50)),
        )
        Table(
            "tablec",
            metadata,
            Column("id", Integer, ForeignKey("tablea.id"), primary_key=True),
            Column("cdata", String(50)),
        )
        Table(
            "tabled",
            metadata,
            Column("id", Integer, ForeignKey("tablec.id"), primary_key=True),
            Column("ddata", String(50)),
        )

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
        tablea, tableb, tablec, tabled = (
            self.tables.tablea,
            self.tables.tableb,
            self.tables.tablec,
            self.tables.tabled,
        )
        A, B, C, D = (
            self.classes.A,
            self.classes.B,
            self.classes.C,
            self.classes.D,
        )
        poly_select = (
            select(tablea, tableb.c.data.label("discriminator"))
            .select_from(tablea.join(tableb))
            .alias("poly")
        )

        self.mapper_registry.map_imperatively(B, tableb)
        self.mapper_registry.map_imperatively(
            A,
            tablea,
            with_polymorphic=("*", poly_select),
            polymorphic_on=poly_select.c.discriminator,
            properties={"b": relationship(B, uselist=False)},
        )
        self.mapper_registry.map_imperatively(
            C, tablec, inherits=A, polymorphic_identity="c"
        )
        self.mapper_registry.map_imperatively(
            D, tabled, inherits=C, polymorphic_identity="d"
        )

        c = C(cdata="c1", adata="a1", b=B(data="c"))
        d = D(cdata="c2", adata="a2", ddata="d2", b=B(data="d"))
        sess = fixture_session()
        sess.add(c)
        sess.add(d)
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(A).all(),
            [C(cdata="c1", adata="a1"), D(cdata="c2", adata="a2", ddata="d2")],
        )


class JoinedInhAdjacencyTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "people",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("type", String(30)),
        )
        Table(
            "users",
            metadata,
            Column("id", Integer, ForeignKey("people.id"), primary_key=True),
            Column("supervisor_id", Integer, ForeignKey("people.id")),
        )
        Table(
            "dudes",
            metadata,
            Column("id", Integer, ForeignKey("users.id"), primary_key=True),
        )

    @classmethod
    def setup_classes(cls):
        class Person(cls.Comparable):
            pass

        class User(Person):
            pass

        class Dude(User):
            pass

    def _roundtrip(self):
        User = self.classes.User
        sess = fixture_session()
        u1 = User()
        u2 = User()
        u2.supervisor = u1
        sess.add_all([u1, u2])
        sess.commit()

        assert u2.supervisor is u1

    def _dude_roundtrip(self):
        Dude, User = self.classes.Dude, self.classes.User
        sess = fixture_session()
        u1 = User()
        d1 = Dude()
        d1.supervisor = u1
        sess.add_all([u1, d1])
        sess.commit()

        assert d1.supervisor is u1

    def test_joined_to_base(self):
        people, users = self.tables.people, self.tables.users
        Person, User = self.classes.Person, self.classes.User

        self.mapper_registry.map_imperatively(
            Person,
            people,
            polymorphic_on=people.c.type,
            polymorphic_identity="person",
        )
        self.mapper_registry.map_imperatively(
            User,
            users,
            inherits=Person,
            polymorphic_identity="user",
            inherit_condition=(users.c.id == people.c.id),
            properties={
                "supervisor": relationship(
                    Person, primaryjoin=users.c.supervisor_id == people.c.id
                )
            },
        )

        assert User.supervisor.property.direction is MANYTOONE
        self._roundtrip()

    def test_joined_to_same_subclass(self):
        people, users = self.tables.people, self.tables.users
        Person, User = self.classes.Person, self.classes.User

        self.mapper_registry.map_imperatively(
            Person,
            people,
            polymorphic_on=people.c.type,
            polymorphic_identity="person",
        )
        self.mapper_registry.map_imperatively(
            User,
            users,
            inherits=Person,
            polymorphic_identity="user",
            inherit_condition=(users.c.id == people.c.id),
            properties={
                "supervisor": relationship(
                    User,
                    primaryjoin=users.c.supervisor_id == people.c.id,
                    remote_side=people.c.id,
                    foreign_keys=[users.c.supervisor_id],
                )
            },
        )
        assert User.supervisor.property.direction is MANYTOONE
        self._roundtrip()

    def test_joined_subclass_to_superclass(self):
        people, users, dudes = (
            self.tables.people,
            self.tables.users,
            self.tables.dudes,
        )
        Person, User, Dude = (
            self.classes.Person,
            self.classes.User,
            self.classes.Dude,
        )

        self.mapper_registry.map_imperatively(
            Person,
            people,
            polymorphic_on=people.c.type,
            polymorphic_identity="person",
        )
        self.mapper_registry.map_imperatively(
            User,
            users,
            inherits=Person,
            polymorphic_identity="user",
            inherit_condition=(users.c.id == people.c.id),
        )
        self.mapper_registry.map_imperatively(
            Dude,
            dudes,
            inherits=User,
            polymorphic_identity="dude",
            inherit_condition=(dudes.c.id == users.c.id),
            properties={
                "supervisor": relationship(
                    User,
                    primaryjoin=users.c.supervisor_id == people.c.id,
                    remote_side=people.c.id,
                    foreign_keys=[users.c.supervisor_id],
                )
            },
        )
        assert Dude.supervisor.property.direction is MANYTOONE
        self._dude_roundtrip()


class Ticket2419Test(fixtures.DeclarativeMappedTest):
    """Test [ticket:2419]'s test case."""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )

        class B(Base):
            __tablename__ = "b"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            ds = relationship("D")
            es = relationship("E")

        class C(A):
            __tablename__ = "c"

            id = Column(Integer, ForeignKey("a.id"), primary_key=True)
            b_id = Column(Integer, ForeignKey("b.id"))
            b = relationship("B", primaryjoin=b_id == B.id)

        class D(Base):
            __tablename__ = "d"

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            b_id = Column(Integer, ForeignKey("b.id"))

        class E(Base):
            __tablename__ = "e"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            b_id = Column(Integer, ForeignKey("b.id"))

    @testing.fails_on(
        ["oracle", "mssql"],
        "Oracle / SQL server engines can't handle this, "
        "not clear if there's an expression-level bug on our "
        "end though",
    )
    def test_join_w_eager_w_any(self):
        B, C, D = (self.classes.B, self.classes.C, self.classes.D)
        s = fixture_session()

        b = B(ds=[D()])
        s.add_all([C(b=b)])

        s.commit()

        q = s.query(B, B.ds.any(D.id == 1)).options(joinedload(B.es))
        q = q.join(C, C.b_id == B.id)
        q = q.limit(5)
        eq_(q.all(), [(b, True)])


class ColSubclassTest(
    fixtures.DeclarativeMappedTest, testing.AssertsCompiledSQL
):
    """Test [ticket:2918]'s test case."""

    run_create_tables = run_deletes = None
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        from sqlalchemy.schema import Column

        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)

        class MySpecialColumn(Column):
            inherit_cache = True

        class B(A):
            __tablename__ = "b"

            id = Column(ForeignKey("a.id"), primary_key=True)
            x = MySpecialColumn(String)

    def test_polymorphic_adaptation_auto(self):
        A, B = self.classes.A, self.classes.B

        s = fixture_session()
        with testing.expect_warnings(
            "An alias is being generated automatically "
            r"against joined entity Mapper\[B\(b\)\] due to overlapping"
        ):
            self.assert_compile(
                s.query(A).join(B).filter(B.x == "test"),
                "SELECT a.id AS a_id FROM a JOIN "
                "(a AS a_1 JOIN b AS b_1 ON a_1.id = b_1.id) "
                "ON a.id = b_1.id WHERE b_1.x = :x_1",
            )

    def test_polymorphic_adaptation_manual_alias(self):
        A, B = self.classes.A, self.classes.B

        b1 = aliased(B, flat=True)
        s = fixture_session()
        self.assert_compile(
            s.query(A).join(b1).filter(b1.x == "test"),
            "SELECT a.id AS a_id FROM a JOIN "
            "(a AS a_1 JOIN b AS b_1 ON a_1.id = b_1.id) "
            "ON a.id = b_1.id WHERE b_1.x = :x_1",
        )


class CorrelateExceptWPolyAdaptTest(
    fixtures.DeclarativeMappedTest, testing.AssertsCompiledSQL
):
    # test [ticket:4537]'s test case.

    run_create_tables = run_deletes = None
    run_setup_classes = run_setup_mappers = run_define_tables = "each"
    __dialect__ = "default"

    def _fixture(self, use_correlate_except):
        Base = self.DeclarativeBasic

        class Superclass(Base):
            __tablename__ = "s1"
            id = Column(Integer, primary_key=True)
            common_id = Column(ForeignKey("c.id"))
            common_relationship = relationship(
                "Common", uselist=False, innerjoin=True, lazy="noload"
            )
            discriminator_field = Column(String)
            __mapper_args__ = {
                "polymorphic_identity": "superclass",
                "polymorphic_on": discriminator_field,
            }

        class Subclass(Superclass):
            __tablename__ = "s2"
            id = Column(ForeignKey("s1.id"), primary_key=True)
            __mapper_args__ = {"polymorphic_identity": "subclass"}

        class Common(Base):
            __tablename__ = "c"
            id = Column(Integer, primary_key=True)

        if use_correlate_except:
            Common.num_superclass = column_property(
                select(func.count(Superclass.id))
                .where(Superclass.common_id == Common.id)
                .correlate_except(Superclass)
                .scalar_subquery()
            )

        if not use_correlate_except:
            Common.num_superclass = column_property(
                select(func.count(Superclass.id))
                .where(Superclass.common_id == Common.id)
                .correlate(Common)
                .scalar_subquery()
            )

        return Common, Superclass

    def test_poly_query_on_correlate(self):
        Common, Superclass = self._fixture(False)

        with expect_noload_deprecation():
            poly = with_polymorphic(Superclass, "*")

        s = fixture_session()
        q = (
            s.query(poly)
            .options(contains_eager(poly.common_relationship))
            .join(poly.common_relationship)
            .filter(Common.id == 1)
        )

        # note the order of c.id, subquery changes based on if we
        # used correlate or correlate_except; this is only with the
        # patch in place.   Not sure why this happens.
        self.assert_compile(
            q,
            "SELECT c.id AS c_id, (SELECT count(s1.id) AS count_1 "
            "FROM s1 LEFT OUTER JOIN s2 ON s1.id = s2.id "
            "WHERE s1.common_id = c.id) AS anon_1, "
            "s1.id AS s1_id, "
            "s1.common_id AS s1_common_id, "
            "s1.discriminator_field AS s1_discriminator_field, "
            "s2.id AS s2_id FROM s1 "
            "LEFT OUTER JOIN s2 ON s1.id = s2.id "
            "JOIN c ON c.id = s1.common_id WHERE c.id = :id_1",
        )

    def test_poly_query_on_correlate_except(self):
        Common, Superclass = self._fixture(True)

        with expect_noload_deprecation():
            poly = with_polymorphic(Superclass, "*")

        s = fixture_session()
        q = (
            s.query(poly)
            .options(contains_eager(poly.common_relationship))
            .join(poly.common_relationship)
            .filter(Common.id == 1)
        )

        self.assert_compile(
            q,
            "SELECT c.id AS c_id, (SELECT count(s1.id) AS count_1 "
            "FROM s1 LEFT OUTER JOIN s2 ON s1.id = s2.id "
            "WHERE s1.common_id = c.id) AS anon_1, "
            "s1.id AS s1_id, "
            "s1.common_id AS s1_common_id, "
            "s1.discriminator_field AS s1_discriminator_field, "
            "s2.id AS s2_id FROM s1 "
            "LEFT OUTER JOIN s2 ON s1.id = s2.id "
            "JOIN c ON c.id = s1.common_id WHERE c.id = :id_1",
        )


class Issue8168Test(AssertsCompiledSQL, fixtures.TestBase):
    """tests for #8168 which was fixed by #8456"""

    __dialect__ = "default"

    @testing.fixture
    def mapping(self, decl_base):
        Base = decl_base

        def go(scenario, use_poly, use_poly_on_retailer):
            class Customer(Base):
                __tablename__ = "customer"
                id = Column(Integer, primary_key=True)
                type = Column(String(20))

                __mapper_args__ = {
                    "polymorphic_on": "type",
                    "polymorphic_identity": "customer",
                }

            class Store(Customer):
                __tablename__ = "store"
                id = Column(
                    Integer, ForeignKey("customer.id"), primary_key=True
                )
                retailer_id = Column(Integer, ForeignKey("retailer.id"))
                retailer = relationship(
                    "Retailer",
                    back_populates="stores",
                    foreign_keys=[retailer_id],
                )

                __mapper_args__ = {
                    "polymorphic_identity": "store",
                    "polymorphic_load": "inline" if use_poly else None,
                }

            class Retailer(Customer):
                __tablename__ = "retailer"
                id = Column(
                    Integer, ForeignKey("customer.id"), primary_key=True
                )
                stores = relationship(
                    "Store",
                    back_populates="retailer",
                    foreign_keys=[Store.retailer_id],
                )

                if scenario.mapped_cls:
                    store_tgt = corr_except = Store

                elif scenario.table:
                    corr_except = Store.__table__
                    store_tgt = Store.__table__.c
                elif scenario.table_alias:
                    corr_except = Store.__table__.alias()
                    store_tgt = corr_except.c
                else:
                    scenario.fail()

                store_count = column_property(
                    select(func.count(store_tgt.id))
                    .where(store_tgt.retailer_id == id)
                    .correlate_except(corr_except)
                    .scalar_subquery()
                )

                __mapper_args__ = {
                    "polymorphic_identity": "retailer",
                    "polymorphic_load": (
                        "inline" if use_poly_on_retailer else None
                    ),
                }

            return Customer, Store, Retailer

        yield go

    @testing.variation("scenario", ["mapped_cls", "table", "table_alias"])
    @testing.variation("use_poly", [True, False])
    @testing.variation("use_poly_on_retailer", [True, False])
    def test_select_attr_only(
        self, scenario, use_poly, use_poly_on_retailer, mapping
    ):
        Customer, Store, Retailer = mapping(
            scenario, use_poly, use_poly_on_retailer
        )

        if scenario.mapped_cls:
            self.assert_compile(
                select(Retailer.store_count).select_from(Retailer),
                "SELECT (SELECT count(store.id) AS count_1 "
                "FROM customer JOIN store ON customer.id = store.id "
                "WHERE store.retailer_id = retailer.id) AS anon_1 "
                "FROM customer JOIN retailer ON customer.id = retailer.id",
            )
        elif scenario.table:
            self.assert_compile(
                select(Retailer.store_count).select_from(Retailer),
                "SELECT (SELECT count(store.id) AS count_1 "
                "FROM store "
                "WHERE store.retailer_id = retailer.id) AS anon_1 "
                "FROM customer JOIN retailer ON customer.id = retailer.id",
            )
        elif scenario.table_alias:
            self.assert_compile(
                select(Retailer.store_count).select_from(Retailer),
                "SELECT (SELECT count(store_1.id) AS count_1 FROM store "
                "AS store_1 "
                "WHERE store_1.retailer_id = retailer.id) AS anon_1 "
                "FROM customer JOIN retailer ON customer.id = retailer.id",
            )
        else:
            scenario.fail()

    @testing.variation("scenario", ["mapped_cls", "table", "table_alias"])
    @testing.variation("use_poly", [True, False])
    @testing.variation("use_poly_on_retailer", [True, False])
    def test_select_cls(
        self, scenario, mapping, use_poly, use_poly_on_retailer
    ):
        Customer, Store, Retailer = mapping(
            scenario, use_poly, use_poly_on_retailer
        )

        if scenario.mapped_cls:
            self.assert_compile(
                select(Retailer),
                "SELECT (SELECT count(store.id) AS count_1 FROM customer "
                "JOIN store ON customer.id = store.id "
                "WHERE store.retailer_id = retailer.id) AS anon_1, "
                "retailer.id, customer.id AS id_1, customer.type "
                "FROM customer JOIN retailer ON customer.id = retailer.id",
            )
        elif scenario.table:
            self.assert_compile(
                select(Retailer),
                "SELECT (SELECT count(store.id) AS count_1 FROM store "
                "WHERE store.retailer_id = retailer.id) AS anon_1, "
                "retailer.id, customer.id AS id_1, customer.type "
                "FROM customer JOIN retailer ON customer.id = retailer.id",
            )
        elif scenario.table_alias:
            self.assert_compile(
                select(Retailer),
                "SELECT (SELECT count(store_1.id) AS count_1 "
                "FROM store AS store_1 WHERE store_1.retailer_id = "
                "retailer.id) AS anon_1, retailer.id, customer.id AS id_1, "
                "customer.type "
                "FROM customer JOIN retailer ON customer.id = retailer.id",
            )
        else:
            scenario.fail()


class PolyIntoSelfReferentialTest(
    fixtures.DeclarativeMappedTest, AssertsExecutionResults
):
    """test for #9715"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(
                primary_key=True, autoincrement=True
            )

            rel_id: Mapped[int] = mapped_column(ForeignKey("related.id"))

            related = relationship("Related")

        class Related(Base):
            __tablename__ = "related"

            id: Mapped[int] = mapped_column(
                primary_key=True, autoincrement=True
            )
            rel_data: Mapped[str]
            type: Mapped[str] = mapped_column()

            other_related_id: Mapped[int] = mapped_column(
                ForeignKey("other_related.id")
            )

            other_related = relationship("OtherRelated")

            __mapper_args__ = {
                "polymorphic_identity": "related",
                "polymorphic_on": type,
            }

        class SubRelated(Related):
            __tablename__ = "sub_related"

            id: Mapped[int] = mapped_column(
                ForeignKey("related.id"), primary_key=True
            )
            sub_rel_data: Mapped[str]

            __mapper_args__ = {"polymorphic_identity": "sub_related"}

        class OtherRelated(Base):
            __tablename__ = "other_related"

            id: Mapped[int] = mapped_column(
                primary_key=True, autoincrement=True
            )
            name: Mapped[str]

            parent_id: Mapped[Optional[int]] = mapped_column(
                ForeignKey("other_related.id")
            )
            parent = relationship("OtherRelated", lazy="raise", remote_side=id)

    @classmethod
    def insert_data(cls, connection):
        A, SubRelated, OtherRelated = cls.classes(
            "A", "SubRelated", "OtherRelated"
        )

        with Session(connection) as sess:
            grandparent_otherrel1 = OtherRelated(name="GP1")
            grandparent_otherrel2 = OtherRelated(name="GP2")

            parent_otherrel1 = OtherRelated(
                name="P1", parent=grandparent_otherrel1
            )
            parent_otherrel2 = OtherRelated(
                name="P2", parent=grandparent_otherrel2
            )

            otherrel1 = OtherRelated(name="A1", parent=parent_otherrel1)
            otherrel3 = OtherRelated(name="A2", parent=parent_otherrel2)

            address1 = SubRelated(
                rel_data="ST1", other_related=otherrel1, sub_rel_data="w1"
            )
            address3 = SubRelated(
                rel_data="ST2", other_related=otherrel3, sub_rel_data="w2"
            )

            a1 = A(related=address1)
            a2 = A(related=address3)

            sess.add_all([a1, a2])
            sess.commit()

    def _run_load(self, *opt):
        A = self.classes.A
        stmt = select(A).options(*opt)

        sess = fixture_session()
        all_a = sess.scalars(stmt).all()

        sess.close()

        with self.assert_statement_count(testing.db, 0):
            for a1 in all_a:
                d1 = a1.related
                d2 = d1.other_related
                d3 = d2.parent
                d4 = d3.parent
                assert d4.name in ("GP1", "GP2")

    @testing.variation("use_workaround", [True, False])
    def test_workaround(self, use_workaround):
        A, Related, SubRelated, OtherRelated = self.classes(
            "A", "Related", "SubRelated", "OtherRelated"
        )

        related = with_polymorphic(Related, [SubRelated], flat=True)

        opt = [
            (
                joinedload(A.related.of_type(related))
                .joinedload(related.other_related)
                .joinedload(
                    OtherRelated.parent,
                )
            )
        ]
        if use_workaround:
            opt.append(
                joinedload(
                    A.related,
                    Related.other_related,
                    OtherRelated.parent,
                    OtherRelated.parent,
                )
            )
        else:
            opt[0] = opt[0].joinedload(OtherRelated.parent)

        self._run_load(*opt)

    @testing.combinations(
        (("joined", "joined", "joined", "joined"),),
        (("selectin", "selectin", "selectin", "selectin"),),
        (("selectin", "selectin", "joined", "joined"),),
        (("selectin", "selectin", "joined", "selectin"),),
        (("joined", "selectin", "joined", "selectin"),),
        # TODO: immediateload (and lazyload) do not support the target item
        # being a with_polymorphic.  this seems to be a limitation in the
        # current_path logic
        # (("immediate", "joined", "joined", "joined"),),
        argnames="loaders",
    )
    @testing.variation("use_wpoly", [True, False])
    def test_all_load(self, loaders, use_wpoly):
        A, Related, SubRelated, OtherRelated = self.classes(
            "A", "Related", "SubRelated", "OtherRelated"
        )

        if use_wpoly:
            related = with_polymorphic(Related, [SubRelated], flat=True)
        else:
            related = SubRelated

        opt = None
        for i, (load_type, element) in enumerate(
            zip(
                loaders,
                [
                    A.related.of_type(related),
                    related.other_related,
                    OtherRelated.parent,
                    OtherRelated.parent,
                ],
            )
        ):
            if i == 0:
                if load_type == "joined":
                    opt = joinedload(element)
                elif load_type == "selectin":
                    opt = selectinload(element)
                elif load_type == "immediate":
                    opt = immediateload(element)
                else:
                    assert False
            else:
                assert opt is not None
                if load_type == "joined":
                    opt = opt.joinedload(element)
                elif load_type == "selectin":
                    opt = opt.selectinload(element)
                elif load_type == "immediate":
                    opt = opt.immediateload(element)
                else:
                    assert False

        self._run_load(opt)


class AdaptExistsSubqTest(fixtures.DeclarativeMappedTest):
    """test for #9777"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Discriminator(Base):
            __tablename__ = "discriminator"
            id = Column(Integer, primary_key=True, autoincrement=False)
            value = Column(String(50))

        class Entity(Base):
            __tablename__ = "entity"
            __mapper_args__ = {"polymorphic_on": "type"}

            id = Column(Integer, primary_key=True, autoincrement=False)
            type = Column(String(50))

            discriminator_id = Column(
                ForeignKey("discriminator.id"), nullable=False
            )
            discriminator = relationship(
                "Discriminator", foreign_keys=discriminator_id
            )

        class Parent(Entity):
            __tablename__ = "parent"
            __mapper_args__ = {"polymorphic_identity": "parent"}

            id = Column(Integer, ForeignKey("entity.id"), primary_key=True)
            some_data = Column(String(30))

        class Child(Entity):
            __tablename__ = "child"
            __mapper_args__ = {"polymorphic_identity": "child"}

            id = Column(Integer, ForeignKey("entity.id"), primary_key=True)

            some_data = Column(String(30))
            parent_id = Column(ForeignKey("parent.id"), nullable=False)
            parent = relationship(
                "Parent",
                foreign_keys=parent_id,
                backref="children",
            )

    @classmethod
    def insert_data(cls, connection):
        Parent, Child, Discriminator = cls.classes(
            "Parent", "Child", "Discriminator"
        )

        with Session(connection) as sess:
            discriminator_zero = Discriminator(id=1, value="zero")
            discriminator_one = Discriminator(id=2, value="one")
            discriminator_two = Discriminator(id=3, value="two")

            parent = Parent(id=1, discriminator=discriminator_zero)
            child_1 = Child(
                id=2,
                discriminator=discriminator_one,
                parent=parent,
                some_data="c1data",
            )
            child_2 = Child(
                id=3,
                discriminator=discriminator_two,
                parent=parent,
                some_data="c2data",
            )
            sess.add_all([parent, child_1, child_2])
            sess.commit()

    def test_explicit_aliasing(self):
        Parent, Child, Discriminator = self.classes(
            "Parent", "Child", "Discriminator"
        )

        parent_id = 1
        discriminator_one_id = 2

        session = fixture_session()
        c_alias = aliased(Child, flat=True)
        retrieved = (
            session.query(Parent)
            .filter_by(id=parent_id)
            .outerjoin(
                Parent.children.of_type(c_alias).and_(
                    c_alias.discriminator.has(
                        and_(
                            Discriminator.id == discriminator_one_id,
                            c_alias.some_data == "c1data",
                        )
                    )
                )
            )
            .options(contains_eager(Parent.children.of_type(c_alias)))
            .populate_existing()
            .one()
        )
        eq_(len(retrieved.children), 1)

    def test_implicit_aliasing(self):
        Parent, Child, Discriminator = self.classes(
            "Parent", "Child", "Discriminator"
        )

        parent_id = 1
        discriminator_one_id = 2

        session = fixture_session()
        q = (
            session.query(Parent)
            .filter_by(id=parent_id)
            .outerjoin(
                Parent.children.and_(
                    Child.discriminator.has(
                        and_(
                            Discriminator.id == discriminator_one_id,
                            Child.some_data == "c1data",
                        )
                    )
                )
            )
            .options(contains_eager(Parent.children))
            .populate_existing()
        )

        with expect_warnings("An alias is being generated automatically"):
            retrieved = q.one()

        eq_(len(retrieved.children), 1)

    @testing.combinations(joinedload, selectinload, argnames="loader")
    def test_eager_loaders(self, loader):
        Parent, Child, Discriminator = self.classes(
            "Parent", "Child", "Discriminator"
        )

        parent_id = 1
        discriminator_one_id = 2

        session = fixture_session()
        retrieved = (
            session.query(Parent)
            .filter_by(id=parent_id)
            .options(
                loader(
                    Parent.children.and_(
                        Child.discriminator.has(
                            and_(
                                Discriminator.id == discriminator_one_id,
                                Child.some_data == "c1data",
                            )
                        )
                    )
                )
            )
            .populate_existing()
            .one()
        )

        eq_(len(retrieved.children), 1)


@testing.combinations(
    ("single",),
    ("joined",),
    id_="s",
    argnames="inheritance_type",
)
class MultiOfTypeContainsEagerTest(fixtures.DeclarativeMappedTest):
    """test for #10006"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        employee_m2m = Table(
            "employee_m2m",
            Base.metadata,
            Column(
                "left", Integer, ForeignKey("employee.id"), primary_key=True
            ),
            Column(
                "right", Integer, ForeignKey("employee.id"), primary_key=True
            ),
        )

        class Property(ComparableEntity, Base):
            __tablename__ = "property"
            id: Mapped[int] = mapped_column(primary_key=True)
            value: Mapped[str] = mapped_column(name="value")
            user_id: Mapped[int] = mapped_column(ForeignKey("employee.id"))

        class Employee(ComparableEntity, Base):
            __tablename__ = "employee"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str]
            type: Mapped[str]
            prop1 = relationship(Property, lazy="raise", uselist=False)

            colleagues = relationship(
                "Employee",
                secondary=employee_m2m,
                primaryjoin=lambda: Employee.id == employee_m2m.c.left,
                secondaryjoin=lambda: Employee.id == employee_m2m.c.right,
                lazy="raise",
                collection_class=set,
            )

            __mapper_args__ = {
                "polymorphic_on": "type",
                "polymorphic_identity": "employee",
            }

        class Manager(Employee):
            if cls.inheritance_type == "joined":
                __tablename__ = "manager"
                id: Mapped[int] = mapped_column(  # noqa: A001
                    ForeignKey("employee.id"), primary_key=True
                )
            __mapper_args__ = {"polymorphic_identity": "manager"}

        class Engineer(Employee):
            if cls.inheritance_type == "joined":
                __tablename__ = "engineer"
                id: Mapped[int] = mapped_column(  # noqa: A001
                    ForeignKey("employee.id"), primary_key=True
                )
            __mapper_args__ = {"polymorphic_identity": "engineer"}

        class Clerk(Employee):
            if cls.inheritance_type == "joined":
                __tablename__ = "clerk"
                id: Mapped[int] = mapped_column(  # noqa: A001
                    ForeignKey("employee.id"), primary_key=True
                )
            __mapper_args__ = {"polymorphic_identity": "clerk"}

        class UnitHead(Employee):
            if cls.inheritance_type == "joined":
                __tablename__ = "unithead"
                id: Mapped[int] = mapped_column(  # noqa: A001
                    ForeignKey("employee.id"), primary_key=True
                )
            managers = relationship(
                "Manager",
                secondary=employee_m2m,
                primaryjoin=lambda: Employee.id == employee_m2m.c.left,
                secondaryjoin=lambda: (
                    and_(
                        Employee.id == employee_m2m.c.right,
                        Employee.type == "manager",
                    )
                ),
                viewonly=True,
                lazy="raise",
                collection_class=set,
            )
            __mapper_args__ = {"polymorphic_identity": "unithead"}

    @classmethod
    def insert_data(cls, connection):
        UnitHead, Manager, Engineer, Clerk, Property = cls.classes(
            "UnitHead", "Manager", "Engineer", "Clerk", "Property"
        )

        with Session(connection) as sess:
            unithead = UnitHead(
                type="unithead",
                name="unithead1",
                prop1=Property(value="val unithead"),
            )
            manager = Manager(
                type="manager",
                name="manager1",
                prop1=Property(value="val manager"),
            )
            other_manager = Manager(
                type="manager",
                name="manager2",
                prop1=Property(value="val other manager"),
            )
            engineer = Engineer(
                type="engineer",
                name="engineer1",
                prop1=Property(value="val engineer"),
            )
            clerk = Clerk(
                type="clerk", name="clerk1", prop1=Property(value="val clerk")
            )
            unithead.colleagues.update([manager, other_manager])
            manager.colleagues.update([engineer, clerk])
            sess.add_all([unithead, manager, other_manager, engineer, clerk])
            sess.commit()

    @testing.variation("query_type", ["joinedload", "contains_eager"])
    @testing.variation("use_criteria", [True, False])
    def test_big_query(self, query_type, use_criteria):
        Employee, UnitHead, Manager, Engineer, Clerk, Property = self.classes(
            "Employee", "UnitHead", "Manager", "Engineer", "Clerk", "Property"
        )

        if query_type.contains_eager:
            mgr = aliased(Manager)
            clg = aliased(Employee)
            clgs_prop1 = aliased(Property, name="clgs_prop1")

            query = (
                select(UnitHead)
                .options(
                    contains_eager(UnitHead.managers.of_type(mgr))
                    .contains_eager(mgr.colleagues.of_type(clg))
                    .contains_eager(clg.prop1.of_type(clgs_prop1)),
                )
                .outerjoin(UnitHead.managers.of_type(mgr))
                .outerjoin(mgr.colleagues.of_type(clg))
                .outerjoin(clg.prop1.of_type(clgs_prop1))
            )
            if use_criteria:
                ma_prop1 = aliased(Property)
                uhead_prop1 = aliased(Property)
                query = (
                    query.outerjoin(UnitHead.prop1.of_type(uhead_prop1))
                    .outerjoin(mgr.prop1.of_type(ma_prop1))
                    .where(
                        uhead_prop1.value == "val unithead",
                        ma_prop1.value == "val manager",
                        clgs_prop1.value == "val engineer",
                    )
                )
        elif query_type.joinedload:
            if use_criteria:
                query = (
                    select(UnitHead)
                    .options(
                        joinedload(
                            UnitHead.managers.and_(
                                Manager.prop1.has(value="val manager")
                            )
                        )
                        .joinedload(
                            Manager.colleagues.and_(
                                Employee.prop1.has(value="val engineer")
                            )
                        )
                        .joinedload(Employee.prop1),
                    )
                    .where(UnitHead.prop1.has(value="val unithead"))
                )
            else:
                query = select(UnitHead).options(
                    joinedload(UnitHead.managers)
                    .joinedload(Manager.colleagues)
                    .joinedload(Employee.prop1),
                )

        session = fixture_session()
        head = session.scalars(query).unique().one()

        if use_criteria:
            expected_managers = {
                Manager(
                    name="manager1",
                    colleagues={Engineer(name="engineer1", prop1=Property())},
                )
            }
        else:
            expected_managers = {
                Manager(
                    name="manager1",
                    colleagues={
                        Engineer(name="engineer1", prop1=Property()),
                        Clerk(name="clerk1"),
                    },
                ),
                Manager(name="manager2"),
            }
        eq_(
            head,
            UnitHead(managers=expected_managers),
        )


@testing.combinations(
    (2,),
    (3,),
    id_="s",
    argnames="num_levels",
)
@testing.combinations(
    ("with_poly_star",),
    ("inline",),
    ("selectin",),
    ("none",),
    id_="s",
    argnames="wpoly_type",
)
class SubclassWithPolyEagerLoadTest(fixtures.DeclarativeMappedTest):
    """test #11446"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))

        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            type = Column(String(10))
            bs = relationship("B")

            if cls.wpoly_type == "selectin":
                __mapper_args__ = {"polymorphic_on": "type"}
            elif cls.wpoly_type == "inline":
                __mapper_args__ = {"polymorphic_on": "type"}
            elif cls.wpoly_type == "with_poly_star":
                __mapper_args__ = {
                    "with_polymorphic": "*",
                    "polymorphic_on": "type",
                }
            else:
                __mapper_args__ = {"polymorphic_on": "type"}

        class ASub(A):
            __tablename__ = "asub"
            id = Column(ForeignKey("a.id"), primary_key=True)
            sub_data = Column(String(10))

            if cls.wpoly_type == "selectin":
                __mapper_args__ = {
                    "polymorphic_load": "selectin",
                    "polymorphic_identity": "asub",
                }
            elif cls.wpoly_type == "inline":
                __mapper_args__ = {
                    "polymorphic_load": "inline",
                    "polymorphic_identity": "asub",
                }
            elif cls.wpoly_type == "with_poly_star":
                __mapper_args__ = {
                    "with_polymorphic": "*",
                    "polymorphic_identity": "asub",
                }
            else:
                __mapper_args__ = {"polymorphic_identity": "asub"}

        if cls.num_levels == 3:

            class ASubSub(ASub):
                __tablename__ = "asubsub"
                id = Column(ForeignKey("asub.id"), primary_key=True)
                sub_sub_data = Column(String(10))

                if cls.wpoly_type == "selectin":
                    __mapper_args__ = {
                        "polymorphic_load": "selectin",
                        "polymorphic_identity": "asubsub",
                    }
                elif cls.wpoly_type == "inline":
                    __mapper_args__ = {
                        "polymorphic_load": "inline",
                        "polymorphic_identity": "asubsub",
                    }
                elif cls.wpoly_type == "with_poly_star":
                    __mapper_args__ = {
                        "with_polymorphic": "*",
                        "polymorphic_identity": "asubsub",
                    }
                else:
                    __mapper_args__ = {"polymorphic_identity": "asubsub"}

    @classmethod
    def insert_data(cls, connection):
        if cls.num_levels == 3:
            ASubSub, B = cls.classes("ASubSub", "B")

            with Session(connection) as sess:
                sess.add_all(
                    [
                        ASubSub(
                            sub_data="sub",
                            sub_sub_data="subsub",
                            bs=[B(), B(), B()],
                        )
                        for i in range(3)
                    ]
                )

                sess.commit()
        else:
            ASub, B = cls.classes("ASub", "B")

            with Session(connection) as sess:
                sess.add_all(
                    [
                        ASub(sub_data="sub", bs=[B(), B(), B()])
                        for i in range(3)
                    ]
                )
                sess.commit()

    @testing.variation("query_from", ["aliased_class", "class_", "parent"])
    @testing.combinations(selectinload, subqueryload, argnames="loader_fn")
    def test_thing(self, query_from, loader_fn):

        A = self.classes.A

        if self.num_levels == 2:
            target = self.classes.ASub
        elif self.num_levels == 3:
            target = self.classes.ASubSub

        if query_from.aliased_class:
            asub_alias = aliased(target)
            query = select(asub_alias).options(loader_fn(asub_alias.bs))
        elif query_from.class_:
            query = select(target).options(loader_fn(A.bs))
        elif query_from.parent:
            query = select(A).options(loader_fn(A.bs))

        s = fixture_session()

        # NOTE: this is likely a different bug - setting
        # polymorphic_load to "inline" and loading from the parent does not
        # descend to the ASubSub subclass; however "selectin" setting
        # **does**.   this is inconsistent
        if (
            query_from.parent
            and self.wpoly_type == "inline"
            and self.num_levels == 3
        ):
            # this should ideally be "2"
            expected_q = 5

        elif query_from.parent and self.wpoly_type == "none":
            expected_q = 5
        elif query_from.parent and self.wpoly_type == "selectin":
            expected_q = 3
        else:
            expected_q = 2

        with self.assert_statement_count(testing.db, expected_q):
            for obj in s.scalars(query):
                # test both that with_polymorphic loaded
                eq_(obj.sub_data, "sub")
                if self.num_levels == 3:
                    eq_(obj.sub_sub_data, "subsub")

                # as well as the collection eagerly loaded
                assert obj.bs
