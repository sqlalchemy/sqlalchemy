from __future__ import unicode_literals

"""Benchmark for SQLAlchemy.

An adaptation of Robert Brewers' ZooMark speed tests. """


import datetime
from sqlalchemy import Table, Column, Integer, Unicode, Date, \
    DateTime, Time, Float, Sequence, ForeignKey, \
    select, and_, func
from sqlalchemy.orm import mapper
from sqlalchemy.testing import replay_fixture

ITERATIONS = 1

Zoo = Animal = session = None


class ZooMarkTest(replay_fixture.ReplayFixtureTest):

    """Runs the ZooMark and squawks if method counts vary from the norm.


    """

    __requires__ = 'cpython',
    __only_on__ = 'postgresql+psycopg2'

    def _run_steps(self, ctx):
        with ctx():
            self._baseline_1a_populate()
        with ctx():
            self._baseline_2_insert()
        with ctx():
            self._baseline_3_properties()
        with ctx():
            self._baseline_4_expressions()
        with ctx():
            self._baseline_5_aggregates()
        with ctx():
            self._baseline_6_editing()

    def setup_engine(self):
        self._baseline_1_create_tables()

    def teardown_engine(self):
        self._baseline_7_drop()

    def _baseline_1_create_tables(self):
        zoo = Table(
            'Zoo',
            self.metadata,
            Column('ID', Integer, Sequence('zoo_id_seq'),
                   primary_key=True, index=True),
            Column('Name', Unicode(255)),
            Column('Founded', Date),
            Column('Opens', Time),
            Column('LastEscape', DateTime),
            Column('Admission', Float),
        )
        animal = Table(
            'Animal',
            self.metadata,
            Column('ID', Integer, Sequence('animal_id_seq'),
                   primary_key=True),
            Column('ZooID', Integer, ForeignKey('Zoo.ID'), index=True),
            Column('Name', Unicode(100)),
            Column('Species', Unicode(100)),
            Column('Legs', Integer, default=4),
            Column('LastEscape', DateTime),
            Column('Lifespan', Float(4)),
            Column('MotherID', Integer, ForeignKey('Animal.ID')),
            Column('PreferredFoodID', Integer),
            Column('AlternateFoodID', Integer),
        )
        self.metadata.create_all()
        global Zoo, Animal

        class Zoo(object):

            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)

        class Animal(object):

            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)

        mapper(Zoo, zoo)
        mapper(Animal, animal)

    def _baseline_1a_populate(self):
        wap = Zoo(
            Name='Wild Animal Park', Founded=datetime.date(
                2000, 1, 1), Opens=datetime.time(
                8, 15, 59), LastEscape=datetime.datetime(
                2004, 7, 29, 5, 6, 7, ), Admission=4.95)
        self.session.add(wap)
        sdz = Zoo(
            Name='San Diego Zoo', Founded=datetime.date(
                1835, 9, 13), Opens=datetime.time(
                9, 0, 0), Admission=0)
        self.session.add(sdz)
        bio = Zoo(Name='Montr\xe9al Biod\xf4me',
                  Founded=datetime.date(1992, 6, 19),
                  Opens=datetime.time(9, 0, 0), Admission=11.75)
        self.session.add(bio)
        seaworld = Zoo(Name='Sea_World', Admission=60)
        self.session.add(seaworld)

        # Let's add a crazy futuristic Zoo to test large date values.

        lp = Zoo(Name='Luna Park', Founded=datetime.date(2072, 7, 17),
                 Opens=datetime.time(0, 0, 0), Admission=134.95)
        self.session.add(lp)

        # Animals

        leopard = Animal(Species='Leopard', Lifespan=73.5)
        self.session.add(leopard)
        leopard.ZooID = wap.ID
        leopard.LastEscape = \
            datetime.datetime(2004, 12, 21, 8, 15, 0, 999907, )
        self.session.add(Animal(Species='Lion', ZooID=wap.ID))
        self.session.add(Animal(Species='Slug', Legs=1, Lifespan=.75))
        self.session.add(Animal(Species='Tiger', ZooID=sdz.ID))

        # Override Legs.default with itself just to make sure it works.

        self.session.add(Animal(Species='Bear', Legs=4))
        self.session.add(Animal(Species='Ostrich', Legs=2, Lifespan=103.2))
        self.session.add(Animal(Species='Centipede', Legs=100))
        self.session.add(Animal(Species='Emperor Penguin', Legs=2,
                                ZooID=seaworld.ID))
        self.session.add(Animal(Species='Adelie Penguin', Legs=2,
                                ZooID=seaworld.ID))
        self.session.add(Animal(Species='Millipede', Legs=1000000,
                                ZooID=sdz.ID))

        # Add a mother and child to test relationships

        bai_yun = Animal(Species='Ape', Nameu='Bai Yun', Legs=2)
        self.session.add(bai_yun)
        self.session.add(Animal(Species='Ape', Name='Hua Mei', Legs=2,
                                MotherID=bai_yun.ID))
        self.session.commit()

    def _baseline_2_insert(self):
        for x in range(ITERATIONS):
            self.session.add(Animal(Species='Tick', Name='Tick %d' % x,
                                    Legs=8))
        self.session.flush()

    def _baseline_3_properties(self):
        for x in range(ITERATIONS):

            # Zoos

            list(self.session.query(Zoo).filter(
                Zoo.Name == 'Wild Animal Park'))
            list(
                self.session.query(Zoo).filter(
                    Zoo.Founded == datetime.date(
                        1835,
                        9,
                        13)))
            list(
                self.session.query(Zoo).filter(
                    Zoo.Name == 'Montr\xe9al Biod\xf4me'))
            list(self.session.query(Zoo).filter(Zoo.Admission == float(60)))

            # Animals

            list(self.session.query(Animal).filter(
                    Animal.Species == 'Leopard'))
            list(self.session.query(Animal).filter(
                    Animal.Species == 'Ostrich'))
            list(self.session.query(Animal).filter(
                    Animal.Legs == 1000000))
            list(self.session.query(Animal).filter(
                    Animal.Species == 'Tick'))

    def _baseline_4_expressions(self):
        for x in range(ITERATIONS):
            assert len(list(self.session.query(Zoo))) == 5
            assert len(list(self.session.query(Animal))) == ITERATIONS + 12
            assert len(list(self.session.query(Animal)
                            .filter(Animal.Legs == 4))) == 4
            assert len(list(self.session.query(Animal)
                            .filter(Animal.Legs == 2))) == 5
            assert len(
                list(
                    self.session.query(Animal).filter(
                        and_(
                            Animal.Legs >= 2,
                            Animal.Legs < 20)))) == ITERATIONS + 9
            assert len(list(self.session.query(Animal)
                            .filter(Animal.Legs > 10))) == 2
            assert len(list(self.session.query(Animal)
                            .filter(Animal.Lifespan > 70))) == 2
            assert len(list(self.session.query(Animal).
                            filter(Animal.Species.like('L%')))) == 2
            assert len(list(self.session.query(Animal).
                            filter(Animal.Species.like('%pede')))) == 2
            assert len(list(self.session.query(Animal)
                            .filter(Animal.LastEscape != None))) == 1  # noqa
            assert len(
                list(
                    self.session.query(Animal).filter(
                        Animal.LastEscape == None))) == ITERATIONS + 11  # noqa

            # In operator (containedby)

            assert len(list(self.session.query(Animal).filter(
                Animal.Species.like('%pede%')))) == 2
            assert len(
                list(
                    self.session.query(Animal). filter(
                        Animal.Species.in_(
                            ('Lion', 'Tiger', 'Bear'))))) == 3

            # Try In with cell references
            class thing(object):
                pass

            pet, pet2 = thing(), thing()
            pet.Name, pet2.Name = 'Slug', 'Ostrich'
            assert len(list(self.session.query(Animal).
                            filter(Animal.Species.in_((pet.Name,
                                                       pet2.Name))))) == 2

            # logic and other functions

            name = 'Lion'
            assert len(list(self.session.query(Animal).
                            filter(func.length(Animal.Species)
                                   == len(name)))) == ITERATIONS + 3
            assert len(list(self.session.query(Animal).
                            filter(Animal.Species.like('%i%'
                                                       )))) == ITERATIONS + 7

            # Test now(), today(), year(), month(), day()

            assert len(
                list(
                    self.session.query(Zoo).filter(
                        and_(
                            Zoo.Founded != None,  # noqa
                            Zoo.Founded < func.now())))) == 3
            assert len(list(self.session.query(Animal)
                            .filter(Animal.LastEscape == func.now()))) == 0
            assert len(list(self.session.query(Animal).filter(
                func.date_part('year', Animal.LastEscape) == 2004))) == 1
            assert len(
                list(
                    self.session.query(Animal). filter(
                        func.date_part(
                            'month',
                            Animal.LastEscape) == 12))) == 1
            assert len(list(self.session.query(Animal).filter(
                func.date_part('day', Animal.LastEscape) == 21))) == 1

    def _baseline_5_aggregates(self):
        Animal = self.metadata.tables['Animal']
        Zoo = self.metadata.tables['Zoo']

        # TODO: convert to ORM
        engine = self.metadata.bind
        for x in range(ITERATIONS):

            # views

            view = engine.execute(select([Animal.c.Legs])).fetchall()
            legs = sorted([x[0] for x in view])
            expected = {
                'Leopard': 73.5,
                'Slug': .75,
                'Tiger': None,
                'Lion': None,
                'Bear': None,
                'Ostrich': 103.2,
                'Centipede': None,
                'Emperor Penguin': None,
                'Adelie Penguin': None,
                'Millipede': None,
                'Ape': None,
                'Tick': None,
            }
            for species, lifespan in engine.execute(
                    select([Animal.c.Species, Animal.c.Lifespan])).fetchall():
                assert lifespan == expected[species]
            expected = ['Montr\xe9al Biod\xf4me', 'Wild Animal Park']
            e = select([Zoo.c.Name],
                       and_(Zoo.c.Founded != None,  # noqa
                            Zoo.c.Founded <= func.current_timestamp(),
                            Zoo.c.Founded >= datetime.date(1990,
                                                           1,
                                                           1)))
            values = [val[0] for val in engine.execute(e).fetchall()]
            assert set(values) == set(expected)

            # distinct

            legs = [
                x[0]
                for x in engine.execute(
                    select([Animal.c.Legs],
                           distinct=True)).fetchall()]
            legs.sort()

    def _baseline_6_editing(self):
        for x in range(ITERATIONS):

            # Edit

            SDZ = self.session.query(Zoo).filter(Zoo.Name == 'San Diego Zoo') \
                  .one()
            SDZ.Name = 'The San Diego Zoo'
            SDZ.Founded = datetime.date(1900, 1, 1)
            SDZ.Opens = datetime.time(7, 30, 0)
            SDZ.Admission = 35.00

            # Test edits

            SDZ = self.session.query(Zoo) \
                .filter(Zoo.Name == 'The San Diego Zoo').one()
            assert SDZ.Founded == datetime.date(1900, 1, 1), SDZ.Founded

            # Change it back

            SDZ.Name = 'San Diego Zoo'
            SDZ.Founded = datetime.date(1835, 9, 13)
            SDZ.Opens = datetime.time(9, 0, 0)
            SDZ.Admission = 0

            # Test re-edits

            SDZ = self.session.query(Zoo).filter(Zoo.Name == 'San Diego Zoo') \
                .one()
            assert SDZ.Founded == datetime.date(1835, 9, 13), \
                SDZ.Founded

    def _baseline_7_drop(self):
        self.session.rollback()
        self.metadata.drop_all()
