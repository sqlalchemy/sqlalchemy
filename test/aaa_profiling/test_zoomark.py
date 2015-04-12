from __future__ import unicode_literals

"""Benchmark for SQLAlchemy.

An adaptation of Robert Brewers' ZooMark speed tests. """


import datetime
from sqlalchemy import Table, Column, Integer, Unicode, Date, \
    DateTime, Time, Float, Sequence, ForeignKey,  \
    select, join, and_, outerjoin, func
from sqlalchemy.testing import replay_fixture

ITERATIONS = 1


class ZooMarkTest(replay_fixture.ReplayFixtureTest):

    """Runs the ZooMark and squawks if method counts vary from the norm."""

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
        with ctx():
            self._baseline_7_multiview()

    def setup_engine(self):
        self._baseline_1_create_tables()

    def teardown_engine(self):
        self._baseline_8_drop()

    def _baseline_1_create_tables(self):
        Table(
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
        Table(
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

    def _baseline_1a_populate(self):
        Zoo = self.metadata.tables['Zoo']
        Animal = self.metadata.tables['Animal']
        engine = self.metadata.bind
        wap = engine.execute(Zoo.insert(), Name='Wild Animal Park',
                             Founded=datetime.date(2000, 1, 1),
                             Opens=datetime.time(8, 15, 59),
                             LastEscape=datetime.datetime(
                                 2004, 7, 29, 5, 6, 7),
                             Admission=4.95).inserted_primary_key[0]
        sdz = engine.execute(Zoo.insert(), Name='San Diego Zoo',
                             Founded=datetime.date(1935, 9, 13),
                             Opens=datetime.time(9, 0, 0),
                             Admission=0).inserted_primary_key[0]
        engine.execute(Zoo.insert(inline=True), Name='Montr\xe9al Biod\xf4me',
                       Founded=datetime.date(1992, 6, 19),
                       Opens=datetime.time(9, 0, 0), Admission=11.75)
        seaworld = engine.execute(Zoo.insert(), Name='Sea_World',
                                  Admission=60).inserted_primary_key[0]

        # Let's add a crazy futuristic Zoo to test large date values.

        engine.execute(
            Zoo.insert(), Name='Luna Park',
            Founded=datetime.date(2072, 7, 17),
            Opens=datetime.time(0, 0, 0),
            Admission=134.95).inserted_primary_key[0]

        # Animals

        leopardid = engine.execute(Animal.insert(), Species='Leopard',
                                   Lifespan=73.5).inserted_primary_key[0]
        engine.execute(Animal.update(Animal.c.ID == leopardid), ZooID=wap,
                       LastEscape=datetime.datetime(
                           2004, 12, 21, 8, 15, 0, 999907,)
                       )
        engine.execute(
            Animal.insert(),
            Species='Lion', ZooID=wap).inserted_primary_key[0]

        engine.execute(Animal.insert(), Species='Slug', Legs=1, Lifespan=.75)
        engine.execute(Animal.insert(), Species='Tiger',
                       ZooID=sdz).inserted_primary_key[0]

        # Override Legs.default with itself just to make sure it works.

        engine.execute(Animal.insert(inline=True), Species='Bear', Legs=4)
        engine.execute(Animal.insert(inline=True), Species='Ostrich', Legs=2,
                       Lifespan=103.2)
        engine.execute(Animal.insert(inline=True), Species='Centipede',
                       Legs=100)
        engine.execute(Animal.insert(), Species='Emperor Penguin',
                       Legs=2, ZooID=seaworld).inserted_primary_key[0]
        engine.execute(Animal.insert(), Species='Adelie Penguin',
                       Legs=2, ZooID=seaworld).inserted_primary_key[0]
        engine.execute(Animal.insert(inline=True), Species='Millipede',
                       Legs=1000000, ZooID=sdz)

        # Add a mother and child to test relationships

        bai_yun = engine.execute(
            Animal.insert(),
            Species='Ape',
            Name='Bai Yun',
            Legs=2).inserted_primary_key[0]
        engine.execute(Animal.insert(inline=True), Species='Ape',
                       Name='Hua Mei', Legs=2, MotherID=bai_yun)

    def _baseline_2_insert(self):
        Animal = self.metadata.tables['Animal']
        i = Animal.insert(inline=True)
        for x in range(ITERATIONS):
            i.execute(Species='Tick', Name='Tick %d' % x, Legs=8)

    def _baseline_3_properties(self):
        Zoo = self.metadata.tables['Zoo']
        Animal = self.metadata.tables['Animal']
        engine = self.metadata.bind

        def fullobject(select):
            """Iterate over the full result row."""

            return list(engine.execute(select).first())

        for x in range(ITERATIONS):

            # Zoos

            fullobject(Zoo.select(Zoo.c.Name == 'Wild Animal Park'))
            fullobject(Zoo.select(Zoo.c.Founded ==
                       datetime.date(1935, 9, 13)))
            fullobject(Zoo.select(Zoo.c.Name ==
                       'Montr\xe9al Biod\xf4me'))
            fullobject(Zoo.select(Zoo.c.Admission == float(60)))

            # Animals

            fullobject(Animal.select(Animal.c.Species == 'Leopard'))
            fullobject(Animal.select(Animal.c.Species == 'Ostrich'))
            fullobject(Animal.select(Animal.c.Legs == 1000000))
            fullobject(Animal.select(Animal.c.Species == 'Tick'))

    def _baseline_4_expressions(self):
        Zoo = self.metadata.tables['Zoo']
        Animal = self.metadata.tables['Animal']
        engine = self.metadata.bind

        def fulltable(select):
            """Iterate over the full result table."""

            return [list(row) for row in engine.execute(select).fetchall()]

        for x in range(ITERATIONS):
            assert len(fulltable(Zoo.select())) == 5
            assert len(fulltable(Animal.select())) == ITERATIONS + 12
            assert len(fulltable(Animal.select(Animal.c.Legs == 4))) \
                == 4
            assert len(fulltable(Animal.select(Animal.c.Legs == 2))) \
                == 5
            assert len(
                fulltable(
                    Animal.select(
                        and_(
                            Animal.c.Legs >= 2,
                            Animal.c.Legs < 20)))) == ITERATIONS + 9
            assert len(fulltable(Animal.select(Animal.c.Legs > 10))) \
                == 2
            assert len(fulltable(Animal.select(Animal.c.Lifespan
                                               > 70))) == 2
            assert len(fulltable(Animal.select(Animal.c.Species.
                                               startswith('L')))) == 2
            assert len(fulltable(Animal.select(Animal.c.Species.
                                               endswith('pede')))) == 2
            assert len(fulltable(Animal.select(Animal.c.LastEscape
                                               != None))) == 1
            assert len(
                fulltable(
                    Animal.select(
                        None == Animal.c.LastEscape))) == ITERATIONS + 11

            # In operator (containedby)

            assert len(fulltable(Animal.select(Animal.c.Species.like('%pede%'
                                                                     )))) == 2
            assert len(
                fulltable(
                    Animal.select(
                        Animal.c.Species.in_(
                            ['Lion', 'Tiger', 'Bear'])))) == 3

            # Try In with cell references
            class thing(object):
                pass

            pet, pet2 = thing(), thing()
            pet.Name, pet2.Name = 'Slug', 'Ostrich'
            assert len(
                fulltable(
                    Animal.select(
                        Animal.c.Species.in_([pet.Name, pet2.Name])))) == 2

            # logic and other functions

            assert len(fulltable(Animal.select(Animal.c.Species.like('Slug'
                                                                     )))) == 1
            assert len(fulltable(Animal.select(Animal.c.Species.like('%pede%'
                                                                     )))) == 2
            name = 'Lion'
            assert len(
                fulltable(
                    Animal.select(
                        func.length(
                            Animal.c.Species) == len(name)))) == ITERATIONS + 3
            assert len(
                fulltable(
                    Animal.select(
                        Animal.c.Species.like('%i%')))) == ITERATIONS + 7

            # Test now(), today(), year(), month(), day()

            assert len(
                fulltable(
                    Zoo.select(
                        and_(
                            Zoo.c.Founded != None,
                            Zoo.c.Founded < func.current_timestamp(
                                _type=Date))))) == 3
            assert len(
                fulltable(
                    Animal.select(
                        Animal.c.LastEscape == func.current_timestamp(
                            _type=Date)))) == 0
            assert len(
                fulltable(
                    Animal.select(
                        func.date_part(
                            'year',
                            Animal.c.LastEscape) == 2004))) == 1
            assert len(
                fulltable(
                    Animal.select(
                        func.date_part(
                            'month',
                            Animal.c.LastEscape) == 12))) == 1
            assert len(
                fulltable(
                    Animal.select(
                        func.date_part(
                            'day',
                            Animal.c.LastEscape) == 21))) == 1

    def _baseline_5_aggregates(self):
        Animal = self.metadata.tables['Animal']
        Zoo = self.metadata.tables['Zoo']
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
                       and_(Zoo.c.Founded != None,
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
        Zoo = self.metadata.tables['Zoo']
        engine = self.metadata.bind
        for x in range(ITERATIONS):

            # Edit

            SDZ = engine.execute(Zoo.select(Zoo.c.Name == 'San Diego Zoo'
                                            )).first()
            engine.execute(
                Zoo.update(
                    Zoo.c.ID == SDZ['ID']),
                Name='The San Diego Zoo',
                Founded=datetime.date(1900, 1, 1),
                Opens=datetime.time(7, 30, 0), Admission='35.00')

            # Test edits

            SDZ = engine.execute(Zoo.select(Zoo.c.Name == 'The San Diego Zoo'
                                            )).first()
            assert SDZ['Founded'] == datetime.date(1900, 1, 1), \
                SDZ['Founded']

            # Change it back

            engine.execute(Zoo.update(Zoo.c.ID == SDZ['ID'
                                                      ]), Name='San Diego Zoo',
                           Founded=datetime.date(1935, 9, 13),
                           Opens=datetime.time(9, 0, 0),
                           Admission='0')

            # Test re-edits

            SDZ = engine.execute(Zoo.select(Zoo.c.Name == 'San Diego Zoo'
                                            )).first()
            assert SDZ['Founded'] == datetime.date(1935, 9, 13)

    def _baseline_7_multiview(self):
        Zoo = self.metadata.tables['Zoo']
        Animal = self.metadata.tables['Animal']
        engine = self.metadata.bind

        def fulltable(select):
            """Iterate over the full result table."""

            return [list(row) for row in engine.execute(select).fetchall()]

        for x in range(ITERATIONS):
            fulltable(
                select(
                    [Zoo.c.ID] + list(Animal.c),
                    Zoo.c.Name == 'San Diego Zoo',
                    from_obj=[join(Zoo, Animal)]))
            Zoo.select(Zoo.c.Name == 'San Diego Zoo')
            fulltable(
                select(
                    [Zoo.c.ID, Animal.c.ID],
                    and_(
                        Zoo.c.Name == 'San Diego Zoo',
                        Animal.c.Species == 'Leopard'
                    ),
                    from_obj=[join(Zoo, Animal)])
            )

            # Now try the same query with INNER, LEFT, and RIGHT JOINs.

            fulltable(select([
                Zoo.c.Name, Animal.c.Species],
                from_obj=[join(Zoo, Animal)]))
            fulltable(select([
                Zoo.c.Name, Animal.c.Species],
                from_obj=[outerjoin(Zoo, Animal)]))
            fulltable(select([
                Zoo.c.Name, Animal.c.Species],
                from_obj=[outerjoin(Animal, Zoo)]))

    def _baseline_8_drop(self):
        self.metadata.drop_all()
