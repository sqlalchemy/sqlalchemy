"""Benchmark for SQLAlchemy.

An adaptation of Robert Brewers' ZooMark speed tests. """


import datetime
import sys
import time
from sqlalchemy import *
from sqlalchemy.orm import *
from test.lib import *
ITERATIONS = 1
dbapi_session = engines.ReplayableSession()
metadata = None


class ZooMarkTest(fixtures.TestBase):

    """Runs the ZooMark and squawks if method counts vary from the norm.

    Each test has an associated `call_range`, the total number of
    accepted function calls made during the test.  The count can vary
    between Python 2.4 and 2.5.

    Unlike a unit test, this is a ordered collection of steps.  Running
    components individually will fail.

    """

    __requires__ = 'cpython',
    __only_on__ = 'postgresql+psycopg2'
    __skip_if__ = lambda : sys.version_info < (2, 5),

    def test_baseline_0_setup(self):
        global metadata, session
        creator = testing.db.pool._creator
        recorder = lambda : dbapi_session.recorder(creator())
        engine = engines.testing_engine(options={'creator': recorder, 'use_reaper':False})
        metadata = MetaData(engine)
        session = sessionmaker(engine)()
        engine.connect()

    def test_baseline_1_create_tables(self):
        zoo = Table(
            'Zoo',
            metadata,
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
            metadata,
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
        metadata.create_all()
        global Zoo, Animal


        class Zoo(object):

            def __init__(self, **kwargs):
                for k, v in kwargs.iteritems():
                    setattr(self, k, v)


        class Animal(object):

            def __init__(self, **kwargs):
                for k, v in kwargs.iteritems():
                    setattr(self, k, v)


        mapper(Zoo, zoo)
        mapper(Animal, animal)

    def test_baseline_1a_populate(self):
        wap = Zoo(Name=u'Wild Animal Park', Founded=datetime.date(2000,
                  1, 1), Opens=datetime.time(8, 15, 59),
                  LastEscape=datetime.datetime( 2004, 7, 29, 5, 6, 7, ),
                  Admission=4.95)
        session.add(wap)
        sdz = Zoo(Name=u'San Diego Zoo', Founded=datetime.date(1835, 9,
                  13), Opens=datetime.time(9, 0, 0), Admission=0)
        session.add(sdz)
        bio = Zoo(Name=u'Montr\xe9al Biod\xf4me',
                  Founded=datetime.date(1992, 6, 19),
                  Opens=datetime.time(9, 0, 0), Admission=11.75)
        session.add(bio)
        seaworld = Zoo(Name=u'Sea_World', Admission=60)
        session.add(seaworld)

        # Let's add a crazy futuristic Zoo to test large date values.

        lp = Zoo(Name=u'Luna Park', Founded=datetime.date(2072, 7, 17),
                 Opens=datetime.time(0, 0, 0), Admission=134.95)
        session.add(lp)
        session.flush()

        # Animals

        leopard = Animal(Species=u'Leopard', Lifespan=73.5)
        session.add(leopard)
        leopard.ZooID = wap.ID
        leopard.LastEscape = \
                datetime.datetime(2004, 12, 21, 8, 15, 0, 999907, )
        session.add(Animal(Species=u'Lion', ZooID=wap.ID))
        session.add(Animal(Species=u'Slug', Legs=1, Lifespan=.75))
        session.add(Animal(Species=u'Tiger', ZooID=sdz.ID))

        # Override Legs.default with itself just to make sure it works.

        session.add(Animal(Species=u'Bear', Legs=4))
        session.add(Animal(Species=u'Ostrich', Legs=2, Lifespan=103.2))
        session.add(Animal(Species=u'Centipede', Legs=100))
        session.add(Animal(Species=u'Emperor Penguin', Legs=2,
                    ZooID=seaworld.ID))
        session.add(Animal(Species=u'Adelie Penguin', Legs=2,
                    ZooID=seaworld.ID))
        session.add(Animal(Species=u'Millipede', Legs=1000000,
                    ZooID=sdz.ID))

        # Add a mother and child to test relationships

        bai_yun = Animal(Species=u'Ape', Nameu=u'Bai Yun', Legs=2)
        session.add(bai_yun)
        session.add(Animal(Species=u'Ape', Name=u'Hua Mei', Legs=2,
                    MotherID=bai_yun.ID))
        session.flush()
        session.commit()

    def test_baseline_2_insert(self):
        for x in xrange(ITERATIONS):
            session.add(Animal(Species=u'Tick', Name=u'Tick %d' % x,
                        Legs=8))
        session.flush()

    def test_baseline_3_properties(self):
        for x in xrange(ITERATIONS):

            # Zoos

            WAP = list(session.query(Zoo).filter(Zoo.Name
                       == u'Wild Animal Park'))
            SDZ = list(session.query(Zoo).filter(Zoo.Founded
                       == datetime.date(1835, 9, 13)))
            Biodome = list(session.query(Zoo).filter(Zoo.Name
                           == u'Montr\xe9al Biod\xf4me'))
            seaworld = list(session.query(Zoo).filter(Zoo.Admission
                            == float(60)))

            # Animals

            leopard = list(session.query(Animal).filter(Animal.Species
                           == u'Leopard'))
            ostrich = list(session.query(Animal).filter(Animal.Species
                           == u'Ostrich'))
            millipede = list(session.query(Animal).filter(Animal.Legs
                             == 1000000))
            ticks = list(session.query(Animal).filter(Animal.Species
                         == u'Tick'))

    def test_baseline_4_expressions(self):
        for x in xrange(ITERATIONS):
            assert len(list(session.query(Zoo))) == 5
            assert len(list(session.query(Animal))) == ITERATIONS + 12
            assert len(list(session.query(Animal).filter(Animal.Legs
                       == 4))) == 4
            assert len(list(session.query(Animal).filter(Animal.Legs
                       == 2))) == 5
            assert len(list(session.query(Animal).filter(and_(Animal.Legs
                       >= 2, Animal.Legs < 20)))) == ITERATIONS + 9
            assert len(list(session.query(Animal).filter(Animal.Legs
                       > 10))) == 2
            assert len(list(session.query(Animal).filter(Animal.Lifespan
                       > 70))) == 2
            assert len(list(session.query(Animal).
                        filter(Animal.Species.like(u'L%')))) == 2
            assert len(list(session.query(Animal).
                        filter(Animal.Species.like(u'%pede')))) == 2
            assert len(list(session.query(Animal).filter(Animal.LastEscape
                       != None))) == 1
            assert len(list(session.query(Animal).filter(Animal.LastEscape
                       == None))) == ITERATIONS + 11

            # In operator (containedby)

            assert len(list(session.query(Animal).filter(
                    Animal.Species.like(u'%pede%')))) == 2
            assert len(list(session.query(Animal).
                    filter(Animal.Species.in_((u'Lion'
                       , u'Tiger', u'Bear'))))) == 3

            # Try In with cell references
            class thing(object):
                pass

            pet, pet2 = thing(), thing()
            pet.Name, pet2.Name = u'Slug', u'Ostrich'
            assert len(list(session.query(Animal).
                    filter(Animal.Species.in_((pet.Name,
                       pet2.Name))))) == 2

            # logic and other functions

            name = u'Lion'
            assert len(list(session.query(Animal).
                    filter(func.length(Animal.Species)
                       == len(name)))) == ITERATIONS + 3
            assert len(list(session.query(Animal).
                    filter(Animal.Species.like(u'%i%'
                       )))) == ITERATIONS + 7

            # Test now(), today(), year(), month(), day()

            assert len(list(session.query(Zoo).filter(and_(Zoo.Founded
                       != None, Zoo.Founded < func.now())))) == 3
            assert len(list(session.query(Animal).filter(Animal.LastEscape
                       == func.now()))) == 0
            assert len(list(session.query(Animal).filter(func.date_part('year'
                       , Animal.LastEscape) == 2004))) == 1
            assert len(list(session.query(Animal).
                    filter(func.date_part('month'
                       , Animal.LastEscape) == 12))) == 1
            assert len(list(session.query(Animal).filter(func.date_part('day'
                       , Animal.LastEscape) == 21))) == 1

    def test_baseline_5_aggregates(self):
        Animal = metadata.tables['Animal']
        Zoo = metadata.tables['Zoo']

        # TODO: convert to ORM
        engine = metadata.bind
        for x in xrange(ITERATIONS):

            # views

            view = engine.execute(select([Animal.c.Legs])).fetchall()
            legs = [x[0] for x in view]
            legs.sort()
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
            for species, lifespan in engine.execute(select([Animal.c.Species,
                    Animal.c.Lifespan])).fetchall():
                assert lifespan == expected[species]
            expected = [u'Montr\xe9al Biod\xf4me', 'Wild Animal Park']
            e = select([Zoo.c.Name], and_(Zoo.c.Founded != None,
                       Zoo.c.Founded <= func.current_timestamp(),
                       Zoo.c.Founded >= datetime.date(1990, 1, 1)))
            values = [val[0] for val in engine.execute(e).fetchall()]
            assert set(values) == set(expected)

            # distinct

            legs = [x[0] for x in engine.execute(select([Animal.c.Legs],
                    distinct=True)).fetchall()]
            legs.sort()

    def test_baseline_6_editing(self):
        for x in xrange(ITERATIONS):

            # Edit

            SDZ = session.query(Zoo).filter(Zoo.Name == u'San Diego Zoo'
                    ).one()
            SDZ.Name = u'The San Diego Zoo'
            SDZ.Founded = datetime.date(1900, 1, 1)
            SDZ.Opens = datetime.time(7, 30, 0)
            SDZ.Admission = 35.00

            # Test edits

            SDZ = session.query(Zoo).filter(Zoo.Name
                    == u'The San Diego Zoo').one()
            assert SDZ.Founded == datetime.date(1900, 1, 1), SDZ.Founded

            # Change it back

            SDZ.Name = u'San Diego Zoo'
            SDZ.Founded = datetime.date(1835, 9, 13)
            SDZ.Opens = datetime.time(9, 0, 0)
            SDZ.Admission = 0

            # Test re-edits

            SDZ = session.query(Zoo).filter(Zoo.Name == u'San Diego Zoo'
                    ).one()
            assert SDZ.Founded == datetime.date(1835, 9, 13), \
                SDZ.Founded

    def test_baseline_7_drop(self):
        session.rollback()
        metadata.drop_all()

    # Now, run all of these tests again with the DB-API driver factored
    # out: the ReplayableSession playback stands in for the database.
    #
    # How awkward is this in a unittest framework?  Very.

    def test_profile_0(self):
        global metadata, session
        player = lambda : dbapi_session.player()
        engine = create_engine('postgresql:///', creator=player)
        metadata = MetaData(engine)
        session = sessionmaker(engine)()
        engine.connect()

    @profiling.function_call_count(5600)
    def test_profile_1_create_tables(self):
        self.test_baseline_1_create_tables()

    @profiling.function_call_count(5786, {'2.7+cextension':5683, 
                                            '2.6+cextension':5992})
    def test_profile_1a_populate(self):
        self.test_baseline_1a_populate()

    @profiling.function_call_count(393, {'3.2':360})
    def test_profile_2_insert(self):
        self.test_baseline_2_insert()

    # this number...

    @profiling.function_call_count(6783, {
        '2.6': 6058,
        '2.7': 5922,
        '2.7+cextension': 5714,
        '2.6+cextension': 5714,
        '3.2':5787,
        })
    def test_profile_3_properties(self):
        self.test_baseline_3_properties()

    # and this number go down slightly when using the C extensions

    @profiling.function_call_count(17698, {'2.7+cextension':17698, '2.6': 18943, '2.7':19110})
    def test_profile_4_expressions(self):
        self.test_baseline_4_expressions()

    @profiling.function_call_count(1172, {'2.6+cextension': 1090,
                                   '2.7+cextension': 1086},
                                   variance=0.1)
    def test_profile_5_aggregates(self):
        self.test_baseline_5_aggregates()

    @profiling.function_call_count(2545)
    def test_profile_6_editing(self):
        self.test_baseline_6_editing()

    def test_profile_7_drop(self):
        self.test_baseline_7_drop()
