# adaptation of Robert Brewers' ZooMark speed tests

"""Benchmark for SQLAlchemy."""

import datetime
import sys
import time
import testbase
from testlib import testing, profiling


from sqlalchemy import *


class ZooMarkTest(testing.AssertMixin):
    """Runs the ZooMark and squawks if method counts vary from the norm.

    Each test has an associated `call_range`, the total number of accepted
    function calls made during the test.  The count can vary between Python
    2.4 and 2.5.
    """
    
    @testing.supported('postgres')
    @profiling.profiled('create', call_range=(1500, 1880), always=True)        
    def test_1_create_tables(self):
        global metadata
        metadata = MetaData(testbase.db)
        
        Zoo = Table('Zoo', metadata,
                    Column('ID', Integer, Sequence('zoo_id_seq'), primary_key=True, index=True),
                    Column('Name', Unicode(255)),
                    Column('Founded', Date),
                    Column('Opens', Time),
                    Column('LastEscape', DateTime),
                    Column('Admission', Float),
                    )
        
        Animal = Table('Animal', metadata,
                       Column('ID', Integer, Sequence('animal_id_seq'), primary_key=True),
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
        
    @testing.supported('postgres')
    @profiling.profiled('populate', call_range=(2800, 3700), always=True)
    def test_1a_populate(self):
        Zoo = metadata.tables['Zoo']
        Animal = metadata.tables['Animal']
        
        wap = Zoo.insert().execute(Name='Wild Animal Park',
                           Founded=datetime.date(2000, 1, 1),
                           # 59 can give rounding errors with divmod, which
                           # AdapterFromADO needs to correct.
                           Opens=datetime.time(8, 15, 59),
                           LastEscape=datetime.datetime(2004, 7, 29, 5, 6, 7),
                           Admission=4.95,
                           ).last_inserted_ids()[0]
        
        sdz = Zoo.insert().execute(Name = 'San Diego Zoo',
                           Founded = datetime.date(1935, 9, 13),
                           Opens = datetime.time(9, 0, 0),
                           Admission = 0,
                           ).last_inserted_ids()[0]
        
        Zoo.insert().execute(
                  Name = u'Montr\xe9al Biod\xf4me',
                  Founded = datetime.date(1992, 6, 19),
                  Opens = datetime.time(9, 0, 0),
                  Admission = 11.75,
                  )
        
        seaworld = Zoo.insert().execute(
                Name = 'Sea_World', Admission = 60).last_inserted_ids()[0]
        
        # Let's add a crazy futuristic Zoo to test large date values.
        lp = Zoo.insert().execute(Name = 'Luna Park',
                                  Founded = datetime.date(2072, 7, 17),
                                  Opens = datetime.time(0, 0, 0),
                                  Admission = 134.95,
                                  ).last_inserted_ids()[0]
        
        # Animals
        leopardid = Animal.insert().execute(Species='Leopard', Lifespan=73.5,
                                            ).last_inserted_ids()[0]
        Animal.update(Animal.c.ID==leopardid).execute(ZooID=wap,
                LastEscape=datetime.datetime(2004, 12, 21, 8, 15, 0, 999907))
        
        lion = Animal.insert().execute(Species='Lion', ZooID=wap).last_inserted_ids()[0]
        Animal.insert().execute(Species='Slug', Legs=1, Lifespan=.75)
        
        tiger = Animal.insert().execute(Species='Tiger', ZooID=sdz
                                        ).last_inserted_ids()[0]
        
        # Override Legs.default with itself just to make sure it works.
        Animal.insert().execute(Species='Bear', Legs=4)
        Animal.insert().execute(Species='Ostrich', Legs=2, Lifespan=103.2)
        Animal.insert().execute(Species='Centipede', Legs=100)
        
        emp = Animal.insert().execute(Species='Emperor Penguin', Legs=2,
                                      ZooID=seaworld).last_inserted_ids()[0]
        adelie = Animal.insert().execute(Species='Adelie Penguin', Legs=2,
                                         ZooID=seaworld).last_inserted_ids()[0]
        
        Animal.insert().execute(Species='Millipede', Legs=1000000, ZooID=sdz)
        
        # Add a mother and child to test relationships
        bai_yun = Animal.insert().execute(Species='Ape', Name='Bai Yun',
                                          Legs=2).last_inserted_ids()[0]
        Animal.insert().execute(Species='Ape', Name='Hua Mei', Legs=2,
                                MotherID=bai_yun)
    
    @testing.supported('postgres')
    @profiling.profiled('insert', call_range=(150, 220), always=True)
    def test_2_insert(self):
        Animal = metadata.tables['Animal']
        i = Animal.insert()
        for x in xrange(ITERATIONS):
            tick = i.execute(Species='Tick', Name='Tick %d' % x, Legs=8)
    
    @testing.supported('postgres')
    @profiling.profiled('properties', call_range=(2900, 3330), always=True)
    def test_3_properties(self):
        Zoo = metadata.tables['Zoo']
        Animal = metadata.tables['Animal']
        
        def fullobject(select):
            """Iterate over the full result row."""
            return list(select.execute().fetchone())
        
        for x in xrange(ITERATIONS):
            # Zoos
            WAP = fullobject(Zoo.select(Zoo.c.Name=='Wild Animal Park'))
            SDZ = fullobject(Zoo.select(Zoo.c.Founded==datetime.date(1935, 9, 13)))
            Biodome = fullobject(Zoo.select(Zoo.c.Name==u'Montr\xe9al Biod\xf4me'))
            seaworld = fullobject(Zoo.select(Zoo.c.Admission == float(60)))
            
            # Animals
            leopard = fullobject(Animal.select(Animal.c.Species == 'Leopard'))
            ostrich = fullobject(Animal.select(Animal.c.Species=='Ostrich'))
            millipede = fullobject(Animal.select(Animal.c.Legs==1000000))
            ticks = fullobject(Animal.select(Animal.c.Species=='Tick'))
    
    @testing.supported('postgres')
    @profiling.profiled('expressions', call_range=(10350, 12200), always=True)
    def test_4_expressions(self):
        Zoo = metadata.tables['Zoo']
        Animal = metadata.tables['Animal']
        
        def fulltable(select):
            """Iterate over the full result table."""
            return [list(row) for row in select.execute().fetchall()]
            
        for x in xrange(ITERATIONS):
            assert len(fulltable(Zoo.select())) == 5
            assert len(fulltable(Animal.select())) == ITERATIONS + 12
            assert len(fulltable(Animal.select(Animal.c.Legs==4))) == 4
            assert len(fulltable(Animal.select(Animal.c.Legs == 2))) == 5
            assert len(fulltable(Animal.select(and_(Animal.c.Legs >= 2, Animal.c.Legs < 20)
                                     ))) == ITERATIONS + 9
            assert len(fulltable(Animal.select(Animal.c.Legs > 10))) == 2
            assert len(fulltable(Animal.select(Animal.c.Lifespan > 70))) == 2
            assert len(fulltable(Animal.select(Animal.c.Species.startswith('L')))) == 2
            assert len(fulltable(Animal.select(Animal.c.Species.endswith('pede')))) == 2
            
            assert len(fulltable(Animal.select(Animal.c.LastEscape != None))) == 1
            assert len(fulltable(Animal.select(None == Animal.c.LastEscape
                                     ))) == ITERATIONS + 11
            
            # In operator (containedby)
            assert len(fulltable(Animal.select(Animal.c.Species.like('%pede%')))) == 2
            assert len(fulltable(Animal.select(Animal.c.Species.in_(['Lion', 'Tiger', 'Bear'])))) == 3
            
            # Try In with cell references
            class thing(object): pass
            pet, pet2 = thing(), thing()
            pet.Name, pet2.Name = 'Slug', 'Ostrich'
            assert len(fulltable(Animal.select(Animal.c.Species.in_([pet.Name, pet2.Name])))) == 2
            
            # logic and other functions
            assert len(fulltable(Animal.select(Animal.c.Species.like('Slug')))) == 1
            assert len(fulltable(Animal.select(Animal.c.Species.like('%pede%')))) == 2
            name = 'Lion'
            assert len(fulltable(Animal.select(func.length(Animal.c.Species) == len(name)
                                     ))) == ITERATIONS + 3
            
            assert len(fulltable(Animal.select(Animal.c.Species.like('%i%')
                                     ))) == ITERATIONS + 7
            
            # Test now(), today(), year(), month(), day()
            assert len(fulltable(Zoo.select(Zoo.c.Founded != None
                                  and Zoo.c.Founded < func.current_timestamp(_type=Date)))) == 3
            assert len(fulltable(Animal.select(Animal.c.LastEscape == func.current_timestamp(_type=Date)))) == 0
            assert len(fulltable(Animal.select(func.date_part('year', Animal.c.LastEscape) == 2004))) == 1
            assert len(fulltable(Animal.select(func.date_part('month', Animal.c.LastEscape) == 12))) == 1
            assert len(fulltable(Animal.select(func.date_part('day', Animal.c.LastEscape) == 21))) == 1
    
    @testing.supported('postgres')
    @profiling.profiled('aggregates', call_range=(960, 1170), always=True)
    def test_5_aggregates(self):
        Animal = metadata.tables['Animal']
        Zoo = metadata.tables['Zoo']
        
        for x in xrange(ITERATIONS):
            # views
            view = select([Animal.c.Legs]).execute().fetchall()
            legs = [x[0] for x in view]
            legs.sort()
            
            expected = {'Leopard': 73.5,
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
            for species, lifespan in select([Animal.c.Species, Animal.c.Lifespan]
                                            ).execute().fetchall():
                assert lifespan == expected[species]
            
            expected = [u'Montr\xe9al Biod\xf4me', 'Wild Animal Park']
            e = select([Zoo.c.Name],
                       and_(Zoo.c.Founded != None,
                            Zoo.c.Founded <= func.current_timestamp(),
                            Zoo.c.Founded >= datetime.date(1990, 1, 1)))
            values = [val[0] for val in e.execute().fetchall()]
            assert set(values) == set(expected)
            
            # distinct
            legs = [x[0] for x in
                    select([Animal.c.Legs], distinct=True).execute().fetchall()]
            legs.sort()
    
    @testing.supported('postgres')
    @profiling.profiled('editing', call_range=(1200, 1290), always=True)
    def test_6_editing(self):
        Zoo = metadata.tables['Zoo']
        
        for x in xrange(ITERATIONS):
            # Edit
            SDZ = Zoo.select(Zoo.c.Name=='San Diego Zoo').execute().fetchone()
            Zoo.update(Zoo.c.ID==SDZ['ID']).execute(
                     Name='The San Diego Zoo',
                     Founded = datetime.date(1900, 1, 1),
                     Opens = datetime.time(7, 30, 0),
                     Admission = "35.00")
            
            # Test edits
            SDZ = Zoo.select(Zoo.c.Name=='The San Diego Zoo').execute().fetchone()
            assert SDZ['Founded'] == datetime.date(1900, 1, 1), SDZ['Founded']
            
            # Change it back
            Zoo.update(Zoo.c.ID==SDZ['ID']).execute(
                     Name = 'San Diego Zoo',
                     Founded = datetime.date(1935, 9, 13),
                     Opens = datetime.time(9, 0, 0),
                     Admission = "0")
            
            # Test re-edits
            SDZ = Zoo.select(Zoo.c.Name=='San Diego Zoo').execute().fetchone()
            assert SDZ['Founded'] == datetime.date(1935, 9, 13)
    
    @testing.supported('postgres')
    @profiling.profiled('multiview', call_range=(2300, 2500), always=True)
    def test_7_multiview(self):
        Zoo = metadata.tables['Zoo']
        Animal = metadata.tables['Animal']
        
        def fulltable(select):
            """Iterate over the full result table."""
            return [list(row) for row in select.execute().fetchall()]
        
        for x in xrange(ITERATIONS):
            za = fulltable(select([Zoo.c.ID] + list(Animal.c),
                                  Zoo.c.Name == 'San Diego Zoo',
                                  from_obj = [join(Zoo, Animal)]))
            
            SDZ = Zoo.select(Zoo.c.Name=='San Diego Zoo')
            
            e = fulltable(select([Zoo.c.ID, Animal.c.ID],
                                 and_(Zoo.c.Name=='San Diego Zoo',
                                      Animal.c.Species=='Leopard'),
                                 from_obj = [join(Zoo, Animal)]))
            
            # Now try the same query with INNER, LEFT, and RIGHT JOINs.
            e = fulltable(select([Zoo.c.Name, Animal.c.Species],
                                 from_obj=[join(Zoo, Animal)]))
            e = fulltable(select([Zoo.c.Name, Animal.c.Species],
                                 from_obj=[outerjoin(Zoo, Animal)]))
            e = fulltable(select([Zoo.c.Name, Animal.c.Species],
                                 from_obj=[outerjoin(Animal, Zoo)]))

    @testing.supported('postgres')
    def test_8_drop(self):
        metadata.drop_all()

ITERATIONS = 1

if __name__ == '__main__':
    testbase.main()
