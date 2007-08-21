import testbase
import datetime, os
from sqlalchemy import *
from sqlalchemy import exceptions, sql
from sqlalchemy.orm import *
from sqlalchemy.orm.shard import ShardedSession
from sqlalchemy.sql import operators
from testlib import PersistTest

# TODO: ShardTest can be turned into a base for further subclasses

class ShardTest(PersistTest):
    def setUpAll(self):
        global db1, db2, db3, db4, weather_locations, weather_reports
        
        db1 = create_engine('sqlite:///shard1.db')
        db2 = create_engine('sqlite:///shard2.db')
        db3 = create_engine('sqlite:///shard3.db')
        db4 = create_engine('sqlite:///shard4.db')

        meta = MetaData()
        ids = Table('ids', meta,
            Column('nextid', Integer, nullable=False))

        def id_generator(ctx):
            # in reality, might want to use a separate transaction for this.
            c = db1.connect()
            nextid = c.execute(ids.select(for_update=True)).scalar()
            c.execute(ids.update(values={ids.c.nextid : ids.c.nextid + 1}))
            return nextid

        weather_locations = Table("weather_locations", meta,
                Column('id', Integer, primary_key=True, default=id_generator),
                Column('continent', String(30), nullable=False),
                Column('city', String(50), nullable=False)
            )

        weather_reports = Table("weather_reports", meta,
            Column('id', Integer, primary_key=True),
            Column('location_id', Integer, ForeignKey('weather_locations.id')),
            Column('temperature', Float),
            Column('report_time', DateTime, default=datetime.datetime.now),
        )
        
        for db in (db1, db2, db3, db4):
            meta.create_all(db)
        
        db1.execute(ids.insert(), nextid=1)

        self.setup_session()
        self.setup_mappers()
        
    def tearDownAll(self):
        for db in (db1, db2, db3, db4):
            db.connect().invalidate()        
        for i in range(1,5):
            os.remove("shard%d.db" % i)

    def setup_session(self):
        global create_session

        shard_lookup = {
            'North America':'north_america',
            'Asia':'asia',
            'Europe':'europe',
            'South America':'south_america'
        }
        
        def shard_chooser(mapper, instance):
            if isinstance(instance, WeatherLocation):
                return shard_lookup[instance.continent]
            else:
                return shard_chooser(mapper, instance.location)

        def id_chooser(query, ident):
            return ['north_america', 'asia', 'europe', 'south_america']

        def query_chooser(query):
            ids = []

            class FindContinent(sql.ClauseVisitor):
                def visit_binary(self, binary):
                    if binary.left is weather_locations.c.continent:
                        if binary.operator == operators.eq:
                            ids.append(shard_lookup[binary.right.value])
                        elif binary.operator == operators.in_op:
                            for bind in binary.right.clauses:
                                ids.append(shard_lookup[bind.value])

            FindContinent().traverse(query._criterion)
            if len(ids) == 0:
                return ['north_america', 'asia', 'europe', 'south_america']
            else:
                return ids
        
        create_session = sessionmaker(class_=ShardedSession, autoflush=True, transactional=True)

        create_session.configure(shards={
            'north_america':db1,
            'asia':db2,
            'europe':db3,
            'south_america':db4
        }, shard_chooser=shard_chooser, id_chooser=id_chooser, query_chooser=query_chooser)
        

    def setup_mappers(self):
        global WeatherLocation, Report
        
        class WeatherLocation(object):
            def __init__(self, continent, city):
                self.continent = continent
                self.city = city

        class Report(object):
            def __init__(self, temperature):
                self.temperature = temperature

        mapper(WeatherLocation, weather_locations, properties={
            'reports':relation(Report, backref='location')
        })

        mapper(Report, weather_reports)    

    def test_roundtrip(self):
        tokyo = WeatherLocation('Asia', 'Tokyo')
        newyork = WeatherLocation('North America', 'New York')
        toronto = WeatherLocation('North America', 'Toronto')
        london = WeatherLocation('Europe', 'London')
        dublin = WeatherLocation('Europe', 'Dublin')
        brasilia = WeatherLocation('South America', 'Brasila')
        quito = WeatherLocation('South America', 'Quito')

        tokyo.reports.append(Report(80.0))
        newyork.reports.append(Report(75))
        quito.reports.append(Report(85))

        sess = create_session()
        for c in [tokyo, newyork, toronto, london, dublin, brasilia, quito]:
            sess.save(c)
        sess.commit()

        sess.clear()

        assert db2.execute(weather_locations.select()).fetchall() == [(1, 'Asia', 'Tokyo')]
        assert db1.execute(weather_locations.select()).fetchall() == [(2, 'North America', 'New York'), (3, 'North America', 'Toronto')]
        
        t = sess.query(WeatherLocation).get(tokyo.id)
        assert t.city == tokyo.city
        assert t.reports[0].temperature == 80.0

        north_american_cities = sess.query(WeatherLocation).filter(WeatherLocation.continent == 'North America')
        assert set([c.city for c in north_american_cities]) == set(['New York', 'Toronto'])

        asia_and_europe = sess.query(WeatherLocation).filter(WeatherLocation.continent.in_('Europe', 'Asia'))
        assert set([c.city for c in asia_and_europe]) == set(['Tokyo', 'London', 'Dublin'])



if __name__ == '__main__':
    testbase.main()
    
