import datetime, os
from sqlalchemy import *
from sqlalchemy import sql
from sqlalchemy.orm import *
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.sql import operators
from sqlalchemy.test import *
from sqlalchemy.test.testing import eq_
from nose import SkipTest

# TODO: ShardTest can be turned into a base for further subclasses

class ShardTest(TestBase):
    @classmethod
    def setup_class(cls):
        global db1, db2, db3, db4, weather_locations, weather_reports

        try:
            db1 = create_engine('sqlite:///shard1.db')
        except ImportError:
            raise SkipTest('Requires sqlite')
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

        weather_reports = Table(
            'weather_reports',
            meta,
            Column('id', Integer, primary_key=True),
            Column('location_id', Integer,
                   ForeignKey('weather_locations.id')),
            Column('temperature', Float),
            Column('report_time', DateTime,
                   default=datetime.datetime.now),
            )

        for db in (db1, db2, db3, db4):
            meta.create_all(db)

        db1.execute(ids.insert(), nextid=1)

        cls.setup_session()
        cls.setup_mappers()

    @classmethod
    def teardown_class(cls):
        for db in (db1, db2, db3, db4):
            db.connect().invalidate()
        for i in range(1,5):
            os.remove("shard%d.db" % i)

    @classmethod
    def setup_session(cls):
        global create_session
        shard_lookup = {
            'North America': 'north_america',
            'Asia': 'asia',
            'Europe': 'europe',
            'South America': 'south_america',
            }

        def shard_chooser(mapper, instance, clause=None):
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
                    if binary.left.shares_lineage(
                            weather_locations.c.continent):
                        if binary.operator == operators.eq:
                            ids.append(shard_lookup[binary.right.value])
                        elif binary.operator == operators.in_op:
                            for bind in binary.right.clauses:
                                ids.append(shard_lookup[bind.value])


            FindContinent().traverse(query._criterion)
            if len(ids) == 0:
                return ['north_america', 'asia', 'europe',
                        'south_america']
            else:
                return ids

        create_session = sessionmaker(class_=ShardedSession,
                autoflush=True, autocommit=False)
        create_session.configure(shards={
            'north_america': db1,
            'asia': db2,
            'europe': db3,
            'south_america': db4,
            }, shard_chooser=shard_chooser, id_chooser=id_chooser,
                query_chooser=query_chooser)


    @classmethod
    def setup_mappers(cls):
        global WeatherLocation, Report

        class WeatherLocation(object):
            def __init__(self, continent, city):
                self.continent = continent
                self.city = city

        class Report(object):
            def __init__(self, temperature):
                self.temperature = temperature

        mapper(WeatherLocation, weather_locations, properties={
            'reports':relationship(Report, backref='location'),
            'city': deferred(weather_locations.c.city),
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
        for c in [
            tokyo,
            newyork,
            toronto,
            london,
            dublin,
            brasilia,
            quito,
            ]:
            sess.add(c)
        sess.commit()
        tokyo.city  # reload 'city' attribute on tokyo
        sess.expunge_all()
        eq_(db2.execute(weather_locations.select()).fetchall(), [(1,
            'Asia', 'Tokyo')])
        eq_(db1.execute(weather_locations.select()).fetchall(), [(2,
            'North America', 'New York'), (3, 'North America', 'Toronto'
            )])
        eq_(sess.execute(weather_locations.select(), shard_id='asia'
            ).fetchall(), [(1, 'Asia', 'Tokyo')])
        t = sess.query(WeatherLocation).get(tokyo.id)
        eq_(t.city, tokyo.city)
        eq_(t.reports[0].temperature, 80.0)
        north_american_cities = \
            sess.query(WeatherLocation).filter(WeatherLocation.continent
                == 'North America')
        eq_(set([c.city for c in north_american_cities]),
            set(['New York', 'Toronto']))
        asia_and_europe = \
            sess.query(WeatherLocation).filter(
                WeatherLocation.continent.in_(['Europe', 'Asia']))
        eq_(set([c.city for c in asia_and_europe]), set(['Tokyo',
            'London', 'Dublin']))

