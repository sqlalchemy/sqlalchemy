"""a basic example of using the SQLAlchemy Sharding API.
Sharding refers to horizontally scaling data across multiple
databases.

In this example, four sqlite databases will store information about
weather data on a database-per-continent basis.

To set up a sharding system, you need:
    1. multiple databases, each assined a 'shard id'
    2. a function which can return a single shard id, given an instance
    to be saved; this is called "shard_chooser"
    3. a function which can return a list of shard ids which apply to a particular
    instance identifier; this is called "id_chooser".  If it returns all shard ids,
    all shards will be searched.
    4. a function which can return a list of shard ids to try, given a particular 
    Query ("query_chooser").  If it returns all shard ids, all shards will be 
    queried and the results joined together.
"""

# step 1. imports
from sqlalchemy import (create_engine, MetaData, Table, Column, Integer,
    String, ForeignKey, Float, DateTime)
from sqlalchemy.orm import sessionmaker, mapper, relation
from sqlalchemy.orm.shard import ShardedSession
from sqlalchemy.sql import operators
from sqlalchemy import sql
import datetime

# step 2. databases
echo = True
db1 = create_engine('sqlite://', echo=echo)
db2 = create_engine('sqlite://', echo=echo)
db3 = create_engine('sqlite://', echo=echo)
db4 = create_engine('sqlite://', echo=echo)


# step 3. create session function.  this binds the shard ids
# to databases within a ShardedSession and returns it.
create_session = sessionmaker(class_=ShardedSession)

create_session.configure(shards={
    'north_america':db1,
    'asia':db2,
    'europe':db3,
    'south_america':db4
})


# step 4.  table setup.
meta = MetaData()

# we need a way to create identifiers which are unique across all
# databases.  one easy way would be to just use a composite primary key, where one
# value is the shard id.  but here, we'll show something more "generic", an 
# id generation function.  we'll use a simplistic "id table" stored in database
# #1.  Any other method will do just as well; UUID, hilo, application-specific, etc.

ids = Table('ids', meta,
    Column('nextid', Integer, nullable=False))

def id_generator(ctx):
    # in reality, might want to use a separate transaction for this.
    c = db1.connect()
    nextid = c.execute(ids.select(for_update=True)).scalar()
    c.execute(ids.update(values={ids.c.nextid : ids.c.nextid + 1}))
    return nextid

# table setup.  we'll store a lead table of continents/cities,
# and a secondary table storing locations.
# a particular row will be placed in the database whose shard id corresponds to the
# 'continent'.  in this setup, secondary rows in 'weather_reports' will 
# be placed in the same DB as that of the parent, but this can be changed
# if you're willing to write more complex sharding functions.

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

# create tables
for db in (db1, db2, db3, db4):
    meta.drop_all(db)
    meta.create_all(db)
    
# establish initial "id" in db1
db1.execute(ids.insert(), nextid=1)


# step 5. define sharding functions.  

# we'll use a straight mapping of a particular set of "country" 
# attributes to shard id.
shard_lookup = {
    'North America':'north_america',
    'Asia':'asia',
    'Europe':'europe',
    'South America':'south_america'
}

# shard_chooser - looks at the given instance and returns a shard id
# note that we need to define conditions for 
# the WeatherLocation class, as well as our secondary Report class which will
# point back to its WeatherLocation via its 'location' attribute.
def shard_chooser(mapper, instance, clause=None):
    if isinstance(instance, WeatherLocation):
        return shard_lookup[instance.continent]
    else:
        return shard_chooser(mapper, instance.location)

# id_chooser.  given a primary key, returns a list of shards
# to search.  here, we don't have any particular information from a
# pk so we just return all shard ids. often, youd want to do some 
# kind of round-robin strategy here so that requests are evenly 
# distributed among DBs
def id_chooser(query, ident):
    return ['north_america', 'asia', 'europe', 'south_america']

# query_chooser.  this also returns a list of shard ids, which can
# just be all of them.  but here we'll search into the Query in order
# to try to narrow down the list of shards to query.
def query_chooser(query):
    ids = []

    # here we will traverse through the query's criterion, searching
    # for SQL constructs.  we'll grab continent names as we find them
    # and convert to shard ids
    class FindContinent(sql.ClauseVisitor):
        def visit_binary(self, binary):
            # "shares_lineage()" returns True if both columns refer to the same
            # statement column, adjusting for any annotations present.
            # (an annotation is an internal clone of a Column object
            # and occur when using ORM-mapped attributes like 
            # "WeatherLocation.continent"). A simpler comparison, though less accurate, 
            # would be "binary.left.key == 'continent'".
            if binary.left.shares_lineage(weather_locations.c.continent):
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

# further configure create_session to use these functions
create_session.configure(shard_chooser=shard_chooser, id_chooser=id_chooser, query_chooser=query_chooser)

# step 6.  mapped classes.    
class WeatherLocation(object):
    def __init__(self, continent, city):
        self.continent = continent
        self.city = city

class Report(object):
    def __init__(self, temperature):
        self.temperature = temperature

# step 7.  mappers
mapper(WeatherLocation, weather_locations, properties={
    'reports':relation(Report, backref='location')
})

mapper(Report, weather_reports)    


# save and load objects!

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
    sess.add(c)
sess.flush()

sess.expunge_all()

t = sess.query(WeatherLocation).get(tokyo.id)
assert t.city == tokyo.city
assert t.reports[0].temperature == 80.0

north_american_cities = sess.query(WeatherLocation).filter(WeatherLocation.continent == 'North America')
assert [c.city for c in north_american_cities] == ['New York', 'Toronto']

asia_and_europe = sess.query(WeatherLocation).filter(WeatherLocation.continent.in_(['Europe', 'Asia']))
assert set([c.city for c in asia_and_europe]) == set(['Tokyo', 'London', 'Dublin'])

