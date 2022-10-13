"""Illustrates sharding using a single database with multiple schemas,
where a different "schema_translates_map" can be used for each shard.

In this example we will set a "shard id" at all times.

"""
import datetime
import os

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker


echo = True
engine = create_engine("sqlite://", echo=echo)


with engine.connect() as conn:
    # use attached databases on sqlite to get "schemas"
    for i in range(1, 5):
        if os.path.exists("schema_%s.db" % i):
            os.remove("schema_%s.db" % i)
        conn.exec_driver_sql(
            'ATTACH DATABASE "schema_%s.db" AS schema_%s' % (i, i)
        )

db1 = engine.execution_options(schema_translate_map={None: "schema_1"})
db2 = engine.execution_options(schema_translate_map={None: "schema_2"})
db3 = engine.execution_options(schema_translate_map={None: "schema_3"})
db4 = engine.execution_options(schema_translate_map={None: "schema_4"})


# create session function.  this binds the shard ids
# to databases within a ShardedSession and returns it.
Session = sessionmaker(
    class_=ShardedSession,
    shards={
        "north_america": db1,
        "asia": db2,
        "europe": db3,
        "south_america": db4,
    },
)


# mappings and tables
Base = declarative_base()


# table setup.  we'll store a lead table of continents/cities, and a secondary
# table storing locations. a particular row will be placed in the database
# whose shard id corresponds to the 'continent'.  in this setup, secondary rows
# in 'weather_reports' will be placed in the same DB as that of the parent, but
# this can be changed if you're willing to write more complex sharding
# functions.


class WeatherLocation(Base):
    __tablename__ = "weather_locations"

    id = Column(Integer, primary_key=True)
    continent = Column(String(30), nullable=False)
    city = Column(String(50), nullable=False)

    reports = relationship("Report", backref="location")

    def __init__(self, continent, city):
        self.continent = continent
        self.city = city


class Report(Base):
    __tablename__ = "weather_reports"

    id = Column(Integer, primary_key=True)
    location_id = Column(
        "location_id", Integer, ForeignKey("weather_locations.id")
    )
    temperature = Column("temperature", Float)
    report_time = Column(
        "report_time", DateTime, default=datetime.datetime.now
    )

    def __init__(self, temperature):
        self.temperature = temperature


# create tables
for db in (db1, db2, db3, db4):
    Base.metadata.create_all(db)


# step 5. define sharding functions.

# we'll use a straight mapping of a particular set of "country"
# attributes to shard id.
shard_lookup = {
    "North America": "north_america",
    "Asia": "asia",
    "Europe": "europe",
    "South America": "south_america",
}


def shard_chooser(mapper, instance, clause=None):
    """shard chooser.

    this is primarily invoked at persistence time.

    looks at the given instance and returns a shard id
    note that we need to define conditions for
    the WeatherLocation class, as well as our secondary Report class which will
    point back to its WeatherLocation via its 'location' attribute.

    """
    if isinstance(instance, WeatherLocation):
        return shard_lookup[instance.continent]
    else:
        return shard_chooser(mapper, instance.location)


def id_chooser(query, ident):
    """id chooser.

    given a primary key identity and a legacy :class:`_orm.Query`,
    return which shard we should look at.

    in this case, we only want to support this for lazy-loaded items;
    any primary query should have shard id set up front.

    """
    if query.lazy_loaded_from:
        # if we are in a lazy load, we can look at the parent object
        # and limit our search to that same shard, assuming that's how we've
        # set things up.
        return [query.lazy_loaded_from.identity_token]
    else:
        raise NotImplementedError()


def execute_chooser(context):
    """statement execution chooser.

    given an :class:`.ORMExecuteState` for a statement, return a list
    of shards we should consult.

    As before, we want a "shard_id" execution option to be present.
    Otherwise, this would be a lazy load from a parent object where we
    will look for the previous token.

    """
    if context.lazy_loaded_from:
        return [context.lazy_loaded_from.identity_token]
    else:
        return [context.execution_options["shard_id"]]


# configure shard chooser
Session.configure(
    shard_chooser=shard_chooser,
    id_chooser=id_chooser,
    execute_chooser=execute_chooser,
)

# save and load objects!

tokyo = WeatherLocation("Asia", "Tokyo")
newyork = WeatherLocation("North America", "New York")
toronto = WeatherLocation("North America", "Toronto")
london = WeatherLocation("Europe", "London")
dublin = WeatherLocation("Europe", "Dublin")
brasilia = WeatherLocation("South America", "Brasila")
quito = WeatherLocation("South America", "Quito")

tokyo.reports.append(Report(80.0))
newyork.reports.append(Report(75))
quito.reports.append(Report(85))

with Session() as sess:

    sess.add_all([tokyo, newyork, toronto, london, dublin, brasilia, quito])

    sess.commit()

    t = sess.get(
        WeatherLocation,
        tokyo.id,
        # for session.get(), we currently need to use identity_token.
        # the horizontal sharding API does not yet pass through the
        # execution options
        identity_token="asia",
        # future version
        # execution_options={"shard_id": "asia"}
    )
    assert t.city == tokyo.city
    assert t.reports[0].temperature == 80.0

    north_american_cities = sess.execute(
        select(WeatherLocation).filter(
            WeatherLocation.continent == "North America"
        ),
        execution_options={"shard_id": "north_america"},
    ).scalars()

    assert {c.city for c in north_american_cities} == {"New York", "Toronto"}

    europe = sess.execute(
        select(WeatherLocation).filter(WeatherLocation.continent == "Europe"),
        execution_options={"shard_id": "europe"},
    ).scalars()

    assert {c.city for c in europe} == {"London", "Dublin"}

    # the Report class uses a simple integer primary key.  So across two
    # databases, a primary key will be repeated.  The "identity_token" tracks
    # in memory that these two identical primary keys are local to different
    # databases.
    newyork_report = newyork.reports[0]
    tokyo_report = tokyo.reports[0]

    assert inspect(newyork_report).identity_key == (
        Report,
        (1,),
        "north_america",
    )
    assert inspect(tokyo_report).identity_key == (Report, (1,), "asia")

    # the token representing the originating shard is also available directly

    assert inspect(newyork_report).identity_token == "north_america"
    assert inspect(tokyo_report).identity_token == "asia"
