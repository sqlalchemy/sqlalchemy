"""Illustrates sharding using a single database with multiple schemas,
where a different "schema_translates_map" can be used for each shard.

In this example we will set a "shard id" at all times.

"""

from __future__ import annotations

import datetime
import os

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy.ext.horizontal_shard import set_shard_id
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
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
class Base(DeclarativeBase):
    pass


# table setup.  we'll store a lead table of continents/cities, and a secondary
# table storing locations. a particular row will be placed in the database
# whose shard id corresponds to the 'continent'.  in this setup, secondary rows
# in 'weather_reports' will be placed in the same DB as that of the parent, but
# this can be changed if you're willing to write more complex sharding
# functions.


class WeatherLocation(Base):
    __tablename__ = "weather_locations"

    id: Mapped[int] = mapped_column(primary_key=True)
    continent: Mapped[str]
    city: Mapped[str]

    reports: Mapped[list[Report]] = relationship(back_populates="location")

    def __init__(self, continent: str, city: str):
        self.continent = continent
        self.city = city


class Report(Base):
    __tablename__ = "weather_reports"

    id: Mapped[int] = mapped_column(primary_key=True)
    location_id: Mapped[int] = mapped_column(
        ForeignKey("weather_locations.id")
    )
    temperature: Mapped[float]
    report_time: Mapped[datetime.datetime] = mapped_column(
        default=datetime.datetime.now
    )

    location: Mapped[WeatherLocation] = relationship(back_populates="reports")

    def __init__(self, temperature: float):
        self.temperature = temperature


# define sharding functions.

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


def identity_chooser(mapper, primary_key, *, lazy_loaded_from, **kw):
    """identity chooser.

    given a primary key identity, return which shard we should look at.

    in this case, we only want to support this for lazy-loaded items;
    any primary query should have shard id set up front.

    """
    if lazy_loaded_from:
        # if we are in a lazy load, we can look at the parent object
        # and limit our search to that same shard, assuming that's how we've
        # set things up.
        return [lazy_loaded_from.identity_token]
    else:
        raise NotImplementedError()


def execute_chooser(context):
    """statement execution chooser.

    given an :class:`.ORMExecuteState` for a statement, return a list
    of shards we should consult.

    """
    if context.lazy_loaded_from:
        return [context.lazy_loaded_from.identity_token]
    else:
        return ["north_america", "asia", "europe", "south_america"]


# configure shard chooser
Session.configure(
    shard_chooser=shard_chooser,
    identity_chooser=identity_chooser,
    execute_chooser=execute_chooser,
)


def setup():
    # create tables
    for db in (db1, db2, db3, db4):
        Base.metadata.create_all(db)


def main():
    setup()

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
        sess.add_all(
            [tokyo, newyork, toronto, london, dublin, brasilia, quito]
        )

        sess.commit()

        t = sess.get(
            WeatherLocation,
            tokyo.id,
            identity_token="asia",
        )
        assert t.city == tokyo.city
        assert t.reports[0].temperature == 80.0

        # select across shards
        asia_and_europe = sess.execute(
            select(WeatherLocation).filter(
                WeatherLocation.continent.in_(["Europe", "Asia"])
            )
        ).scalars()

        assert {c.city for c in asia_and_europe} == {
            "Tokyo",
            "London",
            "Dublin",
        }

        # optionally set a shard id for the query and all related loaders
        north_american_cities_w_t = sess.execute(
            select(WeatherLocation)
            .filter(WeatherLocation.city.startswith("T"))
            .options(set_shard_id("north_america"))
        ).scalars()

        # Tokyo not included since not in the north_america shard
        assert {c.city for c in north_american_cities_w_t} == {
            "Toronto",
        }

        # the Report class uses a simple integer primary key.  So across two
        # databases, a primary key will be repeated.  The "identity_token"
        # tracks in memory that these two identical primary keys are local to
        # different shards.
        newyork_report = newyork.reports[0]
        tokyo_report = tokyo.reports[0]

        assert inspect(newyork_report).identity_key == (
            Report,
            (1,),
            "north_america",
        )
        assert inspect(tokyo_report).identity_key == (Report, (1,), "asia")

        # the token representing the originating shard is also available
        # directly
        assert inspect(newyork_report).identity_token == "north_america"
        assert inspect(tokyo_report).identity_token == "asia"


if __name__ == "__main__":
    main()
