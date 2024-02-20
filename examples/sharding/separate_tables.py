"""Illustrates sharding using a single SQLite database, that will however
have multiple tables using a naming convention."""

from __future__ import annotations

import datetime

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy.ext.horizontal_shard import set_shard_id
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import operators
from sqlalchemy.sql import visitors

echo = True
engine = create_engine("sqlite://", echo=echo)

db1 = engine.execution_options(table_prefix="north_america")
db2 = engine.execution_options(table_prefix="asia")
db3 = engine.execution_options(table_prefix="europe")
db4 = engine.execution_options(table_prefix="south_america")


@event.listens_for(engine, "before_cursor_execute", retval=True)
def before_cursor_execute(
    conn, cursor, statement, parameters, context, executemany
):
    table_prefix = context.execution_options.get("table_prefix", None)
    if table_prefix:
        statement = statement.replace("_prefix_", table_prefix)
    return statement, parameters


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


# we need a way to create identifiers which are unique across all databases.
# one easy way would be to just use a composite primary key, where  one value
# is the shard id.  but here, we'll show something more "generic", an id
# generation function.  we'll use a simplistic "id table" stored in database
# #1.  Any other method will do just as well; UUID, hilo, application-specific,
# etc.

ids = Table("ids", Base.metadata, Column("nextid", Integer, nullable=False))


def id_generator(ctx):
    # in reality, might want to use a separate transaction for this.
    with engine.begin() as conn:
        nextid = conn.scalar(ids.select().with_for_update())
        conn.execute(ids.update().values({ids.c.nextid: ids.c.nextid + 1}))
    return nextid


# table setup.  we'll store a lead table of continents/cities, and a secondary
# table storing locations. a particular row will be placed in the database
# whose shard id corresponds to the 'continent'.  in this setup, secondary rows
# in 'weather_reports' will be placed in the same DB as that of the parent, but
# this can be changed if you're willing to write more complex sharding
# functions.


class WeatherLocation(Base):
    __tablename__ = "_prefix__weather_locations"

    id: Mapped[int] = mapped_column(primary_key=True, default=id_generator)
    continent: Mapped[str]
    city: Mapped[str]

    reports: Mapped[list[Report]] = relationship(back_populates="location")

    def __init__(self, continent: str, city: str):
        self.continent = continent
        self.city = city


class Report(Base):
    __tablename__ = "_prefix__weather_reports"

    id: Mapped[int] = mapped_column(primary_key=True)
    location_id: Mapped[int] = mapped_column(
        ForeignKey("_prefix__weather_locations.id")
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

    given a primary key, returns a list of shards
    to search.  here, we don't have any particular information from a
    pk so we just return all shard ids. often, you'd want to do some
    kind of round-robin strategy here so that requests are evenly
    distributed among DBs.

    """
    if lazy_loaded_from:
        # if we are in a lazy load, we can look at the parent object
        # and limit our search to that same shard, assuming that's how we've
        # set things up.
        return [lazy_loaded_from.identity_token]
    else:
        return ["north_america", "asia", "europe", "south_america"]


def execute_chooser(context):
    """statement execution chooser.

    this also returns a list of shard ids, which can just be all of them. but
    here we'll search into the execution context in order to try to narrow down
    the list of shards to SELECT.

    """
    ids = []

    # we'll grab continent names as we find them
    # and convert to shard ids
    for column, operator, value in _get_select_comparisons(context.statement):
        # "shares_lineage()" returns True if both columns refer to the same
        # statement column, adjusting for any annotations present.
        # (an annotation is an internal clone of a Column object
        # and occur when using ORM-mapped attributes like
        # "WeatherLocation.continent"). A simpler comparison, though less
        # accurate, would be "column.key == 'continent'".
        if column.shares_lineage(WeatherLocation.__table__.c.continent):
            if operator == operators.eq:
                ids.append(shard_lookup[value])
            elif operator == operators.in_op:
                ids.extend(shard_lookup[v] for v in value)

    if len(ids) == 0:
        return ["north_america", "asia", "europe", "south_america"]
    else:
        return ids


def _get_select_comparisons(statement):
    """Search a Select or Query object for binary expressions.

    Returns expressions which match a Column against one or more
    literal values as a list of tuples of the form
    (column, operator, values).   "values" is a single value
    or tuple of values depending on the operator.

    """
    binds = {}
    clauses = set()
    comparisons = []

    def visit_bindparam(bind):
        # visit a bind parameter.

        value = bind.effective_value
        binds[bind] = value

    def visit_column(column):
        clauses.add(column)

    def visit_binary(binary):
        if binary.left in clauses and binary.right in binds:
            comparisons.append(
                (binary.left, binary.operator, binds[binary.right])
            )

        elif binary.left in binds and binary.right in clauses:
            comparisons.append(
                (binary.right, binary.operator, binds[binary.left])
            )

    # here we will traverse through the query's criterion, searching
    # for SQL constructs.  We will place simple column comparisons
    # into a list.
    if statement.whereclause is not None:
        visitors.traverse(
            statement.whereclause,
            {},
            {
                "bindparam": visit_bindparam,
                "binary": visit_binary,
                "column": visit_column,
            },
        )
    return comparisons


# further configure create_session to use these functions
Session.configure(
    shard_chooser=shard_chooser,
    identity_chooser=identity_chooser,
    execute_chooser=execute_chooser,
)


def setup():
    # create tables
    for db in (db1, db2, db3, db4):
        Base.metadata.create_all(db)

    # establish initial "id" in db1
    with db1.begin() as conn:
        conn.execute(ids.insert(), {"nextid": 1})


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

        t = sess.get(WeatherLocation, tokyo.id)
        assert t.city == tokyo.city
        assert t.reports[0].temperature == 80.0

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
