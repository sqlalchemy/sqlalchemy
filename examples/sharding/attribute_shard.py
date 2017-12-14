from sqlalchemy import (create_engine, Table, Column, Integer,
                        String, ForeignKey, Float, DateTime)
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.sql import operators, visitors
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect

import datetime

# db1 is used for id generation. The "pool_threadlocal"
# causes the id_generator() to use the same connection as that
# of an ongoing transaction within db1.
echo = True
db1 = create_engine('sqlite://', echo=echo, pool_threadlocal=True)
db2 = create_engine('sqlite://', echo=echo)
db3 = create_engine('sqlite://', echo=echo)
db4 = create_engine('sqlite://', echo=echo)


# create session function.  this binds the shard ids
# to databases within a ShardedSession and returns it.
create_session = sessionmaker(class_=ShardedSession)

create_session.configure(shards={
    'north_america': db1,
    'asia': db2,
    'europe': db3,
    'south_america': db4
})


# mappings and tables
Base = declarative_base()

# we need a way to create identifiers which are unique across all databases.
# one easy way would be to just use a composite primary key, where  one value
# is the shard id.  but here, we'll show something more "generic", an id
# generation function.  we'll use a simplistic "id table" stored in database
# #1.  Any other method will do just as well; UUID, hilo, application-specific,
# etc.

ids = Table(
    'ids', Base.metadata,
    Column('nextid', Integer, nullable=False))


def id_generator(ctx):
    # in reality, might want to use a separate transaction for this.
    with db1.connect() as conn:
        nextid = conn.scalar(ids.select(for_update=True))
        conn.execute(ids.update(values={ids.c.nextid: ids.c.nextid + 1}))
    return nextid

# table setup.  we'll store a lead table of continents/cities, and a secondary
# table storing locations. a particular row will be placed in the database
# whose shard id corresponds to the 'continent'.  in this setup, secondary rows
# in 'weather_reports' will be placed in the same DB as that of the parent, but
# this can be changed if you're willing to write more complex sharding
# functions.


class WeatherLocation(Base):
    __tablename__ = "weather_locations"

    id = Column(Integer, primary_key=True, default=id_generator)
    continent = Column(String(30), nullable=False)
    city = Column(String(50), nullable=False)

    reports = relationship("Report", backref='location')

    def __init__(self, continent, city):
        self.continent = continent
        self.city = city


class Report(Base):
    __tablename__ = "weather_reports"

    id = Column(Integer, primary_key=True)
    location_id = Column(
        'location_id', Integer, ForeignKey('weather_locations.id'))
    temperature = Column('temperature', Float)
    report_time = Column(
        'report_time', DateTime, default=datetime.datetime.now)

    def __init__(self, temperature):
        self.temperature = temperature

# create tables
for db in (db1, db2, db3, db4):
    Base.metadata.drop_all(db)
    Base.metadata.create_all(db)

# establish initial "id" in db1
db1.execute(ids.insert(), nextid=1)


# step 5. define sharding functions.

# we'll use a straight mapping of a particular set of "country"
# attributes to shard id.
shard_lookup = {
    'North America': 'north_america',
    'Asia': 'asia',
    'Europe': 'europe',
    'South America': 'south_america'
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


def id_chooser(query, ident):
    """id chooser.

    given a primary key, returns a list of shards
    to search.  here, we don't have any particular information from a
    pk so we just return all shard ids. often, you'd want to do some
    kind of round-robin strategy here so that requests are evenly
    distributed among DBs.

    """
    return ['north_america', 'asia', 'europe', 'south_america']


def query_chooser(query):
    """query chooser.

    this also returns a list of shard ids, which can
    just be all of them.  but here we'll search into the Query in order
    to try to narrow down the list of shards to query.

    """
    ids = []

    # we'll grab continent names as we find them
    # and convert to shard ids
    for column, operator, value in _get_query_comparisons(query):
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
        return ['north_america', 'asia', 'europe', 'south_america']
    else:
        return ids


def _get_query_comparisons(query):
    """Search an orm.Query object for binary expressions.

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

        # check in _params for it first
        if bind.key in query._params:
            value = query._params[bind.key]
        elif bind.callable:
            # some ORM functions (lazy loading)
            # place the bind's value as a
            # callable for deferred evaluation.
            value = bind.callable()
        else:
            # just use .value
            value = bind.value

        binds[bind] = value

    def visit_column(column):
        clauses.add(column)

    def visit_binary(binary):
        # special handling for "col IN (params)"
        if binary.left in clauses and \
                binary.operator == operators.in_op and \
                hasattr(binary.right, 'clauses'):
            comparisons.append(
                (
                    binary.left, binary.operator,
                    tuple(binds[bind] for bind in binary.right.clauses)
                )
            )
        elif binary.left in clauses and binary.right in binds:
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
    if query._criterion is not None:
        visitors.traverse_depthfirst(
            query._criterion, {},
            {'bindparam': visit_bindparam,
                'binary': visit_binary,
                'column': visit_column}
        )
    return comparisons

# further configure create_session to use these functions
create_session.configure(
    shard_chooser=shard_chooser,
    id_chooser=id_chooser,
    query_chooser=query_chooser
)

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

sess.add_all([tokyo, newyork, toronto, london, dublin, brasilia, quito])

sess.commit()

t = sess.query(WeatherLocation).get(tokyo.id)
assert t.city == tokyo.city
assert t.reports[0].temperature == 80.0

north_american_cities = sess.query(WeatherLocation).filter(
    WeatherLocation.continent == 'North America')
assert {c.city for c in north_american_cities} == {'New York', 'Toronto'}

asia_and_europe = sess.query(WeatherLocation).filter(
    WeatherLocation.continent.in_(['Europe', 'Asia']))
assert {c.city for c in asia_and_europe} == {'Tokyo', 'London', 'Dublin'}

# the Report class uses a simple integer primary key.  So across two databases,
# a primary key will be repeated.  The "identity_token" tracks in memory
# that these two identical primary keys are local to different databases.
newyork_report = newyork.reports[0]
tokyo_report = tokyo.reports[0]

assert inspect(newyork_report).identity_key == (Report, (1, ), "north_america")
assert inspect(tokyo_report).identity_key == (Report, (1, ), "asia")

# the token representing the originating shard is also available directly

assert inspect(newyork_report).identity_token == "north_america"
assert inspect(tokyo_report).identity_token == "asia"
