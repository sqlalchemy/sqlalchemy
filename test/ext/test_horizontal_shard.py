import datetime
import os

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import delete
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy import util
from sqlalchemy.ext.horizontal_shard import set_shard_id
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import defer
from sqlalchemy.orm import deferred
from sqlalchemy.orm import lazyload
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import SingletonThreadPool
from sqlalchemy.sql import operators
from sqlalchemy.sql import Select
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_deprecated
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import provision
from sqlalchemy.testing.assertions import expect_raises_message
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.testing.engines import testing_reaper


class ShardTest:
    __skip_if__ = (lambda: util.win32,)
    __requires__ = ("sqlite",)

    run_create_tables = None

    schema = None

    @classmethod
    def define_tables(cls, metadata):
        global db1, db2, db3, db4, weather_locations, weather_reports

        cls.tables.ids = ids = Table(
            "ids", metadata, Column("nextid", Integer, nullable=False)
        )

        def id_generator(ctx):
            # in reality, might want to use a separate transaction for this.

            with db1.begin() as c:
                nextid = c.execute(ids.select().with_for_update()).scalar()
                c.execute(
                    ids.update().values({ids.c.nextid: ids.c.nextid + 1})
                )
                return nextid

        cls.tables.weather_locations = weather_locations = Table(
            "weather_locations",
            metadata,
            Column("id", Integer, primary_key=True, default=id_generator),
            Column("continent", String(30), nullable=False),
            Column("city", String(50), nullable=False),
            schema=cls.schema,
        )

        cls.tables.weather_reports = Table(
            "weather_reports",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("location_id", Integer, ForeignKey(weather_locations.c.id)),
            Column("temperature", Numeric(asdecimal=False)),
            Column("report_time", DateTime, default=datetime.datetime.now),
            schema=cls.schema,
        )

    def setup_test(self):
        global db1, db2, db3, db4
        db1, db2, db3, db4 = self._dbs = self.dbs = self._init_dbs()

        for db in (db1, db2, db3, db4):
            self.tables_test_metadata.create_all(db)

        ids = self.tables.ids
        with db1.begin() as conn:
            conn.execute(ids.insert(), dict(nextid=1))

        self.setup_session()

    @classmethod
    def setup_session(cls):
        global sharded_session
        shard_lookup = {
            "North America": "north_america",
            "Asia": "asia",
            "Europe": "europe",
            "South America": "south_america",
        }

        def shard_chooser(mapper, instance, clause=None):
            if isinstance(instance, WeatherLocation):
                return shard_lookup[instance.continent]
            else:
                return shard_chooser(mapper, instance.location)

        def identity_chooser(
            mapper,
            primary_key,
            *,
            lazy_loaded_from,
            execution_options,
            bind_arguments,
            **kw,
        ):
            return ["north_america", "asia", "europe", "south_america"]

        def execute_chooser(orm_context):
            ids = []

            query = orm_context.statement

            class FindContinent(sql.ClauseVisitor):
                def visit_binary(self, binary):
                    if binary.left.shares_lineage(
                        weather_locations.c.continent
                    ):
                        if binary.operator == operators.eq:
                            ids.append(shard_lookup[binary.right.value])
                        elif binary.operator == operators.in_op:
                            for value in binary.right.value:
                                ids.append(shard_lookup[value])

            if isinstance(query, Select) and query.whereclause is not None:
                FindContinent().traverse(query.whereclause)
            if len(ids) == 0:
                return ["north_america", "asia", "europe", "south_america"]
            else:
                return ids

        sharded_session = sessionmaker(class_=ShardedSession, autoflush=True)
        sharded_session.configure(
            shards={
                "north_america": db1,
                "asia": db2,
                "europe": db3,
                "south_america": db4,
            },
            shard_chooser=shard_chooser,
            identity_chooser=identity_chooser,
            execute_chooser=execute_chooser,
        )

    @classmethod
    def setup_mappers(cls):
        global WeatherLocation, Report

        class WeatherLocation:
            def __init__(self, continent, city):
                self.continent = continent
                self.city = city

        class Report:
            def __init__(self, temperature, id_=None):
                self.temperature = temperature
                if id_:
                    self.id = id_

        weather_locations = cls.tables.weather_locations

        cls.mapper_registry.map_imperatively(
            WeatherLocation,
            weather_locations,
            properties={
                "reports": relationship(Report, backref="location"),
                "city": deferred(weather_locations.c.city),
            },
        )

        weather_reports = cls.tables.weather_reports

        cls.mapper_registry.map_imperatively(Report, weather_reports)

    def _fixture_data(self):
        tokyo = WeatherLocation("Asia", "Tokyo")
        newyork = WeatherLocation("North America", "New York")
        toronto = WeatherLocation("North America", "Toronto")
        london = WeatherLocation("Europe", "London")
        dublin = WeatherLocation("Europe", "Dublin")
        brasilia = WeatherLocation("South America", "Brasilia")
        quito = WeatherLocation("South America", "Quito")
        tokyo.reports.append(Report(80.0, id_=1))
        newyork.reports.append(Report(75, id_=1))
        quito.reports.append(Report(85))
        sess = sharded_session()
        for c in [tokyo, newyork, toronto, london, dublin, brasilia, quito]:
            sess.add(c)
        sess.flush()

        eq_(inspect(newyork).key[2], "north_america")
        eq_(inspect(newyork).identity_token, "north_america")
        eq_(inspect(dublin).key[2], "europe")
        eq_(inspect(dublin).identity_token, "europe")

        sess.commit()
        sess.close()
        return sess

    def test_get(self):
        sess = self._fixture_data()
        tokyo = sess.get(WeatherLocation, 1)
        eq_(tokyo.city, "Tokyo")

        newyork = sess.get(WeatherLocation, 2)
        eq_(newyork.city, "New York")

        t2 = sess.get(WeatherLocation, 1)
        is_(t2, tokyo)

    def test_get_one(self):
        sess = self._fixture_data()
        brasilia = sess.get_one(WeatherLocation, 6)
        eq_(brasilia.id, 6)
        eq_(brasilia.city, "Brasilia")

        toronto = sess.get_one(WeatherLocation, 3)
        eq_(toronto.id, 3)
        eq_(toronto.city, "Toronto")

        with expect_raises_message(
            exc.NoResultFound, "No row was found when one was required"
        ):
            sess.get_one(WeatherLocation, 25)

    def test_get_explicit_shard(self):
        sess = self._fixture_data()
        tokyo = (
            sess.query(WeatherLocation)
            .set_shard("europe")
            .where(WeatherLocation.id == 1)
            .first()
        )
        is_(tokyo, None)

        newyork = (
            sess.query(WeatherLocation)
            .set_shard("north_america")
            .where(WeatherLocation.id == 2)
            .first()
        )
        eq_(newyork.city, "New York")

        # now it found it
        t2 = sess.get(WeatherLocation, 1)
        eq_(t2.city, "Tokyo")

    @testing.variation("option_type", ["none", "lazyload", "selectinload"])
    @testing.variation(
        "limit_shard",
        ["none", "lead_only", "propagate_to_loaders", "bind_arg"],
    )
    def test_set_shard_option_relationship(self, option_type, limit_shard):
        sess = self._fixture_data()

        stmt = select(WeatherLocation).filter(
            WeatherLocation.city == "New York"
        )

        bind_arguments = {}

        if limit_shard.none:
            # right now selectinload / lazyload runs all the shards even if the
            # ids are limited to just one shard, since that information
            # is not transferred
            counts = [2, 2, 2, 2]
        elif limit_shard.lead_only:
            if option_type.selectinload:
                counts = [2, 0, 0, 0]
            else:
                counts = [2, 1, 1, 1]
        elif limit_shard.bind_arg:
            counts = [2, 1, 1, 1]
        elif limit_shard.propagate_to_loaders:
            counts = [2, 0, 0, 0]
        else:
            limit_shard.fail()

        if option_type.lazyload:
            stmt = stmt.options(lazyload(WeatherLocation.reports))
        elif option_type.selectinload:
            stmt = stmt.options(selectinload(WeatherLocation.reports))

        if limit_shard.lead_only:
            stmt = stmt.options(
                set_shard_id("north_america", propagate_to_loaders=False)
            )
        elif limit_shard.propagate_to_loaders:
            stmt = stmt.options(set_shard_id("north_america"))
        elif limit_shard.bind_arg:
            bind_arguments["shard_id"] = "north_america"

        with self.assert_statement_count_multi_db(self.dbs, counts):
            w1 = sess.scalars(stmt, bind_arguments=bind_arguments).first()
            w1.reports

    @testing.variation("option_type", ["none", "defer"])
    @testing.variation(
        "limit_shard",
        ["none", "lead_only", "propagate_to_loaders", "bind_arg"],
    )
    def test_set_shard_option_column(self, option_type, limit_shard):
        sess = self._fixture_data()

        stmt = select(WeatherLocation).filter(
            WeatherLocation.city == "New York"
        )

        bind_arguments = {}

        if limit_shard.none:
            if option_type.defer:
                counts = [2, 1, 1, 1]
            else:
                counts = [1, 1, 1, 1]
        elif limit_shard.lead_only or limit_shard.propagate_to_loaders:
            if option_type.defer:
                counts = [2, 0, 0, 0]
            else:
                counts = [1, 0, 0, 0]
        elif limit_shard.bind_arg:
            if option_type.defer:
                counts = [2, 0, 0, 0]
            else:
                counts = [1, 0, 0, 0]
        else:
            limit_shard.fail()

        if option_type.defer:
            stmt = stmt.options(defer(WeatherLocation.continent))

        if limit_shard.lead_only:
            stmt = stmt.options(
                set_shard_id("north_america", propagate_to_loaders=False)
            )
        elif limit_shard.propagate_to_loaders:
            stmt = stmt.options(set_shard_id("north_america"))
        elif limit_shard.bind_arg:
            bind_arguments["shard_id"] = "north_america"

        with self.assert_statement_count_multi_db(self.dbs, counts):
            w1 = sess.scalars(stmt, bind_arguments=bind_arguments).first()
            w1.continent

    def test_query_explicit_shard_via_bind_opts(self):
        sess = self._fixture_data()

        stmt = select(WeatherLocation).filter(WeatherLocation.id == 1)

        tokyo = (
            sess.execute(stmt, bind_arguments={"shard_id": "asia"})
            .scalars()
            .first()
        )

        eq_(tokyo.city, "Tokyo")

    def test_plain_db_lookup(self):
        self._fixture_data()
        # not sure what this is testing except the fixture data itself
        eq_(
            db2.connect().execute(weather_locations.select()).fetchall(),
            [(1, "Asia", "Tokyo")],
        )
        eq_(
            db1.connect().execute(weather_locations.select()).fetchall(),
            [
                (2, "North America", "New York"),
                (3, "North America", "Toronto"),
            ],
        )

    def test_plain_core_lookup_w_shard(self):
        sess = self._fixture_data()
        eq_(
            sess.execute(
                weather_locations.select(),
                bind_arguments=dict(shard_id="asia"),
            ).fetchall(),
            [(1, "Asia", "Tokyo")],
        )

    def test_roundtrip_future(self):
        sess = self._fixture_data()

        tokyo = (
            sess.execute(select(WeatherLocation).filter_by(city="Tokyo"))
            .scalars()
            .one()
        )
        eq_(tokyo.city, "Tokyo")

        asia_and_europe = sess.execute(
            select(WeatherLocation).filter(
                WeatherLocation.continent.in_(["Europe", "Asia"])
            )
        ).scalars()
        eq_(
            {c.city for c in asia_and_europe},
            {"Tokyo", "London", "Dublin"},
        )

    def test_roundtrip(self):
        sess = self._fixture_data()
        tokyo = sess.query(WeatherLocation).filter_by(city="Tokyo").one()

        eq_(tokyo.city, "Tokyo")
        tokyo.city  # reload 'city' attribute on tokyo
        sess.expire_all()

        t = sess.get(WeatherLocation, tokyo.id)
        eq_(t.city, tokyo.city)
        eq_(t.reports[0].temperature, 80.0)
        north_american_cities = sess.query(WeatherLocation).filter(
            WeatherLocation.continent == "North America"
        )
        eq_(
            {c.city for c in north_american_cities},
            {"New York", "Toronto"},
        )
        asia_and_europe = sess.query(WeatherLocation).filter(
            WeatherLocation.continent.in_(["Europe", "Asia"])
        )
        eq_(
            {c.city for c in asia_and_europe},
            {"Tokyo", "London", "Dublin"},
        )

        # inspect the shard token stored with each instance
        eq_(
            {inspect(c).key[2] for c in asia_and_europe},
            {"europe", "asia"},
        )

        eq_(
            {inspect(c).identity_token for c in asia_and_europe},
            {"europe", "asia"},
        )

        newyork = sess.query(WeatherLocation).filter_by(city="New York").one()
        newyork_report = newyork.reports[0]
        tokyo_report = tokyo.reports[0]

        # same primary key, two identity keys
        eq_(
            inspect(newyork_report).identity_key,
            (Report, (1,), "north_america"),
        )
        eq_(inspect(tokyo_report).identity_key, (Report, (1,), "asia"))

        # the token representing the originating shard is available
        eq_(inspect(newyork_report).identity_token, "north_america")
        eq_(inspect(tokyo_report).identity_token, "asia")

    def test_get_baked_query(self):
        sess = self._fixture_data()

        tokyo = sess.query(WeatherLocation).filter_by(city="Tokyo").one()
        tokyo.city
        sess.expunge_all()

        from sqlalchemy.ext.baked import BakedQuery

        bakery = BakedQuery.bakery()

        bq = bakery(lambda session: session.query(WeatherLocation))
        t = bq(sess).get(tokyo.id)
        eq_(t.city, tokyo.city)

        eq_(inspect(t).key[2], "asia")

    def test_get_baked_query_shard_id(self):
        sess = self._fixture_data()

        tokyo = sess.query(WeatherLocation).filter_by(city="Tokyo").one()
        tokyo.city
        sess.expunge_all()

        from sqlalchemy.ext.baked import BakedQuery

        bakery = BakedQuery.bakery()

        bq = bakery(lambda session: session.query(WeatherLocation))
        t = (
            bq(sess)
            .with_post_criteria(lambda q: q.set_shard("asia"))
            .get(tokyo.id)
        )
        eq_(t.city, tokyo.city)

        eq_(inspect(t).key[2], "asia")

    def test_filter_baked_query_shard_id(self):
        sess = self._fixture_data()

        tokyo = sess.query(WeatherLocation).filter_by(city="Tokyo").one()
        tokyo.city
        sess.expunge_all()

        from sqlalchemy.ext.baked import BakedQuery

        bakery = BakedQuery.bakery()

        bq = bakery(
            lambda session: session.query(WeatherLocation)
        ).with_criteria(lambda q: q.filter_by(id=tokyo.id))
        t = bq(sess).with_post_criteria(lambda q: q.set_shard("asia")).one()
        eq_(t.city, tokyo.city)

    def test_shard_id_event(self):
        # this test is kind of important, it's testing that
        # when the load event is emitted for an ORM result,
        # the context is set up in the state that is expected.
        # prior to 1.4, we were changing a single context in place,
        # as we would join result sets by fully evaluating and concatenating.
        # in 1.4 onwards we return a Result that has not run for each
        # individual result yet, so each one has its own context that
        # is a shallow copy from the original.

        canary = []

        def load(instance, ctx):
            canary.append(ctx.bind_arguments["shard_id"])

        event.listen(WeatherLocation, "load", load)
        sess = self._fixture_data()

        tokyo = (  # noqa
            sess.query(WeatherLocation)
            .filter_by(city="Tokyo")
            .set_shard("asia")
            .one()
        )

        sess.query(WeatherLocation).all()
        eq_(
            canary,
            [
                "asia",
                "north_america",
                "north_america",
                "europe",
                "europe",
                "south_america",
                "south_america",
            ],
        )

    def test_baked_mix(self):
        sess = self._fixture_data()

        tokyo = sess.query(WeatherLocation).filter_by(city="Tokyo").one()
        tokyo.city
        sess.expunge_all()

        from sqlalchemy.ext.baked import BakedQuery

        bakery = BakedQuery.bakery()

        def get_tokyo(sess):
            bq = bakery(lambda session: session.query(WeatherLocation))
            t = bq(sess).get(tokyo.id)
            return t

        Sess = sessionmaker(class_=Session, bind=db2, autoflush=True)
        sess2 = Sess()

        t = get_tokyo(sess)
        eq_(t.city, tokyo.city)

        t = get_tokyo(sess2)
        eq_(t.city, tokyo.city)

    @testing.combinations(
        "fetch", "evaluate", "auto", argnames="synchronize_session"
    )
    @testing.combinations(True, False, argnames="legacy")
    def test_orm_update_synchronize(self, synchronize_session, legacy):
        sess = self._fixture_data()

        eq_(
            {row.temperature for row in sess.query(Report.temperature)},
            {80.0, 75.0, 85.0},
        )

        temps = sess.query(Report).all()
        eq_({t.temperature for t in temps}, {80.0, 75.0, 85.0})

        if legacy:
            sess.query(Report).filter(Report.temperature >= 80).update(
                {"temperature": Report.temperature + 6},
                synchronize_session=synchronize_session,
            )
        else:
            sess.execute(
                update(Report)
                .filter(Report.temperature >= 80)
                .values(temperature=Report.temperature + 6)
                .execution_options(synchronize_session=synchronize_session)
            )

        with self.assert_statement_count_multi_db(self.dbs, [0, 0, 0, 0]):
            eq_({t.temperature for t in temps}, {86.0, 75.0, 91.0})

        eq_(
            {row.temperature for row in sess.query(Report.temperature)},
            {86.0, 75.0, 91.0},
        )

    @testing.combinations(
        "fetch", "evaluate", "auto", argnames="synchronize_session"
    )
    @testing.combinations(True, False, argnames="legacy")
    def test_orm_delete_synchronize(self, synchronize_session, legacy):
        sess = self._fixture_data()

        temps = sess.query(Report).all()
        eq_({t.temperature for t in temps}, {80.0, 75.0, 85.0})

        if legacy:
            sess.query(Report).filter(Report.temperature >= 80).delete(
                synchronize_session=synchronize_session
            )
        else:
            sess.execute(
                delete(Report)
                .filter(Report.temperature >= 80)
                .execution_options(synchronize_session=synchronize_session)
            )

        with self.assert_statement_count_multi_db(self.dbs, [0, 0, 0, 0]):
            # test synchronize session
            for t in temps:
                assert inspect(t).deleted is (t.temperature >= 80)

        eq_(
            {row.temperature for row in sess.query(Report.temperature)},
            {75.0},
        )


class DistinctEngineShardTest(ShardTest, fixtures.MappedTest):
    def _init_dbs(self):
        db1 = testing_engine(
            "sqlite:///shard1_%s.db" % provision.FOLLOWER_IDENT,
            options=dict(poolclass=SingletonThreadPool),
        )
        db2 = testing_engine(
            "sqlite:///shard2_%s.db" % provision.FOLLOWER_IDENT
        )
        db3 = testing_engine(
            "sqlite:///shard3_%s.db" % provision.FOLLOWER_IDENT
        )
        db4 = testing_engine(
            "sqlite:///shard4_%s.db" % provision.FOLLOWER_IDENT
        )

        self.dbs = [db1, db2, db3, db4]
        return self.dbs

    def teardown_test(self):
        testing_reaper.checkin_all()
        for i in range(1, 5):
            os.remove("shard%d_%s.db" % (i, provision.FOLLOWER_IDENT))

    def test_plain_core_textual_lookup_w_shard(self):
        sess = self._fixture_data()

        stmt = text("SELECT * FROM weather_locations")

        eq_(
            sess.execute(
                stmt, bind_arguments=dict(shard_id="asia")
            ).fetchall(),
            [(1, "Asia", "Tokyo")],
        )

    def test_plain_core_textual_lookup(self):
        sess = self._fixture_data()

        stmt = text("SELECT * FROM weather_locations WHERE id=1")
        eq_(
            sess.execute(stmt).fetchall(),
            [(1, "Asia", "Tokyo")],
        )


class LegacyAPIShardTest(DistinctEngineShardTest):
    @classmethod
    def setup_session(cls):
        global sharded_session
        shard_lookup = {
            "North America": "north_america",
            "Asia": "asia",
            "Europe": "europe",
            "South America": "south_america",
        }

        def shard_chooser(mapper, instance, clause=None):
            if isinstance(instance, WeatherLocation):
                return shard_lookup[instance.continent]
            else:
                return shard_chooser(mapper, instance.location)

        def id_chooser(query, primary_key):
            return ["north_america", "asia", "europe", "south_america"]

        def query_chooser(query):
            ids = []

            class FindContinent(sql.ClauseVisitor):
                def visit_binary(self, binary):
                    if binary.left.shares_lineage(
                        weather_locations.c.continent
                    ):
                        if binary.operator == operators.eq:
                            ids.append(shard_lookup[binary.right.value])
                        elif binary.operator == operators.in_op:
                            for value in binary.right.value:
                                ids.append(shard_lookup[value])

            if isinstance(query, Select) and query.whereclause is not None:
                FindContinent().traverse(query.whereclause)
            if len(ids) == 0:
                return ["north_america", "asia", "europe", "south_america"]
            else:
                return ids

        sm = sessionmaker(class_=ShardedSession, autoflush=True)
        sm.configure(
            shards={
                "north_america": db1,
                "asia": db2,
                "europe": db3,
                "south_america": db4,
            },
            shard_chooser=shard_chooser,
            id_chooser=id_chooser,
            query_chooser=query_chooser,
        )

        def sharded_session():
            with expect_deprecated(
                "The ``id_chooser`` parameter is deprecated",
                "The ``query_chooser`` parameter is deprecated",
            ):
                return sm()


class AttachedFileShardTest(ShardTest, fixtures.MappedTest):
    """Use modern schema conventions along with SQLite ATTACH."""

    schema = "changeme"

    def _init_dbs(self):
        e = testing_engine("sqlite://")
        with e.connect() as conn:
            for i in range(1, 5):
                conn.exec_driver_sql(
                    'ATTACH DATABASE "shard%s_%s.db" AS shard%s'
                    % (i, provision.FOLLOWER_IDENT, i)
                )

        db1 = e.execution_options(schema_translate_map={"changeme": "shard1"})
        db2 = e.execution_options(schema_translate_map={"changeme": "shard2"})
        db3 = e.execution_options(schema_translate_map={"changeme": "shard3"})
        db4 = e.execution_options(schema_translate_map={"changeme": "shard4"})

        self.engine = e
        return db1, db2, db3, db4

    def teardown_test(self):
        testing_reaper.checkin_all()
        for i in range(1, 5):
            os.remove("shard%d_%s.db" % (i, provision.FOLLOWER_IDENT))


class TableNameConventionShardTest(ShardTest, fixtures.MappedTest):
    """This fixture uses a single SQLite database along with a table naming
    convention to achieve sharding.   Event hooks are used to rewrite SQL
    statements.

    This used to be called "AttachedFileShardTest" but I didn't see any
    ATTACH going on.

    A more modern approach here would be to use the schema_translate_map
    option.

    """

    schema = "changeme"

    def _init_dbs(self):
        dbmain = testing_engine("sqlite://")
        db1 = dbmain.execution_options(shard_id="shard1")
        db2 = dbmain.execution_options(shard_id="shard2")
        db3 = dbmain.execution_options(shard_id="shard3")
        db4 = dbmain.execution_options(shard_id="shard4")

        import re

        @event.listens_for(dbmain, "before_cursor_execute", retval=True)
        def _switch_shard(conn, cursor, stmt, params, context, executemany):
            shard_id = conn._execution_options["shard_id"]
            # because SQLite can't just give us a "use" statement, we have
            # to use the schema hack to locate table names
            if shard_id:
                stmt = re.sub(r"\"?changeme\"?\.", shard_id + "_", stmt)

            return stmt, params

        return db1, db2, db3, db4


class MultipleDialectShardTest(ShardTest, fixtures.MappedTest):
    __only_on__ = "postgresql"

    schema = "changeme"

    def _init_dbs(self):
        e1 = testing_engine("sqlite://")
        with e1.connect() as conn:
            for i in [1, 3]:
                conn.exec_driver_sql(
                    'ATTACH DATABASE "shard%s_%s.db" AS shard%s'
                    % (i, provision.FOLLOWER_IDENT, i)
                )

        e2 = testing_engine()
        with e2.begin() as conn:
            for i in [2, 4]:
                conn.exec_driver_sql(
                    "CREATE SCHEMA IF NOT EXISTS shard%s" % (i,)
                )

        db1 = e1.execution_options(schema_translate_map={"changeme": "shard1"})
        db2 = e2.execution_options(schema_translate_map={"changeme": "shard2"})
        db3 = e1.execution_options(schema_translate_map={"changeme": "shard3"})
        db4 = e2.execution_options(schema_translate_map={"changeme": "shard4"})

        self.sqlite_engine = e1
        self.postgresql_engine = e2
        return db1, db2, db3, db4

    def teardown_test(self):
        clear_mappers()

        # the tests in this suite don't cleanly close out the Session
        # at the moment so use the reaper to close all connections
        testing_reaper.checkin_all()

        for i in [1, 3]:
            os.remove("shard%d_%s.db" % (i, provision.FOLLOWER_IDENT))

        with self.postgresql_engine.begin() as conn:
            self.tables_test_metadata.drop_all(conn)
            for i in [2, 4]:
                conn.exec_driver_sql("DROP SCHEMA shard%s CASCADE" % (i,))
        self.postgresql_engine.dispose()


class SelectinloadRegressionTest(fixtures.DeclarativeMappedTest):
    """test #4175"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Book(Base):
            __tablename__ = "book"
            id = Column(Integer, primary_key=True)
            pages = relationship("Page")

        class Page(Base):
            __tablename__ = "page"
            id = Column(Integer, primary_key=True)
            book_id = Column(ForeignKey("book.id"))

    def test_selectinload_query(self):
        session = ShardedSession(
            shards={"test": testing.db},
            shard_chooser=lambda *args: "test",
            identity_chooser=lambda *args: None,
            execute_chooser=lambda *args: ["test"],
        )

        Book, Page = self.classes("Book", "Page")
        book = Book()
        book.pages.append(Page())

        session.add(book)
        session.commit()

        result = session.query(Book).options(selectinload(Book.pages)).all()
        eq_(result, [book])


class RefreshDeferExpireTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            data = Column(String(30))
            deferred_data = deferred(Column(String(30)))

    @classmethod
    def insert_data(cls, connection):
        A = cls.classes.A
        s = Session(connection)
        s.add(A(data="d1", deferred_data="d2"))
        s.commit()

    def _session_fixture(self, **kw):
        # the "fake" key here is to ensure that neither id_chooser
        # nor query_chooser are actually used, only shard_chooser
        # should be used.

        return ShardedSession(
            shards={"main": testing.db},
            shard_chooser=lambda *args: "main",
            identity_chooser=lambda *args: ["fake", "main"],
            execute_chooser=lambda *args: ["fake", "main"],
            **kw,
        )

    def test_refresh(self):
        A = self.classes.A
        session = self._session_fixture()
        a1 = session.query(A).set_shard("main").first()

        session.refresh(a1)

    def test_deferred(self):
        A = self.classes.A
        session = self._session_fixture()
        a1 = session.query(A).set_shard("main").first()

        eq_(a1.deferred_data, "d2")

    def test_unexpire(self):
        A = self.classes.A
        session = self._session_fixture()
        a1 = session.query(A).set_shard("main").first()

        session.expire(a1)
        eq_(a1.data, "d1")


class LazyLoadIdentityKeyTest(fixtures.DeclarativeMappedTest):
    def _init_dbs(self):
        self.db1 = db1 = testing_engine(
            "sqlite:///shard1_%s.db" % provision.FOLLOWER_IDENT
        )
        self.db2 = db2 = testing_engine(
            "sqlite:///shard2_%s.db" % provision.FOLLOWER_IDENT
        )

        for db in (db1, db2):
            self.tables_test_metadata.create_all(db)

        self.dbs = [db1, db2]

        return self.dbs

    def teardown_test(self):
        for db in self.dbs:
            db.connect().invalidate()

        testing_reaper.checkin_all()
        for i in range(1, 3):
            os.remove("shard%d_%s.db" % (i, provision.FOLLOWER_IDENT))

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Book(Base):
            __tablename__ = "book"
            id = Column(Integer, primary_key=True)
            title = Column(String(50), nullable=False)
            pages = relationship("Page", backref="book")

        class Page(Base):
            __tablename__ = "page"
            id = Column(Integer, primary_key=True)
            book_id = Column(ForeignKey("book.id"))
            title = Column(String(50))

    def _fixture(self, lazy_load_book=False, lazy_load_pages=False):
        Book, Page = self.classes("Book", "Page")

        def shard_for_book(book):
            if book.title == "title 1":
                return "test"
            elif book.title == "title 2":
                return "test2"
            else:
                assert False

        def identity_chooser(
            mapper,
            primary_key,
            *,
            lazy_loaded_from,
            execution_options,
            bind_arguments,
            **kw,
        ):
            assert lazy_loaded_from
            if isinstance(lazy_loaded_from.obj(), Book):
                token = shard_for_book(lazy_loaded_from.obj())
                assert lazy_loaded_from.identity_token == token

            return [lazy_loaded_from.identity_token]

        def execute_chooser(orm_context):
            if (
                orm_context.statement.column_descriptions[0]["type"] is Book
                and lazy_load_book
            ):
                assert isinstance(orm_context.lazy_loaded_from.obj(), Page)
            elif (
                orm_context.statement.column_descriptions[0]["type"] is Page
                and lazy_load_pages
            ):
                assert isinstance(orm_context.lazy_loaded_from.obj(), Book)

            if orm_context.lazy_loaded_from is None:
                return ["test", "test2"]
            else:
                return [orm_context.lazy_loaded_from.identity_token]

        def shard_chooser(mapper, instance, **kw):
            if isinstance(instance, Page):
                return shard_for_book(instance.book)
            else:
                return shard_for_book(instance)

        db1, db2 = self._init_dbs()
        session = ShardedSession(
            shards={"test": db1, "test2": db2},
            shard_chooser=shard_chooser,
            identity_chooser=identity_chooser,
            execute_chooser=execute_chooser,
        )

        return session

    def test_lazy_load_from_identity_map(self):
        session = self._fixture()

        Book, Page = self.classes("Book", "Page")
        book = Book(title="title 1")
        book.pages.append(Page())

        session.add(book)
        session.flush()

        page = session.query(Page).first()

        session.expire(page, ["book"])

        with self.assert_statement_count_multi_db(self.dbs, [0, 0]):
            # doesn't emit SQL
            eq_(page.book, book)

    def test_lazy_load_from_db(self):
        session = self._fixture(lazy_load_book=True)

        Book, Page = self.classes("Book", "Page")
        book1 = Book(title="title 1")
        book1.pages.append(Page(title="book 1 page 1"))

        session.add(book1)
        session.flush()

        book1_id = inspect(book1).identity_key
        session.expunge(book1)

        book1_page = session.query(Page).first()
        session.expire(book1_page, ["book"])

        with self.assert_statement_count_multi_db(self.dbs, [1, 0]):
            # emits one query
            eq_(inspect(book1_page.book).identity_key, book1_id)

    def test_lazy_load_no_baked_conflict(self):
        session = self._fixture(lazy_load_pages=True)

        Book, Page = self.classes("Book", "Page")
        book1 = Book(title="title 1")
        book1.pages.append(Page(title="book 1 page 1"))

        book2 = Book(title="title 2")
        book2.pages.append(Page(title="book 2 page 1"))

        session.add(book1)
        session.add(book2)
        session.flush()

        session.expire(book1, ["pages"])
        session.expire(book2, ["pages"])

        eq_(book1.pages[0].title, "book 1 page 1")

        # second lazy load uses correct state
        eq_(book2.pages[0].title, "book 2 page 1")


class UseAssocProxForM2MTest(fixtures.DeclarativeMappedTest):
    """illustrate the test case for #4376"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        from sqlalchemy.ext.associationproxy import association_proxy

        class Book(Base):
            __tablename__ = "book"
            id = Column(Integer, primary_key=True)
            authors = association_proxy(
                "book_authors",
                "author",
                creator=lambda author: BookAuthor(author=author),
            )
            book_authors = relationship("BookAuthor", back_populates="book")

        class BookAuthor(Base):
            __tablename__ = "book_author"
            authorid = Column(ForeignKey("author.id"), primary_key=True)
            bookid = Column(ForeignKey("book.id"), primary_key=True)

            book = relationship("Book", back_populates="book_authors")
            author = relationship("Author", back_populates="book_authors")

        class Author(Base):
            __tablename__ = "author"
            id = Column(Integer, primary_key=True)

            books = association_proxy(
                "book_authors",
                "book",
                creator=lambda book: BookAuthor(book=book),
            )

            book_authors = relationship(BookAuthor, back_populates="author")

    def test_update_many_to_many_sharded(self):
        session = ShardedSession(
            shards={"test": testing.db},
            shard_chooser=self.shard_chooser,
            identity_chooser=lambda *args: None,
            execute_chooser=lambda *args: ["test"],
        )

        Book, Author = self.classes("Book", "Author")
        book = Book()
        book.authors.append(Author())

        session.add(book)
        session.commit()

    def test_update_many_to_many_sharded__save_junction_table_directly(self):
        session = ShardedSession(
            shards={"test": testing.db},
            shard_chooser=self.shard_chooser,
            identity_chooser=lambda *args: None,
            execute_chooser=lambda *args: ["test"],
        )

        Book, Author, BookAuthor = self.classes("Book", "Author", "BookAuthor")

        book = Book()
        author = Author()

        session.add(book)
        session.add(author)
        session.commit()

        book_author = BookAuthor()
        book_author.bookid = book.id
        book_author.authorid = author.id

        session.add(book_author)
        session.commit()

    def shard_chooser(self, mapper, instance, clause=None):
        if not instance and not clause:
            raise Exception("Cannot determine shard")

        return "test"
