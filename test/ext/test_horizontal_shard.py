import datetime
import os
from sqlalchemy import *
from sqlalchemy import event
from sqlalchemy import sql, util
from sqlalchemy.orm import *
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.sql import operators
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.testing import eq_
from sqlalchemy import testing

# TODO: ShardTest can be turned into a base for further subclasses


class ShardTest(object):
    __skip_if__ = (lambda: util.win32,)
    __requires__ = 'sqlite',

    schema = None

    def setUp(self):
        global db1, db2, db3, db4, weather_locations, weather_reports

        db1, db2, db3, db4 = self._init_dbs()

        meta = MetaData()
        ids = Table('ids', meta,
                    Column('nextid', Integer, nullable=False))

        def id_generator(ctx):
            # in reality, might want to use a separate transaction for this.

            c = db1.contextual_connect()
            nextid = c.execute(ids.select(for_update=True)).scalar()
            c.execute(ids.update(values={ids.c.nextid: ids.c.nextid + 1}))
            return nextid

        weather_locations = Table(
            "weather_locations", meta,
            Column('id', Integer, primary_key=True, default=id_generator),
            Column('continent', String(30), nullable=False),
            Column('city', String(50), nullable=False),
            schema=self.schema
        )

        weather_reports = Table(
            'weather_reports',
            meta,
            Column('id', Integer, primary_key=True),
            Column('location_id', Integer,
                   ForeignKey(weather_locations.c.id)),
            Column('temperature', Float),
            Column('report_time', DateTime,
                   default=datetime.datetime.now),
            schema=self.schema
            )

        for db in (db1, db2, db3, db4):
            meta.create_all(db)

        db1.execute(ids.insert(), nextid=1)

        self.setup_session()
        self.setup_mappers()

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

            if query._criterion is not None:
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
            def __init__(self, temperature, id_=None):
                self.temperature = temperature
                if id_:
                    self.id = id_

        mapper(WeatherLocation, weather_locations, properties={
            'reports': relationship(Report, backref='location'),
            'city': deferred(weather_locations.c.city),
        })

        mapper(Report, weather_reports)

    def _fixture_data(self):
        tokyo = WeatherLocation('Asia', 'Tokyo')
        newyork = WeatherLocation('North America', 'New York')
        toronto = WeatherLocation('North America', 'Toronto')
        london = WeatherLocation('Europe', 'London')
        dublin = WeatherLocation('Europe', 'Dublin')
        brasilia = WeatherLocation('South America', 'Brasila')
        quito = WeatherLocation('South America', 'Quito')
        tokyo.reports.append(Report(80.0, id_=1))
        newyork.reports.append(Report(75, id_=1))
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
        sess.flush()

        eq_(inspect(newyork).key[2], "north_america")
        eq_(inspect(newyork).identity_token, "north_america")
        eq_(inspect(dublin).key[2], "europe")
        eq_(inspect(dublin).identity_token, "europe")

        sess.commit()
        sess.close()
        return sess

    def test_roundtrip(self):
        sess = self._fixture_data()
        tokyo = sess.query(WeatherLocation).filter_by(city="Tokyo").one()
        tokyo.city  # reload 'city' attribute on tokyo
        sess.expire_all()
        eq_(db2.execute(weather_locations.select()).fetchall(), [(1,
            'Asia', 'Tokyo')])
        eq_(db1.execute(weather_locations.select()).fetchall(), [(2,
            'North America', 'New York'), (3, 'North America', 'Toronto')])
        eq_(sess.execute(weather_locations.select(), shard_id='asia')
            .fetchall(), [(1, 'Asia', 'Tokyo')])
        t = sess.query(WeatherLocation).get(tokyo.id)
        eq_(t.city, tokyo.city)
        eq_(t.reports[0].temperature, 80.0)
        north_american_cities = \
            sess.query(WeatherLocation).filter(
                WeatherLocation.continent == 'North America')
        eq_(set([c.city for c in north_american_cities]),
            set(['New York', 'Toronto']))
        asia_and_europe = \
            sess.query(WeatherLocation).filter(
                WeatherLocation.continent.in_(['Europe', 'Asia']))
        eq_(set([c.city for c in asia_and_europe]), set(['Tokyo',
            'London', 'Dublin']))

        # inspect the shard token stored with each instance
        eq_(
            set(inspect(c).key[2] for c in asia_and_europe),
            set(['europe', 'asia']))

        eq_(
            set(inspect(c).identity_token for c in asia_and_europe),
            set(['europe', 'asia']))

        newyork = sess.query(WeatherLocation).filter_by(city="New York").one()
        newyork_report = newyork.reports[0]
        tokyo_report = tokyo.reports[0]

        # same primary key, two identity keys
        eq_(
            inspect(newyork_report).identity_key,
            (Report, (1, ), "north_america")
        )
        eq_(
            inspect(tokyo_report).identity_key,
            (Report, (1, ), "asia")
        )

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

        eq_(inspect(t).key[2], 'asia')

    def test_get_baked_query_shard_id(self):
        sess = self._fixture_data()

        tokyo = sess.query(WeatherLocation).filter_by(city="Tokyo").one()
        tokyo.city
        sess.expunge_all()

        from sqlalchemy.ext.baked import BakedQuery

        bakery = BakedQuery.bakery()

        bq = bakery(lambda session: session.query(WeatherLocation))
        t = bq(sess).with_post_criteria(
            lambda q: q.set_shard("asia")).get(tokyo.id)
        eq_(t.city, tokyo.city)

        eq_(inspect(t).key[2], 'asia')

    def test_filter_baked_query_shard_id(self):
        sess = self._fixture_data()

        tokyo = sess.query(WeatherLocation).filter_by(city="Tokyo").one()
        tokyo.city
        sess.expunge_all()

        from sqlalchemy.ext.baked import BakedQuery

        bakery = BakedQuery.bakery()

        bq = bakery(lambda session: session.query(WeatherLocation)).\
            with_criteria(lambda q: q.filter_by(id=tokyo.id))
        t = bq(sess).with_post_criteria(
            lambda q: q.set_shard("asia")).one()
        eq_(t.city, tokyo.city)

    def test_shard_id_event(self):
        canary = []

        def load(instance, ctx):
            canary.append(ctx.attributes["shard_id"])

        event.listen(WeatherLocation, "load", load)
        sess = self._fixture_data()

        tokyo = sess.query(WeatherLocation).\
            filter_by(city="Tokyo").set_shard("asia").one()

        sess.query(WeatherLocation).all()
        eq_(
            canary,
            ['asia', 'north_america', 'north_america',
             'europe', 'europe', 'south_america',
             'south_america']
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

        Sess = sessionmaker(class_=Session, bind=db2,
                                      autoflush=True, autocommit=False)
        sess2 = Sess()

        t = get_tokyo(sess)
        eq_(t.city, tokyo.city)

        t = get_tokyo(sess2)
        eq_(t.city, tokyo.city)

    def test_bulk_update(self):
        sess = self._fixture_data()

        eq_(
            set(row.temperature for row in sess.query(Report.temperature)),
            {80.0, 75.0, 85.0}
        )

        temps = sess.query(Report).all()
        eq_(
            set(t.temperature for t in temps),
            {80.0, 75.0, 85.0}
        )

        sess.query(Report).filter(
            Report.temperature >= 80).update(
                {"temperature": Report.temperature + 6})

        eq_(
            set(row.temperature for row in sess.query(Report.temperature)),
            {86.0, 75.0, 91.0}
        )

        # test synchronize session as well
        eq_(
            set(t.temperature for t in temps),
            {86.0, 75.0, 91.0}
        )

    def test_bulk_delete(self):
        sess = self._fixture_data()

        temps = sess.query(Report).all()
        eq_(
            set(t.temperature for t in temps),
            {80.0, 75.0, 85.0}
        )

        sess.query(Report).filter(
            Report.temperature >= 80).delete()

        eq_(
            set(row.temperature for row in sess.query(Report.temperature)),
            {75.0}
        )

        # test synchronize session as well
        for t in temps:
            assert inspect(t).deleted is (t.temperature >= 80)



from sqlalchemy.testing import provision


class DistinctEngineShardTest(ShardTest, fixtures.TestBase):
    def _init_dbs(self):
        db1 = testing_engine(
            'sqlite:///shard1_%s.db' % provision.FOLLOWER_IDENT,
            options=dict(pool_threadlocal=True))
        db2 = testing_engine(
            'sqlite:///shard2_%s.db' % provision.FOLLOWER_IDENT)
        db3 = testing_engine(
            'sqlite:///shard3_%s.db' % provision.FOLLOWER_IDENT)
        db4 = testing_engine(
            'sqlite:///shard4_%s.db' % provision.FOLLOWER_IDENT)

        self.dbs = [db1, db2, db3, db4]
        return self.dbs

    def teardown(self):
        clear_mappers()

        for db in self.dbs:
            db.connect().invalidate()
        for i in range(1, 5):
            os.remove("shard%d_%s.db" % (i, provision.FOLLOWER_IDENT))


class AttachedFileShardTest(ShardTest, fixtures.TestBase):
    schema = "changeme"

    def _init_dbs(self):
        db1 = testing_engine('sqlite://', options={"execution_options":
                                                   {"shard_id": "shard1"}})
        db2 = db1.execution_options(shard_id="shard2")
        db3 = db1.execution_options(shard_id="shard3")
        db4 = db1.execution_options(shard_id="shard4")

        import re

        @event.listens_for(db1, "before_cursor_execute", retval=True)
        def _switch_shard(conn, cursor, stmt, params, context, executemany):
            shard_id = conn._execution_options['shard_id']
            # because SQLite can't just give us a "use" statement, we have
            # to use the schema hack to locate table names
            if shard_id:
                stmt = re.sub(r"\"?changeme\"?\.", shard_id + "_", stmt)

            return stmt, params

        return db1, db2, db3, db4


class SelectinloadRegressionTest(fixtures.DeclarativeMappedTest):
    """test #4175
    """
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Book(Base):
            __tablename__ = 'book'
            id = Column(Integer, primary_key=True)
            pages = relationship('Page')

        class Page(Base):
            __tablename__ = 'page'
            id = Column(Integer, primary_key=True)
            book_id = Column(ForeignKey('book.id'))

    def test_selectinload_query(self):
        session = ShardedSession(
            shards={"test": testing.db},
            shard_chooser=lambda *args: 'test',
            id_chooser=lambda *args: None,
            query_chooser=lambda *args: ['test']
        )

        Book, Page = self.classes("Book", "Page")
        book = Book()
        book.pages.append(Page())

        session.add(book)
        session.commit()

        result = session.query(Book).options(selectinload('pages')).all()
        eq_(result, [book])

class RefreshDeferExpireTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = 'a'
            id = Column(Integer, primary_key=True)
            data = Column(String(30))
            deferred_data = deferred(Column(String(30)))

    @classmethod
    def insert_data(cls):
        A = cls.classes.A
        s = Session()
        s.add(A(data='d1', deferred_data='d2'))
        s.commit()

    def _session_fixture(self):

        return ShardedSession(
            shards={
                "main": testing.db,
            },
            shard_chooser=lambda *args: 'main',
            id_chooser=lambda *args: ['fake', 'main'],
            query_chooser=lambda *args: ['fake', 'main']
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
            'sqlite:///shard1_%s.db' % provision.FOLLOWER_IDENT,
            options=dict(pool_threadlocal=True))
        self.db2 = db2 = testing_engine(
            'sqlite:///shard2_%s.db' % provision.FOLLOWER_IDENT)

        for db in (db1, db2):
            self.metadata.create_all(db)

        self.dbs = [db1, db2]

        return self.dbs

    def teardown(self):
        for db in self.dbs:
            db.connect().invalidate()
        for i in range(1, 3):
            os.remove("shard%d_%s.db" % (i, provision.FOLLOWER_IDENT))

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Book(Base):
            __tablename__ = 'book'
            id = Column(Integer, primary_key=True)
            title = Column(String(50), nullable=False)
            pages = relationship('Page', backref='book')

        class Page(Base):
            __tablename__ = 'page'
            id = Column(Integer, primary_key=True)
            book_id = Column(ForeignKey('book.id'))
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

        def id_chooser(query, ident):
            assert query.lazy_loaded_from
            if isinstance(query.lazy_loaded_from.obj(), Book):
                token = shard_for_book(query.lazy_loaded_from.obj())
                assert query.lazy_loaded_from.identity_token == token

            return [query.lazy_loaded_from.identity_token]

        def no_query_chooser(query):
            if query.column_descriptions[0]['type'] is Book and lazy_load_book:
                assert isinstance(query.lazy_loaded_from.obj(), Page)
            elif query.column_descriptions[0]['type'] is Page and lazy_load_pages:
                assert isinstance(query.lazy_loaded_from.obj(), Book)

            if query.lazy_loaded_from is None:
                return ['test', 'test2']
            else:
                return [query.lazy_loaded_from.identity_token]

        def shard_chooser(mapper, instance, **kw):
            if isinstance(instance, Page):
                return shard_for_book(instance.book)
            else:
                return shard_for_book(instance)

        db1, db2 = self._init_dbs()
        session = ShardedSession(
            shards={"test": db1, "test2": db2},
            shard_chooser=shard_chooser,
            id_chooser=id_chooser,
            query_chooser=no_query_chooser
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

        session.expire(page, ['book'])

        def go():
            eq_(page.book, book)

        # doesn't emit SQL
        self.assert_multiple_sql_count(
            self.dbs,
            go,
            [0, 0])

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
        session.expire(book1_page, ['book'])

        def go():
            eq_(inspect(book1_page.book).identity_key, book1_id)

        # emits one query
        self.assert_multiple_sql_count(
            self.dbs,
            go,
            [1, 0])

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

        session.expire(book1, ['pages'])
        session.expire(book2, ['pages'])

        eq_(book1.pages[0].title, "book 1 page 1")

        # second lazy load uses correct state
        eq_(book2.pages[0].title, "book 2 page 1")
