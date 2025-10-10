import random
import threading
import time

import sqlalchemy as sa
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table

NUM_THREADS = 10
ITERATIONS = 10


class _ThreadTest:
    def run_threaded(
        self,
        func,
        *thread_args,
        nthreads=NUM_THREADS,
        use_barrier=False,
        **thread_kwargs,
    ):
        barrier = threading.Barrier(nthreads)
        results = []
        errors = []

        def thread_func(*args, **kwargs):
            thread_name = threading.current_thread().name
            if use_barrier:
                barrier.wait()

            local_result = []
            try:
                func(local_result, thread_name, *args, **kwargs)
                results.append(tuple(local_result))
            except Exception as e:
                # raise
                errors.append((thread_name, repr(e)))

        threads = [
            threading.Thread(
                name=f"thread-{i}",
                target=thread_func,
                args=thread_args,
                kwargs=thread_kwargs,
            )
            for i in range(nthreads)
        ]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        return results, errors

    @testing.fixture
    def num_threads_engine(self, testing_engine):
        return testing_engine(options=dict(pool_size=NUM_THREADS))


@testing.add_to_marker.timing_intensive
class EngineThreadSafetyTest(_ThreadTest, fixtures.TablesTest):
    run_dispose_bind = "once"

    __requires__ = ("multithreading_support",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("thread_id", Integer),
            Column("data", String(50)),
        )

    @testing.combinations(
        (NUM_THREADS, 0),
        (3, 5),
        (3, 0),
        (7, 0),
        argnames="pool_size, max_overflow",
    )
    def test_engine_thread_safe(self, testing_engine, pool_size, max_overflow):
        """Test that a single Engine can be safely shared across threads."""
        test_table = self.tables.test_table

        engine = testing_engine(
            options=dict(pool_size=pool_size, max_overflow=max_overflow)
        )

        def worker(results, thread_name):
            for _ in range(ITERATIONS):
                with engine.connect() as conn:
                    conn.execute(
                        test_table.insert(),
                        {"data": thread_name},
                    )
                    conn.commit()

                    result = conn.execute(
                        sa.select(test_table.c.data).where(
                            test_table.c.data == thread_name
                        )
                    ).scalar()
                    results.append(result)

        results, errors = self.run_threaded(worker)

        eq_(errors, [])
        eq_(
            set(results),
            {
                tuple([f"thread-{i}" for j in range(ITERATIONS)])
                for i in range(NUM_THREADS)
            },
        )

    def test_metadata_thread_safe(self, num_threads_engine):
        """Test that MetaData objects are thread-safe for reads."""
        metadata = sa.MetaData()

        for thread_id in range(NUM_THREADS):
            Table(
                f"thread-{thread_id}",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("data", String(50)),
            )

        metadata.create_all(testing.db)

        def worker(results, thread_name):
            table_key = thread_name
            assert table_key in metadata.tables, f"{table_key} does not exist"
            with num_threads_engine.connect() as conn:
                # Will raise if it cannot connect so erros will be populated
                conn.execute(sa.select(metadata.tables[table_key]))

        _, errors = self.run_threaded(worker)
        eq_(errors, [])


@testing.add_to_marker.timing_intensive
class SessionThreadingTest(_ThreadTest, fixtures.MappedTest):
    run_dispose_bind = "once"

    __requires__ = ("multithreading_support",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
            Column("thread_id", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

    def test_sessionmaker_thread_safe(self, num_threads_engine):
        """Test that sessionmaker factory is thread-safe."""
        users, User = self.tables.users, self.classes.User
        self.mapper_registry.map_imperatively(User, users)

        # Single sessionmaker shared across threads
        SessionFactory = sessionmaker(num_threads_engine)

        def worker(results, thread_name):
            thread_id = thread_name

            for _ in range(ITERATIONS):
                with SessionFactory() as session:
                    for i in range(3):
                        user = User(
                            name=f"user_{thread_id}_{i}", thread_id=thread_id
                        )
                        session.add(user)
                    session.commit()

                    count = (
                        session.query(User)
                        .filter_by(thread_id=thread_id)
                        .count()
                    )
                    results.append(count)

        results, errors = self.run_threaded(worker)

        eq_(errors, [])
        eq_(
            results,
            [
                tuple(range(3, 3 * ITERATIONS + 3, 3))
                for _ in range(NUM_THREADS)
            ],
        )

    def test_scoped_session_thread_local(self, num_threads_engine):
        """Test that scoped_session provides thread-local sessions."""
        users, User = self.tables.users, self.classes.User
        self.mapper_registry.map_imperatively(User, users)

        # Create scoped session
        Session = scoped_session(sessionmaker(num_threads_engine))

        session_ids = {}

        def worker(results, thread_name):
            thread_id = thread_name

            session = Session()
            session_ids[thread_id] = id(session)
            session.close()

            for _ in range(ITERATIONS):
                user = User(
                    name=f"scoped_user_{thread_id}", thread_id=thread_id
                )
                Session.add(user)
                Session.commit()

                session2 = Session()
                assert id(session2) == session_ids[thread_id]
                session2.close()

                count = (
                    Session.query(User).filter_by(thread_id=thread_id).count()
                )
                results.append(count)
            Session.remove()

        results, errors = self.run_threaded(worker)

        eq_(errors, [])
        unique_sessions = set(session_ids.values())
        eq_(len(unique_sessions), NUM_THREADS)
        eq_(
            results,
            [tuple(range(1, ITERATIONS + 1)) for _ in range(NUM_THREADS)],
        )


@testing.add_to_marker.timing_intensive
class FromClauseConcurrencyTest(_ThreadTest, fixtures.TestBase):
    """test for issue #12302"""

    @testing.variation("collection", ["c", "primary_key", "foreign_keys"])
    def test_c_collection(self, collection):
        dictionary_meta = MetaData()
        all_indexes_table = Table(
            "all_indexes",
            dictionary_meta,
            *[Column(f"col{i}", Integer) for i in range(50)],
        )

        def use_table(results, errors):
            for i in range(3):
                time.sleep(random.random() * 0.0001)
                if collection.c:
                    all_indexes.c.col35
                elif collection.primary_key:
                    all_indexes.primary_key
                elif collection.foreign_keys:
                    all_indexes.foreign_keys

        for j in range(1000):
            all_indexes = all_indexes_table.alias("a_indexes")

            results, errors = self.run_threaded(
                use_table, use_barrier=False, nthreads=5
            )

            eq_(errors, [])
            eq_(len(results), 5)
