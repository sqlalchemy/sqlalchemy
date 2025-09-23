import threading

import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table

NUM_THREADS = 10


def run_threaded(func, *thread_args, nthreads=NUM_THREADS, **thread_kwargs):
    barrier = threading.Barrier(nthreads)
    results = []
    errors = []

    def thread_func(*args, **kwargs):
        name = threading.current_thread().name
        barrier.wait()
        try:
            result = func(*args, **kwargs)
            results.append(result)
        except Exception as e:
            errors.append((name, str(e)))

    threads = []
    for i in range(nthreads):
        thread = threading.Thread(
            name=f"thread-{i}",
            target=thread_func,
            args=thread_args,
            kwargs=thread_kwargs,
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    return results, errors


class EngineThreadSafetyTest(fixtures.TablesTest):
    run_dispose_bind = "once"

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

    @classmethod
    def setup_bind(cls):
        # Create engine with sufficient pool size for concurrent testing
        return create_engine(testing.db.url, pool_size=NUM_THREADS)

    @testing.skip_if(testing.requires.sqlite_memory)
    def test_engine_thread_safe(self):
        """Test that a single Engine can be safely shared across threads."""
        test_table = self.tables.test_table

        def worker():
            thread_name = threading.current_thread().name
            with self.bind.connect() as conn:
                conn.execute(
                    test_table.insert(),
                    {
                        "data": thread_name,
                    },
                )
                conn.commit()

                result = conn.execute(
                    sa.select(test_table.c.data).where(
                        test_table.c.data == thread_name
                    )
                ).scalar()
                return result

        results, errors = run_threaded(worker)

        eq_(errors, [])
        eq_(len(results), NUM_THREADS)
        for i in range(NUM_THREADS):
            in_(f"thread-{i}", results)

    @testing.skip_if(testing.requires.sqlite_memory)
    def test_metadata_thread_safe(self):
        """Test that MetaData objects are thread-safe for reads."""
        metadata = sa.MetaData()

        for thread_id in range(NUM_THREADS):
            Table(
                f"thread-{thread_id}",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("data", String(50)),
            )

        metadata.create_all(self.bind)

        def create_table():
            table_key = threading.current_thread().name
            assert table_key in metadata.tables, f"{table_key} does not exist"
            with self.bind.connect() as conn:
                # Will raise if it cannot connect so erros will be populated
                conn.execute(sa.select(metadata.tables[table_key]))

        _, errors = run_threaded(create_table)
        eq_(errors, [])


class SessionThreadingTest(fixtures.MappedTest):
    run_dispose_bind = "once"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
            Column("thread_id", Integer),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

    @classmethod
    def setup_bind(cls):
        return sa.create_engine(testing.db.url, pool_size=NUM_THREADS)

    @testing.skip_if(testing.requires.sqlite_memory)
    def test_sessionmaker_thread_safe(self):
        """Test that sessionmaker factory is thread-safe."""
        users, User = self.tables.users, self.classes.User
        self.mapper_registry.map_imperatively(User, users)

        # Single sessionmaker shared across threads
        SessionFactory = sessionmaker(self.bind)

        def worker():
            thread_id = threading.current_thread().name

            # Each thread creates its own session from the factory
            session = SessionFactory()

            try:
                for i in range(3):
                    user = User(
                        name=f"user_{thread_id}_{i}", thread_id=thread_id
                    )
                    session.add(user)
                session.commit()

                count = (
                    session.query(User).filter_by(thread_id=thread_id).count()
                )
                return count
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        results, errors = run_threaded(worker)

        eq_(errors, [])
        eq_(len(results), NUM_THREADS)
        for i in range(NUM_THREADS):
            eq_(results[i], 3)

    @testing.skip_if(testing.requires.sqlite_memory)
    def test_scoped_session_thread_local(self):
        """Test that scoped_session provides thread-local sessions."""
        users, User = self.tables.users, self.classes.User
        self.mapper_registry.map_imperatively(User, users)

        # Create scoped session
        Session = scoped_session(sessionmaker(self.bind))

        session_ids = {}

        def worker():
            thread_id = threading.current_thread().name

            session = Session()
            session_ids[thread_id] = id(session)
            session.close()

            user = User(name=f"scoped_user_{thread_id}", thread_id=thread_id)
            Session.add(user)
            Session.commit()

            session2 = Session()
            assert id(session2) == session_ids[thread_id]
            session2.close()

            count = Session.query(User).filter_by(thread_id=thread_id).count()
            Session.remove()
            return count

        results, errors = run_threaded(worker)

        eq_(errors, [])
        unique_sessions = set(session_ids.values())
        eq_(len(unique_sessions), NUM_THREADS)
        for i in range(NUM_THREADS):
            eq_(results[i], 1)
