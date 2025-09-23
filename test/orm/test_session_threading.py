import threading

import sqlalchemy as sa
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table

# Number of threads to use in concurrent tests
NUM_THREADS = 10


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

        results = {}
        errors = []
        barrier = threading.Barrier(parties=NUM_THREADS)

        def worker(thread_id):
            barrier.wait()
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
                results[thread_id] = count
            except Exception as e:
                errors.append((thread_id, str(e)))
                session.rollback()
            finally:
                session.close()

        threads = []
        for i in range(NUM_THREADS):
            t = threading.Thread(target=worker, args=(i,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

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
        results = {}
        barrier = threading.Barrier(parties=NUM_THREADS)

        def worker(thread_id):
            barrier.wait()

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
            results[thread_id] = count

            Session.remove()

        threads = []
        for i in range(NUM_THREADS):
            t = threading.Thread(target=worker, args=(i,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        unique_sessions = set(session_ids.values())
        eq_(len(unique_sessions), NUM_THREADS)

        for i in range(NUM_THREADS):
            eq_(results[i], 1)
