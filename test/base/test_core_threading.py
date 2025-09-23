import threading

import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table

NUM_THREADS = 10


class EngineThreadSafetyTest(fixtures.TablesTest):
    run_dispose_bind = "once"
    __requires__ = ("threading_with_mock",)

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

        results = {}
        errors = []
        barrier = threading.Barrier(parties=NUM_THREADS)

        def worker(thread_id):
            barrier.wait()
            try:
                with self.bind.connect() as conn:
                    conn.execute(
                        test_table.insert(),
                        {
                            "thread_id": thread_id,
                            "data": f"thread_{thread_id}",
                        },
                    )
                    conn.commit()

                    result = conn.execute(
                        sa.select(test_table.c.data).where(
                            test_table.c.thread_id == thread_id
                        )
                    ).scalar()
                    results[thread_id] = result
            except Exception as e:
                errors.append((thread_id, str(e)))

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
            eq_(results[i], f"thread_{i}")

    @testing.skip_if(testing.requires.sqlite_memory)
    def test_metadata_thread_safe(self):
        """Test that MetaData objects are thread-safe for reads."""
        metadata = sa.MetaData()

        for thread_id in range(NUM_THREADS):
            Table(
                f"thread_table_{thread_id}",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("data", String(50)),
            )

        metadata.create_all(self.bind)

        errors = []
        barrier = threading.Barrier(parties=NUM_THREADS)

        def create_table(thread_id):
            barrier.wait()
            table_key = f"thread_table_{thread_id}"
            if table_key not in metadata.tables:
                errors.append((thread_id, f"{table_key} does not exist"))

            with self.bind.connect() as conn:
                try:
                    conn.execute(sa.select(metadata.tables[table_key]))
                except Exception as e:
                    errors.append((thread_id, str(e)))

        threads = []
        for i in range(NUM_THREADS):
            t = threading.Thread(target=create_table, args=(i,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        eq_(errors, [])
