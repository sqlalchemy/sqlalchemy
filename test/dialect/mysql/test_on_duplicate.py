from sqlalchemy.testing.assertions import eq_, assert_raises
from sqlalchemy.testing import fixtures
from sqlalchemy import exc, testing
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import Table, Column, Boolean, Integer, String, func


class OnDuplicateTest(fixtures.TablesTest):
    __only_on__ = 'mysql',
    __backend__ = True
    run_define_tables = 'each'

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'foos', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('bar', String(10)),
            Column('baz', String(10)),
            Column('updated_once', Boolean, default=False),
        )

    def test_bad_args(self):
        assert_raises(
            ValueError,
            insert(self.tables.foos, values={}).on_duplicate_key_update
        )
        assert_raises(
            exc.ArgumentError,
            insert(self.tables.foos, values={}).on_duplicate_key_update,
            {'id': 1, 'bar': 'b'},
            id=1,
            bar='b',
        )
        assert_raises(
            exc.ArgumentError,
            insert(self.tables.foos, values={}).on_duplicate_key_update,
            {'id': 1, 'bar': 'b'},
            {'id': 2, 'bar': 'baz'},
        )

    def test_on_duplicate_key_update(self):
        foos = self.tables.foos
        with testing.db.connect() as conn:
            conn.execute(insert(foos, dict(id=1, bar='b', baz='bz')))
            stmt = insert(foos).values(
                [dict(id=1, bar='ab'), dict(id=2, bar='b')])
            stmt = stmt.on_duplicate_key_update(bar=stmt.inserted.bar)
            result = conn.execute(stmt)
            eq_(result.inserted_primary_key, [2])
            eq_(
                conn.execute(foos.select().where(foos.c.id == 1)).fetchall(),
                [(1, 'ab', 'bz', False)]
            )

    def test_on_duplicate_key_update_preserve_order(self):
        foos = self.tables.foos
        with testing.db.connect() as conn:
            conn.execute(insert(foos,
                [dict(id=1, bar='b', baz='bz'), dict(id=2, bar='b', baz='bz2')]))

            stmt = insert(foos)
            update_condition = (foos.c.updated_once == False)

            # The following statements show importance of the columns update ordering
            # as old values being referenced in UPDATE clause are getting replaced one
            # by one from left to right with their new values.
            stmt1 = stmt.on_duplicate_key_update([
                ('bar', func.if_(update_condition, func.values(foos.c.bar), foos.c.bar)),
                ('updated_once', func.if_(update_condition, True, foos.c.updated_once)),
            ])
            stmt2 = stmt.on_duplicate_key_update([
                ('updated_once', func.if_(update_condition, True, foos.c.updated_once)),
                ('bar', func.if_(update_condition, func.values(foos.c.bar), foos.c.bar)),
            ])
            # First statement should succeed updating column bar
            conn.execute(stmt1, dict(id=1, bar='ab'))
            eq_(
                conn.execute(foos.select().where(foos.c.id == 1)).fetchall(),
                [(1, 'ab', 'bz', True)],
            )
            # Second statement will do noop update of column bar
            conn.execute(stmt2, dict(id=2, bar='ab'))
            eq_(
                conn.execute(foos.select().where(foos.c.id == 2)).fetchall(),
                [(2, 'b', 'bz2', True)]
            )

    def test_last_inserted_id(self):
        foos = self.tables.foos
        with testing.db.connect() as conn:
            stmt = insert(foos).values({"bar": "b", "baz": "bz"})
            result = conn.execute(
                stmt.on_duplicate_key_update(
                    bar=stmt.inserted.bar, baz="newbz")
            )
            eq_(result.inserted_primary_key, [1])

            stmt = insert(foos).values({"id": 1, "bar": "b", "baz": "bz"})
            result = conn.execute(
                stmt.on_duplicate_key_update(
                    bar=stmt.inserted.bar, baz="newbz")
            )
            eq_(result.inserted_primary_key, [1])
