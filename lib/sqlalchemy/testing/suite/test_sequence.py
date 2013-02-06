from .. import fixtures, config
from ..config import requirements
from ..assertions import eq_

from sqlalchemy import Integer, String, Sequence

from ..schema import Table, Column

class SequenceTest(fixtures.TablesTest):
    __requires__ = ('sequences',)

    run_create_tables = 'each'

    @classmethod
    def define_tables(cls, metadata):
        Table('seq_pk', metadata,
                Column('id', Integer, Sequence('tab_id_seq'), primary_key=True),
                Column('data', String(50))
            )

    def test_insert_roundtrip(self):
        config.db.execute(
            self.tables.seq_pk.insert(),
            data="some data"
        )
        self._assert_round_trip(self.tables.seq_pk, config.db)

    def test_insert_lastrowid(self):
        r = config.db.execute(
            self.tables.seq_pk.insert(),
            data="some data"
        )
        eq_(
            r.inserted_primary_key,
            [1]
        )

    def test_nextval_direct(self):
        r = config.db.execute(
            self.tables.seq_pk.c.id.default
        )
        eq_(
            r, 1
        )



    def _assert_round_trip(self, table, conn):
        row = conn.execute(table.select()).first()
        eq_(
            row,
            (1, "some data")
        )

