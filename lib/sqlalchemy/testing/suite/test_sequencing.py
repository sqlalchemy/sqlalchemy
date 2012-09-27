from .. import fixtures, config, util
from ..config import requirements
from ..assertions import eq_

from sqlalchemy import Table, Column, Integer, String


class InsertSequencingTest(fixtures.TablesTest):
    run_deletes = 'each'

    @classmethod
    def define_tables(cls, metadata):
        Table('plain_pk', metadata,
                Column('id', Integer, primary_key=True),
                Column('data', String(50))
            )

    def _assert_round_trip(self, table):
        row = config.db.execute(table.select()).first()
        eq_(
            row,
            (1, "some data")
        )

    @requirements.autoincrement_insert
    def test_autoincrement_on_insert(self):

        config.db.execute(
            self.tables.plain_pk.insert(),
            data="some data"
        )
        self._assert_round_trip(self.tables.plain_pk)



__all__ = ('InsertSequencingTest',)