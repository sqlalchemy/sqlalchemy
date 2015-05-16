from .. import fixtures, config
from ..config import requirements
from sqlalchemy import exc
from sqlalchemy import Integer, String
from .. import assert_raises
from ..schema import Table, Column


class ExceptionTest(fixtures.TablesTest):
    """Test basic exception wrapping.

    DBAPIs vary a lot in exception behavior so to actually anticipate
    specific exceptions from real round trips, we need to be conservative.

    """
    run_deletes = 'each'

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table('manual_pk', metadata,
              Column('id', Integer, primary_key=True, autoincrement=False),
              Column('data', String(50))
              )

    @requirements.duplicate_key_raises_integrity_error
    def test_integrity_error(self):

        with config.db.begin() as conn:
            conn.execute(
                self.tables.manual_pk.insert(),
                {'id': 1, 'data': 'd1'}
            )

            assert_raises(
                exc.IntegrityError,
                conn.execute,
                self.tables.manual_pk.insert(),
                {'id': 1, 'data': 'd1'}
            )
