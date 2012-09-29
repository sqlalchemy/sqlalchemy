"""Global database feature support policy.

Provides decorators to mark tests requiring specific feature support from the
target database.

External dialect test suites should subclass SuiteRequirements
to provide specific inclusion/exlusions.

"""

from . import exclusions

class Requirements(object):
    def __init__(self, db, config):
        self.db = db
        self.config = config


class SuiteRequirements(Requirements):

    @property
    def create_table(self):
        """target platform can emit basic CreateTable DDL."""

        return exclusions.open()

    @property
    def drop_table(self):
        """target platform can emit basic DropTable DDL."""

        return exclusions.open()

    @property
    def autoincrement_insert(self):
        """target platform generates new surrogate integer primary key values
        when insert() is executed, excluding the pk column."""

        return exclusions.open()

    @property
    def returning(self):
        """target platform supports RETURNING."""

        return exclusions.closed()

    @property
    def dbapi_lastrowid(self):
        """"target platform includes a 'lastrowid' accessor on the DBAPI
        cursor object.

        """
        return exclusions.closed()

    @property
    def views(self):
        """Target database must support VIEWs."""

        return exclusions.closed()

    @property
    def schemas(self):
        """Target database must support external schemas, and have one
        named 'test_schema'."""

        return exclusions.closed()

    @property
    def sequences(self):
        """Target database must support SEQUENCEs."""

        return exclusions.only_if([
                lambda: self.config.db.dialect.supports_sequences
            ], "no SEQUENCE support")

    @property
    def reflects_pk_names(self):
        return exclusions.closed()

