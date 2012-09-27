"""Requirement definitions used by the generic dialect suite.

External dialect test suites should subclass SuiteRequirements
to provide specific inclusion/exlusions.

"""
from ..requirements import Requirements
from .. import exclusions


class SuiteRequirements(Requirements):

    @property
    def create_table(self):
        """target platform can emit basic CreateTable DDL."""

        return exclusions.open

    @property
    def drop_table(self):
        """target platform can emit basic DropTable DDL."""

        return exclusions.open

    @property
    def autoincrement_insert(self):
        """target platform generates new surrogate integer primary key values
        when insert() is executed, excluding the pk column."""

        return exclusions.open
