# engine/ddl.py
# Copyright (C) 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Routines to handle CREATE/DROP workflow."""

from sqlalchemy import engine, schema
from sqlalchemy.sql import util as sql_util


class DDLBase(schema.SchemaVisitor):
    def __init__(self, connection):
        self.connection = connection

class SchemaGenerator(DDLBase):
    def __init__(self, dialect, connection, checkfirst=False, tables=None, **kwargs):
        super(SchemaGenerator, self).__init__(connection, **kwargs)
        self.checkfirst = checkfirst
        self.tables = tables and set(tables) or None
        self.preparer = dialect.identifier_preparer
        self.dialect = dialect

    def _can_create(self, table):
        self.dialect.validate_identifier(table.name)
        if table.schema:
            self.dialect.validate_identifier(table.schema)
        return not self.checkfirst or not self.dialect.has_table(self.connection, table.name, schema=table.schema)

    def visit_metadata(self, metadata):
        if self.tables:
            tables = self.tables
        else:
            tables = metadata.tables.values()
        collection = [t for t in sql_util.sort_tables(tables) if self._can_create(t)]
        
        for listener in metadata.ddl_listeners['before-create']:
            listener('before-create', metadata, self.connection, tables=collection)
            
        for table in collection:
            self.traverse_single(table, create_ok=True)

        for listener in metadata.ddl_listeners['after-create']:
            listener('after-create', metadata, self.connection, tables=collection)

    def visit_table(self, table, create_ok=False):
        if not create_ok and not self._can_create(table):
            return
            
        for listener in table.ddl_listeners['before-create']:
            listener('before-create', table, self.connection)

        for column in table.columns:
            if column.default is not None:
                self.traverse_single(column.default)

        self.connection.execute(schema.CreateTable(table))

        if hasattr(table, 'indexes'):
            for index in table.indexes:
                self.traverse_single(index)

        for listener in table.ddl_listeners['after-create']:
            listener('after-create', table, self.connection)

    def visit_sequence(self, sequence):
        if self.dialect.supports_sequences:
            if ((not self.dialect.sequences_optional or
                 not sequence.optional) and
                (not self.checkfirst or
                 not self.dialect.has_sequence(self.connection, sequence.name, schema=sequence.schema))):
                self.connection.execute(schema.CreateSequence(sequence))

    def visit_index(self, index):
        self.connection.execute(schema.CreateIndex(index))


class SchemaDropper(DDLBase):
    def __init__(self, dialect, connection, checkfirst=False, tables=None, **kwargs):
        super(SchemaDropper, self).__init__(connection, **kwargs)
        self.checkfirst = checkfirst
        self.tables = tables
        self.preparer = dialect.identifier_preparer
        self.dialect = dialect

    def visit_metadata(self, metadata):
        if self.tables:
            tables = self.tables
        else:
            tables = metadata.tables.values()
        collection = [t for t in reversed(sql_util.sort_tables(tables)) if self._can_drop(t)]
        
        for listener in metadata.ddl_listeners['before-drop']:
            listener('before-drop', metadata, self.connection, tables=collection)
        
        for table in collection:
            self.traverse_single(table, drop_ok=True)

        for listener in metadata.ddl_listeners['after-drop']:
            listener('after-drop', metadata, self.connection, tables=collection)

    def _can_drop(self, table):
        self.dialect.validate_identifier(table.name)
        if table.schema:
            self.dialect.validate_identifier(table.schema)
        return not self.checkfirst or self.dialect.has_table(self.connection, table.name, schema=table.schema)

    def visit_index(self, index):
        self.connection.execute(schema.DropIndex(index))

    def visit_table(self, table, drop_ok=False):
        if not drop_ok and not self._can_drop(table):
            return
            
        for listener in table.ddl_listeners['before-drop']:
            listener('before-drop', table, self.connection)

        for column in table.columns:
            if column.default is not None:
                self.traverse_single(column.default)

        self.connection.execute(schema.DropTable(table))

        for listener in table.ddl_listeners['after-drop']:
            listener('after-drop', table, self.connection)

    def visit_sequence(self, sequence):
        if self.dialect.supports_sequences:
            if ((not self.dialect.sequences_optional or
                 not sequence.optional) and
                (not self.checkfirst or
                 self.dialect.has_sequence(self.connection, sequence.name, schema=sequence.schema))):
                self.connection.execute(schema.DropSequence(sequence))
