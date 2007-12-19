import testbase
from testlib import testing
schema = None

__all__ = 'Table', 'Column',

table_options = {}

def Table(*args, **kw):
    """A schema.Table wrapper/hook for dialect-specific tweaks."""

    global schema
    if schema is None:
        from sqlalchemy import schema

    test_opts = dict([(k,kw.pop(k)) for k in kw.keys()
                      if k.startswith('test_')])

    kw.update(table_options)

    if testbase.db.name == 'mysql':
        if 'mysql_engine' not in kw and 'mysql_type' not in kw:
            if 'test_needs_fk' in test_opts or 'test_needs_acid' in test_opts:
                kw['mysql_engine'] = 'InnoDB'

    # Apply some default cascading rules for self-referential foreign keys.
    # MySQL InnoDB has some issues around seleting self-refs too.
    if testing.against('firebird'):
        table_name = args[0]
        unpack = (testing.config.db.dialect.
                  identifier_preparer.unformat_identifiers)

        # Only going after ForeignKeys in Columns.  May need to
        # expand to ForeignKeyConstraint too.
        fks = [fk
               for col in args if isinstance(col, schema.Column)
               for fk in col.args if isinstance(fk, schema.ForeignKey)]

        for fk in fks:
            # root around in raw spec
            ref = fk._colspec
            if isinstance(ref, schema.Column):
                name = ref.table.name
            else:
                # take just the table name: on FB there cannot be
                # a schema, so the first element is always the
                # table name, possibly followed by the field name
                name = unpack(ref)[0]
            print name, table_name
            if name == table_name:
                if fk.ondelete is None:
                    fk.ondelete = 'CASCADE'
                if fk.onupdate is None:
                    fk.onupdate = 'CASCADE'

    return schema.Table(*args, **kw)

def Column(*args, **kw):
    """A schema.Column wrapper/hook for dialect-specific tweaks."""

    global schema
    if schema is None:
        from sqlalchemy import schema

    # TODO: a Column that creates a Sequence automatically for PK columns,
    # which would help Oracle tests
    return schema.Column(*args, **kw)
