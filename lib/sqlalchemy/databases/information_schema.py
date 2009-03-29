"""
information schema implementation.

This module is deprecated and will not be present in this form in SQLAlchemy 0.6.

"""
from sqlalchemy import util

util.warn_deprecated("the information_schema module is deprecated.")

import sqlalchemy.sql as sql
import sqlalchemy.exc as exc
from sqlalchemy import select, MetaData, Table, Column, String, Integer
from sqlalchemy.schema import DefaultClause, ForeignKeyConstraint

ischema = MetaData()

schemata = Table("schemata", ischema,
    Column("catalog_name", String),
    Column("schema_name", String),
    Column("schema_owner", String),
    schema="information_schema")

tables = Table("tables", ischema,
    Column("table_catalog", String),
    Column("table_schema", String),
    Column("table_name", String),
    Column("table_type", String),
    schema="information_schema")

columns = Table("columns", ischema,
    Column("table_schema", String),
    Column("table_name", String),
    Column("column_name", String),
    Column("is_nullable", Integer),
    Column("data_type", String),
    Column("ordinal_position", Integer),
    Column("character_maximum_length", Integer),
    Column("numeric_precision", Integer),
    Column("numeric_scale", Integer),
    Column("column_default", Integer),
    Column("collation_name", String),
    schema="information_schema")

constraints = Table("table_constraints", ischema,
    Column("table_schema", String),
    Column("table_name", String),
    Column("constraint_name", String),
    Column("constraint_type", String),
    schema="information_schema")

column_constraints = Table("constraint_column_usage", ischema,
    Column("table_schema", String),
    Column("table_name", String),
    Column("column_name", String),
    Column("constraint_name", String),
    schema="information_schema")

pg_key_constraints = Table("key_column_usage", ischema,
    Column("table_schema", String),
    Column("table_name", String),
    Column("column_name", String),
    Column("constraint_name", String),
    Column("ordinal_position", Integer),
    schema="information_schema")

#mysql_key_constraints = Table("key_column_usage", ischema,
#    Column("table_schema", String),
#    Column("table_name", String),
#    Column("column_name", String),
#    Column("constraint_name", String),
#    Column("referenced_table_schema", String),
#    Column("referenced_table_name", String),
#    Column("referenced_column_name", String),
#    schema="information_schema")

key_constraints = pg_key_constraints

ref_constraints = Table("referential_constraints", ischema,
    Column("constraint_catalog", String),
    Column("constraint_schema", String),
    Column("constraint_name", String),
    Column("unique_constraint_catlog", String),
    Column("unique_constraint_schema", String),
    Column("unique_constraint_name", String),
    Column("match_option", String),
    Column("update_rule", String),
    Column("delete_rule", String),
    schema="information_schema")


def table_names(connection, schema):
    s = select([tables.c.table_name], tables.c.table_schema==schema)
    return [row[0] for row in connection.execute(s)]


def reflecttable(connection, table, include_columns, ischema_names):
    key_constraints = pg_key_constraints

    if table.schema is not None:
        current_schema = table.schema
    else:
        current_schema = connection.default_schema_name()

    s = select([columns],
        sql.and_(columns.c.table_name==table.name,
        columns.c.table_schema==current_schema),
        order_by=[columns.c.ordinal_position])

    c = connection.execute(s)
    found_table = False
    while True:
        row = c.fetchone()
        if row is None:
            break
        #print "row! " + repr(row)
 #       continue
        found_table = True
        (name, type, nullable, charlen, numericprec, numericscale, default) = (
            row[columns.c.column_name],
            row[columns.c.data_type],
            row[columns.c.is_nullable] == 'YES',
            row[columns.c.character_maximum_length],
            row[columns.c.numeric_precision],
            row[columns.c.numeric_scale],
            row[columns.c.column_default]
            )
        if include_columns and name not in include_columns:
            continue

        args = []
        for a in (charlen, numericprec, numericscale):
            if a is not None:
                args.append(a)
        coltype = ischema_names[type]
        #print "coltype " + repr(coltype) + " args " +  repr(args)
        coltype = coltype(*args)
        colargs = []
        if default is not None:
            colargs.append(DefaultClause(sql.text(default)))
        table.append_column(Column(name, coltype, nullable=nullable, *colargs))

    if not found_table:
        raise exc.NoSuchTableError(table.name)

    # we are relying on the natural ordering of the constraint_column_usage table to return the referenced columns
    # in an order that corresponds to the ordinal_position in the key_constraints table, otherwise composite foreign keys
    # wont reflect properly.  dont see a way around this based on whats available from information_schema
    s = select([constraints.c.constraint_name, constraints.c.constraint_type, constraints.c.table_name, key_constraints], use_labels=True, from_obj=[constraints.join(column_constraints, column_constraints.c.constraint_name==constraints.c.constraint_name).join(key_constraints, key_constraints.c.constraint_name==column_constraints.c.constraint_name)], order_by=[key_constraints.c.ordinal_position])
    s.append_column(column_constraints)
    s.append_whereclause(constraints.c.table_name==table.name)
    s.append_whereclause(constraints.c.table_schema==current_schema)
    colmap = [constraints.c.constraint_type, key_constraints.c.column_name, column_constraints.c.table_schema, column_constraints.c.table_name, column_constraints.c.column_name, constraints.c.constraint_name, key_constraints.c.ordinal_position]
    c = connection.execute(s)

    fks = {}
    while True:
        row = c.fetchone()
        if row is None:
            break
        (type, constrained_column, referred_schema, referred_table, referred_column, constraint_name, ordinal_position) = (
            row[colmap[0]],
            row[colmap[1]],
            row[colmap[2]],
            row[colmap[3]],
            row[colmap[4]],
            row[colmap[5]],
            row[colmap[6]]
        )
        #print "type %s on column %s to remote %s.%s.%s" % (type, constrained_column, referred_schema, referred_table, referred_column)
        if type == 'PRIMARY KEY':
            table.primary_key.add(table.c[constrained_column])
        elif type == 'FOREIGN KEY':
            try:
                fk = fks[constraint_name]
            except KeyError:
                fk = ([], [])
                fks[constraint_name] = fk
            if current_schema == referred_schema:
                referred_schema = table.schema
            if referred_schema is not None:
                Table(referred_table, table.metadata, autoload=True, schema=referred_schema, autoload_with=connection)
                refspec = ".".join([referred_schema, referred_table, referred_column])
            else:
                Table(referred_table, table.metadata, autoload=True, autoload_with=connection)
                refspec = ".".join([referred_table, referred_column])
            if constrained_column not in fk[0]:
                fk[0].append(constrained_column)
            if refspec not in fk[1]:
                fk[1].append(refspec)

    for name, value in fks.iteritems():
        table.append_constraint(ForeignKeyConstraint(value[0], value[1], name=name))
