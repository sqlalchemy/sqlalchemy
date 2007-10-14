# oracle.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


import sys, StringIO, string, re, warnings

from sqlalchemy import util, sql, engine, schema, ansisql, exceptions, logging
from sqlalchemy.engine import default, base
import sqlalchemy.types as sqltypes

import datetime


class OracleNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        if self.precision is None:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}

class OracleInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"

class OracleSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"

class OracleDate(sqltypes.Date):
    def get_col_spec(self):
        return "DATE"
    def convert_bind_param(self, value, dialect):
        return value
    def convert_result_value(self, value, dialect):
        if not isinstance(value, datetime.datetime):
            return value
        else:
            return value.date()

class OracleDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "DATE"
        
    def convert_result_value(self, value, dialect):
        if value is None or isinstance(value,datetime.datetime):
            return value
        else:
            # convert cx_oracle datetime object returned pre-python 2.4
            return datetime.datetime(value.year,value.month,
                value.day,value.hour, value.minute, value.second)

# Note:
# Oracle DATE == DATETIME
# Oracle does not allow milliseconds in DATE
# Oracle does not support TIME columns

# only if cx_oracle contains TIMESTAMP
class OracleTimestamp(sqltypes.TIMESTAMP):
    def get_col_spec(self):
        return "TIMESTAMP"

    def get_dbapi_type(self, dialect):
        return dialect.TIMESTAMP

    def convert_result_value(self, value, dialect):
        if value is None or isinstance(value,datetime.datetime):
            return value
        else:
            # convert cx_oracle datetime object returned pre-python 2.4
            return datetime.datetime(value.year,value.month,
                value.day,value.hour, value.minute, value.second)


class OracleString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}

class OracleText(sqltypes.TEXT):
    def get_dbapi_type(self, dbapi):
        return dbapi.CLOB

    def get_col_spec(self):
        return "CLOB"

    def convert_result_value(self, value, dialect):
        if value is None:
            return None
        else:
            return super(OracleText, self).convert_result_value(value.read(), dialect)


class OracleRaw(sqltypes.Binary):
    def get_col_spec(self):
        return "RAW(%(length)s)" % {'length' : self.length}

class OracleChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}

class OracleBinary(sqltypes.Binary):
    def get_dbapi_type(self, dbapi):
        return dbapi.BLOB

    def get_col_spec(self):
        return "BLOB"

    def convert_bind_param(self, value, dialect):
        return value

    def convert_result_value(self, value, dialect):
        if value is None:
            return None
        else:
            return value.read()

class OracleBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "SMALLINT"

    def convert_result_value(self, value, dialect):
        if value is None:
            return None
        return value and True or False

    def convert_bind_param(self, value, dialect):
        if value is True:
            return 1
        elif value is False:
            return 0
        elif value is None:
            return None
        else:
            return value and True or False

colspecs = {
    sqltypes.Integer : OracleInteger,
    sqltypes.Smallinteger : OracleSmallInteger,
    sqltypes.Numeric : OracleNumeric,
    sqltypes.Float : OracleNumeric,
    sqltypes.DateTime : OracleDateTime,
    sqltypes.Date : OracleDate,
    sqltypes.String : OracleString,
    sqltypes.Binary : OracleBinary,
    sqltypes.Boolean : OracleBoolean,
    sqltypes.TEXT : OracleText,
    sqltypes.TIMESTAMP : OracleTimestamp,
    sqltypes.CHAR: OracleChar,
}

ischema_names = {
    'VARCHAR2' : OracleString,
    'DATE' : OracleDate,
    'DATETIME' : OracleDateTime,
    'NUMBER' : OracleNumeric,
    'BLOB' : OracleBinary,
    'CLOB' : OracleText,
    'TIMESTAMP' : OracleTimestamp,
    'RAW' : OracleRaw,
    'FLOAT' : OracleNumeric,
    'DOUBLE PRECISION' : OracleNumeric,
    'LONG' : OracleText,
}

def descriptor():
    return {'name':'oracle',
    'description':'Oracle',
    'arguments':[
        ('dsn', 'Data Source Name', None),
        ('user', 'Username', None),
        ('password', 'Password', None)
    ]}

class OracleExecutionContext(default.DefaultExecutionContext):
    def pre_exec(self):
        super(OracleExecutionContext, self).pre_exec()
        if self.dialect.auto_setinputsizes:
            self.set_input_sizes()

    def get_result_proxy(self):
        if self.cursor.description is not None:
            if self.dialect.auto_convert_lobs and self.typemap is None:
                typemap = {}
                binary = False
                for column in self.cursor.description:
                    type_code = column[1]
                    if type_code in self.dialect.ORACLE_BINARY_TYPES:
                        binary = True
                        typemap[column[0].lower()] = OracleBinary()
                self.typemap = typemap
                if binary:
                    return base.BufferedColumnResultProxy(self)
            else:
                for column in self.cursor.description:
                    type_code = column[1]
                    if type_code in self.dialect.ORACLE_BINARY_TYPES:
                        return base.BufferedColumnResultProxy(self)
        
        return base.ResultProxy(self)

class OracleDialect(ansisql.ANSIDialect):
    def __init__(self, use_ansi=True, auto_setinputsizes=True, auto_convert_lobs=True, threaded=True, **kwargs):
        ansisql.ANSIDialect.__init__(self, default_paramstyle='named', **kwargs)
        self.use_ansi = use_ansi
        self.threaded = threaded
        self.supports_timestamp = self.dbapi is None or hasattr(self.dbapi, 'TIMESTAMP' )
        self.auto_setinputsizes = auto_setinputsizes
        self.auto_convert_lobs = auto_convert_lobs
        if self.dbapi is not None:
            self.ORACLE_BINARY_TYPES = [getattr(self.dbapi, k) for k in ["BFILE", "CLOB", "NCLOB", "BLOB"] if hasattr(self.dbapi, k)]
        else:
            self.ORACLE_BINARY_TYPES = []

    def dbapi(cls):
        import cx_Oracle
        return cx_Oracle
    dbapi = classmethod(dbapi)
    
    def create_connect_args(self, url):
        if url.database:
            # if we have a database, then we have a remote host
            port = url.port
            if port:
                port = int(port)
            else:
                port = 1521
            dsn = self.dbapi.makedsn(url.host,port,url.database)
        else:
            # we have a local tnsname
            dsn = url.host
        opts = dict(
            user=url.username,
            password=url.password,
            dsn = dsn,
            threaded = self.threaded
            )
        opts.update(url.query)
        util.coerce_kw_type(opts, 'use_ansi', bool)
        return ([], opts)

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def supports_unicode_statements(self):
        """indicate whether the DBAPI can receive SQL statements as Python unicode strings"""
        return False

    def max_identifier_length(self):
        return 30
        
    def oid_column_name(self, column):
        if not isinstance(column.table, sql.TableClause) and not isinstance(column.table, sql.Select):
            return None
        else:
            return "rowid"

    def create_execution_context(self, *args, **kwargs):
        return OracleExecutionContext(self, *args, **kwargs)

    def compiler(self, statement, bindparams, **kwargs):
        return OracleCompiler(self, statement, bindparams, **kwargs)

    def schemagenerator(self, *args, **kwargs):
        return OracleSchemaGenerator(self, *args, **kwargs)

    def schemadropper(self, *args, **kwargs):
        return OracleSchemaDropper(self, *args, **kwargs)

    def defaultrunner(self, connection, **kwargs):
        return OracleDefaultRunner(connection, **kwargs)

    def has_table(self, connection, table_name, schema=None):
        cursor = connection.execute("""select table_name from all_tables where table_name=:name""", {'name':table_name.upper()})
        return bool( cursor.fetchone() is not None )

    def has_sequence(self, connection, sequence_name):
        cursor = connection.execute("""select sequence_name from all_sequences where sequence_name=:name""", {'name':sequence_name.upper()})
        return bool( cursor.fetchone() is not None )

    def _locate_owner_row(self, owner, name, rows, raiseerr=False):
        """return the row in the given list of rows which references the given table name and owner name."""
        if not rows:
            if raiseerr:
                raise exceptions.NoSuchTableError(name)
            else:
                return None
        else:
            if owner is not None:
                for row in rows:
                    if owner.upper() in row[0]:
                        return row
                else:
                    if raiseerr:
                        raise exceptions.AssertionError("Specified owner %s does not own table %s" % (owner, name))
                    else:
                        return None
            else:
                if len(rows)==1:
                    return rows[0]
                else:
                    if raiseerr:
                        raise exceptions.AssertionError("There are multiple tables with name '%s' visible to the schema, you must specifiy owner" % name)
                    else:
                        return None

    def _resolve_table_owner(self, connection, name, table, dblink=''):
        """Locate the given table in the ``ALL_TAB_COLUMNS`` view,
        including searching for equivalent synonyms and dblinks.
        """

        c = connection.execute ("select distinct OWNER from ALL_TAB_COLUMNS%(dblink)s where TABLE_NAME = :table_name" % {'dblink':dblink}, {'table_name':name})
        rows = c.fetchall()
        try:
            row = self._locate_owner_row(table.owner, name, rows, raiseerr=True)
            return name, row['OWNER'], ''
        except exceptions.SQLAlchemyError:
            # locate synonyms
            c = connection.execute ("""select OWNER, TABLE_OWNER, TABLE_NAME, DB_LINK
                                       from   ALL_SYNONYMS%(dblink)s
                                       where  SYNONYM_NAME = :synonym_name
                                       and (DB_LINK IS NOT NULL
                                               or ((TABLE_NAME, TABLE_OWNER) in
                                                    (select TABLE_NAME, OWNER from ALL_TAB_COLUMNS%(dblink)s)))""" % {'dblink':dblink},
                                    {'synonym_name':name})
            rows = c.fetchall()
            row = self._locate_owner_row(table.owner, name, rows)
            if row is None:
                row = self._locate_owner_row("PUBLIC", name, rows)

            if row is not None:
                owner, name, dblink = row['TABLE_OWNER'], row['TABLE_NAME'], row['DB_LINK']
                if dblink:
                    dblink = '@' + dblink
                    if not owner:
                        # re-resolve table owner using new dblink variable
                        t1, owner, t2 = self._resolve_table_owner(connection, name, table, dblink=dblink)
                else:
                    dblink = ''
                return name, owner, dblink
            raise

    def reflecttable(self, connection, table):
        preparer = self.identifier_preparer
        if not preparer.should_quote(table):
            name = table.name.upper()
        else:
            name = table.name

        # search for table, including across synonyms and dblinks.
        # locate the actual name of the table, the real owner, and any dblink clause needed.
        actual_name, owner, dblink = self._resolve_table_owner(connection, name, table)

        c = connection.execute ("select COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT from ALL_TAB_COLUMNS%(dblink)s where TABLE_NAME = :table_name and OWNER = :owner" % {'dblink':dblink}, {'table_name':actual_name, 'owner':owner})

        while True:
            row = c.fetchone()
            if row is None:
                break
            found_table = True

            #print "ROW:" , row
            (colname, coltype, length, precision, scale, nullable, default) = (row[0], row[1], row[2], row[3], row[4], row[5]=='Y', row[6])

            # INTEGER if the scale is 0 and precision is null
            # NUMBER if the scale and precision are both null
            # NUMBER(9,2) if the precision is 9 and the scale is 2
            # NUMBER(3) if the precision is 3 and scale is 0
            #length is ignored except for CHAR and VARCHAR2
            if coltype=='NUMBER' :
                if precision is None and scale is None:
                    coltype = OracleNumeric
                elif precision is None and scale == 0  :
                    coltype = OracleInteger
                else :
                    coltype = OracleNumeric(precision, scale)
            elif coltype=='CHAR' or coltype=='VARCHAR2':
                coltype = ischema_names.get(coltype, OracleString)(length)
            else:
                coltype = re.sub(r'\(\d+\)', '', coltype)
                try:
                    coltype = ischema_names[coltype]
                except KeyError:
                    raise exceptions.AssertionError("Can't get coltype for type '%s' on colname '%s'" % (coltype, colname))

            colargs = []
            if default is not None:
                colargs.append(schema.PassiveDefault(sql.text(default)))

            # if name comes back as all upper, assume its case folded
            if (colname.upper() == colname):
                colname = colname.lower()

            table.append_column(schema.Column(colname, coltype, nullable=nullable, *colargs))

        if not len(table.columns):
           raise exceptions.AssertionError("Couldn't find any column information for table %s" % actual_name)

        c = connection.execute("""SELECT
             ac.constraint_name,
             ac.constraint_type,
             LOWER(loc.column_name) AS local_column,
             LOWER(rem.table_name) AS remote_table,
             LOWER(rem.column_name) AS remote_column,
             LOWER(rem.owner) AS remote_owner
           FROM all_constraints%(dblink)s ac,
             all_cons_columns%(dblink)s loc,
             all_cons_columns%(dblink)s rem
           WHERE ac.table_name = :table_name
           AND ac.constraint_type IN ('R','P')
           AND ac.owner = :owner
           AND ac.owner = loc.owner
           AND ac.constraint_name = loc.constraint_name
           AND ac.r_owner = rem.owner(+)
           AND ac.r_constraint_name = rem.constraint_name(+)
           -- order multiple primary keys correctly
           ORDER BY ac.constraint_name, loc.position, rem.position"""
         % {'dblink':dblink}, {'table_name' : actual_name, 'owner' : owner})

        fks = {}
        while True:
            row = c.fetchone()
            if row is None:
                break
            #print "ROW:" , row
            (cons_name, cons_type, local_column, remote_table, remote_column, remote_owner) = row
            if cons_type == 'P':
                table.primary_key.add(table.c[local_column])
            elif cons_type == 'R':
                try:
                    fk = fks[cons_name]
                except KeyError:
                   fk = ([], [])
                   fks[cons_name] = fk
                if remote_table is None:
                    # ticket 363
                    warnings.warn("Got 'None' querying 'table_name' from all_cons_columns%(dblink)s - does the user have proper rights to the table?" % {'dblink':dblink})
                    continue
                refspec = ".".join([remote_table, remote_column])
                schema.Table(remote_table, table.metadata, autoload=True, autoload_with=connection, owner=remote_owner)
                if local_column not in fk[0]:
                    fk[0].append(local_column)
                if refspec not in fk[1]:
                    fk[1].append(refspec)

        for name, value in fks.iteritems():
            table.append_constraint(schema.ForeignKeyConstraint(value[0], value[1], name=name))

    def do_executemany(self, c, statement, parameters, context=None):
        rowcount = 0
        for param in parameters:
            c.execute(statement, param)
            rowcount += c.rowcount
        if context is not None:
            context._rowcount = rowcount


OracleDialect.logger = logging.class_logger(OracleDialect)

class OracleCompiler(ansisql.ANSICompiler):
    """Oracle compiler modifies the lexical structure of Select
    statements to work under non-ANSI configured Oracle databases, if
    the use_ansi flag is False.
    """

    def __init__(self, *args, **kwargs):
        super(OracleCompiler, self).__init__(*args, **kwargs)
        # we have to modify SELECT objects a little bit, so store state here
        self._select_state = {}
        
    def default_from(self):
        """Called when a ``SELECT`` statement has no froms, and no ``FROM`` clause is to be appended.

        The Oracle compiler tacks a "FROM DUAL" to the statement.
        """

        return " FROM DUAL"

    def apply_function_parens(self, func):
        return len(func.clauses) > 0

    def visit_join(self, join):
        if self.dialect.use_ansi:
            return ansisql.ANSICompiler.visit_join(self, join)

        self.froms[join] = self.get_from_text(join.left) + ", " + self.get_from_text(join.right)
        where = self.wheres.get(join.left, None)
        if where is not None:
            self.wheres[join] = sql.and_(where, join.onclause)
        else:
            self.wheres[join] = join.onclause
#        self.wheres[join] = sql.and_(self.wheres.get(join.left, None), join.onclause)
        self.strings[join] = self.froms[join]

        if join.isouter:
            # if outer join, push on the right side table as the current "outertable"
            self._outertable = join.right

            # now re-visit the onclause, which will be used as a where clause
            # (the first visit occured via the Join object itself right before it called visit_join())
            self.traverse(join.onclause)

            self._outertable = None

        self.wheres[join].accept_visitor(self)

    def visit_insert_sequence(self, column, sequence, parameters):
        """This is the `sequence` equivalent to ``ANSICompiler``'s
        `visit_insert_column_default` which ensures that the column is
        present in the generated column list.
        """

        parameters.setdefault(column.key, None)

    def visit_alias(self, alias):
        """Oracle doesn't like ``FROM table AS alias``.  Is the AS standard SQL??"""

        self.froms[alias] = self.get_from_text(alias.original) + " " + alias.name
        self.strings[alias] = self.get_str(alias.original)

    def visit_column(self, column):
        ansisql.ANSICompiler.visit_column(self, column)
        if not self.dialect.use_ansi and getattr(self, '_outertable', None) is not None and column.table is self._outertable:
            self.strings[column] = self.strings[column] + "(+)"

    def visit_insert(self, insert):
        """``INSERT`` s are required to have the primary keys be explicitly present.

         Mapper will by default not put them in the insert statement
         to comply with autoincrement fields that require they not be
         present.  so, put them all in for all primary key columns.
         """

        for c in insert.table.primary_key:
            if not self.parameters.has_key(c.key):
                self.parameters[c.key] = None
        return ansisql.ANSICompiler.visit_insert(self, insert)

    def _TODO_visit_compound_select(self, select):
        """Need to determine how to get ``LIMIT``/``OFFSET`` into a ``UNION`` for Oracle."""

        if getattr(select, '_oracle_visit', False):
            # cancel out the compiled order_by on the select
            if hasattr(select, "order_by_clause"):
                self.strings[select.order_by_clause] = ""
            ansisql.ANSICompiler.visit_compound_select(self, select)
            return

        if select.limit is not None or select.offset is not None:
            select._oracle_visit = True
            # to use ROW_NUMBER(), an ORDER BY is required.
            orderby = self.strings[select.order_by_clause]
            if not orderby:
                orderby = select.oid_column
                self.traverse(orderby)
                orderby = self.strings[orderby]
            class SelectVisitor(sql.NoColumnVisitor):
                def visit_select(self, select):
                    select.append_column(sql.literal_column("ROW_NUMBER() OVER (ORDER BY %s)" % orderby).label("ora_rn"))
            SelectVisitor().traverse(select)
            limitselect = sql.select([c for c in select.c if c.key!='ora_rn'])
            if select.offset is not None:
                limitselect.append_whereclause("ora_rn>%d" % select.offset)
                if select.limit is not None:
                    limitselect.append_whereclause("ora_rn<=%d" % (select.limit + select.offset))
            else:
                limitselect.append_whereclause("ora_rn<=%d" % select.limit)
            self.traverse(limitselect)
            self.strings[select] = self.strings[limitselect]
            self.froms[select] = self.froms[limitselect]
        else:
            ansisql.ANSICompiler.visit_compound_select(self, select)

    def visit_select(self, select):
        """Look for ``LIMIT`` and OFFSET in a select statement, and if
        so tries to wrap it in a subquery with ``row_number()`` criterion.
        """

        # TODO: put a real copy-container on Select and copy, or somehow make this
        # not modify the Select statement
        if self._select_state.get((select, 'visit'), False):
            # cancel out the compiled order_by on the select
            if hasattr(select, "order_by_clause"):
                self.strings[select.order_by_clause] = ""
            ansisql.ANSICompiler.visit_select(self, select)
            return

        if select.limit is not None or select.offset is not None:
            self._select_state[(select, 'visit')] = True
            # to use ROW_NUMBER(), an ORDER BY is required.
            orderby = self.strings[select.order_by_clause]
            if not orderby:
                orderby = select.oid_column
                self.traverse(orderby)
                orderby = self.strings[orderby]
            if not hasattr(select, '_oracle_visit'):
                select.append_column(sql.literal_column("ROW_NUMBER() OVER (ORDER BY %s)" % orderby).label("ora_rn"))
                select._oracle_visit = True
            limitselect = sql.select([c for c in select.c if c.key!='ora_rn'])
            if select.offset is not None:
                limitselect.append_whereclause("ora_rn>%d" % select.offset)
                if select.limit is not None:
                    limitselect.append_whereclause("ora_rn<=%d" % (select.limit + select.offset))
            else:
                limitselect.append_whereclause("ora_rn<=%d" % select.limit)
            self.traverse(limitselect)
            self.strings[select] = self.strings[limitselect]
            self.froms[select] = self.froms[limitselect]
        else:
            ansisql.ANSICompiler.visit_select(self, select)

    def limit_clause(self, select):
        return ""

    def for_update_clause(self, select):
        if select.for_update=="nowait":
            return " FOR UPDATE NOWAIT"
        else:
            return super(OracleCompiler, self).for_update_clause(select)

    def visit_binary(self, binary):
        if binary.operator == '%': 
            self.strings[binary] = ("MOD(%s,%s)"%(self.get_str(binary.left), self.get_str(binary.right)))
        else:
            return ansisql.ANSICompiler.visit_binary(self, binary)
        

class OracleSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        colspec += " " + column.type.dialect_impl(self.dialect).get_col_spec()
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    def visit_sequence(self, sequence):
        if not self.dialect.has_sequence(self.connection, sequence.name):
            self.append("CREATE SEQUENCE %s" % self.preparer.format_sequence(sequence))
            self.execute()

class OracleSchemaDropper(ansisql.ANSISchemaDropper):
    def visit_sequence(self, sequence):
        if self.dialect.has_sequence(self.connection, sequence.name):
            self.append("DROP SEQUENCE %s" % sequence.name)
            self.execute()

class OracleDefaultRunner(ansisql.ANSIDefaultRunner):
    def exec_default_sql(self, default):
        c = sql.select([default.arg], from_obj=["DUAL"]).compile(engine=self.connection)
        return self.connection.execute_compiled(c).scalar()

    def visit_sequence(self, seq):
        return self.connection.execute_text("SELECT " + seq.name + ".nextval FROM DUAL").scalar()

dialect = OracleDialect
