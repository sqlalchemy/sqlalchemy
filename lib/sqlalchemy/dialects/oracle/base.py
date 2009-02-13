# oracle/base.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Support for the Oracle database.

Oracle version 8 through current (11g at the time of this writing) are supported.

For information on connecting via specific drivers, see the documentation
for that driver.

Connect Arguments
-----------------

The dialect supports several :func:`~sqlalchemy.create_engine()` arguments which 
affect the behavior of the dialect regardless of driver in use.

* *use_ansi* - Use ANSI JOIN constructs (see the section on Oracle 8).  Defaults
  to ``True``.  If ``False``, Oracle-8 compatible constructs are used for joins.

* *optimize_limits* - defaults to ``False``. see the section on LIMIT/OFFSET.

Auto Increment Behavior
-----------------------

SQLAlchemy Table objects which include integer primary keys are usually assumed to have
"autoincrementing" behavior, meaning they can generate their own primary key values upon
INSERT.  Since Oracle has no "autoincrement" feature, SQLAlchemy relies upon sequences 
to produce these values.   With the Oracle dialect, *a sequence must always be explicitly
specified to enable autoincrement*.  This is divergent with the majority of documentation 
examples which assume the usage of an autoincrement-capable database.   To specify sequences,
use the sqlalchemy.schema.Sequence object which is passed to a Column construct::

  t = Table('mytable', metadata, 
        Column('id', Integer, Sequence('id_seq'), primary_key=True),
        Column(...), ...
  )

This step is also required when using table reflection, i.e. autoload=True::

  t = Table('mytable', metadata, 
        Column('id', Integer, Sequence('id_seq'), primary_key=True),
        autoload=True
  ) 

LIMIT/OFFSET Support
--------------------

Oracle has no support for the LIMIT or OFFSET keywords.  Whereas previous versions of SQLAlchemy
used the "ROW NUMBER OVER..." construct to simulate LIMIT/OFFSET, SQLAlchemy 0.5 now uses 
a wrapped subquery approach in conjunction with ROWNUM.  The exact methodology is taken from
http://www.oracle.com/technology/oramag/oracle/06-sep/o56asktom.html .  Note that the 
"FIRST ROWS()" optimization keyword mentioned is not used by default, as the user community felt
this was stepping into the bounds of optimization that is better left on the DBA side, but this
prefix can be added by enabling the optimize_limits=True flag on create_engine().

Oracle 8 Compatibility
----------------------

When using Oracle 8, a "use_ansi=False" flag is available which converts all
JOIN phrases into the WHERE clause, and in the case of LEFT OUTER JOIN
makes use of Oracle's (+) operator.

Synonym/DBLINK Reflection
-------------------------

When using reflection with Table objects, the dialect can optionally search for tables
indicated by synonyms that reference DBLINK-ed tables by passing the flag 
oracle_resolve_synonyms=True as a keyword argument to the Table construct.  If DBLINK 
is not in use this flag should be left off.

"""

import datetime, random, re

from sqlalchemy import util, sql, schema, log
from sqlalchemy.engine import default, base
from sqlalchemy.sql import compiler, visitors, expression
from sqlalchemy.sql import operators as sql_operators, functions as sql_functions
from sqlalchemy import types as sqltypes

RESERVED_WORDS = set('''SHARE RAW DROP BETWEEN FROM DESC OPTION PRIOR LONG THEN DEFAULT ALTER IS INTO MINUS INTEGER NUMBER GRANT IDENTIFIED ALL TO ORDER ON FLOAT DATE HAVING CLUSTER NOWAIT RESOURCE ANY TABLE INDEX FOR UPDATE WHERE CHECK SMALLINT WITH DELETE BY ASC REVOKE LIKE SIZE RENAME NOCOMPRESS NULL GROUP VALUES AS IN VIEW EXCLUSIVE COMPRESS SYNONYM SELECT INSERT EXISTS NOT TRIGGER ELSE CREATE INTERSECT PCTFREE DISTINCT CONNECT SET MODE OF UNIQUE VARCHAR2 VARCHAR LOCK OR CHAR DECIMAL UNION PUBLIC AND START'''.split()) 

class OracleDate(sqltypes.Date):
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect):
        def process(value):
            if not isinstance(value, datetime.datetime):
                return value
            else:
                return value.date()
        return process

class OracleDateTime(sqltypes.DateTime):
    def result_processor(self, dialect):
        def process(value):
            if value is None or isinstance(value, datetime.datetime):
                return value
            else:
                # convert cx_oracle datetime object returned pre-python 2.4
                return datetime.datetime(value.year, value.month,
                    value.day,value.hour, value.minute, value.second)
        return process

# Note:
# Oracle DATE == DATETIME
# Oracle does not allow milliseconds in DATE
# Oracle does not support TIME columns

# only if cx_oracle contains TIMESTAMP
class OracleTimestamp(sqltypes.TIMESTAMP):
    def result_processor(self, dialect):
        def process(value):
            if value is None or isinstance(value, datetime.datetime):
                return value
            else:
                # convert cx_oracle datetime object returned pre-python 2.4
                return datetime.datetime(value.year, value.month,
                    value.day,value.hour, value.minute, value.second)
        return process

class OracleText(sqltypes.Text):
    def get_dbapi_type(self, dbapi):
        return dbapi.CLOB

    def result_processor(self, dialect):
        super_process = super(OracleText, self).result_processor(dialect)
        if not dialect.auto_convert_lobs:
            return super_process
        lob = dialect.dbapi.LOB
        def process(value):
            if isinstance(value, lob):
                if super_process:
                    return super_process(value.read())
                else:
                    return value.read()
            else:
                if super_process:
                    return super_process(value)
                else:
                    return value
        return process


class OracleBinary(sqltypes.Binary):
    def get_dbapi_type(self, dbapi):
        return dbapi.BLOB

    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect):
        if not dialect.auto_convert_lobs:
            return None
        lob = dialect.dbapi.LOB
        def process(value):
            if isinstance(value, lob):
                return value.read()
            else:
                return value
        return process

class OracleRaw(OracleBinary):
    def get_col_spec(self):
        return "RAW(%(length)s)" % {'length' : self.length}

class OracleBoolean(sqltypes.Boolean):
    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return value and True or False
        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is True:
                return 1
            elif value is False:
                return 0
            elif value is None:
                return None
            else:
                return value and True or False
        return process

colspecs = {
    sqltypes.DateTime : OracleDateTime,
    sqltypes.Date : OracleDate,
    sqltypes.Binary : OracleBinary,
    sqltypes.Boolean : OracleBoolean,
    sqltypes.Text : OracleText,
    sqltypes.TIMESTAMP : OracleTimestamp,
}

ischema_names = {
    'VARCHAR2' : sqltypes.VARCHAR,
    'NVARCHAR2' : sqltypes.NVARCHAR,
    'CHAR' : sqltypes.CHAR,
    'DATE' : sqltypes.DATE,
    'DATETIME' : sqltypes.DATETIME,
    'NUMBER' : sqltypes.Numeric,
    'BLOB' : sqltypes.BLOB,
    'BFILE' : sqltypes.Binary,
    'CLOB' : sqltypes.CLOB,
    'TIMESTAMP' : sqltypes.TIMESTAMP,
    'RAW' : OracleRaw,
    'FLOAT' : sqltypes.Float,
    'DOUBLE PRECISION' : sqltypes.Numeric,
    'LONG' : sqltypes.Text,
}


class OracleTypeCompiler(compiler.GenericTypeCompiler):
    # Note:
    # Oracle DATE == DATETIME
    # Oracle does not allow milliseconds in DATE
    # Oracle does not support TIME columns
    
    def visit_datetime(self, type_):
        return self.visit_DATE(type_)
        
    def visit_VARCHAR(self, type_):
        return "VARCHAR(%(length)s)" % {'length' : type_.length}

    def visit_NVARCHAR(self, type_):
        return "NVARCHAR2(%(length)s)" % {'length' : type_.length}
    
    def visit_text(self, type_):
        return self.visit_CLOB(type_)

    def visit_binary(self, type_):
        return self.visit_BLOB(type_)
    
    def visit_boolean(self, type_):
        return self.visit_SMALLINT(type_)
    
    def visit_RAW(self, type_):
        return "RAW(%(length)s)" % {'length' : type_.length}

class OracleCompiler(compiler.SQLCompiler):
    """Oracle compiler modifies the lexical structure of Select
    statements to work under non-ANSI configured Oracle databases, if
    the use_ansi flag is False.
    """

    operators = util.update_copy(
        compiler.SQLCompiler.operators,
        {
            sql_operators.mod : lambda x, y:"mod(%s, %s)" % (x, y),
            sql_operators.match_op: lambda x, y: "CONTAINS (%s, %s)" % (x, y)
        }
    )

    functions = util.update_copy(
        compiler.SQLCompiler.functions,
        {
            sql_functions.now : 'CURRENT_TIMESTAMP'
        }
    )

    def __init__(self, *args, **kwargs):
        super(OracleCompiler, self).__init__(*args, **kwargs)
        self.__wheres = {}
        self._quoted_bind_names = {}

    def bindparam_string(self, name):
        if self.preparer._bindparam_requires_quotes(name):
            quoted_name = '"%s"' % name
            self._quoted_bind_names[name] = quoted_name
            return compiler.SQLCompiler.bindparam_string(self, quoted_name)
        else:
            return compiler.SQLCompiler.bindparam_string(self, name)

    def default_from(self):
        """Called when a ``SELECT`` statement has no froms, and no ``FROM`` clause is to be appended.

        The Oracle compiler tacks a "FROM DUAL" to the statement.
        """

        return " FROM DUAL"

    def apply_function_parens(self, func):
        return len(func.clauses) > 0

    def visit_join(self, join, **kwargs):
        if self.dialect.use_ansi:
            return compiler.SQLCompiler.visit_join(self, join, **kwargs)
        else:
            return self.process(join.left, asfrom=True) + ", " + self.process(join.right, asfrom=True)

    def _get_nonansi_join_whereclause(self, froms):
        clauses = []

        def visit_join(join):
            if join.isouter:
                def visit_binary(binary):
                    if binary.operator == sql_operators.eq:
                        if binary.left.table is join.right:
                            binary.left = _OuterJoinColumn(binary.left)
                        elif binary.right.table is join.right:
                            binary.right = _OuterJoinColumn(binary.right)
                clauses.append(visitors.cloned_traverse(join.onclause, {}, {'binary':visit_binary}))
            else:
                clauses.append(join.onclause)

        for f in froms:
            visitors.traverse(f, {}, {'join':visit_join})
        return sql.and_(*clauses)

    def visit_outer_join_column(self, vc):
        return self.process(vc.column) + "(+)"

    def visit_sequence(self, seq):
        return self.dialect.identifier_preparer.format_sequence(seq) + ".nextval"

    def visit_alias(self, alias, asfrom=False, **kwargs):
        """Oracle doesn't like ``FROM table AS alias``.  Is the AS standard SQL??"""

        if asfrom:
            return self.process(alias.original, asfrom=asfrom, **kwargs) + " " + self.preparer.format_alias(alias, self._anonymize(alias.name))
        else:
            return self.process(alias.original, **kwargs)

    def _TODO_visit_compound_select(self, select):
        """Need to determine how to get ``LIMIT``/``OFFSET`` into a ``UNION`` for Oracle."""
        pass

    def visit_select(self, select, **kwargs):
        """Look for ``LIMIT`` and OFFSET in a select statement, and if
        so tries to wrap it in a subquery with ``rownum`` criterion.
        """

        if not getattr(select, '_oracle_visit', None):
            if not self.dialect.use_ansi:
                if self.stack and 'from' in self.stack[-1]:
                    existingfroms = self.stack[-1]['from']
                else:
                    existingfroms = None

                froms = select._get_display_froms(existingfroms)
                whereclause = self._get_nonansi_join_whereclause(froms)
                if whereclause:
                    select = select.where(whereclause)
                    select._oracle_visit = True

            if select._limit is not None or select._offset is not None:
                # See http://www.oracle.com/technology/oramag/oracle/06-sep/o56asktom.html
                #
                # Generalized form of an Oracle pagination query:
                #   select ... from (
                #     select /*+ FIRST_ROWS(N) */ ...., rownum as ora_rn from (
                #         select distinct ... where ... order by ...
                #     ) where ROWNUM <= :limit+:offset
                #   ) where ora_rn > :offset
                # Outer select and "ROWNUM as ora_rn" can be dropped if limit=0

                # TODO: use annotations instead of clone + attr set ?
                select = select._generate()
                select._oracle_visit = True

                # Wrap the middle select and add the hint
                limitselect = sql.select([c for c in select.c])
                if select._limit and self.dialect.optimize_limits:
                    limitselect = limitselect.prefix_with("/*+ FIRST_ROWS(%d) */" % select._limit)

                limitselect._oracle_visit = True
                limitselect._is_wrapper = True

                # If needed, add the limiting clause
                if select._limit is not None:
                    max_row = select._limit
                    if select._offset is not None:
                        max_row += select._offset
                    limitselect.append_whereclause(
                            sql.literal_column("ROWNUM")<=max_row)

                # If needed, add the ora_rn, and wrap again with offset.
                if select._offset is None:
                    select = limitselect
                else:
                     limitselect = limitselect.column(
                             sql.literal_column("ROWNUM").label("ora_rn"))
                     limitselect._oracle_visit = True
                     limitselect._is_wrapper = True

                     offsetselect = sql.select(
                             [c for c in limitselect.c if c.key!='ora_rn'])
                     offsetselect._oracle_visit = True
                     offsetselect._is_wrapper = True

                     offsetselect.append_whereclause(
                             sql.literal_column("ora_rn")>select._offset)

                     select = offsetselect

        kwargs['iswrapper'] = getattr(select, '_is_wrapper', False)
        return compiler.SQLCompiler.visit_select(self, select, **kwargs)

    def limit_clause(self, select):
        return ""

    def for_update_clause(self, select):
        if select.for_update == "nowait":
            return " FOR UPDATE NOWAIT"
        else:
            return super(OracleCompiler, self).for_update_clause(select)

class OracleDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        colspec += " " + self.dialect.type_compiler.process(column.type)
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    def visit_create_sequence(self, create):
        return "CREATE SEQUENCE %s" % self.preparer.format_sequence(create.element)

    def visit_drop_sequence(self, drop):
        return "DROP SEQUENCE %s" % self.preparer.format_sequence(drop.element)

class OracleDefaultRunner(base.DefaultRunner):
    def visit_sequence(self, seq):
        return self.execute_string("SELECT " + self.dialect.identifier_preparer.format_sequence(seq) + ".nextval FROM DUAL", {})

class OracleIdentifierPreparer(compiler.IdentifierPreparer):
    
    reserved_words = set([x.lower() for x in RESERVED_WORDS])

    def _bindparam_requires_quotes(self, value):
        """Return True if the given identifier requires quoting."""
        lc_value = value.lower()
        return (lc_value in self.reserved_words
                or self.illegal_initial_characters.match(value[0])
                or not self.legal_characters.match(unicode(value))
                )
    
    def format_savepoint(self, savepoint):
        name = re.sub(r'^_+', '', savepoint.ident)
        return super(OracleIdentifierPreparer, self).format_savepoint(savepoint, name)
        
class OracleInfoCache(default.DefaultInfoCache):
    pass

class OracleDialect(default.DefaultDialect):
    name = 'oracle'
    supports_alter = True
    supports_unicode_statements = False
    supports_unicode_binds = False
    max_identifier_length = 30
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    supports_sequences = True
    sequences_optional = False
    preexecute_pk_sequences = True
    supports_pk_autoincrement = False
    default_paramstyle = 'named'
    colspecs = colspecs
    ischema_names = ischema_names
    
    supports_default_values = False
    supports_empty_insert = False
    
    statement_compiler = OracleCompiler
    ddl_compiler = OracleDDLCompiler
    type_compiler = OracleTypeCompiler
    preparer = OracleIdentifierPreparer
    defaultrunner = OracleDefaultRunner
    info_cache = OracleInfoCache
    
    def __init__(self, 
                use_ansi=True, 
                optimize_limits=False, 
                **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)
        self.use_ansi = use_ansi
        self.optimize_limits = optimize_limits

    def has_table(self, connection, table_name, schema=None):
        if not schema:
            schema = self.get_default_schema_name(connection)
        cursor = connection.execute("""select table_name from all_tables where table_name=:name and owner=:schema_name""", {'name':self._denormalize_name(table_name), 'schema_name':self._denormalize_name(schema)})
        return cursor.fetchone() is not None

    def has_sequence(self, connection, sequence_name, schema=None):
        if not schema:
            schema = self.get_default_schema_name(connection)
        cursor = connection.execute("""select sequence_name from all_sequences where sequence_name=:name and sequence_owner=:schema_name""", {'name':self._denormalize_name(sequence_name), 'schema_name':self._denormalize_name(schema)})
        return cursor.fetchone() is not None

    def _normalize_name(self, name):
        if name is None:
            return None
        elif name.upper() == name and not self.identifier_preparer._requires_quotes(name.lower().decode(self.encoding)):
            return name.lower().decode(self.encoding)
        else:
            return name.decode(self.encoding)

    def _denormalize_name(self, name):
        if name is None:
            return None
        elif name.lower() == name and not self.identifier_preparer._requires_quotes(name.lower()):
            return name.upper().encode(self.encoding)
        else:
            return name.encode(self.encoding)

    @base.connection_memoize(('dialect', 'default_schema_name'))
    def get_default_schema_name(self, connection):
        return self._normalize_name(connection.execute('SELECT USER FROM DUAL').scalar())

    def table_names(self, connection, schema):
        # note that table_names() isnt loading DBLINKed or synonym'ed tables
        if schema is None:
            s = "select table_name from all_tables where nvl(tablespace_name, 'no tablespace') NOT IN ('SYSTEM', 'SYSAUX')"
            cursor = connection.execute(s)
        else:
            s = "select table_name from all_tables where nvl(tablespace_name, 'no tablespace') NOT IN ('SYSTEM','SYSAUX') AND OWNER = :owner"
            cursor = connection.execute(s, {'owner': self._denormalize_name(schema)})
        return [self._normalize_name(row[0]) for row in cursor]

    def _resolve_synonym(self, connection, desired_owner=None, desired_synonym=None, desired_table=None):
        """search for a local synonym matching the given desired owner/name.

        if desired_owner is None, attempts to locate a distinct owner.

        returns the actual name, owner, dblink name, and synonym name if found.
        """

        sql = """select OWNER, TABLE_OWNER, TABLE_NAME, DB_LINK, SYNONYM_NAME
                   from   ALL_SYNONYMS WHERE """

        clauses = []
        params = {}
        if desired_synonym:
            clauses.append("SYNONYM_NAME=:synonym_name")
            params['synonym_name'] = desired_synonym
        if desired_owner:
            clauses.append("TABLE_OWNER=:desired_owner")
            params['desired_owner'] = desired_owner
        if desired_table:
            clauses.append("TABLE_NAME=:tname")
            params['tname'] = desired_table

        sql += " AND ".join(clauses)

        result = connection.execute(sql, **params)
        if desired_owner:
            row = result.fetchone()
            if row:
                return row['TABLE_NAME'], row['TABLE_OWNER'], row['DB_LINK'], row['SYNONYM_NAME']
            else:
                return None, None, None, None
        else:
            rows = result.fetchall()
            if len(rows) > 1:
                raise AssertionError("There are multiple tables visible to the schema, you must specify owner")
            elif len(rows) == 1:
                row = rows[0]
                return row['TABLE_NAME'], row['TABLE_OWNER'], row['DB_LINK'], row['SYNONYM_NAME']
            else:
                return None, None, None, None

    def _prepare_reflection_args(self, connection, tablename, schemaname=None,
                                 resolve_synonyms=False, dblink=''):

        if resolve_synonyms:
            actual_name, owner, dblink, synonym = self._resolve_synonym(connection, desired_owner=self._denormalize_name(schemaname), desired_synonym=self._denormalize_name(tablename))
        else:
            actual_name, owner, dblink, synonym = None, None, None, None
        if not actual_name:
            actual_name = self._denormalize_name(tablename)
        if not dblink:
            dblink = ''
        if not owner:
            owner = self._denormalize_name(schemaname or self.get_default_schema_name(connection))
        return (actual_name, owner, dblink, synonym)

    def get_columns(self, connection, tablename, schemaname=None,
                    info_cache=None, resolve_synonyms=False, dblink=''):

        
        (tablename, schemaname, dblink, synonym) = \
            self._prepare_reflection_args(connection, tablename, schemaname,
                                          resolve_synonyms, dblink)
        if info_cache:
            columns = info_cache.getColumns(tablename, schemaname)
            if columns:
                return columns
        columns = []
        c = connection.execute ("select COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT from ALL_TAB_COLUMNS%(dblink)s where TABLE_NAME = :table_name and OWNER = :owner" % {'dblink':dblink}, {'table_name':tablename, 'owner':schemaname})

        while True:
            row = c.fetchone()
            if row is None:
                break

            (colname, coltype, length, precision, scale, nullable, default) = (self._normalize_name(row[0]), row[1], row[2], row[3], row[4], row[5]=='Y', row[6])

            # INTEGER if the scale is 0 and precision is null
            # NUMBER if the scale and precision are both null
            # NUMBER(9,2) if the precision is 9 and the scale is 2
            # NUMBER(3) if the precision is 3 and scale is 0
            #length is ignored except for CHAR and VARCHAR2
            if coltype == 'NUMBER' :
                if precision is None and scale is None:
                    coltype = sqltypes.NUMERIC
                elif precision is None and scale == 0  :
                    coltype = sqltypes.INTEGER
                else :
                    coltype = sqltypes.NUMERIC(precision, scale)
            elif coltype=='CHAR' or coltype=='VARCHAR2':
                coltype = self.ischema_names.get(coltype)(length)
            else:
                coltype = re.sub(r'\(\d+\)', '', coltype)
                try:
                    coltype = self.ischema_names[coltype]
                except KeyError:
                    util.warn("Did not recognize type '%s' of column '%s'" %
                              (coltype, colname))
                    coltype = sqltypes.NULLTYPE

            colargs = []
            if default is not None:
                colargs.append(schema.DefaultClause(sql.text(default)))
            cdict = {
                'name': colname,
                'type': coltype,
                'nullable': nullable,
                'default': default,
                'attrs': colargs
            }
            columns.append(cdict)
        if info_cache:
            info_cache.setColumns(columns, tablename, schemaname)
        return columns

    def _get_constraint_data(self, connection, tablename, schemaname=None,
                             info_cache=None, dblink=''):

        if info_cache:
            table_cache = info_cache.getTable(tablename, schemaname)
            if table_cache and ['constraints'] in table_cache.keys():
                return table_cache['constraints']
        rp = connection.execute("""SELECT
             ac.constraint_name,
             ac.constraint_type,
             loc.column_name AS local_column,
             rem.table_name AS remote_table,
             rem.column_name AS remote_column,
             rem.owner AS remote_owner
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
         % {'dblink':dblink}, {'table_name' : tablename, 'owner' : schemaname})
        constraint_data = rp.fetchall()
        if info_cache:
            table_cache = info_cache.getTable(tablename, schemaname,
                                              create=True)
            table_cache['constraints'] = constraint_data
        return constraint_data

    def get_primary_keys(self, connection, tablename, schemaname=None,
                         info_cache=None, resolve_synonyms=False, dblink=''):
        (tablename, schemaname, dblink, synonym) = \
            self._prepare_reflection_args(connection, tablename, schemaname,
                                          resolve_synonyms, dblink)
        if info_cache:
            pkeys = info_cache.getPrimaryKeys(tablename, schemaname)
            if pkeys is not None:
                return pkeys
        pkeys = []
        constraint_data = self._get_constraint_data(connection, tablename,
                                        schemaname, info_cache, dblink)
        for row in constraint_data:
            #print "ROW:" , row
            (cons_name, cons_type, local_column, remote_table, remote_column, remote_owner) = row[0:2] + tuple([self._normalize_name(x) for x in row[2:]])
            if cons_type == 'P':
                pkeys.append(local_column)
        if info_cache:
            info_cache.setPrimaryKeys(pkeys, tablename, schemaname)
        return pkeys

    def get_foreign_keys(self, connection, tablename, schemaname=None,
                         info_cache=None, resolve_synonyms=False, dblink=''):
        (tablename, schemaname, dblink, synonym) = \
            self._prepare_reflection_args(connection, tablename, schemaname,
                                          resolve_synonyms, dblink)
        if info_cache:
            fkeys = info_cache.getForeignKeys(tablename, schemaname)
            if fkeys is not None:
                return fkeys

        constraint_data = self._get_constraint_data(connection, tablename,
                                                schemaname, info_cache, dblink)
        fkeys = []
        fks = {}
        for row in constraint_data:
            (cons_name, cons_type, local_column, remote_table, remote_column, remote_owner) = row[0:2] + tuple([self._normalize_name(x) for x in row[2:]])
            if cons_type == 'R':
                try:
                    fk = fks[cons_name]
                except KeyError:
                    fk = ([], [])
                    fks[cons_name] = fk
                if remote_table is None:
                    # ticket 363
                    util.warn(
                        ("Got 'None' querying 'table_name' from "
                         "all_cons_columns%(dblink)s - does the user have "
                         "proper rights to the table?") % {'dblink':dblink})
                    continue

                if resolve_synonyms:
                    ref_remote_name, ref_remote_owner, ref_dblink, ref_synonym = self._resolve_synonym(connection, desired_owner=self._denormalize_name(remote_owner), desired_table=self._denormalize_name(remote_table))
                    if ref_synonym:
                        remote_table = self._normalize_name(ref_synonym)
                        remote_owner = self._normalize_name(ref_remote_owner)
                if local_column not in fk[0]:
                    fk[0].append(local_column)
                if remote_column not in fk[1]:
                    fk[1].append(remote_column)
        for (name, value) in fks.items():
            if remote_table and value[1]:
                fkeys.append((name, value[0], remote_owner, remote_table, value[1]))
        if info_cache:
            info_cache.setForeignKeys(fkeys, tablename, schemaname)
        return fkeys

    def reflecttable(self, connection, table, include_columns):
        preparer = self.identifier_preparer
        info_cache = OracleInfoCache()

        resolve_synonyms = table.kwargs.get('oracle_resolve_synonyms', False)

        (actual_name, owner, dblink, synonym) = \
            self._prepare_reflection_args(connection, table.name, table.schema,
                                          resolve_synonyms)

        # columns
        columns = self.get_columns(connection, actual_name, owner, info_cache,
                                                                        dblink)
        for cdict in columns:
            colname = cdict['name']
            coltype = cdict['type']
            nullable = cdict['nullable']
            colargs = cdict['attrs']
            if include_columns and colname not in include_columns:
                continue
            table.append_column(schema.Column(colname, coltype,
                                              nullable=nullable, *colargs))
        if not table.columns:
            raise AssertionError("Couldn't find any column information for table %s" % actual_name)

        # primary keys
        for pkcol in self.get_primary_keys(connection, actual_name, owner,
                                                           info_cache, dblink):
            if pkcol in table.c:
                table.primary_key.add(table.c[pkcol])

        # foreign keys
        fks = {}
        fkeys = []
        fkeys = self.get_foreign_keys(connection, actual_name, owner,
                                      info_cache, resolve_synonyms, dblink)
        refspecs = []
        for (conname, constrained_columns, referred_schema, referred_table,
             referred_columns) in fkeys:
            for (i, ref_col) in enumerate(referred_columns):
                if not table.schema and self._denormalize_name(referred_schema) == self._denormalize_name(owner):
                    t = schema.Table(referred_table, table.metadata, autoload=True, autoload_with=connection, oracle_resolve_synonyms=resolve_synonyms, useexisting=True)

                    refspec =  ".".join([referred_table, ref_col])
                else:
                    refspec = '.'.join([x for x in [referred_schema,
                                    referred_table, ref_col] if x is not None])

                    t = schema.Table(referred_table, table.metadata, autoload=True, autoload_with=connection, schema=referred_schema, oracle_resolve_synonyms=resolve_synonyms, useexisting=True)
                refspecs.append(refspec)
            table.append_constraint(
                schema.ForeignKeyConstraint(constrained_columns, refspecs,
                                        name=conname, link_to_name=True))


class _OuterJoinColumn(sql.ClauseElement):
    __visit_name__ = 'outer_join_column'
    
    def __init__(self, column):
        self.column = column



