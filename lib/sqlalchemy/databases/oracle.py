# oracle.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Support for the Oracle database.

Oracle version 8 through current (11g at the time of this writing) are supported.

Driver
------

The Oracle dialect uses the cx_oracle driver, available at 
http://cx-oracle.sourceforge.net/ .   The dialect has several behaviors 
which are specifically tailored towards compatibility with this module.

Connecting
----------

Connecting with create_engine() uses the standard URL approach of 
``oracle://user:pass@host:port/dbname[?key=value&key=value...]``.  If dbname is present, the 
host, port, and dbname tokens are converted to a TNS name using the cx_oracle 
:func:`makedsn()` function.  Otherwise, the host token is taken directly as a TNS name.

Additional arguments which may be specified either as query string arguments on the
URL, or as keyword arguments to :func:`~sqlalchemy.create_engine()` are:

* *allow_twophase* - enable two-phase transactions.  Defaults to ``True``.

* *auto_convert_lobs* - defaults to True, see the section on LOB objects.

* *auto_setinputsizes* - the cx_oracle.setinputsizes() call is issued for all bind parameters.
  This is required for LOB datatypes but can be disabled to reduce overhead.  Defaults
  to ``True``.

* *mode* - This is given the string value of SYSDBA or SYSOPER, or alternatively an
  integer value.  This value is only available as a URL query string argument.

* *threaded* - enable multithreaded access to cx_oracle connections.  Defaults
  to ``True``.  Note that this is the opposite default of cx_oracle itself.

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

LOB Objects
-----------

cx_oracle presents some challenges when fetching LOB objects.  A LOB object in a result set
is presented by cx_oracle as a cx_oracle.LOB object which has a read() method.  By default, 
SQLAlchemy converts these LOB objects into Python strings.  This is for two reasons.  First,
the LOB object requires an active cursor association, meaning if you were to fetch many rows
at once such that cx_oracle had to go back to the database and fetch a new batch of rows,
the LOB objects in the already-fetched rows are now unreadable and will raise an error. 
SQLA "pre-reads" all LOBs so that their data is fetched before further rows are read.  
The size of a "batch of rows" is controlled by the cursor.arraysize value, which SQLAlchemy
defaults to 50 (cx_oracle normally defaults this to one).  

Secondly, the LOB object is not a standard DBAPI return value so SQLAlchemy seeks to 
"normalize" the results to look more like other DBAPIs.

The conversion of LOB objects by this dialect is unique in SQLAlchemy in that it takes place
for all statement executions, even plain string-based statements for which SQLA has no awareness
of result typing.  This is so that calls like fetchmany() and fetchall() can work in all cases
without raising cursor errors.  The conversion of LOB in all cases, as well as the "prefetch"
of LOB objects, can be disabled using auto_convert_lobs=False.  

LIMIT/OFFSET Support
--------------------

Oracle has no support for the LIMIT or OFFSET keywords.  Whereas previous versions of SQLAlchemy
used the "ROW NUMBER OVER..." construct to simulate LIMIT/OFFSET, SQLAlchemy 0.5 now uses 
a wrapped subquery approach in conjunction with ROWNUM.  The exact methodology is taken from
http://www.oracle.com/technology/oramag/oracle/06-sep/o56asktom.html .  Note that the 
"FIRST ROWS()" optimization keyword mentioned is not used by default, as the user community felt
this was stepping into the bounds of optimization that is better left on the DBA side, but this
prefix can be added by enabling the optimize_limits=True flag on create_engine().

Two Phase Transaction Support
-----------------------------

Two Phase transactions are implemented using XA transactions.  Success has been reported of them
working successfully but this should be regarded as an experimental feature.

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


class OracleNumeric(sqltypes.Numeric):
    def get_col_spec(self):
        if self.precision is None:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(scale)s)" % {'precision': self.precision, 'scale' : self.scale}

class OracleInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"

class OracleSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"

class OracleDate(sqltypes.Date):
    def get_col_spec(self):
        return "DATE"
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
    def get_col_spec(self):
        return "DATE"

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
    def get_col_spec(self):
        return "TIMESTAMP"

    def get_dbapi_type(self, dialect):
        return dialect.TIMESTAMP

    def result_processor(self, dialect):
        def process(value):
            if value is None or isinstance(value, datetime.datetime):
                return value
            else:
                # convert cx_oracle datetime object returned pre-python 2.4
                return datetime.datetime(value.year, value.month,
                    value.day,value.hour, value.minute, value.second)
        return process

class OracleString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}

class OracleNVarchar(sqltypes.Unicode, OracleString):
    def get_col_spec(self):
        return "NVARCHAR2(%(length)s)" % {'length' : self.length}

class OracleText(sqltypes.Text):
    def get_dbapi_type(self, dbapi):
        return dbapi.CLOB

    def get_col_spec(self):
        return "CLOB"

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


class OracleChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}

class OracleBinary(sqltypes.Binary):
    def get_dbapi_type(self, dbapi):
        return dbapi.BLOB

    def get_col_spec(self):
        return "BLOB"

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
    def get_col_spec(self):
        return "SMALLINT"

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
    sqltypes.Integer : OracleInteger,
    sqltypes.Smallinteger : OracleSmallInteger,
    sqltypes.Numeric : OracleNumeric,
    sqltypes.Float : OracleNumeric,
    sqltypes.DateTime : OracleDateTime,
    sqltypes.Date : OracleDate,
    sqltypes.String : OracleString,
    sqltypes.Binary : OracleBinary,
    sqltypes.Boolean : OracleBoolean,
    sqltypes.Text : OracleText,
    sqltypes.TIMESTAMP : OracleTimestamp,
    sqltypes.CHAR: OracleChar,
}

ischema_names = {
    'VARCHAR2' : OracleString,
    'NVARCHAR2' : OracleNVarchar,
    'CHAR' : OracleString,
    'DATE' : OracleDateTime,
    'DATETIME' : OracleDateTime,
    'NUMBER' : OracleNumeric,
    'BLOB' : OracleBinary,
    'BFILE' : OracleBinary,
    'CLOB' : OracleText,
    'TIMESTAMP' : OracleTimestamp,
    'RAW' : OracleRaw,
    'FLOAT' : OracleNumeric,
    'DOUBLE PRECISION' : OracleNumeric,
    'LONG' : OracleText,
}

class OracleExecutionContext(default.DefaultExecutionContext):
    def pre_exec(self):
        super(OracleExecutionContext, self).pre_exec()
        if self.dialect.auto_setinputsizes:
            self.set_input_sizes()
        if self.compiled_parameters is not None and len(self.compiled_parameters) == 1:
            for key in self.compiled.binds:
                bindparam = self.compiled.binds[key]
                name = self.compiled.bind_names[bindparam]
                value = self.compiled_parameters[0][name]
                if bindparam.isoutparam:
                    dbtype = bindparam.type.dialect_impl(self.dialect).get_dbapi_type(self.dialect.dbapi)
                    if not hasattr(self, 'out_parameters'):
                        self.out_parameters = {}
                    self.out_parameters[name] = self.cursor.var(dbtype)
                    self.parameters[0][name] = self.out_parameters[name]

    def create_cursor(self):
        c = self._connection.connection.cursor()
        if self.dialect.arraysize:
            c.cursor.arraysize = self.dialect.arraysize
        return c

    def get_result_proxy(self):
        if hasattr(self, 'out_parameters'):
            if self.compiled_parameters is not None and len(self.compiled_parameters) == 1:
                for bind, name in self.compiled.bind_names.iteritems():
                    if name in self.out_parameters:
                        type = bind.type
                        result_processor = type.dialect_impl(self.dialect).result_processor(self.dialect)
                        if result_processor is not None:
                            self.out_parameters[name] = result_processor(self.out_parameters[name].getvalue())
                        else:
                            self.out_parameters[name] = self.out_parameters[name].getvalue()
            else:
                for k in self.out_parameters:
                    self.out_parameters[k] = self.out_parameters[k].getvalue()

        if self.cursor.description is not None:
            for column in self.cursor.description:
                type_code = column[1]
                if type_code in self.dialect.ORACLE_BINARY_TYPES:
                    return base.BufferedColumnResultProxy(self)

        return base.ResultProxy(self)

class OracleDialect(default.DefaultDialect):
    name = 'oracle'
    supports_alter = True
    supports_unicode_statements = False
    max_identifier_length = 30
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    preexecute_pk_sequences = True
    supports_pk_autoincrement = False
    default_paramstyle = 'named'

    def __init__(self, use_ansi=True, auto_setinputsizes=True, auto_convert_lobs=True, threaded=True, allow_twophase=True, optimize_limits=False, arraysize=50, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)
        self.use_ansi = use_ansi
        self.threaded = threaded
        self.arraysize = arraysize
        self.allow_twophase = allow_twophase
        self.optimize_limits = optimize_limits
        self.supports_timestamp = self.dbapi is None or hasattr(self.dbapi, 'TIMESTAMP' )
        self.auto_setinputsizes = auto_setinputsizes
        self.auto_convert_lobs = auto_convert_lobs
        if self.dbapi is None or not self.auto_convert_lobs or not 'CLOB' in self.dbapi.__dict__:
            self.dbapi_type_map = {}
            self.ORACLE_BINARY_TYPES = []
        else:
            # only use this for LOB objects.  using it for strings, dates
            # etc. leads to a little too much magic, reflection doesn't know if it should
            # expect encoded strings or unicodes, etc.
            self.dbapi_type_map = {
                self.dbapi.CLOB: OracleText(),
                self.dbapi.BLOB: OracleBinary(),
                self.dbapi.BINARY: OracleRaw(),
            }
            self.ORACLE_BINARY_TYPES = [getattr(self.dbapi, k) for k in ["BFILE", "CLOB", "NCLOB", "BLOB"] if hasattr(self.dbapi, k)]

    def dbapi(cls):
        import cx_Oracle
        return cx_Oracle
    dbapi = classmethod(dbapi)

    def create_connect_args(self, url):
        dialect_opts = dict(url.query)
        for opt in ('use_ansi', 'auto_setinputsizes', 'auto_convert_lobs',
                    'threaded', 'allow_twophase'):
            if opt in dialect_opts:
                util.coerce_kw_type(dialect_opts, opt, bool)
                setattr(self, opt, dialect_opts[opt])

        if url.database:
            # if we have a database, then we have a remote host
            port = url.port
            if port:
                port = int(port)
            else:
                port = 1521
            dsn = self.dbapi.makedsn(url.host, port, url.database)
        else:
            # we have a local tnsname
            dsn = url.host

        opts = dict(
            user=url.username,
            password=url.password,
            dsn=dsn,
            threaded=self.threaded,
            twophase=self.allow_twophase,
            )
        if 'mode' in url.query:
            opts['mode'] = url.query['mode']
            if isinstance(opts['mode'], basestring):
                mode = opts['mode'].upper()
                if mode == 'SYSDBA':
                    opts['mode'] = self.dbapi.SYSDBA
                elif mode == 'SYSOPER':
                    opts['mode'] = self.dbapi.SYSOPER
                else:
                    util.coerce_kw_type(opts, 'mode', int)
        # Can't set 'handle' or 'pool' via URL query args, use connect_args

        return ([], opts)

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.InterfaceError):
            return "not connected" in str(e)
        else:
            return "ORA-03114" in str(e) or "ORA-03113" in str(e)

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def create_xid(self):
        """create a two-phase transaction ID.

        this id will be passed to do_begin_twophase(), do_rollback_twophase(),
        do_commit_twophase().  its format is unspecified."""

        id = random.randint(0, 2 ** 128)
        return (0x1234, "%032x" % id, "%032x" % 9)
        
    def do_release_savepoint(self, connection, name):
        # Oracle does not support RELEASE SAVEPOINT
        pass

    def do_begin_twophase(self, connection, xid):
        connection.connection.begin(*xid)

    def do_prepare_twophase(self, connection, xid):
        connection.connection.prepare()

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        self.do_rollback(connection.connection)

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        self.do_commit(connection.connection)

    def do_recover_twophase(self, connection):
        pass

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

    def get_default_schema_name(self, connection):
        return self._normalize_name(connection.execute('SELECT USER FROM DUAL').scalar())
    get_default_schema_name = base.connection_memoize(
        ('dialect', 'default_schema_name'))(get_default_schema_name)

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

    def reflecttable(self, connection, table, include_columns):
        preparer = self.identifier_preparer

        resolve_synonyms = table.kwargs.get('oracle_resolve_synonyms', False)

        if resolve_synonyms:
            actual_name, owner, dblink, synonym = self._resolve_synonym(connection, desired_owner=self._denormalize_name(table.schema), desired_synonym=self._denormalize_name(table.name))
        else:
            actual_name, owner, dblink, synonym = None, None, None, None

        if not actual_name:
            actual_name = self._denormalize_name(table.name)
        if not dblink:
            dblink = ''
        if not owner:
            owner = self._denormalize_name(table.schema or self.get_default_schema_name(connection))

        c = connection.execute ("select COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT from ALL_TAB_COLUMNS%(dblink)s where TABLE_NAME = :table_name and OWNER = :owner" % {'dblink':dblink}, {'table_name':actual_name, 'owner':owner})

        while True:
            row = c.fetchone()
            if row is None:
                break

            (colname, coltype, length, precision, scale, nullable, default) = (self._normalize_name(row[0]), row[1], row[2], row[3], row[4], row[5]=='Y', row[6])

            if include_columns and colname not in include_columns:
                continue

            # INTEGER if the scale is 0 and precision is null
            # NUMBER if the scale and precision are both null
            # NUMBER(9,2) if the precision is 9 and the scale is 2
            # NUMBER(3) if the precision is 3 and scale is 0
            #length is ignored except for CHAR and VARCHAR2
            if coltype == 'NUMBER' :
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
                    util.warn("Did not recognize type '%s' of column '%s'" %
                              (coltype, colname))
                    coltype = sqltypes.NULLTYPE

            colargs = []
            if default is not None:
                colargs.append(schema.DefaultClause(sql.text(default)))

            table.append_column(schema.Column(colname, coltype, nullable=nullable, *colargs))

        if not table.columns:
            raise AssertionError("Couldn't find any column information for table %s" % actual_name)

        c = connection.execute("""SELECT
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
         % {'dblink':dblink}, {'table_name' : actual_name, 'owner' : owner})

        fks = {}
        while True:
            row = c.fetchone()
            if row is None:
                break
            #print "ROW:" , row
            (cons_name, cons_type, local_column, remote_table, remote_column, remote_owner) = row[0:2] + tuple([self._normalize_name(x) for x in row[2:]])
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

                if not table.schema and self._denormalize_name(remote_owner) == owner:
                    refspec =  ".".join([remote_table, remote_column])
                    t = schema.Table(remote_table, table.metadata, autoload=True, autoload_with=connection, oracle_resolve_synonyms=resolve_synonyms, useexisting=True)
                else:
                    refspec =  ".".join([x for x in [remote_owner, remote_table, remote_column] if x])
                    t = schema.Table(remote_table, table.metadata, autoload=True, autoload_with=connection, schema=remote_owner, oracle_resolve_synonyms=resolve_synonyms, useexisting=True)

                if local_column not in fk[0]:
                    fk[0].append(local_column)
                if refspec not in fk[1]:
                    fk[1].append(refspec)

        for name, value in fks.iteritems():
            table.append_constraint(schema.ForeignKeyConstraint(value[0], value[1], name=name, link_to_name=True))


class _OuterJoinColumn(sql.ClauseElement):
    __visit_name__ = 'outer_join_column'
    
    def __init__(self, column):
        self.column = column

class OracleCompiler(compiler.DefaultCompiler):
    """Oracle compiler modifies the lexical structure of Select
    statements to work under non-ANSI configured Oracle databases, if
    the use_ansi flag is False.
    """

    operators = compiler.DefaultCompiler.operators.copy()
    operators.update(
        {
            sql_operators.mod : lambda x, y:"mod(%s, %s)" % (x, y),
            sql_operators.match_op: lambda x, y: "CONTAINS (%s, %s)" % (x, y)
        }
    )

    functions = compiler.DefaultCompiler.functions.copy()
    functions.update (
        {
            sql_functions.now : 'CURRENT_TIMESTAMP'
        }
    )

    def __init__(self, *args, **kwargs):
        super(OracleCompiler, self).__init__(*args, **kwargs)
        self.__wheres = {}

    def default_from(self):
        """Called when a ``SELECT`` statement has no froms, and no ``FROM`` clause is to be appended.

        The Oracle compiler tacks a "FROM DUAL" to the statement.
        """

        return " FROM DUAL"

    def apply_function_parens(self, func):
        return len(func.clauses) > 0

    def visit_join(self, join, **kwargs):
        if self.dialect.use_ansi:
            return compiler.DefaultCompiler.visit_join(self, join, **kwargs)
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
            alias_name = isinstance(alias.name, expression._generated_label) and \
                            self._truncated_identifier("alias", alias.name) or alias.name
            
            return self.process(alias.original, asfrom=True, **kwargs) + " " +\
                    self.preparer.format_alias(alias, alias_name)
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
        return compiler.DefaultCompiler.visit_select(self, select, **kwargs)

    def limit_clause(self, select):
        return ""

    def for_update_clause(self, select):
        if select.for_update == "nowait":
            return " FOR UPDATE NOWAIT"
        else:
            return super(OracleCompiler, self).for_update_clause(select)


class OracleSchemaGenerator(compiler.SchemaGenerator):
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
        if not self.checkfirst  or not self.dialect.has_sequence(self.connection, sequence.name, sequence.schema):
            self.append("CREATE SEQUENCE %s" % self.preparer.format_sequence(sequence))
            self.execute()

class OracleSchemaDropper(compiler.SchemaDropper):
    def visit_sequence(self, sequence):
        if not self.checkfirst or self.dialect.has_sequence(self.connection, sequence.name, sequence.schema):
            self.append("DROP SEQUENCE %s" % self.preparer.format_sequence(sequence))
            self.execute()

class OracleDefaultRunner(base.DefaultRunner):
    def visit_sequence(self, seq):
        return self.execute_string("SELECT " + self.dialect.identifier_preparer.format_sequence(seq) + ".nextval FROM DUAL", {})

class OracleIdentifierPreparer(compiler.IdentifierPreparer):
    def format_savepoint(self, savepoint):
        name = re.sub(r'^_+', '', savepoint.ident)
        return super(OracleIdentifierPreparer, self).format_savepoint(savepoint, name)


dialect = OracleDialect
dialect.statement_compiler = OracleCompiler
dialect.schemagenerator = OracleSchemaGenerator
dialect.schemadropper = OracleSchemaDropper
dialect.preparer = OracleIdentifierPreparer
dialect.defaultrunner = OracleDefaultRunner
dialect.execution_ctx_cls = OracleExecutionContext
