# mssql.py

"""MSSQL backend, thru either pymssq, adodbapi or pyodbc interfaces.

* ``IDENTITY`` columns are supported by using SA ``schema.Sequence()``
  objects. In other words::

    Table('test', mss_engine,
           Column('id',   Integer, Sequence('blah',100,10), primary_key=True),
           Column('name', String(20))
         ).create()

  would yield::

   CREATE TABLE test (
     id INTEGER NOT NULL IDENTITY(100,10) PRIMARY KEY,
     name VARCHAR(20)
     )

  Note that the start & increment values for sequences are optional
  and will default to 1,1.

* Support for ``SET IDENTITY_INSERT ON`` mode (automagic on / off for 
  ``INSERT`` s)

* Support for auto-fetching of ``@@IDENTITY`` on ``INSERT``

* ``select.limit`` implemented as ``SELECT TOP n``


Known issues / TODO:

* No support for more than one ``IDENTITY`` column per table

* No support for table reflection of ``IDENTITY`` columns with
  (seed,increment) values other than (1,1)

* No support for ``GUID`` type columns (yet)

* pymssql has problems with binary and unicode data that this module
  does **not** work around
  
"""

import sys, StringIO, string, types, re, datetime

import sqlalchemy.sql as sql
import sqlalchemy.engine as engine
import sqlalchemy.engine.default as default
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
import sqlalchemy.types as sqltypes
import sqlalchemy.exceptions as exceptions


class MSNumeric(sqltypes.Numeric):
    def convert_result_value(self, value, dialect):
        return value

    def convert_bind_param(self, value, dialect):
        if value is None:
            # Not sure that this exception is needed
            return value
        else:
            return str(value) 

    def get_col_spec(self):
        if self.precision is None:
            return "NUMERIC"
        else:
            return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}

class MSFloat(sqltypes.Float):
    def get_col_spec(self):
        return "FLOAT(%(precision)s)" % {'precision': self.precision}

    def convert_bind_param(self, value, dialect):
        """By converting to string, we can use Decimal types round-trip."""
        return value and str(value) or None

class MSInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"

class MSTinyInteger(sqltypes.Integer): 
    def get_col_spec(self):
        return "TINYINT"

class MSSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"

class MSDateTime(sqltypes.DateTime):
    def __init__(self, *a, **kw):
        super(MSDateTime, self).__init__(False)

    def get_col_spec(self):
        return "DATETIME"

    def convert_bind_param(self, value, dialect):
        if hasattr(value, "isoformat"):
            #return value.isoformat(' ')
            # isoformat() bings on apodbapi -- reported/suggested by Peter Buschman
            return value.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return value

    def convert_result_value(self, value, dialect):
        # adodbapi will return datetimes with empty time values as datetime.date() objects.
        # Promote them back to full datetime.datetime()
        if value and not hasattr(value, 'second'):
            return datetime.datetime(value.year, value.month, value.day)
        return value

class MSDate(sqltypes.Date):
    def __init__(self, *a, **kw):
        super(MSDate, self).__init__(False)

    def get_col_spec(self):
        return "SMALLDATETIME"
    
    def convert_bind_param(self, value, dialect):
        if value and hasattr(value, "isoformat"):
            return value.strftime('%Y-%m-%d %H:%M')
        return value

    def convert_result_value(self, value, dialect):
        # pymssql will return SMALLDATETIME values as datetime.datetime(), truncate it back to datetime.date()
        if value and hasattr(value, 'second'):
            return value.date()
        return value

class MSText(sqltypes.TEXT):
    def get_col_spec(self):
        if self.dialect.text_as_varchar:
            return "VARCHAR(max)"            
        else:
            return "TEXT"

class MSString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}

class MSNVarchar(MSString):
    """NVARCHAR string, does Unicode conversion if `dialect.convert_encoding` is True.  """
    impl = sqltypes.Unicode

    def get_col_spec(self):
        if self.length:
            return "NVARCHAR(%(length)s)" % {'length' : self.length}
        elif self.dialect.text_as_varchar:
            return "NVARCHAR(max)"
        else:
            return "NTEXT"

class AdoMSNVarchar(MSNVarchar):
    def convert_bind_param(self, value, dialect):
        return value

    def convert_result_value(self, value, dialect):
        return value        

class MSUnicode(sqltypes.Unicode):
    """Unicode subclass, does Unicode conversion in all cases, uses NVARCHAR impl."""
    impl = MSNVarchar

class AdoMSUnicode(MSUnicode):
    impl = AdoMSNVarchar

    def convert_bind_param(self, value, dialect):
        return value

    def convert_result_value(self, value, dialect):
        return value        

class MSChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}

class MSNChar(sqltypes.NCHAR):
    def get_col_spec(self):
        return "NCHAR(%(length)s)" % {'length' : self.length}

class MSBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "IMAGE"

class MSBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BIT"

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
        
def descriptor():
    return {'name':'mssql',
    'description':'MSSQL',
    'arguments':[
        ('user',"Database Username",None),
        ('password',"Database Password",None),
        ('db',"Database Name",None),
        ('host',"Hostname", None),
    ]}

class MSSQLExecutionContext(default.DefaultExecutionContext):
    def __init__(self, dialect):
        self.IINSERT = self.HASIDENT = False
        super(MSSQLExecutionContext, self).__init__(dialect)

    def _has_implicit_sequence(self, column):
        if column.primary_key and column.autoincrement:
            if isinstance(column.type, sqltypes.Integer) and not column.foreign_key:
                if column.default is None or (isinstance(column.default, schema.Sequence) and \
                                              column.default.optional):
                    return True
        return False

    def pre_exec(self, engine, proxy, compiled, parameters, **kwargs):
        """MS-SQL has a special mode for inserting non-NULL values
        into IDENTITY columns.
        
        Activate it if the feature is turned on and needed.
        """
        if getattr(compiled, "isinsert", False):
            tbl = compiled.statement.table
            if not hasattr(tbl, 'has_sequence'):
                tbl.has_sequence = False
                for column in tbl.c:
                    if getattr(column, 'sequence', False) or self._has_implicit_sequence(column):
                        tbl.has_sequence = column
                        break

            self.HASIDENT = bool(tbl.has_sequence)
            if engine.dialect.auto_identity_insert and self.HASIDENT:
                if isinstance(parameters, list):
                    self.IINSERT = parameters[0].has_key(tbl.has_sequence.key)
                else:
                    self.IINSERT = parameters.has_key(tbl.has_sequence.key)
            else:
                self.IINSERT = False

            if self.IINSERT:
                proxy("SET IDENTITY_INSERT %s ON" % compiled.statement.table.name)

        super(MSSQLExecutionContext, self).pre_exec(engine, proxy, compiled, parameters, **kwargs)

    def post_exec(self, engine, proxy, compiled, parameters, **kwargs):
        """Turn off the INDENTITY_INSERT mode if it's been activated,
        and fetch recently inserted IDENTIFY values (works only for
        one column).
        """
        
        if getattr(compiled, "isinsert", False):
            if self.IINSERT:
                proxy("SET IDENTITY_INSERT %s OFF" % compiled.statement.table.name)
                self.IINSERT = False
            elif self.HASIDENT:
                cursor = proxy("SELECT @@IDENTITY AS lastrowid")
                row = cursor.fetchone()
                self._last_inserted_ids = [int(row[0])]
                # print "LAST ROW ID", self._last_inserted_ids
            self.HASIDENT = False


class MSSQLDialect(ansisql.ANSIDialect):
    colspecs = {
        sqltypes.Integer : MSInteger,
        sqltypes.Smallinteger: MSSmallInteger,
        sqltypes.Numeric : MSNumeric,
        sqltypes.Float : MSFloat,
        sqltypes.DateTime : MSDateTime,
        sqltypes.Date : MSDate,
        sqltypes.String : MSString,
        sqltypes.Unicode : MSUnicode,
        sqltypes.Binary : MSBinary,
        sqltypes.Boolean : MSBoolean,
        sqltypes.TEXT : MSText,
        sqltypes.CHAR: MSChar,
        sqltypes.NCHAR: MSNChar,
    }

    ischema_names = {
        'int' : MSInteger,
        'smallint' : MSSmallInteger,
        'tinyint' : MSTinyInteger,
        'varchar' : MSString,
        'nvarchar' : MSUnicode,
        'char' : MSChar,
        'nchar' : MSNChar,
        'text' : MSText,
        'ntext' : MSText,
        'decimal' : MSNumeric,
        'numeric' : MSNumeric,
        'float' : MSFloat,
        'datetime' : MSDateTime,
        'smalldatetime' : MSDate,
        'binary' : MSBinary,
        'bit': MSBoolean,
        'real' : MSFloat,
        'image' : MSBinary
    }

    def __new__(cls, module_name=None, *args, **kwargs):
        module = kwargs.get('module', None)
        if cls != MSSQLDialect:
            return super(MSSQLDialect, cls).__new__(cls, *args, **kwargs)
        if module_name:
            dialect = dialect_mapping.get(module_name)
            if not dialect:
                raise exceptions.InvalidRequestError('Unsupported MSSQL module requested (must be adodbpi, pymssql or pyodbc): ' + module_name)
            if not hasattr(dialect, 'module'):
                raise dialect.saved_import_error
            return dialect(*args, **kwargs)
        elif module:
            return object.__new__(cls, *args, **kwargs)
        else:
            for dialect in dialect_preference:
                if hasattr(dialect, 'module'):
                    return dialect(*args, **kwargs)
            #raise ImportError('No DBAPI module detected for MSSQL - please install adodbapi, pymssql or pyodbc')
            else:
                return object.__new__(cls, *args, **kwargs)
                
    def __init__(self, module_name=None, module=None, auto_identity_insert=True, **params):
        if not hasattr(self, 'module'):
            self.module = module
        super(MSSQLDialect, self).__init__(**params)
        self.auto_identity_insert = auto_identity_insert
        self.text_as_varchar = False
        self.set_default_schema_name("dbo")
            
    def create_connect_args(self, url):
        opts = url.translate_connect_args(['host', 'database', 'user', 'password', 'port'])
        opts.update(url.query)
        if opts.has_key('auto_identity_insert'):
            self.auto_identity_insert = bool(opts.pop('auto_identity_insert'))
        if opts.has_key('query_timeout'):
            self.query_timeout = int(opts.pop('query_timeout'))
        if opts.has_key('text_as_varchar'):
            self.text_as_varchar = bool(opts.pop('text_as_varchar'))
        return self.make_connect_string(opts)

    def create_execution_context(self):
        return MSSQLExecutionContext(self)

    def type_descriptor(self, typeobj):
        newobj = sqltypes.adapt_type(typeobj, self.colspecs)
        # Some types need to know about the dialect
        if isinstance(newobj, (MSText, MSNVarchar)):
            newobj.dialect = self
        return newobj

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    # this is only implemented in the dbapi-specific subclasses
    def supports_sane_rowcount(self):
        raise NotImplementedError()

    def compiler(self, statement, bindparams, **kwargs):
        return MSSQLCompiler(self, statement, bindparams, **kwargs)

    def schemagenerator(self, *args, **kwargs):
        return MSSQLSchemaGenerator(*args, **kwargs)

    def schemadropper(self, *args, **kwargs):
        return MSSQLSchemaDropper(*args, **kwargs)

    def defaultrunner(self, engine, proxy):
        return MSSQLDefaultRunner(engine, proxy)

    def preparer(self):
        return MSSQLIdentifierPreparer(self)

    def get_default_schema_name(self):
        return self.schema_name

    def set_default_schema_name(self, schema_name):
        self.schema_name = schema_name

    def last_inserted_ids(self):
        return self.context.last_inserted_ids
            
    def do_execute(self, cursor, statement, params, **kwargs):
        if params == {}:
            params = ()
        super(MSSQLDialect, self).do_execute(cursor, statement, params, **kwargs)

    def _execute(self, c, statement, parameters):
        try:
            if parameters == {}:
                parameters = ()
            c.execute(statement, parameters)
            self.context.rowcount = c.rowcount
            c.DBPROP_COMMITPRESERVE = "Y"
        except Exception, e:
            raise exceptions.SQLError(statement, parameters, e)

    def raw_connection(self, connection):
        """Pull the raw pymmsql connection out--sensative to "pool.ConnectionFairy" and pymssql.pymssqlCnx Classes"""
        try:
            return connection.connection.__dict__['_pymssqlCnx__cnx']
        except:
            return connection.connection.adoConn

    def connection(self):
        """returns a managed DBAPI connection from this SQLEngine's connection pool."""
        c = self._pool.connect()
        c.supportsTransactions = 0
        return c
  
    def dbapi(self):
        return self.module

    def uppercase_table(self, t):
        # convert all names to uppercase -- fixes refs to INFORMATION_SCHEMA for case-senstive DBs, and won't matter for case-insensitive
        t.name = t.name.upper()
        if t.schema:
            t.schema = t.schema.upper()
        for c in t.columns:
            c.name = c.name.upper()
        return t

    def has_table(self, connection, tablename, schema=None):
        import sqlalchemy.databases.information_schema as ischema

        current_schema = schema or self.get_default_schema_name()
        columns = self.uppercase_table(ischema.columns)
        s = sql.select([columns],
                   current_schema
                       and sql.and_(columns.c.table_name==tablename, columns.c.table_schema==current_schema)
                       or columns.c.table_name==tablename,
                   )
        
        c = connection.execute(s)
        row  = c.fetchone()
        return row is not None
        
    def reflecttable(self, connection, table):
        import sqlalchemy.databases.information_schema as ischema
        
        # Get base columns
        if table.schema is not None:
            current_schema = table.schema
        else:
            current_schema = self.get_default_schema_name()

        columns = self.uppercase_table(ischema.columns)
        s = sql.select([columns],
                   current_schema
                       and sql.and_(columns.c.table_name==table.name, columns.c.table_schema==current_schema)
                       or columns.c.table_name==table.name,
                   order_by=[columns.c.ordinal_position])
        
        c = connection.execute(s)
        found_table = False
        while True:
            row = c.fetchone()
            if row is None:
                break
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

            args = []
            for a in (charlen, numericprec, numericscale):
                if a is not None:
                    args.append(a)
            coltype = self.ischema_names[type]
            if coltype == MSString and charlen == -1:
                coltype = MSText()                
            else:
                if coltype == MSNVarchar and charlen == -1:
                    charlen = None
                coltype = coltype(*args)
            colargs= []
            if default is not None:
                colargs.append(schema.PassiveDefault(sql.text(default)))
                
            table.append_column(schema.Column(name, coltype, nullable=nullable, *colargs))
        
        if not found_table:
            raise exceptions.NoSuchTableError(table.name)

        # We also run an sp_columns to check for identity columns:
        # FIXME: note that this only fetches the existence of an identity column, not it's properties like (seed, increment)
        #        also, add a check to make sure we specify the schema name of the table
        # cursor = table.engine.execute("sp_columns " + table.name, {})
        cursor = connection.execute("sp_columns " + table.name)
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            col_name, type_name = row[3], row[5]
            if type_name.endswith("identity"):
                ic = table.c[col_name]
                # setup a psuedo-sequence to represent the identity attribute - we interpret this at table.create() time as the identity attribute
                ic.sequence = schema.Sequence(ic.name + '_identity')

        # Add constraints
        RR = self.uppercase_table(ischema.ref_constraints)    #information_schema.referential_constraints
        TC = self.uppercase_table(ischema.constraints)        #information_schema.table_constraints
        C  = self.uppercase_table(ischema.pg_key_constraints).alias('C') #information_schema.constraint_column_usage: the constrained column 
        R  = self.uppercase_table(ischema.pg_key_constraints).alias('R') #information_schema.constraint_column_usage: the referenced column

        # Primary key constraints
        s = sql.select([C.c.column_name, TC.c.constraint_type], sql.and_(TC.c.constraint_name == C.c.constraint_name,
                                                                         C.c.table_name == table.name))
        c = connection.execute(s)
        for row in c:
            if 'PRIMARY' in row[TC.c.constraint_type.name]:
                table.primary_key.add(table.c[row[0]])

        # Foreign key constraints
        s = sql.select([C.c.column_name,
                        R.c.table_schema, R.c.table_name, R.c.column_name,
                        RR.c.constraint_name, RR.c.match_option, RR.c.update_rule, RR.c.delete_rule],
                       sql.and_(C.c.table_name == table.name,
                                C.c.table_schema == current_schema,
                                C.c.constraint_name == RR.c.constraint_name,
                                R.c.constraint_name == RR.c.unique_constraint_name,
                                C.c.ordinal_position == R.c.ordinal_position
                                ),
                       order_by = [RR.c.constraint_name, R.c.ordinal_position])
        rows = connection.execute(s).fetchall()

        # group rows by constraint ID, to handle multi-column FKs
        fknm, scols, rcols = (None, [], [])
        for r in rows:
            scol, rschema, rtbl, rcol, rfknm, fkmatch, fkuprule, fkdelrule = r
            if rfknm != fknm:
                if fknm:
                    table.append_constraint(schema.ForeignKeyConstraint(scols, ['%s.%s' % (t,c) for (s,t,c) in rcols], fknm))
                fknm, scols, rcols = (rfknm, [], [])
            if (not scol in scols): scols.append(scol)
            if (not (rschema, rtbl, rcol) in rcols): rcols.append((rschema, rtbl, rcol))

        if fknm and scols:
            table.append_constraint(schema.ForeignKeyConstraint(scols, ['%s.%s' % (t,c) for (s,t,c) in rcols], fknm))

class MSSQLDialect_pymssql(MSSQLDialect):
    try:
        import pymssql as module
        # pymmsql doesn't have a Binary method.  we use string
        module.Binary = lambda st: str(st)
    except ImportError, e:
        saved_import_error = e

    def supports_sane_rowcount(self):
        return True

    def do_rollback(self, connection):
        # pymssql throws an error on repeated rollbacks. Ignore it.
        try:
            connection.rollback()
        except:
            pass

    def create_connect_args(self, url):
        r = super(MSSQLDialect_pymssql, self).create_connect_args(url)
        if hasattr(self, 'query_timeout'):
            self.module._mssql.set_query_timeout(self.query_timeout)
        return r

    def make_connect_string(self, keys):
        if keys.get('port'):
            # pymssql expects port as host:port, not a separate arg
            keys['host'] = ''.join([keys.get('host', ''), ':', str(keys['port'])])
            del keys['port']
        return [[], keys]


##    This code is leftover from the initial implementation, for reference
##    def do_begin(self, connection):
##        """implementations might want to put logic here for turning autocommit on/off, etc."""
##        pass  

##    def do_rollback(self, connection):
##        """implementations might want to put logic here for turning autocommit on/off, etc."""
##        try:
##            # connection.rollback() for pymmsql failed sometimes--the begin tran doesn't show up
##            # this is a workaround that seems to be handle it.
##            r = self.raw_connection(connection)
##            r.query("if @@trancount > 0 rollback tran")
##            r.fetch_array()
##            r.query("begin tran")
##            r.fetch_array()
##        except:
##            pass

##    def do_commit(self, connection):
##        """implementations might want to put logic here for turning autocommit on/off, etc.
##            do_commit is set for pymmsql connections--ADO seems to handle transactions without any issue 
##        """
##        # ADO Uses Implicit Transactions.
##        # This is very pymssql specific.  We use this instead of its commit, because it hangs on failed rollbacks.
##        # By using the "if" we don't assume an open transaction--much better.
##        r = self.raw_connection(connection)
##        r.query("if @@trancount > 0 commit tran")
##        r.fetch_array()
##        r.query("begin tran")
##        r.fetch_array()

class MSSQLDialect_pyodbc(MSSQLDialect):
    try:
        import pyodbc as module
    except ImportError, e:
        saved_import_error = e

    colspecs = MSSQLDialect.colspecs.copy()
    colspecs[sqltypes.Unicode] = AdoMSUnicode
    ischema_names = MSSQLDialect.ischema_names.copy()
    ischema_names['nvarchar'] = AdoMSUnicode

    def supports_sane_rowcount(self):
        return False

    def make_connect_string(self, keys):
        connectors = ["Driver={SQL Server}"]
        connectors.append("Server=%s" % keys.get("host"))
        connectors.append("Database=%s" % keys.get("database"))
        user = keys.get("user")
        if user:
            connectors.append("UID=%s" % user)
            connectors.append("PWD=%s" % keys.get("password", ""))
        else:
            connectors.append ("TrustedConnection=Yes")
        return [[";".join (connectors)], {}]


class MSSQLDialect_adodbapi(MSSQLDialect):
    try:
        import adodbapi as module
    except ImportError, e:
        saved_import_error = e

    colspecs = MSSQLDialect.colspecs.copy()
    colspecs[sqltypes.Unicode] = AdoMSUnicode
    ischema_names = MSSQLDialect.ischema_names.copy()
    ischema_names['nvarchar'] = AdoMSUnicode

    def supports_sane_rowcount(self):
        return True

    def make_connect_string(self, keys):
        connectors = ["Provider=SQLOLEDB"]
        connectors.append ("Data Source=%s" % keys.get("host"))
        connectors.append ("Initial Catalog=%s" % keys.get("database"))
        user = keys.get("user")
        if user:
            connectors.append("User Id=%s" % user)
            connectors.append("Password=%s" % keys.get("password", ""))
        else:
            connectors.append("Integrated Security=SSPI")
        return [[";".join (connectors)], {}]


dialect_mapping = {
    'pymssql':  MSSQLDialect_pymssql,
    'pyodbc':   MSSQLDialect_pyodbc,
    'adodbapi': MSSQLDialect_adodbapi
    }
dialect_preference = [MSSQLDialect_adodbapi, MSSQLDialect_pymssql, MSSQLDialect_pyodbc]


class MSSQLCompiler(ansisql.ANSICompiler):
    def __init__(self, dialect, statement, parameters, **kwargs):
        super(MSSQLCompiler, self).__init__(dialect, statement, parameters, **kwargs)
        self.tablealiases = {}

    def visit_select_precolumns(self, select):
        """ MS-SQL puts TOP, it's version of LIMIT here """
        s = select.distinct and "DISTINCT " or ""
        if select.limit:
            s += "TOP %s " % (select.limit,)
        if select.offset:
            raise exceptions.InvalidRequestError('MSSQL does not support LIMIT with an offset')
        return s

    def limit_clause(self, select):    
        # Limit in mssql is after the select keyword
        return ""
            
    def visit_table(self, table):
        # alias schema-qualified tables
        if getattr(table, 'schema', None) is not None and not self.tablealiases.has_key(table):
            alias = table.alias()
            self.tablealiases[table] = alias
            self.traverse(alias)
            self.froms[('alias', table)] = self.froms[table]
            for c in alias.c:
                self.traverse(c)
            self.traverse(alias.oid_column)
            self.tablealiases[alias] = self.froms[table]
            self.froms[table] = self.froms[alias]
        else:
           super(MSSQLCompiler, self).visit_table(table)
 
    def visit_alias(self, alias):
        # translate for schema-qualified table aliases
        if self.froms.has_key(('alias', alias.original)):
            self.froms[alias] = self.froms[('alias', alias.original)] + " AS " + alias.name
            self.strings[alias] = ""
        else:
            super(MSSQLCompiler, self).visit_alias(alias)

    def visit_column(self, column):
        # translate for schema-qualified table aliases
        super(MSSQLCompiler, self).visit_column(column)
        if column.table is not None and self.tablealiases.has_key(column.table):
            self.strings[column] = \
                self.strings[self.tablealiases[column.table].corresponding_column(column)]

    def visit_binary(self, binary):
        """Move bind parameters to the right-hand side of an operator, where possible."""
        if isinstance(binary.left, sql._BindParamClause) and binary.operator == '=':
            binary.left, binary.right = binary.right, binary.left
        super(MSSQLCompiler, self).visit_binary(binary)

    function_rewrites =  {'current_date': 'getdate',
                          'length':     'len',
                          }
    def visit_function(self, func):
        func.name = self.function_rewrites.get(func.name, func.name)
        super(MSSQLCompiler, self).visit_function(func)            

    def for_update_clause(self, select):
        # "FOR UPDATE" is only allowed on "DECLARE CURSOR" which SQLAlchemy doesn't use
        return ''

    def order_by_clause(self, select):
        order_by = self.get_str(select.order_by_clause)

        # MSSQL only allows ORDER BY in subqueries if there is a LIMIT
        if order_by and (not select.is_subquery or select.limit):
            return " ORDER BY " + order_by
        else:
            return ""


class MSSQLSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column) + " " + column.type.engine_impl(self.engine).get_col_spec()
        
        # install a IDENTITY Sequence if we have an implicit IDENTITY column
        if (not getattr(column.table, 'has_sequence', False)) and column.primary_key and \
                column.autoincrement and isinstance(column.type, sqltypes.Integer) and not column.foreign_key:
            if column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional):
                column.sequence = schema.Sequence(column.name + '_seq')

        if not column.nullable:
            colspec += " NOT NULL"

        if hasattr(column, 'sequence'):
            column.table.has_sequence = column
            colspec += " IDENTITY(%s,%s)" % (column.sequence.start or 1, column.sequence.increment or 1)
        else:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default
        
        return colspec

class MSSQLSchemaDropper(ansisql.ANSISchemaDropper):
    def visit_index(self, index):
        self.append("\nDROP INDEX " + index.table.name + "." + index.name)
        self.execute()

class MSSQLDefaultRunner(ansisql.ANSIDefaultRunner):
    pass

class MSSQLIdentifierPreparer(ansisql.ANSIIdentifierPreparer):
    def __init__(self, dialect):
        super(MSSQLIdentifierPreparer, self).__init__(dialect, initial_quote='[', final_quote=']')

    def _escape_identifier(self, value):
        #TODO: determin MSSQL's escapeing rules
        return value

    def _fold_identifier_case(self, value):
        #TODO: determin MSSQL's case folding rules
        return value

dialect = MSSQLDialect

