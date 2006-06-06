# mssql.py

"""
notes:
  supports both pymssql and adodbapi interfaces

  IDENTITY columns are supported by using SA schema.Sequence() objects. In other words:
         Table('test', mss_engine,
                Column('id',   Integer, Sequence('blah',100,10), primary_key=True),
                Column('name', String(20))
              ).create()

         would yield:
         CREATE TABLE test (
           id INTEGER NOT NULL IDENTITY(100,10) PRIMARY KEY,
           name VARCHAR(20)
           )
  note that the start & increment values for sequences are optional and will default to 1,1

  support for SET IDENTITY_INSERT ON mode (automagic on / off for INSERTs)

  support for auto-fetching of @@IDENTITY on insert

  select.limit implemented as SELECT TOP n


Known issues / TODO:
  no support for more than one IDENTITY column per table
  no support for table reflection of IDENTITY columns with (seed,increment) values other than (1,1)
  no support for GUID type columns (yet)
  pymssql has problems with transaction control that this module attempts to work around
  pymssql has problems with binary and unicode data that this module does NOT work around
  adodbapi fails testtypes.py unit test on unicode data too -- issue with the test?

"""

import sys, StringIO, string, types, re, datetime

import sqlalchemy.sql as sql
import sqlalchemy.engine as engine
import sqlalchemy.engine.default as default
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
import sqlalchemy.types as sqltypes
import sqlalchemy.exceptions as exceptions

try:
    import adodbapi as dbmodule
    # ADODBAPI has a non-standard Connection method
    connect = dbmodule.Connection
    make_connect_string = lambda keys: \
        [["Provider=SQLOLEDB;Data Source=%s;User Id=%s;Password=%s;Initial Catalog=%s" % (
            keys["host"], keys["user"], keys["password"], keys["database"])], {}]
    do_commit = False
except:
    try:
        import pymssql as dbmodule
        connect = dbmodule.connect
        # pymmsql doesn't have a Binary method.  we use string
        dbmodule.Binary = lambda st: str(st)
        make_connect_string = lambda keys:  \
                    [[], keys]
        do_commit = True
    except:
        dbmodule = None
        make_connect_string = lambda keys: [[],{}]
    
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
        return "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}

class MSFloat(sqltypes.Float):
    def get_col_spec(self):
        return "FLOAT(%(precision)s)" % {'precision': self.precision}
    def convert_bind_param(self, value, dialect):
        """By converting to string, we can use Decimal types round-trip."""
        return str(value) 

class MSInteger(sqltypes.Integer):
    def get_col_spec(self):
        return "INTEGER"

class MSSmallInteger(sqltypes.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"

class MSDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "DATETIME"

    def convert_bind_param(self, value, dialect):
        if hasattr(value, "isoformat"):
            #return value.isoformat(' ')
            return value.strftime('%Y-%m-%d %H:%M:%S')            # isoformat() bings on apodbapi -- reported/suggested by Peter Buschman
        else:
            return value

    def convert_result_value(self, value, dialect):
        # adodbapi will return datetimes with empty time values as datetime.date() objects. Promote them back to full datetime.datetime()
        if value and not hasattr(value, 'second'):
            return datetime.datetime(value.year, value.month, value.day)
        return value

class MSDate(sqltypes.Date):
    def get_col_spec(self):
        return "SMALLDATETIME"
    
    def convert_bind_param(self, value, dialect):
        if value and hasattr(value, "isoformat"):
            return value.isoformat()
        return value

    def convert_result_value(self, value, dialect):
        # pymssql will return SMALLDATETIME values as datetime.datetime(), truncate it back to datetime.date()
        if value and hasattr(value, 'second'):
            return value.date()
        return value

class MSText(sqltypes.TEXT):
    def get_col_spec(self):
        return "TEXT"
class MSString(sqltypes.String):
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}
class MSChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}
class MSBinary(sqltypes.Binary):
    def get_col_spec(self):
        return "IMAGE"
class MSBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BIT"
        
colspecs = {
    sqltypes.Integer : MSInteger,
    sqltypes.Smallinteger: MSSmallInteger,
    sqltypes.Numeric : MSNumeric,
    sqltypes.Float : MSFloat,
    sqltypes.DateTime : MSDateTime,
    sqltypes.Date : MSDate,
    sqltypes.String : MSString,
    sqltypes.Binary : MSBinary,
    sqltypes.Boolean : MSBoolean,
    sqltypes.TEXT : MSText,
    sqltypes.CHAR: MSChar,
}

ischema_names = {
    'int' : MSInteger,
    'smallint' : MSSmallInteger,
    'varchar' : MSString,
    'char' : MSChar,
    'text' : MSText,
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

def engine(opts, **params):
    return MSSQLEngine(opts, **params)

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
    def pre_exec(self, engine, proxy, compiled, parameters, **kwargs):
        """ MS-SQL has a special mode for inserting non-NULL values into IDENTITY columns. Activate it if needed. """
        if getattr(compiled, "isinsert", False):
            self.IINSERT = False
            self.HASIDENT = False
            for c in compiled.statement.table.c:
                if hasattr(c,'sequence'):
                    self.HASIDENT = True
                    if parameters.has_key(c.name):
                        self.IINSERT = True
                    break
            if self.IINSERT:
                proxy("SET IDENTITY_INSERT %s ON" % compiled.statement.table.name)

    def post_exec(self, engine, proxy, compiled, parameters, **kwargs):
        """ Turn off the INDENTITY_INSERT mode if it's been activated, and fetch recently inserted IDENTIFY values (works only for one column) """
        if getattr(compiled, "isinsert", False):
            if self.IINSERT:
                proxy("SET IDENTITY_INSERT %s OFF" % compiled.statement.table.name)
                self.IINSERT = False
            elif self.HASIDENT:
                cursor = proxy("SELECT @@IDENTITY AS lastrowid")
                row = cursor.fetchone()
                self.last_inserted_ids = [row[0]]
            self.HASIDENT = False

class MSSQLDialect(ansisql.ANSIDialect):            
    def __init__(self, module = None, **params):
        self.module = module or dbmodule
        self.opts = {}
        ansisql.ANSIDialect.__init__(self, **params)

    def create_connect_args(self, url):
        self.opts = url.translate_connect_args(['host', 'database', 'user', 'password', 'port'])
        return ([], self.opts)

    def connect_args(self):
        return make_connect_string(self.opts)

    def create_execution_context(self):
        return MSSQLExecutionContext(self)

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def supports_sane_rowcount(self):
        return True

    def compiler(self, statement, bindparams, **kwargs):
        return MSSQLCompiler(self, statement, bindparams, **kwargs)

    def schemagenerator(self, *args, **kwargs):
        return MSSQLSchemaGenerator(*args, **kwargs)

    def schemadropper(self, *args, **kwargs):
        return MSSQLSchemaDropper(*args, **kwargs)

    def defaultrunner(self, engine, proxy):
        return MSSQLDefaultRunner(engine, proxy)

    def get_default_schema_name(self):
        return "dbo"
        
    def last_inserted_ids(self):
        return self.context.last_inserted_ids
            
    def do_begin(self, connection):
        """implementations might want to put logic here for turning autocommit on/off, etc."""
        if do_commit:
            pass  

    def _execute(self, c, statement, parameters):
        try:
            c.execute(statement, parameters)
            self.context.rowcount = c.rowcount
            c.DBPROP_COMMITPRESERVE = "Y"
        except Exception, e:
            # del c.parent  # Close the Parent Connection, delete it from the pool        columns = ischema.columns.toengine(self)

            raise exceptions.SQLError(statement, parameters, e)

    def do_rollback(self, connection):
        """implementations might want to put logic here for turning autocommit on/off, etc."""
        if do_commit:
            try:
                # connection.rollback() for pymmsql failed sometimes--the begin tran doesn't show up
                # this is a workaround that seems to be handle it.
                r = self.raw_connection(connection)
                r.query("if @@trancount > 0 rollback tran")
                r.fetch_array()
                r.query("begin tran")
                r.fetch_array()
            except:
                pass
        try:
            del connection
        except:
            raise

    def raw_connection(self, connection):
        """Pull the raw pymmsql connection out--sensative to "pool.ConnectionFairy" and pymssql.pymssqlCnx Classes"""
        try:
            return connection.connection.__dict__['_pymssqlCnx__cnx']
        except:
            return connection.connection.adoConn

    def do_commit(self, connection):
        """implementations might want to put logic here for turning autocommit on/off, etc.
            do_commit is set for pymmsql connections--ADO seems to handle transactions without any issue 
        """
        # ADO Uses Implicit Transactions.
        if do_commit:
            # This is very pymssql specific.  We use this instead of its commit, because it hangs on failed rollbacks.
            # By using the "if" we don't assume an open transaction--much better.
            r = self.raw_connection(connection)
            r.query("if @@trancount > 0 commit tran")
            r.fetch_array()
            r.query("begin tran")
            r.fetch_array()
        else:
            pass
            #connection.supportsTransactions = 1
            try:
                pass
                #connection.adoConn.CommitTrans()
            except:
                pass
                #connection.adoConn.execute("begin trans", {})
            #connection.adoConn.BeginTrans()

    def connection(self):
        """returns a managed DBAPI connection from this SQLEngine's connection pool."""
        c = self._pool.connect()
        c.supportsTransactions = 0
        return c

          
    def dbapi(self):
        return self.module

    def reflecttable(self, connection, table):
        import sqlalchemy.databases.information_schema as ischema
        
        # Get base columns
        if table.schema is not None:
            current_schema = table.schema
        else:
            current_schema = self.get_default_schema_name()

        columns = ischema.columns.toengine(self)
        s = select([columns],
                   current_schema and sql.and_(columns.c.table_name==table.name, columns.c.table_schema==current_schema) or columns.c.table_name==table.name,
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
                    coltype = ischema_names[type]
            coltype = coltype(*args)
            colargs= []
            if default is not None:
                colargs.append(PassiveDefault(sql.text(default)))
                
            table.append_item(schema.Column(name, coltype, nullable=nullable, *colargs))
        
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
        RR = ischema.ref_constraints.toengine(self)    #information_schema.referential_constraints
        TC = ischema.constraints.toengine(self)        #information_schema.table_constraints
        C  = ischema.column_constraints.toengine(self).alias('C') #information_schema.constraint_column_usage: the constrained column 
        R  = ischema.column_constraints.toengine(self).alias('R') #information_schema.constraint_column_usage: the referenced column

        fromjoin = TC.join(RR, RR.c.constraint_name == TC.c.constraint_name).join(C, C.c.constraint_name == RR.c.constraint_name)
        fromjoin = fromjoin.join(R, R.c.constraint_name == RR.c.unique_constraint_name)

        s = select([TC.c.constraint_type, C.c.table_schema, C.c.table_name, C.c.column_name,
                    R.c.table_schema, R.c.table_name, R.c.column_name],
                   and_(RR.c.constraint_schema == current_schema,  C.c.table_name == table.name),
                   from_obj = [fromjoin]
                   )
               
        c = connection.execute(s)

        while True:
            row = c.fetchone()
            if row is None:
                break
            (type, constrained_column, referred_schema, referred_table, referred_column) = (
                row[colmap[0]],
                row[colmap[3]],
                row[colmap[4]],
                row[colmap[5]],
                row[colmap[6]]
                )

            if type=='PRIMARY KEY':
                table.c[constrained_column]._set_primary_key()
            elif type=='FOREIGN KEY':
                remotetable = Table(referred_table, self, autoload = True, schema=referred_schema)
                table.c[constrained_column].append_item(schema.ForeignKey(remotetable.c[referred_column]))
        


class MSSQLCompiler(ansisql.ANSICompiler):
    def __init__(self, dialect, statement, parameters, **kwargs):
        super(MSSQLCompiler, self).__init__(dialect, statement, parameters, **kwargs)
        self.tablealiases = {}

    def visit_select_precolumns(self, select):
        """ MS-SQL puts TOP, it's version of LIMIT here """
        s = select.distinct and "DISTINCT " or ""
        if (select.limit):
            s += "TOP %s " % (select.limit,)
        return s

    def limit_clause(self, select):
        # Limit in mssql is after the select keyword; MSsql has no support for offset
        return ""

            
    def visit_table(self, table):
        # alias schema-qualified tables
        if getattr(table, 'schema', None) is not None and not self.tablealiases.has_key(table):
            alias = table.alias()
            self.tablealiases[table] = alias
            alias.accept_visitor(self)
            self.froms[('alias', table)] = self.froms[table]
            for c in alias.c:
                c.accept_visitor(self)
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
                self.strings[self.tablealiases[column.table].corresponding_column(column.original)]

        
class MSSQLSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, override_pk=False, first_pk=False):
        colspec = column.name + " " + column.type.engine_impl(self.engine).get_col_spec()

        # install a IDENTITY Sequence if we have an implicit IDENTITY column
        if column.primary_key and isinstance(column.type, sqltypes.Integer):
            if column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional):
                column.sequence = schema.Sequence(column.name + '_seq')

        if not column.nullable:
            colspec += " NOT NULL"

        if hasattr(column, 'sequence'):
            colspec += " IDENTITY(%s,%s)" % (column.sequence.start or 1, column.sequence.increment or 1)
        else:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if column.primary_key:
            if not override_pk:
                colspec += " PRIMARY KEY"
        if column.foreign_key:
            colspec += " REFERENCES %s(%s)" % (column.foreign_key.column.table.fullname, column.foreign_key.column.name)
        
        return colspec


class MSSQLSchemaDropper(ansisql.ANSISchemaDropper):
    def visit_index(self, index):
        self.append("\nDROP INDEX " + index.table.name + "." + index.name)
        self.execute()

class MSSQLDefaultRunner(ansisql.ANSIDefaultRunner):
    pass

dialect = MSSQLDialect
