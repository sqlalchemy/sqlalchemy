# mysql.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import sys, StringIO, string, types, re, datetime

from sqlalchemy import sql,engine,schema,ansisql
from sqlalchemy.engine import default
import sqlalchemy.types as sqltypes
import sqlalchemy.exceptions as exceptions
from array import array

try:
    import MySQLdb as mysql
    import MySQLdb.constants.CLIENT as CLIENT_FLAGS
except:
    mysql = None
    CLIENT_FLAGS = None

def kw_colspec(self, spec):
    if self.unsigned:
        spec += ' UNSIGNED'
    if self.zerofill:
        spec += ' ZEROFILL'
    return spec
        
class MSNumeric(sqltypes.Numeric):
    def __init__(self, precision = 10, length = 2, **kw):
        self.unsigned = 'unsigned' in kw
        self.zerofill = 'zerofill' in kw
        super(MSNumeric, self).__init__(precision, length)
    def get_col_spec(self):
        if self.precision is None:
            return kw_colspec(self, "NUMERIC")
        else:
            return kw_colspec(self, "NUMERIC(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length})
class MSDecimal(MSNumeric):
    def get_col_spec(self):
        if self.precision is not None and self.length is not None:
            return kw_colspec(self, "DECIMAL(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length})
class MSDouble(MSNumeric):
    def __init__(self, precision=10, length=2, **kw):
        if (precision is None and length is not None) or (precision is not None and length is None):
            raise exceptions.ArgumentError("You must specify both precision and length or omit both altogether.")
        self.unsigned = 'unsigned' in kw
        self.zerofill = 'zerofill' in kw
        super(MSDouble, self).__init__(precision, length)
    def get_col_spec(self):
        if self.precision is not None and self.length is not None:
            return "DOUBLE(%(precision)s, %(length)s)" % {'precision': self.precision, 'length' : self.length}
        else:
            return kw_colspec(self, "DOUBLE")
class MSFloat(sqltypes.Float):
    def __init__(self, precision=10, length=None, **kw):
        if length is not None:
            self.length=length
        self.unsigned = 'unsigned' in kw
        self.zerofill = 'zerofill' in kw
        super(MSFloat, self).__init__(precision)
    def get_col_spec(self):
        if hasattr(self, 'length') and self.length is not None:
            return kw_colspec(self, "FLOAT(%(precision)s,%(length)s)" % {'precision': self.precision, 'length' : self.length})
        elif self.precision is not None:
            return kw_colspec(self, "FLOAT(%(precision)s)" % {'precision': self.precision})
        else:
            return kw_colspec(self, "FLOAT")
class MSInteger(sqltypes.Integer):
    def __init__(self, length=None, **kw):
        self.length = length
        self.unsigned = 'unsigned' in kw
        self.zerofill = 'zerofill' in kw
        super(MSInteger, self).__init__()
    def get_col_spec(self):
        if self.length is not None:
            return kw_colspec(self, "INTEGER(%(length)s)" % {'length': self.length})
        else:
            return kw_colspec(self, "INTEGER")
class MSBigInteger(MSInteger):
    def get_col_spec(self):
        if self.length is not None:
            return kw_colspec(self, "BIGINT(%(length)s)" % {'length': self.length})
        else:
            return kw_colspec(self, "BIGINT")
class MSSmallInteger(sqltypes.Smallinteger):
    def __init__(self, length=None, **kw):
        self.length = length
        self.unsigned = 'unsigned' in kw
        self.zerofill = 'zerofill' in kw
        super(MSSmallInteger, self).__init__()
    def get_col_spec(self):
        if self.length is not None:
            return kw_colspec(self, "SMALLINT(%(length)s)" % {'length': self.length})
        else:
            return kw_colspec(self, "SMALLINT")
class MSDateTime(sqltypes.DateTime):
    def get_col_spec(self):
        return "DATETIME"
class MSDate(sqltypes.Date):
    def get_col_spec(self):
        return "DATE"
class MSTime(sqltypes.Time):
    def get_col_spec(self):
        return "TIME"
    def convert_result_value(self, value, dialect):
        # convert from a timedelta value
        if value is not None:
            return datetime.time(value.seconds/60/60, value.seconds/60%60, value.seconds - (value.seconds/60*60))
        else:
            return None
            
class MSText(sqltypes.TEXT):
    def __init__(self, **kw):
        self.binary = 'binary' in kw
        super(MSText, self).__init__()
    def get_col_spec(self):
        return "TEXT"
class MSTinyText(MSText):
    def get_col_spec(self):
        if self.binary:
            return "TEXT BINARY"
        else:
           return "TEXT"
class MSMediumText(MSText):
    def get_col_spec(self):
        if self.binary:
            return "MEDIUMTEXT BINARY"
        else:
            return "MEDIUMTEXT"
class MSLongText(MSText):
    def get_col_spec(self):
        if self.binary:
            return "LONGTEXT BINARY"
        else:
            return "LONGTEXT"
class MSString(sqltypes.String):
    def __init__(self, length=None, *extra):
        sqltypes.String.__init__(self, length=length)
    def get_col_spec(self):
        return "VARCHAR(%(length)s)" % {'length' : self.length}
class MSChar(sqltypes.CHAR):
    def get_col_spec(self):
        return "CHAR(%(length)s)" % {'length' : self.length}
class MSBinary(sqltypes.Binary):
    def get_col_spec(self):
        if self.length is not None and self.length <=255:
            # the binary2G type seems to return a value that is null-padded
            return "BINARY(%d)" % self.length
        else:
            return "BLOB"
    def convert_result_value(self, value, dialect):
        if value is None:
            return None
        else:
            return buffer(value)

class MSMediumBlob(MSBinary):
    def get_col_spec(self):
        return "MEDIUMBLOB"
            
class MSEnum(MSString):
    def __init__(self, *enums):
        self.__enums_hidden = enums
        length = 0
        strip_enums = []
        for a in enums:
            if a[0:1] == '"' or a[0:1] == "'":
                a = a[1:-1]
            if len(a) > length:
                length=len(a)
            strip_enums.append(a)
        self.enums = strip_enums
        super(MSEnum, self).__init__(length)
    def get_col_spec(self):
        return "ENUM(%s)" % ",".join(self.__enums_hidden)
        

class MSBoolean(sqltypes.Boolean):
    def get_col_spec(self):
        return "BOOLEAN"
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
#    sqltypes.BIGinteger : MSInteger,
    sqltypes.Integer : MSInteger,
    sqltypes.Smallinteger : MSSmallInteger,
    sqltypes.Numeric : MSNumeric,
    sqltypes.Float : MSFloat,
    sqltypes.DateTime : MSDateTime,
    sqltypes.Date : MSDate,
    sqltypes.Time : MSTime,
    sqltypes.String : MSString,
    sqltypes.Binary : MSBinary,
    sqltypes.Boolean : MSBoolean,
    sqltypes.TEXT : MSText,
    sqltypes.CHAR: MSChar,
}

ischema_names = {
    'boolean':MSBoolean,
    'bigint' : MSBigInteger,
    'int' : MSInteger,
    'mediumint' : MSInteger,
    'smallint' : MSSmallInteger,
    'tinyint' : MSSmallInteger, 
    'varchar' : MSString,
    'char' : MSChar,
    'text' : MSText,
    'tinytext' : MSTinyText,
    'mediumtext': MSMediumText,
    'longtext': MSLongText,
    'decimal' : MSDecimal,
    'numeric' : MSNumeric,
    'float' : MSFloat,
    'double' : MSDouble,
    'timestamp' : MSDateTime,
    'datetime' : MSDateTime,
    'date' : MSDate,
    'time' : MSTime,
    'binary' : MSBinary,
    'blob' : MSBinary,
    'enum': MSEnum,
}

def descriptor():
    return {'name':'mysql',
    'description':'MySQL',
    'arguments':[
        ('username',"Database Username",None),
        ('password',"Database Password",None),
        ('database',"Database Name",None),
        ('host',"Hostname", None),
    ]}


class MySQLExecutionContext(default.DefaultExecutionContext):
    def post_exec(self, engine, proxy, compiled, parameters, **kwargs):
        if getattr(compiled, "isinsert", False):
            self._last_inserted_ids = [proxy().lastrowid]

class MySQLDialect(ansisql.ANSIDialect):
    def __init__(self, module = None, **kwargs):
        if module is None:
            self.module = mysql
        else:
            self.module = module
        ansisql.ANSIDialect.__init__(self, default_paramstyle='format', **kwargs)

    def create_connect_args(self, url):
        opts = url.translate_connect_args(['host', 'db', 'user', 'passwd', 'port'])
        opts.update(url.query)
        def coercetype(param, type):
            if param in opts and type(param) is not type:
                if type is bool:
                    opts[param] = bool(int(opts[param]))
                else:
                    opts[param] = type(opts[param])
        coercetype('compress', bool)
        coercetype('connect_timeout', int)
        coercetype('use_unicode', bool)   # this could break SA Unicode type
        coercetype('charset', str)        # this could break SA Unicode type
        # TODO: what about options like "ssl", "cursorclass" and "conv" ?

        client_flag = opts.get('client_flag', 0)
        if CLIENT_FLAGS is not None:
            client_flag |= CLIENT_FLAGS.FOUND_ROWS
        opts['client_flag'] = client_flag

        return [[], opts]

    def create_execution_context(self):
        return MySQLExecutionContext(self)

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

    def supports_sane_rowcount(self):
        return True

    def compiler(self, statement, bindparams, **kwargs):
        return MySQLCompiler(self, statement, bindparams, **kwargs)

    def schemagenerator(self, *args, **kwargs):
        return MySQLSchemaGenerator(*args, **kwargs)

    def schemadropper(self, *args, **kwargs):
        return MySQLSchemaDropper(*args, **kwargs)

    def preparer(self):
        return MySQLIdentifierPreparer(self)

    def do_executemany(self, cursor, statement, parameters, context=None, **kwargs):
        try:
            rowcount = cursor.executemany(statement, parameters)
            if context is not None:
                context._rowcount = rowcount
        except mysql.OperationalError, o:
            if o.args[0] == 2006 or o.args[0] == 2014:
                cursor.invalidate()
            raise o
    def do_execute(self, cursor, statement, parameters, **kwargs):
        try:
            cursor.execute(statement, parameters)
        except mysql.OperationalError, o:
            if o.args[0] == 2006 or o.args[0] == 2014:
                cursor.invalidate()
            raise o
            

    def do_rollback(self, connection):
        # MySQL without InnoDB doesnt support rollback()
        try:
            connection.rollback()
        except:
            pass

    def get_default_schema_name(self):
        if not hasattr(self, '_default_schema_name'):
            self._default_schema_name = text("select database()", self).scalar()
        return self._default_schema_name
    
    def dbapi(self):
        return self.module

    def has_table(self, connection, table_name, schema=None):
        cursor = connection.execute("show table status like '" + table_name + "'")
        return bool( not not cursor.rowcount )

    def reflecttable(self, connection, table):
        # reference:  http://dev.mysql.com/doc/refman/5.0/en/name-case-sensitivity.html
        cs = connection.execute("show variables like 'lower_case_table_names'").fetchone()[1]
        if isinstance(cs, array):
            cs = cs.tostring()
        case_sensitive = int(cs) == 0
        
        if not case_sensitive:
            table.name = table.name.lower()
            table.metadata.tables[table.name]= table
        try:
            c = connection.execute("describe " + table.fullname, {})
        except:
            raise exceptions.NoSuchTableError(table.name)
        found_table = False
        while True:
            row = c.fetchone()
            if row is None:
                break
            #print "row! " + repr(row)
            if not found_table:
                found_table = True

            # these can come back as unicode if use_unicode=1 in the mysql connection
            (name, type, nullable, primary_key, default) = (str(row[0]), str(row[1]), row[2] == 'YES', row[3] == 'PRI', row[4])
            
            match = re.match(r'(\w+)(\(.*?\))?\s*(\w+)?\s*(\w+)?', type)
            col_type = match.group(1)
            args = match.group(2)
            extra_1 = match.group(3)
            extra_2 = match.group(4)

            #print "coltype: " + repr(col_type) + " args: " + repr(args) + "extras:" + repr(extra_1) + ' ' + repr(extra_2)
            coltype = ischema_names.get(col_type, MSString)
            kw = {}
            if extra_1 is not None:
                kw[extra_1] = True
            if extra_2 is not None:
                kw[extra_2] = True

            if args is not None:
                if col_type == 'enum':
                    args= args[1:-1]
                    argslist = args.split(',')
                    coltype = coltype(*argslist, **kw)
                else:
                    argslist = re.findall(r'(\d+)', args)
                    coltype = coltype(*[int(a) for a in argslist], **kw)

            colargs= []
            if default:
                colargs.append(schema.PassiveDefault(sql.text(default)))
            table.append_column(schema.Column(name, coltype, *colargs, 
                                            **dict(primary_key=primary_key,
                                                   nullable=nullable,
                                                   )))

        tabletype = self.moretableinfo(connection, table=table)
        table.kwargs['mysql_engine'] = tabletype

        if not found_table:
            raise exceptions.NoSuchTableError(table.name)
    
    def moretableinfo(self, connection, table):
        """Return (tabletype, {colname:foreignkey,...})
        execute(SHOW CREATE TABLE child) =>
        CREATE TABLE `child` (
        `id` int(11) default NULL,
        `parent_id` int(11) default NULL,
        KEY `par_ind` (`parent_id`),
        CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE\n) TYPE=InnoDB
        """
        c = connection.execute("SHOW CREATE TABLE " + table.fullname, {})
        desc_fetched = c.fetchone()[1]

        # this can come back as unicode if use_unicode=1 in the mysql connection
        if type(desc_fetched) is unicode:
            desc_fetched = str(desc_fetched)
        elif type(desc_fetched) is not str:
            # may get array.array object here, depending on version (such as mysql 4.1.14 vs. 4.1.11)
            desc_fetched = desc_fetched.tostring()
        desc = desc_fetched.strip()

        tabletype = ''
        lastparen = re.search(r'\)[^\)]*\Z', desc)
        if lastparen:
            match = re.search(r'\b(?:TYPE|ENGINE)=(?P<ttype>.+)\b', desc[lastparen.start():], re.I)
            if match:
                tabletype = match.group('ttype')

        fkpat = r'''CONSTRAINT [`"'](?P<name>.+?)[`"'] FOREIGN KEY \((?P<columns>.+?)\) REFERENCES [`"'](?P<reftable>.+?)[`"'] \((?P<refcols>.+?)\)'''
        for match in re.finditer(fkpat, desc):
            columns = re.findall(r'''[`"'](.+?)[`"']''', match.group('columns'))
            refcols = [match.group('reftable') + "." + x for x in re.findall(r'''[`"'](.+?)[`"']''', match.group('refcols'))]
            schema.Table(match.group('reftable'), table.metadata, autoload=True, autoload_with=connection)
            constraint = schema.ForeignKeyConstraint(columns, refcols, name=match.group('name'))
            table.append_constraint(constraint)

        return tabletype
        

class MySQLCompiler(ansisql.ANSICompiler):

    def visit_cast(self, cast):
        """hey ho MySQL supports almost no types at all for CAST"""
        if (isinstance(cast.type, sqltypes.Date) or isinstance(cast.type, sqltypes.Time) or isinstance(cast.type, sqltypes.DateTime)):
            return super(MySQLCompiler, self).visit_cast(cast)
        else:
            # so just skip the CAST altogether for now.
            # TODO: put whatever MySQL does for CAST here.
            self.strings[cast] = self.strings[cast.clause]

    def for_update_clause(self, select):
        if select.for_update == 'read':
             return ' LOCK IN SHARE MODE'
        else:
            return super(MySQLCompiler, self).for_update_clause(select)

    def limit_clause(self, select):
        text = ""
        if select.limit is not None:
            text +=  " \n LIMIT " + str(select.limit)
        if select.offset is not None:
            if select.limit is None:
                # striaght from the MySQL docs, I kid you not
                text += " \n LIMIT 18446744073709551615"
            text += " OFFSET " + str(select.offset)
        return text
        
class MySQLSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, override_pk=False, first_pk=False):
        t = column.type.engine_impl(self.engine)
        colspec = self.preparer.format_column(column) + " " + column.type.engine_impl(self.engine).get_col_spec()
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        if column.primary_key:
            if len(column.foreign_keys)==0 and first_pk and column.autoincrement and isinstance(column.type, sqltypes.Integer):
                colspec += " AUTO_INCREMENT"
        return colspec

    def post_create_table(self, table):
        args = ""
        for k in table.kwargs:
            if k.startswith('mysql_'):
                opt = k[6:]
                args += " %s=%s" % (opt.upper(), table.kwargs[k])
        return args

class MySQLSchemaDropper(ansisql.ANSISchemaDropper):
    def visit_index(self, index):
        self.append("\nDROP INDEX " + index.name + " ON " + index.table.name)
        self.execute()
    def drop_foreignkey(self, constraint):
        self.append("ALTER TABLE %s DROP FOREIGN KEY %s" % (self.preparer.format_table(constraint.table), constraint.name))
        self.execute()

class MySQLIdentifierPreparer(ansisql.ANSIIdentifierPreparer):
    def __init__(self, dialect):
        super(MySQLIdentifierPreparer, self).__init__(dialect, initial_quote='`')
    def _escape_identifier(self, value):
        #TODO: determin MySQL's escaping rules
        return value
    def _fold_identifier_case(self, value):
        #TODO: determin MySQL's case folding rules
        return value

dialect = MySQLDialect
