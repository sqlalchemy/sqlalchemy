# access.py
# Copyright (C) 2007 Paul Johnston, paj@pajhome.org.uk
# Portions derived from jet2sql.py by Matt Keranen, mksql@yahoo.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import sys, string, re, datetime, random
from sqlalchemy import sql, engine, schema, ansisql, types, exceptions, pool
import sqlalchemy.engine.default as default


class AcNumeric(types.Numeric):
    def convert_result_value(self, value, dialect):
        return value

    def convert_bind_param(self, value, dialect):
        if value is None:
            # Not sure that this exception is needed
            return value
        else:
            return str(value)

    def get_col_spec(self):
        return "NUMERIC"

class AcFloat(types.Float):
    def get_col_spec(self):
        return "FLOAT"

    def convert_bind_param(self, value, dialect):
        """By converting to string, we can use Decimal types round-trip."""
        if not value is None:
            return str(value)
        return None

class AcInteger(types.Integer):
    def get_col_spec(self):
        return "INTEGER"

class AcTinyInteger(types.Integer):
    def get_col_spec(self):
        return "TINYINT"

class AcSmallInteger(types.Smallinteger):
    def get_col_spec(self):
        return "SMALLINT"

class AcDateTime(types.DateTime):
    def __init__(self, *a, **kw):
        super(AcDateTime, self).__init__(False)

    def get_col_spec(self):
        return "DATETIME"

class AcDate(types.Date):
    def __init__(self, *a, **kw):
        super(AcDate, self).__init__(False)

    def get_col_spec(self):
        return "DATETIME"

class AcText(types.TEXT):
    def get_col_spec(self):
        return "MEMO"

class AcString(types.String):
    def get_col_spec(self):
        return "TEXT" + (self.length and ("(%d)" % self.length) or "")

class AcUnicode(types.Unicode):
    def get_col_spec(self):
        return "TEXT" + (self.length and ("(%d)" % self.length) or "")

    def convert_bind_param(self, value, dialect):
        return value

    def convert_result_value(self, value, dialect):
        return value

class AcChar(types.CHAR):
    def get_col_spec(self):        
        return "TEXT" + (self.length and ("(%d)" % self.length) or "")

class AcBinary(types.Binary):
    def get_col_spec(self):
        return "BINARY"

class AcBoolean(types.Boolean):
    def get_col_spec(self):
        return "YESNO"

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

class AcTimeStamp(types.TIMESTAMP):
    def get_col_spec(self):
        return "TIMESTAMP"

def descriptor():
    return {'name':'access',
    'description':'Microsoft Access',
    'arguments':[
        ('user',"Database user name",None),
        ('password',"Database password",None),
        ('db',"Path to database file",None),
    ]}

class AccessExecutionContext(default.DefaultExecutionContext):
    def _has_implicit_sequence(self, column):
        if column.primary_key and column.autoincrement:
            if isinstance(column.type, types.Integer) and not column.foreign_key:
                if column.default is None or (isinstance(column.default, schema.Sequence) and \
                                              column.default.optional):
                    return True
        return False

    def post_exec(self):
        """If we inserted into a row with a COUNTER column, fetch the ID"""

        if self.compiled.isinsert:
            tbl = self.compiled.statement.table
            if not hasattr(tbl, 'has_sequence'):
                tbl.has_sequence = None
                for column in tbl.c:
                    if getattr(column, 'sequence', False) or self._has_implicit_sequence(column):
                        tbl.has_sequence = column
                        break

            if bool(tbl.has_sequence):
                if not len(self._last_inserted_ids) or self._last_inserted_ids[0] is None:
                    self.cursor.execute("SELECT @@identity AS lastrowid")
                    row = self.cursor.fetchone()
                    self._last_inserted_ids = [int(row[0])] + self._last_inserted_ids[1:]
                    # print "LAST ROW ID", self._last_inserted_ids

        super(AccessExecutionContext, self).post_exec()


class AccessDialect(ansisql.ANSIDialect):
    colspecs = {
        types.Unicode : AcUnicode,
        types.Integer : AcInteger,
        types.Smallinteger: AcSmallInteger,
        types.Numeric : AcNumeric,
        types.Float : AcFloat,
        types.DateTime : AcDateTime,
        types.Date : AcDate,
        types.String : AcString,
        types.Binary : AcBinary,
        types.Boolean : AcBoolean,
        types.TEXT : AcText,
        types.CHAR: AcChar,
        types.TIMESTAMP: AcTimeStamp,
    }

    def type_descriptor(self, typeobj):
        newobj = types.adapt_type(typeobj, self.colspecs)
        return newobj

    def __init__(self, **params):
        super(AccessDialect, self).__init__(**params)
        self.text_as_varchar = False
        self._dtbs = None

    def dbapi(cls):
        import win32com.client
        win32com.client.gencache.EnsureModule('{00025E01-0000-0000-C000-000000000046}', 0, 5, 0)

        global const, daoEngine
        const = win32com.client.constants
        daoEngine = win32com.client.Dispatch('DAO.DBEngine.36')

        import pyodbc as module
        return module
    dbapi = classmethod(dbapi)

    def create_connect_args(self, url):
        opts = url.translate_connect_args(['host', 'database', 'username', 'password', 'port'])
        connectors = ["Driver={Microsoft Access Driver (*.mdb)}"]
        connectors.append("Dbq=%s" % opts["database"])
        user = opts.get("user")
        if user:
            connectors.append("UID=%s" % user)
            connectors.append("PWD=%s" % opts.get("password", ""))
        return [[";".join (connectors)], {}]

    def create_execution_context(self, *args, **kwargs):
        return AccessExecutionContext(self, *args, **kwargs)

    def supports_sane_rowcount(self):
        return False

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def compiler(self, statement, bindparams, **kwargs):
        return AccessCompiler(self, statement, bindparams, **kwargs)

    def schemagenerator(self, *args, **kwargs):
        return AccessSchemaGenerator(self, *args, **kwargs)

    def schemadropper(self, *args, **kwargs):
        return AccessSchemaDropper(self, *args, **kwargs)

    def defaultrunner(self, connection, **kwargs):
        return AccessDefaultRunner(connection, **kwargs)

    def preparer(self):
        return AccessIdentifierPreparer(self)

    def do_execute(self, cursor, statement, params, **kwargs):
        if params == {}:
            params = ()
        super(AccessDialect, self).do_execute(cursor, statement, params, **kwargs)

    def _execute(self, c, statement, parameters):
        try:
            if parameters == {}:
                parameters = ()
            c.execute(statement, parameters)
            self.context.rowcount = c.rowcount
        except Exception, e:
            raise exceptions.SQLError(statement, parameters, e)

    def has_table(self, connection, tablename, schema=None):
        # This approach seems to be more reliable that using DAO
        try:
            connection.execute('select top 1 * from [%s]' % tablename)
            return True
        except Exception, e:
            return False

    def reflecttable(self, connection, table):        
        # This is defined in the function, as it relies on win32com constants,
        # that aren't imported until dbapi method is called
        if not hasattr(self, 'ischema_names'):
            self.ischema_names = {
                const.dbByte:       AcBinary,
                const.dbInteger:    AcInteger,
                const.dbLong:       AcInteger,
                const.dbSingle:     AcFloat,
                const.dbDouble:     AcFloat,
                const.dbDate:       AcDateTime,
                const.dbLongBinary: AcBinary,
                const.dbMemo:       AcText,
                const.dbBoolean:    AcBoolean,
                const.dbText:       AcUnicode, # All Access strings are unicode
            }
            
        # A fresh DAO connection is opened for each reflection
        # This is necessary, so we get the latest updates
        opts = connection.engine.url.translate_connect_args(['host', 'database', 'username', 'password', 'port'])
        dtbs = daoEngine.OpenDatabase(opts['database'])
        
        try:
            for tbl in dtbs.TableDefs:
                if tbl.Name.lower() == table.name.lower():
                    break
            else:
                raise exceptions.NoSuchTableError(table.name)

            for col in tbl.Fields:
                coltype = self.ischema_names[col.Type]
                if col.Type == const.dbText:
                    coltype = coltype(col.Size)

                colargs = \
                {
                    'nullable': not(col.Required or col.Attributes & const.dbAutoIncrField),
                }
                default = col.DefaultValue

                if col.Attributes & const.dbAutoIncrField:
                    colargs['default'] = schema.Sequence(col.Name + '_seq')
                elif default:
                    if col.Type == const.dbBoolean:
                        default = default == 'Yes' and '1' or '0'
                    colargs['default'] = schema.PassiveDefault(sql.text(default))

                table.append_column(schema.Column(col.Name, coltype, **colargs))

                # TBD: check constraints

            # Find primary key columns first
            for idx in tbl.Indexes:
                if idx.Primary:
                    for col in idx.Fields:
                        thecol = table.c[col.Name]
                        table.primary_key.add(thecol)
                        if isinstance(thecol.type, AcInteger) and \
                                not (thecol.default and isinstance(thecol.default.arg, schema.Sequence)):
                            thecol.autoincrement = False

            # Then add other indexes
            for idx in tbl.Indexes:
                if not idx.Primary:
                    if len(idx.Fields) == 1:
                        col = table.c[idx.Fields[0].Name]
                        if not col.primary_key:
                            col.index = True
                            col.unique = idx.Unique
                    else:
                        pass # TBD: multi-column indexes
                

            for fk in dtbs.Relations:
                if fk.ForeignTable != table.name:
                    continue
                scols = [c.ForeignName for c in fk.Fields]
                rcols = ['%s.%s' % (fk.Table, c.Name) for c in fk.Fields]
                table.append_constraint(schema.ForeignKeyConstraint(scols, rcols))

        finally:
            dtbs.Close()

    def table_names(self, connection, schema):
        # A fresh DAO connection is opened for each reflection
        # This is necessary, so we get the latest updates
        opts = connection.engine.url.translate_connect_args(['host', 'database', 'username', 'password', 'port'])
        dtbs = daoEngine.OpenDatabase(opts['database'])

        names = [t.Name for t in dtbs.TableDefs if t.Name[:4] != "MSys" and t.Name[:4] <> "~TMP"]
        dtbs.Close()
        return names


class AccessCompiler(ansisql.ANSICompiler):
    def visit_select_precolumns(self, select):
        """Access puts TOP, it's version of LIMIT here """
        s = select.distinct and "DISTINCT " or ""
        if select.limit:
            s += "TOP %s " % (select.limit)
        if select.offset:
            raise exceptions.InvalidRequestError('Access does not support LIMIT with an offset')
        return s

    def limit_clause(self, select):
        """Limit in access is after the select keyword"""
        return ""

    def binary_operator_string(self, binary):
        """Access uses "mod" instead of "%" """
        return binary.operator == '%' and 'mod' or binary.operator

    def visit_select(self, select):
        """Label function calls, so they return a name in cursor.description"""
        for i,c in enumerate(select._raw_columns):
            if isinstance(c, sql._Function):
                select._raw_columns[i] = c.label(c.name + "_" + hex(random.randint(0, 65535))[2:])

        super(AccessCompiler, self).visit_select(select)

    function_rewrites =  {'current_date':       'now',
                          'current_timestamp':  'now',
                          'length':             'len',
                          }
    def visit_function(self, func):
        """Access function names differ from the ANSI SQL names; rewrite common ones"""
        func.name = self.function_rewrites.get(func.name, func.name)
        super(AccessCompiler, self).visit_function(func)

    def for_update_clause(self, select):
        """FOR UPDATE is not supported by Access; silently ignore"""
        return ''


class AccessSchemaGenerator(ansisql.ANSISchemaGenerator):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column) + " " + column.type.dialect_impl(self.dialect).get_col_spec()

        # install a sequence if we have an implicit IDENTITY column
        if (not getattr(column.table, 'has_sequence', False)) and column.primary_key and \
                column.autoincrement and isinstance(column.type, types.Integer) and not column.foreign_key:
            if column.default is None or (isinstance(column.default, schema.Sequence) and column.default.optional):
                column.sequence = schema.Sequence(column.name + '_seq')

        if not column.nullable:
            colspec += " NOT NULL"

        if hasattr(column, 'sequence'):
            column.table.has_sequence = column
            colspec = self.preparer.format_column(column) + " counter"
        else:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        return colspec

class AccessSchemaDropper(ansisql.ANSISchemaDropper):
    def visit_index(self, index):
        self.append("\nDROP INDEX [%s].[%s]" % (index.table.name, index.name))
        self.execute()

class AccessDefaultRunner(ansisql.ANSIDefaultRunner):
    pass

class AccessIdentifierPreparer(ansisql.ANSIIdentifierPreparer):
    def __init__(self, dialect):
        super(AccessIdentifierPreparer, self).__init__(dialect, initial_quote='[', final_quote=']')


dialect = AccessDialect
dialect.poolclass = pool.SingletonThreadPool
