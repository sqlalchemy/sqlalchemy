"""Requirements specific to SQLAlchemy's own unit tests.


"""

from sqlalchemy import util
import sys
from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing import exclusions
from sqlalchemy.testing.exclusions import \
     skip, \
     skip_if,\
     only_if,\
     only_on,\
     fails_on_everything_except,\
     fails_on,\
     fails_if,\
     succeeds_if,\
     SpecPredicate,\
     against,\
     LambdaPredicate

def no_support(db, reason):
    return SpecPredicate(db, description=reason)

def exclude(db, op, spec, description=None):
    return SpecPredicate(db, op, spec, description=description)

class DefaultRequirements(SuiteRequirements):
    @property
    def deferrable_or_no_constraints(self):
        """Target database must support derferable constraints."""

        return skip_if([
            no_support('firebird', 'not supported by database'),
            no_support('mysql', 'not supported by database'),
            no_support('mssql', 'not supported by database'),
            ])

    @property
    def named_constraints(self):
        """target database must support names for constraints."""

        return skip_if([
            no_support('sqlite', 'not supported by database'),
            ])

    @property
    def foreign_keys(self):
        """Target database must support foreign keys."""

        return skip_if(
                no_support('sqlite', 'not supported by database')
            )

    @property
    def on_update_cascade(self):
        """target database must support ON UPDATE..CASCADE behavior in
        foreign keys."""

        return skip_if(
                    ['sqlite', 'oracle'],
                    'target backend %(doesnt_support)s ON UPDATE CASCADE'
                )

    @property
    def non_updating_cascade(self):
        """target database must *not* support ON UPDATE..CASCADE behavior in
        foreign keys."""

        return fails_on_everything_except('sqlite', 'oracle', '+zxjdbc') + \
            skip_if('mssql')

    @property
    def deferrable_fks(self):
        """target database must support deferrable fks"""

        return only_on(['oracle'])


    @property
    def unbounded_varchar(self):
        """Target database must support VARCHAR with no length"""

        return skip_if([
                "firebird", "oracle", "mysql"
            ], "not supported by database"
            )

    @property
    def boolean_col_expressions(self):
        """Target database must support boolean expressions as columns"""
        return skip_if([
            no_support('firebird', 'not supported by database'),
            no_support('oracle', 'not supported by database'),
            no_support('mssql', 'not supported by database'),
            no_support('sybase', 'not supported by database'),
        ])

    @property
    def standalone_binds(self):
        """target database/driver supports bound parameters as column expressions
        without being in the context of a typed column.

        """
        return skip_if(["firebird", "mssql+mxodbc"],
                "not supported by driver")

    @property
    def identity(self):
        """Target database must support GENERATED AS IDENTITY or a facsimile.

        Includes GENERATED AS IDENTITY, AUTOINCREMENT, AUTO_INCREMENT, or other
        column DDL feature that fills in a DB-generated identifier at INSERT-time
        without requiring pre-execution of a SEQUENCE or other artifact.

        """
        return skip_if(["firebird", "oracle", "postgresql", "sybase"],
                "not supported by database"
            )

    @property
    def reflectable_autoincrement(self):
        """Target database must support tables that can automatically generate
        PKs assuming they were reflected.

        this is essentially all the DBs in "identity" plus Postgresql, which
        has SERIAL support.  FB and Oracle (and sybase?) require the Sequence to
        be explicitly added, including if the table was reflected.
        """
        return skip_if(["firebird", "oracle", "sybase"],
                "not supported by database"
            )

    @property
    def insert_from_select(self):
        return skip_if(
                    ["firebird"], "crashes for unknown reason"
                )

    @property
    def fetch_rows_post_commit(self):
        return skip_if(
                    ["firebird"], "not supported"
                )

    @property
    def binary_comparisons(self):
        """target database/driver can allow BLOB/BINARY fields to be compared
        against a bound parameter value.
        """
        return skip_if(["oracle", "mssql"],
                "not supported by database/driver"
            )

    @property
    def binary_literals(self):
        """target backend supports simple binary literals, e.g. an
        expression like::

            SELECT CAST('foo' AS BINARY)

        Where ``BINARY`` is the type emitted from :class:`.LargeBinary`,
        e.g. it could be ``BLOB`` or similar.

        Basically fails on Oracle.

        """
        # adding mssql here since it doesn't support comparisons either,
        # have observed generally bad behavior with binary / mssql.

        return skip_if(["oracle", "mssql"],
                "not supported by database/driver"
            )

    @property
    def independent_cursors(self):
        """Target must support simultaneous, independent database cursors
        on a single connection."""

        return skip_if(["mssql+pyodbc", "mssql+mxodbc"], "no driver support")

    @property
    def independent_connections(self):
        """Target must support simultaneous, independent database connections."""

        # This is also true of some configurations of UnixODBC and probably win32
        # ODBC as well.
        return skip_if([
                    no_support("sqlite",
                            "independent connections disabled "
                                "when :memory: connections are used"),
                    exclude("mssql", "<", (9, 0, 0),
                            "SQL Server 2005+ is required for "
                                "independent connections"
                        )
                    ]
                )

    @property
    def updateable_autoincrement_pks(self):
        """Target must support UPDATE on autoincrement/integer primary key."""

        return skip_if(["mssql", "sybase"],
                "IDENTITY columns can't be updated")

    @property
    def isolation_level(self):
        return only_on(
                    ('postgresql', 'sqlite', 'mysql'),
                    "DBAPI has no isolation level support"
                ) + fails_on('postgresql+pypostgresql',
                          'pypostgresql bombs on multiple isolation level calls')

    @property
    def row_triggers(self):
        """Target must support standard statement-running EACH ROW triggers."""

        return skip_if([
            # no access to same table
            no_support('mysql', 'requires SUPER priv'),
            exclude('mysql', '<', (5, 0, 10), 'not supported by database'),

            # huh?  TODO: implement triggers for PG tests, remove this
            no_support('postgresql',
                    'PG triggers need to be implemented for tests'),
        ])

    @property
    def correlated_outer_joins(self):
        """Target must support an outer join to a subquery which
        correlates to the parent."""

        return skip_if("oracle", 'Raises "ORA-01799: a column may not be '
                    'outer-joined to a subquery"')

    @property
    def update_from(self):
        """Target must support UPDATE..FROM syntax"""

        return only_on(['postgresql', 'mssql', 'mysql'],
                "Backend does not support UPDATE..FROM")


    @property
    def update_where_target_in_subquery(self):
        """Target must support UPDATE where the same table is present in a
        subquery in the WHERE clause.

        This is an ANSI-standard syntax that apparently MySQL can't handle,
        such as:

        UPDATE documents SET flag=1 WHERE documents.title IN
            (SELECT max(documents.title) AS title
                FROM documents GROUP BY documents.user_id
            )
        """
        return fails_if('mysql', 'MySQL error 1093 "Cant specify target table '
                                        'for update in FROM clause"')

    @property
    def savepoints(self):
        """Target database must support savepoints."""

        return skip_if([
                    "sqlite",
                    "sybase",
                    ("mysql", "<", (5, 0, 3)),
                    ], "savepoints not supported")


    @property
    def schemas(self):
        """Target database must support external schemas, and have one
        named 'test_schema'."""

        return skip_if([
                    "sqlite",
                    "firebird"
                ], "no schema support")

    @property
    def cross_schema_fk_reflection(self):
        """target system must support reflection of inter-schema foreign keys
        """
        return only_on([
                    "postgresql"
                ])

    @property
    def unique_constraint_reflection(self):
        return fails_on_everything_except(
                    "postgresql",
                    "mysql",
                    "sqlite"
                )

    @property
    def update_nowait(self):
        """Target database must support SELECT...FOR UPDATE NOWAIT"""
        return skip_if(["firebird", "mssql", "mysql", "sqlite", "sybase"],
                "no FOR UPDATE NOWAIT support"
            )

    @property
    def subqueries(self):
        """Target database must support subqueries."""

        return skip_if(exclude('mysql', '<', (4, 1, 1)), 'no subquery support')

    @property
    def mod_operator_as_percent_sign(self):
        """target database must use a plain percent '%' as the 'modulus'
        operator."""

        return only_if(
                    ['mysql', 'sqlite', 'postgresql+psycopg2', 'mssql']
                )

    @property
    def intersect(self):
        """Target database must support INTERSECT or equivalent."""

        return fails_if([
                "firebird", "mysql", "sybase",
            ], 'no support for INTERSECT')

    @property
    def except_(self):
        """Target database must support EXCEPT or equivalent (i.e. MINUS)."""
        return fails_if([
                "firebird", "mysql", "sybase",
            ], 'no support for EXCEPT')

    @property
    def offset(self):
        """Target database must support some method of adding OFFSET or
        equivalent to a result set."""
        return fails_if([
                "sybase"
            ], 'no support for OFFSET or equivalent')

    @property
    def window_functions(self):
        return only_if([
                    "postgresql", "mssql", "oracle"
                ], "Backend does not support window functions")

    @property
    def two_phase_transactions(self):
        """Target database must support two-phase transactions."""

        return skip_if([
            no_support('firebird', 'no SA implementation'),
            no_support('mssql', 'two-phase xact not supported by drivers'),
            no_support('oracle', 'two-phase xact not implemented in SQLA/oracle'),
            no_support('drizzle', 'two-phase xact not supported by database'),
            no_support('sqlite', 'two-phase xact not supported by database'),
            no_support('sybase', 'two-phase xact not supported by drivers/SQLA'),
            no_support('postgresql+zxjdbc',
                    'FIXME: JDBC driver confuses the transaction state, may '
                       'need separate XA implementation'),
            exclude('mysql', '<', (5, 0, 3),
                        'two-phase xact not supported by database'),
            no_support("postgresql+pg8000", "not supported and/or hangs")
            ])

    @property
    def graceful_disconnects(self):
        """Target driver must raise a DBAPI-level exception, such as
        InterfaceError, when the underlying connection has been closed
        and the execute() method is called.
        """
        return fails_on(
                    "postgresql+pg8000", "Driver crashes"
                )

    @property
    def views(self):
        """Target database must support VIEWs."""

        return skip_if("drizzle", "no VIEW support")

    @property
    def empty_strings_varchar(self):
        """target database can persist/return an empty string with a varchar."""

        return fails_if(["oracle"],
                        'oracle converts empty strings to a blank space')

    @property
    def empty_strings_text(self):
        """target database can persist/return an empty string with an
        unbounded text."""

        return exclusions.open()

    @property
    def unicode_data(self):
        """target drive must support unicode data stored in columns."""
        return skip_if([
            no_support("sybase", "no unicode driver support")
            ])

    @property
    def unicode_connections(self):
        """Target driver must support some encoding of Unicode across the wire."""
        # TODO: expand to exclude MySQLdb versions w/ broken unicode
        return skip_if([
            exclude('mysql', '<', (4, 1, 1), 'no unicode connection support'),
            ])

    @property
    def unicode_ddl(self):
        """Target driver must support some degree of non-ascii symbol names."""
        # TODO: expand to exclude MySQLdb versions w/ broken unicode

        return skip_if([
            no_support('oracle', 'FIXME: no support in database?'),
            no_support('sybase', 'FIXME: guessing, needs confirmation'),
            no_support('mssql+pymssql', 'no FreeTDS support'),
            LambdaPredicate(
                lambda config: against(config, "mysql+mysqlconnector") and
                config.db.dialect._mysqlconnector_version_info > (2, 0) and
                util.py2k,
                "bug in mysqlconnector 2.0"
            ),
            LambdaPredicate(
                lambda config: against(config, 'mssql+pyodbc') and
                config.db.dialect.freetds and
                config.db.dialect.freetds_driver_version < "0.91",
                "older freetds doesn't support unicode DDL"
            ),
            exclude('mysql', '<', (4, 1, 1), 'no unicode connection support'),
        ])

    @property
    def sane_rowcount(self):
        return skip_if(
            lambda config: not config.db.dialect.supports_sane_rowcount,
            "driver doesn't support 'sane' rowcount"
        )


    @property
    def emulated_lastrowid(self):
        """"target dialect retrieves cursor.lastrowid or an equivalent
        after an insert() construct executes.
        """
        return fails_on_everything_except('mysql',
                                      'sqlite+pysqlite',
                                      'sybase', 'mssql')

    @property
    def implements_get_lastrowid(self):
        return skip_if([
            no_support('sybase', 'not supported by database'),
            ])

    @property
    def dbapi_lastrowid(self):
        """"target backend includes a 'lastrowid' accessor on the DBAPI
        cursor object.

        """
        return skip_if('mssql+pymssql', 'crashes on pymssql') + \
                    fails_on_everything_except('mysql',
                                       'sqlite+pysqlite')

    @property
    def sane_multi_rowcount(self):
        return fails_if(
            lambda config: not config.db.dialect.supports_sane_multi_rowcount,
            "driver %(driver)s %(doesnt_support)s 'sane' multi row count"
        )

    @property
    def nullsordering(self):
        """Target backends that support nulls ordering."""
        return fails_on_everything_except('postgresql', 'oracle', 'firebird')

    @property
    def reflects_pk_names(self):
        """Target driver reflects the name of primary key constraints."""

        return fails_on_everything_except('postgresql', 'oracle', 'mssql',
                    'sybase')

    @property
    def datetime_literals(self):
        """target dialect supports rendering of a date, time, or datetime as a
        literal string, e.g. via the TypeEngine.literal_processor() method.

        """

        return fails_on_everything_except("sqlite")

    @property
    def datetime(self):
        """target dialect supports representation of Python
        datetime.datetime() objects."""

        return exclusions.open()

    @property
    def datetime_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects."""

        return skip_if(['mssql', 'mysql', 'firebird', '+zxjdbc',
                    'oracle', 'sybase'])

    @property
    def datetime_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1900) values."""

        return succeeds_if(['sqlite', 'postgresql', 'firebird'])

    @property
    def date(self):
        """target dialect supports representation of Python
        datetime.date() objects."""

        return exclusions.open()

    @property
    def date_coerces_from_datetime(self):
        """target dialect accepts a datetime object as the target
        of a date column."""

        return fails_on('mysql+mysqlconnector')

    @property
    def date_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1900) values."""

        return succeeds_if(['sqlite', 'postgresql', 'firebird'])

    @property
    def time(self):
        """target dialect supports representation of Python
        datetime.time() objects."""

        return skip_if(['oracle'])

    @property
    def time_microseconds(self):
        """target dialect supports representation of Python
        datetime.time() with microsecond objects."""

        return skip_if(['mssql', 'mysql', 'firebird', '+zxjdbc',
                    'oracle', 'sybase'])


    @property
    def precision_numerics_general(self):
        """target backend has general support for moderately high-precision
        numerics."""
        return exclusions.open()

    @property
    def precision_numerics_enotation_small(self):
        """target backend supports Decimal() objects using E notation
        to represent very small values."""
        # NOTE: this exclusion isn't used in current tests.
        return exclusions.open()

    @property
    def precision_numerics_enotation_large(self):
        """target backend supports Decimal() objects using E notation
        to represent very large values."""

        return skip_if(
                [("sybase+pyodbc", None, None,
                    "Don't know how do get these values through FreeTDS + Sybase"),
                ("firebird", None, None, "Precision must be from 1 to 18"),]
            )

    @property
    def precision_numerics_many_significant_digits(self):
        """target backend supports values with many digits on both sides,
        such as 319438950232418390.273596, 87673.594069654243

        """
        return fails_if(
                    [('sqlite', None, None, 'TODO'),
                    ("firebird", None, None, "Precision must be from 1 to 18"),
                    ("sybase+pysybase", None, None, "TODO"),
                    ('mssql+pymssql', None, None, 'FIXME: improve pymssql dec handling')]
                )

    @property
    def precision_numerics_retains_significant_digits(self):
        """A precision numeric type will return empty significant digits,
        i.e. a value such as 10.000 will come back in Decimal form with
        the .000 maintained."""

        return fails_if(
                [
                    ('oracle', None, None,
            "this may be a bug due to the difficulty in handling "
            "oracle precision numerics"),
                    ("firebird", None, None,
        "database and/or driver truncates decimal places.")
                ]
                )

    @property
    def precision_generic_float_type(self):
        """target backend will return native floating point numbers with at
        least seven decimal places when using the generic Float type."""

        return fails_if([
                    ('mysql', None, None,
                                'mysql FLOAT type only returns 4 decimals'),
                    ('firebird', None, None,
                                "firebird FLOAT type isn't high precision"),
                ])

    @property
    def floats_to_four_decimals(self):
        return fails_if([
                    ("mysql+oursql", None, None, "Floating point error"),
                    ("firebird", None, None,
                        "Firebird still has FP inaccuracy even "
                        "with only four decimal places"),
                    ('mssql+pyodbc', None, None,
                                'mssql+pyodbc has FP inaccuracy even with '
                                'only four decimal places '
                            ),
                    ('mssql+pymssql', None, None,
                                'mssql+pymssql has FP inaccuracy even with '
                                'only four decimal places '),
                    (
                        'postgresql+pg8000', None, None,
                        'postgresql+pg8000 has FP inaccuracy even with '
                        'only four decimal places '),
                ])

    @property
    def fetch_null_from_numeric(self):
        return skip_if(
                    ("mssql+pyodbc", None, None, "crashes due to bug #351"),
                )

    @property
    def python2(self):
        return skip_if(
                lambda: sys.version_info >= (3,),
                "Python version 2.xx is required."
                )

    @property
    def python3(self):
        return skip_if(
                lambda: sys.version_info < (3,),
                "Python version 3.xx is required."
                )

    @property
    def cpython(self):
        return only_if(lambda: util.cpython,
               "cPython interpreter needed"
             )


    @property
    def non_broken_pickle(self):
        from sqlalchemy.util import pickle
        return only_if(
            lambda: not util.pypy and pickle.__name__ == 'cPickle'
                or sys.version_info >= (3, 2),
            "Needs cPickle+cPython or newer Python 3 pickle"
        )


    @property
    def predictable_gc(self):
        """target platform must remove all cycles unconditionally when
        gc.collect() is called, as well as clean out unreferenced subclasses.

        """
        return self.cpython

    @property
    def hstore(self):
        def check_hstore(config):
            if not against(config, "postgresql"):
                return False
            try:
                config.db.execute("SELECT 'a=>1,a=>2'::hstore;")
                return True
            except:
                return False

        return only_if(check_hstore)

    @property
    def range_types(self):
        def check_range_types(config):
            if not against(config, "postgresql+psycopg2"):
                return False
            try:
                config.db.execute("select '[1,2)'::int4range;")
                # only supported in psycopg 2.5+
                from psycopg2.extras import NumericRange
                return True
            except:
                return False

        return only_if(check_range_types)


    @property
    def oracle_test_dblink(self):
        return skip_if(
                    lambda config: not config.file_config.has_option(
                        'sqla_testing', 'oracle_db_link'),
                    "oracle_db_link option not specified in config"
                )

    @property
    def percent_schema_names(self):
        return skip_if(
            [
                (
                    "+psycopg2", None, None,
                    "psycopg2 2.4 no longer accepts percent "
                    "sign in bind placeholders"),
                ("mysql", None, None, "executemany() doesn't work here")
            ]
        )

    @property
    def order_by_label_with_expression(self):
        return fails_if([
                    ('firebird', None, None, "kinterbasdb doesn't send full type information"),
                    ('postgresql', None, None, 'only simple labels allowed'),
                    ('sybase', None, None, 'only simple labels allowed'),
                    ('mssql', None, None, 'only simple labels allowed')
                ])


    @property
    def skip_mysql_on_windows(self):
        """Catchall for a large variety of MySQL on Windows failures"""

        return skip_if(self._has_mysql_on_windows,
                "Not supported on MySQL + Windows"
            )

    @property
    def threading_with_mock(self):
        """Mark tests that use threading and mock at the same time - stability
        issues have been observed with coverage + python 3.3

        """
        return skip_if(
                lambda config: util.py3k and
                    config.options.has_coverage,
                "Stability issues with coverage + py3k"
            )

    @property
    def selectone(self):
        """target driver must support the literal statement 'select 1'"""
        return skip_if(["oracle", "firebird"], "non-standard SELECT scalar syntax")


    @property
    def mysql_fully_case_sensitive(self):
        return only_if(self._has_mysql_fully_case_sensitive)

    def _has_mysql_on_windows(self, config):
        return against(config, 'mysql') and \
                config.db.dialect._detect_casing(config.db) == 1

    def _has_mysql_fully_case_sensitive(self, config):
        return against(config, 'mysql') and \
                config.db.dialect._detect_casing(config.db) == 0

