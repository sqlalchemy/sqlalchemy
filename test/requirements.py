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
     LambdaPredicate,\
     requires_tag


def no_support(db, reason):
    return SpecPredicate(db, description=reason)


def exclude(db, op, spec, description=None):
    return SpecPredicate(db, op, spec, description=description)


class DefaultRequirements(SuiteRequirements):
    @property
    def deferrable_or_no_constraints(self):
        """Target database must support deferrable constraints."""

        return skip_if([
            no_support('firebird', 'not supported by database'),
            no_support('mysql', 'not supported by database'),
            no_support('mssql', 'not supported by database'),
            ])

    @property
    def check_constraints(self):
        """Target database must support check constraints."""

        return exclusions.open()

    @property
    def enforces_check_constraints(self):
        """Target database must also enforce check constraints."""

        return self.check_constraints + fails_on(
            ['mysql'], "check constraints don't enforce"
        )

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
    def foreign_key_constraint_option_reflection(self):
        return only_on(['postgresql', 'mysql', 'sqlite'])

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
        return skip_if(["firebird", "mssql+mxodbc"], "not supported by driver")

    @property
    def identity(self):
        """Target database must support GENERATED AS IDENTITY or a facsimile.

        Includes GENERATED AS IDENTITY, AUTOINCREMENT, AUTO_INCREMENT, or other
        column DDL feature that fills in a DB-generated identifier at
        INSERT-time without requiring pre-execution of a SEQUENCE or other
        artifact.

        """
        return skip_if(["firebird", "oracle", "postgresql", "sybase"],
                       "not supported by database")

    @property
    def temporary_tables(self):
        """target database supports temporary tables"""
        return skip_if(
                    ["mssql", "firebird"], "not supported (?)"
                )

    @property
    def temp_table_reflection(self):
        return self.temporary_tables

    @property
    def reflectable_autoincrement(self):
        """Target database must support tables that can automatically generate
        PKs assuming they were reflected.

        this is essentially all the DBs in "identity" plus PostgreSQL, which
        has SERIAL support.  FB and Oracle (and sybase?) require the Sequence
        to be explicitly added, including if the table was reflected.
        """
        return skip_if(["firebird", "oracle", "sybase"],
                       "not supported by database")

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
        return skip_if(["oracle", "mssql"], "not supported by database/driver")

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

        return skip_if(["oracle", "mssql"], "not supported by database/driver")

    @property
    def independent_cursors(self):
        """Target must support simultaneous, independent database cursors
        on a single connection."""

        return skip_if(["mssql+pyodbc", "mssql+mxodbc"], "no driver support")

    @property
    def independent_connections(self):
        """
        Target must support simultaneous, independent database connections.
        """

        # This is also true of some configurations of UnixODBC and probably
        # win32 ODBC as well.
        return skip_if([
            no_support("sqlite",
                       "independent connections disabled "
                       "when :memory: connections are used"),
            exclude("mssql", "<", (9, 0, 0),
                    "SQL Server 2005+ is required for "
                    "independent connections")])

    @property
    def updateable_autoincrement_pks(self):
        """Target must support UPDATE on autoincrement/integer primary key."""

        return skip_if(["mssql", "sybase"],
                       "IDENTITY columns can't be updated")

    @property
    def isolation_level(self):
        return only_on(
            ('postgresql', 'sqlite', 'mysql', 'mssql'),
            "DBAPI has no isolation level support") \
            + fails_on('postgresql+pypostgresql',
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
        return fails_if('mysql',
                        'MySQL error 1093 "Cant specify target table '
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
    def savepoints_w_release(self):
        return self.savepoints + skip_if(
            "oracle", "oracle doesn't support release of savepoint")

    @property
    def schemas(self):
        """Target database must support external schemas, and have one
        named 'test_schema'."""

        return skip_if([
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
    def unique_constraint_reflection_no_index_overlap(self):
        return self.unique_constraint_reflection + skip_if("mysql")

    @property
    def check_constraint_reflection(self):
        return fails_on_everything_except(
                    "postgresql",
                    "sqlite"
                )

    @property
    def temp_table_names(self):
        """target dialect supports listing of temporary table names"""

        return only_on(['sqlite', 'oracle'])

    @property
    def temporary_views(self):
        """target database supports temporary views"""
        return only_on(['sqlite', 'postgresql'])

    @property
    def update_nowait(self):
        """Target database must support SELECT...FOR UPDATE NOWAIT"""
        return skip_if(["firebird", "mssql", "mysql", "sqlite", "sybase"],
                       "no FOR UPDATE NOWAIT support")

    @property
    def subqueries(self):
        """Target database must support subqueries."""

        return skip_if(exclude('mysql', '<', (4, 1, 1)), 'no subquery support')

    @property
    def ctes(self):
        """Target database supports CTEs"""

        return only_if(
            ['postgresql', 'mssql']
        )

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
    def parens_in_union_contained_select_w_limit_offset(self):
        """Target database must support parenthesized SELECT in UNION
        when LIMIT/OFFSET is specifically present.

        E.g. (SELECT ...) UNION (SELECT ..)

        This is known to fail on SQLite.

        """
        return fails_if('sqlite')

    @property
    def parens_in_union_contained_select_wo_limit_offset(self):
        """Target database must support parenthesized SELECT in UNION
        when OFFSET/LIMIT is specifically not present.

        E.g. (SELECT ... LIMIT ..) UNION (SELECT .. OFFSET ..)

        This is known to fail on SQLite.  It also fails on Oracle
        because without LIMIT/OFFSET, there is currently no step that
        creates an additional subquery.

        """
        return fails_if(['sqlite', 'oracle'])

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
                    "postgresql>=8.4", "mssql", "oracle"
                ], "Backend does not support window functions")

    @property
    def two_phase_transactions(self):
        """Target database must support two-phase transactions."""

        return skip_if([
            no_support('firebird', 'no SA implementation'),
            no_support('mssql', 'two-phase xact not supported by drivers'),
            no_support('oracle',
                       'two-phase xact not implemented in SQLA/oracle'),
            no_support('drizzle', 'two-phase xact not supported by database'),
            no_support('sqlite', 'two-phase xact not supported by database'),
            no_support('sybase',
                       'two-phase xact not supported by drivers/SQLA'),
            no_support('postgresql+zxjdbc',
                       'FIXME: JDBC driver confuses the transaction state, '
                       'may need separate XA implementation'),
            no_support('mysql',
                       'recent MySQL communiity editions have too many issues '
                       '(late 2016), disabling for now')])

    @property
    def two_phase_recovery(self):
        return self.two_phase_transactions + (
            skip_if(
               "mysql",
               "crashes on most mariadb and mysql versions"
            )
        )

    @property
    def views(self):
        """Target database must support VIEWs."""

        return skip_if("drizzle", "no VIEW support")

    @property
    def empty_strings_varchar(self):
        """
        target database can persist/return an empty string with a varchar.
        """

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
        """
        Target driver must support some encoding of Unicode across the wire.
        """
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
                                          'sqlite+pysqlcipher',
                                          'sybase',
                                          'mssql')

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
                                       'sqlite+pysqlite',
                                       'sqlite+pysqlcipher')

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
                                          'sybase', 'sqlite')

    @property
    def array_type(self):
        return only_on([
            lambda config: against(config, "postgresql") and
            not against(config, "+pg8000") and not against(config, "+zxjdbc")
        ])

    @property
    def json_type(self):
        return only_on([
            lambda config: against(config, "mysql >= 5.7") and
            not config.db.dialect._is_mariadb,
            "postgresql >= 9.3"
        ])

    @property
    def json_array_indexes(self):
        return self.json_type + fails_if("+pg8000")

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
             ("firebird", None, None, "Precision must be from 1 to 18")])

    @property
    def precision_numerics_many_significant_digits(self):
        """target backend supports values with many digits on both sides,
        such as 319438950232418390.273596, 87673.594069654243

        """
        return fails_if(
            [('sqlite', None, None, 'TODO'),
             ("firebird", None, None, "Precision must be from 1 to 18"),
             ("sybase+pysybase", None, None, "TODO"),
             ('mssql+pymssql', None, None,
              'FIXME: improve pymssql dec handling')]
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
             'only four decimal places '),
            ('mssql+pymssql', None, None,
             'mssql+pymssql has FP inaccuracy even with '
             'only four decimal places '),
            ('postgresql+pg8000', None, None,
             'postgresql+pg8000 has FP inaccuracy even with '
             'only four decimal places '),
            ('postgresql+psycopg2cffi', None, None,
             'postgresql+psycopg2cffi has FP inaccuracy even with '
             'only four decimal places ')])

    @property
    def fetch_null_from_numeric(self):
        return skip_if(
                    ("mssql+pyodbc", None, None, "crashes due to bug #351"),
                )

    @property
    def duplicate_key_raises_integrity_error(self):
        return fails_on("postgresql+pg8000")

    @property
    def hstore(self):
        def check_hstore(config):
            if not against(config, "postgresql"):
                return False
            try:
                config.db.execute("SELECT 'a=>1,a=>2'::hstore;")
                return True
            except Exception:
                return False

        return only_if(check_hstore)

    @property
    def range_types(self):
        def check_range_types(config):
            if not against(
                    config,
                    ["postgresql+psycopg2", "postgresql+psycopg2cffi"]):
                return False
            try:
                config.db.scalar("select '[1,2)'::int4range;")
                return True
            except Exception:
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
    def postgresql_test_dblink(self):
        return skip_if(
                    lambda config: not config.file_config.has_option(
                        'sqla_testing', 'postgres_test_db_link'),
                    "postgres_test_db_link option not specified in config"
                )

    @property
    def postgresql_jsonb(self):
        return only_on("postgresql >= 9.4") + skip_if(
            lambda config:
            config.db.dialect.driver == "pg8000" and
            config.db.dialect._dbapi_version <= (1, 10, 1)
        )

    @property
    def psycopg2_native_json(self):
        return self.psycopg2_compatibility

    @property
    def psycopg2_native_hstore(self):
        return self.psycopg2_compatibility

    @property
    def psycopg2_compatibility(self):
        return only_on(
            ["postgresql+psycopg2", "postgresql+psycopg2cffi"]
        )

    @property
    def psycopg2_or_pg8000_compatibility(self):
        return only_on(
            ["postgresql+psycopg2", "postgresql+psycopg2cffi",
             "postgresql+pg8000"]
        )

    @property
    def percent_schema_names(self):
        return skip_if(
            [
                (
                    "+psycopg2", None, None,
                    "psycopg2 2.4 no longer accepts percent "
                    "sign in bind placeholders"),
                (
                    "+psycopg2cffi", None, None,
                    "psycopg2cffi does not accept percent signs in "
                    "bind placeholders"),
                ("mysql", None, None, "executemany() doesn't work here")
            ]
        )

    @property
    def order_by_label_with_expression(self):
        return fails_if([
            ('firebird', None, None,
             "kinterbasdb doesn't send full type information"),
            ('postgresql', None, None, 'only simple labels allowed'),
            ('sybase', None, None, 'only simple labels allowed'),
            ('mssql', None, None, 'only simple labels allowed')
        ])

    @property
    def skip_mysql_on_windows(self):
        """Catchall for a large variety of MySQL on Windows failures"""

        return skip_if(self._has_mysql_on_windows,
                       "Not supported on MySQL + Windows")

    @property
    def mssql_freetds(self):
        return only_on(
            LambdaPredicate(
                lambda config: (
                    (against(config, 'mssql+pyodbc') and
                     config.db.dialect.freetds)
                    or against(config, 'mssql+pymssql')
                )
            )
        )

    @property
    def ad_hoc_engines(self):
        return exclusions.skip_if(["oracle"])

    @property
    def no_mssql_freetds(self):
        return self.mssql_freetds.not_()

    @property
    def selectone(self):
        """target driver must support the literal statement 'select 1'"""
        return skip_if(["oracle", "firebird"],
                       "non-standard SELECT scalar syntax")

    @property
    def mysql_fsp(self):
        return only_if('mysql >= 5.6.4')

    @property
    def mysql_fully_case_sensitive(self):
        return only_if(self._has_mysql_fully_case_sensitive)

    @property
    def mysql_zero_date(self):
        def check(config):
            row = config.db.execute("show variables like 'sql_mode'").first()
            return not row or "NO_ZERO_DATE" not in row[1]

        return only_if(check)

    @property
    def mysql_non_strict(self):
        def check(config):
            row = config.db.execute("show variables like 'sql_mode'").first()
            return not row or "STRICT" not in row[1]

        return only_if(check)

    def _has_mysql_on_windows(self, config):
        return against(config, 'mysql') and \
                config.db.dialect._detect_casing(config.db) == 1

    def _has_mysql_fully_case_sensitive(self, config):
        return against(config, 'mysql') and \
                config.db.dialect._detect_casing(config.db) == 0

    @property
    def postgresql_utf8_server_encoding(self):
        return only_if(
            lambda config: against(config, 'postgresql') and
            config.db.scalar("show server_encoding").lower() == "utf8"
        )
