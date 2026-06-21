import sqlalchemy.dialects.mssql.provision as mssql_provision
import sqlalchemy.dialects.mysql.provision as mysql_provision
import sqlalchemy.dialects.oracle.provision as oracle_provision
import sqlalchemy.dialects.postgresql.provision as postgresql_provision
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.provision import create_db
from sqlalchemy.testing.provision import drop_db
from sqlalchemy.testing.provision import validate_follower_ident

_PROVISION_MODULES = (
    mssql_provision,
    mysql_provision,
    oracle_provision,
    postgresql_provision,
)


class CaptureConnection:
    def __init__(self):
        self.sql = []

    def __enter__(self):
        return self

    def __exit__(self, *arg):
        return False

    def begin(self):
        return self

    def execution_options(self, **kw):
        return self

    def exec_driver_sql(self, statement, *arg, **kw):
        self.sql.append(statement)


class CaptureEngine:
    def __init__(self):
        self.conn = CaptureConnection()

    def begin(self):
        return self.conn

    def connect(self):
        return self.conn

    def execution_options(self, **kw):
        return self


class ProvisionFollowerIdentTest(fixtures.TestBase):
    def test_validate_follower_ident(self):
        eq_(
            validate_follower_ident("test_012345abcdef"),
            "test_012345abcdef",
        )
        eq_(
            validate_follower_ident(
                "test_012345abcdef_test_schema", include_related=True
            ),
            "test_012345abcdef_test_schema",
        )

    def test_validate_follower_ident_rejects_unexpected_text(self):
        assert_raises_message(
            ValueError,
            "Unsafe SQLAlchemy test follower identifier",
            validate_follower_ident,
            "test_012345abcdef; SELECT 1; --",
        )

        assert_raises_message(
            ValueError,
            "Unsafe SQLAlchemy test follower identifier",
            validate_follower_ident,
            "test_012345abcdef_test_schema",
        )

    def test_valid_mysql_create_still_emits_sql(self):
        engine = CaptureEngine()

        create_db.fns["mysql"](None, engine, "test_012345abcdef")

        eq_(
            engine.conn.sql[-3:],
            [
                "CREATE DATABASE test_012345abcdef CHARACTER SET utf8mb4",
                "CREATE DATABASE test_012345abcdef_test_schema "
                "CHARACTER SET utf8mb4",
                "CREATE DATABASE test_012345abcdef_test_schema_2 "
                "CHARACTER SET utf8mb4",
            ],
        )

    def test_create_db_rejects_unsafe_ident_before_sql(self):
        unsafe_ident = "test_012345abcdef; SELECT 1; --"

        for backend in ("mssql", "mysql", "oracle", "postgresql"):
            engine = CaptureEngine()

            assert_raises_message(
                ValueError,
                "Unsafe SQLAlchemy test follower identifier",
                create_db.fns[backend],
                None,
                engine,
                unsafe_ident,
            )
            eq_(engine.conn.sql, [])

    def test_drop_db_rejects_unsafe_ident_before_sql(self):
        unsafe_ident = "test_012345abcdef; SELECT 1; --"

        for backend in ("mssql", "mysql", "oracle", "postgresql"):
            engine = CaptureEngine()

            assert_raises_message(
                ValueError,
                "Unsafe SQLAlchemy test follower identifier",
                drop_db.fns[backend],
                None,
                engine,
                unsafe_ident,
            )
            eq_(engine.conn.sql, [])
