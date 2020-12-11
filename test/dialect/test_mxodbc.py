from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.mock import Mock


def mock_dbapi():
    return Mock(
        paramstyle="qmark",
        connect=Mock(
            return_value=Mock(
                cursor=Mock(return_value=Mock(description=None, rowcount=None))
            )
        ),
    )


class MxODBCTest(fixtures.TestBase):
    def test_native_odbc_execute(self):
        t1 = Table("t1", MetaData(), Column("c1", Integer))
        dbapi = mock_dbapi()

        engine = engines.testing_engine(
            "mssql+mxodbc://localhost",
            options={"module": dbapi, "_initialize": False},
        )
        conn = engine.connect()

        with conn.begin():
            # crud: uses execute
            conn.execute(t1.insert().values(c1="foo"))
            conn.execute(t1.delete().where(t1.c.c1 == "foo"))
            conn.execute(t1.update().where(t1.c.c1 == "foo").values(c1="bar"))

            # select: uses executedirect
            conn.execute(t1.select())

            # manual flagging
            conn.execution_options(native_odbc_execute=True).execute(
                t1.select()
            )
            conn.execution_options(native_odbc_execute=False).execute(
                t1.insert().values(c1="foo")
            )

            eq_(
                # fmt: off
                [
                    c[2]
                    for c in dbapi.connect.return_value.cursor.
                    return_value.execute.mock_calls
                ],
                # fmt: on
                [
                    {"direct": True},
                    {"direct": True},
                    {"direct": True},
                    {"direct": True},
                    {"direct": False},
                    {"direct": True},
                ]
            )
