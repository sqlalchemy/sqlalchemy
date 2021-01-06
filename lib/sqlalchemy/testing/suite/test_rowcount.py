from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


class RowCountTest(fixtures.TablesTest):
    """test rowcount functionality"""

    __requires__ = ("sane_rowcount",)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "employees",
            metadata,
            Column(
                "employee_id",
                Integer,
                Sequence("employee_id_seq", optional=True),
                primary_key=True,
            ),
            Column("name", String(50)),
            Column("department", String(1)),
        )

    @classmethod
    def insert_data(cls, connection):
        cls.data = data = [
            ("Angela", "A"),
            ("Andrew", "A"),
            ("Anand", "A"),
            ("Bob", "B"),
            ("Bobette", "B"),
            ("Buffy", "B"),
            ("Charlie", "C"),
            ("Cynthia", "C"),
            ("Chris", "C"),
        ]

        employees_table = cls.tables.employees
        connection.execute(
            employees_table.insert(),
            [{"name": n, "department": d} for n, d in data],
        )

    def test_basic(self, connection):
        employees_table = self.tables.employees
        s = select(
            employees_table.c.name, employees_table.c.department
        ).order_by(employees_table.c.employee_id)
        rows = connection.execute(s).fetchall()

        eq_(rows, self.data)

    def test_update_rowcount1(self, connection):
        employees_table = self.tables.employees

        # WHERE matches 3, 3 rows changed
        department = employees_table.c.department
        r = connection.execute(
            employees_table.update(department == "C"), {"department": "Z"}
        )
        assert r.rowcount == 3

    def test_update_rowcount2(self, connection):
        employees_table = self.tables.employees

        # WHERE matches 3, 0 rows changed
        department = employees_table.c.department

        r = connection.execute(
            employees_table.update(department == "C"), {"department": "C"}
        )
        eq_(r.rowcount, 3)

    @testing.requires.sane_rowcount_w_returning
    def test_update_rowcount_return_defaults(self, connection):
        employees_table = self.tables.employees

        department = employees_table.c.department
        stmt = (
            employees_table.update(department == "C")
            .values(name=employees_table.c.department + "Z")
            .return_defaults()
        )

        r = connection.execute(stmt)
        eq_(r.rowcount, 3)

    def test_raw_sql_rowcount(self, connection):
        # test issue #3622, make sure eager rowcount is called for text
        result = connection.exec_driver_sql(
            "update employees set department='Z' where department='C'"
        )
        eq_(result.rowcount, 3)

    def test_text_rowcount(self, connection):
        # test issue #3622, make sure eager rowcount is called for text
        result = connection.execute(
            text("update employees set department='Z' " "where department='C'")
        )
        eq_(result.rowcount, 3)

    def test_delete_rowcount(self, connection):
        employees_table = self.tables.employees

        # WHERE matches 3, 3 rows deleted
        department = employees_table.c.department
        r = connection.execute(employees_table.delete(department == "C"))
        eq_(r.rowcount, 3)

    @testing.requires.sane_multi_rowcount
    def test_multi_update_rowcount(self, connection):
        employees_table = self.tables.employees
        stmt = (
            employees_table.update()
            .where(employees_table.c.name == bindparam("emp_name"))
            .values(department="C")
        )

        r = connection.execute(
            stmt,
            [
                {"emp_name": "Bob"},
                {"emp_name": "Cynthia"},
                {"emp_name": "nonexistent"},
            ],
        )

        eq_(r.rowcount, 2)

    @testing.requires.sane_multi_rowcount
    def test_multi_delete_rowcount(self, connection):
        employees_table = self.tables.employees

        stmt = employees_table.delete().where(
            employees_table.c.name == bindparam("emp_name")
        )

        r = connection.execute(
            stmt,
            [
                {"emp_name": "Bob"},
                {"emp_name": "Cynthia"},
                {"emp_name": "nonexistent"},
            ],
        )

        eq_(r.rowcount, 2)
