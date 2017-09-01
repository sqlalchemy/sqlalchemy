from sqlalchemy import *
from sqlalchemy.testing import fixtures, AssertsExecutionResults
from sqlalchemy import testing
from sqlalchemy.testing import eq_


class FoundRowsTest(fixtures.TestBase, AssertsExecutionResults):

    """tests rowcount functionality"""

    __requires__ = ('sane_rowcount', )
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global employees_table, metadata
        metadata = MetaData(testing.db)

        employees_table = Table(
            'employees', metadata,
            Column(
                'employee_id', Integer,
                Sequence('employee_id_seq', optional=True), primary_key=True),
            Column('name', String(50)),
            Column('department', String(1)))
        metadata.create_all()

    def setup(self):
        global data
        data = [('Angela', 'A'),
                ('Andrew', 'A'),
                ('Anand', 'A'),
                ('Bob', 'B'),
                ('Bobette', 'B'),
                ('Buffy', 'B'),
                ('Charlie', 'C'),
                ('Cynthia', 'C'),
                ('Chris', 'C')]

        i = employees_table.insert()
        i.execute(*[{'name': n, 'department': d} for n, d in data])

    def teardown(self):
        employees_table.delete().execute()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_basic(self):
        s = employees_table.select()
        r = s.execute().fetchall()

        assert len(r) == len(data)

    def test_update_rowcount1(self):
        # WHERE matches 3, 3 rows changed
        department = employees_table.c.department
        r = employees_table.update(department == 'C').execute(department='Z')
        assert r.rowcount == 3

    def test_update_rowcount2(self):
        # WHERE matches 3, 0 rows changed
        department = employees_table.c.department
        r = employees_table.update(department == 'C').execute(department='C')
        assert r.rowcount == 3

    @testing.skip_if(
        testing.requires.oracle5x,
        "unknown DBAPI error fixed in later version")
    @testing.requires.sane_rowcount_w_returning
    def test_update_rowcount_return_defaults(self):
        department = employees_table.c.department
        stmt = employees_table.update(department == 'C').values(
            name=employees_table.c.department + 'Z').return_defaults()

        r = stmt.execute()
        assert r.rowcount == 3

    def test_raw_sql_rowcount(self):
        # test issue #3622, make sure eager rowcount is called for text
        with testing.db.connect() as conn:
            result = conn.execute(
                "update employees set department='Z' where department='C'")
            eq_(result.rowcount, 3)

    def test_text_rowcount(self):
        # test issue #3622, make sure eager rowcount is called for text
        with testing.db.connect() as conn:
            result = conn.execute(
                text(
                    "update employees set department='Z' "
                    "where department='C'"))
            eq_(result.rowcount, 3)

    def test_delete_rowcount(self):
        # WHERE matches 3, 3 rows deleted
        department = employees_table.c.department
        r = employees_table.delete(department == 'C').execute()
        assert r.rowcount == 3

    @testing.requires.sane_multi_rowcount
    def test_multi_update_rowcount(self):
        stmt = employees_table.update().\
            where(employees_table.c.name == bindparam('emp_name')).\
            values(department="C")

        r = testing.db.execute(
            stmt,
            [{"emp_name": "Bob"}, {"emp_name": "Cynthia"},
             {"emp_name": "nonexistent"}]
        )

        eq_(
            r.rowcount, 2
        )

    @testing.requires.sane_multi_rowcount
    def test_multi_delete_rowcount(self):
        stmt = employees_table.delete().\
            where(employees_table.c.name == bindparam('emp_name'))

        r = testing.db.execute(
            stmt,
            [{"emp_name": "Bob"}, {"emp_name": "Cynthia"},
             {"emp_name": "nonexistent"}]
        )

        eq_(
            r.rowcount, 2
        )

