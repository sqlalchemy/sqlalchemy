from sqlalchemy.testing import assert_raises, assert_raises_message
import datetime
import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy import Integer, String, ForeignKey, MetaData, and_, \
    select, func
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, relation, \
    backref, create_session, configure_mappers, \
    clear_mappers, sessionmaker, attributes,\
    Session, composite, column_property, foreign,\
    remote, synonym, joinedload, subqueryload
from sqlalchemy.orm.interfaces import ONETOMANY, MANYTOONE
from sqlalchemy.testing import eq_, startswith_, AssertsCompiledSQL, is_
from sqlalchemy.testing import fixtures
from test.orm import _fixtures
from sqlalchemy import exc
from sqlalchemy import inspect


class _RelationshipErrors(object):

    def _assert_raises_no_relevant_fks(self, fn, expr, relname,
                                       primary, *arg, **kw):
        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not locate any relevant foreign key columns "
            "for %s join condition '%s' on relationship %s.  "
            "Ensure that referencing columns are associated with "
            "a ForeignKey or ForeignKeyConstraint, or are annotated "
            r"in the join condition with the foreign\(\) annotation."
            % (
                primary, expr, relname
            ),
            fn, *arg, **kw
        )

    def _assert_raises_no_equality(self, fn, expr, relname,
                                   primary, *arg, **kw):
        assert_raises_message(
            sa.exc.ArgumentError,
            "Could not locate any simple equality expressions "
            "involving locally mapped foreign key columns for %s join "
            "condition '%s' on relationship %s.  "
            "Ensure that referencing columns are associated with a "
            "ForeignKey or ForeignKeyConstraint, or are annotated in "
            r"the join condition with the foreign\(\) annotation. "
            "To allow comparison operators other than '==', "
            "the relationship can be marked as viewonly=True." % (
                primary, expr, relname
            ),
            fn, *arg, **kw
        )

    def _assert_raises_ambig_join(self, fn, relname, secondary_arg,
                                  *arg, **kw):
        if secondary_arg is not None:
            assert_raises_message(
                exc.ArgumentError,
                "Could not determine join condition between "
                "parent/child tables on relationship %s - "
                "there are multiple foreign key paths linking the "
                "tables via secondary table '%s'.  "
                "Specify the 'foreign_keys' argument, providing a list "
                "of those columns which should be counted as "
                "containing a foreign key reference from the "
                "secondary table to each of the parent and child tables."
                % (relname, secondary_arg),
                fn, *arg, **kw)
        else:
            assert_raises_message(
                exc.ArgumentError,
                "Could not determine join "
                "condition between parent/child tables on "
                "relationship %s - there are multiple foreign key "
                "paths linking the tables.  Specify the "
                "'foreign_keys' argument, providing a list of those "
                "columns which should be counted as containing a "
                "foreign key reference to the parent table."
                % (relname,),
                fn, *arg, **kw)

    def _assert_raises_no_join(self, fn, relname, secondary_arg,
                               *arg, **kw):
        if secondary_arg is not None:
            assert_raises_message(
                exc.NoForeignKeysError,
                "Could not determine join condition between "
                "parent/child tables on relationship %s - "
                "there are no foreign keys linking these tables "
                "via secondary table '%s'.  "
                "Ensure that referencing columns are associated with a "
                "ForeignKey "
                "or ForeignKeyConstraint, or specify 'primaryjoin' and "
                "'secondaryjoin' expressions"
                % (relname, secondary_arg),
                fn, *arg, **kw)
        else:
            assert_raises_message(
                exc.NoForeignKeysError,
                "Could not determine join condition between "
                "parent/child tables on relationship %s - "
                "there are no foreign keys linking these tables.  "
                "Ensure that referencing columns are associated with a "
                "ForeignKey "
                "or ForeignKeyConstraint, or specify a 'primaryjoin' "
                "expression."
                % (relname,),
                fn, *arg, **kw)

    def _assert_raises_ambiguous_direction(self, fn, relname, *arg, **kw):
        assert_raises_message(
            sa.exc.ArgumentError,
            "Can't determine relationship"
            " direction for relationship '%s' - foreign "
            "key columns within the join condition are present "
            "in both the parent and the child's mapped tables.  "
            "Ensure that only those columns referring to a parent column "
            r"are marked as foreign, either via the foreign\(\) annotation or "
            "via the foreign_keys argument."
            % relname,
            fn, *arg, **kw
        )

    def _assert_raises_no_local_remote(self, fn, relname, *arg, **kw):
        assert_raises_message(
            sa.exc.ArgumentError,
            "Relationship %s could not determine "
            "any unambiguous local/remote column "
            "pairs based on join condition and remote_side arguments.  "
            r"Consider using the remote\(\) annotation to "
            "accurately mark those elements of the join "
            "condition that are on the remote side of the relationship." % (
                relname
            ),

            fn, *arg, **kw
        )


class DependencyTwoParentTest(fixtures.MappedTest):

    """Test flush() when a mapper is dependent on multiple relationships"""

    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table("tbl_a", metadata,
              Column("id", Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column("name", String(128)))
        Table("tbl_b", metadata,
              Column("id", Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column("name", String(128)))
        Table("tbl_c", metadata,
              Column("id", Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column("tbl_a_id", Integer, ForeignKey("tbl_a.id"),
                     nullable=False),
              Column("name", String(128)))
        Table("tbl_d", metadata,
              Column("id", Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column("tbl_c_id", Integer, ForeignKey("tbl_c.id"),
                     nullable=False),
              Column("tbl_b_id", Integer, ForeignKey("tbl_b.id")),
              Column("name", String(128)))

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

        class C(cls.Basic):
            pass

        class D(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        A, C, B, D, tbl_b, tbl_c, tbl_a, tbl_d = (cls.classes.A,
                                                  cls.classes.C,
                                                  cls.classes.B,
                                                  cls.classes.D,
                                                  cls.tables.tbl_b,
                                                  cls.tables.tbl_c,
                                                  cls.tables.tbl_a,
                                                  cls.tables.tbl_d)

        mapper(A, tbl_a, properties=dict(
            c_rows=relationship(C, cascade="all, delete-orphan",
                                backref="a_row")))
        mapper(B, tbl_b)
        mapper(C, tbl_c, properties=dict(
            d_rows=relationship(D, cascade="all, delete-orphan",
                                backref="c_row")))
        mapper(D, tbl_d, properties=dict(
            b_row=relationship(B)))

    @classmethod
    def insert_data(cls):
        A, C, B, D = (cls.classes.A,
                      cls.classes.C,
                      cls.classes.B,
                      cls.classes.D)

        session = create_session()
        a = A(name='a1')
        b = B(name='b1')
        c = C(name='c1', a_row=a)

        d1 = D(name='d1', b_row=b, c_row=c)  # noqa
        d2 = D(name='d2', b_row=b, c_row=c)  # noqa
        d3 = D(name='d3', b_row=b, c_row=c)  # noqa
        session.add(a)
        session.add(b)
        session.flush()

    def test_DeleteRootTable(self):
        A = self.classes.A

        session = create_session()
        a = session.query(A).filter_by(name='a1').one()

        session.delete(a)
        session.flush()

    def test_DeleteMiddleTable(self):
        C = self.classes.C

        session = create_session()
        c = session.query(C).filter_by(name='c1').one()

        session.delete(c)
        session.flush()


class M2ODontOverwriteFKTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'a', metadata,
            Column('id', Integer, primary_key=True),
            Column('bid', ForeignKey('b.id'))
        )
        Table(
            'b', metadata,
            Column('id', Integer, primary_key=True),
        )

    def _fixture(self, uselist=False):
        a, b = self.tables.a, self.tables.b

        class A(fixtures.BasicEntity):
            pass

        class B(fixtures.BasicEntity):
            pass

        mapper(A, a, properties={
            'b': relationship(B, uselist=uselist)
        })
        mapper(B, b)
        return A, B

    def test_joinedload_doesnt_produce_bogus_event(self):
        A, B = self._fixture()
        sess = Session()

        b1 = B()
        sess.add(b1)
        sess.flush()

        a1 = A()
        sess.add(a1)
        sess.commit()

        # test that was broken by #3060
        a1 = sess.query(A).options(joinedload("b")).first()
        a1.bid = b1.id
        sess.flush()

        eq_(a1.bid, b1.id)

    def test_init_doesnt_produce_scalar_event(self):
        A, B = self._fixture()
        sess = Session()

        b1 = B()
        sess.add(b1)
        sess.flush()

        a1 = A()
        assert a1.b is None
        a1.bid = b1.id
        sess.add(a1)
        sess.flush()
        assert a1.bid is not None

    def test_init_doesnt_produce_collection_event(self):
        A, B = self._fixture(uselist=True)
        sess = Session()

        b1 = B()
        sess.add(b1)
        sess.flush()

        a1 = A()
        assert a1.b == []
        a1.bid = b1.id
        sess.add(a1)
        sess.flush()
        assert a1.bid is not None

    def test_scalar_relationship_overrides_fk(self):
        A, B = self._fixture()
        sess = Session()

        b1 = B()
        sess.add(b1)
        sess.flush()

        a1 = A()
        a1.bid = b1.id
        a1.b = None
        sess.add(a1)
        sess.flush()
        assert a1.bid is None

    def test_collection_relationship_overrides_fk(self):
        A, B = self._fixture(uselist=True)
        sess = Session()

        b1 = B()
        sess.add(b1)
        sess.flush()

        a1 = A()
        a1.bid = b1.id
        a1.b = []
        sess.add(a1)
        sess.flush()
        # this is weird
        assert a1.bid is not None


class DirectSelfRefFKTest(fixtures.MappedTest, AssertsCompiledSQL):

    """Tests the ultimate join condition, a single column
    that points to itself, e.g. within a SQL function or similar.
    The test is against a materialized path setup.

    this is an **extremely** unusual case::

    Entity
    ------
     path -------+
       ^         |
       +---------+

    In this case, one-to-many and many-to-one are no longer accurate.
    Both relationships return collections.   I'm not sure if this is a good
    idea.

    """

    __dialect__ = 'default'

    @classmethod
    def define_tables(cls, metadata):
        Table('entity', metadata,
              Column('path', String(100), primary_key=True)
              )

    @classmethod
    def setup_classes(cls):
        class Entity(cls.Basic):

            def __init__(self, path):
                self.path = path

    def _descendants_fixture(self, data=True):
        Entity = self.classes.Entity
        entity = self.tables.entity

        m = mapper(Entity, entity, properties={
            "descendants": relationship(
                Entity,
                primaryjoin=remote(foreign(entity.c.path)).like(
                    entity.c.path.concat('/%')),
                viewonly=True,
                order_by=entity.c.path)
        })
        configure_mappers()
        assert m.get_property("descendants").direction is ONETOMANY
        if data:
            return self._fixture()

    def _anscestors_fixture(self, data=True):
        Entity = self.classes.Entity
        entity = self.tables.entity

        m = mapper(Entity, entity, properties={
            "anscestors": relationship(
                Entity,
                primaryjoin=entity.c.path.like(
                    remote(foreign(entity.c.path)).concat('/%')),
                viewonly=True,
                order_by=entity.c.path)
        })
        configure_mappers()
        assert m.get_property("anscestors").direction is ONETOMANY
        if data:
            return self._fixture()

    def _fixture(self):
        Entity = self.classes.Entity
        sess = Session()
        sess.add_all([
            Entity("/foo"),
            Entity("/foo/bar1"),
            Entity("/foo/bar2"),
            Entity("/foo/bar2/bat1"),
            Entity("/foo/bar2/bat2"),
            Entity("/foo/bar3"),
            Entity("/bar"),
            Entity("/bar/bat1")
        ])
        return sess

    def test_descendants_lazyload_clause(self):
        self._descendants_fixture(data=False)
        Entity = self.classes.Entity
        self.assert_compile(
            Entity.descendants.property.strategy._lazywhere,
            "entity.path LIKE (:param_1 || :path_1)"
        )

        self.assert_compile(
            Entity.descendants.property.strategy._rev_lazywhere,
            ":param_1 LIKE (entity.path || :path_1)"
        )

    def test_ancestors_lazyload_clause(self):
        self._anscestors_fixture(data=False)
        Entity = self.classes.Entity
        # :param_1 LIKE (:param_1 || :path_1)
        self.assert_compile(
            Entity.anscestors.property.strategy._lazywhere,
            ":param_1 LIKE (entity.path || :path_1)"
        )

        self.assert_compile(
            Entity.anscestors.property.strategy._rev_lazywhere,
            "entity.path LIKE (:param_1 || :path_1)"
        )

    def test_descendants_lazyload(self):
        sess = self._descendants_fixture()
        Entity = self.classes.Entity
        e1 = sess.query(Entity).filter_by(path="/foo").first()
        eq_(
            [e.path for e in e1.descendants],
            ["/foo/bar1", "/foo/bar2", "/foo/bar2/bat1",
                "/foo/bar2/bat2", "/foo/bar3"]
        )

    def test_anscestors_lazyload(self):
        sess = self._anscestors_fixture()
        Entity = self.classes.Entity
        e1 = sess.query(Entity).filter_by(path="/foo/bar2/bat1").first()
        eq_(
            [e.path for e in e1.anscestors],
            ["/foo", "/foo/bar2"]
        )

    def test_descendants_joinedload(self):
        sess = self._descendants_fixture()
        Entity = self.classes.Entity
        e1 = sess.query(Entity).filter_by(path="/foo").\
            options(joinedload(Entity.descendants)).first()

        eq_(
            [e.path for e in e1.descendants],
            ["/foo/bar1", "/foo/bar2", "/foo/bar2/bat1",
                "/foo/bar2/bat2", "/foo/bar3"]
        )

    def test_descendants_subqueryload(self):
        sess = self._descendants_fixture()
        Entity = self.classes.Entity
        e1 = sess.query(Entity).filter_by(path="/foo").\
            options(subqueryload(Entity.descendants)).first()

        eq_(
            [e.path for e in e1.descendants],
            ["/foo/bar1", "/foo/bar2", "/foo/bar2/bat1",
                "/foo/bar2/bat2", "/foo/bar3"]
        )

    def test_anscestors_joinedload(self):
        sess = self._anscestors_fixture()
        Entity = self.classes.Entity
        e1 = sess.query(Entity).filter_by(path="/foo/bar2/bat1").\
            options(joinedload(Entity.anscestors)).first()
        eq_(
            [e.path for e in e1.anscestors],
            ["/foo", "/foo/bar2"]
        )

    def test_plain_join_descendants(self):
        self._descendants_fixture(data=False)
        Entity = self.classes.Entity
        sess = Session()
        self.assert_compile(
            sess.query(Entity).join(Entity.descendants, aliased=True),
            "SELECT entity.path AS entity_path FROM entity JOIN entity AS "
            "entity_1 ON entity_1.path LIKE (entity.path || :path_1)"
        )


class CompositeSelfRefFKTest(fixtures.MappedTest, AssertsCompiledSQL):

    """Tests a composite FK where, in
    the relationship(), one col points
    to itself in the same table.

    this is a very unusual case::

    company         employee
    ----------      ----------
    company_id <--- company_id ------+
    name                ^            |
                        +------------+

                    emp_id <---------+
                    name             |
                    reports_to_id ---+

    employee joins to its sub-employees
    both on reports_to_id, *and on company_id to itself*.

    """

    __dialect__ = 'default'

    @classmethod
    def define_tables(cls, metadata):
        Table('company_t', metadata,
              Column('company_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(30)))

        Table('employee_t', metadata,
              Column('company_id', Integer, primary_key=True),
              Column('emp_id', Integer, primary_key=True),
              Column('name', String(30)),
              Column('reports_to_id', Integer),
              sa.ForeignKeyConstraint(
                  ['company_id'],
                  ['company_t.company_id']),
              sa.ForeignKeyConstraint(
                  ['company_id', 'reports_to_id'],
                  ['employee_t.company_id', 'employee_t.emp_id']))

    @classmethod
    def setup_classes(cls):
        class Company(cls.Basic):

            def __init__(self, name):
                self.name = name

        class Employee(cls.Basic):

            def __init__(self, name, company, emp_id, reports_to=None):
                self.name = name
                self.company = company
                self.emp_id = emp_id
                self.reports_to = reports_to

    def test_explicit(self):
        Employee, Company, employee_t, company_t = (self.classes.Employee,
                                                    self.classes.Company,
                                                    self.tables.employee_t,
                                                    self.tables.company_t)

        mapper(Company, company_t)
        mapper(Employee, employee_t, properties={
            'company': relationship(Company,
                                    primaryjoin=employee_t.c.company_id ==
                                    company_t.c.company_id,
                                    backref='employees'),
            'reports_to': relationship(Employee, primaryjoin=sa.and_(
                employee_t.c.emp_id == employee_t.c.reports_to_id,
                employee_t.c.company_id == employee_t.c.company_id
            ),
                remote_side=[employee_t.c.emp_id, employee_t.c.company_id],
                foreign_keys=[
                    employee_t.c.reports_to_id, employee_t.c.company_id],
                backref=backref('employees',
                                foreign_keys=[employee_t.c.reports_to_id,
                                              employee_t.c.company_id]))
        })

        self._test()

    def test_implicit(self):
        Employee, Company, employee_t, company_t = (self.classes.Employee,
                                                    self.classes.Company,
                                                    self.tables.employee_t,
                                                    self.tables.company_t)

        mapper(Company, company_t)
        mapper(Employee, employee_t, properties={
            'company': relationship(Company, backref='employees'),
            'reports_to': relationship(
                Employee,
                remote_side=[employee_t.c.emp_id, employee_t.c.company_id],
                foreign_keys=[employee_t.c.reports_to_id,
                              employee_t.c.company_id],
                backref=backref(
                    'employees',
                    foreign_keys=[
                        employee_t.c.reports_to_id, employee_t.c.company_id])
            )
        })

        self._test()

    def test_very_implicit(self):
        Employee, Company, employee_t, company_t = (self.classes.Employee,
                                                    self.classes.Company,
                                                    self.tables.employee_t,
                                                    self.tables.company_t)

        mapper(Company, company_t)
        mapper(Employee, employee_t, properties={
            'company': relationship(Company, backref='employees'),
            'reports_to': relationship(
                Employee,
                remote_side=[employee_t.c.emp_id, employee_t.c.company_id],
                backref='employees'
            )
        })

        self._test()

    def test_very_explicit(self):
        Employee, Company, employee_t, company_t = (self.classes.Employee,
                                                    self.classes.Company,
                                                    self.tables.employee_t,
                                                    self.tables.company_t)

        mapper(Company, company_t)
        mapper(Employee, employee_t, properties={
            'company': relationship(Company, backref='employees'),
            'reports_to': relationship(
                Employee,
                _local_remote_pairs=[
                    (employee_t.c.reports_to_id, employee_t.c.emp_id),
                    (employee_t.c.company_id, employee_t.c.company_id)
                ],
                foreign_keys=[
                    employee_t.c.reports_to_id,
                    employee_t.c.company_id],
                backref=backref(
                    'employees',
                    foreign_keys=[
                        employee_t.c.reports_to_id, employee_t.c.company_id])
            )
        })

        self._test()

    def test_annotated(self):
        Employee, Company, employee_t, company_t = (self.classes.Employee,
                                                    self.classes.Company,
                                                    self.tables.employee_t,
                                                    self.tables.company_t)

        mapper(Company, company_t)
        mapper(Employee, employee_t, properties={
            'company': relationship(Company, backref='employees'),
            'reports_to': relationship(
                Employee,
                primaryjoin=sa.and_(
                    remote(employee_t.c.emp_id) == employee_t.c.reports_to_id,
                    remote(employee_t.c.company_id) == employee_t.c.company_id
                ),
                backref=backref('employees')
            )
        })

        self._assert_lazy_clauses()
        self._test()

    def test_overlapping_warning(self):
        Employee, Company, employee_t, company_t = (self.classes.Employee,
                                                    self.classes.Company,
                                                    self.tables.employee_t,
                                                    self.tables.company_t)

        mapper(Company, company_t)
        mapper(Employee, employee_t, properties={
            'company': relationship(Company, backref='employees'),
            'reports_to': relationship(
                Employee,
                primaryjoin=sa.and_(
                    remote(employee_t.c.emp_id) == employee_t.c.reports_to_id,
                    remote(employee_t.c.company_id) == employee_t.c.company_id
                ),
                backref=backref('employees')
            )
        })

        assert_raises_message(
            exc.SAWarning,
            r"relationship .* will copy column .* to column "
            "employee_t.company_id, which conflicts with relationship\(s\)",
            configure_mappers
        )

    def test_annotated_no_overwriting(self):
        Employee, Company, employee_t, company_t = (self.classes.Employee,
                                                    self.classes.Company,
                                                    self.tables.employee_t,
                                                    self.tables.company_t)

        mapper(Company, company_t)
        mapper(Employee, employee_t, properties={
            'company': relationship(Company, backref='employees'),
            'reports_to': relationship(
                Employee,
                primaryjoin=sa.and_(
                    remote(employee_t.c.emp_id) ==
                    foreign(employee_t.c.reports_to_id),
                    remote(employee_t.c.company_id) == employee_t.c.company_id
                ),
                backref=backref('employees')
            )
        })

        self._assert_lazy_clauses()
        self._test_no_warning()

    def _test_no_overwrite(self, sess, expect_failure):
        # test [ticket:3230]

        Employee, Company = self.classes.Employee, self.classes.Company

        c1 = sess.query(Company).filter_by(name='c1').one()
        e3 = sess.query(Employee).filter_by(name='emp3').one()
        e3.reports_to = None

        if expect_failure:
            # if foreign() isn't applied specifically to
            # employee_t.c.reports_to_id only, then
            # employee_t.c.company_id goes foreign as well and then
            # this happens
            assert_raises_message(
                AssertionError,
                "Dependency rule tried to blank-out primary key column "
                "'employee_t.company_id'",
                sess.flush
            )
        else:
            sess.flush()
            eq_(e3.company, c1)

    @testing.emits_warning("relationship .* will copy column ")
    def _test(self):
        self._test_no_warning(overwrites=True)

    def _test_no_warning(self, overwrites=False):
        configure_mappers()
        self._test_relationships()
        sess = Session()
        self._setup_data(sess)
        self._test_lazy_relations(sess)
        self._test_join_aliasing(sess)
        self._test_no_overwrite(sess, expect_failure=overwrites)

    @testing.emits_warning("relationship .* will copy column ")
    def _assert_lazy_clauses(self):
        configure_mappers()
        Employee = self.classes.Employee
        self.assert_compile(
            Employee.employees.property.strategy._lazywhere,
            ":param_1 = employee_t.reports_to_id AND "
            ":param_2 = employee_t.company_id"
        )

        self.assert_compile(
            Employee.employees.property.strategy._rev_lazywhere,
            "employee_t.emp_id = :param_1 AND "
            "employee_t.company_id = :param_2"
        )

    def _test_relationships(self):
        Employee = self.classes.Employee
        employee_t = self.tables.employee_t
        eq_(
            set(Employee.employees.property.local_remote_pairs),
            set([
                (employee_t.c.company_id, employee_t.c.company_id),
                (employee_t.c.emp_id, employee_t.c.reports_to_id),
            ])
        )
        eq_(
            Employee.employees.property.remote_side,
            set([employee_t.c.company_id, employee_t.c.reports_to_id])
        )
        eq_(
            set(Employee.reports_to.property.local_remote_pairs),
            set([
                (employee_t.c.company_id, employee_t.c.company_id),
                (employee_t.c.reports_to_id, employee_t.c.emp_id),
                ])
        )

    def _setup_data(self, sess):
        Employee, Company = self.classes.Employee, self.classes.Company

        c1 = Company('c1')
        c2 = Company('c2')

        e1 = Employee('emp1', c1, 1)
        e2 = Employee('emp2', c1, 2, e1)  # noqa
        e3 = Employee('emp3', c1, 3, e1)
        e4 = Employee('emp4', c1, 4, e3)  # noqa
        e5 = Employee('emp5', c2, 1)
        e6 = Employee('emp6', c2, 2, e5)  # noqa
        e7 = Employee('emp7', c2, 3, e5)  # noqa

        sess.add_all((c1, c2))
        sess.commit()
        sess.close()

    def _test_lazy_relations(self, sess):
        Employee, Company = self.classes.Employee, self.classes.Company

        c1 = sess.query(Company).filter_by(name='c1').one()
        c2 = sess.query(Company).filter_by(name='c2').one()
        e1 = sess.query(Employee).filter_by(name='emp1').one()
        e5 = sess.query(Employee).filter_by(name='emp5').one()

        test_e1 = sess.query(Employee).get([c1.company_id, e1.emp_id])
        assert test_e1.name == 'emp1', test_e1.name
        test_e5 = sess.query(Employee).get([c2.company_id, e5.emp_id])
        assert test_e5.name == 'emp5', test_e5.name
        assert [x.name for x in test_e1.employees] == ['emp2', 'emp3']
        assert sess.query(Employee).\
            get([c1.company_id, 3]).reports_to.name == 'emp1'
        assert sess.query(Employee).\
            get([c2.company_id, 3]).reports_to.name == 'emp5'

    def _test_join_aliasing(self, sess):
        Employee, Company = self.classes.Employee, self.classes.Company
        eq_(
            [n for n, in sess.query(Employee.name).
             join(Employee.reports_to, aliased=True).
             filter_by(name='emp5').
             reset_joinpoint().
             order_by(Employee.name)],
            ['emp6', 'emp7']
        )


class CompositeJoinPartialFK(fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    @classmethod
    def define_tables(cls, metadata):
        Table("parent", metadata,
              Column('x', Integer, primary_key=True),
              Column('y', Integer, primary_key=True),
              Column('z', Integer),
              )
        Table("child", metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('x', Integer),
              Column('y', Integer),
              Column('z', Integer),
              # note 'z' is not here
              sa.ForeignKeyConstraint(
                  ["x", "y"],
                  ["parent.x", "parent.y"]
              )
              )

    @classmethod
    def setup_mappers(cls):
        parent, child = cls.tables.parent, cls.tables.child

        class Parent(cls.Comparable):
            pass

        class Child(cls.Comparable):
            pass
        mapper(Parent, parent, properties={
            'children': relationship(Child, primaryjoin=and_(
                parent.c.x == child.c.x,
                parent.c.y == child.c.y,
                parent.c.z == child.c.z,
            ))
        })
        mapper(Child, child)

    def test_joins_fully(self):
        Parent, Child = self.classes.Parent, self.classes.Child

        self.assert_compile(
            Parent.children.property.strategy._lazywhere,
            ":param_1 = child.x AND :param_2 = child.y AND :param_3 = child.z"
        )


class SynonymsAsFKsTest(fixtures.MappedTest):

    """Syncrules on foreign keys that are also primary"""

    @classmethod
    def define_tables(cls, metadata):
        Table("tableA", metadata,
              Column("id", Integer, primary_key=True),
              Column("foo", Integer,),
              test_needs_fk=True)

        Table("tableB", metadata,
              Column("id", Integer, primary_key=True),
              Column("_a_id", Integer, key='a_id', primary_key=True),
              test_needs_fk=True)

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):

            @property
            def a_id(self):
                return self._a_id

    def test_synonym_fk(self):
        """test that active history is enabled on a
        one-to-many/one that has use_get==True"""

        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)

        mapper(B, tableB, properties={
            'a_id': synonym('_a_id', map_column=True)})
        mapper(A, tableA, properties={
            'b': relationship(B, primaryjoin=(tableA.c.id == foreign(B.a_id)),
                              uselist=False)})

        sess = create_session()

        b = B(id=0)
        a = A(id=0, b=b)
        sess.add(a)
        sess.add(b)
        sess.flush()
        sess.expunge_all()

        assert a.b == b
        assert a.id == b.a_id
        assert a.id == b._a_id


class FKsAsPksTest(fixtures.MappedTest):

    """Syncrules on foreign keys that are also primary"""

    @classmethod
    def define_tables(cls, metadata):
        Table("tableA", metadata,
              Column("id", Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column("foo", Integer,),
              test_needs_fk=True)

        Table("tableB", metadata,
              Column("id", Integer, ForeignKey("tableA.id"), primary_key=True),
              test_needs_fk=True)

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

    def test_onetoone_switch(self):
        """test that active history is enabled on a
        one-to-many/one that has use_get==True"""

        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)

        mapper(A, tableA, properties={
            'b': relationship(B, cascade="all,delete-orphan", uselist=False)})
        mapper(B, tableB)

        configure_mappers()
        assert A.b.property.strategy.use_get

        sess = create_session()

        a1 = A()
        sess.add(a1)
        sess.flush()
        sess.close()
        a1 = sess.query(A).first()
        a1.b = B()
        sess.flush()

    def test_no_delete_PK_AtoB(self):
        """A cant be deleted without B because B would have no PK value."""

        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)

        mapper(A, tableA, properties={
            'bs': relationship(B, cascade="save-update")})
        mapper(B, tableB)

        a1 = A()
        a1.bs.append(B())
        sess = create_session()
        sess.add(a1)
        sess.flush()

        sess.delete(a1)
        try:
            sess.flush()
            assert False
        except AssertionError as e:
            startswith_(str(e),
                        "Dependency rule tried to blank-out "
                        "primary key column 'tableB.id' on instance ")

    def test_no_delete_PK_BtoA(self):
        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)

        mapper(B, tableB, properties={
            'a': relationship(A, cascade="save-update")})
        mapper(A, tableA)

        b1 = B()
        a1 = A()
        b1.a = a1
        sess = create_session()
        sess.add(b1)
        sess.flush()
        b1.a = None
        try:
            sess.flush()
            assert False
        except AssertionError as e:
            startswith_(str(e),
                        "Dependency rule tried to blank-out "
                        "primary key column 'tableB.id' on instance ")

    @testing.fails_on_everything_except('sqlite', 'mysql')
    def test_nullPKsOK_BtoA(self):
        A, tableA = self.classes.A, self.tables.tableA

        # postgresql cant handle a nullable PK column...?
        tableC = Table(
            'tablec', tableA.metadata,
            Column('id', Integer, primary_key=True),
            Column('a_id', Integer, ForeignKey('tableA.id'),
                   primary_key=True, nullable=True))
        tableC.create()

        class C(fixtures.BasicEntity):
            pass
        mapper(C, tableC, properties={
            'a': relationship(A, cascade="save-update")
        })
        mapper(A, tableA)

        c1 = C()
        c1.id = 5
        c1.a = None
        sess = create_session()
        sess.add(c1)
        # test that no error is raised.
        sess.flush()

    def test_delete_cascade_BtoA(self):
        """No 'blank the PK' error when the child is to
        be deleted as part of a cascade"""

        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)

        for cascade in ("save-update, delete",
                        #"save-update, delete-orphan",
                        "save-update, delete, delete-orphan"):
            mapper(B, tableB, properties={
                'a': relationship(A, cascade=cascade, single_parent=True)
            })
            mapper(A, tableA)

            b1 = B()
            a1 = A()
            b1.a = a1
            sess = create_session()
            sess.add(b1)
            sess.flush()
            sess.delete(b1)
            sess.flush()
            assert a1 not in sess
            assert b1 not in sess
            sess.expunge_all()
            sa.orm.clear_mappers()

    def test_delete_cascade_AtoB(self):
        """No 'blank the PK' error when the child is to
        be deleted as part of a cascade"""

        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)

        for cascade in ("save-update, delete",
                        #"save-update, delete-orphan",
                        "save-update, delete, delete-orphan"):
            mapper(A, tableA, properties={
                'bs': relationship(B, cascade=cascade)
            })
            mapper(B, tableB)

            a1 = A()
            b1 = B()
            a1.bs.append(b1)
            sess = create_session()
            sess.add(a1)
            sess.flush()

            sess.delete(a1)
            sess.flush()
            assert a1 not in sess
            assert b1 not in sess
            sess.expunge_all()
            sa.orm.clear_mappers()

    def test_delete_manual_AtoB(self):
        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)

        mapper(A, tableA, properties={
            'bs': relationship(B, cascade="none")})
        mapper(B, tableB)

        a1 = A()
        b1 = B()
        a1.bs.append(b1)
        sess = create_session()
        sess.add(a1)
        sess.add(b1)
        sess.flush()

        sess.delete(a1)
        sess.delete(b1)
        sess.flush()
        assert a1 not in sess
        assert b1 not in sess
        sess.expunge_all()

    def test_delete_manual_BtoA(self):
        tableB, A, B, tableA = (self.tables.tableB,
                                self.classes.A,
                                self.classes.B,
                                self.tables.tableA)

        mapper(B, tableB, properties={
            'a': relationship(A, cascade="none")})
        mapper(A, tableA)

        b1 = B()
        a1 = A()
        b1.a = a1
        sess = create_session()
        sess.add(b1)
        sess.add(a1)
        sess.flush()
        sess.delete(b1)
        sess.delete(a1)
        sess.flush()
        assert a1 not in sess
        assert b1 not in sess


class UniqueColReferenceSwitchTest(fixtures.MappedTest):

    """test a relationship based on a primary
    join against a unique non-pk column"""

    @classmethod
    def define_tables(cls, metadata):
        Table("table_a", metadata,
              Column("id", Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column("ident", String(10), nullable=False,
                     unique=True),
              )

        Table("table_b", metadata,
              Column("id", Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column("a_ident", String(10),
                     ForeignKey('table_a.ident'),
                     nullable=False),
              )

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

        class B(cls.Comparable):
            pass

    def test_switch_parent(self):
        A, B, table_b, table_a = (self.classes.A,
                                  self.classes.B,
                                  self.tables.table_b,
                                  self.tables.table_a)

        mapper(A, table_a)
        mapper(B, table_b, properties={"a": relationship(A, backref="bs")})

        session = create_session()
        a1, a2 = A(ident="uuid1"), A(ident="uuid2")
        session.add_all([a1, a2])
        a1.bs = [
            B(), B()
        ]
        session.flush()
        session.expire_all()
        a1, a2 = session.query(A).all()

        for b in list(a1.bs):
            b.a = a2
        session.delete(a1)
        session.flush()


class RelationshipToSelectableTest(fixtures.MappedTest):

    """Test a map to a select that relates to a map to the table."""

    @classmethod
    def define_tables(cls, metadata):
        Table('items', metadata,
              Column('item_policy_num', String(10), primary_key=True,
                     key='policyNum'),
              Column('item_policy_eff_date', sa.Date, primary_key=True,
                     key='policyEffDate'),
              Column('item_type', String(20), primary_key=True,
                     key='type'),
              Column('item_id', Integer, primary_key=True,
                     key='id', autoincrement=False))

    def test_basic(self):
        items = self.tables.items

        class Container(fixtures.BasicEntity):
            pass

        class LineItem(fixtures.BasicEntity):
            pass

        container_select = sa.select(
            [items.c.policyNum, items.c.policyEffDate, items.c.type],
            distinct=True,
        ).alias('container_select')

        mapper(LineItem, items)

        mapper(
            Container,
            container_select,
            properties=dict(
                lineItems=relationship(
                    LineItem,
                    lazy='select',
                    cascade='all, delete-orphan',
                    order_by=sa.asc(items.c.id),
                    primaryjoin=sa.and_(
                        container_select.c.policyNum == items.c.policyNum,
                        container_select.c.policyEffDate ==
                        items.c.policyEffDate,
                        container_select.c.type == items.c.type),
                    foreign_keys=[
                        items.c.policyNum,
                        items.c.policyEffDate,
                        items.c.type
                    ]
                )
            )
        )

        session = create_session()
        con = Container()
        con.policyNum = "99"
        con.policyEffDate = datetime.date.today()
        con.type = "TESTER"
        session.add(con)
        for i in range(0, 10):
            li = LineItem()
            li.id = i
            con.lineItems.append(li)
            session.add(li)
        session.flush()
        session.expunge_all()
        newcon = session.query(Container).\
            order_by(container_select.c.type).first()
        assert con.policyNum == newcon.policyNum
        assert len(newcon.lineItems) == 10
        for old, new in zip(con.lineItems, newcon.lineItems):
            eq_(old.id, new.id)


class FKEquatedToConstantTest(fixtures.MappedTest):

    """test a relationship with a non-column entity in the primary join,
    is not viewonly, and also has the non-column's clause mentioned in the
    foreign keys list.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table('tags', metadata, Column("id", Integer, primary_key=True,
                                       test_needs_autoincrement=True),
              Column("data", String(50)),
              )

        Table('tag_foo', metadata,
              Column("id", Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('tagid', Integer),
              Column("data", String(50)),
              )

    def test_basic(self):
        tag_foo, tags = self.tables.tag_foo, self.tables.tags

        class Tag(fixtures.ComparableEntity):
            pass

        class TagInstance(fixtures.ComparableEntity):
            pass

        mapper(Tag, tags, properties={
            'foo': relationship(
                TagInstance,
                primaryjoin=sa.and_(tag_foo.c.data == 'iplc_case',
                                    tag_foo.c.tagid == tags.c.id),
                foreign_keys=[tag_foo.c.tagid, tag_foo.c.data]),
        })

        mapper(TagInstance, tag_foo)

        sess = create_session()
        t1 = Tag(data='some tag')
        t1.foo.append(TagInstance(data='iplc_case'))
        t1.foo.append(TagInstance(data='not_iplc_case'))
        sess.add(t1)
        sess.flush()
        sess.expunge_all()

        # relationship works
        eq_(
            sess.query(Tag).all(),
            [Tag(data='some tag', foo=[TagInstance(data='iplc_case')])]
        )

        # both TagInstances were persisted
        eq_(
            sess.query(TagInstance).order_by(TagInstance.data).all(),
            [TagInstance(data='iplc_case'), TagInstance(data='not_iplc_case')]
        )


class BackrefPropagatesForwardsArgs(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(50))
              )
        Table('addresses', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('user_id', Integer),
              Column('email', String(50))
              )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

    def test_backref(self):
        User, Address, users, addresses = (self.classes.User,
                                           self.classes.Address,
                                           self.tables.users,
                                           self.tables.addresses)

        mapper(User, users, properties={
            'addresses': relationship(
                Address,
                primaryjoin=addresses.c.user_id == users.c.id,
                foreign_keys=addresses.c.user_id,
                backref='user')
        })
        mapper(Address, addresses)

        sess = sessionmaker()()
        u1 = User(name='u1', addresses=[Address(email='a1')])
        sess.add(u1)
        sess.commit()
        eq_(sess.query(Address).all(), [
            Address(email='a1', user=User(name='u1'))
            ])


class AmbiguousJoinInterpretedAsSelfRef(fixtures.MappedTest):

    """test ambiguous joins due to FKs on both sides treated as
    self-referential.

    this mapping is very similar to that of
    test/orm/inheritance/query.py
    SelfReferentialTestJoinedToBase , except that inheritance is
    not used here.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'subscriber', metadata,
            Column(
                'id', Integer, primary_key=True,
                test_needs_autoincrement=True))

        Table(
            'address', metadata,
            Column(
                'subscriber_id', Integer,
                ForeignKey('subscriber.id'), primary_key=True),
            Column('type', String(1), primary_key=True),
        )

    @classmethod
    def setup_mappers(cls):
        subscriber, address = cls.tables.subscriber, cls.tables.address

        subscriber_and_address = subscriber.join(
            address,
            and_(address.c.subscriber_id == subscriber.c.id,
                 address.c.type.in_(['A', 'B', 'C'])))

        class Address(cls.Comparable):
            pass

        class Subscriber(cls.Comparable):
            pass

        mapper(Address, address)

        mapper(Subscriber, subscriber_and_address, properties={
            'id': [subscriber.c.id, address.c.subscriber_id],
            'addresses': relationship(Address,
                                      backref=backref("customer"))
        })

    def test_mapping(self):
        Subscriber, Address = self.classes.Subscriber, self.classes.Address

        sess = create_session()
        assert Subscriber.addresses.property.direction is ONETOMANY
        assert Address.customer.property.direction is MANYTOONE

        s1 = Subscriber(type='A',
                        addresses=[
                            Address(type='D'),
                            Address(type='E'),
                        ]
                        )
        a1 = Address(type='B', customer=Subscriber(type='C'))

        assert s1.addresses[0].customer is s1
        assert a1.customer.addresses[0] is a1

        sess.add_all([s1, a1])

        sess.flush()
        sess.expunge_all()

        eq_(
            sess.query(Subscriber).order_by(Subscriber.type).all(),
            [
                Subscriber(id=1, type='A'),
                Subscriber(id=2, type='B'),
                Subscriber(id=2, type='C')
            ]
        )


class ManualBackrefTest(_fixtures.FixtureTest):

    """Test explicit relationships that are backrefs to each other."""

    run_inserts = None

    def test_o2m(self):
        users, Address, addresses, User = (self.tables.users,
                                           self.classes.Address,
                                           self.tables.addresses,
                                           self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(Address, back_populates='user')
        })

        mapper(Address, addresses, properties={
            'user': relationship(User, back_populates='addresses')
        })

        sess = create_session()

        u1 = User(name='u1')
        a1 = Address(email_address='foo')
        u1.addresses.append(a1)
        assert a1.user is u1

        sess.add(u1)
        sess.flush()
        sess.expire_all()
        assert sess.query(Address).one() is a1
        assert a1.user is u1
        assert a1 in u1.addresses

    def test_invalid_key(self):
        users, Address, addresses, User = (self.tables.users,
                                           self.classes.Address,
                                           self.tables.addresses,
                                           self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(Address, back_populates='userr')
        })

        mapper(Address, addresses, properties={
            'user': relationship(User, back_populates='addresses')
        })

        assert_raises(sa.exc.InvalidRequestError, configure_mappers)

    def test_invalid_target(self):
        addresses, Dingaling, User, dingalings, Address, users = (
            self.tables.addresses,
            self.classes.Dingaling,
            self.classes.User,
            self.tables.dingalings,
            self.classes.Address,
            self.tables.users)

        mapper(User, users, properties={
            'addresses': relationship(Address, back_populates='dingaling'),
        })

        mapper(Dingaling, dingalings)
        mapper(Address, addresses, properties={
            'dingaling': relationship(Dingaling)
        })

        assert_raises_message(sa.exc.ArgumentError,
                              r"reverse_property 'dingaling' on relationship "
                              "User.addresses references "
                              "relationship Address.dingaling, which does not "
                              "reference mapper Mapper\|User\|users",
                              configure_mappers)


class JoinConditionErrorTest(fixtures.TestBase):

    def test_clauseelement_pj(self):
        from sqlalchemy.ext.declarative import declarative_base
        Base = declarative_base()

        class C1(Base):
            __tablename__ = 'c1'
            id = Column('id', Integer, primary_key=True)

        class C2(Base):
            __tablename__ = 'c2'
            id = Column('id', Integer, primary_key=True)
            c1id = Column('c1id', Integer, ForeignKey('c1.id'))
            c2 = relationship(C1, primaryjoin=C1.id)

        assert_raises(sa.exc.ArgumentError, configure_mappers)

    def test_clauseelement_pj_false(self):
        from sqlalchemy.ext.declarative import declarative_base
        Base = declarative_base()

        class C1(Base):
            __tablename__ = 'c1'
            id = Column('id', Integer, primary_key=True)

        class C2(Base):
            __tablename__ = 'c2'
            id = Column('id', Integer, primary_key=True)
            c1id = Column('c1id', Integer, ForeignKey('c1.id'))
            c2 = relationship(C1, primaryjoin="x" == "y")

        assert_raises(sa.exc.ArgumentError, configure_mappers)

    def test_only_column_elements(self):
        m = MetaData()
        t1 = Table('t1', m,
                   Column('id', Integer, primary_key=True),
                   Column('foo_id', Integer, ForeignKey('t2.id')),
                   )
        t2 = Table('t2', m,
                   Column('id', Integer, primary_key=True),
                   )

        class C1(object):
            pass

        class C2(object):
            pass

        mapper(C1, t1, properties={
            'c2': relationship(C2, primaryjoin=t1.join(t2))})
        mapper(C2, t2)
        assert_raises(sa.exc.ArgumentError, configure_mappers)

    def test_invalid_string_args(self):
        from sqlalchemy.ext.declarative import declarative_base

        for argname, arg in [
            ('remote_side', ['c1.id']),
            ('remote_side', ['id']),
            ('foreign_keys', ['c1id']),
            ('foreign_keys', ['C2.c1id']),
            ('order_by', ['id']),
        ]:
            clear_mappers()
            kw = {argname: arg}
            Base = declarative_base()

            class C1(Base):
                __tablename__ = 'c1'
                id = Column('id', Integer, primary_key=True)

            class C2(Base):
                __tablename__ = 'c2'
                id_ = Column('id', Integer, primary_key=True)
                c1id = Column('c1id', Integer, ForeignKey('c1.id'))
                c2 = relationship(C1, **kw)

            assert_raises_message(
                sa.exc.ArgumentError,
                "Column-based expression object expected "
                "for argument '%s'; got: '%s', type %r" %
                (argname, arg[0], type(arg[0])),
                configure_mappers)

    def test_fk_error_not_raised_unrelated(self):
        m = MetaData()
        t1 = Table('t1', m,
                   Column('id', Integer, primary_key=True),
                   Column('foo_id', Integer, ForeignKey('t2.nonexistent_id')),
                   )
        t2 = Table('t2', m,  # noqa
                   Column('id', Integer, primary_key=True),
                   )

        t3 = Table('t3', m,
                   Column('id', Integer, primary_key=True),
                   Column('t1id', Integer, ForeignKey('t1.id'))
                   )

        class C1(object):
            pass

        class C2(object):
            pass

        mapper(C1, t1, properties={'c2': relationship(C2)})
        mapper(C2, t3)
        assert C1.c2.property.primaryjoin.compare(t1.c.id == t3.c.t1id)

    def test_join_error_raised(self):
        m = MetaData()
        t1 = Table('t1', m,
                   Column('id', Integer, primary_key=True),
                   )
        t2 = Table('t2', m,  # noqa
                   Column('id', Integer, primary_key=True),
                   )

        t3 = Table('t3', m,
                   Column('id', Integer, primary_key=True),
                   Column('t1id', Integer)
                   )

        class C1(object):
            pass

        class C2(object):
            pass

        mapper(C1, t1, properties={'c2': relationship(C2)})
        mapper(C2, t3)

        assert_raises(sa.exc.ArgumentError, configure_mappers)

    def teardown(self):
        clear_mappers()


class TypeMatchTest(fixtures.MappedTest):

    """test errors raised when trying to add items
        whose type is not handled by a relationship"""

    @classmethod
    def define_tables(cls, metadata):
        Table("a", metadata,
              Column('aid', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('adata', String(30)))
        Table("b", metadata,
              Column('bid', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column("a_id", Integer, ForeignKey("a.aid")),
              Column('bdata', String(30)))
        Table("c", metadata,
              Column('cid', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column("b_id", Integer, ForeignKey("b.bid")),
              Column('cdata', String(30)))
        Table("d", metadata,
              Column('did', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column("a_id", Integer, ForeignKey("a.aid")),
              Column('ddata', String(30)))

    def test_o2m_oncascade(self):
        a, c, b = (self.tables.a,
                   self.tables.c,
                   self.tables.b)

        class A(fixtures.BasicEntity):
            pass

        class B(fixtures.BasicEntity):
            pass

        class C(fixtures.BasicEntity):
            pass
        mapper(A, a, properties={'bs': relationship(B)})
        mapper(B, b)
        mapper(C, c)

        a1 = A()
        b1 = B()
        c1 = C()
        a1.bs.append(b1)
        a1.bs.append(c1)
        sess = create_session()
        try:
            sess.add(a1)
            assert False
        except AssertionError as err:
            eq_(str(err),
                "Attribute 'bs' on class '%s' doesn't handle "
                "objects of type '%s'" % (A, C))

    def test_o2m_onflush(self):
        a, c, b = (self.tables.a,
                   self.tables.c,
                   self.tables.b)

        class A(fixtures.BasicEntity):
            pass

        class B(fixtures.BasicEntity):
            pass

        class C(fixtures.BasicEntity):
            pass
        mapper(A, a, properties={'bs': relationship(B, cascade="none")})
        mapper(B, b)
        mapper(C, c)

        a1 = A()
        b1 = B()
        c1 = C()
        a1.bs.append(b1)
        a1.bs.append(c1)
        sess = create_session()
        sess.add(a1)
        sess.add(b1)
        sess.add(c1)
        assert_raises_message(sa.orm.exc.FlushError,
                              "Attempting to flush an item",
                              sess.flush)

    def test_o2m_nopoly_onflush(self):
        a, c, b = (self.tables.a,
                   self.tables.c,
                   self.tables.b)

        class A(fixtures.BasicEntity):
            pass

        class B(fixtures.BasicEntity):
            pass

        class C(B):
            pass
        mapper(A, a, properties={'bs': relationship(B, cascade="none")})
        mapper(B, b)
        mapper(C, c, inherits=B)

        a1 = A()
        b1 = B()
        c1 = C()
        a1.bs.append(b1)
        a1.bs.append(c1)
        sess = create_session()
        sess.add(a1)
        sess.add(b1)
        sess.add(c1)
        assert_raises_message(sa.orm.exc.FlushError,
                              "Attempting to flush an item",
                              sess.flush)

    def test_m2o_nopoly_onflush(self):
        a, b, d = (self.tables.a,
                   self.tables.b,
                   self.tables.d)

        class A(fixtures.BasicEntity):
            pass

        class B(A):
            pass

        class D(fixtures.BasicEntity):
            pass
        mapper(A, a)
        mapper(B, b, inherits=A)
        mapper(D, d, properties={"a": relationship(A, cascade="none")})
        b1 = B()
        d1 = D()
        d1.a = b1
        sess = create_session()
        sess.add(b1)
        sess.add(d1)
        assert_raises_message(sa.orm.exc.FlushError,
                              "Attempting to flush an item",
                              sess.flush)

    def test_m2o_oncascade(self):
        a, b, d = (self.tables.a,
                   self.tables.b,
                   self.tables.d)

        class A(fixtures.BasicEntity):
            pass

        class B(fixtures.BasicEntity):
            pass

        class D(fixtures.BasicEntity):
            pass
        mapper(A, a)
        mapper(B, b)
        mapper(D, d, properties={"a": relationship(A)})
        b1 = B()
        d1 = D()
        d1.a = b1
        sess = create_session()
        assert_raises_message(AssertionError,
                              "doesn't handle objects of type",
                              sess.add, d1)


class TypedAssociationTable(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        class MySpecialType(sa.types.TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                return "lala" + value

            def process_result_value(self, value, dialect):
                return value[4:]

        Table('t1', metadata,
              Column('col1', MySpecialType(30), primary_key=True),
              Column('col2', String(30)))
        Table('t2', metadata,
              Column('col1', MySpecialType(30), primary_key=True),
              Column('col2', String(30)))
        Table('t3', metadata,
              Column('t1c1', MySpecialType(30), ForeignKey('t1.col1')),
              Column('t2c1', MySpecialType(30), ForeignKey('t2.col1')))

    def test_m2m(self):
        """Many-to-many tables with special types for candidate keys."""

        t2, t3, t1 = (self.tables.t2,
                      self.tables.t3,
                      self.tables.t1)

        class T1(fixtures.BasicEntity):
            pass

        class T2(fixtures.BasicEntity):
            pass
        mapper(T2, t2)
        mapper(T1, t1, properties={
            't2s': relationship(T2, secondary=t3, backref='t1s')})

        a = T1()
        a.col1 = "aid"
        b = T2()
        b.col1 = "bid"
        c = T2()
        c.col1 = "cid"
        a.t2s.append(b)
        a.t2s.append(c)
        sess = create_session()
        sess.add(a)
        sess.flush()

        eq_(select([func.count('*')]).select_from(t3).scalar(), 2)

        a.t2s.remove(c)
        sess.flush()

        eq_(select([func.count('*')]).select_from(t3).scalar(), 1)


class CustomOperatorTest(fixtures.MappedTest, AssertsCompiledSQL):

    """test op() in conjunction with join conditions"""

    run_create_tables = run_deletes = None

    __dialect__ = 'default'

    @classmethod
    def define_tables(cls, metadata):
        Table('a', metadata,
              Column('id', Integer, primary_key=True),
              Column('foo', String(50))
              )
        Table('b', metadata,
              Column('id', Integer, primary_key=True),
              Column('foo', String(50))
              )

    def test_join_on_custom_op(self):
        class A(fixtures.BasicEntity):
            pass

        class B(fixtures.BasicEntity):
            pass

        mapper(A, self.tables.a, properties={
            'bs': relationship(B,
                               primaryjoin=self.tables.a.c.foo.op(
                                   '&*', is_comparison=True
                               )(foreign(self.tables.b.c.foo)),
                               viewonly=True
                               )
        })
        mapper(B, self.tables.b)
        self.assert_compile(
            Session().query(A).join(A.bs),
            "SELECT a.id AS a_id, a.foo AS a_foo "
            "FROM a JOIN b ON a.foo &* b.foo"
        )


class ViewOnlyHistoryTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table("t1", metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(40)))
        Table("t2", metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(40)),
              Column('t1id', Integer, ForeignKey('t1.id')))

    def _assert_fk(self, a1, b1, is_set):
        s = Session(testing.db)
        s.add_all([a1, b1])
        s.flush()

        if is_set:
            eq_(b1.t1id, a1.id)
        else:
            eq_(b1.t1id, None)

        return s

    def test_o2m_viewonly_oneside(self):
        class A(fixtures.ComparableEntity):
            pass

        class B(fixtures.ComparableEntity):
            pass

        mapper(A, self.tables.t1, properties={
            "bs": relationship(B, viewonly=True,
                               backref=backref("a", viewonly=False))
        })
        mapper(B, self.tables.t2)

        a1 = A()
        b1 = B()
        a1.bs.append(b1)
        assert b1.a is a1
        assert not inspect(a1).attrs.bs.history.has_changes()
        assert inspect(b1).attrs.a.history.has_changes()

        sess = self._assert_fk(a1, b1, True)

        a1.bs.remove(b1)
        assert a1 not in sess.dirty
        assert b1 in sess.dirty

    def test_m2o_viewonly_oneside(self):
        class A(fixtures.ComparableEntity):
            pass

        class B(fixtures.ComparableEntity):
            pass

        mapper(A, self.tables.t1, properties={
            "bs": relationship(B, viewonly=False,
                               backref=backref("a", viewonly=True))
        })
        mapper(B, self.tables.t2)

        a1 = A()
        b1 = B()
        b1.a = a1
        assert b1 in a1.bs
        assert inspect(a1).attrs.bs.history.has_changes()
        assert not inspect(b1).attrs.a.history.has_changes()

        sess = self._assert_fk(a1, b1, True)

        a1.bs.remove(b1)
        assert a1 in sess.dirty
        assert b1 not in sess.dirty

    def test_o2m_viewonly_only(self):
        class A(fixtures.ComparableEntity):
            pass

        class B(fixtures.ComparableEntity):
            pass

        mapper(A, self.tables.t1, properties={
            "bs": relationship(B, viewonly=True)
        })
        mapper(B, self.tables.t2)

        a1 = A()
        b1 = B()
        a1.bs.append(b1)
        assert not inspect(a1).attrs.bs.history.has_changes()

        self._assert_fk(a1, b1, False)

    def test_m2o_viewonly_only(self):
        class A(fixtures.ComparableEntity):
            pass

        class B(fixtures.ComparableEntity):
            pass

        mapper(A, self.tables.t1)
        mapper(B, self.tables.t2, properties={
            'a': relationship(A, viewonly=True)
        })

        a1 = A()
        b1 = B()
        b1.a = a1
        assert not inspect(b1).attrs.a.history.has_changes()

        self._assert_fk(a1, b1, False)


class ViewOnlyM2MBackrefTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table("t1", metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(40)))
        Table("t2", metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(40)),
              )
        Table("t1t2", metadata,
              Column('t1id', Integer, ForeignKey('t1.id'), primary_key=True),
              Column('t2id', Integer, ForeignKey('t2.id'), primary_key=True),
              )

    def test_viewonly(self):
        t1t2, t2, t1 = (self.tables.t1t2,
                        self.tables.t2,
                        self.tables.t1)

        class A(fixtures.ComparableEntity):
            pass

        class B(fixtures.ComparableEntity):
            pass

        mapper(A, t1, properties={
            'bs': relationship(B, secondary=t1t2,
                               backref=backref('as_', viewonly=True))
        })
        mapper(B, t2)

        sess = create_session()
        a1 = A()
        b1 = B(as_=[a1])

        assert not inspect(b1).attrs.as_.history.has_changes()

        sess.add(a1)
        sess.flush()
        eq_(
            sess.query(A).first(), A(bs=[B(id=b1.id)])
        )
        eq_(
            sess.query(B).first(), B(as_=[A(id=a1.id)])
        )


class ViewOnlyOverlappingNames(fixtures.MappedTest):

    """'viewonly' mappings with overlapping PK column names."""

    @classmethod
    def define_tables(cls, metadata):
        Table("t1", metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(40)))
        Table("t2", metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(40)),
              Column('t1id', Integer, ForeignKey('t1.id')))
        Table("t3", metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(40)),
              Column('t2id', Integer, ForeignKey('t2.id')))

    def test_three_table_view(self):
        """A three table join with overlapping PK names.

        A third table is pulled into the primary join condition using
        overlapping PK column names and should not produce 'conflicting column'
        error.

        """

        t2, t3, t1 = (self.tables.t2,
                      self.tables.t3,
                      self.tables.t1)

        class C1(fixtures.BasicEntity):
            pass

        class C2(fixtures.BasicEntity):
            pass

        class C3(fixtures.BasicEntity):
            pass

        mapper(C1, t1, properties={
            't2s': relationship(C2),
            't2_view': relationship(
                C2,
                viewonly=True,
                primaryjoin=sa.and_(t1.c.id == t2.c.t1id,
                                    t3.c.t2id == t2.c.id,
                                    t3.c.data == t1.c.data))})
        mapper(C2, t2)
        mapper(C3, t3, properties={
            't2': relationship(C2)})

        c1 = C1()
        c1.data = 'c1data'
        c2a = C2()
        c1.t2s.append(c2a)
        c2b = C2()
        c1.t2s.append(c2b)
        c3 = C3()
        c3.data = 'c1data'
        c3.t2 = c2b
        sess = create_session()
        sess.add(c1)
        sess.add(c3)
        sess.flush()
        sess.expunge_all()

        c1 = sess.query(C1).get(c1.id)
        assert set([x.id for x in c1.t2s]) == set([c2a.id, c2b.id])
        assert set([x.id for x in c1.t2_view]) == set([c2b.id])


class ViewOnlyUniqueNames(fixtures.MappedTest):

    """'viewonly' mappings with unique PK column names."""

    @classmethod
    def define_tables(cls, metadata):
        Table("t1", metadata,
              Column('t1id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(40)))
        Table("t2", metadata,
              Column('t2id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(40)),
              Column('t1id_ref', Integer, ForeignKey('t1.t1id')))
        Table("t3", metadata,
              Column('t3id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(40)),
              Column('t2id_ref', Integer, ForeignKey('t2.t2id')))

    def test_three_table_view(self):
        """A three table join with overlapping PK names.

        A third table is pulled into the primary join condition using unique
        PK column names and should not produce 'mapper has no columnX' error.

        """

        t2, t3, t1 = (self.tables.t2,
                      self.tables.t3,
                      self.tables.t1)

        class C1(fixtures.BasicEntity):
            pass

        class C2(fixtures.BasicEntity):
            pass

        class C3(fixtures.BasicEntity):
            pass

        mapper(C1, t1, properties={
            't2s': relationship(C2),
            't2_view': relationship(
                C2,
                viewonly=True,
                primaryjoin=sa.and_(t1.c.t1id == t2.c.t1id_ref,
                                    t3.c.t2id_ref == t2.c.t2id,
                                    t3.c.data == t1.c.data))})
        mapper(C2, t2)
        mapper(C3, t3, properties={
            't2': relationship(C2)})

        c1 = C1()
        c1.data = 'c1data'
        c2a = C2()
        c1.t2s.append(c2a)
        c2b = C2()
        c1.t2s.append(c2b)
        c3 = C3()
        c3.data = 'c1data'
        c3.t2 = c2b
        sess = create_session()

        sess.add_all((c1, c3))
        sess.flush()
        sess.expunge_all()

        c1 = sess.query(C1).get(c1.t1id)
        assert set([x.t2id for x in c1.t2s]) == set([c2a.t2id, c2b.t2id])
        assert set([x.t2id for x in c1.t2_view]) == set([c2b.t2id])


class ViewOnlyLocalRemoteM2M(fixtures.TestBase):

    """test that local-remote is correctly determined for m2m"""

    def test_local_remote(self):
        meta = MetaData()

        t1 = Table('t1', meta,
                   Column('id', Integer, primary_key=True),
                   )
        t2 = Table('t2', meta,
                   Column('id', Integer, primary_key=True),
                   )
        t12 = Table('tab', meta,
                    Column('t1_id', Integer, ForeignKey('t1.id',)),
                    Column('t2_id', Integer, ForeignKey('t2.id',)),
                    )

        class A(object):
            pass

        class B(object):
            pass
        mapper(B, t2, )
        m = mapper(A, t1, properties=dict(
            b_view=relationship(B, secondary=t12, viewonly=True),
            b_plain=relationship(B, secondary=t12),
        )
        )
        configure_mappers()
        assert m.get_property('b_view').local_remote_pairs == \
            m.get_property('b_plain').local_remote_pairs == \
            [(t1.c.id, t12.c.t1_id), (t2.c.id, t12.c.t2_id)]


class ViewOnlyNonEquijoin(fixtures.MappedTest):

    """'viewonly' mappings based on non-equijoins."""

    @classmethod
    def define_tables(cls, metadata):
        Table('foos', metadata,
              Column('id', Integer, primary_key=True))
        Table('bars', metadata,
              Column('id', Integer, primary_key=True),
              Column('fid', Integer))

    def test_viewonly_join(self):
        bars, foos = self.tables.bars, self.tables.foos

        class Foo(fixtures.ComparableEntity):
            pass

        class Bar(fixtures.ComparableEntity):
            pass

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 primaryjoin=foos.c.id > bars.c.fid,
                                 foreign_keys=[bars.c.fid],
                                 viewonly=True)})

        mapper(Bar, bars)

        sess = create_session()
        sess.add_all((Foo(id=4),
                      Foo(id=9),
                      Bar(id=1, fid=2),
                      Bar(id=2, fid=3),
                      Bar(id=3, fid=6),
                      Bar(id=4, fid=7)))
        sess.flush()

        sess = create_session()
        eq_(sess.query(Foo).filter_by(id=4).one(),
            Foo(id=4, bars=[Bar(fid=2), Bar(fid=3)]))
        eq_(sess.query(Foo).filter_by(id=9).one(),
            Foo(id=9, bars=[Bar(fid=2), Bar(fid=3), Bar(fid=6), Bar(fid=7)]))


class ViewOnlyRepeatedRemoteColumn(fixtures.MappedTest):

    """'viewonly' mappings that contain the same 'remote' column twice"""

    @classmethod
    def define_tables(cls, metadata):
        Table('foos', metadata,
              Column(
                  'id', Integer, primary_key=True,
                  test_needs_autoincrement=True),
              Column('bid1', Integer, ForeignKey('bars.id')),
              Column('bid2', Integer, ForeignKey('bars.id')))

        Table('bars', metadata,
              Column(
                  'id', Integer, primary_key=True,
                  test_needs_autoincrement=True),
              Column('data', String(50)))

    def test_relationship_on_or(self):
        bars, foos = self.tables.bars, self.tables.foos

        class Foo(fixtures.ComparableEntity):
            pass

        class Bar(fixtures.ComparableEntity):
            pass

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 primaryjoin=sa.or_(bars.c.id == foos.c.bid1,
                                                    bars.c.id == foos.c.bid2),
                                 uselist=True,
                                 viewonly=True)})
        mapper(Bar, bars)

        sess = create_session()
        b1 = Bar(id=1, data='b1')
        b2 = Bar(id=2, data='b2')
        b3 = Bar(id=3, data='b3')
        f1 = Foo(bid1=1, bid2=2)
        f2 = Foo(bid1=3, bid2=None)

        sess.add_all((b1, b2, b3))
        sess.flush()

        sess.add_all((f1, f2))
        sess.flush()

        sess.expunge_all()
        eq_(sess.query(Foo).filter_by(id=f1.id).one(),
            Foo(bars=[Bar(data='b1'), Bar(data='b2')]))
        eq_(sess.query(Foo).filter_by(id=f2.id).one(),
            Foo(bars=[Bar(data='b3')]))


class ViewOnlyRepeatedLocalColumn(fixtures.MappedTest):

    """'viewonly' mappings that contain the same 'local' column twice"""

    @classmethod
    def define_tables(cls, metadata):
        Table('foos', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(50)))

        Table('bars', metadata, Column('id', Integer, primary_key=True,
                                       test_needs_autoincrement=True),
              Column('fid1', Integer, ForeignKey('foos.id')),
              Column('fid2', Integer, ForeignKey('foos.id')),
              Column('data', String(50)))

    def test_relationship_on_or(self):
        bars, foos = self.tables.bars, self.tables.foos

        class Foo(fixtures.ComparableEntity):
            pass

        class Bar(fixtures.ComparableEntity):
            pass

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 primaryjoin=sa.or_(bars.c.fid1 == foos.c.id,
                                                    bars.c.fid2 == foos.c.id),
                                 viewonly=True)})
        mapper(Bar, bars)

        sess = create_session()
        f1 = Foo(id=1, data='f1')
        f2 = Foo(id=2, data='f2')
        b1 = Bar(fid1=1, data='b1')
        b2 = Bar(fid2=1, data='b2')
        b3 = Bar(fid1=2, data='b3')
        b4 = Bar(fid1=1, fid2=2, data='b4')

        sess.add_all((f1, f2))
        sess.flush()

        sess.add_all((b1, b2, b3, b4))
        sess.flush()

        sess.expunge_all()
        eq_(sess.query(Foo).filter_by(id=f1.id).one(),
            Foo(bars=[Bar(data='b1'), Bar(data='b2'), Bar(data='b4')]))
        eq_(sess.query(Foo).filter_by(id=f2.id).one(),
            Foo(bars=[Bar(data='b3'), Bar(data='b4')]))


class ViewOnlyComplexJoin(_RelationshipErrors, fixtures.MappedTest):

    """'viewonly' mappings with a complex join condition."""

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(50)))
        Table('t2', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(50)),
              Column('t1id', Integer, ForeignKey('t1.id')))
        Table('t3', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(50)))
        Table('t2tot3', metadata,
              Column('t2id', Integer, ForeignKey('t2.id')),
              Column('t3id', Integer, ForeignKey('t3.id')))

    @classmethod
    def setup_classes(cls):
        class T1(cls.Comparable):
            pass

        class T2(cls.Comparable):
            pass

        class T3(cls.Comparable):
            pass

    def test_basic(self):
        T1, t2, T2, T3, t3, t2tot3, t1 = (self.classes.T1,
                                          self.tables.t2,
                                          self.classes.T2,
                                          self.classes.T3,
                                          self.tables.t3,
                                          self.tables.t2tot3,
                                          self.tables.t1)

        mapper(T1, t1, properties={
            't3s': relationship(T3, primaryjoin=sa.and_(
                t1.c.id == t2.c.t1id,
                t2.c.id == t2tot3.c.t2id,
                t3.c.id == t2tot3.c.t3id),
                viewonly=True,
                foreign_keys=t3.c.id, remote_side=t2.c.t1id)
        })
        mapper(T2, t2, properties={
            't1': relationship(T1),
            't3s': relationship(T3, secondary=t2tot3)
        })
        mapper(T3, t3)

        sess = create_session()
        sess.add(T2(data='t2', t1=T1(data='t1'), t3s=[T3(data='t3')]))
        sess.flush()
        sess.expunge_all()

        a = sess.query(T1).first()
        eq_(a.t3s, [T3(data='t3')])

    def test_remote_side_escalation(self):
        T1, t2, T2, T3, t3, t2tot3, t1 = (self.classes.T1,
                                          self.tables.t2,
                                          self.classes.T2,
                                          self.classes.T3,
                                          self.tables.t3,
                                          self.tables.t2tot3,
                                          self.tables.t1)

        mapper(T1, t1, properties={
            't3s': relationship(T3,
                                primaryjoin=sa.and_(t1.c.id == t2.c.t1id,
                                                    t2.c.id == t2tot3.c.t2id,
                                                    t3.c.id == t2tot3.c.t3id
                                                    ),
                                viewonly=True,
                                foreign_keys=t3.c.id)})
        mapper(T2, t2, properties={
            't1': relationship(T1),
            't3s': relationship(T3, secondary=t2tot3)})
        mapper(T3, t3)
        self._assert_raises_no_local_remote(configure_mappers, "T1.t3s")


class RemoteForeignBetweenColsTest(fixtures.DeclarativeMappedTest):

    """test a complex annotation using between().

    Using declarative here as an integration test for the local()
    and remote() annotations in conjunction with already annotated
    instrumented attributes, etc.

    """
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Network(fixtures.ComparableEntity, Base):
            __tablename__ = "network"

            id = Column(sa.Integer, primary_key=True,
                        test_needs_autoincrement=True)
            ip_net_addr = Column(Integer)
            ip_broadcast_addr = Column(Integer)

            addresses = relationship(
                "Address",
                primaryjoin="remote(foreign(Address.ip_addr)).between("
                "Network.ip_net_addr,"
                "Network.ip_broadcast_addr)",
                viewonly=True
            )

        class Address(fixtures.ComparableEntity, Base):
            __tablename__ = "address"

            ip_addr = Column(Integer, primary_key=True)

    @classmethod
    def insert_data(cls):
        Network, Address = cls.classes.Network, cls.classes.Address
        s = Session(testing.db)

        s.add_all([
            Network(ip_net_addr=5, ip_broadcast_addr=10),
            Network(ip_net_addr=15, ip_broadcast_addr=25),
            Network(ip_net_addr=30, ip_broadcast_addr=35),
            Address(ip_addr=17), Address(ip_addr=18), Address(ip_addr=9),
            Address(ip_addr=27)
        ])
        s.commit()

    def test_col_query(self):
        Network, Address = self.classes.Network, self.classes.Address

        session = Session(testing.db)
        eq_(
            session.query(Address.ip_addr).
            select_from(Network).
            join(Network.addresses).
            filter(Network.ip_net_addr == 15).
            all(),
            [(17, ), (18, )]
        )

    def test_lazyload(self):
        Network, Address = self.classes.Network, self.classes.Address

        session = Session(testing.db)

        n3 = session.query(Network).filter(Network.ip_net_addr == 5).one()
        eq_([a.ip_addr for a in n3.addresses], [9])


class ExplicitLocalRemoteTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
              Column('id', String(50), primary_key=True),
              Column('data', String(50)))
        Table('t2', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(50)),
              Column('t1id', String(50)))

    @classmethod
    def setup_classes(cls):
        class T1(cls.Comparable):
            pass

        class T2(cls.Comparable):
            pass

    def test_onetomany_funcfk_oldstyle(self):
        T2, T1, t2, t1 = (self.classes.T2,
                          self.classes.T1,
                          self.tables.t2,
                          self.tables.t1)

        # old _local_remote_pairs
        mapper(T1, t1, properties={
            't2s': relationship(
                T2,
                primaryjoin=t1.c.id == sa.func.lower(t2.c.t1id),
                _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
                foreign_keys=[t2.c.t1id]
            )
        })
        mapper(T2, t2)
        self._test_onetomany()

    def test_onetomany_funcfk_annotated(self):
        T2, T1, t2, t1 = (self.classes.T2,
                          self.classes.T1,
                          self.tables.t2,
                          self.tables.t1)

        # use annotation
        mapper(T1, t1, properties={
            't2s': relationship(T2,
                                primaryjoin=t1.c.id ==
                                foreign(sa.func.lower(t2.c.t1id)),
                                )})
        mapper(T2, t2)
        self._test_onetomany()

    def _test_onetomany(self):
        T2, T1, t2, t1 = (self.classes.T2,
                          self.classes.T1,
                          self.tables.t2,
                          self.tables.t1)
        is_(T1.t2s.property.direction, ONETOMANY)
        eq_(T1.t2s.property.local_remote_pairs, [(t1.c.id, t2.c.t1id)])
        sess = create_session()
        a1 = T1(id='number1', data='a1')
        a2 = T1(id='number2', data='a2')
        b1 = T2(data='b1', t1id='NuMbEr1')
        b2 = T2(data='b2', t1id='Number1')
        b3 = T2(data='b3', t1id='Number2')
        sess.add_all((a1, a2, b1, b2, b3))
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(T1).first(),
            T1(id='number1', data='a1', t2s=[
               T2(data='b1', t1id='NuMbEr1'),
               T2(data='b2', t1id='Number1')]))

    def test_manytoone_funcfk(self):
        T2, T1, t2, t1 = (self.classes.T2,
                          self.classes.T1,
                          self.tables.t2,
                          self.tables.t1)

        mapper(T1, t1)
        mapper(T2, t2, properties={
            't1': relationship(T1,
                               primaryjoin=t1.c.id == sa.func.lower(t2.c.t1id),
                               _local_remote_pairs=[(t2.c.t1id, t1.c.id)],
                               foreign_keys=[t2.c.t1id],
                               uselist=True)})

        sess = create_session()
        a1 = T1(id='number1', data='a1')
        a2 = T1(id='number2', data='a2')
        b1 = T2(data='b1', t1id='NuMbEr1')
        b2 = T2(data='b2', t1id='Number1')
        b3 = T2(data='b3', t1id='Number2')
        sess.add_all((a1, a2, b1, b2, b3))
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(T2).filter(T2.data.in_(['b1', 'b2'])).all(),
            [T2(data='b1', t1=[T1(id='number1', data='a1')]),
             T2(data='b2', t1=[T1(id='number1', data='a1')])])

    def test_onetomany_func_referent(self):
        T2, T1, t2, t1 = (self.classes.T2,
                          self.classes.T1,
                          self.tables.t2,
                          self.tables.t1)

        mapper(T1, t1, properties={
            't2s': relationship(
                T2,
                primaryjoin=sa.func.lower(t1.c.id) == t2.c.t1id,
                _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
                foreign_keys=[t2.c.t1id])})
        mapper(T2, t2)

        sess = create_session()
        a1 = T1(id='NuMbeR1', data='a1')
        a2 = T1(id='NuMbeR2', data='a2')
        b1 = T2(data='b1', t1id='number1')
        b2 = T2(data='b2', t1id='number1')
        b3 = T2(data='b2', t1id='number2')
        sess.add_all((a1, a2, b1, b2, b3))
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(T1).first(),
            T1(id='NuMbeR1', data='a1', t2s=[
                T2(data='b1', t1id='number1'),
                T2(data='b2', t1id='number1')]))

    def test_manytoone_func_referent(self):
        T2, T1, t2, t1 = (self.classes.T2,
                          self.classes.T1,
                          self.tables.t2,
                          self.tables.t1)

        mapper(T1, t1)
        mapper(T2, t2, properties={
            't1': relationship(T1,
                               primaryjoin=sa.func.lower(t1.c.id) == t2.c.t1id,
                               _local_remote_pairs=[(t2.c.t1id, t1.c.id)],
                               foreign_keys=[t2.c.t1id], uselist=True)})

        sess = create_session()
        a1 = T1(id='NuMbeR1', data='a1')
        a2 = T1(id='NuMbeR2', data='a2')
        b1 = T2(data='b1', t1id='number1')
        b2 = T2(data='b2', t1id='number1')
        b3 = T2(data='b3', t1id='number2')
        sess.add_all((a1, a2, b1, b2, b3))
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(T2).filter(T2.data.in_(['b1', 'b2'])).all(),
            [T2(data='b1', t1=[T1(id='NuMbeR1', data='a1')]),
             T2(data='b2', t1=[T1(id='NuMbeR1', data='a1')])])

    def test_escalation_1(self):
        T2, T1, t2, t1 = (self.classes.T2,
                          self.classes.T1,
                          self.tables.t2,
                          self.tables.t1)

        mapper(T1, t1, properties={
            't2s': relationship(
                T2,
                primaryjoin=t1.c.id == sa.func.lower(t2.c.t1id),
                _local_remote_pairs=[(t1.c.id, t2.c.t1id)],
                foreign_keys=[t2.c.t1id],
                remote_side=[t2.c.t1id])})
        mapper(T2, t2)
        assert_raises(sa.exc.ArgumentError, sa.orm.configure_mappers)

    def test_escalation_2(self):
        T2, T1, t2, t1 = (self.classes.T2,
                          self.classes.T1,
                          self.tables.t2,
                          self.tables.t1)

        mapper(T1, t1, properties={
            't2s': relationship(
                T2,
                primaryjoin=t1.c.id == sa.func.lower(t2.c.t1id),
                _local_remote_pairs=[(t1.c.id, t2.c.t1id)])})
        mapper(T2, t2)
        assert_raises(sa.exc.ArgumentError, sa.orm.configure_mappers)


class InvalidRemoteSideTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
              Column('id', Integer, primary_key=True),
              Column('data', String(50)),
              Column('t_id', Integer, ForeignKey('t1.id'))
              )

    @classmethod
    def setup_classes(cls):
        class T1(cls.Comparable):
            pass

    def test_o2m_backref(self):
        T1, t1 = self.classes.T1, self.tables.t1

        mapper(T1, t1, properties={
            't1s': relationship(T1, backref='parent')
        })

        assert_raises_message(
            sa.exc.ArgumentError,
            "T1.t1s and back-reference T1.parent are "
            r"both of the same direction symbol\('ONETOMANY'\).  Did you "
            "mean to set remote_side on the many-to-one side ?",
            configure_mappers)

    def test_m2o_backref(self):
        T1, t1 = self.classes.T1, self.tables.t1

        mapper(T1, t1, properties={
            't1s': relationship(T1,
                                backref=backref('parent', remote_side=t1.c.id),
                                remote_side=t1.c.id)
        })

        assert_raises_message(
            sa.exc.ArgumentError,
            "T1.t1s and back-reference T1.parent are "
            r"both of the same direction symbol\('MANYTOONE'\).  Did you "
            "mean to set remote_side on the many-to-one side ?",
            configure_mappers)

    def test_o2m_explicit(self):
        T1, t1 = self.classes.T1, self.tables.t1

        mapper(T1, t1, properties={
            't1s': relationship(T1, back_populates='parent'),
            'parent': relationship(T1, back_populates='t1s'),
        })

        # can't be sure of ordering here
        assert_raises_message(
            sa.exc.ArgumentError,
            r"both of the same direction symbol\('ONETOMANY'\).  Did you "
            "mean to set remote_side on the many-to-one side ?",
            configure_mappers)

    def test_m2o_explicit(self):
        T1, t1 = self.classes.T1, self.tables.t1

        mapper(T1, t1, properties={
            't1s': relationship(T1, back_populates='parent',
                                remote_side=t1.c.id),
            'parent': relationship(T1, back_populates='t1s',
                                   remote_side=t1.c.id)
        })

        # can't be sure of ordering here
        assert_raises_message(
            sa.exc.ArgumentError,
            r"both of the same direction symbol\('MANYTOONE'\).  Did you "
            "mean to set remote_side on the many-to-one side ?",
            configure_mappers)


class AmbiguousFKResolutionTest(_RelationshipErrors, fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table("a", metadata,
              Column('id', Integer, primary_key=True)
              )
        Table("b", metadata,
              Column('id', Integer, primary_key=True),
              Column('aid_1', Integer, ForeignKey('a.id')),
              Column('aid_2', Integer, ForeignKey('a.id')),
              )
        Table("atob", metadata,
              Column('aid', Integer),
              Column('bid', Integer),
              )
        Table("atob_ambiguous", metadata,
              Column('aid1', Integer, ForeignKey('a.id')),
              Column('bid1', Integer, ForeignKey('b.id')),
              Column('aid2', Integer, ForeignKey('a.id')),
              Column('bid2', Integer, ForeignKey('b.id')),
              )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

    def test_ambiguous_fks_o2m(self):
        A, B = self.classes.A, self.classes.B
        a, b = self.tables.a, self.tables.b
        mapper(A, a, properties={
            'bs': relationship(B)
        })
        mapper(B, b)
        self._assert_raises_ambig_join(
            configure_mappers,
            "A.bs",
            None
        )

    def test_with_fks_o2m(self):
        A, B = self.classes.A, self.classes.B
        a, b = self.tables.a, self.tables.b
        mapper(A, a, properties={
            'bs': relationship(B, foreign_keys=b.c.aid_1)
        })
        mapper(B, b)
        sa.orm.configure_mappers()
        assert A.bs.property.primaryjoin.compare(
            a.c.id == b.c.aid_1
        )
        eq_(
            A.bs.property._calculated_foreign_keys,
            set([b.c.aid_1])
        )

    def test_with_pj_o2m(self):
        A, B = self.classes.A, self.classes.B
        a, b = self.tables.a, self.tables.b
        mapper(A, a, properties={
            'bs': relationship(B, primaryjoin=a.c.id == b.c.aid_1)
        })
        mapper(B, b)
        sa.orm.configure_mappers()
        assert A.bs.property.primaryjoin.compare(
            a.c.id == b.c.aid_1
        )
        eq_(
            A.bs.property._calculated_foreign_keys,
            set([b.c.aid_1])
        )

    def test_with_annotated_pj_o2m(self):
        A, B = self.classes.A, self.classes.B
        a, b = self.tables.a, self.tables.b
        mapper(A, a, properties={
            'bs': relationship(B, primaryjoin=a.c.id == foreign(b.c.aid_1))
        })
        mapper(B, b)
        sa.orm.configure_mappers()
        assert A.bs.property.primaryjoin.compare(
            a.c.id == b.c.aid_1
        )
        eq_(
            A.bs.property._calculated_foreign_keys,
            set([b.c.aid_1])
        )

    def test_no_fks_m2m(self):
        A, B = self.classes.A, self.classes.B
        a, b, a_to_b = self.tables.a, self.tables.b, self.tables.atob
        mapper(A, a, properties={
            'bs': relationship(B, secondary=a_to_b)
        })
        mapper(B, b)
        self._assert_raises_no_join(
            sa.orm.configure_mappers,
            "A.bs", a_to_b,
        )

    def test_ambiguous_fks_m2m(self):
        A, B = self.classes.A, self.classes.B
        a, b, a_to_b = self.tables.a, self.tables.b, self.tables.atob_ambiguous
        mapper(A, a, properties={
            'bs': relationship(B, secondary=a_to_b)
        })
        mapper(B, b)

        self._assert_raises_ambig_join(
            configure_mappers,
            "A.bs",
            "atob_ambiguous"
        )

    def test_with_fks_m2m(self):
        A, B = self.classes.A, self.classes.B
        a, b, a_to_b = self.tables.a, self.tables.b, self.tables.atob_ambiguous
        mapper(A, a, properties={
            'bs': relationship(B, secondary=a_to_b,
                               foreign_keys=[a_to_b.c.aid1, a_to_b.c.bid1])
        })
        mapper(B, b)
        sa.orm.configure_mappers()


class SecondaryNestedJoinTest(fixtures.MappedTest, AssertsCompiledSQL,
                              testing.AssertsExecutionResults):

    """test support for a relationship where the 'secondary' table is a
    compound join().

    join() and joinedload() should use a "flat" alias, lazyloading needs
    to ensure the join renders.

    """
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'a', metadata,
            Column(
                'id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column('name', String(30)),
            Column('b_id', ForeignKey('b.id'))
        )
        Table('b', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(30)),
              Column('d_id', ForeignKey('d.id'))
              )
        Table('c', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(30)),
              Column('a_id', ForeignKey('a.id')),
              Column('d_id', ForeignKey('d.id'))
              )
        Table('d', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(30)),
              )

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

        class B(cls.Comparable):
            pass

        class C(cls.Comparable):
            pass

        class D(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B, C, D = cls.classes.A, cls.classes.B, cls.classes.C, cls.classes.D
        a, b, c, d = cls.tables.a, cls.tables.b, cls.tables.c, cls.tables.d
        j = sa.join(b, d, b.c.d_id == d.c.id).join(c, c.c.d_id == d.c.id)
        #j = join(b, d, b.c.d_id == d.c.id).join(c, c.c.d_id == d.c.id).alias()
        mapper(A, a, properties={
            "b": relationship(B),
            "d": relationship(
                D, secondary=j,
                primaryjoin=and_(a.c.b_id == b.c.id, a.c.id == c.c.a_id),
                secondaryjoin=d.c.id == b.c.d_id,
                #primaryjoin=and_(a.c.b_id == j.c.b_id, a.c.id == j.c.c_a_id),
                #secondaryjoin=d.c.id == j.c.b_d_id,
                uselist=False,
                viewonly=True
            )
        })
        mapper(B, b, properties={
            "d": relationship(D)
        })
        mapper(C, c, properties={
            "a": relationship(A),
            "d": relationship(D)
        })
        mapper(D, d)

    @classmethod
    def insert_data(cls):
        A, B, C, D = cls.classes.A, cls.classes.B, cls.classes.C, cls.classes.D
        sess = Session()
        a1, a2, a3, a4 = A(name='a1'), A(name='a2'), A(name='a3'), A(name='a4')
        b1, b2, b3, b4 = B(name='b1'), B(name='b2'), B(name='b3'), B(name='b4')
        c1, c2, c3, c4 = C(name='c1'), C(name='c2'), C(name='c3'), C(name='c4')
        d1, d2 = D(name='d1'), D(name='d2')

        a1.b = b1
        a2.b = b2
        a3.b = b3
        a4.b = b4

        c1.a = a1
        c2.a = a2
        c3.a = a2
        c4.a = a4

        c1.d = d1
        c2.d = d2
        c3.d = d1
        c4.d = d2

        b1.d = d1
        b2.d = d1
        b3.d = d2
        b4.d = d2

        sess.add_all([a1, a2, a3, a4, b1, b2, b3, b4, c1, c2, c4, c4, d1, d2])
        sess.commit()

    def test_render_join(self):
        A, D = self.classes.A, self.classes.D
        sess = Session()
        self.assert_compile(
            sess.query(A).join(A.d),
            "SELECT a.id AS a_id, a.name AS a_name, a.b_id AS a_b_id "
            "FROM a JOIN (b AS b_1 JOIN d AS d_1 ON b_1.d_id = d_1.id "
            "JOIN c AS c_1 ON c_1.d_id = d_1.id) ON a.b_id = b_1.id "
            "AND a.id = c_1.a_id JOIN d ON d.id = b_1.d_id",
            dialect="postgresql"
        )

    def test_render_joinedload(self):
        A, D = self.classes.A, self.classes.D
        sess = Session()
        self.assert_compile(
            sess.query(A).options(joinedload(A.d)),
            "SELECT a.id AS a_id, a.name AS a_name, a.b_id AS a_b_id, "
            "d_1.id AS d_1_id, d_1.name AS d_1_name FROM a LEFT OUTER JOIN "
            "(b AS b_1 JOIN d AS d_2 ON b_1.d_id = d_2.id JOIN c AS c_1 "
            "ON c_1.d_id = d_2.id JOIN d AS d_1 ON d_1.id = b_1.d_id) "
            "ON a.b_id = b_1.id AND a.id = c_1.a_id",
            dialect="postgresql"
        )

    def test_render_lazyload(self):
        from sqlalchemy.testing.assertsql import CompiledSQL

        A, D = self.classes.A, self.classes.D
        sess = Session()
        a1 = sess.query(A).filter(A.name == 'a1').first()

        def go():
            a1.d

        # here, the "lazy" strategy has to ensure the "secondary"
        # table is part of the "select_from()", since it's a join().
        # referring to just the columns wont actually render all those
        # join conditions.
        self.assert_sql_execution(
            testing.db,
            go,
            CompiledSQL(
                "SELECT d.id AS d_id, d.name AS d_name FROM b "
                "JOIN d ON b.d_id = d.id JOIN c ON c.d_id = d.id "
                "WHERE :param_1 = b.id AND :param_2 = c.a_id "
                "AND d.id = b.d_id",
                {'param_1': a1.id, 'param_2': a1.id}
            )
        )

    mapping = {
        "a1": "d1",
        "a2": None,
        "a3": None,
        "a4": "d2"
    }

    def test_join(self):
        A, D = self.classes.A, self.classes.D
        sess = Session()

        for a, d in sess.query(A, D).outerjoin(A.d):
            eq_(self.mapping[a.name], d.name if d is not None else None)

    def test_joinedload(self):
        A, D = self.classes.A, self.classes.D
        sess = Session()

        for a in sess.query(A).options(joinedload(A.d)):
            d = a.d
            eq_(self.mapping[a.name], d.name if d is not None else None)

    def test_lazyload(self):
        A, D = self.classes.A, self.classes.D
        sess = Session()

        for a in sess.query(A):
            d = a.d
            eq_(self.mapping[a.name], d.name if d is not None else None)


class InvalidRelationshipEscalationTest(
        _RelationshipErrors, fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('foos', metadata,
              Column('id', Integer, primary_key=True),
              Column('fid', Integer))
        Table('bars', metadata,
              Column('id', Integer, primary_key=True),
              Column('fid', Integer))

        Table('foos_with_fks', metadata,
              Column('id', Integer, primary_key=True),
              Column('fid', Integer, ForeignKey('foos_with_fks.id')))
        Table('bars_with_fks', metadata,
              Column('id', Integer, primary_key=True),
              Column('fid', Integer, ForeignKey('foos_with_fks.id')))

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

        class Bar(cls.Basic):
            pass

    def test_no_join(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar)})
        mapper(Bar, bars)

        self._assert_raises_no_join(sa.orm.configure_mappers,
                                    "Foo.bars", None
                                    )

    def test_no_join_self_ref(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'foos': relationship(Foo)})
        mapper(Bar, bars)

        self._assert_raises_no_join(
            configure_mappers,
            "Foo.foos",
            None
        )

    def test_no_equated(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 primaryjoin=foos.c.id > bars.c.fid)})
        mapper(Bar, bars)

        self._assert_raises_no_relevant_fks(
            configure_mappers,
            "foos.id > bars.fid", "Foo.bars", "primary"
        )

    def test_no_equated_fks(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 primaryjoin=foos.c.id > bars.c.fid,
                                 foreign_keys=bars.c.fid)})
        mapper(Bar, bars)
        self._assert_raises_no_equality(
            sa.orm.configure_mappers,
            "foos.id > bars.fid", "Foo.bars", "primary"
        )

    def test_no_equated_wo_fks_works_on_relaxed(self):
        foos_with_fks, Foo, Bar, bars_with_fks, foos = (
            self.tables.foos_with_fks,
            self.classes.Foo,
            self.classes.Bar,
            self.tables.bars_with_fks,
            self.tables.foos)

        # very unique - the join between parent/child
        # has no fks, but there is an fk join between two other
        # tables in the join condition, for those users that try creating
        # these big-long-string-of-joining-many-tables primaryjoins.
        # in this case we don't get eq_pairs, but we hit the
        # "works if viewonly" rule.  so here we add another clause regarding
        # "try foreign keys".
        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 primaryjoin=and_(
                                     bars_with_fks.c.fid == foos_with_fks.c.id,
                                     foos_with_fks.c.id == foos.c.id,
                                 )
                                 )})
        mapper(Bar, bars_with_fks)

        self._assert_raises_no_equality(
            sa.orm.configure_mappers,
            "bars_with_fks.fid = foos_with_fks.id "
            "AND foos_with_fks.id = foos.id",
            "Foo.bars", "primary"
        )

    def test_ambiguous_fks(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 primaryjoin=foos.c.id == bars.c.fid,
                                 foreign_keys=[foos.c.id, bars.c.fid])})
        mapper(Bar, bars)

        self._assert_raises_ambiguous_direction(
            sa.orm.configure_mappers,
            "Foo.bars"
        )

    def test_ambiguous_remoteside_o2m(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 primaryjoin=foos.c.id == bars.c.fid,
                                 foreign_keys=[bars.c.fid],
                                 remote_side=[foos.c.id, bars.c.fid],
                                 viewonly=True
                                 )})
        mapper(Bar, bars)

        self._assert_raises_no_local_remote(
            configure_mappers,
            "Foo.bars",
        )

    def test_ambiguous_remoteside_m2o(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 primaryjoin=foos.c.id == bars.c.fid,
                                 foreign_keys=[foos.c.id],
                                 remote_side=[foos.c.id, bars.c.fid],
                                 viewonly=True
                                 )})
        mapper(Bar, bars)

        self._assert_raises_no_local_remote(
            configure_mappers,
            "Foo.bars",
        )

    def test_no_equated_self_ref_no_fks(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'foos': relationship(Foo,
                                 primaryjoin=foos.c.id > foos.c.fid)})
        mapper(Bar, bars)

        self._assert_raises_no_relevant_fks(
            configure_mappers,
            "foos.id > foos.fid", "Foo.foos", "primary"
        )

    def test_no_equated_self_ref_no_equality(self):
        bars, Foo, Bar, foos = (self.tables.bars,
                                self.classes.Foo,
                                self.classes.Bar,
                                self.tables.foos)

        mapper(Foo, foos, properties={
            'foos': relationship(Foo,
                                 primaryjoin=foos.c.id > foos.c.fid,
                                 foreign_keys=[foos.c.fid])})
        mapper(Bar, bars)

        self._assert_raises_no_equality(configure_mappers,
                                        "foos.id > foos.fid", "Foo.foos", "primary"
                                        )

    def test_no_equated_viewonly(self):
        bars, Bar, bars_with_fks, foos_with_fks, Foo, foos = (
            self.tables.bars,
            self.classes.Bar,
            self.tables.bars_with_fks,
            self.tables.foos_with_fks,
            self.classes.Foo,
            self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 primaryjoin=foos.c.id > bars.c.fid,
                                 viewonly=True)})
        mapper(Bar, bars)

        self._assert_raises_no_relevant_fks(
            sa.orm.configure_mappers,
            "foos.id > bars.fid", "Foo.bars", "primary"
        )

        sa.orm.clear_mappers()
        mapper(Foo, foos_with_fks, properties={
            'bars': relationship(
                Bar,
                primaryjoin=foos_with_fks.c.id > bars_with_fks.c.fid,
                viewonly=True)})
        mapper(Bar, bars_with_fks)
        sa.orm.configure_mappers()

    def test_no_equated_self_ref_viewonly(self):
        bars, Bar, bars_with_fks, foos_with_fks, Foo, foos = (
            self.tables.bars,
            self.classes.Bar,
            self.tables.bars_with_fks,
            self.tables.foos_with_fks,
            self.classes.Foo,
            self.tables.foos)

        mapper(Foo, foos, properties={
            'foos': relationship(Foo,
                                 primaryjoin=foos.c.id > foos.c.fid,
                                 viewonly=True)})
        mapper(Bar, bars)

        self._assert_raises_no_relevant_fks(
            sa.orm.configure_mappers,
            "foos.id > foos.fid", "Foo.foos", "primary"
        )

        sa.orm.clear_mappers()
        mapper(Foo, foos_with_fks, properties={
            'foos': relationship(
                Foo,
                primaryjoin=foos_with_fks.c.id > foos_with_fks.c.fid,
                viewonly=True)})
        mapper(Bar, bars_with_fks)
        sa.orm.configure_mappers()

    def test_no_equated_self_ref_viewonly_fks(self):
        Foo, foos = self.classes.Foo, self.tables.foos

        mapper(Foo, foos, properties={
            'foos': relationship(Foo,
                                 primaryjoin=foos.c.id > foos.c.fid,
                                 viewonly=True,
                                 foreign_keys=[foos.c.fid])})

        sa.orm.configure_mappers()
        eq_(Foo.foos.property.local_remote_pairs, [(foos.c.id, foos.c.fid)])

    def test_equated(self):
        bars, Bar, bars_with_fks, foos_with_fks, Foo, foos = (
            self.tables.bars,
            self.classes.Bar,
            self.tables.bars_with_fks,
            self.tables.foos_with_fks,
            self.classes.Foo,
            self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 primaryjoin=foos.c.id == bars.c.fid)})
        mapper(Bar, bars)

        self._assert_raises_no_relevant_fks(
            configure_mappers,
            "foos.id = bars.fid", "Foo.bars", "primary"
        )

        sa.orm.clear_mappers()
        mapper(Foo, foos_with_fks, properties={
            'bars': relationship(
                Bar,
                primaryjoin=foos_with_fks.c.id == bars_with_fks.c.fid)})
        mapper(Bar, bars_with_fks)
        sa.orm.configure_mappers()

    def test_equated_self_ref(self):
        Foo, foos = self.classes.Foo, self.tables.foos

        mapper(Foo, foos, properties={
            'foos': relationship(Foo,
                                 primaryjoin=foos.c.id == foos.c.fid)})

        self._assert_raises_no_relevant_fks(
            configure_mappers,
            "foos.id = foos.fid", "Foo.foos", "primary"
        )

    def test_equated_self_ref_wrong_fks(self):
        bars, Foo, foos = (self.tables.bars,
                           self.classes.Foo,
                           self.tables.foos)

        mapper(Foo, foos, properties={
            'foos': relationship(Foo,
                                 primaryjoin=foos.c.id == foos.c.fid,
                                 foreign_keys=[bars.c.id])})

        self._assert_raises_no_relevant_fks(
            configure_mappers,
            "foos.id = foos.fid", "Foo.foos", "primary"
        )


class InvalidRelationshipEscalationTestM2M(
        _RelationshipErrors, fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('foos', metadata,
              Column('id', Integer, primary_key=True))
        Table('foobars', metadata,
              Column('fid', Integer), Column('bid', Integer))
        Table('bars', metadata,
              Column('id', Integer, primary_key=True))

        Table('foobars_with_fks', metadata,
              Column('fid', Integer, ForeignKey('foos.id')),
              Column('bid', Integer, ForeignKey('bars.id'))
              )

        Table('foobars_with_many_columns', metadata,
              Column('fid', Integer),
              Column('bid', Integer),
              Column('fid1', Integer),
              Column('bid1', Integer),
              Column('fid2', Integer),
              Column('bid2', Integer),
              )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

        class Bar(cls.Basic):
            pass

    def test_no_join(self):
        foobars, bars, Foo, Bar, foos = (self.tables.foobars,
                                         self.tables.bars,
                                         self.classes.Foo,
                                         self.classes.Bar,
                                         self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar, secondary=foobars)})
        mapper(Bar, bars)

        self._assert_raises_no_join(
            configure_mappers,
            "Foo.bars",
            "foobars"
        )

    def test_no_secondaryjoin(self):
        foobars, bars, Foo, Bar, foos = (self.tables.foobars,
                                         self.tables.bars,
                                         self.classes.Foo,
                                         self.classes.Bar,
                                         self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 secondary=foobars,
                                 primaryjoin=foos.c.id > foobars.c.fid)})
        mapper(Bar, bars)

        self._assert_raises_no_join(
            configure_mappers,
            "Foo.bars",
            "foobars"
        )

    def test_no_fks(self):
        foobars_with_many_columns, bars, Bar, foobars, Foo, foos = (
            self.tables.foobars_with_many_columns,
            self.tables.bars,
            self.classes.Bar,
            self.tables.foobars,
            self.classes.Foo,
            self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar, secondary=foobars,
                                 primaryjoin=foos.c.id == foobars.c.fid,
                                 secondaryjoin=foobars.c.bid == bars.c.id)})
        mapper(Bar, bars)
        sa.orm.configure_mappers()
        eq_(
            Foo.bars.property.synchronize_pairs,
            [(foos.c.id, foobars.c.fid)]
        )
        eq_(
            Foo.bars.property.secondary_synchronize_pairs,
            [(bars.c.id, foobars.c.bid)]
        )

        sa.orm.clear_mappers()
        mapper(Foo, foos, properties={
            'bars': relationship(
                Bar,
                secondary=foobars_with_many_columns,
                primaryjoin=foos.c.id ==
                foobars_with_many_columns.c.fid,
                secondaryjoin=foobars_with_many_columns.c.bid ==
                bars.c.id)})
        mapper(Bar, bars)
        sa.orm.configure_mappers()
        eq_(
            Foo.bars.property.synchronize_pairs,
            [(foos.c.id, foobars_with_many_columns.c.fid)]
        )
        eq_(
            Foo.bars.property.secondary_synchronize_pairs,
            [(bars.c.id, foobars_with_many_columns.c.bid)]
        )

    def test_local_col_setup(self):
        foobars_with_fks, bars, Bar, Foo, foos = (
            self.tables.foobars_with_fks,
            self.tables.bars,
            self.classes.Bar,
            self.classes.Foo,
            self.tables.foos)

        # ensure m2m backref is set up with correct annotations
        # [ticket:2578]
        mapper(Foo, foos, properties={
            'bars': relationship(Bar, secondary=foobars_with_fks, backref="foos")
        })
        mapper(Bar, bars)
        sa.orm.configure_mappers()
        eq_(
            Foo.bars.property._join_condition.local_columns,
            set([foos.c.id])
        )
        eq_(
            Bar.foos.property._join_condition.local_columns,
            set([bars.c.id])
        )

    def test_bad_primaryjoin(self):
        foobars_with_fks, bars, Bar, foobars, Foo, foos = (
            self.tables.foobars_with_fks,
            self.tables.bars,
            self.classes.Bar,
            self.tables.foobars,
            self.classes.Foo,
            self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 secondary=foobars,
                                 primaryjoin=foos.c.id > foobars.c.fid,
                                 secondaryjoin=foobars.c.bid <= bars.c.id)})
        mapper(Bar, bars)

        self._assert_raises_no_equality(
            configure_mappers,
            'foos.id > foobars.fid',
            "Foo.bars",
            "primary")

        sa.orm.clear_mappers()
        mapper(Foo, foos, properties={
            'bars': relationship(
                Bar,
                secondary=foobars_with_fks,
                primaryjoin=foos.c.id > foobars_with_fks.c.fid,
                secondaryjoin=foobars_with_fks.c.bid <= bars.c.id)})
        mapper(Bar, bars)
        self._assert_raises_no_equality(
            configure_mappers,
            'foos.id > foobars_with_fks.fid',
            "Foo.bars",
            "primary")

        sa.orm.clear_mappers()
        mapper(Foo, foos, properties={
            'bars': relationship(
                Bar,
                secondary=foobars_with_fks,
                primaryjoin=foos.c.id > foobars_with_fks.c.fid,
                secondaryjoin=foobars_with_fks.c.bid <= bars.c.id,
                viewonly=True)})
        mapper(Bar, bars)
        sa.orm.configure_mappers()

    def test_bad_secondaryjoin(self):
        foobars, bars, Foo, Bar, foos = (self.tables.foobars,
                                         self.tables.bars,
                                         self.classes.Foo,
                                         self.classes.Bar,
                                         self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 secondary=foobars,
                                 primaryjoin=foos.c.id == foobars.c.fid,
                                 secondaryjoin=foobars.c.bid <= bars.c.id,
                                 foreign_keys=[foobars.c.fid])})
        mapper(Bar, bars)
        self._assert_raises_no_relevant_fks(
            configure_mappers,
            "foobars.bid <= bars.id",
            "Foo.bars",
            "secondary"
        )

    def test_no_equated_secondaryjoin(self):
        foobars, bars, Foo, Bar, foos = (self.tables.foobars,
                                         self.tables.bars,
                                         self.classes.Foo,
                                         self.classes.Bar,
                                         self.tables.foos)

        mapper(Foo, foos, properties={
            'bars': relationship(Bar,
                                 secondary=foobars,
                                 primaryjoin=foos.c.id == foobars.c.fid,
                                 secondaryjoin=foobars.c.bid <= bars.c.id,
                                 foreign_keys=[foobars.c.fid, foobars.c.bid])})
        mapper(Bar, bars)

        self._assert_raises_no_equality(
            configure_mappers,
            "foobars.bid <= bars.id",
            "Foo.bars",
            "secondary"
        )


class ActiveHistoryFlagTest(_fixtures.FixtureTest):
    run_inserts = None
    run_deletes = None

    def _test_attribute(self, obj, attrname, newvalue):
        sess = Session()
        sess.add(obj)
        oldvalue = getattr(obj, attrname)
        sess.commit()

        # expired
        assert attrname not in obj.__dict__

        setattr(obj, attrname, newvalue)
        eq_(
            attributes.get_history(obj, attrname),
            ([newvalue, ], (), [oldvalue, ])
        )

    def test_column_property_flag(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users, properties={
            'name': column_property(users.c.name,
                                    active_history=True)
        })
        u1 = User(name='jack')
        self._test_attribute(u1, 'name', 'ed')

    def test_relationship_property_flag(self):
        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(Address, addresses, properties={
            'user': relationship(User, active_history=True)
        })
        mapper(User, users)
        u1 = User(name='jack')
        u2 = User(name='ed')
        a1 = Address(email_address='a1', user=u1)
        self._test_attribute(a1, 'user', u2)

    def test_composite_property_flag(self):
        Order, orders = self.classes.Order, self.tables.orders

        class MyComposite(object):

            def __init__(self, description, isopen):
                self.description = description
                self.isopen = isopen

            def __composite_values__(self):
                return [self.description, self.isopen]

            def __eq__(self, other):
                return isinstance(other, MyComposite) and \
                    other.description == self.description
        mapper(Order, orders, properties={
            'composite': composite(
                MyComposite,
                orders.c.description,
                orders.c.isopen,
                active_history=True)
        })
        o1 = Order(composite=MyComposite('foo', 1))
        self._test_attribute(o1, "composite", MyComposite('bar', 1))


class RelationDeprecationTest(fixtures.MappedTest):

    """test usage of the old 'relation' function."""

    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table('users_table', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(64)))

        Table('addresses_table', metadata,
              Column('id', Integer, primary_key=True),
              Column('user_id', Integer, ForeignKey('users_table.id')),
              Column('email_address', String(128)),
              Column('purpose', String(16)),
              Column('bounces', Integer, default=0))

    @classmethod
    def setup_classes(cls):
        class User(cls.Basic):
            pass

        class Address(cls.Basic):
            pass

    @classmethod
    def fixtures(cls):
        return dict(
            users_table=(
                ('id', 'name'),
                (1, 'jack'),
                (2, 'ed'),
                (3, 'fred'),
                (4, 'chuck')),

            addresses_table=(
                ('id', 'user_id', 'email_address', 'purpose', 'bounces'),
                (1, 1, 'jack@jack.home', 'Personal', 0),
                (2, 1, 'jack@jack.bizz', 'Work', 1),
                (3, 2, 'ed@foo.bar', 'Personal', 0),
                (4, 3, 'fred@the.fred', 'Personal', 10)))

    def test_relation(self):
        addresses_table, User, users_table, Address = (
            self.tables.addresses_table,
            self.classes.User,
            self.tables.users_table,
            self.classes.Address)

        mapper(User, users_table, properties=dict(
            addresses=relation(Address, backref='user'),
        ))
        mapper(Address, addresses_table)

        session = create_session()

        session.query(User).filter(User.addresses.any(
            Address.email_address == 'ed@foo.bar')).one()
