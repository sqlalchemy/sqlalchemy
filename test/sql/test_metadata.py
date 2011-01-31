from test.lib.testing import assert_raises
from test.lib.testing import assert_raises_message
from test.lib.testing import emits_warning

import pickle
from sqlalchemy import Integer, String, UniqueConstraint, \
    CheckConstraint, ForeignKey, MetaData, Sequence, \
    ForeignKeyConstraint, ColumnDefault, Index, event,\
    events
from test.lib.schema import Table, Column
from sqlalchemy import schema, exc
import sqlalchemy as tsa
from test.lib import TestBase, ComparesTables, \
    AssertsCompiledSQL, testing, engines
from test.lib.testing import eq_

class MetaDataTest(TestBase, ComparesTables):
    def test_metadata_connect(self):
        metadata = MetaData()
        t1 = Table('table1', metadata, 
            Column('col1', Integer, primary_key=True),
            Column('col2', String(20)))
        metadata.bind = testing.db
        metadata.create_all()
        try:
            assert t1.count().scalar() == 0
        finally:
            metadata.drop_all()

    def test_metadata_contains(self):
        metadata = MetaData()
        t1 = Table('t1', metadata, Column('x', Integer))
        t2 = Table('t2', metadata, Column('x', Integer), schema='foo')
        t3 = Table('t2', MetaData(), Column('x', Integer))
        t4 = Table('t1', MetaData(), Column('x', Integer), schema='foo')

        assert "t1" in metadata
        assert "foo.t2" in metadata
        assert "t2" not in metadata
        assert "foo.t1" not in metadata
        assert t1 in metadata
        assert t2 in metadata
        assert t3 not in metadata
        assert t4 not in metadata

    def test_uninitialized_column_copy(self):
        for col in [
            Column('foo', String(), nullable=False),
            Column('baz', String(), unique=True),
            Column(Integer(), primary_key=True),
            Column('bar', Integer(), Sequence('foo_seq'), primary_key=True,
                                                            key='bar'),
            Column(Integer(), ForeignKey('bat.blah'), doc="this is a col"),
            Column('bar', Integer(), ForeignKey('bat.blah'), primary_key=True,
                                                            key='bar'),
            Column('bar', Integer(), info={'foo':'bar'}),
        ]:
            c2 = col.copy()
            for attr in ('name', 'type', 'nullable', 
                        'primary_key', 'key', 'unique', 'info',
                        'doc'):
                eq_(getattr(col, attr), getattr(c2, attr))
            eq_(len(col.foreign_keys), len(c2.foreign_keys))
            if col.default:
                eq_(c2.default.name, 'foo_seq')
            for a1, a2 in zip(col.foreign_keys, c2.foreign_keys):
                assert a1 is not a2
                eq_(a2._colspec, 'bat.blah')

    def test_uninitialized_column_copy_events(self):
        msgs = []
        def write(c, t):
            msgs.append("attach %s.%s" % (t.name, c.name))
        c1 = Column('foo', String())
        m = MetaData()
        for i in xrange(3):
            cx = c1.copy()
            # as of 0.7, these events no longer copy.  its expected
            # that listeners will be re-established from the
            # natural construction of things.
            cx._on_table_attach(write)
            t = Table('foo%d' % i, m, cx)
        eq_(msgs, ['attach foo0.foo', 'attach foo1.foo', 'attach foo2.foo'])

    def test_schema_collection_add(self):
        metadata = MetaData()

        t1 = Table('t1', metadata, Column('x', Integer), schema='foo')
        t2 = Table('t2', metadata, Column('x', Integer), schema='bar')
        t3 = Table('t3', metadata, Column('x', Integer))

        eq_(metadata._schemas, set(['foo', 'bar']))
        eq_(len(metadata.tables), 3)

    def test_schema_collection_remove(self):
        metadata = MetaData()

        t1 = Table('t1', metadata, Column('x', Integer), schema='foo')
        t2 = Table('t2', metadata, Column('x', Integer), schema='bar')
        t3 = Table('t3', metadata, Column('x', Integer), schema='bar')

        metadata.remove(t3)
        eq_(metadata._schemas, set(['foo', 'bar']))
        eq_(len(metadata.tables), 2)

        metadata.remove(t1)
        eq_(metadata._schemas, set(['bar']))
        eq_(len(metadata.tables), 1)

    def test_schema_collection_remove_all(self):
        metadata = MetaData()

        t1 = Table('t1', metadata, Column('x', Integer), schema='foo')
        t2 = Table('t2', metadata, Column('x', Integer), schema='bar')

        metadata.clear()
        eq_(metadata._schemas, set())
        eq_(len(metadata.tables), 0)

    def test_metadata_tables_immutable(self):
        metadata = MetaData()

        t1 = Table('t1', metadata, Column('x', Integer))
        assert 't1' in metadata.tables

        assert_raises(
            TypeError,
            lambda: metadata.tables.pop('t1')
        )

    @testing.provide_metadata
    def test_dupe_tables(self):
        t1 = Table('table1', metadata, 
            Column('col1', Integer, primary_key=True),
            Column('col2', String(20)))

        metadata.create_all()
        t1 = Table('table1', metadata, autoload=True)
        def go():
            t2 = Table('table1', metadata, 
                Column('col1', Integer, primary_key=True),
                Column('col2', String(20)))
        assert_raises_message(
            tsa.exc.InvalidRequestError,
            "Table 'table1' is already defined for this "\
            "MetaData instance.  Specify 'useexisting=True' "\
            "to redefine options and columns on an existing "\
            "Table object.",
            go
        )

    def test_fk_copy(self):
        c1 = Column('foo', Integer)
        c2 = Column('bar', Integer)
        m = MetaData()
        t1 = Table('t', m, c1, c2)

        kw = dict(onupdate="X", 
                        ondelete="Y", use_alter=True, name='f1',
                        deferrable="Z", initially="Q", link_to_name=True)

        fk1 = ForeignKey(c1, **kw) 
        fk2 = ForeignKeyConstraint((c1,), (c2,), **kw)

        t1.append_constraint(fk2)
        fk1c = fk1.copy()
        fk2c = fk2.copy()

        for k in kw:
            eq_(getattr(fk1c, k), kw[k])
            eq_(getattr(fk2c, k), kw[k])

    def test_check_constraint_copy(self):
        r = lambda x: x
        c = CheckConstraint("foo bar", 
                            name='name', 
                            initially=True, 
                            deferrable=True, 
                            _create_rule = r)
        c2 = c.copy()
        eq_(c2.name, 'name')
        eq_(str(c2.sqltext), "foo bar")
        eq_(c2.initially, True)
        eq_(c2.deferrable, True)
        assert c2._create_rule is r

    def test_fk_construct(self):
        c1 = Column('foo', Integer)
        c2 = Column('bar', Integer)
        m = MetaData()
        t1 = Table('t', m, c1, c2)
        fk1 = ForeignKeyConstraint(('foo', ), ('bar', ), table=t1)
        assert fk1 in t1.constraints

    @testing.exclude('mysql', '<', (4, 1, 1), 'early types are squirrely')
    def test_to_metadata(self):
        meta = MetaData()

        table = Table('mytable', meta,
            Column('myid', Integer, Sequence('foo_id_seq'), primary_key=True),
            Column('name', String(40), nullable=True),
            Column('foo', String(40), nullable=False, server_default='x',
                                                        server_onupdate='q'),
            Column('bar', String(40), nullable=False, default='y',
                                                        onupdate='z'),
            Column('description', String(30),
                                    CheckConstraint("description='hi'")),
            UniqueConstraint('name'),
            test_needs_fk=True,
        )

        table2 = Table('othertable', meta,
            Column('id', Integer, Sequence('foo_seq'), primary_key=True),
            Column('myid', Integer, 
                        ForeignKey('mytable.myid'),
                    ),
            test_needs_fk=True,
            )

        def test_to_metadata():
            meta2 = MetaData()
            table_c = table.tometadata(meta2)
            table2_c = table2.tometadata(meta2)
            return (table_c, table2_c)

        def test_pickle():
            meta.bind = testing.db
            meta2 = pickle.loads(pickle.dumps(meta))
            assert meta2.bind is None
            meta3 = pickle.loads(pickle.dumps(meta2))
            return (meta2.tables['mytable'], meta2.tables['othertable'])

        def test_pickle_via_reflect():
            # this is the most common use case, pickling the results of a
            # database reflection
            meta2 = MetaData(bind=testing.db)
            t1 = Table('mytable', meta2, autoload=True)
            t2 = Table('othertable', meta2, autoload=True)
            meta3 = pickle.loads(pickle.dumps(meta2))
            assert meta3.bind is None
            assert meta3.tables['mytable'] is not t1
            return (meta3.tables['mytable'], meta3.tables['othertable'])

        meta.create_all(testing.db)
        try:
            for test, has_constraints, reflect in (test_to_metadata,
                    True, False), (test_pickle, True, False), \
                (test_pickle_via_reflect, False, True):
                table_c, table2_c = test()
                self.assert_tables_equal(table, table_c)
                self.assert_tables_equal(table2, table2_c)
                assert table is not table_c
                assert table.primary_key is not table_c.primary_key
                assert list(table2_c.c.myid.foreign_keys)[0].column \
                    is table_c.c.myid
                assert list(table2_c.c.myid.foreign_keys)[0].column \
                    is not table.c.myid
                assert 'x' in str(table_c.c.foo.server_default.arg)
                if not reflect:
                    assert isinstance(table_c.c.myid.default, Sequence)
                    assert str(table_c.c.foo.server_onupdate.arg) == 'q'
                    assert str(table_c.c.bar.default.arg) == 'y'
                    assert getattr(table_c.c.bar.onupdate.arg, 'arg',
                                   table_c.c.bar.onupdate.arg) == 'z'
                    assert isinstance(table2_c.c.id.default, Sequence)

                # constraints dont get reflected for any dialect right
                # now

                if has_constraints:
                    for c in table_c.c.description.constraints:
                        if isinstance(c, CheckConstraint):
                            break
                    else:
                        assert False
                    assert str(c.sqltext) == "description='hi'"
                    for c in table_c.constraints:
                        if isinstance(c, UniqueConstraint):
                            break
                    else:
                        assert False
                    assert c.columns.contains_column(table_c.c.name)
                    assert not c.columns.contains_column(table.c.name)
        finally:
            meta.drop_all(testing.db)

    def test_tometadata_with_schema(self):
        meta = MetaData()

        table = Table('mytable', meta,
            Column('myid', Integer, primary_key=True),
            Column('name', String(40), nullable=True),
            Column('description', String(30),
                            CheckConstraint("description='hi'")),
            UniqueConstraint('name'),
            test_needs_fk=True,
        )

        table2 = Table('othertable', meta,
            Column('id', Integer, primary_key=True),
            Column('myid', Integer, ForeignKey('mytable.myid')),
            test_needs_fk=True,
            )

        meta2 = MetaData()
        table_c = table.tometadata(meta2, schema='someschema')
        table2_c = table2.tometadata(meta2, schema='someschema')

        eq_(str(table_c.join(table2_c).onclause), str(table_c.c.myid
            == table2_c.c.myid))
        eq_(str(table_c.join(table2_c).onclause),
            'someschema.mytable.myid = someschema.othertable.myid')

    def test_tometadata_kwargs(self):
        meta = MetaData()

        table = Table('mytable', meta,
            Column('myid', Integer, primary_key=True),
            mysql_engine='InnoDB',
        )

        meta2 = MetaData()
        table_c = table.tometadata(meta2)

        eq_(table.kwargs,table_c.kwargs)

    def test_tometadata_indexes(self):
        meta = MetaData()

        table = Table('mytable', meta,
            Column('id', Integer, primary_key=True),
            Column('data1', Integer, index=True),
            Column('data2', Integer),
        )
        Index('multi',table.c.data1,table.c.data2),

        meta2 = MetaData()
        table_c = table.tometadata(meta2)

        def _get_key(i):
            return [i.name,i.unique] + \
                    sorted(i.kwargs.items()) + \
                    i.columns.keys()

        eq_(
            sorted([_get_key(i) for i in table.indexes]),
            sorted([_get_key(i) for i in table_c.indexes])
        )

    @emits_warning("Table '.+' already exists within the given MetaData")
    def test_tometadata_already_there(self):

        meta1 = MetaData()
        table1 = Table('mytable', meta1,
            Column('myid', Integer, primary_key=True),
        )
        meta2 = MetaData()
        table2 = Table('mytable', meta2,
            Column('yourid', Integer, primary_key=True),
        )

        meta3 = MetaData()

        table_c = table1.tometadata(meta2)
        table_d = table2.tometadata(meta2)

        # d'oh!
        assert table_c is table_d

    def test_tometadata_default_schema(self):
        meta = MetaData()

        table = Table('mytable', meta,
            Column('myid', Integer, primary_key=True),
            Column('name', String(40), nullable=True),
            Column('description', String(30),
                        CheckConstraint("description='hi'")),
            UniqueConstraint('name'),
            test_needs_fk=True,
            schema='myschema',
        )

        table2 = Table('othertable', meta,
            Column('id', Integer, primary_key=True),
            Column('myid', Integer, ForeignKey('myschema.mytable.myid')),
            test_needs_fk=True,
            schema='myschema',
            )

        meta2 = MetaData()
        table_c = table.tometadata(meta2)
        table2_c = table2.tometadata(meta2)

        eq_(str(table_c.join(table2_c).onclause), str(table_c.c.myid
            == table2_c.c.myid))
        eq_(str(table_c.join(table2_c).onclause),
            'myschema.mytable.myid = myschema.othertable.myid')

    def test_manual_dependencies(self):
        meta = MetaData()
        a = Table('a', meta, Column('foo', Integer))
        b = Table('b', meta, Column('foo', Integer))
        c = Table('c', meta, Column('foo', Integer))
        d = Table('d', meta, Column('foo', Integer))
        e = Table('e', meta, Column('foo', Integer))

        e.add_is_dependent_on(c)
        a.add_is_dependent_on(b)
        b.add_is_dependent_on(d)
        e.add_is_dependent_on(b)
        c.add_is_dependent_on(a)
        eq_(
            meta.sorted_tables,
            [d, b, a, c, e]
        )

    def test_tometadata_strip_schema(self):
        meta = MetaData()

        table = Table('mytable', meta,
            Column('myid', Integer, primary_key=True),
            Column('name', String(40), nullable=True),
            Column('description', String(30),
                        CheckConstraint("description='hi'")),
            UniqueConstraint('name'),
            test_needs_fk=True,
        )

        table2 = Table('othertable', meta,
            Column('id', Integer, primary_key=True),
            Column('myid', Integer, ForeignKey('mytable.myid')),
            test_needs_fk=True,
            )

        meta2 = MetaData()
        table_c = table.tometadata(meta2, schema=None)
        table2_c = table2.tometadata(meta2, schema=None)

        eq_(str(table_c.join(table2_c).onclause), str(table_c.c.myid
            == table2_c.c.myid))
        eq_(str(table_c.join(table2_c).onclause),
            'mytable.myid = othertable.myid')

    def test_nonexistent(self):
        assert_raises(tsa.exc.NoSuchTableError, Table,
                          'fake_table',
                          MetaData(testing.db), autoload=True)


class TableTest(TestBase, AssertsCompiledSQL):
    def test_prefixes(self):
        table1 = Table("temporary_table_1", MetaData(),
                      Column("col1", Integer),
                      prefixes = ["TEMPORARY"])

        self.assert_compile(
            schema.CreateTable(table1), 
            "CREATE TEMPORARY TABLE temporary_table_1 (col1 INTEGER)"
        )

        table2 = Table("temporary_table_2", MetaData(),
                      Column("col1", Integer),
                      prefixes = ["VIRTUAL"])
        self.assert_compile(
          schema.CreateTable(table2), 
          "CREATE VIRTUAL TABLE temporary_table_2 (col1 INTEGER)"
        )

    def test_table_info(self):
        metadata = MetaData()
        t1 = Table('foo', metadata, info={'x':'y'})
        t2 = Table('bar', metadata, info={})
        t3 = Table('bat', metadata)
        assert t1.info == {'x':'y'}
        assert t2.info == {}
        assert t3.info == {}
        for t in (t1, t2, t3):
            t.info['bar'] = 'zip'
            assert t.info['bar'] == 'zip'

    def test_c_immutable(self):
        m = MetaData()
        t1 = Table('t', m, Column('x', Integer), Column('y', Integer))
        assert_raises(
            TypeError,
            t1.c.extend, [Column('z', Integer)]
        )

        def assign():
            t1.c['z'] = Column('z', Integer)
        assert_raises(
            TypeError,
            assign
        )

        def assign():
            t1.c.z = Column('z', Integer)
        assert_raises(
            TypeError,
            assign
        )


class ColumnDefinitionTest(TestBase):
    """Test Column() construction."""

    # flesh this out with explicit coverage...

    def columns(self):
        return [ Column(Integer),
                 Column('b', Integer),
                 Column(Integer),
                 Column('d', Integer),
                 Column(Integer, name='e'),
                 Column(type_=Integer),
                 Column(Integer()),
                 Column('h', Integer()),
                 Column(type_=Integer()) ]

    def test_basic(self):
        c = self.columns()

        for i, v in ((0, 'a'), (2, 'c'), (5, 'f'), (6, 'g'), (8, 'i')):
            c[i].name = v
            c[i].key = v
        del i, v

        tbl = Table('table', MetaData(), *c)

        for i, col in enumerate(tbl.c):
            assert col.name == c[i].name

    def test_incomplete(self):
        c = self.columns()

        assert_raises(exc.ArgumentError, Table, 't', MetaData(), *c)

    def test_incomplete_key(self):
        c = Column(Integer)
        assert c.name is None
        assert c.key is None

        c.name = 'named'
        t = Table('t', MetaData(), c)

        assert c.name == 'named'
        assert c.name == c.key


    def test_bogus(self):
        assert_raises(exc.ArgumentError, Column, 'foo', name='bar')
        assert_raises(exc.ArgumentError, Column, 'foo', Integer,
                          type_=Integer())


class ColumnOptionsTest(TestBase):

    def test_default_generators(self):
        g1, g2 = Sequence('foo_id_seq'), ColumnDefault('f5')
        assert Column(String, default=g1).default is g1
        assert Column(String, onupdate=g1).onupdate is g1
        assert Column(String, default=g2).default is g2
        assert Column(String, onupdate=g2).onupdate is g2

    def test_type_required(self):
        assert_raises(exc.ArgumentError, Column)
        assert_raises(exc.ArgumentError, Column, "foo")
        assert_raises(exc.ArgumentError, Column, default="foo")
        assert_raises(exc.ArgumentError, Column, Sequence("a"))
        assert_raises(exc.ArgumentError, Column, "foo", default="foo")
        assert_raises(exc.ArgumentError, Column, "foo", Sequence("a"))
        Column(ForeignKey('bar.id'))
        Column("foo", ForeignKey('bar.id'))
        Column(ForeignKey('bar.id'), default="foo")
        Column(ForeignKey('bar.id'), Sequence("a"))
        Column("foo", ForeignKey('bar.id'), default="foo")
        Column("foo", ForeignKey('bar.id'), Sequence("a"))

    def test_column_info(self):

        c1 = Column('foo', String, info={'x':'y'})
        c2 = Column('bar', String, info={})
        c3 = Column('bat', String)
        assert c1.info == {'x':'y'}
        assert c2.info == {}
        assert c3.info == {}

        for c in (c1, c2, c3):
            c.info['bar'] = 'zip'
            assert c.info['bar'] == 'zip'


class CatchAllEventsTest(TestBase):

    def teardown(self):
        events.SchemaEventTarget.dispatch._clear()

    def test_all_events(self):
        canary = []
        def before_attach(obj, parent):
            canary.append("%s->%s" % (obj.__class__.__name__, parent.__class__.__name__))

        def after_attach(obj, parent):
            canary.append("%s->%s" % (obj, parent))

        event.listen(events.SchemaEventTarget, "before_parent_attach", before_attach)
        event.listen(events.SchemaEventTarget, "after_parent_attach", after_attach)

        m = MetaData()
        t1 = Table('t1', m, 
            Column('id', Integer, Sequence('foo_id'), primary_key=True),
            Column('bar', String, ForeignKey('t2.id'))
        )
        t2 = Table('t2', m,
            Column('id', Integer, primary_key=True),
        )

        # TODO: test more conditions here, constraints, defaults, etc.
        eq_(
            canary,
            [
                'Sequence->Column', 
                "Sequence('foo_id', start=None, increment=None, optional=False)->id", 
                'ForeignKey->Column', 
                "ForeignKey('t2.id')->bar", 
                'Table->MetaData', 
                'Column->Table', 't1.id->t1', 
                'Column->Table', 't1.bar->t1', 
                'ForeignKeyConstraint->Table', 
                'ForeignKeyConstraint()->t1', 
                't1->MetaData(None)', 
                'Table->MetaData', 'Column->Table', 
                't2.id->t2', 't2->MetaData(None)']
        )

