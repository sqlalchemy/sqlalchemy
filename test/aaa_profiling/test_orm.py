from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.orm import mapper, relationship, \
    sessionmaker, Session, defer
from sqlalchemy import testing
from sqlalchemy.testing import profiling
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.schema import Table, Column


class MergeTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'parent',
            metadata,
            Column(
                'id',
                Integer,
                primary_key=True,
                test_needs_autoincrement=True),
            Column(
                'data',
                String(20)))
        Table(
            'child', metadata,
            Column(
                'id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column(
                'data', String(20)),
            Column(
                'parent_id', Integer, ForeignKey('parent.id'), nullable=False))

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

        class Child(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Child, Parent, parent, child = (cls.classes.Child,
                                        cls.classes.Parent,
                                        cls.tables.parent,
                                        cls.tables.child)

        mapper(
            Parent,
            parent,
            properties={
                'children': relationship(
                    Child,
                    backref='parent')})
        mapper(Child, child)

    @classmethod
    def insert_data(cls):
        parent, child = cls.tables.parent, cls.tables.child

        parent.insert().execute({'id': 1, 'data': 'p1'})
        child.insert().execute({'id': 1, 'data': 'p1c1', 'parent_id': 1})

    def test_merge_no_load(self):
        Parent = self.classes.Parent

        sess = sessionmaker()()
        sess2 = sessionmaker()()
        p1 = sess.query(Parent).get(1)
        p1.children

        # down from 185 on this this is a small slice of a usually
        # bigger operation so using a small variance

        @profiling.function_call_count(variance=0.10)
        def go1():
            return sess2.merge(p1, load=False)
        p2 = go1()

        # third call, merge object already present. almost no calls.

        @profiling.function_call_count(variance=0.10)
        def go2():
            return sess2.merge(p2, load=False)
        go2()

    def test_merge_load(self):
        Parent = self.classes.Parent

        sess = sessionmaker()()
        sess2 = sessionmaker()()
        p1 = sess.query(Parent).get(1)
        p1.children

        # preloading of collection took this down from 1728 to 1192
        # using sqlite3 the C extension took it back up to approx. 1257
        # (py2.6)

        @profiling.function_call_count()
        def go():
            sess2.merge(p1)
        go()

        # one more time, count the SQL

        def go2():
            sess2.merge(p1)
        sess2 = sessionmaker(testing.db)()
        self.assert_sql_count(testing.db, go2, 2)


class LoadManyToOneFromIdentityTest(fixtures.MappedTest):

    """test overhead associated with many-to-one fetches.

    Prior to the refactor of LoadLazyAttribute and
    query._get(), the load from identity map took 2x
    as many calls (65K calls here instead of around 33K)
    to load 1000 related objects from the identity map.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table('parent', metadata,
              Column('id', Integer, primary_key=True),
              Column('data', String(20)),
              Column('child_id', Integer, ForeignKey('child.id'))
              )

        Table('child', metadata,
              Column('id', Integer, primary_key=True),
              Column('data', String(20))
              )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

        class Child(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Child, Parent, parent, child = (cls.classes.Child,
                                        cls.classes.Parent,
                                        cls.tables.parent,
                                        cls.tables.child)

        mapper(Parent, parent, properties={
            'child': relationship(Child)})
        mapper(Child, child)

    @classmethod
    def insert_data(cls):
        parent, child = cls.tables.parent, cls.tables.child

        child.insert().execute([
            {'id': i, 'data': 'c%d' % i}
            for i in range(1, 251)
        ])
        parent.insert().execute([
            {
                'id': i,
                'data': 'p%dc%d' % (i, (i % 250) + 1),
                'child_id': (i % 250) + 1
            }
            for i in range(1, 1000)
        ])

    def test_many_to_one_load_no_identity(self):
        Parent = self.classes.Parent

        sess = Session()
        parents = sess.query(Parent).all()

        @profiling.function_call_count(variance=.2)
        def go():
            for p in parents:
                p.child
        go()

    def test_many_to_one_load_identity(self):
        Parent, Child = self.classes.Parent, self.classes.Child

        sess = Session()
        parents = sess.query(Parent).all()
        children = sess.query(Child).all()
        children  # strong reference

        @profiling.function_call_count()
        def go():
            for p in parents:
                p.child
        go()


class MergeBackrefsTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('a', metadata,
              Column('id', Integer, primary_key=True),
              Column('c_id', Integer, ForeignKey('c.id'))
              )
        Table('b', metadata,
              Column('id', Integer, primary_key=True),
              Column('a_id', Integer, ForeignKey('a.id'))
              )
        Table('c', metadata,
              Column('id', Integer, primary_key=True),
              )
        Table('d', metadata,
              Column('id', Integer, primary_key=True),
              Column('a_id', Integer, ForeignKey('a.id'))
              )

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
        A, B, C, D = cls.classes.A, cls.classes.B, \
            cls.classes.C, cls.classes.D
        a, b, c, d = cls.tables.a, cls.tables.b, \
            cls.tables.c, cls.tables.d
        mapper(A, a, properties={
            'bs': relationship(B, backref='a'),
            'c': relationship(C, backref='as'),
            'ds': relationship(D, backref='a'),
        })
        mapper(B, b)
        mapper(C, c)
        mapper(D, d)

    @classmethod
    def insert_data(cls):
        A, B, C, D = cls.classes.A, cls.classes.B, \
            cls.classes.C, cls.classes.D
        s = Session()
        s.add_all([
            A(id=i,
                bs=[B(id=(i * 5) + j) for j in range(1, 5)],
                c=C(id=i),
                ds=[D(id=(i * 5) + j) for j in range(1, 5)]
              )
            for i in range(1, 5)
        ])
        s.commit()

    @profiling.function_call_count(variance=.10)
    def test_merge_pending_with_all_pks(self):
        A, B, C, D = self.classes.A, self.classes.B, \
            self.classes.C, self.classes.D
        s = Session()
        for a in [
            A(id=i,
                bs=[B(id=(i * 5) + j) for j in range(1, 5)],
                c=C(id=i),
                ds=[D(id=(i * 5) + j) for j in range(1, 5)]
              )
            for i in range(1, 5)
        ]:
            s.merge(a)


class DeferOptionsTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('a', metadata,
              Column('id', Integer, primary_key=True),
              Column('x', String(5)),
              Column('y', String(5)),
              Column('z', String(5)),
              Column('q', String(5)),
              Column('p', String(5)),
              Column('r', String(5)),
              )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        A = cls.classes.A
        a = cls.tables.a
        mapper(A, a)

    @classmethod
    def insert_data(cls):
        A = cls.classes.A
        s = Session()
        s.add_all([
            A(id=i,
                **dict((letter, "%s%d" % (letter, i)) for letter in
                       ['x', 'y', 'z', 'p', 'q', 'r'])
              ) for i in range(1, 1001)
        ])
        s.commit()

    @profiling.function_call_count(variance=.10)
    def test_baseline(self):
        # as of [ticket:2778], this is at 39025
        A = self.classes.A
        s = Session()
        s.query(A).all()

    @profiling.function_call_count(variance=.10)
    def test_defer_many_cols(self):
        # with [ticket:2778], this goes from 50805 to 32817,
        # as it should be fewer function calls than the baseline
        A = self.classes.A
        s = Session()
        s.query(A).options(
            *[defer(letter) for letter in ['x', 'y', 'z', 'p', 'q', 'r']]).\
            all()


class AttributeOverheadTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'parent',
            metadata,
            Column(
                'id',
                Integer,
                primary_key=True,
                test_needs_autoincrement=True),
            Column(
                'data',
                String(20)))
        Table(
            'child', metadata,
            Column(
                'id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column(
                'data', String(20)), Column(
                'parent_id', Integer, ForeignKey('parent.id'), nullable=False))

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

        class Child(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Child, Parent, parent, child = (cls.classes.Child,
                                        cls.classes.Parent,
                                        cls.tables.parent,
                                        cls.tables.child)

        mapper(
            Parent,
            parent,
            properties={
                'children': relationship(
                    Child,
                    backref='parent')})
        mapper(Child, child)

    def test_attribute_set(self):
        Parent, Child = self.classes.Parent, self.classes.Child
        p1 = Parent()
        c1 = Child()

        @profiling.function_call_count()
        def go():
            for i in range(30):
                c1.parent = p1
                c1.parent = None
                c1.parent = p1
                del c1.parent
        go()

    def test_collection_append_remove(self):
        Parent, Child = self.classes.Parent, self.classes.Child
        p1 = Parent()
        children = [Child() for i in range(100)]

        @profiling.function_call_count()
        def go():
            for child in children:
                p1.children.append(child)
            for child in children:
                p1.children.remove(child)
        go()


class SessionTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            'parent',
            metadata,
            Column('id', Integer,
                   primary_key=True, test_needs_autoincrement=True),
            Column('data', String(20)))
        Table(
            'child', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column(
                'data', String(20)), Column(
                'parent_id', Integer, ForeignKey('parent.id'), nullable=False))

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

        class Child(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Child, Parent, parent, child = (cls.classes.Child,
                                        cls.classes.Parent,
                                        cls.tables.parent,
                                        cls.tables.child)

        mapper(
            Parent, parent, properties={
                'children': relationship(
                    Child,
                    backref='parent')})
        mapper(Child, child)

    def test_expire_lots(self):
        Parent, Child = self.classes.Parent, self.classes.Child
        obj = [Parent(
            children=[Child() for j in range(10)]) for i in range(10)]

        sess = Session()
        sess.add_all(obj)
        sess.flush()

        @profiling.function_call_count()
        def go():
            sess.expire_all()
        go()


class QueryTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            'parent',
            metadata,
            Column('id', Integer,
                   primary_key=True, test_needs_autoincrement=True),
            Column('data1', String(20)),
            Column('data2', String(20)),
            Column('data3', String(20)),
            Column('data4', String(20)),
        )

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Parent = cls.classes.Parent
        parent = cls.tables.parent

        mapper(Parent, parent)

    def _fixture(self):
        Parent = self.classes.Parent
        sess = Session()
        sess.add_all([
            Parent(data1='d1', data2='d2', data3='d3', data4='d4')
            for i in range(10)
        ])
        sess.commit()
        sess.close()

    def test_query_cols(self):
        Parent = self.classes.Parent
        self._fixture()
        sess = Session()

        @profiling.function_call_count()
        def go():
            for i in range(10):
                q = sess.query(
                    Parent.data1, Parent.data2, Parent.data3, Parent.data4
                )

                q.all()

        go()
