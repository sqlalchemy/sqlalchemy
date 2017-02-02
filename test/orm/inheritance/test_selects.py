from sqlalchemy import String, Integer, ForeignKey, select
from sqlalchemy.orm import mapper, Session

from sqlalchemy import testing

from sqlalchemy.testing import fixtures, eq_
from sqlalchemy.testing.schema import Table, Column


class InheritingSelectablesTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        foo = Table('foo', metadata,
                    Column('a', String(30), primary_key=1),
                    Column('b', String(30), nullable=0))

        cls.tables.bar = foo.select(foo.c.b == 'bar').alias('bar')
        cls.tables.baz = foo.select(foo.c.b == 'baz').alias('baz')

    def test_load(self):
        foo, bar, baz = self.tables.foo, self.tables.bar, self.tables.baz
        # TODO: add persistence test also
        testing.db.execute(foo.insert(), a='not bar', b='baz')
        testing.db.execute(foo.insert(), a='also not bar', b='baz')
        testing.db.execute(foo.insert(), a='i am bar', b='bar')
        testing.db.execute(foo.insert(), a='also bar', b='bar')

        class Foo(fixtures.ComparableEntity):
            pass

        class Bar(Foo):
            pass

        class Baz(Foo):
            pass

        mapper(Foo, foo, polymorphic_on=foo.c.b)

        mapper(Baz, baz,
               with_polymorphic=('*',
                                 foo.join(baz, foo.c.b == 'baz').alias('baz')),
               inherits=Foo, inherit_condition=(foo.c.a == baz.c.a),
               inherit_foreign_keys=[baz.c.a],
               polymorphic_identity='baz')

        mapper(Bar, bar,
               with_polymorphic=('*',
                                 foo.join(bar, foo.c.b == 'bar').alias('bar')),
               inherits=Foo, inherit_condition=(foo.c.a == bar.c.a),
               inherit_foreign_keys=[bar.c.a],
               polymorphic_identity='bar')

        s = Session()

        assert [Baz(), Baz(), Bar(), Bar()] == s.query(
            Foo).order_by(Foo.b.desc()).all()
        assert [Bar(), Bar()] == s.query(Bar).all()


class JoinFromSelectPersistenceTest(fixtures.MappedTest):
    """test for [ticket:2885]"""

    @classmethod
    def define_tables(cls, metadata):
        Table('base', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('type', String(50)))
        Table('child', metadata,
              # 1. name of column must be different, so that we rely on
              # mapper._table_to_equated to link the two cols
              Column('child_id', Integer, ForeignKey(
                  'base.id'), primary_key=True),
              Column('name', String(50)))

    @classmethod
    def setup_classes(cls):
        class Base(cls.Comparable):
            pass

        class Child(Base):
            pass

    def test_map_to_select(self):
        Base, Child = self.classes.Base, self.classes.Child
        base, child = self.tables.base, self.tables.child

        base_select = select([base]).alias()
        mapper(Base, base_select, polymorphic_on=base_select.c.type,
               polymorphic_identity='base')
        mapper(Child, child, inherits=Base,
               polymorphic_identity='child')

        sess = Session()

        # 2. use an id other than "1" here so can't rely on
        # the two inserts having the same id
        c1 = Child(id=12, name='c1')
        sess.add(c1)

        sess.commit()
        sess.close()

        c1 = sess.query(Child).one()
        eq_(c1.name, 'c1')
