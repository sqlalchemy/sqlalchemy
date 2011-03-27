from sqlalchemy import *
from sqlalchemy.orm import *

from test.lib import testing

from test.lib import fixtures

class InheritingSelectablesTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global foo, bar, baz
        foo = Table('foo', metadata,
                    Column('a', String(30), primary_key=1),
                    Column('b', String(30), nullable=0))

        bar = foo.select(foo.c.b == 'bar').alias('bar')
        baz = foo.select(foo.c.b == 'baz').alias('baz')

    def test_load(self):
        # TODO: add persistence test also
        testing.db.execute(foo.insert(), a='not bar', b='baz')
        testing.db.execute(foo.insert(), a='also not bar', b='baz')
        testing.db.execute(foo.insert(), a='i am bar', b='bar')
        testing.db.execute(foo.insert(), a='also bar', b='bar')

        class Foo(fixtures.ComparableEntity): pass
        class Bar(Foo): pass
        class Baz(Foo): pass

        mapper(Foo, foo, polymorphic_on=foo.c.b)

        mapper(Baz, baz,
                    with_polymorphic=('*', foo.join(baz, foo.c.b=='baz').alias('baz')),
                    inherits=Foo,
                    inherit_condition=(foo.c.a==baz.c.a),
                    inherit_foreign_keys=[baz.c.a],
                    polymorphic_identity='baz')

        mapper(Bar, bar,
                    with_polymorphic=('*', foo.join(bar, foo.c.b=='bar').alias('bar')),
                    inherits=Foo,
                    inherit_condition=(foo.c.a==bar.c.a),
                    inherit_foreign_keys=[bar.c.a],
                    polymorphic_identity='bar')

        s = sessionmaker(bind=testing.db)()

        assert [Baz(), Baz(), Bar(), Bar()] == s.query(Foo).order_by(Foo.b.desc()).all()
        assert [Bar(), Bar()] == s.query(Bar).all()

