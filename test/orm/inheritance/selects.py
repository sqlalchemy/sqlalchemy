import testbase
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *

# TODO: refactor "fixtures" to be part of testlib, so Base is globally available
_recursion_stack = util.Set()
class Base(object):
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])
    
    def __ne__(self, other):
        return not self.__eq__(other)
        
    def __eq__(self, other):
        """'passively' compare this object to another.
        
        only look at attributes that are present on the source object.
        
        """
        if self in _recursion_stack:
            return True
        _recursion_stack.add(self)
        try:
            # use __dict__ to avoid instrumented properties
            for attr in self.__dict__.keys():
                if attr[0] == '_':
                    continue
                value = getattr(self, attr)
                if hasattr(value, '__iter__') and not isinstance(value, basestring):
                    try:
                        # catch AttributeError so that lazy loaders trigger
                        otherattr = getattr(other, attr)
                    except AttributeError:
                        return False
                    if len(value) != len(getattr(other, attr)):
                       return False
                    for (us, them) in zip(value, getattr(other, attr)):
                        if us != them:
                            return False
                    else:
                        continue
                else:
                    if value is not None:
                        print "KEY", attr, "COMPARING", value, "TO", getattr(other, attr, None)
                        if value != getattr(other, attr, None):
                            return False
            else:
                return True
        finally:
            _recursion_stack.remove(self)

class InheritingSelectablesTest(ORMTest):
    def define_tables(self, metadata):
        global foo, bar, baz
        foo = Table('foo', metadata,
                    Column('a', String(30), primary_key=1),
                    Column('b', String(30), nullable=0))

        bar = foo.select(foo.c.b == 'bar').alias('bar')
        baz = foo.select(foo.c.b == 'baz').alias('baz')

    def test_load(self):
        # TODO: add persistence test also
        testbase.db.execute(foo.insert(), a='not bar', b='baz')
        testbase.db.execute(foo.insert(), a='also not bar', b='baz')
        testbase.db.execute(foo.insert(), a='i am bar', b='bar')
        testbase.db.execute(foo.insert(), a='also bar', b='bar')

        class Foo(Base): pass
        class Bar(Foo): pass
        class Baz(Foo): pass

        mapper(Foo, foo, polymorphic_on=foo.c.b)

        mapper(Baz, baz, 
                    select_table=foo.join(baz, foo.c.b=='baz').alias('baz'),
                    inherits=Foo,
                    inherit_condition=(foo.c.a==baz.c.a),
                    inherit_foreign_keys=[baz.c.a],
                    polymorphic_identity='baz')

        mapper(Bar, bar,
                    select_table=foo.join(bar, foo.c.b=='bar').alias('bar'),
                    inherits=Foo, 
                    inherit_condition=(foo.c.a==bar.c.a),
                    inherit_foreign_keys=[bar.c.a],
                    polymorphic_identity='bar')

        s = sessionmaker(bind=testbase.db)()

        assert [Baz(), Baz(), Bar(), Bar()] == s.query(Foo).all()
        assert [Bar(), Bar()] == s.query(Bar).all()

if __name__ == '__main__':
    testbase.main()
