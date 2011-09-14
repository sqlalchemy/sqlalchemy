from test.lib.testing import eq_, assert_raises
import copy
import pickle

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.orm.collections import collection, attribute_mapped_collection
from sqlalchemy.ext.associationproxy import *
from sqlalchemy.ext.associationproxy import _AssociationList
from test.lib import *
from test.lib.testing import assert_raises_message
from test.lib.util import gc_collect
from sqlalchemy.sql import not_
from test.lib import fixtures


class DictCollection(dict):
    @collection.appender
    def append(self, obj):
        self[obj.foo] = obj
    @collection.remover
    def remove(self, obj):
        del self[obj.foo]


class SetCollection(set):
    pass


class ListCollection(list):
    pass


class ObjectCollection(object):
    def __init__(self):
        self.values = list()
    @collection.appender
    def append(self, obj):
        self.values.append(obj)
    @collection.remover
    def remove(self, obj):
        self.values.remove(obj)
    def __iter__(self):
        return iter(self.values)


class _CollectionOperations(fixtures.TestBase):
    def setup(self):
        collection_class = self.collection_class

        metadata = MetaData(testing.db)

        parents_table = Table('Parent', metadata,
                              Column('id', Integer, primary_key=True,
                                     test_needs_autoincrement=True),
                              Column('name', String(128)))
        children_table = Table('Children', metadata,
                               Column('id', Integer, primary_key=True,
                                      test_needs_autoincrement=True),
                               Column('parent_id', Integer,
                                      ForeignKey('Parent.id')),
                               Column('foo', String(128)),
                               Column('name', String(128)))

        class Parent(object):
            children = association_proxy('_children', 'name')

            def __init__(self, name):
                self.name = name

        class Child(object):
            if collection_class and issubclass(collection_class, dict):
                def __init__(self, foo, name):
                    self.foo = foo
                    self.name = name
            else:
                def __init__(self, name):
                    self.name = name

        mapper(Parent, parents_table, properties={
            '_children': relationship(Child, lazy='joined',
                                  collection_class=collection_class)})
        mapper(Child, children_table)

        metadata.create_all()

        self.metadata = metadata
        self.session = create_session()
        self.Parent, self.Child = Parent, Child

    def teardown(self):
        self.metadata.drop_all()

    def roundtrip(self, obj):
        if obj not in self.session:
            self.session.add(obj)
        self.session.flush()
        id, type_ = obj.id, type(obj)
        self.session.expunge_all()
        return self.session.query(type_).get(id)

    def _test_sequence_ops(self):
        Parent, Child = self.Parent, self.Child

        p1 = Parent('P1')

        self.assert_(not p1._children)
        self.assert_(not p1.children)

        ch = Child('regular')
        p1._children.append(ch)

        self.assert_(ch in p1._children)
        self.assert_(len(p1._children) == 1)

        self.assert_(p1.children)
        self.assert_(len(p1.children) == 1)
        self.assert_(ch not in p1.children)
        self.assert_('regular' in p1.children)

        p1.children.append('proxied')

        self.assert_('proxied' in p1.children)
        self.assert_('proxied' not in p1._children)
        self.assert_(len(p1.children) == 2)
        self.assert_(len(p1._children) == 2)

        self.assert_(p1._children[0].name == 'regular')
        self.assert_(p1._children[1].name == 'proxied')

        del p1._children[1]

        self.assert_(len(p1._children) == 1)
        self.assert_(len(p1.children) == 1)
        self.assert_(p1._children[0] == ch)

        del p1.children[0]

        self.assert_(len(p1._children) == 0)
        self.assert_(len(p1.children) == 0)

        p1.children = ['a','b','c']
        self.assert_(len(p1._children) == 3)
        self.assert_(len(p1.children) == 3)

        del ch
        p1 = self.roundtrip(p1)

        self.assert_(len(p1._children) == 3)
        self.assert_(len(p1.children) == 3)

        popped = p1.children.pop()
        self.assert_(len(p1.children) == 2)
        self.assert_(popped not in p1.children)
        p1 = self.roundtrip(p1)
        self.assert_(len(p1.children) == 2)
        self.assert_(popped not in p1.children)

        p1.children[1] = 'changed-in-place'
        self.assert_(p1.children[1] == 'changed-in-place')
        inplace_id = p1._children[1].id
        p1 = self.roundtrip(p1)
        self.assert_(p1.children[1] == 'changed-in-place')
        assert p1._children[1].id == inplace_id

        p1.children.append('changed-in-place')
        self.assert_(p1.children.count('changed-in-place') == 2)

        p1.children.remove('changed-in-place')
        self.assert_(p1.children.count('changed-in-place') == 1)

        p1 = self.roundtrip(p1)
        self.assert_(p1.children.count('changed-in-place') == 1)

        p1._children = []
        self.assert_(len(p1.children) == 0)

        after = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
        p1.children = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
        self.assert_(len(p1.children) == 10)
        self.assert_([c.name for c in p1._children] == after)

        p1.children[2:6] = ['x'] * 4
        after = ['a', 'b', 'x', 'x', 'x', 'x', 'g', 'h', 'i', 'j']
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children[2:6] = ['y']
        after = ['a', 'b', 'y', 'g', 'h', 'i', 'j']
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children[2:3] = ['z'] * 4
        after = ['a', 'b', 'z', 'z', 'z', 'z', 'g', 'h', 'i', 'j']
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children[2::2] = ['O'] * 4
        after = ['a', 'b', 'O', 'z', 'O', 'z', 'O', 'h', 'O', 'j']
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        assert_raises(TypeError, set, [p1.children])

        p1.children *= 0
        after = []
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children += ['a', 'b']
        after = ['a', 'b']
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children += ['c']
        after = ['a', 'b', 'c']
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children *= 1
        after = ['a', 'b', 'c']
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children *= 2
        after = ['a', 'b', 'c', 'a', 'b', 'c']
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        p1.children = ['a']
        after = ['a']
        self.assert_(p1.children == after)
        self.assert_([c.name for c in p1._children] == after)

        self.assert_((p1.children * 2) == ['a', 'a'])
        self.assert_((2 * p1.children) == ['a', 'a'])
        self.assert_((p1.children * 0) == [])
        self.assert_((0 * p1.children) == [])

        self.assert_((p1.children + ['b']) == ['a', 'b'])
        self.assert_((['b'] + p1.children) == ['b', 'a'])

        try:
            p1.children + 123
            assert False
        except TypeError:
            assert True

class DefaultTest(_CollectionOperations):
    def __init__(self, *args, **kw):
        super(DefaultTest, self).__init__(*args, **kw)
        self.collection_class = None

    def test_sequence_ops(self):
        self._test_sequence_ops()


class ListTest(_CollectionOperations):
    def __init__(self, *args, **kw):
        super(ListTest, self).__init__(*args, **kw)
        self.collection_class = list

    def test_sequence_ops(self):
        self._test_sequence_ops()

class CustomListTest(ListTest):
    def __init__(self, *args, **kw):
        super(CustomListTest, self).__init__(*args, **kw)
        self.collection_class = list

# No-can-do until ticket #213
class DictTest(_CollectionOperations):
    pass

class CustomDictTest(DictTest):
    def __init__(self, *args, **kw):
        super(DictTest, self).__init__(*args, **kw)
        self.collection_class = DictCollection

    def test_mapping_ops(self):
        Parent, Child = self.Parent, self.Child

        p1 = Parent('P1')

        self.assert_(not p1._children)
        self.assert_(not p1.children)

        ch = Child('a', 'regular')
        p1._children.append(ch)

        self.assert_(ch in p1._children.values())
        self.assert_(len(p1._children) == 1)

        self.assert_(p1.children)
        self.assert_(len(p1.children) == 1)
        self.assert_(ch not in p1.children)
        self.assert_('a' in p1.children)
        self.assert_(p1.children['a'] == 'regular')
        self.assert_(p1._children['a'] == ch)

        p1.children['b'] = 'proxied'

        self.assert_('proxied' in p1.children.values())
        self.assert_('b' in p1.children)
        self.assert_('proxied' not in p1._children)
        self.assert_(len(p1.children) == 2)
        self.assert_(len(p1._children) == 2)

        self.assert_(p1._children['a'].name == 'regular')
        self.assert_(p1._children['b'].name == 'proxied')

        del p1._children['b']

        self.assert_(len(p1._children) == 1)
        self.assert_(len(p1.children) == 1)
        self.assert_(p1._children['a'] == ch)

        del p1.children['a']

        self.assert_(len(p1._children) == 0)
        self.assert_(len(p1.children) == 0)

        p1.children = {'d': 'v d', 'e': 'v e', 'f': 'v f'}
        self.assert_(len(p1._children) == 3)
        self.assert_(len(p1.children) == 3)

        self.assert_(set(p1.children) == set(['d','e','f']))

        del ch
        p1 = self.roundtrip(p1)
        self.assert_(len(p1._children) == 3)
        self.assert_(len(p1.children) == 3)

        p1.children['e'] = 'changed-in-place'
        self.assert_(p1.children['e'] == 'changed-in-place')
        inplace_id = p1._children['e'].id
        p1 = self.roundtrip(p1)
        self.assert_(p1.children['e'] == 'changed-in-place')
        self.assert_(p1._children['e'].id == inplace_id)

        p1._children = {}
        self.assert_(len(p1.children) == 0)

        try:
            p1._children = []
            self.assert_(False)
        except TypeError:
            self.assert_(True)

        try:
            p1._children = None
            self.assert_(False)
        except TypeError:
            self.assert_(True)

        assert_raises(TypeError, set, [p1.children])


class SetTest(_CollectionOperations):
    def __init__(self, *args, **kw):
        super(SetTest, self).__init__(*args, **kw)
        self.collection_class = set

    def test_set_operations(self):
        Parent, Child = self.Parent, self.Child

        p1 = Parent('P1')

        self.assert_(not p1._children)
        self.assert_(not p1.children)

        ch1 = Child('regular')
        p1._children.add(ch1)

        self.assert_(ch1 in p1._children)
        self.assert_(len(p1._children) == 1)

        self.assert_(p1.children)
        self.assert_(len(p1.children) == 1)
        self.assert_(ch1 not in p1.children)
        self.assert_('regular' in p1.children)

        p1.children.add('proxied')

        self.assert_('proxied' in p1.children)
        self.assert_('proxied' not in p1._children)
        self.assert_(len(p1.children) == 2)
        self.assert_(len(p1._children) == 2)

        self.assert_(set([o.name for o in p1._children]) ==
                     set(['regular', 'proxied']))

        ch2 = None
        for o in p1._children:
            if o.name == 'proxied':
                ch2 = o
                break

        p1._children.remove(ch2)

        self.assert_(len(p1._children) == 1)
        self.assert_(len(p1.children) == 1)
        self.assert_(p1._children == set([ch1]))

        p1.children.remove('regular')

        self.assert_(len(p1._children) == 0)
        self.assert_(len(p1.children) == 0)

        p1.children = ['a','b','c']
        self.assert_(len(p1._children) == 3)
        self.assert_(len(p1.children) == 3)

        del ch1
        p1 = self.roundtrip(p1)

        self.assert_(len(p1._children) == 3)
        self.assert_(len(p1.children) == 3)

        self.assert_('a' in p1.children)
        self.assert_('b' in p1.children)
        self.assert_('d' not in p1.children)

        self.assert_(p1.children == set(['a','b','c']))

        try:
            p1.children.remove('d')
            self.fail()
        except KeyError:
            pass

        self.assert_(len(p1.children) == 3)
        p1.children.discard('d')
        self.assert_(len(p1.children) == 3)
        p1 = self.roundtrip(p1)
        self.assert_(len(p1.children) == 3)

        popped = p1.children.pop()
        self.assert_(len(p1.children) == 2)
        self.assert_(popped not in p1.children)
        p1 = self.roundtrip(p1)
        self.assert_(len(p1.children) == 2)
        self.assert_(popped not in p1.children)

        p1.children = ['a','b','c']
        p1 = self.roundtrip(p1)
        self.assert_(p1.children == set(['a','b','c']))

        p1.children.discard('b')
        p1 = self.roundtrip(p1)
        self.assert_(p1.children == set(['a', 'c']))

        p1.children.remove('a')
        p1 = self.roundtrip(p1)
        self.assert_(p1.children == set(['c']))

        p1._children = set()
        self.assert_(len(p1.children) == 0)

        try:
            p1._children = []
            self.assert_(False)
        except TypeError:
            self.assert_(True)

        try:
            p1._children = None
            self.assert_(False)
        except TypeError:
            self.assert_(True)

        assert_raises(TypeError, set, [p1.children])


    def test_set_comparisons(self):
        Parent, Child = self.Parent, self.Child

        p1 = Parent('P1')
        p1.children = ['a','b','c']
        control = set(['a','b','c'])

        for other in (set(['a','b','c']), set(['a','b','c','d']),
                      set(['a']), set(['a','b']),
                      set(['c','d']), set(['e', 'f', 'g']),
                      set()):

            eq_(p1.children.union(other),
                             control.union(other))
            eq_(p1.children.difference(other),
                             control.difference(other))
            eq_((p1.children - other),
                             (control - other))
            eq_(p1.children.intersection(other),
                             control.intersection(other))
            eq_(p1.children.symmetric_difference(other),
                             control.symmetric_difference(other))
            eq_(p1.children.issubset(other),
                             control.issubset(other))
            eq_(p1.children.issuperset(other),
                             control.issuperset(other))

            self.assert_((p1.children == other)  ==  (control == other))
            self.assert_((p1.children != other)  ==  (control != other))
            self.assert_((p1.children < other)   ==  (control < other))
            self.assert_((p1.children <= other)  ==  (control <= other))
            self.assert_((p1.children > other)   ==  (control > other))
            self.assert_((p1.children >= other)  ==  (control >= other))

    def test_set_mutation(self):
        Parent, Child = self.Parent, self.Child

        # mutations
        for op in ('update', 'intersection_update',
                   'difference_update', 'symmetric_difference_update'):
            for base in (['a', 'b', 'c'], []):
                for other in (set(['a','b','c']), set(['a','b','c','d']),
                              set(['a']), set(['a','b']),
                              set(['c','d']), set(['e', 'f', 'g']),
                              set()):
                    p = Parent('p')
                    p.children = base[:]
                    control = set(base[:])

                    getattr(p.children, op)(other)
                    getattr(control, op)(other)
                    try:
                        self.assert_(p.children == control)
                    except:
                        print 'Test %s.%s(%s):' % (set(base), op, other)
                        print 'want', repr(control)
                        print 'got', repr(p.children)
                        raise

                    p = self.roundtrip(p)

                    try:
                        self.assert_(p.children == control)
                    except:
                        print 'Test %s.%s(%s):' % (base, op, other)
                        print 'want', repr(control)
                        print 'got', repr(p.children)
                        raise

        # in-place mutations
        for op in ('|=', '-=', '&=', '^='):
            for base in (['a', 'b', 'c'], []):
                for other in (set(['a','b','c']), set(['a','b','c','d']),
                              set(['a']), set(['a','b']),
                              set(['c','d']), set(['e', 'f', 'g']),
                              frozenset(['e', 'f', 'g']),
                              set()):
                    p = Parent('p')
                    p.children = base[:]
                    control = set(base[:])

                    exec "p.children %s other" % op
                    exec "control %s other" % op

                    try:
                        self.assert_(p.children == control)
                    except:
                        print 'Test %s %s %s:' % (set(base), op, other)
                        print 'want', repr(control)
                        print 'got', repr(p.children)
                        raise

                    p = self.roundtrip(p)

                    try:
                        self.assert_(p.children == control)
                    except:
                        print 'Test %s %s %s:' % (base, op, other)
                        print 'want', repr(control)
                        print 'got', repr(p.children)
                        raise


class CustomSetTest(SetTest):
    def __init__(self, *args, **kw):
        super(CustomSetTest, self).__init__(*args, **kw)
        self.collection_class = SetCollection

class CustomObjectTest(_CollectionOperations):
    def __init__(self, *args, **kw):
        super(CustomObjectTest, self).__init__(*args, **kw)
        self.collection_class = ObjectCollection

    def test_basic(self):
        Parent, Child = self.Parent, self.Child

        p = Parent('p1')
        self.assert_(len(list(p.children)) == 0)

        p.children.append('child')
        self.assert_(len(list(p.children)) == 1)

        p = self.roundtrip(p)
        self.assert_(len(list(p.children)) == 1)

        # We didn't provide an alternate _AssociationList implementation
        # for our ObjectCollection, so indexing will fail.

        try:
            v = p.children[1]
            self.fail()
        except TypeError:
            pass

class ProxyFactoryTest(ListTest):
    def setup(self):
        metadata = MetaData(testing.db)

        parents_table = Table('Parent', metadata,
                              Column('id', Integer, primary_key=True,
                                     test_needs_autoincrement=True),
                              Column('name', String(128)))
        children_table = Table('Children', metadata,
                               Column('id', Integer, primary_key=True,
                                      test_needs_autoincrement=True),
                               Column('parent_id', Integer,
                                      ForeignKey('Parent.id')),
                               Column('foo', String(128)),
                               Column('name', String(128)))

        class CustomProxy(_AssociationList):
            def __init__(
                self,
                lazy_collection,
                creator,
                value_attr,
                parent,
                ):
                getter, setter = parent._default_getset(lazy_collection)
                _AssociationList.__init__(
                    self,
                    lazy_collection,
                    creator,
                    getter,
                    setter,
                    parent,
                    )

        class Parent(object):
            children = association_proxy('_children', 'name', 
                        proxy_factory=CustomProxy, 
                        proxy_bulk_set=CustomProxy.extend
                    )

            def __init__(self, name):
                self.name = name

        class Child(object):
            def __init__(self, name):
                self.name = name

        mapper(Parent, parents_table, properties={
            '_children': relationship(Child, lazy='joined',
                                  collection_class=list)})
        mapper(Child, children_table)

        metadata.create_all()

        self.metadata = metadata
        self.session = create_session()
        self.Parent, self.Child = Parent, Child

    def test_sequence_ops(self):
        self._test_sequence_ops()


class ScalarTest(fixtures.TestBase):
    def test_scalar_proxy(self):
        metadata = MetaData(testing.db)

        parents_table = Table('Parent', metadata,
                              Column('id', Integer, primary_key=True,
                                     test_needs_autoincrement=True),
                              Column('name', String(128)))
        children_table = Table('Children', metadata,
                               Column('id', Integer, primary_key=True,
                                      test_needs_autoincrement=True),
                               Column('parent_id', Integer,
                                      ForeignKey('Parent.id')),
                               Column('foo', String(128)),
                               Column('bar', String(128)),
                               Column('baz', String(128)))

        class Parent(object):
            foo = association_proxy('child', 'foo')
            bar = association_proxy('child', 'bar',
                                    creator=lambda v: Child(bar=v))
            baz = association_proxy('child', 'baz',
                                    creator=lambda v: Child(baz=v))

            def __init__(self, name):
                self.name = name

        class Child(object):
            def __init__(self, **kw):
                for attr in kw:
                    setattr(self, attr, kw[attr])

        mapper(Parent, parents_table, properties={
            'child': relationship(Child, lazy='joined',
                              backref='parent', uselist=False)})
        mapper(Child, children_table)

        metadata.create_all()
        session = create_session()

        def roundtrip(obj):
            if obj not in session:
                session.add(obj)
            session.flush()
            id, type_ = obj.id, type(obj)
            session.expunge_all()
            return session.query(type_).get(id)

        p = Parent('p')

        # No child
        try:
            v = p.foo
            self.fail()
        except:
            pass

        p.child = Child(foo='a', bar='b', baz='c')

        self.assert_(p.foo == 'a')
        self.assert_(p.bar == 'b')
        self.assert_(p.baz == 'c')

        p.bar = 'x'
        self.assert_(p.foo == 'a')
        self.assert_(p.bar == 'x')
        self.assert_(p.baz == 'c')

        p = roundtrip(p)

        self.assert_(p.foo == 'a')
        self.assert_(p.bar == 'x')
        self.assert_(p.baz == 'c')

        p.child = None

        # No child again
        try:
            v = p.foo
            self.fail()
        except:
            pass

        # Bogus creator for this scalar type
        try:
            p.foo = 'zzz'
            self.fail()
        except TypeError:
            pass

        p.bar = 'yyy'

        self.assert_(p.foo is None)
        self.assert_(p.bar == 'yyy')
        self.assert_(p.baz is None)

        del p.child

        p = roundtrip(p)

        self.assert_(p.child is None)

        p.baz = 'xxx'

        self.assert_(p.foo is None)
        self.assert_(p.bar is None)
        self.assert_(p.baz == 'xxx')

        p = roundtrip(p)

        self.assert_(p.foo is None)
        self.assert_(p.bar is None)
        self.assert_(p.baz == 'xxx')

        # Ensure an immediate __set__ works.
        p2 = Parent('p2')
        p2.bar = 'quux'


class LazyLoadTest(fixtures.TestBase):
    def setup(self):
        metadata = MetaData(testing.db)

        parents_table = Table('Parent', metadata,
                              Column('id', Integer, primary_key=True,
                                     test_needs_autoincrement=True),
                              Column('name', String(128)))
        children_table = Table('Children', metadata,
                               Column('id', Integer, primary_key=True,
                                      test_needs_autoincrement=True),
                               Column('parent_id', Integer,
                                      ForeignKey('Parent.id')),
                               Column('foo', String(128)),
                               Column('name', String(128)))

        class Parent(object):
            children = association_proxy('_children', 'name')

            def __init__(self, name):
                self.name = name

        class Child(object):
            def __init__(self, name):
                self.name = name


        mapper(Child, children_table)
        metadata.create_all()

        self.metadata = metadata
        self.session = create_session()
        self.Parent, self.Child = Parent, Child
        self.table = parents_table

    def teardown(self):
        self.metadata.drop_all()

    def roundtrip(self, obj):
        self.session.add(obj)
        self.session.flush()
        id, type_ = obj.id, type(obj)
        self.session.expunge_all()
        return self.session.query(type_).get(id)

    def test_lazy_list(self):
        Parent, Child = self.Parent, self.Child

        mapper(Parent, self.table, properties={
            '_children': relationship(Child, lazy='select',
                                  collection_class=list)})

        p = Parent('p')
        p.children = ['a','b','c']

        p = self.roundtrip(p)

        # Is there a better way to ensure that the association_proxy
        # didn't convert a lazy load to an eager load?  This does work though.
        self.assert_('_children' not in p.__dict__)
        self.assert_(len(p._children) == 3)
        self.assert_('_children' in p.__dict__)

    def test_eager_list(self):
        Parent, Child = self.Parent, self.Child

        mapper(Parent, self.table, properties={
            '_children': relationship(Child, lazy='joined',
                                  collection_class=list)})

        p = Parent('p')
        p.children = ['a','b','c']

        p = self.roundtrip(p)

        self.assert_('_children' in p.__dict__)
        self.assert_(len(p._children) == 3)

    def test_lazy_scalar(self):
        Parent, Child = self.Parent, self.Child

        mapper(Parent, self.table, properties={
            '_children': relationship(Child, lazy='select', uselist=False)})


        p = Parent('p')
        p.children = 'value'

        p = self.roundtrip(p)

        self.assert_('_children' not in p.__dict__)
        self.assert_(p._children is not None)

    def test_eager_scalar(self):
        Parent, Child = self.Parent, self.Child

        mapper(Parent, self.table, properties={
            '_children': relationship(Child, lazy='joined', uselist=False)})


        p = Parent('p')
        p.children = 'value'

        p = self.roundtrip(p)

        self.assert_('_children' in p.__dict__)
        self.assert_(p._children is not None)


class Parent(object):
    def __init__(self, name):
        self.name = name

class Child(object):
    def __init__(self, name):
        self.name = name

class KVChild(object):
    def __init__(self, name, value):
        self.name = name
        self.value = value

class ReconstitutionTest(fixtures.TestBase):

    def setup(self):
        metadata = MetaData(testing.db)
        parents = Table('parents', metadata, Column('id', Integer,
                        primary_key=True,
                        test_needs_autoincrement=True), Column('name',
                        String(30)))
        children = Table('children', metadata, Column('id', Integer,
                         primary_key=True,
                         test_needs_autoincrement=True),
                         Column('parent_id', Integer,
                         ForeignKey('parents.id')), Column('name',
                         String(30)))
        metadata.create_all()
        parents.insert().execute(name='p1')
        self.metadata = metadata
        self.parents = parents
        self.children = children
        Parent.kids = association_proxy('children', 'name')

    def teardown(self):
        self.metadata.drop_all()
        clear_mappers()

    def test_weak_identity_map(self):
        mapper(Parent, self.parents,
               properties=dict(children=relationship(Child)))
        mapper(Child, self.children)
        session = create_session(weak_identity_map=True)

        def add_child(parent_name, child_name):
            parent = \
                session.query(Parent).filter_by(name=parent_name).one()
            parent.kids.append(child_name)

        add_child('p1', 'c1')
        gc_collect()
        add_child('p1', 'c2')
        session.flush()
        p = session.query(Parent).filter_by(name='p1').one()
        assert set(p.kids) == set(['c1', 'c2']), p.kids

    def test_copy(self):
        mapper(Parent, self.parents,
               properties=dict(children=relationship(Child)))
        mapper(Child, self.children)
        p = Parent('p1')
        p.kids.extend(['c1', 'c2'])
        p_copy = copy.copy(p)
        del p
        gc_collect()
        assert set(p_copy.kids) == set(['c1', 'c2']), p.kids

    def test_pickle_list(self):
        mapper(Parent, self.parents,
               properties=dict(children=relationship(Child)))
        mapper(Child, self.children)
        p = Parent('p1')
        p.kids.extend(['c1', 'c2'])
        r1 = pickle.loads(pickle.dumps(p))
        assert r1.kids == ['c1', 'c2']
        r2 = pickle.loads(pickle.dumps(p.kids))
        assert r2 == ['c1', 'c2']

    def test_pickle_set(self):
        mapper(Parent, self.parents,
               properties=dict(children=relationship(Child,
               collection_class=set)))
        mapper(Child, self.children)
        p = Parent('p1')
        p.kids.update(['c1', 'c2'])
        r1 = pickle.loads(pickle.dumps(p))
        assert r1.kids == set(['c1', 'c2'])
        r2 = pickle.loads(pickle.dumps(p.kids))
        assert r2 == set(['c1', 'c2'])

    def test_pickle_dict(self):
        mapper(Parent, self.parents,
               properties=dict(children=relationship(KVChild,
               collection_class=
                    collections.mapped_collection(PickleKeyFunc('name')))))
        mapper(KVChild, self.children)
        p = Parent('p1')
        p.kids.update({'c1': 'v1', 'c2': 'v2'})
        assert p.kids == {'c1': 'c1', 'c2': 'c2'}
        r1 = pickle.loads(pickle.dumps(p))
        assert r1.kids == {'c1': 'c1', 'c2': 'c2'}
        r2 = pickle.loads(pickle.dumps(p.kids))
        assert r2 == {'c1': 'c1', 'c2': 'c2'}

class PickleKeyFunc(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, obj):
        return getattr(obj, self.name)

class ComparatorTest(fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    run_inserts = 'once'
    run_deletes = None
    run_setup_mappers = 'once'
    run_setup_classes = 'once'

    @classmethod
    def define_tables(cls, metadata):
        Table('userkeywords', metadata, 
          Column('keyword_id', Integer,ForeignKey('keywords.id'), primary_key=True),
          Column('user_id', Integer, ForeignKey('users.id'))
        )
        Table('users', metadata, 
            Column('id', Integer,
              primary_key=True, test_needs_autoincrement=True),
            Column('name', String(64)),
            Column('singular_id', Integer, ForeignKey('singular.id'))
        )
        Table('keywords', metadata, 
            Column('id', Integer,
              primary_key=True, test_needs_autoincrement=True),
            Column('keyword', String(64)),
            Column('singular_id', Integer, ForeignKey('singular.id'))
        )
        Table('singular', metadata,
            Column('id', Integer,
              primary_key=True, test_needs_autoincrement=True),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            def __init__(self, name):
                self.name = name

            # o2m -> m2o
            # uselist -> nonuselist
            keywords = association_proxy('user_keywords', 'keyword',
                    creator=lambda k: UserKeyword(keyword=k))

            # m2o -> o2m
            # nonuselist -> uselist
            singular_keywords = association_proxy('singular', 'keywords')

        class Keyword(cls.Comparable):
            def __init__(self, keyword):
                self.keyword = keyword

            # o2o -> m2o
            # nonuselist -> nonuselist
            user = association_proxy('user_keyword', 'user')

        class UserKeyword(cls.Comparable):
            def __init__(self, user=None, keyword=None):
                self.user = user
                self.keyword = keyword

        class Singular(cls.Comparable):
            def __init__(self, value=None):
                self.value = value

    @classmethod
    def setup_mappers(cls):
        users, Keyword, UserKeyword, singular, \
            userkeywords, User, keywords, Singular = (cls.tables.users,
                                cls.classes.Keyword,
                                cls.classes.UserKeyword,
                                cls.tables.singular,
                                cls.tables.userkeywords,
                                cls.classes.User,
                                cls.tables.keywords,
                                cls.classes.Singular)

        mapper(User, users, properties={
            'singular':relationship(Singular)
        })
        mapper(Keyword, keywords, properties={
            'user_keyword':relationship(UserKeyword, uselist=False)
        })

        mapper(UserKeyword, userkeywords, properties={
            'user' : relationship(User, backref='user_keywords'), 
            'keyword' : relationship(Keyword)
        })
        mapper(Singular, singular, properties={
            'keywords': relationship(Keyword)
        })

    @classmethod
    def insert_data(cls):
        UserKeyword, User, Keyword, Singular = (cls.classes.UserKeyword,
                                cls.classes.User,
                                cls.classes.Keyword,
                                cls.classes.Singular)

        session = sessionmaker()()
        words = (
            'quick', 'brown',
            'fox', 'jumped', 'over',
            'the', 'lazy',
            )
        for ii in range(4):
            user = User('user%d' % ii)
            user.singular = Singular()
            session.add(user)
            for jj in words[ii:ii + 3]:
                k = Keyword(jj)
                user.keywords.append(k)
                user.singular.keywords.append(k)
        orphan = Keyword('orphan')
        orphan.user_keyword = UserKeyword(keyword=orphan, user=None)
        session.add(orphan)
        session.commit()
        cls.u = user
        cls.kw = user.keywords[0]
        cls.session = session

    def _equivalent(self, q_proxy, q_direct):
        eq_(q_proxy.all(), q_direct.all())

    def test_filter_any_kwarg_ul_nul(self):
        UserKeyword, User = self.classes.UserKeyword, self.classes.User

        self._equivalent(self.session.query(User).
                    filter(User.keywords.any(keyword='jumped'
                         )),
                         self.session.query(User).filter(
                                User.user_keywords.any(
                            UserKeyword.keyword.has(keyword='jumped'
                         ))))

    def test_filter_has_kwarg_nul_nul(self):
        UserKeyword, Keyword = self.classes.UserKeyword, self.classes.Keyword

        self._equivalent(self.session.query(Keyword).
                    filter(Keyword.user.has(name='user2'
                         )),
                         self.session.query(Keyword).
                            filter(Keyword.user_keyword.has(
                            UserKeyword.user.has(name='user2'
                         ))))

    def test_filter_has_kwarg_nul_ul(self):
        User, Singular = self.classes.User, self.classes.Singular

        self._equivalent(
            self.session.query(User).\
                        filter(User.singular_keywords.any(keyword='jumped')),
            self.session.query(User).\
                        filter(
                            User.singular.has(
                                Singular.keywords.any(keyword='jumped')
                            )
                        )
        )

    def test_filter_any_criterion_ul_nul(self):
        UserKeyword, User, Keyword = (self.classes.UserKeyword,
                                self.classes.User,
                                self.classes.Keyword)

        self._equivalent(self.session.query(User).
                    filter(User.keywords.any(Keyword.keyword
                         == 'jumped')),
                         self.session.query(User).
                            filter(User.user_keywords.any(
                            UserKeyword.keyword.has(Keyword.keyword
                         == 'jumped'))))

    def test_filter_has_criterion_nul_nul(self):
        UserKeyword, User, Keyword = (self.classes.UserKeyword,
                                self.classes.User,
                                self.classes.Keyword)

        self._equivalent(self.session.query(Keyword).
                filter(Keyword.user.has(User.name
                         == 'user2')),
                         self.session.query(Keyword).
                            filter(Keyword.user_keyword.has(
                                UserKeyword.user.has(User.name
                         == 'user2'))))

    def test_filter_any_criterion_nul_ul(self):
        User, Keyword, Singular = (self.classes.User,
                                self.classes.Keyword,
                                self.classes.Singular)

        self._equivalent(
            self.session.query(User).\
                        filter(User.singular_keywords.any(Keyword.keyword=='jumped')),
            self.session.query(User).\
                        filter(
                            User.singular.has(
                                Singular.keywords.any(Keyword.keyword=='jumped')
                            )
                        )
        )

    def test_filter_contains_ul_nul(self):
        User = self.classes.User

        self._equivalent(self.session.query(User).
        filter(User.keywords.contains(self.kw)),
                         self.session.query(User).
                         filter(User.user_keywords.any(keyword=self.kw)))

    def test_filter_contains_nul_ul(self):
        User, Singular = self.classes.User, self.classes.Singular

        self._equivalent(
            self.session.query(User).filter(
                            User.singular_keywords.contains(self.kw)
            ),
            self.session.query(User).filter(
                            User.singular.has(
                                Singular.keywords.contains(self.kw)
                            )
            ),
        )

    def test_filter_eq_nul_nul(self):
        Keyword = self.classes.Keyword

        self._equivalent(self.session.query(Keyword).filter(Keyword.user
                         == self.u),
                         self.session.query(Keyword).
                         filter(Keyword.user_keyword.has(user=self.u)))

    def test_filter_ne_nul_nul(self):
        Keyword = self.classes.Keyword

        self._equivalent(self.session.query(Keyword).filter(Keyword.user
                         != self.u),
                         self.session.query(Keyword).
                         filter(not_(Keyword.user_keyword.has(user=self.u))))

    def test_filter_eq_null_nul_nul(self):
        UserKeyword, Keyword = self.classes.UserKeyword, self.classes.Keyword

        self._equivalent(self.session.query(Keyword).filter(Keyword.user
                         == None),
                         self.session.query(Keyword).
                            filter(Keyword.user_keyword.has(UserKeyword.user
                         == None)))

    def test_filter_scalar_contains_fails_nul_nul(self):
        Keyword = self.classes.Keyword

        assert_raises(exceptions.InvalidRequestError, lambda : \
                      Keyword.user.contains(self.u))

    def test_filter_scalar_any_fails_nul_nul(self):
        Keyword = self.classes.Keyword

        assert_raises(exceptions.InvalidRequestError, lambda : \
                      Keyword.user.any(name='user2'))

    def test_filter_collection_has_fails_ul_nul(self):
        User = self.classes.User

        assert_raises(exceptions.InvalidRequestError, lambda : \
                      User.keywords.has(keyword='quick'))

    def test_filter_collection_eq_fails_ul_nul(self):
        User = self.classes.User

        assert_raises(exceptions.InvalidRequestError, lambda : \
                      User.keywords == self.kw)

    def test_filter_collection_ne_fails_ul_nul(self):
        User = self.classes.User

        assert_raises(exceptions.InvalidRequestError, lambda : \
                      User.keywords != self.kw)

    def test_join_separate_attr(self):
        User = self.classes.User
        self.assert_compile(
            self.session.query(User).join(
                        User.keywords.local_attr, 
                        User.keywords.remote_attr),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users.singular_id AS users_singular_id "
            "FROM users JOIN userkeywords ON users.id = "
            "userkeywords.user_id JOIN keywords ON keywords.id = "
            "userkeywords.keyword_id"
        )

    def test_join_single_attr(self):
        User = self.classes.User
        self.assert_compile(
            self.session.query(User).join(
                        *User.keywords.attr),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users.singular_id AS users_singular_id "
            "FROM users JOIN userkeywords ON users.id = "
            "userkeywords.user_id JOIN keywords ON keywords.id = "
            "userkeywords.keyword_id"
        )

class DictOfTupleUpdateTest(fixtures.TestBase):
    def setup(self):
        class B(object):
            def __init__(self, key, elem):
                self.key = key
                self.elem = elem

        class A(object):
            elements = association_proxy("orig", "elem", creator=B)

        m = MetaData()
        a = Table('a', m, Column('id', Integer, primary_key=True))
        b = Table('b', m, Column('id', Integer, primary_key=True), 
                    Column('aid', Integer, ForeignKey('a.id')))
        mapper(A, a, properties={
            'orig':relationship(B, collection_class=attribute_mapped_collection('key'))
        })
        mapper(B, b)
        self.A = A
        self.B = B

    def test_update_one_elem_dict(self):
        a1 = self.A()
        a1.elements.update({("B", 3): 'elem2'})
        eq_(a1.elements, {("B",3):'elem2'})

    def test_update_multi_elem_dict(self):
        a1 = self.A()
        a1.elements.update({("B", 3): 'elem2', ("C", 4): "elem3"})
        eq_(a1.elements, {("B",3):'elem2', ("C", 4): "elem3"})

    def test_update_one_elem_list(self):
        a1 = self.A()
        a1.elements.update([(("B", 3), 'elem2')])
        eq_(a1.elements, {("B",3):'elem2'})

    def test_update_multi_elem_list(self):
        a1 = self.A()
        a1.elements.update([(("B", 3), 'elem2'), (("C", 4), "elem3")])
        eq_(a1.elements, {("B",3):'elem2', ("C", 4): "elem3"})

    def test_update_one_elem_varg(self):
        a1 = self.A()
        assert_raises_message(
            ValueError,
            "dictionary update sequence requires "
            "2-element tuples",
            a1.elements.update, (("B", 3), 'elem2')
        )

    def test_update_multi_elem_varg(self):
        a1 = self.A()
        assert_raises_message(
            TypeError,
            "update expected at most 1 arguments, got 2",
            a1.elements.update,
            (("B", 3), 'elem2'), (("C", 4), "elem3")
        )
