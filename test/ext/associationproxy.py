import testbase

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.orm.collections import collection
from sqlalchemy.ext.associationproxy import *
from testlib import *


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

class _CollectionOperations(PersistTest):
    def setUp(self):
        collection_class = self.collection_class

        metadata = MetaData(testbase.db)
    
        parents_table = Table('Parent', metadata,
                              Column('id', Integer, primary_key=True),
                              Column('name', String))
        children_table = Table('Children', metadata,
                               Column('id', Integer, primary_key=True),
                               Column('parent_id', Integer,
                                      ForeignKey('Parent.id')),
                               Column('foo', String),
                               Column('name', String))

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
            '_children': relation(Child, lazy=False,
                                  collection_class=collection_class)})
        mapper(Child, children_table)

        metadata.create_all()

        self.metadata = metadata
        self.session = create_session()
        self.Parent, self.Child = Parent, Child

    def tearDown(self):
        self.metadata.drop_all()

    def roundtrip(self, obj):
        self.session.save(obj)
        self.session.flush()
        id, type_ = obj.id, type(obj)
        self.session.clear()
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
        except exceptions.ArgumentError:
            self.assert_(True)

        try:
            p1._children = None
            self.assert_(False)
        except exceptions.ArgumentError:
            self.assert_(True)

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
        except exceptions.ArgumentError:
            self.assert_(True)

        try:
            p1._children = None
            self.assert_(False)
        except exceptions.ArgumentError:
            self.assert_(True)


    def test_set_comparisons(self):
        Parent, Child = self.Parent, self.Child

        p1 = Parent('P1')
        p1.children = ['a','b','c']
        control = set(['a','b','c'])

        for other in (set(['a','b','c']), set(['a','b','c','d']),
                      set(['a']), set(['a','b']),
                      set(['c','d']), set(['e', 'f', 'g']),
                      set()):

            self.assertEqual(p1.children.union(other),
                             control.union(other))
            self.assertEqual(p1.children.difference(other),
                             control.difference(other))
            self.assertEqual((p1.children - other),
                             (control - other))
            self.assertEqual(p1.children.intersection(other),
                             control.intersection(other))
            self.assertEqual(p1.children.symmetric_difference(other),
                             control.symmetric_difference(other))
            self.assertEqual(p1.children.issubset(other),
                             control.issubset(other))
            self.assertEqual(p1.children.issuperset(other),
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

        # We didn't provide an alternate _AssociationList implementation for
        # our ObjectCollection, so indexing will fail.
        try:
            v = p.children[1]
            self.fail()
        except TypeError:
            pass

class ScalarTest(PersistTest):
    def test_scalar_proxy(self):
        metadata = MetaData(testbase.db)
    
        parents_table = Table('Parent', metadata,
                              Column('id', Integer, primary_key=True),
                              Column('name', String))
        children_table = Table('Children', metadata,
                               Column('id', Integer, primary_key=True),
                               Column('parent_id', Integer,
                                      ForeignKey('Parent.id')),
                               Column('foo', String),
                               Column('bar', String),
                               Column('baz', String))

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
            'child': relation(Child, lazy=False,
                              backref='parent', uselist=False)})
        mapper(Child, children_table)

        metadata.create_all()
        session = create_session()

        def roundtrip(obj):
            session.save(obj)
            session.flush()
            id, type_ = obj.id, type(obj)
            session.clear()
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
        

class LazyLoadTest(PersistTest):
    def setUp(self):
        metadata = MetaData(testbase.db)
    
        parents_table = Table('Parent', metadata,
                              Column('id', Integer, primary_key=True),
                              Column('name', String))
        children_table = Table('Children', metadata,
                               Column('id', Integer, primary_key=True),
                               Column('parent_id', Integer,
                                      ForeignKey('Parent.id')),
                               Column('foo', String),
                               Column('name', String))

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

    def tearDown(self):
        self.metadata.drop_all()

    def roundtrip(self, obj):
        self.session.save(obj)
        self.session.flush()
        id, type_ = obj.id, type(obj)
        self.session.clear()
        return self.session.query(type_).get(id)

    def test_lazy_list(self):
        Parent, Child = self.Parent, self.Child

        mapper(Parent, self.table, properties={
            '_children': relation(Child, lazy=True,
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
            '_children': relation(Child, lazy=False,
                                  collection_class=list)})

        p = Parent('p')
        p.children = ['a','b','c']

        p = self.roundtrip(p)

        self.assert_('_children' in p.__dict__)
        self.assert_(len(p._children) == 3)

    def test_lazy_scalar(self):
        Parent, Child = self.Parent, self.Child

        mapper(Parent, self.table, properties={
            '_children': relation(Child, lazy=True, uselist=False)})


        p = Parent('p')
        p.children = 'value'

        p = self.roundtrip(p)

        self.assert_('_children' not in p.__dict__)
        self.assert_(p._children is not None)

    def test_eager_scalar(self):
        Parent, Child = self.Parent, self.Child

        mapper(Parent, self.table, properties={
            '_children': relation(Child, lazy=False, uselist=False)})


        p = Parent('p')
        p.children = 'value'

        p = self.roundtrip(p)

        self.assert_('_children' in p.__dict__)
        self.assert_(p._children is not None)
    

if __name__ == "__main__":
    testbase.main()        
