import testbase
from sqlalchemy import *
from sqlalchemy.orm import create_session, mapper, relation, \
    interfaces, attributes
import sqlalchemy.orm.collections as collections
from sqlalchemy.orm.collections import collection
from sqlalchemy import util
from operator import and_


class Canary(interfaces.AttributeExtension):
    def __init__(self):
        self.data = set()
        self.added = set()
        self.removed = set()
    def append(self, obj, value, initiator):
        assert value not in self.added
        self.data.add(value)
        self.added.add(value)
    def remove(self, obj, value, initiator):
        assert value not in self.removed
        self.data.remove(value)
        self.removed.add(value)
    def set(self, obj, value, oldvalue, initiator):
        if oldvalue is not None:
            self.remove(obj, oldvalue, None)
        self.append(obj, value, None)

class Entity(object):
    def __init__(self, a=None, b=None, c=None):
        self.a = a
        self.b = b
        self.c = c
    def __repr__(self):
        return str((id(self), self.a, self.b, self.c))

manager = attributes.AttributeManager()

_id = 1
def entity_maker():
    global _id
    _id += 1
    return Entity(_id)

class CollectionsTest(testbase.PersistTest):
    def _test_adapter(self, collection_class, creator=entity_maker,
                      to_set=None):
        class Foo(object):
            pass

        canary = Canary()
        manager.register_attribute(Foo, 'attr', True, extension=canary,
                                   typecallable=collection_class)

        obj = Foo()
        adapter = collections.collection_adapter(obj.attr)
        direct = obj.attr
        if to_set is None:
            to_set = lambda col: set(col)

        def assert_eq():
            self.assert_(to_set(direct) == set(canary.data))
            self.assert_(set(adapter) == set(canary.data))            
        assert_ne = lambda: self.assert_(set(obj.attr) != set(canary.data))

        e1, e2 = creator(), creator()

        adapter.append_with_event(e1)
        assert_eq()
        
        adapter.append_without_event(e2)
        assert_ne()
        canary.data.add(e2)
        assert_eq()
        
        adapter.remove_without_event(e2)
        assert_ne()
        canary.data.remove(e2)
        assert_eq()

        adapter.remove_with_event(e1)
        assert_eq()

    def _test_list(self, collection_class, creator=entity_maker):
        class Foo(object):
            pass
        
        canary = Canary()
        manager.register_attribute(Foo, 'attr', True, extension=canary,
                                   collection_class=collection_class)

        obj = Foo()
        adapter = collections.collection_adapter(obj.attr)
        direct = obj.attr
        control = list()

        def assert_eq():
            self.assert_(set(direct) == set(canary.data))
            self.assert_(set(adapter) == set(canary.data))
            self.assert_(direct == control)
        
        # assume append() is available for list tests
        e = creator()
        direct.append(e)
        control.append(e)
        assert_eq()

        if hasattr(direct, 'pop'):
            direct.pop()
            control.pop()
            assert_eq()

        if hasattr(direct, '__setitem__'):
            e = creator()
            direct.append(e)
            control.append(e)
            
            e = creator()
            direct[0] = e
            control[0] = e
            assert_eq()

            if reduce(and_, [hasattr(direct, a) for a in
                             ('__delitem', 'insert', '__len__')], True):
                values = [creator(), creator(), creator(), creator()]
                direct[slice(0,1)] = values
                control[slice(0,1)] = values
                assert_eq()

                values = [creator(), creator()]
                direct[slice(0,-1,2)] = values
                control[slice(0,-1,2)] = values
                assert_eq()

                values = [creator()]
                direct[slice(0,-1)] = values
                control[slice(0,-1)] = values
                assert_eq()

        if hasattr(direct, '__delitem__'):
            e = creator()
            direct.append(e)
            control.append(e)
            del direct[-1]
            del control[-1]
            assert_eq()

            if hasattr(direct, '__getslice__'):
                for e in [creator(), creator(), creator(), creator()]:
                    direct.append(e)
                    control.append(e)

                del direct[:-3]
                del control[:-3]
                assert_eq()

                del direct[0:1]
                del control[0:1]
                assert_eq()

                del direct[::2]
                del control[::2]
                assert_eq()

        if hasattr(direct, 'remove'):
            e = creator()
            direct.append(e)
            control.append(e)
            
            direct.remove(e)
            control.remove(e)
            assert_eq()

        if hasattr(direct, '__setslice__'):
            values = [creator(), creator()]
            direct[0:1] = values
            control[0:1] = values
            assert_eq()

            values = [creator()]
            direct[0:] = values
            control[0:] = values
            assert_eq()
        
        if hasattr(direct, '__delslice__'):
            for i in range(1, 4):
                e = creator()
                direct.append(e)
                control.append(e)

            del direct[-1:]
            del control[-1:] 
            assert_eq()

            del direct[1:2]
            del control[1:2]
            assert_eq()

            del direct[:]
            del control[:]
            assert_eq()

        if hasattr(direct, 'extend'):
            values = [creator(), creator(), creator()]

            direct.extend(values)
            control.extend(values)
            assert_eq()
                    
    def test_list(self):
        self._test_adapter(list)
        self._test_list(list)

    def test_list_subclass(self):
        class MyList(list):
            pass
        self._test_adapter(MyList)
        self._test_list(MyList)
        self.assert_(getattr(MyList, '_sa_instrumented') == id(MyList))

    def test_list_duck(self):
        class ListLike(object):
            def __init__(self):
                self.data = list()
            def append(self, item):
                self.data.append(item)
            def remove(self, item):
                self.data.remove(item)
            def insert(self, index, item):
                self.data.insert(index, item)
            def pop(self, index=-1):
                self.data.pop(index)
            def extend(self):
                assert False
            def __iter__(self):
                return iter(self.data)
            
        self._test_adapter(ListLike)
        self._test_list(ListLike)
        self.assert_(getattr(ListLike, '_sa_instrumented') == id(ListLike))

    def test_list_emulates(self):
        class ListIsh(object):
            __emulates__ = list
            def __init__(self):
                self.data = list()
            def append(self, item):
                self.data.append(item)
            def remove(self, item):
                self.data.remove(item)
            def insert(self, index, item):
                self.data.insert(index, item)
            def pop(self, index=-1):
                self.data.pop(index)
            def extend(self):
                assert False
            def __iter__(self):
                return iter(self.data)
            
        self._test_adapter(ListIsh)
        self._test_list(ListIsh)
        self.assert_(getattr(ListIsh, '_sa_instrumented') == id(ListIsh))

    def test_set(self):
        self._test_adapter(set)

    def test_dict(self):
        def dictable_entity(a=None, b=None, c=None):
            global _id
            _id += 1
            return Entity(a or str(_id), b or 'value %s' % _id, c)
        
        self._test_adapter(collections.attribute_mapped_collection('a'),
                           dictable_entity, to_set=lambda c: set(c.values()))

class DictHelpersTest(testbase.ORMTest):
    def define_tables(self, metadata):
        global parents, children, Parent, Child
        
        parents = Table('parents', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('label', String))
        children = Table('children', metadata,
                         Column('id', Integer, primary_key=True),
                         Column('parent_id', Integer, ForeignKey('parents.id'),
                                nullable=False),
                         Column('a', String),
                         Column('b', String),
                         Column('c', String))

        class Parent(object):
            def __init__(self, label=None):
                self.label = label
        class Child(object):
            def __init__(self, a=None, b=None, c=None):
                self.a = a
                self.b = b
                self.c = c

    def _test_scalar_mapped(self, collection_class):
        mapper(Child, children)
        mapper(Parent, parents, properties={
            'children': relation(Child, collection_class=collection_class,
                                 cascade="all, delete-orphan")
            })
        
        p = Parent()
        p.children['foo'] = Child('foo', 'value')
        p.children['bar'] = Child('bar', 'value')
        session = create_session()
        session.save(p)
        session.flush()
        pid = p.id
        session.clear()

        p = session.query(Parent).get(pid)

        self.assert_(set(p.children.keys()) == set(['foo', 'bar']))
        cid = p.children['foo'].id

        collections.collection_adapter(p.children).append_with_event(
            Child('foo', 'newvalue'))
        
        session.save(p)
        session.flush()
        session.clear()
        
        p = session.query(Parent).get(pid)
        
        self.assert_(set(p.children.keys()) == set(['foo', 'bar']))
        self.assert_(p.children['foo'].id != cid)
        
        self.assert_(len(list(collections.collection_adapter(p.children))) == 2)
        session.flush()
        session.clear()

        p = session.query(Parent).get(pid)
        self.assert_(len(list(collections.collection_adapter(p.children))) == 2)

        collections.collection_adapter(p.children).remove_with_event(
            p.children['foo'])
        
        self.assert_(len(list(collections.collection_adapter(p.children))) == 1)
        session.flush()
        session.clear()

        p = session.query(Parent).get(pid)
        self.assert_(len(list(collections.collection_adapter(p.children))) == 1)

        del p.children['bar']
        self.assert_(len(list(collections.collection_adapter(p.children))) == 0)
        session.flush()
        session.clear()

        p = session.query(Parent).get(pid)
        self.assert_(len(list(collections.collection_adapter(p.children))) == 0)
        

    def _test_composite_mapped(self, collection_class):
        mapper(Child, children)
        mapper(Parent, parents, properties={
            'children': relation(Child, collection_class=collection_class,
                                 cascade="all, delete-orphan")
            })
        
        p = Parent()
        p.children[('foo', '1')] = Child('foo', '1', 'value 1')
        p.children[('foo', '2')] = Child('foo', '2', 'value 2')

        session = create_session()
        session.save(p)
        session.flush()
        pid = p.id
        session.clear()
        
        p = session.query(Parent).get(pid)

        self.assert_(set(p.children.keys()) == set([('foo', '1'), ('foo', '2')]))
        cid = p.children[('foo', '1')].id

        collections.collection_adapter(p.children).append_with_event(
            Child('foo', '1', 'newvalue'))
        
        session.save(p)
        session.flush()
        session.clear()
        
        p = session.query(Parent).get(pid)
        
        self.assert_(set(p.children.keys()) == set([('foo', '1'), ('foo', '2')]))
        self.assert_(p.children[('foo', '1')].id != cid)
        
        self.assert_(len(list(collections.collection_adapter(p.children))) == 2)
        
    def test_mapped_collection(self):
        collection_class = collections.mapped_collection(lambda c: c.a)
        self._test_scalar_mapped(collection_class)

    def test_mapped_collection2(self):
        collection_class = collections.mapped_collection(lambda c: (c.a, c.b))
        self._test_composite_mapped(collection_class)

    def test_attr_mapped_collection(self):
        collection_class = collections.attribute_mapped_collection('a')
        self._test_scalar_mapped(collection_class)

    def test_column_mapped_collection(self):
        collection_class = collections.column_mapped_collection(children.c.a)
        self._test_scalar_mapped(collection_class)

    def test_column_mapped_collection2(self):
        collection_class = collections.column_mapped_collection((children.c.a,
                                                                 children.c.b))
        self._test_composite_mapped(collection_class)

    def test_mixin(self):
        class Ordered(util.OrderedDict, collections.MappedCollection):
            def __init__(self):
                collections.MappedCollection.__init__(self, lambda v: v.a)
                util.OrderedDict.__init__(self)
        collection_class = Ordered
        self._test_scalar_mapped(collection_class)

    def test_mixin2(self):
        class Ordered2(util.OrderedDict, collections.MappedCollection):
            def __init__(self, keyfunc):
                collections.MappedCollection.__init__(self, keyfunc)
                util.OrderedDict.__init__(self)
        collection_class = lambda: Ordered2(lambda v: (v.a, v.b))
        self._test_composite_mapped(collection_class)

if __name__ == "__main__":
    testbase.main()
