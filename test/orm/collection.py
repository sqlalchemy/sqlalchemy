import testbase
from sqlalchemy import *
import sqlalchemy.exceptions as exceptions
from sqlalchemy.orm import create_session, mapper, relation, \
    interfaces, attributes
import sqlalchemy.orm.collections as collections
from sqlalchemy.orm.collections import collection
from sqlalchemy import util
from operator import and_
from testlib import *

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

attributes.register_class(Entity)

_id = 1
def entity_maker():
    global _id
    _id += 1
    return Entity(_id)
def dictable_entity(a=None, b=None, c=None):
    global _id
    _id += 1
    return Entity(a or str(_id), b or 'value %s' % _id, c)


class CollectionsTest(PersistTest):
    def _test_adapter(self, typecallable, creator=entity_maker,
                      to_set=None):
        class Foo(object):
            pass

        canary = Canary()
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', True, extension=canary,
                                   typecallable=typecallable, useobject=True)

        obj = Foo()
        adapter = collections.collection_adapter(obj.attr)
        direct = obj.attr
        if to_set is None:
            to_set = lambda col: set(col)

        def assert_eq():
            self.assert_(to_set(direct) == canary.data)
            self.assert_(set(adapter) == canary.data)
        assert_ne = lambda: self.assert_(to_set(direct) != canary.data)

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

    def _test_list(self, typecallable, creator=entity_maker):
        class Foo(object):
            pass

        canary = Canary()
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', True, extension=canary,
                                   typecallable=typecallable, useobject=True)

        obj = Foo()
        adapter = collections.collection_adapter(obj.attr)
        direct = obj.attr
        control = list()

        def assert_eq():
            self.assert_(set(direct) == canary.data)
            self.assert_(set(adapter) == canary.data)
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
                             ('__delitem__', 'insert', '__len__')], True):
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

            values = [creator()]
            direct[:1] = values
            control[:1] = values
            assert_eq()

            values = [creator()]
            direct[-1::2] = values
            control[-1::2] = values
            assert_eq()

            values = [creator()] * len(direct[1::2])
            direct[1::2] = values
            control[1::2] = values
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

        if hasattr(direct, '__iadd__'):
            values = [creator(), creator(), creator()]

            direct += values
            control += values
            assert_eq()

            direct += []
            control += []
            assert_eq()

            values = [creator(), creator()]
            obj.attr += values
            control += values
            assert_eq()

        if hasattr(direct, '__imul__'):
            direct *= 2
            control *= 2
            assert_eq()

            obj.attr *= 2
            control *= 2
            assert_eq()

    def _test_list_bulk(self, typecallable, creator=entity_maker):
        class Foo(object):
            pass

        canary = Canary()
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', True, extension=canary,
                                   typecallable=typecallable, useobject=True)

        obj = Foo()
        direct = obj.attr

        e1 = creator()
        obj.attr.append(e1)

        like_me = typecallable()
        e2 = creator()
        like_me.append(e2)

        self.assert_(obj.attr is direct)
        obj.attr = like_me
        self.assert_(obj.attr is not direct)
        self.assert_(obj.attr is not like_me)
        self.assert_(set(obj.attr) == set([e2]))
        self.assert_(e1 in canary.removed)
        self.assert_(e2 in canary.added)

        e3 = creator()
        real_list = [e3]
        obj.attr = real_list
        self.assert_(obj.attr is not real_list)
        self.assert_(set(obj.attr) == set([e3]))
        self.assert_(e2 in canary.removed)
        self.assert_(e3 in canary.added)

        e4 = creator()
        try:
            obj.attr = set([e4])
            self.assert_(False)
        except TypeError:
            self.assert_(e4 not in canary.data)
            self.assert_(e3 in canary.data)

        e5 = creator()
        e6 = creator()
        e7 = creator()
        obj.attr = [e5, e6, e7]
        self.assert_(e5 in canary.added)
        self.assert_(e6 in canary.added)
        self.assert_(e7 in canary.added)

        obj.attr = [e6, e7]
        self.assert_(e5 in canary.removed)
        self.assert_(e6 in canary.added)
        self.assert_(e7 in canary.added)
        self.assert_(e6 not in canary.removed)
        self.assert_(e7 not in canary.removed)

    def test_list(self):
        self._test_adapter(list)
        self._test_list(list)
        self._test_list_bulk(list)

    def test_list_subclass(self):
        class MyList(list):
            pass
        self._test_adapter(MyList)
        self._test_list(MyList)
        self._test_list_bulk(MyList)
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
                return self.data.pop(index)
            def extend(self):
                assert False
            def __iter__(self):
                return iter(self.data)
            def __eq__(self, other):
                return self.data == other
            def __repr__(self):
                return 'ListLike(%s)' % repr(self.data)

        self._test_adapter(ListLike)
        self._test_list(ListLike)
        self._test_list_bulk(ListLike)
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
                return self.data.pop(index)
            def extend(self):
                assert False
            def __iter__(self):
                return iter(self.data)
            def __eq__(self, other):
                return self.data == other
            def __repr__(self):
                return 'ListIsh(%s)' % repr(self.data)

        self._test_adapter(ListIsh)
        self._test_list(ListIsh)
        self._test_list_bulk(ListIsh)
        self.assert_(getattr(ListIsh, '_sa_instrumented') == id(ListIsh))

    def _test_set(self, typecallable, creator=entity_maker):
        class Foo(object):
            pass

        canary = Canary()
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', True, extension=canary,
                                   typecallable=typecallable, useobject=True)

        obj = Foo()
        adapter = collections.collection_adapter(obj.attr)
        direct = obj.attr
        control = set()

        def assert_eq():
            self.assert_(set(direct) == canary.data)
            self.assert_(set(adapter) == canary.data)
            self.assert_(direct == control)

        def addall(*values):
            for item in values:
                direct.add(item)
                control.add(item)
            assert_eq()
        def zap():
            for item in list(direct):
                direct.remove(item)
            control.clear()

        # assume add() is available for list tests
        addall(creator())

        if hasattr(direct, 'pop'):
            direct.pop()
            control.pop()
            assert_eq()

        if hasattr(direct, 'remove'):
            e = creator()
            addall(e)

            direct.remove(e)
            control.remove(e)
            assert_eq()

            e = creator()
            try:
                direct.remove(e)
            except KeyError:
                assert_eq()
                self.assert_(e not in canary.removed)
            else:
                self.assert_(False)

        if hasattr(direct, 'discard'):
            e = creator()
            addall(e)

            direct.discard(e)
            control.discard(e)
            assert_eq()

            e = creator()
            direct.discard(e)
            self.assert_(e not in canary.removed)
            assert_eq()

        if hasattr(direct, 'update'):
            zap()
            e = creator()
            addall(e)

            values = set([e, creator(), creator()])

            direct.update(values)
            control.update(values)
            assert_eq()

        if hasattr(direct, '__ior__'):
            zap()
            e = creator()
            addall(e)

            values = set([e, creator(), creator()])

            direct |= values
            control |= values
            assert_eq()

            # cover self-assignment short-circuit
            values = set([e, creator(), creator()])
            obj.attr |= values
            control |= values
            assert_eq()

            try:
                direct |= [e, creator()]
                assert False
            except TypeError:
                assert True

        if hasattr(direct, 'clear'):
            addall(creator(), creator())
            direct.clear()
            control.clear()
            assert_eq()

        if hasattr(direct, 'difference_update'):
            zap()
            e = creator()
            addall(creator(), creator())
            values = set([creator()])

            direct.difference_update(values)
            control.difference_update(values)
            assert_eq()
            values.update(set([e, creator()]))
            direct.difference_update(values)
            control.difference_update(values)
            assert_eq()

        if hasattr(direct, '__isub__'):
            zap()
            e = creator()
            addall(creator(), creator())
            values = set([creator()])

            direct -= values
            control -= values
            assert_eq()
            values.update(set([e, creator()]))
            direct -= values
            control -= values
            assert_eq()

            values = set([creator()])
            obj.attr -= values
            control -= values
            assert_eq()

            try:
                direct -= [e, creator()]
                assert False
            except TypeError:
                assert True

        if hasattr(direct, 'intersection_update'):
            zap()
            e = creator()
            addall(e, creator(), creator())
            values = set(control)

            direct.intersection_update(values)
            control.intersection_update(values)
            assert_eq()

            values.update(set([e, creator()]))
            direct.intersection_update(values)
            control.intersection_update(values)
            assert_eq()

        if hasattr(direct, '__iand__'):
            zap()
            e = creator()
            addall(e, creator(), creator())
            values = set(control)

            direct &= values
            control &= values
            assert_eq()

            values.update(set([e, creator()]))
            direct &= values
            control &= values
            assert_eq()

            values.update(set([creator()]))
            obj.attr &= values
            control &= values
            assert_eq()

            try:
                direct &= [e, creator()]
                assert False
            except TypeError:
                assert True

        if hasattr(direct, 'symmetric_difference_update'):
            zap()
            e = creator()
            addall(e, creator(), creator())

            values = set([e, creator()])
            direct.symmetric_difference_update(values)
            control.symmetric_difference_update(values)
            assert_eq()

            e = creator()
            addall(e)
            values = set([e])
            direct.symmetric_difference_update(values)
            control.symmetric_difference_update(values)
            assert_eq()

            values = set()
            direct.symmetric_difference_update(values)
            control.symmetric_difference_update(values)
            assert_eq()

        if hasattr(direct, '__ixor__'):
            zap()
            e = creator()
            addall(e, creator(), creator())

            values = set([e, creator()])
            direct ^= values
            control ^= values
            assert_eq()

            e = creator()
            addall(e)
            values = set([e])
            direct ^= values
            control ^= values
            assert_eq()

            values = set()
            direct ^= values
            control ^= values
            assert_eq()

            values = set([creator()])
            obj.attr ^= values
            control ^= values
            assert_eq()

            try:
                direct ^= [e, creator()]
                assert False
            except TypeError:
                assert True

    def _test_set_bulk(self, typecallable, creator=entity_maker):
        class Foo(object):
            pass

        canary = Canary()
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', True, extension=canary,
                                   typecallable=typecallable, useobject=True)

        obj = Foo()
        direct = obj.attr

        e1 = creator()
        obj.attr.add(e1)

        like_me = typecallable()
        e2 = creator()
        like_me.add(e2)

        self.assert_(obj.attr is direct)
        obj.attr = like_me
        self.assert_(obj.attr is not direct)
        self.assert_(obj.attr is not like_me)
        self.assert_(obj.attr == set([e2]))
        self.assert_(e1 in canary.removed)
        self.assert_(e2 in canary.added)

        e3 = creator()
        real_set = set([e3])
        obj.attr = real_set
        self.assert_(obj.attr is not real_set)
        self.assert_(obj.attr == set([e3]))
        self.assert_(e2 in canary.removed)
        self.assert_(e3 in canary.added)

        e4 = creator()
        try:
            obj.attr = [e4]
            self.assert_(False)
        except TypeError:
            self.assert_(e4 not in canary.data)
            self.assert_(e3 in canary.data)

    def test_set(self):
        self._test_adapter(set)
        self._test_set(set)
        self._test_set_bulk(set)

    def test_set_subclass(self):
        class MySet(set):
            pass
        self._test_adapter(MySet)
        self._test_set(MySet)
        self._test_set_bulk(MySet)
        self.assert_(getattr(MySet, '_sa_instrumented') == id(MySet))

    def test_set_duck(self):
        class SetLike(object):
            def __init__(self):
                self.data = set()
            def add(self, item):
                self.data.add(item)
            def remove(self, item):
                self.data.remove(item)
            def discard(self, item):
                self.data.discard(item)
            def pop(self):
                return self.data.pop()
            def update(self, other):
                self.data.update(other)
            def __iter__(self):
                return iter(self.data)
            def __eq__(self, other):
                return self.data == other

        self._test_adapter(SetLike)
        self._test_set(SetLike)
        self._test_set_bulk(SetLike)
        self.assert_(getattr(SetLike, '_sa_instrumented') == id(SetLike))

    def test_set_emulates(self):
        class SetIsh(object):
            __emulates__ = set
            def __init__(self):
                self.data = set()
            def add(self, item):
                self.data.add(item)
            def remove(self, item):
                self.data.remove(item)
            def discard(self, item):
                self.data.discard(item)
            def pop(self):
                return self.data.pop()
            def update(self, other):
                self.data.update(other)
            def __iter__(self):
                return iter(self.data)
            def __eq__(self, other):
                return self.data == other

        self._test_adapter(SetIsh)
        self._test_set(SetIsh)
        self._test_set_bulk(SetIsh)
        self.assert_(getattr(SetIsh, '_sa_instrumented') == id(SetIsh))

    def _test_dict(self, typecallable, creator=dictable_entity):
        class Foo(object):
            pass

        canary = Canary()
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', True, extension=canary,
                                   typecallable=typecallable, useobject=True)

        obj = Foo()
        adapter = collections.collection_adapter(obj.attr)
        direct = obj.attr
        control = dict()

        def assert_eq():
            self.assert_(set(direct.values()) == canary.data)
            self.assert_(set(adapter) == canary.data)
            self.assert_(direct == control)

        def addall(*values):
            for item in values:
                direct.set(item)
                control[item.a] = item
            assert_eq()
        def zap():
            for item in list(adapter):
                direct.remove(item)
            control.clear()

        # assume an 'set' method is available for tests
        addall(creator())

        if hasattr(direct, '__setitem__'):
            e = creator()
            direct[e.a] = e
            control[e.a] = e
            assert_eq()

            e = creator(e.a, e.b)
            direct[e.a] = e
            control[e.a] = e
            assert_eq()

        if hasattr(direct, '__delitem__'):
            e = creator()
            addall(e)

            del direct[e.a]
            del control[e.a]
            assert_eq()

            e = creator()
            try:
                del direct[e.a]
            except KeyError:
                self.assert_(e not in canary.removed)

        if hasattr(direct, 'clear'):
            addall(creator(), creator(), creator())

            direct.clear()
            control.clear()
            assert_eq()

            direct.clear()
            control.clear()
            assert_eq()

        if hasattr(direct, 'pop'):
            e = creator()
            addall(e)

            direct.pop(e.a)
            control.pop(e.a)
            assert_eq()

            e = creator()
            try:
                direct.pop(e.a)
            except KeyError:
                self.assert_(e not in canary.removed)

        if hasattr(direct, 'popitem'):
            zap()
            e = creator()
            addall(e)

            direct.popitem()
            control.popitem()
            assert_eq()

        if hasattr(direct, 'setdefault'):
            e = creator()

            val_a = direct.setdefault(e.a, e)
            val_b = control.setdefault(e.a, e)
            assert_eq()
            self.assert_(val_a is val_b)

            val_a = direct.setdefault(e.a, e)
            val_b = control.setdefault(e.a, e)
            assert_eq()
            self.assert_(val_a is val_b)

        if hasattr(direct, 'update'):
            e = creator()
            d = dict([(ee.a, ee) for ee in [e, creator(), creator()]])
            addall(e, creator())

            direct.update(d)
            control.update(d)
            assert_eq()

            kw = dict([(ee.a, ee) for ee in [e, creator()]])
            direct.update(**kw)
            control.update(**kw)
            assert_eq()

    def _test_dict_bulk(self, typecallable, creator=dictable_entity):
        class Foo(object):
            pass

        canary = Canary()
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', True, extension=canary,
                                   typecallable=typecallable, useobject=True)

        obj = Foo()
        direct = obj.attr

        e1 = creator()
        collections.collection_adapter(direct).append_with_event(e1)

        like_me = typecallable()
        e2 = creator()
        like_me.set(e2)

        self.assert_(obj.attr is direct)
        obj.attr = like_me
        self.assert_(obj.attr is not direct)
        self.assert_(obj.attr is not like_me)
        self.assert_(set(collections.collection_adapter(obj.attr)) == set([e2]))
        self.assert_(e1 in canary.removed)
        self.assert_(e2 in canary.added)


        # key validity on bulk assignment is a basic feature of MappedCollection
        # but is not present in basic, @converter-less dict collections.
        e3 = creator()
        if isinstance(obj.attr, collections.MappedCollection):
            real_dict = dict(badkey=e3)
            try:
                obj.attr = real_dict
                self.assert_(False)
            except TypeError:
                pass
            self.assert_(obj.attr is not real_dict)
            self.assert_('badkey' not in obj.attr)
            self.assertEquals(set(collections.collection_adapter(obj.attr)),
                              set([e2]))
            self.assert_(e3 not in canary.added)
        else:
            real_dict = dict(keyignored1=e3)
            obj.attr = real_dict
            self.assert_(obj.attr is not real_dict)
            self.assert_('keyignored1' not in obj.attr)
            self.assertEquals(set(collections.collection_adapter(obj.attr)),
                              set([e3]))
            self.assert_(e2 in canary.removed)
            self.assert_(e3 in canary.added)

        obj.attr = typecallable()
        self.assertEquals(list(collections.collection_adapter(obj.attr)), [])

        e4 = creator()
        try:
            obj.attr = [e4]
            self.assert_(False)
        except TypeError:
            self.assert_(e4 not in canary.data)

    def test_dict(self):
        try:
            self._test_adapter(dict, dictable_entity,
                               to_set=lambda c: set(c.values()))
            self.assert_(False)
        except exceptions.ArgumentError, e:
            self.assert_(e.args[0] == 'Type InstrumentedDict must elect an appender method to be a collection class')

        try:
            self._test_dict(dict)
            self.assert_(False)
        except exceptions.ArgumentError, e:
            self.assert_(e.args[0] == 'Type InstrumentedDict must elect an appender method to be a collection class')

    def test_dict_subclass(self):
        class MyDict(dict):
            @collection.appender
            @collection.internally_instrumented
            def set(self, item, _sa_initiator=None):
                self.__setitem__(item.a, item, _sa_initiator=_sa_initiator)
            @collection.remover
            @collection.internally_instrumented
            def _remove(self, item, _sa_initiator=None):
                self.__delitem__(item.a, _sa_initiator=_sa_initiator)

        self._test_adapter(MyDict, dictable_entity,
                           to_set=lambda c: set(c.values()))
        self._test_dict(MyDict)
        self._test_dict_bulk(MyDict)
        self.assert_(getattr(MyDict, '_sa_instrumented') == id(MyDict))

    def test_dict_subclass2(self):
        class MyEasyDict(collections.MappedCollection):
            def __init__(self):
                super(MyEasyDict, self).__init__(lambda e: e.a)

        self._test_adapter(MyEasyDict, dictable_entity,
                           to_set=lambda c: set(c.values()))
        self._test_dict(MyEasyDict)
        self._test_dict_bulk(MyEasyDict)
        self.assert_(getattr(MyEasyDict, '_sa_instrumented') == id(MyEasyDict))

    def test_dict_subclass3(self):
        class MyOrdered(util.OrderedDict, collections.MappedCollection):
            def __init__(self):
                collections.MappedCollection.__init__(self, lambda e: e.a)
                util.OrderedDict.__init__(self)

        self._test_adapter(MyOrdered, dictable_entity,
                           to_set=lambda c: set(c.values()))
        self._test_dict(MyOrdered)
        self._test_dict_bulk(MyOrdered)
        self.assert_(getattr(MyOrdered, '_sa_instrumented') == id(MyOrdered))

    def test_dict_duck(self):
        class DictLike(object):
            def __init__(self):
                self.data = dict()

            @collection.appender
            @collection.replaces(1)
            def set(self, item):
                current = self.data.get(item.a, None)
                self.data[item.a] = item
                return current
            @collection.remover
            def _remove(self, item):
                del self.data[item.a]
            def __setitem__(self, key, value):
                self.data[key] = value
            def __getitem__(self, key):
                return self.data[key]
            def __delitem__(self, key):
                del self.data[key]
            def values(self):
                return self.data.values()
            def __contains__(self, key):
                return key in self.data
            @collection.iterator
            def itervalues(self):
                return self.data.itervalues()
            def __eq__(self, other):
                return self.data == other
            def __repr__(self):
                return 'DictLike(%s)' % repr(self.data)

        self._test_adapter(DictLike, dictable_entity,
                           to_set=lambda c: set(c.itervalues()))
        self._test_dict(DictLike)
        self._test_dict_bulk(DictLike)
        self.assert_(getattr(DictLike, '_sa_instrumented') == id(DictLike))

    def test_dict_emulates(self):
        class DictIsh(object):
            __emulates__ = dict
            def __init__(self):
                self.data = dict()

            @collection.appender
            @collection.replaces(1)
            def set(self, item):
                current = self.data.get(item.a, None)
                self.data[item.a] = item
                return current
            @collection.remover
            def _remove(self, item):
                del self.data[item.a]
            def __setitem__(self, key, value):
                self.data[key] = value
            def __getitem__(self, key):
                return self.data[key]
            def __delitem__(self, key):
                del self.data[key]
            def values(self):
                return self.data.values()
            def __contains__(self, key):
                return key in self.data
            @collection.iterator
            def itervalues(self):
                return self.data.itervalues()
            def __eq__(self, other):
                return self.data == other
            def __repr__(self):
                return 'DictIsh(%s)' % repr(self.data)

        self._test_adapter(DictIsh, dictable_entity,
                           to_set=lambda c: set(c.itervalues()))
        self._test_dict(DictIsh)
        self._test_dict_bulk(DictIsh)
        self.assert_(getattr(DictIsh, '_sa_instrumented') == id(DictIsh))

    def _test_object(self, typecallable, creator=entity_maker):
        class Foo(object):
            pass

        canary = Canary()
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', True, extension=canary,
                                   typecallable=typecallable, useobject=True)

        obj = Foo()
        adapter = collections.collection_adapter(obj.attr)
        direct = obj.attr
        control = set()

        def assert_eq():
            self.assert_(set(direct) == canary.data)
            self.assert_(set(adapter) == canary.data)
            self.assert_(direct == control)

        # There is no API for object collections.  We'll make one up
        # for the purposes of the test.
        e = creator()
        direct.push(e)
        control.add(e)
        assert_eq()

        direct.zark(e)
        control.remove(e)
        assert_eq()

        e = creator()
        direct.maybe_zark(e)
        control.discard(e)
        assert_eq()

        e = creator()
        direct.push(e)
        control.add(e)
        assert_eq()

        e = creator()
        direct.maybe_zark(e)
        control.discard(e)
        assert_eq()

    def test_object_duck(self):
        class MyCollection(object):
            def __init__(self):
                self.data = set()
            @collection.appender
            def push(self, item):
                self.data.add(item)
            @collection.remover
            def zark(self, item):
                self.data.remove(item)
            @collection.removes_return()
            def maybe_zark(self, item):
                if item in self.data:
                    self.data.remove(item)
                    return item
            @collection.iterator
            def __iter__(self):
                return iter(self.data)
            def __eq__(self, other):
                return self.data == other

        self._test_adapter(MyCollection)
        self._test_object(MyCollection)
        self.assert_(getattr(MyCollection, '_sa_instrumented') ==
                     id(MyCollection))

    def test_object_emulates(self):
        class MyCollection2(object):
            __emulates__ = None
            def __init__(self):
                self.data = set()
            # looks like a list
            def append(self, item):
                assert False
            @collection.appender
            def push(self, item):
                self.data.add(item)
            @collection.remover
            def zark(self, item):
                self.data.remove(item)
            @collection.removes_return()
            def maybe_zark(self, item):
                if item in self.data:
                    self.data.remove(item)
                    return item
            @collection.iterator
            def __iter__(self):
                return iter(self.data)
            def __eq__(self, other):
                return self.data == other

        self._test_adapter(MyCollection2)
        self._test_object(MyCollection2)
        self.assert_(getattr(MyCollection2, '_sa_instrumented') ==
                     id(MyCollection2))

    def test_recipes(self):
        class Custom(object):
            def __init__(self):
                self.data = []
            @collection.appender
            @collection.adds('entity')
            def put(self, entity):
                self.data.append(entity)

            @collection.remover
            @collection.removes(1)
            def remove(self, entity):
                self.data.remove(entity)

            @collection.adds(1)
            def push(self, *args):
                self.data.append(args[0])

            @collection.removes('entity')
            def yank(self, entity, arg):
                self.data.remove(entity)

            @collection.replaces(2)
            def replace(self, arg, entity, **kw):
                self.data.insert(0, entity)
                return self.data.pop()

            @collection.removes_return()
            def pop(self, key):
                return self.data.pop()

            @collection.iterator
            def __iter__(self):
                return iter(self.data)

        class Foo(object):
            pass
        canary = Canary()
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', True, extension=canary,
                                   typecallable=Custom, useobject=True)

        obj = Foo()
        adapter = collections.collection_adapter(obj.attr)
        direct = obj.attr
        control = list()
        def assert_eq():
            self.assert_(set(direct) == canary.data)
            self.assert_(set(adapter) == canary.data)
            self.assert_(list(direct) == control)
        creator = entity_maker

        e1 = creator()
        direct.put(e1)
        control.append(e1)
        assert_eq()

        e2 = creator()
        direct.put(entity=e2)
        control.append(e2)
        assert_eq()

        direct.remove(e2)
        control.remove(e2)
        assert_eq()

        direct.remove(entity=e1)
        control.remove(e1)
        assert_eq()

        e3 = creator()
        direct.push(e3)
        control.append(e3)
        assert_eq()

        direct.yank(e3, 'blah')
        control.remove(e3)
        assert_eq()

        e4, e5, e6, e7 = creator(), creator(), creator(), creator()
        direct.put(e4)
        direct.put(e5)
        control.append(e4)
        control.append(e5)

        dr1 = direct.replace('foo', e6, bar='baz')
        control.insert(0, e6)
        cr1 = control.pop()
        assert_eq()
        self.assert_(dr1 is cr1)

        dr2 = direct.replace(arg=1, entity=e7)
        control.insert(0, e7)
        cr2 = control.pop()
        assert_eq()
        self.assert_(dr2 is cr2)

        dr3 = direct.pop('blah')
        cr3 = control.pop()
        assert_eq()
        self.assert_(dr3 is cr3)

    def test_lifecycle(self):
        class Foo(object):
            pass

        canary = Canary()
        creator = entity_maker
        attributes.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', True, extension=canary, useobject=True)

        obj = Foo()
        col1 = obj.attr

        e1 = creator()
        obj.attr.append(e1)

        e2 = creator()
        bulk1 = [e2]
        # empty & sever col1 from obj
        obj.attr = bulk1
        self.assert_(len(col1) == 0)
        self.assert_(len(canary.data) == 1)
        self.assert_(obj.attr is not col1)
        self.assert_(obj.attr is not bulk1)
        self.assert_(obj.attr == bulk1)

        e3 = creator()
        col1.append(e3)
        self.assert_(e3 not in canary.data)
        self.assert_(collections.collection_adapter(col1) is None)

        obj.attr[0] = e3
        self.assert_(e3 in canary.data)

class DictHelpersTest(ORMTest):
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
