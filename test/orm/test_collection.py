from sqlalchemy.testing import eq_, ne_
import sys
from operator import and_

import sqlalchemy.orm.collections as collections
from sqlalchemy.orm.collections import collection

import sqlalchemy as sa
from sqlalchemy import Integer, String, ForeignKey, text
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy import util, exc as sa_exc
from sqlalchemy.orm import create_session, mapper, relationship, \
    attributes, instrumentation
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import assert_raises, assert_raises_message


class Canary(sa.orm.interfaces.AttributeExtension):
    def __init__(self):
        self.data = set()
        self.added = set()
        self.removed = set()

    def append(self, obj, value, initiator):
        assert value not in self.added
        self.data.add(value)
        self.added.add(value)
        return value

    def remove(self, obj, value, initiator):
        assert value not in self.removed
        self.data.remove(value)
        self.removed.add(value)

    def set(self, obj, value, oldvalue, initiator):
        if isinstance(value, str):
            value = CollectionsTest.entity_maker()

        if oldvalue is not None:
            self.remove(obj, oldvalue, None)
        self.append(obj, value, None)
        return value


class CollectionsTest(fixtures.ORMTest):
    class Entity(object):
        def __init__(self, a=None, b=None, c=None):
            self.a = a
            self.b = b
            self.c = c

        def __repr__(self):
            return str((id(self), self.a, self.b, self.c))

    @classmethod
    def setup_class(cls):
        instrumentation.register_class(cls.Entity)

    @classmethod
    def teardown_class(cls):
        instrumentation.unregister_class(cls.Entity)
        super(CollectionsTest, cls).teardown_class()

    _entity_id = 1

    @classmethod
    def entity_maker(cls):
        cls._entity_id += 1
        return cls.Entity(cls._entity_id)

    @classmethod
    def dictable_entity(cls, a=None, b=None, c=None):
        id = cls._entity_id = (cls._entity_id + 1)
        return cls.Entity(a or str(id), b or 'value %s' % id, c)

    def _test_adapter(self, typecallable, creator=None, to_set=None):
        if creator is None:
            creator = self.entity_maker

        class Foo(object):
            pass

        canary = Canary()
        instrumentation.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', uselist=True,
                                      extension=canary,
                                      typecallable=typecallable,
                                      useobject=True)

        obj = Foo()
        adapter = collections.collection_adapter(obj.attr)
        direct = obj.attr
        if to_set is None:
            def to_set(col): return set(col)

        def assert_eq():
            self.assert_(to_set(direct) == canary.data)
            self.assert_(set(adapter) == canary.data)

        def assert_ne():
            self.assert_(to_set(direct) != canary.data)

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

    def _test_list(self, typecallable, creator=None):
        if creator is None:
            creator = self.entity_maker

        class Foo(object):
            pass

        canary = Canary()
        instrumentation.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', uselist=True,
                                      extension=canary,
                                      typecallable=typecallable,
                                      useobject=True)

        obj = Foo()
        adapter = collections.collection_adapter(obj.attr)
        direct = obj.attr
        control = list()

        def assert_eq():
            eq_(set(direct), canary.data)
            eq_(set(adapter), canary.data)
            eq_(direct, control)

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

            if util.reduce(and_, [hasattr(direct, a) for a in
                                  ('__delitem__', 'insert', '__len__')], True):
                values = [creator(), creator(), creator(), creator()]
                direct[slice(0, 1)] = values
                control[slice(0, 1)] = values
                assert_eq()

                values = [creator(), creator()]
                direct[slice(0, -1, 2)] = values
                control[slice(0, -1, 2)] = values
                assert_eq()

                values = [creator()]
                direct[slice(0, -1)] = values
                control[slice(0, -1)] = values
                assert_eq()

                values = [creator(), creator(), creator()]
                control[:] = values
                direct[:] = values

                def invalid():
                    direct[slice(0, 6, 2)] = [creator()]
                assert_raises(ValueError, invalid)

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

        if hasattr(direct, '__setitem__') or hasattr(direct, '__setslice__'):

            values = [creator(), creator()]
            direct[:] = values
            control[:] = values
            assert_eq()

            # test slice assignment where
            # slice size goes over the number of items
            values = [creator(), creator()]
            direct[1:3] = values
            control[1:3] = values
            assert_eq()

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

            values = [creator(), creator()]
            direct[-1:-3] = values
            control[-1:-3] = values
            assert_eq()

            values = [creator(), creator()]
            direct[-2:-1] = values
            control[-2:-1] = values
            assert_eq()

            values = [creator()]
            direct[0:0] = values
            control[0:0] = values
            assert_eq()

        if hasattr(direct, '__delitem__') or hasattr(direct, '__delslice__'):
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

        if hasattr(direct, 'clear'):
            for i in range(1, 4):
                e = creator()
                direct.append(e)
                control.append(e)

            direct.clear()
            control.clear()
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

    def _test_list_bulk(self, typecallable, creator=None):
        if creator is None:
            creator = self.entity_maker

        class Foo(object):
            pass

        canary = Canary()
        instrumentation.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', uselist=True,
                                      extension=canary,
                                      typecallable=typecallable,
                                      useobject=True)

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

    def test_list_setitem_with_slices(self):

        # this is a "list" that has no __setslice__
        # or __delslice__ methods.  The __setitem__
        # and __delitem__ must therefore accept
        # slice objects (i.e. as in py3k)
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

            def __len__(self):
                return len(self.data)

            def __setitem__(self, key, value):
                self.data[key] = value

            def __getitem__(self, key):
                return self.data[key]

            def __delitem__(self, key):
                del self.data[key]

            def __iter__(self):
                return iter(self.data)
            __hash__ = object.__hash__

            def __eq__(self, other):
                return self.data == other

            def __repr__(self):
                return 'ListLike(%s)' % repr(self.data)

        self._test_adapter(ListLike)
        self._test_list(ListLike)
        self._test_list_bulk(ListLike)

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
            __hash__ = object.__hash__

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
            __hash__ = object.__hash__

            def __eq__(self, other):
                return self.data == other

            def __repr__(self):
                return 'ListIsh(%s)' % repr(self.data)

        self._test_adapter(ListIsh)
        self._test_list(ListIsh)
        self._test_list_bulk(ListIsh)
        self.assert_(getattr(ListIsh, '_sa_instrumented') == id(ListIsh))

    def _test_set(self, typecallable, creator=None):
        if creator is None:
            creator = self.entity_maker

        class Foo(object):
            pass

        canary = Canary()
        instrumentation.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', uselist=True,
                                      extension=canary,
                                      typecallable=typecallable,
                                      useobject=True)

        obj = Foo()
        adapter = collections.collection_adapter(obj.attr)
        direct = obj.attr
        control = set()

        def assert_eq():
            eq_(set(direct), canary.data)
            eq_(set(adapter), canary.data)
            eq_(direct, control)

        def addall(*values):
            for item in values:
                direct.add(item)
                control.add(item)
            assert_eq()

        def zap():
            for item in list(direct):
                direct.remove(item)
            control.clear()

        addall(creator())

        e = creator()
        addall(e)
        addall(e)

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

            values = frozenset([e, creator()])
            obj.attr |= values
            control |= values
            assert_eq()

            try:
                direct |= [e, creator()]
                assert False
            except TypeError:
                assert True

        addall(creator(), creator())
        direct.clear()
        control.clear()
        assert_eq()

        # note: the clear test previously needs
        # to have executed in order for this to
        # pass in all cases; else there's the possibility
        # of non-deterministic behavior.
        addall(creator())
        direct.pop()
        control.pop()
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

            values = frozenset([creator()])
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

    def _test_set_bulk(self, typecallable, creator=None):
        if creator is None:
            creator = self.entity_maker

        class Foo(object):
            pass

        canary = Canary()
        instrumentation.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', uselist=True,
                                      extension=canary,
                                      typecallable=typecallable,
                                      useobject=True)

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

            def clear(self):
                self.data.clear()

            def pop(self):
                return self.data.pop()

            def update(self, other):
                self.data.update(other)

            def __iter__(self):
                return iter(self.data)
            __hash__ = object.__hash__

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

            def clear(self):
                self.data.clear()
            __hash__ = object.__hash__

            def __eq__(self, other):
                return self.data == other

        self._test_adapter(SetIsh)
        self._test_set(SetIsh)
        self._test_set_bulk(SetIsh)
        self.assert_(getattr(SetIsh, '_sa_instrumented') == id(SetIsh))

    def _test_dict(self, typecallable, creator=None):
        if creator is None:
            creator = self.dictable_entity

        class Foo(object):
            pass

        canary = Canary()
        instrumentation.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', uselist=True,
                                      extension=canary,
                                      typecallable=typecallable,
                                      useobject=True)

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

    def _test_dict_bulk(self, typecallable, creator=None):
        if creator is None:
            creator = self.dictable_entity

        class Foo(object):
            pass

        canary = Canary()
        instrumentation.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', uselist=True,
                                      extension=canary,
                                      typecallable=typecallable,
                                      useobject=True)

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
        self.assert_(
            set(collections.collection_adapter(obj.attr)) == set([e2]))
        self.assert_(e1 in canary.removed)
        self.assert_(e2 in canary.added)

        # key validity on bulk assignment is a basic feature of
        # MappedCollection but is not present in basic, @converter-less
        # dict collections.
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
            eq_(set(collections.collection_adapter(obj.attr)),
                set([e2]))
            self.assert_(e3 not in canary.added)
        else:
            real_dict = dict(keyignored1=e3)
            obj.attr = real_dict
            self.assert_(obj.attr is not real_dict)
            self.assert_('keyignored1' not in obj.attr)
            eq_(set(collections.collection_adapter(obj.attr)),
                set([e3]))
            self.assert_(e2 in canary.removed)
            self.assert_(e3 in canary.added)

        obj.attr = typecallable()
        eq_(list(collections.collection_adapter(obj.attr)), [])

        e4 = creator()
        try:
            obj.attr = [e4]
            self.assert_(False)
        except TypeError:
            self.assert_(e4 not in canary.data)

    def test_dict(self):
        assert_raises_message(
            sa_exc.ArgumentError,
            'Type InstrumentedDict must elect an appender '
            'method to be a collection class',
            self._test_adapter, dict, self.dictable_entity,
            to_set=lambda c: set(c.values())
        )

        assert_raises_message(
            sa_exc.ArgumentError,
            'Type InstrumentedDict must elect an appender method '
            'to be a collection class',
            self._test_dict, dict
        )

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

        self._test_adapter(MyDict, self.dictable_entity,
                           to_set=lambda c: set(c.values()))
        self._test_dict(MyDict)
        self._test_dict_bulk(MyDict)
        self.assert_(getattr(MyDict, '_sa_instrumented') == id(MyDict))

    def test_dict_subclass2(self):
        class MyEasyDict(collections.MappedCollection):
            def __init__(self):
                super(MyEasyDict, self).__init__(lambda e: e.a)

        self._test_adapter(MyEasyDict, self.dictable_entity,
                           to_set=lambda c: set(c.values()))
        self._test_dict(MyEasyDict)
        self._test_dict_bulk(MyEasyDict)
        self.assert_(getattr(MyEasyDict, '_sa_instrumented') == id(MyEasyDict))

    def test_dict_subclass3(self):
        class MyOrdered(util.OrderedDict, collections.MappedCollection):
            def __init__(self):
                collections.MappedCollection.__init__(self, lambda e: e.a)
                util.OrderedDict.__init__(self)

        self._test_adapter(MyOrdered, self.dictable_entity,
                           to_set=lambda c: set(c.values()))
        self._test_dict(MyOrdered)
        self._test_dict_bulk(MyOrdered)
        self.assert_(getattr(MyOrdered, '_sa_instrumented') == id(MyOrdered))

    def test_dict_subclass4(self):
        # tests #2654
        class MyDict(collections.MappedCollection):
            def __init__(self):
                super(MyDict, self).__init__(lambda value: "k%d" % value)

            @collection.converter
            def _convert(self, dictlike):
                for key, value in dictlike.items():
                    yield value + 5

        class Foo(object):
            pass

        canary = Canary()

        instrumentation.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', uselist=True,
                                      extension=canary,
                                      typecallable=MyDict, useobject=True)

        f = Foo()
        f.attr = {"k1": 1, "k2": 2}

        eq_(f.attr, {'k7': 7, 'k6': 6})

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
                return list(self.data.values())

            def __contains__(self, key):
                return key in self.data

            @collection.iterator
            def itervalues(self):
                return iter(self.data.values())
            __hash__ = object.__hash__

            def __eq__(self, other):
                return self.data == other

            def __repr__(self):
                return 'DictLike(%s)' % repr(self.data)

        self._test_adapter(DictLike, self.dictable_entity,
                           to_set=lambda c: set(c.values()))
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
                return list(self.data.values())

            def __contains__(self, key):
                return key in self.data

            @collection.iterator
            def itervalues(self):
                return iter(self.data.values())
            __hash__ = object.__hash__

            def __eq__(self, other):
                return self.data == other

            def __repr__(self):
                return 'DictIsh(%s)' % repr(self.data)

        self._test_adapter(DictIsh, self.dictable_entity,
                           to_set=lambda c: set(c.values()))
        self._test_dict(DictIsh)
        self._test_dict_bulk(DictIsh)
        self.assert_(getattr(DictIsh, '_sa_instrumented') == id(DictIsh))

    def _test_object(self, typecallable, creator=None):
        if creator is None:
            creator = self.entity_maker

        class Foo(object):
            pass

        canary = Canary()
        instrumentation.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', uselist=True,
                                      extension=canary,
                                      typecallable=typecallable,
                                      useobject=True)

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
            __hash__ = object.__hash__

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
            __hash__ = object.__hash__

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
        instrumentation.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', uselist=True,
                                      extension=canary,
                                      typecallable=Custom, useobject=True)

        obj = Foo()
        adapter = collections.collection_adapter(obj.attr)
        direct = obj.attr
        control = list()

        def assert_eq():
            self.assert_(set(direct) == canary.data)
            self.assert_(set(adapter) == canary.data)
            self.assert_(list(direct) == control)
        creator = self.entity_maker

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
        creator = self.entity_maker
        instrumentation.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', uselist=True,
                                      extension=canary, useobject=True)

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


class DictHelpersTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('parents', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('label', String(128)))
        Table('children', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('parent_id', Integer, ForeignKey('parents.id'),
                     nullable=False),
              Column('a', String(128)),
              Column('b', String(128)),
              Column('c', String(128)))

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            def __init__(self, label=None):
                self.label = label

        class Child(cls.Basic):
            def __init__(self, a=None, b=None, c=None):
                self.a = a
                self.b = b
                self.c = c

    def _test_scalar_mapped(self, collection_class):
        parents, children, Parent, Child = (self.tables.parents,
                                            self.tables.children,
                                            self.classes.Parent,
                                            self.classes.Child)

        mapper(Child, children)
        mapper(Parent, parents, properties={
            'children': relationship(Child, collection_class=collection_class,
                                     cascade="all, delete-orphan")})

        p = Parent()
        p.children['foo'] = Child('foo', 'value')
        p.children['bar'] = Child('bar', 'value')
        session = create_session()
        session.add(p)
        session.flush()
        pid = p.id
        session.expunge_all()

        p = session.query(Parent).get(pid)

        eq_(set(p.children.keys()), set(['foo', 'bar']))
        cid = p.children['foo'].id

        collections.collection_adapter(p.children).append_with_event(
            Child('foo', 'newvalue'))

        session.flush()
        session.expunge_all()

        p = session.query(Parent).get(pid)

        self.assert_(set(p.children.keys()) == set(['foo', 'bar']))
        self.assert_(p.children['foo'].id != cid)

        self.assert_(
            len(list(collections.collection_adapter(p.children))) == 2)
        session.flush()
        session.expunge_all()

        p = session.query(Parent).get(pid)
        self.assert_(
            len(list(collections.collection_adapter(p.children))) == 2)

        collections.collection_adapter(p.children).remove_with_event(
            p.children['foo'])

        self.assert_(
            len(list(collections.collection_adapter(p.children))) == 1)
        session.flush()
        session.expunge_all()

        p = session.query(Parent).get(pid)
        self.assert_(
            len(list(collections.collection_adapter(p.children))) == 1)

        del p.children['bar']
        self.assert_(
            len(list(collections.collection_adapter(p.children))) == 0)
        session.flush()
        session.expunge_all()

        p = session.query(Parent).get(pid)
        self.assert_(
            len(list(collections.collection_adapter(p.children))) == 0)

    def _test_composite_mapped(self, collection_class):
        parents, children, Parent, Child = (self.tables.parents,
                                            self.tables.children,
                                            self.classes.Parent,
                                            self.classes.Child)

        mapper(Child, children)
        mapper(Parent, parents, properties={
            'children': relationship(Child, collection_class=collection_class,
                                     cascade="all, delete-orphan")
        })

        p = Parent()
        p.children[('foo', '1')] = Child('foo', '1', 'value 1')
        p.children[('foo', '2')] = Child('foo', '2', 'value 2')

        session = create_session()
        session.add(p)
        session.flush()
        pid = p.id
        session.expunge_all()

        p = session.query(Parent).get(pid)

        self.assert_(
            set(p.children.keys()) == set([('foo', '1'), ('foo', '2')]))
        cid = p.children[('foo', '1')].id

        collections.collection_adapter(p.children).append_with_event(
            Child('foo', '1', 'newvalue'))

        session.flush()
        session.expunge_all()

        p = session.query(Parent).get(pid)

        self.assert_(
            set(p.children.keys()) == set([('foo', '1'), ('foo', '2')]))
        self.assert_(p.children[('foo', '1')].id != cid)

        self.assert_(
            len(list(collections.collection_adapter(p.children))) == 2)

    def test_mapped_collection(self):
        collection_class = collections.mapped_collection(lambda c: c.a)
        self._test_scalar_mapped(collection_class)

    def test_mapped_collection2(self):
        collection_class = collections.mapped_collection(lambda c: (c.a, c.b))
        self._test_composite_mapped(collection_class)

    def test_attr_mapped_collection(self):
        collection_class = collections.attribute_mapped_collection('a')
        self._test_scalar_mapped(collection_class)

    def test_declarative_column_mapped(self):
        """test that uncompiled attribute usage works with
        column_mapped_collection"""

        from sqlalchemy.ext.declarative import declarative_base

        BaseObject = declarative_base()

        class Foo(BaseObject):
            __tablename__ = "foo"
            id = Column(Integer(), primary_key=True)
            bar_id = Column(Integer, ForeignKey('bar.id'))

        for spec, obj, expected in (
            (Foo.id, Foo(id=3), 3),
            ((Foo.id, Foo.bar_id), Foo(id=3, bar_id=12), (3, 12))
        ):
            eq_(
                collections.column_mapped_collection(spec)().keyfunc(obj),
                expected
            )

    def test_column_mapped_assertions(self):
        assert_raises_message(sa_exc.ArgumentError,
                              "Column-based expression object expected "
                              "for argument 'mapping_spec'; got: 'a'",
                              collections.column_mapped_collection, 'a')
        assert_raises_message(sa_exc.ArgumentError,
                              "Column-based expression object expected "
                              "for argument 'mapping_spec'; got: 'a'",
                              collections.column_mapped_collection,
                              text('a'))

    def test_column_mapped_collection(self):
        children = self.tables.children

        collection_class = collections.column_mapped_collection(
            children.c.a)
        self._test_scalar_mapped(collection_class)

    def test_column_mapped_collection2(self):
        children = self.tables.children

        collection_class = collections.column_mapped_collection(
            (children.c.a, children.c.b))
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

        def collection_class():
            return Ordered2(lambda v: (v.a, v.b))
        self._test_composite_mapped(collection_class)


class ColumnMappedWSerialize(fixtures.MappedTest):
    """test the column_mapped_collection serializer against
    multi-table and indirect table edge cases, including
    serialization."""

    run_create_tables = run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table('foo', metadata,
              Column('id', Integer(), primary_key=True),
              Column('b', String(128)))
        Table('bar', metadata,
              Column('id', Integer(), primary_key=True),
              Column('foo_id', Integer, ForeignKey('foo.id')),
              Column('bat_id', Integer),
              schema="x")

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

        class Bar(Foo):
            pass

    def test_indirect_table_column_mapped(self):
        Foo = self.classes.Foo
        Bar = self.classes.Bar
        bar = self.tables["x.bar"]
        mapper(Foo, self.tables.foo, properties={
            "foo_id": self.tables.foo.c.id
        })
        mapper(Bar, bar, inherits=Foo, properties={
            "bar_id": bar.c.id,
        })

        bar_spec = Bar(foo_id=1, bar_id=2, bat_id=3)
        self._run_test([
            (Foo.foo_id, bar_spec, 1),
            ((Bar.bar_id, Bar.bat_id), bar_spec, (2, 3)),
            (Bar.foo_id, bar_spec, 1),
            (bar.c.id, bar_spec, 2),
        ])

    def test_selectable_column_mapped(self):
        from sqlalchemy import select
        s = select([self.tables.foo]).alias()
        Foo = self.classes.Foo
        mapper(Foo, s)
        self._run_test([
            (Foo.b, Foo(b=5), 5),
            (s.c.b, Foo(b=5), 5)
        ])

    def _run_test(self, specs):
        from sqlalchemy.testing.util import picklers
        for spec, obj, expected in specs:
            coll = collections.column_mapped_collection(spec)()
            eq_(
                coll.keyfunc(obj),
                expected
            )
            # ensure we do the right thing with __reduce__
            for loads, dumps in picklers():
                c2 = loads(dumps(coll))
                eq_(c2.keyfunc(obj), expected)
                c3 = loads(dumps(c2))
                eq_(c3.keyfunc(obj), expected)


class CustomCollectionsTest(fixtures.MappedTest):
    """test the integration of collections with mapped classes."""

    @classmethod
    def define_tables(cls, metadata):
        Table('sometable', metadata,
              Column('col1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(30)))
        Table('someothertable', metadata,
              Column('col1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('scol1', Integer,
                     ForeignKey('sometable.col1')),
              Column('data', String(20)))

    def test_basic(self):
        someothertable, sometable = self.tables.someothertable, \
            self.tables.sometable

        class MyList(list):
            pass

        class Foo(object):
            pass

        class Bar(object):
            pass

        mapper(Foo, sometable, properties={
            'bars': relationship(Bar, collection_class=MyList)
        })
        mapper(Bar, someothertable)
        f = Foo()
        assert isinstance(f.bars, MyList)

    def test_lazyload(self):
        """test that a 'set' can be used as a collection and can lazyload."""

        someothertable, sometable = self.tables.someothertable, \
            self.tables.sometable

        class Foo(object):
            pass

        class Bar(object):
            pass
        mapper(Foo, sometable, properties={
            'bars': relationship(Bar, collection_class=set)
        })
        mapper(Bar, someothertable)
        f = Foo()
        f.bars.add(Bar())
        f.bars.add(Bar())
        sess = create_session()
        sess.add(f)
        sess.flush()
        sess.expunge_all()
        f = sess.query(Foo).get(f.col1)
        assert len(list(f.bars)) == 2
        f.bars.clear()

    def test_dict(self):
        """test that a 'dict' can be used as a collection and can lazyload."""

        someothertable, sometable = self.tables.someothertable, \
            self.tables.sometable

        class Foo(object):
            pass

        class Bar(object):
            pass

        class AppenderDict(dict):
            @collection.appender
            def set(self, item):
                self[id(item)] = item

            @collection.remover
            def remove(self, item):
                if id(item) in self:
                    del self[id(item)]

        mapper(Foo, sometable, properties={
            'bars': relationship(Bar, collection_class=AppenderDict)
        })
        mapper(Bar, someothertable)
        f = Foo()
        f.bars.set(Bar())
        f.bars.set(Bar())
        sess = create_session()
        sess.add(f)
        sess.flush()
        sess.expunge_all()
        f = sess.query(Foo).get(f.col1)
        assert len(list(f.bars)) == 2
        f.bars.clear()

    def test_dict_wrapper(self):
        """test that the supplied 'dict' wrapper can be used as a
        collection and can lazyload."""

        someothertable, sometable = self.tables.someothertable, \
            self.tables.sometable

        class Foo(object):
            pass

        class Bar(object):
            def __init__(self, data): self.data = data

        mapper(Foo, sometable, properties={
            'bars': relationship(
                Bar, collection_class=collections.column_mapped_collection(
                    someothertable.c.data))
        })
        mapper(Bar, someothertable)

        f = Foo()
        col = collections.collection_adapter(f.bars)
        col.append_with_event(Bar('a'))
        col.append_with_event(Bar('b'))
        sess = create_session()
        sess.add(f)
        sess.flush()
        sess.expunge_all()
        f = sess.query(Foo).get(f.col1)
        assert len(list(f.bars)) == 2

        strongref = list(f.bars.values())
        existing = set([id(b) for b in strongref])

        col = collections.collection_adapter(f.bars)
        col.append_with_event(Bar('b'))
        f.bars['a'] = Bar('a')
        sess.flush()
        sess.expunge_all()
        f = sess.query(Foo).get(f.col1)
        assert len(list(f.bars)) == 2

        replaced = set([id(b) for b in list(f.bars.values())])
        ne_(existing, replaced)

    def test_list(self):
        self._test_list(list)

    def test_list_no_setslice(self):
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

            def __len__(self):
                return len(self.data)

            def __setitem__(self, key, value):
                self.data[key] = value

            def __getitem__(self, key):
                return self.data[key]

            def __delitem__(self, key):
                del self.data[key]

            def __iter__(self):
                return iter(self.data)
            __hash__ = object.__hash__

            def __eq__(self, other):
                return self.data == other

            def __repr__(self):
                return 'ListLike(%s)' % repr(self.data)

        self._test_list(ListLike)

    def _test_list(self, listcls):
        someothertable, sometable = self.tables.someothertable, \
            self.tables.sometable

        class Parent(object):
            pass

        class Child(object):
            pass

        mapper(Parent, sometable, properties={
            'children': relationship(Child, collection_class=listcls)
        })
        mapper(Child, someothertable)

        control = list()
        p = Parent()

        o = Child()
        control.append(o)
        p.children.append(o)
        assert control == p.children
        assert control == list(p.children)

        o = [Child(), Child(), Child(), Child()]
        control.extend(o)
        p.children.extend(o)
        assert control == p.children
        assert control == list(p.children)

        assert control[0] == p.children[0]
        assert control[-1] == p.children[-1]
        assert control[1:3] == p.children[1:3]

        del control[1]
        del p.children[1]
        assert control == p.children
        assert control == list(p.children)

        o = [Child()]
        control[1:3] = o

        p.children[1:3] = o
        assert control == p.children
        assert control == list(p.children)

        o = [Child(), Child(), Child(), Child()]
        control[1:3] = o
        p.children[1:3] = o
        assert control == p.children
        assert control == list(p.children)

        o = [Child(), Child(), Child(), Child()]
        control[-1:-2] = o
        p.children[-1:-2] = o
        assert control == p.children
        assert control == list(p.children)

        o = [Child(), Child(), Child(), Child()]
        control[4:] = o
        p.children[4:] = o
        assert control == p.children
        assert control == list(p.children)

        o = Child()
        control.insert(0, o)
        p.children.insert(0, o)
        assert control == p.children
        assert control == list(p.children)

        o = Child()
        control.insert(3, o)
        p.children.insert(3, o)
        assert control == p.children
        assert control == list(p.children)

        o = Child()
        control.insert(999, o)
        p.children.insert(999, o)
        assert control == p.children
        assert control == list(p.children)

        del control[0:1]
        del p.children[0:1]
        assert control == p.children
        assert control == list(p.children)

        del control[1:1]
        del p.children[1:1]
        assert control == p.children
        assert control == list(p.children)

        del control[1:3]
        del p.children[1:3]
        assert control == p.children
        assert control == list(p.children)

        del control[7:]
        del p.children[7:]
        assert control == p.children
        assert control == list(p.children)

        assert control.pop() == p.children.pop()
        assert control == p.children
        assert control == list(p.children)

        assert control.pop(0) == p.children.pop(0)
        assert control == p.children
        assert control == list(p.children)

        assert control.pop(2) == p.children.pop(2)
        assert control == p.children
        assert control == list(p.children)

        o = Child()
        control.insert(2, o)
        p.children.insert(2, o)
        assert control == p.children
        assert control == list(p.children)

        control.remove(o)
        p.children.remove(o)
        assert control == p.children
        assert control == list(p.children)

    def test_custom(self):
        someothertable, sometable = self.tables.someothertable, \
            self.tables.sometable

        class Parent(object):
            pass

        class Child(object):
            pass

        class MyCollection(object):
            def __init__(self):
                self.data = []

            @collection.appender
            def append(self, value):
                self.data.append(value)

            @collection.remover
            def remove(self, value):
                self.data.remove(value)

            @collection.iterator
            def __iter__(self):
                return iter(self.data)

        mapper(Parent, sometable, properties={
            'children': relationship(Child, collection_class=MyCollection)
        })
        mapper(Child, someothertable)

        control = list()
        p1 = Parent()

        o = Child()
        control.append(o)
        p1.children.append(o)
        assert control == list(p1.children)

        o = Child()
        control.append(o)
        p1.children.append(o)
        assert control == list(p1.children)

        o = Child()
        control.append(o)
        p1.children.append(o)
        assert control == list(p1.children)

        sess = create_session()
        sess.add(p1)
        sess.flush()
        sess.expunge_all()

        p2 = sess.query(Parent).get(p1.col1)
        o = list(p2.children)
        assert len(o) == 3


class InstrumentationTest(fixtures.ORMTest):
    def test_uncooperative_descriptor_in_sweep(self):
        class DoNotTouch(object):
            def __get__(self, obj, owner):
                raise AttributeError

        class Touchy(list):
            no_touch = DoNotTouch()

        assert 'no_touch' in Touchy.__dict__
        assert not hasattr(Touchy, 'no_touch')
        assert 'no_touch' in dir(Touchy)

        collections._instrument_class(Touchy)

    def test_name_setup(self):

        class Base(object):
            @collection.iterator
            def base_iterate(self, x):
                return "base_iterate"

            @collection.appender
            def base_append(self, x):
                return "base_append"

            @collection.converter
            def base_convert(self, x):
                return "base_convert"

            @collection.remover
            def base_remove(self, x):
                return "base_remove"

        from sqlalchemy.orm.collections import _instrument_class
        _instrument_class(Base)

        eq_(Base._sa_remover(Base(), 5), "base_remove")
        eq_(Base._sa_appender(Base(), 5), "base_append")
        eq_(Base._sa_iterator(Base(), 5), "base_iterate")
        eq_(Base._sa_converter(Base(), 5), "base_convert")

        class Sub(Base):
            @collection.converter
            def base_convert(self, x):
                return "sub_convert"

            @collection.remover
            def sub_remove(self, x):
                return "sub_remove"
        _instrument_class(Sub)

        eq_(Sub._sa_appender(Sub(), 5), "base_append")
        eq_(Sub._sa_remover(Sub(), 5), "sub_remove")
        eq_(Sub._sa_iterator(Sub(), 5), "base_iterate")
        eq_(Sub._sa_converter(Sub(), 5), "sub_convert")

    def test_link_event(self):
        canary = []

        class Collection(list):
            @collection.linker
            def _on_link(self, obj):
                canary.append(obj)

        class Foo(object):
            pass

        instrumentation.register_class(Foo)
        attributes.register_attribute(Foo, 'attr', uselist=True,
                                      typecallable=Collection, useobject=True)

        f1 = Foo()
        f1.attr.append(3)

        eq_(canary, [f1.attr._sa_adapter])
        adapter_1 = f1.attr._sa_adapter

        l2 = Collection()
        f1.attr = l2
        eq_(canary, [adapter_1, f1.attr._sa_adapter, None])

    def test_referenced_by_owner(self):

        class Foo(object):
            pass

        instrumentation.register_class(Foo)
        attributes.register_attribute(
            Foo, 'attr', uselist=True, useobject=True)

        f1 = Foo()
        f1.attr.append(3)

        adapter = collections.collection_adapter(f1.attr)
        assert adapter._referenced_by_owner

        f1.attr = []
        assert not adapter._referenced_by_owner
