import testbase
from sqlalchemy import *
from sqlalchemy.orm import create_session, mapper, relation
import sqlalchemy.orm.collections as collections
from sqlalchemy.orm.collections import collection
from sqlalchemy import util


class CollectionsTest(testbase.PersistTest):
    # FIXME: ...
    pass

class DictsTest(testbase.ORMTest):
    def define_tables(self, metadata):
        global parents, children, Parent, Child
        
        parents = Table('parents', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('label', String))
        children = Table('children', metadata,
                         Column('id', Integer, primary_key=True),
                         Column('parent_id', Integer, ForeignKey('parents.id'), nullable=False),
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

        assert set(p.children.keys()) == set(['foo', 'bar'])
        cid = p.children['foo'].id

        collections.collection_adapter(p.children).append_with_event(
            Child('foo', 'newvalue'))
        
        session.save(p)
        session.flush()
        session.clear()
        
        p = session.query(Parent).get(pid)
        
        assert set(p.children.keys()) == set(['foo', 'bar'])
        assert p.children['foo'].id != cid
        
        assert(len(list(collections.collection_adapter(p.children))) == 2)
        session.flush()
        session.clear()

        p = session.query(Parent).get(pid)
        assert(len(list(collections.collection_adapter(p.children))) == 2)

        collections.collection_adapter(p.children).remove_with_event(
            p.children['foo'])
        
        assert(len(list(collections.collection_adapter(p.children))) == 1)
        session.flush()
        session.clear()

        p = session.query(Parent).get(pid)
        assert(len(list(collections.collection_adapter(p.children))) == 1)

        del p.children['bar']
        assert(len(list(collections.collection_adapter(p.children))) == 0)
        session.flush()
        session.clear()

        p = session.query(Parent).get(pid)
        assert(len(list(collections.collection_adapter(p.children))) == 0)
        

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

        assert set(p.children.keys()) == set([('foo', '1'), ('foo', '2')])
        cid = p.children[('foo', '1')].id

        collections.collection_adapter(p.children).append_with_event(
            Child('foo', '1', 'newvalue'))
        
        session.save(p)
        session.flush()
        session.clear()
        
        p = session.query(Parent).get(pid)
        
        assert set(p.children.keys()) == set([('foo', '1'), ('foo', '2')])
        assert p.children[('foo', '1')].id != cid
        
        assert(len(list(collections.collection_adapter(p.children))) == 2)
        
    def test_mapped_collection(self):
        return
        collection_class = collections.mapped_collection(lambda c: c.a)
        self._test_scalar_mapped(collection_class)

    def test_mapped_collection2(self):
        return
        collection_class = collections.mapped_collection(lambda c: (c.a, c.b))
        self._test_composite_mapped(collection_class)

    def test_attr_mapped_collection(self):
        return
        collection_class = collections.attribute_mapped_collection('a')
        self._test_scalar_mapped(collection_class)

    def test_column_mapped_collection(self):
        return
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
