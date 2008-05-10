import testenv; testenv.configure_for_tests()
from testlib import sa, testing
from testlib.sa import Table, Column, Integer, String, ForeignKey
from testlib.sa.orm import mapper, relation, backref, create_session
from testlib.testing import eq_
from orm import _base


class EntityTest(_base.MappedTest):
    """Mappers scoped to an entity_name"""

    def define_tables(self, metadata):
        Table('user1', metadata,
            Column('user_id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('name', String(60), nullable=False))

        Table('user2', metadata,
            Column('user_id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('name', String(60), nullable=False))

        Table('address1', metadata,
            Column('address_id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
              Column('user_id', Integer, ForeignKey('user1.user_id'),
                     nullable=False),
              Column('email', String(100), nullable=False))

        Table('address2', metadata,
              Column('address_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('user_id', Integer, ForeignKey('user2.user_id'),
                     nullable=False),
              Column('email', String(100), nullable=False))

    def setup_classes(self):
        class User(_base.BasicEntity):
            pass

        class Address(_base.BasicEntity):
            pass

        class Address1(_base.BasicEntity):
            pass

        class Address2(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_entity_name_assignment_by_extension(self):
        """
        Tests a pair of one-to-many mapper structures, establishing that both
        parent and child objects honor the "entity_name" attribute attached to
        the object instances.

        """
        ctx = sa.orm.scoped_session(create_session)

        ma1 = mapper(Address, address1, entity_name='address1',
                     extension=ctx.extension)
        ma2 = mapper(Address, address2, entity_name='address2',
                     extension=ctx.extension)

        mapper(User, user1, entity_name='user1',
               extension=ctx.extension,
               properties=dict(addresses=relation(ma1)))

        mapper(User, user2, entity_name='user2',
               extension=ctx.extension,
               properties=dict(addresses=relation(ma2)))

        u1 = User(name='this is user 1', _sa_entity_name='user1')
        a1 = Address(email='a1@foo.com', _sa_entity_name='address1')
        u1.addresses.append(a1)

        u2 = User(name='this is user 2', _sa_entity_name='user2')
        a2 = Address(email='a2@foo.com', _sa_entity_name='address2')
        u2.addresses.append(a2)

        ctx.flush()
        eq_(user1.select().execute().fetchall(), [(u1.user_id, u1.name)])
        eq_(user2.select().execute().fetchall(), [(u2.user_id, u2.name)])
        eq_(address1.select().execute().fetchall(),
            [(a1.address_id, u1.user_id, 'a1@foo.com')])
        eq_(address2.select().execute().fetchall(),
            [(a1.address_id, u2.user_id, 'a2@foo.com')])

        ctx.clear()
        u1list = ctx.query(User, entity_name='user1').all()
        u2list = ctx.query(User, entity_name='user2').all()
        eq_(len(u1list), 1)
        eq_(len(u2list), 1)
        assert u1list[0] is not u2list[0]

        eq_(len(u1list[0].addresses), 1)
        eq_(len(u2list[0].addresses), 1)

        u1 = ctx.query(User, entity_name='user1').first()
        ctx.refresh(u1)
        ctx.expire(u1)

    @testing.resolve_artifact_names
    def test_cascade(self):
        ma1 = mapper(Address, address1, entity_name='address1')
        ma2 = mapper(Address, address2, entity_name='address2')

        mapper(User, user1, entity_name='user1', properties=dict(
            addresses=relation(ma1)))
        mapper(User, user2, entity_name='user2', properties=dict(
            addresses=relation(ma2)))

        sess = create_session()
        u1 = User(name='this is user 1')
        sess.add(u1, entity_name='user1')

        a1 = Address(email='a1@foo.com')
        u1.addresses.append(a1)

        u2 = User(name='this is user 2')
        a2 = Address(email='a2@foo.com')
        u2.addresses.append(a2)
        sess.add(u2, entity_name='user2')

        sess.flush()
        eq_(user1.select().execute().fetchall(), [(u1.user_id, u1.name)])
        eq_(user2.select().execute().fetchall(), [(u2.user_id, u2.name)])
        eq_(address1.select().execute().fetchall(),
            [(a1.address_id, u1.user_id, 'a1@foo.com')])
        eq_(address2.select().execute().fetchall(),
            [(a1.address_id, u2.user_id, 'a2@foo.com')])

        sess.clear()
        u1list = sess.query(User, entity_name='user1').all()
        u2list = sess.query(User, entity_name='user2').all()
        eq_(len(u1list), 1)
        eq_(len(u2list), 1)
        assert u1list[0] is not u2list[0]
        eq_(len(u1list[0].addresses), 1)
        eq_(len(u2list[0].addresses), 1)

    @testing.resolve_artifact_names
    def test_polymorphic(self):
        """entity_name can be used to have two kinds of relations on the same class."""
        ctx = sa.orm.scoped_session(create_session)

        ma1 = mapper(Address1, address1, extension=ctx.extension)
        ma2 = mapper(Address2, address2, extension=ctx.extension)

        mapper(User, user1, entity_name='user1', extension=ctx.extension,
               properties=dict(
                 addresses=relation(ma1)))

        mapper(User, user2, entity_name='user2', extension=ctx.extension,
               properties=dict(
                 addresses=relation(ma2)))

        u1 = User(name='this is user 1', _sa_entity_name='user1')
        a1 = Address1(email='a1@foo.com')
        u1.addresses.append(a1)

        u2 = User(name='this is user 2', _sa_entity_name='user2')
        a2 = Address2(email='a2@foo.com')
        u2.addresses.append(a2)

        ctx.flush()
        eq_(user1.select().execute().fetchall(), [(u1.user_id, u1.name)])
        eq_(user2.select().execute().fetchall(), [(u2.user_id, u2.name)])
        eq_(address1.select().execute().fetchall(),
            [(a1.address_id, u1.user_id, 'a1@foo.com')])
        eq_(address2.select().execute().fetchall(),
            [(a1.address_id, u2.user_id, 'a2@foo.com')])

        ctx.clear()
        u1list = ctx.query(User, entity_name='user1').all()
        u2list = ctx.query(User, entity_name='user2').all()
        eq_(len(u1list), 1)
        eq_(len(u2list), 1)
        assert u1list[0] is not u2list[0]
        eq_(len(u1list[0].addresses), 1)
        eq_(len(u2list[0].addresses), 1)

        # the lazy load requires that setup_loader() check that the correct
        # LazyLoader is setting up for each load
        assert isinstance(u1list[0].addresses[0], Address1)
        assert isinstance(u2list[0].addresses[0], Address2)

    @testing.resolve_artifact_names
    def testpolymorphic_deferred(self):
        """Deferred columns load properly using entity names"""

        mapper(User, user1, entity_name='user1', properties=dict(
            name=sa.orm.deferred(user1.c.name)))
        mapper(User, user2, entity_name='user2', properties=dict(
            name=sa.orm.deferred(user2.c.name)))

        u1 = User(name='this is user 1')
        u2 = User(name='this is user 2')

        session = create_session()
        session.add(u1, entity_name='user1')
        session.add(u2, entity_name='user2')
        session.flush()

        eq_(user1.select().execute().fetchall(), [(u1.user_id, u1.name)])
        eq_(user2.select().execute().fetchall(), [(u2.user_id, u2.name)])

        session.clear()
        u1list = session.query(User, entity_name='user1').all()
        u2list = session.query(User, entity_name='user2').all()

        eq_(len(u1list), 1)
        eq_(len(u2list), 1)
        assert u1list[0] is not u2list[0]

        # the deferred column load requires that setup_loader() check that the
        # correct DeferredColumnLoader is setting up for each load
        eq_(u1list[0].name, 'this is user 1')
        eq_(u2list[0].name, 'this is user 2')


class SelfReferentialTest(_base.MappedTest):
    def define_tables(self, metadata):
        Table('nodes', metadata,
              Column('id', Integer, primary_key=True),
              Column('parent_id', Integer, ForeignKey('nodes.id')),
              Column('data', String(50)),
              Column('type', String(50)))

    def setup_classes(self):
        class Node(_base.ComparableEntity):
            pass

    # fails inconsistently.  entity name needs deterministic instrumentation.
    @testing.resolve_artifact_names
    def FIXME_test_relation(self):
        foonodes = nodes.select().where(nodes.c.type=='foo').alias()
        barnodes = nodes.select().where(nodes.c.type=='bar').alias()

        # TODO: the order of instrumentation here is not deterministic;
        # therefore the test fails sporadically since "Node.data" references
        # different mappers at different times
        m1 = mapper(Node, nodes)
        m2 = mapper(Node, foonodes, entity_name='foo')
        m3 = mapper(Node, barnodes, entity_name='bar')

        m1.add_property('foonodes', relation(
            m2,
            primaryjoin=nodes.c.id == foonodes.c.parent_id,
            backref=backref('foo_parent',
                            remote_side=nodes.c.id,
                            primaryjoin=nodes.c.id==foonodes.c.parent_id)))

        m1.add_property('barnodes', relation(
            m3,
            primaryjoin=nodes.c.id==barnodes.c.parent_id,
            backref=backref('bar_parent',
                            remote_side=nodes.c.id,
                            primaryjoin=nodes.c.id==barnodes.c.parent_id)))

        sess = create_session()

        n1 = Node(data='n1', type='bat')
        n1.foonodes.append(Node(data='n2', type='foo'))
        Node(data='n3', type='bar', bar_parent=n1)
        sess.add(n1)
        sess.flush()
        sess.clear()

        eq_(sess.query(Node, entity_name="bar").one(),
            Node(data='n3'))
        eq_(sess.query(Node).filter(Node.data=='n1').one(),
            Node(data='n1',
                 foonodes=[Node(data='n2')],
                 barnodes=[Node(data='n3')]))


if __name__ == "__main__":
    testenv.main()
