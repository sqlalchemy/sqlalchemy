from test.orm import _fixtures
from sqlalchemy.testing import fixtures, assert_raises, eq_, ne_, \
    assert_raises_message
from sqlalchemy.orm import mapper, Session, validates, relationship
from sqlalchemy.testing.mock import Mock, call
from sqlalchemy import exc


class ValidatorTest(_fixtures.FixtureTest):
    def test_scalar(self):
        users = self.tables.users
        canary = Mock()

        class User(fixtures.ComparableEntity):
            @validates('name')
            def validate_name(self, key, name):
                canary(key, name)
                ne_(name, 'fred')
                return name + ' modified'

        mapper(User, users)
        sess = Session()
        u1 = User(name='ed')
        eq_(u1.name, 'ed modified')
        assert_raises(AssertionError, setattr, u1, "name", "fred")
        eq_(u1.name, 'ed modified')
        eq_(canary.mock_calls, [call('name', 'ed'), call('name', 'fred')])

        sess.add(u1)
        sess.commit()

        eq_(
            sess.query(User).filter_by(name='ed modified').one(),
            User(name='ed')
        )

    def test_collection(self):
        users, addresses, Address = (self.tables.users,
                                     self.tables.addresses,
                                     self.classes.Address)

        canary = Mock()

        class User(fixtures.ComparableEntity):
            @validates('addresses')
            def validate_address(self, key, ad):
                canary(key, ad)
                assert '@' in ad.email_address
                return ad

        mapper(User, users, properties={
            'addresses': relationship(Address)}
        )
        mapper(Address, addresses)
        sess = Session()
        u1 = User(name='edward')
        a0 = Address(email_address='noemail')
        assert_raises(AssertionError, u1.addresses.append, a0)
        a1 = Address(id=15, email_address='foo@bar.com')
        u1.addresses.append(a1)
        eq_(canary.mock_calls, [call('addresses', a0), call('addresses', a1)])
        sess.add(u1)
        sess.commit()

        eq_(
            sess.query(User).filter_by(name='edward').one(),
            User(name='edward', addresses=[
                 Address(email_address='foo@bar.com')])
        )

    def test_validators_dict(self):
        users, addresses, Address = (self.tables.users,
                                     self.tables.addresses,
                                     self.classes.Address)

        class User(fixtures.ComparableEntity):

            @validates('name')
            def validate_name(self, key, name):
                ne_(name, 'fred')
                return name + ' modified'

            @validates('addresses')
            def validate_address(self, key, ad):
                assert '@' in ad.email_address
                return ad

            def simple_function(self, key, value):
                return key, value

        u_m = mapper(User, users, properties={
            'addresses': relationship(Address)})
        mapper(Address, addresses)

        eq_(
            dict((k, v[0].__name__) for k, v in list(u_m.validators.items())),
            {'name': 'validate_name',
             'addresses': 'validate_address'}
        )

    def test_validator_w_removes(self):
        users, addresses, Address = (self.tables.users,
                                     self.tables.addresses,
                                     self.classes.Address)
        canary = Mock()

        class User(fixtures.ComparableEntity):

            @validates('name', include_removes=True)
            def validate_name(self, key, item, remove):
                canary(key, item, remove)
                return item

            @validates('addresses', include_removes=True)
            def validate_address(self, key, item, remove):
                canary(key, item, remove)
                return item

        mapper(User, users, properties={
            'addresses': relationship(Address)
        })
        mapper(Address, addresses)

        u1 = User()
        u1.name = "ed"
        u1.name = "mary"
        del u1.name
        a1, a2, a3 = Address(), Address(), Address()
        u1.addresses.append(a1)
        u1.addresses.remove(a1)
        u1.addresses = [a1, a2]
        u1.addresses = [a2, a3]

        eq_(canary.mock_calls, [
            call('name', 'ed', False),
            call('name', 'mary', False),
            call('name', 'mary', True),
            # append a1
            call('addresses', a1, False),
            # remove a1
            call('addresses', a1, True),
            # set to [a1, a2] - this is two appends
            call('addresses', a1, False), call('addresses', a2, False),
            # set to [a2, a3] - this is a remove of a1,
            # append of a3.  the appends are first.
            # in 1.2 due to #3896, we also get 'a2' in the
            # validates as it is part of the set
            call('addresses', a2, False),
            call('addresses', a3, False),
            call('addresses', a1, True),
        ])

    def test_validator_bulk_collection_set(self):
        users, addresses, Address = (self.tables.users,
                                     self.tables.addresses,
                                     self.classes.Address)

        class User(fixtures.ComparableEntity):

            @validates('addresses', include_removes=True)
            def validate_address(self, key, item, remove):
                if not remove:
                    assert isinstance(item, str)
                else:
                    assert isinstance(item, Address)
                item = Address(email_address=item)
                return item

        mapper(User, users, properties={
            'addresses': relationship(Address)
        })
        mapper(Address, addresses)

        u1 = User()
        u1.addresses.append("e1")
        u1.addresses.append("e2")
        eq_(
            u1.addresses,
            [Address(email_address="e1"), Address(email_address="e2")]
        )
        u1.addresses = ["e3", "e4"]
        eq_(
            u1.addresses,
            [Address(email_address="e3"), Address(email_address="e4")]
        )

    def test_validator_multi_warning(self):
        users = self.tables.users

        class Foo(object):
            @validates("name")
            def validate_one(self, key, value):
                pass

            @validates("name")
            def validate_two(self, key, value):
                pass

        assert_raises_message(
            exc.InvalidRequestError,
            "A validation function for mapped attribute "
            "'name' on mapper Mapper|Foo|users already exists",
            mapper, Foo, users
        )

        class Bar(object):
            @validates("id")
            def validate_three(self, key, value):
                return value + 10

            @validates("id", "name")
            def validate_four(self, key, value):
                return value + "foo"

        assert_raises_message(
            exc.InvalidRequestError,
            "A validation function for mapped attribute "
            "'name' on mapper Mapper|Bar|users already exists",
            mapper, Bar, users
        )

    def test_validator_wo_backrefs_wo_removes(self):
        self._test_validator_backrefs(False, False)

    def test_validator_wo_backrefs_w_removes(self):
        self._test_validator_backrefs(False, True)

    def test_validator_w_backrefs_wo_removes(self):
        self._test_validator_backrefs(True, False)

    def test_validator_w_backrefs_w_removes(self):
        self._test_validator_backrefs(True, True)

    def _test_validator_backrefs(self, include_backrefs, include_removes):
        users, addresses = (self.tables.users,
                            self.tables.addresses)
        canary = Mock()

        class User(fixtures.ComparableEntity):

            if include_removes:
                @validates('addresses', include_removes=True,
                           include_backrefs=include_backrefs)
                def validate_address(self, key, item, remove):
                    canary(key, item, remove)
                    return item
            else:
                @validates('addresses', include_removes=False,
                           include_backrefs=include_backrefs)
                def validate_address(self, key, item):
                    canary(key, item)
                    return item

        class Address(fixtures.ComparableEntity):
            if include_removes:
                @validates('user', include_backrefs=include_backrefs,
                           include_removes=True)
                def validate_user(self, key, item, remove):
                    canary(key, item, remove)
                    return item
            else:
                @validates('user', include_backrefs=include_backrefs)
                def validate_user(self, key, item):
                    canary(key, item)
                    return item

        mapper(User, users, properties={
            'addresses': relationship(Address, backref="user")
        })
        mapper(Address, addresses)

        u1 = User()
        u2 = User()
        a1, a2 = Address(), Address()

        # 3 append/set, two removes
        u1.addresses.append(a1)
        u1.addresses.append(a2)
        a2.user = u2
        del a1.user
        u2.addresses.remove(a2)

        # copy, so that generation of the
        # comparisons don't get caught
        calls = list(canary.mock_calls)

        if include_backrefs:
            if include_removes:
                eq_(calls,
                    [
                        # append #1
                        call('addresses', Address(), False),

                        # backref for append
                        call('user', User(addresses=[]), False),

                        # append #2
                        call('addresses', Address(user=None), False),

                        # backref for append
                        call('user', User(addresses=[]), False),

                        # assign a2.user = u2
                        call('user', User(addresses=[]), False),

                        # backref for u1.addresses.remove(a2)
                        call('addresses', Address(user=None), True),

                        # backref for u2.addresses.append(a2)
                        call('addresses', Address(user=None), False),

                        # del a1.user
                        call('user', User(addresses=[]), True),

                        # backref for u1.addresses.remove(a1)
                        call('addresses', Address(), True),

                        # u2.addresses.remove(a2)
                        call('addresses', Address(user=None), True),

                        # backref for a2.user = None
                        call('user', None, False)
                    ])
            else:
                eq_(calls,
                    [
                        call('addresses', Address()),
                        call('user', User(addresses=[])),
                        call('addresses', Address(user=None)),
                        call('user', User(addresses=[])),
                        call('user', User(addresses=[])),
                        call('addresses', Address(user=None)),
                        call('user', None)
                    ])
        else:
            if include_removes:
                eq_(calls,
                    [
                        call('addresses', Address(), False),
                        call('addresses', Address(user=None), False),
                        call('user', User(addresses=[]), False),
                        call('user', User(addresses=[]), True),
                        call('addresses', Address(user=None), True)
                    ])
            else:
                eq_(calls,
                    [
                        call('addresses', Address()),
                        call('addresses', Address(user=None)),
                        call('user', User(addresses=[]))
                    ])
