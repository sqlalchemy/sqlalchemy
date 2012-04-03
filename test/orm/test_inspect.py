"""test the inspection registry system."""

from test.lib.testing import eq_, assert_raises
from sqlalchemy import exc, util
from sqlalchemy import inspect
from test.orm import _fixtures
from sqlalchemy.orm import class_mapper, synonym
from sqlalchemy.orm.attributes import instance_state

class TestORMInspection(_fixtures.FixtureTest):
    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()
        inspect(cls.classes.User).add_property(
            "name_syn",synonym("name")
        )

    def test_class_mapper(self):
        User = self.classes.User

        assert inspect(User) is class_mapper(User)

    def test_instance_state(self):
        User = self.classes.User
        u1 = User()

        assert inspect(u1) is instance_state(u1)

    def test_synonyms(self):
        User = self.classes.User
        syn = inspect(User).synonyms

        # TODO: some of the synonym debacle in 0.7
        # has led User.name_syn.property to be the 
        # ColumnProperty.  not sure if we want that
        # implicit jump in there though, perhaps get Query/etc. to 
        # call upon "effective_property" or something like that

        eq_(inspect(User).synonyms, {
            "name_syn":class_mapper(User).get_property("name_syn")
        })

    # TODO: test all these accessors...

"""
# column collection
>>> b.columns
[<id column>, <name column>]

# its a ColumnCollection
>>> b.columns.id
<id column>

# i.e. from mapper
>>> b.primary_key
(<id column>, )

# i.e. from mapper
>>> b.local_table
<user table>

# ColumnProperty
>>> b.attr.id.columns
[<id column>]

# but perhaps we use a collection with some helpers
>>> b.attr.id.columns.first
<id column>

# and a mapper?  its None since this is a column
>>> b.attr.id.mapper
None

# attr is basically the _props
>>> b.attr.keys()
['id', 'name', 'name_syn', 'addresses']

# b itself is likely just the mapper
>>> b
<User mapper>

# get only column attributes
>>> b.column_attrs
[<id prop>, <name prop>]

# its a namespace
>>> b.column_attrs.id
<id prop>

# get only synonyms
>>> b.synonyms
[<name syn prop>]

# get only relationships
>>> b.relationships
[<addresses prop>]

# its a namespace
>>> b.relationships.addresses
<addresses prop>

# point inspect() at a class level attribute,
# basically returns ".property"
>>> b = inspect(User.addresses)
>>> b
<addresses prop>

# mapper
>>> b.mapper
<Address mapper>

# None columns collection, just like columnprop has empty mapper
>>> b.columns
None

# the parent
>>> b.parent
<User mapper>

# __clause_element__()
>>> b.expression
User.id==Address.user_id

>>> inspect(User.id).expression
<id column with ORM annotations>


# inspect works on instances !  
>>> u1 = User(id=3, name='x')
>>> b = inspect(u1)

# what's b here ?  probably InstanceState
>>> b
<InstanceState>

>>> b.attr.keys()
['id', 'name', 'name_syn', 'addresses']

# this is class level stuff - should this require b.mapper.columns ?
>>> b.columns
[<id column>, <name column>]

# does this return '3'?  or an object?
>>> b.attr.id
<magic attribute inspect thing>

# or does this ?
>>> b.attr.id.value 
3

>>> b.attr.id.history
<history object>

>>> b.attr.id.history.unchanged
3

>>> b.attr.id.history.deleted
None

# lets assume the object is persistent
>>> s = Session()
>>> s.add(u1)
>>> s.commit()

# big one - the primary key identity !  always
# works in query.get()
>>> b.identity
[3]

# the mapper level key
>>> b.identity_key
(User, [3])

>>> b.persistent
True

>>> b.transient
False

>>> b.deleted
False

>>> b.detached
False

>>> b.session
<session>

# the object.  this navigates obj()
# of course, would be nice if it was b.obj...
>>> b.object_
<User instance u1>

"""
