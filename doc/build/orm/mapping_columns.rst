.. _mapping_columns_toplevel:

.. currentmodule:: sqlalchemy.orm

Mapping Table Columns
=====================

The default behavior of :func:`_orm.mapper` is to assemble all the columns in
the mapped :class:`_schema.Table` into mapped object attributes, each of which are
named according to the name of the column itself (specifically, the ``key``
attribute of :class:`_schema.Column`).  This behavior can be
modified in several ways.

.. _mapper_column_distinct_names:

Naming Columns Distinctly from Attribute Names
----------------------------------------------

A mapping by default shares the same name for a
:class:`_schema.Column` as that of the mapped attribute - specifically
it matches the :attr:`_schema.Column.key` attribute on :class:`_schema.Column`, which
by default is the same as the :attr:`_schema.Column.name`.

The name assigned to the Python attribute which maps to
:class:`_schema.Column` can be different from either
:attr:`_schema.Column.name` or :attr:`_schema.Column.key` just by assigning
it that way, as we illustrate here in a Declarative mapping::

    class User(Base):
        __tablename__ = 'user'
        id = Column('user_id', Integer, primary_key=True)
        name = Column('user_name', String(50))

Where above ``User.id`` resolves to a column named ``user_id``
and ``User.name`` resolves to a column named ``user_name``.

When mapping to an existing table, the :class:`_schema.Column` object
can be referenced directly::

    class User(Base):
        __table__ = user_table
        id = user_table.c.user_id
        name = user_table.c.user_name

The corresponding technique for an :term:`imperative` mapping is
to place the desired key in the :paramref:`_orm.mapper.properties`
dictionary with the desired key::

    mapper_registry.map_imperatively(User, user_table, properties={
       'id': user_table.c.user_id,
       'name': user_table.c.user_name,
    })

In the next section we'll examine the usage of ``.key`` more closely.

.. _mapper_automated_reflection_schemes:

Automating Column Naming Schemes from Reflected Tables
------------------------------------------------------

In the previous section :ref:`mapper_column_distinct_names`, we showed how
a :class:`_schema.Column` explicitly mapped to a class can have a different attribute
name than the column.  But what if we aren't listing out :class:`_schema.Column`
objects explicitly, and instead are automating the production of :class:`_schema.Table`
objects using reflection (e.g. as described in :ref:`metadata_reflection_toplevel`)?
In this case we can make use of the :meth:`_events.DDLEvents.column_reflect` event
to intercept the production of :class:`_schema.Column` objects and provide them
with the :attr:`_schema.Column.key` of our choice.   The event is most easily
associated with the :class:`_schema.MetaData` object that's in use,
such as below we use the one linked to the :class:`_orm.declarative_base`
instance::

    @event.listens_for(Base.metadata, "column_reflect")
    def column_reflect(inspector, table, column_info):
        # set column.key = "attr_<lower_case_name>"
        column_info['key'] = "attr_%s" % column_info['name'].lower()

With the above event, the reflection of :class:`_schema.Column` objects will be intercepted
with our event that adds a new ".key" element, such as in a mapping as below::

    class MyClass(Base):
        __table__ = Table("some_table", Base.metadata,
                    autoload_with=some_engine)

The approach also works with the :ref:`automap_toplevel` extension.  See
the section :ref:`automap_intercepting_columns` for background.

.. seealso::

    :meth:`_events.DDLEvents.column_reflect`

    :ref:`automap_intercepting_columns` - in the :ref:`automap_toplevel` documentation

.. _column_prefix:

Naming All Columns with a Prefix
--------------------------------

A quick approach to prefix column names, typically when mapping
to an existing :class:`_schema.Table` object, is to use ``column_prefix``::

    class User(Base):
        __table__ = user_table
        __mapper_args__ = {'column_prefix':'_'}

The above will place attribute names such as ``_user_id``, ``_user_name``,
``_password`` etc. on the mapped ``User`` class.

This approach is uncommon in modern usage.   For dealing with reflected
tables, a more flexible approach is to use that described in
:ref:`mapper_automated_reflection_schemes`.

.. _column_property_options:

Using column_property for column level options
----------------------------------------------

Options can be specified when mapping a :class:`_schema.Column` using the
:func:`.column_property` function.  This function
explicitly creates the :class:`.ColumnProperty` used by the
:func:`.mapper` to keep track of the :class:`_schema.Column`; normally, the
:func:`.mapper` creates this automatically.   Using :func:`.column_property`,
we can pass additional arguments about how we'd like the :class:`_schema.Column`
to be mapped.   Below, we pass an option ``active_history``,
which specifies that a change to this column's value should
result in the former value being loaded first::

    from sqlalchemy.orm import column_property

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = column_property(Column(String(50)), active_history=True)

:func:`.column_property` is also used to map a single attribute to
multiple columns.  This use case arises when mapping to a :func:`_expression.join`
which has attributes which are equated to each other::

    class User(Base):
        __table__ = user.join(address)

        # assign "user.id", "address.user_id" to the
        # "id" attribute
        id = column_property(user_table.c.id, address_table.c.user_id)

For more examples featuring this usage, see :ref:`maptojoin`.

Another place where :func:`.column_property` is needed is to specify SQL expressions as
mapped attributes, such as below where we create an attribute ``fullname``
that is the string concatenation of the ``firstname`` and ``lastname``
columns::

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        firstname = Column(String(50))
        lastname = Column(String(50))
        fullname = column_property(firstname + " " + lastname)

See examples of this usage at :ref:`mapper_sql_expressions`.

.. autofunction:: column_property

.. _include_exclude_cols:

Mapping a Subset of Table Columns
---------------------------------

Sometimes, a :class:`_schema.Table` object was made available using the
reflection process described at :ref:`metadata_reflection` to load
the table's structure from the database.
For such a table that has lots of columns that don't need to be referenced
in the application, the ``include_properties`` or ``exclude_properties``
arguments can specify that only a subset of columns should be mapped.
For example::

    class User(Base):
        __table__ = user_table
        __mapper_args__ = {
            'include_properties' :['user_id', 'user_name']
        }

...will map the ``User`` class to the ``user_table`` table, only including
the ``user_id`` and ``user_name`` columns - the rest are not referenced.
Similarly::

    class Address(Base):
        __table__ = address_table
        __mapper_args__ = {
            'exclude_properties' : ['street', 'city', 'state', 'zip']
        }

...will map the ``Address`` class to the ``address_table`` table, including
all columns present except ``street``, ``city``, ``state``, and ``zip``.

When this mapping is used, the columns that are not included will not be
referenced in any SELECT statements emitted by :class:`_query.Query`, nor will there
be any mapped attribute on the mapped class which represents the column;
assigning an attribute of that name will have no effect beyond that of
a normal Python attribute assignment.

In some cases, multiple columns may have the same name, such as when
mapping to a join of two or more tables that share some column name.
``include_properties`` and ``exclude_properties`` can also accommodate
:class:`_schema.Column` objects to more accurately describe which columns
should be included or excluded::

    class UserAddress(Base):
        __table__ = user_table.join(addresses_table)
        __mapper_args__ = {
            'exclude_properties' :[address_table.c.id],
            'primary_key' : [user_table.c.id]
        }

.. note::

    insert and update defaults configured on individual :class:`_schema.Column`
    objects, i.e. those described at :ref:`metadata_defaults` including those
    configured by the :paramref:`_schema.Column.default`,
    :paramref:`_schema.Column.onupdate`, :paramref:`_schema.Column.server_default` and
    :paramref:`_schema.Column.server_onupdate` parameters, will continue to function
    normally even if those :class:`_schema.Column` objects are not mapped. This is
    because in the case of :paramref:`_schema.Column.default` and
    :paramref:`_schema.Column.onupdate`, the :class:`_schema.Column` object is still present
    on the underlying :class:`_schema.Table`, thus allowing the default functions to
    take place when the ORM emits an INSERT or UPDATE, and in the case of
    :paramref:`_schema.Column.server_default` and :paramref:`_schema.Column.server_onupdate`,
    the relational database itself emits these defaults as a server side
    behavior.


