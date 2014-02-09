# ext/automap.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Define an extension to the :mod:`sqlalchemy.ext.declarative` system
which automatically generates mapped classes and relationships from a database
schema, typically though not necessarily one which is reflected.

.. versionadded:: 0.9.1 Added :mod:`sqlalchemy.ext.automap`.

.. note::

    The :mod:`sqlalchemy.ext.automap` extension should be considered
    **experimental** as of 0.9.1.   Featureset and API stability is
    not guaranteed at this time.

It is hoped that the :class:`.AutomapBase` system provides a quick
and modernized solution to the problem that the very famous
`SQLSoup <https://sqlsoup.readthedocs.org/en/latest/>`_
also tries to solve, that of generating a quick and rudimentary object
model from an existing database on the fly.  By addressing the issue strictly
at the mapper configuration level, and integrating fully with existing
Declarative class techniques, :class:`.AutomapBase` seeks to provide
a well-integrated approach to the issue of expediently auto-generating ad-hoc
mappings.


Basic Use
=========

The simplest usage is to reflect an existing database into a new model.
We create a new :class:`.AutomapBase` class in a similar manner as to how
we create a declarative base class, using :func:`.automap_base`.
We then call :meth:`.AutomapBase.prepare` on the resulting base class,
asking it to reflect the schema and produce mappings::

    from sqlalchemy.ext.automap import automap_base
    from sqlalchemy.orm import Session
    from sqlalchemy import create_engine

    Base = automap_base()

    # engine, suppose it has two tables 'user' and 'address' set up
    engine = create_engine("sqlite:///mydatabase.db")

    # reflect the tables
    Base.prepare(engine, reflect=True)

    # mapped classes are now created with names by default
    # matching that of the table name.
    User = Base.classes.user
    Address = Base.classes.address

    session = Session(engine)

    # rudimentary relationships are produced
    session.add(Address(email_address="foo@bar.com", user=User(name="foo")))
    session.commit()

    # collection-based relationships are by default named "<classname>_collection"
    print (u1.address_collection)

Above, calling :meth:`.AutomapBase.prepare` while passing along the
:paramref:`.AutomapBase.prepare.reflect` parameter indicates that the
:meth:`.MetaData.reflect` method will be called on this declarative base
classes' :class:`.MetaData` collection; then, each viable
:class:`.Table` within the :class:`.MetaData` will get a new mapped class
generated automatically.  The :class:`.ForeignKeyConstraint` objects which
link the various tables together will be used to produce new, bidirectional
:func:`.relationship` objects between classes.   The classes and relationships
follow along a default naming scheme that we can customize.  At this point,
our basic mapping consisting of related ``User`` and ``Address`` classes is ready
to use in the traditional way.

Generating Mappings from an Existing MetaData
=============================================

We can pass a pre-declared :class:`.MetaData` object to :func:`.automap_base`.
This object can be constructed in any way, including programmatically, from
a serialized file, or from itself being reflected using :meth:`.MetaData.reflect`.
Below we illustrate a combination of reflection and explicit table declaration::

    from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey
    engine = create_engine("sqlite:///mydatabase.db")

    # produce our own MetaData object
    metadata = MetaData()

    # we can reflect it ourselves from a database, using options
    # such as 'only' to limit what tables we look at...
    metadata.reflect(engine, only=['user', 'address'])

    # ... or just define our own Table objects with it (or combine both)
    Table('user_order', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('user_id', ForeignKey('user.id'))
                )

    # we can then produce a set of mappings from this MetaData.
    Base = automap_base(metadata=metadata)

    # calling prepare() just sets up mapped classes and relationships.
    Base.prepare()

    # mapped classes are ready
    User, Address, Order = Base.classes.user, Base.classes.address, Base.classes.user_order

Specifying Classes Explcitly
============================

The :mod:`.sqlalchemy.ext.automap` extension allows classes to be defined
explicitly, in a way similar to that of the :class:`.DeferredReflection` class.
Classes that extend from :class:`.AutomapBase` act like regular declarative
classes, but are not immediately mapped after their construction, and are instead
mapped when we call :meth:`.AutomapBase.prepare`.  The :meth:`.AutomapBase.prepare`
method will make use of the classes we've established based on the table name
we use.  If our schema contains tables ``user`` and ``address``, we can define
one or both of the classes to be used::

    from sqlalchemy.ext.automap import automap_base
    from sqlalchemy import create_engine

    # automap base
    Base = automap_base()

    # pre-declare User for the 'user' table
    class User(Base):
        __tablename__ = 'user'

        # override schema elements like Columns
        user_name = Column('name', String)

        # override relationships too, if desired.
        # we must use the same name that automap would use for the relationship,
        # and also must refer to the class name that automap will generate
        # for "address"
        address_collection = relationship("address", collection_class=set)

    # reflect
    engine = create_engine("sqlite:///mydatabase.db")
    Base.prepare(engine, reflect=True)

    # we still have Address generated from the tablename "address",
    # but User is the same as Base.classes.User now

    Address = Base.classes.address

    u1 = session.query(User).first()
    print (u1.address_collection)

    # the backref is still there:
    a1 = session.query(Address).first()
    print (a1.user)

Above, one of the more intricate details is that we illustrated overriding
one of the :func:`.relationship` objects that automap would have created.
To do this, we needed to make sure the names match up with what automap
would normally generate, in that the relationship name would be ``User.address_collection``
and the name of the class referred to, from automap's perspective, is called
``address``, even though we are referring to it as ``Address`` within our usage
of this class.

Overriding Naming Schemes
=========================

:mod:`.sqlalchemy.ext.automap` is tasked with producing mapped classes and
relationship names based on a schema, which means it has decision points in how
these names are determined.  These three decision points are provided using
functions which can be passed to the :meth:`.AutomapBase.prepare` method, and
are known as :func:`.classname_for_table`,
:func:`.name_for_scalar_relationship`,
and :func:`.name_for_collection_relationship`.  Any or all of these
functions are provided as in the example below, where we use a "camel case"
scheme for class names and a "pluralizer" for collection names using the
`Inflect <https://pypi.python.org/pypi/inflect>`_ package::

    import re
    import inflect

    def camelize_classname(base, tablename, table):
        "Produce a 'camelized' class name, e.g. "
        "'words_and_underscores' -> 'WordsAndUnderscores'"

        return str(tablename[0].upper() + \\
                re.sub(r'_(\w)', lambda m: m.group(1).upper(), tablename[1:]))

    _pluralizer = inflect.engine()
    def pluralize_collection(base, local_cls, referred_cls, constraint):
        "Produce an 'uncamelized', 'pluralized' class name, e.g. "
        "'SomeTerm' -> 'some_terms'"

        referred_name = referred_cls.__name__
        uncamelized = referred_name[0].lower() + \\
                        re.sub(r'\W',
                                lambda m: "_%s" % m.group(0).lower(),
                                referred_name[1:])
        pluralized = _pluralizer.plural(uncamelized)
        return pluralized

    from sqlalchemy.ext.automap import automap_base

    Base = automap_base()

    engine = create_engine("sqlite:///mydatabase.db")

    Base.prepare(engine, reflect=True,
                classname_for_table=camelize_classname,
                name_for_collection_relationship=pluralize_collection
        )

From the above mapping, we would now have classes ``User`` and ``Address``,
where the collection from ``User`` to ``Address`` is called ``User.addresses``::

    User, Address = Base.classes.User, Base.classes.Address

    u1 = User(addresses=[Address(email="foo@bar.com")])

Relationship Detection
======================

The vast majority of what automap accomplishes is the generation of
:func:`.relationship` structures based on foreign keys.  The mechanism
by which this works for many-to-one and one-to-many relationships is as follows:

1. A given :class:`.Table`, known to be mapped to a particular class,
   is examined for :class:`.ForeignKeyConstraint` objects.

2. From each :class:`.ForeignKeyConstraint`, the remote :class:`.Table`
   object present is matched up to the class to which it is to be mapped,
   if any, else it is skipped.

3. As the :class:`.ForeignKeyConstraint` we are examining correponds to a reference
   from the immediate mapped class,
   the relationship will be set up as a many-to-one referring to the referred class;
   a corresponding one-to-many backref will be created on the referred class referring
   to this class.

4. The names of the relationships are determined using the
   :paramref:`.AutomapBase.prepare.name_for_scalar_relationship` and
   :paramref:`.AutomapBase.prepare.name_for_collection_relationship`
   callable functions.  It is important to note that the default relationship
   naming derives the name from the **the actual class name**.  If you've
   given a particular class an explicit name by declaring it, or specified an
   alternate class naming scheme, that's the name from which the relationship
   name will be derived.

5. The classes are inspected for an existing mapped property matching these
   names.  If one is detected on one side, but none on the other side, :class:`.AutomapBase`
   attempts to create a relationship on the missing side, then uses the
   :paramref:`.relationship.back_populates` parameter in order to point
   the new relationship to the other side.

6. In the usual case where no relationship is on either side,
   :meth:`.AutomapBase.prepare` produces a :func:`.relationship` on the "many-to-one"
   side and matches it to the other using the :paramref:`.relationship.backref`
   parameter.

7. Production of the :func:`.relationship` and optionally the :func:`.backref`
   is handed off to the :paramref:`.AutomapBase.prepare.generate_relationship`
   function, which can be supplied by the end-user in order to augment
   the arguments passed to :func:`.relationship` or :func:`.backref` or to
   make use of custom implementations of these functions.

Custom Relationship Arguments
-----------------------------

The :paramref:`.AutomapBase.prepare.generate_relationship` hook can be used
to add parameters to relationships.  For most cases, we can make use of the
existing :func:`.automap.generate_relationship` function to return
the object, after augmenting the given keyword dictionary with our own
arguments.

Below is an illustration of how to send
:paramref:`.relationship.cascade` and
:paramref:`.relationship.passive_deletes`
options along to all one-to-many relationships::

    from sqlalchemy.ext.automap import generate_relationship

    def _gen_relationship(base, direction, return_fn,
                                    attrname, local_cls, referred_cls, **kw):
        if direction is interfaces.ONETOMANY:
            kw['cascade'] = 'all, delete-orphan'
            kw['passive_deletes'] = True
        # make use of the built-in function to actually return
        # the result.
        return generate_relationship(base, direction, return_fn,
                                        attrname, local_cls, referred_cls, **kw)

    from sqlalchemy.ext.automap import automap_base
    from sqlalchemy import create_engine

    # automap base
    Base = automap_base()

    engine = create_engine("sqlite:///mydatabase.db")
    Base.prepare(engine, reflect=True,
                generate_relationship=_gen_relationship)

Many-to-Many relationships
--------------------------

:mod:`.sqlalchemy.ext.automap` will generate many-to-many relationships, e.g.
those which contain a ``secondary`` argument.  The process for producing these
is as follows:

1. A given :class:`.Table` is examined for :class:`.ForeignKeyConstraint` objects,
   before any mapped class has been assigned to it.

2. If the table contains two and exactly two :class:`.ForeignKeyConstraint`
   objects, and all columns within this table are members of these two
   :class:`.ForeignKeyConstraint` objects, the table is assumed to be a
   "secondary" table, and will **not be mapped directly**.

3. The two (or one, for self-referential) external tables to which the :class:`.Table`
   refers to are matched to the classes to which they will be mapped, if any.

4. If mapped classes for both sides are located, a many-to-many bi-directional
   :func:`.relationship` / :func:`.backref` pair is created between the two
   classes.

5. The override logic for many-to-many works the same as that of one-to-many/
   many-to-one; the :func:`.generate_relationship` function is called upon
   to generate the strucures and existing attributes will be maintained.

Using Automap with Explicit Declarations
========================================

As noted previously, automap has no dependency on reflection, and can make
use of any collection of :class:`.Table` objects within a :class:`.MetaData`
collection.  From this, it follows that automap can also be used
generate missing relationships given an otherwise complete model that fully defines
table metadata::

    from sqlalchemy.ext.automap import automap_base
    from sqlalchemy import Column, Integer, String, ForeignKey

    Base = automap_base()

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = Column(String)

    class Address(Base):
        __tablename__ = 'address'

        id = Column(Integer, primary_key=True)
        email = Column(String)
        user_id = Column(ForeignKey('user.id'))

    # produce relationships
    Base.prepare()

    # mapping is complete, with "address_collection" and
    # "user" relationships
    a1 = Address(email='u1')
    a2 = Address(email='u2')
    u1 = User(address_collection=[a1, a2])
    assert a1.user is u1

Above, given mostly complete ``User`` and ``Address`` mappings, the
:class:`.ForeignKey` which we defined on ``Address.user_id`` allowed a
bidirectional relationship pair ``Address.user`` and ``User.address_collection``
to be generated on the mapped classes.

Note that when subclassing :class:`.AutomapBase`, the :meth:`.AutomapBase.prepare`
method is required; if not called, the classes we've declared are in an
un-mapped state.


"""
from .declarative import declarative_base as _declarative_base
from .declarative.base import _DeferredMapperConfig
from ..sql import and_
from ..schema import ForeignKeyConstraint
from ..orm import relationship, backref, interfaces
from .. import util


def classname_for_table(base, tablename, table):
    """Return the class name that should be used, given the name
    of a table.

    The default implementation is::

        return str(tablename)

    Alternate implementations can be specified using the
    :paramref:`.AutomapBase.prepare.classname_for_table`
    parameter.

    :param base: the :class:`.AutomapBase` class doing the prepare.

    :param tablename: string name of the :class:`.Table`.

    :param table: the :class:`.Table` object itself.

    :return: a string class name.

     .. note::

        In Python 2, the string used for the class name **must** be a non-Unicode
        object, e.g. a ``str()`` object.  The ``.name`` attribute of
        :class:`.Table` is typically a Python unicode subclass, so the ``str()``
        function should be applied to this name, after accounting for any non-ASCII
        characters.

    """
    return str(tablename)

def name_for_scalar_relationship(base, local_cls, referred_cls, constraint):
    """Return the attribute name that should be used to refer from one
    class to another, for a scalar object reference.

    The default implementation is::

        return referred_cls.__name__.lower()

    Alternate implementations can be specified using the
    :paramref:`.AutomapBase.prepare.name_for_scalar_relationship`
    parameter.

    :param base: the :class:`.AutomapBase` class doing the prepare.

    :param local_cls: the class to be mapped on the local side.

    :param referred_cls: the class to be mapped on the referring side.

    :param constraint: the :class:`.ForeignKeyConstraint` that is being
     inspected to produce this relationship.

    """
    return referred_cls.__name__.lower()

def name_for_collection_relationship(base, local_cls, referred_cls, constraint):
    """Return the attribute name that should be used to refer from one
    class to another, for a collection reference.

    The default implementation is::

        return referred_cls.__name__.lower() + "_collection"

    Alternate implementations
    can be specified using the :paramref:`.AutomapBase.prepare.name_for_collection_relationship`
    parameter.

    :param base: the :class:`.AutomapBase` class doing the prepare.

    :param local_cls: the class to be mapped on the local side.

    :param referred_cls: the class to be mapped on the referring side.

    :param constraint: the :class:`.ForeignKeyConstraint` that is being
     inspected to produce this relationship.

    """
    return referred_cls.__name__.lower() + "_collection"

def generate_relationship(base, direction, return_fn, attrname, local_cls, referred_cls, **kw):
    """Generate a :func:`.relationship` or :func:`.backref` on behalf of two
    mapped classes.

    An alternate implementation of this function can be specified using the
    :paramref:`.AutomapBase.prepare.generate_relationship` parameter.

    The default implementation of this function is as follows::

        if return_fn is backref:
            return return_fn(attrname, **kw)
        elif return_fn is relationship:
            return return_fn(referred_cls, **kw)
        else:
            raise TypeError("Unknown relationship function: %s" % return_fn)

    :param base: the :class:`.AutomapBase` class doing the prepare.

    :param direction: indicate the "direction" of the relationship; this will
     be one of :data:`.ONETOMANY`, :data:`.MANYTOONE`, :data:`.MANYTOONE`.

    :param return_fn: the function that is used by default to create the
     relationship.  This will be either :func:`.relationship` or :func:`.backref`.
     The :func:`.backref` function's result will be used to produce a new
     :func:`.relationship` in a second step, so it is critical that user-defined
     implementations correctly differentiate between the two functions, if
     a custom relationship function is being used.

    :attrname: the attribute name to which this relationship is being assigned.
     If the value of :paramref:`.generate_relationship.return_fn` is the
     :func:`.backref` function, then this name is the name that is being
     assigned to the backref.

    :param local_cls: the "local" class to which this relationship or backref
     will be locally present.

    :param referred_cls: the "referred" class to which the relationship or backref
     refers to.

    :param \**kw: all additional keyword arguments are passed along to the
     function.

    :return: a :func:`.relationship` or :func:`.backref` construct, as dictated
     by the :paramref:`.generate_relationship.return_fn` parameter.

    """
    if return_fn is backref:
        return return_fn(attrname, **kw)
    elif return_fn is relationship:
        return return_fn(referred_cls, **kw)
    else:
        raise TypeError("Unknown relationship function: %s" % return_fn)

class AutomapBase(object):
    """Base class for an "automap" schema.

    The :class:`.AutomapBase` class can be compared to the "declarative base"
    class that is produced by the :func:`.declarative.declarative_base`
    function.  In practice, the :class:`.AutomapBase` class is always used
    as a mixin along with an actual declarative base.

    A new subclassable :class:`.AutomapBase` is typically instantated
    using the :func:`.automap_base` function.

    .. seealso::

        :ref:`automap_toplevel`

    """
    __abstract__ = True

    classes = None
    """An instance of :class:`.util.Properties` containing classes.

    This object behaves much like the ``.c`` collection on a table.  Classes
    are present under the name they were given, e.g.::

        Base = automap_base()
        Base.prepare(engine=some_engine, reflect=True)

        User, Address = Base.classes.User, Base.classes.Address

    """

    @classmethod
    def prepare(cls,
                engine=None,
                reflect=False,
                classname_for_table=classname_for_table,
                collection_class=list,
                name_for_scalar_relationship=name_for_scalar_relationship,
                name_for_collection_relationship=name_for_collection_relationship,
                generate_relationship=generate_relationship):

        """Extract mapped classes and relationships from the :class:`.MetaData` and
        perform mappings.

        :param engine: an :class:`.Engine` or :class:`.Connection` with which
         to perform schema reflection, if specified.
         If the :paramref:`.AutomapBase.prepare.reflect` argument is False, this
         object is not used.

        :param reflect: if True, the :meth:`.MetaData.reflect` method is called
         on the :class:`.MetaData` associated with this :class:`.AutomapBase`.
         The :class:`.Engine` passed via :paramref:`.AutomapBase.prepare.engine` will
         be used to perform the reflection if present; else, the :class:`.MetaData`
         should already be bound to some engine else the operation will fail.

        :param classname_for_table: callable function which will be used to
         produce new class names, given a table name.  Defaults to
         :func:`.classname_for_table`.

        :param name_for_scalar_relationship: callable function which will be used
         to produce relationship names for scalar relationships.  Defaults to
         :func:`.name_for_scalar_relationship`.

        :param name_for_collection_relationship: callable function which will be used
         to produce relationship names for collection-oriented relationships.  Defaults to
         :func:`.name_for_collection_relationship`.

        :param generate_relationship: callable function which will be used to
         actually generate :func:`.relationship` and :func:`.backref` constructs.
         Defaults to :func:`.generate_relationship`.

        :param collection_class: the Python collection class that will be used
         when a new :func:`.relationship` object is created that represents a
         collection.  Defaults to ``list``.

        """
        if reflect:
            cls.metadata.reflect(
                        engine,
                        extend_existing=True,
                        autoload_replace=False
                    )

        table_to_map_config = dict(
                                (m.local_table, m)
                                for m in _DeferredMapperConfig.
                                    classes_for_base(cls, sort=False)
                            )

        many_to_many = []

        for table in cls.metadata.tables.values():
            lcl_m2m, rem_m2m, m2m_const = _is_many_to_many(cls, table)
            if lcl_m2m is not None:
                many_to_many.append((lcl_m2m, rem_m2m, m2m_const, table))
            elif not table.primary_key:
                continue
            elif table not in table_to_map_config:
                mapped_cls = type(
                    classname_for_table(cls, table.name, table),
                    (cls, ),
                    {"__table__": table}
                )
                map_config = _DeferredMapperConfig.config_for_cls(mapped_cls)
                cls.classes[map_config.cls.__name__] = mapped_cls
                table_to_map_config[table] = map_config

        for map_config in table_to_map_config.values():
            _relationships_for_fks(cls,
                            map_config,
                            table_to_map_config,
                            collection_class,
                            name_for_scalar_relationship,
                            name_for_collection_relationship,
                            generate_relationship)

        for lcl_m2m, rem_m2m, m2m_const, table in many_to_many:
            _m2m_relationship(cls, lcl_m2m, rem_m2m, m2m_const, table,
                            table_to_map_config,
                            collection_class,
                            name_for_scalar_relationship,
                            name_for_collection_relationship,
                            generate_relationship)

        for map_config in _DeferredMapperConfig.classes_for_base(cls):
            map_config.map()


    _sa_decl_prepare = True
    """Indicate that the mapping of classes should be deferred.

    The presence of this attribute name indicates to declarative
    that the call to mapper() should not occur immediately; instead,
    information about the table and attributes to be mapped are gathered
    into an internal structure called _DeferredMapperConfig.  These
    objects can be collected later using classes_for_base(), additional
    mapping decisions can be made, and then the map() method will actually
    apply the mapping.

    The only real reason this deferral of the whole
    thing is needed is to support primary key columns that aren't reflected
    yet when the class is declared; everything else can theoretically be
    added to the mapper later.  However, the _DeferredMapperConfig is a
    nice interface in any case which exists at that not usually exposed point
    at which declarative has the class and the Table but hasn't called
    mapper() yet.

    """

def automap_base(declarative_base=None, **kw):
    """Produce a declarative automap base.

    This function produces a new base class that is a product of the
    :class:`.AutomapBase` class as well a declarative base produced by
    :func:`.declarative.declarative_base`.

    All parameters other than ``declarative_base`` are keyword arguments
    that are passed directly to the :func:`.declarative.declarative_base`
    function.

    :param declarative_base: an existing class produced by
     :func:`.declarative.declarative_base`.  When this is passed, the function
     no longer invokes :func:`.declarative.declarative_base` itself, and all other
     keyword arguments are ignored.

    :param \**kw: keyword arguments are passed along to
     :func:`.declarative.declarative_base`.

    """
    if declarative_base is None:
        Base = _declarative_base(**kw)
    else:
        Base = declarative_base

    return type(
                Base.__name__,
                (AutomapBase, Base,),
                {"__abstract__": True, "classes": util.Properties({})}
            )

def _is_many_to_many(automap_base, table):
    fk_constraints = [const for const in table.constraints
                    if isinstance(const, ForeignKeyConstraint)]
    if len(fk_constraints) != 2:
        return None, None, None

    cols = sum(
                [[fk.parent for fk in fk_constraint.elements]
                for fk_constraint in fk_constraints], [])

    if set(cols) != set(table.c):
        return None, None, None

    return (
        fk_constraints[0].elements[0].column.table,
        fk_constraints[1].elements[0].column.table,
        fk_constraints
    )

def _relationships_for_fks(automap_base, map_config, table_to_map_config,
                                collection_class,
                                name_for_scalar_relationship,
                                name_for_collection_relationship,
                                generate_relationship):
    local_table = map_config.local_table
    local_cls = map_config.cls

    if local_table is None:
        return
    for constraint in local_table.constraints:
        if isinstance(constraint, ForeignKeyConstraint):
            fks = constraint.elements
            referred_table = fks[0].column.table
            referred_cfg = table_to_map_config.get(referred_table, None)
            if referred_cfg is None:
                continue
            referred_cls = referred_cfg.cls

            relationship_name = name_for_scalar_relationship(
                                        automap_base,
                                        local_cls,
                                        referred_cls, constraint)
            backref_name = name_for_collection_relationship(
                                        automap_base,
                                        referred_cls,
                                        local_cls,
                                        constraint
                                    )

            create_backref = backref_name not in referred_cfg.properties

            if relationship_name not in map_config.properties:
                if create_backref:
                    backref_obj = generate_relationship(automap_base,
                                        interfaces.ONETOMANY, backref,
                                        backref_name, referred_cls, local_cls,
                                        collection_class=collection_class)
                else:
                    backref_obj = None
                map_config.properties[relationship_name] = \
                    generate_relationship(automap_base,
                        interfaces.MANYTOONE,
                        relationship,
                        relationship_name,
                        local_cls, referred_cls,
                        foreign_keys=[fk.parent for fk in constraint.elements],
                        backref=backref_obj,
                        remote_side=[fk.column for fk in constraint.elements]
                    )
                if not create_backref:
                    referred_cfg.properties[backref_name].back_populates = relationship_name
            elif create_backref:
                referred_cfg.properties[backref_name] = \
                    generate_relationship(automap_base,
                        interfaces.ONETOMANY,
                        relationship,
                        backref_name,
                        referred_cls, local_cls,
                        foreign_keys=[fk.parent for fk in constraint.elements],
                        back_populates=relationship_name,
                        collection_class=collection_class)
                map_config.properties[relationship_name].back_populates = backref_name

def _m2m_relationship(automap_base, lcl_m2m, rem_m2m, m2m_const, table,
                            table_to_map_config,
                            collection_class,
                            name_for_scalar_relationship,
                            name_for_collection_relationship,
                            generate_relationship):

    map_config = table_to_map_config.get(lcl_m2m, None)
    referred_cfg = table_to_map_config.get(rem_m2m, None)
    if map_config is None or referred_cfg is None:
        return

    local_cls = map_config.cls
    referred_cls = referred_cfg.cls

    relationship_name = name_for_collection_relationship(
                                automap_base,
                                local_cls,
                                referred_cls, m2m_const[0])
    backref_name = name_for_collection_relationship(
                                automap_base,
                                referred_cls,
                                local_cls,
                                m2m_const[1]
                            )

    create_backref = backref_name not in referred_cfg.properties

    if relationship_name not in map_config.properties:
        if create_backref:
            backref_obj = generate_relationship(automap_base,
                            interfaces.MANYTOMANY,
                            backref,
                            backref_name,
                            referred_cls, local_cls,
                            collection_class=collection_class
                            )
        else:
            backref_obj = None
        map_config.properties[relationship_name] = \
            generate_relationship(automap_base,
                interfaces.MANYTOMANY,
                relationship,
                relationship_name,
                local_cls, referred_cls,
                secondary=table,
                primaryjoin=and_(fk.column == fk.parent for fk in m2m_const[0].elements),
                secondaryjoin=and_(fk.column == fk.parent for fk in m2m_const[1].elements),
                backref=backref_obj,
                collection_class=collection_class
                )
        if not create_backref:
            referred_cfg.properties[backref_name].back_populates = relationship_name
    elif create_backref:
        referred_cfg.properties[backref_name] = \
            generate_relationship(automap_base,
                interfaces.MANYTOMANY,
                relationship,
                backref_name,
                referred_cls, local_cls,
                secondary=table,
                primaryjoin=and_(fk.column == fk.parent for fk in m2m_const[1].elements),
                secondaryjoin=and_(fk.column == fk.parent for fk in m2m_const[0].elements),
                back_populates=relationship_name,
                collection_class=collection_class)
        map_config.properties[relationship_name].back_populates = backref_name
