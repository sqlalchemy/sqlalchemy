.. _associationproxy_toplevel:

Association Proxy
=================

.. module:: sqlalchemy.ext.associationproxy

``associationproxy`` is used to create a read/write view of a
target attribute across a relationship.  It essentially conceals
the usage of a "middle" attribute between two endpoints, and
can be used to cherry-pick fields from both a collection of
related objects or scalar relationship. or to reduce the verbosity
of using the association object pattern.
Applied creatively, the association proxy allows
the construction of sophisticated collections and dictionary
views of virtually any geometry, persisted to the database using
standard, transparently configured relational patterns.

.. _associationproxy_scalar_collections:

Simplifying Scalar Collections
------------------------------

Consider a many-to-many mapping between two classes, ``User`` and ``Keyword``.
Each ``User`` can have any number of ``Keyword`` objects, and vice-versa
(the many-to-many pattern is described at :ref:`relationships_many_to_many`).
The example below illustrates this pattern in the same way, with the
exception of an extra attribute added to the ``User`` class called
``User.keywords``::

    from __future__ import annotations

    from typing import Final
    from typing import List

    from sqlalchemy import Column
    from sqlalchemy import ForeignKey
    from sqlalchemy import Integer
    from sqlalchemy import String
    from sqlalchemy import Table
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import relationship
    from sqlalchemy.ext.associationproxy import association_proxy
    from sqlalchemy.ext.associationproxy import AssociationProxy


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __tablename__ = "user"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(64))
        kw: Mapped[List[Keyword]] = relationship(secondary=lambda: user_keyword_table)

        def __init__(self, name: str):
            self.name = name

        # proxy the 'keyword' attribute from the 'kw' relationship
        keywords: AssociationProxy[List[str]] = association_proxy("kw", "keyword")


    class Keyword(Base):
        __tablename__ = "keyword"
        id: Mapped[int] = mapped_column(primary_key=True)
        keyword: Mapped[str] = mapped_column(String(64))

        def __init__(self, keyword: str):
            self.keyword = keyword


    user_keyword_table: Final[Table] = Table(
        "user_keyword",
        Base.metadata,
        Column("user_id", Integer, ForeignKey("user.id"), primary_key=True),
        Column("keyword_id", Integer, ForeignKey("keyword.id"), primary_key=True),
    )

In the above example, :func:`.association_proxy` is applied to the ``User``
class to produce a "view" of the ``kw`` relationship, which exposes the string
value of ``.keyword`` associated with each ``Keyword`` object.  It also
creates new ``Keyword`` objects transparently when strings are added to the
collection::

    >>> user = User("jek")
    >>> user.keywords.append("cheese-inspector")
    >>> user.keywords.append("snack-ninja")
    >>> print(user.keywords)
    ['cheese-inspector', 'snack-ninja']

To understand the mechanics of this, first review the behavior of
``User`` and ``Keyword`` without using the ``.keywords`` association proxy.
Normally, reading and manipulating the collection of "keyword" strings associated
with ``User`` requires traversal from each collection element to the ``.keyword``
attribute, which can be awkward.  The example below illustrates the identical
series of operations applied without using the association proxy::

    >>> # identical operations without using the association proxy
    >>> user = User("jek")
    >>> user.kw.append(Keyword("cheese-inspector"))
    >>> user.kw.append(Keyword("snack-ninja"))
    >>> print([keyword.keyword for keyword in user.kw])
    ['cheese-inspector', 'snack-ninja']

The :class:`.AssociationProxy` object produced by the :func:`.association_proxy` function
is an instance of a `Python descriptor <https://docs.python.org/howto/descriptor.html>`_,
and is not considered to be "mapped" by the :class:`.Mapper` in any way.  Therefore,
it's always indicated inline within the class definition of the mapped class,
regardless of whether Declarative or Imperative mappings are used.

The proxy functions by operating upon the underlying mapped attribute
or collection in response to operations, and changes made via the proxy are immediately
apparent in the mapped attribute, as well as vice versa.   The underlying
attribute remains fully accessible.

When first accessed, the association proxy performs introspection
operations on the target collection so that its behavior corresponds correctly.
Details such as if the locally proxied attribute is a collection (as is typical)
or a scalar reference, as well as if the collection acts like a set, list,
or dictionary is taken into account, so that the proxy should act just like
the underlying collection or attribute does.

.. _associationproxy_creator:

Creation of New Values
^^^^^^^^^^^^^^^^^^^^^^

When a list ``append()`` event (or set ``add()``, dictionary ``__setitem__()``,
or scalar assignment event) is intercepted by the association proxy, it
instantiates a new instance of the "intermediary" object using its constructor,
passing as a single argument the given value. In our example above, an
operation like::

    user.keywords.append("cheese-inspector")

Is translated by the association proxy into the operation::

    user.kw.append(Keyword("cheese-inspector"))

The example works here because we have designed the constructor for ``Keyword``
to accept a single positional argument, ``keyword``. For those cases where a
single-argument constructor isn't feasible, the association proxy's creational
behavior can be customized using the :paramref:`.association_proxy.creator`
argument, which references a callable (i.e. Python function) that will produce
a new object instance given the singular argument. Below we illustrate this
using a lambda as is typical::

    class User(Base):
        ...

        # use Keyword(keyword=kw) on append() events
        keywords: AssociationProxy[List[str]] = association_proxy(
            "kw", "keyword", creator=lambda kw: Keyword(keyword=kw)
        )

The ``creator`` function accepts a single argument in the case of a list-
or set- based collection, or a scalar attribute.  In the case of a dictionary-based
collection, it accepts two arguments, "key" and "value".   An example
of this is below in :ref:`proxying_dictionaries`.

Simplifying Association Objects
-------------------------------

The "association object" pattern is an extended form of a many-to-many
relationship, and is described at :ref:`association_pattern`. Association
proxies are useful for keeping "association objects" out of the way during
regular use.

Suppose our ``user_keyword`` table above had additional columns
which we'd like to map explicitly, but in most cases we don't
require direct access to these attributes.  Below, we illustrate
a new mapping which introduces the ``UserKeywordAssociation`` class, which
is mapped to the ``user_keyword`` table illustrated earlier.
This class adds an additional column ``special_key``, a value which
we occasionally want to access, but not in the usual case.   We
create an association proxy on the ``User`` class called
``keywords``, which will bridge the gap from the ``user_keyword_associations``
collection of ``User`` to the ``.keyword`` attribute present on each
``UserKeywordAssociation``::

    from __future__ import annotations

    from typing import List
    from typing import Optional

    from sqlalchemy import ForeignKey
    from sqlalchemy import String
    from sqlalchemy.ext.associationproxy import association_proxy
    from sqlalchemy.ext.associationproxy import AssociationProxy
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __tablename__ = "user"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(64))

        user_keyword_associations: Mapped[List[UserKeywordAssociation]] = relationship(
            back_populates="user",
            cascade="all, delete-orphan",
        )

        # association proxy of "user_keyword_associations" collection
        # to "keyword" attribute
        keywords: AssociationProxy[List[Keyword]] = association_proxy(
            "user_keyword_associations",
            "keyword",
            creator=lambda keyword_obj: UserKeywordAssociation(keyword=keyword_obj),
        )

        def __init__(self, name: str):
            self.name = name


    class UserKeywordAssociation(Base):
        __tablename__ = "user_keyword"
        user_id: Mapped[int] = mapped_column(ForeignKey("user.id"), primary_key=True)
        keyword_id: Mapped[int] = mapped_column(ForeignKey("keyword.id"), primary_key=True)
        special_key: Mapped[Optional[str]] = mapped_column(String(50))

        user: Mapped[User] = relationship(back_populates="user_keyword_associations")

        keyword: Mapped[Keyword] = relationship()


    class Keyword(Base):
        __tablename__ = "keyword"
        id: Mapped[int] = mapped_column(primary_key=True)
        keyword: Mapped[str] = mapped_column("keyword", String(64))

        def __init__(self, keyword: str):
            self.keyword = keyword

        def __repr__(self) -> str:
            return f"Keyword({self.keyword!r})"

With the above configuration, we can operate upon the ``.keywords`` collection
of each ``User`` object, each of which exposes a collection of ``Keyword``
objects that are obtained from the underlying ``UserKeywordAssociation`` elements::

    >>> user = User("log")
    >>> for kw in (Keyword("new_from_blammo"), Keyword("its_big")):
    ...     user.keywords.append(kw)
    >>> print(user.keywords)
    [Keyword('new_from_blammo'), Keyword('its_big')]

This example is in contrast to the example illustrated previously at
:ref:`associationproxy_scalar_collections`, where the association proxy exposed
a collection of strings, rather than a collection of composed objects.
In this case, each ``.keywords.append()`` operation is equivalent to::

    >>> user.user_keyword_associations.append(
    ...     UserKeywordAssociation(keyword=Keyword("its_heavy"))
    ... )

The ``UserKeywordAssociation`` object has two attributes that are both
populated within the scope of the ``append()`` operation of the association
proxy; ``.keyword``, which refers to the
``Keyword`` object, and ``.user``, which refers to the ``User`` object.
The ``.keyword`` attribute is populated first, as the association proxy
generates a new ``UserKeywordAssociation`` object in response to the ``.append()``
operation, assigning the given ``Keyword`` instance to the ``.keyword``
attribute. Then, as the ``UserKeywordAssociation`` object is appended to the
``User.user_keyword_associations`` collection, the ``UserKeywordAssociation.user`` attribute,
configured as ``back_populates`` for ``User.user_keyword_associations``, is initialized
upon the given ``UserKeywordAssociation`` instance to refer to the parent ``User``
receiving the append operation.  The ``special_key``
argument above is left at its default value of ``None``.

For those cases where we do want ``special_key`` to have a value, we
create the ``UserKeywordAssociation`` object explicitly.  Below we assign all
three attributes, wherein the assignment of ``.user`` during
construction, has the effect of appending the new ``UserKeywordAssociation`` to
the ``User.user_keyword_associations`` collection (via the relationship)::

    >>> UserKeywordAssociation(
    ...     keyword=Keyword("its_wood"), user=user, special_key="my special key"
    ... )

The association proxy returns to us a collection of ``Keyword`` objects represented
by all these operations::

    >>> print(user.keywords)
    [Keyword('new_from_blammo'), Keyword('its_big'), Keyword('its_heavy'), Keyword('its_wood')]

.. _proxying_dictionaries:

Proxying to Dictionary Based Collections
----------------------------------------

The association proxy can proxy to dictionary based collections as well.   SQLAlchemy
mappings usually use the :func:`.attribute_keyed_dict` collection type to
create dictionary collections, as well as the extended techniques described in
:ref:`dictionary_collections`.

The association proxy adjusts its behavior when it detects the usage of a
dictionary-based collection. When new values are added to the dictionary, the
association proxy instantiates the intermediary object by passing two
arguments to the creation function instead of one, the key and the value. As
always, this creation function defaults to the constructor of the intermediary
class, and can be customized using the ``creator`` argument.

Below, we modify our ``UserKeywordAssociation`` example such that the ``User.user_keyword_associations``
collection will now be mapped using a dictionary, where the ``UserKeywordAssociation.special_key``
argument will be used as the key for the dictionary.   We also apply a ``creator``
argument to the ``User.keywords`` proxy so that these values are assigned appropriately
when new elements are added to the dictionary::

    from __future__ import annotations
    from typing import Dict

    from sqlalchemy import ForeignKey
    from sqlalchemy import String
    from sqlalchemy.ext.associationproxy import association_proxy
    from sqlalchemy.ext.associationproxy import AssociationProxy
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import relationship
    from sqlalchemy.orm.collections import attribute_keyed_dict


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __tablename__ = "user"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(64))

        # user/user_keyword_associations relationship, mapping
        # user_keyword_associations with a dictionary against "special_key" as key.
        user_keyword_associations: Mapped[Dict[str, UserKeywordAssociation]] = relationship(
            back_populates="user",
            collection_class=attribute_keyed_dict("special_key"),
            cascade="all, delete-orphan",
        )
        # proxy to 'user_keyword_associations', instantiating
        # UserKeywordAssociation assigning the new key to 'special_key',
        # values to 'keyword'.
        keywords: AssociationProxy[Dict[str, Keyword]] = association_proxy(
            "user_keyword_associations",
            "keyword",
            creator=lambda k, v: UserKeywordAssociation(special_key=k, keyword=v),
        )

        def __init__(self, name: str):
            self.name = name


    class UserKeywordAssociation(Base):
        __tablename__ = "user_keyword"
        user_id: Mapped[int] = mapped_column(ForeignKey("user.id"), primary_key=True)
        keyword_id: Mapped[int] = mapped_column(ForeignKey("keyword.id"), primary_key=True)
        special_key: Mapped[str]

        user: Mapped[User] = relationship(
            back_populates="user_keyword_associations",
        )
        keyword: Mapped[Keyword] = relationship()


    class Keyword(Base):
        __tablename__ = "keyword"
        id: Mapped[int] = mapped_column(primary_key=True)
        keyword: Mapped[str] = mapped_column(String(64))

        def __init__(self, keyword: str):
            self.keyword = keyword

        def __repr__(self) -> str:
            return f"Keyword({self.keyword!r})"

We illustrate the ``.keywords`` collection as a dictionary, mapping the
``UserKeywordAssociation.special_key`` value to ``Keyword`` objects::

    >>> user = User("log")

    >>> user.keywords["sk1"] = Keyword("kw1")
    >>> user.keywords["sk2"] = Keyword("kw2")

    >>> print(user.keywords)
    {'sk1': Keyword('kw1'), 'sk2': Keyword('kw2')}

.. _composite_association_proxy:

Composite Association Proxies
-----------------------------

Given our previous examples of proxying from relationship to scalar
attribute, proxying across an association object, and proxying dictionaries,
we can combine all three techniques together to give ``User``
a ``keywords`` dictionary that deals strictly with the string value
of ``special_key`` mapped to the string ``keyword``.  Both the ``UserKeywordAssociation``
and ``Keyword`` classes are entirely concealed.  This is achieved by building
an association proxy on ``User`` that refers to an association proxy
present on ``UserKeywordAssociation``::

    from __future__ import annotations

    from sqlalchemy import ForeignKey
    from sqlalchemy import String
    from sqlalchemy.ext.associationproxy import association_proxy
    from sqlalchemy.ext.associationproxy import AssociationProxy
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import relationship
    from sqlalchemy.orm.collections import attribute_keyed_dict


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __tablename__ = "user"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(64))

        user_keyword_associations: Mapped[Dict[str, UserKeywordAssociation]] = relationship(
            back_populates="user",
            collection_class=attribute_keyed_dict("special_key"),
            cascade="all, delete-orphan",
        )
        # the same 'user_keyword_associations'->'keyword' proxy as in
        # the basic dictionary example.
        keywords: AssociationProxy[Dict[str, str]] = association_proxy(
            "user_keyword_associations",
            "keyword",
            creator=lambda k, v: UserKeywordAssociation(special_key=k, keyword=v),
        )

        def __init__(self, name: str):
            self.name = name


    class UserKeywordAssociation(Base):
        __tablename__ = "user_keyword"
        user_id: Mapped[int] = mapped_column(ForeignKey("user.id"), primary_key=True)
        keyword_id: Mapped[int] = mapped_column(ForeignKey("keyword.id"), primary_key=True)
        special_key: Mapped[str] = mapped_column(String(64))
        user: Mapped[User] = relationship(
            back_populates="user_keyword_associations",
        )

        # the relationship to Keyword is now called
        # 'kw'
        kw: Mapped[Keyword] = relationship()

        # 'keyword' is changed to be a proxy to the
        # 'keyword' attribute of 'Keyword'
        keyword: AssociationProxy[Dict[str, str]] = association_proxy("kw", "keyword")


    class Keyword(Base):
        __tablename__ = "keyword"
        id: Mapped[int] = mapped_column(primary_key=True)
        keyword: Mapped[str] = mapped_column(String(64))

        def __init__(self, keyword: str):
            self.keyword = keyword

``User.keywords`` is now a dictionary of string to string, where
``UserKeywordAssociation`` and ``Keyword`` objects are created and removed for us
transparently using the association proxy. In the example below, we illustrate
usage of the assignment operator, also appropriately handled by the
association proxy, to apply a dictionary value to the collection at once::

    >>> user = User("log")
    >>> user.keywords = {"sk1": "kw1", "sk2": "kw2"}
    >>> print(user.keywords)
    {'sk1': 'kw1', 'sk2': 'kw2'}

    >>> user.keywords["sk3"] = "kw3"
    >>> del user.keywords["sk2"]
    >>> print(user.keywords)
    {'sk1': 'kw1', 'sk3': 'kw3'}

    >>> # illustrate un-proxied usage
    ... print(user.user_keyword_associations["sk3"].kw)
    <__main__.Keyword object at 0x12ceb90>

One caveat with our example above is that because ``Keyword`` objects are created
for each dictionary set operation, the example fails to maintain uniqueness for
the ``Keyword`` objects on their string name, which is a typical requirement for
a tagging scenario such as this one.  For this use case the recipe
`UniqueObject <https://www.sqlalchemy.org/trac/wiki/UsageRecipes/UniqueObject>`_, or
a comparable creational strategy, is
recommended, which will apply a "lookup first, then create" strategy to the constructor
of the ``Keyword`` class, so that an already existing ``Keyword`` is returned if the
given name is already present.

Querying with Association Proxies
---------------------------------

The :class:`.AssociationProxy` features simple SQL construction capabilities
which work at the class level in a similar way as other ORM-mapped attributes,
and provide rudimentary filtering support primarily based on the
SQL ``EXISTS`` keyword.


.. note:: The primary purpose of the association proxy extension is to allow
   for improved persistence and object-access patterns with mapped object
   instances that are already loaded.  The class-bound querying feature
   is of limited use and will not replace the need to refer to the underlying
   attributes when constructing SQL queries with JOINs, eager loading
   options, etc.

For this section, assume a class with both an association proxy
that refers to a column, as well as an association proxy that refers
to a related object, as in the example mapping below::

    from __future__ import annotations
    from sqlalchemy import Column, ForeignKey, Integer, String
    from sqlalchemy.ext.associationproxy import association_proxy, AssociationProxy
    from sqlalchemy.orm import DeclarativeBase, relationship
    from sqlalchemy.orm.collections import attribute_keyed_dict
    from sqlalchemy.orm.collections import Mapped


    class Base(DeclarativeBase):
        pass


    class User(Base):
        __tablename__ = "user"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(64))

        user_keyword_associations: Mapped[UserKeywordAssociation] = relationship(
            cascade="all, delete-orphan",
        )

        # object-targeted association proxy
        keywords: AssociationProxy[List[Keyword]] = association_proxy(
            "user_keyword_associations",
            "keyword",
        )

        # column-targeted association proxy
        special_keys: AssociationProxy[List[str]] = association_proxy(
            "user_keyword_associations", "special_key"
        )


    class UserKeywordAssociation(Base):
        __tablename__ = "user_keyword"
        user_id: Mapped[int] = mapped_column(ForeignKey("user.id"), primary_key=True)
        keyword_id: Mapped[int] = mapped_column(ForeignKey("keyword.id"), primary_key=True)
        special_key: Mapped[str] = mapped_column(String(64))
        keyword: Mapped[Keyword] = relationship()


    class Keyword(Base):
        __tablename__ = "keyword"
        id: Mapped[int] = mapped_column(primary_key=True)
        keyword: Mapped[str] = mapped_column(String(64))

The SQL generated takes the form of a correlated subquery against
the EXISTS SQL operator so that it can be used in a WHERE clause without
the need for additional modifications to the enclosing query.  If the
immediate target of an association proxy is a **mapped column expression**,
standard column operators can be used which will be embedded in the subquery.
For example a straight equality operator:

.. sourcecode:: pycon+sql

    >>> print(session.scalars(select(User).where(User.special_keys == "jek")))
    {printsql}SELECT "user".id AS user_id, "user".name AS user_name
    FROM "user"
    WHERE EXISTS (SELECT 1
    FROM user_keyword
    WHERE "user".id = user_keyword.user_id AND user_keyword.special_key = :special_key_1)

a LIKE operator:

.. sourcecode:: pycon+sql

    >>> print(session.scalars(select(User).where(User.special_keys.like("%jek"))))
    {printsql}SELECT "user".id AS user_id, "user".name AS user_name
    FROM "user"
    WHERE EXISTS (SELECT 1
    FROM user_keyword
    WHERE "user".id = user_keyword.user_id AND user_keyword.special_key LIKE :special_key_1)

For association proxies where the immediate target is a **related object or collection,
or another association proxy or attribute on the related object**, relationship-oriented
operators can be used instead, such as :meth:`_orm.PropComparator.has` and
:meth:`_orm.PropComparator.any`.   The ``User.keywords`` attribute is in fact
two association proxies linked together, so when using this proxy for generating
SQL phrases, we get two levels of EXISTS subqueries:

.. sourcecode:: pycon+sql

    >>> print(session.scalars(select(User).where(User.keywords.any(Keyword.keyword == "jek"))))
    {printsql}SELECT "user".id AS user_id, "user".name AS user_name
    FROM "user"
    WHERE EXISTS (SELECT 1
    FROM user_keyword
    WHERE "user".id = user_keyword.user_id AND (EXISTS (SELECT 1
    FROM keyword
    WHERE keyword.id = user_keyword.keyword_id AND keyword.keyword = :keyword_1)))

This is not the most efficient form of SQL, so while association proxies can be
convenient for generating WHERE criteria quickly, SQL results should be
inspected and "unrolled" into explicit JOIN criteria for best use, especially
when chaining association proxies together.

.. _cascade_scalar_deletes:

Cascading Scalar Deletes
------------------------

Given a mapping as::

    from __future__ import annotations
    from sqlalchemy import Column, ForeignKey, Integer, String
    from sqlalchemy.ext.associationproxy import association_proxy, AssociationProxy
    from sqlalchemy.orm import DeclarativeBase, relationship
    from sqlalchemy.orm.collections import attribute_keyed_dict
    from sqlalchemy.orm.collections import Mapped


    class Base(DeclarativeBase):
        pass


    class A(Base):
        __tablename__ = "test_a"
        id: Mapped[int] = mapped_column(primary_key=True)
        ab: Mapped[AB] = relationship(uselist=False)
        b: AssociationProxy[B] = association_proxy(
            "ab", "b", creator=lambda b: AB(b=b), cascade_scalar_deletes=True
        )


    class B(Base):
        __tablename__ = "test_b"
        id: Mapped[int] = mapped_column(primary_key=True)


    class AB(Base):
        __tablename__ = "test_ab"
        a_id: Mapped[int] = mapped_column(ForeignKey(A.id), primary_key=True)
        b_id: Mapped[int] = mapped_column(ForeignKey(B.id), primary_key=True)

        b: Mapped[B] = relationship()

An assignment to ``A.b`` will generate an ``AB`` object::

    a.b = B()

The ``A.b`` association is scalar, and includes use of the parameter
:paramref:`.AssociationProxy.cascade_scalar_deletes`.  When this parameter
is enabled, setting ``A.b``
to ``None`` will remove ``A.ab`` as well::

    a.b = None
    assert a.ab is None

When :paramref:`.AssociationProxy.cascade_scalar_deletes` is not set,
the association object ``a.ab`` above would remain in place.

Note that this is not the behavior for collection-based association proxies;
in that case, the intermediary association object is always removed when
members of the proxied collection are removed.  Whether or not the row is
deleted depends on the relationship cascade setting.

.. seealso::

    :ref:`unitofwork_cascades`

Scalar Relationships
--------------------

The example below illustrates the use of the association proxy on the many
side of of a one-to-many relationship, accessing attributes of a scalar
object::

    from __future__ import annotations

    from typing import List

    from sqlalchemy import ForeignKey
    from sqlalchemy import String
    from sqlalchemy.ext.associationproxy import association_proxy
    from sqlalchemy.ext.associationproxy import AssociationProxy
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
    from sqlalchemy.orm import relationship


    class Base(DeclarativeBase):
        pass


    class Recipe(Base):
        __tablename__ = "recipe"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(64))

        steps: Mapped[List[Step]] = relationship(back_populates="recipe")
        step_descriptions: AssociationProxy[List[str]] = association_proxy(
            "steps", "description"
        )


    class Step(Base):
        __tablename__ = "step"
        id: Mapped[int] = mapped_column(primary_key=True)
        description: Mapped[str]
        recipe_id: Mapped[int] = mapped_column(ForeignKey("recipe.id"))
        recipe: Mapped[Recipe] = relationship(back_populates="steps")

        recipe_name: AssociationProxy[str] = association_proxy("recipe", "name")

        def __init__(self, description: str) -> None:
            self.description = description


    my_snack = Recipe(
        name="afternoon snack",
        step_descriptions=[
            "slice bread",
            "spread peanut butted",
            "eat sandwich",
        ],
    )

A summary of the steps of ``my_snack`` can be printed using::

    >>> for i, step in enumerate(my_snack.steps, 1):
    ...     print(f"Step {i} of {step.recipe_name!r}: {step.description}")
    Step 1 of 'afternoon snack': slice bread
    Step 2 of 'afternoon snack': spread peanut butted
    Step 3 of 'afternoon snack': eat sandwich

API Documentation
-----------------

.. autofunction:: association_proxy

.. autoclass:: AssociationProxy
   :members:
   :undoc-members:
   :inherited-members:

.. autoclass:: AssociationProxyInstance
   :members:
   :undoc-members:
   :inherited-members:

.. autoclass:: ObjectAssociationProxyInstance
   :members:
   :inherited-members:

.. autoclass:: ColumnAssociationProxyInstance
   :members:
   :inherited-members:

.. autoclass:: AssociationProxyExtensionType
   :members:
