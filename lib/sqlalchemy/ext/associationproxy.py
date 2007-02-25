"""Contain the ``AssociationProxy`` class.

The ``AssociationProxy`` is a Python property object which provides
transparent proxied access to the endpoint of an association object.

See the example ``examples/association/proxied_association.py``.
"""

from sqlalchemy.orm import class_mapper

class AssociationProxy(object):
    """A property object that automatically sets up ``AssociationLists`` on a parent object."""

    def __init__(self, targetcollection, attr, creator=None):
        """Create a new association property.

        targetcollection
          The attribute name which stores the collection of Associations.

        attr
          Name of the attribute on the Association in which to get/set target values.

        creator
          Optional callable which is used to create a new association
          object.  This callable is given a single argument which is
          an instance of the *proxied* object.  If creator is not
          given, the association object is created using the class
          associated with the targetcollection attribute, using its
          ``__init__()`` constructor and setting the proxied
          attribute.
        """
        self.targetcollection = targetcollection
        self.attr = attr
        self.creator = creator

    def __init_deferred(self):
        prop = class_mapper(self._owner_class).props[self.targetcollection]
        self._cls = prop.mapper.class_
        self._uselist = prop.uselist

    def _get_class(self):
        try:
            return self._cls
        except AttributeError:
            self.__init_deferred()
            return self._cls

    def _get_uselist(self):
        try:
            return self._uselist
        except AttributeError:
            self.__init_deferred()
            return self._uselist

    cls = property(_get_class)
    uselist = property(_get_uselist)

    def create(self, target, **kw):
        if self.creator is not None:
            return self.creator(target, **kw)
        else:
            assoc = self.cls(**kw)
            setattr(assoc, self.attr, target)
            return assoc

    def __get__(self, obj, owner):
        self._owner_class = owner
        if obj is None:
            return self
        storage_key = '_AssociationProxy_%s' % self.targetcollection
        if self.uselist:
            try:
                return getattr(obj, storage_key)
            except AttributeError:
                a = _AssociationList(self, obj)
                setattr(obj, storage_key, a)
                return a
        else:
            return getattr(getattr(obj, self.targetcollection), self.attr)

    def __set__(self, obj, value):
        if self.uselist:
            setattr(obj, self.targetcollection, [self.create(x) for x in value])
        else:
            setattr(obj, self.targetcollection, self.create(value))

    def __del__(self, obj):
        delattr(obj, self.targetcollection)

class _AssociationList(object):
    """Generic proxying list which proxies list operations to a
    different list-holding attribute of the parent object, converting
    Association objects to and from a target attribute on each
    Association object.
    """

    def __init__(self, proxy, parent):
        """Create a new ``AssociationList``."""
        self.proxy = proxy
        self.parent = parent

    def append(self, item, **kw):
        a = self.proxy.create(item, **kw)
        getattr(self.parent, self.proxy.targetcollection).append(a)

    def __iter__(self):
        return iter([getattr(x, self.proxy.attr) for x in getattr(self.parent, self.proxy.targetcollection)])

    def __repr__(self):
        return repr([getattr(x, self.proxy.attr) for x in getattr(self.parent, self.proxy.targetcollection)])

    def __len__(self):
        return len(getattr(self.parent, self.proxy.targetcollection))

    def __getitem__(self, index):
        return getattr(getattr(self.parent, self.proxy.targetcollection)[index], self.proxy.attr)

    def __setitem__(self, index, value):
        a = self.proxy.create(item)
        getattr(self.parent, self.proxy.targetcollection)[index] = a
