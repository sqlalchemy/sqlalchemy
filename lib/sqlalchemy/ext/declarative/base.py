# ext/declarative/base.py
# Copyright (C) 2005-2017 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Internal implementation for declarative."""

from ...schema import Table, Column
from ...orm import mapper, class_mapper, synonym
from ...orm.interfaces import MapperProperty
from ...orm.properties import ColumnProperty, CompositeProperty
from ...orm.attributes import QueryableAttribute
from ...orm.base import _is_mapped_class
from ... import util, exc
from ...util import topological
from ...sql import expression
from ... import event
from . import clsregistry
import collections
import weakref
from sqlalchemy.orm import instrumentation

declared_attr = declarative_props = None


def _declared_mapping_info(cls):
    # deferred mapping
    if _DeferredMapperConfig.has_cls(cls):
        return _DeferredMapperConfig.config_for_cls(cls)
    # regular mapping
    elif _is_mapped_class(cls):
        return class_mapper(cls, configure=False)
    else:
        return None


def _resolve_for_abstract(cls):
    if cls is object:
        return None

    if _get_immediate_cls_attr(cls, '__abstract__', strict=True):
        for sup in cls.__bases__:
            sup = _resolve_for_abstract(sup)
            if sup is not None:
                return sup
        else:
            return None
    else:
        return cls


def _get_immediate_cls_attr(cls, attrname, strict=False):
    """return an attribute of the class that is either present directly
    on the class, e.g. not on a superclass, or is from a superclass but
    this superclass is a mixin, that is, not a descendant of
    the declarative base.

    This is used to detect attributes that indicate something about
    a mapped class independently from any mapped classes that it may
    inherit from.

    """
    if not issubclass(cls, object):
        return None

    for base in cls.__mro__:
        _is_declarative_inherits = hasattr(base, '_decl_class_registry')
        if attrname in base.__dict__ and (
            base is cls or
            ((base in cls.__bases__ if strict else True)
                and not _is_declarative_inherits)
        ):
            return getattr(base, attrname)
    else:
        return None


def _as_declarative(cls, classname, dict_):
    global declared_attr, declarative_props
    if declared_attr is None:
        from .api import declared_attr
        declarative_props = (declared_attr, util.classproperty)

    if _get_immediate_cls_attr(cls, '__abstract__', strict=True):
        return

    _MapperConfig.setup_mapping(cls, classname, dict_)


def _check_declared_props_nocascade(obj, name, cls):

    if isinstance(obj, declarative_props):
        if getattr(obj, '_cascading', False):
            util.warn(
                "@declared_attr.cascading is not supported on the %s "
                "attribute on class %s.  This attribute invokes for "
                "subclasses in any case." % (name, cls))
        return True
    else:
        return False


class _MapperConfig(object):

    @classmethod
    def setup_mapping(cls, cls_, classname, dict_):
        defer_map = _get_immediate_cls_attr(
            cls_, '_sa_decl_prepare_nocascade', strict=True) or \
            hasattr(cls_, '_sa_decl_prepare')

        if defer_map:
            cfg_cls = _DeferredMapperConfig
        else:
            cfg_cls = _MapperConfig
        cfg_cls(cls_, classname, dict_)

    def __init__(self, cls_, classname, dict_):

        self.cls = cls_

        # dict_ will be a dictproxy, which we can't write to, and we need to!
        self.dict_ = dict(dict_)
        self.classname = classname
        self.mapped_table = None
        self.properties = util.OrderedDict()
        self.declared_columns = set()
        self.column_copies = {}
        self._setup_declared_events()

        # temporary registry.  While early 1.0 versions
        # set up the ClassManager here, by API contract
        # we can't do that until there's a mapper.
        self.cls._sa_declared_attr_reg = {}

        self._scan_attributes()

        clsregistry.add_class(self.classname, self.cls)

        self._extract_mappable_attributes()

        self._extract_declared_columns()

        self._setup_table()

        self._setup_inheritance()

        self._early_mapping()

    def _early_mapping(self):
        self.map()

    def _setup_declared_events(self):
        if _get_immediate_cls_attr(self.cls, '__declare_last__'):
            @event.listens_for(mapper, "after_configured")
            def after_configured():
                self.cls.__declare_last__()

        if _get_immediate_cls_attr(self.cls, '__declare_first__'):
            @event.listens_for(mapper, "before_configured")
            def before_configured():
                self.cls.__declare_first__()

    def _scan_attributes(self):
        cls = self.cls
        dict_ = self.dict_
        column_copies = self.column_copies
        mapper_args_fn = None
        table_args = inherited_table_args = None
        tablename = None

        for base in cls.__mro__:
            class_mapped = base is not cls and \
                _declared_mapping_info(base) is not None and \
                not _get_immediate_cls_attr(
                    base, '_sa_decl_prepare_nocascade', strict=True)

            if not class_mapped and base is not cls:
                self._produce_column_copies(base)

            for name, obj in vars(base).items():
                if name == '__mapper_args__':
                    check_decl = \
                        _check_declared_props_nocascade(obj, name, cls)
                    if not mapper_args_fn and (
                        not class_mapped or
                        check_decl
                    ):
                        # don't even invoke __mapper_args__ until
                        # after we've determined everything about the
                        # mapped table.
                        # make a copy of it so a class-level dictionary
                        # is not overwritten when we update column-based
                        # arguments.
                        mapper_args_fn = lambda: dict(cls.__mapper_args__)  # noqa
                elif name == '__tablename__':
                    check_decl = \
                        _check_declared_props_nocascade(obj, name, cls)
                    if not tablename and (
                        not class_mapped or
                        check_decl
                    ):
                        tablename = cls.__tablename__
                elif name == '__table_args__':
                    check_decl = \
                        _check_declared_props_nocascade(obj, name, cls)
                    if not table_args and (
                        not class_mapped or
                        check_decl
                    ):
                        table_args = cls.__table_args__
                        if not isinstance(
                                table_args, (tuple, dict, type(None))):
                            raise exc.ArgumentError(
                                "__table_args__ value must be a tuple, "
                                "dict, or None")
                        if base is not cls:
                            inherited_table_args = True
                elif class_mapped:
                    if isinstance(obj, declarative_props):
                        util.warn("Regular (i.e. not __special__) "
                                  "attribute '%s.%s' uses @declared_attr, "
                                  "but owning class %s is mapped - "
                                  "not applying to subclass %s."
                                  % (base.__name__, name, base, cls))
                    continue
                elif base is not cls:
                    # we're a mixin, abstract base, or something that is
                    # acting like that for now.
                    if isinstance(obj, Column):
                        # already copied columns to the mapped class.
                        continue
                    elif isinstance(obj, MapperProperty):
                        raise exc.InvalidRequestError(
                            "Mapper properties (i.e. deferred,"
                            "column_property(), relationship(), etc.) must "
                            "be declared as @declared_attr callables "
                            "on declarative mixin classes.")
                    elif isinstance(obj, declarative_props):
                        oldclassprop = isinstance(obj, util.classproperty)
                        if not oldclassprop and obj._cascading:
                            if name in dict_:
                                # unfortunately, while we can use the user-
                                # defined attribute here to allow a clean
                                # override, if there's another
                                # subclass below then it still tries to use
                                # this.  not sure if there is enough information
                                # here to add this as a feature later on.
                                util.warn(
                                    "Attribute '%s' on class %s cannot be "
                                    "processed due to "
                                    "@declared_attr.cascading; "
                                    "skipping" % (name, cls))
                            dict_[name] = column_copies[obj] = \
                                ret = obj.__get__(obj, cls)
                            setattr(cls, name, ret)
                        else:
                            if oldclassprop:
                                util.warn_deprecated(
                                    "Use of sqlalchemy.util.classproperty on "
                                    "declarative classes is deprecated.")
                            dict_[name] = column_copies[obj] = \
                                ret = getattr(cls, name)
                        if isinstance(ret, (Column, MapperProperty)) and \
                                ret.doc is None:
                            ret.doc = obj.__doc__

        if inherited_table_args and not tablename:
            table_args = None

        self.table_args = table_args
        self.tablename = tablename
        self.mapper_args_fn = mapper_args_fn

    def _produce_column_copies(self, base):
        cls = self.cls
        dict_ = self.dict_
        column_copies = self.column_copies
        # copy mixin columns to the mapped class
        for name, obj in vars(base).items():
            if isinstance(obj, Column):
                if getattr(cls, name) is not obj:
                    # if column has been overridden
                    # (like by the InstrumentedAttribute of the
                    # superclass), skip
                    continue
                elif obj.foreign_keys:
                    raise exc.InvalidRequestError(
                        "Columns with foreign keys to other columns "
                        "must be declared as @declared_attr callables "
                        "on declarative mixin classes. ")
                elif name not in dict_ and not (
                        '__table__' in dict_ and
                        (obj.name or name) in dict_['__table__'].c
                ):
                    column_copies[obj] = copy_ = obj.copy()
                    copy_._creation_order = obj._creation_order
                    setattr(cls, name, copy_)
                    dict_[name] = copy_

    def _extract_mappable_attributes(self):
        cls = self.cls
        dict_ = self.dict_

        our_stuff = self.properties

        late_mapped = _get_immediate_cls_attr(
                    cls, '_sa_decl_prepare_nocascade', strict=True)

        for k in list(dict_):

            if k in ('__table__', '__tablename__', '__mapper_args__'):
                continue

            value = dict_[k]
            if isinstance(value, declarative_props):
                if isinstance(value, declared_attr) and value._cascading:
                    util.warn(
                        "Use of @declared_attr.cascading only applies to "
                        "Declarative 'mixin' and 'abstract' classes.  "
                        "Currently, this flag is ignored on mapped class "
                        "%s" % self.cls)

                value = getattr(cls, k)

            elif isinstance(value, QueryableAttribute) and \
                    value.class_ is not cls and \
                    value.key != k:
                # detect a QueryableAttribute that's already mapped being
                # assigned elsewhere in userland, turn into a synonym()
                value = synonym(value.key)
                setattr(cls, k, value)

            if (isinstance(value, tuple) and len(value) == 1 and
                    isinstance(value[0], (Column, MapperProperty))):
                util.warn("Ignoring declarative-like tuple value of attribute "
                          "%s: possibly a copy-and-paste error with a comma "
                          "left at the end of the line?" % k)
                continue
            elif not isinstance(value, (Column, MapperProperty)):
                # using @declared_attr for some object that
                # isn't Column/MapperProperty; remove from the dict_
                # and place the evaluated value onto the class.
                if not k.startswith('__'):
                    dict_.pop(k)
                    if not late_mapped:
                        setattr(cls, k, value)
                continue
            # we expect to see the name 'metadata' in some valid cases;
            # however at this point we see it's assigned to something trying
            # to be mapped, so raise for that.
            elif k == 'metadata':
                raise exc.InvalidRequestError(
                    "Attribute name 'metadata' is reserved "
                    "for the MetaData instance when using a "
                    "declarative base class."
                )
            prop = clsregistry._deferred_relationship(cls, value)
            our_stuff[k] = prop

    def _extract_declared_columns(self):
        our_stuff = self.properties

        # set up attributes in the order they were created
        our_stuff.sort(key=lambda key: our_stuff[key]._creation_order)

        # extract columns from the class dict
        declared_columns = self.declared_columns
        name_to_prop_key = collections.defaultdict(set)
        for key, c in list(our_stuff.items()):
            if isinstance(c, (ColumnProperty, CompositeProperty)):
                for col in c.columns:
                    if isinstance(col, Column) and \
                            col.table is None:
                        _undefer_column_name(key, col)
                        if not isinstance(c, CompositeProperty):
                            name_to_prop_key[col.name].add(key)
                        declared_columns.add(col)
            elif isinstance(c, Column):
                _undefer_column_name(key, c)
                name_to_prop_key[c.name].add(key)
                declared_columns.add(c)
                # if the column is the same name as the key,
                # remove it from the explicit properties dict.
                # the normal rules for assigning column-based properties
                # will take over, including precedence of columns
                # in multi-column ColumnProperties.
                if key == c.key:
                    del our_stuff[key]

        for name, keys in name_to_prop_key.items():
            if len(keys) > 1:
                util.warn(
                    "On class %r, Column object %r named "
                    "directly multiple times, "
                    "only one will be used: %s. "
                    "Consider using orm.synonym instead" %
                    (self.classname, name, (", ".join(sorted(keys))))
                )

    def _setup_table(self):
        cls = self.cls
        tablename = self.tablename
        table_args = self.table_args
        dict_ = self.dict_
        declared_columns = self.declared_columns

        declared_columns = self.declared_columns = sorted(
            declared_columns, key=lambda c: c._creation_order)
        table = None

        if hasattr(cls, '__table_cls__'):
            table_cls = util.unbound_method_to_callable(cls.__table_cls__)
        else:
            table_cls = Table

        if '__table__' not in dict_:
            if tablename is not None:

                args, table_kw = (), {}
                if table_args:
                    if isinstance(table_args, dict):
                        table_kw = table_args
                    elif isinstance(table_args, tuple):
                        if isinstance(table_args[-1], dict):
                            args, table_kw = table_args[0:-1], table_args[-1]
                        else:
                            args = table_args

                autoload = dict_.get('__autoload__')
                if autoload:
                    table_kw['autoload'] = True

                cls.__table__ = table = table_cls(
                    tablename, cls.metadata,
                    *(tuple(declared_columns) + tuple(args)),
                    **table_kw)
        else:
            table = cls.__table__
            if declared_columns:
                for c in declared_columns:
                    if not table.c.contains_column(c):
                        raise exc.ArgumentError(
                            "Can't add additional column %r when "
                            "specifying __table__" % c.key
                        )
        self.local_table = table

    def _setup_inheritance(self):
        table = self.local_table
        cls = self.cls
        table_args = self.table_args
        declared_columns = self.declared_columns
        for c in cls.__bases__:
            c = _resolve_for_abstract(c)
            if c is None:
                continue
            if _declared_mapping_info(c) is not None and \
                    not _get_immediate_cls_attr(
                        c, '_sa_decl_prepare_nocascade', strict=True):
                self.inherits = c
                break
        else:
            self.inherits = None

        if table is None and self.inherits is None and \
                not _get_immediate_cls_attr(cls, '__no_table__'):

            raise exc.InvalidRequestError(
                "Class %r does not have a __table__ or __tablename__ "
                "specified and does not inherit from an existing "
                "table-mapped class." % cls
            )
        elif self.inherits:
            inherited_mapper = _declared_mapping_info(self.inherits)
            inherited_table = inherited_mapper.local_table
            inherited_mapped_table = inherited_mapper.mapped_table

            if table is None:
                # single table inheritance.
                # ensure no table args
                if table_args:
                    raise exc.ArgumentError(
                        "Can't place __table_args__ on an inherited class "
                        "with no table."
                    )
                # add any columns declared here to the inherited table.
                for c in declared_columns:
                    if c.primary_key:
                        raise exc.ArgumentError(
                            "Can't place primary key columns on an inherited "
                            "class with no table."
                        )
                    if c.name in inherited_table.c:
                        if inherited_table.c[c.name] is c:
                            continue
                        raise exc.ArgumentError(
                            "Column '%s' on class %s conflicts with "
                            "existing column '%s'" %
                            (c, cls, inherited_table.c[c.name])
                        )
                    inherited_table.append_column(c)
                    if inherited_mapped_table is not None and \
                            inherited_mapped_table is not inherited_table:
                        inherited_mapped_table._refresh_for_new_column(c)

    def _prepare_mapper_arguments(self):
        properties = self.properties
        if self.mapper_args_fn:
            mapper_args = self.mapper_args_fn()
        else:
            mapper_args = {}

        # make sure that column copies are used rather
        # than the original columns from any mixins
        for k in ('version_id_col', 'polymorphic_on',):
            if k in mapper_args:
                v = mapper_args[k]
                mapper_args[k] = self.column_copies.get(v, v)

        assert 'inherits' not in mapper_args, \
            "Can't specify 'inherits' explicitly with declarative mappings"

        if self.inherits:
            mapper_args['inherits'] = self.inherits

        if self.inherits and not mapper_args.get('concrete', False):
            # single or joined inheritance
            # exclude any cols on the inherited table which are
            # not mapped on the parent class, to avoid
            # mapping columns specific to sibling/nephew classes
            inherited_mapper = _declared_mapping_info(self.inherits)
            inherited_table = inherited_mapper.local_table

            if 'exclude_properties' not in mapper_args:
                mapper_args['exclude_properties'] = exclude_properties = \
                    set(
                        [c.key for c in inherited_table.c
                         if c not in inherited_mapper._columntoproperty]
                ).union(
                    inherited_mapper.exclude_properties or ()
                )
                exclude_properties.difference_update(
                    [c.key for c in self.declared_columns])

            # look through columns in the current mapper that
            # are keyed to a propname different than the colname
            # (if names were the same, we'd have popped it out above,
            # in which case the mapper makes this combination).
            # See if the superclass has a similar column property.
            # If so, join them together.
            for k, col in list(properties.items()):
                if not isinstance(col, expression.ColumnElement):
                    continue
                if k in inherited_mapper._props:
                    p = inherited_mapper._props[k]
                    if isinstance(p, ColumnProperty):
                        # note here we place the subclass column
                        # first.  See [ticket:1892] for background.
                        properties[k] = [col] + p.columns
        result_mapper_args = mapper_args.copy()
        result_mapper_args['properties'] = properties
        self.mapper_args = result_mapper_args

    def map(self):
        self._prepare_mapper_arguments()
        if hasattr(self.cls, '__mapper_cls__'):
            mapper_cls = util.unbound_method_to_callable(
                self.cls.__mapper_cls__)
        else:
            mapper_cls = mapper

        self.cls.__mapper__ = mp_ = mapper_cls(
            self.cls,
            self.local_table,
            **self.mapper_args
        )
        del self.cls._sa_declared_attr_reg
        return mp_


class _DeferredMapperConfig(_MapperConfig):
    _configs = util.OrderedDict()

    def _early_mapping(self):
        pass

    @property
    def cls(self):
        return self._cls()

    @cls.setter
    def cls(self, class_):
        self._cls = weakref.ref(class_, self._remove_config_cls)
        self._configs[self._cls] = self

    @classmethod
    def _remove_config_cls(cls, ref):
        cls._configs.pop(ref, None)

    @classmethod
    def has_cls(cls, class_):
        # 2.6 fails on weakref if class_ is an old style class
        return isinstance(class_, type) and \
            weakref.ref(class_) in cls._configs

    @classmethod
    def config_for_cls(cls, class_):
        return cls._configs[weakref.ref(class_)]

    @classmethod
    def classes_for_base(cls, base_cls, sort=True):
        classes_for_base = [
            m for m, cls_ in
            [(m, m.cls) for m in cls._configs.values()]
            if cls_ is not None and issubclass(cls_, base_cls)
        ]

        if not sort:
            return classes_for_base

        all_m_by_cls = dict(
            (m.cls, m)
            for m in classes_for_base
        )

        tuples = []
        for m_cls in all_m_by_cls:
            tuples.extend(
                (all_m_by_cls[base_cls], all_m_by_cls[m_cls])
                for base_cls in m_cls.__bases__
                if base_cls in all_m_by_cls
            )
        return list(
            topological.sort(
                tuples,
                classes_for_base
            )
        )

    def map(self):
        self._configs.pop(self._cls, None)
        return super(_DeferredMapperConfig, self).map()


def _add_attribute(cls, key, value):
    """add an attribute to an existing declarative class.

    This runs through the logic to determine MapperProperty,
    adds it to the Mapper, adds a column to the mapped Table, etc.

    """

    if '__mapper__' in cls.__dict__:
        if isinstance(value, Column):
            _undefer_column_name(key, value)
            cls.__table__.append_column(value)
            cls.__mapper__.add_property(key, value)
        elif isinstance(value, ColumnProperty):
            for col in value.columns:
                if isinstance(col, Column) and col.table is None:
                    _undefer_column_name(key, col)
                    cls.__table__.append_column(col)
            cls.__mapper__.add_property(key, value)
        elif isinstance(value, MapperProperty):
            cls.__mapper__.add_property(
                key,
                clsregistry._deferred_relationship(cls, value)
            )
        elif isinstance(value, QueryableAttribute) and value.key != key:
            # detect a QueryableAttribute that's already mapped being
            # assigned elsewhere in userland, turn into a synonym()
            value = synonym(value.key)
            cls.__mapper__.add_property(
                key,
                clsregistry._deferred_relationship(cls, value)
            )
        else:
            type.__setattr__(cls, key, value)
    else:
        type.__setattr__(cls, key, value)


def _declarative_constructor(self, **kwargs):
    """A simple constructor that allows initialization from kwargs.

    Sets attributes on the constructed instance using the names and
    values in ``kwargs``.

    Only keys that are present as
    attributes of the instance's class are allowed. These could be,
    for example, any mapped columns or relationships.
    """
    cls_ = type(self)
    for k in kwargs:
        if not hasattr(cls_, k):
            raise TypeError(
                "%r is an invalid keyword argument for %s" %
                (k, cls_.__name__))
        setattr(self, k, kwargs[k])
_declarative_constructor.__name__ = '__init__'


def _undefer_column_name(key, column):
    if column.key is None:
        column.key = key
    if column.name is None:
        column.name = key
