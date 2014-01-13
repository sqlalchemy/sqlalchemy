# orm/properties.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""MapperProperty implementations.

This is a private module which defines the behavior of invidual ORM-
mapped attributes.

"""
from __future__ import absolute_import

from .. import util, log
from ..sql import expression
from . import attributes
from .util import _orm_full_deannotate

from .interfaces import PropComparator, StrategizedProperty

__all__ = ['ColumnProperty', 'CompositeProperty', 'SynonymProperty',
           'ComparableProperty', 'RelationshipProperty']


@log.class_logger
class ColumnProperty(StrategizedProperty):
    """Describes an object attribute that corresponds to a table column.

    Public constructor is the :func:`.orm.column_property` function.

    """

    strategy_wildcard_key = 'column'

    def __init__(self, *columns, **kwargs):
        """Provide a column-level property for use with a Mapper.

        Column-based properties can normally be applied to the mapper's
        ``properties`` dictionary using the :class:`.Column` element directly.
        Use this function when the given column is not directly present within the
        mapper's selectable; examples include SQL expressions, functions, and
        scalar SELECT queries.

        Columns that aren't present in the mapper's selectable won't be persisted
        by the mapper and are effectively "read-only" attributes.

        :param \*cols:
              list of Column objects to be mapped.

        :param active_history=False:
          When ``True``, indicates that the "previous" value for a
          scalar attribute should be loaded when replaced, if not
          already loaded. Normally, history tracking logic for
          simple non-primary-key scalar values only needs to be
          aware of the "new" value in order to perform a flush. This
          flag is available for applications that make use of
          :func:`.attributes.get_history` or :meth:`.Session.is_modified`
          which also need to know
          the "previous" value of the attribute.

          .. versionadded:: 0.6.6

        :param comparator_factory: a class which extends
           :class:`.ColumnProperty.Comparator` which provides custom SQL clause
           generation for comparison operations.

        :param group:
            a group name for this property when marked as deferred.

        :param deferred:
              when True, the column property is "deferred", meaning that
              it does not load immediately, and is instead loaded when the
              attribute is first accessed on an instance.  See also
              :func:`~sqlalchemy.orm.deferred`.

        :param doc:
              optional string that will be applied as the doc on the
              class-bound descriptor.

        :param expire_on_flush=True:
            Disable expiry on flush.   A column_property() which refers
            to a SQL expression (and not a single table-bound column)
            is considered to be a "read only" property; populating it
            has no effect on the state of data, and it can only return
            database state.   For this reason a column_property()'s value
            is expired whenever the parent object is involved in a
            flush, that is, has any kind of "dirty" state within a flush.
            Setting this parameter to ``False`` will have the effect of
            leaving any existing value present after the flush proceeds.
            Note however that the :class:`.Session` with default expiration
            settings still expires
            all attributes after a :meth:`.Session.commit` call, however.

            .. versionadded:: 0.7.3

        :param info: Optional data dictionary which will be populated into the
            :attr:`.MapperProperty.info` attribute of this object.

            .. versionadded:: 0.8

        :param extension:
            an
            :class:`.AttributeExtension`
            instance, or list of extensions, which will be prepended
            to the list of attribute listeners for the resulting
            descriptor placed on the class.
            **Deprecated.** Please see :class:`.AttributeEvents`.

        """
        self._orig_columns = [expression._labeled(c) for c in columns]
        self.columns = [expression._labeled(_orm_full_deannotate(c))
                            for c in columns]
        self.group = kwargs.pop('group', None)
        self.deferred = kwargs.pop('deferred', False)
        self.instrument = kwargs.pop('_instrument', True)
        self.comparator_factory = kwargs.pop('comparator_factory',
                                            self.__class__.Comparator)
        self.descriptor = kwargs.pop('descriptor', None)
        self.extension = kwargs.pop('extension', None)
        self.active_history = kwargs.pop('active_history', False)
        self.expire_on_flush = kwargs.pop('expire_on_flush', True)

        if 'info' in kwargs:
            self.info = kwargs.pop('info')

        if 'doc' in kwargs:
            self.doc = kwargs.pop('doc')
        else:
            for col in reversed(self.columns):
                doc = getattr(col, 'doc', None)
                if doc is not None:
                    self.doc = doc
                    break
            else:
                self.doc = None

        if kwargs:
            raise TypeError(
                "%s received unexpected keyword argument(s): %s" % (
                    self.__class__.__name__,
                    ', '.join(sorted(kwargs.keys()))))

        util.set_creation_order(self)

        self.strategy_class = self._strategy_lookup(
                                    ("deferred", self.deferred),
                                    ("instrument", self.instrument)
                                    )

    @property
    def expression(self):
        """Return the primary column or expression for this ColumnProperty.

        """
        return self.columns[0]

    def instrument_class(self, mapper):
        if not self.instrument:
            return

        attributes.register_descriptor(
            mapper.class_,
            self.key,
            comparator=self.comparator_factory(self, mapper),
            parententity=mapper,
            doc=self.doc
            )

    def do_init(self):
        super(ColumnProperty, self).do_init()
        if len(self.columns) > 1 and \
                set(self.parent.primary_key).issuperset(self.columns):
            util.warn(
                ("On mapper %s, primary key column '%s' is being combined "
                 "with distinct primary key column '%s' in attribute '%s'.  "
                 "Use explicit properties to give each column its own mapped "
                 "attribute name.") % (self.parent, self.columns[1],
                                       self.columns[0], self.key))

    def copy(self):
        return ColumnProperty(
                        deferred=self.deferred,
                        group=self.group,
                        active_history=self.active_history,
                        *self.columns)

    def _getcommitted(self, state, dict_, column,
                    passive=attributes.PASSIVE_OFF):
        return state.get_impl(self.key).\
                    get_committed_value(state, dict_, passive=passive)

    def merge(self, session, source_state, source_dict, dest_state,
                                dest_dict, load, _recursive):
        if not self.instrument:
            return
        elif self.key in source_dict:
            value = source_dict[self.key]

            if not load:
                dest_dict[self.key] = value
            else:
                impl = dest_state.get_impl(self.key)
                impl.set(dest_state, dest_dict, value, None)
        elif dest_state.has_identity and self.key not in dest_dict:
            dest_state._expire_attributes(dest_dict, [self.key])

    class Comparator(PropComparator):
        """Produce boolean, comparison, and other operators for
        :class:`.ColumnProperty` attributes.

        See the documentation for :class:`.PropComparator` for a brief
        overview.

        See also:

        :class:`.PropComparator`

        :class:`.ColumnOperators`

        :ref:`types_operators`

        :attr:`.TypeEngine.comparator_factory`

        """
        @util.memoized_instancemethod
        def __clause_element__(self):
            if self.adapter:
                return self.adapter(self.prop.columns[0])
            else:
                return self.prop.columns[0]._annotate({
                    "parententity": self._parentmapper,
                    "parentmapper": self._parentmapper})

        @util.memoized_property
        def info(self):
            ce = self.__clause_element__()
            try:
                return ce.info
            except AttributeError:
                return self.prop.info

        def __getattr__(self, key):
            """proxy attribute access down to the mapped column.

            this allows user-defined comparison methods to be accessed.
            """
            return getattr(self.__clause_element__(), key)

        def operate(self, op, *other, **kwargs):
            return op(self.__clause_element__(), *other, **kwargs)

        def reverse_operate(self, op, other, **kwargs):
            col = self.__clause_element__()
            return op(col._bind_param(op, other), col, **kwargs)

    def __str__(self):
        return str(self.parent.class_.__name__) + "." + self.key

