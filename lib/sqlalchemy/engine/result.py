# engine/result.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Define result set constructs including :class:`.ResultProxy`
and :class:`.RowProxy."""


from .. import exc, util
from ..sql import expression, sqltypes
import collections
import operator

# This reconstructor is necessary so that pickles with the C extension or
# without use the same Binary format.
try:
    # We need a different reconstructor on the C extension so that we can
    # add extra checks that fields have correctly been initialized by
    # __setstate__.
    from sqlalchemy.cresultproxy import safe_rowproxy_reconstructor

    # The extra function embedding is needed so that the
    # reconstructor function has the same signature whether or not
    # the extension is present.
    def rowproxy_reconstructor(cls, state):
        return safe_rowproxy_reconstructor(cls, state)
except ImportError:
    def rowproxy_reconstructor(cls, state):
        obj = cls.__new__(cls)
        obj.__setstate__(state)
        return obj

try:
    from sqlalchemy.cresultproxy import BaseRowProxy
except ImportError:
    class BaseRowProxy(object):
        __slots__ = ('_parent', '_row', '_processors', '_keymap')

        def __init__(self, parent, row, processors, keymap):
            """RowProxy objects are constructed by ResultProxy objects."""

            self._parent = parent
            self._row = row
            self._processors = processors
            self._keymap = keymap

        def __reduce__(self):
            return (rowproxy_reconstructor,
                    (self.__class__, self.__getstate__()))

        def values(self):
            """Return the values represented by this RowProxy as a list."""
            return list(self)

        def __iter__(self):
            for processor, value in zip(self._processors, self._row):
                if processor is None:
                    yield value
                else:
                    yield processor(value)

        def __len__(self):
            return len(self._row)

        def __getitem__(self, key):
            try:
                processor, obj, index = self._keymap[key]
            except KeyError:
                processor, obj, index = self._parent._key_fallback(key)
            except TypeError:
                if isinstance(key, slice):
                    l = []
                    for processor, value in zip(self._processors[key],
                                                self._row[key]):
                        if processor is None:
                            l.append(value)
                        else:
                            l.append(processor(value))
                    return tuple(l)
                else:
                    raise
            if index is None:
                raise exc.InvalidRequestError(
                    "Ambiguous column name '%s' in result set! "
                    "try 'use_labels' option on select statement." % key)
            if processor is not None:
                return processor(self._row[index])
            else:
                return self._row[index]

        def __getattr__(self, name):
            try:
                return self[name]
            except KeyError as e:
                raise AttributeError(e.args[0])


class RowProxy(BaseRowProxy):
    """Proxy values from a single cursor row.

    Mostly follows "ordered dictionary" behavior, mapping result
    values to the string-based column name, the integer position of
    the result in the row, as well as Column instances which can be
    mapped to the original Columns that produced this result set (for
    results that correspond to constructed SQL expressions).
    """
    __slots__ = ()

    def __contains__(self, key):
        return self._parent._has_key(key)

    def __getstate__(self):
        return {
            '_parent': self._parent,
            '_row': tuple(self)
        }

    def __setstate__(self, state):
        self._parent = parent = state['_parent']
        self._row = state['_row']
        self._processors = parent._processors
        self._keymap = parent._keymap

    __hash__ = None

    def _op(self, other, op):
        return op(tuple(self), tuple(other)) \
            if isinstance(other, RowProxy) \
            else op(tuple(self), other)

    def __lt__(self, other):
        return self._op(other, operator.lt)

    def __le__(self, other):
        return self._op(other, operator.le)

    def __ge__(self, other):
        return self._op(other, operator.ge)

    def __gt__(self, other):
        return self._op(other, operator.gt)

    def __eq__(self, other):
        return self._op(other, operator.eq)

    def __ne__(self, other):
        return self._op(other, operator.ne)

    def __repr__(self):
        return repr(tuple(self))

    def has_key(self, key):
        """Return True if this RowProxy contains the given key."""

        return self._parent._has_key(key)

    def items(self):
        """Return a list of tuples, each tuple containing a key/value pair."""
        # TODO: no coverage here
        return [(key, self[key]) for key in self.keys()]

    def keys(self):
        """Return the list of keys as strings represented by this RowProxy."""

        return self._parent.keys

    def iterkeys(self):
        return iter(self._parent.keys)

    def itervalues(self):
        return iter(self)

try:
    # Register RowProxy with Sequence,
    # so sequence protocol is implemented
    from collections import Sequence
    Sequence.register(RowProxy)
except ImportError:
    pass


class ResultMetaData(object):
    """Handle cursor.description, applying additional info from an execution
    context."""

    def __init__(self, parent, metadata):
        context = parent.context
        dialect = context.dialect
        typemap = dialect.dbapi_type_map
        translate_colname = context._translate_colname
        self.case_sensitive = case_sensitive = dialect.case_sensitive

        if context.result_column_struct:
            result_columns, cols_are_ordered = context.result_column_struct
            num_ctx_cols = len(result_columns)
        else:
            num_ctx_cols = None

        if num_ctx_cols and \
                cols_are_ordered and \
                num_ctx_cols == len(metadata):
            # case 1 - SQL expression statement, number of columns
            # in result matches number of cols in compiled.  This is the
            # vast majority case for SQL expression constructs.  In this
            # case we don't bother trying to parse or match up to
            # the colnames in the result description.
            raw = [
                (
                    idx,
                    key,
                    name.lower() if not case_sensitive else name,
                    context.get_result_processor(
                        type_, key, metadata[idx][1]
                    ),
                    obj,
                    None
                ) for idx, (key, name, obj, type_)
                in enumerate(result_columns)
            ]
            self.keys = [
                elem[0] for elem in result_columns
            ]
        else:
            # case 2 - raw string, or number of columns in result does
            # not match number of cols in compiled.  The raw string case
            # is very common.   The latter can happen
            # when text() is used with only a partial typemap, or
            # in the extremely unlikely cases where the compiled construct
            # has a single element with multiple col expressions in it
            # (e.g. has commas embedded) or there's some kind of statement
            # that is adding extra columns.
            # In all these cases we fall back to the "named" approach
            # that SQLAlchemy has used up through 0.9.

            if num_ctx_cols:
                result_map = self._create_result_map(
                    result_columns, case_sensitive)

            raw = []
            self.keys = []
            untranslated = None
            for idx, rec in enumerate(metadata):
                colname = rec[0]
                coltype = rec[1]

                if dialect.description_encoding:
                    colname = dialect._description_decoder(colname)

                if translate_colname:
                    colname, untranslated = translate_colname(colname)

                if dialect.requires_name_normalize:
                    colname = dialect.normalize_name(colname)

                self.keys.append(colname)
                if not case_sensitive:
                    colname = colname.lower()

                if num_ctx_cols:
                    try:
                        ctx_rec = result_map[colname]
                    except KeyError:
                        mapped_type = typemap.get(coltype, sqltypes.NULLTYPE)
                        obj = None
                    else:
                        obj = ctx_rec[1]
                        mapped_type = ctx_rec[2]
                else:
                    mapped_type = typemap.get(coltype, sqltypes.NULLTYPE)
                    obj = None
                processor = context.get_result_processor(
                    mapped_type, colname, coltype)

                raw.append(
                    (idx, colname, colname, processor, obj, untranslated)
                )

        # keymap indexes by integer index...
        self._keymap = dict([
            (elem[0], (elem[3], elem[4], elem[0]))
            for elem in raw
        ])

        # processors in key order for certain per-row
        # views like __iter__ and slices
        self._processors = [elem[3] for elem in raw]

        if num_ctx_cols:
            # keymap by primary string...
            by_key = dict([
                (elem[2], (elem[3], elem[4], elem[0]))
                for elem in raw
            ])

            # if by-primary-string dictionary smaller (or bigger?!) than
            # number of columns, assume we have dupes, rewrite
            # dupe records with "None" for index which results in
            # ambiguous column exception when accessed.
            if len(by_key) != num_ctx_cols:
                seen = set()
                for rec in raw:
                    key = rec[1]
                    if key in seen:
                        by_key[key] = (None, by_key[key][1], None)
                    seen.add(key)

            # update keymap with secondary "object"-based keys
            self._keymap.update([
                (obj_elem, by_key[elem[2]])
                for elem in raw if elem[4]
                for obj_elem in elem[4]
            ])

            # update keymap with primary string names taking
            # precedence
            self._keymap.update(by_key)
        else:
            self._keymap.update([
                (elem[2], (elem[3], elem[4], elem[0]))
                for elem in raw
            ])
            # update keymap with "translated" names (sqlite-only thing)
            if translate_colname:
                self._keymap.update([
                    (elem[5], self._keymap[elem[2]])
                    for elem in raw if elem[5]
                ])

    @classmethod
    def _create_result_map(cls, result_columns, case_sensitive=True):
        d = {}
        for elem in result_columns:
            key, rec = elem[0], elem[1:]
            if not case_sensitive:
                key = key.lower()
            if key in d:
                # conflicting keyname, just double up the list
                # of objects.  this will cause an "ambiguous name"
                # error if an attempt is made by the result set to
                # access.
                e_name, e_obj, e_type = d[key]
                d[key] = e_name, e_obj + rec[1], e_type
            else:
                d[key] = rec
        return d

    @util.pending_deprecation("0.8", "sqlite dialect uses "
                              "_translate_colname() now")
    def _set_keymap_synonym(self, name, origname):
        """Set a synonym for the given name.

        Some dialects (SQLite at the moment) may use this to
        adjust the column names that are significant within a
        row.

        """
        rec = (processor, obj, i) = self._keymap[origname if
                                                 self.case_sensitive
                                                 else origname.lower()]
        if self._keymap.setdefault(name, rec) is not rec:
            self._keymap[name] = (processor, obj, None)

    def _key_fallback(self, key, raiseerr=True):
        map = self._keymap
        result = None
        if isinstance(key, util.string_types):
            result = map.get(key if self.case_sensitive else key.lower())
        # fallback for targeting a ColumnElement to a textual expression
        # this is a rare use case which only occurs when matching text()
        # or colummn('name') constructs to ColumnElements, or after a
        # pickle/unpickle roundtrip
        elif isinstance(key, expression.ColumnElement):
            if key._label and (
                    key._label
                    if self.case_sensitive
                    else key._label.lower()) in map:
                result = map[key._label
                             if self.case_sensitive
                             else key._label.lower()]
            elif hasattr(key, 'name') and (
                    key.name
                    if self.case_sensitive
                    else key.name.lower()) in map:
                # match is only on name.
                result = map[key.name
                             if self.case_sensitive
                             else key.name.lower()]
            # search extra hard to make sure this
            # isn't a column/label name overlap.
            # this check isn't currently available if the row
            # was unpickled.
            if result is not None and \
                    result[1] is not None:
                for obj in result[1]:
                    if key._compare_name_for_result(obj):
                        break
                else:
                    result = None
        if result is None:
            if raiseerr:
                raise exc.NoSuchColumnError(
                    "Could not locate column in row for column '%s'" %
                    expression._string_or_unprintable(key))
            else:
                return None
        else:
            map[key] = result
        return result

    def _has_key(self, key):
        if key in self._keymap:
            return True
        else:
            return self._key_fallback(key, False) is not None

    def _getter(self, key):
        if key in self._keymap:
            processor, obj, index = self._keymap[key]
        else:
            ret = self._key_fallback(key, False)
            if ret is None:
                return None
            processor, obj, index = ret

        if index is None:
            raise exc.InvalidRequestError(
                "Ambiguous column name '%s' in result set! "
                "try 'use_labels' option on select statement." % key)

        return operator.itemgetter(index)

    def __getstate__(self):
        return {
            '_pickled_keymap': dict(
                (key, index)
                for key, (processor, obj, index) in self._keymap.items()
                if isinstance(key, util.string_types + util.int_types)
            ),
            'keys': self.keys,
            "case_sensitive": self.case_sensitive,
        }

    def __setstate__(self, state):
        # the row has been processed at pickling time so we don't need any
        # processor anymore
        self._processors = [None for _ in range(len(state['keys']))]
        self._keymap = keymap = {}
        for key, index in state['_pickled_keymap'].items():
            # not preserving "obj" here, unfortunately our
            # proxy comparison fails with the unpickle
            keymap[key] = (None, None, index)
        self.keys = state['keys']
        self.case_sensitive = state['case_sensitive']
        self._echo = False


class ResultProxy(object):
    """Wraps a DB-API cursor object to provide easier access to row columns.

    Individual columns may be accessed by their integer position,
    case-insensitive column name, or by ``schema.Column``
    object. e.g.::

      row = fetchone()

      col1 = row[0]    # access via integer position

      col2 = row['col2']   # access via name

      col3 = row[mytable.c.mycol] # access via Column object.

    ``ResultProxy`` also handles post-processing of result column
    data using ``TypeEngine`` objects, which are referenced from
    the originating SQL statement that produced this result set.

    """

    _process_row = RowProxy
    out_parameters = None
    _can_close_connection = False
    _metadata = None
    _soft_closed = False
    closed = False

    def __init__(self, context):
        self.context = context
        self.dialect = context.dialect
        self.cursor = self._saved_cursor = context.cursor
        self.connection = context.root_connection
        self._echo = self.connection._echo and \
            context.engine._should_log_debug()
        self._init_metadata()

    def _getter(self, key):
        try:
            getter = self._metadata._getter
        except AttributeError:
            return self._non_result(None)
        else:
            return getter(key)

    def _has_key(self, key):
        try:
            has_key = self._metadata._has_key
        except AttributeError:
            return self._non_result(None)
        else:
            return has_key(key)

    def _init_metadata(self):
        metadata = self._cursor_description()
        if metadata is not None:
            if self.context.compiled and \
                    'compiled_cache' in self.context.execution_options:
                if self.context.compiled._cached_metadata:
                    self._metadata = self.context.compiled._cached_metadata
                else:
                    self._metadata = self.context.compiled._cached_metadata = \
                        ResultMetaData(self, metadata)
            else:
                self._metadata = ResultMetaData(self, metadata)
            if self._echo:
                self.context.engine.logger.debug(
                    "Col %r", tuple(x[0] for x in metadata))

    def keys(self):
        """Return the current set of string keys for rows."""
        if self._metadata:
            return self._metadata.keys
        else:
            return []

    @util.memoized_property
    def rowcount(self):
        """Return the 'rowcount' for this result.

        The 'rowcount' reports the number of rows *matched*
        by the WHERE criterion of an UPDATE or DELETE statement.

        .. note::

           Notes regarding :attr:`.ResultProxy.rowcount`:


           * This attribute returns the number of rows *matched*,
             which is not necessarily the same as the number of rows
             that were actually *modified* - an UPDATE statement, for example,
             may have no net change on a given row if the SET values
             given are the same as those present in the row already.
             Such a row would be matched but not modified.
             On backends that feature both styles, such as MySQL,
             rowcount is configured by default to return the match
             count in all cases.

           * :attr:`.ResultProxy.rowcount` is *only* useful in conjunction
             with an UPDATE or DELETE statement.  Contrary to what the Python
             DBAPI says, it does *not* return the
             number of rows available from the results of a SELECT statement
             as DBAPIs cannot support this functionality when rows are
             unbuffered.

           * :attr:`.ResultProxy.rowcount` may not be fully implemented by
             all dialects.  In particular, most DBAPIs do not support an
             aggregate rowcount result from an executemany call.
             The :meth:`.ResultProxy.supports_sane_rowcount` and
             :meth:`.ResultProxy.supports_sane_multi_rowcount` methods
             will report from the dialect if each usage is known to be
             supported.

           * Statements that use RETURNING may not return a correct
             rowcount.

        """
        try:
            return self.context.rowcount
        except Exception as e:
            self.connection._handle_dbapi_exception(
                e, None, None, self.cursor, self.context)

    @property
    def lastrowid(self):
        """return the 'lastrowid' accessor on the DBAPI cursor.

        This is a DBAPI specific method and is only functional
        for those backends which support it, for statements
        where it is appropriate.  It's behavior is not
        consistent across backends.

        Usage of this method is normally unnecessary when
        using insert() expression constructs; the
        :attr:`~ResultProxy.inserted_primary_key` attribute provides a
        tuple of primary key values for a newly inserted row,
        regardless of database backend.

        """
        try:
            return self._saved_cursor.lastrowid
        except Exception as e:
            self.connection._handle_dbapi_exception(
                e, None, None,
                self._saved_cursor, self.context)

    @property
    def returns_rows(self):
        """True if this :class:`.ResultProxy` returns rows.

        I.e. if it is legal to call the methods
        :meth:`~.ResultProxy.fetchone`,
        :meth:`~.ResultProxy.fetchmany`
        :meth:`~.ResultProxy.fetchall`.

        """
        return self._metadata is not None

    @property
    def is_insert(self):
        """True if this :class:`.ResultProxy` is the result
        of a executing an expression language compiled
        :func:`.expression.insert` construct.

        When True, this implies that the
        :attr:`inserted_primary_key` attribute is accessible,
        assuming the statement did not include
        a user defined "returning" construct.

        """
        return self.context.isinsert

    def _cursor_description(self):
        """May be overridden by subclasses."""

        return self._saved_cursor.description

    def _soft_close(self, _autoclose_connection=True):
        """Soft close this :class:`.ResultProxy`.

        This releases all DBAPI cursor resources, but leaves the
        ResultProxy "open" from a semantic perspective, meaning the
        fetchXXX() methods will continue to return empty results.

        This method is called automatically when:

        * all result rows are exhausted using the fetchXXX() methods.
        * cursor.description is None.

        This method is **not public**, but is documented in order to clarify
        the "autoclose" process used.

        .. versionadded:: 1.0.0

        .. seealso::

            :meth:`.ResultProxy.close`


        """
        if self._soft_closed:
            return
        self._soft_closed = True
        cursor = self.cursor
        self.connection._safe_close_cursor(cursor)
        if _autoclose_connection and \
                self.connection.should_close_with_result:
            self.connection.close()
        self.cursor = None

    def close(self):
        """Close this ResultProxy.

        This closes out the underlying DBAPI cursor corresonding
        to the statement execution, if one is stil present.  Note that the
        DBAPI cursor is automatically released when the :class:`.ResultProxy`
        exhausts all available rows.  :meth:`.ResultProxy.close` is generally
        an optional method except in the case when discarding a
        :class:`.ResultProxy` that still has additional rows pending for fetch.

        In the case of a result that is the product of
        :ref:`connectionless execution <dbengine_implicit>`,
        the underyling :class:`.Connection` object is also closed, which
        :term:`releases` DBAPI connection resources.

        After this method is called, it is no longer valid to call upon
        the fetch methods, which will raise a :class:`.ResourceClosedError`
        on subsequent use.

        .. versionchanged:: 1.0.0 - the :meth:`.ResultProxy.close` method
           has been separated out from the process that releases the underlying
           DBAPI cursor resource.   The "auto close" feature of the
           :class:`.Connection` now performs a so-called "soft close", which
           releases the underlying DBAPI cursor, but allows the
           :class:`.ResultProxy` to still behave as an open-but-exhausted
           result set; the actual :meth:`.ResultProxy.close` method is never
           called.    It is still safe to discard a :class:`.ResultProxy`
           that has been fully exhausted without calling this method.

        .. seealso::

            :ref:`connections_toplevel`

            :meth:`.ResultProxy._soft_close`

        """

        if not self.closed:
            self._soft_close()
            self.closed = True

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                return
            else:
                yield row

    @util.memoized_property
    def inserted_primary_key(self):
        """Return the primary key for the row just inserted.

        The return value is a list of scalar values
        corresponding to the list of primary key columns
        in the target table.

        This only applies to single row :func:`.insert`
        constructs which did not explicitly specify
        :meth:`.Insert.returning`.

        Note that primary key columns which specify a
        server_default clause,
        or otherwise do not qualify as "autoincrement"
        columns (see the notes at :class:`.Column`), and were
        generated using the database-side default, will
        appear in this list as ``None`` unless the backend
        supports "returning" and the insert statement executed
        with the "implicit returning" enabled.

        Raises :class:`~sqlalchemy.exc.InvalidRequestError` if the executed
        statement is not a compiled expression construct
        or is not an insert() construct.

        """

        if not self.context.compiled:
            raise exc.InvalidRequestError(
                "Statement is not a compiled "
                "expression construct.")
        elif not self.context.isinsert:
            raise exc.InvalidRequestError(
                "Statement is not an insert() "
                "expression construct.")
        elif self.context._is_explicit_returning:
            raise exc.InvalidRequestError(
                "Can't call inserted_primary_key "
                "when returning() "
                "is used.")

        return self.context.inserted_primary_key

    def last_updated_params(self):
        """Return the collection of updated parameters from this
        execution.

        Raises :class:`~sqlalchemy.exc.InvalidRequestError` if the executed
        statement is not a compiled expression construct
        or is not an update() construct.

        """
        if not self.context.compiled:
            raise exc.InvalidRequestError(
                "Statement is not a compiled "
                "expression construct.")
        elif not self.context.isupdate:
            raise exc.InvalidRequestError(
                "Statement is not an update() "
                "expression construct.")
        elif self.context.executemany:
            return self.context.compiled_parameters
        else:
            return self.context.compiled_parameters[0]

    def last_inserted_params(self):
        """Return the collection of inserted parameters from this
        execution.

        Raises :class:`~sqlalchemy.exc.InvalidRequestError` if the executed
        statement is not a compiled expression construct
        or is not an insert() construct.

        """
        if not self.context.compiled:
            raise exc.InvalidRequestError(
                "Statement is not a compiled "
                "expression construct.")
        elif not self.context.isinsert:
            raise exc.InvalidRequestError(
                "Statement is not an insert() "
                "expression construct.")
        elif self.context.executemany:
            return self.context.compiled_parameters
        else:
            return self.context.compiled_parameters[0]

    @property
    def returned_defaults(self):
        """Return the values of default columns that were fetched using
        the :meth:`.ValuesBase.return_defaults` feature.

        The value is an instance of :class:`.RowProxy`, or ``None``
        if :meth:`.ValuesBase.return_defaults` was not used or if the
        backend does not support RETURNING.

        .. versionadded:: 0.9.0

        .. seealso::

            :meth:`.ValuesBase.return_defaults`

        """
        return self.context.returned_defaults

    def lastrow_has_defaults(self):
        """Return ``lastrow_has_defaults()`` from the underlying
        :class:`.ExecutionContext`.

        See :class:`.ExecutionContext` for details.

        """

        return self.context.lastrow_has_defaults()

    def postfetch_cols(self):
        """Return ``postfetch_cols()`` from the underlying
        :class:`.ExecutionContext`.

        See :class:`.ExecutionContext` for details.

        Raises :class:`~sqlalchemy.exc.InvalidRequestError` if the executed
        statement is not a compiled expression construct
        or is not an insert() or update() construct.

        """

        if not self.context.compiled:
            raise exc.InvalidRequestError(
                "Statement is not a compiled "
                "expression construct.")
        elif not self.context.isinsert and not self.context.isupdate:
            raise exc.InvalidRequestError(
                "Statement is not an insert() or update() "
                "expression construct.")
        return self.context.postfetch_cols

    def prefetch_cols(self):
        """Return ``prefetch_cols()`` from the underlying
        :class:`.ExecutionContext`.

        See :class:`.ExecutionContext` for details.

        Raises :class:`~sqlalchemy.exc.InvalidRequestError` if the executed
        statement is not a compiled expression construct
        or is not an insert() or update() construct.

        """

        if not self.context.compiled:
            raise exc.InvalidRequestError(
                "Statement is not a compiled "
                "expression construct.")
        elif not self.context.isinsert and not self.context.isupdate:
            raise exc.InvalidRequestError(
                "Statement is not an insert() or update() "
                "expression construct.")
        return self.context.prefetch_cols

    def supports_sane_rowcount(self):
        """Return ``supports_sane_rowcount`` from the dialect.

        See :attr:`.ResultProxy.rowcount` for background.

        """

        return self.dialect.supports_sane_rowcount

    def supports_sane_multi_rowcount(self):
        """Return ``supports_sane_multi_rowcount`` from the dialect.

        See :attr:`.ResultProxy.rowcount` for background.

        """

        return self.dialect.supports_sane_multi_rowcount

    def _fetchone_impl(self):
        try:
            return self.cursor.fetchone()
        except AttributeError:
            return self._non_result(None)

    def _fetchmany_impl(self, size=None):
        try:
            if size is None:
                return self.cursor.fetchmany()
            else:
                return self.cursor.fetchmany(size)
        except AttributeError:
            return self._non_result([])

    def _fetchall_impl(self):
        try:
            return self.cursor.fetchall()
        except AttributeError:
            return self._non_result([])

    def _non_result(self, default):
        if self._metadata is None:
            raise exc.ResourceClosedError(
                "This result object does not return rows. "
                "It has been closed automatically.",
            )
        elif self.closed:
            raise exc.ResourceClosedError("This result object is closed.")
        else:
            return default

    def process_rows(self, rows):
        process_row = self._process_row
        metadata = self._metadata
        keymap = metadata._keymap
        processors = metadata._processors
        if self._echo:
            log = self.context.engine.logger.debug
            l = []
            for row in rows:
                log("Row %r", row)
                l.append(process_row(metadata, row, processors, keymap))
            return l
        else:
            return [process_row(metadata, row, processors, keymap)
                    for row in rows]

    def fetchall(self):
        """Fetch all rows, just like DB-API ``cursor.fetchall()``.

        After all rows have been exhausted, the underlying DBAPI
        cursor resource is released, and the object may be safely
        discarded.

        Subsequent calls to :meth:`.ResultProxy.fetchall` will return
        an empty list.   After the :meth:`.ResultProxy.close` method is
        called, the method will raise :class:`.ResourceClosedError`.

        .. versionchanged:: 1.0.0 - Added "soft close" behavior which
           allows the result to be used in an "exhausted" state prior to
           calling the :meth:`.ResultProxy.close` method.

        """

        try:
            l = self.process_rows(self._fetchall_impl())
            self._soft_close()
            return l
        except Exception as e:
            self.connection._handle_dbapi_exception(
                e, None, None,
                self.cursor, self.context)

    def fetchmany(self, size=None):
        """Fetch many rows, just like DB-API
        ``cursor.fetchmany(size=cursor.arraysize)``.

        After all rows have been exhausted, the underlying DBAPI
        cursor resource is released, and the object may be safely
        discarded.

        Calls to :meth:`.ResultProxy.fetchmany` after all rows have been
        exhuasted will return
        an empty list.   After the :meth:`.ResultProxy.close` method is
        called, the method will raise :class:`.ResourceClosedError`.

        .. versionchanged:: 1.0.0 - Added "soft close" behavior which
           allows the result to be used in an "exhausted" state prior to
           calling the :meth:`.ResultProxy.close` method.

        """

        try:
            l = self.process_rows(self._fetchmany_impl(size))
            if len(l) == 0:
                self._soft_close()
            return l
        except Exception as e:
            self.connection._handle_dbapi_exception(
                e, None, None,
                self.cursor, self.context)

    def fetchone(self):
        """Fetch one row, just like DB-API ``cursor.fetchone()``.

        After all rows have been exhausted, the underlying DBAPI
        cursor resource is released, and the object may be safely
        discarded.

        Calls to :meth:`.ResultProxy.fetchone` after all rows have
        been exhausted will return ``None``.
        After the :meth:`.ResultProxy.close` method is
        called, the method will raise :class:`.ResourceClosedError`.

        .. versionchanged:: 1.0.0 - Added "soft close" behavior which
           allows the result to be used in an "exhausted" state prior to
           calling the :meth:`.ResultProxy.close` method.

        """
        try:
            row = self._fetchone_impl()
            if row is not None:
                return self.process_rows([row])[0]
            else:
                self._soft_close()
                return None
        except Exception as e:
            self.connection._handle_dbapi_exception(
                e, None, None,
                self.cursor, self.context)

    def first(self):
        """Fetch the first row and then close the result set unconditionally.

        Returns None if no row is present.

        After calling this method, the object is fully closed,
        e.g. the :meth:`.ResultProxy.close` method will have been called.

        """
        if self._metadata is None:
            return self._non_result(None)

        try:
            row = self._fetchone_impl()
        except Exception as e:
            self.connection._handle_dbapi_exception(
                e, None, None,
                self.cursor, self.context)

        try:
            if row is not None:
                return self.process_rows([row])[0]
            else:
                return None
        finally:
            self.close()

    def scalar(self):
        """Fetch the first column of the first row, and close the result set.

        Returns None if no row is present.

        After calling this method, the object is fully closed,
        e.g. the :meth:`.ResultProxy.close` method will have been called.

        """
        row = self.first()
        if row is not None:
            return row[0]
        else:
            return None


class BufferedRowResultProxy(ResultProxy):
    """A ResultProxy with row buffering behavior.

    ``ResultProxy`` that buffers the contents of a selection of rows
    before ``fetchone()`` is called.  This is to allow the results of
    ``cursor.description`` to be available immediately, when
    interfacing with a DB-API that requires rows to be consumed before
    this information is available (currently psycopg2, when used with
    server-side cursors).

    The pre-fetching behavior fetches only one row initially, and then
    grows its buffer size by a fixed amount with each successive need
    for additional rows up to a size of 1000.

    The size argument is configurable using the ``max_row_buffer``
    execution option::

        with psycopg2_engine.connect() as conn:

            result = conn.execution_options(
                stream_results=True, max_row_buffer=50
                ).execute("select * from table")

    .. versionadded:: 1.0.6 Added the ``max_row_buffer`` option.

    .. seealso::

        :ref:`psycopg2_execution_options`
    """

    def _init_metadata(self):
        self._max_row_buffer = self.context.execution_options.get(
            'max_row_buffer', None)
        self.__buffer_rows()
        super(BufferedRowResultProxy, self)._init_metadata()

    # this is a "growth chart" for the buffering of rows.
    # each successive __buffer_rows call will use the next
    # value in the list for the buffer size until the max
    # is reached
    size_growth = {
        1: 5,
        5: 10,
        10: 20,
        20: 50,
        50: 100,
        100: 250,
        250: 500,
        500: 1000
    }

    def __buffer_rows(self):
        if self.cursor is None:
            return
        size = getattr(self, '_bufsize', 1)
        self.__rowbuffer = collections.deque(self.cursor.fetchmany(size))
        self._bufsize = self.size_growth.get(size, size)
        if self._max_row_buffer is not None:
            self._bufsize = min(self._max_row_buffer, self._bufsize)

    def _soft_close(self, **kw):
        self.__rowbuffer.clear()
        super(BufferedRowResultProxy, self)._soft_close(**kw)

    def _fetchone_impl(self):
        if self.cursor is None:
            return self._non_result(None)
        if not self.__rowbuffer:
            self.__buffer_rows()
            if not self.__rowbuffer:
                return None
        return self.__rowbuffer.popleft()

    def _fetchmany_impl(self, size=None):
        if size is None:
            return self._fetchall_impl()
        result = []
        for x in range(0, size):
            row = self._fetchone_impl()
            if row is None:
                break
            result.append(row)
        return result

    def _fetchall_impl(self):
        if self.cursor is None:
            return self._non_result([])
        self.__rowbuffer.extend(self.cursor.fetchall())
        ret = self.__rowbuffer
        self.__rowbuffer = collections.deque()
        return ret


class FullyBufferedResultProxy(ResultProxy):
    """A result proxy that buffers rows fully upon creation.

    Used for operations where a result is to be delivered
    after the database conversation can not be continued,
    such as MSSQL INSERT...OUTPUT after an autocommit.

    """

    def _init_metadata(self):
        super(FullyBufferedResultProxy, self)._init_metadata()
        self.__rowbuffer = self._buffer_rows()

    def _buffer_rows(self):
        return collections.deque(self.cursor.fetchall())

    def _soft_close(self, **kw):
        self.__rowbuffer.clear()
        super(FullyBufferedResultProxy, self)._soft_close(**kw)

    def _fetchone_impl(self):
        if self.__rowbuffer:
            return self.__rowbuffer.popleft()
        else:
            return self._non_result(None)

    def _fetchmany_impl(self, size=None):
        if size is None:
            return self._fetchall_impl()
        result = []
        for x in range(0, size):
            row = self._fetchone_impl()
            if row is None:
                break
            result.append(row)
        return result

    def _fetchall_impl(self):
        if not self.cursor:
            return self._non_result([])
        ret = self.__rowbuffer
        self.__rowbuffer = collections.deque()
        return ret


class BufferedColumnRow(RowProxy):
    def __init__(self, parent, row, processors, keymap):
        # preprocess row
        row = list(row)
        # this is a tad faster than using enumerate
        index = 0
        for processor in parent._orig_processors:
            if processor is not None:
                row[index] = processor(row[index])
            index += 1
        row = tuple(row)
        super(BufferedColumnRow, self).__init__(parent, row,
                                                processors, keymap)


class BufferedColumnResultProxy(ResultProxy):
    """A ResultProxy with column buffering behavior.

    ``ResultProxy`` that loads all columns into memory each time
    fetchone() is called.  If fetchmany() or fetchall() are called,
    the full grid of results is fetched.  This is to operate with
    databases where result rows contain "live" results that fall out
    of scope unless explicitly fetched.  Currently this includes
    cx_Oracle LOB objects.

    """

    _process_row = BufferedColumnRow

    def _init_metadata(self):
        super(BufferedColumnResultProxy, self)._init_metadata()
        metadata = self._metadata
        # orig_processors will be used to preprocess each row when they are
        # constructed.
        metadata._orig_processors = metadata._processors
        # replace the all type processors by None processors.
        metadata._processors = [None for _ in range(len(metadata.keys))]
        keymap = {}
        for k, (func, obj, index) in metadata._keymap.items():
            keymap[k] = (None, obj, index)
        self._metadata._keymap = keymap

    def fetchall(self):
        # can't call cursor.fetchall(), since rows must be
        # fully processed before requesting more from the DBAPI.
        l = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            l.append(row)
        return l

    def fetchmany(self, size=None):
        # can't call cursor.fetchmany(), since rows must be
        # fully processed before requesting more from the DBAPI.
        if size is None:
            return self.fetchall()
        l = []
        for i in range(size):
            row = self.fetchone()
            if row is None:
                break
            l.append(row)
        return l
