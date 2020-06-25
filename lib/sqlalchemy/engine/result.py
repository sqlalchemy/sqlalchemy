# engine/result.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Define result set constructs including :class:`_engine.ResultProxy`
and :class:`.RowProxy`."""


import collections
import operator

from .. import exc
from .. import util
from ..sql import expression
from ..sql import sqltypes
from ..sql import util as sql_util


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

    _baserowproxy_usecext = True
except ImportError:
    _baserowproxy_usecext = False

    class BaseRowProxy(object):
        __slots__ = ("_parent", "_row", "_processors", "_keymap")

        def __init__(self, parent, row, processors, keymap):
            """RowProxy objects are constructed by ResultProxy objects."""

            self._parent = parent
            self._row = row
            self._processors = processors
            self._keymap = keymap

        def __reduce__(self):
            return (
                rowproxy_reconstructor,
                (self.__class__, self.__getstate__()),
            )

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
            except KeyError as err:
                processor, obj, index = self._parent._key_fallback(key, err)
            except TypeError:
                if isinstance(key, slice):
                    l = []
                    for processor, value in zip(
                        self._processors[key], self._row[key]
                    ):
                        if processor is None:
                            l.append(value)
                        else:
                            l.append(processor(value))
                    return tuple(l)
                else:
                    raise
            if index is None:
                raise exc.InvalidRequestError(
                    "Ambiguous column name '%s' in "
                    "result set column descriptions" % obj
                )
            if processor is not None:
                return processor(self._row[index])
            else:
                return self._row[index]

        def __getattr__(self, name):
            try:
                return self[name]
            except KeyError as e:
                util.raise_(AttributeError(e.args[0]), replace_context=e)


class RowProxy(BaseRowProxy):
    """Represent a single result row.

    The :class:`.RowProxy` object is retrieved from a database result, from the
    :class:`_engine.ResultProxy` object using methods like
    :meth:`_engine.ResultProxy.fetchall`.

    The :class:`.RowProxy` object seeks to act mostly like a Python named
    tuple, but also provides some Python dictionary behaviors at the same time.

    .. seealso::

        :ref:`coretutorial_selecting` - includes examples of selecting
        rows from SELECT statements.

    """

    __slots__ = ()

    def __contains__(self, key):
        return self._parent._has_key(key)

    def __getstate__(self):
        return {"_parent": self._parent, "_row": tuple(self)}

    def __setstate__(self, state):
        self._parent = parent = state["_parent"]
        self._row = state["_row"]
        self._processors = parent._processors
        self._keymap = parent._keymap

    __hash__ = None

    def _op(self, other, op):
        return (
            op(tuple(self), tuple(other))
            if isinstance(other, RowProxy)
            else op(tuple(self), other)
        )

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
        return repr(sql_util._repr_row(self))

    def has_key(self, key):
        """Return True if this :class:`.RowProxy` contains the given key.

        Through the SQLAlchemy 1.x series, the ``__contains__()`` method
        of :class:`.RowProxy` also links to :meth:`.RowProxy.has_key`, in that
        an expression such as ::

            "some_col" in row

        Will return True if the row contains a column named ``"some_col"``,
        in the way that a Python mapping works.

        However, it is planned that the 2.0 series of SQLAlchemy will reverse
        this behavior so that ``__contains__()`` will refer to a value being
        present in the row, in the way that a Python tuple works.

        """

        return self._parent._has_key(key)

    def items(self):
        """Return a list of tuples, each tuple containing a key/value pair.

        This method is analogous to the Python dictionary ``.items()`` method,
        except that it returns a list, not an iterator.

        """

        return [(key, self[key]) for key in self.keys()]

    def keys(self):
        """Return the list of keys as strings represented by this
        :class:`.RowProxy`.

        This method is analogous to the Python dictionary ``.keys()`` method,
        except that it returns a list, not an iterator.

        """

        return self._parent.keys

    def iterkeys(self):
        """Return a an iterator against the :meth:`.RowProxy.keys` method.

        This method is analogous to the Python-2-only dictionary
        ``.iterkeys()`` method.

        """
        return iter(self._parent.keys)

    def itervalues(self):
        """Return a an iterator against the :meth:`.RowProxy.values` method.

        This method is analogous to the Python-2-only dictionary
        ``.itervalues()`` method.

        """
        return iter(self)

    def values(self):
        """Return the values represented by this :class:`.RowProxy` as a list.

        This method is analogous to the Python dictionary ``.values()`` method,
        except that it returns a list, not an iterator.

        """
        return super(RowProxy, self).values()


try:
    # Register RowProxy with Sequence,
    # so sequence protocol is implemented
    util.collections_abc.Sequence.register(RowProxy)
except ImportError:
    pass


class ResultMetaData(object):
    """Handle cursor.description, applying additional info from an execution
    context."""

    __slots__ = (
        "_keymap",
        "case_sensitive",
        "matched_on_name",
        "_processors",
        "keys",
        "_orig_processors",
    )

    def __init__(self, parent, cursor_description):
        context = parent.context
        dialect = context.dialect
        self.case_sensitive = dialect.case_sensitive
        self.matched_on_name = False
        self._orig_processors = None

        if context.result_column_struct:
            (
                result_columns,
                cols_are_ordered,
                textual_ordered,
            ) = context.result_column_struct
            num_ctx_cols = len(result_columns)
        else:
            result_columns = (
                cols_are_ordered
            ) = num_ctx_cols = textual_ordered = False

        # merge cursor.description with the column info
        # present in the compiled structure, if any
        raw = self._merge_cursor_description(
            context,
            cursor_description,
            result_columns,
            num_ctx_cols,
            cols_are_ordered,
            textual_ordered,
        )

        self._keymap = {}
        if not _baserowproxy_usecext:
            # keymap indexes by integer index: this is only used
            # in the pure Python BaseRowProxy.__getitem__
            # implementation to avoid an expensive
            # isinstance(key, util.int_types) in the most common
            # case path

            len_raw = len(raw)

            self._keymap.update(
                [(elem[0], (elem[3], elem[4], elem[0])) for elem in raw]
                + [
                    (elem[0] - len_raw, (elem[3], elem[4], elem[0]))
                    for elem in raw
                ]
            )

        # processors in key order for certain per-row
        # views like __iter__ and slices
        self._processors = [elem[3] for elem in raw]

        # keymap by primary string...
        by_key = dict([(elem[2], (elem[3], elem[4], elem[0])) for elem in raw])

        # for compiled SQL constructs, copy additional lookup keys into
        # the key lookup map, such as Column objects, labels,
        # column keys and other names
        if num_ctx_cols:

            # if by-primary-string dictionary smaller (or bigger?!) than
            # number of columns, assume we have dupes, rewrite
            # dupe records with "None" for index which results in
            # ambiguous column exception when accessed.
            if len(by_key) != num_ctx_cols:
                seen = set()
                for rec in raw:
                    key = rec[1]
                    if key in seen:
                        # this is an "ambiguous" element, replacing
                        # the full record in the map
                        key = key.lower() if not self.case_sensitive else key
                        by_key[key] = (None, key, None)
                    seen.add(key)

                # copy secondary elements from compiled columns
                # into self._keymap, write in the potentially "ambiguous"
                # element
                self._keymap.update(
                    [
                        (obj_elem, by_key[elem[2]])
                        for elem in raw
                        if elem[4]
                        for obj_elem in elem[4]
                    ]
                )

                # if we did a pure positional match, then reset the
                # original "expression element" back to the "unambiguous"
                # entry.  This is a new behavior in 1.1 which impacts
                # TextAsFrom but also straight compiled SQL constructs.
                if not self.matched_on_name:
                    self._keymap.update(
                        [
                            (elem[4][0], (elem[3], elem[4], elem[0]))
                            for elem in raw
                            if elem[4]
                        ]
                    )
            else:
                # no dupes - copy secondary elements from compiled
                # columns into self._keymap
                self._keymap.update(
                    [
                        (obj_elem, (elem[3], elem[4], elem[0]))
                        for elem in raw
                        if elem[4]
                        for obj_elem in elem[4]
                    ]
                )

        # update keymap with primary string names taking
        # precedence
        self._keymap.update(by_key)

        # update keymap with "translated" names (sqlite-only thing)
        if not num_ctx_cols and context._translate_colname:
            self._keymap.update(
                [(elem[5], self._keymap[elem[2]]) for elem in raw if elem[5]]
            )

    def _merge_cursor_description(
        self,
        context,
        cursor_description,
        result_columns,
        num_ctx_cols,
        cols_are_ordered,
        textual_ordered,
    ):
        """Merge a cursor.description with compiled result column information.

        There are at least four separate strategies used here, selected
        depending on the type of SQL construct used to start with.

        The most common case is that of the compiled SQL expression construct,
        which generated the column names present in the raw SQL string and
        which has the identical number of columns as were reported by
        cursor.description.  In this case, we assume a 1-1 positional mapping
        between the entries in cursor.description and the compiled object.
        This is also the most performant case as we disregard extracting /
        decoding the column names present in cursor.description since we
        already have the desired name we generated in the compiled SQL
        construct.

        The next common case is that of the completely raw string SQL,
        such as passed to connection.execute().  In this case we have no
        compiled construct to work with, so we extract and decode the
        names from cursor.description and index those as the primary
        result row target keys.

        The remaining fairly common case is that of the textual SQL
        that includes at least partial column information; this is when
        we use a :class:`.TextAsFrom` construct.   This construct may have
        unordered or ordered column information.  In the ordered case, we
        merge the cursor.description and the compiled construct's information
        positionally, and warn if there are additional description names
        present, however we still decode the names in cursor.description
        as we don't have a guarantee that the names in the columns match
        on these.   In the unordered case, we match names in cursor.description
        to that of the compiled construct based on name matching.
        In both of these cases, the cursor.description names and the column
        expression objects and names are indexed as result row target keys.

        The final case is much less common, where we have a compiled
        non-textual SQL expression construct, but the number of columns
        in cursor.description doesn't match what's in the compiled
        construct.  We make the guess here that there might be textual
        column expressions in the compiled construct that themselves include
        a comma in them causing them to split.  We do the same name-matching
        as with textual non-ordered columns.

        The name-matched system of merging is the same as that used by
        SQLAlchemy for all cases up through te 0.9 series.   Positional
        matching for compiled SQL expressions was introduced in 1.0 as a
        major performance feature, and positional matching for textual
        :class:`.TextAsFrom` objects in 1.1.  As name matching is no longer
        a common case, it was acceptable to factor it into smaller generator-
        oriented methods that are easier to understand, but incur slightly
        more performance overhead.

        """

        case_sensitive = context.dialect.case_sensitive

        if (
            num_ctx_cols
            and cols_are_ordered
            and not textual_ordered
            and num_ctx_cols == len(cursor_description)
        ):
            self.keys = [elem[0] for elem in result_columns]
            # pure positional 1-1 case; doesn't need to read
            # the names from cursor.description
            return [
                (
                    idx,
                    key,
                    name.lower() if not case_sensitive else name,
                    context.get_result_processor(
                        type_, key, cursor_description[idx][1]
                    ),
                    obj,
                    None,
                )
                for idx, (key, name, obj, type_) in enumerate(result_columns)
            ]
        else:
            # name-based or text-positional cases, where we need
            # to read cursor.description names
            if textual_ordered:
                # textual positional case
                raw_iterator = self._merge_textual_cols_by_position(
                    context, cursor_description, result_columns
                )
            elif num_ctx_cols:
                # compiled SQL with a mismatch of description cols
                # vs. compiled cols, or textual w/ unordered columns
                raw_iterator = self._merge_cols_by_name(
                    context, cursor_description, result_columns
                )
            else:
                # no compiled SQL, just a raw string
                raw_iterator = self._merge_cols_by_none(
                    context, cursor_description
                )

            return [
                (
                    idx,
                    colname,
                    colname,
                    context.get_result_processor(
                        mapped_type, colname, coltype
                    ),
                    obj,
                    untranslated,
                )
                for (
                    idx,
                    colname,
                    mapped_type,
                    coltype,
                    obj,
                    untranslated,
                ) in raw_iterator
            ]

    def _colnames_from_description(self, context, cursor_description):
        """Extract column names and data types from a cursor.description.

        Applies unicode decoding, column translation, "normalization",
        and case sensitivity rules to the names based on the dialect.

        """

        dialect = context.dialect
        case_sensitive = dialect.case_sensitive
        translate_colname = context._translate_colname
        description_decoder = (
            dialect._description_decoder
            if dialect.description_encoding
            else None
        )
        normalize_name = (
            dialect.normalize_name if dialect.requires_name_normalize else None
        )
        untranslated = None

        self.keys = []

        for idx, rec in enumerate(cursor_description):
            colname = rec[0]
            coltype = rec[1]

            if description_decoder:
                colname = description_decoder(colname)

            if translate_colname:
                colname, untranslated = translate_colname(colname)

            if normalize_name:
                colname = normalize_name(colname)

            self.keys.append(colname)
            if not case_sensitive:
                colname = colname.lower()

            yield idx, colname, untranslated, coltype

    def _merge_textual_cols_by_position(
        self, context, cursor_description, result_columns
    ):
        num_ctx_cols = len(result_columns) if result_columns else None

        if num_ctx_cols > len(cursor_description):
            util.warn(
                "Number of columns in textual SQL (%d) is "
                "smaller than number of columns requested (%d)"
                % (num_ctx_cols, len(cursor_description))
            )
        seen = set()
        for (
            idx,
            colname,
            untranslated,
            coltype,
        ) in self._colnames_from_description(context, cursor_description):
            if idx < num_ctx_cols:
                ctx_rec = result_columns[idx]
                obj = ctx_rec[2]
                mapped_type = ctx_rec[3]
                if obj[0] in seen:
                    raise exc.InvalidRequestError(
                        "Duplicate column expression requested "
                        "in textual SQL: %r" % obj[0]
                    )
                seen.add(obj[0])
            else:
                mapped_type = sqltypes.NULLTYPE
                obj = None

            yield idx, colname, mapped_type, coltype, obj, untranslated

    def _merge_cols_by_name(self, context, cursor_description, result_columns):
        dialect = context.dialect
        case_sensitive = dialect.case_sensitive
        result_map = self._create_result_map(result_columns, case_sensitive)

        self.matched_on_name = True
        for (
            idx,
            colname,
            untranslated,
            coltype,
        ) in self._colnames_from_description(context, cursor_description):
            try:
                ctx_rec = result_map[colname]
            except KeyError:
                mapped_type = sqltypes.NULLTYPE
                obj = None
            else:
                obj = ctx_rec[1]
                mapped_type = ctx_rec[2]
            yield idx, colname, mapped_type, coltype, obj, untranslated

    def _merge_cols_by_none(self, context, cursor_description):
        for (
            idx,
            colname,
            untranslated,
            coltype,
        ) in self._colnames_from_description(context, cursor_description):
            yield idx, colname, sqltypes.NULLTYPE, coltype, None, untranslated

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

    def _key_fallback(self, key, err, raiseerr=True):
        map_ = self._keymap
        result = None
        if isinstance(key, util.string_types):
            result = map_.get(key if self.case_sensitive else key.lower())
        # fallback for targeting a ColumnElement to a textual expression
        # this is a rare use case which only occurs when matching text()
        # or colummn('name') constructs to ColumnElements, or after a
        # pickle/unpickle roundtrip
        elif isinstance(key, expression.ColumnElement):
            if (
                key._label
                and (key._label if self.case_sensitive else key._label.lower())
                in map_
            ):
                result = map_[
                    key._label if self.case_sensitive else key._label.lower()
                ]
            elif (
                hasattr(key, "name")
                and (key.name if self.case_sensitive else key.name.lower())
                in map_
            ):
                # match is only on name.
                result = map_[
                    key.name if self.case_sensitive else key.name.lower()
                ]
            # search extra hard to make sure this
            # isn't a column/label name overlap.
            # this check isn't currently available if the row
            # was unpickled.
            if result is not None and result[1] is not None:
                for obj in result[1]:
                    if key._compare_name_for_result(obj):
                        break
                else:
                    result = None
        if result is None:
            if raiseerr:
                util.raise_(
                    exc.NoSuchColumnError(
                        "Could not locate column in row for column '%s'"
                        % expression._string_or_unprintable(key)
                    ),
                    replace_context=err,
                )
            else:
                return None
        else:
            map_[key] = result
        return result

    def _has_key(self, key):
        if key in self._keymap:
            return True
        else:
            return self._key_fallback(key, None, False) is not None

    def _getter(self, key, raiseerr=True):
        if key in self._keymap:
            processor, obj, index = self._keymap[key]
        else:
            ret = self._key_fallback(key, None, raiseerr)
            if ret is None:
                return None
            processor, obj, index = ret

        if index is None:
            util.raise_(
                exc.InvalidRequestError(
                    "Ambiguous column name '%s' in "
                    "result set column descriptions" % obj
                ),
                from_=None,
            )

        return operator.itemgetter(index)

    def __getstate__(self):
        return {
            "_pickled_keymap": dict(
                (key, index)
                for key, (processor, obj, index) in self._keymap.items()
                if isinstance(key, util.string_types + util.int_types)
            ),
            "keys": self.keys,
            "case_sensitive": self.case_sensitive,
            "matched_on_name": self.matched_on_name,
        }

    def __setstate__(self, state):
        # the row has been processed at pickling time so we don't need any
        # processor anymore
        self._processors = [None for _ in range(len(state["keys"]))]
        self._keymap = keymap = {}
        for key, index in state["_pickled_keymap"].items():
            # not preserving "obj" here, unfortunately our
            # proxy comparison fails with the unpickle
            keymap[key] = (None, None, index)
        self.keys = state["keys"]
        self.case_sensitive = state["case_sensitive"]
        self.matched_on_name = state["matched_on_name"]


class ResultProxy(object):
    """A facade around a DBAPI cursor object.

    Returns database rows via the :class:`.RowProxy` class, which provides
    additional API features and behaviors on top of the raw data returned
    by the DBAPI.

    .. seealso::

        :ref:`coretutorial_selecting` - introductory material for accessing
        :class:`_engine.ResultProxy` and :class:`.RowProxy` objects.

    """

    _process_row = RowProxy
    out_parameters = None
    _autoclose_connection = False
    _metadata = None
    _soft_closed = False
    closed = False

    def __init__(self, context):
        self.context = context
        self.dialect = context.dialect
        self.cursor = self._saved_cursor = context.cursor
        self.connection = context.root_connection
        self._echo = (
            self.connection._echo and context.engine._should_log_debug()
        )
        self._init_metadata()

    def _getter(self, key, raiseerr=True):
        try:
            getter = self._metadata._getter
        except AttributeError as err:
            return self._non_result(None, err)
        else:
            return getter(key, raiseerr)

    def _has_key(self, key):
        try:
            has_key = self._metadata._has_key
        except AttributeError as err:
            return self._non_result(None, err)
        else:
            return has_key(key)

    def _init_metadata(self):
        cursor_description = self._cursor_description()
        if cursor_description is not None:
            if (
                self.context.compiled
                and "compiled_cache" in self.context.execution_options
            ):
                if self.context.compiled._cached_metadata:
                    self._metadata = self.context.compiled._cached_metadata
                else:
                    self._metadata = (
                        self.context.compiled._cached_metadata
                    ) = ResultMetaData(self, cursor_description)
            else:
                self._metadata = ResultMetaData(self, cursor_description)
            if self._echo:
                self.context.engine.logger.debug(
                    "Col %r", tuple(x[0] for x in cursor_description)
                )

    def keys(self):
        """Return the list of string keys that would represented by each
        :class:`.RowProxy`."""

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

           Notes regarding :attr:`_engine.ResultProxy.rowcount`:


           * This attribute returns the number of rows *matched*,
             which is not necessarily the same as the number of rows
             that were actually *modified* - an UPDATE statement, for example,
             may have no net change on a given row if the SET values
             given are the same as those present in the row already.
             Such a row would be matched but not modified.
             On backends that feature both styles, such as MySQL,
             rowcount is configured by default to return the match
             count in all cases.

           * :attr:`_engine.ResultProxy.rowcount`
             is *only* useful in conjunction
             with an UPDATE or DELETE statement.  Contrary to what the Python
             DBAPI says, it does *not* return the
             number of rows available from the results of a SELECT statement
             as DBAPIs cannot support this functionality when rows are
             unbuffered.

           * :attr:`_engine.ResultProxy.rowcount`
             may not be fully implemented by
             all dialects.  In particular, most DBAPIs do not support an
             aggregate rowcount result from an executemany call.
             The :meth:`_engine.ResultProxy.supports_sane_rowcount` and
             :meth:`_engine.ResultProxy.supports_sane_multi_rowcount` methods
             will report from the dialect if each usage is known to be
             supported.

           * Statements that use RETURNING may not return a correct
             rowcount.

        """
        try:
            return self.context.rowcount
        except BaseException as e:
            self.connection._handle_dbapi_exception(
                e, None, None, self.cursor, self.context
            )

    @property
    def lastrowid(self):
        """Return the 'lastrowid' accessor on the DBAPI cursor.

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
        except BaseException as e:
            self.connection._handle_dbapi_exception(
                e, None, None, self._saved_cursor, self.context
            )

    @property
    def returns_rows(self):
        """True if this :class:`_engine.ResultProxy` returns rows.

        I.e. if it is legal to call the methods
        :meth:`_engine.ResultProxy.fetchone`,
        :meth:`_engine.ResultProxy.fetchmany`
        :meth:`_engine.ResultProxy.fetchall`.

        """
        return self._metadata is not None

    @property
    def is_insert(self):
        """True if this :class:`_engine.ResultProxy` is the result
        of a executing an expression language compiled
        :func:`_expression.insert` construct.

        When True, this implies that the
        :attr:`inserted_primary_key` attribute is accessible,
        assuming the statement did not include
        a user defined "returning" construct.

        """
        return self.context.isinsert

    def _cursor_description(self):
        """May be overridden by subclasses."""

        return self._saved_cursor.description

    def _soft_close(self):
        """Soft close this :class:`_engine.ResultProxy`.

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

            :meth:`_engine.ResultProxy.close`


        """
        if self._soft_closed:
            return
        self._soft_closed = True
        cursor = self.cursor
        self.connection._safe_close_cursor(cursor)
        if self._autoclose_connection:
            self.connection.close()
        self.cursor = None

    def close(self):
        """Close this ResultProxy.

        This closes out the underlying DBAPI cursor corresponding
        to the statement execution, if one is still present.  Note that the
        DBAPI cursor is automatically released when the
        :class:`_engine.ResultProxy`
        exhausts all available rows.  :meth:`_engine.ResultProxy.close`
        is generally
        an optional method except in the case when discarding a
        :class:`_engine.ResultProxy`
        that still has additional rows pending for fetch.

        In the case of a result that is the product of
        :ref:`connectionless execution <dbengine_implicit>`,
        the underlying :class:`_engine.Connection` object is also closed,
        which
        :term:`releases` DBAPI connection resources.

        After this method is called, it is no longer valid to call upon
        the fetch methods, which will raise a :class:`.ResourceClosedError`
        on subsequent use.

        .. versionchanged:: 1.0.0 - the :meth:`_engine.ResultProxy.close`
           method
           has been separated out from the process that releases the underlying
           DBAPI cursor resource.   The "auto close" feature of the
           :class:`_engine.Connection` now performs a so-called "soft close",
           which
           releases the underlying DBAPI cursor, but allows the
           :class:`_engine.ResultProxy`
           to still behave as an open-but-exhausted
           result set; the actual :meth:`_engine.ResultProxy.close`
           method is never
           called.    It is still safe to discard a
           :class:`_engine.ResultProxy`
           that has been fully exhausted without calling this method.

        .. seealso::

            :ref:`connections_toplevel`

        """

        if not self.closed:
            self._soft_close()
            self.closed = True

    def __iter__(self):
        """Implement iteration protocol."""

        while True:
            row = self.fetchone()
            if row is None:
                return
            else:
                yield row

    def __next__(self):
        """Implement the Python next() protocol.

        This method, mirrored as both ``.next()`` and  ``.__next__()``, is part
        of Python's API for producing iterator-like behavior.

        .. versionadded:: 1.2

        """
        row = self.fetchone()
        if row is None:
            raise StopIteration()
        else:
            return row

    next = __next__

    @util.memoized_property
    def inserted_primary_key(self):
        """Return the primary key for the row just inserted.

        The return value is a list of scalar values
        corresponding to the list of primary key columns
        in the target table.

        This only applies to single row :func:`_expression.insert`
        constructs which did not explicitly specify
        :meth:`_expression.Insert.returning`.

        Note that primary key columns which specify a
        server_default clause,
        or otherwise do not qualify as "autoincrement"
        columns (see the notes at :class:`_schema.Column`), and were
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
                "Statement is not a compiled " "expression construct."
            )
        elif not self.context.isinsert:
            raise exc.InvalidRequestError(
                "Statement is not an insert() " "expression construct."
            )
        elif self.context._is_explicit_returning:
            raise exc.InvalidRequestError(
                "Can't call inserted_primary_key "
                "when returning() "
                "is used."
            )

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
                "Statement is not a compiled " "expression construct."
            )
        elif not self.context.isupdate:
            raise exc.InvalidRequestError(
                "Statement is not an update() " "expression construct."
            )
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
                "Statement is not a compiled " "expression construct."
            )
        elif not self.context.isinsert:
            raise exc.InvalidRequestError(
                "Statement is not an insert() " "expression construct."
            )
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
                "Statement is not a compiled " "expression construct."
            )
        elif not self.context.isinsert and not self.context.isupdate:
            raise exc.InvalidRequestError(
                "Statement is not an insert() or update() "
                "expression construct."
            )
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
                "Statement is not a compiled " "expression construct."
            )
        elif not self.context.isinsert and not self.context.isupdate:
            raise exc.InvalidRequestError(
                "Statement is not an insert() or update() "
                "expression construct."
            )
        return self.context.prefetch_cols

    def supports_sane_rowcount(self):
        """Return ``supports_sane_rowcount`` from the dialect.

        See :attr:`_engine.ResultProxy.rowcount` for background.

        """

        return self.dialect.supports_sane_rowcount

    def supports_sane_multi_rowcount(self):
        """Return ``supports_sane_multi_rowcount`` from the dialect.

        See :attr:`_engine.ResultProxy.rowcount` for background.

        """

        return self.dialect.supports_sane_multi_rowcount

    def _fetchone_impl(self):
        try:
            return self.cursor.fetchone()
        except AttributeError as err:
            return self._non_result(None, err)

    def _fetchmany_impl(self, size=None):
        try:
            if size is None:
                return self.cursor.fetchmany()
            else:
                return self.cursor.fetchmany(size)
        except AttributeError as err:
            return self._non_result([], err)

    def _fetchall_impl(self):
        try:
            return self.cursor.fetchall()
        except AttributeError as err:
            return self._non_result([], err)

    def _non_result(self, default, err=None):
        if self._metadata is None:
            util.raise_(
                exc.ResourceClosedError(
                    "This result object does not return rows. "
                    "It has been closed automatically."
                ),
                replace_context=err,
            )
        elif self.closed:
            util.raise_(
                exc.ResourceClosedError("This result object is closed."),
                replace_context=err,
            )
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
                log("Row %r", sql_util._repr_row(row))
                l.append(process_row(metadata, row, processors, keymap))
            return l
        else:
            return [
                process_row(metadata, row, processors, keymap) for row in rows
            ]

    def fetchall(self):
        """Fetch all rows, just like DB-API ``cursor.fetchall()``.

        After all rows have been exhausted, the underlying DBAPI
        cursor resource is released, and the object may be safely
        discarded.

        Subsequent calls to :meth:`_engine.ResultProxy.fetchall` will return
        an empty list.   After the :meth:`_engine.ResultProxy.close` method is
        called, the method will raise :class:`.ResourceClosedError`.

        :return: a list of :class:`.RowProxy` objects

        """

        try:
            l = self.process_rows(self._fetchall_impl())
            self._soft_close()
            return l
        except BaseException as e:
            self.connection._handle_dbapi_exception(
                e, None, None, self.cursor, self.context
            )

    def fetchmany(self, size=None):
        """Fetch many rows, just like DB-API
        ``cursor.fetchmany(size=cursor.arraysize)``.

        After all rows have been exhausted, the underlying DBAPI
        cursor resource is released, and the object may be safely
        discarded.

        Calls to :meth:`_engine.ResultProxy.fetchmany`
        after all rows have been
        exhausted will return
        an empty list.   After the :meth:`_engine.ResultProxy.close` method is
        called, the method will raise :class:`.ResourceClosedError`.

        :return: a list of :class:`.RowProxy` objects

        """

        try:
            l = self.process_rows(self._fetchmany_impl(size))
            if len(l) == 0:
                self._soft_close()
            return l
        except BaseException as e:
            self.connection._handle_dbapi_exception(
                e, None, None, self.cursor, self.context
            )

    def fetchone(self):
        """Fetch one row, just like DB-API ``cursor.fetchone()``.

        After all rows have been exhausted, the underlying DBAPI
        cursor resource is released, and the object may be safely
        discarded.

        Calls to :meth:`_engine.ResultProxy.fetchone` after all rows have
        been exhausted will return ``None``.
        After the :meth:`_engine.ResultProxy.close` method is
        called, the method will raise :class:`.ResourceClosedError`.

        :return: a :class:`.RowProxy` object, or None if no rows remain

        """
        try:
            row = self._fetchone_impl()
            if row is not None:
                return self.process_rows([row])[0]
            else:
                self._soft_close()
                return None
        except BaseException as e:
            self.connection._handle_dbapi_exception(
                e, None, None, self.cursor, self.context
            )

    def first(self):
        """Fetch the first row and then close the result set unconditionally.

        After calling this method, the object is fully closed,
        e.g. the :meth:`_engine.ResultProxy.close`
        method will have been called.

        :return: a :class:`.RowProxy` object, or None if no rows remain

        """
        if self._metadata is None:
            return self._non_result(None)

        try:
            row = self._fetchone_impl()
        except BaseException as e:
            self.connection._handle_dbapi_exception(
                e, None, None, self.cursor, self.context
            )

        try:
            if row is not None:
                return self.process_rows([row])[0]
            else:
                return None
        finally:
            self.close()

    def scalar(self):
        """Fetch the first column of the first row, and close the result set.

        After calling this method, the object is fully closed,
        e.g. the :meth:`_engine.ResultProxy.close`
        method will have been called.

        :return: a Python scalar value , or None if no rows remain

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
            "max_row_buffer", None
        )
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
        500: 1000,
    }

    def __buffer_rows(self):
        if self.cursor is None:
            return
        size = getattr(self, "_bufsize", 1)
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
        super(BufferedColumnRow, self).__init__(
            parent, row, processors, keymap
        )


class BufferedColumnResultProxy(ResultProxy):
    """A ResultProxy with column buffering behavior.

    ``ResultProxy`` that loads all columns into memory each time
    fetchone() is called.  If fetchmany() or fetchall() are called,
    the full grid of results is fetched.  This is to operate with
    databases where result rows contain "live" results that fall out
    of scope unless explicitly fetched.

    .. versionchanged:: 1.2  This :class:`_engine.ResultProxy` is not used by
       any SQLAlchemy-included dialects.

    """

    _process_row = BufferedColumnRow

    def _init_metadata(self):
        super(BufferedColumnResultProxy, self)._init_metadata()

        metadata = self._metadata

        # don't double-replace the processors, in the case
        # of a cached ResultMetaData
        if metadata._orig_processors is None:
            # orig_processors will be used to preprocess each row when
            # they are constructed.
            metadata._orig_processors = metadata._processors
            # replace the all type processors by None processors.
            metadata._processors = [None for _ in range(len(metadata.keys))]
            keymap = {}
            for k, (func, obj, index) in metadata._keymap.items():
                keymap[k] = (None, obj, index)
            metadata._keymap = keymap

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
