# engine/result.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Define result set constructs including :class:`.Result`"""


import collections
import functools
import operator

from .row import _baserow_usecext
from .row import BaseRow  # noqa
from .row import LegacyRow  # noqa
from .row import Row  # noqa
from .row import RowMapping  # noqa
from .row import RowProxy  # noqa
from .row import rowproxy_reconstructor  # noqa
from .. import exc
from .. import util
from ..sql import expression
from ..sql import sqltypes
from ..sql import util as sql_util
from ..sql.compiler import RM_NAME
from ..sql.compiler import RM_OBJECTS
from ..sql.compiler import RM_RENDERED_NAME
from ..sql.compiler import RM_TYPE

if _baserow_usecext:
    from sqlalchemy.cresultproxy import tuplegetter as _tuplegetter

_UNPICKLED = util.symbol("unpickled")

# cyclical import for sqlalchemy.future
_future_Result = None

# metadata entry tuple indexes.
# using raw tuple is faster than namedtuple.
MD_INDEX = 0  # integer index in cursor.description
MD_OBJECTS = 1  # other string keys and ColumnElement obj that can match
MD_LOOKUP_KEY = 2  # string key we usually expect for key-based lookup
MD_RENDERED_NAME = 3  # name that is usually in cursor.description
MD_PROCESSOR = 4  # callable to process a result value into a row
MD_UNTRANSLATED = 5  # raw name from cursor.description


class ResultMetaData(object):
    __slots__ = ()

    def _has_key(self, key):
        return key in self._keymap

    def _key_fallback(self, key, err):
        if isinstance(key, int):
            util.raise_(IndexError(key), replace_context=err)
        else:
            util.raise_(KeyError(key), replace_context=err)


class SimpleResultMetaData(ResultMetaData):
    __slots__ = "keys", "_keymap", "_processors"

    def __init__(self, keys, extra=None):
        self.keys = list(keys)

        len_keys = len(keys)

        self._keymap = {
            name: (index, name) for index, name in enumerate(self.keys)
        }
        if not _baserow_usecext:
            self._keymap.update(
                {
                    index: (index, None, self.keys[index])
                    for index in range(len_keys)
                }
            )
        if extra:
            for key, ex in zip(keys, extra):
                rec = self._keymap[key]
                self._keymap.update({e: rec for e in ex})
        self._processors = [None] * len(keys)

    def __getstate__(self):
        return {"keys": self.keys}

    def __setstate__(self, state):
        self.__init__(state["keys"])

    def _has_key(self, key):
        return key in self._keymap

    def _contains(self, value, row):
        return value in row._data


def result_tuple(fields, extra=None):
    parent = SimpleResultMetaData(fields, extra)
    return functools.partial(Row, parent, parent._processors, parent._keymap)


class CursorResultMetaData(ResultMetaData):
    """Handle cursor.description, applying additional info from an execution
    context."""

    __slots__ = (
        "_keymap",
        "case_sensitive",
        "matched_on_name",
        "_processors",
        "keys",
    )

    def __init__(self, parent, cursor_description):
        context = parent.context
        dialect = context.dialect
        self.case_sensitive = dialect.case_sensitive
        self.matched_on_name = False

        if context.result_column_struct:
            (
                result_columns,
                cols_are_ordered,
                textual_ordered,
                loose_column_name_matching,
            ) = context.result_column_struct
            num_ctx_cols = len(result_columns)
        else:
            result_columns = (
                cols_are_ordered
            ) = (
                num_ctx_cols
            ) = loose_column_name_matching = textual_ordered = False

        # merge cursor.description with the column info
        # present in the compiled structure, if any
        raw = self._merge_cursor_description(
            context,
            cursor_description,
            result_columns,
            num_ctx_cols,
            cols_are_ordered,
            textual_ordered,
            loose_column_name_matching,
        )

        self._keymap = {}
        if not _baserow_usecext:
            # keymap indexes by integer index: this is only used
            # in the pure Python BaseRow.__getitem__
            # implementation to avoid an expensive
            # isinstance(key, util.int_types) in the most common
            # case path

            len_raw = len(raw)

            self._keymap.update(
                [
                    (metadata_entry[MD_INDEX], metadata_entry)
                    for metadata_entry in raw
                ]
                + [
                    (metadata_entry[MD_INDEX] - len_raw, metadata_entry)
                    for metadata_entry in raw
                ]
            )

        # processors in key order for certain per-row
        # views like __iter__ and slices
        self._processors = [
            metadata_entry[MD_PROCESSOR] for metadata_entry in raw
        ]

        # keymap by primary string...
        by_key = dict(
            [
                (metadata_entry[MD_LOOKUP_KEY], metadata_entry)
                for metadata_entry in raw
            ]
        )

        # for compiled SQL constructs, copy additional lookup keys into
        # the key lookup map, such as Column objects, labels,
        # column keys and other names
        if num_ctx_cols:

            # if by-primary-string dictionary smaller (or bigger?!) than
            # number of columns, assume we have dupes, rewrite
            # dupe records with "None" for index which results in
            # ambiguous column exception when accessed.
            if len(by_key) != num_ctx_cols:
                # new in 1.4: get the complete set of all possible keys,
                # strings, objects, whatever, that are dupes across two
                # different records, first.
                index_by_key = {}
                dupes = set()
                for metadata_entry in raw:
                    for key in (metadata_entry[MD_RENDERED_NAME],) + (
                        metadata_entry[MD_OBJECTS] or ()
                    ):
                        if not self.case_sensitive and isinstance(
                            key, util.string_types
                        ):
                            key = key.lower()
                        idx = metadata_entry[MD_INDEX]
                        # if this key has been associated with more than one
                        # positional index, it's a dupe
                        if index_by_key.setdefault(key, idx) != idx:
                            dupes.add(key)

                # then put everything we have into the keymap excluding only
                # those keys that are dupes.
                self._keymap.update(
                    [
                        (obj_elem, metadata_entry)
                        for metadata_entry in raw
                        if metadata_entry[MD_OBJECTS]
                        for obj_elem in metadata_entry[MD_OBJECTS]
                        if obj_elem not in dupes
                    ]
                )

                # then for the dupe keys, put the "ambiguous column"
                # record into by_key.
                by_key.update({key: (None, (), key) for key in dupes})

            else:
                # no dupes - copy secondary elements from compiled
                # columns into self._keymap
                self._keymap.update(
                    [
                        (obj_elem, metadata_entry)
                        for metadata_entry in raw
                        if metadata_entry[MD_OBJECTS]
                        for obj_elem in metadata_entry[MD_OBJECTS]
                    ]
                )

        # update keymap with primary string names taking
        # precedence
        self._keymap.update(by_key)

        # update keymap with "translated" names (sqlite-only thing)
        if not num_ctx_cols and context._translate_colname:
            self._keymap.update(
                [
                    (
                        metadata_entry[MD_UNTRANSLATED],
                        self._keymap[metadata_entry[MD_LOOKUP_KEY]],
                    )
                    for metadata_entry in raw
                    if metadata_entry[MD_UNTRANSLATED]
                ]
            )

    def _merge_cursor_description(
        self,
        context,
        cursor_description,
        result_columns,
        num_ctx_cols,
        cols_are_ordered,
        textual_ordered,
        loose_column_name_matching,
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
        we use a :class:`.TextualSelect` construct.   This construct may have
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
        :class:`.TextualSelect` objects in 1.1.  As name matching is no longer
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
                    rmap_entry[RM_OBJECTS],
                    rmap_entry[RM_NAME].lower()
                    if not case_sensitive
                    else rmap_entry[RM_NAME],
                    rmap_entry[RM_RENDERED_NAME],
                    context.get_result_processor(
                        rmap_entry[RM_TYPE],
                        rmap_entry[RM_RENDERED_NAME],
                        cursor_description[idx][1],
                    ),
                    None,
                )
                for idx, rmap_entry in enumerate(result_columns)
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
                    context,
                    cursor_description,
                    result_columns,
                    loose_column_name_matching,
                )
            else:
                # no compiled SQL, just a raw string
                raw_iterator = self._merge_cols_by_none(
                    context, cursor_description
                )

            return [
                (
                    idx,
                    obj,
                    cursor_colname,
                    cursor_colname,
                    context.get_result_processor(
                        mapped_type, cursor_colname, coltype
                    ),
                    untranslated,
                )
                for (
                    idx,
                    cursor_colname,
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
                obj = ctx_rec[RM_OBJECTS]
                mapped_type = ctx_rec[RM_TYPE]
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

    def _merge_cols_by_name(
        self,
        context,
        cursor_description,
        result_columns,
        loose_column_name_matching,
    ):
        dialect = context.dialect
        case_sensitive = dialect.case_sensitive
        match_map = self._create_description_match_map(
            result_columns, case_sensitive, loose_column_name_matching
        )

        self.matched_on_name = True
        for (
            idx,
            colname,
            untranslated,
            coltype,
        ) in self._colnames_from_description(context, cursor_description):
            try:
                ctx_rec = match_map[colname]
            except KeyError:
                mapped_type = sqltypes.NULLTYPE
                obj = None
            else:
                obj = ctx_rec[1]
                mapped_type = ctx_rec[2]
            yield idx, colname, mapped_type, coltype, obj, untranslated

    @classmethod
    def _create_description_match_map(
        cls,
        result_columns,
        case_sensitive=True,
        loose_column_name_matching=False,
    ):
        """when matching cursor.description to a set of names that are present
        in a Compiled object, as is the case with TextualSelect, get all the
        names we expect might match those in cursor.description.
        """

        d = {}
        for elem in result_columns:
            key = elem[RM_RENDERED_NAME]

            if not case_sensitive:
                key = key.lower()
            if key in d:
                # conflicting keyname - just add the column-linked objects
                # to the existing record.  if there is a duplicate column
                # name in the cursor description, this will allow all of those
                # objects to raise an ambiguous column error
                e_name, e_obj, e_type = d[key]
                d[key] = e_name, e_obj + elem[RM_OBJECTS], e_type
            else:
                d[key] = (elem[RM_NAME], elem[RM_OBJECTS], elem[RM_TYPE])

            if loose_column_name_matching:
                # when using a textual statement with an unordered set
                # of columns that line up, we are expecting the user
                # to be using label names in the SQL that match to the column
                # expressions.  Enable more liberal matching for this case;
                # duplicate keys that are ambiguous will be fixed later.
                for r_key in elem[RM_OBJECTS]:
                    d.setdefault(
                        r_key, (elem[RM_NAME], elem[RM_OBJECTS], elem[RM_TYPE])
                    )

        return d

    def _merge_cols_by_none(self, context, cursor_description):
        for (
            idx,
            colname,
            untranslated,
            coltype,
        ) in self._colnames_from_description(context, cursor_description):
            yield idx, colname, sqltypes.NULLTYPE, coltype, None, untranslated

    def _key_fallback(self, key, err, raiseerr=True):
        if raiseerr:
            util.raise_(
                exc.NoSuchColumnError(
                    "Could not locate column in row for column '%s'"
                    % util.string_or_unprintable(key)
                ),
                replace_context=err,
            )
        else:
            return None

    def _raise_for_ambiguous_column_name(self, rec):
        raise exc.InvalidRequestError(
            "Ambiguous column name '%s' in "
            "result set column descriptions" % rec[MD_LOOKUP_KEY]
        )

    def _warn_for_nonint(self, key):
        raise TypeError(
            "TypeError: tuple indices must be integers or slices, not %s"
            % type(key).__name__
        )

    def _getter(self, key, raiseerr=True):
        try:
            rec = self._keymap[key]
        except KeyError as ke:
            rec = self._key_fallback(key, ke, raiseerr)
            if rec is None:
                return None

        index, obj = rec[0:2]

        if index is None:
            self._raise_for_ambiguous_column_name(rec)

        return operator.methodcaller("_get_by_key_impl_mapping", index)

    def _tuple_getter(self, keys, raiseerr=True):
        """Given a list of keys, return a callable that will deliver a tuple.

        This is strictly used by the ORM and the keys are Column objects.
        However, this might be some nice-ish feature if we could find a very
        clean way of presenting it.

        note that in the new world of "row._mapping", this is a mapping-getter.
        maybe the name should indicate that somehow.


        """
        indexes = []
        for key in keys:
            try:
                rec = self._keymap[key]
            except KeyError as ke:
                rec = self._key_fallback(key, ke, raiseerr)
                if rec is None:
                    return None

            index, obj = rec[0:2]

            if index is None:
                self._raise_for_ambiguous_column_name(obj)
            indexes.append(index)

        if _baserow_usecext:
            return _tuplegetter(*indexes)
        else:
            return self._pure_py_tuplegetter(*indexes)

    def _pure_py_tuplegetter(self, *indexes):
        getters = [
            operator.methodcaller("_get_by_key_impl_mapping", index)
            for index in indexes
        ]
        return lambda rec: tuple(getter(rec) for getter in getters)

    def __getstate__(self):
        return {
            "_keymap": {
                key: (rec[MD_INDEX], _UNPICKLED, key)
                for key, rec in self._keymap.items()
                if isinstance(key, util.string_types + util.int_types)
            },
            "keys": self.keys,
            "case_sensitive": self.case_sensitive,
            "matched_on_name": self.matched_on_name,
        }

    def __setstate__(self, state):
        self._processors = [None for _ in range(len(state["keys"]))]
        self._keymap = state["_keymap"]

        self.keys = state["keys"]
        self.case_sensitive = state["case_sensitive"]
        self.matched_on_name = state["matched_on_name"]


class LegacyCursorResultMetaData(CursorResultMetaData):
    def _contains(self, value, row):
        key = value
        if key in self._keymap:
            util.warn_deprecated(
                "Using the 'in' operator to test for string or column "
                "keys, or integer indexes, in a :class:`.Row` object is "
                "deprecated and will "
                "be removed in a future release. "
                "Use the `Row._fields` or `Row._mapping` attribute, i.e. "
                "'key in row._fields'"
            )
            return True
        else:
            return self._key_fallback(key, None, False) is not None

    def _key_fallback(self, key, err, raiseerr=True):
        map_ = self._keymap
        result = None

        if isinstance(key, util.string_types):
            result = map_.get(key if self.case_sensitive else key.lower())
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
            if result is not None and result[MD_OBJECTS] not in (
                None,
                _UNPICKLED,
            ):
                for obj in result[MD_OBJECTS]:
                    if key._compare_name_for_result(obj):
                        break
                else:
                    result = None
            if result is not None:
                if result[MD_OBJECTS] is _UNPICKLED:
                    util.warn_deprecated(
                        "Retreiving row values using Column objects from a "
                        "row that was unpickled is deprecated; adequate "
                        "state cannot be pickled for this to be efficient.   "
                        "This usage will raise KeyError in a future release."
                    )
                else:
                    util.warn_deprecated(
                        "Retreiving row values using Column objects with only "
                        "matching names as keys is deprecated, and will raise "
                        "KeyError in a future release; only Column "
                        "objects that are explicitly part of the statement "
                        "object should be used."
                    )
        if result is None:
            if raiseerr:
                util.raise_(
                    exc.NoSuchColumnError(
                        "Could not locate column in row for column '%s'"
                        % util.string_or_unprintable(key)
                    ),
                    replace_context=err,
                )
            else:
                return None
        else:
            map_[key] = result
        return result

    def _warn_for_nonint(self, key):
        util.warn_deprecated_20(
            "Using non-integer/slice indices on Row is deprecated and will "
            "be removed in version 2.0; please use row._mapping[<key>], or "
            "the mappings() accessor on the sqlalchemy.future result object.",
            stacklevel=4,
        )

    def _has_key(self, key):
        if key in self._keymap:
            return True
        else:
            return self._key_fallback(key, None, False) is not None


class CursorFetchStrategy(object):
    """Define a cursor strategy for a result object.

    Subclasses define different ways of fetching rows, typically but
    not necessarily using a DBAPI cursor object.

    .. versionadded:: 1.4

    """

    __slots__ = ("dbapi_cursor", "cursor_description")

    def __init__(self, dbapi_cursor, cursor_description):
        self.dbapi_cursor = dbapi_cursor
        self.cursor_description = cursor_description

    @classmethod
    def create(cls, result):
        raise NotImplementedError()

    def soft_close(self, result):
        raise NotImplementedError()

    def hard_close(self, result):
        raise NotImplementedError()

    def fetchone(self):
        raise NotImplementedError()

    def fetchmany(self, size=None):
        raise NotImplementedError()

    def fetchall(self):
        raise NotImplementedError()


class NoCursorDQLFetchStrategy(CursorFetchStrategy):
    """Cursor strategy for a DQL result that has no open cursor.

    This is a result set that can return rows, i.e. for a SELECT, or for an
    INSERT, UPDATE, DELETE that includes RETURNING. However it is in the state
    where the cursor is closed and no rows remain available.  The owning result
    object may or may not be "hard closed", which determines if the fetch
    methods send empty results or raise for closed result.

    """

    __slots__ = ("closed",)

    def __init__(self, closed):
        self.closed = closed
        self.cursor_description = None

    def soft_close(self, result):
        pass

    def hard_close(self, result):
        self.closed = True

    def fetchone(self):
        return self._non_result(None)

    def fetchmany(self, size=None):
        return self._non_result([])

    def fetchall(self):
        return self._non_result([])

    def _non_result(self, default, err=None):
        if self.closed:
            util.raise_(
                exc.ResourceClosedError("This result object is closed."),
                replace_context=err,
            )
        else:
            return default


class NoCursorDMLFetchStrategy(CursorFetchStrategy):
    """Cursor strategy for a DML result that has no open cursor.

    This is a result set that does not return rows, i.e. for an INSERT,
    UPDATE, DELETE that does not include RETURNING.

    """

    __slots__ = ("closed",)

    def __init__(self, closed):
        self.closed = closed
        self.cursor_description = None

    def soft_close(self, result):
        pass

    def hard_close(self, result):
        self.closed = True

    def fetchone(self):
        return self._non_result(None)

    def fetchmany(self, size=None):
        return self._non_result([])

    def fetchall(self):
        return self._non_result([])

    def _non_result(self, default, err=None):
        util.raise_(
            exc.ResourceClosedError(
                "This result object does not return rows. "
                "It has been closed automatically."
            ),
            replace_context=err,
        )


class DefaultCursorFetchStrategy(CursorFetchStrategy):
    """Call fetch methods from a DBAPI cursor.

    Alternate versions of this class may instead buffer the rows from
    cursors or not use cursors at all.

    """

    @classmethod
    def create(cls, result):
        dbapi_cursor = result.cursor
        description = dbapi_cursor.description

        if description is None:
            return NoCursorDMLFetchStrategy(False)
        else:
            return cls(dbapi_cursor, description)

    def soft_close(self, result):
        result.cursor_strategy = NoCursorDQLFetchStrategy(False)

    def hard_close(self, result):
        result.cursor_strategy = NoCursorDQLFetchStrategy(True)

    def fetchone(self):
        return self.dbapi_cursor.fetchone()

    def fetchmany(self, size=None):
        if size is None:
            return self.dbapi_cursor.fetchmany()
        else:
            return self.dbapi_cursor.fetchmany(size)

    def fetchall(self):
        return self.dbapi_cursor.fetchall()


class BufferedRowCursorFetchStrategy(DefaultCursorFetchStrategy):
    """A cursor fetch strategy with row buffering behavior.

    This strategy buffers the contents of a selection of rows
    before ``fetchone()`` is called.  This is to allow the results of
    ``cursor.description`` to be available immediately, when
    interfacing with a DB-API that requires rows to be consumed before
    this information is available (currently psycopg2, when used with
    server-side cursors).

    The pre-fetching behavior fetches only one row initially, and then
    grows its buffer size by a fixed amount with each successive need
    for additional rows up the ``max_row_buffer`` size, which defaults
    to 1000::

        with psycopg2_engine.connect() as conn:

            result = conn.execution_options(
                stream_results=True, max_row_buffer=50
                ).execute("select * from table")

    .. versionadded:: 1.4 ``max_row_buffer`` may now exceed 1000 rows.

    .. seealso::

        :ref:`psycopg2_execution_options`
    """

    __slots__ = ("_max_row_buffer", "_rowbuffer", "_bufsize")

    def __init__(
        self, max_row_buffer, dbapi_cursor, description, initial_buffer
    ):
        super(BufferedRowCursorFetchStrategy, self).__init__(
            dbapi_cursor, description
        )

        self._max_row_buffer = max_row_buffer
        self._growth_factor = 5
        self._rowbuffer = initial_buffer

        self._bufsize = min(self._max_row_buffer, self._growth_factor)

    @classmethod
    def create(cls, result):
        """Buffered row strategy has to buffer the first rows *before*
        cursor.description is fetched so that it works with named cursors
        correctly

        """

        dbapi_cursor = result.cursor

        initial_buffer = collections.deque(dbapi_cursor.fetchmany(1))

        description = dbapi_cursor.description

        if description is None:
            return NoCursorDMLFetchStrategy(False)
        else:
            max_row_buffer = result.context.execution_options.get(
                "max_row_buffer", 1000
            )
            return cls(
                max_row_buffer, dbapi_cursor, description, initial_buffer
            )

    def __buffer_rows(self):
        size = self._bufsize
        self._rowbuffer = collections.deque(self.dbapi_cursor.fetchmany(size))
        if size < self._max_row_buffer:
            self._bufsize = min(
                self._max_row_buffer, size * self._growth_factor
            )

    def soft_close(self, result):
        self._rowbuffer.clear()
        super(BufferedRowCursorFetchStrategy, self).soft_close(result)

    def hard_close(self, result):
        self._rowbuffer.clear()
        super(BufferedRowCursorFetchStrategy, self).hard_close(result)

    def fetchone(self):
        if not self._rowbuffer:
            self.__buffer_rows()
            if not self._rowbuffer:
                return None
        return self._rowbuffer.popleft()

    def fetchmany(self, size=None):
        if size is None:
            return self.fetchall()
        result = []
        for x in range(0, size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)
        return result

    def fetchall(self):
        self._rowbuffer.extend(self.dbapi_cursor.fetchall())
        ret = self._rowbuffer
        self._rowbuffer = collections.deque()
        return ret


class FullyBufferedCursorFetchStrategy(DefaultCursorFetchStrategy):
    """A cursor strategy that buffers rows fully upon creation.

    Used for operations where a result is to be delivered
    after the database conversation can not be continued,
    such as MSSQL INSERT...OUTPUT after an autocommit.

    """

    __slots__ = ("_rowbuffer",)

    def __init__(self, dbapi_cursor, description, initial_buffer=None):
        super(FullyBufferedCursorFetchStrategy, self).__init__(
            dbapi_cursor, description
        )
        if initial_buffer is not None:
            self._rowbuffer = collections.deque(initial_buffer)
        else:
            self._rowbuffer = self._buffer_rows()

    @classmethod
    def create_from_buffer(cls, dbapi_cursor, description, buffer):
        return cls(dbapi_cursor, description, buffer)

    def _buffer_rows(self):
        return collections.deque(self.dbapi_cursor.fetchall())

    def soft_close(self, result):
        self._rowbuffer.clear()
        super(FullyBufferedCursorFetchStrategy, self).soft_close(result)

    def hard_close(self, result):
        self._rowbuffer.clear()
        super(FullyBufferedCursorFetchStrategy, self).hard_close(result)

    def fetchone(self):
        if self._rowbuffer:
            return self._rowbuffer.popleft()
        else:
            return None

    def fetchmany(self, size=None):
        if size is None:
            return self.fetchall()
        result = []
        for x in range(0, size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)
        return result

    def fetchall(self):
        ret = self._rowbuffer
        self._rowbuffer = collections.deque()
        return ret


class BaseResult(object):
    """Base class for database result objects.


    :class:`.BaseResult` is the base class for the 1.x style
    :class:`.ResultProxy` class as well as the 2.x style
    :class:`.future.Result` class.

    """

    out_parameters = None
    _metadata = None
    _soft_closed = False
    closed = False

    @classmethod
    def _create_for_context(cls, context):
        if context._is_future_result:
            obj = object.__new__(_future_Result)
        else:
            obj = object.__new__(ResultProxy)
        obj.__init__(context)
        return obj

    def __init__(self, context):
        self.context = context
        self.dialect = context.dialect
        self.cursor = context.cursor
        self.connection = context.root_connection
        self._echo = (
            self.connection._echo and context.engine._should_log_debug()
        )
        self._init_metadata()

    def _init_metadata(self):
        self.cursor_strategy = strat = self.context.get_result_cursor_strategy(
            self
        )

        if strat.cursor_description is not None:
            if self.context.compiled:
                if self.context.compiled._cached_metadata:
                    self._metadata = self.context.compiled._cached_metadata
                else:
                    self._metadata = (
                        self.context.compiled._cached_metadata
                    ) = self._cursor_metadata(self, strat.cursor_description)
            else:
                self._metadata = self._cursor_metadata(
                    self, strat.cursor_description
                )
            if self._echo:
                self.context.engine.logger.debug(
                    "Col %r", tuple(x[0] for x in strat.cursor_description)
                )
        # leave cursor open so that execution context can continue
        # setting up things like rowcount

    def keys(self):
        """Return the list of string keys that would represented by each
        :class:`.Row`."""

        if self._metadata:
            return self._metadata.keys
        else:
            return []

    def _getter(self, key, raiseerr=True):
        try:
            getter = self._metadata._getter
        except AttributeError as err:
            return self.cursor_strategy._non_result(None, err)
        else:
            return getter(key, raiseerr)

    def _tuple_getter(self, key, raiseerr=True):
        try:
            getter = self._metadata._tuple_getter
        except AttributeError as err:
            return self.cursor_strategy._non_result(None, err)
        else:
            return getter(key, raiseerr)

    def _has_key(self, key):
        try:
            has_key = self._metadata._has_key
        except AttributeError as err:
            return self.cursor_strategy._non_result(None, err)
        else:
            return has_key(key)

    def _soft_close(self, hard=False):
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

        if (not hard and self._soft_closed) or (hard and self.closed):
            return

        if hard:
            self.closed = True
            self.cursor_strategy.hard_close(self)
        else:
            self.cursor_strategy.soft_close(self)

        if not self._soft_closed:
            cursor = self.cursor
            self.cursor = None
            self.connection._safe_close_cursor(cursor)
            self._soft_closed = True

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

        The value is an instance of :class:`.Row`, or ``None``
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

        See :attr:`.ResultProxy.rowcount` for background.

        """

        return self.dialect.supports_sane_rowcount

    def supports_sane_multi_rowcount(self):
        """Return ``supports_sane_multi_rowcount`` from the dialect.

        See :attr:`.ResultProxy.rowcount` for background.

        """

        return self.dialect.supports_sane_multi_rowcount

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
        except BaseException as e:
            self.connection._handle_dbapi_exception(
                e, None, None, self.cursor, self.context
            )

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
            return self.context.get_lastrowid()
        except BaseException as e:
            self.connection._handle_dbapi_exception(
                e, None, None, self.cursor, self.context
            )

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


class ResultProxy(BaseResult):
    """A facade around a DBAPI cursor object.

    Returns database rows via the :class:`.Row` class, which provides
    additional API features and behaviors on top of the raw data returned
    by the DBAPI.

    Within the scope of the 1.x series of SQLAlchemy, the :class:`.ResultProxy`
    will in fact return instances of the :class:`.LegacyRow` class, which
    maintains Python mapping (i.e. dictionary) like behaviors upon the object
    itself.  Going forward, the :attr:`.Row._mapping` attribute should be used
    for dictionary behaviors.

    .. seealso::

        :ref:`coretutorial_selecting` - introductory material for accessing
        :class:`.ResultProxy` and :class:`.Row` objects.

    """

    _autoclose_connection = False
    _process_row = LegacyRow
    _cursor_metadata = LegacyCursorResultMetaData
    _cursor_strategy_cls = DefaultCursorFetchStrategy

    def __iter__(self):
        """Implement iteration protocol."""

        while True:
            row = self.fetchone()
            if row is None:
                return
            else:
                yield row

    def close(self):
        """Close this ResultProxy.

        This closes out the underlying DBAPI cursor corresponding
        to the statement execution, if one is still present.  Note that the
        DBAPI cursor is automatically released when the :class:`.ResultProxy`
        exhausts all available rows.  :meth:`.ResultProxy.close` is generally
        an optional method except in the case when discarding a
        :class:`.ResultProxy` that still has additional rows pending for fetch.

        In the case of a result that is the product of
        :ref:`connectionless execution <dbengine_implicit>`,
        the underlying :class:`.Connection` object is also closed, which
        :term:`releases` DBAPI connection resources.

        .. deprecated:: 2.0 "connectionless" execution is deprecated and will
           be removed in version 2.0.   Version 2.0 will feature the
           :class:`.Result` object that will no longer affect the status
           of the originating connection in any case.

        After this method is called, it is no longer valid to call upon
        the fetch methods, which will raise a :class:`.ResourceClosedError`
        on subsequent use.

        .. seealso::

            :ref:`connections_toplevel`

        """
        self._soft_close(hard=True)

    def _soft_close(self, hard=False):
        soft_closed = self._soft_closed
        super(ResultProxy, self)._soft_close(hard=hard)
        if (
            not soft_closed
            and self._soft_closed
            and self._autoclose_connection
        ):
            self.connection.close()

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
                l.append(process_row(metadata, processors, keymap, row))
            return l
        else:
            return [
                process_row(metadata, processors, keymap, row) for row in rows
            ]

    def fetchall(self):
        """Fetch all rows, just like DB-API ``cursor.fetchall()``.

        After all rows have been exhausted, the underlying DBAPI
        cursor resource is released, and the object may be safely
        discarded.

        Subsequent calls to :meth:`.ResultProxy.fetchall` will return
        an empty list.   After the :meth:`.ResultProxy.close` method is
        called, the method will raise :class:`.ResourceClosedError`.

        :return: a list of :class:`.Row` objects

        """

        try:
            l = self.process_rows(self.cursor_strategy.fetchall())
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

        Calls to :meth:`.ResultProxy.fetchmany` after all rows have been
        exhausted will return
        an empty list.   After the :meth:`.ResultProxy.close` method is
        called, the method will raise :class:`.ResourceClosedError`.

        :return: a list of :class:`.Row` objects

        """

        try:
            l = self.process_rows(self.cursor_strategy.fetchmany(size))
            if len(l) == 0:
                self._soft_close()
            return l
        except BaseException as e:
            self.connection._handle_dbapi_exception(
                e, None, None, self.cursor, self.context
            )

    def _onerow(self):
        return self.fetchone()

    def fetchone(self):
        """Fetch one row, just like DB-API ``cursor.fetchone()``.

        After all rows have been exhausted, the underlying DBAPI
        cursor resource is released, and the object may be safely
        discarded.

        Calls to :meth:`.ResultProxy.fetchone` after all rows have
        been exhausted will return ``None``.
        After the :meth:`.ResultProxy.close` method is
        called, the method will raise :class:`.ResourceClosedError`.

        :return: a :class:`.Row` object, or None if no rows remain

        """
        try:
            row = self.cursor_strategy.fetchone()
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
        e.g. the :meth:`.ResultProxy.close` method will have been called.

        :return: a :class:`.Row` object, or None if no rows remain

        """
        try:
            row = self.cursor_strategy.fetchone()
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
        e.g. the :meth:`.ResultProxy.close` method will have been called.

        :return: a Python scalar value , or None if no rows remain

        """
        row = self.first()
        if row is not None:
            return row[0]
        else:
            return None


class BufferedRowResultProxy(ResultProxy):
    """A ResultProxy with row buffering behavior.

    .. deprecated::  1.4 this class is now supplied using a strategy object.
       See :class:`.BufferedRowCursorFetchStrategy`.

    """

    _cursor_strategy_cls = BufferedRowCursorFetchStrategy


class FullyBufferedResultProxy(ResultProxy):
    """A result proxy that buffers rows fully upon creation.

    .. deprecated::  1.4 this class is now supplied using a strategy object.
       See :class:`.FullyBufferedCursorFetchStrategy`.

    """

    _cursor_strategy_cls = FullyBufferedCursorFetchStrategy


class BufferedColumnRow(LegacyRow):
    """Row is now BufferedColumn in all cases"""


class BufferedColumnResultProxy(ResultProxy):
    """A ResultProxy with column buffering behavior.

    .. versionchanged:: 1.4   This is now the default behavior of the Row
       and this class does not change behavior in any way.

    """

    _process_row = BufferedColumnRow
