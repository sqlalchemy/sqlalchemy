# engine/cursor.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Define cursor-specific result set constructs including
:class:`.BaseCursorResult`, :class:`.CursorResult`."""


import collections

from .result import Result
from .result import ResultMetaData
from .result import SimpleResultMetaData
from .result import tuplegetter
from .row import _baserow_usecext
from .row import LegacyRow
from .. import exc
from .. import util
from ..sql import expression
from ..sql import sqltypes
from ..sql import util as sql_util
from ..sql.base import _generative
from ..sql.base import HasMemoized
from ..sql.compiler import RM_NAME
from ..sql.compiler import RM_OBJECTS
from ..sql.compiler import RM_RENDERED_NAME
from ..sql.compiler import RM_TYPE

_UNPICKLED = util.symbol("unpickled")


# metadata entry tuple indexes.
# using raw tuple is faster than namedtuple.
MD_INDEX = 0  # integer index in cursor.description
MD_OBJECTS = 1  # other string keys and ColumnElement obj that can match
MD_LOOKUP_KEY = 2  # string key we usually expect for key-based lookup
MD_RENDERED_NAME = 3  # name that is usually in cursor.description
MD_PROCESSOR = 4  # callable to process a result value into a row
MD_UNTRANSLATED = 5  # raw name from cursor.description


class CursorResultMetaData(ResultMetaData):
    """Result metadata for DBAPI cursors."""

    __slots__ = (
        "_keymap",
        "case_sensitive",
        "_processors",
        "_keys",
        "_tuplefilter",
        "_translated_indexes",
        # don't need _unique_filters support here for now.  Can be added
        # if a need arises.
    )

    returns_rows = True

    def _for_freeze(self):
        return SimpleResultMetaData(
            self._keys,
            extra=[self._keymap[key][MD_OBJECTS] for key in self._keys],
        )

    def _reduce(self, keys):
        recs = list(self._metadata_for_keys(keys))

        indexes = [rec[MD_INDEX] for rec in recs]
        new_keys = [rec[MD_LOOKUP_KEY] for rec in recs]

        if self._translated_indexes:
            indexes = [self._translated_indexes[idx] for idx in indexes]

        tup = tuplegetter(*indexes)

        new_metadata = self.__class__.__new__(self.__class__)
        new_metadata.case_sensitive = self.case_sensitive
        new_metadata._processors = self._processors
        new_metadata._keys = new_keys
        new_metadata._tuplefilter = tup
        new_metadata._translated_indexes = indexes

        new_recs = [
            (index,) + rec[1:]
            for index, rec in enumerate(self._metadata_for_keys(keys))
        ]
        new_metadata._keymap = {rec[MD_LOOKUP_KEY]: rec for rec in new_recs}
        if not _baserow_usecext:
            # TODO: can consider assembling ints + negative ints here
            new_metadata._keymap.update(
                {
                    index: (index, new_keys[index], ())
                    for index in range(len(new_keys))
                }
            )

        new_metadata._keymap.update(
            {e: new_rec for new_rec in new_recs for e in new_rec[MD_OBJECTS]}
        )

        return new_metadata

    def _adapt_to_context(self, context):
        """When using a cached result metadata against a new context,
        we need to rewrite the _keymap so that it has the specific
        Column objects in the new context inside of it.  this accommodates
        for select() constructs that contain anonymized columns and
        are cached.

        """
        if not context.compiled._result_columns:
            return self

        compiled_statement = context.compiled.statement
        invoked_statement = context.invoked_statement

        # same statement was invoked as the one we cached against,
        # return self
        if compiled_statement is invoked_statement:
            return self

        # make a copy and add the columns from the invoked statement
        # to the result map.
        md = self.__class__.__new__(self.__class__)

        md._keymap = self._keymap.copy()

        # match up new columns positionally to the result columns
        for existing, new in zip(
            context.compiled._result_columns,
            invoked_statement._exported_columns_iterator(),
        ):
            md._keymap[new] = md._keymap[existing[RM_NAME]]

        md.case_sensitive = self.case_sensitive
        md._processors = self._processors
        assert not self._tuplefilter
        md._tuplefilter = None
        md._translated_indexes = None
        md._keys = self._keys
        return md

    def __init__(self, parent, cursor_description):
        context = parent.context
        dialect = context.dialect
        self._tuplefilter = None
        self._translated_indexes = None
        self.case_sensitive = dialect.case_sensitive

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
        we use a :class:`_expression.TextualSelect` construct.
        This construct may have
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
        :class:`_expression.TextualSelect` objects in 1.1.
        As name matching is no longer
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
            self._keys = [elem[0] for elem in result_columns]
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

        self._keys = []

        for idx, rec in enumerate(cursor_description):
            colname = rec[0]
            coltype = rec[1]

            if description_decoder:
                colname = description_decoder(colname)

            if translate_colname:
                colname, untranslated = translate_colname(colname)

            if normalize_name:
                colname = normalize_name(colname)

            self._keys.append(colname)
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

    def _index_for_key(self, key, raiseerr=True):
        # TODO: can consider pre-loading ints and negative ints
        # into _keymap - also no coverage here
        if isinstance(key, int):
            key = self._keys[key]

        try:
            rec = self._keymap[key]
        except KeyError as ke:
            rec = self._key_fallback(key, ke, raiseerr)
            if rec is None:
                return None

        index = rec[0]

        if index is None:
            self._raise_for_ambiguous_column_name(rec)
        return index

    def _indexes_for_keys(self, keys):
        for rec in self._metadata_for_keys(keys):
            yield rec[0]

    def _metadata_for_keys(self, keys):
        for key in keys:
            # TODO: can consider pre-loading ints and negative ints
            # into _keymap
            if isinstance(key, int):
                key = self._keys[key]

            try:
                rec = self._keymap[key]
            except KeyError as ke:
                rec = self._key_fallback(key, ke)

            index = rec[0]

            if index is None:
                self._raise_for_ambiguous_column_name(rec)

            yield rec

    def __getstate__(self):
        return {
            "_keymap": {
                key: (rec[MD_INDEX], _UNPICKLED, key)
                for key, rec in self._keymap.items()
                if isinstance(key, util.string_types + util.int_types)
            },
            "_keys": self._keys,
            "case_sensitive": self.case_sensitive,
            "_translated_indexes": self._translated_indexes,
            "_tuplefilter": self._tuplefilter,
        }

    def __setstate__(self, state):
        self._processors = [None for _ in range(len(state["_keys"]))]
        self._keymap = state["_keymap"]

        self._keys = state["_keys"]
        self.case_sensitive = state["case_sensitive"]

        if state["_translated_indexes"]:
            self._translated_indexes = state["_translated_indexes"]
            self._tuplefilter = tuplegetter(*self._translated_indexes)
        else:
            self._translated_indexes = self._tuplefilter = None


class LegacyCursorResultMetaData(CursorResultMetaData):
    def _contains(self, value, row):
        key = value
        if key in self._keymap:
            util.warn_deprecated_20(
                "Using the 'in' operator to test for string or column "
                "keys, or integer indexes, in a :class:`.Row` object is "
                "deprecated and will "
                "be removed in a future release. "
                "Use the `Row._fields` or `Row._mapping` attribute, i.e. "
                "'key in row._fields'",
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
                        "This usage will raise KeyError in a future release.",
                        version="1.4",
                    )
                else:
                    util.warn_deprecated(
                        "Retreiving row values using Column objects with only "
                        "matching names as keys is deprecated, and will raise "
                        "KeyError in a future release; only Column "
                        "objects that are explicitly part of the statement "
                        "object should be used.",
                        version="1.4",
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
            "the mappings() accessor on the Result object.",
            stacklevel=4,
        )

    def _has_key(self, key):
        if key in self._keymap:
            return True
        else:
            return self._key_fallback(key, None, False) is not None


class ResultFetchStrategy(object):
    """Define a fetching strategy for a result object.


    .. versionadded:: 1.4

    """

    __slots__ = ()

    def soft_close(self, result):
        raise NotImplementedError()

    def hard_close(self, result):
        raise NotImplementedError()

    def yield_per(self, result, num):
        return

    def fetchone(self, result):
        raise NotImplementedError()

    def fetchmany(self, result, size=None):
        raise NotImplementedError()

    def fetchall(self, result):
        raise NotImplementedError()

    def handle_exception(self, result, err):
        raise err


class NoCursorFetchStrategy(ResultFetchStrategy):
    """Cursor strategy for a result that has no open cursor.

    There are two varities of this strategy, one for DQL and one for
    DML (and also DDL), each of which represent a result that had a cursor
    but no longer has one.

    """

    __slots__ = ("closed",)

    def __init__(self, closed):
        self.closed = closed
        self.cursor_description = None

    def soft_close(self, result):
        pass

    def hard_close(self, result):
        self.closed = True

    def fetchone(self, result):
        return self._non_result(result, None)

    def fetchmany(self, result, size=None):
        return self._non_result(result, [])

    def fetchall(self, result):
        return self._non_result(result, [])

    def _non_result(self, result, default, err=None):
        raise NotImplementedError()


class NoCursorDQLFetchStrategy(NoCursorFetchStrategy):
    """Cursor strategy for a DQL result that has no open cursor.

    This is a result set that can return rows, i.e. for a SELECT, or for an
    INSERT, UPDATE, DELETE that includes RETURNING. However it is in the state
    where the cursor is closed and no rows remain available.  The owning result
    object may or may not be "hard closed", which determines if the fetch
    methods send empty results or raise for closed result.

    """

    def _non_result(self, result, default, err=None):
        if self.closed:
            util.raise_(
                exc.ResourceClosedError("This result object is closed."),
                replace_context=err,
            )
        else:
            return default


class NoCursorDMLFetchStrategy(NoCursorFetchStrategy):
    """Cursor strategy for a DML result that has no open cursor.

    This is a result set that does not return rows, i.e. for an INSERT,
    UPDATE, DELETE that does not include RETURNING.

    """

    def _non_result(self, result, default, err=None):
        # we only expect to have a _NoResultMetaData() here right now.
        assert not result._metadata.returns_rows
        result._metadata._we_dont_return_rows(err)


class CursorFetchStrategy(ResultFetchStrategy):
    """Call fetch methods from a DBAPI cursor.

    Alternate versions of this class may instead buffer the rows from
    cursors or not use cursors at all.

    """

    __slots__ = ("dbapi_cursor", "cursor_description")

    def __init__(self, dbapi_cursor, cursor_description):
        self.dbapi_cursor = dbapi_cursor
        self.cursor_description = cursor_description

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

    def handle_exception(self, result, err):
        result.connection._handle_dbapi_exception(
            err, None, None, self.dbapi_cursor, result.context
        )

    def yield_per(self, result, num):
        result.cursor_strategy = BufferedRowCursorFetchStrategy(
            self.dbapi_cursor,
            self.cursor_description,
            num,
            collections.deque(),
            growth_factor=0,
        )

    def fetchone(self, result):
        try:
            row = self.dbapi_cursor.fetchone()
            if row is None:
                result._soft_close()
            return row
        except BaseException as e:
            self.handle_exception(result, e)

    def fetchmany(self, result, size=None):
        try:
            if size is None:
                l = self.dbapi_cursor.fetchmany()
            else:
                l = self.dbapi_cursor.fetchmany(size)

            if not l:
                result._soft_close()
            return l
        except BaseException as e:
            self.handle_exception(result, e)

    def fetchall(self, result):
        try:
            rows = self.dbapi_cursor.fetchall()
            result._soft_close()
            return rows
        except BaseException as e:
            self.handle_exception(result, e)


class BufferedRowCursorFetchStrategy(CursorFetchStrategy):
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
                ).execute(text("select * from table"))

    .. versionadded:: 1.4 ``max_row_buffer`` may now exceed 1000 rows.

    .. seealso::

        :ref:`psycopg2_execution_options`
    """

    __slots__ = ("_max_row_buffer", "_rowbuffer", "_bufsize", "_growth_factor")

    def __init__(
        self,
        dbapi_cursor,
        description,
        max_row_buffer,
        initial_buffer,
        growth_factor=5,
    ):
        super(BufferedRowCursorFetchStrategy, self).__init__(
            dbapi_cursor, description
        )

        self._max_row_buffer = max_row_buffer
        self._growth_factor = growth_factor
        self._rowbuffer = initial_buffer

        if growth_factor:
            self._bufsize = min(self._max_row_buffer, self._growth_factor)
        else:
            self._bufsize = self._max_row_buffer

    @classmethod
    def create(cls, result):
        """Buffered row strategy has to buffer the first rows *before*
        cursor.description is fetched so that it works with named cursors
        correctly

        """

        dbapi_cursor = result.cursor

        # TODO: is create() called within a handle_error block externally?
        # can this be guaranteed / tested / etc
        initial_buffer = collections.deque(dbapi_cursor.fetchmany(1))

        description = dbapi_cursor.description

        if description is None:
            return NoCursorDMLFetchStrategy(False)
        else:
            max_row_buffer = result.context.execution_options.get(
                "max_row_buffer", 1000
            )
            return cls(
                dbapi_cursor, description, max_row_buffer, initial_buffer
            )

    def _buffer_rows(self, result):
        size = self._bufsize
        try:
            if size < 1:
                new_rows = self.dbapi_cursor.fetchall()
            else:
                new_rows = self.dbapi_cursor.fetchmany(size)
        except BaseException as e:
            self.handle_exception(result, e)

        if not new_rows:
            return
        self._rowbuffer = collections.deque(new_rows)
        if self._growth_factor and size < self._max_row_buffer:
            self._bufsize = min(
                self._max_row_buffer, size * self._growth_factor
            )

    def yield_per(self, result, num):
        self._growth_factor = 0
        self._max_row_buffer = self._bufsize = num

    def soft_close(self, result):
        self._rowbuffer.clear()
        super(BufferedRowCursorFetchStrategy, self).soft_close(result)

    def hard_close(self, result):
        self._rowbuffer.clear()
        super(BufferedRowCursorFetchStrategy, self).hard_close(result)

    def fetchone(self, result):
        if not self._rowbuffer:
            self._buffer_rows(result)
            if not self._rowbuffer:
                try:
                    result._soft_close()
                except BaseException as e:
                    self.handle_exception(result, e)
                return None
        return self._rowbuffer.popleft()

    def fetchmany(self, result, size=None):
        if size is None:
            return self.fetchall(result)

        buf = list(self._rowbuffer)
        lb = len(buf)
        if size > lb:
            try:
                buf.extend(self.dbapi_cursor.fetchmany(size - lb))
            except BaseException as e:
                self.handle_exception(result, e)

        result = buf[0:size]
        self._rowbuffer = collections.deque(buf[size:])
        return result

    def fetchall(self, result):
        try:
            ret = list(self._rowbuffer) + list(self.dbapi_cursor.fetchall())
            self._rowbuffer.clear()
            result._soft_close()
            return ret
        except BaseException as e:
            self.handle_exception(result, e)


class FullyBufferedCursorFetchStrategy(CursorFetchStrategy):
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
            self._rowbuffer = collections.deque(self.dbapi_cursor.fetchall())

    @classmethod
    def create_from_buffer(cls, dbapi_cursor, description, buffer):
        return cls(dbapi_cursor, description, buffer)

    def yield_per(self, result, num):
        pass

    def soft_close(self, result):
        self._rowbuffer.clear()
        super(FullyBufferedCursorFetchStrategy, self).soft_close(result)

    def hard_close(self, result):
        self._rowbuffer.clear()
        super(FullyBufferedCursorFetchStrategy, self).hard_close(result)

    def fetchone(self, result):
        if self._rowbuffer:
            return self._rowbuffer.popleft()
        else:
            result._soft_close()
            return None

    def fetchmany(self, result, size=None):
        if size is None:
            return self.fetchall(result)

        buf = list(self._rowbuffer)
        rows = buf[0:size]
        self._rowbuffer = collections.deque(buf[size:])
        if not rows:
            result._soft_close()
        return rows

    def fetchall(self, result):
        ret = self._rowbuffer
        self._rowbuffer = collections.deque()
        result._soft_close()
        return ret


class _NoResultMetaData(ResultMetaData):
    __slots__ = ()

    returns_rows = False

    def _we_dont_return_rows(self, err=None):
        util.raise_(
            exc.ResourceClosedError(
                "This result object does not return rows. "
                "It has been closed automatically."
            ),
            replace_context=err,
        )

    def _index_for_key(self, keys, raiseerr):
        self._we_dont_return_rows()

    def _metadata_for_keys(self, key):
        self._we_dont_return_rows()

    def _reduce(self, keys):
        self._we_dont_return_rows()

    @property
    def _keymap(self):
        self._we_dont_return_rows()

    @property
    def keys(self):
        self._we_dont_return_rows()


_no_result_metadata = _NoResultMetaData()


class BaseCursorResult(object):
    """Base class for database result objects.

    """

    out_parameters = None
    _metadata = None
    _soft_closed = False
    closed = False

    @classmethod
    def _create_for_context(cls, context):
        if context._is_future_result:
            obj = object.__new__(CursorResult)
        else:
            obj = object.__new__(LegacyCursorResult)
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
                    cached_md = self.context.compiled._cached_metadata
                    self._metadata = cached_md._adapt_to_context(self.context)

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
        else:
            self._metadata = _no_result_metadata
        # leave cursor open so that execution context can continue
        # setting up things like rowcount

    def _soft_close(self, hard=False):
        """Soft close this :class:`_engine.CursorResult`.

        This releases all DBAPI cursor resources, but leaves the
        CursorResult "open" from a semantic perspective, meaning the
        fetchXXX() methods will continue to return empty results.

        This method is called automatically when:

        * all result rows are exhausted using the fetchXXX() methods.
        * cursor.description is None.

        This method is **not public**, but is documented in order to clarify
        the "autoclose" process used.

        .. versionadded:: 1.0.0

        .. seealso::

            :meth:`_engine.CursorResult.close`


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

        See :attr:`_engine.CursorResult.rowcount` for background.

        """

        return self.dialect.supports_sane_rowcount

    def supports_sane_multi_rowcount(self):
        """Return ``supports_sane_multi_rowcount`` from the dialect.

        See :attr:`_engine.CursorResult.rowcount` for background.

        """

        return self.dialect.supports_sane_multi_rowcount

    @util.memoized_property
    def rowcount(self):
        """Return the 'rowcount' for this result.

        The 'rowcount' reports the number of rows *matched*
        by the WHERE criterion of an UPDATE or DELETE statement.

        .. note::

           Notes regarding :attr:`_engine.CursorResult.rowcount`:


           * This attribute returns the number of rows *matched*,
             which is not necessarily the same as the number of rows
             that were actually *modified* - an UPDATE statement, for example,
             may have no net change on a given row if the SET values
             given are the same as those present in the row already.
             Such a row would be matched but not modified.
             On backends that feature both styles, such as MySQL,
             rowcount is configured by default to return the match
             count in all cases.

           * :attr:`_engine.CursorResult.rowcount`
             is *only* useful in conjunction
             with an UPDATE or DELETE statement.  Contrary to what the Python
             DBAPI says, it does *not* return the
             number of rows available from the results of a SELECT statement
             as DBAPIs cannot support this functionality when rows are
             unbuffered.

           * :attr:`_engine.CursorResult.rowcount`
             may not be fully implemented by
             all dialects.  In particular, most DBAPIs do not support an
             aggregate rowcount result from an executemany call.
             The :meth:`_engine.CursorResult.supports_sane_rowcount` and
             :meth:`_engine.CursorResult.supports_sane_multi_rowcount` methods
             will report from the dialect if each usage is known to be
             supported.

           * Statements that use RETURNING may not return a correct
             rowcount.

        """
        try:
            return self.context.rowcount
        except BaseException as e:
            self.cursor_strategy.handle_exception(self, e)

    @property
    def lastrowid(self):
        """return the 'lastrowid' accessor on the DBAPI cursor.

        This is a DBAPI specific method and is only functional
        for those backends which support it, for statements
        where it is appropriate.  It's behavior is not
        consistent across backends.

        Usage of this method is normally unnecessary when
        using insert() expression constructs; the
        :attr:`~CursorResult.inserted_primary_key` attribute provides a
        tuple of primary key values for a newly inserted row,
        regardless of database backend.

        """
        try:
            return self.context.get_lastrowid()
        except BaseException as e:
            self.cursor_strategy.handle_exception(self, e)

    @property
    def returns_rows(self):
        """True if this :class:`_engine.CursorResult` returns zero or more rows.

        I.e. if it is legal to call the methods
        :meth:`_engine.CursorResult.fetchone`,
        :meth:`_engine.CursorResult.fetchmany`
        :meth:`_engine.CursorResult.fetchall`.

        Overall, the value of :attr:`_engine.CursorResult.returns_rows` should
        always be synonymous with whether or not the DBAPI cursor had a
        ``.description`` attribute, indicating the presence of result columns,
        noting that a cursor that returns zero rows still has a
        ``.description`` if a row-returning statement was emitted.

        This attribute should be True for all results that are against
        SELECT statements, as well as for DML statements INSERT/UPDATE/DELETE
        that use RETURNING.   For INSERT/UPDATE/DELETE statements that were
        not using RETURNING, the value will usually be False, however
        there are some dialect-specific exceptions to this, such as when
        using the MSSQL / pyodbc dialect a SELECT is emitted inline in
        order to retrieve an inserted primary key value.


        """
        return self._metadata.returns_rows

    @property
    def is_insert(self):
        """True if this :class:`_engine.CursorResult` is the result
        of a executing an expression language compiled
        :func:`_expression.insert` construct.

        When True, this implies that the
        :attr:`inserted_primary_key` attribute is accessible,
        assuming the statement did not include
        a user defined "returning" construct.

        """
        return self.context.isinsert


class CursorResult(BaseCursorResult, Result):
    """A Result that is representing state from a DBAPI cursor.

    .. versionchanged:: 1.4  The :class:`.CursorResult` and
       :class:`.LegacyCursorResult`
       classes replace the previous :class:`.ResultProxy` interface.
       These classes are based on the :class:`.Result` calling API
       which provides an updated usage model and calling facade for
       SQLAlchemy Core and SQLAlchemy ORM.

    Returns database rows via the :class:`.Row` class, which provides
    additional API features and behaviors on top of the raw data returned by
    the DBAPI.   Through the use of filters such as the :meth:`.Result.scalars`
    method, other kinds of objects may also be returned.

    Within the scope of the 1.x series of SQLAlchemy, Core SQL results in
    version 1.4 return an instance of :class:`._engine.LegacyCursorResult`
    which takes the place of the ``CursorResult`` class used for the 1.3 series
    and previously.  This object returns rows as :class:`.LegacyRow` objects,
    which maintains Python mapping (i.e. dictionary) like behaviors upon the
    object itself.  Going forward, the :attr:`.Row._mapping` attribute should
    be used for dictionary behaviors.

    .. seealso::

        :ref:`coretutorial_selecting` - introductory material for accessing
        :class:`_engine.CursorResult` and :class:`.Row` objects.

    """

    _cursor_metadata = CursorResultMetaData
    _cursor_strategy_cls = CursorFetchStrategy

    @HasMemoized.memoized_attribute
    def _row_logging_fn(self):
        if self._echo:
            log = self.context.engine.logger.debug

            def log_row(row):
                log("Row %r", sql_util._repr_row(row))
                return row

            return log_row
        else:
            return None

    def _fetchiter_impl(self):
        fetchone = self.cursor_strategy.fetchone

        while True:
            row = fetchone(self)
            if row is None:
                break
            yield row

    def _fetchone_impl(self):
        return self.cursor_strategy.fetchone(self)

    def _fetchall_impl(self):
        return self.cursor_strategy.fetchall(self)

    def _fetchmany_impl(self, size=None):
        return self.cursor_strategy.fetchmany(self, size)

    def _soft_close(self, **kw):
        BaseCursorResult._soft_close(self, **kw)

    def _raw_row_iterator(self):
        return self._fetchiter_impl()

    def close(self):
        """Close this :class:`_engine.CursorResult`.

        This closes out the underlying DBAPI cursor corresponding to the
        statement execution, if one is still present.  Note that the DBAPI
        cursor is automatically released when the :class:`_engine.CursorResult`
        exhausts all available rows.  :meth:`_engine.CursorResult.close` is
        generally an optional method except in the case when discarding a
        :class:`_engine.CursorResult` that still has additional rows pending
        for fetch.

        After this method is called, it is no longer valid to call upon
        the fetch methods, which will raise a :class:`.ResourceClosedError`
        on subsequent use.

        .. seealso::

            :ref:`connections_toplevel`

        """
        self._soft_close(hard=True)

    @_generative
    def yield_per(self, num):
        self._yield_per = num
        self.cursor_strategy.yield_per(self, num)


class LegacyCursorResult(CursorResult):
    """Legacy version of :class:`.CursorResult`.

    This class includes connection "connection autoclose" behavior for use with
    "connectionless" execution, as well as delivers rows using the
    :class:`.LegacyRow` row implementation.

    .. versionadded:: 1.4

    """

    _autoclose_connection = False
    _process_row = LegacyRow
    _cursor_metadata = LegacyCursorResultMetaData
    _cursor_strategy_cls = CursorFetchStrategy

    def close(self):
        """Close this :class:`_engine.LegacyCursorResult`.

        This method has the same behavior as that of
        :meth:`._engine.CursorResult`, but it also may close
        the underlying :class:`.Connection` for the case of "connectionless"
        execution.

        .. deprecated:: 2.0 "connectionless" execution is deprecated and will
           be removed in version 2.0.   Version 2.0 will feature the
           :class:`_future.Result`
           object that will no longer affect the status
           of the originating connection in any case.

        After this method is called, it is no longer valid to call upon
        the fetch methods, which will raise a :class:`.ResourceClosedError`
        on subsequent use.

        .. seealso::

            :ref:`connections_toplevel`

            :ref:`dbengine_implicit`
        """
        self._soft_close(hard=True)

    def _soft_close(self, hard=False):
        soft_closed = self._soft_closed
        super(LegacyCursorResult, self)._soft_close(hard=hard)
        if (
            not soft_closed
            and self._soft_closed
            and self._autoclose_connection
        ):
            self.connection.close()


ResultProxy = LegacyCursorResult


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
