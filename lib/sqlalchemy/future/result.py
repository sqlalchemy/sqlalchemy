import operator

from .. import util
from ..engine.result import _baserow_usecext
from ..engine.result import BaseResult
from ..engine.result import CursorResultMetaData
from ..engine.result import DefaultCursorFetchStrategy
from ..engine.result import Row
from ..sql import util as sql_util
from ..sql.base import _generative
from ..sql.base import Generative


class Result(Generative, BaseResult):
    """Interim "future" result proxy so that dialects can build on
    upcoming 2.0 patterns.


    """

    _process_row = Row
    _cursor_metadata = CursorResultMetaData
    _cursor_strategy_cls = DefaultCursorFetchStrategy

    _column_slice_filter = None
    _post_creational_filter = None

    def close(self):
        """Close this :class:`.Result`.

        This closes out the underlying DBAPI cursor corresponding
        to the statement execution, if one is still present.  Note that the
        DBAPI cursor is automatically released when the :class:`.Result`
        exhausts all available rows.  :meth:`.Result.close` is generally
        an optional method except in the case when discarding a
        :class:`.Result` that still has additional rows pending for fetch.

        After this method is called, it is no longer valid to call upon
        the fetch methods, which will raise a :class:`.ResourceClosedError`
        on subsequent use.

        .. seealso::

            :ref:`connections_toplevel`

        """
        self._soft_close(hard=True)

    def columns(self, *col_expressions):
        indexes = []
        for key in col_expressions:
            try:
                rec = self._keymap[key]
            except KeyError:
                rec = self._key_fallback(key, True)
                if rec is None:
                    return None

            index, obj = rec[0:2]

            if index is None:
                self._metadata._raise_for_ambiguous_column_name(obj)
            indexes.append(index)
        return self._column_slices(indexes)

    def scalars(self):
        result = self._column_slices(0)
        result._post_creational_filter = operator.itemgetter(0)
        return result

    @_generative
    def _column_slices(self, indexes):
        if _baserow_usecext:
            self._column_slice_filter = self._metadata._tuplegetter(*indexes)
        else:
            self._column_slice_filter = self._metadata._pure_py_tuplegetter(
                *indexes
            )

    @_generative
    def mappings(self):
        self._post_creational_filter = operator.attrgetter("_mapping")

    def _row_getter(self):
        process_row = self._process_row
        metadata = self._metadata
        keymap = metadata._keymap
        processors = metadata._processors

        fns = ()

        if self._echo:
            log = self.context.engine.logger.debug

            def log_row(row):
                log("Row %r", sql_util._repr_row(row))
                return row

            fns += (log_row,)

        if self._column_slice_filter:
            fns += (self._column_slice_filter,)

        if self._post_creational_filter:
            fns += (self._post_creational_filter,)

        def make_row(row):
            row = process_row(metadata, processors, keymap, row)
            for fn in fns:
                row = fn(row)
            return row

        return make_row

    def _safe_fetchone_impl(self):
        try:
            return self.cursor_strategy.fetchone()
        except BaseException as e:
            self.connection._handle_dbapi_exception(
                e, None, None, self.cursor, self.context
            )

    def _safe_fetchall_impl(self):
        try:
            result = self.cursor_strategy.fetchall()
            self._soft_close()
            return result
        except BaseException as e:
            self.connection._handle_dbapi_exception(
                e, None, None, self.cursor, self.context
            )

    def _safe_fetchmany_impl(self, size=None):
        try:
            l = self.process_rows(self.cursor_strategy.fetchmany(size))
            if len(l) == 0:
                self._soft_close()
            return l
        except BaseException as e:
            self.connection._handle_dbapi_exception(
                e, None, None, self.cursor, self.context
            )

    def __iter__(self):
        getter = self._row_getter()
        return (getter(r) for r in self._safe_fetchall_impl())

    def _onerow(self):
        getter = self._row_getter()
        row = self._safe_fetchone_impl()
        if row is None:
            return None
        else:
            return getter(row)

    def all(self):
        getter = self._row_getter()
        return [getter(r) for r in self._safe_fetchall_impl()]

    def first(self):
        getter = self._row_getter()
        row = self._safe_fetchone_impl()
        if row is None:
            return None
        else:
            row = getter(row)
            second_row = self._safe_fetchone_impl()
            if second_row is not None:
                self._soft_close()
                util.warn("Additional rows remain")
            return row
