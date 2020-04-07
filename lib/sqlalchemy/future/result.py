import operator

from .. import util
from ..engine.result import BaseResult
from ..engine.result import CursorResultMetaData
from ..engine.result import DefaultCursorFetchStrategy
from ..engine.result import Row
from ..sql import util as sql_util
from ..sql.base import _generative
from ..sql.base import InPlaceGenerative


class Result(InPlaceGenerative, BaseResult):
    """Interim "future" result proxy so that dialects can build on
    upcoming 2.0 patterns.


    """

    _process_row = Row
    _cursor_metadata = CursorResultMetaData
    _cursor_strategy_cls = DefaultCursorFetchStrategy

    _column_slice_filter = None
    _post_creational_filter = None

    def close(self):
        """Close this :class:`_future.Result`.

        This closes out the underlying DBAPI cursor corresponding
        to the statement execution, if one is still present.  Note that the
        DBAPI cursor is automatically released when the
        :class:`_future.Result`
        exhausts all available rows.  :meth:`_future.Result.close`
        is generally
        an optional method except in the case when discarding a
        :class:`_future.Result`
        that still has additional rows pending for fetch.

        After this method is called, it is no longer valid to call upon
        the fetch methods, which will raise a :class:`.ResourceClosedError`
        on subsequent use.

        .. seealso::

            :ref:`connections_toplevel`

        """
        self._soft_close(hard=True)

    def columns(self, *col_expressions):
        r"""Establish the columns that should be returned in each row.

        This method may be used to limit the columns returned as well
        as to reorder them.   The given list of expressions are normally
        a series of integers or string key names.   They may also be
        appropriate :class:`.ColumnElement` objects which correspond to
        a given statement construct.

        E.g.::

            statement = select(table.c.x, table.c.y, table.c.z)
            result = connection.execute(statement)

            for z, y in result.columns('z', 'y'):
                # ...


        Example of using the column objects from the statement itself::

            for z, y in result.columns(
                    statement.selected_columns.c.z,
                    statement.selected_columns.c.y
            ):
                # ...

        :param \*col_expressions: indicates columns to be returned.  Elements
         may be integer row indexes, string column names, or appropriate
         :class:`.ColumnElement` objects corresponding to a select construct.

        :return: this :class:`_future.Result` object with the modifications
         given.

        """
        return self._column_slices(col_expressions)

    def partitions(self, size=100):
        """Iterate through sub-lists of rows of the size given.

        Each list will be of the size given, excluding the last list to
        be yielded, which may have a small number of rows.  No empty
        lists will be yielded.

        The result object is automatically closed when the iterator
        is fully consumed.

        Note that the backend driver will usually buffer the entire result
        ahead of time unless the
        :paramref:`.Connection.execution_options.stream_results` execution
        option is used indicating that the driver should not pre-buffer
        results, if possible.   Not all drivers support this option and
        the option is silently ignored for those who do.   For a positive
        assertion that the driver supports streaming results that will
        fail if not supported, use the
        :paramref:`.Connection.execution_options.stream_per`
        execution option.

        :param size: indicate the maximum number of rows to be present
         in each list yielded.
        :return: iterator of lists

        """
        getter = self._row_getter()
        while True:
            partition = [
                getter(r) for r in self._safe_fetchmany_impl(size=size)
            ]
            if partition:
                yield partition
            else:
                break

    def scalars(self):
        result = self._column_slices(0)
        result._post_creational_filter = operator.itemgetter(0)
        return result

    @_generative
    def _column_slices(self, indexes):
        self._column_slice_filter = self._metadata._tuple_getter(indexes)

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
            l = self.cursor_strategy.fetchmany(size)
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

    @util.deprecated(
        "2.0",
        "The :meth:`_future.Result.fetchall` "
        "method is provided for backwards "
        "compatibility and will be removed in a future release.",
    )
    def fetchall(self):
        """A synonym for the :meth:`_future.Result.all` method."""

        return self.all()

    @util.deprecated(
        "2.0",
        "The :meth:`_future.Result.fetchone` "
        "method is provided for backwards "
        "compatibility and will be removed in a future release.",
    )
    def fetchone(self):
        """Fetch one row.

        this method is provided for backwards compatibility with
        SQLAlchemy 1.x.x.

        To fetch the first row of a result only, use the
        :meth:`.future.Result.first` method.  To iterate through all
        rows, iterate the :class:`_future.Result` object directly.

        """
        return self._onerow()

    @util.deprecated(
        "2.0",
        "The :meth:`_future.Result.fetchmany` "
        "method is provided for backwards "
        "compatibility and will be removed in a future release.",
    )
    def fetchmany(self, size=None):
        """Fetch many rows.

        this method is provided for backwards compatibility with
        SQLAlchemy 1.x.x.

        To fetch rows in groups, use the :meth:`.future.Result.partitions`
        method, or the :meth:`.future.Result.chunks` method in combination
        with the :paramref:`.Connection.execution_options.stream_per`
        option which sets up the buffer size before fetching the result.

        """
        getter = self._row_getter()
        return [getter(r) for r in self._safe_fetchmany_impl(size=size)]

    def all(self):
        """Return all rows in a list.

        Closes the result set after invocation.

        :return: a list of :class:`.Row` objects.

        """
        getter = self._row_getter()
        return [getter(r) for r in self._safe_fetchall_impl()]

    def first(self):
        """Fetch the first row or None if no row is present.

        Closes the result set and discards remaining rows.  A warning
        is emitted if additional rows remain.

        :return: a :class:`.Row` object, or None if no rows remain

        """
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
