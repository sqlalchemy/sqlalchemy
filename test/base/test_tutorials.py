from __future__ import annotations

import doctest
import logging
import os
import re
import sys

from sqlalchemy.engine.url import make_url
from sqlalchemy.testing import config
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import requires
from sqlalchemy.testing import skip_test


class DocTest(fixtures.TestBase):
    # getting engine cleanup issues with free-threaded python.   can't
    # isolate what the cause would be except for GC not being very good
    __requires__ = ("insert_returning", "insertmanyvalues", "gil_enabled")
    __only_on__ = "sqlite+pysqlite"

    def _setup_logger(self):
        rootlogger = logging.getLogger("sqlalchemy.engine.Engine")

        class MyStream:
            def write(self, string):
                sys.stdout.write(string)
                sys.stdout.flush()

            def flush(self):
                pass

        self._handler = handler = logging.StreamHandler(MyStream())
        handler.setFormatter(logging.Formatter("%(message)s"))
        rootlogger.addHandler(handler)

    def _teardown_logger(self):
        rootlogger = logging.getLogger("sqlalchemy.engine.Engine")
        rootlogger.removeHandler(self._handler)

    def _setup_create_table_patcher(self):
        from sqlalchemy.sql import ddl

        self.orig_sort = ddl.sort_tables_and_constraints

        def our_sort(tables, **kw):
            return self.orig_sort(sorted(tables, key=lambda t: t.key), **kw)

        ddl.sort_tables_and_constraints = our_sort

    def _teardown_create_table_patcher(self):
        from sqlalchemy.sql import ddl

        ddl.sort_tables_and_constraints = self.orig_sort

    def setup_test(self):
        self._setup_logger()
        self._setup_create_table_patcher()

    def teardown_test(self):
        self._teardown_create_table_patcher()
        self._teardown_logger()

    def _run_doctest(self, *fnames):
        here = os.path.dirname(__file__)
        sqla_base = os.path.normpath(os.path.join(here, "..", ".."))

        optionflags = (
            doctest.ELLIPSIS
            | doctest.NORMALIZE_WHITESPACE
            | doctest.IGNORE_EXCEPTION_DETAIL
        )
        runner = doctest.DocTestRunner(
            verbose=config.options.verbose >= 2, optionflags=optionflags
        )
        parser = doctest.DocTestParser()
        globs = {"print_function": print}

        try:
            self._run_doctest_impl(fnames, sqla_base, runner, parser, globs)
        finally:
            # Clear all references from doctest execution
            globs.clear()

            # Delete references to avoid keeping stack frames alive
            del runner
            del parser
            del globs

    def _run_doctest_impl(self, fnames, sqla_base, runner, parser, globs):
        for fname in fnames:
            path = os.path.join(sqla_base, "doc/build", fname)
            if not os.path.exists(path):
                config.skip_test("Can't find documentation file %r" % path)

            buf = []

            with open(path, encoding="utf-8") as file_:

                def load_include(m):
                    fname = m.group(1)
                    sub_path = os.path.join(os.path.dirname(path), fname)
                    with open(sub_path, encoding="utf-8") as file_:
                        for i, line in enumerate(file_, 1):
                            buf.append((i, line))
                    return fname

                def run_buf(fname, is_include):
                    if not buf:
                        return

                    test = parser.get_doctest(
                        "".join(line for _, line in buf),
                        globs,
                        fname,
                        fname,
                        buf[0][0],
                    )
                    buf[:] = []
                    try:
                        runner.run(test, clear_globs=False)
                        globs.update(test.globs)
                    finally:
                        # Clear test object references
                        test.globs.clear()
                        test.examples.clear()

                doctest_enabled = True

                for line_counter, line in enumerate(file_, 1):
                    line = re.sub(
                        r"{(?:stop|sql|opensql|execsql|printsql)}", "", line
                    )

                    include = re.match(r"\.\. doctest-include (.+\.rst)", line)
                    if include:
                        run_buf(fname, False)
                        include_fname = load_include(include)
                        run_buf(include_fname, True)

                    doctest_disable = re.match(
                        r"\.\. doctest-(enable|disable)", line
                    )
                    if doctest_disable:
                        doctest_enabled = doctest_disable.group(1) == "enable"

                    if doctest_enabled:
                        buf.append((line_counter, line))
                    else:
                        buf.append((line_counter, "\n"))

                run_buf(fname, False)

            runner.summarize()
            assert not runner.failures

    @requires.has_json_each
    def test_20_style(self):
        self._run_doctest(
            "tutorial/index.rst",
            "tutorial/engine.rst",
            "tutorial/dbapi_transactions.rst",
            "tutorial/metadata.rst",
            "tutorial/data.rst",
            "tutorial/data_insert.rst",
            "tutorial/data_select.rst",
            "tutorial/data_update.rst",
            "tutorial/orm_data_manipulation.rst",
            "tutorial/orm_related_objects.rst",
        )

    def test_core_operators(self):
        self._run_doctest("core/operators.rst")

    def test_orm_queryguide_select(self):
        self._run_doctest(
            "orm/queryguide/_plain_setup.rst",
            "orm/queryguide/select.rst",
            "orm/queryguide/api.rst",
            "orm/queryguide/_end_doctest.rst",
        )

    def test_orm_queryguide_inheritance(self):
        self._run_doctest("orm/queryguide/inheritance.rst")

    @requires.update_from
    def test_orm_queryguide_dml(self):
        self._run_doctest("orm/queryguide/dml.rst")

    def test_orm_large_collections(self):
        self._run_doctest("orm/large_collections.rst")

    def test_orm_queryguide_columns(self):
        self._run_doctest("orm/queryguide/columns.rst")

    def test_orm_quickstart(self):
        self._run_doctest("orm/quickstart.rst")

    # this crashes on 3.13t but passes on 3.14t.
    # just requiring non-freethreaded for now
    @requires.gil_enabled
    @requires.greenlet
    def test_asyncio(self):
        try:
            make_url("sqlite+aiosqlite://").get_dialect().import_dbapi()
        except ImportError:
            skip_test("missing aiosqile")
        self._run_doctest("orm/extensions/asyncio.rst")


# increase number to force pipeline run. 1
