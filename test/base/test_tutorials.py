from __future__ import print_function
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import config
import doctest
import logging
import sys
import re
import os


class DocTest(fixtures.TestBase):
    def _setup_logger(self):
        rootlogger = logging.getLogger('sqlalchemy.engine.base.Engine')

        class MyStream(object):
            def write(self, string):
                sys.stdout.write(string)
                sys.stdout.flush()

            def flush(self):
                pass

        self._handler = handler = logging.StreamHandler(MyStream())
        handler.setFormatter(logging.Formatter('%(message)s'))
        rootlogger.addHandler(handler)

    def _teardown_logger(self):
        rootlogger = logging.getLogger('sqlalchemy.engine.base.Engine')
        rootlogger.removeHandler(self._handler)

    def _setup_create_table_patcher(self):
        from sqlalchemy.sql import ddl
        self.orig_sort = ddl.sort_tables_and_constraints

        def our_sort(tables, **kw):
            return self.orig_sort(
                sorted(tables, key=lambda t: t.key), **kw
            )
        ddl.sort_tables_and_constraints = our_sort

    def _teardown_create_table_patcher(self):
        from sqlalchemy.sql import ddl
        ddl.sort_tables_and_constraints = self.orig_sort

    def setup(self):
        self._setup_logger()
        self._setup_create_table_patcher()

    def teardown(self):
        self._teardown_create_table_patcher()
        self._teardown_logger()

    def _run_doctest_for_content(self, name, content):
        optionflags = (
            doctest.ELLIPSIS |
            doctest.NORMALIZE_WHITESPACE |
            doctest.IGNORE_EXCEPTION_DETAIL |
            _get_allow_unicode_flag()
        )
        runner = doctest.DocTestRunner(
            verbose=None, optionflags=optionflags,
            checker=_get_unicode_checker())
        globs = {
            'print_function': print_function}
        parser = doctest.DocTestParser()
        test = parser.get_doctest(content, globs, name, name, 0)
        runner.run(test)
        runner.summarize()
        assert not runner.failures

    def _run_doctest(self, fname):
        here = os.path.dirname(__file__)
        sqla_base = os.path.normpath(os.path.join(here, "..", ".."))
        path = os.path.join(sqla_base, "doc/build", fname)
        if not os.path.exists(path):
            config.skip_test("Can't find documentation file %r" % path)
        with open(path) as file_:
            content = file_.read()
            content = re.sub(r'{(?:stop|sql|opensql)}', '', content)
            self._run_doctest_for_content(fname, content)

    def test_orm(self):
        self._run_doctest("orm/tutorial.rst")

    def test_core(self):
        self._run_doctest("core/tutorial.rst")


# unicode checker courtesy py.test


def _get_unicode_checker():
    """
    Returns a doctest.OutputChecker subclass that takes in account the
    ALLOW_UNICODE option to ignore u'' prefixes in strings. Useful
    when the same doctest should run in Python 2 and Python 3.

    An inner class is used to avoid importing "doctest" at the module
    level.
    """
    if hasattr(_get_unicode_checker, 'UnicodeOutputChecker'):
        return _get_unicode_checker.UnicodeOutputChecker()

    import doctest
    import re

    class UnicodeOutputChecker(doctest.OutputChecker):
        """
        Copied from doctest_nose_plugin.py from the nltk project:
            https://github.com/nltk/nltk
        """

        _literal_re = re.compile(r"(\W|^)[uU]([rR]?[\'\"])", re.UNICODE)

        def check_output(self, want, got, optionflags):
            res = doctest.OutputChecker.check_output(self, want, got,
                                                     optionflags)
            if res:
                return True

            if not (optionflags & _get_allow_unicode_flag()):
                return False

            else:  # pragma: no cover
                # the code below will end up executed only in Python 2 in
                # our tests, and our coverage check runs in Python 3 only
                def remove_u_prefixes(txt):
                    return re.sub(self._literal_re, r'\1\2', txt)

                want = remove_u_prefixes(want)
                got = remove_u_prefixes(got)
                res = doctest.OutputChecker.check_output(self, want, got,
                                                         optionflags)
                return res

    _get_unicode_checker.UnicodeOutputChecker = UnicodeOutputChecker
    return _get_unicode_checker.UnicodeOutputChecker()


def _get_allow_unicode_flag():
    """
    Registers and returns the ALLOW_UNICODE flag.
    """
    import doctest
    return doctest.register_optionflag('ALLOW_UNICODE')
