from test.lib import *
import os
import re


def find_py_files(dirs):
    for dn in dirs:
        dn = os.path.abspath(dn)
        for root, dirs, files in os.walk(dn):
            for r in '.svn', 'CVS', '.git', '.hg':
                try:
                    dirs.remove(r)
                except ValueError:
                    pass
            pyfiles = [fn for fn in files if fn.endswith('.py')]
            if not pyfiles:
                continue

            # Find the root of the packages.

            packroot = root
            while 1:
                if not os.path.exists(os.path.join(packroot,
                        '__init__.py')):
                    break
                packroot = os.path.dirname(packroot)
            for fn in pyfiles:
                yield os.path.join(root[len(packroot) + 1:], fn)


def filename_to_module_name(fn):
    if os.path.basename(fn) == '__init__.py':
        fn = os.path.dirname(fn)
    return re.sub('\.py$', '', fn.replace(os.sep, '.'))


def find_modules(*args):
    for fn in find_py_files(args or ('./examples', )):
        yield filename_to_module_name(fn)


def check_import(module):
    __import__(module)


class ExamplesTest(fixtures.TestBase):

    # TODO: ensure examples are actually run regardless of check for
    # "__main__", perhaps standardizing the format of all examples.
    # ensure that examples with external dependencies are not run if
    # those dependencies are not present (i.e. elementtree, postgis)

    def test_examples(self):
        pass


        # for module in find_modules(): check_import.description =
        # module yield check_import, module
