import os
import pathlib
import shutil

from sqlalchemy import testing
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


def _incremental_dirs():
    path = os.path.join(os.path.dirname(__file__), "incremental")
    files = []
    for d in os.listdir(path):
        if os.path.isdir(os.path.join(path, d)):
            files.append(
                os.path.join(os.path.dirname(__file__), "incremental", d)
            )

    for extra_dir in testing.config.options.mypy_extra_test_paths:
        if extra_dir and os.path.isdir(extra_dir):
            for d in os.listdir(os.path.join(extra_dir, "incremental")):
                if os.path.isdir(os.path.join(path, d)):
                    files.append(os.path.join(extra_dir, "incremental", d))
    return files


class MypyPluginTest(fixtures.MypyTest):
    @testing.combinations(
        *[
            (pathlib.Path(pathname).name, pathname)
            for pathname in _incremental_dirs()
        ],
        argnames="pathname",
        id_="ia",
    )
    @testing.requires.patch_library
    def test_incremental(self, mypy_runner, per_func_cachedir, pathname):
        import patch

        cachedir = per_func_cachedir

        dest = os.path.join(cachedir, "mymodel")
        os.mkdir(dest)

        patches = set()

        print("incremental test: %s" % pathname)

        for fname in os.listdir(pathname):
            if fname.endswith(".py"):
                shutil.copy(
                    os.path.join(pathname, fname), os.path.join(dest, fname)
                )
                print("copying to: %s" % os.path.join(dest, fname))
            elif fname.endswith(".testpatch"):
                patches.add(fname)

        for patchfile in [None] + sorted(patches):
            if patchfile is not None:
                print("Applying patchfile %s" % patchfile)
                patch_obj = patch.fromfile(os.path.join(pathname, patchfile))
                assert patch_obj.apply(1, dest), (
                    "pathfile %s failed" % patchfile
                )
            print("running mypy against %s" % dest)
            result = mypy_runner(
                dest,
                use_plugin=True,
                use_cachedir=cachedir,
            )
            eq_(
                result[2],
                0,
                msg="Failure after applying patch %s: %s"
                % (patchfile, result[0]),
            )

    @testing.combinations(
        *(
            (os.path.basename(path), path, True)
            for path in fixtures.MypyTest.file_combinations("plugin_files")
        ),
        argnames="path",
        id_="ia",
    )
    def test_plugin_files(self, mypy_typecheck_file, path):
        mypy_typecheck_file(path, use_plugin=True)
