# stdlib
import os
import pprint
import shutil
import sys
import time
from typing import Optional
from typing import Union

# pypi
from fabric import task
from fabric.connection import Connection

# local
from ._utils import BASE_DIR

# ==============================================================================

# Environment Control Vars
AUDIT_PERFORMANCE: bool = bool(int(os.getenv("SQLA_AUDIT_PERFORMANCE", "1")))
AUDIT_FLAKE8: bool = bool(int(os.getenv("SQLA_AUDIT_FLAKE8", "1")))
AUDIT_MYPY: bool = bool(int(os.getenv("SQLA_AUDIT_MYPY", "1")))
VERBOSE_REPORTING: bool = bool(int(os.getenv("SQLA_AUDIT_VERBOSE", "1")))
# set our audit mode, for test scripts
os.environ["SQLA_AUDIT_MODE"] = "1"

# Typing hints
TYPE_EXAMPLES_MATRIX = dict[str, str]
TYPE_ExampleMetadata = dict[str, dict[str, dict[str, Union[str, bool]]]]

# Globals
EXAMPLES_MATRIX: Optional[TYPE_EXAMPLES_MATRIX] = None
EXAMPLES_BASEDIR = os.path.join(BASE_DIR, "examples")
ExampleMetadata: Optional[TYPE_ExampleMetadata] = None


def _valid_example_file(fname: str) -> bool:
    """
    logic to determine if a file in an /examples/ directory is valid or not.
    """
    if fname in ("__init__.py", "__pycache__"):
        return False
    elif fname[0] == ".":
        return False
    elif fname[-3:] == ".db":
        return False
    return True


def get_ExamplesMatrix(force: bool = False) -> TYPE_EXAMPLES_MATRIX:
    """
    get the global `EXAMPLES_MATRIX`
    generate * memoize the value if unset
    """
    global EXAMPLES_MATRIX
    if EXAMPLES_MATRIX is None or force:
        _dir_candidates = [
            (i, os.path.join(EXAMPLES_BASEDIR, i))
            for i in os.listdir(EXAMPLES_BASEDIR)
            if i not in ("__pycache__", ".mypy_cache")
        ]
        EXAMPLES_MATRIX = {
            i[0]: i[1] for i in _dir_candidates if os.path.isdir(i[1])
        }
    return EXAMPLES_MATRIX


def get_ExampleMetadata(
    c: Connection,
) -> TYPE_ExampleMetadata:
    """
    get the global `ExampleMetadata`
    generate * memoize the value if unset
    """
    global ExampleMetadata
    if ExampleMetadata is None:
        ExampleMetadata = examples_audit(
            c,
            audit_flake8=False,
            audit_mypy=False,
            echo=False,
        )
    return ExampleMetadata


@task
def examples_audit(
    c: Connection,
    audit_flake8: bool = AUDIT_FLAKE8,
    audit_mypy: bool = AUDIT_MYPY,
    echo: bool = True,
) -> ExampleMetadata:
    """quickly audit examples to determine requirements & flake8/mypy compat"""

    _og_path = sys.path.copy()
    results_pg: list[str] = []
    results_pypi: dict[str, list[str]] = {}
    results_flake8: dict[str, str] = {}
    results_mypy: dict[str, str] = {}
    do_not_execute: dict[str, list[str]] = {}

    _ExampleMetadata: TYPE_ExampleMetadata = {}

    try:
        sys.path.insert(0, BASE_DIR)  # remove in `finally` block
        examples_package = __import__("examples")
        examples = get_ExamplesMatrix()
        package_name: str
        path: str
        
        for package_name, path in examples.items():
            _imported = __import__("examples." + package_name)  # noqa: F841
            _packaged = getattr(examples_package, package_name)
            _ExampleMetadata[package_name] = {}
            if hasattr(_packaged, "REQUIREMENTS"):
                for fname, data in _packaged.REQUIREMENTS.items():
                    _package_data = {}
                    fname_full = os.path.join(package_name, fname)
                    if "postgresql" in data and data["postgresql"]:
                        results_pg.append(fname_full)
                    if "pypi" in data:
                        for pypi in data["pypi"]:
                            if pypi not in results_pypi:
                                results_pypi[pypi] = []
                            results_pypi[pypi].append(fname_full)
                    if "executable" in data:
                        if data["executable"] is False:
                            if package_name not in do_not_execute:
                                do_not_execute[package_name] = []
                            do_not_execute[package_name].append(fname)
                            _package_data["executable"] = False
                    if "repeat_run" in data:
                        _package_data["repeat_run"] = data["repeat_run"]
                    if "cleanup_dir" in data:
                        _package_data["cleanup_dir"] = data["cleanup_dir"]
                    if "cleanup_files" in data:
                        _package_data["cleanup_files"] = data["cleanup_files"]
                    if "persistent_subprocess" in data:
                        if data["persistent_subprocess"] is True:
                            _package_data["persistent_subprocess"] = True
                    _ExampleMetadata[package_name][fname] = _package_data

            if audit_flake8:
                _result = c.run(
                    "flake8 examples/%s" % package_name, hide="both", warn=True
                )
                if _result.failed:
                    results_flake8[package_name] = (
                        _result.stdout if VERBOSE_REPORTING else "x"
                    )
            if audit_mypy:
                _result = c.run(
                    "mypy examples/%s" % package_name, hide="both", warn=True
                )
                if _result.failed:
                    results_mypy[package_name] = (
                        _result.stdout if VERBOSE_REPORTING else "x"
                    )
        if echo:
            if results_pg:
                print("The following examples require a PostgreSQL database:")
                for i in results_pg:
                    print("\t%s" % i)
            for pypi in results_pypi:
                print(
                    "The following examples require the PyPI package `%s`:"
                    % pypi
                )
                for i in results_pypi[pypi]:
                    print("\t%s" % i)
            if audit_flake8:
                if results_flake8:
                    print("The following examples have flake8 issues:")
                    for fpath, stdout in results_flake8.items():
                        print(stdout)
                else:
                    print("No flake8 issues detected")
            else:
                print("flake8 audit disabled")
            if audit_mypy:
                if results_mypy:
                    print("The following examples have mypy issues:")
                    for fpath, stdout in results_mypy.items():
                        print(stdout)
                else:
                    print("No mypy issues detected")
            else:
                print("mypy audit disabled")
    finally:
        sys.path = _og_path

    # reset the global
    global ExampleMetadata
    ExampleMetadata = _ExampleMetadata
    return _ExampleMetadata


@task
def examples_test(c: Connection) -> None:
    """
    run each example, report if it works or not
    """
    examples = get_ExamplesMatrix()
    metadata = get_ExampleMetadata(c)

    failures: dict[str, dict[str, str]] = {}
    deprecateds: dict[str, dict[str, str]] = {}
    for package_name, path in examples.items():
        if package_name not in metadata:
            raise ValueError("Could not load metadata for: %s" % package_name)
        if package_name == "performance":
            if not AUDIT_PERFORMANCE:
                print("Not testing `performance`")
                continue
        package_files = [i for i in os.listdir(path) if _valid_example_file(i)]
        for _file in package_files:
            fbase = _file.split(".")[0]

            _cleanup_dir: Optional[str] = None
            _cleanup_files: Optional[List[str]] = None
            _executable: Optional[bool] = None
            _repeat_run: Optional[bool] = None
            _persistent_subprocess: Optional[bool] = None
            if _file in metadata[package_name]:
                _cleanup_dir = metadata[package_name][_file].get(
                    "cleanup_dir", None
                )
                _cleanup_files = metadata[package_name][_file].get(
                    "cleanup_files", None
                )
                _repeat_run = metadata[package_name][_file].get(
                    "repeat_run", None
                )
                _executable = metadata[package_name][_file].get(
                    "executable", None
                )
                _persistent_subprocess = metadata[package_name][_file].get(
                    "persistent_subprocess", None
                )

            if _executable is False:
                print("NOT RUNNING: examples.%s.%s" % (package_name, fbase))
                # continue

            print("Running: examples.%s.%s" % (package_name, fbase))

            if _cleanup_dir:
                if _cleanup_dir == "dogpile_data":  # ONLY remove this
                    if os.path.exists("dogpile_data"):
                        print("ingress cleanup: `/dogpile_data`")
                        shutil.rmtree("dogpile_data")
            if _cleanup_files:
                for _fname in _cleanup_files:
                    if os.path.exists(_fname):
                        print("ingress cleanup: `/%s`" % _fname)
                        os.unlink(_fname)

            _asynchronous = False
            if _persistent_subprocess:
                _asynchronous = True

            _result = c.run(
                "python -m examples.%s.%s" % (package_name, fbase),
                hide="both",
                warn=True,
                asynchronous=_asynchronous,
            )
            if _asynchronous:
                # _result is an Invoke promise
            
                # wait 2 seconds, it should still be active
                time.sleep(2)

                #   _output = _result.runner.stdout
                _result.runner.kill()  # will set return_code to -9
                _result = _result.join()

            if _result.failed and (_result.return_code != -9):
                if package_name not in failures:
                    failures[package_name] = {}
                failures[package_name][fbase] = (
                    _result.stderr if VERBOSE_REPORTING else "x"
                )
            else:
                if (
                    "DeprecationWarning" in _result.stdout
                    or "RemovedIn20Warning" in _result.stdout
                ):
                    if package_name not in deprecateds:
                        deprecateds[package_name] = {}
                    deprecateds[package_name][fbase] = (
                        _result.stdout if VERBOSE_REPORTING else "x"
                    )

            if _repeat_run:
                print("Running second invocation:")

                _result = c.run(
                    "python -m examples.%s.%s" % (package_name, fbase),
                    hide="both",
                    warn=True,
                )
                if _result.failed:
                    if package_name not in failures:
                        failures[package_name] = {}
                    failures[package_name][fbase] = "SECOND RUN: " + (
                        _result.stderr if VERBOSE_REPORTING else "x"
                    )
                else:
                    if (
                        "DeprecationWarning" in _result.stdout
                        or "RemovedIn20Warning" in _result.stdout
                    ):
                        if package_name not in deprecateds:
                            deprecateds[package_name] = {}
                        deprecateds[package_name][fbase] = (
                            _result.stdout if VERBOSE_REPORTING else "x"
                        )

            if _cleanup_dir:
                if _cleanup_dir == "dogpile_data":  # ONLY remove this
                    if os.path.exists("dogpile_data"):
                        print("egress cleanup: `/dogpile_data`")
                        shutil.rmtree("dogpile_data")
            if _cleanup_files:
                for _fname in _cleanup_files:
                    if os.path.exists(_fname):
                        print("egress cleanup: `/%s`" % _fname)
                        os.unlink(_fname)

    if failures:
        print("The following files had failures:")
        pprint.pprint(failures)
    else:
        print("No failures detected.")

    if deprecateds:
        print("The following files had deprecateds:")
        pprint.pprint(deprecateds)
    else:
        print("No deprecateds detected.")
