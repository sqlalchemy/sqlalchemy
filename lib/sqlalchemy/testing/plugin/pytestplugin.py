try:
    # installed by bootstrap.py
    import sqla_plugin_base as plugin_base
except ImportError:
    # assume we're a package, use traditional import
    from . import plugin_base

import argparse
import collections
import inspect
import itertools
import operator
import os
import re
import sys

import pytest


try:
    import xdist  # noqa

    has_xdist = True
except ImportError:
    has_xdist = False


def pytest_addoption(parser):
    group = parser.getgroup("sqlalchemy")

    def make_option(name, **kw):
        callback_ = kw.pop("callback", None)
        if callback_:

            class CallableAction(argparse.Action):
                def __call__(
                    self, parser, namespace, values, option_string=None
                ):
                    callback_(option_string, values, parser)

            kw["action"] = CallableAction

        zeroarg_callback = kw.pop("zeroarg_callback", None)
        if zeroarg_callback:

            class CallableAction(argparse.Action):
                def __init__(
                    self,
                    option_strings,
                    dest,
                    default=False,
                    required=False,
                    help=None,  # noqa
                ):
                    super(CallableAction, self).__init__(
                        option_strings=option_strings,
                        dest=dest,
                        nargs=0,
                        const=True,
                        default=default,
                        required=required,
                        help=help,
                    )

                def __call__(
                    self, parser, namespace, values, option_string=None
                ):
                    zeroarg_callback(option_string, values, parser)

            kw["action"] = CallableAction

        group.addoption(name, **kw)

    plugin_base.setup_options(make_option)
    plugin_base.read_config()


def pytest_configure(config):
    pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

    if hasattr(config, "slaveinput"):
        plugin_base.restore_important_follower_config(config.slaveinput)
        plugin_base.configure_follower(config.slaveinput["follower_ident"])
    else:
        if config.option.write_idents and os.path.exists(
            config.option.write_idents
        ):
            os.remove(config.option.write_idents)

    plugin_base.pre_begin(config.option)

    plugin_base.set_coverage_flag(
        bool(getattr(config.option, "cov_source", False))
    )

    plugin_base.set_fixture_functions(PytestFixtureFunctions)


def pytest_sessionstart(session):
    plugin_base.post_begin()


def pytest_sessionfinish(session):
    plugin_base.final_process_cleanup()


if has_xdist:
    import uuid

    def pytest_configure_node(node):
        # the master for each node fills slaveinput dictionary
        # which pytest-xdist will transfer to the subprocess

        plugin_base.memoize_important_follower_config(node.slaveinput)

        node.slaveinput["follower_ident"] = "test_%s" % uuid.uuid4().hex[0:12]
        from sqlalchemy.testing import provision

        provision.create_follower_db(node.slaveinput["follower_ident"])

    def pytest_testnodedown(node, error):
        from sqlalchemy.testing import provision

        provision.drop_follower_db(node.slaveinput["follower_ident"])


def pytest_collection_modifyitems(session, config, items):
    # look for all those classes that specify __backend__ and
    # expand them out into per-database test cases.

    # this is much easier to do within pytest_pycollect_makeitem, however
    # pytest is iterating through cls.__dict__ as makeitem is
    # called which causes a "dictionary changed size" error on py3k.
    # I'd submit a pullreq for them to turn it into a list first, but
    # it's to suit the rather odd use case here which is that we are adding
    # new classes to a module on the fly.

    rebuilt_items = collections.defaultdict(
        lambda: collections.defaultdict(list)
    )

    items[:] = [
        item
        for item in items
        if isinstance(item.parent, pytest.Instance)
        and not item.parent.parent.name.startswith("_")
    ]

    test_classes = set(item.parent for item in items)
    for test_class in test_classes:
        for sub_cls in plugin_base.generate_sub_tests(
            test_class.cls, test_class.parent.module
        ):
            if sub_cls is not test_class.cls:
                per_cls_dict = rebuilt_items[test_class.cls]

                for inst in pytest.Class(
                    sub_cls.__name__, parent=test_class.parent.parent
                ).collect():
                    for t in inst.collect():
                        per_cls_dict[t.name].append(t)

    newitems = []
    for item in items:
        if item.parent.cls in rebuilt_items:
            newitems.extend(rebuilt_items[item.parent.cls][item.name])
        else:
            newitems.append(item)

    # seems like the functions attached to a test class aren't sorted already?
    # is that true and why's that? (when using unittest, they're sorted)
    items[:] = sorted(
        newitems,
        key=lambda item: (
            item.parent.parent.parent.name,
            item.parent.parent.name,
            item.name,
        ),
    )


def pytest_pycollect_makeitem(collector, name, obj):

    if inspect.isclass(obj) and plugin_base.want_class(name, obj):
        return [
            pytest.Class(parametrize_cls.__name__, parent=collector)
            for parametrize_cls in _parametrize_cls(collector.module, obj)
        ]
    elif (
        inspect.isfunction(obj)
        and isinstance(collector, pytest.Instance)
        and plugin_base.want_method(collector.cls, obj)
    ):
        # None means, fall back to default logic, which includes
        # method-level parametrize
        return None
    else:
        # empty list means skip this item
        return []


_current_class = None


def _parametrize_cls(module, cls):
    """implement a class-based version of pytest parametrize."""

    if "_sa_parametrize" not in cls.__dict__:
        return [cls]

    _sa_parametrize = cls._sa_parametrize
    classes = []
    for full_param_set in itertools.product(
        *[params for argname, params in _sa_parametrize]
    ):
        cls_variables = {}

        for argname, param in zip(
            [_sa_param[0] for _sa_param in _sa_parametrize], full_param_set
        ):
            if not argname:
                raise TypeError("need argnames for class-based combinations")
            argname_split = re.split(r",\s*", argname)
            for arg, val in zip(argname_split, param.values):
                cls_variables[arg] = val
        parametrized_name = "_".join(
            # token is a string, but in py2k py.test is giving us a unicode,
            # so call str() on it.
            str(re.sub(r"\W", "", token))
            for param in full_param_set
            for token in param.id.split("-")
        )
        name = "%s_%s" % (cls.__name__, parametrized_name)
        newcls = type.__new__(type, name, (cls,), cls_variables)
        setattr(module, name, newcls)
        classes.append(newcls)
    return classes


def pytest_runtest_setup(item):
    # here we seem to get called only based on what we collected
    # in pytest_collection_modifyitems.   So to do class-based stuff
    # we have to tear that out.
    global _current_class

    if not isinstance(item, pytest.Function):
        return

    # ... so we're doing a little dance here to figure it out...
    if _current_class is None:
        class_setup(item.parent.parent)
        _current_class = item.parent.parent

        # this is needed for the class-level, to ensure that the
        # teardown runs after the class is completed with its own
        # class-level teardown...
        def finalize():
            global _current_class
            class_teardown(item.parent.parent)
            _current_class = None

        item.parent.parent.addfinalizer(finalize)

    test_setup(item)


def pytest_runtest_teardown(item):
    # ...but this works better as the hook here rather than
    # using a finalizer, as the finalizer seems to get in the way
    # of the test reporting failures correctly (you get a bunch of
    # py.test assertion stuff instead)
    test_teardown(item)


def test_setup(item):
    plugin_base.before_test(
        item, item.parent.module.__name__, item.parent.cls, item.name
    )


def test_teardown(item):
    plugin_base.after_test(item)


def class_setup(item):
    plugin_base.start_test_class(item.cls)


def class_teardown(item):
    plugin_base.stop_test_class(item.cls)


def getargspec(fn):
    if sys.version_info.major == 3:
        return inspect.getfullargspec(fn)
    else:
        return inspect.getargspec(fn)


class PytestFixtureFunctions(plugin_base.FixtureFunctions):
    def skip_test_exception(self, *arg, **kw):
        return pytest.skip.Exception(*arg, **kw)

    _combination_id_fns = {
        "i": lambda obj: obj,
        "r": repr,
        "s": str,
        "n": operator.attrgetter("__name__"),
    }

    def combinations(self, *arg_sets, **kw):
        """facade for pytest.mark.paramtrize.

        Automatically derives argument names from the callable which in our
        case is always a method on a class with positional arguments.

        ids for parameter sets are derived using an optional template.

        """
        from sqlalchemy.testing import exclusions

        if sys.version_info.major == 3:
            if len(arg_sets) == 1 and hasattr(arg_sets[0], "__next__"):
                arg_sets = list(arg_sets[0])
        else:
            if len(arg_sets) == 1 and hasattr(arg_sets[0], "next"):
                arg_sets = list(arg_sets[0])

        argnames = kw.pop("argnames", None)

        exclusion_combinations = []

        def _filter_exclusions(args):
            result = []
            gathered_exclusions = []
            for a in args:
                if isinstance(a, exclusions.compound):
                    gathered_exclusions.append(a)
                else:
                    result.append(a)

            exclusion_combinations.extend(
                [(exclusion, result) for exclusion in gathered_exclusions]
            )
            return result

        id_ = kw.pop("id_", None)

        if id_:
            _combination_id_fns = self._combination_id_fns

            # because itemgetter is not consistent for one argument vs.
            # multiple, make it multiple in all cases and use a slice
            # to omit the first argument
            _arg_getter = operator.itemgetter(
                0,
                *[
                    idx
                    for idx, char in enumerate(id_)
                    if char in ("n", "r", "s", "a")
                ]
            )
            fns = [
                (operator.itemgetter(idx), _combination_id_fns[char])
                for idx, char in enumerate(id_)
                if char in _combination_id_fns
            ]

            arg_sets = [
                pytest.param(
                    *_arg_getter(_filter_exclusions(arg))[1:],
                    id="-".join(
                        comb_fn(getter(arg)) for getter, comb_fn in fns
                    )
                )
                for arg in [
                    (arg,) if not isinstance(arg, tuple) else arg
                    for arg in arg_sets
                ]
            ]
        else:
            # ensure using pytest.param so that even a 1-arg paramset
            # still needs to be a tuple.  otherwise paramtrize tries to
            # interpret a single arg differently than tuple arg
            arg_sets = [
                pytest.param(*_filter_exclusions(arg))
                for arg in [
                    (arg,) if not isinstance(arg, tuple) else arg
                    for arg in arg_sets
                ]
            ]

        def decorate(fn):
            if inspect.isclass(fn):
                if "_sa_parametrize" not in fn.__dict__:
                    fn._sa_parametrize = []
                fn._sa_parametrize.append((argnames, arg_sets))
                return fn
            else:
                if argnames is None:
                    _argnames = getargspec(fn).args[1:]
                else:
                    _argnames = argnames

                if exclusion_combinations:
                    for exclusion, combination in exclusion_combinations:
                        combination_by_kw = {
                            argname: val
                            for argname, val in zip(_argnames, combination)
                        }
                        exclusion = exclusion.with_combination(
                            **combination_by_kw
                        )
                        fn = exclusion(fn)
                return pytest.mark.parametrize(_argnames, arg_sets)(fn)

        return decorate

    def param_ident(self, *parameters):
        ident = parameters[0]
        return pytest.param(*parameters[1:], id=ident)

    def fixture(self, *arg, **kw):
        return pytest.fixture(*arg, **kw)

    def get_current_test_name(self):
        return os.environ.get("PYTEST_CURRENT_TEST")
