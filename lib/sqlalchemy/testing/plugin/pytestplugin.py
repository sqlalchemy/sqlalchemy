import pytest
import argparse
import inspect
from . import plugin_base
import collections

def pytest_addoption(parser):
    group = parser.getgroup("sqlalchemy")

    def make_option(name, **kw):
        callback_ = kw.pop("callback", None)
        if callback_:
            class CallableAction(argparse.Action):
                def __call__(self, parser, namespace, values, option_string=None):
                    callback_(option_string, values, parser)
            kw["action"] = CallableAction

        group.addoption(name, **kw)

    plugin_base.setup_options(make_option)
    plugin_base.read_config()

def pytest_configure(config):
    plugin_base.pre_begin(config.option)

    plugin_base.set_coverage_flag(bool(getattr(config.option, "cov_source", False)))

    plugin_base.post_begin()


def pytest_collection_modifyitems(session, config, items):
    # look for all those classes that specify __backend__ and
    # expand them out into per-database test cases.

    # this is much easier to do within pytest_pycollect_makeitem, however
    # pytest is iterating through cls.__dict__ as makeitem is
    # called which causes a "dictionary changed size" error on py3k.
    # I'd submit a pullreq for them to turn it into a list first, but
    # it's to suit the rather odd use case here which is that we are adding
    # new classes to a module on the fly.

    rebuilt_items = collections.defaultdict(list)
    test_classes = set(item.parent for item in items)
    for test_class in test_classes:
        for sub_cls in plugin_base.generate_sub_tests(test_class.cls, test_class.parent.module):
            if sub_cls is not test_class.cls:
                list_ = rebuilt_items[test_class.cls]

                for inst in pytest.Class(sub_cls.__name__,
                                parent=test_class.parent.parent).collect():
                    list_.extend(inst.collect())

    newitems = []
    for item in items:
        if item.parent.cls in rebuilt_items:
            newitems.extend(rebuilt_items[item.parent.cls])
            rebuilt_items[item.parent.cls][:] = []
        else:
            newitems.append(item)

    # seems like the functions attached to a test class aren't sorted already?
    # is that true and why's that? (when using unittest, they're sorted)
    items[:] = sorted(newitems, key=lambda item: (
                                        item.parent.parent.parent.name,
                                        item.parent.parent.name,
                                        item.name
                                    )
                        )



def pytest_pycollect_makeitem(collector, name, obj):

    if inspect.isclass(obj) and plugin_base.want_class(obj):
        return pytest.Class(name, parent=collector)
    elif inspect.isfunction(obj) and \
            name.startswith("test_") and \
            isinstance(collector, pytest.Instance):
        return pytest.Function(name, parent=collector)
    else:
        return []

_current_class = None

def pytest_runtest_setup(item):
    # here we seem to get called only based on what we collected
    # in pytest_collection_modifyitems.   So to do class-based stuff
    # we have to tear that out.
    global _current_class

    if not isinstance(item, pytest.Function):
        return

    # ... so we're doing a little dance here to figure it out...
    if item.parent.parent is not _current_class:

        class_setup(item.parent.parent)
        _current_class = item.parent.parent

        # this is needed for the class-level, to ensure that the
        # teardown runs after the class is completed with its own
        # class-level teardown...
        item.parent.parent.addfinalizer(lambda: class_teardown(item.parent.parent))

    test_setup(item)

def pytest_runtest_teardown(item):
    # ...but this works better as the hook here rather than
    # using a finalizer, as the finalizer seems to get in the way
    # of the test reporting failures correctly (you get a bunch of
    # py.test assertion stuff instead)
    test_teardown(item)

def test_setup(item):
    plugin_base.before_test(item,
                item.parent.module.__name__, item.parent.cls, item.name)

def test_teardown(item):
    plugin_base.after_test(item)

def class_setup(item):
    plugin_base.start_test_class(item.cls)

def class_teardown(item):
    plugin_base.stop_test_class(item.cls)
