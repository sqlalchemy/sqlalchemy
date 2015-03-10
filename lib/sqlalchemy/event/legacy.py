# event/legacy.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Routines to handle adaption of legacy call signatures,
generation of deprecation notes and docstrings.

"""

from .. import util


def _legacy_signature(since, argnames, converter=None):
    def leg(fn):
        if not hasattr(fn, '_legacy_signatures'):
            fn._legacy_signatures = []
        fn._legacy_signatures.append((since, argnames, converter))
        return fn
    return leg


def _wrap_fn_for_legacy(dispatch_collection, fn, argspec):
    for since, argnames, conv in dispatch_collection.legacy_signatures:
        if argnames[-1] == "**kw":
            has_kw = True
            argnames = argnames[0:-1]
        else:
            has_kw = False

        if len(argnames) == len(argspec.args) \
                and has_kw is bool(argspec.keywords):

            if conv:
                assert not has_kw

                def wrap_leg(*args):
                    return fn(*conv(*args))
            else:
                def wrap_leg(*args, **kw):
                    argdict = dict(zip(dispatch_collection.arg_names, args))
                    args = [argdict[name] for name in argnames]
                    if has_kw:
                        return fn(*args, **kw)
                    else:
                        return fn(*args)
            return wrap_leg
    else:
        return fn


def _indent(text, indent):
    return "\n".join(
        indent + line
        for line in text.split("\n")
    )


def _standard_listen_example(dispatch_collection, sample_target, fn):
    example_kw_arg = _indent(
        "\n".join(
            "%(arg)s = kw['%(arg)s']" % {"arg": arg}
            for arg in dispatch_collection.arg_names[0:2]
        ),
        "    ")
    if dispatch_collection.legacy_signatures:
        current_since = max(since for since, args, conv
                            in dispatch_collection.legacy_signatures)
    else:
        current_since = None
    text = (
        "from sqlalchemy import event\n\n"
        "# standard decorator style%(current_since)s\n"
        "@event.listens_for(%(sample_target)s, '%(event_name)s')\n"
        "def receive_%(event_name)s("
        "%(named_event_arguments)s%(has_kw_arguments)s):\n"
        "    \"listen for the '%(event_name)s' event\"\n"
        "\n    # ... (event handling logic) ...\n"
    )

    if len(dispatch_collection.arg_names) > 3:
        text += (

            "\n# named argument style (new in 0.9)\n"
            "@event.listens_for("
            "%(sample_target)s, '%(event_name)s', named=True)\n"
            "def receive_%(event_name)s(**kw):\n"
            "    \"listen for the '%(event_name)s' event\"\n"
            "%(example_kw_arg)s\n"
            "\n    # ... (event handling logic) ...\n"
        )

    text %= {
        "current_since": " (arguments as of %s)" %
        current_since if current_since else "",
        "event_name": fn.__name__,
        "has_kw_arguments": ", **kw" if dispatch_collection.has_kw else "",
        "named_event_arguments": ", ".join(dispatch_collection.arg_names),
        "example_kw_arg": example_kw_arg,
        "sample_target": sample_target
    }
    return text


def _legacy_listen_examples(dispatch_collection, sample_target, fn):
    text = ""
    for since, args, conv in dispatch_collection.legacy_signatures:
        text += (
            "\n# legacy calling style (pre-%(since)s)\n"
            "@event.listens_for(%(sample_target)s, '%(event_name)s')\n"
            "def receive_%(event_name)s("
            "%(named_event_arguments)s%(has_kw_arguments)s):\n"
            "    \"listen for the '%(event_name)s' event\"\n"
            "\n    # ... (event handling logic) ...\n" % {
                "since": since,
                "event_name": fn.__name__,
                "has_kw_arguments": " **kw"
                if dispatch_collection.has_kw else "",
                "named_event_arguments": ", ".join(args),
                "sample_target": sample_target
            }
        )
    return text


def _version_signature_changes(dispatch_collection):
    since, args, conv = dispatch_collection.legacy_signatures[0]
    return (
        "\n.. versionchanged:: %(since)s\n"
        "    The ``%(event_name)s`` event now accepts the \n"
        "    arguments ``%(named_event_arguments)s%(has_kw_arguments)s``.\n"
        "    Listener functions which accept the previous argument \n"
        "    signature(s) listed above will be automatically \n"
        "    adapted to the new signature." % {
            "since": since,
            "event_name": dispatch_collection.name,
            "named_event_arguments": ", ".join(dispatch_collection.arg_names),
            "has_kw_arguments": ", **kw" if dispatch_collection.has_kw else ""
        }
    )


def _augment_fn_docs(dispatch_collection, parent_dispatch_cls, fn):
    header = ".. container:: event_signatures\n\n"\
        "     Example argument forms::\n"\
        "\n"

    sample_target = getattr(parent_dispatch_cls, "_target_class_doc", "obj")
    text = (
        header +
        _indent(
            _standard_listen_example(
                dispatch_collection, sample_target, fn),
            " " * 8)
    )
    if dispatch_collection.legacy_signatures:
        text += _indent(
            _legacy_listen_examples(
                dispatch_collection, sample_target, fn),
            " " * 8)

        text += _version_signature_changes(dispatch_collection)

    return util.inject_docstring_text(fn.__doc__,
                                      text,
                                      1
                                      )
