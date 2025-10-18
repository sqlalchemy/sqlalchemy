"""Provides the tox_parameters() utility, which generates parameterized
sections for nox tests, which include tags that indicate various combinations
of those parameters in such a way that it's somewhat similar to how
we were using the tox project; where individual dash-separated tags could
be added to add more specificity to the suite configuation, or omitting them
would fall back to defaults.


"""

from __future__ import annotations

import collections
import re
import sys
from typing import Any
from typing import Callable
from typing import Generator
from typing import Sequence

import nox

OUR_PYTHON = f"{sys.version_info.major}.{sys.version_info.minor}"


def tox_parameters(
    names: Sequence[str],
    token_lists: Sequence[Sequence[str]],
    *,
    base_tag: str | None = None,
    filter_: Callable[..., bool] | None = None,
    always_include_in_tag: Sequence[str] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    r"""Decorator to create a parameter/tagging structure for a nox session
    function that acts to a large degree like tox's generative environments.

    The output is a ``nox.parametrize()`` decorator that's built up from
    individual ``nox.param()`` instances.

    :param names: names of the parameters sent to the session function.
     These names go straight to the first argument of ``nox.parametrize()``
     and should all match argument names accepted by the decorated function
     (except for ``python``, which is optional).
    :param token_lists: a sequence of lists of values for each parameter.  a
     ``nox.param()`` will be created for the full product of these values,
     minus those filtered out using the ``filter_`` callable.   These tokens
     are used to create the args, tags, and ids of each ``nox.param()``.  The
     list of tags will be generated out including all values for a parameter
     joined by ``-``, as well as combinations that include a subset of those
     values, where the omitted elements of the tag are implicitly considered to
     match the "default" value, indicated by them being first in their
     collection (with the exception of "python", where the current python in
     use is the default). Additionally, values that start with an underscore
     are omitted from all ids and tags.   Values that refer to Python versions
     wlil be expanded to the full Python executable name when passed as
     arguments to the session function, which is currently a workaround to
     allow free-threaded python interpreters to be located.
    :param base_tag: optional tag that will be appended to all tags generated,
     e.g. if the decorator yields tags like ``python314-x86-windows``, a
     ``basetag`` value of ``all`` would yield the
     tag as ``python314-x86-windows-all``.
    :param filter\_: optional filtering function, must accept keyword arguments
     matching the names in ``names``.   Returns True or False indicating if
     a certain tag combination should be included.
    :param always_include_in_tag: list of names from ``names`` that indicate
     parameters that should always be part of all tags, and not be omitted
     as a "default"


    """

    PY_RE = re.compile(r"(?:python)?([234]\.\d+(t?))")

    def _is_py_version(token: str) -> bool:
        return bool(PY_RE.match(token))

    def _python_to_tag(token: str) -> str:
        m = PY_RE.match(token)
        if m:
            return f"py{m.group(1).replace('.', '')}"
        else:
            return token

    if always_include_in_tag:
        name_to_list = dict(zip(names, token_lists))
        must_be_present = [
            name_to_list[name] for name in always_include_in_tag
        ]
    else:
        must_be_present = None

    def _recur_param(
        prevtokens: list[str],
        prevtags: list[str],
        token_lists: Sequence[Sequence[str]],
    ) -> Generator[tuple[list[str], list[str], str], None, None]:

        if not token_lists:
            return

        tokens = token_lists[0]
        remainder = token_lists[1:]

        for i, token in enumerate(tokens):

            if _is_py_version(token):
                is_our_python = token == OUR_PYTHON
                tokentag = _python_to_tag(token)
                is_default_token = is_our_python
            else:
                is_our_python = False
                tokentag = token
                is_default_token = i == 0

            if is_our_python:
                our_python_tags = ["py"]
            else:
                our_python_tags = []

            if not tokentag.startswith("_"):
                tags = (
                    prevtags
                    + [tokentag]
                    + [tag + "-" + tokentag for tag in prevtags]
                    + our_python_tags
                )
            else:
                tags = prevtags + our_python_tags

            if remainder:
                for args, newtags, ids in _recur_param(
                    prevtokens + [token], tags, remainder
                ):
                    if not is_default_token:
                        newtags = [
                            t
                            for t in newtags
                            if tokentag in t or t in our_python_tags
                        ]

                    yield args, newtags, ids
            else:
                if not is_default_token:
                    newtags = [
                        t
                        for t in tags
                        if tokentag in t or t in our_python_tags
                    ]
                else:
                    newtags = tags

                if base_tag:
                    newtags = [t + f"-{base_tag}" for t in newtags]
                if must_be_present:
                    for t in list(newtags):
                        for required_tokens in must_be_present:
                            if not any(r in t for r in required_tokens):
                                newtags.remove(t)
                                break

                yield prevtokens + [token], newtags, "-".join(
                    _python_to_tag(t)
                    for t in prevtokens + [token]
                    if not t.startswith("_")
                )

    params = [
        nox.param(*args, tags=tags, id=ids)
        for args, tags, ids in _recur_param([], [], token_lists)
        if filter_ is None or filter_(**dict(zip(names, args)))
    ]

    # for p in params:
    #     print(f"PARAM {'-'.join(p.args)} TAGS {p.tags}")

    return nox.parametrize(names, params)


def extract_opts(posargs: list[str], *args: str) -> tuple[list[str], Any]:
    """Pop individual flag options from session.posargs.

    Returns a named tuple with the individual flag options indicated,
    as well the new posargs with those flags removed from the string list
    so that the posargs can be forwarded onto pytest.

    Basically if nox had an option for additional environmental flags that
    didn't require putting them after ``--``, we wouldn't need this, but this
    is probably more flexible.

    """
    underscore_args = [arg.replace("-", "_") for arg in args]
    return_tuple = collections.namedtuple("options", underscore_args)  # type: ignore  # noqa: E501

    look_for_args = {f"--{arg}": idx for idx, arg in enumerate(args)}
    return_args = [False for arg in args]

    def extract(arg: str) -> bool:
        if arg in look_for_args:
            return_args[look_for_args[arg]] = True
            return True
        else:
            return False

    return [arg for arg in posargs if not extract(arg)], return_tuple(
        *return_args
    )
