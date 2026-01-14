# engine/processors.py
# Copyright (C) 2010-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
# Copyright (C) 2010 Gaetan de Menten gdementen@gmail.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""defines generic type conversion functions, as used in bind and result
processors.

They all share one common characteristic: None is passed through unchanged.

"""
from __future__ import annotations

import datetime
from typing import Callable
from typing import Optional
from typing import Pattern
from typing import TypeVar
from typing import Union

from ._processors_cy import int_to_boolean as int_to_boolean  # noqa: F401
from ._processors_cy import str_to_date as str_to_date  # noqa: F401
from ._processors_cy import str_to_datetime as str_to_datetime  # noqa: F401
from ._processors_cy import str_to_time as str_to_time  # noqa: F401
from ._processors_cy import to_float as to_float  # noqa: F401
from ._processors_cy import to_str as to_str  # noqa: F401

if True:
    from ._processors_cy import (  # noqa: F401
        to_decimal_processor_factory as to_decimal_processor_factory,
    )


_DT = TypeVar(
    "_DT", bound=Union[datetime.datetime, datetime.time, datetime.date]
)


def str_to_datetime_processor_factory(
    regexp: Pattern[str], type_: Callable[..., _DT]
) -> Callable[[Optional[str]], Optional[_DT]]:
    rmatch = regexp.match
    # Even on python2.6 datetime.strptime is both slower than this code
    # and it does not support microseconds.
    has_named_groups = bool(regexp.groupindex)

    def process(value: Optional[str]) -> Optional[_DT]:
        if value is None:
            return None
        else:
            try:
                m = rmatch(value)
            except TypeError as err:
                raise ValueError(
                    "Couldn't parse %s string '%r' "
                    "- value is not a string." % (type_.__name__, value)
                ) from err

            if m is None:
                raise ValueError(
                    "Couldn't parse %s string: "
                    "'%s'" % (type_.__name__, value)
                )
            if has_named_groups:
                groups = m.groupdict(0)
                return type_(
                    **dict(
                        list(
                            zip(
                                iter(groups.keys()),
                                list(map(int, iter(groups.values()))),
                            )
                        )
                    )
                )
            else:
                return type_(*list(map(int, m.groups(0))))

    return process
