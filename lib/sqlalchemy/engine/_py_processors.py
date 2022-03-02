# sqlalchemy/processors.py
# Copyright (C) 2010-2022 the SQLAlchemy authors and contributors
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
from decimal import Decimal
import re
import typing
from typing import Any
from typing import Callable
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import Union

_DT = TypeVar(
    "_DT", bound=Union[datetime.datetime, datetime.time, datetime.date]
)


def str_to_datetime_processor_factory(
    regexp: typing.Pattern[str], type_: Callable[..., _DT]
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


def to_decimal_processor_factory(
    target_class: Type[Decimal], scale: int
) -> Callable[[Optional[float]], Optional[Decimal]]:
    fstring = "%%.%df" % scale

    def process(value: Optional[float]) -> Optional[Decimal]:
        if value is None:
            return None
        else:
            return target_class(fstring % value)

    return process


def to_float(value: Optional[Union[int, float]]) -> Optional[float]:
    if value is None:
        return None
    else:
        return float(value)


def to_str(value: Optional[Any]) -> Optional[str]:
    if value is None:
        return None
    else:
        return str(value)


def int_to_boolean(value: Optional[int]) -> Optional[bool]:
    if value is None:
        return None
    else:
        return bool(value)


DATETIME_RE = re.compile(r"(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?")
TIME_RE = re.compile(r"(\d+):(\d+):(\d+)(?:\.(\d+))?")
DATE_RE = re.compile(r"(\d+)-(\d+)-(\d+)")

str_to_datetime = str_to_datetime_processor_factory(
    DATETIME_RE, datetime.datetime
)
str_to_time = str_to_datetime_processor_factory(TIME_RE, datetime.time)
str_to_date = str_to_datetime_processor_factory(DATE_RE, datetime.date)
