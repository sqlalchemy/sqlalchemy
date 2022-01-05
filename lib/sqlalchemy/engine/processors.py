# sqlalchemy/processors.py
# Copyright (C) 2010-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
# Copyright (C) 2010 Gaetan de Menten gdementen@gmail.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""defines generic type conversion functions, as used in bind and result
processors.

They all share one common characteristic: None is passed through unchanged.

"""
from ._py_processors import str_to_datetime_processor_factory  # noqa

try:
    from sqlalchemy.cyextension.processors import (
        DecimalResultProcessor,
    )  # noqa
    from sqlalchemy.cyextension.processors import int_to_boolean  # noqa
    from sqlalchemy.cyextension.processors import str_to_date  # noqa
    from sqlalchemy.cyextension.processors import str_to_datetime  # noqa
    from sqlalchemy.cyextension.processors import str_to_time  # noqa
    from sqlalchemy.cyextension.processors import to_float  # noqa
    from sqlalchemy.cyextension.processors import to_str  # noqa

    def to_decimal_processor_factory(target_class, scale):
        # Note that the scale argument is not taken into account for integer
        # values in the C implementation while it is in the Python one.
        # For example, the Python implementation might return
        # Decimal('5.00000') whereas the C implementation will
        # return Decimal('5'). These are equivalent of course.
        return DecimalResultProcessor(target_class, "%%.%df" % scale).process

except ImportError:
    from ._py_processors import int_to_boolean  # noqa
    from ._py_processors import str_to_date  # noqa
    from ._py_processors import str_to_datetime  # noqa
    from ._py_processors import str_to_time  # noqa
    from ._py_processors import to_decimal_processor_factory  # noqa
    from ._py_processors import to_float  # noqa
    from ._py_processors import to_str  # noqa
