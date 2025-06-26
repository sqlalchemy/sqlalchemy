# dialects/postgresql/bitstring.py
# Copyright (C) 2013-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
from __future__ import annotations

import functools
import math
from typing import Any
from typing import cast
from typing import Literal
from typing import SupportsIndex


@functools.total_ordering
class BitString(str):
    """Represent a PostgreSQL bit string in python"""

    _DIGITS = frozenset("01")

    def __new__(cls, _value: str, _check: bool = True) -> BitString:
        do_check = not isinstance(_value, BitString) or _check
        if do_check and cls._DIGITS.union(_value) > cls._DIGITS:
            raise ValueError("BitString must only contain '0' and '1' chars")
        return super().__new__(cls, _value)

    @classmethod
    def from_int(cls, value: int, length: int) -> BitString:
        """
        Returns a BitString consisting of the bits in the integer ``value``.
        A ``ValueError`` is raised if ``value`` is not a non-negative integer.

        If the provided ``value`` can not be represented in a bit string
        of at most ``length``, a ``ValueError`` will be raised. The bitstring
        will be padded on the left by ``'0'`` to bits to produce a
        bitstring of the desired length.
        """
        if value < 0:
            raise ValueError("value must be non-negative")
        if length < 0:
            raise ValueError("length must be non-negative")

        template_str = f"{{0:0{length}b}}" if length > 0 else ""
        r = template_str.format(value)

        if (length == 0 and value > 0) or len(r) > length:
            raise ValueError(
                f"Cannot encode {value} as a BitString of length {length}"
            )

        return cls(r)

    @classmethod
    def from_bytes(cls, value: bytes, length: int = -1) -> BitString:
        """
        Returns a ``BitString`` consisting of the bits in the given ``value``
        bytes.

        If ``length`` is provided, then the length of the provided string
        will be exactly ``length``, with ``'0'`` bits inserted at the left of
        the string in order to produce a value of the required length.
        If, the bits obtained by omitting the leading `0` bits of ``value``
        cannot be represented in a string of this length, then a ``ValueError``
        will be raised.
        """
        str_v: str = "".join(f"{int(c):08b}" for c in value)
        if length >= 0:
            str_v = str_v.lstrip("0")

            if len(str_v) > length:
                raise ValueError(
                    f"Cannot encode {value!r} as a BitString of "
                    f"length {length}"
                )
            str_v = str_v.zfill(length)

        return cls(str_v)

    def get_bit(self, index: int) -> Literal["0", "1"]:
        """
        Returns the value of the flag at the given index

        e.g.::

            BitString("0101").get_flag(4) == "1"
        """
        return cast(Literal["0", "1"], super().__getitem__(index))

    @property
    def bit_length(self) -> int:
        return len(self)

    @property
    def octet_length(self) -> int:
        return math.ceil(len(self) / 8)

    def has_bit(self, index: int) -> bool:
        return self.get_bit(index) == "1"

    def set_bit(
        self, index: int, value: bool | int | Literal["0", "1"]
    ) -> BitString:
        """
        Set the bit at index to the given value.

        If value is an int, then it is considered to be '1' iff nonzero.
        """
        if index < 0 or index >= len(self):
            raise IndexError("BitString index out of range")

        if isinstance(value, (bool, int)):
            value = "1" if value else "0"

        if self.get_bit(index) == value:
            return self

        return BitString(
            "".join([self[:index], value, self[index + 1 :]]), False
        )

    def lstrip(self, char: str | None = None) -> BitString:
        """
        Returns a copy of the BitString with leading characters removed.

        If omitted or None, 'chars' defaults '0'

        e.g.
            BitString('00010101000').lstrip() === BitString('00010101')
            BitString('11110101111').lstrip('1') === BitString('1111010')
        """
        if char is None:
            char = "0"
        return BitString(super().lstrip(char), False)

    def rstrip(self, char: str | None = "0") -> BitString:
        """
        Returns a copy of the BitString with trailing characters removed.

        If omitted or None, 'char' defaults to "0"

        e.g.
            BitString('00010101000').rstrip() === BitString('10101000')
            BitString('11110101111').rstrip('1') === BitString('10101111')
        """
        if char is None:
            char = "0"
        return BitString(super().rstrip(char), False)

    def strip(self, char: str | None = "0") -> BitString:
        """
        Returns a copy of the BitString with both leading and trailing
        characters removed.
        If ommitted or None, char defaults to "0"

        e.g.
            BitString('00010101000').rstrip() === BitString('10101')
            BitString('11110101111').rstrip('1') === BitString('1010')
        """
        if char is None:
            char = "0"
        return BitString(super().strip(char))

    def removeprefix(self, prefix: str, /) -> BitString:
        return BitString(super().removeprefix(prefix), False)

    def removesuffix(self, suffix: str, /) -> BitString:
        return BitString(super().removesuffix(suffix), False)

    def replace(
        self,
        old: str,
        new: str,
        count: SupportsIndex = -1,
    ) -> BitString:
        new = BitString(new)
        return BitString(super().replace(old, new, count), False)

    def split(
        self,
        sep: str | None = None,
        maxsplit: SupportsIndex = -1,
    ) -> list[str]:
        return [BitString(word) for word in super().split(sep, maxsplit)]

    def zfill(self, width: SupportsIndex) -> BitString:
        return BitString(super().zfill(width), False)

    def __repr__(self) -> str:
        return f'BitString("{self.__str__()}")'

    def __int__(self) -> int:
        return int(self, 2) if self else 0

    def to_bytes(self, length: int = -1) -> bytes:
        return int(self).to_bytes(length if length >= 0 else self.octet_length)

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __lt__(self, o: object) -> bool:
        if isinstance(o, BitString):
            return super().__lt__(o)
        return NotImplemented

    def __eq__(self, o: object) -> bool:
        return isinstance(o, BitString) and super().__eq__(o)

    def __hash__(self) -> int:
        return hash(BitString) ^ super().__hash__()

    def __getitem__(
        self, key: SupportsIndex | slice[Any, Any, Any]
    ) -> BitString:
        return BitString(super().__getitem__(key), False)

    def __add__(self, o: str) -> BitString:
        """Return self + o"""
        if not isinstance(o, str):
            raise TypeError(
                f"Can only concatenate str (not '{type(self)}') to BitString"
            )
        return BitString("".join([self, o]))

    def __radd__(self, o: str) -> BitString:
        if not isinstance(o, str):
            raise TypeError(
                f"Can only concatenate str (not '{type(self)}') to BitString"
            )
        return BitString("".join([o, self]))

    def __lshift__(self, amount: int) -> BitString:
        """
        Shifts each the bitstring to the left by the given amount.
        String length is preserved.

        e.g.::
            BitString("000101") << 1 == BitString("001010")
        """
        return BitString(
            "".join([self, *("0" for _ in range(amount))])[-len(self) :], False
        )

    def __rshift__(self, amount: int) -> BitString:
        """
        Shifts each bit in the bitstring to the right by the given amount.
        String length is preserved.

        e.g.::
            BitString("101") >> 1 == BitString("010")
        """
        return BitString(self[:-amount], False).zfill(width=len(self))

    def __invert__(self) -> BitString:
        """
        Inverts (~) each bit in the bitstring

        e.g.::
            ~BitString("01010") == BitString("10101")
        """
        return BitString("".join("1" if x == "0" else "0" for x in self))

    def __and__(self, o: str) -> BitString:
        """
        Performs a bitwise and (``&``) with the given operand.
        A ``ValueError`` is raised if the operand is not the same length.

        e.g.::
            BitString("011") & BitString("011") == BitString("010")
        """

        if not isinstance(o, str):
            return NotImplemented
        o = BitString(o)
        if len(self) != len(o):
            raise ValueError("Operands must be the same length")

        return BitString(
            "".join(
                "1" if (x == "1" and y == "1") else "0"
                for x, y in zip(self, o)
            ),
            False,
        )

    def __or__(self, o: str) -> BitString:
        """
        Performs a bitwise or (``|``) with the given operand.
        A ``ValueError`` is raised if the operand is not the same length.

        e.g.::
            BitString("011") | BitString("010") == BitString("011")
        """
        if not isinstance(o, str):
            return NotImplemented

        if len(self) != len(o):
            raise ValueError("Operands must be the same length")

        o = BitString(o)
        return BitString(
            "".join(
                "1" if (x == "1" or y == "1") else "0"
                for (x, y) in zip(self, o)
            ),
            False,
        )

    def __xor__(self, o: str) -> BitString:
        """
        Performs a bitwise xor (``^``) with the given operand.
        A ``ValueError`` is raised if the operand is not the same length.

        e.g.::
            BitString("011") ^ BitString("010") == BitString("001")
        """

        if not isinstance(o, BitString):
            return NotImplemented

        if len(self) != len(o):
            raise ValueError("Operands must be the same length")

        return BitString(
            "".join(
                (
                    "1"
                    if ((x == "1" and y == "0") or (x == "0" and y == "1"))
                    else "0"
                )
                for (x, y) in zip(self, o)
            ),
            False,
        )

    __rand__ = __and__
    __ror__ = __or__
    __rxor__ = __xor__
