from sqlalchemy.testing import fixtures

from sqlalchemy.dialects.postgresql import BitString
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertions import is_false
from sqlalchemy.testing.assertions import is_true
from sqlalchemy.testing.assertions import assert_raises


class BitStringTests(fixtures.TestBase):

    def test_ctor(self):
        x = BitString("1110111")
        eq_(str(x), "1110111")
        eq_(int(x), 119)

        eq_(BitString("111"), BitString("111"))
        is_false(BitString("111") == "111")

        eq_(hash(BitString("011")), hash(BitString("011")))
        is_false(hash(BitString("011")) == hash("011"))

        eq_(BitString("011")[1], BitString("1"))

    def test_int_conversion(self):
        assert_raises(ValueError, lambda: BitString.from_int(127, length=6))

        eq_(BitString.from_int(127, length=8), BitString("01111111"))
        eq_(int(BitString.from_int(127, length=8)), 127)

        eq_(BitString.from_int(119, length=10), BitString("0001110111"))
        eq_(int(BitString.from_int(119, length=10)), 119)

    def test_bytes_conversion(self):
        eq_(BitString.from_bytes(b"\x01"), BitString("0000001"))
        eq_(BitString.from_bytes(b"\x01", 4), BitString("00000001"))

        eq_(BitString.from_bytes(b"\xaf\x04"), BitString("101011110010"))
        eq_(
            BitString.from_bytes(b"\xaf\x04", 12),
            BitString("0000101011110010"),
        )
        assert_raises(
            ValueError, lambda: BitString.from_bytes(b"\xaf\x04", 4), 1
        )

    def test_get_set_bit(self):
        eq_(BitString("1010").get_bit(2), "1")
        eq_(BitString("0101").get_bit(2), "0")
        assert_raises(IndexError, lambda: BitString("0").get_bit(1))

        eq_(BitString("0101").set_bit(3, "0"), BitString("0100"))
        eq_(BitString("0101").set_bit(3, "1"), BitString("0101"))
        assert_raises(IndexError, lambda: BitString("1111").set_bit(5, "1"))

    def test_string_methods(self):

        # Which of these methods should be overridden to produce BitStrings?
        eq_(BitString("111").center(8), "  111   ")

        eq_(BitString("0101").ljust(8), "0101    ")
        eq_(BitString("0110").rjust(8), "    0110")

        eq_(BitString("01100").lstrip(), BitString("1100"))
        eq_(BitString("01100").rstrip(), BitString("011"))
        eq_(BitString("01100").strip(), BitString("11"))

        eq_(BitString("11100").removeprefix("111"), BitString("00"))
        eq_(BitString("11100").removeprefix("0"), BitString("11100"))

        eq_(BitString("11100").removesuffix("10"), BitString("11100"))
        eq_(BitString("11100").removesuffix("00"), BitString("111"))

        eq_(
            BitString("010101011").replace("0101", "11", 1),
            BitString("1101011"),
        )
        eq_(
            BitString("01101101").split("1", 2),
            [BitString("0"), BitString(""), BitString("01101")],
        )

        eq_(BitString("0110").split("11"), [BitString("0"), BitString("0")])
        eq_(BitString("111").zfill(8), BitString("00000111"))

    def test_str_ops(self):
        is_true("1" in BitString("001"))
        is_true("0" in BitString("110"))
        is_false("1" in BitString("000"))

        is_true("001" in BitString("01001"))
        is_true(BitString("001") in BitString("01001"))
        is_false(BitString("000") in BitString("01001"))

        eq_(BitString("010") + "001", BitString("010001"))
        eq_("001" + BitString("010"), BitString("001010"))

    def test_bitwise_ops(self):
        eq_(~BitString("0101"), BitString("1010"))
        eq_(BitString("010") & BitString("011"), BitString("010"))
        eq_(BitString("010") | BitString("011"), BitString("011"))
        eq_(BitString("010") ^ BitString("011"), BitString("001"))

        eq_(BitString("001100") << 2, BitString("110000"))
        eq_(BitString("001100") >> 2, BitString("000011"))
