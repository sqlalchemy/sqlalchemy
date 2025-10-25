from sqlalchemy import testing
from sqlalchemy.dialects.postgresql import BitString
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertions import is_
from sqlalchemy.testing.assertions import is_false
from sqlalchemy.testing.assertions import is_not
from sqlalchemy.testing.assertions import is_true


class BitStringTests(fixtures.TestBase):

    @testing.combinations(
        lambda: BitString("111") == BitString("111"),
        lambda: BitString("111") == "111",
        lambda: BitString("111") != BitString("110"),
        lambda: BitString("111") != "110",
        lambda: hash(BitString("011")) == hash(BitString("011")),
        lambda: hash(BitString("011")) == hash("011"),
        lambda: BitString("011")[1] == BitString("1"),
        lambda: BitString("010") > BitString("001"),
        lambda: "010" > BitString("001"),
        lambda: "011" <= BitString("011"),
        lambda: "011" <= BitString("101"),
    )
    def test_comparisons(self, case):
        is_true(case())

    def test_sorting(self):
        eq_(
            sorted([BitString("110"), BitString("010"), "111", "101"]),
            [BitString("010"), "101", BitString("110"), "111"],
        )

    def test_str_conversion(self):
        x = BitString("1110111")
        eq_(str(x), "1110111")

        assert_raises(ValueError, lambda: BitString("1246"))

    def test_same_instance_returned(self):
        x = BitString("1110111")
        y = BitString("1110111")
        z = BitString(x)

        eq_(x, y)
        eq_(x, z)

        is_not(x, y)
        is_(x, z)

    @testing.combinations(
        (0, 0, BitString("")),
        (0, 1, BitString("0")),
        (1, 1, BitString("1")),
        (1, 0, ValueError),
        (1, -1, ValueError),
        (2, 1, ValueError),
        (-1, 4, ValueError),
        (1, 4, BitString("0001")),
        (1, 10, BitString("0000000001")),
        (127, 8, BitString("01111111")),
        (127, 10, BitString("0001111111")),
        (1404, 8, ValueError),
        (1404, 12, BitString("010101111100")),
        argnames="source, bitlen, result_or_error",
    )
    def test_int_conversion(self, source, bitlen, result_or_error):
        if isinstance(result_or_error, type):
            assert_raises(
                result_or_error, lambda: BitString.from_int(source, bitlen)
            )
            return

        result = result_or_error

        bits = BitString.from_int(source, bitlen)
        eq_(bits, result)
        eq_(int(bits), source)

    @testing.combinations(
        (b"", -1, BitString("")),
        (b"", 4, BitString("0000")),
        (b"\x00", 1, BitString("0")),
        (b"\x01", 1, BitString("1")),
        (b"\x01", 4, BitString("0001")),
        (b"\x01", 10, BitString("0000000001")),
        (b"\x01", -1, BitString("00000001")),
        (b"\xff", 10, BitString("0011111111")),
        (b"\xaf\x04", 8, ValueError),
        (b"\xaf\x04", 16, BitString("1010111100000100")),
        (b"\xaf\x04", 20, BitString("00001010111100000100")),
        argnames="source, bitlen, result_or_error",
    )
    def test_bytes_conversion(self, source, bitlen, result_or_error):
        if isinstance(result_or_error, type):
            assert_raises(
                result_or_error,
                lambda: BitString.from_bytes(source, length=bitlen),
            )
            return
        result = result_or_error

        bits = BitString.from_bytes(source, bitlen)
        eq_(bits, result)

        # Expecting a roundtrip conversion in this case is nonsensical
        if source == b"" and bitlen > 0:
            return
        eq_(bits.to_bytes(len(source)), source)

    def test_get_set_bit(self):
        eq_(BitString("1010").get_bit(2), "1")
        eq_(BitString("0101").get_bit(2), "0")
        assert_raises(IndexError, lambda: BitString("0").get_bit(1))

        eq_(BitString("0101").set_bit(3, "0"), BitString("0100"))
        eq_(BitString("0101").set_bit(3, "1"), BitString("0101"))
        assert_raises(IndexError, lambda: BitString("1111").set_bit(5, "1"))

    def test_string_methods(self):

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

    def test_string_operators(self):
        is_true("1" in BitString("001"))
        is_true("0" in BitString("110"))
        is_false("1" in BitString("000"))

        is_true("001" in BitString("01001"))
        is_true(BitString("001") in BitString("01001"))
        is_false(BitString("000") in BitString("01001"))

        eq_(BitString("010") + "001", BitString("010001"))
        eq_("001" + BitString("010"), BitString("001010"))

    def test_bitwise_operators(self):
        eq_(~BitString("0101"), BitString("1010"))
        eq_(BitString("010") & BitString("011"), BitString("010"))
        eq_(BitString("010") | BitString("011"), BitString("011"))
        eq_(BitString("010") ^ BitString("011"), BitString("001"))

        eq_(BitString("001100") << 2, BitString("110000"))
        eq_(BitString("001100") >> 2, BitString("000011"))
