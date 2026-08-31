"""Tests for the decoding and sanitisation layer.

These matter more than they look. `name()` and `symbol()` are arbitrary strings
chosen by whoever deployed the token, so every value flowing through here is
hostile input from an untrusted party. The published CSV once contained 155 NUL
bytes because this was not defended -- `file(1)` classified the dataset as
binary rather than text.

Run: python -m unittest discover tests
No third-party dependencies.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scan import (  # noqa: E402
    balances_from_logs,
    csv_safe,
    decode_string,
    decode_uint,
    sanitize_text,
    topic_to_addr,
    ZERO_ADDR,
)


def abi_string(s: str) -> str:
    """ABI-encode a string the way eth_call returns it: offset, length, data."""
    raw = s.encode()
    body = raw + b"\x00" * ((32 - len(raw) % 32) % 32)
    return ("0x"
            + (32).to_bytes(32, "big").hex()
            + len(raw).to_bytes(32, "big").hex()
            + body.hex())


class TestSanitizeText(unittest.TestCase):
    """Token names are attacker-controlled. Everything here is a real risk."""

    def test_strips_nul_bytes(self):
        # The actual bug: a token whose name was 31 NUL bytes turned the
        # published CSV into a binary file.
        self.assertIsNone(sanitize_text("\x00" * 31))
        self.assertEqual(sanitize_text("AB\x00CD"), "ABCD")

    def test_strips_ansi_escapes(self):
        # A name carrying escape sequences can rewrite a terminal's output,
        # letting a token misrepresent itself in any CLI that prints it.
        self.assertEqual(sanitize_text("\x1b[31mDANGER\x1b[0m"), "[31mDANGER[0m")
        self.assertNotIn("\x1b", sanitize_text("\x1b[2J") or "")

    def test_strips_bidi_overrides_and_c1(self):
        self.assertEqual(sanitize_text("safe\x85name"), "safename")
        self.assertEqual(sanitize_text("a\x7fb"), "ab")

    def test_keeps_tab_and_normal_unicode(self):
        self.assertEqual(sanitize_text("Lady Marian"), "Lady Marian")
        self.assertEqual(sanitize_text("Cash Cat \U0001f431"), "Cash Cat \U0001f431")

    def test_caps_length(self):
        self.assertEqual(len(sanitize_text("A" * 5000)), 200)

    def test_empty_becomes_none(self):
        # Distinguishing "no name" from "empty string" keeps the CSV honest.
        self.assertIsNone(sanitize_text(""))
        self.assertIsNone(sanitize_text("   "))
        self.assertIsNone(sanitize_text(None))


class TestCsvSafe(unittest.TestCase):
    """Spreadsheet formula injection: a name starting = + - @ executes on open."""

    def test_neutralises_formula_leaders(self):
        for bad in ("=1+1", "+1", "-1", "@hooddeploys"):
            self.assertTrue(csv_safe(bad).startswith("'"),
                            f"{bad!r} was not neutralised")

    def test_real_case_from_the_dataset(self):
        # This token actually exists on chain, at block 1,144,800.
        self.assertEqual(csv_safe("@hooddeploys"), "'@hooddeploys")

    def test_leaves_ordinary_values_alone(self):
        self.assertEqual(csv_safe("MARIAN"), "MARIAN")
        self.assertEqual(csv_safe("Lady Marian"), "Lady Marian")

    def test_passes_through_non_strings(self):
        self.assertEqual(csv_safe(58539), 58539)
        self.assertIsNone(csv_safe(None))


class TestDecodeString(unittest.TestCase):
    def test_dynamic_string(self):
        self.assertEqual(decode_string(abi_string("Lady Marian")), "Lady Marian")

    def test_bytes32_fallback(self):
        # Older MKR-style tokens return a padded bytes32 rather than a string.
        raw = b"WETH" + b"\x00" * 28
        self.assertEqual(decode_string("0x" + raw.hex()), "WETH")

    def test_sanitises_on_the_way_out(self):
        # Decoding must not be a hole around the sanitiser.
        self.assertEqual(decode_string(abi_string("BAD\x00NAME")), "BADNAME")

    def test_malformed_input_returns_none(self):
        for bad in (None, "0x", "0x0", "0xzz", ""):
            self.assertIsNone(decode_string(bad))


class TestDecodeUint(unittest.TestCase):
    def test_values(self):
        self.assertEqual(decode_uint("0x" + (10 ** 27).to_bytes(32, "big").hex()),
                         10 ** 27)
        self.assertEqual(decode_uint("0x0"), 0)

    def test_none_for_missing(self):
        # A contract with no totalSupply() is not an ERC-20; that must be
        # distinguishable from a supply of zero.
        self.assertIsNone(decode_uint(None))
        self.assertIsNone(decode_uint("0x"))
        self.assertIsNone(decode_uint("0xnothex"))


class TestTopicToAddr(unittest.TestCase):
    def test_extracts_low_20_bytes(self):
        topic = "0x" + "00" * 12 + "937933e11ad6307ae0d8b8115986e91734be2d5c"
        self.assertEqual(topic_to_addr(topic),
                         "0x937933e11ad6307ae0d8b8115986e91734be2d5c")

    def test_short_input(self):
        self.assertIsNone(topic_to_addr("0x1234"))
        self.assertIsNone(topic_to_addr(None))


class TestBalancesFromLogs(unittest.TestCase):
    """Holder counts are replayed from Transfer logs rather than trusted."""

    @staticmethod
    def transfer(frm, to, value):
        pad = lambda a: "0x" + "00" * 12 + a[2:]
        return {"topics": ["0xddf", pad(frm), pad(to)],
                "data": "0x" + value.to_bytes(32, "big").hex()}

    def test_mint_then_transfer(self):
        a = "0x" + "11" * 20
        b = "0x" + "22" * 20
        bal = balances_from_logs([
            self.transfer(ZERO_ADDR, a, 1000),   # mint
            self.transfer(a, b, 400),
        ])
        self.assertEqual(bal[a], 600)
        self.assertEqual(bal[b], 400)
        # the zero address is never counted as a holder
        self.assertNotIn(ZERO_ADDR, bal)

    def test_ignores_erc721_shaped_logs(self):
        # ERC-721 shares the Transfer topic but has 4 topics and empty data.
        bal = balances_from_logs([{"topics": ["0xddf", "0x1", "0x2", "0x3"],
                                   "data": "0x"}])
        self.assertEqual(bal, {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
