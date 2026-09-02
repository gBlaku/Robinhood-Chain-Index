"""Tests for on-demand (live) launcher attribution.

The index is a contiguous prefix; the chain is not. Discovery reaches block
22,583,328 while the chain head is past 52,000,000, so most tokens anyone would
actually look up are outside the index entirely. `resolve_launcher_live` answers
for those by going to the chain, and these tests pin the two properties that
make that safe to do:

  1. it never answers "unknown" when the chain can answer, and
  2. it never leaks an ad-hoc lookup into the published dataset.

(2) is the subtle one. `export` selects on `first_tx_from IS NOT NULL AND
is_erc20 = 1` with no block bound -- correct only because batch attribution
stops cleanly. Writing live results into `token` would put scattered,
metadata-less rows from block 20M into a CSV whose stated coverage is blocks
0 to 9,000,000. Hence a separate table, and a regression test for it.

Run: python -m unittest discover tests
No network, no third-party dependencies.
"""

import os
import sqlite3
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import scan  # noqa: E402
from scan import (  # noqa: E402
    RpcError,
    SCHEMA,
    first_mint_tx_live,
    resolve_launcher_live,
)

TOKEN = "0x" + "ab" * 20
LAUNCHER = "0x" + "cd" * 20
LAUNCHPAD = "0x" + "ef" * 20
TXH = "0x" + "11" * 32

# The condition cmd_export uses to choose rows for the published CSV.
EXPORT_WHERE = ("SELECT COUNT(*) FROM token WHERE first_tx_from IS NOT NULL "
                "AND is_erc20 = 1")


def abi_string(s):
    raw = s.encode()
    body = raw + b"\x00" * ((32 - len(raw) % 32) % 32)
    return ("0x" + (32).to_bytes(32, "big").hex()
            + len(raw).to_bytes(32, "big").hex() + body.hex())


def abi_uint(n):
    return "0x" + int(n).to_bytes(32, "big").hex()


def mint_log(block, log_index=0, tx=TXH):
    return {"address": TOKEN, "blockNumber": hex(block),
            "logIndex": hex(log_index), "transactionHash": tx,
            "topics": [scan.TRANSFER_TOPIC, scan.ZERO_TOPIC, "0x0"],
            "data": "0x" + "00" * 32}


class StubRpc:
    """Records every method called so tests can assert on call count."""

    def __init__(self, logs=None, tx=None, meta=True, fail=None):
        self.logs, self.tx, self.meta, self.fail = logs, tx, meta, fail
        self.calls = []

    def call(self, method, params, retries=8, timeout=120):
        self.calls.append(method)
        if self.fail:
            raise RpcError(self.fail)
        if method == "eth_getLogs":
            return self.logs
        if method == "eth_getTransactionByHash":
            return self.tx
        raise AssertionError(f"unexpected method {method}")

    def safe_batch(self, calls):
        # probe_token issues 4 eth_calls: totalSupply, name, symbol, decimals.
        self.calls.append("probe")
        if not self.meta:
            return [None, None, None, None]
        return [abi_uint(10 ** 24), abi_string("Flying Ketamin Cat"),
                abi_string("ketcat"), abi_uint(18)]


def fresh_db():
    con = sqlite3.connect(":memory:")
    con.executescript(SCHEMA)
    return con


class TestFirstMintTxLive(unittest.TestCase):
    def test_picks_earliest_log_not_first_returned(self):
        # Ordering is the endpoint's choice, so the caller must not assume it.
        rpc = StubRpc(logs=[mint_log(900, 5, "0x" + "22" * 32),
                            mint_log(400, 2, "0x" + "33" * 32),
                            mint_log(400, 1, "0x" + "44" * 32)])
        self.assertEqual(first_mint_tx_live(rpc, TOKEN),
                         ("0x" + "44" * 32, 400))

    def test_no_logs_means_not_a_token(self):
        self.assertIsNone(first_mint_tx_live(StubRpc(logs=[]), TOKEN))


class TestResolveLauncherLive(unittest.TestCase):
    def setUp(self):
        self.con = fresh_db()
        self.tx = {"from": LAUNCHER.upper(), "to": LAUNCHPAD,
                   "blockNumber": hex(20_000_000)}

    def test_known_token_costs_one_tx_lookup(self):
        # The common case: the log scan already recorded the mint tx, so
        # attribution is a single call and must not re-scan logs.
        self.con.execute("INSERT INTO token (address, first_block, first_tx) "
                         "VALUES (?,?,?)", (TOKEN, 20_000_000, TXH))
        rpc = StubRpc(tx=self.tx)
        rec = resolve_launcher_live(rpc, self.con, TOKEN)
        self.assertEqual(rec["first_tx_from"], LAUNCHER)
        self.assertEqual(rec["first_tx_to"], LAUNCHPAD)
        self.assertNotIn("eth_getLogs", rpc.calls)
        self.assertEqual(rpc.calls.count("eth_getTransactionByHash"), 1)

    def test_unknown_token_falls_back_to_log_scan(self):
        # Nothing indexed at all: 30M blocks of this chain are in this state.
        rpc = StubRpc(logs=[mint_log(20_000_000)], tx=self.tx)
        rec = resolve_launcher_live(rpc, self.con, TOKEN)
        self.assertEqual(rec["first_tx_from"], LAUNCHER)
        self.assertEqual(rec["first_block"], 20_000_000)
        self.assertIn("eth_getLogs", rpc.calls)

    def test_normalises_case(self):
        # tx.from casing varies by endpoint; the index is lowercase throughout.
        rpc = StubRpc(logs=[mint_log(20_000_000)], tx=self.tx)
        rec = resolve_launcher_live(rpc, self.con, TOKEN.upper())
        self.assertEqual(rec["address"], TOKEN)
        self.assertEqual(rec["first_tx_from"], LAUNCHER)

    def test_second_lookup_is_cached_and_makes_no_calls(self):
        rpc = StubRpc(logs=[mint_log(20_000_000)], tx=self.tx)
        resolve_launcher_live(rpc, self.con, TOKEN)
        rpc2 = StubRpc(fail="must not be called")
        rec = resolve_launcher_live(rpc2, self.con, TOKEN)
        self.assertEqual(rec["first_tx_from"], LAUNCHER)
        self.assertEqual(rpc2.calls, [])

    def test_rpc_failure_returns_none_and_caches_nothing(self):
        # A dead endpoint must degrade to "I don't know", not a traceback, and
        # must not poison the cache with a negative result.
        rpc = StubRpc(fail="connection reset")
        self.assertIsNone(resolve_launcher_live(rpc, self.con, TOKEN))
        self.assertEqual(
            self.con.execute("SELECT COUNT(*) FROM live_attribution")
            .fetchone()[0], 0)

    def test_address_with_no_mints_returns_none(self):
        self.assertIsNone(resolve_launcher_live(StubRpc(logs=[]), self.con, TOKEN))

    def test_missing_metadata_still_attributes(self):
        # name()/symbol() are cosmetic; failing to read them must not cost us
        # the launcher, which is the part that matters.
        rpc = StubRpc(logs=[mint_log(20_000_000)], tx=self.tx, meta=False)
        rec = resolve_launcher_live(rpc, self.con, TOKEN)
        self.assertEqual(rec["first_tx_from"], LAUNCHER)
        self.assertIsNone(rec["symbol"])

    def test_malformed_tx_returns_none(self):
        rpc = StubRpc(logs=[mint_log(20_000_000)], tx={"nope": 1})
        self.assertIsNone(resolve_launcher_live(rpc, self.con, TOKEN))


class TestPublishedDatasetIsolation(unittest.TestCase):
    """A live lookup must never change what `export` ships."""

    def test_live_lookup_does_not_add_rows_to_the_export_set(self):
        con = fresh_db()
        # one legitimately attributed, probed token: the shipped kind of row
        con.execute("INSERT INTO token (address, first_block, first_tx, "
                    "first_tx_from, is_erc20) VALUES (?,?,?,?,1)",
                    ("0x" + "99" * 20, 1000, "0x" + "88" * 32, LAUNCHER))
        before = con.execute(EXPORT_WHERE).fetchone()[0]

        rpc = StubRpc(logs=[mint_log(20_000_000)],
                      tx={"from": LAUNCHER, "to": LAUNCHPAD,
                          "blockNumber": hex(20_000_000)})
        resolve_launcher_live(rpc, con, TOKEN)

        self.assertEqual(con.execute(EXPORT_WHERE).fetchone()[0], before,
                         "a live lookup leaked into the published CSV")
        self.assertEqual(
            con.execute("SELECT COUNT(*) FROM live_attribution").fetchone()[0], 1)

    def test_live_row_is_not_written_into_token(self):
        con = fresh_db()
        rpc = StubRpc(logs=[mint_log(20_000_000)],
                      tx={"from": LAUNCHER, "to": LAUNCHPAD,
                          "blockNumber": hex(20_000_000)})
        resolve_launcher_live(rpc, con, TOKEN)
        self.assertIsNone(
            con.execute("SELECT 1 FROM token WHERE address=?", (TOKEN,)).fetchone())


if __name__ == "__main__":
    unittest.main(verbosity=2)
