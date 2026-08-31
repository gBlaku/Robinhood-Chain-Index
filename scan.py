#!/usr/bin/env python3
"""
An index of every ERC-20 on Robinhood Chain (chain ID 4663), keyed on the
wallet that actually launched each token.

Why this exists
---------------
Launchpads deploy tokens via CREATE2, so the *contract creator* recorded
on-chain is the launchpad, not the person. Block explorers report this
faithfully and therefore uselessly: Blockscout names the NOXA Fun contract as
the creator of all 3,276 tokens launched through it, and Pons as the creator of
its 167,000+. "Contracts created by this wallet" returns an empty list for even
the most prolific launcher.

The human is the transaction SENDER:

    tx.from  ->  the human who clicked launch      (stored as first_tx_from)
    tx.to    ->  the launchpad they clicked it on  (stored as first_tx_to)
    creator  ->  always the launchpad. useless.

Indexing first_tx_from restores the "dev's previous deployments" lookup that
EVM explorers cannot answer.

How tokens are found
--------------------
Pass A (primary): `eth_getLogs` from genesis for Transfer with `from == 0x0`
(a mint), recording the first block each contract appears in. That is birth
order. It catches factory- and launchpad-deployed tokens, which a `to == null`
contract-creation scan would miss entirely -- and that is most memecoins.

ERC-721 shares the Transfer topic, so it is filtered out: ERC-20 Transfer has
3 topics and a >=32-byte data payload; ERC-721 has 4 topics and empty data.

Pass B (reconciliation): walks block bodies for top-level `to == null`
creations. This catches tokens that deploy WITHOUT ever minting, which Pass A
cannot see by construction -- it found a USDG at block 56 with totalSupply()==0,
older than anything in the mint scan. Scoped to the pre-launch era; chain-wide
would take ~117h at the measured ceiling. Neither pass subsumes the other.

Everything checkpoints to SQLite (out/scan.db) after every chunk, so any
subcommand can be killed and resumed without redoing work.

Endpoint limits, calibrated empirically (see the `phase1` subcommand)
--------------------------------------------------------------------
  - eth_getLogs has NO block-range cap. It caps on RESULTS: 10,000 logs
    ("logs matched by query exceeds limit of 10000").
  - The rate limiter is a per-IP token bucket priced by METHOD COST, not
    request count. Batched sub-request caps differ per method:
        eth_call                25    (~200 sub-req/s)
        eth_getBlockByNumber    10    (~118 full blocks/s)
    An over-large batch is refused no matter how long you wait -- the fix is a
    smaller batch, not a longer sleep.
  - Parallel connections LOWER throughput (3 threads: 68 blk/s vs 118 serial).
    Keep this serial.
  - A default urllib/python User-Agent gets HTTP 403. A curl-like UA works.

Usage
-----
    python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
    .venv/bin/python scan.py phase1            # verify chain, probe limits
    .venv/bin/python scan.py pass-a --end 50500000
    .venv/bin/python scan.py pass-b --end 65000
    .venv/bin/python scan.py meta              # name/symbol/decimals/supply
    .venv/bin/python scan.py creators          # resolves first_tx_from
    .venv/bin/python scan.py classify
    .venv/bin/python scan.py enrich --limit 40
    .venv/bin/python scan.py mcap
    .venv/bin/python scan.py export            # -> data/*.csv
    .venv/bin/python scan.py clusters          # top launchpads / deployers
    .venv/bin/python scan.py status

    # Use your own RPC if you have one -- the public endpoint rate-limits hard:
    ... --rpc https://your-endpoint/v2/YOUR_KEY

Classification anchors (launchpads, bridges, issuer EOAs) live in
config/known_addresses.json with the evidence for each. Labels derive from
on-chain relationships -- owner() matching, first_tx_to clustering, deployer
population statistics -- never from a token's name.

READ-ONLY: this program only ever issues eth_call / eth_get* / eth_chainId.
It never builds, signs, or sends a transaction, and holds no keys.
"""

import argparse
import csv
import json
import os
import sqlite3
import sys
import time

import requests

DEFAULT_RPC = "https://rpc.mainnet.chain.robinhood.com"
CHAIN_ID = 4663
_ROOT = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(_ROOT, "out")      # working dir: scan.db, logs (gitignored)
DATA_DIR = os.path.join(_ROOT, "data")    # committed deliverables: the CSVs
DB_PATH = os.path.join(OUT_DIR, "scan.db")

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_TOPIC = "0x" + "00" * 32
ZERO_ADDR = "0x" + "00" * 20

SEL_NAME = "0x06fdde03"
SEL_SYMBOL = "0x95d89b41"
SEL_DECIMALS = "0x313ce567"
SEL_TOTAL_SUPPLY = "0x18160ddd"

# Uniswap V2 PairCreated(address,address,address,uint256)
PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"

KNOWN_WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"  # per brief, sanity check

# eth_getLogs refuses >10,000 results. Aim just under it: the limiter counts
# requests, so an under-filled request is wasted throughput. Leaving headroom
# absorbs density spikes without tripping the cap and paying for a retry.
LOG_TARGET = 8500


class RpcError(Exception):
    pass


class Rpc:
    """JSON-RPC client with an adaptive throttle.

    The public endpoint 429s aggressively on expensive calls, so we keep a
    per-call delay that grows on 429 and slowly decays on sustained success.
    """

    def __init__(self, url, sleep=0.0, verbose=True):
        self.url = url
        self.delay = sleep
        self.min_delay = sleep
        self.max_delay = 8.0
        self.verbose = verbose
        self.session = requests.Session()
        # NOTE: a default python UA gets HTTP 403 from the public endpoint.
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "curl/8.7.1",
        })
        self._id = 0
        self.n_calls = 0
        self.n_429 = 0
        self._ok_streak = 0
        # Calibrated against the public endpoint: it enforces a per-IP token
        # bucket priced by method cost, NOT by request count. Batched eth_call
        # tops out at 25 sub-requests (~200 sub-req/s); eth_getBlockByNumber
        # with full tx bodies tops out at 10 (~118 blk/s). Opening parallel
        # connections does not raise the ceiling -- it lowers it.
        self.batch_caps = {"eth_call": 25, "eth_getBlockByNumber": 10,
                           "eth_getTransactionReceipt": 10,
                           "eth_getTransactionByHash": 15}
        self.default_cap = 10

    def _post(self, payload, timeout, retries):
        backoff = 0.2
        last = None
        saw_429 = False
        for attempt in range(retries):
            try:
                r = self.session.post(self.url, json=payload, timeout=timeout)
            except requests.RequestException as e:
                last = e
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            self.n_calls += 1
            if r.status_code == 429 or (
                r.status_code == 200 and '"code":429' in r.text[:200]
            ):
                self.n_429 += 1
                self._ok_streak = 0
                saw_429 = True
                time.sleep(backoff)
                backoff = min(backoff * 1.9, 20.0)
                continue
            if r.status_code != 200:
                last = RpcError(f"HTTP {r.status_code}: {r.text[:200]}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            try:
                body = r.json()
            except json.JSONDecodeError as e:
                last = e
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            self._ok_streak += 1
            if self.delay:
                time.sleep(self.delay)
            return body
        if saw_429 and last is None:
            # Persistent 429 with no other error: the query is too expensive for
            # the bucket, not merely mistimed. Callers should shrink it.
            raise RpcError(f"rate-limited: {retries} consecutive 429s "
                           f"(query too expensive -- narrow the range)")
        raise RpcError(f"request failed after {retries} attempts: {last}")

    def call(self, method, params, retries=8, timeout=120):
        self._id += 1
        body = self._post(
            {"jsonrpc": "2.0", "id": self._id, "method": method, "params": params},
            timeout, retries,
        )
        if isinstance(body, dict) and "error" in body:
            raise RpcError(body["error"].get("message", str(body["error"])))
        return body["result"]

    def batch(self, calls, retries=8, timeout=180):
        """calls = [(method, params), ...] -> [result_or_RpcError, ...]"""
        if not calls:
            return []
        payload = []
        for i, (m, p) in enumerate(calls):
            self._id += 1
            payload.append({"jsonrpc": "2.0", "id": i, "method": m, "params": p})
        body = self._post(payload, timeout, retries)
        if isinstance(body, dict):
            # whole-batch error (e.g. rate limit expressed as a single object)
            raise RpcError(str(body.get("error", body))[:300])
        out = [None] * len(calls)
        for item in body:
            idx = item.get("id")
            if not isinstance(idx, int) or not (0 <= idx < len(calls)):
                continue
            out[idx] = (RpcError(item["error"].get("message", "?"))
                        if "error" in item else item.get("result"))
        return out

    def safe_batch(self, calls, max_sub=None):
        """Batch, split to the per-method sub-request cap the endpoint allows.

        The limiter prices a request by its method cost, so an over-large batch
        429s no matter how long we wait -- the fix is a smaller batch, not a
        longer sleep. Caps are calibrated in `batch_caps`.
        """
        if not calls:
            return []
        cap = max_sub or self.batch_caps.get(calls[0][0], self.default_cap)
        out = []
        for i in range(0, len(calls), cap):
            grp = calls[i:i + cap]
            try:
                out.extend(self.batch(grp, retries=10))
            except RpcError:
                # fall back to singles, tolerating individual failures
                for m, p in grp:
                    try:
                        out.append(self.call(m, p, retries=6))
                    except RpcError as e:
                        out.append(e)
        return out


# ---------- minimal ABI decoding (no eth-abi dependency) ----------

def decode_string(hexdata):
    """Decode an ABI-encoded string return value. Falls back to bytes32."""
    if not hexdata or hexdata in ("0x", "0x0"):
        return None
    try:
        raw = bytes.fromhex(hexdata[2:])
    except ValueError:
        return None
    # Dynamic string: offset (32) + length (32) + data
    if len(raw) >= 64:
        try:
            offset = int.from_bytes(raw[0:32], "big")
            if 0 < offset < len(raw):
                length = int.from_bytes(raw[offset:offset + 32], "big")
                if 0 < length <= len(raw) - offset - 32:
                    s = raw[offset + 32: offset + 32 + length]
                    return sanitize_text(s.decode("utf-8", errors="replace"))
        except (ValueError, IndexError):
            pass
    # bytes32-style name/symbol (older tokens, e.g. MKR-style)
    return sanitize_text(raw.rstrip(b"\x00").decode("utf-8", errors="replace"))


_CTRL = {c: None for c in range(32) if c not in (9,)} | {0x7f: None}
_CTRL.update({c: None for c in range(0x80, 0xa0)})


def sanitize_text(v, limit=200):
    """Token name()/symbol() are attacker-controlled. Never trust them.

    Strips C0/C1 control characters (NUL, ANSI escapes, bidi overrides can all
    ride in here) and caps length. A token named with escape sequences would
    otherwise corrupt any terminal or CSV that renders this dataset.
    """
    if v is None:
        return None
    v = str(v).translate(_CTRL).strip()
    return v[:limit] or None


def csv_safe(v):
    """Neutralise spreadsheet formula injection.

    A name starting with = + - @ is executed as a formula by Excel/Sheets on
    open. Prefixing with an apostrophe renders it inert while keeping it
    readable. This dataset contains at least one such name (@hooddeploys).
    """
    if isinstance(v, str) and v[:1] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + v
    return v


def decode_uint(hexdata):
    if not hexdata or hexdata in ("0x", "0x0"):
        return 0 if hexdata == "0x0" else None
    try:
        return int(hexdata, 16)
    except ValueError:
        return None


def topic_to_addr(topic):
    if not topic or len(topic) < 42:
        return None
    return "0x" + topic[-40:].lower()


# ---------- storage ----------

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);

-- Pass A: first mint (Transfer from 0x0) seen per contract
CREATE TABLE IF NOT EXISTS mint_first (
    address       TEXT PRIMARY KEY,
    first_block   INTEGER NOT NULL,
    log_index     INTEGER,
    tx_hash       TEXT,
    mint_to       TEXT,      -- topic2, who received the first mint
    n_topics      INTEGER,
    data_len      INTEGER
);
CREATE INDEX IF NOT EXISTS mint_first_blk ON mint_first(first_block, log_index);

-- Pass A also records every address seen with a 4-topic Transfer (likely ERC-721)
CREATE TABLE IF NOT EXISTS nft_first (
    address     TEXT PRIMARY KEY,
    first_block INTEGER NOT NULL,
    tx_hash     TEXT
);

-- Pass B: top-level contract creations (tx.to == null)
CREATE TABLE IF NOT EXISTS creation (
    address     TEXT PRIMARY KEY,
    block       INTEGER NOT NULL,
    tx_hash     TEXT,
    tx_index    INTEGER,
    deployer    TEXT
);
CREATE INDEX IF NOT EXISTS creation_blk ON creation(block, tx_index);

-- Phase 3: token metadata + creation provenance
CREATE TABLE IF NOT EXISTS token (
    address       TEXT PRIMARY KEY,
    name          TEXT,
    symbol        TEXT,
    decimals      INTEGER,
    total_supply  TEXT,
    is_erc20      INTEGER,
    first_block   INTEGER,
    first_ts      INTEGER,
    first_tx      TEXT,
    mint_to       TEXT,
    creation_tx   TEXT,
    deployer      TEXT,
    creation_block INTEGER,
    creation_method TEXT,     -- 'toplevel' | 'factory/internal' | 'unknown'
    source        TEXT,       -- 'mint' | 'creation' | 'both'
    klass         TEXT,
    evidence      TEXT,
    first_tx_from TEXT,       -- EOA that sent the tx containing the first mint
    first_tx_to   TEXT,       -- contract that tx called (factory / launchpad / gateway)
    minter        TEXT        -- resolved label for first_tx_to, if known
);

-- Phase 4/5 enrichment
CREATE TABLE IF NOT EXISTS enrich (
    address        TEXT PRIMARY KEY,
    holders        INTEGER,
    holders_src    TEXT,
    n_transfers    INTEGER,
    last_transfer_block INTEGER,
    last_transfer_ts    INTEGER,
    active_30d     INTEGER,
    pool_address   TEXT,
    pool_block     INTEGER,
    pool_kind      TEXT,
    liquidity      TEXT,
    verified       TEXT,
    deployer_bal   TEXT,
    deployer_pct   REAL,
    obscurity      TEXT,
    notes          TEXT
);
"""


def db_open(path=DB_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path, timeout=120)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    migrate(con)
    return con


def migrate(con):
    """CREATE TABLE IF NOT EXISTS won't add columns to an existing DB."""
    for table, cols in (
        ("token", [("first_tx_from", "TEXT"), ("first_tx_to", "TEXT"),
                   ("minter", "TEXT"), ("klass", "TEXT"), ("evidence", "TEXT")]),
        ("enrich", [("first_pool_ts", "INTEGER"), ("supply_at_deployer", "TEXT")]),
    ):
        have = {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
        for name, typ in cols:
            if name not in have:
                con.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")
    con.commit()


def meta_get(con, k, default=None):
    row = con.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return row[0] if row else default


def meta_set(con, k, v):
    con.execute("INSERT INTO meta(k,v) VALUES(?,?) "
                "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))


# ---------- phase 1 ----------

def cmd_phase1(rpc, con, args):
    chain_id = int(rpc.call("eth_chainId", []), 16)
    latest = int(rpc.call("eth_blockNumber", []), 16)
    print(f"chain_id      = {chain_id}" + ("  OK" if chain_id == CHAIN_ID else
                                           f"  !! expected {CHAIN_ID}"))
    print(f"latest_block  = {latest:,}")
    print(f"client        = {rpc.call('web3_clientVersion', [])}")

    g = rpc.call("eth_getBlockByNumber", ["0x0", False])
    gts = int(g["timestamp"], 16)
    print(f"genesis ts    = {gts} ({fmt_ts(gts)})  hash={g['hash']}")
    b1 = rpc.call("eth_getBlockByNumber", ["0x1", False])
    b1ts = int(b1["timestamp"], 16)
    print(f"block 1 ts    = {b1ts} ({fmt_ts(b1ts)})")

    meta_set(con, "chain_id", chain_id)
    meta_set(con, "genesis_ts", gts)
    meta_set(con, "genesis_hash", g["hash"])
    meta_set(con, "block1_ts", b1ts)
    meta_set(con, "latest_at_scan", latest)
    meta_set(con, "client_version", rpc.call("web3_clientVersion", []))

    # Empirically bisect the getLogs limit.
    print("\ngetLogs probe (Transfer+zero-topic filter, from block 1):")
    limit_note = "no block-range cap observed; result cap"
    for span in (1_000, 10_000, 100_000, 1_000_000):
        try:
            logs = rpc.call("eth_getLogs", [{"fromBlock": "0x1", "toBlock": hex(span),
                                             "topics": [TRANSFER_TOPIC, ZERO_TOPIC]}])
            print(f"  span {span:>9,}: OK, {len(logs):,} logs")
        except RpcError as e:
            print(f"  span {span:>9,}: {e}")
            limit_note = str(e)
    # wide span with a filter that matches nothing -> proves no range cap
    try:
        rpc.call("eth_getLogs", [{"fromBlock": "0x1", "toBlock": hex(latest),
                                  "topics": ["0x" + "ab" * 32]}])
        print(f"  span {latest:>9,} (no-match filter): OK -> no block-range cap")
    except RpcError as e:
        print(f"  span {latest:>9,} (no-match filter): {e}")
    meta_set(con, "getlogs_limit_note", limit_note)

    # sanity check: the brief's known WETH
    w = probe_token(rpc, KNOWN_WETH)
    print(f"\nsanity WETH {KNOWN_WETH}: {w}")
    con.commit()


def fmt_ts(ts):
    if not ts:
        return "?"
    return time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime(ts))


# ---------- pass A: mint logs ----------

def cmd_pass_a(rpc, con, args):
    """Walk forward from genesis collecting first-seen Transfer-from-zero events."""
    start = args.start
    if start is None:
        start = int(meta_get(con, "pass_a_cursor", "0"))
    end = args.end
    chunk = args.chunk
    t0 = time.time()
    n_new = 0

    cursor = start
    while cursor <= end:
        hi = min(cursor + chunk - 1, end)
        params = [{"fromBlock": hex(cursor), "toBlock": hex(hi),
                   "topics": [TRANSFER_TOPIC, ZERO_TOPIC]}]
        try:
            logs = rpc.call("eth_getLogs", params, retries=14)
        except RpcError as e:
            msg = str(e).lower()
            if chunk > 1 and any(k in msg for k in
                                 ("exceeds limit", "range", "too many", "exceed",
                                  "large", "timeout", "timed out", "response size",
                                  "context deadline", "rate-limited")):
                chunk = max(1, chunk // 2)
                print(f"  [narrowing chunk to {chunk}]", file=sys.stderr)
                continue
            raise

        logs.sort(key=lambda l: (int(l["blockNumber"], 16), int(l["logIndex"], 16)))
        rows_erc20, rows_nft = [], []
        for log in logs:
            addr = log["address"].lower()
            topics = log["topics"]
            data = log.get("data", "0x")
            blk = int(log["blockNumber"], 16)
            # ERC-20 Transfer: 3 topics, >=32-byte data. ERC-721: 4 topics, no data.
            if len(topics) == 3 and len(data) >= 66:
                rows_erc20.append((addr, blk, int(log["logIndex"], 16),
                                   log["transactionHash"], topic_to_addr(topics[2]),
                                   len(topics), (len(data) - 2) // 2))
            elif len(topics) == 4:
                rows_nft.append((addr, blk, log["transactionHash"]))

        cur = con.cursor()
        before = cur.execute("SELECT COUNT(*) FROM mint_first").fetchone()[0]
        cur.executemany(
            "INSERT OR IGNORE INTO mint_first"
            "(address,first_block,log_index,tx_hash,mint_to,n_topics,data_len)"
            " VALUES(?,?,?,?,?,?,?)", rows_erc20)
        cur.executemany(
            "INSERT OR IGNORE INTO nft_first(address,first_block,tx_hash)"
            " VALUES(?,?,?)", rows_nft)
        after = cur.execute("SELECT COUNT(*) FROM mint_first").fetchone()[0]
        n_new += after - before
        meta_set(con, "pass_a_cursor", hi + 1)
        meta_set(con, "pass_a_end", end)
        con.commit()

        el = time.time() - t0
        rate = (hi - start + 1) / el if el else 0
        print(f"  A blk {cursor:>9,}-{hi:>9,} | {len(logs):>5} logs | "
              f"tokens={after:>5} (+{after-before}) | chunk={chunk} | "
              f"{rate:,.0f} blk/s | delay={rpc.delay:.2f} 429s={rpc.n_429}",
              file=sys.stderr)

        # The endpoint's binding constraint is getLogs REQUESTS per second, not
        # blocks -- and each request may return up to 10,000 logs. So the goal is
        # to keep every request as close to that cap as possible: a chunk that
        # returns 4k logs wastes half the request. Size proportionally toward a
        # target just under the cap, damped to avoid oscillating into failures.
        if logs:
            ideal = chunk * (LOG_TARGET / max(len(logs), 1))
            chunk = int(max(chunk * 0.5, min(ideal, chunk * 2.0, args.chunk)))
            chunk = max(chunk, 1)
        elif chunk < args.chunk:
            chunk = min(chunk * 4, args.chunk)   # empty region: stride out fast
        cursor = hi + 1

    print(f"pass A done: {n_new} new addresses, cursor={end+1}, "
          f"{time.time()-t0:.0f}s", file=sys.stderr)


# ---------- pass B: contract creations ----------

def cmd_pass_b(rpc, con, args):
    """Scan block bodies for top-level `to == null` contract creations."""
    start = args.start
    if start is None:
        start = int(meta_get(con, "pass_b_cursor", "0"))
    end = args.end
    batch_n = args.batch
    t0 = time.time()

    cursor = start
    pending_creates = []   # (block, tx_index, tx_hash, deployer)
    while cursor <= end:
        hi = min(cursor + batch_n - 1, end)
        calls = [("eth_getBlockByNumber", [hex(b), True]) for b in range(cursor, hi + 1)]
        results = rpc.safe_batch(calls)

        n_tx = 0
        for res in results:
            if res is None or isinstance(res, RpcError) or not isinstance(res, dict):
                continue
            blk = int(res["number"], 16)
            for tx in res["transactions"]:
                n_tx += 1
                if tx.get("to") is None:
                    pending_creates.append(
                        (blk, int(tx["transactionIndex"], 16), tx["hash"],
                         tx["from"].lower()))

        # resolve created addresses via receipts (batched)
        if pending_creates:
            for i in range(0, len(pending_creates), 100):
                grp = pending_creates[i:i + 100]
                rec = rpc.safe_batch([("eth_getTransactionReceipt", [c[2]]) for c in grp])
                rows = []
                for (blk, ti, txh, dep), r in zip(grp, rec):
                    if isinstance(r, dict) and r.get("contractAddress"):
                        rows.append((r["contractAddress"].lower(), blk, txh, ti, dep))
                con.executemany(
                    "INSERT OR IGNORE INTO creation"
                    "(address,block,tx_hash,tx_index,deployer) VALUES(?,?,?,?,?)", rows)
            pending_creates = []

        meta_set(con, "pass_b_cursor", hi + 1)
        meta_set(con, "pass_b_end", end)
        con.commit()

        el = time.time() - t0
        rate = (hi - start + 1) / el if el else 0
        if (hi // batch_n) % 10 == 0 or hi >= end:
            tot = con.execute("SELECT COUNT(*) FROM creation").fetchone()[0]
            eta = (end - hi) / rate / 60 if rate else 0
            print(f"  B blk {cursor:>9,}-{hi:>9,} | txs={n_tx:>4} | creations={tot:>5} "
                  f"| {rate:,.0f} blk/s | eta {eta:,.0f}m | delay={rpc.delay:.2f} "
                  f"429s={rpc.n_429}", file=sys.stderr)
        cursor = hi + 1

    print(f"pass B done through {end}, {time.time()-t0:.0f}s", file=sys.stderr)


# ---------- phase 3: metadata ----------

def eth_call(rpc, to, data, block="latest"):
    try:
        return rpc.call("eth_call", [{"to": to, "data": data}, block])
    except RpcError:
        return None


def probe_tokens_bulk(rpc, addresses, per_req=25):
    """Probe many addresses at once: 4 eth_calls each, packed into batches.

    The endpoint accepts 25 eth_call sub-requests per batch, so 6 tokens per
    request at 4 calls each; safe_batch splits to that cap automatically.
    """
    out = {}
    for i in range(0, len(addresses), per_req):
        grp = addresses[i:i + per_req]
        calls = []
        for a in grp:
            for sel in (SEL_TOTAL_SUPPLY, SEL_NAME, SEL_SYMBOL, SEL_DECIMALS):
                calls.append(("eth_call", [{"to": a, "data": sel}, "latest"]))
        res = rpc.safe_batch(calls)
        for j, a in enumerate(grp):
            r = res[j * 4:(j + 1) * 4]
            def ok(x):
                return x if isinstance(x, str) else None
            total = decode_uint(ok(r[0]))
            out[a] = None if total is None else {
                "address": a,
                "name": decode_string(ok(r[1])),
                "symbol": decode_string(ok(r[2])),
                "decimals": decode_uint(ok(r[3])),
                "total_supply": total,
            }
    return out


def block_ts_bulk(rpc, blocks, per_req=100):
    """Fetch timestamps for many block numbers."""
    out = {}
    blocks = sorted(set(blocks))
    for i in range(0, len(blocks), per_req):
        grp = blocks[i:i + per_req]
        res = rpc.safe_batch(
            [("eth_getBlockByNumber", [hex(b), False]) for b in grp])
        for b, r in zip(grp, res):
            if isinstance(r, dict) and r.get("timestamp"):
                out[b] = int(r["timestamp"], 16)
    return out


def probe_token(rpc, address):
    """Single-address ERC-20 probe. Thin wrapper over the bulk path."""
    return probe_tokens_bulk(rpc, [address]).get(address)


def cmd_meta(rpc, con, args):
    """Populate the `token` table for everything Pass A and Pass B found."""
    rows = con.execute("""
        SELECT address, first_block, log_index, tx_hash, mint_to FROM mint_first
        ORDER BY first_block, log_index
    """).fetchall()
    creations = dict((r[0], r) for r in con.execute(
        "SELECT address, block, tx_hash, tx_index, deployer FROM creation").fetchall())

    seen = set()
    work = []
    for addr, fb, li, txh, mto in rows:
        seen.add(addr)
        work.append((addr, fb, txh, mto, "mint"))
    # creations that never minted -> the reconciliation set
    for addr, (a, blk, txh, ti, dep) in creations.items():
        if addr not in seen:
            work.append((addr, blk, txh, None, "creation"))

    done = set(r[0] for r in con.execute(
        "SELECT address FROM token WHERE is_erc20 IS NOT NULL").fetchall())
    # rows seeded by `attribute` have is_erc20 NULL -> they get picked up here
    work = [w for w in work if w[0] not in done]
    print(f"metadata: {len(work)} addresses to probe "
          f"({len(done)} already done)", file=sys.stderr)

    t0 = time.time()
    CH = 250
    for i in range(0, len(work), CH):
        grp = work[i:i + CH]
        metas = probe_tokens_bulk(rpc, [w[0] for w in grp])
        tss = block_ts_bulk(rpc, [w[1] for w in grp])
        rows = []
        for addr, fb, txh, mto, source in grp:
            m = metas.get(addr)
            c = creations.get(addr)
            rows.append((
                addr, (m or {}).get("name"), (m or {}).get("symbol"),
                (m or {}).get("decimals"),
                str(m["total_supply"]) if m else None,
                1 if m else 0, fb, tss.get(fb), txh, mto,
                c[2] if c else None, c[4] if c else None, c[1] if c else None,
                "toplevel" if c else "factory/internal",
                "both" if (source == "mint" and c) else source))
        con.executemany("""
            INSERT INTO token(address,name,symbol,decimals,total_supply,is_erc20,
                              first_block,first_ts,first_tx,mint_to,
                              creation_tx,deployer,creation_block,creation_method,source)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(address) DO UPDATE SET
              name=excluded.name, symbol=excluded.symbol, decimals=excluded.decimals,
              total_supply=excluded.total_supply, is_erc20=excluded.is_erc20,
              first_ts=COALESCE(excluded.first_ts, token.first_ts),
              creation_tx=excluded.creation_tx, deployer=excluded.deployer,
              creation_block=excluded.creation_block,
              creation_method=excluded.creation_method, source=excluded.source
        """, rows)
        con.commit()
        el = time.time() - t0
        n = min(i + CH, len(work))
        print(f"  meta {n}/{len(work)} | {n/el:.0f} tok/s | delay={rpc.delay:.2f} "
              f"429s={rpc.n_429}", file=sys.stderr)
    print(f"metadata done in {time.time()-t0:.0f}s", file=sys.stderr)


def cmd_creators(rpc, con, args):
    """Resolve the tx that carried each token's first mint: sender and callee.

    `first_tx_to` is the strongest single classification signal available: it is
    the factory, launchpad or bridge gateway that produced the token. Tokens
    deployed directly by an EOA have first_tx_to == the token itself or null.
    """
    rows = con.execute("""
        SELECT address, first_tx FROM token
        WHERE first_tx IS NOT NULL AND first_tx_from IS NULL
          AND first_block < ?
        ORDER BY first_block
    """, (args.max_block,)).fetchall()
    print(f"resolving first-mint tx for {len(rows)} tokens", file=sys.stderr)
    t0 = time.time()
    for i in range(0, len(rows), 300):
        grp = rows[i:i + 300]
        res = rpc.safe_batch([("eth_getTransactionByHash", [r[1]]) for r in grp])
        upd = []
        for (addr, txh), tx in zip(grp, res):
            if not isinstance(tx, dict) or not tx.get("from"):
                continue
            upd.append((tx["from"].lower(),
                        (tx.get("to") or "").lower() or None,
                        tx["from"].lower(), int(tx["blockNumber"], 16), addr))
        con.executemany(
            "UPDATE token SET first_tx_from=?, first_tx_to=?,"
            " deployer=COALESCE(deployer,?),"
            " creation_block=COALESCE(creation_block,?) WHERE address=?", upd)
        con.commit()
        n = min(i + 300, len(rows))
        rate = n / max(time.time() - t0, 1e-9)
        eta = (len(rows) - n) / rate / 60 if rate else 0
        print(f"  txinfo {n:,}/{len(rows):,} | {rate:.0f}/s | eta {eta:,.0f}m",
              file=sys.stderr)


def cmd_clusters(rpc, con, args):
    """Show the addresses responsible for the most token births.

    Used to identify factories, launchpads and bridge gateways empirically
    rather than guessing from token names.
    """
    print("=== top `first_tx_to` (factory / launchpad / gateway called) ===")
    for to, n, ex, fb in con.execute("""
        SELECT first_tx_to, COUNT(*) n, MIN(symbol), MIN(first_block)
        FROM token WHERE first_tx_to IS NOT NULL
        GROUP BY first_tx_to ORDER BY n DESC LIMIT 25"""):
        print(f"  {n:>5} tokens  from blk {fb:>8}  {to}  e.g. {ex}")
    print("\n=== top deployers (EOA sending the first-mint tx) ===")
    for d, n, fb in con.execute("""
        SELECT first_tx_from, COUNT(*) n, MIN(first_block)
        FROM token WHERE first_tx_from IS NOT NULL
        GROUP BY first_tx_from ORDER BY n DESC LIMIT 25"""):
        print(f"  {n:>5} tokens  from blk {fb:>8}  {d}")
    print("\n=== top mint recipients ===")
    for d, n, fb in con.execute("""
        SELECT mint_to, COUNT(*) n, MIN(first_block)
        FROM token WHERE mint_to IS NOT NULL
        GROUP BY mint_to ORDER BY n DESC LIMIT 20"""):
        print(f"  {n:>5} tokens  from blk {fb:>8}  {d}")


LAUNCHPAD_A = "0x5818fddddb96bcd50bee4253f11b324e7dea961b"
LAUNCHPAD_B_NOXA = "0xd9ec2db5f3d1b236843925949fe5bd8a3836fccb"
UNIV2_ROUTER = "0x89e5db8b5aa49aa85ac63f691524311aeb649eba"
SEL_GET_RESERVES = "0x0902f1ac"
SEL_TOKEN0 = "0x0dfe1681"
SEL_TOKEN1 = "0xd21220a7"


# ---------- phase 4 helpers: pools, balances ----------


def as_topic(addr):
    return "0x" + addr.lower().replace("0x", "").rjust(64, "0")


def find_pools(rpc, token):
    """Every PairCreated naming this token, from any factory, whole chain."""
    out = []
    for slot in (1, 2):
        topics = [PAIR_CREATED_TOPIC, None, None]
        topics[slot] = as_topic(token)
        try:
            logs = rpc.call("eth_getLogs", [{"fromBlock": "0x0", "toBlock": "latest",
                                             "topics": topics}])
        except RpcError:
            continue
        for l in logs:
            out.append({
                "pool": "0x" + l["data"][2:66][-40:],
                "factory": l["address"].lower(),
                "block": int(l["blockNumber"], 16),
                "other": topic_to_addr(l["topics"][2 if slot == 1 else 1]),
                "tx": l["transactionHash"],
            })
    out.sort(key=lambda p: p["block"])
    return out


def discover_pools_by_counterparty(rpc, token, logs, top=20):
    """Find AMM pairs a token trades against without relying on PairCreated.

    Some launchpads deploy pools that never emit a standard Uniswap
    PairCreated event, so a topic scan misses them entirely. Instead take the
    token's busiest transfer counterparties and ask each one directly whether
    it is a pair holding this token (token0/token1), then read its reserves.
    """
    from collections import Counter
    c = Counter()
    for l in logs:
        if len(l["topics"]) == 3:
            for t in (l["topics"][1], l["topics"][2]):
                a = topic_to_addr(t)
                if a and a != ZERO_ADDR:
                    c[a] += 1
    cands = [a for a, _ in c.most_common(top)]
    if not cands:
        return []
    res = rpc.safe_batch(
        [("eth_call", [{"to": a, "data": sel}, "latest"])
         for a in cands for sel in (SEL_TOKEN0, SEL_TOKEN1)])
    out = []
    for i, a in enumerate(cands):
        r0, r1 = res[2 * i], res[2 * i + 1]
        if not isinstance(r0, str) or not isinstance(r1, str):
            continue
        if len(r0) != 66 or len(r1) != 66:
            continue
        t0, t1 = topic_to_addr(r0), topic_to_addr(r1)
        pair = {t0, t1}
        if token in pair and len(pair) == 2:
            out.append({"pool": a, "factory": "discovered-by-counterparty",
                        "block": None,
                        "other": (pair - {token}).pop(), "tx": None})
    return out


def pool_state(rpc, pool):
    r = rpc.safe_batch([
        ("eth_call", [{"to": pool, "data": SEL_GET_RESERVES}, "latest"]),
        ("eth_call", [{"to": pool, "data": SEL_TOKEN0}, "latest"]),
        ("eth_call", [{"to": pool, "data": SEL_TOKEN1}, "latest"]),
    ])
    if not isinstance(r[0], str) or len(r[0]) < 130:
        return None
    raw = r[0][2:]
    return {
        "reserve0": int(raw[0:64], 16),
        "reserve1": int(raw[64:128], 16),
        "token0": topic_to_addr(r[1]) if isinstance(r[1], str) else None,
        "token1": topic_to_addr(r[2]) if isinstance(r[2], str) else None,
    }


def token_transfer_history(rpc, token, latest, start=0, cap=250_000):
    """All Transfer logs for one token, from its birth block forward.

    Chunk size adapts to the token's own log density: start wide, halve when we
    hit the endpoint's 10k-result ceiling, widen again when a chunk comes back
    sparse. Starting at `start` (the token's first block) rather than 0 avoids
    re-scanning tens of millions of empty blocks.
    """
    logs, cur = [], max(0, start)
    chunk = 8_000_000
    while cur <= latest:
        hi = min(cur + chunk - 1, latest)
        try:
            got = rpc.call("eth_getLogs", [{"fromBlock": hex(cur), "toBlock": hex(hi),
                                            "address": token,
                                            "topics": [TRANSFER_TOPIC]}], retries=4)
        except RpcError as e:
            if chunk > 2000 and any(k in str(e).lower() for k in
                                    ("exceeds limit", "timed out", "timeout",
                                     "too many", "response size")):
                chunk = max(2000, chunk // 4)
                continue
            return logs, True
        logs.extend(got)
        cur = hi + 1
        if len(got) < 2000 and chunk < 8_000_000:
            chunk = min(chunk * 4, 8_000_000)
        if len(logs) > cap:
            return logs, True
    return logs, False


def balances_from_logs(logs):
    bal = {}
    for l in logs:
        if len(l["topics"]) != 3 or len(l.get("data", "0x")) < 66:
            continue
        v = int(l["data"][2:66], 16)
        frm = topic_to_addr(l["topics"][1])
        to = topic_to_addr(l["topics"][2])
        if frm != ZERO_ADDR:
            bal[frm] = bal.get(frm, 0) - v
        if to != ZERO_ADDR:
            bal[to] = bal.get(to, 0) + v
    return bal


# ---------- phase 3: classification ----------
# Every label below is anchored to something observed on chain (a factory
# address seen in first_tx_to, a shared owner(), a deployer cluster) rather
# than to the token's name. See REPORT.md for the derivation of each.

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "config", "known_addresses.json")
with open(CONFIG_PATH) as _fh:
    ANCHORS = json.load(_fh)

CANONICAL_MINTER = next(iter(ANCHORS["bridged"]))
SHARED_OWNER = ANCHORS["bridged"][CANONICAL_MINTER]["shared_owner"]
STOCK_WRAP_FACTORY = "0x4262efbd176f02824af27010bea218429c33c7e8"
STOCK_ISSUER_EOA = "0x2b94105fff37630f98e1f24811dad588fc5c3a87"
AA_ENTRYPOINT = next(iter(ANCHORS["deployment_routes"]))
STAGING_END = ANCHORS["staging_window"]["end_block"]

LAUNCHPADS = {a: f"{v['label']} ({a}, operator {v['operator']})"
              for a, v in ANCHORS["launchpads"].items()}

INFRA = "Infrastructure"
BRIDGED = "Bridged"
OFFICIAL = "Official / RWA"
INDEPENDENT = "Independent"


def cmd_classify(rpc, con, args):
    rows = con.execute("""
        SELECT address,symbol,name,first_block,first_tx_from,first_tx_to,mint_to,
               deployer,creation_method,is_erc20
        FROM token""").fetchall()

    klass, evid = {}, {}

    def setk(a, k, e):
        klass[a], evid[a] = k, e

    for (a, sym, nm, fb, txf, txt, mto, dep, cm, is20) in rows:
        sym = sym or ""
        nm = nm or ""
        if a == KNOWN_WETH:
            setk(a, INFRA, "canonical WETH for the chain's ETH gas token; "
                           "name()=WETH; designated in brief"); continue
        if sym == "UNI-V2" or nm == "Uniswap V2":
            setk(a, INFRA, f"Uniswap-V2 LP share token (name()='Uniswap V2'), "
                           f"minted via factory/router {txt}; an AMM pair, not a launch")
            continue
        if txt == CANONICAL_MINTER:
            setk(a, BRIDGED, f"first mint executed by canonical-asset minter "
                             f"{CANONICAL_MINTER}, which shares owner() "
                             f"{SHARED_OWNER} with this token -> mint/burn "
                             f"representation of an off-chain asset, not a launch")
            continue
        if txt == STOCK_WRAP_FACTORY:
            setk(a, OFFICIAL, f"minted through stock-token wrapper factory "
                              f"{STOCK_WRAP_FACTORY}"); continue
        if dep == STOCK_ISSUER_EOA or "• Robinhood" in nm:
            setk(a, OFFICIAL, f"issuer marker: deployer {STOCK_ISSUER_EOA} / "
                              f"name carries the '• Robinhood' RWA suffix"); continue
        if txt in LAUNCHPADS:
            setk(a, INDEPENDENT, f"deployed by EOA {txf} through public "
                                 f"{LAUNCHPADS[txt]}"); continue
        if nm.startswith("ERC20Mock") or "Mock" in nm or sym in ("E20M", "tMOCK"):
            setk(a, INFRA, f"test/mock ERC-20 (name()={nm!r}) deployed during the "
                           f"pre-launch staging period"); continue

    # --- propagate by deployer cluster: the brief's "deployer also deployed
    # known infra" signal. A deployer that produced official/bridged/infra
    # tokens and never used a launchpad is treated as a protocol deployer.
    by_dep = {}
    for (a, sym, nm, fb, txf, txt, mto, dep, cm, is20) in rows:
        if dep:
            by_dep.setdefault(dep, []).append(a)
    propagated = 0
    for dep, addrs in by_dep.items():
        labs = {klass[x] for x in addrs if x in klass}
        if not labs or INDEPENDENT in labs:
            continue
        seed = (OFFICIAL if OFFICIAL in labs else
                BRIDGED if BRIDGED in labs else INFRA)
        for x in addrs:
            if x not in klass:
                setk(x, seed, f"same deployer ({dep}) as {len(addrs)-1} other "
                              f"token(s) already classified {seed}")
                propagated += 1

    # --- permissioned staging window ---
    # Empirical: only 16 distinct EOAs deployed ANY token in blocks 0-58,539
    # (36 days, 2026-05-11 to 2026-06-15). In the next 38k blocks that jumps to
    # 148. A 36-day window with 16 deployers and no launchpad is a closed
    # partner-onboarding period, so tokens born in it are partner/protocol
    # deployments rather than open launches. This is a population statistic,
    # not a read on the token names.
    n_staging_deployers = len({r[0] for r in con.execute(
        "SELECT first_tx_from FROM token WHERE first_block < ?", (STAGING_END,))
        if r[0]})
    staged = 0
    for (a, sym, nm, fb, txf, txt, mto, dep, cm, is20) in rows:
        if a in klass or fb >= STAGING_END:
            continue
        setk(a, OFFICIAL, f"deployed at block {fb}, inside the permissioned "
                          f"pre-launch window (blocks 0-{STAGING_END}) in which only "
                          f"{n_staging_deployers} distinct EOAs deployed any token "
                          f"across 36 days; deployer {txf} is one of them")
        staged += 1

    # --- anything still unlabelled ---
    first_launchpad_block = con.execute(
        "SELECT MIN(first_block) FROM token WHERE first_tx_to IN (?,?)",
        (LAUNCHPAD_A, LAUNCHPAD_B_NOXA)).fetchone()[0] or 0
    unl = 0
    for (a, sym, nm, fb, txf, txt, mto, dep, cm, is20) in rows:
        if a in klass:
            continue
        unl += 1
        if is20 is None and txf is None:
            # discovered by the log scan but not yet attributed or probed --
            # we know it minted, and nothing else. Say so rather than guess.
            setk(a, "Unclassified", "discovered by mint scan; launcher not yet "
                                    "resolved and metadata not yet probed")
        elif txt is None or txt == a:
            setk(a, INDEPENDENT, f"direct deployment by EOA {txf} "
                                 f"(first-mint tx has to={'null' if not txt else 'self'}); "
                                 f"no factory, no infra-linked deployer")
        elif txt == AA_ENTRYPOINT:
            setk(a, INDEPENDENT, f"deployed via ERC-4337 EntryPoint {AA_ENTRYPOINT} "
                                 f"(smart-account tx), origin EOA {txf}")
        else:
            setk(a, INDEPENDENT, f"deployed via contract {txt} by {txf}; that "
                                 f"contract is not a known infra factory")

    con.executemany("UPDATE token SET klass=?, evidence=? WHERE address=?",
                    [(klass[a], evid[a], a) for a in klass])
    con.commit()
    print(f"classified {len(klass)} tokens ({propagated} by deployer-cluster "
          f"propagation, {staged} by staging-window rule, {unl} by fallback)")
    print(f"first launchpad-deployed token at block {first_launchpad_block}")
    for k, n in con.execute("SELECT klass, COUNT(*) FROM token GROUP BY klass "
                            "ORDER BY COUNT(*) DESC"):
        print(f"  {str(k):<18} {n:>6}")


# ---------- phase 4: enrichment ----------

def eth_usd(rpc, con):
    """Price ETH in USD from the deepest WETH/<6-decimal USD token> pair."""
    best = None
    for (a, sym) in con.execute(
            "SELECT address,symbol FROM token WHERE decimals=6 AND symbol LIKE '%USD%' "
            "AND klass IN ('Official / RWA','Bridged') ORDER BY first_block LIMIT 8"):
        for p in find_pools(rpc, a):
            if p["other"] != KNOWN_WETH:
                continue
            st = pool_state(rpc, p["pool"])
            if not st:
                continue
            if st["token0"] == KNOWN_WETH:
                weth, usd = st["reserve0"], st["reserve1"]
            else:
                weth, usd = st["reserve1"], st["reserve0"]
            if weth > 10 ** 18 and (best is None or weth > best[0]):
                best = (weth, usd / 1e6 / (weth / 1e18), sym, p["pool"])
    return best


def cmd_enrich(rpc, con, args):
    latest = int(rpc.call("eth_blockNumber", []), 16)
    now = int(rpc.call("eth_getBlockByNumber", [hex(latest), False])["timestamp"], 16)
    px = eth_usd(rpc, con)
    if px:
        print(f"ETH/USD = {px[1]:,.2f} (from {px[2]} pair {px[3]}, "
              f"{px[0]/1e18:,.2f} WETH deep)", file=sys.stderr)
        meta_set(con, "eth_usd", px[1]); meta_set(con, "eth_usd_pool", px[3])
    eth_px = px[1] if px else None

    rows = con.execute("""
        SELECT address,symbol,name,first_block,first_ts,decimals,total_supply,
               deployer,first_tx_to
        FROM token WHERE klass=?
          AND address NOT IN (SELECT address FROM enrich WHERE holders IS NOT NULL)
        ORDER BY first_block LIMIT ?""",
        (INDEPENDENT, args.limit)).fetchall()
    print(f"enriching {len(rows)} earliest independent tokens", file=sys.stderr)

    for i, (a, sym, nm, fb, fts, dec, tsup, dep, txt) in enumerate(rows, 1):
        dec = dec if dec is not None else 18
        supply = int(tsup) if tsup else 0
        pools = find_pools(rpc, a)
        pool = pools[0] if pools else None
        liq_note, price_eth, mcap = None, None, None
        if pool:
            st = pool_state(rpc, pool["pool"])
            if st:
                if st["token0"] == a:
                    rt, ro = st["reserve0"], st["reserve1"]
                else:
                    rt, ro = st["reserve1"], st["reserve0"]
                other = pool["other"]
                if rt > 0 and ro > 0:
                    if other == KNOWN_WETH:
                        price_eth = (ro / 1e18) / (rt / 10 ** dec)
                        liq_note = f"{ro/1e18:.6f} WETH / {rt/10**dec:,.0f} {sym}"
                        if eth_px:
                            mcap = price_eth * eth_px * (supply / 10 ** dec)
                    else:
                        liq_note = (f"paired with {other}: "
                                    f"{ro:,} / {rt/10**dec:,.0f} {sym}")
                else:
                    liq_note = "pool exists but reserves are zero (drained/never funded)"

        logs, trunc = token_transfer_history(rpc, a, latest, start=fb)
        bal = balances_from_logs(logs)
        holders = sum(1 for v in bal.values() if v > 0)
        last_blk = max((int(l["blockNumber"], 16) for l in logs), default=fb)
        lts = int(rpc.call("eth_getBlockByNumber",
                           [hex(last_blk), False])["timestamp"], 16)
        dep_bal = bal.get(dep, 0) if dep else 0
        dep_pct = (dep_bal / supply * 100) if supply else 0.0

        con.execute("""INSERT INTO enrich(address,holders,holders_src,n_transfers,
                       last_transfer_block,last_transfer_ts,active_30d,pool_address,
                       pool_block,pool_kind,liquidity,deployer_bal,deployer_pct,notes)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(address) DO UPDATE SET
                        holders=excluded.holders,n_transfers=excluded.n_transfers,
                        last_transfer_block=excluded.last_transfer_block,
                        last_transfer_ts=excluded.last_transfer_ts,
                        active_30d=excluded.active_30d,pool_address=excluded.pool_address,
                        pool_block=excluded.pool_block,pool_kind=excluded.pool_kind,
                        liquidity=excluded.liquidity,deployer_bal=excluded.deployer_bal,
                        deployer_pct=excluded.deployer_pct,notes=excluded.notes""",
            (a, holders, "replayed Transfer logs" + (" (TRUNCATED)" if trunc else ""),
             len(logs), last_blk, lts, 1 if (now - lts) <= 30*86400 else 0,
             pool["pool"] if pool else None, pool["block"] if pool else None,
             pool["factory"] if pool else None, liq_note, str(dep_bal), dep_pct,
             (f"mcap_usd={mcap:,.2f}" if mcap is not None else "mcap=unknown")
             + (f" price_eth={price_eth:.3e}" if price_eth else "")))
        con.commit()
        print(f"  {i:>3}/{len(rows)} {str(sym)[:12]:<13} holders={holders:<5} "
              f"tx={len(logs):<5} pool={'yes' if pool else 'no':<3} "
              f"mcap={('$%.0f'%mcap) if mcap is not None else '?':<12} {liq_note or ''}",
              file=sys.stderr)


def cmd_mcap(rpc, con, args):
    """Recompute price/market cap from the DEEPEST pool, not the first one.

    A token's earliest pair is often drained or was never funded; pricing off
    it produces a meaningless number. We evaluate every pair the token appears
    in and quote from the one with the largest WETH reserve. Tokens whose only
    venue is a launchpad bonding curve have no pair at all and are reported as
    such rather than being given a fabricated price.
    """
    eth_px = float(meta_get(con, "eth_usd", "0")) or None
    head = int(rpc.call("eth_blockNumber", []), 16)
    rows = con.execute("""
        SELECT t.address,t.symbol,t.name,t.decimals,t.total_supply,t.first_block,
               t.first_ts,t.deployer,t.first_tx_to,e.holders,e.n_transfers
        FROM token t JOIN enrich e ON e.address=t.address
        ORDER BY t.first_block""").fetchall()
    print(f"repricing {len(rows)} tokens at ETH=${eth_px:,.2f}" if eth_px else "no ETH px",
          file=sys.stderr)
    out = []
    for (a, sym, nm, dec, tsup, fb, fts, dep, txt, holders, ntx) in rows:
        dec = dec if dec is not None else 18
        supply = int(tsup) if tsup else 0
        best = None
        cand = find_pools(rpc, a)
        if not any(p["other"] == KNOWN_WETH for p in cand):
            hist, _ = token_transfer_history(rpc, a, head, start=fb, cap=60000)
            cand = cand + discover_pools_by_counterparty(rpc, a, hist)
        for p in cand:
            st = pool_state(rpc, p["pool"])
            if not st:
                continue
            if st["token0"] == a:
                rt, ro = st["reserve0"], st["reserve1"]
            else:
                rt, ro = st["reserve1"], st["reserve0"]
            if p["other"] != KNOWN_WETH or rt <= 0 or ro <= 0:
                continue
            if best is None or ro > best["weth"]:
                best = {"pool": p["pool"], "weth": ro, "tok": rt,
                        "block": p["block"], "factory": p["factory"]}
        pool_created_block = best["block"] if best else None
        if best:
            price_eth = (best["weth"] / 1e18) / (best["tok"] / 10 ** dec)
            fdv = price_eth * (supply / 10 ** dec) * (eth_px or 0)
            # value actually removable is bounded by the pool's WETH side
            liq_usd = (best["weth"] / 1e18) * (eth_px or 0)
            out.append((a, sym, nm, fb, fts, holders, ntx, best["pool"],
                        best["block"], price_eth, fdv, liq_usd, best["weth"] / 1e18,
                        best["tok"] / 10 ** dec, supply / 10 ** dec, "pair"))
        else:
            out.append((a, sym, nm, fb, fts, holders, ntx, None, None, None, None,
                        0.0, 0.0, 0.0, supply / 10 ** dec,
                        "no WETH pair (launchpad curve or never listed)"))
        con.execute("UPDATE enrich SET pool_address=?, pool_block=?, liquidity=?, notes=? "
                    "WHERE address=?",
                    (out[-1][7], out[-1][8],
                     (f"{out[-1][12]:.6f} WETH / {out[-1][13]:,.0f} {sym}"
                      if out[-1][7] else out[-1][15]),
                     (f"fdv_usd={out[-1][10]:,.2f} pool_weth_usd={out[-1][11]:,.2f}"
                      if out[-1][10] is not None else "no priceable venue"), a))
    con.commit()

    print(f"\n{'#':<3} {'SYM':<14} {'blk':>7} {'holders':>8} {'txs':>7} "
          f"{'FDV USD':>14} {'POOL WETH($)':>13}  venue")
    for i, r in enumerate(sorted(out, key=lambda x: -(x[10] or 0)), 1):
        (a, sym, nm, fb, fts, holders, ntx, pool, pb, px, fdv, liq, w, tk, sup, kind) = r
        print(f"{i:<3} {str(sym)[:13]:<14} {fb:>7} {str(holders):>8} {str(ntx):>7} "
              f"{('$' + format(fdv, ',.0f') if fdv else '-'):>14} "
              f"{('$%.2f'%liq if liq else '-'):>13}  {pool or kind}")


def cmd_export(rpc, con, args):
    """Write out/all_tokens.csv and out/independent.csv."""
    os.makedirs(DATA_DIR, exist_ok=True)

    p1 = os.path.join(DATA_DIR, "all_tokens.csv")
    cols = ["rank", "first_block", "first_ts_utc", "symbol", "name", "address",
            "classification", "decimals", "total_supply", "deployer",
            "first_tx_to_factory", "mint_to", "creation_method", "detected_by",
            "first_mint_tx", "evidence"]
    n = 0
    with open(p1, "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(cols)
        for i, r in enumerate(con.execute("""
            SELECT first_block,first_ts,symbol,name,address,klass,decimals,
                   total_supply,deployer,first_tx_to,mint_to,creation_method,
                   source,first_tx,evidence
            FROM token ORDER BY first_block, address"""), 1):
            w.writerow([csv_safe(sanitize_text(x) if isinstance(x, str) else x)
                        for x in ([i, r[0], fmt_ts(r[1])] + list(r[2:]))])
            n = i
    print(f"wrote {p1} ({n} rows)")

    p2 = os.path.join(DATA_DIR, "independent.csv")
    cols2 = ["rank", "first_block", "first_ts_utc", "symbol", "name", "address",
             "deployer", "launchpad_or_factory", "total_supply_whole",
             "holders", "n_transfers", "last_transfer_utc", "active_30d",
             "pool_address", "pool_weth_usd", "fdv_usd", "deployer_pct_supply",
             "verified_on_blockscout", "obscurity", "evidence", "notes"]
    n2 = 0
    with open(p2, "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(cols2)
        for i, r in enumerate(con.execute("""
            SELECT t.first_block,t.first_ts,t.symbol,t.name,t.address,t.deployer,
                   t.first_tx_to,t.total_supply,t.decimals,
                   e.holders,e.n_transfers,e.last_transfer_ts,e.active_30d,
                   e.pool_address,e.liquidity,e.notes,e.deployer_pct,
                   e.verified,e.obscurity,t.evidence
            FROM token t LEFT JOIN enrich e ON e.address=t.address
            WHERE t.klass='Independent' AND e.address IS NOT NULL
            ORDER BY t.first_block"""), 1):
            dec = r[8] if r[8] is not None else 18
            supply = int(r[7]) / 10 ** dec if r[7] else 0
            notes = r[15] or ""
            fdv = pool_usd = ""
            for part in notes.split():
                if part.startswith("fdv_usd="):
                    fdv = part.split("=", 1)[1]
                if part.startswith("pool_weth_usd="):
                    pool_usd = part.split("=", 1)[1]
            w.writerow([csv_safe(sanitize_text(x) if isinstance(x, str) else x)
                        for x in [i, r[0], fmt_ts(r[1]), r[2], r[3], r[4], r[5],
                                  r[6], f"{supply:.0f}", r[9], r[10],
                                  fmt_ts(r[11]), r[12], r[13], pool_usd, fdv,
                                  f"{r[16]:.4f}" if r[16] is not None else "",
                                  r[17] or "unknown", r[18] or "unknown",
                                  r[19], notes]])
            n2 = i
    print(f"wrote {p2} ({n2} rows)")
    verify_csv(p1, p2)


def verify_csv(*paths):
    """Published CSVs must be clean text.

    Token names are attacker-controlled, so every export pulls fresh untrusted
    strings. This asserts the sanitiser actually held rather than trusting that
    it did: no NUL, no control bytes, no cell that a spreadsheet would execute.
    """
    bad = False
    for path in paths:
        with open(path, "rb") as fh:
            raw = fh.read()
        nul = raw.count(bytes([0]))
        ctrl = sum(1 for b in raw if b < 32 and b not in (9, 10, 13))
        formula = sum(1 for line in raw.split(b"\n")[1:]
                      for cell in line.split(b",")
                      if cell[:1] in (b"=", b"+", b"@"))
        ok = (nul == 0 and ctrl == 0 and formula == 0)
        bad = bad or not ok
        print(f"  integrity {os.path.basename(path)}: NUL={nul} control={ctrl} "
              f"formula_cells={formula} [{'OK' if ok else 'FAIL'}]")
    if bad:
        raise SystemExit("refusing to ship: CSV contains unsanitised content")


def cmd_launcher(rpc, con, args):
    """Every token a wallet has launched -- the lookup explorers cannot do.

    Keyed on first_tx_from (the transaction sender), not the contract creator,
    because for any launchpad-deployed token the creator is the launchpad.
    Pass a wallet to list its launches, or a token address to identify its
    launcher and then list everything else that wallet has launched.
    """
    q = (args.address or "").lower().strip()
    if not q.startswith("0x") or len(q) != 42:
        print("usage: scan.py launcher <wallet-or-token-address>", file=sys.stderr)
        return

    # If given a token, resolve to its launcher first.
    row = con.execute(
        "SELECT symbol, name, first_tx_from, first_tx_to FROM token WHERE address=?",
        (q,)).fetchone()
    if row and row[2]:
        print(f"{q} is a token: {row[0]} ({row[1]})")
        print(f"  explorers report creator : {row[3] or '(direct deploy)'}"
              f"   <- the launchpad, not a person")
        print(f"  actual launcher (tx.from): {row[2]}")
        q = row[2]
        print()

    launches = con.execute("""
        SELECT first_block, first_ts, symbol, name, address, first_tx_to, klass
        FROM token WHERE first_tx_from = ? ORDER BY first_block""", (q,)).fetchall()
    if not launches:
        print(f"no launches indexed for {q}")
        att = meta_get(con, "attribution_through_block")
        cur = meta_get(con, "pass_a_cursor", "0")
        if att:
            print(f"(launcher attribution covers blocks 0-{int(att):,}; token "
                  f"discovery reaches {int(cur)-1:,}. A launch above the "
                  f"attribution boundary is known but not yet attributed.)")
        else:
            print(f"(index covers blocks 0-{int(cur)-1:,}; a launch outside "
                  f"that range would not appear)")
        return

    lp = {a: v["label"] for a, v in ANCHORS["launchpads"].items()}
    print(f"{q} launched {len(launches)} token(s):\n")
    print(f"  {'block':>9}  {'when':<17} {'symbol':<14} {'via':<12} address")
    for fb, ts, sym, nm, addr, txt, k in launches:
        via = lp.get(txt, "direct" if not txt else "other")
        print(f"  {fb:>9,}  {fmt_ts(ts)[:16]:<17} {str(sym)[:13]:<14} {via:<12} {addr}")

    print(f"\n  nonce={int(rpc.call('eth_getTransactionCount', [q, 'latest']), 16)}"
          f"  balance={int(rpc.call('eth_getBalance', [q, 'latest']), 16)/1e18:.4f} ETH")


def cmd_attribute(rpc, con, args):
    """Populate launcher attribution WITHOUT the per-token metadata calls.

    `meta` costs 4 eth_calls per token (~50 tokens/s against the public
    endpoint), which is hours at chain scale. But launcher attribution -- the
    thing explorers cannot do, and the reason this index exists -- only needs
    the first-mint transaction, which is already in mint_first from the log
    scan. So seed token rows straight from SQL and resolve senders in batches.

    Metadata (name/symbol/decimals/supply) is a separate, resumable backfill:
    run `meta` afterwards for as long as you care to. Attribution works without
    it.
    """
    t0 = time.time()
    con.execute("""
        INSERT OR IGNORE INTO token
            (address, first_block, first_tx, mint_to, source, creation_method)
        SELECT m.address, m.first_block, m.tx_hash, m.mint_to, 'mint', 'unknown'
        FROM mint_first m""")
    con.commit()
    seeded = con.execute("SELECT COUNT(*) FROM token").fetchone()[0]
    print(f"token rows seeded from mint_first: {seeded:,} "
          f"({time.time()-t0:.1f}s)", file=sys.stderr)

    # Block timestamps are NOT needed to attribute a launcher, and at chain
    # scale they cost ~60k requests (eth_getBlockByNumber caps at 10/batch).
    # Opt-in only, so the core feature is never gated behind them.
    missing_ts = [] if not args.timestamps else [r[0] for r in con.execute(
        "SELECT DISTINCT first_block FROM token WHERE first_ts IS NULL")]
    if missing_ts:
        print(f"resolving {len(missing_ts):,} block timestamps", file=sys.stderr)
        for i in range(0, len(missing_ts), 500):
            grp = missing_ts[i:i + 500]
            tss = block_ts_bulk(rpc, grp)
            con.executemany("UPDATE token SET first_ts=? WHERE first_block=?",
                            [(v, k) for k, v in tss.items()])
            con.commit()
            if (i // 500) % 10 == 0:
                print(f"  ts {min(i+500,len(missing_ts)):,}/{len(missing_ts):,}",
                      file=sys.stderr)

    cmd_creators(rpc, con, args)
    remaining = con.execute(
        "SELECT COUNT(*) FROM token WHERE first_tx_from IS NULL AND first_tx "
        "IS NOT NULL AND first_block < ?", (args.max_block,)).fetchone()[0]
    if remaining == 0:
        bound = con.execute(
            "SELECT MIN(first_block) FROM token WHERE first_tx_from IS NULL "
            "AND first_tx IS NOT NULL").fetchone()[0]
        meta_set(con, "attribution_through_block",
                 (bound - 1) if bound else meta_get(con, "pass_a_cursor", "0"))
        con.commit()
        print(f"attribution boundary recorded: block "
              f"{meta_get(con, 'attribution_through_block')}", file=sys.stderr)
    done = con.execute(
        "SELECT COUNT(*) FROM token WHERE first_tx_from IS NOT NULL").fetchone()[0]
    print(f"attribution complete: {done:,}/{seeded:,} tokens have a launcher "
          f"({time.time()-t0:.0f}s total)", file=sys.stderr)


def cmd_selftest(rpc, con, args):
    """Verify every function's global references actually resolve.

    This exists because an over-eager dead-code removal once deleted two live
    helpers (probe_tokens_bulk, block_ts_bulk) that sat between a function and
    its neighbour. Python only raises NameError when the line finally runs, so
    the breakage surfaced hours later, mid-pipeline. Disassembling every
    function and checking its LOAD_GLOBAL targets catches it in a second.
    """
    import dis
    import builtins
    mod = sys.modules[__name__]
    known = set(dir(mod)) | set(dir(builtins))
    missing = []
    for name in dir(mod):
        fn = getattr(mod, name)
        if not callable(fn) or not hasattr(fn, "__code__"):
            continue
        if getattr(fn, "__module__", None) != __name__:
            continue
        for ins in dis.get_instructions(fn.__code__):
            if ins.opname == "LOAD_GLOBAL":
                ref = (ins.argval or "").lstrip("+")
                if ref and ref not in known:
                    missing.append((name, ref))
    for fname, ref in missing:
        print(f"  BROKEN: {fname}() references undefined global {ref!r}")
    cmds = [c for c in dir(mod) if c.startswith("cmd_")]
    print(f"checked {len(cmds)} commands, {len(known)} module names")
    if missing:
        raise SystemExit(f"selftest FAILED: {len(missing)} unresolved reference(s)")
    print("selftest OK: every global reference resolves")


def cmd_status(rpc, con, args):
    a = int(meta_get(con, "pass_a_cursor", "0"))
    b = int(meta_get(con, "pass_b_cursor", "0"))
    q = lambda s: con.execute(s).fetchone()[0]
    print(f"pass A cursor : {a:,}")
    print(f"pass B cursor : {b:,}")
    print(f"mint_first    : {q('SELECT COUNT(*) FROM mint_first'):,}")
    print(f"nft_first     : {q('SELECT COUNT(*) FROM nft_first'):,}")
    print(f"creation      : {q('SELECT COUNT(*) FROM creation'):,}")
    print(f"token         : {q('SELECT COUNT(*) FROM token'):,}")
    print(f"token erc20   : {q('SELECT COUNT(*) FROM token WHERE is_erc20=1'):,}")
    print(f"classified    : {q('SELECT COUNT(*) FROM token WHERE klass IS NOT NULL'):,}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["phase1", "pass-a", "pass-b", "meta",
                                    "creators", "clusters", "classify", "enrich", "mcap",
                                    "export", "attribute", "launcher", "selftest",
                                    "status"])
    ap.add_argument("--rpc", default=DEFAULT_RPC)
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--start", type=int, default=None)
    ap.add_argument("--end", type=int, default=1_000_000)
    ap.add_argument("--chunk", type=int, default=100_000, help="pass-a blocks/getLogs")
    ap.add_argument("--batch", type=int, default=200,
                    help="pass-b blocks per checkpoint (split to RPC caps internally)")
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--limit", type=int, default=25, help="enrich: how many tokens")
    ap.add_argument("--max-block", type=int, default=1 << 62,
                    help="attribute: only resolve tokens born below this block, "
                         "so attribution coverage is an exact contiguous prefix")
    ap.add_argument("--timestamps", action="store_true",
                    help="attribute: also backfill block timestamps (slow)")
    ap.add_argument("address", nargs="?", default=None,
                    help="launcher: wallet or token address to look up")
    args = ap.parse_args()

    rpc = Rpc(args.rpc, sleep=args.sleep)
    con = db_open(args.db)
    {"phase1": cmd_phase1, "pass-a": cmd_pass_a, "pass-b": cmd_pass_b,
     "meta": cmd_meta, "creators": cmd_creators, "clusters": cmd_clusters,
     "classify": cmd_classify, "enrich": cmd_enrich, "mcap": cmd_mcap,
     "export": cmd_export, "attribute": cmd_attribute,
     "launcher": cmd_launcher, "selftest": cmd_selftest,
     "status": cmd_status}[args.cmd](rpc, con, args)
    con.close()


if __name__ == "__main__":
    main()
