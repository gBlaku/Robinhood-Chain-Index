# robinhood-chain-token-index

**Block explorers name the wrong creator for every launchpad token on every EVM chain. This works out the real one, and uses it to index a chain from genesis.**

Built against Robinhood Chain (chain ID 4663), but the problem is general. It
shows up anywhere tokens get deployed through a factory.

---

## The problem

Ask Blockscout who created CASHCAT, a ~$212M memecoin:

```http
GET /api/v2/addresses/0x020bfC650A365f8BB26819deAAbF3E21291018b4
→ "creator_address_hash": "0xD9eC2db5f3D1b236843925949fe5bd8a3836FCcB"
```

That address is a launchpad contract. It's not a person. Within the range this
index covers it gets named as the creator of **60,112** different tokens. The
chain's biggest launchpad claims 167,000+ on its own site.

This isn't Blockscout getting it wrong. The launchpad calls `CREATE2`, so at the
protocol level it really is the deployer. But the effect is that **filtering any
EVM explorer by "contracts created by this address" gives you an empty list**,
even for someone who has shipped hundreds of tokens. The person who actually
deployed it isn't in the creation record at all.

So the "what else has this dev launched" check, which works out of the box on
Solana because pump.fun and Photon read it from the bonding curve account, just
quietly doesn't work on EVM.

## The fix

The person is the transaction sender, not the contract creator:

```
tx.from   →  who actually deployed it            (stored as first_tx_from)
tx.to     →  the launchpad they used             (stored as first_tx_to)
creator   →  always the launchpad. no use to anyone.
```

Index `first_tx_from` for every token and the lookup works again:

```console
$ scan.py launcher 0x020bfc650a365f8bb26819deaabf3e21291018b4

0x020bfc650a365f8bb26819deaabf3e21291018b4 is a token: CASHCAT (Cash Cat)
  explorers report creator : 0xd9ec2db5f3d1b236843925949fe5bd8a3836fccb   <- the launchpad
  actual launcher (tx.from): 0xcdfc08a1c1fbafb355645e5ddc32122e5716ca90

0xcdfc08a1c1fbafb355645e5ddc32122e5716ca90 launched 7 token(s):

      block  when              symbol         via          address
     71,699  2026-06-17 14:04  SHR            NOXA Fun     0xe8dfea4c…
     73,889  2026-06-17 14:53  TH             NOXA Fun     0xf16a1a03…
     75,571  2026-06-17 15:28  AIMLESS        NOXA Fun     0x8d05f9ca…
     76,761  2026-06-17 15:53  LCH            NOXA Fun     0xc643afa8…
     79,013  2026-06-17 17:03  HB             NOXA Fun     0x15b82256…
     88,836  2026-06-18 20:01  CASHCAT        NOXA Fun     0x020bfc65…   ← $212M
     88,907  2026-06-18 20:05  TEST           NOXA Fun     0x25267b09…
```

CASHCAT was that wallet's sixth try. The five before it went nowhere, and it
launched another token four minutes later. You can't see any of that on an
explorer.

## What I found

Full writeup is in [`docs/REPORT.md`](docs/REPORT.md). The short version:

**The chain was running 62 days before it launched.** Block 1 is dated
2026-04-30. The announced launch was 2026-07-01, which a binary search over
block timestamps puts at block 653,325. Early blocks are about 11 seconds apart
rather than the advertised 100ms, because Nitro only makes blocks when there's
something to put in them.

**The first 27 tokens are all infrastructure.** WETH, Uniswap V2 pairs, bridged
LINK and wstETH, Paxos USDG, Ethena, Lido, tokenised equities. The first token
anyone actually launched is MARIAN at block 58,539, deployed straight from an
EOA with ownership renounced, 16 days before the public launch.

**The two ways of finding tokens disagree, and both are correct.** Scanning mint
logs catches factory-deployed tokens. Scanning for contract creations catches
tokens that deploy without ever minting. Reconciling the two turned up an ERC-20
at block 56 with `totalSupply() == 0`, six days older than anything the log scan
can possibly see. Neither method covers the other, and the report goes through
all three reasons they diverge.

**Being early didn't help.** The first token sits around $1M. The 136th, and a
platform token from a month later, are both over $200M.

## How classification works

Every label comes from something observable on chain, never from a token's name.
The anchors live in [`config/known_addresses.json`](config/known_addresses.json)
with the evidence written next to each one, so you can check or disprove any of
them without taking my word for it.

| Signal | Example |
|---|---|
| `owner()` matching | One launchpad was identified because its `owner()` is the account that deployed a token called *"Testing NOXA Fun ;)"*. The `Bridged` bucket is tokens that share an `owner()` with whatever mints them. |
| `first_tx_to` clustering | Tens of thousands of tokens all calling one contract tells you it's a launchpad. No guessing needed. |
| Deployer population stats | 16 distinct accounts deployed any token in the first 58,539 blocks. Over the next 38,000 blocks that jumps to 148. |

Anything without enough evidence gets labelled `Unclassified` rather than
dropped into a bucket that looks about right. An earlier version defaulted those
to `Independent`, which meant asserting 634k labels the data didn't support.

## Engineering notes

**The rate limiter is a per-IP token bucket priced by how expensive the method
is, not by how many requests you send.** Working that out was the difference
between a 100 minute scan and a 100 hour one.

| | |
|---|---|
| `eth_getLogs` block range cap | none. It caps on *results* instead: 10,000 logs |
| Batch cap, `eth_call` | 25 sub-requests (~200/s) |
| Batch cap, `eth_getBlockByNumber` (full bodies) | 10 sub-requests (~118 blocks/s) |
| Batch cap, `eth_getTransactionByHash` | 15 sub-requests (~15 to 20/s) |
| Parallel connections | make it *worse*. 3 threads measured 68 blk/s against 118 single threaded |
| Default Python `User-Agent` | HTTP 403 |

Things that fell out of this:

- **A batch that's too big gets refused no matter how long you wait.** The fix is
  a smaller batch, not a longer sleep. An early version backed off hard on 429s
  and ended up *slower* (8/s versus 15/s) because failed batches fell back to
  one call per item.
- **`eth_getLogs` chunks are sized to aim just under the 10,000 result cap.**
  The limiter counts requests, so a chunk that comes back with 4,000 logs has
  thrown away half a request. Measured utilisation was 47% before this got fixed.
- **Attribution is separate from metadata.** Getting `name` and `symbol` costs 4
  `eth_call`s per token. Working out who launched it only needs the first mint
  transaction, which the log scan already recorded. Splitting them means the part
  that matters isn't stuck behind the part that's just cosmetic.
- **Every stage checkpoints to SQLite and can resume.** A full scan takes hours
  against a public endpoint and will get interrupted. The provider dropped
  connections on me repeatedly.

`scan.py selftest` disassembles every function and checks its `LOAD_GLOBAL`
targets actually exist. It's there because a dead code cleanup once deleted two
functions that were still in use. Python only raises `NameError` when the line
runs, so it didn't show up until hours later, halfway through a pipeline.

## Data integrity

`name()` and `symbol()` are arbitrary strings picked by whoever deployed the
token, so this dataset is hostile input by default. Two real problems it caused:

- **155 NUL bytes** made it into the published CSV. Enough that `file(1)` called
  it binary instead of text. ANSI escapes and bidi overrides come in the same way.
- **A token called `@hooddeploys`.** Anything starting with `=`, `+`, `-` or `@`
  runs as a formula when you open the CSV in Excel or Sheets.

Both get sanitised at decode time and again at export, and `export` now refuses
to write a file that fails its own integrity check. Both are covered by tests.

## Coverage

Three layers, three different ranges. Quoting one number for all of them would
oversell what this can actually answer.

| Layer | What you get | Coverage |
|---|---|---|
| Discovery | address and birth block, from the mint log scan | blocks 0 to 22,583,345 · **635,045 tokens** (in `scan.db`, not shipped) |
| Attribution | who launched it (`first_tx_from`) | blocks 0 to 9,000,000 · **125,961 tokens** |
| Metadata | name, symbol, decimals, supply | backfilling |

Attribution is always a contiguous prefix, and `launcher` tells you where the
boundary is when it can't answer, instead of returning nothing. Returning
nothing would read as "this account has never launched anything", which is the
worst possible way for a due diligence tool to fail.

There's also a contract creation pass covering blocks 0 to 65,000. Doing that
chain-wide would take about 117 hours at the measured ceiling, so it's
deliberately limited to the pre-launch stretch where the "first token" questions
actually live.

## Limitations

- Prices and market caps come from Blockscout, not from here. On-chain FDV is
  worked out from Uniswap V2 style reserves only. V3, V4 and bonding curve
  venues get reported as unpriced rather than given a made up number.
- V3 and V4 "TVL" is just the pool's raw token balance, which overstates depth
  for concentrated liquidity.
- One account is treated as one identity. Someone using a fresh wallet per
  launch looks clean here.
- One chain. No cross-chain clustering.

## Layout

```
scan.py                        the pipeline, one file, subcommand dispatch
config/known_addresses.json    classification anchors and their evidence
data/all_tokens.csv.gz         125,961 attributed tokens, creation order
data/first_2000_tokens.csv     the same table's opening, browsable on GitHub
data/independent.csv           earliest independent launches, enriched
docs/REPORT.md                 full writeup
tests/                         decoding and sanitisation, stdlib unittest
```

The full table ships gzipped because GitHub will not preview anything over
about 5MB, and the raw CSV is 55MB. `first_2000_tokens.csv` is the same data,
truncated so you can actually read it in the web UI. Both cover the attributed
range. The wider discovery set (635,045 tokens, address and block only) stays in
the local SQLite database rather than the repo, since those rows carry no
launcher, no metadata, and an identical placeholder string repeated half a
million times.

## Usage

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

.venv/bin/python scan.py phase1                    # check chain, probe limits
.venv/bin/python scan.py pass-a  --end 50500000    # mint scan, the slow one
.venv/bin/python scan.py pass-b  --end 65000       # contract creation pass
.venv/bin/python scan.py attribute --max-block 9000000
.venv/bin/python scan.py meta    --max-block 9000000
.venv/bin/python scan.py classify
.venv/bin/python scan.py export

.venv/bin/python scan.py launcher 0x…              # the lookup explorers can't do
.venv/bin/python -m unittest discover tests
```

Read only throughout. It issues `eth_call`, `eth_get*` and `eth_chainId` and
nothing else. It never builds, signs or sends a transaction, and never touches
a key.

## License

MIT
