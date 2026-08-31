# robinhood-chain-token-index

**Block explorers report the wrong creator for every launchpad-deployed token on every EVM chain. This reconstructs the right one, and uses it to index a chain from genesis.**

Built against Robinhood Chain (chain ID 4663), but the attribution problem is
general: it applies to any chain where tokens are deployed through a factory.

---

## The problem

Ask Blockscout who created CASHCAT, a ~$212M memecoin:

```http
GET /api/v2/addresses/0x020bfC650A365f8BB26819deAAbF3E21291018b4
→ "creator_address_hash": "0xD9eC2db5f3D1b236843925949fe5bd8a3836FCcB"
```

That address is a **launchpad contract**, not a person. Within the range this
index attributes, it is reported as the creator of **60,112** tokens. The
chain's largest launchpad claims 167,000+ on its own site.

This is not a bug in Blockscout. The launchpad calls `CREATE2`, so at the
protocol level it genuinely *is* the deployer. But it means **filtering any EVM
explorer by "contracts created by this address" returns an empty list for even
the most prolific launcher** — the human who deployed the token appears nowhere
in the creation record.

The practical consequence: the "check what else this dev has shipped" workflow
that exists natively on Solana (pump.fun, Photon, Solscan surface it from the
bonding-curve account) silently does not work anywhere on EVM.

## The fix

The human is the **transaction sender**, not the contract creator:

```
tx.from   →  the account that deployed it          (indexed as first_tx_from)
tx.to     →  the launchpad it was deployed through (indexed as first_tx_to)
creator   →  always the launchpad. structurally useless.
```

Indexing `first_tx_from` for every token restores the lookup:

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

  nonce=662  balance=0.0065 ETH
```

CASHCAT was that wallet's **sixth** attempt. The five before it are worthless,
and it deployed another token four minutes afterwards. None of this is visible
on any explorer.

## Findings

Full writeup: [`docs/REPORT.md`](docs/REPORT.md).

**The chain ran for 62 days before its announced launch.** Block 1 is dated
`2026-04-30`; the public launch was `2026-07-01`, which binary search over
block timestamps places at block **653,325**. Early blocks are ~11s apart, not
the advertised 100ms — Nitro only produces blocks on demand.

**The first 27 tokens are all infrastructure.** WETH, Uniswap V2 pairs, bridged
LINK and wstETH, Paxos USDG, Ethena, Lido, and tokenised equities. The first
independently launched token is `MARIAN`, block **58,539**, deployed directly
by an EOA with ownership renounced, 16 days before the public launch.

**Two detection methods disagree, and both are right.** A mint-log scan finds
factory-deployed tokens; a contract-creation scan finds tokens that deploy
without ever minting. Reconciling them surfaced an ERC-20 at **block 56** with
`totalSupply() == 0` — six days older than anything the log scan can see.
Neither method subsumes the other, and the report documents all three causes of
divergence.

**Launch order does not predict outcome.** The first token settled near $1M;
the 136th and a later platform token both exceed $200M.

## How classification works

Every label is anchored to an on-chain relationship, never to a token's name.
Anchors live in [`config/known_addresses.json`](config/known_addresses.json)
with the evidence recorded per entry, so any label can be audited or disproved
without trusting the author.

| Signal | Example |
|---|---|
| **`owner()` correspondence** | A launchpad was identified because its `owner()` is the account that deployed a token named *"Testing NOXA Fun ;)"*. The `Bridged` bucket is tokens sharing an `owner()` with the contract that mints them. |
| **`first_tx_to` clustering** | Tens of thousands of tokens calling one contract identifies a launchpad without guessing. |
| **Deployer population statistics** | 16 distinct accounts deployed any token in the first 58,539 blocks; the next 38,000 blocks jump to 148. |

Tokens without sufficient evidence are labelled `Unclassified`, not assigned a
plausible-looking bucket. An earlier revision defaulted them to `Independent`,
which asserted 634k labels the data did not support.

## Engineering notes

**The endpoint's rate limiter is a per-IP token bucket priced by method cost,
not request count.** Calibrating it was the difference between a 100-minute
scan and a 100-hour one:

| | |
|---|---|
| `eth_getLogs` block-range cap | none — it caps on *results*: 10,000 logs |
| Batch cap, `eth_call` | 25 sub-requests (~200/s) |
| Batch cap, `eth_getBlockByNumber` (full bodies) | 10 sub-requests (~118 blocks/s) |
| Batch cap, `eth_getTransactionByHash` | 15 sub-requests (~15–20/s) |
| Parallel connections | **reduce** throughput: 3 threads measured 68 blk/s vs 118 serial |
| Default Python `User-Agent` | HTTP 403 |

Consequences that shaped the design:

- **An over-large batch is refused no matter how long you wait.** The fix is a
  smaller batch, not a longer sleep. An early version backed off aggressively on
  429s and got *slower* (8/s vs 15/s) because failed batches fell back to
  per-item calls.
- **`eth_getLogs` chunks are sized proportionally toward the 10,000-result
  cap.** Since the limiter counts requests, a chunk returning 4,000 logs wastes
  half a request; measured utilisation was 47% before this was fixed.
- **Attribution is decoupled from metadata.** Resolving `name`/`symbol` costs
  4 `eth_call`s per token; launcher attribution needs only the first-mint
  transaction, which the log scan already recorded. Separating them means the
  core capability isn't gated behind the slow cosmetic one.
- **Every stage checkpoints to SQLite** and is resumable. A full scan takes
  hours against a public endpoint and *will* be interrupted — the provider
  dropped connections repeatedly mid-run.

`scan.py selftest` disassembles every function and verifies its `LOAD_GLOBAL`
targets resolve. It exists because a dead-code removal once deleted two live
helpers; Python raises `NameError` only when the line executes, so the breakage
surfaced hours later mid-pipeline.

## Data integrity

`name()` and `symbol()` are arbitrary strings chosen by whoever deployed the
token, so the dataset is adversarial input by construction. Two real problems
this caused:

- **155 NUL bytes** reached the published CSV, enough that `file(1)` classified
  it as binary rather than text. ANSI escapes and bidi overrides ride the same
  path.
- **A token named `@hooddeploys`** — a leading `=`, `+`, `-` or `@` executes as
  a formula when the CSV is opened in Excel or Sheets.

Both are sanitised at decode *and* at export, and `export` now refuses to write
a file that fails its own integrity check. Covered by tests.

## Coverage

Three tiers with genuinely different ranges. Reporting one number for all three
would overstate what the index can answer.

| Tier | Provides | Coverage |
|---|---|---|
| **Discovery** | address + birth block, from the mint-log scan | blocks 0 – 22,583,345 · **635,045 tokens** |
| **Attribution** | the launcher account (`first_tx_from`) | blocks 0 – 9,000,000 · **125,961 tokens** |
| **Metadata** | name / symbol / decimals / supply | backfilling |

Attribution is a contiguous prefix, and `launcher` cites the boundary when it
cannot answer rather than returning an empty result — which would read as "this
account launched nothing," the most misleading possible failure for a
due-diligence lookup.

A separate contract-creation pass covers blocks 0–65,000. Chain-wide it would
take ~117 hours at the measured ceiling, so it is deliberately scoped to the
pre-launch era where the "first token" questions live.

## Limitations

- Prices and market caps are Blockscout's, not derived here. On-chain FDV is
  computed from Uniswap-V2-style reserves only; V3/V4 and bonding-curve venues
  are reported as unpriced rather than given a fabricated number.
- V3/V4 pool "TVL" is the pool's raw token balance, which overstates depth for
  concentrated liquidity.
- One account is treated as one identity. A deployer using a fresh wallet per
  launch looks clean.
- Single chain; no cross-chain clustering.

## Layout

```
scan.py                        pipeline (single file, subcommand dispatch)
config/known_addresses.json    classification anchors + recorded evidence
data/all_tokens.csv            every token, creation order, launcher, evidence
data/independent.csv           earliest independent launches, enriched
docs/REPORT.md                 full findings writeup
tests/                         decoding + sanitisation (stdlib unittest)
```

## Usage

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

.venv/bin/python scan.py phase1                    # verify chain, probe limits
.venv/bin/python scan.py pass-a  --end 50500000    # mint scan (the long pole)
.venv/bin/python scan.py pass-b  --end 65000       # contract-creation pass
.venv/bin/python scan.py attribute --max-block 9000000
.venv/bin/python scan.py meta    --max-block 9000000
.venv/bin/python scan.py classify
.venv/bin/python scan.py export

.venv/bin/python scan.py launcher 0x…              # the lookup explorers can't do
.venv/bin/python -m unittest discover tests
```

Read-only throughout: `eth_call` / `eth_get*` / `eth_chainId` only. No
transaction is ever constructed, signed or sent, and no keys are handled.

## License

MIT
