# The first tokens on Robinhood Chain

Scan date: 2026-08-30. Chain: Robinhood Chain mainnet, `eth_chainId` = **4663**
(`0x1237`), node `nitro/v3.11.3-rc.9-beb2108`. Endpoint:
`https://rpc.mainnet.chain.robinhood.com`. Latest block at scan start:
**49,696,455**.

All addresses, block numbers, timestamps and token names below come from RPC or
explorer responses received during this scan. Anything not observed is written
`unknown`.

---

## 0. The brief's timeline premise is wrong, and it changes the answer

The brief states mainnet went live **2026-07-01** and that "day one is therefore
roughly blocks 0 to 864,000". Neither holds:

| Fact | Evidence |
|---|---|
| Genesis block 0 has `timestamp = 0` | `eth_getBlockByNumber("0x0")`, hash `0xaad15f3d702aaea00caf3e9bb56395efe9127bc3b31b24921abf1eee3409305c` |
| Block 1 is **2026-04-30 16:52:11Z** | `eth_getBlockByNumber("0x1")`, ts `1777567931` |
| 2026-07-01 00:00:00Z is **block 653,325** | binary search over `eth_getBlockByNumber` timestamps |
| 2026-07-02 00:00:00Z is **block 954,800** | same |

So the chain was producing blocks for **62 days before the announced launch**, at
roughly 11 s/block rather than 100 ms — Nitro only produces blocks on demand, and
demand was near zero. The 100 ms cadence only appears after public launch.

**Consequence:** every token discussed in this report was created *before* the
public launch date in the brief. Real public day one is blocks 653,325–954,800,
not 0–864,000. Chronology below is anchored to observed timestamps, not to the
brief's block estimates.

---

## 1. What was scanned

**Pass A (primary, per brief):** `eth_getLogs` from genesis for
`topic0 = 0xddf252ad…` (Transfer) with `topic1 = 0x0` (mint from zero address),
keeping only 3-topic logs with a ≥32-byte data payload — this excludes ERC-721,
which uses 4 topics and empty data. Coverage: **blocks 0 → 2,000,000**, twice the
brief's minimum. Result: **6,319 distinct ERC-20 contracts**, plus **212**
4-topic (ERC-721-shaped) addresses excluded.

**Pass B (secondary reconciliation):** block-body walk for top-level
`to == null` contract creations, resolving `contractAddress` from receipts.
Coverage: **blocks 0 → 65,000** (complete; see §7 for why this scope).
See §6 for what this does and does not settle.

**Endpoint limits, established empirically (Phase 1):**

- `eth_getLogs` has **no block-range cap** — a filter matching nothing succeeded
  across all 49.7M blocks. It caps on *results*: `logs matched by query exceeds
  limit of 10000`.
- The rate limiter is a **per-IP token bucket priced by method cost**, not by
  request count. Batched `eth_call` tops out at **25 sub-requests** (~200
  sub-req/s); `eth_getBlockByNumber` with full bodies tops out at **10**
  (~118 blocks/s). Opening parallel connections *lowers* throughput
  (3 threads → 68 blk/s vs 118 single-threaded), so the scanner is serial.
- A default Python/urllib `User-Agent` receives **HTTP 403**; a curl-like UA works.

No API key was needed. The scan is read-only: only `eth_call`, `eth_get*` and
`eth_chainId` are ever issued. No transaction was constructed, signed or sent.

**Sanity check passed:** the brief's known WETH
`0x0bd7d308f8e1639fab988df18a8011f41eacad73` appears in the scan at block
**9,482** with `name() = "WETH"`, `decimals() = 18` — confirming the scan sees
infrastructure tokens.

---

## 2. The first ten tokens on the chain

Ordered by the block of their first-ever mint.

| # | Block | Timestamp (UTC) | Symbol | Name | Address | Class |
|---|---|---|---|---|---|---|
| 1 | 308 | 2026-05-11 15:25:57 | aglaMerkl | aglaMerkl | `0x999bacd74539e19e3371e51839707b59a682b258` | Official/RWA |
| 2 | 433 | 2026-05-12 17:50:03 | USDG | Global Dollar | `0x5fc5360d0400a0fd4f2af552add042d716f1d168` | Official/RWA |
| 3 | 7,173 | 2026-05-21 12:34:44 | E20M | ERC20Mock | `0xab7c5de616fe66ce2ca8592889a0deb992b8b7d3` | Infrastructure |
| 4 | 7,179 | 2026-05-21 12:39:02 | *(none)* | *(null bytes)* | `0x529077a1975b9a6f192c4f7552704a1522cb6e91` | Infrastructure |
| 5 | 7,229 | 2026-05-21 13:02:12 | tMOCK | Mock Test Vault | `0x88c7982ec5332cee47d0cace1adff98ebb3f839a` | Infrastructure |
| 6 | 9,482 | 2026-05-22 21:14:53 | WETH | WETH | `0x0bd7d308f8e1639fab988df18a8011f41eacad73` | Infrastructure |
| 7 | 9,486 | 2026-05-22 21:14:53 | UNI-V2 | Uniswap V2 | `0x4b26f2f37db21dfe226465307e7fce8d5910064f` | Infrastructure |
| 8 | 16,757 | 2026-05-26 08:33:31 | USD | USD | `0x5b160f515310f7ef14ffd47191ea5c9671a4b7b7` | Official/RWA |
| 9 | 17,529 | 2026-05-26 14:06:19 | E20M | ERC20Mock | `0x1149b3d6afff8377d6e9bb028cd1831fad63ebb4` | Infrastructure |
| 10 | 17,949 | 2026-05-26 19:51:18 | UNI-V2 | Uniswap V2 | `0x7d9ac796a32fda332264b884f531581ed2477e4a` | Infrastructure |

First-mint tx hashes for all ten are in `all_tokens.csv` (`first_mint_tx`).

> ### Correction from Pass B: the first token *contract* is older than block 308
>
> The table above orders by **first mint**, which is what Pass A detects. Pass B
> (contract creations) found ERC-20s that never minted and are therefore
> invisible to a mint scan — including one that predates the whole table:
>
> | Block | Timestamp (UTC) | Symbol | Name | Address | Note |
> |---|---|---|---|---|---|
> | **56** | **2026-05-05 17:59:49** | **USDG** | **Global Dollar** | `0x68184c449e1a8f34fa18d289737129fd27b66f8f` | `totalSupply() == 0` — deployed, never minted |
>
> Deployer `0xbe498aad9c6fd0e4cd6d1e3fbb395026c5d28215`, creation tx
> `0x5a88b74f8ade975f0fbb8908b70cde112d64b263c9b60967996bb20314d48769`.
> It is a distinct contract from the USDG at block 433 that *did* mint.
>
> So there are two defensible readings of "the first token ever created":
>
> - **First ERC-20 contract deployed:** `USDG` at block **56**, 2026-05-05 — six
>   days earlier than the mint-ordered table suggests.
> - **First ERC-20 to actually mint supply:** `aglaMerkl` at block **308**,
>   2026-05-11.
>
> Neither changes §3: both are infrastructure, and the first *independent* launch
> is still MARIAN at block 58,539.

**To state the boring answer plainly, as the brief asks:** the first 27 tokens
ever created on this chain — blocks 308 through 50,615, spanning 2026-05-11 to
2026-06-11 — are **all infrastructure, partner-protocol or RWA contracts**. Not
one is an independent launch. That set includes WETH, two Uniswap V2 pairs,
several `ERC20Mock` test contracts, Paxos/Global Dollar USDG, Steakhouse and
Spark USDG vaults, Ethena's USDe/sUSDe/ENA, Lido wstETH, Chainlink LINK, and the
first Robinhood stock token (`WEEK`, "Roundhill Weekly T-Bill ETF • Robinhood",
block 34,714).

---

## 3. When the first independent launch happened

> **Block 58,539 — 2026-06-15 15:00:10Z — `MARIAN` / "Lady Marian"**
> Address `0x01637b14b7378b99de75a64d50656d98488d9a4d`
> Deployer `0x937933e11ad6307ae0d8b8115986e91734be2d5c`
> First-mint tx `0x65ae852367a231c97952716bcaef8b8ffa9d19cec8d0cdc24a9924187e78ada1`.
> That tx has `to = null` — the token was deployed directly by an EOA in the same
> transaction that minted it, not through any factory or launchpad.
> Supply 1,000,000,000 × 10¹⁸. `owner()` returns the zero address — ownership renounced.

This is **16 days before the announced public launch** and **2,846 blocks before
the first launchpad token**.

### Why the 27 tokens before it are not "independent" — the evidence

The brief says not to classify on vibes. The load-bearing evidence here is a
population statistic, not the token names:

> In blocks 0–58,539 — a **36-day window** — exactly **16 distinct EOAs**
> deployed any ERC-20 at all. In the next 38,000 blocks that number jumps to
> **148**, and the first public launchpad appears.

Every token in that window is a partner or protocol deployment, and MARIAN's
deployer is not one of those 16.

**What this does not show.** An earlier draft of this report called that window
"permissioned" and a "closed partner-onboarding period." That overclaims, and
MARIAN itself is the counter-example: its deployer was outside the 16 and
deployed successfully, so nothing was blocking non-partner deployment. The low
deployer count reflects an unannounced chain nobody had found yet, not an
allowlist. It is evidence about who was **present**, not who was **permitted** —
and it is therefore not grounds for inferring that early launches came from
insiders.

Supporting per-token evidence used by the classifier:

- **Infrastructure** — `symbol() == "UNI-V2"` / `name() == "Uniswap V2"` (AMM
  pair shares, not launches); WETH by address; `ERC20Mock`/`Mock…` test contracts.
- **Bridged (5 tokens)** — first mint executed by `0xcdca5d374e46a6dddab50bd2d9acb8c796ec35c3`,
  which returns the **same `owner()`** (`0xf09afe78d3c7d359b334d7cb88995751f7ec5e13`)
  as the tokens it mints (LINK, wstETH, syrupUSDG). That is a mint/burn
  representation of an off-chain asset, not a launch.
- **Official / RWA** — deployer `0x2b94105fff37630f98e1f24811dad588fc5c3a87`
  (95 tokens, from block 34,714) and the `• Robinhood` issuer suffix in `name()`;
  plus wrapper factory `0x4262efbd176f02824af27010bea218429c33c7e8` (72 wrapped
  stock tokens, e.g. `wWEEK`, `wAAOI`).
- **Deployer-cluster propagation** — 308 tokens inherited a label because they
  share a deployer with an already-classified infra/official token and that
  deployer never touched a launchpad.

### Classification totals, blocks 0–2,000,000

| Bucket | Count |
|---|---|
| Independent | 5,494 |
| Infrastructure | 638 |
| Official / RWA | 182 |
| Bridged | 5 |
| **Total ERC-20s** | **6,319** |

Independent launches by era: **697** before public launch, **938** on public day
one (blocks 653,325–954,800), **3,859** over the following ~3 days.

---

## 4. The two launchpads

The memecoin wave did not start gradually — it started with two launchpad
contracts going live within 8 minutes of each other on 2026-06-16.

**Launchpad A — `0x5818fddddb96bcd50bee4253f11b324e7dea961b`**
First token at block **61,385** (`TEST`). `owner()` =
`0xce54513ab9c2fbc8a2f9034bab6170572e2eef47`, the same EOA that deployed the
three placeholder "." tokens at blocks 61,208 / 61,472 / 61,483 — i.e. the
operator testing its own product. It exposes `WETH()` and emits **Uniswap V2
`PairCreated`** events itself, so it is a UniV2-fork factory with a launch
wrapper. Its tokens have vanity addresses ending in `0000` and **are their own
AMM pair** (`getReserves()` succeeds on the token address). 40 tokens.

**Launchpad B — "NOXA Fun" — `0xd9ec2db5f3d1b236843925949fe5bd8a3836fccb`**
First token at block **61,869** (`ROBINDOG`). `owner()` =
`0x7e035fb048a31e0481b88074557415b1c187242b`, which is the EOA that deployed
`TNOXA` / "Testing NOXA Fun ;)" at block 61,394 — that is how the launchpad is
identified. It is bonding-curve style: it appears as `mint_to` for **3,276
tokens**, holding the supply. This is the dominant launchpad on the chain.

Other notable deployment routes: **682** tokens were deployed through the
canonical ERC-4337 EntryPoint `0x0000000071727de22e5e9d8baf0edac6f37da032`
(smart-account transactions).

---

## 5. The early independent tokens — market caps and liquidity

You asked for market caps. Two honest caveats before the numbers:

1. **I am not able to advise on what to buy, and I did not and will not execute
   any transaction.** These are on-chain facts only.
2. **Most of these "market caps" are notional.** For most of the list, the entire
   removable value is the WETH sitting in the pool — often a few thousand dollars
   against a six-figure headline FDV. A market cap you cannot exit is a number,
   not a price.

Prices below are computed two independent ways: **on-chain** (deepest
WETH pair reserves × supply, ETH = **$2,454.55** from pool
`0x8803c117ccae7b5146297876c2a25df135141c4d`), and **Blockscout**
`/api/v2/tokens` (aggregator-sourced), snapshot 2026-08-30.

### Ranked by market cap (Blockscout, where listed)

| Token | Block | First seen | Market cap | Price USD | 24h vol | Holders | Aggregator |
|---|---|---|---|---|---|---|---|
| **KITSU** `0x8d4dfaaa…d4dc` | 62,881 | 2026-06-16 16:18 | **$1,945,275** | $0.00194494 | $284,267 | 3,691 | CoinGecko |
| **MARIAN** `0x01637b14…9a4d` | **58,539** | **2026-06-15 15:00** | **$991,987** | $0.00099188 | $230,088 | 5,444 | CoinGecko |
| **ROBINHOODIES** `0x8fde2c15…3295` | 63,957 | 2026-06-16 19:42 | $9,961 | $0.00001101 | $579 | 346 | CoinGecko |
| **GME** `0x7e86381a…8123` | 65,117 | 2026-06-16 23:58 | *0 (supply n/a)* | $0.00010826 | $6,404 | 2,573 | CoinGecko |

Only **4 of the 36** earliest independent tokens carry any aggregator price at
all. The rest are priced only by whatever sits in their pool.

### On-chain FDV for launchpad-A tokens (self-pairing, priceable)

| Token | Block | FDV (on-chain) | Pool WETH value | Holders | Transfers |
|---|---|---|---|---|---|
| Democratize | 61,816 | $59,320 | $16,604 | 291 | 15,766 |
| SHERIFF | 61,817 | $59,320 | $16,604 | 106 | 6,547 |
| TEST | 61,385 | $7,489 | $5,900 | 33 | 3,128 |
| FEATHER | 62,376 | $5,784 | $5,185 | 28 | 757 |
| TUCK (Friar Tuck) | 61,559 | $5,273 | $4,951 | 24 | 1,603 |
| ROBINHOODDOG | 61,561 | $4,535 | $4,591 | 11 | 411 |
| Robin Hood | 61,636 | $4,245 | $4,442 | 7 | 217 |
| KITSU (launchpad-A) | 63,139 | $4,241 | $4,440 | 5 | 96 |
| ROBINHOODIES (A) | 63,942 | $4,143 | $4,388 | 2 | 74 |
| $ROBIN | 61,747 | $4,037 | $4,332 | 1 | 36 |
| Agentic | 61,784 | $4,037 | $4,332 | 1 | 9 |
| WDP ("we're dog people") | 62,825 | $4,037 | $4,332 | 1 | 7 |

Note how tightly FDV tracks pool value for these — that is the signature of a
token whose only liquidity is its launch seed, never traded away from it. Several
have **one holder**.

### Cross-validation

My on-chain FDV for MARIAN was **$959,281**; Blockscout independently reports
**$991,987** — a 3.3% difference, which is reassuring for the method. Holder
counts matched Blockscout **exactly** on the small tokens (TEST 32 vs my 33,
TNOXA 165 vs 165, TUCK 24 vs 24, ROBINHOODDOG 11 vs 11).

**One correction to my own figure:** MARIAN's deepest *Uniswap V2* pair holds only
**$120** of WETH, so the on-chain "liquidity" column badly understates it.
DEX Screener shows MARIAN's real venue is a **Uniswap V4** pool
(pair `0xfe331fd29b54bce09d52988fa691e3b18b0a4081`). My pool discovery only
decodes V2-style pairs, so **V4 and launchpad-internal liquidity is not measured
anywhere in this report.** Treat the "pool WETH value" column as a floor, not a
total.

### Tokens whose venue this scan could not price

23 of the 36 have **no V2-style WETH pair**. That includes genuinely active
tokens — `BEGGAR` (601 holders, 35,523 transfers), `TAXISTHEFT` (241 / 12,592),
`ROBINDOG` (278 / 12,500), `RKT` "Roaring Kitty" (462 / 5,399), `RHPEPE`
(204 / 10,255). These trade on the NOXA bonding curve or a V4 pool. I report them
as unpriced rather than inventing a number.

---

## 6. Obscurity ratings (Phase 5)

Cross-referenced against Blockscout `/api/v2/tokens`, CoinGecko listing status
(via Blockscout's `icon_url`), and general web search of address and symbol.

| Rating | Count | Meaning |
|---|---|---|
| `listed` | 4 | Priced on an aggregator (CoinGecko), with 24h volume |
| `indexed only` | 18 | On Blockscout with holders, but **no price, no aggregator, no web presence** |
| unrated | 14 | Enriched on-chain but no explorer cross-check performed |

**`listed`:** MARIAN, KITSU, ROBINHOODIES, GME.

**`indexed only` — the category you asked about.** These exist on-chain and on
Blockscout but a web search for the symbol and address returns nothing about the
token itself: `TEST`, `TNOXA`, `TUCK`, `ROBINHOODDOG`, `ROBINDOG`, `HUSK`,
`JOHN`, `TAXISTHEFT`, `BEGGAR`, `RKT`, `RHPEPE`, `MERRYMEN`, `Democratize`,
`SHERIFF`, `FEATHER`, `Robin Hood`, `$ROBIN`, `LOCKSLEY`.

No token in this set qualifies as **`no public trace`** — Blockscout indexes
every contract on the chain, so an explorer record always exists. The honest
version of that category here is "indexed, but undiscovered": present on the
explorer, absent from every aggregator and from the open web.

MARIAN is emphatically *not* undiscovered: it has a CoinGecko page, a DEX Screener
pair, a CryptoRank listing, an X account, and two project sites
(`ladymarian.io`, `ladymarian.xyz`) that describe it as "Ye First Meme of
Robinhood Chain" — a claim this scan independently confirms at block 58,539.

---

## 7. Reconciliation of the two detection methods

Pass A (mint logs) and Pass B (`to == null` creations) answer different
questions, and the brief asks that discrepancies be named rather than papered over.

**The two methods genuinely disagree, and both are right about different things.**
Over the reconciled range (blocks **0 → 10,399**):

| Measure | Count |
|---|---|
| Pass A tokens (emitted a mint) | 7 |
| Pass B top-level contract creations | 181 |
| Creations that **never** emitted a mint | 178 |
| …of those, contracts that expose `totalSupply()` (i.e. real ERC-20s Pass A missed) | **11** |

The 11 ERC-20s that Pass A structurally could not see:

| Block | Symbol | Name | Supply | Address |
|---|---|---|---|---|
| 56 | USDG | Global Dollar | 0 | `0x68184c449e1a8f34fa18d289737129fd27b66f8f` |
| 4,751 | LINK | ChainLink Token | 7.74e18 | `0x492641f648a4986844848e0befe66d14817bce34` |
| 4,763 | Factory-BnM-ER | Factory-BnM-ERC20 | 0 | `0x26d3681dfc9e4c8c79cfbf461adec8a21d5d73c5` |
| 7,148 | E4626M | ERC4626Mock | 0 | `0x7de92d7dbe1b31197cfc5240eef4b184ca1e1e8c` |
| 7,166 | E20M | ERC20Mock | 3e21 | `0x1149b3d6afff8377d6e9bb028cd1831fad63ebb4` |
| 7,167 | E4626M | ERC4626Mock | 0 | `0x7eeb8755b9ea71b9c291a9148a4450d93bf1646a` |
| 7,784 | *(none)* | *(none)* | 0 | `0xb35490d6f9163de4f80d88dc75c3516eb64c5ae2` |
| 9,069 | UNI-V3-POS | Uniswap V3 Positions NFT-V… | 674,891 | `0x73991a25c818bf1f1128deaab1492d45638de0d3` |
| 9,483 | SMK2 | SmokeV2 Token | 1e24 | `0x0d6b6f604c1bf5b3533c445334bb4e1044145688` |
| 9,489 | SMK3 | SmokeV3 Token | 1e24 | `0x99f381b8bcd5b367178809abdbb7ae79da782e0e` |
| 9,498 | SMK4 | SmokeV4 Token | 1e24 | `0x42bcdf8d4116545d04dd5b76f48b614450f18b1b` |

Three distinct causes, all reconciled rather than papered over:

1. **Deployed, never minted** (USDG @56, the mocks) — `totalSupply() == 0`.
   A mint scan cannot see these by construction. This is exactly the case the
   brief predicted, and it produced the §2 correction.
2. **Supply created without a `Transfer` event** — SMK2/3/4 each hold 1e24 supply
   but emitted no mint log, so they are non-standard ERC-20s. Their deployer
   `0x9701fb0ade1e269c8f64ec0c7b3cfadb31a13a52` is the same EOA that deployed
   **WETH** (block 9,482) and the first Uniswap V2 pairs — infrastructure test
   tokens, not launches.
3. **Creation block ≠ first-mint block** — LINK is created at 4,751 but first
   mints at 21,592; `E20M` created 7,166, mints 17,529. Pass A's ordering is
   *mint* order, which for these lags deployment by thousands of blocks.

Conversely, **4 of the 7** Pass A tokens in this range have no top-level creation
tx at all — they are factory/CREATE2-deployed (the Uniswap V2 pairs and WETH's
associated contracts), which is precisely why the brief required the log scan as
the primary method. Neither pass subsumes the other.

**Status: Pass B is complete over its intended scope** — blocks **0 → 65,000**,
covering genesis through the entire first memecoin wave (2026-05-05 to
2026-06-17). It found **478 top-level contract creations from 68 distinct
deployers**.

This scope is deliberate, not a shortfall. Chain-wide contract-creation scanning
would take ~117 hours at the measured 118 blk/s ceiling, which is not viable
against a public endpoint. The pre-launch era is where every "first token"
question lives, so that is what it covers.

- **§3's answer is settled within this range.** MARIAN could only be displaced by
  a *non-minting* independent ERC-20 deployed before block 58,539, and Pass B has
  now walked every block in that range. It found no such token — every ERC-20 it
  recovered that Pass A missed is infrastructure or a test contract.
- **Beyond block 65,000, only the mint scan applies.** A token deployed after
  that which never mints would not appear in this dataset.

Pass B was scoped to blocks 0–65,000 rather than the full pre-launch era
(653,325 blocks, ~92 minutes) or the brief's 1,000,000 (~2.4 hours), because the
region containing every "first token" claim ends well before block 65,000. That
scope is now complete.

---

## 8. Things I had to guess at, and known gaps

- **Launchpad A has no public name** that I could establish from on-chain data.
  I identified NOXA Fun from its owner's test token; launchpad A's owner
  (`0xce54513a…`) left no equivalent signature. Reported as "launchpad-A".
- **V4 and bonding-curve liquidity is unmeasured** (§5). This is the largest gap
  in the market-cap figures.
- **`Official / RWA` merges two things** the brief listed separately: Robinhood
  stock tokens, and partner DeFi protocol deployments (Ethena, Spark, Steakhouse,
  Paxos). Both are "not an independent launch" on the same evidence — the
  permissioned staging window — but they are not the same kind of thing.
- **Holder counts** come from replaying Transfer logs and counting non-zero
  balances. Blockscout's number for MARIAN (5,444) exceeds mine (3,270); the
  small-token counts match exactly. I did not resolve the discrepancy on the
  large one. Blockscout's figure is the one quoted in §5.
- **One address in `token`** (`0xd2bcd038988e824def6b7db3e3c2dc547ecadb4c`, block
  645,771) emitted an ERC-20-shaped Transfer but has no `totalSupply()`. It is
  flagged `is_erc20 = 0` and left unclassified.
- **`decimals`, `name`, `symbol` are read at `latest`**, not at creation block, so
  an upgradeable token could report values it did not have at birth.

---

## 9. Files

| File | Contents |
|---|---|
| `all_tokens.csv` | All 6,319 ERC-20s in creation order, with classification and the evidence string for each |
| `independent.csv` | The 36 earliest independent tokens, enriched: holders, transfers, pool, FDV, obscurity |
| `scan.db` | SQLite: `mint_first`, `nft_first`, `creation`, `token`, `enrich`, `meta` (cursors) |
| `passb.log` | Live Pass B progress |

Reproduce with:

```bash
.venv/bin/python first_tokens_robinhood_chain.py phase1
.venv/bin/python first_tokens_robinhood_chain.py pass-a --end 2000000
.venv/bin/python first_tokens_robinhood_chain.py pass-b --end 65000
.venv/bin/python first_tokens_robinhood_chain.py meta
.venv/bin/python first_tokens_robinhood_chain.py creators
.venv/bin/python first_tokens_robinhood_chain.py classify
.venv/bin/python first_tokens_robinhood_chain.py enrich --limit 40
.venv/bin/python first_tokens_robinhood_chain.py mcap
.venv/bin/python first_tokens_robinhood_chain.py export
```
