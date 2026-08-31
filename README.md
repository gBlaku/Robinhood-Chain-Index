# robinhood-chain-token-index

**Every ERC-20 on Robinhood Chain from genesis, indexed by the wallet that actually launched it — which block explorers get wrong.**

---

## The problem

Ask Blockscout who created CASHCAT, the $200M memecoin on Robinhood Chain:

```
GET /api/v2/addresses/0x020bfC650A365f8BB26819deAAbF3E21291018b4
→ creator_address_hash: "0xD9eC2db5f3D1b236843925949fe5bd8a3836FCcB"
```

That address is the **NOXA Fun launchpad contract**. Not a person.

It reports the same creator for all 3,276 tokens launched through it. Pons — the chain's dominant launchpad — reports itself as the creator of all **167,000+** of its tokens. Filter any EVM explorer by "contracts created by this wallet" and a prolific launcher returns an **empty list**.

This isn't a bug in Blockscout. The launchpad contract calls `CREATE2`, so at the protocol level the launchpad genuinely *is* the deployer. The human who clicked "launch" appears nowhere in the creation record.

**If you came from Solana, this is why "dev's previous deployments" — the single most-used due-diligence check on pump.fun/Photon/Solscan — silently doesn't work here.** On Solana the creator is a field in the bonding-curve account and indexers surface it for free. On EVM you have to reconstruct it.

## The fix

The human is the **transaction sender**, not the contract creator:

```
tx.from  ←  the human who clicked launch      (0xcdfc08a1…)
tx.to    ←  the launchpad they clicked it on  (0xd9ec2db5…)
creator  ←  always the launchpad. useless.
```

This repo indexes `first_tx_from` for every token on the chain. So the question that explorers can't answer becomes one line:

```sql
SELECT symbol, name, first_block, address
FROM token
WHERE first_tx_from = '0xcdfc08a1c1fbafb355645e5ddc32122e5716ca90'
ORDER BY first_block;
```

```
SHR       SHERWOOD                    71699   0xe8dfea4ce952be956fad851fc042c7bfb4129271
TH        Throbbin Hood               73889   0xf16a1a03e81f4d3e58d895ee18ec41e6b319f9df
AIMLESS   AIMLESS                     75571   0x8d05f9ca5c1c58b02ee74330b67d1b7e92f80612
LCH       Larry the Cucumber Hood     76761   0xc643afa888ad6e1025363e86de94d37373e7b22a
HB        Hood Bridge                 79013   0x15b82256c8ee8308c0f4391710cffabdadd663a5
CASHCAT   Cash Cat                    88836   0x020bfc650a365f8bb26819deaabf3e21291018b4   ← $200M
TEST      test                        88907   0x25267b0960822d8e99511673115322c07ec8e8ee
```

CASHCAT was attempt #6. The five before it are all dead — no price, no listing — and the same wallet launched another token four minutes after CASHCAT. None of that is visible on any explorer.

## Quick start

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python scan.py phase1          # verify chain, calibrate endpoint limits
.venv/bin/python scan.py pass-a --end 50500000   # the mint scan (the long pole)
.venv/bin/python scan.py meta            # name/symbol/decimals/supply
.venv/bin/python scan.py creators        # ← resolves first_tx_from. the important one.
.venv/bin/python scan.py classify
.venv/bin/python scan.py export
```

Or skip all that and read [`data/all_tokens.csv`](data/all_tokens.csv).

## What's in `data/`

| File | Contents |
|---|---|
| `all_tokens.csv` | Every ERC-20 in creation order: block, timestamp, symbol, name, address, **launcher**, launchpad, classification, and the evidence string behind each label |
| `independent.csv` | The earliest independent launches, enriched: holders, transfers, pool, FDV, obscurity rating |

## Findings

Full writeup in [`docs/REPORT.md`](docs/REPORT.md). The short version:

**The chain was live 62 days before its announced launch.** Block 1 is `2026-04-30`, not `2026-07-01`. Blocks ran at ~11s early on, not 100ms — Nitro only produces blocks on demand. `2026-07-01T00:00:00Z` is block **653,325**, found by binary search over timestamps.

**The first 27 tokens were all infrastructure.** WETH, Uniswap V2 pairs, ERC20Mocks, Paxos USDG, Ethena, Spark, Lido, Chainlink, and the first Robinhood stock token. Not one independent launch until #28.

**The first independent launch was `MARIAN` / "Lady Marian"** — block **58,539**, `2026-06-15T15:00:10Z`, `0x01637b14b7378b99de75a64d50656d98488d9a4d`. Direct EOA deploy, ownership renounced, 16 days before public launch.

**Being first was worth nothing.** MARIAN sits around $1M. CASHCAT (~109th) and PONS (a month later) are both >$200M. Launch order and outcome are uncorrelated on this chain.

**Two launchpads opened within an hour of each other** on 2026-06-16, and 31 of the first 36 independent tokens launched that single day.

## How classification works

Labels come from on-chain relationships, never from token names. All anchors live in [`config/known_addresses.json`](config/known_addresses.json) with the evidence for each, so you can audit or disprove any of them:

- **`owner()` matching** — NOXA Fun was identified because its `owner()` is the EOA that deployed a token literally called *"Testing NOXA Fun ;)"*. The `Bridged` bucket is tokens that share an `owner()` with the contract minting them.
- **`first_tx_to` clustering** — 3,276 tokens calling one contract is a launchpad, no guessing required.
- **Deployer population statistics** — only **16 distinct EOAs** deployed any token in the first 58,539 blocks (36 days); the next 38k blocks jump to 148. That's a closed partner-onboarding window, and it's what separates partner deployments from open launches.

## Endpoint limits (worth reading before you scan anything)

Calibrated empirically against the public RPC. These are the difference between a 100-minute scan and a 100-hour one:

| | |
|---|---|
| `eth_getLogs` block-range cap | **none** — it caps on *results*: 10,000 logs |
| Batch cap, `eth_call` | **25** sub-requests (~200/s) |
| Batch cap, `eth_getBlockByNumber` (full) | **10** sub-requests (~118 blocks/s) |
| Parallelism | **makes it worse** — 3 threads measured 68 blk/s vs 118 single-threaded |
| Default Python `User-Agent` | **HTTP 403**. Use a curl-like UA. |

The limiter is a per-IP token bucket priced by *method cost*, not request count — so an over-large batch gets 429'd no matter how long you wait. The fix is a smaller batch, not a longer sleep.

## Scope and limitations

Stated plainly, because the numbers are only as good as these.

- **Contract-creation reconciliation covers blocks 0–65,000, deliberately.** A second pass walks block bodies for top-level `to == null` creations, which catches tokens that deploy *without minting* — invisible to a mint scan. Chain-wide this would take ~117 hours at the measured 118 blk/s ceiling, so it is scoped to the pre-launch era where the "first token" questions live. It earned its place: it found a `USDG` at **block 56** with `totalSupply() == 0`, six days older than anything the mint scan can see.
- **Prices and market caps are Blockscout's, not derived here.** On-chain FDV is computed from Uniswap-V2-style pool reserves only. Tokens on V3/V4 pools or launchpad bonding curves are reported as unpriced rather than given a fabricated number.
- **V3/V4 "TVL" is the pool's raw token balance**, which *overstates* depth for concentrated liquidity.
- **One wallet = one identity is an assumption.** A dev using a fresh wallet per launch looks clean here.
- **Single chain.** No cross-chain clustering.
- **Market-cap figures are a snapshot** dated in the report, not a live feed.

This is a research index, not a trading tool. It tells you what happened on-chain and who did it. It does not tell you what anything is worth.

## License

MIT
