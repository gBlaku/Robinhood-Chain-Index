"""Tests for ERC-4337 launcher resolution.

The index's whole premise is that tx.from is the person and the contract
creator is not. Account abstraction breaks that premise: a bundler submits
other people's operations, so tx.from is the bundler. On this chain that put
three bundlers at the top of the launcher leaderboard with 2,997, 2,802 and
2,287 "launches" -- each routing 100% through EntryPoint, a shape no human
launcher has.

The real account is the `sender` of the UserOperationEvent EntryPoint emits per
operation. Because a bundler batches several operations into one transaction,
picking the right event is an ordering problem: EntryPoint emits the event
*after* the logs of the operation it describes, so a mint belongs to the first
UserOperationEvent that follows it.

Verified against tx 0x7a04f7b6a8ce9fe6b93936db19c2f3b600c89920cbec1c61a2360a0380c64f56,
where token 0xb3229c50e4f2d823d96dc1aa040f16b7cf591ab1 was credited to bundler
0xb1bcca2ac714a6eed7742866a2ec772394e9ea1a but was actually launched by smart
account 0x06b96f1c2ebe7090b67f44e1bd632eb6cece7524.

Run: python -m unittest discover tests
No network, no third-party dependencies.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scan import (  # noqa: E402
    ENTRYPOINTS,
    TRANSFER_TOPIC,
    USEROP_EVENT_TOPIC,
    userop_sender,
)

ALICE = "0x" + "a1" * 20
BOB = "0x" + "b0" * 20


def pad(addr):
    return "0x" + "00" * 12 + addr[2:]


def userop_event(log_index, sender):
    return {"logIndex": hex(log_index), "address": list(ENTRYPOINTS)[0],
            "topics": [USEROP_EVENT_TOPIC, "0x" + "cc" * 32, pad(sender),
                       "0x" + "00" * 32]}


def other_log(log_index):
    return {"logIndex": hex(log_index), "address": "0x" + "ee" * 20,
            "topics": [TRANSFER_TOPIC, "0x" + "00" * 32, pad(ALICE)],
            "data": "0x" + "00" * 32}


class TestUserOpSender(unittest.TestCase):
    def test_single_operation(self):
        logs = [other_log(0), userop_event(1, ALICE)]
        self.assertEqual(userop_sender(logs, 0), ALICE)

    def test_batched_operations_pick_the_enclosing_one(self):
        # Two operations in one bundle. The mint at index 3 belongs to Bob,
        # whose event closes at index 4 -- not to Alice, who closed at 1.
        logs = [other_log(0), userop_event(1, ALICE),
                other_log(3), userop_event(4, BOB)]
        self.assertEqual(userop_sender(logs, 3), BOB)
        self.assertEqual(userop_sender(logs, 0), ALICE)

    def test_unordered_logs(self):
        # Log order in a receipt is the endpoint's choice, not a guarantee.
        logs = [userop_event(4, BOB), userop_event(1, ALICE), other_log(3)]
        self.assertEqual(userop_sender(logs, 3), BOB)

    def test_mint_after_every_event_is_unattributable(self):
        # Nothing encloses the mint. Guessing the nearest event would invent a
        # launcher, so the honest answer is none.
        logs = [userop_event(1, ALICE), userop_event(2, BOB)]
        self.assertIsNone(userop_sender(logs, 9))

    def test_missing_log_index_single_op_is_safe(self):
        # Tokens found by the contract-creation pass have no mint log index.
        # With one operation in the batch there is no ambiguity.
        self.assertIsNone(userop_sender([], None))
        self.assertEqual(userop_sender([userop_event(1, ALICE)], None), ALICE)

    def test_missing_log_index_batched_is_refused(self):
        logs = [userop_event(1, ALICE), userop_event(4, BOB)]
        self.assertIsNone(userop_sender(logs, None))

    def test_ignores_non_userop_logs(self):
        self.assertIsNone(userop_sender([other_log(0), other_log(1)], 0))

    def test_tolerates_malformed_topics(self):
        # A log claiming to be a UserOperationEvent without the indexed sender
        # must not raise.
        bad = {"logIndex": "0x1", "topics": [USEROP_EVENT_TOPIC]}
        self.assertIsNone(userop_sender([bad], 0))

    def test_log_missing_index_is_skipped(self):
        # A well-formed event with no logIndex cannot be ordered against the
        # mint, so it must be ignored rather than raise mid-backfill.
        bad = {"topics": [USEROP_EVENT_TOPIC, "0x" + "cc" * 32, pad(ALICE),
                          "0x" + "00" * 32]}
        self.assertIsNone(userop_sender([bad], 0))
        self.assertEqual(userop_sender([bad, userop_event(2, BOB)], 0), BOB)

    def test_empty_input(self):
        self.assertIsNone(userop_sender([], 0))
        self.assertIsNone(userop_sender(None, 0))


class TestEntryPointConstants(unittest.TestCase):
    def test_addresses_are_lowercase_and_well_formed(self):
        # first_tx_to is stored lowercased, so a mixed-case constant here would
        # silently match nothing and quietly disable the whole correction.
        for a in ENTRYPOINTS:
            self.assertEqual(a, a.lower())
            self.assertEqual(len(a), 42)
            self.assertTrue(a.startswith("0x"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
