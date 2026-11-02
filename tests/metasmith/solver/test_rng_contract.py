import math
import numpy as np
import pytest

from metasmith.models.solver_rng import (
    SOLVER_RNG_VERSION, CHACHA_ROUNDS,
    chacha_block, seed_to_key, ChaCha8, DecisionStream,
    top_k_indices, argmax_index, argmin_index,
)

class TestChaChaCore:
    def test_rfc8439_block_vector(self):
        # RFC 8439 section 2.3.2. This is the only external anchor in the whole
        # contract: it proves the permutation is the reference one rather than
        # something that merely agrees with itself. The 8-round stream the
        # solver uses is the same permutation with fewer double-rounds.
        key = bytes(range(32))
        nonce = bytes.fromhex("000000090000004a00000000")
        assert chacha_block(key, 1, nonce, rounds=20) == [
            0xE4E7F110, 0x15593BD1, 0x1FDD0F50, 0xC47120A3,
            0xC7F4D1C7, 0x0368C033, 0x9AAA2204, 0x4E6CD4C3,
            0x466482D2, 0x09AA9F07, 0x05D7C214, 0xA2028BD9,
            0xD19C12B5, 0xB94E16DE, 0xE883D0CB, 0x4E3C50A2,
        ]

    def test_solver_uses_eight_rounds(self):
        assert CHACHA_ROUNDS == 8

    def test_seed_to_key_is_little_endian_then_zeros(self):
        assert seed_to_key(42) == bytes([42]) + bytes(31)
        assert seed_to_key(0x0102030405060708) == bytes([8, 7, 6, 5, 4, 3, 2, 1]) + bytes(24)

    def test_stream_is_pinned_and_matches_rand_chacha(self):
        # Verified against `rand_chacha` 0.10.0 via
        # `ChaCha8Rng::from_seed(seed_to_key(42))` and eight `next_u32` calls,
        # which produced these words exactly. Pinned here so the Rust port has
        # a target and so any drift in either implementation is loud.
        rng = ChaCha8(42)
        assert [rng.next_u32() for _ in range(8)] == [
            0x198FA887, 0x59273471, 0x169DF72B, 0x49238AA4,
            0x64FC90F6, 0x7E54361F, 0x96CEF6E2, 0x5E2E306A,
        ]

    def test_block_boundary_advances_the_counter(self):
        # Word 16 must come from block 1, not from a re-emitted block 0.
        first = [ChaCha8(1).next_u32() for _ in range(1)][0]
        rng = ChaCha8(1)
        words = [rng.next_u32() for _ in range(32)]
        assert words[:16] == chacha_block(seed_to_key(1), 0)
        assert words[16:] == chacha_block(seed_to_key(1), 1)
        assert words[0] == first

    def test_distinct_seeds_give_distinct_streams(self):
        a = [ChaCha8(42).next_u32() for _ in range(4)]
        b = [ChaCha8(43).next_u32() for _ in range(4)]
        assert a != b

class TestBoundedInt:
    def test_range(self):
        rng = DecisionStream(3)
        assert all(0 <= rng.bounded_int(7) < 7 for _ in range(2000))

    def test_degenerate_bounds_consume_no_words(self):
        # Part of the contract, not an optimisation: a stream that drifts by
        # one word against the other implementation diverges from there on.
        rng = DecisionStream(3)
        assert rng.bounded_int(1) == 0
        assert rng.bounded_int(0) == 0
        assert rng.bounded_int(-5) == 0
        assert rng.draws == 0

    def test_power_of_two_never_rejects(self):
        rng = DecisionStream(3)
        for _ in range(500): rng.bounded_int(256)
        assert rng.draws == 500

    def test_uniform_enough(self):
        rng = DecisionStream(11)
        n, trials = 7, 70000
        counts = [0]*n
        for _ in range(trials): counts[rng.bounded_int(n)] += 1
        expected = trials/n
        chi2 = sum((c-expected)**2/expected for c in counts)
        assert chi2 < 22.5, counts

    def test_deterministic_for_a_seed(self):
        a = DecisionStream(5); b = DecisionStream(5)
        assert [a.bounded_int(1000) for _ in range(50)] == [b.bounded_int(1000) for _ in range(50)]

class TestWeightedIndex:
    def test_respects_integer_weights(self):
        rng = DecisionStream(13)
        trials = 100000
        counts = {0: 0, 1: 0, 2: 0}
        for _ in range(trials): counts[rng.weighted_index([75, 20, 5])] += 1
        assert abs(counts[0]/trials - 0.75) < 0.01
        assert abs(counts[1]/trials - 0.20) < 0.01
        assert abs(counts[2]/trials - 0.05) < 0.01

    def test_zero_weight_is_never_chosen(self):
        rng = DecisionStream(17)
        assert all(rng.weighted_index([1, 0, 1]) != 1 for _ in range(2000))

    def test_single_weight_consumes_nothing(self):
        rng = DecisionStream(17)
        assert rng.weighted_index([9]) == 0
        assert rng.draws == 0

    def test_empty_weights_is_an_error(self):
        with pytest.raises(AssertionError):
            DecisionStream(1).weighted_index([])

class TestOrderingRules:
    def test_top_k_is_best_first_and_ties_go_to_the_lower_index(self):
        assert top_k_indices([1.0, 3.0, 3.0, 2.0], 3) == [1, 2, 3]

    def test_top_k_clamps_to_the_available(self):
        assert top_k_indices([1.0, 2.0], 5) == [1, 0]
        assert top_k_indices([], 3) == []
        assert top_k_indices([1.0], 0) == []

    def test_argmax_and_argmin_take_the_first_extremum(self):
        assert argmax_index([1.0, 5.0, 5.0, 2.0]) == 1
        assert argmin_index([3.0, 1.0, 1.0, 2.0]) == 1
        assert argmax_index([2.0]) == 0

    def test_nan_ranks_last_everywhere(self):
        nan = math.nan
        assert argmax_index([nan, 1.0]) == 1
        assert argmin_index([nan, 1.0]) == 1
        assert top_k_indices([nan, 1.0, 2.0], 3) == [2, 1, 0]

    def test_ordering_rules_are_not_numpy_argpartition(self):
        scores = np.array([5.0, 5.0, 5.0, 1.0])
        assert top_k_indices(list(scores), 1) == [0]
        assert argmax_index(list(scores)) == 0

class TestPickTopK:
    def test_picks_within_the_top_k(self):
        rng = DecisionStream(19)
        scores = [0.0, 9.0, 8.0, 9.0]
        picks = {rng.pick_top_k(scores, 2) for _ in range(200)}
        assert picks == {1, 3}

    def test_k_of_one_is_deterministic_and_free(self):
        rng = DecisionStream(19)
        scores = [0.0, 9.0, 8.0]
        assert all(rng.pick_top_k(scores, 1) == 1 for _ in range(100))
        assert rng.draws == 0

    def test_empty_frontier_is_an_error(self):
        with pytest.raises(AssertionError):
            DecisionStream(1).pick_top_k([], 1)

class TestSolverIntegration:
    def test_solve_does_not_touch_the_global_numpy_stream(self):
        # The point of killing `np.random.seed(seed)`: a solve used to reset
        # process-global state, so an unrelated numpy consumer silently shared
        # the solver's stream and two solves in one process were not
        # independent of each other's neighbours.
        from metasmith.testing.solver_verification import generate_problem, GeneratorDials

        np.random.seed(1234)
        before = np.random.rand(3).tolist()
        np.random.seed(1234)
        problem = generate_problem(7, GeneratorDials())
        problem.solve()
        after = np.random.rand(3).tolist()
        assert before == after

    def test_same_seed_solves_identically_within_one_process(self):
        from metasmith.testing.solver_verification import generate_problem, GeneratorDials, plan_fingerprint

        problem = generate_problem(11, GeneratorDials())
        a = problem.solve()
        b = problem.solve()
        assert plan_fingerprint(a.dependency_plan) == plan_fingerprint(b.dependency_plan)

    def test_version_is_declared(self):
        assert isinstance(SOLVER_RNG_VERSION, int) and SOLVER_RNG_VERSION >= 1
