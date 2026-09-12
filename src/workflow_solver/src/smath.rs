//! The Rust half of `solver_math.py`. See that file for why it exists.
//!
//! The short version: the score this feeds is compared, and comparisons break
//! ties by index, so a last-bit disagreement between the two implementations is
//! a different plan. numpy's pairwise `sum` and numpy's own `log2` were both
//! such a disagreement, so the Python side stopped using them and states the
//! summation order instead.
//!
//! `log2` is the remaining shared dependency, and it is a genuine one: this is
//! `f64::log2` against CPython's `math.log2`, which is glibc -- and these
//! binaries link musl. Both libms carry the ARM optimized-routines
//! implementation today, which is why they agree, and "today" is exactly why the
//! `log2` op in `wire` exists to check it across the boundary rather than trust
//! it.

/// Negative Shannon entropy in bits, of counts read as a distribution.
///
/// Summed left to right in the caller's order, because that is the order
/// `solver_math.entropy` states. The total is an integer sum, so it is exact
/// and needs no order at all.
pub fn entropy(counts: &[i64]) -> f64 {
    let total: i64 = counts.iter().sum();
    if total <= 0 { return 0.0; }
    let total = total as f64;
    let mut acc = 0.0f64;
    for &c in counts {
        let p = c as f64/total;
        if p <= 0.0 { continue; }
        acc += p*p.log2();
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_uniform_distribution_is_minus_log2_n() {
        assert_eq!(entropy(&[1, 1, 1, 1]), -2.0);
        assert_eq!(entropy(&[3, 3]), -1.0);
    }

    #[test]
    fn a_certain_outcome_carries_no_information() {
        assert_eq!(entropy(&[7]), 0.0);
        // and neither does nothing at all -- the python side returns 0.0 for an
        // empty `lineage_usage`, which is the common case on a plan with no
        // lineage constraints.
        assert_eq!(entropy(&[]), 0.0);
        assert_eq!(entropy(&[0, 0]), 0.0);
    }

    /// Not a rounding check -- an *order* check. Nine terms is where numpy's
    /// pairwise sum stops agreeing with a left-to-right one, so nine terms is
    /// where a port that reached for a "better" summation would start
    /// disagreeing with the contract.
    #[test]
    fn nine_terms_sum_left_to_right() {
        let counts: Vec<i64> = (1..=9).collect();
        let total: f64 = 45.0;
        let mut expect = 0.0f64;
        for c in 1..=9 {
            let p = c as f64/total;
            expect += p*p.log2();
        }
        assert_eq!(entropy(&counts), expect);
    }
}
