//! Dense sets over a bounded index, accumulated **by move**.
//!
//! Every set in this crate is a `Vec<bool>`, and every operation takes it by
//! value and hands it back. That is not a style: a `&mut` collection mutated
//! inside a nested loop is the one shape Aeneas cannot lower -- its loop fixed
//! point cannot unify the borrow context across two depths -- and moving the
//! vector through the loop instead removes the reborrow entirely. Measured: the
//! nested-loop accumulation below extracts with no hole and no external model.
//!
//! Set equality is then pointwise, so nothing here sorts or deduplicates, and no
//! clause needs a permutation argument on either side.

pub fn zeros(n: usize) -> Vec<bool> {
    let mut b: Vec<bool> = Vec::new();
    let mut i = 0;
    while i < n {
        b.push(false);
        i += 1;
    }
    b
}

pub fn get(b: &[bool], i: usize) -> bool {
    if i < b.len() { b[i] } else { false }
}

pub fn set(mut b: Vec<bool>, i: usize) -> Vec<bool> {
    if i < b.len() {
        b[i] = true;
    }
    b
}

pub fn union(mut a: Vec<bool>, b: &[bool]) -> Vec<bool> {
    let mut i = 0;
    while i < a.len() {
        if get(b, i) {
            a[i] = true;
        }
        i += 1;
    }
    a
}

pub fn of_ids(n: usize, ids: &[usize]) -> Vec<bool> {
    let mut b = zeros(n);
    let mut i = 0;
    while i < ids.len() {
        b = set(b, ids[i]);
        i += 1;
    }
    b
}

pub fn eq(a: &[bool], b: &[bool]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// `a ⊆ b`.
pub fn subset(a: &[bool], b: &[bool]) -> bool {
    let mut i = 0;
    while i < a.len() {
        if a[i] && !get(b, i) {
            return false;
        }
        i += 1;
    }
    true
}
