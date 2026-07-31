//! The solver's nouns, as arenas rather than as objects.
//!
//! Python has `Node`, `Dependency`, `Endpoint` and `Transform`, all hashed by a
//! string signature that is rebuilt and re-hashed constantly --
//! `Application.Signature()` alone is called 18.1 million times on the
//! metagenomics template. Here those identities are indices.
//!
//! **The distinction this file is built around is identity versus equality, and
//! Python needs both.** `Node.__eq__` compares signatures, so two endpoints with
//! the same properties and the same lineage are *equal*; but the plan
//! fingerprint keys endpoints by `id(e)`, so those same two endpoints are two
//! nodes in the graph it hashes. Collapsing them -- which is what interning
//! every endpoint would do -- gives one endpoint with two producers where the
//! solver had two endpoints with one each. That is a different plan, and it
//! would have been introduced by the port rather than found by it.
//!
//! So the split is explicit and in the type system, which is what the plan asked
//! for when it said `ApplicationId` and `ApplicationSig` must not be one string:
//!
//! - **`EpId`** is an endpoint's identity -- an arena index, `id(e)`.
//! - **`EpSig`** is an endpoint's structure -- interned, so equal structure
//!   shares one. Every `dict` and `set` Python keys by an endpoint is keyed by
//!   this, because that is what its `__hash__` and `__eq__` compare.
//!
//! Dependencies get no such split, and that is a claim rather than an oversight:
//! nothing in the solver distinguishes two structurally identical dependencies.
//! They are ranked, looked up and printed by structure alone, so `DepId` is
//! interned and identity never comes up.
//!
//! Transforms are the mirror image. `Transform` defines `__hash__` and *not*
//! `__eq__`, so `==` is `is` and two duplicate transforms are two transforms --
//! identity, always. Their structural key exists too, since `str(Transform)`
//! prints properties only, and the distance walk's cycle guard uses it.
//!
//! Finally, a type is a bitset. `IsA` -- "are y's properties a subset of x's" --
//! is the hottest primitive in the system, and interning property strings to
//! indices once turns it into a couple of ANDs over a fixed number of words.

use crate::det::{self, Map};

pub type PropId = u32;
pub type TypeId = u32;
pub type DepId = u32;
pub type EpId = u32;
pub type EpSig = u32;
pub type TransformId = u32;
pub type TransformSig = u32;

/// Interned property sets. `IsA` reads nothing else -- lineage is checked
/// separately, in Python and here.
pub struct Types {
    stride: usize,
    bits: Vec<u64>,
    intern: Map<Vec<u64>, TypeId>,
}

impl Types {
    pub fn new(n_properties: usize) -> Self {
        Self { stride: n_properties.div_ceil(64).max(1), bits: Vec::new(), intern: det::map() }
    }

    pub fn len(&self) -> usize { self.bits.len()/self.stride }

    pub fn intern(&mut self, props: &[PropId]) -> TypeId {
        let mut bits = vec![0u64; self.stride];
        for &p in props { bits[p as usize/64] |= 1u64 << (p as usize%64); }
        if let Some(&t) = self.intern.get(&bits) { return t; }
        let t = (self.bits.len()/self.stride) as TypeId;
        self.bits.extend_from_slice(&bits);
        self.intern.insert(bits, t);
        t
    }

    #[inline]
    pub fn words(&self, t: TypeId) -> &[u64] {
        let i = t as usize*self.stride;
        &self.bits[i..i + self.stride]
    }

    /// `x.IsA(y)`: y's properties are a subset of x's, so x can stand in for y.
    #[inline]
    pub fn is_a(&self, x: TypeId, y: TypeId) -> bool {
        let (a, b) = (x as usize*self.stride, y as usize*self.stride);
        for k in 0..self.stride {
            if self.bits[b + k] & !self.bits[a + k] != 0 { return false; }
        }
        true
    }

    /// The property indices of a type, ascending -- the ingredient of a
    /// transform's printed key, which is why it exists at all.
    pub fn props(&self, t: TypeId) -> Vec<PropId> {
        let mut out = Vec::new();
        for (w, &word) in self.words(t).iter().enumerate() {
            let mut bits = word;
            while bits != 0 {
                out.push((w*64) as u32 + bits.trailing_zeros());
                bits &= bits - 1;
            }
        }
        out
    }
}

/// Dependencies: interned, because nothing tells two structurally identical ones
/// apart. The index *is* the equality class, and `dep_rank` is first appearance
/// walking the caller's sequence.
#[derive(Default)]
pub struct Deps {
    pub types: Vec<TypeId>,
    /// Sorted and deduplicated, which is what makes interning work -- Python
    /// gets the same canonical form from `sorted(p.key for p in self.parents)`
    /// over a `set`.
    pub parents: Vec<Vec<DepId>>,
    intern: Map<(TypeId, Vec<DepId>), DepId>,
}

impl Deps {
    pub fn intern(&mut self, ty: TypeId, parents: &[DepId]) -> DepId {
        let mut ps = parents.to_vec();
        ps.sort_unstable();
        ps.dedup();
        if let Some(&d) = self.intern.get(&(ty, ps.clone())) { return d; }
        let d = self.types.len() as DepId;
        self.types.push(ty);
        self.parents.push(ps.clone());
        self.intern.insert((ty, ps), d);
        d
    }

    pub fn len(&self) -> usize { self.types.len() }
    #[inline]
    pub fn ty(&self, d: DepId) -> TypeId { self.types[d as usize] }
    #[inline]
    pub fn parents(&self, d: DepId) -> &[DepId] { &self.parents[d as usize] }
}

/// Endpoints: an arena, *not* interned. Each entry is one Python object.
///
/// `sig` is the equality class. It is stored rather than derived on demand
/// because `rectify` mutates parents and then calls `RefreshHash()`, so an
/// endpoint's structure genuinely changes during a solve and everything already
/// pointing at it sees the change. Recomputing is `resign`.
#[derive(Default)]
pub struct Endpoints {
    pub types: Vec<TypeId>,
    pub parents: Vec<Vec<EpId>>,
    sig: Vec<EpSig>,
    intern: Map<(TypeId, Vec<EpSig>), EpSig>,
}

impl Endpoints {
    pub fn len(&self) -> usize { self.types.len() }
    /// How many distinct endpoint structures have been interned. `EpSig` values
    /// are handed out densely from zero, so this is also their upper bound.
    pub fn n_sigs(&self) -> usize { self.intern.len() }
    #[inline]
    pub fn ty(&self, e: EpId) -> TypeId { self.types[e as usize] }
    #[inline]
    pub fn parents(&self, e: EpId) -> &[EpId] { &self.parents[e as usize] }
    #[inline]
    pub fn sig(&self, e: EpId) -> EpSig { self.sig[e as usize] }

    fn compute_sig(&mut self, ty: TypeId, parents: &[EpId]) -> EpSig {
        let mut ps: Vec<EpSig> = parents.iter().map(|&p| self.sig[p as usize]).collect();
        ps.sort_unstable();
        ps.dedup();
        if let Some(&s) = self.intern.get(&(ty, ps.clone())) { return s; }
        let s = self.intern.len() as EpSig;
        self.intern.insert((ty, ps), s);
        s
    }

    /// A new endpoint. Always a new identity, even when an equal one exists --
    /// that is the whole point of this arena.
    pub fn new_endpoint(&mut self, ty: TypeId, parents: &[EpId]) -> EpId {
        let sig = self.compute_sig(ty, parents);
        let e = self.types.len() as EpId;
        self.types.push(ty);
        let mut ps = parents.to_vec();
        ps.sort_unstable();
        ps.dedup();
        self.parents.push(ps);
        self.sig.push(sig);
        e
    }

    /// Add parents to an existing endpoint and recompute its signature --
    /// `new_e.parents |= ...; new_e.RefreshHash()`. Every reference to this
    /// endpoint now sees the wider lineage, which is exactly what Python's
    /// in-place mutation does and why endpoints cannot be interned values.
    pub fn extend_parents(&mut self, e: EpId, extra: &[EpId]) {
        let mut ps = std::mem::take(&mut self.parents[e as usize]);
        ps.extend_from_slice(extra);
        ps.sort_unstable();
        ps.dedup();
        let ty = self.types[e as usize];
        let sig = self.compute_sig(ty, &ps);
        self.parents[e as usize] = ps;
        self.sig[e as usize] = sig;
    }

    /// Is `ancestor` reachable from `e` through parents, comparing by equality?
    ///
    /// `_is_ancestor`, and the subtlety it was written to fix is worth keeping
    /// in view: the two endpoint shapes differ. Inputs carry a nested lineage
    /// tree, one direct level per endpoint; produced endpoints carry a one-hop
    /// flattened set per step. A membership test on the direct parents alone
    /// only works on the flattened shape, which is how a multi-level input
    /// lineage used to be dropped silently.
    pub fn is_ancestor(&self, ancestor: EpSig, e: EpId, seen: &mut det::Set<EpSig>) -> bool {
        if self.parents(e).iter().any(|&p| self.sig(p) == ancestor) { return true; }
        for i in 0..self.parents[e as usize].len() {
            let p = self.parents[e as usize][i];
            if !seen.insert(self.sig(p)) { continue; }
            if self.is_ancestor(ancestor, p, seen) { return true; }
        }
        false
    }
}

/// A transform, in the caller's declaration order. The index is the T5a rank.
pub struct Transform {
    pub requires: Vec<DepId>,
    pub produces: Vec<Vec<DepId>>,
    /// Structural identity, shared by duplicate transforms.
    ///
    /// Python's key comes from `str(self)`, which prints *properties only* --
    /// a dependency's lineage does not appear. Two transforms that differ only
    /// in a lineage constraint therefore share a key, and applications of them
    /// collide. That is reproduced rather than tidied up: it is one of the
    /// collisions the port is required to keep.
    pub sig: TransformSig,
}

impl Transform {
    pub fn is_free(&self) -> bool { self.requires.is_empty() }
}
