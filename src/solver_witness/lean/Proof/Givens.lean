/-
  `givens` -- the clause that says the presented givens are the declared ones,
  paired one to one, and that each pairing is a real correspondence.

  Read `Proof/Basis.lean` first for the loop recipe, `Proof/Bits.lean` for the
  four `bits` primitives, and `Proof/Slots.lean` for `SameSetLt` and the
  `Nodup`-shaped loop this file reuses. What follows is what is specific to this
  clause, and one finding that matters more than the proofs.

  ## The finding: this clause is not independent of `indexed` either, and for TWO
  reasons

  `Proof/Slots.lean` records that a bounded bit-set comparison cannot see an id
  at or beyond its width. `same_props` is `bits::eq` over two sets of `n_props`
  bits, so it has exactly that defect: a property id at or beyond `n_props` sets
  no bit on either side and is INVISIBLE to the comparison. What the checker
  decides is `SameSetLt (nProps p)`, not `SolverSpec.SameSet`.

  `Counterexample` below is that gap as a theorem, on a three-row problem:

      n_props   = 0
      nodes     = [ { props := [0], parents := [] } ]
      given     = [[0]]
      endpoints = [ { props := [], parents := [] } ]
      givens    = [(0, 0)]

  `cl_givens` returns `true` on it -- both bit sets are empty -- while
  `SameSet (epProps q 0) (nodeProps p 0)` is `SameSet [] [0]`, which is false.
  `cl_indexed` rejects the plan, on the conjunct `∀ d < nNodes p, ∀ x ∈
  nodeProps p d, x < nProps p`, so `check` is right about it and `check_spec` is
  unharmed; what is false is the UNCONDITIONAL clause lemma.

  There is a second, independent reason, and it has no counterexample here only
  because building one costs a `usize::MAX`-valued id. `given_node_of` and
  `given_ep_of` report "no such pairing" as `NONE = usize::MAX`, a value that
  lives in the same space as the ids they return. A given whose node id were
  literally `usize::MAX` would be reported as absent. `GivensR` therefore carries
  `nodeOf G f ≠ NONE` as a conjunct rather than assuming it away, and
  `givensR_iff_givens` is where a table no longer than `usize::MAX` retires it --
  the same move `Proof/Lineage.lean` makes for `access::bound_to`.

  So the clause is proved TWICE, as the two sibling modules do it:

  - `cl_givens_raw_spec` -- UNCONDITIONAL and exact, against `GivensR`, which is
    the conjunct with every property comparison confined to the property table
    and with the sentinel written in.
  - `cl_givens_spec` -- the conjunct of `SolverSpec.ValidC` verbatim, under
    `WellIndexed p q`, which is clause 0.

  ## What is NOT here, and deliberately

  There is no "one declared group" condition, in the Rust or in the conjunct.
  `Spec.lean`'s docstring on the `givens` field says why: the groups are one per
  sample, a multi-sample workflow spans all of them, and what keeps a step from
  mixing two samples is the lineage anchors in `conformance`. The two agree, and
  a proof that seemed to want such a condition would be the wrong proof.

  ## The one place the checker and the conjunct differ in SHAPE

  The conjunct asks for SOME pairing carrying the lineage edge --
  `∃ hn ∈ givens q, hn.1 = f ∧ hn.2 ∈ nodeParents p gn.2`. The checker resolves
  `f` to ONE pairing, the first, and demands the edge of that one. Those coincide
  only because the pairing is injective, which is the same clause's first two
  conjuncts. The Rust gets this right by ordering: the `Nodup` scan runs first
  and `ok` is already `false` by the time `givens_at` would be consulted. The
  proof pays for it in `givensR_iff_givens`, where `nodeOf_eq` needs the `Nodup`
  hypothesis to turn "the first match" into "the match".

  ## Recipe notes specific to this file

  **An extracted loop that takes a record's FIELDS cannot be stepped as it
  stands.** `same_props_loop0` carries the six fields of `Problem` and rebuilds
  the record to call an accessor, so the goal after `eq_def` mentions
  `access.node_prop { n_props := p.n_props, ... } n i`, which is eta-equal to
  `access.node_prop p n i` and not syntactically it. `step` will find a spec for
  the literal and hand back a hypothesis about `nodeProps { ... } n.val`, which
  no `rw` will connect to `nodeProps p n.val`. Rewrite the record away FIRST --
  `hp : { n_props := p.n_props, ... } = p := rfl` -- while it is still inside the
  program, where Basis's second warning permits a rewrite. `givens_at_loop0` and
  `givens_at_loop1` take the three fields of `Plan` and need the same move.

  **A search loop with a sentinel accumulator wants two lemmas, not one
  invariant.** `given_node_of_loop` stops as soon as `out ≠ NONE`, so the
  postcondition would have to be an `if` on the accumulator. It is cheaper to
  prove the stopped case separately (`given_node_of_stop`) and state the scan at
  `out = NONE` only; the body's own case split then reaches whichever it needs.
  Note the loop keeps scanning past a matching row whose value IS the sentinel,
  which is why `nodeOf` has a nested `if` rather than one.
-/

import SolverWitness.Types
import SolverWitness.Funs
import SolverWitness.Spec
import SolverWitness.Proof.Basis
import SolverWitness.Proof.Bits
import SolverWitness.Proof.Access
import SolverWitness.Proof.Slots

set_option maxHeartbeats 2000000
set_option maxRecDepth 8000
set_option synthInstance.maxSize 1000
set_option synthInstance.maxHeartbeats 4000000

namespace SolverProof

open Aeneas Aeneas.Std Result
open solver_witness

namespace Givens

/-! ## Bridges over the given list -/

theorem givens_length (q : types.Plan) :
    (SolverSpec.givens q).length = q.givens.val.length := by
  simp [SolverSpec.givens, SolverSpec.idPairs]

/-- The hoisted given list, in the form `Slots.pairs_getElem!` wants. -/
theorem givens_pairs (q : types.Plan) (G : List (Nat × Nat))
    (hG : SolverSpec.givens q = G) :
    q.givens.val.map (fun b => (b.1.val, b.2.val)) = G := hG

theorem givens_length_of (q : types.Plan) (G : List (Nat × Nat))
    (hG : SolverSpec.givens q = G) : G.length = q.givens.val.length := by
  rw [← hG]; exact givens_length q

/-! ## Injectivity on both sides, as one condition over the list -/

/-- What the first scan of `cl_givens` decides: the pairing is injective in each
component. Stated as two `Nodup`s rather than as a `Pairwise`, so that the
`Decidable` instance is the one `List.Nodup` already carries. -/
@[reducible] def NodupBoth (u : List (Nat × Nat)) : Prop :=
  (u.map Prod.fst).Nodup ∧ (u.map Prod.snd).Nodup

theorem not_mem_map_iff {α β} (f : α → β) (x : β) (t : List α) :
    x ∉ t.map f ↔ ∀ y ∈ t, x ≠ f y := by
  constructor
  · intro h y hy hc
    exact h (by rw [hc]; exact List.mem_map.mpr ⟨y, hy, rfl⟩)
  · intro h hc
    obtain ⟨y, hy, hyx⟩ := List.mem_map.mp hc
    exact h y hy hyx.symm

/-- The head against the tail, which is the shape the inner loop decides. -/
theorem nodupBoth_cons (b : Nat × Nat) (t : List (Nat × Nat)) :
    NodupBoth (b :: t) ↔ ((∀ y ∈ t, b.1 ≠ y.1 ∧ b.2 ≠ y.2) ∧ NodupBoth t) := by
  unfold NodupBoth
  rw [List.map_cons, List.map_cons, List.nodup_cons, List.nodup_cons,
      not_mem_map_iff, not_mem_map_iff]
  constructor
  · rintro ⟨⟨h1, h2⟩, ⟨h3, h4⟩⟩
    exact ⟨fun y hy => ⟨h1 y hy, h3 y hy⟩, h2, h4⟩
  · rintro ⟨h1, h2, h3⟩
    exact ⟨⟨fun y hy => (h1 y hy).1, h2⟩, fun y hy => (h1 y hy).2, h3⟩

/-! ## What the two resolvers compute

Both scan the pairing for a match and report `NONE` when there is none. Both keep
scanning past a matching row whose value is itself the sentinel, which is the
nested `if`. -/

def nodeOf (G : List (Nat × Nat)) (f : Nat) : Nat :=
  match G with
  | [] => access.NONE.val
  | b :: rest =>
      if b.1 = f then (if b.2 = access.NONE.val then nodeOf rest f else b.2)
      else nodeOf rest f

def epOf (G : List (Nat × Nat)) (a : Nat) : Nat :=
  match G with
  | [] => access.NONE.val
  | b :: rest =>
      if b.2 = a then (if b.1 = access.NONE.val then epOf rest a else b.1)
      else epOf rest a

theorem nodeOf_nil (f : Nat) : nodeOf [] f = access.NONE.val := rfl

theorem nodeOf_cons (b : Nat × Nat) (t : List (Nat × Nat)) (f : Nat) :
    nodeOf (b :: t) f
      = if b.1 = f then (if b.2 = access.NONE.val then nodeOf t f else b.2)
        else nodeOf t f := rfl

theorem epOf_nil (a : Nat) : epOf [] a = access.NONE.val := rfl

theorem epOf_cons (b : Nat × Nat) (t : List (Nat × Nat)) (a : Nat) :
    epOf (b :: t) a
      = if b.2 = a then (if b.1 = access.NONE.val then epOf t a else b.1)
        else epOf t a := rfl

/-- A resolved answer names a pairing that is really there. No injectivity
needed: this direction is what the scan literally found. -/
theorem nodeOf_mem : ∀ (G : List (Nat × Nat)) (f : Nat), nodeOf G f ≠ access.NONE.val →
    ∃ hn ∈ G, hn.1 = f ∧ hn.2 = nodeOf G f := by
  intro G
  induction G with
  | nil => intro f h; exact absurd rfl h
  | cons b t ih =>
    intro f h
    rw [nodeOf_cons] at h ⊢
    by_cases h1 : b.1 = f
    · rw [if_pos h1] at h ⊢
      by_cases h2 : b.2 = access.NONE.val
      · rw [if_pos h2] at h ⊢
        obtain ⟨hn, hm, he, hv⟩ := ih f h
        exact ⟨hn, List.mem_cons_of_mem _ hm, he, hv⟩
      · rw [if_neg h2] at h ⊢
        exact ⟨b, List.mem_cons_self, h1, rfl⟩
    · rw [if_neg h1] at h ⊢
      obtain ⟨hn, hm, he, hv⟩ := ih f h
      exact ⟨hn, List.mem_cons_of_mem _ hm, he, hv⟩

theorem epOf_mem : ∀ (G : List (Nat × Nat)) (a : Nat), epOf G a ≠ access.NONE.val →
    ∃ hn ∈ G, hn.2 = a ∧ hn.1 = epOf G a := by
  intro G
  induction G with
  | nil => intro a h; exact absurd rfl h
  | cons b t ih =>
    intro a h
    rw [epOf_cons] at h ⊢
    by_cases h1 : b.2 = a
    · rw [if_pos h1] at h ⊢
      by_cases h2 : b.1 = access.NONE.val
      · rw [if_pos h2] at h ⊢
        obtain ⟨hn, hm, he, hv⟩ := ih a h
        exact ⟨hn, List.mem_cons_of_mem _ hm, he, hv⟩
      · rw [if_neg h2] at h ⊢
        exact ⟨b, List.mem_cons_self, h1, rfl⟩
    · rw [if_neg h1] at h ⊢
      obtain ⟨hn, hm, he, hv⟩ := ih a h
      exact ⟨hn, List.mem_cons_of_mem _ hm, he, hv⟩

/-- The converse, and the ONLY place the clause's own injectivity is spent: the
first match is the match. -/
theorem nodeOf_eq : ∀ (G : List (Nat × Nat)) (hn : Nat × Nat), (G.map Prod.fst).Nodup →
    hn ∈ G → hn.2 ≠ access.NONE.val → nodeOf G hn.1 = hn.2 := by
  intro G
  induction G with
  | nil => intro hn _ hmem _; simp at hmem
  | cons b t ih =>
    intro hn hnd hmem hne
    rw [List.map_cons, List.nodup_cons, not_mem_map_iff] at hnd
    rcases List.mem_cons.mp hmem with h | h
    · subst h
      rw [nodeOf_cons, if_pos rfl, if_neg hne]
    · have hb1 : b.1 ≠ hn.1 := hnd.1 hn h
      rw [nodeOf_cons, if_neg hb1]
      exact ih hn hnd.2 h hne

theorem epOf_eq : ∀ (G : List (Nat × Nat)) (hn : Nat × Nat), (G.map Prod.snd).Nodup →
    hn ∈ G → hn.1 ≠ access.NONE.val → epOf G hn.2 = hn.1 := by
  intro G
  induction G with
  | nil => intro hn _ hmem _; simp at hmem
  | cons b t ih =>
    intro hn hnd hmem hne
    rw [List.map_cons, List.nodup_cons, not_mem_map_iff] at hnd
    rcases List.mem_cons.mp hmem with h | h
    · subst h
      rw [epOf_cons, if_pos rfl, if_neg hne]
    · have hb2 : b.2 ≠ hn.2 := hnd.1 hn h
      rw [epOf_cons, if_neg hb2]
      exact ih hn hnd.2 h hne

/-! ## The clause, as the checker decides it -/

/-- One lineage edge on the endpoint side, resolved. -/
@[reducible] def NodeParentOk (p : types.Problem) (G : List (Nat × Nat)) (n f : Nat) : Prop :=
  nodeOf G f ≠ access.NONE.val ∧ nodeOf G f ∈ SolverSpec.nodeParents p n

/-- One lineage edge on the node side, resolved. -/
@[reducible] def EpParentOk (q : types.Plan) (G : List (Nat × Nat)) (e a : Nat) : Prop :=
  epOf G a ≠ access.NONE.val ∧ epOf G a ∈ SolverSpec.epParents q e

/-- What `givens_at` decides for one pairing: property agreement WITHIN the
property table, and a resolved edge each way. -/
@[reducible] def PairOkR (p : types.Problem) (q : types.Plan) (G : List (Nat × Nat))
    (gn : Nat × Nat) : Prop :=
  Slots.SameSetLt (SolverSpec.nProps p)
      (SolverSpec.nodeProps p gn.2) (SolverSpec.epProps q gn.1) ∧
  (∀ f ∈ SolverSpec.epParents q gn.1, NodeParentOk p G gn.2 f) ∧
  (∀ a ∈ SolverSpec.nodeParents p gn.2, EpParentOk q G gn.1 a)

/-- What `cl_givens` decides, exactly and unconditionally. -/
@[reducible] def GivensR (p : types.Problem) (q : types.Plan) : Prop :=
  NodupBoth (SolverSpec.givens q) ∧
  ∀ gn ∈ SolverSpec.givens q,
    (∃ g ∈ SolverSpec.givenGroups p, gn.2 ∈ g) ∧ PairOkR p q (SolverSpec.givens q) gn

/-- The conjunct of `SolverSpec.ValidC`, named so the equivalence below can be
stated once. Reducible, so it IS that conjunct rather than a paraphrase. -/
@[reducible] def GivensSpec (p : types.Problem) (q : types.Plan) : Prop :=
  ((SolverSpec.givens q).map Prod.fst).Nodup ∧ ((SolverSpec.givens q).map Prod.snd).Nodup ∧
  (∀ gn ∈ SolverSpec.givens q, ∃ g ∈ SolverSpec.givenGroups p, gn.2 ∈ g) ∧
  (∀ gn ∈ SolverSpec.givens q,
      SolverSpec.SameSet (SolverSpec.epProps q gn.1) (SolverSpec.nodeProps p gn.2)) ∧
  (∀ gn ∈ SolverSpec.givens q, ∀ f ∈ SolverSpec.epParents q gn.1,
      ∃ hn ∈ SolverSpec.givens q, hn.1 = f ∧ hn.2 ∈ SolverSpec.nodeParents p gn.2) ∧
  (∀ gn ∈ SolverSpec.givens q, ∀ a ∈ SolverSpec.nodeParents p gn.2,
      ∃ hn ∈ SolverSpec.givens q, hn.2 = a ∧ hn.1 ∈ SolverSpec.epParents q gn.1)

/-- A table no longer than `usize::MAX` cannot contain the id reserved for
"nothing". -/
theorem nodes_bound (p : types.Problem) : SolverSpec.nNodes p ≤ access.NONE.val := by
  have h := alloc.vec.Vec.len_ineq p.nodes
  simp only [access.NONE, SolverSpec.nNodes]
  scalar_tac

theorem endpoints_bound (q : types.Plan) : SolverSpec.nEndpoints q ≤ access.NONE.val := by
  have h := alloc.vec.Vec.len_ineq q.endpoints
  simp only [access.NONE, SolverSpec.nEndpoints]
  scalar_tac

end Givens

open Givens
open Slots

/-! ## `same_props` -- property agreement, WITHIN THE PROPERTY TABLE

Two accumulating loops and one comparison. `bits::set` is a no-op out of range
and `bits::zeros` writes `n_props` bits, so a property id at or beyond `n_props`
sets nothing and cannot be seen. -/

theorem same_props_loop0_spec (p : types.Problem) (n : Std.Usize)
    (L : SolverSpec.Ids) (hL : SolverSpec.nodeProps p n.val = L)
    (na : Std.Usize) (hna : na.val = L.length) :
    ∀ (k : Nat) (a : alloc.vec.Vec Bool) (i : Std.Usize), L.length - i.val ≤ k →
      clauses.same_props_loop0 p.n_props p.nodes p.transforms p.given p.given_tr
        p.target_tr n a na i ⦃ r => r.val = setNats a.val (L.drop i.val) ⦄ := by
  have hp : ({ n_props := p.n_props, nodes := p.nodes, transforms := p.transforms,
               given := p.given, given_tr := p.given_tr, target_tr := p.target_tr }
             : types.Problem) = p := rfl
  intro k
  induction k with
  | zero =>
    intro a i hfuel
    rw [clauses.same_props_loop0.eq_def]
    rw [hp]
    split
    · exfalso
      have h1 : i.val < na.val := by scalar_tac
      omega
    · next hge =>
      have h1 : na.val ≤ i.val := by scalar_tac
      have hnil : L.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
      rw [hnil, setNats_nil]
      exact rfl
  | succ k ih =>
    intro a i hfuel
    rw [clauses.same_props_loop0.eq_def]
    rw [hp]
    split
    · next hlt =>
      have h1 : i.val < na.val := by scalar_tac
      have hiL : i.val < L.length := by omega
      have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
      step with node_prop_spec p n i as ⟨ x, hx ⟩
      have hxv : x.val = L[i.val]! := by
        rw [hx, hL, List.getElem?_eq_getElem hiL, Option.getD_some]
        exact (getElem!_pos L i.val hiL).symm
      step as ⟨ a1, ha1 ⟩
      step as ⟨ i5, hi5 ⟩
      have hi5' : i5.val = i.val + 1 := by scalar_tac
      have hcons : L.drop i.val = L[i.val]! :: L.drop (i.val + 1) :=
        drop_cons_getElem! L i.val hiL
      refine WP.spec_mono (ih a1 i5 (by omega)) ?_
      intro r hr
      refine hr.trans ?_
      rw [ha1, hxv, hi5', hcons, setNats_cons]
    · next hge =>
      have h1 : na.val ≤ i.val := by scalar_tac
      have hnil : L.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
      rw [hnil, setNats_nil]
      exact rfl

theorem same_props_loop1_spec (q : types.Plan) (e : Std.Usize)
    (M : SolverSpec.Ids) (hM : SolverSpec.epProps q e.val = M)
    (nb : Std.Usize) (hnb : nb.val = M.length) :
    ∀ (k : Nat) (b : alloc.vec.Vec Bool) (i : Std.Usize), M.length - i.val ≤ k →
      clauses.same_props_loop1 q e i b nb ⦃ r => r.val = setNats b.val (M.drop i.val) ⦄ := by
  intro k
  induction k with
  | zero =>
    intro b i hfuel
    rw [clauses.same_props_loop1.eq_def]
    split
    · exfalso
      have h1 : i.val < nb.val := by scalar_tac
      omega
    · next hge =>
      have h1 : nb.val ≤ i.val := by scalar_tac
      have hnil : M.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
      rw [hnil, setNats_nil]
      exact rfl
  | succ k ih =>
    intro b i hfuel
    rw [clauses.same_props_loop1.eq_def]
    split
    · next hlt =>
      have h1 : i.val < nb.val := by scalar_tac
      have hiM : i.val < M.length := by omega
      have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
      step with ep_prop_spec q e i as ⟨ x, hx ⟩
      have hxv : x.val = M[i.val]! := by
        rw [hx, hM, List.getElem?_eq_getElem hiM, Option.getD_some]
        exact (getElem!_pos M i.val hiM).symm
      step as ⟨ b1, hb1 ⟩
      step as ⟨ i2, hi2 ⟩
      have hi2' : i2.val = i.val + 1 := by scalar_tac
      have hcons : M.drop i.val = M[i.val]! :: M.drop (i.val + 1) :=
        drop_cons_getElem! M i.val hiM
      refine WP.spec_mono (ih b1 i2 (by omega)) ?_
      intro r hr
      refine hr.trans ?_
      rw [hb1, hxv, hi2', hcons, setNats_cons]
    · next hge =>
      have h1 : nb.val ≤ i.val := by scalar_tac
      have hnil : M.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
      rw [hnil, setNats_nil]
      exact rfl

/-- The loop from `0`, with the suffix already retired. `step` normalises the
hypothesis it introduces, and whether that leaves `L.drop ↑0#usize` standing
depends on what else is in the postcondition -- so the conversion is done here,
where the shape is known, rather than by a `rw` at the call site. -/
theorem same_props_loop0_zero (p : types.Problem) (n : Std.Usize)
    (L : SolverSpec.Ids) (hL : SolverSpec.nodeProps p n.val = L)
    (na : Std.Usize) (hna : na.val = L.length) (a : alloc.vec.Vec Bool) :
    clauses.same_props_loop0 p.n_props p.nodes p.transforms p.given p.given_tr
      p.target_tr n a na 0#usize ⦃ r => r.val = setNats a.val L ⦄ := by
  refine WP.spec_mono (same_props_loop0_spec p n L hL na hna L.length a 0#usize
    (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  simp

theorem same_props_loop1_zero (q : types.Plan) (e : Std.Usize)
    (M : SolverSpec.Ids) (hM : SolverSpec.epProps q e.val = M)
    (nb : Std.Usize) (hnb : nb.val = M.length) (b : alloc.vec.Vec Bool) :
    clauses.same_props_loop1 q e 0#usize b nb ⦃ r => r.val = setNats b.val M ⦄ := by
  refine WP.spec_mono (same_props_loop1_spec q e M hM nb hnb M.length b 0#usize
    (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  simp

theorem same_props_spec (p : types.Problem) (q : types.Plan) (n e : Std.Usize) :
    clauses.same_props p q n e ⦃ r =>
      r = decide (SameSetLt (SolverSpec.nProps p)
            (SolverSpec.nodeProps p n.val) (SolverSpec.epProps q e.val)) ⦄ := by
  rw [clauses.same_props.eq_def]
  step as ⟨ a, ha ⟩
  step as ⟨ na, hna ⟩
  step with same_props_loop0_zero p n (SolverSpec.nodeProps p n.val) rfl na hna a
    as ⟨ a1, ha1 ⟩
  step as ⟨ b, hb ⟩
  step as ⟨ nb, hnb ⟩
  step with same_props_loop1_zero q e (SolverSpec.epProps q e.val) rfl nb hnb b
    as ⟨ b1, hb1 ⟩
  have halen : a.val.length = SolverSpec.nProps p := by
    rw [ha]; simp [SolverSpec.nProps]
  have hblen : b.val.length = SolverSpec.nProps p := by
    rw [hb]; simp [SolverSpec.nProps]
  have ha1len : a1.val.length = SolverSpec.nProps p := by
    rw [ha1, setNats_length, halen]
  have hb1len : b1.val.length = SolverSpec.nProps p := by
    rw [hb1, setNats_length, hblen]
  have ha1mem : ∀ j < SolverSpec.nProps p,
      (a1.val[j]! = true ↔ j ∈ SolverSpec.nodeProps p n.val) := by
    intro j hj
    have hjb : j < a.val.length := by omega
    have hz : a.val[j]! = false := by
      rw [ha]; exact List.getElem!_replicate false hj
    rw [ha1, setNats_getElem! _ a.val j hjb, hz]
    simp
  have hb1mem : ∀ j < SolverSpec.nProps p,
      (b1.val[j]! = true ↔ j ∈ SolverSpec.epProps q e.val) := by
    intro j hj
    have hjb : j < b.val.length := by omega
    have hz : b.val[j]! = false := by
      rw [hb]; exact List.getElem!_replicate false hj
    rw [hb1, setNats_getElem! _ b.val j hjb, hz]
    simp
  have hkey : (a1.val = b1.val)
      ↔ SameSetLt (SolverSpec.nProps p)
          (SolverSpec.nodeProps p n.val) (SolverSpec.epProps q e.val) :=
    bits_eq_iff_sameSetLt ha1len hb1len ha1mem hb1mem
  refine WP.spec_mono (eq_spec (alloc.vec.Vec.deref a1) (alloc.vec.Vec.deref b1)) ?_
  intro r hr
  refine hr.trans ?_
  exact decide_eq_decide.mpr hkey

/-! ## `given_node_of` and `given_ep_of` -- the two resolvers

The loop stops as soon as its accumulator is not the sentinel, so the stopped
case is its own lemma and the scan is stated at `NONE` only. -/

theorem given_node_of_stop (q : types.Plan) (f out j : Std.Usize)
    (h : ¬ (out = access.NONE)) :
    clauses.given_node_of_loop q f out j ⦃ r => r = out ⦄ := by
  rw [clauses.given_node_of_loop.eq_def]
  dsimp only
  split
  · next hh => exact absurd hh h
  · exact rfl

theorem given_ep_of_stop (q : types.Plan) (a out j : Std.Usize)
    (h : ¬ (out = access.NONE)) :
    clauses.given_ep_of_loop q a out j ⦃ r => r = out ⦄ := by
  rw [clauses.given_ep_of_loop.eq_def]
  dsimp only
  split
  · next hh => exact absurd hh h
  · exact rfl

theorem given_node_of_loop_spec (q : types.Plan) (G : List (Nat × Nat))
    (hG : SolverSpec.givens q = G) (f : Std.Usize) :
    ∀ (k : Nat) (j : Std.Usize), G.length - j.val ≤ k →
      clauses.given_node_of_loop q f access.NONE j ⦃ r => r.val = nodeOf (G.drop j.val) f.val ⦄ := by
  have hlen : G.length = q.givens.val.length := givens_length_of q G hG
  intro k
  induction k with
  | zero =>
    intro j hfuel
    rw [clauses.given_node_of_loop.eq_def]
    dsimp only
    split
    · split
      · exfalso
        have h1 : j.val < q.givens.val.length := by scalar_tac
        omega
      · next hge =>
        have h1 : q.givens.val.length ≤ j.val := by scalar_tac
        have hnil : G.drop j.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        rw [hnil, nodeOf_nil]
        exact rfl
    · next hh => exact absurd rfl hh
  | succ k ih =>
    intro j hfuel
    rw [clauses.given_node_of_loop.eq_def]
    dsimp only
    split
    · split
      · next hlt =>
        have hjv : j.val < q.givens.val.length := by scalar_tac
        have hjG : j.val < G.length := by omega
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        obtain ⟨x, y, hxy⟩ : ∃ x y, (q.givens.val[j.val]'hjv) = (x, y) := ⟨_, _, rfl⟩
        have hGj : G[j.val]! = (x.val, y.val) := by
          rw [pairs_getElem! q.givens.val G (givens_pairs q G hG) j.val hjv, hxy]
        have hcons : G.drop j.val = (x.val, y.val) :: G.drop (j.val + 1) := by
          rw [drop_cons_getElem! G j.val hjG, hGj]
        rw [index_eq q.givens j hjv, hxy]
        show (do
            let out1 ← if x = f then ok y else ok access.NONE
            let j1 ← j + 1#usize
            clauses.given_node_of_loop q f out1 j1) ⦃ r =>
              r.val = nodeOf (G.drop j.val) f.val ⦄
        split
        · next heq =>
          have hxf : x.val = f.val := by rw [heq]
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          by_cases hy : y = access.NONE
          · rw [hy]
            refine WP.spec_mono (ih j1 (by omega)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hj1', hcons, nodeOf_cons, if_pos hxf,
                if_pos (show y.val = access.NONE.val by rw [hy])]
          · refine WP.spec_mono (given_node_of_stop q f y j1 hy) ?_
            intro r hr
            rw [hr, hcons, nodeOf_cons, if_pos hxf,
                if_neg (show ¬ (y.val = access.NONE.val) from
                  fun hc => hy (usize_eq_of_val hc))]
        · next hne =>
          have hxf : ¬ (x.val = f.val) := fun hc => hne (usize_eq_of_val hc)
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          refine WP.spec_mono (ih j1 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hj1', hcons, nodeOf_cons, if_neg hxf]
      · next hge =>
        have h1 : q.givens.val.length ≤ j.val := by scalar_tac
        have hnil : G.drop j.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        rw [hnil, nodeOf_nil]
        exact rfl
    · next hh => exact absurd rfl hh

theorem given_node_of_spec (q : types.Plan) (G : List (Nat × Nat))
    (hG : SolverSpec.givens q = G) (f : Std.Usize) :
    clauses.given_node_of q f ⦃ r => r.val = nodeOf G f.val ⦄ := by
  rw [clauses.given_node_of.eq_def]
  refine WP.spec_mono (given_node_of_loop_spec q G hG f G.length 0#usize (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  simp

theorem given_ep_of_loop_spec (q : types.Plan) (G : List (Nat × Nat))
    (hG : SolverSpec.givens q = G) (a : Std.Usize) :
    ∀ (k : Nat) (j : Std.Usize), G.length - j.val ≤ k →
      clauses.given_ep_of_loop q a access.NONE j ⦃ r => r.val = epOf (G.drop j.val) a.val ⦄ := by
  have hlen : G.length = q.givens.val.length := givens_length_of q G hG
  intro k
  induction k with
  | zero =>
    intro j hfuel
    rw [clauses.given_ep_of_loop.eq_def]
    dsimp only
    split
    · split
      · exfalso
        have h1 : j.val < q.givens.val.length := by scalar_tac
        omega
      · next hge =>
        have h1 : q.givens.val.length ≤ j.val := by scalar_tac
        have hnil : G.drop j.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        rw [hnil, epOf_nil]
        exact rfl
    · next hh => exact absurd rfl hh
  | succ k ih =>
    intro j hfuel
    rw [clauses.given_ep_of_loop.eq_def]
    dsimp only
    split
    · split
      · next hlt =>
        have hjv : j.val < q.givens.val.length := by scalar_tac
        have hjG : j.val < G.length := by omega
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        obtain ⟨x, y, hxy⟩ : ∃ x y, (q.givens.val[j.val]'hjv) = (x, y) := ⟨_, _, rfl⟩
        have hGj : G[j.val]! = (x.val, y.val) := by
          rw [pairs_getElem! q.givens.val G (givens_pairs q G hG) j.val hjv, hxy]
        have hcons : G.drop j.val = (x.val, y.val) :: G.drop (j.val + 1) := by
          rw [drop_cons_getElem! G j.val hjG, hGj]
        rw [index_eq q.givens j hjv, hxy]
        show (do
            let out1 ← if y = a then ok x else ok access.NONE
            let j1 ← j + 1#usize
            clauses.given_ep_of_loop q a out1 j1) ⦃ r =>
              r.val = epOf (G.drop j.val) a.val ⦄
        split
        · next heq =>
          have hya : y.val = a.val := by rw [heq]
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          by_cases hx : x = access.NONE
          · rw [hx]
            refine WP.spec_mono (ih j1 (by omega)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hj1', hcons, epOf_cons, if_pos hya,
                if_pos (show x.val = access.NONE.val by rw [hx])]
          · refine WP.spec_mono (given_ep_of_stop q a x j1 hx) ?_
            intro r hr
            rw [hr, hcons, epOf_cons, if_pos hya,
                if_neg (show ¬ (x.val = access.NONE.val) from
                  fun hc => hx (usize_eq_of_val hc))]
        · next hne =>
          have hya : ¬ (y.val = a.val) := fun hc => hne (usize_eq_of_val hc)
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          refine WP.spec_mono (ih j1 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hj1', hcons, epOf_cons, if_neg hya]
      · next hge =>
        have h1 : q.givens.val.length ≤ j.val := by scalar_tac
        have hnil : G.drop j.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        rw [hnil, epOf_nil]
        exact rfl
    · next hh => exact absurd rfl hh

theorem given_ep_of_spec (q : types.Plan) (G : List (Nat × Nat))
    (hG : SolverSpec.givens q = G) (a : Std.Usize) :
    clauses.given_ep_of q a ⦃ r => r.val = epOf G a.val ⦄ := by
  rw [clauses.given_ep_of.eq_def]
  refine WP.spec_mono (given_ep_of_loop_spec q G hG a G.length 0#usize (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  simp

/-! ## `node_has_parent` and `ep_has_parent` -- two `ok`-flag membership scans -/

theorem node_has_parent_loop_spec (p : types.Problem) (n a : Std.Usize)
    (L : SolverSpec.Ids) (hL : SolverSpec.nodeParents p n.val = L)
    (k : Std.Usize) (hk : k.val = L.length) :
    ∀ (m : Nat) (hit : Bool) (i : Std.Usize), L.length - i.val ≤ m →
      clauses.node_has_parent_loop p n a hit k i ⦃ r =>
        r = (hit || decide (a.val ∈ L.drop i.val)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro hit i hfuel
    rw [clauses.node_has_parent_loop.eq_def]
    split
    · next hh => exact ok_post (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      split
      · exfalso
        have h1 : i.val < k.val := by scalar_tac
        omega
      · next hge =>
        have h1 : k.val ≤ i.val := by scalar_tac
        have hnil : L.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hhf, hnil]; simp)
  | succ m ih =>
    intro hit i hfuel
    rw [clauses.node_has_parent_loop.eq_def]
    split
    · next hh => exact ok_post (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      split
      · next hlt =>
        have h1 : i.val < k.val := by scalar_tac
        have hiL : i.val < L.length := by omega
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        step with node_parent_spec p n i as ⟨ x, hx ⟩
        have hxv : x.val = L[i.val]! := by
          rw [hx, hL, List.getElem?_eq_getElem hiL, Option.getD_some]
          exact (getElem!_pos L i.val hiL).symm
        have hcons : L.drop i.val = L[i.val]! :: L.drop (i.val + 1) :=
          drop_cons_getElem! L i.val hiL
        split
        · next heq =>
          have hhead : L[i.val]! = a.val := by rw [← hxv, heq]
          have hyes : a.val ∈ L.drop i.val := by
            rw [hcons, hhead]; exact List.mem_cons_self
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          refine WP.spec_mono (ih true i2 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf]
          simp only [Bool.false_or, Bool.true_or]
          exact (decide_eq_true hyes).symm
        · next hne =>
          have hnev : a.val ≠ L[i.val]! := fun hc =>
            hne (usize_eq_of_val (by rw [hxv, ← hc]))
          have hiff : (a.val ∈ L.drop (i.val + 1)) ↔ (a.val ∈ L.drop i.val) := by
            rw [hcons]
            constructor
            · intro h; exact List.mem_cons_of_mem _ h
            · intro h
              rcases List.mem_cons.mp h with h' | h'
              · exact absurd h' hnev
              · exact h'
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          refine WP.spec_mono (ih false i2 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf, hi2']
          simp only [Bool.false_or]
          exact decide_eq_decide.mpr hiff
      · next hge =>
        have h1 : k.val ≤ i.val := by scalar_tac
        have hnil : L.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hhf, hnil]; simp)

theorem node_has_parent_spec (p : types.Problem) (n a : Std.Usize)
    (L : SolverSpec.Ids) (hL : SolverSpec.nodeParents p n.val = L) :
    clauses.node_has_parent p n a ⦃ r => r = decide (a.val ∈ L) ⦄ := by
  rw [clauses.node_has_parent.eq_def]
  step as ⟨ k, hk ⟩
  refine WP.spec_mono (node_has_parent_loop_spec p n a L hL k (by rw [hk, hL])
    L.length false 0#usize (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  simp

theorem ep_has_parent_loop_spec (q : types.Plan) (e f : Std.Usize)
    (L : SolverSpec.Ids) (hL : SolverSpec.epParents q e.val = L)
    (k : Std.Usize) (hk : k.val = L.length) :
    ∀ (m : Nat) (hit : Bool) (i : Std.Usize), L.length - i.val ≤ m →
      clauses.ep_has_parent_loop q e f hit k i ⦃ r =>
        r = (hit || decide (f.val ∈ L.drop i.val)) ⦄ := by
  intro m
  induction m with
  | zero =>
    intro hit i hfuel
    rw [clauses.ep_has_parent_loop.eq_def]
    split
    · next hh => exact ok_post (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      split
      · exfalso
        have h1 : i.val < k.val := by scalar_tac
        omega
      · next hge =>
        have h1 : k.val ≤ i.val := by scalar_tac
        have hnil : L.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hhf, hnil]; simp)
  | succ m ih =>
    intro hit i hfuel
    rw [clauses.ep_has_parent_loop.eq_def]
    split
    · next hh => exact ok_post (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      split
      · next hlt =>
        have h1 : i.val < k.val := by scalar_tac
        have hiL : i.val < L.length := by omega
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        step with ep_parent_spec q e i as ⟨ x, hx ⟩
        have hxv : x.val = L[i.val]! := by
          rw [hx, hL, List.getElem?_eq_getElem hiL, Option.getD_some]
          exact (getElem!_pos L i.val hiL).symm
        have hcons : L.drop i.val = L[i.val]! :: L.drop (i.val + 1) :=
          drop_cons_getElem! L i.val hiL
        split
        · next heq =>
          have hhead : L[i.val]! = f.val := by rw [← hxv, heq]
          have hyes : f.val ∈ L.drop i.val := by
            rw [hcons, hhead]; exact List.mem_cons_self
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          refine WP.spec_mono (ih true i2 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf]
          simp only [Bool.false_or, Bool.true_or]
          exact (decide_eq_true hyes).symm
        · next hne =>
          have hnev : f.val ≠ L[i.val]! := fun hc =>
            hne (usize_eq_of_val (by rw [hxv, ← hc]))
          have hiff : (f.val ∈ L.drop (i.val + 1)) ↔ (f.val ∈ L.drop i.val) := by
            rw [hcons]
            constructor
            · intro h; exact List.mem_cons_of_mem _ h
            · intro h
              rcases List.mem_cons.mp h with h' | h'
              · exact absurd h' hnev
              · exact h'
          simp only [bind_tc_ok]
          step as ⟨ i2, hi2 ⟩
          have hi2' : i2.val = i.val + 1 := by scalar_tac
          refine WP.spec_mono (ih false i2 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf, hi2']
          simp only [Bool.false_or]
          exact decide_eq_decide.mpr hiff
      · next hge =>
        have h1 : k.val ≤ i.val := by scalar_tac
        have hnil : L.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hhf, hnil]; simp)

theorem ep_has_parent_spec (q : types.Plan) (e f : Std.Usize)
    (L : SolverSpec.Ids) (hL : SolverSpec.epParents q e.val = L) :
    clauses.ep_has_parent q e f ⦃ r => r = decide (f.val ∈ L) ⦄ := by
  rw [clauses.ep_has_parent.eq_def]
  step as ⟨ k, hk ⟩
  refine WP.spec_mono (ep_has_parent_loop_spec q e f L hL k (by rw [hk, hL])
    L.length false 0#usize (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  simp

/-! ## `givens_at` -- one pairing, both directions of the lineage edge -/

theorem givens_at_loop0_spec (p : types.Problem) (q : types.Plan)
    (G : List (Nat × Nat)) (hG : SolverSpec.givens q = G) (e n : Std.Usize)
    (EP : SolverSpec.Ids) (hEP : SolverSpec.epParents q e.val = EP)
    (neps : Std.Usize) (hneps : neps.val = EP.length) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), EP.length - i.val ≤ m →
      clauses.givens_at_loop0 p q.endpoints q.givens q.steps ok1 e n neps i ⦃ r =>
        r = (ok1 && decide (∀ f ∈ EP.drop i.val, NodeParentOk p G n.val f)) ⦄ := by
  have hq : ({ endpoints := q.endpoints, givens := q.givens, steps := q.steps }
             : types.Plan) = q := rfl
  intro m
  induction m with
  | zero =>
    intro ok1 i hfuel
    rw [clauses.givens_at_loop0.eq_def]
    rw [hq]
    split
    · next hh =>
      split
      · exfalso
        have h1 : i.val < neps.val := by scalar_tac
        omega
      · next hge =>
        have h1 : neps.val ≤ i.val := by scalar_tac
        have hnil : EP.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hfuel
    rw [clauses.givens_at_loop0.eq_def]
    rw [hq]
    split
    · next hh =>
      split
      · next hlt =>
        have h1 : i.val < neps.val := by scalar_tac
        have hiEP : i.val < EP.length := by omega
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        step with ep_parent_spec q e i as ⟨ f, hf ⟩
        have hfv : f.val = EP[i.val]! := by
          rw [hf, hEP, List.getElem?_eq_getElem hiEP, Option.getD_some]
          exact (getElem!_pos EP i.val hiEP).symm
        have hcons : EP.drop i.val = EP[i.val]! :: EP.drop (i.val + 1) :=
          drop_cons_getElem! EP i.val hiEP
        step with given_node_of_spec q G hG f as ⟨ a, ha ⟩
        split
        · next hnone =>
          have hbad : ¬ NodeParentOk p G n.val f.val := by
            rintro ⟨h1', -⟩
            exact h1' (by rw [← ha, hnone])
          have hno : ¬ (∀ x ∈ EP.drop i.val, NodeParentOk p G n.val x) := by
            intro hc
            exact hbad (by
              have := hc EP[i.val]! (by rw [hcons]; exact List.mem_cons_self)
              rw [hfv]; exact this)
          simp only [bind_tc_ok]
          step as ⟨ i1, hi1 ⟩
          have hi1' : i1.val = i.val + 1 := by
            clear hbad hno ha hnone
            scalar_tac
          refine WP.spec_mono (ih false i1 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hh]
          simp only [Bool.false_and, Bool.true_and]
          exact (decide_eq_false hno).symm
        · next hnone =>
          have hane : nodeOf G f.val ≠ access.NONE.val := by
            rw [← ha]
            exact fun hc => hnone (usize_eq_of_val hc)
          rw [eq_of_spec (node_has_parent_spec p n a (SolverSpec.nodeParents p n.val) rfl)]
          simp only [bind_tc_ok]
          split
          · next hyes =>
            have hgood : NodeParentOk p G n.val f.val := by
              refine ⟨hane, ?_⟩
              rw [← ha]
              exact of_decide_eq_true hyes
            simp only [bind_tc_ok]
            step as ⟨ i1, hi1 ⟩
            have hi1' : i1.val = i.val + 1 := by scalar_tac
            have hiff : (∀ x ∈ EP.drop (i.val + 1), NodeParentOk p G n.val x)
                ↔ (∀ x ∈ EP.drop i.val, NodeParentOk p G n.val x) := by
              rw [hcons]
              constructor
              · intro hc x hx
                rcases List.mem_cons.mp hx with hx' | hx'
                · rw [hx', ← hfv]; exact hgood
                · exact hc x hx'
              · intro hc x hx
                exact hc x (List.mem_cons_of_mem _ hx)
            refine WP.spec_mono (ih true i1 (by omega)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hh, hi1']
            simp only [Bool.true_and]
            exact decide_eq_decide.mpr hiff
          · next hno' =>
            have hbad : ¬ NodeParentOk p G n.val f.val := by
              rintro ⟨-, h2'⟩
              refine hno' ?_
              rw [ha]
              exact decide_eq_true h2'
            have hno : ¬ (∀ x ∈ EP.drop i.val, NodeParentOk p G n.val x) := by
              intro hc
              exact hbad (by
                have := hc EP[i.val]! (by rw [hcons]; exact List.mem_cons_self)
                rw [hfv]; exact this)
            simp only [bind_tc_ok]
            step as ⟨ i1, hi1 ⟩
            have hi1' : i1.val = i.val + 1 := by scalar_tac
            refine WP.spec_mono (ih false i1 (by omega)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hh]
            simp only [Bool.false_and, Bool.true_and]
            exact (decide_eq_false hno).symm
      · next hge =>
        have h1 : neps.val ≤ i.val := by scalar_tac
        have hnil : EP.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)

theorem givens_at_loop1_spec (p : types.Problem) (q : types.Plan)
    (G : List (Nat × Nat)) (hG : SolverSpec.givens q = G) (e n : Std.Usize)
    (ND : SolverSpec.Ids) (hND : SolverSpec.nodeParents p n.val = ND)
    (nnds : Std.Usize) (hnnds : nnds.val = ND.length) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), ND.length - i.val ≤ m →
      clauses.givens_at_loop1 p q.endpoints q.givens q.steps ok1 e n i nnds ⦃ r =>
        r = (ok1 && decide (∀ a ∈ ND.drop i.val, EpParentOk q G e.val a)) ⦄ := by
  have hq : ({ endpoints := q.endpoints, givens := q.givens, steps := q.steps }
             : types.Plan) = q := rfl
  intro m
  induction m with
  | zero =>
    intro ok1 i hfuel
    rw [clauses.givens_at_loop1.eq_def]
    rw [hq]
    split
    · next hh =>
      split
      · exfalso
        have h1 : i.val < nnds.val := by scalar_tac
        omega
      · next hge =>
        have h1 : nnds.val ≤ i.val := by scalar_tac
        have hnil : ND.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hfuel
    rw [clauses.givens_at_loop1.eq_def]
    rw [hq]
    split
    · next hh =>
      split
      · next hlt =>
        have h1 : i.val < nnds.val := by scalar_tac
        have hiND : i.val < ND.length := by omega
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        step with node_parent_spec p n i as ⟨ a, ha ⟩
        have hav : a.val = ND[i.val]! := by
          rw [ha, hND, List.getElem?_eq_getElem hiND, Option.getD_some]
          exact (getElem!_pos ND i.val hiND).symm
        have hcons : ND.drop i.val = ND[i.val]! :: ND.drop (i.val + 1) :=
          drop_cons_getElem! ND i.val hiND
        step with given_ep_of_spec q G hG a as ⟨ f, hf ⟩
        split
        · next hnone =>
          have hbad : ¬ EpParentOk q G e.val a.val := by
            rintro ⟨h1', -⟩
            exact h1' (by rw [← hf, hnone])
          have hno : ¬ (∀ x ∈ ND.drop i.val, EpParentOk q G e.val x) := by
            intro hc
            exact hbad (by
              have := hc ND[i.val]! (by rw [hcons]; exact List.mem_cons_self)
              rw [hav]; exact this)
          simp only [bind_tc_ok]
          step as ⟨ i1, hi1 ⟩
          have hi1' : i1.val = i.val + 1 := by
            clear hbad hno hf hnone
            scalar_tac
          refine WP.spec_mono (ih false i1 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hh]
          simp only [Bool.false_and, Bool.true_and]
          exact (decide_eq_false hno).symm
        · next hnone =>
          have hfne : epOf G a.val ≠ access.NONE.val := by
            rw [← hf]
            exact fun hc => hnone (usize_eq_of_val hc)
          rw [eq_of_spec (ep_has_parent_spec q e f (SolverSpec.epParents q e.val) rfl)]
          simp only [bind_tc_ok]
          split
          · next hyes =>
            have hgood : EpParentOk q G e.val a.val := by
              refine ⟨hfne, ?_⟩
              rw [← hf]
              exact of_decide_eq_true hyes
            simp only [bind_tc_ok]
            step as ⟨ i1, hi1 ⟩
            have hi1' : i1.val = i.val + 1 := by scalar_tac
            have hiff : (∀ x ∈ ND.drop (i.val + 1), EpParentOk q G e.val x)
                ↔ (∀ x ∈ ND.drop i.val, EpParentOk q G e.val x) := by
              rw [hcons]
              constructor
              · intro hc x hx
                rcases List.mem_cons.mp hx with hx' | hx'
                · rw [hx', ← hav]; exact hgood
                · exact hc x hx'
              · intro hc x hx
                exact hc x (List.mem_cons_of_mem _ hx)
            refine WP.spec_mono (ih true i1 (by omega)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hh, hi1']
            simp only [Bool.true_and]
            exact decide_eq_decide.mpr hiff
          · next hno' =>
            have hbad : ¬ EpParentOk q G e.val a.val := by
              rintro ⟨-, h2'⟩
              refine hno' ?_
              rw [hf]
              exact decide_eq_true h2'
            have hno : ¬ (∀ x ∈ ND.drop i.val, EpParentOk q G e.val x) := by
              intro hc
              exact hbad (by
                have := hc ND[i.val]! (by rw [hcons]; exact List.mem_cons_self)
                rw [hav]; exact this)
            simp only [bind_tc_ok]
            step as ⟨ i1, hi1 ⟩
            have hi1' : i1.val = i.val + 1 := by scalar_tac
            refine WP.spec_mono (ih false i1 (by omega)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hh]
            simp only [Bool.false_and, Bool.true_and]
            exact (decide_eq_false hno).symm
      · next hge =>
        have h1 : nnds.val ≤ i.val := by scalar_tac
        have hnil : ND.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)

theorem givens_at_spec (p : types.Problem) (q : types.Plan) (gi : Std.Usize)
    (hgi : gi.val < q.givens.val.length)
    (G : List (Nat × Nat)) (hG : SolverSpec.givens q = G) :
    clauses.givens_at p q gi ⦃ r => r = decide (PairOkR p q G (G[gi.val]!)) ⦄ := by
  have hzero : (0#usize : Std.Usize).val = 0 := by simp
  rw [clauses.givens_at.eq_def]
  dsimp only
  split
  · obtain ⟨e, n, hen⟩ : ∃ e n, (q.givens.val[gi.val]'hgi) = (e, n) := ⟨_, _, rfl⟩
    have hGgi : G[gi.val]! = (e.val, n.val) := by
      rw [pairs_getElem! q.givens.val G (givens_pairs q G hG) gi.val hgi, hen]
    rw [index_eq q.givens gi hgi, hen]
    show (do
        let ok1 ← clauses.same_props p q n e
        let neps ← access.ep_nparents q e
        let ok2 ← clauses.givens_at_loop0 p q.endpoints q.givens q.steps ok1 e n neps 0#usize
        let nnds ← access.node_nparents p n
        clauses.givens_at_loop1 p q.endpoints q.givens q.steps ok2 e n 0#usize nnds) ⦃ r =>
          r = decide (PairOkR p q G (G[gi.val]!)) ⦄
    step with same_props_spec p q n e as ⟨ ok1, hok1 ⟩
    step as ⟨ neps, hneps ⟩
    step with givens_at_loop0_spec p q G hG e n (SolverSpec.epParents q e.val) rfl
      neps hneps (SolverSpec.epParents q e.val).length ok1 0#usize (by scalar_tac)
      as ⟨ ok2, hok2 ⟩
    step as ⟨ nnds, hnnds ⟩
    refine WP.spec_mono (givens_at_loop1_spec p q G hG e n (SolverSpec.nodeParents p n.val) rfl
      nnds hnnds (SolverSpec.nodeParents p n.val).length ok2 0#usize (by scalar_tac)) ?_
    intro r hr
    refine hr.trans ?_
    rw [hok2, hok1, hzero, List.drop_zero, List.drop_zero, hGgi]
    refine Bool.eq_iff_iff.mpr ?_
    simp only [Bool.and_eq_true, decide_eq_true_eq]
    exact ⟨fun h => ⟨h.1.1, h.1.2, h.2⟩, fun h => ⟨⟨h.1, h.2.1⟩, h.2.2⟩⟩
  · exfalso; scalar_tac

/-! ## `is_declared_given` -- two nested membership scans over the declared groups -/

theorem is_declared_given_inner_spec (p : types.Problem) (n : Std.Usize) (g : Std.Usize)
    (hg : g.val < p.given.val.length)
    (gg : SolverSpec.Ids) (hgg : SolverSpec.nats (p.given.val[g.val]'hg) = gg) :
    ∀ (m : Nat) (hit : Bool) (mi : Std.Usize), gg.length - mi.val ≤ m →
      clauses.is_declared_given_loop0_loop0 p.given n hit g mi ⦃ r =>
        r = (hit || decide (n.val ∈ gg.drop mi.val)) ⦄ := by
  have hgl : gg.length = (p.given.val[g.val]'hg).val.length := by
    rw [← hgg]; simp [SolverSpec.nats]
  have hids : (p.given.val[g.val]'hg).val.map (fun x => x.val) = gg := hgg
  intro m
  induction m with
  | zero =>
    intro hit mi hfuel
    rw [clauses.is_declared_given_loop0_loop0.eq_def]
    split
    · next hh => exact ok_post (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      rw [index_eq p.given g hg]
      simp only [bind_tc_ok]
      split
      · exfalso
        have h1 : mi.val < (p.given.val[g.val]'hg).val.length := by scalar_tac
        omega
      · next hge =>
        have h1 : (p.given.val[g.val]'hg).val.length ≤ mi.val := by scalar_tac
        have hnil : gg.drop mi.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hhf, hnil]; simp)
  | succ m ih =>
    intro hit mi hfuel
    rw [clauses.is_declared_given_loop0_loop0.eq_def]
    split
    · next hh => exact ok_post (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      rw [index_eq p.given g hg]
      simp only [bind_tc_ok]
      split
      · next hlt =>
        have hmv : mi.val < (p.given.val[g.val]'hg).val.length := by scalar_tac
        have hmg : mi.val < gg.length := by omega
        have hbnd : mi.val + 1 ≤ Usize.max := by scalar_tac
        rw [index_eq _ mi hmv]
        simp only [bind_tc_ok]
        have hxv : gg[mi.val]! = ((p.given.val[g.val]'hg).val[mi.val]'hmv).val :=
          ids_getElem! _ gg hids mi.val hmv
        have hcons : gg.drop mi.val = gg[mi.val]! :: gg.drop (mi.val + 1) :=
          drop_cons_getElem! gg mi.val hmg
        split
        · next heq =>
          have hyes : n.val ∈ gg.drop mi.val := by
            rw [hcons, hxv, ← heq]
            exact List.mem_cons_self
          simp only [bind_tc_ok]
          step as ⟨ m1, hm1 ⟩
          have hm1' : m1.val = mi.val + 1 := by scalar_tac
          refine WP.spec_mono (ih true m1 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf]
          simp only [Bool.false_or, Bool.true_or]
          exact (decide_eq_true hyes).symm
        · next hne =>
          have hnev : n.val ≠ gg[mi.val]! := fun hc =>
            hne (usize_eq_of_val (by rw [← hxv, ← hc]))
          have hiff : (n.val ∈ gg.drop (mi.val + 1)) ↔ (n.val ∈ gg.drop mi.val) := by
            rw [hcons]
            constructor
            · intro h; exact List.mem_cons_of_mem _ h
            · intro h
              rcases List.mem_cons.mp h with h' | h'
              · exact absurd h' hnev
              · exact h'
          simp only [bind_tc_ok]
          step as ⟨ m1, hm1 ⟩
          have hm1' : m1.val = mi.val + 1 := by scalar_tac
          refine WP.spec_mono (ih false m1 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hhf, hm1']
          simp only [Bool.false_or]
          exact decide_eq_decide.mpr hiff
      · next hge =>
        have h1 : (p.given.val[g.val]'hg).val.length ≤ mi.val := by scalar_tac
        have hnil : gg.drop mi.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hhf, hnil]; simp)

theorem is_declared_given_outer_spec (p : types.Problem) (n : Std.Usize) :
    ∀ (m : Nat) (hit : Bool) (g : Std.Usize), p.given.val.length - g.val ≤ m →
      clauses.is_declared_given_loop0 p n hit g ⦃ r =>
        r = (hit || decide (∃ gg ∈ (SolverSpec.givenGroups p).drop g.val, n.val ∈ gg)) ⦄ := by
  have hlen : (SolverSpec.givenGroups p).length = p.given.val.length := by
    simp [SolverSpec.givenGroups, SolverSpec.idLists]
  intro m
  induction m with
  | zero =>
    intro hit g hfuel
    rw [clauses.is_declared_given_loop0.eq_def]
    dsimp only
    split
    · next hh => exact ok_post (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      split
      · exfalso
        have h1 : g.val < p.given.val.length := by scalar_tac
        omega
      · next hge =>
        have h1 : p.given.val.length ≤ g.val := by scalar_tac
        have hnil : (SolverSpec.givenGroups p).drop g.val = [] :=
          List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hhf, hnil]; simp)
  | succ m ih =>
    intro hit g hfuel
    rw [clauses.is_declared_given_loop0.eq_def]
    dsimp only
    split
    · next hh => exact ok_post (by rw [hh]; simp)
    · next hh =>
      have hhf : hit = false := by simpa using hh
      split
      · next hlt =>
        have hgv : g.val < p.given.val.length := by scalar_tac
        have hgl : g.val < (SolverSpec.givenGroups p).length := by omega
        have hbnd : g.val + 1 ≤ Usize.max := by scalar_tac
        have hhead : (SolverSpec.givenGroups p)[g.val]!
            = SolverSpec.nats (p.given.val[g.val]'hgv) := by
          rw [getElem!_pos _ g.val hgl]
          simp [SolverSpec.givenGroups, SolverSpec.idLists]
        have hcons : (SolverSpec.givenGroups p).drop g.val
            = SolverSpec.nats (p.given.val[g.val]'hgv)
              :: (SolverSpec.givenGroups p).drop (g.val + 1) := by
          rw [drop_cons_getElem! _ g.val hgl, hhead]
        have hin : clauses.is_declared_given_loop0_loop0 p.given n false g 0#usize
            = ok (decide (n.val ∈ SolverSpec.nats (p.given.val[g.val]'hgv))) := by
          rw [eq_of_spec (is_declared_given_inner_spec p n g hgv
            (SolverSpec.nats (p.given.val[g.val]'hgv)) rfl
            (SolverSpec.nats (p.given.val[g.val]'hgv)).length false 0#usize (by scalar_tac))]
          refine congrArg ok ?_
          simp
        rw [hin]
        simp only [bind_tc_ok]
        step as ⟨ g1, hg1 ⟩
        have hg1' : g1.val = g.val + 1 := by scalar_tac
        have hiff : ((n.val ∈ SolverSpec.nats (p.given.val[g.val]'hgv))
            ∨ (∃ gg ∈ (SolverSpec.givenGroups p).drop (g.val + 1), n.val ∈ gg))
            ↔ (∃ gg ∈ (SolverSpec.givenGroups p).drop g.val, n.val ∈ gg) := by
          rw [hcons]
          constructor
          · rintro (h | ⟨gg, hgg, hn⟩)
            · exact ⟨_, List.mem_cons_self, h⟩
            · exact ⟨gg, List.mem_cons_of_mem _ hgg, hn⟩
          · rintro ⟨gg, hgg, hn⟩
            rcases List.mem_cons.mp hgg with h' | h'
            · exact Or.inl (by rw [← h']; exact hn)
            · exact Or.inr ⟨gg, h', hn⟩
        refine WP.spec_mono (ih _ g1 (by omega)) ?_
        intro r hr
        refine hr.trans ?_
        rw [hhf, hg1']
        simp only [Bool.false_or]
        refine Bool.eq_iff_iff.mpr ?_
        simp only [Bool.or_eq_true, decide_eq_true_eq]
        exact hiff
      · next hge =>
        have h1 : p.given.val.length ≤ g.val := by scalar_tac
        have hnil : (SolverSpec.givenGroups p).drop g.val = [] :=
          List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hhf, hnil]; simp)

theorem is_declared_given_spec (p : types.Problem) (n : Std.Usize) :
    clauses.is_declared_given p n ⦃ r =>
      r = decide (∃ g ∈ SolverSpec.givenGroups p, n.val ∈ g) ⦄ := by
  rw [clauses.is_declared_given.eq_def]
  refine WP.spec_mono (is_declared_given_outer_spec p n p.given.val.length false 0#usize
    (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  simp

/-! ## `cl_givens` -- the injectivity scan, then the per-pairing scan -/

theorem cl_givens_nodup_inner_spec (q : types.Plan) (G : List (Nat × Nat))
    (hG : SolverSpec.givens q = G) (i : Std.Usize) (hi : i.val < q.givens.val.length) :
    ∀ (m : Nat) (ok1 : Bool) (j : Std.Usize), G.length - j.val ≤ m →
      clauses.cl_givens_loop0_loop0 q.givens ok1 i j ⦃ r =>
        r = (ok1 && decide (∀ y ∈ G.drop j.val,
              G[i.val]!.1 ≠ y.1 ∧ G[i.val]!.2 ≠ y.2)) ⦄ := by
  have hlen : G.length = q.givens.val.length := givens_length_of q G hG
  have hGi : G[i.val]! = ((q.givens.val[i.val]'hi).1.val, (q.givens.val[i.val]'hi).2.val) :=
    pairs_getElem! q.givens.val G (givens_pairs q G hG) i.val hi
  intro m
  induction m with
  | zero =>
    intro ok1 j hfuel
    rw [clauses.cl_givens_loop0_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · exfalso
        have h1 : j.val < q.givens.val.length := by scalar_tac
        omega
      · next hge =>
        have h1 : q.givens.val.length ≤ j.val := by scalar_tac
        have hnil : G.drop j.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 j hfuel
    rw [clauses.cl_givens_loop0_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · next hlt =>
        have hjv : j.val < q.givens.val.length := by scalar_tac
        have hjG : j.val < G.length := by omega
        have hbnd : j.val + 1 ≤ Usize.max := by scalar_tac
        have hGj : G[j.val]! = ((q.givens.val[j.val]'hjv).1.val, (q.givens.val[j.val]'hjv).2.val) :=
          pairs_getElem! q.givens.val G (givens_pairs q G hG) j.val hjv
        have hcons : G.drop j.val = G[j.val]! :: G.drop (j.val + 1) :=
          drop_cons_getElem! G j.val hjG
        obtain ⟨x1, x2, hx⟩ : ∃ a b, (q.givens.val[i.val]'hi) = (a, b) := ⟨_, _, rfl⟩
        obtain ⟨y1, y2, hy⟩ : ∃ a b, (q.givens.val[j.val]'hjv) = (a, b) := ⟨_, _, rfl⟩
        rw [index_eq q.givens i hi, hx]
        simp only [bind_tc_ok]
        rw [index_eq q.givens j hjv, hy]
        show (do
            let ok2 ← if x1 = y1 then ok false else if x2 = y2 then ok false else ok true
            let j1 ← j + 1#usize
            clauses.cl_givens_loop0_loop0 q.givens ok2 i j1) ⦃ r =>
              r = (ok1 && decide (∀ y ∈ G.drop j.val,
                    G[i.val]!.1 ≠ y.1 ∧ G[i.val]!.2 ≠ y.2)) ⦄
        rw [hx] at hGi
        rw [hy] at hGj
        split
        · next heq =>
          have hbad : ¬ (∀ y ∈ G.drop j.val, G[i.val]!.1 ≠ y.1 ∧ G[i.val]!.2 ≠ y.2) := by
            intro hc
            have := (hc G[j.val]! (by rw [hcons]; exact List.mem_cons_self)).1
            rw [hGi, hGj] at this
            exact this (by rw [heq])
          simp only [bind_tc_ok]
          step as ⟨ j1, hj1 ⟩
          have hj1' : j1.val = j.val + 1 := by scalar_tac
          refine WP.spec_mono (ih false j1 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hh]
          simp only [Bool.false_and, Bool.true_and]
          exact (decide_eq_false hbad).symm
        · next hne1 =>
          split
          · next heq2 =>
            have hbad : ¬ (∀ y ∈ G.drop j.val, G[i.val]!.1 ≠ y.1 ∧ G[i.val]!.2 ≠ y.2) := by
              intro hc
              have := (hc G[j.val]! (by rw [hcons]; exact List.mem_cons_self)).2
              rw [hGi, hGj] at this
              exact this (by rw [heq2])
            simp only [bind_tc_ok]
            step as ⟨ j1, hj1 ⟩
            have hj1' : j1.val = j.val + 1 := by scalar_tac
            refine WP.spec_mono (ih false j1 (by omega)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hh]
            simp only [Bool.false_and, Bool.true_and]
            exact (decide_eq_false hbad).symm
          · next hne2 =>
            have hhead : G[i.val]!.1 ≠ G[j.val]!.1 ∧ G[i.val]!.2 ≠ G[j.val]!.2 := by
              rw [hGi, hGj]
              exact ⟨fun hc => hne1 (usize_eq_of_val hc),
                     fun hc => hne2 (usize_eq_of_val hc)⟩
            have hiff : (∀ y ∈ G.drop (j.val + 1), G[i.val]!.1 ≠ y.1 ∧ G[i.val]!.2 ≠ y.2)
                ↔ (∀ y ∈ G.drop j.val, G[i.val]!.1 ≠ y.1 ∧ G[i.val]!.2 ≠ y.2) := by
              rw [hcons]
              constructor
              · intro hc y hy
                rcases List.mem_cons.mp hy with hy' | hy'
                · rw [hy']; exact hhead
                · exact hc y hy'
              · intro hc y hy
                exact hc y (List.mem_cons_of_mem _ hy)
            simp only [bind_tc_ok]
            step as ⟨ j1, hj1 ⟩
            have hj1' : j1.val = j.val + 1 := by scalar_tac
            refine WP.spec_mono (ih true j1 (by omega)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hh, hj1']
            simp only [Bool.true_and]
            exact decide_eq_decide.mpr hiff
      · next hge =>
        have h1 : q.givens.val.length ≤ j.val := by scalar_tac
        have hnil : G.drop j.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)

theorem cl_givens_nodup_spec (q : types.Plan) (G : List (Nat × Nat))
    (hG : SolverSpec.givens q = G) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), G.length - i.val ≤ m →
      clauses.cl_givens_loop0 q ok1 i ⦃ r =>
        r = (q, ok1 && decide (NodupBoth (G.drop i.val))) ⦄ := by
  have hlen : G.length = q.givens.val.length := givens_length_of q G hG
  intro m
  induction m with
  | zero =>
    intro ok1 i hfuel
    rw [clauses.cl_givens_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · exfalso
        have h1 : i.val < q.givens.val.length := by scalar_tac
        omega
      · next hge =>
        have h1 : q.givens.val.length ≤ i.val := by scalar_tac
        have hnil : G.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hh, hnil]; simp [NodupBoth])
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hfuel
    rw [clauses.cl_givens_loop0.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · next hlt =>
        have hiv : i.val < q.givens.val.length := by scalar_tac
        have hiG : i.val < G.length := by omega
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        have hcons : G.drop i.val = G[i.val]! :: G.drop (i.val + 1) :=
          drop_cons_getElem! G i.val hiG
        step as ⟨ j, hj ⟩
        have hj' : j.val = i.val + 1 := by scalar_tac
        have hin := eq_of_spec (cl_givens_nodup_inner_spec q G hG i hiv G.length true j
          (by omega))
        rw [hin]
        simp only [bind_tc_ok]
        step as ⟨ i2, hi2 ⟩
        have hi2' : i2.val = i.val + 1 := by scalar_tac
        have hiff : NodupBoth (G.drop i.val)
            ↔ ((∀ y ∈ G.drop (i.val + 1), G[i.val]!.1 ≠ y.1 ∧ G[i.val]!.2 ≠ y.2)
                ∧ NodupBoth (G.drop (i.val + 1))) := by
          rw [hcons]
          exact nodupBoth_cons _ _
        refine WP.spec_mono (ih _ i2 (by omega)) ?_
        intro r hr
        refine hr.trans ?_
        rw [hh, hj', hi2']
        simp only [Bool.true_and]
        refine congrArg (Prod.mk q) ?_
        refine Bool.eq_iff_iff.mpr ?_
        simp only [Bool.and_eq_true, decide_eq_true_eq]
        exact hiff.symm
      · next hge =>
        have h1 : q.givens.val.length ≤ i.val := by scalar_tac
        have hnil : G.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hh, hnil]; simp [NodupBoth])
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)

theorem cl_givens_check_loop_spec (p : types.Problem) (q : types.Plan)
    (G : List (Nat × Nat)) (hG : SolverSpec.givens q = G) :
    ∀ (m : Nat) (ok1 : Bool) (i : Std.Usize), G.length - i.val ≤ m →
      clauses.cl_givens_loop1 p q ok1 i ⦃ r =>
        r = (ok1 && decide (∀ gn ∈ G.drop i.val,
              (∃ g ∈ SolverSpec.givenGroups p, gn.2 ∈ g) ∧ PairOkR p q G gn)) ⦄ := by
  have hlen : G.length = q.givens.val.length := givens_length_of q G hG
  intro m
  induction m with
  | zero =>
    intro ok1 i hfuel
    rw [clauses.cl_givens_loop1.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · exfalso
        have h1 : i.val < q.givens.val.length := by scalar_tac
        omega
      · next hge =>
        have h1 : q.givens.val.length ≤ i.val := by scalar_tac
        have hnil : G.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)
  | succ m ih =>
    intro ok1 i hfuel
    rw [clauses.cl_givens_loop1.eq_def]
    dsimp only
    split
    · next hh =>
      split
      · next hlt =>
        have hiv : i.val < q.givens.val.length := by scalar_tac
        have hiG : i.val < G.length := by omega
        have hbnd : i.val + 1 ≤ Usize.max := by scalar_tac
        obtain ⟨e, n, hen⟩ : ∃ e n, (q.givens.val[i.val]'hiv) = (e, n) := ⟨_, _, rfl⟩
        have hGi : G[i.val]! = (e.val, n.val) := by
          rw [pairs_getElem! q.givens.val G (givens_pairs q G hG) i.val hiv, hen]
        have hcons : G.drop i.val = G[i.val]! :: G.drop (i.val + 1) :=
          drop_cons_getElem! G i.val hiG
        rw [index_eq q.givens i hiv, hen]
        show (do
            let b ← clauses.is_declared_given p n
            let ok2 ← if b
                      then (do
                        let b1 ← clauses.givens_at p q i
                        if b1 then ok true else ok false)
                      else ok false
            let i3 ← i + 1#usize
            clauses.cl_givens_loop1 p q ok2 i3) ⦃ r =>
              r = (ok1 && decide (∀ gn ∈ G.drop i.val,
                    (∃ g ∈ SolverSpec.givenGroups p, gn.2 ∈ g) ∧ PairOkR p q G gn)) ⦄
        rw [eq_of_spec (is_declared_given_spec p n)]
        simp only [bind_tc_ok]
        split
        · next hdecl =>
          have hd : ∃ g ∈ SolverSpec.givenGroups p, n.val ∈ g := of_decide_eq_true hdecl
          rw [eq_of_spec (givens_at_spec p q i hiv G hG)]
          simp only [bind_tc_ok]
          split
          · next hat =>
            have hpo : PairOkR p q G (G[i.val]!) := of_decide_eq_true hat
            have hhead : (∃ g ∈ SolverSpec.givenGroups p, (G[i.val]!).2 ∈ g)
                ∧ PairOkR p q G (G[i.val]!) := ⟨by rw [hGi]; exact hd, hpo⟩
            have hiff : (∀ gn ∈ G.drop (i.val + 1),
                  (∃ g ∈ SolverSpec.givenGroups p, gn.2 ∈ g) ∧ PairOkR p q G gn)
                ↔ (∀ gn ∈ G.drop i.val,
                  (∃ g ∈ SolverSpec.givenGroups p, gn.2 ∈ g) ∧ PairOkR p q G gn) := by
              rw [hcons]
              constructor
              · intro hc x hx
                rcases List.mem_cons.mp hx with hx' | hx'
                · rw [hx']; exact hhead
                · exact hc x hx'
              · intro hc x hx
                exact hc x (List.mem_cons_of_mem _ hx)
            simp only [bind_tc_ok]
            step as ⟨ i3, hi3 ⟩
            have hi3' : i3.val = i.val + 1 := by scalar_tac
            refine WP.spec_mono (ih true i3 (by omega)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hh, hi3']
            simp only [Bool.true_and]
            exact decide_eq_decide.mpr hiff
          · next hat =>
            have hnpo : ¬ PairOkR p q G (G[i.val]!) :=
              of_decide_eq_false (by simpa using hat)
            have hno : ¬ (∀ gn ∈ G.drop i.val,
                (∃ g ∈ SolverSpec.givenGroups p, gn.2 ∈ g) ∧ PairOkR p q G gn) := by
              intro hc
              exact hnpo (hc G[i.val]! (by rw [hcons]; exact List.mem_cons_self)).2
            simp only [bind_tc_ok]
            step as ⟨ i3, hi3 ⟩
            refine WP.spec_mono (ih false i3 (by omega)) ?_
            intro r hr
            refine hr.trans ?_
            rw [hh]
            simp only [Bool.false_and, Bool.true_and]
            exact (decide_eq_false hno).symm
        · next hdecl =>
          have hnd : ¬ (∃ g ∈ SolverSpec.givenGroups p, n.val ∈ g) :=
            of_decide_eq_false (by simpa using hdecl)
          have hno : ¬ (∀ gn ∈ G.drop i.val,
              (∃ g ∈ SolverSpec.givenGroups p, gn.2 ∈ g) ∧ PairOkR p q G gn) := by
            intro hc
            have := (hc G[i.val]! (by rw [hcons]; exact List.mem_cons_self)).1
            rw [hGi] at this
            exact hnd this
          simp only [bind_tc_ok]
          step as ⟨ i3, hi3 ⟩
          refine WP.spec_mono (ih false i3 (by omega)) ?_
          intro r hr
          refine hr.trans ?_
          rw [hh]
          simp only [Bool.false_and, Bool.true_and]
          exact (decide_eq_false hno).symm
      · next hge =>
        have h1 : q.givens.val.length ≤ i.val := by scalar_tac
        have hnil : G.drop i.val = [] := List.drop_eq_nil_iff.mpr (by omega)
        exact ok_post (by rw [hh, hnil]; simp)
    · next hh =>
      have hf : ok1 = false := by simpa using hh
      exact ok_post (by rw [hf]; simp)

/-- What the checker ACTUALLY decides: the clause with property agreement
confined to the property table and the sentinel written in. Unconditional, and
exact. -/
theorem cl_givens_raw_spec (p : types.Problem) (q : types.Plan) :
    clauses.cl_givens p q ⦃ r => r = decide (GivensR p q) ⦄ := by
  have hzero : (0#usize : Std.Usize).val = 0 := by simp
  rw [clauses.cl_givens.eq_def]
  have h0 := eq_of_spec (cl_givens_nodup_spec q (SolverSpec.givens q) rfl
    (SolverSpec.givens q).length true 0#usize (by scalar_tac))
  rw [h0]
  show clauses.cl_givens_loop1 p q
      (true && decide (NodupBoth ((SolverSpec.givens q).drop (0#usize : Std.Usize).val)))
      0#usize ⦃ r => r = decide (GivensR p q) ⦄
  refine WP.spec_mono (cl_givens_check_loop_spec p q (SolverSpec.givens q) rfl
    (SolverSpec.givens q).length _ 0#usize (by scalar_tac)) ?_
  intro r hr
  refine hr.trans ?_
  rw [hzero, List.drop_zero]
  refine Bool.eq_iff_iff.mpr ?_
  -- `GivensR` is the pair of conjuncts the two loops just decided, so this
  -- closes by `rfl` once the `Bool` algebra is out of the way.
  simp only [Bool.and_eq_true, Bool.true_and, decide_eq_true_eq]

/-! ## The gap, machine checked

The bit-width defect `Proof/Slots.lean` records for `same_slots` is present here
too, one table over: `same_props` compares two sets of `n_props` bits. The plan
below is the smallest witness -- one node whose only property is `0` in a problem
that declares no properties at all, and an endpoint with none. `cl_indexed`
rejects it, on the conjunct that bounds a node's properties by `nProps`, so
`check` is right about it and `check_spec` is unharmed. What it rules out is an
UNCONDITIONAL `cl_givens_spec`. -/

namespace Givens.Counterexample

def cxNode : types.Node :=
  { props := ⟨[0#usize], by scalar_tac⟩, parents := ⟨[], by scalar_tac⟩ }

def cxTransform : types.Transform :=
  { requires := ⟨[], by scalar_tac⟩, produces := ⟨[], by scalar_tac⟩ }

def cxP : types.Problem :=
  { n_props := 0#usize
    nodes := ⟨[cxNode], by scalar_tac⟩
    transforms := ⟨[cxTransform], by scalar_tac⟩
    given := ⟨[⟨[0#usize], by scalar_tac⟩], by scalar_tac⟩
    given_tr := 0#usize
    target_tr := 0#usize }

def cxEndpoint : types.Endpoint :=
  { props := ⟨[], by scalar_tac⟩, parents := ⟨[], by scalar_tac⟩ }

def cxQ : types.Plan :=
  { endpoints := ⟨[cxEndpoint], by scalar_tac⟩
    givens := ⟨[(0#usize, 0#usize)], by scalar_tac⟩
    steps := ⟨[], by scalar_tac⟩ }

theorem cx_givens : SolverSpec.givens cxQ = [(0, 0)] := by
  simp [SolverSpec.givens, SolverSpec.idPairs, cxQ]

theorem cx_nodeProps : SolverSpec.nodeProps cxP 0 = [0] := by
  simp [SolverSpec.nodeProps, SolverSpec.nd, SolverSpec.nats, cxP, cxNode]

theorem cx_epProps : SolverSpec.epProps cxQ 0 = [] := by
  simp [SolverSpec.epProps, SolverSpec.ep, SolverSpec.nats, cxQ, cxEndpoint]

theorem cx_nodeParents : SolverSpec.nodeParents cxP 0 = [] := by
  simp [SolverSpec.nodeParents, SolverSpec.nd, SolverSpec.nats, cxP, cxNode]

theorem cx_epParents : SolverSpec.epParents cxQ 0 = [] := by
  simp [SolverSpec.epParents, SolverSpec.ep, SolverSpec.nats, cxQ, cxEndpoint]

theorem cx_groups : SolverSpec.givenGroups cxP = [[0]] := by
  simp [SolverSpec.givenGroups, SolverSpec.idLists, SolverSpec.nats, cxP]

theorem cx_nProps : SolverSpec.nProps cxP = 0 := rfl

theorem cx_givensR : GivensR cxP cxQ := by
  refine ⟨?_, ?_⟩
  · unfold NodupBoth
    rw [cx_givens]
    simp
  · rw [cx_givens]
    intro gn hgn
    have hgn' : gn = (0, 0) := by simpa using hgn
    subst hgn'
    refine ⟨⟨[0], by rw [cx_groups]; simp, by simp⟩, ?_, ?_, ?_⟩
    · rw [cx_nProps]
      intro j hj
      exact absurd hj (Nat.not_lt_zero j)
    · rw [cx_epParents]
      intro f hf
      simp at hf
    · rw [cx_nodeParents]
      intro a ha
      simp at ha

theorem cx_not_spec : ¬ GivensSpec cxP cxQ := by
  intro hc
  have h4 := hc.2.2.2.1 (0, 0) (by rw [cx_givens]; simp)
  have h := h4.2 0 (by rw [cx_nodeProps]; simp)
  rw [cx_epProps] at h
  simp at h

/-- The checker accepts the plan the specification's conjunct rejects. This is
the gap, as a computation rather than as a claim. -/
theorem cx_checker : clauses.cl_givens cxP cxQ = ok true := by
  rw [eq_of_spec (cl_givens_raw_spec cxP cxQ), decide_eq_true cx_givensR]

/-- And `check` is not wrong about it: clause 0 rejects the plan, on exactly the
conjunct the bit width elides. -/
theorem cx_not_wellIndexed : ¬ SolverSpec.WellIndexed cxP cxQ := by
  intro hc
  obtain ⟨-, -, -, -, -, -, h7, -⟩ := hc
  have h := h7 0 (by simp [SolverSpec.nNodes, cxP]) 0 (by rw [cx_nodeProps]; simp)
  rw [cx_nProps] at h
  exact absurd h (Nat.not_lt_zero 0)

end Givens.Counterexample

/-! ## The `givens` clause, as the specification states it

`givensR_iff_givens` is where both range restrictions are paid for, and `hwi` is
what pays: the property comparison is `nProps` bits wide, and the two resolvers
report absence with a value that is itself an id. See the module header. -/

theorem givensR_iff_givens (p : types.Problem) (q : types.Plan)
    (hwi : SolverSpec.WellIndexed p q) :
    GivensR p q ↔ GivensSpec p q := by
  obtain ⟨-, -, -, hgr, -, -, hnp, hep, -, -⟩ := hwi
  have hprops : ∀ gn ∈ SolverSpec.givens q,
      (∀ x ∈ SolverSpec.nodeProps p gn.2, x < SolverSpec.nProps p) ∧
      (∀ x ∈ SolverSpec.epProps q gn.1, x < SolverSpec.nProps p) := by
    intro gn hgn
    exact ⟨hnp gn.2 (hgr gn hgn).2, hep gn.1 (hgr gn hgn).1⟩
  have hne1 : ∀ gn ∈ SolverSpec.givens q, gn.1 ≠ access.NONE.val := by
    intro gn hgn hc
    have h1 := (hgr gn hgn).1
    have h2 := endpoints_bound q
    omega
  have hne2 : ∀ gn ∈ SolverSpec.givens q, gn.2 ≠ access.NONE.val := by
    intro gn hgn hc
    have h1 := (hgr gn hgn).2
    have h2 := nodes_bound p
    omega
  constructor
  · rintro ⟨⟨hn1, hn2⟩, hrest⟩
    refine ⟨hn1, hn2, fun gn hgn => (hrest gn hgn).1, ?_, ?_, ?_⟩
    · intro gn hgn
      have hss := (hrest gn hgn).2.1
      have hr := (sameSetLt_iff _ _ _ (hprops gn hgn).1 (hprops gn hgn).2).mp hss
      exact ⟨hr.2, hr.1⟩
    · intro gn hgn f hf
      obtain ⟨h1, h2⟩ := (hrest gn hgn).2.2.1 f hf
      obtain ⟨hn, hm, he, hv⟩ := nodeOf_mem _ f h1
      exact ⟨hn, hm, he, by rw [hv]; exact h2⟩
    · intro gn hgn a ha
      obtain ⟨h1, h2⟩ := (hrest gn hgn).2.2.2 a ha
      obtain ⟨hn, hm, he, hv⟩ := epOf_mem _ a h1
      exact ⟨hn, hm, he, by rw [hv]; exact h2⟩
  · rintro ⟨hn1, hn2, hdecl, hsame, hfwd, hbwd⟩
    refine ⟨⟨hn1, hn2⟩, ?_⟩
    intro gn hgn
    refine ⟨hdecl gn hgn, ?_, ?_, ?_⟩
    · refine (sameSetLt_iff _ _ _ (hprops gn hgn).1 (hprops gn hgn).2).mpr ?_
      exact ⟨(hsame gn hgn).2, (hsame gn hgn).1⟩
    · intro f hf
      obtain ⟨hn, hm, he, hv⟩ := hfwd gn hgn f hf
      have hnne : hn.2 ≠ access.NONE.val := hne2 hn hm
      have hres : nodeOf (SolverSpec.givens q) f = hn.2 := by
        rw [← he]
        exact nodeOf_eq _ hn hn1 hm hnne
      exact ⟨by rw [hres]; exact hnne, by rw [hres]; exact hv⟩
    · intro a ha
      obtain ⟨hn, hm, he, hv⟩ := hbwd gn hgn a ha
      have hnne : hn.1 ≠ access.NONE.val := hne1 hn hm
      have hres : epOf (SolverSpec.givens q) a = hn.1 := by
        rw [← he]
        exact epOf_eq _ hn hn2 hm hnne
      exact ⟨by rw [hres]; exact hnne, by rw [hres]; exact hv⟩

/-- The `givens` conjunct of `SolverSpec.ValidC`.

**`hwi` is not decoration.** See the module header and
`Givens.Counterexample.cx_checker`: without it the equation is FALSE, and the
counterexample is a one-node, one-endpoint plan. -/
theorem cl_givens_spec (p : types.Problem) (q : types.Plan)
    (hwi : SolverSpec.WellIndexed p q) :
    clauses.cl_givens p q ⦃ r =>
      r = decide (((SolverSpec.givens q).map Prod.fst).Nodup ∧
        ((SolverSpec.givens q).map Prod.snd).Nodup ∧
        (∀ gn ∈ SolverSpec.givens q, ∃ g ∈ SolverSpec.givenGroups p, gn.2 ∈ g) ∧
        (∀ gn ∈ SolverSpec.givens q,
            SolverSpec.SameSet (SolverSpec.epProps q gn.1) (SolverSpec.nodeProps p gn.2)) ∧
        (∀ gn ∈ SolverSpec.givens q, ∀ f ∈ SolverSpec.epParents q gn.1,
            ∃ hn ∈ SolverSpec.givens q, hn.1 = f ∧ hn.2 ∈ SolverSpec.nodeParents p gn.2) ∧
        (∀ gn ∈ SolverSpec.givens q, ∀ a ∈ SolverSpec.nodeParents p gn.2,
            ∃ hn ∈ SolverSpec.givens q, hn.2 = a ∧ hn.1 ∈ SolverSpec.epParents q gn.1)) ⦄ := by
  refine WP.spec_mono (cl_givens_raw_spec p q) ?_
  intro r hr
  refine hr.trans ?_
  refine Bool.eq_iff_iff.mpr ?_
  simp only [decide_eq_true_eq]
  exact givensR_iff_givens p q hwi

#print axioms SolverProof.same_props_spec
#print axioms SolverProof.given_node_of_spec
#print axioms SolverProof.given_ep_of_spec
#print axioms SolverProof.givens_at_spec
#print axioms SolverProof.is_declared_given_spec
#print axioms SolverProof.cl_givens_raw_spec
#print axioms SolverProof.Givens.Counterexample.cx_checker
#print axioms SolverProof.Givens.Counterexample.cx_not_spec
#print axioms SolverProof.Givens.Counterexample.cx_not_wellIndexed
#print axioms SolverProof.givensR_iff_givens
#print axioms SolverProof.cl_givens_spec

end SolverProof
