/-
  Where the ten clause lemmas meet.

  This is the only module that imports every other, which makes it the first
  place a name declared twice across two sibling modules would be rejected. The
  siblings were written concurrently and each invented its own helpers, so two
  of them kept theirs in an inner namespace (`Slots`, `Lineage`) rather than at
  `SolverProof`. That is what keeps this file buildable, and it is why a helper
  belongs either in `Basis` or behind a module namespace, never bare.
-/

import SolverWitness.Proof.Basis
import SolverWitness.Proof.Bits
import SolverWitness.Proof.Access
import SolverWitness.Proof.Ancestry
import SolverWitness.Proof.Steps
import SolverWitness.Proof.Slots
import SolverWitness.Proof.Lineage
import SolverWitness.Proof.Givens
import SolverWitness.Proof.Indexed

namespace SolverProof

open Aeneas Aeneas.Std Result
-- `types.*` lives under `solver_witness`; `Spec.lean` opens it the same way.
open solver_witness

/- The clause lemmas, reachable from one place. Three are stated under
   `WellIndexed` and two more under a range hypothesis it supplies, which is the
   case split `check_spec` owes; the `_raw` and `_exact` variants beside them are
   unconditional and carry the other branch. -/
#check @cl_indexed_spec
#check @cl_target_spec
#check @cl_unique_producer_spec
#check @cl_provenance_spec
#check @cl_schedulable_spec
#check @cl_shape_spec
#check @cl_shape_raw_spec
#check @cl_derived_spec
#check @cl_derived_raw_spec
#check @cl_conformance_spec
#check @cl_conformance_spec_exact
#check @cl_emission_spec
#check @cl_emission_spec_exact
#check @cl_givens_spec
#check @cl_givens_raw_spec
#check @ancestors_spec
#check @descends_spec

namespace Compose

set_option maxHeartbeats 2000000
set_option synthInstance.maxSize 1000
set_option synthInstance.maxHeartbeats 4000000

/-! ## The ten conjuncts, indexed the way `clause_holds` dispatches them

`SolverSpec.ValidC` is a right-nested conjunction and the loop walks an index, so
the two are bridged by a function from the index to that conjunct's decision.
Written as a `Bool` rather than as a `Prop` because that is what the loop carries:
the `ok` flag is `true` until a clause clears it, and nothing in the extraction
ever sees the proposition. -/
def cb (p : types.Problem) (q : types.Plan) : Nat → Bool
  | 0 => decide (SolverSpec.WellIndexed p q)
  | 1 => decide (∀ s ∈ SolverSpec.steps q, SolverSpec.Shape p s)
  | 2 => decide (∀ s ∈ SolverSpec.steps q, ∀ b ∈ s.used,
            SolverSpec.Satisfies p q s.used b.2 b.1)
  | 3 => decide (∀ s ∈ SolverSpec.steps q, ∀ g ∈ s.produced, ∀ b ∈ g,
            SolverSpec.Satisfies p q s.used b.2 b.1)
  | 4 => decide (∀ s ∈ SolverSpec.steps q, ∀ g ∈ s.produced, ∀ b ∈ g,
            SolverSpec.Derived q s.used b.2)
  | 5 => decide (∀ e < SolverSpec.nEndpoints q, ∀ i ∈ (SolverSpec.steps q).zipIdx,
            ∀ j ∈ (SolverSpec.steps q).zipIdx,
              SolverSpec.Emits i.1 e → SolverSpec.Emits j.1 e → i.2 = j.2)
  | 6 => decide (∀ s ∈ SolverSpec.steps q, ∀ b ∈ s.used,
            (∃ s' ∈ SolverSpec.steps q, SolverSpec.Emits s' b.2) ∨
            (∃ gn ∈ SolverSpec.givens q, gn.1 = b.2))
  | 7 => decide (((SolverSpec.givens q).map Prod.fst).Nodup ∧
            ((SolverSpec.givens q).map Prod.snd).Nodup ∧
            (∀ gn ∈ SolverSpec.givens q, ∃ g ∈ SolverSpec.givenGroups p, gn.2 ∈ g) ∧
            (∀ gn ∈ SolverSpec.givens q,
              SolverSpec.SameSet (SolverSpec.epProps q gn.1) (SolverSpec.nodeProps p gn.2)) ∧
            (∀ gn ∈ SolverSpec.givens q, ∀ f ∈ SolverSpec.epParents q gn.1,
              ∃ hn ∈ SolverSpec.givens q, hn.1 = f ∧ hn.2 ∈ SolverSpec.nodeParents p gn.2) ∧
            (∀ gn ∈ SolverSpec.givens q, ∀ a ∈ SolverSpec.nodeParents p gn.2,
              ∃ hn ∈ SolverSpec.givens q, hn.2 = a ∧ hn.1 ∈ SolverSpec.epParents q gn.1))
  | 8 => decide (∀ cj ∈ (SolverSpec.steps q).zipIdx, ∀ b ∈ cj.1.used,
            ∀ pi ∈ (SolverSpec.steps q).zipIdx, SolverSpec.Emits pi.1 b.2 → pi.2 < cj.2)
  | 9 => decide (((SolverSpec.steps q).filter
            (fun s => s.transform == SolverSpec.targetTr p)).length = 1)
  | _ + 10 => true

/-- The conjunction of the clauses still ahead of index `k`, on the fuel `n` that
counts them. Two arguments rather than one so the recursion is structural: `n` is
what decreases, `k` is what indexes. -/
def conjB (f : Nat → Bool) : Nat → Nat → Bool
  | 0, _ => true
  | n + 1, k => f k && conjB f n (k + 1)

theorem conj_ten (f : Nat → Bool) :
    conjB f 10 0 =
      (f 0 && (f 1 && (f 2 && (f 3 && (f 4 && (f 5 && (f 6 && (f 7 && (f 8 && (f 9 && true)))))))))) :=
  rfl

/-! ## The loop -/

theorem n_clauses : clauses.N_CLAUSES = 10#usize := by
  simp [clauses.N_CLAUSES]

/-- The loop guard, as a fact about the index. `N_CLAUSES` is `irreducible`, so
nothing reduces it on its own. -/
theorem lt_clauses (k : Std.Usize) : k < clauses.N_CLAUSES ↔ k.val < 10 := by
  rw [n_clauses]
  constructor <;> intro h <;> scalar_tac

/-- The index advances and cannot overflow: there are ten clauses. -/
theorem succ_ok (k : Std.Usize) (hk : k.val < 10) :
    ∃ k1 : Std.Usize, k + 1#usize = ok k1 ∧ k1.val = k.val + 1 := by
  obtain ⟨k1, h1, h2⟩ := ok_of_spec (Std.Usize.add_spec (x := k) (y := 1#usize) (by scalar_tac))
  exact ⟨k1, h1, by scalar_tac⟩

/-- One iteration. The flag is cleared by conjunction rather than by a branch,
which is what makes the ten clauses compose as a `Bool` fold. -/
theorem step_eq (p : types.Problem) (q : types.Plan)
    (anc : alloc.vec.Vec (alloc.vec.Vec Bool)) (b v : Bool) (k k1 : Std.Usize)
    (hk : k.val < 10)
    (hc : clauses.clause_holds p q (alloc.vec.Vec.deref anc) k = ok v)
    (hk1 : k + 1#usize = ok k1) :
    solver_witness.check_loop p q anc b k = solver_witness.check_loop p q anc (b && v) k1 := by
  conv_lhs => rw [solver_witness.check_loop.eq_def]
  rw [if_pos ((lt_clauses k).mpr hk)]
  simp only [hc, bind_tc_ok, hk1]
  cases v <;> simp

/-- The exhausted loop reports the flag it carried. -/
theorem done_eq (p : types.Problem) (q : types.Plan)
    (anc : alloc.vec.Vec (alloc.vec.Vec Bool)) (b : Bool) (k : Std.Usize)
    (hk : ¬ (k.val < 10)) :
    solver_witness.check_loop p q anc b k = ok b := by
  rw [solver_witness.check_loop.eq_def, if_neg (fun hc => hk ((lt_clauses k).mp hc))]

/-- What the loop computes, given what each clause decides. `f` is a parameter
rather than `cb` itself because the two branches of `check_spec` instantiate it
differently: the conditional clause lemmas are available only under
`WellIndexed`. -/
theorem check_loop_spec (p : types.Problem) (q : types.Plan)
    (anc : alloc.vec.Vec (alloc.vec.Vec Bool)) (f : Nat → Bool)
    (hcl : ∀ k : Std.Usize, k.val < 10 →
      clauses.clause_holds p q (alloc.vec.Vec.deref anc) k = ok (f k.val)) :
    ∀ (n : Nat) (k : Std.Usize) (b : Bool), 10 - k.val ≤ n →
      solver_witness.check_loop p q anc b k = ok (b && conjB f (10 - k.val) k.val) := by
  intro n
  induction n with
  | zero =>
    intro k b h
    have hk : ¬ (k.val < 10) := by omega
    have h0 : 10 - k.val = 0 := by omega
    rw [done_eq p q anc b k hk, h0]
    simp only [conjB, Bool.and_true]
  | succ n ih =>
    intro k b h
    by_cases hk : k.val < 10
    · obtain ⟨k1, hk1, hk1v⟩ := succ_ok k hk
      refine (step_eq p q anc b (f k.val) k k1 hk (hcl k hk) hk1).trans ?_
      refine (ih k1 (b && f k.val) (by omega)).trans ?_
      have hrem : 10 - k.val = (10 - (k.val + 1)) + 1 := by omega
      rw [hk1v, hrem]
      simp only [conjB, Bool.and_assoc]
    · have h0 : 10 - k.val = 0 := by omega
      rw [done_eq p q anc b k hk, h0]
      simp only [conjB, Bool.and_true]

/-- Once the flag is cleared it stays cleared, and that needs nothing of the
clauses but that they answer. This is the branch where `cl_indexed` has already
failed, so no clause lemma below it is available in the form the conjunction
would want. -/
theorem check_loop_false (p : types.Problem) (q : types.Plan)
    (anc : alloc.vec.Vec (alloc.vec.Vec Bool))
    (hex : ∀ k : Std.Usize, k.val < 10 →
      ∃ v, clauses.clause_holds p q (alloc.vec.Vec.deref anc) k = ok v) :
    ∀ (n : Nat) (k : Std.Usize), 10 - k.val ≤ n →
      solver_witness.check_loop p q anc false k = ok false := by
  intro n
  induction n with
  | zero =>
    intro k h
    exact done_eq p q anc false k (by omega)
  | succ n ih =>
    intro k h
    by_cases hk : k.val < 10
    · obtain ⟨v, hv⟩ := hex k hk
      obtain ⟨k1, hk1, hk1v⟩ := succ_ok k hk
      refine (step_eq p q anc false v k k1 hk hv hk1).trans ?_
      rw [Bool.false_and]
      exact ih k1 (by omega)
    · exact done_eq p q anc false k hk

end Compose

open Compose

/-! ## The obligation

  Moved here from `Spec.lean` because the import direction forbids it there:
  every proof module imports the specification for its vocabulary, so the
  specification cannot import them back to use their lemmas.

  Stated as ONE EQUATION rather than as the biconditional. The biconditional
  alone does not pin the negative case -- a checker that FAILS on every invalid
  plan satisfies `check p q = ok true ↔ Valid p q` vacuously. The equation gives
  totality, soundness and completeness together.

  **The proof case-splits on `WellIndexed`.** Five of the ten clause lemmas --
  Shape, Derived, Conformance, Emission, Givens -- are equivalent to their
  conjuncts only under it, for three unrelated reasons, each machine-checked in
  the module that found it. On the branch where `cl_indexed` is false, `check` is
  false and so is `Valid`, and the unconditional `_raw`/`_exact` forms carry that
  branch. On the branch where it holds, its conjuncts ARE the hypotheses those
  five need. `cl_indexed_spec` is unconditional, which is what makes the split
  available at all.

  The two lineage lemmas are stated about a table they are handed, so what ties
  them to the checker is that `check` hands them the table `ancestors` built:
  `ancestors_spec` supplies both its length and `RowsOk`, and the same value goes
  to `clause_holds`. Against any other table they would be true and say nothing.
-/

set_option maxHeartbeats 4000000
set_option synthInstance.maxSize 1000
set_option synthInstance.maxHeartbeats 4000000

theorem check_spec (p : types.Problem) (q : types.Plan) :
    solver_witness.check p q = Result.ok (decide (SolverSpec.Valid p q)) := by
  rw [decide_eq_decide.mpr (SolverSpec.valid_iff_validC p q)]
  obtain ⟨anc, hanc, hlen, hrows⟩ := ok_of_spec (ancestors_spec q)
  -- `Slice` is a `def` for the same subtype, so the table `descends` reads is
  -- the table `ancestors` built -- definitionally, which is why these are
  -- `exact` and not `rw`. See `Proof/Ancestry.lean`'s header.
  have hlenD : (alloc.vec.Vec.deref anc).val.length = SolverSpec.nEndpoints q := hlen
  have hrowsD : RowsOk q (SolverSpec.nEndpoints q) (alloc.vec.Vec.deref anc).val := hrows
  rw [solver_witness.check.eq_def, hanc]
  simp only [bind_tc_ok]
  by_cases hwi : SolverSpec.WellIndexed p q
  · -- `WellIndexed` holds, so the five conditional clause lemmas apply and the
    -- ten conjuncts are exactly what the loop folds.
    have hsteps := hwi.2.2.2.2.2.2.2.2.2
    have hrC : ∀ s ∈ SolverSpec.steps q, ∀ b ∈ s.used, b.2 < SolverSpec.nEndpoints q :=
      fun s hs b hb => ((hsteps s hs).2.1 b hb).2
    have hrE : ∀ s ∈ SolverSpec.steps q, ∀ g ∈ s.produced, ∀ b ∈ g,
        b.2 < SolverSpec.nEndpoints q :=
      fun s hs g hg b hb => ((hsteps s hs).2.2 g hg b hb).2
    have e0 := eq_of_spec (cl_indexed_spec p q)
    have e1 := eq_of_spec (cl_shape_spec p q hwi)
    have e2 := eq_of_spec (cl_conformance_spec p q (alloc.vec.Vec.deref anc)
      (SolverSpec.nEndpoints q) hlenD hrowsD hrC)
    have e3 := eq_of_spec (cl_emission_spec p q (alloc.vec.Vec.deref anc)
      (SolverSpec.nEndpoints q) hlenD hrowsD hrE)
    have e4 := eq_of_spec (cl_derived_spec p q hwi)
    have e5 := eq_of_spec (cl_unique_producer_spec q)
    have e6 := eq_of_spec (cl_provenance_spec q)
    have e7 := eq_of_spec (cl_givens_spec p q hwi)
    have e8 := eq_of_spec (cl_schedulable_spec q)
    have e9 := eq_of_spec (cl_target_spec p q)
    have hcl : ∀ k : Std.Usize, k.val < 10 →
        clauses.clause_holds p q (alloc.vec.Vec.deref anc) k = ok (Compose.cb p q k.val) := by
      intro k hk
      rw [clauses.clause_holds.eq_def]
      split
      · rename_i h; subst h
        rw [show ((0#usize : Std.Usize).val) = 0 from by scalar_tac]; exact e0
      · split
        · rename_i h; subst h
          rw [show ((1#usize : Std.Usize).val) = 1 from by scalar_tac]; exact e1
        · split
          · rename_i h; subst h
            rw [show ((2#usize : Std.Usize).val) = 2 from by scalar_tac]; exact e2
          · split
            · rename_i h; subst h
              rw [show ((3#usize : Std.Usize).val) = 3 from by scalar_tac]; exact e3
            · split
              · rename_i h; subst h
                rw [show ((4#usize : Std.Usize).val) = 4 from by scalar_tac]; exact e4
              · split
                · rename_i h; subst h
                  rw [show ((5#usize : Std.Usize).val) = 5 from by scalar_tac]; exact e5
                · split
                  · rename_i h; subst h
                    rw [show ((6#usize : Std.Usize).val) = 6 from by scalar_tac]; exact e6
                  · split
                    · rename_i h; subst h
                      rw [show ((7#usize : Std.Usize).val) = 7 from by scalar_tac]; exact e7
                    · split
                      · rename_i h; subst h
                        rw [show ((8#usize : Std.Usize).val) = 8 from by scalar_tac]; exact e8
                      · split
                        · rename_i h; subst h
                          rw [show ((9#usize : Std.Usize).val) = 9 from by scalar_tac]; exact e9
                        · exfalso; scalar_tac
    rw [check_loop_spec p q anc (Compose.cb p q) hcl 10 0#usize true (by scalar_tac)]
    rw [show ((0#usize : Std.Usize).val) = 0 from by scalar_tac]
    rw [Compose.conj_ten (Compose.cb p q)]
    refine congrArg Result.ok ?_
    refine Bool.eq_iff_iff.mpr ?_
    -- `ValidC` is this conjunction, in this order and with this association, so
    -- once the `Bool` algebra is out of the way the two sides are the same term.
    simp only [Compose.cb, Bool.true_and, Bool.and_true, Bool.and_eq_true, decide_eq_true_eq]
  · -- `cl_indexed` is exact and unconditional, so clause 0 clears the flag and
    -- the loop can only report `false`. `Valid` fails on its own `indexed`
    -- field, so both sides are `false` and no other clause is consulted.
    have hex : ∀ k : Std.Usize, k.val < 10 →
        ∃ v, clauses.clause_holds p q (alloc.vec.Vec.deref anc) k = ok v := by
      intro k hk
      rw [clauses.clause_holds.eq_def]
      split
      · exact ⟨_, eq_of_spec (cl_indexed_spec p q)⟩
      · split
        · exact ⟨_, eq_of_spec (cl_shape_raw_spec p q)⟩
        · split
          · exact ⟨_, eq_of_spec (cl_conformance_spec_exact p q (alloc.vec.Vec.deref anc)
              (SolverSpec.nEndpoints q) hlenD hrowsD)⟩
          · split
            · exact ⟨_, eq_of_spec (cl_emission_spec_exact p q (alloc.vec.Vec.deref anc)
                (SolverSpec.nEndpoints q) hlenD hrowsD)⟩
            · split
              · exact ⟨_, eq_of_spec (cl_derived_raw_spec q)⟩
              · split
                · exact ⟨_, eq_of_spec (cl_unique_producer_spec q)⟩
                · split
                  · exact ⟨_, eq_of_spec (cl_provenance_spec q)⟩
                  · split
                    · exact ⟨_, eq_of_spec (cl_givens_raw_spec p q)⟩
                    · split
                      · exact ⟨_, eq_of_spec (cl_schedulable_spec q)⟩
                      · split
                        · exact ⟨_, eq_of_spec (cl_target_spec p q)⟩
                        · exact ⟨_, rfl⟩
    have h0 : clauses.clause_holds p q (alloc.vec.Vec.deref anc) 0#usize = ok false := by
      rw [clauses.clause_holds.eq_def, if_pos rfl]
      rw [eq_of_spec (cl_indexed_spec p q), decide_eq_false hwi]
    obtain ⟨k1, hk1, hk1v⟩ := succ_ok 0#usize (by scalar_tac)
    rw [step_eq p q anc true false 0#usize k1 (by scalar_tac) h0 hk1]
    rw [Bool.and_false]
    rw [check_loop_false p q anc hex 10 k1 (by omega)]
    exact congrArg Result.ok (decide_eq_false (fun hv => hwi hv.1)).symm

theorem check_correct (p : types.Problem) (q : types.Plan) :
    solver_witness.check p q = Result.ok true ↔ SolverSpec.Valid p q := by
  rw [check_spec]; simp

end SolverProof
